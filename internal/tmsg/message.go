package tmsg

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
)

// ErrMessageDeleted reports a message that is not there any more.
//
// It is an answer, not a failure: a queued download whose post was deleted
// between being indexed and being fetched is skipped and counted, and the task
// carries on. Callers distinguish it from a request that did not complete,
// which has to be retried.
var ErrMessageDeleted = errors.New("message may be deleted")

// GetSingleMessage reads one message by id.
//
// It reads history rather than using messages.getMessages, which is what the
// upstream implementation did. The difference matters: this one costs a request
// per message, and it is the caller's job to prefer BatchReader (see reader.go)
// when it has more than one id to fetch.
func GetSingleMessage(ctx context.Context, client *tg.Client, peer tg.InputPeerClass, id int) (*tg.Message, error) {
	iterator := query.Messages(client).
		GetHistory(peer).OffsetID(id + 1).
		BatchSize(1).Iter()

	if !iterator.Next(ctx) {
		// Two things end the walk, and they could not be further apart: an empty
		// page, which says the message is gone, and a request that failed, which
		// says nothing about the message at all.
		//
		// Answering "deleted" for both is how a proxy that drops a connection
		// turned into a permanent verdict on a file. The account reaches Telegram
		// through a proxy here; a blip on that link made this return
		// ErrMessageDeleted, the download path classifies that as an answer no
		// later attempt can change, and the file was settled as failed with a
		// reason that was not true. Observed on the development instance: a
		// batch read refused with "retry limit reached", the per-message
		// fallback asked again, and the file was recorded as 消息已删除.
		if err := iterator.Err(); err != nil {
			return nil, err
		}
		// The page is empty, so the message is gone. This is returned explicitly:
		// wrapping a nil error still produces an error here, but one that carries
		// no cause, which reads downstream as an unexplained failure rather than
		// as a deleted message.
		return nil, ErrMessageDeleted
	}

	message, ok := iterator.Value().Msg.(*tg.Message)
	if !ok {
		return nil, fmt.Errorf("invalid message %d", id)
	}
	// Reading the page starting one id above the target lands on the target when
	// it exists. A different id means the target is missing and this is the
	// nearest older message instead.
	if message.GetID() != id {
		return nil, fmt.Errorf("the message %d/%d: %w", GetInputPeerID(peer), id, ErrMessageDeleted)
	}
	return message, nil
}

// GetGroupedMessages returns every message of the album one message belongs to,
// oldest first.
//
// An album is fetched by walking back from the given message, because the ids
// of an album's members are not derivable from the one id in hand. This is the
// expensive way to learn what an album contains, which is why callers that have
// already expanded the album should pass its members directly instead of asking
// the download engine to expand it again - see the Group field on the engine's
// Options.
func GetGroupedMessages(ctx context.Context, client *tg.Client, peer tg.InputPeerClass, message *tg.Message) ([]*tg.Message, error) {
	group, ok := message.GetGroupedID()
	if !ok {
		return nil, errors.New("not grouped message")
	}
	// An album holds up to ten photos or videos, and the walk back has to cover
	// them plus the gap to the next album.
	// https://telegram.org/blog/albums-saved-messages
	const batchSize = 20

	iterator := query.Messages(client).
		GetHistory(peer).
		OffsetID(message.ID + 11).
		BatchSize(batchSize).Iter()

	messages := make([]*tg.Message, 0, batchSize)
	for i := 0; iterator.Next(ctx) && i < batchSize; i++ {
		member, ok := iterator.Value().Msg.(*tg.Message)
		if !ok {
			continue
		}
		memberGroup, ok := member.GetGroupedID()
		if !ok || memberGroup != group {
			continue
		}
		// The message in hand is used rather than the one from the page, because
		// the caller's copy carries flags the page may not - a forward edit, for
		// instance.
		if member.ID == message.ID {
			messages = append(messages, message)
		} else {
			messages = append(messages, member)
		}
	}

	// A walk that stopped because the request failed found a prefix of the
	// album, and returning it would download part of an album while reporting
	// success - the file count is the only thing that would show it, and only to
	// someone who knew how many members to expect. The caller has to be able to
	// tell a short album from a refused request.
	if err := iterator.Err(); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("the album of message %d holds nothing readable", message.ID)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })
	return messages, nil
}

// GetInputPeerID returns the numeric id inside an input peer.
//
// It is used to name a dialog in an error message and to key a peer that has no
// username. Saved Messages have no id of their own and answer zero.
func GetInputPeerID(peer tg.InputPeerClass) int64 {
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return p.UserID
	case *tg.InputPeerChat:
		return p.ChatID
	case *tg.InputPeerChannel:
		return p.ChannelID
	}
	return 0
}

// GetPeerID returns the numeric id inside a peer, the answered form of what
// GetInputPeerID reads.
func GetPeerID(peer tg.PeerClass) int64 {
	switch p := peer.(type) {
	case *tg.PeerUser:
		return p.UserID
	case *tg.PeerChat:
		return p.ChatID
	case *tg.PeerChannel:
		return p.ChannelID
	}
	return 0
}

// threadsLevels caps how many parallel part requests one file is worth.
//
// A small file split across eight connections finishes no sooner than across
// one, and each connection is a request: the levels keep the parallelism - and
// so the request count - proportional to the file.
var threadsLevels = []struct {
	threads int
	size    int64
}{
	{1, 1 << 20},
	{2, 5 << 20},
	{4, 20 << 20},
	{8, 50 << 20},
}

// BestThreads reports how many parts of a file to fetch in parallel.
func BestThreads(size int64, max int) int {
	for _, level := range threadsLevels {
		if size < level.size {
			return min(level.threads, max)
		}
	}
	return max
}
