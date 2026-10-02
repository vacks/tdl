package transfer

import (
	"context"
	"sync"
	"time"

	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/tmsg"
)

const (
	// batchSize is how many ids one request asks for.
	//
	// Telegram accepts more - clients pass up to 200 - but the response carries
	// a whole message object per id, entities and media included, and this is
	// read on the connection that also carries the account's updates. A hundred
	// keeps the response a couple of hundred kilobytes at worst.
	batchSize = 100

	// batchTTL is how long a fetched window stays usable.
	//
	// A window is fetched when its first id is wanted, but the ids in it are
	// handed out one file at a time, so a slow transfer can leave the hundredth
	// id unused for a long while. That matters because a message carries a file
	// reference, and a file reference expires. Three minutes is far below the
	// shortest lifetime Telegram documents and far above how long a hundred
	// small files take; a window that ages out is re-read, which costs one
	// request - never one per file.
	batchTTL = 3 * time.Minute
)

type cachedMessage struct {
	message *tg.Message
	// missing records a message the server did not return. It is cached so a
	// batch with a deleted post does not re-ask for it every window.
	missing bool
	fetched time.Time
}

// batchSource reads messages by id, a window at a time.
//
// The request it replaces was one messages.getHistory per message - a full
// round trip, serially, on the critical path of every single file. For a
// session task holding thousands of files that is thousands of round trips
// before any of them can be transferred, and it is the reason the account's
// metadata traffic could not be paced: at four requests a second the budget
// would have defined the download's throughput.
type batchSource struct {
	pool  Pool
	peer  tg.InputPeerClass
	ids   []int
	index map[int]int

	// degraded is set when a batch request fails. The source then reads one
	// message at a time for the rest of the run, which is exactly what the
	// previous implementation did, so a failure here can only cost speed.
	degraded bool

	// ttl is how long a fetched window stays usable. It is a field so a test
	// can age a window out without waiting three minutes; production uses
	// batchTTL.
	ttl time.Duration

	mu     sync.Mutex
	cached map[int]cachedMessage
	stats  batchStats
}

type batchStats struct {
	batchCalls  int
	singleCalls int
}

func newBatchSource(pool Pool, peer tg.InputPeerClass, ids []int) *batchSource {
	index := make(map[int]int, len(ids))
	for position, id := range ids {
		// A repeated id keeps its first position: the caller hands them out in
		// order, and re-positioning one would move the window backwards.
		if _, exists := index[id]; !exists {
			index[id] = position
		}
	}
	return &batchSource{pool: pool, peer: peer, ids: ids, index: index, cached: map[int]cachedMessage{}, ttl: batchTTL}
}

// Message returns one message, reading a whole window of them if it has to.
func (s *batchSource) Message(ctx context.Context, id int) (*tg.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.cached[id]; ok {
		// The age is judged when the message is handed out, not when the window
		// was fetched: that is the moment its file reference has to still be
		// good for.
		if time.Since(entry.fetched) < s.ttl {
			return s.entry(entry)
		}
	}

	if s.degraded {
		s.stats.singleCalls++
		return tmsg.GetSingleMessage(ctx, s.pool.Default(ctx), s.peer, id)
	}

	position, known := s.index[id]
	if !known {
		// An id this source was not built for. Falling back keeps the semantics
		// identical to the per-message reader instead of guessing.
		s.stats.singleCalls++
		return tmsg.GetSingleMessage(ctx, s.pool.Default(ctx), s.peer, id)
	}

	if err := s.loadWindow(ctx, position); err != nil {
		s.stats.singleCalls++
		return tmsg.GetSingleMessage(ctx, s.pool.Default(ctx), s.peer, id)
	}
	if entry, ok := s.cached[id]; ok {
		return s.entry(entry)
	}
	// The window was re-read and this id is still not in it, which means the
	// server did not return it. That is the deleted case.
	return nil, tmsg.ErrMessageDeleted
}

// Refresh reads one message past the cache.
//
// It is the repair for an expired file reference: the media this batch holds
// was read up to TTL ago - or, for a task that waited in the queue, much
// longer - and Telegram will not serve a file with a spent reference. Reading
// the message again is the whole fix, and it is one request rather than the
// task-level retry it replaces, which re-reads every message of the task and
// costs a round of database writes besides.
//
// It is deliberately the single-message read and not a window: the point is to
// get a reference that is new *now*, and a window would hand out the same
// mixture of ages the cache already holds.
func (s *batchSource) Refresh(ctx context.Context, id int) (*tg.Message, error) {
	message, err := tmsg.GetSingleMessage(ctx, s.pool.Default(ctx), s.peer, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cached[id] = cachedMessage{message: message, fetched: time.Now()}
	s.stats.singleCalls++
	s.mu.Unlock()
	return message, nil
}

func (s *batchSource) entry(entry cachedMessage) (*tg.Message, error) {
	if entry.missing {
		return nil, tmsg.ErrMessageDeleted
	}
	return entry.message, nil
}

// loadWindow reads the ids at and after position, and caches them.
func (s *batchSource) loadWindow(ctx context.Context, position int) error {
	end := position + batchSize
	if end > len(s.ids) {
		end = len(s.ids)
	}
	window := s.ids[position:end]
	if len(window) == 0 {
		return nil
	}

	messages, err := s.fetch(ctx, window)
	if err != nil {
		// Once is enough. A batch request that failed - the channel is not
		// reachable, the ids are refused, the connection dropped - will most
		// likely fail again, and retrying it per window would spend more
		// requests than the per-message path it replaced.
		s.degraded = true
		applog.Warn("download", "message_batch_read_failed",
			"peer_id", tmsg.GetInputPeerID(s.peer), "ids", len(window), "error", err.Error())
		return err
	}

	s.stats.batchCalls++
	now := time.Now()
	// The cache is replaced rather than merged: everything in it is older than
	// the window just read, and the ids before this position are already done
	// with.
	s.cached = make(map[int]cachedMessage, len(window))
	for _, id := range window {
		s.cached[id] = cachedMessage{missing: true, fetched: now}
	}
	for _, message := range messages {
		s.cached[message.ID] = cachedMessage{message: message, fetched: now}
	}
	return nil
}

// fetch asks for the whole window in one request.
//
// A channel's messages are addressed through channels.getMessages with the
// channel itself; every other dialog resolves the bare ids, because for those
// the ids come from the account's own message sequence. Saved Messages are in
// the second group, which is why they need no special case.
func (s *batchSource) fetch(ctx context.Context, ids []int) ([]*tg.Message, error) {
	input := make([]tg.InputMessageClass, 0, len(ids))
	for _, id := range ids {
		input = append(input, &tg.InputMessageID{ID: id})
	}

	var (
		result tg.MessagesMessagesClass
		err    error
	)
	client := s.pool.Default(ctx)
	if channel, ok := s.peer.(*tg.InputPeerChannel); ok {
		result, err = client.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash},
			ID:      input,
		})
	} else {
		result, err = client.MessagesGetMessages(ctx, input)
	}
	if err != nil {
		return nil, err
	}

	messages := make([]*tg.Message, 0, len(ids))
	for _, raw := range messagesOf(result) {
		if message, ok := raw.(*tg.Message); ok {
			messages = append(messages, message)
		}
		// A MessageEmpty entry is a message that is not there. It is left out
		// here and recorded as missing by the caller, which turns into the same
		// "deleted" answer the per-message reader gives.
	}
	return messages, nil
}

// messagesOf unwraps the three containers Telegram answers with.
func messagesOf(result tg.MessagesMessagesClass) []tg.MessageClass {
	switch value := result.(type) {
	case *tg.MessagesMessages:
		return value.Messages
	case *tg.MessagesMessagesSlice:
		return value.Messages
	case *tg.MessagesChannelMessages:
		return value.Messages
	default:
		return nil
	}
}

// counts reports the requests this source spent.
func (s *batchSource) counts() batchStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
