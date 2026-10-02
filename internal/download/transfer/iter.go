package transfer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/tmedia"
	"github.com/vacks/tdl/internal/tmsg"
)

// element is one file on its way to disk.
type element struct {
	dialogID int64
	message  *tg.Message
	media    *tmedia.Media
	to       *os.File
}

// iterator turns the batch's ids into elements for the transfer workers.
//
// It produces one element at a time and waits to be asked for the next, so the
// message read for the file after this one happens while this one is being
// transferred. That is enough now that the reads are batched: a hundred files
// cost one request, not a hundred.
type iterator struct {
	source  MessageSource
	manager *peers.Manager
	peer    tg.InputPeerClass
	ids     []int
	delay   time.Duration
	dir     string
	elem    chan *element

	index int
	seen  map[int]struct{}
	err   error

	// from is the resolved dialog, looked up once. It carries the display name
	// the callbacks report and costs a request if the peer is not cached yet,
	// which is a per-task cost rather than a per-file one.
	from   peers.Peer
	fromAt bool

	deleted int
	empty   int
}

// Next reports whether another element is ready, resolving messages as it goes.
//
// A message that is gone or holds nothing downloadable is skipped without
// failing the batch: both are ordinary in a task assembled minutes ago, and the
// caller's items were built from what the scan saw at the time.
func (i *iterator) Next(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		i.err = ctx.Err()
		return false
	default:
	}

	if len(i.elem) > 0 {
		return true
	}
	if i.delay > 0 && i.index > 0 {
		select {
		case <-ctx.Done():
			i.err = ctx.Err()
			return false
		case <-time.After(i.delay):
		}
	}

	for i.index < len(i.ids) {
		ready, skip := i.process(ctx)
		if skip {
			continue
		}
		return ready
	}
	return false
}

func (i *iterator) process(ctx context.Context) (ready bool, skip bool) {
	id := i.ids[i.index]
	i.index++
	if _, duplicate := i.seen[id]; duplicate {
		return false, true
	}
	i.seen[id] = struct{}{}

	message, err := i.source.Message(ctx, id)
	if err != nil {
		if errors.Is(err, tmsg.ErrMessageDeleted) {
			i.deleted++
			return false, true
		}
		i.err = fmt.Errorf("读取消息 %d: %w", id, err)
		return false, false
	}

	media, ok := tmedia.GetMedia(message)
	if !ok {
		// No media is not a failure - most messages in a channel are text.
		i.empty++
		return false, true
	}

	from, err := i.dialog(ctx)
	if err != nil {
		i.err = fmt.Errorf("解析会话: %w", err)
		return false, false
	}

	to, err := createTempFile(i.dir, message.ID, media.Name)
	if err != nil {
		i.err = err
		return false, false
	}

	i.elem <- &element{dialogID: from.ID(), message: message, media: media, to: to}
	return true, false
}

// Value returns the element produced by the last Next.
func (i *iterator) Value() *element {
	return <-i.elem
}

// Err reports why iteration stopped, if it stopped for a reason.
func (i *iterator) Err() error {
	return i.err
}

func (i *iterator) dialog(ctx context.Context) (peers.Peer, error) {
	if i.fromAt {
		return i.from, nil
	}
	from, err := i.manager.FromInputPeer(ctx, i.peer)
	if err != nil {
		return nil, err
	}
	i.from, i.fromAt = from, true
	return from, nil
}

// createTempFile opens the file a transfer writes into.
//
// The name carries the message id so two files called the same thing cannot
// collide, and the media's own name so an interrupted working directory is
// readable. The file is created before the transfer starts, which is what makes
// the presence of a `.tmp` file mean "a transfer was going to happen here".
func createTempFile(dir string, messageID int, name string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建下载临时目录: %w", err)
	}
	path := filepath.Join(dir, tempName(messageID, name))
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("创建下载临时文件: %w", err)
	}
	return file, nil
}

// reportOutcome logs what a batch turned out to contain.
//
// Neither count is an error: a post deleted between being indexed and being
// read, and a message that never held media, are both ordinary. But they are
// the shape of a task that downloads fewer files than it lists, and saying so
// once is what makes that outcome readable afterwards instead of mysterious.
func (i *iterator) reportOutcome(accountID string, peer tg.InputPeerClass) {
	if i.deleted == 0 && i.empty == 0 {
		return
	}
	applog.Info("download", "batch_read_outcome", "account_id", accountID,
		"peer_id", tmsg.GetInputPeerID(peer), "requested", len(i.ids),
		"deleted", i.deleted, "without_media", i.empty)
}
