// Package transfer downloads a list of message ids from one Telegram dialog.
//
// It is this application's own download engine. What it replaced was a
// command-line tool's downloader: it read its configuration from a process-wide
// flag registry, expanded albums a second time after the caller had already
// expanded them, fetched each message with a request of its own, and reported a
// transfer that failed as a transfer that succeeded. None of that was reachable
// from here to fix, which is why the engine is now ours.
//
// Three properties are worth stating, because they are why the code below has
// the shape it has:
//
//   - Reading messages is batched. A batch of N ids costs about N/100 requests
//     instead of N, which is the difference between pacing the account's
//     metadata traffic and being unable to.
//   - Every id stands on its own. Albums are expanded by the caller, which
//     already had to expand them to know what the user asked for, so this
//     package never re-reads history to discover an album's members.
//   - A transfer that failed is reported as a failure. The engine still lets
//     the rest of the batch proceed - one deleted file must not abandon the
//     other ninety-nine - but it counts the failure and logs it, instead of
//     renaming a half-written file and calling it complete.
package transfer

import (
	"context"
	"errors"
	"time"

	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/kv"
)

// Pool is what the engine needs from the account's connection pool.
//
// It does not close the pool: the pool belongs to the account's session and
// outlives any one transfer.
type Pool interface {
	Client(ctx context.Context, dc int) *tg.Client
	Default(ctx context.Context) *tg.Client
}

// Deps is what one transfer needs from the account it runs on.
type Deps struct {
	// Pool carries the file bytes and the metadata requests this engine makes.
	Pool Pool
	// KV backs the peer cache, so a dialog resolved once is not resolved again.
	KV kv.Storage
	// AccountID names the account in logs. It carries no authority: the pool and
	// the storage handed in here are already that account's.
	AccountID string
}

// MessageSource resolves one message of the batch. See batch.go for the
// implementation production uses.
type MessageSource interface {
	Message(ctx context.Context, id int) (*tg.Message, error)
}

// Refresher is a MessageSource that can read a message past its own cache.
//
// It exists for one error: Telegram hands out a file reference with a lifetime,
// and once it is spent the media cannot be fetched with it. The documented
// remedy is to go back to where the media was found and read it again - which
// is why every official client keeps a table mapping a file to its source, and
// why the source here is the message. A source that can answer this question
// lets the engine repair one file instead of failing it.
type Refresher interface {
	Refresh(ctx context.Context, id int) (*tg.Message, error)
}

// ProgressUpdate is one file's byte progress. Completed is set when the file
// reaches its media byte total.
type ProgressUpdate struct {
	DialogID   int64
	MessageID  int
	Downloaded int64
	Total      int64
	Completed  bool
}

// FileCompletedUpdate identifies a file that is safe to publish: fully written,
// closed, renamed out of its temporary name, and stamped with the message's
// date.
type FileCompletedUpdate struct {
	DialogID  int64
	MessageID int
	Path      string
}

// Options describes one batch.
type Options struct {
	// Dir is the working directory. Temporary files are created here and the
	// caller moves the finished ones out.
	Dir string
	// Peer is the dialog every id belongs to.
	Peer tg.InputPeerClass
	// Messages are the ids to fetch, in the order they should be handed out.
	// Every member of an album belongs here; see the package comment.
	Messages []int
	// Threads caps the parallel part requests within one file. BestThreads
	// lowers it further for small files.
	Threads int
	// Tasks caps how many files are transferred at once.
	Tasks int
	// Delay is a pause before each file after the first, for an account that
	// wants to stay well below what Telegram will tolerate.
	Delay time.Duration
	// Source overrides how messages are read. Nil builds the batched reader;
	// tests inject a fake.
	Source MessageSource
	// OnProgress receives byte progress.
	OnProgress func(ProgressUpdate)
	// OnFileCompleted receives each file as it becomes publishable. It runs on
	// a transfer worker, so it must return promptly.
	OnFileCompleted func(FileCompletedUpdate)
}

// Stats is what one batch cost.
//
// It exists because none of this was measurable before: a batch of three
// thousand files looked like a batch of three, and the per-message history read
// that made large batches slow was a request nobody counted.
type Stats struct {
	// Files is how many media files were handed to a transfer.
	Files int
	// Deleted is how many messages were gone before they could be read.
	Deleted int
	// Empty is how many messages held nothing downloadable.
	Empty int
	// Failed is how many transfers did not complete. The batch continues past
	// them; the caller decides what the task's outcome is.
	Failed int
	// BatchCalls and SingleCalls are the requests spent reading messages. A
	// healthy batch has SingleCalls at zero.
	BatchCalls  int
	SingleCalls int
	// Refreshed counts files whose file reference had expired and was renewed by
	// reading the message again. It is zero on an ordinary batch, and worth
	// seeing when it is not: it means the batch waited long enough between
	// reading and transferring for Telegram to retire the references.
	Refreshed int
}

// Run transfers every message in the batch and reports what it cost.
//
// A failure to read a message stops the batch: without that message there is
// nothing to decide about the ones after it. A failure to transfer one file
// does not - it is counted, logged, and the batch carries on.
func Run(ctx context.Context, deps Deps, opts Options) (Stats, error) {
	if opts.Peer == nil {
		return Stats{}, errors.New("下载批次缺少 Telegram 会话引用")
	}
	if len(opts.Messages) == 0 {
		return Stats{}, nil
	}

	source := opts.Source
	if source == nil {
		source = newBatchSource(deps.Pool, opts.Peer, opts.Messages)
	}

	engine := &engine{
		deps: deps,
		opts: opts,
		iter: &iterator{
			source:  source,
			manager: peers.Options{Storage: kv.NewPeers(deps.KV)}.Build(deps.Pool.Default(ctx)),
			peer:    opts.Peer,
			ids:     opts.Messages,
			delay:   opts.Delay,
			dir:     opts.Dir,
			elem:    make(chan *element, 1),
			seen:    make(map[int]struct{}, len(opts.Messages)),
		},
	}
	return engine.download(ctx)
}
