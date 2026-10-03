package transfer

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/sync/errgroup"

	"github.com/vacks/tdl/internal/tmedia"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/tmsg"
)

// maxPartSize is Telegram's part size for upload.getFile, and the unit the
// progress callback counts in.
//
// One megabyte, which is the largest a single request may ask for and what this
// has always used. The official clients ask for 128KB instead, and the
// difference was measured rather than assumed: on the same account, network and
// file, 128KB downloaded a 345MB file in 588 seconds against 40 for 1MB, and a
// 74MB file in 167 against 40. Both sizes completed without a single transfer
// error either way, so the smaller requests buy nothing here - the official
// clients use them for streaming, where a 128KB granularity is what a player
// can start on, and pay for it with a connection manager that re-issues a
// request after a reconnect and a file they can resume from any offset. This
// engine wants whole files, and a request that is retried from the part it was
// on (see the pool's middlewares in internal/telegram) does not need the
// smaller unit to survive a connection that drops.
const maxPartSize = 1024 * 1024

// engine runs one batch: it pulls elements from the iterator and hands each to
// a transfer worker.
type engine struct {
	deps Deps
	opts Options
	iter *iterator

	files     atomic.Int64
	failed    atomic.Int64
	refreshed atomic.Int64
}

// download runs the batch to completion.
//
// A worker never returns an error, because errgroup cancels its context when
// one does, and that would abandon the files still queued behind it: one post
// deleted mid-task must not cost the other ninety-nine. A failed transfer is
// counted and logged instead, and the caller decides the task's outcome from
// the per-file states it keeps.
func (e *engine) download(ctx context.Context) (Stats, error) {
	// Cancellable on its own so an iterator failure can stop the workers too,
	// rather than leaving them transferring into a directory the caller is
	// about to remove.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(max(1, e.opts.Tasks))
	for e.iter.Next(groupCtx) {
		item := e.iter.Value()
		group.Go(func() error {
			e.transfer(groupCtx, item)
			return nil
		})
	}

	if err := e.iter.Err(); err != nil {
		cancel()
		_ = group.Wait()
		return e.stats(), err
	}
	if err := group.Wait(); err != nil {
		return e.stats(), err
	}

	e.iter.reportOutcome(e.deps.AccountID, e.opts.Peer)
	return e.stats(), nil
}

func (e *engine) stats() Stats {
	counts := batchStats{}
	if source, ok := e.iter.source.(*batchSource); ok {
		counts = source.counts()
	}
	return Stats{
		Files:       int(e.files.Load()),
		Deleted:     e.iter.deleted,
		Empty:       e.iter.empty,
		Failed:      int(e.failed.Load()),
		Refreshed:   int(e.refreshed.Load()),
		BatchCalls:  counts.batchCalls,
		SingleCalls: counts.singleCalls,
	}
}

// transfer moves one file and makes it publishable.
func (e *engine) transfer(ctx context.Context, item *element) {
	// Closed whatever happens: a worker that leaves the file open holds a
	// descriptor until the process ends.
	defer item.to.Close()

	e.files.Add(1)
	// Reported before the first byte so the file counts as started the moment
	// work begins on it, rather than only once a chunk has landed.
	e.progress(item, 0)

	err := e.fetch(ctx, item)
	if err != nil && isStaleReference(err) && e.refresh(ctx, item) {
		// Telegram expires file references. The remedy its own clients use is to
		// read the message again and retry - they keep a table mapping a file to
		// where it was found for exactly this. Repairing the one file here costs
		// a single read; the alternative is failing it, which returns the task to
		// the queue and re-reads every message of it on the next attempt.
		//
		// A fresh writer, because the retry starts at offset zero and the byte
		// counter would otherwise report the first attempt's bytes a second time.
		e.progress(item, 0)
		err = e.fetch(ctx, item)
	}
	if err != nil {
		// A transfer the caller stopped is not a transfer that failed. Pause,
		// cancel, a database outage and shutdown all arrive here as a context
		// error, and reporting them would mark every in-flight file of a paused
		// task as failed - overwriting the pause the caller is in the middle of
		// writing. Whatever stopped the batch owns those rows.
		if ctx.Err() != nil {
			return
		}
		e.failed.Add(1)
		// The partial file is removed rather than left in place. The engine this
		// replaced renamed it out of its temporary name and reported success,
		// which is how a task could show a completed file of zero bytes.
		if err := os.Remove(item.to.Name()); err != nil && !os.IsNotExist(err) {
			applog.Warn("download", "partial_file_not_removed", "path", item.to.Name(), "error", err.Error())
		}
		applog.Error("download", "transfer_failed", "account_id", e.deps.AccountID,
			"dialog_id", item.dialogID, "message_id", item.message.ID,
			"name", item.media.Name, "size", item.media.Size, "error", err.Error())
		e.outcome(item, err)
		return
	}

	path, err := finalize(item)
	if err != nil {
		e.failed.Add(1)
		// finalize renames the working file before it stamps it, so a failure
		// here can leave the bytes under either name. Neither is publishable -
		// the caller never heard about it - and leaving them is what fills a
		// working directory with files a person would mistake for downloads.
		e.discard(item)
		applog.Error("download", "transfer_finalize_failed", "account_id", e.deps.AccountID,
			"dialog_id", item.dialogID, "message_id", item.message.ID, "error", err.Error())
		e.outcome(item, err)
		return
	}

	if e.opts.OnFileCompleted != nil {
		e.opts.OnFileCompleted(FileCompletedUpdate{
			DialogID:  item.dialogID,
			MessageID: item.message.ID,
			Path:      path,
		})
	}
}

// outcome reports that one message of the batch produced no file.
func (e *engine) outcome(item *element, err error) {
	if e.opts.OnFileOutcome == nil {
		return
	}
	e.opts.OnFileOutcome(FileOutcomeUpdate{MessageID: item.message.ID, Err: err})
}

// discard removes whatever a failed finalize left on disk, under either of the
// two names it can hold.
func (e *engine) discard(item *element) {
	for _, path := range []string{item.to.Name(), strings.TrimSuffix(item.to.Name(), tempExt)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			applog.Warn("download", "working_file_not_removed", "path", path, "error", err.Error())
		}
	}
}

// isStaleReference reports whether Telegram refused because the file reference
// has to be renewed.
//
// The three spellings are the states a spent reference can be in: expired,
// invalid, and empty - an empty one is what a message whose media was never
// fully populated carries. All three are cured the same way, by reading the
// message again.
func isStaleReference(err error) bool {
	return tgerr.Is(err, tg.ErrFileReferenceExpired, tg.ErrFileReferenceInvalid, tg.ErrFileReferenceEmpty)
}

// refresh re-reads the message a file came from, so the transfer has a
// reference Telegram will accept.
//
// A source that cannot read past its cache leaves the error alone: the transfer
// fails and the task's own retry picks it up, which is what happened before this
// existed.
func (e *engine) refresh(ctx context.Context, item *element) bool {
	source, ok := e.iter.source.(Refresher)
	if !ok {
		return false
	}
	message, err := source.Refresh(ctx, item.message.ID)
	if err != nil {
		applog.Warn("download", "message_refresh_failed", "account_id", e.deps.AccountID,
			"dialog_id", item.dialogID, "message_id", item.message.ID, "error", err.Error())
		return false
	}
	media, ok := tmedia.GetMedia(message)
	if !ok {
		// The media is gone from the message between the two reads. There is no
		// reference to renew because there is nothing to fetch.
		return false
	}
	item.message, item.media = message, media
	e.refreshed.Add(1)
	applog.Info("download", "stale_reference_repaired", "account_id", e.deps.AccountID,
		"dialog_id", item.dialogID, "message_id", message.ID)
	return true
}

// fetch pulls the bytes onto disk.
func (e *engine) fetch(ctx context.Context, item *element) error {
	client := e.deps.Pool.Client(ctx, item.media.DC)
	_, err := downloader.NewDownloader().
		WithPartSize(maxPartSize).
		Download(client, item.media.InputFileLoc).
		WithThreads(tmsg.BestThreads(item.media.Size, e.opts.Threads)).
		Parallel(ctx, &progressWriter{item: item, engine: e})
	return err
}

func (e *engine) progress(item *element, downloaded int64) {
	if e.opts.OnProgress == nil {
		return
	}
	e.opts.OnProgress(ProgressUpdate{
		DialogID:   item.dialogID,
		MessageID:  item.message.ID,
		Downloaded: downloaded,
		Total:      item.media.Size,
		Completed:  item.media.Size > 0 && downloaded >= item.media.Size,
	})
}

// progressWriter reports progress as parts land.
//
// It does not sleep between parts. The implementation it replaces paused two
// hundred milliseconds whenever a write was smaller than a part - which on a
// file under a megabyte is every file - so that a terminal progress bar had
// time to redraw. Nothing here renders a progress bar.
type progressWriter struct {
	item   *element
	engine *engine
	total  atomic.Int64
}

func (w *progressWriter) WriteAt(p []byte, off int64) (int, error) {
	written, err := w.item.to.WriteAt(p, off)
	if err != nil {
		return written, err
	}
	w.engine.progress(w.item, w.total.Add(int64(written)))
	return written, nil
}

// finalize turns the working file into the one the caller publishes.
func finalize(item *element) (string, error) {
	working := item.to.Name()
	path := strings.TrimSuffix(working, tempExt)
	if err := os.Rename(working, path); err != nil {
		return "", err
	}
	// The file is stamped with the message's date, so a downloaded archive
	// sorts by when it was posted rather than by when it was fetched.
	if item.media.Date > 0 {
		stamp := time.Unix(item.media.Date, 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			return "", err
		}
	}
	return filepath.Clean(path), nil
}

const (
	tempExt = ".tmp"
	// maxNameBytes keeps one path component inside the 255 bytes a filesystem
	// allows, leaving room for the message id, the separator and the extension.
	maxNameBytes = 180
)

// tempName builds the working file's name.
//
// It is not the published name - the caller chooses that from its own template
// - so it only has to be unique and safe: the message id makes it unique, and
// the sanitiser makes it one path component a filesystem will accept.
func tempName(messageID int, name string) string {
	return strconv.Itoa(messageID) + "_" + sanitizeName(name) + tempExt
}

// sanitizeName replaces what a filesystem refuses and bounds the length.
//
// Control characters, invalid UTF-8 and the separators and reserved characters
// of the common filesystems become underscores. A trailing dot or space is
// dropped, because some filesystems drop them silently and a name that reads
// differently than it was written is worse than one that was trimmed.
func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r == utf8.RuneError, r < 0x20, r == 0x7f, unicode.IsControl(r), strings.ContainsRune(`/\:*?"<>|`, r):
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	out := strings.Trim(b.String(), ". ")
	if out == "" {
		out = "file"
	}
	for len(out) > maxNameBytes {
		_, size := utf8.DecodeLastRuneInString(out)
		out = out[:len(out)-size]
	}
	return strings.TrimRight(out, ". ")
}
