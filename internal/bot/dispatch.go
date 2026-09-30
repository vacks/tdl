package bot

import "sync"

// updateDispatch hands updates to command workers without letting one slow
// command stop the poll loop.
//
// The loop used to call the handler inline, so a link that took Telegram ninety
// seconds to resolve stopped getUpdates for that long: no other user's command
// was even fetched, and every update behind it in the same batch waited. A
// failure was worse: the loop stopped at the first one and left the rest of the
// batch behind its retry backoff.
//
// Two properties are preserved. The stored offset only ever moves past an update
// whose handling has completed, so a crash cannot lose a command; and a command
// is never handled twice, even though Telegram redelivers everything at or above
// that offset while an earlier update is still running.
type updateDispatch struct {
	mu       sync.Mutex
	running  map[int64]struct{}
	finished map[int64]struct{}
}

func newUpdateDispatch() *updateDispatch {
	return &updateDispatch{running: map[int64]struct{}{}, finished: map[int64]struct{}{}}
}

// claim reports whether this update should be handed to a worker now. An update
// already running, or already finished but not yet acknowledgeable because an
// earlier one has not completed, is not handled again.
func (d *updateDispatch) claim(id int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.running[id]; ok {
		return false
	}
	if _, ok := d.finished[id]; ok {
		return false
	}
	d.running[id] = struct{}{}
	return true
}

// release undoes a claim taken for an update that was not actually dispatched,
// so a later getUpdates can offer it again.
func (d *updateDispatch) release(id int64) {
	d.mu.Lock()
	delete(d.running, id)
	d.mu.Unlock()
}

// finish records a handled update. The record stays until the offset passes it,
// so a redelivery in the meantime is not handled a second time.
func (d *updateDispatch) finish(id int64) {
	d.mu.Lock()
	delete(d.running, id)
	d.finished[id] = struct{}{}
	d.mu.Unlock()
}

// acknowledge returns the offset to store, given the current one. Telegram's
// offset is one past the last confirmed update, so this walks forward over every
// consecutive finished update and stops at the first gap: an update that is
// still running, or one that failed and is being retried, holds the offset until
// it completes.
func (d *updateDispatch) acknowledge(after int64) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		if _, ok := d.finished[after]; !ok {
			return after
		}
		delete(d.finished, after)
		after++
	}
}

// runningCount reports how many updates are being handled. It exists so a test
// can wait for the workers instead of sleeping.
func (d *updateDispatch) runningCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.running)
}
