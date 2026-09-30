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
	// lowest is the smallest update id a getUpdates batch has returned since the
	// stored offset last moved, or 0 when no batch has been seen since.
	//
	// It is the walk's starting point, and it has to come from the batch rather
	// than from the stored offset. Telegram only returns updates at or above the
	// offset it was given, so an id below the smallest one actually delivered
	// does not exist and can never be finished - walking from it can only ever
	// stop on the same missing id. A fresh install stores offset 0, which no
	// update ever carries (update identifiers start from a positive number), so
	// without this seed the walk never took its first step and the cursor stayed
	// at 0 for the life of the process: nothing was ever confirmed, and the
	// unconfirmed backlog the server keeps redelivering grew without bound.
	lowest int64
}

func newUpdateDispatch() *updateDispatch {
	return &updateDispatch{running: map[int64]struct{}{}, finished: map[int64]struct{}{}}
}

// noteDelivered seeds the acknowledgement walk with an id the server confirmed
// exists. Only ids at or above the stored offset are meaningful; a lower one
// cannot advance anything and is ignored.
func (d *updateDispatch) noteDelivered(id int64) {
	if id <= 0 {
		return
	}
	d.mu.Lock()
	if d.lowest == 0 || id < d.lowest {
		d.lowest = id
	}
	d.mu.Unlock()
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

// acknowledgeable returns the offset that may be stored now, given the current
// one. Telegram's offset is one past the last confirmed update, so this walks
// forward over every consecutive finished update and stops at the first gap: an
// update that is still running, or one that failed and is being retried, holds
// the offset until it completes.
//
// It changes nothing. The finished records are pruned by committed, and only
// after the returned offset has been written durably - see that method for why
// the order matters. Returning after unchanged means "nothing new can be
// confirmed", which is also what the caller compares against, so a walk that
// started at the seed rather than at the stored offset does not by itself move
// the cursor past an update that has not been handled.
func (d *updateDispatch) acknowledgeable(after int64) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	start := after
	if d.lowest > start {
		start = d.lowest
	}
	walked := start
	for {
		if _, ok := d.finished[walked]; !ok {
			break
		}
		walked++
	}
	if walked == start {
		return after
	}
	return walked
}

// committed forgets the finished records the newly stored offset confirms. It
// runs only after the offset has been written to disk.
//
// Doing this inside the walk was a correctness bug in the other direction: the
// records were deleted before the write was attempted, so a write that failed
// (a full disk, a read-only directory) left the stored offset behind while the
// only record that those updates had already run was gone. The next poll
// redelivered them, claim succeeded, and the command ran a second time - the
// exact duplicate the finished set exists to prevent.
func (d *updateDispatch) committed(next int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lowest != 0 && d.lowest < next {
		// The seed is consumed: the next batch establishes a new one. Keeping a
		// stale, now-unreachable id would only pin the walk to it.
		d.lowest = 0
	}
	for id := range d.finished {
		if id < next {
			delete(d.finished, id)
		}
	}
}

// runningCount reports how many updates are being handled. It exists so a test
// can wait for the workers instead of sleeping.
func (d *updateDispatch) runningCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.running)
}
