package bot

import "testing"

// The offset decides what Telegram redelivers, so both of its failure modes are
// user-visible: advancing past a command that has not run loses it, and failing
// to advance past a finished one replays it. This pins the rule that only a
// consecutive run of finished updates moves the offset.
func TestUpdateDispatchAcknowledgesOnlyConsecutiveFinishedUpdates(t *testing.T) {
	d := newUpdateDispatch()
	d.noteDelivered(10)
	if got := d.acknowledgeable(10); got != 10 {
		t.Fatalf("an idle dispatch moved the offset to %d; want it left at 10", got)
	}
	if !d.claim(10) {
		t.Fatal("a fresh update was not claimable")
	}
	if !d.claim(11) {
		t.Fatal("a second fresh update was not claimable")
	}
	d.finish(11)
	// 11 finished while 10 has not, so the offset cannot pass 10: doing so
	// would confirm 10 as handled and it would never be delivered again.
	if got := d.acknowledgeable(10); got != 10 {
		t.Fatalf("offset moved to %d past an unfinished update; want 10", got)
	}
	// A redelivery of the finished 11 must not run it a second time.
	if d.claim(11) {
		t.Fatal("a finished update was claimable again before it was acknowledged")
	}
	d.finish(10)
	if got := d.acknowledgeable(10); got != 12 {
		t.Fatalf("offset = %d after both finished; want 12", got)
	}
	// Only the durable write of that offset makes the records disposable.
	d.committed(12)
	if len(d.finished) != 0 {
		t.Fatalf("%d acknowledged updates are still recorded as finished", len(d.finished))
	}
}

// A claim taken for an update that was never dispatched has to be undoable, or
// the worker pool being full would silently drop the command.
func TestUpdateDispatchReleaseReturnsAnUndispatchedUpdate(t *testing.T) {
	d := newUpdateDispatch()
	if !d.claim(7) {
		t.Fatal("a fresh update was not claimable")
	}
	if d.claim(7) {
		t.Fatal("a claimed update was claimable twice")
	}
	d.release(7)
	if !d.claim(7) {
		t.Fatal("a released update was not claimable again; the command would be lost")
	}
	d.finish(7)
	if got := d.acknowledgeable(7); got != 8 {
		t.Fatalf("offset = %d after release, finish and acknowledge; want 8", got)
	}
}

// An update still running holds everything behind it, which is what makes the
// crash window safe: the offset is written only for work that completed.
func TestUpdateDispatchKeepsRunningUpdatesOpen(t *testing.T) {
	d := newUpdateDispatch()
	d.claim(3)
	if got := d.acknowledgeable(3); got != 3 {
		t.Fatalf("offset = %d while an update is running; want 3", got)
	}
	if running := d.runningCount(); running != 1 {
		t.Fatalf("runningCount() = %d; want 1", running)
	}
	d.finish(3)
	if running := d.runningCount(); running != 0 {
		t.Fatalf("runningCount() after finish = %d; want 0", running)
	}
}

// A fresh install stores offset 0. Telegram's update identifiers start from a
// positive number, so no update ever carries id 0 and a walk that starts there
// can never take a step: the offset stayed at 0 for the life of the process,
// nothing was ever confirmed, and the unconfirmed backlog the server kept
// redelivering grew until it stopped returning new commands at all.
func TestUpdateDispatchAdvancesFromAZeroOffset(t *testing.T) {
	d := newUpdateDispatch()
	// The first batch a new Bot ever sees. Ids are positive and not necessarily 1.
	d.noteDelivered(100)
	if !d.claim(100) {
		t.Fatal("the first delivered update was not claimable")
	}
	d.finish(100)
	if got := d.acknowledgeable(0); got != 101 {
		t.Fatalf("a fresh install's cursor moved to %d; want 101. The walk is still anchored to the stored offset instead of the batch", got)
	}
}

// The seed comes from the batch, but it must not let the offset jump over an
// update that was delivered and has not been handled.
func TestUpdateDispatchDoesNotSkipAnUnhandledSeed(t *testing.T) {
	d := newUpdateDispatch()
	d.noteDelivered(100)
	d.noteDelivered(101)
	if !d.claim(100) {
		t.Fatal("100 was not claimable")
	}
	if !d.claim(101) {
		t.Fatal("101 was not claimable")
	}
	d.finish(101)
	// 100 is still running, so the offset cannot move even though the walk now
	// starts at 100 rather than at the stored 0.
	if got := d.acknowledgeable(0); got != 0 {
		t.Fatalf("offset moved to %d past a running update; want it left at 0", got)
	}
	d.finish(100)
	if got := d.acknowledgeable(0); got != 102 {
		t.Fatalf("offset = %d once both finished; want 102", got)
	}
}

// The finished records are what stop a redelivery from running twice, so they
// outlive the walk. Only the durable write of the new offset retires them.
func TestUpdateDispatchKeepsFinishedRecordsUntilTheOffsetIsStored(t *testing.T) {
	d := newUpdateDispatch()
	d.noteDelivered(5)
	d.claim(5)
	d.finish(5)
	next := d.acknowledgeable(0)
	if next != 6 {
		t.Fatalf("acknowledgeable = %d; want 6", next)
	}
	// The caller writes the cursor file here. Until that write succeeds the
	// record has to survive, or a failed write would redeliver the update and run
	// the command a second time.
	if d.claim(5) {
		t.Fatal("a finished update was claimable before the offset was stored")
	}
	d.committed(next)
	if len(d.finished) != 0 {
		t.Fatalf("%d finished records survived the stored offset", len(d.finished))
	}
	// The seed is consumed with them, so the next batch establishes its own.
	if d.lowest != 0 {
		t.Fatalf("lowest = %d after committing the offset; want it reset", d.lowest)
	}
}
