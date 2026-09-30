package bot

import "testing"

// The offset decides what Telegram redelivers, so both of its failure modes are
// user-visible: advancing past a command that has not run loses it, and failing
// to advance past a finished one replays it. This pins the rule that only a
// consecutive run of finished updates moves the offset.
func TestUpdateDispatchAcknowledgesOnlyConsecutiveFinishedUpdates(t *testing.T) {
	d := newUpdateDispatch()
	if got := d.acknowledge(10); got != 10 {
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
	if got := d.acknowledge(10); got != 10 {
		t.Fatalf("offset moved to %d past an unfinished update; want 10", got)
	}
	// A redelivery of the finished 11 must not run it a second time.
	if d.claim(11) {
		t.Fatal("a finished update was claimable again before it was acknowledged")
	}
	d.finish(10)
	if got := d.acknowledge(10); got != 12 {
		t.Fatalf("offset = %d after both finished; want 12", got)
	}
	// An acknowledged update is forgotten, so the bookkeeping cannot grow while
	// the offset advances. (A redelivery of it is harmless: the poll loop skips
	// anything below the stored offset before claiming.)
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
	if got := d.acknowledge(7); got != 8 {
		t.Fatalf("offset = %d after release, finish and acknowledge; want 8", got)
	}
}

// An update still running holds everything behind it, which is what makes the
// crash window safe: the offset is written only for work that completed.
func TestUpdateDispatchKeepsRunningUpdatesOpen(t *testing.T) {
	d := newUpdateDispatch()
	d.claim(3)
	if got := d.acknowledge(3); got != 3 {
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
