package telegram

import "testing"

// A connection that drops and comes back is invisible from inside this process,
// so the listeners have to be told.
//
// Telegram delivers an update once and never repeats it. gotd reconnects under
// the same client and the same authorization, without calling the Run callback
// again - so the listeners keep waiting on the update stream they started with,
// the messages published while the connection was down are never delivered, and
// nothing notices. This application drives a bare update dispatcher rather than
// the updates manager that would replay the difference, so the framework will
// not recover them either.
//
// What the listeners already have is the way to recover: the reconciliation
// they run when they attach closes the interval between each task's persisted
// watermark and the connection being ready. A reconnect is the same situation
// arriving a second time, and this is what says so.
//
// The wiring above this - tgclient forwarding OnSelfSuccess to gotd, and the
// session handing it noteReconnect - is not covered here. Driving it needs a
// live account and a connection that actually drops, which is the same limit
// that leaves the rest of the listener path covered by review rather than by
// tests.
func TestAReconnectTellsEverySubscriberAgain(t *testing.T) {
	session := &accountSession{readyCh: make(chan struct{}), readies: map[uint64]func(){}}
	calls := 0
	session.readies[1] = func() { calls++ }

	// The first connection. Self succeeding is what gotd reports first, and at
	// that moment the listeners have not been attached yet - markReady is about
	// to run them, and running them here as well would give every listener two
	// overlapping reconciliations of the same interval.
	session.noteReconnect()
	if calls != 0 {
		t.Fatalf("the first connection ran the ready callbacks %d times before markReady", calls)
	}

	for _, callback := range session.markReady(nil) {
		callback()
	}
	if calls != 1 {
		t.Fatalf("markReady ran the callbacks %d times, want 1", calls)
	}

	// The connection dropped and came back. Same client, same session, no new
	// Run callback - only this.
	session.noteReconnect()
	if calls != 2 {
		t.Fatalf("a reconnect ran the callbacks %d times, want 2: the messages published "+
			"while the connection was down are never delivered again, so the only recovery "+
			"is the one the callbacks perform", calls)
	}
}
