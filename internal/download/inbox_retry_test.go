package download

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The update dispatcher already told gotd the event was handled, so Telegram
// never redelivers it: a failed inbox write has to be retried in this process or
// it is lost. This covers the retry path actually recovering a transient error.
func TestInboxRetryQueueRecoversTransientFailure(t *testing.T) {
	m := &Manager{inboxRetry: make(chan inboxRetry, 4), events: newEventBus()}
	// The worker only attempts a write while the database is considered
	// available; these tests never touch a real database, so state it directly.
	m.dbHealthMu.Lock()
	m.dbHealth = DatabaseHealth{Status: "connected"}
	m.dbHealthMu.Unlock()

	// calls is only touched from the worker goroutine, so it needs no lock.
	calls := 0
	recovered := make(chan struct{})
	m.scheduleInboxRetry("probe", func() error {
		calls++
		if calls == 1 {
			return errors.New("transient failure")
		}
		close(recovered)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.inboxRetryWorker(ctx)

	waitForCondition(t, 20*time.Second, "the retry queue to recover the failed write", func() bool {
		select {
		case <-recovered:
			return true
		default:
			return false
		}
	})
}

// Producers are gotd dispatcher callbacks, so queueing a retry must never block
// them — a full queue drops the event loudly rather than stalling the update
// stream.
func TestInboxRetryQueueNeverBlocksItsProducer(t *testing.T) {
	m := &Manager{inboxRetry: make(chan inboxRetry, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := 0; index < 50; index++ {
			m.scheduleInboxRetry("overflow", func() error { return nil })
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduleInboxRetry blocked when the queue was full")
	}
}

// A Manager built without New has no queue. This asserts only that queueing
// still returns promptly; it does not observe the log line that reports the
// loss, so it must not claim to. (Verified by mutation: removing that log leaves
// this test passing.)
func TestInboxRetryWithoutQueueDoesNotBlockItsProducer(t *testing.T) {
	m := &Manager{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.scheduleInboxRetry("no-queue", func() error { return nil })
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduleInboxRetry blocked without a queue")
	}
}
