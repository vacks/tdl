package tgclient

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tgerr"
)

// busyInvoker answers with one scripted error per call, then succeeds.
type busyInvoker struct {
	mu    sync.Mutex
	errs  []error
	calls int
}

func (b *busyInvoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	index := b.calls
	b.calls++
	if index < len(b.errs) {
		return b.errs[index]
	}
	return nil
}

func (b *busyInvoker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// The resends of a busy server are spaced, and the spacing grows.
//
// Every error this middleware retries means the server has just said it has no
// capacity. The loop used to resend with no wait at all, so one call became five
// requests back to back, and every file in flight did the same at the same
// moment: the burst arrived at a server that had already reported being
// overloaded, which is the one response guaranteed to make it worse.
func TestABusyServerIsRetriedWithGrowingWaits(t *testing.T) {
	busy := &tgerr.Error{Code: 500, Message: "RPC_CALL_FAIL", Type: "RPC_CALL_FAIL"}
	inner := &busyInvoker{errs: []error{busy, busy, busy}}
	var waits []time.Duration
	middleware := retryMiddleware{
		max: 5,
		sleep: func(_ context.Context, d time.Duration) error {
			waits = append(waits, d)
			return nil
		},
	}

	if err := middleware.Handle(inner)(context.Background(), nil, nil); err != nil {
		t.Fatalf("Handle() = %v, want the fourth attempt to succeed", err)
	}
	if got := inner.count(); got != 4 {
		t.Fatalf("the request was sent %d times, want 4 (three refusals, then success)", got)
	}
	want := []time.Duration{retryBackoffBase, 2 * retryBackoffBase, 4 * retryBackoffBase}
	if len(waits) != len(want) {
		t.Fatalf("waited %v before the resends, want %v", waits, want)
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("wait %d was %s, want %s (full sequence %v)", i, waits[i], want[i], waits)
		}
	}
}

// The wait is capped, so a server that stays busy is not waited on for minutes
// by a middleware that only ever gets five attempts.
func TestTheRetryWaitIsCapped(t *testing.T) {
	busy := &tgerr.Error{Code: 500, Message: "WORKER_BUSY_TOO_LONG_RETRY", Type: "WORKER_BUSY_TOO_LONG_RETRY"}
	// One refusal per attempt the middleware is allowed, so every one of them
	// happens and none of the waits can be skipped by a lucky answer.
	inner := &busyInvoker{errs: []error{busy, busy, busy, busy, busy, busy, busy, busy}}
	var longest time.Duration
	middleware := retryMiddleware{
		max: 8,
		sleep: func(_ context.Context, d time.Duration) error {
			if d > longest {
				longest = d
			}
			return nil
		},
	}

	if err := middleware.Handle(inner)(context.Background(), nil, nil); err == nil {
		t.Fatal("Handle() succeeded against a server that never answered")
	}
	if longest != retryBackoffMax {
		t.Fatalf("longest wait was %s, want the %s cap", longest, retryBackoffMax)
	}
}

// An error Telegram did not classify as its own problem is not retried, and
// keeps the prefix the rest of the application matches on.
func TestAnAnsweredErrorIsNotRetried(t *testing.T) {
	refused := &tgerr.Error{Code: 400, Message: "FILE_REFERENCE_EXPIRED", Type: "FILE_REFERENCE_EXPIRED"}
	inner := &busyInvoker{errs: []error{refused}}
	slept := false
	middleware := retryMiddleware{
		max: 5,
		sleep: func(context.Context, time.Duration) error {
			slept = true
			return nil
		},
	}

	err := middleware.Handle(inner)(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("Handle() succeeded on an answered error")
	}
	if inner.count() != 1 {
		t.Fatalf("the request was sent %d times, want 1", inner.count())
	}
	if slept {
		t.Fatal("an answered error was waiting to be retried")
	}
	if !tgerr.Is(errors.Unwrap(err), "FILE_REFERENCE_EXPIRED") {
		t.Fatalf("Handle() = %v, want the original error wrapped so its type survives", err)
	}
}

// A caller that goes away while the middleware waits is not kept waiting: the
// file must fail as cancelled, not as a retry that ran out.
func TestAStoppedCallerEndsTheWait(t *testing.T) {
	busy := &tgerr.Error{Code: 500, Message: "RPC_CALL_FAIL", Type: "RPC_CALL_FAIL"}
	inner := &busyInvoker{errs: []error{busy, busy, busy, busy, busy}}
	ctx, cancel := context.WithCancel(context.Background())
	middleware := retryMiddleware{
		max: 5,
		sleep: func(ctx context.Context, d time.Duration) error {
			cancel()
			return waitBeforeRetry(ctx, d)
		},
	}

	err := middleware.Handle(inner)(ctx, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle() = %v, want context.Canceled so a pause is not recorded as a failed file", err)
	}
	if inner.count() != 1 {
		t.Fatalf("the request was sent %d times after the caller stopped, want 1", inner.count())
	}
}

// The real wait returns as soon as the context ends rather than sitting out the
// delay, which is what makes the test above a statement about the real path.
func TestTheRealWaitHonoursTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := waitBeforeRetry(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitBeforeRetry() = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waitBeforeRetry() waited %s on a cancelled context", elapsed)
	}
}
