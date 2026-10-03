package tgclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

type fakeInvoker struct {
	mu     sync.Mutex
	err    error
	calls  int
	closed bool
}

func (f *fakeInvoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.err
}

func (f *fakeInvoker) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeInvoker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestPool(open func(context.Context, int) (telegram.CloseInvoker, error)) *pool {
	p := &pool{
		size:     4,
		open:     open,
		invokers: make(map[int]tg.Invoker),
		closes:   make(map[int]func() error),
	}
	return p
}

// Opening a datacenter is a network round trip, and it must not hold the lock
// that every other datacenter and Close itself go through.
//
// It did. A transfer to a route that accepted the connection and then said
// nothing held the account's pool mutex for as long as the network stayed quiet,
// so every other transfer on that account - including ones already running,
// which come back through here to reach their own datacenter - stopped, and the
// pool could not even be closed. Nothing failed and nothing timed out; the whole
// account simply stopped transferring.
func TestOpeningADatacenterDoesNotHoldThePoolLock(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	p := newTestPool(func(context.Context, int) (telegram.CloseInvoker, error) {
		close(entered)
		<-release
		return &fakeInvoker{}, nil
	})

	go func() { _ = p.Client(context.Background(), 2) }()
	<-entered

	done := make(chan struct{})
	go func() { _ = p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Close blocked behind an in-progress open; every other datacenter would too")
	}
	close(release)
}

// A datacenter whose connections are gone is reopened instead of being reused
// for the life of the account.
//
// The cache assumes a pool stays usable once opened, and it does not: the
// library closes the connections when one is found dead, and what the next
// transfer then saw was the same closed pool on every attempt. A task retried
// three times and failed three times with "engine was closed" while other files
// on other datacenters downloaded normally.
func TestAClosedDatacenterIsReopened(t *testing.T) {
	first := &fakeInvoker{err: errors.New("invoke pool: request: engine was closed")}
	second := &fakeInvoker{}
	opened := 0
	p := newTestPool(func(context.Context, int) (telegram.CloseInvoker, error) {
		opened++
		if opened == 1 {
			return first, nil
		}
		return second, nil
	})

	if err := p.invoker(context.Background(), 2).Invoke(context.Background(), nil, nil); err == nil {
		t.Fatal("the dead pool reported success")
	}

	// The next transfer has to get a fresh pool, not the one that just said it
	// was closed.
	if err := p.invoker(context.Background(), 2).Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("the reopened pool failed: %v", err)
	}
	if opened != 2 {
		t.Fatalf("the datacenter was opened %d time(s); want it reopened after the pool was reported closed", opened)
	}
	if second.count() != 1 {
		t.Fatalf("the new pool saw %d request(s), want 1", second.count())
	}
}

// A request the caller cancelled says nothing about the connections.
//
// Pausing a task cancels its transfer, and every request in flight comes back as
// a context error. Treating that as a dead pool would close and reopen the
// account's connections every time somebody pressed pause.
func TestACancelledRequestDoesNotRetireThePool(t *testing.T) {
	first := &fakeInvoker{err: context.Canceled}
	opened := 0
	p := newTestPool(func(context.Context, int) (telegram.CloseInvoker, error) {
		opened++
		return first, nil
	})

	if err := p.invoker(context.Background(), 2).Invoke(context.Background(), nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Invoke() = %v, want the caller's cancellation", err)
	}
	p.invoker(context.Background(), 2)
	if opened != 1 {
		t.Fatalf("the datacenter was opened %d time(s) for a cancelled request; want 1", opened)
	}
}

// The dead-pool and cancelled-request shapes arrive together, and the pair has
// to be read as a dead pool.
//
// The library closes the connections itself when a request is cancelled, and
// reports that as "engine forcibly closed" with the cancellation as its cause.
// A rule that asks about the cause first therefore answers the opposite of the
// truth for the one error that actually occurs - which is what the first version
// of this did, and the pool was never retired.
func TestADeadPoolReportedThroughACancellationIsStillRetired(t *testing.T) {
	first := &fakeInvoker{err: fmt.Errorf("invoke pool: rpcDoRequest: retryUntilAck: engine forcibly closed: %w", context.Canceled)}
	opened := 0
	p := newTestPool(func(context.Context, int) (telegram.CloseInvoker, error) {
		opened++
		return first, nil
	})

	_ = p.invoker(context.Background(), 2).Invoke(context.Background(), nil, nil)
	p.invoker(context.Background(), 2)
	if opened != 2 {
		t.Fatalf("the datacenter was opened %d time(s); a pool the library closed has to be reopened", opened)
	}
}
