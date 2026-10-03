package tgclient

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"go.uber.org/multierr"
)

// Pool moves file bytes over several connections while keeping the account's
// single session.
//
// A file transfer is the one thing worth more than one connection: parts are
// fetched in parallel, and a single connection serialises them. Everything
// else - reading messages, resolving peers - stays on the account's resident
// session, because that is where the rate-limit gate and the caches live.
type Pool interface {
	// Client returns a client on the given datacenter, opening the pool for it
	// on first use.
	Client(ctx context.Context, dc int) *tg.Client
	// Default returns a client on the account's own datacenter, which is where
	// requests that name a message id have to go.
	Default(ctx context.Context) *tg.Client
	// Close releases every connection the pool opened.
	Close() error
}

type pool struct {
	api         *telegram.Client
	size        int64
	middlewares []telegram.Middleware
	// open builds the invoker for one datacenter. It is a field so a test can
	// hold it open and observe what that does to the pool's lock.
	open func(ctx context.Context, dc int) (telegram.CloseInvoker, error)

	mu       sync.Mutex
	invokers map[int]tg.Invoker
	closes   map[int]func() error
}

// dcOpenTimeout bounds opening a datacenter pool.
//
// The work is one authorization transfer, so it is short. It is also the only
// thing here that can wait on the network while a caller holds no lock of its
// own, and a pool that never opens would otherwise hold the caller - and, before
// this, the pool's mutex - for as long as the network stayed quiet.
const dcOpenTimeout = 30 * time.Second

// NewPool builds a pool of at most size connections per datacenter.
//
// The middlewares are the account's own, appended after the client's defaults -
// which places them innermost, where a FLOOD_WAIT is still an error rather than
// a delay the waiter is sleeping off. That is what lets the rate-limit gate pace
// the pool's metadata requests and record the refusals; the byte transfers
// themselves are not in the gate's allow-list and spend no budget.
func NewPool(client *telegram.Client, size int64, middlewares ...telegram.Middleware) Pool {
	p := &pool{
		api:         client,
		size:        size,
		middlewares: middlewares,
		invokers:    make(map[int]tg.Invoker),
		closes:      make(map[int]func() error),
	}
	p.open = p.openDatacenter
	return p
}

// current is the datacenter the account's session lives on. Message ids are
// only meaningful there.
func (p *pool) current() int {
	return p.api.Config().ThisDC
}

func (p *pool) Client(ctx context.Context, dc int) *tg.Client {
	return tg.NewClient(p.invoker(ctx, dc))
}

// invoker returns the chain for one datacenter, opening it on first use, and
// waits for an in-progress open rather than opening a second one.
//
// Two things happen here that did not, and both were a transfer that never
// finished rather than a transfer that failed.
//
// The first is that the open happens outside the lock. Opening is a network
// round trip, and the lock covers every datacenter: a single open that waited -
// on a dead route, on a proxy that accepted a connection and then said nothing -
// stopped every other transfer of the account, including the ones already
// running, because they all pass through here to reach their own datacenter. The
// pool could not even be closed: Close takes the same lock.
//
// The second is that the pool is opened under a context of its own rather than
// the caller's. The caller's context is the transfer's, and a transfer is
// cancelled by pausing or cancelling a task - which is a normal thing to do, and
// is not a reason to abandon building a connection that the next task will want.
// Building under the caller's context also made a cancelled open indistinguishable
// from a failed one at this layer, so the cancellation was reported to the
// transfer as an ordinary dead pool.
func (p *pool) invoker(ctx context.Context, dc int) tg.Invoker {
	p.mu.Lock()
	if invoker, ok := p.invokers[dc]; ok {
		p.mu.Unlock()
		return invoker
	}
	p.mu.Unlock()

	openCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dcOpenTimeout)
	defer cancel()
	invoker, err := p.open(openCtx, dc)
	if err != nil {
		// A pool that cannot be opened falls back to the resident session rather
		// than failing the transfer: slower, but the file still arrives.
		return p.api
	}

	chained := chain(&invalidatingInvoker{pool: p, dc: dc, next: invoker}, p.middlewares...)

	p.mu.Lock()
	defer p.mu.Unlock()
	// Another caller opened the same datacenter while this one was waiting on the
	// network. Its connection is as good as this one and is already published, so
	// this one is closed rather than left to hold connections nothing refers to.
	if existing, ok := p.invokers[dc]; ok {
		go func() { _ = invoker.Close() }()
		return existing
	}
	p.closes[dc] = invoker.Close
	p.invokers[dc] = chained
	return chained
}

// openDatacenter opens one datacenter's connections.
func (p *pool) openDatacenter(ctx context.Context, dc int) (telegram.CloseInvoker, error) {
	if dc == p.current() {
		// The current datacenter cannot be transferred to: the pool shares the
		// session's own authorization there.
		return p.api.Pool(p.size)
	}
	return p.api.DC(ctx, dc, p.size)
}

// invalidatingInvoker drops a datacenter's cached invoker once its connections
// are gone, so the next transfer opens fresh ones instead of reusing a pool
// that can never answer again.
//
// The cache is keyed by datacenter and lived for the life of the account, which
// assumes a pool stays usable once opened. It does not: the connections behind
// it are closed when the session reconnects, when a transfer is cancelled
// mid-request, and by the library when a connection is found dead. What a
// transfer saw then was the same closed pool on every attempt - a task that
// retried three times and failed three times with "engine was closed", on a
// datacenter whose other files were downloading normally.
type invalidatingInvoker struct {
	pool *pool
	dc   int
	next tg.Invoker
}

func (i *invalidatingInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	err := i.next.Invoke(ctx, input, output)
	if poolIsGone(err) {
		i.pool.forget(i.dc)
	}
	return err
}

// forget retires one datacenter's invoker. The pool is not closed here: the
// caller that sees the error is inside a request it must finish unwinding, and
// closing connections underneath it is what turns a failed request into a stuck
// one. Dropping the reference is enough - the next transfer opens a new pool,
// and this one is closed with the pool as a whole.
func (p *pool) forget(dc int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.invokers, dc)
	delete(p.closes, dc)
}

// poolIsGone reports whether an error says the connections behind it are gone
// rather than that this request failed.
//
// The list is matched by text because the library reports all three as plain
// errors. It is deliberately narrow: an error that is not on it leaves the
// cached pool in place, because opening a new one for every failed request
// would turn a flaky route into a connection storm.
func poolIsGone(err error) bool {
	if err == nil {
		return false
	}
	// The phrases are checked first, and that order is the whole of it: the
	// library reports a pool it closed itself as "engine forcibly closed" with
	// the context that cancelled it as the cause, so the combined error is both
	// a dead pool and a cancelled request. Testing for the context first would
	// take the one shape that actually happened for the caller's own
	// cancellation, and the pool would never be retired.
	text := err.Error()
	for _, phrase := range []string{
		"engine was closed",
		"engine forcibly closed",
		"connection dead",
	} {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	// A plain context error is the request's own: pausing a transfer cancels it,
	// and that says nothing about whether the connections are usable.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return false
}

func (p *pool) Default(ctx context.Context) *tg.Client {
	return p.Client(ctx, p.current())
}

func (p *pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for dc, closer := range p.closes {
		err = multierr.Append(err, closer())
		delete(p.closes, dc)
		delete(p.invokers, dc)
	}
	return err
}

// chain applies middlewares the way gotd does: the first listed is outermost,
// so the last one ends up closest to the wire.
func chain(invoker tg.Invoker, middlewares ...telegram.Middleware) tg.Invoker {
	for i := len(middlewares) - 1; i >= 0; i-- {
		invoker = middlewares[i].Handle(invoker)
	}
	return invoker
}
