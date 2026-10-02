package tgclient

import (
	"context"
	"sync"

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

	mu       sync.Mutex
	invokers map[int]tg.Invoker
	closes   map[int]func() error
}

// NewPool builds a pool of at most size connections per datacenter.
//
// The middlewares are the account's own, appended after the client's defaults -
// which places them innermost, where a FLOOD_WAIT is still an error rather than
// a delay the waiter is sleeping off. That is what lets the rate-limit gate pace
// the pool's metadata requests and record the refusals; the byte transfers
// themselves are not in the gate's allow-list and spend no budget.
func NewPool(client *telegram.Client, size int64, middlewares ...telegram.Middleware) Pool {
	return &pool{
		api:         client,
		size:        size,
		middlewares: middlewares,
		invokers:    make(map[int]tg.Invoker),
		closes:      make(map[int]func() error),
	}
}

// current is the datacenter the account's session lives on. Message ids are
// only meaningful there.
func (p *pool) current() int {
	return p.api.Config().ThisDC
}

func (p *pool) Client(ctx context.Context, dc int) *tg.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return tg.NewClient(p.invoker(ctx, dc))
}

// invoker returns (and on first use opens) the chain for one datacenter. The
// caller holds p.mu; this is not reentrant, and taking the lock here would
// deadlock against Client.
func (p *pool) invoker(ctx context.Context, dc int) tg.Invoker {
	if invoker, ok := p.invokers[dc]; ok {
		return invoker
	}

	// Opened on first use, because a download may touch at most a couple of
	// datacenters and opening all of them would cost connections nothing asks
	// for.
	var (
		invoker telegram.CloseInvoker
		err     error
	)
	if dc == p.current() {
		// The current datacenter cannot be transferred to: the pool shares the
		// session's own authorization there.
		invoker, err = p.api.Pool(p.size)
	} else {
		invoker, err = p.api.DC(ctx, dc, p.size)
	}
	if err != nil {
		// A pool that cannot be opened falls back to the resident session rather
		// than failing the transfer: slower, but the file still arrives.
		return p.api
	}

	p.closes[dc] = invoker.Close
	p.invokers[dc] = chain(invoker, p.middlewares...)
	return p.invokers[dc]
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
