package tgclient

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// DefaultMiddlewares is the chain every client starts with, outermost first.
//
// Callers' own middlewares are appended after these, which puts them innermost
// - closer to the wire than the flood-wait waiter. That position is what lets
// the account's rate-limit gate observe a FLOOD_WAIT: the waiter consumes the
// refusal by sleeping and retrying inside the invoker, so anything outside it
// sees only a request that took a long time.
func DefaultMiddlewares(ctx context.Context, reconnectTimeout time.Duration) []telegram.Middleware {
	return []telegram.Middleware{
		recoveryMiddleware{ctx: ctx, backoff: reconnectBackoff(reconnectTimeout)},
		retryMiddleware{max: 5},
		floodwait.NewSimpleWaiter(),
	}
}

// reconnectBackoff bounds how long a broken connection is worth retrying.
func reconnectBackoff(timeout time.Duration) backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.Multiplier = 1.1
	b.MaxElapsedTime = timeout
	b.MaxInterval = 10 * time.Second
	return b
}

// recoveryMiddleware retries a request that failed for a reason that is not
// Telegram answering.
//
// A connection reset, a dial timeout or a dropped socket are transport
// failures: the request never reached anyone, so repeating it is free of the
// side effects that make a blind retry unsafe. Anything Telegram answered -
// including every error code the retry middleware below knows by name - is
// passed through untouched.
type recoveryMiddleware struct {
	ctx     context.Context
	backoff backoff.BackOff
}

func (r recoveryMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return backoff.RetryNotify(func() error {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			if r.shouldRecover(ctx, err) {
				return errors.Wrap(err, "recover")
			}
			return backoff.Permanent(err)
		}, r.backoff, func(error, time.Duration) {
			// The wait is already visible as the request's duration, and the
			// application's own slow-request reporting names the method when
			// that duration grows. A line here would repeat it once per retry.
		})
	}
}

func (r recoveryMiddleware) shouldRecover(ctx context.Context, err error) bool {
	// Recovery stops when either context ends, so a shutdown or a cancelled
	// caller does not have to wait out the backoff.
	select {
	case <-r.ctx.Done():
		return false
	case <-ctx.Done():
		return false
	default:
	}

	// A Telegram error is an answer, not a broken connection.
	_, answered := tgerr.As(err)
	return !answered
}

// internalErrors are failures the server reported about itself - not about the
// request - so the same request is worth sending again.
var internalErrors = []string{
	"Timedout",
	"No workers running",
	"RPC_CALL_FAIL",
	"RPC_MCGET_FAIL",
	"WORKER_BUSY_TOO_LONG_RETRY",
	"memory limit exit",
}

const (
	// retryBackoffBase is the wait before the first resend, doubled per
	// attempt up to retryBackoffMax: 200ms, 400ms, 800ms, 1.6s, which is three
	// seconds spent on a request the server refused five times.
	retryBackoffBase = 200 * time.Millisecond
	retryBackoffMax  = 2 * time.Second
)

// retryMiddleware resends a request Telegram refused because it was busy.
//
// The resends are spaced. Every error it retries is the server saying it has no
// capacity at the moment - "no workers running", "worker busy too long",
// a call that timed out inside the server - so resending as fast as the loop can
// go is the one response certain to make that worse: five immediate attempts per
// call, multiplied by every file downloading at once, arrive as a burst aimed at
// a server that has just reported being overloaded. The wait is short enough
// that a refusal which clears still costs far less than failing the file, whose
// alternative on this path is the whole task going back on the queue.
//
// sleep exists so a test can observe the spacing without spending it; nil is the
// real wait.
type retryMiddleware struct {
	max   int
	sleep func(ctx context.Context, d time.Duration) error
}

func (r retryMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	sleep := r.sleep
	if sleep == nil {
		sleep = waitBeforeRetry
	}
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		delay := retryBackoffBase
		for attempt := 0; attempt < r.max; attempt++ {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			if !tgerr.Is(err, internalErrors...) {
				// Wrapped, not returned bare, because this middleware has been
				// adding this prefix to every Telegram error for as long as the
				// application has existed. Callers that match on the text of an
				// error - and several do, for FLOOD_WAIT and for the error names
				// Telegram uses for an unreadable source - see that text through
				// it. Changing the spelling is a change to what the rest of the
				// application reads, so it belongs in its own change, not in a
				// move of this code.
				return errors.Wrap(err, "retry middleware skip")
			}
			if attempt+1 == r.max {
				break
			}
			if err := sleep(ctx, delay); err != nil {
				// The caller's context ended while waiting, so this request is
				// over. Returned unwrapped because the callers that matter match
				// on it: a batch stopped by a pause must not report its in-flight
				// files as failures.
				return err
			}
			if delay *= 2; delay > retryBackoffMax {
				delay = retryBackoffMax
			}
		}
		return errors.Errorf("retry limit reached after %d attempts", r.max)
	}
}

// waitBeforeRetry waits out one retry delay, or gives up when the caller's
// context ends - a shutdown or a pause must not have to sit through the backoff.
func waitBeforeRetry(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
