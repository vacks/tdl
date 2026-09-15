package download

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// transferWatchdog is shared by both message and chat download invocations.
// Upstream tdl is callback-driven; a missing callback must never permanently
// reserve a worker slot or leave an otherwise healthy queue stranded.
type transferWatchdog struct {
	lastActivity atomic.Int64
	hasActivity  atomic.Bool
	stalled      atomic.Bool
	done         chan struct{}
	once         sync.Once
}

func startTransferWatchdog(ctx context.Context, period, initialTimeout, idleTimeout time.Duration, stop func(), active func() bool, onTimeout func(time.Duration)) *transferWatchdog {
	w := &transferWatchdog{done: make(chan struct{})}
	w.lastActivity.Store(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-w.done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !active() {
					stop()
					return
				}
				timeout := initialTimeout
				if w.hasActivity.Load() {
					timeout = idleTimeout
				}
				if time.Since(time.Unix(0, w.lastActivity.Load())) < timeout {
					continue
				}
				w.stalled.Store(true)
				onTimeout(timeout)
				stop()
				return
			}
		}
	}()
	return w
}

func (w *transferWatchdog) Touch() {
	w.hasActivity.Store(true)
	w.lastActivity.Store(time.Now().UnixNano())
}
func (w *transferWatchdog) Stalled() bool { return w.stalled.Load() }
func (w *transferWatchdog) Close()        { w.once.Do(func() { close(w.done) }) }
