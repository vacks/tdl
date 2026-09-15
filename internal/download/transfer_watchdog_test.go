package download

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransferWatchdogStopsSilentTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stopped atomic.Bool
	watchdog := startTransferWatchdog(ctx, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond, func() { stopped.Store(true) }, func() bool { return true }, func(time.Duration) {})
	defer watchdog.Close()
	deadline := time.Now().Add(time.Second)
	for !stopped.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !stopped.Load() || !watchdog.Stalled() {
		t.Fatal("silent transfer was not marked stalled and stopped")
	}
}

func TestTransferWatchdogTouchDelaysTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stopped atomic.Bool
	watchdog := startTransferWatchdog(ctx, time.Millisecond, 12*time.Millisecond, 60*time.Millisecond, func() { stopped.Store(true) }, func() bool { return true }, func(time.Duration) {})
	defer watchdog.Close()
	for range 4 {
		time.Sleep(5 * time.Millisecond)
		watchdog.Touch()
	}
	if stopped.Load() {
		t.Fatal("active transfer was stopped")
	}
}
