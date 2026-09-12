package reaction

import (
	"context"
	"testing"
	"time"
)

func TestReconnectDelayIsBoundedAndStable(t *testing.T) {
	for _, attempt := range []int{-1, 0, 1, 5, 6, 100} {
		delay := reconnectDelay(attempt)
		if delay < 2*time.Second || delay > 64*time.Second+time.Second {
			t.Fatalf("reconnectDelay(%d) = %s outside bounded backoff", attempt, delay)
		}
		if again := reconnectDelay(attempt); again != delay {
			t.Fatalf("reconnectDelay(%d) changed from %s to %s", attempt, delay, again)
		}
	}
}

func TestListenerHealthStateAndContextWait(t *testing.T) {
	l := &listener{}
	l.set("connected", "", time.Time{}, 0)
	if l.state != "connected" || l.connectedAt == "" || l.retryAt != "" || l.attempt != 0 {
		t.Fatalf("connected state = %#v", l)
	}
	next := time.Now().UTC().Add(time.Minute)
	l.set("retrying", "network", next, 3)
	if l.state != "retrying" || l.lastError != "network" || l.retryAt == "" || l.attempt != 3 {
		t.Fatalf("retrying state = %#v", l)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitContext(ctx, time.Second) {
		t.Fatal("waitContext reported success after cancellation")
	}
}
