package download

import (
	"errors"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/settings"
)

func TestTelegramWaitDuration(t *testing.T) {
	for _, test := range []struct {
		err  error
		want time.Duration
	}{
		{errors.New("rpc error FLOOD_WAIT_17"), 17 * time.Second},
		{errors.New("SLOWMODE_WAIT: 3"), 3 * time.Second},
		{errors.New("network unavailable"), 0},
	} {
		if got := telegramWaitDuration(test.err); got != test.want {
			t.Fatalf("telegramWaitDuration(%v) = %s, want %s", test.err, got, test.want)
		}
	}
}

func TestTransferPermitsUseOnlyGlobalLimit(t *testing.T) {
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	values := store.Get()
	values.Download.ConcurrentJobs = 2
	if err := store.Update(values); err != nil {
		t.Fatal(err)
	}
	m := &Manager{settings: store, slotWake: make(chan struct{}, 1)}

	releaseA, ok := m.tryAcquireTransfer(transferMessage, false)
	if !ok {
		t.Fatal("first global transfer permit was not granted")
	}
	releaseB, ok := m.tryAcquireTransfer(transferMessage, false)
	if !ok {
		t.Fatal("second permit for the same Telegram account must be allowed")
	}
	if _, ok := m.tryAcquireTransfer(transferMessage, false); ok {
		t.Fatal("permit exceeded the configured global limit")
	}
	releaseA()
	if releaseC, ok := m.tryAcquireTransfer(transferMessage, false); !ok {
		t.Fatal("released global permit was not reusable")
	} else {
		releaseC()
	}
	releaseB()
}

func TestChatTransferCapacityYieldsOnlyWhileMessageWaits(t *testing.T) {
	for _, test := range []struct {
		global  int
		queued  bool
		wantMax int
	}{
		{global: 1, queued: false, wantMax: 1},
		{global: 1, queued: true, wantMax: 0},
		{global: 2, queued: false, wantMax: 2},
		{global: 2, queued: true, wantMax: 1},
		{global: 3, queued: true, wantMax: 1},
		{global: 4, queued: true, wantMax: 2},
	} {
		if got := chatTransferLimit(test.global, test.queued); got != test.wantMax {
			t.Fatalf("chatTransferLimit(%d, %t) = %d, want %d", test.global, test.queued, got, test.wantMax)
		}
	}
}

func TestMessageQueueReservesOnlyPartOfGlobalCapacity(t *testing.T) {
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	values := store.Get()
	values.Download.ConcurrentJobs = 4
	if err := store.Update(values); err != nil {
		t.Fatal(err)
	}
	m := &Manager{settings: store, slotWake: make(chan struct{}, 1)}
	releases := make([]func(), 0, 4)
	for range 2 {
		release, ok := m.tryAcquireTransfer(transferChat, true)
		if !ok {
			t.Fatal("chat should retain half of the global capacity")
		}
		releases = append(releases, release)
	}
	if _, ok := m.tryAcquireTransfer(transferChat, true); ok {
		t.Fatal("chat exceeded the capacity reserved while a message waits")
	}
	for range 2 {
		release, ok := m.tryAcquireTransfer(transferMessage, false)
		if !ok {
			t.Fatal("message did not receive a reserved global slot")
		}
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
}
