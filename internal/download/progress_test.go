package download

import (
	"testing"
	"time"

	upstreamDL "github.com/iyear/tdl/app/dl"
)

func TestProgressStoreTracksRateAndClearsOnlyRequestedJob(t *testing.T) {
	store := newProgressStore()
	item := Item{DialogType: "channel", DialogKey: "channel:42", DialogID: 42, MessageID: 7}
	started, completed := store.Update("job-a", item, upstreamDL.ProgressUpdate{DialogID: 42, MessageID: 7, Downloaded: 100, Total: 1000})
	if !started || completed {
		t.Fatalf("initial Update() = started:%v completed:%v, want true:false", started, completed)
	}

	// Simulate a later upstream callback without making this unit test sleep.
	key := progressKey(item.DialogKey, item.MessageID)
	store.mu.Lock()
	state := store.files[key]
	state.lastMeasuredAt = time.Now().Add(-2 * time.Second)
	state.lastMeasuredByte = 100
	store.files[key] = state
	store.mu.Unlock()
	started, completed = store.Update("job-a", item, upstreamDL.ProgressUpdate{DialogID: 42, MessageID: 7, Downloaded: 500, Total: 1000})
	if started || completed {
		t.Fatalf("later Update() = started:%v completed:%v, want false:false", started, completed)
	}
	snapshot := store.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Downloaded != 500 || snapshot[0].SpeedBPS <= 0 {
		t.Fatalf("progress snapshot = %#v, want updated bytes and positive rate", snapshot)
	}

	other := Item{DialogType: "channel", DialogKey: "channel:42", DialogID: 42, MessageID: 8}
	store.Update("job-b", other, upstreamDL.ProgressUpdate{DialogID: 42, MessageID: 8, Downloaded: 1, Total: 1, Completed: true})
	store.ClearJob("job-a")
	snapshot = store.Snapshot()
	if len(snapshot) != 1 || snapshot[0].MessageID != 8 {
		t.Fatalf("ClearJob removed wrong progress entries: %#v", snapshot)
	}
}

func TestProgressStoreRestartsMeasurementWhenBytesReset(t *testing.T) {
	store := newProgressStore()
	item := Item{DialogKey: "channel:42", MessageID: 7}
	store.Update("job-a", item, upstreamDL.ProgressUpdate{MessageID: 7, Downloaded: 900, Total: 1000})
	started, _ := store.Update("job-a", item, upstreamDL.ProgressUpdate{MessageID: 7, Downloaded: 10, Total: 1000})
	if !started {
		t.Fatal("progress reset was not treated as a new measurement")
	}
	progress := store.Snapshot()[0]
	if progress.Downloaded != 10 || progress.SpeedBPS != 0 {
		t.Fatalf("reset progress = %#v, want 10 bytes and no inherited speed", progress)
	}
}
