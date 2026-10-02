package download

import (
	"testing"
	"time"

	transfer "github.com/vacks/tdl/internal/download/transfer"
)

// The task's rate sample outlives the batch; the files in it do not.
//
// A session task runs in batches, and the end of a batch is not the end of the
// task. Clearing both together - which is what ClearJob did at the end of every
// batch - meant the task's one-second rate window was thrown away and restarted
// each time, so a task downloading small files in short batches reported a rate
// that was mostly zero and often never produced one at all.
func TestAChatTaskKeepsItsRateAcrossBatches(t *testing.T) {
	p := newProgressStore()
	item := Item{DialogType: "channel", DialogKey: "channel:1", DialogID: 1, MessageID: 7}

	// One batch's worth of progress, over enough time for the window to close.
	p.Update("chat-1", item, transfer.ProgressUpdate{DialogID: 1, MessageID: 7, Downloaded: 0, Total: 1000})
	time.Sleep(1100 * time.Millisecond)
	p.Update("chat-1", item, transfer.ProgressUpdate{DialogID: 1, MessageID: 7, Downloaded: 1000, Total: 1000})
	_, before := p.Aggregate("chat-1")
	if before <= 0 {
		t.Fatalf("the task reported no rate while it was transferring: %v", before)
	}

	// The batch ends. Its finished file goes; the task's rate stays.
	p.ClearJobFiles("chat-1")
	if files, _ := p.Aggregate("chat-1"); files != 0 {
		t.Fatalf("%d files survived the end of their batch", files)
	}
	if _, after := p.Aggregate("chat-1"); after != before {
		t.Fatalf("the task's rate was thrown away at the end of a batch: %v became %v", before, after)
	}

	// The task itself ending does clear it.
	p.ClearJob("chat-1")
	if _, after := p.Aggregate("chat-1"); after != 0 {
		t.Fatalf("a finished task still reports %v", after)
	}
}

// A gap makes the previous window describe nothing.
//
// The sample is cumulative over the task, so a task that was quiet for ten
// seconds and then sent a kilobyte would divide the bytes counted before the
// gap by the length of the gap - a rate nothing is achieving, in either
// direction.
func TestATaskThatWasQuietStartsANewWindow(t *testing.T) {
	p := newProgressStore()
	item := Item{DialogKey: "channel:1", DialogID: 1, MessageID: 7}
	p.Update("job-1", item, transfer.ProgressUpdate{MessageID: 7, Downloaded: 0, Total: 100})
	time.Sleep(1100 * time.Millisecond)
	p.Update("job-1", item, transfer.ProgressUpdate{MessageID: 7, Downloaded: 100000, Total: 200000})

	// Quiet for longer than the window Aggregate itself trusts.
	time.Sleep(jobSpeedIdleGap + 100*time.Millisecond)
	p.Update("job-1", item, transfer.ProgressUpdate{MessageID: 7, Downloaded: 150000, Total: 200000})
	time.Sleep(1100 * time.Millisecond)
	p.Update("job-1", item, transfer.ProgressUpdate{MessageID: 7, Downloaded: 160000, Total: 200000})

	_, speed := p.Aggregate("job-1")
	// 60000 bytes over ~1.1s. The previous window's 100000 bytes must not be in
	// it: that would report about 145000/s.
	if speed > 100000 {
		t.Fatalf("the rate after a quiet gap is %v, which still counts bytes from before it", speed)
	}
}
