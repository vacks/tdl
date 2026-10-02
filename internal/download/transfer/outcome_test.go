package transfer

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/tmsg"
)

// Every message the batch could not deliver has to be named, because the
// caller keeps a durable row per message and nothing else will ever revisit it.
//
// The counts in Stats are not enough for that. A task of sixty-four files that
// lost one to a dropped connection left that file's row in "running" - a state
// no other pass selects - and the task pinned in 下载中 with the failure
// visible only in a log line. Three kinds have to be told apart here: the file
// that failed, the message that is gone, and the message that never held
// anything.
func TestRunNamesEveryMessageItCouldNotDeliver(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(2000)
	for _, id := range []int{1, 2, 3, 4} {
		api.messages[id] = documentMessage(id, testDate, len(api.file))
	}
	// File 2's transfer is refused outright.
	api.failFileFor[2000] = true
	// Message 3 is gone, which is what a deleted post looks like.
	api.missing[3] = true
	// Message 4 is present and holds no media at all. The media flag is what
	// makes GetMedia answer at all, so a message built without it is one that
	// carries nothing - see documentMessage.
	api.messages[4] = &tg.Message{ID: 4, Date: testDate}

	deps, _ := testDeps(api)
	var completed []int
	var outcomes []FileOutcomeUpdate
	stats, err := Run(context.Background(), deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: []int{1, 2, 3, 4},
		Threads:  4,
		Tasks:    1,
		OnFileCompleted: func(update FileCompletedUpdate) {
			completed = append(completed, update.MessageID)
		},
		OnFileOutcome: func(update FileOutcomeUpdate) {
			outcomes = append(outcomes, update)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(completed) != 1 || completed[0] != 1 {
		t.Fatalf("published %v, want just message 1", completed)
	}
	if stats.Failed != 1 || stats.Deleted != 1 || stats.Empty != 1 {
		t.Fatalf("stats are %+v", stats)
	}

	byMessage := map[int]error{}
	for _, outcome := range outcomes {
		if _, duplicate := byMessage[outcome.MessageID]; duplicate {
			t.Fatalf("message %d was reported twice", outcome.MessageID)
		}
		byMessage[outcome.MessageID] = outcome.Err
	}
	if len(byMessage) != 3 {
		t.Fatalf("%d messages were reported, want 3 (a file that failed, a message that is gone, a message with no media)", len(byMessage))
	}
	if _, published := byMessage[1]; published {
		t.Fatal("the message that was delivered was reported as an outcome")
	}
	if err := byMessage[2]; err == nil {
		t.Fatal("the failed transfer was reported without a reason")
	}
	if err := byMessage[3]; !errors.Is(err, tmsg.ErrMessageDeleted) {
		t.Fatalf("the deleted message reports %v, want ErrMessageDeleted", err)
	}
	if err, reported := byMessage[4]; !reported || err != nil {
		t.Fatalf("the message with no media reports %v (reported: %t), want a nil reason", err, reported)
	}
}

// A transfer the caller stopped is not a transfer that failed.
//
// Pause, cancel, a database outage and shutdown all reach the worker as a
// context error. Reporting those would mark every in-flight file of a paused
// task failed - overwriting the pause the caller is in the middle of writing,
// and turning "the user stopped this" into "these files are broken".
func TestRunReportsNothingForATransferTheCallerStopped(t *testing.T) {
	api := newFakeAPI()
	api.file = byteFile(2000)
	api.messages[1] = documentMessage(1, testDate, len(api.file))
	// Refused anyway, so the worker reaches its failure branch whatever the
	// context does - the guard is what has to keep quiet, not the failure.
	api.failFileFor[1000] = true

	deps, _ := testDeps(api)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var outcomes []FileOutcomeUpdate
	// Progress is reported before the first byte, so cancelling here lands
	// before the transfer starts rather than racing it.
	_, _ = Run(ctx, deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: []int{1},
		Threads:  4,
		Tasks:    1,
		OnProgress: func(update ProgressUpdate) {
			if update.Downloaded == 0 {
				cancel()
			}
		},
		OnFileOutcome: func(update FileOutcomeUpdate) {
			outcomes = append(outcomes, update)
		},
	})

	if len(outcomes) != 0 {
		t.Fatalf("a stopped batch reported %d outcomes: %+v", len(outcomes), outcomes)
	}
}

// cancellingSource drops the batch's context and then fails the read, which is
// what a pause does to a read that is already in flight.
type cancellingSource struct{ cancel context.CancelFunc }

func (s *cancellingSource) Message(_ context.Context, _ int) (*tg.Message, error) {
	s.cancel()
	return nil, errors.New("read tcp 10.0.0.2:52418->149.154.167.51:443: read: connection reset by peer")
}

// A read the caller's own cancellation interrupted is not a message that failed.
//
// The guard on the transfer workers covers a file being fetched; this covers the
// message being read for the file after it, which is the same pause arriving one
// step earlier. Without it the pause produced a failure for a message nothing
// was wrong with - and the caller, which is settling the rows that pause just
// moved, wrote it onto them.
func TestAReadCancelledByTheCallerIsNotAMessageThatFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deps, _ := testDeps(newFakeAPI())
	var outcomes []FileOutcomeUpdate
	stats, _ := Run(ctx, deps, Options{
		Dir:      t.TempDir(),
		Peer:     testPeer,
		Messages: []int{1, 2, 3},
		Threads:  1,
		Tasks:    1,
		Source:   &cancellingSource{cancel: cancel},
		OnFileOutcome: func(update FileOutcomeUpdate) {
			outcomes = append(outcomes, update)
		},
	})

	if len(outcomes) != 0 {
		t.Fatalf("a batch the caller stopped reported %d outcomes: %+v", len(outcomes), outcomes)
	}
	if stats.Files != 0 {
		t.Fatalf("a stopped batch reports %d files", stats.Files)
	}
}
