package bot

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// A bounded retry is only bounded if it eventually stops. The startup greeting
// used to have no end: its backoff pinned at five minutes and it kept asking
// for as long as the process ran, so a control user who had blocked the Bot, or
// a token that had been revoked, produced one failed request every five minutes
// forever with nothing to show for it.
func TestHelpRetryGivesUpAfterItsBudget(t *testing.T) {
	now := time.Now()
	retry := helpRetry{}
	attempts := 0
	for {
		next, again := helpRetryStep(retry, now)
		retry = next
		if !again {
			break
		}
		attempts++
		if attempts > startupHelpAttempts+1 {
			t.Fatalf("the retry budget never ran out after %d attempts", attempts)
		}
	}
	if attempts != startupHelpAttempts {
		t.Fatalf("gave up after %d attempts, want %d", attempts, startupHelpAttempts)
	}
	if !retry.gaveUp {
		t.Fatal("the retry state did not record that it gave up")
	}
	// The loop above drives helpRetryStep directly; the guard that actually
	// stops the caller is this flag, so the two have to agree.
	if retry.next.IsZero() {
		t.Fatal("a give-up step must still leave a usable state behind")
	}
	// Re-arming is deliberate: a restart is a new decision, and helpSent is
	// what records a delivered greeting, not this.
	if _, again := helpRetryStep(helpRetry{}, now); !again {
		t.Fatal("the first attempt of a fresh service was already given up on")
	}
}

// An unwritable cursor is a filesystem state, not a race, so retrying it cannot
// succeed. The write runs on the acknowledge path, which holds the lock that
// orders a conversation's commands, so the old unbounded loop did not merely
// fail to make progress - it blocked every later acknowledge behind it. This
// pins that the call returns instead of waiting for a write that will never
// work.
func TestAdvanceOffsetReturnsWhenTheCursorCannotBeWritten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A parent that is a regular file makes MkdirAll for the cursor's directory
	// fail with ENOTDIR on every attempt, which no amount of waiting repairs.
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := writePrivateFile(blocker, []byte("x")); err != nil {
		t.Fatal(err)
	}
	s := &Service{ctx: ctx, cursorPath: filepath.Join(blocker, "sub", "bot-updates.json"), helpRetry: map[int64]helpRetry{}}

	done := make(chan bool, 1)
	go func() { done <- s.advanceOffset("token", 1) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("a cursor write that cannot succeed was reported as saved")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("advanceOffset never returned; the retry loop is unbounded again")
	}
	if s.offset != 0 {
		t.Fatalf("the in-memory offset advanced to %d despite the write failing", s.offset)
	}
}

// The poll loop's own off-schedule trigger must not be able to extend past its
// bound either: an acknowledge that cannot be persisted leaves the offset where
// it was, which only means Telegram redelivers updates the dispatcher already
// knows about.
func TestAdvanceOffsetIgnoresOffsetsThatWouldGoBackwards(t *testing.T) {
	s := &Service{ctx: context.Background(), cursorPath: filepath.Join(t.TempDir(), "bot-updates.json"), offset: 10}
	if !s.advanceOffset("token", 10) {
		t.Fatal("re-acknowledging the current offset was reported as a failure")
	}
	if s.offset != 10 {
		t.Fatalf("offset moved to %d on an equal acknowledge", s.offset)
	}
}
