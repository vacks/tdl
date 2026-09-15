package download

import (
	"testing"
	"time"
)

func TestChatLocksDoNotShareChildTaskLocks(t *testing.T) {
	manager := &Manager{}
	job := manager.jobLock("same-id")
	chat := manager.chatLock("same-id")
	if job == chat {
		t.Fatal("chat control lock must be independent from child task lock")
	}
	job.Lock()
	done := make(chan struct{})
	go func() {
		chat.Lock()
		chat.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("chat lock waited on child task lock")
	}
	job.Unlock()
}
