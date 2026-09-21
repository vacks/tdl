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

func TestSavedChatIdentityAndMode(t *testing.T) {
	history := ChatJob{DialogType: "self", SourceURL: "tg://saved/account-a", ListenNew: false, StartMessageID: 0}
	if !isSavedChat(history) || isSavedListen(history) {
		t.Fatal("saved history task classification is incorrect")
	}
	listener := ChatJob{DialogType: "self", SourceURL: "tg://saved/account-a", ListenNew: true, StartMessageID: -1}
	if !isSavedChat(listener) || !isSavedListen(listener) {
		t.Fatal("saved listener task classification is incorrect")
	}
	if isSavedChat(ChatJob{DialogType: "channel", SourceURL: "tg://saved/account-a"}) {
		t.Fatal("non-self task must not be classified as a saved task")
	}
}
