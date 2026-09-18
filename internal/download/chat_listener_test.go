package download

import "testing"

func TestTargetIncludesRepliesUsesPersistedSnapshot(t *testing.T) {
	if !targetIncludesReplies(storedChatTarget{}) {
		t.Fatal("missing legacy snapshot must retain the default enabled setting")
	}
	if targetIncludesReplies(storedChatTarget{configJSON: `{"includeReplies":false}`}) {
		t.Fatal("persisted disabled setting must override current global defaults")
	}
	if !targetIncludesReplies(storedChatTarget{configJSON: `{"includeReplies":true}`}) {
		t.Fatal("persisted enabled setting must stay enabled")
	}
}
