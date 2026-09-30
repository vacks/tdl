package download

import (
	"testing"

	"github.com/vacks/tdl/internal/telegram"
)

// A listener can only exist for an account that is signed in. Starting one for
// any other state opens a connection only to be told so: the account's task
// goes on having no listener while the attempt repeats on the reconciliation
// cadence, and every attempt logs a failure.
func TestAuthorizedChatAccountsExcludesAccountsThatCannotListen(t *testing.T) {
	authorized := authorizedChatAccounts([]telegram.Account{
		{ID: "signed-in", State: "authorized"},
		{ID: "signed-out", State: "expired"},
		{ID: "waiting-for-qr", State: "pending"},
		{ID: "unknown-state", State: ""},
		{ID: "also-signed-in", State: "authorized"},
	})
	if len(authorized) != 2 {
		t.Fatalf("authorized set = %v, want exactly the two signed-in accounts", authorized)
	}
	for _, id := range []string{"signed-in", "also-signed-in"} {
		if _, ok := authorized[id]; !ok {
			t.Errorf("%s was left out, so its listening tasks would never get a listener", id)
		}
	}
	for _, id := range []string{"signed-out", "waiting-for-qr", "unknown-state"} {
		if _, ok := authorized[id]; ok {
			t.Errorf("%s was accepted, so a listener would be started only to fail immediately", id)
		}
	}
	if len(authorizedChatAccounts(nil)) != 0 {
		t.Fatal("an empty account list produced an authorized account")
	}
}

// The comparison is what notices a sign-in or an expiry, and it decides whether
// the listener set is rebuilt on this pass. Getting it wrong in either
// direction is a real fault: missing a change means a signed-in account waits
// for the slow refresh before it can listen again, and inventing one means the
// snapshot is rebuilt on every pass forever.
func TestChatListenerAccountsChangedNoticesSignInAndExpiry(t *testing.T) {
	signedIn := map[string]struct{}{"a": {}}
	alsoSignedIn := map[string]struct{}{"a": {}, "b": {}}
	if chatListenerAccountsChanged(signedIn, map[string]struct{}{"a": {}}) {
		t.Error("an unchanged set was reported as changed, which rebuilds the snapshot every pass")
	}
	if !chatListenerAccountsChanged(map[string]struct{}{}, signedIn) {
		t.Error("a sign-in was not noticed, so the listener would wait for the slow refresh")
	}
	if !chatListenerAccountsChanged(signedIn, map[string]struct{}{}) {
		t.Error("an expiry was not noticed, so a dead listener would be left in place")
	}
	if !chatListenerAccountsChanged(signedIn, alsoSignedIn) {
		t.Error("an added account was not noticed")
	}
	if !chatListenerAccountsChanged(alsoSignedIn, signedIn) {
		t.Error("a removed account was not noticed")
	}
	// A swap of the same size must not compare equal just because the counts
	// match - that is the shape a signed-out account being replaced by a
	// different signed-in one takes.
	if !chatListenerAccountsChanged(map[string]struct{}{"a": {}}, map[string]struct{}{"b": {}}) {
		t.Error("a replaced account was not noticed, so the old listener would stay and the new one would never start")
	}
	// The first pass has no previous set, and an empty account store is a real
	// state - a manager whose accounts are still loading. Neither is a change.
	if chatListenerAccountsChanged(nil, map[string]struct{}{}) {
		t.Error("the first pass of a manager with no accounts was reported as a change")
	}
	if !chatListenerAccountsChanged(nil, signedIn) {
		t.Error("the first pass of a manager that already has an account was not reported as a change")
	}
}

// A discussion group learned while the listener snapshot is being rebuilt has
// to survive it. The rebuild replaces the whole map, so a key added between the
// query and the assignment is dropped a moment after it was learned, and the
// group's comments are covered by no other path - the gap walk reads only the
// task's own dialog. Adding a key therefore has to report that it was new, so
// the caller can ask for another rebuild; a key that was already there is the
// common case and must not, or every post in a busy channel would rebuild the
// snapshot.
func TestAddChatWatchedReportsOnlyKeysThatWereNew(t *testing.T) {
	m := &Manager{chatWatched: make(map[string]map[string]struct{})}
	if !m.addChatWatched("account", "channel:1") {
		t.Fatal("the first key for an account was not reported as new")
	}
	if m.addChatWatched("account", "channel:1") {
		t.Fatal("an existing key was reported as new, so every post would rebuild the snapshot")
	}
	if !m.addChatWatched("account", "channel:2") {
		t.Fatal("a second key for the same account was not reported as new")
	}
	if !m.addChatWatched("other", "channel:1") {
		t.Fatal("a key that is new for another account was not reported as new")
	}
	// Keys with nothing to add are not keys: an empty one would collide across
	// every account that has no dialog recorded.
	for _, empty := range [][2]string{{"", "channel:1"}, {"account", ""}, {"", ""}} {
		if m.addChatWatched(empty[0], empty[1]) {
			t.Fatalf("addChatWatched(%q, %q) reported a key it should have ignored", empty[0], empty[1])
		}
	}
	if len(m.chatWatched) != 2 || len(m.chatWatched["account"]) != 2 {
		t.Fatalf("watched set = %v, want the two accounts and two dialogs added", m.chatWatched)
	}
}

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
