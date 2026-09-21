package telegram

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/tg"
)

func TestSessionCheckOnlyMarksSuccessfulProbe(t *testing.T) {
	if !shouldMarkSessionChecked(nil) {
		t.Fatal("successful probe was not accepted")
	}
	if shouldMarkSessionChecked(errors.New("proxy timeout")) {
		t.Fatal("failed network probe was accepted as a session check")
	}
}

func TestAccountStoreSeparatesSessionAndUpstreamState(t *testing.T) {
	root := t.TempDir()
	store := &accountStore{
		sessionPath: filepath.Join(root, "sessions", "account.session"),
		stateDir:    filepath.Join(root, "state", "account"),
	}
	ctx := context.Background()
	if err := store.Set(ctx, "session", []byte("telegram-session")); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, "resume:download-fingerprint", []byte("upstream-resume")); err != nil {
		t.Fatal(err)
	}
	session, err := store.Get(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	resume, err := store.Get(ctx, "resume:download-fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(session), "telegram-session"; got != want {
		t.Fatalf("session = %q, want %q", got, want)
	}
	if got, want := string(resume), "upstream-resume"; got != want {
		t.Fatalf("resume = %q, want %q", got, want)
	}
	if _, err := os.Stat(store.path("resume:download-fingerprint")); err != nil {
		t.Fatalf("resume state file missing: %v", err)
	}
	if store.path("session") == store.path("resume:download-fingerprint") {
		t.Fatal("session and resume state use the same file")
	}
}

func TestReactionFallbackURLKeepsDialogType(t *testing.T) {
	tests := []struct {
		peer tg.InputPeerClass
		want string
	}{
		{&tg.InputPeerUser{UserID: 9}, "tg://reaction/user/9/7"},
		{&tg.InputPeerChat{ChatID: 9}, "tg://reaction/chat/9/7"},
		{&tg.InputPeerChannel{ChannelID: 9}, "tg://reaction/channel/9/7"},
		{&tg.InputPeerSelf{}, "tg://reaction/self/account/7"},
	}
	for _, test := range tests {
		if got := reactionFallbackURL(test.peer, "account", 7); got != test.want {
			t.Errorf("reactionFallbackURL() = %q, want %q", got, test.want)
		}
	}
}

func TestNormalizeRawSelfPeerWhenEntitiesAreMissing(t *testing.T) {
	m := &Manager{accounts: []Account{{ID: "account-1", TelegramID: 12345, State: "authorized"}}}
	if _, ok := m.normalizeRawSelfPeer("account-1", &tg.PeerUser{UserID: 12345}).(*tg.InputPeerSelf); !ok {
		t.Fatal("current user's raw peer was not normalized to InputPeerSelf")
	}
	if peer := m.normalizeRawSelfPeer("account-1", &tg.PeerUser{UserID: 999}); peer != nil {
		t.Fatal("unrelated user was incorrectly normalized to InputPeerSelf")
	}
}

func TestOwnReactionEmojisOnlyReturnsCurrentAccountStandardEmoji(t *testing.T) {
	chosen := tg.ReactionCount{Reaction: &tg.ReactionEmoji{Emoticon: "❤️"}, Count: 1}
	chosen.SetChosenOrder(1)
	reactions := &tg.MessageReactions{
		RecentReactions: []tg.MessagePeerReaction{
			{My: true, Reaction: &tg.ReactionEmoji{Emoticon: "👍"}},
			{My: false, Reaction: &tg.ReactionEmoji{Emoticon: "👎"}},
			{My: true, Reaction: &tg.ReactionCustomEmoji{}},
		},
		Results: []tg.ReactionCount{
			chosen,
			{Reaction: &tg.ReactionEmoji{Emoticon: "🔥"}, Count: 10},
		},
	}
	got := ownReactionEmojis(reactions)
	if len(got) != 2 || got[0] != "👍" || got[1] != "❤️" {
		t.Fatalf("ownReactionEmojis() = %#v, want [👍 ❤️]", got)
	}
}
