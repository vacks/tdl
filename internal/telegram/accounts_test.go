package telegram

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

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
