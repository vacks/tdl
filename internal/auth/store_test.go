package auth

import (
	"path/filepath"
	"testing"
)

func TestBootstrapPasswordIsOnlyUsedForFirstInitialization(t *testing.T) {
	dir := t.TempDir()
	store, initialized, err := Open(filepath.Join(dir, "data"), "admin", "secret-value")
	if err != nil {
		t.Fatal(err)
	}
	if !initialized || !store.Verify("admin", "secret-value") {
		t.Fatal("expected a usable newly initialized administrator")
	}
	_, initialized, err = Open(filepath.Join(dir, "data"), "admin", "")
	if err != nil || initialized {
		t.Fatalf("existing administrator must reopen without a bootstrap password: initialized=%v err=%v", initialized, err)
	}
}
