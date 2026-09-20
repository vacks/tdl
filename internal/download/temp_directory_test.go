package download

import (
	"path/filepath"
	"testing"
)

func TestTemporaryDialogDirectorySeparatesDialogs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "task")
	channel, err := temporaryDialogDirectory(root, "channel:42")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "channel:42"); channel != want {
		t.Fatalf("channel directory = %q, want %q", channel, want)
	}
	comment, err := temporaryDialogDirectory(root, "channel:84")
	if err != nil {
		t.Fatal(err)
	}
	if comment == channel {
		t.Fatal("different dialogs share a temporary directory")
	}
}
