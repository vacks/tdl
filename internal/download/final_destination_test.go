package download

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// defaultPattern mirrors the filename template shipped in settings, which is
// what every task created without an explicit configuration uses.
const defaultPattern = "{{ .OriginDialogName }}/{{ .OriginMessageID }}_{{ if .IsComment }}c_{{ end }}{{ .MessageID }}{{ if .MessageText }}_{{ .MessageText }}{{ end }}{{ .FileExt }}"

func destinationItem() source {
	return source{Item: Item{
		DialogID:         7,
		MessageID:        42,
		OriginMessageID:  42,
		OriginDialogName: "chan",
		OriginalName:     "photo.jpg",
	}, DialogName: "chan"}
}

// A name that fits is used as given, and its directory is prepared so the
// caller can move a file into it.
func TestFinalDestinationKeepsAFittingName(t *testing.T) {
	root := t.TempDir()
	item := destinationItem()
	item.MessageText = "hello"

	path, err := finalDestination(root, defaultPattern, item)
	if err != nil {
		t.Fatalf("finalDestination: %v", err)
	}
	want := filepath.Join(root, "chan", "42_42_hello.jpg")
	if path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil || !info.IsDir() {
		t.Fatalf("destination directory was not prepared: %v", err)
	}
}

// A caption long enough to overflow a path component is shortened from the
// middle, keeping the beginning and the end of the original.
func TestFinalDestinationShrinksLongCaption(t *testing.T) {
	root := t.TempDir()
	item := destinationItem()
	item.MessageText = strings.Repeat("a", 200) + strings.Repeat("z", 200)

	path, err := finalDestination(root, defaultPattern, item)
	if err != nil {
		t.Fatalf("finalDestination: %v", err)
	}
	base := filepath.Base(path)
	if got := len([]byte(base)); got > linuxNameMaxBytes {
		t.Fatalf("basename is %d bytes, over the %d limit: %q", got, linuxNameMaxBytes, base)
	}
	if !strings.HasPrefix(base, "42_42_a") || !strings.HasSuffix(base, "z.jpg") {
		t.Fatalf("shrunk caption lost its beginning or end: %q", base)
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil || !info.IsDir() {
		t.Fatalf("destination directory was not prepared: %v", err)
	}
}

// An oversized upstream filename is shortened while keeping its extension, and
// is the only thing shortened: the caption is already absent here.
func TestFinalDestinationShrinksLongFilename(t *testing.T) {
	root := t.TempDir()
	pattern := "{{ .OriginDialogName }}/{{ .FileName }}"
	item := destinationItem()
	item.OriginalName = strings.Repeat("y", 400) + ".bin"

	path, err := finalDestination(root, pattern, item)
	if err != nil {
		t.Fatalf("finalDestination: %v", err)
	}
	base := filepath.Base(path)
	if got := len([]byte(base)); got > linuxNameMaxBytes {
		t.Fatalf("basename is %d bytes, over the %d limit", got, linuxNameMaxBytes)
	}
	if !strings.HasSuffix(base, ".bin") {
		t.Fatalf("shortened filename lost its extension: %q", base)
	}
}

// The shrinking searches evaluate many candidates, and only the chosen one may
// reach the filesystem. Creating the directory of every discarded candidate left
// the download root full of directories for names that were never used.
func TestFinalDestinationPreparesOnlyTheChosenDirectory(t *testing.T) {
	root := t.TempDir()
	// The caption is a path component here, so each shrunken candidate would
	// have rendered a differently named directory.
	pattern := "{{ .OriginDialogName }}/{{ .MessageText }}/{{ .MessageID }}{{ .FileExt }}"
	item := destinationItem()
	item.MessageText = strings.Repeat("b", 1000)

	path, err := finalDestination(root, pattern, item)
	if err != nil {
		t.Fatalf("finalDestination: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "chan"))
	if err != nil {
		t.Fatalf("read download root: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("prepared %d directories, want only the chosen one: %v", len(entries), names)
	}
	if chosen := filepath.Base(filepath.Dir(path)); chosen != entries[0].Name() {
		t.Fatalf("prepared %q but chose %q", entries[0].Name(), chosen)
	}
	if len([]byte(entries[0].Name())) > linuxNameMaxBytes {
		t.Fatalf("chosen directory name is over the limit: %d bytes", len([]byte(entries[0].Name())))
	}
}

// A template that cannot produce a usable path is an administrator error and
// must stay visible. It is not a length problem, so no amount of shrinking is
// allowed to hide it.
func TestFinalDestinationRejectsUnsafeTemplate(t *testing.T) {
	root := t.TempDir()
	item := destinationItem()
	item.MessageText = strings.Repeat("c", 500)

	_, err := finalDestination(root, "../{{ .MessageText }}", item)
	if err == nil {
		t.Fatal("expected an error for a template that escapes the download root")
	}
	if errors.Is(err, errFinalNameTooLong) {
		t.Fatalf("unsafe template was reported as a length problem: %v", err)
	}
}
