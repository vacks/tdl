package download

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinalDestinationShortensMessageTextByBytes(t *testing.T) {
	root := t.TempDir()
	item := source{Item: Item{
		DialogID:     1,
		MessageID:    2,
		MessageText:  strings.Repeat("测试正文", 160),
		OriginalName: "video.mp4",
	}}
	path, err := finalDestination(root, "{{ .MessageText }}_{{ .FileName }}", item)
	if err != nil {
		t.Fatalf("finalDestination() error = %v", err)
	}
	name := filepath.Base(path)
	if len([]byte(name)) > linuxNameMaxBytes {
		t.Fatalf("name has %d bytes, want <= %d", len([]byte(name)), linuxNameMaxBytes)
	}
	if !strings.Contains(name, "…") {
		t.Fatalf("name %q does not contain middle ellipsis", name)
	}
}

func TestFinalDestinationSanitizesTemplateValues(t *testing.T) {
	root := t.TempDir()
	item := source{Item: Item{MessageText: "a/b:*?", OriginalName: "file.txt"}}
	path, err := finalDestination(root, "{{ .MessageText }}_{{ .FileName }}", item)
	if err != nil {
		t.Fatalf("finalDestination() error = %v", err)
	}
	if got, want := filepath.Base(path), "a_b:*?_file.txt"; got != want {
		t.Fatalf("filename = %q, want %q", got, want)
	}
}

func TestFinalDestinationSupportsDialogNameAndFileExt(t *testing.T) {
	root := t.TempDir()
	item := source{DialogName: "频道/名称", Item: Item{OriginalName: "archive.tar.gz"}}
	path, err := finalDestination(root, "{{ .DialogName }}/{{ .FileName }}{{ .FileExt }}", item)
	if err != nil {
		t.Fatalf("finalDestination() error = %v", err)
	}
	if got, want := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator))), "频道_名称/archive.tar.gz.gz"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
}

func TestFinalDestinationSupportsFormattedDownloadDate(t *testing.T) {
	root := t.TempDir()
	path, err := finalDestination(root, "{{ formatDate .DownloadDate \"2006-01-02\" }}/{{ .FileName }}", source{Item: Item{OriginalName: "file.txt"}})
	if err != nil {
		t.Fatalf("finalDestination() error = %v", err)
	}
	wantPrefix := time.Now().Format("2006-01-02") + "/"
	if got := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator))); !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("path = %q, want prefix %q", got, wantPrefix)
	}
}

func TestFinalDestinationSupportsGroupedID(t *testing.T) {
	root := t.TempDir()
	item := source{Item: Item{DialogID: 100, GroupedID: 200, MessageID: 3, OriginalName: "file.txt"}}
	path, err := finalDestination(root, "{{ .DialogID }}_{{ .GroupedID }}_{{ .MessageID }}_{{ .FileName }}", item)
	if err != nil {
		t.Fatalf("finalDestination() error = %v", err)
	}
	if got, want := filepath.Base(path), "100_200_3_file.txt"; got != want {
		t.Fatalf("filename = %q, want %q", got, want)
	}
}

func TestFinalDestinationCreatesMissingDirectory(t *testing.T) {
	root := t.TempDir()
	path, err := finalDestination(root, "missing/{{ .FileName }}", source{Item: Item{OriginalName: "file.txt"}})
	if err != nil {
		t.Fatalf("finalDestination() error = %v", err)
	}
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("final directory was not created: %v", err)
	}
}

func TestFinalDestinationRejectsSymlinkOutsideDownloadRoot(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(root, "outside")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err := finalDestination(root, "outside/{{ .FileName }}", source{Item: Item{OriginalName: "file.txt"}})
	if err == nil || !strings.Contains(err.Error(), "下载目录外") {
		t.Fatalf("error = %v, want outside-root error", err)
	}
}

func TestPublishNoReplace(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.WriteFile(source, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publishNoReplace(source, destination); err != nil {
		t.Fatalf("first publishNoReplace() error = %v", err)
	}
	if err := os.WriteFile(source, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publishNoReplace(source, destination); !errors.Is(err, os.ErrExist) && !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("second publishNoReplace() error = %v, want existing destination", err)
	}
}
