package download

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

func TestMigrateItemIdentitySeparatesDialogNamespaces(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
CREATE TABLE download_jobs (id TEXT PRIMARY KEY, source_url TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE download_items (
 id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, dialog_id INTEGER NOT NULL, message_id INTEGER NOT NULL, grouped_id INTEGER NOT NULL DEFAULT 0,
 message_text TEXT NOT NULL DEFAULT '', original_name TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0, final_path TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '', elapsed_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
 UNIQUE(dialog_id, message_id), FOREIGN KEY(job_id) REFERENCES download_jobs(id)
);
INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('legacy-job', 'https://t.me/example/1', 'completed', 'now', 'now');
INSERT INTO download_items(job_id, dialog_id, message_id, original_name, status) VALUES ('legacy-job', 42, 7, 'legacy.bin', 'completed');`); err != nil {
		t.Fatalf("prepare legacy database: %v", err)
	}
	m := &Manager{db: db}
	if err := m.migrate(); err != nil {
		t.Fatalf("migrate(): %v", err)
	}
	var legacyKey string
	if err := db.QueryRow(`SELECT dialog_key FROM download_items WHERE job_id = 'legacy-job'`).Scan(&legacyKey); err != nil {
		t.Fatal(err)
	}
	if legacyKey != "legacy:42" {
		t.Fatalf("legacy dialog key = %q, want legacy:42", legacyKey)
	}
	// Equal numeric IDs from different Telegram peer namespaces must coexist.
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('new-job', 'tg://reaction/channel/42/7', 'queued', 'now', 'now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('new-job', 'channel', 'channel:42', 42, 7, 'new.bin', 'queued')`); err != nil {
		t.Fatalf("insert another peer namespace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('new-job', 'channel', 'channel:42', 42, 7, 'duplicate.bin', 'queued')`); err == nil {
		t.Fatal("duplicate dialog key and message ID was accepted")
	}
}

func TestDialogIdentityUsesPeerNamespaces(t *testing.T) {
	tests := []struct {
		name     string
		peer     tg.InputPeerClass
		account  string
		wantType string
		wantKey  string
		wantID   int64
	}{
		{"private", &tg.InputPeerUser{UserID: 42}, "account-a", "user", "user:42", 42},
		{"basic group", &tg.InputPeerChat{ChatID: 42}, "account-a", "chat", "chat:42", 42},
		{"channel", &tg.InputPeerChannel{ChannelID: 42}, "account-a", "channel", "channel:42", 42},
		{"saved messages", &tg.InputPeerSelf{}, "account-a", "self", "self:account-a", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, key, id := dialogIdentity(test.peer, test.account)
			if kind != test.wantType || key != test.wantKey || id != test.wantID {
				t.Fatalf("dialogIdentity() = (%q, %q, %d), want (%q, %q, %d)", kind, key, id, test.wantType, test.wantKey, test.wantID)
			}
		})
	}
}

func TestIsPublicMessageLink(t *testing.T) {
	for value, want := range map[string]bool{
		"https://t.me/example/1":     true,
		"http://t.me/example/1":      true,
		"tg://reaction/user/1/2":     false,
		"https://example.com/t.me/a": false,
	} {
		if got := isPublicMessageLink(value); got != want {
			t.Errorf("isPublicMessageLink(%q) = %v, want %v", value, got, want)
		}
	}
}

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
