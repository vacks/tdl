package download

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/settings"
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

func TestChatJobsAreStoredSeparatelyFromMessageJobs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tdl.db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	created, err := m.createChatJob(ChatJob{SourceURL: "https://t.me/example", DialogType: "channel", DialogKey: "channel:42", DialogID: 42, DialogName: "示例频道", AccountID: "account-a", UpperMessageID: 99, ListenNew: true}, directPeer{kind: "channel", id: 42, hash: 7}, "{}")
	if err != nil {
		t.Fatalf("createChatJob(): %v", err)
	}
	if created.Status != ChatStatusQueued || created.ScanState != chatScanPending || created.ID == "" {
		t.Fatalf("created chat job = %#v", created)
	}
	var streamCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_streams WHERE chat_job_id = ?`, created.ID).Scan(&streamCount); err != nil {
		t.Fatal(err)
	}
	if streamCount != len(chatStreamKinds) {
		t.Fatalf("chat stream count = %d, want %d", streamCount, len(chatStreamKinds))
	}
	jobs, total, _, err := m.ListChats("", 10)
	if err != nil {
		t.Fatalf("ListChats(): %v", err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != created.ID || !jobs[0].ListenNew {
		t.Fatalf("chat jobs = %#v, total = %d", jobs, total)
	}
	got, err := m.GetChat(created.ID)
	if err != nil {
		t.Fatalf("GetChat(): %v", err)
	}
	if got.UpperMessageID != 99 || got.DialogKey != "channel:42" {
		t.Fatalf("GetChat() = %#v", got)
	}
	if _, err := m.createChatJob(ChatJob{SourceURL: "https://t.me/example", DialogType: "channel", DialogKey: "channel:42", DialogID: 42, DialogName: "示例频道", AccountID: "account-a", UpperMessageID: 100, ListenNew: true}, directPeer{kind: "channel", id: 42, hash: 7}, "{}"); err == nil {
		t.Fatal("duplicate active chat job was accepted")
	}
}

func TestMigrateQueuesCompletedChatWhenNewMediaStreamIsAdded(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, status, scan_state, created_at, updated_at) VALUES ('old-chat', 'tg://chat', 'channel', 'channel:9', 9, '频道', 'account-a', 0, 10, 'listening', 'completed', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// Mimic a database created before the two newer media streams existed.
	if _, err := db.Exec(`INSERT INTO chat_download_streams(chat_job_id, stream_kind, completed) VALUES ('old-chat', 'photo_video', 1), ('old-chat', 'document', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := m.migrate(); err != nil {
		t.Fatalf("upgrade migrate(): %v", err)
	}
	var status, scan string
	var streams int
	if err := db.QueryRow(`SELECT status, scan_state FROM chat_download_jobs WHERE id = 'old-chat'`).Scan(&status, &scan); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_streams WHERE chat_job_id = 'old-chat'`).Scan(&streams); err != nil {
		t.Fatal(err)
	}
	if status != ChatStatusQueued || scan != chatScanPending || streams != len(chatStreamKinds) {
		t.Fatalf("upgraded chat = status:%q scan:%q streams:%d", status, scan, streams)
	}
}

func TestQueueIndexedChatMediaKeepsNonDuplicateMembers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tdl.db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{db: db, settings: store, wake: make(chan struct{}, 1), events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	chat, err := m.createChatJob(ChatJob{SourceURL: "https://t.me/example", DialogType: "channel", DialogKey: "channel:42", DialogID: 42, DialogName: "示例频道", AccountID: "account-a", UpperMessageID: 20}, directPeer{kind: "channel", id: 42, hash: 7}, "{}")
	if err != nil {
		t.Fatal(err)
	}
	existing := source{DialogName: "示例频道", Item: Item{DialogType: "channel", DialogKey: "channel:42", DialogID: 42, MessageID: 10, OriginalName: "old.bin"}}
	if _, err := m.enqueueIntent(DownloadIntent{Source: SourceWeb, AccountID: "account-a", URL: "https://t.me/example/10"}, []source{existing}, directPeer{kind: "channel", id: 42, hash: 7}); err != nil {
		t.Fatal(err)
	}
	newItem := source{DialogName: "示例频道", Item: Item{DialogType: "channel", DialogKey: "channel:42", DialogID: 42, MessageID: 11, OriginalName: "new.bin"}}
	for _, item := range []source{existing, newItem} {
		if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, dialog_type, dialog_id, original_name, discovered_at) VALUES (?, ?, ?, ?, ?, ?, 'now')`, chat.ID, item.DialogKey, item.MessageID, item.DialogType, item.DialogID, item.OriginalName); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.queueIndexedChatMedia(chat.ID); err != nil {
		t.Fatal(err)
	}
	var linked int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = ? AND child_job_id != ''`, chat.ID).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != 2 {
		t.Fatalf("linked items = %d, want 2", linked)
	}
}

func TestCleanupHistoryRemovesTerminalChatIndexes(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -2).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, status, scan_state, created_at, updated_at) VALUES ('chat-old', 'tg://chat', 'channel', 'channel:9', 9, '旧频道', 'account-a', 0, 10, 'completed', 'completed', ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, discovered_at) VALUES ('chat-old', 'channel:9', 1, ?)`, old); err != nil {
		t.Fatal(err)
	}
	result, err := m.CleanupHistory(1)
	if err != nil {
		t.Fatal(err)
	}
	if result.ChatJobs != 1 {
		t.Fatalf("chat jobs = %d, want 1", result.ChatJobs)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_jobs`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining chat jobs = %d, want 0", remaining)
	}
}

func TestDeleteChatPreservesChildReferencedByAnotherChat(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), downloadDir: dir}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, id := range []string{"chat-a", "chat-b"} {
		if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, status, scan_state, created_at, updated_at) VALUES (?, 'tg://chat', 'channel', 'channel:9', 9, '频道', 'account-a', 0, 10, 'completed', 'completed', ?, ?)`, id, now, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, parent_chat_id, created_at, updated_at) VALUES ('child', 'tg://chat/chat-a/1', 'completed', 'chat-a', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('child', 'channel', 'channel:9', 9, 1, 'file.bin', 'completed')`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"chat-a", "chat-b"} {
		if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, child_job_id, discovered_at) VALUES (?, 'channel:9', 1, 'child', ?)`, id, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.DeleteChat("chat-a"); err != nil {
		t.Fatalf("DeleteChat(): %v", err)
	}
	var children, linked int
	if err := db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE id = 'child'`).Scan(&children); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'chat-b' AND child_job_id = 'child'`).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if children != 1 || linked != 1 {
		t.Fatalf("shared child was removed: children=%d linked=%d", children, linked)
	}
}

func TestChatControlsPropagateToOwnedDownloadJobs(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), downloadDir: dir}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, status, scan_state, created_at, updated_at) VALUES ('chat', 'tg://chat', 'channel', 'channel:9', 9, '频道', 'account-a', 0, 10, 'downloading', 'completed', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, account_id, status, parent_chat_id, created_at, updated_at) VALUES ('child', 'tg://chat/chat/1', 'account-a', 'queued', 'chat', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('child', 'channel', 'channel:9', 9, 1, 'file.bin', 'queued')`); err != nil {
		t.Fatal(err)
	}
	if err := m.PauseChat("chat"); err != nil {
		t.Fatalf("PauseChat(): %v", err)
	}
	assertTaskStatuses(t, db, "paused", "paused")
	if err := m.ResumeChat("chat"); err != nil {
		t.Fatalf("ResumeChat(): %v", err)
	}
	assertTaskStatuses(t, db, "downloading", "queued")
	if err := m.CancelChat("chat"); err != nil {
		t.Fatalf("CancelChat(): %v", err)
	}
	assertTaskStatuses(t, db, "cancelled", "cancelled")
	if err := m.RetryChat("chat"); err != nil {
		t.Fatalf("RetryChat(): %v", err)
	}
	assertTaskStatuses(t, db, "downloading", "queued")
}

func TestCleanupHistoryPreservesChildReferencedByRetainedChat(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), downloadDir: dir}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -2).Format(time.RFC3339Nano)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, job := range []struct{ id, stamp string }{{"chat-old", old}, {"chat-kept", now}} {
		if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, status, scan_state, created_at, updated_at) VALUES (?, 'tg://chat', 'channel', 'channel:9', 9, '频道', 'account-a', 0, 10, 'completed', 'completed', ?, ?)`, job.id, job.stamp, job.stamp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, parent_chat_id, created_at, updated_at) VALUES ('shared-child', 'tg://chat/chat-old/1', 'completed', 'chat-old', ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('shared-child', 'channel', 'channel:9', 9, 1, 'file.bin', 'completed')`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"chat-old", "chat-kept"} {
		if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, child_job_id, discovered_at) VALUES (?, 'channel:9', 1, 'shared-child', ?)`, id, now); err != nil {
			t.Fatal(err)
		}
	}
	result, err := m.CleanupHistory(1)
	if err != nil {
		t.Fatalf("CleanupHistory(): %v", err)
	}
	if result.ChatJobs != 1 {
		t.Fatalf("cleaned chat jobs = %d, want 1", result.ChatJobs)
	}
	var child, linked int
	if err := db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE id = 'shared-child'`).Scan(&child); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'chat-kept' AND child_job_id = 'shared-child'`).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if child != 1 || linked != 1 {
		t.Fatalf("shared child was cleaned: child=%d linked=%d", child, linked)
	}
}

func TestCleanupHistoryCollectsStandaloneJobAfterChatReferenceExpires(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), downloadDir: dir}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -2).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, status, scan_state, created_at, updated_at) VALUES ('chat-old', 'tg://chat', 'channel', 'channel:9', 9, '频道', 'account-a', 0, 10, 'completed', 'completed', ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}
	// This represents a pre-existing manual download linked to the chat by
	// de-duplication. It has no parent_chat_id and must be collected only
	// after the expired chat index row is gone.
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, account_id, status, created_at, updated_at) VALUES ('manual', 'https://t.me/example/1', 'account-a', 'completed', ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('manual', 'channel', 'channel:9', 9, 1, 'file.bin', 'completed')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, child_job_id, discovered_at) VALUES ('chat-old', 'channel:9', 1, 'manual', ?)`, old); err != nil {
		t.Fatal(err)
	}
	result, err := m.CleanupHistory(1)
	if err != nil {
		t.Fatalf("CleanupHistory(): %v", err)
	}
	if result.ChatJobs != 1 || result.Jobs != 1 {
		t.Fatalf("cleanup result = %#v, want one chat and one normal job", result)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE id = 'manual'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("standalone job remained after its chat reference expired: %d", remaining)
	}
}

func assertTaskStatuses(t *testing.T, db *sql.DB, wantChat, wantChild string) {
	t.Helper()
	var chat, child string
	if err := db.QueryRow(`SELECT status FROM chat_download_jobs WHERE id = 'chat'`).Scan(&chat); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'child'`).Scan(&child); err != nil {
		t.Fatal(err)
	}
	if chat != wantChat || child != wantChild {
		t.Fatalf("statuses = chat:%q child:%q, want chat:%q child:%q", chat, child, wantChat, wantChild)
	}
}

func TestChatSourceBatchesDoNotSplitAlbums(t *testing.T) {
	items := []source{
		{Item: Item{MessageID: 1}},
		{Item: Item{MessageID: 2, GroupedID: 9}},
		{Item: Item{MessageID: 3, GroupedID: 9}},
		{Item: Item{MessageID: 4, GroupedID: 9}},
		{Item: Item{MessageID: 5}},
	}
	batches := chatSourceBatches(items, 3)
	if len(batches) != 3 || len(batches[0]) != 1 || len(batches[1]) != 3 || len(batches[2]) != 1 {
		t.Fatalf("batches = %#v", batches)
	}
}

func TestEnqueueIntentAttachesDuplicateRequestToExistingJob(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tdl.db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{db: db, settings: store, wake: make(chan struct{}, 1), events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	sources := []source{{DialogName: "test", Item: Item{DialogType: "channel", DialogKey: "channel:42", DialogID: 42, MessageID: 7, OriginalName: "file.bin", Status: "queued"}}}
	first, err := m.enqueueIntent(DownloadIntent{Source: SourceWeb, AccountID: "account-a", URL: "https://t.me/example/7"}, sources, directPeer{})
	if err != nil {
		t.Fatalf("first enqueueIntent(): %v", err)
	}
	if !first.Created || first.Duplicate || first.Job.ID == "" {
		t.Fatalf("first submission = %#v, want created job", first)
	}
	second, err := m.enqueueIntent(DownloadIntent{Source: SourceBot, AccountID: "account-a", URL: "https://t.me/example/7"}, sources, directPeer{})
	if err != nil {
		t.Fatalf("duplicate enqueueIntent(): %v", err)
	}
	if second.Created || !second.Duplicate || second.Job.ID != first.Job.ID {
		t.Fatalf("duplicate submission = %#v, want attachment to %q", second, first.Job.ID)
	}
	var requests int
	if err := db.QueryRow(`SELECT COUNT(1) FROM download_requests WHERE job_id = ?`, first.Job.ID).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("request count = %d, want 2", requests)
	}
	var snapshot string
	if err := db.QueryRow(`SELECT config_json FROM download_jobs WHERE id = ?`, first.Job.ID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snapshot, "tempFilenameTemplate") || !strings.Contains(snapshot, "finalFilenameTemplate") {
		t.Fatalf("download configuration snapshot missing expected templates: %s", snapshot)
	}
	if _, err := db.Exec(`UPDATE download_jobs SET status = 'completed' WHERE id = ?`, first.Job.ID); err != nil {
		t.Fatal(err)
	}
	m.downloadDir = t.TempDir()
	if err := m.Delete(first.Job.ID); err != nil {
		t.Fatalf("Delete() with request/event history: %v", err)
	}
}

func TestCancelledTaskReactivatesForRepeatedLink(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{db: db, settings: store, wake: make(chan struct{}, 1), events: newEventBus(), cancels: make(map[string]context.CancelFunc)}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	sources := []source{{DialogName: "test", Item: Item{DialogType: "channel", DialogKey: "channel:42", DialogID: 42, MessageID: 7, OriginalName: "file.bin", Status: "queued"}}}
	first, err := m.enqueueIntent(DownloadIntent{Source: SourceWeb, AccountID: "account-a", URL: "https://t.me/example/7"}, sources, directPeer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(first.Job.ID); err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	second, err := m.enqueueIntent(DownloadIntent{Source: SourceWeb, AccountID: "account-a", URL: "https://t.me/example/7"}, sources, directPeer{})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || !second.Reactivated || second.Job.ID != first.Job.ID || second.Job.Status != "queued" {
		t.Fatalf("reactivated submission = %#v", second)
	}
	if err := m.Cancel(first.Job.ID); err != nil {
		t.Fatalf("Cancel before explicit retry: %v", err)
	}
	if err := m.Retry(first.Job.ID); err != nil {
		t.Fatalf("Retry cancelled task: %v", err)
	}
}

func TestCancelledReactionCanBeTriggeredAgain(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, wake: make(chan struct{}, 1), events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('cancelled-job', 'tg://reaction/user/42/7', 'cancelled', 'now', 'now')`); err != nil {
		t.Fatal(err)
	}
	intent := DownloadIntent{Source: SourceReaction, AccountID: "account-a", Message: &MessageRef{DialogName: "private", DialogID: 42, MessageID: 7, InputPeer: &tg.InputPeerUser{UserID: 42, AccessHash: 99}}}
	queued, err := m.QueueReaction(intent, "👍")
	if err != nil || !queued {
		t.Fatalf("first QueueReaction() = (%v, %v)", queued, err)
	}
	events, err := m.ClaimReactionInbox(1)
	if err != nil || len(events) != 1 {
		t.Fatalf("ClaimReactionInbox() = (%d, %v)", len(events), err)
	}
	if err := m.CompleteReactionInbox(events[0].ID, "cancelled-job"); err != nil {
		t.Fatal(err)
	}
	queued, err = m.QueueReaction(intent, "👍")
	if err != nil || !queued {
		t.Fatalf("repeated cancelled reaction = (%v, %v), want (true, nil)", queued, err)
	}
}

func TestReactionInboxPersistsAndLeasesEvent(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, wake: make(chan struct{}, 1), events: newEventBus()}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	intent := DownloadIntent{Source: SourceReaction, AccountID: "account-a", Message: &MessageRef{DialogName: "private", DialogID: 42, MessageID: 7, InputPeer: &tg.InputPeerUser{UserID: 42, AccessHash: 99}}}
	queued, err := m.QueueReaction(intent, "👍")
	if err != nil || !queued {
		t.Fatalf("QueueReaction() = (%v, %v), want (true, nil)", queued, err)
	}
	queued, err = m.QueueReaction(intent, "👍")
	if err != nil || queued {
		t.Fatalf("duplicate QueueReaction() = (%v, %v), want (false, nil)", queued, err)
	}
	events, err := m.ClaimReactionInbox(1)
	if err != nil || len(events) != 1 {
		t.Fatalf("ClaimReactionInbox() = (%d, %v), want one event", len(events), err)
	}
	if events[0].Intent.Message == nil || events[0].Intent.Message.MessageID != 7 || events[0].Attempts != 1 {
		t.Fatalf("leased event = %#v", events[0])
	}
	if err := m.CompleteReactionInbox(events[0].ID, "job-a"); err != nil {
		t.Fatal(err)
	}
	var status, jobID string
	if err := db.QueryRow(`SELECT status, job_id FROM reaction_inbox WHERE id = ?`, events[0].ID).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != "done" || jobID != "job-a" {
		t.Fatalf("inbox status = (%q, %q), want (done, job-a)", status, jobID)
	}
}

func TestListCursorReturnsSummariesWithoutItems(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"job-3", "job-2", "job-1"} {
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES (?, ?, 'queued', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, id, "https://t.me/example/1"); err != nil {
			t.Fatal(err)
		}
	}
	first, total, cursor, err := m.ListCursor("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(first) != 2 || cursor == "" || first[0].ID != "job-3" || first[0].Items != nil {
		t.Fatalf("first cursor page = %#v, total=%d, cursor=%q", first, total, cursor)
	}
	second, _, next, err := m.ListCursor(cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != "job-1" || next != "" {
		t.Fatalf("second cursor page = %#v, next=%q", second, next)
	}
}

func TestCleanupHistoryProcessesMoreThanOneBatch(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db}
	if err := m.migrate(); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES (?, 'https://t.me/example/1', 'completed', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cleanupBatchSize+1; i++ {
		if _, err := stmt.Exec(fmt.Sprintf("job-%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result, err := m.CleanupHistory(1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Jobs != cleanupBatchSize+1 {
		t.Fatalf("removed %d jobs, want %d", result.Jobs, cleanupBatchSize+1)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(1) FROM download_jobs`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining jobs = %d, want 0", remaining)
	}
}

func TestDatabaseInstanceIDIsStable(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "tdl.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	first := &Manager{db: db}
	if err := first.migrate(); err != nil {
		t.Fatal(err)
	}
	if err := first.loadInstanceID(); err != nil {
		t.Fatal(err)
	}
	if first.InstanceID() == "" {
		t.Fatal("database instance ID is empty")
	}
	second := &Manager{db: db}
	if err := second.loadInstanceID(); err != nil {
		t.Fatal(err)
	}
	if second.InstanceID() != first.InstanceID() {
		t.Fatalf("instance ID changed from %q to %q", first.InstanceID(), second.InstanceID())
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
