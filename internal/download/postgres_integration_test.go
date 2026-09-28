package download

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

func clearPostgresDownloadTestData(db *database) error {
	for _, statement := range []string{
		`DELETE FROM chat_download_streams`, `DELETE FROM chat_download_items`, `DELETE FROM chat_download_jobs`,
		`DELETE FROM downloaded_media`,
		`DELETE FROM bot_lifecycle_messages`, `DELETE FROM download_requests`, `DELETE FROM download_events`,
		`DELETE FROM reaction_inbox`, `DELETE FROM chat_message_inbox`, `DELETE FROM download_items`, `DELETE FROM download_jobs`,
		// Account cooldowns are per-process state that a test seeds through the
		// production path, so leaving them behind would leak into later tests.
		`DELETE FROM telegram_rate_limits`,
	} {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

// TestPostgresSchemaAndAtomicItemIdentity exercises the production driver and
// placeholder binding. It intentionally requires a disposable database URL so
// normal unit tests never touch a developer's running history database.
func TestPostgresSchemaAndAtomicItemIdentity(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatalf("migratePostgres(): %v", err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('pg-concurrent-a', 'tg://test', 'queued', ?, ?), ('pg-concurrent-b', 'tg://test', 'queued', ?, ?)`, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, id := range []string{"pg-concurrent-a", "pg-concurrent-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES (?, 'channel', 'channel:integration', 1, 1, 'file.bin', 'queued') ON CONFLICT(dialog_key, message_id) DO NOTHING`, id); err != nil {
				t.Errorf("concurrent insert: %v", err)
			}
		}(id)
	}
	wg.Wait()
	var count, version int
	if err := db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE dialog_key = 'channel:integration' AND message_id = 1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 2`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if count != 1 || version != 1 {
		t.Fatalf("atomic items=%d, schema version rows=%d; want 1, 1", count, version)
	}
}

func TestPostgresVisibleTaskCountsUseStartupCache(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('visible-a', 'tg://a', 'queued', ?, ?), ('hidden-child', 'tg://b', 'queued', ?, ?), ('deleted-a', 'tg://c', 'deleted', ?, ?)`, now, now, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE download_jobs SET parent_chat_id = 'chat-a' WHERE id = 'hidden-child'`); err != nil {
		t.Fatal(err)
	}
	if err := m.loadVisibleCounts(); err != nil {
		t.Fatal(err)
	}
	if got := m.visibleJobCount(); got != 1 {
		t.Fatalf("visible job count=%d, want 1", got)
	}
	// The list reads the cached count: changing the database alone must not
	// change the reported total until a committed domain transition updates it.
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('visible-late', 'tg://late', 'queued', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	_, total, _, err := m.ListCursor("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("cached total=%d, want 1", total)
	}
	m.jobBecameVisible("")
	_, total, _, err = m.ListCursor("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("updated cached total=%d, want 2", total)
	}
}

func TestPostgresChatControlsUpdateOnlyMediaIndex(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('control-chat','tg://chat','channel','channel:control',1,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, discovered_at) VALUES ('control-chat','channel:control',1,'file.bin','queued',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:control', 1, 'claimed', 'chat', 'control-chat', ?)`, now); err != nil {
		t.Fatal(err)
	}

	if err := m.PauseChat("control-chat"); err != nil {
		t.Fatalf("PauseChat: %v", err)
	}
	assertChatStats(t, db, "control-chat", chatStats{discovered: 1, paused: 1})
	assertStatus := func(table, id, want string) {
		t.Helper()
		var got string
		if err := db.QueryRow(`SELECT status FROM `+table+` WHERE id = ?`, id).Scan(&got); err != nil || got != want {
			t.Fatalf("%s %s status=%q err=%v, want %q", table, id, got, err, want)
		}
	}
	assertStatus("chat_download_jobs", "control-chat", "paused")
	var itemStatus string
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'control-chat' AND message_id = 1`).Scan(&itemStatus); err != nil || itemStatus != "paused" {
		t.Fatalf("media status=%q err=%v", itemStatus, err)
	}
	if err := m.CancelChat("control-chat"); err != nil {
		t.Fatalf("CancelChat: %v", err)
	}
	assertStatus("chat_download_jobs", "control-chat", "cancelled")
	assertChatStats(t, db, "control-chat", chatStats{discovered: 1, cancelled: 1})
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'control-chat' AND message_id = 1`).Scan(&itemStatus); err != nil || itemStatus != "cancelled" {
		t.Fatalf("media status=%q err=%v", itemStatus, err)
	}
	var claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:control' AND message_id = 1`).Scan(&claims); err != nil || claims != 0 {
		t.Fatalf("cancelled chat claim count=%d err=%v, want 0", claims, err)
	}
	if err := m.RetryChat("control-chat"); err != nil {
		t.Fatalf("RetryChat: %v", err)
	}
	assertStatus("chat_download_jobs", "control-chat", "downloading")
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'control-chat' AND message_id = 1`).Scan(&itemStatus); err != nil || itemStatus != "queued" {
		t.Fatalf("media status=%q err=%v", itemStatus, err)
	}
	claim, _, err := m.claimChatMedia("control-chat", source{Item: Item{DialogKey: "channel:control", MessageID: 1}})
	if err != nil || claim != "queued" {
		t.Fatalf("retry claim=%q err=%v, want queued", claim, err)
	}
	var owner string
	if err := db.QueryRow(`SELECT owner_id FROM downloaded_media WHERE dialog_key = 'channel:control' AND message_id = 1`).Scan(&owner); err != nil || owner != "control-chat" {
		t.Fatalf("reclaimed owner=%q err=%v", owner, err)
	}
	assertChatStats(t, db, "control-chat", chatStats{discovered: 1, queued: 1})
}

type chatStats struct {
	discovered, queued, waiting, running, downloaded, completed, failed, paused, cancelled int
}

func assertChatStats(t *testing.T, db *database, chatID string, want chatStats) {
	t.Helper()
	var got chatStats
	err := db.QueryRow(`SELECT discovered, queued, waiting, running, downloaded, completed, failed, paused, cancelled FROM chat_download_stats WHERE chat_job_id = ?`, chatID).Scan(&got.discovered, &got.queued, &got.waiting, &got.running, &got.downloaded, &got.completed, &got.failed, &got.paused, &got.cancelled)
	if err != nil || got != want {
		t.Fatalf("chat stats for %s = %#v err=%v, want %#v", chatID, got, err, want)
	}
}

// Statement-level summary triggers must handle the mixed bulk transitions
// used by pause, retry, recovery, and purge without a caller maintaining
// fragile per-file counter deltas.
func TestPostgresChatStatsTrackBulkTransitionsAndDeletes(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('stats-chat','tg://chat','channel','channel:stats',1,'stats','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, discovered_at) VALUES
 ('stats-chat','channel:stats',10,'a','queued',?),
 ('stats-chat','channel:stats',9,'b','waiting',?),
 ('stats-chat','channel:stats',8,'c','running',?),
 ('stats-chat','channel:stats',7,'d','completed',?)`, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	assertChatStats(t, db, "stats-chat", chatStats{discovered: 4, queued: 1, waiting: 1, running: 1, completed: 1})
	job, err := m.GetChat("stats-chat")
	if err != nil || job.Discovered != 4 || job.Completed != 1 || job.Failed != 0 || job.EarliestMediaID != 7 {
		t.Fatalf("GetChat summary=%#v err=%v", job, err)
	}
	if err := m.loadVisibleCounts(); err != nil {
		t.Fatal(err)
	}
	jobs, total, _, err := m.ListChats("", 10)
	if err != nil || total != 1 || len(jobs) != 1 || jobs[0].Discovered != 4 || jobs[0].Completed != 1 || jobs[0].EarliestMediaID != 7 {
		t.Fatalf("ListChats jobs=%#v total=%d err=%v", jobs, total, err)
	}
	if _, err := db.Exec(`UPDATE chat_download_items SET status = 'paused' WHERE chat_job_id = 'stats-chat' AND status IN ('queued', 'running')`); err != nil {
		t.Fatal(err)
	}
	assertChatStats(t, db, "stats-chat", chatStats{discovered: 4, waiting: 1, completed: 1, paused: 2})
	if _, err := db.Exec(`UPDATE chat_download_items SET status = 'queued' WHERE chat_job_id = 'stats-chat' AND status IN ('paused', 'waiting')`); err != nil {
		t.Fatal(err)
	}
	assertChatStats(t, db, "stats-chat", chatStats{discovered: 4, queued: 3, completed: 1})
	if _, err := db.Exec(`DELETE FROM chat_download_items WHERE chat_job_id = 'stats-chat' AND message_id = 10`); err != nil {
		t.Fatal(err)
	}
	assertChatStats(t, db, "stats-chat", chatStats{discovered: 3, queued: 2, completed: 1})
	if _, err := db.Exec(`DELETE FROM chat_download_jobs WHERE id = 'stats-chat'`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_stats WHERE chat_job_id = 'stats-chat'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("purged summary rows=%d err=%v, want 0", count, err)
	}
}

// A missing downloaded_media row is the ordinary first-seen case. Regression
// coverage ensures it is claimed and indexed rather than escaping as
// sql.ErrNoRows through the Telegram callback.
func TestPostgresRegisterFirstChatMediaCreatesClaim(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('first-media-chat','tg://chat','channel','channel:first-media',1,'test','account','scanning','indexing','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if err := m.registerChatMedia("first-media-chat", []source{{Item: Item{DialogType: "channel", DialogKey: "channel:first-media", DialogID: 1, MessageID: 42, OriginalName: "first.bin"}, DialogName: "test", MediaType: "image"}}, false); err != nil {
		t.Fatalf("registerChatMedia() first media: %v", err)
	}
	var itemStatus, ownerKind, ownerID string
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'first-media-chat' AND message_id = 42`).Scan(&itemStatus); err != nil || itemStatus != "queued" {
		t.Fatalf("indexed media status=%q err=%v, want queued", itemStatus, err)
	}
	if err := db.QueryRow(`SELECT owner_kind, owner_id FROM downloaded_media WHERE dialog_key = 'channel:first-media' AND message_id = 42`).Scan(&ownerKind, &ownerID); err != nil || ownerKind != "chat" || ownerID != "first-media-chat" {
		t.Fatalf("claim=%q/%q err=%v, want chat/first-media-chat", ownerKind, ownerID, err)
	}
}

func TestPostgresChatReplyRootSurvivesFilePolicy(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// The snapshot permits only images; this discovered document must not be
	// indexed, while its root still has to remain available for a later image
	// posted to the same discussion thread.
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('filtered-reply-chat','tg://chat','channel','channel:root',1,'test','account','scanning','indexing','{"fileTypes":["image"],"includeReplies":true}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{DialogType: "chat", DialogKey: "chat:discussion", DialogID: 2, MessageID: 42, OriginalName: "filtered.bin", IsComment: true, ReplyRootID: 7, OriginMessageID: 11}, DialogName: "discussion", MediaType: "document"}
	if err := m.registerChatMedia("filtered-reply-chat", []source{item}, false); err != nil {
		t.Fatal(err)
	}
	var roots, indexed int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_reply_roots WHERE chat_job_id = 'filtered-reply-chat' AND discussion_dialog_key = 'chat:discussion' AND root_message_id = 7`).Scan(&roots); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'filtered-reply-chat'`).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if roots != 1 || indexed != 0 {
		t.Fatalf("roots=%d indexed=%d, want 1 and 0", roots, indexed)
	}
}

func TestPostgresMessageTaskAdoptsCompletedChatMedia(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	path := t.TempDir() + "/ready.bin"
	if err := os.WriteFile(path, []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('message-adopt','tg://message','channel','channel:shared','test','account','queued',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('message-adopt','channel','channel:shared',1,7,'ready.bin','queued')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES ('channel:shared',7,?,'completed','chat','chat-owner',?)`, path, now); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{DialogKey: "channel:shared", MessageID: 7}}
	claim, adoptedPath, err := m.claimMessageMedia("message-adopt", item)
	if err != nil || claim != "completed" || adoptedPath != path {
		t.Fatalf("claimMessageMedia()=%q,%q,%v; want completed,%q,nil", claim, adoptedPath, err, path)
	}
	if err := m.adoptCompletedMessageItem("message-adopt", item, adoptedPath); err != nil {
		t.Fatal(err)
	}
	if !m.allItemsCompleted("message-adopt") {
		t.Fatal("message item was not adopted as completed")
	}
}

func TestPostgresMessageWaitsForChatClaimAndChatCanTakeFailedMessageClaim(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('message-wait','tg://message','channel','channel:waiting','test','account','queued',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('message-wait','channel','channel:waiting',1,8,'wait.bin','queued')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:waiting',8,'claimed','chat','chat-owner',?)`, now); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{DialogKey: "channel:waiting", MessageID: 8}}
	claim, _, err := m.claimMessageMedia("message-wait", item)
	if err != nil || claim != "waiting" {
		t.Fatalf("chat-owned media claim=%q err=%v, want waiting", claim, err)
	}
	if err := m.setMessageItemWaiting("message-wait", item); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT status FROM download_items WHERE job_id = 'message-wait'`).Scan(&state); err != nil || state != "waiting" {
		t.Fatalf("message state=%q err=%v, want waiting", state, err)
	}
	if _, err := db.Exec(`UPDATE downloaded_media SET status='claimed', owner_kind='message', owner_id='message-wait' WHERE dialog_key='channel:waiting' AND message_id=8`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE download_items SET status='failed' WHERE job_id='message-wait'`); err != nil {
		t.Fatal(err)
	}
	m.releaseFailedMessageClaims("message-wait")
	claim, _, err = m.claimChatMedia("chat-next", item)
	if err != nil || claim != "queued" {
		t.Fatalf("released message claim=%q err=%v, want queued for chat", claim, err)
	}
}

func TestPostgresStalledChatBatchRequeuesOnlyUnfinishedMedia(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('stalled-chat','tg://chat','channel','channel:stalled',1,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, started_at, discovered_at) VALUES ('stalled-chat','channel:stalled',1,'stuck.bin','running',?,?), ('stalled-chat','channel:stalled',2,'done.bin','completed','',?), ('stalled-chat','channel:stalled',3,'retry-limit.bin','queued','',?)`, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chat_download_items SET attempts = ? WHERE chat_job_id = 'stalled-chat' AND message_id = 3`, maxStalledAttempts); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:stalled',1,'claimed','chat','stalled-chat',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:stalled',3,'claimed','chat','stalled-chat',?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := m.requeueStalledChatBatch("stalled-chat", []source{{Item: Item{DialogKey: "channel:stalled", MessageID: 1}}, {Item: Item{DialogKey: "channel:stalled", MessageID: 2}}, {Item: Item{DialogKey: "channel:stalled", MessageID: 3}}}); err != nil {
		t.Fatal(err)
	}
	var running, completed, claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'stalled-chat' AND status = 'queued'`).Scan(&running); err != nil || running != 1 {
		t.Fatalf("queued=%d err=%v, want 1", running, err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'stalled-chat' AND status = 'completed'`).Scan(&completed); err != nil || completed != 1 {
		t.Fatalf("completed=%d err=%v, want 1", completed, err)
	}
	var failed int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'stalled-chat' AND status = 'failed'`).Scan(&failed); err != nil || failed != 1 {
		t.Fatalf("failed=%d err=%v, want 1", failed, err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:stalled'`).Scan(&claims); err != nil || claims != 0 {
		t.Fatalf("claims=%d err=%v, want 0", claims, err)
	}
}

func TestPostgresChatPublishedFileIsReconciledWithoutRestart(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	path := t.TempDir() + "/published.bin"
	if err := os.WriteFile(path, []byte("published"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('reconcile-chat','tg://chat','channel','channel:reconcile',1,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, final_path, status, discovered_at) VALUES ('reconcile-chat','channel:reconcile',7,'published.bin',?,'downloaded',?)`, path, now); err != nil {
		t.Fatal(err)
	}
	if err := m.reconcileChatPublishedItems(); err != nil {
		t.Fatal(err)
	}
	var itemStatus, mediaStatus, finalPath string
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'reconcile-chat' AND message_id = 7`).Scan(&itemStatus); err != nil || itemStatus != "completed" {
		t.Fatalf("chat item status=%q err=%v, want completed", itemStatus, err)
	}
	if err := db.QueryRow(`SELECT status, final_path FROM downloaded_media WHERE dialog_key = 'channel:reconcile' AND message_id = 7`).Scan(&mediaStatus, &finalPath); err != nil || mediaStatus != "completed" || finalPath != path {
		t.Fatalf("global media=%q/%q err=%v, want completed/%q", mediaStatus, finalPath, err, path)
	}
}

func TestPostgresSummaryIncludesChatDownloadItems(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('summary-message','tg://message','queued',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_key, dialog_id, message_id, original_name, status, finished_at) VALUES ('summary-message','channel:summary',1,1,'message.bin','queued',''), ('summary-message','channel:summary',1,2,'failed.bin','failed',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('summary-chat','tg://chat','channel','channel:chat-summary',2,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, finished_at, discovered_at) VALUES ('summary-chat','channel:chat-summary',1,'chat.bin','running','',?), ('summary-chat','channel:chat-summary',2,'chat-failed.bin','failed',?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	active, failed := m.Summary()
	if active != 2 || failed != 2 {
		t.Fatalf("Summary()=%d active, %d failed; want 2, 2", active, failed)
	}
}

func wantDownloadItemStatus(t *testing.T, db *database, jobID, want string) {
	t.Helper()
	var state string
	if err := db.QueryRow(`SELECT status FROM download_items WHERE job_id = ?`, jobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != want {
		t.Fatalf("download_items status for %s = %q; want %q", jobID, state, want)
	}
}

// A paused chat task transfers nothing, so it must not keep owning media in
// downloaded_media. A message task waits on the claim rather than on the owner's
// task status, so a claim left behind after pausing would stall the message task
// until the chat task was resumed. This asserts the claim is released and the
// message task is promoted by reconciliation alone, with no resume.
func TestPostgresPausingChatReleasesClaimsSoWaitingMessageTaskProceeds(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES ('chat-pause','tg://chat','channel','channel:pause',1,'test','account','downloading','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('message-blocked','tg://message','channel','channel:pause','test','account','queued',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('message-blocked','channel','channel:pause',1,9,'pause.bin','queued')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:pause',9,'claimed','chat','chat-pause',?)`, now); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{DialogKey: "channel:pause", MessageID: 9}}
	claim, _, err := m.claimMessageMedia("message-blocked", item)
	if err != nil || claim != "waiting" {
		t.Fatalf("claimMessageMedia()=%q,%v; want waiting while the chat task owns the media", claim, err)
	}
	if err := m.setMessageItemWaiting("message-blocked", item); err != nil {
		t.Fatal(err)
	}
	// While the chat task is active its claim is legitimate, so reconciliation
	// must leave the waiting item exactly where it is.
	if promoted, _, err := m.reconcileMessageClaims(0); err != nil || promoted != 0 {
		t.Fatalf("reconcileMessageClaims()=%d,%v while the chat claim is held; want 0 promotions", promoted, err)
	}
	wantDownloadItemStatus(t, db, "message-blocked", "waiting")

	if err := m.PauseChat("chat-pause"); err != nil {
		t.Fatalf("PauseChat(): %v", err)
	}
	var claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:pause' AND message_id = 9`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("paused chat task still owns %d claim(s) on the waiting media; want 0", claims)
	}
	// The released claim must be promoted by reconciliation alone.
	if promoted, _, err := m.reconcileMessageClaims(0); err != nil || promoted != 1 {
		t.Fatalf("reconcileMessageClaims()=%d,%v after the claim was released; want 1 promotion", promoted, err)
	}
	wantDownloadItemStatus(t, db, "message-blocked", "queued")
	// Reconciling a released claim also transfers ownership of it, so a resumed
	// chat task must wait for the message task instead of downloading the same
	// media a second time. This is what keeps pause safe: releasing the claim
	// moves the file to whoever is still working, it does not duplicate it.
	chatClaim, _, err := m.claimChatMedia("chat-pause", item)
	if err != nil || chatClaim != "waiting" {
		t.Fatalf("claimChatMedia()=%q,%v after pause and re-claim; want waiting", chatClaim, err)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func downloadItemState(db *database, jobID string) string {
	var state string
	if err := db.QueryRow(`SELECT status FROM download_items WHERE job_id = ?`, jobID).Scan(&state); err != nil {
		return ""
	}
	return state
}

// seedWaitingItem inserts one waiting message item owned by jobID, optionally
// blocked by a claim held by chatOwner.
func seedWaitingItem(t *testing.T, db *database, jobID, dialogKey string, messageID int, chatOwner string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, error) VALUES (?,'channel',?,1,?,'wait.bin','waiting','等待其他任务完成同一文件')`, jobID, dialogKey, messageID); err != nil {
		t.Fatal(err)
	}
	if chatOwner == "" {
		return
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?,?,'claimed','chat',?,?)`, dialogKey, messageID, chatOwner, now); err != nil {
		t.Fatal(err)
	}
}

func seedReconcileJob(t *testing.T, db *database, jobID, dialogKey, status string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES (?,'tg://message','channel',?,'test','account',?,?,?)`, jobID, dialogKey, status, now, now); err != nil {
		t.Fatal(err)
	}
}

// Waiting items are promoted by a dedicated goroutine now, not by the download
// workers. This asserts promotion happens with no worker running at all, and
// that a wake signal is enough to notice a claim released a moment earlier.
func TestPostgresReconcileWorkerPromotesWaitingItemWithoutDownloadWorkers(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, workerCount), reconcileWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	m.updateDatabaseHealth()
	if !m.DatabaseAvailable() {
		t.Fatal("test requires a reachable database; the reconciler skips its work otherwise")
	}
	seedReconcileJob(t, db, "message-reconcile", "channel:reconcile", "queued")
	seedWaitingItem(t, db, "message-reconcile", "channel:reconcile", 11, "chat-owner")

	ctx, cancel := context.WithCancel(context.Background())
	// The worker must be fully stopped before this test returns: these tests
	// share one database, and a pass still in flight would mutate rows belonging
	// to whichever test runs next.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		m.reconcileWorker(ctx)
	}()
	defer func() {
		cancel()
		<-stopped
	}()
	// While the claim is genuinely held the item must stay waiting.
	time.Sleep(300 * time.Millisecond)
	wantDownloadItemStatus(t, db, "message-reconcile", "waiting")

	// The owning chat task finishes or releases the transfer.
	if _, err := db.Exec(`DELETE FROM downloaded_media WHERE dialog_key = 'channel:reconcile' AND message_id = 11`); err != nil {
		t.Fatal(err)
	}
	m.signalReconcile()
	// The window must stay below reconcileIdleInterval. With a longer one this
	// test passes even when the wake path is entirely broken, because the
	// reconciler's own idle poll would promote the item within the window and the
	// assertion could not tell the two apart.
	waitForCondition(t, reconcileIdleInterval/2, "the reconciler to promote the released claim on a wake signal", func() bool {
		return downloadItemState(db, "message-reconcile") == "queued"
	})
}

// A blocked head must not hide a promotable item behind it. The previous query
// ordered by the owner's updated_at under a fixed LIMIT, so the same blocked
// rows filled the window on every pass and anything past them was never read.
func TestPostgresReconcileCursorRotatesPastBlockedItems(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, workerCount)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	seedReconcileJob(t, db, "message-rotate", "channel:rotate", "queued")
	total := reconcileBatchSize + 4
	for i := 1; i <= total; i++ {
		seedWaitingItem(t, db, "message-rotate", "channel:rotate", i, "chat-owner")
	}
	// Only the last item, which sits past one full batch, becomes available.
	if _, err := db.Exec(`DELETE FROM downloaded_media WHERE dialog_key = 'channel:rotate' AND message_id = ?`, total); err != nil {
		t.Fatal(err)
	}

	promoted, cursor, err := m.reconcileMessageClaims(0)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 0 {
		t.Fatalf("first pass promoted %d; want 0 because every item in the batch is still claimed", promoted)
	}
	if cursor == 0 {
		t.Fatal("first pass did not advance the cursor past the blocked batch; a blocked head would be re-examined forever")
	}

	promoted, _, err = m.reconcileMessageClaims(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 1 {
		t.Fatalf("second pass from the cursor promoted %d; want 1, the released item behind the blocked batch", promoted)
	}
	var state string
	if err := db.QueryRow(`SELECT status FROM download_items WHERE job_id = 'message-rotate' AND message_id = ?`, total).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "queued" {
		t.Fatalf("item %d state = %q; want queued", total, state)
	}
}

// A task that is not queued can never start a transfer, so taking a claim for it
// would block the chat task that can still finish the file, on behalf of a
// download that will never run. It must still adopt a file someone else
// completed.
func TestPostgresReconcileNeverClaimsForNonQueuedTaskButStillAdopts(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, workerCount)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	// A job that failed partway: it is not queued, so it cannot consume a claim.
	seedReconcileJob(t, db, "message-partial", "channel:partial", "partial")
	seedWaitingItem(t, db, "message-partial", "channel:partial", 21, "")

	promoted, _, err := m.reconcileMessageClaims(0)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 0 {
		t.Fatalf("promoted %d waiting items for a non-queued job; want 0", promoted)
	}
	var claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:partial' AND message_id = 21`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("a non-queued task acquired %d claim(s) on a file it will never download; want 0", claims)
	}

	// The same job must still adopt media another task finished.
	path := t.TempDir() + "/done.bin"
	if err := os.WriteFile(path, []byte("done"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES ('channel:partial',21,?,'completed','chat','chat-owner',?)`, path, now); err != nil {
		t.Fatal(err)
	}
	promoted, _, err = m.reconcileMessageClaims(0)
	if err != nil {
		t.Fatal(err)
	}
	if promoted != 1 {
		t.Fatalf("adopting a completed file promoted %d items; want 1", promoted)
	}
	wantDownloadItemStatus(t, db, "message-partial", "completed")
}

// A status-filtered total is an index-only count over permanent history that a
// filtered list asks for on every page. It is memoized briefly and is display
// only, since pagination uses cursors. This proves the memo is real and keyed
// per status, so one status can never answer for another.
func TestPostgresFilteredJobTotalIsMemoizedPerStatus(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	seedReconcileJob(t, db, "count-a", "channel:count", "completed")
	seedReconcileJob(t, db, "count-b", "channel:count", "completed")
	total, err := m.visibleJobTotal("completed")
	if err != nil || total != 2 {
		t.Fatalf("visibleJobTotal(completed)=%d,%v; want 2", total, err)
	}
	// A row inserted inside the memo window must not be counted yet.
	seedReconcileJob(t, db, "count-c", "channel:count", "completed")
	if total, err = m.visibleJobTotal("completed"); err != nil || total != 2 {
		t.Fatalf("visibleJobTotal(completed)=%d,%v inside the memo window; want the cached 2", total, err)
	}
	// A different status must be counted independently rather than served from
	// the memo entry belonging to "completed".
	seedReconcileJob(t, db, "count-d", "channel:count", "failed")
	if total, err = m.visibleJobTotal("failed"); err != nil || total != 1 {
		t.Fatalf("visibleJobTotal(failed)=%d,%v; want 1", total, err)
	}
}

// A session task's terminal state has to say what actually happened, and it has
// to avoid concluding while files are merely held by another task.
//
//   - Every file failed: "failed". Calling that "partial" claims some of the
//     task succeeded, which is exactly what a message task refuses to say.
//   - Some files published: "partial".
//   - Nothing finished but files are waiting on another task's claim: not
//     terminal at all. They are promoted once that claim is released, and the
//     promotion pass only looks at a task that can still run, so concluding here
//     would strand them permanently.
func TestPostgresChatStateConcludesOnlyWhenNothingIsLeft(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		id       string
		statuses []string
		want     string
	}{
		{"state-all-failed", []string{"failed", "failed"}, ChatStatusFailed},
		{"state-mixed", []string{"completed", "failed"}, ChatStatusPartial},
		{"state-all-waiting", []string{"waiting", "waiting"}, ChatStatusDownloading},
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, testCase := range cases {
		if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES (?,'tg://chat','channel',?,1,'test','account','downloading','completed',?,?)`, testCase.id, "channel:"+testCase.id, now, now); err != nil {
			t.Fatal(err)
		}
		for index, status := range testCase.statuses {
			if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, dialog_type, dialog_id, status, discovered_at) VALUES (?,?,?,'channel',1,?,?)`, testCase.id, "channel:"+testCase.id, index+1, status, now); err != nil {
				t.Fatal(err)
			}
		}
	}

	m.refreshChatStates()

	for _, testCase := range cases {
		var got string
		if err := db.QueryRow(`SELECT status FROM chat_download_jobs WHERE id = ?`, testCase.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != testCase.want {
			t.Fatalf("%s with items %v: got status %q, want %q", testCase.id, testCase.statuses, got, testCase.want)
		}
	}
}

// Giving up on a listener event used to be permanent: the claim query only ever
// reads 'pending' rows, so a 'failed' row was unreachable forever and the
// message it carried was silently dropped. Being told about the same message
// again reopens it, the way a repeated reaction reopens its own inbox row.
func TestPostgresChatInboxReopensFailedEventOnRedelivery(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), chatEventWake: make(chan struct{}, 1),
		chatWatched: make(map[string]map[string]struct{})}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	m.updateDatabaseHealth()

	peer := &tg.InputPeerChannel{ChannelID: 77, AccessHash: 88}
	_, key, _ := dialogIdentity(peer, "account")
	// The message is a watched dialog, so admission reaches the insert.
	m.chatWatched["account"] = map[string]struct{}{key: {}}
	m.listenerSnapshotReady = true
	event := telegram.NewMessageEvent{AccountID: "account", DialogID: 77, MessageID: 500, InputPeer: peer}

	if err := m.admitChatMessage(event); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chat_message_inbox SET status = 'failed', attempts = 5, error = 'boom' WHERE message_id = 500`); err != nil {
		t.Fatal(err)
	}
	if err := m.admitChatMessage(event); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts FROM chat_message_inbox WHERE message_id = 500`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 0 {
		t.Fatalf("a redelivered message must reopen its failed event, got status=%q attempts=%d", status, attempts)
	}

	// A finished event must not be resurrected by a duplicate delivery.
	if _, err := db.Exec(`UPDATE chat_message_inbox SET status = 'done' WHERE message_id = 500`); err != nil {
		t.Fatal(err)
	}
	if err := m.admitChatMessage(event); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM chat_message_inbox WHERE message_id = 500`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("a completed event must stay completed, got %q", status)
	}
}

// nextQueued only selects a queued job that still has a queued item, so a job
// left queued with every item already terminal is never claimed again: it sits
// at "排队中" forever with nothing to do. Interrupted-task recovery produces
// exactly that shape, because it only settles a job whose items all completed.
func TestPostgresSettleQueuedJobsWithoutPendingWork(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), downloadDir: t.TempDir()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		id       string
		statuses []string
		want     string
	}{
		{"stranded-failed", []string{"failed", "failed"}, "failed"},
		{"stranded-partial", []string{"completed", "failed"}, "partial"},
		// Waiting is work in progress owned by the claim reconciler, and a queued
		// item means the scheduler will pick the job up by itself.
		{"stranded-waiting", []string{"waiting"}, "queued"},
		{"stranded-queued", []string{"queued"}, "queued"},
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, testCase := range cases {
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES (?,'tg://message','channel',?,'test','account','queued',?,?)`, testCase.id, "channel:"+testCase.id, now, now); err != nil {
			t.Fatal(err)
		}
		for index, status := range testCase.statuses {
			if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES (?,'channel',?,1,?,?,?)`, testCase.id, "channel:"+testCase.id, index+1, "file.bin", status); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Drive the repair loop rather than the helper, so a stranded job is only
	// settled if the reconciler actually reaches it.
	m.updateDatabaseHealth()
	if !m.DatabaseAvailable() {
		t.Fatal("the repair loop skips its passes while the database is not connected")
	}
	// Run the real loop and wait for the observable effect instead of stopping
	// it up front: the drain pass promotes the waiting case, and a cancelled
	// context makes the loop return from inside that drain before it ever
	// reaches the settle pass.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		m.reconcileWorker(ctx)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var settled string
		if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'stranded-failed'`).Scan(&settled); err == nil && settled != "queued" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the repair loop never settled a queued job with nothing pending")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-stopped

	for _, testCase := range cases {
		var got string
		if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = ?`, testCase.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != testCase.want {
			t.Fatalf("%s with items %v: got status %q, want %q", testCase.id, testCase.statuses, got, testCase.want)
		}
	}
}

// seedQueuedJobForAccount creates one queued message task with a single queued
// item, for the given account, using an explicit fixed-width timestamp so the
// scheduler's created_at ordering is deterministic.
func seedQueuedJobForAccount(t *testing.T, db *database, jobID, dialogKey, accountID, createdAt string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES (?,'tg://message','channel',?,'test',?,'queued',?,?)`, jobID, dialogKey, accountID, createdAt, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES (?,'channel',?,1,1,'cooldown.bin','queued')`, jobID, dialogKey); err != nil {
		t.Fatal(err)
	}
}

// File transfer used to run with no knowledge of the account-wide Telegram
// cooldown: recordTelegramRPCError persisted the window, but the scheduler
// claimed the task again immediately, re-opened a client and hit the same
// flood. The scheduler must now step over a blocked account — and must not let
// one blocked account stall every other account's queued work.
func TestPostgresNextQueuedSkipsAccountsInsideCooldown(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// rpcState must be initialized: the zero-value map would panic on write.
	m := &Manager{db: db, events: newEventBus(), rpcState: make(map[string]*telegramRPCState), wake: make(chan struct{}, workerCount), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM telegram_rate_limits`); err != nil {
		t.Fatal(err)
	}
	seedQueuedJobForAccount(t, db, "cooldown-older", "channel:blocked", "account-blocked", "2026-01-01T00:00:00Z")
	seedQueuedJobForAccount(t, db, "cooldown-newer", "channel:free", "account-free", "2026-01-01T00:00:01Z")

	// With no cooldown the oldest task wins, which is the normal FIFO order.
	job, _, err := m.nextQueued()
	if err != nil || job.ID != "cooldown-older" {
		t.Fatalf("nextQueued()=%q,%v before any cooldown; want cooldown-older", job.ID, err)
	}

	// Telegram tells us to wait. The record is account-wide by design and is
	// persisted, not only kept in memory.
	if !m.recordTelegramRPCError("account-blocked", errors.New("FLOOD_WAIT_3600")) {
		t.Fatal("recordTelegramRPCError() did not record the flood window")
	}
	job, _, err = m.nextQueued()
	if err != nil || job.ID != "cooldown-newer" {
		t.Fatalf("nextQueued()=%q,%v while the older task's account is blocked; want the scheduler to step over it to cooldown-newer", job.ID, err)
	}

	// The persisted cooldown must exclude the account on its own, without the
	// in-memory guard: that guard is best effort, and a process restart (or a
	// different instance) only has the stored row to go on.
	m.rpcMu.Lock()
	m.rpcState["account-blocked"].blockedUntil = time.Time{}
	m.rpcMu.Unlock()
	job, _, err = m.nextQueued()
	if err != nil || job.ID != "cooldown-newer" {
		t.Fatalf("nextQueued()=%q,%v with only the persisted cooldown set; want the stored blocked_until to keep excluding it", job.ID, err)
	}

	// Once the window is cleared the skipped task is eligible again.
	if _, err := db.Exec(`DELETE FROM telegram_rate_limits WHERE account_id = 'account-blocked'`); err != nil {
		t.Fatal(err)
	}
	job, _, err = m.nextQueued()
	if err != nil || job.ID != "cooldown-older" {
		t.Fatalf("nextQueued()=%q,%v after the cooldown was cleared; want cooldown-older", job.ID, err)
	}
}

// Every flood retry also extends the account cooldown it is waiting for, so an
// uncapped retry loop makes the restriction strictly worse. The retry budget
// mirrors the stalled-item cap the chat queue already uses.
func TestPostgresRateLimitedTaskStopsRetryingAtAttemptCap(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), rpcState: make(map[string]*telegramRPCState)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	seed := func(jobID string, attempts int) []source {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, attempts, created_at, updated_at) VALUES (?,'tg://message','channel',?,'test','account','running',?,?,?)`, jobID, "channel:"+jobID, attempts, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES (?,'channel',?,1,1,'flood.bin','running')`, jobID, "channel:"+jobID); err != nil {
			t.Fatal(err)
		}
		return []source{{Item: Item{DialogKey: "channel:" + jobID, MessageID: 1}}}
	}
	cause := errors.New("FLOOD_WAIT_3600")

	// Budget remaining: the task goes back to the queue to wait out the window.
	retrySources := seed("flood-retry", maxStalledAttempts-1)
	if err := m.requeueRateLimitedMessage("flood-retry", retrySources, cause); err != nil {
		t.Fatal(err)
	}
	if state := m.status("flood-retry"); state != "queued" {
		t.Fatalf("attempts below the cap left the task in %q; want queued", state)
	}
	wantDownloadItemStatus(t, db, "flood-retry", "queued")

	// Budget exhausted: stop retrying instead of extending the cooldown forever.
	spentSources := seed("flood-spent", maxStalledAttempts)
	if err := m.requeueRateLimitedMessage("flood-spent", spentSources, cause); err != nil {
		t.Fatal(err)
	}
	if state := m.status("flood-spent"); state != "failed" {
		t.Fatalf("attempts at the cap left the task in %q; want failed", state)
	}
	wantDownloadItemStatus(t, db, "flood-spent", "failed")
}

func seedProcessingInboxes(t *testing.T, db *database, updatedAt string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, emoji, status, next_attempt_at, created_at, updated_at) VALUES ('account','channel:inbox',1,'channel','👍','processing',?,?,?)`, updatedAt, updatedAt, updatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, status, next_attempt_at, created_at, updated_at) VALUES ('account','channel:inbox',1,'channel','processing',?,?,?)`, updatedAt, updatedAt, updatedAt); err != nil {
		t.Fatal(err)
	}
}

func inboxStates(t *testing.T, db *database) (reaction, chat string) {
	t.Helper()
	if err := db.QueryRow(`SELECT status FROM reaction_inbox LIMIT 1`).Scan(&reaction); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM chat_message_inbox LIMIT 1`).Scan(&chat); err != nil {
		t.Fatal(err)
	}
	return reaction, chat
}

// A row claimed by a worker that then lost the database stays 'processing'
// forever: the submit fails and so does the retry write. Startup resets these,
// but startup alone means a database blip strands every claimed trigger until
// the next restart, so recovery must reset them too.
func TestPostgresOutageRecoveryReleasesInboxLeases(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore(), rpcState: make(map[string]*telegramRPCState),
		wake: make(chan struct{}, workerCount), chatWake: make(chan struct{}, 1), reconcileWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	seedProcessingInboxes(t, db, now)

	if err := m.recoverAfterDatabaseOutage(); err != nil {
		t.Fatalf("recoverAfterDatabaseOutage(): %v", err)
	}
	reaction, chat := inboxStates(t, db)
	if reaction != "pending" || chat != "pending" {
		t.Fatalf("after recovery inbox states = reaction:%q chat:%q; want both pending", reaction, chat)
	}
}

// A worker that dies mid-submit without a database outage would otherwise strand
// its row until the next restart. The lease timeout is the safety net for that
// case, and it must not touch a lease a healthy worker still holds.
func TestPostgresStaleInboxLeaseIsReapedButFreshLeaseIsKept(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().UTC().Add(-inboxLeaseTimeout - time.Minute).Format(time.RFC3339Nano)
	seedProcessingInboxes(t, db, stale)
	if err := m.reapStaleInboxLeases(); err != nil {
		t.Fatalf("reapStaleInboxLeases(): %v", err)
	}
	reaction, chat := inboxStates(t, db)
	if reaction != "pending" || chat != "pending" {
		t.Fatalf("stale lease states = reaction:%q chat:%q; want both reaped to pending", reaction, chat)
	}

	// A lease inside the timeout belongs to a worker that may still be working.
	if _, err := db.Exec(`UPDATE reaction_inbox SET status = 'processing', updated_at = ?`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chat_message_inbox SET status = 'processing', updated_at = ?`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := m.reapStaleInboxLeases(); err != nil {
		t.Fatalf("reapStaleInboxLeases(): %v", err)
	}
	reaction, chat = inboxStates(t, db)
	if reaction != "processing" || chat != "processing" {
		t.Fatalf("fresh lease states = reaction:%q chat:%q; want both left processing", reaction, chat)
	}
}

// A process that dies between the final file move and the completion write
// leaves the item at 'downloaded' and the ownership row at 'claimed'. Completing
// only the item heals the symptom and hides the damage: a waiting task tests the
// claim, not the item status, so the published file can never be adopted again —
// and releaseFailedMessageClaims cannot undo it, because the item is no longer
// failed or cancelled. Recovery must repair the ownership row too.
func TestPostgresPublishedFileReconcileAlsoHealsOwnershipRow(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/published.bin"
	if err := os.WriteFile(path, []byte("done"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('crash-msg','tg://m','channel','channel:crash','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, final_path) VALUES ('crash-msg','channel','channel:crash',1,55,'published.bin','downloaded',?)`, path); err != nil {
		t.Fatal(err)
	}
	// The claim the crashed process never promoted.
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES ('channel:crash',55,'','claimed','message','crash-msg',?)`, now); err != nil {
		t.Fatal(err)
	}

	if err := m.reconcilePublishedItems(); err != nil {
		t.Fatalf("reconcilePublishedItems(): %v", err)
	}

	wantDownloadItemStatus(t, db, "crash-msg", "completed")
	var mediaStatus, finalPath string
	if err := db.QueryRow(`SELECT status, final_path FROM downloaded_media WHERE dialog_key = 'channel:crash' AND message_id = 55`).Scan(&mediaStatus, &finalPath); err != nil {
		t.Fatal(err)
	}
	if mediaStatus != "completed" {
		t.Fatalf("ownership row status = %q after recovery; want completed so the published file can be adopted", mediaStatus)
	}
	if finalPath != path {
		t.Fatalf("ownership row final_path = %q; want %q", finalPath, path)
	}

	// The end-to-end consequence: another task must be able to adopt the file
	// instead of waiting on a claim its owner abandoned.
	item := source{Item: Item{DialogKey: "channel:crash", MessageID: 55}}
	claim, adoptedPath, err := m.claimChatMedia("chat-wants", item)
	if err != nil || claim != "completed" || adoptedPath != path {
		t.Fatalf("claimChatMedia()=%q,%q,%v; want the published file to be adoptable", claim, adoptedPath, err)
	}
}

// "Something exists at the destination" and "this media is already downloaded"
// are different questions, and conflating them produces a failure no retry can
// clear: a directory or dangling symlink at the destination is not a published
// file, yet publishing still refuses to overwrite it, so every retry downloads
// the whole file again and fails identically. Both outcomes must be
// distinguishable and must name the path to clear.
func TestPostgresPublishRefusesNonRegularDestinationWithActionableError(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := t.TempDir()
	m := &Manager{db: db, downloadDir: root, events: newEventBus(), progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	const pattern = "{{ .MessageID }}_blocked.bin"
	config := settings.Defaults()
	config.Download.FinalFilenameTemplate = pattern
	item := source{Item: Item{DialogKey: "channel:blocked", MessageID: 7, OriginalName: "blocked.bin"}}
	finalPath, err := finalDestination(root, pattern, item)
	if err != nil {
		t.Fatal(err)
	}
	// A directory occupies the destination without being a published file.
	if err := os.MkdirAll(finalPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if regularFileExists(finalPath) {
		t.Fatal("a directory must not count as an already-published file")
	}
	tempPath := filepath.Join(root, "incoming.bin")
	if err := os.WriteFile(tempPath, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('blocked-dest','tg://m','channel','channel:blocked','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('blocked-dest','channel','channel:blocked',1,7,'blocked.bin','queued')`); err != nil {
		t.Fatal(err)
	}

	err = m.publishItem("blocked-dest", tempPath, item, config)
	if err == nil {
		t.Fatal("publishItem() succeeded over an occupied destination; want a refusal")
	}
	if !strings.Contains(err.Error(), finalPath) {
		t.Fatalf("publishItem() error %q does not name the blocking path %q, so the operator cannot act on it", err.Error(), finalPath)
	}
	wantDownloadItemStatus(t, db, "blocked-dest", "failed")
	var saved string
	if err := db.QueryRow(`SELECT error FROM download_items WHERE job_id = 'blocked-dest'`).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saved, "不是普通文件") {
		t.Fatalf("saved item error = %q; want it to distinguish a non-regular occupant from an existing download", saved)
	}
	// The transfer was refused, so the source file must not have been moved.
	if _, statErr := os.Stat(tempPath); statErr != nil {
		t.Fatalf("the incoming file was consumed despite the refusal: %v", statErr)
	}
}

// Stop has to end the worker loops. They are not driven by a task context, so
// cancelling transfers leaves them polling — and the poll after Stop closes the
// database is an error against a closed pool, repeated for the whole shutdown
// window until the process happens to exit.
func TestPostgresStopEndsEveryWorkerLoop(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// chatWorker reaches the settings store for the listener reconcile, so the
	// manager needs one the way New would provide it.
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{db: db, settings: store, events: newEventBus(), progress: newProgressStore(),
		stopCh: make(chan struct{}), wake: make(chan struct{}, workerCount), chatWake: make(chan struct{}, 1),
		chatDownloadWake: []chan struct{}{make(chan struct{}, 1)},
		chatEventWake:    make(chan struct{}, 1), slotWake: make(chan struct{}, 1),
		rpcState: make(map[string]*telegramRPCState), chatActive: make(map[string]struct{}),
		chatCancels: make(map[string]map[uint64]context.CancelFunc), cancels: make(map[string]context.CancelFunc)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	m.updateDatabaseHealth()

	loops := map[string]func(){
		"worker":             m.worker,
		"chatWorker":         m.chatWorker,
		"chatDownloadWorker": func() { m.chatDownloadWorker(m.chatDownloadWake[0]) },
		"chatEventWorker":    m.chatEventWorker,
	}
	done := make(chan string, len(loops))
	for name, loop := range loops {
		go func(name string, loop func()) {
			loop()
			done <- name
		}(name, loop)
	}
	// Let each loop reach its wait before stopping, so this measures the stop
	// signal rather than a race with the first iteration.
	time.Sleep(300 * time.Millisecond)
	m.Stop()

	exited := make(map[string]bool, len(loops))
	deadline := time.After(10 * time.Second)
	for len(exited) < len(loops) {
		select {
		case name := <-done:
			exited[name] = true
		case <-deadline:
			missing := make([]string, 0, len(loops))
			for name := range loops {
				if !exited[name] {
					missing = append(missing, name)
				}
			}
			t.Fatalf("these loops kept running after Stop(): %v", missing)
		}
	}
}

func seedWaitingChatItem(t *testing.T, db *database, chatID, dialogKey string, messageID int, mediaOwner string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, discovered_at) VALUES (?,?,?,'wait.bin','waiting',?)`, chatID, dialogKey, messageID, now); err != nil {
		t.Fatal(err)
	}
	if mediaOwner == "" {
		return
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?,?,'claimed','chat',?,?)`, dialogKey, messageID, mediaOwner, now); err != nil {
		t.Fatal(err)
	}
}

func seedChatJobWithStatus(t *testing.T, db *database, id, dialogKey, status string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES (?,'tg://c','channel',?,1,'test','account',?,'completed',?,?)`, id, dialogKey, status, now, now); err != nil {
		t.Fatal(err)
	}
}

// The chat waiting-item scan must rotate like the message one. A blocked head
// that fills the whole LIMIT window would otherwise be re-read on every pass
// while everything behind it went unexamined — and an item whose blocker has
// been released would never be promoted.
func TestPostgresChatReconcileCursorRotatesPastBlockedItems(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore(), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	seedChatJobWithStatus(t, db, "chat-rotate", "channel:chatrotate", ChatStatusDownloading)
	total := chatClaimBatchSize + 4
	for i := 1; i <= total; i++ {
		seedWaitingChatItem(t, db, "chat-rotate", "channel:chatrotate", i, "other-owner")
	}
	// Only the last item, past one full batch, has its blocker released.
	if _, err := db.Exec(`DELETE FROM downloaded_media WHERE dialog_key = 'channel:chatrotate' AND message_id = ?`, total); err != nil {
		t.Fatal(err)
	}

	cursor := m.reconcileChatClaims(chatClaimCursor{})
	if cursor == (chatClaimCursor{}) {
		t.Fatal("the pass did not advance the cursor past the blocked batch; a blocked head would be re-examined forever")
	}
	var state string
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'chat-rotate' AND message_id = ?`, total).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "waiting" {
		t.Fatalf("the blocked batch already promoted item %d to %q; want it untouched on the first pass", total, state)
	}

	m.reconcileChatClaims(cursor)
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'chat-rotate' AND message_id = ?`, total).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "queued" {
		t.Fatalf("item %d state = %q after resuming from the cursor; want queued, the released item behind the blocked batch", total, state)
	}
}

// A task that cannot transfer must not be handed ownership of media: the claim
// would be held by nobody and would block every message task waiting on it,
// which is exactly the stall that releasing claims on pause fixes from the
// other side.
func TestPostgresChatReconcileRefusesToClaimForANonRunnableTask(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore(), chatWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	seedChatJobWithStatus(t, db, "chat-paused", "channel:chatpaused", ChatStatusPaused)
	seedWaitingChatItem(t, db, "chat-paused", "channel:chatpaused", 31, "")
	seedChatJobWithStatus(t, db, "chat-live", "channel:chatlive", ChatStatusDownloading)
	seedWaitingChatItem(t, db, "chat-live", "channel:chatlive", 32, "")

	m.reconcileChatClaims(chatClaimCursor{})

	var pausedClaims, liveClaims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:chatpaused' AND message_id = 31`).Scan(&pausedClaims); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:chatlive' AND message_id = 32`).Scan(&liveClaims); err != nil {
		t.Fatal(err)
	}
	if pausedClaims != 0 {
		t.Fatalf("a non-runnable task was granted %d claim(s); the media is now held by a task that will never transfer it", pausedClaims)
	}
	if liveClaims != 1 {
		t.Fatalf("a runnable task holds %d claim(s) for its waiting item; want 1 so it can proceed", liveClaims)
	}
}

// Pausing a message task must release the media it holds, exactly as PauseChat
// does. A parked task transfers nothing, and a claim it keeps is released by
// nothing else, so every other task wanting that media waits until the user
// resumes — visible as "等待其他任务完成同一文件" with no error and no timeout.
// Releasing must not cost the paused task anything: resuming re-acquires the
// claim and continues from the temporary file and upstream resume state.
func TestPostgresPausingMessageTaskReleasesClaimsWithoutLosingResume(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// downloadDir must be a temporary directory: this test writes a partial
	// download, and an empty downloadDir resolves relative to the package
	// directory, leaving the artifact inside the source tree.
	m := &Manager{db: db, downloadDir: t.TempDir(), events: newEventBus(), progress: newProgressStore(),
		cancels: make(map[string]context.CancelFunc), chatWake: make(chan struct{}, 1),
		reconcileWake: make(chan struct{}, 1), wake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('pause-msg','tg://m','channel','channel:pausemsg','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('pause-msg','channel','channel:pausemsg',1,88,'paused.bin','running')`); err != nil {
		t.Fatal(err)
	}
	// The claim this running task holds, plus the partial download it must keep.
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:pausemsg',88,'claimed','message','pause-msg',?)`, now); err != nil {
		t.Fatal(err)
	}
	tmpDir := filepath.Join(m.downloadDir, ".tdl-tmp", "pause-msg")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(tmpDir, "88_paused.bin")
	if err := os.WriteFile(partial, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := m.Pause("pause-msg"); err != nil {
		t.Fatalf("Pause(): %v", err)
	}

	var claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:pausemsg' AND message_id = 88`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("a paused task still holds %d claim(s); every other task wanting that media is blocked until it is resumed", claims)
	}
	// The paused task keeps everything it needs to resume.
	wantDownloadItemStatus(t, db, "pause-msg", "paused")
	if _, statErr := os.Stat(partial); statErr != nil {
		t.Fatalf("the partial download was discarded on pause: %v", statErr)
	}
	item := source{Item: Item{DialogKey: "channel:pausemsg", MessageID: 88}}
	// Resuming re-acquires the freed claim, so the release costs the paused task
	// nothing: it continues from its own temporary file.
	if resumedClaim, _, err := m.claimMessageMedia("pause-msg", item); err != nil || resumedClaim != "queued" {
		t.Fatalf("claimMessageMedia()=%q,%v on resume; want queued so the task continues from its temporary file", resumedClaim, err)
	}
	// Hand the media to the competing chat task instead, as would happen if the
	// pause had stood. It must now be able to proceed.
	if _, err := db.Exec(`DELETE FROM downloaded_media WHERE dialog_key = 'channel:pausemsg' AND message_id = 88`); err != nil {
		t.Fatal(err)
	}
	if otherClaim, _, err := m.claimChatMedia("chat-wants", item); err != nil || otherClaim != "queued" {
		t.Fatalf("claimChatMedia()=%q,%v after a pause; want queued so the waiting task proceeds", otherClaim, err)
	}
	// With the chat task holding it, the message task waits and will adopt the
	// finished file rather than downloading the same media a second time.
	if laterClaim, _, err := m.claimMessageMedia("pause-msg", item); err != nil || laterClaim != "waiting" {
		t.Fatalf("claimMessageMedia()=%q,%v while another task owns the media; want waiting so it adopts instead of re-downloading", laterClaim, err)
	}
}

// A task that pauses part-way and later finds another task has published the
// media adopts those files, so it never resumes its own transfer. That path
// returns before the code that removes a task's temporary directory, leaving the
// partial download on disk until the task is deleted.
func TestPostgresAdoptingEveryItemRemovesTheStaleTemporaryDirectory(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := t.TempDir()
	m := &Manager{db: db, downloadDir: root, events: newEventBus(), progress: newProgressStore(), settings: nil}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('adopt-all','tg://m','channel','channel:adopt','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('adopt-all','channel','channel:adopt',1,91,'done.bin','completed')`); err != nil {
		t.Fatal(err)
	}
	tmpDir := filepath.Join(root, ".tdl-tmp", "adopt-all")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "91_done.bin"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	m.finishTaskWithoutPendingWork("adopt-all", false)

	if state := m.status("adopt-all"); state != "completed" {
		t.Fatalf("task state = %q; want completed", state)
	}
	if _, statErr := os.Stat(tmpDir); !os.IsNotExist(statErr) {
		t.Fatalf("the task's temporary directory survived completion (stat err = %v); its partial download leaks on disk", statErr)
	}
}

// A chat item that exhausts its flood retry budget becomes terminal, and every
// other chat transition to a terminal state releases the item's global claim.
// An item that keeps owning media in downloaded_media blocks every other task
// waiting on that media for good, because a waiter tests the claim rather than
// the item's status — so the release has to share the transaction that fails it.
func TestPostgresFloodedChatItemReleasesItsClaimWhenItGivesUp(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore(), reconcileWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES ('flood-chat','tg://c','channel','channel:flood',1,'test','account','downloading','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// One item has spent its budget, the other still has retries left. Both hold
	// a claim while running.
	seed := func(dialogKey string, messageID, attempts int) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, attempts, discovered_at) VALUES ('flood-chat',?,?,'f.bin','running',?,?)`, dialogKey, messageID, attempts, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?,?,'claimed','chat','flood-chat',?)`, dialogKey, messageID, now); err != nil {
			t.Fatal(err)
		}
	}
	seed("channel:flood", 41, maxStalledAttempts)
	seed("channel:flood", 42, 1)

	if err := m.requeueFloodedChatItems("flood-chat"); err != nil {
		t.Fatalf("requeueFloodedChatItems(): %v", err)
	}

	var spentState, retryState string
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'flood-chat' AND message_id = 41`).Scan(&spentState); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = 'flood-chat' AND message_id = 42`).Scan(&retryState); err != nil {
		t.Fatal(err)
	}
	if spentState != "failed" || retryState != "queued" {
		t.Fatalf("item states = spent:%q retry:%q; want failed and queued", spentState, retryState)
	}
	var spentClaims, retryClaims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:flood' AND message_id = 41`).Scan(&spentClaims); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:flood' AND message_id = 42`).Scan(&retryClaims); err != nil {
		t.Fatal(err)
	}
	if spentClaims != 0 {
		t.Fatalf("the given-up item still owns %d claim(s); every other task waiting on that media is now blocked for good", spentClaims)
	}
	if retryClaims != 1 {
		t.Fatalf("the retrying item's claim count = %d; want 1 kept, since it will be retried", retryClaims)
	}
}

// A task left at 'running' is never claimed again, because the scheduler only
// picks 'queued' rows. The pre-flight destination check can fail every item of a
// task before any transfer starts, so every outcome of "no work selected" has to
// write a state — including the one where nothing is pending and nothing is
// waiting on another task.
func TestPostgresTaskWithNoPendingWorkIsNeverLeftRunning(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	seed := func(jobID string, itemStates ...string) {
		t.Helper()
		if err := clearPostgresDownloadTestData(db); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES (?,'tg://m','channel',?,'test','account','running',?,?)`, jobID, "channel:"+jobID, now, now); err != nil {
			t.Fatal(err)
		}
		for index, state := range itemStates {
			if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES (?,'channel',?,1,?,'f.bin',?)`, jobID, "channel:"+jobID, index+1, state); err != nil {
				t.Fatal(err)
			}
		}
	}
	check := func(jobID string, itemStates []string, waiting bool, want string) {
		t.Helper()
		seed(jobID, itemStates...)
		m.finishTaskWithoutPendingWork(jobID, waiting)
		if state := m.status(jobID); state != want {
			t.Fatalf("finishTaskWithoutPendingWork() with items %v waiting=%v left the task %q; want %q", itemStates, waiting, state, want)
		}
	}

	// The destination-blocked case: every item failed before any transfer.
	check("no-work-failed", []string{"failed", "failed"}, false, "failed")
	// Some files made it before the rest were blocked.
	check("no-work-partial", []string{"completed", "failed"}, false, "partial")
	// Everything is waiting on another task's claim.
	check("no-work-waiting", []string{"waiting"}, false, "queued")
	// Nothing left to do at all.
	check("no-work-complete", []string{"completed"}, false, "completed")
	// The caller's own flag, independent of the items: a task that observed a
	// wait during this run must return to the queue rather than be failed.
	check("no-work-flagged", []string{"failed"}, true, "queued")
}

// Admission must tell "nothing to do" apart from "try again". A filtered event
// is not an error and must never be retried; an unavailable database is an error
// the caller has to retry, because Telegram will not redeliver the update.
func TestPostgresChatInboxAdmissionDistinguishesFailureFromFiltered(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), progress: newProgressStore(), inboxRetry: make(chan inboxRetry, 4)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	// Admission gates on DatabaseAvailable, so its health state has to be real
	// before either assertion below means anything.
	m.updateDatabaseHealth()
	if !m.DatabaseAvailable() {
		t.Fatal("test requires a reachable database")
	}
	event := telegram.NewMessageEvent{AccountID: "account", DialogKey: "channel:admit", MessageID: 3}

	// Database unavailable: the event is recoverable, so it must be reported.
	m.dbOutage.Store(true)
	if err := m.admitChatMessage(event); !errors.Is(err, errInboxUnavailable) {
		t.Fatalf("admitChatMessage()=%v while the database is unavailable; want errInboxUnavailable so the caller retries", err)
	}
	m.dbOutage.Store(false)

	// The dialog is not watched, so the event is simply not ours. Reporting it as
	// a failure would retry it five times and then log a spurious loss.
	if err := m.admitChatMessage(event); err != nil {
		t.Fatalf("admitChatMessage()=%v for an unwatched dialog; want nil so it is not retried", err)
	}
}
