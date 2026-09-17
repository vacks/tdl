package download

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

func clearPostgresDownloadTestData(db *database) error {
	for _, statement := range []string{
		`DELETE FROM chat_download_streams`, `DELETE FROM chat_download_items`, `DELETE FROM chat_download_jobs`,
		`DELETE FROM downloaded_media`,
		`DELETE FROM bot_lifecycle_messages`, `DELETE FROM download_requests`, `DELETE FROM download_events`,
		`DELETE FROM reaction_inbox`, `DELETE FROM download_items`, `DELETE FROM download_jobs`,
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
	if err := m.registerChatMedia("first-media-chat", []source{{Item: Item{DialogType: "channel", DialogKey: "channel:first-media", DialogID: 1, MessageID: 42, OriginalName: "first.bin"}, DialogName: "test"}}, false); err != nil {
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
