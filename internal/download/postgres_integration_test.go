package download

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

// newSubmissionTestManager builds the manager a submission needs: a settings
// store for the configuration snapshot every task carries, and the wake
// channels the caller signals.
func newSubmissionTestManager(t *testing.T, db *database) *Manager {
	t.Helper()
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Manager{db: db, settings: store, events: newEventBus(), progress: newProgressStore(),
		wake: make(chan struct{}, workerCount), chatWake: make(chan struct{}, 1),
		slotWake: make(chan struct{}, 1), stopCh: make(chan struct{}),
		rpcState: make(map[string]*telegramRPCState), chatActive: make(map[string]struct{})}
}

func clearPostgresDownloadTestData(db *database) error {
	for _, statement := range []string{
		`DELETE FROM chat_download_streams`, `DELETE FROM chat_download_items`, `DELETE FROM chat_download_jobs`,
		`DELETE FROM downloaded_media`,
		`DELETE FROM bot_lifecycle_messages`, `DELETE FROM download_requests`,
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

// The task list has to answer from the maintained summary and the task's own
// row, never from the file rows. That is the whole point of the summary: a task
// holds one file row per message or per linked comment, which is unbounded, so
// reading them made a page cost the union of its tasks' file histories. The
// second half of this test is what pins it - the figures follow the summary even
// when the rows disagree with it, which cannot happen if the query still
// aggregates download_items.
func TestPostgresTaskListReadsTheSummaryNotTheFileRows(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, message_text, created_at, updated_at) VALUES ('list-job','tg://message','channel','channel:list','名字','account','queued','一条说明',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, message_text, status) VALUES
 ('list-job','channel','channel:list',1,1,'a.bin','一条说明','completed'),
 ('list-job','channel','channel:list',1,2,'b.bin','一条说明','completed'),
 ('list-job','channel','channel:list',1,3,'c.bin','一条说明','queued')`); err != nil {
		t.Fatal(err)
	}
	jobs, _, _, err := m.ListCursor("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("list returned %d tasks; want 1", len(jobs))
	}
	if jobs[0].TotalItems != 3 || jobs[0].CompletedItems != 2 {
		t.Fatalf("list reported %d/%d files; want 3 total with 2 completed", jobs[0].TotalItems, jobs[0].CompletedItems)
	}
	if jobs[0].MessageText != "一条说明" {
		t.Fatalf("list reported message text %q; want it taken from the task row", jobs[0].MessageText)
	}

	// Diverging the summary from the rows is impossible through the application,
	// which is exactly why it isolates the query: only a list that reads the
	// summary can report these figures.
	if _, err := db.Exec(`UPDATE download_item_stats SET total_items = 99, completed_items = 7 WHERE job_id = 'list-job'`); err != nil {
		t.Fatal(err)
	}
	jobs, _, _, err = m.ListCursor("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].TotalItems != 99 || jobs[0].CompletedItems != 7 {
		t.Fatalf("list still reads the file rows: got %+v; want the summary's 99/7", jobs)
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
	if _, err := m.reconcileChatPublishedItems(nil, chatPublishedCursor{}); err != nil {
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

// Indexing a page of media costs a handful of statements whatever the page
// holds, measured through the call the scan actually makes.
//
// The claim test beside this one measures the pipeline by handing its helpers a
// counting handle, which cannot see the statements the same function writes
// afterwards - and those were one INSERT per file, inside the transaction that
// indexes the page. A page is whatever the server returned, so the indexing rate
// of a channel with a million posts was set by how many of them carried media.
// This opens the database through a driver that counts, so the whole ingest is
// measured as the scan runs it.
func TestPostgresIngestingAPageCostsAFixedNumberOfStatements(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, counter := openCountingDatabase(t, url)
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1), progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('ingest-chat','tg://x','channel','channel:ingest',1,'test','account','scanning','indexing','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	// The same page twice, at two sizes: the count must be the same, because
	// what it measures is the page, not the files in it.
	for _, size := range []int{64, 200} {
		items := make([]source, 0, size)
		for id := 1; id <= size; id++ {
			items = append(items, source{Item: Item{DialogType: "channel", DialogKey: "channel:ingest", DialogID: 1, MessageID: id, OriginalName: fmt.Sprintf("f%d.bin", id)}, DialogName: "test", MediaType: "image"})
		}
		before := counter.count()
		if err := m.registerChatMedia("ingest-chat", items, false); err != nil {
			t.Fatal(err)
		}
		sent := counter.count() - before
		if sent == 0 {
			t.Fatal("the ingest sent no statements at all, so the count below means nothing")
		}
		t.Logf("ingesting %d media cost %d statements", size, sent)
		// One read of the message task's rows, one claim, one insert of the
		// page's rows. The bound is loose on purpose; what it has to catch is a
		// return to a statement per file, which is sixty-four here.
		if sent > 6 {
			t.Fatalf("ingesting %d media cost %d statements, want at most 6: the per-file form this replaced spent one for each", size, sent)
		}
		var stored int
		if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'ingest-chat'`).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if stored != size {
			t.Fatalf("after a page of %d, %d rows are indexed", size, stored)
		}
	}
}

// The sweep that repairs a crash mid-publish is bounded, and it resumes where
// the last pass stopped.
//
// Its set is normally empty, so reading all of it costs nothing - but an outage
// that leaves a backlog in it turns every three-second tick into a full read
// with a file test per row, and a row whose file is gone can never leave the
// set, so an unbounded pass would re-read that row from the top for ever and
// never reach what is behind it. The rows here are arranged so that an
// unbounded pass is visible: the first page cannot be published, and the rows
// that can be are behind it.
func TestPostgresChatPublishedSweepIsBoundedAndResumes(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('bounded-chat','tg://chat','channel','channel:bounded',1,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// A full page of rows that cannot be published: their files are not on disk.
	missing := t.TempDir() + "/gone.bin"
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, final_path, status, discovered_at)
SELECT 'bounded-chat', 'channel:bounded', g, 'gone.bin', ?, 'downloaded', ? FROM generate_series(1, ?) g`, missing, now, reconcileBatchSize); err != nil {
		t.Fatal(err)
	}
	// Behind them, two rows that can be, at the highest message ids so that only
	// a pass which ignores the page bound ever reaches them.
	published := t.TempDir() + "/published.bin"
	if err := os.WriteFile(published, []byte("published"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, final_path, status, discovered_at) VALUES ('bounded-chat','channel:bounded',?, 'published.bin', ?, 'downloaded', ?), ('bounded-chat','channel:bounded',?, 'published.bin', ?, 'downloaded', ?)`, reconcileBatchSize+1, published, now, reconcileBatchSize+2, published, now); err != nil {
		t.Fatal(err)
	}

	cursor, err := m.reconcileChatPublishedItems(nil, chatPublishedCursor{})
	if err != nil {
		t.Fatal(err)
	}
	var completed int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'bounded-chat' AND status = 'completed'`).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 0 {
		t.Fatalf("one pass published %d files, want 0: the pass is meant to read one page, and both publishable rows are behind it", completed)
	}
	if cursor.messageID != reconcileBatchSize {
		t.Fatalf("the pass resumed at message %d, want %d (the last row it read)", cursor.messageID, reconcileBatchSize)
	}

	next, err := m.reconcileChatPublishedItems(nil, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_download_items WHERE chat_job_id = 'bounded-chat' AND status = 'completed'`).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 2 {
		t.Fatalf("the second pass published %d files, want the 2 behind the first page", completed)
	}
	if next != (chatPublishedCursor{}) {
		t.Fatalf("the sweep resumed at %#v after a short page, want the start of the set", next)
	}
}

// The counts a person asks for through the Bot must cover both queue types: a
// session task owns an indexed file queue rather than child jobs, so counting
// only message items would report zero while a session download is running.
func TestPostgresDownloadCountsIncludeChatDownloadItems(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('counts-message','tg://message','queued',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_key, dialog_id, message_id, original_name, status, finished_at) VALUES ('counts-message','channel:counts',1,1,'message.bin','queued',''), ('counts-message','channel:counts',1,2,'failed.bin','failed',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('counts-chat','tg://chat','channel','channel:chat-counts',2,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, status, finished_at, discovered_at) VALUES ('counts-chat','channel:chat-counts',1,'chat.bin','running','',?), ('counts-chat','channel:chat-counts',2,'chat-failed.bin','failed',?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	active, recentFailures, err := m.DownloadCounts()
	if err != nil {
		t.Fatal(err)
	}
	if active != 2 || recentFailures != 2 {
		t.Fatalf("DownloadCounts() = %d active, %d recent failures; want 2, 2", active, recentFailures)
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

// A media claim exists to stop another task downloading the same file, so a
// claim held by a task that cannot transfer protects nothing and blocks
// everyone. Deleting or purging a session task removes its ability to run while
// leaving the claim behind, and the affected message task then waits forever on
// an owner that no longer exists - a state the user cannot clear by retrying,
// because retrying returns to the same wait. The orphan sweep is the backstop
// for claims stranded before these releases existed.
func TestPostgresRemovingChatTaskReleasesItsMediaClaims(t *testing.T) {
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
	seedChatJob := func(id, status string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES (?, 'tg://chat','channel',? ,1,'test','account',?,'completed',?,?)`, id, "channel:"+id, status, now, now); err != nil {
			t.Fatal(err)
		}
	}
	seedBlockedMessage := func(jobID string, messageID int) source {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES (?,'tg://message','channel',?,'test','account','queued',?,?)`, jobID, "channel:"+jobID, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES (?, 'channel', ?, 1, ?, 'media.bin', 'queued')`, jobID, "channel:"+jobID, messageID); err != nil {
			t.Fatal(err)
		}
		return source{Item: Item{DialogKey: "channel:" + jobID, MessageID: messageID}}
	}
	wantsMedia := func(t *testing.T, ownerID string, messageID int) bool {
		t.Helper()
		item := source{Item: Item{DialogKey: "channel:" + ownerID, MessageID: messageID}}
		if _, err := db.Exec(`UPDATE downloaded_media SET dialog_key = ? WHERE owner_id = ?`, item.DialogKey, ownerID); err != nil {
			t.Fatal(err)
		}
		claim, _, err := m.claimMessageMedia("message-blocked", item)
		if err != nil {
			t.Fatal(err)
		}
		return claim == "waiting"
	}

	// Deleting keeps the task row for history, so only the release inside the
	// delete can tell that the claim is dead.
	seedChatJob("chat-del", ChatStatusCompleted)
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:chat-del',11,'claimed','chat','chat-del',?)`, now); err != nil {
		t.Fatal(err)
	}
	seedBlockedMessage("message-del", 11)
	if !wantsMedia(t, "chat-del", 11) {
		t.Fatal("a claim held by an active-looking chat task must make the message task wait")
	}
	if err := m.DeleteChat("chat-del"); err != nil {
		t.Fatalf("DeleteChat(): %v", err)
	}
	var claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_id = 'chat-del'`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("deleted chat task still owns %d claim(s); want 0", claims)
	}

	// Purging removes the task row, which would leave a claim pointing at an id
	// nothing can ever match.
	seedChatJob("chat-purge", ChatStatusCancelled)
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:chat-purge',12,'claimed','chat','chat-purge',?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := m.PurgeChat("chat-purge"); err != nil {
		t.Fatalf("PurgeChat(): %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_id = 'chat-purge'`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("purged chat task still owns %d claim(s); want 0", claims)
	}

	// The backstop: a claim stranded before those releases existed, whose owner
	// cannot transfer any more.
	seedChatJob("chat-stale", ChatStatusFailed)
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:chat-stale',13,'claimed','chat','chat-stale',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:chat-gone',14,'claimed','chat','chat-deleted-long-ago',?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := m.orphanedMediaClaims(); err != nil {
		t.Fatalf("orphanedMediaClaims(): %v", err)
	}
	for _, owner := range []string{"chat-stale", "chat-deleted-long-ago"} {
		if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_id = ?`, owner).Scan(&claims); err != nil {
			t.Fatal(err)
		}
		if claims != 0 {
			t.Fatalf("orphan sweep left %d claim(s) owned by %s; want 0", claims, owner)
		}
	}
	// A claim held by a task that can still run must survive the sweep: removing
	// it would let a second task download the same file.
	seedChatJob("chat-live", ChatStatusDownloading)
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:chat-live',15,'claimed','chat','chat-live',?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := m.orphanedMediaClaims(); err != nil {
		t.Fatalf("orphanedMediaClaims(): %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_id = 'chat-live'`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 1 {
		t.Fatalf("orphan sweep removed a claim held by a runnable task; want it kept")
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

// A task in 'running' is selected by nothing: the scheduler wants 'queued' and
// the claim reconciler wants waiting items under queued, partial or failed
// tasks. So a worker that gave up on its task while the process stayed up left
// it in 下载中 with its files queued, and only a restart would ever move it.
// This drives the sweep the maintenance loop actually calls, and checks the two
// things that make it safe: it recovers that task, and it leaves alone a task
// that some worker in this process still owns.
func TestPostgresStrandedRunningTaskIsRecoveredWithoutARestart(t *testing.T) {
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
	// A task whose worker vanished: no entry in m.cancels, and old enough that
	// the claim-to-registration gap cannot explain it.
	seedReconcileJob(t, db, "stranded-job", "channel:stranded", "running")
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, started_at) VALUES ('stranded-job','channel','channel:stranded',1,1,'stranded.bin','running',?)`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE download_jobs SET updated_at = ? WHERE id = 'stranded-job'`, time.Now().UTC().Add(-2*jobLeaseTimeout).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	// A task this process still owns must be untouched, however long it has been
	// silent: a slow but healthy transfer writes no progress to the database.
	seedReconcileJob(t, db, "live-job", "channel:live", "running")
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, started_at) VALUES ('live-job','channel','channel:live',1,1,'live.bin','running',?)`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE download_jobs SET updated_at = ? WHERE id = 'live-job'`, time.Now().UTC().Add(-2*jobLeaseTimeout).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	_, liveCancel := context.WithCancel(context.Background())
	defer liveCancel()
	m.mu.Lock()
	m.cancels = map[string]context.CancelFunc{"live-job": liveCancel}
	m.mu.Unlock()

	m.recoverStrandedRunningJobsIfDue()

	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'stranded-job'`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "queued" {
		t.Fatalf("stranded task status = %q; want queued so a worker can claim it again", jobStatus)
	}
	wantDownloadItemStatus(t, db, "stranded-job", "queued")
	if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'live-job'`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "running" {
		t.Fatalf("task with a live worker was requeued to %q; want it left running", jobStatus)
	}
	wantDownloadItemStatus(t, db, "live-job", "running")
}

// A listener event that used up its five fast attempts is a download request the
// user made. It has no other way back into the queue: the dispatcher returns nil
// to gotd, which advances Telegram's update state, so the same update is never
// delivered again, and a group task had no gap walk either. It was therefore
// dropped silently. This drives the maintenance sweep the worker loop calls and
// checks both halves of the replacement: a recoverable event is offered again,
// and one that has exhausted the total budget is left alone with its error kept.
func TestPostgresExhaustedListenerEventIsRetriedNotDropped(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1), chatEventWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().UTC().Add(-2 * inboxReviveInterval).Format(time.RFC3339Nano)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	seed := func(id int64, attempts int) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO chat_message_inbox(id, account_id, dialog_key, dialog_name, dialog_id, message_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, error, created_at, updated_at)
 VALUES (?, 'account', ?, 'test', 1, ?, 'channel', 1, 0, 'failed', ?, ?, 'boom', ?, ?)`, id, "channel:revive", id, attempts, now, now, stale); err != nil {
			t.Fatal(err)
		}
	}
	seed(1, inboxFastAttempts+1)
	seed(2, inboxAttemptLimit)

	m.runMaintenanceSweepsIfDue()

	var status string
	if err := db.QueryRow(`SELECT status FROM chat_message_inbox WHERE id = 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("an event with %d attempts stayed %q; want pending so it is retried", inboxFastAttempts+1, status)
	}
	var errorText string
	if err := db.QueryRow(`SELECT status, error FROM chat_message_inbox WHERE id = 2`).Scan(&status, &errorText); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || errorText != "boom" {
		t.Fatalf("an event at the attempt limit is %q/%q; want it left failed with its error kept", status, errorText)
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

// The gap walk has to cover every listened dialog, not only channels and
// groups: the Saved Messages task listens too, and a message saved while the
// process was down is as recoverable - and as lost - there as anywhere else.
// It must leave alone the tasks with no settled watermark or no responsibility
// for new media.
func TestPostgresListenerGapCandidatesCoverChannels(t *testing.T) {
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
	cases := []struct {
		id         string
		dialogType string
		start      int
		listen     int
		scanState  string
		status     string
		want       bool
	}{
		{"gap-channel", "channel", 1000, 1, chatScanCompleted, ChatStatusListening, true},
		{"gap-channel-downloading", "channel", 1000, 1, chatScanCompleted, ChatStatusDownloading, true},
		{"gap-saved", "self", -1, 1, chatScanCompleted, ChatStatusListening, true},
		// A supergroup is identified as 'chat'. Leaving it out meant a group's
		// task showed 监听中 while recovering nothing published during downtime,
		// because the live stream is the only other source and it was not
		// connected then.
		{"gap-group", "chat", 1000, 1, chatScanCompleted, ChatStatusListening, true},
		{"gap-group-downloading", "chat", 1000, 1, chatScanCompleted, ChatStatusDownloading, true},
		{"gap-group-not-listening", "chat", 1000, 0, chatScanCompleted, ChatStatusDownloading, false},
		{"gap-channel-not-listening", "channel", 1000, 0, chatScanCompleted, ChatStatusDownloading, false},
		{"gap-channel-scanning", "channel", 1000, 1, chatScanIndexing, ChatStatusScanning, false},
		{"gap-channel-completed", "channel", 1000, 1, chatScanCompleted, ChatStatusCompleted, false},
		// A saved task that has scanned a history range and then listens is the
		// same responsibility as a channel's: the scan froze an upper bound, and
		// everything above it arrives over a connection that can drop.
		{"gap-saved-history-form", "self", 1000, 1, chatScanCompleted, ChatStatusListening, true},
	}
	for _, testCase := range cases {
		if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, listen_new, status, scan_state, created_at, updated_at) VALUES (?,?,?,?,1,'test','account',?,?,?,?,?,?)`, testCase.id, savedSourcePrefix+"account", testCase.dialogType, "channel:"+testCase.id, testCase.start, testCase.listen, testCase.status, testCase.scanState, now, now); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := m.listenerGapCandidateIDs("account")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(ids))
	for _, id := range ids {
		got[id] = true
	}
	for _, testCase := range cases {
		if got[testCase.id] != testCase.want {
			t.Fatalf("%s: selected=%v, want %v (selection: %v)", testCase.id, got[testCase.id], testCase.want, ids)
		}
	}
}

// The requested counts come from one statement holding four subqueries, two per
// figure. Column order is the whole contract there, so this pins both numbers
// against data that reaches them from different tables, and checks that the
// answer is recomputed rather than cached: the Bot asks for it once, and a
// stale answer would be wrong exactly when somebody is looking.
func TestPostgresDownloadCountsCountBothQueuesAndRecentFailures(t *testing.T) {
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
	seedReconcileJob(t, db, "counts-job", "channel:counts", "running")
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, finished_at) VALUES ('counts-job','channel','channel:counts',1,1,'queued.bin','queued',''), ('counts-job','channel','channel:counts',1,2,'failed.bin','failed',?)`, now); err != nil {
		t.Fatal(err)
	}
	// A session task contributes through its trigger-maintained counters rather
	// than a direct count, which is the branch a column swap would break.
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES ('counts-chat','tg://chat','channel','channel:counts-chat',1,'test','account','downloading','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, dialog_type, dialog_id, status, discovered_at) VALUES ('counts-chat','channel:counts-chat',1,'channel',1,'queued',?)`, now); err != nil {
		t.Fatal(err)
	}

	active, recentFailures, err := m.DownloadCounts()
	if err != nil {
		t.Fatal(err)
	}
	if active != 2 || recentFailures != 1 {
		t.Fatalf("DownloadCounts() = (%d, %d), want (2, 1)", active, recentFailures)
	}

	// The count is answered when it is asked for, so a write that the manager
	// never saw must still be reflected.
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('counts-job','channel','channel:counts',1,3,'late.bin','queued')`); err != nil {
		t.Fatal(err)
	}
	if active, recentFailures, err = m.DownloadCounts(); err != nil || active != 3 || recentFailures != 1 {
		t.Fatalf("DownloadCounts() after an external write = (%d, %d, %v), want (3, 1, nil)", active, recentFailures, err)
	}

	// A failure older than the window ages out of the recent figure instead of
	// pinning it for as long as the row is retained.
	old := time.Now().UTC().Add(-40 * 24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := db.Exec(`UPDATE download_items SET finished_at = ? WHERE original_name = 'failed.bin'`, old); err != nil {
		t.Fatal(err)
	}
	if _, recentFailures, err = m.DownloadCounts(); err != nil || recentFailures != 0 {
		t.Fatalf("DownloadCounts() recent failures = (%d, %v), want (0, nil)", recentFailures, err)
	}
}

// A chat item waiting on media that no message task owns, under a task that can
// no longer run, can only ever be re-read and refused. Keeping those rows in the
// reconcile scan meant a task that ended with many of them held a share of this
// pass forever and crowded the batch window that movable items need. They are
// skipped now, which shows up as a pass that never fills.
func TestPostgresStrandedWaitingItemsDoNotOccupyTheReconcileWindow(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES ('stranded-chat','tg://chat','channel','channel:stranded',1,'test','account','completed','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// More than one batch, so a scan that still visits them cannot look short.
	const stranded = chatClaimBatchSize + 44
	for index := 0; index < stranded; index++ {
		if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, dialog_type, dialog_id, status, discovered_at) VALUES ('stranded-chat','channel:stranded',?,'channel',1,'waiting',?)`, index+1, now); err != nil {
			t.Fatal(err)
		}
	}

	if next := m.reconcileChatClaims(chatClaimCursor{}); next != (chatClaimCursor{}) {
		t.Fatalf("stranded waiting items filled the batch window: cursor = %#v", next)
	}

	// The item still moves when its task can run, so the skip is not a blanket
	// exclusion of waiting items.
	if _, err := db.Exec(`UPDATE chat_download_jobs SET status = 'downloading' WHERE id = 'stranded-chat'`); err != nil {
		t.Fatal(err)
	}
	if next := m.reconcileChatClaims(chatClaimCursor{}); next == (chatClaimCursor{}) {
		t.Fatal("a runnable task's waiting items must still be examined")
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

// Upstream reports a failed per-file transfer as a success: its worker logs the
// error and returns nil, and that return value is what drives the completion
// callback. A download that stopped part-way - the media deleted or revoked
// mid-transfer, an expired file reference, a dropped connection - therefore
// arrives at publishing indistinguishable from a finished one, carrying a
// truncated or zero-byte file that upstream has already renamed into place.
//
// Publishing it is what made a task whose media disappeared mid-transfer report
// every file as downloaded: the item was marked downloaded, the partial file was
// moved into the download directory and the task settled as completed. The size
// Telegram advertised is the only evidence left, so publishing has to check it.
func TestPostgresPublishRefusesIncompleteDownload(t *testing.T) {
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
	const pattern = "{{ .MessageID }}_media.bin"
	config := settings.Defaults()
	config.Download.FinalFilenameTemplate = pattern
	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Two shapes of the same failure: a transfer that stopped part-way, and one
	// that failed before the first byte. The second is what leaves a zero-byte
	// file behind, and it is also the case that never fires a progress callback,
	// so the item is still 'queued' when the completion callback arrives.
	for _, testCase := range []struct {
		id       string
		message  int
		status   string
		onDisk   int
		expected int64
	}{
		{id: "partial-publish", message: 11, status: "downloaded", onDisk: 4096, expected: 65536},
		{id: "empty-publish", message: 12, status: "queued", onDisk: 0, expected: 65536},
	} {
		item := source{Item: Item{
			DialogKey:    "channel:incomplete",
			DialogID:     1,
			MessageID:    testCase.message,
			OriginalName: "media.bin",
			Size:         testCase.expected,
		}}
		finalPath, destErr := finalDestination(root, pattern, item)
		if destErr != nil {
			t.Fatal(destErr)
		}
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES (?,'tg://m','channel','channel:incomplete','test','account','running',?,?)`, testCase.id, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, size, status) VALUES (?,'channel','channel:incomplete',1,?,'media.bin',?,?)`, testCase.id, testCase.message, testCase.expected, testCase.status); err != nil {
			t.Fatal(err)
		}
		// Upstream hands over its temporary file already renamed, so this is the
		// path the completion callback reports.
		incoming := filepath.Join(root, fmt.Sprintf("incoming-%d.bin", testCase.message))
		if err := os.WriteFile(incoming, make([]byte, testCase.onDisk), 0o600); err != nil {
			t.Fatal(err)
		}

		err := m.publishItem(testCase.id, incoming, item, config)
		if err == nil {
			t.Fatalf("%s: publishItem() accepted an incomplete file as a download", testCase.id)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("实际 %d 字节", testCase.onDisk)) {
			t.Fatalf("%s: error %q does not report the bytes actually on disk, so the operator cannot tell what happened", testCase.id, err.Error())
		}
		wantDownloadItemStatus(t, db, testCase.id, "failed")
		var saved string
		if err := db.QueryRow(`SELECT error FROM download_items WHERE job_id = ?`, testCase.id).Scan(&saved); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(saved, "文件未下载完整") {
			t.Fatalf("%s: saved item error = %q; want it to say the file is incomplete rather than reporting a completed download", testCase.id, saved)
		}
		// The decisive outcomes: the partial file must not reach the download
		// directory, and no task may be told it may adopt one.
		if _, statErr := os.Stat(finalPath); statErr == nil {
			t.Fatalf("%s: the incomplete file was published to %s", testCase.id, finalPath)
		}
		if _, statErr := os.Stat(incoming); statErr == nil {
			t.Fatalf("%s: the incomplete file was left behind for a person to mistake for a download", testCase.id)
		}
		var owners int
		if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:incomplete' AND message_id = ?`, testCase.message).Scan(&owners); err != nil {
			t.Fatal(err)
		}
		if owners != 0 {
			t.Fatalf("%s: an incomplete file was recorded as an owned, reusable download", testCase.id)
		}
	}

	// The control that keeps this from passing by refusing everything: a file of
	// exactly the advertised size still publishes and is still adoptable.
	complete := source{Item: Item{
		DialogKey:    "channel:incomplete",
		DialogID:     1,
		MessageID:    13,
		OriginalName: "media.bin",
		Size:         4096,
	}}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('complete-publish','tg://m','channel','channel:incomplete','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, size, status) VALUES ('complete-publish','channel','channel:incomplete',1,13,'media.bin',4096,'downloaded')`); err != nil {
		t.Fatal(err)
	}
	completeIncoming := filepath.Join(root, "incoming-complete.bin")
	if err := os.WriteFile(completeIncoming, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.publishItem("complete-publish", completeIncoming, complete, config); err != nil {
		t.Fatalf("publishItem() refused a complete download: %v", err)
	}
	wantDownloadItemStatus(t, db, "complete-publish", "completed")
	completeFinal, err := finalDestination(root, pattern, complete)
	if err != nil {
		t.Fatal(err)
	}
	if info, statErr := os.Stat(completeFinal); statErr != nil || info.Size() != 4096 {
		t.Fatalf("complete download not published to %s: stat=%v", completeFinal, statErr)
	}
	var owner string
	if err := db.QueryRow(`SELECT owner_id FROM downloaded_media WHERE dialog_key = 'channel:incomplete' AND message_id = 13`).Scan(&owner); err != nil {
		t.Fatalf("complete download has no ownership row: %v", err)
	}
	if owner != "complete-publish" {
		t.Fatalf("ownership row owner = %q; want complete-publish", owner)
	}
}

// The chat path publishes through its own function, so the same refusal has to
// be wired there too: a session task that recorded a truncated file as completed
// would report a finished batch and never download the media again.
func TestPostgresPublishChatItemRefusesIncompleteDownload(t *testing.T) {
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
	const pattern = "{{ .MessageID }}_chatmedia.bin"
	config := settings.Defaults()
	config.Download.FinalFilenameTemplate = pattern
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES ('incomplete-chat','tg://chat','channel','channel:incomplete-chat',1,'test','account','downloading','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{
		DialogKey:    "channel:incomplete-chat",
		DialogID:     1,
		MessageID:    21,
		OriginalName: "chatmedia.bin",
		Size:         65536,
	}}
	if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, original_name, size, status, discovered_at) VALUES ('incomplete-chat','channel:incomplete-chat',21,'chatmedia.bin',65536,'downloaded',?)`, now); err != nil {
		t.Fatal(err)
	}
	incoming := filepath.Join(root, "incoming-chat.bin")
	if err := os.WriteFile(incoming, make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	finalPath, err := finalDestination(root, pattern, item)
	if err != nil {
		t.Fatal(err)
	}

	err = m.publishChatItem("incomplete-chat", incoming, item, config)
	if err == nil {
		t.Fatal("publishChatItem() accepted an incomplete file as a download")
	}
	if !strings.Contains(err.Error(), "文件未下载完整") {
		t.Fatalf("publishChatItem() error %q; want it to say the file is incomplete", err.Error())
	}
	var status, saved string
	if err := db.QueryRow(`SELECT status, error FROM chat_download_items WHERE chat_job_id = 'incomplete-chat' AND message_id = 21`).Scan(&status, &saved); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || !strings.Contains(saved, "文件未下载完整") {
		t.Fatalf("chat item status=%q error=%q; want failed with an incomplete-file reason", status, saved)
	}
	if _, statErr := os.Stat(finalPath); statErr == nil {
		t.Fatalf("the incomplete file was published to %s", finalPath)
	}
	if _, statErr := os.Stat(incoming); statErr == nil {
		t.Fatal("the incomplete file was left behind for a person to mistake for a download")
	}
	var owners int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:incomplete-chat' AND message_id = 21`).Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if owners != 0 {
		t.Fatal("an incomplete file was recorded as an owned, reusable download")
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
	// The chat worker reconciles listeners, which needs to know which accounts
	// can hold a connection, so this Manager needs the account store the way
	// New would provide it. Leaving it out made the loop panic rather than
	// exercise the stop signal this test is about.
	accounts, err := telegram.Open(t.TempDir(), func() string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(accounts.Stop)
	m := &Manager{db: db, settings: store, accounts: accounts, events: newEventBus(), progress: newProgressStore(),
		chatWatched: make(map[string]map[string]struct{}), chatListeners: make(map[string]*chatListener),
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

// A published file that the user deleted by hand leaves an ownership row that
// still says completed. Nothing will ever publish that file again, so an item
// waiting behind the row could only stay in "waiting" forever while its task
// stayed pinned in 下载中 — with no error, no timeout and nothing to act on.
//
// The row has to be retired rather than respected, and retiring it takes two
// things that were each missing: the pass must let a completed-but-fileless row
// fall through to the claim, and the claim must be able to replace the row it
// finds. Both are exercised here, along with the two neighbours that must keep
// their old behaviour — a completed row whose file is still there is adopted,
// and a live claim is still waited on.
func TestPostgresChatReconcileRetiresAStaleCompletedClaim(t *testing.T) {
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
	seedChatJobWithStatus(t, db, "chat-stale", "channel:chatstale", ChatStatusDownloading)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	missing := filepath.Join(t.TempDir(), "deleted-by-hand.bin")
	present := filepath.Join(t.TempDir(), "still-here.bin")
	if err := os.WriteFile(present, []byte("published"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The stale row a deleted file leaves behind, owned by a chat task that has
	// since finished.
	seedWaitingChatItem(t, db, "chat-stale", "channel:chatstale", 41, "")
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES ('channel:chatstale', 41, ?, 'completed', 'chat', 'finished-chat', ?)`, missing, now); err != nil {
		t.Fatal(err)
	}
	// The same staleness seen from the message table: a message task published
	// this file and its own item row says completed, but the file is gone. That
	// row must not keep the session item waiting either.
	seedWaitingChatItem(t, db, "chat-stale", "channel:chatstale", 42, "")
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES ('channel:chatstale', 42, ?, 'completed', 'message', 'finished-message', ?)`, missing, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('finished-message','tg://stale','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, final_path) VALUES ('finished-message','channel','channel:chatstale',1,42,'stale.bin','completed',?)`, missing); err != nil {
		t.Fatal(err)
	}
	// Adopted, not re-downloaded: the file is there.
	seedWaitingChatItem(t, db, "chat-stale", "channel:chatstale", 43, "")
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES ('channel:chatstale', 43, ?, 'completed', 'chat', 'finished-chat', ?)`, present, now); err != nil {
		t.Fatal(err)
	}
	// Held right now by another task: waiting is the correct answer.
	seedWaitingChatItem(t, db, "chat-stale", "channel:chatstale", 44, "live-chat")

	m.reconcileChatClaims(chatClaimCursor{})

	states := map[int]string{}
	rows, err := db.Query(`SELECT message_id, status FROM chat_download_items WHERE chat_job_id = 'chat-stale'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		var state string
		if err := rows.Scan(&id, &state); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		states[id] = state
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{41, 42} {
		if states[id] != "queued" {
			t.Fatalf("item %d is %q after the pass; a completed ownership row whose file is gone has to be retired so the item can be fetched again", id, states[id])
		}
	}
	if states[43] != "completed" {
		t.Fatalf("item 43 is %q; a completed row whose file is still on disk must be adopted, not re-downloaded", states[43])
	}
	if states[44] != "waiting" {
		t.Fatalf("item 44 is %q; media another task holds right now must still be waited on", states[44])
	}
	var ownerKind, ownerID, ownerStatus string
	if err := db.QueryRow(`SELECT owner_kind, owner_id, status FROM downloaded_media WHERE dialog_key = 'channel:chatstale' AND message_id = 41`).Scan(&ownerKind, &ownerID, &ownerStatus); err != nil {
		t.Fatal(err)
	}
	if ownerKind != "chat" || ownerID != "chat-stale" || ownerStatus != "claimed" {
		t.Fatalf("the stale row is now %s/%s/%s; the task that can transfer it must hold the claim", ownerKind, ownerID, ownerStatus)
	}
}

// A request names files; a task is just where they live. When a submission
// names files the task it matched does not hold, those files have to join it.
//
// The shape that produced this is an album: one member fetched on its own with
// ?single, and then the whole album submitted. The match is that one-file task,
// and answering with it dropped the rest of the request on the floor — the rows
// were never written, and a settled task is never claimed again, so the album
// became unreachable behind the one file that had been downloaded. The user saw
// a 202 and a duplicate, and waited for files that were never queued.
func TestPostgresSubmissionExtendsTheTaskThatHoldsPartOfTheRequest(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := newSubmissionTestManager(t, db)
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('album-one','https://t.me/c/700/100?single','channel','channel:album','相册','account','completed',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status, final_path) VALUES ('album-one','channel','channel:album',700,100,'one.bin','completed','/downloads/one.bin')`); err != nil {
		t.Fatal(err)
	}

	// The same album, now submitted whole: one file the task holds and two it
	// does not.
	sources := []source{
		{Item: Item{DialogType: "channel", DialogKey: "channel:album", DialogID: 700, MessageID: 100, OriginalName: "one.bin"}, DialogName: "相册"},
		{Item: Item{DialogType: "channel", DialogKey: "channel:album", DialogID: 700, MessageID: 101, OriginalName: "two.bin"}, DialogName: "相册"},
		{Item: Item{DialogType: "channel", DialogKey: "channel:album", DialogID: 700, MessageID: 102, OriginalName: "three.bin"}, DialogName: "相册"},
	}
	submission, err := m.enqueueIntent(DownloadIntent{Source: SourceWeb, AccountID: "account", URL: "https://t.me/c/700/100"}, sources, directPeer{})
	if err != nil {
		t.Fatal(err)
	}
	if !submission.Duplicate || !submission.Reactivated {
		t.Fatalf("the submission reports duplicate=%t reactivated=%t; the task it matched has to take the new files and go back on the queue", submission.Duplicate, submission.Reactivated)
	}
	if submission.Job.ID != "album-one" {
		t.Fatalf("the submission matched task %q; want the one that already holds part of the album", submission.Job.ID)
	}

	var total, queued int
	if err := db.QueryRow(`SELECT COUNT(1), COUNT(1) FILTER (WHERE status = 'queued') FROM download_items WHERE job_id = 'album-one'`).Scan(&total, &queued); err != nil {
		t.Fatal(err)
	}
	if total != 3 || queued != 2 {
		t.Fatalf("the task holds %d file(s), %d queued; want 3 with the 2 new ones queued", total, queued)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'album-one'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("the task is %q; a task that just took on files has to be claimable again", status)
	}

	// A request that brings nothing new is still the plain duplicate it always
	// was: no status change, no spurious reactivation.
	before := submission.Job.UpdatedAt
	again, err := m.enqueueIntent(DownloadIntent{Source: SourceWeb, AccountID: "account", URL: "https://t.me/c/700/100"}, sources, directPeer{})
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Reactivated {
		t.Fatalf("re-submitting the same files reports duplicate=%t reactivated=%t; want a plain duplicate", again.Duplicate, again.Reactivated)
	}
	var after string
	if err := db.QueryRow(`SELECT updated_at FROM download_jobs WHERE id = 'album-one'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("the task's updated_at moved from %q to %q for a request that added nothing", before, after)
	}
}

// A task is displayed as the request that produced it, not as its first file.
//
// A channel post with no media of its own yields no item of its own either -
// only its comments, which live in the linked discussion group and carry that
// group's name and one commenter's text. Taking the name and the text from the
// first source therefore showed the discussion group and a comment where the
// person had asked for a channel post: the same task was named 在花🎗️科技圈 in
// the directory it wrote into and 在花小茶馆 on its card, and its card showed a
// commenter's sentence instead of the post being downloaded.
func TestPostgresTaskShowsTheOriginDialogAndPostText(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := newSubmissionTestManager(t, db)
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	// Two comments of a post that has no media of its own.
	comment := func(id int, text string) source {
		return source{
			Item: Item{
				DialogType: "chat", DialogKey: "channel:777", DialogID: 777,
				MessageID: id, MessageText: text, IsComment: true,
				OriginDialogName: "在花🎗️科技圈", OriginMessageID: 43466,
				OriginMessageText: "原帖的文字",
			},
			DialogName: "在花小茶馆",
		}
	}
	sources := []source{comment(413217, "哈哈哈"), comment(413218, "有多少人点看链接")}
	submission, err := m.enqueueIntent(DownloadIntent{Source: SourceReaction, AccountID: "account", URL: "tg://reaction/channel/1125107539/43466"}, sources, directPeer{})
	if err != nil {
		t.Fatal(err)
	}
	var dialogName, messageText string
	if err := db.QueryRow(`SELECT dialog_name, message_text FROM download_jobs WHERE id = ?`, submission.Job.ID).Scan(&dialogName, &messageText); err != nil {
		t.Fatal(err)
	}
	if dialogName != "在花🎗️科技圈" {
		t.Fatalf("the task is displayed as %q; want the channel the post is in, not the discussion group its comments live in", dialogName)
	}
	if messageText != "原帖的文字" {
		t.Fatalf("the task's text is %q; want the post's, not a commenter's", messageText)
	}
	// The files themselves keep the album rule: a comment with its own caption
	// is named from it. That is a different field on a different row.
	var itemText string
	if err := db.QueryRow(`SELECT message_text FROM download_items WHERE job_id = ? AND message_id = 413217`, submission.Job.ID).Scan(&itemText); err != nil {
		t.Fatal(err)
	}
	if itemText != "哈哈哈" {
		t.Fatalf("the file's own text is %q; want the comment's own caption", itemText)
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

// A listening task admits its own dialog and its linked discussion group, and
// nothing else - not even a reply-shaped message in another group the account
// has joined.
//
// The watch set is built by the production snapshot query, so this fails if the
// discussion arm is dropped from it (the comment would never be admitted) or if
// an account-wide rule is allowed back in (the unrelated reply would be). The
// unrelated message is deliberately shaped like a comment: reply_to set, from a
// channel the account is in, with no task of its own.
func TestPostgresAdmitsOnlyListenedDialogAndItsDiscussionGroup(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), chatEventWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	m.updateDatabaseHealth()
	if !m.DatabaseAvailable() {
		t.Fatal("test requires a reachable database")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, discussion_dialog_key, discussion_peer_type, discussion_peer_id, discussion_peer_hash, listen_new, status, scan_state, config_json, created_at, updated_at) VALUES ('link-chat','tg://chat','channel','channel:100',100,'频道','account','channel',100,11,'channel:200','channel',200,22,1,?,'completed','{}',?,?)`, ChatStatusListening, now, now); err != nil {
		t.Fatal(err)
	}
	wanted, watched, err := m.watchedDialogKeys()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := wanted["account"]; !ok {
		t.Fatal("an account with a listening task needs an update connection")
	}
	m.chatWatched = watched

	// A comment on one of the channel's posts arrives in the discussion group.
	comment := telegram.NewMessageEvent{AccountID: "account", DialogID: 200, MessageID: 5, ReplyToMessageID: 4, InputPeer: &tg.InputPeerChannel{ChannelID: 200, AccessHash: 22}}
	if err := m.admitChatMessage(comment); err != nil {
		t.Fatalf("admitChatMessage()=%v for the task's own discussion group", err)
	}
	// An ordinary reply in a group the account has joined but no task listens to.
	// This is the shape that used to be admitted account-wide, and each one cost a
	// doomed Telegram lookup retried five times.
	unrelated := telegram.NewMessageEvent{AccountID: "account", DialogID: 300, MessageID: 6, ReplyToMessageID: 1, InputPeer: &tg.InputPeerChannel{ChannelID: 300, AccessHash: 33}}
	if err := m.admitChatMessage(unrelated); err != nil {
		t.Fatalf("admitChatMessage()=%v for an unwatched dialog; want nil so it is not retried", err)
	}

	var groupRows, unrelatedRows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_message_inbox WHERE dialog_key = 'channel:200'`).Scan(&groupRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM chat_message_inbox WHERE dialog_key = 'channel:300'`).Scan(&unrelatedRows); err != nil {
		t.Fatal(err)
	}
	if groupRows != 1 || unrelatedRows != 0 {
		t.Fatalf("discussion group rows=%d unrelated rows=%d, want 1 and 0", groupRows, unrelatedRows)
	}
}

// Remembering a link has to update the in-memory watch set in the same call.
// The set is otherwise rebuilt only by the periodic snapshot, so a comment
// arriving in that interval - up to five minutes - would be filtered out at
// admission, and the dispatcher acknowledges the update, which means Telegram
// never sends it again.
func TestPostgresRememberDiscussionLinkUpdatesWatchSet(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), chatWatched: make(map[string]map[string]struct{})}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, listen_new, status, scan_state, config_json, created_at, updated_at) VALUES ('learn-chat','tg://chat','channel','channel:100',100,'频道','account',1,?,'completed','{}',?,?)`, ChatStatusListening, now, now); err != nil {
		t.Fatal(err)
	}
	if err := m.rememberDiscussionLink("learn-chat", "account", "channel:200", &tg.InputPeerChannel{ChannelID: 200, AccessHash: 22}); err != nil {
		t.Fatal(err)
	}
	var key, kind string
	var id, hash int64
	if err := db.QueryRow(`SELECT discussion_dialog_key, discussion_peer_type, discussion_peer_id, discussion_peer_hash FROM chat_download_jobs WHERE id = 'learn-chat'`).Scan(&key, &kind, &id, &hash); err != nil {
		t.Fatal(err)
	}
	if key != "channel:200" || kind != "channel" || id != 200 || hash != 22 {
		t.Fatalf("recorded link=%q peer=%s/%d/%d, want channel:200 channel/200/22", key, kind, id, hash)
	}
	if _, ok := m.chatWatched["account"]["channel:200"]; !ok {
		t.Fatal("the learned group must be watched immediately, not at the next snapshot rebuild")
	}

	// An empty peer is still a usable record: admission matches on the dialog
	// key alone, and the peer is only what entity-less update recovery uses.
	if err := m.rememberDiscussionLink("learn-chat", "account", "channel:201", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT discussion_dialog_key FROM chat_download_jobs WHERE id = 'learn-chat'`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != "channel:201" {
		t.Fatalf("key-only link recorded as %q, want channel:201", key)
	}
}

// Version 20 backfills the recorded group from the per-thread mapping that older
// tasks wrote, because admission no longer reads that table. Without the
// backfill such a task silently stops receiving comments on its posts.
func TestPostgresDiscussionLinkBackfillFromReplyRoots(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, listen_new, status, scan_state, config_json, created_at, updated_at) VALUES ('legacy-chat','tg://chat','channel','channel:100',100,'频道','account',1,?,'completed','{}',?,?)`, ChatStatusListening, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_reply_roots(chat_job_id, account_id, discussion_dialog_key, root_message_id, origin_message_id, discussion_peer_type, discussion_peer_id, discussion_peer_hash) VALUES ('legacy-chat','account','channel:200',7,11,'channel',200,22), ('legacy-chat','account','channel:200',9,11,'',0,0)`); err != nil {
		t.Fatal(err)
	}
	// Replay the upgrade: the columns exist and are empty, and version 20 has not
	// run yet.
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version >= 20`); err != nil {
		t.Fatal(err)
	}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	var key, kind, probedAt string
	var id, hash int64
	if err := db.QueryRow(`SELECT discussion_dialog_key, discussion_peer_type, discussion_peer_id, discussion_peer_hash, discussion_probed_at FROM chat_download_jobs WHERE id = 'legacy-chat'`).Scan(&key, &kind, &id, &hash, &probedAt); err != nil {
		t.Fatal(err)
	}
	if key != "channel:200" || kind != "channel" || id != 200 || hash != 22 {
		t.Fatalf("backfilled link=%q peer=%s/%d/%d, want channel:200 channel/200/22", key, kind, id, hash)
	}
	// The timestamp is what stops the periodic resolver from asking Telegram the
	// same question again on the next pass.
	if probedAt == "" {
		t.Fatal("a backfilled link must carry a probe timestamp")
	}
	if _, err := time.Parse(time.RFC3339Nano, probedAt); err != nil {
		t.Fatalf("backfilled probe timestamp %q is not readable by the resolver: %v", probedAt, err)
	}
}

// A request Telegram rejected cannot succeed later, so it stops on the first
// attempt and spends the whole attempt budget at once. The budget is what the
// hourly revive reads, so this is also the assertion that a rejected event does
// not come back to spend a request every hour for a day: the control row beside
// it has the same age and the same status and is revived, which is what makes
// the difference the classification rather than the clock.
func TestPostgresRejectedInboxEventStopsAndIsNotRevived(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',1,'channel',100,1,'processing',1,?,?,?), ('account','channel:100',2,'channel',100,1,'processing',1,?,?,?)`, now, now, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	ids := map[int]int64{}
	rows, err := db.Query(`SELECT message_id, id FROM chat_message_inbox`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var messageID int
		var id int64
		if err := rows.Scan(&messageID, &id); err != nil {
			t.Fatal(err)
		}
		ids[messageID] = id
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	if err := m.retryChatMessageInbox(ids[1], 1, tgerr.New(400, "CHANNEL_INVALID")); err != nil {
		t.Fatal(err)
	}
	if err := m.retryChatMessageInbox(ids[2], 1, errors.New("connection reset by peer")); err != nil {
		t.Fatal(err)
	}

	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts FROM chat_message_inbox WHERE id = ?`, ids[1]).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != inboxAttemptLimit {
		t.Fatalf("rejected event: status=%q attempts=%d, want failed/%d", status, attempts, inboxAttemptLimit)
	}
	if err := db.QueryRow(`SELECT status, attempts FROM chat_message_inbox WHERE id = ?`, ids[2]).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 1 {
		t.Fatalf("transient event: status=%q attempts=%d, want pending/1", status, attempts)
	}

	// Age both rows past the revive window. The transient one comes back; the
	// rejected one must not.
	old := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	if _, err := db.Exec(`UPDATE chat_message_inbox SET updated_at = ? WHERE id IN (?, ?)`, old, ids[1], ids[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE chat_message_inbox SET status = 'failed', attempts = 5 WHERE id = ?`, ids[2]); err != nil {
		t.Fatal(err)
	}
	if err := m.reviveExhaustedInboxEvents(); err != nil {
		t.Fatal(err)
	}
	var rejectedStatus, revivedStatus string
	if err := db.QueryRow(`SELECT status FROM chat_message_inbox WHERE id = ?`, ids[1]).Scan(&rejectedStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM chat_message_inbox WHERE id = ?`, ids[2]).Scan(&revivedStatus); err != nil {
		t.Fatal(err)
	}
	if rejectedStatus != "failed" {
		t.Fatalf("rejected event was revived as %q; a rejection must not be retried", rejectedStatus)
	}
	if revivedStatus != "pending" {
		t.Fatalf("a failed event below the attempt limit was not revived (status %q)", revivedStatus)
	}
}

// The status line reports waiting apart from in-flight work, because a pending
// row can sit out an hour-long backoff and counting it as work in progress hid
// an event that was doing nothing behind what looked like activity.
func TestPostgresListenerInboxCountsSeparateWaitingFromStopped(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',1,'channel',100,1,'processing',1,?,?,?), ('account','channel:100',2,'channel',100,1,'pending',0,?,?,?), ('account','channel:100',3,'channel',100,1,'failed',20,?,?,?)`, now, now, now, now, now, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, emoji, status, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',4,'channel','👍','pending',?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	processing, waiting, stopped, err := m.ListenerInboxCounts()
	if err != nil {
		t.Fatal(err)
	}
	if processing != 1 || waiting != 2 || stopped != 1 {
		t.Fatalf("counts=%d processing/%d waiting/%d stopped, want 1/2/1", processing, waiting, stopped)
	}
}

// TestPostgresListenerEventPagesWalkBothInboxesExactlyOnce seeds more open
// events than one page holds, across both inboxes, and walks the whole list.
// The properties it checks are the ones a merged keyset page can break
// silently: a completed row leaking into a list that is not supposed to show
// it, a row shown twice or skipped when a page boundary falls inside a tie, and
// one of the two inboxes being lost by the merge.
func TestPostgresListenerEventPagesWalkBothInboxesExactlyOnce(t *testing.T) {
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
	const openMessages, openReactions, completed = 25, 12, 20
	// Both arms use the same clock so the two share a created_at on most of
	// their rows: the merge then has to order them by something other than the
	// timestamp, which is exactly where a cursor can skip or repeat a row.
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < openMessages; i++ {
		stamp := base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339Nano)
		status := "pending"
		if i%5 == 0 {
			status = "processing"
		}
		if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, dialog_name, message_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',?,?,'channel',100,1,?,?,?,?,?)`,
			fmt.Sprintf("会话 %d", i), 1000+i, status, i%3, stamp, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < openReactions; i++ {
		stamp := base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339Nano)
		if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, dialog_name, message_id, peer_type, peer_id, peer_hash, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',?,?,'channel',100,1,'👍','failed',20,?,?,?)`,
			fmt.Sprintf("反应会话 %d", i), 2000+i, stamp, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	// Completed rows are stamped newer than every open one, so a list that
	// forgot to exclude them would put them on the first page rather than
	// merely at the end.
	for i := 0; i < completed; i++ {
		stamp := base.Add(time.Duration(1000+i) * time.Minute).Format(time.RFC3339Nano)
		if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, dialog_name, message_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',?,?,'channel',100,1,'done',0,?,?,?)`,
			fmt.Sprintf("已完成 %d", i), 3000+i, stamp, stamp, stamp); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, dialog_name, message_id, peer_type, peer_id, peer_hash, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:100',?,?,'channel',100,1,'👍','done',0,?,?,?)`,
			fmt.Sprintf("已完成反应 %d", i), 4000+i, stamp, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}

	total := openMessages + openReactions
	seen := map[string]bool{}
	walked := make([]ListenerEvent, 0, total)
	cursor, pages := "", 0
	for {
		pages++
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
		events, reported, next, err := m.ListListenerEvents(cursor, 10)
		if err != nil {
			t.Fatal(err)
		}
		if reported != total {
			t.Fatalf("page %d reports %d open events, want %d", pages, reported, total)
		}
		if len(events) == 0 {
			t.Fatalf("page %d is empty before the list ended (cursor %q)", pages, cursor)
		}
		if len(events) > 10 {
			t.Fatalf("page %d holds %d events, want at most 10", pages, len(events))
		}
		for _, event := range events {
			key := eventKey(event)
			if seen[key] {
				t.Fatalf("event %s appeared on more than one page", key)
			}
			seen[key] = true
			if event.Status == "done" {
				t.Fatalf("event %s is completed and must not be listed", key)
			}
			if len(walked) > 0 && walked[len(walked)-1].CreatedAt < event.CreatedAt {
				t.Fatalf("page order is not newest first: %s came after %s", event.CreatedAt, walked[len(walked)-1].CreatedAt)
			}
			walked = append(walked, event)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(walked) != total {
		t.Fatalf("walking the pages yielded %d events, want %d", len(walked), total)
	}
	bySource := map[string]int{}
	for _, event := range walked {
		bySource[event.Source]++
	}
	if bySource["message"] != openMessages || bySource["reaction"] != openReactions {
		t.Fatalf("walked %d message and %d reaction events, want %d and %d", bySource["message"], bySource["reaction"], openMessages, openReactions)
	}
	if pages != 4 {
		t.Fatalf("walked %d pages, want 4 for %d events at 10 per page", pages, total)
	}
}

// TestPostgresClearStoppedEventsRemovesOnlyFailedRows checks the boundary of
// the only operation that destroys listener events: it must take exactly the
// rows /status counts as stopped, and leave queued, in-flight and completed
// work alone.
func TestPostgresClearStoppedEventsRemovesOnlyFailedRows(t *testing.T) {
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
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	// The message inbox gets a failed row below the attempt limit as well as
	// one at it: /status counts both as stopped, so the clear has to take both.
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at) VALUES
 ('account','channel:100',1,'channel',100,1,'failed',20,?,?,?),
 ('account','channel:100',2,'channel',100,1,'failed',6,?,?,?),
 ('account','channel:100',3,'channel',100,1,'pending',0,?,?,?),
 ('account','channel:100',4,'channel',100,1,'processing',1,?,?,?),
 ('account','channel:100',5,'channel',100,1,'done',0,?,?,?)`,
		stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES
 ('account','channel:100',6,'channel','👍','failed',20,?,?,?),
 ('account','channel:100',7,'channel','👍','pending',0,?,?,?),
 ('account','channel:100',8,'channel','👍','done',0,?,?,?)`,
		stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	stopped, revivable, err := m.StoppedEventCounts()
	if err != nil {
		t.Fatal(err)
	}
	if stopped != 3 || revivable != 1 {
		t.Fatalf("StoppedEventCounts() = %d stopped/%d revivable, want 3/1", stopped, revivable)
	}
	messages, reactions, err := m.ClearStoppedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if messages != 2 || reactions != 1 {
		t.Fatalf("ClearStoppedEvents() removed %d message and %d reaction events, want 2 and 1", messages, reactions)
	}
	for _, check := range []struct {
		table  string
		status string
		want   int
	}{
		{"chat_message_inbox", "failed", 0},
		{"chat_message_inbox", "pending", 1},
		{"chat_message_inbox", "processing", 1},
		{"chat_message_inbox", "done", 1},
		{"reaction_inbox", "failed", 0},
		{"reaction_inbox", "pending", 1},
		{"reaction_inbox", "done", 1},
	} {
		var rows int
		if err := db.QueryRow(`SELECT COUNT(1) FROM `+check.table+` WHERE status = ?`, check.status).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != check.want {
			t.Fatalf("%s has %d %s rows, want %d", check.table, rows, check.status, check.want)
		}
	}
	if stopped, _, err := m.StoppedEventCounts(); err != nil {
		t.Fatal(err)
	} else if stopped != 0 {
		t.Fatalf("StoppedEventCounts() still reports %d stopped events after a clear", stopped)
	}
}

// claimMediaBatch is the set-based form of claimMedia, and both batch call
// sites depend on the two answering identically. Every fixture below seeds two
// media in the same state - one claimed per item, one through the batch - and
// the verdicts are compared, so the fast path cannot drift away from the
// guarded per-item path it replaced.
func TestPostgresBatchClaimMatchesPerItemClaim(t *testing.T) {
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
		t.Fatalf("migratePostgres(): %v", err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// A completed row only counts as published while the bytes are still on
	// disk, so the adoptable case needs a real file and the stale case needs a
	// path that no longer resolves.
	dir := t.TempDir()
	published := filepath.Join(dir, "published.bin")
	if err := os.WriteFile(published, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	vanished := filepath.Join(dir, "vanished.bin")
	claimRow := func(status, path, kind, id string) func(*testing.T, mediaClaimKey) {
		return func(t *testing.T, key mediaClaimKey) {
			t.Helper()
			if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, key.dialogKey, key.messageID, path, status, kind, id, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	noRow := func(*testing.T, mediaClaimKey) {}

	for _, tc := range []struct {
		name      string
		seed      func(*testing.T, mediaClaimKey)
		wantState string
		wantPath  string
		// wantOwner is the ownership row after the call. A "waiting" verdict
		// must leave another task's ownership exactly as it found it: taking it
		// would be the double download the claim exists to prevent.
		wantOwner string
	}{
		{"unowned", noRow, "queued", "", "chat/batch-chat"},
		{"owned by this task", claimRow("claimed", "", "chat", "batch-chat"), "queued", "", "chat/batch-chat"},
		{"owned by another chat task", claimRow("claimed", "", "chat", "other-chat"), "waiting", "", "chat/other-chat"},
		{"owned by a message task", claimRow("claimed", "", "message", "other-message"), "waiting", "", "message/other-message"},
		{"published", claimRow("completed", published, "chat", "other-chat"), "completed", published, "chat/other-chat"},
		{"published file removed", claimRow("completed", vanished, "chat", "other-chat"), "queued", "", "chat/batch-chat"},
		{"published without a path", claimRow("completed", "", "chat", "other-chat"), "queued", "", "chat/batch-chat"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Two identities in the same state, so neither call can observe or
			// disturb the other's row.
			itemKey := mediaClaimKey{dialogKey: "channel:" + tc.name, messageID: 1}
			batchKey := mediaClaimKey{dialogKey: "channel:" + tc.name, messageID: 2}
			tc.seed(t, itemKey)
			tc.seed(t, batchKey)

			itemState, itemPath, err := m.claimMedia("chat", "batch-chat", source{Item: Item{DialogKey: itemKey.dialogKey, MessageID: itemKey.messageID}})
			if err != nil {
				t.Fatalf("claimMedia(): %v", err)
			}
			claims, err := m.claimMediaBatch(m.db, "chat", "batch-chat", []source{{Item: Item{DialogKey: batchKey.dialogKey, MessageID: batchKey.messageID}}})
			if err != nil {
				t.Fatalf("claimMediaBatch(): %v", err)
			}
			batched, ok := claims[batchKey]
			if !ok {
				t.Fatalf("claimMediaBatch() returned no verdict for %+v", batchKey)
			}
			if batched.state != tc.wantState || batched.path != tc.wantPath {
				t.Fatalf("claimMediaBatch()=%q,%q; want %q,%q", batched.state, batched.path, tc.wantState, tc.wantPath)
			}
			if batched.state != itemState || batched.path != itemPath {
				t.Fatalf("claimMediaBatch()=%q,%q disagrees with claimMedia()=%q,%q on the same fixture", batched.state, batched.path, itemState, itemPath)
			}
			var kind, id string
			if err := db.QueryRow(`SELECT owner_kind, owner_id FROM downloaded_media WHERE dialog_key = ? AND message_id = ?`, batchKey.dialogKey, batchKey.messageID).Scan(&kind, &id); err != nil {
				t.Fatalf("read the batched row back: %v", err)
			}
			if got := kind + "/" + id; got != tc.wantOwner {
				t.Fatalf("owner after the batch claim = %s, want %s", got, tc.wantOwner)
			}
		})
	}
}

// A task holds a row per media comment, so the set handed to the batch claim is
// not bounded by the batch window. Chunking has to be invisible: one call
// returns a verdict for every identity it was given, and the pass after it -
// the resumed case - reports this task's own claims as its own rather than as
// someone else's.
func TestPostgresBatchClaimSpansMoreThanOneChunk(t *testing.T) {
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
		t.Fatalf("migratePostgres(): %v", err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	total := mediaClaimChunk*2 + 7
	items := make([]source, 0, total)
	for index := 0; index < total; index++ {
		items = append(items, source{Item: Item{DialogKey: "channel:wide", MessageID: index + 1}})
	}
	claims, err := m.claimMediaBatch(m.db, "chat", "wide-chat", items)
	if err != nil {
		t.Fatalf("claimMediaBatch(): %v", err)
	}
	if len(claims) != total {
		t.Fatalf("verdicts=%d, want %d", len(claims), total)
	}
	var owned int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_id = 'wide-chat'`).Scan(&owned); err != nil || owned != total {
		t.Fatalf("owned rows=%d err=%v, want %d", owned, err, total)
	}
	again, err := m.claimMediaBatch(m.db, "chat", "wide-chat", items)
	if err != nil {
		t.Fatalf("claimMediaBatch() resumed: %v", err)
	}
	for key, claim := range again {
		if claim.state != "queued" {
			t.Fatalf("resumed verdict for %+v = %q, want queued", key, claim.state)
		}
	}
	// Repeating an identity inside one call is the same claim, not a second one.
	repeated := append(append([]source(nil), items...), items...)
	deduped, err := m.claimMediaBatch(m.db, "chat", "wide-chat", repeated)
	if err != nil {
		t.Fatalf("claimMediaBatch() with repeats: %v", err)
	}
	if len(deduped) != total {
		t.Fatalf("verdicts with repeats=%d, want %d", len(deduped), total)
	}
}
