package download

import (
	"context"
	"os"
	"testing"
	"time"
)

// openChatItemTestManager prepares a database with one session task in it.
func openChatItemTestManager(t *testing.T, jobID, status string, items []chatTestItem) *Manager {
	t.Helper()
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1), reconcileWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, start_message_id, upper_message_id, created_at, updated_at) VALUES (?, 'tg://x','channel','channel:items',1,'test','account',?,'completed','{}',0,1000,?,?)`, jobID, status, now, now); err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if _, err := db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, dialog_id, message_id, original_name, status, attempts, elapsed_ms, started_at, discovered_at) VALUES (?, 'channel:items', 1, ?, 'f.bin', ?, ?, ?, ?, ?)`,
			jobID, item.messageID, item.status, item.attempts, item.elapsedMs, item.startedAt, now); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

type chatTestItem struct {
	messageID int
	status    string
	attempts  int
	elapsedMs int
	startedAt string
}

// Completion writes the item's terminal state and the global ownership record
// every other task consults, and the two have to agree.
//
// They used to be two statements of which the second ran regardless of what the
// first had done. A row that was no longer there - cancelled a moment earlier,
// or deleted with its task - still had an ownership record written for it, and
// that record says "this media is downloaded" about a file nobody has. The
// message path has written it as one statement since the divergence was found
// there; this pins the session path to the same shape.
func TestPostgresCompletingAChatItemOnlyWritesOwnershipForARowThatExists(t *testing.T) {
	m := openChatItemTestManager(t, "chat-complete", ChatStatusDownloading, []chatTestItem{
		{messageID: 1, status: "downloaded"},
	})
	item := source{Item: Item{DialogKey: "channel:items", MessageID: 1}}

	if err := m.setChatItem("chat-complete", item, "completed", "/downloads/ok/1.bin", ""); err != nil {
		t.Fatal(err)
	}
	var status, path string
	if err := m.db.QueryRow(`SELECT status, final_path FROM chat_download_items WHERE chat_job_id = 'chat-complete' AND message_id = 1`).Scan(&status, &path); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || path != "/downloads/ok/1.bin" {
		t.Fatalf("the item settled as %q/%q", status, path)
	}
	var owner string
	if err := m.db.QueryRow(`SELECT owner_id FROM downloaded_media WHERE dialog_key = 'channel:items' AND message_id = 1`).Scan(&owner); err != nil {
		t.Fatalf("the completed file has no ownership record: %v", err)
	}
	if owner != "chat-complete" {
		t.Fatalf("the file is owned by %q", owner)
	}

	// The row that is not there. Nothing may be claimed on its behalf: an
	// ownership record for a row nobody has is what makes every other task skip
	// a file that was never downloaded.
	missing := source{Item: Item{DialogKey: "channel:items", MessageID: 999}}
	if err := m.setChatItem("chat-complete", missing, "completed", "/downloads/ghost/999.bin", ""); err != nil {
		t.Fatal(err)
	}
	var ghost int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE dialog_key = 'channel:items' AND message_id = 999`).Scan(&ghost); err != nil {
		t.Fatal(err)
	}
	if ghost != 0 {
		t.Fatal("completing a row that does not exist claimed the media anyway, so every " +
			"other task now reads it as downloaded and never fetches it")
	}
}

// A paused task pauses all of its work, including the files that were waiting
// on another task's claim.
//
// 暂停 asks the task to stop. A waiting file left out of it is not stopped: no
// later statement selects it, because resuming looks for paused rows and this
// one was never made one - so the file is only ever promoted back by the claim
// reconciler, and only if its owner happens to finish.
func TestPostgresPausingAChatTaskAlsoPausesWaitingFiles(t *testing.T) {
	m := openChatItemTestManager(t, "chat-pause", ChatStatusDownloading, []chatTestItem{
		{messageID: 1, status: "queued"},
		{messageID: 2, status: "waiting"},
		{messageID: 3, status: "running", startedAt: time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339Nano)},
	})

	if err := m.PauseChat("chat-pause"); err != nil {
		t.Fatal(err)
	}

	for _, id := range []int{1, 2, 3} {
		var status, finishedAt string
		if err := m.db.QueryRow(`SELECT status, finished_at FROM chat_download_items WHERE chat_job_id = 'chat-pause' AND message_id = ?`, id).Scan(&status, &finishedAt); err != nil {
			t.Fatal(err)
		}
		if status != "paused" {
			t.Errorf("file %d is %q after the task was paused, want paused", id, status)
		}
		if finishedAt != "" {
			t.Errorf("file %d carries a finish time while paused: %q", id, finishedAt)
		}
	}
}

// Resuming a task hands back the whole attempt budget.
//
// A file that ran its attempts down before the pause would otherwise be one
// stall away from being given up on, so pressing 恢复 would return a task whose
// files are already out of retries. The message path resets the counter on
// resume for that reason; this is the same decision on the other table.
func TestPostgresResumingAChatTaskRestoresTheAttemptBudget(t *testing.T) {
	m := openChatItemTestManager(t, "chat-resume", ChatStatusPaused, []chatTestItem{
		{messageID: 1, status: "paused", attempts: maxStalledAttempts, elapsedMs: 42000},
	})

	if err := m.ResumeChat("chat-resume"); err != nil {
		t.Fatal(err)
	}

	var status string
	var attempts, elapsed int
	if err := m.db.QueryRow(`SELECT status, attempts, elapsed_ms FROM chat_download_items WHERE chat_job_id = 'chat-resume' AND message_id = 1`).Scan(&status, &attempts, &elapsed); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("the file is %q after the task was resumed, want queued", status)
	}
	if attempts != 0 {
		t.Errorf("the file kept %d attempts, so a person resuming a task gets a file with no retries left", attempts)
	}
	if elapsed != 0 {
		t.Errorf("the file kept %d ms of elapsed time across a pause", elapsed)
	}
}
