package download

import (
	"context"
	"os"
	"testing"
	"time"
)

// settleRaceManager gives the settle path a real database with one task whose
// media turned out to belong to a session task, which is the state the worker
// finds itself in when every file it tried to claim came back "waiting".
func settleRaceManager(t *testing.T, status string) *Manager {
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
	m := &Manager{db: db, events: newEventBus(), chatWatched: map[string]map[string]struct{}{}}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES ('settle-race', 'tg://test', ?, ?, ?)`, status, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('settle-race', 'channel', 'channel:9', 9, 1, 'file.bin', ?)`, status); err != nil {
		t.Fatal(err)
	}
	return m
}

func settleRaceItem(t *testing.T, m *Manager) string {
	t.Helper()
	var status string
	if err := m.db.QueryRow(`SELECT status FROM download_items WHERE job_id = 'settle-race' AND message_id = 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func settleRaceJob(t *testing.T, m *Manager) string {
	t.Helper()
	var status string
	if err := m.db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'settle-race'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// Cancelling a task whose files are all owned by another task used to undo
// itself. The worker parks the files as "waiting" and then settles the task as
// "queued"; the claim reconciler promotes the waiting files of a queued task,
// which restarts the download the user stopped. Both writes have to refuse to
// touch a task that is no longer running.
func TestCancelledTaskIsNotRevivedByTheSettlePath(t *testing.T) {
	m := settleRaceManager(t, "cancelled")
	if err := m.setMessageItemsWaiting("settle-race", []source{{Item: Item{DialogKey: "channel:9", MessageID: 1}}}); err != nil {
		t.Fatal(err)
	}
	if got := settleRaceItem(t, m); got != "cancelled" {
		t.Fatalf("item status=%q after the settle path, want the cancelled state the user asked for", got)
	}
	m.finishTaskWithoutPendingWork("settle-race", true)
	if got := settleRaceJob(t, m); got != "cancelled" {
		t.Fatalf("task status=%q after settling, want cancelled; a queued task is picked up again, so this restarts the download", got)
	}
}

// The same guard must not stop the settle path from doing its job. A task that
// really is running still parks its files and still hands its slot back.
func TestRunningTaskIsStillSettledByTheSettlePath(t *testing.T) {
	m := settleRaceManager(t, "running")
	if err := m.setMessageItemsWaiting("settle-race", []source{{Item: Item{DialogKey: "channel:9", MessageID: 1}}}); err != nil {
		t.Fatal(err)
	}
	if got := settleRaceItem(t, m); got != "waiting" {
		t.Fatalf("item status=%q, want waiting: a running task's unclaimed files must still be parked", got)
	}
	m.finishTaskWithoutPendingWork("settle-race", true)
	if got := settleRaceJob(t, m); got != "queued" {
		t.Fatalf("task status=%q, want queued so the task is picked up when its files are released", got)
	}
}
