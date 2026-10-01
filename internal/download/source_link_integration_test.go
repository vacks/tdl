package download

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestPostgresTaskLinkIsDerivedOnRead covers the wiring rather than the rule.
// The rule itself is unit-tested, but a unit test of MessageJumpURL still
// passes if the list and get paths stop calling it - which is the failure this
// guards, because the bug being fixed was precisely a task whose link existed
// but was never put in front of the Web UI.
//
// The seeded row is the reported shape: a channel task created by a reaction,
// whose stored source URL is internal bookkeeping and therefore useless to a
// user, with the channel and message IDs living only in the URL and the files.
func TestPostgresTaskLinkIsDerivedOnRead(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, status, created_at, updated_at) VALUES ('link-reaction', 'tg://reaction/channel/1934225758/9522', 'channel', 'channel:1934225758', '你有一条新的资源', 'completed', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// The list must not need this row, and does not read it; it is here so the
	// get path is exercised against a task that has its files loaded.
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, original_name, status) VALUES ('link-reaction', 'channel', 'channel:1934225758', 1934225758, 9522, 'video.mov', 'completed')`); err != nil {
		t.Fatal(err)
	}
	if err := m.loadVisibleCounts(); err != nil {
		t.Fatal(err)
	}

	const want = "https://t.me/c/1934225758/9522"

	jobs, _, _, err := m.ListCursor("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("list returned %d tasks, want 1", len(jobs))
	}
	if jobs[0].SourceLink != want {
		t.Fatalf("list SourceLink = %q, want %q", jobs[0].SourceLink, want)
	}

	job, err := m.Get("link-reaction")
	if err != nil {
		t.Fatal(err)
	}
	if job.SourceLink != want {
		t.Fatalf("get SourceLink = %q, want %q", job.SourceLink, want)
	}
}
