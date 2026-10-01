package download

import (
	"context"
	"os"
	"testing"
	"time"
)

// savedTaskFixture opens the disposable database and seeds one chat task.
type savedTaskFixture struct {
	t  *testing.T
	db *database
	m  *Manager
}

func newSavedTaskFixture(t *testing.T) *savedTaskFixture {
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
	m := &Manager{db: db, progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatalf("migratePostgres(): %v", err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	return &savedTaskFixture{t: t, db: db, m: m}
}

// seedSaved inserts the account's Saved Messages task in one state.
func (f *savedTaskFixture) seedSaved(id, accountID string, startMessage, listenNew int, status, scanState string) {
	f.t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := f.db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, listen_new, status, scan_state, created_at, updated_at)
		VALUES (?, ?, 'self', ?, 1, '收藏消息', ?, ?, 900, ?, ?, ?, ?, ?)`,
		id, savedSourcePrefix+accountID, "self:"+accountID, accountID, startMessage, listenNew, status, scanState, now, now); err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.db.Exec(`INSERT INTO chat_download_stats(chat_job_id) VALUES (?)`, id); err != nil {
		f.t.Fatal(err)
	}
	for _, kind := range append(append([]string(nil), chatStreamKinds...), listenerGapStream) {
		if _, err := f.db.Exec(`INSERT INTO chat_download_streams(chat_job_id, stream_kind) VALUES (?, ?)`, id, kind); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *savedTaskFixture) seedItem(jobID, dialogKey string, messageID int, status string, attempts int) {
	f.t.Helper()
	if _, err := f.db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, status, attempts, discovered_at) VALUES (?, ?, ?, ?, ?, ?)`, jobID, dialogKey, messageID, status, attempts, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		f.t.Fatal(err)
	}
}

func (f *savedTaskFixture) job(id string) ChatJob {
	f.t.Helper()
	job, err := f.m.GetChat(id)
	if err != nil {
		f.t.Fatal(err)
	}
	return job
}

func (f *savedTaskFixture) itemStatus(jobID, dialogKey string, messageID int) (string, int) {
	f.t.Helper()
	var status string
	var attempts int
	if err := f.db.QueryRow(`SELECT status, attempts FROM chat_download_items WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ?`, jobID, dialogKey, messageID).Scan(&status, &attempts); err != nil {
		f.t.Fatal(err)
	}
	return status, attempts
}

func (f *savedTaskFixture) stream(jobID, kind string) (int, int) {
	f.t.Helper()
	var offset, completed int
	if err := f.db.QueryRow(`SELECT offset_message_id, completed FROM chat_download_streams WHERE chat_job_id = ? AND stream_kind = ?`, jobID, kind).Scan(&offset, &completed); err != nil {
		f.t.Fatal(err)
	}
	return offset, completed
}

// An account has one Saved Messages task, and the lookup that decides that has
// to see a finished one. It did not before: the query that guarded against a
// duplicate only looked at tasks that were still running, so asking for the
// history again after the first task had completed built a second task over the
// same dialog - and the two then raced for every file.
func TestPostgresSavedTaskLookupFindsTheTaskInAnyState(t *testing.T) {
	f := newSavedTaskFixture(t)
	for _, status := range []string{ChatStatusQueued, ChatStatusScanning, ChatStatusDownloading, ChatStatusListening, ChatStatusPaused, ChatStatusCompleted, ChatStatusPartial, ChatStatusFailed, ChatStatusCancelled} {
		f.t.Run(status, func(t *testing.T) {
			if err := clearPostgresDownloadTestData(f.db); err != nil {
				t.Fatal(err)
			}
			f.seedSaved("saved-1", "acc", 0, 0, status, chatScanPending)
			id, found, err := f.m.keepOneSavedChat("acc")
			if err != nil {
				t.Fatal(err)
			}
			if !found || id != "saved-1" {
				t.Fatalf("a %s saved task was not found (id=%q found=%v); /saved_all would create a second one", status, id, found)
			}
		})
	}
	// A purged task is gone, and the next request is a request to build one.
	if err := clearPostgresDownloadTestData(f.db); err != nil {
		t.Fatal(err)
	}
	f.seedSaved("saved-1", "acc", 0, 0, ChatStatusDeleted, chatScanCompleted)
	if _, found, err := f.m.keepOneSavedChat("acc"); err != nil || found {
		t.Fatalf("a deleted saved task was reused (found=%v, err=%v)", found, err)
	}
	// And one account's task is not another's.
	f.seedSaved("saved-2", "other", 0, 0, ChatStatusCompleted, chatScanCompleted)
	if id, found, err := f.m.keepOneSavedChat("acc"); err != nil || found {
		t.Fatalf("another account's saved task was returned (id=%q found=%v, err=%v)", id, found, err)
	}
}

// The two-task era left a database that can hold both shapes at once, and the
// index makes the choice between them load-bearing: a listen-only task moved
// onto the range a running history task already holds is refused outright, so a
// lookup that returned the wrong row would make /saved_all fail with a
// duplicate-key error instead of scanning anything.
func TestPostgresRescanningTheWrongSavedRowIsRefused(t *testing.T) {
	f := newSavedTaskFixture(t)
	seedBothShapes := func(t *testing.T) {
		t.Helper()
		if err := clearPostgresDownloadTestData(f.db); err != nil {
			t.Fatal(err)
		}
		f.seedSaved("saved-history", "acc", 0, 0, ChatStatusDownloading, chatScanCompleted)
		// Created second, so a lookup ordered by age alone would pick this one.
		f.seedSaved("saved-listen", "acc", -1, 1, ChatStatusListening, chatScanCompleted)
	}
	seedBothShapes(t)
	if err := f.m.rescanSavedChat("saved-listen", 950); err == nil {
		t.Fatal("rescanning the listen-only task was allowed while a history task holds its range; the index should refuse to move it there")
	}
	// The row the lookup prefers can be rescanned in place.
	if err := f.m.rescanSavedChat("saved-history", 950); err != nil {
		t.Fatalf("rescanning the history task failed: %v", err)
	}
}

// Which is why the lookup prefers the history task and retires the other: one
// account has one Saved Messages task, and after the first command touches the
// account there is only one left to reach.
func TestPostgresSavedChatsCollapseToOne(t *testing.T) {
	f := newSavedTaskFixture(t)
	f.seedSaved("saved-history", "acc", 0, 0, ChatStatusDownloading, chatScanCompleted)
	// Created second, so a lookup ordered by age alone would pick this one.
	f.seedSaved("saved-listen", "acc", -1, 1, ChatStatusListening, chatScanCompleted)
	f.seedSaved("saved-other", "other", 0, 1, ChatStatusListening, chatScanCompleted)

	id, found, err := f.m.keepOneSavedChat("acc")
	if err != nil {
		t.Fatal(err)
	}
	if !found || id != "saved-history" {
		t.Fatalf("kept %q (found=%v), want the task that carries the history range", id, found)
	}
	// The other one is stopped and hidden. Deleted is the status every reader of
	// the listening flag already pairs it with - the listener snapshot, the gap
	// walk, the admission lookup and the toggle all name the statuses they act
	// on - so hiding the row is what takes it out of the listener on its own,
	// and the flag it leaves behind is inert.
	job := f.job("saved-listen")
	if job.Status != ChatStatusDeleted {
		t.Fatalf("the other saved task is %q, want it retired", job.Status)
	}
	// And a running one is stopped through the path that releases what it holds.
	var claims int
	if err := f.db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_kind = 'chat'`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("%d media claims outlived the retired task", claims)
	}
	// Another account's task is untouched, and the account now has one task.
	if other := f.job("saved-other"); other.Status != ChatStatusListening {
		t.Fatalf("another account's task became %q", other.Status)
	}
	jobs, total, _, err := f.m.ListChats("", 10, SavedChatsFilter("acc"))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || total != 1 || jobs[0].ID != "saved-history" {
		t.Fatalf("the account's saved list = %v (total %d), want exactly the kept task", chatJobIDs(jobs), total)
	}
	// Running it a second time is a no-op rather than a second retirement.
	again, found, err := f.m.keepOneSavedChat("acc")
	if err != nil || !found || again != "saved-history" {
		t.Fatalf("second lookup = %q, %v, %v", again, found, err)
	}
}

// Re-scanning is the only thing /saved_all does to a task that already exists,
// so it has to do all four parts: freeze the new upper bound, walk the history
// again, offer the files that never made it, and leave the published ones and
// the listener's own cursor alone.
func TestPostgresRescanSavedTaskWalksTheHistoryAgain(t *testing.T) {
	f := newSavedTaskFixture(t)
	f.seedSaved("saved-1", "acc", 0, 0, ChatStatusCompleted, chatScanCompleted)
	f.seedItem("saved-1", "self:acc", 1, "completed", 1)
	f.seedItem("saved-1", "self:acc", 2, "failed", 7)
	f.seedItem("saved-1", "self:acc", 3, "queued", 0)
	// A walk that finished leaves its cursors at the bottom and marked done.
	if _, err := f.db.Exec(`UPDATE chat_download_streams SET offset_message_id = 400, completed = 1 WHERE chat_job_id = 'saved-1' AND stream_kind != ?`, listenerGapStream); err != nil {
		t.Fatal(err)
	}
	// The listener's own walk has its own position and must keep it.
	if _, err := f.db.Exec(`UPDATE chat_download_streams SET offset_message_id = 777 WHERE chat_job_id = 'saved-1' AND stream_kind = ?`, listenerGapStream); err != nil {
		t.Fatal(err)
	}

	if err := f.m.rescanSavedChat("saved-1", 950); err != nil {
		t.Fatal(err)
	}

	job := f.job("saved-1")
	if job.Status != ChatStatusQueued || job.ScanState != chatScanPending {
		t.Fatalf("re-scanned task is %s/%s, want queued/pending: nothing would walk it", job.Status, job.ScanState)
	}
	if job.StartMessageID != 0 || job.UpperMessageID != 950 {
		t.Fatalf("re-scanned range is %d—%d, want 0—950", job.StartMessageID, job.UpperMessageID)
	}
	// The file that failed is offered again, with a fresh attempt budget; the
	// one that was already published is not downloaded a second time.
	if status, attempts := f.itemStatus("saved-1", "self:acc", 2); status != "queued" || attempts != 0 {
		t.Fatalf("failed file is %q with %d attempts, want queued with a fresh budget", status, attempts)
	}
	if status, attempts := f.itemStatus("saved-1", "self:acc", 1); status != "completed" || attempts != 1 {
		t.Fatalf("published file was reset to %q/%d attempts, so the re-scan downloads it again", status, attempts)
	}
	for _, kind := range chatStreamKinds {
		if offset, completed := f.stream("saved-1", kind); offset != 0 || completed != 0 {
			t.Fatalf("stream %s is at offset %d completed=%d, so the scan would not walk again", kind, offset, completed)
		}
	}
	if offset, completed := f.stream("saved-1", listenerGapStream); offset != 777 || completed != 0 {
		t.Fatalf("the listener cursor moved to %d/%d; the history scan and the listener walk are different questions", offset, completed)
	}
}

// A listener's flag can be turned on and off at any point in a task's life.
// Two of the requests the controls actually send used to fail: stopping while
// the history downloads, and starting to listen right after asking for that
// download. Neither is a reason to refuse - the flag is a wish, and the
// listener acts on it once the task has a settled scan.
func TestPostgresChatListeningTogglesWhileTheTaskIsRunning(t *testing.T) {
	f := newSavedTaskFixture(t)
	for _, status := range []string{ChatStatusQueued, ChatStatusScanning, ChatStatusDownloading} {
		f.t.Run("enable during "+status, func(t *testing.T) {
			if err := clearPostgresDownloadTestData(f.db); err != nil {
				t.Fatal(err)
			}
			f.seedSaved("saved-1", "acc", 0, 0, status, chatScanPending)
			if err := f.m.SetChatListening("saved-1", true); err != nil {
				t.Fatalf("enabling listening on a %s task failed: %v", status, err)
			}
			job := f.job("saved-1")
			if !job.ListenNew {
				t.Fatal("the flag was not set, so the listener would never start")
			}
			if job.Status != status {
				t.Fatalf("status became %q; a task that is still working must not claim to be listening", job.Status)
			}
		})
	}
	// Disabling is what /saved_listen's stop direction sends, and it has to work
	// on a task that is still downloading: the flag is the only thing it means.
	if err := clearPostgresDownloadTestData(f.db); err != nil {
		t.Fatal(err)
	}
	f.seedSaved("saved-1", "acc", 0, 1, ChatStatusDownloading, chatScanPending)
	if err := f.m.SetChatListening("saved-1", false); err != nil {
		t.Fatalf("stopping listening on a downloading task failed: %v", err)
	}
	if job := f.job("saved-1"); job.ListenNew || job.Status != ChatStatusDownloading {
		t.Fatalf("after stopping the task is %q listening=%v, want the status kept and the flag cleared", job.Status, job.ListenNew)
	}
	// A settled task still becomes the listening state it is.
	if err := f.m.SetChatListening("saved-1", true); err != nil {
		t.Fatal(err)
	}
	if job := f.job("saved-1"); job.Status != ChatStatusDownloading {
		t.Fatalf("a downloading task jumped to %q", job.Status)
	}
	// And a cancelled task is not something to listen on.
	if err := clearPostgresDownloadTestData(f.db); err != nil {
		t.Fatal(err)
	}
	f.seedSaved("saved-1", "acc", 0, 0, ChatStatusCancelled, chatScanCompleted)
	if err := f.m.SetChatListening("saved-1", true); err == nil {
		t.Fatal("a cancelled task accepted a listening request")
	}
}

// The saved list is the session list narrowed to one account, so the narrowing
// has to mean exactly that: this account's saved task, none of its other tasks,
// and no other account's.
func TestPostgresChatListNarrowsToTheAccountsSavedTask(t *testing.T) {
	f := newSavedTaskFixture(t)
	f.seedSaved("saved-acc", "acc", 0, 0, ChatStatusCompleted, chatScanCompleted)
	f.seedSaved("saved-other", "other", 0, 0, ChatStatusCompleted, chatScanCompleted)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := f.db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES ('channel-acc', 'tg://chat', 'channel', 'channel:1', 1, '某频道', 'acc', 'completed', 'completed', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}

	jobs, total, next, err := f.m.ListChats("", 10, SavedChatsFilter("acc"))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != "saved-acc" {
		t.Fatalf("saved list = %v, want only this account's saved task", chatJobIDs(jobs))
	}
	if total != 1 {
		t.Fatalf("saved list total = %d, want 1: the count must describe the rows the filter selects", total)
	}
	if next != "" {
		t.Fatalf("saved list offered a next cursor (%q) with one row", next)
	}
	// The unfiltered list is unchanged: it still holds every task. Its total is
	// the counter maintained by the manager rather than a query, and these rows
	// were inserted directly, so only the rows are asserted here.
	all, _, _, err := f.m.ListChats("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("unfiltered list = %v, want all three tasks", chatJobIDs(all))
	}
}

// Paging a filtered list is where the filter and the cursor have to be handed
// to one statement in the right order. The clause and its argument are built
// separately, so a filter added in the wrong place pages over the wrong rows
// while still returning a page-shaped answer.
func TestPostgresChatListPagesWithinTheSavedFilter(t *testing.T) {
	f := newSavedTaskFixture(t)
	for index, id := range []string{"saved-1", "saved-2", "saved-3"} {
		f.seedSaved(id, "acc", 0, 0, ChatStatusCompleted, chatScanCompleted)
		// created_at orders the list, and the fixture writes one timestamp.
		if _, err := f.db.Exec(`UPDATE chat_download_jobs SET created_at = ? WHERE id = ?`, time.Date(2026, 1, 1, 0, index, 0, 0, time.UTC).Format(time.RFC3339Nano), id); err != nil {
			t.Fatal(err)
		}
	}
	f.seedSaved("saved-other", "other", 0, 0, ChatStatusCompleted, chatScanCompleted)

	seen := make([]string, 0, 3)
	cursor := ""
	for page := 0; page < 4; page++ {
		jobs, total, next, err := f.m.ListChats(cursor, 1, SavedChatsFilter("acc"))
		if err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Fatalf("page %d reported total %d, want the three rows the filter selects", page+1, total)
		}
		if len(jobs) != 1 {
			t.Fatalf("page %d returned %d rows, want one: %v", page+1, len(jobs), chatJobIDs(jobs))
		}
		seen = append(seen, jobs[0].ID)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 3 || seen[0] != "saved-3" || seen[1] != "saved-2" || seen[2] != "saved-1" {
		t.Fatalf("paged saved list = %v, want the account's three tasks newest first", seen)
	}
}

func chatJobIDs(jobs []ChatJob) []string {
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.ID)
	}
	return ids
}
