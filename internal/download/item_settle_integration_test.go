package download

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	transfer "github.com/vacks/tdl/internal/download/transfer"
	"github.com/vacks/tdl/internal/tmsg"
)

// A file that produced nothing has to leave the row in a state the rest of the
// system understands, and which state that is depends on whether another
// attempt could change the answer.
//
// The row was set to "running" by the first progress callback, and nothing else
// in this program revisits a running row. A batch that lost one file to a
// dropped connection therefore left that row running for good: the task was
// pinned in 下载中, the working directory was removed under it, and the file was
// gone with no error anywhere. Both tables have to settle it, and they have to
// agree - the failure of a link task and of a listened channel differs only in
// which table it is written to.
func TestPostgresTransferOutcomesSettleBothTaskTables(t *testing.T) {
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

	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('settle-job','tg://x','channel','channel:settle','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('settle-chat','tg://x','channel','channel:settle',1,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	// One message per case, in both tables:
	//   101  a transfer that failed and has attempts left -> back on the queue
	//   102  a transfer that failed with the budget spent   -> failed
	//   103  a message that is gone                        -> failed, and says so
	//   104  a file that was already delivered             -> untouched
	for _, table := range []itemTable{messageItems, chatItems} {
		for _, id := range []int{101, 102, 103, 104} {
			owner, status, attempts, path := "settle-job", "running", 1, ""
			if table.items == chatItems.items {
				owner = "settle-chat"
			}
			if id == 102 {
				attempts = maxStalledAttempts
			}
			if id == 104 {
				status, path = "completed", "/downloads/settle/104.bin"
			}
			// The two tables are the same shape but not identical: only the
			// session task's rows carry the time they were discovered.
			columns := table.owner + `, dialog_key, dialog_id, message_id, original_name, status, attempts, final_path, started_at`
			values := []any{owner, id, "f.bin", status, attempts, path, now}
			if table.items == chatItems.items {
				columns += ", discovered_at"
				values = append(values, now)
			}
			placeholders := "?, 'channel:settle', 1, ?, ?, ?, ?, ?, ?"
			if table.items == chatItems.items {
				placeholders += ", ?"
			}
			if _, err := db.Exec(`INSERT INTO `+table.items+`(`+columns+`) VALUES (`+placeholders+`)`, values...); err != nil {
				t.Fatalf("%s: %v", table.items, err)
			}
		}
	}

	outcomes := []transfer.FileOutcomeUpdate{
		{MessageID: 101, Err: errors.New("read tcp: connection reset by peer")},
		{MessageID: 102, Err: errors.New("read tcp: connection reset by peer")},
		{MessageID: 103, Err: tmsg.ErrMessageDeleted},
	}
	byMessage := map[int]source{}
	for _, id := range []int{101, 102, 103, 104} {
		byMessage[id] = source{Item: Item{DialogKey: "channel:settle", MessageID: id}}
	}
	planned := planItemFailures(outcomes, byMessage)

	for _, table := range []itemTable{messageItems, chatItems} {
		owner := "settle-job"
		if table.items == chatItems.items {
			owner = "settle-chat"
		}
		settled, requeued, err := m.applyItemFailures(table, owner, planned)
		if err != nil {
			t.Fatalf("%s: %v", table.items, err)
		}
		if settled == 0 {
			t.Errorf("%s: nothing was settled", table.items)
		}
		if !requeued {
			t.Errorf("%s: a file with attempts left was not reported as requeued", table.items)
		}

		type row struct {
			status, error, finalPath, startedAt string
		}
		read := func(id int) row {
			t.Helper()
			var got row
			if err := db.QueryRow(`SELECT status, error, final_path, started_at FROM `+table.items+` WHERE `+table.owner+` = ? AND message_id = ?`, owner, id).
				Scan(&got.status, &got.error, &got.finalPath, &got.startedAt); err != nil {
				t.Fatalf("%s message %d: %v", table.items, id, err)
			}
			return got
		}

		if got := read(101); got.status != "queued" || got.startedAt != "" {
			t.Errorf("%s: a failure with attempts left settled as %+v, want it queued with no start time", table.items, got)
		}
		if got := read(102); got.status != "failed" {
			t.Errorf("%s: a failure with the budget spent settled as %q, want failed", table.items, got.status)
		}
		if got := read(103); got.status != "failed" || got.error != "消息已删除" {
			t.Errorf("%s: a deleted message settled as %+v, want failed/消息已删除", table.items, got)
		}
		// A published file is not un-published by a report about an attempt at
		// it, which is why the write carries the same exclusion fail() does.
		if got := read(104); got.status != "completed" || got.finalPath != "/downloads/settle/104.bin" {
			t.Errorf("%s: a completed file was overwritten: %+v", table.items, got)
		}
	}
}

// The budget is what keeps a proxy that never comes back from retrying forever.
// Every attempt is another request against the account's Telegram allowance,
// which is the scarce thing this whole path is built around - so the third
// failure has to stop, and it has to say that it stopped rather than that the
// file is broken.
func TestPostgresARepeatedTransportFailureStopsAtTheBudget(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('budget-job','tg://x','channel','channel:budget','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_key, dialog_id, message_id, original_name, status, attempts) VALUES ('budget-job','channel:budget',1,7,'f.bin','running',0)`); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{DialogKey: "channel:budget", MessageID: 7}}
	failure := planItemFailures([]transfer.FileOutcomeUpdate{{MessageID: 7, Err: errors.New("connection reset by peer")}}, map[int]source{7: item})

	for attempt := 1; attempt <= maxStalledAttempts; attempt++ {
		// Every attempt is recorded the way a run records it, and every failure
		// is settled the way the batch settles it.
		if _, err := db.Exec(`UPDATE download_items SET attempts = attempts + 1, status = 'running' WHERE job_id = 'budget-job' AND message_id = 7`); err != nil {
			t.Fatal(err)
		}
		_, requeued, err := m.applyItemFailures(messageItems, "budget-job", failure)
		if err != nil {
			t.Fatal(err)
		}
		var status, message string
		if err := db.QueryRow(`SELECT status, error FROM download_items WHERE job_id = 'budget-job' AND message_id = 7`).Scan(&status, &message); err != nil {
			t.Fatal(err)
		}
		if attempt < maxStalledAttempts && (!requeued || status != "queued") {
			t.Fatalf("attempt %d of %d settled as %q (requeued %t), want queued", attempt, maxStalledAttempts, status, requeued)
		}
		if attempt == maxStalledAttempts {
			if requeued || status != "failed" {
				t.Fatalf("the attempt that spent the budget settled as %q (requeued %t), want failed", status, requeued)
			}
			if message == "" {
				t.Fatal("the file gave up without saying why")
			}
		}
	}
}

// The reason a file failed is what a person acts on, and the task-level
// sentence must not overwrite it.
//
// fail() writes one message onto every unfinished file of a task, which is the
// right answer when the task failed for one reason. It is the wrong answer when
// the batch already told each file apart: "the connection dropped" and "the
// message was deleted" call for different responses, and the whole point of
// carrying the engine's per-message report up here is to keep them apart. The
// task's own status still has to be decided, and that is all that is left.
func TestPostgresAPerFileReasonSurvivesTheTaskFailing(t *testing.T) {
	m := openChatItemTestManager(t, "unused", ChatStatusDownloading, nil)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := m.db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('reason-job','tg://x','channel','channel:reason','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	// One file the batch could not fetch, and one message that is gone.
	for id, status := range map[int]string{11: "running", 12: "running"} {
		if _, err := m.db.Exec(`INSERT INTO download_items(job_id, dialog_key, dialog_id, message_id, original_name, status, attempts) VALUES ('reason-job','channel:reason',1,?,'f.bin',?,1)`, id, status); err != nil {
			t.Fatal(err)
		}
	}
	byMessage := map[int]source{
		11: {Item: Item{DialogKey: "channel:reason", MessageID: 11}},
		12: {Item: Item{DialogKey: "channel:reason", MessageID: 12}},
	}
	settled, requeued, err := m.applyItemFailures(messageItems, "reason-job", planItemFailures([]transfer.FileOutcomeUpdate{
		{MessageID: 11, Err: &tgerr.Error{Code: 400, Type: tg.ErrMessageIDInvalid, Message: tg.ErrMessageIDInvalid}},
		{MessageID: 12, Err: tmsg.ErrMessageDeleted},
	}, byMessage))
	if err != nil {
		t.Fatal(err)
	}
	if settled != 2 || requeued {
		t.Fatalf("settled %d files (requeued %t), want 2 and no retries", settled, requeued)
	}

	m.settleMessageTask("reason-job", requeued, settled)

	for id, want := range map[int]string{11: tg.ErrMessageIDInvalid, 12: "消息已删除"} {
		var status, message string
		if err := m.db.QueryRow(`SELECT status, error FROM download_items WHERE job_id = 'reason-job' AND message_id = ?`, id).Scan(&status, &message); err != nil {
			t.Fatal(err)
		}
		if status != "failed" {
			t.Errorf("file %d is %q, want failed", id, status)
		}
		if !strings.Contains(message, want) {
			t.Errorf("file %d says %q, which does not carry %q: the task's own sentence overwrote "+
				"the reason the batch knew, which is the one a person decides from", id, message, want)
		}
	}
	var jobStatus, jobError string
	if err := m.db.QueryRow(`SELECT status, error FROM download_jobs WHERE id = 'reason-job'`).Scan(&jobStatus, &jobError); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "failed" {
		t.Errorf("the task is %q, want failed", jobStatus)
	}
	if jobError == "" {
		t.Error("the task failed without saying anything")
	}
}

// A task with a file waiting for another attempt is not a finished task.
//
// The whole point of retrying a dropped connection is that the task carries on.
// Settling it as failed - or as partial - ends it while one of its files is on
// the queue, and the next worker pass then finds a task that is not queued and
// a file that never transfers.
func TestPostgresATaskWithARetryIsNotSettledAsFinished(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('retry-job','tg://x','channel','channel:retry','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO download_items(job_id, dialog_key, dialog_id, message_id, original_name, status, attempts) VALUES ('retry-job','channel:retry',1,5,'f.bin','running',1)`); err != nil {
		t.Fatal(err)
	}
	item := source{Item: Item{DialogKey: "channel:retry", MessageID: 5}}
	settled, requeued, err := m.applyItemFailures(messageItems, "retry-job", planItemFailures(
		[]transfer.FileOutcomeUpdate{{MessageID: 5, Err: errors.New("read tcp: connection reset by peer")}},
		map[int]source{5: item}))
	if err != nil {
		t.Fatal(err)
	}
	if !requeued || settled != 1 {
		t.Fatalf("settled %d (requeued %t), want one file back on the queue", settled, requeued)
	}

	m.settleMessageTask("retry-job", requeued, settled)

	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM download_jobs WHERE id = 'retry-job'`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "queued" {
		t.Fatalf("a task with a file waiting for another attempt settled as %q, want queued: "+
			"nothing will pick it up again", jobStatus)
	}
	var itemStatus string
	if err := db.QueryRow(`SELECT status FROM download_items WHERE job_id = 'retry-job' AND message_id = 5`).Scan(&itemStatus); err != nil {
		t.Fatal(err)
	}
	if itemStatus != "queued" {
		t.Fatalf("the file is %q, want queued", itemStatus)
	}
}

// The two tables differ in exactly two ways, and both are decisions rather than
// accidents. This pins them, because the merge that gave them one writer is
// exactly the change that could quietly level them.
//
// A file that fails gives up its ownership claim immediately on the session
// side, where a batch carries on past one failure and the file it names would
// otherwise wait for a task that has already given up on it. On the message
// side the claim is released a moment later, at task level, by fail() - and
// releasing it here as well would be a second writer of the same decision.
func TestPostgresTheTwoTablesReleaseClaimsDifferentlyOnFailure(t *testing.T) {
	m := openChatItemTestManager(t, "rel-chat", ChatStatusDownloading, nil)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := m.db.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, status, created_at, updated_at) VALUES ('rel-job','tg://x','channel','channel:rel','test','account','running',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := m.db.Exec(`INSERT INTO download_items(job_id, dialog_key, dialog_id, message_id, original_name, status) VALUES ('rel-job','channel:rel',1,1,'f','running')`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.db.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, dialog_id, message_id, original_name, status, discovered_at) VALUES ('rel-chat','channel:rel',1,2,'f','running',?)`, now); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key, kind, owner string
		messageID        int
	}{
		{"channel:rel", "message", "rel-job", 1},
		{"channel:rel", "chat", "rel-chat", 2},
	} {
		if _, err := m.db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?, ?, 'claimed', ?, ?, ?)`, row.key, row.messageID, row.kind, row.owner, now); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.setItemState(messageItems, "", source{Item: Item{DialogKey: "channel:rel", MessageID: 1}}, "failed", "", "boom"); err != nil {
		t.Fatal(err)
	}
	if err := m.setItemState(chatItems, "rel-chat", source{Item: Item{DialogKey: "channel:rel", MessageID: 2}}, "failed", "", "boom"); err != nil {
		t.Fatal(err)
	}

	var messageClaim, chatClaim int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_kind = 'message' AND owner_id = 'rel-job'`).Scan(&messageClaim); err != nil {
		t.Fatal(err)
	}
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_kind = 'chat' AND owner_id = 'rel-chat'`).Scan(&chatClaim); err != nil {
		t.Fatal(err)
	}
	if chatClaim != 0 {
		t.Fatal("a failed session file kept its claim; the task it belongs to has given up on it, " +
			"and every other task wanting that media waits behind a claim nothing will release")
	}
	if messageClaim != 1 {
		t.Fatal("a failed message file released its claim here, which is not this writer's decision: " +
			"the task releases everything it holds when it fails, and two writers of one decision is what " +
			"this merge exists to remove")
	}
}
