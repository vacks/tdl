package download

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

func openSkipTestManager(t *testing.T) (*Manager, *database) {
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
	m := &Manager{db: db, progress: newProgressStore(), events: newEventBus()}
	if err := m.migratePostgres(); err != nil {
		t.Fatalf("migratePostgres(): %v", err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	return m, db
}

func inboxStatus(t *testing.T, db *database, table string, id int64) (string, int, string) {
	t.Helper()
	var status, recorded string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts, error FROM `+table+` WHERE id = ?`, id).Scan(&status, &attempts, &recorded); err != nil {
		t.Fatal(err)
	}
	return status, attempts, recorded
}

// An event whose request was answered must stop, and it must stop in a state
// that is neither "waiting" nor "failed". The bug this replaced retried the
// answer five times in half a minute, revived it hourly to twenty, and left a
// permanent 已停止重试 row in the Bot's list - for a message whose files simply
// did not match the filter, which no later attempt could change.
func TestPostgresAnsweredInboxEventsSettleSkipped(t *testing.T) {
	m, db := openSkipTestManager(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, peer_id, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','user:1',4891,'user',1,'❤','processing',1,?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:1',7,'channel',1,'processing',1,?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	reactionID := lastInsertID(t, db, `SELECT id FROM reaction_inbox`)
	messageID := lastInsertID(t, db, `SELECT id FROM chat_message_inbox`)

	if err := m.RetryReactionInbox(reactionID, 1, nothingToDo(ErrNoEligibleMedia)); err != nil {
		t.Fatal(err)
	}
	if err := m.retryChatMessageInbox(messageID, 1, nothingToDo(ErrNoEligibleMedia)); err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		table string
		id    int64
	}{{"reaction_inbox", reactionID}, {"chat_message_inbox", messageID}} {
		status, attempts, recorded := inboxStatus(t, db, testCase.table, testCase.id)
		if status != "skipped" {
			t.Fatalf("%s: an answered event settled as %q, want skipped", testCase.table, status)
		}
		// The attempt count is left where it was rather than being spent: the
		// budget was never used, so recording it as exhausted would misreport
		// what happened.
		if attempts != 1 {
			t.Fatalf("%s: attempts=%d, want 1 (unchanged)", testCase.table, attempts)
		}
		if recorded == "" {
			t.Fatalf("%s: the reason the event was skipped was not recorded", testCase.table)
		}
	}

	// The revive sweep is the other half of the fix: a skipped row is terminal
	// by construction, so offering it again would re-ask the question.
	if _, err := db.Exec(`UPDATE reaction_inbox SET updated_at = ? WHERE id = ?`, time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339Nano), reactionID); err != nil {
		t.Fatal(err)
	}
	if err := m.reviveExhaustedInboxEvents(); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := inboxStatus(t, db, "reaction_inbox", reactionID); status != "skipped" {
		t.Fatalf("a skipped event was revived as %q; an answer must not be re-asked", status)
	}
}

// A rejection still settles as failed and still stops at once, which is what
// makes the new status an addition rather than a replacement.
func TestPostgresRejectedInboxEventStillSpendsTheBudgetAndFails(t *testing.T) {
	m, db := openSkipTestManager(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:1',7,'channel',1,'processing',1,?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	messageID := lastInsertID(t, db, `SELECT id FROM chat_message_inbox`)
	if err := m.retryChatMessageInbox(messageID, 1, tgerr.New(401, "SESSION_REVOKED")); err != nil {
		t.Fatal(err)
	}
	status, attempts, _ := inboxStatus(t, db, "chat_message_inbox", messageID)
	if status != "failed" || attempts != inboxAttemptLimit {
		t.Fatalf("revoked session: status=%q attempts=%d, want failed/%d", status, attempts, inboxAttemptLimit)
	}
}

// Settled events are not work. This is the read side of the status: the list
// and the counts the Bot shows must exclude both terminal states, and they must
// do it through the same predicate the partial index carries, or the list falls
// back to sorting the whole queue on every page.
func TestPostgresSkippedEventsLeaveTheOpenQueue(t *testing.T) {
	m, db := openSkipTestManager(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	seed := func(status string, messageID, attempts int) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:1',?,'channel',1,?,?,?,?,?)`, messageID, status, attempts, now, now, now); err != nil {
			t.Fatal(err)
		}
	}
	seed("pending", 1, 1)
	// Seeded with the whole budget spent so it counts as fully stopped rather
	// than as one that is only resting, which is what makes the stopped figure
	// below able to tell the two terminal statuses apart.
	seed("failed", 2, inboxAttemptLimit)
	seed("done", 3, 1)
	seed("skipped", 4, 1)
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, peer_id, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','user:1',5,'user',1,'❤','skipped',1,?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}

	events, total, _, err := m.ListListenerEvents("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(events) != 2 {
		t.Fatalf("open events: total=%d listed=%d, want 2 and 2", total, len(events))
	}
	for _, event := range events {
		if event.Status != "pending" && event.Status != "failed" {
			t.Fatalf("a settled event (%s) was listed as open work", event.Status)
		}
	}

	processing, waiting, stopped, err := m.ListenerInboxCounts()
	if err != nil {
		t.Fatal(err)
	}
	if processing != 0 || waiting != 1 || stopped != 1 {
		t.Fatalf("counts: processing=%d waiting=%d stopped=%d, want 0/1/1", processing, waiting, stopped)
	}

	// The figure the /status card presents as a problem signal counts only
	// failures. A skipped event is not one, and inflating it would make a queue
	// that is working correctly look like one that is breaking.
	stoppedEvents, revivable, err := m.StoppedEventCounts()
	if err != nil {
		t.Fatal(err)
	}
	if stoppedEvents != 1 || revivable != 0 {
		t.Fatalf("stopped=%d revivable=%d, want 1 and 0", stoppedEvents, revivable)
	}
}

// The answer is revisable, and a new reaction is the only way to ask again:
// nothing else re-reads a settled row. Skipping must therefore be reopenable,
// or changing the file filter and reacting a second time would do nothing at
// all and would say nothing about why.
func TestPostgresQueueReactionReopensSkippedEvent(t *testing.T) {
	m, db := openSkipTestManager(t)
	intent := DownloadIntent{
		Source:  SourceReaction,
		Message: &MessageRef{DialogName: "chat", DialogID: 100, InputPeer: &tg.InputPeerChannel{ChannelID: 100, AccessHash: 5}, MessageID: 4891},
	}
	queued, err := m.QueueReaction(intent, "❤")
	if err != nil || !queued {
		t.Fatalf("first reaction: queued=%v err=%v, want true and no error", queued, err)
	}
	id := lastInsertID(t, db, `SELECT id FROM reaction_inbox`)
	if err := m.RetryReactionInbox(id, 1, nothingToDo(ErrNoEligibleMedia)); err != nil {
		t.Fatal(err)
	}

	queued, err = m.QueueReaction(intent, "❤")
	if err != nil {
		t.Fatal(err)
	}
	if !queued {
		t.Fatal("a second reaction did not reopen the skipped event")
	}
	status, attempts, recorded := inboxStatus(t, db, "reaction_inbox", id)
	if status != "pending" || attempts != 0 {
		t.Fatalf("reopened event: status=%q attempts=%d, want pending/0", status, attempts)
	}
	if recorded != "" {
		t.Fatalf("reopening kept the previous reason %q", recorded)
	}
	// Reopening must not multiply rows: the identity of an event is the
	// account, the dialog, the message and the emoji.
	var rows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM reaction_inbox`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("reopening created a second row: %d rows", rows)
	}
}

// Retention has to know about the new terminal status, or a settled event that
// is invisible in the list would be kept for the life of the database.
func TestPostgresCleanupRemovesSkippedEvents(t *testing.T) {
	m, db := openSkipTestManager(t)
	old := time.Now().UTC().AddDate(0, 0, -2).Format(time.RFC3339Nano)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, peer_id, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','user:1',1,'user',1,'❤','skipped',1,?,?,?)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:1',2,'channel',1,'skipped',1,?,?,?)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	// A pending event of the same age is work, not history, and must survive.
	if _, err := db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, peer_id, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','channel:1',3,'channel',1,'pending',1,?,?,?)`, now, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := m.cleanupPostgresHistory(1); err != nil {
		t.Fatal(err)
	}
	var reactions, settled, pending int
	if err := db.QueryRow(`SELECT (SELECT COUNT(1) FROM reaction_inbox), (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'skipped'), (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'pending')`).Scan(&reactions, &settled, &pending); err != nil {
		t.Fatal(err)
	}
	if reactions != 0 || settled != 0 {
		t.Fatalf("skipped events survived retention: reaction=%d message=%d", reactions, settled)
	}
	if pending != 1 {
		t.Fatalf("retention removed a pending event that is still work (%d left)", pending)
	}
}

// A transfer failure the task cannot recover from must end the task and release
// what it was holding. Returning it to the worker instead left the task in
// 下载中 with nothing in flight, its claim held against every other task that
// wanted the same file, and its attempt counter growing on every three second
// pass for as long as the process ran.
func TestPostgresFailChatTransferEndsTheTaskAndReleasesClaims(t *testing.T) {
	m, db := openSkipTestManager(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('chat-stuck','tg://chat','channel','channel:stuck',1,'test','account','downloading','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES ('channel:stuck', 1, 'claimed', 'chat', 'chat-stuck', ?)`, now); err != nil {
		t.Fatal(err)
	}

	m.failChatTransfer("chat-stuck", "会话下载配置快照无效")

	var status, recorded string
	if err := db.QueryRow(`SELECT status, error FROM chat_download_jobs WHERE id = 'chat-stuck'`).Scan(&status, &recorded); err != nil {
		t.Fatal(err)
	}
	if status != ChatStatusFailed {
		t.Fatalf("task status=%q, want %q", status, ChatStatusFailed)
	}
	if recorded == "" {
		t.Fatal("the task failed without recording why")
	}
	var claims int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_kind = 'chat' AND owner_id = 'chat-stuck'`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("%d media claims were left behind by a task that will never transfer", claims)
	}
}

// The same terminal path must not be reachable for a task that is already
// settled: a late failure report from a worker that lost a race with a cancel
// must not overwrite the user's decision.
func TestPostgresFailChatTransferLeavesSettledTasksAlone(t *testing.T) {
	m, db := openSkipTestManager(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('chat-cancelled','tg://chat','channel','channel:cancelled',1,'test','account','cancelled','completed','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	m.failChatTransfer("chat-cancelled", "会话下载配置快照无效")
	var status string
	if err := db.QueryRow(`SELECT status FROM chat_download_jobs WHERE id = 'chat-cancelled'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != ChatStatusCancelled {
		t.Fatalf("a settled task was moved to %q by a late failure", status)
	}
}

func lastInsertID(t *testing.T, db *database, query string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(query).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// A permanent failure marked at its source reaches the inbox as one, which is
// what makes the classification a property of the error rather than of the call
// site that happens to produce it.
func TestPostgresMarkedPermanentFailureStopsImmediately(t *testing.T) {
	m, db := openSkipTestManager(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, peer_id, emoji, status, attempts, next_attempt_at, created_at, updated_at) VALUES ('account','user:1',1,'user',1,'❤','processing',1,?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	id := lastInsertID(t, db, `SELECT id FROM reaction_inbox`)
	cause := permanentFailure(errors.New("消息组中的文件已关联到不同下载任务，无法安全合并"))
	if err := m.RetryReactionInbox(id, 1, cause); err != nil {
		t.Fatal(err)
	}
	status, attempts, _ := inboxStatus(t, db, "reaction_inbox", id)
	if status != "failed" || attempts != inboxAttemptLimit {
		t.Fatalf("status=%q attempts=%d, want failed/%d", status, attempts, inboxAttemptLimit)
	}
}
