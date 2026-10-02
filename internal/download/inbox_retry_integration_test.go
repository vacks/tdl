package download

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/tmsg"
)

// Both inboxes settle an event the same way, and the three outcomes are not
// obvious enough to be written twice safely.
//
// The message inbox and the reaction inbox take events from different places
// and keep different columns, but what happens to an event that did not go
// through is one question with three answers, and it was fourteen hundred
// characters answered twice. This drives each table through the shared
// settlement and asserts the answer each one gives, so a change to the policy
// is a change that reaches both.
func TestPostgresBothInboxesSettleAnEventTheSameWay(t *testing.T) {
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

	insert := map[string]string{
		messageInbox.table:  `INSERT INTO chat_message_inbox(account_id, dialog_key, message_id, peer_type, status, next_attempt_at, created_at, updated_at) VALUES ('acc','channel:inbox',?,'channel','processing',?,?,?) RETURNING id`,
		reactionInbox.table: `INSERT INTO reaction_inbox(account_id, dialog_key, message_id, peer_type, emoji, status, next_attempt_at, created_at, updated_at) VALUES ('acc','channel:inbox',?,'channel','👍','processing',?,?,?) RETURNING id`,
	}
	settle := map[string]func(id int64, attempts int, cause error) error{
		messageInbox.table:  m.retryChatMessageInbox,
		reactionInbox.table: m.RetryReactionInbox,
	}

	cases := []struct {
		name     string
		cause    error
		attempts int
		want     string
		wantFull bool
	}{
		{"answered: nothing to download", nothingToDo(errors.New("没有符合过滤条件的文件")), 1, "skipped", false},
		{"refused: no attempt clears it", permanentFailure(errors.New("CHANNEL_INVALID")), 1, "failed", true},
		{"transient: backs off", errors.New("read tcp: connection reset by peer"), 1, "pending", false},
		{"transient, out of fast attempts", errors.New("read tcp: connection reset by peer"), inboxFastAttempts, "failed", false},
		// A message that is gone is an answer, not a fault, and it reaches the
		// policy as the error the engine classifies it as.
		{"message deleted", tmsg.ErrMessageDeleted, 1, "pending", false},
	}

	messageID := 0
	for _, table := range []string{messageInbox.table, reactionInbox.table} {
		for _, testCase := range cases {
			messageID++
			t.Run(table+"/"+testCase.name, func(t *testing.T) {
				var id int64
				if err := db.QueryRow(insert[table], messageID, now, now, now).Scan(&id); err != nil {
					t.Fatal(err)
				}
				if err := settle[table](id, testCase.attempts, testCase.cause); err != nil {
					t.Fatalf("settling: %v", err)
				}
				var status, saved string
				var attempts int
				if err := db.QueryRow(`SELECT status, error, attempts FROM `+table+` WHERE id = ?`, id).Scan(&status, &saved, &attempts); err != nil {
					t.Fatal(err)
				}
				if status != testCase.want {
					t.Fatalf("the event settled as %q, want %q (%s)", status, testCase.want, testCase.name)
				}
				if saved == "" {
					t.Fatal("the event settled without recording why")
				}
				if testCase.wantFull && attempts != inboxAttemptLimit {
					t.Fatalf("a refused event holds %d attempts, want the whole budget spent (%d): "+
						"the revive pass offers rows below the limit, so one left under it comes back "+
						"every hour to spend another request", attempts, inboxAttemptLimit)
				}
			})
		}
	}
}
