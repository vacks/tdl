package download

import (
	"context"
	"os"
	"testing"
	"time"
)

// /saved_listen decides between starting a listener and stopping one by asking
// whether the account already has one, so this query is that whole decision.
//
// Every way of answering "yes" wrongly starts a second listener for a chat
// that already has one; every way of answering "no" wrongly stops the listener
// that is doing the work. So the query is checked against the four jobs that
// look like a listener without being one - a history download, a finished
// listener, another account's listener - and the two states a running listener
// can be in.
func TestPostgresSavedListenerIsTheRunningListenOnlyTask(t *testing.T) {
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

	seed := func(t *testing.T, id, accountID string, startMessageID, listenNew int, status string) {
		t.Helper()
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, listen_new, status, scan_state, created_at, updated_at)
			VALUES (?, 'tg://saved/1', 'self', ?, 1, '收藏夹', ?, ?, 0, ?, ?, '', ?, ?)`,
			id, "self:"+accountID, accountID, startMessageID, listenNew, status, now, now); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name         string
		accountID    string
		startMessage int
		listenNew    int
		status       string
		want         bool
	}{
		{name: "a listener that is running", accountID: "acc", startMessage: -1, listenNew: 1, status: ChatStatusListening, want: true},
		{name: "a listener nobody has resumed yet", accountID: "acc", startMessage: -1, listenNew: 1, status: ChatStatusQueued, want: true},
		{name: "a paused listener still holds the task", accountID: "acc", startMessage: -1, listenNew: 1, status: ChatStatusPaused, want: true},
		{name: "an ordinary history download is not a listener", accountID: "acc", startMessage: 0, listenNew: 0, status: ChatStatusDownloading},
		// The account has one saved task, and /saved_all leaves listening on it
		// alone. So a task that is downloading its history and listening is the
		// listener, and the toggle has to be able to stop it - keying on the
		// listen-only shape instead meant this task could be started but never
		// stopped.
		{name: "a history download that also listens is the listener", accountID: "acc", startMessage: 0, listenNew: 1, status: ChatStatusDownloading, want: true},
		{name: "a finished listener has stopped", accountID: "acc", startMessage: -1, listenNew: 1, status: ChatStatusCompleted},
		{name: "a cancelled listener has stopped", accountID: "acc", startMessage: -1, listenNew: 1, status: ChatStatusCancelled},
		{name: "another account's listener is not this one's", accountID: "other", startMessage: -1, listenNew: 1, status: ChatStatusListening},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := clearPostgresDownloadTestData(db); err != nil {
				t.Fatal(err)
			}
			seed(t, "saved-"+test.name, test.accountID, test.startMessage, test.listenNew, test.status)
			job, found, err := m.FindSavedListener("acc")
			if err != nil {
				t.Fatal(err)
			}
			if found != test.want {
				t.Fatalf("FindSavedListener found=%v, want %v: /saved_listen would do the opposite of what it should", found, test.want)
			}
			// The toggle believes this predicate, not the query, so the two have
			// to agree about every row the query can return - a row it returned
			// that the predicate rejected would be stopped by nothing and started
			// again by nothing.
			if found && !IsSavedListenForBot(job) {
				t.Fatalf("the query returned %s but the predicate rejects it, so /saved_listen can neither start nor stop it", job.ID)
			}
		})
	}
}
