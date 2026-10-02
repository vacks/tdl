package download

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// A status that could not be read is not a task that stopped.
//
// The message path learned this first: a worker that reads an empty status
// concludes the task was taken away from it and returns, and the task sits in
// 下载中 with its files queued and nothing that would ever look at it again.
// The session path kept the other half of the same mistake - publishChatItem
// read the empty status, decided it was not allowed to publish, and returned
// without a word while the working directory was removed under the finished
// file. The distinction is what these two functions exist for, so it is
// asserted directly rather than through a worker that would need a live
// Telegram account to reach.
func TestAnUnreadableStatusIsNotAStoppedTask(t *testing.T) {
	// Opened lazily, so this never dials; closing it makes every query answer
	// ErrConnDone, which is the transient failure the distinction is about.
	db, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	m := &Manager{db: &database{db: db}}

	if status := m.chatStatus("any-task"); status != "" {
		t.Fatalf("an unreadable status reads as %q, want the empty string", status)
	}
	if !m.chatRunningOrUnknown("any-task") {
		t.Fatal("a task whose status could not be read was treated as stopped; " +
			"the worker returns, the file it just finished is removed with the working " +
			"directory, and the task waits forever for a completion that already happened")
	}
	// And the message path's answer to the same question, which is what the
	// session path was aligned with.
	if !m.runningOrUnknown("any-job") {
		t.Fatal("the message path regressed: an unreadable status stopped its worker")
	}
}
