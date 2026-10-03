package download

import "testing"

// A session task's detail carries the newest few failed files, so the Bot card
// can say which file stopped and why. Before this, only the count reached the
// card, and a count is not something anyone can act on.
//
// It carries the newest few of its OWN task. Both halves are asserted here
// because both can be got wrong in the same query: the window is a LIMIT, and
// the ownership is a WHERE - and a query that lost the WHERE still returns
// exactly three plausible-looking files.
func TestPostgresChatDetailCarriesTheNewestFailuresOfItsOwnTask(t *testing.T) {
	m := openChatItemTestManager(t, "chat-failures", ChatStatusListening, []chatTestItem{
		{messageID: 40, status: "completed"},
		{messageID: 41, status: "failed", name: "旧失败.mkv", err: "第一次失败"},
		{messageID: 42, status: "failed", name: "中失败.mkv", err: "第二次失败"},
		{messageID: 43, status: "failed", name: "新失败.mkv", err: "第三次失败"},
		{messageID: 44, status: "failed", name: "最新失败.mkv", err: "第四次失败"},
	})
	// A second task over the same dialog, holding a failure with a higher
	// message id than anything above. It is newer than every file of the task
	// under test, so an unfiltered scan would return it first. It is cancelled
	// because only one task at a time may be active over a dialog.
	insertChatJob(t, m.db, "chat-decoy", ChatStatusCancelled)
	insertChatItems(t, m.db, "chat-decoy", []chatTestItem{
		{messageID: 90, status: "failed", name: "别人的文件.mkv", err: "别人的错误"},
	})

	job, err := m.GetChat("chat-failures")
	if err != nil {
		t.Fatal(err)
	}
	if job.Failed != 4 {
		t.Fatalf("the task reports %d failures, want the 4 that were seeded", job.Failed)
	}
	want := []ChatFailure{
		{Name: "最新失败.mkv", Error: "第四次失败"},
		{Name: "新失败.mkv", Error: "第三次失败"},
		{Name: "中失败.mkv", Error: "第二次失败"},
	}
	if len(job.Failures) != len(want) {
		t.Fatalf("the detail carried %d failures, want %d: %+v", len(job.Failures), len(want), job.Failures)
	}
	for i, expected := range want {
		if job.Failures[i] != expected {
			t.Errorf("failure %d is %+v, want %+v", i, job.Failures[i], expected)
		}
	}
}

// A task with nothing stuck must not gain an empty section: the card hides the
// block on an empty list, so an empty-but-present list is the difference
// between a card that reads as it always did and one that grows a heading over
// nothing.
func TestPostgresChatDetailWithNothingFailedCarriesNoFailures(t *testing.T) {
	m := openChatItemTestManager(t, "chat-clean", ChatStatusCompleted, []chatTestItem{
		{messageID: 1, status: "completed"},
	})

	job, err := m.GetChat("chat-clean")
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Failures) != 0 {
		t.Fatalf("a task with no failures carried %+v", job.Failures)
	}
}
