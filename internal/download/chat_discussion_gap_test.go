package download

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

// discussionGapManager builds the smallest Manager the discussion walk needs:
// a real database, one listening task, and the dialog marked as watched so
// admission does not filter the walk's events away before they can be counted.
func discussionGapManager(t *testing.T, jobID, accountID, dialogKey string) *Manager {
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
	m := &Manager{db: db, chatWatched: map[string]map[string]struct{}{}, chatEventWake: make(chan struct{}, 1)}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	m.dbHealthMu.Lock()
	m.dbHealth = DatabaseHealth{Status: "connected"}
	m.dbHealthMu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, created_at, updated_at) VALUES (?, 'tg://test', 'channel', ?, 1, '频道', ?, ?, ?, ?, ?)`,
		jobID, dialogKey, accountID, ChatStatusListening, chatScanCompleted, now, now); err != nil {
		t.Fatal(err)
	}
	m.addChatWatched(accountID, dialogKey)
	return m
}

func discussionGapTarget(jobID, accountID, dialogKey string) storedChatTarget {
	return storedChatTarget{ChatJob: ChatJob{ID: jobID, AccountID: accountID, DialogName: "频道"}}
}

func discussionGapPeer() tg.InputPeerClass {
	return &tg.InputPeerChannel{ChannelID: 1, AccessHash: 1}
}

// A backlog larger than the page budget must keep its cursor. The walk reads
// newest page first, so a cursor left where it stopped is the only thing that
// lets the next walk resume *below* it; clearing it restarts from the newest
// message, and because this walk has already raised the inbox watermark to that
// same message, the next walk stops on its first page and everything under it
// becomes unreachable. That is the whole reason the cap costs latency rather
// than coverage.
func TestDiscussionGapWalkKeepsItsCursorWhenThePageBudgetRunsOut(t *testing.T) {
	const (
		jobID     = "discussion-gap-budget"
		accountID = "account-gap"
		dialogKey = "channel:1"
		boundary  = 1
	)
	m := discussionGapManager(t, jobID, accountID, dialogKey)
	pages := 0
	fetch := func(offset int) ([]tg.MessageClass, error) {
		pages++
		// Every page is new and none of them reaches the boundary, so the walk
		// ends only because it ran out of budget.
		base := 1_000_000 - pages*discussionGapTestPage
		page := make([]tg.MessageClass, 0, discussionGapTestPage)
		for index := 0; index < discussionGapTestPage; index++ {
			page = append(page, &tg.Message{ID: base - index})
		}
		return page, nil
	}
	if err := m.walkDiscussionGap(discussionGapTarget(jobID, accountID, dialogKey), discussionGapPeer(), dialogKey, boundary, fetch); err != nil {
		t.Fatal(err)
	}
	if pages != discussionGapPageCap {
		t.Fatalf("walk fetched %d pages, want the full budget of %d", pages, discussionGapPageCap)
	}
	if got := m.gapStreamOffset(jobID, listenerGapDiscussionStream); got == 0 {
		t.Fatal("the walk ran out of page budget and cleared its cursor; the backlog below the boundary can never be reached again")
	}
}

// discussionGapTestPage matches the Limit the production fetch asks Telegram
// for, so a page budget of N covers the same number of messages in both.
const discussionGapTestPage = 100

// The other ending: the walk proves it has nothing left to recover. It reached
// the boundary, so the cursor must return to zero. A cursor left behind is a
// walk that resumes downwards and therefore never sees a comment published
// since - the one thing the next walk exists to find.
func TestDiscussionGapWalkClearsItsCursorOnceItReachesTheBoundary(t *testing.T) {
	const (
		jobID     = "discussion-gap-reached"
		accountID = "account-gap"
		dialogKey = "channel:1"
		boundary  = 250
	)
	m := discussionGapManager(t, jobID, accountID, dialogKey)
	// A walk interrupted mid-backlog left a position behind.
	m.setGapStreamOffset(jobID, listenerGapDiscussionStream, 900)
	fetch := func(offset int) ([]tg.MessageClass, error) {
		return []tg.MessageClass{&tg.Message{ID: 300}, &tg.Message{ID: 250}, &tg.Message{ID: 200}}, nil
	}
	if err := m.walkDiscussionGap(discussionGapTarget(jobID, accountID, dialogKey), discussionGapPeer(), dialogKey, boundary, fetch); err != nil {
		t.Fatal(err)
	}
	if got := m.gapStreamOffset(jobID, listenerGapDiscussionStream); got != 0 {
		t.Fatalf("cursor=%d after reaching the boundary, want 0 so the next walk starts from the newest message", got)
	}
}

// A comment recovered from the discussion group has to arrive at admission
// carrying the same attribution a live update carries. Admission finds the task
// a comment belongs to through the reply header only - the comment sits in the
// discussion group, and the task is registered under the channel - so an event
// rebuilt without it is dropped as belonging to no task, silently, exactly as
// if the file had never been posted. This walks the whole chain: the walk
// builds the event, the inbox stores it, and the claim that the admission path
// reads rebuilds it from those columns.
func TestDiscussionGapRecoveredCommentKeepsItsAttribution(t *testing.T) {
	const (
		jobID     = "discussion-gap-attribution"
		accountID = "account-gap"
		dialogKey = "channel:1"
		boundary  = 400
		rootID    = 42
	)
	m := discussionGapManager(t, jobID, accountID, dialogKey)
	fetch := func(offset int) ([]tg.MessageClass, error) {
		if offset != 0 {
			return nil, nil
		}
		comment := &tg.Message{ID: 500, PeerID: &tg.PeerChannel{ChannelID: 1}}
		header := &tg.MessageReplyHeader{}
		header.SetReplyToTopID(rootID)
		header.SetReplyToMsgID(rootID)
		comment.SetReplyTo(header)
		return []tg.MessageClass{comment, &tg.Message{ID: boundary}}, nil
	}
	if err := m.walkDiscussionGap(discussionGapTarget(jobID, accountID, dialogKey), discussionGapPeer(), dialogKey, boundary, fetch); err != nil {
		t.Fatal(err)
	}
	var replyToTop, replyToMessage int
	if err := m.db.QueryRow(`SELECT reply_to_top_id, reply_to_message_id FROM chat_message_inbox WHERE account_id = ? AND dialog_key = ? AND message_id = 500`, accountID, dialogKey).Scan(&replyToTop, &replyToMessage); err != nil {
		t.Fatalf("the recovered comment never reached the inbox: %v", err)
	}
	if replyToTop != rootID || replyToMessage != rootID {
		t.Fatalf("inbox stored reply_to_top_id=%d reply_to_message_id=%d, want %d/%d", replyToTop, replyToMessage, rootID, rootID)
	}
	events, err := m.claimChatMessageInbox(10)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.event.MessageID != 500 {
			continue
		}
		if event.event.ReplyToTopID != rootID {
			t.Fatalf("the claim handed admission ReplyToTopID=%d, want %d; admission drops a comment it cannot attribute", event.event.ReplyToTopID, rootID)
		}
		return
	}
	t.Fatal("the recovered comment was not claimable from the inbox")
}

// cursorManager gives the list queries a database with tasks that share one
// timestamp, which is the case a keyset cursor exists to get right.
func cursorManager(t *testing.T, now string, count int) *Manager {
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
	m := &Manager{db: db}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= count; index++ {
		// One timestamp for every row: ordering then rests entirely on the id
		// tiebreaker, which is the half of the cursor an OR form gets wrong.
		if _, err := db.Exec(`INSERT INTO download_jobs(id, source_url, status, created_at, updated_at) VALUES (?, 'tg://test', 'queued', ?, ?)`,
			fmt.Sprintf("job-%02d", index), now, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.loadVisibleCounts(); err != nil {
		t.Fatal(err)
	}
	return m
}

// Paginating a list whose rows all share a timestamp has to visit every row
// exactly once, newest id first. This is what the keyset predicate decides, and
// it is one parameter shorter as a row comparison than as the OR form it
// replaced - so a binding left behind is a page that silently skips or repeats
// rows rather than an error.
func TestTaskListPagingIsCompleteAcrossTiedTimestamps(t *testing.T) {
	const now = "2026-01-02T03:04:05Z"
	const count = 7
	m := cursorManager(t, now, count)
	seen := make([]string, 0, count)
	for cursor := ""; ; {
		jobs, _, next, err := m.ListCursor(cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, job := range jobs {
			seen = append(seen, job.ID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != count {
		t.Fatalf("paging returned %d tasks (%v), want all %d", len(seen), seen, count)
	}
	want := []string{"job-07", "job-06", "job-05", "job-04", "job-03", "job-02", "job-01"}
	for index, id := range seen {
		if id != want[index] {
			t.Fatalf("page order %v, want %v", seen, want)
		}
	}
}

// The predicate itself, pinned. The OR form is the same answer written in a way
// the planner cannot push into the index, which makes a page cost the whole
// history in front of it - a difference that only shows up on a large table and
// so is never caught by a correctness test above.
func TestKeysetPredicateIsASingleRangeCondition(t *testing.T) {
	for _, alias := range []string{"j", "c"} {
		condition := keysetAfter(alias)
		if strings.Contains(condition, " OR ") {
			t.Fatalf("keyset condition for %q is %q; the OR form is applied as a filter over a scan from the top of the index", alias, condition)
		}
		if strings.Count(condition, "?") != 2 {
			t.Fatalf("keyset condition for %q binds %d parameters, want 2", alias, strings.Count(condition, "?"))
		}
	}
}

// The retention sweep's indexes have to exist for the sweep to stay bounded.
// Their absence is invisible in behaviour - the delete returns the same rows,
// just by reading and sorting the whole queue once per batch - so nothing else
// would notice one being dropped from the index set.
func TestRetentionIndexesSurviveTheIndexSet(t *testing.T) {
	m := cursorManager(t, "2026-01-02T03:04:05Z", 0)
	for _, name := range []string{"reaction_inbox_retention", "chat_message_inbox_retention"} {
		var found bool
		if err := m.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = ?)`, name).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("index %s was not created; the retention sweep falls back to sorting the whole queue on every batch", name)
		}
	}
}
