package download

import (
	"context"
	"os"
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
