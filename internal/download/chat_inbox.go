package download

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/telegram"
)

// chatMessageInboxEvent is a durable new-message notification. Telegram can
// redeliver updates, so its identity is unique per account/dialog/message.
type chatMessageInboxEvent struct {
	id       int64
	attempts int
	event    telegram.NewMessageEvent
}

// errInboxUnavailable reports that an inbox write could not be attempted or did
// not persist. Callers distinguish it from a filtered event, which is not an
// error and must never be retried.
var errInboxUnavailable = errors.New("收件箱暂不可写入")

// enqueueChatMessage admits one new-message trigger. The dispatcher handler that
// produced this event returns nil to gotd, which advances the update state, so
// Telegram never redelivers: an event dropped here is dropped for good. Every
// path that can fail therefore logs and queues a bounded retry instead of
// returning silently.
//
// It reports whether the event is durably accounted for. False means the row was
// not written and the only copy of the request now lives in this process's retry
// queue, which the gap walk has to know: that walk is the last thing that can
// re-offer the message, and advancing past it would leave nothing behind.
// A filtered event - one this service deliberately does not want - returns true,
// because it is an answer rather than a failure.
func (m *Manager) enqueueChatMessage(event telegram.NewMessageEvent) bool {
	if err := m.admitChatMessage(event); err != nil {
		applog.Error("download", "inbox_enqueue_failed", "account_id", event.AccountID, "dialog_key", event.DialogKey, "message_id", event.MessageID, "error", err.Error())
		m.scheduleInboxRetry(fmt.Sprintf("chat_message_inbox account=%s dialog=%s message=%d", event.AccountID, event.DialogKey, event.MessageID), func() error {
			return m.admitChatMessage(event)
		})
		return false
	}
	return true
}

// admitChatMessage performs the filtered checks and the durable insert. It
// returns an error only when a valid event could not be admitted, so "nothing to
// do" and "try again" stay distinguishable.
func (m *Manager) admitChatMessage(event telegram.NewMessageEvent) error {
	if event.MessageID <= 0 {
		return nil
	}
	if !m.DatabaseAvailable() {
		// Attempting the insert here would block the update dispatcher for the
		// full statement timeout while the database is down, so wait outside it.
		return errInboxUnavailable
	}
	key := event.DialogKey
	if event.InputPeer != nil {
		_, key, _ = dialogIdentity(event.InputPeer, event.AccountID)
	}
	if key == "" {
		return nil
	}
	if event.InputPeer == nil {
		// Telegram may omit entities from an update. Resolve the durable peer
		// before consulting the in-memory snapshot; otherwise an otherwise valid
		// watched message could be discarded before recovery gets a chance.
		event.InputPeer = m.recoverChatEventPeer(event.AccountID, key)
	}
	m.mu.Lock()
	_, watched := m.chatWatched[event.AccountID][key]
	m.mu.Unlock()
	if !watched {
		// Only the dialogs a task is responsible for are admitted: the listened
		// dialog itself, and the channel's linked discussion group, where its
		// comments arrive. Everything else the account has joined is filtered
		// here, in memory, with no database query and no request.
		//
		// This used to admit any reply-shaped message whenever the account had a
		// channel listener at all, because the first comment of a thread whose
		// discussion group was not yet mapped would otherwise be lost for good:
		// the dispatcher acknowledges the update, so Telegram never redelivers
		// it. That group is now recorded on the task before comments can arrive -
		// resolved when listening is enabled, and by every post that is indexed
		// or received - so the guess is gone. It was expensive: one enable
		// queued 24 events from seven dialogs that had no task at all, each one
		// a Telegram lookup retried five times.
		return nil
	}
	if event.InputPeer == nil {
		// The peer could not be resolved. That is a database failure rather than a
		// filtered event, so let the retry queue resolve it once the database is
		// back instead of discarding a watched message.
		return errInboxUnavailable
	}
	direct := makeDirectPeer(event.InputPeer)
	if direct.kind != "self" && direct.kind != "chat" && direct.kind != "channel" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := m.db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, dialog_name, dialog_id, message_id, reply_to_message_id, reply_to_top_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?) ON CONFLICT(account_id, dialog_key, message_id) DO UPDATE SET status = 'pending', attempts = 0, error = '', next_attempt_at = EXCLUDED.next_attempt_at, updated_at = EXCLUDED.updated_at WHERE chat_message_inbox.status = 'failed'`, event.AccountID, key, event.DialogName, event.DialogID, event.MessageID, event.ReplyToMessageID, event.ReplyToTopID, direct.kind, direct.id, direct.hash, now, now, now)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed == 1 {
		select {
		case m.chatEventWake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *Manager) recoverChatEventPeer(accountID, dialogKey string) tg.InputPeerClass {
	var kind string
	var id, hash int64
	// A discussion-group update carries the group's key, not its channel's, so
	// the task that owns this dialog is the one whose linked group it is. The
	// group's peer is stored beside that link for exactly this case: an update
	// that omitted its entities still has to be attributed.
	err := m.db.QueryRow(`SELECT discussion_peer_type, discussion_peer_id, discussion_peer_hash FROM chat_download_jobs WHERE account_id = ? AND discussion_dialog_key = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?) AND discussion_peer_type <> '' ORDER BY updated_at DESC LIMIT 1`, accountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening).Scan(&kind, &id, &hash)
	if err != nil {
		// A listened dialog itself has a task keyed on its own dialog key.
		err = m.db.QueryRow(`SELECT direct_peer_type, direct_peer_id, direct_peer_hash FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?) ORDER BY updated_at DESC LIMIT 1`, accountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening).Scan(&kind, &id, &hash)
	}
	if err != nil {
		// Threads mapped before the task learned its group carry the same peer,
		// per root.
		err = m.db.QueryRow(`SELECT r.discussion_peer_type, r.discussion_peer_id, r.discussion_peer_hash FROM chat_reply_roots r JOIN chat_download_jobs j ON j.id = r.chat_job_id WHERE r.account_id = ? AND r.discussion_dialog_key = ? AND j.listen_new = 1 AND j.scan_state = ? AND j.status IN (?, ?) AND r.discussion_peer_type <> '' ORDER BY j.updated_at DESC LIMIT 1`, accountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening).Scan(&kind, &id, &hash)
	}
	if err != nil {
		return nil
	}
	return inboxPeer(kind, id, hash)
}

func (m *Manager) claimChatMessageInbox(limit int) ([]chatMessageInboxEvent, error) {
	if limit < 1 {
		limit = 1
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// Probe before opening a transaction. An idle listener polls this queue on
	// a timer, and BEGIN + SELECT + COMMIT costs three statements on the common
	// empty case where a single index probe answers the same question.
	var pending bool
	if err := m.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM chat_message_inbox WHERE status = 'pending' AND next_attempt_at::timestamptz <= ?::timestamptz)`, now).Scan(&pending); err != nil {
		return nil, err
	}
	if !pending {
		return nil, nil
	}
	tx, err := m.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT id, account_id, dialog_name, dialog_id, message_id, reply_to_message_id, reply_to_top_id, peer_type, peer_id, peer_hash, attempts
FROM chat_message_inbox WHERE status = 'pending' AND next_attempt_at::timestamptz <= ?::timestamptz ORDER BY next_attempt_at, id LIMIT ?`, now, limit)
	if err != nil {
		return nil, err
	}
	result := make([]chatMessageInboxEvent, 0, limit)
	for rows.Next() {
		var item chatMessageInboxEvent
		var accountID, name, peerType string
		var dialogID, messageID, replyToMessageID, replyToTopID, peerID, peerHash int64
		if err := rows.Scan(&item.id, &accountID, &name, &dialogID, &messageID, &replyToMessageID, &replyToTopID, &peerType, &peerID, &peerHash, &item.attempts); err != nil {
			return nil, err
		}
		peer := inboxPeer(peerType, peerID, peerHash)
		if peer == nil {
			return nil, fmt.Errorf("会话消息事件 %d 的会话引用无效", item.id)
		}
		item.event = telegram.NewMessageEvent{AccountID: accountID, DialogID: dialogID, DialogName: name, MessageID: int(messageID), ReplyToMessageID: int(replyToMessageID), ReplyToTopID: int(replyToTopID), InputPeer: peer}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range result {
		item := &result[index]
		update, err := tx.Exec(`UPDATE chat_message_inbox SET status = 'processing', attempts = attempts + 1, updated_at = ? WHERE id = ? AND status = 'pending'`, now, item.id)
		if err != nil {
			return nil, err
		}
		if changed, _ := update.RowsAffected(); changed == 1 {
			item.attempts++
		} else {
			result[index].id = 0
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	claimed := result[:0]
	for _, item := range result {
		if item.id != 0 {
			claimed = append(claimed, item)
		}
	}
	return claimed, nil
}

// inboxPeer rebuilds the peer an inbox event was admitted with.
//
// The session inbox's copy of this was the same function without the user case,
// which it never produces: its rows are written from a listener's dialog, and
// a dialog is a channel, a group or the account's own saved messages.
func inboxPeer(kind string, id, hash int64) tg.InputPeerClass {
	switch kind {
	case "self":
		return &tg.InputPeerSelf{}
	case "chat":
		return &tg.InputPeerChat{ChatID: id}
	case "channel":
		return &tg.InputPeerChannel{ChannelID: id, AccessHash: hash}
	default:
		return nil
	}
}

func (m *Manager) completeChatMessageInbox(id int64) error {
	_, err := m.db.Exec(`UPDATE chat_message_inbox SET status = 'done', error = '', updated_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

// retryChatMessageInbox backs an event off after a failure and stops the fast
// retries after five attempts.
//
// Stopping used to be the end of the road for a live event, and the comment
// here claimed otherwise: the claim query only reads 'pending' rows, so the way
// back in was said to be the conflict clause in the insert, which reopens a
// failed row when the same message arrives again. That path cannot fire. The
// update dispatcher returns nil to gotd, which advances Telegram's update state,
// so the same new message is never delivered twice. A group task had no gap walk
// either. The row was therefore dropped silently, with nothing in the UI or the
// logs to say a download had been requested and lost.
//
// The way back in is now explicit: reviveExhaustedInboxEvents returns these rows
// to the queue periodically, up to a total attempt budget. A transient failure
// is recovered, and one that can never succeed stops for good with its error
// recorded and counted.
func (m *Manager) retryChatMessageInbox(id int64, attempts int, cause error) error {
	return m.retryInboxEvent(messageInbox, id, attempts, cause)
}

// retryableTelegram400Types are the answers in Telegram's 400 family that are
// about timing or a momentarily unusable peer rather than about the request
// itself. Matched as substrings because the wait errors carry their argument in
// the type (FLOOD_WAIT_42).
var retryableTelegram400Types = []string{
	"FLOOD_WAIT",    // the account is throttled; usually 420, listed for the variants
	"SLOWMODE_WAIT", // the group's slow mode is on
	"PEER_FLOOD",    // the account is flagged; the block is temporary
	"MSG_WAIT_FAILED",
	"HISTORY_GET_FAILED",
	"RPC_CALL_FAIL",
	"TIMEOUT",
}

// permanentInboxError reports whether Telegram rejected the request itself,
// which no later attempt can change.
//
// A 400 is a statement about the request: the dialog is gone (CHANNEL_INVALID,
// PEER_ID_INVALID), the message is gone (MSG_ID_INVALID), the account may not
// reference it (CHAT_WRITE_FORBIDDEN). The retry budget exists for the other
// class - a closed connection, a dialog that was briefly unreachable - and
// spending it on a rejection buys nothing and costs requests. The incident that
// motivated this: 24 events, every one rejected on its first attempt, each
// retried five times and then revived hourly for a day.
//
// The allowlist is deliberately short. A 400 that is not in it stops on the
// first attempt with its error recorded and visible, which is the same place a
// fully retried event ends up, only sooner and without the requests.
//
// 401 and 403 belong to the same class for the same reason. They are statements
// about the session, not about timing: SESSION_REVOKED, AUTH_KEY_UNREGISTERED,
// USER_DEACTIVATED_BAN and CHAT_WRITE_FORBIDDEN are all answers that only
// re-authenticating or relinking the dialog can change. Reaching them by asking
// again is impossible, so they stop immediately like a rejection. Only the 400
// allowlist above names answers that a later attempt can outlast.
func permanentInboxError(err error) bool {
	if isPermanentFailure(err) {
		return true
	}
	rpcErr, ok := tgerr.As(err)
	if !ok {
		return false
	}
	switch rpcErr.Code {
	case 400, 401, 403:
	default:
		return false
	}
	upper := strings.ToUpper(rpcErr.Type)
	for _, retryable := range retryableTelegram400Types {
		if strings.Contains(upper, retryable) {
			return false
		}
	}
	return true
}

// inboxFastAttempts is when an event stops being retried every few seconds and
// waits for the periodic revive, which retries it far more slowly.
const inboxFastAttempts = 5

// inboxReviveInterval is how often an event that used up its fast attempts is
// offered to a worker again.
const inboxReviveInterval = time.Hour

// inboxAttemptLimit is the total number of attempts an event gets before it is
// left failed for good. Combined with the revive interval this is a long, slow
// tail rather than either a permanent loss or an unbounded retry: an event that
// cannot succeed - the message was deleted, the dialog is gone - stops, and its
// recorded error stays visible until retention removes the row.
const inboxAttemptLimit = 20

// reviveExhaustedInboxEvents returns events that used up their fast attempts to
// the queue, on the far slower revive cadence.
//
// The alternative was to drop them: nothing else selects a failed row, and for a
// live update there is no redelivery to reopen it. These are download requests
// the user made, so a slow retry is the right default and a permanent, silent
// loss is not. Errors are returned rather than swallowed so the sweep's own
// failures are visible too.
//
// It selects 'failed' and nothing else. A 'skipped' row is the other terminal
// outcome and is deliberately excluded: it records an answer - there is nothing
// to download - and offering it again would re-ask a question that already has
// one, which is exactly the retry loop this row exists to avoid.
func (m *Manager) reviveExhaustedInboxEvents() error {
	now := time.Now().UTC()
	retryAt := now.Add(inboxReviveInterval).Format(time.RFC3339Nano)
	nowText := now.Format(time.RFC3339Nano)
	cutoff := now.Add(-inboxReviveInterval).Format(time.RFC3339Nano)
	if _, err := m.db.Exec(`UPDATE chat_message_inbox SET status = 'pending', next_attempt_at = ?, updated_at = ?
WHERE status = 'failed' AND attempts < ? AND updated_at::timestamptz < ?::timestamptz`, retryAt, nowText, inboxAttemptLimit, cutoff); err != nil {
		return err
	}
	if _, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', next_attempt_at = ?, updated_at = ?
WHERE status = 'failed' AND attempts < ? AND updated_at::timestamptz < ?::timestamptz`, retryAt, nowText, inboxAttemptLimit, cutoff); err != nil {
		return err
	}
	return nil
}

// ListenerInboxCounts reports how many listener events are being handled, how
// many are waiting, and how many have stopped being retried. They are shown
// where a person can see them, because an event that gives up is a download
// request that will not happen.
//
// Waiting is counted apart from in-flight work. A pending row is one that is
// queued or backing off, and after a failure an event can wait an hour at a
// time, so folding the two together reported an event that was doing nothing as
// work in progress - which is exactly how an event that can never succeed stayed
// invisible in the count while it was being retried.
func (m *Manager) ListenerInboxCounts() (processing, waiting, stopped int, err error) {
	err = m.db.QueryRow(`SELECT
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'processing') +
 (SELECT COUNT(1) FROM reaction_inbox WHERE status = 'processing'),
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'pending') +
 (SELECT COUNT(1) FROM reaction_inbox WHERE status = 'pending'),
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'failed') +
 (SELECT COUNT(1) FROM reaction_inbox WHERE status = 'failed')`).Scan(&processing, &waiting, &stopped)
	if err != nil {
		return 0, 0, 0, err
	}
	return processing, waiting, stopped, nil
}

// waitChatEvent pauses an inbox worker between attempts. It reports false when
// the manager is stopping, so the loop exits instead of sleeping through the
// shutdown window and then querying a closed database.
func (m *Manager) waitChatEvent(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-m.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

// An event inbox has one status machine, and it was written twice.
//
// The two inboxes take events from different places - a listened message and a
// reaction on one - and their tables carry different columns, but what happens
// to an event that failed is the same question with the same three answers, and
// they were the same forty lines twice over. The answers are not obvious ones,
// which is what makes a second copy dangerous:
//
//   - nothingToDo is not a failure. The event was evaluated and the answer was
//     "there is nothing here to download", so it settles as skipped and stays
//     out of the queue the Bot lists, while still recording that the request
//     was seen.
//   - a permanent failure spends the whole attempt budget at once, because
//     reviveExhaustedInboxEvents only offers rows below the limit - which is
//     what keeps a rejected event from coming back every hour to spend another
//     Telegram request.
//   - anything else is a transport failure, and it backs off.
type inboxTable struct {
	table     string
	component string
	// event names the log lines. The event's own name is the one thing the two
	// really do say differently, because they are read by different people.
	event string
}

var (
	messageInbox  = inboxTable{table: "chat_message_inbox", component: "chat_download", event: "new_message_event"}
	reactionInbox = inboxTable{table: "reaction_inbox", component: "reaction", event: "reaction_event"}
)

// retryInboxEvent settles one event that did not go through.
func (m *Manager) retryInboxEvent(t inboxTable, id int64, attempts int, cause error) error {
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	if IsNothingToDo(cause) {
		// The event was answered: nothing in it matches the filter, so there is
		// nothing to download. Settling it as skipped keeps it out of the queue
		// the Bot lists while still recording that the request was seen.
		if _, err := m.db.Exec(`UPDATE `+t.table+` SET status = 'skipped', error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`, cause.Error(), stamp, stamp, id); err != nil {
			return err
		}
		applog.Info(t.component, t.event+"_skipped", "inbox_id", id, "attempts", attempts, "reason", cause.Error())
		return nil
	}
	if permanentInboxError(cause) {
		// Telegram rejected the request itself, so no later attempt can succeed.
		// Spend the whole budget at once: the revive pass only offers rows below
		// the limit, which is what keeps a rejected event from coming back every
		// hour to spend another request and to keep showing up as waiting work.
		if _, err := m.db.Exec(`UPDATE `+t.table+` SET status = 'failed', attempts = ?, error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`, inboxAttemptLimit, cause.Error(), stamp, stamp, id); err != nil {
			return err
		}
		applog.Error(t.component, t.event+"_rejected", "inbox_id", id, "attempts", attempts, "error", cause.Error())
		return nil
	}
	status := "pending"
	if attempts >= inboxFastAttempts {
		status = "failed"
	}
	delay := time.Duration(1<<min(attempts, 6)) * time.Second
	if _, err := m.db.Exec(`UPDATE `+t.table+` SET status = ?, error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`, status, cause.Error(), now.Add(delay).Format(time.RFC3339Nano), stamp, id); err != nil {
		return err
	}
	if status == "failed" {
		applog.Error(t.component, t.event+"_gave_up", "inbox_id", id, "attempts", attempts, "error", cause.Error())
	}
	return nil
}
