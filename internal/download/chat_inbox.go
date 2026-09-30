package download

import (
	"errors"
	"fmt"
	"time"

	"github.com/gotd/td/tg"
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
func (m *Manager) enqueueChatMessage(event telegram.NewMessageEvent) {
	if err := m.admitChatMessage(event); err != nil {
		applog.Error("download", "inbox_enqueue_failed", "account_id", event.AccountID, "dialog_key", event.DialogKey, "message_id", event.MessageID, "error", err.Error())
		m.scheduleInboxRetry(fmt.Sprintf("chat_message_inbox account=%s dialog=%s message=%d", event.AccountID, event.DialogKey, event.MessageID), func() error {
			return m.admitChatMessage(event)
		})
	}
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
	potential := m.potentialDiscussion[event.AccountID]
	snapshotReady := m.listenerSnapshotReady
	m.mu.Unlock()
	if !watched {
		// The first reply under an old channel post has no stored discussion
		// mapping yet, so its discussion group is not in chatWatched. Admit only
		// reply-shaped channel events while this account has an eligible channel
		// listener; the durable worker will then either map it or discard it.
		if event.InputPeer == nil || (event.ReplyToTopID <= 0 && event.ReplyToMessageID <= 0) || !m.hasPotentialDiscussionListener(event.AccountID, potential, snapshotReady) {
			return nil
		}
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
	err := m.db.QueryRow(`SELECT direct_peer_type, direct_peer_id, direct_peer_hash FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?) ORDER BY updated_at DESC LIMIT 1`, accountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening).Scan(&kind, &id, &hash)
	if err != nil {
		// A discussion-group update has a different dialog key from its channel
		// task. Its persisted root mapping contains the authoritative peer.
		err = m.db.QueryRow(`SELECT r.discussion_peer_type, r.discussion_peer_id, r.discussion_peer_hash FROM chat_reply_roots r JOIN chat_download_jobs j ON j.id = r.chat_job_id WHERE r.account_id = ? AND r.discussion_dialog_key = ? AND j.listen_new = 1 AND j.scan_state = ? AND j.status IN (?, ?) AND r.discussion_peer_type <> '' ORDER BY j.updated_at DESC LIMIT 1`, accountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening).Scan(&kind, &id, &hash)
	}
	if err != nil {
		return nil
	}
	return chatInboxPeer(kind, id, hash)
}

func (m *Manager) hasPotentialDiscussionListener(accountID string, potential, snapshotReady bool) bool {
	// A positive snapshot is safe even while a newer rebuild is pending: it can
	// only admit a reply for later durable validation. A negative snapshot is
	// trusted only once it is current; otherwise fall back to the old query so a
	// just-enabled listener can never lose its first reply.
	if potential {
		return true
	}
	if snapshotReady && !m.listenerDirty.Load() {
		return false
	}
	var exists bool
	if err := m.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM chat_download_jobs WHERE account_id = ? AND dialog_type = 'channel' AND listen_new = 1 AND scan_state = ? AND status IN (?, ?))`, accountID, chatScanCompleted, ChatStatusDownloading, ChatStatusListening).Scan(&exists); err != nil {
		return false
	}
	return exists
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
		peer := chatInboxPeer(peerType, peerID, peerHash)
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

func chatInboxPeer(kind string, id, hash int64) tg.InputPeerClass {
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
	status := "pending"
	if attempts >= inboxFastAttempts {
		status = "failed"
	}
	delay := time.Duration(1<<min(attempts, 6)) * time.Second
	if _, err := m.db.Exec(`UPDATE chat_message_inbox SET status = ?, error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`, status, cause.Error(), time.Now().UTC().Add(delay).Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return err
	}
	if status == "failed" {
		applog.Error("chat_download", "new_message_event_gave_up", "inbox_id", id, "attempts", attempts, "error", cause.Error())
	}
	return nil
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

// ListenerInboxCounts reports how many listener events are queued and how many
// have stopped being retried. They are shown where a person can see them,
// because an event that gives up is a download request that will not happen.
func (m *Manager) ListenerInboxCounts() (pending, stopped int, err error) {
	err = m.db.QueryRow(`SELECT
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status IN ('pending', 'processing')) +
 (SELECT COUNT(1) FROM reaction_inbox WHERE status IN ('pending', 'processing')),
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'failed') +
 (SELECT COUNT(1) FROM reaction_inbox WHERE status = 'failed')`).Scan(&pending, &stopped)
	if err != nil {
		return 0, 0, err
	}
	return pending, stopped, nil
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
