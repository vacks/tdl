package download

import (
	"errors"
	"fmt"
	"time"
)

// ReactionInboxEvent is a durable, pre-resolution record of a reaction
// trigger. It contains only Telegram identifiers and no message text/media.
type ReactionInboxEvent struct {
	ID       int64
	Intent   DownloadIntent
	Emoji    string
	Attempts int
}

// QueueReaction persists a trigger before any Telegram RPC is made. The unique
// key makes repeated update delivery idempotent for one account/message/emoji.
func (m *Manager) QueueReaction(intent DownloadIntent, emoji string) (queued bool, err error) {
	if intent.Source != SourceReaction || intent.Message == nil {
		return false, errors.New("表情事件必须包含 Telegram 消息引用")
	}
	if err := intent.validate(); err != nil {
		return false, err
	}
	direct := makeDirectPeer(intent.Message.InputPeer)
	if direct.kind == "" {
		return false, errors.New("不支持的 Telegram 会话类型")
	}
	_, key, _ := dialogIdentity(intent.Message.InputPeer, intent.AccountID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := m.db.Exec(`INSERT INTO reaction_inbox(account_id, dialog_key, dialog_name, dialog_id, message_id, source_url, peer_type, peer_id, peer_hash, emoji, status, attempts, next_attempt_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?) ON CONFLICT(account_id, dialog_key, message_id, emoji) DO NOTHING`,
		intent.AccountID, key, intent.Message.DialogName, intent.Message.DialogID, intent.Message.MessageID, intent.Message.SourceURL, direct.kind, direct.id, direct.hash, emoji, now, now, now)
	if err != nil {
		return false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	// A row that already exists is reopened only where a new reaction is a
	// genuine request to try again. Each arm below names one such state and is
	// tried in turn, because the states are disjoint.
	reopen := func(condition string) (bool, error) {
		result, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', attempts = 0, error = '', next_attempt_at = ?, updated_at = ? WHERE account_id = ? AND dialog_key = ? AND message_id = ? AND emoji = ? AND `+condition, now, now, intent.AccountID, key, intent.Message.MessageID, emoji)
		if err != nil {
			return false, err
		}
		changed, err := result.RowsAffected()
		return changed == 1, err
	}
	queued = inserted == 1
	if !queued {
		// A terminal failure is not a permanent user-facing ban. A new matching
		// reaction is an intentional retry request and starts a fresh attempt set.
		queued, err = reopen(`status = 'failed'`)
	}
	if err == nil && !queued {
		// A skipped row records the answer "nothing in this message matches the
		// filter". That answer is a function of the message and the current
		// settings, and a new reaction is the only way to ask again - so it must
		// re-evaluate instead of being told the event is a duplicate. Without
		// this arm, changing the file filter and reacting again would do nothing
		// at all, and would say nothing about why.
		queued, err = reopen(`status = 'skipped'`)
	}
	if err == nil && !queued {
		// A second application of the same emoji is a fresh user action when
		// its earlier task was cancelled or deleted. Completed jobs remain
		// idempotent so an ordinary redelivered Telegram update cannot revive
		// them.
		queued, err = reopen(`status = 'done' AND job_id IN (SELECT id FROM download_jobs WHERE status IN ('cancelled', 'deleted'))`)
	}
	if err != nil {
		return false, err
	}
	if queued {
		m.signal()
	}
	return queued, nil
}

// ClaimReactionInbox atomically leases a small batch. A process restart turns
// old leases back into pending work during migration/open recovery.
func (m *Manager) ClaimReactionInbox(limit int) ([]ReactionInboxEvent, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 8 {
		limit = 8
	}
	now := time.Now().UTC()
	// Probe before opening a transaction. Each resident worker polls this queue
	// on a one second timer, and BEGIN + SELECT + COMMIT costs three statements
	// on the common empty case where a single index probe answers the same
	// question. The poll stays short because a reaction is interactive work and
	// has no wake channel of its own.
	var pending bool
	if err := m.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM reaction_inbox WHERE status = 'pending' AND next_attempt_at::timestamptz <= ?::timestamptz)`, now.Format(time.RFC3339Nano)).Scan(&pending); err != nil {
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
	rows, err := tx.Query(`SELECT id, account_id, dialog_name, dialog_id, message_id, source_url, peer_type, peer_id, peer_hash, emoji, attempts
FROM reaction_inbox WHERE status = 'pending' AND next_attempt_at::timestamptz <= ?::timestamptz ORDER BY next_attempt_at, id LIMIT ?`, now.Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	result := make([]ReactionInboxEvent, 0, limit)
	for rows.Next() {
		var event ReactionInboxEvent
		var accountID, name, sourceURL, peerType string
		var dialogID, messageID, peerID, peerHash int64
		if err := rows.Scan(&event.ID, &accountID, &name, &dialogID, &messageID, &sourceURL, &peerType, &peerID, &peerHash, &event.Emoji, &event.Attempts); err != nil {
			return nil, err
		}
		peer := inboxPeer(peerType, peerID, peerHash)
		if peer == nil {
			return nil, fmt.Errorf("收件箱事件 %d 的会话引用无效", event.ID)
		}
		event.Intent = DownloadIntent{Source: SourceReaction, AccountID: accountID, Message: &MessageRef{SourceURL: sourceURL, DialogName: name, DialogID: dialogID, InputPeer: peer, MessageID: int(messageID)}, Trigger: map[string]string{"emoji": event.Emoji}}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	claimed := make([]ReactionInboxEvent, 0, len(result))
	for index := range result {
		event := &result[index]
		update, err := tx.Exec(`UPDATE reaction_inbox SET status = 'processing', attempts = attempts + 1, updated_at = ? WHERE id = ? AND status = 'pending'`, now.Format(time.RFC3339Nano), event.ID)
		if err != nil {
			return nil, err
		}
		if changed, _ := update.RowsAffected(); changed == 1 {
			event.Attempts++
			claimed = append(claimed, *event)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (m *Manager) CompleteReactionInbox(id int64, jobID string) error {
	_, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'done', job_id = ?, error = '', updated_at = ? WHERE id = ?`, jobID, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

// RetryReactionInbox follows the same rule as the message inbox: fast attempts
// stop at inboxFastAttempts, and a row that stops is offered again by
// reviveExhaustedInboxEvents rather than being left behind. A reaction row does
// have a redelivery path - a later reaction on the same message reopens it -
// but that depends on the user reacting again, which is not something a lost
// download should require.
//
// A rejection from Telegram is the exception to that rule, exactly as in the
// message inbox: it cannot succeed on a later attempt, so it stops now and
// spends the attempt budget at once instead of being revived every hour.
//
// An answered event is the other exception, and it is the one this queue needed
// most. A reaction whose message holds no file the current filter accepts used
// to be treated as a failure and retried like one - five times in half a minute,
// then hourly to twenty - every attempt re-reading the same message from
// Telegram to reach the answer it already had. It now settles as skipped on the
// first attempt.
func (m *Manager) RetryReactionInbox(id int64, attempts int, cause error) error {
	return m.retryInboxEvent(reactionInbox, id, attempts, cause)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
