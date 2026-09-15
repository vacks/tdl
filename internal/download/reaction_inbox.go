package download

import (
	"errors"
	"fmt"
	"time"

	"github.com/gotd/td/tg"
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
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed == 0 {
		// A terminal failure is not a permanent user-facing ban. A new matching
		// reaction is an intentional retry request and starts a fresh attempt set.
		result, err = m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', attempts = 0, error = '', next_attempt_at = ?, updated_at = ? WHERE account_id = ? AND dialog_key = ? AND message_id = ? AND emoji = ? AND status = 'failed'`, now, now, intent.AccountID, key, intent.Message.MessageID, emoji)
		if err != nil {
			return false, err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return false, err
		}
		if changed == 0 {
			// A second application of the same emoji is a fresh user action when
			// its earlier task was cancelled or deleted. Completed jobs remain
			// idempotent so an ordinary redelivered Telegram update cannot revive
			// them.
			result, err = m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', attempts = 0, error = '', next_attempt_at = ?, updated_at = ? WHERE account_id = ? AND dialog_key = ? AND message_id = ? AND emoji = ? AND status = 'done' AND job_id IN (SELECT id FROM download_jobs WHERE status IN ('cancelled', 'deleted'))`, now, now, intent.AccountID, key, intent.Message.MessageID, emoji)
			if err != nil {
				return false, err
			}
			changed, err = result.RowsAffected()
			if err != nil {
				return false, err
			}
		}
	}
	if changed == 1 {
		m.signal()
	}
	return changed == 1, nil
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
	tx, err := m.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	rows, err := tx.Query(`SELECT id, account_id, dialog_name, dialog_id, message_id, source_url, peer_type, peer_id, peer_hash, emoji, attempts
FROM reaction_inbox WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY id LIMIT ?`, now.Format(time.RFC3339Nano), limit)
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

func inboxPeer(kind string, id, hash int64) tg.InputPeerClass {
	switch kind {
	case "self":
		return &tg.InputPeerSelf{}
	case "user":
		return &tg.InputPeerUser{UserID: id, AccessHash: hash}
	case "chat":
		return &tg.InputPeerChat{ChatID: id}
	case "channel":
		return &tg.InputPeerChannel{ChannelID: id, AccessHash: hash}
	default:
		return nil
	}
}

func (m *Manager) CompleteReactionInbox(id int64, jobID string) error {
	_, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'done', job_id = ?, error = '', updated_at = ? WHERE id = ?`, jobID, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

func (m *Manager) RetryReactionInbox(id int64, attempts int, cause error) error {
	status := "pending"
	if attempts >= 5 {
		status = "failed"
	}
	delay := time.Duration(1<<min(attempts, 6)) * time.Second
	_, err := m.db.Exec(`UPDATE reaction_inbox SET status = ?, error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`, status, cause.Error(), time.Now().UTC().Add(delay).Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
