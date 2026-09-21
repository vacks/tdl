package download

import (
	"fmt"
	"time"

	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/telegram"
)

// chatMessageInboxEvent is a durable new-message notification. Telegram can
// redeliver updates, so its identity is unique per account/dialog/message.
type chatMessageInboxEvent struct {
	id       int64
	attempts int
	event    telegram.NewMessageEvent
}

func (m *Manager) enqueueChatMessage(event telegram.NewMessageEvent) {
	if event.MessageID <= 0 {
		return
	}
	if !m.DatabaseAvailable() {
		return
	}
	key := event.DialogKey
	if event.InputPeer != nil {
		_, key, _ = dialogIdentity(event.InputPeer, event.AccountID)
	}
	if key == "" {
		return
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
			return
		}
	}
	if event.InputPeer == nil {
		return
	}
	direct := makeDirectPeer(event.InputPeer)
	if direct.kind != "self" && direct.kind != "chat" && direct.kind != "channel" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := m.db.Exec(`INSERT INTO chat_message_inbox(account_id, dialog_key, dialog_name, dialog_id, message_id, reply_to_message_id, reply_to_top_id, peer_type, peer_id, peer_hash, status, attempts, next_attempt_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?) ON CONFLICT(account_id, dialog_key, message_id) DO NOTHING`, event.AccountID, key, event.DialogName, event.DialogID, event.MessageID, event.ReplyToMessageID, event.ReplyToTopID, direct.kind, direct.id, direct.hash, now, now, now)
	if err != nil {
		// A transient persistence error is retried only when Telegram redelivers
		// this update. Once admitted, later processing is fully durable.
		return
	}
	if changed, _ := result.RowsAffected(); changed == 1 {
		select {
		case m.chatEventWake <- struct{}{}:
		default:
		}
	}
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
	tx, err := m.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rows, err := tx.Query(`SELECT id, account_id, dialog_name, dialog_id, message_id, reply_to_message_id, reply_to_top_id, peer_type, peer_id, peer_hash, attempts
FROM chat_message_inbox WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY id LIMIT ?`, now, limit)
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

func (m *Manager) retryChatMessageInbox(id int64, attempts int, cause error) error {
	status := "pending"
	if attempts >= 5 {
		status = "failed"
	}
	delay := time.Duration(1<<min(attempts, 6)) * time.Second
	_, err := m.db.Exec(`UPDATE chat_message_inbox SET status = ?, error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`, status, cause.Error(), time.Now().UTC().Add(delay).Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

func waitChatEvent(delay time.Duration) bool {
	time.Sleep(delay)
	return true
}
