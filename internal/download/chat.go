package download

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	upstreamDL "github.com/iyear/tdl/app/dl"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/iyear/tdl/pkg/tmessage"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

// ChatJob is the single durable task for one channel or group. Its media index
// is an implementation detail, not a collection of child download tasks.
type ChatJob struct {
	ID              string  `json:"id"`
	SourceURL       string  `json:"sourceUrl"`
	DialogType      string  `json:"dialogType"`
	DialogKey       string  `json:"dialogKey"`
	DialogID        int64   `json:"dialogId"`
	DialogName      string  `json:"dialogName"`
	AccountID       string  `json:"accountId"`
	StartMessageID  int     `json:"startMessageId"`
	UpperMessageID  int     `json:"upperMessageId"`
	ListenNew       bool    `json:"listenNew"`
	Status          string  `json:"status"`
	ScanState       string  `json:"scanState"`
	Error           string  `json:"error,omitempty"`
	CreatedAt       string  `json:"createdAt"`
	UpdatedAt       string  `json:"updatedAt"`
	Discovered      int     `json:"discovered"`
	Completed       int     `json:"completed"`
	Failed          int     `json:"failed"`
	EarliestMediaID int     `json:"earliestMediaId"`
	ActiveFiles     int     `json:"activeFiles"`
	SpeedBPS        float64 `json:"speedBps"`
}

const (
	ChatStatusQueued      = "queued"
	ChatStatusScanning    = "scanning"
	ChatStatusDownloading = "downloading"
	ChatStatusListening   = "listening"
	ChatStatusPaused      = "paused"
	ChatStatusCancelled   = "cancelled"
	ChatStatusFailed      = "failed"
	ChatStatusPartial     = "partial"
	ChatStatusCompleted   = "completed"
	ChatStatusDeleted     = "deleted"
)

const (
	chatScanPending   = "pending"
	chatScanIndexing  = "indexing"
	chatScanCompleted = "completed"
	chatBatchSize     = 64
)

// Telegram exposes the media gallery as separate server-side filters. These
// four streams cover ordinary photos/videos, files, music, and the special
// voice/round-video class. Every result still enters the same message-ID
// index, so a server-side filter overlap can never create a duplicate file.
// Keep ordinary media streams first: each stream registers files page by page
// and the downloader can begin immediately. Reply metadata is intentionally
// last so a very large text history never blocks the first media downloads.
var chatStreamKinds = []string{"photo_video", "document", "music", "round_voice", "reply_candidates"}

// createChatJob persists only a resolved and bounded chat target. Discovery
// workers are attached in a later layer; keeping creation transactional lets
// callers safely retry an interrupted HTTP/Bot request without partial rows.
func (m *Manager) createChatJob(job ChatJob, direct directPeer, configJSON string) (ChatJob, error) {
	if job.DialogType != "channel" && job.DialogType != "chat" {
		return ChatJob{}, errors.New("会话下载仅支持频道和群组")
	}
	if job.DialogKey == "" || job.AccountID == "" || job.UpperMessageID < job.StartMessageID {
		return ChatJob{}, errors.New("会话下载目标不完整")
	}
	var existing string
	err := m.db.QueryRow(`SELECT id FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND start_message_id = ? AND status IN (?, ?, ?, ?, ?) LIMIT 1`, job.AccountID, job.DialogKey, job.StartMessageID, ChatStatusQueued, ChatStatusScanning, ChatStatusDownloading, ChatStatusListening, ChatStatusPaused).Scan(&existing)
	if err == nil {
		return ChatJob{}, errors.New("该会话下载任务已存在，请在会话下载列表中管理")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ChatJob{}, err
	}
	id, err := randomID()
	if err != nil {
		return ChatJob{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	job.ID, job.Status, job.ScanState, job.CreatedAt, job.UpdatedAt = id, ChatStatusQueued, chatScanPending, now, now
	tx, err := m.db.Begin()
	if err != nil {
		return ChatJob{}, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, start_message_id, upper_message_id, listen_new, status, scan_state, error, config_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)`, job.ID, job.SourceURL, job.DialogType, job.DialogKey, job.DialogID, job.DialogName, job.AccountID, direct.kind, direct.id, direct.hash, job.StartMessageID, job.UpperMessageID, boolInt(job.ListenNew), job.Status, job.ScanState, configJSON, now, now); err != nil {
		if isUniqueConstraintError(err) {
			return ChatJob{}, errors.New("该会话下载任务已存在，请在会话下载列表中管理")
		}
		return ChatJob{}, err
	}
	if _, err := tx.Exec(`INSERT INTO chat_download_stats(chat_job_id, updated_at) VALUES (?, ?)`, job.ID, now); err != nil {
		return ChatJob{}, err
	}
	for _, kind := range chatStreamKinds {
		if _, err := tx.Exec(`INSERT INTO chat_download_streams(chat_job_id, stream_kind) VALUES (?, ?)`, job.ID, kind); err != nil {
			return ChatJob{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ChatJob{}, err
	}
	m.visibleChats.Add(1)
	m.touch()
	return job, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// GetChat returns one parent task and its transactionally maintained summary.
// Individual media rows are deliberately not loaded here.
func (m *Manager) GetChat(id string) (ChatJob, error) {
	var job ChatJob
	var listen int
	err := m.db.QueryRow(`SELECT c.id, c.source_url, c.dialog_type, c.dialog_key, c.dialog_id, c.dialog_name, c.account_id, c.start_message_id, c.upper_message_id, c.listen_new, c.status, c.scan_state, c.error, c.created_at, c.updated_at,
 COALESCE(s.discovered, 0), COALESCE(s.completed, 0), COALESCE(s.failed, 0), COALESCE(s.earliest_message_id, 0)
 FROM chat_download_jobs c LEFT JOIN chat_download_stats s ON s.chat_job_id = c.id
	WHERE c.id = ?`, id).Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogID, &job.DialogName, &job.AccountID, &job.StartMessageID, &job.UpperMessageID, &listen, &job.Status, &job.ScanState, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.Discovered, &job.Completed, &job.Failed, &job.EarliestMediaID)
	if errors.Is(err, sql.ErrNoRows) {
		return ChatJob{}, errors.New("会话下载任务不存在")
	}
	if err != nil {
		return ChatJob{}, err
	}
	job.ListenNew = listen != 0
	job.ActiveFiles, job.SpeedBPS = m.progress.Aggregate(job.ID)
	return job, nil
}

// ListChats uses the same opaque cursor approach as message downloads. The
// count stays cheap even after media details have grown very large.
func (m *Manager) ListChats(cursor string, pageSize int) ([]ChatJob, int, string, error) {
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}
	total := m.visibleChatCount()
	where, args := "WHERE c.status != ?", []any{ChatStatusDeleted, pageSize + 1}
	if cursor != "" {
		createdAt, id, err := decodeChatCursor(cursor)
		if err != nil {
			return nil, 0, "", errors.New("分页游标无效，请返回第一页")
		}
		where += " AND (c.created_at < ? OR (c.created_at = ? AND c.id < ?))"
		args = []any{ChatStatusDeleted, createdAt, createdAt, id, pageSize + 1}
	}
	// The media index can be millions of rows. Its derived one-row summary is
	// maintained transactionally, so list rendering never aggregates history.
	rows, err := m.db.Query(`SELECT c.id, c.source_url, c.dialog_type, c.dialog_key, c.dialog_id, c.dialog_name, c.account_id, c.start_message_id, c.upper_message_id, c.listen_new, c.status, c.scan_state, c.error, c.created_at, c.updated_at,
	COALESCE(s.discovered, 0), COALESCE(s.completed, 0), COALESCE(s.failed, 0), COALESCE(s.earliest_message_id, 0)
FROM (SELECT * FROM chat_download_jobs c `+where+` ORDER BY created_at DESC, id DESC LIMIT ?) c
	LEFT JOIN chat_download_stats s ON s.chat_job_id = c.id
	ORDER BY c.created_at DESC, c.id DESC`, args...)
	if err != nil {
		return nil, 0, "", err
	}
	defer rows.Close()
	jobs := make([]ChatJob, 0, pageSize+1)
	for rows.Next() {
		var job ChatJob
		var listen int
		if err := rows.Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogID, &job.DialogName, &job.AccountID, &job.StartMessageID, &job.UpperMessageID, &listen, &job.Status, &job.ScanState, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.Discovered, &job.Completed, &job.Failed, &job.EarliestMediaID); err != nil {
			return nil, 0, "", err
		}
		job.ListenNew = listen != 0
		job.ActiveFiles, job.SpeedBPS = m.progress.Aggregate(job.ID)
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", err
	}
	next := ""
	if len(jobs) > pageSize {
		jobs = jobs[:pageSize]
		last := jobs[len(jobs)-1]
		next = encodeChatCursor(last.CreatedAt, last.ID)
	}
	return jobs, total, next, nil
}

func encodeChatCursor(createdAt, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(createdAt + "\x00" + id))
}

func decodeChatCursor(cursor string) (string, string, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(string(data), "\x00", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid cursor")
	}
	return parts[0], parts[1], nil
}

type storedChatTarget struct {
	ChatJob
	direct     directPeer
	configJSON string
}

// chatListener records the network configuration used to establish one
// account's long-lived update connection. A proxy change must replace this
// connection: otherwise only newly-created short-lived Telegram clients would
// honor the new configuration while new-media events kept arriving over the
// old route.
type chatListener struct {
	cancel context.CancelFunc
	proxy  string
}

func (m *Manager) beginChatExecution(id string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.chatCancelSeq++
	key := m.chatCancelSeq
	if m.chatCancels[id] == nil {
		m.chatCancels[id] = make(map[uint64]context.CancelFunc)
	}
	m.chatCancels[id][key] = cancel
	m.mu.Unlock()
	return ctx, func() {
		cancel()
		m.mu.Lock()
		delete(m.chatCancels[id], key)
		if len(m.chatCancels[id]) == 0 {
			delete(m.chatCancels, id)
		}
		m.mu.Unlock()
	}
}

func (m *Manager) cancelChatExecutions(id string) {
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.chatCancels[id]))
	for _, cancel := range m.chatCancels[id] {
		cancels = append(cancels, cancel)
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (m *Manager) chatTarget(id string) (storedChatTarget, error) {
	var target storedChatTarget
	var listen int
	err := m.db.QueryRow(`SELECT id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, listen_new, status, scan_state, error, created_at, updated_at, direct_peer_type, direct_peer_id, direct_peer_hash, config_json FROM chat_download_jobs WHERE id = ?`, id).Scan(&target.ID, &target.SourceURL, &target.DialogType, &target.DialogKey, &target.DialogID, &target.DialogName, &target.AccountID, &target.StartMessageID, &target.UpperMessageID, &listen, &target.Status, &target.ScanState, &target.Error, &target.CreatedAt, &target.UpdatedAt, &target.direct.kind, &target.direct.id, &target.direct.hash, &target.configJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return storedChatTarget{}, errors.New("会话下载任务不存在")
	}
	if err != nil {
		return storedChatTarget{}, err
	}
	target.ListenNew = listen != 0
	return target, nil
}

func targetConfigJSON(target storedChatTarget) string {
	if target.configJSON != "" {
		return target.configJSON
	}
	return "{}"
}

func (target storedChatTarget) inputPeer() tg.InputPeerClass {
	switch target.direct.kind {
	case "channel":
		return &tg.InputPeerChannel{ChannelID: target.direct.id, AccessHash: target.direct.hash}
	case "chat":
		return &tg.InputPeerChat{ChatID: target.direct.id}
	default:
		return nil
	}
}

// registerChatMedia stores each discovered media directly in this task's
// durable index. It intentionally never creates a download_jobs child.
func (m *Manager) registerChatMedia(chatID string, candidates []source, startTransfers bool) error {
	if len(candidates) == 0 {
		return nil
	}
	target, err := m.chatTarget(chatID)
	if err != nil {
		return err
	}
	if target.Status == ChatStatusPaused || target.Status == ChatStatusCancelled {
		return nil
	}
	// Historical stream rows originate in the target conversation. Reply rows
	// provide their own peer; fill only the legacy/original rows here.
	for i := range candidates {
		if candidates[i].OriginDialogName == "" {
			candidates[i].OriginDialogName = target.DialogName
		}
		if candidates[i].OriginMessageID == 0 {
			candidates[i].OriginMessageID = candidates[i].MessageID
		}
		if candidates[i].SourcePeerType == "" {
			candidates[i] = setSourcePeer([]source{candidates[i]}, target.inputPeer())[0]
		}
	}
	config := settings.Defaults().Download
	if target.configJSON != "" {
		if err := json.Unmarshal([]byte(target.configJSON), &config); err != nil {
			return fmt.Errorf("会话下载配置快照无效: %w", err)
		}
	}
	candidates = filterSources(candidates, config)
	if len(candidates) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	inserted := int64(0)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Persist the discussion-root map even when the currently discovered reply
	// is filtered out. Future matching replies can then be associated with this
	// listening task without scanning the channel again.
	for _, item := range candidates {
		if !item.IsComment || item.ReplyRootID <= 0 || item.DialogKey == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO chat_reply_roots(chat_job_id, account_id, discussion_dialog_key, root_message_id, origin_message_id) VALUES (?, ?, ?, ?, ?) ON CONFLICT(chat_job_id, discussion_dialog_key, root_message_id) DO NOTHING`, chatID, target.AccountID, item.DialogKey, item.ReplyRootID, item.OriginMessageID); err != nil {
			return err
		}
	}
	for _, item := range candidates {
		var existingStatus, finalPath, ownerKind, ownerID string
		err := tx.QueryRow(`SELECT status, final_path, owner_kind, owner_id FROM downloaded_media WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&existingStatus, &finalPath, &ownerKind, &ownerID)
		if errors.Is(err, sql.ErrNoRows) {
			// An absent global claim is the normal first-seen case. The
			// following branch either adopts an existing message task or creates
			// a new chat claim, so the original ErrNoRows must not escape this
			// loop as a fatal indexing error.
			err = nil
			// Ordinary message tasks predate the global claim table while they
			// are running. Treat one as an owner until it reaches a terminal
			// state; otherwise a newly indexed chat would download it again.
			var messageStatus, messagePath string
			messageErr := tx.QueryRow(`SELECT status, final_path FROM download_items WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&messageStatus, &messagePath)
			if messageErr == nil {
				if messageStatus == "completed" && messagePath != "" && regularFileExists(messagePath) {
					if _, err = tx.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES (?, ?, ?, 'completed', 'message', '', ?) ON CONFLICT(dialog_key, message_id) DO NOTHING`, item.DialogKey, item.MessageID, messagePath, now); err != nil {
						return err
					}
					existingStatus, finalPath = "completed", messagePath
				} else {
					existingStatus, ownerKind, ownerID = "claimed", "message", ""
				}
			} else if !errors.Is(messageErr, sql.ErrNoRows) {
				return messageErr
			}
		}
		if err != nil {
			return err
		}
		if existingStatus == "" {
			result, claimErr := tx.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?, ?, 'claimed', 'chat', ?, ?) ON CONFLICT(dialog_key, message_id) DO NOTHING`, item.DialogKey, item.MessageID, chatID, now)
			if claimErr != nil {
				return claimErr
			}
			claimed, _ := result.RowsAffected()
			if claimed == 1 {
				existingStatus, ownerKind, ownerID = "claimed", "chat", chatID
			} else if err = tx.QueryRow(`SELECT status, final_path, owner_kind, owner_id FROM downloaded_media WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&existingStatus, &finalPath, &ownerKind, &ownerID); err != nil {
				return err
			}
		}
		status := "queued"
		if existingStatus == "completed" && finalPath != "" && regularFileExists(finalPath) {
			status = "completed"
		}
		if existingStatus == "claimed" && (ownerKind != "chat" || ownerID != chatID) {
			status = "waiting"
		}
		result, err := tx.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, dialog_type, dialog_id, grouped_id, message_text, origin_dialog_name, origin_message_id, is_comment, source_peer_type, source_peer_id, source_peer_hash, original_name, size, final_path, status, discovered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_job_id, dialog_key, message_id) DO NOTHING`, chatID, item.DialogKey, item.MessageID, item.DialogType, item.DialogID, item.GroupedID, item.MessageText, item.OriginDialogName, item.OriginMessageID, boolInt(item.IsComment), item.SourcePeerType, item.SourcePeerID, item.SourcePeerHash, item.OriginalName, item.Size, finalPath, status, now)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed > 0 {
			inserted += changed
		}
	}
	if startTransfers && inserted > 0 {
		if _, err := tx.Exec(`UPDATE chat_download_jobs SET status = ?, error = '', updated_at = ? WHERE id = ? AND status = ?`, ChatStatusDownloading, now, chatID, ChatStatusListening); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	m.signalChat()
	return nil
}

// chatWorker is deliberately single-threaded. Telegram's search pagination is
// inexpensive because it asks only for media, and serializing it keeps API
// pressure predictable even when a user creates many large chat tasks.
func (m *Manager) chatWorker() {
	for {
		if !m.DatabaseAvailable() {
			select {
			case <-m.chatWake:
			case <-time.After(5 * time.Second):
			}
			continue
		}
		if err := m.scanOneChat(); err != nil {
			applog.Error("chat_download", "media_index_failed", "error", err.Error())
		}
		// A process can be interrupted after a final move but before the final
		// database state update. Reconcile on the regular worker cadence, not
		// only during restart/database recovery, so such a row cannot leave its
		// session task permanently in "下载中".
		if err := m.reconcileChatPublishedItems(); err != nil {
			applog.Error("chat_download", "published_file_reconcile_failed", "error", err.Error())
		}
		m.reconcileChatClaims()
		m.refreshChatStates()
		m.reconcileChatListeners()
		select {
		case <-m.chatWake:
		case <-time.After(3 * time.Second):
		}
	}
}

func (m *Manager) chatDownloadWorker() {
	for {
		if m.DatabaseAvailable() {
			if err := m.runOneChatBatch(); err != nil {
				applog.Error("chat_download", "media_transfer_failed", "error", err.Error())
			}
		}
		select {
		case <-m.chatWake:
		case <-time.After(750 * time.Millisecond):
		}
	}
}

// reconcileChatClaims wakes media that was held by another active task. A
// completed claim is adopted without another download; a released claim is
// atomically acquired on the next pass.
func (m *Manager) reconcileChatClaims() {
	rows, err := m.db.Query(`SELECT i.chat_job_id, i.dialog_key, i.message_id, m.status, m.final_path FROM chat_download_items i LEFT JOIN downloaded_media m ON m.dialog_key = i.dialog_key AND m.message_id = i.message_id WHERE i.status = 'waiting' LIMIT 256`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var chatID, key, status, path string
		var messageID int
		if rows.Scan(&chatID, &key, &messageID, &status, &path) != nil {
			continue
		}
		if status == "completed" && path != "" && regularFileExists(path) {
			if result, err := m.db.Exec(`UPDATE chat_download_items SET status = 'completed', final_path = ?, error = '' WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'waiting'`, path, chatID, key, messageID); err == nil {
				if changed, _ := result.RowsAffected(); changed == 1 {
					m.touch()
				}
			}
			continue
		}
		if status != "" {
			continue
		}
		var messageStatus, messagePath string
		messageErr := m.db.QueryRow(`SELECT status, final_path FROM download_items WHERE dialog_key = ? AND message_id = ?`, key, messageID).Scan(&messageStatus, &messagePath)
		if messageErr == nil {
			if messageStatus == "completed" && messagePath != "" && regularFileExists(messagePath) {
				_, _ = m.db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES (?, ?, ?, 'completed', 'message', '', ?) ON CONFLICT(dialog_key, message_id) DO NOTHING`, key, messageID, messagePath, time.Now().UTC().Format(time.RFC3339Nano))
				if result, err := m.db.Exec(`UPDATE chat_download_items SET status = 'completed', final_path = ?, error = '' WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'waiting'`, messagePath, chatID, key, messageID); err == nil {
					if changed, _ := result.RowsAffected(); changed == 1 {
						m.touch()
					}
				}
			}
			// A queued/running/paused regular task remains the owner. A terminal
			// failure or cancellation releases the media to this chat task.
			if messageStatus != "failed" && messageStatus != "cancelled" && messageStatus != "deleted" {
				continue
			}
		}
		if messageErr != nil && !errors.Is(messageErr, sql.ErrNoRows) {
			continue
		}
		result, claimErr := m.db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?, ?, 'claimed', 'chat', ?, ?) ON CONFLICT(dialog_key, message_id) DO NOTHING`, key, messageID, chatID, time.Now().UTC().Format(time.RFC3339Nano))
		if claimErr == nil {
			if changed, _ := result.RowsAffected(); changed == 1 {
				if result, err := m.db.Exec(`UPDATE chat_download_items SET status = 'queued', error = '' WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'waiting'`, chatID, key, messageID); err == nil {
					if updated, _ := result.RowsAffected(); updated == 1 {
						m.touch()
						m.signalChat()
					}
				}
			}
		}
	}
}

// claimChatMedia validates a queued media row immediately before transferring
// it. Retried rows reacquire their claim; completed rows are adopted instead
// of being downloaded a second time.
func (m *Manager) claimChatMedia(chatID string, item source) (string, string, error) {
	return m.claimMedia("chat", chatID, item)
}

// runOneChatBatch claims only a bounded slice of the media index. New media
// is made eligible as soon as it is indexed; within that visible queue, newer
// message IDs always take precedence. There is no per-media download task:
// the chat job owns the context, temporary directory and all durable item
// state.
func (m *Manager) runOneChatBatch() error {
	rows, err := m.db.Query(`SELECT id FROM chat_download_jobs WHERE status IN ('scanning', 'downloading', 'listening') AND EXISTS (SELECT 1 FROM chat_download_items i WHERE i.chat_job_id = chat_download_jobs.id AND i.status = 'queued') ORDER BY created_at LIMIT 16`)
	if err != nil {
		return err
	}
	var id string
	for rows.Next() {
		var candidate string
		if rows.Scan(&candidate) == nil {
			m.mu.Lock()
			_, busy := m.chatActive[candidate]
			if !busy {
				m.chatActive[candidate] = struct{}{}
				id = candidate
			}
			m.mu.Unlock()
			if id != "" {
				break
			}
		}
	}
	_ = rows.Close()
	if id == "" {
		return nil
	}
	defer func() { m.mu.Lock(); delete(m.chatActive, id); m.mu.Unlock() }()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.inputPeer() == nil {
		return errors.New("会话下载任务缺少 Telegram 会话引用")
	}
	rows, err = m.db.Query(`SELECT dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, origin_dialog_name, origin_message_id, is_comment, source_peer_type, source_peer_id, source_peer_hash, original_name, size FROM chat_download_items WHERE chat_job_id = ? AND status = 'queued' ORDER BY message_id DESC LIMIT ?`, id, chatBatchSize)
	if err != nil {
		return err
	}
	batch := make([]source, 0, chatBatchSize)
	for rows.Next() {
		var item Item
		var isComment int
		if err := rows.Scan(&item.DialogType, &item.DialogKey, &item.DialogID, &item.MessageID, &item.GroupedID, &item.MessageText, &item.OriginDialogName, &item.OriginMessageID, &isComment, &item.SourcePeerType, &item.SourcePeerID, &item.SourcePeerHash, &item.OriginalName, &item.Size); err != nil {
			rows.Close()
			return err
		}
		item.IsComment = isComment != 0
		batch = append(batch, source{Item: item, DialogName: target.DialogName, Direct: directPeer{kind: item.SourcePeerType, id: item.SourcePeerID, hash: item.SourcePeerHash}})
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	// A session may now contain channel media and linked-discussion replies.
	// Upstream completion callbacks carry only MessageID, so never place two
	// Telegram dialogs in one invocation where numeric IDs could collide.
	selected := batch[0].Direct
	if selected.kind == "" {
		selected = makeDirectPeer(target.inputPeer())
	}
	selectedKey := fmt.Sprintf("%s:%d:%d", selected.kind, selected.id, selected.hash)
	filtered := make([]source, 0, len(batch))
	for _, item := range batch {
		direct := item.Direct
		if direct.kind == "" {
			direct = makeDirectPeer(target.inputPeer())
		}
		if fmt.Sprintf("%s:%d:%d", direct.kind, direct.id, direct.hash) != selectedKey {
			continue
		}
		item.Direct = direct
		filtered = append(filtered, item)
	}
	batch = filtered
	transfer := make([]source, 0, len(batch))
	for _, item := range batch {
		claim, path, err := m.claimChatMedia(id, item)
		if err != nil {
			return err
		}
		switch claim {
		case "completed":
			if err := m.setChatItem(id, item, "completed", path, ""); err != nil {
				return err
			}
		case "waiting":
			if _, err := m.db.Exec(`UPDATE chat_download_items SET status = 'waiting', error = '' WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'queued'`, id, item.DialogKey, item.MessageID); err != nil {
				return err
			}
		default:
			transfer = append(transfer, item)
		}
	}
	batch = transfer
	if len(batch) == 0 {
		return nil
	}
	for _, item := range batch {
		if _, err := m.db.Exec(`UPDATE chat_download_items SET attempts = attempts + 1, error = '', started_at = '', finished_at = '' WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'queued'`, id, item.DialogKey, item.MessageID); err != nil {
			return err
		}
	}
	// Register this transfer while holding the same lock as pause/cancel. This
	// closes the small window where a control action could finish before this
	// batch had published its cancel function, leaving a newly-started upstream
	// transfer outside that action's reach.
	lock := m.chatLock(id)
	lock.Lock()
	if !chatRunnable(m.chatStatus(id)) {
		lock.Unlock()
		return nil
	}
	ctx, release := m.beginChatExecution(id)
	lock.Unlock()
	defer func() { release(); m.progress.ClearJob(id) }()
	transferCtx, stopTransfer := context.WithCancel(ctx)
	defer stopTransfer()
	config := m.settings.Get()
	if target.configJSON != "" {
		if err := json.Unmarshal([]byte(target.configJSON), &config.Download); err != nil {
			return fmt.Errorf("会话下载配置快照无效: %w", err)
		}
	}
	byMessage := make(map[int]source, len(batch))
	ids := make([]int, 0, len(batch))
	groups := map[int64]struct{}{}
	for _, item := range batch {
		byMessage[item.MessageID] = item
		if item.GroupedID != 0 {
			if _, ok := groups[item.GroupedID]; ok {
				continue
			}
			groups[item.GroupedID] = struct{}{}
		}
		ids = append(ids, item.MessageID)
	}
	tmpDir := filepath.Join(m.downloadDir, ".tdl-tmp", "chat-"+id)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	watchdog := startTransferWatchdog(transferCtx, upstreamWatchPeriod, upstreamInitialTimeout, upstreamIdleTimeout, stopTransfer, func() bool {
		status := m.chatStatus(id)
		return status == ChatStatusScanning || status == ChatStatusDownloading || status == ChatStatusListening
	}, func(timeout time.Duration) {
		applog.Info("chat_download", "batch_no_progress_timeout", "chat_job_id", id, "item_count", len(batch), "timeout", timeout.String())
	})
	defer watchdog.Close()
	var publishWG sync.WaitGroup
	var publishMu sync.Mutex
	var publishErr error
	recordPublishErr := func(err error) {
		if err == nil {
			return
		}
		publishMu.Lock()
		if publishErr == nil {
			publishErr = err
		}
		publishMu.Unlock()
	}
	err = m.accounts.Run(transferCtx, target.AccountID, func(runCtx context.Context, client *gotd.Client, kvd storage.Storage) error {
		peer := selected.inputPeer()
		if peer == nil {
			return errors.New("会话下载文件缺少 Telegram 来源会话")
		}
		opts := upstreamDL.Options{Dir: tmpDir, Template: config.Download.TempFilenameTemplate, Group: true, Continue: true, Quiet: true, Runtime: &upstreamDL.RuntimeOptions{Threads: config.Download.Threads, TaskLimit: config.Download.TaskLimit, PoolSize: config.Download.PoolSize, Delay: time.Duration(config.Download.DelayMS) * time.Millisecond, DisableProgressPS: true}, DirectDialogs: [][]*tmessage.Dialog{{{Peer: peer, Messages: ids}}}, ProgressCallback: func(update upstreamDL.ProgressUpdate) {
			item, ok := byMessage[update.MessageID]
			if !ok {
				return
			}
			watchdog.Touch()
			started, _ := m.progress.Update(id, item.Item, update)
			if started {
				_, _ = m.db.Exec(`UPDATE chat_download_items SET status='running', started_at=? WHERE chat_job_id=? AND dialog_key=? AND message_id=? AND status='queued'`, time.Now().UTC().Format(time.RFC3339Nano), id, item.DialogKey, item.MessageID)
			}
		}, FileCompletedCallback: func(update upstreamDL.FileCompletedUpdate) {
			item, ok := byMessage[update.MessageID]
			if !ok {
				return
			}
			watchdog.Touch()
			m.progress.ClearItem(id, item.Item)
			// Upstream invokes completion callbacks from transfer workers. Keep
			// final file moves out of those workers, but wait below before the
			// batch decides its final state. publishChatItem itself records a
			// per-file failure; a post-move database failure is recovered by the
			// reconciliation immediately after the wait and on worker cadence.
			publishWG.Add(1)
			go func() {
				defer publishWG.Done()
				if publishErr := m.publishChatItem(id, update.Path, item, config); publishErr != nil {
					recordPublishErr(publishErr)
					applog.Error("chat_download", "file_publish_failed", "chat_job_id", id, "message_id", item.MessageID, "error", publishErr.Error())
				}
			}()
		}}
		return upstreamDL.Run(runCtx, client, kvd, opts)
	})
	publishWG.Wait()
	if reconcileErr := m.reconcileChatPublishedItems(); reconcileErr != nil {
		return fmt.Errorf("核对已移动文件: %w", reconcileErr)
	}
	publishMu.Lock()
	persistErr := publishErr
	publishMu.Unlock()
	if persistErr != nil {
		// A transient database write failure before the final move leaves the
		// item running. Put only those still-running rows back on the durable
		// queue; rows that were already reconciled to completed are untouched.
		if recoverErr := m.requeueChatPublishFailures(id, batch); recoverErr != nil {
			return fmt.Errorf("恢复文件发布状态: %w", recoverErr)
		}
	}
	if watchdog.Stalled() && (m.chatStatus(id) == ChatStatusScanning || m.chatStatus(id) == ChatStatusDownloading || m.chatStatus(id) == ChatStatusListening) {
		return m.requeueStalledChatBatch(id, batch)
	}
	if err != nil && m.chatStatus(id) != ChatStatusPaused && m.chatStatus(id) != ChatStatusCancelled {
		for _, item := range batch {
			// Another file in this upstream call can fail after this one has
			// completed its final move. Never overwrite that completed state.
			if state := m.chatItemStatus(id, item); state != "completed" && state != "downloaded" {
				_ = m.setChatItem(id, item, "failed", "", err.Error())
			}
		}
		return err
	}
	m.touch()
	return nil
}

// requeueChatPublishFailures recovers only rows whose final-path state could
// not be saved. It deliberately retains downloaded/completed rows: the former
// are reconciled from their already-moved file and the latter are final.
func (m *Manager) requeueChatPublishFailures(chatID string, batch []source) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	changed := false
	for _, item := range batch {
		result, err := tx.Exec(`UPDATE chat_download_items
SET status = 'queued', error = '文件发布状态保存失败，已自动重试',
    elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END,
    started_at = '', finished_at = ''
WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'running'`, now, chatID, item.DialogKey, item.MessageID)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count > 0 {
			changed = true
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if changed {
		m.touch()
		m.signalChat()
	}
	return nil
}

// requeueStalledChatBatch leaves completed rows untouched and gives the
// remaining media back to the bounded chat queue. Its global claims are
// released in the same transaction so another valid request may also adopt
// them; this task will reclaim them on its next batch attempt.
func (m *Manager) requeueStalledChatBatch(chatID string, batch []source) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range batch {
		if _, err := tx.Exec(`UPDATE chat_download_items
SET status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'queued' END,
    error = CASE WHEN attempts >= ? THEN '连续无进度，已停止自动重试' ELSE '下载长时间无进度，已自动重试' END,
    elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END,
    started_at = '', finished_at = CASE WHEN attempts >= ? AND finished_at = '' THEN ? ELSE finished_at END
WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status IN ('queued', 'running')`, maxStalledAttempts, maxStalledAttempts, now, maxStalledAttempts, now, chatID, item.DialogKey, item.MessageID); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM downloaded_media WHERE dialog_key = ? AND message_id = ? AND status = 'claimed' AND owner_kind = 'chat' AND owner_id = ?`, item.DialogKey, item.MessageID, chatID); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	m.signalChat()
	applog.Info("chat_download", "stalled_batch_requeued", "chat_job_id", chatID, "item_count", len(batch))
	return nil
}

func (m *Manager) chatStatus(id string) string {
	var status string
	_ = m.db.QueryRow(`SELECT status FROM chat_download_jobs WHERE id = ?`, id).Scan(&status)
	return status
}

func chatRunnable(status string) bool {
	return status == ChatStatusScanning || status == ChatStatusDownloading || status == ChatStatusListening
}

func (m *Manager) chatItemStatus(chatID string, item source) string {
	var status string
	_ = m.db.QueryRow(`SELECT status FROM chat_download_items WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ?`, chatID, item.DialogKey, item.MessageID).Scan(&status)
	return status
}

func (m *Manager) setChatItem(chatID string, item source, status, path, message string) error {
	finished := ""
	if status == "completed" || status == "failed" || status == "cancelled" {
		finished = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err := m.db.Exec(`UPDATE chat_download_items SET status=?, final_path=?, error=?, finished_at=CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE chat_job_id=? AND dialog_key=? AND message_id=?`, status, path, message, finished, finished, chatID, item.DialogKey, item.MessageID)
	if err != nil {
		return err
	}
	if status == "failed" || status == "cancelled" {
		_, err = m.db.Exec(`DELETE FROM downloaded_media WHERE dialog_key = ? AND message_id = ? AND status = 'claimed' AND owner_kind = 'chat' AND owner_id = ?`, item.DialogKey, item.MessageID, chatID)
		return err
	}
	if status != "completed" {
		return nil
	}
	_, err = m.db.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES (?, ?, ?, 'completed', 'chat', ?, ?) ON CONFLICT(dialog_key, message_id) DO UPDATE SET final_path = EXCLUDED.final_path, status = EXCLUDED.status, owner_kind = EXCLUDED.owner_kind, owner_id = EXCLUDED.owner_id, updated_at = EXCLUDED.updated_at`, item.DialogKey, item.MessageID, path, chatID, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// reconcileChatPublishedItems closes the crash window after a final file move
// but before the media index and global ownership record were committed.
func (m *Manager) reconcileChatPublishedItems() error {
	rows, err := m.db.Query(`SELECT chat_job_id, dialog_key, message_id, final_path FROM chat_download_items WHERE status = 'downloaded' AND final_path <> ''`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type candidate struct {
		chatID    string
		dialogKey string
		messageID int
		path      string
	}
	items := make([]candidate, 0)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.chatID, &item.dialogKey, &item.messageID, &item.path); err != nil {
			return err
		}
		if regularFileExists(item.path) {
			items = append(items, item)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, item := range items {
		// Publish, pause and cancel must share a single per-chat linearization
		// point. Otherwise a completion callback which started just before a
		// cancellation can recreate a completed row after CancelChat marked it
		// cancelled and released its global claim.
		lock := m.chatLock(item.chatID)
		lock.Lock()
		tx, err := m.db.Begin()
		if err != nil {
			lock.Unlock()
			return err
		}
		_, err = tx.Exec(`UPDATE chat_download_items SET status = 'completed', error = '', finished_at = CASE WHEN finished_at = '' THEN ? ELSE finished_at END WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'downloaded'`, now, item.chatID, item.dialogKey, item.messageID)
		if err == nil {
			_, err = tx.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES (?, ?, ?, 'completed', 'chat', ?, ?) ON CONFLICT(dialog_key, message_id) DO UPDATE SET final_path = EXCLUDED.final_path, status = EXCLUDED.status, owner_kind = EXCLUDED.owner_kind, owner_id = EXCLUDED.owner_id, updated_at = EXCLUDED.updated_at`, item.dialogKey, item.messageID, item.path, item.chatID, now)
		}
		if err != nil {
			_ = tx.Rollback()
			lock.Unlock()
			return err
		}
		if err := tx.Commit(); err != nil {
			lock.Unlock()
			return err
		}
		lock.Unlock()
	}
	return nil
}

func (m *Manager) publishChatItem(chatID, path string, item source, config settings.Values) error {
	// A control action waits for an in-flight final-file publish to reach one
	// durable result. Later callbacks observe the paused/cancelled parent and
	// return without moving files or changing item state.
	lock := m.chatLock(chatID)
	lock.Lock()
	defer lock.Unlock()
	if status := m.chatStatus(chatID); status != ChatStatusScanning && status != ChatStatusDownloading && status != ChatStatusListening {
		return nil
	}
	finalPath, err := finalDestination(m.downloadDir, config.Download.FinalFilenameTemplate, item)
	if err != nil {
		return m.setChatItem(chatID, item, "failed", "", err.Error())
	}
	if _, err := os.Stat(finalPath); err == nil {
		return m.setChatItem(chatID, item, "failed", "", "目标文件已存在，未覆盖")
	}
	if err := m.setChatItem(chatID, item, "downloaded", finalPath, ""); err != nil {
		return err
	}
	if err := publishNoReplace(path, finalPath); err != nil {
		return m.setChatItem(chatID, item, "failed", "", err.Error())
	}
	return m.setChatItem(chatID, item, "completed", finalPath, "")
}

func (m *Manager) reconcileChatListeners() {
	rows, err := m.db.Query(`SELECT account_id, dialog_key FROM chat_download_jobs WHERE listen_new = 1 AND scan_state = ? AND status IN (?, ?)
UNION
SELECT r.account_id, r.discussion_dialog_key FROM chat_reply_roots r JOIN chat_download_jobs j ON j.id = r.chat_job_id WHERE j.listen_new = 1 AND j.scan_state = ? AND j.status IN (?, ?)`, chatScanCompleted, ChatStatusDownloading, ChatStatusListening, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return
	}
	defer rows.Close()
	wanted := make(map[string]struct{})
	watched := make(map[string]map[string]struct{})
	for rows.Next() {
		var accountID, dialogKey string
		if rows.Scan(&accountID, &dialogKey) == nil && accountID != "" && dialogKey != "" {
			wanted[accountID] = struct{}{}
			if watched[accountID] == nil {
				watched[accountID] = make(map[string]struct{})
			}
			watched[accountID][dialogKey] = struct{}{}
		}
	}
	proxyURL := m.settings.ProxyURL()
	m.mu.Lock()
	m.chatWatched = watched
	for accountID := range wanted {
		if current, exists := m.chatListeners[accountID]; exists && current.proxy == proxyURL {
			continue
		} else if exists {
			// Leave replacement ownership with the new pointer. The old goroutine
			// will only remove itself when it still owns the map entry.
			current.cancel()
		}
		ctx, cancel := context.WithCancel(context.Background())
		listener := &chatListener{cancel: cancel, proxy: proxyURL}
		m.chatListeners[accountID] = listener
		go m.runChatListener(ctx, accountID, listener)
	}
	for accountID, listener := range m.chatListeners {
		if _, keep := wanted[accountID]; !keep {
			listener.cancel()
			delete(m.chatListeners, accountID)
		}
	}
	m.mu.Unlock()
}

// addChatWatched closes the interval between persisting a discussion mapping
// and the next periodic listener reconciliation. The account listener is
// already running for the parent channel, so only its in-memory filter needs
// updating.
func (m *Manager) addChatWatched(accountID, dialogKey string) {
	if accountID == "" || dialogKey == "" {
		return
	}
	m.mu.Lock()
	if m.chatWatched[accountID] == nil {
		m.chatWatched[accountID] = make(map[string]struct{})
	}
	m.chatWatched[accountID][dialogKey] = struct{}{}
	m.mu.Unlock()
}

func (m *Manager) runChatListener(ctx context.Context, accountID string, listener *chatListener) {
	err := m.accounts.ListenNewMessages(ctx, accountID, func(_ context.Context, event telegram.NewMessageEvent) {
		m.enqueueChatMessage(event)
	}, nil)
	if err != nil && ctx.Err() == nil {
		applog.Error("chat_download", "new_message_listener_failed", "account_id", accountID, "error", err.Error())
	}
	m.mu.Lock()
	if current, ok := m.chatListeners[accountID]; ok && current == listener {
		delete(m.chatListeners, accountID)
	}
	m.mu.Unlock()
}

func (m *Manager) chatEventWorker() {
	for {
		if !m.DatabaseAvailable() {
			if !waitChatEvent(time.Second * 5) {
				return
			}
			continue
		}
		events, err := m.claimChatMessageInbox(1)
		if err != nil {
			applog.Error("chat_download", "new_message_claim_failed", "error", err.Error())
			if !waitChatEvent(2 * time.Second) {
				return
			}
			continue
		}
		if len(events) == 0 {
			select {
			case <-m.chatEventWake:
			case <-time.After(time.Second):
			}
			continue
		}
		for _, event := range events {
			if err := m.handleNewChatMessage(event.event); err != nil {
				if retryErr := m.retryChatMessageInbox(event.id, event.attempts, err); retryErr != nil {
					applog.Error("chat_download", "new_message_retry_update_failed", "inbox_id", event.id, "error", retryErr.Error())
				}
				continue
			}
			if err := m.completeChatMessageInbox(event.id); err != nil {
				applog.Error("chat_download", "new_message_complete_update_failed", "inbox_id", event.id, "error", err.Error())
			}
		}
	}
}

func (m *Manager) handleNewChatMessage(event telegram.NewMessageEvent) error {
	if event.InputPeer == nil || event.MessageID <= 0 {
		return nil
	}
	_, dialogKey, _ := dialogIdentity(event.InputPeer, event.AccountID)
	rows, err := m.db.Query(`SELECT id FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?)`, event.AccountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// A later discussion comment arrives in its linked discussion group, not
	// the channel being watched. Associate it through the persisted discussion
	// root recorded during history indexing.
	originByJob := make(map[string]int)
	if len(ids) == 0 {
		rootID := event.ReplyToTopID
		if rootID == 0 {
			rootID = event.ReplyToMessageID
		}
		if rootID <= 0 {
			return nil
		}
		replyRows, queryErr := m.db.Query(`SELECT r.chat_job_id, r.origin_message_id FROM chat_reply_roots r JOIN chat_download_jobs j ON j.id = r.chat_job_id WHERE r.account_id = ? AND r.discussion_dialog_key = ? AND r.root_message_id = ? AND j.listen_new = 1 AND j.scan_state = ? AND j.status IN (?, ?)`, event.AccountID, dialogKey, rootID, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
		if queryErr != nil {
			return queryErr
		}
		for replyRows.Next() {
			var id string
			var originID int
			if replyRows.Scan(&id, &originID) == nil {
				ids = append(ids, id)
				originByJob[id] = originID
			}
		}
		if err := replyRows.Close(); err != nil {
			return err
		}
		if len(ids) == 0 {
			// The original post may have had zero replies while history was
			// indexed, therefore no persistent root mapping existed yet. Recover
			// its channel/post from the discussion-root forward header on demand.
			originKey, originID, resolvedRootID, discoverErr := m.discoverDiscussionOrigin(event, rootID)
			if discoverErr != nil {
				return discoverErr
			}
			if originKey == "" || originID <= 0 || resolvedRootID <= 0 {
				return nil
			}
			originRows, originErr := m.db.Query(`SELECT id, config_json FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?)`, event.AccountID, originKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
			if originErr != nil {
				return originErr
			}
			for originRows.Next() {
				var id, configJSON string
				if originRows.Scan(&id, &configJSON) != nil {
					continue
				}
				if !targetIncludesReplies(storedChatTarget{configJSON: configJSON}) {
					continue
				}
				ids = append(ids, id)
				originByJob[id] = originID
				direct := makeDirectPeer(event.InputPeer)
				if _, persistErr := m.db.Exec(`INSERT INTO chat_reply_roots(chat_job_id, account_id, discussion_dialog_key, root_message_id, origin_message_id, discussion_peer_type, discussion_peer_id, discussion_peer_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_job_id, discussion_dialog_key, root_message_id) DO UPDATE SET discussion_peer_type = EXCLUDED.discussion_peer_type, discussion_peer_id = EXCLUDED.discussion_peer_id, discussion_peer_hash = EXCLUDED.discussion_peer_hash`, id, event.AccountID, dialogKey, resolvedRootID, originID, direct.kind, direct.id, direct.hash); persistErr != nil {
					_ = originRows.Close()
					return persistErr
				}
				m.addChatWatched(event.AccountID, dialogKey)
			}
			if originErr := originRows.Err(); originErr != nil {
				_ = originRows.Close()
				return originErr
			}
			if err := originRows.Close(); err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
		}
	}
	resolved := make(map[bool][]source, 2)
	resolvedErr := make(map[bool]error, 2)
	for _, id := range ids {
		target, targetErr := m.chatTarget(id)
		if targetErr != nil {
			return targetErr
		}
		includeReplies := targetIncludesReplies(target)
		// A comment event has already been associated with its parent; expanding
		// replies beneath that reply would both waste API calls and violate the
		// stored task's origin mapping.
		if originByJob[id] > 0 {
			if !includeReplies {
				// Candidate admission is account-wide so an unmapped first comment
				// can be discovered. Per-task snapshots remain authoritative.
				continue
			}
			includeReplies = false
		}
		if _, done := resolved[includeReplies]; !done && resolvedErr[includeReplies] == nil {
			resolved[includeReplies], resolvedErr[includeReplies] = m.resolvePeerWithReplies(context.Background(), event.AccountID, event.InputPeer, event.DialogID, event.MessageID, event.DialogName, includeReplies)
		}
		if resolveErr := resolvedErr[includeReplies]; resolveErr != nil {
			applog.Info("chat_download", "new_message_not_downloadable", "account_id", event.AccountID, "message_id", event.MessageID, "error", resolveErr.Error())
			continue
		}
		items := resolved[includeReplies]
		if originID := originByJob[id]; originID > 0 {
			items = make([]source, len(resolved[false]))
			copy(items, resolved[false])
			items = setOrigin(items, target.DialogName, originID, true)
		}
		if err := m.registerChatMedia(id, items, true); err != nil {
			applog.Error("chat_download", "new_media_register_failed", "chat_job_id", id, "message_id", event.MessageID, "error", err.Error())
			return err
		}
		if originByJob[id] == 0 && includeReplies {
			if err := m.rememberNewChatDiscussion(event, target); err != nil {
				logRelatedWarning(event.AccountID, event.MessageID, err)
			}
		}
		applog.Info("chat_download", "new_media_queued", "chat_job_id", id, "message_id", event.MessageID, "item_count", len(items))
	}
	return nil
}

func targetIncludesReplies(target storedChatTarget) bool {
	config := settings.Defaults().Download
	if target.configJSON != "" && json.Unmarshal([]byte(target.configJSON), &config) != nil {
		return false
	}
	return config.IncludeReplies
}

func (m *Manager) rememberNewChatDiscussion(event telegram.NewMessageEvent, target storedChatTarget) error {
	if target.DialogType != "channel" || event.InputPeer == nil || event.MessageID <= 0 {
		return nil
	}
	return m.accounts.Run(context.Background(), event.AccountID, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
		return m.rememberChatDiscussionRoot(ctx, client.API(), target, event.MessageID, event.MessageID)
	})
}

func (m *Manager) discoverDiscussionOrigin(event telegram.NewMessageEvent, rootID int) (string, int, int, error) {
	var originKey string
	var originID, resolvedRootID int
	err := m.accounts.Run(context.Background(), event.AccountID, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
		var err error
		originKey, originID, resolvedRootID, err = discussionOriginFromRoot(ctx, client.API(), event.InputPeer, rootID)
		return err
	})
	return originKey, originID, resolvedRootID, err
}

func (m *Manager) scanOneChat() error {
	var id string
	err := m.db.QueryRow(`SELECT id FROM chat_download_jobs WHERE status = 'queued' AND scan_state != 'completed' ORDER BY created_at LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	claim, err := m.db.Exec(`UPDATE chat_download_jobs SET status = ?, scan_state = ?, error = '', updated_at = ? WHERE id = ? AND status = 'queued'`, ChatStatusScanning, chatScanIndexing, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return err
	}
	if changed, _ := claim.RowsAffected(); changed != 1 {
		return nil
	}
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	ctx, release := m.beginChatExecution(id)
	defer release()
	err = m.accounts.Run(ctx, target.AccountID, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
		for _, kind := range chatStreamKinds {
			if _, err := m.db.Exec(`INSERT INTO chat_download_streams(chat_job_id, stream_kind) VALUES (?, ?) ON CONFLICT(chat_job_id, stream_kind) DO NOTHING`, target.ID, kind); err != nil {
				return err
			}
			if kind == "reply_candidates" {
				if err := m.scanChatReplyCandidates(ctx, client, target); err != nil {
					return err
				}
				continue
			}
			if err := m.scanChatStream(ctx, client, target, kind); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_, _ = m.db.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, ChatStatusFailed, err.Error(), time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning)
		m.touch()
		return err
	}
	current, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if current.Status != ChatStatusScanning {
		// Pause/cancel may race the network request. Do not enqueue the
		// remaining indexed media after the parent has stopped accepting work.
		return nil
	}
	_, err = m.db.Exec(`UPDATE chat_download_jobs SET scan_state = ?, status = ?, error = '', updated_at = ? WHERE id = ? AND status = ?`, chatScanCompleted, ChatStatusDownloading, time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning)
	if err == nil {
		m.touch()
	}
	return err
}

// scanChatStream uses messages.search with a media-only filter. OffsetID is
// persisted after every page, so recovery never has to start the history over;
// MinID/MaxID keep the immutable range fixed at task creation time.
func (m *Manager) scanChatStream(ctx context.Context, client *gotd.Client, target storedChatTarget, kind string) error {
	var offset int
	var completed int
	if err := m.db.QueryRow(`SELECT offset_message_id, completed FROM chat_download_streams WHERE chat_job_id = ? AND stream_kind = ?`, target.ID, kind).Scan(&offset, &completed); err != nil {
		return err
	}
	if completed != 0 {
		return nil
	}
	filter, label, err := chatStreamFilter(kind)
	if err != nil {
		return err
	}
	for {
		current, err := m.chatTarget(target.ID)
		if err != nil {
			return err
		}
		if current.Status != ChatStatusScanning {
			return nil
		}
		minID := target.StartMessageID - 1
		if minID < 0 {
			minID = 0
		}
		result, err := client.API().MessagesSearch(ctx, &tg.MessagesSearchRequest{Peer: target.inputPeer(), Q: "", Filter: filter, OffsetID: offset, Limit: 100, MinID: minID, MaxID: target.UpperMessageID + 1})
		if err != nil {
			return fmt.Errorf("搜索%s文件: %w", label, err)
		}
		messages := searchMessages(result)
		if len(messages) == 0 {
			_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = ?`, target.ID, kind)
			return err
		}
		candidates := make([]source, 0, len(messages))
		seenGroups := make(map[int64]struct{})
		nextOffset := offset
		for _, raw := range messages {
			message, ok := raw.(*tg.Message)
			if !ok || message.ID < target.StartMessageID || message.ID > target.UpperMessageID {
				continue
			}
			if nextOffset == 0 || message.ID < nextOffset {
				nextOffset = message.ID
			}
			groupedID, grouped := message.GetGroupedID()
			if grouped {
				if _, exists := seenGroups[groupedID]; exists {
					continue
				}
				seenGroups[groupedID] = struct{}{}
			}
			members := []*tg.Message{message}
			if grouped {
				if expanded, groupErr := tutil.GetGroupedMessages(ctx, client.API(), target.inputPeer(), message); groupErr == nil && len(expanded) > 0 {
					members = expanded
				}
			}
			originID := firstMessageID(members, message.ID)
			caption := groupDisplayText(members)
			for _, member := range members {
				if member == nil || member.ID < target.StartMessageID || member.ID > target.UpperMessageID {
					continue
				}
				media, ok := tmedia.GetMedia(member)
				if !ok {
					continue
				}
				memberGroup, _ := member.GetGroupedID()
				candidates = append(candidates, source{Item: Item{DialogType: target.DialogType, DialogKey: target.DialogKey, DialogID: target.DialogID, MessageID: member.ID, GroupedID: memberGroup, MessageText: caption, OriginalName: media.Name, Size: media.Size, OriginDialogName: target.DialogName, OriginMessageID: originID}, DialogName: target.DialogName, MediaType: messageMediaType(member), Direct: makeDirectPeer(target.inputPeer())})
			}
			config := settings.Defaults().Download
			if target.configJSON != "" {
				_ = json.Unmarshal([]byte(target.configJSON), &config)
			}
		}
		if err := m.registerChatMedia(target.ID, candidates, false); err != nil {
			return err
		}
		if nextOffset == 0 || nextOffset == offset {
			return errors.New("Telegram 文件搜索未推进分页游标")
		}
		offset = nextOffset
		if _, err := m.db.Exec(`UPDATE chat_download_streams SET offset_message_id = ? WHERE chat_job_id = ? AND stream_kind = ?`, offset, target.ID, kind); err != nil {
			return err
		}
		m.touch()
	}
}

// scanChatReplyCandidates walks lightweight message metadata so replies under
// a text-only channel post are not silently missed by the media-only gallery
// streams. The expensive discussion/replies APIs are invoked only for a
// message Telegram marks as having replies.
func (m *Manager) scanChatReplyCandidates(ctx context.Context, client *gotd.Client, target storedChatTarget) error {
	config := settings.Defaults().Download
	if target.configJSON != "" {
		if err := json.Unmarshal([]byte(target.configJSON), &config); err != nil {
			return fmt.Errorf("会话下载配置快照无效: %w", err)
		}
	}
	if !config.IncludeReplies {
		_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, target.ID)
		return err
	}
	var offset, completed int
	if err := m.db.QueryRow(`SELECT offset_message_id, completed FROM chat_download_streams WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, target.ID).Scan(&offset, &completed); err != nil {
		return err
	}
	if completed != 0 {
		return nil
	}
	for {
		current, err := m.chatTarget(target.ID)
		if err != nil {
			return err
		}
		if current.Status != ChatStatusScanning {
			return nil
		}
		minID := target.StartMessageID - 1
		if minID < 0 {
			minID = 0
		}
		result, err := client.API().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: target.inputPeer(), OffsetID: offset, Limit: 100, MinID: minID, MaxID: target.UpperMessageID + 1})
		if err != nil {
			return fmt.Errorf("读取会话回复索引: %w", err)
		}
		messages := searchMessages(result)
		if len(messages) == 0 {
			_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, target.ID)
			return err
		}
		nextOffset := offset
		candidates := make([]source, 0)
		seenGroups := make(map[int64]struct{})
		for _, raw := range messages {
			message, ok := raw.(*tg.Message)
			if !ok || message.ID < target.StartMessageID || message.ID > target.UpperMessageID {
				continue
			}
			if nextOffset == 0 || message.ID < nextOffset {
				nextOffset = message.ID
			}
			replies, hasReplies := message.GetReplies()
			if !hasReplies || replies.GetReplies() <= 0 {
				continue
			}
			groupID, grouped := message.GetGroupedID()
			if grouped {
				if _, done := seenGroups[groupID]; done {
					continue
				}
				seenGroups[groupID] = struct{}{}
			}
			members := []*tg.Message{message}
			if grouped {
				if expanded, groupErr := tutil.GetGroupedMessages(ctx, client.API(), target.inputPeer(), message); groupErr == nil && len(expanded) > 0 {
					members = expanded
				}
			}
			originID := firstMessageID(members, message.ID)
			if err := m.rememberChatDiscussionRoot(ctx, client.API(), target, message.ID, originID); err != nil {
				logRelatedWarning(target.AccountID, message.ID, err)
			}
			related, relatedErr := relatedSources(ctx, client.API(), target.AccountID, target.inputPeer(), target.DialogName, members, message.ID, originID)
			if relatedErr != nil {
				// A missing or inaccessible discussion never fails channel history.
				logRelatedWarning(target.AccountID, message.ID, relatedErr)
				continue
			}
			candidates = append(candidates, related...)
		}
		if err := m.registerChatMedia(target.ID, candidates, false); err != nil {
			return err
		}
		if nextOffset == 0 || nextOffset == offset {
			return errors.New("Telegram 回复索引未推进分页游标")
		}
		offset = nextOffset
		if _, err := m.db.Exec(`UPDATE chat_download_streams SET offset_message_id = ? WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, offset, target.ID); err != nil {
			return err
		}
		m.touch()
	}
}

func (m *Manager) rememberChatDiscussionRoot(ctx context.Context, api *tg.Client, target storedChatTarget, sourceMessageID, originMessageID int) error {
	if _, ok := target.inputPeer().(*tg.InputPeerChannel); !ok {
		return nil
	}
	peer, rootID, found, err := discussionThread(ctx, api, target.AccountID, target.inputPeer(), sourceMessageID, 0)
	if err != nil || !found {
		return err
	}
	_, key, _ := dialogIdentity(peer, target.AccountID)
	if key == "" {
		return nil
	}
	direct := makeDirectPeer(peer)
	_, err = m.db.Exec(`INSERT INTO chat_reply_roots(chat_job_id, account_id, discussion_dialog_key, root_message_id, origin_message_id, discussion_peer_type, discussion_peer_id, discussion_peer_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_job_id, discussion_dialog_key, root_message_id) DO UPDATE SET discussion_peer_type = EXCLUDED.discussion_peer_type, discussion_peer_id = EXCLUDED.discussion_peer_id, discussion_peer_hash = EXCLUDED.discussion_peer_hash`, target.ID, target.AccountID, key, rootID, originMessageID, direct.kind, direct.id, direct.hash)
	if err == nil {
		m.addChatWatched(target.AccountID, key)
	}
	return err
}

func chatStreamFilter(kind string) (tg.MessagesFilterClass, string, error) {
	switch kind {
	case "photo_video":
		return &tg.InputMessagesFilterPhotoVideo{}, "照片和视频", nil
	case "document":
		return &tg.InputMessagesFilterDocument{}, "文档", nil
	case "music":
		return &tg.InputMessagesFilterMusic{}, "音乐", nil
	case "round_voice":
		return &tg.InputMessagesFilterRoundVoice{}, "语音和圆形视频", nil
	default:
		return nil, "", fmt.Errorf("未知会话文件筛选器: %s", kind)
	}
}

func searchMessages(result tg.MessagesMessagesClass) []tg.MessageClass {
	switch value := result.(type) {
	case *tg.MessagesMessages:
		return value.Messages
	case *tg.MessagesMessagesSlice:
		return value.Messages
	case *tg.MessagesChannelMessages:
		return value.Messages
	default:
		return nil
	}
}

func (m *Manager) refreshChatStates() {
	// Only an actively draining history queue needs aggregation. Completed and
	// listening-only tasks are stable; a newly persisted listener event switches
	// its parent back to downloading in registerChatMedia.
	// Query one compact cached summary per active parent. Its trigger-maintained
	// counters cover all single-item and batch state transitions atomically.
	rows, err := m.db.Query(`SELECT j.id, j.listen_new, j.status,
	COALESCE(s.queued, 0) + COALESCE(s.running, 0) + COALESCE(s.downloaded, 0),
	COALESCE(s.failed, 0), COALESCE(s.queued, 0)
FROM chat_download_jobs j LEFT JOIN chat_download_stats s ON s.chat_job_id = j.id
WHERE j.status = ? AND j.scan_state = ?`, ChatStatusDownloading, chatScanCompleted)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, status string
		var listen int
		var active, failed, pending int
		if err := rows.Scan(&id, &listen, &status, &active, &failed, &pending); err != nil {
			continue
		}
		next := ChatStatusCompleted
		if active > 0 || pending > 0 {
			next = ChatStatusDownloading
		} else if listen != 0 {
			next = ChatStatusListening
		} else if failed > 0 {
			next = ChatStatusPartial
		}
		if next != status {
			_, _ = m.db.Exec(`UPDATE chat_download_jobs SET status = ?, updated_at = ? WHERE id = ?`, next, time.Now().UTC().Format(time.RFC3339Nano), id)
			m.touch()
		}
		if next == ChatStatusListening && failed > 0 {
			_, _ = m.db.Exec(`UPDATE chat_download_jobs SET error = ? WHERE id = ?`, fmt.Sprintf("历史下载有 %d 个文件失败，可重新开始失败项", failed), id)
		}
		if pending > 0 {
			m.signalChat()
		}
	}
}

// PauseChat stops both indexing and active transfers for this one visible
// task. Its global claims remain reserved while it is merely paused.
func (m *Manager) PauseChat(id string) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status != ChatStatusQueued && target.Status != ChatStatusScanning && target.Status != ChatStatusDownloading && target.Status != ChatStatusListening {
		return errors.New("当前会话任务不能暂停")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// The parent and media index transition together. The scanner and listener
	// observe the durable parent state, while a failed media update rolls the
	// whole operation back instead of leaving a half-paused task.
	if err := m.transitionChatItems(id, target.Status, ChatStatusPaused, "", `UPDATE chat_download_items SET status = 'paused', elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END, started_at = '' WHERE chat_job_id = ? AND status IN ('queued','running','downloaded')`, nil, now, id); err != nil {
		return err
	}
	m.cancelChatExecutions(id)
	return nil
}

func (m *Manager) ResumeChat(id string) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status != ChatStatusPaused {
		return errors.New("当前会话任务不能恢复")
	}
	next := ChatStatusQueued
	if target.ScanState == chatScanCompleted {
		next = ChatStatusDownloading
	}
	if err := m.transitionChatItems(id, ChatStatusPaused, next, "", `UPDATE chat_download_items SET status = 'queued', error = '', started_at = '', finished_at = '' WHERE chat_job_id = ? AND status = 'paused'`, nil, id); err != nil {
		return err
	}
	m.signalChat()
	m.signal()
	return nil
}

func (m *Manager) RetryChat(id string) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status != ChatStatusFailed && target.Status != ChatStatusPartial && target.Status != ChatStatusCancelled && target.Status != ChatStatusListening {
		return errors.New("当前会话任务不能重新开始")
	}
	next := ChatStatusQueued
	if target.ScanState == chatScanCompleted {
		next = ChatStatusDownloading
	}
	if err := m.transitionChatItems(id, target.Status, next, "", `UPDATE chat_download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE chat_job_id = ? AND status != 'completed'`, nil, id); err != nil {
		return err
	}
	m.signalChat()
	m.signal()
	return nil
}

func (m *Manager) CancelChat(id string) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status == ChatStatusCompleted || target.Status == ChatStatusFailed || target.Status == ChatStatusPartial || target.Status == ChatStatusCancelled {
		return errors.New("当前会话任务不能取消")
	}
	if err := m.transitionChatItems(id, target.Status, ChatStatusCancelled, "已取消，可重新开始", `UPDATE chat_download_items SET status = 'cancelled', finished_at = ? WHERE chat_job_id = ? AND status != 'completed'`, func(tx *databaseTx) error {
		_, err := tx.Exec(`DELETE FROM downloaded_media WHERE status = 'claimed' AND owner_kind = 'chat' AND owner_id = ?`, id)
		return err
	}, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return err
	}
	m.cancelChatExecutions(id)
	return nil
}

// SetChatListening changes only whether new messages are accepted after the
// historical range has been indexed. Enabling it never scans history again.
func (m *Manager) SetChatListening(id string, enabled bool) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.ScanState != chatScanCompleted {
		return errors.New("历史下载尚未完成索引，暂时不能修改消息监听")
	}
	if enabled {
		if target.ListenNew {
			return nil
		}
		if target.Status != ChatStatusCompleted && target.Status != ChatStatusPartial && target.Status != ChatStatusFailed {
			return errors.New("当前会话任务不能开启消息监听")
		}
		if _, err := m.db.Exec(`UPDATE chat_download_jobs SET listen_new = 1, status = ?, error = '', updated_at = ? WHERE id = ?`, ChatStatusListening, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
			return err
		}
	} else {
		if !target.ListenNew {
			return nil
		}
		next := target.Status
		if target.Status == ChatStatusListening {
			var failed int
			if err := m.db.QueryRow(`SELECT COALESCE(failed, 0) FROM chat_download_stats WHERE chat_job_id = ?`, id).Scan(&failed); err != nil {
				return err
			}
			if failed > 0 {
				next = ChatStatusPartial
			} else {
				next = ChatStatusCompleted
			}
		}
		if _, err := m.db.Exec(`UPDATE chat_download_jobs SET listen_new = 0, status = ?, error = '', updated_at = ? WHERE id = ?`, next, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
			return err
		}
	}
	m.touch()
	m.signalChat()
	return nil
}

// transitionChatItems makes a chat parent and its indexed media change state
// atomically. The optional after hook is used for ownership changes that must
// commit with cancellation.
func (m *Manager) transitionChatItems(id, expected, next, message, itemSQL string, after func(*databaseTx) error, args ...any) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, next, message, time.Now().UTC().Format(time.RFC3339Nano), id, expected)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("会话任务状态已变化，请刷新后重试")
	}
	if _, err := tx.Exec(itemSQL, args...); err != nil {
		return err
	}
	if after != nil {
		if err := after(tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	return nil
}

// updateChatStatus makes concurrent parent controls observable instead of
// silently succeeding after another request has already changed the task.
func (m *Manager) updateChatStatus(id, expected, next, message string) error {
	result, err := m.db.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, next, message, time.Now().UTC().Format(time.RFC3339Nano), id, expected)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("会话任务状态已变化，请刷新后重试")
	}
	return nil
}

func (m *Manager) DeleteChat(id string) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status != ChatStatusCompleted && target.Status != ChatStatusFailed && target.Status != ChatStatusPartial && target.Status != ChatStatusCancelled {
		return errors.New("请先取消或等待会话任务结束后再删除")
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE chat_download_jobs SET status = ?, error = '会话任务已删除', updated_at = ? WHERE id = ? AND status IN (?, ?, ?, ?)`, ChatStatusDeleted, time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusCompleted, ChatStatusFailed, ChatStatusPartial, ChatStatusCancelled)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("会话任务状态已变化，请刷新后重试")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.visibleChats.Add(-1)
	_ = os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", "chat-"+id))
	m.touch()
	return nil
}

// PurgeChat permanently removes a terminal chat task and its media index. It
// never removes final files; only claims exclusively owned by this task go.
func (m *Manager) PurgeChat(id string) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status != ChatStatusCompleted && target.Status != ChatStatusFailed && target.Status != ChatStatusPartial && target.Status != ChatStatusCancelled && target.Status != ChatStatusDeleted {
		return errors.New("请先取消或等待会话任务结束后再彻底删除")
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM chat_download_jobs WHERE id = ?`, id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if target.Status != ChatStatusDeleted {
		m.visibleChats.Add(-1)
	}
	_ = os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", "chat-"+id))
	m.touch()
	return nil
}
