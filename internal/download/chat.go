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
	"time"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

// ChatJob is a long-lived parent task for one channel or group. Its children
// are ordinary download jobs, intentionally hidden from the message task UI.
// This keeps the existing per-file download engine reusable while making the
// chat lifecycle and its media index independently manageable.
type ChatJob struct {
	ID             string `json:"id"`
	SourceURL      string `json:"sourceUrl"`
	DialogType     string `json:"dialogType"`
	DialogKey      string `json:"dialogKey"`
	DialogID       int64  `json:"dialogId"`
	DialogName     string `json:"dialogName"`
	AccountID      string `json:"accountId"`
	StartMessageID int    `json:"startMessageId"`
	UpperMessageID int    `json:"upperMessageId"`
	ListenNew      bool   `json:"listenNew"`
	Status         string `json:"status"`
	ScanState      string `json:"scanState"`
	Error          string `json:"error,omitempty"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
	Discovered     int    `json:"discovered"`
	Completed      int    `json:"completed"`
	Failed         int    `json:"failed"`
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
)

// Telegram exposes the media gallery as separate server-side filters. These
// four streams cover ordinary photos/videos, files, music, and the special
// voice/round-video class. Every result still enters the same message-ID
// index, so a server-side filter overlap can never create a duplicate file.
var chatStreamKinds = []string{"photo_video", "document", "music", "round_voice"}

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
	m.touch()
	return job, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// GetChat returns one parent task together with aggregate state from its
// child download jobs. Individual media rows are deliberately not loaded here.
func (m *Manager) GetChat(id string) (ChatJob, error) {
	var job ChatJob
	var listen int
	err := m.db.QueryRow(`SELECT c.id, c.source_url, c.dialog_type, c.dialog_key, c.dialog_id, c.dialog_name, c.account_id, c.start_message_id, c.upper_message_id, c.listen_new, c.status, c.scan_state, c.error, c.created_at, c.updated_at,
 COUNT(i.message_id), COALESCE(SUM(CASE WHEN d.status = 'completed' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN d.status IN ('failed', 'partial') THEN 1 ELSE 0 END), 0)
 FROM chat_download_jobs c LEFT JOIN chat_download_items i ON i.chat_job_id = c.id LEFT JOIN download_items d ON d.dialog_key = i.dialog_key AND d.message_id = i.message_id
 WHERE c.id = ? GROUP BY c.id`, id).Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogID, &job.DialogName, &job.AccountID, &job.StartMessageID, &job.UpperMessageID, &listen, &job.Status, &job.ScanState, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.Discovered, &job.Completed, &job.Failed)
	if errors.Is(err, sql.ErrNoRows) {
		return ChatJob{}, errors.New("会话下载任务不存在")
	}
	if err != nil {
		return ChatJob{}, err
	}
	job.ListenNew = listen != 0
	return job, nil
}

// ListChats uses the same opaque cursor approach as message downloads. The
// count stays cheap even after media details have grown very large.
func (m *Manager) ListChats(cursor string, pageSize int) ([]ChatJob, int, string, error) {
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}
	var total int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM chat_download_jobs WHERE status != ?`, ChatStatusDeleted).Scan(&total); err != nil {
		return nil, 0, "", err
	}
	where, args := "WHERE c.status != ?", []any{ChatStatusDeleted, pageSize + 1}
	if cursor != "" {
		createdAt, id, err := decodeChatCursor(cursor)
		if err != nil {
			return nil, 0, "", errors.New("分页游标无效，请返回第一页")
		}
		where += " AND (c.created_at < ? OR (c.created_at = ? AND c.id < ?))"
		args = []any{ChatStatusDeleted, createdAt, createdAt, id, pageSize + 1}
	}
	rows, err := m.db.Query(`SELECT c.id, c.source_url, c.dialog_type, c.dialog_key, c.dialog_id, c.dialog_name, c.account_id, c.start_message_id, c.upper_message_id, c.listen_new, c.status, c.scan_state, c.error, c.created_at, c.updated_at,
 COUNT(i.message_id), COALESCE(SUM(CASE WHEN d.status = 'completed' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN d.status IN ('failed', 'partial') THEN 1 ELSE 0 END), 0)
 FROM chat_download_jobs c LEFT JOIN chat_download_items i ON i.chat_job_id = c.id LEFT JOIN download_items d ON d.dialog_key = i.dialog_key AND d.message_id = i.message_id `+where+`
 GROUP BY c.id ORDER BY c.created_at DESC, c.id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, 0, "", err
	}
	defer rows.Close()
	jobs := make([]ChatJob, 0, pageSize+1)
	for rows.Next() {
		var job ChatJob
		var listen int
		if err := rows.Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogID, &job.DialogName, &job.AccountID, &job.StartMessageID, &job.UpperMessageID, &listen, &job.Status, &job.ScanState, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.Discovered, &job.Completed, &job.Failed); err != nil {
			return nil, 0, "", err
		}
		job.ListenNew = listen != 0
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

// ListChatsPage is a Bot presentation helper. The Web API deliberately uses
// keyset cursors; Bot pages are capped to small human-scale lists and are
// converted here without exposing opaque cursor state in callback payloads.
func (m *Manager) ListChatsPage(page, pageSize int) ([]ChatJob, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}
	var cursor string
	var total int
	for current := 1; current <= page; current++ {
		jobs, found, next, err := m.ListChats(cursor, pageSize)
		if err != nil {
			return nil, 0, err
		}
		total = found
		if current == page || next == "" {
			return jobs, total, nil
		}
		cursor = next
	}
	return nil, total, nil
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

// registerChatMedia atomically relates every discovered media message to its
// chat task. Existing standalone downloads are attached as satisfied records;
// only genuinely new messages are handed to the normal downloader in batches.
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
	var config settings.Download
	if target.configJSON != "" {
		if err := json.Unmarshal([]byte(target.configJSON), &config); err != nil {
			return fmt.Errorf("会话下载配置快照无效: %w", err)
		}
	}
	candidates = filterSources(candidates, config)
	if len(candidates) == 0 {
		return nil
	}
	newSources := make([]source, 0, len(candidates))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range candidates {
		var existingJob string
		err := tx.QueryRow(`SELECT job_id FROM download_items WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&existingJob)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		result, err := tx.Exec(`INSERT INTO chat_download_items(chat_job_id, dialog_key, message_id, child_job_id, dialog_type, dialog_id, grouped_id, message_text, original_name, size, discovered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_job_id, dialog_key, message_id) DO NOTHING`, chatID, item.DialogKey, item.MessageID, existingJob, item.DialogType, item.DialogID, item.GroupedID, item.MessageText, item.OriginalName, item.Size, now)
		if err != nil {
			return err
		}
		inserted, _ := result.RowsAffected()
		if existingJob == "" && startTransfers && inserted != 0 {
			newSources = append(newSources, item)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if len(newSources) == 0 {
		m.touch()
		return nil
	}
	if target.inputPeer() == nil {
		return errors.New("会话下载任务缺少 Telegram 会话引用")
	}
	// Bound batches keep upstream's iterator and resume metadata small while
	// avoiding one visible task per media file. Grouped albums are collapsed by
	// the scanner before reaching this point.
	for _, batch := range chatSourceBatches(newSources, 64) {
		if err := m.enqueueChatBatch(target, chatID, batch); err != nil {
			return err
		}
	}
	m.touch()
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
		m.refreshChatStates()
		m.reconcileChatListeners()
		select {
		case <-m.chatWake:
		case <-time.After(3 * time.Second):
		}
	}
}

func (m *Manager) reconcileChatListeners() {
	rows, err := m.db.Query(`SELECT DISTINCT account_id FROM chat_download_jobs WHERE listen_new = 1 AND scan_state = ? AND status IN (?, ?)`, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return
	}
	defer rows.Close()
	wanted := make(map[string]struct{})
	for rows.Next() {
		var accountID string
		if rows.Scan(&accountID) == nil && accountID != "" {
			wanted[accountID] = struct{}{}
		}
	}
	proxyURL := m.settings.ProxyURL()
	m.mu.Lock()
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

func (m *Manager) runChatListener(ctx context.Context, accountID string, listener *chatListener) {
	err := m.accounts.ListenNewMessages(ctx, accountID, func(_ context.Context, event telegram.NewMessageEvent) {
		select {
		case m.chatEvents <- event:
		default:
			// This should be exceptional (the consumer only resolves media in
			// explicitly selected chats). It is visible in logs rather than
			// blocking Telegram's update dispatcher indefinitely.
			applog.Error("chat_download", "new_message_queue_full", "account_id", event.AccountID, "message_id", event.MessageID)
		}
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
	for event := range m.chatEvents {
		m.handleNewChatMessage(event)
	}
}

func (m *Manager) handleNewChatMessage(event telegram.NewMessageEvent) {
	if event.InputPeer == nil || event.MessageID <= 0 {
		return
	}
	_, dialogKey, _ := dialogIdentity(event.InputPeer, event.AccountID)
	rows, err := m.db.Query(`SELECT id FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?)`, event.AccountID, dialogKey, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	sources, err := m.resolvePeer(context.Background(), event.AccountID, event.InputPeer, event.DialogID, event.MessageID, event.DialogName)
	if err != nil {
		applog.Info("chat_download", "new_message_not_downloadable", "account_id", event.AccountID, "message_id", event.MessageID, "error", err.Error())
		return
	}
	for _, id := range ids {
		if err := m.registerChatMedia(id, sources, true); err != nil {
			applog.Error("chat_download", "new_media_register_failed", "chat_job_id", id, "message_id", event.MessageID, "error", err.Error())
			continue
		}
		applog.Info("chat_download", "new_media_queued", "chat_job_id", id, "message_id", event.MessageID, "item_count", len(sources))
	}
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
	err = m.accounts.Run(context.Background(), target.AccountID, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
		for _, kind := range chatStreamKinds {
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
	if err := m.queueIndexedChatMedia(id); err != nil {
		_, _ = m.db.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?`, ChatStatusFailed, err.Error(), time.Now().UTC().Format(time.RFC3339Nano), id)
		m.touch()
		return err
	}
	_, err = m.db.Exec(`UPDATE chat_download_jobs SET scan_state = ?, status = ?, error = '', updated_at = ? WHERE id = ? AND status = ?`, chatScanCompleted, ChatStatusDownloading, time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning)
	if err == nil {
		m.touch()
	}
	return err
}

// queueIndexedChatMedia runs only after both media-only search streams have
// reached the frozen lower bound. Sorting by message ID makes a “from earliest”
// task actually transfer media from oldest to newest, while the durable index
// still allows an interrupted scan to resume safely.
func (m *Manager) queueIndexedChatMedia(chatID string) error {
	target, err := m.chatTarget(chatID)
	if err != nil {
		return err
	}
	rows, err := m.db.Query(`SELECT dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, original_name, size FROM chat_download_items WHERE chat_job_id = ? AND child_job_id = '' ORDER BY message_id ASC`, chatID)
	if err != nil {
		return err
	}
	defer rows.Close()
	candidates := make([]source, 0)
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.DialogType, &item.DialogKey, &item.DialogID, &item.MessageID, &item.GroupedID, &item.MessageText, &item.OriginalName, &item.Size); err != nil {
			return err
		}
		if item.OriginalName == "" {
			continue
		}
		candidates = append(candidates, source{Item: item, DialogName: target.DialogName})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, batch := range chatSourceBatches(candidates, 64) {
		if err := m.enqueueChatBatch(target, chatID, batch); err != nil {
			return err
		}
	}
	m.signal()
	return nil
}

// chatSourceBatches keeps every Telegram album in exactly one upstream
// invocation. The upstream downloader expands grouped media itself; splitting
// an album across two 64-item batches would otherwise request it twice.
func chatSourceBatches(items []source, limit int) [][]source {
	if limit < 1 {
		limit = 64
	}
	batches := make([][]source, 0, (len(items)+limit-1)/limit)
	current := make([]source, 0, limit)
	for index := 0; index < len(items); {
		end := index + 1
		if group := items[index].GroupedID; group != 0 {
			for end < len(items) && items[end].GroupedID == group {
				end++
			}
		}
		group := items[index:end]
		if len(current) > 0 && len(current)+len(group) > limit {
			batches = append(batches, current)
			current = make([]source, 0, limit)
		}
		current = append(current, group...)
		index = end
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

// enqueueChatBatch never silently drops the non-conflicting members of a
// batch when another entry point (Web, Bot or a reaction) wins one media
// identity concurrently. A duplicate batch is reconciled and any still
// unmapped media is retried individually, preserving global message de-dupe.
func (m *Manager) enqueueChatBatch(target storedChatTarget, chatID string, batch []source) error {
	if len(batch) == 0 {
		return nil
	}
	intent := DownloadIntent{Source: SourceAPI, AccountID: target.AccountID, URL: fmt.Sprintf("tg://chat/%s/%d", chatID, batch[0].MessageID)}
	submission, err := m.enqueueIntentParentSnapshot(intent, batch, target.direct, chatID, targetConfigJSON(target))
	if err != nil {
		return err
	}
	if submission.Created {
		return m.linkChatItems(chatID, submission.Job.ID, batch)
	}
	remaining := make([]source, 0, len(batch))
	for _, item := range batch {
		var jobID string
		err := m.db.QueryRow(`SELECT job_id FROM download_items WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&jobID)
		if err == nil && jobID != "" {
			if err := m.linkChatItems(chatID, jobID, []source{item}); err != nil {
				return err
			}
			continue
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		remaining = append(remaining, item)
	}
	for _, item := range remaining {
		oneIntent := DownloadIntent{Source: SourceAPI, AccountID: target.AccountID, URL: fmt.Sprintf("tg://chat/%s/%d", chatID, item.MessageID)}
		one, err := m.enqueueIntentParentSnapshot(oneIntent, []source{item}, target.direct, chatID, targetConfigJSON(target))
		if err != nil {
			return err
		}
		if one.Created {
			if err := m.linkChatItems(chatID, one.Job.ID, []source{item}); err != nil {
				return err
			}
			continue
		}
		var jobID string
		if err := m.db.QueryRow(`SELECT job_id FROM download_items WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&jobID); err != nil || jobID == "" {
			return errors.New("并发去重后无法关联会话媒体")
		}
		if err := m.linkChatItems(chatID, jobID, []source{item}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) linkChatItems(chatID, childID string, items []source) error {
	for _, item := range items {
		if _, err := m.db.Exec(`UPDATE chat_download_items SET child_job_id = ? WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ? AND child_job_id = ''`, childID, chatID, item.DialogKey, item.MessageID); err != nil {
			return err
		}
	}
	return nil
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
			return fmt.Errorf("搜索%s媒体: %w", label, err)
		}
		messages := searchMessages(result)
		if len(messages) == 0 {
			_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = ?`, target.ID, kind)
			return err
		}
		candidates := make([]source, 0, len(messages))
		nextOffset := offset
		for _, raw := range messages {
			message, ok := raw.(*tg.Message)
			if !ok || message.ID < target.StartMessageID || message.ID > target.UpperMessageID {
				continue
			}
			if nextOffset == 0 || message.ID < nextOffset {
				nextOffset = message.ID
			}
			media, ok := tmedia.GetMedia(message)
			if !ok {
				continue
			}
			groupedID, _ := message.GetGroupedID()
			candidates = append(candidates, source{Item: Item{DialogType: target.DialogType, DialogKey: target.DialogKey, DialogID: target.DialogID, MessageID: message.ID, GroupedID: groupedID, MessageText: message.Message, OriginalName: media.Name, Size: media.Size}, DialogName: target.DialogName, MediaType: messageMediaType(message)})
		}
		if err := m.registerChatMedia(target.ID, candidates, false); err != nil {
			return err
		}
		if nextOffset == 0 || nextOffset == offset {
			return errors.New("Telegram 媒体搜索未推进分页游标")
		}
		offset = nextOffset
		if _, err := m.db.Exec(`UPDATE chat_download_streams SET offset_message_id = ? WHERE chat_job_id = ? AND stream_kind = ?`, offset, target.ID, kind); err != nil {
			return err
		}
		m.touch()
	}
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
		return nil, "", fmt.Errorf("未知会话媒体筛选器: %s", kind)
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
	rows, err := m.db.Query(`SELECT id, listen_new, status, scan_state FROM chat_download_jobs WHERE status NOT IN (?, ?, ?, ?)`, ChatStatusPaused, ChatStatusCancelled, ChatStatusFailed, ChatStatusDeleted)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, status, scan string
		var listen int
		if err := rows.Scan(&id, &listen, &status, &scan); err != nil {
			continue
		}
		if scan != chatScanCompleted {
			continue
		}
		var active, failed int
		if err := m.db.QueryRow(`SELECT COALESCE(SUM(CASE WHEN d.status IN ('queued', 'running', 'paused') THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN d.status IN ('failed', 'partial', 'cancelled') THEN 1 ELSE 0 END), 0) FROM chat_download_items i LEFT JOIN download_items d ON d.dialog_key = i.dialog_key AND d.message_id = i.message_id WHERE i.chat_job_id = ?`, id).Scan(&active, &failed); err != nil {
			continue
		}
		next := ChatStatusCompleted
		if active > 0 {
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
	}
}

// PauseChat and the other parent controls always operate on the child jobs as
// well. A parent record is never merely a cosmetic wrapper: once paused or
// cancelled it prevents both further indexing and any queued media transfer.
func (m *Manager) PauseChat(id string) error {
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status != ChatStatusQueued && target.Status != ChatStatusScanning && target.Status != ChatStatusDownloading && target.Status != ChatStatusListening {
		return errors.New("当前会话任务不能暂停")
	}
	if err := m.updateChatStatus(id, target.Status, ChatStatusPaused, ""); err != nil {
		return err
	}
	if err := m.applyChatChildren(id, "暂停", func(child string) error {
		if status := m.status(child); status == "queued" || status == "running" {
			return m.Pause(child)
		}
		return nil
	}); err != nil {
		return err
	}
	m.touch()
	return nil
}

func (m *Manager) ResumeChat(id string) error {
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
	if err := m.updateChatStatus(id, ChatStatusPaused, next, ""); err != nil {
		return err
	}
	if err := m.applyChatChildren(id, "恢复", func(child string) error {
		if m.status(child) == "paused" {
			return m.Resume(child)
		}
		return nil
	}); err != nil {
		return err
	}
	m.touch()
	m.signalChat()
	m.signal()
	return nil
}

func (m *Manager) RetryChat(id string) error {
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
	if err := m.updateChatStatus(id, target.Status, next, ""); err != nil {
		return err
	}
	if err := m.applyChatChildren(id, "重新开始", func(child string) error {
		if status := m.status(child); status == "failed" || status == "partial" || status == "cancelled" {
			return m.Retry(child)
		}
		return nil
	}); err != nil {
		return err
	}
	m.touch()
	m.signalChat()
	m.signal()
	return nil
}

func (m *Manager) CancelChat(id string) error {
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if target.Status == ChatStatusCompleted || target.Status == ChatStatusFailed || target.Status == ChatStatusPartial || target.Status == ChatStatusCancelled {
		return errors.New("当前会话任务不能取消")
	}
	if err := m.updateChatStatus(id, target.Status, ChatStatusCancelled, "已取消，可重新开始"); err != nil {
		return err
	}
	if err := m.applyChatChildren(id, "取消", func(child string) error {
		if status := m.status(child); status == "queued" || status == "running" || status == "paused" {
			return m.Cancel(child)
		}
		return nil
	}); err != nil {
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

// applyChatChildren never discards a child-control failure. The parent state
// already prevents further indexing, and its error field records any child
// that needs a later retry instead of presenting a misleading clean result.
func (m *Manager) applyChatChildren(chatID, action string, apply func(string) error) error {
	var failures []error
	for _, child := range m.chatChildJobIDs(chatID) {
		if err := apply(child); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", child, err))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	message := fmt.Sprintf("会话已%s，但 %d 个子任务未完成对应操作", action, len(failures))
	if _, err := m.db.Exec(`UPDATE chat_download_jobs SET error = ?, updated_at = ? WHERE id = ?`, message, time.Now().UTC().Format(time.RFC3339Nano), chatID); err != nil {
		failures = append(failures, fmt.Errorf("记录会话任务错误: %w", err))
	}
	m.touch()
	return errors.Join(failures...)
}

func (m *Manager) DeleteChat(id string) error {
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
	// Overlapping chat ranges may deliberately share a child task. Only mark a
	// child deleted when no other visible parent references it; physical rows
	// remain permanent history and global media de-duplication evidence.
	children, err := chatExclusiveChildJobIDs(tx, []string{id})
	if err != nil {
		return err
	}
	for _, child := range children {
		if _, err := tx.Exec(`UPDATE download_jobs SET status = 'deleted', error = '所属会话任务已删除', updated_at = ? WHERE id = ? AND parent_chat_id = ? AND status != 'completed'`, time.Now().UTC().Format(time.RFC3339Nano), child, id); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE chat_download_jobs SET status = ?, error = '会话任务已删除', updated_at = ? WHERE id = ?`, ChatStatusDeleted, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, child := range children {
		_ = os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", child))
	}
	m.touch()
	return nil
}

// chatExclusiveChildJobIDs returns parent-owned child jobs that are not also
// referenced by another chat parent. Standalone jobs are excluded because
// they have no parent_chat_id.
func chatExclusiveChildJobIDs(tx *databaseTx, chatIDs []string) ([]string, error) {
	if len(chatIDs) == 0 {
		return nil, nil
	}
	marks := strings.TrimRight(strings.Repeat("?,", len(chatIDs)), ",")
	args := make([]any, 0, len(chatIDs)*2)
	for _, id := range chatIDs {
		args = append(args, id)
	}
	for _, id := range chatIDs {
		args = append(args, id)
	}
	rows, err := tx.Query(`SELECT DISTINCT j.id
 FROM download_jobs j
 WHERE j.parent_chat_id IN (`+marks+`)
   AND NOT EXISTS (
     SELECT 1 FROM chat_download_items i
     WHERE i.child_job_id = j.id AND i.chat_job_id NOT IN (`+marks+`)
   )`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (m *Manager) chatChildJobIDs(chatID string) []string {
	rows, err := m.db.Query(`SELECT id FROM download_jobs WHERE parent_chat_id = ?`, chatID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

// IsChatChild lets delivery adapters keep implementation-detail child jobs out
// of their normal task feeds. The chat parent is the sole user-visible unit.
func (m *Manager) IsChatChild(jobID string) bool {
	var parent string
	if err := m.db.QueryRow(`SELECT parent_chat_id FROM download_jobs WHERE id = ?`, jobID).Scan(&parent); err != nil {
		return false
	}
	return parent != ""
}
