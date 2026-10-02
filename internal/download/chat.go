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
	transfer "github.com/vacks/tdl/internal/download/transfer"
	"github.com/vacks/tdl/internal/tmedia"
	"github.com/vacks/tdl/internal/tmsg"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/kv"
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

const savedSourcePrefix = "tg://saved/"

// savedDialogKey is the dialog_key of an account's Saved Messages task. It has
// one definition because it has to agree with what dialogIdentity derives for
// the account's own peer: the listener matches an admitted message to the task
// that wants it by this key, so a second literal that drifted would silently
// stop saved messages being downloaded.
func savedDialogKey(accountID string) string { return "self:" + accountID }

func isSavedChat(job ChatJob) bool {
	return job.DialogType == "self" && strings.HasPrefix(job.SourceURL, savedSourcePrefix)
}

// isSavedListen reports whether a saved task is the one accepting new messages.
//
// The flag alone decides it. A saved task used to be one of two shapes - a
// history scan, or a listen-only task marked by a negative start - so listening
// was inferred from that marker. There is now at most one saved task per
// account, and the same task can be told to scan its history and to listen, so
// the marker no longer says anything about listening.
func isSavedListen(job ChatJob) bool {
	return isSavedChat(job) && job.ListenNew
}

// IsSavedListenForBot exposes only the task classification needed by the Bot
// control layer; task storage details remain internal to the downloader.
func IsSavedListenForBot(job ChatJob) bool { return isSavedListen(job) }

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
	// listenerGapBudget bounds one whole gap walk, and listenerGapPageTimeout one
	// history request inside it. The split matters: the walk persists its
	// position per page, so a long backlog is recoverable across attempts, while
	// a single request must not be allowed to hang the account lease.
	listenerGapBudget      = 5 * time.Minute
	listenerGapPageTimeout = 30 * time.Second
	// chatDownloadIdleInterval is the fallback only. Every transition that
	// queues media signals this worker class directly, and a filled claim
	// window continues without waiting at all, so this bounds how long a missed
	// wake can stall a transfer rather than how fast a queue drains.
	chatDownloadIdleInterval = 3 * time.Second
	// chatEventIdleInterval is likewise a fallback. Registering a listener
	// event signals chatEventWake, and the empty-queue probe is a single index
	// lookup, so this only bounds the cost of idle polling.
	chatEventIdleInterval = 3 * time.Second
)

// Telegram exposes the media gallery as separate server-side filters. These
// four streams cover ordinary photos/videos, files, music, and the special
// voice/round-video class. Every result still enters the same message-ID
// index, so a server-side filter overlap can never create a duplicate file.
// Keep ordinary media streams first: each stream registers files page by page
// and the downloader can begin immediately. Reply metadata is intentionally
// last so a very large text history never blocks the first media downloads.
var chatStreamKinds = []string{"photo_video", "document", "music", "round_voice", "reply_candidates"}

// chatStreamEnabled keeps the server-side index plan aligned with the
// immutable download-policy snapshot stored on the chat task. Telegram's
// gallery filters are broader than our semantic file types, so an enabled
// stream may still yield items that filterSources rejects; the reverse must
// never happen, otherwise an allowed file could be missed.
func chatStreamEnabled(kind string, config settings.Download) bool {
	allowed := make(map[string]struct{}, len(config.FileTypes))
	for _, fileType := range config.FileTypes {
		allowed[fileType] = struct{}{}
	}
	has := func(types ...string) bool {
		for _, fileType := range types {
			if _, ok := allowed[fileType]; ok {
				return true
			}
		}
		return false
	}
	switch kind {
	case "photo_video":
		return has("image", "video")
	case "document":
		return has("document", "sticker", "gif")
	case "music":
		return has("music")
	case "round_voice":
		// Telegram keeps round videos in this special gallery even though
		// their semantic type is video.
		return has("voice", "video")
	case "reply_candidates":
		// With no eligible types there is nothing to index or listen for.
		return config.IncludeReplies && len(allowed) > 0
	default:
		return false
	}
}

func chatDownloadConfig(target storedChatTarget) (settings.Download, error) {
	config := settings.Defaults().Download
	if target.configJSON == "" {
		return config, nil
	}
	if err := json.Unmarshal([]byte(target.configJSON), &config); err != nil {
		// The snapshot a task was created with is read on every pass and never
		// rewritten, so a snapshot that does not parse will not parse on the
		// next attempt either. Marked permanent so the listener inbox stops on
		// the first one: a task in this state used to fail every single new
		// message of its dialog five times fast and then hourly to twenty, each
		// pass a Telegram lookup, and each message its own failed event.
		return settings.Download{}, permanentFailure(fmt.Errorf("会话下载配置快照无效: %w", err))
	}
	return config, nil
}

// createChatJob persists only a resolved and bounded chat target. Discovery
// workers are attached in a later layer; keeping creation transactional lets
// callers safely retry an interrupted HTTP/Bot request without partial rows.
func (m *Manager) createChatJob(job ChatJob, direct directPeer, configJSON string) (ChatJob, error) {
	if job.DialogType != "channel" && job.DialogType != "chat" && job.DialogType != "self" {
		return ChatJob{}, errors.New("会话下载仅支持频道和群组")
	}
	if job.DialogType == "self" && !isSavedChat(job) {
		return ChatJob{}, errors.New("收藏夹任务标识无效")
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
	if job.Status == "" {
		job.Status = ChatStatusQueued
	}
	if job.ScanState == "" {
		job.ScanState = chatScanPending
	}
	job.ID, job.CreatedAt, job.UpdatedAt = id, now, now
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
	// updated_at is deliberately left to the triggers. One column written by
	// both Go's RFC3339Nano and PostgreSQL's now()::text holds two different
	// textual layouts, which no comparison can order reliably; nothing reads
	// this column, and leaving it to a single writer keeps that true.
	if _, err := tx.Exec(`INSERT INTO chat_download_stats(chat_job_id) VALUES (?)`, job.ID); err != nil {
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
	m.markChatListenerDirty()
	return job, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// ErrChatJobNotFound is the sibling of ErrJobNotFound for session tasks. The
// same distinction matters: a card whose task is gone must be dropped, while one
// the database could not answer for must be retried rather than quietly
// forgotten.
var ErrChatJobNotFound = errors.New("会话下载任务不存在")

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
		return ChatJob{}, ErrChatJobNotFound
	}
	if err != nil {
		return ChatJob{}, err
	}
	job.ListenNew = listen != 0
	job.ActiveFiles, job.SpeedBPS = m.progress.Aggregate(job.ID)
	return job, nil
}

// FindSavedListener returns the account's saved task when it is the one
// accepting new messages.
func (m *Manager) FindSavedListener(accountID string) (ChatJob, bool, error) {
	var id string
	err := m.db.QueryRow(`SELECT id FROM chat_download_jobs WHERE account_id = ? AND dialog_type = 'self' AND listen_new = 1 AND status IN (?, ?, ?, ?, ?) ORDER BY updated_at DESC LIMIT 1`, accountID, ChatStatusQueued, ChatStatusScanning, ChatStatusDownloading, ChatStatusListening, ChatStatusPaused).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ChatJob{}, false, nil
	}
	if err != nil {
		return ChatJob{}, false, err
	}
	job, err := m.GetChat(id)
	return job, err == nil, err
}

// savedChatFilterPrefix marks the ListChats filter that narrows the list to one
// account's Saved Messages task. See SavedChatsFilter.
const savedChatFilterPrefix = "saved:"

// SavedChatsFilter restricts ListChats to the single Saved Messages task of one
// account.
//
// It is a filter rather than a second listing function because the two lists
// render identically - the same card, the same cursor, the same counts - and a
// saved task is an ordinary session task that happens to have the account's own
// dialog as its target.
func SavedChatsFilter(accountID string) string { return savedChatFilterPrefix + accountID }

// ListChats uses the same opaque cursor approach as message downloads. The
// count stays cheap even after media details have grown very large.
func (m *Manager) ListChats(cursor string, pageSize int, filters ...string) ([]ChatJob, int, string, error) {
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}
	where, args := "WHERE c.status != ?", []any{ChatStatusDeleted}
	total := m.visibleChatCount()
	if len(filters) > 0 && strings.HasPrefix(filters[0], savedChatFilterPrefix) {
		accountID := strings.TrimPrefix(filters[0], savedChatFilterPrefix)
		where += " AND c.dialog_type = 'self' AND c.account_id = ?"
		args = append(args, accountID)
		// The cached counter covers every session task. This one is a point
		// lookup on the partial index over one account's saved tasks, so it is
		// both cheaper than the counter and the only count that matches the rows.
		if err := m.db.QueryRow(`SELECT COUNT(1) FROM chat_download_jobs WHERE account_id = ? AND dialog_type = 'self' AND status != ?`, accountID, ChatStatusDeleted).Scan(&total); err != nil {
			return nil, 0, "", err
		}
	}
	if cursor != "" {
		createdAt, id, err := decodeChatCursor(cursor)
		if err != nil {
			return nil, 0, "", errors.New("分页游标无效，请返回第一页")
		}
		where += keysetAfter("c")
		args = append(args, createdAt, id)
	}
	args = append(args, pageSize+1)
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
	direct directPeer
	// discussion is the channel's linked discussion group: the dialog a comment
	// update arrives from, and the only dialog besides this task's own that the
	// listener admits. discussionKey is empty until the link has been learned,
	// either from a post's resolved thread or from one linked-chat lookup.
	discussion    directPeer
	discussionKey string
	configJSON    string
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

// errChatTaskGone reports that a session task row is no longer there.
var errChatTaskGone = errors.New("会话下载任务不存在")

func (m *Manager) chatTarget(id string) (storedChatTarget, error) {
	var target storedChatTarget
	var listen int
	err := m.db.QueryRow(`SELECT id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, start_message_id, upper_message_id, listen_new, status, scan_state, error, created_at, updated_at, direct_peer_type, direct_peer_id, direct_peer_hash, config_json, discussion_dialog_key, discussion_peer_type, discussion_peer_id, discussion_peer_hash FROM chat_download_jobs WHERE id = ?`, id).Scan(&target.ID, &target.SourceURL, &target.DialogType, &target.DialogKey, &target.DialogID, &target.DialogName, &target.AccountID, &target.StartMessageID, &target.UpperMessageID, &listen, &target.Status, &target.ScanState, &target.Error, &target.CreatedAt, &target.UpdatedAt, &target.direct.kind, &target.direct.id, &target.direct.hash, &target.configJSON, &target.discussionKey, &target.discussion.kind, &target.discussion.id, &target.discussion.hash)
	if errors.Is(err, sql.ErrNoRows) {
		// The task this work belonged to was deleted between being admitted and
		// being handled. That is an answer, not a fault - there is nothing left
		// to download - so a listener event that lands here settles as skipped
		// instead of being retried against a row that is not coming back.
		return storedChatTarget{}, nothingToDo(errChatTaskGone)
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
	case "self":
		return &tg.InputPeerSelf{}
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
	config, err := chatDownloadConfig(target)
	if err != nil {
		return err
	}
	hasReplyRoots := false
	for _, item := range candidates {
		if item.IsComment && item.ReplyRootID > 0 && item.DialogKey != "" {
			hasReplyRoots = true
			break
		}
	}
	// The overwhelmingly common historical-media path has no discussion root.
	// Filter it before opening a transaction so a disabled subtype does not
	// produce a PostgreSQL round trip for every indexed page.
	if !hasReplyRoots {
		candidates = filterSources(candidates, config)
		if len(candidates) == 0 {
			return nil
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	inserted := int64(0)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Persist the discussion-root map before applying the file policy. A thread
	// with only currently filtered files can later receive an allowed comment;
	// that event must still be associated with this listening task.
	for _, item := range candidates {
		if !item.IsComment || item.ReplyRootID <= 0 || item.DialogKey == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO chat_reply_roots(chat_job_id, account_id, discussion_dialog_key, root_message_id, origin_message_id) VALUES (?, ?, ?, ?, ?) ON CONFLICT(chat_job_id, discussion_dialog_key, root_message_id) DO NOTHING`, chatID, target.AccountID, item.DialogKey, item.ReplyRootID, item.OriginMessageID); err != nil {
			return err
		}
	}
	if hasReplyRoots {
		candidates = filterSources(candidates, config)
		if len(candidates) == 0 {
			if err := tx.Commit(); err != nil {
				return err
			}
			return nil
		}
	}
	// A message task writes its file rows when the link is submitted and claims
	// the media when it first runs, so there is a window in which this media has
	// a download_items row and no ownership row. Claiming it here would have both
	// tasks transfer the same file; the implementation this replaces waited, and
	// so does this. A completed message task whose file is still on disk is not
	// waited for - it is adopted, by writing the ownership record the message
	// task would have written had it finished under the current rules.
	//
	// Both questions are asked once per page. The per-item form asked them once
	// per candidate, inside the ingest transaction, on the path whose whole job
	// is to walk a channel that may hold a million posts.
	deferred := make(map[mediaClaimKey]struct{})
	adopted := make(map[mediaClaimKey]string)
	if err := forEachKeyChunk(mediaKeys(candidates), func(chunk []mediaClaimKey) error {
		observed, err := messageTaskRows(tx, chunk)
		if err != nil {
			return err
		}
		for key, row := range observed {
			if row.status == "completed" && row.path != "" && regularFileExists(row.path) {
				adopted[key] = row.path
				continue
			}
			deferred[key] = struct{}{}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := adoptMessageTaskFiles(tx, adopted, now); err != nil {
		return err
	}
	claimable := make([]source, 0, len(candidates))
	for _, item := range candidates {
		if _, waiting := deferred[mediaClaimKey{dialogKey: item.DialogKey, messageID: item.MessageID}]; waiting {
			continue
		}
		claimable = append(claimable, item)
	}
	claims, err := m.claimMediaBatch(tx, "chat", chatID, claimable)
	if err != nil {
		return err
	}
	for _, item := range candidates {
		key := mediaClaimKey{dialogKey: item.DialogKey, messageID: item.MessageID}
		status, finalPath := "queued", ""
		claim, claimed := claims[key]
		switch {
		case adopted[key] != "":
			status, finalPath = "completed", adopted[key]
		case claimed:
			status, finalPath = claim.state, claim.path
		default:
			// No ownership row is written for a file another task is working on,
			// and this is the only verdict that leaves the row alone.
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

// messageTaskRow is what a message task's own file table says about one media
// identity, as far as the ingest path has to care.
type messageTaskRow struct {
	status string
	path   string
}

// messageTaskRows reads the message task's file rows for a chunk of identities,
// in one statement.
//
// It exists because a message task's claim is written when it first runs rather
// than when it is submitted, so its rows are visible here before its claim is.
// The other task is treated as an owner until it reaches a terminal state.
func messageTaskRows(q querier, keys []mediaClaimKey) (map[mediaClaimKey]messageTaskRow, error) {
	list, args := tupleInList(keys)
	rows, err := q.Query(`SELECT dialog_key, message_id, status, final_path FROM download_items WHERE (dialog_key, message_id) IN (`+list+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	observed := make(map[mediaClaimKey]messageTaskRow, len(keys))
	for rows.Next() {
		var key mediaClaimKey
		var row messageTaskRow
		if err := rows.Scan(&key.dialogKey, &key.messageID, &row.status, &row.path); err != nil {
			return nil, err
		}
		observed[key] = row
	}
	return observed, rows.Err()
}

// adoptMessageTaskFiles records the ownership a finished message task never
// wrote, so that every later reader learns the file is already here instead of
// downloading it a second time.
func adoptMessageTaskFiles(q querier, adopted map[mediaClaimKey]string, now string) error {
	if len(adopted) == 0 {
		return nil
	}
	keys := make([]mediaClaimKey, 0, len(adopted))
	for key := range adopted {
		keys = append(keys, key)
	}
	return forEachKeyChunk(keys, func(chunk []mediaClaimKey) error {
		values := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*3)
		for _, key := range chunk {
			values = append(values, "(?,?,?)")
			args = append(args, key.dialogKey, key.messageID, adopted[key])
		}
		args = append(args, now)
		_, err := q.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at)
 SELECT v.dialog_key, v.message_id, v.final_path, 'completed', 'message', '', ? FROM (VALUES `+strings.Join(values, ",")+`) AS v(dialog_key, message_id, final_path)
 ON CONFLICT(dialog_key, message_id) DO NOTHING`, args...)
		return err
	})
}

// chatWorker is deliberately single-threaded. Telegram's search pagination is
// inexpensive because it asks only for media, and serializing it keeps API
// pressure predictable even when a user creates many large chat tasks.
func (m *Manager) chatWorker() {
	// The waiting-item rotation is owned by this single goroutine.
	var claimCursor chatClaimCursor
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
		if err := m.reconcileChatPublishedItems(nil); err != nil {
			applog.Error("chat_download", "published_file_reconcile_failed", "error", err.Error())
		}
		claimCursor = m.reconcileChatClaims(claimCursor)
		m.refreshChatStates()
		m.reconcileChatListeners()
		// Neither inbox has a maintenance loop of its own, so their lease safety
		// net rides along here. It is self-throttled to stay off the hot path.
		m.runMaintenanceSweepsIfDue()
		select {
		case <-m.stopCh:
			return
		case <-m.chatWake:
		case <-time.After(3 * time.Second):
		}
	}
}

// chatDownloadWorker owns one private wake channel. Chat transfers may run
// concurrently up to the configured global limit, so several of these workers
// exist; sharing a single token between them and the indexing loop would let
// either class consume the other's wakeup.
func (m *Manager) chatDownloadWorker(wake <-chan struct{}) {
	for {
		if m.DatabaseAvailable() {
			more, err := m.runOneChatBatch()
			if err != nil {
				applog.Error("chat_download", "media_transfer_failed", "error", err.Error())
			}
			if more {
				// The claim window filled, so more media is already indexed
				// behind this batch. Start the next one immediately instead of
				// idling out the fallback interval; the batch itself is the
				// rate limit. Stop is still checked so a long queue cannot
				// delay shutdown.
				select {
				case <-m.stopCh:
					return
				default:
				}
				continue
			}
		}
		select {
		case <-m.stopCh:
			return
		case <-wake:
		case <-time.After(chatDownloadIdleInterval):
		}
	}
}

// reconcileChatClaims wakes media that was held by another active task. A
// completed claim is adopted without another download; a released claim is
// atomically acquired on the next pass.
// chatClaimBatchSize bounds one waiting-item reconcile pass.
const chatClaimBatchSize = 256

// chatClaimCursor is the composite key of the last waiting row a pass examined.
// chat_download_items has no surrogate id, so its primary key is the rotation
// key. It lives only in the caller's memory: losing it restarts the rotation,
// which costs nothing but a re-read of rows already known to be blocked.
type chatClaimCursor struct {
	chatJobID string
	dialogKey string
	messageID int
}

// reconcileChatClaims makes one bounded pass over waiting chat items and returns
// the cursor for the next pass. Two things it must do, and previously did not:
//
//   - Rotate. The scan is ordered by the primary key and resumes after the last
//     row it saw, so a permanently blocked head cannot keep the whole LIMIT
//     window to itself while everything behind it goes unexamined.
//   - Refuse to grant a claim on behalf of a task that is not runnable. A paused
//     or cancelled task transfers nothing, so a fresh claim taken for it is held
//     by nobody and silently blocks every message task waiting on that media —
//     the same stall that releasing claims on pause fixes from the other side.
//
// Adopting media another task already published is always safe and stays
// unconditional.
func (m *Manager) reconcileChatClaims(cursor chatClaimCursor) chatClaimCursor {
	// the ownership columns are coalesced, not read raw: the LEFT JOIN yields NULL
	// when nothing owns the media, and scanning NULL into a string fails, which
	// silently dropped exactly the rows this pass exists to promote — an item
	// whose blocker has released its claim. An empty status is the "unowned" case
	// the logic below already handles.
	// Skip an item whose task cannot run and whose media no message task could
	// have finished. Such a row can only ever be re-read and refused: promotion
	// is gated on the task being runnable, and the remaining branch needs a
	// message task to own the media. A task that ended with many of them kept
	// them in the scan forever, taking a permanent share of this reconciler and
	// of the database, and crowding out the batch window that items which can
	// actually move are waiting on.
	//
	// A task that cannot run yet whose media a message task owns is kept,
	// because there the item is adopted straight to completed rather than
	// promoted, and that is worth doing whatever the task's own state is.
	rows, err := m.db.Query(`SELECT i.chat_job_id, i.dialog_key, i.message_id, COALESCE(m.status, ''), COALESCE(m.final_path, ''), j.status FROM chat_download_items i LEFT JOIN downloaded_media m ON m.dialog_key = i.dialog_key AND m.message_id = i.message_id JOIN chat_download_jobs j ON j.id = i.chat_job_id WHERE i.status = 'waiting' AND (j.status IN (?, ?, ?) OR m.owner_kind = 'message') AND (i.chat_job_id, i.dialog_key, i.message_id) > (?, ?, ?) ORDER BY i.chat_job_id, i.dialog_key, i.message_id LIMIT ?`, ChatStatusScanning, ChatStatusDownloading, ChatStatusListening, cursor.chatJobID, cursor.dialogKey, cursor.messageID, chatClaimBatchSize)
	if err != nil {
		return cursor
	}
	defer rows.Close()
	scanned := 0
	next := cursor
	for rows.Next() {
		var chatID, key, status, path, parentStatus string
		var messageID int
		if rows.Scan(&chatID, &key, &messageID, &status, &path, &parentStatus) != nil {
			continue
		}
		scanned++
		next = chatClaimCursor{chatJobID: chatID, dialogKey: key, messageID: messageID}
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
			// Only a message task that is still working on this file keeps it.
			// Its own status is the question here, and the states listed are the
			// ones that mean work is in progress or already finished.
			//
			// Paused is deliberately not among them. A paused task has already
			// released its claim in downloaded_media for exactly this reason, so
			// refusing to take the file over left the session task waiting on a
			// task that transfers nothing: this pass is the only thing that can
			// return a waiting item to the queue, and the message-side promotion
			// pass cannot help either, because the claim it would test is gone.
			// The observable result was a session task pinned at "downloading"
			// for as long as the message task stayed paused, with no error and
			// nothing for the user to act on.
			//
			// Taking the file over cannot download it twice. Whoever reaches the
			// transfer first holds the claim; the other side sees it and waits, or
			// - if the transfer already finished - adopts the published file
			// instead of fetching it again. A resumed message task therefore still
			// completes, it just finds the file already done.
			if messageStatus == "queued" || messageStatus == "running" || messageStatus == "downloaded" || messageStatus == "completed" {
				continue
			}
		}
		if messageErr != nil && !errors.Is(messageErr, sql.ErrNoRows) {
			continue
		}
		// Only a task that can actually transfer may take ownership. A paused or
		// cancelled task transfers nothing, so a claim granted on its behalf would
		// be held by nobody and block every message task waiting on this media.
		if !chatRunnable(parentStatus) {
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
	if err := rows.Err(); err != nil {
		return next
	}
	// A short pass means the rotation reached the end of the waiting set.
	if scanned < chatClaimBatchSize {
		next = chatClaimCursor{}
	}
	return next
}

// claimChatMedia validates a queued media row immediately before transferring
// it. Retried rows reacquire their claim; completed rows are adopted instead
// of being downloaded a second time.
func (m *Manager) claimChatMedia(chatID string, item source) (string, string, error) {
	return m.claimMedia("chat", chatID, item)
}

// markChatItemsWaiting parks a whole claim window whose media another task
// owns, in one statement. The rows all belong to this session task, so the
// composite primary key answers the row-value lookup.
func (m *Manager) markChatItemsWaiting(chatID string, items []source) error {
	return forEachKeyChunk(mediaKeys(items), func(chunk []mediaClaimKey) error {
		list, args := tupleInList(chunk)
		_, err := m.db.Exec(`UPDATE chat_download_items SET status = 'waiting', error = '' WHERE chat_job_id = ? AND status = 'queued' AND (dialog_key, message_id) IN (`+list+`)`, append([]any{chatID}, args...)...)
		return err
	})
}

// beginChatItemAttempts records one transfer attempt for a whole window, in one
// statement. The status guard carries the same meaning it does in
// beginItemAttempts: a pause or cancel that landed while the window was being
// assembled must not be undone by attempt bookkeeping.
func (m *Manager) beginChatItemAttempts(chatID string, items []source) error {
	return forEachKeyChunk(mediaKeys(items), func(chunk []mediaClaimKey) error {
		list, args := tupleInList(chunk)
		_, err := m.db.Exec(`UPDATE chat_download_items SET attempts = attempts + 1, error = '', started_at = '', finished_at = '' WHERE chat_job_id = ? AND status = 'queued' AND (dialog_key, message_id) IN (`+list+`)`, append([]any{chatID}, args...)...)
		return err
	})
}

// runOneChatBatch claims only a bounded slice of the media index. New media
// is made eligible as soon as it is indexed; within that visible queue, newer
// message IDs always take precedence. There is no per-media download task:
// the chat job owns the context, temporary directory and all durable item
// state.
//
// more reports that the claim window was full and this call moved at least one
// item forward, so the caller should start the next batch without waiting out
// its idle fallback. Leaving that to the fallback would add a full idle
// interval between batches, which is minutes of dead time on a ten-thousand
// file task. The batch itself is the rate limit, and a full window that only
// produced "waiting" items is excluded because those are held by other tasks:
// re-claiming them immediately would spin without transferring anything.
func (m *Manager) runOneChatBatch() (more bool, err error) {
	// claimedWindow counts the rows the bounded claim returned, before the
	// dialog filter narrows what a single upstream call may transfer. adopted
	// counts items this call moved straight to completed by adopting media an
	// earlier task had already fetched.
	claimedWindow := 0
	adopted := 0
	toTransfer := make([]source, 0)
	defer func() {
		more = err == nil && claimedWindow == chatBatchSize && adopted+len(toTransfer) > 0
	}()
	// A message task created from a link, Bot command or reaction is interactive
	// work. While any such task is ready, the shared scheduler caps chat batches
	// at half of global capacity. Existing batches are allowed to finish safely;
	// each is bounded and then releases its global permit.
	priority, err := m.hasPriorityMessageTask()
	if err != nil {
		return false, err
	}
	// Accounts inside a Telegram cooldown are excluded by the query, not only by
	// the in-memory guard below: the candidate window is bounded, so filtering
	// only in Go would let sixteen blocked tasks at the head hide every other
	// account's work. blocked_until is compared as a timestamptz because the
	// stored text uses RFC3339Nano, whose trailing-zero trimming makes
	// lexicographic comparison wrong.
	rows, err := m.db.Query(`SELECT id, account_id FROM chat_download_jobs WHERE status IN ('scanning', 'downloading', 'listening') AND EXISTS (SELECT 1 FROM chat_download_items i WHERE i.chat_job_id = chat_download_jobs.id AND i.status = 'queued') AND NOT EXISTS (SELECT 1 FROM telegram_rate_limits l WHERE l.account_id = chat_download_jobs.account_id AND l.blocked_until::timestamptz > ?::timestamptz) ORDER BY created_at LIMIT 16`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	var id string
	for rows.Next() {
		var candidate, accountID string
		if rows.Scan(&candidate, &accountID) == nil {
			// An account inside its cooldown window must not be selected: the
			// batch would open a Telegram client only to be told to wait again,
			// and every such attempt extends the account-wide cooldown.
			if m.telegramAccountBlocked(accountID) {
				continue
			}
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
		return false, nil
	}
	defer func() { m.mu.Lock(); delete(m.chatActive, id); m.mu.Unlock() }()
	target, err := m.chatTarget(id)
	if err != nil {
		return false, err
	}
	releaseTransfer, acquired := m.tryAcquireTransfer(transferChat, priority)
	if !acquired {
		// Keep the durable media rows queued. Another worker will retry after a
		// permit is released, without creating a competing upstream transfer.
		return false, nil
	}
	defer releaseTransfer()
	if target.inputPeer() == nil {
		reason := "会话下载任务缺少 Telegram 会话引用"
		m.failChatTransfer(id, reason)
		return false, errors.New(reason)
	}
	rows, err = m.db.Query(`SELECT dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, origin_dialog_name, origin_message_id, is_comment, source_peer_type, source_peer_id, source_peer_hash, original_name, size FROM chat_download_items WHERE chat_job_id = ? AND status = 'queued' ORDER BY message_id DESC LIMIT ?`, id, chatBatchSize)
	if err != nil {
		return false, err
	}
	batch := make([]source, 0, chatBatchSize)
	for rows.Next() {
		var item Item
		var isComment int
		if err := rows.Scan(&item.DialogType, &item.DialogKey, &item.DialogID, &item.MessageID, &item.GroupedID, &item.MessageText, &item.OriginDialogName, &item.OriginMessageID, &isComment, &item.SourcePeerType, &item.SourcePeerID, &item.SourcePeerHash, &item.OriginalName, &item.Size); err != nil {
			rows.Close()
			return false, err
		}
		item.IsComment = isComment != 0
		batch = append(batch, source{Item: item, DialogName: target.DialogName, Direct: directPeer{kind: item.SourcePeerType, id: item.SourcePeerID, hash: item.SourcePeerHash}})
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	claimedWindow = len(batch)
	if len(batch) == 0 {
		return false, nil
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
	// Claiming a file and confirming this task may still run share one critical
	// section with pause and cancel, and the section deliberately spans the claim
	// loop rather than starting after it.
	//
	// A control action releases every claim the task holds. A claim taken after
	// that release belongs to a task that will transfer nothing, and nothing else
	// ever releases it: pause and cancel release only what exists when they run,
	// and the claim is not failed, cancelled or deleted, so the repair paths that
	// look for those states skip it. Every other task wanting that media then
	// waits behind it indefinitely. Checking the state before claiming cannot
	// close this, because the release can land between the check and the claim;
	// holding the lock across both is what makes the pair atomic.
	//
	// The lock is per chat, so this only delays that task's own control actions
	// and publishes. "A bounded batch is a short hold" used to be the argument
	// for that, and it was not true: the window was bounded in files but each
	// file cost its own transaction, so a full batch held the lock across
	// hundreds of round trips and the task's own pause queued behind them. The
	// three set-based statements below are what makes the bound mean something.
	lock := m.chatLock(id)
	lock.Lock()
	if !m.chatRunningOrUnknown(id) {
		lock.Unlock()
		return false, nil
	}
	claims, err := m.claimMediaBatch(m.db, "chat", id, batch)
	if err != nil {
		lock.Unlock()
		return false, err
	}
	toTransfer = make([]source, 0, len(batch))
	waiting := make([]source, 0, len(batch))
	for _, item := range batch {
		claim := claims[mediaClaimKey{dialogKey: item.DialogKey, messageID: item.MessageID}]
		switch claim.state {
		case "completed":
			if err := m.setChatItem(id, item, "completed", claim.path, ""); err != nil {
				lock.Unlock()
				return false, err
			}
			adopted++
		case "waiting":
			waiting = append(waiting, item)
		default:
			toTransfer = append(toTransfer, item)
		}
	}
	if len(waiting) > 0 {
		if err := m.markChatItemsWaiting(id, waiting); err != nil {
			lock.Unlock()
			return false, err
		}
	}
	batch = toTransfer
	if len(batch) == 0 {
		lock.Unlock()
		return false, nil
	}
	if err := m.beginChatItemAttempts(id, batch); err != nil {
		lock.Unlock()
		return false, err
	}
	// Register this transfer while still holding the same lock as pause/cancel.
	// This closes the remaining window where a control action could finish before
	// this batch had published its cancel function, leaving a newly-started
	// upstream transfer outside that action's reach.
	ctx, release := m.beginChatExecution(id)
	lock.Unlock()
	defer func() { release(); m.progress.ClearJob(id) }()
	transferCtx, stopTransfer := context.WithCancel(ctx)
	defer stopTransfer()
	config := m.settings.Get()
	if target.configJSON != "" {
		if err := json.Unmarshal([]byte(target.configJSON), &config.Download); err != nil {
			reason := fmt.Sprintf("会话下载配置快照无效: %v", err)
			m.failChatTransfer(id, reason)
			return false, errors.New(reason)
		}
	}
	byMessage := make(map[int]source, len(batch))
	ids := make([]int, 0, len(batch))
	seenMessages := map[int]struct{}{}
	var outcomesMu sync.Mutex
	var outcomes []transfer.FileOutcomeUpdate
	for _, item := range batch {
		byMessage[item.MessageID] = item
		// Every member of an album is listed, not only its first: the items were
		// built by expanding the album, and the engine no longer expands it
		// again. An album that arrived as one item stays one file.
		if _, exists := seenMessages[item.MessageID]; exists {
			continue
		}
		seenMessages[item.MessageID] = struct{}{}
		ids = append(ids, item.MessageID)
	}
	tmpRoot := filepath.Join(m.downloadDir, ".tdl-tmp", "chat-"+id)
	tmpDir, err := temporaryDialogDirectory(tmpRoot, batch[0].DialogKey)
	// Removed however this batch ends. The batch is the unit that creates it, and
	// a later batch of the same task builds its own - so a failed batch used to
	// leave its half-written files under a directory nothing would ever clean.
	defer func() { _ = os.RemoveAll(tmpDir) }()
	if err != nil {
		// A filesystem that refuses the task's working directory is an
		// environment fault, and it is terminal here for the same reason as the
		// two above: the retry is uncapped, so leaving it to the worker meant
		// the task held its claims and its 下载中 label forever while nothing
		// was ever attempted. Failing it names the problem, and the task can be
		// started again once the environment is fixed.
		reason := fmt.Sprintf("创建会话临时目录: %v", err)
		m.failChatTransfer(id, reason)
		return false, errors.New(reason)
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
	err = m.accounts.Run(transferCtx, target.AccountID, func(runCtx context.Context, client *gotd.Client, kvd kv.Storage) error {
		// The account's pool, not this task's: it is built once per account and
		// its invoker chain carries the rate limit gate. See
		// telegram.Manager.TransferPool.
		pool := m.accounts.TransferPool(target.AccountID, client, config.Download.PoolSize)
		peer := selected.inputPeer()
		if peer == nil {
			return errors.New("会话下载文件缺少 Telegram 来源会话")
		}
		stats, runErr := transfer.Run(runCtx, transfer.Deps{Pool: pool, KV: kvd, Peers: m.accounts.Peers(target.AccountID, client.API(), kvd), AccountID: target.AccountID}, transfer.Options{
			Dir:      tmpDir,
			Peer:     peer,
			Messages: ids,
			Threads:  config.Download.Threads,
			Tasks:    config.Download.TaskLimit,
			Delay:    time.Duration(config.Download.DelayMS) * time.Millisecond,
			OnProgress: func(update transfer.ProgressUpdate) {
				item, ok := byMessage[update.MessageID]
				if !ok {
					return
				}
				watchdog.Touch()
				started, _ := m.progress.Update(id, item.Item, update)
				if started {
					_, _ = m.db.Exec(`UPDATE chat_download_items SET status='running', started_at=? WHERE chat_job_id=? AND dialog_key=? AND message_id=? AND status='queued'`, time.Now().UTC().Format(time.RFC3339Nano), id, item.DialogKey, item.MessageID)
				}
			},
			OnFileCompleted: func(update transfer.FileCompletedUpdate) {
				item, ok := byMessage[update.MessageID]
				if !ok {
					return
				}
				watchdog.Touch()
				m.progress.ClearItem(id, item.Item)
				// The engine invokes completion callbacks from transfer workers.
				// Keep final file moves out of those workers, but wait below
				// before the batch decides its final state. publishChatItem itself
				// records a per-file failure; a post-move database failure is
				// recovered by the reconciliation immediately after the wait and
				// on worker cadence.
				publishWG.Add(1)
				go func() {
					defer publishWG.Done()
					if publishErr := m.publishChatItem(id, update.Path, item, config); publishErr != nil {
						recordPublishErr(publishErr)
						applog.Error("chat_download", "file_publish_failed", "chat_job_id", id, "message_id", item.MessageID, "error", publishErr.Error())
					}
				}()
			},
			// Collected rather than written here, for the reason the message
			// path collects them: this runs on a transfer worker or on the
			// iterator, and a round trip there would hold up the files queued
			// behind it.
			OnFileOutcome: func(update transfer.FileOutcomeUpdate) {
				outcomesMu.Lock()
				outcomes = append(outcomes, update)
				outcomesMu.Unlock()
			},
		})
		applog.Info("chat_download", "batch_transferred", "chat_job_id", id, "requested", len(ids),
			"files", stats.Files, "deleted", stats.Deleted, "without_media", stats.Empty,
			"failed", stats.Failed, "batch_reads", stats.BatchCalls, "single_reads", stats.SingleCalls)
		return runErr
	})
	publishWG.Wait()
	if reconcileErr := m.reconcileChatPublishedItems(batchKeys(id, batch)); reconcileErr != nil {
		return false, fmt.Errorf("核对已移动文件: %w", reconcileErr)
	}
	// Settled before the batch decides it is over: a file that went back on the
	// queue is work the task still has, and the remaining checks below all
	// assume the rows they read are final.
	outcomesMu.Lock()
	batchOutcomes := outcomes
	outcomesMu.Unlock()
	_, requeued, settleErr := m.applyItemFailures(chatItems, id, planItemFailures(batchOutcomes, byMessage))
	if settleErr != nil {
		return false, fmt.Errorf("记录文件下载结果: %w", settleErr)
	}
	if requeued {
		m.signalChat()
	}
	publishMu.Lock()
	persistErr := publishErr
	publishMu.Unlock()
	if persistErr != nil {
		// A transient database write failure before the final move leaves the
		// item running. Put only those still-running rows back on the durable
		// queue; rows that were already reconciled to completed are untouched.
		if recoverErr := m.requeueChatPublishFailures(id, batch); recoverErr != nil {
			return false, fmt.Errorf("恢复文件发布状态: %w", recoverErr)
		}
	}
	if watchdog.Stalled() && m.chatRunningOrUnknown(id) {
		return false, m.requeueStalledChatBatch(id, batch)
	}
	if err != nil && m.chatControlHasNotLanded(id) {
		if m.recordTelegramRPCError(target.AccountID, err) {
			// Mirrors the stalled-item rule: a flood that keeps recurring must not
			// retry forever, because every retry extends the cooldown it is
			// waiting for. No immediate wake either — the account-wide cooldown
			// now keeps this task out of the scheduler until the window expires.
			//
			return false, m.requeueFloodedChatItems(id)
		}
		for _, item := range batch {
			// Another file in this upstream call can fail after this one has
			// completed its final move. Never overwrite that completed state.
			if state := m.chatItemStatus(id, item); state != "completed" && state != "downloaded" {
				_ = m.setChatItem(id, item, "failed", "", err.Error())
			}
		}
		return false, err
	}
	m.touch()
	return false, nil
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
	// The released claims are exactly what this comment promises another request
	// may adopt, so tell the reconciler rather than letting it discover them on
	// its next idle pass.
	m.signalReconcile()
	applog.Info("chat_download", "stalled_batch_requeued", "chat_job_id", chatID, "item_count", len(batch))
	return nil
}

// requeueFloodedChatItems returns a chat batch's running items to the queue, or
// fails the ones that have spent their retry budget. Mirrors the stalled-item
// rule so a flood that keeps recurring does not retry forever, because every
// retry extends the cooldown being waited for.
//
// The claim release shares the transaction because the items that reach the cap
// become terminal, and every other chat transition to a terminal state releases
// the item's global claim. An item left owning media in downloaded_media blocks
// every other task waiting on that media for good: a waiter tests the claim, not
// the item's status.
func (m *Manager) requeueFloodedChatItems(chatID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chat_download_items SET status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'queued' END, error = CASE WHEN attempts >= ? THEN 'Telegram 限流反复出现，已停止自动重试' ELSE 'Telegram 限流中，等待自动恢复' END, started_at = '', finished_at = CASE WHEN attempts >= ? AND finished_at = '' THEN ? ELSE finished_at END WHERE chat_job_id = ? AND status = 'running'`, maxStalledAttempts, maxStalledAttempts, maxStalledAttempts, now, chatID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM downloaded_media m USING chat_download_items i WHERE m.dialog_key = i.dialog_key AND m.message_id = i.message_id AND m.status = 'claimed' AND m.owner_kind = 'chat' AND m.owner_id = ? AND i.chat_job_id = ? AND i.status = 'failed'`, chatID, chatID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	// The released claims may be exactly what a waiting message task needs.
	m.signalReconcile()
	return nil
}

func (m *Manager) chatStatus(id string) string {
	status, _ := m.chatJobStatus(id)
	return status
}

// chatJobStatus reads a session task's state and says whether the read worked.
//
// The two answers are not interchangeable, and the callers that decide whether
// to keep working need the second one. A status that could not be read is an
// empty string, which is a value no task holds - and comparing it against the
// states a task can be in reads as "this task is not running any more", which
// is the opposite of what an unreadable status means.
func (m *Manager) chatJobStatus(id string) (string, bool) {
	var status string
	if err := m.db.QueryRow(`SELECT status FROM chat_download_jobs WHERE id = ?`, id).Scan(&status); err != nil {
		return "", false
	}
	return status, true
}

// chatRunningOrUnknown reports whether a worker should keep working on its task.
//
// It is runningOrUnknown's counterpart for session tasks, and it exists because
// the message path was fixed and this one was not. There, a failed status read
// stopped a worker mid-task: the worker read an empty status, concluded the task
// had been taken away from it, and returned - leaving the task in 下载中 with
// its files queued and nothing that would ever look at it again. Here the same
// mistake swallowed a finished download: publishChatItem read the empty status,
// decided it was not allowed to publish, and returned without a word, so the
// file was removed with the working directory and the task waited forever for a
// completion that had already happened.
func (m *Manager) chatRunningOrUnknown(id string) bool {
	status, ok := m.chatJobStatus(id)
	return !ok || chatRunnable(status)
}

// chatControlHasNotLanded reports whether a failed batch should still settle
// its files.
//
// A pause or a cancel that landed first owns those rows, and the batch must not
// write over the decision. A status that could not be read does not answer
// "yes", and that is the answer that keeps the batch working - the same
// direction runningOrUnknown takes, and the one that cannot lose a failure a
// person needs to see. This reads the status once; the condition it replaces
// read it twice, which is two round trips on the path every failed batch takes.
func (m *Manager) chatControlHasNotLanded(id string) bool {
	status, ok := m.chatJobStatus(id)
	if !ok {
		return true
	}
	return status != ChatStatusPaused && status != ChatStatusCancelled
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
	return m.setItemState(chatItems, chatID, item, status, path, message)
}

// reconcileChatPublishedItems closes the crash window after a final file move
// but before the media index and global ownership record were committed.
//
// scope, when non-nil, limits the pass to the media a caller already knows it
// may have left mid-publish. Every batch used to run the unrestricted query, so
// finishing one batch of sixty-four files scanned the global set of
// published-but-unrecorded rows on a table holding one row per indexed message
// - work proportional to the whole index rather than to the batch that had just
// run. The nil scope keeps the periodic and startup passes, which are the ones
// that must be able to find a row no caller knows about.
// chatItemIndexed reports whether a session task has already been told about
// the media at this identity. It is a primary key probe, so it costs one index
// lookup and saves a Telegram round trip.
func (m *Manager) chatItemIndexed(chatID, dialogKey string, messageID int) bool {
	var indexed bool
	if err := m.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM chat_download_items WHERE chat_job_id = ? AND dialog_key = ? AND message_id = ?)`, chatID, dialogKey, messageID).Scan(&indexed); err != nil {
		return false
	}
	return indexed
}

// mediaKey identifies one downloadable media item of one session task.
type mediaKey struct {
	chatID    string
	dialogKey string
	messageID int
}

// batchKeys names the media one batch claimed, so the reconcile pass that
// follows it examines exactly the work it just did and nothing else.
func batchKeys(chatID string, batch []source) map[mediaKey]struct{} {
	keys := make(map[mediaKey]struct{}, len(batch))
	for _, item := range batch {
		keys[mediaKey{chatID: chatID, dialogKey: item.DialogKey, messageID: item.MessageID}] = struct{}{}
	}
	return keys
}

func (m *Manager) reconcileChatPublishedItems(scope map[mediaKey]struct{}) error {
	query := `SELECT chat_job_id, dialog_key, message_id, final_path FROM chat_download_items WHERE status = 'downloaded' AND final_path <> ''`
	args := []any{}
	if len(scope) > 0 {
		keys := make([]any, 0, len(scope))
		for key := range scope {
			keys = append(keys, key.chatID, key.dialogKey, key.messageID)
		}
		query += ` AND (chat_job_id, dialog_key, message_id) IN (` + strings.TrimSuffix(strings.Repeat("(?,?,?),", len(scope)), ",") + `)`
		args = keys
	}
	rows, err := m.db.Query(query, args...)
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
	return m.publish(chatItems, m.chatLock(chatID), func() bool { return m.chatRunningOrUnknown(chatID) }, chatID, path, item, config)
}

// watchedDialogKeys returns the accounts that need an update connection and, per
// account, the dialogs whose messages may be admitted.
//
// Exactly two kinds of dialog are watched:
//
//   - the listened dialog itself, whose new posts and in-group replies the user
//     asked for;
//   - the channel's linked discussion group, which is where its comments
//     actually arrive - the update carries the group as its peer, not the
//     channel.
//
// The group cannot be derived at admission time without a request, so it is
// recorded on the task by whichever path resolved it (see
// rememberDiscussionLink) and read back here.
//
// This used to also carry an account-level flag meaning "this account has some
// channel listener", which admitted every reply-shaped message in every group the
// account had joined. That is a guess about a message that cannot be validated
// without a request, and its cost was measured: a single enable queued 24 events
// from seven unrelated dialogs in three minutes, each one a Telegram lookup that
// was then retried five times.
//
// It is a separate function because the keys can then be tested without a
// listener connection, a Telegram account or a proxy setting.
func (m *Manager) watchedDialogKeys() (wanted map[string]struct{}, watched map[string]map[string]struct{}, err error) {
	rows, err := m.db.Query(`SELECT account_id, dialog_key, discussion_dialog_key FROM chat_download_jobs WHERE listen_new = 1 AND scan_state = ? AND status IN (?, ?)`, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	wanted = make(map[string]struct{})
	watched = make(map[string]map[string]struct{})
	for rows.Next() {
		var accountID, dialogKey, discussionKey string
		if rows.Scan(&accountID, &dialogKey, &discussionKey) != nil || accountID == "" {
			continue
		}
		for _, key := range []string{dialogKey, discussionKey} {
			if key == "" {
				continue
			}
			if watched[accountID] == nil {
				watched[accountID] = make(map[string]struct{})
			}
			wanted[accountID] = struct{}{}
			watched[accountID][key] = struct{}{}
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, nil, rowsErr
	}
	return wanted, watched, nil
}

// authorizedChatAccounts names the accounts that can hold an update connection
// at all. It is the same predicate ListenNewMessages tests before it connects,
// so starting a listener for anything outside this set can only produce an
// immediate ErrNotAuthorized.
func authorizedChatAccounts(accounts []telegram.Account) map[string]struct{} {
	authorized := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		if account.State == "authorized" {
			authorized[account.ID] = struct{}{}
		}
	}
	return authorized
}

// chatListenerAccountsChanged reports whether the set of authorized accounts
// differs from the one a previous pass used. Signing in, signing out and a
// session expiring all change it, and each of them changes which listeners
// should exist.
func chatListenerAccountsChanged(previous, current map[string]struct{}) bool {
	if len(previous) != len(current) {
		return true
	}
	for id := range current {
		if _, ok := previous[id]; !ok {
			return true
		}
	}
	return false
}

func (m *Manager) reconcileChatListeners() {
	const listenerSafetyRefresh = 5 * time.Minute
	accounts, _ := m.accounts.List()
	authorized := authorizedChatAccounts(accounts)
	now := time.Now()
	due := m.listenerSnapshotAt.Load() == 0 || now.UnixNano()-m.listenerSnapshotAt.Load() >= listenerSafetyRefresh.Nanoseconds()
	// The authorized set is compared on every pass, not only when something
	// marked the snapshot dirty. Nothing marks it when an account is signed in
	// or expires - those transitions happen inside the Telegram manager, which
	// does not know this snapshot exists - so without this an account that was
	// signed in again would wait for the five minute refresh before its
	// listener came back. The comparison is over a handful of in-memory
	// records, which is what makes it cheap enough to run here every three
	// seconds.
	//
	// The five minute refresh below remains the guarantee, not this comparison:
	// even if the set never changed, a rebuild happens at least that often, so
	// a listener can be late but never permanently absent. Only pass 0 and a
	// dirty flag skip their rebuild.
	if !due && !m.listenerDirty.Load() && !chatListenerAccountsChanged(m.listenerAccounts, authorized) {
		return
	}
	// Clear before querying. A concurrent state change sets it again, ensuring
	// this slightly older snapshot is promptly replaced instead of losing the
	// wake-up.
	m.listenerDirty.Store(false)
	wanted, watched, err := m.watchedDialogKeys()
	if err != nil {
		m.listenerDirty.Store(true)
		return
	}
	proxyURL := m.settings.ProxyURL()
	m.mu.Lock()
	// The admission set keeps the entries of accounts that cannot currently
	// listen. A listener for such an account is not running, so the entries are
	// inert, and keeping them means a listener that comes back needs no second
	// pass to be correct again.
	m.chatWatched = watched
	for accountID := range wanted {
		if _, canListen := authorized[accountID]; !canListen {
			// Starting one would open a connection only to be told the account
			// is not signed in. That is what this used to do every five
			// minutes, for every listening task of every account that had been
			// signed out, and each attempt logged a failure.
			continue
		}
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
		_, wantedNow := wanted[accountID]
		_, canListen := authorized[accountID]
		if !wantedNow || !canListen {
			listener.cancel()
			delete(m.chatListeners, accountID)
		}
	}
	m.mu.Unlock()
	m.listenerAccounts = authorized
	m.listenerSnapshotAt.Store(now.UnixNano())
	// The gap walk's only other trigger is a listener becoming ready, and that
	// does not happen again while the listener stays up. A walk that was cut off
	// by a timeout, a flood wait or a restart therefore had nothing left to
	// retry it: the messages it had not reached stayed undownloaded until the
	// next reconnect, which on a healthy installation is the next deployment.
	// Riding this refresh makes every one of those cases self-healing within one
	// interval. On a task that is already up to date the walk costs a single
	// history page before it recognises the watermark.
	for accountID := range wanted {
		if _, canListen := authorized[accountID]; !canListen {
			// Walking a gap means reading history, which the same account state
			// gates. Running it anyway spends three attempts and two sleeps per
			// candidate task to collect an ErrNotAuthorized each time, and the
			// walk has nothing to do until the account is usable again - at
			// which point the changed set rebuilds this snapshot and walks.
			continue
		}
		go m.reconcileListenerGaps(accountID)
	}
	// A listening task that has no linked group yet is a task whose comments
	// cannot be admitted. State changes reach the link worker through
	// markChatListenerDirty, but a task can also change outside this process
	// (an operator correcting a row, a restored database), so the snapshot
	// rebuild - which already reads the task table - is the backstop.
	select {
	case m.chatLinkWake <- struct{}{}:
	default:
	}
}

func (m *Manager) markChatListenerDirty() {
	m.listenerDirty.Store(true)
	select {
	case m.chatWake <- struct{}{}:
	default:
	}
	// A state change that starts a listener is also the moment a task may need
	// its linked discussion group learned: a channel whose history held no
	// comments at all teaches the link to nothing, and the listener would then
	// run without it until the next slow probe. Waking the worker here keeps
	// that window to one query instead of a quarter of an hour.
	select {
	case m.chatLinkWake <- struct{}{}:
	default:
	}
}

// addChatWatched closes the interval between persisting a discussion mapping
// and the next periodic listener reconciliation. The account listener is
// already running for the parent channel, so only its in-memory filter needs
// updating.
// It reports whether the key was new. The caller needs that to know whether the
// snapshot has to be rebuilt, because a rebuild replaces the whole map: a key
// added while one is being built - after watchedDialogKeys has read the table
// and before its result is assigned - is otherwise removed again a moment after
// it was learned, and the discussion group's comments are covered by nothing
// else. That window is milliseconds, but the task then runs without the group
// until the five minute safety refresh restores it, and the dispatcher's
// acknowledgement means any comment arriving in between is lost for good.
func (m *Manager) addChatWatched(accountID, dialogKey string) bool {
	if accountID == "" || dialogKey == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.chatWatched[accountID] == nil {
		m.chatWatched[accountID] = make(map[string]struct{})
	}
	if _, exists := m.chatWatched[accountID][dialogKey]; exists {
		return false
	}
	m.chatWatched[accountID][dialogKey] = struct{}{}
	return true
}

// rememberDiscussionLink records which discussion group belongs to a listening
// task and adds it to the in-memory watch set in the same breath.
//
// Persisting the link is what makes admission exact. A comment on a channel
// post arrives in the channel's linked discussion group, so the group - not the
// channel - is the peer the update carries; without this record the task cannot
// tell its own comments from the chatter of every other group the account has
// joined, and the account-wide guess that used to bridge that gap queued 24
// unrelated events and roughly 120 doomed Telegram requests in three minutes on
// a single enable.
//
// Updating chatWatched here is not an optimisation. The in-memory set is
// rebuilt by the periodic snapshot, so between learning a link and that rebuild
// - minutes on a quiet account - every comment would be filtered out at
// admission, and the dispatcher's acknowledgement means Telegram never
// redelivers it.
func (m *Manager) rememberDiscussionLink(chatJobID, accountID, dialogKey string, peer tg.InputPeerClass) error {
	if chatJobID == "" || accountID == "" || dialogKey == "" {
		return nil
	}
	direct := makeDirectPeer(peer)
	var err error
	if direct.kind == "" {
		// The group is known but its peer reference is not. Admission only needs
		// the key; the peer is what entity-less update recovery uses, and it is
		// filled in by whichever path resolves it first.
		_, err = m.db.Exec(`UPDATE chat_download_jobs SET discussion_dialog_key = ? WHERE id = ? AND discussion_dialog_key IS DISTINCT FROM ?`, dialogKey, chatJobID, dialogKey)
	} else {
		// Guarded so a link relearned on every new post writes nothing once it is
		// already stored: an unguarded update would leave a dead row version and a
		// new index entry per post on a busy channel.
		_, err = m.db.Exec(`UPDATE chat_download_jobs SET discussion_dialog_key = ?, discussion_peer_type = ?, discussion_peer_id = ?, discussion_peer_hash = ? WHERE id = ? AND (discussion_dialog_key IS DISTINCT FROM ? OR discussion_peer_type IS DISTINCT FROM ? OR discussion_peer_hash IS DISTINCT FROM ?)`,
			dialogKey, direct.kind, direct.id, direct.hash, chatJobID, dialogKey, direct.kind, direct.hash)
	}
	if err != nil {
		return err
	}
	if m.addChatWatched(accountID, dialogKey) {
		// The key is new, so a snapshot that is being built right now may not
		// contain it. Asking for another build is what makes the persisted link
		// and the in-memory admission set agree again; a key that was already
		// present needs nothing, which is the common case and the reason this
		// does not fire once per post on a busy channel.
		m.markChatListenerDirty()
	}
	return nil
}

// chatDiscussionProbeInterval is how often the worker looks for listening tasks
// whose linked discussion group is unknown or whose last answer has gone stale.
// It is slow on purpose: the answer changes only when an administrator relinks
// a group, and a task that has just started listening is woken explicitly.
const chatDiscussionProbeInterval = 15 * time.Minute

// chatDiscussionRelinkInterval is how long an answer is trusted. A channel that
// had no comments when it was probed, or none when it was probed as linked, is
// asked again only after this, so a relinked group is picked up on a quiet
// channel too - at one Telegram lookup per task per interval, never per message.
const chatDiscussionRelinkInterval = 24 * time.Hour

// chatDiscussionProbeTimeout bounds the one probe that runs inside the request
// enabling listening. The background pass is not bounded: it delays only the
// next task it would probe.
const chatDiscussionProbeTimeout = 30 * time.Second

// recordDiscussionProbe stores the outcome of one linked-chat lookup: the group
// it found, if any, and the time it was asked. The timestamp is what keeps the
// periodic pass from asking the same question on every run, so it is written
// unconditionally - including when the answer was "no linked group" and when
// the group is the one already recorded.
func (m *Manager) recordDiscussionProbe(chatJobID, accountID, dialogKey string, peer tg.InputPeerClass) error {
	if dialogKey != "" {
		if err := m.rememberDiscussionLink(chatJobID, accountID, dialogKey, peer); err != nil {
			return err
		}
	}
	if chatJobID == "" {
		return nil
	}
	_, err := m.db.Exec(`UPDATE chat_download_jobs SET discussion_probed_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339Nano), chatJobID)
	return err
}

// probeChatDiscussionLink resolves one task's linked discussion group now rather
// than at the next periodic pass.
//
// It runs when listening is enabled, before the listener connection is allowed
// to start. Until the group is recorded, a comment arriving from it is filtered
// out at admission, and the dispatcher acknowledges the update to Telegram, so
// it is never redelivered. The remaining hole is a second task enabled on an
// account whose connection is already up: a comment landing inside this one
// request is still lost, which is why the probe is synchronous rather than
// queued.
func (m *Manager) probeChatDiscussionLink(id string) {
	target, err := m.chatTarget(id)
	if err != nil {
		return
	}
	if !targetIncludesReplies(target) {
		return
	}
	if target.discussionKey != "" {
		// Already known, and the group is re-confirmed by every new post's
		// resolution and by the periodic pass.
		return
	}
	// Bounded because this one runs inside the request that enables listening: a
	// rate-limited or unreachable account must not hold the caller open. A
	// timeout is not recorded as an answer, so the worker retries it.
	ctx, cancel := context.WithTimeout(context.Background(), chatDiscussionProbeTimeout)
	defer cancel()
	answered, dialogKey, peer, err := m.resolveChatDiscussionLink(ctx, target)
	if err != nil {
		applog.Error("chat_download", "discussion_link_probe_failed", "chat_job_id", id, "dialog_key", target.DialogKey, "error", err.Error())
		return
	}
	if !answered {
		return
	}
	if err := m.recordDiscussionProbe(id, target.AccountID, dialogKey, peer); err != nil {
		applog.Error("chat_download", "discussion_link_probe_record_failed", "chat_job_id", id, "error", err.Error())
	}
}

// chatDiscussionLinkWorker learns the linked discussion group of listening tasks
// that do not know it, and re-confirms the ones that do.
//
// It deliberately does not ride chatWorker: that goroutine is single threaded,
// and scanOneChat records what a long Telegram call there costs - state
// refresh, claim reconciliation and listener upkeep all stop for its duration.
// Nor can it ride the listener snapshot, which is built under m.mu and only
// reads. The work is bounded by the number of listening tasks rather than by the
// number of messages, so a goroutine with a slow timer and a wake-up is enough.
func (m *Manager) chatDiscussionLinkWorker() {
	for {
		// Startup starts these goroutines before the database health probe has
		// run, so the first pass can find the database unavailable. Waiting the
		// full probe interval there would leave every listening task - and every
		// comment arriving in the meantime - without a link for a quarter of an
		// hour.
		if !m.DatabaseAvailable() {
			if !m.waitChatEvent(5 * time.Second) {
				return
			}
			continue
		}
		if err := m.refreshChatDiscussionLinks(); err != nil {
			applog.Error("chat_download", "discussion_link_refresh_failed", "error", err.Error())
		}
		select {
		case <-m.stopCh:
			return
		case <-m.chatLinkWake:
		case <-time.After(chatDiscussionProbeInterval):
		}
	}
}

// refreshChatDiscussionLinks probes every reply-enabled listening task whose
// linked discussion group is unknown or whose last answer has gone stale.
func (m *Manager) refreshChatDiscussionLinks() error {
	if !m.DatabaseAvailable() {
		return nil
	}
	// Task scale, not history scale: one row per listening task. The
	// configuration snapshot is JSON, so the reply check stays in Go.
	rows, err := m.db.Query(`SELECT id, account_id, dialog_type, dialog_key, dialog_name, start_message_id, direct_peer_type, direct_peer_id, direct_peer_hash, config_json, discussion_probed_at FROM chat_download_jobs WHERE listen_new = 1 AND scan_state = ? AND status IN (?, ?)`, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return err
	}
	targets := make([]storedChatTarget, 0, 4)
	probedAt := make(map[string]string)
	for rows.Next() {
		var target storedChatTarget
		var timestamp string
		if err := rows.Scan(&target.ID, &target.AccountID, &target.DialogType, &target.DialogKey, &target.DialogName, &target.StartMessageID, &target.direct.kind, &target.direct.id, &target.direct.hash, &target.configJSON, &timestamp); err != nil {
			_ = rows.Close()
			return err
		}
		targets = append(targets, target)
		probedAt[target.ID] = timestamp
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	now := time.Now().UTC()
	accounts, _ := m.accounts.List()
	authorized := authorizedChatAccounts(accounts)
	for _, target := range targets {
		if !targetIncludesReplies(target) {
			continue
		}
		if _, canProbe := authorized[target.AccountID]; !canProbe {
			// The probe is a Telegram request, and the account state gates it
			// before any connection is opened. Trying anyway records a failure
			// per listening task and, because a failure deliberately leaves the
			// probe timestamp untouched, does it again on every wake - which
			// is the rebuild. Skipping is not a loss: a probe is retried on the
			// interval regardless, and a sign-in changes the account set, which
			// rebuilds the snapshot and wakes this worker.
			continue
		}
		// An unparseable timestamp is treated as stale: asking once more is
		// cheaper than never asking again after a hand-written value.
		if at, parseErr := time.Parse(time.RFC3339Nano, probedAt[target.ID]); parseErr == nil && now.Sub(at) < chatDiscussionRelinkInterval {
			continue
		}
		answered, dialogKey, peer, probeErr := m.resolveChatDiscussionLink(context.Background(), target)
		if probeErr != nil {
			applog.Error("chat_download", "discussion_link_probe_failed", "chat_job_id", target.ID, "dialog_key", target.DialogKey, "error", probeErr.Error())
			// A failure must not look like an answer, or the retry would wait a
			// whole interval and a transient error would read as "no discussion
			// group" in the task row.
			continue
		}
		if !answered {
			continue
		}
		if err := m.recordDiscussionProbe(target.ID, target.AccountID, dialogKey, peer); err != nil {
			applog.Error("chat_download", "discussion_link_probe_record_failed", "chat_job_id", target.ID, "error", err.Error())
		}
	}
	return nil
}

// resolveChatDiscussionLink asks Telegram which discussion group is linked to a
// listening channel.
//
// It is the one path that can learn the link with nothing else to learn it from:
// a channel whose history holds no comments at all, and a task whose listening
// was enabled after its history was indexed, have no resolved post to take it
// from.
//
// answered reports whether Telegram was asked and replied. key is empty when the
// answer was "no linked discussion group", which is not an error and must be
// recorded so the question is not asked again immediately.
func (m *Manager) resolveChatDiscussionLink(ctx context.Context, target storedChatTarget) (answered bool, key string, peer tg.InputPeerClass, err error) {
	// Only a broadcast channel can have a linked discussion group: a supergroup
	// has none, Saved Messages has none, and a linked group has no linked group
	// of its own. dialog_type is the refined kind, so this costs nothing to ask.
	if target.DialogType != "channel" || target.direct.kind != "channel" {
		return true, "", nil, nil
	}
	channel, ok := target.inputPeer().(*tg.InputPeerChannel)
	if !ok {
		return true, "", nil, nil
	}
	runErr := m.accounts.Run(ctx, target.AccountID, func(ctx context.Context, client *gotd.Client, kvd kv.Storage) error {
		result, rpcErr := client.API().ChannelsGetFullChannel(ctx, &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash})
		if rpcErr != nil {
			m.recordTelegramRPCError(target.AccountID, rpcErr)
			return rpcErr
		}
		answered = true
		channelFull, ok := result.FullChat.(*tg.ChannelFull)
		if !ok {
			return nil
		}
		linkedID, ok := channelFull.GetLinkedChatID()
		if !ok || linkedID <= 0 {
			return nil
		}
		// The dialog key a comment update carries is derived from the peer, so
		// the group is recorded under the same convention as everything else
		// (see dialogIdentity); a linked group is always an InputPeerChannel.
		key = fmt.Sprintf("channel:%d", linkedID)
		// The group's own entity usually travels in the same response. When it
		// does not, resolve it through the peer cache so the record carries a
		// usable reference for entity-less update recovery instead of only a key.
		for _, raw := range result.Chats {
			group, isChannel := raw.(*tg.Channel)
			if !isChannel || group.ID != linkedID {
				continue
			}
			peer = &tg.InputPeerChannel{ChannelID: group.ID, AccessHash: group.AccessHash}
			return nil
		}
		resolved, resolveErr := m.accounts.Peers(target.AccountID, client.API(), kvd).ResolvePeer(ctx, &tg.PeerChannel{ChannelID: linkedID})
		if resolveErr != nil {
			// The key is known even though the peer is not; the next pass records
			// it with the peer and this request is not repeated per message.
			return resolveErr
		}
		peer = resolved.InputPeer()
		return nil
	})
	if runErr != nil {
		return answered, "", nil, runErr
	}
	return true, key, peer, nil
}

func (m *Manager) runChatListener(ctx context.Context, accountID string, listener *chatListener) {
	err := m.accounts.ListenNewMessages(ctx, accountID, func(_ context.Context, event telegram.NewMessageEvent) {
		m.enqueueChatMessage(event)
	}, func() {
		// Recover the interval between each task's persisted listener watermark
		// and the shared update connection becoming ready. This applies to every
		// listened dialog: an update stream that starts after a message was
		// published never delivers it.
		go m.reconcileListenerGaps(accountID)
	})
	if err != nil && ctx.Err() == nil {
		applog.Error("chat_download", "new_message_listener_failed", "account_id", accountID, "error", err.Error())
	}
	m.mu.Lock()
	if current, ok := m.chatListeners[accountID]; ok && current == listener {
		delete(m.chatListeners, accountID)
	}
	m.mu.Unlock()
}

// listensForNewMedia reports whether a task is responsible for media published
// after the history it indexed, and is therefore the set the gap walk applies
// to. A task still scanning has no settled watermark to walk back to, and one
// that does not listen carries no such responsibility.
func listensForNewMedia(job ChatJob) bool {
	return job.ListenNew && job.ScanState == chatScanCompleted &&
		(job.Status == ChatStatusDownloading || job.Status == ChatStatusListening)
}

// listenerGapCandidateIDs names the tasks an account's gap walk covers: every
// listened dialog that has a real persisted peer, which is every channel, every
// supergroup the user created a task for, and the Saved Messages task.
//
// A supergroup is identified as 'chat', not 'channel': a channel and a
// supergroup are both an InputPeerChannel on the wire, and the kind is refined
// from the resolved peer's broadcast flag. Matching only on 'channel' therefore
// excluded every group, and a group's task had no gap walk at all - media
// posted while the process was down was never recovered, while the task went on
// showing 监听中. A linked discussion group is still reached through its parent
// channel task, but that only covers the groups this service created implicitly;
// it never covered one the user asked for.
func (m *Manager) listenerGapCandidateIDs(accountID string) ([]string, error) {
	rows, err := m.db.Query(`SELECT id FROM chat_download_jobs WHERE account_id = ? AND listen_new = 1 AND scan_state = ? AND status IN (?, ?) AND dialog_type IN ('channel', 'chat', 'self')`, accountID, chatScanCompleted, ChatStatusDownloading, ChatStatusListening)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 4)
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

// gapBatch reports which messages of one history page sit above the watermark,
// the page's oldest message id, and whether the walk has reached media the task
// already covered. reached is what licenses advancing the watermark, so a page
// that is entirely above it must report false.
func gapBatch(page []tg.MessageClass, boundary int) (above []*tg.Message, oldest int, reached bool) {
	for _, raw := range page {
		message, ok := raw.(*tg.Message)
		if !ok {
			continue
		}
		if oldest == 0 || message.ID < oldest {
			oldest = message.ID
		}
		if message.ID > boundary {
			above = append(above, message)
		}
	}
	return above, oldest, oldest == 0 || oldest <= boundary
}

func (m *Manager) reconcileListenerGaps(accountID string) {
	ids, err := m.listenerGapCandidateIDs(accountID)
	if err != nil {
		return
	}
	for _, id := range ids {
		for attempt := 0; attempt < 3; attempt++ {
			if err := m.reconcileListenerGap(id); err == nil {
				break
			} else {
				applog.Info("chat_download", "listener_gap_reconcile_failed", "chat_job_id", id, "account_id", accountID, "attempt", attempt+1, "error", err.Error())
				if attempt == 2 {
					break
				}
				timer := time.NewTimer(time.Duration(2*(attempt+1)) * time.Second)
				<-timer.C
			}
		}
	}
}

// reconcileListenerGap closes the two windows in which media published to a
// listened dialog is covered by nothing.
//
// History scanning is bounded by the task's upper_message_id, which is captured
// when the task is created, so anything published while the scan runs sits above
// that bound and is never scanned. The live update stream only covers a dialog
// once a listener is watching it, which begins after the scan completes, and it
// covers nothing while that connection is down or still being established.
//
// The walk therefore reads from the newest message backwards, hands every
// message above the watermark to the ordinary live path, and stops as soon as
// it reaches the watermark. Re-delivery is safe: the inbox is unique per
// message, and an item the task already has is left alone.
func (m *Manager) reconcileListenerGap(id string) error {
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	if !listensForNewMedia(target.ChatJob) {
		return nil
	}
	peer := target.inputPeer()
	if peer == nil {
		return nil
	}
	// Watching the dialog here means a message arriving while this walk runs is
	// admitted by the live path too. The overlap costs one duplicate inbox row
	// at most, because the insert ignores a message it already has.
	m.addChatWatched(target.AccountID, target.DialogKey)
	// One walk is no longer limited to a single short deadline. The position is
	// persisted page by page, so an interrupted walk resumes where it stopped;
	// before that, a backlog larger than one short pass was simply never
	// recovered, because every retry restarted from the newest message and the
	// union of the retries never grew. The outer bound now only stops a walk that
	// cannot make progress at all.
	ctx, cancel := context.WithTimeout(context.Background(), listenerGapBudget)
	defer cancel()
	return m.accounts.Run(ctx, target.AccountID, func(ctx context.Context, client *gotd.Client, _ kv.Storage) error {
		// The linked discussion group is a different dialog with its own
		// history, so the walk below - which reads this task's own dialog -
		// cannot reach the comments left in it. Its failure is reported and
		// swallowed: the two walks keep independent cursors, and one dialog
		// being unreadable must not cost the other its recovery.
		if err := m.reconcileDiscussionGap(ctx, target, client); err != nil {
			applog.Error("chat_download", "listener_gap_discussion_failed", "chat_job_id", target.ID, "account_id", target.AccountID, "error", err.Error())
		}
		boundary := target.UpperMessageID
		watermark := boundary
		offset := m.listenerGapOffset(id)
		reachedBoundary := false
		for {
			// Reading history is an ordinary Telegram request and is paced like
			// every other one. It was the only call in the repository that was
			// not, which made the moment a backlog is largest - a reconnect or a
			// restart, with every listened dialog walking at once - the moment the
			// account was most likely to be put into a flood wait, and a flood
			// wait recorded nowhere, so the scheduler kept feeding it.
			pageCtx, cancelPage := context.WithTimeout(ctx, listenerGapPageTimeout)
			result, err := client.API().MessagesGetHistory(pageCtx, &tg.MessagesGetHistoryRequest{Peer: peer, OffsetID: offset, Limit: 100})
			cancelPage()
			if err != nil {
				m.recordTelegramRPCError(target.AccountID, err)
				return err
			}
			above, oldest, reached := gapBatch(searchMessages(result), boundary)
			// Enqueue before testing termination: the page that finally reaches
			// the watermark also holds the last stretch above it, so returning
			// first would drop exactly the messages closest to the boundary.
			for _, message := range above {
				// Deliberately built without a reply header, unlike the events
				// the discussion walk rebuilds. This walk reads the task's own
				// dialog, so the dialog key is already the whole attribution and
				// admission matches the task on it directly. Carrying a reply
				// header here would not add information - it would send a post
				// that happens to answer another post down admission's
				// comment-attribution path, which resolves a discussion origin
				// over the network for a message this walk already knows the
				// owner of.
				if !m.enqueueChatMessage(telegram.NewMessageEvent{AccountID: target.AccountID, DialogKey: target.DialogKey, DialogName: target.DialogName, MessageID: message.ID, InputPeer: peer}) {
					// The row was not written, so this message exists only in the
					// retry queue. Stop the walk where it is: the watermark is not
					// advanced, the page cursor is not moved, and the next walk
					// re-offers this whole stretch. Re-offering is free - the inbox
					// is unique per message - while advancing would make the one
					// recovery path for this message skip it for good, since the
					// dispatcher has already acknowledged the update to Telegram.
					return nil
				}
				if message.ID > watermark {
					watermark = message.ID
				}
			}
			if reached {
				reachedBoundary = true
				break
			}
			if oldest == 0 || oldest == offset {
				// The server returned a page already walked. Spinning on it would
				// consume the whole budget against the rate limiter, so stop and
				// keep the cursor for a later attempt.
				return nil
			}
			offset = oldest
			m.setListenerGapOffset(id, offset)
		}
		m.setListenerGapOffset(id, 0)
		// Advance the watermark only once the walk actually reached the previous
		// one. Writing it after every page looks harmless because the walk runs
		// newest to oldest, but it makes an interrupted walk unrecoverable: the
		// next run would read the already advanced watermark as its boundary and
		// stop on the first page, leaving everything below it unread for good —
		// which is exactly the stretch it exists to recover.
		if !reachedBoundary || watermark <= target.UpperMessageID {
			return nil
		}
		_, err := m.db.Exec(`UPDATE chat_download_jobs SET upper_message_id = ?, updated_at = ? WHERE id = ? AND upper_message_id < ?`, watermark, time.Now().UTC().Format(time.RFC3339Nano), id, watermark)
		return err
	})
}

// listenerGapStream is the resumable position of the gap walk, kept in the same
// per-task stream table the history scan uses. offset_message_id is the page
// offset to resume from, and zero means no walk is in progress: the next one
// starts from the newest message.
const listenerGapStream = "listener_gap"

// listenerGapDiscussionStream is the same position for the linked discussion
// group's own history. It is a separate stream because the two walks cover
// different dialogs whose message ids are unrelated: sharing one cursor would
// let the channel's position decide how far back the group is read.
const listenerGapDiscussionStream = "listener_gap_discussion"

func (m *Manager) listenerGapOffset(id string) int {
	return m.gapStreamOffset(id, listenerGapStream)
}

func (m *Manager) setListenerGapOffset(id string, offset int) {
	m.setGapStreamOffset(id, listenerGapStream, offset)
}

func (m *Manager) gapStreamOffset(id, streamKind string) int {
	var offset int
	if err := m.db.QueryRow(`SELECT offset_message_id FROM chat_download_streams WHERE chat_job_id = ? AND stream_kind = ?`, id, streamKind).Scan(&offset); err != nil {
		return 0
	}
	return offset
}

func (m *Manager) setGapStreamOffset(id, streamKind string, offset int) {
	_, _ = m.db.Exec(`INSERT INTO chat_download_streams(chat_job_id, stream_kind, offset_message_id) VALUES (?, ?, ?) ON CONFLICT(chat_job_id, stream_kind) DO UPDATE SET offset_message_id = EXCLUDED.offset_message_id`, id, streamKind, offset)
}

// discussionGapPageCap bounds one discussion walk. A backlog larger than this
// keeps its persisted cursor and is continued by the next walk, so the cap costs
// latency rather than coverage.
const discussionGapPageCap = 20

// reconcileDiscussionGap admits the comments published into a listening
// channel's linked discussion group while the update connection was down.
//
// The walk in reconcileListenerGap cannot cover them, and this is the gap it
// leaves: it reads the channel's history, and a comment is a message in the
// discussion group, so a comment left under an older post while the listener was
// disconnected was never offered again - the dispatcher had already acknowledged
// the update to Telegram, and the periodic probe only re-establishes the link.
// The task went on showing 监听中 and the file was simply gone.
//
// The watermark is derived rather than stored. The highest message id this
// account has ever admitted from that dialog is exactly what the inbox table
// already records, and it is the right boundary: everything above it is new.
// Re-reading below it costs nothing but requests, because the inbox insert
// ignores a message it already has, so a row aged out by retention makes the
// walk go further back rather than create a duplicate download.
//
// Nothing here decides what a comment means: every message above the boundary is
// offered through the same admission path a live update takes, so a comment
// recovered here is resolved, attributed and filtered exactly as it would have
// been had the connection stayed up.
func (m *Manager) reconcileDiscussionGap(ctx context.Context, target storedChatTarget, client *gotd.Client) error {
	peer := target.discussion.inputPeer()
	if peer == nil {
		// The group is either not linked or its peer has not been learned yet.
		return nil
	}
	// Derived from the peer rather than read from the task, so the key this walk
	// filters by is the same key the admission path will compute for the event.
	_, dialogKey, _ := dialogIdentity(peer, target.AccountID)
	if dialogKey == "" {
		return nil
	}
	var boundary int
	if err := m.db.QueryRow(`SELECT COALESCE(MAX(message_id), 0) FROM chat_message_inbox WHERE account_id = ? AND dialog_key = ?`, target.AccountID, dialogKey).Scan(&boundary); err != nil {
		return err
	}
	// A group with nothing admitted yet has no boundary to walk back to. The
	// task only knows this dialog because it read one of the channel's posts, so
	// anything older than the newest message is history this task never covered
	// and is not what a reconnect owes.
	if boundary == 0 {
		return nil
	}
	// Admission is gated on the watched set, and the live path is what normally
	// populates it for this dialog. A walk that ran before the first comment ever
	// arrived would otherwise be filtered away silently.
	m.addChatWatched(target.AccountID, dialogKey)
	return m.walkDiscussionGap(target, peer, dialogKey, boundary, func(offset int) ([]tg.MessageClass, error) {
		pageCtx, cancelPage := context.WithTimeout(ctx, listenerGapPageTimeout)
		defer cancelPage()
		result, err := client.API().MessagesGetHistory(pageCtx, &tg.MessagesGetHistoryRequest{Peer: peer, OffsetID: offset, Limit: 100})
		if err != nil {
			m.recordTelegramRPCError(target.AccountID, err)
			return nil, err
		}
		return searchMessages(result), nil
	})
}

// walkDiscussionGap drives one pass over a listening task's linked discussion
// group, newest page first. fetch returns the page of that group's history
// strictly older than the offset it is handed.
//
// The page loop takes a function rather than a client because the cursor rule
// below is the part that loses messages when it is wrong, and a rule that can
// only be exercised against a live Telegram connection is a rule nobody
// exercises. A caller with no connection can hand it a history.
//
// The persisted offset is where an interrupted walk resumes, and the two ways a
// walk can end are not the same thing:
//
//   - It proved there is nothing left to recover below it. It reached the
//     boundary, or the group's history ran out. The offset returns to zero, and
//     that is not a detail: a walk resumes downwards from its cursor, so a
//     cursor left behind is a walk that can never look at anything new. Only a
//     walk that starts from the newest message can see a comment published
//     since the last one.
//   - It ran out of page budget mid-backlog. Here the offset stays exactly
//     where it is. This walk has already raised the inbox watermark to the
//     newest message it admitted, and the next walk takes that watermark as its
//     boundary - so restarting from the top would stop on its first page and
//     strand every message below it for good. Resuming downwards is the only
//     direction that still reaches them.
//
// historyMessageID returns the id a history entry carries, or zero for an entry
// this build does not know. Both message variants carry one, and a walk needs
// the oldest id in a page to move its cursor past that page - whatever the page
// contains, because a page of service messages still has to be walked past.
func historyMessageID(raw tg.MessageClass) int {
	switch value := raw.(type) {
	case *tg.Message:
		return value.ID
	case *tg.MessageService:
		return value.ID
	}
	return 0
}

func (m *Manager) walkDiscussionGap(target storedChatTarget, peer tg.InputPeerClass, dialogKey string, boundary int, fetch func(offset int) ([]tg.MessageClass, error)) error {
	offset := m.gapStreamOffset(target.ID, listenerGapDiscussionStream)
	completed := false
	for page := 0; page < discussionGapPageCap; page++ {
		messages, err := fetch(offset)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			completed = true
			break
		}
		oldest := 0
		reached := false
		for _, raw := range messages {
			// The cursor advances on any entry, not only on the ones that carry
			// media. A page that happens to hold nothing but service messages
			// has no download in it, but it does have a position, and taking
			// one from every entry is what makes the walk able to move past it.
			// Deriving it from the download candidates alone left the walk with
			// no resume point on such a page, and a walk with no resume point
			// restarts from the newest message and reads the same page again.
			if id := historyMessageID(raw); id > 0 && (oldest == 0 || id < oldest) {
				oldest = id
			}
			message, ok := raw.(*tg.Message)
			if !ok {
				continue
			}
			if message.ID <= boundary {
				reached = true
				continue
			}
			// Built through the one constructor that fills the reply header.
			// Admission attributes a comment to the channel post it answers
			// from those two fields, and drops an event without them as
			// unattributable - silently, because a message that belongs to no
			// task is not an error.
			if !m.enqueueChatMessage(telegram.NewMessageEventFor(target.AccountID, dialogKey, target.DialogName, 0, message, peer)) {
				// Only in the retry queue, so stop and keep the cursor: the next
				// walk re-reads this stretch instead of skipping it.
				return nil
			}
		}
		if reached {
			completed = true
			break
		}
		if oldest == 0 || oldest == offset {
			// Either the page held nothing with an id at all, or the server
			// returned the page it was asked to move past. There is nothing to
			// resume from, so start the next walk from the newest message rather
			// than spinning on this page. Both are degenerate: the first needs a
			// page whose every entry is an unknown type, the second a server
			// that ignores OffsetID.
			completed = true
			break
		}
		offset = oldest
		m.setGapStreamOffset(target.ID, listenerGapDiscussionStream, offset)
	}
	if completed {
		m.setGapStreamOffset(target.ID, listenerGapDiscussionStream, 0)
	}
	return nil
}

func (m *Manager) chatEventWorker() {
	for {
		if !m.DatabaseAvailable() {
			if !m.waitChatEvent(time.Second * 5) {
				return
			}
			continue
		}
		events, err := m.claimChatMessageInbox(1)
		if err != nil {
			applog.Error("chat_download", "new_message_claim_failed", "error", err.Error())
			if !m.waitChatEvent(2 * time.Second) {
				return
			}
			continue
		}
		if len(events) == 0 {
			select {
			case <-m.stopCh:
				return
			case <-m.chatEventWake:
			case <-time.After(chatEventIdleInterval):
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
				// The group this comment arrived from now belongs to this task.
				// Recording it on the task row is what admission reads; the
				// in-memory watch set is updated by the same call, so the next
				// comment in this group is not filtered out while the snapshot is
				// still stale.
				if linkErr := m.rememberDiscussionLink(id, event.AccountID, dialogKey, event.InputPeer); linkErr != nil {
					applog.Error("chat_download", "discussion_link_persist_failed", "chat_job_id", id, "dialog_key", dialogKey, "error", linkErr.Error())
				}
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
		// An album is delivered as one update per member, and resolving any one
		// member returns the whole group - so a ten photo album cost ten
		// identical Telegram lookups and ten database passes to download the
		// same ten files once. After the first member is registered the others
		// are already indexed and are skipped here. A member the task's file
		// filter rejected is not indexed, so it is still resolved and judged.
		if m.chatItemIndexed(id, dialogKey, event.MessageID) {
			continue
		}
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
			// A listener learns the discussion root of a newly received post
			// even when it has no comments yet, so the first comment can be
			// attributed without a second lookup.
			// event.Message is the message the update carried, so this resolve
			// reads nothing: the post that just arrived is what it names. Only a
			// rebuilt event - one replayed from the durable inbox, which has no
			// update behind it - leaves it nil and pays for a read.
			resolved[includeReplies], resolvedErr[includeReplies] = m.resolveSourcesFor(context.Background(), resolveTarget{
				AccountID:      event.AccountID,
				Peer:           event.InputPeer,
				DialogID:       event.DialogID,
				MessageID:      event.MessageID,
				DialogName:     target.DialogName,
				Message:        event.Message,
				IncludeReplies: includeReplies,
				LearnRoot:      true,
				ChatJobID:      id,
			})
		}
		if resolveErr := resolvedErr[includeReplies]; resolveErr != nil {
			// A resolution that did not complete is not the same answer as "this
			// message holds nothing to download". Treating the two alike used to
			// discard the update for good: the dispatcher has already
			// acknowledged it to Telegram, so it is never redelivered; the inbox
			// insert only reopens a failed row, so the 'done' row this produced
			// could never be retried; and the gap walk advances the watermark
			// past this message the next time it runs, so it was not even
			// re-offered. A connection reset, a deadline or a flood wait during
			// resolution therefore lost a published file silently, with one Info
			// line as the only trace.
			//
			// Returning it hands the event to the ordinary retry budget. That is
			// safe to repeat: the inbox is unique per message, registering is
			// idempotent, and a rejection Telegram states outright is classified
			// below rather than retried.
			if permanentInboxError(resolveErr) {
				// Telegram answered the request: the post was deleted between
				// being published and being read, or this account may not read
				// it. No later attempt changes either. It settles as skipped - a
				// settled event nobody has to act on - because a listener that
				// raised an error card for every deleted post in a busy channel
				// would be unusable, and because the alternative this replaced
				// was to drop it with no record at all.
				applog.Info("chat_download", "new_message_not_downloadable", "account_id", event.AccountID, "dialog_key", dialogKey, "message_id", event.MessageID, "error", resolveErr.Error())
				return nothingToDo(resolveErr)
			}
			applog.Info("chat_download", "new_message_resolve_failed", "account_id", event.AccountID, "dialog_key", dialogKey, "message_id", event.MessageID, "error", resolveErr.Error())
			return fmt.Errorf("解析新消息 %d: %w", event.MessageID, resolveErr)
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

func (m *Manager) discoverDiscussionOrigin(event telegram.NewMessageEvent, rootID int) (string, int, int, error) {
	var originKey string
	var originID, resolvedRootID int
	err := m.accounts.Run(context.Background(), event.AccountID, func(ctx context.Context, client *gotd.Client, _ kv.Storage) error {
		var err error
		originKey, originID, resolvedRootID, err = discussionOriginFromRoot(m, ctx, client.API(), event.AccountID, event.InputPeer, rootID)
		return err
	})
	return originKey, originID, resolvedRootID, err
}

func (m *Manager) scanOneChat() error {
	// An account inside a Telegram cooldown is skipped here, not only inside the
	// API await. History scanning is the one long-running Telegram call owned by
	// a single goroutine, so waiting out a flood window inside it stopped every
	// other thing that goroutine does - state refresh, claim reconciliation and
	// listener upkeep - for as long as the window lasted.
	var id string
	err := m.db.QueryRow(`SELECT id FROM chat_download_jobs j WHERE status = 'queued' AND scan_state != 'completed'
 AND NOT EXISTS (SELECT 1 FROM telegram_rate_limits l WHERE l.account_id = j.account_id AND l.blocked_until::timestamptz > ?::timestamptz)
 ORDER BY created_at LIMIT 1`, time.Now().UTC().Format(time.RFC3339Nano)).Scan(&id)
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
	config, err := chatDownloadConfig(target)
	if err != nil {
		_, _ = m.db.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, ChatStatusFailed, err.Error(), time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning)
		m.touch()
		return err
	}
	hasEnabledStream := false
	for _, kind := range chatStreamKinds {
		if chatStreamEnabled(kind, config) {
			hasEnabledStream = true
			break
		}
	}
	if !hasEnabledStream {
		// An explicit empty file-type selection means "download nothing".
		// Do not establish a Telegram connection or perform an otherwise
		// pointless history scan for a task that cannot admit any file.
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ?`, id); err != nil {
			return err
		}
		_, err := m.db.Exec(`UPDATE chat_download_jobs SET scan_state = ?, status = ?, error = '', updated_at = ? WHERE id = ? AND status = ?`, chatScanCompleted, ChatStatusCompleted, now, id, ChatStatusScanning)
		if err == nil {
			m.touch()
			m.markChatListenerDirty()
		}
		return err
	}
	ctx, release := m.beginChatExecution(id)
	defer release()
	err = m.accounts.Run(ctx, target.AccountID, func(ctx context.Context, client *gotd.Client, _ kv.Storage) error {
		for _, kind := range chatStreamKinds {
			if _, err := m.db.Exec(`INSERT INTO chat_download_streams(chat_job_id, stream_kind) VALUES (?, ?) ON CONFLICT(chat_job_id, stream_kind) DO NOTHING`, target.ID, kind); err != nil {
				return err
			}
			if !chatStreamEnabled(kind, config) {
				if _, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = ?`, target.ID, kind); err != nil {
					return err
				}
				continue
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
		if m.recordTelegramRPCError(target.AccountID, err) {
			_, _ = m.db.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, ChatStatusQueued, "Telegram 限流中，等待自动恢复", time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning)
			m.touch()
			m.markChatListenerDirty()
			return nil
		}
		// A real indexing failure must stop any batches already admitted while
		// history scanning ran in parallel. Leaving them alive would expose a
		// failed parent whose child files are still downloading.
		m.cancelChatExecutions(id)
		m.failChatScan(id, err.Error())
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
		m.markChatListenerDirty()
		// The scan just became complete, which is the moment this task's live
		// coverage begins. Everything published while it scanned sits above the
		// watermark the scan was bounded by, so walk that stretch now instead of
		// waiting for a listener restart that may not come for days.
		if current.ListenNew {
			go m.reconcileListenerGap(id)
		}
	}
	return err
}

// scanChatStream uses messages.search with a media-only filter. OffsetID is
// persisted after every page, so recovery never has to start the history over;
// MinID/MaxID keep the immutable range fixed at task creation time.
func (m *Manager) scanChatStream(ctx context.Context, client *gotd.Client, target storedChatTarget, kind string) error {
	config, err := chatDownloadConfig(target)
	if err != nil {
		return err
	}
	if !chatStreamEnabled(kind, config) {
		_, err = m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = ?`, target.ID, kind)
		return err
	}
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
	// A grouped album can straddle only a page boundary. Keep successful
	// boundary expansions for this scan so the same album is not looked up
	// again on the following page.
	expandedGroups := make(map[int64]struct{})
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
			m.recordTelegramRPCError(target.AccountID, err)
			return fmt.Errorf("搜索%s文件: %w", label, err)
		}
		messages := searchMessages(result)
		if len(messages) == 0 {
			_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = ?`, target.ID, kind)
			return err
		}
		page := chatPageGroups(messages, target.StartMessageID, target.UpperMessageID)
		candidates := make([]source, 0, len(messages))
		for _, group := range page {
			members := group.members
			if group.id != 0 && group.boundary {
				if _, done := expandedGroups[group.id]; done {
					continue
				}
				if expanded, groupErr := tmsg.GetGroupedMessages(ctx, client.API(), target.inputPeer(), group.members[0]); groupErr == nil && len(expanded) > 0 {
					members = expanded
					expandedGroups[group.id] = struct{}{}
				} else if groupErr != nil && m.recordTelegramRPCError(target.AccountID, groupErr) {
					// Keep the durable page cursor in place after a flood response;
					// otherwise an album crossing a page boundary could be incomplete.
					return fmt.Errorf("展开相册: %w", groupErr)
				}
			}
			if len(members) == 0 {
				continue
			}
			message := members[0]
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
		}
		if err := m.registerChatMedia(target.ID, candidates, false); err != nil {
			return err
		}
		nextOffset, exhausted, cursorErr := nextChatPageCursor(messages, offset, minID)
		if cursorErr != nil {
			return fmt.Errorf("%s文件索引分页: %w", label, cursorErr)
		}
		offset = nextOffset
		if exhausted {
			if _, err := m.db.Exec(`UPDATE chat_download_streams SET offset_message_id = ?, completed = 1 WHERE chat_job_id = ? AND stream_kind = ?`, offset, target.ID, kind); err != nil {
				return err
			}
			m.touch()
			return nil
		}
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
	// Saved Messages are a private self-dialog and do not have Telegram
	// channel discussion threads. Avoid walking the entire history for reply
	// metadata when this task uses the global comment setting.
	if target.DialogType == "self" {
		_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, target.ID)
		return err
	}
	config, err := chatDownloadConfig(target)
	if err != nil {
		return err
	}
	if !chatStreamEnabled("reply_candidates", config) {
		_, err = m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, target.ID)
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
			m.recordTelegramRPCError(target.AccountID, err)
			return fmt.Errorf("读取会话回复索引: %w", err)
		}
		messages := searchMessages(result)
		if len(messages) == 0 {
			_, err := m.db.Exec(`UPDATE chat_download_streams SET completed = 1 WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, target.ID)
			return err
		}
		candidates := make([]source, 0)
		seenGroups := make(map[int64]struct{})
		for _, raw := range messages {
			message, ok := raw.(*tg.Message)
			if !ok || message.ID < target.StartMessageID || message.ID > target.UpperMessageID {
				continue
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
				if expanded, groupErr := tmsg.GetGroupedMessages(ctx, client.API(), target.inputPeer(), message); groupErr == nil && len(expanded) > 0 {
					members = expanded
				} else if groupErr != nil && m.recordTelegramRPCError(target.AccountID, groupErr) {
					return fmt.Errorf("展开回复相册: %w", groupErr)
				}
			}
			originID := firstMessageID(members, message.ID)
			related, relatedErr := relatedSources(m, ctx, client.API(), target.AccountID, target.inputPeer(), target.DialogName, members, message.ID, originID, false, target.ID)
			if relatedErr != nil {
				if telegramWaitDuration(relatedErr) > 0 {
					// A rate-limited reply lookup must retry this durable history
					// page, rather than silently advancing past its comments.
					return relatedErr
				}
				// A missing or inaccessible discussion never fails channel history.
				logRelatedWarning(target.AccountID, message.ID, relatedErr)
				continue
			}
			candidates = append(candidates, related...)
		}
		if err := m.registerChatMedia(target.ID, candidates, false); err != nil {
			return err
		}
		nextOffset, exhausted, cursorErr := nextChatPageCursor(messages, offset, minID)
		if cursorErr != nil {
			return fmt.Errorf("回复索引分页: %w", cursorErr)
		}
		offset = nextOffset
		if exhausted {
			if _, err := m.db.Exec(`UPDATE chat_download_streams SET offset_message_id = ?, completed = 1 WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, offset, target.ID); err != nil {
				return err
			}
			m.touch()
			return nil
		}
		if _, err := m.db.Exec(`UPDATE chat_download_streams SET offset_message_id = ? WHERE chat_job_id = ? AND stream_kind = 'reply_candidates'`, offset, target.ID); err != nil {
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

type chatPageGroup struct {
	id       int64
	members  []*tg.Message
	boundary bool
}

// chatPageGroups mirrors Telegram's local history model: messages carrying a
// GroupedID are contiguous in a history/search page. Therefore every group
// wholly inside the page is already complete; only the first and last group
// may continue on an adjacent page and need an optional API expansion.
func chatPageGroups(raw []tg.MessageClass, startID, upperID int) []chatPageGroup {
	groups := make([]chatPageGroup, 0, len(raw))
	positions := make(map[int64]int)
	var firstGroup, lastGroup int64
	first := true
	for _, entry := range raw {
		message, ok := entry.(*tg.Message)
		if !ok || message.ID < startID || message.ID > upperID {
			continue
		}
		groupID, grouped := message.GetGroupedID()
		if first {
			if grouped {
				firstGroup = groupID
			}
			first = false
		}
		if grouped {
			lastGroup = groupID
		} else {
			lastGroup = 0
		}
		if !grouped {
			groups = append(groups, chatPageGroup{members: []*tg.Message{message}})
			continue
		}
		if position, exists := positions[groupID]; exists {
			groups[position].members = append(groups[position].members, message)
			continue
		}
		positions[groupID] = len(groups)
		groups = append(groups, chatPageGroup{id: groupID, members: []*tg.Message{message}})
	}
	for index := range groups {
		if groups[index].id != 0 && (groups[index].id == firstGroup || groups[index].id == lastGroup) {
			groups[index].boundary = true
		}
	}
	return groups
}

// pageOldestMessageID returns the oldest message identity Telegram supplied,
// including deleted placeholders and service messages. Those records cannot
// produce a download, but they are still part of the cursor space and must
// advance pagination.
func pageOldestMessageID(page []tg.MessageClass) int {
	oldest := 0
	for _, raw := range page {
		var id int
		switch message := raw.(type) {
		case *tg.Message:
			id = message.ID
		case *tg.MessageEmpty:
			id = message.ID
		case *tg.MessageService:
			id = message.ID
		}
		if id > 0 && (oldest == 0 || id < oldest) {
			oldest = id
		}
	}
	return oldest
}

// nextChatPageCursor turns a Telegram page into a durable history cursor.
// OffsetID pages occasionally repeat their boundary record. That record has
// already been observed and persisted (or was a non-downloadable placeholder),
// so moving one ID older is safe and prevents a false fatal "cursor stuck".
// A page that reaches minID has exhausted this task's fixed range.
func nextChatPageCursor(page []tg.MessageClass, previous, minID int) (next int, completed bool, err error) {
	oldest := pageOldestMessageID(page)
	if oldest == 0 {
		return 0, false, errors.New("Telegram 分页响应不含可用消息 ID")
	}
	if oldest <= minID {
		return oldest, true, nil
	}
	if previous > 0 {
		switch {
		case oldest < previous:
			return oldest, false, nil
		case oldest == previous:
			if previous-1 <= minID {
				return previous - 1, true, nil
			}
			return previous - 1, false, nil
		default:
			return 0, false, fmt.Errorf("Telegram 分页游标倒退异常（当前 %d，页面最早 %d）", previous, oldest)
		}
	}
	return oldest, false, nil
}

func (m *Manager) refreshChatStates() {
	// Only an actively draining history queue needs aggregation. Completed and
	// listening-only tasks are stable; a newly persisted listener event switches
	// its parent back to downloading in registerChatMedia.
	// Query one compact cached summary per active parent. Its trigger-maintained
	// counters cover all single-item and batch state transitions atomically.
	// waiting items count as work in progress. They are files this task wants
	// that another task is holding, and they become downloadable on their own
	// once that claim is released, so a task whose whole queue is waiting has not
	// finished — without them here it would be concluded as completed, and the
	// promotion pass only ever looks at a task that can still run, so its waiting
	// files would never come back. The message side keeps such a task queued for
	// the same reason.
	rows, err := m.db.Query(`SELECT j.id, j.listen_new, j.status,
	COALESCE(s.queued, 0) + COALESCE(s.running, 0) + COALESCE(s.downloaded, 0) + COALESCE(s.waiting, 0),
	COALESCE(s.failed, 0), COALESCE(s.queued, 0), COALESCE(s.completed, 0)
FROM chat_download_jobs j LEFT JOIN chat_download_stats s ON s.chat_job_id = j.id
WHERE j.status = ? AND j.scan_state = ?`, ChatStatusDownloading, chatScanCompleted)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, status string
		var listen int
		var active, failed, pending, completed int
		if err := rows.Scan(&id, &listen, &status, &active, &failed, &pending, &completed); err != nil {
			continue
		}
		next := ChatStatusCompleted
		if active > 0 || pending > 0 {
			next = ChatStatusDownloading
		} else if listen != 0 {
			next = ChatStatusListening
		} else if failed > 0 {
			// "Partial" claims some of the task succeeded, so it is only right
			// when at least one file actually published. A task whose files all
			// failed has nothing to be partial about, and reporting it as partly
			// done hides that every file needs attention. This matches what a
			// message task reports for the same outcome.
			next = ChatStatusPartial
			if completed == 0 {
				next = ChatStatusFailed
			}
		}
		// Every write below is conditional on the status this pass observed.
		// Pause and cancel take the chat lock and stop the task themselves, but
		// this pass reads a batch of tasks and then writes them one at a time, so
		// without the guard a control action landing in that window was
		// overwritten: a task the user had just cancelled turned itself back into
		// completed or failed a moment later.
		if next != status {
			result, err := m.db.Exec(`UPDATE chat_download_jobs SET status = ?, updated_at = ? WHERE id = ? AND status = ?`, next, time.Now().UTC().Format(time.RFC3339Nano), id, status)
			if err != nil {
				continue
			}
			if changed, _ := result.RowsAffected(); changed != 1 {
				// The task left 'downloading' between the read and this write.
				continue
			}
			m.touch()
			m.markChatListenerDirty()
		}
		if next == ChatStatusListening && failed > 0 {
			_, _ = m.db.Exec(`UPDATE chat_download_jobs SET error = ? WHERE id = ? AND status = ?`, fmt.Sprintf("历史下载有 %d 个文件失败，可重新开始失败项", failed), id, next)
		}
		if next == ChatStatusFailed && failed > 0 {
			_, _ = m.db.Exec(`UPDATE chat_download_jobs SET error = ? WHERE id = ? AND status = ?`, fmt.Sprintf("全部 %d 个文件均下载失败，请查看各文件的失败原因", failed), id, next)
		}
		if pending > 0 {
			// This runs inside chatWorker, so it must wake only the transfer
			// workers: signalling chatWake here would hand chatWorker the token
			// it just sent and spin the loop without its fallback delay.
			m.signalChatDownload()
		}
	}
}

// releaseChatClaims drops every media claim this chat task still owns. It runs
// inside the same transaction as the parent state change, so a task can never be
// observed paused or cancelled while still owning media. A waiter tests the
// claim rather than the owner's task status, so a claim left behind would stall
// every message task that wants the same file.
func releaseChatClaims(id string) func(*databaseTx) error {
	return func(tx *databaseTx) error {
		_, err := tx.Exec(`DELETE FROM downloaded_media WHERE status = 'claimed' AND owner_kind = 'chat' AND owner_id = ?`, id)
		return err
	}
}

// failChatScan marks a session task failed and releases the media it had
// claimed while indexing, in one transaction.
//
// The two writes belong together. A failed task transfers nothing, so a claim it
// keeps is held by nobody and blocks every other task that wants that file; and
// media indexed before the failure has already been claimed, so failing without
// releasing is exactly how those claims get stranded.
func (m *Manager) failChatScan(id, reason string) {
	tx, err := m.db.Begin()
	if err != nil {
		applog.Error("chat_download", "chat_scan_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, ChatStatusFailed, reason, time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning); err != nil {
		applog.Error("chat_download", "chat_scan_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	if err := releaseChatClaims(id)(tx); err != nil {
		applog.Error("chat_download", "chat_scan_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		applog.Error("chat_download", "chat_scan_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	m.touch()
	m.markChatListenerDirty()
	m.signalChat()
}

// failChatTransfer marks a session task failed when what it needs in order to
// make progress cannot be read at all, and releases the media it had claimed.
//
// These are the failures the transfer worker's own retry cannot fix: the
// snapshot of download settings the task was created with does not parse, or
// the task has no usable Telegram peer reference. Returning them from
// runOneChatBatch looked harmless, because the worker logs and tries again - but
// the try happens on the idle interval with no cap and no attempt ceiling on
// this path. The claimed items have already had their attempts incremented a few
// lines earlier, and the per item failure handling that would have stopped them
// runs later, after a transfer that is never reached. The task therefore stayed
// in 下载中 with nothing in flight and nothing that could move it, held its
// claim window against every other task that wanted the same files, and grew its
// attempt counters for as long as the process ran.
//
// Failing it is what turns that into something a person can see and act on. The
// media it had claimed is released in the same transaction, for the reason
// failChatScan releases: a claim left behind by a task that will never transfer
// blocks every other task that wants that file.
func (m *Manager) failChatTransfer(id, reason string) {
	tx, err := m.db.Begin()
	if err != nil {
		applog.Error("chat_download", "media_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chat_download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status IN (?, ?, ?)`, ChatStatusFailed, reason, time.Now().UTC().Format(time.RFC3339Nano), id, ChatStatusScanning, ChatStatusDownloading, ChatStatusListening); err != nil {
		applog.Error("chat_download", "media_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	if err := releaseChatClaims(id)(tx); err != nil {
		applog.Error("chat_download", "media_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		applog.Error("chat_download", "media_failure_not_recorded", "chat_job_id", id, "error", err.Error())
		return
	}
	applog.Error("chat_download", "media_failure_terminal", "chat_job_id", id, "reason", reason)
	m.touch()
	m.markChatListenerDirty()
	m.signalChat()
}

// orphanedMediaClaims removes global media claims held by a task that cannot
// transfer, in either direction.
//
// A claim exists to stop another task downloading the same media, so a claim
// held by a task that is not running protects nothing: it only blocks every
// other task that wants that file. The release paths cover pause and cancel,
// and deleting or purging a session task now releases explicitly, but a claim
// can still be stranded - by a task removed before those paths existed, by a
// failure branch that ends a task without passing through either, or by a task
// the user deletes while its scan is failing. This sweep is what makes the
// invariant self-healing rather than dependent on every writer remembering.
//
// Both kinds follow the same rule: a claim is kept only while its owner can
// transfer. An ordinary message task used to be judged by a weaker one - its row
// had to be gone altogether - which meant a claim it left behind while paused,
// failed or partial was held by nobody and released by nothing. Every other task
// wanting that media waited on it, and the release paths could not catch up,
// because each of them only covers the item states it happens to write.
//
// The concern that motivated the weaker rule - dropping a claim a task still
// intends to use would download the same file twice - is answered by claimMedia
// rather than by keeping the claim: a task that still wants the media re-acquires
// it on its next pass, and if another task got there first it adopts the
// finished file instead of transferring it again. A permanently held claim has
// no such recovery.
func (m *Manager) orphanedMediaClaims() error {
	_, err := m.db.Exec(`DELETE FROM downloaded_media d
WHERE d.status = 'claimed' AND (
 (d.owner_kind = 'chat' AND NOT EXISTS (
   SELECT 1 FROM chat_download_jobs j WHERE j.id = d.owner_id
   AND j.status IN ('queued', 'scanning', 'downloading', 'listening')))
 OR (d.owner_kind = 'message' AND NOT EXISTS (
   SELECT 1 FROM download_jobs j WHERE j.id = d.owner_id
   AND j.status IN ('queued', 'running')))
 OR d.owner_kind NOT IN ('chat', 'message')
)`)
	return err
}

// PauseChat stops both indexing and active transfers for this one visible task.
// It releases its global claims because a paused task transfers nothing: a
// message task waiting on the same file must be able to proceed instead of
// stalling until this task is resumed. Resuming re-acquires the claim, or adopts
// the finished file when the message task got there first, so the media is still
// downloaded only once.
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
	// waiting is in the list for the reason it is in the message path's: a file
	// waiting on another task's claim is still this task's work, and pausing
	// only what happens to be queued leaves it out of every later statement
	// that selects by state - resuming asks for paused rows, and this one was
	// never made one.
	if err := m.transitionChatItems(id, target.Status, ChatStatusPaused, "", chatItems.pauseStatement(), releaseChatClaims(id), now, id); err != nil {
		return err
	}
	m.cancelChatExecutions(id)
	// Releasing the claims is what unblocks message tasks waiting on this task's
	// media, so they must be told to re-examine them now.
	m.signalReconcile()
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
	// The attempt budget is reset for the reason Resume resets it on the other
	// table: a file that ran the budget down before the pause would be one stall
	// away from being given up on, so pressing 恢复 would hand back a file with
	// no attempts left. Starting a task again is the person saying the earlier
	// failures were circumstances, not the file.
	if err := m.transitionChatItems(id, ChatStatusPaused, next, "", chatItems.requeueStatement(true), nil, id); err != nil {
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
	// A file's attempts counter bounds the automatic stall retries. A click is a
	// new budget - leaving the counter at the cap made the next stall fail the
	// file immediately with "已停止自动重试", so asking for a retry did nothing.
	if err := m.transitionChatItems(id, target.Status, next, "", chatItems.requeueStatement(false), nil, id); err != nil {
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
	if err := m.transitionChatItems(id, target.Status, ChatStatusCancelled, "已取消，可重新开始", chatItems.cancelStatement(), releaseChatClaims(id), time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return err
	}
	m.cancelChatExecutions(id)
	m.signalReconcile()
	return nil
}

// SetChatListening changes only whether new messages are accepted after the
// historical range has been indexed. Enabling it never scans history again.
func (m *Manager) SetChatListening(id string, enabled bool) error {
	changed, err := m.setChatListening(id, enabled)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if enabled {
		// Learn which discussion group this channel's comments arrive in before
		// the listener connection is allowed to start. Until it is recorded, a
		// comment from that group is filtered out at admission, and the
		// dispatcher acknowledges the update to Telegram, so it is never
		// redelivered. The probe is a Telegram request, so it runs outside the
		// task lock this function's state change took.
		m.probeChatDiscussionLink(id)
	}
	m.touch()
	m.markChatListenerDirty()
	m.signalChat()
	return nil
}

// setChatListening applies the state change under the task lock and reports
// whether it changed anything, so a no-op request does not rebuild the listener
// snapshot or wake the workers.
//
// The flag is what the listener reads, and the listener only starts for a task
// whose scan has completed and whose status says it is running or settling. So
// the flag may be turned on or off at any point in a task's life: a task that is
// still scanning keeps its status and picks the wish up when it gets there,
// rather than being refused with a message that made the two requests the
// controls actually send - "stop listening" while the history downloads, and
// "listen too" right after starting that download - fail for no reason.
func (m *Manager) setChatListening(id string, enabled bool) (bool, error) {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return false, err
	}
	// A cancelled or deleted task is not running, and a flag on it would be a
	// standing request to accept new messages for work its owner abandoned.
	if target.Status == ChatStatusCancelled || target.Status == ChatStatusDeleted {
		return false, errors.New("当前会话任务不能修改消息监听")
	}
	next := target.Status
	if enabled {
		if target.ListenNew {
			return false, nil
		}
		if target.Status == ChatStatusCompleted || target.Status == ChatStatusPartial || target.Status == ChatStatusFailed {
			// The task has nothing left to do, so listening is now its whole
			// state and the card should say so.
			next = ChatStatusListening
		}
		if _, err := m.db.Exec(`UPDATE chat_download_jobs SET listen_new = 1, status = ?, error = '', updated_at = ? WHERE id = ?`, next, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
			return false, err
		}
		return true, nil
	}
	if !target.ListenNew {
		return false, nil
	}
	if target.Status == ChatStatusListening {
		var failed int
		if err := m.db.QueryRow(`SELECT COALESCE(failed, 0) FROM chat_download_stats WHERE chat_job_id = ?`, id).Scan(&failed); err != nil {
			return false, err
		}
		if failed > 0 {
			next = ChatStatusPartial
		} else {
			next = ChatStatusCompleted
		}
	}
	if _, err := m.db.Exec(`UPDATE chat_download_jobs SET listen_new = 0, status = ?, error = '', updated_at = ? WHERE id = ?`, next, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return false, err
	}
	return true, nil
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
	m.markChatListenerDirty()
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
	// A deleted task transfers nothing, so it must not keep owning media. The
	// task row stays for history, which means the orphan sweep cannot tell that
	// this claim is dead: it has to be released here, in the same transaction
	// that removes the task's ability to run.
	if err := releaseChatClaims(id)(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.visibleChats.Add(-1)
	_ = os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", "chat-"+id))
	m.touch()
	m.markChatListenerDirty()
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
	// The media index is removed with the task, so the claims go first and in the
	// same transaction. Deleting the task alone leaves claim rows pointing at an
	// id that no longer exists, and nothing can ever release them again - every
	// other task that wants that media waits for an owner that is gone.
	if err := releaseChatClaims(id)(tx); err != nil {
		return err
	}
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
	m.markChatListenerDirty()
	return nil
}
