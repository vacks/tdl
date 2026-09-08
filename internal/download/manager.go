package download

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	upstreamDL "github.com/iyear/tdl/app/dl"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/iyear/tdl/pkg/consts"
	"github.com/iyear/tdl/pkg/tmessage"
	"github.com/spf13/viper"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

var (
	ErrDuplicate        = errors.New("该 Telegram 消息已存在于下载记录中")
	errFinalNameTooLong = errors.New("最终文件名或完整路径超过 Linux 长度限制")
)

const (
	linuxNameMaxBytes = 255
	linuxPathMaxBytes = 4096
)

type Item struct {
	ID           int64  `json:"id"`
	DialogType   string `json:"dialogType"`
	DialogKey    string `json:"dialogKey"`
	DialogID     int64  `json:"dialogId"`
	MessageID    int    `json:"messageId"`
	GroupedID    int64  `json:"groupedId,omitempty"`
	MessageText  string `json:"messageText,omitempty"`
	OriginalName string `json:"originalName"`
	Size         int64  `json:"size"`
	FinalPath    string `json:"finalPath,omitempty"`
	StartedAt    string `json:"startedAt,omitempty"`
	FinishedAt   string `json:"finishedAt,omitempty"`
	ElapsedMS    int64  `json:"elapsedMs"`
	Attempts     int    `json:"attempts"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
}

type Job struct {
	ID             string `json:"id"`
	SourceURL      string `json:"sourceUrl"`
	DialogType     string `json:"dialogType"`
	DialogKey      string `json:"dialogKey"`
	DialogName     string `json:"dialogName"`
	HasPublicLink  bool   `json:"hasPublicLink"`
	MessageText    string `json:"messageText,omitempty"`
	AccountID      string `json:"accountId"`
	Status         string `json:"status"`
	Attempts       int    `json:"attempts"`
	Error          string `json:"error,omitempty"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
	TotalItems     int    `json:"totalItems"`
	CompletedItems int    `json:"completedItems"`
	DirectPeerType string
	DirectPeerID   int64
	DirectPeerHash int64
	Items          []Item `json:"items"`
}

// BotMessageRef identifies one Bot lifecycle message. It is persisted so a
// container restart does not make a later job transition create a duplicate
// notification instead of editing the original message.
type BotMessageRef struct {
	ChatID    int64
	MessageID int64
	Text      string
}

type source struct {
	Item
	DialogName string
}
type directPeer struct {
	kind     string
	id, hash int64
}

type Manager struct {
	db          *sql.DB
	downloadDir string
	settings    *settings.Store
	accounts    *telegram.Manager
	mu          sync.Mutex
	cancels     map[string]context.CancelFunc
	wake        chan struct{}
	progress    *progressStore
	events      *eventBus
	revision    atomic.Uint64
}

func Open(dataDir, downloadDir string, store *settings.Store, accounts *telegram.Manager) (*Manager, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	// WAL allows the Web UI to read task state while the worker writes it. The
	// busy timeout turns short writer contention into a wait instead of an HTTP
	// error, which is especially common just after retrying a task.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", filepath.ToSlash(filepath.Join(dataDir, "tdl.db")))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	m := &Manager{db: db, downloadDir: downloadDir, settings: store, accounts: accounts, cancels: make(map[string]context.CancelFunc), wake: make(chan struct{}, 1), progress: newProgressStore(), events: newEventBus()}
	if err := m.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Retain the upstream temporary files and resume keys. A worker will resume
	// these jobs using the account originally selected at enqueue time.
	_, _ = m.db.Exec(`UPDATE download_jobs SET status = 'queued', error = '服务重启，任务等待恢复', updated_at = ? WHERE status = 'running'`, time.Now().UTC().Format(time.RFC3339))
	// Jobs created before account binding was introduced cannot safely be resumed:
	// selecting whichever account happens to be current could download under the
	// wrong Telegram identity. Keep their records visible, but require a new job.
	_, _ = m.db.Exec(`UPDATE download_jobs SET status = 'failed', error = '旧任务缺少 Telegram 账户绑定，无法安全重试，请重新创建下载任务', updated_at = ? WHERE status NOT IN ('completed', 'cancelled') AND account_id = ''`, time.Now().UTC().Format(time.RFC3339))
	go m.worker()
	return m, nil
}

func (m *Manager) migrate() error {
	_, err := m.db.Exec(`
CREATE TABLE IF NOT EXISTS download_jobs (
 id TEXT PRIMARY KEY, source_url TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL DEFAULT '', dialog_name TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '', direct_peer_type TEXT NOT NULL DEFAULT '', direct_peer_id INTEGER NOT NULL DEFAULT 0, direct_peer_hash INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS download_items (
 id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL, dialog_id INTEGER NOT NULL, message_id INTEGER NOT NULL, grouped_id INTEGER NOT NULL DEFAULT 0,
 message_text TEXT NOT NULL DEFAULT '', original_name TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0, final_path TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '', elapsed_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
 UNIQUE(dialog_key, message_id), FOREIGN KEY(job_id) REFERENCES download_jobs(id)
);
CREATE INDEX IF NOT EXISTS download_items_job_id ON download_items(job_id);`)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(`
CREATE TABLE IF NOT EXISTS bot_lifecycle_messages (
 job_id TEXT NOT NULL,
 chat_id INTEGER NOT NULL,
 message_id INTEGER NOT NULL,
	message_text TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(job_id, chat_id),
 FOREIGN KEY(job_id) REFERENCES download_jobs(id) ON DELETE CASCADE
)`)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(`CREATE TABLE IF NOT EXISTS download_resets (
 account_id TEXT NOT NULL, source_url TEXT NOT NULL,
 PRIMARY KEY(account_id, source_url)
)`)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(`
CREATE TABLE IF NOT EXISTS download_requests (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL, source_kind TEXT NOT NULL, account_id TEXT NOT NULL,
 source_url TEXT NOT NULL DEFAULT '', dialog_key TEXT NOT NULL DEFAULT '', message_id INTEGER NOT NULL DEFAULT 0,
 trigger_json TEXT NOT NULL DEFAULT '{}', outcome TEXT NOT NULL, created_at TEXT NOT NULL,
 FOREIGN KEY(job_id) REFERENCES download_jobs(id)
);
CREATE INDEX IF NOT EXISTS download_requests_job_id ON download_requests(job_id);
CREATE TABLE IF NOT EXISTS download_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, request_id TEXT NOT NULL DEFAULT '',
 kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
 FOREIGN KEY(job_id) REFERENCES download_jobs(id)
);
CREATE INDEX IF NOT EXISTS download_events_job_id ON download_events(job_id, id);`)
	if err != nil {
		return err
	}
	// Existing development databases get the same new columns without a data reset.
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN account_id TEXT NOT NULL DEFAULT ''`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN dialog_name TEXT NOT NULL DEFAULT ''`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN direct_peer_type TEXT NOT NULL DEFAULT ''`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN direct_peer_id INTEGER NOT NULL DEFAULT 0`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN direct_peer_hash INTEGER NOT NULL DEFAULT 0`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN dialog_type TEXT NOT NULL DEFAULT 'legacy'`)
	_, _ = m.db.Exec(`ALTER TABLE download_jobs ADD COLUMN dialog_key TEXT NOT NULL DEFAULT ''`)
	_, _ = m.db.Exec(`ALTER TABLE download_items ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`)
	_, _ = m.db.Exec(`ALTER TABLE download_items ADD COLUMN size INTEGER NOT NULL DEFAULT 0`)
	_, _ = m.db.Exec(`ALTER TABLE download_items ADD COLUMN started_at TEXT NOT NULL DEFAULT ''`)
	_, _ = m.db.Exec(`ALTER TABLE download_items ADD COLUMN finished_at TEXT NOT NULL DEFAULT ''`)
	_, _ = m.db.Exec(`ALTER TABLE download_items ADD COLUMN elapsed_ms INTEGER NOT NULL DEFAULT 0`)
	_, _ = m.db.Exec(`ALTER TABLE bot_lifecycle_messages ADD COLUMN message_text TEXT NOT NULL DEFAULT ''`)
	if err := m.migrateItemIdentity(); err != nil {
		return err
	}
	return nil
}

// migrateItemIdentity replaces the old numeric-only unique key. Telegram user,
// basic-chat and channel IDs live in different namespaces, so dialog_id alone
// is not a safe cross-dialog identity.
func (m *Manager) migrateItemIdentity() error {
	rows, err := m.db.Query(`PRAGMA table_info(download_items)`)
	if err != nil {
		return err
	}
	hasDialogKey := false
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primary int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
			_ = rows.Close()
			return err
		}
		if name == "dialog_key" {
			hasDialogKey = true
		}
	}
	if err := rows.Close(); err != nil || hasDialogKey {
		return err
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`CREATE TABLE download_items_v2 (
 id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL, dialog_id INTEGER NOT NULL, message_id INTEGER NOT NULL, grouped_id INTEGER NOT NULL DEFAULT 0,
 message_text TEXT NOT NULL DEFAULT '', original_name TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0, final_path TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '', elapsed_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
 UNIQUE(dialog_key, message_id), FOREIGN KEY(job_id) REFERENCES download_jobs(id)
)`); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO download_items_v2(id, job_id, dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, original_name, size, final_path, started_at, finished_at, elapsed_ms, attempts, status, error)
 SELECT id, job_id, 'legacy', 'legacy:' || dialog_id, dialog_id, message_id, grouped_id, message_text, original_name, size, final_path, started_at, finished_at, elapsed_ms, attempts, status, error FROM download_items`); err != nil {
		return err
	}
	if _, err = tx.Exec(`DROP TABLE download_items`); err != nil {
		return err
	}
	if _, err = tx.Exec(`ALTER TABLE download_items_v2 RENAME TO download_items`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX download_items_job_id ON download_items(job_id)`); err != nil {
		return err
	}
	return tx.Commit()
}

// SaveBotLifecycleMessage stores the one lifecycle card shown to a Bot user.
func (m *Manager) SaveBotLifecycleMessage(jobID string, ref BotMessageRef) error {
	_, err := m.db.Exec(`INSERT OR REPLACE INTO bot_lifecycle_messages(job_id, chat_id, message_id, message_text) VALUES (?, ?, ?, ?)`, jobID, ref.ChatID, ref.MessageID, ref.Text)
	return err
}

// BotLifecycleMessages returns all lifecycle cards for a task.
func (m *Manager) BotLifecycleMessages(jobID string) ([]BotMessageRef, error) {
	rows, err := m.db.Query(`SELECT chat_id, message_id, message_text FROM bot_lifecycle_messages WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]BotMessageRef, 0)
	for rows.Next() {
		var ref BotMessageRef
		if err := rows.Scan(&ref.ChatID, &ref.MessageID, &ref.Text); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func (m *Manager) List() ([]Job, error) {
	jobs, _, err := m.ListPage(1, 100)
	return jobs, err
}

// Get returns one task including its file records. It is used by the Bot to
// refresh one displayed task without repeatedly loading unrelated task pages.
func (m *Manager) Get(id string) (Job, error) {
	var job Job
	err := m.db.QueryRow(`SELECT id, source_url, dialog_type, dialog_key, dialog_name, account_id, attempts, status, error, created_at, updated_at FROM download_jobs WHERE id = ?`, id).Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogName, &job.AccountID, &job.Attempts, &job.Status, &job.Error, &job.CreatedAt, &job.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, errors.New("下载任务不存在")
	}
	if err != nil {
		return Job{}, err
	}
	job.HasPublicLink = isPublicMessageLink(job.SourceURL)
	items, err := m.items(id)
	if err != nil {
		return Job{}, err
	}
	job.Items, job.TotalItems = items, len(items)
	for _, item := range items {
		if job.MessageText == "" && item.MessageText != "" {
			job.MessageText = item.MessageText
		}
		if item.Status == "completed" {
			job.CompletedItems++
		}
	}
	return job, nil
}

// ListPage fetches only the requested page of task records. Item detail is
// intentionally loaded after the job rows are closed, so the same SQLite
// connection can safely serve both queries.
func (m *Manager) ListPage(page, pageSize int) ([]Job, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 50 {
		pageSize = 50
	}
	var total int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_jobs`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := m.db.Query(`SELECT id, source_url, dialog_type, dialog_key, dialog_name, account_id, attempts, status, error, created_at, updated_at FROM download_jobs ORDER BY created_at DESC LIMIT ? OFFSET ?`, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, err
	}
	jobs := make([]Job, 0)
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogName, &job.AccountID, &job.Attempts, &job.Status, &job.Error, &job.CreatedAt, &job.UpdatedAt); err != nil {
			_ = rows.Close()
			return nil, 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, 0, err
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	for index := range jobs {
		jobs[index].HasPublicLink = isPublicMessageLink(jobs[index].SourceURL)
		items, err := m.items(jobs[index].ID)
		if err != nil {
			return nil, 0, err
		}
		jobs[index].Items = items
		jobs[index].TotalItems = len(items)
		for _, item := range items {
			if jobs[index].MessageText == "" && item.MessageText != "" {
				jobs[index].MessageText = item.MessageText
			}
			if item.Status == "completed" {
				jobs[index].CompletedItems++
			}
		}
	}
	return jobs, total, nil
}

func (m *Manager) Summary() (active, completedItems, failedItems int) {
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE status IN ('queued', 'running', 'downloaded', 'paused')`).Scan(&active)
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE status = 'completed'`).Scan(&completedItems)
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE status = 'failed'`).Scan(&failedItems)
	return
}

func (m *Manager) LiveProgress() []FileProgress {
	return m.progress.Snapshot()
}

// Revision increases only when persistent task state changes. It lets the UI
// avoid polling SQLite while still refreshing promptly after state transitions.
func (m *Manager) Revision() uint64 { return m.revision.Load() }
func (m *Manager) touch()           { m.revision.Add(1) }

func (m *Manager) Enqueue(ctx context.Context, sourceURL string) (Job, error) {
	submission, err := m.Submit(ctx, DownloadIntent{Source: SourceWeb, URL: sourceURL})
	if err != nil {
		return Job{}, err
	}
	if submission.Duplicate {
		return submission.Job, ErrDuplicate
	}
	return submission.Job, nil
}

// EnqueueReaction creates a normal download job from a reaction update. The
// message is resolved with the exact InputPeer supplied by Telegram, so private
// chats and channels work without requiring a shareable public message link.
func (m *Manager) EnqueueReaction(ctx context.Context, accountID, sourceURL, dialogName string, dialogID int64, peer tg.InputPeerClass, messageID int) (Job, error) {
	submission, err := m.Submit(ctx, DownloadIntent{Source: SourceReaction, AccountID: accountID, Message: &MessageRef{SourceURL: sourceURL, DialogName: dialogName, DialogID: dialogID, InputPeer: peer, MessageID: messageID}})
	if err != nil {
		return Job{}, err
	}
	if submission.Duplicate {
		return submission.Job, ErrDuplicate
	}
	return submission.Job, nil
}

// taskLogFiles adds useful creation context without exposing final paths.
// File names are the original Telegram media names and are emitted only once
// per task, never for every progress update.
func taskLogFiles(sources []source) []string {
	names := make([]string, 0, len(sources))
	for _, item := range sources {
		names = append(names, item.OriginalName)
	}
	return names
}

func (m *Manager) enqueueIntent(intent DownloadIntent, sources []source, direct directPeer) (Submission, error) {
	id, err := randomID()
	if err != nil {
		return Submission{}, err
	}
	requestID, err := randomID()
	if err != nil {
		return Submission{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	trigger := triggerJSON(intent.Trigger)
	tx, err := m.db.Begin()
	if err != nil {
		return Submission{}, err
	}
	defer tx.Rollback()
	var existingID string
	for _, item := range sources {
		var jobID string
		err = tx.QueryRow(`SELECT job_id FROM download_items WHERE dialog_key = ? AND message_id = ? LIMIT 1`, item.DialogKey, item.MessageID).Scan(&jobID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Submission{}, err
		}
		if jobID != "" {
			if existingID != "" && existingID != jobID {
				return Submission{}, errors.New("消息组中的媒体已关联到不同下载任务，无法安全合并")
			}
			existingID = jobID
		}
	}
	if existingID != "" {
		if _, err = tx.Exec(`INSERT INTO download_requests(id, job_id, source_kind, account_id, source_url, dialog_key, message_id, trigger_json, outcome, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'duplicate', ?)`, requestID, existingID, intent.Source, intent.AccountID, intent.URL, sources[0].DialogKey, sources[0].MessageID, trigger, now); err != nil {
			return Submission{}, err
		}
		if err = tx.Commit(); err != nil {
			return Submission{}, err
		}
		job, err := m.Get(existingID)
		if err != nil {
			return Submission{}, err
		}
		m.emit(existingID, requestID, "request_attached", job.Status)
		return Submission{RequestID: requestID, Job: job, Duplicate: true}, nil
	}
	if _, err = tx.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`, id, intent.URL, sources[0].DialogType, sources[0].DialogKey, sources[0].DialogName, intent.AccountID, direct.kind, direct.id, direct.hash, now, now); err != nil {
		return Submission{}, err
	}
	for _, item := range sources {
		if _, err = tx.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, original_name, size, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued')`, id, item.DialogType, item.DialogKey, item.DialogID, item.MessageID, item.GroupedID, item.MessageText, item.OriginalName, item.Size); err != nil {
			return Submission{}, err
		}
	}
	if _, err = tx.Exec(`INSERT INTO download_requests(id, job_id, source_kind, account_id, source_url, dialog_key, message_id, trigger_json, outcome, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'created', ?)`, requestID, id, intent.Source, intent.AccountID, intent.URL, sources[0].DialogKey, sources[0].MessageID, trigger, now); err != nil {
		return Submission{}, err
	}
	if err = tx.Commit(); err != nil {
		return Submission{}, err
	}
	m.touch()
	job := Job{ID: id, SourceURL: intent.URL, DialogType: sources[0].DialogType, DialogKey: sources[0].DialogKey, DialogName: sources[0].DialogName, HasPublicLink: isPublicMessageLink(intent.URL), AccountID: intent.AccountID, DirectPeerType: direct.kind, DirectPeerID: direct.id, DirectPeerHash: direct.hash, Status: "queued", CreatedAt: now, UpdatedAt: now, TotalItems: len(sources)}
	m.emit(id, requestID, "job_created", "queued")
	m.signal()
	return Submission{RequestID: requestID, Job: job, Created: true}, nil
}

func (m *Manager) worker() {
	for {
		job, sources, err := m.nextQueued()
		if err == nil && job.ID != "" {
			m.run(job, sources)
			continue
		}
		select {
		case <-m.wake:
		case <-time.After(3 * time.Second):
		}
	}
}

func (m *Manager) nextQueued() (Job, []source, error) {
	var job Job
	err := m.db.QueryRow(`SELECT id, source_url, dialog_type, dialog_key, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, attempts, status, error, created_at, updated_at FROM download_jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1`).Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogName, &job.AccountID, &job.DirectPeerType, &job.DirectPeerID, &job.DirectPeerHash, &job.Attempts, &job.Status, &job.Error, &job.CreatedAt, &job.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, nil, nil
	}
	if err != nil {
		return Job{}, nil, err
	}
	sources, err := m.sources(job.ID)
	return job, sources, err
}

func (m *Manager) run(job Job, sources []source) {
	claim, err := m.db.Exec(`UPDATE download_jobs SET status = 'running', attempts = attempts + 1, error = '', updated_at = ? WHERE id = ? AND status = 'queued'`, time.Now().UTC().Format(time.RFC3339), job.ID)
	if err != nil {
		applog.Error("download", "task_claim_failed", "job_id", job.ID, "error", err.Error())
		return
	}
	if changed, _ := claim.RowsAffected(); changed != 1 {
		return
	}
	m.touch()
	m.emit(job.ID, "", "job_status_changed", "running")
	applog.Info("download", "task_started", "job_id", job.ID, "account_id", job.AccountID, "item_count", len(sources))
	pending := make([]source, 0, len(sources))
	for _, item := range sources {
		if item.Status == "completed" {
			continue
		}
		pending = append(pending, item)
		m.beginItemAttempt(item)
	}
	if len(pending) == 0 {
		m.setJob(job.ID, "completed", "")
		return
	}
	base, stop := context.WithTimeout(context.Background(), 24*time.Hour)
	ctx, cancel := context.WithCancel(base)
	m.mu.Lock()
	m.cancels[job.ID] = cancel
	m.mu.Unlock()
	defer func() { cancel(); stop(); m.mu.Lock(); delete(m.cancels, job.ID); m.mu.Unlock() }()
	defer m.progress.ClearJob(job.ID)
	tmpDir := filepath.Join(m.downloadDir, ".tdl-tmp", job.ID)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		m.fail(job.ID, pending, err)
		return
	}
	config := m.settings.Get()
	// One upstream invocation resolves exactly one dialog, so MessageID is
	// sufficient for callback correlation here. Do not use its numeric dialog
	// ID: Telegram represents some dialogs (notably "Saved Messages") with a
	// different peer form in download callbacks than in the update stream.
	pendingByMessage := make(map[int]source, len(pending))
	for _, item := range pending {
		pendingByMessage[item.MessageID] = item
	}
	var publishWG sync.WaitGroup
	// A deleted task may have left an upstream resume key. Consume this one-shot
	// marker so recreating it starts with a new temporary directory.
	restart := m.consumeRestart(job.AccountID, job.SourceURL)
	err = m.accounts.Run(ctx, job.AccountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		viper.Set(consts.FlagThreads, config.Download.Threads)
		viper.Set(consts.FlagLimit, config.Download.TaskLimit)
		viper.Set(consts.FlagPoolSize, config.Download.PoolSize)
		viper.Set(consts.FlagDelay, time.Duration(config.Download.DelayMS)*time.Millisecond)
		viper.Set(consts.FlagDisableProgressPS, true)
		opts := upstreamDL.Options{URLs: []string{job.SourceURL}, Dir: tmpDir, Template: config.Download.TempFilenameTemplate, Group: true, Continue: true, Restart: restart, Quiet: true, ProgressCallback: func(update upstreamDL.ProgressUpdate) {
			item, ok := pendingByMessage[update.MessageID]
			if !ok {
				return
			}
			started, _ := m.progress.Update(job.ID, item.Item, update)
			if started {
				m.markItemStarted(item)
			}
		}, FileCompletedCallback: func(update upstreamDL.FileCompletedUpdate) {
			item, ok := pendingByMessage[update.MessageID]
			if !ok {
				return
			}
			m.markItemFinished(item)
			publishWG.Add(1)
			go func() {
				defer publishWG.Done()
				m.publishItem(job.ID, update.Path, item, config)
			}()
		}}
		if peer := job.directInputPeer(); peer != nil {
			opts.URLs = nil
			opts.DirectDialogs = [][]*tmessage.Dialog{{{Peer: peer, Messages: []int{sources[0].MessageID}}}}
		}
		return upstreamDL.Run(ctx, client, kvd, opts)
	})
	// File completion callbacks run in the upstream download workers. Their
	// publishing work is deliberately asynchronous, but the job state must not
	// be decided until every accepted file has finished moving.
	publishWG.Wait()
	if err != nil {
		if m.status(job.ID) == "paused" {
			for _, item := range pending {
				m.pauseItem(item)
			}
			return
		}
		if m.status(job.ID) == "cancelled" {
			return
		}
		// Upstream can return an error while persisting resume metadata after all
		// media callbacks have already completed and every final move succeeded.
		// The observable task result is still successful in that case.
		if m.allItemsCompleted(job.ID) {
			m.setJob(job.ID, "completed", "")
			_ = os.RemoveAll(tmpDir)
			return
		}
		m.fail(job.ID, pending, err)
		return
	}
	// A cancellation can race with the upstream call finishing. Never publish a
	// completed temporary file after the user has paused or cancelled its job.
	// The temporary file and upstream resume state stay intact for a later resume.
	if status := m.status(job.ID); status != "running" {
		if status == "paused" {
			for _, item := range pending {
				m.pauseItem(item)
			}
		}
		return
	}
	if !m.allItemsCompleted(job.ID) {
		m.fail(job.ID, pending, errors.New("部分文件未收到收尾完成回调或移动失败"))
	} else {
		m.setJob(job.ID, "completed", "")
		_ = os.RemoveAll(tmpDir)
	}
}

func makeDirectPeer(peer tg.InputPeerClass) directPeer {
	switch value := peer.(type) {
	case *tg.InputPeerSelf:
		return directPeer{kind: "self"}
	case *tg.InputPeerUser:
		return directPeer{kind: "user", id: value.UserID, hash: value.AccessHash}
	case *tg.InputPeerChat:
		return directPeer{kind: "chat", id: value.ChatID}
	case *tg.InputPeerChannel:
		return directPeer{kind: "channel", id: value.ChannelID, hash: value.AccessHash}
	default:
		return directPeer{}
	}
}

func (j Job) directInputPeer() tg.InputPeerClass {
	switch j.DirectPeerType {
	case "self":
		return &tg.InputPeerSelf{}
	case "user":
		return &tg.InputPeerUser{UserID: j.DirectPeerID, AccessHash: j.DirectPeerHash}
	case "chat":
		return &tg.InputPeerChat{ChatID: j.DirectPeerID}
	case "channel":
		return &tg.InputPeerChannel{ChannelID: j.DirectPeerID, AccessHash: j.DirectPeerHash}
	default:
		return nil
	}
}

func dialogIdentity(peer tg.InputPeerClass, accountID string) (kind, key string, id int64) {
	switch value := peer.(type) {
	case *tg.InputPeerSelf:
		return "self", "self:" + accountID, 0
	case *tg.InputPeerUser:
		return "user", fmt.Sprintf("user:%d", value.UserID), value.UserID
	case *tg.InputPeerChat:
		return "chat", fmt.Sprintf("chat:%d", value.ChatID), value.ChatID
	case *tg.InputPeerChannel:
		return "channel", fmt.Sprintf("channel:%d", value.ChannelID), value.ChannelID
	default:
		return "unknown", "unknown:0", 0
	}
}

func isPublicMessageLink(value string) bool {
	return strings.HasPrefix(value, "https://t.me/") || strings.HasPrefix(value, "http://t.me/")
}

func (m *Manager) allItemsCompleted(jobID string) bool {
	var incomplete int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status != 'completed'`, jobID).Scan(&incomplete); err != nil {
		return false
	}
	return incomplete == 0
}

// publishItem moves a file only after upstream tdl has closed and finalized
// its temporary file. It runs outside the upstream download worker.
func (m *Manager) publishItem(jobID, path string, item source, config settings.Values) {
	if m.status(jobID) != "running" {
		return
	}
	finalPath, err := finalDestination(m.downloadDir, config.Download.FinalFilenameTemplate, item)
	if err != nil {
		m.setItem(item, "failed", "", err.Error())
		return
	}
	if _, statErr := os.Stat(finalPath); statErr == nil {
		m.setItem(item, "failed", "", "目标文件已存在，未覆盖")
		return
	}
	if err := publishNoReplace(path, finalPath); err != nil {
		if errors.Is(err, unix.EEXIST) {
			m.setItem(item, "failed", "", "目标文件已存在，未覆盖")
			return
		}
		m.setItem(item, "failed", "", err.Error())
		return
	}
	m.setItem(item, "completed", finalPath, "")
}

func (m *Manager) resolve(ctx context.Context, accountID, sourceURL string) ([]source, error) {
	var result []source
	err := m.accounts.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(client.API())
		peer, messageID, err := tutil.ParseMessageLink(ctx, manager, sourceURL)
		if err != nil {
			return err
		}
		message, err := tutil.GetSingleMessage(ctx, client.API(), peer.InputPeer(), messageID)
		if err != nil {
			return err
		}
		messages := []*tg.Message{message}
		groupedID := int64(0)
		if group, ok := message.GetGroupedID(); ok {
			groupedID = group
			messages, err = tutil.GetGroupedMessages(ctx, client.API(), peer.InputPeer(), message)
			if err != nil {
				return err
			}
		}
		messageText := groupDisplayText(messages)
		dialogType, dialogKey, dialogID := dialogIdentity(peer.InputPeer(), accountID)
		for _, msg := range messages {
			media, ok := tmedia.GetMedia(msg)
			if !ok {
				continue
			}
			result = append(result, source{Item: Item{DialogType: dialogType, DialogKey: dialogKey, DialogID: dialogID, MessageID: msg.ID, GroupedID: groupedID, MessageText: messageText, OriginalName: media.Name, Size: media.Size}, DialogName: peer.VisibleName()})
		}
		return nil
	})
	return result, err
}

func (m *Manager) resolvePeer(ctx context.Context, accountID string, inputPeer tg.InputPeerClass, dialogID int64, messageID int, dialogName string) ([]source, error) {
	var result []source
	err := m.accounts.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
		message, err := tutil.GetSingleMessage(ctx, client.API(), inputPeer, messageID)
		if err != nil {
			return err
		}
		messages := []*tg.Message{message}
		groupedID := int64(0)
		if group, ok := message.GetGroupedID(); ok {
			groupedID = group
			messages, err = tutil.GetGroupedMessages(ctx, client.API(), inputPeer, message)
			if err != nil {
				return err
			}
		}
		messageText := groupDisplayText(messages)
		dialogType, dialogKey, resolvedDialogID := dialogIdentity(inputPeer, accountID)
		if resolvedDialogID == 0 && dialogID != 0 {
			resolvedDialogID = dialogID
		}
		for _, msg := range messages {
			media, ok := tmedia.GetMedia(msg)
			if !ok {
				continue
			}
			result = append(result, source{Item: Item{DialogType: dialogType, DialogKey: dialogKey, DialogID: resolvedDialogID, MessageID: msg.ID, GroupedID: groupedID, MessageText: messageText, OriginalName: media.Name, Size: media.Size}, DialogName: dialogName})
		}
		return nil
	})
	return result, err
}

func groupDisplayText(messages []*tg.Message) string {
	nonEmpty := ""
	count := 0
	for _, message := range messages {
		if strings.TrimSpace(message.Message) != "" {
			count++
			nonEmpty = message.Message
		}
	}
	if count == 1 {
		return nonEmpty
	}
	return ""
}

func (m *Manager) items(jobID string) ([]Item, error) {
	rows, err := m.db.Query(`SELECT id, dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, original_name, size, final_path, started_at, finished_at, elapsed_ms, attempts, status, error FROM download_items WHERE job_id = ? ORDER BY id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Item, 0)
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.ID, &item.DialogType, &item.DialogKey, &item.DialogID, &item.MessageID, &item.GroupedID, &item.MessageText, &item.OriginalName, &item.Size, &item.FinalPath, &item.StartedAt, &item.FinishedAt, &item.ElapsedMS, &item.Attempts, &item.Status, &item.Error); err != nil {
			return nil, err
		}
		// Older records predate the size column. When their final file still
		// exists, expose its actual size without requiring a database reset.
		if item.Size == 0 && item.FinalPath != "" {
			if info, err := os.Stat(item.FinalPath); err == nil && info.Mode().IsRegular() {
				item.Size = info.Size()
			}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (m *Manager) sources(jobID string) ([]source, error) {
	var dialogName string
	if err := m.db.QueryRow(`SELECT dialog_name FROM download_jobs WHERE id = ?`, jobID).Scan(&dialogName); err != nil {
		return nil, err
	}
	items, err := m.items(jobID)
	if err != nil {
		return nil, err
	}
	result := make([]source, 0, len(items))
	for _, item := range items {
		result = append(result, source{Item: item, DialogName: dialogName})
	}
	return result, nil
}
func (m *Manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}
func (m *Manager) status(id string) string {
	var status string
	_ = m.db.QueryRow(`SELECT status FROM download_jobs WHERE id = ?`, id).Scan(&status)
	return status
}
func (m *Manager) itemStatus(dialogKey string, messageID int) string {
	var status string
	_ = m.db.QueryRow(`SELECT status FROM download_items WHERE dialog_key = ? AND message_id = ?`, dialogKey, messageID).Scan(&status)
	return status
}

func (m *Manager) Pause(id string) error {
	status := m.status(id)
	if status == "" {
		return errors.New("下载任务不存在")
	}
	if status != "queued" && status != "running" {
		return errors.New("当前任务不能暂停")
	}
	m.setJob(id, "paused", "已暂停，可继续恢复")
	if status == "queued" {
		_, _ = m.db.Exec(`UPDATE download_items SET status = 'paused' WHERE job_id = ? AND status != 'completed'`, id)
		m.touch()
		return nil
	}
	m.mu.Lock()
	cancel := m.cancels[id]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
func (m *Manager) Resume(id string) error {
	if m.status(id) != "paused" {
		return errors.New("当前任务不能恢复")
	}
	m.setJob(id, "queued", "")
	_, _ = m.db.Exec(`UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE job_id = ? AND status != 'completed'`, id)
	m.touch()
	m.signal()
	return nil
}
func (m *Manager) Retry(id string) error {
	status := m.status(id)
	if status != "failed" && status != "partial" {
		return errors.New("只有失败或部分完成任务可以重试")
	}
	var accountID string
	if err := m.db.QueryRow(`SELECT account_id FROM download_jobs WHERE id = ?`, id).Scan(&accountID); err != nil {
		return errors.New("下载任务不存在")
	}
	if accountID == "" {
		return errors.New("该任务创建于账户绑定功能启用前，无法安全重试，请重新创建下载任务")
	}
	// Attempts count real worker runs, not clicks. The worker increments it only
	// after atomically claiming this queued task.
	_, _ = m.db.Exec(`UPDATE download_jobs SET status = 'queued', error = '', updated_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339), id)
	_, _ = m.db.Exec(`UPDATE download_items SET status = 'queued', error = '' WHERE job_id = ? AND status != 'completed'`, id)
	m.touch()
	m.emit(id, "", "job_status_changed", "queued")
	m.signal()
	return nil
}
func (m *Manager) Cancel(id string) error {
	status := m.status(id)
	if status != "queued" && status != "running" && status != "paused" {
		return errors.New("当前任务不能取消")
	}
	m.setJob(id, "cancelled", "已取消；临时文件保留，可手动清理")
	_, _ = m.db.Exec(`UPDATE download_items SET status = 'cancelled', finished_at = ? WHERE job_id = ? AND status != 'completed'`, time.Now().UTC().Format(time.RFC3339Nano), id)
	m.touch()
	m.mu.Lock()
	cancel := m.cancels[id]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// Delete removes the task record and only its temporary directory. Final files
// are intentionally never removed. A terminal task is required so a worker
// cannot still be writing files while its record is being removed.
func (m *Manager) Delete(id string) error {
	status := m.status(id)
	if status != "completed" && status != "failed" && status != "partial" && status != "cancelled" {
		return errors.New("请先取消或等待任务结束后再删除")
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var accountID, sourceURL string
	if err := tx.QueryRow(`SELECT account_id, source_url FROM download_jobs WHERE id = ?`, id).Scan(&accountID, &sourceURL); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("下载任务不存在")
		}
		return err
	}
	if accountID != "" {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO download_resets(account_id, source_url) VALUES (?, ?)`, accountID, sourceURL); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM download_items WHERE job_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM download_jobs WHERE id = ?`, id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	if err := os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", id)); err != nil {
		return fmt.Errorf("任务记录已删除，但清理临时目录失败: %w", err)
	}
	return nil
}

func (m *Manager) consumeRestart(accountID, sourceURL string) bool {
	tx, err := m.db.Begin()
	if err != nil {
		return false
	}
	defer tx.Rollback()
	var found int
	if err := tx.QueryRow(`SELECT COUNT(1) FROM download_resets WHERE account_id = ? AND source_url = ?`, accountID, sourceURL).Scan(&found); err != nil || found == 0 {
		return false
	}
	if _, err := tx.Exec(`DELETE FROM download_resets WHERE account_id = ? AND source_url = ?`, accountID, sourceURL); err != nil {
		return false
	}
	return tx.Commit() == nil
}
func (m *Manager) setJob(id, status, message string) {
	_, _ = m.db.Exec(`UPDATE download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?`, status, message, time.Now().UTC().Format(time.RFC3339), id)
	m.touch()
	m.emit(id, "", "job_status_changed", status)
}
func (m *Manager) setItem(item source, status, path, message string) {
	finishedAt := ""
	if status == "completed" || status == "failed" || status == "cancelled" {
		finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, _ = m.db.Exec(`UPDATE download_items SET status = ?, final_path = ?, error = ?, finished_at = CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE dialog_key = ? AND message_id = ?`, status, path, message, finishedAt, finishedAt, item.DialogKey, item.MessageID)
	m.touch()
}
func (m *Manager) beginItemAttempt(item source) {
	// A task may be running while this particular file is still waiting for an
	// upstream worker slot. It becomes "running" only on its first byte-level
	// progress callback.
	_, _ = m.db.Exec(`UPDATE download_items SET attempts = attempts + 1, status = 'queued', error = '', started_at = '', finished_at = '' WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID)
	m.touch()
}
func (m *Manager) pauseItem(item source) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, _ = m.db.Exec(`UPDATE download_items SET status = 'paused', error = '', elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN CAST((julianday(?) - julianday(started_at)) * 86400000 AS INTEGER) ELSE 0 END, started_at = '', finished_at = '' WHERE dialog_key = ? AND message_id = ? AND status IN ('queued', 'running')`, now, item.DialogKey, item.MessageID)
	m.touch()
}
func (m *Manager) markItemStarted(item source) {
	_, _ = m.db.Exec(`UPDATE download_items SET status = 'running', started_at = ? WHERE dialog_key = ? AND message_id = ? AND status = 'queued' AND started_at = ''`, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID)
	m.touch()
}
func (m *Manager) markItemFinished(item source) {
	_, _ = m.db.Exec(`UPDATE download_items SET status = 'downloaded', finished_at = ? WHERE dialog_key = ? AND message_id = ? AND status = 'running' AND started_at != '' AND finished_at = ''`, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID)
	m.touch()
}
func (m *Manager) fail(id string, sources []source, err error) {
	for _, item := range sources {
		if m.itemStatus(item.DialogKey, item.MessageID) == "completed" {
			continue
		}
		m.setItem(item, "failed", "", err.Error())
	}
	var completed int
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status = 'completed'`, id).Scan(&completed)
	if completed > 0 {
		m.setJob(id, "partial", "部分文件未完成，可重试失败文件")
		return
	}
	m.setJob(id, "failed", err.Error())
}

func renderName(pattern string, item source) (string, error) {
	data := struct {
		DialogID, GroupedID     int64
		MessageID               int
		DialogName, MessageText string
		FileName, FileExt       string
		DownloadDate            int64
	}{item.DialogID, item.GroupedID, item.MessageID, sanitizeComponent(item.DialogName), sanitizeComponent(item.MessageText), sanitizeComponent(item.OriginalName), sanitizeComponent(filepath.Ext(item.OriginalName)), time.Now().Unix()}
	tpl, err := template.New("filename").Funcs(template.FuncMap{
		"formatDate": func(timestamp int64, layout string) string {
			return time.Unix(timestamp, 0).Format(layout)
		},
	}).Parse(pattern)
	if err != nil {
		return "", fmt.Errorf("解析文件命名模板: %w", err)
	}
	var out bytes.Buffer
	if err = tpl.Execute(&out, data); err != nil {
		return "", err
	}
	return normalizeRelativePath(out.String())
}

// finalDestination renders the complete final relative path and validates it
// against the destination directory before a file is moved. Only a length
// error is recoverable by regenerating a shorter name; a missing directory or
// unsafe template must remain visible to the administrator.
func finalDestination(root, pattern string, item source) (string, error) {
	path, err := fitMessageText(root, pattern, item)
	if err == nil {
		return path, nil
	}
	if !errors.Is(err, errFinalNameTooLong) {
		return "", err
	}

	// A long upstream filename can still overflow after MessageText has been
	// removed. Shorten it while retaining its extension, then use as much of
	// MessageText as the complete final path permits.
	originalName := item.OriginalName
	low, high := 0, len([]byte(originalName))
	var shortened source
	found := false
	for low <= high {
		budget := low + (high-low)/2
		candidate := item
		candidate.MessageText = ""
		candidate.OriginalName = ellipsizeFileName(originalName, budget)
		_, checkErr := checkedFinalPath(root, pattern, candidate)
		if checkErr == nil {
			shortened, found = candidate, true
			low = budget + 1
			continue
		}
		if !errors.Is(checkErr, errFinalNameTooLong) {
			return "", checkErr
		}
		high = budget - 1
	}
	if !found {
		return "", err
	}
	return fitMessageText(root, pattern, shortened)
}

// fitMessageText keeps as much client-visible message text as possible. When
// it must shrink, the middle is replaced by a single ellipsis, retaining both
// the beginning and end of the original caption.
func fitMessageText(root, pattern string, item source) (string, error) {
	path, err := checkedFinalPath(root, pattern, item)
	if err == nil || !errors.Is(err, errFinalNameTooLong) || item.MessageText == "" {
		return path, err
	}

	original := item.MessageText
	low, high := 0, len([]byte(original))
	best := ""
	for low <= high {
		budget := low + (high-low)/2
		candidate := item
		candidate.MessageText = ellipsizeMiddle(original, budget)
		path, checkErr := checkedFinalPath(root, pattern, candidate)
		if checkErr == nil {
			best = path
			low = budget + 1
			continue
		}
		if !errors.Is(checkErr, errFinalNameTooLong) {
			return "", checkErr
		}
		high = budget - 1
	}
	if best == "" {
		return "", err
	}
	return best, nil
}

func checkedFinalPath(root, pattern string, item source) (string, error) {
	relative, err := renderName(pattern, item)
	if err != nil {
		return "", err
	}
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if len([]byte(component)) > linuxNameMaxBytes {
			return "", fmt.Errorf("%w：单个路径段最多 %d 字节", errFinalNameTooLong, linuxNameMaxBytes)
		}
	}
	finalPath := filepath.Join(root, relative)
	if len([]byte(finalPath)) >= linuxPathMaxBytes {
		return "", fmt.Errorf("%w：完整路径最多 %d 字节", errFinalNameTooLong, linuxPathMaxBytes-1)
	}
	if err := ensureFinalDirectory(root, relative); err != nil {
		return "", err
	}
	return finalPath, nil
}

// ensureFinalDirectory creates directories from a relative final template one
// level at a time. Checking each existing symlink before continuing prevents a
// template such as "link/subdir/file" from creating files outside downloads.
func ensureFinalDirectory(root, relative string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("无法创建下载根目录：%w", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("无法解析下载根目录：%w", err)
	}
	parts := strings.Split(relative, string(filepath.Separator))
	directory := root
	for _, part := range parts[:len(parts)-1] {
		directory = filepath.Join(directory, part)
		info, err := os.Lstat(directory)
		if os.IsNotExist(err) {
			if err := os.Mkdir(directory, 0o755); err != nil {
				return fmt.Errorf("无法创建最终目录：%w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("无法访问最终目录：%w", err)
		}
		if info.Mode()&os.ModeSymlink == 0 && !info.IsDir() {
			return fmt.Errorf("最终路径的父级不是目录：%s", directory)
		}
		resolvedDirectory, err := filepath.EvalSymlinks(directory)
		if err != nil {
			return fmt.Errorf("无法解析最终目录：%w", err)
		}
		relativeDirectory, err := filepath.Rel(resolvedRoot, resolvedDirectory)
		if err != nil || relativeDirectory == ".." || strings.HasPrefix(relativeDirectory, ".."+string(filepath.Separator)) || filepath.IsAbs(relativeDirectory) {
			return errors.New("最终目录不能通过符号链接指向下载目录外")
		}
	}
	return nil
}

func normalizeRelativePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || filepath.IsAbs(raw) {
		return "", errors.New("文件命名模板生成了空路径或绝对路径")
	}
	parts := strings.Split(raw, "/")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("文件命名模板包含不安全路径段")
		}
		part = sanitizeComponent(part)
		if part == "" || part == "." || part == ".." {
			return "", errors.New("文件命名模板生成了无效路径段")
		}
		cleaned = append(cleaned, part)
	}
	return filepath.Join(cleaned...), nil
}

func sanitizeComponent(input string) string {
	return strings.Map(func(r rune) rune {
		// Linux filenames only prohibit slash and NUL. Control characters are
		// additionally replaced because they make terminals and web UIs unsafe
		// or unreadable; punctuation such as : ? * < > | remains unchanged.
		if r == '/' || r == 0 || unicode.IsControl(r) {
			return '_'
		}
		return r
	}, strings.TrimSpace(input))
}

func ellipsizeMiddle(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(input)) <= maxBytes {
		return input
	}
	const ellipsis = "…"
	if maxBytes <= len(ellipsis) {
		return ""
	}
	remaining := maxBytes - len(ellipsis)
	left := truncateUTF8(input, (remaining+1)/2)
	right := truncateUTF8FromEnd(input, remaining-len([]byte(left)))
	return left + ellipsis + right
}

func ellipsizeFileName(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(input)) <= maxBytes {
		return input
	}
	extension := filepath.Ext(input)
	if len([]byte(extension)) >= maxBytes {
		return truncateUTF8(extension, maxBytes)
	}
	stem := strings.TrimSuffix(input, extension)
	return ellipsizeMiddle(stem, maxBytes-len([]byte(extension))) + extension
}

func truncateUTF8(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	end := 0
	for end < len(input) {
		_, size := utf8.DecodeRuneInString(input[end:])
		if end+size > maxBytes {
			break
		}
		end += size
	}
	return input[:end]
}

func truncateUTF8FromEnd(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	start := len(input)
	for start > 0 {
		_, size := utf8.DecodeLastRuneInString(input[:start])
		if len(input)-start+size > maxBytes {
			break
		}
		start -= size
	}
	return input[start:]
}

func publishNoReplace(source, destination string) error {
	err := unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, unix.RENAME_NOREPLACE)
	if err == nil || !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EOPNOTSUPP) {
		return err
	}
	// All supported deployment filesystems handle hard links. This fallback is
	// also atomic with respect to an already existing destination and never
	// overwrites it. Source and destination are both below /downloads.
	if err := os.Link(source, destination); err != nil {
		return err
	}
	return os.Remove(source)
}
func randomID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", data), nil
}
