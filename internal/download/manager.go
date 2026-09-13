package download

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
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
	"github.com/iyear/tdl/pkg/tmessage"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
	"golang.org/x/sys/unix"
)

var (
	errFinalNameTooLong = errors.New("最终文件名或完整路径超过 Linux 长度限制")
)

const (
	linuxNameMaxBytes = 255
	linuxPathMaxBytes = 4096
	workerCount       = 16
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
	ConfigJSON     string
	Items          []Item `json:"items"`
}

// BotMessageRef identifies one Bot lifecycle message. It is persisted so a
// container restart does not make a later job transition create a duplicate
// notification instead of editing the original message.
type BotMessageRef struct {
	ChatID    int64
	MessageID int64
	Text      string
	TokenHash string
}

type source struct {
	Item
	DialogName string
	MediaType  string
}
type directPeer struct {
	kind     string
	id, hash int64
}

const bytesPerMiB int64 = 1024 * 1024

// filterSources applies the global download policy before any task or chat
// index row is created. An empty allowed-type list and zero size limits mean
// unrestricted, preserving the default behavior.
func filterSources(sources []source, config settings.Download) []source {
	allowed := make(map[string]struct{}, len(config.FileTypes))
	for _, kind := range config.FileTypes {
		allowed[kind] = struct{}{}
	}
	minBytes := config.MinFileSizeMB * bytesPerMiB
	maxBytes := config.MaxFileSizeMB * bytesPerMiB
	filtered := make([]source, 0, len(sources))
	for _, item := range sources {
		if minBytes > 0 && item.Size < minBytes {
			continue
		}
		if maxBytes > 0 && item.Size > maxBytes {
			continue
		}
		if len(allowed) > 0 {
			if _, ok := allowed[item.MediaType]; !ok {
				continue
			}
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func messageMediaType(message *tg.Message) string {
	media, ok := message.GetMedia()
	if !ok {
		return "document"
	}
	switch value := media.(type) {
	case *tg.MessageMediaPhoto:
		return "image"
	case *tg.MessageMediaDocument:
		document, ok := value.Document.(*tg.Document)
		if !ok {
			return "document"
		}
		mime := strings.ToLower(document.MimeType)
		switch {
		case strings.HasPrefix(mime, "image/"):
			return "image"
		case strings.HasPrefix(mime, "video/"):
			return "video"
		case strings.HasPrefix(mime, "audio/"):
			return "audio"
		default:
			return "document"
		}
	default:
		return "document"
	}
}

type Manager struct {
	db            *database
	instanceID    string
	downloadDir   string
	settings      *settings.Store
	accounts      *telegram.Manager
	mu            sync.Mutex
	cancels       map[string]context.CancelFunc
	wake          chan struct{}
	chatWake      chan struct{}
	chatEvents    chan telegram.NewMessageEvent
	chatListeners map[string]*chatListener
	slotWake      chan struct{}
	progress      *progressStore
	events        *eventBus
	slotMu        sync.Mutex
	activeJobs    int
	jobLocks      [64]sync.Mutex
	revision      atomic.Uint64
	dbHealthMu    sync.RWMutex
	dbHealth      DatabaseHealth
	dbMonitorStop context.CancelFunc
	dbOutage      atomic.Bool
}

type DatabaseHealth struct {
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	CheckedAt string `json:"checkedAt"`
}

func Open(dataDir, downloadDir, databaseURL string, store *settings.Store, accounts *telegram.Manager) (*Manager, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	db, err := openDatabase(context.Background(), databaseURL)
	if err != nil {
		return nil, err
	}
	m := &Manager{db: db, downloadDir: downloadDir, settings: store, accounts: accounts, cancels: make(map[string]context.CancelFunc), wake: make(chan struct{}, workerCount), chatWake: make(chan struct{}, 1), chatEvents: make(chan telegram.NewMessageEvent, 512), chatListeners: make(map[string]*chatListener), slotWake: make(chan struct{}, 1), progress: newProgressStore(), events: newEventBus()}
	if err := m.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.loadInstanceID(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.reconcilePublishedItems(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("reconcile published files: %w", err)
	}
	// Retain the upstream temporary files and resume keys. A worker will resume
	// these jobs using the account originally selected at enqueue time.
	if _, err := m.db.Exec(`UPDATE download_jobs SET status = 'queued', error = '服务重启，任务等待恢复', updated_at = ? WHERE status = 'running'`, time.Now().UTC().Format(time.RFC3339)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover running tasks: %w", err)
	}
	if _, err := m.db.Exec(`UPDATE chat_download_jobs SET status = 'queued', error = '', updated_at = ? WHERE status = 'scanning'`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover chat indexing tasks: %w", err)
	}
	// Jobs created before account binding was introduced cannot safely be resumed:
	// selecting whichever account happens to be current could download under the
	// wrong Telegram identity. Keep their records visible, but require a new job.
	if _, err := m.db.Exec(`UPDATE download_jobs SET status = 'failed', error = '旧任务缺少 Telegram 账户绑定，无法安全重试，请重新创建下载任务', updated_at = ? WHERE status NOT IN ('completed', 'cancelled') AND account_id = ''`, time.Now().UTC().Format(time.RFC3339)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover legacy tasks: %w", err)
	}
	// This process is the only inbox consumer. Any lease left by a previous
	// process is safe to recover immediately; waiting for an arbitrary age here
	// would leave a just-claimed event permanently stuck after a restart.
	if _, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', next_attempt_at = ?, updated_at = ? WHERE status = 'processing'`, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover reaction inbox: %w", err)
	}
	for worker := 0; worker < workerCount; worker++ {
		go m.worker()
	}
	go m.chatWorker()
	go m.chatEventWorker()
	go m.cleanupLoop()
	monitorCtx, cancelMonitor := context.WithCancel(context.Background())
	m.dbMonitorStop = cancelMonitor
	m.updateDatabaseHealth()
	go m.databaseMonitor(monitorCtx)
	return m, nil
}

func (m *Manager) databaseMonitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.updateDatabaseHealth()
		}
	}
}

func (m *Manager) updateDatabaseHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := m.db.PingContext(ctx)
	cancel()
	health := DatabaseHealth{Status: "connected", CheckedAt: time.Now().UTC().Format(time.RFC3339)}
	if err != nil {
		health.Status, health.Error = "unavailable", "数据库暂时不可用，服务将自动重连"
		if !m.dbOutage.Swap(true) {
			applog.Error("database", "connection_lost", "error", err.Error())
			m.cancelTransfersForDatabaseOutage()
		}
	} else if m.dbOutage.Swap(false) {
		if recoverErr := m.recoverAfterDatabaseOutage(); recoverErr != nil {
			health.Status, health.Error = "unavailable", "数据库已连接，任务状态恢复中"
			m.dbOutage.Store(true)
			applog.Error("database", "recovery_failed", "error", recoverErr.Error())
		} else {
			applog.Info("database", "connection_recovered")
		}
	}
	m.dbHealthMu.Lock()
	m.dbHealth = health
	m.dbHealthMu.Unlock()
}

func (m *Manager) cancelTransfersForDatabaseOutage() {
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.cancels))
	for _, cancel := range m.cancels {
		cancels = append(cancels, cancel)
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (m *Manager) recoverAfterDatabaseOutage() error {
	if err := m.reconcilePublishedItems(); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := m.db.Exec(`UPDATE download_items SET status = 'queued', started_at = '', finished_at = '' WHERE status = 'running'`); err != nil {
		return err
	}
	if _, err := m.db.Exec(`UPDATE download_jobs SET status = 'queued', error = '数据库连接恢复，任务等待继续', updated_at = ? WHERE status = 'running'`, now); err != nil {
		return err
	}
	m.touch()
	for worker := 0; worker < workerCount; worker++ {
		m.signal()
	}
	return nil
}

func (m *Manager) DatabaseHealth() DatabaseHealth {
	m.dbHealthMu.RLock()
	defer m.dbHealthMu.RUnlock()
	return m.dbHealth
}

func (m *Manager) DatabaseAvailable() bool {
	return !m.dbOutage.Load() && m.DatabaseHealth().Status == "connected"
}

func (m *Manager) migrate() error { return m.migratePostgres() }

// migrateSQLiteLegacy is retained only to support the isolated SQLite unit
// fixtures while the production application uses PostgreSQL exclusively.
func (m *Manager) migrateSQLiteLegacy() error {
	_, err := m.db.Exec(`
CREATE TABLE IF NOT EXISTS download_jobs (
 id TEXT PRIMARY KEY, source_url TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL DEFAULT '', dialog_name TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '', direct_peer_type TEXT NOT NULL DEFAULT '', direct_peer_id INTEGER NOT NULL DEFAULT 0, direct_peer_hash INTEGER NOT NULL DEFAULT 0, parent_chat_id TEXT NOT NULL DEFAULT '', config_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
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
	token_hash TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(job_id, chat_id),
 FOREIGN KEY(job_id) REFERENCES download_jobs(id) ON DELETE CASCADE
)`)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(`CREATE TABLE IF NOT EXISTS app_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(`CREATE TABLE IF NOT EXISTS download_resets (
 account_id TEXT NOT NULL, source_url TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '',
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
	_, err = m.db.Exec(`
CREATE TABLE IF NOT EXISTS reaction_inbox (
 id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL, dialog_key TEXT NOT NULL, dialog_name TEXT NOT NULL DEFAULT '', dialog_id INTEGER NOT NULL DEFAULT 0, message_id INTEGER NOT NULL,
 source_url TEXT NOT NULL DEFAULT '', peer_type TEXT NOT NULL, peer_id INTEGER NOT NULL DEFAULT 0, peer_hash INTEGER NOT NULL DEFAULT 0, emoji TEXT NOT NULL,
 status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(account_id, dialog_key, message_id, emoji)
);
CREATE INDEX IF NOT EXISTS reaction_inbox_ready ON reaction_inbox(status, next_attempt_at, id);`)
	if err != nil {
		return err
	}
	// Chat download jobs are deliberately separate from the existing message-job
	// table. A chat can contain millions of media records, so its lifecycle must
	// not turn the normal message task list into millions of rows.
	_, err = m.db.Exec(`
CREATE TABLE IF NOT EXISTS chat_download_jobs (
 id TEXT PRIMARY KEY, source_url TEXT NOT NULL, dialog_type TEXT NOT NULL, dialog_key TEXT NOT NULL, dialog_id INTEGER NOT NULL,
 dialog_name TEXT NOT NULL, account_id TEXT NOT NULL, direct_peer_type TEXT NOT NULL DEFAULT '', direct_peer_id INTEGER NOT NULL DEFAULT 0,
 direct_peer_hash INTEGER NOT NULL DEFAULT 0, start_message_id INTEGER NOT NULL DEFAULT 0, upper_message_id INTEGER NOT NULL DEFAULT 0,
 listen_new INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, scan_state TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', config_json TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS chat_download_jobs_created_id ON chat_download_jobs(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS chat_download_jobs_account_status ON chat_download_jobs(account_id, status, updated_at);
CREATE TABLE IF NOT EXISTS chat_download_items (
 chat_job_id TEXT NOT NULL, dialog_key TEXT NOT NULL, message_id INTEGER NOT NULL, child_job_id TEXT NOT NULL DEFAULT '',
 dialog_type TEXT NOT NULL DEFAULT '', dialog_id INTEGER NOT NULL DEFAULT 0, grouped_id INTEGER NOT NULL DEFAULT 0, message_text TEXT NOT NULL DEFAULT '', original_name TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
 discovered_at TEXT NOT NULL, PRIMARY KEY(chat_job_id, dialog_key, message_id),
 FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS chat_download_items_child_job ON chat_download_items(child_job_id);
CREATE TABLE IF NOT EXISTS chat_download_streams (
 chat_job_id TEXT NOT NULL, stream_kind TEXT NOT NULL, offset_message_id INTEGER NOT NULL DEFAULT 0,
 initialized INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(chat_job_id, stream_kind),
 FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS chat_download_jobs_scan ON chat_download_jobs(status, scan_state, created_at);
CREATE INDEX IF NOT EXISTS chat_download_jobs_listener ON chat_download_jobs(account_id, dialog_key, listen_new, scan_state, status);
CREATE INDEX IF NOT EXISTS chat_download_jobs_active_target ON chat_download_jobs(account_id, dialog_key, start_message_id, status);
CREATE INDEX IF NOT EXISTS chat_download_items_job ON chat_download_items(chat_job_id);`)
	if err != nil {
		return err
	}
	// Existing databases get the same columns without a data reset. Duplicate
	// column errors are expected; every other migration failure must stop
	// startup instead of leaving a partially upgraded schema running.
	for _, statement := range []string{
		`ALTER TABLE download_jobs ADD COLUMN account_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_jobs ADD COLUMN dialog_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE download_jobs ADD COLUMN direct_peer_type TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_jobs ADD COLUMN direct_peer_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE download_jobs ADD COLUMN direct_peer_hash INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE download_jobs ADD COLUMN dialog_type TEXT NOT NULL DEFAULT 'legacy'`,
		`ALTER TABLE download_jobs ADD COLUMN dialog_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_jobs ADD COLUMN config_json TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_jobs ADD COLUMN parent_chat_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE chat_download_items ADD COLUMN dialog_type TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE chat_download_items ADD COLUMN dialog_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE chat_download_items ADD COLUMN grouped_id INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE chat_download_items ADD COLUMN message_text TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE chat_download_items ADD COLUMN original_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE chat_download_items ADD COLUMN size INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE download_items ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE download_items ADD COLUMN dialog_type TEXT NOT NULL DEFAULT 'legacy'`,
		`ALTER TABLE download_items ADD COLUMN dialog_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_items ADD COLUMN size INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE download_items ADD COLUMN started_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_items ADD COLUMN finished_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_items ADD COLUMN elapsed_ms INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE bot_lifecycle_messages ADD COLUMN message_text TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE bot_lifecycle_messages ADD COLUMN token_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE download_resets ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`,
	} {
		if err := m.addColumn(statement); err != nil {
			return err
		}
	}
	if _, err := m.db.Exec(`UPDATE download_resets SET created_at = ? WHERE created_at = ''`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	for _, statement := range []string{
		`CREATE INDEX IF NOT EXISTS download_jobs_created_id ON download_jobs(created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_status_updated_id ON download_jobs(status, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_parent_created_id ON download_jobs(parent_chat_id, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS download_items_status ON download_items(status)`,
	} {
		if _, err := m.db.Exec(statement); err != nil {
			return err
		}
	}
	if err := m.migrateItemIdentity(); err != nil {
		return err
	}
	// Existing databases may already contain chat jobs created before a new
	// media stream was introduced. Add the missing durable cursors without
	// disturbing streams that have already completed.
	for _, kind := range chatStreamKinds {
		statement := `INSERT INTO chat_download_streams(chat_job_id, stream_kind) SELECT id, ? FROM chat_download_jobs ON CONFLICT(chat_job_id, stream_kind) DO NOTHING`
		if !m.db.isPostgres() {
			statement = `INSERT OR IGNORE INTO chat_download_streams(chat_job_id, stream_kind) SELECT id, ? FROM chat_download_jobs`
		}
		result, err := m.db.Exec(statement, kind)
		if err != nil {
			return err
		}
		// Only a schema upgrade can insert a new stream for an already-complete
		// chat. Queue that parent again so the newly supported media class is
		// actually indexed; paused/cancelled/failed tasks retain their explicit
		// user state and will pick it up on resume/retry instead.
		if added, _ := result.RowsAffected(); added > 0 {
			if _, err := m.db.Exec(`UPDATE chat_download_jobs
 SET scan_state = ?, status = CASE WHEN status IN (?, ?, ?) THEN status ELSE ? END,
     error = CASE WHEN status IN (?, ?, ?) THEN error ELSE '' END,
     updated_at = ?
 WHERE scan_state = ? AND id IN (
   SELECT chat_job_id FROM chat_download_streams WHERE stream_kind = ? AND completed = 0
 )`, chatScanPending, ChatStatusPaused, ChatStatusCancelled, ChatStatusFailed, ChatStatusQueued, ChatStatusPaused, ChatStatusCancelled, ChatStatusFailed, time.Now().UTC().Format(time.RFC3339Nano), chatScanCompleted, kind); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) addColumn(statement string) error {
	if _, err := m.db.Exec(statement); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		return fmt.Errorf("migrate database: %w", err)
	}
	return nil
}

// InstanceID changes only when the task database is replaced. Delivery
// adapters use it to distinguish a fresh installation from a normal restart.
func (m *Manager) InstanceID() string { return m.instanceID }

func (m *Manager) loadInstanceID() error {
	var value string
	err := m.db.QueryRow(`SELECT value FROM app_metadata WHERE key = 'instance_id'`).Scan(&value)
	if err == nil {
		m.instanceID = value
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	value, err = randomID()
	if err != nil {
		return err
	}
	if _, err = m.db.Exec(`INSERT INTO app_metadata(key, value) VALUES ('instance_id', ?)`, value); err != nil {
		return err
	}
	m.instanceID = value
	return nil
}

// migrateItemIdentity replaces the old numeric-only unique key. Telegram user,
// basic-chat and channel IDs live in different namespaces, so dialog_id alone
// is not a safe cross-dialog identity.
func (m *Manager) migrateItemIdentity() error {
	rows, err := m.db.Query(`PRAGMA index_list(download_items)`)
	if err != nil {
		return err
	}
	hasIdentityIndex := false
	for rows.Next() {
		var sequence int
		var name, origin string
		var unique, partial int
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			_ = rows.Close()
			return err
		}
		if unique == 0 {
			continue
		}
		columns, err := m.db.Query(`PRAGMA index_info(` + name + `)`)
		if err != nil {
			_ = rows.Close()
			return err
		}
		var indexed []string
		for columns.Next() {
			var position, columnID int
			var column string
			if err := columns.Scan(&position, &columnID, &column); err != nil {
				_ = columns.Close()
				_ = rows.Close()
				return err
			}
			indexed = append(indexed, column)
		}
		if err := columns.Close(); err != nil {
			_ = rows.Close()
			return err
		}
		if len(indexed) == 2 && indexed[0] == "dialog_key" && indexed[1] == "message_id" {
			hasIdentityIndex = true
			break
		}
	}
	if err := rows.Close(); err != nil || hasIdentityIndex {
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
 SELECT id, job_id, CASE WHEN dialog_type = '' THEN 'legacy' ELSE dialog_type END, CASE WHEN dialog_key = '' THEN 'legacy:' || dialog_id ELSE dialog_key END, dialog_id, message_id, grouped_id, message_text, original_name, size, final_path, started_at, finished_at, elapsed_ms, attempts, status, error FROM download_items`); err != nil {
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
	if _, err = tx.Exec(`CREATE INDEX download_items_status ON download_items(status)`); err != nil {
		return err
	}
	return tx.Commit()
}

// SaveBotLifecycleMessage stores the one lifecycle card shown to a Bot user.
func (m *Manager) SaveBotLifecycleMessage(jobID string, ref BotMessageRef) error {
	_, err := m.db.Exec(`INSERT INTO bot_lifecycle_messages(job_id, chat_id, message_id, message_text, token_hash) VALUES (?, ?, ?, ?, ?) ON CONFLICT(job_id, chat_id) DO UPDATE SET message_id = EXCLUDED.message_id, message_text = EXCLUDED.message_text, token_hash = EXCLUDED.token_hash`, jobID, ref.ChatID, ref.MessageID, ref.Text, ref.TokenHash)
	return err
}

func (m *Manager) RemoveBotLifecycleMessage(jobID string, chatID int64) error {
	_, err := m.db.Exec(`DELETE FROM bot_lifecycle_messages WHERE job_id = ? AND chat_id = ?`, jobID, chatID)
	return err
}

// ClearBotLifecycleMessages detaches cards created by a previous Bot token.
// Telegram Bot API messages belong to the token that created them; retaining
// those references would otherwise cause failed edits after the token changes.
func (m *Manager) ClearBotLifecycleMessages() error {
	_, err := m.db.Exec(`DELETE FROM bot_lifecycle_messages`)
	return err
}

// BotLifecycleMessages returns all lifecycle cards for a task.
func (m *Manager) BotLifecycleMessages(jobID string) ([]BotMessageRef, error) {
	rows, err := m.db.Query(`SELECT chat_id, message_id, message_text, token_hash FROM bot_lifecycle_messages WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]BotMessageRef, 0)
	for rows.Next() {
		var ref BotMessageRef
		if err := rows.Scan(&ref.ChatID, &ref.MessageID, &ref.Text, &ref.TokenHash); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
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

// ListPage is retained for Bot page navigation. The Web UI uses ListCursor;
// Bot callbacks carry only a human page number, so use one bounded SQL query
// rather than replaying every earlier page for a deep page request.
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
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE parent_chat_id = '' AND status != 'deleted'`).Scan(&total); err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []Job{}, 0, nil
	}
	pages := (total + pageSize - 1) / pageSize
	if page > pages {
		page = pages
	}
	offset := (page - 1) * pageSize
	return m.listSummaries("WHERE j.parent_chat_id = '' AND j.status != 'deleted'", `ORDER BY j.created_at DESC, j.id DESC LIMIT ? OFFSET ?`, []any{pageSize, offset}, total)
}

// ListCursor is stable when new tasks are inserted: it continues after the
// last row seen rather than making deep pages increasingly expensive with
// OFFSET. Its jobs intentionally omit Items; file details are fetched on row
// expansion through Get.
func (m *Manager) ListCursor(cursor string, pageSize int) ([]Job, int, string, error) {
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 50 {
		pageSize = 50
	}
	var total int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE parent_chat_id = '' AND status != 'deleted'`).Scan(&total); err != nil {
		return nil, 0, "", err
	}
	where := "WHERE j.parent_chat_id = '' AND j.status != 'deleted'"
	pagination := `ORDER BY j.created_at DESC, j.id DESC LIMIT ?`
	args := []any{pageSize + 1}
	if cursor != "" {
		createdAt, id, err := decodeJobCursor(cursor)
		if err != nil {
			return nil, 0, "", errors.New("分页游标无效，请返回第一页")
		}
		where += ` AND (j.created_at < ? OR (j.created_at = ? AND j.id < ?))`
		args = []any{createdAt, createdAt, id, pageSize + 1}
	}
	jobs, total, err := m.listSummaries(where, pagination, args, total)
	if err != nil {
		return nil, 0, "", err
	}
	next := ""
	if len(jobs) > pageSize {
		jobs = jobs[:pageSize]
		last := jobs[len(jobs)-1]
		next = encodeJobCursor(last.CreatedAt, last.ID)
	}
	return jobs, total, next, nil
}

func (m *Manager) listSummaries(where, pagination string, args []any, total int) ([]Job, int, error) {
	query := `SELECT j.id, j.source_url, j.dialog_type, j.dialog_key, j.dialog_name, j.account_id, j.attempts, j.status, j.error, j.created_at, j.updated_at,
 COUNT(i.id), COALESCE(SUM(CASE WHEN i.status = 'completed' THEN 1 ELSE 0 END), 0), COALESCE(MAX(NULLIF(i.message_text, '')), '')
 FROM download_jobs j LEFT JOIN download_items i ON i.job_id = j.id ` + where + `
 GROUP BY j.id ` + pagination
	rows, err := m.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	jobs := make([]Job, 0)
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogName, &job.AccountID, &job.Attempts, &job.Status, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.TotalItems, &job.CompletedItems, &job.MessageText); err != nil {
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
	}
	return jobs, total, nil
}

func encodeJobCursor(createdAt, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(createdAt + "\x00" + id))
}

func decodeJobCursor(cursor string) (string, string, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(string(data), "\x00", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("invalid cursor")
	}
	return parts[0], parts[1], nil
}

func (m *Manager) Summary() (active, completedItems, failedItems int) {
	_ = m.db.QueryRow(`SELECT
 COALESCE(SUM(CASE WHEN status IN ('queued', 'running', 'downloaded', 'paused') THEN 1 ELSE 0 END), 0),
 COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0),
 COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0)
 FROM download_items`).Scan(&active, &completedItems, &failedItems)
	return
}

// ActiveAccountJobs reports work that would lose its bound Telegram session if
// the account were removed. Terminal history does not block account removal.
func (m *Manager) ActiveAccountJobs(accountID string) (int, error) {
	var count int
	err := m.db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE account_id = ? AND status IN ('queued', 'running', 'paused')`, accountID).Scan(&count)
	return count, err
}

func (m *Manager) LiveProgress() []FileProgress {
	return m.progress.Snapshot()
}

// Stop asks all active upstream transfers to stop at a resumable boundary.
// It is called before process shutdown so a restart does not depend on a hard
// kill to recover task state.
func (m *Manager) Stop() {
	if m.dbMonitorStop != nil {
		m.dbMonitorStop()
	}
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.cancels))
	for _, cancel := range m.cancels {
		cancels = append(cancels, cancel)
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	m.mu.Lock()
	listeners := make([]context.CancelFunc, 0, len(m.chatListeners))
	for _, listener := range m.chatListeners {
		listeners = append(listeners, listener.cancel)
	}
	m.chatListeners = make(map[string]*chatListener)
	m.mu.Unlock()
	for _, cancel := range listeners {
		cancel()
	}
	if m.db != nil {
		_ = m.db.Close()
	}
}

// Revision increases only when persistent task state changes. It lets the UI
// avoid polling the database while still refreshing promptly after transitions.
func (m *Manager) Revision() uint64 { return m.revision.Load() }
func (m *Manager) touch()           { m.revision.Add(1) }

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
	return m.enqueueIntentParent(intent, sources, direct, "")
}

// enqueueIntentParent shares the mature message downloader with chat-index
// batches. Parent jobs are hidden from ordinary message task listings but keep
// every existing resume, progress and final-file safety guarantee.
func (m *Manager) enqueueIntentParent(intent DownloadIntent, sources []source, direct directPeer, parentChatID string) (Submission, error) {
	configJSON, err := json.Marshal(m.settings.Get().Download)
	if err != nil {
		return Submission{}, err
	}
	return m.enqueueIntentParentSnapshot(intent, sources, direct, parentChatID, string(configJSON))
}

// enqueueIntentParentSnapshot keeps every child of one chat task on the
// configuration captured when that parent was created. A lengthy media index
// must not silently mix old and newly edited download settings.
func (m *Manager) enqueueIntentParentSnapshot(intent DownloadIntent, sources []source, direct directPeer, parentChatID, configJSON string) (Submission, error) {
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
		var existingStatus string
		if err := tx.QueryRow(`SELECT status FROM download_jobs WHERE id = ?`, existingID).Scan(&existingStatus); err != nil {
			return Submission{}, err
		}
		reactivated := existingStatus == "cancelled" || existingStatus == "deleted"
		if reactivated {
			if _, err := tx.Exec(`UPDATE download_jobs SET status = 'queued', error = '', updated_at = ? WHERE id = ? AND status IN ('cancelled', 'deleted')`, now, existingID); err != nil {
				return Submission{}, err
			}
			// Completed files are already safely published and must never be
			// downloaded again. Every other file restarts or resumes normally.
			if _, err := tx.Exec(`UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE job_id = ? AND status != 'completed'`, existingID); err != nil {
				return Submission{}, err
			}
		}
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
		if reactivated {
			m.touch()
			m.emit(existingID, requestID, "job_reactivated", "queued")
			m.signal()
			return Submission{RequestID: requestID, Job: job, Duplicate: true, Reactivated: true}, nil
		}
		m.emit(existingID, requestID, "request_attached", job.Status)
		return Submission{RequestID: requestID, Job: job, Duplicate: true}, nil
	}
	if _, err = tx.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, parent_chat_id, config_json, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`, id, intent.URL, sources[0].DialogType, sources[0].DialogKey, sources[0].DialogName, intent.AccountID, direct.kind, direct.id, direct.hash, parentChatID, configJSON, now, now); err != nil {
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
	job := Job{ID: id, SourceURL: intent.URL, DialogType: sources[0].DialogType, DialogKey: sources[0].DialogKey, DialogName: sources[0].DialogName, HasPublicLink: isPublicMessageLink(intent.URL), AccountID: intent.AccountID, DirectPeerType: direct.kind, DirectPeerID: direct.id, DirectPeerHash: direct.hash, ConfigJSON: configJSON, Status: "queued", CreatedAt: now, UpdatedAt: now, TotalItems: len(sources)}
	m.emit(id, requestID, "job_created", "queued")
	m.signal()
	return Submission{RequestID: requestID, Job: job, Created: true}, nil
}

func (m *Manager) worker() {
	for {
		if !m.acquireJobSlot() {
			return
		}
		job, sources, err := m.nextQueued()
		if err == nil && job.ID != "" {
			m.run(job, sources)
			m.releaseJobSlot()
			continue
		}
		m.releaseJobSlot()
		select {
		case <-m.wake:
		case <-time.After(3 * time.Second):
		}
	}
}

// acquireJobSlot applies the configured cross-task limit in this wrapper. It
// is intentionally independent from upstream tdl's per-invocation media
// limit, so two distinct Telegram links can run at the same time.
func (m *Manager) acquireJobSlot() bool {
	for {
		m.slotMu.Lock()
		limit := m.settings.Get().Download.ConcurrentJobs
		if m.activeJobs < limit {
			m.activeJobs++
			m.slotMu.Unlock()
			return true
		}
		m.slotMu.Unlock()
		// A release wakes one waiting worker immediately. The timeout also
		// picks up a changed concurrency setting without busy-waiting.
		select {
		case <-m.slotWake:
		case <-time.After(time.Second):
		}
	}
}

func (m *Manager) releaseJobSlot() {
	m.slotMu.Lock()
	if m.activeJobs > 0 {
		m.activeJobs--
	}
	m.slotMu.Unlock()
	select {
	case m.slotWake <- struct{}{}:
	default:
	}
}

func (m *Manager) nextQueued() (Job, []source, error) {
	var job Job
	err := m.db.QueryRow(`SELECT id, source_url, dialog_type, dialog_key, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, config_json, attempts, status, error, created_at, updated_at FROM download_jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1`).Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogName, &job.AccountID, &job.DirectPeerType, &job.DirectPeerID, &job.DirectPeerHash, &job.ConfigJSON, &job.Attempts, &job.Status, &job.Error, &job.CreatedAt, &job.UpdatedAt)
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
	// Register cancellation before any per-file state mutation. A pause or
	// cancellation that races task startup can then stop this worker before it
	// starts an upstream transfer or overwrites the terminal task state.
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancels[job.ID] = cancel
	m.mu.Unlock()
	defer func() { cancel(); m.mu.Lock(); delete(m.cancels, job.ID); m.mu.Unlock() }()
	defer m.progress.ClearJob(job.ID)
	pending := make([]source, 0, len(sources))
	for _, item := range sources {
		if ctx.Err() != nil || m.status(job.ID) != "running" {
			return
		}
		if item.Status == "completed" || (item.Status == "downloaded" && regularFileExists(item.FinalPath)) {
			continue
		}
		pending = append(pending, item)
		if err := m.beginItemAttempt(item); err != nil {
			m.fail(job.ID, sources, fmt.Errorf("记录文件下载尝试失败: %w", err))
			return
		}
	}
	if len(pending) == 0 {
		_ = m.setJob(job.ID, "completed", "")
		return
	}
	if ctx.Err() != nil || m.status(job.ID) != "running" {
		return
	}
	// A media transfer must not fail merely because it is slow. Cancellation is
	// controlled by pause/cancel and graceful application shutdown instead of a
	// fixed wall-clock deadline.
	tmpDir := filepath.Join(m.downloadDir, ".tdl-tmp", job.ID)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		m.fail(job.ID, pending, err)
		return
	}
	config := m.settings.Get()
	if job.ConfigJSON != "" {
		var snapshot settings.Download
		if err := json.Unmarshal([]byte(job.ConfigJSON), &snapshot); err != nil {
			m.fail(job.ID, pending, fmt.Errorf("任务下载配置快照无效: %w", err))
			return
		}
		config.Download = snapshot
	}
	// One upstream invocation resolves exactly one dialog, so MessageID is
	// sufficient for callback correlation here. Do not use its numeric dialog
	// ID: Telegram represents some dialogs (notably "Saved Messages") with a
	// different peer form in download callbacks than in the update stream.
	pendingByMessage := make(map[int]source, len(pending))
	for _, item := range pending {
		pendingByMessage[item.MessageID] = item
	}
	var publishWG sync.WaitGroup
	var stateMu sync.Mutex
	var stateErr error
	recordStateErr := func(err error) {
		if err == nil {
			return
		}
		stateMu.Lock()
		if stateErr == nil {
			stateErr = err
		}
		stateMu.Unlock()
	}
	// A deleted task may have left an upstream resume key. Consume this one-shot
	// marker so recreating it starts with a new temporary directory.
	restart := m.consumeRestart(job.AccountID, job.SourceURL)
	err = m.accounts.Run(ctx, job.AccountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		opts := upstreamDL.Options{URLs: []string{job.SourceURL}, Dir: tmpDir, Template: config.Download.TempFilenameTemplate, Group: true, Continue: true, Restart: restart, Quiet: true, Runtime: &upstreamDL.RuntimeOptions{Threads: config.Download.Threads, TaskLimit: config.Download.TaskLimit, PoolSize: config.Download.PoolSize, Delay: time.Duration(config.Download.DelayMS) * time.Millisecond, DisableProgressPS: true}, ProgressCallback: func(update upstreamDL.ProgressUpdate) {
			item, ok := pendingByMessage[update.MessageID]
			if !ok {
				return
			}
			started, _ := m.progress.Update(job.ID, item.Item, update)
			if started {
				recordStateErr(m.markItemStarted(item))
			}
		}, FileCompletedCallback: func(update upstreamDL.FileCompletedUpdate) {
			item, ok := pendingByMessage[update.MessageID]
			if !ok {
				return
			}
			recordStateErr(m.markItemFinished(item))
			publishWG.Add(1)
			go func() {
				defer publishWG.Done()
				recordStateErr(m.publishItem(job.ID, update.Path, item, config))
			}()
		}}
		if peer := job.directInputPeer(); peer != nil {
			opts.URLs = nil
			// A chat child job may contain many media messages. Albums are passed
			// once by their earliest member because upstream expands Group=true to
			// all of their siblings; sending every member would download an album
			// repeatedly.
			messageIDs := make([]int, 0, len(sources))
			seenMessages := make(map[int]struct{}, len(sources))
			seenGroups := make(map[int64]struct{})
			for _, item := range sources {
				if item.GroupedID != 0 {
					if _, exists := seenGroups[item.GroupedID]; exists {
						continue
					}
					seenGroups[item.GroupedID] = struct{}{}
				}
				if _, exists := seenMessages[item.MessageID]; exists {
					continue
				}
				seenMessages[item.MessageID] = struct{}{}
				messageIDs = append(messageIDs, item.MessageID)
			}
			opts.DirectDialogs = [][]*tmessage.Dialog{{{Peer: peer, Messages: messageIDs}}}
		}
		return upstreamDL.Run(ctx, client, kvd, opts)
	})
	// File completion callbacks run in the upstream download workers. Their
	// publishing work is deliberately asynchronous, but the job state must not
	// be decided until every accepted file has finished moving.
	publishWG.Wait()
	stateMu.Lock()
	persistErr := stateErr
	stateMu.Unlock()
	if persistErr != nil {
		m.fail(job.ID, pending, fmt.Errorf("保存下载状态失败: %w", persistErr))
		return
	}
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
		// A database outage cancels the upstream context. Recovery may already
		// have returned this job to the queue by the time this callback exits;
		// never overwrite that recovery state with a transfer failure.
		if m.status(job.ID) != "running" {
			return
		}
		// Upstream can return an error while persisting resume metadata after all
		// media callbacks have already completed and every final move succeeded.
		// The observable task result is still successful in that case.
		if m.allItemsCompleted(job.ID) {
			_ = m.setJob(job.ID, "completed", "")
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
		_ = m.setJob(job.ID, "completed", "")
		_ = os.RemoveAll(tmpDir)
	}
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
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

// reconcilePublishedItems completes the narrow crash window between moving a
// file and persisting its terminal status. A path is trusted only when it is
// still a regular file; otherwise a normal resumable retry remains possible.
func (m *Manager) reconcilePublishedItems() error {
	rows, err := m.db.Query(`SELECT dialog_key, message_id, final_path FROM download_items WHERE status = 'downloaded' AND final_path != ''`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type candidate struct {
		dialogKey string
		messageID int
		path      string
	}
	items := make([]candidate, 0)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.dialogKey, &item.messageID, &item.path); err != nil {
			return err
		}
		if info, statErr := os.Stat(item.path); statErr == nil && info.Mode().IsRegular() {
			items = append(items, item)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range items {
		if _, err := m.db.Exec(`UPDATE download_items SET status = 'completed', finished_at = CASE WHEN finished_at = '' THEN ? ELSE finished_at END WHERE dialog_key = ? AND message_id = ? AND status = 'downloaded'`, time.Now().UTC().Format(time.RFC3339Nano), item.dialogKey, item.messageID); err != nil {
			return err
		}
	}
	if len(items) > 0 {
		_, err = m.db.Exec(`UPDATE download_jobs SET status = 'completed', error = '', updated_at = ? WHERE status IN ('queued', 'running', 'failed', 'partial') AND NOT EXISTS (SELECT 1 FROM download_items WHERE download_items.job_id = download_jobs.id AND download_items.status != 'completed')`, time.Now().UTC().Format(time.RFC3339Nano))
	}
	return err
}

// publishItem moves a file only after upstream tdl has closed and finalized
// its temporary file. It runs outside the upstream download worker.
func (m *Manager) publishItem(jobID, path string, item source, config settings.Values) error {
	lock := m.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	if m.status(jobID) != "running" {
		return nil
	}
	finalPath, err := finalDestination(m.downloadDir, config.Download.FinalFilenameTemplate, item)
	if err != nil {
		return m.setItem(item, "failed", "", err.Error())
	}
	if _, statErr := os.Stat(finalPath); statErr == nil {
		return m.setItem(item, "failed", "", "目标文件已存在，未覆盖")
	}
	// Persist the selected final path before the irreversible move. If the
	// database disconnects after a successful move, reconciliation can verify
	// the path instead of downloading this media again.
	if err := m.setItem(item, "downloaded", finalPath, ""); err != nil {
		return fmt.Errorf("保存待发布状态: %w", err)
	}
	if err := publishNoReplace(path, finalPath); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return m.setItem(item, "failed", "", "目标文件已存在，未覆盖")
		}
		return m.setItem(item, "failed", "", err.Error())
	}
	return m.setItem(item, "completed", finalPath, "")
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
			result = append(result, source{Item: Item{DialogType: dialogType, DialogKey: dialogKey, DialogID: dialogID, MessageID: msg.ID, GroupedID: groupedID, MessageText: messageText, OriginalName: media.Name, Size: media.Size}, DialogName: peer.VisibleName(), MediaType: messageMediaType(msg)})
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
			result = append(result, source{Item: Item{DialogType: dialogType, DialogKey: dialogKey, DialogID: resolvedDialogID, MessageID: msg.ID, GroupedID: groupedID, MessageText: messageText, OriginalName: media.Name, Size: media.Size}, DialogName: dialogName, MediaType: messageMediaType(msg)})
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
	// Keep one wake-up per worker so several newly queued jobs can begin in
	// parallel instead of waiting for the workers' idle polling timeout.
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) signalChat() {
	select {
	case m.chatWake <- struct{}{}:
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
	lock := m.jobLock(id)
	lock.Lock()
	defer lock.Unlock()
	status := m.status(id)
	if status == "" {
		return errors.New("下载任务不存在")
	}
	if status != "queued" && status != "running" {
		return errors.New("当前任务不能暂停")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := m.transitionItems(id, status, "paused", "已暂停，可继续恢复", m.pauseItemsSQL(), now, id); err != nil {
		return fmt.Errorf("暂停任务: %w", err)
	}
	m.mu.Lock()
	cancel := m.cancels[id]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (m *Manager) pauseItemsSQL() string {
	if m.db.isPostgres() {
		return `UPDATE download_items SET status = 'paused', error = '', elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END, started_at = '', finished_at = '' WHERE job_id = ? AND status IN ('queued', 'running', 'downloaded')`
	}
	return `UPDATE download_items SET status = 'paused', error = '', elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN CAST((julianday(?) - julianday(started_at)) * 86400000 AS INTEGER) ELSE 0 END, started_at = '', finished_at = '' WHERE job_id = ? AND status IN ('queued', 'running', 'downloaded')`
}
func (m *Manager) Resume(id string) error {
	lock := m.jobLock(id)
	lock.Lock()
	defer lock.Unlock()
	if m.status(id) != "paused" {
		return errors.New("当前任务不能恢复")
	}
	if err := m.transitionItems(id, "paused", "queued", "", `UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE job_id = ? AND status != 'completed'`, id); err != nil {
		return fmt.Errorf("恢复任务: %w", err)
	}
	m.signal()
	return nil
}
func (m *Manager) Retry(id string) error {
	lock := m.jobLock(id)
	lock.Lock()
	defer lock.Unlock()
	status := m.status(id)
	if status != "failed" && status != "partial" && status != "cancelled" {
		return errors.New("只有失败、部分完成或已取消任务可以重新开始")
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
	if err := m.transitionItems(id, status, "queued", "", `UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE job_id = ? AND status != 'completed'`, id); err != nil {
		return fmt.Errorf("重试任务: %w", err)
	}
	m.signal()
	return nil
}
func (m *Manager) Cancel(id string) error {
	lock := m.jobLock(id)
	lock.Lock()
	defer lock.Unlock()
	status := m.status(id)
	if status != "queued" && status != "running" && status != "paused" {
		return errors.New("当前任务不能取消")
	}
	if err := m.transitionItems(id, status, "cancelled", "已取消；临时文件保留，可手动清理", `UPDATE download_items SET status = 'cancelled', finished_at = ? WHERE job_id = ? AND status != 'completed'`, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return fmt.Errorf("取消任务: %w", err)
	}
	m.mu.Lock()
	cancel := m.cancels[id]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// transitionItems updates the task and its non-final file rows in one database
// transaction. A control request therefore never leaves a task marked as
// paused/cancelled/queued while its file rows still describe a different
// durable state.
func (m *Manager) transitionItems(id, expected, next, message, itemSQL string, args ...any) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = ?`, next, message, time.Now().UTC().Format(time.RFC3339), id, expected)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("任务状态已变化，请刷新后重试")
	}
	if _, err := tx.Exec(itemSQL, args...); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	m.emit(id, "", "job_status_changed", next)
	return nil
}

// Delete hides a terminal task and removes only its temporary directory. The
// task, media identities and audit records remain as permanent history; a
// later matching submission reactivates this same task instead of creating a
// second media identity.
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
		if _, err := tx.Exec(`INSERT INTO download_resets(account_id, source_url, created_at) VALUES (?, ?, ?) ON CONFLICT(account_id, source_url) DO UPDATE SET created_at = EXCLUDED.created_at`, accountID, sourceURL, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE download_jobs SET status = 'deleted', error = '任务已删除', updated_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.touch()
	if err := os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", id)); err != nil {
		return fmt.Errorf("任务已删除，但清理临时目录失败: %w", err)
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
func (m *Manager) jobLock(id string) *sync.Mutex {
	var hash uint32
	for index := 0; index < len(id); index++ {
		hash = hash*33 + uint32(id[index])
	}
	return &m.jobLocks[hash%uint32(len(m.jobLocks))]
}

func (m *Manager) setJob(id, status, message string) error {
	if _, err := m.db.Exec(`UPDATE download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?`, status, message, time.Now().UTC().Format(time.RFC3339), id); err != nil {
		applog.Error("download", "task_state_save_failed", "job_id", id, "status", status, "error", err.Error())
		return err
	}
	m.touch()
	m.emit(id, "", "job_status_changed", status)
	return nil
}
func (m *Manager) setItem(item source, status, path, message string) error {
	finishedAt := ""
	if status == "completed" || status == "failed" || status == "cancelled" {
		finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := m.db.Exec(`UPDATE download_items SET status = ?, final_path = ?, error = ?, finished_at = CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE dialog_key = ? AND message_id = ?`, status, path, message, finishedAt, finishedAt, item.DialogKey, item.MessageID); err != nil {
		applog.Error("download", "item_state_save_failed", "dialog_key", item.DialogKey, "message_id", item.MessageID, "status", status, "error", err.Error())
		return err
	}
	m.touch()
	return nil
}
func (m *Manager) beginItemAttempt(item source) error {
	// A task may be running while this particular file is still waiting for an
	// upstream worker slot. It becomes "running" only on its first byte-level
	// progress callback.
	return m.execItemState("item_attempt_save_failed", `UPDATE download_items SET attempts = attempts + 1, status = 'queued', error = '', started_at = '', finished_at = '' WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID)
}
func (m *Manager) pauseItem(item source) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := m.pauseItemsSQL()
	query = strings.Replace(query, "WHERE job_id = ? AND status IN ('queued', 'running', 'downloaded')", "WHERE dialog_key = ? AND message_id = ? AND status IN ('queued', 'running')", 1)
	m.execItemState("item_pause_save_failed", query, now, item.DialogKey, item.MessageID)
}
func (m *Manager) markItemStarted(item source) error {
	return m.execItemState("item_start_save_failed", `UPDATE download_items SET status = 'running', started_at = ? WHERE dialog_key = ? AND message_id = ? AND status = 'queued' AND started_at = ''`, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID)
}
func (m *Manager) markItemFinished(item source) error {
	return m.execItemState("item_finish_save_failed", `UPDATE download_items SET status = 'downloaded', finished_at = ? WHERE dialog_key = ? AND message_id = ? AND status = 'running' AND started_at != '' AND finished_at = ''`, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID)
}

func (m *Manager) execItemState(event, query string, args ...any) error {
	if _, err := m.db.Exec(query, args...); err != nil {
		applog.Error("download", event, "error", err.Error())
		return err
	}
	m.touch()
	return nil
}
func (m *Manager) fail(id string, sources []source, err error) {
	for _, item := range sources {
		if status := m.itemStatus(item.DialogKey, item.MessageID); status == "completed" || status == "downloaded" {
			continue
		}
		_ = m.setItem(item, "failed", "", err.Error())
	}
	var completed int
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status = 'completed'`, id).Scan(&completed)
	if completed > 0 {
		_ = m.setJob(id, "partial", "部分文件未完成，可重试失败文件")
		return
	}
	_ = m.setJob(id, "failed", err.Error())
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
