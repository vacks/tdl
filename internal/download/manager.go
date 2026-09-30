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
	"net/url"
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
	// messageIdleInterval is the fallback only. Creating, reactivating, resuming
	// and retrying a task each wake every worker directly, so this bounds how
	// long a missed wake can stall a queued task rather than how fast the queue
	// drains. It is deliberately long because the poll itself is multiplied by
	// workerCount.
	messageIdleInterval = 15 * time.Second
	// chatDownloadWorkerCount bounds how many session batches can transfer at
	// once. The global concurrency setting remains the real cap; these workers
	// only need to be numerous enough to keep that many batches in flight.
	chatDownloadWorkerCount = 3
	maxDuplicateRetries     = 4
	maxStalledAttempts      = 3
	// A request which never receives its first byte is likely a dead
	// connection. After a large file starts, allow a much longer quiet period
	// so slow proxy links are not treated as a stalled transfer.
	upstreamInitialTimeout = 2 * time.Minute
	upstreamIdleTimeout    = 10 * time.Minute
	upstreamWatchPeriod    = 5 * time.Second
	// Temporary files are isolated by complete Telegram dialog identity below,
	// so the message ID is sufficient within each private directory.
	temporaryFilenameTemplate = "{{ .MessageID }}_{{ filenamify .FileName }}"
	// Waiting message items are promoted by one dedicated goroutine instead of
	// by every download worker. It drains in bounded passes, pauses between them
	// so a saturated database cannot be hammered, and falls back to the idle
	// interval whenever a pass promotes nothing: that timeout, not the wake
	// signal, is what guarantees an item is eventually promoted, so a dropped
	// wake only costs latency.
	reconcileBatchSize      = 256
	reconcileIdleInterval   = 3 * time.Second
	reconcileDrainPause     = 20 * time.Millisecond
	reconcileMaxDrainPasses = 64
	// filteredCountTTL bounds how stale a status-filtered task total may be. It
	// only affects a displayed number, never pagination.
	filteredCountTTL = 5 * time.Second
	// queuedCandidateWindow bounds how far the scheduler looks ahead for a task
	// whose account is not inside a Telegram cooldown. A handful of rows is
	// enough to step over a blocked account without scanning permanent history.
	queuedCandidateWindow = 16
	// Inbox writes are the one place where losing an event is unrecoverable: the
	// update dispatcher returns nil to gotd, which advances the update state, so
	// Telegram never redelivers. A failed insert is therefore retried in memory,
	// bounded so an outage cannot grow the process without limit.
	inboxRetryQueueSize = 512
	inboxRetryAttempts  = 5
	inboxRetryBackoff   = 2 * time.Second
)

// inboxRetry is one failed inbox write waiting to be re-attempted.
type inboxRetry struct {
	describe string
	run      func() error
}

// filteredJobCount is one memoized status-filtered task total.
type filteredJobCount struct {
	total   int
	expires time.Time
}

type Item struct {
	ID         int64  `json:"id"`
	DialogType string `json:"dialogType"`
	DialogKey  string `json:"dialogKey"`
	DialogID   int64  `json:"dialogId"`
	MessageID  int    `json:"messageId"`
	GroupedID  int64  `json:"groupedId,omitempty"`
	// Origin* is the stable presentation context. It deliberately differs from
	// the actual source dialog for a channel discussion reply: downloads and
	// deduplication always use DialogKey/MessageID, while final naming can keep
	// a post and its replies together in the originating channel directory.
	OriginDialogName string `json:"originDialogName,omitempty"`
	OriginMessageID  int    `json:"originMessageId,omitempty"`
	IsComment        bool   `json:"isComment,omitempty"`
	SourcePeerType   string `json:"-"`
	SourcePeerID     int64  `json:"-"`
	SourcePeerHash   int64  `json:"-"`
	ReplyRootID      int    `json:"-"`
	MessageText      string `json:"messageText,omitempty"`
	OriginalName     string `json:"originalName"`
	Size             int64  `json:"size"`
	FinalPath        string `json:"finalPath,omitempty"`
	StartedAt        string `json:"startedAt,omitempty"`
	FinishedAt       string `json:"finishedAt,omitempty"`
	ElapsedMS        int64  `json:"elapsedMs"`
	Attempts         int    `json:"attempts"`
	Status           string `json:"status"`
	Error            string `json:"error,omitempty"`
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
	Direct     directPeer
}
type directPeer struct {
	kind     string
	id, hash int64
}

func (p directPeer) inputPeer() tg.InputPeerClass {
	switch p.kind {
	case "self":
		return &tg.InputPeerSelf{}
	case "user":
		return &tg.InputPeerUser{UserID: p.id, AccessHash: p.hash}
	case "chat":
		return &tg.InputPeerChat{ChatID: p.id}
	case "channel":
		return &tg.InputPeerChannel{ChannelID: p.id, AccessHash: p.hash}
	default:
		return nil
	}
}

func setSourcePeer(items []source, input tg.InputPeerClass) []source {
	direct := makeDirectPeer(input)
	for i := range items {
		items[i].Direct = direct
		items[i].SourcePeerType = direct.kind
		items[i].SourcePeerID = direct.id
		items[i].SourcePeerHash = direct.hash
	}
	return items
}

const bytesPerMiB int64 = 1024 * 1024

// filterSources applies the global download policy before any task or chat
// index row is created. FileTypes is an explicit allow-list: an empty list
// intentionally means no file type is eligible for download.
func filterSources(sources []source, config settings.Download) []source {
	allowed := make(map[string]struct{}, len(config.FileTypes))
	for _, kind := range config.FileTypes {
		allowed[kind] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
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
		// Telegram does not use the filename extension to classify shared
		// content. A document carries semantic attributes supplied by the
		// sender/server (sticker, animated GIF, audio/voice, video, ...).
		// Keep this ordering deliberate: a sticker remains a sticker even
		// when its container is WebP, TGS, or a video format.
		for _, attribute := range document.Attributes {
			if _, ok := attribute.(*tg.DocumentAttributeSticker); ok {
				return "sticker"
			}
		}
		for _, attribute := range document.Attributes {
			if _, ok := attribute.(*tg.DocumentAttributeAnimated); ok {
				return "gif"
			}
		}
		for _, attribute := range document.Attributes {
			audio, ok := attribute.(*tg.DocumentAttributeAudio)
			if !ok {
				continue
			}
			if audio.Voice {
				return "voice"
			}
			return "music"
		}
		for _, attribute := range document.Attributes {
			if _, ok := attribute.(*tg.DocumentAttributeVideo); ok {
				return "video"
			}
		}
		return "document"
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
	chatCancels   map[string]map[uint64]context.CancelFunc
	chatCancelSeq uint64
	chatActive    map[string]struct{}
	wake          chan struct{}
	chatWake      chan struct{}
	// chatDownloadWake holds one channel per chat download worker. It is
	// deliberately separate from chatWake for the same reason reconcileWake is
	// separate from wake: a token consumed by the wrong class of consumer is a
	// lost wakeup, because the indexing loop would swallow the signal a transfer
	// worker is waiting for and that worker would then sit out a full idle
	// interval. One channel per worker rather than one shared buffered channel,
	// so that a single signal can start every idle worker: indexing a history
	// page usually queues far more media than the transfer concurrency.
	chatDownloadWake []chan struct{}
	chatEventWake    chan struct{}
	chatListeners    map[string]*chatListener
	chatWatched      map[string]map[string]struct{}
	// chatLinkWake asks the discussion-link worker to probe now: a task whose
	// listener is about to start must learn its linked group before the first
	// comment can arrive, because a filtered update is never redelivered.
	chatLinkWake       chan struct{}
	listenerDirty      atomic.Bool
	listenerSnapshotAt atomic.Int64
	// listenerAccounts is the set of authorized accounts the listener snapshot
	// was last built with. It is read and written only by the single chat
	// worker goroutine, inside reconcileChatListeners, which is the only place
	// that starts a listener - so it needs no lock of its own. Comparing it is
	// how a sign-in or an expiry is noticed promptly rather than at the next
	// five minute refresh.
	listenerAccounts map[string]struct{}
	slotWake         chan struct{}
	// reconcileWake is deliberately separate from wake. The claim reconciler is
	// an additional consumer of its own signal, and sharing wake would let it
	// take the token a download worker needs, delaying a newly queued task by a
	// full idle interval for no visible reason.
	reconcileWake chan struct{}
	progress      *progressStore
	events        *eventBus
	slotMu        sync.Mutex
	activeJobs    int
	activeChats   int
	rpcMu         sync.Mutex
	rpcState      map[string]*telegramRPCState
	jobLocks      [64]sync.Mutex
	chatLocks     [64]sync.Mutex
	revision      atomic.Uint64
	dbHealthMu    sync.RWMutex
	dbHealth      DatabaseHealth
	dbMonitorStop context.CancelFunc
	cleanupStop   context.CancelFunc
	reconcileStop context.CancelFunc
	dbOutage      atomic.Bool
	visibleJobs   atomic.Int64
	visibleChats  atomic.Int64
	// filteredCounts memoizes the per-status task counts the list view asks for.
	// Lazily initialized so a zero-value Manager stays usable.
	filteredCountsMu sync.Mutex
	filteredCounts   map[string]filteredJobCount
	// stopCh is closed once by Stop so the worker loops can exit. A nil channel
	// (a Manager built without New, as tests do) never fires in a select, so the
	// loops stay correct without a nil guard.
	stopCh   chan struct{}
	stopOnce sync.Once
	// lastInboxReap throttles the inbox lease safety net (unix nanoseconds).
	lastInboxReap atomic.Int64
	// lastJobLeaseSweep throttles recovery of tasks stranded in 'running'
	// (unix nanoseconds).
	lastJobLeaseSweep atomic.Int64
	// reconcilePublishedCursor rotates the published-file sweep across passes.
	// The sweep exists for a narrow crash window, so its set is normally empty
	// and reading all of it costs nothing - but a database outage that leaves
	// many rows mid-publish makes the same full pass expensive, and it ran every
	// minute. A cursor keeps one pass bounded and lets the next resume.
	reconcilePublishedCursor atomic.Int64
	// inboxRetry carries inbox writes that failed, to a single bounded worker.
	inboxRetry     chan inboxRetry
	inboxRetryStop context.CancelFunc
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
	chatDownloadWake := make([]chan struct{}, chatDownloadWorkerCount)
	for i := range chatDownloadWake {
		chatDownloadWake[i] = make(chan struct{}, 1)
	}
	m := &Manager{db: db, downloadDir: downloadDir, settings: store, accounts: accounts, cancels: make(map[string]context.CancelFunc), chatCancels: make(map[string]map[uint64]context.CancelFunc), chatActive: make(map[string]struct{}), wake: make(chan struct{}, workerCount), chatWake: make(chan struct{}, 1), chatDownloadWake: chatDownloadWake, chatEventWake: make(chan struct{}, 1), chatListeners: make(map[string]*chatListener), chatWatched: make(map[string]map[string]struct{}), chatLinkWake: make(chan struct{}, 1), slotWake: make(chan struct{}, 1), reconcileWake: make(chan struct{}, 1), stopCh: make(chan struct{}), inboxRetry: make(chan inboxRetry, inboxRetryQueueSize), rpcState: make(map[string]*telegramRPCState), progress: newProgressStore(), events: newEventBus()}
	// The first worker pass constructs the listener snapshot before accepting
	// updates. Subsequent rebuilds are only needed after state changes.
	m.listenerDirty.Store(true)
	if err := m.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.loadTelegramRateLimits(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load Telegram rate limits: %w", err)
	}
	if err := m.loadInstanceID(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.loadVisibleCounts(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.reconcilePublishedItems(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("reconcile published files: %w", err)
	}
	// Retain upstream temporary files and resume keys, but return both the
	// parent job and every unfinished item to the same durable queue. Updating
	// only the parent would leave a queued job with no queued items after a
	// process crash.
	if err := m.recoverInterruptedMessageTasks("服务重启，任务等待恢复"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover message tasks: %w", err)
	}
	if err := m.reconcileChatPublishedItems(nil); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("reconcile published chat files: %w", err)
	}
	if err := m.recoverInterruptedChatTasks(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover chat tasks: %w", err)
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
	if _, err := m.db.Exec(`UPDATE chat_message_inbox SET status = 'pending', next_attempt_at = ?, updated_at = ? WHERE status = 'processing'`, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover chat message inbox: %w", err)
	}
	// The account gate is installed before any worker starts, so the first
	// request an account makes is already paced. The download service owns the
	// budget because the limit is per Telegram account and the accounts are
	// shared with the reaction and listener features.
	accounts.SetRPCGate(m.awaitTelegramRPC)
	for worker := 0; worker < workerCount; worker++ {
		go m.worker()
	}
	go m.chatWorker()
	// Learning a listening channel's linked discussion group is a Telegram
	// request, so it runs here and not on the chat worker: that goroutine is
	// single threaded and a long call in it stops state refresh, claim
	// reconciliation and listener upkeep for its duration.
	go m.chatDiscussionLinkWorker()
	for worker := 0; worker < chatDownloadWorkerCount; worker++ {
		go m.chatDownloadWorker(m.chatDownloadWake[worker])
	}
	for worker := 0; worker < 2; worker++ {
		go m.chatEventWorker()
	}
	cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
	m.cleanupStop = cancelCleanup
	go m.cleanupLoop(cleanupCtx)
	reconcileCtx, cancelReconcile := context.WithCancel(context.Background())
	m.reconcileStop = cancelReconcile
	// Start the lease-reap clock now so boot does not immediately repeat the
	// inbox reset that startup recovery just performed.
	m.lastInboxReap.Store(time.Now().UnixNano())
	inboxRetryCtx, cancelInboxRetry := context.WithCancel(context.Background())
	m.inboxRetryStop = cancelInboxRetry
	go m.inboxRetryWorker(inboxRetryCtx)
	// Waiting items can survive a restart: recoverInterruptedMessageTasks only
	// returns running and downloaded rows to the queue, so a task whose items
	// were all waiting is still waiting after boot. The worker loop's first pass
	// used to promote those; now this goroutine must run immediately rather than
	// wait out the idle interval.
	go m.reconcileWorker(reconcileCtx)
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
	cancels := make([]context.CancelFunc, 0, len(m.cancels)+len(m.chatCancels))
	for _, cancel := range m.cancels {
		cancels = append(cancels, cancel)
	}
	for _, byExecution := range m.chatCancels {
		for _, cancel := range byExecution {
			cancels = append(cancels, cancel)
		}
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
	if err := m.reconcileChatPublishedItems(nil); err != nil {
		return err
	}
	if err := m.recoverInterruptedMessageTasks("数据库连接恢复，任务等待继续"); err != nil {
		return err
	}
	if err := m.recoverInterruptedChatTasks(); err != nil {
		return err
	}
	// The two inboxes are durable work queues too, and an outage is exactly when
	// a worker loses the row it had claimed: the submit failed and the retry
	// write failed with it. Startup resets these for the same reason (this
	// process is the only inbox consumer), but startup alone left every row
	// claimed during an outage stranded until the next restart.
	if err := m.resetInboxLeases("数据库连接恢复，等待重新处理"); err != nil {
		return err
	}
	m.touch()
	for worker := 0; worker < workerCount; worker++ {
		m.signal()
	}
	// Recovery restored running rows to the queue, which can release claims that
	// waiting items were blocked on.
	m.signalReconcile()
	return nil
}

// recoverInterruptedMessageTasks restores the parent and its file rows in one
// transaction. It also heals an older inconsistent queued parent whose every
// file has already completed.
func (m *Manager) recoverInterruptedMessageTasks(message string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE download_items SET status = 'queued', error = '', final_path = CASE WHEN status = 'downloaded' THEN '' ELSE final_path END, elapsed_ms = elapsed_ms + CASE WHEN started_at <> '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END, started_at = '', finished_at = '' WHERE status IN ('running', 'downloaded') AND job_id IN (SELECT id FROM download_jobs WHERE status = 'running')`, now); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE download_jobs SET status = 'queued', error = ?, updated_at = ? WHERE status = 'running'`, message, now); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE download_jobs SET status = 'completed', error = '', updated_at = ? WHERE status = 'queued' AND EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = download_jobs.id) AND NOT EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = download_jobs.id AND i.status <> 'completed')`, now); err != nil {
		return err
	}
	return tx.Commit()
}

// recoverInterruptedChatTasks applies the equivalent recovery rule to the
// single-parent chat queue. Chat workers only consume queued media rows, so
// leaving a persisted "running" or "downloaded" row after interruption would
// otherwise strand the whole chat indefinitely.
func (m *Manager) recoverInterruptedChatTasks() error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chat_download_items SET status = 'queued', error = '', final_path = CASE WHEN status = 'downloaded' THEN '' ELSE final_path END, elapsed_ms = elapsed_ms + CASE WHEN started_at <> '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END, started_at = '', finished_at = '' WHERE status IN ('running', 'downloaded') AND chat_job_id IN (SELECT id FROM chat_download_jobs WHERE status IN ('scanning', 'downloading', 'listening'))`, now); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE chat_download_jobs SET status = 'queued', error = '', updated_at = ? WHERE status = 'scanning'`, now); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE chat_download_jobs SET status = 'downloading', error = '', updated_at = ? WHERE status IN ('downloading', 'listening')`, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Recompute terminal/listening parents after the transaction has made every
	// unfinished row eligible. This preserves the listener setting when there is
	// no historical media left to transfer.
	m.refreshChatStates()
	return nil
}

// resetInboxLeases returns every claimed inbox row to the pending queue. Both
// inbox tables are durable work queues whose only consumer is this process, so a
// lease still held after a database outage is known to be orphaned: the submit
// failed and the retry write failed with it.
func (m *Manager) resetInboxLeases(reason string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', error = ?, next_attempt_at = ?, updated_at = ? WHERE status = 'processing'`, reason, now, now); err != nil {
		return fmt.Errorf("释放 reaction_inbox 租约: %w", err)
	}
	if _, err := m.db.Exec(`UPDATE chat_message_inbox SET status = 'pending', error = ?, next_attempt_at = ?, updated_at = ? WHERE status = 'processing'`, reason, now, now); err != nil {
		return fmt.Errorf("释放 chat_message_inbox 租约: %w", err)
	}
	return nil
}

// inboxLeaseTimeout bounds how long one worker may hold an inbox row in
// 'processing'. It is far longer than any single submit, so a healthy worker is
// never raided; it exists only so a worker that died mid-submit cannot strand
// its row until the next restart.
const inboxLeaseTimeout = 10 * time.Minute

// inboxReapInterval throttles the safety-net sweep. It is deliberately much
// coarser than the maintenance cadence: the database-recovery hook already
// covers the common case, so this costs two statements per interval at most.
const inboxReapInterval = time.Minute

// reapStaleInboxLeases returns inbox rows whose lease outlived inboxLeaseTimeout
// to the pending queue.
func (m *Manager) reapStaleInboxLeases() error {
	// Timestamps are TEXT. Comparing them lexicographically would be wrong
	// whenever two values differ in fractional-second width, because RFC3339Nano
	// trims trailing zeros, so compare as timestamptz instead. The inbox tables
	// stay small — retention removes finished rows — so losing index support on
	// updated_at here is not a concern.
	cutoff := time.Now().UTC().Add(-inboxLeaseTimeout).Format(time.RFC3339Nano)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := m.db.Exec(`UPDATE reaction_inbox SET status = 'pending', next_attempt_at = ?, updated_at = ? WHERE status = 'processing' AND updated_at::timestamptz < ?::timestamptz`, now, now, cutoff); err != nil {
		return err
	}
	if _, err := m.db.Exec(`UPDATE chat_message_inbox SET status = 'pending', next_attempt_at = ?, updated_at = ? WHERE status = 'processing' AND updated_at::timestamptz < ?::timestamptz`, now, now, cutoff); err != nil {
		return err
	}
	return nil
}

// jobLeaseTimeout is how long a task may stay in 'running' with no worker
// attached before it is returned to the queue.
//
// A live task keeps an entry in m.cancels for its whole life, so its age is
// never what this sweep reacts to - the sweep only ever sees a task no worker
// in this process owns. The window exists to cover the gap between claiming a
// task (which is what sets 'running') and registering its cancel function, and
// to stay well clear of a slow but healthy transfer when a task was orphaned by
// a process that is still running.
const jobLeaseTimeout = 10 * time.Minute

// jobLeaseInterval throttles the sweep. It is a safety net for a state that
// should not occur, not a periodic poll of a healthy system, so it is coarse.
const jobLeaseInterval = 2 * time.Minute

// recoverStrandedRunningJobs returns tasks left in 'running' by a worker that
// did not finish with them, so they can be claimed again.
//
// Startup recovery and database-outage recovery both cover the case where the
// whole process went away. This covers the one they cannot: a worker that gave
// up on its task while the process kept running. Nothing else ever looks at a
// task in 'running' - the scheduler selects 'queued', and the claim reconciler
// selects waiting items under queued, partial or failed tasks - so such a task
// could never move again, and its files stayed queued behind it forever.
//
// Only tasks with no live worker in this process are considered. The application
// is single instance by design; running a second copy would need the executors
// to be identified per process, which is a separate change.
func (m *Manager) recoverStrandedRunningJobs() error {
	m.mu.Lock()
	active := make(map[string]struct{}, len(m.cancels))
	for id := range m.cancels {
		active[id] = struct{}{}
	}
	m.mu.Unlock()
	now := time.Now().UTC()
	// Only rows already past the lease are read, and only the oldest of those.
	// Reading every running row to compare timestamps in Go made one pass
	// proportional to however many tasks a crash or an outage left behind, which
	// is exactly when the table is at its largest. Ordering oldest-first means
	// the rows most likely to be genuinely stranded are the ones inside the
	// window; a live task's row is refreshed as it works, so it sorts last.
	//
	// The stored text is RFC3339Nano, whose trailing-zero trimming makes
	// lexicographic comparison wrong, so the comparison is done as timestamptz
	// rather than as text.
	cutoff := now.Add(-jobLeaseTimeout).Format(time.RFC3339Nano)
	rows, err := m.db.Query(`SELECT id, updated_at FROM download_jobs WHERE status = 'running' AND updated_at::timestamptz <= ?::timestamptz ORDER BY updated_at, id LIMIT ?`, cutoff, reconcileBatchSize)
	if err != nil {
		return err
	}
	type stranded struct{ id, updatedAt string }
	found := make([]stranded, 0)
	for rows.Next() {
		var candidate stranded
		if err := rows.Scan(&candidate.id, &candidate.updatedAt); err != nil {
			continue
		}
		if _, live := active[candidate.id]; live {
			continue
		}
		// The comparison in the query is the same one, kept here as the
		// authority: parse rather than compare, and never requeue a task whose
		// timestamp cannot be read.
		updated, parseErr := time.Parse(time.RFC3339Nano, candidate.updatedAt)
		if parseErr != nil {
			continue
		}
		if now.Sub(updated) < jobLeaseTimeout {
			continue
		}
		found = append(found, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for _, candidate := range found {
		applog.Info("download", "stranded_task_requeued", "job_id", candidate.id, "idle_since", candidate.updatedAt)
		// The same item transition the stall watchdog performs, so a task
		// recovered here resumes exactly where a recovered task would: completed
		// files are kept, and the upstream temporary files and resume keys are
		// left in place for the next attempt.
		err := m.transitionItems(candidate.id, "running", "queued", "任务长时间没有工作线程处理，已自动恢复",
			`UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '' WHERE job_id = ? AND status IN ('running', 'downloaded')`, candidate.id)
		if err != nil {
			applog.Error("download", "stranded_task_requeue_failed", "job_id", candidate.id, "error", err.Error())
			continue
		}
		m.signal()
	}
	return nil
}

// recoverStrandedRunningJobsIfDue runs the sweep at most once per
// jobLeaseInterval.
func (m *Manager) recoverStrandedRunningJobsIfDue() {
	now := time.Now().UnixNano()
	last := m.lastJobLeaseSweep.Load()
	if now-last < int64(jobLeaseInterval) {
		return
	}
	if !m.lastJobLeaseSweep.CompareAndSwap(last, now) {
		return
	}
	if err := m.recoverStrandedRunningJobs(); err != nil {
		applog.Error("download", "stranded_task_recovery_failed", "error", err.Error())
	}
}

// runMaintenanceSweepsIfDue runs the sweeps that belong to no single task, at
// most once per inboxReapInterval, from the existing maintenance goroutine, so
// the safety nets do not add another resident database poller. Each one repairs
// a state that no other pass looks at: an expired inbox lease, a media claim
// whose owner cannot transfer, a listener event that stopped being retried, and
// a file moved but never recorded.
func (m *Manager) runMaintenanceSweepsIfDue() {
	now := time.Now().UnixNano()
	last := m.lastInboxReap.Load()
	if now-last < int64(inboxReapInterval) {
		return
	}
	if !m.lastInboxReap.CompareAndSwap(last, now) {
		return
	}
	if err := m.reapStaleInboxLeases(); err != nil {
		applog.Error("download", "inbox_lease_reap_failed", "error", err.Error())
	}
	// A claim whose owner cannot transfer blocks every other task that wants
	// that file, so it is invisible to whoever is affected and permanent until
	// something releases it. This sweep is that something.
	if err := m.orphanedMediaClaims(); err != nil {
		applog.Error("download", "media_claim_reap_failed", "error", err.Error())
	}
	// A listener event that used up its fast attempts is a download request the
	// user made and nothing else will pick up, because Telegram does not redeliver
	// an update the dispatcher acknowledged. Offering it again slowly is the
	// difference between a lost download and a late one.
	if err := m.reviveExhaustedInboxEvents(); err != nil {
		applog.Error("download", "inbox_event_revive_failed", "error", err.Error())
	}
	// The chat side reconciles files left mid-publish on every worker pass; the
	// message side only did it at startup and after a database outage, so a write
	// failure that did not take the process down left a file that was downloaded
	// and moved but never recorded - the task reported failure, and retrying it
	// then refused the destination as already occupied. Running the same
	// reconciler here closes that window without another resident poller.
	if err := m.reconcilePublishedItems(); err != nil {
		applog.Error("download", "published_file_reconcile_failed", "error", err.Error())
	}
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

// loadVisibleCounts performs the only full task-count queries in a process
// lifetime. Thereafter task creation, reactivation and deletion update these
// atomics after their database transaction commits.
func (m *Manager) loadVisibleCounts() error {
	var jobs, chats int64
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE parent_chat_id = '' AND status != 'deleted'`).Scan(&jobs); err != nil {
		return err
	}
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM chat_download_jobs WHERE status != 'deleted'`).Scan(&chats); err != nil {
		return err
	}
	m.visibleJobs.Store(jobs)
	m.visibleChats.Store(chats)
	return nil
}

func (m *Manager) visibleJobCount() int  { return int(m.visibleJobs.Load()) }
func (m *Manager) visibleChatCount() int { return int(m.visibleChats.Load()) }

func (m *Manager) jobBecameVisible(parentChatID string) {
	if parentChatID == "" {
		m.visibleJobs.Add(1)
	}
}

func (m *Manager) jobBecameHidden(parentChatID string) {
	if parentChatID == "" {
		m.visibleJobs.Add(-1)
	}
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
	err := m.db.QueryRow(`SELECT id, source_url, dialog_type, dialog_key, dialog_name, account_id, attempts, status, error, created_at, updated_at, message_text FROM download_jobs WHERE id = ?`, id).Scan(&job.ID, &job.SourceURL, &job.DialogType, &job.DialogKey, &job.DialogName, &job.AccountID, &job.Attempts, &job.Status, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.MessageText)
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
	// The counts come from the rows already in hand rather than from the
	// maintained summary, so this single-task view stays exact even if a summary
	// row were ever missing.
	for _, item := range items {
		if item.Status == "completed" {
			job.CompletedItems++
		}
	}
	return job, nil
}

// ListCursor is stable when new tasks are inserted: it continues after the
// last row seen rather than making deep pages increasingly expensive with
// OFFSET. Its jobs intentionally omit Items; file details are fetched on row
// expansion through Get.
func (m *Manager) ListCursor(cursor string, pageSize int, filters ...string) ([]Job, int, string, error) {
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 50 {
		pageSize = 50
	}
	status, err := taskStatusFilter(filters)
	if err != nil {
		return nil, 0, "", err
	}
	total, err := m.visibleJobTotal(status)
	if err != nil {
		return nil, 0, "", err
	}
	jobs, next, err := m.listCursor(cursor, pageSize, status)
	if err != nil {
		return nil, 0, "", err
	}
	return jobs, total, next, nil
}

// listCursor performs just the indexed page query; Web and Bot both retain
// the opaque cursor for their next/previous navigation.
func (m *Manager) listCursor(cursor string, pageSize int, status string) ([]Job, string, error) {
	where := "WHERE j.parent_chat_id = '' AND j.status != 'deleted'"
	whereArgs := make([]any, 0, 1)
	if status != "" {
		where += " AND j.status = ?"
		whereArgs = append(whereArgs, status)
	}
	pagination := `ORDER BY j.created_at DESC, j.id DESC LIMIT ?`
	args := append(whereArgs, pageSize+1)
	if cursor != "" {
		createdAt, id, err := decodeJobCursor(cursor)
		if err != nil {
			return nil, "", errors.New("分页游标无效，请返回第一页")
		}
		where += ` AND (j.created_at < ? OR (j.created_at = ? AND j.id < ?))`
		args = append(whereArgs, createdAt, createdAt, id, pageSize+1)
	}
	jobs, _, err := m.listSummaries(where, pagination, args, 0)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(jobs) > pageSize {
		jobs = jobs[:pageSize]
		last := jobs[len(jobs)-1]
		next = encodeJobCursor(last.CreatedAt, last.ID)
	}
	return jobs, next, nil
}

func taskStatusFilter(filters []string) (string, error) {
	if len(filters) == 0 {
		return "", nil
	}
	return NormalizeTaskStatusFilter(filters[0])
}

// visibleJobTotal uses the in-memory total for the unfiltered list. A status
// filter is an explicit, bounded user action; its indexed count is queried in
// PostgreSQL so page counts stay correct without retaining millions of task
// rows in process memory.
func (m *Manager) visibleJobTotal(status string) (int, error) {
	if status == "" {
		return m.visibleJobCount(), nil
	}
	// This count is an index-only scan whose cost grows with permanent history,
	// and a filtered list asks for it on every page fetch. Memoizing for a few
	// seconds collapses a burst of paging into one scan. It is display-only —
	// pagination uses cursors and never reads this total — so bounded staleness
	// is safe. Note this reduces how often the scan runs, not its O(history)
	// cost; a status-filtered count over millions of rows would need a maintained
	// counter, which is deliberately not done here because task status is also
	// written by several direct SQL paths that a counter would have to track.
	now := time.Now()
	m.filteredCountsMu.Lock()
	if entry, ok := m.filteredCounts[status]; ok && now.Before(entry.expires) {
		m.filteredCountsMu.Unlock()
		return entry.total, nil
	}
	m.filteredCountsMu.Unlock()
	var total int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_jobs WHERE parent_chat_id = '' AND status = ?`, status).Scan(&total); err != nil {
		return 0, err
	}
	m.filteredCountsMu.Lock()
	if m.filteredCounts == nil {
		m.filteredCounts = make(map[string]filteredJobCount)
	}
	// Keyed by status, and there are only a handful of statuses, so the map needs
	// no eviction.
	m.filteredCounts[status] = filteredJobCount{total: total, expires: now.Add(filteredCountTTL)}
	m.filteredCountsMu.Unlock()
	return total, nil
}

func (m *Manager) listSummaries(where, pagination string, args []any, total int) ([]Job, int, error) {
	// A page of tasks is read from two small tables only. The file counts come
	// from download_item_stats, which the triggers on download_items maintain,
	// and the display text lives on the task itself; neither requires reading a
	// single download_items row. That matters because a task's file count is not
	// bounded: with linked comments and replies enabled - the default - one
	// channel post's task holds a row per media comment, so even a page of fifty
	// tasks could previously cost the union of their file histories, on a list
	// the Web UI re-reads while anything is downloading.
	query := `SELECT j.id, j.source_url, j.dialog_type, j.dialog_key, j.dialog_name, j.account_id, j.attempts, j.status, j.error, j.created_at, j.updated_at,
	 COALESCE(summary.total_items, 0), COALESCE(summary.completed_items, 0), j.message_text
 FROM (SELECT * FROM download_jobs j ` + where + ` ` + pagination + `) j
 LEFT JOIN download_item_stats summary ON summary.job_id = j.id
 ORDER BY j.created_at DESC, j.id DESC`
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

// downloadCountsWindow is how far back the recent-failure figure reaches.
const downloadCountsWindow = 30 * 24 * time.Hour

// DownloadCounts reports how many files are still in flight and how many failed
// within the last 30 days, counting both ordinary message tasks and session
// (chat) tasks.
//
// It is asked for on demand - the Bot's /status command is the only caller -
// and deliberately not polled. A message task owns a file row per message or
// per linked comment, and a session task owns one per indexed media message, so
// neither count can be answered from a fixed amount of work: the active figure
// is an index-only scan over the rows still in flight plus a sum over the
// per-task summaries, and the failure figure is an index range scan over the
// last 30 days of failures. Both are cheap when little is happening and grow
// with how much is, which is exactly the shape of work that must not sit behind
// a three second poll on a dashboard nobody is reading.
//
// Errors are returned rather than swallowed. The figures are shown to a person
// who asked for them, so an unavailable database must say so instead of
// reporting a confident zero.
func (m *Manager) DownloadCounts() (active, recentFailures int, err error) {
	cutoff := time.Now().UTC().Add(-downloadCountsWindow).Format(time.RFC3339Nano)
	err = m.db.QueryRow(`SELECT
 (SELECT COUNT(1) FROM download_items WHERE status IN ('queued', 'waiting', 'running', 'downloaded', 'paused')) +
 (SELECT COALESCE(SUM(queued + waiting + running + downloaded + paused), 0) FROM chat_download_stats),
 (SELECT COUNT(1) FROM download_items WHERE status = 'failed' AND finished_at >= ?) +
 (SELECT COUNT(1) FROM chat_download_items WHERE status = 'failed' AND finished_at >= ?)`, cutoff, cutoff).Scan(&active, &recentFailures)
	if err != nil {
		return 0, 0, err
	}
	return active, recentFailures, nil
}

// ActiveAccountJobs reports every non-terminal message or chat task that would
// lose its bound Telegram session if the account were removed.
func (m *Manager) ActiveAccountJobs(accountID string) (int, error) {
	var count int
	err := m.db.QueryRow(`SELECT
 (SELECT COUNT(1) FROM download_jobs WHERE account_id = ? AND status IN ('queued', 'running', 'paused')) +
 (SELECT COUNT(1) FROM chat_download_jobs WHERE account_id = ? AND status IN ('queued', 'scanning', 'downloading', 'listening', 'paused'))`, accountID, accountID).Scan(&count)
	return count, err
}

func (m *Manager) LiveProgress() []FileProgress {
	return m.progress.Snapshot()
}

// Stop asks all active upstream transfers to stop at a resumable boundary.
// It is called before process shutdown so a restart does not depend on a hard
// kill to recover task state.
func (m *Manager) Stop() {
	// Signal the worker loops first. They are not stopped by cancelling a task
	// context, so without this they keep polling for the whole shutdown window —
	// and once the database below is closed, every poll is an error against a
	// closed pool.
	if m.stopCh != nil {
		m.stopOnce.Do(func() { close(m.stopCh) })
	}
	if m.dbMonitorStop != nil {
		m.dbMonitorStop()
	}
	if m.cleanupStop != nil {
		m.cleanupStop()
	}
	if m.reconcileStop != nil {
		m.reconcileStop()
	}
	if m.inboxRetryStop != nil {
		m.inboxRetryStop()
	}
	// Every in-flight transfer is cancelled before the database closes, message
	// and session alike. Session batches were left out, so shutdown cut them off
	// mid-write against a pool that was closing underneath them; the same map is
	// cancelled for a database outage, where the reasoning is identical.
	m.cancelTransfersForDatabaseOutage()
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

// taskMessageText picks the text shown for a whole task. Every file of one
// message or album carries the same caption, and a first file without one must
// not hide a caption that a later file of the same group has.
func taskMessageText(sources []source) string {
	for _, item := range sources {
		if item.MessageText != "" {
			return item.MessageText
		}
	}
	return ""
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
	for attempt := 0; attempt < maxDuplicateRetries; attempt++ {
		submission, retry, err := m.enqueueIntentParentSnapshotAttempt(intent, sources, direct, parentChatID, configJSON)
		if err != nil {
			return Submission{}, err
		}
		if !retry {
			return submission, nil
		}
	}
	return Submission{}, errors.New("任务创建竞争过于频繁，请稍后重试")
}

// enqueueIntentParentSnapshotAttempt returns retry=true only when another
// request claimed a media identity between the preflight lookup and insert.
// The caller uses a bounded loop so contention cannot grow the call stack.
func (m *Manager) enqueueIntentParentSnapshotAttempt(intent DownloadIntent, sources []source, direct directPeer, parentChatID, configJSON string) (Submission, bool, error) {
	id, err := randomID()
	if err != nil {
		return Submission{}, false, err
	}
	requestID, err := randomID()
	if err != nil {
		return Submission{}, false, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	trigger := triggerJSON(intent.Trigger)
	tx, err := m.db.Begin()
	if err != nil {
		return Submission{}, false, err
	}
	defer tx.Rollback()
	var existingID string
	for _, item := range sources {
		var jobID string
		err = tx.QueryRow(`SELECT job_id FROM download_items WHERE dialog_key = ? AND message_id = ? LIMIT 1`, item.DialogKey, item.MessageID).Scan(&jobID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Submission{}, false, err
		}
		if jobID != "" {
			if existingID != "" && existingID != jobID {
				// Two different tasks already own files of this group. That is a
				// property of the stored rows, not of when this request was made,
				// so no later attempt can merge them - the inbox must stop on the
				// first one and show it rather than retrying a decision that has
				// already been made.
				return Submission{}, false, permanentFailure(errors.New("消息组中的文件已关联到不同下载任务，无法安全合并"))
			}
			existingID = jobID
		}
	}
	if existingID != "" {
		var existingStatus, existingParent string
		// Lock the durable owner before changing its item rows. A concurrent
		// submission must observe either the old terminal task or the fully
		// reactivated result, never a queued task with only completed items.
		if err := tx.QueryRow(`SELECT status, parent_chat_id FROM download_jobs WHERE id = ? FOR UPDATE`, existingID).Scan(&existingStatus, &existingParent); err != nil {
			return Submission{}, false, err
		}
		reactivated := existingStatus == "cancelled" || existingStatus == "deleted"
		reactivationStatus := "queued"
		if reactivated {
			// Completed files are already safely published and must never be
			// downloaded again. Every other file restarts or resumes normally.
			if _, err := tx.Exec(`UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE job_id = ? AND status != 'completed'`, existingID); err != nil {
				return Submission{}, false, err
			}
			// A completed database row is only durable proof while its published
			// file still exists. Users may move final files outside this service;
			// make such rows eligible so claimMedia can safely reclaim them.
			rows, err := tx.Query(`SELECT dialog_key, message_id, final_path FROM download_items WHERE job_id = ? AND status = 'completed'`, existingID)
			if err != nil {
				return Submission{}, false, err
			}
			type completedItem struct {
				dialogKey string
				messageID int
				path      string
			}
			missing := make([]completedItem, 0)
			for rows.Next() {
				var item completedItem
				if err := rows.Scan(&item.dialogKey, &item.messageID, &item.path); err != nil {
					_ = rows.Close()
					return Submission{}, false, err
				}
				if !regularFileExists(item.path) {
					missing = append(missing, item)
				}
			}
			if err := rows.Close(); err != nil {
				return Submission{}, false, err
			}
			for _, item := range missing {
				if _, err := tx.Exec(`UPDATE download_items SET status = 'queued', final_path = '', error = '', started_at = '', finished_at = '', elapsed_ms = 0 WHERE job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'completed'`, existingID, item.dialogKey, item.messageID); err != nil {
					return Submission{}, false, err
				}
			}
			var pending int
			if err := tx.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status != 'completed'`, existingID).Scan(&pending); err != nil {
				return Submission{}, false, err
			}
			// Reopening a deleted task whose files are all still published is a
			// successful deduplicated submission, not a task with work to queue.
			if pending == 0 {
				reactivationStatus = "completed"
			}
			// A cancelled/deleted child can be discovered again by a new chat
			// download. Reattach it to that visible parent so later parent controls
			// operate on the reactivated work instead of its deleted predecessor.
			result, err := tx.Exec(`UPDATE download_jobs SET status = ?, error = '', updated_at = ?, parent_chat_id = CASE WHEN ? <> '' THEN ? ELSE parent_chat_id END, config_json = CASE WHEN ? <> '' THEN ? ELSE config_json END WHERE id = ? AND status IN ('cancelled', 'deleted')`, reactivationStatus, now, parentChatID, parentChatID, configJSON, configJSON, existingID)
			if err != nil {
				return Submission{}, false, err
			}
			changed, err := result.RowsAffected()
			if err != nil {
				return Submission{}, false, err
			}
			// A concurrent request already reactivated this job while we were
			// waiting for its row lock. It is now a normal duplicate request.
			reactivated = changed == 1
		}
		if _, err = tx.Exec(`INSERT INTO download_requests(id, job_id, source_kind, account_id, source_url, dialog_key, message_id, trigger_json, outcome, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'duplicate', ?)`, requestID, existingID, intent.Source, intent.AccountID, intent.URL, sources[0].DialogKey, sources[0].MessageID, trigger, now); err != nil {
			return Submission{}, false, err
		}
		if err = tx.Commit(); err != nil {
			return Submission{}, false, err
		}
		job, err := m.Get(existingID)
		if err != nil {
			return Submission{}, false, err
		}
		if reactivated {
			if existingStatus == "deleted" {
				m.jobBecameVisible(existingParent)
			}
			m.touch()
			m.emit(existingID, requestID, "job_reactivated", reactivationStatus)
			if reactivationStatus == "queued" {
				m.signal()
			}
			return Submission{RequestID: requestID, Job: job, Duplicate: true, Reactivated: true}, false, nil
		}
		m.emit(existingID, requestID, "request_attached", job.Status)
		return Submission{RequestID: requestID, Job: job, Duplicate: true}, false, nil
	}
	// The task's display text is one message's caption and is identical for every
	// file of the task, so it is stored once on the task rather than repeated on
	// every file row. The task list then never has to read download_items for it.
	messageText := taskMessageText(sources)
	if _, err = tx.Exec(`INSERT INTO download_jobs(id, source_url, dialog_type, dialog_key, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, parent_chat_id, config_json, message_text, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`, id, intent.URL, sources[0].DialogType, sources[0].DialogKey, sources[0].DialogName, intent.AccountID, direct.kind, direct.id, direct.hash, parentChatID, configJSON, messageText, now, now); err != nil {
		return Submission{}, false, err
	}
	for _, item := range sources {
		result, insertErr := tx.Exec(`INSERT INTO download_items(job_id, dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, origin_dialog_name, origin_message_id, is_comment, source_peer_type, source_peer_id, source_peer_hash, original_name, size, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued') ON CONFLICT(dialog_key, message_id) DO NOTHING`, id, item.DialogType, item.DialogKey, item.DialogID, item.MessageID, item.GroupedID, item.MessageText, item.OriginDialogName, item.OriginMessageID, boolInt(item.IsComment), item.SourcePeerType, item.SourcePeerID, item.SourcePeerHash, item.OriginalName, item.Size)
		if insertErr != nil {
			return Submission{}, false, insertErr
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			// A competing Web/Bot/reaction request committed after the
			// preflight lookup. Discard this incomplete job and resolve through
			// the normal duplicate path against the durable owner.
			_ = tx.Rollback()
			return Submission{}, true, nil
		}
	}
	if _, err = tx.Exec(`INSERT INTO download_requests(id, job_id, source_kind, account_id, source_url, dialog_key, message_id, trigger_json, outcome, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'created', ?)`, requestID, id, intent.Source, intent.AccountID, intent.URL, sources[0].DialogKey, sources[0].MessageID, trigger, now); err != nil {
		return Submission{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Submission{}, false, err
	}
	m.jobBecameVisible(parentChatID)
	m.touch()
	job := Job{ID: id, SourceURL: intent.URL, DialogType: sources[0].DialogType, DialogKey: sources[0].DialogKey, DialogName: sources[0].DialogName, MessageText: messageText, HasPublicLink: isPublicMessageLink(intent.URL), AccountID: intent.AccountID, DirectPeerType: direct.kind, DirectPeerID: direct.id, DirectPeerHash: direct.hash, ConfigJSON: configJSON, Status: "queued", CreatedAt: now, UpdatedAt: now, TotalItems: len(sources)}
	m.emit(id, requestID, "job_created", "queued")
	m.signal()
	// Message tasks are interactive work. Wake chat workers so their next
	// historical batch observes this newly queued higher-priority task.
	m.signalChat()
	return Submission{RequestID: requestID, Job: job, Created: true}, false, nil
}

func (m *Manager) worker() {
	for {
		job, sources, err := m.nextQueued()
		if err == nil && job.ID != "" {
			release, acquired := m.tryAcquireTransfer(transferMessage, false)
			if !acquired {
				select {
				case <-m.stopCh:
					return
				case <-m.slotWake:
				case <-time.After(time.Second):
				}
				continue
			}
			m.run(job, sources)
			release()
			continue
		}
		select {
		case <-m.stopCh:
			return
		case <-m.wake:
		case <-time.After(messageIdleInterval):
		}
	}
}

func (m *Manager) nextQueued() (Job, []source, error) {
	// Accounts inside a cooldown are excluded by the query, not only by the
	// in-memory guard below: the candidate window is bounded, so filtering only
	// in Go would let sixteen blocked tasks at the head of the queue hide every
	// other account's work behind them. The persisted cooldown is compared as a
	// timestamptz because the stored text uses RFC3339Nano, whose trailing-zero
	// trimming makes lexicographic comparison wrong.
	rows, err := m.db.Query(`SELECT id, source_url, dialog_type, dialog_key, dialog_name, account_id, direct_peer_type, direct_peer_id, direct_peer_hash, config_json, attempts, status, error, created_at, updated_at FROM download_jobs WHERE status = 'queued' AND EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = download_jobs.id AND i.status = 'queued') AND NOT EXISTS (SELECT 1 FROM telegram_rate_limits l WHERE l.account_id = download_jobs.account_id AND l.blocked_until::timestamptz > ?::timestamptz) ORDER BY created_at LIMIT ?`, time.Now().UTC().Format(time.RFC3339Nano), queuedCandidateWindow)
	if err != nil {
		return Job{}, nil, err
	}
	var job Job
	found := false
	for rows.Next() {
		var candidate Job
		if err := rows.Scan(&candidate.ID, &candidate.SourceURL, &candidate.DialogType, &candidate.DialogKey, &candidate.DialogName, &candidate.AccountID, &candidate.DirectPeerType, &candidate.DirectPeerID, &candidate.DirectPeerHash, &candidate.ConfigJSON, &candidate.Attempts, &candidate.Status, &candidate.Error, &candidate.CreatedAt, &candidate.UpdatedAt); err != nil {
			_ = rows.Close()
			return Job{}, nil, err
		}
		// A task whose account is inside a Telegram cooldown is left queued
		// instead of being claimed and abandoned. Claiming increments attempts,
		// so claiming during a cooldown would spend the retry budget on a window
		// where no transfer is allowed to start. Looking a few rows ahead keeps
		// one blocked account from stalling every other account's tasks.
		if m.telegramAccountBlocked(candidate.AccountID) {
			continue
		}
		job = candidate
		found = true
		break
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return Job{}, nil, err
	}
	if err := rows.Close(); err != nil {
		return Job{}, nil, err
	}
	if !found {
		return Job{}, nil, nil
	}
	sources, err := m.sources(job.ID)
	return job, sources, err
}

// finishTaskWithoutPendingWork settles a task whose item loop selected nothing
// to transfer. Every outcome has to be written: a task left at 'running' is
// never claimed again, because the scheduler only picks 'queued' rows, so
// returning without a terminal or queued state strands it until the user acts.
//
// Only the task status is written here. Per-item errors carry the actionable
// diagnostics (which path is blocked, why a file failed) and must not be
// overwritten with a generic message.
func (m *Manager) finishTaskWithoutPendingWork(jobID string, waitingForOwner bool) {
	if m.allItemsCompleted(jobID) {
		_ = m.setJob(jobID, "completed", "")
		// Every file is published, so this task's private temporary directory
		// holds only leftovers. The usual case is a task that paused part-way and
		// then found another task had completed the media: its own partial
		// download is never resumed, because the items were adopted rather than
		// transferred. The normal completion paths remove this directory, and
		// this one returns before reaching them, so without this the partial file
		// stays on disk until the task is deleted.
		_ = os.RemoveAll(filepath.Join(m.downloadDir, ".tdl-tmp", jobID))
		return
	}
	if waitingForOwner || m.hasWaitingMessageItems(jobID) {
		_ = m.setJob(jobID, "queued", "等待其他任务完成同一文件")
		return
	}
	var completed int
	if err := m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status = 'completed'`, jobID).Scan(&completed); err != nil {
		return
	}
	if completed > 0 {
		_ = m.setJob(jobID, "partial", "部分文件未完成，可重试失败文件")
		return
	}
	_ = m.setJob(jobID, "failed", "没有可继续下载的文件，请查看各文件的失败原因")
}

// settleQueuedJobsWithoutPendingWork repairs a task the scheduler can never
// pick up again. nextQueued only selects a queued job that still has a queued
// item, so a job left queued with every item already terminal waits at
// "排队中" forever with nothing to do and no way for the user to tell that it is
// finished. Interrupted-task recovery causes exactly this: it settles a job
// whose items are all completed, but a job interrupted after its last item
// failed is reset to queued with nothing queued behind it.
//
// Jobs with waiting items are excluded because reconcileMessageClaims owns
// them: a waiting item is work, not a terminal outcome, and settling the parent
// here would fight that pass.
// It walks its set in id order from a cursor rather than re-reading the same
// bounded prefix every few seconds. The predicate it tests is expensive to
// disprove - two index probes per queued task - so a fixed head made the cost of
// one pass grow with the queue behind it while never reaching the rest.
func (m *Manager) settleQueuedJobsWithoutPendingWork(cursor string) (string, error) {
	rows, err := m.db.Query(`SELECT j.id FROM download_jobs j
WHERE j.status = 'queued' AND j.parent_chat_id = '' AND j.id > ?
  AND EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = j.id)
  AND NOT EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = j.id AND i.status IN ('queued', 'waiting'))
ORDER BY j.id LIMIT ?`, cursor, reconcileBatchSize)
	if err != nil {
		return cursor, err
	}
	ids := make([]string, 0, reconcileBatchSize)
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			_ = rows.Close()
			return cursor, scanErr
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return cursor, err
	}
	if err := rows.Close(); err != nil {
		return cursor, err
	}
	// finishTaskWithoutPendingWork owns the decision, so a stranded task ends up
	// in exactly the state the normal path would have written.
	for _, id := range ids {
		m.finishTaskWithoutPendingWork(id, false)
	}
	// A short page means this rotation reached the end of the set.
	next := cursor
	if len(ids) > 0 {
		next = ids[len(ids)-1]
	}
	if len(ids) < reconcileBatchSize {
		next = ""
	}
	return next, nil
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
	transferCtx, stopTransfer := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancels[job.ID] = stopTransfer
	m.mu.Unlock()
	defer func() { stopTransfer(); cancel(); m.mu.Lock(); delete(m.cancels, job.ID); m.mu.Unlock() }()
	defer m.progress.ClearJob(job.ID)
	// Resolved before the item loop and reused below, so the pre-flight check and
	// the publish path cannot disagree about a file's destination. A snapshot
	// that fails to parse is left to the existing check further down, which
	// already reports it.
	preflightConfig, preflightErr := m.downloadConfigForJob(job.ConfigJSON)
	pending := make([]source, 0, len(sources))
	waitingForOwner := false
	for _, item := range sources {
		if ctx.Err() != nil || !m.runningOrUnknown(job.ID) {
			return
		}
		if item.Status == "completed" || (item.Status == "downloaded" && regularFileExists(item.FinalPath)) {
			continue
		}
		// Publishing never overwrites, so a destination already held by a
		// non-regular entry can never be published. Fail before the transfer
		// rather than after a complete re-download, so a retry does not repeat
		// the whole download only to hit the same refusal, and name the path
		// that has to be cleared.
		if preflightErr == nil {
			if finalPath, destErr := finalDestination(m.downloadDir, preflightConfig.FinalFilenameTemplate, item); destErr == nil {
				if occupied, regularFile := destinationOccupied(finalPath); occupied && !regularFile {
					if err := m.setItem(item, "failed", "", blockedDestinationMessage(finalPath)); err != nil {
						m.fail(job.ID, err)
						return
					}
					continue
				}
			}
		}
		claim, path, claimErr := m.claimMessageMedia(job.ID, item)
		if claimErr != nil {
			m.fail(job.ID, fmt.Errorf("确认文件归属失败: %w", claimErr))
			return
		}
		switch claim {
		case "completed":
			if err := m.adoptCompletedMessageItem(job.ID, item, path); err != nil {
				m.fail(job.ID, fmt.Errorf("复用已完成文件失败: %w", err))
				return
			}
			continue
		case "waiting":
			if err := m.setMessageItemWaiting(job.ID, item); err != nil {
				m.fail(job.ID, fmt.Errorf("保存文件等待状态失败: %w", err))
				return
			}
			waitingForOwner = true
			continue
		}
		pending = append(pending, item)
	}
	if len(pending) == 0 {
		m.finishTaskWithoutPendingWork(job.ID, waitingForOwner)
		return
	}
	// Recorded for the whole pending set at once. The per-file shape wrote one
	// rounded trip per file, serially, before the first byte of the transfer
	// started, so a task holding a media comment per row - the default for a
	// channel post - spent its entire startup in the database while the worker
	// that could have been transferring sat idle.
	if err := m.beginItemAttempts(pending); err != nil {
		m.fail(job.ID, fmt.Errorf("记录文件下载尝试失败: %w", err))
		return
	}
	if ctx.Err() != nil || !m.runningOrUnknown(job.ID) {
		return
	}
	// A media transfer must not fail merely because it is slow. Cancellation is
	// controlled by pause/cancel and graceful application shutdown instead of a
	// fixed wall-clock deadline.
	tmpRoot := filepath.Join(m.downloadDir, ".tdl-tmp", job.ID)
	config := m.settings.Get()
	if snapshot, snapshotErr := m.downloadConfigForJob(job.ConfigJSON); snapshotErr != nil {
		m.fail(job.ID, snapshotErr)
		return
	} else {
		config.Download = snapshot
	}
	watchdog := startTransferWatchdog(transferCtx, upstreamWatchPeriod, upstreamInitialTimeout, upstreamIdleTimeout, stopTransfer, func() bool {
		if m.stopForInactiveParent(job.ID) {
			return false
		}
		// An unreadable status must not be mistaken for a stopped task: the
		// watchdog would abort a healthy transfer.
		return m.runningOrUnknown(job.ID)
	}, func(timeout time.Duration) {
		applog.Info("download", "task_no_progress_timeout", "job_id", job.ID, "timeout", timeout.String())
	})
	defer watchdog.Close()
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
	err = m.accounts.Run(transferCtx, job.AccountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		// Upstream callbacks identify only a message ID. Execute one real
		// Telegram dialog at a time so a discussion-group comment cannot collide
		// with a channel post that happens to have the same numeric message ID.
		groups := make([][]source, 0, 2)
		groupIndex := make(map[string]int)
		for _, item := range pending {
			direct := item.Direct
			if direct.kind == "" {
				direct = directPeer{kind: job.DirectPeerType, id: job.DirectPeerID, hash: job.DirectPeerHash}
			}
			key := fmt.Sprintf("%s:%d:%d", direct.kind, direct.id, direct.hash)
			index, exists := groupIndex[key]
			if !exists {
				index = len(groups)
				groupIndex[key] = index
				groups = append(groups, nil)
			}
			item.Direct = direct
			groups[index] = append(groups[index], item)
		}
		for groupNumber, batch := range groups {
			peer := batch[0].Direct.inputPeer()
			if peer == nil {
				return errors.New("下载文件缺少 Telegram 会话引用")
			}
			tmpDir, err := temporaryDialogDirectory(tmpRoot, batch[0].DialogKey)
			if err != nil {
				return fmt.Errorf("创建会话临时目录: %w", err)
			}
			byMessage := make(map[int]source, len(batch))
			messageIDs := make([]int, 0, len(batch))
			seenMessages := make(map[int]struct{}, len(batch))
			seenGroups := make(map[int64]struct{})
			for _, item := range batch {
				byMessage[item.MessageID] = item
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
			opts := upstreamDL.Options{Dir: tmpDir, Template: temporaryFilenameTemplate, Group: true, Continue: true, Restart: restart && groupNumber == 0, Quiet: true, Runtime: &upstreamDL.RuntimeOptions{Threads: config.Download.Threads, TaskLimit: config.Download.TaskLimit, PoolSize: config.Download.PoolSize, Delay: time.Duration(config.Download.DelayMS) * time.Millisecond, DisableProgressPS: true}, DirectDialogs: [][]*tmessage.Dialog{{{Peer: peer, Messages: messageIDs}}}, ProgressCallback: func(update upstreamDL.ProgressUpdate) {
				if !m.runningOrUnknown(job.ID) {
					return
				}
				watchdog.Touch()
				item, ok := byMessage[update.MessageID]
				if !ok {
					return
				}
				if started, _ := m.progress.Update(job.ID, item.Item, update); started {
					recordStateErr(m.markItemStarted(item))
				}
			}, FileCompletedCallback: func(update upstreamDL.FileCompletedUpdate) {
				if !m.runningOrUnknown(job.ID) {
					return
				}
				watchdog.Touch()
				item, ok := byMessage[update.MessageID]
				if !ok {
					return
				}
				recordStateErr(m.markItemFinished(item))
				m.progress.ClearItem(job.ID, item.Item)
				publishWG.Add(1)
				go func(item source, path string) {
					defer publishWG.Done()
					recordStateErr(m.publishItem(job.ID, path, item, config))
				}(item, update.Path)
			}}
			if err := upstreamDL.Run(ctx, client, kvd, opts); err != nil {
				return err
			}
		}
		return nil
	})
	// File completion callbacks run in the upstream download workers. Their
	// publishing work is deliberately asynchronous, but the job state must not
	// be decided until every accepted file has finished moving.
	publishWG.Wait()
	stateMu.Lock()
	persistErr := stateErr
	stateMu.Unlock()
	if persistErr != nil {
		m.fail(job.ID, fmt.Errorf("保存下载状态失败: %w", persistErr))
		return
	}
	if watchdog.Stalled() && m.status(job.ID) == "running" {
		if err := m.requeueStalledJob(job.ID); err != nil {
			m.fail(job.ID, fmt.Errorf("无进度下载自动恢复失败: %w", err))
			return
		}
		applog.Info("download", "task_requeued_after_no_progress", "job_id", job.ID)
		m.signal()
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
		if !m.runningOrUnknown(job.ID) {
			return
		}
		// Upstream can return an error while persisting resume metadata after all
		// media callbacks have already completed and every final move succeeded.
		// The observable task result is still successful in that case.
		if m.allItemsCompleted(job.ID) {
			_ = m.setJob(job.ID, "completed", "")
			_ = os.RemoveAll(tmpRoot)
			return
		}
		if m.recordTelegramRPCError(job.AccountID, err) {
			_ = m.requeueRateLimitedMessage(job.ID, pending, err)
			return
		}
		m.fail(job.ID, err)
		return
	}
	// A cancellation can race with the upstream call finishing. Never publish a
	// completed temporary file after the user has paused or cancelled its job.
	// The temporary file and upstream resume state stay intact for a later resume.
	// A status that cannot be read is not a cancellation: returning here would
	// abandon the task in 下载中 with its files already on disk.
	if status, ok := m.jobStatus(job.ID); ok && status != "running" {
		if status == "paused" {
			for _, item := range pending {
				m.pauseItem(item)
			}
		}
		return
	}
	if !m.allItemsCompleted(job.ID) {
		if m.hasWaitingMessageItems(job.ID) {
			_ = m.setJob(job.ID, "queued", "等待其他任务完成同一文件")
			return
		}
		m.fail(job.ID, errors.New("部分文件未收到收尾完成回调或移动失败"))
	} else {
		_ = m.setJob(job.ID, "completed", "")
		_ = os.RemoveAll(tmpRoot)
	}
}

// requeueRateLimitedMessage returns a flood-limited task to the queue. It
// deliberately does not signal: the account-wide cooldown now keeps the task
// from being claimed before the window expires, so waking a worker would only
// have it re-examine a task it must skip.
//
// attempts is incremented when a task is claimed, so a task that keeps meeting
// the same flood is bounded here instead of retrying forever. Every retry also
// extends blocked_until through recordTelegramRPCError, which deepens the
// restriction the retries are trying to wait out.
func (m *Manager) requeueRateLimitedMessage(id string, pending []source, cause error) error {
	var attempts int
	if err := m.db.QueryRow(`SELECT attempts FROM download_jobs WHERE id = ?`, id).Scan(&attempts); err != nil {
		return err
	}
	if attempts >= maxStalledAttempts {
		m.fail(id, fmt.Errorf("Telegram 限流反复出现，已停止自动重试，请稍后手动重试: %w", cause))
		return nil
	}
	if _, err := m.db.Exec(`UPDATE download_items SET status = 'queued', error = 'Telegram 限流中，等待自动恢复', started_at = '', finished_at = '' WHERE job_id = ? AND status = 'running'`, id); err != nil {
		return err
	}
	return m.setJob(id, "queued", "Telegram 限流中，等待自动恢复")
}

// stopForInactiveParent makes a running chat child obey its durable parent
// state independently of the request that initiated pause/cancel.
func (m *Manager) stopForInactiveParent(jobID string) bool {
	var parentID, parentStatus string
	err := m.db.QueryRow(`SELECT j.parent_chat_id, COALESCE(c.status, '') FROM download_jobs j LEFT JOIN chat_download_jobs c ON c.id = j.parent_chat_id WHERE j.id = ?`, jobID).Scan(&parentID, &parentStatus)
	if err != nil || parentID == "" || (parentStatus != ChatStatusPaused && parentStatus != ChatStatusCancelled) {
		return false
	}
	next, message := "paused", "父会话已暂停"
	if parentStatus == ChatStatusCancelled {
		next, message = "cancelled", "父会话已取消"
	}
	result, err := m.db.Exec(`UPDATE download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ? AND status = 'running'`, next, message, time.Now().UTC().Format(time.RFC3339Nano), jobID)
	if err != nil {
		applog.Error("download", "parent_stop_state_save_failed", "job_id", jobID, "error", err.Error())
		return false
	}
	changed, _ := result.RowsAffected()
	if changed == 1 {
		m.touch()
		m.emit(jobID, "", "job_status_changed", next)
	}
	return changed == 1
}

// requeueStalledJob preserves completed media and upstream resume data while
// releasing a task whose upstream call stopped making observable progress.
func (m *Manager) requeueStalledJob(id string) error {
	// Bounded exactly like the flood path. Without this a file the source never
	// delivers restarts the task forever: each cycle reopens a Telegram client,
	// rewinds the visible progress and never produces a terminal state, so the
	// task can never be retried or diagnosed by the user either.
	var attempts int
	if err := m.db.QueryRow(`SELECT attempts FROM download_jobs WHERE id = ?`, id).Scan(&attempts); err != nil {
		return err
	}
	if attempts >= maxStalledAttempts {
		return errors.New("连续多次长时间无进度，已停止自动重试，请稍后手动重试")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return m.transitionItems(id, "running", "queued", "下载长时间无进度，已自动重试未完成文件", `UPDATE download_items
SET status = 'queued', error = '下载长时间无进度，已自动重试',
    elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END,
    started_at = '', finished_at = ''
WHERE job_id = ? AND status IN ('queued', 'running')`, now, id)
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// downloadConfigForJob resolves the download settings a task runs with: the
// snapshot captured when the task was created, falling back to current settings.
// Pre-flight checks and the publish path must resolve it identically, or they
// would disagree about which destination a file is destined for.
func (m *Manager) downloadConfigForJob(jobConfigJSON string) (settings.Download, error) {
	config := m.settings.Get().Download
	if jobConfigJSON != "" {
		if err := json.Unmarshal([]byte(jobConfigJSON), &config); err != nil {
			return config, fmt.Errorf("任务下载配置快照无效: %w", err)
		}
	}
	return config, nil
}

// destinationOccupied reports whether anything already exists at path, and
// whether that entry is a published regular file. The two answers are used for
// different decisions and must never be conflated: publishing refuses to
// overwrite *anything*, while only a regular file is evidence that this media
// was already downloaded. Treating a directory or a dangling symlink as "already
// downloaded" skips a file that was never fetched, and treating it as "missing"
// re-downloads a file that publishing will refuse again — the loop this helper
// exists to break.
func destinationOccupied(path string) (occupied bool, publishedFile bool) {
	info, err := os.Stat(path)
	if err != nil {
		return false, false
	}
	return true, info.Mode().IsRegular()
}

// blockedDestinationMessage describes a destination that publishing cannot use
// and that re-downloading cannot clear, so the operator is told exactly what to
// remove instead of seeing the generic "already exists" text on every retry.
func blockedDestinationMessage(path string) string {
	return fmt.Sprintf("目标路径已被占用且不是普通文件，不会覆盖也无法自动重试，请手动处理后重试: %s", path)
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

// dialogIdentityForPeer preserves the stable InputPeer-based identity key but
// refines the display kind from flags that are only visible on a resolved peer.
// A supergroup and a broadcast channel are both InputPeerChannel at the
// protocol level, and a bot is a User at the protocol level.
//
// Only the returned kind may change here: the key must stay derived from
// dialogIdentity so deduplication and direct-peer round-trips are unaffected.
func dialogIdentityForPeer(peer peers.Peer, accountID string) (kind, key string, id int64) {
	kind, key, id = dialogIdentity(peer.InputPeer(), accountID)
	if channel, ok := peer.(peers.Channel); ok && !channel.IsBroadcast() {
		return "chat", key, id
	}
	// Read the decoded field, not GetBot(): the accessor reports the wire flag,
	// which is only populated alongside the field on the decode path.
	if user, ok := peer.(peers.User); ok && user.Raw().Bot {
		return "bot", key, id
	}
	return kind, key, id
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

func (m *Manager) hasWaitingMessageItems(jobID string) bool {
	var waiting int
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status = 'waiting'`, jobID).Scan(&waiting)
	return waiting > 0
}

// claimMessageMedia is the normal-task counterpart of claimChatMedia. The
// downloaded_media row is the sole cross-task ownership record: a message
// link must adopt a completed chat download, or wait for an active chat owner,
// rather than downloading the same Telegram media a second time.
func (m *Manager) claimMessageMedia(jobID string, item source) (string, string, error) {
	return m.claimMedia("message", jobID, item)
}

// claimMedia atomically returns one of queued, waiting, or completed. It is
// shared by message and chat tasks so their collision rules cannot drift.
func (m *Manager) claimMedia(ownerKind, ownerID string, item source) (string, string, error) {
	for attempt := 0; attempt < maxDuplicateRetries; attempt++ {
		tx, err := m.db.Begin()
		if err != nil {
			return "", "", err
		}
		var status, path, currentKind, currentID string
		err = tx.QueryRow(`SELECT status, final_path, owner_kind, owner_id FROM downloaded_media WHERE dialog_key = ? AND message_id = ?`, item.DialogKey, item.MessageID).Scan(&status, &path, &currentKind, &currentID)
		if errors.Is(err, sql.ErrNoRows) {
			result, insertErr := tx.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, status, owner_kind, owner_id, updated_at) VALUES (?, ?, 'claimed', ?, ?, ?) ON CONFLICT(dialog_key, message_id) DO NOTHING`, item.DialogKey, item.MessageID, ownerKind, ownerID, time.Now().UTC().Format(time.RFC3339Nano))
			if insertErr != nil {
				_ = tx.Rollback()
				return "", "", insertErr
			}
			changed, _ := result.RowsAffected()
			if changed == 1 {
				if err = tx.Commit(); err != nil {
					return "", "", err
				}
				return "queued", "", nil
			}
			_ = tx.Rollback()
			continue
		}
		if err != nil {
			_ = tx.Rollback()
			return "", "", err
		}
		if status == "completed" && path != "" && regularFileExists(path) {
			if err = tx.Commit(); err != nil {
				return "", "", err
			}
			return "completed", path, nil
		}
		if status == "claimed" && currentKind == ownerKind && currentID == ownerID {
			if err = tx.Commit(); err != nil {
				return "", "", err
			}
			return "queued", "", nil
		}
		if status == "claimed" {
			if err = tx.Commit(); err != nil {
				return "", "", err
			}
			return "waiting", "", nil
		}
		// A completed row whose file was removed is no longer proof of a
		// successful download. Replace it only if its observed state did not
		// change while this transaction was running.
		result, updateErr := tx.Exec(`UPDATE downloaded_media SET final_path = '', status = 'claimed', owner_kind = ?, owner_id = ?, updated_at = ? WHERE dialog_key = ? AND message_id = ? AND status = ?`, ownerKind, ownerID, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID, status)
		if updateErr != nil {
			_ = tx.Rollback()
			return "", "", updateErr
		}
		changed, _ := result.RowsAffected()
		if err = tx.Commit(); err != nil {
			return "", "", err
		}
		if changed == 1 {
			return "queued", "", nil
		}
	}
	return "", "", errors.New("文件归属竞争过于频繁，请重试")
}

func (m *Manager) adoptCompletedMessageItem(jobID string, item source, path string) error {
	_, err := m.db.Exec(`UPDATE download_items SET status = 'completed', final_path = ?, error = '', finished_at = CASE WHEN finished_at = '' THEN ? ELSE finished_at END WHERE job_id = ? AND dialog_key = ? AND message_id = ? AND status != 'completed'`, path, time.Now().UTC().Format(time.RFC3339Nano), jobID, item.DialogKey, item.MessageID)
	if err == nil {
		m.touch()
	}
	return err
}

func (m *Manager) setMessageItemWaiting(jobID string, item source) error {
	_, err := m.db.Exec(`UPDATE download_items SET status = 'waiting', error = '等待其他任务完成同一文件', started_at = '', finished_at = '' WHERE job_id = ? AND dialog_key = ? AND message_id = ? AND status NOT IN ('completed', 'waiting')`, jobID, item.DialogKey, item.MessageID)
	if err == nil {
		m.touch()
		// A new waiting item is only useful once the reconciler looks at it, and
		// the claim it is waiting on may already be gone.
		m.signalReconcile()
	}
	return err
}

// completedMediaPath reports the final path of a file another task already
// finished, using a read-only lookup. A task that cannot transfer anything must
// never acquire a claim, so it needs a way to adopt a finished file without
// taking ownership of it.
func (m *Manager) completedMediaPath(item source) (string, bool) {
	var path string
	if err := m.db.QueryRow(`SELECT final_path FROM downloaded_media WHERE dialog_key = ? AND message_id = ? AND status = 'completed'`, item.DialogKey, item.MessageID).Scan(&path); err != nil {
		return "", false
	}
	if path == "" || !regularFileExists(path) {
		return "", false
	}
	return path, true
}

// ScheduleInboxRetry hands a failed inbox write to the shared bounded retry
// queue. Adapters that persist their own inbox rows (the reaction service) use
// it so both queues degrade the same way instead of each inventing a policy.
func (m *Manager) ScheduleInboxRetry(describe string, run func() error) {
	m.scheduleInboxRetry(describe, run)
}

func (m *Manager) scheduleInboxRetry(describe string, run func() error) {
	if m.inboxRetry == nil {
		// A Manager assembled without New (tests) has nowhere to queue, so say so
		// rather than silently discarding the event.
		applog.Error("download", "inbox_retry_unavailable", "event", describe)
		return
	}
	select {
	case m.inboxRetry <- inboxRetry{describe: describe, run: run}:
	default:
		// Bounded on purpose: during a long outage the alternative is unbounded
		// memory growth. Dropping loudly keeps the loss visible. Report the
		// channel's real capacity rather than the default constant, which a
		// differently sized queue would make a lie.
		applog.Error("download", "inbox_retry_queue_full", "event", describe, "capacity", cap(m.inboxRetry))
	}
}

// inboxRetryWorker drains failed inbox writes. It waits for the database rather
// than giving up immediately, because these events cannot be recovered from
// anywhere else.
func (m *Manager) inboxRetryWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-m.inboxRetry:
			m.retryInboxWrite(ctx, item)
		}
	}
}

func (m *Manager) retryInboxWrite(ctx context.Context, item inboxRetry) {
	for attempt := 1; attempt <= inboxRetryAttempts; attempt++ {
		if m.DatabaseAvailable() {
			if err := item.run(); err == nil {
				applog.Info("download", "inbox_write_recovered", "event", item.describe, "attempt", attempt)
				return
			}
		}
		if attempt == inboxRetryAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(attempt) * inboxRetryBackoff):
		}
	}
	applog.Error("download", "inbox_write_dropped", "event", item.describe, "attempts", inboxRetryAttempts)
}

// signalReconcile asks the claim reconciler to make a pass. Like signal it is
// best effort: the reconciler's idle timeout is what guarantees progress, so a
// dropped token costs latency rather than correctness.
func (m *Manager) signalReconcile() {
	select {
	case m.reconcileWake <- struct{}{}:
	default:
	}
}

// reconcileWorker is the single owner of waiting-item promotion. That sweep used
// to run inside every download worker, duplicating identical maintenance work
// sixteen times over and letting it crowd out the transfers those workers exist
// to perform. It drains in bounded passes and sleeps only once a pass stops
// making progress, so an idle queue costs one indexed query per interval.
func (m *Manager) reconcileWorker(ctx context.Context) {
	cursor := int64(0)
	// settleCursor is the settle pass's own rotation position, kept here because
	// this loop is its only caller.
	settleCursor := ""
	for {
		if m.DatabaseAvailable() {
			for pass := 0; pass < reconcileMaxDrainPasses; pass++ {
				promoted, next, err := m.reconcileMessageClaims(cursor)
				cursor = next
				if err != nil || promoted == 0 {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(reconcileDrainPause):
				}
			}
			// Claim promotion only covers jobs that still have waiting items.
			// A job stranded with nothing queued and nothing waiting has no
			// other pass that will ever look at it.
			next, err := m.settleQueuedJobsWithoutPendingWork(settleCursor)
			settleCursor = next
			if err != nil {
				applog.Error("download", "queued_job_settle_failed", "error", err.Error())
			}
			// A task stranded in 'running' is the mirror image of that one: it
			// has pending files, but no pass selects a task in that state, so
			// without this it waits for a restart that may never come.
			m.recoverStrandedRunningJobsIfDue()
		}
		select {
		case <-ctx.Done():
			return
		case <-m.reconcileWake:
		case <-time.After(reconcileIdleInterval):
		}
	}
}

// reconcileMessageClaims makes one bounded pass over waiting message items. It
// returns how many it promoted and the cursor the next pass resumes from, so the
// caller can drain the set without re-examining rows it already found to be
// legitimately blocked.
//
// Ordering by item id instead of the owner's updated_at is what stops a blocked
// head from starving the rest of the queue. An item whose claim is genuinely
// held cannot be promoted, so a stable ordering would keep it inside the LIMIT
// window forever while everything behind it went unexamined.
func (m *Manager) reconcileMessageClaims(cursor int64) (int, int64, error) {
	rows, err := m.db.Query(`SELECT i.id, i.job_id, i.dialog_type, i.dialog_key, i.dialog_id, i.message_id, i.grouped_id, i.message_text, i.origin_dialog_name, i.origin_message_id, i.is_comment, i.source_peer_type, i.source_peer_id, i.source_peer_hash, i.original_name, i.size, j.dialog_name, j.status FROM download_items i JOIN download_jobs j ON j.id = i.job_id WHERE i.status = 'waiting' AND j.status IN ('queued', 'partial', 'failed') AND i.id > ? ORDER BY i.id LIMIT ?`, cursor, reconcileBatchSize)
	if err != nil {
		return 0, cursor, err
	}
	defer rows.Close()
	promoted, scanned := 0, 0
	next := cursor
	for rows.Next() {
		var itemID int64
		var jobID, dialogName, jobStatus string
		var isComment int
		var item Item
		if err := rows.Scan(&itemID, &jobID, &item.DialogType, &item.DialogKey, &item.DialogID, &item.MessageID, &item.GroupedID, &item.MessageText, &item.OriginDialogName, &item.OriginMessageID, &isComment, &item.SourcePeerType, &item.SourcePeerID, &item.SourcePeerHash, &item.OriginalName, &item.Size, &dialogName, &jobStatus); err != nil {
			continue
		}
		scanned++
		next = itemID
		item.IsComment = isComment != 0
		candidate := source{Item: item, DialogName: dialogName, Direct: directPeer{kind: item.SourcePeerType, id: item.SourcePeerID, hash: item.SourcePeerHash}}
		// A task that is not queued cannot transfer anything, so it must never
		// acquire a claim: owning one would block the chat task that can still
		// finish the file, on behalf of a download this task will never start.
		// It can still adopt a file that someone else finished.
		if jobStatus != "queued" {
			path, ok := m.completedMediaPath(candidate)
			if ok && m.adoptCompletedMessageItem(jobID, candidate, path) == nil {
				promoted++
				if m.allItemsCompleted(jobID) {
					_ = m.setJob(jobID, "completed", "")
				}
			}
			continue
		}
		claim, path, err := m.claimMessageMedia(jobID, candidate)
		if err != nil || claim == "waiting" {
			continue
		}
		if claim == "completed" {
			if m.adoptCompletedMessageItem(jobID, candidate, path) == nil {
				promoted++
				if m.allItemsCompleted(jobID) {
					_ = m.setJob(jobID, "completed", "")
				}
			}
			continue
		}
		result, err := m.db.Exec(`UPDATE download_items SET status = 'queued', error = '' WHERE job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'waiting'`, jobID, item.DialogKey, item.MessageID)
		if err != nil {
			continue
		}
		if changed, _ := result.RowsAffected(); changed == 1 {
			promoted++
			m.touch()
			// One wake-up per promoted item, capped by the channel, so several
			// newly runnable tasks can start in parallel instead of waiting out
			// the workers' idle poll.
			m.signal()
		}
	}
	if err := rows.Err(); err != nil {
		return promoted, next, err
	}
	// A short pass means the rotation reached the end of the waiting set.
	if scanned < reconcileBatchSize {
		next = 0
	}
	return promoted, next, nil
}

func (m *Manager) releaseFailedMessageClaims(jobID string) {
	_, _ = m.db.Exec(`DELETE FROM downloaded_media m USING download_items i WHERE m.dialog_key = i.dialog_key AND m.message_id = i.message_id AND m.status = 'claimed' AND m.owner_kind = 'message' AND m.owner_id = ? AND i.job_id = ? AND i.status IN ('failed', 'cancelled', 'deleted')`, jobID, jobID)
	m.signalChat()
	m.signalReconcile()
}

// releaseMessageClaims drops every media claim this message task still owns,
// whatever state its items are in. It mirrors releaseChatClaims, and the
// difference from releaseFailedMessageClaims is the point: that one only covers
// failed, cancelled and deleted items, so it cannot serve a pause — a paused item
// matches none of them and the claim would survive.
//
// A task that is not transferring must not keep owning media in downloaded_media,
// because a waiter tests the claim rather than the item's status. Otherwise every
// other task that wants the same media waits until this one is resumed, and a
// claim left by a task the user paused is never released by anything.
//
// Resuming is unaffected: the item rows, the temporary file and the upstream
// resume state all stay on disk, and claimMedia re-acquires the claim (or the
// resuming task adopts the file when another task finished it first).
func (m *Manager) releaseMessageClaims(jobID string) {
	_, _ = m.db.Exec(`DELETE FROM downloaded_media WHERE status = 'claimed' AND owner_kind = 'message' AND owner_id = ?`, jobID)
	m.signalChat()
	m.signalReconcile()
}

// reconcilePublishedItems completes the narrow crash window between moving a
// file and persisting its terminal status. A path is trusted only when it is
// still a regular file; otherwise a normal resumable retry remains possible.
//
// It must heal the global ownership record, not just the item row. A waiting
// task tests the claim rather than the item status, so completing the item while
// leaving downloaded_media at 'claimed' would publish a file that no other task
// can ever adopt — and releaseFailedMessageClaims cannot undo it, because the
// item is no longer failed or cancelled. The chat side's counterpart already
// does both writes in one transaction for this reason.
func (m *Manager) reconcilePublishedItems() error {
	// One bounded page per pass, rotated by item id. A row that cannot be
	// published stays in the set, so without a cursor a single bad row at the
	// head would be re-read by every pass forever, and a large backlog - what a
	// database outage leaves behind - would be read in full every minute.
	cursor := m.reconcilePublishedCursor.Load()
	rows, err := m.db.Query(`SELECT id, job_id, dialog_key, message_id, final_path FROM download_items WHERE status = 'downloaded' AND final_path != '' AND id > ? ORDER BY id LIMIT ?`, cursor, reconcileBatchSize)
	if err != nil {
		return err
	}
	defer rows.Close()
	type candidate struct {
		itemID    int64
		jobID     string
		dialogKey string
		messageID int
		path      string
	}
	items := make([]candidate, 0)
	scanned := 0
	next := cursor
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.itemID, &item.jobID, &item.dialogKey, &item.messageID, &item.path); err != nil {
			return err
		}
		scanned++
		next = item.itemID
		if info, statErr := os.Stat(item.path); statErr == nil && info.Mode().IsRegular() {
			items = append(items, item)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// A short page means this rotation reached the end of the set, so the next
	// pass starts over. Recording it before any publish is deliberate: a pass
	// that returns early on a publish error has still examined these rows, and
	// leaving the cursor behind them would re-read them immediately.
	if scanned < reconcileBatchSize {
		next = 0
	}
	m.reconcilePublishedCursor.Store(next)
	for _, item := range items {
		// Publish, pause and cancel share one linearization point per task, so a
		// completion callback that started just before a cancellation cannot
		// recreate a published row after Cancel released the task's claims.
		lock := m.jobLock(item.jobID)
		lock.Lock()
		err := m.publishCompletedItem(item.jobID, item.dialogKey, item.messageID, item.path)
		lock.Unlock()
		if err != nil {
			return err
		}
	}
	if len(items) > 0 {
		// Scoped to the tasks whose files were just published. Without the id
		// filter this statement swept every task in a non-terminal state, and
		// 'failed' and 'partial' are permanent: it re-examined a task's entire
		// history on every pass, on a table that holds one row per task ever
		// created. The work here is always about the tasks the loop above
		// touched, and the settle pass covers a task stranded with nothing
		// queued and nothing waiting.
		ids := make([]string, 0, len(items))
		seen := make(map[string]struct{}, len(items))
		for _, item := range items {
			if _, exists := seen[item.jobID]; exists {
				continue
			}
			seen[item.jobID] = struct{}{}
			ids = append(ids, item.jobID)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
		args := make([]any, 0, len(ids)+1)
		args = append(args, time.Now().UTC().Format(time.RFC3339Nano))
		for _, id := range ids {
			args = append(args, id)
		}
		_, err = m.db.Exec(`UPDATE download_jobs SET status = 'completed', error = '', updated_at = ? WHERE id IN (`+placeholders+`) AND status IN ('queued', 'running', 'failed', 'partial') AND NOT EXISTS (SELECT 1 FROM download_items WHERE download_items.job_id = download_jobs.id AND download_items.status != 'completed')`, args...)
	}
	return err
}

// publishCompletedItem records an already-moved file as completed together with
// its global ownership row, in one transaction.
func (m *Manager) publishCompletedItem(jobID, dialogKey string, messageID int, path string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE download_items SET status = 'completed', finished_at = CASE WHEN finished_at = '' THEN ? ELSE finished_at END WHERE job_id = ? AND dialog_key = ? AND message_id = ? AND status = 'downloaded'`, now, jobID, dialogKey, messageID); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) VALUES (?, ?, ?, 'completed', 'message', ?, ?) ON CONFLICT(dialog_key, message_id) DO UPDATE SET final_path = EXCLUDED.final_path, status = EXCLUDED.status, owner_kind = EXCLUDED.owner_kind, owner_id = EXCLUDED.owner_id, updated_at = EXCLUDED.updated_at`, dialogKey, messageID, path, jobID, now); err != nil {
		return err
	}
	return tx.Commit()
}

// publishItem moves a file only after upstream tdl has closed and finalized
// its temporary file. It runs outside the upstream download worker.
func (m *Manager) publishItem(jobID, path string, item source, config settings.Values) error {
	lock := m.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	if !m.runningOrUnknown(jobID) {
		return nil
	}
	finalPath, err := finalDestination(m.downloadDir, config.Download.FinalFilenameTemplate, item)
	if err != nil {
		if saveErr := m.setItem(item, "failed", "", err.Error()); saveErr != nil {
			return saveErr
		}
		return err
	}
	if occupied, regularFile := destinationOccupied(finalPath); occupied {
		message := "目标文件已存在，未覆盖"
		if !regularFile {
			message = blockedDestinationMessage(finalPath)
		}
		if err := m.setItem(item, "failed", "", message); err != nil {
			return err
		}
		return errors.New(message)
	}
	// Persist the selected final path before the irreversible move. If the
	// database disconnects after a successful move, reconciliation can verify
	// the path instead of downloading this media again.
	if err := m.setItem(item, "downloaded", finalPath, ""); err != nil {
		return fmt.Errorf("保存待发布状态: %w", err)
	}
	if err := publishNoReplace(path, finalPath); err != nil {
		if errors.Is(err, unix.EEXIST) {
			// Something appeared between the check above and the move. Report it
			// with the same distinction, because the item is now blocked exactly
			// as it would have been by the earlier check.
			message := "目标文件已存在，未覆盖"
			if occupied, regularFile := destinationOccupied(finalPath); occupied && !regularFile {
				message = blockedDestinationMessage(finalPath)
			}
			if saveErr := m.setItem(item, "failed", "", message); saveErr != nil {
				return saveErr
			}
			return errors.New(message)
		}
		if saveErr := m.setItem(item, "failed", "", err.Error()); saveErr != nil {
			return saveErr
		}
		return err
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
			m.recordTelegramRPCError(accountID, err)
			return err
		}
		messages := []*tg.Message{message}
		groupedID := int64(0)
		if group, ok := message.GetGroupedID(); ok {
			groupedID = group
			// ?single asks for the one message the link points at rather than the
			// album it belongs to. Telegram's own clients put the marker on a link
			// copied from a single member, and the upstream link parser reads
			// every query parameter except ?comment as noise, so without this the
			// download did not match the link that was copied into the box.
			//
			// Only the expansion is skipped: the real grouped id is still recorded
			// on the item, because a naming template may legitimately use it, and
			// it is the album's identity rather than a property of this one file.
			if !linkAsksForSingleMessage(sourceURL) {
				messages, err = tutil.GetGroupedMessages(ctx, client.API(), peer.InputPeer(), message)
				if err != nil {
					m.recordTelegramRPCError(accountID, err)
					return err
				}
			}
		}
		messageText := groupDisplayText(messages)
		dialogType, dialogKey, dialogID := dialogIdentityForPeer(peer, accountID)
		for _, msg := range messages {
			media, ok := tmedia.GetMedia(msg)
			if !ok {
				continue
			}
			result = append(result, source{Item: Item{DialogType: dialogType, DialogKey: dialogKey, DialogID: dialogID, MessageID: msg.ID, GroupedID: groupedID, MessageText: messageText, OriginalName: media.Name, Size: media.Size}, DialogName: peer.VisibleName(), MediaType: messageMediaType(msg)})
		}
		originID := firstMessageID(messages, message.ID)
		result = setOrigin(result, peer.VisibleName(), originID, false)
		result = setSourcePeer(result, peer.InputPeer())
		if m.settings.Get().Download.IncludeReplies {
			related, relatedErr := relatedSources(m, ctx, client.API(), accountID, peer.InputPeer(), peer.VisibleName(), messages, message.ID, originID, false, "")
			if relatedErr != nil {
				if telegramWaitDuration(relatedErr) > 0 {
					return relatedErr
				}
				logRelatedWarning(accountID, message.ID, relatedErr)
			} else {
				result = append(result, related...)
			}
		}
		return nil
	})
	// A successful read that produced nothing is an answer, not an empty result
	// the caller has to interpret: the message held no downloadable file. Left as
	// an empty slice it reached the caller's filter check, which reported it with
	// the file-filter wording and sent the reader to settings that were never
	// involved.
	if len(result) == 0 && err == nil {
		err = nothingToDo(ErrNoMedia)
	}
	return result, err
}

// linkAsksForSingleMessage reports whether a message link carries Telegram's
// ?single marker. The marker has no value in links copied from clients, but a
// valued form is accepted too, so only the key is tested. A URL that will not
// parse is not a request for one file: link parsing reports that separately.
func linkAsksForSingleMessage(rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	_, present := parsed.Query()["single"]
	return present
}

func (m *Manager) resolvePeer(ctx context.Context, accountID string, inputPeer tg.InputPeerClass, dialogID int64, messageID int, dialogName string) ([]source, error) {
	return m.resolvePeerWithReplies(ctx, accountID, inputPeer, dialogID, messageID, dialogName, m.settings.Get().Download.IncludeReplies, false, "")
}

// resolvePeerWithReplies is the shared listener/reaction resolver. Chat
// download tasks pass their persisted configuration snapshot so later global
// setting changes cannot alter an already-created task.
//
// learnRoot is set only by the listener path, which resolves a new post even
// with an empty comment section in order to record its discussion root.
//
// chatJobID is the session task a resolved discussion group should be recorded
// on, or empty for a plain message task that has no session task to record it
// on. See relatedSources.
func (m *Manager) resolvePeerWithReplies(ctx context.Context, accountID string, inputPeer tg.InputPeerClass, dialogID int64, messageID int, dialogName string, includeReplies, learnRoot bool, chatJobID string) ([]source, error) {
	var result []source
	err := m.accounts.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		message, err := tutil.GetSingleMessage(ctx, client.API(), inputPeer, messageID)
		if err != nil {
			m.recordTelegramRPCError(accountID, err)
			return err
		}
		messages := []*tg.Message{message}
		groupedID := int64(0)
		if group, ok := message.GetGroupedID(); ok {
			groupedID = group
			messages, err = tutil.GetGroupedMessages(ctx, client.API(), inputPeer, message)
			if err != nil {
				m.recordTelegramRPCError(accountID, err)
				return err
			}
		}
		messageText := groupDisplayText(messages)
		dialogType, dialogKey, resolvedDialogID := dialogIdentity(inputPeer, accountID)
		// Direct peers originate from reactions and listener updates. Resolve them
		// once here so a supergroup does not inherit the generic channel label.
		manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(client.API())
		var resolved peers.Peer
		if peer, resolveErr := manager.FromInputPeer(ctx, inputPeer); resolveErr == nil {
			resolved = peer
			dialogType, dialogKey, resolvedDialogID = dialogIdentityForPeer(peer, accountID)
			// An event queued before its name could be resolved carries an empty
			// name. Fill it from the authoritative peer here so the name is
			// correct the first time it is written; never invent a substitute.
			if strings.TrimSpace(dialogName) == "" {
				dialogName = peer.VisibleName()
			}
		}
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
		originID := firstMessageID(messages, message.ID)
		result = setOrigin(result, dialogName, originID, false)
		result = setSourcePeer(result, inputPeer)
		if includeReplies {
			related, relatedErr := relatedSources(m, ctx, client.API(), accountID, inputPeer, dialogName, messages, message.ID, originID, eagerDiscussionRoot(learnRoot, broadcastOf(resolved)), chatJobID)
			if relatedErr != nil {
				if telegramWaitDuration(relatedErr) > 0 {
					return relatedErr
				}
				logRelatedWarning(accountID, message.ID, relatedErr)
			} else {
				result = append(result, related...)
			}
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
	rows, err := m.db.Query(`SELECT id, dialog_type, dialog_key, dialog_id, message_id, grouped_id, message_text, origin_dialog_name, origin_message_id, is_comment, source_peer_type, source_peer_id, source_peer_hash, original_name, size, final_path, started_at, finished_at, elapsed_ms, attempts, status, error FROM download_items WHERE job_id = ? ORDER BY id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Item, 0)
	for rows.Next() {
		var item Item
		var isComment int
		if err := rows.Scan(&item.ID, &item.DialogType, &item.DialogKey, &item.DialogID, &item.MessageID, &item.GroupedID, &item.MessageText, &item.OriginDialogName, &item.OriginMessageID, &isComment, &item.SourcePeerType, &item.SourcePeerID, &item.SourcePeerHash, &item.OriginalName, &item.Size, &item.FinalPath, &item.StartedAt, &item.FinishedAt, &item.ElapsedMS, &item.Attempts, &item.Status, &item.Error); err != nil {
			return nil, err
		}
		item.IsComment = isComment != 0
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
		result = append(result, source{Item: item, DialogName: dialogName, Direct: directPeer{kind: item.SourcePeerType, id: item.SourcePeerID, hash: item.SourcePeerHash}})
	}
	return result, nil
}
func (m *Manager) signal() {
	// Keep one wake-up per worker so several newly queued jobs can begin in
	// parallel instead of waiting for the workers' idle polling timeout. A
	// single token would wake exactly one of them, because a receive from a
	// buffered channel hands the value to one receiver; the rest would then
	// have to wait out the whole idle interval. Fill the buffer until it is
	// full, which also bounds this to workerCount sends.
	for sent := 0; sent < workerCount; sent++ {
		select {
		case m.wake <- struct{}{}:
		default:
			return
		}
	}
}

// signalChat wakes both the indexing loop and every transfer worker. Nearly
// every session state transition concerns both classes, and an extra wake on an
// idle worker costs one cheap query while a missed one costs a full idle
// interval.
//
// Callers that only create transfer work should use signalChatDownload instead.
// That matters most inside chatWorker itself: signalling chatWake from its own
// iteration would consume the token it just sent and spin the loop without its
// fallback delay.
func (m *Manager) signalChat() {
	m.signalChatIndex()
	m.signalChatDownload()
}

// signalChatIndex wakes only the indexing/reconciliation loop.
func (m *Manager) signalChatIndex() {
	select {
	case m.chatWake <- struct{}{}:
	default:
	}
}

// signalChatDownload wakes every transfer worker, so a page of newly indexed
// media can fill the whole transfer concurrency instead of one slot.
func (m *Manager) signalChatDownload() {
	for _, wake := range m.chatDownloadWake {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
func (m *Manager) status(id string) string {
	status, _ := m.jobStatus(id)
	return status
}

// jobStatus reports a task's durable status and whether the read succeeded.
// A worker deciding its own fate must never confuse "the database could not
// answer" with "the task is no longer running".
func (m *Manager) jobStatus(id string) (string, bool) {
	var status string
	if err := m.db.QueryRow(`SELECT status FROM download_jobs WHERE id = ?`, id).Scan(&status); err != nil {
		return "", false
	}
	return status, true
}

// runningOrUnknown reports whether a worker should keep working on its task.
//
// A status read that fails is treated as "still running". The alternative was
// the cause of a task that could never finish: one transient database error -
// a pool timeout, a momentary blip too short for the health monitor to notice -
// made the worker read an empty status, conclude the task had been taken away
// from it, and return. The task stayed in 下载中 with its files queued, and
// nothing would ever look at it again, because every other pass selects either
// queued tasks or waiting items. Continuing is safe: if the outage is real the
// next write fails loudly and the task is failed with a reason, which is a
// state the user can act on.
func (m *Manager) runningOrUnknown(id string) bool {
	status, ok := m.jobStatus(id)
	return !ok || status == "running"
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
	// A paused task transfers nothing, so it must not keep owning media: a
	// waiting task tests the claim, and a claim held by a task the user paused is
	// released by nothing else. PauseChat does the same for the same reason.
	m.releaseMessageClaims(id)
	m.mu.Lock()
	cancel := m.cancels[id]
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (m *Manager) pauseItemsSQL() string {
	return `UPDATE download_items SET status = 'paused', error = '', elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END, started_at = '', finished_at = '' WHERE job_id = ? AND status IN ('queued', 'waiting', 'running', 'downloaded')`
}
func (m *Manager) Resume(id string) error {
	lock := m.jobLock(id)
	lock.Lock()
	defer lock.Unlock()
	if m.status(id) != "paused" {
		return errors.New("当前任务不能恢复")
	}
	if err := m.transitionItems(id, "paused", "queued", "", `UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0, attempts = 0 WHERE job_id = ? AND status != 'completed'`, id); err != nil {
		return fmt.Errorf("恢复任务: %w", err)
	}
	m.resetAttempts(id)
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
	// attempts bounds automatic retries. It is not reset by the automatic paths,
	// which is the point, but a click is a new budget: leaving the counter at the
	// cap made the next flood or stall fail the task on its first recurrence with
	// "已停止自动重试", so the retry the user asked for changed nothing.
	if err := m.transitionItems(id, status, "queued", "", `UPDATE download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0, attempts = 0 WHERE job_id = ? AND status != 'completed'`, id); err != nil {
		return fmt.Errorf("重试任务: %w", err)
	}
	m.resetAttempts(id)
	m.signal()
	return nil
}

// resetAttempts gives a task the full automatic-retry budget again, for the
// paths a person triggered. Those are the only ones allowed to do it: an
// automatic path resetting the counter would remove the bound it exists for.
func (m *Manager) resetAttempts(id string) {
	if _, err := m.db.Exec(`UPDATE download_jobs SET attempts = 0 WHERE id = ?`, id); err != nil {
		applog.Error("download", "attempt_budget_reset_failed", "job_id", id, "error", err.Error())
	}
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
	m.releaseFailedMessageClaims(id)
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
	m.signalChat()
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
	var accountID, sourceURL, parentChatID string
	if err := tx.QueryRow(`SELECT account_id, source_url, parent_chat_id FROM download_jobs WHERE id = ?`, id).Scan(&accountID, &sourceURL, &parentChatID); err != nil {
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
	result, err := tx.Exec(`UPDATE download_jobs SET status = 'deleted', error = '任务已删除', updated_at = ? WHERE id = ? AND status IN ('completed', 'failed', 'partial', 'cancelled')`, time.Now().UTC().Format(time.RFC3339Nano), id)
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
	if err := tx.Commit(); err != nil {
		return err
	}
	m.releaseFailedMessageClaims(id)
	m.jobBecameHidden(parentChatID)
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
	return lockFor(&m.jobLocks, id)
}

func (m *Manager) chatLock(id string) *sync.Mutex {
	return lockFor(&m.chatLocks, id)
}

func lockFor(locks *[64]sync.Mutex, id string) *sync.Mutex {
	var hash uint32
	for index := 0; index < len(id); index++ {
		hash = hash*33 + uint32(id[index])
	}
	return &locks[hash%uint32(len(locks))]
}

func (m *Manager) setJob(id, status, message string) error {
	if _, err := m.db.Exec(`UPDATE download_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?`, status, message, time.Now().UTC().Format(time.RFC3339), id); err != nil {
		applog.Error("download", "task_state_save_failed", "job_id", id, "status", status, "error", err.Error())
		return err
	}
	m.touch()
	m.emit(id, "", "job_status_changed", status)
	m.signalChat()
	return nil
}
func (m *Manager) setItem(item source, status, path, message string) error {
	finishedAt := ""
	if status == "completed" || status == "failed" || status == "cancelled" {
		finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	// Completion writes two rows that must agree: the item's terminal state and
	// the global ownership record every other task consults before deciding
	// whether this media still has to be downloaded. They travel in one
	// statement, so a failure leaves neither written rather than an item that
	// claims to be finished while nothing owns the file.
	//
	// Writing them separately was wrong in both directions. The ownership write
	// was issued after the item update and its error discarded, so a transient
	// failure produced exactly the state above and the task still reported
	// success; and a second round trip on the completion path of every file is
	// the one write amplification this path can least afford.
	//
	// A statement matching no row is the ordinary case of a state that already
	// changed, not an error. RETURNING owner_id carries the owning task id back
	// so the caller does not need a follow-up query to learn what it belongs to;
	// on the conflict branch it is EXCLUDED.owner_id, which is the same task.
	if status == "completed" && path != "" {
		var jobID string
		err := m.db.QueryRow(`WITH updated AS (
 UPDATE download_items SET status = ?, final_path = ?, error = ?, finished_at = CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE dialog_key = ? AND message_id = ? RETURNING job_id
)
INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at)
 SELECT ?, ?, ?, 'completed', 'message', job_id, ? FROM updated
 ON CONFLICT(dialog_key, message_id) DO UPDATE SET final_path = EXCLUDED.final_path, status = EXCLUDED.status, owner_kind = EXCLUDED.owner_kind, owner_id = EXCLUDED.owner_id, updated_at = EXCLUDED.updated_at
RETURNING owner_id`, status, path, message, finishedAt, finishedAt, item.DialogKey, item.MessageID, item.DialogKey, item.MessageID, path, time.Now().UTC().Format(time.RFC3339Nano)).Scan(&jobID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			applog.Error("download", "item_state_save_failed", "dialog_key", item.DialogKey, "message_id", item.MessageID, "status", status, "error", err.Error())
			return err
		}
		m.touch()
		if jobID != "" {
			m.emit(jobID, "", "item_status_changed", status)
		}
		return nil
	}
	// One statement carries the change, names the owning task and the status
	// that goes with it. The ownership row below needs that task id, which used
	// to cost a second query on the completion path alone.
	var jobID string
	err := m.db.QueryRow(`UPDATE download_items SET status = ?, final_path = ?, error = ?, finished_at = CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE dialog_key = ? AND message_id = ? RETURNING job_id`, status, path, message, finishedAt, finishedAt, item.DialogKey, item.MessageID).Scan(&jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		applog.Error("download", "item_state_save_failed", "dialog_key", item.DialogKey, "message_id", item.MessageID, "status", status, "error", err.Error())
		return err
	}
	m.touch()
	if jobID != "" {
		m.emit(jobID, "", "item_status_changed", status)
	}
	return nil
}

// beginItemAttempts records one transfer attempt for every pending file, in one
// statement per dialog.
//
// The status list is a guard, not a filter for its own sake. A pause or cancel
// that lands while the pending set is being assembled must not be undone by the
// attempt bookkeeping: without it this statement flipped a paused file back to
// queued, which left the task's own control action partly reverted. The states
// listed are exactly the ones a task about to transfer can legitimately find its
// files in, so a file that reached a terminal or user-driven state in the
// meantime keeps it.
//
// Grouping by dialog is what keeps the statement bounded: message ids are only
// unique inside one dialog, so the id list has to carry the dialog it belongs to.
func (m *Manager) beginItemAttempts(items []source) error {
	byDialog := make(map[string][]int, 2)
	for _, item := range items {
		byDialog[item.DialogKey] = append(byDialog[item.DialogKey], item.MessageID)
	}
	for dialogKey, messageIDs := range byDialog {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(messageIDs)), ",")
		args := make([]any, 0, len(messageIDs)+1)
		args = append(args, dialogKey)
		for _, messageID := range messageIDs {
			args = append(args, messageID)
		}
		// A task may be running while a particular file is still waiting for an
		// upstream worker slot. It becomes "running" only on its first byte-level
		// progress callback.
		if _, err := m.db.Exec(`UPDATE download_items SET attempts = attempts + 1, status = 'queued', error = '', started_at = '', finished_at = '' WHERE dialog_key = ? AND message_id IN (`+placeholders+`) AND status IN ('queued', 'failed', 'downloaded', 'running')`, args...); err != nil {
			applog.Error("download", "item_attempt_save_failed", "dialog_key", dialogKey, "error", err.Error())
			return err
		}
	}
	m.touch()
	return nil
}
func (m *Manager) pauseItem(item source) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := m.pauseItemsSQL()
	query = strings.Replace(query, "WHERE job_id = ? AND status IN ('queued', 'waiting', 'running', 'downloaded')", "WHERE dialog_key = ? AND message_id = ? AND status IN ('queued', 'waiting', 'running')", 1)
	m.execItemState("item_pause_save_failed", item.Item, "paused", query, now, item.DialogKey, item.MessageID)
}
func (m *Manager) markItemStarted(item source) error {
	return m.execItemState("item_start_save_failed", item.Item, "running", `UPDATE download_items SET status = 'running', started_at = ? WHERE dialog_key = ? AND message_id = ? AND status = 'queued' AND started_at = ''`, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID)
}
func (m *Manager) markItemFinished(item source) error {
	return m.execItemState("item_finish_save_failed", item.Item, "downloaded", `UPDATE download_items SET status = 'downloaded', finished_at = ? WHERE dialog_key = ? AND message_id = ? AND status = 'running' AND started_at != '' AND finished_at = ''`, time.Now().UTC().Format(time.RFC3339Nano), item.DialogKey, item.MessageID)
}

// execItemState applies one item state change and reports it in a single round
// trip. RETURNING hands back the owning task, so the change does not need a
// follow-up query to discover what it belongs to, and the caller passes the
// status it just wrote rather than reading it back. Without both, every state
// change cost an extra statement, and a task moves each file through several.
//
// A statement that matches no row is not an error: it is the ordinary case of
// a state that already changed, which the old Exec reported the same way.
func (m *Manager) execItemState(event string, item Item, status, query string, args ...any) error {
	var jobID string
	err := m.db.QueryRow(query+" RETURNING job_id", args...).Scan(&jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		applog.Error("download", event, "error", err.Error())
		return err
	}
	m.touch()
	if jobID != "" {
		m.emit(jobID, "", "item_status_changed", status)
	}
	return nil
}

// fail marks every file of a task that still needs attention as failed.
//
// The callers pass the task's items, but the statement is scoped by task id
// instead of by that list. Every row in the list belongs to this task - media
// identity is unique across the table, so each item has exactly one owning row -
// and the caller's list is itself derived from those rows, so the two select the
// same set. The difference is cost: the per-item shape was a status probe and an
// update for every file, on the failure path of a task that may hold a row per
// media comment.
func (m *Manager) fail(id string, err error) {
	if _, execErr := m.db.Exec(`UPDATE download_items SET status = 'failed', error = ?, finished_at = ? WHERE job_id = ? AND status NOT IN ('completed', 'downloaded', 'waiting')`, err.Error(), time.Now().UTC().Format(time.RFC3339Nano), id); execErr != nil {
		applog.Error("download", "task_fail_items_save_failed", "job_id", id, "error", execErr.Error())
	} else {
		m.touch()
	}
	var completed int
	_ = m.db.QueryRow(`SELECT COUNT(1) FROM download_items WHERE job_id = ? AND status = 'completed'`, id).Scan(&completed)
	if completed > 0 {
		_ = m.setJob(id, "partial", "部分文件未完成，可重试失败文件")
		m.releaseFailedMessageClaims(id)
		return
	}
	_ = m.setJob(id, "failed", err.Error())
	m.releaseFailedMessageClaims(id)
}

// filenameDialogComponent renders one dialog name for the filename template,
// falling back to a stable identity when Telegram supplied no visible name.
//
// An empty name is not cosmetic. The default template leads with the dialog
// name as a path segment, and normalizeRelativePath rejects an empty one, so a
// peer whose name could not be resolved - a deleted account, or a reaction or
// listener event whose peer lookup failed - made every file of the task fail at
// publish time. The failure arrived after the bytes had been downloaded, and its
// message pointed at the template rather than at the missing name.
//
// The fallback is derived from the identity the media itself is keyed by, so it
// is stable across retries and cannot make two dialogs share a directory.
func filenameDialogComponent(name string, dialogID int64, dialogKey string) string {
	if component := sanitizeComponent(name); component != "" {
		return component
	}
	if dialogID != 0 {
		return fmt.Sprintf("dialog_%d", dialogID)
	}
	if component := sanitizeComponent(dialogKey); component != "" {
		return component
	}
	return "dialog"
}

// filenameTemplates caches parsed naming templates by pattern text.
//
// The pattern changes only when the operator edits it, but rendering runs once
// per file - and again once per step of each shrinking search when a name is too
// long. Parsing a template that had not changed since the previous file was
// therefore among the more repeated pieces of work on the publish path. A parsed
// template is safe for concurrent use.
//
// The map is never evicted. Its size is the number of distinct templates this
// process has been asked to render - the current setting plus whatever an
// operator typed through the settings page - so it is bounded by human edits,
// not by download traffic.
var filenameTemplates sync.Map

func filenameTemplate(pattern string) (*template.Template, error) {
	if cached, ok := filenameTemplates.Load(pattern); ok {
		return cached.(*template.Template), nil
	}
	tpl, err := template.New("filename").Funcs(template.FuncMap{
		"formatDate": func(timestamp int64, layout string) string {
			return time.Unix(timestamp, 0).Format(layout)
		},
	}).Parse(pattern)
	if err != nil {
		return nil, fmt.Errorf("解析文件命名模板: %w", err)
	}
	filenameTemplates.Store(pattern, tpl)
	return tpl, nil
}

func renderName(pattern string, item source) (string, error) {
	data := struct {
		DialogID, GroupedID                       int64
		MessageID, OriginMessageID                int
		DialogName, OriginDialogName, MessageText string
		IsComment                                 bool
		FileName, FileExt                         string
		DownloadDate                              int64
	}{item.DialogID, item.GroupedID, item.MessageID, item.OriginMessageID, filenameDialogComponent(item.DialogName, item.DialogID, item.DialogKey), filenameDialogComponent(item.OriginDialogName, item.DialogID, item.DialogKey), sanitizeComponent(item.MessageText), item.IsComment, sanitizeComponent(item.OriginalName), sanitizeComponent(filepath.Ext(item.OriginalName)), time.Now().Unix()}
	tpl, err := filenameTemplate(pattern)
	if err != nil {
		return "", err
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
	fitted, err := fitMessageText(root, pattern, item)
	if err != nil {
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
			if candidate.OriginalName == "" {
				// Same reasoning as the caption search: an empty name is a
				// missing path segment, not a shorter one.
				high = budget - 1
				continue
			}
			if _, _, checkErr := measureFinalPath(root, pattern, candidate); checkErr == nil {
				shortened, found = candidate, true
				low = budget + 1
				continue
			} else if !errors.Is(checkErr, errFinalNameTooLong) {
				return "", checkErr
			}
			high = budget - 1
		}
		if !found {
			return "", err
		}
		if fitted, err = fitMessageText(root, pattern, shortened); err != nil {
			return "", err
		}
	}
	// The directory is prepared here, once, for the candidate that was actually
	// chosen. Both searches above evaluate many names and discard all but one,
	// and creating a directory tree for each discarded candidate meant the
	// operator was left with empty directories for names that were never used.
	return checkedFinalPath(root, pattern, fitted)
}

// fitMessageText keeps as much client-visible message text as possible. When
// it must shrink, the middle is replaced by a single ellipsis, retaining both
// the beginning and end of the original caption.
//
// It returns the file description to use rather than a path, so the caller
// finalizes the destination exactly once.
func fitMessageText(root, pattern string, item source) (source, error) {
	_, _, err := measureFinalPath(root, pattern, item)
	if err == nil || !errors.Is(err, errFinalNameTooLong) || item.MessageText == "" {
		return item, err
	}

	original := item.MessageText
	low, high := 0, len([]byte(original))
	var best source
	found := false
	for low <= high {
		budget := low + (high-low)/2
		candidate := item
		candidate.MessageText = ellipsizeMiddle(original, budget)
		if candidate.MessageText == "" {
			// A zero-length caption is not a shorter name, it is a missing path
			// segment: a template that uses the caption as a directory cannot
			// render this candidate at all. Reporting the resulting "unsafe path
			// segment" as fatal would abort the search on the smallest budget
			// instead of walking down to the largest one that still renders, so
			// this budget is simply recorded as not fitting.
			high = budget - 1
			continue
		}
		if _, _, checkErr := measureFinalPath(root, pattern, candidate); checkErr == nil {
			best, found = candidate, true
			low = budget + 1
			continue
		} else if !errors.Is(checkErr, errFinalNameTooLong) {
			return item, checkErr
		}
		high = budget - 1
	}
	if !found {
		return item, err
	}
	return best, nil
}

// measureFinalPath renders one candidate destination and validates its length.
// It deliberately touches nothing on disk: see finalDestination for why the
// side effect belongs to the chosen candidate alone.
func measureFinalPath(root, pattern string, item source) (relative, finalPath string, err error) {
	relative, err = renderName(pattern, item)
	if err != nil {
		return "", "", err
	}
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if len([]byte(component)) > linuxNameMaxBytes {
			return "", "", fmt.Errorf("%w：单个路径段最多 %d 字节", errFinalNameTooLong, linuxNameMaxBytes)
		}
	}
	finalPath = filepath.Join(root, relative)
	if len([]byte(finalPath)) >= linuxPathMaxBytes {
		return "", "", fmt.Errorf("%w：完整路径最多 %d 字节", errFinalNameTooLong, linuxPathMaxBytes-1)
	}
	return relative, finalPath, nil
}

func checkedFinalPath(root, pattern string, item source) (string, error) {
	relative, finalPath, err := measureFinalPath(root, pattern, item)
	if err != nil {
		return "", err
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

// temporaryDialogDirectory isolates upstream tdl's temporary files by the
// complete Telegram dialog identity. Message IDs are unique only inside one
// dialog, so a channel post and a linked-discussion reply with the same
// numeric ID must never share a continuation path.
func temporaryDialogDirectory(taskRoot, dialogKey string) (string, error) {
	name := sanitizeComponent(dialogKey)
	if name == "" || name == "." || name == ".." {
		return "", errors.New("临时目录的会话身份无效")
	}
	dir := filepath.Join(taskRoot, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
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
