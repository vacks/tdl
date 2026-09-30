package telegram

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth/qrlogin"
	messagePeer "github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/iyear/tdl/core/storage"
	upstreamKey "github.com/iyear/tdl/pkg/key"
	upstreamClient "github.com/iyear/tdl/pkg/tclient"
	"github.com/skip2/go-qrcode"
	"github.com/vacks/tdl/internal/applog"
)

var (
	ErrNotFound       = errors.New("Telegram 账户不存在")
	ErrNotAuthorized  = errors.New("该 Telegram 账户尚未登录完成")
	ErrPasswordNeeded = errors.New("当前账户未等待两步验证密码")
)

const (
	sessionCheckInitialDelay = 15 * time.Second
	sessionCheckInterval     = 30 * time.Minute
	// sessionCheckTimeout bounds one validity probe. It no longer has to cover a
	// connection handshake: the probe runs on the account's resident session, so
	// the budget is for a single request plus whatever reconnect the session is
	// already in the middle of.
	sessionCheckTimeout = 30 * time.Second
	// sessionStopTimeout bounds the wait for a connection to finish closing.
	// Callers that need the wait are HTTP requests removing an account, and a
	// timeout there only means the session files may be rewritten once more.
	sessionStopTimeout = 2 * time.Second
)

type Account struct {
	ID         string `json:"id"`
	TelegramID int64  `json:"telegramId,omitempty"`
	FirstName  string `json:"firstName,omitempty"`
	LastName   string `json:"lastName,omitempty"`
	Username   string `json:"username,omitempty"`
	State      string `json:"state"`
	Error      string `json:"error,omitempty"`
	QRCode     string `json:"qrCode,omitempty"`
	CreatedAt  string `json:"createdAt"`
	CheckedAt  string `json:"checkedAt,omitempty"`
}

// ReactionEvent represents a reaction made by the current logged-in account.
// InputPeer is retained so the download manager can resolve the exact message
// without depending on a public t.me link.
type ReactionEvent struct {
	AccountID  string
	DialogID   int64
	DialogName string
	MessageID  int
	SourceURL  string
	InputPeer  tg.InputPeerClass
	Emojis     []string
}

// NewMessageEvent is a normalized update for one newly received message.
// It intentionally carries an InputPeer, so consumers do not depend on
// usernames or public links (both may be unavailable for private groups).
type NewMessageEvent struct {
	AccountID        string
	DialogKey        string
	DialogID         int64
	DialogName       string
	MessageID        int
	InputPeer        tg.InputPeerClass
	ReplyToMessageID int
	ReplyToTopID     int
}

// NewMessageEventFor builds the event the dispatcher would have produced for a
// message it saw, so a caller that synthesises an event from a history page
// offers exactly what the live path offers.
//
// Build events through this function rather than with a struct literal, because
// the reply header is not decoration: it is how admission attributes a comment
// to the channel post it answers. An event without it is not a smaller event,
// it is one that admission discards as unattributable. The discussion walk
// rebuilt events by hand and left both fields zero, so every message it
// recovered was dropped in silence - a whole recovery path that could never
// deliver anything, with no error to show for it.
func NewMessageEventFor(accountID, dialogKey, dialogName string, dialogID int64, message *tg.Message, peer tg.InputPeerClass) NewMessageEvent {
	event := NewMessageEvent{
		AccountID:        accountID,
		DialogKey:        dialogKey,
		DialogID:         dialogID,
		DialogName:       dialogName,
		InputPeer:        peer,
		ReplyToMessageID: replyMessageID(message),
		ReplyToTopID:     replyTopID(message),
	}
	if message != nil {
		event.MessageID = message.ID
	}
	return event
}

type persisted struct {
	Accounts  []Account `json:"accounts"`
	CurrentID string    `json:"currentId"`
}

type loginJob struct {
	cancel   context.CancelFunc
	password chan string
}

// accountSession is the one long-lived Telegram connection belonging to an
// account. It carries both the updates every listener subscribes to and the
// plain requests operations make, so an authorization key is used by exactly
// one main session for as long as the account is authorized.
//
// It exists because every operation used to open a connection of its own.
// Resolving a link opened one and the download that followed opened another; a
// listener opened a third for each message it received. Each of those paid a
// full TCP and MTProto handshake, and none of that cost was related to how much
// was downloaded - it was a fixed price on every single operation.
//
// Keeping one connection also settled a question the lease could not. Validity
// checks used to take an exclusive account lease so they would not open a
// second main session beside an active one, but the update listener never took
// that lease - so the invariant was already untrue - while an account that was
// downloading or listening never released the lease, so the check those
// accounts most needed never ran at all. There is nothing left to arbitrate
// once there is only one connection for the check to use.
type accountSession struct {
	accountID string
	// proxy is what the connection was opened with. A session whose proxy no
	// longer matches the configured one is replaced rather than reused, because
	// the upstream client reads the proxy once, when it builds the connection.
	proxy  string
	store  *accountStore
	cancel context.CancelFunc
	// done closes when the run loop has finished with this session, whether it
	// ended because the account was removed, because the shutdown cancelled it,
	// or because the authorization turned out to be unusable.
	done chan struct{}

	mu      sync.Mutex
	client  *gotd.Client
	readyCh chan struct{}
	ready   bool
	lastErr error
	nextID  uint64

	reactions map[uint64]func(context.Context, ReactionEvent)
	messages  map[uint64]func(context.Context, NewMessageEvent)
	readies   map[uint64]func()
}

// waitReady blocks until the connection is authenticated and returns the client
// to use. A session that has ended reports why, so a caller learns the reason
// instead of waiting out its own deadline.
func (s *accountSession) waitReady(ctx context.Context) (*gotd.Client, error) {
	for {
		s.mu.Lock()
		client, readyCh := s.client, s.readyCh
		s.mu.Unlock()
		if client != nil {
			return client, nil
		}
		select {
		case <-readyCh:
			// The connection can drop between the signal and the read above. The
			// loop then waits for the next one rather than handing out a client
			// that is already gone.
		case <-s.done:
			return nil, s.terminalError()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// markReady publishes a freshly authenticated connection and returns the
// listeners that have to be told about it. Both happen under one acquisition of
// the lock: a listener subscribing while this runs must be told exactly once,
// either by the snapshot or by its own ready check, and never by both.
//
// A reconnect fires them again on purpose. Work that only runs while updates
// arrive - the listener gap walk - has to be re-armed after every interruption,
// because whatever was missed while the connection was down is exactly what it
// exists to recover.
func (s *accountSession) markReady(client *gotd.Client) []func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = client
	s.ready = true
	// A successful connection supersedes whatever the last failure was, so a
	// later terminal error cannot report a cause that has already been outlived.
	s.lastErr = nil
	select {
	case <-s.readyCh:
	default:
		close(s.readyCh)
	}
	callbacks := make([]func(), 0, len(s.readies))
	for _, callback := range s.readies {
		callbacks = append(callbacks, callback)
	}
	return callbacks
}

// markDisconnected records that the current connection is gone and arms a fresh
// readiness signal, so a caller waiting on it waits for the next connection
// rather than proceeding with the dead one.
//
// The channel being replaced is closed, not discarded. A caller inside waitReady
// is parked on whichever channel it read, and only a close wakes it; replacing
// the field leaves that caller waiting on a channel nothing will ever close,
// while the connection it is waiting for closes a different one. The connection
// that follows a failed attempt is the ordinary case, not an exotic one, so
// discarding the channel stranded an operation - and, because that operation
// holds the account's read lease, it stranded account removal behind it too.
func (s *accountSession) markDisconnected(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = nil
	s.ready = false
	select {
	case <-s.readyCh:
	default:
		close(s.readyCh)
	}
	s.readyCh = make(chan struct{})
	if err != nil {
		s.lastErr = err
	}
}

func (s *accountSession) terminalError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr != nil {
		return s.lastErr
	}
	return errors.New("Telegram 连接已关闭")
}

// sessionBuildError marks a failure to construct the connection at all, as
// opposed to a connection that was built and then dropped.
//
// The two need opposite treatment. A dropped connection is worth retrying
// behind the caller's back, which is what the session loop is for. A connection
// that cannot be constructed at all is not: building it reads the app
// configuration and the proxy from the account store, and when either is
// unusable no amount of waiting changes the answer. Retrying it hides the
// reason and leaves every caller waiting on a session that will never be ready,
// which - for the callers that pass a context with no deadline - means forever.
// The old code returned this error straight to the caller, and so does this.
type sessionBuildError struct{ err error }

func (e *sessionBuildError) Error() string { return e.err.Error() }
func (e *sessionBuildError) Unwrap() error { return e.err }

// stop cancels the session and waits, within a bound, for its run loop to
// finish.
func (s *accountSession) stop(timeout time.Duration) {
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(timeout):
		applog.Error("telegram", "session_stop_timeout", "account_id", s.accountID)
	}
}

// Manager owns account metadata and keeps each Telegram session in its own private file.
// The session format itself is handled by upstream tdl's tclient adapter.
type Manager struct {
	root string
	path string

	mu         sync.RWMutex
	accounts   []Account
	current    string
	jobs       map[string]*loginJob
	operations map[string]*sync.RWMutex
	stores     map[string]*sync.RWMutex
	proxyURL   func() string
	ctx        context.Context
	cancel     context.CancelFunc
	sessionsMu sync.Mutex
	sessions   map[string]*accountSession
	// rpcGate paces the metadata requests this manager issues. It is installed
	// by whoever owns the account-wide rate limit - the download service - and
	// called before every request in the list below. Held behind a lock rather
	// than set once at construction because the two managers are built in
	// sequence, not together.
	rpcGateMu sync.RWMutex
	rpcGate   RPCGate
}

// RPCGate is the account-wide metadata gate.
//
// Acquire paces one request. Report is handed whatever that request returned,
// and it is the only place a server-mandated wait can be observed: the
// flood-wait middleware the upstream client installs ahead of this one sleeps
// off a FLOOD_WAIT and retries inside the invoker, so the error never reaches
// the caller that would otherwise record it. Reporting from here is what makes
// the account-wide cooldown real for metadata requests rather than only for the
// byte transfers, whose invoker has no such middleware.
type RPCGate interface {
	Acquire(ctx context.Context, accountID string) error
	Report(accountID string, err error)
}

// SetRPCGate installs the account-wide request gate. A nil gate disables
// pacing, which is what tests and any future account type that does not share
// the download service's budget want.
func (m *Manager) SetRPCGate(gate RPCGate) {
	m.rpcGateMu.Lock()
	defer m.rpcGateMu.Unlock()
	m.rpcGate = gate
}

func (m *Manager) currentRPCGate() RPCGate {
	m.rpcGateMu.RLock()
	defer m.rpcGateMu.RUnlock()
	return m.rpcGate
}

// pacedRequest reports whether one request is the kind the account gate exists
// for: reading messages, resolving a peer, or asking about a dialog.
//
// The list is deliberately an allow-list rather than "everything except the
// byte transfer". The client's own startup issues requests - asking for the
// config, initialising the connection - from inside the same invoker, and a gate
// that stops to wait for a token can deadlock against the very request that
// would unblock it. Naming what is paced also keeps the expensive, invisible
// requests out: a file transfer and the update long-poll must never queue behind
// a metadata budget.
func pacedRequest(input bin.Encoder) bool {
	switch input.(type) {
	case *tg.MessagesGetHistoryRequest,
		*tg.MessagesGetMessagesRequest,
		*tg.MessagesSearchRequest,
		*tg.MessagesGetRepliesRequest,
		*tg.MessagesGetDiscussionMessageRequest,
		*tg.ChannelsGetFullChannelRequest,
		*tg.ChannelsGetChannelsRequest,
		*tg.ChannelsGetMessagesRequest,
		*tg.ContactsResolveUsernameRequest,
		*tg.UsersGetFullUserRequest,
		*tg.UsersGetUsersRequest:
		return true
	default:
		return false
	}
}

// rpcGateMiddleware paces this account's metadata requests through the shared
// gate.
//
// It used to be the caller's job: eighteen call sites inside the download
// service each remembered to ask for a token before making a request. That
// arrangement cannot be right by construction - a new call site, or a request
// made by a library on the way to something else, silently bypassed the gate -
// and it was already wrong in practice: building a peers.Manager resolves an
// access hash with channels.getChannels, which no caller ever paced, and a
// ?comment link resolution spends two requests under one token.
//
// Note the boundary: this wraps the invoker behind Client.API(). The downloader
// pool that performs the byte transfers builds its own invoker chain
// (dcpool.NewPool) and does not pass through here, so the per-message metadata
// reads it makes are still un-paced. Closing that needs the pool to receive a
// middleware of its own.
func (m *Manager) rpcGateMiddleware(accountID string) gotd.Middleware {
	return gotd.MiddlewareFunc(func(next tg.Invoker) gotd.InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			gate := m.currentRPCGate()
			if gate == nil || !pacedRequest(input) {
				return next.Invoke(ctx, input, output)
			}
			if err := gate.Acquire(ctx, accountID); err != nil {
				return err
			}
			err := next.Invoke(ctx, input, output)
			// Reported before it is returned, because the middleware outside this
			// one consumes a FLOOD_WAIT rather than passing it on.
			gate.Report(accountID, err)
			return err
		}
	})
}

func Open(dataDir string, proxyURL func() string) (*Manager, error) {
	root := filepath.Join(dataDir, "telegram")
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o700); err != nil {
		return nil, fmt.Errorf("create Telegram data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "state"), 0o700); err != nil {
		return nil, fmt.Errorf("create Telegram state directory: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{root: root, path: filepath.Join(root, "accounts.json"), jobs: make(map[string]*loginJob), operations: make(map[string]*sync.RWMutex), stores: make(map[string]*sync.RWMutex), proxyURL: proxyURL, ctx: ctx, cancel: cancel, sessions: make(map[string]*accountSession)}
	if data, err := os.ReadFile(m.path); err == nil {
		var saved persisted
		if err := json.Unmarshal(data, &saved); err != nil {
			return nil, fmt.Errorf("read Telegram accounts: %w", err)
		}
		for i := range saved.Accounts {
			if saved.Accounts[i].State != "authorized" && saved.Accounts[i].State != "expired" {
				saved.Accounts[i].State = "stopped"
				saved.Accounts[i].QRCode = ""
			}
		}
		m.accounts, m.current = saved.Accounts, saved.CurrentID
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open Telegram accounts: %w", err)
	}
	go m.monitorSessions(ctx)
	return m, nil
}

func (m *Manager) List() ([]Account, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	accounts := make([]Account, len(m.accounts))
	copy(accounts, m.accounts)
	return accounts, m.current
}

// StartQR begins a QR login and returns the account record it created.
//
// A login already waiting for its code is returned as it is rather than being
// duplicated. Every call otherwise creates another account record, another
// persisted login job and another background QR goroutine holding its own
// MTProto connection, so a double click, a retried request or the renew flow in
// the accounts view left two half-authenticated accounts behind. Unlike task
// creation, which is deduplicated by the media identity, this had no guard at
// all.
func (m *Manager) StartQR() (Account, error) {
	id, err := randomID()
	if err != nil {
		return Account{}, err
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	// The check and the append share one acquisition of the write lock. Reading
	// for a pending login under the read lock and appending afterwards left
	// exactly the window the guard exists to close: two concurrent requests - a
	// double click, a retried request, the renew flow - both saw none pending and
	// both appended an account and started an MTProto login, which is the
	// duplicate pair the comment above describes as fixed.
	if existing, ok := m.pendingLoginLocked(); ok {
		m.mu.Unlock()
		cancel()
		return existing, nil
	}
	account := Account{ID: id, State: "starting", CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	m.accounts = append(m.accounts, account)
	m.jobs[id] = &loginJob{cancel: cancel, password: make(chan string, 1)}
	err = m.saveLocked()
	if err != nil {
		m.accounts = m.accounts[:len(m.accounts)-1]
		delete(m.jobs, id)
	}
	m.mu.Unlock()
	if err != nil {
		cancel()
		return Account{}, err
	}
	applog.Info("telegram", "login_started", "account_id", id)
	go m.runQR(ctx, id)
	return account, nil
}

// pendingLoginLocked reports a login that has not reached a verdict yet, and
// must be called with m.mu held. A second request while one is in flight
// duplicates it: two accounts, two MTProto logins, and a Telegram user with two
// authorized sessions.
//
// The login job is the question, not the account state. A job is created with
// the account and removed when the login ends, the account is deleted, or the
// account is rolled back - so it is present for exactly as long as there is
// something to duplicate. Listing states instead is what let the duplicate
// back in: the list held 'starting' and 'waiting_for_qr', but the two-factor
// prompt moves the account to 'waiting_for_2fa', and that step waits for a
// person to type. A retried request or a second click during it appended a
// second account. States also survive a restart as history (a login interrupted
// by one is reloaded as 'stopped'), so they were never the right question.
func (m *Manager) pendingLoginLocked() (Account, bool) {
	for id := range m.jobs {
		for _, account := range m.accounts {
			if account.ID == id {
				return account, true
			}
		}
	}
	return Account{}, false
}

func (m *Manager) SubmitPassword(id, password string) error {
	if password == "" {
		return errors.New("请输入两步验证密码")
	}
	m.mu.RLock()
	job, ok := m.jobs[id]
	account, found := m.accountLocked(id)
	m.mu.RUnlock()
	if !found {
		return ErrNotFound
	}
	if !ok || account.State != "waiting_for_2fa" {
		return ErrPasswordNeeded
	}
	select {
	case job.password <- password:
		return nil
	default:
		return errors.New("密码正在验证，请稍候")
	}
}

func (m *Manager) Select(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	account, ok := m.accountLocked(id)
	if !ok {
		return ErrNotFound
	}
	if account.State != "authorized" {
		return ErrNotAuthorized
	}
	previous := m.current
	m.current = id
	if err := m.saveLocked(); err != nil {
		m.current = previous
		return err
	}
	return nil
}

func (m *Manager) Delete(id string) error {
	// Account removal must wait for in-flight reads/downloads, so its session
	// and state files cannot disappear underneath an active client.
	operation := m.operation(id)
	operation.Lock()
	defer operation.Unlock()
	m.mu.Lock()
	index := -1
	for i := range m.accounts {
		if m.accounts[i].ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		m.mu.Unlock()
		return ErrNotFound
	}
	previousAccounts, previousCurrent := m.accounts, m.current
	nextAccounts := append([]Account(nil), m.accounts[:index]...)
	nextAccounts = append(nextAccounts, m.accounts[index+1:]...)
	nextCurrent := m.current
	if nextCurrent == id {
		nextCurrent = ""
	}
	m.accounts, m.current = nextAccounts, nextCurrent
	if err := m.saveLocked(); err != nil {
		m.accounts, m.current = previousAccounts, previousCurrent
		m.mu.Unlock()
		return err
	}
	if job, ok := m.jobs[id]; ok {
		job.cancel()
		delete(m.jobs, id)
	}
	delete(m.stores, id)
	delete(m.operations, id)
	m.mu.Unlock()
	// The account's connection belongs to the account, not to whatever was using
	// it. Removing the account has to close it, and close it before the files
	// come off disk: the session holds its own MTProto client and its own
	// accountStore, and that store writes the session and state files back. A
	// session left running recreated the material this method had just removed,
	// and kept an authenticated connection open for an account that no longer
	// exists.
	m.stopSession(id)
	// The account metadata is now durably removed. Surface a private-file cleanup
	// failure to the caller so it can be handled instead of silently leaving an
	// orphaned Telegram session on disk.
	var cleanupErr error
	if err := os.Remove(m.sessionPath(id)); err != nil && !os.IsNotExist(err) {
		applog.Error("telegram", "account_session_cleanup_failed", "account_id", id, "error", err.Error())
		cleanupErr = fmt.Errorf("清理会话文件: %w", err)
	}
	if err := os.RemoveAll(m.statePath(id)); err != nil {
		applog.Error("telegram", "account_state_cleanup_failed", "account_id", id, "error", err.Error())
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("清理状态目录: %w", err))
	}
	applog.Info("telegram", "account_deleted", "account_id", id)
	if cleanupErr != nil {
		return fmt.Errorf("账户记录已删除，但敏感文件未能完全清理: %w", cleanupErr)
	}
	return nil
}

func (m *Manager) Status() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == "" {
		return "not_connected"
	}
	account, ok := m.accountLocked(m.current)
	if !ok || account.State != "authorized" {
		return "not_connected"
	}
	return "connected"
}

// RunCurrent runs an authenticated operation with the currently selected account.
// Callers never receive the session bytes themselves.
func (m *Manager) RunCurrent(ctx context.Context, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	m.mu.RLock()
	currentID := m.current
	m.mu.RUnlock()
	return m.Run(ctx, currentID, fn)
}

func (m *Manager) CurrentID() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	account, ok := m.accountLocked(m.current)
	if !ok || account.State != "authorized" {
		return "", ErrNotAuthorized
	}
	return account.ID, nil
}

// AuthorizedByTelegramID resolves the local session belonging to one
// Telegram user. Saved Messages are private to that user and must not follow
// the globally selected account.
func (m *Manager) AuthorizedByTelegramID(telegramID int64) (Account, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var found Account
	for _, account := range m.accounts {
		if account.TelegramID != telegramID || account.State != "authorized" {
			continue
		}
		if found.ID != "" {
			return Account{}, errors.New("同一个 Telegram 用户存在多个已登录会话")
		}
		found = account
	}
	if found.ID == "" {
		return Account{}, ErrNotAuthorized
	}
	return found, nil
}

func (m *Manager) normalizeSelfPeer(accountID string, input tg.InputPeerClass) tg.InputPeerClass {
	user, ok := input.(*tg.InputPeerUser)
	if !ok {
		return input
	}
	m.mu.RLock()
	account, found := m.accountLocked(accountID)
	m.mu.RUnlock()
	if found && account.TelegramID != 0 && user.UserID == account.TelegramID {
		return &tg.InputPeerSelf{}
	}
	return input
}

// savedDialogName is the display name of a Saved Messages dialog. It is one
// definition because both the message and the reaction path have to attribute
// the same dialog, and a second spelling would put their downloads in different
// directories.
const savedDialogName = "收藏消息"

func (m *Manager) normalizeRawSelfPeer(accountID string, raw tg.PeerClass) tg.InputPeerClass {
	user, ok := raw.(*tg.PeerUser)
	if !ok {
		return nil
	}
	m.mu.RLock()
	account, found := m.accountLocked(accountID)
	m.mu.RUnlock()
	if found && account.TelegramID != 0 && account.TelegramID == user.UserID {
		return &tg.InputPeerSelf{}
	}
	return nil
}

// acquireSession returns the account's resident connection, opening it if the
// account has none yet.
//
// A session built for a different proxy is replaced rather than reused. The
// upstream client reads the proxy once, when it builds the connection, so
// reusing the session would keep talking through a route the operator has
// already abandoned.
func (m *Manager) acquireSession(id string) (*accountSession, error) {
	for {
		m.mu.RLock()
		account, ok := m.accountLocked(id)
		m.mu.RUnlock()
		if !ok || account.State != "authorized" {
			return nil, ErrNotAuthorized
		}
		proxy := m.proxyURL()
		m.sessionsMu.Lock()
		session := m.sessions[id]
		if session != nil && session.proxy == proxy {
			m.sessionsMu.Unlock()
			return session, nil
		}
		if session == nil {
			ctx, cancel := context.WithCancel(m.ctx)
			session = &accountSession{
				accountID: id,
				proxy:     proxy,
				store:     m.accountStore(id),
				cancel:    cancel,
				done:      make(chan struct{}),
				readyCh:   make(chan struct{}),
				reactions: make(map[uint64]func(context.Context, ReactionEvent)),
				messages:  make(map[uint64]func(context.Context, NewMessageEvent)),
				readies:   make(map[uint64]func()),
			}
			m.sessions[id] = session
			m.sessionsMu.Unlock()
			// The account may have been removed between the read above and this
			// registration. Delete detaches whatever is registered when it runs,
			// so a session registered after that point would survive it - and
			// this one is permanent, so it would go on holding an authenticated
			// connection and rewriting the session files Delete just removed.
			m.mu.RLock()
			_, stillPresent := m.accountLocked(id)
			m.mu.RUnlock()
			if !stillPresent {
				m.cancelSession(session)
				return nil, ErrNotAuthorized
			}
			go m.runSession(ctx, session)
			return session, nil
		}
		delete(m.sessions, id)
		m.sessionsMu.Unlock()
		applog.Info("telegram", "session_proxy_changed", "account_id", id, "proxy_changed", true)
		session.stop(sessionStopTimeout)
	}
}

// detachSession removes an account's session from the registry without stopping
// it, so the caller decides whether the wait is needed.
func (m *Manager) detachSession(id string) *accountSession {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	session := m.sessions[id]
	delete(m.sessions, id)
	return session
}

// dropSession cancels an account's connection without waiting for it. It is for
// the state changes that make the connection unusable - the authorization was
// rejected, the account is being logged in again - where nothing on disk
// depends on the wait.
func (m *Manager) dropSession(id string) {
	if session := m.detachSession(id); session != nil {
		session.cancel()
	}
}

// stopSession cancels an account's connection and waits for it to finish.
//
// Only removal needs the wait. The session holds its own MTProto client and its
// own accountStore, and that store writes the session and state files back, so
// taking those files off disk while the session was still running let it
// recreate them and left an authenticated connection open for an account that
// no longer exists.
func (m *Manager) stopSession(id string) {
	if session := m.detachSession(id); session != nil {
		session.stop(sessionStopTimeout)
	}
}

// forgetSession clears a session the run loop has finished with. It only
// removes the entry if that entry is still this session, because a replacement
// may already have taken its place.
func (m *Manager) forgetSession(session *accountSession) {
	m.sessionsMu.Lock()
	if m.sessions[session.accountID] == session {
		delete(m.sessions, session.accountID)
	}
	m.sessionsMu.Unlock()
}

// cancelSession stops one specific session, detaching it only if it is still the
// registered one: a session that has already been replaced must not remove its
// replacement from the registry on its way out.
func (m *Manager) cancelSession(session *accountSession) {
	m.forgetSession(session)
	session.cancel()
}

// ListenReactions subscribes to the account's shared Telegram update
// connection and reports only reactions made by that account.
func (m *Manager) ListenReactions(ctx context.Context, id string, onEvent func(context.Context, ReactionEvent), onReady func()) error {
	return m.listenUpdates(ctx, id, onEvent, nil, onReady)
}

// ListenNewMessages subscribes to the same shared update connection used by
// reaction triggers. One account therefore has exactly one update consumer.
func (m *Manager) ListenNewMessages(ctx context.Context, id string, onEvent func(context.Context, NewMessageEvent), onReady func()) error {
	return m.listenUpdates(ctx, id, nil, onEvent, onReady)
}

func (m *Manager) listenUpdates(ctx context.Context, id string, onReaction func(context.Context, ReactionEvent), onMessage func(context.Context, NewMessageEvent), onReady func()) error {
	session, err := m.acquireSession(id)
	if err != nil {
		return err
	}

	session.mu.Lock()
	session.nextID++
	subscriptionID := session.nextID
	if onReaction != nil {
		session.reactions[subscriptionID] = onReaction
	}
	if onMessage != nil {
		session.messages[subscriptionID] = onMessage
	}
	if onReady != nil {
		session.readies[subscriptionID] = onReady
		if session.ready {
			go onReady()
		}
	}
	session.mu.Unlock()

	<-ctx.Done()
	m.removeUpdateSubscription(session, subscriptionID)
	return ctx.Err()
}

// removeUpdateSubscription retires one listener's callbacks. It deliberately
// does not close the connection when the last one leaves: the session belongs
// to the account, not to the listener, and keeping it open is what makes the
// next incoming message - and the next link, and the next validity probe - cost
// no handshake.
func (m *Manager) removeUpdateSubscription(session *accountSession, subscriptionID uint64) {
	session.mu.Lock()
	defer session.mu.Unlock()
	delete(session.reactions, subscriptionID)
	delete(session.messages, subscriptionID)
	delete(session.readies, subscriptionID)
}

// runSession owns one account's connection: it opens it, and reopens it after a
// failure that waiting can outlast.
//
// This is the only place that constructs a connection for an account to make
// requests on. It runs for as long as the account stays authorized, so the
// handshake it performs is paid once rather than once per operation.
func (m *Manager) runSession(ctx context.Context, session *accountSession) {
	defer close(session.done)
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		err := m.runSessionConnection(ctx, session)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			return
		}
		session.markDisconnected(err)
		// A connection that cannot be built is not improved by another attempt,
		// so the session ends and the caller is handed the reason instead of
		// being left to wait out its own deadline. The next operation builds a
		// fresh session, so a corrected proxy or app configuration takes effect
		// without the account having to be re-logged-in.
		var buildErr *sessionBuildError
		if errors.As(err, &buildErr) {
			applog.Error("telegram", "session_unbuildable", "account_id", session.accountID, "error", err.Error())
			m.forgetSession(session)
			return
		}
		if isTerminalSessionError(err) {
			// Nothing about waiting makes an unusable authorization usable.
			// Marking the account expired stops every listener that depends on
			// it and tells the person to log in again, which is the only thing
			// that can actually fix this.
			applog.Error("telegram", "session_rejected", "account_id", session.accountID, "error", err.Error())
			m.markExpired(session.accountID)
			// Drop the registration before returning. A subscription arriving
			// after a re-login would otherwise attach to a finished session and
			// wait for updates that can never arrive.
			m.forgetSession(session)
			return
		}
		// The retry budget is for answers a later attempt can outlast - a closed
		// connection, a peer that was briefly unreachable - and every one of the
		// terminal errors above is deliberately not among them.
		delay := time.Duration(1<<min(attempt, 5)) * time.Second
		applog.Error("telegram", "session_retry_scheduled", "account_id", session.accountID, "attempt", attempt+1, "retry_after", delay.String(), "error", err.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (m *Manager) runSessionConnection(ctx context.Context, session *accountSession) error {
	accountID := session.accountID
	store := session.store
	dispatcher := tg.NewUpdateDispatcher()
	var client *gotd.Client
	dispatchReaction := func(updateCtx context.Context, entities tg.Entities, raw tg.MessageClass, updateType string) {
		m.dispatchEditedReaction(updateCtx, accountID, updateType, entities, raw, client, func(_ context.Context, event ReactionEvent) { m.dispatchReaction(session, event) })
	}
	dispatcher.OnMessageReactions(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateMessageReactions) error {
		reactions := &update.Reactions
		emojis := ownReactionEmojis(reactions)
		if len(emojis) == 0 {
			return nil
		}
		event, ok := m.reactionEvent(updateCtx, accountID, entities, update.Peer, update.MsgID, emojis, client)
		if !ok {
			return nil
		}
		applog.Info("reaction", "own_reaction_received", "account_id", accountID, "dialog_id", event.DialogID, "message_id", update.MsgID, "emojis", emojis, "summary", reactions.Min)
		m.dispatchReaction(session, event)
		return nil
	})
	dispatchMessage := func(updateCtx context.Context, entities tg.Entities, raw tg.MessageClass) {
		message, ok := raw.(*tg.Message)
		if !ok {
			return
		}
		input, extractErr := messagePeer.EntitiesFromUpdate(entities).ExtractPeer(message.PeerID)
		if extractErr == nil {
			input = m.normalizeSelfPeer(accountID, input)
			// The update already carries an authoritative InputPeer, and the
			// display name travels in the same entity map. Reading it here is a
			// lookup, not an RPC — resolving a name only when ExtractPeer failed
			// is what left every ordinary message labelled with a placeholder.
			dialogID := inputPeerID(input)
			key := newMessageDialogKey(input, accountID)
			m.dispatchMessage(session, NewMessageEventFor(accountID, key, inputPeerDisplayName(input, entities), dialogID, message, input))
			return
		}
		// The entity is missing from this update, so only a resolved peer can
		// supply the InputPeer. It is also the only source of a name here; when
		// it fails the name stays empty rather than becoming a dialog type label.
		name := ""
		manager := peers.Options{Storage: storage.NewPeers(store)}.Build(client.API())
		if peer, err := manager.ResolvePeer(updateCtx, message.PeerID); err == nil {
			input, name = peer.InputPeer(), peer.VisibleName()
		} else {
			// Telegram occasionally omits an entity from an otherwise valid
			// update. The persistent peer cache can still resolve it; only drop
			// the event when both sources fail, rather than losing a watched
			// message merely because this particular update was incomplete.
			// Keep the raw stable identity. A watched private group/channel has
			// its authoritative InputPeer saved with its chat task, so downstream
			// code can restore that peer even when this update omitted entities.
			// Saved Messages are represented by the current user's raw peer. When
			// Telegram omits entities, normalize that raw identity as well; using
			// user:<id> here would never match the durable self:<account> key.
			if input := m.normalizeRawSelfPeer(accountID, message.PeerID); input != nil {
				m.dispatchMessage(session, NewMessageEventFor(accountID, "self:"+accountID, savedDialogName, 0, message, input))
				return
			}
			kind, id := peerIdentity(message.PeerID)
			key := kind + ":" + fmt.Sprint(id)
			m.dispatchMessage(session, NewMessageEventFor(accountID, key, name, id, message, nil))
			return
		}
		dialogID := inputPeerID(input)
		key := newMessageDialogKey(input, accountID)
		m.dispatchMessage(session, NewMessageEventFor(accountID, key, name, dialogID, message, input))
	}
	dispatcher.OnNewMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
		dispatchMessage(updateCtx, entities, update.Message)
		return nil
	})
	dispatcher.OnNewChannelMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateNewChannelMessage) error {
		dispatchMessage(updateCtx, entities, update.Message)
		return nil
	})
	// An edited message is also offered to the download path. Telegram only
	// reports the new state, so a message edited to attach media - or one whose
	// media was replaced - was previously never considered: the task's range is
	// fixed by its watermark and nothing rewound it.
	//
	// Reusing the new-message admission is safe and cheap. The inbox is unique
	// per (account, dialog, message) and an item the task already holds is left
	// alone, so an edit that only changed a caption or the reaction bookkeeping
	// costs one filtered lookup and downloads nothing twice.
	dispatcher.OnEditMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateEditMessage) error {
		dispatchReaction(updateCtx, entities, update.Message, "edit_message")
		dispatchMessage(updateCtx, entities, update.Message)
		return nil
	})
	dispatcher.OnEditChannelMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateEditChannelMessage) error {
		dispatchReaction(updateCtx, entities, update.Message, "edit_channel_message")
		dispatchMessage(updateCtx, entities, update.Message)
		return nil
	})

	// The gate is installed here too. Resolving a peer for an incoming reaction
	// or message issues the same users.getUsers / channels.getChannels /
	// contacts.resolveUsername requests the download path does, against the same
	// account and the same server-side window, so leaving them unpaced meant the
	// listener and the transfers were not sharing one budget at all.
	client, err := upstreamClient.New(ctx, upstreamClient.Options{KV: store, Proxy: session.proxy, UpdateHandler: dispatcher}, false, m.rpcGateMiddleware(accountID))
	if err != nil {
		return &sessionBuildError{err: fmt.Errorf("创建 Telegram 连接: %w", err)}
	}
	applog.Info("telegram", "session_connected", "account_id", accountID)
	return client.Run(ctx, func(runCtx context.Context) error {
		if _, err := client.Self(runCtx); err != nil {
			return err
		}
		for _, callback := range session.markReady(client) {
			callback()
		}
		<-runCtx.Done()
		return nil
	})
}

func (m *Manager) dispatchReaction(session *accountSession, event ReactionEvent) {
	session.mu.Lock()
	callbacks := make([]func(context.Context, ReactionEvent), 0, len(session.reactions))
	for _, callback := range session.reactions {
		callbacks = append(callbacks, callback)
	}
	session.mu.Unlock()
	// The manager's own context travels with the event. A fresh background one
	// discarded cancellation and deadlines, which made the consumers' ctx.Err()
	// guards unreachable and left nothing able to interrupt a callback already
	// running when the account was stopped.
	for _, callback := range callbacks {
		callback(m.ctx, event)
	}
}

func (m *Manager) dispatchMessage(session *accountSession, event NewMessageEvent) {
	session.mu.Lock()
	callbacks := make([]func(context.Context, NewMessageEvent), 0, len(session.messages))
	for _, callback := range session.messages {
		callbacks = append(callbacks, callback)
	}
	session.mu.Unlock()
	for _, callback := range callbacks {
		callback(m.ctx, event)
	}
}

func replyMessageID(message *tg.Message) int {
	if reply, ok := message.GetReplyTo(); ok {
		if header, ok := reply.(*tg.MessageReplyHeader); ok {
			value, _ := header.GetReplyToMsgID()
			return value
		}
	}
	return 0
}

func replyTopID(message *tg.Message) int {
	if reply, ok := message.GetReplyTo(); ok {
		if header, ok := reply.(*tg.MessageReplyHeader); ok {
			value, _ := header.GetReplyToTopID()
			return value
		}
	}
	return 0
}

func (m *Manager) dispatchEditedReaction(ctx context.Context, accountID, updateType string, entities tg.Entities, raw tg.MessageClass, client *gotd.Client, onEvent func(context.Context, ReactionEvent)) {
	message, ok := raw.(*tg.Message)
	if !ok {
		return
	}
	reactions, ok := message.GetReactions()
	if !ok {
		return
	}
	emojis := ownReactionEmojis(&reactions)
	if len(emojis) == 0 {
		return
	}
	event, ok := m.reactionEvent(ctx, accountID, entities, message.PeerID, message.ID, emojis, client)
	if !ok {
		return
	}
	applog.Info("reaction", "own_reaction_received", "account_id", accountID, "update_type", updateType, "dialog_id", event.DialogID, "message_id", event.MessageID, "emojis", emojis, "summary", reactions.Min)
	dispatchReactionEvent(ctx, onEvent, event)
}

// reactionEvent turns a Telegram peer into a direct-download event. A public
// t.me URL is optional metadata only; the InputPeer is authoritative and works
// for private dialogs as well.
func (m *Manager) reactionEvent(ctx context.Context, accountID string, entities tg.Entities, rawPeer tg.PeerClass, messageID int, emojis []string, client *gotd.Client) (ReactionEvent, bool) {
	inputPeer, err := messagePeer.EntitiesFromUpdate(entities).ExtractPeer(rawPeer)
	if err != nil {
		// Telegram omits entities from some updates while still naming the dialog,
		// and Saved Messages are the case that matters: their peer is the current
		// user. The new-message path has always normalized that identity rather
		// than give up on it; reactions did not, so a reaction on a Saved Message
		// was logged as an unresolvable peer and dropped - even though
		// reactionFallbackURL has a dedicated tg://reaction/self/... form for it,
		// which nothing could reach.
		if self := m.normalizeRawSelfPeer(accountID, rawPeer); self != nil {
			return ReactionEvent{AccountID: accountID, DialogName: savedDialogName, MessageID: messageID, SourceURL: reactionFallbackURL(self, accountID, messageID), InputPeer: self, Emojis: emojis}, true
		}
		peerType, peerID := peerIdentity(rawPeer)
		applog.Error("reaction", "peer_extract_failed", "account_id", accountID, "peer_type", peerType, "dialog_id", peerID, "message_id", messageID, "error", err.Error())
		return ReactionEvent{}, false
	}
	dialogID := inputPeerID(inputPeer)
	// A dialog name that cannot be resolved stays empty. Falling back to a type
	// label would attribute unrelated dialogs to the same download directory.
	dialogName := ""
	sourceURL := reactionFallbackURL(inputPeer, accountID, messageID)
	manager := peers.Options{Storage: storage.NewPeers(m.accountStore(accountID))}.Build(client.API())
	// Resolve through the InputPeer extracted from this update: ExtractPeer only
	// succeeds when the entity is present, and it copies that entity's access
	// hash into the InputPeer. ResolvePeer(rawPeer) would discard the hash and
	// look the peer up by ID alone, which fails whenever the peers storage has
	// not been seeded yet and silently degraded the name to a type label.
	peer, resolveErr := manager.FromInputPeer(ctx, inputPeer)
	if resolveErr != nil {
		applog.Info("reaction", "peer_name_unavailable", "account_id", accountID, "dialog_id", dialogID, "message_id", messageID, "error", resolveErr.Error())
	} else {
		// peers.User maps the current user to InputPeerSelf, which preserves the
		// special identity of Saved Messages.
		inputPeer = peer.InputPeer()
		dialogID, dialogName = peer.ID(), peer.VisibleName()
		sourceURL = reactionSourceURL(peer, accountID, messageID)
	}
	return ReactionEvent{AccountID: accountID, DialogID: dialogID, DialogName: dialogName, MessageID: messageID, SourceURL: sourceURL, InputPeer: inputPeer, Emojis: emojis}, true
}

func dispatchReactionEvent(ctx context.Context, onEvent func(context.Context, ReactionEvent), event ReactionEvent) {
	// The consumer owns bounded buffering. Do not create one goroutine for each
	// update: a busy group plus a slow database write would otherwise grow memory
	// without limit.
	//
	// The caller's context travels with the event; see dispatchReaction.
	onEvent(ctx, event)
}

// logReactionEditUpdate emits a privacy-safe diagnostic only when an edited
// message actually contains reaction state. It intentionally excludes message
// text, captions, usernames and media names.
func peerIdentity(peer tg.PeerClass) (string, int64) {
	switch value := peer.(type) {
	case *tg.PeerUser:
		return "user", value.UserID
	case *tg.PeerChat:
		return "chat", value.ChatID
	case *tg.PeerChannel:
		return "channel", value.ChannelID
	default:
		return "unknown", 0
	}
}

// inputPeerID returns the stable Telegram ID of a dialog. It intentionally
// returns no name: a dialog *type* label is not a name, and using one as a name
// collapsed every unresolved private dialog onto a single download directory.
// Type labels belong to the presentation layer only.
func inputPeerID(input tg.InputPeerClass) int64 {
	switch peer := input.(type) {
	case *tg.InputPeerSelf:
		return 0
	case *tg.InputPeerUser:
		return peer.UserID
	case *tg.InputPeerChat:
		return peer.ChatID
	case *tg.InputPeerChannel:
		return peer.ChannelID
	default:
		return 0
	}
}

// inputPeerDisplayName reads a dialog display name out of the entity map that
// accompanied the update. Callers must already have established that the entity
// is present, which is what makes this a map lookup instead of an RPC.
//
// It returns "" rather than a dialog type label when nothing is available: a
// label is not a name, and persisting one attributes unrelated dialogs to the
// same download directory.
func inputPeerDisplayName(input tg.InputPeerClass, entities tg.Entities) string {
	switch peer := input.(type) {
	case *tg.InputPeerSelf:
		return savedDialogName
	case *tg.InputPeerUser:
		if user, ok := entities.Users[peer.UserID]; ok {
			return visibleUserName(user)
		}
	case *tg.InputPeerChat:
		if chat, ok := entities.Chats[peer.ChatID]; ok {
			return chat.Title
		}
	case *tg.InputPeerChannel:
		if channel, ok := entities.Channels[peer.ChannelID]; ok {
			return channel.Title
		}
	}
	return ""
}

// visibleUserName mirrors gotd's peers.User.VisibleName so a dialog named from
// an update entity matches the same dialog named through a resolved peer.
func visibleUserName(user *tg.User) string {
	if user.LastName == "" {
		return user.FirstName
	}
	return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
}

func newMessageDialogKey(input tg.InputPeerClass, accountID string) string {
	switch peer := input.(type) {
	case *tg.InputPeerSelf:
		return "self:" + accountID
	case *tg.InputPeerUser:
		return fmt.Sprintf("user:%d", peer.UserID)
	case *tg.InputPeerChat:
		return fmt.Sprintf("chat:%d", peer.ChatID)
	case *tg.InputPeerChannel:
		return fmt.Sprintf("channel:%d", peer.ChannelID)
	default:
		return ""
	}
}

func ownReactionEmojis(reactions *tg.MessageReactions) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, reaction := range reactions.RecentReactions {
		if !reaction.My {
			continue
		}
		emoji, ok := reaction.Reaction.(*tg.ReactionEmoji)
		if !ok || emoji.Emoticon == "" {
			continue // Custom emoji cannot be selected reliably by a text setting.
		}
		if _, ok := seen[emoji.Emoticon]; ok {
			continue
		}
		seen[emoji.Emoticon] = struct{}{}
		result = append(result, emoji.Emoticon)
	}
	// Telegram may send a compact reaction update without RecentReactions. The
	// per-reaction Chosen flag is still enough to determine whether this account
	// currently has a matching standard emoji on the message.
	for _, count := range reactions.Results {
		if _, chosen := count.GetChosenOrder(); !chosen {
			continue
		}
		emoji, ok := count.Reaction.(*tg.ReactionEmoji)
		if !ok || emoji.Emoticon == "" {
			continue
		}
		if _, ok := seen[emoji.Emoticon]; ok {
			continue
		}
		seen[emoji.Emoticon] = struct{}{}
		result = append(result, emoji.Emoticon)
	}
	return result
}

func reactionSourceURL(peer peers.Peer, accountID string, messageID int) string {
	if username, ok := peer.Username(); ok && username != "" {
		return fmt.Sprintf("https://t.me/%s/%d", username, messageID)
	}
	return reactionFallbackURL(peer.InputPeer(), accountID, messageID)
}

func reactionFallbackURL(peer tg.InputPeerClass, accountID string, messageID int) string {
	if _, ok := peer.(*tg.InputPeerSelf); ok {
		return fmt.Sprintf("tg://reaction/self/%s/%d", accountID, messageID)
	}
	switch value := peer.(type) {
	case *tg.InputPeerUser:
		return fmt.Sprintf("tg://reaction/user/%d/%d", value.UserID, messageID)
	case *tg.InputPeerChat:
		return fmt.Sprintf("tg://reaction/chat/%d/%d", value.ChatID, messageID)
	case *tg.InputPeerChannel:
		return fmt.Sprintf("tg://reaction/channel/%d/%d", value.ChannelID, messageID)
	default:
		return fmt.Sprintf("tg://reaction/unknown/0/%d", messageID)
	}
}

// Run executes an operation with one specific authorized account. Download
// jobs store this ID at enqueue time so changing the UI selection later cannot
// change which account resumes a task.
func (m *Manager) Run(ctx context.Context, id string, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	return m.runAccount(ctx, id, fn)
}

func (m *Manager) runAccount(ctx context.Context, id string, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	operation := m.operation(id)
	// A shared lease, which concurrent operations all take at once. It is no
	// longer about how many connections an account may have - there is one - but
	// about ordering against removal: Delete takes the lease exclusively, so the
	// account's session and state files cannot disappear underneath an operation
	// that is still using them.
	operation.RLock()
	defer operation.RUnlock()
	return m.runAccountLocked(ctx, id, fn)
}

// runAccountLocked runs one operation on the account's resident connection.
//
// It used to build a client and run it for the duration of the call, which is
// where every operation's fixed cost came from. It now waits for the account's
// session instead, and the only thing that opens a connection is runSession.
func (m *Manager) runAccountLocked(ctx context.Context, id string, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	session, err := m.acquireSession(id)
	if err != nil {
		return err
	}
	client, err := session.waitReady(ctx)
	if err != nil {
		return err
	}
	// The operation's context ends when the account's connection does, not only
	// when its caller gives up. Callers hand this context to a transfer, and a
	// transfer whose connection is gone has to stop rather than keep waiting for
	// bytes that can no longer arrive.
	opCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-session.done:
			cancel()
		case <-opCtx.Done():
		}
	}()
	return fn(opCtx, client, session.store)
}

func (m *Manager) operation(id string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	operation := m.operations[id]
	if operation == nil {
		operation = &sync.RWMutex{}
		m.operations[id] = operation
	}
	return operation
}

func (m *Manager) monitorSessions(ctx context.Context) {
	timer := time.NewTimer(sessionCheckInitialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.checkSessions(ctx)
			timer.Reset(sessionCheckInterval)
		}
	}
}

// checkSessions confirms that Telegram still accepts each account's saved
// authorization key. A successful Self request is the whole test.
//
// The probe runs on the account's resident connection, so it opens nothing. It
// used to take an exclusive lease for the account so that it would not open a
// second main session beside a download or a listener, which had two outcomes
// and both were wrong: the listener never took that lease, so the invariant the
// lease defended was never true, and an account with work in flight never
// released it, so the check was skipped for exactly the accounts whose sessions
// are most likely to be noticed as revoked. Riding the one connection removes
// the question - there is no second session to avoid, and nothing to wait for.
//
// The shared lease it does take is the same one every other operation takes,
// and it is there for removal rather than for connections: Delete waits for
// in-flight work before it takes the session files off disk.
func (m *Manager) checkSessions(parent context.Context) {
	m.mu.RLock()
	ids := make([]string, 0, len(m.accounts))
	for _, account := range m.accounts {
		if account.State == "authorized" {
			ids = append(ids, account.ID)
		}
	}
	m.mu.RUnlock()
	for _, id := range ids {
		operation := m.operation(id)
		operation.RLock()
		ctx, cancel := context.WithTimeout(parent, sessionCheckTimeout)
		err := m.runAccountLocked(ctx, id, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
			_, err := client.Self(ctx)
			return err
		})
		cancel()
		operation.RUnlock()
		if isTerminalSessionError(err) {
			m.markExpired(id)
			continue
		}
		if shouldMarkSessionChecked(err) {
			m.markChecked(id)
			continue
		}
		// A timeout or unavailable proxy does not prove the authorization is
		// healthy. Keep the prior successful check time intact rather than
		// presenting a failed network probe as a fresh validation.
		applog.Info("telegram", "session_check_failed", "account_id", id, "error", err.Error())
	}
}

// Stop releases periodic checks and incomplete QR login connections during a
// graceful application shutdown.
func (m *Manager) Stop() {
	m.cancel()
	m.mu.RLock()
	cancels := make([]context.CancelFunc, 0, len(m.jobs))
	for _, job := range m.jobs {
		cancels = append(cancels, job.cancel)
	}
	m.mu.RUnlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// terminalSessionErrors are the answers that say the stored authorization can
// no longer be used at all, so only logging in again can change the outcome.
// They are matched through tgerr and on the raw text, because the same
// rejection also arrives wrapped by the transport with no type left to read.
//
// AUTH_KEY_UNREGISTERED used to be the only one recognized. A session revoked
// from another device answers SESSION_REVOKED instead, and the account stayed
// marked "authorized": the update hub reconnected every 32 seconds for as long
// as the process lived, no listener ever stopped, and nothing ever told the
// person that the session had to be restored. The retry budget is for answers
// that a later attempt can outlast - a closed connection, a peer that was
// briefly unreachable - and none of these are.
var terminalSessionErrors = []string{
	"AUTH_KEY_UNREGISTERED",
	"SESSION_REVOKED",
	"SESSION_EXPIRED",
	"API_ID_INVALID",
	// Covers USER_DEACTIVATED_BAN as well as the plain form.
	"USER_DEACTIVATED",
}

func isTerminalSessionError(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	for _, name := range terminalSessionErrors {
		if tgerr.Is(err, name) || strings.Contains(text, name) {
			return true
		}
	}
	return false
}

func shouldMarkSessionChecked(err error) bool { return err == nil }

func (m *Manager) markChecked(id string) {
	m.update(id, func(a *Account) { a.CheckedAt = time.Now().UTC().Format(time.RFC3339) })
}

func (m *Manager) markExpired(id string) {
	// The connection is unusable whatever the loop below decides, so it is
	// dropped either way. This is registered before the lock so that it runs
	// after the state change has been saved, and it does not wait: the run loop
	// that reported the failure is usually the one being cancelled, and it has
	// nothing left to protect on disk.
	defer m.dropSession(id)
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.accounts {
		if m.accounts[i].ID != id || m.accounts[i].State != "authorized" {
			continue
		}
		previous, previousCurrent := m.accounts[i], m.current
		m.accounts[i].State = "expired"
		m.accounts[i].Error = "Telegram 会话已失效，请重新登录"
		m.accounts[i].QRCode = ""
		m.accounts[i].CheckedAt = time.Now().UTC().Format(time.RFC3339)
		if m.current == id {
			m.current = ""
		}
		if err := m.saveLocked(); err != nil {
			m.accounts[i], m.current = previous, previousCurrent
			applog.Error("telegram", "session_expiry_save_failed", "account_id", id, "error", err.Error())
			return
		}
		applog.Error("telegram", "session_expired", "account_id", id)
		return
	}
}

func (m *Manager) runQR(ctx context.Context, id string) {
	store := m.accountStore(id)
	dispatcher := tg.NewUpdateDispatcher()
	client, err := upstreamClient.New(ctx, upstreamClient.Options{KV: store, Proxy: m.proxyURL(), UpdateHandler: dispatcher}, true, m.rpcGateMiddleware(id))
	if err != nil {
		m.setError(id, fmt.Errorf("创建 Telegram 客户端失败: %w", err))
		return
	}
	err = client.Run(ctx, func(ctx context.Context) error {
		_, err := client.QR().Auth(ctx, qrlogin.OnLoginToken(dispatcher), func(_ context.Context, token qrlogin.Token) error {
			png, err := qrcode.Encode(token.URL(), qrcode.Medium, 280)
			if err != nil {
				return err
			}
			m.setQR(id, "data:image/png;base64,"+base64.StdEncoding.EncodeToString(png))
			return nil
		})
		if err != nil && tgerr.Is(err, "SESSION_PASSWORD_NEEDED") {
			for {
				m.setState(id, "waiting_for_2fa", "")
				password, ok := m.waitPassword(ctx, id)
				if !ok {
					return ctx.Err()
				}
				if _, err = client.Auth().Password(ctx, password); err == nil {
					break
				}
				m.setState(id, "waiting_for_2fa", "两步验证密码不正确，请重试")
			}
			err = nil
		}
		if err != nil {
			return err
		}
		user, err := client.Self(ctx)
		if err != nil {
			return err
		}
		m.authorize(id, user)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		m.setError(id, fmt.Errorf("Telegram 登录失败: %w", err))
	}
	m.mu.Lock()
	delete(m.jobs, id)
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *Manager) waitPassword(ctx context.Context, id string) (string, bool) {
	m.mu.RLock()
	job := m.jobs[id]
	m.mu.RUnlock()
	if job == nil {
		return "", false
	}
	select {
	case password := <-job.password:
		return password, true
	case <-ctx.Done():
		return "", false
	}
}
func (m *Manager) setQR(id, qr string) {
	m.update(id, func(a *Account) { a.State, a.Error, a.QRCode = "waiting_for_qr", "", qr })
}
func (m *Manager) setState(id, state, message string) {
	m.update(id, func(a *Account) {
		a.State, a.Error = state, message
		if state != "waiting_for_qr" {
			a.QRCode = ""
		}
	})
}
func (m *Manager) setError(id string, err error) {
	applog.Error("telegram", "account_error", "account_id", id, "error", err.Error())
	m.setState(id, "error", err.Error())
	// The account is no longer authorized, so nothing may still be using its
	// connection. A later login builds a new one from the new session file.
	m.dropSession(id)
}
func (m *Manager) authorize(id string, user *tg.User) {
	// A fresh authorization invalidates any connection opened with the material
	// that preceded it. There is normally none - the account was not authorized
	// a moment ago - and this is what makes that true even if it was.
	m.dropSession(id)
	m.update(id, func(a *Account) {
		a.TelegramID, a.FirstName, a.LastName, a.Username = user.ID, user.FirstName, user.LastName, user.Username
		a.State, a.Error, a.QRCode = "authorized", "", ""
		if m.current == "" {
			m.current = id
		}
	})
	applog.Info("telegram", "login_authorized", "account_id", id, "telegram_id", user.ID)
}
func (m *Manager) update(id string, update func(*Account)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.accounts {
		if m.accounts[i].ID == id {
			previous := m.accounts[i]
			update(&m.accounts[i])
			if err := m.saveLocked(); err != nil {
				m.accounts[i] = previous
				applog.Error("telegram", "account_state_save_failed", "account_id", id, "error", err.Error())
			}
			return
		}
	}
}
func (m *Manager) accountLocked(id string) (Account, bool) {
	for _, account := range m.accounts {
		if account.ID == id {
			return account, true
		}
	}
	return Account{}, false
}
func (m *Manager) sessionPath(id string) string {
	return filepath.Join(m.root, "sessions", id+".session")
}

func (m *Manager) statePath(id string) string {
	return filepath.Join(m.root, "state", id)
}

func (m *Manager) accountStore(id string) *accountStore {
	m.mu.Lock()
	fileMu := m.stores[id]
	if fileMu == nil {
		fileMu = &sync.RWMutex{}
		m.stores[id] = fileMu
	}
	m.mu.Unlock()
	return &accountStore{sessionPath: m.sessionPath(id), stateDir: m.statePath(id), fileMu: fileMu}
}
func (m *Manager) saveLocked() error {
	data, err := json.MarshalIndent(persisted{Accounts: m.accounts, CurrentID: m.current}, "", "  ")
	if err != nil {
		return err
	}
	return writePrivateFile(m.path, data)
}
func randomID() (string, error) {
	data := make([]byte, 18)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

// accountStore separates the sensitive Telegram authorization session from
// arbitrary upstream tdl KV keys such as peers and download resume progress.
// They must never share a file: dl.Run persists resume data through this same
// Storage interface.
type accountStore struct {
	sessionPath string
	stateDir    string
	fileMu      *sync.RWMutex
}

func (s *accountStore) Get(_ context.Context, key string) ([]byte, error) {
	if s.fileMu != nil {
		s.fileMu.RLock()
		defer s.fileMu.RUnlock()
	}
	if key == upstreamKey.App() {
		return []byte(upstreamClient.AppDesktop), nil
	}
	data, err := os.ReadFile(s.path(key))
	if os.IsNotExist(err) {
		return nil, storage.ErrNotFound
	}
	return data, err
}
func (s *accountStore) Set(_ context.Context, key string, value []byte) error {
	if s.fileMu != nil {
		s.fileMu.Lock()
		defer s.fileMu.Unlock()
	}
	return writePrivateFile(s.path(key), value)
}
func (s *accountStore) Delete(_ context.Context, key string) error {
	if s.fileMu != nil {
		s.fileMu.Lock()
		defer s.fileMu.Unlock()
	}
	if err := os.Remove(s.path(key)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *accountStore) path(key string) string {
	if key == "session" {
		return s.sessionPath
	}
	return filepath.Join(s.stateDir, base64.RawURLEncoding.EncodeToString([]byte(key)))
}

func writePrivateFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
