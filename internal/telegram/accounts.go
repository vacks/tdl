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

type persisted struct {
	Accounts  []Account `json:"accounts"`
	CurrentID string    `json:"currentId"`
}

type loginJob struct {
	cancel   context.CancelFunc
	password chan string
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
	operations map[string]*sync.Mutex
	proxyURL   func() string
}

func Open(dataDir string, proxyURL func() string) (*Manager, error) {
	root := filepath.Join(dataDir, "telegram")
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o700); err != nil {
		return nil, fmt.Errorf("create Telegram data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "state"), 0o700); err != nil {
		return nil, fmt.Errorf("create Telegram state directory: %w", err)
	}
	m := &Manager{root: root, path: filepath.Join(root, "accounts.json"), jobs: make(map[string]*loginJob), operations: make(map[string]*sync.Mutex), proxyURL: proxyURL}
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
	go m.monitorSessions()
	return m, nil
}

func (m *Manager) List() ([]Account, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	accounts := make([]Account, len(m.accounts))
	copy(accounts, m.accounts)
	return accounts, m.current
}

func (m *Manager) StartQR() (Account, error) {
	id, err := randomID()
	if err != nil {
		return Account{}, err
	}
	account := Account{ID: id, State: "starting", CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.accounts = append(m.accounts, account)
	m.jobs[id] = &loginJob{cancel: cancel, password: make(chan string, 1)}
	err = m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		cancel()
		return Account{}, err
	}
	applog.Info("telegram", "login_started", "account_id", id)
	go m.runQR(ctx, id)
	return account, nil
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
	m.current = id
	return m.saveLocked()
}

func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := -1
	for i := range m.accounts {
		if m.accounts[i].ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		return ErrNotFound
	}
	if job, ok := m.jobs[id]; ok {
		job.cancel()
		delete(m.jobs, id)
	}
	m.accounts = append(m.accounts[:index], m.accounts[index+1:]...)
	if m.current == id {
		m.current = ""
	}
	if err := os.Remove(m.sessionPath(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.RemoveAll(m.statePath(id)); err != nil {
		return err
	}
	err := m.saveLocked()
	if err == nil {
		applog.Info("telegram", "account_deleted", "account_id", id)
	}
	return err
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

// ListenReactions keeps a separate MTProto connection open for one account
// and reports only reactions Telegram marks as sent by that account (My).
// Download operations remain short-lived and use their existing connection;
// this listener must never hold the account operation mutex for its lifetime.
func (m *Manager) ListenReactions(ctx context.Context, id string, onEvent func(context.Context, ReactionEvent)) error {
	m.mu.RLock()
	account, ok := m.accountLocked(id)
	proxyURL := m.proxyURL
	m.mu.RUnlock()
	if !ok || account.State != "authorized" {
		return ErrNotAuthorized
	}

	store := m.accountStore(id)
	dispatcher := tg.NewUpdateDispatcher()
	var client *gotd.Client
	dispatcher.OnMessageReactions(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateMessageReactions) error {
		reactions := &update.Reactions
		emojis := ownReactionEmojis(reactions)
		if len(emojis) == 0 {
			applog.Info("reaction", "update_ignored_not_own", "account_id", id, "message_id", update.MsgID, "summary", reactions.Min)
			return nil
		}
		inputPeer, err := messagePeer.EntitiesFromUpdate(entities).ExtractPeer(update.Peer)
		if err != nil {
			applog.Error("reaction", "peer_extract_failed", "account_id", id, "message_id", update.MsgID, "error", err.Error())
			return nil
		}
		manager := peers.Options{Storage: storage.NewPeers(store)}.Build(client.API())
		peer, err := manager.ResolvePeer(updateCtx, update.Peer)
		if err != nil {
			applog.Info("reaction", "peer_name_unavailable", "account_id", id, "message_id", update.MsgID, "error", err.Error())
		}
		dialogID, dialogName := inputPeerInfo(inputPeer)
		sourceURL := fmt.Sprintf("tg://reaction/%d/%d", dialogID, update.MsgID)
		if err == nil {
			dialogID, dialogName = peer.ID(), peer.VisibleName()
			sourceURL = reactionSourceURL(peer, update.MsgID)
		}
		event := ReactionEvent{AccountID: id, DialogID: dialogID, DialogName: dialogName, MessageID: update.MsgID, SourceURL: sourceURL, InputPeer: inputPeer, Emojis: emojis}
		applog.Info("reaction", "own_reaction_received", "account_id", id, "dialog_id", peer.ID(), "message_id", update.MsgID, "emojis", emojis, "summary", reactions.Min)
		// Update dispatch must stay responsive. Task parsing is independent and
		// is bounded so a temporary Telegram/API error cannot block later updates.
		go func() {
			workCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			onEvent(workCtx, event)
		}()
		return nil
	})

	var err error
	client, err = upstreamClient.New(ctx, upstreamClient.Options{KV: store, Proxy: proxyURL(), UpdateHandler: dispatcher}, false)
	if err != nil {
		applog.Error("reaction", "listener_connect_failed", "account_id", id, "error", err.Error())
		return fmt.Errorf("创建表情监听连接失败: %w", err)
	}
	applog.Info("reaction", "listener_connected", "account_id", id)
	return client.Run(ctx, func(runCtx context.Context) error {
		if _, err := client.Self(runCtx); err != nil {
			applog.Error("reaction", "listener_authorization_check_failed", "account_id", id, "error", err.Error())
			return err
		}
		<-runCtx.Done()
		return nil
	})
}

func inputPeerInfo(input tg.InputPeerClass) (int64, string) {
	switch peer := input.(type) {
	case *tg.InputPeerUser:
		return peer.UserID, "私聊"
	case *tg.InputPeerChat:
		return peer.ChatID, "群组"
	case *tg.InputPeerChannel:
		return peer.ChannelID, "频道"
	default:
		return 0, "会话"
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

func reactionSourceURL(peer peers.Peer, messageID int) string {
	if username, ok := peer.Username(); ok && username != "" {
		return fmt.Sprintf("https://t.me/%s/%d", username, messageID)
	}
	if channel, ok := peer.(peers.Channel); ok {
		return fmt.Sprintf("https://t.me/c/%d/%d", channel.ID(), messageID)
	}
	return fmt.Sprintf("tg://reaction/%d/%d", peer.ID(), messageID)
}

// Run executes an operation with one specific authorized account. Download
// jobs store this ID at enqueue time so changing the UI selection later cannot
// change which account resumes a task.
func (m *Manager) Run(ctx context.Context, id string, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	return m.runAccount(ctx, id, fn)
}

func (m *Manager) runAccount(ctx context.Context, id string, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	operation := m.operation(id)
	operation.Lock()
	defer operation.Unlock()
	return m.runAccountLocked(ctx, id, fn)
}

func (m *Manager) runAccountLocked(ctx context.Context, id string, fn func(context.Context, *gotd.Client, storage.Storage) error) error {
	m.mu.RLock()
	account, ok := m.accountLocked(id)
	proxyURL := m.proxyURL
	m.mu.RUnlock()
	if !ok || account.State != "authorized" {
		return ErrNotAuthorized
	}
	store := m.accountStore(id)
	client, err := upstreamClient.New(ctx, upstreamClient.Options{KV: store, Proxy: proxyURL()}, false)
	if err != nil {
		return fmt.Errorf("创建 Telegram 客户端失败: %w", err)
	}
	return client.Run(ctx, func(ctx context.Context) error { return fn(ctx, client, store) })
}

func (m *Manager) operation(id string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	operation := m.operations[id]
	if operation == nil {
		operation = &sync.Mutex{}
		m.operations[id] = operation
	}
	return operation
}

func (m *Manager) monitorSessions() {
	timer := time.NewTimer(sessionCheckInitialDelay)
	defer timer.Stop()
	for {
		<-timer.C
		m.checkSessions()
		timer.Reset(sessionCheckInterval)
	}
}

// checkSessions is intentionally independent of downloads and other business
// operations. A successful Self request confirms that Telegram still accepts
// the saved authorization key for that individual account.
func (m *Manager) checkSessions() {
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
		// Do not open a second main MTProto session with the same authorization
		// key while a download or another account operation is active.
		if !operation.TryLock() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		err := m.runAccountLocked(ctx, id, func(ctx context.Context, client *gotd.Client, _ storage.Storage) error {
			_, err := client.Self(ctx)
			return err
		})
		cancel()
		operation.Unlock()
		if isAuthKeyUnregistered(err) {
			m.markExpired(id)
			continue
		}
		m.markChecked(id)
	}
}

func isAuthKeyUnregistered(err error) bool {
	return err != nil && (tgerr.Is(err, "AUTH_KEY_UNREGISTERED") || strings.Contains(err.Error(), "AUTH_KEY_UNREGISTERED"))
}

func (m *Manager) markChecked(id string) {
	m.update(id, func(a *Account) { a.CheckedAt = time.Now().UTC().Format(time.RFC3339) })
}

func (m *Manager) markExpired(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.accounts {
		if m.accounts[i].ID != id || m.accounts[i].State != "authorized" {
			continue
		}
		m.accounts[i].State = "expired"
		m.accounts[i].Error = "Telegram 会话已失效，请重新登录"
		m.accounts[i].QRCode = ""
		m.accounts[i].CheckedAt = time.Now().UTC().Format(time.RFC3339)
		if m.current == id {
			m.current = ""
		}
		_ = m.saveLocked()
		applog.Error("telegram", "session_expired", "account_id", id)
		return
	}
}

func (m *Manager) runQR(ctx context.Context, id string) {
	store := m.accountStore(id)
	dispatcher := tg.NewUpdateDispatcher()
	client, err := upstreamClient.New(ctx, upstreamClient.Options{KV: store, Proxy: m.proxyURL(), UpdateHandler: dispatcher}, true)
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
func (m *Manager) setError(id string, err error) { applog.Error("telegram", "account_error", "account_id", id, "error", err.Error()); m.setState(id, "error", err.Error()) }
func (m *Manager) authorize(id string, user *tg.User) {
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
			update(&m.accounts[i])
			_ = m.saveLocked()
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
	return &accountStore{sessionPath: m.sessionPath(id), stateDir: m.statePath(id)}
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
}

func (s *accountStore) Get(_ context.Context, key string) ([]byte, error) {
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
	return writePrivateFile(s.path(key), value)
}
func (s *accountStore) Delete(_ context.Context, key string) error {
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
