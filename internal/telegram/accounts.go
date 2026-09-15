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

// NewMessageEvent is a normalized update for one newly received message.
// It intentionally carries an InputPeer, so consumers do not depend on
// usernames or public links (both may be unavailable for private groups).
type NewMessageEvent struct {
	AccountID  string
	DialogID   int64
	DialogName string
	MessageID  int
	InputPeer  tg.InputPeerClass
}

type persisted struct {
	Accounts  []Account `json:"accounts"`
	CurrentID string    `json:"currentId"`
}

type loginJob struct {
	cancel   context.CancelFunc
	password chan string
}

// updateHub owns the single long-lived Telegram update connection for one
// account. Reaction and new-message consumers subscribe to this hub instead
// of opening competing connections with the same authorization key.
type updateHub struct {
	cancel    context.CancelFunc
	done      chan struct{}
	proxy     string
	ready     bool
	nextID    uint64
	reactions map[uint64]func(context.Context, ReactionEvent)
	messages  map[uint64]func(context.Context, NewMessageEvent)
	readies   map[uint64]func()
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
	updatesMu  sync.Mutex
	updates    map[string]*updateHub
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
	m := &Manager{root: root, path: filepath.Join(root, "accounts.json"), jobs: make(map[string]*loginJob), operations: make(map[string]*sync.RWMutex), stores: make(map[string]*sync.RWMutex), proxyURL: proxyURL, ctx: ctx, cancel: cancel, updates: make(map[string]*updateHub)}
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

func (m *Manager) StartQR() (Account, error) {
	id, err := randomID()
	if err != nil {
		return Account{}, err
	}
	account := Account{ID: id, State: "starting", CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	ctx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
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
	m.mu.RLock()
	account, ok := m.accountLocked(id)
	m.mu.RUnlock()
	if !ok || account.State != "authorized" {
		return ErrNotAuthorized
	}

	m.updatesMu.Lock()
	hub := m.updates[id]
	if hub == nil {
		hubCtx, cancel := context.WithCancel(m.ctx)
		hub = &updateHub{cancel: cancel, done: make(chan struct{}), proxy: m.proxyURL(), reactions: make(map[uint64]func(context.Context, ReactionEvent)), messages: make(map[uint64]func(context.Context, NewMessageEvent)), readies: make(map[uint64]func())}
		m.updates[id] = hub
		go m.runUpdateHub(hubCtx, id, hub)
	}
	hub.nextID++
	subscriptionID := hub.nextID
	if onReaction != nil {
		hub.reactions[subscriptionID] = onReaction
	}
	if onMessage != nil {
		hub.messages[subscriptionID] = onMessage
	}
	if onReady != nil {
		hub.readies[subscriptionID] = onReady
		if hub.ready {
			go onReady()
		}
	}
	m.updatesMu.Unlock()

	<-ctx.Done()
	m.removeUpdateSubscription(id, hub, subscriptionID)
	return ctx.Err()
}

func (m *Manager) removeUpdateSubscription(id string, hub *updateHub, subscriptionID uint64) {
	m.updatesMu.Lock()
	defer m.updatesMu.Unlock()
	if m.updates[id] != hub {
		return
	}
	delete(hub.reactions, subscriptionID)
	delete(hub.messages, subscriptionID)
	delete(hub.readies, subscriptionID)
	if len(hub.reactions) == 0 && len(hub.messages) == 0 {
		delete(m.updates, id)
		hub.cancel()
	}
}

func (m *Manager) runUpdateHub(ctx context.Context, accountID string, hub *updateHub) {
	defer close(hub.done)
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		if err := m.runUpdateConnection(ctx, accountID, hub); err != nil && ctx.Err() == nil {
			delay := time.Duration(1<<min(attempt, 5)) * time.Second
			applog.Error("telegram", "update_listener_retry_scheduled", "account_id", accountID, "attempt", attempt+1, "retry_after", delay.String(), "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		return
	}
}

func (m *Manager) runUpdateConnection(ctx context.Context, accountID string, hub *updateHub) error {
	store := m.accountStore(accountID)
	dispatcher := tg.NewUpdateDispatcher()
	var client *gotd.Client
	dispatchReaction := func(updateCtx context.Context, entities tg.Entities, raw tg.MessageClass, updateType string) {
		m.dispatchEditedReaction(updateCtx, accountID, updateType, entities, raw, client, func(_ context.Context, event ReactionEvent) { m.dispatchReaction(accountID, event) })
	}
	dispatcher.OnEditMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateEditMessage) error {
		dispatchReaction(updateCtx, entities, update.Message, "edit_message")
		return nil
	})
	dispatcher.OnEditChannelMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateEditChannelMessage) error {
		dispatchReaction(updateCtx, entities, update.Message, "edit_channel_message")
		return nil
	})
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
		m.dispatchReaction(accountID, event)
		return nil
	})
	dispatchMessage := func(updateCtx context.Context, entities tg.Entities, raw tg.MessageClass) {
		message, ok := raw.(*tg.Message)
		if !ok {
			return
		}
		input, err := messagePeer.EntitiesFromUpdate(entities).ExtractPeer(message.PeerID)
		if err != nil {
			applog.Info("chat_download", "new_message_peer_extract_failed", "account_id", accountID, "message_id", message.ID, "error", err.Error())
			return
		}
		name := "会话"
		manager := peers.Options{Storage: storage.NewPeers(store)}.Build(client.API())
		if peer, err := manager.ResolvePeer(updateCtx, message.PeerID); err == nil {
			input, name = peer.InputPeer(), peer.VisibleName()
		}
		dialogID, _ := inputPeerInfo(input)
		m.dispatchMessage(accountID, NewMessageEvent{AccountID: accountID, DialogID: dialogID, DialogName: name, MessageID: message.ID, InputPeer: input})
	}
	dispatcher.OnNewMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
		dispatchMessage(updateCtx, entities, update.Message)
		return nil
	})
	dispatcher.OnNewChannelMessage(func(updateCtx context.Context, entities tg.Entities, update *tg.UpdateNewChannelMessage) error {
		dispatchMessage(updateCtx, entities, update.Message)
		return nil
	})

	client, err := upstreamClient.New(ctx, upstreamClient.Options{KV: store, Proxy: m.proxyURL(), UpdateHandler: dispatcher}, false)
	if err != nil {
		return fmt.Errorf("创建 Telegram 更新监听连接: %w", err)
	}
	applog.Info("telegram", "update_listener_connected", "account_id", accountID)
	return client.Run(ctx, func(runCtx context.Context) error {
		if _, err := client.Self(runCtx); err != nil {
			return err
		}
		m.markUpdateHubReady(accountID, hub)
		<-runCtx.Done()
		return nil
	})
}

func (m *Manager) markUpdateHubReady(accountID string, hub *updateHub) {
	m.updatesMu.Lock()
	if m.updates[accountID] != hub {
		m.updatesMu.Unlock()
		return
	}
	hub.ready = true
	callbacks := make([]func(), 0, len(hub.readies))
	for _, callback := range hub.readies {
		callbacks = append(callbacks, callback)
	}
	m.updatesMu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

func (m *Manager) dispatchReaction(accountID string, event ReactionEvent) {
	m.updatesMu.Lock()
	hub := m.updates[accountID]
	callbacks := make([]func(context.Context, ReactionEvent), 0)
	if hub != nil {
		for _, callback := range hub.reactions {
			callbacks = append(callbacks, callback)
		}
	}
	m.updatesMu.Unlock()
	for _, callback := range callbacks {
		callback(context.Background(), event)
	}
}

func (m *Manager) dispatchMessage(accountID string, event NewMessageEvent) {
	m.updatesMu.Lock()
	hub := m.updates[accountID]
	callbacks := make([]func(context.Context, NewMessageEvent), 0)
	if hub != nil {
		for _, callback := range hub.messages {
			callbacks = append(callbacks, callback)
		}
	}
	m.updatesMu.Unlock()
	for _, callback := range callbacks {
		callback(context.Background(), event)
	}
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
	dispatchReactionEvent(onEvent, event)
}

// reactionEvent turns a Telegram peer into a direct-download event. A public
// t.me URL is optional metadata only; the InputPeer is authoritative and works
// for private dialogs as well.
func (m *Manager) reactionEvent(ctx context.Context, accountID string, entities tg.Entities, rawPeer tg.PeerClass, messageID int, emojis []string, client *gotd.Client) (ReactionEvent, bool) {
	inputPeer, err := messagePeer.EntitiesFromUpdate(entities).ExtractPeer(rawPeer)
	if err != nil {
		peerType, peerID := peerIdentity(rawPeer)
		applog.Error("reaction", "peer_extract_failed", "account_id", accountID, "peer_type", peerType, "dialog_id", peerID, "message_id", messageID, "error", err.Error())
		return ReactionEvent{}, false
	}
	dialogID, dialogName := inputPeerInfo(inputPeer)
	sourceURL := reactionFallbackURL(inputPeer, accountID, messageID)
	manager := peers.Options{Storage: storage.NewPeers(m.accountStore(accountID))}.Build(client.API())
	peer, resolveErr := manager.ResolvePeer(ctx, rawPeer)
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

func dispatchReactionEvent(onEvent func(context.Context, ReactionEvent), event ReactionEvent) {
	// The consumer owns bounded buffering. Do not create one goroutine for each
	// update: a busy group plus a slow database write would otherwise grow memory
	// without limit.
	onEvent(context.Background(), event)
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

func inputPeerInfo(input tg.InputPeerClass) (int64, string) {
	switch peer := input.(type) {
	case *tg.InputPeerSelf:
		return 0, "收藏消息"
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
	// Telegram authorization is safe to use from several independent client
	// connections. A shared read lease permits the configured job concurrency;
	// exclusive leases remain for removal and session validation.
	operation.RLock()
	defer operation.RUnlock()
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

// checkSessions is intentionally independent of downloads and other business
// operations. A successful Self request confirms that Telegram still accepts
// the saved authorization key for that individual account.
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
		// Do not open a second main MTProto session with the same authorization
		// key while a download or another account operation is active.
		if !operation.TryLock() {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, 45*time.Second)
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

func isAuthKeyUnregistered(err error) bool {
	return err != nil && (tgerr.Is(err, "AUTH_KEY_UNREGISTERED") || strings.Contains(err.Error(), "AUTH_KEY_UNREGISTERED"))
}

func shouldMarkSessionChecked(err error) bool { return err == nil }

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
func (m *Manager) setError(id string, err error) {
	applog.Error("telegram", "account_error", "account_id", id, "error", err.Error())
	m.setState(id, "error", err.Error())
}
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
