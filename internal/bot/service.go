package bot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/monitor"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
	"golang.org/x/net/proxy"
)

// Service is intentionally a small Telegram Bot API client. It is separate
// from the user-account MTProto client used by tdl downloads.
type Service struct {
	settings   *settings.Store
	downloads  *download.Manager
	telegram   *telegram.Manager
	monitor    *monitor.Monitor
	cursorPath string
	instanceID string
	cursor     updateCursor
	ctx        context.Context
	cancel     context.CancelFunc

	mu          sync.Mutex
	clientMu    sync.Mutex
	client      *http.Client
	clientProxy string
	// lifecycleMu serializes a status refresh with deletion. Without it a
	// refresh holding an old completed snapshot could overwrite a later
	// "task deleted" card or send a replacement notification.
	lifecycleMu sync.Mutex
	offset      int64
	token       string
	known       map[string]string
	tracked     map[string]trackedRef
	chatTracked map[string]chatTrackedRef
	// lifecycle keeps the one notification card for a job in each authorized
	// chat. The card is edited as the job advances instead of sending a new
	// Bot message for every state transition.
	lifecycle map[string]map[int64]messageRef
	deleted   map[string]struct{}
	dirty     map[string]struct{}
	// suppressedStatus consumes the state event emitted synchronously by a
	// successful Bot action. The callback itself has already rendered that
	// state, so the generic lifecycle refresher must not edit the same card a
	// second time.
	suppressedStatus map[string]string
	ready            bool
	helpSent         map[int64]bool
	helpRetry        map[int64]helpRetry
	lastLiveEdit     map[string]time.Time
	nextLiveEdit     time.Time
	retryUpdates     map[int64]int
	// downloadWake receives coalesced domain events. It shortens lifecycle
	// updates without making the Bot poll the task table more aggressively.
	downloadWake chan struct{}
}

type updateCursor struct {
	TokenHash  string `json:"tokenHash"`
	InstanceID string `json:"instanceId"`
	Offset     int64  `json:"offset"`
}

type messageRef struct {
	ChatID, MessageID int64
	Text              string
	TokenHash         string
}
type trackedRef struct {
	JobID string
	messageRef
}
type chatTrackedRef struct {
	JobID string
	Page  int
	messageRef
}
type helpRetry struct {
	next     time.Time
	attempts int
}

func New(store *settings.Store, downloads *download.Manager, telegram *telegram.Manager, monitor *monitor.Monitor, dataDir string) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{settings: store, downloads: downloads, telegram: telegram, monitor: monitor, cursorPath: filepath.Join(dataDir, "bot-updates.json"), instanceID: downloads.InstanceID(), ctx: ctx, cancel: cancel, known: map[string]string{}, tracked: map[string]trackedRef{}, chatTracked: map[string]chatTrackedRef{}, lifecycle: map[string]map[int64]messageRef{}, deleted: map[string]struct{}{}, dirty: map[string]struct{}{}, suppressedStatus: map[string]string{}, helpSent: map[int64]bool{}, helpRetry: map[int64]helpRetry{}, lastLiveEdit: map[string]time.Time{}, retryUpdates: map[int64]int{}, downloadWake: make(chan struct{}, 1)}
	if data, err := os.ReadFile(s.cursorPath); err == nil {
		if err := json.Unmarshal(data, &s.cursor); err != nil {
			applog.Error("bot", "update_cursor_read_failed", "error", err.Error())
		}
	}
	applog.Info("bot", "service_started")
	events, _ := downloads.SubscribeEvents()
	go s.watchDownloadEvents(events)
	go s.loop()
	return s
}

func (s *Service) watchDownloadEvents(events <-chan download.Event) {
	for event := range events {
		if s.downloads.IsChatChild(event.JobID) {
			continue
		}
		s.mu.Lock()
		if expected, suppressed := s.suppressedStatus[event.JobID]; suppressed && event.Status == expected {
			delete(s.suppressedStatus, event.JobID)
		} else {
			s.dirty[event.JobID] = struct{}{}
		}
		s.mu.Unlock()
		select {
		case s.downloadWake <- struct{}{}:
		default:
		}
	}
}

func (s *Service) loop() {
	for {
		if s.ctx.Err() != nil {
			return
		}
		cfg := s.settings.Get().Bot
		if !cfg.Enabled || strings.TrimSpace(cfg.Token) == "" || len(cfg.ControlUserIDs) == 0 {
			if !waitContext(s.ctx, 3*time.Second) {
				return
			}
			continue
		}
		commandsChanged := false
		lifecycleTokenChanged := false
		s.mu.Lock()
		if s.token != cfg.Token {
			// A restart starts with an empty in-memory token. Compare the stored
			// cursor too, so a changed token can never edit cards owned by an old
			// Bot identity after a stopped service is configured again.
			lifecycleTokenChanged = s.token != "" || (s.cursor.TokenHash != "" && s.cursor.TokenHash != tokenFingerprint(cfg.Token))
			s.token = cfg.Token
			if s.cursor.TokenHash == tokenFingerprint(cfg.Token) && s.cursor.InstanceID == s.instanceID {
				s.offset = s.cursor.Offset
			} else {
				s.offset = 0
			}
			s.known, s.tracked, s.lifecycle, s.deleted, s.dirty, s.suppressedStatus, s.ready, s.helpSent, s.helpRetry, s.lastLiveEdit, s.retryUpdates, s.nextLiveEdit = map[string]string{}, map[string]trackedRef{}, map[string]map[int64]messageRef{}, map[string]struct{}{}, map[string]struct{}{}, map[string]string{}, true, map[int64]bool{}, map[int64]helpRetry{}, map[string]time.Time{}, map[int64]int{}, time.Time{}
			s.chatTracked = map[string]chatTrackedRef{}
			commandsChanged = true
		}
		s.mu.Unlock()
		if commandsChanged {
			if lifecycleTokenChanged {
				if err := s.downloads.ClearBotLifecycleMessages(); err != nil {
					applog.Error("bot", "lifecycle_messages_clear_failed", "error", err.Error())
				}
			}
			s.configureCommands(cfg.Token)
		}
		s.sendStartupHelp(cfg)
		updates, err := s.getUpdates(cfg.Token)
		if err == nil {
			for _, update := range updates {
				if !s.handle(cfg, update) {
					// Keep this update (and every later one) visible to getUpdates.
					// Advancing beyond a transient submission failure would lose a
					// user command permanently.
					if !waitContext(s.ctx, s.retryDelay(update.UpdateID)) {
						return
					}
					break
				}
				s.clearRetry(update.UpdateID)
				// Do not accept another update until this one has been durably
				// acknowledged. Advancing only in memory would lose a successfully
				// handled command if the process crashes after a filesystem failure.
				if !s.advanceOffset(cfg.Token, update.UpdateID+1) {
					return
				}
			}
		} else {
			applog.Error("bot", "updates_fetch_failed", "error", err.Error())
		}
		s.refresh(cfg)
		select {
		case <-s.ctx.Done():
			return
		case <-s.downloadWake:
		case <-time.After(2 * time.Second):
		}
	}
}

// Stop cancels long polling and releases idle Bot API connections.
func (s *Service) Stop() {
	s.cancel()
	s.clientMu.Lock()
	if s.client != nil {
		if transport, ok := s.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	s.clientMu.Unlock()
}

// sendStartupHelp confirms to authorized users that Bot control is available
// after this application instance has completed its initialization.
func (s *Service) sendStartupHelp(cfg settings.Bot) {
	for _, id := range cfg.ControlUserIDs {
		s.mu.Lock()
		alreadySent := s.helpSent[id]
		retry := s.helpRetry[id]
		s.mu.Unlock()
		if alreadySent || time.Now().Before(retry.next) {
			continue
		}
		if _, err := s.send(cfg.Token, id, helpText(), nil); err != nil {
			applog.Error("bot", "startup_help_send_failed", "chat_id", id, "error", err.Error())
			if retry.attempts < 8 {
				retry.attempts++
			}
			delay := time.Second * time.Duration(1<<min(retry.attempts, 8))
			if delay > 5*time.Minute {
				delay = 5 * time.Minute
			}
			s.mu.Lock()
			s.helpRetry[id] = helpRetry{next: time.Now().Add(delay), attempts: retry.attempts}
			s.mu.Unlock()
			continue
		}
		s.mu.Lock()
		s.helpSent[id] = true
		delete(s.helpRetry, id)
		s.mu.Unlock()
	}
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	Result      T      `json:"result"`
}
type update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *message       `json:"message"`
	CallbackQuery *callbackQuery `json:"callback_query"`
}
type message struct {
	MessageID int64 `json:"message_id"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	From struct {
		ID int64 `json:"id"`
	} `json:"from"`
	Text string `json:"text"`
}
type callbackQuery struct {
	ID   string `json:"id"`
	From struct {
		ID int64 `json:"id"`
	} `json:"from"`
	Data    string   `json:"data"`
	Message *message `json:"message"`
}

func (s *Service) getUpdates(token string) ([]update, error) {
	s.mu.Lock()
	offset := s.offset
	s.mu.Unlock()
	var result apiResponse[[]update]
	if err := s.call(token, "getUpdates", map[string]any{"offset": offset, "timeout": 1, "allowed_updates": []string{"message", "callback_query"}}, &result); err != nil {
		return nil, err
	}
	if !result.OK {
		return nil, fmt.Errorf("Bot API: %s", result.Description)
	}
	return result.Result, nil
}

func (s *Service) advanceOffset(token string, offset int64) bool {
	s.mu.Lock()
	if offset <= s.offset {
		s.mu.Unlock()
		return true
	}
	s.mu.Unlock()
	cursor := updateCursor{TokenHash: tokenFingerprint(token), InstanceID: s.instanceID, Offset: offset}
	data, err := json.Marshal(cursor)
	if err != nil {
		applog.Error("bot", "update_cursor_encode_failed", "error", err.Error())
		return false
	}
	for attempt := 0; ; attempt++ {
		if err := writePrivateFile(s.cursorPath, data); err == nil {
			s.mu.Lock()
			if offset > s.offset {
				s.offset = offset
				s.cursor = cursor
			}
			s.mu.Unlock()
			return true
		} else {
			applog.Error("bot", "update_cursor_save_failed", "offset", offset, "attempt", attempt+1, "error", err.Error())
		}
		delay := time.Second * time.Duration(1<<min(attempt, 5))
		if !waitContext(s.ctx, delay) {
			return false
		}
	}
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}

func (s *Service) handle(cfg settings.Bot, update update) bool {
	if update.Message != nil {
		if !allowed(cfg, update.Message.From.ID) || !privateChat(*update.Message) {
			applog.Info("bot", "update_rejected", "kind", "message", "user_id", update.Message.From.ID)
			return true
		}
		applog.Info("bot", "command_received", "user_id", update.Message.From.ID)
		return s.handleMessage(cfg, *update.Message)
	}
	if update.CallbackQuery != nil && allowed(cfg, update.CallbackQuery.From.ID) && privateCallback(*update.CallbackQuery) {
		applog.Info("bot", "callback_received", "user_id", update.CallbackQuery.From.ID)
		s.handleCallback(cfg, *update.CallbackQuery)
	}
	return true
}

// Bot control is deliberately private-chat only. An authorized user can be a
// member of many groups, but task names and download state must never be sent
// into those groups merely because they invoked a command there.
func privateChat(message message) bool { return message.Chat.ID == message.From.ID }

// CallbackQuery.Message.From is the Bot that sent the card, not the person
// pressing it. Test private-ness against CallbackQuery.From instead.
func privateCallback(query callbackQuery) bool {
	return query.Message != nil && query.Message.Chat.ID == query.From.ID
}

func (s *Service) handleMessage(cfg settings.Bot, msg message) bool {
	text := normalizeCommand(strings.TrimSpace(msg.Text))
	switch {
	case text == "/start" || text == "/help":
		s.send(cfg.Token, msg.Chat.ID, helpText(), nil)
	case text == "/list":
		s.sendTaskList(cfg, msg.Chat.ID, 1)
	case text == "/chat":
		s.sendChatList(cfg, msg.Chat.ID, 1)
	case strings.HasPrefix(text, "/chat "):
		s.createChatTask(cfg, msg, strings.TrimSpace(strings.TrimPrefix(text, "/chat ")))
	case text == "/status":
		s.send(cfg.Token, msg.Chat.ID, s.statusText(), nil)
	case text == "/config":
		s.send(cfg.Token, msg.Chat.ID, configText(s.settings.Get()), nil)
	case text == "/restart":
		if _, err := s.send(cfg.Token, msg.Chat.ID, "🔄 正在重启所有服务…", nil); err != nil {
			applog.Error("bot", "restart_notice_send_failed", "error", err.Error())
			return !retryableSubmitError(err)
		}
		applog.Info("bot", "service_restart_requested", "user_id", msg.From.ID)
		go func() {
			time.Sleep(time.Second)
			if process, err := os.FindProcess(os.Getpid()); err == nil {
				_ = process.Signal(syscall.SIGTERM)
			}
		}()
	case isTelegramLink(text):
		ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
		submission, err := s.downloads.Submit(ctx, download.DownloadIntent{Source: download.SourceBot, URL: text})
		cancel()
		if err != nil {
			applog.Error("bot", "task_create_failed", "user_id", msg.From.ID, "error", err.Error())
			s.send(cfg.Token, msg.Chat.ID, "❌ 创建下载任务失败："+html.EscapeString(err.Error()), nil)
			return !retryableSubmitError(err)
		}
		job := submission.Job
		if submission.Duplicate {
			applog.Info("bot", "task_request_attached", "user_id", msg.From.ID, "job_id", job.ID)
		}
		applog.Info("bot", "task_created", "user_id", msg.From.ID, "job_id", job.ID, "item_count", job.TotalItems)
		s.mu.Lock()
		s.known[job.ID] = job.Status
		s.mu.Unlock()
		text := lifecycleText(job)
		messageID, err := s.send(cfg.Token, msg.Chat.ID, text, notificationKeyboard(job.ID))
		if err != nil {
			applog.Error("bot", "lifecycle_message_send_failed", "job_id", job.ID, "error", err.Error())
			return true
		}
		s.rememberLifecycle(job.ID, messageRef{ChatID: msg.Chat.ID, MessageID: messageID, Text: text})
	default:
		s.send(cfg.Token, msg.Chat.ID, "发送 <code>/help</code> 查看可用命令。", nil)
	}
	return true
}

func retryableSubmitError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "timeout") || strings.Contains(message, "temporar") || strings.Contains(message, "connection") || strings.Contains(message, "network") || strings.Contains(message, "flood_wait")
}

func (s *Service) retryDelay(updateID int64) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt := s.retryUpdates[updateID] + 1
	if attempt > 8 {
		attempt = 8
	}
	s.retryUpdates[updateID] = attempt
	delay := time.Second * time.Duration(1<<attempt)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func (s *Service) clearRetry(updateID int64) {
	s.mu.Lock()
	delete(s.retryUpdates, updateID)
	s.mu.Unlock()
}

func (s *Service) handleCallback(cfg settings.Bot, query callbackQuery) {
	if query.Message == nil {
		return
	}
	if strings.HasPrefix(query.Data, "c:") {
		s.handleChatCallback(cfg, query)
		return
	}
	if strings.HasPrefix(query.Data, "l:") {
		var page int
		if _, err := fmt.Sscanf(strings.TrimPrefix(query.Data, "l:"), "%d", &page); err != nil || page < 1 {
			s.answer(cfg.Token, query.ID, "页码无效")
			return
		}
		s.untrackMessage(query.Message.Chat.ID, query.Message.MessageID)
		s.editTaskList(cfg, query.Message.Chat.ID, query.Message.MessageID, page)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if strings.HasPrefix(query.Data, "v:") {
		parts := strings.Split(query.Data, ":")
		if len(parts) != 3 {
			return
		}
		var page int
		if _, err := fmt.Sscanf(parts[2], "%d", &page); err != nil || page < 1 {
			s.answer(cfg.Token, query.ID, "页码无效")
			return
		}
		s.editTask(cfg, query.Message.Chat.ID, query.Message.MessageID, parts[1], page)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	parts := strings.Split(query.Data, ":")
	if len(parts) < 3 || len(parts) > 4 || parts[0] != "t" {
		return
	}
	id, action := parts[1], parts[2]
	listPage := 0
	if len(parts) == 4 {
		if _, err := fmt.Sscanf(parts[3], "%d", &listPage); err != nil || listPage < 1 {
			return
		}
	}
	if id == "list" && action == "refresh" {
		s.sendTaskList(cfg, query.Message.Chat.ID, 1)
		s.answer(cfg.Token, query.ID, "已刷新")
		return
	}
	if action == "view" {
		s.editTask(cfg, query.Message.Chat.ID, query.Message.MessageID, id, listPage)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if action == "delete" {
		// Keep the task's references before Delete removes their database rows.
		// The lock also prevents a stale refresh from emitting a completion card
		// after this deletion has been confirmed.
		s.lifecycleMu.Lock()
		refs := s.lifecycleRefs(id)
		s.markDeleted(id)
		err := s.downloads.Delete(id)
		if err != nil {
			s.unmarkDeleted(id)
			s.lifecycleMu.Unlock()
			s.answer(cfg.Token, query.ID, err.Error())
			return
		}
		s.markLifecycleDeleted(cfg, refs, query.Message.Chat.ID, query.Message.MessageID)
		s.untrackJob(id)
		s.forgetLifecycle(id)
		s.lifecycleMu.Unlock()
		s.answer(cfg.Token, query.ID, "操作成功")
		s.edit(cfg.Token, query.Message.Chat.ID, query.Message.MessageID, deletedTaskText(query.Message.Text), deletedTaskKeyboard(listPage))
		return
	}
	var err error
	switch action {
	case "pause":
		err = s.downloads.Pause(id)
	case "resume":
		err = s.downloads.Resume(id)
	case "retry":
		err = s.downloads.Retry(id)
	case "cancel":
		err = s.downloads.Cancel(id)
	default:
		err = fmt.Errorf("未知操作")
	}
	if err != nil {
		s.answer(cfg.Token, query.ID, err.Error())
		return
	}
	s.answer(cfg.Token, query.ID, "操作成功")
	s.renderActionResult(cfg, *query.Message, id, listPage)
}

// renderActionResult is the sole post-action renderer. A callback already has
// the final durable status, so consuming its matching domain event prevents a
// second, visually confusing edit from the generic lifecycle refresh.
func (s *Service) renderActionResult(cfg settings.Bot, message message, jobID string, listPage int) {
	job, err := s.downloads.Get(jobID)
	if err != nil {
		s.edit(cfg.Token, message.Chat.ID, message.MessageID, "任务不存在或已删除。", nil)
		return
	}
	s.mu.Lock()
	s.known[job.ID] = job.Status
	delete(s.dirty, job.ID)
	s.suppressedStatus[job.ID] = job.Status
	s.mu.Unlock()
	if s.isLifecycleMessage(job.ID, message.Chat.ID, message.MessageID) {
		s.updateLifecycle(cfg, job)
		return
	}
	s.edit(cfg.Token, message.Chat.ID, message.MessageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, listPage))
	if active(job.Status) {
		s.track(job.ID, messageRef{ChatID: message.Chat.ID, MessageID: message.MessageID})
	} else {
		s.untrackMessage(message.Chat.ID, message.MessageID)
	}
}

func (s *Service) sendTaskList(cfg settings.Bot, chatID int64, page int) {
	text, buttons, err := s.taskList(page)
	if err != nil {
		s.send(cfg.Token, chatID, "读取任务列表失败。", nil)
		return
	}
	s.send(cfg.Token, chatID, text, buttons)
}

func (s *Service) editTaskList(cfg settings.Bot, chatID, messageID int64, page int) {
	text, buttons, err := s.taskList(page)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "读取任务列表失败。", nil)
		return
	}
	s.edit(cfg.Token, chatID, messageID, text, buttons)
}

func (s *Service) taskList(page int) (string, [][]button, error) {
	jobs, total, err := s.downloads.ListPage(page, 10)
	if err != nil {
		return "", nil, err
	}
	if len(jobs) == 0 {
		return "暂无下载任务。", nil, nil
	}
	totalPages := (total + 9) / 10
	if page > totalPages {
		page = totalPages
	}
	buttons := make([][]button, 0, len(jobs)+1)
	for _, job := range jobs {
		label := fmt.Sprintf("%s %s %d/%d文件", statusIcon(job.Status), short(job.DialogName, 18), job.CompletedItems, job.TotalItems)
		buttons = append(buttons, []button{{Text: label, CallbackData: fmt.Sprintf("v:%s:%d", job.ID, page)}})
	}
	navigation := make([]button, 0, 3)
	if page > 1 {
		navigation = append(navigation, button{Text: "‹ 上一页", CallbackData: fmt.Sprintf("l:%d", page-1)})
	}
	navigation = append(navigation, button{Text: fmt.Sprintf("第 %d/%d 页", page, totalPages), CallbackData: fmt.Sprintf("l:%d", page)})
	if page < totalPages {
		navigation = append(navigation, button{Text: "下一页 ›", CallbackData: fmt.Sprintf("l:%d", page+1)})
	}
	buttons = append(buttons, navigation)
	return fmt.Sprintf("<b>下载任务</b> · 共 %d 个 · 第 %d/%d 页", total, page, totalPages), buttons, nil
}

func (s *Service) createChatTask(cfg settings.Bot, msg message, rawURL string) {
	if !isTelegramLink(rawURL) {
		s.send(cfg.Token, msg.Chat.ID, "请输入有效的 Telegram 频道或群组链接。", nil)
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	job, err := s.downloads.SubmitChat(ctx, download.ChatIntent{Source: download.SourceBot, URL: rawURL})
	cancel()
	if err != nil {
		applog.Error("bot", "chat_task_create_failed", "user_id", msg.From.ID, "error", err.Error())
		s.send(cfg.Token, msg.Chat.ID, "❌ 创建会话下载失败："+html.EscapeString(err.Error()), nil)
		return
	}
	applog.Info("bot", "chat_task_created", "user_id", msg.From.ID, "chat_job_id", job.ID)
	messageID, err := s.send(cfg.Token, msg.Chat.ID, chatTaskText(job), chatTaskKeyboard(job, 1))
	if err == nil && active(job.Status) {
		s.trackChat(job.ID, 1, messageRef{ChatID: msg.Chat.ID, MessageID: messageID})
	}
}

func (s *Service) sendChatList(cfg settings.Bot, chatID int64, page int) {
	text, buttons, err := s.chatTaskList(page)
	if err != nil {
		s.send(cfg.Token, chatID, "读取会话下载列表失败。", nil)
		return
	}
	s.send(cfg.Token, chatID, text, buttons)
}

func (s *Service) editChatList(cfg settings.Bot, chatID, messageID int64, page int) {
	text, buttons, err := s.chatTaskList(page)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "读取会话下载列表失败。", nil)
		return
	}
	s.edit(cfg.Token, chatID, messageID, text, buttons)
}

// The Bot API callback payload is bounded, so the opaque SQLite cursor used
// by the Web is intentionally not exposed here. Bot lists have small pages;
// page-number access is translated through the manager's presentation helper.
func (s *Service) chatTaskList(page int) (string, [][]button, error) {
	if page < 1 {
		page = 1
	}
	jobs, total, err := s.downloads.ListChatsPage(page, 10)
	if err != nil {
		return "", nil, err
	}
	if len(jobs) == 0 {
		return "暂无会话下载任务。", nil, nil
	}
	totalPages := (total + 9) / 10
	if page > totalPages {
		page = totalPages
	}
	buttons := make([][]button, 0, len(jobs)+1)
	for _, job := range jobs {
		label := fmt.Sprintf("%s %s %d/%d媒体", statusIcon(job.Status), short(job.DialogName, 18), job.Completed, job.Discovered)
		buttons = append(buttons, []button{{Text: label, CallbackData: fmt.Sprintf("c:v:%s:%d", job.ID, page)}})
	}
	navigation := make([]button, 0, 3)
	if page > 1 {
		navigation = append(navigation, button{Text: "‹ 上一页", CallbackData: fmt.Sprintf("c:l:%d", page-1)})
	}
	navigation = append(navigation, button{Text: fmt.Sprintf("第 %d/%d 页", page, totalPages), CallbackData: fmt.Sprintf("c:l:%d", page)})
	if page < totalPages {
		navigation = append(navigation, button{Text: "下一页 ›", CallbackData: fmt.Sprintf("c:l:%d", page+1)})
	}
	buttons = append(buttons, navigation)
	return fmt.Sprintf("<b>会话下载</b> · 共 %d 个 · 第 %d/%d 页", total, page, totalPages), buttons, nil
}

func (s *Service) handleChatCallback(cfg settings.Bot, query callbackQuery) {
	if query.Message == nil {
		return
	}
	parts := strings.Split(query.Data, ":")
	if len(parts) < 3 {
		return
	}
	if parts[1] == "l" && len(parts) == 3 {
		var page int
		if _, err := fmt.Sscanf(parts[2], "%d", &page); err != nil || page < 1 {
			s.answer(cfg.Token, query.ID, "页码无效")
			return
		}
		s.untrackChatMessage(query.Message.Chat.ID, query.Message.MessageID)
		s.editChatList(cfg, query.Message.Chat.ID, query.Message.MessageID, page)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if parts[1] == "v" && len(parts) == 4 {
		var page int
		if _, err := fmt.Sscanf(parts[3], "%d", &page); err != nil || page < 1 {
			return
		}
		s.editChatTask(cfg, query.Message.Chat.ID, query.Message.MessageID, parts[2], page)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if parts[1] != "t" || len(parts) != 5 {
		return
	}
	id, action := parts[2], parts[3]
	var page int
	if _, err := fmt.Sscanf(parts[4], "%d", &page); err != nil || page < 1 {
		return
	}
	var err error
	switch action {
	case "pause":
		err = s.downloads.PauseChat(id)
	case "resume":
		err = s.downloads.ResumeChat(id)
	case "retry":
		err = s.downloads.RetryChat(id)
	case "cancel":
		err = s.downloads.CancelChat(id)
	case "delete":
		err = s.downloads.DeleteChat(id)
	default:
		return
	}
	if err != nil {
		s.answer(cfg.Token, query.ID, err.Error())
		return
	}
	s.answer(cfg.Token, query.ID, "操作成功")
	if action == "delete" {
		s.untrackChatMessage(query.Message.Chat.ID, query.Message.MessageID)
		s.edit(cfg.Token, query.Message.Chat.ID, query.Message.MessageID, "会话任务已删除。", [][]button{{{Text: "返回会话列表", CallbackData: "c:l:1"}}})
		return
	}
	s.editChatTask(cfg, query.Message.Chat.ID, query.Message.MessageID, id, page)
}

func (s *Service) editChatTask(cfg settings.Bot, chatID, messageID int64, id string, page int) {
	job, err := s.downloads.GetChat(id)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "会话任务不存在或已删除。", [][]button{{{Text: "返回会话列表", CallbackData: fmt.Sprintf("c:l:%d", page)}}})
		return
	}
	s.edit(cfg.Token, chatID, messageID, chatTaskText(job), chatTaskKeyboard(job, page))
	if active(job.Status) {
		s.trackChat(job.ID, page, messageRef{ChatID: chatID, MessageID: messageID})
	}
}

func chatTaskText(job download.ChatJob) string {
	lines := []string{fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), statusName(job.Status)), "<b>对话：</b>" + html.EscapeString(short(job.DialogName, 36)), fmt.Sprintf("<b>范围：</b>%s — %d", chatStartLabel(job.StartMessageID), job.UpperMessageID), fmt.Sprintf("<b>媒体：</b>%d/%d", job.Completed, job.Discovered)}
	if job.Failed > 0 {
		lines = append(lines, fmt.Sprintf("<b>失败：</b>%d", job.Failed))
	}
	if job.ListenNew {
		lines = append(lines, "<b>新媒体监听：</b>已开启")
	}
	if job.Error != "" {
		lines = append(lines, "<b>说明：</b>"+html.EscapeString(short(job.Error, 100)))
	}
	return strings.Join(lines, "\n")
}

func chatStartLabel(id int) string {
	if id == 0 {
		return "最早媒体"
	}
	return fmt.Sprint(id)
}

func chatTaskKeyboard(job download.ChatJob, page int) [][]button {
	buttons := make([][]button, 0, 3)
	if job.Status == "queued" || job.Status == "scanning" || job.Status == "downloading" || job.Status == "listening" {
		buttons = append(buttons, []button{{Text: "暂停", CallbackData: fmt.Sprintf("c:t:%s:pause:%d", job.ID, page)}, {Text: "取消", CallbackData: fmt.Sprintf("c:t:%s:cancel:%d", job.ID, page)}})
	}
	if job.Status == "paused" {
		buttons = append(buttons, []button{{Text: "恢复", CallbackData: fmt.Sprintf("c:t:%s:resume:%d", job.ID, page)}, {Text: "取消", CallbackData: fmt.Sprintf("c:t:%s:cancel:%d", job.ID, page)}})
	}
	if job.Status == "failed" || job.Status == "partial" || job.Status == "cancelled" || job.Failed > 0 {
		buttons = append(buttons, []button{{Text: "重新开始", CallbackData: fmt.Sprintf("c:t:%s:retry:%d", job.ID, page)}, {Text: "删除", CallbackData: fmt.Sprintf("c:t:%s:delete:%d", job.ID, page)}})
	}
	buttons = append(buttons, []button{{Text: "返回会话列表", CallbackData: fmt.Sprintf("c:l:%d", page)}})
	return buttons
}

func (s *Service) sendTask(cfg settings.Bot, chatID int64, id string) {
	job, err := s.downloads.Get(id)
	if err != nil {
		s.send(cfg.Token, chatID, "未找到该任务。", nil)
		return
	}
	messageID, err := s.send(cfg.Token, chatID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, 0))
	if err == nil && active(job.Status) {
		s.track(job.ID, messageRef{ChatID: chatID, MessageID: messageID})
	}
}
func (s *Service) editTask(cfg settings.Bot, chatID, messageID int64, id string, listPage int) {
	if id == "list" {
		s.sendTaskList(cfg, chatID, 1)
		return
	}
	job, err := s.downloads.Get(id)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "任务不存在或已删除。", nil)
		return
	}
	s.edit(cfg.Token, chatID, messageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, listPage))
	if active(job.Status) {
		s.track(job.ID, messageRef{ChatID: chatID, MessageID: messageID})
	}
}

func (s *Service) refresh(cfg settings.Bot) {
	ids, tracked := s.refreshIDs()
	s.mu.Lock()
	chatTracked := make(map[string]chatTrackedRef, len(s.chatTracked))
	for key, ref := range s.chatTracked {
		chatTracked[key] = ref
	}
	s.mu.Unlock()
	for _, id := range ids {
		job, err := s.downloads.Get(id)
		if err != nil {
			continue
		}
		s.mu.Lock()
		previous, exists := s.known[job.ID]
		ready := s.ready
		s.known[job.ID] = job.Status
		s.mu.Unlock()
		s.lifecycleMu.Lock()
		if !s.isDeleted(job.ID) {
			if ready && !exists {
				// Lifecycle cards survive restarts in SQLite. Reload them before
				// deciding whether a first post-restart event needs a new card.
				if len(s.lifecycleRefs(job.ID)) > 0 {
					s.updateLifecycle(cfg, job)
				} else if cfg.Notifications.TaskCreated || (terminal(job.Status) && shouldUpdateLifecycle(cfg, job.Status)) {
					s.sendLifecycle(cfg, job)
				}
			}
			if exists && previous != job.Status && shouldUpdateLifecycle(cfg, job.Status) {
				// A lifecycle card may have been created by the Web UI, by a Bot
				// link, or by a prior terminal notification. In all three cases the
				// same card is edited in place.
				s.updateLifecycle(cfg, job)
			}
		}
		s.lifecycleMu.Unlock()
	}
	// Only explicitly viewed task cards are refreshed on the cadence. This
	// avoids repeatedly reading the newest task page when there is no event.
	for key, ref := range tracked {
		job, err := s.downloads.Get(ref.JobID)
		if err != nil {
			s.untrack(key)
			continue
		}
		if s.editLive(cfg.Token, ref.ChatID, ref.MessageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, 0)) {
			s.untrack(key)
		}
		if !active(job.Status) {
			s.untrack(key)
		}
	}
	// A viewed chat card is refreshed at the same restrained cadence as an
	// ordinary task card. It is not a lifecycle notification, so it stops once
	// the history has settled and no new-media listener remains active.
	for key, ref := range chatTracked {
		job, err := s.downloads.GetChat(ref.JobID)
		if err != nil {
			s.untrackChat(key)
			continue
		}
		if s.editLive(cfg.Token, ref.ChatID, ref.MessageID, chatTaskText(job), chatTaskKeyboard(job, ref.Page)) {
			s.untrackChat(key)
			continue
		}
		if !active(job.Status) {
			s.untrackChat(key)
		}
	}
}

func (s *Service) refreshIDs() ([]string, map[string]trackedRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
	s.dirty = make(map[string]struct{})
	tracked := make(map[string]trackedRef, len(s.tracked))
	for key, ref := range s.tracked {
		tracked[key] = ref
	}
	return ids, tracked
}

func (s *Service) track(id string, ref messageRef) {
	s.mu.Lock()
	s.tracked[id+":"+fmt.Sprint(ref.ChatID)] = trackedRef{JobID: id, messageRef: ref}
	s.mu.Unlock()
}
func (s *Service) untrack(key string) { s.mu.Lock(); delete(s.tracked, key); s.mu.Unlock() }
func (s *Service) untrackJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, ref := range s.tracked {
		if ref.JobID == jobID {
			delete(s.tracked, key)
		}
	}
}
func (s *Service) untrackMessage(chatID, messageID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, ref := range s.tracked {
		if ref.ChatID == chatID && ref.MessageID == messageID {
			delete(s.tracked, key)
		}
	}
}

func (s *Service) trackChat(id string, page int, ref messageRef) {
	s.mu.Lock()
	s.chatTracked[id+":"+fmt.Sprint(ref.ChatID)] = chatTrackedRef{JobID: id, Page: page, messageRef: ref}
	s.mu.Unlock()
}
func (s *Service) untrackChat(key string) { s.mu.Lock(); delete(s.chatTracked, key); s.mu.Unlock() }
func (s *Service) untrackChatMessage(chatID, messageID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, ref := range s.chatTracked {
		if ref.ChatID == chatID && ref.MessageID == messageID {
			delete(s.chatTracked, key)
		}
	}
}

func (s *Service) rememberLifecycle(jobID string, ref messageRef) {
	if ref.TokenHash == "" {
		s.mu.Lock()
		ref.TokenHash = tokenFingerprint(s.token)
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycle[jobID] == nil {
		s.lifecycle[jobID] = make(map[int64]messageRef)
	}
	s.lifecycle[jobID][ref.ChatID] = ref
	if err := s.downloads.SaveBotLifecycleMessage(jobID, download.BotMessageRef{ChatID: ref.ChatID, MessageID: ref.MessageID, Text: ref.Text, TokenHash: ref.TokenHash}); err != nil {
		applog.Error("bot", "lifecycle_message_save_failed", "job_id", jobID, "chat_id", ref.ChatID, "error", err.Error())
	}
}

func (s *Service) lifecycleRefs(jobID string) []messageRef {
	persisted, err := s.downloads.BotLifecycleMessages(jobID)
	if err != nil {
		applog.Error("bot", "lifecycle_message_load_failed", "job_id", jobID, "error", err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycle[jobID] == nil {
		s.lifecycle[jobID] = make(map[int64]messageRef)
	}
	for _, ref := range persisted {
		s.lifecycle[jobID][ref.ChatID] = messageRef{ChatID: ref.ChatID, MessageID: ref.MessageID, Text: ref.Text, TokenHash: ref.TokenHash}
	}
	refs := s.lifecycle[jobID]
	result := make([]messageRef, 0, len(refs))
	for _, ref := range refs {
		result = append(result, ref)
	}
	return result
}

func (s *Service) forgetLifecycle(jobID string) {
	s.mu.Lock()
	delete(s.lifecycle, jobID)
	s.mu.Unlock()
}

func (s *Service) isLifecycleMessage(jobID string, chatID, messageID int64) bool {
	for _, ref := range s.lifecycleRefs(jobID) {
		if ref.ChatID == chatID && ref.MessageID == messageID {
			return true
		}
	}
	return false
}

func (s *Service) markLifecycleDeleted(cfg settings.Bot, refs []messageRef, skipChatID, skipMessageID int64) {
	tokenHash := tokenFingerprint(cfg.Token)
	for _, ref := range refs {
		if !allowed(cfg, ref.ChatID) || ref.TokenHash != tokenHash || (ref.ChatID == skipChatID && ref.MessageID == skipMessageID) {
			continue
		}
		s.edit(cfg.Token, ref.ChatID, ref.MessageID, deletedTaskText(ref.Text), nil)
	}
}

func (s *Service) markDeleted(jobID string) {
	s.mu.Lock()
	s.deleted[jobID] = struct{}{}
	s.mu.Unlock()
}

func (s *Service) unmarkDeleted(jobID string) {
	s.mu.Lock()
	delete(s.deleted, jobID)
	s.mu.Unlock()
}

func (s *Service) isDeleted(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.deleted[jobID]
	return exists
}

func (s *Service) sendLifecycle(cfg settings.Bot, job download.Job) {
	text := lifecycleText(job)
	for _, id := range cfg.ControlUserIDs {
		messageID, err := s.send(cfg.Token, id, text, notificationKeyboard(job.ID))
		if err != nil {
			applog.Error("bot", "lifecycle_message_send_failed", "job_id", job.ID, "chat_id", id, "error", err.Error())
			continue
		}
		s.rememberLifecycle(job.ID, messageRef{ChatID: id, MessageID: messageID, Text: text})
	}
}

func (s *Service) updateLifecycle(cfg settings.Bot, job download.Job) {
	refs := s.lifecycleRefs(job.ID)
	activeRefs := make([]messageRef, 0, len(refs))
	tokenHash := tokenFingerprint(cfg.Token)
	for _, ref := range refs {
		if ref.TokenHash == tokenHash && allowed(cfg, ref.ChatID) {
			activeRefs = append(activeRefs, ref)
			continue
		}
		// A removed controller must stop receiving task metadata immediately.
		s.forgetLifecycleMessage(job.ID, ref.ChatID)
	}
	refs = activeRefs
	if len(refs) == 0 {
		s.sendLifecycle(cfg, job)
		return
	}
	text := lifecycleText(job)
	for _, ref := range refs {
		if s.edit(cfg.Token, ref.ChatID, ref.MessageID, text, notificationKeyboard(job.ID)) {
			s.forgetLifecycleMessage(job.ID, ref.ChatID)
			continue
		}
		ref.Text = text
		s.rememberLifecycle(job.ID, ref)
	}
}

func (s *Service) forgetLifecycleMessage(jobID string, chatID int64) {
	s.mu.Lock()
	if refs := s.lifecycle[jobID]; refs != nil {
		delete(refs, chatID)
	}
	s.mu.Unlock()
	if err := s.downloads.RemoveBotLifecycleMessage(jobID, chatID); err != nil {
		applog.Error("bot", "lifecycle_message_remove_failed", "job_id", jobID, "chat_id", chatID, "error", err.Error())
	}
}

func shouldUpdateLifecycle(cfg settings.Bot, status string) bool {
	switch status {
	case "completed":
		return cfg.Notifications.TaskCompleted
	case "partial":
		return cfg.Notifications.TaskPartial
	case "failed":
		return cfg.Notifications.TaskFailed
	default:
		// There is no separate switch for queue/running/paused transitions;
		// keep the existing lifecycle card current for these intermediate states.
		return true
	}
}

func terminal(status string) bool {
	switch status {
	case "completed", "partial", "failed", "cancelled":
		return true
	default:
		return false
	}
}
func allowed(cfg settings.Bot, userID int64) bool {
	for _, id := range cfg.ControlUserIDs {
		if id == userID {
			return true
		}
	}
	return false
}

type button struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

func notificationKeyboard(jobID string) [][]button {
	return [][]button{{{Text: "查看详情", CallbackData: "t:" + jobID + ":view"}}}
}

func lifecycleText(job download.Job) string {
	lines := []string{
		fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), lifecycleTitle(job.Status)),
		"对话：" + html.EscapeString(job.DialogName),
		fmt.Sprintf("进度：%d/%d 文件", job.CompletedItems, job.TotalItems),
		"状态：" + statusName(job.Status),
	}
	if job.Error != "" {
		lines = append(lines, "说明："+html.EscapeString(short(job.Error, 180)))
	}
	return strings.Join(lines, "\n")
}

func lifecycleTitle(status string) string {
	return map[string]string{
		"queued":    "下载任务已创建",
		"running":   "下载进行中",
		"paused":    "下载已暂停",
		"completed": "下载完成",
		"partial":   "部分完成",
		"failed":    "下载失败",
		"cancelled": "下载已取消",
	}[status]
}

func deletedTaskText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "🗑 <b>下载任务（任务已删除）</b>"
	}
	lines := strings.Split(text, "\n")
	if strings.Contains(lines[0], "</b>") {
		lines[0] = strings.Replace(lines[0], "</b>", "（任务已删除）</b>", 1)
	} else {
		lines[0] += "（任务已删除）"
	}
	return strings.Join(lines, "\n")
}

func deletedTaskKeyboard(listPage int) [][]button {
	if listPage < 1 {
		return nil
	}
	return [][]button{{{Text: "‹ 返回任务列表", CallbackData: fmt.Sprintf("l:%d", listPage)}}}
}

func taskKeyboard(job download.Job, listPage int) [][]button {
	id := job.ID
	callback := func(action string) string {
		if listPage > 0 {
			return fmt.Sprintf("t:%s:%s:%d", id, action, listPage)
		}
		return "t:" + id + ":" + action
	}
	var rows [][]button
	switch job.Status {
	case "queued", "running":
		rows = append(rows, []button{{"⏸ 暂停", callback("pause")}, {"■ 取消", callback("cancel")}})
	case "paused":
		rows = append(rows, []button{{"▶ 继续", callback("resume")}, {"■ 取消", callback("cancel")}})
	case "failed", "partial":
		rows = append(rows, []button{{"↻ 重试", callback("retry")}, {"删除", callback("delete")}})
	case "cancelled":
		rows = append(rows, []button{{"↻ 重新开始", callback("retry")}, {"删除", callback("delete")}})
	case "completed":
		rows = append(rows, []button{{"删除", callback("delete")}})
	}
	rows = append(rows, []button{{"↻ 刷新", callback("view")}})
	if listPage > 0 {
		rows = append(rows, []button{{"‹ 返回任务列表", fmt.Sprintf("l:%d", listPage)}})
	}
	return rows
}
func taskText(job download.Job, progress []download.FileProgress) string {
	byID := map[string]download.FileProgress{}
	for _, p := range progress {
		byID[fmt.Sprintf("%s:%d", p.DialogKey, p.MessageID)] = p
	}
	lines := []string{fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), statusName(job.Status)), "<b>对话：</b>" + html.EscapeString(short(job.DialogName, 36)), fmt.Sprintf("<b>进度：</b>%d/%d 文件", job.CompletedItems, job.TotalItems)}
	for _, item := range job.Items {
		p := byID[fmt.Sprintf("%s:%d", item.DialogKey, item.MessageID)]
		percent := "—"
		speed := ""
		if item.Status == "completed" {
			percent = "100%"
		} else if p.Total > 0 {
			percent = fmt.Sprintf("%.0f%%", float64(p.Downloaded)*100/float64(p.Total))
			if p.SpeedBPS > 0 {
				speed = " · " + bytesLabel(int64(p.SpeedBPS)) + "/s"
			}
		}
		lines = append(lines, fmt.Sprintf("%s %s  %s%s", statusIcon(item.Status), html.EscapeString(short(item.OriginalName, 26)), percent, speed))
	}
	if job.Error != "" {
		lines = append(lines, "<b>说明：</b>"+html.EscapeString(short(job.Error, 100)))
	}
	return strings.Join(lines, "\n")
}
func helpText() string {
	return "<b>TDL帮助</b>\n\n发送 Telegram 消息链接即可创建下载任务。\n\n<code>/help</code> 获取帮助信息\n<code>/list</code> 获取下载任务\n<code>/chat</code> 获取会话下载；<code>/chat 链接</code> 创建会话下载\n<code>/status</code> 获取当前状态\n<code>/config</code> 获取当前配置\n<code>/restart</code> 重启所有服务"
}

func (s *Service) statusText() string {
	accountName := "未登录"
	if s.telegram != nil {
		accounts, currentID := s.telegram.List()
		for _, account := range accounts {
			if account.ID != currentID || account.State != "authorized" {
				continue
			}
			accountName = strings.TrimSpace(strings.TrimSpace(account.FirstName + " " + account.LastName))
			if accountName == "" {
				accountName = "Telegram 用户 " + fmt.Sprint(account.TelegramID)
			}
			if account.Username != "" {
				accountName += " (@" + account.Username + ")"
			}
			break
		}
	}
	var cpu, memory, receive, transmit float64
	if s.monitor != nil {
		sample, _ := s.monitor.Snapshot()
		cpu = sample.CPUPercent
		if sample.MemoryTotal > 0 {
			memory = float64(sample.MemoryUsed) * 100 / float64(sample.MemoryTotal)
		}
		receive, transmit = sample.ReceiveBPS, sample.TransmitBPS
	}
	return fmt.Sprintf("<b>当前状态</b>\n登录账号：%s\nCPU：%.1f%%\n内存：%.1f%%\n网络：↓ %s/s · ↑ %s/s", html.EscapeString(accountName), cpu, memory, bytesLabel(int64(receive)), bytesLabel(int64(transmit)))
}

func configText(cfg settings.Values) string {
	proxy := "直连"
	if cfg.ProxyURL != "" {
		proxy = safeProxyLabel(cfg.ProxyURL)
	}
	botState := "已关闭"
	if cfg.Bot.Enabled {
		botState = fmt.Sprintf("已启用（%d 位用户）", len(cfg.Bot.ControlUserIDs))
	}
	reactionState := "已关闭"
	if cfg.Reaction.Enabled {
		reactionState = "已启用"
	}
	return fmt.Sprintf("<b>当前配置</b>\n代理：%s\n下载：线程 %d · 单任务文件并发 %d · 任务并发 %d · 连接池 %d · 间隔 %dms\nBot：%s\n表情监听：%s（%s）\n临时命名模板：<code>%s</code>\n最终命名模板：<code>%s</code>", html.EscapeString(proxy), cfg.Download.Threads, cfg.Download.TaskLimit, cfg.Download.ConcurrentJobs, cfg.Download.PoolSize, cfg.Download.DelayMS, botState, reactionState, html.EscapeString(strings.Join(cfg.Reaction.Emojis, " ")), html.EscapeString(short(cfg.Download.TempFilenameTemplate, 180)), html.EscapeString(short(cfg.Download.FinalFilenameTemplate, 180)))
}

func safeProxyLabel(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "已配置"
	}
	return u.Scheme + "://" + u.Host
}
func isTelegramLink(v string) bool {
	u, err := url.Parse(v)
	return err == nil && (u.Host == "t.me" || strings.HasSuffix(u.Host, ".t.me"))
}
func active(status string) bool {
	return status == "queued" || status == "running" || status == "paused" || status == "scanning" || status == "downloading" || status == "listening"
}
func statusName(status string) string {
	return map[string]string{"queued": "排队中", "running": "下载中", "scanning": "索引中", "downloading": "下载中", "listening": "监听中", "paused": "已暂停", "completed": "已完成", "partial": "部分完成", "failed": "失败", "cancelled": "已取消"}[status]
}
func statusIcon(status string) string {
	return map[string]string{"queued": "🕓", "running": "⬇️", "scanning": "🔎", "downloading": "⬇️", "listening": "👂", "paused": "⏸", "completed": "✅", "partial": "⚠️", "failed": "❌", "cancelled": "■"}[status]
}
func short(v string, limit int) string {
	r := []rune(v)
	if len(r) <= limit {
		return v
	}
	return string(r[:limit-1]) + "…"
}
func bytesLabel(value int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	n := float64(value)
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", value)
	}
	return fmt.Sprintf("%.2f %s", n, units[i])
}

func (s *Service) call(token, method string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, "https://api.telegram.org/bot"+token+"/"+method, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Bot API %s returned HTTP %d", method, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

func (s *Service) httpClient() *http.Client {
	raw := strings.TrimSpace(s.settings.ProxyURL())
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if s.client != nil && s.clientProxy == raw {
		return s.client
	}
	if s.client != nil {
		if transport, ok := s.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Timeout: 12 * time.Second, Transport: transport}
	s.clientProxy = raw
	s.client = client
	if raw == "" {
		return client
	}
	u, err := url.Parse(raw)
	if err != nil {
		return client
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		transport.Proxy = http.ProxyURL(u)
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if u.User != nil {
			password, _ := u.User.Password()
			auth = &proxy.Auth{User: u.User.Username(), Password: password}
		}
		dialer, err := proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
		if err == nil {
			transport.Proxy = nil
			transport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
				return dialer.Dial(network, address)
			}
		}
	}
	return client
}

func normalizeCommand(text string) string {
	parts := strings.Fields(text)
	if len(parts) != 1 || !strings.HasPrefix(parts[0], "/") {
		return text
	}
	if at := strings.IndexByte(parts[0], '@'); at > 1 {
		return parts[0][:at]
	}
	return parts[0]
}

func (s *Service) editLive(token string, chatID, messageID int64, text string, keyboard [][]button) bool {
	key := fmt.Sprintf("%d:%d", chatID, messageID)
	now := time.Now()
	s.mu.Lock()
	if now.Sub(s.lastLiveEdit[key]) < 3*time.Second || now.Before(s.nextLiveEdit) {
		s.mu.Unlock()
		return false
	}
	s.lastLiveEdit[key] = now
	s.nextLiveEdit = now.Add(50 * time.Millisecond)
	s.mu.Unlock()
	return s.edit(token, chatID, messageID, text, keyboard)
}

// configureCommands enables Telegram's native slash-command suggestions and
// the command menu button shown at the leading edge of the chat input.
func (s *Service) configureCommands(token string) {
	var result apiResponse[bool]
	commands := []map[string]string{
		{"command": "help", "description": "获取帮助信息"},
		{"command": "list", "description": "获取下载任务"},
		{"command": "chat", "description": "获取或创建会话下载"},
		{"command": "status", "description": "获取当前状态"},
		{"command": "config", "description": "获取当前配置"},
		{"command": "restart", "description": "重启所有服务"},
	}
	if err := s.call(token, "setMyCommands", map[string]any{"commands": commands}, &result); err != nil {
		applog.Error("bot", "commands_configure_failed", "error", err.Error())
		return
	}
	if !result.OK {
		applog.Error("bot", "commands_configure_rejected", "error", result.Description)
		return
	}
	if err := s.call(token, "setChatMenuButton", map[string]any{"menu_button": map[string]string{"type": "commands"}}, &result); err != nil {
		applog.Error("bot", "command_menu_configure_failed", "error", err.Error())
		return
	}
	if !result.OK {
		applog.Error("bot", "command_menu_configure_rejected", "error", result.Description)
	}
}
func (s *Service) send(token string, chatID int64, text string, keyboard [][]button) (int64, error) {
	var result apiResponse[message]
	payload := map[string]any{"chat_id": chatID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true}
	if len(keyboard) > 0 {
		payload["reply_markup"] = map[string]any{"inline_keyboard": keyboard}
	}
	err := s.call(token, "sendMessage", payload, &result)
	if err != nil {
		return 0, err
	}
	if !result.OK {
		return 0, fmt.Errorf("%s", result.Description)
	}
	return result.Result.MessageID, nil
}

// edit returns true only when Telegram confirms that the message no longer exists.
func (s *Service) edit(token string, chatID, messageID int64, text string, keyboard [][]button) bool {
	// editMessageText returns a Message object for normal chat messages, not a
	// boolean. Keep the result opaque: only the API's OK/description fields are
	// relevant here and decoding it as bool makes every successful edit fail.
	var result apiResponse[json.RawMessage]
	payload := map[string]any{"chat_id": chatID, "message_id": messageID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true}
	if keyboard != nil {
		payload["reply_markup"] = map[string]any{"inline_keyboard": keyboard}
	}
	if err := s.call(token, "editMessageText", payload, &result); err != nil {
		applog.Error("bot", "message_edit_failed", "chat_id", chatID, "message_id", messageID, "error", err.Error())
		return false
	}
	if !result.OK {
		if isMissingMessage(result.Description) {
			return true
		}
		applog.Error("bot", "message_edit_rejected", "chat_id", chatID, "message_id", messageID, "error", result.Description)
		return false
	}
	if keyboard == nil {
		s.clearKeyboard(token, chatID, messageID)
	}
	return false
}

func isMissingMessage(description string) bool {
	v := strings.ToLower(description)
	return strings.Contains(v, "message to edit not found") || strings.Contains(v, "message_id_invalid")
}

func (s *Service) clearKeyboard(token string, chatID, messageID int64) {
	var result apiResponse[json.RawMessage]
	payload := map[string]any{
		"chat_id": chatID, "message_id": messageID,
		"reply_markup": map[string]any{"inline_keyboard": [][]button{}},
	}
	if err := s.call(token, "editMessageReplyMarkup", payload, &result); err != nil {
		applog.Error("bot", "keyboard_clear_failed", "chat_id", chatID, "message_id", messageID, "error", err.Error())
		return
	}
	if !result.OK {
		applog.Error("bot", "keyboard_clear_rejected", "chat_id", chatID, "message_id", messageID, "error", result.Description)
	}
}
func (s *Service) answer(token, id, text string) {
	var result apiResponse[bool]
	_ = s.call(token, "answerCallbackQuery", map[string]any{"callback_query_id": id, "text": short(text, 180)}, &result)
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
