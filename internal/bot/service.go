package bot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/monitor"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

// Service is intentionally a small Telegram Bot API client. It is separate
// from the user-account MTProto client used by tdl downloads.
type Service struct {
	settings  *settings.Store
	downloads *download.Manager
	telegram  *telegram.Manager
	monitor   *monitor.Monitor
	client    *http.Client
	cursorPath string
	cursor     updateCursor

	mu      sync.Mutex
	// lifecycleMu serializes a status refresh with deletion. Without it a
	// refresh holding an old completed snapshot could overwrite a later
	// "task deleted" card or send a replacement notification.
	lifecycleMu sync.Mutex
	offset  int64
	token   string
	known   map[string]string
	tracked map[string]trackedRef
	// lifecycle keeps the one notification card for a job in each authorized
	// chat. The card is edited as the job advances instead of sending a new
	// Bot message for every state transition.
	lifecycle map[string]map[int64]messageRef
	deleted   map[string]struct{}
	ready     bool
	helpSent  bool
}

type updateCursor struct {
	TokenHash string `json:"tokenHash"`
	Offset    int64  `json:"offset"`
}

type messageRef struct {
	ChatID, MessageID int64
	Text              string
}
type trackedRef struct {
	JobID string
	messageRef
}

func New(store *settings.Store, downloads *download.Manager, telegram *telegram.Manager, monitor *monitor.Monitor, dataDir string) *Service {
	s := &Service{settings: store, downloads: downloads, telegram: telegram, monitor: monitor, client: &http.Client{Timeout: 12 * time.Second}, cursorPath: filepath.Join(dataDir, "bot-updates.json"), known: map[string]string{}, tracked: map[string]trackedRef{}, lifecycle: map[string]map[int64]messageRef{}, deleted: map[string]struct{}{}}
	if data, err := os.ReadFile(s.cursorPath); err == nil {
		if err := json.Unmarshal(data, &s.cursor); err != nil {
			applog.Error("bot", "update_cursor_read_failed", "error", err.Error())
		}
	}
	applog.Info("bot", "service_started")
	go s.loop()
	return s
}

func (s *Service) loop() {
	for {
		cfg := s.settings.Get().Bot
		if !cfg.Enabled || strings.TrimSpace(cfg.Token) == "" || len(cfg.ControlUserIDs) == 0 {
			time.Sleep(3 * time.Second)
			continue
		}
		commandsChanged := false
		s.mu.Lock()
		if s.token != cfg.Token {
			s.token = cfg.Token
			if s.cursor.TokenHash == tokenFingerprint(cfg.Token) {
				s.offset = s.cursor.Offset
			} else {
				s.offset = 0
			}
			s.known, s.tracked, s.lifecycle, s.deleted, s.ready, s.helpSent = map[string]string{}, map[string]trackedRef{}, map[string]map[int64]messageRef{}, map[string]struct{}{}, false, false
			commandsChanged = true
		}
		s.mu.Unlock()
		if commandsChanged {
			s.configureCommands(cfg.Token)
		}
		s.sendStartupHelp(cfg)
		updates, err := s.getUpdates(cfg.Token)
		if err == nil {
			for _, update := range updates {
				s.handle(cfg, update)
			}
		} else {
			applog.Error("bot", "updates_fetch_failed", "error", err.Error())
		}
		s.refresh(cfg)
		time.Sleep(2 * time.Second)
	}
}

// sendStartupHelp confirms to authorized users that Bot control is available
// after this application instance has completed its initialization.
func (s *Service) sendStartupHelp(cfg settings.Bot) {
	s.mu.Lock()
	if s.helpSent {
		s.mu.Unlock()
		return
	}
	s.helpSent = true
	s.mu.Unlock()
	for _, id := range cfg.ControlUserIDs {
		if _, err := s.send(cfg.Token, id, helpText(), nil); err != nil {
			applog.Error("bot", "startup_help_send_failed", "chat_id", id, "error", err.Error())
		}
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
	if len(result.Result) > 0 {
		s.mu.Lock()
		s.offset = result.Result[len(result.Result)-1].UpdateID + 1
		offset = s.offset
		s.mu.Unlock()
		s.saveCursor(token, offset)
	}
	return result.Result, nil
}

func (s *Service) saveCursor(token string, offset int64) {
	cursor := updateCursor{TokenHash: tokenFingerprint(token), Offset: offset}
	data, err := json.Marshal(cursor)
	if err != nil {
		return
	}
	if err := os.WriteFile(s.cursorPath, data, 0o600); err != nil {
		applog.Error("bot", "update_cursor_save_failed", "error", err.Error())
		return
	}
	s.mu.Lock()
	s.cursor = cursor
	s.mu.Unlock()
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}

func (s *Service) handle(cfg settings.Bot, update update) {
	if update.Message != nil {
		if !allowed(cfg, update.Message.From.ID) {
			applog.Info("bot", "update_rejected", "kind", "message", "user_id", update.Message.From.ID)
			return
		}
		applog.Info("bot", "command_received", "user_id", update.Message.From.ID)
		s.handleMessage(cfg, *update.Message)
		return
	}
	if update.CallbackQuery != nil && allowed(cfg, update.CallbackQuery.From.ID) {
		applog.Info("bot", "callback_received", "user_id", update.CallbackQuery.From.ID)
		s.handleCallback(cfg, *update.CallbackQuery)
	}
}

func (s *Service) handleMessage(cfg settings.Bot, msg message) {
	text := strings.TrimSpace(msg.Text)
	switch {
	case text == "/start" || text == "/help":
		s.send(cfg.Token, msg.Chat.ID, helpText(), nil)
	case text == "/list":
		s.sendTaskList(cfg, msg.Chat.ID, 1)
	case text == "/status":
		s.send(cfg.Token, msg.Chat.ID, s.statusText(), nil)
	case text == "/config":
		s.send(cfg.Token, msg.Chat.ID, configText(s.settings.Get()), nil)
	case text == "/restart":
		if _, err := s.send(cfg.Token, msg.Chat.ID, "🔄 正在重启所有服务…", nil); err != nil {
			applog.Error("bot", "restart_notice_send_failed", "error", err.Error())
			return
		}
		applog.Info("bot", "service_restart_requested", "user_id", msg.From.ID)
		go func() {
			time.Sleep(time.Second)
			os.Exit(0)
		}()
	case isTelegramLink(text):
		job, err := s.downloads.Enqueue(context.Background(), text)
		if err != nil {
			applog.Error("bot", "task_create_failed", "user_id", msg.From.ID, "error", err.Error())
			s.send(cfg.Token, msg.Chat.ID, "❌ 创建下载任务失败："+html.EscapeString(err.Error()), nil)
			return
		}
		applog.Info("bot", "task_created", "user_id", msg.From.ID, "job_id", job.ID, "item_count", job.TotalItems)
		s.mu.Lock()
		s.known[job.ID] = job.Status
		s.mu.Unlock()
		text := lifecycleText(job)
		messageID, err := s.send(cfg.Token, msg.Chat.ID, text, notificationKeyboard(job.ID))
		if err != nil {
			applog.Error("bot", "lifecycle_message_send_failed", "job_id", job.ID, "error", err.Error())
			return
		}
		s.rememberLifecycle(job.ID, messageRef{ChatID: msg.Chat.ID, MessageID: messageID, Text: text})
	default:
		s.send(cfg.Token, msg.Chat.ID, "发送 <code>/help</code> 查看可用命令。", nil)
	}
}

func (s *Service) handleCallback(cfg settings.Bot, query callbackQuery) {
	if query.Message == nil {
		return
	}
	if strings.HasPrefix(query.Data, "l:") {
		var page int
		if _, err := fmt.Sscanf(strings.TrimPrefix(query.Data, "l:"), "%d", &page); err != nil || page < 1 {
			s.answer(cfg.Token, query.ID, "页码无效")
			return
		}
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
		s.markLifecycleDeleted(cfg, refs)
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
	s.editTask(cfg, query.Message.Chat.ID, query.Message.MessageID, id, listPage)
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
	jobs, _, err := s.downloads.ListPage(1, 50)
	if err != nil {
		return
	}
	current := make(map[string]download.Job, len(jobs))
	for _, job := range jobs {
		current[job.ID] = job
		s.mu.Lock()
		previous, exists := s.known[job.ID]
		ready := s.ready
		s.known[job.ID] = job.Status
		s.mu.Unlock()
		s.lifecycleMu.Lock()
		if !s.isDeleted(job.ID) {
			if ready && !exists && cfg.Notifications.TaskCreated {
				s.sendLifecycle(cfg, job)
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
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()
	// A new task observed after Bot startup is a creation notification, including
	// tasks created through the Web UI.
	s.mu.Lock()
	tracked := make(map[string]trackedRef, len(s.tracked))
	for key, ref := range s.tracked {
		tracked[key] = ref
	}
	s.mu.Unlock()
	for key, ref := range tracked {
		id := ref.JobID
		job, ok := current[id]
		if !ok {
			if got, err := s.downloads.Get(id); err == nil {
				job, ok = got, true
			}
		}
		if !ok {
			s.untrack(key)
			continue
		}
		s.edit(cfg.Token, ref.ChatID, ref.MessageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, 0))
		if !active(job.Status) {
			s.untrack(key)
		}
	}
}

func (s *Service) track(id string, ref messageRef) {
	s.mu.Lock()
	s.tracked[id+":"+fmt.Sprint(ref.ChatID)] = trackedRef{JobID: id, messageRef: ref}
	s.mu.Unlock()
}
func (s *Service) untrack(key string) { s.mu.Lock(); delete(s.tracked, key); s.mu.Unlock() }

func (s *Service) rememberLifecycle(jobID string, ref messageRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycle[jobID] == nil {
		s.lifecycle[jobID] = make(map[int64]messageRef)
	}
	s.lifecycle[jobID][ref.ChatID] = ref
	if err := s.downloads.SaveBotLifecycleMessage(jobID, download.BotMessageRef{ChatID: ref.ChatID, MessageID: ref.MessageID, Text: ref.Text}); err != nil {
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
		s.lifecycle[jobID][ref.ChatID] = messageRef{ChatID: ref.ChatID, MessageID: ref.MessageID, Text: ref.Text}
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

func (s *Service) markLifecycleDeleted(cfg settings.Bot, refs []messageRef) {
	for _, ref := range refs {
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
	if len(refs) == 0 {
		s.sendLifecycle(cfg, job)
		return
	}
	text := lifecycleText(job)
	for _, ref := range refs {
		s.edit(cfg.Token, ref.ChatID, ref.MessageID, text, notificationKeyboard(job.ID))
		ref.Text = text
		s.rememberLifecycle(job.ID, ref)
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
		"queued": "下载任务已创建",
		"running": "下载进行中",
		"paused": "下载已暂停",
		"completed": "下载完成",
		"partial": "部分完成",
		"failed": "下载失败",
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
	case "completed", "cancelled":
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
		byID[fmt.Sprintf("%d:%d", p.DialogID, p.MessageID)] = p
	}
	lines := []string{fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), statusName(job.Status)), "<b>对话：</b>" + html.EscapeString(short(job.DialogName, 36)), fmt.Sprintf("<b>进度：</b>%d/%d 文件", job.CompletedItems, job.TotalItems)}
	for _, item := range job.Items {
		p := byID[fmt.Sprintf("%d:%d", item.DialogID, item.MessageID)]
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
	return "<b>TDL帮助</b>\n\n发送 Telegram 消息链接即可创建下载任务。\n\n<code>/help</code> 获取帮助信息\n<code>/list</code> 获取下载任务\n<code>/status</code> 获取当前状态\n<code>/config</code> 获取当前配置\n<code>/restart</code> 重启所有服务"
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
	return fmt.Sprintf("<b>当前配置</b>\n代理：%s\n下载：线程 %d · 同时任务 %d · 连接池 %d · 间隔 %dms\nBot：%s\n表情监听：%s（%s）\n临时命名模板：<code>%s</code>\n最终命名模板：<code>%s</code>", html.EscapeString(proxy), cfg.Download.Threads, cfg.Download.TaskLimit, cfg.Download.PoolSize, cfg.Download.DelayMS, botState, reactionState, html.EscapeString(strings.Join(cfg.Reaction.Emojis, " ")), html.EscapeString(short(cfg.Download.TempFilenameTemplate, 180)), html.EscapeString(short(cfg.Download.FinalFilenameTemplate, 180)))
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
	return status == "queued" || status == "running" || status == "paused"
}
func statusName(status string) string {
	return map[string]string{"queued": "排队中", "running": "下载中", "paused": "已暂停", "completed": "已完成", "partial": "部分完成", "failed": "失败", "cancelled": "已取消"}[status]
}
func statusIcon(status string) string {
	return map[string]string{"queued": "🕓", "running": "⬇️", "paused": "⏸", "completed": "✅", "partial": "⚠️", "failed": "❌", "cancelled": "■"}[status]
}
func short(v string, limit int) string {
	r := []rune(v)
	if len(r) <= limit {
		return v
	}
	return string(r[:limit-1]) + "…"
}
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
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
	req, err := http.NewRequest(http.MethodPost, "https://api.telegram.org/bot"+token+"/"+method, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// configureCommands enables Telegram's native slash-command suggestions and
// the command menu button shown at the leading edge of the chat input.
func (s *Service) configureCommands(token string) {
	var result apiResponse[bool]
	commands := []map[string]string{
		{"command": "help", "description": "获取帮助信息"},
		{"command": "list", "description": "获取下载任务"},
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
func (s *Service) edit(token string, chatID, messageID int64, text string, keyboard [][]button) {
	var result apiResponse[bool]
	payload := map[string]any{"chat_id": chatID, "message_id": messageID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true}
	if keyboard != nil {
		payload["reply_markup"] = map[string]any{"inline_keyboard": keyboard}
	}
	if err := s.call(token, "editMessageText", payload, &result); err != nil {
		applog.Error("bot", "message_edit_failed", "chat_id", chatID, "message_id", messageID, "error", err.Error())
		return
	}
	if !result.OK {
		applog.Error("bot", "message_edit_rejected", "chat_id", chatID, "message_id", messageID, "error", result.Description)
		return
	}
	if keyboard == nil {
		s.clearKeyboard(token, chatID, messageID)
	}
}

func (s *Service) clearKeyboard(token string, chatID, messageID int64) {
	var result apiResponse[bool]
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
