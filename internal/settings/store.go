package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"

	"github.com/iyear/tdl/pkg/tplfunc"
	"github.com/vacks/tdl/internal/applog"
)

type Download struct {
	Threads               int    `json:"threads"`
	TaskLimit             int    `json:"taskLimit"` // media files concurrently processed within one task
	ConcurrentJobs        int    `json:"concurrentJobs"`
	PoolSize              int    `json:"poolSize"`
	DelayMS               int    `json:"delayMs"`
	TempFilenameTemplate  string `json:"tempFilenameTemplate"`
	FinalFilenameTemplate string `json:"finalFilenameTemplate"`
	// Zero means unrestricted. Values are stored in MiB so the Web and Bot can
	// present them without exposing implementation-level byte counts.
	MinFileSizeMB int64    `json:"minFileSizeMB"`
	MaxFileSizeMB int64    `json:"maxFileSizeMB"`
	FileTypes     []string `json:"fileTypes"`
}

type BotNotifications struct {
	TaskCreated   bool `json:"taskCreated"`
	TaskCompleted bool `json:"taskCompleted"`
	TaskPartial   bool `json:"taskPartial"`
	TaskFailed    bool `json:"taskFailed"`
}

type Bot struct {
	Enabled        bool             `json:"enabled"`
	Token          string           `json:"token"`
	ControlUserID  int64            `json:"controlUserId,omitempty"` // legacy single-user setting
	ControlUserIDs []int64          `json:"controlUserIds"`
	Notifications  BotNotifications `json:"notifications"`
}

// Reaction controls automatic downloads triggered by reactions made by one of
// the application's own logged-in Telegram accounts.
type Reaction struct {
	Enabled bool     `json:"enabled"`
	Emojis  []string `json:"emojis"`
}

// Cleanup controls retention of terminal task history and reaction inbox
// records. Final downloaded files are never removed by this policy.
type Cleanup struct {
	RetentionDays int `json:"retentionDays"`
}

type Values struct {
	ProxyURL string   `json:"proxyUrl"`
	Download Download `json:"download"`
	Bot      Bot      `json:"bot"`
	Reaction Reaction `json:"reaction"`
	Cleanup  Cleanup  `json:"cleanup"`
}

func Defaults() Values {
	return Values{Download: Download{
		Threads: 4, TaskLimit: 2, ConcurrentJobs: 1, PoolSize: 8, DelayMS: 0,
		// This is evaluated by upstream tdl while it writes to the private
		// temporary directory. Keep it limited to variables/functions supported
		// by upstream tdl.
		TempFilenameTemplate: "{{ .DialogID }}_{{ .MessageID }}_{{ filenamify .FileName }}",
		// This is evaluated by the web application after download and therefore
		// can include our MessageText mapping.
		FinalFilenameTemplate: "{{ .DialogID }}_{{ .MessageID }}_{{ if .MessageText }}{{ .MessageText }}_{{ end }}{{ .FileName }}",
	}, Bot: Bot{Notifications: BotNotifications{TaskCreated: true, TaskCompleted: true, TaskPartial: true, TaskFailed: true}}, Reaction: Reaction{Emojis: []string{"👍"}}, Cleanup: Cleanup{RetentionDays: 90}}
}

type Store struct {
	path   string
	mu     sync.RWMutex
	values Values
}

func Open(dataDir string) (*Store, error) {
	s := &Store{path: filepath.Join(dataDir, "settings.json"), values: Defaults()}
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		data, err := json.MarshalIndent(s.values, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := writePrivateFile(s.path, data); err != nil {
			return nil, fmt.Errorf("create default settings: %w", err)
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read settings: %w", err)
	}
	if err := json.Unmarshal(data, &s.values); err != nil {
		return nil, fmt.Errorf("parse settings: %w", err)
	}
	normalizeDownload(&s.values.Download)
	normalizeBot(&s.values.Bot)
	normalizeReaction(&s.values.Reaction)
	normalizeCleanup(&s.values.Cleanup)
	if err := Validate(s.values); err != nil {
		return nil, fmt.Errorf("validate settings: %w", err)
	}
	return s, nil
}

func (s *Store) Get() Values      { s.mu.RLock(); defer s.mu.RUnlock(); return s.values }
func (s *Store) ProxyURL() string { s.mu.RLock(); defer s.mu.RUnlock(); return s.values.ProxyURL }
func (s *Store) Update(values Values) error {
	normalizeDownload(&values.Download)
	normalizeBot(&values.Bot)
	normalizeReaction(&values.Reaction)
	normalizeCleanup(&values.Cleanup)
	if err := Validate(values); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return err
	}
	if err = writePrivateFile(s.path, data); err != nil {
		return err
	}
	s.values = values
	applog.Info("settings", "configuration_saved", "proxy_enabled", values.ProxyURL != "", "bot_enabled", values.Bot.Enabled, "reaction_enabled", values.Reaction.Enabled, "reaction_emoji_count", len(values.Reaction.Emojis))
	return nil
}

func Validate(values Values) error {
	if proxy := strings.TrimSpace(values.ProxyURL); proxy != "" {
		u, err := url.Parse(proxy)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return errors.New("代理地址格式无效")
		}
		switch strings.ToLower(u.Scheme) {
		case "http", "https", "socks5", "socks5h":
		default:
			return errors.New("代理仅支持 http、https、socks5 或 socks5h")
		}
	}
	d := values.Download
	if d.Threads < 1 || d.Threads > 32 {
		return errors.New("单任务下载线程数需在 1 至 32 之间")
	}
	if d.TaskLimit < 1 || d.TaskLimit > 16 {
		return errors.New("单任务同时下载文件数需在 1 至 16 之间")
	}
	if d.ConcurrentJobs < 1 || d.ConcurrentJobs > 16 {
		return errors.New("同时执行下载任务数需在 1 至 16 之间")
	}
	if d.PoolSize < 0 || d.PoolSize > 64 {
		return errors.New("连接池大小需在 0 至 64 之间")
	}
	if d.DelayMS < 0 || d.DelayMS > 60000 {
		return errors.New("下载间隔需在 0 至 60000 毫秒之间")
	}
	if d.MinFileSizeMB < 0 || d.MinFileSizeMB > 1048576 || d.MaxFileSizeMB < 0 || d.MaxFileSizeMB > 1048576 {
		return errors.New("文件体积筛选需在 0 至 1048576 MB 之间")
	}
	if d.MinFileSizeMB > 0 && d.MaxFileSizeMB > 0 && d.MinFileSizeMB > d.MaxFileSizeMB {
		return errors.New("最小文件体积不能大于最大文件体积")
	}
	for _, kind := range d.FileTypes {
		switch kind {
		case "image", "video", "audio", "document":
		default:
			return errors.New("文件类型筛选仅支持图片、视频、音频或文档")
		}
	}
	if strings.TrimSpace(d.TempFilenameTemplate) == "" {
		return errors.New("临时文件命名模板不能为空")
	}
	if strings.TrimSpace(d.FinalFilenameTemplate) == "" {
		return errors.New("最终文件命名模板不能为空")
	}
	if err := validateTempTemplate(d.TempFilenameTemplate); err != nil {
		return err
	}
	if err := validateFinalTemplate(d.FinalFilenameTemplate); err != nil {
		return err
	}
	b := values.Bot
	if b.Enabled && strings.TrimSpace(b.Token) == "" {
		return errors.New("启用 Bot 控制前请填写 Bot Token")
	}
	if b.Enabled && len(b.ControlUserIDs) == 0 {
		return errors.New("启用 Bot 控制前请填写至少一个控制用户 ID")
	}
	if values.Reaction.Enabled && len(values.Reaction.Emojis) == 0 {
		return errors.New("启用表情触发下载前请至少填写一个表情")
	}
	for _, emoji := range values.Reaction.Emojis {
		if !validReactionEmoji(emoji) {
			return errors.New("触发表情仅支持普通 Unicode 表情")
		}
	}
	if values.Cleanup.RetentionDays < 1 || values.Cleanup.RetentionDays > 3650 {
		return errors.New("历史保留天数需在 1 至 3650 天之间")
	}
	return nil
}

func validateTempTemplate(pattern string) error {
	// Use the exact function registry linked into upstream tdl. This keeps the
	// Web validator in lockstep with its temporary-file renderer instead of
	// rejecting a valid upstream helper or accepting a nonexistent one.
	funcs := tplfunc.FuncMap(tplfunc.All...)
	tpl, err := template.New("temporary filename").Option("missingkey=error").Funcs(funcs).Parse(pattern)
	if err != nil {
		return fmt.Errorf("临时文件命名模板无效: %w", err)
	}
	var rendered bytes.Buffer
	data := map[string]any{"DialogID": int64(1), "MessageID": 1, "MessageDate": time.Now().Unix(), "FileName": "file.txt", "FileCaption": "caption", "FileSize": "1 KB", "DownloadDate": time.Now().Unix()}
	if err := tpl.Execute(&rendered, data); err != nil {
		return fmt.Errorf("临时文件命名模板无效: %w", err)
	}
	return nil
}

func validateFinalTemplate(pattern string) error {
	funcs := template.FuncMap{
		"formatDate": func(_ int64, layout string) string { return time.Now().Format(layout) },
	}
	tpl, err := template.New("final filename").Option("missingkey=error").Funcs(funcs).Parse(pattern)
	if err != nil {
		return fmt.Errorf("最终文件命名模板无效: %w", err)
	}
	var rendered bytes.Buffer
	data := map[string]any{"DialogID": int64(1), "DialogName": "dialog", "MessageID": 1, "GroupedID": int64(0), "MessageText": "message", "FileName": "file.txt", "FileExt": ".txt", "DownloadDate": time.Now().Unix()}
	if err := tpl.Execute(&rendered, data); err != nil {
		return fmt.Errorf("最终文件命名模板无效: %w", err)
	}
	return nil
}

func normalizeDownload(download *Download) {
	if download.ConcurrentJobs == 0 {
		download.ConcurrentJobs = Defaults().Download.ConcurrentJobs
	}
	seen := make(map[string]struct{}, len(download.FileTypes))
	types := make([]string, 0, len(download.FileTypes))
	for _, kind := range download.FileTypes {
		kind = strings.ToLower(strings.TrimSpace(kind))
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		types = append(types, kind)
	}
	download.FileTypes = types
}

func normalizeCleanup(cleanup *Cleanup) {
	if cleanup.RetentionDays == 0 {
		cleanup.RetentionDays = Defaults().Cleanup.RetentionDays
	}
}

func normalizeBot(bot *Bot) {
	if len(bot.ControlUserIDs) == 0 && bot.ControlUserID != 0 {
		bot.ControlUserIDs = []int64{bot.ControlUserID}
	}
	seen := make(map[int64]struct{}, len(bot.ControlUserIDs))
	ids := make([]int64, 0, len(bot.ControlUserIDs))
	for _, id := range bot.ControlUserIDs {
		if id > 0 {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
	}
	bot.ControlUserIDs = ids
	bot.ControlUserID = 0
}

func normalizeReaction(reaction *Reaction) {
	seen := make(map[string]struct{}, len(reaction.Emojis))
	emojis := make([]string, 0, len(reaction.Emojis))
	for _, emoji := range reaction.Emojis {
		emoji = CanonicalReactionEmoji(strings.TrimSpace(emoji))
		if emoji == "" {
			continue
		}
		if _, ok := seen[emoji]; ok {
			continue
		}
		seen[emoji] = struct{}{}
		emojis = append(emojis, emoji)
	}
	reaction.Emojis = emojis
}

// CanonicalReactionEmoji removes presentation-only variation selectors before
// comparing or storing a Telegram reaction. Telegram may deliver ❤ while a
// browser stores ❤️; both represent the same standard emoji reaction.
func CanonicalReactionEmoji(value string) string {
	return strings.NewReplacer("\ufe0e", "", "\ufe0f", "").Replace(value)
}

func validReactionEmoji(value string) bool {
	if strings.TrimSpace(value) != value || value == "" {
		return false
	}
	hasEmoji := false
	for _, r := range value {
		switch {
		case unicode.Is(unicode.So, r):
			hasEmoji = true
		case r == '\u200d' || r == '\ufe0f' || r == '\u20e3':
			if r == '\u20e3' {
				hasEmoji = true
			}
		case r >= '\U0001f3fb' && r <= '\U0001f3ff': // skin-tone modifier
		case (r >= '0' && r <= '9') || r == '#' || r == '*': // keycap emoji
		default:
			return false
		}
	}
	return hasEmoji
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
