package httpapi

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vacks/tdl/internal/adapter/upstream"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/auth"
	"github.com/vacks/tdl/internal/bot"
	"github.com/vacks/tdl/internal/config"
	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/monitor"
	"github.com/vacks/tdl/internal/reaction"
	"github.com/vacks/tdl/internal/settings"
	telegramAccounts "github.com/vacks/tdl/internal/telegram"
)

//go:embed all:static
var staticFiles embed.FS

const sessionCookie = "tdl_session"

type Server struct {
	cfg       config.Config
	accounts  *auth.Store
	sessions  *auth.Sessions
	telegram  *telegramAccounts.Manager
	settings  *settings.Store
	downloads *download.Manager
	monitor   *monitor.Monitor
	bot       *bot.Service
	reactions *reaction.Service
	mux       *http.ServeMux
	sseSlots  chan struct{}
	// statusSlots bounds the sidebar's status streams separately from the
	// download progress ones. Each stream is one goroutine per open page, and
	// both limits exist to bound them - but a page that is only watching the
	// sidebar must not consume the budget that live download progress needs.
	statusSlots chan struct{}
	// shutdown is closed by Stop to end the long-lived event streams. They are
	// the one handler that outlives a request by design, so http.Server.Shutdown
	// - which waits for active handlers but does not cancel their contexts -
	// cannot end them on its own.
	shutdown   chan struct{}
	shutdownOn sync.Once
	loginMu    sync.Mutex
	logins     map[string]loginAttempt
}

type loginAttempt struct {
	failures int
	until    time.Time
	updated  time.Time
}

func New(cfg config.Config) (*Server, error) {
	accounts, initialized, err := auth.Open(cfg.DataDir, cfg.AdminUsername, cfg.InitialPassword)
	if err != nil {
		return nil, err
	}
	if initialized {
		applog.Info("auth", "administrator_initialized")
	}
	settingsStore, err := settings.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	telegram, err := telegramAccounts.Open(cfg.DataDir, settingsStore.ProxyURL)
	if err != nil {
		return nil, err
	}
	downloads, err := download.Open(cfg.DataDir, cfg.DownloadDir, cfg.DatabaseDSN, settingsStore, telegram)
	if err != nil {
		return nil, err
	}
	reactions := reaction.New(settingsStore, telegram, downloads)
	reactions.Start()
	systemMonitor := monitor.New(cfg.DownloadDir)
	s := &Server{cfg: cfg, accounts: accounts, sessions: auth.NewSessions(), telegram: telegram, settings: settingsStore, downloads: downloads, monitor: systemMonitor, bot: bot.New(settingsStore, downloads, telegram, systemMonitor, cfg.DataDir), reactions: reactions, mux: http.NewServeMux(), sseSlots: make(chan struct{}, 8), statusSlots: make(chan struct{}, 16), shutdown: make(chan struct{}), logins: make(map[string]loginAttempt)}
	s.routes()
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
	s.mux.ServeHTTP(recorder, r)
	// Domain services already log meaningful state changes. Suppress routine
	// polling and stale-session 401s so a dashboard tab cannot turn Docker's
	// stdout log into a high-frequency disk write stream.
	if strings.HasPrefix(r.URL.Path, "/api/") && r.Method != http.MethodGet && recorder.status < 400 {
		applog.Info("http", "request_completed", "method", r.Method, "path", r.URL.Path, "status", recorder.status, "duration_ms", time.Since(start).Milliseconds())
	}
}

// EndStreams ends the long-lived event streams.
//
// It is separate from Stop because of the order the process shuts down in, and
// the order is the whole point: the HTTP server is drained first and the
// services are stopped after it. http.Server.Shutdown waits for active handlers
// and does not cancel them, and an event stream ends only when its client goes
// away - so a stream still open when Shutdown is called makes it wait out its
// entire timeout. Ending the streams has to happen before the drain, which is
// why Stop cannot be the only place that does it. Idempotent; Stop calls it too,
// so a caller that never drains HTTP still ends them.
func (s *Server) EndStreams() {
	s.shutdownOn.Do(func() { close(s.shutdown) })
}

// Stop releases background connections and active downloads before process exit.
func (s *Server) Stop() {
	s.EndStreams()
	s.bot.Stop()
	s.reactions.Stop()
	s.downloads.Stop()
	s.telegram.Stop()
	s.monitor.Stop()
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (w *responseRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseRecorder) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
func (w *responseRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (s *Server) routes() {
	s.mux.HandleFunc("/api/health", s.health)
	s.mux.HandleFunc("/api/auth/session", s.session)
	s.mux.HandleFunc("/api/auth/login", s.login)
	s.mux.HandleFunc("/api/auth/logout", s.requireAuth(s.logout))
	s.mux.HandleFunc("/api/auth/password", s.requireAuth(s.changePassword))
	s.mux.HandleFunc("/api/dashboard", s.requireAuth(s.dashboard))
	s.mux.HandleFunc("/api/status", s.requireAuth(s.statusSnapshot))
	s.mux.HandleFunc("/api/status/events", s.requireAuth(s.statusSSE))
	s.mux.HandleFunc("/api/config", s.requireAuth(s.configAPI))
	s.mux.HandleFunc("/api/telegram/accounts", s.requireAuth(s.telegramAccounts))
	s.mux.HandleFunc("/api/telegram/accounts/", s.requireAuth(s.telegramAccount))
	s.mux.HandleFunc("/api/downloads", s.requireAuth(s.downloadsAPI))
	s.mux.HandleFunc("/api/downloads/progress", s.requireAuth(s.downloadProgressSSE))
	s.mux.HandleFunc("/api/downloads/", s.requireAuth(s.downloadControlAPI))
	s.mux.HandleFunc("/api/chat-downloads", s.requireAuth(s.chatDownloadsAPI))
	s.mux.HandleFunc("/api/chat-downloads/", s.requireAuth(s.chatDownloadControlAPI))
	s.mux.HandleFunc("/", s.app)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	if health := s.downloads.DatabaseHealth(); health.Status != "connected" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "degraded", "database": health})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	if !s.authenticated(r) {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "username": s.accounts.Username()})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	// The same origin check every other state-changing route has. Login is the
	// one route that does not need the existing cookie to succeed, so the
	// cookie's SameSite attribute does not protect it: a cross-site form can
	// post here with the attacker's credentials and leave the victim's browser
	// holding the attacker's session.
	if !s.validRequestOrigin(r) {
		s.reportOriginRejection(r)
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "请求来源无效"})
		return
	}
	client := s.loginClient(r)
	if retry := s.loginRetryAfter(client); retry > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds())+1))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "登录尝试过于频繁，请稍后再试"})
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil || !s.accounts.Verify(input.Username, input.Password) {
		s.recordLoginFailure(client)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "用户名或密码不正确"})
		return
	}
	token, err := s.sessions.Create()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "无法创建会话"})
		return
	}
	s.clearLoginFailures(client)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", HttpOnly: true, Secure: s.requestHTTPS(r), SameSite: http.SameSiteLaxMode, MaxAge: 86400})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.Delete(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: s.requestHTTPS(r), SameSite: http.SameSiteLaxMode})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) changePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var input struct {
		CurrentPassword string `json:"currentPassword"`
		NewPassword     string `json:"newPassword"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请求格式无效"})
		return
	}
	if err := s.accounts.ChangePassword(input.CurrentPassword, input.NewPassword); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.sessions.Clear()
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// dashboard reports only what can be answered without touching the permanent
// download history: connection states and the in-memory resource trend. The
// file counts it used to carry could not be, because a task's file count is
// unbounded, so on this three second poll they grew with the download history
// and with how much was downloading. Those figures are available on demand from
// the Bot's /status command, where a person asks for them once.
func (s *Server) dashboard(w http.ResponseWriter, _ *http.Request) {
	current, trend := s.monitor.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{"telegram": map[string]string{"status": s.telegram.Status()}, "database": s.downloads.DatabaseHealth(), "reactions": s.reactions.Health(), "upstreamVersion": upstream.Version, "system": map[string]any{"current": current, "trend": trend}})
}

func (s *Server) configAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"settings": s.settings.Get(), "downloadDir": s.cfg.DownloadDir, "upstreamVersion": upstream.Version, "aria2Enabled": false})
	case http.MethodPut:
		var values settings.Values
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&values); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请求格式无效"})
			return
		}
		if err := s.settings.Update(values); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"settings": s.settings.Get(), "downloadDir": s.cfg.DownloadDir, "upstreamVersion": upstream.Version, "aria2Enabled": false})
	default:
		methodNotAllowed(w, "GET, PUT")
	}
}

func (s *Server) telegramAccounts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		accounts, currentID := s.telegram.List()
		writeJSON(w, http.StatusOK, map[string]any{"accounts": accounts, "currentId": currentID})
	case http.MethodPost:
		account, err := s.telegram.StartQR()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, account)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) telegramAccount(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/telegram/accounts/"), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodDelete {
		active, err := s.downloads.ActiveAccountJobs(id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if active > 0 {
			writeJSON(w, http.StatusConflict, map[string]string{"error": fmt.Sprintf("该账户仍有 %d 个排队、下载中或暂停任务，请先处理这些任务", active)})
			return
		}
		stopCtx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		err = s.reactions.StopAccount(stopCtx, id)
		cancel()
		if err != nil {
			s.reactions.ResumeAccount(id)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "等待表情监听停止失败: " + err.Error()})
			return
		}
		if err := s.telegram.Delete(id); err != nil {
			accounts, _ := s.telegram.List()
			for _, account := range accounts {
				if account.ID == id {
					s.reactions.ResumeAccount(id)
					break
				}
			}
			telegramError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		methodNotAllowed(w, "POST, DELETE")
		return
	}
	switch parts[1] {
	case "select":
		if err := s.telegram.Select(id); err != nil {
			telegramError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case "password":
		var input struct {
			Password string `json:"password"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请求格式无效"})
			return
		}
		if err := s.telegram.SubmitPassword(id, input.Password); err != nil {
			telegramError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	default:
		http.NotFound(w, r)
	}
}

func telegramError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, telegramAccounts.ErrNotFound) {
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) downloadsAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
		if pageSize < 1 {
			pageSize = 10
		}
		status, err := download.NormalizeTaskStatusFilter(r.URL.Query().Get("status"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		revision := s.downloads.Revision()
		jobs, total, nextCursor, err := s.downloads.ListCursor(r.URL.Query().Get("cursor"), pageSize, status)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": total, "pageSize": pageSize, "nextCursor": nextCursor, "revision": revision})
	case http.MethodPost:
		var input struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil || strings.TrimSpace(input.URL) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请输入 Telegram 消息链接"})
			return
		}
		submission, err, settled := s.submitLink(r.Context(), strings.TrimSpace(input.URL))
		if !settled {
			// The link is being resolved in the background: the request is
			// accepted, and the task will appear through the same event stream
			// every other task does.
			writeJSON(w, http.StatusAccepted, map[string]any{"pending": true})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, submission)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

const (
	// submissionWaitBudget is how long a request waits for a link to resolve
	// before the answer becomes "accepted" and the work continues without it.
	// Creating a task is normally fast - one lookup and one insert - so this
	// window is only reached by the shape that has real work to do: a post whose
	// discussion thread is walked in full.
	submissionWaitBudget = 8 * time.Second
	// submissionWorkBudget bounds the handed-off resolution itself. It has to be
	// generous, because the comment walk is paced by the account's Telegram
	// budget, and it has to exist, because the goroutine outlives the request
	// that started it.
	submissionWorkBudget = 3 * time.Minute
)

// submitLink resolves and queues a message link, waiting for a bounded time.
//
// It exists because the whole resolution used to run inside the request: read
// the message, expand its album, walk every page of its comment thread - tens of
// seconds of paced Telegram requests on a popular post - and only then answer.
// Two things were wrong with that. The browser sat on a button that could not
// finish, with no timeout of its own; and the work was tied to the request
// context, so a client that gave up, or navigated away, cancelled the resolution
// and the task was never created - with nothing recorded that it had ever been
// asked for.
//
// Now the work runs under its own budget, detached from the request, and the
// handler stops waiting after submissionWaitBudget. A link that resolves quickly
// answers exactly as it did before; one that does not is reported as accepted
// and lands in the task list on its own.
//
// settled reports whether the outcome is known. When it is false the caller
// answers "accepted" and the resolution finishes in the background; a failure
// discovered after that point is logged rather than shown, because there is no
// longer a request to show it to.
func (s *Server) submitLink(requestCtx context.Context, url string) (submission download.Submission, err error, settled bool) {
	// WithoutCancel keeps the request's values and drops its cancellation, which
	// is the entire point: the work must survive the client going away.
	workCtx, cancelWork := context.WithTimeout(context.WithoutCancel(requestCtx), submissionWorkBudget)
	result := make(chan struct {
		submission download.Submission
		err        error
	}, 1)
	go func() {
		defer cancelWork()
		submission, err := s.downloads.Submit(workCtx, download.DownloadIntent{Source: download.SourceWeb, URL: url})
		result <- struct {
			submission download.Submission
			err        error
		}{submission, err}
	}()
	timer := time.NewTimer(submissionWaitBudget)
	defer timer.Stop()
	select {
	case outcome := <-result:
		return outcome.submission, outcome.err, true
	case <-timer.C:
		go func() {
			outcome := <-result
			if outcome.err != nil {
				applog.Error("http", "link_resolution_failed_after_accept", "url", url, "error", outcome.err.Error())
				return
			}
			applog.Info("http", "link_resolution_completed_after_accept", "url", url, "job_id", outcome.submission.Job.ID)
		}()
		return download.Submission{}, nil, false
	}
}

func (s *Server) chatDownloadsAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
		jobs, total, nextCursor, err := s.downloads.ListChats(r.URL.Query().Get("cursor"), pageSize)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": total, "pageSize": pageSize, "nextCursor": nextCursor, "revision": s.downloads.Revision()})
	case http.MethodPost:
		var input struct {
			URL       string `json:"url"`
			ListenNew bool   `json:"listenNew"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil || strings.TrimSpace(input.URL) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请输入 Telegram 频道或群组链接"})
			return
		}
		job, err := s.downloads.SubmitChat(r.Context(), download.ChatIntent{Source: download.SourceWeb, URL: strings.TrimSpace(input.URL), ListenNew: input.ListenNew})
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) chatDownloadControlAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/chat-downloads/"), "/"), "/")
	if parts[0] == "" || len(parts) > 2 {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		job, err := s.downloads.GetChat(parts[0])
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, job)
		return
	}
	var err error
	switch parts[1] {
	case "purge":
		if r.Method != http.MethodDelete {
			methodNotAllowed(w, http.MethodDelete)
			return
		}
		err = s.downloads.PurgeChat(parts[0])
	case "delete":
		if r.Method != http.MethodDelete {
			methodNotAllowed(w, http.MethodDelete)
			return
		}
		err = s.downloads.DeleteChat(parts[0])
	case "pause":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		err = s.downloads.PauseChat(parts[0])
	case "resume":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		err = s.downloads.ResumeChat(parts[0])
	case "retry":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		err = s.downloads.RetryChat(parts[0])
	case "cancel":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		err = s.downloads.CancelChat(parts[0])
	case "listen":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		var input struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请求格式无效"})
			return
		}
		err = s.downloads.SetChatListening(parts[0], input.Enabled)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// sseWriteTimeout bounds a single write to an event stream. The stream itself
// is unbounded by design; a write is not.
const sseWriteTimeout = 10 * time.Second

// downloadProgressSSE streams in-memory progress. Download chunks never write
// PostgreSQL; this endpoint is intentionally ephemeral and reconnect-safe.
func (s *Server) downloadProgressSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	select {
	case s.sseSlots <- struct{}{}:
		defer func() { <-s.sseSlots }()
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "已有过多实时连接，请关闭多余页面后重试"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	events, unsubscribe := s.downloads.SubscribeEvents()
	defer unsubscribe()
	// The stream is long-lived, so the server's ordinary write deadline is
	// replaced - but never by an unbounded one. Clearing it outright is what
	// made a client that stopped reading (a suspended phone, a tab whose socket
	// is gone without a reset) block forever inside Write on a full send
	// buffer: the handler never returned, so it never released its slot, and
	// the slot limit turned eight such clients into a permanent refusal for
	// everyone else. Bounding each write instead turns that into an error the
	// loop already knows how to leave on, and the browser reconnects.
	controller := http.NewResponseController(w)
	write := func(event *download.Event) bool {
		data, err := json.Marshal(map[string]any{"files": s.downloads.LiveProgress(), "revision": s.downloads.Revision(), "event": event})
		if err != nil {
			return false
		}
		if err := controller.SetWriteDeadline(time.Now().Add(sseWriteTimeout)); err != nil {
			return false
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return false
		}
		if _, err := w.Write(data); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !write(nil) {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdown:
			return
		case <-r.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			// One burst of state changes, one message. Publishing a multi-file
			// download moves each file through several states in quick
			// succession, and the browser answers every message with two list
			// requests; sending one message per transition made the download
			// itself the thing that kept the client querying. The payload
			// carries the revision read at send time, so a coalesced message
			// tells the client exactly as much as the messages it replaces.
			coalesced := coalesceEvents(events, event)
			if !write(&coalesced) {
				return
			}
		case <-ticker.C:
			// requireAuth ran once, at the handshake, and a stream can outlive
			// what authorized it: a session expires after a day, and changing
			// the password clears every session at once. Re-checked here so a
			// stream stops when an ordinary request would already be refused,
			// instead of continuing to push task state to a browser that is no
			// longer logged in.
			if !s.authenticated(r) {
				return
			}
			if !write(nil) {
				return
			}
		}
	}
}

// sseCoalesceWindow is how long a burst of task events is allowed to gather
// before one message carries them all. It is deliberately short: it has to be
// invisible to someone watching a progress bar, and the progress figures travel
// on the one-second ticker in any case, so this only delays the list refresh
// that follows a status change.
const sseCoalesceWindow = 300 * time.Millisecond

// coalesceEvents absorbs the rest of a burst and returns its last event. The
// events carry no data beyond the task they concern - every consumer re-reads
// canonical state from PostgreSQL - so the last one describes the same situation
// as the ones it replaces, and the revision in the payload is read when the
// message is finally written.
func coalesceEvents(events <-chan download.Event, first download.Event) download.Event {
	timer := time.NewTimer(sseCoalesceWindow)
	defer timer.Stop()
	latest := first
	for {
		select {
		case next, ok := <-events:
			if !ok {
				// The bus closed; the caller detects this on its next receive.
				return latest
			}
			latest = next
		case <-timer.C:
			return latest
		}
	}
}

// statusCheckInterval is how often an open status stream re-reads the two
// connection states it reports. Both reads are in-memory and mutex-guarded, so
// asking is free; writing is not, and that is why the answer is compared with
// the one already sent before anything goes out.
const statusCheckInterval = 2 * time.Second

// statusKeepAliveInterval is how long a status stream may stay silent before it
// writes a comment frame. The states it reports can hold steady for days, and a
// connection that carries nothing at all is one an intermediary closes without
// telling either end.
const statusKeepAliveInterval = 20 * time.Second

// connectionStatus is the connection states the Web sidebar shows.
//
// Every field is a plain string, which is what lets the stream below compare
// two of these directly: the comparison is the change detector, and "nothing
// changed" therefore costs nothing. That is also why it does not carry the
// database health's CheckedAt - the monitor rewrites that every five seconds
// whether or not the state moved, so including it would turn every check into a
// change and every change into a message. The page renders the state, and the
// state is what it is sent.
//
// The Telegram network state is the Bot service's, not the account manager's.
// The Bot owns the proxy-aware client that already talks to Telegram and probes
// with it on a timer; asking the same question a second way, so that the
// sidebar could answer it by itself, would be a second set of requests through
// the same metered proxy for the same answer.
type connectionStatus struct {
	TelegramNetwork struct {
		Status string `json:"status"`
	} `json:"telegramNetwork"`
	Telegram struct {
		Status string `json:"status"`
	} `json:"telegram"`
	Database struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	} `json:"database"`
}

func (s *Server) connectionStatus() connectionStatus {
	var status connectionStatus
	status.TelegramNetwork.Status = s.bot.TelegramNetwork()
	status.Telegram.Status = s.telegram.Status()
	health := s.downloads.DatabaseHealth()
	status.Database.Status = health.Status
	status.Database.Error = health.Error
	return status
}

// statusSnapshot answers the sidebar's read when a page appears. The stream
// below keeps that page correct from then on, but a page that is still loading
// cannot wait for a connection to be established before it shows anything, and
// a stream that fails to open must not leave the sidebar blank.
func (s *Server) statusSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, s.connectionStatus())
}

// statusSSE pushes the two connection states to an open page when - and only
// when - they change. The states live behind mutexes rather than in a channel,
// so there is nothing to subscribe to; the stream watches them instead, which
// keeps the cost of a change to the clients that can see it and adds no
// background loop to the process.
func (s *Server) statusSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	select {
	case s.statusSlots <- struct{}{}:
		defer func() { <-s.statusSlots }()
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "已有过多实时连接，请关闭多余页面后重试"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	// Each write is bounded, for the same reason the download stream's writes
	// are: a client that stopped reading must not block this handler forever
	// while it holds one of the slots above.
	controller := http.NewResponseController(w)
	write := func(frame []byte) bool {
		if err := controller.SetWriteDeadline(time.Now().Add(sseWriteTimeout)); err != nil {
			return false
		}
		if _, err := w.Write(frame); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	send := func(status connectionStatus) bool {
		data, err := json.Marshal(status)
		if err != nil {
			return false
		}
		return write(append(append([]byte("data: "), data...), '\n', '\n'))
	}
	// The states go out on connect, not only when they next change: a browser
	// that reconnects - after a restart, a dropped socket, a laptop waking up -
	// would otherwise have to fetch again to learn what it missed.
	last := s.connectionStatus()
	if !send(last) {
		return
	}
	check := time.NewTicker(statusCheckInterval)
	defer check.Stop()
	keepAlive := time.NewTicker(statusKeepAliveInterval)
	defer keepAlive.Stop()
	for {
		select {
		case <-s.shutdown:
			return
		case <-r.Context().Done():
			return
		case <-check.C:
			// Re-checked here for the reason the download stream re-checks it:
			// a stream is authorized once, at the handshake, and outlives what
			// authorized it - a session expires after a day, and changing the
			// password clears every session at once.
			if !s.authenticated(r) {
				return
			}
			current := s.connectionStatus()
			if current == last {
				continue
			}
			last = current
			if !send(current) {
				return
			}
		case <-keepAlive.C:
			if !write([]byte(": keepalive\n\n")) {
				return
			}
		}
	}
}

func (s *Server) downloadControlAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/downloads/"), "/"), "/")
	if parts[0] == "" || len(parts) > 2 {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		job, err := s.downloads.Get(parts[0])
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, job)
		return
	}
	if parts[1] == "items" {
		// One bounded page of a task's file records. The task read above carries
		// the first page and the cursor that continues it; a task whose file count
		// is not bounded is never returned whole.
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
		items, nextCursor, err := s.downloads.ItemsPage(parts[0], r.URL.Query().Get("cursor"), pageSize)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "nextCursor": nextCursor})
		return
	}
	if parts[1] == "delete" {
		if r.Method != http.MethodDelete {
			methodNotAllowed(w, http.MethodDelete)
			return
		}
		if err := s.downloads.Delete(parts[0]); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var err error
	switch parts[1] {
	case "pause":
		err = s.downloads.Pause(parts[0])
	case "resume":
		err = s.downloads.Resume(parts[0])
	case "retry":
		err = s.downloads.Retry(parts[0])
	case "cancel":
		err = s.downloads.Cancel(parts[0])
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) authenticated(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	return err == nil && s.sessions.Valid(c.Value)
}
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authenticated(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "请先登录"})
			return
		}
		if unsafeMethod(r.Method) && !s.validRequestOrigin(r) {
			s.reportOriginRejection(r)
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "请求来源校验失败"})
			return
		}
		next(w, r)
	}
}

func unsafeMethod(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
}

func (s *Server) validRequestOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// The SPA adds this non-simple header to every state-changing request;
		// a cross-site form cannot forge it without a successful CORS preflight.
		return r.Header.Get("X-Requested-With") == "TDL-Web"
	}
	return origin == s.requestedScheme(r)+"://"+r.Host
}

// requestedScheme is the scheme the same-origin check compares an Origin header
// against. It is derived rather than read: this service terminates no TLS
// itself, so behind the documented reverse proxy the public scheme is knowable
// only from a forwarding header the operator told the service to believe.
func (s *Server) requestedScheme(r *http.Request) string {
	if s.requestHTTPS(r) {
		return "https"
	}
	return "http"
}

// reportOriginRejection records the inputs a refused state-changing request was
// judged on. None of them are otherwise visible, and the usual cause is the
// deployment rather than the client: a reverse proxy that terminates TLS while
// X-Forwarded-Proto is not believed - the header is honoured only from an
// address listed in TDL_TRUSTED_PROXIES, whose default is empty - or one that
// rewrites Host. Either leaves every write answered with "请求来源无效",
// login included, and names nothing that would lead an operator to the header.
func (s *Server) reportOriginRejection(r *http.Request) {
	applog.Warn("http", "request_origin_rejected",
		"method", r.Method,
		"path", r.URL.Path,
		"origin", r.Header.Get("Origin"),
		"host", r.Host,
		"scheme", s.requestedScheme(r),
		"forwarded_proto", r.Header.Get("X-Forwarded-Proto"),
		"requested_with", r.Header.Get("X-Requested-With"),
		"peer", r.RemoteAddr,
	)
}

// requestHTTPS supports ordinary HTTPS and standard reverse-proxy forwarding.
// The header only affects cookie transport and same-origin validation, and it
// is honoured only when the request came from an address configured as a
// trusted proxy. Any direct client can otherwise assert it, which decides
// whether the session cookie is marked Secure and which scheme the same-origin
// check compares against. Client IP addresses are read from forwarding headers
// in exactly one place - the login attempt limit, see loginClient - and under
// the same rule: only from a peer this list names as a proxy.
func (s *Server) requestHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if !s.trustedProxy(r) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]), "https")
}

// trustedProxy reports whether the peer address is one whose forwarding headers
// the operator chose to believe. An empty list means none of them are.
func (s *Server) trustedProxy(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host == "" {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	return peer != nil && s.isTrustedProxyAddress(peer)
}

// isTrustedProxyAddress reports whether an address is one whose forwarded
// headers the operator chose to believe. An empty list means none of them are.
func (s *Server) isTrustedProxyAddress(peer net.IP) bool {
	if len(s.cfg.TrustedProxies) == 0 {
		return false
	}
	for _, entry := range s.cfg.TrustedProxies {
		if _, network, parseErr := net.ParseCIDR(entry); parseErr == nil {
			if network.Contains(peer) {
				return true
			}
			continue
		}
		if candidate := net.ParseIP(entry); candidate != nil && candidate.Equal(peer) {
			return true
		}
	}
	return false
}

// forwardedClient returns the client address a trusted proxy reported, or the
// empty string when there is nothing usable to report - the peer is not a
// trusted proxy, or the header holds no address. Any direct client can assert
// this header, so it is read only from a peer the operator named.
//
// The chain is read from the right. Each hop appends the address it received
// the connection from, so the rightmost entry that is not itself a trusted
// proxy is the closest hop the proxy can vouch for; everything to its left was
// supplied by whoever connected to that hop.
func (s *Server) forwardedClient(r *http.Request) string {
	if !s.trustedProxy(r) {
		return ""
	}
	parts := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for index := len(parts) - 1; index >= 0; index-- {
		candidate := strings.TrimSpace(parts[index])
		ip := net.ParseIP(candidate)
		if ip == nil || s.isTrustedProxyAddress(ip) {
			continue
		}
		return candidate
	}
	return ""
}

// loginClient is the key login attempts are counted against.
//
// Behind a reverse proxy every request arrives from the proxy, so counting by
// peer address put every client in one bucket: five wrong passwords from
// anywhere locked the operator out of their own instance, and the lock renews
// for as long as the attempts continue, so it could be held indefinitely by
// someone who never had credentials. A trusted proxy is the only peer whose
// forwarded address means anything, and it is used only then.
func (s *Server) loginClient(r *http.Request) string {
	if forwarded := s.forwardedClient(r); forwarded != "" {
		return forwarded
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}

func (s *Server) loginRetryAfter(client string) time.Duration {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	s.pruneLoginAttemptsLocked(time.Now())
	attempt := s.logins[client]
	if time.Now().Before(attempt.until) {
		return time.Until(attempt.until)
	}
	return 0
}

// loginLockoutThreshold is the failure count at which the throttle engages.
const loginLockoutThreshold = 5

func (s *Server) recordLoginFailure(client string) {
	s.loginMu.Lock()
	now := time.Now()
	s.pruneLoginAttemptsLocked(now)
	attempt := s.logins[client]
	attempt.failures++
	lockedOut := false
	if attempt.failures >= loginLockoutThreshold {
		delay := time.Second * time.Duration(1<<min(attempt.failures-loginLockoutThreshold, 6))
		if delay > time.Minute {
			delay = time.Minute
		}
		attempt.until = now.Add(delay)
		lockedOut = true
	}
	failures := attempt.failures
	attempt.updated = now
	s.logins[client] = attempt
	s.loginMu.Unlock()
	// A failed login was previously invisible: it was counted in memory and
	// nothing else, so a brute force attempt left no trace in the service log.
	// The password is never logged, and the client address is the same one the
	// throttle already keys on.
	if lockedOut {
		applog.Error("httpapi", "login_locked_out", "client", client, "failures", failures)
		return
	}
	applog.Info("httpapi", "login_failed", "client", client, "failures", failures)
}

func (s *Server) clearLoginFailures(client string) {
	s.loginMu.Lock()
	delete(s.logins, client)
	s.loginMu.Unlock()
}

func (s *Server) pruneLoginAttemptsLocked(now time.Time) {
	for client, attempt := range s.logins {
		if !attempt.updated.IsZero() && now.Sub(attempt.updated) > 15*time.Minute {
			delete(s.logins, client)
		}
	}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Several of these responses carry a credential - the Telegram Bot token is
	// part of the settings document - and none of them are safe to replay from a
	// cache. Without this a browser or an intermediary was free to store them
	// heuristically.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func methodNotAllowed(w http.ResponseWriter, methods string) {
	w.Header().Set("Allow", methods)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) app(w http.ResponseWriter, r *http.Request) {
	// Anything under /api/ that reached this handler is a path no route
	// claimed - a typo, a renamed endpoint, a probe. Answering it with the
	// single-page app means a 200 and an HTML body, and the client parses that
	// as a successful empty response: the caller gets {} and no error, so a
	// misspelled route is indistinguishable from an endpoint that returned
	// nothing. A JSON 404 is the only answer the caller can act on.
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "接口不存在"})
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, "GET, HEAD")
		return
	}
	file := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if file == "" || file == "." {
		file = "index.html"
	}
	static, _ := fs.Sub(staticFiles, "static")
	if _, err := fs.Stat(static, file); err != nil {
		file = "index.html"
	}
	if file == "index.html" {
		contents, err := fs.ReadFile(static, file)
		if err != nil {
			http.Error(w, "web application is unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, file, time.Time{}, bytes.NewReader(contents))
		return
	}
	request := r.Clone(r.Context())
	request.URL.Path = "/" + file
	http.FileServer(http.FS(static)).ServeHTTP(w, request)
}
