package httpapi

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
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
}

func New(cfg config.Config) (*Server, error) {
	accounts, err := auth.Open(cfg.DataDir, cfg.AdminUsername, cfg.InitialPassword)
	if err != nil {
		return nil, err
	}
	settingsStore, err := settings.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	telegram, err := telegramAccounts.Open(cfg.DataDir, settingsStore.ProxyURL)
	if err != nil {
		return nil, err
	}
	downloads, err := download.Open(cfg.DataDir, cfg.DownloadDir, settingsStore, telegram)
	if err != nil {
		return nil, err
	}
	reactions := reaction.New(settingsStore, telegram, downloads)
	reactions.Start()
	systemMonitor := monitor.New(cfg.DownloadDir)
	s := &Server{cfg: cfg, accounts: accounts, sessions: auth.NewSessions(), telegram: telegram, settings: settingsStore, downloads: downloads, monitor: systemMonitor, bot: bot.New(settingsStore, downloads, telegram, systemMonitor, cfg.DataDir), reactions: reactions, mux: http.NewServeMux()}
	s.routes()
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
	s.mux.ServeHTTP(recorder, r)
	if strings.HasPrefix(r.URL.Path, "/api/") {
		applog.Info("http", "request_completed", "method", r.Method, "path", r.URL.Path, "status", recorder.status, "duration_ms", time.Since(start).Milliseconds())
	}
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
	s.mux.HandleFunc("/api/config", s.requireAuth(s.configAPI))
	s.mux.HandleFunc("/api/telegram/accounts", s.requireAuth(s.telegramAccounts))
	s.mux.HandleFunc("/api/telegram/accounts/", s.requireAuth(s.telegramAccount))
	s.mux.HandleFunc("/api/downloads", s.requireAuth(s.downloadsAPI))
	s.mux.HandleFunc("/api/downloads/progress", s.requireAuth(s.downloadProgressSSE))
	s.mux.HandleFunc("/api/downloads/", s.requireAuth(s.downloadControlAPI))
	s.mux.HandleFunc("/", s.app)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
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
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil || !s.accounts.Verify(input.Username, input.Password) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "用户名或密码不正确"})
		return
	}
	token, err := s.sessions.Create()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "无法创建会话"})
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 86400})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.Delete(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
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
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) dashboard(w http.ResponseWriter, _ *http.Request) {
	active, completedItems, failedItems := s.downloads.Summary()
	current, trend := s.monitor.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{"telegram": map[string]string{"status": s.telegram.Status()}, "reactions": s.reactions.Health(), "downloads": map[string]int{"active": active, "completedItems": completedItems, "failedItems": failedItems}, "upstreamVersion": upstream.Version, "system": map[string]any{"current": current, "trend": trend}})
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
		if err := s.telegram.Delete(id); err != nil {
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
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
		if page < 1 {
			page = 1
		}
		if pageSize < 1 {
			pageSize = 10
		}
		revision := s.downloads.Revision()
		jobs, total, err := s.downloads.ListPage(page, pageSize)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "total": total, "page": page, "pageSize": pageSize, "revision": revision})
	case http.MethodPost:
		var input struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&input); err != nil || strings.TrimSpace(input.URL) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请输入 Telegram 消息链接"})
			return
		}
		submission, err := s.downloads.Submit(r.Context(), download.DownloadIntent{Source: download.SourceWeb, URL: strings.TrimSpace(input.URL)})
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, submission)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

// downloadProgressSSE streams in-memory progress. Download chunks never write
// SQLite; this endpoint is intentionally ephemeral and reconnect-safe.
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
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	events, unsubscribe := s.downloads.SubscribeEvents()
	defer unsubscribe()
	write := func(event *download.Event) bool {
		data, err := json.Marshal(map[string]any{"files": s.downloads.LiveProgress(), "revision": s.downloads.Revision(), "event": event})
		if err != nil {
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
	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-events:
			if !ok || !write(&event) {
				return
			}
		case <-ticker.C:
			if !write(nil) {
				return
			}
		}
	}
}

func (s *Server) downloadControlAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/downloads/"), "/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		http.NotFound(w, r)
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
		next(w, r)
	}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func methodNotAllowed(w http.ResponseWriter, methods string) {
	w.Header().Set("Allow", methods)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) app(w http.ResponseWriter, r *http.Request) {
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
