package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vacks/tdl/internal/config"
)

// TestAdminHTTPFlow exercises the public health endpoint and the complete
// administrator session lifecycle without contacting Telegram. It catches
// regressions where a UI request is accepted without a session or an unsafe
// request skips the Origin/X-Requested-With protection.
func TestAdminHTTPFlow(t *testing.T) {
	s := newTestServer(t)

	if rr := perform(s, http.MethodGet, "/api/health", nil, nil); rr.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", rr.Code)
	}
	if rr := perform(s, http.MethodGet, "/api/config", nil, nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous config status = %d, want 401", rr.Code)
	}

	cookie := login(t, s, "admin", "test-password")
	headers := map[string]string{"Cookie": cookie.String()}
	if rr := perform(s, http.MethodGet, "/api/config", nil, headers); rr.Code != http.StatusOK {
		t.Fatalf("authenticated config status = %d, want 200", rr.Code)
	}

	values := s.settings.Get()
	values.Download.DelayMS = 321
	body, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if rr := perform(s, http.MethodPut, "/api/config", body, headers); rr.Code != http.StatusForbidden {
		t.Fatalf("unsafe request without origin guard status = %d, want 403", rr.Code)
	}
	headers["X-Requested-With"] = "TDL-Web"
	if rr := perform(s, http.MethodPut, "/api/config", body, headers); rr.Code != http.StatusOK {
		t.Fatalf("guarded config update status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if got := s.settings.Get().Download.DelayMS; got != 321 {
		t.Fatalf("saved delay = %d, want 321", got)
	}

	passwordBody, _ := json.Marshal(map[string]string{"currentPassword": "test-password", "newPassword": "next-password"})
	if rr := perform(s, http.MethodPost, "/api/auth/password", passwordBody, headers); rr.Code != http.StatusOK {
		t.Fatalf("password change status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if rr := perform(s, http.MethodGet, "/api/config", nil, headers); rr.Code != http.StatusUnauthorized {
		t.Fatalf("old session after password change status = %d, want 401", rr.Code)
	}
	if rr := perform(s, http.MethodPost, "/api/auth/login", mustJSON(t, map[string]string{"username": "admin", "password": "test-password"}), nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("old password login status = %d, want 401", rr.Code)
	}
	_ = login(t, s, "admin", "next-password")
}

func TestAdminLoginRateLimit(t *testing.T) {
	s := newTestServer(t)
	for attempt := 0; attempt < 5; attempt++ {
		rr := perform(s, http.MethodPost, "/api/auth/login", mustJSON(t, map[string]string{"username": "admin", "password": "wrong"}), nil)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("failed login %d status = %d, want 401", attempt+1, rr.Code)
		}
	}
	if rr := perform(s, http.MethodPost, "/api/auth/login", mustJSON(t, map[string]string{"username": "admin", "password": "test-password"}), nil); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited login status = %d, want 429", rr.Code)
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	root := t.TempDir()
	cfg := config.Config{
		DataDir:         filepath.Join(root, "data"),
		DownloadDir:     filepath.Join(root, "downloads"),
		DatabaseURL:     "sqlite://" + filepath.Join(root, "test.db"),
		AdminUsername:   "admin",
		InitialPassword: "test-password",
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.DownloadDir, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

func login(t *testing.T, s *Server, username, password string) *http.Cookie {
	t.Helper()
	rr := perform(s, http.MethodPost, "/api/auth/login", mustJSON(t, map[string]string{"username": username, "password": password}), nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("login status = %d, body=%s", rr.Code, rr.Body.String())
	}
	for _, cookie := range rr.Result().Cookies() {
		if cookie.Name == sessionCookie {
			return cookie
		}
	}
	t.Fatal("successful login did not set a session cookie")
	return nil
}

func perform(s *Server, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
