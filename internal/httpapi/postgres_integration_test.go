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

// TestPostgresHTTPAuthenticationAndTaskList exercises the production server
// stack against a disposable PostgreSQL database. It deliberately skips unless
// TDL_TEST_POSTGRES_URL is supplied, so a normal developer test can never
// open, modify, or count a real download-history database.
func TestPostgresHTTPAuthenticationAndTaskList(t *testing.T) {
	databaseURL := os.Getenv("TDL_TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	root := t.TempDir()
	server, err := New(config.Config{
		DataDir:         filepath.Join(root, "data"),
		DownloadDir:     filepath.Join(root, "downloads"),
		AdminUsername:   "admin",
		InitialPassword: "test-password",
		DatabaseDSN:     databaseURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)

	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "test-password"})
	login := httptest.NewRecorder()
	server.ServeHTTP(login, httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body)))
	if login.Code != http.StatusOK || len(login.Result().Cookies()) != 1 {
		t.Fatalf("login status=%d cookies=%d", login.Code, len(login.Result().Cookies()))
	}
	listRequest := httptest.NewRequest(http.MethodGet, "/api/downloads?pageSize=10", nil)
	listRequest.AddCookie(login.Result().Cookies()[0])
	list := httptest.NewRecorder()
	server.ServeHTTP(list, listRequest)
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var response struct {
		Total int `json:"total"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Total < 0 {
		t.Fatalf("invalid cached total %d", response.Total)
	}
}
