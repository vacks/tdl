package httpapi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/config"
)

// sseTestServer starts the production stack against the disposable database and
// returns a client that is already logged in.
func sseTestServer(t *testing.T) (*Server, *http.Client, string) {
	t.Helper()
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
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "test-password"})
	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/auth/login", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Requested-With", "TDL-Web")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("login status=%d", response.StatusCode)
	}
	return server, client, httpServer.URL
}

// An event stream ends only when its client goes away, and http.Server.Shutdown
// waits for active handlers without cancelling them. Unless stopping the server
// ends the stream itself, every restart waits out the whole shutdown budget
// while downloads keep running behind it - and an operator who restarted
// because something was wrong gets no shorter a window for it.
func TestEventStreamEndsWhenTheServerStops(t *testing.T) {
	server, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/downloads/progress")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status=%d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	// The handler writes one frame immediately, so this also proves the stream
	// was actually established rather than refused.
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("the stream produced no first frame: %v", err)
	}
	server.Stop()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, reader)
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the event stream outlived the server; Shutdown will wait out its whole budget on every restart")
	}
}

// Stopping twice must not panic. Shutdown paths are entered from a signal, a
// test cleanup, and a deferred call in the same process, and a panic there
// would replace a clean exit with a stack trace.
func TestStopIsIdempotent(t *testing.T) {
	server, _, _ := sseTestServer(t)
	server.Stop()
	server.Stop()
}
