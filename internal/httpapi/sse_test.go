package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// Login is the one state-changing route that succeeds without an existing
// cookie, so the cookie's SameSite attribute does not protect it: a cross-site
// form can post here with the attacker's credentials and leave the victim's
// browser holding the attacker's session. It carries the same origin check as
// every other route.
func TestLoginRefusesARequestFromAnotherOrigin(t *testing.T) {
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
	request := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body))
	request.Header.Set("Origin", "https://attacker.example")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin login status=%d, want %d", response.Code, http.StatusForbidden)
	}
	if len(response.Result().Cookies()) != 0 {
		t.Fatal("a cross-origin login set a session cookie")
	}
}

// Anything under /api/ that no route claimed must answer as an API, not as the
// single-page app. The app fallback returns 200 with an HTML body, and the
// client parses that as a successful empty response - so a misspelled endpoint
// looked exactly like one that returned nothing.
func TestUnknownAPIRouteAnswersWithJSON404(t *testing.T) {
	_, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/downloadz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown api route status=%d, want %d", response.StatusCode, http.StatusNotFound)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("unknown api route content type=%q, want JSON", contentType)
	}
}

// The login limit is counted per client, and behind a reverse proxy every
// request arrives from the proxy. Counting the peer put every client in one
// bucket, where five wrong passwords from anywhere locked the operator out of
// their own instance and kept re-locking it for as long as they continued. The
// forwarded chain is read from the right, because each hop appends the address
// it received the connection from - so the entries to the left of the last
// untrusted hop are supplied by whoever connected to it.
func TestLoginClientReadsTheForwardedChainOnlyFromATrustedProxy(t *testing.T) {
	trusted := &Server{cfg: config.Config{TrustedProxies: []string{"10.0.0.1", "192.168.0.0/16"}}}
	untrusted := &Server{}
	request := func(remoteAddr, forwarded string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
		r.RemoteAddr = remoteAddr
		if forwarded != "" {
			r.Header.Set("X-Forwarded-For", forwarded)
		}
		return r
	}
	cases := []struct {
		name   string
		server *Server
		req    *http.Request
		want   string
	}{
		{"direct client is its own key", untrusted, request("203.0.113.9:5555", "1.2.3.4"), "203.0.113.9"},
		{"forwarded from a trusted proxy", trusted, request("10.0.0.1:5555", "203.0.113.9"), "203.0.113.9"},
		{"the rightmost untrusted hop wins", trusted, request("10.0.0.1:5555", "9.9.9.9, 203.0.113.9"), "203.0.113.9"},
		{"a trusted hop in the chain is skipped", trusted, request("10.0.0.1:5555", "203.0.113.9, 192.168.1.7"), "203.0.113.9"},
		{"forwarded from an untrusted peer is ignored", untrusted, request("203.0.113.9:5555", "1.2.3.4"), "203.0.113.9"},
		{"an unparsable chain falls back to the peer", trusted, request("10.0.0.1:5555", "not-an-address"), "10.0.0.1"},
	}
	for _, testCase := range cases {
		if got := testCase.server.loginClient(testCase.req); got != testCase.want {
			t.Fatalf("%s: loginClient() = %q, want %q", testCase.name, got, testCase.want)
		}
	}
}

// A stream is authorized once, at the handshake, and then lives on. A session
// expires after a day, and changing the password clears every session at once -
// so without re-checking, a stream keeps pushing task state to a browser that
// would already be refused the same data on any ordinary request.
func TestEventStreamEndsWhenTheSessionIsCleared(t *testing.T) {
	server, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/downloads/progress")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("the stream produced no first frame: %v", err)
	}
	server.sessions.Clear()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, reader)
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream outlived the session it was authorized with")
	}
}

// The order the process actually shuts down in: the HTTP server is drained, and
// only then are the services stopped. http.Server.Shutdown waits for active
// handlers and does not cancel them, and an event stream ends only when its
// client goes away - so a stream still open when the drain starts makes it wait
// out its whole timeout, on every restart, for as long as a dashboard tab is
// open.
//
// This reproduces that order instead of calling Stop, because calling Stop
// directly is what let the first version of this test pass while the behaviour
// it names was not happening at all: Stop closes the streams, and Stop runs
// after the drain.
func TestDrainingHTTPServerDoesNotWaitForAnOpenEventStream(t *testing.T) {
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server}
	go func() { _ = httpServer.Serve(listener) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	baseURL := "http://" + listener.Addr().String()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "test-password"})
	login, err := http.NewRequest(http.MethodPost, baseURL+"/api/auth/login", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	login.Header.Set("X-Requested-With", "TDL-Web")
	loginResponse, err := client.Do(login)
	if err != nil {
		t.Fatal(err)
	}
	_ = loginResponse.Body.Close()
	if loginResponse.StatusCode != http.StatusOK {
		t.Fatalf("login status=%d", loginResponse.StatusCode)
	}
	response, err := client.Get(baseURL + "/api/downloads/progress")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("the stream produced no first frame: %v", err)
	}
	// The stream is open and this client will not close it. End them first,
	// exactly as the process does, then drain with the deadline it uses.
	server.EndStreams()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	started := time.Now()
	if err := httpServer.Shutdown(ctx); err != nil {
		t.Fatalf("draining returned %v after %s, so it waited for the stream rather than the stream ending first", err, time.Since(started))
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("draining took %s, which is the timeout rather than the handler ending", elapsed)
	}
}
