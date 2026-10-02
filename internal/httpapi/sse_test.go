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

// The sidebar reads the two connection states once, when the page appears, and
// depends on this stream for every change after that. The stream therefore has
// to answer a connection with the states it holds, not with silence: a page
// whose read failed, or whose read raced the stream, has nothing else to show.
//
// It also has to end with the server, for the reason the download stream does -
// a stream still open when the drain starts makes every restart wait it out.
func TestStatusStreamSendsTheCurrentStatesOnConnectAndEndsWithTheServer(t *testing.T) {
	server, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/status/events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status=%d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	frame, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("the stream produced no first frame: %v", err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("the first frame was not terminated: %v", err)
	}
	if !strings.HasPrefix(frame, "data: ") {
		t.Fatalf("first frame = %q, want a data frame", frame)
	}
	var status struct {
		Telegram struct{ Status string } `json:"telegram"`
		Database struct{ Status string } `json:"database"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(frame), "data: ")), &status); err != nil {
		t.Fatalf("the first frame is not the status document: %v", err)
	}
	if status.Telegram.Status == "" || status.Database.Status == "" {
		t.Fatalf("the first frame carries empty states: %+v", status)
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
		t.Fatal("the status stream outlived the server; Shutdown will wait out its whole budget on every restart")
	}
}

// The database monitor rewrites the health's CheckedAt on its own every five
// seconds whether or not the state moved. A stream that treated that as a
// change would wake every open page with the same two states for as long as the
// process ran - the poll this stream exists to replace. Nothing else in these
// two states changes on its own, so a stream with nothing to report must stay
// silent, and its keep-alive must not arrive as a message either.
func TestStatusStreamStaysSilentWhileNothingChanges(t *testing.T) {
	server, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/status/events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	for line := 0; line < 2; line++ {
		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatalf("the stream produced no first frame: %v", err)
		}
	}
	frames := make(chan string, 16)
	go func() {
		defer close(frames)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if strings.TrimSpace(line) != "" {
				frames <- line
			}
		}
	}()
	// Wait for the monitor to publish a health check. The wait is what makes
	// this deterministic: the re-check has definitely happened by the time the
	// stream's next tick comes round, so the tick that follows it is the one a
	// change-triggered push would arrive on.
	seen := server.downloads.DatabaseHealth().CheckedAt
	deadline := time.Now().Add(30 * time.Second)
	for {
		if current := server.downloads.DatabaseHealth().CheckedAt; current != seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the database monitor never re-checked the connection")
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(statusCheckInterval + time.Second)
	select {
	case line, ok := <-frames:
		if !ok {
			t.Fatal("the stream ended while the server was running")
		}
		t.Fatalf("the stream sent %q with nothing to report", line)
	default:
	}
}

// A stream is authorized once, at the handshake, and then lives on - so the
// sidebar's stream re-checks the session on its own tick, as the download
// stream does. Without it a page that is no longer logged in goes on being told
// the service is up, on a connection a session expiry should have closed.
func TestStatusStreamEndsWhenTheSessionIsCleared(t *testing.T) {
	server, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/status/events")
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
		t.Fatal("the status stream outlived the session it was authorized with")
	}
}

// The sidebar's network line is the probe's own state, not a second
// measurement of it. The Web must be able to render it from the stream alone,
// which means the field has to be on the wire from the first frame - and it has
// to be the same answer the Bot's /status gives, since both read one probe and
// the proxy pays for one set of requests.
func TestStatusStreamCarriesTheTelegramNetworkState(t *testing.T) {
	server, client, baseURL := sseTestServer(t)
	response, err := client.Get(baseURL + "/api/status/events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()

	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("the stream produced no first frame: %v", err)
	}
	payload := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(first), "data:"))
	var status connectionStatus
	if err := json.Unmarshal([]byte(payload), &status); err != nil {
		t.Fatalf("the first frame is not a status document: %v\n%s", err, payload)
	}
	if !strings.Contains(payload, `"telegramNetwork"`) {
		// An absent field and an empty one mean different things to the page:
		// the first leaves the line blank forever, the second renders 检测中.
		t.Fatalf("the frame does not carry the network state:\n%s", payload)
	}
	if status.TelegramNetwork.Status != server.bot.TelegramNetwork() {
		t.Fatalf("the frame reports network %q while the probe holds %q", status.TelegramNetwork.Status, server.bot.TelegramNetwork())
	}
}
