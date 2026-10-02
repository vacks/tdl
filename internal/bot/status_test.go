package bot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/buildinfo"
	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

// The list above is only half the fix: /status reads it through the manager,
// and a helper nobody calls is a bug the helper's own tests cannot see.
func TestStatusTextReadsEveryAccountFromTheManager(t *testing.T) {
	dir := t.TempDir()
	payload := `{"accounts":[` +
		`{"id":"a","telegramId":1,"firstName":"张三","username":"zhangsan","state":"authorized","createdAt":"2026-01-01T00:00:00Z"},` +
		`{"id":"b","telegramId":2,"firstName":"李四","state":"authorized","createdAt":"2026-01-01T00:00:00Z"}` +
		`],"currentId":"b"}`
	// Open reads <dataDir>/telegram/accounts.json, not <dataDir>/accounts.json.
	accountDir := filepath.Join(dir, "telegram")
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		t.Fatalf("create account dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(accountDir, "accounts.json"), []byte(payload), 0o600); err != nil {
		t.Fatalf("write accounts.json: %v", err)
	}
	manager, err := telegram.Open(dir, func() string { return "" })
	if err != nil {
		t.Fatalf("open telegram manager: %v", err)
	}
	defer manager.Stop()

	got := (&Service{telegram: manager}).statusText()

	// Without a download manager every figure has to say so instead of showing
	// a zero, and the order of the three lines is the one the card documents.
	want := strings.Join([]string{
		"<b>当前状态</b>",
		"版本：TDL 管理 " + buildinfo.Version + " · 上游 TDL " + buildinfo.UpstreamVersion,
		"数据库状态：未知",
		"Telegram网络：检测中",
		"登录账号（2）：",
		"- 张三 (@zhangsan)",
		"- 李四 · 当前账户",
		"监听事件：暂时无法读取",
		"当前正在下载：暂时无法读取",
		"最近下载失败：暂时无法读取",
		"CPU：0.0%",
		"内存：0.0%",
		"网络：↓ 0 B/s · ↑ 0 B/s",
	}, "\n")
	if got != want {
		t.Fatalf("the assembled status card is wrong:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// Every account the service holds has to appear: the listeners, the reaction
// triggers and the queue all run on them, and naming only the selected one
// reads as if it were the only session logged in.
func TestAccountsStatusTextListsEveryAccount(t *testing.T) {
	accounts := []telegram.Account{
		{ID: "a", TelegramID: 1, FirstName: "张三", Username: "zhangsan", State: "authorized"},
		{ID: "b", TelegramID: 2, FirstName: "李四", LastName: "王", State: "authorized"},
		{ID: "c", TelegramID: 3, FirstName: "赵六", State: "expired"},
	}
	got := accountsStatusText(accounts, "b")

	for _, want := range []string{
		"登录账号（3）：",
		"- 张三 (@zhangsan)",
		"- 李四 王 · 当前账户",
		"- 赵六 · 会话失效",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("status does not contain %q:\n%s", want, got)
		}
	}
	// The state word is carried by the accounts that are not ordinary. Putting
	// it on the authorized ones too would make every line identical in shape.
	if strings.Contains(got, "- 张三 (@zhangsan) · 已登录") {
		t.Fatalf("an authorized account was given a state word:\n%s", got)
	}
	// Exactly one account is current, and it is the one whose id matches.
	if n := strings.Count(got, "当前账户"); n != 1 {
		t.Fatalf("%d accounts are marked current, want 1:\n%s", n, got)
	}
}

// A session that is still being logged in has neither a name nor a Telegram id,
// and rendering it as "Telegram 用户 0" would invent an identity for it.
func TestAccountsStatusTextNamesAnUnnamedAccount(t *testing.T) {
	got := accountsStatusText([]telegram.Account{{ID: "a", State: "waiting_for_qr"}}, "")

	if !strings.Contains(got, "- 未命名账户 · 等待扫码") {
		t.Fatalf("an unnamed account was not described by its state:\n%s", got)
	}
	if strings.Contains(got, "Telegram 用户 0") {
		t.Fatalf("an unnamed account was given the placeholder id 0:\n%s", got)
	}
}

// The text is delivered with the Bot's HTML parse mode, so a display name is
// not safe to interpolate as it stands.
func TestAccountsStatusTextEscapesTheDisplayName(t *testing.T) {
	got := accountsStatusText([]telegram.Account{{ID: "a", FirstName: "<b>x</b>", State: "authorized"}}, "")

	if strings.Contains(got, "<b>x</b>") {
		t.Fatalf("the display name was not escaped:\n%s", got)
	}
	if !strings.Contains(got, "&lt;b&gt;x&lt;/b&gt;") {
		t.Fatalf("the display name was escaped into something unexpected:\n%s", got)
	}
}

// No account at all is the one case the list cannot describe.
func TestAccountsStatusTextWithNoAccounts(t *testing.T) {
	if got := accountsStatusText(nil, ""); got != "登录账号：未登录" {
		t.Fatalf("empty account list rendered as %q", got)
	}
}

// Telegram rejects a message longer than botCardLimit, so a list that grows
// with the number of sessions has to stop somewhere. What matters is that it
// stops visibly: a silently shortened list would read as the whole truth.
func TestAccountsStatusTextStopsVisiblyAtTheBudget(t *testing.T) {
	accounts := make([]telegram.Account, 0, 200)
	for index := 0; index < 200; index++ {
		accounts = append(accounts, telegram.Account{
			ID:         fmt.Sprintf("account-%d", index),
			TelegramID: int64(index),
			FirstName:  strings.Repeat("名", 20),
			State:      "authorized",
		})
	}
	got := accountsStatusText(accounts, "account-0")

	if len([]rune(got)) > botCardLimit {
		t.Fatalf("the account block is %d runes, past the %d Telegram accepts:\n%s", len([]rune(got)), botCardLimit, got)
	}
	if !strings.Contains(got, "另有 200 个账号未显示") && !strings.Contains(got, "个账号未显示") {
		t.Fatalf("the list was shortened without saying so:\n%s", got)
	}
	if strings.Contains(got, "account-199") {
		t.Fatalf("the 200th account was rendered, so nothing was bounded:\n%s", got)
	}
	// Every retained line is whole: cutting inside an escape sequence would
	// produce markup Telegram rejects.
	name := strings.Repeat("名", 20)
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "- ") && !strings.Contains(line, name) {
			t.Fatalf("a line was cut mid-account: %q", line)
		}
	}
}

// The card is read as a whole, so its layout is pinned line by line rather than
// by keywords: a figure that moved to a different line, or a label that drifted
// from the wording the Web UI uses, is a change the reader notices before any
// keyword test would.
func TestStatusCardLayout(t *testing.T) {
	accounts := accountsStatusText([]telegram.Account{
		{ID: "a", TelegramID: 6807640357, FirstName: "rapalw", Username: "rapalw", State: "expired"},
	}, "")
	got := statusCard(
		accounts, "已连接", "连接正常",
		[]string{"监听事件：0 处理中 · 0 等待重试", "当前正在下载：0", "最近下载失败：0"},
		0.2, 5.4, 1064, 2621,
	)
	want := strings.Join([]string{
		"<b>当前状态</b>",
		"版本：TDL 管理 " + buildinfo.Version + " · 上游 TDL " + buildinfo.UpstreamVersion,
		"数据库状态：已连接",
		"Telegram网络：连接正常",
		"登录账号（1）：",
		"- rapalw (@rapalw) · 会话失效",
		"监听事件：0 处理中 · 0 等待重试",
		"当前正在下载：0",
		"最近下载失败：0",
		"CPU：0.2%",
		"内存：5.4%",
		"网络：↓ 1.04 KB/s · ↑ 2.56 KB/s",
	}, "\n")
	if got != want {
		t.Fatalf("the status card does not match the documented layout:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// The network line answers one question - is the path to Telegram working - so
// no two states may share a word, and the answer carries the latency, which is
// the part that tells a proxy with room to spare from one taking eight seconds.
func TestTelegramNetworkLabel(t *testing.T) {
	cases := []struct {
		status  string
		latency time.Duration
		want    string
	}{
		{telegramNetworkConnected, 12 * time.Millisecond, "连接正常（12ms）"},
		{telegramNetworkDisconnected, 10 * time.Second, "连接失败"},
		{"", 0, "检测中"},
	}
	seen := make(map[string]string, len(cases))
	for _, testCase := range cases {
		got := telegramNetworkLabel(testCase.status, testCase.latency)
		if got != testCase.want {
			t.Fatalf("probe result (%q, %s) rendered as %q, want %q", testCase.status, testCase.latency, got, testCase.want)
		}
		if other, repeated := seen[got]; repeated {
			t.Fatalf("results %q and %q both render as %q", other, testCase.status, got)
		}
		seen[got] = testCase.status
	}
}

// The probe has to be able to say "failed", and it has to be able to say it
// again after saying "ok" - a proxy that comes and goes is the case this whole
// line exists for, and a state that only ever improves would report the outage
// before the recovery and never the next outage.
func TestTelegramProbeRecordsBothOutcomes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound) // what the API root actually answers
	}))
	t.Cleanup(server.Close)
	// The probe goes through the service's own client so that it travels the
	// configured proxy, which is the only reason its answer is worth anything.
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open settings: %v", err)
	}
	service := &Service{settings: store}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); service.runTelegramProbe(ctx, server.URL, 10*time.Millisecond) }()

	if !waitForProbe(t, service, telegramNetworkConnected) {
		cancel()
		<-done
		t.Fatalf("a probe against a reachable server did not record success: %s", service.statusText())
	}

	// The same probe, with the far end gone. Nothing about the service changes.
	server.Close()
	if !waitForProbe(t, service, telegramNetworkDisconnected) {
		cancel()
		<-done
		t.Fatalf("a probe against an unreachable server did not record failure")
	}
	cancel()
	<-done
}

func waitForProbe(t *testing.T, service *Service, want string) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if status, _ := service.networkProbe.snapshot(); status == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// Whatever the probe last saw has to reach the card, and the card must read it
// rather than measure anything: the read is what /status does.
func TestStatusTextReadsTheProbeResult(t *testing.T) {
	service := &Service{}
	service.networkProbe.record(true, 34*time.Millisecond)

	if got := service.statusText(); !strings.Contains(got, "Telegram网络：连接正常（34ms）") {
		t.Fatalf("the card does not carry the probe result:\n%s", got)
	}
}

// The Web sidebar renders this state in its own words, so what it reads has to
// be a state and not a sentence: one probe answers both surfaces, and the
// vocabulary is the one the sidebar already uses for a connection.
func TestTelegramNetworkExposesTheStateForTheSidebar(t *testing.T) {
	service := &Service{}
	if got := service.TelegramNetwork(); got != "" {
		t.Fatalf("an unsampled probe reported %q, want an empty state", got)
	}
	service.networkProbe.record(true, time.Millisecond)
	if got := service.TelegramNetwork(); got != telegramNetworkConnected {
		t.Fatalf("a reachable network reported %q, want %q", got, telegramNetworkConnected)
	}
	service.networkProbe.record(false, time.Second)
	if got := service.TelegramNetwork(); got != telegramNetworkDisconnected {
		t.Fatalf("an unreachable network reported %q, want %q", got, telegramNetworkDisconnected)
	}
}

// A proxy that accepts the connection and never answers the handshake is what a
// stalled proxy looks like from here.
//
// The transport does not bound this dial: it detaches the context from the
// request and gives it no deadline, deliberately, so a slow dial can still be
// reused by the next request. Nothing upstream then ever gives up, and the
// socket and its goroutine live until TCP does - one per attempt, from a proxy
// that flaps. The bound has to come from the client, and this is the test that
// says it is still there.
func TestStalledProxyDialIsAbandoned(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			conn, err := blocker.Accept()
			if err != nil {
				return
			}
			// Accepted, never answered: the handshake the client waits for
			// never comes.
			select {
			case accepted <- conn:
			default:
				_ = conn.Close()
			}
		}
	}()

	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open settings: %v", err)
	}
	values := settings.Defaults()
	values.ProxyURL = "socks5://" + blocker.Addr().String()
	if err := store.Update(values); err != nil {
		t.Fatalf("set proxy: %v", err)
	}
	client := (&Service{settings: store, dialTimeout: 1500 * time.Millisecond}).httpClient()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.telegram.org/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	response, err := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("a request through a proxy that never answers succeeded")
	}

	select {
	case conn := <-accepted:
		defer func() { _ = conn.Close() }()
		// Two properties, in order.
		//
		// The first is why the bound has to be here at all: the request's
		// deadline does not reach the dial, so a moment after the request has
		// given up the connection is still open and still being held. Reading
		// until the deadline is how that shows - the smoke test for it would be
		// an end of file, which is what the second read waits for.
		buffer := make([]byte, 64)
		if err := drainUntilSilence(conn, buffer, 200*time.Millisecond); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the connection closed with the request (%v), so this test is not measuring a stalled dial", err)
		}
		// The second is the fix: the dial's own bound releases it, which shows
		// as an end of file rather than another timeout.
		if err := drainUntilSilence(conn, buffer, 3*time.Second); errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("the dial was never abandoned: the stalled connection is still held")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the proxy was never dialed")
	}
}

// drainUntilSilence reads until the connection reports something other than
// data, and returns that error. A deadline error means the peer is still
// holding the connection; anything else means it closed it.
func drainUntilSilence(conn net.Conn, buffer []byte, wait time.Duration) error {
	if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
		return err
	}
	for {
		// The client opens with its SOCKS5 greeting, so the first read is data
		// rather than the end.
		if _, err := conn.Read(buffer); err != nil {
			return err
		}
	}
}

// The database line is the only new part of the card, and the three states it
// can report must be three different words - a connection that has not been
// sampled yet is not an outage.
func TestDatabaseStatusLabel(t *testing.T) {
	cases := []struct {
		health download.DatabaseHealth
		want   string
	}{
		{download.DatabaseHealth{Status: "connected"}, "已连接"},
		{download.DatabaseHealth{Status: "unavailable", Error: "数据库暂时不可用，服务将自动重连"}, "连接异常"},
		{download.DatabaseHealth{}, "检测中"},
	}
	for _, testCase := range cases {
		if got := databaseStatusLabel(testCase.health); got != testCase.want {
			t.Fatalf("health %q rendered as %q, want %q", testCase.health.Status, got, testCase.want)
		}
	}
}
