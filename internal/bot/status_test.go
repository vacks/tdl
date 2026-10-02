package bot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vacks/tdl/internal/adapter/upstream"
	"github.com/vacks/tdl/internal/buildinfo"
	"github.com/vacks/tdl/internal/download"
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
		"版本：TDL 管理 " + buildinfo.Version + " · 上游 TDL " + upstream.Version,
		"数据库状态：未知",
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

// The card is read as a whole, so its layout is pinned line by line rather than
// by keywords: a figure that moved to a different line, or a label that drifted
// from the wording the Web UI uses, is a change the reader notices before any
// keyword test would.
func TestStatusCardLayout(t *testing.T) {
	accounts := accountsStatusText([]telegram.Account{
		{ID: "a", TelegramID: 6807640357, FirstName: "rapalw", Username: "rapalw", State: "expired"},
	}, "")
	got := statusCard(
		accounts, "已连接",
		[]string{"监听事件：0 处理中 · 0 等待重试", "当前正在下载：0", "最近下载失败：0"},
		0.2, 5.4, 1064, 2621,
	)
	want := strings.Join([]string{
		"<b>当前状态</b>",
		"版本：TDL 管理 " + buildinfo.Version + " · 上游 TDL " + upstream.Version,
		"数据库状态：已连接",
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
