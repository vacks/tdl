package bot

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/settings"
)

func TestNormalizeCommand(t *testing.T) {
	tests := map[string]string{
		"/help":               "/help",
		"/help@TDLControlBot": "/help",
		"/list@TDLControlBot": "/list",
		"https://t.me/a/1":    "https://t.me/a/1",
		"/help arguments":     "/help arguments",
	}
	for input, want := range tests {
		if got := normalizeCommand(input); got != want {
			t.Errorf("normalizeCommand(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRetryableSubmitError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "temporary network", err: errors.New("temporary network failure"), want: true},
		{name: "flood wait", err: errors.New("FLOOD_WAIT_5"), want: true},
		{name: "invalid link", err: errors.New("无法解析 Telegram 链接"), want: false},
		{name: "permission denied", err: errors.New("message is not accessible"), want: false},
	}
	for _, test := range tests {
		if got := retryableSubmitError(test.err); got != test.want {
			t.Errorf("%s: retryableSubmitError(%v) = %v, want %v", test.name, test.err, got, test.want)
		}
	}
}

func TestPollRetryDelayIsBoundedAndDeterministic(t *testing.T) {
	if got := pollRetryDelay(1); got < 2*time.Second || got >= 3*time.Second {
		t.Fatalf("first retry delay = %s, want 2s to 3s", got)
	}
	if got, want := pollRetryDelay(99), time.Minute; got != want {
		t.Fatalf("capped retry delay = %s, want %s", got, want)
	}
	if got, want := pollRetryDelay(4), pollRetryDelay(4); got != want {
		t.Fatalf("retry jitter must be stable: %s != %s", got, want)
	}
	if !shouldLogPollFailure(1) || !shouldLogPollFailure(8) || shouldLogPollFailure(3) {
		t.Fatal("unexpected poll failure log cadence")
	}
}

func TestRedactBotError(t *testing.T) {
	token := "123456:secret-token"
	message := "Post \"https://api.telegram.org/bot123456:secret-token/getUpdates\": timeout"
	got := redactBotError(token, message)
	if got == message || !strings.Contains(got, "[REDACTED]") || strings.Contains(got, token) {
		t.Fatalf("redactBotError() = %q", got)
	}
}

func TestPrivateCallbackUsesClickingUser(t *testing.T) {
	query := callbackQuery{}
	query.From.ID = 42
	query.Message = &message{}
	query.Message.Chat.ID = 42
	// Telegram sets callback_query.message.from to the Bot that originally
	// sent the inline keyboard, not the user who clicked it.
	query.Message.From.ID = 999
	if !privateCallback(query) {
		t.Fatal("private callback was rejected because message sender is the Bot")
	}
	query.Message.Chat.ID = -100123
	if privateCallback(query) {
		t.Fatal("group callback was accepted")
	}
}

func TestBotAuthorizationAndTelegramLinks(t *testing.T) {
	cfg := settings.Bot{ControlUserIDs: []int64{7, 9}}
	if !allowed(cfg, 7) || !allowed(cfg, 9) || allowed(cfg, 8) {
		t.Fatal("control-user authorization did not match configured users")
	}
	for raw, want := range map[string]bool{
		"https://t.me/example/1":        true,
		"https://subdomain.t.me/a":      true,
		"https://telegram.me/example/1": false,
		"https://example.com/t.me/test": false,
		"not a URL":                     false,
	} {
		if got := isTelegramLink(raw); got != want {
			t.Errorf("isTelegramLink(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestTaskPresentationContainsEscapedProgressAndActions(t *testing.T) {
	job := download.Job{ID: "job-1", DialogName: "<unsafe>", Status: "running", CompletedItems: 1, TotalItems: 2, Items: []download.Item{
		{DialogKey: "channel:1", MessageID: 1, OriginalName: "<one>.mp4", Status: "completed"},
		{DialogKey: "channel:1", MessageID: 2, OriginalName: "two.mp4", Status: "running"},
	}}
	text := taskText(job, []download.FileProgress{{DialogKey: "channel:1", MessageID: 2, Downloaded: 50, Total: 100, SpeedBPS: 1024}})
	for _, want := range []string{"下载中", "&lt;unsafe&gt;", "1/2 文件", "100%", "50%", "1.00 KB/s", "&lt;one&gt;.mp4"} {
		if !strings.Contains(text, want) {
			t.Errorf("taskText missing %q: %s", want, text)
		}
	}
	keys := taskKeyboard(job, 2)
	joined := ""
	for _, row := range keys {
		for _, button := range row {
			joined += button.CallbackData + " "
		}
	}
	for _, want := range []string{"t:job-1:pause:2", "t:job-1:cancel:2", "t:job-1:view:2", "l:2"} {
		if !strings.Contains(joined, want) {
			t.Errorf("taskKeyboard missing %q: %s", want, joined)
		}
	}
}

func TestLifecycleAndDeletedMessagesKeepExpectedControls(t *testing.T) {
	job := download.Job{ID: "job-1", DialogName: "频道", Status: "completed", CompletedItems: 2, TotalItems: 2}
	if text := lifecycleText(job); !strings.Contains(text, "下载完成") || !strings.Contains(text, "2/2 文件") {
		t.Fatalf("lifecycleText() = %q", text)
	}
	deleted := deletedTaskText("✅ <b>下载完成</b>\n进度：2/2 文件")
	if !strings.Contains(deleted, "任务已删除") || !strings.Contains(deleted, "2/2 文件") {
		t.Fatalf("deletedTaskText() = %q", deleted)
	}
	keyboard := deletedTaskKeyboard(3)
	if len(keyboard) != 1 || keyboard[0][0].CallbackData != "l:3" {
		t.Fatalf("deletedTaskKeyboard() = %#v", keyboard)
	}
}
