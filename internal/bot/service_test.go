package bot

import (
	"context"
	"errors"
	"testing"
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
