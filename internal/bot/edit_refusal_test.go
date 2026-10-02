package bot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/settings"
)

// A card a person deleted out of a chat that still exists is a 400, not a 403,
// and the only place Telegram says so is the response body.
//
// The body of a failed request was not read, so that wording never arrived: the
// reference to the card was kept, and every later status change edited the same
// dead message again - failing, logging, and spending from the same eight
// requests a second that every command reply and every progress refresh shares.
// A handful of dead cards is enough to starve the conversation.
func TestADeletedCardIsRecognizedFromTheBodyOfARefusal(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		missing   bool
		unchanged bool
		why       string
	}{
		{
			name:    "message deleted",
			err:     &botAPIError{method: "editMessageText", status: http.StatusBadRequest, description: "Bad Request: message to edit not found"},
			missing: true,
			why:     "Telegram reports a deleted message with the same status as any malformed request",
		},
		{
			name:    "chat gone",
			err:     &botAPIError{method: "editMessageText", status: http.StatusBadRequest, description: "Bad Request: chat not found"},
			missing: true,
			why:     "the chat itself is gone, so nothing can be rendered there again",
		},
		{
			name:    "id invalid",
			err:     &botAPIError{method: "editMessageText", status: http.StatusBadRequest, description: "Bad Request: MESSAGE_ID_INVALID"},
			missing: true,
			why:     "the id answers for nothing, and no later edit will find it",
		},
		{
			name:    "blocked",
			err:     &botAPIError{method: "editMessageText", status: http.StatusForbidden, description: "Forbidden: bot was blocked by the user"},
			missing: true,
			why:     "the 403 path that was already recognized, kept working",
		},
		{
			name:      "not modified",
			err:       &botAPIError{method: "editMessageText", status: http.StatusBadRequest, description: "Bad Request: message is not modified"},
			unchanged: true,
			why:       "the card already says what the caller wanted; this is not a failure and must not drop the reference",
		},
		{
			name: "a real failure keeps its reference",
			err:  &botAPIError{method: "editMessageText", status: http.StatusInternalServerError, description: "Internal Server Error"},
			why:  "an outage is not a reason to stop tracking the card",
		},
		{
			name: "an error that is not a Bot API answer at all",
			err:  errors.New("dial tcp: connection refused"),
			why:  "the proxy dropped the connection; the next edit may well succeed",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			missing, unchanged := editRefusal(testCase.err)
			if missing != testCase.missing || unchanged != testCase.unchanged {
				t.Fatalf("editRefusal(%v) = (missing %t, unchanged %t), want (%t, %t): %s",
					testCase.err, missing, unchanged, testCase.missing, testCase.unchanged, testCase.why)
			}
		})
	}
}

// The refusal has to arrive with its wording attached, which is the part that
// was missing: the error carried the status code and nothing else.
func TestARefusedCallCarriesWhatTelegramSaid(t *testing.T) {
	err := error(&botAPIError{method: "editMessageText", status: http.StatusBadRequest, description: "Bad Request: message to edit not found"})
	if got := botAPIDescription(err); got != "Bad Request: message to edit not found" {
		t.Fatalf("the refusal carries %q", got)
	}
	if got := botAPIDescription(errors.New("something else")); got != "" {
		t.Fatalf("an error that is not a Bot API answer reports %q", got)
	}
}

// The wording only helps if it survives the trip. This is the half that was
// missing: the error carried the status code and the body was discarded, so a
// test that built the error by hand would have proved nothing about what
// actually happens to a real refusal.
func TestARefusalCarriesItsBodyThroughTheRealTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/botTOKEN/editMessageText" {
			t.Errorf("the call went to %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":400,"description":"Bad Request: message to edit not found"}`))
	}))
	t.Cleanup(server.Close)

	service := newTestService(t, server.URL)
	var out json.RawMessage
	err := service.call("TOKEN", "editMessageText", map[string]any{}, &out)
	if err == nil {
		t.Fatal("a 400 was reported as success")
	}
	if missing, _ := editRefusal(err); !missing {
		t.Fatalf("a deleted card was not recognized from a real refusal: %v (description %q)", err, botAPIDescription(err))
	}
}

// A 429 whose window travels only in the body has to be waited out too.
//
// The header is not guaranteed - an intermediary may drop it, and Telegram's
// own documentation puts the number in the body - so a call that read only the
// header retried on this application's schedule instead of the server's, which
// is how a throttled Bot makes its own throttle longer.
func TestA429BodyAloneStillImposesItsWindow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7","parameters":{"retry_after":7}}`))
	}))
	t.Cleanup(server.Close)

	service := newTestService(t, server.URL)
	var out json.RawMessage
	if err := service.call("TOKEN", "sendMessage", map[string]any{}, &out); err == nil {
		t.Fatal("a 429 was reported as success")
	}

	service.outboundMu.Lock()
	blocked := service.outboundBlockedUntil
	service.outboundMu.Unlock()
	if blocked.IsZero() || time.Until(blocked) < 5*time.Second {
		t.Fatalf("a 429 that named a 7 second window left the service unblocked (%v); "+
			"it will retry on its own schedule and extend the throttle", blocked)
	}
}

// newTestService builds a service that talks to a stand-in for the Bot API.
func newTestService(t *testing.T, base string) *Service {
	t.Helper()
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open settings: %v", err)
	}
	service := &Service{settings: store, apiBase: base}
	service.ctx, service.cancel = context.WithCancel(context.Background())
	t.Cleanup(service.cancel)
	return service
}
