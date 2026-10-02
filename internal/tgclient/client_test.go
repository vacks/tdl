package tgclient

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/vacks/tdl/internal/kv"
)

// stubInvoker counts calls and answers with a scripted error per call.
type stubInvoker struct {
	mu    sync.Mutex
	calls int
	err   func(call int) error
}

func (s *stubInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if s.err != nil {
		return s.err(call)
	}
	return nil
}

func (s *stubInvoker) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type mapStorage map[string][]byte

func (m mapStorage) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := m[key]
	if !ok {
		return nil, kv.ErrNotFound
	}
	return value, nil
}
func (m mapStorage) Set(_ context.Context, key string, value []byte) error {
	m[key] = value
	return nil
}
func (m mapStorage) Delete(_ context.Context, key string) error {
	delete(m, key)
	return nil
}

// The application identity is what the account was authorized as. Presenting a
// different one is not a cosmetic difference: it is the mismatch that gets an
// account looked at, and it is also the value Telegram compares when deciding
// whether a session is still the one it issued.
func TestApplicationIdentityIsTheDesktopClient(t *testing.T) {
	identity, err := application(context.Background(), mapStorage{kv.App(): []byte(AppDesktop)})
	if err != nil {
		t.Fatal(err)
	}
	if identity.id != 2040 {
		t.Fatalf("application id is %d, want 2040 (tdesktop)", identity.id)
	}
	if identity.hash != "b18441a1ff607e10a989891a5462e627" {
		t.Fatalf("application hash is %q", identity.hash)
	}

	desktop, ok := applications[AppDesktop]
	if !ok {
		t.Fatal("the desktop identity is not in the table")
	}
	if desktop != identity {
		t.Fatalf("AppDesktop resolves to %+v but the table holds %+v", identity, desktop)
	}
}

// An account store with nothing written yet is a first run, and the upstream
// default is the identity this application has always fallen back to.
func TestApplicationFallsBackOnlyWhenTheKeyIsAbsent(t *testing.T) {
	identity, err := application(context.Background(), mapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	if identity != applications[AppBuiltin] {
		t.Fatalf("an absent identity resolved to %+v", identity)
	}

	// A stored value with no table entry is a broken state file, not a reason to
	// authorize as somebody else.
	if _, err := application(context.Background(), mapStorage{kv.App(): []byte("some-other-app")}); err == nil {
		t.Fatal("an unknown stored identity was accepted")
	}

	// An unreadable store is not an absent key either.
	if _, err := application(context.Background(), failingStorage{}); err == nil {
		t.Fatal("an unreadable store fell back to a default identity")
	}
}

type failingStorage struct{}

func (failingStorage) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("disk on fire")
}
func (failingStorage) Set(context.Context, string, []byte) error { return nil }
func (failingStorage) Delete(context.Context, string) error      { return nil }

// A transport failure never reached Telegram, so repeating it is free of the
// side effects that make a blind retry unsafe. Anything Telegram answered is an
// answer, and repeating it would just ask the same question again.
func TestRecoveryRetriesTransportFailuresOnly(t *testing.T) {
	transport := &stubInvoker{err: func(int) error { return errors.New("connection reset by peer") }}
	recovery := recoveryMiddleware{ctx: context.Background(), backoff: backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), 2)}
	var out tg.MessagesMessagesBox
	if err := recovery.Handle(transport)(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err == nil {
		t.Fatal("a transport failure was reported as success")
	}
	if transport.count() != 3 {
		t.Fatalf("a transport failure was attempted %d times, want 3 (the first plus two retries)", transport.count())
	}

	answered := &stubInvoker{err: func(int) error { return &tgerr.Error{Code: 400, Type: "MSG_ID_INVALID", Message: "MSG_ID_INVALID"} }}
	if err := recovery.Handle(answered)(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err == nil {
		t.Fatal("a refused request was reported as success")
	}
	if answered.count() != 1 {
		t.Fatalf("a request Telegram refused was sent %d times, want 1", answered.count())
	}
}

// A failure the server reports about itself is worth sending again; every other
// answer is passed through, and it keeps the prefix this middleware has always
// put on it, because callers match on that text.
func TestRetryResendsOnlyServerSideFailures(t *testing.T) {
	busy := &stubInvoker{err: func(int) error {
		return &tgerr.Error{Code: 500, Type: "WORKER_BUSY_TOO_LONG_RETRY", Message: "WORKER_BUSY_TOO_LONG_RETRY"}
	}}
	err := retryMiddleware{max: 3}.Handle(busy)(context.Background(), &tg.MessagesGetHistoryRequest{}, &tg.MessagesMessagesBox{})
	if err == nil || !strings.Contains(err.Error(), "retry limit reached") {
		t.Fatalf("a permanently busy server produced %v", err)
	}
	if busy.count() != 3 {
		t.Fatalf("a busy server was asked %d times, want 3", busy.count())
	}

	refused := &stubInvoker{err: func(int) error { return &tgerr.Error{Code: 403, Type: "CHANNEL_PRIVATE", Message: "CHANNEL_PRIVATE"} }}
	err = retryMiddleware{max: 3}.Handle(refused)(context.Background(), &tg.MessagesGetHistoryRequest{}, &tg.MessagesMessagesBox{})
	if err == nil {
		t.Fatal("a refused request was reported as success")
	}
	if refused.count() != 1 {
		t.Fatalf("a refused request was sent %d times, want 1", refused.count())
	}
	if !strings.Contains(err.Error(), "CHANNEL_PRIVATE") {
		t.Fatalf("the error lost the reason Telegram gave: %v", err)
	}
	if !strings.Contains(err.Error(), "retry middleware skip") {
		t.Fatalf("the error lost the prefix callers match on: %v", err)
	}
}
