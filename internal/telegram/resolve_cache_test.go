package telegram

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

// countingInvoker answers every request the same way and records what it was
// asked, so a cache is observable as a request that never happened.
type countingInvoker struct {
	mu       sync.Mutex
	requests []bin.Encoder
	fail     bool
}

func (c *countingInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	c.mu.Lock()
	c.requests = append(c.requests, input)
	failed := c.fail
	c.mu.Unlock()
	if failed {
		return errors.New("FLOOD_WAIT_60")
	}
	resolved, ok := output.(*tg.ContactsResolvedPeer)
	if !ok {
		return nil
	}
	resolved.Peer = &tg.PeerUser{UserID: 42}
	resolved.Users = []tg.UserClass{&tg.User{ID: 42, Username: "tdlbot"}}
	return nil
}

func (c *countingInvoker) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

// contacts.resolveUsername is the request this application makes most often and
// the one Telegram throttles hardest, because every link, every session-task
// target and every post forwarded to the Bot resolves a name and gotd's
// peers.Manager answers none of them from memory. Reusing an answer is what
// keeps that volume off the account's server-side budget.
func TestUsernameResolutionIsReusedPerAccount(t *testing.T) {
	next := &countingInvoker{}
	m := &Manager{}
	invoke := m.usernameCacheMiddleware("account-1").Handle(next)

	resolve := func(t *testing.T, account, username string) {
		t.Helper()
		var out tg.ContactsResolvedPeer
		if err := invoke(context.Background(), &tg.ContactsResolveUsernameRequest{Username: username}, &out); err != nil {
			t.Fatal(err)
		}
		if out.Peer == nil {
			t.Fatal("the resolution answered nothing, so nothing was cached to reuse")
		}
	}

	resolve(t, "account-1", "tdlbot")
	resolve(t, "account-1", "tdlbot")
	if calls := next.calls(); calls != 1 {
		t.Fatalf("the same name was resolved %d times; it is asked once and reused", calls)
	}

	resolve(t, "account-1", "another")
	if calls := next.calls(); calls != 2 {
		t.Fatalf("a different name was served from the first name's answer: %d requests", calls)
	}

	// An access hash belongs to the account that resolved it, so one account
	// must never be answered with another account's result.
	other := m.usernameCacheMiddleware("account-2").Handle(next)
	var out tg.ContactsResolvedPeer
	if err := other(context.Background(), &tg.ContactsResolveUsernameRequest{Username: "tdlbot"}, &out); err != nil {
		t.Fatal(err)
	}
	if calls := next.calls(); calls != 3 {
		t.Fatalf("another account was answered from the first account's cache: %d requests", calls)
	}
}

// A failed resolution must not be remembered: caching it would turn one refused
// request into a name that can never be resolved again for the rest of the
// cache's life.
func TestAFailedUsernameResolutionIsNotReused(t *testing.T) {
	next := &countingInvoker{fail: true}
	m := &Manager{}
	invoke := m.usernameCacheMiddleware("account-1").Handle(next)

	var out tg.ContactsResolvedPeer
	if err := invoke(context.Background(), &tg.ContactsResolveUsernameRequest{Username: "tdlbot"}, &out); err == nil {
		t.Fatal("the failure was swallowed")
	}
	next.mu.Lock()
	next.fail = false
	next.mu.Unlock()
	if err := invoke(context.Background(), &tg.ContactsResolveUsernameRequest{Username: "tdlbot"}, &out); err != nil {
		t.Fatal(err)
	}
	if calls := next.calls(); calls != 2 {
		t.Fatalf("the failed attempt was reused: %d requests", calls)
	}
}

// hangingInvoker never answers until its context ends, which is what a request
// inside a flood window does: gotd's floodwait middleware sleeps off the
// server's FLOOD_WAIT and only gives up when the caller's deadline passes.
type hangingInvoker struct {
	mu       sync.Mutex
	requests int
}

func (h *hangingInvoker) Invoke(ctx context.Context, _ bin.Encoder, _ bin.Decoder) error {
	h.mu.Lock()
	h.requests++
	h.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (h *hangingInvoker) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.requests
}

// The reaction handler runs inside gotd's update dispatcher, which calls
// handlers synchronously, so a request that waits there stops every later update
// behind it. That happened: while Telegram was refusing this account, a reaction
// handler sat on this lookup for minutes, the updates arriving behind it were
// never handled, and the process restart that followed lost their triggers with
// no log line and no inbox row - both of which happen after this point.
//
// A request that never answers must therefore cost a display name and nothing
// else: the event still arrives, and the name comes from the entity the update
// already carried.
func TestReactionEventDoesNotWaitOnATelegramRequest(t *testing.T) {
	next := &hangingInvoker{}
	api := tg.NewClient(next)
	m := &Manager{root: t.TempDir(), stores: map[string]*sync.RWMutex{}}
	entities := tg.Entities{Users: map[int64]*tg.User{7: {ID: 7, FirstName: "Carl", AccessHash: 11}}}

	startedAt := time.Now()
	event, ok := m.reactionEvent(context.Background(), "account-1", entities, &tg.PeerUser{UserID: 7}, 492, []string{"❤"}, api)
	elapsed := time.Since(startedAt)

	if !ok {
		t.Fatal("the reaction was dropped, which is exactly the trigger that was lost")
	}
	if next.calls() == 0 {
		t.Fatal("the handler never made the lookup, so this test proves nothing about waiting on one")
	}
	if elapsed > 2*updatePeerNameTimeout {
		t.Fatalf("the handler waited %v on one request; the update stream stops for that whole time", elapsed)
	}
	if event.MessageID != 492 {
		t.Fatalf("the event carries message %d, want 492", event.MessageID)
	}
	if event.DialogName != "Carl" {
		t.Fatalf("the dialog name is %q; the update carried it and reading it costs no request", event.DialogName)
	}
	if _, self := event.InputPeer.(*tg.InputPeerUser); !self {
		t.Fatalf("the event's peer is %T; the InputPeer came from the update and must survive the timeout", event.InputPeer)
	}
}

// stubGate accepts every request and records nothing, so a paced request can be
// driven without a download service behind it.
type stubGate struct{}

func (stubGate) Acquire(context.Context, string) error { return nil }
func (stubGate) Report(string, error)                  {}

// sleepingInvoker answers after a delay, which is what a request inside a flood
// window looks like from here: it returns eventually, having spent minutes doing
// nothing the process can see.
type sleepingInvoker struct{ delay time.Duration }

func (s sleepingInvoker) Invoke(ctx context.Context, _ bin.Encoder, _ bin.Decoder) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A flood window is invisible from inside this process - the requests sleep in a
// middleware that swallows the server's refusal - so a resolution that waits
// minutes looks exactly like one that was never asked for. Timing what the gate
// already sees is the only way that becomes visible from here, and this is what
// makes it work.
func TestASlowRequestIsReportedWithItsMethod(t *testing.T) {
	type report struct {
		account, method string
		elapsed         time.Duration
	}
	var reports []report
	m := &Manager{rpcGate: stubGate{}, slowRPC: func(account, method string, elapsed time.Duration, err error) {
		reports = append(reports, report{account, method, elapsed})
	}}

	run := func(invoker tg.Invoker) {
		invoke := m.rpcGateMiddleware("account-1").Handle(invoker)
		var out tg.MessagesMessagesBox
		if err := invoke(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err != nil {
			t.Fatal(err)
		}
	}

	// A request that answers promptly is not news: reporting it would bury the
	// one line that matters under the cost of every ordinary call.
	run(sleepingInvoker{delay: time.Millisecond})
	if len(reports) != 0 {
		t.Fatalf("an ordinary request was reported as slow: %+v", reports)
	}

	run(sleepingInvoker{delay: slowRPCThreshold + 200*time.Millisecond})
	if len(reports) != 1 {
		t.Fatalf("a slow request was not reported: %d reports", len(reports))
	}
	if reports[0].method != "MessagesGetHistoryRequest" {
		t.Fatalf("the report names %q; without the method there is nothing to act on", reports[0].method)
	}
	if reports[0].account != "account-1" {
		t.Fatalf("the report names account %q", reports[0].account)
	}
	if reports[0].elapsed < slowRPCThreshold {
		t.Fatalf("the report claims %v", reports[0].elapsed)
	}
}
