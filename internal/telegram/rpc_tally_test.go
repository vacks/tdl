package telegram

import (
	"context"
	"testing"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// chainFor applies an account's middlewares the way the client does: the first
// one listed is outermost.
//
// It exists so a test exercises the ordering the application actually builds
// rather than an ordering the test finds convenient. The tally's meaning
// depends entirely on where it sits in this chain, and a test that assembled
// its own chain would keep passing after the production order changed.
func chainFor(chain []gotd.Middleware, next tg.Invoker) tg.Invoker {
	invoker := next
	for i := len(chain) - 1; i >= 0; i-- {
		invoker = chain[i].Handle(invoker)
	}
	return invoker
}

// The tally is the only place an account's request volume is visible: a flood
// window surfaces nowhere else, because the waiter sleeps it off inside the
// invoker. Counting by method is what turns "the account felt slow" into "we
// sent four hundred history reads".
func TestRPCTallyCountsWireRequestsByAccountAndMethod(t *testing.T) {
	m := &Manager{}
	next := &countingInvoker{}
	invoke := chainFor(m.accountMiddlewares("account-1"), next)

	var out tg.MessagesMessagesBox
	if err := invoke.Invoke(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err != nil {
		t.Fatal(err)
	}
	if err := invoke.Invoke(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err != nil {
		t.Fatal(err)
	}
	if err := invoke.Invoke(context.Background(), &tg.ChannelsGetChannelsRequest{}, &out); err != nil {
		t.Fatal(err)
	}

	tally := m.RPCTally()
	if got := tally["account-1"]["MessagesGetHistoryRequest"]; got != 2 {
		t.Fatalf("history reads counted as %d, want 2", got)
	}
	if got := tally["account-1"]["ChannelsGetChannelsRequest"]; got != 1 {
		t.Fatalf("channel reads counted as %d, want 1", got)
	}
	// The method name is the TL type without its package qualifier, which is how
	// an operator would name the request in a log line.
	for method := range tally["account-1"] {
		if len(method) > 3 && method[:3] == "tg." {
			t.Fatalf("method %q kept its package qualifier", method)
		}
	}
}

// An access hash belongs to the account that resolved it, so a tally that mixed
// accounts would report one account's budget against another's rate limit.
func TestRPCTallyIsKeptPerAccount(t *testing.T) {
	m := &Manager{}
	var out tg.MessagesMessagesBox

	first := chainFor(m.accountMiddlewares("account-1"), &countingInvoker{})
	if err := first.Invoke(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err != nil {
		t.Fatal(err)
	}
	second := chainFor(m.accountMiddlewares("account-2"), &countingInvoker{})
	if err := second.Invoke(context.Background(), &tg.UsersGetUsersRequest{}, &out); err != nil {
		t.Fatal(err)
	}

	tally := m.RPCTally()
	if tally["account-1"]["MessagesGetHistoryRequest"] != 1 || len(tally["account-1"]) != 1 {
		t.Fatalf("account-1's counts are %v", tally["account-1"])
	}
	if tally["account-2"]["UsersGetUsersRequest"] != 1 || len(tally["account-2"]) != 1 {
		t.Fatalf("account-2's counts are %v", tally["account-2"])
	}
}

// The tally counts what Telegram saw, not what this process asked for. A
// resolution answered from the username cache never reached the wire, and
// counting it would overstate the very budget the tally exists to measure - by
// exactly the amount the cache was added to save.
//
// This is also the test that pins the tally's position in the chain: placed
// outside the cache it would count both resolutions, and the tally would report
// the cache as having done nothing.
func TestRPCTallyCountsOnlyRequestsThatReachedTheWire(t *testing.T) {
	m := &Manager{}
	next := &countingInvoker{}
	invoke := chainFor(m.accountMiddlewares("account-1"), next)

	resolve := func() {
		t.Helper()
		var out tg.ContactsResolvedPeer
		if err := invoke.Invoke(context.Background(), &tg.ContactsResolveUsernameRequest{Username: "tdlbot"}, &out); err != nil {
			t.Fatal(err)
		}
	}
	resolve()
	resolve()

	if next.calls() != 1 {
		t.Fatalf("the invoker saw %d requests, want 1: the cache stopped answering", next.calls())
	}
	if got := m.RPCTally()["account-1"]["ContactsResolveUsernameRequest"]; got != 1 {
		t.Fatalf("the tally counted %d resolutions, want 1: it is measuring calls, not traffic", got)
	}
}

// Reset is what makes the tally a measurement instead of a running total:
// reset, do one thing, read what it cost.
func TestRPCTallyCanBeReset(t *testing.T) {
	m := &Manager{}
	invoke := chainFor(m.accountMiddlewares("account-1"), &countingInvoker{})
	var out tg.MessagesMessagesBox
	if err := invoke.Invoke(context.Background(), &tg.MessagesGetHistoryRequest{}, &out); err != nil {
		t.Fatal(err)
	}
	if len(m.RPCTally()["account-1"]) == 0 {
		t.Fatal("nothing was counted, so the reset assertion below proves nothing")
	}
	m.ResetRPCTally()
	if len(m.RPCTally()) != 0 {
		t.Fatalf("counts survived a reset: %v", m.RPCTally())
	}
}

// A summary line has to stay readable when an account has thirty methods on it,
// so the most requested come first and ties are ordered by name.
func TestRPCTallySummaryOrdersByVolume(t *testing.T) {
	got := rpcTallySummary(map[string]int64{
		"MessagesGetHistoryRequest":      2,
		"ContactsResolveUsernameRequest": 7,
		"UsersGetUsersRequest":           2,
	})
	want := "ContactsResolveUsernameRequest=7 MessagesGetHistoryRequest=2 UsersGetUsersRequest=2"
	if got != want {
		t.Fatalf("summary is %q, want %q", got, want)
	}
}
