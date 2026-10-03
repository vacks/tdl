package telegram

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/vacks/tdl/internal/kv"
)

// A listener-named dialog must read its display name from the same entity map
// ExtractPeer already matched against. Replacing that with a dialog type label
// is what made unrelated dialogs share one download directory.
func TestInputPeerDisplayNameUsesUpdateEntities(t *testing.T) {
	entities := tg.Entities{
		Users: map[int64]*tg.User{
			7:  {ID: 7, FirstName: "TDL", LastName: "DEV"},
			10: {ID: 10, FirstName: "SomeBot"},
		},
		Chats:    map[int64]*tg.Chat{8: {ID: 8, Title: "群组名"}},
		Channels: map[int64]*tg.Channel{9: {ID: 9, Title: "频道名"}},
	}

	tests := []struct {
		name  string
		input tg.InputPeerClass
		want  string
	}{
		{name: "saved messages", input: &tg.InputPeerSelf{}, want: "收藏消息"},
		{name: "user uses first and last name", input: &tg.InputPeerUser{UserID: 7}, want: "TDL DEV"},
		{name: "user without last name", input: &tg.InputPeerUser{UserID: 10}, want: "SomeBot"},
		{name: "chat uses title", input: &tg.InputPeerChat{ChatID: 8}, want: "群组名"},
		{name: "channel uses title", input: &tg.InputPeerChannel{ChannelID: 9}, want: "频道名"},
		{name: "missing entity stays empty", input: &tg.InputPeerUser{UserID: 404}, want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := inputPeerDisplayName(test.input, entities); got != test.want {
				t.Errorf("inputPeerDisplayName() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSessionCheckOnlyMarksSuccessfulProbe(t *testing.T) {
	if !shouldMarkSessionChecked(nil) {
		t.Fatal("successful probe was not accepted")
	}
	if shouldMarkSessionChecked(errors.New("proxy timeout")) {
		t.Fatal("failed network probe was accepted as a session check")
	}
}

// An authorization that no longer works has to be recognized as such, because
// the retry loop that consumes this predicate runs forever otherwise: the
// update hub reconnected on a capped delay with no end, and the account stayed
// marked "authorized" so nothing ever told the person to log in again.
func TestTerminalSessionErrorsStopRetryingAndTransientOnesDoNot(t *testing.T) {
	terminal := []error{
		tgerr.New(401, "SESSION_REVOKED"),
		tgerr.New(401, "SESSION_EXPIRED"),
		tgerr.New(401, "AUTH_KEY_UNREGISTERED"),
		tgerr.New(401, "USER_DEACTIVATED_BAN"),
		tgerr.New(400, "API_ID_INVALID"),
		// The same rejection also arrives wrapped by the transport with no type
		// left to read, which is why the text is matched as well.
		errors.New("rpcDoRequest: rpc error code 401: SESSION_REVOKED"),
	}
	for _, err := range terminal {
		if !isTerminalSessionError(err) {
			t.Errorf("%v was not recognized as terminal", err)
		}
	}
	// Everything a later attempt can outlast must keep being retried. Treating
	// one of these as terminal would log the account out over a network blip.
	transient := []error{
		nil,
		errors.New("connection reset by peer"),
		errors.New("proxy timeout"),
		tgerr.New(420, "FLOOD_WAIT_42"),
		tgerr.New(500, "INTERNAL_SERVER_ERROR"),
		// Duplicated keys resolve once the other client disconnects, so this is
		// deliberately not in the list.
		tgerr.New(406, "AUTH_KEY_DUPLICATED"),
	}
	for _, err := range transient {
		if isTerminalSessionError(err) {
			t.Errorf("%v was wrongly treated as terminal", err)
		}
	}
}

func TestAccountStoreSeparatesSessionAndUpstreamState(t *testing.T) {
	root := t.TempDir()
	store := &accountStore{
		sessionPath: filepath.Join(root, "sessions", "account.session"),
		stateDir:    filepath.Join(root, "state", "account"),
	}
	ctx := context.Background()
	if err := store.Set(ctx, "session", []byte("telegram-session")); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, "resume:download-fingerprint", []byte("upstream-resume")); err != nil {
		t.Fatal(err)
	}
	session, err := store.Get(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	resume, err := store.Get(ctx, "resume:download-fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(session), "telegram-session"; got != want {
		t.Fatalf("session = %q, want %q", got, want)
	}
	if got, want := string(resume), "upstream-resume"; got != want {
		t.Fatalf("resume = %q, want %q", got, want)
	}
	if _, err := os.Stat(store.path("resume:download-fingerprint")); err != nil {
		t.Fatalf("resume state file missing: %v", err)
	}
	if store.path("session") == store.path("resume:download-fingerprint") {
		t.Fatal("session and resume state use the same file")
	}
}

func TestReactionFallbackURLKeepsDialogType(t *testing.T) {
	tests := []struct {
		peer tg.InputPeerClass
		want string
	}{
		{&tg.InputPeerUser{UserID: 9}, "tg://reaction/user/9/7"},
		{&tg.InputPeerChat{ChatID: 9}, "tg://reaction/chat/9/7"},
		{&tg.InputPeerChannel{ChannelID: 9}, "tg://reaction/channel/9/7"},
		{&tg.InputPeerSelf{}, "tg://reaction/self/account/7"},
	}
	for _, test := range tests {
		if got := reactionFallbackURL(test.peer, "account", 7); got != test.want {
			t.Errorf("reactionFallbackURL() = %q, want %q", got, test.want)
		}
	}
}

func TestNormalizeRawSelfPeerWhenEntitiesAreMissing(t *testing.T) {
	m := &Manager{accounts: []Account{{ID: "account-1", TelegramID: 12345, State: "authorized"}}}
	if _, ok := m.normalizeRawSelfPeer("account-1", &tg.PeerUser{UserID: 12345}).(*tg.InputPeerSelf); !ok {
		t.Fatal("current user's raw peer was not normalized to InputPeerSelf")
	}
	if peer := m.normalizeRawSelfPeer("account-1", &tg.PeerUser{UserID: 999}); peer != nil {
		t.Fatal("unrelated user was incorrectly normalized to InputPeerSelf")
	}
}

func TestOwnReactionEmojisOnlyReturnsCurrentAccountStandardEmoji(t *testing.T) {
	chosen := tg.ReactionCount{Reaction: &tg.ReactionEmoji{Emoticon: "❤️"}, Count: 1}
	chosen.SetChosenOrder(1)
	reactions := &tg.MessageReactions{
		RecentReactions: []tg.MessagePeerReaction{
			{My: true, Reaction: &tg.ReactionEmoji{Emoticon: "👍"}},
			{My: false, Reaction: &tg.ReactionEmoji{Emoticon: "👎"}},
			{My: true, Reaction: &tg.ReactionCustomEmoji{}},
		},
		Results: []tg.ReactionCount{
			chosen,
			{Reaction: &tg.ReactionEmoji{Emoticon: "🔥"}, Count: 10},
		},
	}
	got := ownReactionEmojis(reactions)
	if len(got) != 2 || got[0] != "👍" || got[1] != "❤️" {
		t.Fatalf("ownReactionEmojis() = %#v, want [👍 ❤️]", got)
	}
}

// openSessionTestManager builds a manager holding one authorized account.
//
// The account is written before Open reads it, so the manager reaches the same
// state a real one would after a login. The proxy decides which way the
// connection fails - see brokenProxy and unreachableProxy - and in both cases
// it fails locally, so these tests never depend on the network or on a Telegram
// account being available.
func openSessionTestManager(t *testing.T, proxy func() string) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "telegram"), 0o700); err != nil {
		t.Fatal(err)
	}
	const accountID = "session-test-account"
	payload := `{"accounts":[{"id":"` + accountID + `","telegramId":1,"state":"authorized","createdAt":"2026-01-01T00:00:00Z"}],"currentId":"` + accountID + `"}`
	if err := os.WriteFile(filepath.Join(dir, "telegram", "accounts.json"), []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := Open(dir, proxy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m, accountID
}

// brokenProxy cannot even be parsed back into a dialer, so the connection
// cannot be constructed at all. It is the configuration-error case.
func brokenProxy() string { return "://not-a-proxy" }

// unreachableProxy parses, so the connection is constructed - but nothing is
// listening, so it never becomes ready. The session survives that and keeps
// retrying, which is the state the registry tests need to observe. The address
// is a closed port on the loopback interface, so no packet leaves the machine.
func unreachableProxy() string { return "socks5://127.0.0.1:1" }

// Every operation on an account used to build its own client, so two calls
// never shared a connection. The registry is what makes them share one, and
// this is the assertion that the sharing actually happens.
func TestAcquireSessionReusesOneConnectionPerAccount(t *testing.T) {
	m, accountID := openSessionTestManager(t, unreachableProxy)

	first, err := m.acquireSession(accountID)
	if err != nil {
		t.Fatalf("acquireSession(): %v", err)
	}
	second, err := m.acquireSession(accountID)
	if err != nil {
		t.Fatalf("acquireSession() second call: %v", err)
	}
	if first != second {
		t.Fatal("a second operation opened a second connection for the same account")
	}
	m.sessionsMu.Lock()
	live := len(m.sessions)
	m.sessionsMu.Unlock()
	if live != 1 {
		t.Fatalf("registered sessions=%d, want 1", live)
	}
	select {
	case <-first.done:
		t.Fatal("the session ended while it should still be reconnecting")
	default:
	}
}

// The upstream client reads the proxy once, when it builds the connection, so a
// session opened through the old route cannot be reused after the operator
// changes it.
func TestAcquireSessionReplacesConnectionWhenTheProxyChanges(t *testing.T) {
	var mu sync.Mutex
	proxy := unreachableProxy()
	m, accountID := openSessionTestManager(t, func() string {
		mu.Lock()
		defer mu.Unlock()
		return proxy
	})

	first, err := m.acquireSession(accountID)
	if err != nil {
		t.Fatalf("acquireSession(): %v", err)
	}
	mu.Lock()
	proxy = "socks5://127.0.0.1:2"
	mu.Unlock()

	second, err := m.acquireSession(accountID)
	if err != nil {
		t.Fatalf("acquireSession() after a proxy change: %v", err)
	}
	if second == first {
		t.Fatal("the session survived a proxy change and would keep using the old route")
	}
	select {
	case <-first.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced session was never stopped")
	}
}

// Removal has to be able to wait for the connection, because the session writes
// the session and state files back through its own store and Delete removes
// those files immediately afterwards.
func TestStopSessionEndsTheConnectionAndClearsTheRegistry(t *testing.T) {
	m, accountID := openSessionTestManager(t, unreachableProxy)

	session, err := m.acquireSession(accountID)
	if err != nil {
		t.Fatalf("acquireSession(): %v", err)
	}
	m.stopSession(accountID)
	select {
	case <-session.done:
	default:
		t.Fatal("stopSession returned with the connection still running")
	}
	m.sessionsMu.Lock()
	live := len(m.sessions)
	m.sessionsMu.Unlock()
	if live != 0 {
		t.Fatalf("registered sessions=%d after stop, want 0", live)
	}
}

// An operation for an account the store no longer has says so, instead of
// reporting a login that has already finished.
//
// Account ids are minted per sign-in, so removing an account and adding it back
// leaves every task it created pointing at an id nothing can serve - the shape
// a user reaches by revoking a session and signing in again. Both halves of the
// answer matter: the operation must fail rather than wait for a session that
// will never be built, and what it fails with has to name the account, because
// "尚未登录完成" is advice to go and finish a login that is not the problem.
func TestRunNamesAnAccountThatIsNoLongerThere(t *testing.T) {
	m, accountID := openSessionTestManager(t, unreachableProxy)

	err := m.Run(context.Background(), accountID+"-gone", func(context.Context, *gotd.Client, kv.Storage) error {
		t.Fatal("the operation ran against an account that does not exist")
		return nil
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Run() = %v; want it to name the missing account, not a login still in progress", err)
	}
	if errors.Is(err, ErrNotAuthorized) {
		t.Fatal("a missing account reported itself as an unfinished login")
	}
}

// A connection that cannot be constructed has to reach the caller as an error
// rather than become a wait.
//
// Retrying it behind the caller's back is the failure mode this guards: the
// session loop never gives up, so a caller would wait for a session that can
// never become ready - and several call sites pass a context with no deadline,
// for which that wait is forever. The context here is deliberately unbounded
// for that reason.
func TestRunReportsAnUnbuildableConnectionInsteadOfWaitingForever(t *testing.T) {
	m, accountID := openSessionTestManager(t, brokenProxy)

	called := false
	done := make(chan error, 1)
	go func() {
		done <- m.Run(context.Background(), accountID, func(context.Context, *gotd.Client, kv.Storage) error {
			called = true
			return nil
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() succeeded without a connection")
		}
		if !strings.Contains(err.Error(), "创建 Telegram 连接") {
			t.Fatalf("Run() = %v; want the failure to name the connection it could not build", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run() never returned for a connection that cannot be built")
	}
	if called {
		t.Fatal("the operation ran against a connection that was never established")
	}
}

// The validity probe rides the account's own connection, so an account with
// work in flight is no longer excluded from being checked.
//
// The read lease held here is the one a running download holds for its whole
// duration. Under the exclusive lease the probe used to take, TryLock could not
// succeed while any operation was running, so the check was skipped for exactly
// the accounts whose sessions are most likely to have been revoked. The
// observable is the connection appearing in the registry: the probe has to have
// reached the account to put it there.
func TestCheckSessionsProbesWhileAnOperationHoldsTheReadLease(t *testing.T) {
	m, accountID := openSessionTestManager(t, unreachableProxy)

	operation := m.operation(accountID)
	operation.RLock()
	defer operation.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		m.checkSessions(ctx)
	}()

	deadline := time.After(5 * time.Second)
	for {
		m.sessionsMu.Lock()
		live := len(m.sessions)
		m.sessionsMu.Unlock()
		if live == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("checkSessions never reached the account's connection while a read lease was held")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("checkSessions did not return after its context was cancelled")
	}
}

// A caller waiting for the connection is woken by exactly one thing: a close on
// the channel it read. So the channel that is replaced when the connection
// drops has to be closed, not discarded.
//
// Discarding it strands the caller on a channel nothing will ever close, while
// the reconnection it is waiting for closes the new one. The sequence that
// produces it is ordinary - an attempt fails, the next one succeeds - and the
// consequence is not confined to the caller: it waits while holding the
// account's read lease, so account removal queues behind it too. This pins the
// contract directly rather than trying to win a race against the scheduler.
func TestMarkDisconnectedClosesTheChannelAWaiterHolds(t *testing.T) {
	session := &accountSession{done: make(chan struct{}), readyCh: make(chan struct{})}
	held := session.readyCh
	session.markDisconnected(errors.New("connection reset by peer"))

	select {
	case <-held:
	default:
		t.Fatal("markDisconnected replaced the readiness channel without closing it; a caller already waiting on it is never woken by the next connection")
	}
	if session.readyCh == held {
		t.Fatal("the replaced channel is still the current one, so no new readiness signal was armed")
	}
}

// The same contract in the shape it actually occurs: a caller parked before a
// failed attempt has to be woken by the connection that follows it.
func TestWaitReadyIsWokenByTheReconnectionAfterAFailedAttempt(t *testing.T) {
	session := &accountSession{done: make(chan struct{}), readyCh: make(chan struct{})}
	client := gotd.NewClient(1, "0123456789abcdef0123456789abcdef", gotd.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	woke := make(chan *gotd.Client, 1)
	go func() {
		ready, err := session.waitReady(ctx)
		if err != nil {
			woke <- nil
			return
		}
		woke <- ready
	}()

	// Long enough that the caller above has read its state and is parked. A
	// caller that had not yet done so would read the re-armed channel and pass
	// either way, which is the case this delay rules out.
	time.Sleep(50 * time.Millisecond)
	session.markDisconnected(errors.New("connection reset by peer"))
	session.markReady(client)

	select {
	case ready := <-woke:
		if ready != client {
			t.Fatal("waitReady() returned without the reconnected client")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitReady() was never woken by the connection that replaced the failed one")
	}
}

// Starting a login while another is still waiting for its two-factor password
// must report the one in flight rather than begin a second. The two states it
// used to list were both about the QR code; the password step moves the account
// to a third state, and that step waits for a person, so the window was as long
// as someone took to type. A second login means a second account, and a
// Telegram user with two authorized sessions is the one thing the whole guard
// exists to prevent.
func TestPendingLoginCoversTheTwoFactorStep(t *testing.T) {
	// Exercised through every state a login passes through, because the
	// question is not what the account is called but whether a login is still
	// running - so a state added later must not reopen the window.
	for _, state := range []string{"starting", "waiting_for_qr", "waiting_for_2fa"} {
		m := &Manager{
			accounts: []Account{{ID: "account-1", State: state}},
			jobs:     map[string]*loginJob{"account-1": {}},
		}
		m.mu.Lock()
		pending, ok := m.pendingLoginLocked()
		m.mu.Unlock()
		if !ok || pending.ID != "account-1" {
			t.Fatalf("state %q: a login in flight was not reported, so a second one would be started", state)
		}
	}
}

// The other direction, and the one a state list got wrong in the opposite way:
// an account that is merely sitting in a login-shaped state with no login
// running is not a login in flight. A restart reloads an interrupted login as
// 'stopped', which must not block starting a new one.
func TestPendingLoginIgnoresAccountsWithNoLoginRunning(t *testing.T) {
	m := &Manager{
		accounts: []Account{{ID: "account-1", State: "waiting_for_2fa"}, {ID: "account-2", State: "stopped"}},
		jobs:     map[string]*loginJob{},
	}
	m.mu.Lock()
	_, ok := m.pendingLoginLocked()
	m.mu.Unlock()
	if ok {
		t.Fatal("an account with no login job was reported as a login in flight, which would block every new login")
	}
}

// Telegram sends the whole message in the update that announces it. Keeping
// only its id and asking for the message back is one history request per
// received message - the largest avoidable source of requests this application
// had, and the reason a busy listener could not be paced.
//
// The field is asserted here rather than the read that consumes it: the
// consumer needs a live account, and what can be pinned without one is that the
// message survives the trip from the dispatcher to the event.
func TestNewMessageEventCarriesTheMessageTheUpdateDelivered(t *testing.T) {
	message := &tg.Message{ID: 4242, Message: "a post"}
	peer := &tg.InputPeerChannel{ChannelID: 5, AccessHash: 9}

	event := NewMessageEventFor("account-1", "channel:5", "a channel", 5, message, peer)
	if event.Message != message {
		t.Fatal("the event dropped the message, so its consumer has to read it back from Telegram")
	}
	if event.MessageID != message.ID {
		t.Fatalf("the event names message %d but carried %d", event.MessageID, message.ID)
	}

	// A synthesised event stands in for an update the application did not
	// receive - a message recovered from a history page - and is the one case
	// where there is nothing to carry. It must still be usable.
	empty := NewMessageEventFor("account-1", "channel:5", "a channel", 5, nil, peer)
	if empty.Message != nil || empty.MessageID != 0 {
		t.Fatalf("an event built without a message invented one: %+v", empty)
	}
}
