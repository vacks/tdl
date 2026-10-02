package telegram

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/gotd/td/bin"
	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// The account's request tally answers one question that nothing else in this
// process could: how many Telegram requests did we actually make, and of what
// kind?
//
// It exists because the count is invisible everywhere else. A flood window does
// not surface as an error - the flood-wait middleware sleeps it off and retries
// inside the invoker - so an account being throttled looks exactly like an
// account being slow, and "we are making too many requests" has never been a
// statement anyone could check. The gate next door already paces and times the
// requests it sees; this counts them, including the ones the gate does not pace
// (update-state sync, config, connection setup) and which no other code path
// observes at all.
//
// It counts wire requests, not calls: it is installed innermost, so a request
// answered from the username cache is not counted. That is the number that
// matters for a budget - what Telegram saw.
type rpcTally struct {
	mu     sync.Mutex
	counts map[string]map[string]int64 // account id -> method -> count
}

// rpcTallyMiddleware counts every request that reaches the wire for this
// account.
func (m *Manager) rpcTallyMiddleware(accountID string) gotd.Middleware {
	return gotd.MiddlewareFunc(func(next tg.Invoker) gotd.InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			m.tally.record(accountID, rpcMethodName(input))
			return next.Invoke(ctx, input, output)
		}
	})
}

func (t *rpcTally) record(accountID, method string) {
	if accountID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	account := t.counts[accountID]
	if account == nil {
		account = make(map[string]int64)
		if t.counts == nil {
			t.counts = make(map[string]map[string]int64)
		}
		t.counts[accountID] = account
	}
	account[method]++
}

// snapshot returns the counts as account id -> method -> count, ordered by
// method name so two reads of an unchanged tally compare equal.
func (t *rpcTally) snapshot() map[string]map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]map[string]int64, len(t.counts))
	for accountID, counts := range t.counts {
		methods := make(map[string]int64, len(counts))
		for method, count := range counts {
			methods[method] = count
		}
		out[accountID] = methods
	}
	return out
}

func (t *rpcTally) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counts = nil
}

// RPCTally reports the Telegram requests each account has made since the
// process started, keyed by account id and then by request type.
//
// It is a diagnostic surface, not application state: it is read by an operator
// or by a test comparing two runs, and it is safe to reset at any time.
func (m *Manager) RPCTally() map[string]map[string]int64 {
	return m.tally.snapshot()
}

// ResetRPCTally discards every count. It is what makes the tally usable as a
// measurement: reset, do one thing, read what it cost.
func (m *Manager) ResetRPCTally() {
	m.tally.reset()
}

// rpcMethodName names a request the way an operator would say it: the TL type
// without the package qualifier. The type is the only name available - the
// request value itself is opaque at this layer.
func rpcMethodName(input bin.Encoder) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", input), "*tg.")
}

// rpcTallySummary renders one account's counts as "method=count" pairs, most
// requested first, for a log line that stays readable when there are thirty
// methods.
func rpcTallySummary(counts map[string]int64) string {
	type entry struct {
		method string
		count  int64
	}
	entries := make([]entry, 0, len(counts))
	for method, count := range counts {
		entries = append(entries, entry{method: method, count: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].method < entries[j].method
	})
	var b strings.Builder
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%d", e.method, e.count)
	}
	return b.String()
}
