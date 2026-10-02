package telegram

import (
	"context"
	"time"

	"github.com/gotd/td/bin"
	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// resolveUsernameTTL is how long one username resolution is reused.
//
// contacts.resolveUsername is the request this application makes most often and
// is throttled hardest: every link submission, every session-task target and
// every post forwarded to the Bot resolves a name, and each one was a fresh
// request because gotd's peers.Manager is rebuilt per call and never answers
// from memory. A long window of those is what put the account into the
// server-side wait that made every resolution - and, behind it, every reaction -
// queue. Reusing an answer for an hour is conservative in the direction that
// matters: the peers storage already keeps resolved access hashes forever, so a
// bounded lifetime is strictly shorter than what the rest of the application
// already trusts.
const resolveUsernameTTL = time.Hour

// resolveUsernameMemory bounds the cache. An entry is a name and one or two
// entities, so the bound exists to keep a Bot being fed invented usernames from
// growing the process.
const resolveUsernameMemory = 256

type resolvedUsername struct {
	peer *tg.ContactsResolvedPeer
	at   time.Time
}

// usernameCacheMiddleware answers contacts.resolveUsername from memory for this
// account.
//
// It sits in the invoker chain rather than at a call site because every caller
// needs it and none of them should have to remember: the link parser, the
// session-task resolver, the Bot's own two resolutions per forwarded post. A
// cache at any one of those would leave the others spending the same request on
// the same name.
//
// The cache is per account, because an access hash belongs to the account that
// resolved it, and a result found for another account would be refused.
func (m *Manager) usernameCacheMiddleware(accountID string) gotd.Middleware {
	return gotd.MiddlewareFunc(func(next tg.Invoker) gotd.InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			request, ok := input.(*tg.ContactsResolveUsernameRequest)
			if !ok {
				return next.Invoke(ctx, input, output)
			}
			resolved, ok := output.(*tg.ContactsResolvedPeer)
			if !ok {
				// A different result shape means this build of gotd encodes the
				// answer differently. Pass it through rather than guess at it.
				return next.Invoke(ctx, input, output)
			}
			if cached, ok := m.cachedUsername(accountID, request.Username); ok {
				*resolved = *cached
				return nil
			}
			if err := next.Invoke(ctx, input, output); err != nil {
				return err
			}
			m.rememberUsername(accountID, request.Username, resolved)
			return nil
		}
	})
}

func (m *Manager) cachedUsername(accountID, username string) (*tg.ContactsResolvedPeer, bool) {
	m.resolveMu.Lock()
	defer m.resolveMu.Unlock()
	entry, ok := m.resolvedNames[accountID+"\x00"+username]
	if !ok || time.Since(entry.at) >= resolveUsernameTTL {
		return nil, false
	}
	return entry.peer, true
}

func (m *Manager) rememberUsername(accountID, username string, peer *tg.ContactsResolvedPeer) {
	if peer == nil || peer.Peer == nil {
		return
	}
	// The decoded result is copied rather than kept, because the caller owns the
	// value it was written into and may hold on to it.
	copied := *peer
	now := time.Now()
	m.resolveMu.Lock()
	defer m.resolveMu.Unlock()
	if m.resolvedNames == nil {
		m.resolvedNames = map[string]resolvedUsername{}
	}
	for key, entry := range m.resolvedNames {
		if now.Sub(entry.at) >= resolveUsernameTTL {
			delete(m.resolvedNames, key)
		}
	}
	if len(m.resolvedNames) >= resolveUsernameMemory {
		oldestKey, oldestAt := "", now
		for key, entry := range m.resolvedNames {
			if oldestKey == "" || entry.at.Before(oldestAt) {
				oldestKey, oldestAt = key, entry.at
			}
		}
		delete(m.resolvedNames, oldestKey)
	}
	m.resolvedNames[accountID+"\x00"+username] = resolvedUsername{peer: &copied, at: now}
}

// forgetUsernames drops every resolution remembered for one account. Nothing
// calls it in the normal course of events - an entry expires on its own, and a
// stale access hash is answered by Telegram rather than trusted blindly - but an
// account being removed must not leave its access hashes in memory.
func (m *Manager) forgetUsernames(accountID string) {
	prefix := accountID + "\x00"
	m.resolveMu.Lock()
	defer m.resolveMu.Unlock()
	for key := range m.resolvedNames {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(m.resolvedNames, key)
		}
	}
}
