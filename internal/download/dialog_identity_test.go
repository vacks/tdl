package download

import (
	"testing"

	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
)

// dialogIdentityForPeer may only refine the display kind. The identity key must
// keep coming from dialogIdentity, otherwise deduplication and direct-peer
// round-trips would change meaning for the refined kinds.
func TestDialogIdentityForPeerKinds(t *testing.T) {
	manager := &peers.Manager{}

	tests := []struct {
		name     string
		peer     peers.Peer
		wantKind string
		wantKey  string
		wantID   int64
	}{
		{
			name:     "bot keeps the user identity key",
			peer:     manager.User(&tg.User{ID: 42, Bot: true}),
			wantKind: "bot",
			wantKey:  "user:42",
			wantID:   42,
		},
		{
			name:     "plain user is not a bot",
			peer:     manager.User(&tg.User{ID: 42}),
			wantKind: "user",
			wantKey:  "user:42",
			wantID:   42,
		},
		{
			name:     "broadcast channel stays a channel",
			peer:     manager.Channel(&tg.Channel{ID: 7, Broadcast: true}),
			wantKind: "channel",
			wantKey:  "channel:7",
			wantID:   7,
		},
		{
			name:     "supergroup is reported as chat",
			peer:     manager.Channel(&tg.Channel{ID: 7}),
			wantKind: "chat",
			wantKey:  "channel:7",
			wantID:   7,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, key, id := dialogIdentityForPeer(test.peer, "acct")
			if kind != test.wantKind {
				t.Errorf("kind = %q, want %q", kind, test.wantKind)
			}
			if key != test.wantKey {
				t.Errorf("key = %q, want %q", key, test.wantKey)
			}
			if id != test.wantID {
				t.Errorf("id = %d, want %d", id, test.wantID)
			}
		})
	}
}
