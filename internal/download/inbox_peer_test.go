package download

import (
	"fmt"
	"testing"

	"github.com/gotd/td/tg"
)

// Every kind an inbox row can be admitted with has to rebuild a peer.
//
// The reaction inbox stores the peer of the dialog the reaction arrived in, and
// a reaction in a private chat arrives as an InputPeerUser. A kind this cannot
// build is not a smaller peer: the claim that reads the row refuses it and rolls
// the transaction back, so the row stays pending for ever and every event behind
// it waits. Merging this with the session inbox's copy - which never produces a
// user - dropped the case silently, because a missing branch is not a compile
// error.
func TestEveryInboxPeerKindRebuildsAPeer(t *testing.T) {
	cases := []struct {
		kind string
		want tg.InputPeerClass
	}{
		{"self", &tg.InputPeerSelf{}},
		{"user", &tg.InputPeerUser{UserID: 5, AccessHash: 9}},
		{"chat", &tg.InputPeerChat{ChatID: 5}},
		{"channel", &tg.InputPeerChannel{ChannelID: 5, AccessHash: 9}},
	}
	for _, testCase := range cases {
		got, want := inboxPeer(testCase.kind, 5, 9), testCase.want
		// Compared field by field: two input peers that describe the same chat
		// are different pointers.
		if fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", want) {
			t.Errorf("inboxPeer(%q) = %#v, want %#v: a row admitted with this kind can never be claimed",
				testCase.kind, got, want)
		}
	}
	if got := inboxPeer("nonsense", 1, 2); got != nil {
		t.Errorf("an unknown kind built %#v", got)
	}
}
