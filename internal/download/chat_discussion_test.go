package download

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// A channel's linked discussion group is recorded under the same dialog key an
// update from that group carries, so admission can match the two with a map
// lookup and no request. The key is built from the linked chat id when the link
// is resolved and from the peer when the update arrives; this pins the two
// derivations together, because a mismatch would silently stop a listening task
// from ever receiving its own comments - the exact failure the record exists to
// prevent.
func TestDiscussionGroupKeyMatchesUpdateDialogKey(t *testing.T) {
	const groupID = 2391368210
	_, fromPeer, _ := dialogIdentity(&tg.InputPeerChannel{ChannelID: groupID}, "account")
	fromLinkedChatID := fmt.Sprintf("channel:%d", groupID)
	if fromLinkedChatID != fromPeer {
		t.Fatalf("linked group key %q does not match the key an update from it carries (%q)", fromLinkedChatID, fromPeer)
	}
}

// Telegram states a rejection in a 400, and the retry budget must not be spent
// on it: the answer is about the request, not about when it was made. The
// wrapper below is the one the inbox actually stores - upstream's retry
// middleware wraps whatever it declines to retry - so this also pins that the
// classification unwraps to the RPC error instead of reading the outermost text.
func TestPermanentInboxErrorClassifiesRejections(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// The same rejection, reached through the wrapping the inbox stores. This
		// is the case that pins the unwrapping rather than the outermost text.
		{"rejection through the middleware wrapper", fmt.Errorf("retry middleware skip: %w", tgerr.New(400, "CHANNEL_INVALID")), true},
		// Text alone carries no type, so it cannot be classified. Retrying is the
		// safe direction: an event that could never succeed costs a few requests,
		// one wrongly declared permanent is a download the user asked for and
		// will not get.
		{"rejection recorded as text only", errors.New("callback: rpcDoRequest: rpc error code 400: CHANNEL_INVALID"), false},
		{"channel invalid", tgerr.New(400, "CHANNEL_INVALID"), true},
		{"message gone", tgerr.New(400, "MSG_ID_INVALID"), true},
		{"peer unreachable", tgerr.New(400, "PEER_ID_INVALID"), true},
		{"wrapped in our own context", fmt.Errorf("读取讨论根消息: %w", tgerr.New(400, "CHANNEL_INVALID")), true},
		{"flood wait", tgerr.New(420, "FLOOD_WAIT_42"), false},
		{"slow mode", tgerr.New(400, "SLOWMODE_WAIT_9"), false},
		{"account flagged", tgerr.New(400, "PEER_FLOOD"), false},
		{"server hiccup", tgerr.New(400, "MSG_WAIT_FAILED"), false},
		{"not a Telegram error", errors.New("connection reset by peer"), false},
		{"database failure", errInboxUnavailable, false},
		{"server error", tgerr.New(500, "INTERNAL_SERVER_ERROR"), false},
	}
	for _, testCase := range cases {
		if got := permanentInboxError(testCase.err); got != testCase.want {
			t.Fatalf("%s: got %v, want %v", testCase.name, got, testCase.want)
		}
	}
}
