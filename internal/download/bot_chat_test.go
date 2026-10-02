package download

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"
)

// The Bot API numbers a bot chat from the Bot's side, and that numbering is not
// the one the account sees - so the message a forward was made from has to be
// located by the send time the update reported. Two things must never happen:
// picking a message the Bot sent (every task card would look like a download),
// and picking a different message than the one the update described (the task
// would download the wrong file under the right name).
func TestNewestOwnMessageFindsTheMessageTheUpdateDescribed(t *testing.T) {
	const sentAt = int64(1759406176)
	own := func(id, date int) *tg.Message { return &tg.Message{ID: id, Date: date, Out: true} }
	theirs := func(id, date int) *tg.Message { return &tg.Message{ID: id, Date: date, Out: false} }

	cases := []struct {
		name     string
		messages []tg.MessageClass
		wantID   int
	}{
		{
			name:     "the message the account sent at that second",
			messages: []tg.MessageClass{theirs(40, int(sentAt)), own(39, int(sentAt))},
			wantID:   39,
		},
		{
			name:     "a card the Bot sent at the same second is not the message",
			messages: []tg.MessageClass{theirs(40, int(sentAt))},
		},
		{
			name:     "an unrelated message from another minute is not it",
			messages: []tg.MessageClass{own(39, int(sentAt)-600)},
		},
		{
			name:     "a second of clock skew is still the same message",
			messages: []tg.MessageClass{own(39, int(sentAt)+1)},
			wantID:   39,
		},
		{
			name: "an exact match wins over a nearer-in-the-page one that does not agree",
			messages: []tg.MessageClass{
				own(41, int(sentAt)+2),
				own(39, int(sentAt)),
			},
			wantID: 39,
		},
		{
			name: "the newest of several forwards stamped in the same second",
			messages: []tg.MessageClass{
				own(38, int(sentAt)),
				own(40, int(sentAt)),
			},
			wantID: 40,
		},
		{name: "an empty page locates nothing", messages: nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := newestOwnMessage(test.messages, sentAt)
			if test.wantID == 0 {
				if got != nil {
					t.Fatalf("newestOwnMessage() = %d, want nothing: a substitution would download the wrong file", got.ID)
				}
				return
			}
			if got == nil || got.ID != test.wantID {
				t.Fatalf("newestOwnMessage() = %v, want message %d", got, test.wantID)
			}
		})
	}
}

// Without a username there is no chat to read, and the caller has to hear that
// rather than be handed an empty submission.
func TestSubmitBotChatMessageNeedsAUsername(t *testing.T) {
	if _, err := (&Manager{}).SubmitBotChatMessage(context.Background(), BotChatIntent{BotUsername: "  "}); err == nil {
		t.Fatal("a submission with no chat to read must not be attempted")
	}
}
