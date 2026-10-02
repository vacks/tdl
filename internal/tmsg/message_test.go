package tmsg

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

// historyInvoker answers a history read the way a test needs it to.
type historyInvoker struct {
	mu      sync.Mutex
	calls   int
	fail    error
	entries []tg.MessageClass
}

func (h *historyInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	h.mu.Lock()
	h.calls++
	fail := h.fail
	entries := h.entries
	h.mu.Unlock()
	if fail != nil {
		return fail
	}
	box, ok := output.(*tg.MessagesMessagesBox)
	if !ok {
		return errors.New("unexpected output")
	}
	box.Messages = &tg.MessagesMessages{Messages: entries}
	return nil
}

func (h *historyInvoker) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// A read that failed is not a message that is gone.
//
// The account reaches Telegram through a proxy, and that link drops. Answering
// "deleted" for a refused request turns a blip into a permanent verdict: the
// download path treats a deleted message as an answer no later attempt can
// change, settles the file as failed, and stops - and the reason it prints is
// one that was never true. Observed on the development instance: a batch read
// refused with "retry limit reached", the per-message fallback asked again, and
// the file was recorded as 消息已删除.
func TestAReadThatFailedIsNotADeletedMessage(t *testing.T) {
	refused := errors.New("invoke pool: rpcDoRequest: retryUntilAck: retry limit reached after 5 attempts")
	invoker := &historyInvoker{fail: refused}

	_, err := GetSingleMessage(context.Background(), tg.NewClient(invoker), &tg.InputPeerChannel{ChannelID: 1, AccessHash: 1}, 7)
	if err == nil {
		t.Fatal("a refused read returned a message")
	}
	if errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("a refused read was reported as a deleted message: %v", err)
	}
	if !errors.Is(err, refused) {
		t.Fatalf("the refusal was lost: %v", err)
	}
	if invoker.count() != 1 {
		t.Fatalf("the read was attempted %d times, want 1", invoker.count())
	}
}

// The other half, which must not be lost to the fix above: an empty page really
// does mean the message is gone, and it is an answer rather than a failure.
func TestAnEmptyPageIsADeletedMessage(t *testing.T) {
	invoker := &historyInvoker{}

	_, err := GetSingleMessage(context.Background(), tg.NewClient(invoker), &tg.InputPeerChannel{ChannelID: 1, AccessHash: 1}, 7)
	if !errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("an empty page reports %v, want ErrMessageDeleted", err)
	}
}

// A page whose first entry is not the message asked for means the message is
// not there, which is the same answer as an empty page.
func TestAPageStartingBelowTheTargetIsADeletedMessage(t *testing.T) {
	invoker := &historyInvoker{entries: []tg.MessageClass{
		&tg.Message{ID: 5, Date: 1, Flags: bin.Fields(1 << 9)},
	}}

	_, err := GetSingleMessage(context.Background(), tg.NewClient(invoker), &tg.InputPeerChannel{ChannelID: 1, AccessHash: 1}, 7)
	if !errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("a page that skipped the target reports %v, want ErrMessageDeleted", err)
	}
}

// An album walk that stopped early found part of an album, and reporting
// success over it downloads some of the files while saying it downloaded all.
func TestAnAlbumWalkThatFailedIsNotAShortAlbum(t *testing.T) {
	refused := errors.New("read tcp: connection reset by peer")
	invoker := &historyInvoker{fail: refused}
	message := &tg.Message{ID: 20, Date: 1, Flags: bin.Fields(1 << 9)}
	message.SetGroupedID(99)

	_, err := GetGroupedMessages(context.Background(), tg.NewClient(invoker), &tg.InputPeerChannel{ChannelID: 1, AccessHash: 1}, message)
	if !errors.Is(err, refused) {
		t.Fatalf("a refused album walk reports %v, want the refusal", err)
	}
}
