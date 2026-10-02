package transfer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/tmsg"
)

// The request this replaces was one messages.getHistory per message. For a
// session task holding thousands of files that is thousands of round trips
// before any file can be transferred, and it is why the account's metadata
// traffic could not be paced: at four requests a second the budget would have
// defined the download's throughput.
func TestBatchSourceReadsOneRequestPerHundredMessages(t *testing.T) {
	ids := idsFrom(1, 250)
	api := newFakeAPI()
	fillMessages(api, ids)

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	for _, id := range ids {
		message, err := source.Message(context.Background(), id)
		if err != nil {
			t.Fatalf("message %d: %v", id, err)
		}
		if message.ID != id {
			t.Fatalf("asked for %d and received %d", id, message.ID)
		}
	}

	// 250 ids in windows of 100 is three requests.
	requireCount(t, api, "MessagesGetMessagesRequest", 3)
	requireCount(t, api, "MessagesGetHistoryRequest", 0)

	counts := source.counts()
	if counts.batchCalls != 3 || counts.singleCalls != 0 {
		t.Fatalf("the source reports %+v", counts)
	}
}

// A message that is not in the response is a message that is gone. It has to
// read as the same answer the per-message reader gives, because that answer is
// what makes a task skip the post instead of failing.
func TestBatchSourceReportsMissingMessagesAsDeleted(t *testing.T) {
	ids := idsFrom(1, 5)
	api := newFakeAPI()
	fillMessages(api, ids)
	api.missing[3] = true

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	for _, id := range ids {
		_, err := source.Message(context.Background(), id)
		if id == 3 {
			if !errors.Is(err, tmsg.ErrMessageDeleted) {
				t.Fatalf("a missing message answered %v, want ErrMessageDeleted", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("message %d: %v", id, err)
		}
	}
}

// A batch request that fails costs speed and nothing else: the source falls
// back to one request per message, which is exactly what the previous
// implementation did. Retrying the batch per window would spend more requests
// than the path it replaced.
func TestBatchSourceDegradesToPerMessageReads(t *testing.T) {
	ids := idsFrom(1, 4)
	api := newFakeAPI()
	fillMessages(api, ids)
	api.failBatch = true

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	for _, id := range ids {
		if _, err := source.Message(context.Background(), id); err != nil {
			t.Fatalf("message %d: %v", id, err)
		}
	}

	counts := source.counts()
	if counts.batchCalls != 0 {
		t.Fatalf("a failed batch request was counted as a success: %+v", counts)
	}
	if counts.singleCalls != len(ids) {
		t.Fatalf("the fallback read %d messages one at a time, want %d", counts.singleCalls, len(ids))
	}
	// One refusal, then one request per message. A second batch attempt would
	// mean the degradation is not sticky.
	requireCount(t, api, "MessagesGetMessagesRequest", 1)
	requireCount(t, api, "MessagesGetHistoryRequest", len(ids))
}

// A window is fetched when its first id is wanted, but its ids are handed out
// one file at a time. A slow transfer can leave the last id of a window unused
// for a long while, and a message's file reference expires - so a window that
// has aged out is re-read rather than trusted.
func TestBatchSourceRereadsAWindowThatAgedOut(t *testing.T) {
	ids := idsFrom(1, 3)
	api := newFakeAPI()
	fillMessages(api, ids)

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	source.ttl = time.Millisecond

	if _, err := source.Message(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	requireCount(t, api, "MessagesGetMessagesRequest", 1)

	time.Sleep(5 * time.Millisecond)

	if _, err := source.Message(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	// Still batched: the re-read is one request for the window, not one per
	// message.
	requireCount(t, api, "MessagesGetMessagesRequest", 2)
	if got := source.counts().singleCalls; got != 0 {
		t.Fatalf("an aged window cost %d per-message reads, want 0", got)
	}
}

// A channel's messages are addressed through the channel; every other dialog
// resolves bare ids, because for those the ids come from the account's own
// message sequence.
func TestBatchSourceAddressesChannelsThroughTheChannel(t *testing.T) {
	ids := []int{10, 11}
	api := newFakeAPI()
	fillMessages(api, ids)
	pool := &fakePool{client: tg.NewClient(api)}

	channel := &tg.InputPeerChannel{ChannelID: 99, AccessHash: 5}
	source := newBatchSource(pool, channel, ids)
	if _, err := source.Message(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	requireCount(t, api, "ChannelsGetMessagesRequest", 1)
	requireCount(t, api, "MessagesGetMessagesRequest", 0)

	user := newBatchSource(pool, testPeer, ids)
	if _, err := user.Message(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	requireCount(t, api, "MessagesGetMessagesRequest", 1)
	requireCount(t, api, "ChannelsGetMessagesRequest", 1)
}

// An id the source was not built for is not guessed at: it is read the way the
// previous implementation read everything, so an unexpected id costs a request
// rather than an answer that might be wrong.
func TestBatchSourceFallsBackForAnUnknownId(t *testing.T) {
	ids := []int{1, 2}
	api := newFakeAPI()
	fillMessages(api, ids)
	api.messages[77] = documentMessage(77, 1700000000, 0)

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	message, err := source.Message(context.Background(), 77)
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != 77 {
		t.Fatalf("received message %d", message.ID)
	}
	if got := source.counts().singleCalls; got != 1 {
		t.Fatalf("an unexpected id cost %d reads, want 1", got)
	}
}

// The engine hands out ids in order, and the window is computed from that
// order, so a repeated id must not move the window backwards.
func TestBatchSourceKeepsTheFirstPositionOfARepeatedId(t *testing.T) {
	ids := []int{1, 2, 3, 2, 4}
	api := newFakeAPI()
	fillMessages(api, []int{1, 2, 3, 4})

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	for _, id := range ids {
		if _, err := source.Message(context.Background(), id); err != nil {
			t.Fatalf("message %d: %v", id, err)
		}
	}
	requireCount(t, api, "MessagesGetMessagesRequest", 1)
}
