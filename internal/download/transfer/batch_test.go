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

// Renewing an expired reference costs one request for the window, not one for
// each file in it.
//
// A file reference expires by age, and the messages of a window were all read at
// the same moment, so a task that waited in the queue has every reference in
// that window spent at once. Repairing them one at a time is a request per file
// - the per-message path this source exists to replace - and it arrives at the
// moment the task has the most left to get through. The test states the saving
// as the next file costing nothing: the window was read again, so the file
// behind the repaired one is already fresh.
func TestBatchSourceRenewsAWholeWindowInOneRequest(t *testing.T) {
	ids := idsFrom(1, 5)
	api := newFakeAPI()
	api.file = byteFile(4000)
	fillMessages(api, ids)

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	if _, err := source.Message(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	requireCount(t, api, "MessagesGetMessagesRequest", 1)
	loaded := source.cachedAt(t, 1)

	// The clock has to move for a rewritten entry to be distinguishable from
	// the one the first read left.
	time.Sleep(2 * time.Millisecond)
	if _, err := source.Refresh(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	requireCount(t, api, "MessagesGetMessagesRequest", 2)
	requireCount(t, api, "MessagesGetHistoryRequest", 0)

	// Every message of the window was read again. This is the assertion that
	// separates one request for the window from one request for the message:
	// the files behind this one are handed out from the cache, so a repair that
	// only rewrote the message asked for would leave them holding the reference
	// that just expired and would send them to the network one by one.
	for _, id := range ids {
		if rewritten := source.cachedAt(t, id); !rewritten.After(loaded) {
			t.Errorf("message %d still holds the entry from before the repair (%s, loaded %s)", id, rewritten, loaded)
		}
	}
	if counts := source.counts(); counts.batchCalls != 2 || counts.singleCalls != 0 {
		t.Fatalf("the source reports %+v, want two batch reads and no single reads", counts)
	}
}

// cachedAt is when the source read this message, for a test that has to tell a
// rewritten entry from the one an earlier read left behind.
func (s *batchSource) cachedAt(t *testing.T, id int) time.Time {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cached[id]
	if !ok {
		t.Fatalf("message %d is not cached", id)
	}
	return entry.fetched
}

// A source that has fallen back to single reads has no window to re-read, so
// the repair stays the one-message read it has always been on that path.
func TestBatchSourceRefreshesOneMessageWhenDegraded(t *testing.T) {
	ids := idsFrom(1, 5)
	api := newFakeAPI()
	api.file = byteFile(4000)
	fillMessages(api, ids)
	api.failBatch = true

	source := newBatchSource(&fakePool{client: tg.NewClient(api)}, testPeer, ids)
	if _, err := source.Message(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	api.failBatch = false
	if _, err := source.Refresh(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	// One refused batch attempt, then the per-message read for the first fetch
	// and another for the repair.
	requireCount(t, api, "MessagesGetMessagesRequest", 1)
	requireCount(t, api, "MessagesGetHistoryRequest", 2)
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
