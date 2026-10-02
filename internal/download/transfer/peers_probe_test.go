package transfer

import (
	"context"
	"fmt"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/kv"
)

// channelAPI answers channels.getChannels with one channel.
type channelAPI struct {
	calls int
	id    int64
}

func (c *channelAPI) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	switch input.(type) {
	case *tg.ChannelsGetChannelsRequest:
		c.calls++
		box, ok := output.(*tg.MessagesChatsBox)
		if !ok {
			return fmt.Errorf("unexpected output %T", output)
		}
		box.Chats = &tg.MessagesChats{Chats: []tg.ChatClass{
			&tg.Channel{ID: c.id, AccessHash: 12345, Title: "a channel", Photo: &tg.ChatPhotoEmpty{}, Date: 1},
		}}
		return nil
	}
	return nil
}

// Why the same channel was requested twice, established by experiment rather
// than by reading.
//
// The request tally showed a single-file link download paying two
// channels.getChannels: one when the link was resolved, one from the peers
// manager the engine built for its batch. The obvious explanation was that the
// engine's manager was new and had resolved nothing, so building one manager
// per account would fix it. That explanation is only a quarter right, and the
// other three quarters are the reason this file exists.
//
// The first measurement is the one that refuted it: peers.Options with no Cache
// gets peers.NoopCache, whose Find methods report every lookup as a miss. A
// manager built that way does not remember the peer it resolved a statement
// earlier, so it pays a request per lookup, not per manager - sharing one would
// have changed nothing at all.
func TestPeerManagerWithoutACacheAlwaysAsks(t *testing.T) {
	store := memStorage{}
	api := &channelAPI{id: 99}
	manager := peers.Options{Storage: kv.NewPeers(store)}.Build(tg.NewClient(api))

	for i := 0; i < 2; i++ {
		if _, err := manager.ResolveChannelID(context.Background(), 99); err != nil {
			t.Fatal(err)
		}
	}

	if api.calls != 2 {
		t.Fatalf("a manager with no cache made %d requests for one channel twice, want 2", api.calls)
	}
	if len(store) == 0 {
		t.Fatal("nothing was written to the persisted index, so it is not the missing piece either")
	}
}

// The second measurement: with a cache the same manager answers from memory,
// which is what makes sharing one worth anything.
func TestPeerManagerWithACacheAnswersFromMemory(t *testing.T) {
	store := memStorage{}
	api := &channelAPI{id: 99}
	manager := peers.Options{
		Storage: kv.NewPeers(store),
		Cache:   &peers.InmemoryCache{},
	}.Build(tg.NewClient(api))

	for i := 0; i < 2; i++ {
		if _, err := manager.ResolveChannelID(context.Background(), 99); err != nil {
			t.Fatal(err)
		}
	}

	if api.calls != 1 {
		t.Fatalf("a cached manager made %d requests for one channel twice, want 1", api.calls)
	}
}

// The third: sharing is still required, because the cache lives in the manager.
// Two managers over one store both ask, which is the original observation.
func TestPeerCacheDoesNotTravelThroughTheStore(t *testing.T) {
	store := memStorage{}
	api := &channelAPI{id: 99}

	first := peers.Options{Storage: kv.NewPeers(store), Cache: &peers.InmemoryCache{}}.Build(tg.NewClient(api))
	if _, err := first.ResolveChannelID(context.Background(), 99); err != nil {
		t.Fatal(err)
	}
	second := peers.Options{Storage: kv.NewPeers(store), Cache: &peers.InmemoryCache{}}.Build(tg.NewClient(api))
	if _, err := second.ResolveChannelID(context.Background(), 99); err != nil {
		t.Fatal(err)
	}

	if api.calls != 2 {
		t.Fatalf("two managers over one store made %d requests, want 2", api.calls)
	}
}
