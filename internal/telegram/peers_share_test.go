package telegram

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/kv"
)

// peersStore is an in-memory kv.Storage for the peer index.
type peersStore struct {
	mu     sync.Mutex
	values map[string][]byte
}

func (s *peersStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[key]
	if !ok {
		return nil, kv.ErrNotFound
	}
	return value, nil
}

func (s *peersStore) Set(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append([]byte(nil), value...)
	return nil
}

func (s *peersStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
	return nil
}

// channelsInvoker answers channels.getChannels with one channel and counts how
// many times it was asked.
type channelsInvoker struct {
	mu    sync.Mutex
	calls int
}

func (c *channelsInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	if _, ok := input.(*tg.ChannelsGetChannelsRequest); !ok {
		return nil
	}
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	box, ok := output.(*tg.MessagesChatsBox)
	if !ok {
		return fmt.Errorf("unexpected output %T", output)
	}
	box.Chats = &tg.MessagesChats{Chats: []tg.ChatClass{
		&tg.Channel{ID: 99, AccessHash: 12345, Title: "a channel", Photo: &tg.ChatPhotoEmpty{}, Date: 1},
	}}
	return nil
}

func (c *channelsInvoker) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// A manager must answer a repeat lookup without asking Telegram again.
//
// This is not gotd's default behaviour. peers.Options with no Cache is given
// peers.NoopCache, whose Find* methods report every lookup as a miss, so
// getChannel finds nothing and falls through to channels.getChannels - on every
// lookup, not merely on a manager's first. Building the manager without a cache
// is therefore the same as not having one, which is why this is asserted
// separately from the sharing below: they are two independent defects that
// produce the same duplicate request.
func TestPeerManagerAnswersARepeatLookupFromMemory(t *testing.T) {
	invoker := &channelsInvoker{}
	store := &peersStore{values: map[string][]byte{}}
	manager := (&Manager{}).Peers("account-1", tg.NewClient(invoker), store)

	for i := 0; i < 2; i++ {
		if _, err := manager.ResolveChannelID(context.Background(), 99); err != nil {
			t.Fatal(err)
		}
	}

	if invoker.count() != 1 {
		t.Fatalf("a second lookup of one channel made %d requests, want 1", invoker.count())
	}
}

// One peer manager per account and connection, because a manager only
// remembers what it resolved itself.
//
// A manager built per operation has seen nothing and consults the persisted
// index only to find an access hash - it does not trust an entry it did not
// write. So a submission that resolved a channel and the download that followed
// resolved it again, which the request tally caught as a second
// channels.getChannels on a single-file link download. The store is shared
// between the two calls here, exactly as it was in production, and that is what
// makes the assertion below a statement about the manager rather than about the
// storage.
func TestPeerManagerIsSharedAcrossOperationsOnOneAccount(t *testing.T) {
	invoker := &channelsInvoker{}
	m := &Manager{}
	store := &peersStore{values: map[string][]byte{}}
	api := tg.NewClient(invoker)

	first := m.Peers("account-1", api, store)
	if _, err := first.ResolveChannelID(context.Background(), 99); err != nil {
		t.Fatal(err)
	}
	second := m.Peers("account-1", api, store)
	if first != second {
		t.Fatal("a second operation on one account was given a new peer manager")
	}
	if _, err := second.ResolveChannelID(context.Background(), 99); err != nil {
		t.Fatal(err)
	}

	if invoker.count() != 1 {
		t.Fatalf("two operations on one account made %d channel requests, want 1", invoker.count())
	}
}

// A reconnect hands out a different client, and the manager must go with it.
// Keeping the old one would answer from a cache filled over a connection that
// no longer exists.
func TestPeerManagerIsRebuiltWhenTheClientChanges(t *testing.T) {
	m := &Manager{}
	store := &peersStore{values: map[string][]byte{}}

	first := m.Peers("account-1", tg.NewClient(&channelsInvoker{}), store)
	second := m.Peers("account-1", tg.NewClient(&channelsInvoker{}), store)
	if first == second {
		t.Fatal("a new client reused the previous manager")
	}
}

// Two accounts never share a manager, even over one storage, because the cache
// holds access hashes resolved with one account's authorization.
func TestPeerManagersAreSeparatePerAccount(t *testing.T) {
	m := &Manager{}
	store := &peersStore{values: map[string][]byte{}}
	api := tg.NewClient(&channelsInvoker{})

	if m.Peers("account-1", api, store) == m.Peers("account-2", api, store) {
		t.Fatal("two accounts were given the same peer manager")
	}
}
