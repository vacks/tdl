package kv

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gotd/td/telegram/peers"
	upstreamStorage "github.com/iyear/tdl/core/storage"
)

// memStorage is an in-memory Storage.
type memStorage struct {
	values map[string][]byte
	fail   error
}

func newMemStorage() *memStorage { return &memStorage{values: map[string][]byte{}} }

func (m *memStorage) Get(_ context.Context, key string) ([]byte, error) {
	if m.fail != nil {
		return nil, m.fail
	}
	value, ok := m.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	return value, nil
}

func (m *memStorage) Set(_ context.Context, key string, value []byte) error {
	if m.fail != nil {
		return m.fail
	}
	m.values[key] = append([]byte(nil), value...)
	return nil
}

func (m *memStorage) Delete(_ context.Context, key string) error {
	delete(m.values, key)
	return nil
}

// This store is handed to the upstream downloader, which decides whether a
// resume read failed or merely found nothing by comparing against its own
// package's sentinel. Two different sentinels for one answer made every first
// download look like a failed read, and it was abandoned with "key not found"
// before it started - a total failure of the feature, invisible to every test
// that used a fake store.
//
// The assertion is deliberately this blunt. It is not testing behaviour, it is
// testing that one value is still the value the code on the other side of the
// boundary compares against, and it should be deleted along with the alias when
// that code goes.
func TestNotFoundIsTheSentinelTheUpstreamDownloaderComparesAgainst(t *testing.T) {
	if ErrNotFound != upstreamStorage.ErrNotFound {
		t.Fatal("the store answers with a sentinel the upstream downloader does not recognise, so an absent resume key reads as a failed read")
	}
}

// Every key this application has ever written is spelled the way these
// assertions spell it. A key that changes spelling is not renamed, it is gone:
// the peer index would be rebuilt by asking Telegram to resolve every peer
// again, and the session would read as absent, which logs the account out. So
// the spellings are asserted here rather than assumed.
func TestKeySpellingsAreTheOnDiskFormat(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"application identity", App(), "app"},
		{"session", Session(), "session"},
		{"peer", New("peers", "key", "user", "42"), "peers:key:user:42"},
		{"peer phone", New("peers", "phone", "+15551234"), "peers:phone:+15551234"},
		{"contacts hash", New("peers", "contacts", "hash"), "peers:contacts:hash"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s key is %q, want %q", c.name, c.got, c.want)
		}
	}
}

// New hands its buffer back to a pool, so the string it returns has to be a
// copy of the buffer's contents rather than a view into it. A key that aliased
// the pooled buffer would change under the caller as soon as another key was
// built - and it would do so as a lookup that misses, which reads as a peer
// that was never resolved rather than as the corruption it is.
func TestKeysSurviveTheBufferPool(t *testing.T) {
	kept := New("peers", "key", "channel", "1")
	for i := 0; i < 64; i++ {
		New("peers", "key", "user", fmt.Sprint(i))
	}
	if kept != "peers:key:channel:1" {
		t.Fatalf("a key changed after other keys were built: %q", kept)
	}
}

func TestSessionRoundTripsThroughStorage(t *testing.T) {
	store := newMemStorage()
	session := NewSession(store, false)

	// No session yet is the ordinary first run: the client has to be told to
	// start a new authorization, not handed an error.
	data, err := session.LoadSession(context.Background())
	if err != nil {
		t.Fatalf("reading an absent session failed: %v", err)
	}
	if data != nil {
		t.Fatalf("an absent session answered with %d bytes", len(data))
	}

	want := []byte("authorization")
	if err := session.StoreSession(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.values[Session()]; !ok {
		t.Fatalf("the session is stored under %q, which is not the key it is read from", Session())
	}
	got, err := NewSession(store, false).LoadSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("session round-tripped as %q, want %q", got, want)
	}
}

// A client being authorized must not resume whatever session is already on
// disk: a second account's QR login would inherit the first account's
// authorization instead of starting its own.
func TestLoginSessionDoesNotResumeAnExistingSession(t *testing.T) {
	store := newMemStorage()
	if err := NewSession(store, false).StoreSession(context.Background(), []byte("first-account")); err != nil {
		t.Fatal(err)
	}

	data, err := NewSession(store, true).LoadSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("a login client resumed an existing session: %q", data)
	}
}

// A read failure is not an absent session. Treating them alike would send an
// account with an unreadable session into a fresh authorization.
func TestSessionReportsAReadFailure(t *testing.T) {
	store := newMemStorage()
	store.fail = errors.New("disk on fire")
	if _, err := NewSession(store, false).LoadSession(context.Background()); err == nil {
		t.Fatal("an unreadable session was reported as no session")
	}
}

func TestPeersStorageRoundTrips(t *testing.T) {
	store := newMemStorage()
	peerStore := NewPeers(store)
	ctx := context.Background()

	_, found, err := peerStore.Find(ctx, peers.Key{Prefix: "user", ID: 42})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("an unwritten peer was reported as found")
	}

	if err := peerStore.SaveContactsHash(ctx, 99); err != nil {
		t.Fatal(err)
	}
	hash, err := peerStore.GetContactsHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hash != 99 {
		t.Fatalf("contacts hash is %d, want 99", hash)
	}
	// An installation that has never synced contacts reads a zero, not an error.
	if _, err := NewPeers(newMemStorage()).GetContactsHash(ctx); err != nil {
		t.Fatalf("an absent contacts hash failed: %v", err)
	}
}
