package transfer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/vacks/tdl/internal/kv"
)

// fakeAPI answers the requests this engine makes, and records every one of
// them.
//
// Recording is the point: the batching this engine exists for is invisible in
// its output - the files arrive either way - and only the request count shows
// whether a hundred messages cost one request or a hundred.
type fakeAPI struct {
	mu       sync.Mutex
	requests []bin.Encoder

	// missing are message ids the server does not return, which is what a
	// deleted post looks like.
	missing map[int]bool
	// messages are the message bodies served for ids that exist.
	messages map[int]*tg.Message
	// failBatch makes every batch request fail.
	failBatch bool
	// file is what upload.getFile serves.
	file []byte
	// failFileFor marks document ids whose transfer fails partway.
	failFileFor map[int64]bool
	// staleOnce marks document ids whose first byte request is refused as an
	// expired file reference and whose later ones succeed, which is what a
	// reference that aged out looks like once the client has read the message
	// again.
	staleOnce map[int64]bool
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		missing:     map[int]bool{},
		messages:    map[int]*tg.Message{},
		failFileFor: map[int64]bool{},
		staleOnce:   map[int64]bool{},
	}
}

func (f *fakeAPI) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.requests))
	for _, request := range f.requests {
		out = append(out, fmt.Sprintf("%T", request))
	}
	return out
}

func (f *fakeAPI) count(name string) int {
	total := 0
	for _, recorded := range f.names() {
		if recorded == "*tg."+name {
			total++
		}
	}
	return total
}

func (f *fakeAPI) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	f.mu.Lock()
	f.requests = append(f.requests, input)
	f.mu.Unlock()

	switch request := input.(type) {
	case *tg.MessagesGetMessagesRequest:
		if f.failBatch {
			return fmt.Errorf("MESSAGE_BATCH_REFUSED")
		}
		box, ok := output.(*tg.MessagesMessagesBox)
		if !ok {
			return fmt.Errorf("unexpected output %T", output)
		}
		list := make([]tg.MessageClass, 0, len(request.ID))
		for _, id := range request.ID {
			list = append(list, f.classFor(id.(*tg.InputMessageID).ID))
		}
		box.Messages = &tg.MessagesMessages{Messages: list}
		return nil

	case *tg.ChannelsGetMessagesRequest:
		if f.failBatch {
			return fmt.Errorf("MESSAGE_BATCH_REFUSED")
		}
		box, ok := output.(*tg.MessagesMessagesBox)
		if !ok {
			return fmt.Errorf("unexpected output %T", output)
		}
		list := make([]tg.MessageClass, 0, len(request.ID))
		for _, id := range request.ID {
			list = append(list, f.classFor(id.(*tg.InputMessageID).ID))
		}
		box.Messages = &tg.MessagesChannelMessages{Messages: list}
		return nil

	case *tg.MessagesGetHistoryRequest:
		// The per-message fallback path. It answers with the message at the id
		// above the requested one, which is how the history read finds it.
		box, ok := output.(*tg.MessagesMessagesBox)
		if !ok {
			return fmt.Errorf("unexpected output %T", output)
		}
		id := request.OffsetID - 1
		message, exists := f.messages[id]
		if !exists || f.missing[id] {
			// An empty page, which the reader reports as a deleted message.
			box.Messages = &tg.MessagesMessages{}
			return nil
		}
		box.Messages = &tg.MessagesMessages{Messages: []tg.MessageClass{message}}
		return nil

	case *tg.UsersGetUsersRequest:
		box, ok := output.(*tg.UserClassVector)
		if !ok {
			return fmt.Errorf("unexpected output %T", output)
		}
		users := make([]tg.UserClass, 0, len(request.ID))
		for _, raw := range request.ID {
			input, ok := raw.(*tg.InputUser)
			if !ok {
				continue
			}
			users = append(users, &tg.User{ID: input.UserID, AccessHash: input.AccessHash, Username: "peer"})
		}
		box.Elems = users
		return nil

	case *tg.UploadGetFileRequest:
		box, ok := output.(*tg.UploadFileBox)
		if !ok {
			return fmt.Errorf("unexpected output %T", output)
		}
		if location, ok := request.Location.(*tg.InputDocumentFileLocation); ok {
			if f.failFileFor[location.ID] {
				return fmt.Errorf("FILE_REFERENCE_EXPIRED")
			}
			if f.staleOnce[location.ID] {
				f.mu.Lock()
				delete(f.staleOnce, location.ID)
				f.mu.Unlock()
				return &tgerr.Error{Code: 400, Type: tg.ErrFileReferenceExpired, Message: tg.ErrFileReferenceExpired}
			}
		}
		offset := int(request.Offset)
		if offset >= len(f.file) {
			box.File = &tg.UploadFile{}
			return nil
		}
		end := offset + request.Limit
		if end > len(f.file) {
			end = len(f.file)
		}
		box.File = &tg.UploadFile{Bytes: f.file[offset:end]}
		return nil
	}
	return nil
}

func (f *fakeAPI) classFor(id int) tg.MessageClass {
	if f.missing[id] {
		return &tg.MessageEmpty{ID: id}
	}
	if message, ok := f.messages[id]; ok {
		return message
	}
	return &tg.MessageEmpty{ID: id}
}

// fakePool serves every datacenter from one client, and records which
// datacenters were asked for.
type fakePool struct {
	client *tg.Client
	dcs    []int
	mu     sync.Mutex
	kvs    kv.Storage
}

func (p *fakePool) Client(_ context.Context, dc int) *tg.Client {
	p.mu.Lock()
	p.dcs = append(p.dcs, dc)
	p.mu.Unlock()
	return p.client
}

func (p *fakePool) Default(ctx context.Context) *tg.Client { return p.Client(ctx, 0) }

// testDeps wires the engine to a fake Telegram.
func testDeps(api *fakeAPI) (Deps, *fakePool) {
	pool := &fakePool{client: tg.NewClient(api), kvs: memStorage{}}
	return Deps{Pool: pool, KV: memStorage{}, AccountID: "account-1"}, pool
}

type memStorage map[string][]byte

func (m memStorage) Get(_ context.Context, key string) ([]byte, error) {
	value, ok := m[key]
	if !ok {
		return nil, kv.ErrNotFound
	}
	return value, nil
}
func (m memStorage) Set(_ context.Context, key string, value []byte) error {
	m[key] = value
	return nil
}
func (m memStorage) Delete(_ context.Context, key string) error {
	delete(m, key)
	return nil
}

// documentMessage builds a message carrying a downloadable file.
//
// The date is set on the document as well as on the message, because the
// document's is the one the transfer stamps the finished file with - a
// forwarded post's media can be older than the post.
func documentMessage(id, date, size int) *tg.Message {
	return &tg.Message{
		ID:   id,
		Date: date,
		// See the note on the flag in the media field below.
		// The media flag is what makes GetMedia answer at all: the field is
		// optional in the schema, and a message built without the bit set reads as
		// one that carries nothing.
		Flags: bin.Fields(1 << 9),
		Media: &tg.MessageMediaDocument{Document: &tg.Document{
			ID:            int64(id) * 1000,
			AccessHash:    7,
			FileReference: []byte{1, 2, 3},
			DCID:          2,
			Size:          int64(size),
			Date:          date,
			Attributes: []tg.DocumentAttributeClass{
				&tg.DocumentAttributeFilename{FileName: fmt.Sprintf("file-%d.bin", id)},
			},
		}},
	}
}

func idsFrom(from, to int) []int {
	out := make([]int, 0, to-from+1)
	for id := from; id <= to; id++ {
		out = append(out, id)
	}
	return out
}

var testPeer = &tg.InputPeerUser{UserID: 42, AccessHash: 1}

func fillMessages(api *fakeAPI, ids []int) {
	for _, id := range ids {
		api.messages[id] = documentMessage(id, 1700000000, len(api.file))
	}
}

func requireCount(t *testing.T, api *fakeAPI, method string, want int) {
	t.Helper()
	if got := api.count(method); got != want {
		t.Fatalf("%s was called %d times, want %d (all requests: %v)", method, got, want, api.names())
	}
}
