package kv

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/gotd/td/telegram/peers"
)

// peersKV stores gotd's resolved-peer index through Storage.
//
// This index is what keeps the application from asking Telegram to resolve the
// same peer twice: an access hash saved here is reused by every later request,
// so losing it - by changing a key's spelling, say - costs a resolve per peer
// and shows up as traffic rather than as an error. The key layout below is
// therefore a format, copied from the implementation this one replaces.
type peersKV struct {
	kv Storage
}

// NewPeers returns a peers.Storage backed by kv.
func NewPeers(kv Storage) peers.Storage {
	return &peersKV{kv: kv}
}

func (p *peersKV) Save(ctx context.Context, key peers.Key, value peers.Value) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.kv.Set(ctx, p.key(key), encoded)
}

func (p *peersKV) Find(ctx context.Context, key peers.Key) (peers.Value, bool, error) {
	data, err := p.kv.Get(ctx, p.key(key))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return peers.Value{}, false, nil
		}
		return peers.Value{}, false, err
	}

	var value peers.Value
	if err = json.Unmarshal(data, &value); err != nil {
		return peers.Value{}, false, err
	}
	return value, true, nil
}

func (p *peersKV) SavePhone(ctx context.Context, phone string, key peers.Key) error {
	encoded, err := json.Marshal(key)
	if err != nil {
		return err
	}
	return p.kv.Set(ctx, p.phoneKey(phone), encoded)
}

func (p *peersKV) FindPhone(ctx context.Context, phone string) (peers.Key, peers.Value, bool, error) {
	data, err := p.kv.Get(ctx, p.phoneKey(phone))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return peers.Key{}, peers.Value{}, false, nil
		}
		return peers.Key{}, peers.Value{}, false, err
	}

	var key peers.Key
	if err = json.Unmarshal(data, &key); err != nil {
		return peers.Key{}, peers.Value{}, false, err
	}

	value, found, err := p.Find(ctx, key)
	if err != nil {
		return peers.Key{}, peers.Value{}, false, err
	}
	return key, value, found, nil
}

func (p *peersKV) GetContactsHash(ctx context.Context) (int64, error) {
	data, err := p.kv.Get(ctx, p.contactsKey())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(string(data), 10, 64)
}

func (p *peersKV) SaveContactsHash(ctx context.Context, hash int64) error {
	return p.kv.Set(ctx, p.contactsKey(), []byte(strconv.FormatInt(hash, 10)))
}

func (p *peersKV) key(key peers.Key) string {
	return New("peers", "key", key.Prefix, strconv.FormatInt(key.ID, 10))
}

func (p *peersKV) phoneKey(phone string) string {
	return New("peers", "phone", phone)
}

func (p *peersKV) contactsKey() string {
	return New("peers", "contacts", "hash")
}
