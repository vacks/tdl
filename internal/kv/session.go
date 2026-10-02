package kv

import (
	"context"
	"errors"

	"github.com/gotd/td/telegram"
)

// sessionKV stores the MTProto authorization session through Storage.
//
// The bytes are passed through untouched. They are gotd's session format, and
// re-encoding them is the one change that would log every existing account out.
type sessionKV struct {
	kv    Storage
	login bool
}

// NewSession returns the session storage for one client.
//
// login selects the fresh-login behaviour: a client being authorized must not
// resume whatever session is already on disk, or a second account's QR login
// would inherit the first account's authorization instead of starting its own.
func NewSession(kv Storage, login bool) telegram.SessionStorage {
	return &sessionKV{kv: kv, login: login}
}

func (s *sessionKV) LoadSession(ctx context.Context) ([]byte, error) {
	if s.login {
		return nil, nil
	}
	data, err := s.kv.Get(ctx, Session())
	if err != nil {
		// No session yet is the ordinary first run, not a failure.
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (s *sessionKV) StoreSession(ctx context.Context, data []byte) error {
	return s.kv.Set(ctx, Session(), data)
}
