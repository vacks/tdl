// Package kv is the storage contract for the state Telegram clients keep
// between runs: the authorization session, the resolved-peer index, and the
// application's own identity.
//
// It is deliberately three methods on a string key. The Telegram machinery
// needs no more than that, and a small contract is what let this application
// keep its own on-disk layout - one file per key under a per-account state
// directory, with the session in a file of its own - while the clients that
// read and write it were replaced.
//
// The keys are part of the format. An installation upgrading to a build with
// this package reuses the same key strings it wrote before, so an account that
// was logged in stays logged in; see key.go and peers.go, where the exact
// spellings are load-bearing rather than incidental.
package kv

import (
	"context"

	upstreamStorage "github.com/iyear/tdl/core/storage"
)

// Storage is a byte-oriented key-value store.
type Storage interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// ErrNotFound is what Get returns for a key that was never written or has been
// deleted. Callers distinguish it from a read failure, because "no session yet"
// is the ordinary first-run case and "cannot read the session" is not.
//
// It is, for now, the same value the upstream storage package uses, and that
// identity is load-bearing rather than tidy-minded. This store is handed to
// code that predates it: the upstream downloader reads its resume state through
// it and decides whether the read failed or merely found nothing with
// errors.Is(err, storage.ErrNotFound). A store answering with a sentinel of its
// own made every first download look like a failed read - the resume key is
// absent in the ordinary case - and the transfer was abandoned with "key not
// found" before it started.
//
// It is one value, not two aliases, because both sides have to agree: our own
// session and peer storage compare against this too, and they are handed errors
// by the same store.
//
// When the upstream downloader is gone this becomes its own sentinel, which is
// a one-line change with no caller to update.
var ErrNotFound = upstreamStorage.ErrNotFound
