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
	"errors"
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
// It used to be the upstream storage package's sentinel, and the identity was
// load-bearing: the upstream downloader read its resume state through this
// store and decided whether the read had failed by comparing against its own
// package's value. Two sentinels for one answer made every first download look
// like a failed read, and every download was abandoned before it started. That
// consumer is gone, so this is a value of its own - and every reader of this
// store is in this repository, comparing against this name.
var ErrNotFound = errors.New("key not found")
