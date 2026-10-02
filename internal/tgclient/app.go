// Package tgclient builds the MTProto client an account connects with.
//
// It is this application's own implementation of what used to come from the
// upstream TDL module: the same application identity, the same proxy handling,
// the same default middleware chain in the same order. The order is not
// incidental - the account's rate-limit gate has to sit inside the waiter that
// consumes a FLOOD_WAIT, or it never sees the refusal it exists to record - so
// it is built in one place here rather than assembled at each call site.
//
// What is deliberately absent is everything the upstream package carried for a
// command-line tool: NTP clock sources, datacenter overrides read from a
// process-wide variable, and terminal logging. None of it was reachable from
// this application, and all of it was compiled in.
package tgclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/vacks/tdl/internal/kv"
)

// The two identities below are the values the KV store may hold under kv.App().
// A different value means the account was authorized with an application this
// build does not know, and the client cannot be built - see application().
const (
	AppBuiltin = "builtin"
	AppDesktop = "desktop"
)

type applicationID struct {
	id   int
	hash string
}

// applications is the identity table. Only AppDesktop is reachable from this
// application - the account store answers kv.App() with it unconditionally -
// but the table is kept whole because the value is stored per account, and a
// missing entry would be a silent fallback rather than an error.
var applications = map[string]applicationID{
	// Created by tdesktop. The application presents itself as the official
	// desktop client: https://opentele.readthedocs.io/en/latest/documentation/authorization/api/
	AppDesktop: {id: 2040, hash: "b18441a1ff607e10a989891a5462e627"},
	// Created by the upstream TDL project.
	AppBuiltin: {id: 15055931, hash: "021d433426cbb920eeb95164498fe3d3"},
}

// application reads the identity this account presents as.
//
// A stored value with no table entry is an error rather than a default. The
// upstream implementation answered with AppBuiltin here, which meant a typo or
// a half-written state file produced a client with an identity the account was
// never authorized with - and the failure surfaced later as an authorization
// error against the wrong application.
func application(ctx context.Context, store kv.Storage) (applicationID, error) {
	mode, err := store.Get(ctx, kv.App())
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			mode = []byte(AppBuiltin)
		} else {
			return applicationID{}, fmt.Errorf("read Telegram application identity: %w", err)
		}
	}

	identity, ok := applications[string(mode)]
	if !ok {
		return applicationID{}, fmt.Errorf("can't find app: %s, please try re-login", mode)
	}
	return identity, nil
}
