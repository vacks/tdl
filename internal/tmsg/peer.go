// Package tmsg holds this application's own readers for Telegram messages,
// peers and links.
//
// It is the copy of the upstream TDL helpers that this application actually
// calls. What was not copied is everything upstream carried for a command-line
// tool: the blocked-dialog query, the file-existence probe, the message sorter,
// and the request-building helpers used to send and forward.
package tmsg

import (
	"context"
	"fmt"
	"strconv"

	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
)

// Dialog names one peer and the messages wanted from it. It is what a caller
// hands the download engine: a peer with an access hash, and the ids to fetch.
type Dialog struct {
	Peer     tg.InputPeerClass
	Messages []int
}

// GetInputPeer resolves a username, a numeric id or a public link fragment into
// a peer.
//
// A numeric id is tried as a channel, then as a user, then as a basic group,
// because the number alone does not say which it is. The error keeps the last
// attempt's reason, which is what an operator sees when a link points at
// something this account cannot reach.
func GetInputPeer(ctx context.Context, manager *peers.Manager, from string) (peers.Peer, error) {
	id, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		peer, err := manager.Resolve(ctx, from)
		if err != nil {
			return nil, err
		}
		return peer, nil
	}

	var peer peers.Peer
	if peer, err = manager.ResolveChannelID(ctx, id); err == nil {
		return peer, nil
	}
	if peer, err = manager.ResolveUserID(ctx, id); err == nil {
		return peer, nil
	}
	if peer, err = manager.ResolveChatID(ctx, id); err == nil {
		return peer, nil
	}
	return nil, fmt.Errorf("failed to get result from %d：%v", id, err)
}
