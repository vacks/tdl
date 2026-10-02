package tmsg

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/peers"
)

// ParseMessageLink turns a t.me link into the peer it names and the message id
// inside it.
//
// The shapes Telegram uses are all here, including the two that carry a second
// id that is not the message: a topic id (/c/<channel>/<topic>/<message> and
// /<user>/<topic>/<message>) and a comment id (?comment=<id>). A comment link
// names a post in a channel but resolves inside the channel's linked discussion
// group, which is why it costs a channels.getFullChannel.
func ParseMessageLink(ctx context.Context, manager *peers.Manager, raw string) (peers.Peer, int, error) {
	parse := func(from, message string) (peers.Peer, int, error) {
		peer, err := GetInputPeer(ctx, manager, from)
		if err != nil {
			return nil, 0, errors.Wrap(err, "input peer")
		}
		id, err := strconv.Atoi(message)
		if err != nil {
			return nil, 0, errors.Wrap(err, "parse message id")
		}
		return peer, id, nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, 0, err
	}
	paths := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")

	// https://t.me/opencfdchannel/4434?comment=360409
	if comment := parsed.Query().Get("comment"); comment != "" {
		peer, err := GetInputPeer(ctx, manager, paths[0])
		if err != nil {
			return nil, 0, errors.Wrap(err, "input peer")
		}
		channel, ok := peer.(peers.Channel)
		if !ok || !channel.IsBroadcast() {
			return nil, 0, errors.New("not channel")
		}
		full, err := channel.FullRaw(ctx)
		if err != nil {
			return nil, 0, errors.Wrap(err, "full raw")
		}
		linked, ok := full.GetLinkedChatID()
		if !ok {
			return nil, 0, errors.New("no linked chat")
		}
		return parse(strconv.FormatInt(linked, 10), comment)
	}

	switch len(paths) {
	case 2:
		// https://t.me/telegram/193
		// https://t.me/myhostloc/1485524?thread=1485523
		return parse(paths[0], paths[1])
	case 3:
		// https://t.me/c/1697797156/151
		// https://t.me/iFreeKnow/45662/55005
		if paths[0] == "c" {
			return parse(paths[1], paths[2])
		}
		// paths[1] is the topic id, which the message id does not need.
		return parse(paths[0], paths[2])
	case 4:
		// https://t.me/c/1492447836/251015/251021
		if paths[0] != "c" {
			return nil, 0, fmt.Errorf("invalid message link")
		}
		// paths[2] is the topic id, which the message id does not need.
		return parse(paths[1], paths[3])
	default:
		return nil, 0, fmt.Errorf("invalid message link: %s", raw)
	}
}
