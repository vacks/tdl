package download

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/storage"
)

// botChatHistoryScan bounds the page this reads when looking for the message a
// forward was made from. The message is the newest one the account sent to the
// Bot, so it is at the top of the dialog; the page is wide only so that a burst
// of status cards arriving in the same second cannot push it off.
const botChatHistoryScan = 20

// botChatClockSkew tolerates the two send times disagreeing by a moment. The
// Bot API and the account are describing one message, and the second they
// report it in should be the same one, but a message is worth finding when the
// clocks that stamped it disagree by a second.
const botChatClockSkew = 2

// BotChatIntent names a message the account itself sent to its own Bot, as the
// Bot API described it.
//
// It carries a time rather than a message id on purpose. The Bot API numbers
// the messages of a bot chat from the Bot's side, and that numbering is not the
// one the account sees: measured against this installation, the Bot API had a
// card of its own at message 1203 while the account's view of the same chat
// held nothing at or below that id and ran into the 5xxx range. An id from the
// update therefore addresses nothing in the account's history, and a link built
// from one resolves to an empty page - which is what a bare "get single
// message" was reporting. The send time survives the translation.
type BotChatIntent struct {
	Source      SourceKind
	AccountID   string
	BotUsername string
	// SentAt is the update's date, in unix seconds.
	SentAt int64
}

// SubmitBotChatMessage downloads the copy of a message that sits in the user's
// own chat with the Bot - the message a forward was made from.
//
// It is the second way to reach a forwarded post, and the only one that works
// when the post's origin is a chat this account cannot read: a private channel
// it never joined, a chat it cannot re-enter, an origin the forward does not
// name at all. The copy is always readable, because the user can only forward
// what they can see, and it is exactly what reacting to the forwarded message
// downloads - so forwarding and reacting converge on one dialog and one message
// id rather than creating two tasks for one file.
func (m *Manager) SubmitBotChatMessage(ctx context.Context, intent BotChatIntent) (Submission, error) {
	if strings.TrimSpace(intent.BotUsername) == "" {
		return Submission{}, errors.New("Bot 会话下载缺少 Bot 用户名")
	}
	accountID := intent.AccountID
	if accountID == "" {
		var err error
		accountID, err = m.accounts.CurrentID()
		if err != nil {
			return Submission{}, err
		}
	}
	var ref *MessageRef
	err := m.accounts.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(client.API())
		// Resolved as a username rather than through the general resolver, which
		// decides between a username, a phone number and a deeplink by looking at
		// the text: a Bot whose username happens to begin with a digit would be
		// read as a phone number and never found.
		peer, err := manager.ResolveDomain(ctx, intent.BotUsername)
		if err != nil {
			m.recordTelegramRPCError(accountID, err)
			return err
		}
		result, err := client.API().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer.InputPeer(), Limit: botChatHistoryScan})
		if err != nil {
			m.recordTelegramRPCError(accountID, err)
			return err
		}
		message := newestOwnMessage(searchMessages(result), intent.SentAt)
		if message == nil {
			return fmt.Errorf("在 Bot 会话中找不到刚发送的消息（发送时间 %d）", intent.SentAt)
		}
		// The link is built from the account's own id for the message, so it is
		// the same link a reaction on that message records, and it opens the
		// message in a client.
		ref = &MessageRef{SourceURL: fmt.Sprintf("https://t.me/%s/%d", intent.BotUsername, message.ID), DialogName: peer.VisibleName(), DialogID: peer.ID(), InputPeer: peer.InputPeer(), MessageID: message.ID}
		return nil
	})
	if err != nil {
		return Submission{}, err
	}
	// The ordinary submission builds everything else - the album, the dialog
	// identity, the owned-media claim - from the message this located. Nothing
	// about it is special enough to deserve a second implementation.
	return m.Submit(ctx, DownloadIntent{Source: intent.Source, AccountID: accountID, Message: ref})
}

// newestOwnMessage picks the message a Bot API update described out of a page of
// the account's chat with the Bot.
//
// The account's own messages are the only candidates: everything the Bot says
// arrives as an incoming message, so a card sent in answer to a task can never
// be mistaken for the message being downloaded. The send time then has to agree,
// which is what keeps two forwards made a second apart from collapsing onto the
// newest one. A page with no agreement at all yields nothing rather than the
// newest candidate, because the caller reports a missing message and a
// substitution would download the wrong file under the right name.
func newestOwnMessage(messages []tg.MessageClass, sentAt int64) *tg.Message {
	var exact, nearest *tg.Message
	for _, raw := range messages {
		message, ok := raw.(*tg.Message)
		if !ok || !message.Out {
			continue
		}
		switch skew := int64(message.Date) - sentAt; {
		case skew == 0:
			if exact == nil || message.ID > exact.ID {
				exact = message
			}
		case skew <= botChatClockSkew && skew >= -botChatClockSkew:
			if nearest == nil || message.ID > nearest.ID {
				nearest = message
			}
		}
	}
	if exact != nil {
		return exact
	}
	return nearest
}
