package download

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/tmedia"
	"github.com/vacks/tdl/internal/tmsg"
)

// setOrigin records presentation context without changing the true Telegram
// identity used by the downloader and database uniqueness constraints.
func setOrigin(items []source, name string, messageID int, related bool) []source {
	for i := range items {
		items[i].OriginDialogName = name
		items[i].OriginMessageID = messageID
		items[i].IsComment = related
	}
	return items
}

func firstMessageID(messages []*tg.Message, fallback int) int {
	first := fallback
	for _, message := range messages {
		if message != nil && message.ID > 0 && (first == 0 || message.ID < first) {
			first = message.ID
		}
	}
	return first
}

// relatedSources expands a source channel post into its linked-discussion
// replies, or expands a regular group message into its reply thread. Telegram
// identities remain the discussion/group identity; Origin* points back to the
// requested message so final naming can keep all files in one directory.
//
// A missing discussion is expected and returns no items. Transport/permission
// failures are deliberately returned to the caller, which may retain original
// media and surface a non-fatal warning.
//
// learnRoot keeps the eager lookup for the listener path, which resolves a
// newly received post even when its comment section is empty so a comment
// arriving later can be attributed without a second lookup. Every other caller
// spends a request only when the post says there is something to read.
//
// chatJobID names the session task this post belongs to, and is empty for a
// plain message task. It is passed down so a resolved discussion group can be
// recorded on that task: the group is the peer a comment update arrives from,
// and the task's record of it is what lets the listener admit its own comments
// without admitting the whole account's.
func relatedSources(m *Manager, ctx context.Context, api *tg.Client, accountID string, originPeer tg.InputPeerClass, originName string, originMessages []*tg.Message, rootMessageID, originMessageID int, learnRoot bool, chatJobID string) ([]source, error) {
	if len(originMessages) == 0 || originPeer == nil || rootMessageID <= 0 || originMessageID <= 0 {
		return nil, nil
	}
	// A comment section belongs to a broadcast channel post, and
	// messages.getReplies is the call that reads one. Saved Messages has none,
	// and neither has a private chat or a basic group: the dialog can have no
	// linked discussion group (see resolveChatDiscussionLink), and the peer is
	// not merely reply-less but rejected - messages.getReplies answers
	// PEER_ID_INVALID, which is not one of the errors that mean "no discussion".
	// So every message read out of a private chat spent one paced request and
	// one ERROR line on a question whose answer cannot change: the listener path
	// asks about every new message, and both the reaction path and a message
	// forwarded to the Bot read their copy from a user peer. Returning here
	// states the fact instead of asking it.
	//
	// The test is on the peer types that cannot have a section rather than on
	// the one that can, so a peer type this code has not seen yet is still
	// asked about.
	switch originPeer.(type) {
	case *tg.InputPeerSelf, *tg.InputPeerUser, *tg.InputPeerChat:
		return nil, nil
	}
	threadPeer := originPeer
	threadMessageID := rootMessageID
	isDiscussion := false
	expectation := discussionExpectation{}
	if _, channel := originPeer.(*tg.InputPeerChannel); channel {
		expectation = channelDiscussionExpectation(originMessages)
		if !shouldReadDiscussion(expectation, learnRoot) {
			return nil, nil
		}
		resolvedPeer, resolvedRoot, found, err := discussionThread(m, ctx, api, accountID, originPeer, rootMessageID, expectation.dialogID)
		if err != nil {
			return nil, err
		}
		if !found {
			// A post with no comments is the usual case and deliberately remains
			// silent. Telegram's MessageReplies metadata makes the opposite case
			// actionable: comments were declared, but their discussion root could
			// not be recovered.
			if expectation.replyCount > 0 {
				return nil, fmt.Errorf("频道帖子声明有 %d 条评论，但未能解析评论根", expectation.replyCount)
			}
			return nil, nil
		}
		threadPeer, threadMessageID, isDiscussion = resolvedPeer, resolvedRoot, true
	}

	dialogType, dialogKey, dialogID := dialogIdentity(threadPeer, accountID)
	if isDiscussion {
		// Linked discussions are Telegram supergroups, represented as an
		// InputPeerChannel on the wire.
		dialogType = "chat"
		// Record which group this task's comments arrive from. The peer is
		// already in hand, so this costs no request. A failure is logged rather
		// than fatal: the media this call was asked for is still downloadable,
		// and the periodic resolver retries the link from the task row.
		if err := m.rememberDiscussionLink(chatJobID, accountID, dialogKey, threadPeer); err != nil {
			applog.Error("chat_download", "discussion_link_persist_failed", "chat_job_id", chatJobID, "dialog_key", dialogKey, "error", err.Error())
		}
	}
	seen := make(map[int]struct{})
	items := make([]source, 0)
	// This counts actual reply messages, not downloadable files. A discussion
	// containing only text is a healthy, expected result and must not be
	// confused with a failed reply read.
	replyMessages := 0
	// Telegram caps a single reply page. OffsetID is the lowest ID from the
	// previous page, so this always walks newest to oldest until the server
	// confirms there are no more replies. Never use a page length as proof that
	// a thread ended: sparse/deleted messages may yield short intermediate pages.
	offset := 0
	// Albums already expanded during this read of the thread, so a group split
	// across a page boundary is not asked for twice. Scoped to this call on
	// purpose: a grouped id is only unique within one dialog, so anything wider
	// could skip a different album that happens to share an id.
	expandedGroups := make(map[int64]struct{})
	for {
		result, err := api.MessagesGetReplies(ctx, &tg.MessagesGetRepliesRequest{Peer: threadPeer, MsgID: threadMessageID, OffsetID: offset, Limit: 100})
		if err != nil {
			m.recordTelegramRPCError(accountID, err)
			if isNoDiscussionError(err) {
				return items, nil
			}
			return nil, fmt.Errorf("获取关联回复: %w", err)
		}
		page := searchMessages(result)
		if len(page) == 0 {
			break
		}
		entities := entitiesFromMessages(result)
		dialogName := visiblePeerName(entities, threadPeer, originName)
		nextOffset := replyPageNextOffset(page, offset)
		for _, raw := range page {
			message, ok := raw.(*tg.Message)
			if !ok {
				continue
			}
			if message.ID == threadMessageID {
				continue
			}
			replyMessages++
			// GetReplies normally returns every member of an album, so the lookup
			// below is only a safety net for a group split across a page boundary.
			// It has to happen once per album, not once per member: without the
			// guard a ten-photo album asks the identical question ten times and
			// spends ten of the account's rate-limit tokens on one answer. Members
			// already emitted by the first expansion are skipped by seen below, so
			// leaving early loses nothing. Only a successful expansion is
			// remembered, so a failure is retried on the next member rather than
			// silently dropping the group.
			messages := []*tg.Message{message}
			if groupID, grouped := message.GetGroupedID(); grouped {
				if _, alreadyExpanded := expandedGroups[groupID]; alreadyExpanded {
					continue
				}
				group, groupErr := tmsg.GetGroupedMessages(ctx, api, threadPeer, message)
				if groupErr != nil {
					if m.recordTelegramRPCError(accountID, groupErr) {
						// Keep the reply-page cursor intact during a Telegram cooldown.
						// Continuing with a partial group here could lose album members.
						return nil, fmt.Errorf("展开评论相册: %w", groupErr)
					}
				}
				if groupErr == nil && len(group) > 0 {
					expandedGroups[groupID] = struct{}{}
					messages = group
				}
			}
			caption := groupDisplayText(messages)
			for _, member := range messages {
				if member == nil {
					continue
				}
				if _, exists := seen[member.ID]; exists {
					continue
				}
				media, ok := tmedia.GetMedia(member)
				if !ok {
					continue
				}
				seen[member.ID] = struct{}{}
				groupedID, _ := member.GetGroupedID()
				direct := makeDirectPeer(threadPeer)
				items = append(items, source{Item: Item{DialogType: dialogType, DialogKey: dialogKey, DialogID: dialogID, MessageID: member.ID, GroupedID: groupedID, MessageText: caption, OriginalName: media.Name, Size: media.Size, OriginDialogName: originName, OriginMessageID: originMessageID, IsComment: true, SourcePeerType: direct.kind, SourcePeerID: direct.id, SourcePeerHash: direct.hash, ReplyRootID: threadMessageID}, DialogName: dialogName, MediaType: messageMediaType(member), Direct: direct})
			}
		}
		if nextOffset == 0 || nextOffset == offset {
			return nil, fmt.Errorf("关联回复分页游标未推进")
		}
		offset = nextOffset
	}
	if isDiscussion && expectation.replyCount > 0 && replyMessages == 0 {
		// This is not the normal empty-comment case: Telegram said that the
		// channel post has comments, and we successfully located its root. Keep
		// the original download usable, but surface the inconsistent reply read.
		return nil, fmt.Errorf("频道帖子声明有 %d 条评论，但读取评论列表为空", expectation.replyCount)
	}
	return items, nil
}

// discussionExpectation is only meaningful for a channel post. Telegram puts
// it on the original post, including the linked discussion supergroup ID and
// the number of replies currently known to the server.
type discussionExpectation struct {
	dialogID   int64
	replyCount int
}

func channelDiscussionExpectation(messages []*tg.Message) discussionExpectation {
	var result discussionExpectation
	for _, message := range messages {
		replies, ok := message.GetReplies()
		if !ok || !replies.Comments {
			continue
		}
		if replies.Replies > result.replyCount {
			result.replyCount = replies.Replies
		}
		if replies.ChannelID != 0 {
			result.dialogID = replies.ChannelID
		}
	}
	return result
}

// shouldReadDiscussion reports whether a channel post is worth a
// messages.getDiscussionMessage request. Telegram puts both facts on the post
// itself, so a post that declares no comments and names no linked discussion
// has nothing under it and can be answered without a request. The history
// scanner already decides this from the same metadata, which is what makes the
// shortcut safe here.
//
// learnRoot overrides it for the listener path, which resolves a newly received
// post even when its comment section is empty so that a comment arriving later
// can be attributed without a second lookup.
func shouldReadDiscussion(expectation discussionExpectation, learnRoot bool) bool {
	return learnRoot || expectation.replyCount > 0 || expectation.dialogID != 0
}

// broadcastPeer is the single fact this decision needs from a resolved peer. It
// is an interface rather than peers.Peer so the rule can be tested without a
// Telegram client standing behind the peer.
type broadcastPeer interface {
	IsBroadcast() bool
}

// broadcastOf reports the broadcast classification of a resolved peer. Only a
// peers.Channel carries it: a user and a basic group are not candidates, and a
// peer that could not be resolved is nil rather than assumed.
func broadcastOf(peer peers.Peer) broadcastPeer {
	if channel, ok := peer.(peers.Channel); ok {
		return channel
	}
	return nil
}

// eagerDiscussionRoot narrows learnRoot to the only dialog that can have a
// linked discussion group.
//
// learnRoot exists so a newly received post resolves its discussion root before
// its first comment arrives. Only a broadcast channel has one: Telegram marks
// comments only on channel posts, which channelDiscussionExpectation already
// enforces, while a supergroup and a channel are both an InputPeerChannel on the
// wire and a supergroup keeps its replies in-dialog. Without this narrowing a
// listening supergroup spends one messages.getDiscussionMessage per new message
// on a question whose answer is always "no", out of the same rate limit the
// downloads use.
//
// A peer that could not be resolved is not assumed to be a channel: the periodic
// link resolver covers that case, and assuming would restore the per-message
// request this exists to remove.
func eagerDiscussionRoot(learnRoot bool, peer broadcastPeer) bool {
	if !learnRoot {
		return false
	}
	return peer != nil && peer.IsBroadcast()
}

func replyPageNextOffset(page []tg.MessageClass, previous int) int {
	next := previous
	for _, raw := range page {
		var id int
		switch message := raw.(type) {
		case *tg.Message:
			id = message.ID
		case *tg.MessageEmpty:
			// Deleted replies retain an ID and must still advance pagination.
			id = message.ID
		}
		if id > 0 && (next == 0 || id < next) {
			next = id
		}
	}
	return next
}

// discussionThread resolves a channel post to the actual linked discussion
// group root. Telegram guarantees that messages.getDiscussionMessage returns
// its messages in reverse chronological order and that the final message is
// the auto-forwarded discussion root. The post's MessageReplies.ChannelID is
// used as an additional identity check when available.
//
// It is also used for newly received posts with zero replies so future comments
// can be mapped without waiting for the first media reply.
func discussionThread(m *Manager, ctx context.Context, api *tg.Client, accountID string, originPeer tg.InputPeerClass, messageID int, expectedDiscussionID int64) (tg.InputPeerClass, int, bool, error) {
	discussion, err := api.MessagesGetDiscussionMessage(ctx, &tg.MessagesGetDiscussionMessageRequest{Peer: originPeer, MsgID: messageID})
	if err != nil {
		m.recordTelegramRPCError(accountID, err)
		if isNoDiscussionError(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("获取频道评论区: %w", err)
	}
	entities := peer.EntitiesFromResult(discussion)
	originKey := dialogKeyForPeer(originPeer, accountID)
	for _, index := range discussionRootIndexes(discussion.Messages, originKey, expectedDiscussionID) {
		message := discussion.Messages[index].(*tg.Message)
		candidate, extractErr := entities.ExtractPeer(message.PeerID)
		if extractErr != nil || dialogKeyForPeer(candidate, accountID) == originKey {
			continue
		}
		return candidate, message.ID, true, nil
	}
	return nil, 0, false, nil
}

// discussionRootIndexes yields candidates from most to least authoritative.
// The official protocol's final message wins; the reverse fallback preserves
// compatibility with older or malformed server responses without treating a
// random earlier message as the primary root.
func discussionRootIndexes(messages []tg.MessageClass, originKey string, expectedDiscussionID int64) []int {
	indexes := make([]int, 0, len(messages))
	seen := make(map[int]struct{}, len(messages))
	appendCandidate := func(index int, requireExpected bool) {
		if _, ok := seen[index]; ok {
			return
		}
		message, ok := messages[index].(*tg.Message)
		if !ok || message.ID <= 0 {
			return
		}
		if requireExpected && discussionMessagePeerID(message.PeerID) != expectedDiscussionID {
			return
		}
		seen[index] = struct{}{}
		indexes = append(indexes, index)
	}
	if expectedDiscussionID != 0 {
		for index := len(messages) - 1; index >= 0; index-- {
			appendCandidate(index, true)
		}
	}
	// The final non-origin message is the protocol-defined root if metadata was
	// unavailable. The caller still validates the resolved peer identity.
	for index := len(messages) - 1; index >= 0; index-- {
		message, ok := messages[index].(*tg.Message)
		if !ok {
			continue
		}
		if discussionMessagePeerID(message.PeerID) == 0 && originKey != "" {
			continue
		}
		appendCandidate(index, false)
	}
	return indexes
}

func discussionMessagePeerID(value tg.PeerClass) int64 {
	switch peer := value.(type) {
	case *tg.PeerChannel:
		return peer.ChannelID
	case *tg.PeerChat:
		return peer.ChatID
	default:
		return 0
	}
}

// discussionOriginFromRoot recovers the original channel post from a newly
// received discussion reply when a historical task has not yet seen any reply
// for that post. Telegram stores the source channel/post in the forwarded
// discussion-root header, so this costs one request only for the first unknown
// thread instead of an API call for every historical channel message.
func discussionOriginFromRoot(m *Manager, ctx context.Context, api *tg.Client, accountID string, discussionPeer tg.InputPeerClass, rootMessageID int) (originKey string, originMessageID int, resolvedRootMessageID int, err error) {
	channel, ok := discussionPeer.(*tg.InputPeerChannel)
	if !ok {
		return "", 0, rootMessageID, nil
	}
	result, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash},
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: rootMessageID}},
	})
	if err != nil {
		m.recordTelegramRPCError(accountID, err)
		return "", 0, rootMessageID, fmt.Errorf("读取讨论根消息: %w", err)
	}
	for _, raw := range searchMessages(result) {
		message, ok := raw.(*tg.Message)
		if !ok {
			continue
		}
		resolvedRootMessageID = message.ID
		fwd, ok := message.GetFwdFrom()
		if !ok {
			return "", 0, resolvedRootMessageID, nil
		}
		from, ok := fwd.GetFromID()
		if !ok {
			return "", 0, resolvedRootMessageID, nil
		}
		channelFrom, ok := from.(*tg.PeerChannel)
		if !ok {
			return "", 0, resolvedRootMessageID, nil
		}
		postID, ok := fwd.GetChannelPost()
		if !ok || postID <= 0 {
			return "", 0, resolvedRootMessageID, nil
		}
		return fmt.Sprintf("channel:%d", channelFrom.ChannelID), postID, resolvedRootMessageID, nil
	}
	return "", 0, rootMessageID, nil
}

func entitiesFromMessages(result tg.MessagesMessagesClass) peer.Entities {
	switch value := result.(type) {
	case *tg.MessagesMessages:
		return peer.EntitiesFromResult(value)
	case *tg.MessagesMessagesSlice:
		return peer.EntitiesFromResult(value)
	case *tg.MessagesChannelMessages:
		return peer.EntitiesFromResult(value)
	default:
		return peer.NewEntities(map[int64]*tg.User{}, map[int64]*tg.Chat{}, map[int64]*tg.Channel{})
	}
}

func dialogKeyForPeer(input tg.InputPeerClass, accountID string) string {
	_, key, _ := dialogIdentity(input, accountID)
	return key
}

func visiblePeerName(entities peer.Entities, input tg.InputPeerClass, fallback string) string {
	switch value := input.(type) {
	case *tg.InputPeerChannel:
		if entity, ok := entities.Channels()[value.ChannelID]; ok && strings.TrimSpace(entity.Title) != "" {
			return entity.Title
		}
	case *tg.InputPeerChat:
		if entity, ok := entities.Chats()[value.ChatID]; ok && strings.TrimSpace(entity.Title) != "" {
			return entity.Title
		}
	}
	return fallback
}

func isNoDiscussionError(err error) bool {
	text := strings.ToUpper(err.Error())
	// Only a genuinely missing thread is non-fatal. Permission errors must be
	// surfaced to the task/log instead of being mistaken for an empty comment
	// section.
	return strings.Contains(text, "MSG_ID_INVALID") || strings.Contains(text, "MESSAGE_ID_INVALID")
}

func logRelatedWarning(accountID string, messageID int, err error) {
	// This is non-fatal for the original message, but it is operationally
	// significant: silently treating an inaccessible comment area as empty
	// would hide an incomplete download from the administrator.
	applog.Error("discussion_download", "related_messages_unavailable", "account_id", accountID, "message_id", messageID, "error", err.Error())
}
