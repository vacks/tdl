package download

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/vacks/tdl/internal/applog"
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
func relatedSources(ctx context.Context, api *tg.Client, accountID string, originPeer tg.InputPeerClass, originName string, originMessages []*tg.Message, rootMessageID, originMessageID int) ([]source, error) {
	if len(originMessages) == 0 || originPeer == nil || rootMessageID <= 0 || originMessageID <= 0 {
		return nil, nil
	}
	threadPeer := originPeer
	threadMessageID := rootMessageID
	isDiscussion := false
	expectation := discussionExpectation{}
	if _, channel := originPeer.(*tg.InputPeerChannel); channel {
		expectation = channelDiscussionExpectation(originMessages)
		resolvedPeer, resolvedRoot, found, err := discussionThread(ctx, api, accountID, originPeer, rootMessageID, expectation.dialogID)
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
	for {
		result, err := api.MessagesGetReplies(ctx, &tg.MessagesGetRepliesRequest{Peer: threadPeer, MsgID: threadMessageID, OffsetID: offset, Limit: 100})
		if err != nil {
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
			// GetReplies normally returns every member of an album. Ask upstream for
			// its complete group only once as a safety net for a page boundary.
			messages := []*tg.Message{message}
			if _, grouped := message.GetGroupedID(); grouped {
				group, groupErr := tutil.GetGroupedMessages(ctx, api, threadPeer, message)
				if groupErr == nil && len(group) > 0 {
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
func discussionThread(ctx context.Context, api *tg.Client, accountID string, originPeer tg.InputPeerClass, messageID int, expectedDiscussionID int64) (tg.InputPeerClass, int, bool, error) {
	discussion, err := api.MessagesGetDiscussionMessage(ctx, &tg.MessagesGetDiscussionMessageRequest{Peer: originPeer, MsgID: messageID})
	if err != nil {
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
func discussionOriginFromRoot(ctx context.Context, api *tg.Client, discussionPeer tg.InputPeerClass, rootMessageID int) (originKey string, originMessageID int, resolvedRootMessageID int, err error) {
	channel, ok := discussionPeer.(*tg.InputPeerChannel)
	if !ok {
		return "", 0, rootMessageID, nil
	}
	result, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash},
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: rootMessageID}},
	})
	if err != nil {
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
