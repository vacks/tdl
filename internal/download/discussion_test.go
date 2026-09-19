package download

import (
	"strings"
	"testing"

	"github.com/gotd/td/tg"
)

func TestDiscussionOriginIsStableAndDoesNotReplaceTelegramIdentity(t *testing.T) {
	items := []source{{Item: Item{DialogKey: "channel:1", MessageID: 102, GroupedID: 999}, DialogName: "频道"}}
	items = setOrigin(items, "频道", 100, false)
	if items[0].DialogKey != "channel:1" || items[0].MessageID != 102 || items[0].GroupedID != 999 {
		t.Fatal("origin assignment must not alter physical Telegram identity")
	}
	if items[0].OriginDialogName != "频道" || items[0].OriginMessageID != 100 || items[0].IsComment {
		t.Fatalf("unexpected origin context: %#v", items[0].Item)
	}
	comment := setOrigin([]source{{Item: Item{DialogKey: "channel:discussion", MessageID: 9}}}, "频道", 100, true)[0]
	path, err := renderName("{{ .OriginDialogName }}/{{ .OriginMessageID }}_{{ if .IsComment }}c_{{ end }}{{ .MessageID }}{{ .FileExt }}", source{Item: Item{DialogKey: comment.DialogKey, MessageID: comment.MessageID, OriginDialogName: comment.OriginDialogName, OriginMessageID: comment.OriginMessageID, IsComment: comment.IsComment, OriginalName: "reply.jpg"}, DialogName: "讨论组"})
	if err != nil {
		t.Fatal(err)
	}
	if path != "频道/100_c_9.jpg" {
		t.Fatalf("unexpected comment path: %q", path)
	}
}

func TestFirstMessageID(t *testing.T) {
	got := firstMessageID([]*tg.Message{{ID: 103}, {ID: 101}, {ID: 102}}, 103)
	if got != 101 {
		t.Fatalf("got %d, want 101", got)
	}
	if got := firstMessageID(nil, 42); got != 42 {
		t.Fatalf("got %d, want fallback", got)
	}
}

func TestChannelDiscussionExpectationUsesOfficialReplyMetadata(t *testing.T) {
	first := &tg.Message{ID: 100}
	first.SetReplies(tg.MessageReplies{Comments: true, Replies: 4, ChannelID: 200})
	second := &tg.Message{ID: 101}
	second.SetReplies(tg.MessageReplies{Comments: true, Replies: 7, ChannelID: 200})
	got := channelDiscussionExpectation([]*tg.Message{first, second})
	if got.dialogID != 200 || got.replyCount != 7 {
		t.Fatalf("unexpected expectation: %#v", got)
	}
	if got := channelDiscussionExpectation([]*tg.Message{{ID: 102}}); got != (discussionExpectation{}) {
		t.Fatalf("plain channel post must have no expectation: %#v", got)
	}
}

func TestDiscussionRootIndexesPreferProtocolDefinedLastRoot(t *testing.T) {
	messages := []tg.MessageClass{
		&tg.Message{ID: 900, PeerID: &tg.PeerChannel{ChannelID: 10}},
		&tg.Message{ID: 901, PeerID: &tg.PeerChannel{ChannelID: 20}},
		&tg.Message{ID: 902, PeerID: &tg.PeerChannel{ChannelID: 20}},
	}
	indexes := discussionRootIndexes(messages, "channel:10", 20)
	if len(indexes) == 0 || indexes[0] != 2 {
		t.Fatalf("last discussion root must win, got %v", indexes)
	}
}

func TestDiscussionRootIndexesFallsBackFromLastMessage(t *testing.T) {
	messages := []tg.MessageClass{
		&tg.Message{ID: 900, PeerID: &tg.PeerChannel{ChannelID: 10}},
		&tg.Message{ID: 901, PeerID: &tg.PeerChannel{ChannelID: 20}},
	}
	indexes := discussionRootIndexes(messages, "channel:10", 0)
	if len(indexes) == 0 || indexes[0] != 1 {
		t.Fatalf("last returned message must be fallback root, got %v", indexes)
	}
}

func TestNoDiscussionErrorsAreNonFatal(t *testing.T) {
	if !isNoDiscussionError(assertError("rpc error: MSG_ID_INVALID")) {
		t.Fatal("expected missing discussion error")
	}
	if isNoDiscussionError(assertError("network timeout")) {
		t.Fatal("network errors must stay visible")
	}
}

func TestReplyPageNextOffsetAlwaysMovesTowardOlderReplies(t *testing.T) {
	page := []tg.MessageClass{&tg.Message{ID: 300}, &tg.Message{ID: 250}, &tg.Message{ID: 275}}
	if got := replyPageNextOffset(page, 0); got != 250 {
		t.Fatalf("got %d, want oldest page ID 250", got)
	}
	if got := replyPageNextOffset(page, 250); got != 250 {
		t.Fatalf("got %d, want unchanged offset for a repeated page", got)
	}
	if got := replyPageNextOffset([]tg.MessageClass{&tg.MessageEmpty{ID: 200}}, 250); got != 200 {
		t.Fatalf("deleted replies must advance offset: %d", got)
	}
}

func TestDiscussionAccessErrorsRemainVisible(t *testing.T) {
	if isNoDiscussionError(assertError("rpc error: CHANNEL_PRIVATE")) {
		t.Fatal("access failures must not be treated as an empty discussion")
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }

func TestCommentPrefixTemplate(t *testing.T) {
	path, err := renderName("{{ .OriginDialogName }}/{{ .OriginMessageID }}_{{ if .IsComment }}c_{{ end }}{{ .MessageID }}{{ .FileExt }}", source{Item: Item{OriginDialogName: "channel", OriginMessageID: 7, MessageID: 7, OriginalName: "a.bin"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(path, "c_") {
		t.Fatalf("normal source must not use comment prefix: %s", path)
	}
}
