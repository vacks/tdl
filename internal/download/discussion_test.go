package download

import (
	"context"
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

// A channel post that declares neither comments nor a linked discussion has
// nothing under it to read, and Telegram states both facts on the post itself.
// The nil client is the assertion: reaching the network would fault, so a clean
// return is proof that no request was issued. The listener path opts out of
// this shortcut and still resolves the thread, which is why learnRoot exists.
func TestRelatedSourcesSkipsPostThatDeclaresNoComments(t *testing.T) {
	post := &tg.Message{ID: 100}
	peer := &tg.InputPeerChannel{ChannelID: 5, AccessHash: 7}
	items, err := relatedSources(&Manager{}, context.Background(), nil, "acct", peer, "频道", []*tg.Message{post}, 100, 100, false)
	if err != nil {
		t.Fatalf("a post with no comment section must resolve without an error: %v", err)
	}
	if items != nil {
		t.Fatalf("a post with no comment section must yield no items: %#v", items)
	}
}

// The shortcut above must not swallow a post that declares a linked discussion,
// even while it has no comments yet: that group is where the first comment will
// land. It also must not swallow a post that declares a comment count, since
// that is exactly the inconsistent case the caller reports.
func TestShouldReadDiscussionOnlySkipsSilentPosts(t *testing.T) {
	cases := []struct {
		name        string
		expectation discussionExpectation
		learnRoot   bool
		want        bool
	}{
		{"post with no comment section", discussionExpectation{}, false, false},
		{"post with no comment section, listener", discussionExpectation{}, true, true},
		{"post with a linked discussion and no comments yet", discussionExpectation{dialogID: 77}, false, true},
		{"post declaring comments", discussionExpectation{replyCount: 3}, false, true},
		{"post declaring both", discussionExpectation{dialogID: 77, replyCount: 3}, false, true},
	}
	for _, testCase := range cases {
		if got := shouldReadDiscussion(testCase.expectation, testCase.learnRoot); got != testCase.want {
			t.Fatalf("%s: got %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// The gap walk re-reads history from the newest message backwards until it
// meets the watermark. Only the page that meets it licenses advancing that
// watermark: a page entirely above it means the walk has not finished, and an
// interrupted walk that had already advanced would stop on its first page next
// time and never reach the stretch it exists to recover.
func TestGapBatchOnlyReturnsMessagesAboveTheWatermark(t *testing.T) {
	page := func(ids ...int) []tg.MessageClass {
		result := make([]tg.MessageClass, 0, len(ids))
		for _, id := range ids {
			result = append(result, &tg.Message{ID: id})
		}
		return result
	}

	above, oldest, reached := gapBatch(page(120, 115, 110), 100)
	if len(above) != 3 || oldest != 110 || reached {
		t.Fatalf("a page entirely above the watermark must not license advancing it: above=%d oldest=%d reached=%v", len(above), oldest, reached)
	}

	// The page that reaches the watermark still carries the messages just above
	// it, so those must be returned even though the walk stops here.
	above, oldest, reached = gapBatch(page(105, 100, 95), 100)
	if len(above) != 1 || above[0].ID != 105 || oldest != 95 || !reached {
		t.Fatalf("the straddling page must still yield what is above the watermark: above=%v oldest=%d reached=%v", above, oldest, reached)
	}

	// Running out of history means the watermark was passed.
	if above, _, reached = gapBatch(nil, 100); len(above) != 0 || !reached {
		t.Fatalf("an exhausted history must license advancing the watermark: above=%d reached=%v", len(above), reached)
	}
}

// Every listened channel owes a gap walk, not just Saved Messages. A task that
// does not listen, or has not finished indexing, has no settled watermark to
// walk back to.
func TestListensForNewMediaCoversChannelsNotOnlySaved(t *testing.T) {
	cases := []struct {
		name string
		job  ChatJob
		want bool
	}{
		{"listened channel", ChatJob{DialogType: "channel", ListenNew: true, ScanState: chatScanCompleted, Status: ChatStatusListening}, true},
		{"listened channel still downloading history", ChatJob{DialogType: "channel", ListenNew: true, ScanState: chatScanCompleted, Status: ChatStatusDownloading}, true},
		{"saved listener", ChatJob{DialogType: "self", ListenNew: true, ScanState: chatScanCompleted, Status: ChatStatusListening}, true},
		{"not listening", ChatJob{DialogType: "channel", ListenNew: false, ScanState: chatScanCompleted, Status: ChatStatusDownloading}, false},
		{"still scanning", ChatJob{DialogType: "channel", ListenNew: true, ScanState: chatScanIndexing, Status: ChatStatusScanning}, false},
		{"finished task", ChatJob{DialogType: "channel", ListenNew: true, ScanState: chatScanCompleted, Status: ChatStatusCompleted}, false},
	}
	for _, testCase := range cases {
		if got := listensForNewMedia(testCase.job); got != testCase.want {
			t.Fatalf("%s: got %v, want %v", testCase.name, got, testCase.want)
		}
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
