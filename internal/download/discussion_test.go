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

func TestNoDiscussionErrorsAreNonFatal(t *testing.T) {
	if !isNoDiscussionError(assertError("rpc error: MSG_ID_INVALID")) {
		t.Fatal("expected missing discussion error")
	}
	if isNoDiscussionError(assertError("network timeout")) {
		t.Fatal("network errors must stay visible")
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
