package download

import (
	"testing"

	"github.com/gotd/td/tg"
)

// A caption belongs to the album, and an album with none takes the post's.
//
// Telegram attaches one caption to one member of a group and leaves the rest
// empty, which is why the rule is per album rather than per message: naming each
// member from its own text would leave eight of a nine-photo album unnamed. The
// case that was missing is the album with no caption at all - a comment posted
// as a bare photo or video - whose files were named from nothing at all.
func TestAnAlbumsCaptionFallsBackToThePosts(t *testing.T) {
	single := func(id int, text string) []*tg.Message {
		return []*tg.Message{{ID: id, Message: text}}
	}
	post := single(1, "原帖的文字")

	// The album's own caption wins.
	if got := albumCaption(single(7, "评论自己的文字"), post); got != "评论自己的文字" {
		t.Fatalf("album with a caption of its own = %q; want its own text", got)
	}
	// A caption on one member of a group belongs to every member of it.
	album := []*tg.Message{{ID: 7, Message: "相册的 caption"}, {ID: 8}, {ID: 9}}
	if got := albumCaption(album, post); got != "相册的 caption" {
		t.Fatalf("members of one album = %q; want the album's single caption", got)
	}
	// No caption anywhere in the album: the post's.
	if got := albumCaption([]*tg.Message{{ID: 7}, {ID: 8}}, post); got != "原帖的文字" {
		t.Fatalf("album without a caption = %q; want the post's text", got)
	}
	// Neither has one: nothing to name it from, which is not an error.
	if got := albumCaption([]*tg.Message{{ID: 7}}, single(1, "")); got != "" {
		t.Fatalf("album and post both without text = %q; want empty", got)
	}
}
