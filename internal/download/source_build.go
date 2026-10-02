package download

import (
	"github.com/gotd/td/tg"

	"github.com/vacks/tdl/internal/tmedia"
)

// A file record is built from a message in four places: resolving a link,
// resolving a listener or reaction event, indexing a channel's history, and
// walking a discussion group's replies. What those four share is where each
// field comes from - the id and the album from the message, the name and the
// size from its media, the type from both - and what they differ about is how
// the record is decorated: which dialog it belongs to, whether it is a comment,
// which peer to fetch it through.
//
// The shared part is here, once. The four loops used to repeat it, which is the
// shape that drifts: a change to where a name comes from is a change in four
// places, and three of them still work afterwards.
func fileFromMessage(item Item, message *tg.Message, media *tmedia.Media) Item {
	item.MessageID = message.ID
	item.GroupedID, _ = message.GetGroupedID()
	item.OriginalName = media.Name
	item.Size = media.Size
	return item
}

// sourceFromMessage builds one file of a task from the message it came in and
// the media that message carries.
func sourceFromMessage(item Item, message *tg.Message, media *tmedia.Media, dialogName string) source {
	return source{Item: fileFromMessage(item, message, media), DialogName: dialogName, MediaType: messageMediaType(message)}
}
