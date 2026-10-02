// Package tmedia extracts what is downloadable out of a Telegram message.
//
// It is this application's own copy of the media extraction the upstream TDL
// library performed. The parts of that library which turn media back into
// something sendable - converting a message into an InputMedia, for uploads and
// forwards - are deliberately not here: this application only ever reads.
package tmedia

import (
	"github.com/gotd/td/tg"
)

// Media is one downloadable file: where it lives, what it is called, how big it
// is, and which datacenter holds it.
type Media struct {
	InputFileLoc tg.InputFileLocationClass // mtproto file location
	Name         string                    // file name
	Size         int64                     // size in bytes
	DC           int                       // which DC the media is stored on
	Date         int64                     // media creation (upload) timestamp
}

// ExtractMedia reports the downloadable file in a piece of message media.
func ExtractMedia(m tg.MessageMediaClass) (*Media, bool) {
	switch m := m.(type) {
	case *tg.MessageMediaPhoto:
		return GetPhotoInfo(m)
	case *tg.MessageMediaDocument:
		return GetDocumentInfo(m)
	case *tg.MessageMediaInvoice:
		return GetExtendedMedia(m.ExtendedMedia)
	}
	return nil, false
}

// GetMedia reports the downloadable file in a message.
//
// A message that carries no media - text, a service message, a poll - answers
// false, which is the ordinary case for most of what a history scan walks past.
func GetMedia(msg tg.MessageClass) (*Media, bool) {
	message, ok := msg.(*tg.Message)
	if !ok {
		return nil, false
	}
	media, ok := message.GetMedia()
	if !ok {
		return nil, false
	}
	return ExtractMedia(media)
}

// GetExtendedMedia unwraps the media of a paid message.
func GetExtendedMedia(m tg.MessageExtendedMediaClass) (*Media, bool) {
	extended, ok := m.(*tg.MessageExtendedMedia)
	if !ok {
		return nil, false
	}
	return ExtractMedia(extended.Media)
}
