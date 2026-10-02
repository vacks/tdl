package tmedia

import (
	"strconv"

	"github.com/gabriel-vasile/mimetype"
	"github.com/gotd/td/tg"
)

// GetDocumentInfo describes a document - which in Telegram means every file
// that is not a photo: video, audio, archives, voice notes, round video.
func GetDocumentInfo(doc *tg.MessageMediaDocument) (*Media, bool) {
	d, ok := doc.Document.(*tg.Document)
	if !ok {
		return nil, false
	}
	return &Media{
		InputFileLoc: &tg.InputDocumentFileLocation{
			ID:            d.ID,
			AccessHash:    d.AccessHash,
			FileReference: d.FileReference,
		},
		Name: GetDocumentName(d),
		Size: d.Size,
		DC:   d.DCID,
		Date: int64(d.Date),
	}, true
}

// GetDocumentName returns the file's name.
//
// Not every document carries a filename: a round video message, a voice note
// and a sticker are documents whose only identity is their id and their MIME
// type. Those still get downloaded, and the name below becomes the published
// file name, so the extension is resolved from the declared MIME type rather
// than guessed. The id makes it unique, which the temporary file name requires.
func GetDocumentName(doc *tg.Document) string {
	for _, attr := range doc.Attributes {
		if name, ok := attr.(*tg.DocumentAttributeFilename); ok {
			return name.FileName
		}
	}

	extension := ".unknown"
	if mime := mimetype.Lookup(doc.MimeType); mime != nil {
		extension = mime.Extension()
	}
	return strconv.FormatInt(doc.ID, 10) + extension
}
