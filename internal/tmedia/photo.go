package tmedia

import (
	"strconv"

	"github.com/gotd/td/tg"
)

// GetPhotoInfo describes the largest rendition of a photo.
func GetPhotoInfo(photo *tg.MessageMediaPhoto) (*Media, bool) {
	p, ok := photo.Photo.(*tg.Photo)
	if !ok {
		return nil, false
	}
	thumbSize, size, ok := GetPhotoSize(p.Sizes)
	if !ok {
		return nil, false
	}
	return &Media{
		InputFileLoc: &tg.InputPhotoFileLocation{
			ID:            p.ID,
			AccessHash:    p.AccessHash,
			FileReference: p.FileReference,
			ThumbSize:     thumbSize,
		},
		// A photo is re-encoded by Telegram, so the extension is always jpg and
		// the id is the only stable name available. The name has to be unique per
		// photo, because the temporary file is named after it.
		Name: strconv.FormatInt(p.ID, 10) + ".jpg",
		Size: int64(size),
		DC:   p.DCID,
		Date: int64(p.Date),
	}, true
}

// GetPhotoSize returns the type and size of the largest rendition listed.
//
// Telegram lists renditions smallest first, so the last entry is the full-size
// one. A photo with no renditions at all is reported as not downloadable rather
// than indexed out of bounds - the upstream implementation read the last
// element unchecked, which would have crashed a download worker on a malformed
// photo.
func GetPhotoSize(sizes []tg.PhotoSizeClass) (string, int, bool) {
	if len(sizes) == 0 {
		return "", 0, false
	}
	switch last := sizes[len(sizes)-1].(type) {
	case *tg.PhotoSize:
		return last.Type, last.Size, true
	case *tg.PhotoSizeProgressive:
		if len(last.Sizes) == 0 {
			return "", 0, false
		}
		return last.Type, last.Sizes[len(last.Sizes)-1], true
	}
	return "", 0, false
}
