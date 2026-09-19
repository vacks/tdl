package download

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/settings"
)

func documentMessage(attributes ...tg.DocumentAttributeClass) *tg.Message {
	return messageWithMedia(&tg.MessageMediaDocument{Document: &tg.Document{Attributes: attributes}})
}

func TestFilterSourcesUsesSemanticMediaTypes(t *testing.T) {
	sources := []source{
		{Item: Item{Size: 1}, MediaType: "image"},
		{Item: Item{Size: 1}, MediaType: "video"},
		{Item: Item{Size: 1}, MediaType: "gif"},
		{Item: Item{Size: 1}, MediaType: "music"},
		{Item: Item{Size: 1}, MediaType: "voice"},
		{Item: Item{Size: 1}, MediaType: "sticker"},
		{Item: Item{Size: 1}, MediaType: "document"},
	}
	got := filterSources(sources, settings.Download{FileTypes: []string{"sticker", "voice", "gif"}})
	if len(got) != 3 || got[0].MediaType != "gif" || got[1].MediaType != "voice" || got[2].MediaType != "sticker" {
		t.Fatalf("semantic type filtering = %#v", got)
	}
}

func TestFilterSourcesWithNoSelectedTypeDownloadsNothing(t *testing.T) {
	got := filterSources([]source{{Item: Item{Size: 1}, MediaType: "image"}}, settings.Download{})
	if len(got) != 0 {
		t.Fatalf("empty type selection must download nothing, got %#v", got)
	}
}

func messageWithMedia(media tg.MessageMediaClass) *tg.Message {
	message := &tg.Message{}
	message.SetMedia(media)
	return message
}

func TestMessageMediaTypeUsesTelegramDocumentAttributes(t *testing.T) {
	tests := []struct {
		name    string
		message *tg.Message
		want    string
	}{
		{name: "native photo", message: messageWithMedia(&tg.MessageMediaPhoto{}), want: "image"},
		{name: "sticker webp is sticker", message: documentMessage(&tg.DocumentAttributeSticker{}), want: "sticker"},
		{name: "animated document is gif", message: documentMessage(&tg.DocumentAttributeAnimated{}), want: "gif"},
		{name: "music", message: documentMessage(&tg.DocumentAttributeAudio{}), want: "music"},
		{name: "voice", message: documentMessage(&tg.DocumentAttributeAudio{Voice: true}), want: "voice"},
		{name: "video", message: documentMessage(&tg.DocumentAttributeVideo{}), want: "video"},
		{name: "image mime sent as file is document", message: messageWithMedia(&tg.MessageMediaDocument{Document: &tg.Document{MimeType: "image/webp"}}), want: "document"},
		{name: "sticker takes precedence over animation", message: documentMessage(&tg.DocumentAttributeAnimated{}, &tg.DocumentAttributeSticker{}), want: "sticker"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := messageMediaType(test.message); got != test.want {
				t.Fatalf("messageMediaType() = %q, want %q", got, test.want)
			}
		})
	}
}
