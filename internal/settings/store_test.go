package settings

import (
	"path/filepath"
	"testing"
)

func TestCanonicalReactionEmoji(t *testing.T) {
	if got := CanonicalReactionEmoji("❤️"); got != "❤" {
		t.Fatalf("heart with variation selector = %q, want %q", got, "❤")
	}
	if got := CanonicalReactionEmoji("☀️"); got != "☀" {
		t.Fatalf("sun with variation selector = %q, want %q", got, "☀")
	}
	if got := CanonicalReactionEmoji("👍🏽"); got != "👍🏽" {
		t.Fatalf("skin tone emoji changed: %q", got)
	}
}

func TestValidateFilenameTemplatesBeforeSaving(t *testing.T) {
	values := Defaults()
	values.Download.FinalFilenameTemplate = "{{ .Missing }}"
	if err := Validate(values); err == nil {
		t.Fatal("invalid final template was accepted")
	}
	values = Defaults()
	values.Download.TempFilenameTemplate = "{{ if .FileName }}"
	if err := Validate(values); err == nil {
		t.Fatal("invalid temporary template was accepted")
	}
	values = Defaults()
	values.Download.TempFilenameTemplate = "{{ .Missing }}"
	if err := Validate(values); err == nil {
		t.Fatal("temporary template with an unknown variable was accepted")
	}
}

func TestValidateDownloadFilters(t *testing.T) {
	values := Defaults()
	values.Download.MinFileSizeMB = 20
	values.Download.MaxFileSizeMB = 10
	if err := Validate(values); err == nil {
		t.Fatal("inverted file size range was accepted")
	}
	values = Defaults()
	values.Download.FileTypes = []string{"video", "unknown"}
	if err := Validate(values); err == nil {
		t.Fatal("unknown file type was accepted")
	}
	values = Defaults()
	values.Download.MinFileSizeMB = 1
	values.Download.MaxFileSizeMB = 100
	values.Download.FileTypes = []string{"image", "audio"}
	if err := Validate(values); err != nil {
		t.Fatalf("valid filters rejected: %v", err)
	}
}

func TestDiscussionRepliesDefaultEnabled(t *testing.T) {
	if !Defaults().Download.IncludeReplies {
		t.Fatal("discussion/reply downloads must default to enabled")
	}
}

func TestUpdateNormalizesAndPersistsConfiguration(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	values := Defaults()
	values.ProxyURL = "socks5h://proxy.example:1080"
	values.Download.FileTypes = []string{" VIDEO ", "video", "audio", ""}
	values.Bot.ControlUserID = 7 // Legacy field must migrate to the multi-user form.
	values.Bot.ControlUserIDs = []int64{7, 9, 9, -1}
	values.Reaction.Emojis = []string{"❤️", "❤", " 👍 ", "👍"}
	if err := store.Update(values); err != nil {
		t.Fatalf("Update(): %v", err)
	}
	got := store.Get()
	if len(got.Download.FileTypes) != 2 || got.Download.FileTypes[0] != "video" || got.Download.FileTypes[1] != "audio" {
		t.Fatalf("normalized file types = %#v", got.Download.FileTypes)
	}
	if len(got.Bot.ControlUserIDs) != 2 || got.Bot.ControlUserIDs[0] != 7 || got.Bot.ControlUserIDs[1] != 9 || got.Bot.ControlUserID != 0 {
		t.Fatalf("normalized control users = %#v, legacy=%d", got.Bot.ControlUserIDs, got.Bot.ControlUserID)
	}
	if len(got.Reaction.Emojis) != 2 || got.Reaction.Emojis[0] != "❤️" || got.Reaction.Emojis[1] != "👍" {
		t.Fatalf("normalized emojis = %#v", got.Reaction.Emojis)
	}
	reopened, err := Open(filepath.Clean(dir))
	if err != nil {
		t.Fatal(err)
	}
	if reloaded := reopened.Get(); reloaded.ProxyURL != got.ProxyURL || len(reloaded.Bot.ControlUserIDs) != 2 || len(reloaded.Reaction.Emojis) != 2 {
		t.Fatalf("persisted settings = %#v", reloaded)
	}
}
