package settings

import "testing"

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
