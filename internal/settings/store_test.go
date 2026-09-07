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
