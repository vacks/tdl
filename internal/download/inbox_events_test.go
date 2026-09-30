package download

import (
	"encoding/base64"
	"strconv"
	"testing"
)

// TestMergeListenerEventsInterleavesBothArmsNewestFirst covers the merge that
// replaces a UNION ALL: the two inbox tables cannot be ordered as one sequence
// by ID, so each arm is read newest first and the two have to be interleaved
// without dropping or reordering a row.
func TestMergeListenerEventsInterleavesBothArmsNewestFirst(t *testing.T) {
	messages := []ListenerEvent{
		{Source: "message", ID: 30, CreatedAt: "2026-09-30T10:30:00Z"},
		{Source: "message", ID: 20, CreatedAt: "2026-09-30T10:20:00Z"},
		{Source: "message", ID: 10, CreatedAt: "2026-09-30T10:10:00Z"},
	}
	reactions := []ListenerEvent{
		{Source: "reaction", ID: 25, CreatedAt: "2026-09-30T10:25:00Z"},
		// Same instant as the message event below it. The arms have independent
		// id sequences, so a shared created_at is the one place a cursor can
		// land inside a tie, and the order still has to be total for the cursor
		// to be safe there: the larger ID wins, which is the rule each arm's
		// own "id DESC" already applies within itself.
		{Source: "reaction", ID: 11, CreatedAt: "2026-09-30T10:10:00Z"},
		{Source: "reaction", ID: 5, CreatedAt: "2026-09-30T10:05:00Z"},
	}
	want := []string{"message:30", "reaction:25", "message:20", "reaction:11", "message:10", "reaction:5"}
	merged := mergeListenerEvents(messages, reactions)
	if len(merged) != len(want) {
		t.Fatalf("mergeListenerEvents() returned %d rows, want %d: %v", len(merged), len(want), merged)
	}
	for index, key := range want {
		if got := eventKey(merged[index]); got != key {
			t.Fatalf("mergeListenerEvents()[%d] = %s, want %s (full order %v)", index, got, key, merged)
		}
	}
}

func eventKey(event ListenerEvent) string {
	return event.Source + ":" + strconv.FormatInt(event.ID, 10)
}

// TestEventCursorRoundTripsBothArms pins the property the Bot depends on: the
// cursor is opaque, survives a page boundary, and carries one keyset position
// per inbox rather than one for the merged list.
func TestEventCursorRoundTripsBothArms(t *testing.T) {
	message := formatArmCursor("2026-09-30T10:10:00Z", 10)
	reaction := formatArmCursor("2026-09-30T10:05:00Z", 5)
	decodedMessage, decodedReaction, err := decodeEventCursor(encodeEventCursor(message, reaction))
	if err != nil {
		t.Fatalf("decodeEventCursor() error = %v", err)
	}
	if decodedMessage != message || decodedReaction != reaction {
		t.Fatalf("decodeEventCursor() = %q/%q, want %q/%q", decodedMessage, decodedReaction, message, reaction)
	}
	// One arm can be untouched: a page taken entirely from the other inbox
	// leaves its cursor empty, which means "start at that inbox's newest row".
	if message, reaction, err := decodeEventCursor(encodeEventCursor("", reaction)); err != nil || message != "" || reaction != reaction {
		t.Fatalf("decodeEventCursor() with an empty first arm = %q/%q/%v", message, reaction, err)
	}
	if message, reaction, err := decodeEventCursor(""); err != nil || message != "" || reaction != "" {
		t.Fatalf("decodeEventCursor(\"\") = %q/%q/%v, want the first page", message, reaction, err)
	}
}

func TestDecodeEventCursorRejectsMalformedValues(t *testing.T) {
	cases := map[string]string{
		"not base64":       "!!!!",
		"no arm separator": base64.RawURLEncoding.EncodeToString([]byte("one-arm-only")),
		// A well-formed pair of arms is not enough: each arm must still carry
		// both halves of its keyset position.
		"arm without an id":   encodeEventCursor(formatArmCursor("2026-09-30T10:10:00Z", 1), "2026-09-30T10:10:00Z"),
		"arm without a stamp": encodeEventCursor("", "\x001"),
	}
	for name, cursor := range cases {
		if _, _, err := decodeEventCursor(cursor); err == nil {
			t.Fatalf("decodeEventCursor(%s) accepted an invalid cursor", name)
		}
	}
}
