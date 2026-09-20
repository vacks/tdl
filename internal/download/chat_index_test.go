package download

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/settings"
)

func TestChatStreamEnabledFollowsSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		config settings.Download
		want   map[string]bool
	}{
		{
			name:   "default image video",
			config: settings.Download{FileTypes: []string{"image", "video"}, IncludeReplies: true},
			want: map[string]bool{
				"photo_video": true, "document": false, "music": false, "round_voice": true, "reply_candidates": true,
			},
		},
		{
			name:   "document families",
			config: settings.Download{FileTypes: []string{"document", "sticker", "gif"}},
			want: map[string]bool{
				"photo_video": false, "document": true, "music": false, "round_voice": false, "reply_candidates": false,
			},
		},
		{
			name:   "no types means no api streams",
			config: settings.Download{IncludeReplies: true},
			want: map[string]bool{
				"photo_video": false, "document": false, "music": false, "round_voice": false, "reply_candidates": false,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, kind := range chatStreamKinds {
				if got := chatStreamEnabled(kind, test.config); got != test.want[kind] {
					t.Fatalf("stream %s enabled=%v, want %v", kind, got, test.want[kind])
				}
			}
		})
	}
}

func TestNextChatPageCursor(t *testing.T) {
	tests := []struct {
		name      string
		page      []tg.MessageClass
		previous  int
		minID     int
		wantNext  int
		wantDone  bool
		wantError bool
	}{
		{
			name:     "normal page advances to oldest",
			page:     []tg.MessageClass{&tg.Message{ID: 300}, &tg.Message{ID: 250}},
			wantNext: 250,
		},
		{
			name:     "deleted placeholder advances",
			page:     []tg.MessageClass{&tg.MessageEmpty{ID: 200}},
			previous: 250,
			wantNext: 200,
		},
		{
			name:     "service message advances",
			page:     []tg.MessageClass{&tg.MessageService{ID: 199}},
			previous: 200,
			wantNext: 199,
		},
		{
			name:     "repeated boundary moves one older",
			page:     []tg.MessageClass{&tg.Message{ID: 200}},
			previous: 200,
			minID:    1,
			wantNext: 199,
		},
		{
			name:     "range exhaustion completes",
			page:     []tg.MessageClass{&tg.Message{ID: 100}, &tg.MessageEmpty{ID: 80}},
			previous: 150,
			minID:    80,
			wantNext: 80,
			wantDone: true,
		},
		{
			name:      "unknown page is an error",
			page:      []tg.MessageClass{&tg.MessageEmpty{}},
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next, done, err := nextChatPageCursor(test.page, test.previous, test.minID)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v, want error=%v", err, test.wantError)
			}
			if test.wantError {
				return
			}
			if next != test.wantNext || done != test.wantDone {
				t.Fatalf("next=%d done=%v, want next=%d done=%v", next, done, test.wantNext, test.wantDone)
			}
		})
	}
}

func TestChatPageGroupsExpandOnlyActualPageBoundaries(t *testing.T) {
	grouped := func(id int, groupID int64) *tg.Message {
		message := &tg.Message{ID: id}
		message.SetGroupedID(groupID)
		return message
	}
	page := []tg.MessageClass{
		grouped(100, 10), // first group: can continue on the newer page.
		grouped(99, 10),
		&tg.Message{ID: 98},
		grouped(97, 20), // fully enclosed by ordinary page messages.
		grouped(96, 20),
		&tg.Message{ID: 95},
		grouped(94, 30), // last group: can continue on the older page.
	}
	groups := chatPageGroups(page, 1, 100)
	if len(groups) != 5 {
		t.Fatalf("group count=%d, want 5", len(groups))
	}
	if !groups[0].boundary || groups[0].id != 10 || len(groups[0].members) != 2 {
		t.Fatalf("first group=%+v, want boundary album 10", groups[0])
	}
	if groups[2].boundary || groups[2].id != 20 || len(groups[2].members) != 2 {
		t.Fatalf("middle group=%+v, want complete in-page album 20", groups[2])
	}
	if !groups[4].boundary || groups[4].id != 30 || len(groups[4].members) != 1 {
		t.Fatalf("last group=%+v, want boundary album 30", groups[4])
	}
}
