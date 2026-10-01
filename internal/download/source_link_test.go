package download

import "testing"

// TestMessageJumpURL pins the single rule behind both the Bot card's link and
// the Web UI's 消息链接 column. The reported defect was a task that the Bot
// linked to while the Web UI called it a private conversation, so the case that
// matters most is a reaction-created channel task: its source URL is internal
// bookkeeping, and the link has to come out of it anyway.
func TestMessageJumpURL(t *testing.T) {
	tests := []struct {
		name string
		job  Job
		want string
	}{
		{
			name: "reaction channel task links to the private channel route",
			job:  Job{SourceURL: "tg://reaction/channel/1934225758/9522", DialogType: "channel"},
			want: "https://t.me/c/1934225758/9522",
		},
		{
			name: "reaction channel route ignores files it does not need",
			job:  Job{SourceURL: "tg://reaction/channel/100/42", DialogType: "channel", Items: []Item{{DialogID: 100, MessageID: 42}}},
			want: "https://t.me/c/100/42",
		},
		{
			name: "public link is used as given",
			job:  Job{SourceURL: "https://t.me/getoutforchina/230888", DialogType: "channel"},
			want: "https://t.me/getoutforchina/230888",
		},
		{
			name: "www host and plain http are both public links",
			job:  Job{SourceURL: "http://www.t.me/example/42", DialogType: "channel"},
			want: "http://www.t.me/example/42",
		},
		{
			// Pins the boundary as it is rather than as it ought to be:
			// url.Parse gives this a path of "/", so it has always counted as a
			// link. Changing that would be a change to what the Bot shows.
			name: "a bare host with a trailing slash still counts as a link",
			job:  Job{SourceURL: "https://t.me/", DialogType: "channel", Items: []Item{{DialogID: 100, MessageID: 42}}},
			want: "https://t.me/",
		},
		{
			name: "a bare host with no path at all does not",
			job:  Job{SourceURL: "https://t.me", DialogType: "channel", Items: []Item{{DialogID: 100, MessageID: 42}}},
			want: "https://t.me/c/100/42",
		},
		{
			name: "channel task with an unaddressable source falls back to its files",
			job:  Job{SourceURL: "tg://message/channel/100/42", DialogType: "channel", Items: []Item{{DialogID: 100, MessageID: 42}}},
			want: "https://t.me/c/100/42",
		},
		{
			name: "channel task keeps looking past a file with no identifiers",
			job:  Job{SourceURL: "tg://message/channel/100/42", DialogType: "channel", Items: []Item{{DialogID: 0, MessageID: 0}, {DialogID: 100, MessageID: 42}}},
			want: "https://t.me/c/100/42",
		},
		{
			name: "reaction user task exposes no identifier",
			job:  Job{SourceURL: "tg://reaction/user/7959591811/3582", DialogType: "user", Items: []Item{{DialogID: 7959591811, MessageID: 3582}}},
			want: "",
		},
		{
			name: "reaction basic group task exposes no identifier",
			job:  Job{SourceURL: "tg://reaction/chat/100/42", DialogType: "chat", Items: []Item{{DialogID: 100, MessageID: 42}}},
			want: "",
		},
		{
			name: "reaction saved messages task exposes no identifier",
			job:  Job{SourceURL: "tg://reaction/self/account/42", DialogType: "self"},
			want: "",
		},
		{
			name: "channel task with no files and no usable source has no link",
			job:  Job{SourceURL: "tg://reaction/unknown/0/42", DialogType: "channel"},
			want: "",
		},
		{
			name: "a reaction route that is not a channel is not rewritten",
			job:  Job{SourceURL: "tg://reaction/unknown/0/42", DialogType: "channel", Items: []Item{{DialogID: 0, MessageID: 42}}},
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := MessageJumpURL(test.job); got != test.want {
				t.Fatalf("MessageJumpURL() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestMessageJumpURLRejectsMalformedReactionRoutes checks that only a run of
// digits is ever pasted into a URL. strconv.ParseInt alone would accept a sign
// and surrounding space, and the value ends up in an href.
func TestMessageJumpURLRejectsMalformedReactionRoutes(t *testing.T) {
	for _, raw := range []string{
		"tg://reaction/channel/",
		"tg://reaction/channel/100",
		"tg://reaction/channel/100/",
		"tg://reaction/channel//42",
		"tg://reaction/channel/abc/42",
		"tg://reaction/channel/100/abc",
		"tg://reaction/channel/100/42/extra",
		"tg://reaction/channel/-100/42",
		"tg://reaction/channel/100/-42",
		"tg://reaction/channel/+100/42",
		"tg://reaction/channel/ 100/42",
		"tg://reaction/channel/100/0",
		"tg://reaction/channel/0/42",
		"tg://reaction/channel/99999999999999999999999/42",
	} {
		t.Run(raw, func(t *testing.T) {
			if got := reactionChannelLink(raw); got != "" {
				t.Fatalf("reactionChannelLink(%q) = %q, want no link", raw, got)
			}
		})
	}
}
