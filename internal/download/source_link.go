package download

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// MessageJumpURL returns the externally usable Telegram link for a task, or ""
// when the task genuinely has none. The Bot card's "点击查看" and the Web UI's
// 消息链接 column are the same question asked twice, and they used to answer it
// differently: the Bot fell back to the private-channel route, while the Web UI
// only ever looked at SourceURL and so reported every reaction-created task as
// a private conversation with no link - even when the Bot's card for that same
// task had a working one.
//
// The reaction fallback is a pure function of the URL, deliberately not of the
// task's files. A task created by a reaction stores
// tg://reaction/<kind>/<id>/<message> as its source: internal bookkeeping that
// must never be shown to a user, and for a channel it already carries both
// numbers the /c/ route is built from. Reading them here is also what makes the
// link available to the task list, which reads no download_items rows at all.
func MessageJumpURL(job Job) string {
	if sourceTelegramURL(job.SourceURL) {
		return job.SourceURL
	}
	if link := reactionChannelLink(job.SourceURL); link != "" {
		return link
	}
	// Channel tasks whose source URL is not a Telegram link at all still get a
	// route from the numeric ID stored per file. Only channels have one: a
	// private chat's ID is not addressable this way, so exposing it would be
	// wrong rather than merely unhelpful.
	if job.DialogType != "channel" {
		return ""
	}
	for _, item := range job.Items {
		if item.DialogID > 0 && item.MessageID > 0 {
			return fmt.Sprintf("https://t.me/c/%d/%d", item.DialogID, item.MessageID)
		}
	}
	return ""
}

// reactionChannelLink converts tg://reaction/channel/<channel>/<message> into
// the private-channel route clients accept. The other reaction kinds name a
// user, a basic group, or the account itself; none of those has an externally
// usable route, so they return "".
func reactionChannelLink(raw string) string {
	const prefix = "tg://reaction/channel/"
	if !strings.HasPrefix(raw, prefix) {
		return ""
	}
	channel, message, ok := strings.Cut(raw[len(prefix):], "/")
	if !ok || !decimal(channel) || !decimal(message) {
		return ""
	}
	channelID, err := strconv.ParseInt(channel, 10, 64)
	if err != nil || channelID <= 0 {
		return ""
	}
	messageID, err := strconv.ParseInt(message, 10, 64)
	if err != nil || messageID <= 0 {
		return ""
	}
	return fmt.Sprintf("https://t.me/c/%d/%d", channelID, messageID)
}

// decimal reports whether value is a non-empty run of ASCII digits. It gates
// strconv.ParseInt so that the signs, underscores and surrounding spaces it
// would otherwise accept cannot reach a URL.
func decimal(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// sourceTelegramURL reports whether raw is a link a user can open. A bare host
// is not one, so the path must be present: "https://t.me" is rejected. This is
// the rule the Bot card has always used, kept byte for byte, because it decides
// what a user is shown and re-deciding it is not what unifies the two callers.
// Note the boundary it draws: "https://t.me/" parses with a path of "/" and is
// therefore accepted, which is why the tests pin that case rather than assume
// it.
func sourceTelegramURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Path == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return host == "t.me" || host == "www.t.me"
}
