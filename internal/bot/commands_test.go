package bot

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/vacks/tdl/internal/buildinfo"
	"github.com/vacks/tdl/internal/download"
)

// The help message is the contract a user reads, so it is pinned line by line
// rather than by keywords. A command renamed in the dispatcher but not here -
// or here but not in the dispatcher - leaves a person following instructions
// that do nothing.
func TestHelpTextIsTheDocumentedCommandList(t *testing.T) {
	want := []string{
		"<b>TDL帮助</b>",
		"版本：TDL 管理 " + buildinfo.Version + " · 上游 TDL " + buildinfo.UpstreamVersion,
		"",
		"发送 Telegram 消息链接或转发消息即可创建消息下载任务。",
		"",
		"<code>/help</code> 获取帮助信息",
		"<code>/status</code> 获取TDL当前状态",
		"<code>/config</code> 获取TDL当前配置",
		"<code>/restart</code> 重启TDL所有服务",
		"<code>/tasks</code> 获取所有消息下载任务",
		"<code>/task_filter</code> 筛选获取消息下载任务",
		"<code>/chats [链接]</code> 获取/创建会话类型下载",
		"<code>/saved_task</code> 获取收藏夹任务",
		"<code>/saved_all</code> 下载收藏夹历史消息",
		"<code>/saved_listen</code> 开始/停止监听收藏夹新消息",
		"<code>/events</code> 获取监听的正在处理事件",
		"<code>/event_clear</code> 清空已停止重试的事件",
	}
	got := strings.Split(helpText(), "\n")
	if len(got) != len(want) {
		t.Fatalf("the help message has %d lines, want %d:\n%s", len(got), len(want), helpText())
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("help line %d = %q, want %q", index+1, got[index], want[index])
		}
	}
}

var helpCommandPattern = regexp.MustCompile(`<code>(/[^<\s]+)`)

// The menu Telegram shows above the input field and the list /help prints are
// two renderings of one set of commands. A command in either and not the other
// is a control whose existence nothing explains.
func TestCommandMenuAndHelpTextAgree(t *testing.T) {
	menu := map[string]bool{}
	for _, entry := range botCommands() {
		name := "/" + entry["command"]
		if menu[name] {
			t.Fatalf("%s is offered twice in the command menu", name)
		}
		menu[name] = true
	}
	listed := map[string]bool{}
	for _, match := range helpCommandPattern.FindAllStringSubmatch(helpText(), -1) {
		name := match[1]
		if listed[name] {
			t.Fatalf("%s is listed twice in the help message", name)
		}
		listed[name] = true
		if !menu[name] {
			t.Fatalf("the help message lists %s, which the command menu does not offer", name)
		}
		if kind, _ := parseCommand(name); kind == commandUnknown || kind == commandLink {
			t.Fatalf("the help message offers %s, which the Bot does not recognize", name)
		}
	}
	for name := range menu {
		if !listed[name] {
			t.Fatalf("the command menu offers %s, which the help message never mentions", name)
		}
	}
}

// The rename is the whole point of this change, and its failure mode is silent:
// a command the dispatcher no longer recognizes is not an error, it is the
// "send /help" reply. Both directions are pinned - every new name is recognized
// and every old one is not, so a stale alias cannot quietly survive either.
func TestRenamedCommandsReplaceTheOldNames(t *testing.T) {
	names := map[string]command{
		"/tasks":        commandTasks,
		"/task_filter":  commandTaskFilter,
		"/saved_all":    commandSavedAll,
		"/saved_listen": commandSavedListen,
		"/saved_task":   commandSavedTask,
		"/events":       commandEvents,
		"/event_clear":  commandEventClear,
	}
	for text, want := range names {
		if got, _ := parseCommand(text); got != want {
			t.Errorf("parseCommand(%q) = %v, want %v", text, got, want)
		}
		// Telegram appends the Bot's name when a command is picked from the menu.
		if got, _ := parseCommand(normalizeCommand(text + "@TDLControlBot")); got != want {
			t.Errorf("parseCommand(%q@Bot) = %v, want %v", text, got, want)
		}
	}
	for _, text := range []string{
		"/saved", "/saved all", "/saved listen", "/saved stop", "/saved status",
		"/event", "/event clear", "/tasks 下载中", "/tasks 排队中",
	} {
		if got, _ := parseCommand(normalizeCommand(text)); got != commandUnknown {
			t.Errorf("parseCommand(%q) = %v; the old command still runs", text, got)
		}
	}
	// /chats keeps its argument, which is a link rather than a subcommand.
	if kind, argument := parseCommand("/chats https://t.me/example/1"); kind != commandChatCreate || argument != "https://t.me/example/1" {
		t.Fatalf("parseCommand(/chats <link>) = %v, %q", kind, argument)
	}
	if kind, _ := parseCommand("/chats"); kind != commandChatList {
		t.Fatal("a bare /chats no longer lists the sessions")
	}
}

// A status is only offered as a button if the downloader can actually filter by
// it: the button's payload is normalized by the same function every other entry
// point uses, and a filter that function rejects would be a button that answers
// with nothing.
func TestTaskFilterButtonsOnlyOfferFiltersThatProduceAList(t *testing.T) {
	offered := map[string]bool{}
	rows := taskFilterKeyboard()
	if len(rows) == 0 {
		t.Fatal("the filter keyboard is empty")
	}
	for _, row := range rows {
		if len(row) > 3 {
			t.Fatalf("a keyboard row of %d buttons will not fit a phone", len(row))
		}
		for _, key := range row {
			// Telegram rejects the whole message when callback_data is longer.
			if len(key.CallbackData) > 64 {
				t.Fatalf("callback data %q is %d bytes, over Telegram's limit of 64", key.CallbackData, len(key.CallbackData))
			}
			if !strings.HasPrefix(key.CallbackData, "f:") {
				t.Fatalf("the filter button %q carries %q, which the callback dispatcher does not route", key.Text, key.CallbackData)
			}
			status, err := download.NormalizeTaskStatusFilter(strings.TrimPrefix(key.CallbackData, "f:"))
			if err != nil {
				t.Fatalf("the button %q cannot produce a list: %v", key.Text, err)
			}
			if offered[key.Text] {
				t.Fatalf("%s is offered twice", key.Text)
			}
			offered[key.Text] = true
			// An empty status is the unfiltered list, which has no label of its own.
			if want := download.TaskStatusFilterLabel(status); status != "" && key.Text != want {
				t.Fatalf("button %q leads to status %q, whose label is %q", key.Text, status, want)
			}
		}
	}
	for _, status := range download.TaskStatusFilters() {
		if label := download.TaskStatusFilterLabel(status); !offered[label] {
			t.Fatalf("%s (%s) is not offered by the filter keyboard", label, status)
		}
	}
	if !offered["全部"] {
		t.Fatal("there is no way back to the unfiltered list from the filter keyboard")
	}
}

// A forwarded post carries the chat and the message id, which is exactly what a
// link carries, so forwarding reaches the same submission path as pasting a
// link. The cases below are the two ways the Bot API describes a forward and
// the shapes that are deliberately left alone.
func TestForwardedMessageBecomesAMessageLink(t *testing.T) {
	chat := func(id int64, username string) *forwardOriginChat {
		return &forwardOriginChat{ID: id, Username: username}
	}
	tests := []struct {
		name string
		msg  message
		want string
	}{
		{
			name: "a public channel resolves by name",
			msg:  message{ForwardOrigin: &forwardOrigin{Chat: chat(-1001234567890, "example"), MessageID: 42}},
			want: "https://t.me/example/42",
		},
		{
			name: "a channel this instance never resolved still resolves",
			msg:  message{ForwardOrigin: &forwardOrigin{Chat: chat(-1001934225758, ""), MessageID: 9522}},
			want: "https://t.me/c/1934225758/9522",
		},
		{
			name: "the pre-7.0 fields still describe a forward",
			msg:  message{ForwardFromChat: chat(-1001934225758, ""), ForwardFromMessageID: 9522},
			want: "https://t.me/c/1934225758/9522",
		},
		{
			name: "an @ kept by a client is not part of the name",
			msg:  message{ForwardOrigin: &forwardOrigin{Chat: chat(-1001234567890, "@example"), MessageID: 42}},
			want: "https://t.me/example/42",
		},
		{
			name: "a forward from a user has no message link",
			msg:  message{ForwardOrigin: &forwardOrigin{MessageID: 42}},
		},
		{
			name: "a basic group has no message link",
			msg:  message{ForwardOrigin: &forwardOrigin{Chat: chat(-123456, ""), MessageID: 42}},
		},
		{
			name: "a forward without a message id addresses nothing",
			msg:  message{ForwardOrigin: &forwardOrigin{Chat: chat(-1001234567890, "")}},
		},
		{
			name: "an ordinary message is not a forward",
			msg:  message{Text: "https://t.me/example/1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := forwardedMessageURL(test.msg)
			if test.want == "" {
				if ok {
					t.Fatalf("forwardedMessageURL() = %q, want no link: nothing can resolve it", got)
				}
				return
			}
			if !ok || got != test.want {
				t.Fatalf("forwardedMessageURL() = %q, %v; want %q", got, ok, test.want)
			}
		})
	}
}

// Telegram still sends the pre-7.0 fields next to forward_origin, so a message
// usually carries both descriptions of the same forward. Taking the current one
// is a choice about which is authoritative rather than a merge of the two: the
// legacy fields are only consulted when forward_origin has no chat at all,
// which is how a forward from a hidden user arrives.
func TestForwardOriginWinsOverTheLegacyFields(t *testing.T) {
	got, ok := forwardedMessageURL(message{
		ForwardOrigin:        &forwardOrigin{Chat: &forwardOriginChat{ID: -1001934225758}, MessageID: 9522},
		ForwardFromChat:      &forwardOriginChat{ID: -1001234567890},
		ForwardFromMessageID: 7,
	})
	if !ok || got != "https://t.me/c/1934225758/9522" {
		t.Fatalf("forwardedMessageURL() = %q, %v; want the forward_origin message", got, ok)
	}
}

// /saved_task has to read like /tasks: every task is a button on the list, the
// button says which task it is, and the card behind it leads back to the list it
// came from. A column of identical "查看详情" buttons is not a list - it is a
// menu with no labels.
func TestSavedTaskCardIsAListButtonWithAWayBack(t *testing.T) {
	job := download.ChatJob{ID: "saved-1", DialogName: "收藏消息_42", Status: "listening", Discovered: 9, Completed: 4, ListenNew: true}

	// The list row: the task names itself, and its payload opens the card.
	label := fmt.Sprintf("%s %s %d/%d下载", statusIcon(job.Status), short(job.DialogName, 18), job.Completed, job.Discovered)
	if label == "查看详情" || !strings.Contains(label, "收藏消息_42") || !strings.Contains(label, "4/9下载") {
		t.Fatalf("the saved list button reads %q; it has to name the task it opens", label)
	}

	// The card: the same controls a session card carries, plus the way back.
	buttons := chatTaskKeyboard(job, "‹ 返回收藏夹列表")
	joined := ""
	for _, row := range buttons {
		for _, key := range row {
			joined += key.CallbackData + " "
		}
	}
	if !strings.Contains(joined, "c:l:back") {
		t.Fatalf("the saved card has no way back to its list: %s", joined)
	}
	if !strings.Contains(joined, "c:t:saved-1:listenoff") {
		t.Fatalf("the saved card cannot stop listening: %s", joined)
	}
	if got := buttons[len(buttons)-1][0].Text; got != "‹ 返回收藏夹列表" {
		t.Fatalf("the return button reads %q", got)
	}

	// A card that did not come from a list carries no return button at all.
	for _, row := range chatTaskKeyboard(job, "") {
		for _, key := range row {
			if key.CallbackData == "c:l:back" {
				t.Fatal("a card that was not opened from a list offers to return to one")
			}
		}
	}
}

// The return button follows the list the card was opened from, not the task:
// the same Saved Messages task is listed by /chats and by /saved_task, and each
// button has to go back where it came from.
func TestChatListBackFollowsTheListTheCardCameFrom(t *testing.T) {
	s := &Service{listPages: map[string]listPageState{}}
	if back, ok := s.chatListBack(1, 2); ok || back != "" {
		t.Fatalf("a card with no list behind it returned %q, %v", back, ok)
	}
	s.putListPage("chat", 1, 2, listPageState{kind: "chat", page: 1})
	if back, ok := s.chatListBack(1, 2); !ok || back != "‹ 返回会话列表" {
		t.Fatalf("a session list returned %q, %v", back, ok)
	}
	s.putListPage("chat", 3, 4, listPageState{kind: "chat", savedAccountID: "acc", page: 1})
	if back, ok := s.chatListBack(3, 4); !ok || back != "‹ 返回收藏夹列表" {
		t.Fatalf("a saved list returned %q, %v", back, ok)
	}
}
