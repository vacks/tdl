package bot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/monitor"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

func TestNormalizeCommand(t *testing.T) {
	tests := map[string]string{
		"/help":                "/help",
		"/help@TDLControlBot":  "/help",
		"/tasks@TDLControlBot": "/tasks",
		"https://t.me/a/1":     "https://t.me/a/1",
		"/help arguments":      "/help arguments",
		// The suffix belongs to the command token whatever follows it. Stripping
		// it only for a single-token message made every command that takes an
		// argument unrecognizable when Telegram appended the Bot name.
		"/tasks@TDLControlBot 下载中": "/tasks 下载中",
		"/saved@TDLControlBot all": "/saved all",
		"/help  two   spaces":      "/help two spaces",
	}
	for input, want := range tests {
		if got := normalizeCommand(input); got != want {
			t.Errorf("normalizeCommand(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestChatTaskPresentationMirrorsWebRangeRules(t *testing.T) {
	tests := []struct {
		name string
		job  download.ChatJob
		want string
	}{
		{name: "not discovered", job: download.ChatJob{UpperMessageID: 99}, want: "最早 — 99"},
		{name: "discovered while indexing", job: download.ChatJob{EarliestMediaID: 42, UpperMessageID: 99}, want: "42 — 99"},
		{name: "explicit start", job: download.ChatJob{StartMessageID: 50, EarliestMediaID: 42, UpperMessageID: 99}, want: "50 — 99"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := chatRangeLabel(test.job); got != test.want {
				t.Fatalf("chatRangeLabel() = %q, want %q", got, test.want)
			}
		})
	}
	job := download.ChatJob{DialogName: "对话", Status: "scanning", EarliestMediaID: 42, UpperMessageID: 99, Completed: 2, Discovered: 7}
	text := chatTaskText(job)
	for _, want := range []string{"42 — 99", "进度：</b>2/7"} {
		if !strings.Contains(text, want) {
			t.Fatalf("chatTaskText() missing %q: %s", want, text)
		}
	}
	if !strings.Contains(chatTaskText(download.ChatJob{}), "速率：</b>—（0 个文件）") {
		t.Fatal("chatTaskText() must show an idle speed row")
	}
}

func TestChatTaskKeyboardExposesPurgeForTerminalTask(t *testing.T) {
	keys := chatTaskKeyboard(download.ChatJob{ID: "chat-1", Status: "completed"}, true)
	joined := ""
	for _, row := range keys {
		for _, key := range row {
			joined += key.CallbackData + " "
		}
	}
	for _, want := range []string{"c:t:chat-1:delete", "c:t:chat-1:purge", "c:t:chat-1:listenon", "c:l:back"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("chatTaskKeyboard() missing %q: %s", want, joined)
		}
	}
}

func TestRetryableSubmitError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "temporary network", err: errors.New("temporary network failure"), want: true},
		{name: "flood wait", err: errors.New("FLOOD_WAIT_5"), want: true},
		{name: "invalid link", err: errors.New("无法解析 Telegram 链接"), want: false},
		{name: "permission denied", err: errors.New("message is not accessible"), want: false},
	}
	for _, test := range tests {
		if got := retryableSubmitError(test.err); got != test.want {
			t.Errorf("%s: retryableSubmitError(%v) = %v, want %v", test.name, test.err, got, test.want)
		}
	}
}

func TestPollRetryDelayIsBoundedAndDeterministic(t *testing.T) {
	if got := pollRetryDelay(1); got < 2*time.Second || got >= 3*time.Second {
		t.Fatalf("first retry delay = %s, want 2s to 3s", got)
	}
	if got, want := pollRetryDelay(99), time.Minute; got != want {
		t.Fatalf("capped retry delay = %s, want %s", got, want)
	}
	if got, want := pollRetryDelay(4), pollRetryDelay(4); got != want {
		t.Fatalf("retry jitter must be stable: %s != %s", got, want)
	}
	if !shouldLogPollFailure(1) || !shouldLogPollFailure(8) || shouldLogPollFailure(3) {
		t.Fatal("unexpected poll failure log cadence")
	}
}

func TestBotOutboundWaitHonorsServerCooldown(t *testing.T) {
	s := &Service{}
	s.noteOutboundWait(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := s.awaitOutbound(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("awaitOutbound during cooldown = %v, want deadline exceeded", err)
	}
}

func TestRedactBotError(t *testing.T) {
	token := "123456:secret-token"
	message := "Post \"https://api.telegram.org/bot123456:secret-token/getUpdates\": timeout"
	got := redactBotError(token, message)
	if got == message || !strings.Contains(got, "[REDACTED]") || strings.Contains(got, token) {
		t.Fatalf("redactBotError() = %q", got)
	}
}

func TestPrivateCallbackUsesClickingUser(t *testing.T) {
	query := callbackQuery{}
	query.From.ID = 42
	query.Message = &message{}
	query.Message.Chat.ID = 42
	// Telegram sets callback_query.message.from to the Bot that originally
	// sent the inline keyboard, not the user who clicked it.
	query.Message.From.ID = 999
	if !privateCallback(query) {
		t.Fatal("private callback was rejected because message sender is the Bot")
	}
	query.Message.Chat.ID = -100123
	if privateCallback(query) {
		t.Fatal("group callback was accepted")
	}
}

func TestBotAuthorizationAndTelegramLinks(t *testing.T) {
	cfg := settings.Bot{ControlUserIDs: []int64{7, 9}}
	if !allowed(cfg, 7) || !allowed(cfg, 9) || allowed(cfg, 8) {
		t.Fatal("control-user authorization did not match configured users")
	}
	for raw, want := range map[string]bool{
		"https://t.me/example/1":        true,
		"https://subdomain.t.me/a":      true,
		"https://telegram.me/example/1": false,
		"https://example.com/t.me/test": false,
		"not a URL":                     false,
	} {
		if got := isTelegramLink(raw); got != want {
			t.Errorf("isTelegramLink(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestTaskPresentationContainsEscapedProgressAndActions(t *testing.T) {
	job := download.Job{ID: "job-1", DialogName: "<unsafe>", Status: "running", CompletedItems: 1, TotalItems: 2, Items: []download.Item{
		{DialogKey: "channel:1", MessageID: 1, OriginalName: "<one>.mp4", Status: "completed"},
		{DialogKey: "channel:1", MessageID: 2, OriginalName: "two.mp4", Status: "running"},
	}}
	text := taskText(job, []download.FileProgress{{DialogKey: "channel:1", MessageID: 2, Downloaded: 50, Total: 100, SpeedBPS: 1024}})
	for _, want := range []string{"下载中", "&lt;unsafe&gt;", "1/2 文件", "100%", "50%", "1.00 KB/s", "&lt;one&gt;.mp4"} {
		if !strings.Contains(text, want) {
			t.Errorf("taskText missing %q: %s", want, text)
		}
	}
	keys := taskKeyboard(job, true)
	joined := ""
	for _, row := range keys {
		for _, button := range row {
			joined += button.CallbackData + " "
		}
	}
	for _, want := range []string{"t:job-1:pause", "t:job-1:cancel", "t:job-1:view", "l:back"} {
		if !strings.Contains(joined, want) {
			t.Errorf("taskKeyboard missing %q: %s", want, joined)
		}
	}
}

func TestLifecycleAndDeletedMessagesKeepExpectedControls(t *testing.T) {
	job := download.Job{ID: "job-1", DialogName: "频道", Status: "completed", CompletedItems: 2, TotalItems: 2}
	if text := lifecycleText(job); !strings.Contains(text, "下载完成") || !strings.Contains(text, "2/2 文件") {
		t.Fatalf("lifecycleText() = %q", text)
	}
	deleted := deletedTaskText("✅ <b>下载完成</b>\n进度：2/2 文件")
	if !strings.Contains(deleted, "任务已删除") || !strings.Contains(deleted, "2/2 文件") {
		t.Fatalf("deletedTaskText() = %q", deleted)
	}
	keyboard := deletedTaskKeyboard(true)
	if len(keyboard) != 1 || keyboard[0][0].CallbackData != "l:back" {
		t.Fatalf("deletedTaskKeyboard() = %#v", keyboard)
	}
}

func TestListPageStateMovesByCursorWithoutOffsets(t *testing.T) {
	state := listPageState{kind: "task", page: 1, next: "cursor-2"}
	next, ok := moveListPage(state, "next")
	if !ok || next.page != 2 || next.cursor != "cursor-2" || len(next.previous) != 1 || next.previous[0] != "" {
		t.Fatalf("next state = %#v", next)
	}
	next.next = "cursor-3"
	previous, ok := moveListPage(next, "prev")
	if !ok || previous.page != 1 || previous.cursor != "" || len(previous.previous) != 0 {
		t.Fatalf("previous state = %#v", previous)
	}
}

func TestListPageStateExpiresAndIsBounded(t *testing.T) {
	s := &Service{listPages: map[string]listPageState{}}
	now := time.Now()
	s.listPages["expired"] = listPageState{updated: now.Add(-listPageTTL)}
	s.listPages["fresh"] = listPageState{updated: now.Add(-listPageTTL + time.Second)}
	s.pruneListPages(now)
	if _, ok := s.listPages["expired"]; ok {
		t.Fatal("expired list page was retained")
	}
	if _, ok := s.listPages["fresh"]; !ok {
		t.Fatal("fresh list page was removed")
	}

	for index := 0; index < maxListPageEntries+1; index++ {
		s.putListPage("task", int64(index), int64(index), listPageState{kind: "task", page: 1})
	}
	if len(s.listPages) != maxListPageEntries {
		t.Fatalf("list page entries=%d, want %d", len(s.listPages), maxListPageEntries)
	}
	history := make([]string, maxListPageHistory+5)
	s.putListPage("task", 999, 999, listPageState{kind: "task", page: 1, previous: history})
	state, ok := s.getListPage("task", 999, 999)
	if !ok || len(state.previous) != maxListPageHistory {
		t.Fatalf("history length=%d exists=%v, want %d/true", len(state.previous), ok, maxListPageHistory)
	}
}

func TestTaskSourcePresentation(t *testing.T) {
	tests := []struct {
		name string
		job  download.Job
		want string
	}{
		{
			name: "keeps canonical source link",
			job:  download.Job{SourceURL: "https://t.me/example/42", DialogType: "channel"},
			want: `<b>来源：</b><a href="https://t.me/example/42">点击查看</a>`,
		},
		{
			name: "creates private channel link",
			job:  download.Job{SourceURL: "tg://reaction/channel/100/42", DialogType: "channel", Items: []download.Item{{DialogID: 100, MessageID: 42}}},
			want: `<b>来源：</b><a href="https://t.me/c/100/42">点击查看</a>`,
		},
		{
			name: "basic group states limitation",
			job:  download.Job{SourceURL: "tg://reaction/chat/100/42", DialogType: "chat", Items: []download.Item{{DialogID: 100, MessageID: 42}}},
			want: "<b>来源：</b>普通群组不支持跳转",
		},
		{
			name: "does not expose internal private chat identifier",
			job:  download.Job{SourceURL: "tg://reaction/user/100/42", DialogType: "user"},
			want: "<b>来源：</b>私聊不支持跳转",
		},
		{
			name: "bot chat states limitation",
			job:  download.Job{SourceURL: "tg://reaction/bot/100/42", DialogType: "bot"},
			want: "<b>来源：</b>Bot不支持跳转",
		},
		{
			name: "saved messages states limitation",
			job:  download.Job{DialogType: "self"},
			want: "<b>来源：</b>收藏消息不支持跳转",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sourceLine(test.job); got != test.want {
				t.Fatalf("sourceLine() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestEventListShowsTheOpenQueueInTheStatusCardVocabulary checks that the list
// reads as the drill-down behind /status: the same three labels, the attempt
// count that says how close an event is to being left failed for good, and the
// next retry time that distinguishes waiting work from work in flight.
func TestEventListShowsTheOpenQueueInTheStatusCardVocabulary(t *testing.T) {
	events := []download.ListenerEvent{
		{Source: "message", ID: 8412, DialogName: "某频道", MessageID: 1001, Status: "pending", Attempts: 3, NextAttemptAt: "2026-09-30T05:04:00Z"},
		{Source: "reaction", ID: 8390, DialogName: "某群组", MessageID: 2044, Emoji: "👍", Status: "failed", Attempts: download.InboxAttemptLimit, Error: "CHAT_WRITE_FORBIDDEN"},
		{Source: "message", ID: 8377, DialogName: "某群组", MessageID: 88, Status: "processing", Attempts: 1},
	}
	text := eventListText(events, 12, 1, 2)
	for _, want := range []string{
		"<b>监听事件</b>", "共 12 个", "第 1/2 页",
		"#8412", "等待重试 3/20", "下次 ",
		"#8390", "反应 👍", "已停止重试 20/20", "CHAT_WRITE_FORBIDDEN",
		"#8377", "处理中 1/20",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("eventListText() missing %q in:\n%s", want, text)
		}
	}
	// Processing work has no retry time to show, and a card that printed one
	// would describe an in-flight event as if it were backing off.
	if strings.Count(text, "下次 ") != 1 {
		t.Fatalf("eventListText() must show a next attempt only for waiting events:\n%s", text)
	}
}

// TestEventLineEscapesTelegramSuppliedText covers the fields a person does not
// control. The dialog name and emoji come from Telegram and the error text is
// quoted from a server response, so any of them can contain the angle brackets
// that would otherwise become markup in an HTML message.
func TestEventLineEscapesTelegramSuppliedText(t *testing.T) {
	line := eventLine(download.ListenerEvent{
		Source: "reaction", ID: 7, DialogName: "A&B <频道>", MessageID: 9, Emoji: "<b>",
		Status: "failed", Attempts: download.InboxAttemptLimit, Error: "FLOOD_WAIT <500>",
	})
	for _, raw := range []string{"<频道>", "A&B", "<b>", "<500>"} {
		if strings.Contains(line, raw) {
			t.Fatalf("eventLine() left %q unescaped: %s", raw, line)
		}
	}
	for _, want := range []string{"A&amp;B", "&lt;频道&gt;", "FLOOD_WAIT &lt;500&gt;"} {
		if !strings.Contains(line, want) {
			t.Fatalf("eventLine() missing %q: %s", want, line)
		}
	}
	// A recorded error is a chain, and the token naming the cause is its last
	// one - on real rows it is "CHANNEL_INVALID" after four layers of wrapper.
	// Truncating the tail would remove exactly the part worth reading.
	real := eventLine(download.ListenerEvent{
		Source: "message", ID: 33, Status: "failed", Attempts: download.InboxAttemptLimit,
		Error: "callback: 读取讨论根消息: retry middleware skip: rpcDoRequest: rpc error code 400: CHANNEL_INVALID",
	})
	if !strings.Contains(real, "CHANNEL_INVALID") {
		t.Fatalf("eventLine() truncated away the cause: %s", real)
	}
	if !strings.Contains(real, "\n  └ ") {
		t.Fatalf("eventLine() must put the whole error on its own line: %s", real)
	}
}

// TestEventClearConfirmTextNamesRevivableEvents guards the number that decides
// whether a clear destroys work that was still going to be attempted. An event
// below the attempt limit is only resting between revives, so a card that
// reported the total alone would describe it as a dead end.
func TestEventClearConfirmTextNamesRevivableEvents(t *testing.T) {
	text := eventClearConfirmText(12, 4)
	for _, want := range []string{"<b>12</b>", "4 个尚未用尽尝试次数"} {
		if !strings.Contains(text, want) {
			t.Fatalf("eventClearConfirmText(12, 4) missing %q: %s", want, text)
		}
	}
	if quiet := eventClearConfirmText(3, 0); strings.Contains(quiet, "自动重试") {
		t.Fatalf("eventClearConfirmText(3, 0) must not mention a revive that cannot happen: %s", quiet)
	}
}

// dispatchableJob builds the longest card the field bounds allow: a caption at
// the full message limit, a dialog name at its bound, and more files than the
// card lists.
func dispatchableJob() download.Job {
	items := make([]download.Item, 0, botCardItemLines+5)
	for index := 0; index < botCardItemLines+5; index++ {
		items = append(items, download.Item{
			DialogKey:    "channel:1",
			MessageID:    index + 1,
			OriginalName: strings.Repeat("很长的文件名", 4),
			Status:       "queued",
		})
	}
	return download.Job{
		ID:          "job",
		Status:      "running",
		DialogName:  strings.Repeat("频道名称", 20),
		MessageText: strings.Repeat("正", botCardLimit),
		TotalItems:  len(items),
		Items:       items,
	}
}

// Telegram rejects a message longer than its limit outright, so a card that
// exceeds it is not a shortened card - it is no card, and the task's only
// progress surface stops updating for as long as the task runs. The file list
// was bounded and the caption was not, which bounded the short half: a caption
// may itself be as long as a whole message.
func TestTaskCardStaysWithinTelegramsLimit(t *testing.T) {
	text := taskText(dispatchableJob(), nil)
	if got := len([]rune(text)); got > botCardLimit {
		t.Fatalf("card is %d characters, Telegram rejects anything over %d, so this card can never be sent", got, botCardLimit)
	}
	if !strings.Contains(text, "进度") {
		t.Fatal("the card lost its header, so it no longer says which task it is")
	}
	if strings.Contains(text, strings.Repeat("正", botCardMessageText+1)) {
		t.Fatal("the whole caption was rendered; a caption can be as long as a message, so the card cannot stay inside the limit")
	}
	// The caption is what gets shortened, not the rest of the card. Relying on
	// the backstop alone would satisfy the check above - it drops trailing
	// lines until the text fits - while quietly costing every task with a long
	// caption its caption line and its source line, which is most of what the
	// card is for.
	if !strings.Contains(text, "消息：") {
		t.Fatal("a long caption removed the caption line instead of being shortened on it")
	}
	if !strings.Contains(text, "来源：") {
		t.Fatal("a long caption pushed the source line off the card")
	}
	if !strings.Contains(text, strings.Repeat("正", 50)) {
		t.Fatal("the caption was dropped whole rather than shortened to its beginning")
	}
}

// The backstop, reached through the one field on this card that nothing bounds.
// The source URL is stored exactly as it was submitted, and the endpoint that
// accepts one reads a body of up to 8 KiB - so a single link can be longer than
// a whole Telegram message. A shortened URL would be a broken link, so the line
// goes rather than the card.
func TestTaskCardDropsTheSourceLineBeforeItExceedsTheLimit(t *testing.T) {
	job := dispatchableJob()
	job.MessageText = "短说明"
	job.SourceURL = "https://t.me/example/1?x=" + strings.Repeat("a", 6000)
	text := taskText(job, nil)
	if got := len([]rune(text)); got > botCardLimit {
		t.Fatalf("card is %d characters, over the %d Telegram accepts", got, botCardLimit)
	}
	if strings.Contains(text, "来源") {
		t.Fatal("the oversized source line was kept, so the card is still over the limit")
	}
	if !strings.Contains(text, "短说明") {
		t.Fatal("dropping the source line cost the card its caption as well")
	}
}

// The bound must not cost the card its content in the ordinary case: a caption
// short enough to fit is shown whole, and the links and counts stay.
func TestTaskCardKeepsShortCaptionsAndFooter(t *testing.T) {
	job := dispatchableJob()
	job.MessageText = "一条普通说明"
	job.SourceURL = "https://t.me/example/12"
	text := taskText(job, nil)
	if !strings.Contains(text, "一条普通说明") {
		t.Fatal("a caption that fits was dropped from the card")
	}
	if !strings.Contains(text, "对话") || !strings.Contains(text, "进度") {
		t.Fatal("the card lost a field it always showed")
	}
}

// The three-second interval used to be enforced only inside editLive, which is
// handed a task that has already been read - so it suppressed the Telegram
// request and nothing else. Every file transition of a download wakes the
// refresh loop, so the row was read at the event rate to produce an edit the
// same interval then declined to send.
//
// The card below is tracked against a task that does not exist, which is what
// makes the read observable: reading it returns "not found" and the card is
// retired. So a card that survives the pass was not read, and the second half
// of the test - where the interval has passed and the card is retired - is what
// keeps that from also being true of a loop that never reads anything.
func TestRefreshSkipsTheReadForACardThatIsNotDue(t *testing.T) {
	databaseURL := os.Getenv("TDL_TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	root := t.TempDir()
	store, err := settings.Open(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := telegram.Open(filepath.Join(root, "telegram"), func() string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	downloads, err := download.Open(filepath.Join(root, "data"), filepath.Join(root, "downloads"), databaseURL, store, accounts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(downloads.Stop)
	s := &Service{
		downloads:        downloads,
		ctx:              context.Background(),
		tracked:          map[string]trackedRef{},
		chatTracked:      map[string]chatTrackedRef{},
		known:            map[string]string{},
		lifecycle:        map[string]map[int64]messageRef{},
		dirty:            map[string]struct{}{},
		deleted:          map[string]time.Time{},
		suppressedStatus: map[string]string{},
		lastLiveEdit:     map[string]time.Time{},
		liveRendered:     map[string]string{},
	}
	ref := messageRef{ChatID: 1, MessageID: 2}
	s.track("task-that-does-not-exist", ref)
	cfg := settings.Bot{Token: "token", ControlUserIDs: []int64{1}}

	s.lastLiveEdit[ref.key()] = time.Now()
	s.refresh(cfg)
	if len(s.tracked) != 1 {
		t.Fatal("a card that was just edited was read again; the interval is being applied after the read instead of before it")
	}

	s.lastLiveEdit[ref.key()] = time.Now().Add(-2 * liveCardInterval)
	s.refresh(cfg)
	if len(s.tracked) != 0 {
		t.Fatal("a card past its interval was not read, so the check above proves nothing")
	}
}

// Stop has to end the service's goroutines, not just ask them to. The server
// stops its services in order and the download manager closes the database
// immediately after, so anything still running here goes on to query a closed
// database - and the event loop in particular only ends when its subscription
// is released, which is why the unsubscribe function is kept rather than
// dropped.
//
// The assertion is on the time, because that is what distinguishes "every
// goroutine exited" from "the wait gave up": a service that fails to end one of
// them takes the whole bounded timeout.
func TestStopEndsTheServicesGoroutines(t *testing.T) {
	databaseURL := os.Getenv("TDL_TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	root := t.TempDir()
	store, err := settings.Open(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := telegram.Open(filepath.Join(root, "telegram"), func() string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	downloads, err := download.Open(filepath.Join(root, "data"), filepath.Join(root, "downloads"), databaseURL, store, accounts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(downloads.Stop)
	service := New(store, downloads, accounts, monitor.New(filepath.Join(root, "downloads")), filepath.Join(root, "data"))
	started := time.Now()
	service.Stop()
	if elapsed := time.Since(started); elapsed >= botStopTimeout {
		t.Fatalf("Stop took %s, which is the timeout: at least one goroutine did not end", elapsed)
	}
	// Shutdown paths overlap - a signal, a deferred call, a test cleanup.
	service.Stop()
}
