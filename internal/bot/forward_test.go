package bot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/settings"
)

// stubTransport answers the Bot API without a network and records what was
// asked. The Bot's own chat is the fallback target, and reaching it is the
// behaviour under test, so a request that is not the getMe the fallback needs is
// recorded rather than answered.
type stubTransport struct {
	requests []string
	bodies   []string
	// getMe is the username the stub reports. An empty one is a Bot that has
	// none, which is a permanent answer.
	getMe     string
	getMeFail bool
}

func (t *stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	t.requests = append(t.requests, req.URL.Path)
	t.bodies = append(t.bodies, string(body))
	if !strings.HasSuffix(req.URL.Path, "/getMe") {
		return jsonResponse(req, `{"ok":true,"result":{"message_id":7}}`), nil
	}
	if t.getMeFail {
		return nil, errors.New("connection reset by peer")
	}
	return jsonResponse(req, fmt.Sprintf(`{"ok":true,"result":{"username":%q}}`, t.getMe)), nil
}

func (t *stubTransport) getMeCalls() int {
	count := 0
	for _, path := range t.requests {
		if strings.HasSuffix(path, "/getMe") {
			count++
		}
	}
	return count
}

func (t *stubTransport) sends() []string {
	sent := make([]string, 0, len(t.requests))
	for index, path := range t.requests {
		if strings.HasSuffix(path, "/sendMessage") {
			sent = append(sent, t.bodies[index])
		}
	}
	return sent
}

func jsonResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    req,
	}
}

// fakeSubmitter records what the Bot submits and answers each attempt with a
// scripted error, so the two ways of reaching one forwarded post are
// distinguishable.
type fakeSubmitter struct {
	intents    []download.DownloadIntent
	submitErrs []error
	copies     []download.BotChatIntent
	copyErrs   []error
}

func (f *fakeSubmitter) Submit(_ context.Context, intent download.DownloadIntent) (download.Submission, error) {
	f.intents = append(f.intents, intent)
	if len(f.intents) <= len(f.submitErrs) {
		return download.Submission{}, f.submitErrs[len(f.intents)-1]
	}
	return download.Submission{Job: download.Job{ID: "job"}}, nil
}

func (f *fakeSubmitter) SubmitBotChatMessage(_ context.Context, intent download.BotChatIntent) (download.Submission, error) {
	f.copies = append(f.copies, intent)
	if len(f.copies) <= len(f.copyErrs) {
		return download.Submission{}, f.copyErrs[len(f.copies)-1]
	}
	return download.Submission{Job: download.Job{ID: "job"}}, nil
}

func (f *fakeSubmitter) urls() []string {
	urls := make([]string, 0, len(f.intents))
	for _, intent := range f.intents {
		urls = append(urls, intent.URL)
	}
	return urls
}

// newForwardTestService builds a Service wired for the message path only: a
// stub Bot API, a fake download manager, and no goroutines. The cached identity
// is seeded so the getMe path is exercised only by the test that is about it.
func newForwardTestService(t *testing.T, transport *stubTransport, submit *fakeSubmitter) *Service {
	t.Helper()
	store, err := settings.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Service{
		settings:    store,
		submit:      submit,
		ctx:         context.Background(),
		client:      &http.Client{Transport: transport},
		clientProxy: "", // matches settings.ProxyURL(), so httpClient reuses this client
		botIdentity: botIdentity{token: "token", username: "tdlbot"},
	}
}

// forwarded builds the message the Bot API sends for a post forwarded from an
// unjoined channel: an origin that names a channel, and the send time that is
// what actually locates the copy in the account's own chat.
func forwarded() message {
	return message{
		MessageID:     1203,
		Date:          1759406176,
		ForwardOrigin: &forwardOrigin{Chat: &forwardOriginChat{ID: -1001890432834}, MessageID: 77},
	}
}

const unreadableOrigin = "input peer: failed to get result from 1890432834：get chats: retry middleware skip: rpcDoRequest: rpc error code 400: CHAT_ID_INVALID"

// A forward from a channel this account cannot read has no origin to download
// from, but the message the user forwarded is sitting in this chat, readable -
// which is exactly what reacting to it already downloads. The forward must
// therefore reach the same place, and must not tell the user about a failure it
// went on to recover from: one task, one card, and no error message.
//
// The assertion is on there being a second, differently addressed attempt,
// because that is what distinguishes a fallback from a retry. It is addressed
// by the Bot's username and the message's send time rather than by the id the
// update carried: a bot chat is numbered from the Bot's side, so the update's id
// addresses nothing in the account's own history. Deleting that second call
// leaves this test red with no copy attempt at all.
func TestForwardedMessageFallsBackToTheBotChatCopy(t *testing.T) {
	transport := &stubTransport{getMe: "tdlbot"}
	submit := &fakeSubmitter{submitErrs: []error{errors.New(unreadableOrigin)}}
	s := newForwardTestService(t, transport, submit)

	handled := s.handleMessage(settings.Bot{Token: "token"}, forwarded())

	if !handled {
		t.Fatal("a forward whose copy was downloaded is handled; retrying it would download a second time")
	}
	if got := submit.urls(); fmt.Sprint(got) != fmt.Sprint([]string{"https://t.me/c/1890432834/77"}) {
		t.Fatalf("submitted %v; want the origin tried first", got)
	}
	if len(submit.copies) != 1 {
		t.Fatalf("the Bot chat copy was submitted %d times, want once", len(submit.copies))
	}
	copyIntent := submit.copies[0]
	if copyIntent.BotUsername != "tdlbot" {
		t.Fatalf("the copy was addressed to %q, want the Bot's own username", copyIntent.BotUsername)
	}
	if copyIntent.SentAt != 1759406176 {
		t.Fatalf("the copy was located by %d, want the update's send time", copyIntent.SentAt)
	}
	if sent := transport.sends(); len(sent) != 0 {
		t.Fatalf("the recovered failure was reported to the user: %v", sent)
	}
}

// The two ways an origin submission can fail without being a reason to try the
// copy, and they must be kept apart from the one above: an answer about the
// message's contents is final, and a transport fault is retried rather than
// answered by downloading something else.
func TestForwardedMessageKeepsTheAnswerAndTheRetry(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantHandled bool
	}{
		// Both sentinels are what Submit returns for an answered submission, and
		// they arrive wrapped: the wrapper restates the cause's text, so a table
		// that passes the bare sentinel pins the same observable behaviour.
		{"nothing in the post matches the filter", download.ErrNoEligibleMedia, true},
		{"nothing in the post to download", download.ErrNoMedia, true},
		{"a transport fault is retried", errors.New("input peer: connection timeout"), false},
		{"a rate limit is retried", errors.New("input peer: failed to get result from 1：FLOOD_WAIT_5"), false},
		{"a claim conflict is not answered by a second task", errors.New("任务创建竞争过于频繁，请稍后重试"), true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			transport := &stubTransport{getMe: "tdlbot"}
			submit := &fakeSubmitter{submitErrs: []error{test.err}}
			s := newForwardTestService(t, transport, submit)

			handled := s.handleMessage(settings.Bot{Token: "token"}, forwarded())

			if handled != test.wantHandled {
				t.Fatalf("handled = %v, want %v", handled, test.wantHandled)
			}
			if len(submit.copies) != 0 {
				t.Fatalf("this failure must not reach the Bot chat copy: %+v", submit.copies)
			}
			if sent := transport.sends(); len(sent) != 1 {
				t.Fatalf("the failure was reported %d times; want exactly one", len(sent))
			}
		})
	}
}

// The Bot never fetched its own username, so it cannot say which chat to read.
// The origin's failure is then reported as it always was, rather than swallowed.
func TestForwardedMessageWithoutABotUsernameReportsTheOriginError(t *testing.T) {
	transport := &stubTransport{getMe: ""}
	submit := &fakeSubmitter{submitErrs: []error{errors.New(unreadableOrigin)}}
	s := newForwardTestService(t, transport, submit)
	s.botIdentity = botIdentity{}

	handled := s.handleMessage(settings.Bot{Token: "token"}, forwarded())

	if !handled {
		t.Fatal("a permanent failure is reported as handled; the command is not retried forever")
	}
	if len(submit.copies) != 0 {
		t.Fatalf("there is no chat to read without a username: %+v", submit.copies)
	}
	if sent := transport.sends(); len(sent) != 1 {
		t.Fatalf("the origin error was not reported: %v", sent)
	}
}

// A forward from a private chat or a hidden sender names no chat at all, so
// there is no origin to try - but the copy in this chat is still the message the
// user meant. It is taken straight to the Bot chat, with no doomed first
// attempt.
func TestForwardWithoutAnAddressableOriginDownloadsTheBotChatCopy(t *testing.T) {
	cases := []struct {
		name string
		msg  message
	}{
		{"a forward from a hidden sender", message{MessageID: 12, Date: 1759406176, ForwardOrigin: &forwardOrigin{MessageID: 12}, ForwardSenderName: "某人"}},
		{"a pre-7.0 forward from a user", message{MessageID: 12, Date: 1759406176, ForwardFrom: json.RawMessage(`{"id":5}`)}},
		{"a forward from a basic group", message{MessageID: 12, Date: 1759406176, ForwardOrigin: &forwardOrigin{Chat: &forwardOriginChat{ID: -123456}, MessageID: 12}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			transport := &stubTransport{getMe: "tdlbot"}
			submit := &fakeSubmitter{}
			s := newForwardTestService(t, transport, submit)

			msg := test.msg
			msg.Document = json.RawMessage(`{"file_id":"x"}`)
			if !s.handleMessage(settings.Bot{Token: "token"}, msg) {
				t.Fatal("the copy was submitted, so the update is handled")
			}
			if len(submit.intents) != 0 {
				t.Fatalf("there is no addressable origin to try: %v", submit.urls())
			}
			if len(submit.copies) != 1 || submit.copies[0].SentAt != msg.Date {
				t.Fatalf("the copy was not submitted once with the update's send time: %+v", submit.copies)
			}
		})
	}
}

// The new path must not swallow ordinary messages. A forwarded remark has no
// file, so it is still a caption that may be a command, and a command that
// happens to carry a file must still not be turned into a download when it was
// not forwarded.
func TestOnlyAForwardThatCarriesAFileReachesTheBotChatCopy(t *testing.T) {
	cases := []struct {
		name       string
		msg        message
		wantCopies int
	}{
		{"a forwarded remark is still read as a command", message{MessageID: 12, Date: 1759406176, Text: "/help", ForwardSenderName: "某人"}, 0},
		{"a file that was not forwarded is not a download", message{MessageID: 12, Date: 1759406176, Document: json.RawMessage(`{"file_id":"x"}`)}, 0},
		{"a forwarded file is a download", message{MessageID: 12, Date: 1759406176, ForwardSenderName: "某人", Document: json.RawMessage(`{"file_id":"x"}`)}, 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			transport := &stubTransport{getMe: "tdlbot"}
			submit := &fakeSubmitter{}
			s := newForwardTestService(t, transport, submit)

			s.handleMessage(settings.Bot{Token: "token"}, test.msg)
			if got := len(submit.copies); got != test.wantCopies {
				t.Fatalf("the copy was submitted %d times, want %d", got, test.wantCopies)
			}
		})
	}
}

// Telegram sends one update per file of a forwarded album. Each one used to be
// resolved, claimed and submitted on its own, so a ten-file album paid ten times
// over on the account's Telegram request budget and in the database for one
// download. One submission already queues the whole album - the downloader
// expands a message by its grouped id - so the members after the first are
// answered without being looked at.
func TestAlbumMembersAfterTheFirstAreNotSubmittedAgain(t *testing.T) {
	transport := &stubTransport{getMe: "tdlbot"}
	submit := &fakeSubmitter{}
	s := newForwardTestService(t, transport, submit)

	// attempts counts everything one member costs: the origin submission, and
	// the copy when the origin is unreadable. A collapsed member costs neither.
	attempts := func() int { return len(submit.intents) + len(submit.copies) }

	for member := 0; member < 5; member++ {
		msg := forwarded()
		msg.MessageID = 1203 + int64(member)
		msg.Date = 1759406176 + int64(member)
		msg.MediaGroupID = "album-1"
		msg.Document = json.RawMessage(`{"file_id":"x"}`)
		if !s.handleMessage(settings.Bot{Token: "token"}, msg) {
			t.Fatalf("album member %d was left to be retried", member)
		}
	}
	if got := attempts(); got != 1 {
		t.Fatalf("five members of one album cost %d submissions; the first queues all of it", got)
	}

	// A different album is a different download, and must not be mistaken for a
	// member of the one already answered.
	other := forwarded()
	other.MediaGroupID = "album-2"
	other.Document = json.RawMessage(`{"file_id":"x"}`)
	if !s.handleMessage(settings.Bot{Token: "token"}, other) {
		t.Fatal("a second album was not handled")
	}
	if got := attempts(); got != 2 {
		t.Fatalf("a distinct album was collapsed onto the first: %d submissions", got)
	}

	// A message that is not part of any album is unaffected.
	plain := forwarded()
	plain.Date = 1759406300
	if !s.handleMessage(settings.Bot{Token: "token"}, plain) {
		t.Fatal("an ordinary forward was not handled")
	}
	if got := attempts(); got != 3 {
		t.Fatalf("an ungrouped forward was collapsed onto another album: %d submissions", got)
	}
}

// The memory of an album must not outlive the failure it would swallow. A
// retryable failure leaves the update to run again, so the album it belongs to
// has to stay unanswered - otherwise the retry is skipped as a duplicate and the
// download is lost in exactly the case the retry exists for.
func TestAFailedAlbumIsLeftToBeRetried(t *testing.T) {
	transport := &stubTransport{getMe: "tdlbot"}
	submit := &fakeSubmitter{submitErrs: []error{errors.New("input peer: connection timeout")}}
	s := newForwardTestService(t, transport, submit)

	first := forwarded()
	first.MediaGroupID = "album-1"
	first.Document = json.RawMessage(`{"file_id":"x"}`)
	if s.handleMessage(settings.Bot{Token: "token"}, first) {
		t.Fatal("a transport fault has to leave the update to be retried")
	}

	// The retry is this same update, and the next attempt succeeds. It has to
	// reach the downloader again: the album memory must not have claimed it.
	submit.submitErrs = nil
	if !s.handleMessage(settings.Bot{Token: "token"}, first) {
		t.Fatal("the retried album was not handled")
	}
	if got := len(submit.intents); got != 2 {
		t.Fatalf("the retry was swallowed as a duplicate album member: %d attempts", got)
	}
	// And the member that was skipped while the first was failing is still
	// skipped now that the album has been answered.
	second := forwarded()
	second.MessageID = 1204
	second.MediaGroupID = "album-1"
	second.Document = json.RawMessage(`{"file_id":"x"}`)
	if !s.handleMessage(settings.Bot{Token: "token"}, second) {
		t.Fatal("the second member was not handled")
	}
	if got := len(submit.intents); got != 2 {
		t.Fatalf("the album was submitted again after it succeeded: %d attempts", got)
	}
}

func TestCarriesFileReadsEveryFieldAndTheNullTelegramSendsForAbsentOnes(t *testing.T) {
	if carriesFile(message{}) {
		t.Fatal("a message with no file fields carries no file")
	}
	if carriesFile(message{Document: json.RawMessage(`null`)}) {
		t.Fatal("an explicit null is how Telegram says a field is absent")
	}
	fields := map[string]func() message{
		"photo":      func() message { return message{Photo: json.RawMessage(`[{"file_id":"x"}]`)} },
		"video":      func() message { return message{Video: json.RawMessage(`{"file_id":"x"}`)} },
		"animation":  func() message { return message{Animation: json.RawMessage(`{"file_id":"x"}`)} },
		"audio":      func() message { return message{Audio: json.RawMessage(`{"file_id":"x"}`)} },
		"document":   func() message { return message{Document: json.RawMessage(`{"file_id":"x"}`)} },
		"voice":      func() message { return message{Voice: json.RawMessage(`{"file_id":"x"}`)} },
		"video_note": func() message { return message{VideoNote: json.RawMessage(`{"file_id":"x"}`)} },
		"sticker":    func() message { return message{Sticker: json.RawMessage(`{"file_id":"x"}`)} },
	}
	for name, build := range fields {
		if !carriesFile(build()) {
			t.Fatalf("%s holds a file but was read as text", name)
		}
	}
	// The guard is what keeps a forwarded remark out of the download path, so
	// the shapes that carry no file must not be read as one.
	for name, msg := range map[string]message{
		"text with a link preview":  {Text: "https://example.invalid"},
		"an explicit null document": {Document: json.RawMessage(`null`)},
	} {
		if carriesFile(msg) {
			t.Fatalf("%s holds no file but was read as one", name)
		}
	}
}

func TestIsForwardReadsEveryShapeTelegramSends(t *testing.T) {
	forwards := map[string]message{
		"forward_origin with a chat": {ForwardOrigin: &forwardOrigin{Chat: &forwardOriginChat{ID: -1001}}},
		"forward_origin without one": {ForwardOrigin: &forwardOrigin{MessageID: 3}},
		"the legacy chat fields":     {ForwardFromChat: &forwardOriginChat{ID: -1001}},
		"the legacy user field":      {ForwardFrom: json.RawMessage(`{"id":5}`)},
		"a hidden sender's name":     {ForwardSenderName: "某人"},
	}
	for name, msg := range forwards {
		if !isForward(msg) {
			t.Fatalf("%s describes a forward but was read as an ordinary message", name)
		}
	}
	// Telegram sends explicit nulls for the fields it means as absent, and a
	// forwarder it is not naming must not make an ordinary message one.
	for name, msg := range map[string]message{
		"an ordinary message":          {Text: "hello"},
		"a message with a file":        {Document: json.RawMessage(`{"file_id":"x"}`)},
		"an empty sender name":         {ForwardSenderName: "  "},
		"forward_from explicitly null": {ForwardFrom: json.RawMessage(`null`)},
	} {
		if isForward(msg) {
			t.Fatalf("%s is not a forward but was read as one", name)
		}
	}
}

// shouldRetryFromBotChat is the whole safety of the fallback: the failures it
// accepts are the ones a readable copy can answer, and everything else must be
// left exactly as it was. Getting this wrong in either direction costs either a
// silent regression or a second task for one file.
func TestShouldRetryFromBotChatOnlyAcceptsAnUnreadableOrigin(t *testing.T) {
	accepted := []string{
		"input peer: failed to get result from 1890432834：get chats: retry middleware skip: rpcDoRequest: rpc error code 400: CHAT_ID_INVALID",
		"get single message: retry middleware skip: rpcDoRequest: rpc error code 400: CHANNEL_INVALID",
		"get single message",
		"the message 1471535960/195686: message may be deleted",
		"retry middleware skip: rpcDoRequest: rpc error code 400: PEER_ID_INVALID",
		"retry middleware skip: rpcDoRequest: rpc error code 400: USER_ID_INVALID",
		"retry middleware skip: rpcDoRequest: rpc error code 400: CHANNEL_PRIVATE",
	}
	for _, text := range accepted {
		if !shouldRetryFromBotChat(errors.New(text)) {
			t.Fatalf("%q means this account cannot read the post; the copy should be tried", text)
		}
	}
	rejected := []string{
		"任务创建竞争过于频繁，请稍后重试",
		"消息组中的文件已关联到不同下载任务，无法安全合并",
		"连接数据库失败: connection refused",
		"input peer: failed to get result from 1：FLOOD_WAIT_5",
		"保存下载状态失败: 磁盘已满",
	}
	for _, text := range rejected {
		if shouldRetryFromBotChat(errors.New(text)) {
			t.Fatalf("%q is not the origin being unreadable; a second task must not be created for it", text)
		}
	}
	if shouldRetryFromBotChat(nil) {
		t.Fatal("a successful submission is not a reason to submit again")
	}
}

// The Bot's username is a fact about the token, and a changed token is a
// different Bot whose name addresses a different chat. Asking on every forward
// would spend a request that cannot change the answer, and keeping the old one
// would send a forwarded post's copy to whoever now owns that name.
func TestBotUsernameIsFetchedOncePerToken(t *testing.T) {
	transport := &stubTransport{getMe: "tdlbot"}
	s := newForwardTestService(t, transport, &fakeSubmitter{})
	s.botIdentity = botIdentity{}

	cfg := settings.Bot{Token: "token"}
	for i := 0; i < 3; i++ {
		name, err := s.botUsername(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if name != "tdlbot" {
			t.Fatalf("botUsername() = %q, want tdlbot", name)
		}
	}
	if calls := transport.getMeCalls(); calls != 1 {
		t.Fatalf("getMe was asked %d times; the username is a fact about the token", calls)
	}

	// A changed token is a different Bot, and the cached name must not answer
	// for it: the username it holds addresses the previous Bot's chat, so a
	// fallback addressed from it would read the wrong dialog.
	transport.getMe = "anotherbot"
	s.botIdentity = botIdentity{token: "token", username: "tdlbot"}
	name, err := s.botUsername(settings.Bot{Token: "another-token"})
	if err != nil {
		t.Fatal(err)
	}
	if name != "anotherbot" {
		t.Fatalf("botUsername() = %q; the previous token's username answered for this one", name)
	}
	if calls := transport.getMeCalls(); calls != 2 {
		t.Fatalf("getMe was asked %d times across two tokens, want one per token", calls)
	}
}

// A transport failure must not be cached: one outage would otherwise disable
// the fallback for the life of the process, and every later forward would be
// reported as failing for a reason that has passed.
func TestAFailedGetMeIsNotRemembered(t *testing.T) {
	transport := &stubTransport{getMe: "tdlbot", getMeFail: true}
	s := newForwardTestService(t, transport, &fakeSubmitter{})
	s.botIdentity = botIdentity{}

	if _, err := s.botUsername(settings.Bot{Token: "token"}); err == nil {
		t.Fatal("a transport failure has to be reported, not turned into an empty username")
	}
	transport.getMeFail = false
	name, err := s.botUsername(settings.Bot{Token: "token"})
	if err != nil {
		t.Fatalf("the second attempt must reach the Bot again: %v", err)
	}
	if name != "tdlbot" {
		t.Fatalf("botUsername() = %q, want tdlbot once the Bot answers", name)
	}
}
