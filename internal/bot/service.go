package bot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vacks/tdl/internal/adapter/upstream"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/buildinfo"
	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/monitor"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
	"golang.org/x/net/proxy"
)

// Service is intentionally a small Telegram Bot API client. It is separate
// from the user-account MTProto client used by tdl downloads.
type Service struct {
	settings   *settings.Store
	downloads  *download.Manager
	telegram   *telegram.Manager
	monitor    *monitor.Monitor
	cursorPath string
	instanceID string
	cursor     updateCursor
	ctx        context.Context
	cancel     context.CancelFunc

	mu                   sync.Mutex
	clientMu             sync.Mutex
	client               *http.Client
	clientProxy          string
	outboundMu           sync.Mutex
	nextOutbound         time.Time
	outboundBlockedUntil time.Time
	// lifecycleMu serializes a status refresh with deletion. Without it a
	// refresh holding an old completed snapshot could overwrite a later
	// "task deleted" card or send a replacement notification.
	lifecycleMu sync.Mutex
	offset      int64
	token       string
	known       map[string]string
	tracked     map[string]trackedRef
	chatTracked map[string]chatTrackedRef
	listPages   map[string]listPageState
	// lifecycle keeps the one notification card for a job in each authorized
	// chat. The card is edited as the job advances instead of sending a new
	// Bot message for every state transition.
	lifecycle map[string]map[int64]messageRef
	deleted   map[string]time.Time
	dirty     map[string]struct{}
	// suppressedStatus consumes the state event emitted synchronously by a
	// successful Bot action. The callback itself has already rendered that
	// state, so the generic lifecycle refresher must not edit the same card a
	// second time.
	suppressedStatus map[string]string
	ready            bool
	helpSent         map[int64]bool
	helpRetry        map[int64]helpRetry
	lastLiveEdit     map[string]time.Time
	// liveRendered suppresses no-op Bot edits. Telegram counts an edit request
	// even when its visible card is unchanged, so this protects both the Bot
	// quota and active download throughput.
	liveRendered map[string]string
	nextLiveEdit time.Time
	retryUpdates map[int64]int
	// downloadWake receives coalesced domain events. It shortens lifecycle
	// updates without making the Bot poll the task table more aggressively.
	downloadWake chan struct{}
	// dispatch hands commands to workers and tracks which of them the stored
	// offset may move past. See updateDispatch.
	dispatch *updateDispatch
	// commandSlots bounds how many commands run at once. A command waits mostly
	// on Telegram resolution, which is paced per account anyway, so a small pool
	// removes the head-of-line wait without adding load.
	commandSlots chan struct{}
	// wg tracks every goroutine this service starts. Stop waits for it,
	// because the server stops its services in order and the download manager
	// closes the database immediately afterwards: work still running here
	// would be querying a database that is being closed behind it.
	wg sync.WaitGroup
	// unsubscribe releases the domain event subscription. It is kept rather
	// than discarded because closing the channel is what ends the event loop;
	// without it that goroutine blocked on a range that could never finish.
	unsubscribe  func()
	shutdownOnce sync.Once
	// acknowledgeMu serializes the offset file write, which several workers can
	// otherwise attempt at the same moment.
	acknowledgeMu sync.Mutex
	// chatLocks keep the commands of one conversation in submission order. The
	// slot is chosen by chat id, so two unrelated chats occasionally share one;
	// that only costs them a little concurrency, and it avoids a map of locks
	// that would have to be evicted.
	chatLocks [16]sync.Mutex
}

type updateCursor struct {
	TokenHash  string `json:"tokenHash"`
	InstanceID string `json:"instanceId"`
	Offset     int64  `json:"offset"`
}

type messageRef struct {
	ChatID, MessageID int64
	Text              string
	TokenHash         string
}

// key identifies a rendered card in the edit-throttle maps. It is one
// definition because those maps are written from six places - tracking,
// untracking by key, untracking by message, and the throttle itself - and a
// format that drifted in one of them would stop suppressing edits there
// without failing anything.
func (r messageRef) key() string { return fmt.Sprintf("%d:%d", r.ChatID, r.MessageID) }

type trackedRef struct {
	JobID string
	messageRef
}
type chatTrackedRef struct {
	JobID string
	messageRef
}

// listPageState is deliberately kept server-side. Telegram callback data is
// limited to 64 bytes, while a safe database cursor plus task ID and filters
// does not fit. Keeping the small navigation stack per Bot message lets both
// message and chat lists use keyset pagination without OFFSET scans.
type listPageState struct {
	kind   string
	status string
	// savedAccountID narrows a session list to one account's Saved Messages
	// task. The list is otherwise an ordinary session list - same cards, same
	// cursor, same page state - so the narrowing travels with the page rather
	// than needing a second set of callbacks.
	savedAccountID string
	cursor         string
	next           string
	previous       []string
	page           int
	updated        time.Time
}
type helpRetry struct {
	next     time.Time
	attempts int
	// gaveUp ends the retries for this recipient. A send that failed on every
	// attempt with the same answer - the control user blocked the Bot, the chat
	// is gone - will fail on the next one too, and without this the backoff
	// pinned at five minutes means it asks again every five minutes for as long
	// as the process runs. helpSent stays unset, so restarting the service
	// tries once more.
	gaveUp bool
}

const (
	botRequestTimeout  = 12 * time.Second
	botLongPollTimeout = 25 * time.Second
	botPollHTTPTimeout = 35 * time.Second
	// List navigation is convenience state only: canonical tasks remain in
	// PostgreSQL. Bound both its lifetime and its per-message cursor history so
	// a long-running Bot cannot retain unbounded memory from old list cards.
	listPageTTL        = 30 * time.Minute
	maxListPageEntries = 256
	maxListPageHistory = 64
	// startupHelpAttempts bounds the startup greeting. Its backoff caps at five
	// minutes, so without a bound a permanent send failure - a control user who
	// blocked the Bot, a token that was revoked - was retried every five
	// minutes for the life of the process.
	startupHelpAttempts = 8
	// cursorSaveAttempts bounds one write of the update cursor. The write is
	// attempted from the acknowledge path, which holds the lock that orders a
	// conversation's commands, so an unbounded retry there did not merely fail
	// to make progress: it blocked every later acknowledge behind it.
	cursorSaveAttempts = 5
)

func New(store *settings.Store, downloads *download.Manager, telegram *telegram.Manager, monitor *monitor.Monitor, dataDir string) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{settings: store, downloads: downloads, telegram: telegram, monitor: monitor, cursorPath: filepath.Join(dataDir, "bot-updates.json"), instanceID: downloads.InstanceID(), ctx: ctx, cancel: cancel, known: map[string]string{}, tracked: map[string]trackedRef{}, chatTracked: map[string]chatTrackedRef{}, listPages: map[string]listPageState{}, lifecycle: map[string]map[int64]messageRef{}, deleted: map[string]time.Time{}, dirty: map[string]struct{}{}, suppressedStatus: map[string]string{}, helpSent: map[int64]bool{}, helpRetry: map[int64]helpRetry{}, lastLiveEdit: map[string]time.Time{}, liveRendered: map[string]string{}, retryUpdates: map[int64]int{}, downloadWake: make(chan struct{}, 1), dispatch: newUpdateDispatch(), commandSlots: make(chan struct{}, commandWorkers)}
	if data, err := os.ReadFile(s.cursorPath); err == nil {
		if err := json.Unmarshal(data, &s.cursor); err != nil {
			applog.Error("bot", "update_cursor_read_failed", "error", err.Error())
		}
	}
	applog.Info("bot", "service_started")
	events, unsubscribe := downloads.SubscribeEvents()
	s.unsubscribe = unsubscribe
	s.wg.Add(3)
	go func() { defer s.wg.Done(); s.watchDownloadEvents(events) }()
	go func() { defer s.wg.Done(); s.loop() }()
	go func() { defer s.wg.Done(); s.refreshLoop() }()
	return s
}

func (s *Service) watchDownloadEvents(events <-chan download.Event) {
	for event := range events {
		s.mu.Lock()
		// Only a task status change can be the one an action already rendered.
		// Item transitions carry statuses from the same vocabulary - pausing a
		// multi-file task emits one 'paused' per file - and matching on the
		// status alone let those consume a suppression that belonged to the task
		// event, which then went on to produce the second, confusing edit the
		// suppression exists to prevent.
		if expected, suppressed := s.suppressedStatus[event.JobID]; suppressed && event.Kind == "job_status_changed" {
			// The entry is spent either way. If the action landed on a different
			// status than expected, the refresh still has to see it - and leaving
			// the entry behind would let it swallow a later event that happens to
			// carry the status it was waiting for.
			delete(s.suppressedStatus, event.JobID)
			if event.Status != expected {
				s.dirty[event.JobID] = struct{}{}
			}
		} else {
			s.dirty[event.JobID] = struct{}{}
		}
		s.mu.Unlock()
		select {
		case s.downloadWake <- struct{}{}:
		default:
		}
	}
}

func (s *Service) loop() {
	failures := 0
	for {
		if s.ctx.Err() != nil {
			return
		}
		cfg := s.settings.Get().Bot
		if !cfg.Enabled || strings.TrimSpace(cfg.Token) == "" || len(cfg.ControlUserIDs) == 0 {
			if !waitContext(s.ctx, 3*time.Second) {
				return
			}
			continue
		}
		commandsChanged := false
		lifecycleTokenChanged := false
		s.mu.Lock()
		if s.token != cfg.Token {
			// A restart starts with an empty in-memory token. Compare the stored
			// cursor too, so a changed token can never edit cards owned by an old
			// Bot identity after a stopped service is configured again.
			lifecycleTokenChanged = s.token != "" || (s.cursor.TokenHash != "" && s.cursor.TokenHash != tokenFingerprint(cfg.Token))
			s.token = cfg.Token
			if s.cursor.TokenHash == tokenFingerprint(cfg.Token) && s.cursor.InstanceID == s.instanceID {
				s.offset = s.cursor.Offset
			} else {
				s.offset = 0
			}
			s.known, s.tracked, s.lifecycle, s.deleted, s.dirty, s.suppressedStatus, s.ready, s.helpSent, s.helpRetry, s.lastLiveEdit, s.liveRendered, s.retryUpdates, s.nextLiveEdit = map[string]string{}, map[string]trackedRef{}, map[string]map[int64]messageRef{}, map[string]time.Time{}, map[string]struct{}{}, map[string]string{}, true, map[int64]bool{}, map[int64]helpRetry{}, map[string]time.Time{}, map[string]string{}, map[int64]int{}, time.Time{}
			s.chatTracked = map[string]chatTrackedRef{}
			s.listPages = map[string]listPageState{}
			// A different Bot identity starts from its own offset. Workers still
			// finishing for the previous token must not be able to acknowledge
			// into this one, so the tracking starts empty.
			s.dispatch = newUpdateDispatch()
			commandsChanged = true
		}
		s.mu.Unlock()
		if commandsChanged {
			if lifecycleTokenChanged {
				if err := s.downloads.ClearBotLifecycleMessages(); err != nil {
					applog.Error("bot", "lifecycle_messages_clear_failed", "error", err.Error())
				}
			}
			s.configureCommands(cfg.Token)
		}
		s.sendStartupHelp(cfg)
		updates, err := s.getUpdates(cfg.Token)
		if err == nil {
			failures = 0
			for _, update := range updates {
				s.mu.Lock()
				acknowledged := s.offset
				s.mu.Unlock()
				if update.UpdateID < acknowledged {
					continue
				}
				// The smallest id this batch returned is where the acknowledgement
				// walk starts. Telegram only returns updates at or above the
				// offset it was given, so nothing below this one exists, and on a
				// fresh install the stored offset is 0 - an id no update carries.
				// See updateDispatch.lowest.
				s.dispatch.noteDelivered(update.UpdateID)
				if !s.dispatch.claim(update.UpdateID) {
					continue
				}
				// The worker permit is taken inside runCommand, after this
				// conversation's lock, so a command that is waiting for a permit or
				// backing off holds neither the permit nor a place in the queue
				// behind a command that is not working.
				s.wg.Add(1)
				go func() { defer s.wg.Done(); s.runCommand(cfg, update) }()
			}
			s.acknowledgeHandled(cfg.Token)
		} else {
			failures++
			delay := pollRetryDelay(failures)
			if shouldLogPollFailure(failures) {
				applog.Error("bot", "updates_fetch_failed", "attempt", failures, "retry_after", delay.String(), "error", redactBotError(cfg.Token, err.Error()))
			}
			if !waitContext(s.ctx, delay) {
				return
			}
			continue
		}
	}
}

// commandWorkers bounds how many commands are handled at once. Commands do
// Telegram resolution and outbound Bot API calls, both already paced, so the
// pool exists to stop one slow command delaying the others rather than to add
// throughput.
const commandWorkers = 4

// maxCommandAttempts bounds the retries of one command. It is deliberately
// finite: the failures it exists for (a connection error, a flood wait) are
// transient, but the same shape of failure is also what a command that can never
// succeed produces, and an unbounded retry of those keeps the update unfinished
// forever - which stalls the acknowledgement cursor behind it and, once the
// unconfirmed backlog fills, stops the Bot receiving anything.
const maxCommandAttempts = 8

// botCardItemLines bounds the file list a task card renders. Telegram refuses a
// message longer than 4096 characters, and one line is roughly sixty, so this
// leaves room for the header and footer while staying well inside that.
const botCardItemLines = 20

// botCardMessageText bounds how much of the source message's caption the card
// repeats. A caption may itself be the full 4096 characters, and the task table
// keeps it in one column on purpose (see the download_jobs.message_text note) -
// so it is the one field on this card with no natural length. Bounding the file
// list while leaving this one whole bounded the wrong half: the file list was
// already the short part.
const botCardMessageText = 300

// botCardLimit is the longest message Telegram accepts. It applies to the
// finished text, not to the fields composing it, so the fields are bounded
// individually and the assembled card is checked against this as a backstop.
// Exceeding it is not a layout problem: the send is rejected outright, so a
// task's only progress surface stops updating and never recovers.
const botCardLimit = 4096

// runCommand handles one update, retrying in its own goroutine so a failure
// cannot hold the poll loop. It keeps the durable-acknowledgement the loop had:
// the update stays unfinished until it succeeds, and the stored offset cannot
// pass an unfinished update, so a crash during the backoff still leaves the
// command to be redelivered.
func (s *Service) runCommand(cfg settings.Bot, update update) {
	lock := s.commandLock(update)
	lock.Lock()
	defer lock.Unlock()
	for attempt := 1; ; attempt++ {
		// The permit covers one attempt's work, not the backoff between attempts.
		// Holding it across the backoff - as this used to - meant a command
		// waiting out a five minute retry kept one of the four permits for that
		// whole time, while every later command of the same conversation took
		// another one just to block on this conversation's lock.
		if !s.acquireCommandSlot() {
			return
		}
		done := s.handle(cfg, update)
		<-s.commandSlots
		if done {
			s.finishCommand(cfg.Token, update.UpdateID)
			return
		}
		if attempt >= maxCommandAttempts {
			// The command is dropped, loudly, so the offset can move past it. The
			// loop had no limit at all, and a link whose resolution exceeds the
			// submit deadline fails identically every time: retrying it forever is
			// not a retry. It also blocked the cursor, because nothing below an
			// unfinished update can be confirmed, so the unconfirmed backlog grew
			// until the server stopped returning new commands to anyone.
			applog.Error("bot", "command_abandoned", "update_id", update.UpdateID, "attempts", attempt)
			s.finishCommand(cfg.Token, update.UpdateID)
			return
		}
		if !waitContext(s.ctx, s.retryDelay(update.UpdateID)) {
			return
		}
	}
}

// finishCommand records a handled update and stores the offset its completion
// unblocks.
func (s *Service) finishCommand(token string, updateID int64) {
	s.clearRetry(updateID)
	s.dispatch.finish(updateID)
	s.acknowledgeHandled(token)
}

// acquireCommandSlot takes one worker permit, waiting for a free one. It reports
// false when the service is stopping.
func (s *Service) acquireCommandSlot() bool {
	select {
	case s.commandSlots <- struct{}{}:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// commandLock returns the mutex that keeps one conversation's commands in
// submission order, whichever worker picked them up.
func (s *Service) commandLock(update update) *sync.Mutex {
	var chatID int64
	if update.Message != nil {
		chatID = update.Message.Chat.ID
	} else if update.CallbackQuery != nil {
		chatID = update.CallbackQuery.From.ID
	}
	return &s.chatLocks[uint64(chatID)%uint64(len(s.chatLocks))]
}

// acknowledgeHandled stores the highest offset that every finished command
// below it allows, and nothing more: a gap keeps the later updates visible to
// getUpdates so they are redelivered rather than lost.
func (s *Service) acknowledgeHandled(token string) {
	s.acknowledgeMu.Lock()
	defer s.acknowledgeMu.Unlock()
	s.mu.Lock()
	current := s.offset
	s.mu.Unlock()
	next := s.dispatch.acknowledgeable(current)
	if next <= current {
		return
	}
	// The durable write comes first. Deleting the finished records before it - as
	// this used to - meant that a write which never succeeded (a full disk, a
	// read-only data directory) left the stored offset where it was while the
	// only record that those updates had already run was gone: the next poll
	// redelivered them, claim succeeded because nothing remembered them, and the
	// command ran a second time.
	if !s.advanceOffset(token, next) {
		return
	}
	s.dispatch.committed(next)
}

// refreshLoop is intentionally independent from command long-polling. A
// 25-second getUpdates request therefore never delays a viewed task card's
// three-second progress refresh, while an idle Bot performs no task-table I/O.
func (s *Service) refreshLoop() {
	ticker := time.NewTicker(refreshMinInterval)
	defer ticker.Stop()
	for {
		start := time.Now()
		s.pruneListPages(start)
		cfg := s.settings.Get().Bot
		if cfg.Enabled && strings.TrimSpace(cfg.Token) != "" && len(cfg.ControlUserIDs) > 0 {
			s.refresh(cfg)
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		case <-s.downloadWake:
			// A wake asks for the next pass sooner, and the floor is what
			// "sooner" means. Without it a wake runs the next pass immediately,
			// and because the signal is refilled the moment it is drained while
			// a download is emitting a transition per file, the loop ran at the
			// event rate. What it does for each pass is per task, so the loop's
			// cost was set by how many files a task had rather than by how many
			// tasks were being watched. The pass is at most one interval late
			// now, which is not a delay anything can observe.
			if remaining := refreshMinInterval - time.Since(start); remaining > 0 {
				timer := time.NewTimer(remaining)
				select {
				case <-s.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}
}

// refreshMinInterval is the shortest gap between two passes of the refresh
// loop, and the tick that drives it when no wake arrives.
const refreshMinInterval = time.Second

func pollRetryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	shift := min(failures-1, 5)
	delay := 2 * time.Second * time.Duration(1<<shift)
	if delay > time.Minute {
		delay = time.Minute
	}
	// Stable jitter prevents several restarted instances from reconnecting in
	// lockstep without making tests or logs nondeterministic.
	jitter := time.Duration((failures*347)%700) * time.Millisecond
	if delay+jitter > time.Minute {
		return time.Minute
	}
	return delay + jitter
}

func shouldLogPollFailure(failures int) bool {
	return failures == 1 || failures == 2 || failures == 4 || failures == 8 || failures%10 == 0
}

// Stop cancels long polling and releases idle Bot API connections.
func (s *Service) Stop() {
	s.shutdownOnce.Do(func() {
		s.cancel()
		if s.unsubscribe != nil {
			s.unsubscribe()
		}
		s.clientMu.Lock()
		if s.client != nil {
			if transport, ok := s.client.Transport.(*http.Transport); ok {
				transport.CloseIdleConnections()
			}
		}
		s.clientMu.Unlock()
	})
	// Waiting is the point of the group. The server stops its services in
	// order and the download manager closes the database next, so a command
	// still running here would be reading a database being closed under it -
	// which is not a failure anyone can act on, just a burst of errors during
	// a clean shutdown. Bounded, because a shutdown that can hang is worse
	// than one that logs what it left behind.
	stopped := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(botStopTimeout):
		applog.Error("bot", "stop_timed_out", "timeout", botStopTimeout.String())
	}
}

// botStopTimeout bounds how long Stop waits for the service's own goroutines.
const botStopTimeout = 5 * time.Second

// helpRetryStep advances the startup greeting's retry state after a failed
// send, and reports whether to try again at all.
//
// The backoff is capped at five minutes, which is what made the bound
// necessary: a send that fails for a reason no retry can change - the control
// user blocked the Bot, the token was revoked - would otherwise be attempted
// again every five minutes for the life of the process. Giving up leaves
// helpSent unset, so a later start of the service tries once more.
func helpRetryStep(retry helpRetry, now time.Time) (helpRetry, bool) {
	if retry.attempts >= startupHelpAttempts {
		retry.gaveUp = true
		return retry, false
	}
	retry.attempts++
	delay := time.Second * time.Duration(1<<min(retry.attempts, 8))
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	retry.next = now.Add(delay)
	return retry, true
}

// sendStartupHelp confirms to authorized users that Bot control is available
// after this application instance has completed its initialization.
func (s *Service) sendStartupHelp(cfg settings.Bot) {
	for _, id := range cfg.ControlUserIDs {
		s.mu.Lock()
		alreadySent := s.helpSent[id]
		retry := s.helpRetry[id]
		s.mu.Unlock()
		if alreadySent || retry.gaveUp || time.Now().Before(retry.next) {
			continue
		}
		if _, err := s.send(cfg.Token, id, helpText(), nil); err != nil {
			applog.Error("bot", "startup_help_send_failed", "chat_id", id, "attempt", retry.attempts+1, "error", redactBotError(cfg.Token, err.Error()))
			next, again := helpRetryStep(retry, time.Now())
			s.mu.Lock()
			s.helpRetry[id] = next
			s.mu.Unlock()
			if !again {
				applog.Error("bot", "startup_help_send_gave_up", "chat_id", id, "attempts", next.attempts, "error", redactBotError(cfg.Token, err.Error()))
			}
			continue
		}
		s.mu.Lock()
		s.helpSent[id] = true
		delete(s.helpRetry, id)
		s.mu.Unlock()
	}
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	Result      T      `json:"result"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}
type update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *message       `json:"message"`
	CallbackQuery *callbackQuery `json:"callback_query"`
}
type message struct {
	MessageID int64 `json:"message_id"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	From struct {
		ID int64 `json:"id"`
	} `json:"from"`
	Text string `json:"text"`
	// ForwardOrigin is where a forwarded message came from. It is what makes a
	// forward submittable at all: the origin names the chat and the message, which
	// is exactly what a t.me link names.
	ForwardOrigin *forwardOrigin `json:"forward_origin"`
	// The pre-7.0 fields Telegram still sends next to forward_origin. A client
	// that only sets these must not lose the ability to submit a forward, and
	// reading both costs nothing.
	ForwardFromChat      *forwardOriginChat `json:"forward_from_chat"`
	ForwardFromMessageID int                `json:"forward_from_message_id"`
}

// forwardOrigin is narrowed to the fields a message link is made of. The
// origin's own type is deliberately not read: what decides whether a forward
// can be addressed is the id encoding below, and a chat whose type string a
// client left out would still be addressable.
type forwardOrigin struct {
	Chat      *forwardOriginChat `json:"chat"`
	MessageID int                `json:"message_id"`
}

// forwardOriginChat is the chat object inside a forward origin.
type forwardOriginChat struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

// forwardedMessageURL is the link addressing the post a forwarded message came
// from, and reports whether this message was such a forward.
//
// The link is built rather than the message being resolved directly because the
// Bot API never exposes an access hash, so there is no InputPeer to hand the
// downloader - and resolving one would cost a request the link path already
// knows how to make.
//
// Only channels and supergroups qualify. Their Bot API id encodes the id a
// t.me/c link needs; a private chat or a basic group has no message link at
// all, so those forwards are left to the ordinary handling instead of being
// turned into a link nothing can resolve.
func forwardedMessageURL(msg message) (string, bool) {
	chat, messageID := forwardTarget(msg)
	if chat == nil || messageID <= 0 {
		return "", false
	}
	internal, addressable := channelInternalID(chat.ID)
	if !addressable {
		return "", false
	}
	// A public chat is preferred by name: it resolves through the username rather
	// than through the account's cached access hash, so it also works for a
	// channel this instance has never looked at before.
	if username := strings.TrimPrefix(strings.TrimSpace(chat.Username), "@"); username != "" {
		return fmt.Sprintf("https://t.me/%s/%d", username, messageID), true
	}
	return fmt.Sprintf("https://t.me/c/%d/%d", internal, messageID), true
}

// forwardTarget returns the chat and message a forward points at, from either
// shape of the field. forward_origin wins: when Telegram sends both, the legacy
// fields describe the same forward.
func forwardTarget(msg message) (*forwardOriginChat, int) {
	if origin := msg.ForwardOrigin; origin != nil && origin.Chat != nil {
		return origin.Chat, origin.MessageID
	}
	return msg.ForwardFromChat, msg.ForwardFromMessageID
}

// channelInternalID converts the Bot API id of a channel or supergroup - the
// -100 prefix followed by the internal id - into the id a t.me/c link carries.
// It reports false for every other chat, including a basic group, whose
// negative id is not addressable that way.
func channelInternalID(chatID int64) (int64, bool) {
	const prefix = int64(1000000000000)
	if chatID >= -prefix {
		return 0, false
	}
	return -chatID - prefix, true
}

type callbackQuery struct {
	ID   string `json:"id"`
	From struct {
		ID int64 `json:"id"`
	} `json:"from"`
	Data    string   `json:"data"`
	Message *message `json:"message"`
}

func (s *Service) getUpdates(token string) ([]update, error) {
	s.mu.Lock()
	offset := s.offset
	s.mu.Unlock()
	var result apiResponse[[]update]
	if err := s.callWithTimeout(token, "getUpdates", map[string]any{"offset": offset, "timeout": int(botLongPollTimeout / time.Second), "allowed_updates": []string{"message", "callback_query"}}, &result, botPollHTTPTimeout); err != nil {
		return nil, err
	}
	if !result.OK {
		return nil, fmt.Errorf("Bot API: %s", result.Description)
	}
	return result.Result, nil
}

func (s *Service) advanceOffset(token string, offset int64) bool {
	s.mu.Lock()
	if offset <= s.offset {
		s.mu.Unlock()
		return true
	}
	s.mu.Unlock()
	cursor := updateCursor{TokenHash: tokenFingerprint(token), InstanceID: s.instanceID, Offset: offset}
	data, err := json.Marshal(cursor)
	if err != nil {
		applog.Error("bot", "update_cursor_encode_failed", "error", err.Error())
		return false
	}
	// The loop is bounded. It used to run until the write succeeded, which is
	// right for a transient failure and wrong for a path that can never
	// succeed: an unwritable cursor file is a permission or filesystem state,
	// not a race, so the retries went on for as long as the process lived and
	// the caller never returned. Callers are the acknowledge path, which holds
	// the ordering lock for a conversation's commands, so that block also
	// stopped every acknowledge behind it. Giving up here leaves the in-memory
	// offset untouched - which only means Telegram redelivers updates the
	// dispatcher already knows - and the next acknowledge tries again.
	for attempt := 0; attempt < cursorSaveAttempts; attempt++ {
		if err := writePrivateFile(s.cursorPath, data); err == nil {
			s.mu.Lock()
			if offset > s.offset {
				s.offset = offset
				s.cursor = cursor
			}
			s.mu.Unlock()
			return true
		} else {
			applog.Error("bot", "update_cursor_save_failed", "offset", offset, "attempt", attempt+1, "error", err.Error())
		}
		if attempt == cursorSaveAttempts-1 {
			break
		}
		delay := time.Second * time.Duration(1<<min(attempt, 5))
		if !waitContext(s.ctx, delay) {
			return false
		}
	}
	applog.Error("bot", "update_cursor_save_gave_up", "offset", offset, "attempts", cursorSaveAttempts)
	return false
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum[:])
}

func (s *Service) handle(cfg settings.Bot, update update) bool {
	if update.Message != nil {
		if !allowed(cfg, update.Message.From.ID) || !privateChat(*update.Message) {
			applog.Info("bot", "update_rejected", "kind", "message", "user_id", update.Message.From.ID)
			return true
		}
		applog.Info("bot", "command_received", "user_id", update.Message.From.ID)
		return s.handleMessage(cfg, *update.Message)
	}
	if update.CallbackQuery != nil && allowed(cfg, update.CallbackQuery.From.ID) && privateCallback(*update.CallbackQuery) {
		applog.Info("bot", "callback_received", "user_id", update.CallbackQuery.From.ID)
		s.handleCallback(cfg, *update.CallbackQuery)
	}
	return true
}

// Bot control is deliberately private-chat only. An authorized user can be a
// member of many groups, but task names and download state must never be sent
// into those groups merely because they invoked a command there.
func privateChat(message message) bool { return message.Chat.ID == message.From.ID }

// CallbackQuery.Message.From is the Bot that sent the card, not the person
// pressing it. Test private-ness against CallbackQuery.From instead.
func privateCallback(query callbackQuery) bool {
	return query.Message != nil && query.Message.Chat.ID == query.From.ID
}

func (s *Service) handleMessage(cfg settings.Bot, msg message) bool {
	// A forwarded post is the thing the user wants downloaded, whatever its
	// caption happens to say, so it is submitted before the caption is read as a
	// command. Only a forward that names a channel post is taken this way; the
	// rest fall through and are handled exactly as they were before.
	if source, forwarded := forwardedMessageURL(msg); forwarded {
		return s.submitMessageTask(cfg, msg, source)
	}
	kind, argument := parseCommand(normalizeCommand(strings.TrimSpace(msg.Text)))
	switch kind {
	case commandHelp:
		s.send(cfg.Token, msg.Chat.ID, helpText(), nil)
	case commandTasks:
		s.sendTaskList(cfg, msg.Chat.ID, "")
	case commandTaskFilter:
		s.sendTaskFilter(cfg, msg.Chat.ID)
	case commandChatList:
		s.sendChatList(cfg, msg.Chat.ID)
	case commandChatCreate:
		s.createChatTask(cfg, msg, argument)
	case commandSavedAll:
		s.handleSavedCommand(cfg, msg, savedAll)
	case commandSavedListen:
		s.handleSavedCommand(cfg, msg, savedListen)
	case commandSavedTask:
		s.handleSavedCommand(cfg, msg, savedTask)
	case commandStatus:
		s.send(cfg.Token, msg.Chat.ID, s.statusText(), nil)
	case commandEvents:
		s.sendEventList(cfg, msg.Chat.ID)
	case commandEventClear:
		s.sendEventClearConfirm(cfg, msg.Chat.ID)
	case commandConfig:
		s.send(cfg.Token, msg.Chat.ID, configText(s.settings.Get()), nil)
	case commandRestart:
		if _, err := s.send(cfg.Token, msg.Chat.ID, "🔄 正在重启所有服务…", nil); err != nil {
			applog.Error("bot", "restart_notice_send_failed", "error", redactBotError(cfg.Token, err.Error()))
			return !retryableSubmitError(err)
		}
		applog.Info("bot", "service_restart_requested", "user_id", msg.From.ID)
		go func() {
			time.Sleep(time.Second)
			if process, err := os.FindProcess(os.Getpid()); err == nil {
				_ = process.Signal(syscall.SIGTERM)
			}
		}()
	case commandLink:
		return s.submitMessageTask(cfg, msg, argument)
	default:
		s.send(cfg.Token, msg.Chat.ID, "发送 <code>/help</code> 查看可用命令。", nil)
	}
	return true
}

// command is what a message turns out to be asking for. Recognition is a pure
// function of the text so that the set of commands the Bot answers is one thing
// a test can compare against the help text and the Telegram command menu - a
// rename the dispatcher did not follow is otherwise a command that silently
// reaches the "unknown" branch and tells the user to read /help.
type command int

const (
	commandUnknown command = iota
	commandHelp
	commandTasks
	commandTaskFilter
	commandChatList
	commandChatCreate
	commandSavedAll
	commandSavedListen
	commandSavedTask
	commandStatus
	commandEvents
	commandEventClear
	commandConfig
	commandRestart
	commandLink
)

// parseCommand classifies one normalized message and returns its argument, if
// the command takes one.
func parseCommand(text string) (command, string) {
	switch {
	case text == "/start" || text == "/help":
		return commandHelp, ""
	case text == "/tasks":
		return commandTasks, ""
	case text == "/task_filter":
		return commandTaskFilter, ""
	case text == "/chats":
		return commandChatList, ""
	case strings.HasPrefix(text, "/chats "):
		return commandChatCreate, strings.TrimSpace(strings.TrimPrefix(text, "/chats "))
	case text == "/saved_all":
		return commandSavedAll, ""
	case text == "/saved_listen":
		return commandSavedListen, ""
	case text == "/saved_task":
		return commandSavedTask, ""
	case text == "/status":
		return commandStatus, ""
	case text == "/events":
		return commandEvents, ""
	case text == "/event_clear":
		return commandEventClear, ""
	case text == "/config":
		return commandConfig, ""
	case text == "/restart":
		return commandRestart, ""
	case isTelegramLink(text):
		return commandLink, text
	default:
		return commandUnknown, ""
	}
}

// submitMessageTask creates a message download task for a Telegram message
// link. A pasted link and a forwarded post both arrive here, because a forward
// carries what a link carries - which chat, which message - and two entry
// points for one download is how the two ways of asking for it drift apart.
//
// The returned bool is the retry decision the poll loop acts on: a failure that
// another attempt cannot change is reported as handled so the command is not
// retried forever.
func (s *Service) submitMessageTask(cfg settings.Bot, msg message, sourceURL string) bool {
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	submission, err := s.downloads.Submit(ctx, download.DownloadIntent{Source: download.SourceBot, URL: sourceURL})
	cancel()
	if err != nil {
		applog.Error("bot", "task_create_failed", "user_id", msg.From.ID, "error", err.Error())
		s.send(cfg.Token, msg.Chat.ID, "❌ 创建下载任务失败："+html.EscapeString(err.Error()), nil)
		return !retryableSubmitError(err)
	}
	job := submission.Job
	if submission.Duplicate {
		applog.Info("bot", "task_request_attached", "user_id", msg.From.ID, "job_id", job.ID)
	} else {
		applog.Info("bot", "task_created", "user_id", msg.From.ID, "job_id", job.ID, "item_count", job.TotalItems)
	}
	// The lifecycle card is deliberately not rendered here. Submit emits
	// job_created before it returns, and the refresh loop renders exactly one
	// card for a job it has not seen yet — the same single path the Web UI and
	// reactions already rely on. Sending a card from this handler as well raced
	// with that loop and produced "下载任务已创建" plus a second progress card.
	return true
}

func retryableSubmitError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "timeout") || strings.Contains(message, "temporar") || strings.Contains(message, "connection") || strings.Contains(message, "network") || strings.Contains(message, "flood_wait")
}

func (s *Service) retryDelay(updateID int64) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt := s.retryUpdates[updateID] + 1
	if attempt > 8 {
		attempt = 8
	}
	s.retryUpdates[updateID] = attempt
	delay := time.Second * time.Duration(1<<attempt)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func (s *Service) clearRetry(updateID int64) {
	s.mu.Lock()
	delete(s.retryUpdates, updateID)
	s.mu.Unlock()
}

func (s *Service) handleCallback(cfg settings.Bot, query callbackQuery) {
	// Every path out of this handler answers the query. Telegram leaves the
	// button showing its loading state until answerCallbackQuery arrives, so an
	// unrecognized or malformed payload that returned silently left the person
	// who tapped it watching a spinner until the client gave up.
	if query.Message == nil {
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if strings.HasPrefix(query.Data, "c:") {
		s.handleChatCallback(cfg, query)
		return
	}
	if strings.HasPrefix(query.Data, "e:") {
		s.handleEventCallback(cfg, query)
		return
	}
	if strings.HasPrefix(query.Data, "f:") {
		s.taskFilterCallback(cfg, query)
		return
	}
	if strings.HasPrefix(query.Data, "l:") {
		direction := strings.TrimPrefix(query.Data, "l:")
		if direction != "prev" && direction != "next" && direction != "refresh" && direction != "back" {
			s.answer(cfg.Token, query.ID, "")
			return
		}
		s.untrackMessage(query.Message.Chat.ID, query.Message.MessageID)
		if direction == "back" {
			direction = "refresh"
		}
		s.editTaskList(cfg, query.Message.Chat.ID, query.Message.MessageID, direction)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if strings.HasPrefix(query.Data, "v:") {
		id := strings.TrimPrefix(query.Data, "v:")
		if id == "" || strings.Contains(id, ":") {
			s.answer(cfg.Token, query.ID, "")
			return
		}
		s.editTask(cfg, query.Message.Chat.ID, query.Message.MessageID, id, s.hasListPage("task", query.Message.Chat.ID, query.Message.MessageID))
		s.answer(cfg.Token, query.ID, "")
		return
	}
	parts := strings.Split(query.Data, ":")
	if len(parts) != 3 || parts[0] != "t" {
		s.answer(cfg.Token, query.ID, "")
		return
	}
	id, action := parts[1], parts[2]
	if id == "list" && action == "refresh" {
		s.sendTaskList(cfg, query.Message.Chat.ID, "")
		s.answer(cfg.Token, query.ID, "已刷新")
		return
	}
	if action == "view" {
		s.editTask(cfg, query.Message.Chat.ID, query.Message.MessageID, id, s.hasListPage("task", query.Message.Chat.ID, query.Message.MessageID))
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if action == "delete" {
		// Keep the task's references before Delete removes their database rows.
		// The lock also prevents a stale refresh from emitting a completion card
		// after this deletion has been confirmed.
		s.lifecycleMu.Lock()
		refs := s.lifecycleRefs(id)
		s.markDeleted(id)
		err := s.downloads.Delete(id)
		if err != nil {
			s.unmarkDeleted(id)
			s.lifecycleMu.Unlock()
			s.answer(cfg.Token, query.ID, err.Error())
			return
		}
		s.markLifecycleDeleted(cfg, refs, query.Message.Chat.ID, query.Message.MessageID)
		s.untrackJob(id)
		s.forgetLifecycle(id)
		s.lifecycleMu.Unlock()
		s.answer(cfg.Token, query.ID, "操作成功")
		s.edit(cfg.Token, query.Message.Chat.ID, query.Message.MessageID, deletedTaskText(query.Message.Text), deletedTaskKeyboard(s.hasListPage("task", query.Message.Chat.ID, query.Message.MessageID)))
		return
	}
	// Registered before the action runs, not after it. The action emits its
	// status event synchronously from inside the download manager, so a
	// suppression written afterwards - as this used to do, from
	// renderActionResult, across an answerCallbackQuery round trip - raced the
	// watcher goroutine and usually lost, leaving an entry nothing would ever
	// consume and letting the generic refresh edit the card a second time.
	if expected, known := actionResultStatus[action]; known {
		s.mu.Lock()
		s.suppressedStatus[id] = expected
		s.mu.Unlock()
	}
	var err error
	switch action {
	case "pause":
		err = s.downloads.Pause(id)
	case "resume":
		err = s.downloads.Resume(id)
	case "retry":
		err = s.downloads.Retry(id)
	case "cancel":
		err = s.downloads.Cancel(id)
	default:
		err = fmt.Errorf("未知操作")
	}
	if err != nil {
		// The action never ran, so no event is coming to consume the entry.
		s.forgetSuppressed(id)
		s.answer(cfg.Token, query.ID, err.Error())
		return
	}
	s.answer(cfg.Token, query.ID, "操作成功")
	s.renderActionResult(cfg, *query.Message, id, s.hasListPage("task", query.Message.Chat.ID, query.Message.MessageID))
}

// actionResultStatus is the durable status each card action drives its task to.
// The download manager's control methods are the single definition of these
// transitions, and knowing the outcome in advance is what lets the event they
// emit be suppressed before it is emitted.
var actionResultStatus = map[string]string{
	"pause":  "paused",
	"resume": "queued",
	"retry":  "queued",
	"cancel": "cancelled",
}

// forgetSuppressed drops a suppression whose action never produced the event it
// was waiting for.
func (s *Service) forgetSuppressed(jobID string) {
	s.mu.Lock()
	delete(s.suppressedStatus, jobID)
	s.mu.Unlock()
}

// renderActionResult is the sole post-action renderer. It runs after the action
// has already emitted the status event that the pre-registered suppression
// consumed, so it only has to record the status it rendered and clear the
// pending flag that event would otherwise have set.
func (s *Service) renderActionResult(cfg settings.Bot, message message, jobID string, fromList bool) {
	job, err := s.downloads.Get(jobID)
	if err != nil {
		s.edit(cfg.Token, message.Chat.ID, message.MessageID, "任务不存在或已删除。", nil)
		return
	}
	s.mu.Lock()
	s.known[job.ID] = job.Status
	delete(s.dirty, job.ID)
	s.mu.Unlock()
	if s.isLifecycleMessage(job.ID, message.Chat.ID, message.MessageID) {
		s.updateLifecycle(cfg, job)
		return
	}
	s.edit(cfg.Token, message.Chat.ID, message.MessageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, fromList))
	if active(job.Status) {
		s.track(job.ID, messageRef{ChatID: message.Chat.ID, MessageID: message.MessageID})
	} else {
		s.untrackMessage(message.Chat.ID, message.MessageID)
	}
}

func (s *Service) sendTaskList(cfg settings.Bot, chatID int64, status string) {
	state := listPageState{kind: "task", status: status, page: 1}
	text, buttons, err := s.taskList(&state)
	if err != nil {
		s.send(cfg.Token, chatID, "读取任务列表失败。", nil)
		return
	}
	messageID, err := s.send(cfg.Token, chatID, text, buttons)
	if err == nil {
		s.putListPage("task", chatID, messageID, state)
	}
}

// sendTaskFilter offers one button per status a task can be filtered by. The
// filtering itself lives behind the buttons rather than in the command, because
// a status is a choice out of a closed set and a keyboard is how Telegram asks
// for one; it also means a person never has to remember the wording.
func (s *Service) sendTaskFilter(cfg settings.Bot, chatID int64) {
	s.send(cfg.Token, chatID, taskFilterText(), taskFilterKeyboard())
}

func taskFilterText() string {
	return "<b>筛选任务</b>\n请选择要查看的任务状态。"
}

// taskFilterKeyboard is built from the task filters the download package
// defines, so the buttons, the labels and the values they are normalized back
// into are one vocabulary.
func taskFilterKeyboard() [][]button {
	rows := make([][]button, 0, 3)
	row := []button{{Text: "全部", CallbackData: "f:all"}}
	for _, status := range download.TaskStatusFilters() {
		row = append(row, button{Text: download.TaskStatusFilterLabel(status), CallbackData: "f:" + status})
		if len(row) == 3 {
			rows = append(rows, row)
			row = nil
		}
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}
	return rows
}

// editTaskListFilter renders a filtered task list into the message that carried
// the filter buttons, replacing them.
//
// It goes through taskList and putListPage, exactly as a list sent by /tasks
// does, so the resulting card pages through its own cursor - the status travels
// in the stored page state and survives 上一页/下一页 without being repeated in
// every callback payload.
func (s *Service) editTaskListFilter(cfg settings.Bot, chatID, messageID int64, status string) {
	state := listPageState{kind: "task", status: status, page: 1}
	text, buttons, err := s.taskList(&state)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "读取任务列表失败。", nil)
		return
	}
	s.putListPage("task", chatID, messageID, state)
	s.edit(cfg.Token, chatID, messageID, text, buttons)
}

// taskFilterCallback handles a press on a filter button: the status is
// normalized through the same function the rest of the program uses, so an
// unknown payload produces no list rather than an unfiltered one.
func (s *Service) taskFilterCallback(cfg settings.Bot, query callbackQuery) {
	status, err := download.NormalizeTaskStatusFilter(strings.TrimPrefix(query.Data, "f:"))
	if err != nil {
		s.answer(cfg.Token, query.ID, "")
		return
	}
	s.editTaskListFilter(cfg, query.Message.Chat.ID, query.Message.MessageID, status)
	s.answer(cfg.Token, query.ID, download.TaskStatusFilterLabel(status))
}

func (s *Service) editTaskList(cfg settings.Bot, chatID, messageID int64, direction string) {
	state, ok := s.getListPage("task", chatID, messageID)
	if !ok {
		s.edit(cfg.Token, chatID, messageID, "任务列表已过期，请重新发送 /tasks。", nil)
		return
	}
	if direction != "refresh" {
		var moved bool
		state, moved = moveListPage(state, direction)
		if !moved {
			return
		}
	}
	text, buttons, err := s.taskList(&state)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "读取任务列表失败。", nil)
		return
	}
	s.putListPage("task", chatID, messageID, state)
	s.edit(cfg.Token, chatID, messageID, text, buttons)
}

func (s *Service) taskList(state *listPageState) (string, [][]button, error) {
	jobs, total, next, err := s.downloads.ListCursor(state.cursor, 10, state.status)
	if err != nil {
		return "", nil, err
	}
	if len(jobs) == 0 {
		return "暂无下载任务。", nil, nil
	}
	totalPages := max(1, (total+9)/10)
	state.next = next
	buttons := make([][]button, 0, len(jobs)+1)
	for _, job := range jobs {
		label := fmt.Sprintf("%s %s %d/%d文件", statusIcon(job.Status), short(job.DialogName, 18), job.CompletedItems, job.TotalItems)
		buttons = append(buttons, []button{{Text: label, CallbackData: "v:" + job.ID}})
	}
	navigation := make([]button, 0, 3)
	if len(state.previous) > 0 {
		navigation = append(navigation, button{Text: "‹ 上一页", CallbackData: "l:prev"})
	}
	navigation = append(navigation, button{Text: fmt.Sprintf("第 %d/%d 页", state.page, totalPages), CallbackData: "l:refresh"})
	if state.next != "" {
		navigation = append(navigation, button{Text: "下一页 ›", CallbackData: "l:next"})
	}
	buttons = append(buttons, navigation)
	title := "下载任务"
	if label := download.TaskStatusFilterLabel(state.status); label != "" {
		title += " · " + label
	}
	return fmt.Sprintf("<b>%s</b> · 共 %d 个 · 第 %d/%d 页", title, total, state.page, totalPages), buttons, nil
}

func (s *Service) createChatTask(cfg settings.Bot, msg message, rawURL string) {
	if !isTelegramLink(rawURL) {
		s.send(cfg.Token, msg.Chat.ID, "请输入有效的 Telegram 频道或群组链接。", nil)
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	job, err := s.downloads.SubmitChat(ctx, download.ChatIntent{Source: download.SourceBot, URL: rawURL})
	cancel()
	if err != nil {
		applog.Error("bot", "chat_task_create_failed", "user_id", msg.From.ID, "error", err.Error())
		s.send(cfg.Token, msg.Chat.ID, "❌ 创建会话下载失败："+html.EscapeString(err.Error()), nil)
		return
	}
	applog.Info("bot", "chat_task_created", "user_id", msg.From.ID, "chat_job_id", job.ID)
	messageID, err := s.send(cfg.Token, msg.Chat.ID, chatTaskText(job), chatTaskKeyboard(job, ""))
	if err == nil && active(job.Status) {
		s.trackChat(job.ID, messageRef{ChatID: msg.Chat.ID, MessageID: messageID})
	}
}

// savedCommand is one of the three things a control user can ask of their Saved
// Messages. They are separate commands rather than one command with an
// argument, so Telegram offers them in its command menu like any other.
type savedCommand int

const (
	savedAll savedCommand = iota
	savedListen
	savedTask
)

func (s *Service) handleSavedCommand(cfg settings.Bot, msg message, kind savedCommand) {
	account, err := s.telegram.AuthorizedByTelegramID(msg.From.ID)
	if err != nil {
		message := "当前 Telegram 用户尚未登录，无法下载收藏消息。请先在登录管理完成该账号登录。"
		if !errors.Is(err, telegram.ErrNotAuthorized) {
			message = err.Error()
		}
		s.send(cfg.Token, msg.Chat.ID, "❌ "+html.EscapeString(message), nil)
		return
	}
	switch kind {
	case savedAll:
		ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
		job, existing, submitErr := s.downloads.SubmitSaved(ctx, download.SavedIntent{Source: download.SourceBot, AccountID: account.ID, TelegramID: account.TelegramID})
		cancel()
		if submitErr != nil {
			s.send(cfg.Token, msg.Chat.ID, "❌ 创建收藏夹任务失败："+html.EscapeString(submitErr.Error()), nil)
			return
		}
		// The account has one saved task, so this either made it or sent it back
		// over the history it already covers. Saying which keeps the reply
		// honest about a scan that may already be running.
		label := "✅ 收藏夹历史下载任务已创建"
		if existing {
			label = "✅ 已重新扫描收藏夹任务"
		}
		s.send(cfg.Token, msg.Chat.ID, label, [][]button{{{Text: "查看详情", CallbackData: "c:v:" + job.ID}}})
	case savedListen:
		s.toggleSavedListen(cfg, account, msg)
	case savedTask:
		s.sendSavedTaskList(cfg, msg.Chat.ID, account.ID)
	}
}

// toggleSavedListen turns the account's Saved Messages listener on if it is off
// and off if it is on, so one command covers both without the user having to
// know which state they are in - which is the state they would otherwise have
// to look up with /saved_task first.
//
// The two directions are the two operations the separate start and stop
// commands used to be, unchanged: what is new is only the decision between
// them, and FindSavedListener is what makes it.
func (s *Service) toggleSavedListen(cfg settings.Bot, account telegram.Account, msg message) {
	job, found, findErr := s.downloads.FindSavedListener(account.ID)
	if findErr != nil {
		s.send(cfg.Token, msg.Chat.ID, "❌ 读取收藏夹任务失败："+html.EscapeString(findErr.Error()), nil)
		return
	}
	if found && download.IsSavedListenForBot(job) {
		if err := s.downloads.SetChatListening(job.ID, false); err != nil {
			s.send(cfg.Token, msg.Chat.ID, "❌ 停止收藏夹监听失败："+html.EscapeString(err.Error()), nil)
			return
		}
		s.send(cfg.Token, msg.Chat.ID, "✅ 已停止收藏夹新消息监听。", nil)
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	started, _, submitErr := s.downloads.SubmitSaved(ctx, download.SavedIntent{Source: download.SourceBot, AccountID: account.ID, TelegramID: account.TelegramID, ListenOnly: true})
	cancel()
	if submitErr != nil {
		s.send(cfg.Token, msg.Chat.ID, "❌ 开启收藏夹监听失败："+html.EscapeString(submitErr.Error()), nil)
		return
	}
	s.send(cfg.Token, msg.Chat.ID, "✅ 收藏夹新消息监听已开启", [][]button{{{Text: "查看详情", CallbackData: "c:v:" + started.ID}}})
}

func (s *Service) sendChatList(cfg settings.Bot, chatID int64) {
	state := listPageState{kind: "chat", page: 1}
	text, buttons, err := s.chatTaskList(&state)
	if err != nil {
		s.send(cfg.Token, chatID, "读取会话下载列表失败。", nil)
		return
	}
	messageID, err := s.send(cfg.Token, chatID, text, buttons)
	if err == nil {
		s.putListPage("chat", chatID, messageID, state)
	}
}

// sendSavedTaskList shows the account's Saved Messages task through the same
// list the session downloads use: one button per task, a card behind it, and a
// way back to the list.
func (s *Service) sendSavedTaskList(cfg settings.Bot, chatID int64, accountID string) {
	state := listPageState{kind: "chat", savedAccountID: accountID, page: 1}
	text, buttons, err := s.chatTaskList(&state)
	if err != nil {
		s.send(cfg.Token, chatID, "读取收藏夹任务失败。", nil)
		return
	}
	messageID, err := s.send(cfg.Token, chatID, text, buttons)
	if err == nil {
		s.putListPage("chat", chatID, messageID, state)
	}
}

// chatListBack is the return button for a card that was rendered from a session
// list: its label, and whether the card came from one at all.
//
// The label follows the page state kept for this message rather than the task,
// because it is that list the button goes back to - a saved task reached from
// /chats returns to the session list, and the same task reached from
// /saved_task returns to the saved one.
func (s *Service) chatListBack(chatID, messageID int64) (string, bool) {
	state, ok := s.getListPage("chat", chatID, messageID)
	if !ok {
		return "", false
	}
	if state.savedAccountID != "" {
		return "‹ 返回收藏夹列表", true
	}
	return "‹ 返回会话列表", true
}

func (s *Service) editChatList(cfg settings.Bot, chatID, messageID int64, direction string) {
	state, ok := s.getListPage("chat", chatID, messageID)
	if !ok {
		s.edit(cfg.Token, chatID, messageID, "会话下载列表已过期，请重新发送 /chats。", nil)
		return
	}
	if direction != "refresh" {
		var moved bool
		state, moved = moveListPage(state, direction)
		if !moved {
			return
		}
	}
	text, buttons, err := s.chatTaskList(&state)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "读取会话下载列表失败。", nil)
		return
	}
	s.putListPage("chat", chatID, messageID, state)
	s.edit(cfg.Token, chatID, messageID, text, buttons)
}

func (s *Service) chatTaskList(state *listPageState) (string, [][]button, error) {
	var filters []string
	title, empty := "会话下载", "暂无会话下载任务。"
	if state.savedAccountID != "" {
		filters = append(filters, download.SavedChatsFilter(state.savedAccountID))
		title, empty = "收藏夹任务", "暂无收藏夹任务。"
	}
	jobs, total, next, err := s.downloads.ListChats(state.cursor, 10, filters...)
	if err != nil {
		return "", nil, err
	}
	if len(jobs) == 0 {
		return empty, nil, nil
	}
	totalPages := max(1, (total+9)/10)
	state.next = next
	buttons := make([][]button, 0, len(jobs)+1)
	for _, job := range jobs {
		label := fmt.Sprintf("%s %s %d/%d下载", statusIcon(job.Status), short(job.DialogName, 18), job.Completed, job.Discovered)
		buttons = append(buttons, []button{{Text: label, CallbackData: "c:v:" + job.ID}})
	}
	navigation := make([]button, 0, 3)
	if len(state.previous) > 0 {
		navigation = append(navigation, button{Text: "‹ 上一页", CallbackData: "c:l:prev"})
	}
	navigation = append(navigation, button{Text: fmt.Sprintf("第 %d/%d 页", state.page, totalPages), CallbackData: "c:l:refresh"})
	if state.next != "" {
		navigation = append(navigation, button{Text: "下一页 ›", CallbackData: "c:l:next"})
	}
	buttons = append(buttons, navigation)
	return fmt.Sprintf("<b>%s</b> · 共 %d 个 · 第 %d/%d 页", title, total, state.page, totalPages), buttons, nil
}

// eventPageSize is the number of rows one /events page shows. Ten events plus a
// header is a card that reads without scrolling on a phone.
const eventPageSize = 10

func (s *Service) sendEventList(cfg settings.Bot, chatID int64) {
	state := listPageState{kind: "event", page: 1}
	text, buttons, err := s.eventList(&state)
	if err != nil {
		s.send(cfg.Token, chatID, "读取监听事件失败。", nil)
		return
	}
	messageID, err := s.send(cfg.Token, chatID, text, buttons)
	if err == nil {
		s.putListPage("event", chatID, messageID, state)
	}
}

func (s *Service) editEventList(cfg settings.Bot, chatID, messageID int64, direction string) {
	state, ok := s.getListPage("event", chatID, messageID)
	if !ok {
		s.edit(cfg.Token, chatID, messageID, "事件列表已过期，请重新发送 /events。", nil)
		return
	}
	if direction != "refresh" {
		var moved bool
		state, moved = moveListPage(state, direction)
		if !moved {
			return
		}
	}
	text, buttons, err := s.eventList(&state)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "读取监听事件失败。", nil)
		return
	}
	s.putListPage("event", chatID, messageID, state)
	s.edit(cfg.Token, chatID, messageID, text, buttons)
}

func (s *Service) eventList(state *listPageState) (string, [][]button, error) {
	events, total, next, err := s.downloads.ListListenerEvents(state.cursor, eventPageSize)
	if err != nil {
		return "", nil, err
	}
	if len(events) == 0 {
		return "暂无监听事件。", nil, nil
	}
	totalPages := max(1, (total+eventPageSize-1)/eventPageSize)
	state.next = next
	// An event has no per-row action: it is either already being handled or it
	// has stopped, and the only thing a person can do about the latter is the
	// clear that applies to all of them at once. So the card carries navigation
	// and nothing else.
	navigation := make([]button, 0, 3)
	if len(state.previous) > 0 {
		navigation = append(navigation, button{Text: "‹ 上一页", CallbackData: "e:prev"})
	}
	navigation = append(navigation, button{Text: fmt.Sprintf("第 %d/%d 页", state.page, totalPages), CallbackData: "e:refresh"})
	if state.next != "" {
		navigation = append(navigation, button{Text: "下一页 ›", CallbackData: "e:next"})
	}
	return eventListText(events, total, state.page, totalPages), [][]button{navigation}, nil
}

func eventListText(events []download.ListenerEvent, total, page, totalPages int) string {
	lines := make([]string, 0, len(events)+1)
	lines = append(lines, fmt.Sprintf("<b>监听事件</b> · 共 %d 个 · 第 %d/%d 页", total, page, totalPages))
	for _, event := range events {
		lines = append(lines, eventLine(event))
	}
	return strings.Join(lines, "\n")
}

// eventLine renders one queue row. The dialog name, the emoji and the recorded
// error all originate outside this program, so each is escaped before it goes
// into an HTML message.
func eventLine(event download.ListenerEvent) string {
	kind := "消息"
	if event.Source == "reaction" {
		kind = "反应 " + html.EscapeString(event.Emoji)
	}
	line := fmt.Sprintf("#%d · %s · %s · #%d · %s %d/%d", event.ID, kind, html.EscapeString(short(event.DialogName, 18)), event.MessageID, eventStatusLabel(event.Status), event.Attempts, download.InboxAttemptLimit)
	if event.Status == "pending" {
		if next := eventClock(event.NextAttemptAt); next != "" {
			line += " · 下次 " + next
		}
	}
	if event.Status == "failed" && event.Error != "" {
		// The recorded error is the whole chain that produced it, and the token
		// that says what actually happened is at the end of it. Appending a
		// truncated tail to the row would cut off exactly that token, so the
		// error gets its own line and enough room to be read in full.
		line += "\n  └ " + html.EscapeString(short(event.Error, 120))
	}
	return line
}

// eventStatusLabel deliberately reuses the wording of the /status card: the
// list is the drill-down behind those three numbers, so a person moving between
// them must not have to learn a second vocabulary.
func eventStatusLabel(status string) string {
	switch status {
	case "processing":
		return "处理中"
	case "pending":
		return "等待重试"
	case "failed":
		return "已停止重试"
	case "done":
		return "已完成"
	default:
		return status
	}
}

func eventClock(raw string) string {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return ""
	}
	return parsed.Local().Format("01-02 15:04")
}

func (s *Service) sendEventClearConfirm(cfg settings.Bot, chatID int64) {
	stopped, revivable, err := s.downloads.StoppedEventCounts()
	if err != nil {
		s.send(cfg.Token, chatID, "读取已停止重试的事件失败。", nil)
		return
	}
	if stopped == 0 {
		s.send(cfg.Token, chatID, "当前没有已停止重试的事件。", nil)
		return
	}
	s.send(cfg.Token, chatID, eventClearConfirmText(stopped, revivable), [][]button{{
		{Text: fmt.Sprintf("确认清空 %d 个", stopped), CallbackData: "e:clear:confirm"},
		{Text: "取消", CallbackData: "e:clear:cancel"},
	}})
}

// eventClearConfirmText states what is about to be lost. A failed event below
// the attempt limit is not a dead end - the hourly revive offers it to a worker
// again - so a clear removes work that was still going to be attempted, and
// saying only "12 个" would hide that.
func eventClearConfirmText(stopped, revivable int64) string {
	text := fmt.Sprintf("⚠️ 将永久删除 <b>%d</b> 个已停止重试的事件。", stopped)
	if revivable > 0 {
		text += fmt.Sprintf("\n其中 %d 个尚未用尽尝试次数，本会在下一轮自动重试。", revivable)
	}
	return text
}

func (s *Service) handleEventCallback(cfg settings.Bot, query callbackQuery) {
	if query.Message == nil {
		return
	}
	parts := strings.Split(query.Data, ":")
	if len(parts) == 2 {
		direction := parts[1]
		if direction == "back" {
			direction = "refresh"
		}
		if direction != "prev" && direction != "next" && direction != "refresh" {
			return
		}
		s.editEventList(cfg, query.Message.Chat.ID, query.Message.MessageID, direction)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if len(parts) != 3 || parts[1] != "clear" {
		return
	}
	switch parts[2] {
	case "cancel":
		s.edit(cfg.Token, query.Message.Chat.ID, query.Message.MessageID, "已取消。", nil)
		s.answer(cfg.Token, query.ID, "")
	case "confirm":
		s.confirmEventClear(cfg, query)
	}
}

// confirmEventClear recounts rather than trusting the number the card showed.
// The hourly revive runs on its own schedule, so between the card and the press
// an event can have moved back into the queue; deleting what is actually failed
// now is both simpler than pinning a list and more honest about what happened.
func (s *Service) confirmEventClear(cfg settings.Bot, query callbackQuery) {
	message := query.Message
	messages, reactions, err := s.downloads.ClearStoppedEvents()
	if err != nil {
		// The delete runs in batches, so a failure can land after some of them
		// committed. Reporting the count that did go is the difference between
		// a person re-running the command and a person wondering what happened.
		applog.Error("bot", "event_clear_failed", "user_id", query.From.ID, "messages", messages, "reactions", reactions, "error", err.Error())
		s.answer(cfg.Token, query.ID, "清空失败")
		s.edit(cfg.Token, message.Chat.ID, message.MessageID, fmt.Sprintf("❌ 清空已停止重试的事件失败，已删除 %d 个。", messages+reactions), nil)
		return
	}
	applog.Info("bot", "event_clear_completed", "user_id", query.From.ID, "messages", messages, "reactions", reactions)
	text := fmt.Sprintf("✅ 已清空 %d 个已停止重试的事件（消息 %d · 反应 %d）。", messages+reactions, messages, reactions)
	if remaining, _, countErr := s.downloads.StoppedEventCounts(); countErr == nil && remaining > 0 {
		// Reached only when the queue held more failed events than one call
		// clears, which says so instead of reporting a partial clear as done.
		text += fmt.Sprintf("\n仍有 %d 个，请再次执行 <code>/event_clear</code>。", remaining)
	}
	s.answer(cfg.Token, query.ID, "已清空")
	s.edit(cfg.Token, message.Chat.ID, message.MessageID, text, nil)
}

func (s *Service) handleChatCallback(cfg settings.Bot, query callbackQuery) {
	// Same rule as handleCallback: the query is answered on every path out.
	if query.Message == nil {
		s.answer(cfg.Token, query.ID, "")
		return
	}
	parts := strings.Split(query.Data, ":")
	if len(parts) < 3 {
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if parts[1] == "l" && len(parts) == 3 {
		direction := parts[2]
		if direction != "prev" && direction != "next" && direction != "refresh" && direction != "back" {
			s.answer(cfg.Token, query.ID, "")
			return
		}
		s.untrackChatMessage(query.Message.Chat.ID, query.Message.MessageID)
		if direction == "back" {
			direction = "refresh"
		}
		s.editChatList(cfg, query.Message.Chat.ID, query.Message.MessageID, direction)
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if parts[1] == "v" && len(parts) == 3 {
		s.editChatTask(cfg, query.Message.Chat.ID, query.Message.MessageID, parts[2])
		s.answer(cfg.Token, query.ID, "")
		return
	}
	if parts[1] != "t" || len(parts) != 4 {
		s.answer(cfg.Token, query.ID, "")
		return
	}
	id, action := parts[2], parts[3]
	var err error
	switch action {
	case "pause":
		err = s.downloads.PauseChat(id)
	case "resume":
		err = s.downloads.ResumeChat(id)
	case "retry":
		err = s.downloads.RetryChat(id)
	case "cancel":
		err = s.downloads.CancelChat(id)
	case "delete":
		err = s.downloads.DeleteChat(id)
	case "purge":
		err = s.downloads.PurgeChat(id)
	case "listenon":
		err = s.downloads.SetChatListening(id, true)
	case "listenoff":
		err = s.downloads.SetChatListening(id, false)
	default:
		return
	}
	if err != nil {
		s.answer(cfg.Token, query.ID, err.Error())
		return
	}
	s.answer(cfg.Token, query.ID, "操作成功")
	if action == "delete" || action == "purge" {
		s.untrackChatMessage(query.Message.Chat.ID, query.Message.MessageID)
		message := "会话任务已删除。"
		if action == "purge" {
			message = "会话任务已彻底删除；最终下载文件已保留。"
		}
		back, fromList := s.chatListBack(query.Message.Chat.ID, query.Message.MessageID)
		var buttons [][]button
		if fromList {
			buttons = [][]button{{{Text: back, CallbackData: "c:l:back"}}}
		}
		s.edit(cfg.Token, query.Message.Chat.ID, query.Message.MessageID, message, buttons)
		return
	}
	s.editChatTask(cfg, query.Message.Chat.ID, query.Message.MessageID, id)
}

func (s *Service) editChatTask(cfg settings.Bot, chatID, messageID int64, id string) {
	back, fromList := s.chatListBack(chatID, messageID)
	job, err := s.downloads.GetChat(id)
	if err != nil {
		buttons := [][]button(nil)
		if fromList {
			buttons = [][]button{{{Text: back, CallbackData: "c:l:back"}}}
		}
		s.edit(cfg.Token, chatID, messageID, "会话任务不存在或已删除。", buttons)
		return
	}
	s.edit(cfg.Token, chatID, messageID, chatTaskText(job), chatTaskKeyboard(job, back))
	if active(job.Status) {
		s.trackChat(job.ID, messageRef{ChatID: chatID, MessageID: messageID})
	}
}

func chatTaskText(job download.ChatJob) string {
	lines := []string{fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), statusName(job.Status)), "<b>对话：</b>" + html.EscapeString(short(job.DialogName, 36)), "<b>范围：</b>" + chatRangeLabel(job), fmt.Sprintf("<b>进度：</b>%d/%d", job.Completed, job.Discovered)}
	if job.Failed > 0 {
		lines = append(lines, fmt.Sprintf("<b>失败：</b>%d", job.Failed))
	}
	if job.ActiveFiles > 0 {
		lines = append(lines, fmt.Sprintf("<b>速率：</b>%s/s（%d 个文件）", bytesLabel(int64(job.SpeedBPS)), job.ActiveFiles))
	} else {
		lines = append(lines, "<b>速率：</b>—（0 个文件）")
	}
	if job.ListenNew {
		lines = append(lines, "<b>消息监听：</b>已开启")
	}
	if job.Error != "" {
		lines = append(lines, "<b>说明：</b>"+html.EscapeString(short(job.Error, 100)))
	}
	return strings.Join(lines, "\n")
}

// chatRangeLabel mirrors the Web range display. Before the scanner discovers
// media, "最早" is an intent; afterwards the lowest discovered media ID is
// the meaningful range boundary, including while indexing is still running.
//
// A negative start is the one shape with no history at all - a Saved Messages
// task told to listen and not to scan - and it is described rather than given a
// range, because "最早 — N" would claim a coverage the task deliberately does
// not have. It is read from the range itself and not from the listening flag:
// the same saved task both scans a history and listens once /saved_all has been
// asked for, and the flag says nothing about which range it holds.
func chatRangeLabel(job download.ChatJob) string {
	if job.StartMessageID < 0 {
		return "仅监听新消息"
	}
	if job.StartMessageID > 0 {
		return fmt.Sprintf("%d — %d", job.StartMessageID, job.UpperMessageID)
	}
	if job.EarliestMediaID > 0 {
		return fmt.Sprintf("%d — %d", job.EarliestMediaID, job.UpperMessageID)
	}
	return fmt.Sprintf("最早 — %d", job.UpperMessageID)
}

// chatTaskKeyboard renders a session card's controls. back is the label of the
// return button, empty for a card that was not reached from a list.
func chatTaskKeyboard(job download.ChatJob, back string) [][]button {
	buttons := make([][]button, 0, 3)
	if job.Status == "queued" || job.Status == "scanning" || job.Status == "downloading" || job.Status == "listening" {
		buttons = append(buttons, []button{{Text: "暂停", CallbackData: fmt.Sprintf("c:t:%s:pause", job.ID)}, {Text: "取消", CallbackData: fmt.Sprintf("c:t:%s:cancel", job.ID)}})
	}
	if job.Status == "paused" {
		buttons = append(buttons, []button{{Text: "恢复", CallbackData: fmt.Sprintf("c:t:%s:resume", job.ID)}, {Text: "取消", CallbackData: fmt.Sprintf("c:t:%s:cancel", job.ID)}})
	}
	if job.Status == "failed" || job.Status == "partial" || job.Status == "cancelled" || job.Failed > 0 {
		label := "重新开始"
		if job.Status == "listening" && job.Failed > 0 {
			label = "重试失败项"
		}
		buttons = append(buttons, []button{{Text: label, CallbackData: fmt.Sprintf("c:t:%s:retry", job.ID)}})
	}
	if job.Status == "completed" || job.Status == "failed" || job.Status == "partial" || job.Status == "cancelled" {
		buttons = append(buttons, []button{{Text: "删除", CallbackData: fmt.Sprintf("c:t:%s:delete", job.ID)}})
		buttons = append(buttons, []button{{Text: "彻底删除", CallbackData: fmt.Sprintf("c:t:%s:purge", job.ID)}})
	}
	if job.ListenNew && job.Status != "cancelled" && job.Status != "deleted" {
		buttons = append(buttons, []button{{Text: "停止消息监听", CallbackData: fmt.Sprintf("c:t:%s:listenoff", job.ID)}})
	} else if !job.ListenNew && (job.Status == "completed" || job.Status == "failed" || job.Status == "partial") {
		buttons = append(buttons, []button{{Text: "开启消息监听", CallbackData: fmt.Sprintf("c:t:%s:listenon", job.ID)}})
	}
	if back != "" {
		buttons = append(buttons, []button{{Text: back, CallbackData: "c:l:back"}})
	}
	return buttons
}

func (s *Service) sendTask(cfg settings.Bot, chatID int64, id string) {
	job, err := s.downloads.Get(id)
	if err != nil {
		s.send(cfg.Token, chatID, "未找到该任务。", nil)
		return
	}
	messageID, err := s.send(cfg.Token, chatID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, false))
	if err == nil && active(job.Status) {
		s.track(job.ID, messageRef{ChatID: chatID, MessageID: messageID})
	}
}
func (s *Service) editTask(cfg settings.Bot, chatID, messageID int64, id string, fromList bool) {
	if id == "list" {
		s.sendTaskList(cfg, chatID, "")
		return
	}
	job, err := s.downloads.Get(id)
	if err != nil {
		s.edit(cfg.Token, chatID, messageID, "任务不存在或已删除。", nil)
		return
	}
	s.edit(cfg.Token, chatID, messageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, fromList))
	if active(job.Status) {
		s.track(job.ID, messageRef{ChatID: chatID, MessageID: messageID})
	}
}

func (s *Service) refresh(cfg settings.Bot) {
	ids, tracked := s.refreshIDs()
	s.mu.Lock()
	chatTracked := make(map[string]chatTrackedRef, len(s.chatTracked))
	for key, ref := range s.chatTracked {
		chatTracked[key] = ref
	}
	s.mu.Unlock()
	for _, id := range ids {
		job, err := s.downloads.Get(id)
		if err != nil {
			// A task that is genuinely gone settles here. A read the database
			// could not answer stays pending, because these notifications are
			// mostly one-shot - 'completed' is the last status a task ever has -
			// and dropping the event on a moment's database trouble left the card
			// showing the state before it for good.
			if errors.Is(err, download.ErrJobNotFound) {
				s.consumeDirty(id)
				// The row is gone for good, so a remembered deletion of it has
				// nothing left to suppress. Forgetting it here is what keeps the
				// set from growing with every task the user ever deleted.
				s.unmarkDeleted(id)
			}
			continue
		}
		s.consumeDirty(id)
		if s.isDeleted(job.ID) && job.Status != "deleted" {
			// A deleted task that has a live status again was reactivated by a
			// later submission. It is ordinary work once more, and a deletion
			// remembered from its previous life would suppress its card forever.
			s.unmarkDeleted(job.ID)
		}
		s.mu.Lock()
		previous, exists := s.known[job.ID]
		ready := s.ready
		s.known[job.ID] = job.Status
		s.mu.Unlock()
		s.lifecycleMu.Lock()
		if !s.isDeleted(job.ID) {
			if ready && !exists {
				// Lifecycle cards survive restarts in the database. Reload them before
				// deciding whether a first post-restart event needs a new card.
				if len(s.lifecycleRefs(job.ID)) > 0 {
					s.updateLifecycle(cfg, job)
				} else if cfg.Notifications.TaskCreated || (terminal(job.Status) && shouldUpdateLifecycle(cfg, job.Status)) {
					s.sendLifecycle(cfg, job)
				}
			}
			if exists && previous != job.Status && shouldUpdateLifecycle(cfg, job.Status) {
				// A lifecycle card may have been created by the Web UI, by a Bot
				// link, or by a prior terminal notification. In all three cases the
				// same card is edited in place.
				s.updateLifecycle(cfg, job)
			}
		}
		s.lifecycleMu.Unlock()
	}
	// Only explicitly viewed task cards are refreshed on the cadence. This
	// avoids repeatedly reading the newest task page when there is no event.
	for key, ref := range tracked {
		// The interval is checked before the read, not only before the edit it
		// feeds. Checking it inside editLive meant the read had already
		// happened, so the limit suppressed the Telegram request and nothing
		// else - and since every file transition of a download wakes this loop,
		// the task row was read at the event rate to produce an edit the same
		// limit then declined to send.
		if !s.liveCardDue(ref.key(), time.Now()) {
			continue
		}
		job, err := s.downloads.Get(ref.JobID)
		if err != nil {
			// Untracking is permanent: it is how a card stops being refreshed. A
			// read the database could not answer must not end it.
			if errors.Is(err, download.ErrJobNotFound) {
				s.untrack(key)
			}
			continue
		}
		if s.editLive(cfg.Token, ref.ChatID, ref.MessageID, taskText(job, s.downloads.LiveProgress()), taskKeyboard(job, s.hasListPage("task", ref.ChatID, ref.MessageID))) {
			s.untrack(key)
		}
		if !active(job.Status) {
			s.untrack(key)
		}
	}
	// A viewed chat card is refreshed at the same restrained cadence as an
	// ordinary task card. It is not a lifecycle notification, so it stops once
	// the history has settled and no new-media listener remains active.
	for key, ref := range chatTracked {
		if !s.liveCardDue(ref.key(), time.Now()) {
			continue
		}
		job, err := s.downloads.GetChat(ref.JobID)
		if err != nil {
			if errors.Is(err, download.ErrChatJobNotFound) {
				s.untrackChat(key)
			}
			continue
		}
		back, _ := s.chatListBack(ref.ChatID, ref.MessageID)
		if s.editLive(cfg.Token, ref.ChatID, ref.MessageID, chatTaskText(job), chatTaskKeyboard(job, back)) {
			s.untrackChat(key)
			continue
		}
		if !active(job.Status) {
			s.untrackChat(key)
		}
	}
}

// refreshIDs returns the tasks with a pending notification and the viewed cards,
// without retiring either. An id leaves the pending set in consumeDirty, which
// the caller reaches only after the task has actually been read: clearing the
// set here made the read that follows the last chance to observe the change, and
// a read that failed threw the notification away.
func (s *Service) refreshIDs() ([]string, map[string]trackedRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
	tracked := make(map[string]trackedRef, len(s.tracked))
	for key, ref := range s.tracked {
		tracked[key] = ref
	}
	return ids, tracked
}

// consumeDirty retires one task from the pending-notification set.
func (s *Service) consumeDirty(id string) {
	s.mu.Lock()
	delete(s.dirty, id)
	s.mu.Unlock()
}

func (s *Service) track(id string, ref messageRef) {
	s.mu.Lock()
	s.tracked[id+":"+fmt.Sprint(ref.ChatID)] = trackedRef{JobID: id, messageRef: ref}
	s.mu.Unlock()
}
func (s *Service) untrack(key string) {
	s.mu.Lock()
	if ref, ok := s.tracked[key]; ok {
		delete(s.liveRendered, ref.key())
		delete(s.lastLiveEdit, ref.key())
	}
	delete(s.tracked, key)
	s.mu.Unlock()
}

func listPageKey(kind string, chatID, messageID int64) string {
	return kind + ":" + fmt.Sprint(chatID) + ":" + fmt.Sprint(messageID)
}

func (s *Service) getListPage(kind string, chatID, messageID int64) (listPageState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.pruneListPagesLocked(now)
	key := listPageKey(kind, chatID, messageID)
	state, ok := s.listPages[key]
	if ok {
		state.updated = now
		s.listPages[key] = state
	}
	return state, ok
}

func (s *Service) hasListPage(kind string, chatID, messageID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneListPagesLocked(time.Now())
	_, ok := s.listPages[listPageKey(kind, chatID, messageID)]
	return ok
}

func (s *Service) putListPage(kind string, chatID, messageID int64, state listPageState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.pruneListPagesLocked(now)
	if len(state.previous) > maxListPageHistory {
		state.previous = append([]string(nil), state.previous[len(state.previous)-maxListPageHistory:]...)
	}
	state.updated = now
	key := listPageKey(kind, chatID, messageID)
	s.listPages[key] = state
	for len(s.listPages) > maxListPageEntries {
		oldestKey := ""
		var oldest time.Time
		for candidate, current := range s.listPages {
			if oldestKey == "" || current.updated.Before(oldest) {
				oldestKey, oldest = candidate, current.updated
			}
		}
		delete(s.listPages, oldestKey)
	}
}

func (s *Service) pruneListPages(now time.Time) {
	s.mu.Lock()
	s.pruneListPagesLocked(now)
	s.mu.Unlock()
}

func (s *Service) pruneListPagesLocked(now time.Time) {
	for key, state := range s.listPages {
		if state.updated.IsZero() || !now.Before(state.updated.Add(listPageTTL)) {
			delete(s.listPages, key)
		}
	}
}

func moveListPage(state listPageState, direction string) (listPageState, bool) {
	switch direction {
	case "next":
		if state.next == "" {
			return state, false
		}
		state.previous = append(state.previous, state.cursor)
		state.cursor = state.next
		state.next = ""
		state.page++
		return state, true
	case "prev":
		if len(state.previous) == 0 {
			return state, false
		}
		state.cursor = state.previous[len(state.previous)-1]
		state.previous = state.previous[:len(state.previous)-1]
		state.next = ""
		if state.page > 1 {
			state.page--
		}
		return state, true
	default:
		return state, false
	}
}
func (s *Service) untrackJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, ref := range s.tracked {
		if ref.JobID == jobID {
			delete(s.liveRendered, ref.key())
			delete(s.lastLiveEdit, ref.key())
			delete(s.tracked, key)
		}
	}
}
func (s *Service) untrackMessage(chatID, messageID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, ref := range s.tracked {
		if ref.ChatID == chatID && ref.MessageID == messageID {
			delete(s.liveRendered, ref.key())
			delete(s.lastLiveEdit, ref.key())
			delete(s.tracked, key)
		}
	}
}

func (s *Service) trackChat(id string, ref messageRef) {
	s.mu.Lock()
	s.chatTracked[id+":"+fmt.Sprint(ref.ChatID)] = chatTrackedRef{JobID: id, messageRef: ref}
	s.mu.Unlock()
}
func (s *Service) untrackChat(key string) {
	s.mu.Lock()
	if ref, ok := s.chatTracked[key]; ok {
		delete(s.liveRendered, ref.key())
		delete(s.lastLiveEdit, ref.key())
	}
	delete(s.chatTracked, key)
	s.mu.Unlock()
}
func (s *Service) untrackChatMessage(chatID, messageID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, ref := range s.chatTracked {
		if ref.ChatID == chatID && ref.MessageID == messageID {
			delete(s.liveRendered, ref.key())
			delete(s.lastLiveEdit, ref.key())
			delete(s.chatTracked, key)
		}
	}
}

func (s *Service) rememberLifecycle(jobID string, ref messageRef) {
	if ref.TokenHash == "" {
		s.mu.Lock()
		ref.TokenHash = tokenFingerprint(s.token)
		s.mu.Unlock()
	}
	s.mu.Lock()
	if s.lifecycle[jobID] == nil {
		s.lifecycle[jobID] = make(map[int64]messageRef)
	}
	s.lifecycle[jobID][ref.ChatID] = ref
	s.mu.Unlock()
	// The database write happens outside the lock. s.mu is the one lock every
	// other part of the Bot takes - the event watcher, the live refresh, all the
	// trackers - so a synchronous upsert held inside it turned a moment of
	// database trouble into a stall of the whole notification path. Its
	// counterpart forgetLifecycleMessage has always written after unlocking.
	if err := s.downloads.SaveBotLifecycleMessage(jobID, download.BotMessageRef{ChatID: ref.ChatID, MessageID: ref.MessageID, Text: ref.Text, TokenHash: ref.TokenHash}); err != nil {
		applog.Error("bot", "lifecycle_message_save_failed", "job_id", jobID, "chat_id", ref.ChatID, "error", err.Error())
	}
}

func (s *Service) lifecycleRefs(jobID string) []messageRef {
	persisted, err := s.downloads.BotLifecycleMessages(jobID)
	if err != nil {
		applog.Error("bot", "lifecycle_message_load_failed", "job_id", jobID, "error", err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycle[jobID] == nil {
		s.lifecycle[jobID] = make(map[int64]messageRef)
	}
	for _, ref := range persisted {
		s.lifecycle[jobID][ref.ChatID] = messageRef{ChatID: ref.ChatID, MessageID: ref.MessageID, Text: ref.Text, TokenHash: ref.TokenHash}
	}
	refs := s.lifecycle[jobID]
	result := make([]messageRef, 0, len(refs))
	for _, ref := range refs {
		result = append(result, ref)
	}
	return result
}

func (s *Service) forgetLifecycle(jobID string) {
	s.mu.Lock()
	delete(s.lifecycle, jobID)
	s.mu.Unlock()
}

func (s *Service) isLifecycleMessage(jobID string, chatID, messageID int64) bool {
	for _, ref := range s.lifecycleRefs(jobID) {
		if ref.ChatID == chatID && ref.MessageID == messageID {
			return true
		}
	}
	return false
}

func (s *Service) markLifecycleDeleted(cfg settings.Bot, refs []messageRef, skipChatID, skipMessageID int64) {
	tokenHash := tokenFingerprint(cfg.Token)
	for _, ref := range refs {
		if !allowed(cfg, ref.ChatID) || ref.TokenHash != tokenHash || (ref.ChatID == skipChatID && ref.MessageID == skipMessageID) {
			continue
		}
		s.edit(cfg.Token, ref.ChatID, ref.MessageID, deletedTaskText(ref.Text), nil)
	}
}

// deletedRecordTTL bounds how long a deleted task is remembered.
//
// The record exists to stop a refresh that was already in flight when the user
// confirmed the deletion from re-rendering the card a moment later - a window of
// seconds. Keeping it forever grew the set with every task ever deleted, and,
// because a deleted task's row survives as 'deleted' rather than being removed,
// it also kept a task silenced after a later submission reactivated it.
const deletedRecordTTL = 5 * time.Minute

func (s *Service) markDeleted(jobID string) {
	s.mu.Lock()
	s.deleted[jobID] = time.Now()
	s.mu.Unlock()
}

func (s *Service) unmarkDeleted(jobID string) {
	s.mu.Lock()
	delete(s.deleted, jobID)
	s.mu.Unlock()
}

func (s *Service) isDeleted(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, exists := s.deleted[jobID]
	if !exists {
		return false
	}
	if time.Since(at) > deletedRecordTTL {
		delete(s.deleted, jobID)
		return false
	}
	return true
}

func (s *Service) sendLifecycle(cfg settings.Bot, job download.Job) {
	text := lifecycleText(job)
	for _, id := range cfg.ControlUserIDs {
		messageID, err := s.send(cfg.Token, id, text, notificationKeyboard(job.ID))
		if err != nil {
			applog.Error("bot", "lifecycle_message_send_failed", "job_id", job.ID, "chat_id", id, "error", redactBotError(cfg.Token, err.Error()))
			continue
		}
		s.rememberLifecycle(job.ID, messageRef{ChatID: id, MessageID: messageID, Text: text})
	}
}

func (s *Service) updateLifecycle(cfg settings.Bot, job download.Job) {
	refs := s.lifecycleRefs(job.ID)
	activeRefs := make([]messageRef, 0, len(refs))
	tokenHash := tokenFingerprint(cfg.Token)
	for _, ref := range refs {
		if ref.TokenHash == tokenHash && allowed(cfg, ref.ChatID) {
			activeRefs = append(activeRefs, ref)
			continue
		}
		// A removed controller must stop receiving task metadata immediately.
		s.forgetLifecycleMessage(job.ID, ref.ChatID)
	}
	refs = activeRefs
	if len(refs) == 0 {
		s.sendLifecycle(cfg, job)
		return
	}
	text := lifecycleText(job)
	for _, ref := range refs {
		if missing, _ := s.edit(cfg.Token, ref.ChatID, ref.MessageID, text, notificationKeyboard(job.ID)); missing {
			s.forgetLifecycleMessage(job.ID, ref.ChatID)
			continue
		}
		ref.Text = text
		s.rememberLifecycle(job.ID, ref)
	}
}

func (s *Service) forgetLifecycleMessage(jobID string, chatID int64) {
	s.mu.Lock()
	if refs := s.lifecycle[jobID]; refs != nil {
		delete(refs, chatID)
	}
	s.mu.Unlock()
	if err := s.downloads.RemoveBotLifecycleMessage(jobID, chatID); err != nil {
		applog.Error("bot", "lifecycle_message_remove_failed", "job_id", jobID, "chat_id", chatID, "error", err.Error())
	}
}

func shouldUpdateLifecycle(cfg settings.Bot, status string) bool {
	switch status {
	case "completed":
		return cfg.Notifications.TaskCompleted
	case "partial":
		return cfg.Notifications.TaskPartial
	case "failed":
		return cfg.Notifications.TaskFailed
	default:
		// There is no separate switch for queue/running/paused transitions;
		// keep the existing lifecycle card current for these intermediate states.
		return true
	}
}

func terminal(status string) bool {
	switch status {
	case "completed", "partial", "failed", "cancelled":
		return true
	default:
		return false
	}
}
func allowed(cfg settings.Bot, userID int64) bool {
	for _, id := range cfg.ControlUserIDs {
		if id == userID {
			return true
		}
	}
	return false
}

type button struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

func notificationKeyboard(jobID string) [][]button {
	return [][]button{{{Text: "查看详情", CallbackData: "t:" + jobID + ":view"}}}
}

func lifecycleText(job download.Job) string {
	lines := []string{
		fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), lifecycleTitle(job.Status)),
		fmt.Sprintf("<b>进度：</b>%d/%d 文件", job.CompletedItems, job.TotalItems),
		"<b>对话：</b>" + html.EscapeString(job.DialogName),
		"<b>消息：</b>" + html.EscapeString(messagePreview(job.MessageText)),
		sourceLine(job),
	}
	if job.Error != "" {
		lines = append(lines, "说明："+html.EscapeString(short(job.Error, 180)))
	}
	return strings.Join(lines, "\n")
}

func lifecycleTitle(status string) string {
	return map[string]string{
		"queued":    "下载任务已创建",
		"running":   "下载进行中",
		"paused":    "下载已暂停",
		"completed": "下载完成",
		"partial":   "部分完成",
		"failed":    "下载失败",
		"cancelled": "下载已取消",
	}[status]
}

func deletedTaskText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "🗑 <b>下载任务（任务已删除）</b>"
	}
	lines := strings.Split(text, "\n")
	if strings.Contains(lines[0], "</b>") {
		lines[0] = strings.Replace(lines[0], "</b>", "（任务已删除）</b>", 1)
	} else {
		lines[0] += "（任务已删除）"
	}
	return strings.Join(lines, "\n")
}

func deletedTaskKeyboard(fromList bool) [][]button {
	if !fromList {
		return nil
	}
	return [][]button{{{Text: "‹ 返回任务列表", CallbackData: "l:back"}}}
}

func taskKeyboard(job download.Job, fromList bool) [][]button {
	id := job.ID
	callback := func(action string) string {
		return "t:" + id + ":" + action
	}
	var rows [][]button
	switch job.Status {
	case "queued", "running":
		rows = append(rows, []button{{"⏸ 暂停", callback("pause")}, {"■ 取消", callback("cancel")}})
	case "paused":
		rows = append(rows, []button{{"▶ 继续", callback("resume")}, {"■ 取消", callback("cancel")}})
	case "failed", "partial":
		rows = append(rows, []button{{"↻ 重试", callback("retry")}, {"删除", callback("delete")}})
	case "cancelled":
		rows = append(rows, []button{{"↻ 重新开始", callback("retry")}, {"删除", callback("delete")}})
	case "completed":
		rows = append(rows, []button{{"删除", callback("delete")}})
	}
	rows = append(rows, []button{{"↻ 刷新", callback("view")}})
	if fromList {
		rows = append(rows, []button{{"‹ 返回任务列表", "l:back"}})
	}
	return rows
}

func taskStatusFilter(filters []string) string {
	if len(filters) == 0 {
		return ""
	}
	return filters[0]
}

func taskText(job download.Job, progress []download.FileProgress) string {
	byID := map[string]download.FileProgress{}
	for _, p := range progress {
		byID[fmt.Sprintf("%s:%d", p.DialogKey, p.MessageID)] = p
	}
	lines := []string{fmt.Sprintf("%s <b>%s</b>", statusIcon(job.Status), statusName(job.Status)), fmt.Sprintf("<b>进度：</b>%d/%d 文件", job.CompletedItems, job.TotalItems)}
	// The card lists a bounded prefix of the files. A task can hold one row per
	// media comment, and Telegram rejects a message longer than 4096 characters,
	// so rendering every one of them produced a card that could not be sent at
	// all - and the counts above already answer "how much is left", which is what
	// the list is for.
	items := job.Items
	if len(items) > botCardItemLines {
		items = items[:botCardItemLines]
	}
	for _, item := range items {
		p := byID[fmt.Sprintf("%s:%d", item.DialogKey, item.MessageID)]
		percent := "—"
		speed := ""
		if item.Status == "completed" {
			percent = "100%"
		} else if p.Total > 0 {
			percent = fmt.Sprintf("%.0f%%", float64(p.Downloaded)*100/float64(p.Total))
			if p.SpeedBPS > 0 {
				speed = " · " + bytesLabel(int64(p.SpeedBPS)) + "/s"
			}
		}
		origin := ""
		if item.IsComment {
			origin = " · 评论/回复"
		}
		lines = append(lines, fmt.Sprintf("%s %s  %s%s%s", statusIcon(item.Status), html.EscapeString(short(item.OriginalName, 26)), percent, speed, origin))
	}
	// Counted from the task's own total rather than from the rows this read
	// returned. A task's files are unbounded, so a read carries one bounded
	// page of them - fifty at the time of writing - and subtracting the rendered
	// lines from that page's length told a task with 480 files that thirty were
	// not listed.
	remaining := job.TotalItems - len(items)
	if remaining < 0 {
		remaining = len(job.Items) - len(items)
	}
	if remaining > 0 {
		lines = append(lines, fmt.Sprintf("<i>另有 %d 个文件未在此列出，可在管理台查看完整列表。</i>", remaining))
	}
	lines = append(lines,
		"<b>对话：</b>"+html.EscapeString(short(job.DialogName, 36)),
		"<b>消息：</b>"+html.EscapeString(short(messageFullText(job.MessageText), botCardMessageText)),
		sourceLine(job),
	)
	if job.Error != "" {
		lines = append(lines, "<b>说明：</b>"+html.EscapeString(short(job.Error, 100)))
	}
	text := strings.Join(lines, "\n")
	if len([]rune(text)) <= botCardLimit {
		return text
	}
	// Unreachable while every field above is bounded, which is the point: it
	// exists so that a field added later without a bound costs a shorter card
	// rather than every card. Whole lines are dropped instead of being cut
	// mid-string because each line is already HTML-escaped - cutting inside an
	// escape sequence produces markup Telegram rejects, turning a card that is
	// merely too long into no card at all. The first line is always kept, so
	// the result still says what the task is.
	for index := len(lines) - 1; index > 1; index-- {
		candidate := strings.Join(lines[:index], "\n")
		if len([]rune(candidate)) <= botCardLimit {
			return candidate
		}
	}
	return lines[0]
}

// sourceLine only emits externally usable Telegram links. The tg://reaction/*
// and tg://message/* values stored for internal retry bookkeeping must never
// be presented as Bot links. The rule behind it lives in the download package
// because the Web UI asks the same question of the same task, and two copies of
// it had already drifted into two different answers.
func sourceLine(job download.Job) string {
	if target := download.MessageJumpURL(job); target != "" {
		return `<b>来源：</b><a href="` + html.EscapeString(target) + `">点击查看</a>`
	}
	return "<b>来源：</b>" + sourceTypeName(job) + "不支持跳转"
}

func sourceTypeName(job download.Job) string {
	switch job.DialogType {
	case "self":
		return "收藏消息"
	case "user":
		return "私聊"
	case "bot":
		return "Bot"
	case "chat":
		return "普通群组"
	case "channel":
		return "频道或超级群"
	default:
		return "该会话类型"
	}
}

func messagePreview(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "—"
	}
	runes := []rune(value)
	if len(runes) <= 15 {
		return value
	}
	return string(runes[:15]) + "…"
}

func messageFullText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "—"
	}
	return value
}

func helpText() string {
	return fmt.Sprintf("<b>TDL帮助</b>\n版本：TDL 管理 %s · 上游 TDL %s\n\n发送 Telegram 消息链接或转发消息即可创建消息下载任务。\n\n<code>/help</code> 获取帮助信息\n<code>/status</code> 获取TDL当前状态\n<code>/config</code> 获取TDL当前配置\n<code>/restart</code> 重启TDL所有服务\n<code>/tasks</code> 获取所有消息下载任务\n<code>/task_filter</code> 筛选获取消息下载任务\n<code>/chats [链接]</code> 获取/创建会话类型下载\n<code>/saved_task</code> 获取收藏夹任务\n<code>/saved_all</code> 下载收藏夹历史消息\n<code>/saved_listen</code> 开始/停止监听收藏夹新消息\n<code>/events</code> 获取监听的正在处理事件\n<code>/event_clear</code> 清空已停止重试的事件", buildinfo.Version, upstream.Version)
}

// accountsStatusText renders one line per account record.
//
// The Bot's /status used to name only the selected account - and only when it
// was authorized - so someone running several sessions saw a single line and
// read it as the whole truth. Every account is listed instead, with the
// selected one marked, because the update listeners and the download queue run
// on all of them and an account that went stale is information the person
// reading this cannot get anywhere else in the Bot.
//
// Authorized accounts carry no state word: it is the ordinary case, and a word
// repeated on every line is the one the eye stops reading. The marks that carry
// meaning are the ones that differ from their neighbours.
func accountsStatusText(accounts []telegram.Account, currentID string) string {
	if len(accounts) == 0 {
		return "登录账号：未登录"
	}
	lines := make([]string, 0, len(accounts)+1)
	lines = append(lines, fmt.Sprintf("登录账号（%d）：", len(accounts)))
	for _, account := range accounts {
		line := "· " + accountDisplayName(account)
		if account.State != "authorized" {
			line += " · " + accountStateLabel(account.State)
		}
		if account.ID == currentID {
			line += " · 当前账户"
		}
		lines = append(lines, line)
	}
	return html.EscapeString(strings.Join(lines, "\n"))
}

// accountDisplayName is what a person calls this session. An account still
// being logged in has neither a name nor a Telegram id yet, so it is named by
// its state rather than by a placeholder number that means "unknown".
func accountDisplayName(account telegram.Account) string {
	name := strings.TrimSpace(account.FirstName + " " + account.LastName)
	if name == "" {
		if account.TelegramID != 0 {
			name = "Telegram 用户 " + fmt.Sprint(account.TelegramID)
		} else {
			name = "未命名账户"
		}
	}
	if account.Username != "" {
		name += " (@" + account.Username + ")"
	}
	return name
}

// accountStateLabel uses the same wording as the Web accounts view, so the two
// surfaces do not describe one session in two vocabularies.
func accountStateLabel(state string) string {
	switch state {
	case "authorized":
		return "已登录"
	case "starting":
		return "正在准备"
	case "waiting_for_qr":
		return "等待扫码"
	case "waiting_for_2fa":
		return "等待两步验证"
	case "expired":
		return "会话失效"
	case "error":
		return "登录失败"
	case "stopped":
		return "已停止"
	default:
		return state
	}
}

func (s *Service) statusText() string {
	accounts := "登录账号：未登录"
	if s.telegram != nil {
		list, currentID := s.telegram.List()
		accounts = accountsStatusText(list, currentID)
	}
	var cpu, memory, receive, transmit float64
	if s.monitor != nil {
		sample, _ := s.monitor.Snapshot()
		cpu = sample.CPUPercent
		if sample.MemoryTotal > 0 {
			memory = float64(sample.MemoryUsed) * 100 / float64(sample.MemoryTotal)
		}
		receive, transmit = sample.ReceiveBPS, sample.TransmitBPS
	}
	// Both figures are counted when this command is sent and never polled. They
	// are left out entirely, rather than shown as zero, when the database cannot
	// answer: a person reading "0 个下载中" would otherwise be told the opposite
	// of what is happening.
	counts := "下载中文件：暂时无法读取\n最近 30 天失败：暂时无法读取\n监听事件：暂时无法读取"
	if s.downloads != nil {
		if active, recentFailures, err := s.downloads.DownloadCounts(); err == nil {
			counts = fmt.Sprintf("下载中文件：%d\n最近 30 天失败：%d", active, recentFailures)
		}
		// An event that stopped being retried is a download the user asked for
		// and did not get, so it is reported next to the download counts rather
		// than only appearing in the service log.
		// Work in progress and work waiting to be retried are reported apart: an
		// event that failed and is backing off can sit for an hour between
		// attempts, and counting it as "处理中" hid a stuck event behind what
		// looked like activity.
		if processing, waiting, stopped, err := s.downloads.ListenerInboxCounts(); err == nil {
			counts += fmt.Sprintf("\n监听事件：%d 处理中 · %d 等待重试", processing, waiting)
			if stopped > 0 {
				counts += fmt.Sprintf(" · %d 已停止重试", stopped)
			}
		}
	}
	return fmt.Sprintf("<b>当前状态</b>\n版本：TDL 管理 %s · 上游 TDL %s\n%s\n%s\nCPU：%.1f%%\n内存：%.1f%%\n网络：↓ %s/s · ↑ %s/s", buildinfo.Version, upstream.Version, accounts, counts, cpu, memory, bytesLabel(int64(receive)), bytesLabel(int64(transmit)))
}

func configText(cfg settings.Values) string {
	proxy := "直连"
	if cfg.ProxyURL != "" {
		proxy = safeProxyLabel(cfg.ProxyURL)
	}
	botState := "已关闭"
	if cfg.Bot.Enabled {
		botState = fmt.Sprintf("已启用（%d 位用户）", len(cfg.Bot.ControlUserIDs))
	}
	reactionState := "已关闭"
	if cfg.Reaction.Enabled {
		reactionState = "已启用"
	}
	replies := "已关闭"
	if cfg.Download.IncludeReplies {
		replies = "已开启"
	}
	return fmt.Sprintf("<b>当前配置</b>\n代理：%s\n下载：线程 %d · 单任务文件并发 %d · 任务并发 %d · 连接池 %d · 间隔 %dms\n文件筛选：%s\n关联评论/回复：%s\nBot：%s\n表情监听：%s（%s）\n最终命名模板：<code>%s</code>", html.EscapeString(proxy), cfg.Download.Threads, cfg.Download.TaskLimit, cfg.Download.ConcurrentJobs, cfg.Download.PoolSize, cfg.Download.DelayMS, html.EscapeString(downloadFilterText(cfg.Download)), replies, botState, reactionState, html.EscapeString(reactionEmojiText(cfg.Reaction.Emojis)), html.EscapeString(short(cfg.Download.FinalFilenameTemplate, 180)))
}

func reactionEmojiText(emojis []string) string {
	values := make([]string, 0, len(emojis))
	for _, emoji := range emojis {
		values = append(values, settings.DisplayReactionEmoji(emoji))
	}
	return strings.Join(values, " ")
}

func downloadFilterText(cfg settings.Download) string {
	parts := make([]string, 0, 3)
	if cfg.MinFileSizeMB > 0 {
		parts = append(parts, fmt.Sprintf("≥ %d MB", cfg.MinFileSizeMB))
	}
	if cfg.MaxFileSizeMB > 0 {
		parts = append(parts, fmt.Sprintf("≤ %d MB", cfg.MaxFileSizeMB))
	}
	if len(cfg.FileTypes) > 0 {
		names := map[string]string{"image": "图片", "video": "视频", "gif": "GIF", "music": "音乐", "voice": "语音", "sticker": "贴纸", "document": "文档"}
		labels := make([]string, 0, len(cfg.FileTypes))
		for _, kind := range cfg.FileTypes {
			if label, ok := names[kind]; ok {
				labels = append(labels, label)
			}
		}
		parts = append(parts, strings.Join(labels, "、"))
	} else {
		parts = append(parts, "不下载任何类型")
	}
	if len(parts) == 0 {
		return "不限"
	}
	return strings.Join(parts, "；")
}

func safeProxyLabel(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "已配置"
	}
	return u.Scheme + "://" + u.Host
}

// redactBotError keeps a transport error useful while preventing an HTTP URL
// from exposing the Bot API token in Docker logs or diagnostics. Telegram
// clients include the full request URL in errors such as connection timeouts.
func redactBotError(token, message string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return message
	}
	return strings.ReplaceAll(message, token, "[REDACTED]")
}
func isTelegramLink(v string) bool {
	u, err := url.Parse(v)
	return err == nil && (u.Host == "t.me" || strings.HasSuffix(u.Host, ".t.me"))
}
func active(status string) bool {
	return status == "queued" || status == "running" || status == "scanning" || status == "downloading" || status == "listening"
}
func statusName(status string) string {
	return map[string]string{"queued": "排队中", "waiting": "等待中", "running": "下载中", "downloaded": "下载完成", "scanning": "索引中", "downloading": "下载中", "listening": "监听中", "paused": "已暂停", "completed": "已完成", "partial": "部分完成", "failed": "失败", "cancelled": "已取消", "deleted": "已删除"}[status]
}
func statusIcon(status string) string {
	return map[string]string{"queued": "🕓", "waiting": "🕓", "running": "⬇️", "downloaded": "📦", "scanning": "🔎", "downloading": "⬇️", "listening": "👂", "paused": "⏸", "completed": "✅", "partial": "⚠️", "failed": "❌", "cancelled": "■", "deleted": "🗑"}[status]
}
func short(v string, limit int) string {
	r := []rune(v)
	if len(r) <= limit {
		return v
	}
	return string(r[:limit-1]) + "…"
}
func bytesLabel(value int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	n := float64(value)
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", value)
	}
	return fmt.Sprintf("%.2f %s", n, units[i])
}

func (s *Service) call(token, method string, body any, out any) error {
	return s.callWithTimeout(token, method, body, out, botRequestTimeout)
}

func (s *Service) callWithTimeout(token, method string, body any, out any, timeout time.Duration) error {
	if method != "getUpdates" {
		if err := s.awaitOutbound(s.ctx); err != nil {
			return err
		}
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.telegram.org/bot"+token+"/"+method, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			s.noteOutboundWait(time.Duration(seconds) * time.Second)
		}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return &botAPIError{method: method, status: resp.StatusCode}
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var probe struct {
		OK         bool `json:"ok"`
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return err
	}
	if !probe.OK && probe.Parameters.RetryAfter > 0 {
		s.noteOutboundWait(time.Duration(probe.Parameters.RetryAfter) * time.Second)
	}
	return json.Unmarshal(payload, out)
}

// awaitOutbound serializes Bot API writes at a conservative eight requests
// per second. Long polling is deliberately excluded: it is a held request,
// not an outbound notification burst.
func (s *Service) awaitOutbound(ctx context.Context) error {
	for {
		now := time.Now()
		s.outboundMu.Lock()
		ready := s.nextOutbound
		if s.outboundBlockedUntil.After(ready) {
			ready = s.outboundBlockedUntil
		}
		if !ready.After(now) {
			s.nextOutbound = now.Add(125 * time.Millisecond)
			s.outboundMu.Unlock()
			return nil
		}
		s.outboundMu.Unlock()
		timer := time.NewTimer(time.Until(ready))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Service) noteOutboundWait(wait time.Duration) {
	if wait <= 0 {
		return
	}
	s.outboundMu.Lock()
	until := time.Now().Add(wait + 250*time.Millisecond)
	if until.After(s.outboundBlockedUntil) {
		s.outboundBlockedUntil = until
	}
	s.outboundMu.Unlock()
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

func (s *Service) httpClient() *http.Client {
	raw := strings.TrimSpace(s.settings.ProxyURL())
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if s.client != nil && s.clientProxy == raw {
		return s.client
	}
	if s.client != nil {
		if transport, ok := s.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Transport: transport}
	s.clientProxy = raw
	s.client = client
	if raw == "" {
		return client
	}
	u, err := url.Parse(raw)
	if err != nil {
		return client
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		transport.Proxy = http.ProxyURL(u)
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if u.User != nil {
			password, _ := u.User.Password()
			auth = &proxy.Auth{User: u.User.Username(), Password: password}
		}
		dialer, err := proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
		if err == nil {
			transport.Proxy = nil
			transport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
				return dialer.Dial(network, address)
			}
		}
	}
	return client
}

func normalizeCommand(text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 || !strings.HasPrefix(fields[0], "/") {
		return text
	}
	// The @BotName suffix belongs to the command token whatever follows it.
	// Stripping it only for a single-token message meant every command that takes
	// an argument - "/chats@MyBot <链接>", "/saved_all@MyBot" - was rejected as
	// an unknown command.
	if at := strings.IndexByte(fields[0], '@'); at > 1 {
		fields[0] = fields[0][:at]
	}
	return strings.Join(fields, " ")
}

// liveCardInterval is how often a viewed card may be re-read and re-edited.
// The same value gates both, because gating only the edit meant the read
// happened anyway.
const liveCardInterval = 3 * time.Second

// liveCardDue reports whether a viewed card's interval has elapsed, checked
// before the read that renders it. It deliberately does not replace the check
// inside editLive: that one is still the authority on whether an edit is sent,
// and this one only decides whether the row is worth reading to find out.
func (s *Service) liveCardDue(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return now.Sub(s.lastLiveEdit[key]) >= liveCardInterval
}

func (s *Service) editLive(token string, chatID, messageID int64, text string, keyboard [][]button) bool {
	key := fmt.Sprintf("%d:%d", chatID, messageID)
	encoded, _ := json.Marshal(struct {
		Text     string     `json:"text"`
		Keyboard [][]button `json:"keyboard"`
	}{Text: text, Keyboard: keyboard})
	rendered := string(encoded)
	now := time.Now()
	s.mu.Lock()
	if s.liveRendered[key] == rendered {
		s.mu.Unlock()
		return false
	}
	if now.Sub(s.lastLiveEdit[key]) < liveCardInterval || now.Before(s.nextLiveEdit) {
		s.mu.Unlock()
		return false
	}
	s.lastLiveEdit[key] = now
	s.nextLiveEdit = now.Add(50 * time.Millisecond)
	s.mu.Unlock()
	missing, updated := s.edit(token, chatID, messageID, text, keyboard)
	if updated {
		s.mu.Lock()
		s.liveRendered[key] = rendered
		s.mu.Unlock()
	}
	return missing
}

// botCommands is the command menu Telegram shows above the input field. It is
// the same list the help message prints, because a command offered by the menu
// and missing from the help - or the other way round - is a control that exists
// for reasons the person reading /help cannot see.
func botCommands() []map[string]string {
	return []map[string]string{
		{"command": "help", "description": "获取帮助信息"},
		{"command": "status", "description": "获取TDL当前状态"},
		{"command": "config", "description": "获取TDL当前配置"},
		{"command": "restart", "description": "重启TDL所有服务"},
		{"command": "tasks", "description": "获取所有消息下载任务"},
		{"command": "task_filter", "description": "筛选获取消息下载任务"},
		{"command": "chats", "description": "获取/创建会话类型下载"},
		{"command": "saved_task", "description": "获取收藏夹任务"},
		{"command": "saved_all", "description": "下载收藏夹历史消息"},
		{"command": "saved_listen", "description": "开始/停止监听收藏夹新消息"},
		{"command": "events", "description": "获取监听的正在处理事件"},
		{"command": "event_clear", "description": "清空已停止重试的事件"},
	}
}

// configureCommands enables Telegram's native slash-command suggestions and
// the command menu button shown at the leading edge of the chat input.
func (s *Service) configureCommands(token string) {
	var result apiResponse[bool]
	commands := botCommands()
	if err := s.call(token, "setMyCommands", map[string]any{"commands": commands}, &result); err != nil {
		applog.Error("bot", "commands_configure_failed", "error", redactBotError(token, err.Error()))
		return
	}
	if !result.OK {
		applog.Error("bot", "commands_configure_rejected", "error", redactBotError(token, result.Description))
		return
	}
	if err := s.call(token, "setChatMenuButton", map[string]any{"menu_button": map[string]string{"type": "commands"}}, &result); err != nil {
		applog.Error("bot", "command_menu_configure_failed", "error", redactBotError(token, err.Error()))
		return
	}
	if !result.OK {
		applog.Error("bot", "command_menu_configure_rejected", "error", redactBotError(token, result.Description))
	}
}
func (s *Service) send(token string, chatID int64, text string, keyboard [][]button) (int64, error) {
	var result apiResponse[message]
	payload := map[string]any{"chat_id": chatID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true}
	if len(keyboard) > 0 {
		payload["reply_markup"] = map[string]any{"inline_keyboard": keyboard}
	}
	err := s.call(token, "sendMessage", payload, &result)
	if err != nil {
		// Every send is logged, including the ones whose result the caller
		// ignores. Without this a revoked token or a user who blocked the Bot made
		// every command fail silently: the caller discarded the error and
		// callWithTimeout, which is where the transport error surfaces, does not
		// log. The edit and command-registration paths have always logged; this
		// one did not.
		applog.Error("bot", "message_send_failed", "chat_id", chatID, "error", redactBotError(token, err.Error()))
		return 0, err
	}
	if !result.OK {
		applog.Error("bot", "message_send_rejected", "chat_id", chatID, "error", redactBotError(token, result.Description))
		return 0, fmt.Errorf("%s", result.Description)
	}
	return result.Result.MessageID, nil
}

// edit reports whether Telegram confirmed that the message no longer exists,
// then whether the desired card is already/now rendered successfully.
func (s *Service) edit(token string, chatID, messageID int64, text string, keyboard [][]button) (missing, updated bool) {
	// editMessageText returns a Message object for normal chat messages, not a
	// boolean. Keep the result opaque: only the API's OK/description fields are
	// relevant here and decoding it as bool makes every successful edit fail.
	var result apiResponse[json.RawMessage]
	// The keyboard travels in the same request as the text, including when the
	// card has none: an empty inline keyboard is how Telegram is told to remove
	// the existing one. Sending it as a second editMessageReplyMarkup call - as
	// this used to - doubled the outbound cost of every keyboardless render
	// against the same budget the progress refreshes and command replies share.
	markup := keyboard
	if markup == nil {
		markup = [][]button{}
	}
	payload := map[string]any{"chat_id": chatID, "message_id": messageID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true, "reply_markup": map[string]any{"inline_keyboard": markup}}
	if err := s.call(token, "editMessageText", payload, &result); err != nil {
		applog.Error("bot", "message_edit_failed", "chat_id", chatID, "message_id", messageID, "error", redactBotError(token, err.Error()))
		// A chat that can never be written to again is reported as missing so its
		// card reference is dropped instead of being retried on every later status
		// change.
		return unreachableTarget(err), false
	}
	if !result.OK {
		if isMissingMessage(result.Description) {
			return true, false
		}
		if strings.Contains(strings.ToLower(result.Description), "message is not modified") {
			return false, true
		}
		applog.Error("bot", "message_edit_rejected", "chat_id", chatID, "message_id", messageID, "error", redactBotError(token, result.Description))
		return false, false
	}
	return false, true
}

// botAPIError is an HTTP-level Bot API failure. It carries the status code
// because a caller has to tell a conversation that can no longer be written to
// from one that is merely having a bad moment, and the response body - which
// holds Telegram's own wording - is not read on this path.
type botAPIError struct {
	method string
	status int
}

func (e *botAPIError) Error() string {
	return fmt.Sprintf("Bot API %s returned HTTP %d", e.method, e.status)
}

// unreachableTarget reports whether an edit failed because this chat or message
// can never be written to again: the user blocked the Bot, the Bot was removed
// from the group, the account was deleted, or the id is unknown. Telegram
// answers all of those with 403.
//
// Treating them as retryable is what made a blocked user expensive forever: the
// card reference survived, so every later status change of that task sent
// another editMessageText that could not succeed, against the same outbound
// budget the progress refreshes and command replies draw on.
func unreachableTarget(err error) bool {
	var apiErr *botAPIError
	return errors.As(err, &apiErr) && apiErr.status == http.StatusForbidden
}

func isMissingMessage(description string) bool {
	v := strings.ToLower(description)
	for _, permanent := range []string{
		"message to edit not found",
		"message_id_invalid",
		// The chat itself is gone, not just the card in it. Same conclusion:
		// nothing can be rendered here again.
		"chat not found",
		"bot was blocked by the user",
		"bot was kicked",
		"user is deactivated",
		"peer_id_invalid",
	} {
		if strings.Contains(v, permanent) {
			return true
		}
	}
	return false
}

func (s *Service) answer(token, id, text string) {
	var result apiResponse[bool]
	_ = s.call(token, "answerCallbackQuery", map[string]any{"callback_query_id": id, "text": short(text, 180)}, &result)
}

func writePrivateFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
