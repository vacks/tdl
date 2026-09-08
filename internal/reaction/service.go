// Package reaction implements reaction-triggered downloads as a thin layer on
// top of the existing Telegram account and download managers.
package reaction

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/download"
	"github.com/vacks/tdl/internal/settings"
	"github.com/vacks/tdl/internal/telegram"
)

type listener struct {
	cancel                                 context.CancelFunc
	mu                                     sync.RWMutex
	state, lastError, connectedAt, retryAt string
	attempt                                int
}

type ListenerHealth struct {
	AccountID   string `json:"accountId"`
	State       string `json:"state"`
	LastError   string `json:"lastError,omitempty"`
	ConnectedAt string `json:"connectedAt,omitempty"`
	RetryAt     string `json:"retryAt,omitempty"`
	Attempt     int    `json:"attempt"`
}

type Service struct {
	settings  *settings.Store
	telegram  *telegram.Manager
	downloads *download.Manager
	mu        sync.Mutex
	listeners map[string]*listener
}

func New(store *settings.Store, accounts *telegram.Manager, downloads *download.Manager) *Service {
	return &Service{settings: store, telegram: accounts, downloads: downloads, listeners: make(map[string]*listener)}
}

// Start reconciles enabled accounts every few seconds. This makes settings and
// account login/logout changes effective without restarting the container.
func (s *Service) Start() {
	applog.Info("reaction", "service_started")
	go s.run()
	// Two independent leases provide bounded resolution concurrency while the
	// update callback remains a fast SQLite write.
	for worker := 0; worker < 2; worker++ {
		go s.processInbox(worker + 1)
	}
}

func (s *Service) run() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		s.reconcile()
		<-ticker.C
	}
}

func (s *Service) reconcile() {
	config := s.settings.Get().Reaction
	accounts, _ := s.telegram.List()
	wanted := make(map[string]struct{})
	if config.Enabled && len(config.Emojis) > 0 {
		for _, account := range accounts {
			if account.State == "authorized" {
				wanted[account.ID] = struct{}{}
			}
		}
	}
	s.mu.Lock()
	for id, running := range s.listeners {
		if _, ok := wanted[id]; !ok {
			applog.Info("reaction", "listener_stopped", "account_id", id)
			running.cancel()
			delete(s.listeners, id)
		}
	}
	for id := range wanted {
		if _, ok := s.listeners[id]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		running := &listener{cancel: cancel}
		s.listeners[id] = running
		running.set("starting", "", time.Time{}, 0)
		applog.Info("reaction", "listener_starting", "account_id", id, "emoji_count", len(config.Emojis))
		go s.listen(ctx, id, running)
	}
	s.mu.Unlock()
}

func (s *Service) listen(ctx context.Context, accountID string, running *listener) {
	for attempt := 0; ; attempt++ {
		running.set("connecting", "", time.Time{}, attempt)
		err := s.telegram.ListenReactions(ctx, accountID, s.handle, func() { running.set("connected", "", time.Time{}, 0) })
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			applog.Info("reaction", "listener_stopped", "account_id", accountID)
			break
		}
		if err == nil {
			err = errors.New("表情监听连接意外结束")
		}
		delay := reconnectDelay(attempt)
		next := time.Now().UTC().Add(delay)
		running.set("retrying", err.Error(), next, attempt+1)
		applog.Error("reaction", "listener_retry_scheduled", "account_id", accountID, "attempt", attempt+1, "retry_after", delay.String(), "error", err.Error())
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
		if ctx.Err() != nil {
			break
		}
	}
	s.mu.Lock()
	if s.listeners[accountID] == running {
		delete(s.listeners, accountID)
	}
	s.mu.Unlock()
}

func reconnectDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 5 {
		attempt = 5
	}
	base := 2 * time.Second * time.Duration(1<<attempt)
	// Stable jitter prevents several account listeners from reconnecting in the
	// same instant, without adding another random source to persisted behavior.
	return base + time.Duration((attempt*347)%1000)*time.Millisecond
}

func (l *listener) set(state, lastError string, retryAt time.Time, attempt int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state, l.lastError, l.attempt = state, lastError, attempt
	if state == "connected" {
		l.connectedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if retryAt.IsZero() {
		l.retryAt = ""
	} else {
		l.retryAt = retryAt.Format(time.RFC3339Nano)
	}
}

func (s *Service) Health() []ListenerHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]ListenerHealth, 0, len(s.listeners))
	for accountID, listener := range s.listeners {
		listener.mu.RLock()
		result = append(result, ListenerHealth{AccountID: accountID, State: listener.state, LastError: listener.lastError, ConnectedAt: listener.connectedAt, RetryAt: listener.retryAt, Attempt: listener.attempt})
		listener.mu.RUnlock()
	}
	return result
}

func (s *Service) handle(ctx context.Context, event telegram.ReactionEvent) {
	config := s.settings.Get().Reaction
	if !config.Enabled {
		return
	}
	allowed := make(map[string]struct{}, len(config.Emojis))
	for _, emoji := range config.Emojis {
		allowed[settings.CanonicalReactionEmoji(emoji)] = struct{}{}
	}
	for _, emoji := range event.Emojis {
		canonicalEmoji := settings.CanonicalReactionEmoji(emoji)
		if _, ok := allowed[canonicalEmoji]; ok {
			queued, err := s.downloads.QueueReaction(download.DownloadIntent{Source: download.SourceReaction, AccountID: event.AccountID, Message: &download.MessageRef{SourceURL: event.SourceURL, DialogName: event.DialogName, DialogID: event.DialogID, InputPeer: event.InputPeer, MessageID: event.MessageID}, Trigger: map[string]string{"emoji": canonicalEmoji}}, canonicalEmoji)
			if err != nil {
				applog.Error("reaction", "inbox_enqueue_failed", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID, "emoji", canonicalEmoji, "error", err.Error())
			} else if !queued {
				applog.Info("reaction", "inbox_duplicate_ignored", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID, "emoji", canonicalEmoji)
			} else {
				applog.Info("reaction", "inbox_queued", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID, "emoji", canonicalEmoji)
			}
			return
		}
	}
	applog.Info("reaction", "update_ignored_unmatched_emoji", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID)
}

func (s *Service) processInbox(worker int) {
	for {
		events, err := s.downloads.ClaimReactionInbox(1)
		if err != nil {
			applog.Error("reaction", "inbox_claim_failed", "worker", worker, "error", err.Error())
			time.Sleep(2 * time.Second)
			continue
		}
		if len(events) == 0 {
			time.Sleep(time.Second)
			continue
		}
		for _, event := range events {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			submission, submitErr := s.downloads.Submit(ctx, event.Intent)
			cancel()
			if submitErr != nil {
				if err := s.downloads.RetryReactionInbox(event.ID, event.Attempts, submitErr); err != nil {
					applog.Error("reaction", "inbox_retry_update_failed", "inbox_id", event.ID, "error", err.Error())
				}
				applog.Error("reaction", "inbox_submit_failed", "inbox_id", event.ID, "attempt", event.Attempts, "error", submitErr.Error())
				continue
			}
			if err := s.downloads.CompleteReactionInbox(event.ID, submission.Job.ID); err != nil {
				applog.Error("reaction", "inbox_complete_update_failed", "inbox_id", event.ID, "error", err.Error())
				continue
			}
			outcome := "created"
			if submission.Duplicate {
				outcome = "attached"
			}
			applog.Info("reaction", "inbox_processed", "inbox_id", event.ID, "job_id", submission.Job.ID, "outcome", outcome)
		}
	}
}
