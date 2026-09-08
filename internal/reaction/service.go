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

type listener struct{ cancel context.CancelFunc }

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
func (s *Service) Start() { applog.Info("reaction", "service_started"); go s.run() }

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
		applog.Info("reaction", "listener_starting", "account_id", id, "emoji_count", len(config.Emojis))
		go s.listen(ctx, id, running)
	}
	s.mu.Unlock()
}

func (s *Service) listen(ctx context.Context, accountID string, running *listener) {
	err := s.telegram.ListenReactions(ctx, accountID, s.handle)
	if err != nil && !errors.Is(err, context.Canceled) {
		applog.Error("reaction", "listener_stopped_with_error", "account_id", accountID, "error", err.Error())
	} else {
		applog.Info("reaction", "listener_stopped", "account_id", accountID)
	}
	s.mu.Lock()
	if s.listeners[accountID] == running {
		delete(s.listeners, accountID)
	}
	s.mu.Unlock()
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
			submission, err := s.downloads.Submit(ctx, download.DownloadIntent{Source: download.SourceReaction, AccountID: event.AccountID, Message: &download.MessageRef{SourceURL: event.SourceURL, DialogName: event.DialogName, DialogID: event.DialogID, InputPeer: event.InputPeer, MessageID: event.MessageID}, Trigger: map[string]string{"emoji": canonicalEmoji}})
			// Duplicate delivery or an already existing task is intentional and is
			// recorded as a request attached to the original job.
			if err == nil && submission.Duplicate {
				applog.Info("reaction", "task_skipped_duplicate", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID, "emoji", canonicalEmoji)
				return
			}
			if err != nil {
				applog.Error("reaction", "task_create_failed", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID, "emoji", canonicalEmoji, "error", err.Error())
			} else {
				applog.Info("reaction", "task_created", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID, "emoji", canonicalEmoji)
			}
			return
		}
	}
	applog.Info("reaction", "update_ignored_unmatched_emoji", "account_id", event.AccountID, "dialog_id", event.DialogID, "message_id", event.MessageID)
}
