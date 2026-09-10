package download

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
	"github.com/vacks/tdl/internal/applog"
)

func triggerJSON(trigger map[string]string) string {
	if len(trigger) == 0 {
		return "{}"
	}
	encoded, err := json.Marshal(trigger)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// SourceKind records why a download was requested.  It is deliberately an
// application concern: the downloader never needs to know whether a message
// came from the Web UI, Bot, a reaction, or a future automation.
type SourceKind string

const (
	SourceWeb      SourceKind = "web"
	SourceBot      SourceKind = "bot"
	SourceReaction SourceKind = "reaction"
	SourceAPI      SourceKind = "api"
)

// MessageRef is the canonical representation of one Telegram message. Public
// links are optional presentation/audit metadata; private dialogs use the
// InputPeer supplied by Telegram instead.
type MessageRef struct {
	SourceURL  string
	DialogName string
	DialogID   int64
	InputPeer  tg.InputPeerClass
	MessageID  int
}

// DownloadIntent is the single input accepted by the download application
// service. Exactly one target is required: URL for normal shared links, or
// Message for an already-resolved Telegram update.
type DownloadIntent struct {
	Source    SourceKind
	AccountID string
	URL       string
	Message   *MessageRef
	// Trigger is compact audit context, for example {"emoji":"❤️"}. It is
	// intentionally not used for deduplication or download execution.
	Trigger map[string]string
}

// Submission says whether a new job was made or this request was attached to
// an existing one. A duplicate is a successful, idempotent outcome.
type Submission struct {
	RequestID   string `json:"requestId"`
	Job         Job    `json:"job"`
	Created     bool   `json:"created"`
	Duplicate   bool   `json:"duplicate"`
	Reactivated bool   `json:"reactivated"`
}

func (i DownloadIntent) validate() error {
	if i.Source == "" {
		return errors.New("下载请求缺少来源")
	}
	if i.URL == "" && i.Message == nil {
		return errors.New("下载请求缺少 Telegram 消息目标")
	}
	if i.URL != "" && i.Message != nil {
		return errors.New("下载请求只能指定一种消息目标")
	}
	if i.Message != nil && (i.Message.InputPeer == nil || i.Message.MessageID <= 0) {
		return errors.New("Telegram 消息引用不完整")
	}
	return nil
}

// Submit is the only creation use case. Callers should construct an intent
// instead of adding another EnqueueXxx method for each product feature.
func (m *Manager) Submit(ctx context.Context, intent DownloadIntent) (Submission, error) {
	if err := intent.validate(); err != nil {
		return Submission{}, err
	}
	accountID := intent.AccountID
	if accountID == "" {
		var err error
		accountID, err = m.accounts.CurrentID()
		if err != nil {
			return Submission{}, err
		}
	}
	intent.AccountID = accountID

	var (
		sources []source
		direct  directPeer
		err     error
	)
	if intent.Message != nil {
		sources, err = m.resolvePeer(ctx, accountID, intent.Message.InputPeer, intent.Message.DialogID, intent.Message.MessageID, intent.Message.DialogName)
		direct = makeDirectPeer(intent.Message.InputPeer)
		if intent.URL == "" {
			intent.URL = intent.Message.SourceURL
		}
	} else {
		sources, err = m.resolve(ctx, accountID, intent.URL)
	}
	if err != nil {
		return Submission{}, err
	}
	if len(sources) == 0 {
		return Submission{}, errors.New("消息中没有可下载的媒体")
	}
	// A direct Telegram update may not have a public link. Keep a stable,
	// non-public source value for audit and upstream resume bookkeeping rather
	// than collapsing unrelated private messages onto an empty URL.
	if intent.URL == "" {
		intent.URL = fmt.Sprintf("tg://message/%s/%d", sources[0].DialogKey, sources[0].MessageID)
	}
	submission, err := m.enqueueIntent(intent, sources, direct)
	if err != nil {
		applog.Error("download", "task_submit_failed", "source", intent.Source, "account_id", accountID, "error", err.Error())
		return Submission{}, err
	}
	if submission.Created {
		applog.Info("download", "task_created", "job_id", submission.Job.ID, "request_id", submission.RequestID, "source", intent.Source, "account_id", accountID, "item_count", submission.Job.TotalItems, "files", taskLogFiles(sources))
	} else {
		applog.Info("download", "task_request_attached", "job_id", submission.Job.ID, "request_id", submission.RequestID, "source", intent.Source, "account_id", accountID)
	}
	return submission, nil
}

func (i DownloadIntent) String() string {
	if i.URL != "" {
		return i.URL
	}
	if i.Message != nil {
		return fmt.Sprintf("message:%d", i.Message.MessageID)
	}
	return ""
}
