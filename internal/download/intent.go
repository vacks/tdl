package download

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/util/tutil"
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

// ChatIntent creates a bounded historical download for a channel or group.
// A message URL starts at that message (inclusive); a public chat URL starts
// at the earliest available media. New messages are never part of the frozen
// historical range unless ListenNew is explicitly enabled.
type ChatIntent struct {
	Source    SourceKind
	AccountID string
	URL       string
	ListenNew bool
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

func (i ChatIntent) validate() error {
	if i.Source == "" {
		return errors.New("会话下载请求缺少来源")
	}
	if strings.TrimSpace(i.URL) == "" {
		return errors.New("请输入 Telegram 频道或群组链接")
	}
	return nil
}

// SubmitChat persists a resolved, immutable historical range. The indexing
// worker is intentionally separate from link resolution: retries never change
// the upper bound and therefore never make a moving channel history endless.
func (m *Manager) SubmitChat(ctx context.Context, intent ChatIntent) (ChatJob, error) {
	if err := intent.validate(); err != nil {
		return ChatJob{}, err
	}
	accountID := intent.AccountID
	if accountID == "" {
		var err error
		accountID, err = m.accounts.CurrentID()
		if err != nil {
			return ChatJob{}, err
		}
	}
	intent.URL = strings.TrimSpace(intent.URL)
	var created ChatJob
	err := m.accounts.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, kvd storage.Storage) error {
		manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(client.API())
		peer, startID, err := resolveChatTarget(ctx, manager, intent.URL)
		if err != nil {
			return err
		}
		input := peer.InputPeer()
		dialogType, dialogKey, dialogID := dialogIdentity(input, accountID)
		if dialogType != "channel" && dialogType != "chat" {
			return errors.New("会话下载仅支持频道和群组")
		}
		it := query.Messages(client.API()).GetHistory(input).BatchSize(1).Iter()
		if !it.Next(ctx) {
			if err := it.Err(); err != nil {
				return fmt.Errorf("获取会话最新消息: %w", err)
			}
			return errors.New("该会话没有可读取的消息")
		}
		latest, ok := it.Value().Msg.(*tg.Message)
		if !ok {
			return errors.New("无法读取会话最新消息")
		}
		if startID > latest.ID {
			return errors.New("起始消息位置晚于会话最新消息")
		}
		configJSON, err := json.Marshal(m.settings.Get().Download)
		if err != nil {
			return err
		}
		created, err = m.createChatJob(ChatJob{SourceURL: intent.URL, DialogType: dialogType, DialogKey: dialogKey, DialogID: dialogID, DialogName: peer.VisibleName(), AccountID: accountID, StartMessageID: startID, UpperMessageID: latest.ID, ListenNew: intent.ListenNew}, makeDirectPeer(input), string(configJSON))
		return err
	})
	if err != nil {
		applog.Error("chat_download", "task_submit_failed", "source", intent.Source, "account_id", accountID, "error", err.Error())
		return ChatJob{}, err
	}
	applog.Info("chat_download", "task_created", "chat_job_id", created.ID, "account_id", accountID, "dialog_key", created.DialogKey, "start_message_id", created.StartMessageID, "upper_message_id", created.UpperMessageID, "listen_new", created.ListenNew)
	m.signalChat()
	return created, nil
}

func resolveChatTarget(ctx context.Context, manager *peers.Manager, rawURL string) (peers.Peer, int, error) {
	peer, messageID, err := tutil.ParseMessageLink(ctx, manager, rawURL)
	if err == nil {
		return peer, messageID, nil
	}
	parsed, parseErr := url.Parse(rawURL)
	if parseErr != nil || !isTelegramHost(parsed.Host) {
		return nil, 0, fmt.Errorf("解析会话链接: %w", err)
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 1 || parts[0] == "" || strings.EqualFold(parts[0], "c") {
		return nil, 0, errors.New("请提供频道或群组链接；私有会话需使用一条消息链接")
	}
	if _, convertErr := strconv.Atoi(parts[0]); convertErr == nil {
		return nil, 0, errors.New("会话链接缺少用户名")
	}
	resolved, resolveErr := tutil.GetInputPeer(ctx, manager, parts[0])
	if resolveErr != nil {
		return nil, 0, fmt.Errorf("解析会话链接: %w", resolveErr)
	}
	return resolved, 0, nil
}

func isTelegramHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(strings.Split(host, ":")[0]))
	return host == "t.me" || host == "www.t.me" || host == "telegram.me" || host == "www.telegram.me"
}
