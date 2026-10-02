package download

import (
	"context"
	"database/sql"
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
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/vacks/tdl/internal/applog"
	"github.com/vacks/tdl/internal/kv"
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

// SavedIntent targets the Saved Messages dialog of the specified Telegram
// account. It intentionally carries the Telegram user ID for stable directory
// naming; AccountID selects the authenticated session.
type SavedIntent struct {
	Source     SourceKind
	AccountID  string
	TelegramID int64
	ListenOnly bool
}

// SubmitSaved acts on the one Saved Messages task an account has.
//
// There is at most one of them, and that is the point: the task's target is the
// account's own saved dialog, so a second one would index the same media and
// race the first for every file. Asking for the history again is therefore a
// request to scan that task again, not a reason to make another, and ListenOnly
// selects which half of the task the call acts on - start listening on it, or
// re-scan it - rather than which task to build.
//
// The one exception is an account that has never had a task: the first request
// still decides the shape. A history request scans from the oldest message; a
// listen request starts already scanned and therefore never walks history,
// because "listen for new messages" must not begin by downloading everything
// ever saved.
func (m *Manager) SubmitSaved(ctx context.Context, intent SavedIntent) (ChatJob, bool, error) {
	if intent.AccountID == "" || intent.TelegramID <= 0 {
		return ChatJob{}, false, errors.New("收藏夹账号信息不完整")
	}
	if intent.Source == "" {
		intent.Source = SourceBot
	}
	var job ChatJob
	var existing bool
	err := m.accounts.Run(ctx, intent.AccountID, func(ctx context.Context, client *gotd.Client, _ kv.Storage) error {
		key := savedDialogKey(intent.AccountID)
		id, found, err := m.keepOneSavedChat(intent.AccountID)
		if err != nil {
			return err
		}
		if found {
			existing = true
			if intent.ListenOnly {
				if err := m.SetChatListening(id, true); err != nil {
					return err
				}
			} else {
				latestID, err := latestSavedMessageID(ctx, client)
				if err != nil {
					return err
				}
				if err := m.rescanSavedChat(id, latestID); err != nil {
					return err
				}
			}
			job, err = m.GetChat(id)
			return err
		}
		latestID, err := latestSavedMessageID(ctx, client)
		if err != nil {
			return err
		}
		name := fmt.Sprintf("收藏消息_%d", intent.TelegramID)
		start, status, scanState := 0, ChatStatusQueued, chatScanPending
		if intent.ListenOnly {
			start, status, scanState = -1, ChatStatusListening, chatScanCompleted
		}
		configJSON, marshalErr := json.Marshal(m.settings.Get().Download)
		if marshalErr != nil {
			return marshalErr
		}
		job, err = m.createChatJob(ChatJob{SourceURL: savedSourcePrefix + intent.AccountID, DialogType: "self", DialogKey: key, DialogName: name, AccountID: intent.AccountID, StartMessageID: start, UpperMessageID: latestID, ListenNew: intent.ListenOnly, Status: status, ScanState: scanState}, directPeer{kind: "self"}, string(configJSON))
		return err
	})
	if err != nil {
		return ChatJob{}, false, err
	}
	m.signalChat()
	m.markChatListenerDirty()
	if existing {
		applog.Info("chat_download", "saved_task_updated", "chat_job_id", job.ID, "account_id", intent.AccountID, "listen_only", intent.ListenOnly)
	} else {
		applog.Info("chat_download", "saved_task_created", "chat_job_id", job.ID, "account_id", intent.AccountID, "telegram_id", intent.TelegramID, "listen_only", intent.ListenOnly)
	}
	if intent.ListenOnly {
		// A listener that starts after the fact still owes whatever was
		// published between its watermark and now. That is as true of turning
		// listening on for a task that already exists - the usual case now that
		// the account has one task - as it is of creating a listen-only one: the
		// task's upper bound is where its knowledge ends either way, and the
		// live connection only sees what arrives after it is established.
		go m.reconcileListenerGap(job.ID)
	}
	return job, existing, nil
}

// keepOneSavedChat names the account's one Saved Messages task and retires any
// others it still has.
//
// An account is meant to have one, and this is where that is enforced rather
// than at each caller, because every path that acts on the saved task comes
// through here. A database written before the rule can hold two at once - a
// history task and a listen-only task, both running - because the old duplicate
// check only looked for the shape it was about to create. The lookup keeps the
// row that carries a history range and retires the rest.
//
// Preferring the history row is not cosmetic. The active-uniqueness index covers
// (account_id, dialog_key, start_message_id), so rescanning a listen-only task
// while a running history task still holds the range would move it onto a key
// that already exists and the whole command would fail with a duplicate-key
// error - which is exactly the arrangement the two-task era leaves behind.
//
// Deleted is the one status that does not count as existing: a purged task is
// gone, and asking for the history afterwards is a request to build a new one.
func (m *Manager) keepOneSavedChat(accountID string) (string, bool, error) {
	key := savedDialogKey(accountID)
	var id string
	// A false sorts before a true, so a task with a history range is preferred
	// over one that only listens, and the newest of each kind wins otherwise.
	err := m.db.QueryRow(`SELECT id FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND status != ? ORDER BY (start_message_id < 0), created_at DESC LIMIT 1`, accountID, key, ChatStatusDeleted).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if err := m.retireOtherSavedChats(accountID, key, id); err != nil {
		return "", false, err
	}
	return id, true, nil
}

// retireOtherSavedChats stops and hides the account's other Saved Messages
// tasks.
//
// The steps are the ones a person would take by hand - cancel a task that is
// still running, then delete it - because those are the paths that already know
// how to end a task safely: they release the media claims it holds, stop its
// transfers and drop its temporary directory. Deleting a running task directly
// is refused for exactly that reason, so this goes through the same doors rather
// than around them.
func (m *Manager) retireOtherSavedChats(accountID, key, keepID string) error {
	rows, err := m.db.Query(`SELECT id, status FROM chat_download_jobs WHERE account_id = ? AND dialog_key = ? AND id != ? AND status != ?`, accountID, key, keepID, ChatStatusDeleted)
	if err != nil {
		return err
	}
	type savedRow struct{ id, status string }
	others := make([]savedRow, 0, 2)
	for rows.Next() {
		var row savedRow
		if err := rows.Scan(&row.id, &row.status); err != nil {
			rows.Close()
			return err
		}
		others = append(others, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, other := range others {
		// The two guards are complementary and total: the statuses CancelChat
		// refuses are the ones DeleteChat accepts.
		switch other.status {
		case ChatStatusCompleted, ChatStatusFailed, ChatStatusPartial, ChatStatusCancelled:
		default:
			if err := m.CancelChat(other.id); err != nil {
				return err
			}
		}
		if err := m.DeleteChat(other.id); err != nil {
			return err
		}
		applog.Info("chat_download", "saved_task_retired", "chat_job_id", other.id, "account_id", accountID, "kept_chat_job_id", keepID)
	}
	return nil
}

// rescanSavedChat sends the account's saved task back over its history.
//
// It re-reads the dialog's newest message as the upper bound, drops every scan
// cursor so the walk starts from the top, and requeues the files that are not
// already published - so a file that had failed is offered again rather than
// being left as the one thing a re-scan cannot fix.
//
// Walking the history again is what "scan it again" means, and it is safe: the
// media index is keyed by (dialog_key, message_id), so a file that is already
// indexed is left alone instead of being downloaded twice. The gap cursors are
// deliberately left where they are; they belong to the listener's own walk,
// which is a different question from where the history scan stopped.
func (m *Manager) rescanSavedChat(id string, latestID int) error {
	lock := m.chatLock(id)
	lock.Lock()
	defer lock.Unlock()
	target, err := m.chatTarget(id)
	if err != nil {
		return err
	}
	// A cancelled task is revived rather than refused, which is what asking for
	// its history again means; RetryChat already treats the same state that way.
	// A deleted one is not reachable here - the lookup that found this task
	// skips them - so it only guards against a purge that landed in between.
	if target.Status == ChatStatusDeleted {
		return errors.New("当前会话任务已被删除")
	}
	// The item rows and the parent move together, so the scan can never be
	// observed queued while the files it is about to index are still marked
	// finished: a file that failed and was requeued by the same transaction is
	// either both visible or neither.
	//
	// The state change comes before the execution contexts are cancelled, in the
	// order PauseChat established: the lock is held throughout, so a transfer
	// that is publishing right now waits for this to finish and then sees a
	// parent that is no longer running, which is the check it makes anyway.
	if err := m.transitionChatItems(id, target.Status, ChatStatusQueued, "",
		`UPDATE chat_download_items SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0, attempts = 0 WHERE chat_job_id = ? AND status IN ('failed', 'cancelled', 'paused')`,
		func(tx *databaseTx) error {
			if _, err := tx.Exec(`UPDATE chat_download_streams SET offset_message_id = 0, completed = 0 WHERE chat_job_id = ? AND stream_kind NOT IN (?, ?)`, id, listenerGapStream, listenerGapDiscussionStream); err != nil {
				return err
			}
			// start_message_id is put back to zero: a task that was listening
			// only had never claimed a history, and this is the request that
			// claims one.
			_, err := tx.Exec(`UPDATE chat_download_jobs SET start_message_id = 0, upper_message_id = ?, scan_state = ?, error = '' WHERE id = ?`, latestID, chatScanPending, id)
			return err
		}, id); err != nil {
		return err
	}
	m.cancelChatExecutions(id)
	return nil
}

// latestSavedMessageID reads the newest message in the account's saved dialog,
// which is the upper bound a scan of it is frozen at.
func latestSavedMessageID(ctx context.Context, client *gotd.Client) (int, error) {
	latestID := 0
	it := query.Messages(client.API()).GetHistory((&tg.InputPeerSelf{})).BatchSize(1).Iter()
	if it.Next(ctx) {
		if latest, ok := it.Value().Msg.(*tg.Message); ok {
			latestID = latest.ID
		}
	} else if err := it.Err(); err != nil {
		return 0, fmt.Errorf("读取收藏消息: %w", err)
	}
	return latestID, nil
}

// ErrNoEligibleMedia reports that a message was read successfully and every
// file in it was rejected by the current size or type filter.
//
// It is an answer, not a fault. Callers that report to a person keep showing its
// text, which is why it is still an error; the inbox settles the event that
// produced it as skipped, because a message's contents do not change between two
// attempts a second apart and re-asking costs a Telegram request for nothing.
var ErrNoEligibleMedia = errors.New("消息中的文件均不符合当前文件体积或类型筛选条件")

// ErrNoMedia reports that the message was read successfully and simply holds
// nothing downloadable - ordinary text, a poll, a link preview.
//
// It is kept apart from ErrNoEligibleMedia because the two send the reader to
// different places. "Filtered out" is a statement about the configured size and
// type rules and is only true when a file actually was rejected; "no media" is a
// statement about the link. Reporting the second with the first's wording
// pointed people at settings that had no part in the outcome. Both are answers
// rather than faults, so both settle an inbox event as skipped.
var ErrNoMedia = errors.New("该消息不包含可下载的文件（纯文本、投票或链接预览）")

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
		m.recordTelegramRPCError(accountID, err)
		return Submission{}, err
	}
	sources = filterSources(sources, m.settings.Get().Download)
	if len(sources) == 0 {
		return Submission{}, nothingToDo(ErrNoEligibleMedia)
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
	err := m.accounts.Run(ctx, accountID, func(ctx context.Context, client *gotd.Client, kvd kv.Storage) error {
		manager := peers.Options{Storage: kv.NewPeers(kvd)}.Build(client.API())
		peer, startID, err := resolveChatTarget(ctx, manager, intent.URL)
		if err != nil {
			return err
		}
		input := peer.InputPeer()
		dialogType, dialogKey, dialogID := dialogIdentityForPeer(peer, accountID)
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
		m.recordTelegramRPCError(accountID, err)
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
