package download

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/vacks/tdl/internal/tmsg"
)

// A file that produced nothing still has to be settled, and how depends on
// whether another attempt could change the answer.
//
// The engine reports one outcome per message it could not deliver, because a
// count cannot be written back to the row that belongs to the file. This is
// where that report becomes a decision.
//
//   - Transient: the attempt failed for a reason a later one can outlast. The
//     account reaches Telegram through a proxy, and that link drops - a request
//     in flight fails with a reset, a timeout, a read error, a DC that has to
//     be re-dialled. None of that is a statement about the file. Treating it as
//     one would mark files failed that a second attempt fetches in full, so
//     these go back on the queue under the same attempt budget the stalled and
//     flood paths already use.
//   - Terminal: no later attempt can change the answer, and spending the whole
//     budget to arrive at it again is worse than saying so now. The list is an
//     allow-list, for the same reason the Bot's fallback is one: the space of
//     Telegram errors is open-ended, and a broad list turns retryable failures
//     into permanent ones.
//
// A message that was read and held nothing downloadable is terminal too, and it
// is not a fault at all - the media was removed by an edit, or this account can
// no longer see it. It is settled rather than retried, and it says so in its own
// words.
type transferOutcome int

const (
	// transferRetryable is an attempt a later one can outlast.
	transferRetryable transferOutcome = iota
	// transferTerminal is an answer no later attempt will change.
	transferTerminal
)

// classifyTransferError decides which of the two an outcome is.
func classifyTransferError(err error) transferOutcome {
	// The message was read and held nothing to fetch.
	if err == nil {
		return transferTerminal
	}
	if errors.Is(err, tmsg.ErrMessageDeleted) {
		return transferTerminal
	}
	// A stopped batch is not this file's answer. Whatever stopped it owns the
	// row, and settling it here would overwrite that. The engine does not report
	// these, but the classification must not depend on that staying true.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return transferRetryable
	}
	if tgerr.Is(err,
		tg.ErrMessageIDInvalid,
		tg.ErrPeerIDInvalid,
		tg.ErrChannelPrivate,
		tg.ErrFileIDInvalid,
		tg.ErrMediaEmpty,
		tg.ErrUserBannedInChannel,
	) {
		return transferTerminal
	}
	return transferRetryable
}

// transferOutcomeMessage is what a person reads on the file that did not arrive.
//
// A retryable failure is worded as one: the row goes back on the queue, and
// saying "failed" over a file that is about to be tried again would report a
// verdict the task has not reached.
func transferOutcomeMessage(err error, outcome transferOutcome) string {
	switch {
	case err == nil:
		return "消息中已没有可下载的内容"
	case errors.Is(err, tmsg.ErrMessageDeleted):
		return "消息已删除"
	case outcome == transferTerminal:
		return err.Error()
	default:
		return fmt.Sprintf("下载中断，将自动重试: %v", err)
	}
}

// transferOutcomeDetail is the cause alone, with no verdict around it.
//
// transferOutcomeMessage wraps a retryable cause in "下载中断，将自动重试"; the
// sentence that reports giving up on that retry needs the cause, not the
// promise, or the row reads "已停止自动重试: 将自动重试: …".
func transferOutcomeDetail(err error, outcome transferOutcome) string {
	switch {
	case err == nil:
		return "消息中已没有可下载的内容"
	case errors.Is(err, tmsg.ErrMessageDeleted):
		return "消息已删除"
	default:
		return err.Error()
	}
}
