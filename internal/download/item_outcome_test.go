package download

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/vacks/tdl/internal/tmsg"
)

// The account reaches Telegram through a proxy, and that link drops. A request
// in flight then fails with a reset, a timeout, a read error, a DC that has to
// be re-dialled - and none of that is a statement about the file. Classifying
// those as terminal is what turned every blip into a task a person had to
// restart by hand.
func TestATransportFailureIsWorthAnotherAttempt(t *testing.T) {
	retryable := []error{
		errors.New("read tcp 10.0.0.2:52418->149.154.167.51:443: read: connection reset by peer"),
		errors.New("context deadline exceeded"),
		errors.New("unexpected EOF"),
		fmt.Errorf("get file: get next chunk: %w", errors.New("EOF")),
		&tgerr.Error{Code: 500, Type: "INTERNAL", Message: "INTERNAL"},
		// A stopped batch is never this file's answer: whatever stopped it owns
		// the row, and settling it here would overwrite that.
		context.Canceled,
	}
	for _, err := range retryable {
		if got := classifyTransferError(err); got != transferRetryable {
			t.Errorf("%v was classified as terminal; a dropped connection must be retried", err)
		}
	}
}

// The other half of the same decision. A message that is gone, a peer the
// account cannot reach and a file Telegram will not hand over are answers a
// later attempt returns identically, and spending the whole budget to arrive at
// them again only delays what the person watching has to be told.
func TestAnAnswerNoAttemptCanChangeIsTerminal(t *testing.T) {
	terminal := []error{
		tmsg.ErrMessageDeleted,
		// A message that was read and held nothing to fetch.
		nil,
		&tgerr.Error{Code: 400, Type: tg.ErrMessageIDInvalid, Message: tg.ErrMessageIDInvalid},
		&tgerr.Error{Code: 400, Type: tg.ErrChannelPrivate, Message: tg.ErrChannelPrivate},
		&tgerr.Error{Code: 400, Type: tg.ErrFileIDInvalid, Message: tg.ErrFileIDInvalid},
		&tgerr.Error{Code: 400, Type: tg.ErrPeerIDInvalid, Message: tg.ErrPeerIDInvalid},
	}
	for _, err := range terminal {
		if got := classifyTransferError(err); got != transferTerminal {
			t.Errorf("%v was classified as retryable; no attempt clears it", err)
		}
	}
}

// A retryable file says so on its row. The row goes back on the queue, so
// writing a verdict on it would report an outcome the task has not reached.
func TestARetryableFileDoesNotReadAsFailed(t *testing.T) {
	err := errors.New("read: connection reset by peer")
	message := transferOutcomeMessage(err, transferRetryable)
	if message == err.Error() {
		t.Fatalf("the retryable file carries the raw error %q, which reads as a verdict", message)
	}
	if !strings.Contains(message, "重试") {
		t.Fatalf("the retryable file reads %q, which does not say it will be tried again", message)
	}
	// A terminal one names the cause, because that is what the person has to act
	// on and nothing else will say it.
	if got := transferOutcomeMessage(tmsg.ErrMessageDeleted, transferTerminal); got != "消息已删除" {
		t.Fatalf("a deleted message reads %q", got)
	}
}
