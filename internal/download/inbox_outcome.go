package download

import "errors"

// A failed inbox submit has three possible meanings, and which one it is
// decides whether the queue spends another attempt on the event.
//
// A transport failure is worth retrying: the same call can succeed a moment
// later. The other two cannot be retried, and they are deliberately not the
// same outcome, because they do not mean the same thing to the person watching:
//
//   - nothingToDo is a successful answer. The request was evaluated and there is
//     nothing to download: every file in the message failed the size or type
//     filter, or the task it belonged to is gone. Retrying asks a question that
//     already has its answer, and reporting it as a failure would raise an alarm
//     where nothing is wrong. The event settles as 'skipped'.
//   - permanentFailure is a fault no later attempt can clear: Telegram rejected
//     the request outright, or the state the task needs to proceed cannot be
//     read at all. The event settles as 'failed' on the first attempt instead of
//     spending the whole budget to reach the same place.
//
// Classification belongs to the error rather than to the call site, because the
// same Submit serves the Bot, the Web UI and both inboxes, and only the inboxes
// need to tell the three apart. That is also why a nothingToDo outcome is still
// an error: the Bot and the Web UI show its text to the person who asked.
type outcomeKind int

const (
	outcomeNothingToDo outcomeKind = iota
	outcomePermanent
)

// outcomeError carries the way an inbox must treat a cause without changing
// what a person reads, because Error returns the cause's own text.
type outcomeError struct {
	kind  outcomeKind
	cause error
}

func (e *outcomeError) Error() string { return e.cause.Error() }
func (e *outcomeError) Unwrap() error { return e.cause }

// nothingToDo marks an error that reports a finished evaluation whose answer is
// "there is nothing here to download".
func nothingToDo(err error) error {
	if err == nil {
		return nil
	}
	return &outcomeError{kind: outcomeNothingToDo, cause: err}
}

// permanentFailure marks an error that no later attempt can change.
func permanentFailure(err error) error {
	if err == nil {
		return nil
	}
	return &outcomeError{kind: outcomePermanent, cause: err}
}

// IsNothingToDo reports whether an inbox submit was answered rather than
// failed, so the caller settles the event as skipped instead of retrying it.
func IsNothingToDo(err error) bool {
	var outcome *outcomeError
	return errors.As(err, &outcome) && outcome.kind == outcomeNothingToDo
}

func isPermanentFailure(err error) bool {
	var outcome *outcomeError
	return errors.As(err, &outcome) && outcome.kind == outcomePermanent
}
