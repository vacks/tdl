package download

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Answering a request and failing it are different outcomes, and the difference
// has to survive the wrapping the inbox stores. The message inbox stores the
// error the way the call produced it, but the reaction service passes it
// through Submit and both add context, so a marker that only works on the
// unwrapped error would be silently lost and the event would be retried again.
func TestNothingToDoSurvivesWrapping(t *testing.T) {
	cause := fmt.Errorf("提交下载任务: %w", nothingToDo(ErrNoEligibleMedia))
	if !IsNothingToDo(cause) {
		t.Fatal("a wrapped nothing-to-do outcome was not recognized")
	}
	if !errors.Is(cause, ErrNoEligibleMedia) {
		t.Fatal("the sentinel was lost by the wrapping, so callers cannot match it")
	}
	if !strings.Contains(cause.Error(), "不符合当前文件体积或类型筛选条件") {
		t.Fatalf("the person-facing text changed: %q", cause.Error())
	}
	// The two markers are exclusive. A permanent failure that was reported as
	// answered would settle as skipped, which is invisible, and the fault it
	// names would never be seen.
	if IsNothingToDo(permanentFailure(errors.New("boom"))) {
		t.Fatal("a permanent failure was classified as an answered event")
	}
	if isPermanentFailure(nothingToDo(errors.New("boom"))) {
		t.Fatal("an answered event was classified as a permanent failure")
	}
}

func TestInboxOutcomeMarkersIgnoreUnmarkedErrors(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("connection reset by peer")},
		{"nil", nil},
	} {
		if IsNothingToDo(testCase.err) {
			t.Fatalf("%s: classified as nothing to do", testCase.name)
		}
	}
	// nil must stay nil, because both markers are applied to the result of a
	// call that reports success by returning no error at all.
	if nothingToDo(nil) != nil || permanentFailure(nil) != nil {
		t.Fatal("marking a nil error invented a failure out of a success")
	}
}

// The listener path settles an event it was answered about without listing it
// as waiting work. This pins the predicate itself, because the failure this
// replaced was exactly a predicate: the reads used "not done", so the new
// terminal status silently joined the queue the Bot presents as actionable.
func TestOpenEventStatusesAreTheOpenOnes(t *testing.T) {
	for _, eventStatus := range []string{"pending", "processing", "failed"} {
		if !strings.Contains(openEventStatuses, "'"+eventStatus+"'") {
			t.Fatalf("open status %q is missing from %q", eventStatus, openEventStatuses)
		}
	}
	for _, settled := range []string{"done", "skipped"} {
		if strings.Contains(openEventStatuses, "'"+settled+"'") {
			t.Fatalf("settled status %q is listed as open work in %q", settled, openEventStatuses)
		}
	}
}
