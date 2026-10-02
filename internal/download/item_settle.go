package download

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	transfer "github.com/vacks/tdl/internal/download/transfer"
)

// itemFailure is one message of a batch that produced no file, turned into the
// write its row needs.
type itemFailure struct {
	item    source
	retry   bool
	message string
}

// planItemFailures turns the engine's per-message report into per-row decisions.
//
// The classification lives here rather than in either caller so the two task
// kinds cannot disagree about which failures are worth another attempt. What
// each caller does with the decisions is its own - the tables differ - but
// whether a dropped proxy connection is retried must not depend on whether the
// file came from a link or from a listened channel.
func planItemFailures(outcomes []transfer.FileOutcomeUpdate, byMessage map[int]source) []itemFailure {
	planned := make([]itemFailure, 0, len(outcomes))
	for _, outcome := range outcomes {
		item, ok := byMessage[outcome.MessageID]
		if !ok {
			// A message this batch was not asked for. Nothing to write.
			continue
		}
		kind := classifyTransferError(outcome.Err)
		planned = append(planned, itemFailure{
			item:    item,
			retry:   kind == transferRetryable,
			message: transferOutcomeMessage(outcome.Err, kind),
		})
	}
	return planned
}

// itemTable names one task's file table and the column holding its owner.
//
// The two tables are the same shape and their statements have to agree about
// every state string they mention, so the names are parameters of one builder
// instead of being typed out once per table. This is the smallest piece of the
// duplication between the two state machines, kept here because the failure
// write is the one that has to be right under a dropped connection.
type itemTable struct {
	items string
	owner string
}

var (
	messageItems = itemTable{items: "download_items", owner: "job_id"}
	chatItems    = itemTable{items: "chat_download_items", owner: "chat_job_id"}
)

// retryStatement returns a file to the queue, unless it has spent its attempts.
//
// The row is not simply set to queued and left to fail again: an account that
// cannot reach Telegram at all would otherwise retry forever, and every attempt
// is another request against a budget that is already the scarce thing. The
// budget is the same one the stall and flood paths use, and it is read from the
// row's own attempts column so all three share one counter.
func (t itemTable) retryStatement() string {
	// RETURNING is how the caller learns which branch the CASE took. A row count
	// cannot say it: the statement writes either way, and "was a row touched" is
	// not the question - a file that just spent its last attempt is finished,
	// not waiting. Asking with a second read would be a round trip on a path
	// that runs once per lost file.
	return fmt.Sprintf(`UPDATE %[1]s
SET status = CASE WHEN attempts >= ? THEN 'failed' ELSE 'queued' END,
    error = CASE WHEN attempts >= ? THEN ? ELSE ? END,
    started_at = '',
    finished_at = CASE WHEN attempts >= ? AND finished_at = '' THEN ? ELSE finished_at END
WHERE %[2]s = ? AND dialog_key = ? AND message_id = ?
  AND status NOT IN ('completed', 'downloaded')
RETURNING status`, t.items, t.owner)
}

// terminalStatement is the same write for a failure no attempt can clear.
//
// Completed and downloaded rows are excluded for the reason fail() excludes
// them: a file that was published is not un-published by a report about a
// different attempt at it.
func (t itemTable) terminalStatement() string {
	return fmt.Sprintf(`UPDATE %[1]s
SET status = 'failed', error = ?, finished_at = ?
WHERE %[2]s = ? AND dialog_key = ? AND message_id = ?
  AND status NOT IN ('completed', 'downloaded')`, t.items, t.owner)
}

// applyItemFailures writes back every file a batch could not deliver.
//
// It reports how many files it settled and whether any of them went back on the
// queue. Both matter to the caller: a file waiting for another attempt is work
// the task still has, and a file that was given its own reason must not have
// that reason overwritten by a task-level one.
func (m *Manager) applyItemFailures(table itemTable, ownerID string, failures []itemFailure) (settled int, requeued bool, err error) {
	if len(failures) == 0 {
		return 0, false, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, failure := range failures {
		if failure.retry {
			var status string
			queryErr := m.db.QueryRow(table.retryStatement(),
				maxStalledAttempts, // attempts >= ? -> the budget is spent
				maxStalledAttempts, // attempts >= ? -> which of the two messages
				fmt.Sprintf("下载中断，已停止自动重试: %s", failure.message),
				failure.message,
				maxStalledAttempts, now,
				ownerID, failure.item.DialogKey, failure.item.MessageID).Scan(&status)
			// No row means the guard refused it, which is not an error: a pause,
			// a cancel or an adoption by another task has already settled it.
			if errors.Is(queryErr, sql.ErrNoRows) {
				continue
			}
			if queryErr != nil {
				return settled, requeued, queryErr
			}
			settled++
			requeued = requeued || status == "queued"
			continue
		}
		if _, execErr := m.db.Exec(table.terminalStatement(),
			failure.message, now,
			ownerID, failure.item.DialogKey, failure.item.MessageID); execErr != nil {
			return settled, requeued, execErr
		}
		settled++
	}
	if settled > 0 {
		m.touch()
	}
	return settled, requeued, nil
}
