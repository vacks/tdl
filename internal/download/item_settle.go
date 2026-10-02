package download

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/vacks/tdl/internal/applog"
	transfer "github.com/vacks/tdl/internal/download/transfer"
	"github.com/vacks/tdl/internal/settings"
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

// itemTable names one task's file table, the column holding its owner, and the
// three ways the two tables genuinely differ.
//
// They are the same shape, and every statement written against them has to
// agree about the state strings it mentions and about what completing a file
// means. Writing those statements out once per table is how the two state
// machines came to disagree: the completion write was one atomic statement on
// one side and two independent ones on the other, and the session half could
// record an item as finished while writing no ownership for it.
type itemTable struct {
	items string
	owner string
	// ownerKind is what the ownership record calls this kind of task.
	ownerKind string
	// scoped is whether the owning task is part of the row's identity. The
	// message table is keyed by the media alone - one file has one row, in one
	// task - while a session task keys its rows by (chat_job_id, dialog_key,
	// message_id), because the same media can be indexed by several tasks.
	scoped bool
	// emitsItemEvents is whether a single file's state change is published to
	// the interface. The message view renders per-file rows and reacts to them;
	// the session view is a summary that redraws on the shared revision
	// counter, so an event naming a session task would be addressed to a stream
	// that has no such task in it.
	emitsItemEvents bool
	// releasesOnFailure is whether a file that fails gives up its ownership
	// claim at that moment. A session batch continues after one file fails, so
	// the claim has to be released there or the file it names waits for a task
	// that has already given up on it. A message task ends when its files do -
	// fail() releases everything a failed task holds - so its claims are
	// released a moment later, at task level.
	releasesOnFailure bool
}

var (
	messageItems = itemTable{items: "download_items", owner: "job_id", ownerKind: "message", emitsItemEvents: true}
	chatItems    = itemTable{items: "chat_download_items", owner: "chat_job_id", ownerKind: "chat", scoped: true, releasesOnFailure: true}
)

// ownerPredicate names every file one task owns.
//
// The control actions move a task's whole file set at once, so their statements
// are scoped by the owner alone - not by predicate below, which names a single
// file and would leave every other file of the task exactly where it was.
func (t itemTable) ownerPredicate() string { return t.owner + " = ?" }

// predicate names one file row, with the owning task in it when the table's
// identity includes one.
func (t itemTable) predicate() string {
	if t.scoped {
		return t.owner + " = ? AND dialog_key = ? AND message_id = ?"
	}
	return "dialog_key = ? AND message_id = ?"
}

// predicateArgs are predicate's placeholders, in its order.
func (t itemTable) predicateArgs(ownerID string, item source) []any {
	if t.scoped {
		return []any{ownerID, item.DialogKey, item.MessageID}
	}
	return []any{item.DialogKey, item.MessageID}
}

// completeStatement writes an item's terminal state and the ownership record
// every other task consults, in one statement.
//
// One statement, so a failure leaves neither written rather than an item that
// claims to be finished while nothing owns the file. The message path has been
// written this way since the divergence was found there; the session path wrote
// two statements, of which the second ran whether or not the first had matched
// a row, and could therefore record ownership of a file nobody has.
func (t itemTable) completeStatement() string {
	return fmt.Sprintf(`WITH updated AS (
 UPDATE %[1]s SET status = ?, final_path = ?, error = ?, finished_at = CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE %[2]s RETURNING %[3]s
)
INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at)
 SELECT ?, ?, ?, 'completed', '%[4]s', %[3]s, ? FROM updated
 ON CONFLICT(dialog_key, message_id) DO UPDATE SET final_path = EXCLUDED.final_path, status = EXCLUDED.status, owner_kind = EXCLUDED.owner_kind, owner_id = EXCLUDED.owner_id, updated_at = EXCLUDED.updated_at
RETURNING owner_id`, t.items, t.predicate(), t.owner, t.ownerKind)
}

// statement is the same write for every state but completion.
func (t itemTable) statement() string {
	return fmt.Sprintf(`UPDATE %[1]s SET status = ?, final_path = ?, error = ?, finished_at = CASE WHEN ? != '' AND finished_at = '' THEN ? ELSE finished_at END WHERE %[2]s RETURNING %[3]s`, t.items, t.predicate(), t.owner)
}

// setItemState is the one writer of a file's state.
//
// ownerID is only read by the tables whose rows name their task. It is returned
// by the statement from the row itself, which is why a write that matched
// nothing is not an error: the state changed under the caller, and there is
// nothing left to say about it.
func (m *Manager) setItemState(t itemTable, ownerID string, item source, status, path, message string) error {
	finished := ""
	if status == "completed" || status == "failed" || status == "cancelled" {
		finished = time.Now().UTC().Format(time.RFC3339Nano)
	}
	args := []any{status, path, message, finished, finished}
	args = append(args, t.predicateArgs(ownerID, item)...)

	var rowOwner string
	var err error
	if status == "completed" && path != "" {
		// The select list of the ownership insert is written before its own
		// values, so its two identity parameters come next.
		args = append(args, item.DialogKey, item.MessageID, path, time.Now().UTC().Format(time.RFC3339Nano))
		err = m.db.QueryRow(t.completeStatement(), args...).Scan(&rowOwner)
	} else {
		err = m.db.QueryRow(t.statement(), args...).Scan(&rowOwner)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		applog.Error("download", "item_state_save_failed", "dialog_key", item.DialogKey, "message_id", item.MessageID, "status", status, "error", err.Error())
		return err
	}
	m.touch()
	if t.emitsItemEvents && rowOwner != "" {
		m.emit(rowOwner, "", "item_status_changed", status)
	}
	if status == "failed" || status == "cancelled" {
		if !t.releasesOnFailure {
			return nil
		}
		// The owner comes back from the row the statement just wrote, not from
		// the caller. A table whose rows do not carry their task - the message
		// table, keyed by the media alone - has no owner to hand in, and a
		// release that names the wrong one deletes nothing and reports success
		// while the claim a waiting task is stuck behind stays exactly where it
		// was.
		if _, err := m.db.Exec(`DELETE FROM downloaded_media WHERE dialog_key = ? AND message_id = ? AND status = 'claimed' AND owner_kind = ? AND owner_id = ?`, item.DialogKey, item.MessageID, t.ownerKind, rowOwner); err != nil {
			return err
		}
		// Releasing the claim hands this media to whichever task is waiting on it.
		m.signalReconcile()
		return nil
	}
	if status == "completed" && path != "" {
		// The ownership row is what a waiting task adopts, whichever kind it is:
		// a session item waits on a claim a message task holds, and the other way
		// round. Both halves have to say so, or the one that stays quiet is
		// promoted only on the reconciler's next pass. The message half did stay
		// quiet, which is a difference this merge had to resolve rather than
		// preserve - signalling is one coalesced wake-up, and the reconciler it
		// wakes is already running on its own cadence.
		m.signalReconcile()
	}
	return nil
}

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

// publish moves a finished file into place and records what happened to it.
//
// It is the second half of a file's life - the state writer above records what
// the task knows, this decides what the filesystem holds - and the two task
// kinds differ in exactly two ways around it, both of which are passed in here
// rather than written out twice:
//
//   - the lock, which serialises this move against the control action that
//     would pause or cancel the task underneath it;
//   - the runnable check, which is what stops a file being moved into place for
//     a task a person has paused.
//
// Everything else is the same answer for both: a file that is not the size it
// claims to be is not published but failed, a destination that is already taken
// is reported as taken, and the chosen path is written down before the
// irreversible move so a database that goes away mid-move is recoverable by
// reconciliation rather than by downloading the media again.
func (m *Manager) publish(t itemTable, lock *sync.Mutex, runnable func() bool, ownerID, path string, item source, config settings.Values) error {
	lock.Lock()
	defer lock.Unlock()
	if !runnable() {
		return nil
	}
	if problem := incompleteFileError(item, path); problem != nil {
		// The state first, then the file. The other order leaves a partial file
		// on disk with the row still saying the file is being fetched, which is
		// the state nothing revisits; this way a crash in between leaves a
		// stray working file, and the working directory is cleaned up on every
		// path that ends a task.
		if err := m.setItemState(t, ownerID, item, "failed", "", problem.Error()); err != nil {
			return err
		}
		// The partial file is useless to a later attempt - a transfer rewrites
		// its temporary file from the first byte - so leaving it only leaves
		// something a person would mistake for a download.
		_ = os.Remove(path)
		return problem
	}
	finalPath, err := finalDestination(m.downloadDir, config.Download.FinalFilenameTemplate, item)
	if err != nil {
		if saveErr := m.setItemState(t, ownerID, item, "failed", "", err.Error()); saveErr != nil {
			return saveErr
		}
		return err
	}
	if occupied, regularFile := destinationOccupied(finalPath); occupied {
		message := "目标文件已存在，未覆盖"
		if !regularFile {
			message = blockedDestinationMessage(finalPath)
		}
		if err := m.setItemState(t, ownerID, item, "failed", "", message); err != nil {
			return err
		}
		return errors.New(message)
	}
	// Persist the selected final path before the irreversible move. If the
	// database disconnects after a successful move, reconciliation can verify
	// the path instead of downloading this media again.
	if err := m.setItemState(t, ownerID, item, "downloaded", finalPath, ""); err != nil {
		return fmt.Errorf("保存待发布状态: %w", err)
	}
	if err := publishNoReplace(path, finalPath); err != nil {
		message := err.Error()
		if errors.Is(err, unix.EEXIST) {
			// A name taken between the check above and the move is the same
			// answer as one taken before it, and it is reported the same way:
			// the move is atomic against other movers, not against a person
			// dropping a file into the directory. Reporting the raw error
			// instead loses the distinction that tells an operator whether the
			// destination holds an ordinary file or something the move must
			// never replace.
			message = "目标文件已存在，未覆盖"
			if occupied, regularFile := destinationOccupied(finalPath); occupied && !regularFile {
				message = blockedDestinationMessage(finalPath)
			}
		}
		if saveErr := m.setItemState(t, ownerID, item, "failed", "", message); saveErr != nil {
			return saveErr
		}
		return errors.New(message)
	}
	return m.setItemState(t, ownerID, item, "completed", finalPath, "")
}

// pauseStatement moves a task's unfinished files to paused.
//
// The status list is the whole point of this builder. It was written once per
// table, and the session copy was missing 'waiting': a file waiting on another
// task's claim is still this task's work, and one left out of the list is not
// paused, so 恢复 - which asks for paused rows - never picks it up. The two
// lists are now one list.
func (t itemTable) pauseStatement() string {
	return fmt.Sprintf(`UPDATE %[1]s
SET status = 'paused', error = '',
    elapsed_ms = elapsed_ms + CASE WHEN started_at != '' THEN FLOOR(EXTRACT(EPOCH FROM (?::timestamptz - started_at::timestamptz)) * 1000)::BIGINT ELSE 0 END,
    started_at = '', finished_at = ''
WHERE %[2]s AND status IN ('queued', 'waiting', 'running', 'downloaded')`, t.items, t.ownerPredicate())
}

// requeueStatement hands a task's files back to the queue.
//
// pausedOnly is the one place the two task kinds ask for different things, and
// it is a decision rather than a difference in the tables: 恢复 gives back what
// the pause took, while 重试 gives back everything that did not finish, because
// the person asking for a retry is asking about the failures. Both reset the
// attempt budget, for the same reason - leaving the counter at its cap made the
// next stall give up on the file at once, so the click changed nothing.
func (t itemTable) requeueStatement(pausedOnly bool) string {
	scope := "status != 'completed'"
	if pausedOnly {
		scope = "status = 'paused'"
	}
	return fmt.Sprintf(`UPDATE %[1]s SET status = 'queued', error = '', started_at = '', finished_at = '', elapsed_ms = 0, attempts = 0 WHERE %[2]s AND %[3]s`, t.items, t.ownerPredicate(), scope)
}

// cancelStatement is the same for 取消, which is terminal for the files as well.
func (t itemTable) cancelStatement() string {
	return fmt.Sprintf(`UPDATE %[1]s SET status = 'cancelled', finished_at = ? WHERE %[2]s AND status != 'completed'`, t.items, t.ownerPredicate())
}

// releaseClaimsIn drops every media claim a task still owns, inside the
// transaction that is stopping it.
//
// Inside, because the alternative has a window: a process that dies between the
// commit and the release leaves claims held by a task that will never run
// again, and every other task wanting that media waits behind a claim nothing
// will release until the periodic repair sweep notices, a minute later. The
// session path has always done it this way; the message path did not.
func releaseClaimsIn(t itemTable, ownerID string) func(*databaseTx) error {
	return func(tx *databaseTx) error {
		_, err := tx.Exec(`DELETE FROM downloaded_media WHERE status = 'claimed' AND owner_kind = ? AND owner_id = ?`, t.ownerKind, ownerID)
		return err
	}
}
