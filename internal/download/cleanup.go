package download

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vacks/tdl/internal/applog"
)

const cleanupBatchSize = 1000

type CleanupResult struct {
	Jobs           int64 `json:"jobs"`
	ChatJobs       int64 `json:"chatJobs"`
	Requests       int64 `json:"requests"`
	Events         int64 `json:"events"`
	ReactionEvents int64 `json:"reactionEvents"`
	Resets         int64 `json:"resets"`
}

func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		retention := m.settings.Get().Cleanup.RetentionDays
		result, err := m.CleanupHistory(retention)
		if err != nil {
			applog.Error("cleanup", "scheduled_cleanup_failed", "error", err.Error())
			continue
		}
		applog.Info("cleanup", "scheduled_cleanup_completed", "retention_days", retention, "jobs", result.Jobs, "events", result.Events)
	}
}

// CleanupHistory removes terminal database history older than retentionDays.
// It never removes final files under the download directory.
func (m *Manager) CleanupHistory(retentionDays int) (CleanupResult, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Format(time.RFC3339Nano)
	result := CleanupResult{}
	for {
		batch, err := m.cleanupJobsBatch(cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.Jobs += batch.Jobs
		result.Requests += batch.Requests
		result.Events += batch.Events
		if batch.Jobs == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupChatJobsBatch(cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.ChatJobs += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupSimpleBatch(`SELECT id FROM reaction_inbox WHERE status IN ('done', 'failed') AND updated_at < ? ORDER BY id LIMIT ?`, `DELETE FROM reaction_inbox WHERE id IN (%s)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.ReactionEvents += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupSimpleBatch(`SELECT rowid FROM download_resets WHERE created_at < ? ORDER BY rowid LIMIT ?`, `DELETE FROM download_resets WHERE rowid IN (%s)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.Resets += count
		if count == 0 {
			break
		}
	}
	if result.Jobs > 0 || result.ChatJobs > 0 || result.Requests > 0 || result.Events > 0 || result.ReactionEvents > 0 {
		m.touch()
	}
	return result, nil
}

// cleanupChatJobsBatch removes a terminal chat parent together with its
// hidden child download jobs. It is deliberately separate from the normal
// task cleanup so an indexed conversation cannot leave millions of orphaned
// chat_download_items behind after its retention period expires.
func (m *Manager) cleanupChatJobsBatch(cutoff string) (int64, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT id FROM chat_download_jobs WHERE status IN ('completed', 'failed', 'partial', 'cancelled') AND updated_at < ? ORDER BY updated_at, id LIMIT ?`, cutoff, cleanupBatchSize)
	if err != nil {
		return 0, err
	}
	chatIDs := make([]string, 0, cleanupBatchSize)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		chatIDs = append(chatIDs, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(chatIDs) == 0 {
		return 0, nil
	}
	marks, args := placeholders(chatIDs)
	// A retained overlapping chat task may still reference a child created by
	// a parent selected for cleanup. Only remove children that have no such
	// external reference; the parent-row cascade below removes this task's
	// index rows either way.
	childIDs, err := chatExclusiveChildJobIDs(tx, chatIDs)
	if err != nil {
		return 0, err
	}
	if len(childIDs) > 0 {
		childMarks, childArgs := placeholders(childIDs)
		if _, err := tx.Exec(`DELETE FROM download_events WHERE job_id IN (`+childMarks+`)`, childArgs...); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM download_requests WHERE job_id IN (`+childMarks+`)`, childArgs...); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM bot_lifecycle_messages WHERE job_id IN (`+childMarks+`)`, childArgs...); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM download_items WHERE job_id IN (`+childMarks+`)`, childArgs...); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM download_jobs WHERE id IN (`+childMarks+`)`, childArgs...); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(`DELETE FROM chat_download_jobs WHERE id IN (`+marks+`)`, args...); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(chatIDs)), nil
}

// cleanupJobsBatch holds SQLite's write lock only while deleting a bounded
// number of terminal jobs and their dependent rows. This keeps large history
// cleanups responsive for workers and the Web UI.
func (m *Manager) cleanupJobsBatch(cutoff string) (CleanupResult, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return CleanupResult{}, err
	}
	defer tx.Rollback()
	// A normal job can be attached to one or more chat histories through global
	// message de-duplication.  Keep it until the chat cleanup pass decides
	// whether every referencing parent has expired; otherwise the retained chat
	// would be left pointing at a missing child and lose its progress state.
	rows, err := tx.Query(`SELECT j.id FROM download_jobs j
 WHERE j.status IN ('completed', 'failed', 'partial', 'cancelled')
   AND j.updated_at < ?
   AND NOT EXISTS (SELECT 1 FROM chat_download_items i WHERE i.child_job_id = j.id)
 ORDER BY j.updated_at, j.id LIMIT ?`, cutoff, cleanupBatchSize)
	if err != nil {
		return CleanupResult{}, err
	}
	ids := make([]string, 0, cleanupBatchSize)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return CleanupResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return CleanupResult{}, err
	}
	if len(ids) == 0 {
		return CleanupResult{}, nil
	}
	placeholders, args := placeholders(ids)
	result := CleanupResult{Jobs: int64(len(ids))}
	if result.Events, err = deleteCount(tx, `DELETE FROM download_events WHERE job_id IN (`+placeholders+`)`, args...); err != nil {
		return CleanupResult{}, err
	}
	if result.Requests, err = deleteCount(tx, `DELETE FROM download_requests WHERE job_id IN (`+placeholders+`)`, args...); err != nil {
		return CleanupResult{}, err
	}
	if _, err = tx.Exec(`DELETE FROM bot_lifecycle_messages WHERE job_id IN (`+placeholders+`)`, args...); err != nil {
		return CleanupResult{}, err
	}
	if _, err = tx.Exec(`DELETE FROM download_items WHERE job_id IN (`+placeholders+`)`, args...); err != nil {
		return CleanupResult{}, err
	}
	if _, err = tx.Exec(`DELETE FROM download_jobs WHERE id IN (`+placeholders+`)`, args...); err != nil {
		return CleanupResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CleanupResult{}, err
	}
	return result, nil
}

func (m *Manager) cleanupSimpleBatch(selectSQL, deleteTemplate, cutoff string) (int64, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(selectSQL, cutoff, cleanupBatchSize)
	if err != nil {
		return 0, err
	}
	ids := make([]int64, 0, cleanupBatchSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	marks := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i := range ids {
		args[i] = ids[i]
	}
	count, err := deleteCount(tx, fmt.Sprintf(deleteTemplate, marks), args...)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func placeholders(ids []string) (string, []any) {
	marks := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i := range ids {
		args[i] = ids[i]
	}
	return marks, args
}

func deleteCount(tx *sql.Tx, query string, args ...any) (int64, error) {
	result, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
