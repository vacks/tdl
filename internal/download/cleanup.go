package download

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/vacks/tdl/internal/applog"
)

const (
	cleanupBatchSize              = 1000
	auxiliaryHistoryRetentionDays = 30
)

type CleanupResult struct {
	Jobs           int64 `json:"jobs"`
	ChatJobs       int64 `json:"chatJobs"`
	Requests       int64 `json:"requests"`
	ReactionEvents int64 `json:"reactionEvents"`
	ChatMessages   int64 `json:"chatMessages"`
	Resets         int64 `json:"resets"`
	BotMessages    int64 `json:"botMessages"`
}

func (m *Manager) cleanupLoop(ctx context.Context) {
	run := func() {
		// Auxiliary event/audit data is intentionally cleaned in the background.
		// A first pass at startup makes the 30-day retention policy effective
		// even for installations that restart more often than once per day.
		result, err := m.cleanupPostgresHistory(auxiliaryHistoryRetentionDays)
		if err != nil {
			applog.Error("cleanup", "scheduled_cleanup_failed", "error", err.Error())
			return
		}
		removed, err := m.cleanupWorkingDirectories()
		if err != nil {
			applog.Error("cleanup", "working_directory_sweep_failed", "error", err.Error())
		}
		applog.Info("cleanup", "scheduled_cleanup_completed", "retention_days", auxiliaryHistoryRetentionDays, "chat_messages", result.ChatMessages, "working_directories_removed", removed)
	}
	run()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

// workingDirectoryBackstopAge is when a working directory is removed although
// its task cannot be identified at all. It is the last resort for a directory
// whose name matches no task: a week of it is cheaper than the chance of
// deleting one a live transfer is using.
const workingDirectoryBackstopAge = 7 * 24 * time.Hour

// terminalTaskStatuses are the states in which no transfer is running for a
// task. A working directory whose task is in one of them is finished with.
//
// The list is written out rather than expressed as "not running", because a
// status added later would silently be treated as live and its directories
// would never be swept.
var terminalTaskStatuses = []string{"completed", "failed", "cancelled", "deleted", "partial"}

// cleanupWorkingDirectories removes download working directories whose task has
// finished or no longer exists.
//
// A transfer now removes its own directory however it ends, so what this finds
// is what a crash or a kill left behind: the files of a transfer that never
// finished, under the id of a task that is over. Nothing else ever removed
// those, so they accumulated one per interrupted task, forever.
//
// Each directory costs one primary-key lookup and nothing else. The sweep must
// not scan the job tables: at the scale this is built for they hold tens of
// millions of rows, while the directories number however many tasks were
// interrupted, which is a handful.
func (m *Manager) cleanupWorkingDirectories() (int, error) {
	root := filepath.Join(m.downloadDir, ".tdl-tmp")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// A chat task's directory is named "chat-<id>"; a message task's is its
		// id. The name does not say which kind it is, so both are asked.
		id := strings.TrimPrefix(entry.Name(), "chat-")
		switch m.taskLiveness(id) {
		case taskLive:
			continue
		case taskUnknown:
			// Nothing identifies this directory. It is left alone until it is old
			// enough that no transfer could still be writing into it.
			if info, err := entry.Info(); err == nil && time.Since(info.ModTime()) < workingDirectoryBackstopAge {
				continue
			}
		}
		path := filepath.Join(root, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			applog.Warn("cleanup", "working_directory_not_removed", "path", path, "error", err.Error())
			continue
		}
		removed++
	}
	return removed, nil
}

type taskLiveness int

const (
	// taskFinished means every row for the id is in a terminal state.
	taskFinished taskLiveness = iota
	// taskLive means at least one row for the id is still working.
	taskLive
	// taskUnknown means there is no row for the id under that name. It is what a
	// directory belonging to a deleted task looks like, and what a directory
	// nobody can account for looks like.
	taskUnknown
)

// taskLiveness reports what the database knows about one id.
func (m *Manager) taskLiveness(id string) taskLiveness {
	rows, err := m.db.Query(`SELECT status FROM download_jobs WHERE id = ? UNION ALL SELECT status FROM chat_download_jobs WHERE id = ?`, id, id)
	if err != nil {
		// An unreadable database is not evidence that a task is over. The
		// directory is left where it is and swept on the next pass.
		return taskLive
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return taskLive
		}
		found = true
		if !slices.Contains(terminalTaskStatuses, status) {
			return taskLive
		}
	}
	if err := rows.Err(); err != nil {
		return taskLive
	}
	if !found {
		return taskUnknown
	}
	return taskFinished
}

func (m *Manager) cleanupPostgresHistory(retentionDays int) (CleanupResult, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Format(time.RFC3339Nano)
	result := CleanupResult{}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM download_requests WHERE id IN (SELECT id FROM download_requests WHERE created_at < ? ORDER BY created_at, id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.Requests += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM reaction_inbox WHERE id IN (SELECT id FROM reaction_inbox WHERE `+eventRetentionStatuses+` AND updated_at < ? ORDER BY updated_at, id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.ReactionEvents += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM chat_message_inbox WHERE id IN (SELECT id FROM chat_message_inbox WHERE `+eventRetentionStatuses+` AND updated_at < ? ORDER BY updated_at, id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.ChatMessages += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM download_resets WHERE (account_id, source_url) IN (SELECT account_id, source_url FROM download_resets WHERE created_at < ? ORDER BY created_at LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.Resets += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM bot_lifecycle_messages WHERE (job_id, chat_id) IN (SELECT b.job_id, b.chat_id FROM bot_lifecycle_messages b JOIN download_jobs j ON j.id = b.job_id WHERE j.status IN ('completed', 'failed', 'partial', 'cancelled', 'deleted') AND j.updated_at < ? ORDER BY j.updated_at, b.job_id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.BotMessages += count
		if count == 0 {
			break
		}
	}
	if result.Requests > 0 || result.ReactionEvents > 0 || result.ChatMessages > 0 || result.Resets > 0 || result.BotMessages > 0 {
		m.touch()
	}
	return result, nil
}

func (m *Manager) cleanupPostgresBatch(query, cutoff string) (int64, error) {
	result, err := m.db.Exec(query, cutoff, cleanupBatchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
