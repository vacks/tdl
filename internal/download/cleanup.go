package download

import (
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
	Events         int64 `json:"events"`
	ReactionEvents int64 `json:"reactionEvents"`
	Resets         int64 `json:"resets"`
	BotMessages    int64 `json:"botMessages"`
}

func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		result, err := m.cleanupPostgresHistory(auxiliaryHistoryRetentionDays)
		if err != nil {
			applog.Error("cleanup", "scheduled_cleanup_failed", "error", err.Error())
			continue
		}
		applog.Info("cleanup", "scheduled_cleanup_completed", "retention_days", auxiliaryHistoryRetentionDays, "jobs", result.Jobs, "events", result.Events)
	}
}

func (m *Manager) cleanupPostgresHistory(retentionDays int) (CleanupResult, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Format(time.RFC3339Nano)
	result := CleanupResult{}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM download_events WHERE id IN (SELECT id FROM download_events WHERE created_at < ? ORDER BY id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.Events += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM download_requests WHERE id IN (SELECT id FROM download_requests WHERE created_at < ? ORDER BY id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.Requests += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := m.cleanupPostgresBatch(`DELETE FROM reaction_inbox WHERE id IN (SELECT id FROM reaction_inbox WHERE status IN ('done', 'failed') AND updated_at < ? ORDER BY id LIMIT ?)`, cutoff)
		if err != nil {
			return CleanupResult{}, err
		}
		result.ReactionEvents += count
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
	if result.Events > 0 || result.Requests > 0 || result.ReactionEvents > 0 || result.Resets > 0 || result.BotMessages > 0 {
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
