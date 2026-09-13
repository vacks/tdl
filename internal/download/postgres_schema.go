package download

import "fmt"

// migratePostgres creates the durable PostgreSQL schema. Download jobs and
// item identities are intentionally never removed by retention maintenance:
// they are the permanent history and de-duplication source of truth.
func (m *Manager) migratePostgres() error {
	if !m.db.isPostgres() {
		return m.migrateSQLiteLegacy()
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS download_jobs (
 id TEXT PRIMARY KEY, source_url TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL DEFAULT '', dialog_name TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '', direct_peer_type TEXT NOT NULL DEFAULT '', direct_peer_id BIGINT NOT NULL DEFAULT 0, direct_peer_hash BIGINT NOT NULL DEFAULT 0, parent_chat_id TEXT NOT NULL DEFAULT '', config_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS download_items (
 id BIGSERIAL PRIMARY KEY, job_id TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL, dialog_id BIGINT NOT NULL, message_id INTEGER NOT NULL, grouped_id BIGINT NOT NULL DEFAULT 0,
 message_text TEXT NOT NULL DEFAULT '', original_name TEXT NOT NULL, size BIGINT NOT NULL DEFAULT 0, final_path TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '', elapsed_ms BIGINT NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
 UNIQUE(dialog_key, message_id), FOREIGN KEY(job_id) REFERENCES download_jobs(id)
)`,
		`CREATE TABLE IF NOT EXISTS bot_lifecycle_messages (
 job_id TEXT NOT NULL, chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL, message_text TEXT NOT NULL DEFAULT '', token_hash TEXT NOT NULL DEFAULT '', PRIMARY KEY(job_id, chat_id), FOREIGN KEY(job_id) REFERENCES download_jobs(id) ON DELETE CASCADE
)`,
		`CREATE TABLE IF NOT EXISTS app_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS download_resets (account_id TEXT NOT NULL, source_url TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(account_id, source_url))`,
		`CREATE TABLE IF NOT EXISTS download_requests (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL, source_kind TEXT NOT NULL, account_id TEXT NOT NULL, source_url TEXT NOT NULL DEFAULT '', dialog_key TEXT NOT NULL DEFAULT '', message_id INTEGER NOT NULL DEFAULT 0, trigger_json TEXT NOT NULL DEFAULT '{}', outcome TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(job_id) REFERENCES download_jobs(id)
)`,
		`CREATE TABLE IF NOT EXISTS download_events (
 id BIGSERIAL PRIMARY KEY, job_id TEXT NOT NULL, request_id TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, FOREIGN KEY(job_id) REFERENCES download_jobs(id)
)`,
		`CREATE TABLE IF NOT EXISTS reaction_inbox (
 id BIGSERIAL PRIMARY KEY, account_id TEXT NOT NULL, dialog_key TEXT NOT NULL, dialog_name TEXT NOT NULL DEFAULT '', dialog_id BIGINT NOT NULL DEFAULT 0, message_id INTEGER NOT NULL, source_url TEXT NOT NULL DEFAULT '', peer_type TEXT NOT NULL, peer_id BIGINT NOT NULL DEFAULT 0, peer_hash BIGINT NOT NULL DEFAULT 0, emoji TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(account_id, dialog_key, message_id, emoji)
)`,
		`CREATE TABLE IF NOT EXISTS chat_download_jobs (
 id TEXT PRIMARY KEY, source_url TEXT NOT NULL, dialog_type TEXT NOT NULL, dialog_key TEXT NOT NULL, dialog_id BIGINT NOT NULL, dialog_name TEXT NOT NULL, account_id TEXT NOT NULL, direct_peer_type TEXT NOT NULL DEFAULT '', direct_peer_id BIGINT NOT NULL DEFAULT 0, direct_peer_hash BIGINT NOT NULL DEFAULT 0, start_message_id INTEGER NOT NULL DEFAULT 0, upper_message_id INTEGER NOT NULL DEFAULT 0, listen_new SMALLINT NOT NULL DEFAULT 0, status TEXT NOT NULL, scan_state TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', config_json TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS chat_download_items (
 chat_job_id TEXT NOT NULL, dialog_key TEXT NOT NULL, message_id INTEGER NOT NULL, child_job_id TEXT NOT NULL DEFAULT '', dialog_type TEXT NOT NULL DEFAULT '', dialog_id BIGINT NOT NULL DEFAULT 0, grouped_id BIGINT NOT NULL DEFAULT 0, message_text TEXT NOT NULL DEFAULT '', original_name TEXT NOT NULL DEFAULT '', size BIGINT NOT NULL DEFAULT 0, discovered_at TEXT NOT NULL, PRIMARY KEY(chat_job_id, dialog_key, message_id), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
)`,
		`CREATE TABLE IF NOT EXISTS chat_download_streams (
 chat_job_id TEXT NOT NULL, stream_kind TEXT NOT NULL, offset_message_id INTEGER NOT NULL DEFAULT 0, initialized SMALLINT NOT NULL DEFAULT 0, completed SMALLINT NOT NULL DEFAULT 0, PRIMARY KEY(chat_job_id, stream_kind), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
)`,
		`CREATE INDEX IF NOT EXISTS download_items_job_id ON download_items(job_id)`,
		`CREATE INDEX IF NOT EXISTS download_items_status ON download_items(status)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_created_id ON download_jobs(created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_status_updated_id ON download_jobs(status, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_parent_created_id ON download_jobs(parent_chat_id, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS download_requests_job_id ON download_requests(job_id)`,
		`CREATE INDEX IF NOT EXISTS download_requests_created_id ON download_requests(created_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_events_job_id ON download_events(job_id, id)`,
		`CREATE INDEX IF NOT EXISTS download_events_created_id ON download_events(created_at, id)`,
		`CREATE INDEX IF NOT EXISTS reaction_inbox_ready ON reaction_inbox(status, next_attempt_at, id)`,
		`CREATE INDEX IF NOT EXISTS reaction_inbox_cleanup ON reaction_inbox(status, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_resets_created ON download_resets(created_at)`,
		`CREATE INDEX IF NOT EXISTS bot_lifecycle_messages_cleanup ON bot_lifecycle_messages(job_id, chat_id)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_created_id ON chat_download_jobs(created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_account_status ON chat_download_jobs(account_id, status, updated_at)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_scan ON chat_download_jobs(status, scan_state, created_at)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_listener ON chat_download_jobs(account_id, dialog_key, listen_new, scan_state, status)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_active_target ON chat_download_jobs(account_id, dialog_key, start_message_id, status)`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_job ON chat_download_items(chat_job_id)`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_child_job ON chat_download_items(child_job_id)`,
	}
	for _, statement := range statements {
		if _, err := m.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize PostgreSQL schema: %w", err)
		}
	}
	return nil
}
