package download

import (
	"fmt"
	"time"
)

// migratePostgres creates the durable PostgreSQL schema. Download jobs and
// item identities are intentionally never removed by retention maintenance:
// they are the permanent history and de-duplication source of truth.
func (m *Manager) migratePostgres() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS download_jobs (
 id TEXT PRIMARY KEY, source_url TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL DEFAULT '', dialog_name TEXT NOT NULL DEFAULT '', account_id TEXT NOT NULL DEFAULT '', direct_peer_type TEXT NOT NULL DEFAULT '', direct_peer_id BIGINT NOT NULL DEFAULT 0, direct_peer_hash BIGINT NOT NULL DEFAULT 0, parent_chat_id TEXT NOT NULL DEFAULT '', config_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS download_items (
 id BIGSERIAL PRIMARY KEY, job_id TEXT NOT NULL, dialog_type TEXT NOT NULL DEFAULT 'legacy', dialog_key TEXT NOT NULL, dialog_id BIGINT NOT NULL, message_id INTEGER NOT NULL, grouped_id BIGINT NOT NULL DEFAULT 0,
 message_text TEXT NOT NULL DEFAULT '', origin_dialog_name TEXT NOT NULL DEFAULT '', origin_message_id INTEGER NOT NULL DEFAULT 0, is_comment SMALLINT NOT NULL DEFAULT 0, source_peer_type TEXT NOT NULL DEFAULT '', source_peer_id BIGINT NOT NULL DEFAULT 0, source_peer_hash BIGINT NOT NULL DEFAULT 0, original_name TEXT NOT NULL, size BIGINT NOT NULL DEFAULT 0, final_path TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '', elapsed_ms BIGINT NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
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
 chat_job_id TEXT NOT NULL, dialog_key TEXT NOT NULL, message_id INTEGER NOT NULL, child_job_id TEXT NOT NULL DEFAULT '', dialog_type TEXT NOT NULL DEFAULT '', dialog_id BIGINT NOT NULL DEFAULT 0, grouped_id BIGINT NOT NULL DEFAULT 0, message_text TEXT NOT NULL DEFAULT '', origin_dialog_name TEXT NOT NULL DEFAULT '', origin_message_id INTEGER NOT NULL DEFAULT 0, is_comment SMALLINT NOT NULL DEFAULT 0, source_peer_type TEXT NOT NULL DEFAULT '', source_peer_id BIGINT NOT NULL DEFAULT 0, source_peer_hash BIGINT NOT NULL DEFAULT 0, original_name TEXT NOT NULL DEFAULT '', size BIGINT NOT NULL DEFAULT 0, final_path TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '', elapsed_ms BIGINT NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'queued', error TEXT NOT NULL DEFAULT '', discovered_at TEXT NOT NULL, PRIMARY KEY(chat_job_id, dialog_key, message_id), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
)`,
		// chat_download_stats is a derived, rebuildable cache for presentation and
		// parent-state decisions. chat_download_items remains the only source of
		// truth for file ownership, de-duplication, and recovery.
		`CREATE TABLE IF NOT EXISTS chat_download_stats (
 chat_job_id TEXT PRIMARY KEY, discovered BIGINT NOT NULL DEFAULT 0, queued BIGINT NOT NULL DEFAULT 0, waiting BIGINT NOT NULL DEFAULT 0, running BIGINT NOT NULL DEFAULT 0, downloaded BIGINT NOT NULL DEFAULT 0, completed BIGINT NOT NULL DEFAULT 0, failed BIGINT NOT NULL DEFAULT 0, paused BIGINT NOT NULL DEFAULT 0, cancelled BIGINT NOT NULL DEFAULT 0, earliest_message_id INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT '', FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
)`,
		`CREATE TABLE IF NOT EXISTS chat_download_streams (
 chat_job_id TEXT NOT NULL, stream_kind TEXT NOT NULL, offset_message_id INTEGER NOT NULL DEFAULT 0, initialized SMALLINT NOT NULL DEFAULT 0, completed SMALLINT NOT NULL DEFAULT 0, PRIMARY KEY(chat_job_id, stream_kind), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
	)`,
		`CREATE TABLE IF NOT EXISTS chat_message_inbox (
 id BIGSERIAL PRIMARY KEY, account_id TEXT NOT NULL, dialog_key TEXT NOT NULL, dialog_name TEXT NOT NULL DEFAULT '', dialog_id BIGINT NOT NULL DEFAULT 0, message_id INTEGER NOT NULL, reply_to_message_id INTEGER NOT NULL DEFAULT 0, reply_to_top_id INTEGER NOT NULL DEFAULT 0,
 peer_type TEXT NOT NULL, peer_id BIGINT NOT NULL DEFAULT 0, peer_hash BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(account_id, dialog_key, message_id)
)`,
		`CREATE TABLE IF NOT EXISTS chat_reply_roots (
 chat_job_id TEXT NOT NULL, account_id TEXT NOT NULL, discussion_dialog_key TEXT NOT NULL, root_message_id INTEGER NOT NULL, origin_message_id INTEGER NOT NULL, discussion_peer_type TEXT NOT NULL DEFAULT '', discussion_peer_id BIGINT NOT NULL DEFAULT 0, discussion_peer_hash BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(chat_job_id, discussion_dialog_key, root_message_id), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
)`,
		`CREATE TABLE IF NOT EXISTS downloaded_media (
 dialog_key TEXT NOT NULL, message_id INTEGER NOT NULL, final_path TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, owner_kind TEXT NOT NULL DEFAULT '', owner_id TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, PRIMARY KEY(dialog_key, message_id)
)`,
		`CREATE TABLE IF NOT EXISTS telegram_rate_limits (
 account_id TEXT PRIMARY KEY, blocked_until TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS download_items_job_id ON download_items(job_id)`,
		`CREATE INDEX IF NOT EXISTS download_items_queued_job_id ON download_items(job_id) WHERE status = 'queued'`,
		`CREATE INDEX IF NOT EXISTS download_items_status ON download_items(status)`,
		`CREATE INDEX IF NOT EXISTS download_items_failed_finished_at ON download_items(status, finished_at) WHERE status = 'failed'`,
		`CREATE INDEX IF NOT EXISTS download_jobs_created_id ON download_jobs(created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_visible_created_id ON download_jobs(created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,
		`CREATE INDEX IF NOT EXISTS download_jobs_visible_status_created_id ON download_jobs(status, created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,
		`CREATE INDEX IF NOT EXISTS download_jobs_status_updated_id ON download_jobs(status, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_jobs_parent_created_id ON download_jobs(parent_chat_id, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS download_requests_job_id ON download_requests(job_id)`,
		`CREATE INDEX IF NOT EXISTS download_requests_created_id ON download_requests(created_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_events_job_id ON download_events(job_id, id)`,
		`CREATE INDEX IF NOT EXISTS download_events_created_id ON download_events(created_at, id)`,
		`CREATE INDEX IF NOT EXISTS reaction_inbox_ready ON reaction_inbox(status, next_attempt_at, id)`,
		`CREATE INDEX IF NOT EXISTS reaction_inbox_cleanup ON reaction_inbox(status, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS download_resets_created ON download_resets(created_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS chat_download_jobs_active_unique ON chat_download_jobs(account_id, dialog_key, start_message_id) WHERE status IN ('queued', 'scanning', 'downloading', 'listening', 'paused')`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_created_id ON chat_download_jobs(created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_visible_created_id ON chat_download_jobs(created_at DESC, id DESC) WHERE status != 'deleted'`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_account_status ON chat_download_jobs(account_id, status, updated_at)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_scan ON chat_download_jobs(status, scan_state, created_at)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_listener ON chat_download_jobs(account_id, dialog_key, listen_new, scan_state, status)`,
		`CREATE INDEX IF NOT EXISTS chat_download_jobs_active_target ON chat_download_jobs(account_id, dialog_key, start_message_id, status)`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_job ON chat_download_items(chat_job_id)`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_child_job ON chat_download_items(child_job_id)`,
		`CREATE INDEX IF NOT EXISTS chat_message_inbox_ready ON chat_message_inbox(status, next_attempt_at, id)`,
		`CREATE INDEX IF NOT EXISTS chat_message_inbox_cleanup ON chat_message_inbox(status, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS chat_reply_roots_lookup ON chat_reply_roots(account_id, discussion_dialog_key, root_message_id)`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_failed_finished_at ON chat_download_items(status, finished_at) WHERE status = 'failed'`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_active_status ON chat_download_items(status) WHERE status IN ('queued', 'waiting', 'running', 'downloaded', 'paused')`,
		`CREATE INDEX IF NOT EXISTS chat_download_items_state_by_job ON chat_download_items(chat_job_id, status) WHERE status IN ('queued', 'running', 'downloaded', 'failed')`,
		`CREATE INDEX IF NOT EXISTS downloaded_media_status ON downloaded_media(status, updated_at)`,
		`CREATE INDEX IF NOT EXISTS telegram_rate_limits_blocked_until ON telegram_rate_limits(blocked_until)`,
	}
	for _, statement := range statements {
		if _, err := m.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize PostgreSQL schema: %w", err)
		}
	}
	if err := m.applyPostgresMigrations(); err != nil {
		return err
	}
	// This was an exact duplicate of the primary key (job_id, chat_id).
	if _, err := m.db.Exec(`DROP INDEX IF EXISTS bot_lifecycle_messages_cleanup`); err != nil {
		return fmt.Errorf("remove redundant PostgreSQL index: %w", err)
	}
	return nil
}

type postgresMigration struct {
	version    int
	statements []string
}

// Keep every post-v1 structure change here. The base CREATE IF NOT EXISTS
// statements bootstrap a fresh database; upgrades are recorded one version at
// a time so an existing PostgreSQL volume can never silently miss a change.
var postgresMigrations = []postgresMigration{
	{
		version: 1,
	},
	{
		version: 2,
		statements: []string{
			`CREATE INDEX IF NOT EXISTS download_jobs_account_visible_created_id ON download_jobs(account_id, created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,
		},
	},
	{
		// v3 makes media rows self-contained.  A chat task can therefore use the
		// index itself as its bounded work queue instead of manufacturing one
		// download_jobs row for every media message.
		version: 3,
		statements: []string{
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS final_path TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS started_at TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS finished_at TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS elapsed_ms BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'queued'`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS error TEXT NOT NULL DEFAULT ''`,
			`CREATE INDEX IF NOT EXISTS chat_download_items_ready ON chat_download_items(chat_job_id, status, message_id)`,
		},
	},
	{
		// v4 permanently severs the legacy parent/child execution relation.
		// The column stays temporarily so upgrading a live PostgreSQL volume is
		// non-destructive, but no current task can observe or control it.
		version: 4,
		statements: []string{
			`UPDATE download_items SET status = 'cancelled', error = '会话下载架构升级，旧子任务已停止', finished_at = CASE WHEN finished_at = '' THEN NOW()::text ELSE finished_at END WHERE job_id IN (SELECT id FROM download_jobs WHERE parent_chat_id <> '') AND status NOT IN ('completed', 'cancelled')`,
			`UPDATE download_jobs SET status = 'cancelled', error = '会话下载架构升级，旧子任务已停止', updated_at = NOW()::text WHERE parent_chat_id <> '' AND status IN ('queued', 'running', 'paused')`,
			`UPDATE chat_download_items SET child_job_id = '' WHERE child_job_id <> ''`,
			`UPDATE download_jobs SET parent_chat_id = '' WHERE parent_chat_id <> ''`,
		},
	},
	{
		version: 5,
		statements: []string{
			`CREATE TABLE IF NOT EXISTS downloaded_media (dialog_key TEXT NOT NULL, message_id INTEGER NOT NULL, final_path TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, owner_kind TEXT NOT NULL DEFAULT '', owner_id TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, PRIMARY KEY(dialog_key, message_id))`,
			`CREATE INDEX IF NOT EXISTS downloaded_media_status ON downloaded_media(status, updated_at)`,
			`INSERT INTO downloaded_media(dialog_key, message_id, final_path, status, owner_kind, owner_id, updated_at) SELECT dialog_key, message_id, final_path, status, 'message', job_id, NOW()::text FROM download_items WHERE status = 'completed' AND final_path <> '' ON CONFLICT(dialog_key, message_id) DO NOTHING`,
		},
	},
	{
		// v6 makes status-filtered task pages an index range scan even when the
		// permanent task history contains tens of millions of rows.
		version: 6,
		statements: []string{
			`CREATE INDEX IF NOT EXISTS download_jobs_visible_status_created_id ON download_jobs(status, created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,
		},
	},
	{
		version: 7,
		statements: []string{
			`CREATE TABLE IF NOT EXISTS chat_message_inbox (id BIGSERIAL PRIMARY KEY, account_id TEXT NOT NULL, dialog_key TEXT NOT NULL, dialog_name TEXT NOT NULL DEFAULT '', dialog_id BIGINT NOT NULL DEFAULT 0, message_id INTEGER NOT NULL, peer_type TEXT NOT NULL, peer_id BIGINT NOT NULL DEFAULT 0, peer_hash BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(account_id, dialog_key, message_id))`,
			`CREATE INDEX IF NOT EXISTS chat_message_inbox_ready ON chat_message_inbox(status, next_attempt_at, id)`,
			`CREATE INDEX IF NOT EXISTS chat_message_inbox_cleanup ON chat_message_inbox(status, updated_at, id)`,
			`CREATE INDEX IF NOT EXISTS chat_download_items_waiting ON chat_download_items(status, chat_job_id, message_id) WHERE status = 'waiting'`,
			`CREATE INDEX IF NOT EXISTS chat_download_items_downloaded ON chat_download_items(chat_job_id, message_id) WHERE status = 'downloaded'`,
		},
	},
	{
		// v8 keeps the dashboard's recent-failure figure an index range scan
		// for session-download items as well as ordinary message items.
		version: 8,
		statements: []string{
			`CREATE INDEX IF NOT EXISTS chat_download_items_failed_finished_at ON chat_download_items(status, finished_at) WHERE status = 'failed'`,
		},
	},
	{
		// v9 avoids scanning permanent completed chat history for the
		// dashboard's active count and the active-chat state reconciler.
		version: 9,
		statements: []string{
			`CREATE INDEX IF NOT EXISTS chat_download_items_active_status ON chat_download_items(status) WHERE status IN ('queued', 'waiting', 'running', 'downloaded', 'paused')`,
			`CREATE INDEX IF NOT EXISTS chat_download_items_state_by_job ON chat_download_items(chat_job_id, status) WHERE status IN ('queued', 'running', 'downloaded', 'failed')`,
		},
	},
	{
		// v10 replaces repeated full chat-item aggregates in the Web and Bot
		// presentation paths with a transactionally-maintained, rebuildable
		// summary. Statement-level triggers cover every existing direct item SQL
		// update, including batch pause/cancel/recovery operations, without
		// requiring each caller to remember a hand-written counter delta.
		version: 10,
		statements: []string{
			`CREATE TABLE IF NOT EXISTS chat_download_stats (
 chat_job_id TEXT PRIMARY KEY, discovered BIGINT NOT NULL DEFAULT 0, queued BIGINT NOT NULL DEFAULT 0, waiting BIGINT NOT NULL DEFAULT 0, running BIGINT NOT NULL DEFAULT 0, downloaded BIGINT NOT NULL DEFAULT 0, completed BIGINT NOT NULL DEFAULT 0, failed BIGINT NOT NULL DEFAULT 0, paused BIGINT NOT NULL DEFAULT 0, cancelled BIGINT NOT NULL DEFAULT 0, earliest_message_id INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT '', FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
)`,
			`CREATE OR REPLACE FUNCTION tdl_chat_stats_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO chat_download_stats(chat_job_id, discovered, queued, waiting, running, downloaded, completed, failed, paused, cancelled, earliest_message_id, updated_at)
 SELECT chat_job_id, COUNT(*), COUNT(*) FILTER (WHERE status = 'queued'), COUNT(*) FILTER (WHERE status = 'waiting'), COUNT(*) FILTER (WHERE status = 'running'), COUNT(*) FILTER (WHERE status = 'downloaded'), COUNT(*) FILTER (WHERE status = 'completed'), COUNT(*) FILTER (WHERE status = 'failed'), COUNT(*) FILTER (WHERE status = 'paused'), COUNT(*) FILTER (WHERE status = 'cancelled'), MIN(message_id), NOW()::text
 FROM new_rows GROUP BY chat_job_id
 ON CONFLICT (chat_job_id) DO UPDATE SET
  discovered = chat_download_stats.discovered + EXCLUDED.discovered,
  queued = chat_download_stats.queued + EXCLUDED.queued,
  waiting = chat_download_stats.waiting + EXCLUDED.waiting,
  running = chat_download_stats.running + EXCLUDED.running,
  downloaded = chat_download_stats.downloaded + EXCLUDED.downloaded,
  completed = chat_download_stats.completed + EXCLUDED.completed,
  failed = chat_download_stats.failed + EXCLUDED.failed,
  paused = chat_download_stats.paused + EXCLUDED.paused,
  cancelled = chat_download_stats.cancelled + EXCLUDED.cancelled,
  earliest_message_id = CASE WHEN chat_download_stats.earliest_message_id = 0 THEN EXCLUDED.earliest_message_id ELSE LEAST(chat_download_stats.earliest_message_id, EXCLUDED.earliest_message_id) END,
  updated_at = EXCLUDED.updated_at;
 RETURN NULL;
END $$`,
			`CREATE OR REPLACE FUNCTION tdl_chat_stats_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 WITH delta AS (
  SELECT chat_job_id, COUNT(*) AS discovered, COUNT(*) FILTER (WHERE status = 'queued') AS queued, COUNT(*) FILTER (WHERE status = 'waiting') AS waiting, COUNT(*) FILTER (WHERE status = 'running') AS running, COUNT(*) FILTER (WHERE status = 'downloaded') AS downloaded, COUNT(*) FILTER (WHERE status = 'completed') AS completed, COUNT(*) FILTER (WHERE status = 'failed') AS failed, COUNT(*) FILTER (WHERE status = 'paused') AS paused, COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled
  FROM old_rows GROUP BY chat_job_id
 )
 UPDATE chat_download_stats s SET
  discovered = GREATEST(0, s.discovered - d.discovered), queued = GREATEST(0, s.queued - d.queued), waiting = GREATEST(0, s.waiting - d.waiting), running = GREATEST(0, s.running - d.running), downloaded = GREATEST(0, s.downloaded - d.downloaded), completed = GREATEST(0, s.completed - d.completed), failed = GREATEST(0, s.failed - d.failed), paused = GREATEST(0, s.paused - d.paused), cancelled = GREATEST(0, s.cancelled - d.cancelled), updated_at = NOW()::text
 FROM delta d WHERE s.chat_job_id = d.chat_job_id;
 RETURN NULL;
END $$`,
			`CREATE OR REPLACE FUNCTION tdl_chat_stats_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 WITH delta AS (
  SELECT n.chat_job_id,
   COUNT(*) FILTER (WHERE n.status = 'queued') - COUNT(*) FILTER (WHERE o.status = 'queued') AS queued,
   COUNT(*) FILTER (WHERE n.status = 'waiting') - COUNT(*) FILTER (WHERE o.status = 'waiting') AS waiting,
   COUNT(*) FILTER (WHERE n.status = 'running') - COUNT(*) FILTER (WHERE o.status = 'running') AS running,
   COUNT(*) FILTER (WHERE n.status = 'downloaded') - COUNT(*) FILTER (WHERE o.status = 'downloaded') AS downloaded,
   COUNT(*) FILTER (WHERE n.status = 'completed') - COUNT(*) FILTER (WHERE o.status = 'completed') AS completed,
   COUNT(*) FILTER (WHERE n.status = 'failed') - COUNT(*) FILTER (WHERE o.status = 'failed') AS failed,
   COUNT(*) FILTER (WHERE n.status = 'paused') - COUNT(*) FILTER (WHERE o.status = 'paused') AS paused,
   COUNT(*) FILTER (WHERE n.status = 'cancelled') - COUNT(*) FILTER (WHERE o.status = 'cancelled') AS cancelled
  FROM new_rows n JOIN old_rows o USING (chat_job_id, dialog_key, message_id)
  GROUP BY n.chat_job_id
 )
 UPDATE chat_download_stats s SET
  queued = s.queued + d.queued, waiting = s.waiting + d.waiting, running = s.running + d.running, downloaded = s.downloaded + d.downloaded, completed = s.completed + d.completed, failed = s.failed + d.failed, paused = s.paused + d.paused, cancelled = s.cancelled + d.cancelled, updated_at = NOW()::text
 FROM delta d WHERE s.chat_job_id = d.chat_job_id;
 RETURN NULL;
END $$`,
			`DROP TRIGGER IF EXISTS tdl_chat_stats_insert_trigger ON chat_download_items`,
			`DROP TRIGGER IF EXISTS tdl_chat_stats_delete_trigger ON chat_download_items`,
			`DROP TRIGGER IF EXISTS tdl_chat_stats_update_trigger ON chat_download_items`,
			`CREATE TRIGGER tdl_chat_stats_insert_trigger AFTER INSERT ON chat_download_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION tdl_chat_stats_insert()`,
			`CREATE TRIGGER tdl_chat_stats_delete_trigger AFTER DELETE ON chat_download_items REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION tdl_chat_stats_delete()`,
			`CREATE TRIGGER tdl_chat_stats_update_trigger AFTER UPDATE ON chat_download_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION tdl_chat_stats_update()`,
			`INSERT INTO chat_download_stats(chat_job_id, discovered, queued, waiting, running, downloaded, completed, failed, paused, cancelled, earliest_message_id, updated_at)
 SELECT c.id, COUNT(i.message_id), COUNT(*) FILTER (WHERE i.status = 'queued'), COUNT(*) FILTER (WHERE i.status = 'waiting'), COUNT(*) FILTER (WHERE i.status = 'running'), COUNT(*) FILTER (WHERE i.status = 'downloaded'), COUNT(*) FILTER (WHERE i.status = 'completed'), COUNT(*) FILTER (WHERE i.status = 'failed'), COUNT(*) FILTER (WHERE i.status = 'paused'), COUNT(*) FILTER (WHERE i.status = 'cancelled'), COALESCE(MIN(i.message_id), 0), NOW()::text
 FROM chat_download_jobs c LEFT JOIN chat_download_items i ON i.chat_job_id = c.id GROUP BY c.id
 ON CONFLICT (chat_job_id) DO UPDATE SET discovered = EXCLUDED.discovered, queued = EXCLUDED.queued, waiting = EXCLUDED.waiting, running = EXCLUDED.running, downloaded = EXCLUDED.downloaded, completed = EXCLUDED.completed, failed = EXCLUDED.failed, paused = EXCLUDED.paused, cancelled = EXCLUDED.cancelled, earliest_message_id = EXCLUDED.earliest_message_id, updated_at = EXCLUDED.updated_at`,
		},
	},
	{
		version: 11,
		statements: []string{
			`ALTER TABLE download_items ADD COLUMN IF NOT EXISTS origin_dialog_name TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE download_items ADD COLUMN IF NOT EXISTS origin_message_id INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE download_items ADD COLUMN IF NOT EXISTS is_comment SMALLINT NOT NULL DEFAULT 0`,
			`ALTER TABLE download_items ADD COLUMN IF NOT EXISTS source_peer_type TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE download_items ADD COLUMN IF NOT EXISTS source_peer_id BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE download_items ADD COLUMN IF NOT EXISTS source_peer_hash BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS origin_dialog_name TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS origin_message_id INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS is_comment SMALLINT NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS source_peer_type TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS source_peer_id BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_download_items ADD COLUMN IF NOT EXISTS source_peer_hash BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_message_inbox ADD COLUMN IF NOT EXISTS reply_to_message_id INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_message_inbox ADD COLUMN IF NOT EXISTS reply_to_top_id INTEGER NOT NULL DEFAULT 0`,
			`CREATE INDEX IF NOT EXISTS chat_download_items_origin ON chat_download_items(chat_job_id, origin_message_id) WHERE is_comment = 1`,
			`UPDATE download_items i SET origin_dialog_name = j.dialog_name, origin_message_id = i.message_id FROM download_jobs j WHERE j.id = i.job_id AND i.origin_dialog_name = ''`,
			`UPDATE chat_download_items i SET origin_dialog_name = j.dialog_name, origin_message_id = i.message_id FROM chat_download_jobs j WHERE j.id = i.chat_job_id AND i.origin_dialog_name = ''`,
			`CREATE TABLE IF NOT EXISTS chat_reply_roots (chat_job_id TEXT NOT NULL, account_id TEXT NOT NULL, discussion_dialog_key TEXT NOT NULL, root_message_id INTEGER NOT NULL, origin_message_id INTEGER NOT NULL, PRIMARY KEY(chat_job_id, discussion_dialog_key, root_message_id), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE)`,
			`CREATE INDEX IF NOT EXISTS chat_reply_roots_lookup ON chat_reply_roots(account_id, discussion_dialog_key, root_message_id)`,
		},
	},
	{
		version: 12,
		statements: []string{
			`ALTER TABLE chat_reply_roots ADD COLUMN IF NOT EXISTS discussion_peer_type TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE chat_reply_roots ADD COLUMN IF NOT EXISTS discussion_peer_id BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE chat_reply_roots ADD COLUMN IF NOT EXISTS discussion_peer_hash BIGINT NOT NULL DEFAULT 0`,
		},
	},
	{
		version: 13,
		statements: []string{
			`CREATE TABLE IF NOT EXISTS telegram_rate_limits (account_id TEXT PRIMARY KEY, blocked_until TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL)`,
			`CREATE INDEX IF NOT EXISTS telegram_rate_limits_blocked_until ON telegram_rate_limits(blocked_until)`,
		},
	},
	{
		version: 14,
		statements: []string{
			`CREATE INDEX IF NOT EXISTS download_items_queued_job_id ON download_items(job_id) WHERE status = 'queued'`,
		},
	},
}

func (m *Manager) applyPostgresMigrations() error {
	var current int
	if err := m.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read PostgreSQL schema version: %w", err)
	}
	for _, migration := range postgresMigrations {
		if migration.version <= current {
			continue
		}
		tx, err := m.db.Begin()
		if err != nil {
			return fmt.Errorf("start PostgreSQL migration %d: %w", migration.version, err)
		}
		for _, statement := range migration.statements {
			if _, err := tx.Exec(statement); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply PostgreSQL migration %d: %w", migration.version, err)
			}
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)`, migration.version, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record PostgreSQL migration %d: %w", migration.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit PostgreSQL migration %d: %w", migration.version, err)
		}
		current = migration.version
	}
	return nil
}
