package download

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vacks/tdl/internal/applog"
)

// migratePostgres creates the durable PostgreSQL schema. Download jobs and
// item identities are intentionally never removed by retention maintenance:
// they are the permanent history and de-duplication source of truth.
func (m *Manager) migratePostgres() error {
	// This list must contain table DDL only. It runs on every start, before the
	// versioned migrations, so an index listed here would be built non
	// concurrently on an existing database: on a table holding tens of millions
	// of rows that takes a SHARE lock which blocks every insert and update for
	// the whole build, on a plain restart. Index DDL belongs in
	// postgresIndexStatements, which the concurrent channel applies outside a
	// transaction.
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
		// This table records a resumable walk position per task: the history scan
		// uses one row per media stream, and the listener gap walk keeps its own
		// cursor here too. It also carried a never used "initialized" flag,
		// dropped in v19.
		`CREATE TABLE IF NOT EXISTS chat_download_streams (
 chat_job_id TEXT NOT NULL, stream_kind TEXT NOT NULL, offset_message_id INTEGER NOT NULL DEFAULT 0, completed SMALLINT NOT NULL DEFAULT 0, PRIMARY KEY(chat_job_id, stream_kind), FOREIGN KEY(chat_job_id) REFERENCES chat_download_jobs(id) ON DELETE CASCADE
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
		// download_item_stats is a derived, rebuildable summary of one message
		// task's files. download_items remains the only source of truth; this
		// exists so rendering a page of tasks does not have to read every file
		// row of every task on it.
		`CREATE TABLE IF NOT EXISTS download_item_stats (
 job_id TEXT PRIMARY KEY, total_items BIGINT NOT NULL DEFAULT 0, completed_items BIGINT NOT NULL DEFAULT 0, FOREIGN KEY(job_id) REFERENCES download_jobs(id) ON DELETE CASCADE
)`,
		// The task's display text is one message's caption, identical for every
		// file of the task. Keeping it on the task removes the captions - which
		// can be arbitrarily long - from the list query entirely.
		`ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS message_text TEXT NOT NULL DEFAULT ''`,
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

// postgresIndexStatements is the complete set of indexes the application
// relies on. It is applied through the concurrent channel (migration 19) rather
// than the unversioned table block above, so a database that is missing one
// builds it online instead of holding a SHARE lock on a table that may hold
// tens of millions of rows.
//
// Every index here must be justified by a statement that can actually choose
// it. Six indexes that could not are dropped in migration 19; the index-scan
// counter in pg_stat_user_indexes is not evidence in either direction on a
// development database, which is small enough that the planner seq scans most
// tables, so the argument has to come from reading the SQL.
var postgresIndexStatements = []string{
	// ---- download_items: the message task file table ----
	//
	// Serves every per task lookup: item lists, completion counts, the
	// per-task claim scan and the "all items completed" settle check.
	`CREATE INDEX IF NOT EXISTS download_items_job_id ON download_items(job_id)`,
	// The scheduler's ready-work probe, narrowed to the one status it asks
	// about. A queued row that later completes leaves this index entirely.
	`CREATE INDEX IF NOT EXISTS download_items_queued_job_id ON download_items(job_id) WHERE status = 'queued'`,
	// The bot's on demand count of files still in flight. Narrowed to the five
	// in flight statuses so completed rows - the overwhelming majority of a
	// permanent history - are never indexed at all.
	`CREATE INDEX IF NOT EXISTS download_items_active_status ON download_items(status) WHERE status IN ('queued', 'waiting', 'running', 'downloaded', 'paused')`,
	// Interruption recovery updates exactly the running and downloaded rows.
	// This is deliberately narrower than the active-status index: that one also
	// contains queued and waiting, which for a stream task with millions of
	// indexed files would make the planner scan the whole queue to find the
	// handful of rows it is about to repair.
	`CREATE INDEX IF NOT EXISTS download_items_recovery ON download_items(status) WHERE status IN ('running', 'downloaded')`,
	// The waiting rotation resumes from an item id cursor; without an ordered
	// index the reconciler would sort every waiting row on each pass.
	`CREATE INDEX IF NOT EXISTS download_items_waiting_by_id ON download_items(id) WHERE status = 'waiting'`,
	// The dashboard's recent-failure figure and the failure retention sweep.
	`CREATE INDEX IF NOT EXISTS download_items_failed_finished_at ON download_items(status, finished_at) WHERE status = 'failed'`,

	// ---- download_jobs: one row per message task ----
	`CREATE INDEX IF NOT EXISTS download_jobs_created_id ON download_jobs(created_at DESC, id DESC)`,
	`CREATE INDEX IF NOT EXISTS download_jobs_visible_created_id ON download_jobs(created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,
	`CREATE INDEX IF NOT EXISTS download_jobs_visible_status_created_id ON download_jobs(status, created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,
	`CREATE INDEX IF NOT EXISTS download_jobs_status_updated_id ON download_jobs(status, updated_at, id)`,
	`CREATE INDEX IF NOT EXISTS download_jobs_account_visible_created_id ON download_jobs(account_id, created_at DESC, id DESC) WHERE parent_chat_id = '' AND status != 'deleted'`,

	// ---- auxiliary history ----
	`CREATE INDEX IF NOT EXISTS download_requests_created_id ON download_requests(created_at, id)`,
	`CREATE INDEX IF NOT EXISTS reaction_inbox_ready ON reaction_inbox(status, next_attempt_at, id)`,
	`CREATE INDEX IF NOT EXISTS reaction_inbox_cleanup ON reaction_inbox(status, updated_at, id)`,
	`CREATE INDEX IF NOT EXISTS download_resets_created ON download_resets(created_at)`,

	// ---- chat_download_jobs: one row per stream task ----
	`CREATE UNIQUE INDEX IF NOT EXISTS chat_download_jobs_active_unique ON chat_download_jobs(account_id, dialog_key, start_message_id) WHERE status IN ('queued', 'scanning', 'downloading', 'listening', 'paused')`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_created_id ON chat_download_jobs(created_at DESC, id DESC)`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_visible_created_id ON chat_download_jobs(created_at DESC, id DESC) WHERE status != 'deleted'`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_account_status ON chat_download_jobs(account_id, status, updated_at)`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_scan ON chat_download_jobs(status, scan_state, created_at)`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_listener ON chat_download_jobs(account_id, dialog_key, listen_new, scan_state, status)`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_active_target ON chat_download_jobs(account_id, dialog_key, start_message_id, status)`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_self_created ON chat_download_jobs(account_id, dialog_type, created_at DESC, id DESC) WHERE status != 'deleted'`,
	`CREATE INDEX IF NOT EXISTS chat_download_jobs_self_listener ON chat_download_jobs(account_id, dialog_type, listen_new, start_message_id, status, updated_at DESC)`,

	// ---- chat_download_items: the stream task file table ----
	//
	// One index replaces both chat_download_items_ready (a full index holding an
	// entry for every row ever indexed) and chat_download_items_state_by_job (a
	// strict prefix of it). It covers the ordered queued fetch, the pause,
	// cancel, resume and stalled-batch transitions, and it holds no entry for a
	// completed or cancelled row, which is the overwhelming majority of a
	// permanent index.
	`CREATE INDEX IF NOT EXISTS chat_download_items_work ON chat_download_items(chat_job_id, status, message_id) WHERE status IN ('queued', 'running', 'downloaded', 'failed', 'paused')`,
	// The waiting rotation reads strictly in primary-key order and resumes from
	// a (chat_job_id, dialog_key, message_id) cursor. The old index led with
	// status and omitted dialog_key, so the planner could not use it for either
	// the ordering or the cursor and had to sort every waiting row per pass.
	`CREATE INDEX IF NOT EXISTS chat_download_items_waiting_rotation ON chat_download_items(chat_job_id, dialog_key, message_id) WHERE status = 'waiting'`,
	// Interruption recovery updates exactly the running and downloaded rows.
	`CREATE INDEX IF NOT EXISTS chat_download_items_recovery ON chat_download_items(status) WHERE status IN ('running', 'downloaded')`,
	`CREATE INDEX IF NOT EXISTS chat_download_items_downloaded ON chat_download_items(chat_job_id, message_id) WHERE status = 'downloaded'`,
	`CREATE INDEX IF NOT EXISTS chat_download_items_failed_finished_at ON chat_download_items(status, finished_at) WHERE status = 'failed'`,

	`CREATE INDEX IF NOT EXISTS chat_message_inbox_ready ON chat_message_inbox(status, next_attempt_at, id)`,
	`CREATE INDEX IF NOT EXISTS chat_message_inbox_cleanup ON chat_message_inbox(status, updated_at, id)`,
	`CREATE INDEX IF NOT EXISTS chat_reply_roots_lookup ON chat_reply_roots(account_id, discussion_dialog_key, root_message_id)`,
	`CREATE INDEX IF NOT EXISTS downloaded_media_status ON downloaded_media(status, updated_at)`,
	`CREATE INDEX IF NOT EXISTS telegram_rate_limits_blocked_until ON telegram_rate_limits(blocked_until)`,
}

type postgresMigration struct {
	version int
	// longStatements run first, inside the same transaction as statements, but
	// without a client side deadline. They are for the backfills that rewrite
	// every row of a permanent history, which the general per-statement timeout
	// would cancel once that table is large. Applying them before statements is
	// what makes the order in version 19 correct: a summary is filled from the
	// whole table before any trigger that reports deltas exists, so no delta can
	// be recorded and then overwritten by an absolute value.
	longStatements []string
	// statements run inside the same transaction together with the version
	// record, so they are applied all or nothing. A statement PostgreSQL refuses
	// to run in a transaction block, CREATE INDEX CONCURRENTLY above all, must
	// not appear here; index DDL belongs in postgresIndexStatements.
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
	{
		// v15 keeps the waiting-item rotation an ordered index scan. The reconciler
		// resumes each pass from an item id cursor, which the plain status index
		// cannot serve in order; without this every pass would sort all waiting
		// rows before applying the cursor.
		version: 15,
		statements: []string{
			`CREATE INDEX IF NOT EXISTS download_items_waiting_by_id ON download_items(id) WHERE status = 'waiting'`,
		},
	},
	{
		// v16 drops four indexes that no query can choose. Each one is maintained
		// on every insert and every state change of chat_download_items, the table
		// expected to hold tens of millions of rows, so their cost is paid on the
		// hot write path for no read benefit. They were found by reading every
		// statement that touches these tables, not by index scan counters: this
		// database is small enough that the planner seq scans most tables, so a
		// zero counter is not by itself evidence that an index is unused.
		//   - chat_download_items_job is a strict prefix of the primary key
		//     (chat_job_id, dialog_key, message_id), so anything it could serve
		//     the primary key serves as well.
		//   - chat_download_items_child_job indexes a column the v4 migration
		//     already severed from the parent/child execution model. No statement
		//     reads it, and it holds one empty string per row.
		//   - chat_download_items_origin is only useful for filtering on
		//     origin_message_id, which appears in select lists and inserts but
		//     never in a predicate. Reply lookups use chat_reply_roots_lookup.
		//   - download_events_job_id is never used: the table is appended to and
		//     trimmed by created_at, and nothing filters it by job_id.
		//
		// The create statements for three of them also live in the unversioned
		// statement list above, which runs on every startup. Dropping them here
		// without removing those would undo this migration on the next boot, so
		// both had to change together.
		version: 16,
		statements: []string{
			`DROP INDEX IF EXISTS chat_download_items_job`,
			`DROP INDEX IF EXISTS chat_download_items_child_job`,
			`DROP INDEX IF EXISTS chat_download_items_origin`,
			`DROP INDEX IF EXISTS download_events_job_id`,
		},
	},
	{
		// v17 removes the download_events audit table. It recorded one row per
		// task and item state transition, and nothing ever read it: both
		// consumers subscribe to the in-memory event bus, the table was only
		// inserted into and trimmed by age, and no query in the repository ever
		// selected from it. On a task table holding tens of millions of rows
		// that was a write and an index entry per transition for no reader.
		//
		// This drops whatever history has accumulated with it. That is
		// deliberate and was confirmed: there is no reader to lose it from, and
		// leaving the table behind would keep the rows with nothing left to
		// prune them.
		version: 17,
		statements: []string{
			`DROP TABLE IF EXISTS download_events`,
		},
	},
	{
		// v18 narrows the message item status index to the statuses that are
		// still in flight.
		//
		// The dashboard counts active items every three seconds, and interruption
		// recovery updates them, and those are the only statements that filter
		// this table by status without also constraining job_id — every other one
		// is served by a job_id index or by a partial index of its own (waiting
		// by id, failed by finished_at). The full index therefore carried one
		// entry per row, including the completed rows that are the overwhelming
		// majority and are never looked up this way, and paid for them on every
		// insert.
		//
		// Measured on half a million rows: the active count drops from 12.6ms to
		// 6.9ms, and both statements keep an index-only scan.
		//
		// The two index statements that used to live here moved to
		// postgresIndexStatements and postgresIndexDrops. Running them inside
		// this transaction was wrong for anyone upgrading from v17: the unversioned
		// table block above ran CREATE INDEX first, on every start, so the index
		// was built non concurrently - holding a SHARE lock that blocks every
		// insert and update on a table that may hold tens of millions of rows -
		// before this transaction was ever entered, and this version's own
		// CREATE then became a no-op. The statement order below the version
		// record is what the intent was; the mechanism that honours it is now the
		// concurrent index set. The version is kept because it has already been
		// applied, and an empty statement list preserves that record.
		version:    18,
		statements: []string{},
	},
	{
		// v19 moves the message task page's per task file counts out of the
		// request and into a maintained summary, and replaces the index set with
		// one where every entry is justified by a statement that can choose it.
		//
		// Why the counts had to move. The task list rendered one page by running,
		// per task on that page, COUNT(id) / SUM(status='completed') /
		// MAX(message_text) over download_items with only a single column job_id
		// index. None of those three expressions is in that index, so every row
		// of every task on the page was fetched from the heap. A plain message
		// link holds a handful of files and that is invisible, but with
		// "download linked comments/replies" on - which is the default - a single
		// channel post's task holds one row per media comment, and a popular post
		// reaches thousands. The page then cost the union of those tasks' files
		// rather than the page size, on a list the Web UI re-reads while anything
		// is downloading.
		//
		// The order below is deliberate and load bearing. The summary is
		// backfilled BEFORE the triggers exist, so no concurrent transition can
		// record a delta that the absolute backfill then overwrites. A row the
		// backfill could still miss - one committed between its snapshot and its
		// write, with no trigger yet to record it - is repaired by the trigger's
		// insert branch, which computes an absolute count whenever it finds no
		// summary row for that task instead of adding a delta to nothing.
		version: 19,
		longStatements: []string{
			// Fill from the whole table before any trigger exists, so nothing can
			// record a delta that this absolute write then overwrites.
			`INSERT INTO download_item_stats(job_id, total_items, completed_items)
 SELECT job_id, COUNT(*), COUNT(*) FILTER (WHERE status = 'completed') FROM download_items GROUP BY job_id
 ON CONFLICT (job_id) DO UPDATE SET total_items = EXCLUDED.total_items, completed_items = EXCLUDED.completed_items`,
			`UPDATE download_jobs j SET message_text = s.text FROM (
 SELECT job_id, MAX(NULLIF(message_text, '')) AS text FROM download_items GROUP BY job_id
) s WHERE s.job_id = j.id AND s.text IS NOT NULL AND j.message_text = ''`,
		},
		// Every trigger maintains existing rows by delta and creates a missing
		// row from an absolute count. That second branch is what makes the
		// summary self healing: a task the backfill could not see - one whose
		// files committed between its snapshot and its write, while no trigger
		// existed yet - is repaired the next time any of its files changes,
		// instead of carrying a permanent miscount. Counting one task's own
		// files is proportional to that task, never to the whole history.
		statements: []string{
			`CREATE OR REPLACE FUNCTION tdl_item_stats_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 WITH delta AS (
  SELECT job_id, COUNT(*) AS total_items,
         COUNT(*) FILTER (WHERE status = 'completed') AS completed_items
  FROM new_rows GROUP BY job_id
 )
 UPDATE download_item_stats s SET
  total_items = s.total_items + d.total_items,
  completed_items = s.completed_items + d.completed_items
 FROM delta d WHERE s.job_id = d.job_id;
 INSERT INTO download_item_stats(job_id, total_items, completed_items)
 SELECT j.id,
        (SELECT COUNT(*) FROM download_items x WHERE x.job_id = j.id),
        (SELECT COUNT(*) FROM download_items x WHERE x.job_id = j.id AND x.status = 'completed')
 FROM (SELECT DISTINCT job_id AS id FROM new_rows) j
 WHERE NOT EXISTS (SELECT 1 FROM download_item_stats s WHERE s.job_id = j.id);
 RETURN NULL;
END $$`,
			`CREATE OR REPLACE FUNCTION tdl_item_stats_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 WITH delta AS (
  SELECT job_id, COUNT(*) AS total_items,
         COUNT(*) FILTER (WHERE status = 'completed') AS completed_items
  FROM old_rows GROUP BY job_id
 )
 UPDATE download_item_stats s SET
  total_items = GREATEST(0, s.total_items - d.total_items),
  completed_items = GREATEST(0, s.completed_items - d.completed_items)
 FROM delta d WHERE s.job_id = d.job_id;
 INSERT INTO download_item_stats(job_id, total_items, completed_items)
 SELECT j.id,
        (SELECT COUNT(*) FROM download_items x WHERE x.job_id = j.id),
        (SELECT COUNT(*) FROM download_items x WHERE x.job_id = j.id AND x.status = 'completed')
 FROM (SELECT DISTINCT job_id AS id FROM old_rows) j
 WHERE NOT EXISTS (SELECT 1 FROM download_item_stats s WHERE s.job_id = j.id);
 RETURN NULL;
END $$`,
			// total_items is deliberately left alone here. Every statement that
			// updates a file row keeps its identity columns (job_id, dialog_key,
			// message_id) fixed, so an update can never move a row into or out of
			// a task.
			`CREATE OR REPLACE FUNCTION tdl_item_stats_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 WITH delta AS (
  SELECT n.job_id,
   COUNT(*) FILTER (WHERE n.status = 'completed') - COUNT(*) FILTER (WHERE o.status = 'completed') AS completed_items
  FROM new_rows n JOIN old_rows o USING (job_id, dialog_key, message_id)
  GROUP BY n.job_id
 )
 UPDATE download_item_stats s SET
  completed_items = GREATEST(0, s.completed_items + d.completed_items)
 FROM delta d WHERE s.job_id = d.job_id;
 INSERT INTO download_item_stats(job_id, total_items, completed_items)
 SELECT j.id,
        (SELECT COUNT(*) FROM download_items x WHERE x.job_id = j.id),
        (SELECT COUNT(*) FROM download_items x WHERE x.job_id = j.id AND x.status = 'completed')
 FROM (SELECT DISTINCT job_id AS id FROM new_rows) j
 WHERE NOT EXISTS (SELECT 1 FROM download_item_stats s WHERE s.job_id = j.id);
 RETURN NULL;
END $$`,
			// Never read and never written: a resumable walk is described by its
			// offset, and zero already means "not started".
			`ALTER TABLE chat_download_streams DROP COLUMN IF EXISTS initialized`,
			`DROP TRIGGER IF EXISTS tdl_item_stats_insert_trigger ON download_items`,
			`DROP TRIGGER IF EXISTS tdl_item_stats_delete_trigger ON download_items`,
			`DROP TRIGGER IF EXISTS tdl_item_stats_update_trigger ON download_items`,
			`CREATE TRIGGER tdl_item_stats_insert_trigger AFTER INSERT ON download_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION tdl_item_stats_insert()`,
			`CREATE TRIGGER tdl_item_stats_delete_trigger AFTER DELETE ON download_items REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION tdl_item_stats_delete()`,
			`CREATE TRIGGER tdl_item_stats_update_trigger AFTER UPDATE ON download_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION tdl_item_stats_update()`,
		},
	},
}

// postgresIndexDrops names indexes that no statement can choose. Each is
// replaced by something narrower in postgresIndexStatements; they are dropped
// after it, so no query is ever left without a usable index.
var postgresIndexDrops = []string{
	// A full index holding an entry for every file ever indexed, when the only
	// statement that reads it asks for a single status. Its successor,
	// chat_download_items_work, carries the same leading columns and holds no
	// entry for a completed or cancelled row.
	`chat_download_items_ready`,
	// A strict prefix of chat_download_items_ready, and so of _work.
	`chat_download_items_state_by_job`,
	// Every status filtered statement on this table is served by a narrower
	// partial index (_work, _waiting_rotation, _downloaded, _recovery,
	// _failed_finished_at). The dashboard count that once read it now reads
	// chat_download_stats. It was also a planner trap for interruption
	// recovery: it indexes queued as well, so on a stream task with a long
	// queue the planner could scan the entire queue to find the handful of
	// running rows it was repairing.
	`chat_download_items_active_status`,
	// Its sort column omitted dialog_key, so the waiting rotation could use it
	// for neither its ORDER BY nor its tuple cursor.
	`chat_download_items_waiting`,
	// Superseded by download_items_active_status. v18 used to create the
	// replacement and drop this one inside its own transaction; both moved
	// here, so a database still on v17 builds the replacement online instead of
	// blocking every write for the duration of the build.
	`download_items_status`,
	// The leading column is only ever compared to the empty string. Every query
	// that mentions parent_chat_id constrains it to '' as a constant, which the
	// partial predicates on the visible indexes already encode, so this index
	// can never be chosen for a lookup.
	`download_jobs_parent_created_id`,
	// Nothing filters download_requests by job_id: the table is appended per
	// request and swept by created_at.
	`download_requests_job_id`,
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
		for _, statement := range migration.longStatements {
			if _, err := tx.ExecUnbounded(statement); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply PostgreSQL migration %d: %w", migration.version, err)
			}
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
	return m.ensurePostgresIndexes()
}

// ensurePostgresIndexes brings the index set up to postgresIndexStatements.
//
// It runs on every start rather than once, because an index is not part of the
// schema's correctness: it is what makes a query fast, and a database that is
// missing one still works. Making it repeatable means an index build that was
// interrupted - or that failed after its migration was recorded - is retried on
// the next start instead of being lost. The cost when nothing changed is one
// catalog lookup per index.
//
// Each statement gets its own connection so it runs outside a transaction
// block, which CREATE INDEX CONCURRENTLY requires: readers and writers keep
// working while the index is built, which on a table holding tens of millions
// of rows is the difference between a deploy that finishes and one that blocks
// every download for minutes.
//
// A concurrent build that is interrupted leaves an INVALID index behind.
// IF NOT EXISTS treats that as "already there" and would leave the index
// unusable forever, so an invalid relation is dropped and rebuilt first.
func (m *Manager) ensurePostgresIndexes() error {
	for _, statement := range postgresIndexStatements {
		if err := m.execConcurrently(statement); err != nil {
			return fmt.Errorf("ensure PostgreSQL indexes: %w", err)
		}
	}
	// Superseded indexes go last, so the replacement is always in place before
	// the one it supersedes is removed.
	for _, name := range postgresIndexDrops {
		if !validIndexIdentifier(name) {
			return fmt.Errorf("ensure PostgreSQL indexes: invalid index name %q", name)
		}
		if _, err := m.db.ExecUnbounded(`DROP INDEX IF EXISTS ` + name); err != nil {
			return fmt.Errorf("ensure PostgreSQL indexes: drop %s: %w", name, err)
		}
	}
	return nil
}

func (m *Manager) execConcurrently(statement string) error {
	name := concurrentIndexName(statement)
	if name != "" {
		if invalid, err := m.indexInvalid(name); err != nil {
			return err
		} else if invalid {
			applog.Info("schema", "rebuilding_invalid_index", "index", name)
			if _, err := m.db.ExecUnbounded(`DROP INDEX IF EXISTS ` + name); err != nil {
				return fmt.Errorf("drop invalid index %s: %w", name, err)
			}
		}
	}
	if _, err := m.db.ExecUnbounded(statement); err != nil {
		return err
	}
	return nil
}

func (m *Manager) indexInvalid(name string) (bool, error) {
	var valid bool
	err := m.db.QueryRow(`SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = ? AND c.relkind = 'i'`, name).Scan(&valid)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !valid, nil
}

// concurrentIndexName extracts the relation name an index statement targets,
// so an interrupted build can be detected before IF NOT EXISTS hides it. It
// returns an empty string for anything that is not an index statement.
func concurrentIndexName(statement string) string {
	upper := strings.ToUpper(statement)
	marker := "INDEX IF NOT EXISTS "
	index := strings.Index(upper, marker)
	if index < 0 {
		return ""
	}
	rest := statement[index+len(marker):]
	if end := strings.IndexAny(rest, " (\"\n\t"); end >= 0 {
		rest = rest[:end]
	}
	if !validIndexIdentifier(rest) {
		return ""
	}
	return rest
}

// validIndexIdentifier refuses anything that is not a plain lowercase
// identifier, so a name taken out of a statement can never carry SQL into the
// DROP that quotes it back.
func validIndexIdentifier(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for index := 0; index < len(name); index++ {
		char := name[index]
		switch {
		case char >= 'a' && char <= 'z', char >= '0' && char <= '9', char == '_':
		default:
			return false
		}
	}
	return true
}
