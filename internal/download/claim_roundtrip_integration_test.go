package download

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// countingQuerier counts the statements sent through a handle.
type countingQuerier struct {
	inner      querier
	mu         sync.Mutex
	statements int
}

func (c *countingQuerier) count() {
	c.mu.Lock()
	c.statements++
	c.mu.Unlock()
}

func (c *countingQuerier) Exec(query string, args ...any) (sql.Result, error) {
	c.count()
	return c.inner.Exec(query, args...)
}

func (c *countingQuerier) Query(query string, args ...any) (*databaseRows, error) {
	c.count()
	return c.inner.Query(query, args...)
}

func (c *countingQuerier) QueryRow(query string, args ...any) *databaseRow {
	c.count()
	return c.inner.QueryRow(query, args...)
}

func (c *countingQuerier) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statements
}

// A page of indexed media costs a handful of statements, not a handful each.
//
// The ingest path used to answer "who owns this file?" one candidate at a time:
// a read of the ownership table, a read of the message task's table, an insert,
// and sometimes a second read and insert, all inside the transaction that
// indexes the page. A scan walks a channel a hundred messages at a time and a
// large channel holds hundreds of thousands of them, so the whole indexing rate
// was set by bookkeeping that has a set-based form sitting next to it.
//
// This measures the pipeline the ingest path now runs - the message-task read,
// the adoption, and the claim - and holds it to a count that does not grow with
// the page. The bound is deliberately loose; what it has to catch is a return to
// per-candidate statements, which is a factor of fifty here and worse on a page
// with more media.
func TestPostgresClaimingAPageCostsAFixedNumberOfStatements(t *testing.T) {
	url := os.Getenv("TDL_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TDL_TEST_POSTGRES_URL to run PostgreSQL integration tests")
	}
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := &Manager{db: db, events: newEventBus(), wake: make(chan struct{}, 1), chatWake: make(chan struct{}, 1), progress: newProgressStore()}
	if err := m.migratePostgres(); err != nil {
		t.Fatal(err)
	}
	if err := clearPostgresDownloadTestData(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO chat_download_jobs(id, source_url, dialog_type, dialog_key, dialog_id, dialog_name, account_id, status, scan_state, config_json, created_at, updated_at) VALUES ('page-chat','tg://x','channel','channel:page',1,'test','account','scanning','indexing','{}',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	// Sixty-four fresh media, which is what one indexed page holds. Every one of
	// them also has a message task's row, because that is the window this path
	// has to wait out.
	items := make([]source, 0, 64)
	for id := 1; id <= 64; id++ {
		items = append(items, source{Item: Item{DialogType: "channel", DialogKey: "channel:page", DialogID: 1, MessageID: id, OriginalName: fmt.Sprintf("f%d.bin", id)}})
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	txC := &countingQuerier{inner: tx}

	if _, err := messageTaskRows(txC, mediaKeys(items)); err != nil {
		t.Fatal(err)
	}
	if err := adoptMessageTaskFiles(txC, map[mediaClaimKey]string{}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := m.claimMediaBatch(txC, "chat", "page-chat", items); err != nil {
		t.Fatal(err)
	}
	sent := txC.total()
	// One read of the message task's rows, one insert-returning for the claim.
	// Nothing else is needed when nothing existed and nothing was waiting.
	if sent > 3 {
		t.Fatalf("claiming a page of %d media cost %d statements, want at most 3: "+
			"the per-candidate form this replaced spent three to five for each one", len(items), sent)
	}
	if sent == 0 {
		t.Fatal("the pipeline sent no statements at all")
	}

	// Committed before the count below, which reads on the pool and would
	// otherwise not see a claim this transaction is still holding.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// And it still reached the right verdict for every one of them.
	var queued int
	if err := db.QueryRow(`SELECT COUNT(1) FROM downloaded_media WHERE owner_kind = 'chat' AND owner_id = 'page-chat'`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != len(items) {
		t.Fatalf("%d media were claimed, want %d", queued, len(items))
	}
}
