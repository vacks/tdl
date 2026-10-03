package download

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
)

// Counting the statements a path sends is how this package states "this work is
// set-based": a count that does not grow with the page is the difference
// between one statement per page and one per file, and on a channel with a
// million posts that difference is the whole indexing rate.
//
// A counting handle works when the code under test accepts one (querier, as the
// claim rules do). The paths that open their own transaction cannot be handed
// anything, so this wraps the driver instead: every statement either way reaches
// a connection, and a transaction's statements are counted once, on the
// connection it holds.
type statementCounter struct{ total atomic.Int64 }

func (c *statementCounter) count() int64 { return c.total.Load() }

type countingDriver struct {
	inner   driver.Driver
	counter *statementCounter
}

func (d countingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: conn, counter: d.counter}, nil
}

type countingConn struct {
	driver.Conn
	counter *statementCounter
}

func (c countingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.counter.total.Add(1)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c countingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.counter.total.Add(1)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c countingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.counter.total.Add(1)
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

// driverSerial names each registering test's driver uniquely, because
// sql.Register panics on a name it already holds.
var driverSerial atomic.Int64

// openCountingDatabase opens the test database through a driver that counts
// statements, so a test can measure a path that builds its own transactions.
func openCountingDatabase(t *testing.T, url string) (*database, *statementCounter) {
	t.Helper()
	counter := &statementCounter{}
	name := "pgx-counting-" + strconv.FormatInt(driverSerial.Add(1), 10)
	sql.Register(name, countingDriver{inner: stdlib.GetDefaultDriver(), counter: counter})
	previous := postgresDriverName
	postgresDriverName = name
	t.Cleanup(func() { postgresDriverName = previous })
	db, err := openPostgresDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	return db, counter
}
