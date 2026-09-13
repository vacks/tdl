package download

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// database centralizes SQL binding and connection lifecycle. Production uses
// PostgreSQL; the SQLite branch exists only for isolated unit tests and is not
// selected by application configuration.
type database struct {
	db       *sql.DB
	postgres bool
}

type databaseTx struct {
	tx       *sql.Tx
	postgres bool
}

func openPostgresDatabase(ctx context.Context, url string) (*database, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(8)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &database{db: db, postgres: true}, nil
}

// openDatabase only accepts PostgreSQL in application configuration. The
// SQLite branch is deliberately limited to Go unit fixtures.
func openDatabase(ctx context.Context, url string) (*database, error) {
	if strings.HasPrefix(url, "sqlite://") {
		db, err := sql.Open("sqlite", strings.TrimPrefix(url, "sqlite://"))
		if err != nil {
			return nil, err
		}
		return newSQLiteDatabase(db), nil
	}
	return openPostgresDatabase(ctx, url)
}

func newSQLiteDatabase(db *sql.DB) *database { return &database{db: db} }

func (d *database) Close() error { return d.db.Close() }

func (d *database) PingContext(ctx context.Context) error { return d.db.PingContext(ctx) }

func (d *database) Exec(query string, args ...any) (sql.Result, error) {
	return d.db.Exec(d.bind(query), args...)
}

func (d *database) Query(query string, args ...any) (*sql.Rows, error) {
	return d.db.Query(d.bind(query), args...)
}

func (d *database) QueryRow(query string, args ...any) *sql.Row {
	return d.db.QueryRow(d.bind(query), args...)
}

func (d *database) Begin() (*databaseTx, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	return &databaseTx{tx: tx, postgres: d.postgres}, nil
}

func (tx *databaseTx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.tx.Exec(bindSQL(query, tx.postgres), args...)
}

func (tx *databaseTx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.tx.Query(bindSQL(query, tx.postgres), args...)
}

func (tx *databaseTx) QueryRow(query string, args ...any) *sql.Row {
	return tx.tx.QueryRow(bindSQL(query, tx.postgres), args...)
}

func (tx *databaseTx) Commit() error   { return tx.tx.Commit() }
func (tx *databaseTx) Rollback() error { return tx.tx.Rollback() }

func (d *database) bind(query string) string { return bindSQL(query, d.postgres) }

// bindSQL converts question-mark placeholders at the database boundary. SQL
// literals are preserved, so user-facing messages may safely contain '?'.
func bindSQL(query string, postgres bool) string {
	if !postgres || !strings.Contains(query, "?") {
		return query
	}
	var out strings.Builder
	out.Grow(len(query) + 16)
	argument := 0
	inQuote := false
	for index := 0; index < len(query); index++ {
		char := query[index]
		if char == '\'' {
			out.WriteByte(char)
			if inQuote && index+1 < len(query) && query[index+1] == '\'' {
				out.WriteByte(query[index+1])
				index++
				continue
			}
			inQuote = !inQuote
			continue
		}
		if char == '?' && !inQuote {
			argument++
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(argument))
			continue
		}
		out.WriteByte(char)
	}
	return out.String()
}

func (d *database) isPostgres() bool { return d != nil && d.postgres }
