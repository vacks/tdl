package download

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const databaseOperationTimeout = 30 * time.Second

// database centralizes PostgreSQL SQL binding and connection lifecycle.
type database struct {
	db *sql.DB
}

type databaseTx struct {
	tx *sql.Tx
}

type databaseRows struct {
	*sql.Rows
	cancel context.CancelFunc
}

func (r *databaseRows) Close() error {
	r.cancel()
	return r.Rows.Close()
}

type databaseRow struct {
	row    *sql.Row
	cancel context.CancelFunc
}

func (r *databaseRow) Scan(dest ...any) error {
	defer r.cancel()
	return r.row.Scan(dest...)
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
	return &database{db: db}, nil
}

// openDatabase accepts PostgreSQL only. Keeping one SQL dialect eliminates
// divergent production behaviour and makes all schema guarantees testable.
func openDatabase(ctx context.Context, url string) (*database, error) {
	if !strings.HasPrefix(url, "postgres://") && !strings.HasPrefix(url, "postgresql://") {
		return nil, fmt.Errorf("TDL_DATABASE_URL 必须是 PostgreSQL 连接串")
	}
	return openPostgresDatabase(ctx, url)
}

func (d *database) Close() error { return d.db.Close() }

func (d *database) PingContext(ctx context.Context) error { return d.db.PingContext(ctx) }

func (d *database) Exec(query string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	defer cancel()
	return d.db.ExecContext(ctx, d.bind(query), args...)
}

func (d *database) Query(query string, args ...any) (*databaseRows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	rows, err := d.db.QueryContext(ctx, d.bind(query), args...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &databaseRows{Rows: rows, cancel: cancel}, nil
}

func (d *database) QueryRow(query string, args ...any) *databaseRow {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	return &databaseRow{row: d.db.QueryRowContext(ctx, d.bind(query), args...), cancel: cancel}
}

func (d *database) Begin() (*databaseTx, error) {
	// Transactions can span a complete multi-file state transition. Their
	// caller owns commit/rollback, so the generic point-operation timeout must
	// not cancel a valid transaction immediately after it is opened.
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	return &databaseTx{tx: tx}, nil
}

func (tx *databaseTx) Exec(query string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	defer cancel()
	return tx.tx.ExecContext(ctx, bindSQL(query), args...)
}

func (tx *databaseTx) Query(query string, args ...any) (*databaseRows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	rows, err := tx.tx.QueryContext(ctx, bindSQL(query), args...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &databaseRows{Rows: rows, cancel: cancel}, nil
}

func (tx *databaseTx) QueryRow(query string, args ...any) *databaseRow {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	return &databaseRow{row: tx.tx.QueryRowContext(ctx, bindSQL(query), args...), cancel: cancel}
}

func (tx *databaseTx) Commit() error   { return tx.tx.Commit() }
func (tx *databaseTx) Rollback() error { return tx.tx.Rollback() }

func (d *database) bind(query string) string { return bindSQL(query) }

// bindSQL converts question-mark placeholders at the database boundary. SQL
// literals are preserved, so user-facing messages may safely contain '?'.
func bindSQL(query string) string {
	if !strings.Contains(query, "?") {
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

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "unique constraint") || strings.Contains(text, "duplicate key")
}
