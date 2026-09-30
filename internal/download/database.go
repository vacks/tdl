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
	// Sized above the long-lived consumers — 16 download workers, 1 chat worker,
	// 3 chat batch workers, 2 chat event workers and the claim reconciler — so
	// that request-scoped queries are not queued behind resident pollers.
	db.SetMaxOpenConns(32)
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
func openDatabase(ctx context.Context, dsn string) (*database, error) {
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		return nil, fmt.Errorf("PostgreSQL 连接配置无效")
	}
	return openPostgresDatabase(ctx, dsn)
}

func (d *database) Close() error { return d.db.Close() }

func (d *database) PingContext(ctx context.Context) error { return d.db.PingContext(ctx) }

func (d *database) Exec(query string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseOperationTimeout)
	defer cancel()
	return d.db.ExecContext(ctx, d.bind(query), args...)
}

// ExecUnbounded runs one statement with no client side deadline.
//
// It exists for DDL whose duration is proportional to the size of a table
// rather than to the work the statement describes. Building an index on a table
// holding tens of millions of rows legitimately takes minutes, so the general
// per-statement timeout would cancel it part way through, and a cancelled
// CREATE INDEX CONCURRENTLY leaves an INVALID index behind instead of simply
// doing nothing. Only schema maintenance belongs here; nothing on a request or
// worker path may use it.
func (d *database) ExecUnbounded(query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(context.Background(), bindSQL(query), args...)
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

// ExecUnbounded runs one statement inside this transaction with no client side
// deadline. It is for a schema migration that deliberately touches every row of
// a permanent history: the general per-statement timeout would cancel a backfill
// part way through once that table holds tens of millions of rows, and the
// migration would then fail on every subsequent start instead of completing.
// Nothing on a request or worker path may use it.
func (tx *databaseTx) ExecUnbounded(query string, args ...any) (sql.Result, error) {
	return tx.tx.ExecContext(context.Background(), bindSQL(query), args...)
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

// bindSQL converts question-mark placeholders at the database boundary. Every
// position PostgreSQL treats as "not code" is skipped, so a '?' inside a literal
// or a comment can never be renumbered into a parameter marker: that would shift
// every following argument by one, or fail with "there is no parameter $1".
//
// This matters beyond user-visible messages. The schema migrations execute
// plpgsql trigger definitions through this function, and those bodies are
// dollar-quoted and contain both string literals and comments.
//
// Known limitation: backslash escapes inside E-prefixed strings are not decoded,
// so a '?' in such a string is treated as still-inside-the-literal. The effect is
// a PostgreSQL syntax error rather than a silently mis-numbered parameter, and
// this codebase uses no E-prefixed strings.
func bindSQL(query string) string {
	if !strings.Contains(query, "?") {
		return query
	}
	var out strings.Builder
	out.Grow(len(query) + 16)
	argument := 0
	for index := 0; index < len(query); {
		char := query[index]
		switch {
		case char == '\'' || char == '"':
			end := skipQuoted(query, index, char)
			out.WriteString(query[index:end])
			index = end
		case strings.HasPrefix(query[index:], "--"):
			end := strings.IndexByte(query[index:], '\n')
			if end < 0 {
				out.WriteString(query[index:])
				index = len(query)
				break
			}
			end += index + 1
			out.WriteString(query[index:end])
			index = end
		case strings.HasPrefix(query[index:], "/*"):
			end := skipBlockComment(query, index)
			out.WriteString(query[index:end])
			index = end
		case char == '$':
			tag, ok := dollarTag(query, index)
			if !ok {
				out.WriteByte(char)
				index++
				break
			}
			end := skipDollarQuoted(query, index, tag)
			out.WriteString(query[index:end])
			index = end
		case char == '?':
			argument++
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(argument))
			index++
		default:
			out.WriteByte(char)
			index++
		}
	}
	return out.String()
}

// skipQuoted returns the index just past a quoted run beginning at start. A
// doubled quote inside the run is an escaped quote, not a terminator. An
// unterminated run consumes the rest of the input.
func skipQuoted(query string, start int, quote byte) int {
	for index := start + 1; index < len(query); index++ {
		if query[index] != quote {
			continue
		}
		if index+1 < len(query) && query[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(query)
}

// skipBlockComment returns the index just past a block comment. PostgreSQL
// block comments nest, so the depth has to be tracked.
func skipBlockComment(query string, start int) int {
	depth := 0
	for index := start; index < len(query); {
		switch {
		case strings.HasPrefix(query[index:], "/*"):
			depth++
			index += 2
		case strings.HasPrefix(query[index:], "*/"):
			depth--
			index += 2
			if depth == 0 {
				return index
			}
		default:
			index++
		}
	}
	return len(query)
}

// dollarTag reports whether a dollar-quoted string opens at start and returns
// its full tag, which is "$$" for the anonymous form and "$name$" otherwise. A
// '$' that does not open a valid tag is an ordinary character, so a positional
// parameter such as $1 is never mistaken for the start of a quoted body.
func dollarTag(query string, start int) (string, bool) {
	for index := start + 1; index < len(query); index++ {
		char := query[index]
		if char == '$' {
			return query[start : index+1], true
		}
		if !isDollarTagChar(char, index == start+1) {
			return "", false
		}
	}
	return "", false
}

func isDollarTagChar(char byte, first bool) bool {
	switch {
	case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
		return true
	case char >= '0' && char <= '9':
		return !first
	}
	return false
}

// skipDollarQuoted returns the index just past a dollar-quoted body, or the end
// of the input when the closing tag never appears.
func skipDollarQuoted(query string, start int, tag string) int {
	from := start + len(tag)
	offset := strings.Index(query[from:], tag)
	if offset < 0 {
		return len(query)
	}
	return from + offset + len(tag)
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "unique constraint") || strings.Contains(text, "duplicate key")
}
