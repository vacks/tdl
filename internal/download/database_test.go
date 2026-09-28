package download

import "testing"

// bindSQL is the boundary between hand-written SQL and the PostgreSQL driver.
// Every "?" it rewrites shifts the argument numbering for the whole statement,
// so a "?" that is not a placeholder must be left alone. The migrations execute
// dollar-quoted plpgsql bodies through this function, which is why comments and
// dollar-quoting are covered here and not only string literals.
func TestBindSQLRewritesOnlyRealPlaceholders(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain placeholder",
			in:   `SELECT a FROM t WHERE b = ?`,
			want: `SELECT a FROM t WHERE b = $1`,
		},
		{
			name: "several placeholders keep their order",
			in:   `UPDATE t SET a = ?, b = ? WHERE c = ?`,
			want: `UPDATE t SET a = $1, b = $2 WHERE c = $3`,
		},
		{
			name: "question mark inside a string literal",
			in:   `SELECT a FROM t WHERE b = ? AND c = '?'`,
			want: `SELECT a FROM t WHERE b = $1 AND c = '?'`,
		},
		{
			name: "doubled quote inside a literal does not end it",
			in:   `SELECT a FROM t WHERE c = 'it''s ?' AND b = ?`,
			want: `SELECT a FROM t WHERE c = 'it''s ?' AND b = $1`,
		},
		{
			name: "question mark inside a quoted identifier",
			in:   `SELECT "odd?column" FROM t WHERE b = ?`,
			want: `SELECT "odd?column" FROM t WHERE b = $1`,
		},
		{
			name: "question mark inside a line comment",
			in:   "SELECT a FROM t\n-- does this work?\nWHERE b = ?",
			want: "SELECT a FROM t\n-- does this work?\nWHERE b = $1",
		},
		{
			name: "question mark inside a block comment",
			in:   `SELECT a FROM t /* huh? */ WHERE b = ?`,
			want: `SELECT a FROM t /* huh? */ WHERE b = $1`,
		},
		{
			name: "block comments nest",
			in:   `SELECT a /* outer /* inner? */ still? */ FROM t WHERE b = ?`,
			want: `SELECT a /* outer /* inner? */ still? */ FROM t WHERE b = $1`,
		},
		{
			name: "question mark inside an anonymous dollar-quoted body",
			in:   `CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 /* ? */ $$`,
			want: `CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 /* ? */ $$`,
		},
		{
			name: "question mark inside a tagged dollar-quoted body",
			in:   `CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $body$ SELECT 1 /* ? */ $body$`,
			want: `CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $body$ SELECT 1 /* ? */ $body$`,
		},
		{
			// The shape the migrations actually produce: a dollar-quoted trigger
			// body containing comments and string literals with '?', followed by
			// real placeholders that must still be numbered from $1.
			name: "dollar-quoted trigger body followed by real placeholders",
			in: "CREATE OR REPLACE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$\n" +
				"BEGIN\n" +
				" -- is this right?\n" +
				" RAISE NOTICE 'what? %', NEW.id;\n" +
				" RETURN NULL;\n" +
				"END $$;\n" +
				"SELECT a FROM t WHERE b = ? AND c = ?",
			want: "CREATE OR REPLACE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$\n" +
				"BEGIN\n" +
				" -- is this right?\n" +
				" RAISE NOTICE 'what? %', NEW.id;\n" +
				" RETURN NULL;\n" +
				"END $$;\n" +
				"SELECT a FROM t WHERE b = $1 AND c = $2",
		},
		{
			name: "a lone dollar sign is not a quoted body",
			in:   `SELECT $ FROM t WHERE a = ?`,
			want: `SELECT $ FROM t WHERE a = $1`,
		},
		{
			name: "no placeholder returns the statement untouched",
			in:   `CREATE TABLE t (a TEXT DEFAULT 'x?y')`,
			want: `CREATE TABLE t (a TEXT DEFAULT 'x?y')`,
		},
		{
			name: "unterminated literal swallows the rest instead of renumbering",
			in:   `SELECT 'oops FROM t WHERE a = ?`,
			want: `SELECT 'oops FROM t WHERE a = ?`,
		},
		{
			name: "user-facing chinese message containing a question mark",
			in:   `UPDATE t SET error = '失败？请重试' WHERE id = ?`,
			want: `UPDATE t SET error = '失败？请重试' WHERE id = $1`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := bindSQL(testCase.in); got != testCase.want {
				t.Fatalf("bindSQL()\n got: %q\nwant: %q", got, testCase.want)
			}
		})
	}
}

// Statements without a placeholder take an early return, so guard that path too:
// it must never alter the statement.
func TestBindSQLLeavesPlaceholderFreeStatementsAlone(t *testing.T) {
	statement := `CREATE TABLE IF NOT EXISTS t (a TEXT NOT NULL DEFAULT 'x')`
	if got := bindSQL(statement); got != statement {
		t.Fatalf("bindSQL() altered a statement with no placeholder: %q", got)
	}
}
