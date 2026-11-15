package sqlpolicy

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
)

func TestNewDefaults(t *testing.T) {
	p, err := New(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	want := Limits{
		MaxLength:            1 << 20,
		MaxSQLLength:         1 << 20,
		MaxExprDepth:         18,
		MaxParserDepth:       100,
		MaxCompoundSelect:    4,
		MaxVDBEOps:           250_000_000,
		MaxFunctionArgs:      1000,
		MaxLikePatternLength: 4096,
		MaxVariableNumber:    32766,
		MaxTriggerDepth:      8,
		MaxPageCount:         0,
		Timeout:              5 * time.Second,
	}
	if p.limits != want {
		t.Errorf("defaults:\n got %+v\nwant %+v", p.limits, want)
	}
	p, err = New(Limits{MaxExprDepth: 7})
	if err != nil || p.limits.MaxExprDepth != 7 {
		t.Errorf("MaxExprDepth 7: %v %v", p, err)
	}
}

func TestNewValidation(t *testing.T) {
	for _, tc := range []struct {
		l  Limits
		ok bool
	}{
		{Limits{MaxLength: 1_000_000_000}, true},
		{Limits{MaxLength: 1_000_000_001}, false},
		{Limits{MaxLength: -1}, false},
		{Limits{MaxSQLLength: 1_000_000_000}, true},
		{Limits{MaxSQLLength: 1_000_000_001}, false},
		{Limits{MaxExprDepth: 3}, true},
		{Limits{MaxExprDepth: 2}, false},
		{Limits{MaxExprDepth: -1}, false},
		{Limits{MaxParserDepth: 50}, true},
		{Limits{MaxParserDepth: 49}, false},
		{Limits{MaxCompoundSelect: 1}, true},
		{Limits{MaxCompoundSelect: -1}, false},
		{Limits{MaxLikePatternLength: 4096}, true},
		{Limits{MaxLikePatternLength: 4097}, false},
		{Limits{MaxTriggerDepth: 1}, true},
		{Limits{MaxTriggerDepth: -1}, false},
		{Limits{MaxVDBEOps: 250_000_000}, true},
		{Limits{MaxVDBEOps: 250_000_001}, false},
		{Limits{MaxFunctionArgs: 1000}, true},
		{Limits{MaxFunctionArgs: 1001}, false},
		{Limits{MaxVariableNumber: 32766}, true},
		{Limits{MaxVariableNumber: 32767}, false},
		{Limits{MaxPageCount: 4294967294}, true},
		{Limits{MaxPageCount: 4294967295}, false},
		{Limits{MaxPageCount: -1}, false},
		{Limits{Timeout: time.Nanosecond}, true},
		{Limits{Timeout: -time.Second}, false},
		// The nesting limits together: compound terms times nesting levels,
		// plus 2.5 per trigger level, at most 90.
		{Limits{MaxCompoundSelect: 5}, false},                                       // 5*16 + 20 = 100
		{Limits{MaxExprDepth: 19}, true},                                            // 4*17 + 20 = 88
		{Limits{MaxExprDepth: 20}, false},                                           // 4*18 + 20 = 92
		{Limits{MaxParserDepth: 107}, true},                                         // 4*17 + 20 = 88
		{Limits{MaxParserDepth: 108}, false},                                        // 4*18 + 20 = 92
		{Limits{MaxTriggerDepth: 10}, true},                                         // 4*16 + 25 = 89
		{Limits{MaxTriggerDepth: 11}, false},                                        // 4*16 + 27 = 91
		{Limits{MaxCompoundSelect: 1, MaxExprDepth: 90, MaxTriggerDepth: 1}, true},  // 88 + 2
		{Limits{MaxCompoundSelect: 1, MaxExprDepth: 91, MaxTriggerDepth: 1}, false}, // 89 + 2
		{Limits{MaxCompoundSelect: 11, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 1}, true},
		{Limits{MaxCompoundSelect: 11, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 2}, false},
		{Limits{MaxCompoundSelect: 1, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 33}, true},
		{Limits{MaxCompoundSelect: 1, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 34}, false},
	} {
		if _, err := New(tc.l); (err == nil) != tc.ok {
			t.Errorf("New(%+v): %v, want ok %t", tc.l, err, tc.ok)
		}
	}
}

func TestApplyConfiguresConnection(t *testing.T) {
	raw, err := sqlite3.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	// Start from the unsafe value of every setting Apply owns.
	for op, v := range map[sqlite3.DBConfig]bool{
		sqlite3.DBCONFIG_DEFENSIVE:             false,
		sqlite3.DBCONFIG_TRUSTED_SCHEMA:        true,
		sqlite3.DBCONFIG_WRITABLE_SCHEMA:       true,
		sqlite3.DBCONFIG_ENABLE_LOAD_EXTENSION: true,
	} {
		if _, err := raw.Config(op, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Exec("PRAGMA temp_store = FILE"); err != nil {
		t.Fatal(err)
	}
	raw.Limit(sqlite3.LIMIT_ATTACHED, 10)
	l := Limits{MaxLength: 1000, MaxSQLLength: 900, MaxExprDepth: 11, MaxParserDepth: 52, MaxCompoundSelect: 3, MaxVDBEOps: 1400, MaxFunctionArgs: 15, MaxLikePatternLength: 16, MaxVariableNumber: 17, MaxTriggerDepth: 6, MaxPageCount: 100}
	db := &testDB{raw: raw, conn: apply(t, raw, l)}
	defer func() { _ = db.conn.Close() }()

	got := db.owned(t)
	for op, want := range map[sqlite3.DBConfig]bool{
		sqlite3.DBCONFIG_DEFENSIVE:             true,
		sqlite3.DBCONFIG_TRUSTED_SCHEMA:        false,
		sqlite3.DBCONFIG_WRITABLE_SCHEMA:       false,
		sqlite3.DBCONFIG_ENABLE_LOAD_EXTENSION: false,
	} {
		if v := got[fmt.Sprint("dbconfig ", op)]; v != fmt.Sprint(want) {
			t.Errorf("db config %d = %s, want %t", op, v, want)
		}
	}
	if got["temp_store"] != "2" || got["max_page_count"] != "100" {
		t.Errorf("temp_store = %s, want 2 (memory); max_page_count = %s, want 100", got["temp_store"], got["max_page_count"])
	}
	for id, want := range map[sqlite3.LimitCategory]int{
		sqlite3.LIMIT_LENGTH: 1000, sqlite3.LIMIT_SQL_LENGTH: 900, sqlite3.LIMIT_EXPR_DEPTH: 11, sqlite3.LIMIT_PARSER_DEPTH: 52,
		sqlite3.LIMIT_COMPOUND_SELECT: 3, sqlite3.LIMIT_VDBE_OP: 1400, sqlite3.LIMIT_FUNCTION_ARG: 15,
		sqlite3.LIMIT_LIKE_PATTERN_LENGTH: 16, sqlite3.LIMIT_VARIABLE_NUMBER: 17, sqlite3.LIMIT_TRIGGER_DEPTH: 6,
		sqlite3.LIMIT_ATTACHED: 0,
	} {
		if v := raw.Limit(id, -1); v != want {
			t.Errorf("limit %d = %d, want %d", id, v, want)
		}
	}
}

func TestApplyFailsWhenTheDatabaseExceedsMaxPageCount(t *testing.T) {
	raw, err := sqlite3.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	if err := raw.Exec("CREATE TABLE t(a); INSERT INTO t SELECT randomblob(4000) FROM generate_series(1, 10)"); err != nil {
		t.Fatal(err)
	}
	p, err := New(Limits{MaxPageCount: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Apply(raw); err == nil {
		t.Fatal("Apply succeeded on a database larger than MaxPageCount")
	}
}

func TestMaxPageCountBoundsTheDatabase(t *testing.T) {
	db := openDB(t, true, Limits{MaxPageCount: 20})
	db.mustRun(t, "CREATE TABLE t(a)")
	if err := db.run("INSERT INTO t SELECT randomblob(4000) FROM generate_series(1, 100)"); err == nil || !strings.Contains(err.Error(), "full") {
		t.Fatalf("got %v, want database or disk is full", err)
	}
}

func TestStatementPreparedBeforeApplyIsRecompiledUnderThePolicy(t *testing.T) {
	raw, err := sqlite3.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	stmt, _, err := raw.Prepare("PRAGMA journal_mode = OFF")
	if err != nil {
		t.Fatal(err)
	}
	db := &testDB{raw: raw, conn: apply(t, raw, Limits{})}
	defer func() { _ = db.conn.Close() }()
	// sqlite3_set_authorizer expires prepared statements, so this one is
	// compiled again, under the authorizer, when it is stepped.
	if err := stmt.Exec(); err == nil {
		t.Error("statement prepared before Apply ran")
	}
	_ = stmt.Close()
	if got := db.owned(t)["journal_mode"]; got != "delete" {
		t.Errorf("journal_mode = %s", got)
	}
}

// refusals lists statements the policy must refuse, each run on a new
// database that holds the tables t(a), with one row, u(a) and the STRICT
// table s(a).
var refusals = []struct {
	reason Reason
	sql    string
}{
	{ReasonAttach, "ATTACH '{dir}/other.db' AS other"},
	{ReasonAttach, "ATTACH DATABASE 'file:{dir}/uri.db?mode=rwc' AS other"},
	{ReasonAttach, "ATTACH 'file:{dir}/uri2.db?vfs=os&mode=rwc' AS other"},
	{ReasonAttach, "ATTACH ':memory:' AS m"},
	{ReasonAttach, "ATTACH '' AS anon"},
	{ReasonAttach, "ATTACH '{dir}/a' || '.db' AS expr"},
	{ReasonAttach, "SELECT 1; ATTACH '{dir}/second.db' AS second"},
	{ReasonDetach, "DETACH other"},
	{ReasonDetach, "DETACH DATABASE temp"},
	// VACUUM consults no authorizer action of its own; it attaches its output
	// while it runs, and that ATTACH is refused.
	{ReasonAttach, "VACUUM"},
	{ReasonAttach, "VACUUM main"},
	{ReasonAttach, "VACUUM INTO '{dir}/copy.db'"},
	{ReasonAttach, "VACUUM main INTO 'file:{dir}/copy2.db?mode=rwc'"},
	// Transaction control belongs to the server.
	{ReasonTransaction, "BEGIN"},
	{ReasonTransaction, "BEGIN IMMEDIATE"},
	{ReasonTransaction, "BEGIN EXCLUSIVE TRANSACTION"},
	{ReasonTransaction, "COMMIT"},
	{ReasonTransaction, "END"},
	{ReasonTransaction, "ROLLBACK"},
	{ReasonTransaction, "SAVEPOINT s"}, // outside a transaction it would begin one
	{ReasonTransaction, "RELEASE s"},
	{ReasonTransaction, "ROLLBACK TO s"},
	{ReasonFunction, "SELECT sqlite_log(1, 'x')"},
	{ReasonFunction, "SELECT SQLITE_LOG(1, 'x')"},
	{ReasonFunction, "SELECT sqlite_compileoption_get(0)"},
	{ReasonFunction, "SELECT sqlite_compileoption_used('THREADSAFE')"},
	{ReasonFunction, "SELECT sqlite_source_id()"},
	{ReasonFunction, "SELECT match('a', 'b')"},
	{ReasonFunction, "SELECT 'a' REGEXP 'a'"},
	{ReasonFunction, "SELECT regexpi('a', 'A')"},
	{ReasonFunction, "SELECT decimal_add('1', '2')"},
	{ReasonFunction, "SELECT json_patch('{}', '{}')"},
	{ReasonFunction, "SELECT a FROM t WHERE a = abs(sqlite_source_id())"},
	{ReasonFunction, "CREATE TABLE c(a CHECK (sqlite_log(1, 'x') IS NULL))"},
	{ReasonFunction, "CREATE TABLE d(a DEFAULT (sqlite_source_id()))"},
	{ReasonFunction, "CREATE TABLE g(a, b AS (sqlite_source_id()))"},
	{ReasonFunction, "CREATE INDEX i ON t(sqlite_source_id())"},
	{ReasonFunction, "CREATE VIEW v AS SELECT sqlite_source_id(); SELECT * FROM v"},
	{ReasonFunction, "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (sqlite_source_id()); END; INSERT INTO t VALUES (1)"},
	{ReasonFunction, "CREATE TEMP TRIGGER tr AFTER INSERT ON main.t BEGIN SELECT sqlite_source_id(); END; INSERT INTO t VALUES (1)"},
	{ReasonStatement, "CREATE VIRTUAL TABLE s2 USING generate_series"},
	{ReasonPragma, "PRAGMA writable_schema = ON; UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a, b)' WHERE name = 't'"},
	{ReasonPragma, "SELECT * FROM pragma_writable_schema"},
	{ReasonPragma, "PRAGMA journal_mode = DELETE"},
	{ReasonPragma, "PRAGMA main.journal_mode = OFF"},
	{ReasonPragma, "PRAGMA JOURNAL_MODE(MEMORY)"},
	{ReasonPragma, "SELECT * FROM pragma_journal_mode"},
	{ReasonPragma, "SELECT * FROM pragma_journal_mode('main')"},
	{ReasonPragma, "PRAGMA locking_mode = EXCLUSIVE"},
	{ReasonPragma, "PRAGMA locking_mode(EXCLUSIVE)"},
	{ReasonPragma, "SELECT * FROM pragma_locking_mode"},
	{ReasonPragma, "PRAGMA database_list"},
	{ReasonPragma, "PRAGMA temp_store = FILE"},
	{ReasonPragma, "PRAGMA temp_store_directory = '{dir}'"},
	{ReasonPragma, "PRAGMA synchronous = OFF"},
	{ReasonPragma, "PRAGMA page_size = 8192"},
	{ReasonPragma, "PRAGMA max_page_count = 1"},
	{ReasonPragma, "PRAGMA wal_checkpoint(TRUNCATE)"},
	{ReasonPragma, "PRAGMA user_version = 7"},
	{ReasonPragma, "PRAGMA index_xinfo(t)"},
	{ReasonPragma, "SELECT * FROM t, pragma_table_list"},
	// quick_check is allowed only inside ALTER TABLE.
	{ReasonPragma, "PRAGMA quick_check"},
	{ReasonPragma, "SELECT * FROM t, pragma_quick_check"},
	{ReasonPragma, "PRAGMA integrity_check"},
}

func TestRefused(t *testing.T) {
	for _, tc := range refusals {
		t.Run(tc.sql, func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				db.mustRun(t, "CREATE TABLE t(a); INSERT INTO t VALUES (1); CREATE TABLE u(a); CREATE TABLE s(a TEXT) STRICT")
				before, schema := db.owned(t), db.schema(t)
				wantRefused(t, db.run(tc.sql), tc.reason)
				checkUnchanged(t, db, before, schema)
				db.mustRun(t, "SELECT 1") // the connection is still usable
			})
		})
	}
}

// checkUnchanged fails if a refused or failed statement changed a setting the
// server owns, attached a database, used a foreign file or, unless it only
// created a view or trigger, the schema.
func checkUnchanged(t *testing.T, db *testDB, before map[string]string, schema [][]string) {
	t.Helper()
	if after := db.owned(t); !reflect.DeepEqual(before, after) {
		for k, v := range before {
			if after[k] != v {
				t.Errorf("%s changed from %q to %q", k, v, after[k])
			}
		}
	}
	db.checkFiles(t)
	for _, row := range db.schema(t) {
		if row[0] != "view" && row[0] != "trigger" && !slices.ContainsFunc(schema, func(r []string) bool { return slices.Equal(r, row) }) {
			t.Errorf("schema gained %v", row)
		}
	}
	for _, f := range []string{"other.db", "uri.db", "uri2.db", "copy.db", "copy2.db", "second.db", "a.db"} {
		if _, err := os.Stat(filepath.Join(db.dir, f)); err == nil {
			t.Errorf("%s was created", f)
		}
	}
}

// engineRefusals are statements that SQLite itself refuses under the
// policy's configuration, with a fragment of the error it reports.
var engineRefusals = []struct{ sql, msg string }{
	{"CREATE TRIGGER tr AFTER INSERT ON t BEGIN ATTACH '{dir}/trig.db' AS x; END", "syntax error"},
	{"CREATE VIEW v AS ATTACH '{dir}/view.db' AS x", "syntax error"},
	{"SELECT load_extension('{dir}/x.so')", "no such function: load_extension"},
	{"SELECT sqlite_rename_test('main', 'CREATE TABLE t(a)', 'table', 't', 0, 'x', 0)", "no such function: sqlite_rename_test"},
	{"UPDATE t SET a = sqlite_drop_column(0, 'CREATE TABLE t(a, b)', 1)", "no such function: sqlite_drop_column"},
	{"UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a, b)' WHERE name = 't'", "may not be modified"},
	{"INSERT INTO sqlite_schema VALUES ('table', 'z', 'z', 0, 'CREATE TABLE z(a)')", "may not be modified"},
	{"DELETE FROM sqlite_master WHERE name = 't'", "may not be modified"},
	{"UPDATE sqlite_temp_schema SET sql = 'x'", "may not be modified"},
	{"CREATE TRIGGER s AFTER INSERT ON t BEGIN UPDATE sqlite_schema SET sql = 'x'; END; INSERT INTO t VALUES (1)", "may not be modified"},
	{"CREATE TRIGGER s AFTER INSERT ON sqlite_schema BEGIN SELECT 1; END", "cannot create trigger on system table"},
	{"ALTER TABLE sqlite_schema RENAME TO x", "may not be altered"},
	{"DROP TABLE sqlite_schema", "may not be dropped"},
	// trusted_schema is off, so a view may use only functions that SQLite
	// or their registration marks innocuous; base64 is not marked.
	{"CREATE VIEW b AS SELECT base64(x'00'); SELECT * FROM b", "unsafe use of base64()"},
}

func TestEngineRefuses(t *testing.T) {
	for _, tc := range engineRefusals {
		t.Run(tc.sql, func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				db.mustRun(t, "CREATE TABLE t(a); CREATE TABLE u(a)")
				before, schema := db.owned(t), db.schema(t)
				err := db.run(tc.sql)
				var pe *Error
				if err == nil || errors.As(err, &pe) || !strings.Contains(err.Error(), tc.msg) {
					t.Fatalf("got %v, want an engine error containing %q", err, tc.msg)
				}
				checkUnchanged(t, db, before, schema)
				for _, f := range []string{"trig.db", "view.db"} {
					if _, err := os.Stat(filepath.Join(db.dir, f)); err == nil {
						t.Errorf("%s was created", f)
					}
				}
			})
		})
	}
}

func TestWritableSchemaStaysOffWhenTheConnectionHadItOn(t *testing.T) {
	raw, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for op, v := range map[sqlite3.DBConfig]bool{sqlite3.DBCONFIG_DEFENSIVE: false, sqlite3.DBCONFIG_WRITABLE_SCHEMA: true} {
		if _, err := raw.Config(op, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Exec("CREATE TABLE t(a)"); err != nil {
		t.Fatal(err)
	}
	db := &testDB{raw: raw, conn: apply(t, raw, Limits{})}
	defer func() { _ = db.conn.Close() }()
	if err := db.run("UPDATE sqlite_schema SET sql = 'CREATE TABLE t(a, b)' WHERE name = 't'"); err == nil {
		t.Fatal("sqlite_schema was written")
	}
}

// allowed lists statements near the refused ones that must still work, each
// run on a new database that holds t(a), u(a), an index on t and a table
// with a foreign key.
var allowed = []string{
	"SELECT type, name, sql FROM sqlite_schema",
	"SELECT * FROM sqlite_master; SELECT * FROM sqlite_temp_schema; SELECT * FROM sqlite_temp_master",
	"PRAGMA table_info(t)",
	"PRAGMA table_info = t",
	"PRAGMA TABLE_INFO('t')",
	"PRAGMA main.table_info(t)",
	"SELECT * FROM pragma_table_info('t')",
	"SELECT * FROM pragma_table_info('t', 'main')",
	"PRAGMA table_xinfo(t)",
	"SELECT * FROM pragma_table_xinfo('t')",
	"PRAGMA index_list(t)",
	"SELECT * FROM PRAGMA_index_list('t')",
	"PRAGMA index_info(ti)",
	"SELECT name FROM pragma_index_info('ti')",
	"PRAGMA foreign_key_list(c)",
	"SELECT * FROM pragma_foreign_key_list('c')",
	"PRAGMA foreign_keys",
	"PRAGMA foreign_keys = OFF; INSERT INTO c VALUES (1, 99); PRAGMA foreign_keys = ON",
	"SELECT * FROM pragma_foreign_keys",
	"SELECT sqlite_version(), base64(x'00'), json_object('a', 1) ->> '$.a'",
	"SELECT abs(-1), random(), last_insert_rowid(), changes(), unixepoch('now'), printf('%d', 1), ieee754(1.5)",
	"SELECT count(*) OVER (), row_number() OVER (ORDER BY a) FROM t",
	"SELECT * FROM generate_series(1, 3); SELECT * FROM json_each('[1, 2]')",
	"SELECT 'a' LIKE 'A', 'a' GLOB 'a', replace('aa', 'a', 'b'), instr('ab', 'b')",
	"CREATE TABLE x(a INTEGER PRIMARY KEY, b TEXT NOT NULL) STRICT; CREATE UNIQUE INDEX xi ON x(b); REINDEX xi; ANALYZE; DROP INDEX xi; DROP TABLE x",
	"CREATE VIEW v AS SELECT abs(a) FROM t; SELECT * FROM v; DROP VIEW v",
	"CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (new.a); END; INSERT INTO t VALUES (1); DROP TRIGGER tr",
	"CREATE TABLE ck(a CHECK (length(a) < 5), b DEFAULT (abs(-1)), g AS (upper(a))); INSERT INTO ck(a) VALUES ('abc')",
	"CREATE TEMP TABLE tt(a); CREATE INDEX temp.tti ON tt(a); DROP INDEX temp.tti; CREATE TEMP VIEW tv AS SELECT 1; DROP VIEW tv; DROP TABLE tt",
	"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100) SELECT sum(x) FROM c",
	"VALUES (1), (2); SELECT 1 UNION SELECT 2 UNION ALL SELECT 3",
	"INSERT INTO t VALUES (1) RETURNING a; UPDATE t SET a = 2 RETURNING *; DELETE FROM t RETURNING a",
	"INSERT INTO p VALUES (1); INSERT INTO c VALUES (1, 1)",
	"VACUUM temp", // a no-op in SQLite (vacuum.c); it attaches nothing
	"SELECT 1; /* trailing comment */  ",
}

func TestAllowed(t *testing.T) {
	for _, sql := range allowed {
		t.Run(sql, func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				db.mustRun(t, "CREATE TABLE t(a); CREATE TABLE u(a); CREATE INDEX ti ON t(a); CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY, p REFERENCES p(id))")
				before := db.owned(t)
				db.mustRun(t, sql)
				after := db.owned(t)
				if !reflect.DeepEqual(before, after) {
					t.Errorf("owned settings changed:\n%v\n%v", before, after)
				}
				db.checkFiles(t)
			})
		})
	}
}

// alterForms are the forms of ALTER TABLE in SQLite 3.54.1 (parse.y), run on
// a database that holds t(a INTEGER), with one row, p(id INTEGER PRIMARY KEY),
// the STRICT table s(a TEXT), ck(a CHECK (a > 0)) and a view and trigger on t.
// Together they use every internal function in alterFunctions and the
// quick_check that SQLite runs itself.
var alterForms = []string{
	"ALTER TABLE t RENAME TO t2",         // sqlite_rename_table, sqlite_rename_test
	"ALTER TABLE t RENAME COLUMN a TO b", // sqlite_rename_column, sqlite_rename_quotefix
	"ALTER TABLE t RENAME a TO b",
	"ALTER TABLE t ADD COLUMN z TEXT",
	"ALTER TABLE t ADD z INTEGER NOT NULL DEFAULT 0",
	"ALTER TABLE t ADD COLUMN z AS (a + 1)",
	"ALTER TABLE s ADD COLUMN z TEXT",                         // quick_check, on a STRICT table
	"ALTER TABLE ck ADD COLUMN z TEXT",                        // quick_check, on a table with CHECK
	"ALTER TABLE t ADD COLUMN z; ALTER TABLE t DROP COLUMN z", // sqlite_drop_column
	"ALTER TABLE t ADD COLUMN z; ALTER TABLE t DROP z",
	"ALTER TABLE t ADD CONSTRAINT c1 CHECK (a > 0)", // sqlite_find_constraint, sqlite_fail, sqlite_add_constraint
	"ALTER TABLE t ADD CHECK (a > 0)",
	"ALTER TABLE t ADD CONSTRAINT c1 CHECK (a > 0); ALTER TABLE t DROP CONSTRAINT c1", // sqlite_drop_constraint
	"ALTER TABLE t ALTER COLUMN a SET NOT NULL",
	"ALTER TABLE t ALTER a SET NOT NULL ON CONFLICT FAIL",
	"ALTER TABLE t ALTER COLUMN a SET NOT NULL; ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
	"ALTER TABLE t ALTER a DROP NOT NULL",
}

func TestAlterTableForms(t *testing.T) {
	for _, sql := range alterForms {
		t.Run(sql, func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				db.mustRun(t, "CREATE TABLE t(a INTEGER); INSERT INTO t VALUES (1); CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE s(a TEXT) STRICT; CREATE TABLE ck(a CHECK (a > 0)); INSERT INTO ck VALUES (1); CREATE VIEW tv AS SELECT a FROM t; CREATE TRIGGER tg AFTER INSERT ON t BEGIN SELECT new.a; END")
				before := db.owned(t)
				db.mustRun(t, sql)
				if after := db.owned(t); !reflect.DeepEqual(before, after) {
					t.Errorf("owned settings changed:\n%v\n%v", before, after)
				}
				db.checkFiles(t)
			})
		})
	}
	// The quick_check that ALTER TABLE ADD COLUMN runs refuses a row that
	// breaks the table's CHECK constraint once the column exists.
	db := openDB(t, false, Limits{})
	db.mustRun(t, "CREATE TABLE ck(a, CHECK (a > 0)); INSERT INTO ck VALUES (1)")
	if err := db.run("ALTER TABLE ck ADD COLUMN c CHECK (c > 0) DEFAULT 0"); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("got %v, want the CHECK constraint failure from quick_check", err)
	}
	wantRefused(t, db.run("PRAGMA quick_check"), ReasonPragma) // the ALTER's permission ends with its Run
}

func TestSavepointsInsideAServerTransaction(t *testing.T) {
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		db.mustRun(t, "CREATE TABLE t(a)")
		db.server(t, "BEGIN IMMEDIATE")
		db.mustRun(t, "INSERT INTO t VALUES (1); SAVEPOINT s; INSERT INTO t VALUES (2); ROLLBACK TO s; RELEASE s; SAVEPOINT r; INSERT INTO t VALUES (3); RELEASE r")
		wantRefused(t, db.run("COMMIT"), ReasonTransaction)
		wantRefused(t, db.run("ROLLBACK"), ReasonTransaction)
		if db.raw.GetAutocommit() {
			t.Fatal("a client statement ended the server's transaction")
		}
		db.server(t, "COMMIT")
		if r, err := db.rows("SELECT group_concat(a) FROM t"); err != nil || r[0][0] != "1,3" {
			t.Fatalf("got %v %v", r, err)
		}
	})
}

func TestOneStatementPerRun(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		reason Reason // "" when Run must succeed
	}{
		{"INSERT INTO t VALUES (1)", ""},
		{"INSERT INTO t VALUES (1);", ""},
		{"INSERT INTO t VALUES (1) ; \n\t", ""},
		{"INSERT INTO t VALUES (1); -- comment", ""},
		{"INSERT INTO t VALUES (1) /* comment */;", ""},
		{"INSERT INTO t VALUES (1); /* unterminated comment", ""},
		{"INSERT INTO t VALUES (1);\f\r\n", ""},
		{"INSERT INTO t VALUES (1); /*/ still a comment */", ""},
		{"", ReasonStatementCount},
		{"  \n", ReasonStatementCount},
		{"-- only a comment", ReasonStatementCount},
		{";", ReasonStatementCount},
		{"INSERT INTO t VALUES (1);;", ReasonStatementCount},
		{"INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)", ReasonStatementCount},
		{"INSERT INTO t VALUES (1); x", ReasonStatementCount},
		{"INSERT INTO t VALUES (1); -- comment\nINSERT INTO t VALUES (2)", ReasonStatementCount},
		{"INSERT INTO t VALUES (1); /* comment */ INSERT INTO t VALUES (2)", ReasonStatementCount},
		{"INSERT INTO t VALUES (1);\vINSERT INTO t VALUES (2)", ReasonStatementCount},
		{"INSERT INTO t VALUES (1);\v", ReasonStatementCount}, // SQLite's tokenizer does not treat \v as white space
		{"INSERT INTO t VALUES (1); ATTACH '{dir}/second.db' AS second", ReasonStatementCount},
		{"INSERT INTO t VALUES (1);\x00PRAGMA journal_mode = OFF", ReasonNUL},
		{"INSERT INTO t VALUES (1)\x00", ReasonNUL},
		{"\x00INSERT INTO t VALUES (1)", ReasonNUL},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			db := openDB(t, true, Limits{})
			db.mustRun(t, "CREATE TABLE t(a)")
			err := db.conn.Run(context.Background(), strings.ReplaceAll(tc.sql, "{dir}", db.dir), nil)
			want := "0"
			if tc.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				want = "1"
			} else {
				wantRefused(t, err, tc.reason)
			}
			// A refused string runs nothing, not even its first statement.
			if r, err := db.rows("SELECT count(*) FROM t"); err != nil || r[0][0] != want {
				t.Fatalf("t holds %v rows (%v), want %s", r, err, want)
			}
			db.checkFiles(t)
		})
	}
}

func TestSQLLengthIsRefusedBeforePreparing(t *testing.T) {
	db := openDB(t, false, Limits{MaxSQLLength: 100})
	sql := "SELECT '" + rep("x", 90) + "'"
	db.mustRun(t, sql+rep(" ", 100-len(sql)))
	wantRefused(t, db.run(sql+rep(" ", 101-len(sql))), ReasonSQLLength)
	// The default is 1 MiB.
	db = openDB(t, false, Limits{})
	sql = "SELECT 1" + rep(" ", 1<<20-8)
	db.mustRun(t, sql)
	wantRefused(t, db.run(sql+" "), ReasonSQLLength)
}

func TestForeignKeysAreEnforcedByDefault(t *testing.T) {
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		db.mustRun(t, "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(p REFERENCES p(id))")
		if err := db.run("INSERT INTO c VALUES (1)"); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
			t.Fatalf("got %v, want a foreign key violation", err)
		}
	})
}

// wantPragmas is the pragma allowlist; quick_check is allowed only inside
// ALTER TABLE, and is refused here.
var wantPragmas = []string{"foreign_key_list", "foreign_keys", "index_info", "index_list", "table_info", "table_xinfo"}

func TestPragmaAllowlist(t *testing.T) {
	if got := slices.Sorted(maps.Keys(allowedPragmas)); !slices.Equal(got, wantPragmas) {
		t.Errorf("allowed pragmas %v, want %v", got, wantPragmas)
	}
	all := registry(t, "SELECT name FROM pragma_pragma_list")
	for _, p := range append(wantPragmas, "quick_check") {
		if !all[p] {
			t.Errorf("allowed pragma %s does not exist in this build", p)
		}
	}
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		// A table-valued function runs its pragma, and is refused, only when
		// the statement scans it, which here needs a row in t.
		db.mustRun(t, "CREATE TABLE t(a); INSERT INTO t VALUES (1)")
		before, schema := db.owned(t), db.schema(t)
		for p := range all {
			if slices.Contains(wantPragmas, p) {
				continue
			}
			for _, form := range []string{"PRAGMA %s", "PRAGMA %s = 0", "PRAGMA %s(0)", "PRAGMA main.%s = 1", "PRAGMA temp.%s(1)"} {
				sql := fmt.Sprintf(form, p)
				t.Run(sql, func(t *testing.T) { wantRefused(t, db.run(sql), ReasonPragma) })
			}
			// Only pragmas that return rows have a table-valued function.
			// With an argument it does not take, SQLite may fail the
			// statement before the policy sees the pragma.
			exists := hasEponymousTable(t, p)
			for i, form := range []string{"SELECT * FROM pragma_%s", "SELECT * FROM t, pragma_%s('main')", "SELECT * FROM pragma_%s(0)", "SELECT * FROM t, pragma_%s(t.a, 'main')"} {
				sql := fmt.Sprintf(form, p)
				t.Run(sql, func(t *testing.T) {
					err := db.run(sql)
					var pe *Error
					switch {
					case err == nil:
						t.Fatal("succeeded")
					case exists && i == 0, errors.As(err, &pe):
						wantRefused(t, err, ReasonPragma)
					}
				})
			}
		}
		checkUnchanged(t, db, before, schema)
	})
}

// hasEponymousTable reports whether pragma_p exists as a table-valued function.
func hasEponymousTable(t *testing.T, p string) bool {
	raw, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	s, _, err := raw.Prepare("SELECT * FROM pragma_" + p)
	_ = s.Close()
	if err != nil && !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("pragma_%s: %v", p, err)
	}
	return err == nil
}

// registry returns the names a query against an unrestricted connection
// lists.
func registry(t *testing.T, sql string) map[string]bool {
	t.Helper()
	raw, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	s, _, err := raw.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	m := map[string]bool{}
	for s.Step() {
		m[s.ColumnText(0)] = true
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return m
}

// wantFunctions and refusedFunctions together are every function ncruces
// v0.37.0 registers.
var wantFunctions = []string{
	"->", "->>", "abs", "acos", "acosh", "asin", "asinh", "atan", "atan2", "atanh", "avg", "base64", "ceil",
	"ceiling", "changes", "char", "coalesce", "concat", "concat_ws", "cos", "cosh", "count", "cume_dist",
	"current_date", "current_time", "current_timestamp", "date", "datetime", "degrees", "dense_rank", "exp",
	"first_value", "floor", "format", "glob", "group_concat", "hex", "ieee754", "ieee754_exponent",
	"ieee754_from_blob", "ieee754_from_int", "ieee754_inc", "ieee754_mantissa", "ieee754_to_blob", "ieee754_to_int",
	"if", "ifnull", "iif", "instr", "json", "json_array", "json_array_insert", "json_array_length",
	"json_error_position", "json_extract", "json_group_array", "json_group_object", "json_insert", "json_object",
	"json_pretty", "json_quote", "json_remove", "json_replace", "json_set", "json_type", "json_valid", "jsonb",
	"jsonb_array", "jsonb_array_insert", "jsonb_extract", "jsonb_group_array", "jsonb_group_object", "jsonb_insert",
	"jsonb_object", "jsonb_remove", "jsonb_replace", "jsonb_set", "julianday", "lag", "last_insert_rowid",
	"last_value", "lead", "length", "like", "likelihood", "likely", "ln", "log", "log10", "log2", "lower", "ltrim",
	"max", "min", "mod", "nth_value", "ntile", "nullif", "octet_length", "percent_rank", "pi", "pow", "power",
	"printf", "quote", "radians", "random", "randomblob", "rank", "replace", "round", "row_number", "rtrim", "sign",
	"sin", "sinh", "soundex", "sqlite_version", "sqrt", "strftime", "string_agg", "substr", "substring", "subtype",
	"sum", "tan", "tanh", "time", "timediff", "total", "total_changes", "trim", "trunc", "typeof",
	"unicode", "unistr", "unistr_quote", "unixepoch", "unlikely", "upper", "zeroblob",
}

var refusedFunctions = []string{
	"decimal", "decimal_add", "decimal_cmp", "decimal_exp", "decimal_mul", "decimal_pow2", "decimal_sub",
	"decimal_sum", "json_patch", "jsonb_patch", "match", "regexp", "regexpi", "sqlite_compileoption_get",
	"sqlite_compileoption_used", "sqlite_log", "sqlite_source_id", "unhex",
}

var wantAlterFunctions = []string{
	"sqlite_add_constraint", "sqlite_drop_column", "sqlite_drop_constraint", "sqlite_fail", "sqlite_find_constraint",
	"sqlite_rename_column", "sqlite_rename_quotefix", "sqlite_rename_table", "sqlite_rename_test",
}

func TestFunctionAllowlist(t *testing.T) {
	if got := slices.Sorted(maps.Keys(allowedFunctions)); !slices.Equal(got, wantFunctions) {
		t.Errorf("allowed functions differ:\n got %v\nwant %v", got, wantFunctions)
	}
	if got := slices.Sorted(maps.Keys(alterFunctions)); !slices.Equal(got, wantAlterFunctions) {
		t.Errorf("ALTER functions %v, want %v", got, wantAlterFunctions)
	}
	registered := registry(t, "SELECT DISTINCT name FROM pragma_function_list")
	if got, want := slices.Sorted(maps.Keys(registered)), slices.Sorted(slices.Values(append(slices.Clone(wantFunctions), refusedFunctions...))); !slices.Equal(got, want) {
		t.Errorf("registered functions differ from allowed and refused:\n got %v\nwant %v", got, want)
	}
	// Each one, called with an argument count it takes, is refused or not.
	raw, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	db := openDB(t, false, Limits{})
	s, _, err := raw.Prepare("SELECT DISTINCT name, narg, type FROM pragma_function_list WHERE name NOT IN ('->', '->>')")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for s.Step() {
		name, narg, typ := s.ColumnText(0), s.ColumnInt(1), s.ColumnText(2)
		if narg < 0 {
			narg = 1
		}
		sql := "SELECT " + name + "(" + strings.TrimSuffix(rep("NULL, ", narg), ", ") + ")"
		if typ == "w" {
			sql += " OVER ()"
		}
		err := db.run(sql)
		var pe *Error
		if refused := errors.As(err, &pe); refused != slices.Contains(refusedFunctions, name) || refused && pe.Reason != ReasonFunction {
			t.Errorf("%s: %v", sql, err)
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestFunctionRegisteredLaterIsRefused(t *testing.T) {
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		for _, name := range []string{"load_extension", "readfile", "writefile", "edit", "fts3_tokenizer"} {
			err := db.raw.CreateFunction(name, -1, sqlite3.DETERMINISTIC, func(ctx sqlite3.Context, _ ...sqlite3.Value) {
				ctx.ResultText("called")
			})
			if err != nil {
				t.Fatal(err)
			}
			wantRefused(t, db.run(fmt.Sprintf("SELECT %s('{dir}/x')", name)), ReasonFunction)
		}
	})
}

func rep(s string, n int) string { return strings.Repeat(s, n) }

func TestLimits(t *testing.T) {
	l := Limits{MaxLength: 1000, MaxSQLLength: 500, MaxExprDepth: 10, MaxParserDepth: 60, MaxCompoundSelect: 3, MaxFunctionArgs: 4, MaxLikePatternLength: 10, MaxVariableNumber: 5, MaxTriggerDepth: 2}
	triggers := "CREATE TABLE t1(a); CREATE TABLE t2(a); CREATE TABLE t3(a); CREATE TABLE t4(a);" +
		"CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES (new.a); END;" +
		"CREATE TRIGGER r2 AFTER INSERT ON t2 BEGIN INSERT INTO t3 VALUES (new.a); END;" +
		"CREATE TRIGGER r3 AFTER INSERT ON t3 BEGIN INSERT INTO t4 VALUES (new.a); END"
	for _, tc := range []struct {
		name, setup, ok, over, msg string
		l                          Limits
	}{
		{"MaxLength", "", "SELECT zeroblob(1000)", "SELECT zeroblob(1001)", "string or blob too big", l},
		{"MaxLength printf", "", "SELECT printf('%.*c', 1000, 'x')", "SELECT printf('%.*c', 1001, 'x')", "string or blob too big", l},
		{"MaxLength default", "", "SELECT zeroblob(1048576)", "SELECT zeroblob(1048577)", "string or blob too big", Limits{}},
		{"MaxExprDepth", "", "SELECT 1" + rep(" + 1", 5), "SELECT 1" + rep(" + 1", 12), "Expression tree is too large", l},
		{"MaxParserDepth", "", "SELECT " + rep("(", 40) + "1" + rep(")", 40), "SELECT " + rep("(", 70) + "1" + rep(")", 70), "Recursion limit", l},
		{"MaxCompoundSelect", "", "SELECT 1 UNION SELECT 2 UNION SELECT 3", "SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4", "too many terms in compound SELECT", l},
		{"MaxFunctionArgs", "", "SELECT max(1, 2, 3, 4)", "SELECT max(1, 2, 3, 4, 5)", "too many arguments on function max", l},
		{"MaxLikePatternLength", "", "SELECT 'a' LIKE '" + rep("a", 10) + "'", "SELECT 'a' LIKE '" + rep("a", 11) + "'", "LIKE or GLOB pattern too complex", l},
		{"MaxLikePatternLength GLOB", "", "SELECT 'a' GLOB '" + rep("*", 10) + "'", "SELECT 'a' GLOB '" + rep("*", 11) + "'", "LIKE or GLOB pattern too complex", l},
		{"MaxVariableNumber", "", "SELECT ?5", "SELECT ?6", "variable number must be between ?1 and ?5", l},
		{"MaxTriggerDepth", triggers, "INSERT INTO t2 VALUES (1)", "INSERT INTO t1 VALUES (1)", "triggers nested too deep", l},
	} {
		t.Run(tc.name, func(t *testing.T) {
			both(t, tc.l, func(t *testing.T, db *testDB) {
				db.mustRun(t, tc.setup)
				db.mustRun(t, tc.ok)
				err := db.run(tc.over)
				var pe *Error
				if err == nil || errors.As(err, &pe) || !strings.Contains(err.Error(), tc.msg) {
					t.Fatalf("got %v, want an engine error containing %q", err, tc.msg)
				}
				db.mustRun(t, "SELECT 1")
			})
		})
	}
	t.Run("MaxVDBEOps", func(t *testing.T) {
		both(t, Limits{MaxVDBEOps: 2000}, func(t *testing.T, db *testDB) {
			db.mustRun(t, "SELECT "+rep("1, ", 9)+"1")
			wantRefused(t, db.run("SELECT 1 IN ("+rep("1, ", 3000)+"1)"), ReasonMemory)
			if err := db.run("SELECT 1"); !errors.Is(err, ErrBroken) {
				t.Fatalf("after a memory refusal: got %v, want ErrBroken", err)
			}
		})
	})
}

func TestMemoryExhaustionIsRefused(t *testing.T) {
	db := openDB(t, false, Limits{})
	// Temporary tables live in the connection's memory, which ncruces caps at
	// 256 MiB by default.
	wantRefused(t, db.run("CREATE TEMP TABLE b AS SELECT zeroblob(1000000) FROM generate_series(1, 300)"), ReasonMemory)
	if err := db.run("SELECT 1"); !errors.Is(err, ErrBroken) {
		t.Fatalf("got %v, want ErrBroken", err)
	}
	if err := db.conn.Close(); err != nil {
		t.Fatalf("closing after a memory refusal: %v", err)
	}
	openDB(t, false, Limits{}).mustRun(t, "SELECT 1")
}

// Statement shapes that recurse in the C code, built with n levels of
// nesting, each a compound SELECT of t terms joined by op, and an innermost
// expression of depth e.
func expr(e int) string { return "1" + rep(" + 1", e) }

func terms(t int, op string) string { return rep(" "+op+" SELECT 1", t-1) }

var nestings = []struct {
	name string
	gen  func(t, n, e int, op string) string
}{
	{"subqueries in FROM", func(t, n, e int, op string) string {
		s := "SELECT " + expr(e)
		for range n {
			s = "SELECT * FROM (" + s + ")" + terms(t, op)
		}
		return s
	}},
	{"views", func(t, n, e int, op string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "CREATE VIEW v0 AS SELECT %s AS a;", expr(e))
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, "CREATE VIEW v%d AS SELECT * FROM v%d%s;", i, i-1, terms(t, op))
		}
		fmt.Fprintf(&b, "SELECT * FROM v%d", n)
		return b.String()
	}},
	{"CTEs", func(t, n, e int, op string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "WITH c0 AS (SELECT %s)", expr(e))
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, ", c%d AS (SELECT * FROM c%d%s)", i, i-1, terms(t, op))
		}
		fmt.Fprintf(&b, " SELECT * FROM c%d", n)
		return b.String()
	}},
	{"views, CTEs and subqueries", func(t, n, e int, op string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "CREATE VIEW v0 AS SELECT %s AS a;", expr(e))
		for i := 1; i <= n; i++ {
			switch i % 3 {
			case 0:
				fmt.Fprintf(&b, "CREATE VIEW v%d AS SELECT * FROM v%d%s;", i, i-1, terms(t, op))
			case 1:
				fmt.Fprintf(&b, "CREATE VIEW v%d AS WITH c AS (SELECT * FROM v%d%s) SELECT * FROM c;", i, i-1, terms(t, op))
			case 2:
				fmt.Fprintf(&b, "CREATE VIEW v%d AS SELECT * FROM (SELECT * FROM v%d%s)%s;", i, i-1, terms(t, op), terms(t, op))
			}
		}
		fmt.Fprintf(&b, "SELECT * FROM v%d", n)
		return b.String()
	}},
	{"scalar subqueries", func(t, n, e int, op string) string {
		s := "SELECT " + expr(e)
		for range n {
			s = "SELECT (" + s + ")" + terms(t, op)
		}
		return s
	}},
}

// triggerChain builds d-1 tables whose AFTER INSERT triggers insert into the
// next one, and a last trigger whose body is body, then inserts a row.
func triggerChain(d int, body string) string {
	var b strings.Builder
	for i := range d {
		fmt.Fprintf(&b, "CREATE TABLE t%d(a);", i)
	}
	for i := range d - 1 {
		fmt.Fprintf(&b, "CREATE TRIGGER r%d AFTER INSERT ON t%d BEGIN INSERT INTO t%d VALUES (new.a); END;", i, i, i+1)
	}
	fmt.Fprintf(&b, "CREATE TRIGGER rz AFTER INSERT ON t%d BEGIN %s; END;", d-1, body)
	b.WriteString("INSERT INTO t0 VALUES (1)")
	return b.String()
}

// deepest runs gen(n) for n = 1, 2, ... on new connections under l until it
// fails, and fails the test if the connection broke. It returns the last n
// that succeeded and the error that stopped it.
func deepest(t *testing.T, l Limits, gen func(n int) string) (int, error) {
	t.Helper()
	for n := 1; ; n++ {
		db := openDB(t, false, l)
		err := db.run(gen(n))
		if errors.Is(err, ErrBroken) || n > 1000 {
			t.Fatalf("n = %d: %v", n, err)
		}
		if err != nil {
			return n - 1, err
		}
	}
}

// Every nesting, at the deepest the limits allow, fails with SQLite's error
// and leaves the connection usable, at the defaults and at corners of the
// range that New accepts. The figures in the comments are where the
// connection broke with SQLite's own limits, measured on this build.
func TestNestingAtTheLimitsStaysInsideTheStack(t *testing.T) {
	for _, tc := range []struct {
		l             Limits
		views, nested int // deepest nesting of views and of subqueries in FROM
	}{
		{Limits{}, 16, 15}, // 4 terms: views broke at 47 levels, FROM at 48
		{Limits{MaxCompoundSelect: 11, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 1}, 8, 6},   // 11 terms: broke at 17
		{Limits{MaxCompoundSelect: 1, MaxExprDepth: 90, MaxParserDepth: 100, MaxTriggerDepth: 1}, 88, 15}, // 1 term: broke at 197
		{Limits{MaxCompoundSelect: 1, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 33}, 8, 6},
	} {
		tt := tc.l.MaxCompoundSelect
		if tt == 0 {
			tt = 4
		}
		d := tc.l.MaxTriggerDepth
		if d == 0 {
			d = 8
		}
		for _, op := range []string{"UNION ALL", "INTERSECT"} {
			for _, sh := range nestings {
				t.Run(fmt.Sprintf("%+v/%s/%s", tc.l, op, sh.name), func(t *testing.T) {
					n, err := deepest(t, tc.l, func(n int) string { return sh.gen(tt, n, 0, op) })
					var pe *Error
					if errors.As(err, &pe) {
						t.Fatal(err)
					}
					switch sh.name {
					case "views", "CTEs":
						if n != tc.views {
							t.Errorf("deepest %d, want %d", n, tc.views)
						}
					case "subqueries in FROM":
						if n != tc.nested {
							t.Errorf("deepest %d, want %d", n, tc.nested)
						}
					}
					// The same nesting inside the deepest trigger of a chain
					// at the trigger limit.
					if _, err := deepest(t, tc.l, func(n int) string { return triggerChain(d, sh.gen(tt, n, 0, op)) }); errors.As(err, &pe) {
						t.Fatal(err)
					}
				})
			}
		}
	}
	// The trigger chain alone, and an expression alone.
	for _, l := range []Limits{{}, {MaxCompoundSelect: 1, MaxExprDepth: 10, MaxParserDepth: 50, MaxTriggerDepth: 33}, {MaxCompoundSelect: 1, MaxExprDepth: 90, MaxTriggerDepth: 1}} {
		d, _ := deepest(t, l, func(n int) string { return triggerChain(n, "SELECT 1") })
		e, _ := deepest(t, l, func(n int) string { return "SELECT " + expr(n) })
		if want := max(l.MaxTriggerDepth, 8*(1-min(l.MaxTriggerDepth, 1))); d != want {
			t.Errorf("%+v: deepest trigger chain %d, want %d", l, d, want)
		}
		if want := max(l.MaxExprDepth, 18*(1-min(l.MaxExprDepth, 1))) - 1; e != want {
			t.Errorf("%+v: deepest expression %d, want %d", l, e, want)
		}
	}
	// A LIKE pattern at its limit, against the overflow from about 9,000 bytes.
	db := openDB(t, false, Limits{})
	db.mustRun(t, "SELECT '"+rep("a", 2040)+"b' LIKE '"+rep("%a", 2048)+"'")
	db.mustRun(t, "SELECT '"+rep("a", 2040)+"b' GLOB '"+rep("*a", 2048)+"'")
}

// With SQLite's own limits a statement overflows the C stack. Run contains
// the panic to the one connection.
func TestEnginePanicBreaksOnlyItsConnection(t *testing.T) {
	db := openDB(t, false, Limits{})
	other := openDB(t, false, Limits{})
	db.raw.Limit(sqlite3.LIMIT_EXPR_DEPTH, 1000) // behind the policy's back
	if err := db.run("SELECT 1" + rep(" + 1", 999)); !errors.Is(err, ErrBroken) {
		t.Fatalf("got %v, want ErrBroken", err)
	}
	for _, err := range []error{db.run("SELECT 1"), db.conn.Privileged(func(*sqlite3.Conn) error { return nil }), db.conn.Reset(), db.conn.Close()} {
		if !errors.Is(err, ErrBroken) {
			t.Fatalf("after the panic: got %v, want ErrBroken", err)
		}
	}
	other.mustRun(t, "SELECT 1")
}

func TestCallerPanicIsNotABrokenEngine(t *testing.T) {
	db := openDB(t, false, Limits{})
	for name, f := range map[string]func(){
		"Run": func() {
			_ = db.conn.Run(context.Background(), "SELECT 1", func(*sqlite3.Stmt) error {
				i := len(os.Args) // a runtime error in the caller's own code
				_ = os.Args[i]
				return nil
			})
		},
		"Privileged": func() {
			_ = db.conn.Privileged(func(c *sqlite3.Conn) error { panic("caller") })
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: the caller's panic was swallowed", name)
				}
			}()
			f()
		}()
		db.mustRun(t, "SELECT 1")
		wantRefused(t, db.run("PRAGMA journal_mode"), ReasonPragma) // the authorizer is back
	}
}

func TestPrivileged(t *testing.T) {
	db := openDB(t, true, Limits{})
	var mode string
	if err := db.conn.Privileged(func(c *sqlite3.Conn) error {
		return rows(c, "PRAGMA journal_mode", func(s *sqlite3.Stmt) { mode = s.ColumnText(0) })
	}); err != nil || mode != "wal" {
		t.Fatalf("got %q, %v", mode, err)
	}
	want := errors.New("fn failed")
	if err := db.conn.Privileged(func(*sqlite3.Conn) error { return want }); !errors.Is(err, want) {
		t.Fatalf("got %v, want fn's error", err)
	}
	wantRefused(t, db.run("PRAGMA journal_mode"), ReasonPragma)
}

func TestReset(t *testing.T) {
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		db.mustRun(t, "CREATE TABLE t(a)")
		before, schema := db.owned(t), db.schema(t)
		db.mustRun(t, `
			PRAGMA foreign_keys = OFF;
			CREATE TEMP TABLE tt(a);
			CREATE TEMP TABLE "x""y"(id INTEGER PRIMARY KEY AUTOINCREMENT);
			INSERT INTO "x""y" DEFAULT VALUES;
			CREATE INDEX temp.tti ON tt(a);
			CREATE TEMP VIEW tv AS SELECT * FROM tt;
			CREATE TEMP TRIGGER tr AFTER INSERT ON main.t BEGIN INSERT INTO tt VALUES (new.a); END;
			INSERT INTO t VALUES (1);
			ANALYZE temp`)
		db.server(t, "BEGIN")
		if err := db.conn.Reset(); err != nil {
			t.Fatal(err)
		}
		if !db.raw.GetAutocommit() || db.raw.LastInsertRowID() != 0 {
			t.Error("a transaction is open, or the last row id is kept")
		}
		if r, err := db.rows("SELECT type, name FROM temp.sqlite_schema WHERE name <> 'sqlite_sequence' UNION ALL SELECT 'row', name FROM temp.sqlite_sequence"); err != nil || len(r) != 0 {
			t.Errorf("temp schema after Reset: %v %v", r, err)
		}
		if r, err := db.rows("PRAGMA foreign_keys"); err != nil || r[0][0] != "1" {
			t.Errorf("foreign_keys after Reset: %v %v", r, err)
		}
		// SQLite does not let the temp schema's sqlite_sequence be dropped;
		// Reset leaves it empty.
		checkUnchanged(t, db, before, append(schema, []string{"table", "sqlite_sequence", "sqlite_sequence", "CREATE TABLE sqlite_sequence(name,seq)"}))
	})
}

func TestResetDropsTablesThatReferenceEachOther(t *testing.T) {
	db := openDB(t, false, Limits{})
	db.mustRun(t, `
		CREATE TEMP TABLE parent(id INTEGER PRIMARY KEY);
		CREATE TEMP TABLE child(p REFERENCES parent);
		INSERT INTO parent VALUES (1);
		INSERT INTO child VALUES (1)`)
	if err := db.conn.Reset(); err != nil {
		t.Fatal(err)
	}
	db.mustRun(t, "SELECT 1")
}

func TestRunReturnsAStepErrorThatFnIgnored(t *testing.T) {
	db := openDB(t, false, Limits{})
	db.mustRun(t, "CREATE TABLE t(a NOT NULL)")
	err := db.conn.Run(context.Background(), "INSERT INTO t VALUES (NULL)", func(s *sqlite3.Stmt) error {
		s.Step()
		return nil
	})
	if !errors.Is(err, sqlite3.CONSTRAINT) {
		t.Fatalf("got %v, want the constraint error", err)
	}
}

func TestResetFailureBreaksTheConnection(t *testing.T) {
	db := openDB(t, false, Limits{})
	db.mustRun(t, "CREATE TEMP TABLE tt(a)")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db.raw.SetInterrupt(ctx) // every statement Reset runs is interrupted
	if err := db.conn.Reset(); !errors.Is(err, ErrBroken) {
		t.Fatalf("got %v, want ErrBroken", err)
	}
	db.raw.SetInterrupt(context.Background())
	if err := db.run("SELECT 1"); !errors.Is(err, ErrBroken) {
		t.Fatalf("after a failed Reset: got %v, want ErrBroken", err)
	}
}

func TestTimeLimit(t *testing.T) {
	const cpuBound = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"
	l := Limits{Timeout: 100 * time.Millisecond}

	t.Run("recursive query", func(t *testing.T) {
		both(t, l, func(t *testing.T, db *testDB) {
			wantRefused(t, db.run(cpuBound), ReasonTimeLimit)
			if !db.raw.GetAutocommit() {
				t.Error("a transaction is still open")
			}
			db.mustRun(t, "CREATE TABLE t(a); INSERT INTO t VALUES (1)")
			if r, err := db.rows("SELECT count(*) FROM t"); err != nil || r[0][0] != "1" {
				t.Fatalf("after the interrupt: %v %v", r, err)
			}
		})
	})

	t.Run("write in a transaction", func(t *testing.T) {
		both(t, l, func(t *testing.T, db *testDB) {
			db.mustRun(t, "CREATE TABLE t(a)")
			db.server(t, "BEGIN")
			db.mustRun(t, "INSERT INTO t VALUES (1)")
			wantRefused(t, db.run("INSERT INTO t SELECT value FROM generate_series(1, 1000000000000)"), ReasonTimeLimit)
			// SQLite rolls back the whole transaction when it interrupts a write.
			if !db.raw.GetAutocommit() {
				t.Fatal("the transaction is still open")
			}
			if r, err := db.rows("SELECT count(*) FROM t"); err != nil || r[0][0] != "0" {
				t.Fatalf("got %v %v, want the transaction rolled back", r, err)
			}
		})
	})

	t.Run("read in a transaction", func(t *testing.T) {
		both(t, l, func(t *testing.T, db *testDB) {
			db.mustRun(t, "CREATE TABLE t(a)")
			db.server(t, "BEGIN")
			db.mustRun(t, "INSERT INTO t VALUES (1)")
			wantRefused(t, db.run(cpuBound), ReasonTimeLimit)
			// An interrupted read leaves the transaction open.
			if db.raw.GetAutocommit() {
				t.Fatal("the transaction was rolled back")
			}
			db.server(t, "COMMIT")
			if r, err := db.rows("SELECT count(*) FROM t"); err != nil || r[0][0] != "1" {
				t.Fatalf("got %v %v", r, err)
			}
		})
	})

	t.Run("another statement is prepared", func(t *testing.T) {
		both(t, l, func(t *testing.T, db *testDB) {
			other, _, err := db.raw.Prepare("SELECT value FROM generate_series(1, 5)")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = other.Close() }()
			if !other.Step() || other.ColumnInt(0) != 1 {
				t.Fatal(other.Err())
			}
			wantRefused(t, db.run(cpuBound), ReasonTimeLimit)
			var got []int
			for other.Step() {
				got = append(got, other.ColumnInt(0))
			}
			if err := other.Err(); err != nil || !reflect.DeepEqual(got, []int{2, 3, 4, 5}) {
				t.Fatalf("the other statement returned %v, %v", got, err)
			}
		})
	})

	t.Run("caller cancels", func(t *testing.T) {
		db := openDB(t, false, l)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := db.conn.Run(ctx, "SELECT 1", nil)
		var pe *Error
		if !errors.Is(err, sqlite3.INTERRUPT) || errors.As(err, &pe) {
			t.Fatalf("got %v, want the engine's interrupt error", err)
		}
		db.mustRun(t, "SELECT 1")
	})
}

func TestErrorCarriesNoSQL(t *testing.T) {
	db := openDB(t, false, Limits{})
	err := db.run("PRAGMA journal_mode = OFF -- secret")
	wantRefused(t, err, ReasonPragma)
	if got := err.Error(); got != "sqlpolicy: refused (pragma)" {
		t.Fatalf("error text %q", got)
	}
}

func TestRefusalDoesNotCarryOverToTheNextRun(t *testing.T) {
	db := openDB(t, false, Limits{})
	wantRefused(t, db.run("ATTACH ':memory:' AS m"), ReasonAttach)
	err := db.run("SELEC 1")
	var pe *Error
	if err == nil || errors.As(err, &pe) {
		t.Fatalf("got %v, want the engine's syntax error", err)
	}
}

// The policy's two-argument trims return what SQLite's own do.
func TestTrimMatchesSQLite(t *testing.T) {
	raw, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	db := openDB(t, false, Limits{})
	values := []any{nil, "", "x", "  ab  ", "xxaxx", "aé€😀ba", "\xc3\xa9\xc3", "\xc3", "\xc3\xc3a\xc3\xc3", "\x80\x80a\x80", "a\x00b", []byte("\x00ab\x00"), 12.5, int64(-120)}
	sets := []any{nil, "", "x", " ", "ab", "é", "\xc3", "\xc3\xc3", "\x80", "\x80\x80", "\xc3\xa9\x80", "😀a", "a\x00b", []byte("\x00"), 1, "€\xe2"}
	for _, fn := range []string{"trim", "ltrim", "rtrim"} {
		sql := "SELECT quote(" + fn + "(?1, ?2))"
		for _, v := range values {
			for _, set := range sets {
				var want, got string
				run := func(s *sqlite3.Stmt) (string, error) {
					for i, a := range []any{v, set} {
						if err := bind(s, i+1, a); err != nil {
							return "", err
						}
					}
					s.Step()
					return s.ColumnText(0), s.Err()
				}
				s, _, err := raw.Prepare(sql)
				if err != nil {
					t.Fatal(err)
				}
				want, err = run(s)
				_ = s.Close()
				if err != nil {
					t.Fatal(err)
				}
				if err := db.conn.Run(context.Background(), sql, func(s *sqlite3.Stmt) (err error) { got, err = run(s); return err }); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Errorf("%s(%q, %q) = %s, SQLite's is %s", fn, v, set, got, want)
				}
			}
		}
	}
	for _, fn := range []string{"trim", "ltrim", "rtrim"} {
		db.mustRun(t, "SELECT "+fn+"('x', '"+rep("é", 64)+"')")
		wantRefused(t, db.run("SELECT "+fn+"('x', '"+rep("é", 65)+"')"), ReasonFunction)
	}
	db.mustRun(t, "SELECT trim('"+rep(" ", 100)+"')") // one argument is SQLite's
	// Like SQLite's, the policy's trims may be used in schema objects.
	db.mustRun(t, "CREATE VIEW tv AS SELECT trim('xax', 'x') AS a, ltrim('xa', 'x'), rtrim('ax', 'x'); SELECT * FROM tv")
}

func bind(s *sqlite3.Stmt, i int, a any) error {
	switch v := a.(type) {
	case nil:
		return s.BindNull(i)
	case string:
		return s.BindText(i, v)
	case []byte:
		return s.BindBlob(i, v)
	case float64:
		return s.BindFloat(i, v)
	case int:
		return s.BindInt64(i, int64(v))
	case int64:
		return s.BindInt64(i, v)
	}
	return fmt.Errorf("unsupported argument %T", a)
}
