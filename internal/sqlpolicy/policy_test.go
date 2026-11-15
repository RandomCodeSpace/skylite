package sqlpolicy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
)

func TestNewDefaultsAndValidation(t *testing.T) {
	p, err := New(Limits{MaxExprDepth: 7})
	if err != nil {
		t.Fatal(err)
	}
	got := p.limits
	if got.MaxExprDepth != 7 {
		t.Errorf("MaxExprDepth = %d, want the 7 that was set", got.MaxExprDepth)
	}
	if got.Timeout != 5*time.Second || got.MaxSQLLength != 1<<20 || got.MaxParserDepth != 200 || got.MaxLikePatternLength != 4096 {
		t.Errorf("defaults not applied: %+v", got)
	}
	for _, f := range got.limits() {
		if *f.value <= 0 || *f.value > f.max {
			t.Errorf("%s = %d after New", f.name, *f.value)
		}
	}
	for _, bad := range []Limits{
		{MaxExprDepth: -1},
		{MaxExprDepth: 1001},
		{MaxParserDepth: 2501},
		{MaxVariableNumber: 32767},
		{MaxLength: 1_000_000_001},
		{Timeout: -time.Second},
	} {
		if _, err := New(bad); err == nil {
			t.Errorf("New(%+v) succeeded", bad)
		}
	}
}

func TestApplyConfiguresConnection(t *testing.T) {
	raw, err := sqlite3.Open(":memory:")
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
	l := Limits{MaxLength: 1000, MaxSQLLength: 900, MaxExprDepth: 11, MaxParserDepth: 52, MaxCompoundSelect: 13, MaxVDBEOps: 1400, MaxFunctionArgs: 15, MaxLikePatternLength: 16, MaxVariableNumber: 17, MaxTriggerDepth: 18}
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
	if got["temp_store"] != "2" {
		t.Errorf("temp_store = %s, want 2 (memory)", got["temp_store"])
	}
	for _, f := range l.limits() {
		if v := raw.Limit(f.id, -1); v != *f.value {
			t.Errorf("%s: limit %d = %d, want %d", f.name, f.id, v, *f.value)
		}
	}
	if v := raw.Limit(sqlite3.LIMIT_ATTACHED, -1); v != 0 {
		t.Errorf("attached-database limit = %d, want 0", v)
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
// database that holds the tables t(a), with one row, and u(a).
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
	{ReasonFunction, "SELECT sqlite_log(1, 'x')"},
	{ReasonFunction, "SELECT SQLITE_LOG(1, 'x')"},
	{ReasonFunction, "SELECT sqlite_compileoption_get(0)"},
	{ReasonFunction, "SELECT sqlite_compileoption_used('THREADSAFE')"},
	{ReasonFunction, "SELECT sqlite_source_id()"},
	{ReasonFunction, "SELECT match('a', 'b')"},
	{ReasonFunction, "SELECT a FROM t WHERE a = abs(sqlite_source_id())"},
	{ReasonFunction, "CREATE TABLE c(a CHECK (sqlite_log(1, 'x') IS NULL))"},
	{ReasonFunction, "CREATE TABLE d(a DEFAULT (sqlite_source_id()))"},
	{ReasonFunction, "CREATE TABLE g(a, b AS (sqlite_source_id()))"},
	{ReasonFunction, "CREATE INDEX i ON t(sqlite_source_id())"},
	{ReasonFunction, "CREATE VIEW v AS SELECT sqlite_source_id(); SELECT * FROM v"},
	{ReasonFunction, "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (sqlite_source_id()); END; INSERT INTO t VALUES (1)"},
	{ReasonFunction, "CREATE TEMP TRIGGER tr AFTER INSERT ON main.t BEGIN SELECT sqlite_source_id(); END; INSERT INTO t VALUES (1)"},
	{ReasonStatement, "CREATE VIRTUAL TABLE s USING generate_series"},
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
	// GORM reads and sets foreign_keys around DropTable and its table
	// rebuilds, ignoring the errors; see the record.
	{ReasonPragma, "PRAGMA foreign_keys"},
	{ReasonPragma, "PRAGMA foreign_keys = OFF"},
	{ReasonPragma, "PRAGMA foreign_keys = ''"}, // the authorizer cannot tell '' from no value
	{ReasonPragma, "PRAGMA database_list"},
	{ReasonPragma, "PRAGMA temp_store = FILE"},
	{ReasonPragma, "PRAGMA temp_store_directory = '{dir}'"},
	{ReasonPragma, "PRAGMA synchronous = OFF"},
	{ReasonPragma, "PRAGMA page_size = 8192"},
	{ReasonPragma, "PRAGMA wal_checkpoint(TRUNCATE)"},
	{ReasonPragma, "PRAGMA user_version = 7"},
	{ReasonPragma, "PRAGMA index_xinfo(t)"},
	{ReasonPragma, "SELECT * FROM t, pragma_table_list"},
}

func TestRefused(t *testing.T) {
	for _, tc := range refusals {
		t.Run(tc.sql, func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				db.mustRun(t, "CREATE TABLE t(a); INSERT INTO t VALUES (1); CREATE TABLE u(a)")
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
		if row[0] != "view" && row[0] != "trigger" && !containsRow(schema, row) {
			t.Errorf("schema gained %v", row)
		}
	}
	for _, f := range []string{"other.db", "uri.db", "uri2.db", "copy.db", "copy2.db", "second.db", "a.db"} {
		if _, err := os.Stat(filepath.Join(db.dir, f)); err == nil {
			t.Errorf("%s was created", f)
		}
	}
}

func containsRow(rows [][]string, row []string) bool {
	for _, r := range rows {
		if reflect.DeepEqual(r, row) {
			return true
		}
	}
	return false
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
	"SELECT sqlite_version(), base64(x'00'), 'a' REGEXP 'a', json_object('a', 1) ->> '$.a', decimal_add('1', '2')",
	"SELECT abs(-1), random(), last_insert_rowid(), changes(), unixepoch('now'), printf('%d', 1), ieee754(1.5)",
	"SELECT count(*) OVER (), row_number() OVER (ORDER BY a) FROM t",
	"SELECT * FROM generate_series(1, 3); SELECT * FROM json_each('[1, 2]')",
	"SELECT 'a' LIKE 'A', 'a' GLOB 'a', 'a' REGEXP '^a$'",
	"BEGIN; INSERT INTO t VALUES (1); SAVEPOINT s; INSERT INTO t VALUES (2); ROLLBACK TO s; RELEASE s; COMMIT",
	"BEGIN IMMEDIATE; COMMIT; BEGIN EXCLUSIVE; ROLLBACK; BEGIN DEFERRED; END",
	"CREATE TABLE x(a INTEGER PRIMARY KEY, b TEXT NOT NULL) STRICT; ALTER TABLE x ADD COLUMN c TEXT; ALTER TABLE x RENAME TO y; ALTER TABLE y RENAME COLUMN c TO d; ALTER TABLE y DROP COLUMN d; CREATE UNIQUE INDEX yi ON y(b); REINDEX yi; ANALYZE; DROP INDEX yi; DROP TABLE y",
	"CREATE VIEW v AS SELECT abs(a) FROM t; SELECT * FROM v; DROP VIEW v",
	"CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (new.a); END; INSERT INTO t VALUES (1); DROP TRIGGER tr",
	"CREATE TABLE ck(a CHECK (length(a) < 5), b DEFAULT (abs(-1)), g AS (upper(a))); INSERT INTO ck(a) VALUES ('abc')",
	"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100) SELECT sum(x) FROM c",
	"VALUES (1), (2); SELECT 1 UNION SELECT 2 UNION ALL SELECT 3",
	"INSERT INTO t VALUES (1) RETURNING a; UPDATE t SET a = 2 RETURNING *; DELETE FROM t RETURNING a",
	"INSERT INTO p VALUES (1); INSERT INTO c VALUES (1, 1)",
	"",
	"-- only a comment",
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

func TestForeignKeysAreEnforced(t *testing.T) {
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		db.mustRun(t, "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(p REFERENCES p(id))")
		if err := db.run("INSERT INTO c VALUES (1)"); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
			t.Fatalf("got %v, want a foreign key violation", err)
		}
	})
}

func TestPragmaAllowlist(t *testing.T) {
	all := registry(t, "SELECT name FROM pragma_pragma_list")
	for p := range allowedPragmas {
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
			if allowedPragmas[p] {
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

// refusedFunctions are the functions ncruces v0.37.0 registers that the
// policy refuses.
var refusedFunctions = set("sqlite_log", "sqlite_compileoption_get", "sqlite_compileoption_used", "sqlite_source_id", "match")

func TestFunctionAllowlistMatchesRegistry(t *testing.T) {
	registered := registry(t, "SELECT DISTINCT name FROM pragma_function_list")
	for name := range registered {
		if allowedFunctions[name] == refusedFunctions[name] {
			t.Errorf("function %s is registered but neither allowed nor refused", name)
		}
	}
	for name := range allowedFunctions {
		if !registered[name] {
			t.Errorf("allowed function %s is not registered", name)
		}
	}
	for name := range refusedFunctions {
		if !registered[name] {
			t.Errorf("refused function %s is not registered", name)
		}
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
	}{
		{"MaxLength", "", "SELECT zeroblob(1000)", "SELECT zeroblob(1001)", "string or blob too big"},
		{"MaxLength printf", "", "SELECT printf('%.*c', 1000, 'x')", "SELECT printf('%.*c', 1001, 'x')", "string or blob too big"},
		{"MaxSQLLength", "", "SELECT '" + rep("x", 400) + "'", "SELECT '" + rep("x", 600) + "'", "string or blob too big"},
		{"MaxExprDepth", "", "SELECT 1" + rep(" + 1", 5), "SELECT 1" + rep(" + 1", 12), "Expression tree is too large"},
		{"MaxParserDepth", "", "SELECT " + rep("(", 40) + "1" + rep(")", 40), "SELECT " + rep("(", 70) + "1" + rep(")", 70), "Recursion limit"},
		{"MaxCompoundSelect", "", "SELECT 1 UNION SELECT 2 UNION SELECT 3", "SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4", "too many terms in compound SELECT"},
		{"MaxFunctionArgs", "", "SELECT max(1, 2, 3, 4)", "SELECT max(1, 2, 3, 4, 5)", "too many arguments on function max"},
		{"MaxLikePatternLength", "", "SELECT 'a' LIKE '" + rep("a", 10) + "'", "SELECT 'a' LIKE '" + rep("a", 11) + "'", "LIKE or GLOB pattern too complex"},
		{"MaxLikePatternLength GLOB", "", "SELECT 'a' GLOB '" + rep("*", 10) + "'", "SELECT 'a' GLOB '" + rep("*", 11) + "'", "LIKE or GLOB pattern too complex"},
		{"MaxVariableNumber", "", "SELECT ?5", "SELECT ?6", "variable number must be between ?1 and ?5"},
		{"MaxTriggerDepth", triggers, "INSERT INTO t2 VALUES (1)", "INSERT INTO t1 VALUES (1)", "triggers nested too deep"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			both(t, l, func(t *testing.T, db *testDB) {
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
	wantRefused(t, db.run("CREATE TEMP TABLE b AS SELECT zeroblob(8000000) FROM generate_series(1, 64)"), ReasonMemory)
	if err := db.run("SELECT 1"); !errors.Is(err, ErrBroken) {
		t.Fatalf("got %v, want ErrBroken", err)
	}
	if err := db.conn.Close(); err != nil {
		t.Fatalf("closing after a memory refusal: %v", err)
	}
	openDB(t, false, Limits{}).mustRun(t, "SELECT 1")
}

func TestDefaultLimitsKeepNestingInsideTheStack(t *testing.T) {
	expr := "1" + rep(" + 1", 98)
	unionAll := "SELECT " + expr + rep(" UNION ALL SELECT "+expr, 99)
	for _, sql := range []string{
		"SELECT " + expr,
		"SELECT " + rep("abs(", 60) + "1" + rep(")", 60),
		"SELECT " + rep("CASE WHEN 1 THEN ", 98) + "1" + rep(" END", 98),
		"SELECT " + rep("NOT ", 98) + "1",
		unionAll,
		rep("SELECT * FROM (", 30) + unionAll + rep(")", 30),
		"SELECT " + rep("(SELECT ", 35) + expr + rep(")", 35),
		"SELECT '" + rep("a", 2040) + "b' LIKE '" + rep("%a", 2047) + "%'",
		"SELECT '" + rep("a", 2040) + "b' GLOB '" + rep("*a", 2047) + "*'",
		"SELECT json('" + rep("[", 999) + rep("]", 999) + "')",
		"SELECT 1 IN (" + rep("1, ", 32000) + "1)",
		"VALUES " + rep("(1, 2), ", 20000) + "(1, 2)",
		// One past each limit fails with SQLite's error, not a broken connection.
		"SELECT " + expr + " + 1 + 1",
		"SELECT " + rep("(", 250) + "1" + rep(")", 250),
		unionAll + " UNION ALL SELECT 1",
	} {
		t.Run(fmt.Sprintf("%.40s", sql), func(t *testing.T) {
			db := openDB(t, false, Limits{})
			if err := db.run(sql); errors.Is(err, ErrBroken) {
				t.Fatal(err)
			}
			db.mustRun(t, "SELECT 1")
		})
	}
}

func views(n int) string {
	var b strings.Builder
	b.WriteString("CREATE VIEW v0 AS SELECT 1;")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "CREATE VIEW v%d AS SELECT * FROM v%d;", i, i-1)
	}
	fmt.Fprintf(&b, "SELECT * FROM v%d", n)
	return b.String()
}

func ctes(n int) string {
	var b strings.Builder
	b.WriteString("WITH c0 AS (SELECT 1)")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, ", c%d AS (SELECT * FROM c%d)", i, i-1)
	}
	fmt.Fprintf(&b, " SELECT * FROM c%d", n)
	return b.String()
}

func triggerChain(n int) string {
	var b strings.Builder
	for i := 0; i <= n; i++ {
		fmt.Fprintf(&b, "CREATE TABLE t%d(a);", i)
	}
	for i := range n {
		fmt.Fprintf(&b, "CREATE TRIGGER r%d AFTER INSERT ON t%d BEGIN INSERT INTO t%d VALUES (new.a); END;", i, i, i+1)
	}
	b.WriteString("INSERT INTO t0 VALUES (1)")
	return b.String()
}

// Each statement here overflows the C stack at SQLite's own limit; at the
// policy's default it fails with SQLite's error instead.
func TestDefaultLimitsStopNestingBeforeTheStackOverflows(t *testing.T) {
	for _, tc := range []struct {
		ok, over, msg string
	}{
		{"SELECT 1" + rep(" + 1", 98), "SELECT 1" + rep(" + 1", 400), "Expression tree is too large"},
		{rep("SELECT * FROM (", 30) + "SELECT 1" + rep(")", 30), rep("SELECT * FROM (", 300) + "SELECT 1" + rep(")", 300), "Recursion limit"},
		{"SELECT 1" + rep(" UNION ALL SELECT 1", 99), "SELECT 1" + rep(" UNION ALL SELECT 1", 300), "too many terms in compound SELECT"},
		{"SELECT 'a' LIKE '" + rep("%a", 2048) + "'", "SELECT '" + rep("a", 6000) + "b' LIKE '" + rep("%a", 6000) + "%'", "LIKE or GLOB pattern too complex"},
		{views(98), views(250), "VIEWs and/or subqueries nested too deep"},
		{ctes(98), ctes(300), "VIEWs and/or subqueries nested too deep"},
		{triggerChain(32), triggerChain(100), "triggers nested too deep"},
	} {
		t.Run(fmt.Sprintf("%.30s", tc.over), func(t *testing.T) {
			db := openDB(t, false, Limits{})
			db.mustRun(t, tc.ok)
			db = openDB(t, false, Limits{})
			if err := db.run(tc.over); err == nil || errors.Is(err, ErrBroken) || !strings.Contains(err.Error(), tc.msg) {
				t.Fatalf("got %v, want an engine error containing %q", err, tc.msg)
			}
			db.mustRun(t, "SELECT 1")
		})
	}
}

// With SQLite's own limits a statement overflows the C stack. Run contains
// the panic to the one connection.
func TestEnginePanicBreaksOnlyItsConnection(t *testing.T) {
	db := openDB(t, false, Limits{MaxExprDepth: 1000})
	other := openDB(t, false, Limits{})
	if err := db.run("SELECT 1" + rep(" + 1", 999)); !errors.Is(err, ErrBroken) {
		t.Fatalf("got %v, want ErrBroken", err)
	}
	if err := db.run("SELECT 1"); !errors.Is(err, ErrBroken) {
		t.Fatalf("second Run: got %v, want ErrBroken", err)
	}
	if err := db.conn.Close(); !errors.Is(err, ErrBroken) {
		t.Fatalf("Close: got %v, want ErrBroken", err)
	}
	other.mustRun(t, "SELECT 1")
}

func TestTimeLimit(t *testing.T) {
	const cpuBound = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"
	l := Limits{Timeout: 100 * time.Millisecond}

	t.Run("recursive query", func(t *testing.T) {
		both(t, l, func(t *testing.T, db *testDB) {
			start := time.Now()
			wantRefused(t, db.run(cpuBound), ReasonTimeLimit)
			if d := time.Since(start); d > 30*time.Second {
				t.Errorf("interrupt took %v", d)
			}
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
			db.mustRun(t, "CREATE TABLE t(a); BEGIN; INSERT INTO t VALUES (1)")
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
			db.mustRun(t, "CREATE TABLE t(a); BEGIN; INSERT INTO t VALUES (1)")
			wantRefused(t, db.run(cpuBound), ReasonTimeLimit)
			// An interrupted read leaves the transaction open.
			if db.raw.GetAutocommit() {
				t.Fatal("the transaction was rolled back")
			}
			db.mustRun(t, "COMMIT")
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

func TestErrorEchoesABoundedPrefix(t *testing.T) {
	db := openDB(t, false, Limits{})
	sql := "PRAGMA journal_mode = OFF; -- " + rep("é", 100) + "secret"
	err := db.run(sql)
	wantRefused(t, err, ReasonPragma)
	var pe *Error
	errors.As(err, &pe)
	if !strings.HasPrefix(sql, pe.SQL) || len(pe.SQL) < 60 || strings.Contains(err.Error(), "secret") {
		t.Fatalf("SQL = %q", pe.SQL)
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
