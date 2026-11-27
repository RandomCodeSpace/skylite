package server

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/sqlpolicy"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// The tests in this file serve database "d", which exists before the
// server starts, so that its first candidacy takes office.

// newQueryEnv returns a started server that leads database "d". edit, if
// set, changes the server before it starts.
func newQueryEnv(t *testing.T, edit func(*env)) *env {
	t.Helper()
	return newLeaderEnv(t, nil, edit)
}

// newLeaderEnv is newQueryEnv on a store wrapped by wrap, if it is set.
func newLeaderEnv(t *testing.T, wrap func(storage.Store) storage.Store, edit func(*env)) *env {
	t.Helper()
	e := newEnv(t, wrap)
	e.s.cfg.Databases = []string{"d"}
	keys, err := e.s.cfg.Storage.keys("d")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Create(context.Background(), e.store, keys, pageSize, groupSize); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(e)
	}
	if err := e.s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	e.halt = e.start(t)
	e.leader(t)
	return e
}

// leader waits for the server to lead "d" and returns its leader.
func (e *env) leader(t *testing.T) *engine.Leader {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		st := e.s.dbs["d"].loop.State()
		if st.Leader != nil && st.Leader.Err() == nil {
			return st.Leader
		}
		select {
		case <-st.Changed:
		case <-deadline:
			t.Fatalf("the server did not take office: %v", st.Reason)
		}
	}
}

// lease_ttl is the time to live the server judges another holder's lease
// by: a lease last renewed 35 s ago has expired at 30 s, so the server takes
// office at once, where at the default 60 s it would wait 25 s, longer than
// leader waits.
func TestLeaseTTLShortensTakeover(t *testing.T) {
	start := time.Now()
	newLeaderEnv(t, nil, func(e *env) {
		e.s.cfg.LeaseTTL = minLeaseTTL
		keys, err := e.s.cfg.Storage.keys("d")
		if err != nil {
			t.Fatal(err)
		}
		ghost := engine.Candidate{Holder: "ghost", TTL: time.Minute, Now: func() time.Time { return time.Now().Add(-35 * time.Second) }}
		if _, _, err := engine.Takeover(context.Background(), e.store, keys, ghost); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("took office %v after the start", time.Since(start).Round(time.Millisecond))
}

var commitIDs atomic.Int64

// exec runs sql on "d" through the leader, without the policy, and commits
// it: test set-up, as no write path is served yet.
func (e *env) exec(t *testing.T, sql string) {
	t.Helper()
	l := e.leader(t)
	c, err := l.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Run(context.Background(), fmt.Appendf(nil, "test-%d", commitIDs.Add(1)), func(db *sqlite3.Conn) error { return db.Exec("BEGIN; " + sql + "; COMMIT") }); err != nil {
		t.Fatal(err)
	}
}

// pooled runs f, with the authorizer suspended, on the most recently used
// idle pooled connection, which stays pooled.
func (e *env) pooled(t *testing.T, f func(db *sqlite3.Conn) error) {
	t.Helper()
	p := e.s.pool
	p.mu.Lock()
	if len(p.idle) == 0 {
		p.mu.Unlock()
		t.Fatal("no idle pooled connection")
	}
	c := p.idle[len(p.idle)-1]
	p.mu.Unlock()
	if _, err := c.ec.Run(context.Background(), nil, func(*sqlite3.Conn) error { return c.pc.Privileged(f) }); err != nil {
		t.Fatal(err)
	}
}

// answer is a response's status and frames.
type answer struct {
	status int
	frames []map[string]any
}

func (a answer) last() map[string]any {
	if len(a.frames) == 0 {
		return nil
	}
	return a.frames[len(a.frames)-1]
}

func (a answer) code() string { return errorCode(a.last()) }

func (a answer) done() map[string]any {
	d, _ := a.last()["done"].(map[string]any)
	return d
}

// rows returns the rows of the answer, as JSON text each.
func (a answer) rows() []string {
	var rows []string
	for _, f := range a.frames {
		if f["row"] != nil {
			b, _ := json.Marshal(f["row"])
			rows = append(rows, string(b))
		}
	}
	return rows
}

// post sends a query or describe ("query" or "describe") to "d" as subject.
func (e *env) post(t *testing.T, path, subject, body string) answer {
	t.Helper()
	a, err := e.send(path, e.sig.token(t, subject), body)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// postAsync is post from another goroutine; the answer arrives on the
// channel, a zero answer if the request failed.
func (e *env) postAsync(t *testing.T, path, subject, body string) <-chan answer {
	t.Helper()
	token := e.sig.token(t, subject)
	got := make(chan answer, 1)
	go func() {
		a, err := e.send(path, token, body)
		if err != nil {
			t.Error(err)
		}
		got <- a
	}()
	return got
}

func (e *env) send(path, token, body string) (answer, error) {
	req, err := http.NewRequest(http.MethodPost, "http://"+e.addr+"/v1/databases/d/"+path, strings.NewReader(body))
	if err != nil {
		return answer{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := h1Client().Do(req)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Header.Get("Skylite-Protocol") != "1" {
		return answer{}, fmt.Errorf("no protocol marker: %v", resp.Header)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return answer{}, err
	}
	frames, err := parseFrames(b)
	return answer{resp.StatusCode, frames}, err
}

// parseFrames splits an NDJSON body; a row frame becomes {"row": [...]}.
func parseFrames(b []byte) ([]map[string]any, error) {
	if !bytes.HasSuffix(b, []byte("\n")) {
		return nil, fmt.Errorf("body %q does not end with a newline", b)
	}
	var frames []map[string]any
	for line := range bytes.Lines(b) {
		var v any
		d := json.NewDecoder(bytes.NewReader(line))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			return nil, fmt.Errorf("frame %q: %w", line, err)
		}
		if row, ok := v.([]any); ok {
			v = map[string]any{"row": row}
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("frame %q is neither an object nor a row", line)
		}
		frames = append(frames, m)
	}
	return frames, nil
}

func q(sql string) string {
	b, _ := json.Marshal(map[string]any{"sql": sql})
	return string(b)
}

func TestQueryAndDescribe(t *testing.T) {
	e := newQueryEnv(t, nil)
	e.exec(t, "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, price REAL, cover BLOB); "+
		"INSERT INTO books VALUES (1, 'Dune', 9.5, x'00ff'), (2, NULL, NULL, x'')")
	head := e.leader(t).Head()

	a := e.post(t, "query", "reader", `{"sql":"SELECT id, title, price, cover FROM books WHERE id >= ? ORDER BY id","params":[1]}`)
	if a.status != 200 || a.done() == nil {
		t.Fatalf("query: %d %v", a.status, a.frames)
	}
	cols, _ := a.frames[0]["columns"].([]any)
	if c, _ := cols[1].(map[string]any); len(cols) != 4 || c["name"] != "title" || c["decltype"] != "TEXT" || c["table"] != "books" || c["origin"] != "title" || c["database"] != "main" {
		t.Errorf("columns %v", a.frames[0])
	}
	if got, want := strings.Join(a.rows(), ";"), `[1,"Dune",9.5e0,{"blob":"AP8="}];[2,null,null,{"blob":""}]`; got != want {
		t.Errorf("rows %s, want %s", got, want)
	}
	d := a.done()
	if d["txn"] != json.Number(fmt.Sprint(head)) || d["read_only"] != true || d["rows_affected"] != json.Number("0") || d["last_insert_rowid"] != json.Number("0") {
		t.Errorf("done %v, want txn %d", d, head)
	}
	// The read ended: the pooled connection holds no transaction.
	e.pooled(t, func(db *sqlite3.Conn) error {
		if !db.GetAutocommit() {
			t.Error("the pooled connection is still in a transaction")
		}
		return nil
	})

	a = e.post(t, "query", "reader", `{"sql":"SELECT title FROM books WHERE id = :id AND $p = $p","named":{"id":1,"p":"x"}}`)
	if got := strings.Join(a.rows(), ";"); a.status != 200 || got != `["Dune"]` {
		t.Errorf("named: %d %v", a.status, a.frames)
	}
	a = e.post(t, "query", "reader", `{"sql":"SELECT typeof(?), length(?)","params":[{"blob":""},{"blob":""}]}`)
	if got := strings.Join(a.rows(), ""); got != `["blob",0]` {
		t.Errorf("an empty BLOB parameter: %d %v", a.status, a.frames)
	}
	// A statement without result columns has no columns frame.
	if a = e.post(t, "query", "reader", q("PRAGMA foreign_keys = ON")); len(a.frames) != 1 || a.done() == nil {
		t.Errorf("no columns: %v", a.frames)
	}
	a = e.post(t, "query", "reader", q("SELECT 1 WHERE 0"))
	if len(a.frames) != 2 || a.frames[0]["columns"] == nil || a.done() == nil {
		t.Errorf("no rows: %v", a.frames)
	}

	a = e.post(t, "describe", "reader", q("SELECT id, title FROM books WHERE title = :t AND id > ?"))
	if a.status != 200 || a.done()["param_count"] != json.Number("2") || a.done()["txn"] != json.Number(fmt.Sprint(head)) {
		t.Fatalf("describe: %d %v", a.status, a.frames)
	}
	if p, _ := a.frames[1]["params"].(map[string]any); fmt.Sprint(p["names"]) != "[:t <nil>]" || a.frames[0]["columns"] == nil {
		t.Errorf("describe frames %v", a.frames)
	}
	// describe compiles a write without running it.
	a = e.post(t, "describe", "reader", q("INSERT INTO books (title) VALUES (?)"))
	if cols, _ := a.frames[0]["columns"].([]any); a.status != 200 || cols == nil || len(cols) != 0 || a.done()["param_count"] != json.Number("1") {
		t.Errorf("describe insert: %d %v", a.status, a.frames)
	}

	for name, c := range map[string]struct {
		body, code string
		status     int
	}{
		"a parameter unbound":     {`{"sql":"SELECT ?, ?","params":[1]}`, "invalid_request", 200},
		"a value too many":        {`{"sql":"SELECT ?","params":[1,2]}`, "invalid_request", 200},
		"an unknown name":         {`{"sql":"SELECT :a","named":{"b":1}}`, "invalid_request", 200},
		"two statements":          {q("SELECT 1; SELECT 2"), "invalid_request", 400},
		"no statement":            {q("-- nothing"), "invalid_request", 400},
		"no such table":           {q("SELECT * FROM nothing"), "sql_error", 400},
		"an error while stepping": {q("SELECT abs(-9223372036854775807 - 1)"), "sql_error", 200},
		"an unknown field":        {`{"sql":"SELECT 1","seq":1}`, "invalid_request", 400},
		"min_txn above the state": {fmt.Sprintf(`{"sql":"SELECT 1","min_txn":%d}`, head+1), "behind", 503},
	} {
		if a := e.post(t, "query", "reader", c.body); a.code() != c.code || a.status != c.status {
			t.Errorf("%s: %d %v, want %d %s", name, a.status, a.last(), c.status, c.code)
		}
	}
	if a := e.post(t, "query", "reader", fmt.Sprintf(`{"sql":"SELECT 1","min_txn":%d}`, head)); a.status != 200 {
		t.Errorf("min_txn at the state: %d %v", a.status, a.last())
	}
}

func TestNeedsChannelExecutesNothing(t *testing.T) {
	e := newQueryEnv(t, nil)
	e.exec(t, "CREATE TABLE t (a); INSERT INTO t VALUES (1)")
	l := e.leader(t)
	head, creates := l.Head(), e.store.creates.Load()
	for _, sql := range []string{
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3) RETURNING a",
		"UPDATE t SET a = 9",
		"DELETE FROM t",
		"CREATE TABLE u (a)",
		"DROP TABLE t",
		"CREATE TEMP TABLE v (a)",
	} {
		if a := e.post(t, "query", "writer", q(sql)); a.status != http.StatusConflict || a.code() != "needs_channel" || len(a.frames) != 1 {
			t.Errorf("%s: %d %v, want 409 needs_channel alone", sql, a.status, a.frames)
		}
	}
	if l.Head() != head || e.store.creates.Load() != creates {
		t.Errorf("head %d and %d creates, want %d and %d: a commit reached the log", l.Head(), e.store.creates.Load(), head, creates)
	}
	if a := e.post(t, "query", "reader", q("SELECT group_concat(a) FROM t")); strings.Join(a.rows(), "") != `["1"]` {
		t.Errorf("the table holds %v", a.rows())
	}
}

func TestPolicyBeforePrepareIncludingDescribe(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.pool = newPool(1) })
	for _, path := range []string{"query", "describe"} {
		for _, sql := range []string{
			// Pragmas that take effect while the statement is compiled.
			"PRAGMA query_only = 1",
			"PRAGMA recursive_triggers = 1",
			"PRAGMA journal_mode = DELETE",
			"ATTACH 'x.db' AS x",
			"SELECT sqlite_compileoption_get(0)",
		} {
			if a := e.post(t, path, "reader", q(sql)); a.status != http.StatusForbidden || a.code() != "statement_forbidden" {
				t.Errorf("%s %s: %d %v, want 403 statement_forbidden", path, sql, a.status, a.last())
			}
		}
	}
	e.pooled(t, func(db *sqlite3.Conn) error {
		for _, p := range []string{"query_only", "recursive_triggers"} {
			if n := pragma(t, db, p); n != "0" {
				t.Errorf("%s is %s after the refusals", p, n)
			}
		}
		return nil
	})
}

// pragma reads one pragma's value.
func pragma(t *testing.T, db *sqlite3.Conn, name string) string {
	t.Helper()
	s, _, err := db.Prepare("PRAGMA " + name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if !s.Step() {
		t.Fatalf("PRAGMA %s: %v", name, s.Err())
	}
	return s.ColumnText(0)
}

func TestTxnControlRefusedStateless(t *testing.T) {
	e := newQueryEnv(t, nil)
	for _, path := range []string{"query", "describe"} {
		for _, sql := range []string{
			"SAVEPOINT a", " /* c */ savepoint a", "RELEASE a", "RELEASE SAVEPOINT a", "ROLLBACK TO a", "ROLLBACK",
			"BEGIN", "BEGIN IMMEDIATE", "COMMIT", "END", "EXPLAIN SAVEPOINT a", "EXPLAIN QUERY PLAN SAVEPOINT a",
			// SQLite reads a UTF-8 byte order mark as white space.
			"\ufeffSAVEPOINT a", ";\ufeff\ufeff;SAVEPOINT a", "EXPLAIN \ufeffQUERY \ufeffPLAN \ufeffSAVEPOINT a",
			" \vSAVEPOINT a", "EXPLAIN \vSAVEPOINT a", "--\n\vBEGIN",
		} {
			if a := e.post(t, path, "reader", q(sql)); a.code() != "invalid_request" || len(a.frames) != 1 {
				t.Errorf("%s %q: %d %v, want invalid_request alone", path, sql, a.status, a.frames)
			}
		}
	}
	if a := e.post(t, "query", "reader", q("SELECT 'savepoint'")); a.status != 200 {
		t.Errorf("a string that names a keyword: %d %v", a.status, a.last())
	}
}

func FuzzKeywordAgreesWithPolicy(f *testing.F) {
	for _, s := range []string{
		"SAVEPOINT a", "release a", "ROLLBACK TO a", "ROLLBACK", "BEGIN", "END", "COMMIT TRANSACTION", "-- x\nCOMMIT",
		"/* x */ END", "EXPLAIN SAVEPOINT a", "EXPLAIN QUERY PLAN BEGIN", "explain select 1", "SELECT 1", ";SAVEPOINT a", " \vSAVEPOINT a", "--\n\vBEGIN",
		"WITH x AS (SELECT 1) SELECT * FROM x", "endx", "\fBEGIN", "VALUES (1)", "PRAGMA foreign_keys",
		"\ufeffSAVEPOINT a", ";\ufeff;\ufeffBEGIN", "EXPLAIN \ufeffQUERY \ufeffPLAN \ufeffEND", "SELECT\ufeff1",
	} {
		f.Add(s)
	}
	pol, err := sqlpolicy.New(sqlpolicy.Limits{})
	if err != nil {
		f.Fatal(err)
	}
	var pc *sqlpolicy.Conn
	reopen := func(t *testing.T) {
		db, err := sqlite3.Open(":memory:")
		if err == nil {
			pc, err = pol.Apply(context.Background(), db)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	errCompiled := errors.New("compiled")
	f.Fuzz(func(t *testing.T, sql string) {
		if pc == nil {
			reopen(t)
		}
		// Compile outside a transaction, where the policy refuses all
		// transaction control; never step.
		err := pc.Run(context.Background(), sql, func(*sqlite3.Stmt) error { return errCompiled })
		if errors.Is(err, sqlpolicy.ErrBroken) {
			pc = nil
			return
		}
		pe, _ := errors.AsType[*sqlpolicy.Error](err)
		policy := pe != nil && pe.Reason == sqlpolicy.ReasonTransaction
		switch kw := txnControl(sql); {
		case policy && !kw:
			t.Errorf("%q: the policy refuses it as transaction control, the keyword check does not", sql)
		case kw && errors.Is(err, errCompiled):
			t.Errorf("%q: the keyword check refuses it, and it compiles outside a transaction", sql)
		}
	})
}

func TestResetBetweenPrincipals(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.pool = newPool(1) })
	// A query cannot leave state behind on its own: it runs inside the
	// server's read transaction, where PRAGMA foreign_keys is a no-op, and
	// a write is refused. So the state is also planted directly, as another
	// path could leave it.
	if a := e.post(t, "query", "reader", q("PRAGMA foreign_keys = OFF")); a.status != 200 || a.done() == nil {
		t.Fatalf("%d %v", a.status, a.last())
	}
	e.pooled(t, func(db *sqlite3.Conn) error {
		db.SetLastInsertRowID(7)
		return db.Exec("PRAGMA foreign_keys = OFF; CREATE TEMP TABLE left_over (a); CREATE TEMP TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO seq DEFAULT VALUES")
	})
	for sql, want := range map[string]string{
		"PRAGMA foreign_keys": "[1]",
		"SELECT count(*) FROM temp.sqlite_schema WHERE name <> 'sqlite_sequence'": "[0]",
		"SELECT count(*) FROM temp.sqlite_sequence":                               "[0]",
		"SELECT last_insert_rowid()":                                              "[0]",
	} {
		a := e.post(t, "query", "writer", q(sql))
		if got := strings.Join(a.rows(), ""); a.status != 200 || got != want || a.done()["last_insert_rowid"] != json.Number("0") || a.done()["rows_affected"] != json.Number("0") {
			t.Errorf("%s: %d %v, want %s", sql, a.status, a.frames, want)
		}
	}
	if e.s.pool.open != 1 {
		t.Errorf("%d connections open, want the one, reused", e.s.pool.open)
	}
}

func rep(s string, n int) string { return strings.Repeat(s, n) }

func terms(t int, op string) string { return rep(" "+op+" SELECT 1", t-1) }

// TestStackOverflowDiscardsOrFailStops sends ADR 0007's overflow shapes.
// The broken connection must never be pooled again: either the engine
// discarded it and the leader serves on, or the leader stopped.
// overflowViews creates views whose bodies nest subqueries in FROM; SELECT *
// FROM v4 overflows SQLite's stack.
func overflowViews() string {
	views := "CREATE VIEW v0 AS SELECT 1 AS a;"
	for i := 1; i <= 4; i++ {
		s := fmt.Sprintf("SELECT * FROM v%d%s", i-1, terms(4, "INTERSECT"))
		for range 12 {
			s = "SELECT * FROM (" + s + ")" + terms(4, "INTERSECT")
		}
		views += fmt.Sprintf("CREATE VIEW v%d AS %s;", i, s)
	}
	return views
}

func TestStackOverflowDiscardsOrFailStops(t *testing.T) {
	// Each generated column reads the next, and CREATE TABLE compiles a
	// SELECT * of the new table itself.
	gen := []string{"a"}
	for i := 1; i <= 5; i++ {
		next := fmt.Sprint("c", i+1)
		if i == 5 {
			next = "a"
		}
		gen = append(gen, fmt.Sprintf("c%d AS (%s%s%s)", i, rep("0 BETWEEN ", 17), next, rep(" AND 9", 17)))
	}
	views := overflowViews()
	// Flattening copies each level's expression into the next.
	plus := "a" + rep(" + 1", 16)
	flat := "CREATE TABLE fq(a);CREATE VIEW f0 AS SELECT a FROM fq;"
	for i := 1; i <= 5; i++ {
		flat += fmt.Sprintf("CREATE VIEW f%d AS SELECT %s AS a FROM f%d;", i, plus, i-1)
	}
	fsel := "SELECT " + plus + " AS a FROM f5"
	for range 15 {
		fsel = "SELECT " + plus + " AS a FROM (" + fsel + ")"
	}
	for _, c := range []struct{ name, setup, sql, code string }{
		// It overflows only when stepped, which a stateless request never
		// does to a statement that is not a pure read.
		{"chained generated columns", "", "CREATE TABLE g(" + strings.Join(gen, ", ") + ")", "needs_channel"},
		{"views over subqueries in FROM", views, "SELECT * FROM v4", "internal"},
		{"flattened expressions", flat, fsel, "internal"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newQueryEnv(t, nil)
			if c.setup != "" {
				e.exec(t, c.setup)
			}
			if a := e.post(t, "query", "reader", q(c.sql)); a.code() != c.code {
				t.Fatalf("%d %v, want %s", a.status, a.last(), c.code)
			}
			if n, idle := e.s.pool.open, len(e.s.pool.idle); c.code == "internal" && (n != 0 || idle != 0) {
				t.Errorf("%d connections open, %d idle, want the broken one gone", n, idle)
			}
			// The engine, told by engine.ErrBroken, discarded it (ADR 0007, requirement 1).
			if c.code == "internal" && !strings.Contains(e.logs.String(), `"class":"broken"`) {
				t.Errorf("the engine did not report the connection broken: %s", e.logs.String())
			}
			a := e.post(t, "query", "reader", q("SELECT 1"))
			switch l := e.s.dbs["d"].loop.State().Leader; {
			case l != nil && l.Err() != nil:
				t.Logf("the leader stopped: %v", l.Err())
			case a.status != 200 || strings.Join(a.rows(), "") != "[1]":
				t.Errorf("the next query: %d %v", a.status, a.last())
			}
		})
	}
}

func TestEngineSettingsSurviveClientSQL(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.pool = newPool(1) })
	for _, sql := range []string{
		"PRAGMA journal_mode = DELETE", "PRAGMA journal_mode = OFF", "PRAGMA synchronous = OFF", "PRAGMA locking_mode = EXCLUSIVE",
		"PRAGMA wal_autocheckpoint = 0", "PRAGMA cache_size = 1", "PRAGMA mmap_size = 1048576", "PRAGMA temp_store = FILE",
		"PRAGMA writable_schema = ON", "PRAGMA cell_size_check = OFF", "PRAGMA busy_timeout = 0", "PRAGMA main.page_size = 512",
		"PRAGMA wal_checkpoint(TRUNCATE)", "PRAGMA max_page_count = 1",
	} {
		if a := e.post(t, "query", "writer", q(sql)); a.code() != "statement_forbidden" {
			t.Errorf("%s: %d %v, want statement_forbidden", sql, a.status, a.last())
		}
	}
	e.pooled(t, func(db *sqlite3.Conn) error {
		for name, want := range map[string]string{
			"journal_mode": "wal", "synchronous": "2", "locking_mode": "normal", "temp_store": "2", "writable_schema": "0", "query_only": "0",
		} {
			if got := pragma(t, db, name); got != want {
				t.Errorf("%s is %s, want %s", name, got, want)
			}
		}
		return nil
	})
	// Writes are still captured: a commit reaches the log.
	l := e.leader(t)
	head, creates := l.Head(), e.store.creates.Load()
	e.exec(t, "CREATE TABLE after (a)")
	if l.Head() != head+1 || e.store.creates.Load() <= creates {
		t.Errorf("head %d, %d creates after a write, want %d and more than %d", l.Head(), e.store.creates.Load(), head+1, creates)
	}
}

// cpuBound never ends by itself.
const cpuBound = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

// running waits until n pooled connections are in use.
func (e *env) running(t *testing.T, n int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; {
		p := e.s.pool
		p.mu.Lock()
		busy := p.open - len(p.idle)
		p.mu.Unlock()
		if busy == n {
			time.Sleep(100 * time.Millisecond) // into the statement
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections in use, want %d", busy, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCloseInterruptsCPUBoundQuery(t *testing.T) {
	e := newQueryEnv(t, nil)
	l := e.leader(t)
	got := e.postAsync(t, "query", "reader", q(cpuBound))
	e.running(t, 1)
	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5 s: the query was not interrupted")
	}
	a := <-got
	if a.status != 200 || a.code() != "not_serving" {
		t.Errorf("%d %v, want not_serving as the terminal frame", a.status, a.last())
	}
	t.Logf("interrupted after %v", time.Since(start))
}

func TestTimeoutMS(t *testing.T) {
	e := newQueryEnv(t, nil)
	start := time.Now()
	a := e.post(t, "query", "reader", fmt.Sprintf(`{"sql":%q,"timeout_ms":300}`, cpuBound))
	if d := time.Since(start); a.status != 200 || a.code() != "timeout" || d > 5*time.Second {
		t.Errorf("%d %v after %v, want timeout after 300 ms", a.status, a.last(), d)
	}
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != 200 {
		t.Errorf("after a timeout: %d %v", a.status, a.last())
	}
}

// TestTimeoutAfterRows: a statement that reaches its time limit after its
// response started ends with the timeout frame, not a cut stream.
func TestTimeoutAfterRows(t *testing.T) {
	e := newQueryEnv(t, nil)
	// More rows than one frame holds, then no more rows, never ending.
	body := fmt.Sprintf(`{"sql":%q,"timeout_ms":300}`, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT x FROM c WHERE x <= 3000")
	for _, c := range []struct {
		name   string
		client *http.Client
		proto  int
	}{{"HTTP/1.1", h1Client(), 1}, {"HTTP/2", h2cClient(), 2}} {
		t.Run(c.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "http://"+e.addr+"/v1/databases/d/query", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+e.sig.token(t, "reader"))
			resp, err := c.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("HTTP/%d: reading the body: %v", resp.ProtoMajor, err)
			}
			frames, err := parseFrames(b)
			if err != nil {
				t.Fatal(err)
			}
			a := answer{resp.StatusCode, frames}
			if resp.ProtoMajor != c.proto || a.status != 200 || a.code() != "timeout" || len(a.rows()) < 1024 {
				t.Errorf("HTTP/%d %d, %d rows, %v, want rows then timeout", resp.ProtoMajor, a.status, len(a.rows()), a.last())
			}
		})
	}
}

func TestResultTooLarge(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.limits.ResultBytes = 4096 })
	a := e.post(t, "query", "reader", q("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 1000) SELECT x, printf('%100d', x) FROM c"))
	if a.status != 200 || a.code() != "result_too_large" || len(a.rows()) == 0 || len(a.rows()) >= 1000 {
		t.Fatalf("%d, %d rows, %v, want some rows, then result_too_large", a.status, len(a.rows()), a.last())
	}
	// The columns frame is never split: one above the frame size fails alone.
	a = e.post(t, "query", "reader", q("SELECT 1 AS "+rep("c", 8192)))
	if a.code() != "result_too_large" || len(a.frames) != 1 {
		t.Errorf("a columns frame above the result size: %d %v", a.status, a.frames)
	}
}

func TestConnectionLimits(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.pool = newPool(2) }) // one per principal
	e.postAsync(t, "query", "reader", fmt.Sprintf(`{"sql":%q,"timeout_ms":3000}`, cpuBound))
	e.running(t, 1)
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
		t.Errorf("the principal's second: %d %v, want overloaded", a.status, a.last())
	}
	e.postAsync(t, "query", "writer", fmt.Sprintf(`{"sql":%q,"timeout_ms":3000}`, cpuBound))
	e.running(t, 2)
	if a := e.post(t, "query", "admin", q("SELECT 1")); a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
		t.Errorf("a full pool: %d %v, want overloaded", a.status, a.last())
	}
}

// waiting waits until n stateless requests wait for a connection.
func (e *env) waiting(t *testing.T, n int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		e.s.pool.mu.Lock()
		got := len(e.s.pool.queue)
		e.s.pool.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests wait for a connection, want %d", got, n)
		}
	}
}

// settled waits until the pool has no connection in use and no waiter.
func (e *env) settled(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		p := e.s.pool
		p.mu.Lock()
		busy, queue, waiting, by := p.open-len(p.idle), len(p.queue), len(p.waiting), len(p.busy)
		p.mu.Unlock()
		if busy == 0 && queue == 0 && waiting == 0 && by == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections in use, %d waiters, %d principals waiting, %d principals busy; want none", busy, queue, waiting, by)
		}
	}
}

// occupy takes n of subject's connections from the pool until the returned
// function gives them back.
func (e *env) occupy(t *testing.T, subject string, n int) func() {
	t.Helper()
	who := testIssuer + "\x00" + subject // as principal makes it
	for i := range n {
		_, evict, ok := e.s.pool.take(nil, who)
		if !ok {
			t.Fatalf("%s's connection %d is not free", subject, i)
		}
		if evict != nil {
			_ = evict.ec.Close()
		}
	}
	return func() {
		for range n {
			e.s.pool.release(who, nil, false)
		}
	}
}

func TestReadersBeyondQuotaWait(t *testing.T) {
	e := newQueryEnv(t, func(e *env) {
		e.s.pool = newPool(4) // one per principal
		e.s.pool.maxWait = 10 * time.Second
	})
	free := e.occupy(t, "reader", 1)
	var got []<-chan answer
	for range 8 {
		got = append(got, e.postAsync(t, "query", "reader", q("SELECT 1")))
	}
	e.waiting(t, 8)
	free()
	for i, c := range got {
		if a := <-c; a.status != 200 || a.done() == nil {
			t.Errorf("waiting reader %d: %d %v, want rows", i, a.status, a.last())
		}
	}
	e.settled(t)
}

func TestReadWaitExpires(t *testing.T) {
	e := newQueryEnv(t, func(e *env) {
		e.s.pool = newPool(4)
		e.s.pool.maxWait = 100 * time.Millisecond
	})
	free := e.occupy(t, "reader", 1)
	start := time.Now()
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
		t.Errorf("after the wait: %d %v, want overloaded", a.status, a.last())
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Errorf("refused after %v, want the wait first", d)
	}
	// The statement time bounds the wait too.
	e.s.pool.maxWait = 10 * time.Second
	if a := e.post(t, "query", "reader", `{"sql":"SELECT 1","timeout_ms":100}`); a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
		t.Errorf("after timeout_ms: %d %v, want overloaded", a.status, a.last())
	}
	e.waiting(t, 0)
	free()
	e.settled(t)
}

func TestCancelWhileWaitingForConnection(t *testing.T) {
	e := newQueryEnv(t, func(e *env) {
		e.s.pool = newPool(4)
		e.s.pool.maxWait = 10 * time.Second
	})
	free := e.occupy(t, "reader", 1)
	ctx, cancel := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+e.addr+"/v1/databases/d/query", strings.NewReader(q("SELECT 1")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.sig.token(t, "reader"))
	gone := make(chan error, 1)
	go func() {
		resp, err := h1Client().Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		gone <- err
	}()
	e.waiting(t, 1)
	cancel()
	if err := <-gone; !errors.Is(err, context.Canceled) {
		t.Errorf("the cancelled request: %v, want context.Canceled", err)
	}
	e.waiting(t, 0) // while the connection is still held
	free()
	e.settled(t)
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != 200 {
		t.Errorf("after the cancel: %d %v, want rows", a.status, a.last())
	}
}

func TestConnectionWaitFairness(t *testing.T) {
	e := newQueryEnv(t, func(e *env) {
		e.s.pool = newPool(2) // one per principal, two in all
		e.s.pool.maxWait = 10 * time.Second
		e.s.pool.waiters, e.s.pool.waitersBy = 4, 2
	})
	long, short := e.occupy(t, "reader", 1), e.occupy(t, "writer", 1)
	var readers []<-chan answer
	for i := range 2 {
		readers = append(readers, e.postAsync(t, "query", "reader", q("SELECT 1")))
		e.waiting(t, i+1)
	}
	// The principal's waiting bound refuses its third at once; another
	// principal still waits.
	e.refusedAtOnce(t, "reader", 2)
	admin := e.postAsync(t, "query", "admin", q("SELECT 1"))
	e.waiting(t, 3)
	// The connection the writer frees goes to admin, behind the reader's
	// waiters, which the reader's own quota holds back.
	short()
	if a := <-admin; a.status != 200 || a.done() == nil {
		t.Errorf("admin: %d %v, want rows", a.status, a.last())
	}
	e.waiting(t, 2)
	long()
	for i, c := range readers {
		if a := <-c; a.status != 200 || a.done() == nil {
			t.Errorf("reader %d: %d %v, want rows", i, a.status, a.last())
		}
	}
	e.settled(t)
}

// refusedAtOnce posts a query of subject and wants it refused overloaded
// without its joining the n requests that wait for a connection.
func (e *env) refusedAtOnce(t *testing.T, subject string, n int) {
	t.Helper()
	a := e.postAsync(t, "query", subject, q("SELECT 1"))
	for {
		select {
		case a := <-a:
			if a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
				t.Errorf("%s's waiter past the bound: %d %v, want overloaded", subject, a.status, a.last())
			}
			return
		case <-time.After(5 * time.Millisecond):
		}
		e.s.pool.mu.Lock()
		got := len(e.s.pool.queue)
		e.s.pool.mu.Unlock()
		if got > n {
			t.Errorf("%s's waiter past the bound waits, one of %d", subject, got)
			return
		}
	}
}

func TestPodWaitingBound(t *testing.T) {
	e := newQueryEnv(t, func(e *env) {
		e.s.pool = newPool(2) // one per principal, two in all
		e.s.pool.maxWait = 10 * time.Second
		e.s.pool.waiters = 2
	})
	freeReader, freeAdmin := e.occupy(t, "reader", 1), e.occupy(t, "admin", 1)
	var got []<-chan answer
	for i, s := range []string{"reader", "admin"} {
		got = append(got, e.postAsync(t, "query", s, q("SELECT 1")))
		e.waiting(t, i+1)
	}
	// Each principal is below its own bound; the pod is at its own.
	e.refusedAtOnce(t, "writer", 2)
	freeReader()
	freeAdmin()
	for i, c := range got {
		if a := <-c; a.status != 200 || a.done() == nil {
			t.Errorf("waiter %d: %d %v, want rows", i, a.status, a.last())
		}
	}
	e.settled(t)
}

// TestWriteWaitsForTurnover: a write waits for a connection only if a
// stateless request got one of those it lacks within the wait: one of its
// principal's at the principal's limit, and any when the pod is full.
func TestWriteWaitsForTurnover(t *testing.T) {
	p := newPool(4) // one per principal
	for _, who := range []string{"a", "b", "c", "d"} {
		if _, _, ok := p.take(nil, who); !ok {
			t.Fatalf("no connection for %s", who)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.turningLocked("a") || !p.turningLocked("w") {
		t.Error("no wait behind connections just taken by stateless requests")
	}
	delete(p.reads, "a") // a's connection is a transaction's
	if p.turningLocked("a") {
		t.Error("a waits at its limit for the other principals' turnover")
	}
	if !p.turningLocked("w") {
		t.Error("w, below its limit, does not wait for the pod's turnover")
	}
	p.read = time.Now().Add(-p.maxWait)
	if p.turningLocked("w") {
		t.Error("w waits for a pod whose stateless requests are older than the wait")
	}
}

// TestGrantAsWaitEnds: a waiter granted a connection as its wait ends takes
// it, and the pool counts it back once released.
func TestGrantAsWaitEnds(t *testing.T) {
	p := newPool(4)
	p.maxWait = time.Hour
	if _, _, ok := p.take(nil, "a"); !ok {
		t.Fatal("no connection for a")
	}
	ctx, cancel := context.WithCancel(t.Context())
	got := make(chan bool, 1)
	go func() {
		_, _, ok := p.await(ctx, nil, "a", false, false)
		got <- ok
	}()
	for queued := 0; queued == 0; time.Sleep(time.Millisecond) {
		p.mu.Lock()
		queued = len(p.queue)
		p.mu.Unlock()
	}
	p.mu.Lock()
	cancel()
	time.Sleep(50 * time.Millisecond) // the waiter wakes for the cancel and waits for p.mu
	delete(p.busy, "a")               // release, with p.mu already held
	p.open--
	p.grantLocked()
	p.mu.Unlock()
	if !<-got {
		t.Fatal("the waiter dropped the connection granted as its wait ended")
	}
	p.release("a", nil, false)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.open != 0 || len(p.busy) != 0 || len(p.queue) != 0 || len(p.waiting) != 0 {
		t.Errorf("open %d, busy %v, %d queued, waiting %v; want none", p.open, p.busy, len(p.queue), p.waiting)
	}
}

func TestBodyShareOverloaded(t *testing.T) {
	// A principal's quarter of the share is one byte short of this body.
	e := newQueryEnv(t, func(e *env) { e.s.bodyShare = 4*8*int64(len(q("SELECT 1"))) - 1 })
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
		t.Errorf("%d %v, want overloaded", a.status, a.last())
	}
	if a := e.post(t, "query", "reader", q("SELECT")); a.code() == "overloaded" {
		t.Errorf("a body within the share: %d %v", a.status, a.last())
	}
	// Five principals at their quarter each: the fifth finds the share full.
	e.s.bodyShare = 4 * 80
	r := &http.Request{ContentLength: 10}
	var release []func()
	for i := range 4 {
		f, we := e.s.chargeBody(r, fmt.Sprint(i))
		if we != nil {
			t.Fatalf("principal %d: %v", i, we)
		}
		release = append(release, f)
	}
	if _, we := e.s.chargeBody(r, "4"); we == nil || we.Code != "overloaded" {
		t.Errorf("a full share: %v, want overloaded", we)
	}
	release[0]()
	if _, we := e.s.chargeBody(r, "4"); we != nil {
		t.Errorf("after a release: %v", we)
	}
}

func TestTailBoundAnswersMaintenance(t *testing.T) {
	// Creation and the takeover's epoch bump put two commits above the
	// manifest, the bound. Compaction clears it in the background, so a
	// request may also find it cleared already.
	e := newQueryEnv(t, func(e *env) { e.s.engine.TailBound = 2 })
	served := false
	for deadline := time.Now().Add(10 * time.Second); !served; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("compaction did not clear the tail bound")
		}
		for _, path := range []string{"query", "describe"} {
			a := e.post(t, path, "reader", q("SELECT 1"))
			em, _ := a.last()["error"].(map[string]any)
			msg, _ := em["message"].(string)
			switch {
			case a.status == 200 && a.code() == "":
				served = true
			case a.status != http.StatusServiceUnavailable || a.code() != "maintenance" || !strings.Contains(msg, "tail bound of 2") || !strings.Contains(msg, "retry"):
				t.Fatalf("%s: %d %v, want maintenance naming the bound", path, a.status, a.last())
			}
		}
		if !e.s.ready() {
			t.Fatal("not ready at a tail bound compaction clears")
		}
	}
}

func TestTailBoundWithoutCompactionNotReady(t *testing.T) {
	// A leader that cannot compact refuses without ErrRetry.
	e := newQueryEnv(t, nil)
	d := e.s.dbs["d"]
	we := e.s.engineError(d, e.leader(t), fmt.Errorf("%w: %w", engine.ErrNotCommitted, engine.ErrTailBound))
	if we.Code != "maintenance" || !strings.Contains(we.Message, "cannot compact") {
		t.Errorf("%v, want maintenance saying the leader cannot compact", we)
	}
	if e.s.ready() {
		t.Error("ready at a tail bound the leader cannot compact past")
	}
}

func TestRetryableTailBoundStaysReady(t *testing.T) {
	// A tail-bound refusal that compaction is clearing matches ErrRetry.
	e := newQueryEnv(t, nil)
	d := e.s.dbs["d"]
	we := e.s.engineError(d, e.leader(t), fmt.Errorf("%w: %w: %w", engine.ErrNotCommitted, engine.ErrTailBound, engine.ErrRetry))
	if we.Code != "maintenance" || !strings.Contains(we.Message, "retry") {
		t.Errorf("%v, want maintenance saying retry", we)
	}
	if !e.s.ready() {
		t.Error("not ready at a tail bound compaction is clearing")
	}
}

func TestStandbyNotServingAndNotReady(t *testing.T) {
	leader := newQueryEnv(t, nil)
	standby := newEnv(t, nil)
	standby.s.cfg.Storage = leader.s.cfg.Storage
	standby.s.store = leader.s.store
	standby.s.cfg.Databases = []string{"d"}
	if err := standby.s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	standby.start(t)
	probe := func(s *Server) int {
		rec := httptest.NewRecorder()
		s.readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec.Code
	}
	if a := standby.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
		t.Errorf("standby: %d %v, want not_serving", a.status, a.last())
	}
	if c := probe(standby.s); c != http.StatusServiceUnavailable {
		t.Errorf("standby /readyz %d, want 503", c)
	}
	if c := probe(leader.s); c != http.StatusOK {
		t.Errorf("leader /readyz %d, want 200", c)
	}
	if a := leader.post(t, "query", "reader", q("SELECT 1")); a.status != 200 {
		t.Errorf("leader: %d %v", a.status, a.last())
	}
	leader.s.stopping.Store(true)
	if c := probe(leader.s); c != http.StatusServiceUnavailable {
		t.Errorf("a stopping leader's /readyz %d, want 503", c)
	}
}

func TestMaintenanceEndsRunningQuery(t *testing.T) {
	// Writes past the local WAL bound start maintenance, which interrupts
	// the running query.
	e := newQueryEnv(t, func(e *env) { e.s.engine.WALBound = 256 << 10 })
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != 200 {
		t.Fatalf("before maintenance: %d %v", a.status, a.last())
	}
	got := e.postAsync(t, "query", "reader", q(cpuBound))
	e.running(t, 1)
	e.exec(t, "CREATE TABLE t (a)")
	c, err := e.leader(t).Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := 0; ; i++ {
		_, err := c.Run(context.Background(), fmt.Appendf(nil, "test-%d", commitIDs.Add(1)), func(db *sqlite3.Conn) error {
			return db.Exec("INSERT INTO t VALUES (randomblob(32768))")
		})
		if errors.Is(err, engine.ErrRetry) {
			break
		}
		if err != nil || i == 100 {
			t.Fatalf("write %d before the WAL bound: %v", i, err)
		}
	}
	select {
	case a := <-got:
		if a.status != 200 || a.code() != "maintenance" {
			t.Errorf("%d %v, want maintenance as the terminal frame", a.status, a.last())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("maintenance did not end the running query")
	}
	// Maintenance passes: the server stays ready and serves again.
	if !e.s.ready() {
		t.Error("not ready during maintenance")
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if a := e.post(t, "query", "reader", q("SELECT 1")); a.status == 200 && a.code() == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the database did not serve again after maintenance")
		}
	}
}

func TestNotReadyWhileShuttingDown(t *testing.T) {
	e := newQueryEnv(t, nil)
	got := e.postAsync(t, "query", "reader", fmt.Sprintf(`{"sql":%q,"timeout_ms":2000}`, cpuBound))
	e.running(t, 1)
	if !e.s.ready() {
		t.Fatal("not ready while leading")
	}
	go func() { _ = e.halt() }()
	// Shutdown waits for the running query, and the server still leads.
	for deadline := time.Now().Add(time.Second); e.s.ready(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("still ready a second into shutdown")
		}
	}
	if e.s.dbs["d"].leader() == nil {
		t.Error("the server left office before the running query ended")
	}
	<-got
}

// TestDeadlineBeforeTheStatement ends the statement time while the server
// opens, sets up or resets a connection: the answer is timeout, not
// internal.
func TestDeadlineBeforeTheStatement(t *testing.T) {
	e := newQueryEnv(t, nil)
	l, d := e.leader(t), e.s.dbs["d"]
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	// The pool is empty: Open and the policy's set-up meet the deadline.
	if _, we, err := e.s.conn(expired, d, l, "x", false); we == nil || we.Code != "timeout" {
		t.Errorf("open: %v (%v), want timeout", we, err)
	}
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != 200 || len(e.s.pool.idle) != 1 {
		t.Fatalf("%d %v, %d idle", a.status, a.last(), len(e.s.pool.idle))
	}
	// An idle connection: its Reset meets the deadline.
	if _, we, err := e.s.conn(expired, d, l, "x", false); we == nil || we.Code != "timeout" {
		t.Errorf("reset: %v (%v), want timeout", we, err)
	}
	if n := e.s.pool.open; n != 0 {
		t.Errorf("%d connections open after the failed reset, want 0", n)
	}
	for range 60 {
		if a := e.post(t, "query", "reader", `{"sql":"SELECT 1","timeout_ms":1}`); a.code() != "timeout" && a.status != 200 {
			t.Fatalf("%d %v, want timeout or the row", a.status, a.last())
		}
	}
}

// TestMaintenanceKeepsIdleConnection: a refusal by the engine before the
// Reset runs leaves the idle connection pooled.
func TestMaintenanceKeepsIdleConnection(t *testing.T) {
	// A refusal before the Reset runs keeps the pooled connection. Writes
	// past the local WAL bound start maintenance, which refuses every Run
	// while it waits for a held engine connection.
	e := newQueryEnv(t, func(e *env) { e.s.engine.WALBound = 256 << 10 })
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != 200 {
		t.Fatalf("before maintenance: %d %v", a.status, a.last())
	}
	e.exec(t, "CREATE TABLE t (a)")
	l := e.leader(t)
	h, err := l.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()
	entered, release, held := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(held)
		_, _ = h.Run(context.Background(), nil, func(*sqlite3.Conn) error {
			close(entered)
			<-release
			return nil
		})
	}()
	defer func() { close(release); <-held }()
	<-entered
	w, err := l.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	for i := 0; ; i++ {
		_, err := w.Run(context.Background(), fmt.Appendf(nil, "test-%d", commitIDs.Add(1)), func(db *sqlite3.Conn) error {
			return db.Exec("INSERT INTO t VALUES (randomblob(32768))")
		})
		if errors.Is(err, engine.ErrRetry) {
			break
		}
		if err != nil || i == 100 {
			t.Fatalf("write %d before the WAL bound: %v", i, err)
		}
	}
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.code() != "maintenance" {
		t.Fatalf("%d %v, want maintenance", a.status, a.last())
	}
	if open, idle := e.s.pool.open, len(e.s.pool.idle); open != 1 || idle != 1 {
		t.Errorf("%d open, %d idle, want the one connection kept", open, idle)
	}
}

// bodyCharge returns what the body share holds, in all and per principal.
func (e *env) bodyCharge() (int64, map[string]int64) {
	e.s.bodyMu.Lock()
	defer e.s.bodyMu.Unlock()
	return e.s.bodyUsed, maps.Clone(e.s.bodyBy)
}

// TestBodySharePerPrincipal: one principal declaring bodies it never sends
// holds at most a quarter of the body share, and every charge is released.
func TestBodySharePerPrincipal(t *testing.T) {
	body := q("SELECT 1")
	n := 8 * int64(len(body))
	e := newQueryEnv(t, func(e *env) { e.s.bodyShare = 4 * n }) // a principal's quarter holds one body
	token := e.sig.token(t, "reader")
	refused := make(chan int, 4)
	var held []net.Conn
	for range 4 {
		c, err := net.Dial("tcp", e.addr)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
		t.Cleanup(func() { _ = c.Close() })
		if _, err := fmt.Fprintf(c, "POST /v1/databases/d/query HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", token, len(body)); err != nil {
			t.Fatal(err)
		}
		go func() {
			resp, err := http.ReadResponse(bufio.NewReader(c), nil)
			if err != nil {
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusTooManyRequests {
				refused <- 1
			}
		}()
	}
	got := 0
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		for len(refused) > 0 {
			got += <-refused
		}
		used, _ := e.bodyCharge()
		if used/n+int64(got) == 4 {
			if used != n {
				t.Errorf("the principal holds %d of the share, want one body, %d", used, n)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d charged, %d refused, want 4 in all", used, got)
		}
	}
	if a := e.post(t, "query", "writer", body); a.status != 200 {
		t.Errorf("another principal: %d %v, want served", a.status, a.last())
	}
	for range 5 {
		if a := e.post(t, "query", "writer", body); a.status != 200 {
			t.Fatalf("%d %v", a.status, a.last())
		}
	}
	if used, by := e.bodyCharge(); used != n || len(by) != 1 {
		t.Errorf("after the requests: %d charged, %v, want the reader's %d alone", used, by, n)
	}
	// The client that never sent its body goes away: its charge is released.
	for _, c := range held {
		_ = c.Close()
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		used, by := e.bodyCharge()
		if used == 0 && len(by) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d still charged, %v", used, by)
		}
	}
}

// TestMemoryBudgetHolds: whatever the budget and the number of databases,
// the body share, the control capacity, the header reservations, the
// channels' share and the SQLite connections at their ceiling, the leaders'
// included, stay within the memory budget; clients keep a connection, and a principal's quarter of the
// body share holds one body of the largest size.
func TestMemoryBudgetHolds(t *testing.T) {
	base := testConfig(t, jwksFile(t, newSigner(t, "k1")))
	for _, budget := range []int64{0, minBudget, minBudget + 1, 768 << 20, 1026 << 20, 3<<30 + 12345, 33 << 30, maxBudget} {
		_, conns, _ := shares(cmp.Or(budget, defaultBudget), 0)
		for n := range conns + 1 {
			cfg := base
			cfg.MemoryBudget = budget
			for i := range n {
				cfg.Databases = append(cfg.Databases, fmt.Sprint("d", i))
			}
			s, err := newServer(cfg, nil, nil)
			if n == conns {
				if err == nil || cfg.Validate() == nil && budget != 0 {
					t.Errorf("budget %d: %d databases accepted", budget, n)
				} else if least := fmt.Sprintf("memory_budget to at least %d MiB", budgetFor(n+1)>>20); !strings.Contains(err.Error(), least) {
					t.Errorf("budget %d, %d databases: %v, want it to name %q", budget, n, err, least)
				}
				continue
			}
			if err != nil {
				t.Fatalf("budget %d, %d databases: %v", budget, n, err)
			}
			// The channels' share: a receipt per channel, held rows and replay data.
			channels := int64(s.chl.channels)*receiptBytes + s.chl.held + s.chl.replay
			total := s.bodyShare + controlBytes + int64(n)*headerBytes + channels + int64(s.pool.capacity+n)*connCeiling
			if total > s.budget || s.pool.capacity < 1 || s.bodyShare/4 < 8*s.limits.RequestBodyBytes || s.limits.RequestBodyBytes != min(16<<20, s.bodyShare/32) {
				t.Errorf("budget %d, %d databases: %d bytes charged at most, %d client connections, body share %d", s.budget, n, total, s.pool.capacity, s.bodyShare)
			}
		}
	}
	if _, conns, limit := shares(defaultBudget, 1); conns != 14 || limit != 16<<20 {
		t.Errorf("%d connections and %d-byte bodies at 2 GiB, want ADR 0005's 14 and 16 MiB", conns, limit)
	}
	// A 1 GiB container: a 768 MiB budget, (383 MiB less 32 MiB for each
	// database) / 32 bodies, 4 connections.
	if _, conns, limit := shares(768<<20, 0); conns != 4 || limit != (383<<20)/32 {
		t.Errorf("%d connections and %d-byte bodies at 768 MiB, want 4 and %d", conns, limit, (383<<20)/32)
	}
	if _, conns, limit := shares(768<<20, 1); conns != 4 || limit != (351<<20)/32 {
		t.Errorf("%d connections and %d-byte bodies at 768 MiB and one database, want 4 and %d", conns, limit, (351<<20)/32)
	}
	if _, conns, _ := shares(minBudget-2, 0); conns != 0 {
		t.Errorf("%d connections below the least budget, want none", conns)
	}
}

// TestSlowReaderBounded: a client that stops reading a query's rows holds
// its SQLite connection only until a second after the statement time ends.
func TestSlowReaderBounded(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.shutdownTimeout = time.Second })
	body := fmt.Sprintf(`{"sql":%q,"timeout_ms":1000}`, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 200000) SELECT printf('%200d', x) FROM c")
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	start := time.Now()
	if _, err := fmt.Fprintf(c, "POST /v1/databases/d/query HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", e.sig.token(t, "reader"), len(body), body); err != nil {
		t.Fatal(err)
	}
	e.running(t, 1)
	e.running(t, 0) // the client never reads
	// The statement time of 1 s, the terminal margin of 1 s, and slack.
	if d := time.Since(start); d > 3500*time.Millisecond {
		t.Errorf("the connection came back after %v, want about 2 s", d)
	}
}

// TestTransientMaintenance: maintenance for the local WAL bound ends a
// running query with maintenance, and the leader serves again after it.
func TestTransientMaintenance(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.engine.WALBound = 64 << 10 })
	e.exec(t, "CREATE TABLE t (a)")
	got := e.postAsync(t, "query", "reader", q(cpuBound))
	e.running(t, 1)
	c, err := e.leader(t).Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := 0; ; i++ {
		_, err := c.Run(context.Background(), fmt.Appendf(nil, "test-%d", commitIDs.Add(1)), func(db *sqlite3.Conn) error {
			return db.Exec("INSERT INTO t VALUES (randomblob(4000))")
		})
		if errors.Is(err, engine.ErrRetry) {
			break
		}
		if err != nil || i == 100 {
			t.Fatalf("write %d: %v, want the WAL bound", i, err)
		}
	}
	select {
	case a := <-got:
		if a.status != 200 || a.code() != "maintenance" {
			t.Errorf("%d %v, want maintenance as the terminal frame", a.status, a.last())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("maintenance did not end the running query")
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		a := e.post(t, "query", "reader", q("SELECT 1"))
		if a.status == 200 {
			break
		}
		if a.code() != "maintenance" || time.Now().After(deadline) {
			t.Fatalf("after maintenance: %d %v", a.status, a.last())
		}
	}
}

// TestShutdownLeavesOffice: shutdown releases the lease and removes the
// leaders' directory.
func TestShutdownLeavesOffice(t *testing.T) {
	e := newQueryEnv(t, nil)
	if _, err := os.Stat(e.s.dir); err != nil {
		t.Fatalf("the leaders' directory: %v", err)
	}
	if err := e.halt(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(e.s.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the leaders' directory after shutdown: %v", err)
	}
	keys, err := e.s.cfg.Storage.keys("d")
	if err != nil {
		t.Fatal(err)
	}
	st, err := localfs.Open(e.s.cfg.Storage.LocalFS.Path) // Serve closed its own
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r, _, err := st.Get(context.Background(), keys.Lease())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if l, err := format.DecodeLease(b); err != nil || l.Renewed != 0 {
		t.Errorf("lease %+v, %v, want released", l, err)
	}
}

// TestTxnIsTheSnapshotRead: with commits landing concurrently, txn is the
// transaction number of the state the rows were read from.
func TestTxnIsTheSnapshotRead(t *testing.T) {
	e := newQueryEnv(t, nil)
	e.exec(t, "CREATE TABLE t (a)")
	l := e.leader(t)
	base := int64(l.Head()) //nolint:gosec // a test's few commits
	c, err := l.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	stop, wrote := make(chan struct{}), make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				wrote <- nil
				return
			default:
			}
			if _, err := c.Run(context.Background(), fmt.Appendf(nil, "test-%d", commitIDs.Add(1)), func(db *sqlite3.Conn) error {
				return db.Exec("INSERT INTO t VALUES (1)") // one row per commit
			}); err != nil && !errors.Is(err, engine.ErrRetry) { // maintenance refuses commits for a while
				wrote <- err
				return
			}
		}
	}()
	// Each query reads the count, then runs long enough for commits to land
	// before its done frame.
	sql := q("SELECT count(*), (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 100000) SELECT count(*) FROM c) FROM t")
	for read, tries := 0, 0; read < 20; tries++ {
		if tries == 200 {
			t.Fatalf("%d reads in %d tries", read, tries)
		}
		a := e.post(t, "query", "reader", sql)
		if a.code() == "maintenance" { // the writer's commits can start it; retry
			continue
		}
		read++
		var rows int64
		if len(a.rows()) != 1 || a.done() == nil {
			t.Fatalf("%d %v", a.status, a.frames)
		}
		_, _ = fmt.Sscanf(a.rows()[0], "[%d,", &rows)
		if txn, _ := a.done()["txn"].(json.Number).Int64(); txn != base+rows {
			t.Fatalf("txn %d for %d rows, want %d", txn, rows, base+rows)
		}
	}
	close(stop)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}

// TestMemoryExhaustionDiscardsConnection: a pure read that exhausts its
// connection's memory gets internal, and the engine discards the connection.
func TestMemoryExhaustionDiscardsConnection(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.engine.MaxMemory = 16 << 20 })
	a := e.post(t, "query", "reader", q("SELECT count(*) FROM (SELECT randomblob(1000000) AS b FROM generate_series(1, 64) ORDER BY b)"))
	if a.code() != "internal" {
		t.Fatalf("%d %v, want internal", a.status, a.last())
	}
	if !strings.Contains(e.logs.String(), `"class":"broken"`) {
		t.Errorf("the engine did not discard the connection: %s", e.logs.String())
	}
	if n := e.s.pool.open; n != 0 {
		t.Errorf("%d connections open", n)
	}
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != 200 {
		t.Errorf("the next query: %d %v", a.status, a.last())
	}
}

// TestStoppedLeaderNotServing: a leader that stopped serves nothing and
// leaves the server not ready, before the office loop notices.
func TestStoppedLeaderNotServing(t *testing.T) {
	e := newQueryEnv(t, nil)
	if err := e.leader(t).Close(); err != nil {
		t.Fatal(err)
	}
	if e.s.ready() {
		t.Error("ready with the leader stopped")
	}
	a := e.post(t, "query", "reader", q("SELECT 1"))
	if msg := fmt.Sprint(a.last()); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" || !strings.Contains(msg, "does not lead") {
		t.Errorf("%d %v, want not_serving before the body", a.status, a.last())
	}
}
