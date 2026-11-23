package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// stmts is a batch's statements, each without parameters.
func stmts(sqls ...string) []map[string]any {
	s := make([]map[string]any, len(sqls))
	for i, q := range sqls {
		s[i] = map[string]any{"sql": q}
	}
	return s
}

// results returns the entries of an answer's results frames, in order, and
// whether each frame continued where the last ended.
func results(a answer) ([]map[string]any, bool) {
	var out []map[string]any
	for _, f := range a.frames {
		r, ok := f["results"].(map[string]any)
		if !ok {
			continue
		}
		if num(r, "from") != int64(len(out)) {
			return out, false
		}
		es, _ := r["entries"].([]any)
		for _, e := range es {
			m, _ := e.(map[string]any)
			out = append(out, m)
		}
	}
	return out, true
}

// TestBatchAutocommit: an autocommit batch is one commit carrying the
// batch's sequence number, all of it or nothing; a failed entry is named by
// batch_index; the limits and validation are recorded.
func TestBatchAutocommit(t *testing.T) {
	e, _, tok := newTxnEnv(t, func(e *env) { e.s.limits.BatchEntries = 3 })
	db := e.dbID(t)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	body := sq(tok, 2, "batch", "sql", "INSERT INTO t VALUES (?)", "param_sets", [][]any{{1}, {2}, {3}})
	a := post(body)
	d := a.done()
	rs, ok := results(a)
	if a.status != 200 || num(d, "entries") != 3 || num(d, "rows_affected") != 3 || num(d, "last_insert_rowid") != 3 || d["txn_state"] != "none" || num(d, "txn") != e.head(t) || len(rs) != 3 || !ok || num(rs[1], "last_insert_rowid") != 2 {
		t.Fatalf("batch: %d %v", a.status, a.frames)
	}
	if r := post(body); frameText(r) != frameText(a) {
		t.Errorf("replay %v, want %v", r.frames, a.frames)
	}
	if n := withID(e.commits(t, db), wantID(tok, 2)); n != 1 {
		t.Errorf("%d commits carry the batch, want 1", n)
	}
	for i, tc := range []struct {
		name, body, code string
		index            int64 // -1 for none
	}{
		{"a unique violation", sq(tok, 3, "batch", "statements", stmts("INSERT INTO t VALUES (4)", "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (5)")), "sql_error", 1},
		{"an entry with columns", sq(tok, 4, "batch", "statements", stmts("INSERT INTO t VALUES (6)", "SELECT 1")), "invalid_request", 1},
		{"transaction control", sq(tok, 5, "batch", "statements", stmts("INSERT INTO t VALUES (7)", "COMMIT")), "invalid_request", 1},
		{"a binding mismatch", sq(tok, 6, "batch", "sql", "INSERT INTO t VALUES (?)", "param_sets", [][]any{{8}, {9, 10}}), "invalid_request", 1},
		{"a statement that does not compile", sq(tok, 7, "batch", "sql", "INSERT INTO nowhere VALUES (?)", "param_sets", [][]any{{8}}), "sql_error", 0},
		{"over the batch limit", sq(tok, 8, "batch", "sql", "INSERT INTO t VALUES (?)", "param_sets", [][]any{{8}, {9}, {10}, {11}}), "invalid_request", -1},
		{"both forms", sq(tok, 9, "batch", "sql", "INSERT INTO t VALUES (8)", "statements", stmts("INSERT INTO t VALUES (9)")), "invalid_request", -1},
		{"mixed parameter sets", `{"channel":"` + tok + `","seq":10,"op":"batch","sql":"INSERT INTO t VALUES (?)","param_sets":[[8],{"a":9}]}`, "invalid_request", -1},
	} {
		a := post(tc.body)
		_, has := a.err()["batch_index"]
		if a.status != 200 || a.code() != tc.code || a.err()["final"] != true || a.err()["not_executed"] != true || has != (tc.index >= 0) || has && num(a.err(), "batch_index") != tc.index || num(a.err(), "seq") != int64(i+3) {
			t.Errorf("%s: %d %v, want %s at %d", tc.name, a.status, a.frames, tc.code, tc.index)
		}
		if rs, _ := results(a); len(rs) != 0 {
			t.Errorf("%s: results %v of a batch that failed", tc.name, rs)
		}
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1,2,3"]` {
		t.Errorf("rows %s, want the first batch alone", got)
	}
	// The watermark is checked before acceptance.
	if a := post(sq(tok, 11, "batch", "statements", stmts("INSERT INTO t VALUES (8)"), "min_txn", e.head(t)+1)); a.status != http.StatusServiceUnavailable || a.code() != "behind" {
		t.Errorf("behind: %d %v", a.status, a.frames)
	}
	if s, _ := e.chState(tok); s.last != 10 {
		t.Errorf("last %d after a refusal, want 10", s.last)
	}
}

// TestBatchInTxn: inside a transaction a batch stops at its first failing
// entry, whose error is not_executed false, and the entries before it keep
// their effect and their results.
func TestBatchInTxn(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	snap := num(post(sq(tok, 2, "begin")).done(), "txn")
	a := post(sq(tok, 3, "batch", "statements", stmts("INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)")))
	if rs, ok := results(a); a.status != 200 || num(a.done(), "entries") != 2 || num(a.done(), "txn") != snap || a.done()["txn_state"] != "open" || len(rs) != 2 || !ok {
		t.Fatalf("batch: %d %v", a.status, a.frames)
	}
	a = post(sq(tok, 4, "batch", "sql", "INSERT INTO t VALUES (?)", "param_sets", [][]any{{3}, {4}, {1}, {5}}))
	rs, _ := results(a)
	if a.status != 200 || a.code() != "sql_error" || num(a.err(), "batch_index") != 2 || a.err()["not_executed"] != false || a.err()["final"] != true || a.err()["txn_state"] != "open" || len(rs) != 2 {
		t.Errorf("failed batch: %d %v", a.status, a.frames)
	}
	if a := post(sq(tok, 5, "batch", "statements", stmts("SELECT 1"))); a.code() != "invalid_request" || a.err()["not_executed"] != false || a.err()["txn_state"] != "open" {
		t.Errorf("an entry with columns in a transaction: %v", a.frames)
	}
	if a := post(sq(tok, 6, "commit")); a.done()["wrote"] != true {
		t.Fatalf("commit: %v", a.frames)
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1,2,3,4"]` {
		t.Errorf("rows %s", got)
	}
}

// TestBatchCancel: a cancel interrupts a running autocommit batch before its
// commit point; nothing of it commits and the write lock is free.
func TestBatchCancel(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	run := e.postAsync(t, "channel", "writer", sq(tok, 2, "batch", "statements", stmts("INSERT INTO t VALUES (1)", "INSERT INTO t "+cpuBound), "timeout_ms", 10000))
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.running })
	time.Sleep(100 * time.Millisecond)
	r := e.post(t, "channel/cancel", "writer", cancelBody(tok, 2))
	if _, oe := outcome(r); r.status != 200 || oe["code"] != "cancelled" || oe["not_executed"] != true {
		t.Errorf("cancel: %d %v", r.status, r.frames)
	}
	if a := <-run; a.code() != "cancelled" {
		t.Errorf("the batch: %v", a.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("rows %s after a cancelled batch", got)
	}
	e.writes(t)
}

// TestChannelDescribe: describe on an idle channel and inside a
// transaction, where it sees the transaction's schema; its failures, which
// ran nothing; its replay; and the aborted state.
func TestChannelDescribe(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	body := sq(tok, 2, "describe", "sql", "SELECT a FROM t WHERE a = :x OR a = ?")
	a := post(body)
	d := a.done()
	if a.status != 200 || len(a.frames) != 3 || a.frames[0]["columns"] == nil || frameText(answer{frames: a.frames[1:2]}) != `[{"params":{"from":0,"names":[":x",null]}}]` || num(d, "param_count") != 2 || num(d, "txn") != e.head(t) || d["txn_state"] != "none" || num(d, "seq") != 2 {
		t.Fatalf("describe: %d %v", a.status, a.frames)
	}
	if r := post(body); frameText(r) != frameText(a) {
		t.Errorf("replay %v, want %v", r.frames, a.frames)
	}
	// An idle describe, failed or not, returns its connection to the pool.
	pooled := func(what string, run func()) {
		t.Helper()
		p := e.s.pool
		p.mu.Lock()
		open, idle := p.open, len(p.idle)
		p.mu.Unlock()
		if idle == 0 { // the describe opens one
			open, idle = open+1, idle+1
		}
		run()
		p.mu.Lock()
		defer p.mu.Unlock()
		if len(p.busy) != 0 || p.open != open || len(p.idle) != idle {
			t.Errorf("%s: %d principals busy, %d open and %d idle, want 0, %d and %d", what, len(p.busy), p.open, len(p.idle), open, idle)
		}
	}
	pooled("a describe", func() { post(sq(tok, 3, "describe", "sql", "SELECT a FROM t")) })
	for i, tc := range []struct{ sql, code string }{{"SELECT nope FROM t", "sql_error"}, {"BEGIN", "invalid_request"}, {"SELECT 1; SELECT 2", "invalid_request"}, {"SAVEPOINT s", "invalid_request"}} {
		pooled(tc.sql, func() {
			if a := post(sq(tok, int64(i+4), "describe", "sql", tc.sql)); a.status != 200 || a.code() != tc.code || a.err()["not_executed"] != true || a.err()["final"] != true {
				t.Errorf("%s: %d %v", tc.sql, a.status, a.frames)
			}
		})
	}
	snap := num(post(sq(tok, 8, "begin")).done(), "txn")
	post(ex(tok, 9, "CREATE TABLE u (b TEXT)"))
	if a := post(sq(tok, 10, "describe", "sql", "SELECT b FROM u")); a.done()["txn_state"] != "open" || num(a.done(), "txn") != snap || num(a.done(), "param_count") != 0 {
		t.Errorf("describe in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 11, "describe", "sql", "SELECT nope FROM u")); a.code() != "sql_error" || a.err()["not_executed"] != true || a.err()["txn_state"] != "open" {
		t.Errorf("a failed describe in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 12, "describe", "sql", "SAVEPOINT s")); a.done()["txn_state"] != "open" {
		t.Errorf("a savepoint described in the transaction: %v", a.frames)
	}
	post(ex(tok, 13, "INSERT OR ROLLBACK INTO t VALUES (1), (1)"))
	if a := post(sq(tok, 14, "describe", "sql", "SELECT 1")); a.code() != "transaction_aborted" {
		t.Errorf("describe when aborted: %v", a.frames)
	}
	if a := post(sq(tok, 15, "rollback")); a.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v", a.frames)
	}
	if a := post(sq(tok, 16, "describe", "sql", "SELECT b FROM u")); a.code() != "sql_error" {
		t.Errorf("the rolled-back table: %v", a.frames)
	}
}

// TestBatchLimitAndReplay: a batch at the batch limit of 10,000 entries
// returns every entry's result in results frames, autocommit and in a
// transaction; its replay is over the retained-data bound of 64 KiB, so it
// is the receipt alone with data_omitted (ADR 0005 decision 3).
func TestBatchLimitAndReplay(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	sets := func(from int) [][]any {
		s := make([][]any, wire.DefaultLimits().BatchEntries)
		for i := range s {
			s[i] = []any{from + i}
		}
		return s
	}
	check := func(what string, seq int64, state string) {
		t.Helper()
		body := sq(tok, seq, "batch", "sql", "INSERT INTO t VALUES (?)", "param_sets", sets(int(seq)*100000))
		a := post(body)
		rs, ok := results(a)
		if d := a.done(); a.status != 200 || num(d, "entries") != 10000 || num(d, "rows_affected") != 10000 || d["txn_state"] != state || len(rs) != 10000 || !ok || len(a.frames) < 11 {
			t.Fatalf("%s: %d, %d frames, %d results, done %v", what, a.status, len(a.frames), len(rs), d)
		}
		r := post(body)
		d := r.done()
		if len(r.frames) != 1 || d["data_omitted"] != true || num(d, "entries") != 10000 || num(d, "rows_affected") != 10000 || num(d, "seq") != seq {
			t.Errorf("%s replay: %v", what, r.frames)
		}
	}
	check("autocommit", 2, "none")
	post(sq(tok, 3, "begin"))
	check("in a transaction", 4, "open")
	if a := post(sq(tok, 5, "commit")); a.done()["wrote"] != true {
		t.Fatalf("commit: %v", a.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[20000]" {
		t.Errorf("rows %s, want 20000", got)
	}
	if a := post(sq(tok, 6, "batch", "sql", "INSERT INTO t VALUES (?)", "param_sets", append(sets(0), []any{-1}))); a.code() != "invalid_request" {
		t.Errorf("a batch past the limit: %v", a.frames)
	}
}

// TestDescribeTxnControlOutsideTxn: describeOn itself refuses SAVEPOINT
// outside the client's transaction, even on a connection where SQLite's
// authorizer allows it because a transaction of the server's is open, as
// on a read.
func TestDescribeTxnControlOutsideTxn(t *testing.T) {
	e, _, _ := newTxnEnv(t, nil)
	e.count(t, "SELECT 1") // leaves a pooled connection idle
	p := e.s.pool
	p.mu.Lock()
	c := p.idle[len(p.idle)-1]
	p.mu.Unlock()
	out, err := wire.NewWriter(io.Discard, wire.DefaultFrameBytes, e.s.limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ec.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
		if err := c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("BEGIN") }); err != nil {
			return err
		}
		defer func() { _ = c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("ROLLBACK") }) }()
		ictx := db.GetInterrupt()
		if _, err := describeOn(c, ictx, "SAVEPOINT s", out, true); err != nil {
			t.Errorf("SAVEPOINT in the client's transaction: %v", err)
		}
		_, err := describeOn(c, ictx, "SAVEPOINT s", out, false)
		if we, ok := errors.AsType[*wire.Error](err); !ok || we.Code != wire.CodeInvalidRequest {
			t.Errorf("SAVEPOINT outside the client's transaction: %v, want invalid_request", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestBatchOnFailureResults: after a failure batchOn writes the results of
// the entries before it only inside the client's transaction, where they
// keep their effect; outside it, where nothing of the batch is kept, it
// writes none, even when its caller streams frames rather than holding them.
func TestBatchOnFailureResults(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	e.count(t, "SELECT 1") // leaves a pooled connection idle
	p := e.s.pool
	p.mu.Lock()
	c := p.idle[len(p.idle)-1]
	p.mu.Unlock()
	req, we := wire.Parse(wire.KindChannel, []byte(sq(tok, 2, "batch", "statements", stmts("INSERT INTO other VALUES (1)", "INSERT INTO nowhere VALUES (1)"))), 10)
	if we != nil {
		t.Fatal(we)
	}
	for _, inTxn := range []bool{true, false} {
		var b bytes.Buffer
		out, err := wire.NewWriter(&b, wire.DefaultFrameBytes, e.s.limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ec.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
			if err := c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("BEGIN") }); err != nil {
				return err
			}
			defer func() { _ = c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("ROLLBACK") }) }()
			if _, err := batchOn(db, c, db.GetInterrupt(), req, out, inTxn); err == nil {
				t.Error("the batch did not fail")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(b.String(), `"results"`); got != inTxn {
			t.Errorf("inTxn %v: results written %v: %s", inTxn, got, b.String())
		}
	}
}
