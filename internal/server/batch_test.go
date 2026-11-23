package server

import (
	"net/http"
	"testing"
	"time"
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
	e.s.pool.mu.Lock()
	busy := len(e.s.pool.busy)
	e.s.pool.mu.Unlock()
	if busy != 0 {
		t.Errorf("%d principals hold connections after an idle describe", busy)
	}
	for i, tc := range []struct{ sql, code string }{{"SELECT nope FROM t", "sql_error"}, {"BEGIN", "invalid_request"}, {"SELECT 1; SELECT 2", "invalid_request"}} {
		if a := post(sq(tok, int64(i+3), "describe", "sql", tc.sql)); a.status != 200 || a.code() != tc.code || a.err()["not_executed"] != true || a.err()["final"] != true {
			t.Errorf("%s: %d %v", tc.sql, a.status, a.frames)
		}
	}
	snap := num(post(sq(tok, 6, "begin")).done(), "txn")
	post(ex(tok, 7, "CREATE TABLE u (b TEXT)"))
	if a := post(sq(tok, 8, "describe", "sql", "SELECT b FROM u")); a.done()["txn_state"] != "open" || num(a.done(), "txn") != snap || num(a.done(), "param_count") != 0 {
		t.Errorf("describe in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 9, "describe", "sql", "SELECT nope FROM u")); a.code() != "sql_error" || a.err()["not_executed"] != true || a.err()["txn_state"] != "open" {
		t.Errorf("a failed describe in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 10, "describe", "sql", "SAVEPOINT s")); a.done()["txn_state"] != "open" {
		t.Errorf("a savepoint described in the transaction: %v", a.frames)
	}
	post(ex(tok, 11, "INSERT OR ROLLBACK INTO t VALUES (1), (1)"))
	if a := post(sq(tok, 12, "describe", "sql", "SELECT 1")); a.code() != "transaction_aborted" {
		t.Errorf("describe when aborted: %v", a.frames)
	}
	if a := post(sq(tok, 13, "rollback")); a.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v", a.frames)
	}
	if a := post(sq(tok, 14, "describe", "sql", "SELECT b FROM u")); a.code() != "sql_error" {
		t.Errorf("the rolled-back table: %v", a.frames)
	}
}
