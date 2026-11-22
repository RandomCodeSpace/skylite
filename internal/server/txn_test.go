package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// sq is the body of a sequenced request op as seq on the channel of tok,
// with the extra fields.
func sq(tok string, seq int64, op string, extra ...any) string {
	m := map[string]any{"channel": tok, "seq": seq, "op": op}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i].(string)] = extra[i+1]
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// cancelBody is the body of a cancel of seq on the channel of tok.
func cancelBody(tok string, seq int64) string {
	return fmt.Sprintf(`{"channel":%q,"seq":%d}`, tok, seq)
}

// txState returns the transaction state of the channel of tok on "d", and
// whether it holds an open transaction.
func (e *env) txState(tok string) (wire.TxnState, bool) {
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	ch := e.s.dbs["d"].chans.chans[tok]
	if ch == nil {
		return "", false
	}
	return ch.state, ch.tx != nil
}

// writes checks that another channel's autocommit write commits: the write
// lock is free.
func (e *env) writes(t *testing.T) {
	t.Helper()
	tok := e.open(t, "admin")
	if a := e.post(t, "channel", "admin", ex(tok, 1, "INSERT INTO other VALUES (1)")); a.done() == nil {
		t.Errorf("another channel's write: %d %v, want it to commit", a.status, a.frames)
	}
}

// outcome returns the receipt embedded in a cancel or close answer, and
// its error object, if any.
func outcome(a answer) (map[string]any, map[string]any) {
	o, _ := a.done()["outcome"].(map[string]any)
	oe, _ := o["error"].(map[string]any)
	return o, oe
}

// newTxnEnv is newChanEnv with tables t (unique a) and other, and a write
// channel of "writer" with sequence number 1 taken.
func newTxnEnv(t *testing.T, edit func(*env)) (*env, *holdStore, string) {
	t.Helper()
	e, hs := newChanEnv(t, func(e *env) {
		e.s.limits.LockWaitMS = 300
		if edit != nil {
			edit(e)
		}
	})
	tok := e.open(t, "writer")
	if a := e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a UNIQUE)")); a.done() == nil {
		t.Fatal(a.frames)
	}
	e.exec(t, "CREATE TABLE other (a)")
	return e, hs, tok
}

// TestTransactionLifecycle: begin with and without a first statement,
// statements, ping, commit and rollback, the write lock held across
// requests, and the receipts of each.
func TestTransactionLifecycle(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	db := e.dbID(t)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	head := int64(e.leader(t).Head()) //nolint:gosec // small
	begin := sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (?) RETURNING a", "params": []any{1}})
	a := post(begin)
	if d := a.done(); a.status != 200 || d["txn_state"] != "open" || num(d, "txn") != head || num(d, "rows_affected") != 1 || len(a.rows()) != 1 {
		t.Fatalf("begin: %d %v, want open at %d with its row", a.status, a.frames, head)
	}
	if r := post(begin); frameText(r) != frameText(a) {
		t.Errorf("begin's replay %v, want %v", r.frames, a.frames)
	}
	// The transaction holds the write lock between its requests.
	other := e.open(t, "admin")
	if r := e.post(t, "channel", "admin", ex(other, 1, "INSERT INTO other VALUES (1)")); r.code() != "busy" {
		t.Errorf("a write while the transaction is open: %v, want busy", r.frames)
	}
	if r := post(sq(tok, 3, "begin")); r.code() != "invalid_state" || r.err()["not_executed"] != true || r.err()["txn_state"] != "open" {
		t.Errorf("begin inside a transaction: %v", r.frames)
	}
	if r := post(sq(tok, 4, "ping")); num(r.done(), "txn") != head || r.done()["txn_state"] != "open" {
		t.Errorf("ping: %v, want the snapshot %d", r.frames, head)
	}
	if r := post(ex(tok, 5, "INSERT INTO t VALUES (2)")); r.done()["txn_state"] != "open" || num(r.done(), "txn") != head {
		t.Errorf("execute: %v", r.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("a reader sees %s before the commit", got)
	}
	commit := sq(tok, 6, "commit")
	c := post(commit)
	if d := c.done(); c.status != 200 || d["committed"] != true || d["wrote"] != true || num(d, "txn") != head+1 || d["txn_state"] != "none" {
		t.Fatalf("commit: %d %v, want committed at %d", c.status, c.frames, head+1)
	}
	if r := post(commit); frameText(r) != frameText(c) {
		t.Errorf("commit's replay %v", r.frames)
	}
	if n := withID(e.commits(t, db), wantID(tok, 6)); n != 1 {
		t.Errorf("%d commits carry the commit's identifier, want 1", n)
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1,2"]` {
		t.Errorf("rows %s", got)
	}
	// A transaction that wrote nothing commits at its snapshot.
	if r := post(sq(tok, 7, "begin")); r.done()["txn_state"] != "open" || num(r.done(), "txn") != head+1 {
		t.Errorf("begin: %v", r.frames)
	}
	if r := post(sq(tok, 8, "commit")); r.done()["wrote"] != false || num(r.done(), "txn") != head+1 {
		t.Errorf("empty commit: %v", r.frames)
	}
	// Rollback leaves nothing, and frees the lock.
	post(sq(tok, 9, "begin"))
	post(ex(tok, 10, "INSERT INTO t VALUES (3)"))
	if r := post(sq(tok, 11, "rollback")); r.done()["txn_state"] != "none" || num(r.done(), "txn") != head+1 {
		t.Errorf("rollback: %v", r.frames)
	}
	if r := post(sq(tok, 12, "rollback")); r.code() != "invalid_state" {
		t.Errorf("rollback with no transaction: %v", r.frames)
	}
	e.writes(t)
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[2]" {
		t.Errorf("rows %s after the rollback", got)
	}
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	if e.s.dbs["d"].chans.lock.held {
		t.Error("the write lock is held after every transaction ended")
	}
}

func TestBeginFirstStatementFails(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	head := e.leader(t).Head()
	// FAIL keeps the first row inside a transaction; a failed begin does not.
	a := e.post(t, "channel", "writer", sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT OR FAIL INTO t VALUES (1), (1)"}))
	er := a.err()
	if a.status != 200 || a.code() != "sql_error" || er["not_executed"] != true || er["final"] != true || er["txn_state"] != "none" {
		t.Errorf("begin: %d %v, want a recorded sql_error, not executed, no transaction", a.status, a.frames)
	}
	if st, open := e.txState(tok); st != wire.TxnNone || open {
		t.Errorf("state %q, open %v after the failed begin", st, open)
	}
	if r := e.post(t, "channel", "writer", sq(tok, 3, "commit")); r.code() != "invalid_state" {
		t.Errorf("commit after the failed begin: %v", r.frames)
	}
	// A begin that fails its validation after acceptance leaves nothing too.
	if r := e.post(t, "channel", "writer", sq(tok, 4, "begin", "sql", "SELECT 1")); r.code() != "invalid_request" || r.err()["not_executed"] != true || r.err()["txn_state"] != "none" {
		t.Errorf("an invalid begin: %v", r.frames)
	}
	e.writes(t)
	if h := e.leader(t).Head(); h != head+1 {
		t.Errorf("head %d, want %d: only the other channel's write", h, head+1)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("rows %s, want none", got)
	}
}

func TestDeferredFKCommitEndsTxn(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	post(ex(tok, 2, "CREATE TABLE p (id INTEGER PRIMARY KEY)"))
	post(ex(tok, 3, "CREATE TABLE c (p REFERENCES p (id) DEFERRABLE INITIALLY DEFERRED)"))
	head := e.leader(t).Head()
	post(sq(tok, 4, "begin"))
	if r := post(ex(tok, 5, "INSERT INTO c VALUES (7)")); r.done() == nil {
		t.Fatalf("the deferred insert: %v", r.frames)
	}
	a := post(sq(tok, 6, "commit"))
	if er := a.err(); a.code() != "sql_error" || er["not_executed"] != true || er["final"] != true || er["txn_state"] != "none" || num(er, "sqlite_code") != 19 {
		t.Errorf("commit: %v, want a recorded constraint error that ends the transaction", a.frames)
	}
	if r := post(sq(tok, 7, "rollback")); r.code() != "invalid_state" {
		t.Errorf("rollback after the failed commit: %v, want invalid_state", r.frames)
	}
	e.writes(t)
	if h := e.leader(t).Head(); h != head+1 {
		t.Errorf("head %d, want %d", h, head+1)
	}
}

func TestInsertOrRollbackAborts(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	post(sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (1)"}))
	a := post(ex(tok, 3, "INSERT OR ROLLBACK INTO t VALUES (1)"))
	if er := a.err(); a.code() != "sql_error" || er["not_executed"] != false || er["final"] != true || er["txn_state"] != "aborted" {
		t.Errorf("the failed statement: %v, want sql_error, not_executed false, aborted", a.frames)
	}
	// Aborted: the transaction and the lock are gone; only rollback is accepted.
	e.writes(t)
	for i, b := range []string{ex(tok, 4, "INSERT INTO t VALUES (2)"), sq(tok, 5, "ping"), sq(tok, 6, "commit")} {
		r := post(b)
		if er := r.err(); r.code() != "transaction_aborted" || er["not_executed"] != true || er["txn_state"] != "aborted" || er["cause"] != nil {
			t.Errorf("request %d while aborted: %v", i, r.frames)
		}
	}
	if r := post(sq(tok, 7, "rollback", "sql", "SELECT 1")); r.code() != "invalid_request" || r.err()["not_executed"] != true || r.err()["txn_state"] != "aborted" {
		t.Errorf("an invalid rollback while aborted: %v, want invalid_request, still aborted", r.frames)
	}
	if r := post(sq(tok, 8, "rollback")); r.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v", r.frames)
	}
	if r := post(sq(tok, 9, "begin")); r.done()["txn_state"] != "open" {
		t.Errorf("begin after the rollback: %v", r.frames)
	}
	// An interrupted write: SQLite rolls the transaction back.
	slow := "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) INSERT INTO other SELECT x FROM c"
	if r := post(ex(tok, 10, slow, "timeout_ms", 200)); r.code() != "timeout" || r.err()["not_executed"] != false || r.err()["txn_state"] != "aborted" {
		t.Errorf("a write past the statement time: %v, want timeout and aborted", r.frames)
	}
	e.writes(t)
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("rows %s", got)
	}
}

func TestCancelBeforeArrivalSameAndOtherBody(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	cancel := func(seq int64) answer { return e.post(t, "channel/cancel", "writer", cancelBody(tok, seq)) }
	head := e.leader(t).Head()
	// The last request, finished: its receipt.
	if r := cancel(1); r.status != 200 || num(r.done(), "seq") != 1 || r.done()["txn_state"] != "none" {
		t.Errorf("cancel of the last: %d %v", r.status, r.frames)
	} else if o, _ := outcome(r); o["ok"] != true {
		t.Errorf("cancel of the last: %v, want its receipt", r.frames)
	}
	for _, c := range []struct {
		seq  int64
		code string
	}{{0, "invalid_request"}, {3, "sequence_gap"}} {
		if r := cancel(c.seq); r.code() != c.code {
			t.Errorf("cancel %d: %v, want %s", c.seq, r.frames, c.code)
		}
	}
	if r := e.post(t, "channel/cancel", "admin", cancelBody(tok, 2)); r.code() != "channel_lost" {
		t.Errorf("another principal's cancel: %v", r.frames)
	}
	// The next one, before it arrives.
	r := cancel(2)
	o, oe := outcome(r)
	if r.status != 200 || o["ok"] != false || oe["code"] != "cancelled" || oe["not_executed"] != true || oe["final"] != true || num(oe, "seq") != 2 {
		t.Fatalf("cancel before arrival: %d %v", r.status, r.frames)
	}
	for _, b := range []string{ex(tok, 2, "INSERT INTO t VALUES (1)"), ex(tok, 2, "INSERT INTO t VALUES (2)"), sq(tok, 2, "begin")} {
		if a := post(b); a.status != 200 || a.code() != "cancelled" || a.err()["final"] != true || a.err()["not_executed"] != true {
			t.Errorf("%s after its cancel: %d %v, want the cancelled receipt", b, a.status, a.frames)
		}
	}
	if r := cancel(1); r.code() != "stale_sequence" {
		t.Errorf("cancel below the last: %v", r.frames)
	}
	if h := e.leader(t).Head(); h != head {
		t.Errorf("head %d, was %d: a cancelled request ran", h, head)
	}
	// Inside a transaction: the transaction stays open.
	post(sq(tok, 3, "begin"))
	if r := post(sq(tok, 3, "ping")); r.code() != "sequence_conflict" {
		t.Errorf("a different body for the number after the cancelled one: %v, want sequence_conflict", r.frames)
	}
	if r := cancel(4); r.done()["txn_state"] != "open" {
		t.Errorf("cancel in a transaction: %v", r.frames)
	}
	if a := post(ex(tok, 4, "INSERT INTO t VALUES (9)")); a.code() != "cancelled" || a.err()["txn_state"] != "open" {
		t.Errorf("the cancelled statement: %v", a.frames)
	}
	if a := post(sq(tok, 5, "commit")); a.done()["wrote"] != false {
		t.Errorf("commit: %v, want an empty commit", a.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("rows %s", got)
	}
}

// TestCancelRunningStatementInTxn: a cancel interrupts a statement in a
// transaction and answers once it has finished; a read leaves the
// transaction open.
func TestCancelRunningStatementInTxn(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	e.post(t, "channel", "writer", sq(tok, 2, "begin"))
	run := e.postAsync(t, "channel", "writer", ex(tok, 3, cpuBound, "timeout_ms", 10000))
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.running })
	time.Sleep(100 * time.Millisecond)
	if r := e.post(t, "channel/cancel", "writer", cancelBody(tok, 4)); r.code() != "channel_busy" {
		t.Errorf("cancel of the next while one runs: %v", r.frames)
	}
	r := e.post(t, "channel/cancel", "writer", cancelBody(tok, 3))
	_, oe := outcome(r)
	if r.status != 200 || oe["code"] != "cancelled" || oe["not_executed"] != false || r.done()["txn_state"] != "open" {
		t.Errorf("cancel: %d %v, want the cancelled receipt with the transaction open", r.status, r.frames)
	}
	if a := <-run; a.code() != "cancelled" || a.err()["txn_state"] != "open" {
		t.Errorf("the statement: %v", a.frames)
	}
	if a := e.post(t, "channel", "writer", sq(tok, 4, "commit")); a.done()["committed"] != true {
		t.Errorf("commit: %v", a.frames)
	}
}

func TestCancelDuringUploadReportsCommitted(t *testing.T) {
	e, hs, tok := newTxnEnv(t, nil)
	for i, bodies := range [][]string{
		{ex(tok, 2, "INSERT INTO t VALUES (1)")}, // autocommit
		{sq(tok, 3, "begin"), ex(tok, 4, "INSERT INTO t VALUES (2)"), sq(tok, 5, "commit")},
	} {
		for _, b := range bodies[:len(bodies)-1] {
			e.post(t, "channel", "writer", b)
		}
		var last struct{ Seq int64 }
		_ = json.Unmarshal([]byte(bodies[len(bodies)-1]), &last)
		g, release := hs.hold()
		run := e.postAsync(t, "channel", "writer", bodies[len(bodies)-1])
		g.wait(t)
		c := e.postAsync(t, "channel/cancel", "writer", cancelBody(tok, last.Seq))
		select {
		case r := <-c:
			t.Fatalf("case %d: cancel answered %v during the upload", i, r.frames)
		case <-time.After(300 * time.Millisecond):
		}
		release()
		a := <-run
		txn := num(a.done(), "txn")
		r := <-c
		o, _ := outcome(r)
		if txn == 0 || o["ok"] != true || num(o, "txn") != txn || r.done()["txn_state"] != "none" {
			t.Errorf("case %d: write %v, cancel %v; want both committed at one txn", i, a.frames, r.frames)
		}
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[2]" {
		t.Errorf("rows %s", got)
	}
}

func TestLifetimeFiresWhileCommitting(t *testing.T) {
	e, hs, tok := newTxnEnv(t, func(e *env) { e.s.limits.WriteTxnLifetimeMS, e.s.chl.sweep = 1000, 20*time.Millisecond })
	e.post(t, "channel", "writer", sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (1)"}))
	g, release := hs.hold()
	run := e.postAsync(t, "channel", "writer", sq(tok, 3, "commit"))
	g.wait(t)
	time.Sleep(1300 * time.Millisecond)
	release()
	a := <-run
	if d := a.done(); d["committed"] != true || d["wrote"] != true || d["txn_state"] != "none" {
		t.Errorf("commit past the lifetime in its upload: %v, want committed", a.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[1]" {
		t.Errorf("rows %s", got)
	}
	// Before its commit point, the lifetime ends a running statement and
	// aborts the transaction, even a read's. It counts from the begin.
	e.post(t, "channel", "writer", sq(tok, 4, "begin"))
	time.Sleep(600 * time.Millisecond)
	start := time.Now()
	r := e.post(t, "channel", "writer", ex(tok, 5, cpuBound))
	if er := r.err(); r.code() != "timeout" || er["not_executed"] != false || er["txn_state"] != "aborted" || !strings.Contains(er["message"].(string), "lifetime") {
		t.Errorf("a statement past the lifetime: %v", r.frames)
	}
	if d := time.Since(start); d > 900*time.Millisecond {
		t.Errorf("the statement ran %v, want the lifetime to end it about 400ms in", d)
	}
	e.writes(t)
	// And, idle, the transaction is aborted by the sweep.
	e.post(t, "channel", "writer", sq(tok, 6, "rollback"))
	e.post(t, "channel", "writer", sq(tok, 7, "begin"))
	time.Sleep(1300 * time.Millisecond)
	if r := e.post(t, "channel", "writer", sq(tok, 8, "ping")); r.code() != "transaction_aborted" {
		t.Errorf("after the lifetime: %v", r.frames)
	}
}

func TestTxnIdleLimitAndStatementTime(t *testing.T) {
	e, _, tok := newTxnEnv(t, func(e *env) {
		e.s.limits.ChannelIdleMS, e.s.limits.WriteTxnIdleMS, e.s.chl.sweep = 4000, 500, 20*time.Millisecond
	})
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	// The statement time inside a transaction: a read times out, and the
	// transaction stays open.
	post(sq(tok, 2, "begin"))
	if r := post(ex(tok, 3, cpuBound, "timeout_ms", 200)); r.code() != "timeout" || r.err()["not_executed"] != false || r.err()["txn_state"] != "open" {
		t.Errorf("a read past the statement time: %v", r.frames)
	}
	if r := post(sq(tok, 4, "commit")); r.done()["committed"] != true {
		t.Errorf("commit: %v", r.frames)
	}
	post(sq(tok, 5, "begin"))
	time.Sleep(1000 * time.Millisecond)
	if st, open := e.txState(tok); st != wire.TxnAborted || open {
		t.Errorf("state %q, open %v past the transaction idle limit", st, open)
	}
	e.writes(t)
	if r := post(ex(tok, 6, "INSERT INTO t VALUES (1)")); r.code() != "transaction_aborted" || r.err()["cause"] != nil {
		t.Errorf("after the idle limit: %v", r.frames)
	}
	e.waitChan(t, tok, func(_ state, ok bool) bool { return !ok }) // an aborted channel idles away too
}

func TestChannelIdleTimerIgnoredWithTxnOpen(t *testing.T) {
	e, _, tok := newTxnEnv(t, func(e *env) {
		e.s.limits.ChannelIdleMS, e.s.limits.WriteTxnIdleMS, e.s.chl.sweep = 300, 5000, 20*time.Millisecond
	})
	e.post(t, "channel", "writer", sq(tok, 2, "begin"))
	time.Sleep(800 * time.Millisecond)
	if r := e.post(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (1)")); r.done()["txn_state"] != "open" {
		t.Fatalf("a statement after the channel idle limit with a transaction open: %v", r.frames)
	}
	if r := e.post(t, "channel", "writer", sq(tok, 4, "commit")); r.done()["committed"] != true {
		t.Fatalf("commit: %v", r.frames)
	}
	e.waitChan(t, tok, func(_ state, ok bool) bool { return !ok }) // idle again: it ends
}

// TestMaintenanceAbortsOpenTxn: when the engine ends an idle open
// transaction on its own, as maintenance does through Conn.Aborted, the
// write lock is released at once and the channel is aborted with cause
// maintenance. The engine's maintenance cannot start while another holds
// the write lock, so the test ends the transaction by closing the engine
// connection, which closes Aborted too.
func TestMaintenanceAbortsOpenTxn(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	post(sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (1)"}))
	e.s.chMu.Lock()
	ec := e.s.dbs["d"].chans.chans[tok].tx.c.ec
	e.s.chMu.Unlock()
	if err := ec.Close(); err != nil {
		t.Fatal(err)
	}
	e.writes(t)
	if st, open := e.txState(tok); st != wire.TxnAborted || open {
		t.Errorf("state %q, open %v after the engine ended the transaction", st, open)
	}
	if r := post(ex(tok, 3, "INSERT INTO t VALUES (2)")); r.code() != "transaction_aborted" || r.err()["cause"] != "maintenance" {
		t.Errorf("next request: %v, want transaction_aborted with cause maintenance", r.frames)
	}
	if r := post(sq(tok, 4, "rollback")); r.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v", r.frames)
	}
	if r := post(ex(tok, 5, "INSERT INTO t VALUES (3)")); r.done() == nil {
		t.Errorf("a write after the rollback: %v", r.frames)
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["3"]` {
		t.Errorf("rows %s", got)
	}
	// Leaving the aborted state clears the cause.
	post(sq(tok, 6, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (4)"}))
	post(ex(tok, 7, "INSERT OR ROLLBACK INTO t VALUES (3)"))
	if r := post(sq(tok, 8, "ping")); r.code() != "transaction_aborted" || r.err()["cause"] != nil {
		t.Errorf("after a later abort by SQLite: %v, want no cause", r.frames)
	}
	// A channel ended with its transaction open, as when its leader stops,
	// releases the transaction.
	post(sq(tok, 9, "rollback"))
	post(sq(tok, 10, "begin"))
	e.s.chMu.Lock()
	e.s.end(e.s.dbs["d"].chans.chans[tok])
	e.s.chMu.Unlock()
	e.writes(t)
}

// TestConcurrentCloseCancel: closes and cancels that race with running
// requests and open transactions each get a well-formed answer, and leave
// no channel, lock or connection behind.
func TestConcurrentCloseCancel(t *testing.T) {
	e, hs, _ := newTxnEnv(t, nil)
	tk := e.sig.token(t, "writer")
	for i := range 12 {
		tok := e.open(t, "writer")
		e.post(t, "channel", "writer", sq(tok, 1, "begin"))
		var run <-chan answer
		switch i % 3 {
		case 0: // a statement running in the transaction
			run = e.postAsync(t, "channel", "writer", ex(tok, 2, cpuBound, "timeout_ms", 10000))
			e.waitChan(t, tok, func(s state, _ bool) bool { return s.running })
		case 1: // a commit in its upload
			e.post(t, "channel", "writer", ex(tok, 2, "INSERT INTO other VALUES (1)"))
			g, release := hs.hold()
			run = e.postAsync(t, "channel", "writer", sq(tok, 3, "commit"))
			g.wait(t)
			go func() { time.Sleep(50 * time.Millisecond); release() }()
		}
		var wg sync.WaitGroup
		for j := range 8 {
			wg.Go(func() {
				path, body := "channel/close", closeBody(tok)
				if j%2 == 1 {
					// the last sequence number, or the next
					path, body = "channel/cancel", cancelBody(tok, []int64{2, 3, 1}[i%3]+int64(j%4/3))
				}
				a, err := e.send(path, tk, body)
				if err != nil {
					t.Error(err)
					return
				}
				switch c := a.code(); {
				case a.done() != nil, c == "channel_lost", c == "channel_busy", c == "stale_sequence", c == "sequence_gap":
				default:
					t.Errorf("round %d, %s: %d %v", i, path, a.status, a.frames)
				}
			})
		}
		wg.Wait()
		if run != nil {
			if a := <-run; a.done() == nil && a.code() != "cancelled" {
				t.Errorf("round %d: the running request %v", i, a.frames)
			}
		}
		e.waitChan(t, tok, func(_ state, ok bool) bool { return !ok })
	}
	e.writes(t)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		e.s.chMu.Lock()
		n, held := e.s.nChans, e.s.dbs["d"].chans.lock.held
		e.s.chMu.Unlock()
		e.s.pool.mu.Lock()
		busy := len(e.s.pool.busy)
		e.s.pool.mu.Unlock()
		if n == 2 && !held && busy == 0 { // the env's channel and writes'
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d channels, lock held %v, %d principals holding connections", n, held, busy)
		}
	}
	if a := e.post(t, "channel", "writer", ex(e.open(t, "writer"), 1, "INSERT INTO t VALUES (5)")); a.done() == nil || a.status != http.StatusOK {
		t.Errorf("a write after the rounds: %v", a.frames)
	}
}

// TestInvalidRollbackKeepsInvalidCommitEnds: a rollback that fails its
// validation is recorded and leaves the transaction open, holding the
// write lock; a commit that fails its validation ends it.
func TestInvalidRollbackKeepsInvalidCommitEnds(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	other := e.open(t, "admin")
	post(sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (1)"}))
	if r := post(sq(tok, 3, "rollback", "sql", "SELECT 1")); r.code() != "invalid_request" || r.err()["not_executed"] != true || r.err()["txn_state"] != "open" {
		t.Errorf("an invalid rollback: %v, want invalid_request with the transaction open", r.frames)
	}
	if st, open := e.txState(tok); st != wire.TxnOpen || !open {
		t.Errorf("state %q, open %v after the invalid rollback", st, open)
	}
	if r := e.post(t, "channel", "admin", ex(other, 1, "INSERT INTO other VALUES (1)")); r.code() != "busy" {
		t.Errorf("a write after the invalid rollback: %v, want busy", r.frames)
	}
	if r := post(sq(tok, 4, "commit", "sql", "SELECT 1")); r.code() != "invalid_request" || r.err()["not_executed"] != true || r.err()["txn_state"] != "none" {
		t.Errorf("an invalid commit: %v, want invalid_request with the transaction ended", r.frames)
	}
	if st, open := e.txState(tok); st != wire.TxnNone || open {
		t.Errorf("state %q, open %v after the invalid commit", st, open)
	}
	e.writes(t)
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("rows %s, want none", got)
	}
}

// TestCancelWaiterVsNextRequest: a cancel that waits for its running
// request answers with that request's receipt even if the next request is
// accepted, or has finished, before it takes the channel again. The
// interleaving is set by hand.
func TestCancelWaiterVsNextRequest(t *testing.T) {
	for _, nextFinished := range []bool{false, true} {
		e, _, tok := newTxnEnv(t, nil)
		s, d, leader := e.s, e.s.dbs["d"], e.leader(t)
		waiting, fin := make(chan struct{}), make(chan struct{})
		s.chMu.Lock()
		ch := d.chans.chans[tok]
		ch.last, ch.running, ch.receipt, ch.run = 2, true, nil, &completion{finished: fin}
		ch.cancel = func(error) { close(waiting) }
		s.chMu.Unlock()
		type res struct {
			done *wire.Done
			e    *wire.Error
			p    any
		}
		got := make(chan res, 1)
		go func() {
			var r res
			defer func() { r.p = recover(); got <- r }()
			r.done, r.e = s.cancelRequest(d, leader, &wire.Request{Channel: tok, Seq: 2}, ch.who, "id")
		}()
		<-waiting
		s.chMu.Lock() // the cancel waits for fin with chMu released
		ch.running, ch.receipt = false, &wire.Receipt{Done: &wire.Done{Kind: wire.DoneCommit, Seq: 2, Txn: 7, Wrote: true}}
		ch.run.receipt, ch.run.state = ch.receipt, wire.TxnNone
		close(fin)
		ch.last, ch.running, ch.receipt, ch.run = 3, true, nil, &completion{finished: make(chan struct{})} // 3 accepted
		if nextFinished {
			te := wire.NewError(wire.CodeTimeout, "x").Recorded(false)
			te.Seq = 3
			ch.running, ch.receipt, ch.run.receipt = false, &wire.Receipt{Error: te}, &wire.Receipt{Error: te}
			close(ch.run.finished)
		}
		s.chMu.Unlock()
		r := <-got
		if r.p != nil || r.e != nil || r.done == nil || r.done.Kind != wire.DoneCancel || r.done.Seq != 2 || r.done.TxnState != wire.TxnNone ||
			r.done.Outcome == nil || r.done.Outcome.Done == nil || r.done.Outcome.Done.Kind != wire.DoneCommit || r.done.Outcome.Done.Txn != 7 {
			t.Errorf("nextFinished=%v: cancel(2) answered %+v, %+v, panic %v; want commit 2's receipt", nextFinished, r.done, r.e, r.p)
		}
		s.chMu.Lock()
		ch.running = false // nothing runs 3
		s.chMu.Unlock()
	}
}

// TestCloseJoinsRunningTxn: a close that joins a request running in a
// transaction, and a second close and a cancel that join it too, answer
// only once the transaction is rolled back and the write lock released; the
// cancel answers txn_state none, and a cancel of another number is refused. The running request is set by hand and recorded through record,
// with the pool held so that the rollback cannot finish early.
func TestCloseJoinsRunningTxn(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	s, d, leader := e.s, e.s.dbs["d"], e.leader(t)
	e.post(t, "channel", "writer", sq(tok, 2, "begin"))
	interrupted := make(chan struct{}, 3)
	s.chMu.Lock()
	ch := d.chans.chans[tok]
	ch.last, ch.running, ch.receipt, ch.run = 3, true, nil, &completion{finished: make(chan struct{})}
	ch.cancel = func(error) { interrupted <- struct{}{} }
	s.chMu.Unlock()
	type res struct {
		done *wire.Done
		e    *wire.Error
	}
	lockHeld := func() bool {
		s.chMu.Lock()
		defer s.chMu.Unlock()
		return d.chans.lock.held
	}
	closed, cancelled := make(chan res, 2), make(chan res, 1)
	closeChannel := func() {
		var r res
		r.done, r.e = s.closeChannel(d, leader, &wire.Request{Channel: tok}, ch.who)
		if lockHeld() {
			t.Error("the write lock is held after a close answered")
		}
		closed <- r
	}
	go closeChannel()
	<-interrupted
	go closeChannel()
	<-interrupted
	if _, er := s.cancelRequest(d, leader, &wire.Request{Channel: tok, Seq: 4}, ch.who, "id"); er == nil || er.Code != wire.CodeChannelLost {
		t.Errorf("cancel of the next on a closing channel: %+v, want channel_lost", er)
	}
	go func() {
		var r res
		r.done, r.e = s.cancelRequest(d, leader, &wire.Request{Channel: tok, Seq: 3}, ch.who, "id")
		cancelled <- r
	}()
	<-interrupted
	s.pool.mu.Lock()
	ce := wire.NewError(wire.CodeCancelled, "x").Recorded(true)
	ce.Seq, ce.TxnState = 3, wire.TxnOpen
	s.record(ch, &wire.Receipt{Error: ce}, nil, nil, false, wire.TxnOpen)
	select {
	case r := <-closed:
		t.Errorf("a close answered %+v before the transaction's connection was released", r.done)
		closed <- r
	case r := <-cancelled:
		t.Errorf("the joined cancel answered %+v before the transaction's connection was released", r.done)
		cancelled <- r
	case <-time.After(200 * time.Millisecond):
	}
	s.pool.mu.Unlock()
	if r := <-cancelled; r.done == nil || r.done.TxnState != wire.TxnNone || r.done.Outcome == nil || r.done.Outcome.Error.Code != wire.CodeCancelled || lockHeld() {
		t.Errorf("cancel that joined a close: %+v, %+v, lock held %t; want txn_state none with 3's receipt, lock released", r.done, r.e, lockHeld())
	}
	for range 2 {
		if r := <-closed; r.done == nil || r.done.LastSeq != 3 || r.done.Outcome == nil || r.done.Outcome.Error.Code != wire.CodeCancelled {
			t.Errorf("close: %+v, %+v; want last_seq 3 with its cancelled receipt", r.done, r.e)
		}
	}
	e.writes(t)
}

// TestSavepointsInTxn: SAVEPOINT, RELEASE and ROLLBACK TO run inside a
// transaction (0007); outside one they are refused, and so is a plain
// ROLLBACK inside one.
func TestSavepointsInTxn(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	if r := post(ex(tok, 2, "SAVEPOINT s")); r.code() == "" {
		t.Errorf("a savepoint outside a transaction: %v, want refused", r.frames)
	}
	post(sq(tok, 3, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (1)"}))
	for i, sql := range []string{"SAVEPOINT s", "INSERT INTO t VALUES (2)", "ROLLBACK TO s", "SAVEPOINT r", "INSERT INTO t VALUES (3)", "RELEASE r", "rollback transaction to s", "EXPLAIN SAVEPOINT q", "EXPLAIN QUERY PLAN SAVEPOINT q", "RELEASE s"} {
		if r := post(ex(tok, int64(4+i), sql)); r.code() != "" || r.done()["txn_state"] != "open" {
			t.Fatalf("%s: %v", sql, r.frames)
		}
	}
	if r := post(ex(tok, 14, "ROLLBACK")); r.code() != "invalid_request" {
		t.Errorf("a plain ROLLBACK in a transaction: %v, want invalid_request", r.frames)
	}
	if st, open := e.txState(tok); st != wire.TxnOpen || !open {
		t.Errorf("state %q, open %v after the refused ROLLBACK", st, open)
	}
	if r := post(sq(tok, 15, "commit")); r.code() != "" {
		t.Fatalf("commit: %v", r.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[1]" {
		t.Errorf("rows %s, want only the first insert", got)
	}
}

// TestTxnGoneAfterStatement: a statement that succeeds after SQLite's
// transaction has gone underneath it answers aborted and frees the lock
// (ADR 0007 requirement 8). The rollback is injected.
func TestTxnGoneAfterStatement(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	post(sq(tok, 2, "begin", "execute", map[string]any{"sql": "INSERT INTO t VALUES (1)"}))
	e.s.chMu.Lock()
	tx := e.s.dbs["d"].chans.chans[tok].tx
	e.s.chMu.Unlock()
	if _, err := tx.c.ec.Run(t.Context(), nil, func(*sqlite3.Conn) error {
		return tx.c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("ROLLBACK") })
	}); err != nil {
		t.Fatal(err)
	}
	if r := post(ex(tok, 3, "SELECT 1")); r.code() != "" || r.done()["txn_state"] != "aborted" {
		t.Errorf("a statement after the transaction went: %v, want done with txn_state aborted", r.frames)
	}
	other := e.open(t, "admin")
	if r := e.post(t, "channel", "admin", ex(other, 1, "INSERT INTO other VALUES (1)")); r.code() != "" {
		t.Errorf("a write after the transaction went: %v, want the lock free", r.frames)
	}
	if r := post(sq(tok, 4, "rollback")); r.code() != "" || r.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v, want txn_state none", r.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[0]" {
		t.Errorf("rows %s, want none", got)
	}
}

// quiet fails the test if the database lock or the pool still holds anything.
func (e *env) quiet(t *testing.T, what string) {
	t.Helper()
	e.s.chMu.Lock()
	h, q := e.s.dbs["d"].chans.lock.held, len(e.s.dbs["d"].chans.lock.queue)
	e.s.chMu.Unlock()
	e.s.pool.mu.Lock()
	busy, open := len(e.s.pool.busy), e.s.pool.open
	neg := false
	for _, n := range e.s.pool.busy {
		neg = neg || n < 0
	}
	e.s.pool.mu.Unlock()
	if h || q != 0 || busy != 0 || neg || open < 0 {
		t.Errorf("%s: lock held=%v queue=%d pool busy=%d negative=%v open=%d", what, h, q, busy, neg, open)
	}
}

// A request that ends lost (panic guard) with a close, a second close and a
// cancel waiting: every waiter gets the lost error after the rollback.
func TestLostReachesEveryWaiter(t *testing.T) {
	e, _, tok := newTxnEnv(t, nil)
	s, d, leader := e.s, e.s.dbs["d"], e.leader(t)
	e.post(t, "channel", "writer", sq(tok, 2, "begin"))
	interrupted := make(chan struct{}, 3)
	s.chMu.Lock()
	ch := d.chans.chans[tok]
	ch.last, ch.running, ch.receipt, ch.run = 3, true, nil, &completion{finished: make(chan struct{})}
	ch.cancel = func(error) { interrupted <- struct{}{} }
	s.chMu.Unlock()
	got := make(chan *wire.Error, 3)
	closer := func() {
		_, er := s.closeChannel(d, leader, &wire.Request{Channel: tok}, ch.who)
		e.quiet(t, "close answered")
		got <- er
	}
	go closer()
	<-interrupted
	go closer()
	<-interrupted
	go func() {
		_, er := s.cancelRequest(d, leader, &wire.Request{Channel: tok, Seq: 3}, ch.who, "id")
		e.quiet(t, "cancel answered")
		got <- er
	}()
	<-interrupted
	l := wire.NewError(wire.CodeChannelLost, "lost")
	l.Seq = 3
	s.record(ch, nil, l, nil, false, wire.TxnNone)
	seqs := map[int64]int{}
	for range 3 {
		er := <-got
		if er == nil || er.Code != wire.CodeChannelLost {
			t.Fatalf("waiter got %+v, want channel_lost", er)
		}
		seqs[er.Seq]++
	}
	if seqs[0] != 2 || seqs[3] != 1 {
		t.Errorf("seqs %v, want two closes with 0 and a cancel with 3", seqs)
	}
	e.writes(t)
}

func TestSavepointSQL(t *testing.T) {
	for sql, want := range map[string]bool{
		"EXPLAIN QUERY PLAN ROLLBACK TO s":             true,
		"explain query plan rollback transaction to s": true,
		"EXPLAIN ROLLBACK TO SAVEPOINT s":              true,
		"EXPLAIN RELEASE s":                            true,
		"EXPLAIN ROLLBACK":                             false,
		"EXPLAIN QUERY PLAN ROLLBACK":                  false,
		"EXPLAIN QUERY PLAN COMMIT":                    false,
		"EXPLAIN EXPLAIN SAVEPOINT s":                  false,
		"EXPLAIN":                                      false,
	} {
		if got := savepointSQL(sql); got != want {
			t.Errorf("savepointSQL(%q) = %v, want %v", sql, got, want)
		}
		if !txnControl(sql) && sql != "EXPLAIN" && sql != "EXPLAIN EXPLAIN SAVEPOINT s" {
			t.Errorf("txnControl(%q) = false", sql)
		}
	}
}
