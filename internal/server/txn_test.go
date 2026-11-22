package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

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
	if r := post(sq(tok, 7, "rollback")); r.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v", r.frames)
	}
	if r := post(sq(tok, 8, "begin")); r.done()["txn_state"] != "open" {
		t.Errorf("begin after the rollback: %v", r.frames)
	}
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
	// aborts the transaction, even a read's.
	e.post(t, "channel", "writer", sq(tok, 4, "begin"))
	r := e.post(t, "channel", "writer", ex(tok, 5, cpuBound))
	if er := r.err(); r.code() != "timeout" || er["not_executed"] != false || er["txn_state"] != "aborted" || !strings.Contains(er["message"].(string), "lifetime") {
		t.Errorf("a statement past the lifetime: %v", r.frames)
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
	e, _, tok := newTxnEnv(t, func(e *env) { e.s.limits.WriteTxnIdleMS, e.s.chl.sweep = 500, 20*time.Millisecond })
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
