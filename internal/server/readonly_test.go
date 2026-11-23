package server

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// openRO opens a read-only channel on "d" as subject and returns its token.
func (e *env) openRO(t *testing.T, subject string) string {
	t.Helper()
	a := e.post(t, "channels", subject, `{"mode":"read_only"}`)
	c, _ := a.done()["channel"].(map[string]any)
	tok, _ := c["token"].(string)
	if a.status != http.StatusOK || !strings.HasPrefix(tok, readTokenPrefix+".") {
		t.Fatalf("read-only open: %d %v", a.status, a.frames)
	}
	return tok
}

// readonly checks that a is a recorded SQLITE_READONLY failure with flags
// not_executed and the transaction state.
func readonly(t *testing.T, what string, a answer, notExecuted bool, state string) {
	t.Helper()
	er := a.err()
	if a.status != 200 || a.code() != "sql_error" || num(er, "sqlite_code") != int64(sqlite3.READONLY) || er["not_executed"] != notExecuted || er["final"] != true || er["txn_state"] != state {
		t.Errorf("%s: %d %v, want a recorded SQLITE_READONLY", what, a.status, a.frames)
	}
}

// TestReadOnlyChannel: a read-only channel on the leader needs only the
// read role, refuses writes with SQLITE_READONLY, reads one snapshot per
// transaction while writers commit, and holds no write lock.
func TestReadOnlyChannel(t *testing.T) {
	e, _, wtok := newTxnEnv(t, nil)
	e.post(t, "channel", "writer", ex(wtok, 2, "INSERT INTO t VALUES (1)"))
	head := e.head(t)
	if a := e.post(t, "channels", "reader", fmt.Sprintf(`{"mode":"read_only","min_txn":%d}`, head+1)); a.status != http.StatusServiceUnavailable || a.code() != "behind" {
		t.Errorf("an open above the head: %d %v", a.status, a.frames)
	}
	if a := e.post(t, "channels", "reader", `{"mode":"read_only","min_txn":1,"x":1}`); a.status != http.StatusBadRequest {
		t.Errorf("an open with an unknown field: %d %v", a.status, a.frames)
	}
	tok := e.openRO(t, "reader")
	post := func(body string) answer { return e.post(t, "channel", "reader", body) }

	// Autocommit: a read at the head, replayed; a write refused.
	body := ex(tok, 1, "SELECT a FROM t", "min_txn", head)
	a := post(body)
	if d := a.done(); a.status != 200 || len(a.rows()) != 1 || num(d, "txn") != head || d["read_only"] != true || d["txn_state"] != "none" {
		t.Fatalf("read: %d %v", a.status, a.frames)
	}
	if r := post(body); frameText(r) != frameText(a) {
		t.Errorf("replay %v, want %v", r.frames, a.frames)
	}
	readonly(t, "an autocommit write", post(ex(tok, 2, "INSERT INTO t VALUES (2)")), true, "none")
	readonly(t, "an autocommit batch", post(sq(tok, 3, "batch", "statements", stmts("DELETE FROM t"))), true, "none")
	if a := post(ex(tok, 4, "SELECT 1", "min_txn", head+1)); a.code() != "behind" {
		t.Errorf("an execute above the head: %v", a.frames)
	}

	// A transaction reads its snapshot while a writer commits.
	a = post(sq(tok, 4, "begin", "execute", map[string]any{"sql": "SELECT count(*) FROM t"}))
	snap := num(a.done(), "txn")
	if a.done()["txn_state"] != "open" || snap != head || len(a.rows()) != 1 || a.rows()[0] != "[1]" {
		t.Fatalf("begin: %v", a.frames)
	}
	if w := e.post(t, "channel", "writer", ex(wtok, 3, "INSERT INTO t VALUES (2)")); num(w.done(), "txn") != head+1 {
		t.Fatalf("a write while the read is open: %v", w.frames)
	}
	if a := post(ex(tok, 5, "SELECT count(*) FROM t")); a.rows()[0] != "[1]" || num(a.done(), "txn") != snap || a.done()["txn_state"] != "open" {
		t.Errorf("a read in the transaction after a commit: %v", a.frames)
	}
	readonly(t, "a write in the transaction", post(ex(tok, 6, "INSERT INTO t VALUES (3)")), false, "open")
	if a := post(sq(tok, 7, "batch", "statements", stmts("DELETE FROM t"))); num(a.err(), "batch_index") != 0 {
		t.Errorf("a batch in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 8, "describe", "sql", "SELECT a FROM t")); num(a.done(), "txn") != snap {
		t.Errorf("describe in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 9, "ping")); num(a.done(), "txn") != snap || a.done()["txn_state"] != "open" {
		t.Errorf("ping: %v", a.frames)
	}
	if a := post(sq(tok, 10, "commit")); a.done()["committed"] != true || a.done()["wrote"] != false || num(a.done(), "txn") != snap || a.done()["txn_state"] != "none" {
		t.Errorf("commit: %v", a.frames)
	}
	if a := post(sq(tok, 11, "begin")); num(a.done(), "txn") != head+1 {
		t.Errorf("the next begin: %v", a.frames)
	}
	if a := post(sq(tok, 12, "rollback")); a.done()["txn_state"] != "none" {
		t.Errorf("rollback: %v", a.frames)
	}
	if a := post(sq(tok, 13, "describe", "sql", "SELECT a FROM t WHERE a = ?")); num(a.done(), "param_count") != 1 || num(a.done(), "txn") != head+1 {
		t.Errorf("describe: %v", a.frames)
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1,2"]` {
		t.Errorf("rows %s", got)
	}
	// A pooled connection that served the channel takes writes again.
	e.pooled(t, func(db *sqlite3.Conn) error {
		st, _, err := db.Prepare("PRAGMA query_only")
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()
		if !st.Step() || st.ColumnInt(0) != 0 {
			t.Error("a pooled connection is still query_only")
		}
		return st.Err()
	})
	if c := e.post(t, "channel/close", "reader", closeBody(tok)); num(c.done(), "last_seq") != 13 {
		t.Errorf("close: %v", c.frames)
	}
	e.s.pool.mu.Lock()
	busy := len(e.s.pool.busy)
	e.s.pool.mu.Unlock()
	if busy != 0 {
		t.Errorf("%d principals hold connections", busy)
	}
}

// TestReadOnlyTxnLimits: the read-only transaction limits, not the write
// ones, abort an idle transaction and end a running statement; a close
// rolls an open read back.
func TestReadOnlyTxnLimits(t *testing.T) {
	e, _, _ := newTxnEnv(t, func(e *env) {
		e.s.limits.ReadTxnIdleMS, e.s.limits.ReadTxnLifetimeMS, e.s.chl.sweep = 300, 1500, 20*time.Millisecond
	})
	tok := e.openRO(t, "reader")
	post := func(body string) answer { return e.post(t, "channel", "reader", body) }
	post(sq(tok, 1, "begin"))
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if st, open := e.txState(tok); st == wire.TxnAborted && !open {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the idle read-only transaction was not aborted")
		}
	}
	if a := post(sq(tok, 2, "ping")); a.code() != "transaction_aborted" {
		t.Errorf("after the idle limit: %v", a.frames)
	}
	post(sq(tok, 3, "rollback"))
	post(sq(tok, 4, "begin"))
	if a := post(ex(tok, 5, cpuBound)); a.code() != "timeout" || !strings.Contains(fmt.Sprint(a.err()["message"]), "read-only transaction lifetime") || a.err()["txn_state"] != "aborted" {
		t.Errorf("a statement past the lifetime: %v", a.frames)
	}
	post(sq(tok, 6, "rollback"))
	post(sq(tok, 7, "begin"))
	if c := e.post(t, "channel/close", "reader", closeBody(tok)); num(c.done(), "last_seq") != 7 {
		t.Errorf("close: %v", c.frames)
	}
	if _, ok := e.chState(tok); ok {
		t.Error("the channel stayed in the table")
	}
}

// TestReadOnlyHoldsNoWriteLock: a read-only transaction neither takes nor
// releases the write lock that a write transaction holds.
func TestReadOnlyHoldsNoWriteLock(t *testing.T) {
	e, _, wtok := newTxnEnv(t, nil)
	e.post(t, "channel", "writer", sq(wtok, 2, "begin"))
	tok := e.openRO(t, "reader")
	for i, op := range []string{"begin", "commit", "begin", "rollback"} {
		if a := e.post(t, "channel", "reader", sq(tok, int64(i+1), op)); a.done() == nil {
			t.Fatalf("%s: %v", op, a.frames)
		}
	}
	other := e.open(t, "admin")
	if a := e.post(t, "channel", "admin", ex(other, 1, "INSERT INTO other VALUES (1)")); a.code() != "busy" {
		t.Errorf("a write while the write transaction holds the lock: %v", a.frames)
	}
	e.post(t, "channel", "writer", sq(wtok, 3, "commit"))
	e.writes(t)
}

// TestReadOnlyCancelAndRoles: a cancel interrupts a running read; a write
// token on a read-only path is no read-only channel; a read-only channel of
// another principal is lost to it.
func TestReadOnlyCancelAndRoles(t *testing.T) {
	e, _, wtok := newTxnEnv(t, nil)
	tok := e.openRO(t, "reader")
	run := e.postAsync(t, "channel", "reader", ex(tok, 1, cpuBound, "timeout_ms", 10000))
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.running })
	r := e.post(t, "channel/cancel", "reader", cancelBody(tok, 1))
	if _, oe := outcome(r); oe["code"] != "cancelled" || oe["not_executed"] != true {
		t.Errorf("cancel: %d %v", r.status, r.frames)
	}
	if a := <-run; a.code() != "cancelled" {
		t.Errorf("the read: %v", a.frames)
	}
	// The read role is checked on every request; a write channel needs write.
	if a := e.post(t, "channel", "reader", ex(wtok, 2, "SELECT 1")); a.status != http.StatusForbidden {
		t.Errorf("a reader on a write channel: %d %v", a.status, a.frames)
	}
	if a := e.post(t, "channel", "writer", ex(tok, 2, "SELECT 1")); a.code() != "channel_lost" {
		t.Errorf("another principal: %v", a.frames)
	}
	if a := e.post(t, "resolve", "writer", cancelBody(tok, 1)); a.done()["resolution"] != "pending" {
		t.Errorf("resolve of a live read-only channel: %v", a.frames)
	}
}

// TestReadOnlyEngineAbort: when the engine ends an open read on its own, as
// maintenance does, the channel is aborted with cause maintenance.
func TestReadOnlyEngineAbort(t *testing.T) {
	e, _, _ := newTxnEnv(t, nil)
	tok := e.openRO(t, "reader")
	e.post(t, "channel", "reader", sq(tok, 1, "begin"))
	e.s.chMu.Lock()
	ec := e.s.dbs["d"].chans.chans[tok].tx.c.ec
	e.s.chMu.Unlock()
	if err := ec.Close(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if st, open := e.txState(tok); st == wire.TxnAborted && !open {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the channel was not aborted")
		}
	}
	if a := e.post(t, "channel", "reader", sq(tok, 2, "ping")); a.code() != "transaction_aborted" || a.err()["cause"] != "maintenance" {
		t.Errorf("next request: %v", a.frames)
	}
}

// TestForwardReadOnlyChannel: a pod that does not lead forwards a read-only
// open, and the channel's requests, to the leader that owns the channel.
func TestForwardReadOnlyChannel(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	pod := newPod(t, leader, p, nil, false, nil)
	tok := pod.openRO(t, "reader")
	if !strings.HasPrefix(tok, readTokenPrefix+"."+leader.s.instance+".") {
		t.Fatalf("token %q is not the leader's", tok)
	}
	if a := pod.post(t, "channel", "reader", ex(tok, 1, "SELECT count(*) FROM t")); len(a.rows()) != 1 || a.rows()[0] != "[0]" {
		t.Errorf("a read through the pod: %d %v", a.status, a.frames)
	}
	if a := pod.post(t, "channel/close", "reader", closeBody(tok)); num(a.done(), "last_seq") != 1 {
		t.Errorf("close through the pod: %v", a.frames)
	}
}
