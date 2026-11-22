package skylite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

const testChannel = "w1.inst.db.1.secret"

// channels answers opens with a channel of token testChannel, closes and
// cancels with a done frame, and passes every other request to answer.
func channels(answer func(http.ResponseWriter, *wire.Writer, call, *http.Request)) func(http.ResponseWriter, *wire.Writer, call, *http.Request) {
	return func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request) {
		switch c.kind {
		case wire.KindOpen:
			_ = ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{
				Token: testChannel, ID: "c1", Mode: wire.ModeWrite, Epoch: 1, Limits: wire.DefaultLimits(),
			}})
		case wire.KindClose:
			_ = ww.Done(&wire.Done{Kind: wire.DoneClose})
		case wire.KindCancel:
			refuse(w, ww, wire.NewError(wire.CodeChannelLost, "ended"))
		default:
			answer(w, ww, c, r)
		}
	}
}

// execDone is an execute's done frame.
func execDone(c call, state wire.TxnState, txn, rows, id int64) *wire.Done {
	return &wire.Done{Kind: wire.DoneExecute, Seq: c.req.Seq, TxnState: state, Txn: txn, RowsAffected: rows, LastInsertRowID: id}
}

// kinds returns the kinds of calls, and of channel requests their op.
func kinds(calls []call) string {
	var b []string
	for _, c := range calls {
		s := map[wire.Kind]string{
			wire.KindQuery: "query", wire.KindDescribe: "describe", wire.KindOpen: "open", wire.KindCancel: "cancel", wire.KindClose: "close",
		}[c.kind]
		if c.kind == wire.KindChannel {
			s = map[wire.Op]string{wire.OpBegin: "begin", wire.OpExecute: "execute", wire.OpCommit: "commit", wire.OpRollback: "rollback"}[c.req.Op]
		}
		b = append(b, s)
	}
	return strings.Join(b, " ")
}

// waitFor waits until calls of f end with suffix.
func waitFor(t *testing.T, f *fake, suffix string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !strings.HasSuffix(kinds(f.lastCalls()), suffix); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("calls %q; want them to end with %q", kinds(f.lastCalls()), suffix)
		}
	}
}

func TestExecOnAWriteChannel(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		_ = ww.Done(execDone(c, wire.TxnNone, 10+c.req.Seq, 1, 40+c.req.Seq))
	}))
	db := openDB(t, f.config())
	db.SetMaxOpenConns(1)
	for i := int64(1); i <= 2; i++ {
		res, err := db.Exec("INSERT INTO books (title) VALUES (?)", "Dune")
		if err != nil {
			t.Fatal(err)
		}
		ra, _ := res.RowsAffected()
		id, _ := res.LastInsertId()
		if ra != 1 || id != 40+i {
			t.Fatalf("result %d, %d; want 1, %d", ra, id, 40+i)
		}
	}
	_ = db.Close()
	calls := f.lastCalls()
	if got := kinds(calls); got != "open execute execute close" {
		t.Fatalf("calls %q", got)
	}
	if calls[0].body != `{"mode":"write"}` || calls[3].body != `{"channel":"`+testChannel+`"}` {
		t.Fatalf("open %s, close %s", calls[0].body, calls[3].body)
	}
	if want := `{"channel":"` + testChannel + `","seq":2,"op":"execute","sql":"INSERT INTO books (title) VALUES (?)","params":["Dune"],"min_txn":11}`; calls[2].body != want {
		t.Fatalf("second execute %s; want %s", calls[2].body, want)
	}
}

func TestQueryFallsBackToTheChannel(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.kind == wire.KindQuery {
			refuse(w, ww, wire.NewError(wire.CodeNeedsChannel, "not a pure read"))
			return
		}
		_ = ww.Columns(bookColumns[:1])
		_ = ww.Row([]any{int64(7)})
		_ = ww.Row([]any{int64(8)})
		_ = ww.Done(execDone(c, wire.TxnNone, 5, 2, 8))
	}))
	db := openDB(t, f.config())
	rows, err := db.Query("INSERT INTO books (title) VALUES ('a'), ('b') RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil || len(ids) != 2 || ids[0] != 7 || ids[1] != 8 {
		t.Fatalf("ids %v, %v", ids, err)
	}
	if got := kinds(f.lastCalls()); got != "query open execute" {
		t.Fatalf("calls %q", got)
	}
}

func TestResendUsesTheSameSequenceAndBody(t *testing.T) {
	var deliveries atomic.Int32
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch deliveries.Add(1) {
		case 1: // the response is lost
			panic(http.ErrAbortHandler)
		case 2: // the first delivery still runs
			e := wire.NewError(wire.CodeInFlight, "running")
			e.Seq = c.req.Seq
			refuse(w, ww, e)
		default:
			_ = ww.Done(execDone(c, wire.TxnNone, 3, 1, 9))
		}
	}))
	db := openDB(t, f.config())
	res, err := db.Exec("INSERT INTO books (title) VALUES ('Dune')")
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := res.LastInsertId(); id != 9 {
		t.Fatalf("LastInsertId %d", id)
	}
	calls := f.lastCalls()
	if got := kinds(calls); got != "open execute execute execute" {
		t.Fatalf("calls %q", got)
	}
	if calls[1].body != calls[2].body || calls[2].body != calls[3].body || calls[1].req.Seq != 1 {
		t.Fatalf("resent bodies differ: %s, %s, %s", calls[1].body, calls[2].body, calls[3].body)
	}
}

func TestUnknownOutcomes(t *testing.T) {
	for _, code := range []wire.Code{wire.CodeChannelLost, wire.CodeCommitOutcomeUnknown} {
		t.Run(string(code), func(t *testing.T) {
			f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
				if c.req.Seq == 1 && c.body != "" && strings.Contains(c.body, "first") {
					e := wire.NewError(code, "lost")
					e.NotExecuted = false
					refuse(w, ww, e)
					return
				}
				_ = ww.Done(execDone(c, wire.TxnNone, 3, 1, 1))
			}))
			db := openDB(t, f.config())
			db.SetMaxOpenConns(1)
			c, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.ExecContext(context.Background(), "INSERT INTO t VALUES ('first')")
			_ = c.Raw(func(dc any) error {
				if cn := dc.(*conn); cn.IsValid() || !errors.Is(cn.ResetSession(context.Background()), driver.ErrBadConn) {
					t.Error("the connection is still valid after an unknown outcome")
				}
				return nil
			})
			if _, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES (2)"); !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("a discarded connection: %v; want driver.ErrBadConn before anything is sent", err)
			}
			_ = c.Close()
			var se *Error
			if !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, ErrTransactionLost) ||
				!errors.As(err, &se) || se.Code != code || se.ChannelID != "c1" || se.Seq != 1 || se.Epoch != 1 {
				t.Fatalf("err %v (%+v); want ErrOutcomeUnknown with the channel", err, se)
			}
			if strings.Contains(err.Error(), testChannel) {
				t.Fatalf("error %q holds the channel token", err)
			}
			waitFor(t, f, "execute cancel close")
			// The connection was discarded: the next write opens a new channel.
			if _, err := db.Exec("INSERT INTO t VALUES ('second')"); err != nil {
				t.Fatal(err)
			}
			if got := kinds(f.lastCalls()); got != "open execute cancel close open execute" {
				t.Fatalf("calls %q", got)
			}
		})
	}
}

func TestSettledFailureKeepsTheChannel(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.req.Seq == 1 {
			e := wire.NewError(wire.CodeSQLError, "UNIQUE constraint failed").Recorded(false)
			e.Seq, e.SQLiteCode = 1, 19
			refuse(w, ww, e)
			return
		}
		_ = ww.Done(execDone(c, wire.TxnNone, 3, 1, 1))
	}))
	db := openDB(t, f.config())
	db.SetMaxOpenConns(1)
	_, err := db.Exec("INSERT INTO t VALUES (1)")
	var se *Error
	if !errors.As(err, &se) || se.Code != CodeSQLError || !se.NotExecuted || !se.Final || se.SQLiteCode != 19 || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("err %v; want the recorded sql_error", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if calls := f.lastCalls(); kinds(calls) != "open execute execute" || calls[2].req.Seq != 2 {
		t.Fatalf("calls %q", kinds(calls))
	}
}

func TestDeadlineIsAnUnknownOutcome(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, r *http.Request) {
		<-r.Context().Done() // never answers
	}))
	db := openDB(t, f.config())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := db.ExecContext(ctx, "UPDATE t SET a = 1")
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err %v; want ErrOutcomeUnknown and the deadline", err)
	}
	waitFor(t, f, "execute cancel close")
	calls := f.lastCalls()
	if c := calls[2]; c.body != `{"channel":"`+testChannel+`","seq":1}` {
		t.Fatalf("cancel %s", c.body)
	}
	// A context that ended before anything was sent is its own error.
	if _, err := db.ExecContext(ctx, "UPDATE t SET a = 1"); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("err %v; want the deadline alone", err)
	}
}

func TestTransactions(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch c.req.Op {
		case wire.OpBegin:
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 20})
		case wire.OpExecute:
			if strings.HasPrefix(c.req.SQL, "SELECT") {
				_ = ww.Columns(bookColumns[:1])
				_ = ww.Row([]any{int64(1)})
			}
			_ = ww.Done(execDone(c, wire.TxnOpen, 20, 1, 5))
		case wire.OpCommit:
			_ = ww.Done(&wire.Done{Kind: wire.DoneCommit, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 21, Wrote: true})
		case wire.OpRollback:
			_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 21})
		}
	}))
	db := openDB(t, f.config())
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	for _, opts := range []*sql.TxOptions{{ReadOnly: true}, {Isolation: sql.LevelLinearizable}} {
		if _, err := db.BeginTx(ctx, opts); err == nil {
			t.Fatalf("BeginTx(%+v) succeeded", opts)
		}
	}
	if _, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); !errors.Is(err, ErrReadOnlyUnsupported) || !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("read-only BeginTx: %v", err)
	}
	if n := f.hits.Load(); n != 0 {
		t.Fatalf("refused BeginTx sent %d requests", n)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := tx.QueryRow("SELECT id FROM t").Scan(&id); err != nil || id != 1 {
		t.Fatalf("query in the transaction: %d, %v", id, err)
	}
	st, err := tx.Prepare("SELECT id FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.QueryRow(1).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	calls := f.lastCalls()
	if got := kinds(calls); got != "open begin execute execute execute commit begin rollback" {
		t.Fatalf("calls %q", got)
	}
	if calls[1].req.MinTxn != 1 || calls[2].req.MinTxn != 0 || calls[6].req.MinTxn != 21 {
		t.Fatalf("min_txn of begin %d, execute %d, second begin %d; want 1, 0, 21", calls[1].req.MinTxn, calls[2].req.MinTxn, calls[6].req.MinTxn)
	}
}

func TestTransactionLost(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch c.req.Op {
		case wire.OpBegin:
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 2})
		default:
			refuse(w, ww, wire.NewError(wire.CodeChannelLost, "the owner restarted"))
		}
	}))
	db := openDB(t, f.config())
	db.SetMaxOpenConns(1)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrTransactionLost) {
		t.Fatalf("statement: %v; want ErrOutcomeUnknown and ErrTransactionLost", err)
	}
	waitFor(t, f, "execute cancel close")
	before := f.hits.Load()
	if _, err := tx.Exec("INSERT INTO t VALUES (2)"); !errors.Is(err, ErrTransactionLost) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("next statement: %v; want ErrTransactionLost", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTransactionLost) {
		t.Fatalf("commit: %v; want ErrTransactionLost", err)
	}
	if n := f.hits.Load(); n != before {
		t.Fatalf("a lost transaction sent %d requests", n-before)
	}
	tx, err = db.Begin() // on a new connection and channel
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := kinds(f.lastCalls()); got != "open begin execute cancel close open begin rollback" {
		t.Fatalf("calls %q", got)
	}
}

func TestLostRollbackSucceeds(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.req.Op == wire.OpBegin {
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 2})
			return
		}
		refuse(w, ww, wire.NewError(wire.CodeChannelLost, "ended"))
	}))
	db := openDB(t, f.config())
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback on a lost channel: %v", err)
	}
	waitFor(t, f, "rollback cancel close")
	// A transaction already lost rolls back without sending anything.
	if tx, err = db.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); !errors.Is(err, ErrTransactionLost) {
		t.Fatalf("statement: %v", err)
	}
	waitFor(t, f, "execute cancel close")
	before := f.hits.Load()
	if err := tx.Rollback(); err != nil || f.hits.Load() != before {
		t.Fatalf("rollback of a lost transaction: %v, %d requests", err, f.hits.Load()-before)
	}
}

func TestReplayWithoutItsRows(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.kind == wire.KindQuery {
			refuse(w, ww, wire.NewError(wire.CodeNeedsChannel, "not a pure read"))
			return
		}
		d := execDone(c, wire.TxnNone, 4, 1, 1)
		d.DataOmitted = true
		_ = ww.Done(d)
	}))
	db := openDB(t, f.config())
	_, err := db.Query("INSERT INTO t VALUES (1) RETURNING id")
	var se *Error
	if !errors.Is(err, ErrResultUnavailable) || !errors.As(err, &se) || !se.Committed() || se.Txn != 4 {
		t.Fatalf("err %v; want ErrResultUnavailable, committed at 4", err)
	}
}

func TestIdleChannelIsReplaced(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		_ = ww.Done(execDone(c, wire.TxnNone, 3, 1, 1))
	}))
	db := openDB(t, f.config())
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.ExecContext(context.Background(), "DELETE FROM t"); err != nil {
		t.Fatal(err)
	}
	_ = c.Raw(func(dc any) error { dc.(*conn).ch.last = time.Now().Add(-channelIdle - time.Second); return nil })
	if _, err := c.ExecContext(context.Background(), "DELETE FROM t"); err != nil {
		t.Fatal(err)
	}
	if got := kinds(f.lastCalls()); got != "open execute close open execute" {
		t.Fatalf("calls %q", got)
	}
}
