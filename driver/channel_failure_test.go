package skylite

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// roundTripper answers a connection's requests in memory, so that a test
// can run in a testing/synctest bubble.
type roundTripper func(*http.Request) (*http.Response, error)

func (rt roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return rt(r) }

// channelConn returns a connection on rt with an open write channel.
func channelConn(t *testing.T, rt roundTripper) *conn {
	t.Helper()
	c, err := NewConnector(Config{
		Endpoint: "https://example.invalid", Database: "review",
		Token:      TokenFunc(func(context.Context) (string, error) { return testToken, nil }),
		HTTPClient: &http.Client{Transport: rt},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &conn{c: c, ch: &channel{token: testChannel, id: testChannelID, epoch: 1, last: time.Now(), state: wire.TxnNone, limits: wire.DefaultLimits()}}
}

// reply is a response whose body is data.
func reply(data string) (*http.Response, error) {
	return &http.Response{
		StatusCode: 200, Header: http.Header{"Skylite-Protocol": {"1"}},
		Body: io.NopCloser(strings.NewReader(data + "\n")),
	}, nil
}

func TestResendDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deliveries := 0
		cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/channel") {
				deliveries++
				if deliveries > 1 {
					select {
					case <-r.Context().Done():
						return nil, r.Context().Err()
					case <-time.After(2 * resendWindow):
					}
				}
			}
			return nil, io.ErrUnexpectedEOF
		})
		start := time.Now()
		_, err := cn.ExecContext(context.Background(), "insert into t values(1)", nil)
		if elapsed := time.Since(start); elapsed > resendWindow || !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("after %d deliveries took %s, err %v", deliveries, elapsed, err)
		}
		time.Sleep(abandonWait) // the cancel and close, in the background
	})
}

func TestCloseAfterSlowCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var closeLive bool
		cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/cancel") {
				<-r.Context().Done()
			} else if strings.HasSuffix(r.URL.Path, "/close") {
				closeLive = r.Context().Err() == nil
			}
			return nil, io.ErrUnexpectedEOF
		})
		cn.c.abandon(testChannel, 1)
		if !closeLive {
			t.Fatal("slow cancel left close without a live context")
		}
	})
}

func TestIdleCleanupCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
		cn.ch.last = time.Now().Add(-2 * channelIdle)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := cn.ExecContext(ctx, "insert into t values(1)", nil)
		if elapsed := time.Since(start); elapsed > 2*time.Millisecond || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("1ms deadline returned after %s with %v", elapsed, err)
		}
		time.Sleep(abandonWait) // the old channel's close, in the background
	})
}

func TestWrongReceipt(t *testing.T) {
	tests := []struct{ name, receipt string }{
		{"missing_seq", `{"error":{"code":"sql_error","not_executed":true,"final":true,"message":"failed","request_id":"id","channel_id":"0123456789abcdef0123456789abcdef","txn_state":"none"}}`},
		{"other_channel", `{"error":{"code":"sql_error","not_executed":true,"final":true,"message":"failed","request_id":"id","channel_id":"0123456789abcdef0123456789abcdee","seq":1,"txn_state":"none"}}`},
		{"autocommit_left_open", `{"done":{"seq":1,"txn_state":"open","txn":1,"read_only":false,"last_insert_rowid":1,"rows_affected":1}}`},
		{"failure_left_open", `{"error":{"code":"sql_error","not_executed":true,"final":true,"message":"failed","request_id":"id","channel_id":"0123456789abcdef0123456789abcdef","seq":1,"txn_state":"open"}}`},
		{"final_unknown", `{"error":{"code":"commit_outcome_unknown","not_executed":false,"final":true,"message":"unknown","request_id":"id","channel_id":"0123456789abcdef0123456789abcdef","seq":1,"txn_state":"none"}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			cn := channelConn(t, func(r *http.Request) (*http.Response, error) { return reply(tc.receipt) })
			_, err := cn.ExecContext(ctx, "insert into t values(1)", nil)
			if !errors.Is(err, ErrOutcomeUnknown) || cn.IsValid() {
				t.Fatalf("accepted %s: err %v, reusable %t", tc.name, err, cn.IsValid())
			}
		})
	}
}

func TestLostPrepare(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.req.Op == wire.OpBegin {
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			return
		}
		refuse(w, ww, wire.NewError(wire.CodeChannelLost, "ended"))
	}))
	db := openDB(t, f.config())
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.Exec("insert into t values(1)"); !errors.Is(err, ErrTransactionLost) {
		t.Fatal(err)
	}
	if _, err = tx.Prepare("select 1"); !errors.Is(err, ErrTransactionLost) {
		t.Errorf("lost tx.Prepare returned %v", err)
	}
	waitFor(t, f, "cancel close")
}

func TestInvalidIsolation(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.req.Op == wire.OpRollback {
			_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 1})
			return
		}
		_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
	}))
	db := openDB(t, f.config())
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: -1})
	if err == nil {
		t.Errorf("accepted isolation -1, calls %s", kinds(f.lastCalls()))
		_ = tx.Rollback()
	}
}

func TestReadOnlyReplay(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch c.req.Op {
		case wire.OpBegin:
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
		case wire.OpExecute:
			d := execDone(c, wire.TxnOpen, 1, 0, 0)
			d.ReadOnly, d.DataOmitted = true, c.req.Seq == 2
			if !d.DataOmitted {
				_ = ww.Columns(bookColumns[:1])
				_ = ww.Row([]any{int64(1)})
			}
			_ = ww.Done(d)
		case wire.OpRollback:
			_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 1})
		}
	}))
	db := openDB(t, f.config())
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var id int
	if err := tx.QueryRow("select 1").Scan(&id); err != nil || id != 1 {
		t.Fatalf("read-only replay: id %d, err %v, calls %s", id, err, kinds(f.lastCalls()))
	}
}

func TestWrongDoneSequenceRecovery(t *testing.T) {
	var attempts atomic.Int32
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		d := execDone(c, wire.TxnNone, 1, 1, 17)
		if attempts.Add(1) == 1 {
			d.Seq++
			d.LastInsertRowID = 99
		}
		_ = ww.Done(d)
	}))
	db := openDB(t, f.config())
	res, err := db.Exec("insert into t values (1)")
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	reqs := f.lastCalls()
	if err != nil || id != 17 || len(reqs) != 3 || reqs[1].body != reqs[2].body {
		t.Fatalf("id %d, err %v, calls %s", id, err, kinds(reqs))
	}
}

func TestOutstandingRows(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch c.req.Op {
		case wire.OpBegin:
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
		case wire.OpExecute:
			if c.req.Seq == 2 {
				_ = ww.Columns(bookColumns[:1])
				for _, n := range []int64{11, 12} {
					_ = ww.Row([]any{n})
				}
			}
			_ = ww.Done(execDone(c, wire.TxnOpen, 1, 1, 0))
		case wire.OpRollback:
			_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 1})
		}
	}))
	db := openDB(t, f.config())
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query("select id from t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	updated := make(chan error, 1)
	go func() { _, err := tx.Exec("update t set id = 20"); updated <- err }()
	for _, want := range []int{11, 12} {
		var got int
		if !rows.Next() {
			t.Fatalf("row missing: %v", rows.Err())
		}
		if err := rows.Scan(&got); err != nil || got != want {
			t.Fatalf("want %d, got %d, err %v", want, got, err)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatal("rows did not end cleanly")
	}
	if err := <-updated; err != nil {
		t.Fatal(err)
	}
}

func TestResultBudget(t *testing.T) {
	limits := wire.DefaultLimits()
	limits.ResultBytes = 64
	var open bytes.Buffer
	ww, err := wire.NewWriter(&open, wire.DefaultFrameBytes, wire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{
		Token: testChannel, ID: "0123456789abcdef0123456789abcdef", Epoch: 1, Mode: wire.ModeWrite, Limits: limits,
	}}); err != nil {
		t.Fatal(err)
	}
	columns := `{"columns":[{"name":"id"}]}` + "\n"
	rowData := strings.Repeat("[1]\n", 20)
	done := `{"done":{"seq":1,"txn_state":"none","txn":1,"read_only":false,"last_insert_rowid":1,"rows_affected":20}}`
	cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/channels") {
			return reply(strings.TrimSuffix(open.String(), "\n"))
		}
		return reply(columns + rowData + done)
	})
	cn.ch = nil
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result, err := cn.channelQuery(ctx, "insert into t select * from u returning id", nil)
	if err == nil {
		t.Fatalf("accepted %d bytes of columns and rows with result_bytes=64, retained %d rows", len(columns)+len(rowData), len(result.(*rows).buf))
	}
}

func TestCancelledBeforeFirstDelivery(t *testing.T) {
	f := newFake(t, true, channels(func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		_ = ww.Done(execDone(c, wire.TxnNone, 1, 1, 1))
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := f.config()
	var tokens atomic.Int32
	cfg.Token = TokenFunc(func(context.Context) (string, error) {
		if tokens.Add(1) == 2 {
			cancel()
		} // Open succeeded. The execute request does not exist yet.
		return testToken, nil
	})
	db := openDB(t, cfg)
	_, err := db.ExecContext(ctx, "insert into t values(1)")
	if errors.Is(err, ErrOutcomeUnknown) {
		waitFor(t, f, "cancel close")
		t.Fatalf("context ended before execute was built, calls %s, err %v", kinds(f.lastCalls()), err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestFinalFlags(t *testing.T) {
	for _, op := range []wire.Op{wire.OpBegin, wire.OpExecute, wire.OpCommit} {
		t.Run(map[wire.Op]string{wire.OpBegin: "begin", wire.OpExecute: "execute", wire.OpCommit: "commit"}[op], func(t *testing.T) {
			f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
				switch c.req.Op {
				case op:
					e := wire.NewError(wire.CodeSQLError, "failed").Recorded(op == wire.OpExecute)
					e.Seq, e.ChannelID, e.TxnState = c.req.Seq, testChannelID, wire.TxnNone
					if op == wire.OpExecute {
						e.TxnState = wire.TxnOpen
					}
					refuse(w, ww, e)
				case wire.OpBegin:
					_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
				default:
					_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 1})
				}
			}))
			db := openDB(t, f.config())
			tx, err := db.Begin()
			if op != wire.OpBegin {
				if err != nil {
					t.Fatal(err)
				}
				if op == wire.OpCommit {
					err = tx.Commit()
				} else {
					_, err = tx.Exec("insert into t values(1)")
					defer func() { _ = tx.Rollback() }()
				}
			}
			var se *Error
			if !errors.As(err, &se) || !se.Final || se.NotExecuted != (op != wire.OpExecute) || errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, driver.ErrBadConn) {
				t.Fatalf("unexpected flags: %+v", se)
			}
		})
	}
}

func TestRefusalWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sent []time.Time
		var baseline string
		cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/channel") {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				if len(sent) == 0 {
					baseline = string(body)
				} else if string(body) != baseline {
					t.Error("resend changed body")
				}
				sent = append(sent, time.Now())
			}
			return nil, io.ErrUnexpectedEOF
		})
		start := time.Now()
		_, err := cn.ExecContext(context.Background(), "insert into t values (1)", nil)
		if !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, driver.ErrBadConn) || cn.IsValid() {
			t.Fatalf("exhaustion: %v, reusable %t", err, cn.IsValid())
		}
		if elapsed := time.Since(start); elapsed < resendWindow-2*time.Second || elapsed > resendWindow || len(sent) < 2 {
			t.Fatalf("elapsed %s, deliveries %d", elapsed, len(sent))
		}
		want := 100 * time.Millisecond
		for i := 1; i < len(sent); i++ {
			if gap := sent[i].Sub(sent[i-1]); gap != want {
				t.Errorf("gap %d = %s, want %s", i, gap, want)
			}
			want = min(2*want, 2*time.Second)
		}
	})
}

func TestAdvertisedRequestLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := wire.DefaultLimits()
		limits.RequestBodyBytes = 256
		var opening bytes.Buffer
		ww, err := wire.NewWriter(&opening, wire.DefaultFrameBytes, wire.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if err = ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{Token: testChannel, ID: "0123456789abcdef0123456789abcdef", Epoch: 1, Mode: wire.ModeWrite, Limits: limits}}); err != nil {
			t.Fatal(err)
		}
		attempts := 0
		cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/channels") {
				return reply(strings.TrimSuffix(opening.String(), "\n"))
			}
			if strings.HasSuffix(r.URL.Path, "/channel") {
				attempts++
				if r.ContentLength <= limits.RequestBodyBytes {
					t.Fatalf("fixture body only %d bytes", r.ContentLength)
				}
			}
			return reply(`{"error":{"code":"request_too_large","not_executed":true,"final":false,"message":"over 256 bytes","request_id":"id"}}`)
		})
		cn.ch = nil
		_, err = cn.ExecContext(context.Background(), "insert into t values (?)", []driver.NamedValue{{Ordinal: 1, Value: strings.Repeat("a", 320)}})
		if err == nil || errors.Is(err, ErrOutcomeUnknown) || attempts > 0 {
			t.Fatalf("sent %d oversized deliveries, err %v", attempts, err)
		}
	})
}

// TestFailedEndDiscards ends a transaction whose commit or rollback was
// never sent, or whose commit found the transaction aborted: the channel is
// closed and the connection discarded, so that the next borrower opens a
// new channel instead of running inside the old transaction.
func TestFailedEndDiscards(t *testing.T) {
	unavailable := errors.New("token refresh failed")
	aborted := func(err error) bool {
		var se *Error
		return errors.As(err, &se) && se.Code == CodeTransactionAborted
	}
	for _, tc := range []struct {
		name, want string
		reject     bool
		end        func(*sql.Tx) error
		failed     func(error) bool
	}{
		{"commit token", "open begin execute close open execute", true, (*sql.Tx).Commit, func(err error) bool { return errors.Is(err, unavailable) }},
		{"rollback token", "open begin execute close open execute", true, (*sql.Tx).Rollback, func(err error) bool { return errors.Is(err, unavailable) }},
		{"aborted commit", "open begin execute commit close open execute", false, (*sql.Tx).Commit, aborted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
				switch c.req.Op {
				case wire.OpBegin:
					_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
				case wire.OpCommit:
					e := wire.NewError(wire.CodeTransactionAborted, "rolled back").Recorded(true)
					e.Seq, e.ChannelID, e.TxnState = c.req.Seq, testChannelID, wire.TxnAborted
					refuse(w, ww, e)
				default:
					state := wire.TxnOpen
					if c.req.MinTxn > 0 { // an autocommit write
						state = wire.TxnNone
					}
					_ = ww.Done(execDone(c, state, 1, 1, 1))
				}
			}))
			var reject atomic.Bool
			cfg := f.config()
			cfg.Token = TokenFunc(func(context.Context) (string, error) {
				if reject.Swap(false) {
					return "", unavailable
				}
				return testToken, nil
			})
			db := openDB(t, cfg)
			db.SetMaxOpenConns(1)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
				t.Fatal(err)
			}
			reject.Store(tc.reject)
			if err := tc.end(tx); !tc.failed(err) {
				t.Fatalf("end: %v; want the original failure", err)
			}
			if _, err := db.Exec("INSERT INTO t VALUES (2)"); err != nil {
				t.Fatal(err)
			}
			if got := kinds(f.lastCalls()); got != tc.want {
				t.Fatalf("calls %q; want %q", got, tc.want)
			}
		})
	}
}

// TestBehindIsSentOnANewChannel refuses a begin with behind: the driver
// closes the channel and, only when the close's answer proves the begin was
// never accepted, sends it again on a new channel. A close showing the begin
// accepted leaves it on its channel.
func TestBehindIsSentOnANewChannel(t *testing.T) {
	for lastSeq, want := range map[int64]string{0: "open begin close open begin rollback", 1: "open begin close begin rollback"} {
		var begins atomic.Int32
		inner := channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
			switch c.req.Op {
			case wire.OpBegin:
				if begins.Add(1) == 1 {
					refuse(w, ww, wire.NewError(wire.CodeBehind, "below min_txn"))
					return
				}
				_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			case wire.OpRollback:
				_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 1})
			}
		})
		f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request) {
			if c.kind == wire.KindClose && lastSeq > 0 {
				_ = ww.Done(&wire.Done{Kind: wire.DoneClose, LastSeq: lastSeq, Outcome: &wire.Receipt{
					Done: &wire.Done{Kind: wire.DoneBegin, Seq: lastSeq, TxnState: wire.TxnOpen, Txn: 1},
				}})
				return
			}
			inner(w, ww, c, r)
		})
		db := openDB(t, f.config())
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if got := kinds(f.lastCalls()); got != want {
			t.Errorf("last_seq %d: calls %q; want %q", lastSeq, got, want)
		}
	}
}

// TestExecDropsRows reads an answer's rows without keeping them when the
// caller does not want them.
func TestExecDropsRows(t *testing.T) {
	cn := channelConn(t, func(*http.Request) (*http.Response, error) {
		return reply(`{"columns":[{"name":"id"}]}` + "\n[1]\n[2]\n" + `{"done":{"seq":1,"txn_state":"none","txn":1,"read_only":false,"last_insert_rowid":2,"rows_affected":2}}`)
	})
	a, err := cn.do(context.Background(), time.Now().Add(resendWindow), cn.execute("INSERT INTO t VALUES (1), (2) RETURNING id", nil), wire.DoneExecute, false)
	if err != nil || len(a.rows) != 0 || a.done.RowsAffected != 2 {
		t.Fatalf("answer %+v, err %v; want no rows kept", a, err)
	}
}

// TestMaintenanceIsRetryable checks the error of a transaction that
// maintenance ended, and of one SQLite rolled back.
func TestMaintenanceIsRetryable(t *testing.T) {
	for cause, retryable := range map[string]bool{"maintenance": true, "": false} {
		f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
			switch c.req.Op {
			case wire.OpBegin:
				_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			case wire.OpExecute:
				e := wire.NewError(wire.CodeTransactionAborted, "ended").Recorded(true)
				e.Seq, e.ChannelID, e.TxnState, e.Cause = c.req.Seq, testChannelID, wire.TxnAborted, cause
				refuse(w, ww, e)
			case wire.OpRollback:
				_ = ww.Done(&wire.Done{Kind: wire.DoneRollback, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 1})
			}
		}))
		db := openDB(t, f.config())
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec("INSERT INTO t VALUES (1)")
		var se *Error
		if !errors.As(err, &se) || se.Cause != cause || se.Retryable() != retryable {
			t.Errorf("cause %q: %v (%+v)", cause, err, se)
		}
		_ = tx.Rollback()
	}
}

// TestOpenAnswerNeedsAWriteChannel refuses an open answer without a write
// channel's mode and epoch before anything is sent on it.
func TestOpenAnswerNeedsAWriteChannel(t *testing.T) {
	f := newFake(t, true, func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		if c.kind == wire.KindOpen {
			_ = ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{
				Token: testChannel, ID: testChannelID, Mode: wire.ModeReadOnly, Limits: wire.DefaultLimits(),
			}})
		}
	})
	db := openDB(t, f.config())
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err == nil || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("err %v", err)
	}
	if got := kinds(f.lastCalls()); got != "open" {
		t.Fatalf("calls %q", got)
	}
}

// TestChannelRotatesBelowTheSequenceBound replaces an idle channel whose
// next sequence number would pass 2^62.
func TestChannelRotatesBelowTheSequenceBound(t *testing.T) {
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
	_ = c.Raw(func(dc any) error { dc.(*conn).ch.seq = 1 << 62; return nil })
	if _, err := c.ExecContext(context.Background(), "DELETE FROM t"); err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, call := range f.lastCalls() {
		if call.kind == wire.KindChannel {
			seqs = append(seqs, call.req.Seq)
		}
	}
	if len(seqs) != 2 || seqs[1] != 1 || strings.Count(kinds(f.lastCalls()), "open") != 2 {
		t.Fatalf("calls %q, seqs %v", kinds(f.lastCalls()), seqs)
	}
}

// TestTransactionReplayAndUnknownCommit checks that a write replayed
// without its rows inside a transaction is not reported committed, and
// that an unknown commit is not reported as a lost statement.
func TestTransactionReplayAndUnknownCommit(t *testing.T) {
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch c.req.Op {
		case wire.OpBegin:
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 1})
		case wire.OpExecute:
			d := execDone(c, wire.TxnOpen, 1, 1, 1)
			d.DataOmitted = true
			_ = ww.Done(d)
		case wire.OpCommit:
			e := wire.NewError(wire.CodeCommitOutcomeUnknown, "not definite")
			e.Seq, e.ChannelID, e.TxnState = c.req.Seq, testChannelID, wire.TxnNone
			refuse(w, ww, e)
		}
	}))
	db := openDB(t, f.config())
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Query("INSERT INTO t VALUES (1) RETURNING id")
	var se *Error
	if !errors.Is(err, ErrResultUnavailable) || !errors.As(err, &se) || se.Committed() {
		t.Fatalf("replay in a transaction: %v; want ErrResultUnavailable, not committed", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, ErrTransactionLost) {
		t.Fatalf("commit: %v; want ErrOutcomeUnknown only", err)
	}
}
