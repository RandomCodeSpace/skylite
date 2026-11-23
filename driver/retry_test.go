package skylite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// errDrop makes script's roundTripper fail the delivery, as a lost response.
var errDrop = errors.New("dropped")

// sent is one request a script answered.
type sent struct {
	kind wire.Kind
	req  *wire.Request
	at   time.Time
}

// script answers a connection's requests in memory with answer, given each
// request parsed, and records them.
type script struct {
	mu    sync.Mutex
	calls []sent
}

func (s *script) rt(t *testing.T, answer func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error) roundTripper {
	return func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		kind, _, rerr := wire.Route(r.Method, r.URL.Path)
		if rerr != nil {
			t.Errorf("route %s: %v", r.URL.Path, rerr)
			return nil, io.ErrUnexpectedEOF
		}
		req, perr := wire.Parse(kind, body, 10000)
		if perr != nil {
			t.Errorf("parse %s: %v", body, perr)
			return nil, io.ErrUnexpectedEOF
		}
		s.mu.Lock()
		s.calls = append(s.calls, sent{kind, req, time.Now()})
		s.mu.Unlock()
		var dropped bool
		resp, err := frames(func(ww *wire.Writer) error {
			err := answer(kind, req, ww)
			dropped = errors.Is(err, errDrop)
			if dropped {
				return nil
			}
			return err
		})
		if dropped {
			return nil, io.ErrUnexpectedEOF
		}
		return resp, err
	}
}

// kinds names the requests sent, as the package's kinds does.
func (s *script) kinds(of ...wire.Kind) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b []string
	for _, c := range s.calls {
		if len(of) > 0 && c.kind != of[0] {
			continue
		}
		b = append(b, kinds([]call{{kind: c.kind, req: c.req}}))
	}
	return strings.Join(b, " ")
}

func (s *script) all() []sent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sent(nil), s.calls...)
}

// refusal is an error refused before acceptance.
func refusal(code wire.Code) *wire.Error {
	e := wire.NewError(code, string(code))
	e.RequestID = "id"
	return e
}

// recorded is the recorded failure of req.
func recorded(code wire.Code, req *wire.Request, txnStatement bool, state wire.TxnState) *wire.Error {
	e := wire.NewError(code, string(code)).Recorded(txnStatement)
	e.RequestID, e.ChannelID, e.Seq, e.TxnState = "id", testChannelID, req.Seq, state
	return e
}

// lost is a running request's channel_lost.
func lost() *wire.Error {
	e := refusal(wire.CodeChannelLost)
	e.NotExecuted = false
	return e
}

// opened answers an open with a write channel of token.
func opened(ww *wire.Writer, token string) error {
	return ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{
		Token: token, ID: testChannelID, Mode: wire.ModeWrite, Epoch: 2, Limits: wire.DefaultLimits(),
	}})
}

func resolved(ww *wire.Writer, res wire.Resolution, txn int64) error {
	return ww.Done(&wire.Done{Kind: wire.DoneResolve, Resolution: res, Txn: txn})
}

func executed(ww *wire.Writer, req *wire.Request, state wire.TxnState) error {
	return ww.Done(&wire.Done{Kind: wire.DoneExecute, Seq: req.Seq, TxnState: state, Txn: 3, RowsAffected: 1, LastInsertRowID: 7})
}

// TestResolveAfterALostWrite loses an autocommit write with its channel and
// checks each answer of resolve: only committed is reported committed, only
// not_committed lets the driver send the write again, once, and everything
// else is an unknown outcome.
func TestResolveAfterALostWrite(t *testing.T) {
	const second = "w1.inst.db.2.second"
	tests := []struct {
		name    string
		code    wire.Code // the write's answer on the first channel
		resolve []any     // the resolve answers in turn: a Resolution or a refusal code; the last repeats
		lostTwo bool      // the write is lost on the second channel too
		check   func(t *testing.T, err error, cn *conn, s *script)
	}{
		{
			"committed after pending and no_leader", wire.CodeChannelLost,
			[]any{wire.ResolutionPending, wire.CodeNoLeader, wire.ResolutionCommitted},
			false,
			func(t *testing.T, err error, cn *conn, s *script) {
				var se *Error
				if !errors.Is(err, ErrResultUnavailable) || !errors.As(err, &se) || !se.Committed() || se.Txn != 9 || errors.Is(err, ErrOutcomeUnknown) {
					t.Fatalf("err %v; want committed, result unavailable, at 9", err)
				}
				if !cn.IsValid() || cn.ch != nil || cn.c.watermark.Load() != 9 {
					t.Fatalf("valid %t, channel %v, watermark %d", cn.IsValid(), cn.ch, cn.c.watermark.Load())
				}
				if got := s.kinds(); got != "execute resolve resolve resolve" {
					t.Fatalf("calls %q", got)
				}
			},
		},
		{
			"commit_outcome_unknown, committed", wire.CodeCommitOutcomeUnknown,
			[]any{wire.ResolutionCommitted},
			false,
			func(t *testing.T, err error, cn *conn, s *script) {
				var se *Error
				if !errors.As(err, &se) || !se.Committed() || errors.Is(err, ErrOutcomeUnknown) {
					t.Fatalf("err %v; want committed", err)
				}
			},
		},
		{
			"not committed: sent again on a new channel", wire.CodeChannelLost,
			[]any{wire.ResolutionNotCommitted},
			false,
			func(t *testing.T, err error, cn *conn, s *script) {
				calls := s.all()
				if err != nil || s.kinds() != "execute resolve open execute" || calls[3].req.Channel != second || calls[3].req.Seq != 1 {
					t.Fatalf("err %v, calls %q", err, s.kinds())
				}
				if calls[1].req.Channel != testChannel || calls[1].req.Seq != 1 || !cn.IsValid() {
					t.Fatalf("resolve of %s %d, valid %t", calls[1].req.Channel, calls[1].req.Seq, cn.IsValid())
				}
			},
		},
		{
			"not committed twice", wire.CodeChannelLost,
			[]any{wire.ResolutionNotCommitted},
			true,
			func(t *testing.T, err error, cn *conn, s *script) {
				var se *Error
				if !errors.As(err, &se) || !se.NotExecuted || !se.Final || errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, ErrResultUnavailable) {
					t.Fatalf("err %v; want not executed", err)
				}
				if got := s.kinds(); got != "execute resolve open execute resolve" || !cn.IsValid() {
					t.Fatalf("calls %q, valid %t", got, cn.IsValid())
				}
			},
		},
		{
			"unknown", wire.CodeChannelLost,
			[]any{wire.ResolutionUnknown},
			false,
			func(t *testing.T, err error, cn *conn, s *script) {
				if !errors.Is(err, ErrOutcomeUnknown) || cn.IsValid() || !strings.Contains(err.Error(), "resolve answered unknown") {
					t.Fatalf("err %v, valid %t; want unknown", err, cn.IsValid())
				}
			},
		},
		{
			"a refusal that does not pass", wire.CodeChannelLost,
			[]any{wire.CodeForbidden},
			false,
			func(t *testing.T, err error, cn *conn, s *script) {
				if !errors.Is(err, ErrOutcomeUnknown) || s.kinds(wire.KindResolve) != "resolve" {
					t.Fatalf("err %v, calls %q; want unknown after one resolve", err, s.kinds())
				}
			},
		},
		{
			"pending until the wait ends", wire.CodeChannelLost,
			[]any{wire.ResolutionPending},
			false,
			func(t *testing.T, err error, cn *conn, s *script) {
				calls := s.all()
				if took := calls[len(calls)-1].at.Sub(calls[0].at); !errors.Is(err, ErrOutcomeUnknown) || took > resolveWait || took < resolveWait-2*time.Second {
					t.Fatalf("err %v after resolving for %s; want unknown after the resolve wait", err, took)
				}
				if !strings.Contains(err.Error(), "resolve answered pending") || errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("err %v", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := &script{}
				resolves := 0
				cn := channelConn(t, s.rt(t, func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
					switch kind {
					case wire.KindOpen:
						return opened(ww, second)
					case wire.KindResolve:
						a := tc.resolve[min(resolves, len(tc.resolve)-1)]
						resolves++
						if code, ok := a.(wire.Code); ok {
							return ww.Error(refusal(code))
						}
						return resolved(ww, a.(wire.Resolution), 9)
					case wire.KindChannel:
						if req.Channel == testChannel {
							e := refusal(tc.code)
							e.NotExecuted = false
							return ww.Error(e)
						}
						if tc.lostTwo {
							return ww.Error(lost())
						}
						return executed(ww, req, wire.TxnNone)
					}
					return ww.Done(&wire.Done{Kind: wire.DoneClose})
				}))
				_, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil)
				tc.check(t, err, cn, s)
				time.Sleep(abandonWait) // background cancels and closes
			})
		})
	}
}

// TestNoResolveForAStatementOrACancelledCaller checks that a statement
// inside a transaction, which never commits by itself, and a request whose
// caller's context ended, are not resolved.
func TestNoResolveForAStatementOrACancelledCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
			switch {
			case kind == wire.KindChannel && req.Op == wire.OpBegin:
				return ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			case kind == wire.KindChannel:
				return ww.Error(lost())
			case kind == wire.KindResolve:
				return resolved(ww, wire.ResolutionCommitted, 2)
			}
			return ww.Done(&wire.Done{Kind: wire.DoneClose})
		}))
		if _, err := cn.BeginTx(context.Background(), driver.TxOptions{}); err != nil {
			t.Fatal(err)
		}
		_, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil)
		if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrTransactionLost) || s.kinds(wire.KindResolve) != "" {
			t.Fatalf("err %v, calls %q; want a lost transaction, unresolved", err, s.kinds())
		}
		time.Sleep(abandonWait)
	})
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		ctx, cancel := context.WithCancel(context.Background())
		cn := channelConn(t, s.rt(t, func(kind wire.Kind, _ *wire.Request, ww *wire.Writer) error {
			if kind == wire.KindChannel {
				cancel()
				return ww.Error(lost())
			}
			return resolved(ww, wire.ResolutionCommitted, 2)
		}))
		_, err := cn.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
		if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, context.Canceled) || s.kinds(wire.KindResolve) != "" {
			t.Fatalf("err %v, calls %q; want unknown and the context's error, unresolved", err, s.kinds())
		}
		time.Sleep(abandonWait)
	})
}

// TestResolveACommit loses a commit and checks that resolve decides what
// Commit reports, and that the connection is discarded whatever it says.
func TestResolveACommit(t *testing.T) {
	tests := []struct {
		res   wire.Resolution
		check func(error) bool
	}{
		{wire.ResolutionCommitted, func(err error) bool { return err == nil }},
		{wire.ResolutionNotCommitted, func(err error) bool {
			var se *Error
			return errors.Is(err, ErrTransactionLost) && errors.As(err, &se) && se.NotExecuted && !errors.Is(err, ErrOutcomeUnknown)
		}},
		{wire.ResolutionUnknown, func(err error) bool { return errors.Is(err, ErrOutcomeUnknown) && !errors.Is(err, ErrTransactionLost) }},
	}
	for _, tc := range tests {
		t.Run(string(tc.res), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := &script{}
				cn := channelConn(t, s.rt(t, func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
					switch {
					case kind == wire.KindChannel && req.Op == wire.OpBegin:
						return ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: req.Seq, TxnState: wire.TxnOpen, Txn: 1})
					case kind == wire.KindChannel:
						e := refusal(wire.CodeCommitOutcomeUnknown)
						e.Final, e.Seq, e.ChannelID = true, req.Seq, testChannelID
						return ww.Error(e)
					case kind == wire.KindResolve:
						return resolved(ww, tc.res, 4)
					}
					return ww.Done(&wire.Done{Kind: wire.DoneClose})
				}))
				tx, err := cn.BeginTx(context.Background(), driver.TxOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); !tc.check(err) || cn.IsValid() {
					t.Fatalf("commit: %v, valid %t", err, cn.IsValid())
				}
				if calls := s.all(); calls[len(calls)-1].kind == wire.KindResolve && calls[len(calls)-1].req.Seq != 2 {
					t.Fatalf("resolved seq %d; want the commit's, 2", calls[len(calls)-1].req.Seq)
				}
				time.Sleep(abandonWait)
			})
		})
	}
}

// TestCommitDeadline checks that a Commit whose answer never comes ends at
// BeginTx's deadline, matching it, or without one at commitWait, matching
// no context error.
func TestCommitDeadline(t *testing.T) {
	for _, d := range []time.Duration{3 * time.Second, 0} {
		synctest.Test(t, func(t *testing.T) {
			cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/channel") && strings.Contains(readBody(r), `"op":"commit"`) {
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				return frames(func(ww *wire.Writer) error {
					if strings.HasSuffix(r.URL.Path, "/channel") {
						return ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: 1, TxnState: wire.TxnOpen, Txn: 1})
					}
					return ww.Done(&wire.Done{Kind: wire.DoneClose})
				})
			})
			ctx := context.Background()
			if d > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, d)
				defer cancel()
			}
			start := time.Now()
			tx, err := cn.BeginTx(ctx, driver.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			err = tx.Commit()
			took := time.Since(start)
			want := d
			if d == 0 {
				want = 30 * time.Second // the statement time
			}
			if took != want || !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, context.DeadlineExceeded) != (d > 0) {
				t.Fatalf("deadline %s: commit took %s, err %v; want %s and an unknown outcome", d, took, err, want)
			}
			time.Sleep(abandonWait)
		})
	}
}

func readBody(r *http.Request) string {
	b, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(strings.NewReader(string(b)))
	return string(b)
}

// TestBusyIsSentAgain checks that a begin or an autocommit write recorded
// as busy, overloaded or maintenance, which ran nothing, is sent again as a
// new request under the next sequence number after a growing, jittered
// pause, until the request's time ends, and that nothing else is.
func TestBusyIsSentAgain(t *testing.T) {
	t.Run("until it runs", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s := &script{}
			codes := []wire.Code{wire.CodeBusy, wire.CodeOverloaded, wire.CodeMaintenance}
			cn := channelConn(t, s.rt(t, func(_ wire.Kind, req *wire.Request, ww *wire.Writer) error {
				if int(req.Seq) <= len(codes) {
					return ww.Error(recorded(codes[req.Seq-1], req, false, wire.TxnNone))
				}
				return executed(ww, req, wire.TxnNone)
			}))
			if _, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil); err != nil {
				t.Fatal(err)
			}
			calls := s.all()
			if len(calls) != 4 {
				t.Fatalf("calls %q", s.kinds())
			}
			want := 100 * time.Millisecond
			for i, c := range calls {
				if c.req.Seq != int64(i+1) || c.req.Channel != testChannel {
					t.Fatalf("delivery %d: seq %d on %s", i, c.req.Seq, c.req.Channel)
				}
				if i == 0 {
					continue
				}
				if gap := c.at.Sub(calls[i-1].at); gap < want/2 || gap > want {
					t.Errorf("gap %d = %s; want %s less up to half", i, gap, want)
				}
				want *= 2
			}
		})
	})
	t.Run("until the window ends", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s := &script{}
			cn := channelConn(t, s.rt(t, func(_ wire.Kind, req *wire.Request, ww *wire.Writer) error {
				return ww.Error(recorded(wire.CodeBusy, req, false, wire.TxnNone))
			}))
			start := time.Now()
			_, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil)
			var se *Error
			if took := time.Since(start); !errors.As(err, &se) || se.Code != CodeBusy || !se.NotExecuted || errors.Is(err, ErrOutcomeUnknown) ||
				took > resendWindow || took < resendWindow-2*time.Second || !cn.IsValid() || len(s.all()) < 30 {
				t.Fatalf("after %s and %d deliveries: %v; want busy", took, len(s.all()), err)
			}
		})
	})
	t.Run("a begin", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s := &script{}
			cn := channelConn(t, s.rt(t, func(_ wire.Kind, req *wire.Request, ww *wire.Writer) error {
				if req.Seq == 1 {
					return ww.Error(recorded(wire.CodeBusy, req, false, wire.TxnNone))
				}
				return ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			}))
			if _, err := cn.BeginTx(context.Background(), driver.TxOptions{}); err != nil || s.kinds() != "begin begin" {
				t.Fatalf("begin: %v, calls %q", err, s.kinds())
			}
		})
	})
	for _, tc := range []struct {
		name        string
		inTx        bool
		code        wire.Code
		state       wire.TxnState
		notExecuted bool
	}{
		{"a statement of a transaction", true, wire.CodeOverloaded, wire.TxnOpen, false},
		{"a statement of a transaction, marked not executed", true, wire.CodeBusy, wire.TxnOpen, true},
		{"a lasting failure", false, wire.CodeSQLError, wire.TxnNone, true},
		{"a refusal that may have run", false, wire.CodeBusy, wire.TxnNone, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := &script{}
				cn := channelConn(t, s.rt(t, func(_ wire.Kind, req *wire.Request, ww *wire.Writer) error {
					e := recorded(tc.code, req, tc.inTx, tc.state)
					e.NotExecuted = tc.notExecuted
					return ww.Error(e)
				}))
				cn.inTx = tc.inTx
				_, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil)
				var se *Error
				if !errors.As(err, &se) || se.Code != tc.code || len(s.all()) != 1 {
					t.Fatalf("err %v after %d deliveries; want it at once", err, len(s.all()))
				}
			})
		})
	}
}

// TestStatelessRetries checks that a query, a describe and an open refused
// before they ran, with a transient code, are sent again, and that other
// refusals and the end of the statement time return the refusal.
func TestStatelessRetries(t *testing.T) {
	refusals := func(code wire.Code, n int, then func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error) func(wire.Kind, *wire.Request, *wire.Writer) error {
		seen := map[wire.Kind]int{}
		return func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
			if seen[kind]++; seen[kind] <= n {
				return ww.Error(refusal(code))
			}
			return then(kind, req, ww)
		}
	}
	answer := func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
		switch kind {
		case wire.KindQuery:
			_ = ww.Columns(bookColumns[:1])
			_ = ww.Row([]any{int64(5)})
			return ww.Done(queryDone(6))
		case wire.KindDescribe:
			_ = ww.Columns(nil)
			return ww.Done(&wire.Done{Kind: wire.DoneDescribe, Txn: 6})
		case wire.KindOpen:
			return opened(ww, testChannel)
		}
		return executed(ww, req, wire.TxnNone)
	}
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, refusals(wire.CodeOverloaded, 2, answer)))
		r, err := cn.QueryContext(context.Background(), "SELECT id FROM books", nil)
		if err != nil {
			t.Fatal(err)
		}
		dest := make([]driver.Value, 1)
		if err := r.Next(dest); err != nil || dest[0] != int64(5) || s.kinds() != "query query query" {
			t.Fatalf("row %v, %v, calls %q", dest[0], err, s.kinds())
		}
		_ = r.Close()
		if _, err := cn.PrepareContext(context.Background(), "SELECT 1"); err != nil || s.kinds(wire.KindDescribe) != "describe describe describe" {
			t.Fatalf("prepare: %v, calls %q", err, s.kinds())
		}
		cn.ch = nil
		if _, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil); err != nil || s.kinds(wire.KindOpen) != "open open open" {
			t.Fatalf("exec: %v, calls %q", err, s.kinds())
		}
	})
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, refusals(wire.CodeUnauthenticated, 100, answer)))
		var se *Error
		if _, err := cn.QueryContext(context.Background(), "SELECT 1", nil); !errors.As(err, &se) || se.Code != CodeUnauthenticated || len(s.all()) != 1 {
			t.Fatalf("err %v after %d requests; want unauthenticated at once", err, len(s.all()))
		}
	})
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, refusals(wire.CodeNotServing, 1000, answer)))
		start := time.Now()
		_, err := cn.QueryContext(context.Background(), "SELECT 1", nil)
		var se *Error
		if took := time.Since(start); !errors.As(err, &se) || se.Code != CodeNotServing || took > statementTime || took < statementTime-2*time.Second {
			t.Fatalf("err %v after %s; want not_serving at the statement time", err, took)
		}
	})
}

// TestReadOnlyTransaction checks that a read-only transaction opens a
// read-only channel at the watermark, begins on it, sends its statements
// and its commit there, and leaves the write channel for the writes after.
func TestReadOnlyTransaction(t *testing.T) {
	var refused atomic.Bool
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch {
		case c.req.Channel == testChannel:
			_ = ww.Done(execDone(c, wire.TxnNone, 3, 1, 1))
		case c.req.Op == wire.OpBegin:
			if !refused.Swap(true) { // a read-only cap: not executed, not final
				refuse(w, ww, wire.NewError(wire.CodeOverloaded, "read-only transactions are at their bound"))
				return
			}
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 30})
		case c.req.Op == wire.OpExecute:
			_ = ww.Columns(bookColumns[:1])
			_ = ww.Row([]any{int64(8)})
			_ = ww.Done(&wire.Done{Kind: wire.DoneExecute, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 30, ReadOnly: true})
		case c.req.Op == wire.OpCommit:
			_ = ww.Done(&wire.Done{Kind: wire.DoneCommit, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 30})
		}
	}))
	db := openDB(t, f.config())
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil { // opens the write channel at seq 1
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := tx.QueryRow("SELECT id FROM t").Scan(&id); err != nil || id != 8 {
		t.Fatalf("query: %d, %v", id, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	calls := f.lastCalls()
	if got := kinds(calls); got != "open execute open begin begin execute commit execute" {
		t.Fatalf("calls %q", got)
	}
	if calls[2].body != `{"mode":"read_only","min_txn":3}` {
		t.Fatalf("read-only open %s; want it at the watermark", calls[2].body)
	}
	for i, want := range []struct {
		token string
		seq   int64
	}{{testReadChannel, 1}, {testReadChannel, 1}, {testReadChannel, 2}, {testReadChannel, 3}, {testChannel, 2}} {
		if c := calls[3+i]; c.req.Channel != want.token || c.req.Seq != want.seq {
			t.Fatalf("request %d on %s at %d; want %s at %d", 3+i, c.req.Channel, c.req.Seq, want.token, want.seq)
		}
	}
	if calls[3].req.MinTxn != 3 || calls[3].body != calls[4].body {
		t.Fatalf("begin %s, resent %s", calls[3].body, calls[4].body)
	}
}

// TestReadOnlyCommitLost checks that a read-only transaction's commit whose
// channel was lost succeeds without a resolve: it had nothing to commit.
func TestReadOnlyCommitLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
			switch {
			case kind == wire.KindOpen:
				return ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{
					Token: testReadChannel, ID: testChannelID, Mode: wire.ModeReadOnly, Limits: wire.DefaultLimits(),
				}})
			case kind == wire.KindChannel && req.Op == wire.OpBegin:
				return ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			case kind == wire.KindChannel:
				return ww.Error(lost())
			}
			return ww.Done(&wire.Done{Kind: wire.DoneClose})
		}))
		tx, err := cn.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil || s.kinds(wire.KindResolve) != "" || cn.ch == nil || cn.ch.token != testChannel || cn.ro {
			t.Fatalf("commit: %v, calls %q; want success, unresolved, back on the write channel", err, s.kinds())
		}
		time.Sleep(abandonWait)
	})
}

// TestQueryRowRaisesTheWatermark checks that the txn of a stateless read
// whose rows database/sql closes after the first, as QueryRow does, still
// reaches the watermark, so that the next read asks for at least that state.
func TestQueryRowRaisesTheWatermark(t *testing.T) {
	f := newFake(t, true, answerRows(bookColumns[:1], [][]any{{int64(3)}, {int64(4)}}, 0, 77))
	db := openDB(t, f.config())
	var n int64
	for range 2 {
		if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	if calls := f.lastCalls(); calls[1].req.MinTxn != 77 {
		t.Fatalf("second read min_txn %d; want 77, the txn of the first", calls[1].req.MinTxn)
	}
	// A done frame that does not come holds Close back only for drainWait.
	gone := make(chan struct{})
	slow := newFake(t, true, blockingRows(gone))
	db = openDB(t, slow.config())
	start := time.Now()
	if err := db.QueryRow("SELECT id FROM books").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < drainWait || took > drainWait+2*time.Second {
		t.Fatalf("QueryRow took %s; want about drainWait", took)
	}
	<-gone
}

// TestPauseJitter checks that the pauses grow from 100 ms to 2 s and vary
// within the upper half of each step.
func TestPauseJitter(t *testing.T) {
	for n, step := range []time.Duration{100, 200, 400, 800, 1600, 2000, 2000, 2000} {
		step *= time.Millisecond
		lo, hi := step, time.Duration(0)
		for range 200 {
			d := pause(n)
			lo, hi = min(lo, d), max(hi, d)
		}
		if lo < step/2 || hi > step || hi-lo < step/4 {
			t.Errorf("pause(%d) from %s to %s; want spread over %s to %s", n, lo, hi, step/2, step)
		}
	}
}

// TestCloseDrainIsBounded checks that Close reads on for the done frame
// only up to drainBytes: past them the watermark stays where it was.
func TestCloseDrainIsBounded(t *testing.T) {
	big := strings.Repeat("x", 1<<10)
	rows := make([][]any, 512) // 512 KiB, which arrives well within drainWait
	for i := range rows {
		rows[i] = []any{int64(i), big}
	}
	f := newFake(t, true, answerRows(bookColumns[:2], rows, 0, 99))
	db := openDB(t, f.config())
	var id int64
	var title string
	for range 2 {
		if err := db.QueryRow("SELECT id, title FROM books").Scan(&id, &title); err != nil {
			t.Fatal(err)
		}
	}
	if calls := f.lastCalls(); calls[1].req.MinTxn != 0 {
		t.Fatalf("second read min_txn %d; want 0: the done frame lies past the drain bound", calls[1].req.MinTxn)
	}
}

// TestResolveStopsAtTheRequestsBounds checks that a resolve answered
// pending forever ends as soon as the caller cancels, at the caller's
// deadline, and at the end of the request's own time, not at resolveWait.
func TestResolveStopsAtTheRequestsBounds(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cancel, limit time.Duration // the caller's cancel and deadline, if set
		slow          time.Duration // how long the lost write ran before its answer
		want          time.Duration
		ctxErr        error // the context error the outcome must match, if any
	}{
		{"cancel", 2 * time.Second, 0, 0, 2 * time.Second, context.Canceled},
		{"deadline", 0, 5 * time.Second, 0, 5 * time.Second, nil}, // it stops when the next pause would pass the deadline
		{"the request's time", 0, 0, resendWindow - 10*time.Second, resendWindow, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := &script{}
				cn := channelConn(t, s.rt(t, func(kind wire.Kind, _ *wire.Request, ww *wire.Writer) error {
					switch kind {
					case wire.KindChannel:
						time.Sleep(tc.slow)
						return ww.Error(lost())
					case wire.KindResolve:
						return resolved(ww, wire.ResolutionPending, 2)
					}
					return ww.Done(&wire.Done{Kind: wire.DoneClose})
				}))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancel > 0 {
					time.AfterFunc(tc.cancel, cancel)
				}
				if tc.limit > 0 {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, tc.limit)
					defer stop()
				}
				start := time.Now()
				_, err := cn.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
				took := time.Since(start)
				if took > tc.want || took < tc.want-2*time.Second || s.kinds(wire.KindResolve) == "" {
					t.Fatalf("returned after %s, calls %q; want about %s, after a resolve", took, s.kinds(), tc.want)
				}
				if !errors.Is(err, ErrOutcomeUnknown) || tc.ctxErr != nil && !errors.Is(err, tc.ctxErr) {
					t.Fatalf("err %v; want unknown, matching %v", err, tc.ctxErr)
				}
				time.Sleep(abandonWait)
			})
		})
	}
}

// TestFinalErrorRaisesTheWatermark checks that the txn of a recorded
// failure reaches the watermark.
func TestFinalErrorRaisesTheWatermark(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, func(_ wire.Kind, req *wire.Request, ww *wire.Writer) error {
			e := recorded(wire.CodeSQLError, req, false, wire.TxnNone)
			e.Txn = 42
			return ww.Error(e)
		}))
		var se *Error
		if _, err := cn.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil); !errors.As(err, &se) || se.Code != CodeSQLError {
			t.Fatalf("err %v; want sql_error", err)
		}
		if got := cn.c.watermark.Load(); got != 42 {
			t.Fatalf("watermark %d; want 42, the failure's txn", got)
		}
	})
}

// TestReadOnlyCommitFailure checks that a read-only commit's settled
// failure is reported: only an unknown outcome is passed over.
func TestReadOnlyCommitFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &script{}
		cn := channelConn(t, s.rt(t, func(kind wire.Kind, req *wire.Request, ww *wire.Writer) error {
			switch {
			case kind == wire.KindOpen:
				return ww.Done(&wire.Done{Kind: wire.DoneOpen, Txn: 1, Channel: &wire.Channel{
					Token: testReadChannel, ID: testChannelID, Mode: wire.ModeReadOnly, Limits: wire.DefaultLimits(),
				}})
			case kind == wire.KindChannel && req.Op == wire.OpBegin:
				return ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: req.Seq, TxnState: wire.TxnOpen, Txn: 1})
			case kind == wire.KindChannel:
				return ww.Error(recorded(wire.CodeTransactionAborted, req, false, wire.TxnNone))
			}
			return ww.Done(&wire.Done{Kind: wire.DoneClose})
		}))
		tx, err := cn.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		var se *Error
		if err := tx.Commit(); !errors.As(err, &se) || se.Code != CodeTransactionAborted {
			t.Fatalf("commit: %v; want transaction_aborted", err)
		}
	})
}

// TestLostBeginIsBegunAgain checks that a begin lost with its channel, as
// after a failover, fails as not executed and matching driver.ErrBadConn,
// discards the connection, and is begun again by database/sql on a new
// connection and channel, without a resolve.
func TestLostBeginIsBegunAgain(t *testing.T) {
	var begins atomic.Int32
	f := newFake(t, true, channels(func(w http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		switch c.req.Op {
		case wire.OpBegin:
			if begins.Add(1) == 1 {
				e := wire.NewError(wire.CodeChannelLost, "the leader stopped")
				e.NotExecuted = false
				refuse(w, ww, e)
				return
			}
			_ = ww.Done(&wire.Done{Kind: wire.DoneBegin, Seq: c.req.Seq, TxnState: wire.TxnOpen, Txn: 2})
		case wire.OpCommit:
			_ = ww.Done(&wire.Done{Kind: wire.DoneCommit, Seq: c.req.Seq, TxnState: wire.TxnNone, Txn: 2})
		default:
			_ = ww.Done(execDone(c, wire.TxnNone, 1, 1, 1))
		}
	}))
	db := openDB(t, f.config())
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v; want it begun again on a new connection", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range f.lastCalls() {
		if c.kind != wire.KindClose && c.kind != wire.KindCancel {
			got = append(got, kinds([]call{c}))
		}
	}
	if s := strings.Join(got, " "); s != "open execute begin open begin commit" {
		t.Fatalf("calls %q", s)
	}
	cn := channelConn(t, func(r *http.Request) (*http.Response, error) {
		return frames(func(ww *wire.Writer) error {
			if strings.HasSuffix(r.URL.Path, "/channel") {
				return ww.Error(lost())
			}
			return ww.Done(&wire.Done{Kind: wire.DoneClose})
		})
	})
	_, err = cn.BeginTx(context.Background(), driver.TxOptions{})
	var se *Error
	if !errors.Is(err, driver.ErrBadConn) || !errors.As(err, &se) || !se.NotExecuted || se.Code != CodeChannelLost || errors.Is(err, ErrOutcomeUnknown) || cn.IsValid() {
		t.Fatalf("err %v, valid %t; want not executed, a bad connection", err, cn.IsValid())
	}
}
