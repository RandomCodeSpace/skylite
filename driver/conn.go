package skylite

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// conn is a database/sql connection. Each query and describe outside a
// transaction is one stateless request; a read-only transaction goes on its
// read-only channel, and everything else on its write channel.
type conn struct {
	c      *Connector
	closed bool
	ch     *channel // the channel in use: nil until a request needs one, and after it was abandoned
	other  *channel // the channel not in use: the write channel in a read-only transaction
	ro     bool     // ch is the read-only channel, of a read-only transaction
	inTx   bool     // a transaction is open, or lost and not yet rolled back
	lost   bool     // an outcome was unknown: the connection is discarded
}

var (
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
)

// Close closes the connection's channels, waiting for each answer up to
// abandonWait, so that the server releases them at once.
func (cn *conn) Close() error {
	cn.closed = true
	for _, ch := range []*channel{cn.ch, cn.other} {
		if ch != nil {
			cn.c.abandon(ch.token, 0)
		}
	}
	cn.ch, cn.other = nil, nil
	return nil
}

// IsValid reports false for a connection that met an unknown outcome, so
// that database/sql closes it instead of reusing it.
func (cn *conn) IsValid() bool { return !cn.closed && !cn.lost }

// ResetSession returns driver.ErrBadConn for a connection that met an
// unknown outcome, so that database/sql discards it.
func (cn *conn) ResetSession(context.Context) error {
	if cn.lost {
		return driver.ErrBadConn
	}
	return nil
}

// response is a response being read.
type response struct {
	r    *wire.Reader
	body io.ReadCloser
}

// send sends a stateless request and returns its response once its headers
// have arrived. The only driver.ErrBadConn it returns is for a closed or
// discarded connection, before anything is built or sent: that request was
// never sent, so database/sql may run it on another connection (ADR 0005
// decision 4, case 1). Every other failure returns the real error, because
// a request whose bytes may have been written must not be run again by
// database/sql.
func (cn *conn) send(ctx context.Context, kind wire.Kind, call *wire.Call) (*response, error) {
	if cn.closed || cn.lost {
		return nil, driver.ErrBadConn
	}
	done := wire.DoneQuery
	if kind == wire.KindDescribe {
		done = wire.DoneDescribe
	}
	call.MinTxn = cn.c.watermark.Load()
	body, err := wire.AppendCall(nil, kind, call)
	if err != nil {
		return nil, fmt.Errorf("skylite: %w", err)
	}
	req, err := cn.c.request(ctx, kind, body)
	if err != nil {
		return nil, err
	}
	return cn.c.roundTrip(req, done)
}

// request builds a request of kind with body, and its bearer token. Its
// failures come before anything is sent.
func (c *Connector) request(ctx context.Context, kind wire.Kind, body []byte) (*http.Request, error) {
	token, err := c.token.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("skylite: token source: %w", err)
	}
	if !validBearer(token) {
		return nil, errors.New("skylite: the token source returned an empty token or one with characters a bearer token cannot hold")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+wire.Path(kind, c.db), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("skylite: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// roundTrip sends req and returns a reader of its response, whose done
// frame is of kind done, once its headers have arrived.
func (c *Connector) roundTrip(req *http.Request, done wire.DoneKind) (*response, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err // its text holds the request URL
		}
		return nil, fmt.Errorf("skylite: %w", err)
	}
	if resp.Header.Get("Skylite-Protocol") != "1" {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("skylite: the answer (HTTP status %d) has no Skylite-Protocol: 1 header, so it is not from a skylite server", resp.StatusCode)
	}
	r, err := wire.NewReader(resp.Body, done, maxFrame)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return &response{r: r, body: resp.Body}, nil
}

// validBearer reports whether s can be sent as a bearer token: printable
// ASCII without spaces, so that it cannot break the header.
func validBearer(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] <= ' ' || s[i] > '~' {
			return false
		}
	}
	return true
}

// next reads the next frame. After the caller's context ended, net/http's
// error wraps the context's.
func (rs *response) next() (*wire.Frame, error) {
	f, err := rs.r.Next()
	if err != nil {
		return nil, fmt.Errorf("skylite: reading the response: %w", err)
	}
	return f, nil
}

// QueryContext sends a query outside a transaction to the stateless query
// endpoint, and on to the write channel when the server answers that it is
// not a pure read. Inside a transaction it goes on the channel. A query
// refused before it ran, with a transient code, is sent again after a
// pause, within the context and the statement time.
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if cn.inTx {
		return cn.channelQuery(ctx, query, args)
	}
	call, by := cn.call(query, args), time.Now().Add(statementTime)
	var last *wire.Error
	for n := 0; ; n++ {
		qctx, cancel := context.WithCancel(ctx) // rows.Close ends a read it stops waiting for
		rs, err := cn.send(qctx, wire.KindQuery, call)
		if err != nil {
			cancel()
			return nil, failed(ctx, last, err)
		}
		f, err := rs.next()
		if err != nil || f.Kind == wire.FrameError {
			_ = rs.body.Close()
			cancel()
			switch {
			case err != nil:
				return nil, failed(ctx, last, err)
			case f.Error.Code == CodeNeedsChannel: // refused before execution
				return cn.channelQuery(ctx, query, args)
			case transient(f.Error) && backoff(ctx, by, n):
				last = f.Error
				continue
			}
			return nil, refused(ctx, f.Error)
		}
		r := &rows{c: cn.c, rs: rs, cancel: cancel}
		if f.Kind == wire.FrameColumns {
			r.cols = f.Columns
		} else {
			r.end(f.Done) // a pure read without result columns
		}
		return r, nil
	}
}

// ExecContext runs the statement on the write channel: an autocommit
// write, or a statement of the open transaction. Its rows are read and
// dropped.
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	a, err := cn.do(ctx, time.Now().Add(resendWindow), cn.execute(query, args), wire.DoneExecute, false)
	if err != nil {
		return nil, err
	}
	return result{a.done}, nil
}

// channelQuery runs a query on the write channel and returns its rows,
// read whole: ADR 0005 decision 6 settles a channel request before any of
// its rows reach database/sql. A read-only statement whose replay omits its
// rows is sent again as a new request, within the same resend window (ADR
// 0005 decision 3), when it is a statement of a transaction with no rows
// affected: outside one, it is a query refused as not a pure read. It is
// sent again at most reissues times, after a pause, and not once the pause
// would pass the window.
func (cn *conn) channelQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	by := time.Now().Add(resendWindow)
	a, err := cn.do(ctx, by, cn.execute(query, args), wire.DoneExecute, true)
reissue:
	for n, wait := 0, 100*time.Millisecond; err == nil && cn.inTx && a.done.DataOmitted && a.done.ReadOnly && a.done.RowsAffected == 0 && n < reissues; n, wait = n+1, 2*wait {
		if time.Now().Add(wait).After(by) {
			break
		}
		select {
		case <-ctx.Done():
			break reissue
		case <-time.After(wait):
		}
		a, err = cn.do(ctx, by, cn.execute(query, args), wire.DoneExecute, true)
	}
	if err != nil {
		return nil, err
	}
	if a.done.DataOmitted {
		e := &Error{
			Message: "the response was lost and its replay omits its rows", Seq: a.done.Seq, Txn: a.done.Txn,
			committed: !cn.inTx && !a.done.ReadOnly, is: []error{ErrResultUnavailable},
		}
		if err := ctx.Err(); err != nil {
			e.is = append(e.is, err)
		}
		return nil, e
	}
	return &rows{c: cn.c, cols: a.cols, buf: a.rows}, nil
}

func (cn *conn) execute(query string, args []driver.NamedValue) *wire.Call {
	c := cn.call(query, args)
	c.Op = wire.OpExecute
	return c
}

// call builds a query's body from database/sql's arguments, which
// CheckNamedValue has converted. AppendCall refuses a mix of positional and
// named arguments.
func (cn *conn) call(query string, args []driver.NamedValue) *wire.Call {
	c := &wire.Call{SQL: query, TimeoutMS: cn.c.timeoutMS}
	for _, a := range args {
		if a.Name == "" {
			c.Params = append(c.Params, a.Value)
		} else {
			c.Named = append(c.Named, wire.NamedArg{Name: a.Name, Value: a.Value})
		}
	}
	return c
}

// PrepareContext describes the statement: the server compiles it, executes
// nothing, and reports its parameter count. A describe refused with a
// transient code is sent again after a pause, within the context and the
// statement time. Inside a transaction it sends nothing and the count is
// unknown: a stateless describe cannot see the transaction's schema
// changes. A lost transaction refuses it.
func (cn *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if cn.inTx {
		if err := cn.usable(); err != nil {
			return nil, err
		}
		return &stmt{cn: cn, query: query, params: -1}, nil
	}
	by := time.Now().Add(statementTime)
	var last *wire.Error
	for n := 0; ; n++ {
		params, e, err := cn.describe(ctx, query)
		switch {
		case err != nil:
			return nil, failed(ctx, last, err)
		case e == nil:
			return &stmt{cn: cn, query: query, params: params}, nil
		case !transient(e) || !backoff(ctx, by, n):
			return nil, refused(ctx, e)
		}
		last = e
	}
}

// failed is the error of a stateless request that failed with err: after a
// transient refusal last, when the context ended during the next attempt,
// that refusal, which also matches the context's error.
func failed(ctx context.Context, last *wire.Error, err error) error {
	if last != nil && ctx.Err() != nil {
		return refused(ctx, last)
	}
	return err
}

// describe sends one stateless describe of query and returns its parameter
// count, or the server's error.
func (cn *conn) describe(ctx context.Context, query string) (int, *wire.Error, error) {
	rs, err := cn.send(ctx, wire.KindDescribe, &wire.Call{SQL: query})
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = rs.body.Close() }()
	for {
		f, err := rs.next()
		if err != nil {
			return 0, nil, err
		}
		switch f.Kind {
		case wire.FrameError:
			return 0, f.Error, nil
		case wire.FrameDone:
			cn.c.raise(f.Done.Txn)
			return int(f.Done.ParamCount), nil, nil
		}
	}
}

func (cn *conn) Prepare(query string) (driver.Stmt, error) {
	return cn.PrepareContext(context.Background(), query)
}

// Ping describes "SELECT 1": the cheapest request any pod serves without a
// channel, which proves the endpoint, the token and the database.
func (cn *conn) Ping(ctx context.Context) error {
	_, err := cn.PrepareContext(ctx, "SELECT 1")
	return err
}

type stmt struct {
	cn     *conn
	query  string
	params int
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return s.params }

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.cn.QueryContext(ctx, s.query, args)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.cn.ExecContext(ctx, s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), named(args))
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), named(args))
}

func named(args []driver.Value) []driver.NamedValue {
	nv := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return nv
}

type result struct{ d *wire.Done }

func (r result) LastInsertId() (int64, error) { return r.d.LastInsertRowID, nil }
func (r result) RowsAffected() (int64, error) { return r.d.RowsAffected, nil }

// rows streams a stateless query's rows: each is decoded as its frame
// arrives, and Close releases the response, whether or not it has been read
// to its end. A channel query's rows are in buf, and rs is nil.
type rows struct {
	c      *Connector
	rs     *response
	cancel context.CancelFunc // ends rs's request
	cols   []wire.Column
	done   *wire.Done // the terminal frame, once read
	buf    [][]driver.Value
}

var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)

// drainBytes and drainWait bound how far Close reads on for the done frame
// of a stateless query whose rows it closes before their end.
const (
	drainBytes = 64 << 10
	drainWait  = 250 * time.Millisecond
)

func (r *rows) Columns() []string {
	names := make([]string, len(r.cols))
	for i, c := range r.cols {
		names[i] = c.Name
	}
	return names
}

// Close releases the response. Before its end, it first reads on, up to
// drainBytes and drainWait, for the done frame: database/sql closes a
// QueryRow's rows after the first, and only the done frame carries the txn
// of the state read, which the watermark must reach so that no later read
// of the pool sees an older state (ADR 0005 decision 5).
func (r *rows) Close() error {
	if r.rs == nil {
		return nil
	}
	defer r.cancel()
	if r.done == nil {
		t := time.AfterFunc(drainWait, r.cancel)
		r.rs.r.LimitResult(r.rs.r.Result() + drainBytes)
		for {
			f, err := r.rs.r.Next()
			if err != nil || f.Kind == wire.FrameError {
				break
			}
			if f.Kind == wire.FrameDone {
				r.end(f.Done)
				break
			}
		}
		t.Stop()
	}
	return r.rs.body.Close()
}

func (r *rows) end(d *wire.Done) {
	r.done = d
	r.c.raise(d.Txn)
}

func (r *rows) Next(dest []driver.Value) error {
	if r.rs == nil {
		if len(r.buf) == 0 {
			return io.EOF
		}
		copy(dest, r.buf[0])
		r.buf = r.buf[1:]
		return nil
	}
	if r.done != nil {
		return io.EOF
	}
	f, err := r.rs.next()
	if err != nil {
		return err
	}
	switch f.Kind {
	case wire.FrameRow:
		for i, v := range f.Row {
			dest[i] = columnValue(r.cols[i].DeclType, v)
		}
		return nil
	case wire.FrameDone:
		r.end(f.Done)
		return io.EOF
	case wire.FrameError:
		return serverError(f.Error)
	}
	return fmt.Errorf("skylite: reading the response: unexpected frame kind %d", f.Kind)
}

// ColumnTypeDatabaseTypeName returns the column's declared type name, as
// typeName reads it, or "" for an expression.
func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return typeName(r.cols[i].DeclType) }
