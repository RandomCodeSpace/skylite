package skylite

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// conn is a database/sql connection. It holds no server state: each query
// and describe is one stateless request.
type conn struct {
	c      *Connector
	closed bool
}

var (
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
)

func (cn *conn) Close() error { cn.closed = true; return nil }

func (cn *conn) Begin() (driver.Tx, error) { return nil, ErrTransactionsUnsupported }

func (cn *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, ErrTransactionsUnsupported
}

// response is a response being read.
type response struct {
	r    *wire.Reader
	body io.ReadCloser
}

// send sends a stateless request and returns its response once its headers
// have arrived. The only driver.ErrBadConn it returns is for a closed
// connection, before anything is built or sent: that request was never
// sent, so database/sql may run it on another connection (ADR 0005
// decision 4, case 1). Every other failure returns the real error, because
// a request whose bytes may have been written must not be run again by
// database/sql.
func (cn *conn) send(ctx context.Context, kind wire.Kind, call *wire.Call) (*response, error) {
	if cn.closed {
		return nil, driver.ErrBadConn
	}
	u, done := cn.c.query, wire.DoneQuery
	if kind == wire.KindDescribe {
		u, done = cn.c.describe, wire.DoneDescribe
	}
	call.MinTxn = cn.c.watermark.Load()
	body, err := wire.AppendCall(nil, kind, call)
	if err != nil {
		return nil, fmt.Errorf("skylite: %w", err)
	}
	token, err := cn.c.token.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("skylite: token source: %w", err)
	}
	if !validBearer(token) {
		return nil, errors.New("skylite: the token source returned an empty token or one with characters a bearer token cannot hold")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("skylite: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := cn.c.client.Do(req)
	if err != nil {
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

func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rs, err := cn.send(ctx, wire.KindQuery, cn.call(query, args))
	if err != nil {
		return nil, err
	}
	f, err := rs.next()
	if err == nil && f.Kind == wire.FrameError {
		err = serverError(f.Error)
	}
	if err != nil {
		_ = rs.body.Close()
		return nil, err
	}
	r := &rows{c: cn.c, rs: rs}
	if f.Kind == wire.FrameColumns {
		r.cols = f.Columns
	} else {
		r.end(f.Done) // a pure read without result columns
	}
	return r, nil
}

func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dr, err := cn.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	r := dr.(*rows)
	defer func() { _ = r.Close() }()
	dest := make([]driver.Value, len(r.cols))
	for {
		if err := r.Next(dest); errors.Is(err, io.EOF) {
			return result{r.done}, nil
		} else if err != nil {
			return nil, err
		}
	}
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
// nothing, and reports its parameter count.
func (cn *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	rs, err := cn.send(ctx, wire.KindDescribe, &wire.Call{SQL: query})
	if err != nil {
		return nil, err
	}
	defer func() { _ = rs.body.Close() }()
	for {
		f, err := rs.next()
		if err != nil {
			return nil, err
		}
		switch f.Kind {
		case wire.FrameError:
			return nil, serverError(f.Error)
		case wire.FrameDone:
			cn.c.raise(f.Done.Txn)
			return &stmt{cn: cn, query: query, params: int(f.Done.ParamCount)}, nil
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

// rows streams a query's rows: each is decoded as its frame arrives, and
// Close releases the response, whether or not it has been read to its end.
type rows struct {
	c    *Connector
	rs   *response
	cols []wire.Column
	done *wire.Done // the terminal frame, once read
}

var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)

func (r *rows) Columns() []string {
	names := make([]string, len(r.cols))
	for i, c := range r.cols {
		names[i] = c.Name
	}
	return names
}

func (r *rows) Close() error { return r.rs.body.Close() }

func (r *rows) end(d *wire.Done) {
	r.done = d
	r.c.raise(d.Txn)
}

func (r *rows) Next(dest []driver.Value) error {
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

// ColumnTypeDatabaseTypeName returns the column's declared type in upper
// case without its length, or "" for an expression.
func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	t, _, _ := strings.Cut(r.cols[i].DeclType, "(")
	return strings.ToUpper(strings.TrimSpace(t))
}
