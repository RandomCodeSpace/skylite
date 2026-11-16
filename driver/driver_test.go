package skylite

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	_ "time/tzdata" // Asia/Kolkata for TestBindConversionsInAnotherZone

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

var bookColumns = []wire.Column{
	{Name: "id", DeclType: "INTEGER", Database: "main", Table: "books", Origin: "id"},
	{Name: "title", DeclType: "varchar(20)", Database: "main", Table: "books", Origin: "title"},
	{Name: "score"},
	{Name: "cover", DeclType: "BLOB"},
	{Name: "note"},
}

func TestQueryOverHTTP2(t *testing.T) {
	f := newFake(t, true, answerRows(bookColumns, [][]any{
		{int64(78), "Dune", 4.5, []byte{0xff, 0}, nil},
		{int64(79), "", math.Copysign(0, -1), []byte{}, "x"},
	}, 0, 41))
	cfg := f.config()
	cfg.StatementTimeout = 1500 * time.Millisecond
	db := openDB(t, cfg)

	rs, err := db.QueryContext(context.Background(), "SELECT id, title, score, cover, note FROM books WHERE id > ?", 77)
	if err != nil {
		t.Fatal(err)
	}
	cols, _ := rs.Columns()
	types, _ := rs.ColumnTypes()
	if strings.Join(cols, ",") != "id,title,score,cover,note" || types[1].DatabaseTypeName() != "VARCHAR" || types[0].DatabaseTypeName() != "INTEGER" || types[2].DatabaseTypeName() != "" {
		t.Fatalf("columns %v, type names %q %q %q", cols, types[0].DatabaseTypeName(), types[1].DatabaseTypeName(), types[2].DatabaseTypeName())
	}
	var got []string
	for rs.Next() {
		var id int64
		var title string
		var score float64
		var cover []byte
		var note sql.NullString
		if err := rs.Scan(&id, &title, &score, &cover, &note); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d|%s|%v|%x|%v", id, title, score, cover, note))
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rs.Close()
	if want := "78|Dune|4.5|ff00|{ false} 79||-0||{x true}"; strings.Join(got, " ") != want {
		t.Fatalf("rows %q; want %q", strings.Join(got, " "), want)
	}

	if _, err := db.QueryContext(context.Background(), "SELECT * FROM books WHERE title = :t", sql.Named("t", "Dune")); err != nil {
		t.Fatal(err)
	}
	calls := f.lastCalls()
	if len(calls) != 2 || calls[0].proto != 2 {
		t.Fatalf("calls %+v; want 2 over HTTP/2", calls)
	}
	if want := `{"sql":"SELECT id, title, score, cover, note FROM books WHERE id > ?","params":[77],"timeout_ms":1500}`; calls[0].body != want {
		t.Errorf("first body %s; want %s", calls[0].body, want)
	}
	if want := `{"sql":"SELECT * FROM books WHERE title = :t","named":{"t":"Dune"},"min_txn":41,"timeout_ms":1500}`; calls[1].body != want {
		t.Errorf("second body %s; want %s", calls[1].body, want)
	}
}

func TestWatermarkNeverLowers(t *testing.T) {
	txns := []int64{41, 30, 50}
	var n atomic.Int32
	f := newFake(t, true, func(_ http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		_ = ww.Done(queryDone(txns[n.Add(1)-1]))
	})
	db := openDB(t, f.config())
	for range 4 {
		if _, err := db.Exec("SELECT 1"); err != nil {
			t.Fatal(err)
		}
		if int(n.Load()) == len(txns) {
			break
		}
	}
	calls := f.lastCalls()
	for i, want := range []int64{0, 41, 41} {
		if calls[i].req.MinTxn != want {
			t.Errorf("request %d min_txn %d; want %d", i, calls[i].req.MinTxn, want)
		}
	}
}

func TestExecReadsTheResult(t *testing.T) {
	f := newFake(t, true, func(_ http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		_ = ww.Columns(bookColumns[:1])
		_ = ww.Row([]any{int64(1)})
		_ = ww.Row([]any{int64(2)})
		_ = ww.Done(&wire.Done{Kind: wire.DoneQuery, Txn: 3, ReadOnly: true, RowsAffected: 5, LastInsertRowID: 6})
	})
	db := openDB(t, f.config())
	res, err := db.Exec("SELECT id FROM books")
	if err != nil {
		t.Fatal(err)
	}
	ra, _ := res.RowsAffected()
	id, _ := res.LastInsertId()
	if ra != 5 || id != 6 {
		t.Fatalf("result %d, %d; want 5, 6", ra, id)
	}
}

func TestPrepareDescribes(t *testing.T) {
	f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request) {
		if c.kind == wire.KindDescribe {
			_ = ww.Columns(bookColumns[:1])
			_ = ww.Params([]string{":t", ""})
			_ = ww.Done(&wire.Done{Kind: wire.DoneDescribe, Txn: 9, ParamCount: 2})
			return
		}
		answerRows(bookColumns[:1], [][]any{{int64(7)}}, 0, 9)(w, ww, c, r)
	})
	db := openDB(t, f.config())
	st, err := db.Prepare("SELECT id FROM books WHERE title = :t AND id > ?")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.Query("Dune"); err == nil || !strings.Contains(err.Error(), "expected 2 arguments") {
		t.Fatalf("query with one argument: %v; want database/sql's count error", err)
	}
	var id int64
	if err := st.QueryRow("Dune", 3).Scan(&id); err != nil || id != 7 {
		t.Fatalf("QueryRow = %d, %v", id, err)
	}
	calls := f.lastCalls()
	if calls[0].kind != wire.KindDescribe || calls[0].body != `{"sql":"SELECT id FROM books WHERE title = :t AND id > ?"}` {
		t.Fatalf("first call %+v; want the describe", calls[0])
	}
	if last := calls[len(calls)-1]; last.kind != wire.KindQuery || last.req.MinTxn != 9 {
		t.Fatalf("last call %+v; want a query at min_txn 9", last)
	}
}

func TestPing(t *testing.T) {
	missing := false
	f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request) {
		if missing {
			refuse(w, ww, wire.NewError(wire.CodeDatabaseNotFound, "no such database"))
			return
		}
		answerRows([]wire.Column{{Name: "1"}}, nil, 0, 2)(w, ww, c, r)
	})
	db := openDB(t, f.config())
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if c := f.lastCalls(); len(c) != 1 || c[0].kind != wire.KindDescribe || c[0].req.SQL != "SELECT 1" {
		t.Fatalf("ping sent %+v; want one describe of SELECT 1", c)
	}
	missing = true
	var se *Error
	if err := db.Ping(); !errors.As(err, &se) || se.Code != CodeDatabaseNotFound {
		t.Fatalf("ping of a missing database: %v", err)
	}
}

func TestWritesAndTransactionsUnsupported(t *testing.T) {
	f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		refuse(w, ww, wire.NewError(wire.CodeNeedsChannel, "the statement is not a pure read"))
	})
	db := openDB(t, f.config())
	_, err := db.Exec("INSERT INTO books (title) VALUES (?)", "Dune")
	var se *Error
	if !errors.Is(err, ErrWritesUnsupported) || !errors.Is(err, errors.ErrUnsupported) || !errors.As(err, &se) || se.Code != CodeNeedsChannel || !se.NotExecuted {
		t.Fatalf("write: %v", err)
	}
	if !strings.Contains(err.Error(), "writes are not supported in this driver version") {
		t.Fatalf("write error %q does not say writes are unsupported", err)
	}
	if n := f.hits.Load(); n != 1 {
		t.Fatalf("%d requests; database/sql retried the write", n)
	}
	if _, err := db.Begin(); !errors.Is(err, ErrTransactionsUnsupported) {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true}); !errors.Is(err, ErrTransactionsUnsupported) {
		t.Fatalf("BeginTx: %v", err)
	}
	if n := f.hits.Load(); n != 1 {
		t.Fatalf("%d requests; Begin sent something", n)
	}
}

// allCodes is ADR 0005's closed list of error codes.
var allCodes = []Code{
	CodeInvalidRequest, CodeUnauthenticated, CodeForbidden, CodeStatementForbidden, CodeDatabaseNotFound,
	CodeDatabaseExists, CodeNeedsChannel, CodeChannelBusy, CodeInFlight, CodeStaleSequence, CodeSequenceGap,
	CodeSequenceConflict, CodeChannelLost, CodeRequestTooLarge, CodeTooManyChannels, CodeOverloaded,
	CodeInvalidState, CodeTransactionAborted, CodeBusy, CodeTimeout, CodeCancelled, CodeResultTooLarge,
	CodeSQLError, CodeCommitFailed, CodeCommitOutcomeUnknown, CodeNoLeader, CodeNotServing,
	CodeOwnerUnreachable, CodeBehind, CodeMaintenance, CodeInternal,
}

func TestEveryErrorCode(t *testing.T) {
	var code atomic.Value
	f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		c := code.Load().(Code)
		e := wire.NewError(c, "message of "+string(c))
		e.RequestID, e.SQLiteCode, e.SQLiteExtendedCode, e.Txn, e.Epoch = "req-"+string(c), 5, 517, 12, 3
		if ww.Error(e) != nil { // a code the writer does not know
			_, _ = fmt.Fprintf(w, `{"error":{"code":%q,"not_executed":true,"final":false,"message":"message of %s","request_id":"req-%s","sqlite_code":5,"sqlite_extended_code":517,"txn":12,"epoch":3}}`+"\n", c, c, c)
		}
	})
	db := openDB(t, f.config())
	for _, c := range append(allCodes, "a_code_from_a_later_version") {
		t.Run(string(c), func(t *testing.T) {
			code.Store(c)
			before := f.hits.Load()
			_, err := db.Query("SELECT 1")
			var se *Error
			if !errors.As(err, &se) {
				t.Fatalf("err = %v; want an *Error", err)
			}
			want := wire.NewError(c, "")
			if se.Code != c || se.NotExecuted != want.NotExecuted || se.Final != want.Final || se.Message != "message of "+string(c) ||
				se.RequestID != "req-"+string(c) || se.SQLiteCode != 5 || se.SQLiteExtendedCode != 517 || se.Txn != 12 || se.Epoch != 3 {
				t.Fatalf("error %+v", se)
			}
			if errors.Is(err, driver.ErrBadConn) || f.hits.Load()-before != 1 {
				t.Fatalf("err %v sent %d requests; want 1 and no ErrBadConn", err, f.hits.Load()-before)
			}
			if errors.Is(err, ErrWritesUnsupported) != (c == CodeNeedsChannel) {
				t.Fatalf("ErrWritesUnsupported matched %v", err)
			}
			if !strings.Contains(err.Error(), string(c)) || !strings.Contains(err.Error(), "req-"+string(c)) {
				t.Fatalf("message %q lacks the code or request id", err)
			}
		})
	}
}

func TestErrorAfterRows(t *testing.T) {
	f := newFake(t, true, func(_ http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		_ = ww.Columns(bookColumns[:1])
		_ = ww.Row([]any{int64(1)})
		_ = ww.Error(&wire.Error{Code: wire.CodeMaintenance, NotExecuted: true, Message: "maintenance", RequestID: "r"})
	})
	db := openDB(t, f.config())
	rs, err := db.Query("SELECT id FROM books")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rs.Next() {
		n++
	}
	var se *Error
	if n != 1 || !errors.As(rs.Err(), &se) || se.Code != CodeMaintenance || errors.Is(rs.Err(), driver.ErrBadConn) {
		t.Fatalf("%d rows, err %v", n, rs.Err())
	}
}

func TestIncompleteResponseIsAnError(t *testing.T) {
	f := newFake(t, true, func(_ http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		_ = ww.Columns(bookColumns[:1])
		_ = ww.Row([]any{int64(1)})
	})
	db := openDB(t, f.config())
	rs, err := db.Query("SELECT id FROM books")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rs.Next() {
		n++
	}
	if n != 1 || !errors.Is(rs.Err(), io.ErrUnexpectedEOF) {
		t.Fatalf("%d rows, err %v; want one row and io.ErrUnexpectedEOF", n, rs.Err())
	}
}

// TestBadConnOnlyBeforeSend checks that driver.ErrBadConn, on which
// database/sql runs a request again, comes only from a closed connection,
// before anything is sent, and never after a request may have been sent.
func TestBadConnOnlyBeforeSend(t *testing.T) {
	var mode atomic.Int32
	f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request) {
		switch mode.Load() {
		case 1: // the connection drops before any answer
			panic(http.ErrAbortHandler)
		case 2: // the connection drops in the middle of the rows
			_ = ww.Columns(bookColumns[:1])
			_ = ww.Row([]any{int64(1)})
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		case 3: // an answer without the protocol marker
			w.Header().Del("Skylite-Protocol")
			w.WriteHeader(http.StatusBadGateway)
		default:
			answerRows(bookColumns[:1], nil, 0, 1)(w, ww, c, r)
		}
	})
	c, err := NewConnector(f.config())
	if err != nil {
		t.Fatal(err)
	}
	dc, _ := c.Connect(context.Background())
	_ = dc.Close()
	if _, err := dc.(driver.QueryerContext).QueryContext(context.Background(), "SELECT 1", nil); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("query on a closed connection: %v; want driver.ErrBadConn", err)
	}
	if _, err := dc.(driver.ConnPrepareContext).PrepareContext(context.Background(), "SELECT 1"); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("prepare on a closed connection: %v; want driver.ErrBadConn", err)
	}
	if n := f.hits.Load(); n != 0 {
		t.Fatalf("a closed connection sent %d requests", n)
	}

	db := sql.OpenDB(c)
	defer func() { _ = db.Close() }()
	for m, name := range []string{1: "dropped before the answer", 2: "dropped mid-stream", 3: "no marker"} {
		if m == 0 {
			continue
		}
		mode.Store(int32(m)) //nolint:gosec // small
		before := f.hits.Load()
		rs, err := db.Query("SELECT id FROM books")
		if err == nil {
			for rs.Next() {
			}
			err = rs.Err()
			_ = rs.Close()
		}
		if err == nil || errors.Is(err, driver.ErrBadConn) || f.hits.Load()-before != 1 {
			t.Errorf("%s: err %v after %d requests; want one request and an error that is not ErrBadConn", name, err, f.hits.Load()-before)
		}
		if m == 3 && !strings.Contains(fmt.Sprint(err), "Skylite-Protocol") {
			t.Errorf("no marker: %v", err)
		}
	}

	mode.Store(0)
	failing := f.config()
	failing.Token = TokenFunc(func(context.Context) (string, error) { return "", errors.New("the token file is missing") })
	before := f.hits.Load()
	if err := openDB(t, failing).Ping(); err == nil || errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "the token file is missing") {
		t.Fatalf("failing token source: %v", err)
	}
	if f.hits.Load() != before {
		t.Fatal("a request was sent without a token")
	}
}

// blockingRows answers with one row, flushes it, and holds the response open
// until the client goes away, which it reports on gone.
func blockingRows(gone chan<- struct{}) func(http.ResponseWriter, *wire.Writer, call, *http.Request) {
	return func(w http.ResponseWriter, ww *wire.Writer, _ call, r *http.Request) {
		_ = ww.Columns(bookColumns[:1])
		_ = ww.Row([]any{int64(1)})
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(gone)
		case <-time.After(10 * time.Second):
		}
	}
}

func TestCancelMidStream(t *testing.T) {
	gone := make(chan struct{})
	f := newFake(t, true, blockingRows(gone))
	db := openDB(t, f.config())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs, err := db.QueryContext(ctx, "SELECT id FROM books")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rs.Close() }()
	if !rs.Next() {
		t.Fatalf("no first row while the server holds the response: %v", rs.Err())
	}
	cancel()
	for rs.Next() {
	}
	if !errors.Is(rs.Err(), context.Canceled) {
		t.Fatalf("err after cancel %v; want context.Canceled", rs.Err())
	}
	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		t.Fatal("the server's request was not cancelled")
	}
}

// TestCancelMidStreamDriverRows cancels below database/sql, which would
// otherwise substitute the context's error itself.
func TestCancelMidStreamDriverRows(t *testing.T) {
	gone := make(chan struct{})
	f := newFake(t, true, blockingRows(gone))
	c, err := NewConnector(f.config())
	if err != nil {
		t.Fatal(err)
	}
	dc, _ := c.Connect(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dr, err := dc.(driver.QueryerContext).QueryContext(ctx, "SELECT id FROM books", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dr.Close() }()
	dest := make([]driver.Value, 1)
	if err := dr.Next(dest); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := dr.Next(dest); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next after cancel: %v; want context.Canceled", err)
	}
	<-gone
}

func TestRowsCloseMidStream(t *testing.T) {
	gone := make(chan struct{})
	f := newFake(t, true, blockingRows(gone))
	c, err := NewConnector(f.config())
	if err != nil {
		t.Fatal(err)
	}
	dc, _ := c.Connect(context.Background())
	dr, err := dc.(driver.QueryerContext).QueryContext(context.Background(), "SELECT id FROM books", nil)
	if err != nil {
		t.Fatal(err)
	}
	dest := make([]driver.Value, 1)
	if err := dr.Next(dest); err != nil || dest[0] != int64(1) {
		t.Fatalf("first row %v, %v", dest[0], err)
	}
	if err := dr.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release the response")
	}
}

func TestBindConversions(t *testing.T) {
	f := newFake(t, true, answerRows(bookColumns[:1], nil, 0, 1))
	db := openDB(t, f.config())
	at := time.Date(2024, 2, 29, 23, 30, 0, 5, time.FixedZone("", -3600))
	if _, err := db.Exec("SELECT ?, ?, ?, ?, ?", true, false, at, int8(-3), float32(0.5)); err != nil {
		t.Fatal(err)
	}
	if want := `"params":[1,0,"2024-03-01 00:30:00.000000005",-3,5e-1]`; !strings.Contains(f.lastCalls()[0].body, want) {
		t.Fatalf("body %s; want %s", f.lastCalls()[0].body, want)
	}
	refused := []struct {
		name string
		args []any
	}{
		{"NaN", []any{math.NaN()}},
		{"year 10000", []any{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{"year 0", []any{time.Date(0, 12, 31, 23, 0, 0, 0, time.UTC)}},
		{"uint64 above int64", []any{uint64(math.MaxUint64)}},
		{"positional and named", []any{1, sql.Named("a", 2)}},
		{"repeated name", []any{sql.Named("a", 1), sql.Named("a", 2)}},
		{"unsupported type", []any{struct{}{}}},
	}
	before := f.hits.Load()
	for _, r := range refused {
		if _, err := db.Exec("SELECT 1", r.args...); err == nil || errors.Is(err, driver.ErrBadConn) {
			t.Errorf("%s: %v; want a refusal", r.name, err)
		}
	}
	if f.hits.Load() != before {
		t.Fatalf("refused arguments sent %d requests", f.hits.Load()-before)
	}
}

// TestBindConversionsInAnotherZone runs TestBindConversions in a process
// whose local zone is not UTC, so that a time not converted to UTC shows.
func TestBindConversionsInAnotherZone(t *testing.T) {
	if os.Getenv("TZ") == "Asia/Kolkata" {
		t.Skip("already in the other zone")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestBindConversions$", "-test.count=1") //nolint:gosec // the test binary itself
	cmd.Env = append(os.Environ(), "TZ=Asia/Kolkata")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestDateColumns(t *testing.T) {
	cols := []wire.Column{{Name: "d", DeclType: "DATE"}, {Name: "dt", DeclType: "datetime"}, {Name: "ts", DeclType: " Timestamp "}, {Name: "t", DeclType: "TEXT"}, {Name: "n", DeclType: "DATE"}}
	f := newFake(t, true, answerRows(cols, [][]any{
		{"2024-01-02", "2024-01-02T03:04:05.5+01:00", "2024-01-02 03:04Z", "2024-01-02", int64(5)},
		{"tomorrow", "2024-02-30", "2024-01-02 03:04:05.1234567890", "x", 1.5},
	}, 0, 1))
	db := openDB(t, f.config())
	rs, err := db.Query("SELECT d, dt, ts, t, n FROM x")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rs.Close() }()
	var got []string
	for rs.Next() {
		v := make([]any, 5)
		p := make([]any, 5)
		for i := range v {
			p[i] = &v[i]
		}
		if err := rs.Scan(p...); err != nil {
			t.Fatal(err)
		}
		for _, x := range v {
			got = append(got, fmt.Sprintf("%T:%v", x, x))
		}
	}
	want := "time.Time:2024-01-02 00:00:00 +0000 UTC time.Time:2024-01-02 02:04:05.5 +0000 UTC time.Time:2024-01-02 03:04:00 +0000 UTC string:2024-01-02 int64:5 " +
		"string:tomorrow string:2024-02-30 string:2024-01-02 03:04:05.1234567890 string:x float64:1.5"
	if strings.Join(got, " ") != want {
		t.Fatalf("values\n%s\nwant\n%s", strings.Join(got, " "), want)
	}
}

func TestParseTime(t *testing.T) {
	valid := map[string]string{
		"0001-01-01":                    "0001-01-01T00:00:00Z",
		"9999-12-31 23:59:59.999999999": "9999-12-31T23:59:59.999999999Z",
		"2024-02-29T12:00":              "2024-02-29T12:00:00Z",
		"2024-01-01 00:00:00.1":         "2024-01-01T00:00:00.1Z",
		"2024-01-01 00:30-01:30":        "2024-01-01T02:00:00Z",
		"2024-01-01T00:00:00+23:59":     "2023-12-31T00:01:00Z",
		"2024-01-01 10:00:00Z":          "2024-01-01T10:00:00Z",
	}
	for s, want := range valid {
		if got, ok := parseTime(s); !ok || got.Format(time.RFC3339Nano) != want || got.Location() != time.UTC {
			t.Errorf("parseTime(%q) = %v, %v; want %s", s, got, ok, want)
		}
	}
	for _, s := range []string{
		"", "2024", "2024-01", "0000-01-01", "2024-13-01", "2024-00-01", "2024-01-00", "2023-02-29", "2024-01-1",
		"2024/01/01", "2024-01-01Z", "2024-01-01 ", "2024-01-01x10:00", "2024-01-01 24:00", "2024-01-01 10:60",
		"2024-01-01 10:00:60", "2024-01-01 10", "2024-01-01 10:00:", "2024-01-01 10:00:00.", "2024-01-01 10:00:00.1234567890",
		"2024-01-01 10:00+01", "2024-01-01 10:00+24:00", "2024-01-01 10:00+01:60", "2024-01-01 10:00Z ", "+2024-01-01",
		"2024-01-01 1a:00", "2024-01-01 10:00:0a",
	} {
		if got, ok := parseTime(s); ok {
			t.Errorf("parseTime(%q) = %v; want refused", s, got)
		}
	}
}

func TestNewConnectorRefuses(t *testing.T) {
	tok := TokenFunc(func(context.Context) (string, error) { return "t", nil })
	ok := Config{Endpoint: "https://db.example/prefix/", Database: "library", Token: tok}
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"user info", func(c *Config) { c.Endpoint = "https://alice:hunter2@db.example/" }},
		{"query", func(c *Config) { c.Endpoint = "https://db.example/?token=hunter2" }},
		{"empty query", func(c *Config) { c.Endpoint = "https://db.example/?" }},
		{"fragment", func(c *Config) { c.Endpoint = "https://db.example/#hunter2" }},
		{"empty fragment", func(c *Config) { c.Endpoint = "https://db.example/#" }},
		{"plaintext without opt-in", func(c *Config) { c.Endpoint = "http://db.example/" }},
		{"other scheme", func(c *Config) { c.Endpoint = "ftp://db.example/" }},
		{"no host", func(c *Config) { c.Endpoint = "https:///x" }},
		{"relative", func(c *Config) { c.Endpoint = "db.example/x" }},
		{"opaque", func(c *Config) { c.Endpoint = "https:db.example" }},
		{"unparsable", func(c *Config) { c.Endpoint = "https://hunter2\x7f/" }},
		{"dot segment", func(c *Config) { c.Endpoint = "https://db.example/a/./b" }},
		{"encoded dot-dot segment", func(c *Config) { c.Endpoint = "https://db.example/a/%2e%2E" }},
		{"bad database", func(c *Config) { c.Database = "Library" }},
		{"no database", func(c *Config) { c.Database = "" }},
		{"no token", func(c *Config) { c.Token = nil }},
		{"TLS and client", func(c *Config) { c.TLSConfig, c.HTTPClient = &tls.Config{MinVersion: tls.VersionTLS12}, &http.Client{} }},
		{"timeout below 1 ms", func(c *Config) { c.StatementTimeout = time.Microsecond }},
		{"timeout negative", func(c *Config) { c.StatementTimeout = -time.Second }},
		{"timeout too large", func(c *Config) { c.StatementTimeout = (math.MaxInt32 + 1) * time.Millisecond }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ok
			tt.edit(&cfg)
			_, err := NewConnector(cfg)
			if err == nil {
				t.Fatal("NewConnector accepted it")
			}
			if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "db.example") {
				t.Fatalf("error %q echoes the endpoint", err)
			}
		})
	}
	c, err := NewConnector(ok)
	if err != nil || c.query != "https://db.example/prefix/v1/databases/library/query" || c.describe != "https://db.example/prefix/v1/databases/library/describe" {
		t.Fatalf("NewConnector = %+v, %v", c, err)
	}
	ok.AllowPlaintext, ok.Endpoint = true, "http://db.example"
	if c, err := NewConnector(ok); err != nil || c.query != "http://db.example/v1/databases/library/query" {
		t.Fatalf("plaintext with opt-in: %+v, %v", c, err)
	}
}

func TestPlaintextOnlyWithOptIn(t *testing.T) {
	f := newFake(t, false, answerRows(bookColumns[:1], [][]any{{int64(1)}}, 0, 1))
	cfg := f.config()
	var id int64
	if err := openDB(t, cfg).QueryRow("SELECT id FROM books").Scan(&id); err != nil || id != 1 {
		t.Fatalf("plaintext with opt-in: %d, %v", id, err)
	}
	cfg.AllowPlaintext = false
	if _, err := NewConnector(cfg); err == nil {
		t.Fatal("plaintext accepted without opt-in")
	}
}

func TestRedirectsRefused(t *testing.T) {
	var elsewhere atomic.Int32
	other := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { elsewhere.Add(1) }))
	defer other.Close()
	f := newFake(t, true, func(w http.ResponseWriter, _ *wire.Writer, _ call, r *http.Request) {
		http.Redirect(w, r, other.URL+r.URL.Path, http.StatusTemporaryRedirect) //nolint:gosec // the test server redirects to its own peer
	})
	withClient := f.config()
	withClient.TLSConfig, withClient.HTTPClient = nil, f.srv.Client() // a client that would follow redirects
	for name, cfg := range map[string]Config{"own client": f.config(), "caller's client": withClient} {
		_, err := openDB(t, cfg).Query("SELECT 1")
		if !errors.Is(err, errRedirect) || errors.Is(err, driver.ErrBadConn) {
			t.Errorf("%s: %v; want the redirect refused", name, err)
		}
	}
	if elsewhere.Load() != 0 {
		t.Fatal("a request followed the redirect")
	}
}

func TestSecretsNeverEchoed(t *testing.T) {
	f := newFake(t, true, answerRows(bookColumns[:1], nil, 0, 1))
	wrong := f.config()
	wrong.Token = TokenFunc(func(context.Context) (string, error) { return "eyJ.wrong-hunter2.sig", nil })
	_, err := openDB(t, wrong).Query("SELECT 1")
	var se *Error
	if !errors.As(err, &se) || se.Code != CodeUnauthenticated || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("wrong token: %v", err)
	}
	before := f.hits.Load()
	for _, bad := range []string{"eyJ.hunter2\r\nX-Injected: 1", "hunter2 x", ""} {
		cfg := f.config()
		cfg.Token = TokenFunc(func(context.Context) (string, error) { return bad, nil })
		_, err := openDB(t, cfg).Query("SELECT 1")
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("token %q: %v", bad, err)
		}
	}
	if f.hits.Load() != before {
		t.Fatal("an unusable token was sent")
	}
	dsn := "https://alice:hunter2@db.example/?database=library" //nolint:gosec // a fake secret the error must not echo
	if _, err := (Driver{}).Open(dsn); !errors.Is(err, ErrNoDSN) || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("Driver.Open: %v", err)
	}
	db, err := sql.Open("skylite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); !errors.Is(err, ErrNoDSN) {
		t.Fatalf("sql.Open then Ping: %v", err)
	}
}

// vector is one line of clients/conformance/values.ndjson; its README
// describes the format.
type vector struct {
	Name       string  `json:"name"`
	Wire       string  `json:"wire"`
	WireHex    string  `json:"wire_hex"`
	Class      string  `json:"class"`
	Value      *string `json:"value"`
	DecodeOnly bool    `json:"decode_only"`
	Error      string  `json:"error"`
	Repeat     int     `json:"repeat"`
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	fh, err := os.Open("../clients/conformance/values.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	var vs []vector
	sc := bufio.NewScanner(fh)
	sc.Buffer(nil, 8<<20)
	for sc.Scan() {
		var v vector
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		if v.Repeat > 0 {
			v.Wire = `"` + strings.Repeat(v.Wire[1:len(v.Wire)-1], v.Repeat) + `"`
			s := strings.Repeat(*v.Value, v.Repeat)
			v.Value = &s
		}
		if v.WireHex != "" {
			b, _ := hex.DecodeString(v.WireHex)
			v.Wire = string(b)
		}
		vs = append(vs, v)
	}
	if len(vs) < 100 {
		t.Fatalf("read %d vectors", len(vs))
	}
	return vs
}

// goValue is a vector's value as the driver binds and returns it.
func goValue(t *testing.T, v vector) any {
	t.Helper()
	if v.Value == nil {
		return nil
	}
	switch v.Class {
	case "integer":
		n, err := strconv.ParseInt(*v.Value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	case "real":
		bits, err := strconv.ParseUint(*v.Value, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		return math.Float64frombits(bits)
	}
	b, err := hex.DecodeString(*v.Value)
	if err != nil {
		t.Fatal(err)
	}
	if v.Class == "text" {
		return string(b)
	}
	return b
}

func same(a, b any) bool {
	if x, ok := a.(float64); ok {
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	}
	return fmt.Sprintf("%T %x", a, a) == fmt.Sprintf("%T %x", b, b)
}

// TestConformanceVectors sends every vector through the driver: each wire
// form in a row it must decode, or refuse, and each canonical value as a
// parameter whose encoding must be the vector's exact bytes.
func TestConformanceVectors(t *testing.T) {
	var row atomic.Value
	f := newFake(t, true, func(w http.ResponseWriter, ww *wire.Writer, _ call, _ *http.Request) {
		_ = ww.Columns([]wire.Column{{Name: "v"}})
		_, _ = io.WriteString(w, "["+row.Load().(string)+"]\n")
		_, _ = io.WriteString(w, `{"done":{"txn":1,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}`+"\n")
	})
	db := openDB(t, f.config())
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			row.Store(v.Wire)
			var got any
			err := db.QueryRow("SELECT v").Scan(&got)
			if v.Error != "" {
				if strings.TrimSpace(v.Wire) != v.Wire {
					return // whitespace around a value is valid inside a row
				}
				if !errors.Is(err, wire.ErrMalformed) {
					t.Fatalf("decoded %v, %v; want it refused", got, err)
				}
				return
			}
			want := goValue(t, v)
			if err != nil || !same(got, want) {
				t.Fatalf("decoded %T %v, %v; want %T %v", got, got, err, want, want)
			}
			if v.DecodeOnly {
				return
			}
			if err := db.QueryRow("SELECT ?", want).Scan(&got); err != nil {
				t.Fatal(err)
			}
			calls := f.lastCalls()
			if body, w := calls[len(calls)-1].body, `"params":[`+v.Wire+`]`; !strings.Contains(body, w) {
				t.Fatalf("encoded body %.200s; want %.200s", body, w)
			}
		})
	}
}
