package wire

import (
	"bytes"
	"errors"
	"io"
	"math"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestPathIsRouteInverse(t *testing.T) {
	for k := KindQuery; k <= KindPut; k++ {
		method := http.MethodPost
		switch k {
		case KindStatus:
			method = http.MethodGet
		case KindPut:
			method = http.MethodPut
		}
		p := Path(k, "library")
		got, db, err := Route(method, p)
		if err != nil || got != k || db != "library" {
			t.Errorf("Route(%s, %q) = %d, %q, %v; want %d", method, p, got, db, err, k)
		}
	}
	if p := Path(KindQuery, "Library"); p != "" {
		t.Errorf("Path with an invalid name = %q", p)
	}
	if p := Path(0, "library"); p != "" {
		t.Errorf("Path of kind 0 = %q", p)
	}
}

func TestAppendCallParses(t *testing.T) {
	tests := []struct {
		name string
		kind Kind
		call Call
		want string
		args []any
	}{
		{
			"query positional", KindQuery,
			Call{SQL: "SELECT ?, ?", Params: []any{int64(78), "Dune"}, MinTxn: 1210, TimeoutMS: 1500},
			`{"sql":"SELECT ?, ?","params":[78,"Dune"],"min_txn":1210,"timeout_ms":1500}`,
			[]any{int64(78), "Dune"},
		},
		{
			"query named", KindQuery,
			Call{SQL: "SELECT :t, @b", Named: []NamedArg{{"t", 1.5}, {"b", []byte{1}}}},
			`{"sql":"SELECT :t, @b","named":{"t":1.5e0,"b":{"blob":"AQ=="}}}`,
			[]any{1.5, []byte{1}},
		},
		{"query bare", KindQuery, Call{SQL: "SELECT \"a\"\n"}, `{"sql":"SELECT \"a\"\n"}`, nil},
		{"describe", KindDescribe, Call{SQL: "SELECT 1", MinTxn: 3}, `{"sql":"SELECT 1","min_txn":3}`, nil},
		{"open", KindOpen, Call{Mode: ModeWrite}, `{"mode":"write"}`, nil},
		{"open read-only", KindOpen, Call{Mode: ModeReadOnly, MinTxn: 5}, `{"mode":"read_only","min_txn":5}`, nil},
		{
			"execute", KindChannel,
			Call{Channel: "w1.a-b_c", Seq: 14, Op: OpExecute, SQL: "UPDATE t SET a = ?", Params: []any{int64(1)}, MinTxn: 9, TimeoutMS: 20},
			`{"channel":"w1.a-b_c","seq":14,"op":"execute","sql":"UPDATE t SET a = ?","params":[1],"min_txn":9,"timeout_ms":20}`,
			[]any{int64(1)},
		},
		{"begin", KindChannel, Call{Channel: "w1.x", Seq: 1, Op: OpBegin, MinTxn: 3}, `{"channel":"w1.x","seq":1,"op":"begin","min_txn":3}`, nil},
		{"commit", KindChannel, Call{Channel: "w1.x", Seq: 2, Op: OpCommit}, `{"channel":"w1.x","seq":2,"op":"commit"}`, nil},
		{"rollback", KindChannel, Call{Channel: "w1.x", Seq: 3, Op: OpRollback}, `{"channel":"w1.x","seq":3,"op":"rollback"}`, nil},
		{"cancel", KindCancel, Call{Channel: "w1.x", Seq: 3}, `{"channel":"w1.x","seq":3}`, nil},
		{"close", KindClose, Call{Channel: "w1.x"}, `{"channel":"w1.x"}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := AppendCall(nil, tt.kind, &tt.call)
			if err != nil || string(b) != tt.want {
				t.Fatalf("AppendCall = %s, %v; want %s", b, err, tt.want)
			}
			r, perr := Parse(tt.kind, b, 10)
			if perr != nil {
				t.Fatalf("Parse: %v", perr)
			}
			if e := r.Validate(); e != nil || r.SQL != tt.call.SQL || r.MinTxn != tt.call.MinTxn || r.TimeoutMS != tt.call.TimeoutMS ||
				r.Channel != tt.call.Channel || r.Seq != tt.call.Seq || r.Op != tt.call.Op || r.Mode != tt.call.Mode {
				t.Fatalf("Parse = %+v", r)
			}
			names := []string{"", ""}
			if len(tt.call.Named) > 0 {
				names = []string{":t", "@b"}
			}
			var got []any
			_ = r.Bind(r.Args, names[:len(tt.args)], func(_ int, v any) error { got = append(got, v); return nil })
			if !reflect.DeepEqual(got, tt.args) {
				t.Fatalf("bound %v; want %v", got, tt.args)
			}
		})
	}
}

func TestAppendCallRefuses(t *testing.T) {
	tests := []struct {
		name string
		kind Kind
		call Call
	}{
		{"channel request without op", KindChannel, Call{Channel: "w1.x", Seq: 1, SQL: "SELECT 1"}},
		{"batch", KindChannel, Call{Channel: "w1.x", Seq: 1, Op: OpBatch, SQL: "SELECT 1"}},
		{"resolve", KindResolve, Call{Channel: "w1.x", Seq: 1}},
		{"execute without channel", KindChannel, Call{Seq: 1, Op: OpExecute, SQL: "SELECT 1"}},
		{"execute without seq", KindChannel, Call{Channel: "w1.x", Op: OpExecute, SQL: "SELECT 1"}},
		{"execute without sql", KindChannel, Call{Channel: "w1.x", Seq: 1, Op: OpExecute}},
		{"commit with sql", KindChannel, Call{Channel: "w1.x", Seq: 1, Op: OpCommit, SQL: "SELECT 1"}},
		{"rollback with timeout", KindChannel, Call{Channel: "w1.x", Seq: 1, Op: OpRollback, TimeoutMS: 1}},
		{"negative seq", KindCancel, Call{Channel: "w1.x", Seq: -1}},
		{"bad token", KindClose, Call{Channel: "w1 x"}},
		{"close with seq", KindClose, Call{Channel: "w1.x", Seq: 1}},
		{"open without mode", KindOpen, Call{}},
		{"write open with min_txn", KindOpen, Call{Mode: ModeWrite, MinTxn: 1}},
		{"query with channel", KindQuery, Call{SQL: "SELECT 1", Channel: "w1.x"}},
		{"empty sql", KindQuery, Call{}},
		{"NUL in sql", KindQuery, Call{SQL: "SELECT 1\x00"}},
		{"invalid UTF-8 sql", KindQuery, Call{SQL: "SELECT '\xff'"}},
		{"params and named", KindQuery, Call{SQL: "SELECT ?", Params: []any{int64(1)}, Named: []NamedArg{{"a", int64(1)}}}},
		{"describe params", KindDescribe, Call{SQL: "SELECT ?", Params: []any{int64(1)}}},
		{"describe timeout", KindDescribe, Call{SQL: "SELECT 1", TimeoutMS: 1}},
		{"negative min_txn", KindQuery, Call{SQL: "SELECT 1", MinTxn: -1}},
		{"timeout too large", KindQuery, Call{SQL: "SELECT 1", TimeoutMS: math.MaxInt32 + 1}},
		{"negative timeout", KindQuery, Call{SQL: "SELECT 1", TimeoutMS: -1}},
		{"NaN", KindQuery, Call{SQL: "SELECT ?", Params: []any{math.NaN()}}},
		{"named NaN", KindQuery, Call{SQL: "SELECT :a", Named: []NamedArg{{"a", math.NaN()}}}},
		{"not a value", KindQuery, Call{SQL: "SELECT ?", Params: []any{1}}},
		{"empty name", KindQuery, Call{SQL: "SELECT :a", Named: []NamedArg{{"", int64(1)}}}},
		{"repeated name", KindQuery, Call{SQL: "SELECT :a", Named: []NamedArg{{"a", int64(1)}, {"a", int64(2)}}}},
		{"invalid name", KindQuery, Call{SQL: "SELECT :a", Named: []NamedArg{{"\xff", int64(1)}}}},
	}
	for _, tt := range tests {
		if b, err := AppendCall([]byte("x"), tt.kind, &tt.call); err == nil || string(b) != "x" {
			t.Errorf("%s: AppendCall = %q, %v; want an error and dst unchanged", tt.name, b, err)
		}
	}
}

// readAll reads every frame of body.
func readAll(body string, k DoneKind, maxFrame int) ([]Frame, error) {
	r, err := NewReader(strings.NewReader(body), k, maxFrame)
	if err != nil {
		return nil, err
	}
	var frames []Frame
	for {
		f, err := r.Next()
		if errors.Is(err, io.EOF) {
			return frames, nil
		}
		if err != nil {
			return frames, err
		}
		c := *f
		c.Row = append([]any(nil), f.Row...)
		frames = append(frames, c)
	}
}

func TestReaderReadsWhatWriterWrites(t *testing.T) {
	var buf bytes.Buffer
	l := DefaultLimits()
	w, _ := NewWriter(&buf, DefaultFrameBytes, l)
	cols := []Column{{Name: "id", DeclType: "INTEGER", Database: "main", Table: "books", Origin: "id"}, {Name: "x"}}
	row := []any{int64(-9), "é\x00", []byte{}, nil}
	_ = w.Keepalive()
	_ = w.Columns(cols[:2])
	_ = w.Row(row[:2])
	_ = w.Keepalive()
	_ = w.Row([]any{1.5, nil})
	_ = w.Done(&Done{Kind: DoneQuery, Txn: 1210, ReadOnly: true, RowsAffected: 3, LastInsertRowID: -4})
	frames, err := readAll(buf.String(), DoneQuery, 1<<10)
	if err != nil {
		t.Fatal(err)
	}
	want := []Frame{
		{Kind: FrameColumns, Columns: cols[:2]},
		{Kind: FrameRow, Row: row[:2]},
		{Kind: FrameRow, Row: []any{1.5, nil}},
		{Kind: FrameDone, Done: &Done{Kind: DoneQuery, Txn: 1210, ReadOnly: true, RowsAffected: 3, LastInsertRowID: -4}},
	}
	if !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames\n%+v\nwant\n%+v", frames, want)
	}

	// A describe answer with more names than one frame holds.
	buf.Reset()
	w, _ = NewWriter(&buf, DefaultFrameBytes, l)
	names := make([]string, 1500)
	names[0], names[1499] = ":t", "$z"
	_ = w.Columns(nil)
	_ = w.Params(names)
	_ = w.Done(&Done{Kind: DoneDescribe, Txn: 9, ParamCount: 1500})
	frames, err = readAll(buf.String(), DoneDescribe, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range frames[1 : len(frames)-1] {
		got = append(got, f.Params...)
	}
	if len(frames) != 4 || !reflect.DeepEqual(got, names) || frames[3].Done.ParamCount != 1500 || frames[3].Done.Txn != 9 {
		t.Fatalf("describe frames %d, names equal %v, done %+v", len(frames), reflect.DeepEqual(got, names), frames[len(frames)-1].Done)
	}

	// An error with every field.
	buf.Reset()
	w, _ = NewWriter(&buf, DefaultFrameBytes, l)
	bi := int64(3)
	e := &Error{
		Code: CodeSQLError, NotExecuted: true, Final: true, Message: "no such table: t", RequestID: "r-1", ChannelID: "7c1d",
		Seq: 4, TxnState: TxnNone, SQLiteCode: 1, SQLiteExtendedCode: 257, BatchIndex: &bi, Cause: "maintenance", Txn: 8, Epoch: 2,
	}
	_ = w.Error(e)
	frames, err = readAll(buf.String(), DoneQuery, 1<<10)
	if err != nil || len(frames) != 1 || !reflect.DeepEqual(frames[0].Error, e) {
		t.Fatalf("error frames %+v, %v; want %+v", frames, err, e)
	}
}

func TestReaderRefuses(t *testing.T) {
	const (
		cols  = `{"columns":[{"name":"a"}]}` + "\n"
		qdone = `{"done":{"txn":1,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}` + "\n"
		ddone = `{"done":{"txn":1,"param_count":1}}` + "\n"
	)
	tests := []struct {
		name string
		kind DoneKind
		body string
		eof  bool // io.ErrUnexpectedEOF rather than ErrMalformed
	}{
		{"empty body", DoneQuery, "", true},
		{"no terminal frame", DoneQuery, cols + "[1]\n", true},
		{"frame without newline", DoneQuery, cols + strings.TrimSuffix(qdone, "\n"), true},
		{"empty frame", DoneQuery, "\n" + qdone, false},
		{"not JSON", DoneQuery, "nope\n", false},
		{"string frame", DoneQuery, "\"x\"\n", false},
		{"empty object", DoneQuery, "{}\n", false},
		{"row before columns", DoneQuery, "[1]\n" + qdone, false},
		{"columns twice", DoneQuery, cols + cols + qdone, false},
		{"row too wide", DoneQuery, cols + "[1,2]\n" + qdone, false},
		{"row too narrow", DoneQuery, cols + "[]\n" + qdone, false},
		{"bad value", DoneQuery, cols + "[1e999]\n" + qdone, false},
		{"unknown value tag", DoneQuery, cols + `[{"date":"x"}]` + "\n" + qdone, false},
		{"boolean value", DoneQuery, cols + "[true]\n" + qdone, false},
		{"two members", DoneQuery, `{"done":{"txn":1,"read_only":true,"rows_affected":0,"last_insert_rowid":0},"x":1}` + "\n", false},
		{"unknown frame with two members", DoneQuery, `{"future":1,"x":1}` + "\n" + qdone, false},
		{"duplicate member", DoneQuery, `{"done":{"txn":1,"txn":2,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}` + "\n", false},
		{"missing done member", DoneQuery, `{"done":{"txn":1,"read_only":true,"rows_affected":0}}` + "\n", false},
		{"txn zero", DoneQuery, `{"done":{"txn":0,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}` + "\n", false},
		{"txn not an integer", DoneQuery, `{"done":{"txn":1.0,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}` + "\n", false},
		{"read_only not a boolean", DoneQuery, `{"done":{"txn":1,"read_only":1,"rows_affected":0,"last_insert_rowid":0}}` + "\n", false},
		{"done not an object", DoneQuery, `{"done":1}` + "\n", false},
		{"bytes after the terminal frame", DoneQuery, qdone + "\n", false},
		{"params in a query", DoneQuery, cols + `{"params":{"from":0,"names":["a"]}}` + "\n" + qdone, false},
		{"results in a query", DoneQuery, `{"results":{"from":0,"entries":[{"rows_affected":1,"last_insert_rowid":1}]}}` + "\n" + qdone, false},
		{"row in a describe", DoneDescribe, cols + "[1]\n" + `{"done":{"txn":1,"param_count":0}}` + "\n", false},
		{"columns in a commit", DoneCommit, cols + `{"done":{"seq":1,"txn_state":"none","txn":1,"committed":true,"wrote":true}}` + "\n", false},
		{"committed false", DoneCommit, `{"done":{"seq":1,"txn_state":"none","txn":1,"committed":false,"wrote":true}}` + "\n", false},
		{"bad txn_state", DoneRollback, `{"done":{"seq":1,"txn_state":"closed","txn":1}}` + "\n", false},
		{"execute without seq", DoneExecute, `{"done":{"txn_state":"none","txn":1,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}` + "\n", false},
		{"open without token", DoneOpen, `{"done":{"txn":1,"channel":{"id":"c"}}}` + "\n", false},
		{"open with a bad token", DoneOpen, `{"done":{"txn":1,"channel":{"token":"a b","id":"c"}}}` + "\n", false},
		{"describe without columns", DoneDescribe, `{"done":{"txn":1,"param_count":0}}` + "\n", false},
		{"params before columns", DoneDescribe, `{"params":{"from":0,"names":["a"]}}` + "\n" + cols + ddone, false},
		{"params count short", DoneDescribe, cols + ddone, false},
		{"params count long", DoneDescribe, cols + `{"params":{"from":0,"names":["a",null]}}` + "\n" + ddone, false},
		{"params gap", DoneDescribe, cols + `{"params":{"from":1,"names":["a"]}}` + "\n" + ddone, false},
		{"params empty", DoneDescribe, cols + `{"params":{"from":0,"names":[]}}` + "\n" + `{"done":{"txn":1,"param_count":0}}` + "\n", false},
		{"params without names", DoneDescribe, cols + `{"params":{"from":0}}` + "\n" + ddone, false},
		{"param name a number", DoneDescribe, cols + `{"params":{"from":0,"names":[1]}}` + "\n" + ddone, false},
		{"column without a name", DoneQuery, `{"columns":[{"decltype":null}]}` + "\n" + qdone, false},
		{"column name null", DoneQuery, `{"columns":[{"name":null}]}` + "\n" + qdone, false},
		{"columns not an array", DoneQuery, `{"columns":{}}` + "\n" + qdone, false},
		{"error without request_id", DoneQuery, `{"error":{"code":"busy","not_executed":true,"final":true,"message":""}}` + "\n", false},
		{"error sqlite_code range", DoneQuery, `{"error":{"code":"busy","not_executed":true,"final":true,"message":"","request_id":"r","sqlite_code":256}}` + "\n", false},
		{"frame too large", DoneQuery, `{"columns":[{"name":"` + strings.Repeat("a", 200) + `"}]}` + "\n" + qdone, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readAll(tt.body, tt.kind, 128)
			want := ErrMalformed
			if tt.eof {
				want = io.ErrUnexpectedEOF
			}
			if !errors.Is(err, want) {
				t.Fatalf("err = %v; want %v", err, want)
			}
		})
	}
}

func TestReaderReadsChannelAnswers(t *testing.T) {
	l := DefaultLimits()
	for _, d := range []*Done{
		{Kind: DoneOpen, Txn: 7, Channel: &Channel{Token: "w1.x", ID: "c1", Mode: ModeWrite, Epoch: 2, Limits: l}},
		{Kind: DoneBegin, Seq: 1, TxnState: TxnOpen, Txn: 7},
		{Kind: DoneExecute, Seq: 2, TxnState: TxnAborted, Txn: 7, RowsAffected: 1, LastInsertRowID: -3, DataOmitted: true},
		{Kind: DoneCommit, Seq: 3, TxnState: TxnNone, Txn: 8, Wrote: true},
		{Kind: DoneRollback, Seq: 4, TxnState: TxnNone, Txn: 8},
	} {
		var buf bytes.Buffer
		w, _ := NewWriter(&buf, DefaultFrameBytes, l)
		if err := w.Done(d); err != nil {
			t.Fatal(err)
		}
		frames, err := readAll(buf.String(), d.Kind, 1<<10)
		want := *d
		if want.Channel != nil {
			c := *want.Channel
			c.Limits = Limits{} // skipped
			want.Channel = &c
		}
		if err != nil || len(frames) != 1 || !reflect.DeepEqual(frames[0].Done, &want) {
			t.Fatalf("%s: frames %+v, %v; want %+v", buf.String(), frames, err, want)
		}
	}
}

// TestReaderBoundsParamsWhileParsing checks that a params frame is refused at
// its 1,025th name, before that name is parsed: the name here is not a string,
// which would be the error if it were.
func TestReaderBoundsParamsWhileParsing(t *testing.T) {
	names := strings.Repeat(`"a",`, 1024) + "1"
	body := `{"columns":[]}` + "\n" + `{"params":{"from":0,"names":[` + names + `]}}` + "\n"
	_, err := readAll(body, DoneDescribe, 1<<20)
	if !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), "1 to 1024 entries") {
		t.Fatalf("err = %v; want the 1,024-entry bound", err)
	}
}

func TestReaderSkipsWhatItDoesNotKnow(t *testing.T) {
	body := `{"future":{"x":[1]}}` + "\n" +
		`{"columns":[{"name":"a","decltype":null,"extra":true}]}` + "\n" +
		`{"keepalive":{}}` + "\n" +
		`[1]` + "\n" +
		`{"done":{"txn":5,"read_only":false,"rows_affected":0,"last_insert_rowid":0,"later":{"y":1}}}` + "\n"
	frames, err := readAll(body, DoneQuery, 1<<10)
	if err != nil || len(frames) != 3 || frames[2].Done.Txn != 5 || frames[0].Columns[0].Name != "a" {
		t.Fatalf("frames %+v, %v", frames, err)
	}
}

func TestNewReaderRefuses(t *testing.T) {
	if _, err := NewReader(strings.NewReader(""), DoneBatch, 1<<10); err == nil {
		t.Error("NewReader accepted a done kind it does not read")
	}
	if _, err := NewReader(strings.NewReader(""), DoneQuery, 1); err == nil {
		t.Error("NewReader accepted a frame size of 1")
	}
}

// TestReaderRefusesAWideRowEarly checks that a row wider than its columns is
// refused at the first extra value, not after the whole row is decoded.
func TestReaderRefusesAWideRowEarly(t *testing.T) {
	body := `{"columns":[{"name":"a"}]}` + "\n[" + strings.Repeat("1000,", 400_000) + "1000]\n"
	r, err := NewReader(strings.NewReader(body), DoneQuery, len(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = r.Next()
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("a row of 400,001 values for 1 column: %v; want ErrMalformed", err)
	}
	// Buffering, checking and decoding the frame take about 5 times its size;
	// decoding every value of the row took 24 times.
	if got, limit := after.TotalAlloc-before.TotalAlloc, 8*uint64(len(body)); got > limit {
		t.Fatalf("reading the row allocated %d bytes; want at most %d", got, limit)
	}
}

func TestReaderBoundsTheTerminalFrame(t *testing.T) {
	const cols = `{"columns":[{"name":"a"}]}` + "\n"
	done := func(size int) string { // a done frame of size bytes, newline included
		head, tail := `{"done":{"txn":1,"read_only":true,"rows_affected":0,"last_insert_rowid":0,"x":"`, `"}}`+"\n"
		return head + strings.Repeat("y", size-len(head)-len(tail)) + tail
	}
	if _, err := readAll(cols+done(4096), DoneQuery, 1<<20); err != nil {
		t.Fatalf("a done frame of 4,096 bytes: %v", err)
	}
	if _, err := readAll(cols+done(4097), DoneQuery, 1<<20); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a done frame of 4,097 bytes: %v; want ErrMalformed", err)
	}
	e := `{"error":{"code":"busy","not_executed":true,"final":true,"message":"` + strings.Repeat("m", 5000) + `","request_id":"r"}}` + "\n"
	if _, err := readAll(e, DoneQuery, 1<<20); !errors.Is(err, ErrMalformed) {
		t.Fatalf("an error frame of %d bytes: %v; want ErrMalformed", len(e), err)
	}
}
