package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/go-json-experiment/json/jsontext"
)

func newTestWriter(frameBytes int, l Limits) (*Writer, *bytes.Buffer) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, frameBytes, l)
	if err != nil {
		panic(err)
	}
	return w, &buf
}

func lines(buf *bytes.Buffer) []string {
	l := strings.SplitAfter(buf.String(), "\n")
	return l[:len(l)-1]
}

// TestFrameExamples reproduces ADR 0005's example frames byte for byte.
func TestFrameExamples(t *testing.T) {
	const chID = "7c1d2e3f40a1b2c37c1d2e3f40a1b2c3"
	w, buf := newTestWriter(DefaultFrameBytes, DefaultLimits())
	must(t, w.Results([]Result{{1, 91}, {1, 92}}))
	must(t, w.Done(&Done{Kind: DoneBatch, Seq: 15, TxnState: TxnOpen, Txn: 1210, RowsAffected: 2, LastInsertRowID: 92, Entries: 2}))
	w, _ = newTestWriter(DefaultFrameBytes, DefaultLimits())
	w.w = buf
	must(t, w.Done(&Done{Kind: DoneBatch, Seq: 15, TxnState: TxnOpen, Txn: 1210, RowsAffected: 2, LastInsertRowID: 92, Entries: 2, DataOmitted: true}))
	commit := &Done{Kind: DoneCommit, Seq: 13, TxnState: TxnNone, Txn: 1210, Wrote: true}
	replayed := *commit
	replayed.DataOmitted = true // a replay flag, never part of a receipt
	for _, d := range []*Done{
		{Kind: DoneClose, LastSeq: 13, Outcome: &Receipt{Done: &replayed}},
		{Kind: DoneClose, LastSeq: 0},
		{Kind: DoneCancel, Seq: 14, TxnState: TxnAborted, Outcome: &Receipt{Error: &Error{
			Code: CodeCancelled, Final: true, Message: "cancelled", RequestID: "01JDZ3W4Q8M7",
			ChannelID: chID, Seq: 14, TxnState: TxnAborted,
		}}},
		commit,
		{Kind: DoneResolve, Resolution: ResolutionCommitted, Txn: 1210},
		{Kind: DoneExecute, Seq: 12, TxnState: TxnOpen, Txn: 1209, RowsAffected: 1, LastInsertRowID: 78},
	} {
		w, _ = newTestWriter(DefaultFrameBytes, DefaultLimits())
		w.w = buf
		must(t, w.Done(d))
	}
	w, _ = newTestWriter(DefaultFrameBytes, DefaultLimits())
	w.w = buf
	e := NewError(CodeBusy, "the write lock was not granted within 5000 ms").Recorded(false)
	e.RequestID, e.ChannelID, e.Seq, e.TxnState = "01JDZ3W4Q8M6", chID, 4, TxnNone
	must(t, w.Error(e))
	w, _ = newTestWriter(DefaultFrameBytes, DefaultLimits())
	w.w = buf
	must(t, w.Columns([]Column{{Name: "id", DeclType: "INTEGER", Database: "main", Table: "books", Origin: "id"}}))
	must(t, w.Row([]any{int64(78)}))
	must(t, w.Params([]string{":t", ""}))
	must(t, w.Keepalive())
	want := []string{
		`{"results":{"from":0,"entries":[{"rows_affected":1,"last_insert_rowid":91},{"rows_affected":1,"last_insert_rowid":92}]}}`,
		`{"done":{"seq":15,"txn_state":"open","txn":1210,"rows_affected":2,"last_insert_rowid":92,"entries":2}}`,
		`{"done":{"seq":15,"txn_state":"open","txn":1210,"rows_affected":2,"last_insert_rowid":92,"entries":2,"data_omitted":true}}`,
		`{"done":{"last_seq":13,"outcome":{"seq":13,"ok":true,"txn_state":"none","txn":1210,"committed":true,"wrote":true}}}`,
		`{"done":{"last_seq":0}}`,
		`{"done":{"seq":14,"txn_state":"aborted","outcome":{"seq":14,"ok":false,"error":{"code":"cancelled","not_executed":false,"final":true,"message":"cancelled","request_id":"01JDZ3W4Q8M7","channel_id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","seq":14,"txn_state":"aborted"}}}}`,
		`{"done":{"seq":13,"txn_state":"none","txn":1210,"committed":true,"wrote":true}}`,
		`{"done":{"resolution":"committed","txn":1210}}`,
		`{"done":{"seq":12,"txn_state":"open","txn":1209,"read_only":false,"rows_affected":1,"last_insert_rowid":78}}`,
		`{"error":{"code":"busy","not_executed":true,"final":true,"message":"the write lock was not granted within 5000 ms","request_id":"01JDZ3W4Q8M6","channel_id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","seq":4,"txn_state":"none"}}`,
		`{"columns":[{"name":"id","decltype":"INTEGER","database":"main","table":"books","origin":"id"}]}`,
		`[78]`,
		`{"params":{"from":0,"names":[":t",null]}}`,
		`{"keepalive":{}}`,
	}
	got := lines(buf)
	if len(got) != len(want) {
		t.Fatalf("got %d frames:\n%s", len(got), buf)
	}
	for i := range want {
		if got[i] != want[i]+"\n" {
			t.Errorf("frame %d:\n got %s\nwant %s", i, got[i], want[i])
		}
	}
	// The open answer, whose limits the ADR shortens: every member is present.
	w, bb := newTestWriter(DefaultFrameBytes, DefaultLimits())
	b := *bb
	w.w = &b
	must(t, w.Done(&Done{Kind: DoneOpen, Txn: 1200, Channel: &Channel{Token: "w1.9QvX0aYk3H8...", ID: chID, Mode: ModeWrite, Epoch: 3, Limits: DefaultLimits()}})) //nolint:gosec // the ADR's shortened example token
	if !strings.HasPrefix(b.String(), `{"done":{"txn":1200,"channel":{"token":"w1.9QvX0aYk3H8...","id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","mode":"write","epoch":3,"limits":{"request_body_bytes":16777216,`) {
		t.Fatalf("open answer: %s", b.String())
	}
	var v struct {
		Done struct {
			Channel struct {
				Limits map[string]int64 `json:"limits"`
			} `json:"channel"`
		} `json:"done"`
	}
	if err := json.Unmarshal(b.Bytes(), &v); err != nil || len(v.Done.Channel.Limits) != 20 {
		t.Fatalf("limits %v, %v", v.Done.Channel.Limits, err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func maxInt(n int64) *int64 { return &n }

// largestError is an error with every field at its largest at the default
// limits, its code the longest in the list.
func largestError() *Error {
	return &Error{
		Code: CodeCommitOutcomeUnknown, Message: strings.Repeat("m", 2000),
		RequestID: strings.Repeat("r", 64), ChannelID: strings.Repeat("c", 32), Seq: math.MaxInt64,
		TxnState: TxnAborted, SQLiteCode: 255, SQLiteExtendedCode: math.MaxInt32, BatchIndex: maxInt(9999),
		Cause: "maintenance", Txn: math.MaxInt64, Epoch: math.MaxInt64,
	}
}

// TestTerminalFrameBound checks ADR 0005's sizes: an error frame of at most
// 1,437 bytes, a receipt of 1,473 and a cancel answer, the largest terminal
// frame, of 1,543, within the 4 KiB reserved; and that no other terminal
// frame is larger.
func TestTerminalFrameBound(t *testing.T) {
	l := DefaultLimits()
	size := func(write func(*Writer) error) int {
		t.Helper()
		w, buf := newTestWriter(DefaultFrameBytes, l)
		must(t, write(w))
		return buf.Len()
	}
	for _, code := range []Code{CodeTransactionAborted, CodeStatementForbidden, CodeCommitOutcomeUnknown} {
		if len(code) > len(CodeCommitOutcomeUnknown) {
			t.Fatalf("%s is longer than the code the bound assumes", code)
		}
	}
	errFrame := size(func(w *Writer) error { return w.Error(largestError()) })
	cancel := size(func(w *Writer) error {
		return w.Done(&Done{Kind: DoneCancel, Seq: math.MaxInt64, TxnState: TxnAborted, Outcome: &Receipt{Error: largestError()}})
	})
	var receipt int
	{
		w, _ := newTestWriter(DefaultFrameBytes, l)
		b, err := w.appendReceipt(nil, &Receipt{Error: largestError()})
		must(t, err)
		receipt = len(b)
	}
	if errFrame != 1437 || receipt != 1473 || cancel != 1543 {
		t.Fatalf("error frame %d, receipt %d, cancel answer %d; want 1437, 1473, 1543", errFrame, receipt, cancel)
	}
	maxLimits := Limits{
		RequestBodyBytes: math.MaxInt64, HeaderBlockBytes: math.MaxInt64, BatchEntries: math.MaxInt64,
		FrameEntries: math.MaxInt64, ResultBytes: math.MaxInt64, HeldResultBytes: math.MaxInt64,
		TerminalFrameBytes: math.MaxInt64, ErrorMessageBytes: math.MaxInt64, RetainedRequestBytes: math.MaxInt64,
		ResolveBound: math.MaxInt64, StatementMS: math.MaxInt32, ResponseMS: math.MaxInt32,
		KeepaliveMS: math.MaxInt32, LockWaitMS: math.MaxInt32, CatchUpMS: math.MaxInt32,
		ChannelIdleMS: math.MaxInt32, WriteTxnIdleMS: math.MaxInt32, WriteTxnLifetimeMS: math.MaxInt32,
		ReadTxnIdleMS: math.MaxInt32, ReadTxnLifetimeMS: math.MaxInt32,
	}
	const big = math.MaxInt64
	success := &Receipt{Done: &Done{Kind: DoneBatch, Seq: big, TxnState: TxnAborted, Txn: big, RowsAffected: big, LastInsertRowID: math.MinInt64, Entries: big}}
	for _, d := range []*Done{
		{Kind: DoneQuery, Txn: big, ReadOnly: false, RowsAffected: big, LastInsertRowID: math.MinInt64},
		{Kind: DoneDescribe, Txn: big, ParamCount: math.MaxInt32},
		{Kind: DoneOpen, Txn: big, Channel: &Channel{Token: strings.Repeat("t", 256), ID: strings.Repeat("c", 32), Mode: ModeWrite, Epoch: big, Limits: maxLimits}},
		{Kind: DoneExecute, Seq: big, TxnState: TxnAborted, Txn: big, RowsAffected: big, LastInsertRowID: math.MinInt64, DataOmitted: true},
		success.Done,
		{Kind: DoneCommit, Seq: big, TxnState: TxnAborted, Txn: big, Wrote: false, DataOmitted: true},
		{Kind: DoneCancel, Seq: big, TxnState: TxnAborted, Outcome: success},
		{Kind: DoneClose, LastSeq: big, Outcome: &Receipt{Error: largestError()}},
		{Kind: DoneResolve, Resolution: ResolutionNotCommitted, Txn: big},
		{Kind: DoneStatus, Txn: big, Epoch: big, SQLiteVersion: strings.Repeat("v", 32), Limits: &maxLimits},
		{Kind: DonePut, Txn: big, Epoch: big},
	} {
		if n := size(func(w *Writer) error { return w.Done(d) }); n > cancel {
			t.Errorf("done kind %d: %d bytes, above the cancel answer's %d", d.Kind, n, cancel)
		}
	}
	if int64(cancel) > l.TerminalFrameBytes {
		t.Fatalf("the cancel answer exceeds the terminal frame reservation")
	}
}

func TestMessageCut(t *testing.T) {
	for _, s := range []string{
		"", "short", strings.Repeat("a", 2000), strings.Repeat("é", 1000), strings.Repeat("\x01", 500),
		strings.Repeat("a", 1021) + "é", strings.Repeat("a", 1020) + "😀", strings.Repeat("\"", 600),
		strings.Repeat("\xff", 2000), strings.Repeat("a", 1019) + "\xff\xfe",
	} {
		cut := CutMessage(s, 1024)
		q, err := jsontext.AppendQuote(nil, cut)
		if err != nil || len(q) > 1024 || !utf8.ValidString(cut) {
			t.Fatalf("CutMessage(%.20q) = %d bytes quoted, %v", s, len(q), err)
		}
		if utf8.ValidString(s) {
			if !strings.HasPrefix(s, cut) {
				t.Fatalf("CutMessage(%.20q) is not a prefix", s)
			}
			if rest := s[len(cut):]; rest != "" {
				r, _ := utf8.DecodeRuneInString(rest)
				if len(q)+quotedLen(r) <= 1024 {
					t.Fatalf("CutMessage(%.20q) cut %d bytes early", s, 1024-len(q))
				}
			}
		}
	}
	// The cut applies in an error frame.
	for _, limit := range []int64{1024, 100, 2} {
		l := DefaultLimits()
		l.ErrorMessageBytes = limit
		w, buf := newTestWriter(DefaultFrameBytes, l)
		e := NewError(CodeSQLError, strings.Repeat("x", 5000))
		e.RequestID = "r"
		must(t, w.Error(e))
		var v struct {
			Error struct {
				Message jsontext.Value `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(buf.Bytes(), &v); err != nil || int64(len(v.Error.Message)) != limit {
			t.Fatalf("limit %d: message of %d bytes, %v", limit, len(v.Error.Message), err)
		}
	}
}

// TestEscaping checks the encoder against ADR 0005's escaping rule for
// every character, and quotedLen against the encoder.
func TestEscaping(t *testing.T) {
	short := map[rune]string{'"': `\"`, '\\': `\\`, '\b': `\b`, '\t': `\t`, '\n': `\n`, '\f': `\f`, '\r': `\r`}
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		want, ok := short[r]
		switch {
		case ok:
		case r < 0x20:
			want = fmt.Sprintf(`\u%04x`, r)
		default:
			want = string(r)
		}
		q := string(appendQuoted(nil, string(r)))
		if q != `"`+want+`"` {
			t.Fatalf("U+%04X: encoded as %s; want %q", r, q, `"`+want+`"`)
		}
		if quotedLen(r) != len(want) {
			t.Fatalf("U+%04X: quotedLen %d; want %d", r, quotedLen(r), len(want))
		}
	}
	for in, want := range map[string]string{
		"a\xffb":               "\"a\uFFFDb\"",
		"\xed\xa0\x80":         "\"\uFFFD\uFFFD\uFFFD\"",
		"\uFFFD":               "\"\uFFFD\"",
		"/\u2028\u2029\x7f<>&": "\"/\u2028\u2029\x7f<>&\"",
		"":                     `""`,
	} {
		if got := string(appendQuoted(nil, in)); got != want {
			t.Errorf("appendQuoted(%q) = %q; want %q", in, got, want)
		}
	}
	// The values and the diagnostic strings both follow the rule.
	b, err := AppendValue(nil, "\x00\x1f\x7f\"\u00e9")
	if err != nil || string(b) != `"\u0000\u001f`+"\x7f"+`\"é"` {
		t.Fatalf("AppendValue = %s, %v", b, err)
	}
}

type splitFrame struct {
	Results *struct {
		From    int64 `json:"from"`
		Entries []struct {
			RowsAffected    int64 `json:"rows_affected"`
			LastInsertRowID int64 `json:"last_insert_rowid"`
		} `json:"entries"`
	} `json:"results"`
	Params *struct {
		From  int64     `json:"from"`
		Names []*string `json:"names"`
	} `json:"params"`
}

func decodeFrames(t *testing.T, buf *bytes.Buffer) []splitFrame {
	t.Helper()
	var out []splitFrame
	for _, line := range lines(buf) {
		var f splitFrame
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		out = append(out, f)
	}
	return out
}

func TestFrameSplit(t *testing.T) {
	// By entries: 1,500 then 1,000 entries give frames of 1,024, 476 and
	// 1,000: a call does not fill the frame the previous call ended.
	w, buf := newTestWriter(DefaultFrameBytes, DefaultLimits())
	entries := make([]Result, 2500)
	for i := range entries {
		entries[i] = Result{RowsAffected: 1, LastInsertRowID: int64(i)}
	}
	must(t, w.Results(entries[:1500]))
	must(t, w.Results(entries[1500:]))
	frames := decodeFrames(t, buf)
	var from int64
	var sizes []int
	for _, f := range frames {
		if f.Results.From != from {
			t.Fatalf("frame from %d; want %d", f.Results.From, from)
		}
		for i, e := range f.Results.Entries {
			if e.LastInsertRowID != from+int64(i) {
				t.Fatalf("entry %d is %+v", from+int64(i), e)
			}
		}
		from += int64(len(f.Results.Entries))
		sizes = append(sizes, len(f.Results.Entries))
	}
	if want := []int{1024, 476, 1000}; !equalInts(sizes, want) {
		t.Fatalf("frames of %v entries; want %v", sizes, want)
	}

	// By size: a frame ends before the name that would take it past the frame
	// size, and a name too large for any frame goes alone.
	const frameBytes = 100
	w, buf = newTestWriter(frameBytes, DefaultLimits())
	must(t, w.Columns(nil)) // a describe answer's params follow its columns
	buf.Reset()
	names := []string{":a", ":" + strings.Repeat("b", 40), ":" + strings.Repeat("c", 40), ":" + strings.Repeat("d", 200), "", ":e"}
	must(t, w.Params(names))
	var got [][]*string
	from = 0
	for _, line := range lines(buf) {
		f := decodeFrames(t, bytes.NewBufferString(line))[0]
		if f.Params.From != from {
			t.Fatalf("frame from %d; want %d", f.Params.From, from)
		}
		if len(line) > frameBytes && len(f.Params.Names) > 1 {
			t.Fatalf("frame of %d bytes with %d names", len(line), len(f.Params.Names))
		}
		from += int64(len(f.Params.Names))
		got = append(got, f.Params.Names)
	}
	if want := []int{2, 1, 1, 2}; !equalInts(counts(got), want) || from != int64(len(names)) || got[3][0] != nil {
		t.Fatalf("frames of %v names; want %v", counts(got), want)
	}
	// A frame of exactly the frame size is kept whole.
	w, buf = newTestWriter(len(`{"params":{"from":0,"names":[":a",":b"]}}`+"\n"), DefaultLimits())
	must(t, w.Columns(nil))
	buf.Reset()
	must(t, w.Params([]string{":a", ":b"}))
	if n := len(lines(buf)); n != 1 {
		t.Fatalf("%d frames; want 1", n)
	}
	// One byte less and the second name starts a frame of its own.
	w, buf = newTestWriter(len(`{"params":{"from":0,"names":[":a",":b"]}}`+"\n")-1, DefaultLimits())
	must(t, w.Columns(nil))
	buf.Reset()
	must(t, w.Params([]string{":a", ":b"}))
	if n := len(lines(buf)); n != 2 {
		t.Fatalf("%d frames; want 2", n)
	}
}

func counts(fs [][]*string) []int {
	var n []int
	for _, f := range fs {
		n = append(n, len(f))
	}
	return n
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestResultLimit(t *testing.T) {
	l := DefaultLimits()
	cols := `{"columns":[{"name":"a","decltype":null,"database":null,"table":null,"origin":null}]}` + "\n"
	l.ResultBytes = int64(len(cols) + len("[1]\n"))
	w, buf := newTestWriter(DefaultFrameBytes, l)
	must(t, w.Columns([]Column{{Name: "a"}}))
	must(t, w.Row([]any{int64(1)}))
	err := w.Row([]any{int64(2)})
	var we *Error
	if !errors.As(err, &we) || we.Code != CodeResultTooLarge {
		t.Fatalf("Row past the result limit: %v", err)
	}
	if w.Written() != l.ResultBytes {
		t.Fatalf("written %d", w.Written())
	}
	we.RequestID = "r"
	must(t, w.Error(we.Recorded(false)))
	if got := lines(buf); len(got) != 3 || !strings.HasPrefix(got[2], `{"error":{"code":"result_too_large"`) {
		t.Fatalf("body:\n%s", buf)
	}
	if err := w.Keepalive(); !errors.Is(err, errEnded) {
		t.Fatalf("frame after the terminal frame: %v", err)
	}
	// Every frame before the terminal one counts: keepalives, columns and
	// results frames as well as rows.
	const keepalive = `{"keepalive":{}}` + "\n"
	results := `{"results":{"from":0,"entries":[{"rows_affected":1,"last_insert_rowid":2}]}}` + "\n"
	keep := func(w *Writer) error { return w.Keepalive() }
	for _, tc := range []struct {
		name        string
		limit       int
		first, then func(*Writer) error
	}{
		{"keepalives", len(keepalive), keep, keep},
		{"results", len(results), func(w *Writer) error { return w.Results([]Result{{1, 2}}) }, func(w *Writer) error { return w.Results([]Result{{1, 2}}) }},
		{"columns", len(cols), func(w *Writer) error { return w.Columns([]Column{{Name: "a"}}) }, keep},
	} {
		l := DefaultLimits()
		l.ResultBytes = int64(tc.limit)
		w, buf := newTestWriter(DefaultFrameBytes, l)
		must(t, tc.first(w))
		if w.Written() != int64(tc.limit) || buf.Len() != tc.limit {
			t.Fatalf("%s: written %d", tc.name, w.Written())
		}
		if err := tc.then(w); !errors.As(err, &we) || we.Code != CodeResultTooLarge || buf.Len() != tc.limit {
			t.Fatalf("%s past the result limit: %v, %d bytes", tc.name, err, buf.Len())
		}
	}
}

func TestWriterOrder(t *testing.T) {
	w, _ := newTestWriter(DefaultFrameBytes, DefaultLimits())
	if err := w.Row([]any{nil}); !errors.Is(err, errOrder) {
		t.Fatalf("row before columns: %v", err)
	}
	must(t, w.Keepalive())
	must(t, w.Columns([]Column{{Name: "a"}, {Name: "b"}}))
	if err := w.Columns(nil); !errors.Is(err, errOrder) {
		t.Fatalf("second columns: %v", err)
	}
	if err := w.Row([]any{nil}); !errors.Is(err, errColumns) {
		t.Fatalf("short row: %v", err)
	}
	if err := w.Row([]any{nil, math.NaN()}); err == nil {
		t.Fatal("NaN in a row")
	}
	must(t, w.Row([]any{nil, "x"}))
	must(t, w.Keepalive())
	must(t, w.Results([]Result{{}}))
	if err := w.Row([]any{nil, nil}); !errors.Is(err, errOrder) {
		t.Fatalf("row after results: %v", err)
	}
	must(t, w.Done(&Done{Kind: DoneBatch}))
	if err := w.Done(&Done{Kind: DoneBatch}); !errors.Is(err, errEnded) {
		t.Fatalf("second terminal frame: %v", err)
	}
	// A columns frame above the frame size is refused, and one of exactly
	// the frame size is written.
	frame := `{"columns":[{"name":"nn","decltype":null,"database":null,"table":null,"origin":null}]}` + "\n"
	w, buf := newTestWriter(len(frame)-1, DefaultLimits())
	var we *Error
	if err := w.Columns([]Column{{Name: "nn"}}); !errors.As(err, &we) || we.Code != CodeResultTooLarge || buf.Len() != 0 {
		t.Fatalf("oversized columns frame: %v, %d bytes written", err, buf.Len())
	}
	w, buf = newTestWriter(len(frame), DefaultLimits())
	must(t, w.Columns([]Column{{Name: "nn"}}))
	if buf.String() != frame {
		t.Fatalf("columns frame %q", buf)
	}
	// Params only after columns, never after results; nothing after the end.
	w, _ = newTestWriter(DefaultFrameBytes, DefaultLimits())
	if err := w.Params([]string{":a"}); !errors.Is(err, errOrder) {
		t.Fatalf("params before columns: %v", err)
	}
	must(t, w.Results([]Result{{}}))
	if err := w.Params([]string{":a"}); !errors.Is(err, errOrder) {
		t.Fatalf("params after results: %v", err)
	}
	w, _ = newTestWriter(DefaultFrameBytes, DefaultLimits())
	must(t, w.Columns(nil))
	must(t, w.Params([]string{":a"}))
	must(t, w.Keepalive())
	must(t, w.Params([]string{":b"}))
	if err := w.Results([]Result{{}}); !errors.Is(err, errOrder) {
		t.Fatalf("results after params: %v", err)
	}
	if err := w.Row(nil); !errors.Is(err, errOrder) {
		t.Fatalf("row after params: %v", err)
	}
	must(t, w.Done(&Done{Kind: DoneDescribe}))
	for name, write := range map[string]func() error{
		"params":     func() error { return w.Params([]string{":c"}) },
		"no params":  func() error { return w.Params(nil) },
		"no results": func() error { return w.Results(nil) },
		"columns":    func() error { return w.Columns(nil) },
		"row":        func() error { return w.Row(nil) },
		"keepalive":  w.Keepalive,
		"error":      func() error { return w.Error(&Error{Code: CodeInternal, RequestID: "r"}) },
		"done":       func() error { return w.Done(&Done{Kind: DonePut}) },
	} {
		if err := write(); !errors.Is(err, errEnded) {
			t.Errorf("%s after the terminal frame: %v", name, err)
		}
	}
}

// testLimits has a distinct value in every member, so that the encoding
// shows which field each comes from.
var testLimits = Limits{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

const testLimitsJSON = `{"request_body_bytes":1,"header_block_bytes":2,"batch_entries":3,"frame_entries":4,` +
	`"result_bytes":5,"held_result_bytes":6,"terminal_frame_bytes":7,"error_message_bytes":8,` +
	`"retained_request_bytes":9,"resolve_bound":10,"statement_ms":11,"response_ms":12,"keepalive_ms":13,` +
	`"lock_wait_ms":14,"catch_up_ms":15,"channel_idle_ms":16,"write_txn_idle_ms":17,` +
	`"write_txn_lifetime_ms":18,"read_txn_idle_ms":19,"read_txn_lifetime_ms":20}`

// TestDoneKinds checks the exact bytes of every done kind, with a distinct
// value in every field.
func TestDoneKinds(t *testing.T) {
	const chID = "7c1d2e3f40a1b2c37c1d2e3f40a1b2c3"
	ch := func(m Mode) *Channel {
		return &Channel{Token: "t1.x", ID: chID, Mode: m, Epoch: 77, Limits: testLimits}
	}
	base := Done{Seq: 11, Txn: 22, TxnState: TxnOpen, RowsAffected: 33, LastInsertRowID: 44, Entries: 55, ParamCount: 66, Epoch: 77, LastSeq: 88, ReadOnly: true, Wrote: true}
	kind := func(k DoneKind, edit func(*Done)) *Done {
		d := base
		d.Kind = k
		if edit != nil {
			edit(&d)
		}
		return &d
	}
	batch := kind(DoneBatch, nil)
	for _, tc := range []struct {
		d    *Done
		want string
	}{
		{kind(DoneQuery, nil), `{"txn":22,"read_only":true,"rows_affected":33,"last_insert_rowid":44}`},
		{kind(DoneDescribe, nil), `{"txn":22,"param_count":66}`},
		{kind(DoneOpen, func(d *Done) { d.Channel = ch(ModeWrite) }), `{"txn":22,"channel":{"token":"t1.x","id":"` + chID + `","mode":"write","epoch":77,"limits":` + testLimitsJSON + `}}`},
		{kind(DoneOpen, func(d *Done) { d.Channel = ch(ModeReadOnly) }), `{"txn":22,"channel":{"token":"t1.x","id":"` + chID + `","mode":"read_only","limits":` + testLimitsJSON + `}}`},
		{kind(DoneBegin, nil), `{"seq":11,"txn_state":"open","txn":22}`},
		{kind(DoneExecute, nil), `{"seq":11,"txn_state":"open","txn":22,"read_only":true,"rows_affected":33,"last_insert_rowid":44}`},
		{kind(DoneExecute, func(d *Done) { d.DataOmitted, d.ReadOnly = true, false }), `{"seq":11,"txn_state":"open","txn":22,"read_only":false,"rows_affected":33,"last_insert_rowid":44,"data_omitted":true}`},
		{batch, `{"seq":11,"txn_state":"open","txn":22,"rows_affected":33,"last_insert_rowid":44,"entries":55}`},
		{kind(DoneChannelDescribe, func(d *Done) { d.DataOmitted = true }), `{"seq":11,"txn_state":"open","txn":22,"param_count":66,"data_omitted":true}`},
		{kind(DoneCommit, func(d *Done) { d.TxnState, d.Wrote = TxnNone, false }), `{"seq":11,"txn_state":"none","txn":22,"committed":true,"wrote":false}`},
		{kind(DoneRollback, func(d *Done) { d.TxnState, d.DataOmitted = TxnNone, true }), `{"seq":11,"txn_state":"none","txn":22,"data_omitted":true}`},
		{kind(DoneCancel, func(d *Done) { d.Outcome = &Receipt{Done: batch} }), `{"seq":11,"txn_state":"open","outcome":{"seq":11,"ok":true,"txn_state":"open","txn":22,"rows_affected":33,"last_insert_rowid":44,"entries":55}}`},
		{kind(DoneClose, func(d *Done) { d.Outcome = &Receipt{Done: kind(DoneBegin, func(d *Done) { d.DataOmitted = true })} }), `{"last_seq":88,"outcome":{"seq":11,"ok":true,"txn_state":"open","txn":22}}`},
		{kind(DoneResolve, func(d *Done) { d.Resolution = ResolutionPending }), `{"resolution":"pending","txn":22}`},
		{kind(DoneStatus, func(d *Done) { d.SQLiteVersion, d.Limits = "3.54.1", &testLimits }), `{"txn":22,"epoch":77,"sqlite_version":"3.54.1","limits":` + testLimitsJSON + `}`},
		{kind(DonePut, nil), `{"txn":22,"epoch":77}`},
	} {
		w, buf := newTestWriter(DefaultFrameBytes, DefaultLimits())
		must(t, w.Done(tc.d))
		if want := `{"done":` + tc.want + "}\n"; buf.String() != want {
			t.Errorf("done kind %d:\n got %s\nwant %s", tc.d.Kind, buf, want)
		}
	}
}

// TestErrorFields checks the exact bytes of every error field, present and
// absent.
func TestErrorFields(t *testing.T) {
	for _, tc := range []struct {
		e    *Error
		want string
	}{
		{&Error{
			Code: CodeSQLError, Final: true, Message: "m\"", RequestID: "Req_1-z", ChannelID: strings.Repeat("c", 32), Seq: 5,
			TxnState: TxnOpen, SQLiteCode: 19, SQLiteExtendedCode: 2067, BatchIndex: maxInt(3), Cause: "maintenance", Txn: 22, Epoch: 77,
		}, `{"code":"sql_error","not_executed":false,"final":true,"message":"m\"","request_id":"Req_1-z","channel_id":"` + strings.Repeat("c", 32) +
			`","seq":5,"txn_state":"open","sqlite_code":19,"sqlite_extended_code":2067,"batch_index":3,"cause":"maintenance","txn":22,"epoch":77}`},
		{
			&Error{Code: CodeInternal, NotExecuted: true, RequestID: "r", BatchIndex: maxInt(0)},
			`{"code":"internal","not_executed":true,"final":false,"message":"","request_id":"r","batch_index":0}`,
		},
		{
			&Error{Code: CodeBehind, RequestID: strings.Repeat("r", 64), Epoch: 1},
			`{"code":"behind","not_executed":false,"final":false,"message":"","request_id":"` + strings.Repeat("r", 64) + `","epoch":1}`,
		},
	} {
		w, buf := newTestWriter(DefaultFrameBytes, DefaultLimits())
		must(t, w.Error(tc.e))
		if want := `{"error":` + tc.want + "}\n"; buf.String() != want {
			t.Errorf("\n got %s\nwant %s", buf, want)
		}
	}
}

// TestWriterRefuses checks that the writer refuses, writing nothing and
// staying usable, what would be a malformed terminal frame.
func TestWriterRefuses(t *testing.T) {
	okErr := &Error{Code: CodeCancelled, RequestID: "r"}
	for name, write := range map[string]func(*Writer) error{
		"no done":                func(w *Writer) error { return w.Done(nil) },
		"zero done":              func(w *Writer) error { return w.Done(&Done{}) },
		"unknown kind":           func(w *Writer) error { return w.Done(&Done{Kind: DonePut + 1}) },
		"open without channel":   func(w *Writer) error { return w.Done(&Done{Kind: DoneOpen}) },
		"status without limits":  func(w *Writer) error { return w.Done(&Done{Kind: DoneStatus}) },
		"cancel without outcome": func(w *Writer) error { return w.Done(&Done{Kind: DoneCancel, Seq: 1}) },
		"close without outcome":  func(w *Writer) error { return w.Done(&Done{Kind: DoneClose, LastSeq: 1}) },
		"empty receipt":          func(w *Writer) error { return w.Done(&Done{Kind: DoneCancel, Outcome: &Receipt{}}) },
		"two-sided receipt": func(w *Writer) error {
			return w.Done(&Done{Kind: DoneCancel, Outcome: &Receipt{Done: &Done{Kind: DoneBegin}, Error: okErr}})
		},
		"receipt of a query": func(w *Writer) error {
			return w.Done(&Done{Kind: DoneClose, LastSeq: 1, Outcome: &Receipt{Done: &Done{Kind: DoneQuery}}})
		},
		"receipt of a cancel": func(w *Writer) error {
			return w.Done(&Done{Kind: DoneCancel, Outcome: &Receipt{Done: &Done{Kind: DoneCancel}}})
		},
		"receipt of a zero done": func(w *Writer) error { return w.Done(&Done{Kind: DoneCancel, Outcome: &Receipt{Done: &Done{}}}) },
		"receipt with a bad error": func(w *Writer) error {
			return w.Done(&Done{Kind: DoneCancel, Outcome: &Receipt{Error: &Error{Code: CodeCancelled}}})
		},
		"data_omitted on a query": func(w *Writer) error { return w.Done(&Done{Kind: DoneQuery, DataOmitted: true}) },
		"data_omitted on a cancel": func(w *Writer) error {
			return w.Done(&Done{Kind: DoneCancel, DataOmitted: true, Outcome: &Receipt{Error: okErr}})
		},
		"no error":              func(w *Writer) error { return w.Error(nil) },
		"empty code":            func(w *Writer) error { return w.Error(&Error{RequestID: "r"}) },
		"unknown code":          func(w *Writer) error { return w.Error(&Error{Code: "other", RequestID: "r"}) },
		"empty request ID":      func(w *Writer) error { return w.Error(&Error{Code: CodeInternal}) },
		"long request ID":       func(w *Writer) error { return w.Error(&Error{Code: CodeInternal, RequestID: strings.Repeat("r", 65)}) },
		"request ID with a dot": func(w *Writer) error { return w.Error(&Error{Code: CodeInternal, RequestID: "r.1"}) },
	} {
		w, buf := newTestWriter(DefaultFrameBytes, DefaultLimits())
		if err := write(w); err == nil || buf.Len() != 0 {
			t.Errorf("%s: %v, %q written", name, err, buf)
			continue
		}
		must(t, w.Error(okErr))
	}
}

// TestTerminalReservation checks that a terminal frame above its
// reservation is refused and that one at it is written.
func TestTerminalReservation(t *testing.T) {
	const frame = `{"done":{"txn":1,"epoch":1}}` + "\n"
	l := DefaultLimits()
	l.TerminalFrameBytes = int64(len(frame)) - 1
	w, buf := newTestWriter(DefaultFrameBytes, l)
	if err := w.Done(&Done{Kind: DonePut, Txn: 1, Epoch: 1}); err == nil || buf.Len() != 0 {
		t.Fatalf("oversized terminal frame: %v, %q written", err, buf)
	}
	l.TerminalFrameBytes++
	w, buf = newTestWriter(DefaultFrameBytes, l)
	must(t, w.Done(&Done{Kind: DonePut, Txn: 1, Epoch: 1}))
	if buf.String() != frame {
		t.Fatalf("terminal frame %q", buf)
	}
}

func TestNewWriterLimits(t *testing.T) {
	if _, err := NewWriter(io.Discard, DefaultFrameBytes, DefaultLimits()); err != nil {
		t.Fatalf("default limits: %v", err)
	}
	if _, err := NewWriter(io.Discard, DefaultFrameBytes, testLimits); err != nil {
		t.Fatalf("small limits: %v", err)
	}
	for name, edit := range map[string]func(*Limits){
		"zero request body":        func(l *Limits) { l.RequestBodyBytes = 0 },
		"negative result":          func(l *Limits) { l.ResultBytes = -1 },
		"zero frame entries":       func(l *Limits) { l.FrameEntries = 0 },
		"frame entries above 1024": func(l *Limits) { l.FrameEntries = 1025 },
		"batch above 10000":        func(l *Limits) { l.BatchEntries = 10001 },
		"message above 1024":       func(l *Limits) { l.ErrorMessageBytes = 1025 },
		"message below 2":          func(l *Limits) { l.ErrorMessageBytes = 1 },
		"ms above 2^31-1":          func(l *Limits) { l.ResponseMS = math.MaxInt32 + 1 },
		"zero ms":                  func(l *Limits) { l.ReadTxnLifetimeMS = 0 },
	} {
		l := DefaultLimits()
		edit(&l)
		if _, err := NewWriter(io.Discard, DefaultFrameBytes, l); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	l := DefaultLimits()
	l.BatchEntries, l.ErrorMessageBytes, l.FrameEntries, l.StatementMS, l.ResultBytes = 1, 2, 1, math.MaxInt32, math.MaxInt64
	if _, err := NewWriter(io.Discard, 1, l); err != nil {
		t.Fatalf("limits at their bounds: %v", err)
	}
	if _, err := NewWriter(io.Discard, 0, DefaultLimits()); err == nil {
		t.Fatal("a frame size of 0 accepted")
	}
}
