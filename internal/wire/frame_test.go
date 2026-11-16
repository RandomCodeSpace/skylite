package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/go-json-experiment/json/jsontext"
)

func newTestWriter(frameBytes int, l Limits) (*Writer, *bytes.Buffer) {
	var buf bytes.Buffer
	return NewWriter(&buf, frameBytes, l), &buf
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
	var b bytes.Buffer
	w = NewWriter(&b, DefaultFrameBytes, DefaultLimits())
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
		receipt = len(w.appendReceipt(nil, &Receipt{Error: largestError()}))
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
	w, buf := newTestWriter(DefaultFrameBytes, DefaultLimits())
	must(t, w.Error(NewError(CodeSQLError, strings.Repeat("x", 5000))))
	var v struct {
		Error struct {
			Message jsontext.Value `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(buf.Bytes(), &v); err != nil || len(v.Error.Message) != 1024 {
		t.Fatalf("message of %d bytes, %v", len(v.Error.Message), err)
	}
}

// TestQuotedLen checks quotedLen against the encoder for every character.
func TestQuotedLen(t *testing.T) {
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		q, err := jsontext.AppendQuote(nil, string(r))
		if err != nil || len(q)-2 != quotedLen(r) {
			t.Fatalf("U+%04X: encoded in %d bytes, quotedLen %d", r, len(q)-2, quotedLen(r))
		}
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
	// By entries: 2,500 entries in two calls give frames of 1,024, 1,024 and 452.
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
	must(t, w.Params([]string{":a", ":b"}))
	if n := len(lines(buf)); n != 1 {
		t.Fatalf("%d frames; want 1", n)
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
	must(t, w.Error(we.Recorded(false)))
	if got := lines(buf); len(got) != 3 || !strings.HasPrefix(got[2], `{"error":{"code":"result_too_large"`) {
		t.Fatalf("body:\n%s", buf)
	}
	if err := w.Keepalive(); !errors.Is(err, errEnded) {
		t.Fatalf("frame after the terminal frame: %v", err)
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
	// A columns frame above the frame size is refused.
	w, buf := newTestWriter(64, DefaultLimits())
	if err := w.Columns([]Column{{Name: strings.Repeat("n", 64)}}); err == nil || buf.Len() != 0 {
		t.Fatalf("oversized columns frame: %v, %d bytes written", err, buf.Len())
	}
}
