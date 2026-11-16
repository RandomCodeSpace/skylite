package wire

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// The example bodies of ADR 0005, with their tokens shortened as there.
var exampleBodies = []struct {
	kind Kind
	body string
}{
	{KindQuery, `{"sql":"SELECT id, title FROM books WHERE id = ?","params":[78],"min_txn":1210}`},
	{KindDescribe, `{"sql":"SELECT id FROM books WHERE title = :t","min_txn":1210}`},
	{KindOpen, `{"mode":"read_only","min_txn":1210}`},
	{KindChannel, `{"channel":"w1.9QvX0aYk3H8...","seq":12,"op":"begin","min_txn":1209,"execute":{"sql":"INSERT INTO books (title) VALUES (?) RETURNING id","params":["Dune"]}}`},
	{KindChannel, `{"channel":"w1.9QvX0aYk3H8...","seq":14,"op":"execute","sql":"UPDATE books SET title = :t WHERE id = :id","named":{"t":"Dune Messiah","id":78},"min_txn":1210}`},
	{KindChannel, `{"channel":"w1.9QvX0aYk3H8...","seq":15,"op":"batch","sql":"INSERT INTO tags (book, tag) VALUES (?, ?)","param_sets":[[78,"sf"],[78,"classic"]]}`},
	{KindChannel, `{"channel":"w1.9QvX0aYk3H8...","seq":16,"op":"batch","statements":[{"sql":"DELETE FROM tags WHERE book = ?","params":[78]},{"sql":"DELETE FROM books WHERE id = ?","params":[78]}]}`},
	{KindChannel, `{"channel":"r1.Xk2pQ9...","seq":3,"op":"describe","sql":"SELECT * FROM books"}`},
	{KindChannel, `{"channel":"w1.9QvX0aYk3H8...","seq":13,"op":"commit"}`},
	{KindCancel, `{"channel":"w1.9QvX0aYk3H8...","seq":13}`},
	{KindClose, `{"channel":"w1.9QvX0aYk3H8..."}`},
	{KindResolve, `{"channel":"w1.9QvX0aYk3H8...","seq":13}`},
	{KindPut, `{}`},
}

func TestParseExamples(t *testing.T) {
	for _, ex := range exampleBodies {
		r, err := Parse(ex.kind, []byte(ex.body), 10000)
		if err != nil {
			t.Fatalf("%s: %v", ex.body, err)
		}
		if err := r.Validate(); err != nil {
			t.Fatalf("%s: %v", ex.body, err)
		}
	}
	r, _ := Parse(KindChannel, []byte(exampleBodies[5].body), 10000)
	if r.Op != OpBatch || r.Seq != 15 || r.Channel != "w1.9QvX0aYk3H8..." || len(r.ParamSets) != 2 || r.SQL == "" {
		t.Fatalf("batch parsed as %+v", r)
	}
	r, _ = Parse(KindChannel, []byte(exampleBodies[3].body), 10000)
	if r.Op != OpBegin || r.MinTxn != 1209 || r.Execute == nil || !strings.HasPrefix(r.Execute.SQL, "INSERT") {
		t.Fatalf("begin parsed as %+v", r)
	}
}

// refused checks that Parse refuses body before acceptance.
func refused(t *testing.T, kind Kind, body string) {
	t.Helper()
	r, err := Parse(kind, []byte(body), 10)
	if err == nil {
		t.Fatalf("Parse(%q) = %+v; want a refusal", body, r)
	}
	if err.Code != CodeInvalidRequest || !err.NotExecuted || err.Final {
		t.Fatalf("Parse(%q) refused with %+v", body, err)
	}
}

// recorded checks that Parse accepts a channel request's body and Validate
// finds a problem to record.
func recorded(t *testing.T, body string) {
	t.Helper()
	r, err := Parse(KindChannel, []byte(body), 10)
	if err != nil {
		t.Fatalf("Parse(%q) refused: %v", body, err)
	}
	if e := r.Validate(); e == nil || e.Code != CodeInvalidRequest {
		t.Fatalf("Validate(%q) = %v; want invalid_request", body, e)
	}
}

func accepted(t *testing.T, kind Kind, body string) *Request {
	t.Helper()
	r, err := Parse(kind, []byte(body), 10)
	if err != nil {
		t.Fatalf("Parse(%q): %v", body, err)
	}
	if e := r.Validate(); e != nil {
		t.Fatalf("Validate(%q): %v", body, e)
	}
	return r
}

func TestDuplicateKey(t *testing.T) {
	refused(t, KindQuery, `{"sql":"a","sql":"b"}`)
	refused(t, KindQuery, `{"sql":"?","named":{"a":1,"a":2}}`)
	refused(t, KindQuery, `{"sql":"?","params":[{"blob":"","blob":""}]}`)
	// The same name, escaped, at the top level, in named, in a tagged value
	// and in an unknown field.
	for _, pair := range [][2]string{
		{`a`, `\u0061`}, {`é`, `\u00e9`}, {`é`, `\u00E9`}, {`😀`, `\ud83d\ude00`}, {`/`, `\/`}, {`\n`, `\u000a`},
	} {
		for _, names := range [][2]string{pair, {pair[1], pair[0]}} {
			refused(t, KindQuery, `{"sql":"x","named":{"`+names[0]+`":1,"`+names[1]+`":2}}`)
			refused(t, KindPut, `{"`+names[0]+`":1,"`+names[1]+`":2}`)
			refused(t, KindChannel, `{"channel":"c","seq":1,"op":"ping","x":{"`+names[0]+`":1,"`+names[1]+`":2}}`)
		}
	}
	refused(t, KindQuery, `{"sql":"a","\u0073ql":"b"}`)
	refused(t, KindQuery, `{"\u0073ql":"a","sql":"b"}`)
	refused(t, KindQuery, `{"sql":"?","params":[{"blob":"","\u0062lob":""}]}`)
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"ping","\u006fp":"ping"}`)
	// On a channel the duplicate is a framing error, refused, never recorded.
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"ping","op":"ping"}`)
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"execute","sql":"?","named":{"a":1,"a":1}}`)
	// Inside the value of an unknown field, which is otherwise only recorded.
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"ping","x":[{"a":{"b":1,"b":2}}]}`)
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"ping","x":1,"x":2}`)
	// Values are not names, and names in different objects do not clash.
	accepted(t, KindQuery, `{"sql":"x","named":{"a":"b","b":"a","c":{"blob":""},"blob":{"blob":""}}}`)
	accepted(t, KindChannel, `{"channel":"c","seq":1,"op":"batch","statements":[{"sql":"x"},{"sql":"x"}]}`)
	recorded(t, `{"channel":"c","seq":1,"op":"ping","x":{"a":1,"b":{"a":1}},"y":{"a":1}}`)
}

// TestDuplicateCollisions checks that equal hashes alone are not a
// duplicate: with every name hashing alike, the names are compared,
// whichever pair of them is equal.
func TestDuplicateCollisions(t *testing.T) {
	defer func(h func([]byte) uint64) { nameHash = h }(nameHash)
	nameHash = func([]byte) uint64 { return 7 }
	for _, body := range []string{`{}`, `{"a":1}`, `{"a":1,"b":{"a":2}}`, `{"a":1,"b":2,"c":3,"d":{"a":1,"b":2}}`} {
		if err := checkSyntax([]byte(body)); err != nil {
			t.Errorf("%s: %v", body, err)
		}
	}
	for _, body := range []string{
		`{"a":1,"a":2}`, `{"a":1,"b":2,"b":3}`, `{"a":1,"b":2,"c":3,"b":4}`, `{"x":1,"a":2,"b":3,"c":4,"\u0063":5}`,
		`{"a":{"b":1,"c":2,"c":3}}`,
	} {
		if err := checkSyntax([]byte(body)); !errors.Is(err, errDuplicate) {
			t.Errorf("%s: %v; want a duplicate", body, err)
		}
	}
	accepted(t, KindQuery, `{"sql":"x","named":{"a":1,"b":2,"c":{"blob":""}}}`)
	refused(t, KindQuery, `{"sql":"x","named":{"a":1,"b":2,"\u0062":3}}`)
}

// TestDepth checks the nesting bound: 16 levels, the root object included,
// and a deeper body refused before acceptance whatever field it is in.
func TestDepth(t *testing.T) {
	const ch = `{"channel":"c","seq":1,"op":"ping","x":`
	nest := func(levels int, open, close string) string {
		return strings.Repeat(open, levels) + "0" + strings.Repeat(close, levels)
	}
	recorded(t, ch+nest(MaxDepth-1, "[", "]")+`}`)
	recorded(t, ch+nest(MaxDepth-1, `{"a":`, "}")+`}`)
	refused(t, KindChannel, ch+nest(MaxDepth, "[", "]")+`}`)
	refused(t, KindChannel, ch+nest(MaxDepth, `{"a":`, "}")+`}`)
	refused(t, KindChannel, ch+nest(9998, "[", "]")+`}`)
	accepted(t, KindChannel, `{"channel":"c","seq":1,"op":"batch","sql":"x","param_sets":[[{"blob":""}]]}`)
	refused(t, KindQuery, `{"sql":"x","params":[`+nest(MaxDepth-1, "[", "]")+`]}`)
	if err := checkSyntax([]byte(nest(MaxDepth, "[", "]"))); err != nil {
		t.Fatalf("16 levels: %v", err)
	}
	if err := checkSyntax([]byte(nest(MaxDepth+1, "[", "]"))); !errors.Is(err, errDepth) {
		t.Fatalf("17 levels: %v", err)
	}
}

func TestLoneSurrogate(t *testing.T) {
	refused(t, KindQuery, `{"sql":"SELECT '\ud800'"}`)
	refused(t, KindQuery, `{"sql":"?","params":["\udc00"]}`)
	refused(t, KindQuery, `{"sql":"?","named":{"\ud800":1}}`)
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"execute","sql":"x\ude00\ud83d"}`)
	accepted(t, KindQuery, `{"sql":"SELECT '😀'"}`)
	r := accepted(t, KindQuery, `{"sql":"SELECT '\ud83d\ude00'"}`)
	if r.SQL != "SELECT '😀'" {
		t.Fatalf("escaped pair read as %q", r.SQL)
	}
}

func TestInvalidUTF8(t *testing.T) {
	refused(t, KindQuery, "{\"sql\":\"SELECT '\xff'\"}")
	refused(t, KindQuery, "{\"sql\":\"?\",\"params\":[\"\xc3\"]}")
	refused(t, KindQuery, "{\"sql\":\"?\",\"named\":{\"\xed\xa0\x80\":1}}")
	refused(t, KindChannel, "{\"channel\":\"c\",\"seq\":1,\"op\":\"ping\",\"\xc0\xaf\":1}")
}

func TestBOM(t *testing.T) {
	refused(t, KindPut, "\xef\xbb\xbf{}")
	refused(t, KindChannel, "\xef\xbb\xbf{\"channel\":\"c\",\"seq\":1,\"op\":\"ping\"}")
	accepted(t, KindQuery, "{\"sql\":\"\xef\xbb\xbfSELECT 1\"}") // U+FEFF inside text is text
}

func TestTrailingByte(t *testing.T) {
	accepted(t, KindPut, `{}`)
	for _, tail := range []string{" ", "\n", "\r\n", "\t", "x", "}", "{}", "\x00", "0"} {
		refused(t, KindPut, `{}`+tail)
		refused(t, KindChannel, `{"channel":"c","seq":1,"op":"ping"}`+tail)
	}
	for _, body := range []string{" {}", "\n{}", "\t{}", "[{}]"} {
		refused(t, KindPut, body)
	}
	refused(t, KindPut, ``)
	refused(t, KindPut, `[]`)
	refused(t, KindPut, `"{}"`)
}

func TestIntegerRange(t *testing.T) {
	const ch = `{"channel":"c","op":"ping","seq":`
	accepted(t, KindChannel, ch+`1}`)
	accepted(t, KindChannel, ch+`9223372036854775807}`)
	for _, seq := range []string{"0", "-0", "-1", "9223372036854775808", "18446744073709551616", "1.0", "1e0", "1E0", `"1"`, "null"} {
		refused(t, KindChannel, ch+seq+`}`)
		refused(t, KindCancel, `{"channel":"c","seq":`+seq+`}`)
	}
	accepted(t, KindQuery, `{"sql":"x","min_txn":0}`)
	accepted(t, KindQuery, `{"sql":"x","min_txn":9223372036854775807,"timeout_ms":2147483647}`)
	accepted(t, KindQuery, `{"sql":"x","timeout_ms":1}`)
	for _, field := range []string{`"min_txn":-1`, `"min_txn":9223372036854775808`, `"min_txn":1.5`, `"timeout_ms":0`, `"timeout_ms":2147483648`, `"timeout_ms":1e3`} {
		refused(t, KindQuery, `{"sql":"x",`+field+`}`)
		recorded(t, `{"channel":"c","seq":1,"op":"execute","sql":"x",`+field+`}`)
	}
	r := accepted(t, KindChannel, ch+`9223372036854775807}`)
	if r.Seq != 1<<63-1 {
		t.Fatalf("seq = %d", r.Seq)
	}
}

func TestParseShapes(t *testing.T) {
	const c = `"channel":"c","seq":1,`
	for _, body := range []string{
		`{` + c + `"op":"begin"}`,
		`{` + c + `"op":"begin","min_txn":5,"timeout_ms":9,"execute":{"sql":"x","named":{"a":1}}}`,
		`{` + c + `"op":"execute","sql":"x","params":[]}`,
		`{` + c + `"op":"batch","sql":"x","param_sets":[{"a":1},{}]}`,
		`{` + c + `"op":"batch","statements":[{"sql":"x"}],"timeout_ms":1}`,
		`{` + c + `"op":"describe","sql":"x"}`,
		`{` + c + `"op":"commit","timeout_ms":5}`,
		`{` + c + `"op":"rollback"}`,
		`{` + c + `"op":"ping"}`,
		`{"seq":1,"op":"ping","channel":"` + strings.Repeat("A", 256) + `"}`,
		`{"seq":1,"op":"ping","channel":"azAZ09._-"}`,
	} {
		accepted(t, KindChannel, body)
	}
	for _, body := range []string{
		`{` + c + `"op":"begin","sql":"x"}`,
		`{` + c + `"op":"begin","execute":{"sql":"x","params":[],"named":{}}}`,
		`{` + c + `"op":"begin","execute":{"params":[]}}`,
		`{` + c + `"op":"begin","execute":{"sql":"x","other":1}}`,
		`{` + c + `"op":"begin","execute":"x"}`,
		`{` + c + `"op":"execute"}`,
		`{` + c + `"op":"execute","sql":"x","params":[],"named":{}}`,
		`{` + c + `"op":"execute","sql":"x","params":{}}`,
		`{` + c + `"op":"execute","sql":"x","params":[true]}`,
		`{` + c + `"op":"execute","sql":"x","params":[[1]]}`,
		`{` + c + `"op":"execute","sql":"x\u0000"}`,
		`{` + c + `"op":"begin","execute":{"sql":"x\u0000"}}`,
		`{` + c + `"op":"batch","statements":[{"sql":"x"},{"sql":"\u0000"}]}`,
		`{` + c + `"op":"batch","sql":"\u0000","param_sets":[[]]}`,
		`{` + c + `"op":"execute","sql":"x","mode":"write"}`,
		`{` + c + `"op":"execute","sql":1}`,
		`{` + c + `"op":"batch"}`,
		`{` + c + `"op":"batch","sql":"x"}`,
		`{` + c + `"op":"batch","param_sets":[[]]}`,
		`{` + c + `"op":"batch","sql":"x","param_sets":[]}`,
		`{` + c + `"op":"batch","sql":"x","param_sets":[[],{}]}`,
		`{` + c + `"op":"batch","sql":"x","param_sets":[1]}`,
		`{` + c + `"op":"batch","sql":"x","param_sets":[` + strings.Repeat(`[],`, 10) + `[]]}`,
		`{` + c + `"op":"batch","sql":"x","param_sets":[[]],"statements":[{"sql":"x"}]}`,
		`{` + c + `"op":"batch","statements":[]}`,
		`{` + c + `"op":"batch","statements":[{"sql":"x","named":{"a":true}}]}`,
		`{` + c + `"op":"batch","statements":[{}]}`,
		`{` + c + `"op":"batch","statements":[` + strings.Repeat(`{"sql":"x"},`, 10) + `{"sql":"x"}]}`,
		`{` + c + `"op":"describe","sql":"x","params":[]}`,
		`{` + c + `"op":"describe","sql":"x","min_txn":1}`,
		`{` + c + `"op":"commit","min_txn":1}`,
		`{` + c + `"op":"rollback","timeout_ms":1}`,
		`{` + c + `"op":"ping","sql":"x"}`,
		`{` + c + `"op":"vacuum"}`,
		`{` + c + `"op":"PING"}`,
		`{` + c + `"op":1}`,
		`{` + c + `"other":1}`,
		`{"channel":"c","seq":1}`,
	} {
		recorded(t, body)
	}
	for _, body := range []string{
		`{"seq":1,"op":"ping"}`,
		`{"channel":"","seq":1,"op":"ping"}`,
		`{"channel":"a b","seq":1,"op":"ping"}`,
		`{"channel":"a/b","seq":1,"op":"ping"}`,
		`{"channel":"` + strings.Repeat("A", 257) + `","seq":1,"op":"ping"}`,
		`{"channel":1,"seq":1,"op":"ping"}`,
		`{"channel":"c","op":"ping"}`,
		`{"channel":"c","seq":1,"op":"ping"`,
		`{"channel":"c","seq":1,"op":"ping",}`,
	} {
		refused(t, KindChannel, body)
	}
	// Identification comes first, whatever else is wrong.
	for _, kind := range []Kind{KindChannel, KindCancel, KindResolve} {
		if _, err := Parse(kind, []byte(`{"channel":"c","other":1}`), 10); err == nil || !strings.Contains(err.Message, "must carry seq") {
			t.Errorf("kind %d without seq: %v", kind, err)
		}
	}
	for _, kind := range []Kind{KindChannel, KindCancel, KindClose, KindResolve} {
		if _, err := Parse(kind, []byte(`{"channel":"a b","seq":1}`), 10); err == nil || !strings.Contains(err.Message, "must carry channel") {
			t.Errorf("kind %d with a bad channel: %v", kind, err)
		}
	}
	for _, ok := range []struct {
		kind Kind
		body string
	}{
		{KindQuery, `{"sql":"x","named":{},"min_txn":1,"timeout_ms":1}`},
		{KindDescribe, `{"sql":"x","min_txn":1}`},
		{KindOpen, `{"mode":"write"}`},
		{KindOpen, `{"mode":"read_only","min_txn":3}`},
		{KindCancel, `{"channel":"c","seq":1}`},
		{KindClose, `{"channel":"c"}`},
		{KindResolve, `{"channel":"c","seq":1}`},
		{KindPut, `{ }`},
	} {
		accepted(t, ok.kind, ok.body)
	}
	for _, bad := range []struct {
		kind Kind
		body string
	}{
		{KindQuery, `{}`},
		{KindQuery, `{"sql":"x","params":[],"named":{}}`},
		{KindQuery, `{"sql":"x","op":"execute"}`},
		{KindQuery, `{"sql":"x","params":[true]}`},
		{KindQuery, `{"sql":"x","other":1}`},
		{KindDescribe, `{"sql":"x","params":[]}`},
		{KindDescribe, `{"sql":"x","timeout_ms":1}`},
		{KindOpen, `{}`},
		{KindOpen, `{"mode":"write","min_txn":1}`},
		{KindOpen, `{"mode":"read-only"}`},
		{KindOpen, `{"mode":"read_only","channel":"c"}`},
		{KindCancel, `{"channel":"c"}`},
		{KindCancel, `{"channel":"c","seq":1,"op":"ping"}`},
		{KindClose, `{"channel":"c","seq":1}`},
		{KindClose, `{}`},
		{KindResolve, `{"channel":"c","seq":1,"min_txn":1}`},
		{KindPut, `{"mode":"write"}`},
		{KindStatus, `{}`},
	} {
		refused(t, bad.kind, bad.body)
	}
}

// TestRecordedMessage checks the message of a recorded problem, for a
// field whose escaped name and escaped value share the decoder's scratch.
func TestRecordedMessage(t *testing.T) {
	for body, want := range map[string]string{
		`{"channel":"c","seq":1,"op":"execute","\u0073ql":"\u0000"}`:            `field "sql" has a bad value`,
		`{"channel":"c","seq":1,"op":"ping","\u0078yz":1,"other":2}`:            `unknown field "xyz"`,
		`{"channel":"c","seq":1,"op":"execute","sql":"x","params":[1,true,[]]}`: `bad parameter value: a value must be null, a number, a string or a tagged object`,
	} {
		r, err := Parse(KindChannel, []byte(body), 10)
		if err != nil {
			t.Fatal(err)
		}
		if e := r.Validate(); e == nil || e.Message != want {
			t.Errorf("%s: %v; want %q", body, e, want)
		}
	}
}

func TestRecordedFlags(t *testing.T) {
	r, _ := Parse(KindChannel, []byte(`{"channel":"c","seq":1,"op":"execute","sql":"x","other":1}`), 10)
	e := r.Validate().Recorded(false)
	if !e.Final || !e.NotExecuted {
		t.Fatalf("outside a transaction: %+v", e)
	}
	e = r.Validate().Recorded(true)
	if !e.Final || e.NotExecuted {
		t.Fatalf("an execute inside a transaction: %+v", e)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Recorded of a code that is never recorded did not panic")
		}
	}()
	_ = NewError(CodeChannelBusy, "").Recorded(false)
}

func TestNewErrorFlags(t *testing.T) {
	for code, want := range map[Code][2]bool{
		CodeInvalidRequest:       {true, false},
		CodeSequenceConflict:     {true, true},
		CodeCommitOutcomeUnknown: {false, false},
		CodeOverloaded:           {true, false},
	} {
		e := NewError(code, "m")
		if e.NotExecuted != want[0] || e.Final != want[1] {
			t.Errorf("%s: not_executed %v, final %v", code, e.NotExecuted, e.Final)
		}
	}
}

// TestCodes checks every code's status and whether it can be recorded
// against ADR 0005's errors table, where "final frame" is 200, the status
// of the accepted request's response.
func TestCodes(t *testing.T) {
	table := []struct {
		code       Code
		status     int
		recordable bool
	}{
		{CodeInvalidRequest, 400, true},
		{CodeUnauthenticated, 401, false},
		{CodeForbidden, 403, false},
		{CodeStatementForbidden, 403, true},
		{CodeDatabaseNotFound, 404, false},
		{CodeDatabaseExists, 409, false},
		{CodeNeedsChannel, 409, false},
		{CodeChannelBusy, 409, false},
		{CodeInFlight, 409, false},
		{CodeStaleSequence, 409, false},
		{CodeSequenceGap, 409, false},
		{CodeSequenceConflict, 409, false},
		{CodeChannelLost, 410, false},
		{CodeRequestTooLarge, 413, false},
		{CodeTooManyChannels, 429, false},
		{CodeOverloaded, 429, false},
		{CodeInvalidState, 409, true},
		{CodeTransactionAborted, 200, true},
		{CodeBusy, 200, true},
		{CodeTimeout, 200, true},
		{CodeCancelled, 200, true},
		{CodeResultTooLarge, 200, true},
		{CodeSQLError, 400, true},
		{CodeCommitFailed, 200, true},
		{CodeCommitOutcomeUnknown, 200, false},
		{CodeNoLeader, 503, false},
		{CodeNotServing, 503, false},
		{CodeOwnerUnreachable, 503, false},
		{CodeBehind, 503, false},
		{CodeMaintenance, 503, true},
		{CodeInternal, 500, true},
	}
	if len(table) != 31 || len(codes) != len(table) {
		t.Fatalf("%d codes in the table, %d in the package; the ADR lists 31", len(table), len(codes))
	}
	for i, tc := range table {
		if codes[i] != tc.code {
			t.Errorf("code %d is %s; want %s", i, codes[i], tc.code)
		}
		if got := tc.code.Status(); got != tc.status {
			t.Errorf("%s: status %d, want %d", tc.code, got, tc.status)
		}
		if got := tc.code.Recordable(); got != tc.recordable {
			t.Errorf("%s: recordable %v, want %v", tc.code, got, tc.recordable)
		}
	}
	if Code("other").Status() != 500 || Code("other").Recordable() {
		t.Error("an unknown code is not a 500 that is never recorded")
	}
}

func TestRoute(t *testing.T) {
	for _, ok := range []struct {
		method, path string
		kind         Kind
	}{
		{"POST", "/v1/databases/library/query", KindQuery},
		{"POST", "/v1/databases/library/describe", KindDescribe},
		{"POST", "/v1/databases/library/channels", KindOpen},
		{"POST", "/v1/databases/library/channel", KindChannel},
		{"POST", "/v1/databases/library/channel/cancel", KindCancel},
		{"POST", "/v1/databases/library/channel/close", KindClose},
		{"POST", "/v1/databases/library/resolve", KindResolve},
		{"GET", "/v1/databases/library", KindStatus},
		{"PUT", "/v1/databases/library", KindPut},
	} {
		kind, db, err := Route(ok.method, ok.path)
		if err != nil || kind != ok.kind || db != "library" {
			t.Errorf("%s %s = %d %q %v", ok.method, ok.path, kind, db, err)
		}
	}
	for _, bad := range [][2]string{
		{"GET", "/v1/databases/library/query"},
		{"PUT", "/v1/databases/library/channel"},
		{"POST", "/v1/databases/library"},
		{"DELETE", "/v1/databases/library"},
		{"GET", "/v1/databases/library/"},
		{"POST", "/v1/databases/library/query/"},
		{"POST", "/v1/databases/library/channel/other"},
		{"GET", "/v1/databases/"},
		{"GET", "/v1/databases"},
		{"GET", "/v2/databases/library"},
		{"GET", "//v1/databases/library"},
		{"GET", "/v1/databases/Library"},
		{"GET", "/v1/databases/lib_rary"},
		{"GET", "/v1/databases/-lib"},
		{"GET", "/v1/databases/lib%61"},
		{"GET", "/v1/databases/" + strings.Repeat("a", 64)},
	} {
		if _, _, err := Route(bad[0], bad[1]); err == nil || err.Code != CodeInvalidRequest {
			t.Errorf("%s %s: %v; want invalid_request", bad[0], bad[1], err)
		}
	}
	for _, name := range []string{"a", "0", "9-a", "a-", strings.Repeat("z", 63)} {
		if !ValidDatabaseName(name) {
			t.Errorf("%q refused", name)
		}
	}
}

type binding struct {
	index int
	v     any
}

func bind(t *testing.T, args string, names []string) ([]binding, error) {
	t.Helper()
	body := `{"sql":"x"`
	if args != "" {
		body += "," + args
	}
	r := accepted(t, KindQuery, body+"}")
	var got []binding
	err := r.Bind(r.Args, names, func(i int, v any) error {
		got = append(got, binding{i, v})
		return nil
	})
	return got, err
}

func TestBind(t *testing.T) {
	for _, tc := range []struct {
		args  string
		names []string
		want  string
	}{
		{``, nil, `[]`},
		{`"params":[]`, nil, `[]`},
		{`"named":{}`, nil, `[]`},
		{`"params":[1,"a",null]`, []string{"", ":x", "?3"}, `[{1 1} {2 a} {3 <nil>}]`},
		{`"named":{"b":"x","a":1}`, []string{":a", "@b"}, `[{2 x} {1 1}]`},
		{`"named":{"x":{"blob":"AAE="}}`, []string{"$x"}, `[{1 [0 1]}]`},
		{`"params":[1,2]`, []string{":a", "@a"}, `[{1 1} {2 2}]`},
		{`"params":[1,2,3]`, []string{"", "", "?3"}, `[{1 1} {2 2} {3 3}]`},
	} {
		got, err := bind(t, tc.args, tc.names)
		if err != nil || fmt.Sprint(got) != tc.want {
			t.Errorf("Bind(%s, %q) = %v, %v; want %s", tc.args, tc.names, got, err, tc.want)
		}
	}
	for _, tc := range []struct {
		args  string
		names []string
	}{
		{``, []string{""}},
		{`"params":[]`, []string{""}},
		{`"params":[1]`, []string{"", ""}},
		{`"params":[1,2]`, []string{""}},
		{`"params":[1]`, nil},
		{`"named":{"a":1}`, nil},
		{`"named":{"a":1,"z":2}`, []string{":a"}},
		{`"named":{"a":1}`, []string{":a", ""}},
		{`"named":{"a":1}`, []string{":a", "@a"}},
		{`"named":{"3":1}`, []string{"", "", "?3"}},
		{`"named":{":a":1}`, []string{":a"}},
		{`"named":{"1":1}`, []string{"?1"}},
		{`"named":{"a":1}`, []string{"a"}},
	} {
		got, err := bind(t, tc.args, tc.names)
		var we *Error
		if !errors.As(err, &we) || we.Code != CodeInvalidRequest {
			t.Errorf("Bind(%s, %q) = %v, %v; want invalid_request", tc.args, tc.names, got, err)
		}
	}
	if _, err := bind(t, `"named":{"a":1}`, []string{":a", "@a"}); err == nil || !strings.Contains(err.Error(), "prefix") {
		t.Fatalf("names differing only in prefix: %v", err)
	}
	r := accepted(t, KindQuery, `{"sql":"x","params":[1]}`)
	stop := errors.New("stop")
	if err := r.Bind(r.Args, []string{""}, func(int, any) error { return stop }); err != stop { //nolint:errorlint // must be unchanged
		t.Fatalf("Bind returned %v; want the set error unchanged", err)
	}
	r = accepted(t, KindChannel, `{"channel":"c","seq":1,"op":"batch","sql":"x","param_sets":[[1],[2]]}`)
	var got []any
	for _, a := range r.ParamSets {
		if err := r.Bind(a, []string{""}, func(_ int, v any) error { got = append(got, v); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(got, []any{int64(1), int64(2)}) {
		t.Fatalf("param sets bound %v", got)
	}
	// Nothing reaches set unless every parameter is bound exactly once.
	for _, tc := range []struct {
		args  string
		names []string
	}{
		{`"params":[1,2]`, []string{""}},
		{`"params":[1]`, []string{"", ""}},
		{`"named":{"a":1,"b":2}`, []string{":a"}},
		{`"named":{"a":1}`, []string{":a", ":b"}},
	} {
		r := accepted(t, KindQuery, `{"sql":"x",`+tc.args+`}`)
		calls := 0
		err := r.Bind(r.Args, tc.names, func(int, any) error { calls++; return nil })
		var we *Error
		if !errors.As(err, &we) || we.Code != CodeInvalidRequest || calls != 0 {
			t.Errorf("Bind(%s, %q) = %v after %d calls to set; want invalid_request before any", tc.args, tc.names, err, calls)
		}
	}
	// A channel request with a problem to record returns that problem,
	// whether or not Validate was called, and binds nothing.
	r, perr := Parse(KindChannel, []byte(`{"channel":"c","seq":1,"op":"execute","sql":"x","params":[1,{"blob":"x"}]}`), 10)
	if perr != nil {
		t.Fatal(perr)
	}
	calls := 0
	err := r.Bind(r.Args, []string{"", ""}, func(int, any) error { calls++; return nil })
	var we *Error
	if !errors.As(err, &we) || we.Code != CodeInvalidRequest || we.Message != r.Validate().Message || calls != 0 {
		t.Fatalf("Bind of an invalid channel request = %v after %d calls to set", err, calls)
	}
}

// TestProblemsAllocate checks that a problem to record costs no allocation
// after the first: Parse allocates as much for many bad values or unknown
// fields as for few, apart from the hashes of the names.
func TestProblemsAllocate(t *testing.T) {
	// A regression costs an allocation per value or field, thousands here;
	// the slack covers the hash slice's growth and the race detector's noise.
	const slack = 10
	allocs := func(head, item, tail string, n int) float64 {
		body := []byte(head + strings.Repeat(item+",", n-1) + item + tail)
		return testing.AllocsPerRun(5, func() {
			if r, err := Parse(KindChannel, body, 10); err != nil || r.Validate() == nil {
				t.Fatalf("%.40s: %v", body, err)
			}
		})
	}
	const params = `{"channel":"c","seq":1,"op":"execute","sql":"x","params":[`
	for _, item := range []string{"[]", "{}", "true", `{"blob":"x"}`} {
		if few, many := allocs(params, item, "]}", 10), allocs(params, item, "]}", 10000); many > few+slack {
			t.Errorf("bad values %s: %v allocations for 10, %v for 10,000", item, few, many)
		}
	}
	fields := func(n int) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, `,"u%d":0`, i)
		}
		return b.String()
	}
	few := allocs(`{"channel":"c","seq":1,"op":"ping"`, fields(5000), "}", 1)
	many := allocs(`{"channel":"c","seq":1,"op":"ping"`, fields(10000), "}", 1)
	if many > few+slack {
		t.Errorf("unknown fields: %v allocations for 5,000, %v for 10,000", few, many)
	}
}

// TestDecodedMemory checks ADR 0005's charge of 8 times a body's length: the
// raw body once, and at most 7 bytes per body byte for what Parse allocates,
// for bodies of the smallest values, whether Parse accepts them, finds a
// problem to record or refuses them, as for a duplicate name placed last.
func TestDecodedMemory(t *testing.T) {
	many := func(head, item, sep, tail string, n int) []byte {
		return []byte(head + strings.Repeat(item+sep, n-1) + item + tail)
	}
	names := func(head, tail string) []byte {
		var b strings.Builder
		b.WriteString(head)
		for i := range 100000 {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"%x":0`, i)
		}
		b.WriteString(tail)
		return []byte(b.String())
	}
	const ok, record, refuse = 0, 1, 2
	for _, tc := range []struct {
		name string
		kind Kind
		body []byte
		want int
	}{
		{"params", KindQuery, many(`{"sql":"x","params":[`, "0", ",", "]}", 500000), ok},
		{"tagged", KindQuery, many(`{"sql":"x","params":[`, `{"blob":""}`, ",", "]}", 100000), ok},
		{"reals", KindQuery, many(`{"sql":"x","params":[`, `1e1`, ",", "]}", 200000), ok},
		{"named", KindQuery, names(`{"sql":"x","named":{`, `}}`), ok},
		{"param sets", KindChannel, many(`{"channel":"c","seq":1,"op":"batch","sql":"x","param_sets":[`, "[]", ",", "]}", 10000), ok},
		{"statements", KindChannel, many(`{"channel":"c","seq":1,"op":"batch","statements":[`, `{"sql":"x"}`, ",", "]}", 10000), ok},
		{"one statement", KindChannel, []byte(`{"channel":"c","seq":1,"op":"batch","statements":[{"sql":"` + strings.Repeat("x", 20000) + `"}]}`), ok},
		{"sql", KindQuery, []byte(`{"sql":"` + strings.Repeat(`\n`, 500000) + `"}`), ok},
		{"unknown fields", KindChannel, names(`{"channel":"c","seq":1,"op":"ping",`, `}`), record},
		{"named, duplicate last", KindQuery, names(`{"sql":"x","named":{`, `,"0":0}}`), refuse},
		{"unknown fields, duplicate last", KindPut, names(`{`, `,"0":0}`), refuse},
		{"bad values []", KindChannel, many(`{"channel":"c","seq":1,"op":"execute","sql":"x","params":[`, "[]", ",", "]}", 500000), record},
		{"bad values {}", KindChannel, many(`{"channel":"c","seq":1,"op":"execute","sql":"x","params":[`, "{}", ",", "]}", 500000), record},
		{"bad values true", KindChannel, many(`{"channel":"c","seq":1,"op":"execute","sql":"x","params":[`, "true", ",", "]}", 500000), record},
	} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		r, err := Parse(tc.kind, tc.body, 10000)
		runtime.ReadMemStats(&after)
		got := ok
		switch {
		case err != nil:
			got = refuse
		case r.Validate() != nil:
			got = record
		}
		if got != tc.want {
			t.Fatalf("%s: outcome %d, want %d: %v", tc.name, got, tc.want, err)
		}
		alloc, limit := after.TotalAlloc-before.TotalAlloc, 7*uint64(len(tc.body))
		t.Logf("%s: %.2f bytes allocated per body byte", tc.name, float64(alloc)/float64(len(tc.body)))
		if alloc > limit {
			t.Errorf("%s: Parse allocated %d bytes for a %d-byte body; the limit is %d", tc.name, alloc, len(tc.body), limit)
		}
	}
}
