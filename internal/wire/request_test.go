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
	refused(t, KindQuery, `{"sql":"a","sql":"b"}`) // the same name, escaped
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
	// Equal hashes alone are not a duplicate: the names are compared.
	if hasDuplicate([]byte(`{"a":1,"b":{"a":2}}`), []uint64{7, 7}) {
		t.Fatal("distinct names reported as duplicates")
	}
	if !hasDuplicate([]byte(`{"a":1,"\u0061":2}`), []uint64{7, 7}) {
		t.Fatal("an escaped duplicate missed")
	}
}

func TestLoneSurrogate(t *testing.T) {
	refused(t, KindQuery, `{"sql":"SELECT '\ud800'"}`)
	refused(t, KindQuery, `{"sql":"?","params":["\udc00"]}`)
	refused(t, KindQuery, `{"sql":"?","named":{"\ud800":1}}`)
	refused(t, KindChannel, `{"channel":"c","seq":1,"op":"execute","sql":"x\ude00\ud83d"}`)
	accepted(t, KindQuery, `{"sql":"SELECT '😀'"}`)
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

func TestStatus(t *testing.T) {
	for code, want := range map[Code]int{
		CodeInvalidRequest: 400, CodeUnauthenticated: 401, CodeForbidden: 403, CodeStatementForbidden: 403,
		CodeDatabaseNotFound: 404, CodeDatabaseExists: 409, CodeSequenceConflict: 409, CodeInvalidState: 409,
		CodeChannelLost: 410, CodeRequestTooLarge: 413, CodeTooManyChannels: 429, CodeOverloaded: 429,
		CodeSQLError: 400, CodeBusy: 200, CodeCommitOutcomeUnknown: 200, CodeNoLeader: 503, CodeBehind: 503,
		CodeMaintenance: 503, CodeInternal: 500, Code("other"): 500,
	} {
		if got := code.Status(); got != want {
			t.Errorf("%s: status %d, want %d", code, got, want)
		}
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
}

// TestDecodedMemory checks ADR 0005's charge of 8 times a body's length: the
// raw body once, and at most 7 bytes per body byte for what Parse allocates,
// for bodies of the smallest values.
func TestDecodedMemory(t *testing.T) {
	many := func(head, item, sep, tail string, n int) []byte {
		return []byte(head + strings.Repeat(item+sep, n-1) + item + tail)
	}
	var named strings.Builder
	named.WriteString(`{"sql":"x","named":{`)
	for i := range 100000 {
		if i > 0 {
			named.WriteByte(',')
		}
		fmt.Fprintf(&named, `"%x":0`, i)
	}
	named.WriteString(`}}`)
	for _, tc := range []struct {
		name string
		kind Kind
		body []byte
	}{
		{"params", KindQuery, many(`{"sql":"x","params":[`, "0", ",", "]}", 500000)},
		{"tagged", KindQuery, many(`{"sql":"x","params":[`, `{"blob":""}`, ",", "]}", 100000)},
		{"reals", KindQuery, many(`{"sql":"x","params":[`, `1e1`, ",", "]}", 200000)},
		{"named", KindQuery, []byte(named.String())},
		{"param sets", KindChannel, many(`{"channel":"c","seq":1,"op":"batch","sql":"x","param_sets":[`, "[]", ",", "]}", 10000)},
		{"statements", KindChannel, many(`{"channel":"c","seq":1,"op":"batch","statements":[`, `{"sql":"x"}`, ",", "]}", 10000)},
		{"one statement", KindChannel, []byte(`{"channel":"c","seq":1,"op":"batch","statements":[{"sql":"` + strings.Repeat("x", 20000) + `"}]}`)},
		{"sql", KindQuery, []byte(`{"sql":"` + strings.Repeat(`\n`, 500000) + `"}`)},
	} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		r, err := Parse(tc.kind, tc.body, 10000)
		runtime.ReadMemStats(&after)
		if err != nil || r.Validate() != nil {
			t.Fatalf("%s: %v %v", tc.name, err, r.Validate())
		}
		got, limit := after.TotalAlloc-before.TotalAlloc, 7*uint64(len(tc.body))
		t.Logf("%s: %.2f bytes allocated per body byte", tc.name, float64(got)/float64(len(tc.body)))
		if got > limit {
			t.Errorf("%s: Parse allocated %d bytes for a %d-byte body; the limit is %d", tc.name, got, len(tc.body), limit)
		}
	}
}
