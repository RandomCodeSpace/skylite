package wire

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// FuzzParse checks that Parse never panics, that every refusal has the
// flags of an answer before acceptance, that Bind never panics on what
// Parse accepted, and that no proper prefix of a body Parse accepts is
// accepted: a cut request is never a different valid request.
func FuzzParse(f *testing.F) {
	for _, ex := range exampleBodies {
		f.Add(uint8(ex.kind-KindQuery), []byte(ex.body))
	}
	f.Add(uint8(0), // KindQuery
		[]byte(`{"sql":"?","params":[{"real":"Infinity"},{"text_bytes":"/w=="},{"blob":""},-0,1e2,"\u0000"]}`))
	f.Fuzz(func(t *testing.T, kind uint8, body []byte) {
		k := KindQuery + Kind(kind%uint8(KindPut))
		r, err := Parse(k, body, 16)
		if err != nil {
			if r != nil || err.Code != CodeInvalidRequest || !err.NotExecuted || err.Final {
				t.Fatalf("refusal %+v with request %v", err, r)
			}
			return
		}
		_ = r.Validate()
		names := []string{"", ":a", "@b", "$c"}
		for _, a := range append(append([]Args{r.Args}, r.ParamSets...), statementArgs(r)...) {
			for n := range len(names) + 1 {
				_ = r.Bind(a, names[:n], func(int, any) error { return nil })
			}
		}
		for i := range len(body) {
			if p, err := Parse(k, body[:i], 16); err == nil {
				t.Fatalf("prefix %q of %q parsed as %+v", body[:i], body, p)
			}
		}
	})
}

func statementArgs(r *Request) []Args {
	var a []Args
	if r.Execute != nil {
		a = append(a, r.Execute.Args)
	}
	for _, s := range r.Statements {
		a = append(a, s.Args)
	}
	return a
}

// FuzzDecodeValue checks that a decoded value encodes to its canonical form,
// which decodes back to the same value and encodes to the same bytes.
func FuzzDecodeValue(f *testing.F) {
	for _, s := range []string{
		`null`, `1`, `-0`, `1e2`, `1.0`, `"a"`, `"é\/\n"`, `{"blob":"AAE="}`, `{"real":"Infinity"}`,
		`{"text_bytes":"/w=="}`, `{"blob":""}`, `[`, `}`, `{"a":{"b":[1,{"c":2}]}}`, ``,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := DecodeValue(data)
		if err != nil {
			return
		}
		enc, err := AppendValue(nil, v)
		if err != nil {
			t.Fatalf("AppendValue(%#v) from %q: %v", v, data, err)
		}
		v2, err := DecodeValue(enc)
		if err != nil {
			t.Fatalf("decoding %q, the encoding of %q: %v", enc, data, err)
		}
		if f1, ok := v.(float64); ok {
			if f2, ok := v2.(float64); !ok || math.Float64bits(f1) != math.Float64bits(f2) {
				t.Fatalf("%q -> %q: %v != %v", data, enc, v, v2)
			}
		} else if !reflect.DeepEqual(v, v2) {
			t.Fatalf("%q -> %q: %#v != %#v", data, enc, v, v2)
		}
		if enc2, _ := AppendValue(nil, v2); !bytes.Equal(enc, enc2) {
			t.Fatalf("not canonical: %q, then %q", enc, enc2)
		}
	})
}

// FuzzWriter drives a Writer with operations chosen by the input and checks
// that it never panics, that every frame it writes is one complete JSON
// value on one line, that the result size holds, and that nothing follows
// the terminal frame.
func FuzzWriter(f *testing.F) {
	f.Add([]byte{0, 1, 2, 2, 5, 7}, uint16(64), "a\x00\xff\"")
	f.Add([]byte{1, 3, 3, 0, 6}, uint16(200), ":p")
	f.Add([]byte{4, 4, 8, 9}, uint16(4096), "")
	f.Fuzz(func(t *testing.T, ops []byte, size uint16, text string) {
		l := DefaultLimits()
		l.FrameEntries = 3
		l.ResultBytes = int64(size)*4 + 1
		l.ErrorMessageBytes = 40
		var buf bytes.Buffer
		w, err := NewWriter(&buf, int(size)+1, l)
		if err != nil {
			t.Fatal(err)
		}
		ended := false
		for i, op := range ops {
			var err error
			switch op % 10 {
			case 0:
				err = w.Keepalive()
			case 1:
				err = w.Columns([]Column{{Name: text, DeclType: text}, {Name: "b"}})
			case 2:
				err = w.Row([]any{text, int64(i)})
			case 3:
				err = w.Params([]string{text, "", ":x", text})
			case 4:
				err = w.Results([]Result{{int64(i), -1}, {}, {}, {}})
			case 5:
				err = w.Row([]any{[]byte(text), math.Inf(-1)})
			case 6:
				err = w.Done(&Done{
					Kind: DoneKind(i % 16), Seq: int64(i), Channel: &Channel{Token: text}, Limits: &l,
					Outcome: &Receipt{Error: &Error{Code: CodeCancelled, RequestID: "r", Message: text}}, LastSeq: int64(i % 2),
				})
			case 7:
				err = w.Error(&Error{Code: CodeSQLError, RequestID: "r", Message: text + text + text})
			case 8:
				err = w.Done(&Done{Kind: DoneCancel, Outcome: &Receipt{Done: &Done{Kind: DoneKind(i % 16), Limits: &l}}})
			case 9:
				err = w.Row(nil)
			}
			if ended && err == nil {
				t.Fatalf("op %d succeeded after the terminal frame", op)
			}
			if err == nil && (op%10 == 6 || op%10 == 7 || op%10 == 8) {
				ended = true
			}
		}
		if w.Written() > l.ResultBytes {
			t.Fatalf("wrote %d bytes before the terminal frame, above %d", w.Written(), l.ResultBytes)
		}
		for _, line := range bytes.SplitAfter(buf.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			if line[len(line)-1] != '\n' || checkSyntax(line[:len(line)-1]) != nil {
				t.Fatalf("frame %q is not one JSON value on one line", line)
			}
		}
	})
}
