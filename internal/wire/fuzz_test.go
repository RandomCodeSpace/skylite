package wire

import (
	"testing"
)

// FuzzParse checks that Parse never panics, that every refusal has the
// flags of an answer before acceptance, that Bind never panics on what
// Parse accepted, and that no proper prefix of a body Parse accepts is
// accepted: a cut request is never a different valid request.
func FuzzParse(f *testing.F) {
	for _, ex := range exampleBodies {
		f.Add(uint8(ex.kind), []byte(ex.body))
	}
	f.Add(uint8(KindQuery), []byte(`{"sql":"?","params":[{"real":"Infinity"},{"text_bytes":"/w=="},{"blob":""},-0,1e2,"\u0000"]}`))
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
