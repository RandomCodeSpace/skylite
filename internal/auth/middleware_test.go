package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-jose/go-jose/v4"
)

func TestRequire(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	log, logs := newLogger()
	g, err := New(Config{
		Issuers: []Issuer{{Issuer: ti.url, Audience: testAudience}},
		Rules: []Rule{
			{Issuer: ti.url, Subject: "bob", Database: "orders", Role: RoleWrite},
			{Issuer: ti.url, Subject: "alice", Database: "orders", Role: RoleRead},
		},
	}, ti.srv.Client(), c.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	token := func(sub string, change func(map[string]any)) string {
		cl := ti.claims(c)
		cl["sub"] = sub
		if change != nil {
			change(cl)
		}
		return sign(t, k, cl)
	}
	var served *Principal
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok {
			t.Error("no principal in the request context")
		}
		served = p
		w.WriteHeader(http.StatusNoContent)
	})
	h := g.Require(RoleWrite, func(*http.Request) string { return "orders" }, next)

	bob := token("bob", nil)
	expired := token("bob", func(m map[string]any) { m["exp"] = c.Now().Unix() - 3600 })
	badSig := bob[:len(bob)-6] + "AAAAAA"
	if badSig == bob {
		t.Fatal("tampering left the token unchanged")
	}
	cases := []struct {
		name      string
		headers   []string
		status    int
		challenge string
		body      string
		code      Code
	}{
		{"no header", nil, 401, "Bearer", "missing_token", CodeMissingToken},
		{"other scheme", []string{"Basic " + bob}, 401, "Bearer", "missing_token", CodeMissingToken},
		{"scheme only", []string{"Bearer"}, 401, "Bearer", "missing_token", CodeMissingToken},
		{"empty token", []string{"Bearer "}, 401, "Bearer", "missing_token", CodeMissingToken},
		{"two headers", []string{"Bearer " + bob, "Bearer " + bob}, 401, `Bearer error="invalid_token"`, "invalid_token", CodeMalformed},
		{"expired", []string{"Bearer " + expired}, 401, `Bearer error="invalid_token"`, "invalid_token", CodeExpired},
		{"bad signature", []string{"Bearer " + badSig}, 401, `Bearer error="invalid_token"`, "invalid_token", CodeInvalidSignature},
		{"garbage", []string{"Bearer not-a-token"}, 401, `Bearer error="invalid_token"`, "invalid_token", CodeMalformed},
		{"no role", []string{"Bearer " + token("carol", nil)}, 403, `Bearer error="insufficient_scope"`, "forbidden", CodeForbidden},
		{"read only", []string{"Bearer " + token("alice", nil)}, 403, `Bearer error="insufficient_scope"`, "forbidden", CodeForbidden},
		{"write", []string{"Bearer " + bob}, 204, "", "", ""},
		{"lower-case scheme", []string{"bearer " + bob}, 204, "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			served = nil
			before := len(logs.String())
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/db/orders/query", nil)
			for _, v := range tc.headers {
				req.Header.Add("Authorization", v)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tc.status {
				t.Errorf("status %d, want %d", rec.Code, tc.status)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got != tc.challenge {
				t.Errorf("WWW-Authenticate %q, want %q", got, tc.challenge)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tc.body {
				t.Errorf("body %q, want %q", got, tc.body)
			}
			logged := logs.String()[before:]
			if tc.status == 204 {
				if served == nil || served.Subject != "bob" {
					t.Error("next did not see bob's principal")
				}
			} else {
				if served != nil {
					t.Error("next was called for a refused request")
				}
				if !strings.Contains(logged, `"code":"`+string(tc.code)+`"`) {
					t.Errorf("refusal log does not name code %s", tc.code)
				}
			}
			var all []string
			for name, values := range rec.Header() {
				all = append(all, name+": "+strings.Join(values, ","))
			}
			for _, v := range tc.headers {
				_, tok, _ := strings.Cut(v, " ")
				assertNoTokenMaterial(t, tok, rec.Body.String(), strings.Join(all, "\n"), logged)
			}
		})
	}
	if !strings.Contains(logs.String(), `"token_sha256_prefix":"`) {
		t.Error("refusals are not logged with a token hash prefix")
	}
}
