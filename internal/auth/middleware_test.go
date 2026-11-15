package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
		{"several spaces after the scheme", []string{"Bearer   " + bob}, 204, "", "", ""},
		{"tab after the scheme", []string{"Bearer\t" + bob}, 401, "Bearer", "missing_token", CodeMissingToken},
		{"tab after a space", []string{"Bearer \t" + bob}, 401, `Bearer error="invalid_token"`, "invalid_token", CodeMalformed},
		{"something after the token", []string{"Bearer " + bob + " " + bob}, 401, `Bearer error="invalid_token"`, "invalid_token", CodeMalformed},
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
				for _, tok := range strings.Fields(v)[1:] {
					assertNoTokenMaterial(t, tok, rec.Body.String(), strings.Join(all, "\n"), logged)
				}
			}
		})
	}
	if !strings.Contains(logs.String(), `"token_sha256_prefix":"`) {
		t.Error("refusals are not logged with a token hash prefix")
	}
}

// refusalLog serves one request with the given Authorization header through
// h and returns the fields of the single log line it wrote.
func refusalLog(t *testing.T, h http.Handler, logs *lockedBuffer, header string) map[string]any {
	t.Helper()
	before := len(logs.String())
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	req.Header.Set("Authorization", header)
	h.ServeHTTP(httptest.NewRecorder(), req)
	lines := strings.Split(strings.TrimSpace(logs.String()[before:]), "\n")
	if len(lines) != 1 {
		t.Fatalf("%d log lines, want 1", len(lines))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRefusalLogFields(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	log, logs := newLogger()
	g, err := New(Config{Issuers: []Issuer{{Issuer: ti.url, Audience: testAudience}}}, ti.srv.Client(), c.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	db := "orders"
	h := g.Require(RoleWrite, func(*http.Request) string { return db }, http.NotFoundHandler())
	claims := func(sub string, exp int64) map[string]any {
		cl := ti.claims(c)
		cl["sub"], cl["exp"] = sub, exp
		return cl
	}
	expect := func(m map[string]any, want map[string]string, absent ...string) {
		t.Helper()
		for key, v := range want {
			if m[key] != v {
				t.Errorf("log %s = %v, want %q", key, m[key], v)
			}
		}
		for _, key := range absent {
			if _, ok := m[key]; ok {
				t.Errorf("log has %s", key)
			}
		}
	}
	hash := func(tok string) string {
		sum := sha256.Sum256([]byte(tok))
		return hex.EncodeToString(sum[:])[:12]
	}

	expired := sign(t, k, claims("bob", c.Now().Unix()-3600))
	expect(refusalLog(t, h, logs, "Bearer "+expired), map[string]string{
		"code": "token_expired", "issuer": ti.url, "subject": "bob", "kid": "k", "token_sha256_prefix": hash(expired),
	})

	forbidden := sign(t, k, claims("carol", c.Now().Unix()+600))
	expect(refusalLog(t, h, logs, "Bearer "+forbidden), map[string]string{
		"code": "forbidden", "issuer": ti.url, "subject": "carol", "database": "orders", "role": "write", "token_sha256_prefix": hash(forbidden),
	}, "kid")

	// Values from the token and the request are cut to 200 bytes.
	long := strings.Repeat("s", 300)
	db = strings.Repeat("d", 300)
	m := refusalLog(t, h, logs, "Bearer "+sign(t, k, claims(long, c.Now().Unix()+600)))
	expect(m, map[string]string{"subject": long[:200] + "...", "database": db[:200] + "..."})

	// A key id of 64 bytes is logged, one of 65 is not.
	for _, n := range []int{64, 65} {
		kid := strings.Repeat("k", n)
		tok := sign(t, signingKey{kid: kid, alg: k.alg, priv: k.priv}, claims("bob", c.Now().Unix()+600))
		m := refusalLog(t, h, logs, "Bearer "+tok)
		if n == 64 {
			expect(m, map[string]string{"code": "unknown_key", "kid": kid})
		} else {
			expect(m, map[string]string{"code": "unknown_key"}, "kid")
		}
	}
}
