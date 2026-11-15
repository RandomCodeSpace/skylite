package auth

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const testAudience = "skylite-test"

// fakeClock is the injected clock; tests move it explicitly.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type signingKey struct {
	kid  string
	alg  jose.SignatureAlgorithm
	priv crypto.Signer
}

func newKey(t *testing.T, kid string, alg jose.SignatureAlgorithm) signingKey {
	t.Helper()
	var priv crypto.Signer
	var err error
	switch alg {
	case jose.RS256:
		priv, err = rsa.GenerateKey(rand.Reader, 2048)
	case jose.ES256:
		priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case jose.EdDSA:
		_, priv, err = ed25519.GenerateKey(rand.Reader)
	default:
		t.Fatalf("no key type for %s", alg)
	}
	if err != nil {
		t.Fatal(err)
	}
	return signingKey{kid: kid, alg: alg, priv: priv}
}

func jwksOf(t *testing.T, keys ...signingKey) []byte {
	t.Helper()
	var set jose.JSONWebKeySet
	for _, k := range keys {
		set.Keys = append(set.Keys, jose.JSONWebKey{Key: k.priv.Public(), KeyID: k.kid, Algorithm: string(k.alg), Use: "sig"})
	}
	b, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// testIssuer is an OIDC issuer served over TLS by the test: a discovery
// document and a JWKS of the keys it currently publishes.
type testIssuer struct {
	srv           *httptest.Server
	url           string
	mu            sync.Mutex
	jwks          []byte
	discovery     map[string]any // members that replace those of the discovery document
	redirect      string         // if set, discovery answers with a redirect to it
	status        atomic.Int64   // if set, the status of every response; the body is unchanged
	jwksHits      atomic.Int64
	discoveryHits atomic.Int64
}

func newIssuer(t *testing.T, keys ...signingKey) *testIssuer {
	t.Helper()
	ti := &testIssuer{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		ti.discoveryHits.Add(1)
		doc := map[string]any{"issuer": ti.url, "jwks_uri": ti.url + "/jwks"}
		ti.mu.Lock()
		for k, v := range ti.discovery {
			doc[k] = v
		}
		redirect := ti.redirect
		ti.mu.Unlock()
		if redirect != "" {
			w.Header().Set("Location", redirect)
			w.WriteHeader(http.StatusFound)
			return
		}
		ti.writeStatus(w)
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("GET /jwks", func(w http.ResponseWriter, _ *http.Request) {
		ti.jwksHits.Add(1)
		ti.mu.Lock()
		b := ti.jwks
		ti.mu.Unlock()
		ti.writeStatus(w)
		_, _ = w.Write(b)
	})
	ti.srv = httptest.NewTLSServer(mux)
	t.Cleanup(ti.srv.Close)
	ti.url = ti.srv.URL
	ti.publish(t, keys...)
	return ti
}

func (ti *testIssuer) publish(t *testing.T, keys ...signingKey) {
	t.Helper()
	b := jwksOf(t, keys...)
	ti.mu.Lock()
	ti.jwks = b
	ti.mu.Unlock()
}

func (ti *testIssuer) setJWKS(b []byte) {
	ti.mu.Lock()
	ti.jwks = b
	ti.mu.Unlock()
}

func (ti *testIssuer) writeStatus(w http.ResponseWriter) {
	if s := ti.status.Load(); s != 0 {
		w.WriteHeader(int(s))
	}
}

// setDiscovery sets member k of the discovery document to v.
func (ti *testIssuer) setDiscovery(k string, v any) {
	ti.mu.Lock()
	if ti.discovery == nil {
		ti.discovery = map[string]any{}
	}
	ti.discovery[k] = v
	ti.mu.Unlock()
}

func (ti *testIssuer) setDiscoveryIssuer(iss string) { ti.setDiscovery("issuer", iss) }

func (ti *testIssuer) setRedirect(u string) {
	ti.mu.Lock()
	ti.redirect = u
	ti.mu.Unlock()
}

// roundTripFunc is an http.RoundTripper for tests that observe or replace requests.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// claims are valid claims for ti at the clock's time; tests then change them.
func (ti *testIssuer) claims(c *fakeClock) map[string]any {
	now := c.Now().Unix()
	return map[string]any{
		"iss": ti.url,
		"sub": "system:serviceaccount:apps:orders",
		"aud": []string{testAudience},
		"exp": now + 600,
		"iat": now,
		"nbf": now,
	}
}

func sign(t *testing.T, k signingKey, claims map[string]any) string {
	t.Helper()
	opts := (&jose.SignerOptions{}).WithType("JWT")
	if k.kid != "" {
		opts = opts.WithHeader("kid", k.kid)
	}
	s, err := jose.NewSigner(jose.SigningKey{Algorithm: k.alg, Key: k.priv}, opts)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.Signed(s).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// rawToken assembles a compact JWS from parts the test controls entirely.
func rawToken(t *testing.T, header, payload any, sig []byte) string {
	t.Helper()
	enc := func(v any) string {
		b, ok := v.([]byte)
		if !ok {
			var err error
			if b, err = json.Marshal(v); err != nil {
				t.Fatal(err)
			}
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(header) + "." + enc(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// lockedBuffer collects log output from concurrent requests.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newLogger() (*slog.Logger, *lockedBuffer) {
	buf := &lockedBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// assertNoTokenMaterial fails if any output contains the token or one of its
// segments. It never prints the token.
func assertNoTokenMaterial(t *testing.T, token string, outputs ...string) {
	t.Helper()
	segments := append(strings.Split(token, "."), token)
	for i, out := range outputs {
		for j, seg := range segments {
			if len(seg) >= 8 && strings.Contains(out, seg) {
				t.Errorf("output %d contains token segment %d", i, j)
			}
		}
	}
}
