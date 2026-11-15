package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
)

func newGuard(t *testing.T, c *fakeClock, issuers ...*testIssuer) *Guard {
	t.Helper()
	g, _ := newGuardWith(t, c, issuers[0].srv.Client(), issuers...)
	return g
}

// newGuardWith is newGuard with the given client, returning the log as well.
func newGuardWith(t *testing.T, c *fakeClock, client *http.Client, issuers ...*testIssuer) (*Guard, *lockedBuffer) {
	t.Helper()
	cfg := Config{}
	for _, ti := range issuers {
		cfg.Issuers = append(cfg.Issuers, Issuer{Issuer: ti.url, Audience: testAudience})
	}
	log, logs := newLogger()
	g, err := New(cfg, client, c.Now, log)
	if err != nil {
		t.Fatal(err)
	}
	return g, logs
}

// wantCode fails unless err is an *Error with code want, and checks that the
// error text carries no token material.
func wantCode(t *testing.T, token string, err error, want Code) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error is %T, want *Error with code %s", err, want)
	}
	if e.Code != want {
		t.Errorf("code %s, want %s", e.Code, want)
	}
	assertNoTokenMaterial(t, token, err.Error(), fmt.Sprintf("%+v", e))
}

func TestVerifyAcceptsEachAlgorithm(t *testing.T) {
	for _, alg := range []jose.SignatureAlgorithm{jose.RS256, jose.ES256, jose.EdDSA} {
		t.Run(string(alg), func(t *testing.T) {
			c := newClock()
			k := newKey(t, "k-"+string(alg), alg)
			ti := newIssuer(t, k)
			g := newGuard(t, c, ti)
			claims := ti.claims(c)
			claims["groups"] = []string{"analysts"}
			p, err := g.Verify(context.Background(), sign(t, k, claims))
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if want := time.Unix(claims["exp"].(int64), 0).Add(60 * time.Second); !p.Expires.Equal(want) {
				t.Errorf("Expires %v, want %v", p.Expires, want)
			}
			if p.Issuer != ti.url || p.Subject != "system:serviceaccount:apps:orders" {
				t.Errorf("principal (%s, %s)", p.Issuer, p.Subject)
			}
			if groups, _ := p.Claims["groups"].([]any); len(groups) != 1 || groups[0] != "analysts" {
				t.Errorf("groups claim %v", p.Claims["groups"])
			}
		})
	}
}

func TestVerifyRejects(t *testing.T) {
	c := newClock()
	rsaKey := newKey(t, "rsa", jose.RS256)
	ecKey := newKey(t, "ec", jose.ES256)
	ti := newIssuer(t, rsaKey, ecKey)
	otherKey := newKey(t, "other", jose.RS256)
	other := newIssuer(t, otherKey)
	g := newGuard(t, c, ti, other)

	with := func(change func(map[string]any)) map[string]any {
		cl := ti.claims(c)
		change(cl)
		return cl
	}
	now := c.Now().Unix()

	pubDER, err := x509.MarshalPKIXPublicKey(rsaKey.priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	hmacToken := func(secret []byte) string {
		unsigned := rawToken(t, map[string]string{"alg": "HS256", "kid": "rsa", "typ": "JWT"}, ti.claims(c), nil)
		unsigned = strings.TrimSuffix(unsigned, ".")
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(unsigned))
		return rawToken(t, map[string]string{"alg": "HS256", "kid": "rsa", "typ": "JWT"}, ti.claims(c), mac.Sum(nil))
	}
	unknownSigner := newKey(t, "rsa", jose.RS256) // claims a published kid with a different key

	valid := sign(t, rsaKey, ti.claims(c))
	parts := strings.Split(valid, ".")
	forged, err := json.Marshal(with(func(m map[string]any) { m["sub"] = "admin" }))
	if err != nil {
		t.Fatal(err)
	}
	tamperedPayload := parts[0] + "." + base64.RawURLEncoding.EncodeToString(forged) + "." + parts[2]

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: rsaKey.priv}, (&jose.SignerOptions{}).WithHeader("kid", "rsa"))
	if err != nil {
		t.Fatal(err)
	}
	notJSON, err := signer.Sign([]byte("not json"))
	if err != nil {
		t.Fatal(err)
	}
	notJSONToken, err := notJSON.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	jsonSerialized := notJSON.FullSerialize()

	cases := []struct {
		name  string
		token string
		want  Code
	}{
		{"wrong issuer", sign(t, rsaKey, with(func(m map[string]any) { m["iss"] = "https://evil.example" })), CodeUntrustedIssuer},
		{"issuer differs by trailing slash", sign(t, rsaKey, with(func(m map[string]any) { m["iss"] = ti.url + "/" })), CodeUntrustedIssuer},
		{"key of another trusted issuer", sign(t, otherKey, ti.claims(c)), CodeUnknownKey},
		{"wrong audience", sign(t, rsaKey, with(func(m map[string]any) { m["aud"] = "someone-else" })), CodeInvalidAudience},
		{"expired 61 s ago", sign(t, rsaKey, with(func(m map[string]any) { m["exp"] = now - 61 })), CodeExpired},
		{"expired two minutes ago", sign(t, rsaKey, with(func(m map[string]any) { m["exp"] = now - 120 })), CodeExpired},
		{"valid in 61 s", sign(t, rsaKey, with(func(m map[string]any) { m["nbf"] = now + 61 })), CodeNotYetValid},
		{"issued in 61 s", sign(t, rsaKey, with(func(m map[string]any) { m["iat"] = now + 61 })), CodeIssuedInFuture},
		{"valid in two minutes", sign(t, rsaKey, with(func(m map[string]any) { m["nbf"] = now + 120 })), CodeNotYetValid},
		{"issued in the future", sign(t, rsaKey, with(func(m map[string]any) { m["iat"] = now + 3600 })), CodeIssuedInFuture},
		{"missing sub", sign(t, rsaKey, with(func(m map[string]any) { delete(m, "sub") })), CodeMissingClaim},
		{"empty sub", sign(t, rsaKey, with(func(m map[string]any) { m["sub"] = "" })), CodeMissingClaim},
		{"missing exp", sign(t, rsaKey, with(func(m map[string]any) { delete(m, "exp") })), CodeMissingClaim},
		{"missing aud", sign(t, rsaKey, with(func(m map[string]any) { delete(m, "aud") })), CodeMissingClaim},
		{"exp of the wrong type", sign(t, rsaKey, with(func(m map[string]any) { m["exp"] = "tomorrow" })), CodeMalformed},
		{"alg none", rawToken(t, map[string]string{"alg": "none", "typ": "JWT"}, ti.claims(c), nil), CodeUnsupportedAlgorithm},
		{"alg None", rawToken(t, map[string]string{"alg": "None", "kid": "rsa"}, ti.claims(c), []byte("x")), CodeUnsupportedAlgorithm},
		{"HMAC with the public key PEM as secret", hmacToken(pubPEM), CodeUnsupportedAlgorithm},
		{"HMAC with the public key DER as secret", hmacToken(pubDER), CodeUnsupportedAlgorithm},
		{"RS384 is not accepted", rawToken(t, map[string]string{"alg": "RS384", "kid": "rsa"}, ti.claims(c), []byte("sig")), CodeUnsupportedAlgorithm},
		{"alg names the other key type", rawToken(t, map[string]string{"alg": "ES256", "kid": "rsa"}, ti.claims(c), make([]byte, 64)), CodeInvalidSignature},
		{"published kid, different key", sign(t, unknownSigner, ti.claims(c)), CodeInvalidSignature},
		{"tampered payload", tamperedPayload, CodeInvalidSignature},
		{"truncated signature", valid[:len(valid)-4], CodeInvalidSignature},
		{"unknown kid", sign(t, newKey(t, "nobody", jose.ES256), ti.claims(c)), CodeUnknownKey},
		{"no kid", sign(t, signingKey{alg: rsaKey.alg, priv: rsaKey.priv}, ti.claims(c)), CodeUnknownKey},
		{"oversized", sign(t, rsaKey, with(func(m map[string]any) { m["pad"] = strings.Repeat("a", MaxTokenSize) })), CodeTokenTooLarge},
		{"empty", "", CodeMissingToken},
		{"16,384 bytes is not too large", strings.Repeat("a", 16384), CodeMalformed},
		{"16,385 bytes is too large", strings.Repeat("a", 16385), CodeTokenTooLarge},
		{"one segment", "abcdefghijkl", CodeMalformed},
		{"two segments", parts[0] + "." + parts[1], CodeMalformed},
		{"four segments", valid + ".abcd", CodeMalformed},
		{"five segments, JWE shaped", "eyJhbGciOiJSU0EtT0FFUCJ9.a.b.c.d", CodeMalformed},
		{"not base64", "!!!!.????.****", CodeMalformed},
		{"header not JSON", rawToken(t, []byte("not json"), ti.claims(c), []byte("sig")), CodeMalformed},
		{"payload not JSON, validly signed", notJSONToken, CodeMalformed},
		{"JSON serialization", jsonSerialized, CodeMalformed},
		{"whitespace inside", parts[0] + ". " + parts[1] + "." + parts[2], CodeMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := g.Verify(context.Background(), tc.token)
			wantCode(t, tc.token, err, tc.want)
		})
	}
}

func TestVerifyClockSkew(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	now := c.Now().Unix()
	const skew = 60
	for name, change := range map[string]func(map[string]any){
		"expired within skew":       func(m map[string]any) { m["exp"] = now - skew },
		"not yet valid within skew": func(m map[string]any) { m["nbf"] = now + skew },
		"issued within skew":        func(m map[string]any) { m["iat"] = now + skew },
		"audience among several":    func(m map[string]any) { m["aud"] = []string{"other", testAudience} },
		"audience as a string":      func(m map[string]any) { m["aud"] = testAudience },
	} {
		t.Run(name, func(t *testing.T) {
			cl := ti.claims(c)
			change(cl)
			if _, err := g.Verify(context.Background(), sign(t, k, cl)); err != nil {
				t.Errorf("Verify: %v", err)
			}
		})
	}
}

func TestUnknownKeyIDsDoNotCauseUnboundedFetches(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	if _, err := g.Verify(context.Background(), sign(t, k, ti.claims(c))); err != nil {
		t.Fatal(err)
	}
	if n := ti.jwksHits.Load(); n != 1 {
		t.Fatalf("%d JWKS fetches after the first token, want 1", n)
	}
	attacker := newKey(t, "", jose.ES256)
	tokens := make([]string, 50)
	for i := range tokens {
		attacker.kid = fmt.Sprintf("unknown-%d", i)
		tokens[i] = sign(t, attacker, ti.claims(c))
	}
	var wg sync.WaitGroup
	for i, tok := range tokens {
		wg.Go(func() {
			_, err := g.Verify(context.Background(), tok)
			var e *Error
			if !errors.As(err, &e) || e.Code != CodeUnknownKey {
				t.Errorf("token %d: got %v, want %s", i, err, CodeUnknownKey)
			}
		})
	}
	wg.Wait()
	// The first token's fetch is within minRefreshInterval, so none of the 50 may fetch.
	if n := ti.jwksHits.Load(); n != 1 {
		t.Errorf("%d JWKS fetches after 50 unknown key ids, want 1", n)
	}
	for range 10 {
		cl := ti.claims(c)
		cl["iss"] = "https://evil.example"
		_, err := g.Verify(context.Background(), sign(t, attacker, cl))
		wantCode(t, "", err, CodeUntrustedIssuer)
	}
	if n := ti.jwksHits.Load(); n != 1 {
		t.Errorf("%d JWKS fetches after tokens from an untrusted issuer, want 1", n)
	}
	c.Add(59 * time.Second)
	attacker.kid = "still-within-the-minute"
	_, err := g.Verify(context.Background(), sign(t, attacker, ti.claims(c)))
	wantCode(t, "", err, CodeUnknownKey)
	if n := ti.jwksHits.Load(); n != 1 {
		t.Errorf("%d JWKS fetches 59 s after the last, want 1", n)
	}
	c.Add(time.Second)
	for i := range 10 {
		attacker.kid = fmt.Sprintf("later-%d", i)
		_, err := g.Verify(context.Background(), sign(t, attacker, ti.claims(c)))
		wantCode(t, "", err, CodeUnknownKey)
	}
	if n := ti.jwksHits.Load(); n != 2 {
		t.Errorf("%d JWKS fetches after the interval passed, want 2", n)
	}
	if n := ti.discoveryHits.Load(); n != 2 {
		t.Errorf("%d discovery fetches, want 2", n)
	}
}

func TestKeyRotation(t *testing.T) {
	c := newClock()
	oldKey := newKey(t, "old", jose.RS256)
	newK := newKey(t, "new", jose.EdDSA)
	ti := newIssuer(t, oldKey)
	g := newGuard(t, c, ti)
	if _, err := g.Verify(context.Background(), sign(t, oldKey, ti.claims(c))); err != nil {
		t.Fatal(err)
	}
	c.Add(minRefreshInterval)
	ti.publish(t, newK)
	if _, err := g.Verify(context.Background(), sign(t, newK, ti.claims(c))); err != nil {
		t.Fatalf("token signed with the new key: %v", err)
	}
	tok := sign(t, oldKey, ti.claims(c))
	_, err := g.Verify(context.Background(), tok)
	wantCode(t, tok, err, CodeUnknownKey)

	// A known key is refreshed once the cache lifetime passes, so a removed key stops working.
	c.Add(keyCacheLifetime)
	ti.publish(t, oldKey)
	if _, err := g.Verify(context.Background(), sign(t, newK, ti.claims(c))); err == nil {
		t.Error("token signed with a key removed from the JWKS was accepted after the cache expired")
	}
}

func TestIssuerUnreachable(t *testing.T) {
	t.Run("no cached keys", func(t *testing.T) {
		c := newClock()
		k := newKey(t, "k", jose.ES256)
		ti := newIssuer(t, k)
		g := newGuard(t, c, ti)
		ti.srv.Close()
		tok := sign(t, k, ti.claims(c))
		_, err := g.Verify(context.Background(), tok)
		wantCode(t, tok, err, CodeKeysUnavailable)
	})
	t.Run("cached keys", func(t *testing.T) {
		c := newClock()
		k := newKey(t, "k", jose.ES256)
		ti := newIssuer(t, k)
		g := newGuard(t, c, ti)
		if _, err := g.Verify(context.Background(), sign(t, k, ti.claims(c))); err != nil {
			t.Fatal(err)
		}
		ti.srv.Close()
		c.Add(keyCacheLifetime + time.Minute)
		if _, err := g.Verify(context.Background(), sign(t, k, ti.claims(c))); err != nil {
			t.Fatalf("cached key past its lifetime, within the stale bound: %v", err)
		}
		tok := sign(t, newKey(t, "unknown", jose.ES256), ti.claims(c))
		_, err := g.Verify(context.Background(), tok)
		wantCode(t, tok, err, CodeUnknownKey)
		c.Add(keyMaxStale)
		tok = sign(t, k, ti.claims(c))
		_, err = g.Verify(context.Background(), tok)
		wantCode(t, tok, err, CodeKeysUnavailable)
	})
	t.Run("discovery names another issuer", func(t *testing.T) {
		c := newClock()
		k := newKey(t, "k", jose.ES256)
		ti := newIssuer(t, k)
		ti.setDiscoveryIssuer("https://evil.example")
		g := newGuard(t, c, ti)
		tok := sign(t, k, ti.claims(c))
		_, err := g.Verify(context.Background(), tok)
		wantCode(t, tok, err, CodeKeysUnavailable)
	})
	t.Run("JWKS too large", func(t *testing.T) {
		c := newClock()
		k := newKey(t, "k", jose.ES256)
		ti := newIssuer(t, k)
		// A valid key set, padded past the limit.
		jwks := jwksOf(t, k)
		ti.setJWKS(append(jwks[:len(jwks)-1], `,"pad":"`+strings.Repeat("a", maxDocumentSize)+`"}`...))
		g := newGuard(t, c, ti)
		tok := sign(t, k, ti.claims(c))
		_, err := g.Verify(context.Background(), tok)
		wantCode(t, tok, err, CodeKeysUnavailable)
	})
}

func TestParseKeySetSkipsUnusableKeys(t *testing.T) {
	good := newKey(t, "good", jose.ES256)
	small, err := rsa.GenerateKey(rand.Reader, 1024) //nolint:gosec // a key below the minimum, to check it is rejected
	if err != nil {
		t.Fatal(err)
	}
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var set jose.JSONWebKeySet
	set.Keys = append(set.Keys,
		jose.JSONWebKey{Key: good.priv, KeyID: "private-published", Algorithm: "ES256"}, // reduced to its public part
		jose.JSONWebKey{Key: []byte("secret-secret-secret"), KeyID: "hmac", Algorithm: "HS256"},
		jose.JSONWebKey{Key: good.priv.Public(), KeyID: "enc", Use: "enc"},
		jose.JSONWebKey{Key: good.priv.Public()}, // no kid
		jose.JSONWebKey{Key: &small.PublicKey, KeyID: "rsa-1024"},
		jose.JSONWebKey{Key: &p384.PublicKey, KeyID: "p384"},
		jose.JSONWebKey{Key: good.priv.Public(), KeyID: "good", Use: "sig"},
	)
	raw, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := parseKeySet(raw)
	if err != nil {
		t.Fatal(err)
	}
	pub := keys["private-published"]
	if len(keys) != 2 || keys["good"].Key == nil || !pub.IsPublic() {
		t.Errorf("parsed %d keys, want only good and private-published, the latter public", len(keys))
	}
	if _, err := parseKeySet([]byte(`{"keys":[{"kty":"oct","k":"c2VjcmV0","kid":"x"}]}`)); err == nil {
		t.Error("a JWKS with only a symmetric key parsed without error")
	}
}

func TestJWKSFile(t *testing.T) {
	c := newClock()
	k1 := newKey(t, "k1", jose.RS256)
	k2 := newKey(t, "k2", jose.ES256)
	file := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(file, jwksOf(t, k1), 0o600); err != nil {
		t.Fatal(err)
	}
	const iss = "https://kubernetes.default.svc.cluster.local"
	g, err := New(Config{Issuers: []Issuer{{Issuer: iss, Audience: testAudience, JWKSFile: file}}}, nil, c.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	claims := func() map[string]any {
		now := c.Now().Unix()
		return map[string]any{"iss": iss, "sub": "system:serviceaccount:apps:orders", "aud": testAudience, "exp": now + 600}
	}
	if _, err := g.Verify(context.Background(), sign(t, k1, claims())); err != nil {
		t.Fatalf("token signed with the file's key: %v", err)
	}
	// A rotated file is read again for an unknown key id, once the refresh interval has passed.
	if err := os.WriteFile(file, jwksOf(t, k2), 0o600); err != nil {
		t.Fatal(err)
	}
	tok := sign(t, k2, claims())
	_, err = g.Verify(context.Background(), tok)
	wantCode(t, tok, err, CodeUnknownKey)
	c.Add(minRefreshInterval)
	if _, err := g.Verify(context.Background(), sign(t, k2, claims())); err != nil {
		t.Fatalf("token signed with the rotated file's key: %v", err)
	}

	missing, err := New(Config{Issuers: []Issuer{{Issuer: "k8s", Audience: testAudience, JWKSFile: filepath.Join(t.TempDir(), "absent")}}}, nil, c.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	cl := claims()
	cl["iss"] = "k8s"
	tok = sign(t, k1, cl)
	_, err = missing.Verify(context.Background(), tok)
	wantCode(t, tok, err, CodeKeysUnavailable)
}

func TestVerifyRejectsKeyPublishedForAnotherAlgorithm(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.RS256)
	ti := newIssuer(t, k)
	b, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: k.priv.Public(), KeyID: "k", Algorithm: "RS512", Use: "sig"}}})
	if err != nil {
		t.Fatal(err)
	}
	ti.setJWKS(b)
	g := newGuard(t, c, ti)
	tok := sign(t, k, ti.claims(c))
	_, err = g.Verify(context.Background(), tok)
	wantCode(t, tok, err, CodeInvalidSignature)
}

func TestTokenWithoutKeyIDCausesNoFetch(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	verifyOK(t, g, ti, c, k, "first token")
	c.Add(time.Minute)
	verifyCode(t, g, ti, c, signingKey{alg: k.alg, priv: k.priv}, CodeUnknownKey)
	if n := ti.jwksHits.Load(); n != 1 {
		t.Errorf("%d JWKS fetches after a token without a kid, want 1", n)
	}
}

func TestErrorSubjectIsBounded(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	cl := ti.claims(c)
	cl["sub"], cl["exp"] = strings.Repeat("s", 300), c.Now().Unix()-3600
	_, err := g.Verify(context.Background(), sign(t, k, cl))
	e, ok := errors.AsType[*Error](err)
	if !ok || e.Subject != strings.Repeat("s", 200)+"..." {
		t.Errorf("error subject is not cut to 200 bytes (%v)", err)
	}
}
