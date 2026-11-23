package auth

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf8"

	"github.com/go-jose/go-jose/v4"
)

// verifyOK fails the test if a fresh token signed with k does not verify.
func verifyOK(t *testing.T, g *Guard, ti *testIssuer, c *fakeClock, k signingKey, what string) {
	t.Helper()
	if _, err := g.Verify(context.Background(), sign(t, k, ti.claims(c))); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func verifyCode(t *testing.T, g *Guard, ti *testIssuer, c *fakeClock, k signingKey, want Code) {
	t.Helper()
	tok := sign(t, k, ti.claims(c))
	_, err := g.Verify(context.Background(), tok)
	wantCode(t, tok, err, want)
}

func TestKeyCacheLifetimeIsOneHour(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	verifyOK(t, g, ti, c, k, "first token")
	c.Add(time.Hour - time.Second)
	verifyOK(t, g, ti, c, k, "token just before the hour")
	if n := ti.jwksHits.Load(); n != 1 {
		t.Errorf("%d JWKS fetches within the hour, want 1", n)
	}
	c.Add(time.Second)
	verifyOK(t, g, ti, c, k, "token at the hour")
	if n := ti.jwksHits.Load(); n != 2 {
		t.Errorf("%d JWKS fetches once the hour passed, want 2", n)
	}
}

func TestStaleKeysLastTwentyFourHours(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	verifyOK(t, g, ti, c, k, "first token")
	ti.status.Store(http.StatusInternalServerError)
	c.Add(24 * time.Hour)
	verifyOK(t, g, ti, c, k, "token 24 h after the last fetch")
	if n := ti.discoveryHits.Load(); n != 2 {
		t.Errorf("%d discovery fetches, want 2: the stale key must still trigger a refresh", n)
	}
	c.Add(time.Second)
	verifyCode(t, g, ti, c, k, CodeKeysUnavailable)
}

func TestFailedFetchCountsAsAttempt(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	ti.status.Store(http.StatusInternalServerError)
	g := newGuard(t, c, ti)
	verifyCode(t, g, ti, c, k, CodeKeysUnavailable)
	verifyCode(t, g, ti, c, k, CodeKeysUnavailable)
	if n := ti.discoveryHits.Load(); n != 1 {
		t.Errorf("%d discovery fetches within a minute of a failed one, want 1", n)
	}
	ti.status.Store(0)
	c.Add(time.Minute)
	verifyOK(t, g, ti, c, k, "token a minute after the failed fetch")
}

func TestFetchRules(t *testing.T) {
	k := newKey(t, "k", jose.ES256)
	t.Run("status must be 200", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		ti.status.Store(http.StatusNonAuthoritativeInfo) // a 2xx with a valid body
		verifyCode(t, newGuard(t, c, ti), ti, c, k, CodeKeysUnavailable)
	})
	t.Run("jwks_uri must be https", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		var hits atomic.Int64
		plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = w.Write(jwksOf(t, k))
		}))
		t.Cleanup(plain.Close)
		ti.setDiscovery("jwks_uri", plain.URL+"/jwks")
		verifyCode(t, newGuard(t, c, ti), ti, c, k, CodeKeysUnavailable)
		if n := hits.Load(); n != 0 {
			t.Errorf("%d requests to an http jwks_uri", n)
		}
	})
	t.Run("redirects are not followed", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		var hits atomic.Int64
		other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": ti.url, "jwks_uri": ti.url + "/jwks"})
		}))
		t.Cleanup(other.Close)
		ti.setRedirect(other.URL + "/.well-known/openid-configuration")
		// other has the same certificate as ti, so only the redirect rule stops the request.
		g, logs := newGuardWith(t, c, ti.srv.Client(), ti)
		verifyCode(t, g, ti, c, k, CodeKeysUnavailable)
		if n := hits.Load(); n != 0 {
			t.Errorf("%d requests to the redirect target", n)
		}
		if strings.Contains(logs.String(), other.URL) {
			t.Error("the log names the redirect target")
		}
	})
	t.Run("trailing slash on the issuer", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		ti.setDiscovery("jwks_uri", ti.url+"/jwks")
		ti.url += "/" // the configured issuer, the discovery issuer and the tokens' iss
		verifyOK(t, newGuard(t, c, ti), ti, c, k, "issuer with a trailing slash")
	})
	t.Run("not cancelled by the caller", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		base := ti.srv.Client().Transport
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			return base.RoundTrip(r)
		})}
		g, _ := newGuardWith(t, c, client, ti)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := g.Verify(ctx, sign(t, k, ti.claims(c))); err != nil {
			t.Errorf("Verify with a cancelled context: %v", err)
		}
	})
	t.Run("10 s timeout", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		base := ti.srv.Client().Transport
		var before time.Time
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			after := time.Now()
			d, ok := r.Context().Deadline()
			if !ok || d.Before(before.Add(10*time.Second)) || d.After(after.Add(10*time.Second)) {
				t.Errorf("fetch deadline is not 10 s after the refresh began (set %v)", ok)
			}
			return base.RoundTrip(r)
		})}
		g, _ := newGuardWith(t, c, client, ti)
		before = time.Now()
		verifyOK(t, g, ti, c, k, "token")
	})
	t.Run("response without a request", func(t *testing.T) {
		c := newClock()
		ti := newIssuer(t, k)
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := jwksOf(t, k)
			if strings.HasSuffix(r.URL.Path, "/openid-configuration") {
				body, _ = json.Marshal(map[string]string{"issuer": ti.url, "jwks_uri": ti.url + "/jwks"})
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}, nil
		})}
		g, _ := newGuardWith(t, c, client, ti)
		verifyOK(t, g, ti, c, k, "token")
	})
}

func TestRefreshPanicReleasesTheLock(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	base := ti.srv.Client().Transport
	var panics atomic.Bool
	panics.Store(true)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if panics.Load() {
			panic("transport failure")
		}
		return base.RoundTrip(r)
	})}
	g, _ := newGuardWith(t, c, client, ti)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the transport's panic did not reach the caller")
			}
		}()
		_, _ = g.Verify(context.Background(), sign(t, k, ti.claims(c)))
	}()
	src := g.sources[ti.url]
	select {
	case src.fetching <- struct{}{}:
		<-src.fetching
	default:
		t.Fatal("the refresh lock is still held after a panic")
	}
	panics.Store(false)
	c.Add(time.Minute)
	verifyOK(t, g, ti, c, k, "token after the panic")
}

func TestStaleKeyDoesNotWaitForARefresh(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	verifyOK(t, g, ti, c, k, "first token")
	c.Add(2 * time.Hour)
	src := g.sources[ti.url]
	src.fetching <- struct{}{} // another request is refreshing; a blocking refresh would deadlock here
	defer func() { <-src.fetching }()
	verifyOK(t, g, ti, c, k, "stale key while a refresh runs")
	if n := ti.jwksHits.Load(); n != 1 {
		t.Errorf("%d JWKS fetches, want 1", n)
	}
}

// TestRefreshWaiter: a request that must wait for another's refresh calls
// its wait hook first, does not wait if the hook refuses, and gives up when
// its context ends, after which the hook's resume runs.
func TestRefreshWaiter(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	g := newGuard(t, c, ti)
	verifyOK(t, g, ti, c, k, "first token")
	c.Add(2 * minRefreshInterval)
	src := g.sources[ti.url]
	src.fetching <- struct{}{} // another request is refreshing
	defer func() { <-src.fetching }()
	unknown := sign(t, newKey(t, "other", jose.ES256), ti.claims(c))
	var waits, resumes int
	refuse := WithRefreshWait(context.Background(), func() (func(), bool) { waits++; return nil, false })
	_, err := g.Verify(refuse, unknown)
	wantCode(t, unknown, err, CodeUnknownKey)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ctx = WithRefreshWait(ctx, func() (func(), bool) { waits++; return func() { resumes++ }, true })
	_, err = g.Verify(ctx, unknown)
	wantCode(t, unknown, err, CodeUnknownKey)
	if waits != 2 || resumes != 1 {
		t.Errorf("%d waits and %d resumes, want 2 and 1", waits, resumes)
	}
	verifyOK(t, g, ti, c, k, "a known key while a refresh runs") // calls no hook: it has none and does not wait
}

// padded returns jwks padded with trailing white space to exactly n bytes.
func padded(t *testing.T, jwks []byte, n int) []byte {
	t.Helper()
	if len(jwks) > n {
		t.Fatalf("JWKS of %d bytes exceeds %d", len(jwks), n)
	}
	return append(jwks, bytes.Repeat([]byte(" "), n-len(jwks))...)
}

func TestDocumentSizeLimitIsOneMiB(t *testing.T) {
	k := newKey(t, "k", jose.ES256)
	for _, tc := range []struct {
		size int
		ok   bool
	}{{1 << 20, true}, {1<<20 + 1, false}} {
		t.Run(fmt.Sprintf("fetched %d bytes", tc.size), func(t *testing.T) {
			c := newClock()
			ti := newIssuer(t, k)
			ti.setJWKS(padded(t, jwksOf(t, k), tc.size))
			g := newGuard(t, c, ti)
			_, err := g.Verify(context.Background(), sign(t, k, ti.claims(c)))
			if (err == nil) != tc.ok {
				t.Errorf("JWKS of %d bytes: error %v", tc.size, err)
			}
		})
		t.Run(fmt.Sprintf("file of %d bytes", tc.size), func(t *testing.T) {
			c := newClock()
			file := filepath.Join(t.TempDir(), "jwks.json")
			if err := os.WriteFile(file, padded(t, jwksOf(t, k), tc.size), 0o600); err != nil {
				t.Fatal(err)
			}
			g, err := New(Config{Issuers: []Issuer{{Issuer: "k8s", Audience: testAudience, JWKSFile: file}}}, nil, c.Now, nil)
			if err != nil {
				t.Fatal(err)
			}
			now := c.Now().Unix()
			_, err = g.Verify(context.Background(), sign(t, k, map[string]any{"iss": "k8s", "sub": "s", "aud": testAudience, "exp": now + 600}))
			if (err == nil) != tc.ok {
				t.Errorf("JWKS file of %d bytes: error %v", tc.size, err)
			}
		})
	}
}

func TestParseKeySetRules(t *testing.T) {
	ec := newKey(t, "ec", jose.ES256)
	rsa2048 := newKey(t, "rsa", jose.RS256)
	rsa2047, err := rsa.GenerateKey(rand.Reader, 2047) //nolint:gosec // one bit below the minimum, to check it is rejected
	if err != nil {
		t.Fatal(err)
	}
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// jwk encodes a public key with the given members added.
	jwk := func(key any, kid string, extra map[string]any) json.RawMessage {
		b, err := json.Marshal(jose.JSONWebKey{Key: key, KeyID: kid})
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		for k, v := range extra {
			m[k] = v
		}
		if b, err = json.Marshal(m); err != nil {
			t.Fatal(err)
		}
		return b
	}
	pub := ec.priv.Public()
	set := []json.RawMessage{
		jwk(&rsa2047.PublicKey, "rsa-2047", nil),
		jwk(rsa2048.priv.Public(), "rsa-2048", nil),
		jwk(pub, "ops-encrypt", map[string]any{"key_ops": []string{"encrypt"}}),
		jwk(pub, "ops-empty", map[string]any{"key_ops": []string{}}),
		jwk(pub, "ops-not-a-list", map[string]any{"key_ops": "verify"}),
		jwk(pub, "ops-verify", map[string]any{"key_ops": []string{"sign", "verify"}}),
		jwk(pub, "ops-verify-only", map[string]any{"key_ops": []string{"verify"}}),
		jwk(pub, "ops-sign-only", map[string]any{"key_ops": []string{"sign"}}),
		jwk(&p384.PublicKey, "dup", nil), // not usable, so the next key with this id is kept
		jwk(pub, "dup", nil),
		jwk(pub, "dup2", nil),
		jwk(rsa2048.priv.Public(), "dup2", nil), // the first usable key with this id is kept
	}
	raw, err := json.Marshal(map[string]any{"keys": set})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := parseKeySet(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"rsa-2048": true, "ops-verify": true, "ops-verify-only": true, "dup": true, "dup2": true}
	for kid := range keys {
		if !want[kid] {
			t.Errorf("key %q accepted", kid)
		}
	}
	for kid := range want {
		if _, ok := keys[kid]; !ok {
			t.Errorf("key %q skipped", kid)
		}
	}
	for _, kid := range []string{"dup", "dup2"} {
		if _, ok := keys[kid].Key.(*ecdsa.PublicKey); !ok {
			t.Errorf("key %q is %T, want the P-256 key", kid, keys[kid].Key)
		}
	}
}

// TestRefreshLogIsBounded checks that values from a remote document reach the
// log cut to a bounded length.
func TestRefreshLogIsBounded(t *testing.T) {
	k := newKey(t, "k", jose.ES256)
	long := strings.Repeat("x", 100_000)
	for name, setup := range map[string]func(*testIssuer){
		"discovery issuer": func(ti *testIssuer) { ti.setDiscoveryIssuer(long) },
		"jwks_uri":         func(ti *testIssuer) { ti.setDiscovery("jwks_uri", "http://"+long) },
		"https jwks_uri":   func(ti *testIssuer) { ti.setDiscovery("jwks_uri", ti.url+"/"+long) },
		"unreachable jwks_uri": func(ti *testIssuer) {
			ti.setDiscovery("jwks_uri", "https://127.0.0.1:1/"+long) // nothing listens on port 1
		},
		"number":      func(ti *testIssuer) { ti.setDiscovery("issuer", json.Number("1"+strings.Repeat("0", 100_000))) },
		"redirect":    func(ti *testIssuer) { ti.setRedirect(ti.url + "/" + long) },
		"JWKS number": func(ti *testIssuer) { ti.setJWKS([]byte(`{"keys":` + "1" + strings.Repeat("0", 100_000) + `}`)) },
	} {
		t.Run(name, func(t *testing.T) {
			c := newClock()
			ti := newIssuer(t, k)
			setup(ti)
			g, logs := newGuardWith(t, c, ti.srv.Client(), ti)
			verifyCode(t, g, ti, c, k, CodeKeysUnavailable)
			out := logs.String()
			if !strings.Contains(out, "refreshing signing keys failed") {
				t.Fatal("no refresh failure logged")
			}
			if len(out) > 1000 {
				t.Errorf("refresh failure log is %d bytes", len(out))
			}
		})
	}
}

// TestColdRequestWaitsForTheRunningRefresh checks that a token arriving while
// another request fetches the first key set waits for that fetch instead of
// being refused. The test passes or fails without depending on timing: the
// pause only gives a refusing implementation the chance to return early.
func TestColdRequestWaitsForTheRunningRefresh(t *testing.T) {
	c := newClock()
	k := newKey(t, "k", jose.ES256)
	ti := newIssuer(t, k)
	base := ti.srv.Client().Transport
	var g *Guard
	var started atomic.Bool
	second := make(chan error, 1)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if started.CompareAndSwap(false, true) {
			tok := sign(t, k, ti.claims(c))
			go func() {
				_, err := g.Verify(context.Background(), tok)
				second <- err
			}()
			select {
			case err := <-second:
				second <- err
			case <-time.After(100 * time.Millisecond):
			}
		}
		return base.RoundTrip(r)
	})}
	g, _ = newGuardWith(t, c, client, ti)
	verifyOK(t, g, ti, c, k, "first token")
	if err := <-second; err != nil {
		t.Errorf("token that arrived during the first fetch: %v", err)
	}
}

// A logged value of exactly 200 bytes is whole, a longer one is cut to 200
// bytes, and a cut never leaves half a character.
func TestClip(t *testing.T) {
	whole := strings.Repeat("a", 200)
	if got := clip(whole); got != whole {
		t.Errorf("a 200-byte value was changed to %d bytes", len(got))
	}
	if got := clip(whole + "b"); got != whole+"..." {
		t.Errorf("a 201-byte value became %q", got)
	}
	got := clip(strings.Repeat("a", 199) + "\u20ac" + "tail") // the cut falls inside the three-byte character
	if got != strings.Repeat("a", 199)+"..." || !utf8.ValidString(got) {
		t.Errorf("a value cut inside a character became %q", got)
	}
}

// The name of a document that cannot be read or is too large is cut before it
// reaches an error.
func TestReadLimitedClipsTheName(t *testing.T) {
	name := "https://issuer.example/" + strings.Repeat("k", 500)
	for what, r := range map[string]io.Reader{
		"too large":  strings.NewReader(strings.Repeat("x", 1<<20+1)),
		"read error": iotest.ErrReader(errors.New(strings.Repeat("e", 500))),
	} {
		_, err := readLimited(r, name)
		if err == nil || len(err.Error()) > 500 || strings.Contains(err.Error(), name) {
			t.Errorf("%s: error of %d bytes: %.80v", what, len(fmt.Sprint(err)), err)
		}
	}
}
