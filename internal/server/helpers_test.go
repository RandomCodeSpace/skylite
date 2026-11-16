package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/RandomCodeSpace/skylite/internal/auth"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// testIssuer has no discovery document: its keys come from a JWKS file, so
// a fetch from it would fail and fail the test.
const (
	testIssuer   = "https://issuer.invalid"
	testAudience = "skylite"
)

// signer signs tokens for testIssuer.
type signer struct {
	kid string
	key *ecdsa.PrivateKey
}

func newSigner(t *testing.T, kid string) signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return signer{kid: kid, key: k}
}

// jwksFile writes a JWKS of the signers' public keys and returns its path.
func jwksFile(t *testing.T, signers ...signer) string {
	t.Helper()
	var set jose.JSONWebKeySet
	for _, s := range signers {
		set.Keys = append(set.Keys, jose.JSONWebKey{Key: s.key.Public(), KeyID: s.kid, Algorithm: string(jose.ES256), Use: "sig"})
	}
	b, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func (s signer) token(t *testing.T, subject string) string {
	t.Helper()
	js, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: s.key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", s.kid))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tok, err := jwt.Signed(js).Claims(map[string]any{
		"iss": testIssuer, "sub": subject, "aud": testAudience, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// testConfig is a plaintext configuration on a local directory: "admin" is
// admin on every database, "reader" reads every database, and "scoped" is
// admin on the databases starting with "b-".
func testConfig(t *testing.T, jwks string) Config {
	t.Helper()
	rule := func(sub, db string, role auth.Role) auth.Rule {
		return auth.Rule{Issuer: testIssuer, Subject: sub, Database: db, Role: role}
	}
	return Config{
		Listen:    "127.0.0.1:0",
		Plaintext: true,
		Auth: auth.Config{
			Issuers: []auth.Issuer{{Issuer: testIssuer, Audience: testAudience, JWKSFile: jwks}},
			Rules:   []auth.Rule{rule("admin", "*", auth.RoleAdmin), rule("reader", "*", auth.RoleRead), rule("scoped", "b-*", auth.RoleAdmin)},
		},
		Storage: Storage{Prefix: "dbs", LocalFS: &LocalFS{Path: t.TempDir()}},
	}
}

// lockedBuffer collects log output from concurrent handlers.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// waitFor waits until the log holds n lines containing every substring of want.
func (b *lockedBuffer) waitFor(t *testing.T, n int, want ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := 0
		for line := range strings.Lines(b.String()) {
			ok := true
			for _, w := range want {
				ok = ok && strings.Contains(line, w)
			}
			if ok {
				got++
			}
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log has %d lines with %q, want %d; log:\n%s", got, want, n, b.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// countingStore counts the mutations a server sends.
type countingStore struct {
	storage.Store
	creates atomic.Int64
}

func (s *countingStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	s.creates.Add(1)
	return s.Store.Create(ctx, key, body)
}

// env is a server under test with its signer and log.
type env struct {
	s     *Server
	sig   signer
	logs  *lockedBuffer
	store *countingStore
	addr  string // set by start
}

// newEnv returns a server on a local directory store, wrapped by wrap if
// it is not nil.
func newEnv(t *testing.T, wrap func(storage.Store) storage.Store) *env {
	t.Helper()
	sig := newSigner(t, "k1")
	cfg := testConfig(t, jwksFile(t, sig))
	st, err := localfs.Open(cfg.Storage.LocalFS.Path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cs := &countingStore{Store: st}
	var inner storage.Store = cs
	if wrap != nil {
		inner = wrap(cs)
	}
	logs := &lockedBuffer{}
	s, err := newServer(cfg, slog.New(slog.NewJSONHandler(logs, nil)), inner)
	if err != nil {
		t.Fatal(err)
	}
	return &env{s: s, sig: sig, logs: logs, store: cs}
}

// start serves e on a local port until the test ends.
func (e *env) start(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e.addr = ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
}

func h1Client() *http.Client { return &http.Client{Transport: &http.Transport{}} }

func h2cClient() *http.Client {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	return &http.Client{Transport: &http.Transport{Protocols: &p}}
}

// reply is the part of a response the tests check; put has closed its body.
type reply struct {
	Status     string
	StatusCode int
	ProtoMajor int
	Header     http.Header
}

// put sends PUT /v1/databases/{db} with body {} and the token, if any.
func (e *env) put(t *testing.T, c *http.Client, db, token string) (reply, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, "http://"+e.addr+"/v1/databases/"+db, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return reply{resp.Status, resp.StatusCode, resp.ProtoMajor, resp.Header}, frame(t, resp.Body)
}

// frame reads a body of exactly one NDJSON frame.
func frame(t *testing.T, r io.Reader) map[string]any {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(b, []byte("\n")) != 1 || !bytes.HasSuffix(b, []byte("\n")) {
		t.Fatalf("body %q is not one frame", b)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("frame %q: %v", b, err)
	}
	return m
}

// errorCode returns the code of an error frame, or "" for another frame.
func errorCode(m map[string]any) string {
	e, _ := m["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

// raw sends request text over a new HTTP/1.1 connection and reads the answer.
func raw(t *testing.T, addr, request string) *http.Response {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// putHead is the start of an HTTP/1.1 PUT, up to its last header line.
func putHead(db, token string) string {
	return "PUT /v1/databases/" + db + " HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer " + token + "\r\n"
}
