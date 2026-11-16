package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
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
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

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
// admin on every database, "writer" writes and "reader" reads every
// database, and "scoped" is admin on the databases starting with "b-".
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
			Rules:   []auth.Rule{rule("admin", "*", auth.RoleAdmin), rule("writer", "*", auth.RoleWrite), rule("reader", "*", auth.RoleRead), rule("scoped", "b-*", auth.RoleAdmin)},
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
	addr  string       // set by start
	halt  func() error // set by newQueryEnv: stops the server and returns Serve's error
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

// start serves e on a local port. The server stops when the test ends, or
// earlier when the test calls the function start returns, which returns
// what Serve returned.
func (e *env) start(t *testing.T) (stop func() error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e.addr = ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.s.Serve(ctx, ln) }()
	var once sync.Once
	var serr error
	var called bool // the test checks what it got
	halt := func() error {
		once.Do(func() { cancel(); serr = <-done })
		return serr
	}
	t.Cleanup(func() {
		if err := halt(); err != nil && !called {
			t.Errorf("Serve: %v", err)
		}
	})
	return func() error { called = true; return halt() }
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

// raw sends request text over a new HTTP/1.1 connection and reads the
// answer, which must come within 5 seconds.
func raw(t *testing.T, addr, request string) *http.Response {
	t.Helper()
	resp, _, _ := rawConn(t, addr, request)
	return resp
}

// rawConn is raw, also returning the connection and its reader, positioned
// after the response's header block.
func rawConn(t *testing.T, addr, request string) (*http.Response, net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp, c, br
}

// closedWithin reports whether the server closes c, or the connection under
// r, within d, rather than d passing first.
func closedWithin(c net.Conn, r io.Reader, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, r)
	var ne net.Error
	return !errors.As(err, &ne) || !ne.Timeout()
}

// h2Conn is a prior-knowledge HTTP/2 connection driven frame by frame.
type h2Conn struct {
	net.Conn
	fr       *http2.Framer
	settings map[http2.SettingID]uint32 // the server's first SETTINGS
}

// dialH2 opens an h2Conn, sends settings, and acknowledges the server's.
func dialH2(t *testing.T, addr string, settings ...http2.Setting) *h2Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	h := &h2Conn{Conn: c, fr: http2.NewFramer(c, c), settings: map[http2.SettingID]uint32{}}
	h.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	h.must(t, func() error { _, err := io.WriteString(c, http2.ClientPreface); return err }())
	h.must(t, h.fr.WriteSettings(settings...))
	for {
		f, err := h.fr.ReadFrame()
		h.must(t, err)
		if sf, ok := f.(*http2.SettingsFrame); ok && !sf.IsAck() {
			h.must(t, sf.ForeachSetting(func(s http2.Setting) error { h.settings[s.ID] = s.Val; return nil }))
			h.must(t, h.fr.WriteSettingsAck())
			return h
		}
	}
}

func (h *h2Conn) must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// block encodes header fields given as name, value pairs.
func block(fields ...string) []byte {
	var b bytes.Buffer
	enc := hpack.NewEncoder(&b)
	for i := 0; i < len(fields); i += 2 {
		_ = enc.WriteField(hpack.HeaderField{Name: fields[i], Value: fields[i+1]})
	}
	return b.Bytes()
}

// h2Put returns the header fields of a PUT of database db with token, if
// any, and the extra fields.
func h2Put(db, token string, extra ...string) []string {
	f := []string{":method", "PUT", ":scheme", "http", ":authority", "skylite", ":path", "/v1/databases/" + db}
	if token != "" {
		f = append(f, "authorization", "Bearer "+token)
	}
	return append(f, extra...)
}

// status reads frames until the response header block of stream id and
// returns its status, or "" if the connection ends or d passes first.
func (h *h2Conn) status(id uint32, d time.Duration) string {
	_ = h.SetReadDeadline(time.Now().Add(d))
	for {
		f, err := h.fr.ReadFrame()
		if err != nil {
			return ""
		}
		if mh, ok := f.(*http2.MetaHeadersFrame); ok && mh.StreamID == id {
			return mh.PseudoValue("status")
		}
	}
}

// gateStore holds every create until release is closed, then fails it if
// its context has ended, as a network store would. entered is closed when
// the first create arrives.
type gateStore struct {
	storage.Store
	entered, release chan struct{}
	once             *sync.Once
}

func newGateStore(st storage.Store) gateStore {
	return gateStore{st, make(chan struct{}), make(chan struct{}), &sync.Once{}}
}

func (s gateStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return s.Store.Create(ctx, key, body)
}

// putHead is the start of an HTTP/1.1 PUT, up to its last header line.
func putHead(db, token string) string {
	return "PUT /v1/databases/" + db + " HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer " + token + "\r\n"
}
