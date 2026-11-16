package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

func TestPutCreatesOnce(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	admin := e.sig.token(t, "admin")
	for _, c := range []struct {
		name   string
		client *http.Client
		proto  int
	}{{"HTTP/1.1", h1Client(), 1}, {"HTTP/2", h2cClient(), 2}} {
		t.Run(c.name, func(t *testing.T) {
			db := "orders-" + strconv.Itoa(c.proto)
			resp, m := e.put(t, c.client, db, admin)
			if resp.StatusCode != http.StatusOK || resp.ProtoMajor != c.proto {
				t.Fatalf("first PUT: HTTP/%d %s %v", resp.ProtoMajor, resp.Status, m)
			}
			done, _ := m["done"].(map[string]any)
			if len(done) != 2 || done["txn"] != 1.0 || done["epoch"] != 1.0 {
				t.Errorf("done = %v, want txn 1 and epoch 1", m)
			}
			for k, v := range map[string]string{"Skylite-Protocol": "1", "Content-Type": "application/x-ndjson", "Cache-Control": "no-store"} {
				if got := resp.Header.Get(k); got != v {
					t.Errorf("%s = %q, want %q", k, got, v)
				}
			}
			id := resp.Header.Get("Skylite-Request-Id")
			resp, m = e.put(t, c.client, db, admin)
			if resp.StatusCode != http.StatusConflict || errorCode(m) != "database_exists" {
				t.Fatalf("second PUT: %s %v", resp.Status, m)
			}
			if got := m["error"].(map[string]any)["request_id"]; got != resp.Header.Get("Skylite-Request-Id") || got == id || got == "" {
				t.Errorf("request_id %v, header %q, first %q: want the header's, new per request", got, resp.Header.Get("Skylite-Request-Id"), id)
			}
		})
	}
}

func TestForbiddenWhetherOrNotExists(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	c := h1Client()
	if resp, m := e.put(t, c, "taken", e.sig.token(t, "admin")); resp.StatusCode != http.StatusOK {
		t.Fatalf("admin PUT: %s %v", resp.Status, m)
	}
	creates := e.store.creates.Load()
	tokens := map[string]string{}
	for _, sub := range []string{"writer", "reader", "scoped", "nobody"} {
		tok := e.sig.token(t, sub)
		tokens[sub] = tok
		var frames []map[string]any
		for _, db := range []string{"taken", "absent"} {
			resp, m := e.put(t, c, db, tok)
			if resp.StatusCode != http.StatusForbidden || errorCode(m) != "forbidden" {
				t.Fatalf("%s PUT %s: %s %v", sub, db, resp.Status, m)
			}
			delete(m["error"].(map[string]any), "request_id")
			frames = append(frames, m)
		}
		if fmt.Sprint(frames[0]) != fmt.Sprint(frames[1]) {
			t.Errorf("%s: answers differ: %v and %v", sub, frames[0], frames[1])
		}
	}
	if n := e.store.creates.Load(); n != creates {
		t.Errorf("refused PUTs sent %d creates", n-creates)
	}
	e.logs.waitFor(t, 1, `"msg":"database created"`, `"issuer":"`+testIssuer+`"`, `"subject":"admin"`, `"database":"taken"`)
	sum := sha256.Sum256([]byte(tokens["writer"]))
	e.logs.waitFor(t, 1, `"msg":"authorisation refused"`, `"code":"forbidden"`, `"issuer":"`+testIssuer+`"`, `"subject":"writer"`,
		`"token_sha256_prefix":"`+hex.EncodeToString(sum[:6])+`"`, `"database":"absent"`, `"role":"admin"`)
}

// failingBody fails the test that reads it.
type failingBody struct{ read *bool }

func (b failingBody) Read([]byte) (int, error) { *b.read = true; return 0, io.ErrUnexpectedEOF }

func TestUnauthenticatedBodyNeverRead(t *testing.T) {
	e := newEnv(t, nil)
	other := newSigner(t, "k1") // same key id, key not in the JWKS
	for _, c := range []struct {
		name, method, path, auth string
		code                     string
	}{
		{"no token", http.MethodPut, "/v1/databases/a", "", "unauthenticated"},
		{"basic scheme", http.MethodPut, "/v1/databases/a", "Basic YTpi", "unauthenticated"},
		{"garbage token", http.MethodPut, "/v1/databases/a", "Bearer x.y.z", "unauthenticated"},
		{"unknown key", http.MethodPut, "/v1/databases/a", "Bearer " + other.token(t, "admin"), "unauthenticated"},
		{"query, no token", http.MethodPost, "/v1/databases/a/query", "", "unauthenticated"},
		{"query, authenticated", http.MethodPost, "/v1/databases/a/query", "Bearer " + e.sig.token(t, "reader"), "not_serving"},
		{"query, no role", http.MethodPost, "/v1/databases/a/query", "Bearer " + e.sig.token(t, "nobody"), "forbidden"},
		{"status, no role on the name", http.MethodGet, "/v1/databases/a", "Bearer " + e.sig.token(t, "scoped"), "forbidden"},
		{"unknown path", http.MethodPost, "/v1/databases/a/other", "", "invalid_request"},
	} {
		t.Run(c.name, func(t *testing.T) {
			read := false
			req := httptest.NewRequest(c.method, c.path, failingBody{&read})
			req.ContentLength = 2
			req.Header.Set("Content-Type", "application/json")
			if c.auth != "" {
				req.Header.Set("Authorization", c.auth)
			}
			rec := httptest.NewRecorder()
			e.s.ServeHTTP(rec, req)
			if read {
				t.Error("the body was read")
			}
			m := frame(t, rec.Body)
			if got := errorCode(m); got != c.code {
				t.Errorf("code %q (%v), want %q", got, m, c.code)
			}
			if c.code == "unauthenticated" && rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("WWW-Authenticate = %q, want Bearer", rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

// TestBodyCutAtEveryOffset cuts a PUT whose Content-Length is 4 after each
// of its first 4 bytes. At offset 2 the bytes sent, {}, are a valid body by
// themselves; net/http already fails the read of a body that ends before
// its Content-Length, and the length check is a second guard.
func TestBodyCutAtEveryOffset(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	tok := e.sig.token(t, "admin")
	const body = "{}{}"
	modes := []string{"h1 close", "h1 half-close", "h2 close", "h2 reset", "h2 end stream"}
	for _, mode := range modes {
		for off := range len(body) {
			db := fmt.Sprintf("cut-%s-%d", strings.ReplaceAll(mode, " ", "-"), off)
			if strings.HasPrefix(mode, "h1") {
				cutH1(t, e.addr, putHead(db, tok)+"Content-Type: application/json\r\nContent-Length: 4\r\n\r\n"+body[:off], mode == "h1 half-close")
			} else {
				cutH2(t, e, db, tok, body[:off], strings.TrimPrefix(mode, "h2 "))
			}
		}
	}
	e.logs.waitFor(t, len(modes)*len(body), `"code":"invalid_request"`)
	if n := e.store.creates.Load(); n != 0 {
		t.Errorf("cut requests sent %d creates", n)
	}
}

func cutH1(t *testing.T, addr, request string, half bool) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	if !half {
		return
	}
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(c)
	if !bytes.HasPrefix(b, []byte("HTTP/1.1 400 ")) || !bytes.Contains(b, []byte(`"code":"invalid_request"`)) {
		t.Errorf("half-closed request answered %q", b)
	}
}

// cutH2 sends a PUT on a prior-knowledge HTTP/2 connection with body bytes
// sent, then ends it: "close" closes the connection, "reset" resets the
// stream and "end stream" ends the stream, which the server answers.
func cutH2(t *testing.T, e *env, db, tok, sent, how string) {
	t.Helper()
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(c, c)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(fr.WriteSettings())
	for { // the server's settings, acknowledged before the request
		f, err := fr.ReadFrame()
		must(err)
		if s, ok := f.(*http2.SettingsFrame); ok && !s.IsAck() {
			must(fr.WriteSettingsAck())
			break
		}
	}
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for _, f := range [][2]string{
		{":method", "PUT"},
		{":scheme", "http"},
		{":authority", "skylite"},
		{":path", "/v1/databases/" + db},
		{"authorization", "Bearer " + tok},
		{"content-type", "application/json"},
		{"content-length", "4"},
	} {
		must(enc.WriteField(hpack.HeaderField{Name: f[0], Value: f[1]}))
	}
	must(fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true}))
	if sent != "" || how == "end stream" {
		must(fr.WriteData(1, how == "end stream", []byte(sent)))
	}
	switch how {
	case "reset":
		must(fr.WriteRSTStream(1, http2.ErrCodeCancel))
	case "end stream":
		for {
			f, err := fr.ReadFrame()
			must(err)
			if h, ok := f.(*http2.MetaHeadersFrame); ok && h.StreamID == 1 {
				if s := h.PseudoValue("status"); s != "400" {
					t.Errorf("stream ended early answered %s", s)
				}
				return
			}
		}
	}
	// Hold the connection until the handler has run, so that a close cuts
	// the body, not the header block.
	time.Sleep(20 * time.Millisecond)
}

func TestContentLengthRules(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	tok := e.sig.token(t, "admin")
	const ct = "Content-Type: application/json\r\n"
	for i, c := range []struct {
		name, headers, body string
		status              int
		code, message       string
	}{
		{"chunked", ct + "Transfer-Encoding: chunked\r\n", "2\r\n{}\r\n0\r\n\r\n", 400, "invalid_request", "Transfer-Encoding"},
		{"no Content-Length", ct, "", 400, "invalid_request", "Content-Length is required"},
		{"above the body limit", ct + "Content-Length: 16777217\r\n", "", 413, "request_too_large", "exceeds"},
		{"no Content-Type", "Content-Length: 2\r\n", "{}", 400, "invalid_request", "Content-Type"},
		{"another Content-Type", "Content-Type: text/plain\r\nContent-Length: 2\r\n", "{}", 400, "invalid_request", "Content-Type"},
		{"empty body", ct + "Content-Length: 0\r\n", "", 400, "invalid_request", ""},
		{"byte after the brace", ct + "Content-Length: 3\r\n", "{} ", 400, "invalid_request", ""},
		{"a field", ct + "Content-Length: 7\r\n", `{"a":1}`, 400, "invalid_request", ""},
		{"Content-Type in another case, with parameters", "Content-Type: Application/JSON ; charset=utf-8\r\nContent-Length: 2\r\n", "{}", 200, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			resp := raw(t, e.addr, putHead("rules-"+strconv.Itoa(i), tok)+c.headers+"\r\n"+c.body)
			defer func() { _ = resp.Body.Close() }()
			m := frame(t, resp.Body)
			if resp.StatusCode != c.status || errorCode(m) != c.code {
				t.Fatalf("%s %v, want %d %q", resp.Status, m, c.status, c.code)
			}
			em, _ := m["error"].(map[string]any)
			if msg, _ := em["message"].(string); !strings.Contains(msg, c.message) {
				t.Errorf("message %q, want it to contain %q", msg, c.message)
			}
		})
	}
}

func TestHeaderLimit431(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	tok := e.sig.token(t, "admin")
	for _, c := range []struct {
		pad    int
		status int
	}{{60 << 10, http.StatusOK}, {70 << 10, http.StatusRequestHeaderFieldsTooLarge}} {
		req := putHead("pad-"+strconv.Itoa(c.pad), tok) + "X-Pad: " + strings.Repeat("a", c.pad) + "\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
		resp := raw(t, e.addr, req)
		_ = resp.Body.Close()
		if resp.StatusCode != c.status {
			t.Errorf("header of %d bytes: %s, want %d", c.pad, resp.Status, c.status)
		}
	}
}

func TestPutRace(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	tok := e.sig.token(t, "admin")
	c := h2cClient()
	for round := range 10 {
		db := "race-" + strconv.Itoa(round)
		got := make([]string, 2)
		var wg sync.WaitGroup
		for i := range got {
			wg.Go(func() {
				resp, m := e.put(t, c, db, tok)
				got[i] = strconv.Itoa(resp.StatusCode) + " " + errorCode(m)
			})
		}
		wg.Wait()
		slices.Sort(got)
		if want := []string{"200 ", "409 database_exists"}; !slices.Equal(got, want) {
			t.Fatalf("round %d: %q, want %q", round, got, want)
		}
	}
}

// unknownStore answers every create with an unknown outcome and writes nothing.
type unknownStore struct{ storage.Store }

func (s unknownStore) Create(_ context.Context, key string, _ io.Reader) (string, error) {
	return "", &storage.Error{Op: "create", Key: key, Err: storage.ErrOutcomeUnknown}
}

func TestPutUnknownOutcome(t *testing.T) {
	e := newEnv(t, func(st storage.Store) storage.Store { return unknownStore{st} })
	e.start(t)
	resp, m := e.put(t, h1Client(), "unknown", e.sig.token(t, "admin"))
	if resp.StatusCode != http.StatusInternalServerError || errorCode(m) != "internal" {
		t.Fatalf("%s %v, want 500 internal", resp.Status, m)
	}
	em := m["error"].(map[string]any)
	if em["not_executed"] != false || em["final"] != false || em["message"] != "the create's outcome is unknown: the database may or may not exist" {
		t.Errorf("error %v, want not_executed false, final false and the unknown-outcome message", em)
	}
	e.logs.waitFor(t, 1, `"msg":"database create outcome unknown"`, `"database":"unknown"`)
}

// failStore fails op ("get" or "create") with an error that is not a
// storage error.
type failStore struct {
	storage.Store
	op string
}

func (s failStore) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if s.op == "get" {
		return nil, storage.Info{}, errors.New("disk on fire")
	}
	return s.Store.Get(ctx, key)
}

func (s failStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if s.op == "create" {
		return "", errors.New("disk on fire")
	}
	return s.Store.Create(ctx, key, body)
}

func TestPutStoreFailure(t *testing.T) {
	for _, op := range []string{"get", "create"} {
		t.Run(op, func(t *testing.T) {
			e := newEnv(t, func(st storage.Store) storage.Store { return failStore{st, op} })
			e.start(t)
			resp, m := e.put(t, h1Client(), "broken", e.sig.token(t, "admin"))
			em, _ := m["error"].(map[string]any)
			if resp.StatusCode != http.StatusInternalServerError || errorCode(m) != "internal" || m["done"] != nil ||
				em["not_executed"] != true || em["final"] != false || em["message"] != "the database could not be created" {
				t.Fatalf("%s %v, want 500 internal, not executed", resp.Status, m)
			}
			e.logs.waitFor(t, 1, `"msg":"database create failed"`, `"database":"broken"`, `disk on fire`)
		})
	}
}

func TestJWKSFileIssuer(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	resp, m := e.put(t, h1Client(), "jwks", e.sig.token(t, "admin"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token signed with the file's key: %s %v", resp.Status, m)
	}
	resp, m = e.put(t, h1Client(), "jwks", newSigner(t, "k2").token(t, "admin"))
	if resp.StatusCode != http.StatusUnauthorized || errorCode(m) != "unauthenticated" {
		t.Fatalf("token signed with another key: %s %v", resp.Status, m)
	}
	tok := newSigner(t, "k2").token(t, "admin")
	e.put(t, h1Client(), "jwks", tok)
	sum := sha256.Sum256([]byte(tok))
	e.logs.waitFor(t, 1, `"msg":"authentication refused"`, `"code":"unknown_key"`, `"issuer":"`+testIssuer+`"`, `"kid":"k2"`,
		`"token_sha256_prefix":"`+hex.EncodeToString(sum[:6])+`"`)
}

func TestNoSecretInLogs(t *testing.T) {
	e := newEnv(t, func(st storage.Store) storage.Store { return unknownStore{st} })
	e.start(t)
	c := h1Client()
	admin, reader := e.sig.token(t, "admin"), e.sig.token(t, "reader")
	forged := admin[:strings.LastIndexByte(admin, '.')+1] + "AAAA"
	var bodies []string
	for _, tok := range []string{admin, reader, forged} {
		_, m := e.put(t, c, "secrets", tok)
		bodies = append(bodies, fmt.Sprint(m))
	}
	e.logs.waitFor(t, 3, "request refused")
	logs := e.logs.String()
	for _, tok := range []string{admin, reader, forged} {
		for part := range strings.SplitSeq(tok, ".") {
			if len(part) > 8 && strings.Contains(logs, part) {
				t.Errorf("the log holds part of a token: %q\n%s", part, logs)
			}
			for _, b := range bodies {
				if len(part) > 8 && strings.Contains(b, part) {
					t.Errorf("a response holds part of a token: %q\n%s", part, b)
				}
			}
		}
	}
}

func TestServeTLSAndShutdown(t *testing.T) {
	dir := t.TempDir()
	cert, pool := selfSigned(t, dir)
	cfg := testConfig(t, "")
	sig := newSigner(t, "k1")
	cfg.Auth.Issuers[0].JWKSFile = jwksFile(t, sig)
	cfg.Plaintext, cfg.TLS = false, cert
	s, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate := newGateStore(s.store)
	s.store = gate
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- s.Serve(ctx, ln) }()

	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}, ForceAttemptHTTP2: true}}
	type answer struct {
		resp *http.Response
		body string
	}
	got := make(chan answer, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPut, "https://"+ln.Addr().String()+"/v1/databases/tls", strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+sig.token(t, "admin"))
		resp, err := c.Do(req)
		if err != nil {
			t.Error(err)
			got <- answer{}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		got <- answer{resp, string(b)}
	}()
	<-gate.entered // the request is running
	cancel()
	select {
	case err := <-served:
		t.Fatalf("Serve returned while a request was running: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gate.release)
	a := <-got
	if a.resp == nil || a.resp.StatusCode != http.StatusOK || a.resp.ProtoMajor != 2 {
		t.Fatalf("PUT over TLS during shutdown: %+v", a)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

// selfSigned writes a certificate for 127.0.0.1 and its key into dir.
func selfSigned(t *testing.T, dir string) (*TLS, *x509.CertPool) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "skylite test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.Public(), k)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	c := &TLS{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem")}
	for path, b := range map[string]*pem.Block{c.CertFile: {Type: "CERTIFICATE", Bytes: der}, c.KeyFile: {Type: "PRIVATE KEY", Bytes: kb}} {
		if err := os.WriteFile(path, pem.EncodeToMemory(b), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return c, pool
}

func TestNewRefusesABadJWKSFile(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(empty, []byte(`{"keys":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"missing": filepath.Join(dir, "absent.json"), "a directory": dir, "no usable key": empty} {
		_, err := New(testConfig(t, path), nil)
		if err == nil || (name == "missing" && !errors.Is(err, os.ErrNotExist)) {
			t.Errorf("New with a JWKS file that is %s: %v", name, err)
		}
	}
}

func TestNewRefusesABadCertificate(t *testing.T) {
	cfg := testConfig(t, jwksFile(t, newSigner(t, "k1")))
	cfg.Plaintext, cfg.TLS = false, &TLS{CertFile: filepath.Join(t.TempDir(), "absent.pem"), KeyFile: "absent.key"}
	if _, err := New(cfg, nil); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("New with a missing certificate: %v", err)
	}
}
