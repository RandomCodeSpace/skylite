package server

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// TestRefusalWithBodyUnsent sends requests that declare a body and never
// send it. Each refusal made before the body is read must come at once, well
// inside the 30-second body deadline, and close an HTTP/1.1 connection, so
// that net/http does not wait for the body to discard it before answering.
// TestBodyDeadline checks that the connection then closes.
func TestRefusalWithBodyUnsent(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	admin, reader, nobody := e.sig.token(t, "admin"), e.sig.token(t, "reader"), e.sig.token(t, "nobody")
	const ct = "Content-Type: application/json\r\n"
	const cl = "Content-Length: 10\r\n"
	req := func(line, token, headers string) string {
		r := line + " HTTP/1.1\r\nHost: skylite\r\n" + headers
		if token != "" {
			r += "Authorization: Bearer " + token + "\r\n"
		}
		return r + "\r\n"
	}
	for _, c := range []struct {
		name, request string
		status        int
	}{
		{"no token", req("PUT /v1/databases/a", "", ct+cl), 401},
		{"no token, chunked", req("PUT /v1/databases/a", "", ct+"Transfer-Encoding: chunked\r\n"), 401},
		{"no token, Expect", req("PUT /v1/databases/a", "", ct+cl+"Expect: 100-continue\r\n"), 401},
		{"read role only", req("PUT /v1/databases/a", reader, ct+cl), 403},
		{"no role, query", req("POST /v1/databases/a/query", nobody, ct+cl), 403},
		{"not serving", req("POST /v1/databases/a/query", admin, ct+cl), 503},
		{"unknown path", req("POST /v1/databases/a/other", admin, ct+cl), 400},
		{"a query string", req("PUT /v1/databases/a?x=1", admin, ct+cl), 400},
		{"wrong Content-Type", req("PUT /v1/databases/a", admin, "Content-Type: text/plain\r\n"+cl), 400},
		{"body too large", req("PUT /v1/databases/a", admin, ct+"Content-Length: 16777217\r\n"), 413},
	} {
		t.Run(c.name, func(t *testing.T) {
			start := time.Now()
			resp := raw(t, e.addr, c.request)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != c.status || !resp.Close {
				t.Fatalf("%s, Connection: close %v, want %d and close", resp.Status, resp.Close, c.status)
			}
			if c.status == 400 {
				if m := frame(t, resp.Body); errorCode(m) != "invalid_request" {
					t.Errorf("frame %v, want invalid_request", m)
				}
			}
			if d := time.Since(start); d > 3*time.Second {
				t.Errorf("answered after %v", d)
			}
		})
	}
	t.Run("HTTP/2", func(t *testing.T) {
		h := dialH2(t, e.addr)
		h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(h2Put("a", "", "content-type", "application/json", "content-length", "10")...), EndHeaders: true}))
		if s := h.status(1, 3*time.Second); s != "401" {
			t.Errorf("status %q, want 401 at once", s)
		}
	})
}

func TestIdleConnectionsClosed(t *testing.T) {
	e := newEnv(t, nil)
	e.s.idleTimeout = 200 * time.Millisecond
	e.start(t)
	t.Run("HTTP/1.1 after a request", func(t *testing.T) {
		resp, c, br := rawConn(t, e.addr, putHead("idle", e.sig.token(t, "admin"))+"Content-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Close {
			t.Fatalf("%s, close %v, want 200 and a kept connection", resp.Status, resp.Close)
		}
		if !closedWithin(c, br, 3*time.Second) {
			t.Error("an idle HTTP/1.1 connection stayed open")
		}
	})
	t.Run("HTTP/2 with settings only", func(t *testing.T) {
		h := dialH2(t, e.addr)
		if !closedWithin(h, h, 3*time.Second) {
			t.Error("an idle HTTP/2 connection stayed open")
		}
	})
	t.Run("HTTP/2 with a header block never ended", func(t *testing.T) {
		h := dialH2(t, e.addr)
		h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(h2Put("idle", "")...), EndHeaders: false}))
		if !closedWithin(h, h, 3*time.Second) {
			t.Error("an HTTP/2 connection with an unfinished header block stayed open")
		}
	})
}

func TestConnectionBound(t *testing.T) {
	e := newEnv(t, nil)
	e.s.maxConns = 1
	e.start(t)
	first, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	second, err := net.Dial("tcp", e.addr) // the kernel accepts it; the server does not, yet
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	if _, err := io.WriteString(second, "GET /x HTTP/1.1\r\nHost: skylite\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	_ = second.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := second.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("a connection beyond the bound was served: %d bytes, %v", n, err)
	}
	_ = first.Close()
	_ = second.SetReadDeadline(time.Now().Add(3 * time.Second))
	b := make([]byte, 12)
	if _, err := io.ReadFull(second, b); err != nil || string(b) != "HTTP/1.1 400" {
		t.Fatalf("after the first closed: %q, %v", b, err)
	}
}

// TestShutdownWithAResponseBlocked holds a response that can never be
// written: the client gives HTTP/2 flow control no window. The response
// deadline must end it, so that shutdown finishes cleanly.
func TestShutdownWithAResponseBlocked(t *testing.T) {
	e := newEnv(t, nil)
	e.s.responseDeadline = 200 * time.Millisecond
	e.s.shutdownTimeout = 5 * time.Second
	stop := e.start(t)
	h := dialH2(t, e.addr, http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0})
	h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(h2Put("blocked", "")...), EndHeaders: true, EndStream: true}))
	if s := h.status(1, 3*time.Second); s != "401" {
		t.Fatalf("status %q, want 401", s)
	}
	if err := stop(); err != nil {
		t.Errorf("Serve: %v", err)
	}
}

// TestShutdownCutsRequestsPastItsWait holds a create past the shutdown
// wait: Serve must close the connection and report it.
func TestShutdownCutsRequestsPastItsWait(t *testing.T) {
	var gate gateStore
	e := newEnv(t, func(st storage.Store) storage.Store { gate = newGateStore(st); return gate })
	e.s.shutdownTimeout = 300 * time.Millisecond
	stop := e.start(t)
	defer close(gate.release)
	got := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPut, "http://"+e.addr+"/v1/databases/held", strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+e.sig.token(t, "admin"))
		resp, err := h1Client().Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		got <- err
	}()
	<-gate.entered
	if err := stop(); err == nil || !strings.Contains(err.Error(), "cut") {
		t.Errorf("Serve = %v, want an error saying requests were cut", err)
	}
	select {
	case err := <-got:
		if err == nil {
			t.Error("the held request was answered")
		}
	case <-time.After(3 * time.Second):
		t.Error("the held request's connection stayed open")
	}
}

func TestHTTP2ContentLength(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	tok := e.sig.token(t, "admin")
	for i, cls := range [][]string{{"2", "4"}, {"2", "2"}} {
		h := dialH2(t, e.addr)
		f := h2Put("twocl-"+string(rune('a'+i)), tok, "content-type", "application/json", "content-length", cls[0], "content-length", cls[1])
		h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(f...), EndHeaders: true}))
		h.must(t, h.fr.WriteData(1, true, []byte("{}")))
		if s := h.status(1, 3*time.Second); s != "400" {
			t.Errorf("content-length %v: status %q, want 400", cls, s)
		}
	}
	resp := raw(t, e.addr, putHead("twocl-h1", tok)+"Content-Type: application/json\r\nContent-Length: 2\r\nContent-Length: 4\r\n\r\n{}")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("HTTP/1.1 with two Content-Lengths: %s, want 400", resp.Status)
	}
}

func TestHeaderDeadline(t *testing.T) {
	e := newEnv(t, nil)
	e.s.headerDeadline = 200 * time.Millisecond
	e.start(t)
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, "PUT /v1/databases/slow HTTP/1.1\r\nHost: skylite\r\n"); err != nil {
		t.Fatal(err)
	}
	if !closedWithin(c, c, 3*time.Second) {
		t.Error("a header block never ended kept its connection open")
	}
}

func TestBodyDeadline(t *testing.T) {
	e := newEnv(t, nil)
	e.s.bodyDeadline = 200 * time.Millisecond
	e.start(t)
	tok := e.sig.token(t, "admin")
	resp := raw(t, e.addr, putHead("slow-1", tok)+"Content-Type: application/json\r\nContent-Length: 2\r\n\r\n")
	defer func() { _ = resp.Body.Close() }()
	if m := frame(t, resp.Body); resp.StatusCode != http.StatusBadRequest || errorCode(m) != "invalid_request" {
		t.Errorf("HTTP/1.1 body never sent: %s %v, want 400 invalid_request", resp.Status, m)
	}
	h := dialH2(t, e.addr)
	h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(h2Put("slow-2", tok, "content-type", "application/json", "content-length", "2")...), EndHeaders: true}))
	if s := h.status(1, 3*time.Second); s != "400" {
		t.Errorf("HTTP/2 body never sent: status %q, want 400", s)
	}
	// The whole body, but a stream that never ends: only the read error refuses it.
	h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, BlockFragment: block(h2Put("slow-4", tok, "content-type", "application/json", "content-length", "2")...), EndHeaders: true}))
	h.must(t, h.fr.WriteData(3, false, []byte("{}")))
	if s := h.status(3, 3*time.Second); s != "400" {
		t.Errorf("HTTP/2 stream never ended: status %q, want 400", s)
	}
	// A refusal before the body leaves it unread; net/http reads it after
	// the answer, within the same deadline, then closes the connection.
	resp3, c, br := rawConn(t, e.addr, "PUT /v1/databases/slow-3 HTTP/1.1\r\nHost: skylite\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n")
	defer func() { _ = resp3.Body.Close() }()
	if resp3.StatusCode != http.StatusUnauthorized || !closedWithin(c, br, 3*time.Second) {
		t.Errorf("unauthenticated, body never sent: %s, and the connection stayed open", resp3.Status)
	}
}

// TestHTTP2ClientThatNeverReads sends unauthenticated requests over HTTP/2
// and never reads, until the server's frame writes block in TCP. The
// connection must still close, freeing its slot, and shutdown stay clean.
func TestHTTP2ClientThatNeverReads(t *testing.T) {
	e := newEnv(t, nil)
	e.s.idleTimeout = 200 * time.Millisecond
	e.s.responseDeadline = 200 * time.Millisecond
	e.s.shutdownTimeout = 2 * time.Second
	e.s.maxConns = 1
	stop := e.start(t)
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) { _ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 1024) })
	}}
	c, err := d.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	fr := http2.NewFramer(c, c)
	if _, err := c.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	_ = fr.WriteSettings()
	_ = fr.WriteSettingsAck()
	_ = fr.WriteWindowUpdate(0, 1<<30)
	answered := func() int { return strings.Count(e.logs.String(), `"msg":"authentication refused"`) }
	pad := strings.Repeat("x", 3000)
	stalls, sent, id := 0, 0, uint32(1)
	for stalls < 2 && sent < 200000 {
		before := answered()
		for range 8 {
			_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: block(h2Put("p", "", "x-pad", pad)...), EndHeaders: true, EndStream: true}); err != nil {
				break // the server closed the connection
			}
			id += 2
			sent++
		}
		for dl := time.Now().Add(500 * time.Millisecond); answered() == before && time.Now().Before(dl); {
			time.Sleep(time.Millisecond)
		}
		if answered() == before {
			stalls++
		} else {
			stalls = 0
		}
	}
	if stalls < 2 {
		t.Fatalf("the server's writes never blocked after %d requests", sent)
	}
	time.Sleep(time.Second)
	// With one connection allowed, a second is served only once the first closed.
	c2, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	_, _ = c2.Write([]byte("GET /x HTTP/1.1\r\nHost: s\r\n\r\n"))
	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, _ := c2.Read(make([]byte, 1)); n == 0 {
		t.Error("the connection that stopped reading still holds the only slot")
	}
	if err := stop(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestHTTP2StreamLimit(t *testing.T) {
	e := newEnv(t, nil)
	e.start(t)
	if got := dialH2(t, e.addr).settings[http2.SettingMaxConcurrentStreams]; got != maxStreams {
		t.Errorf("SETTINGS_MAX_CONCURRENT_STREAMS = %d, want %d", got, maxStreams)
	}
}

// tlsServer serves a TLS configuration until the test ends.
func tlsServer(t *testing.T) (addr string, pool *x509.CertPool) {
	t.Helper()
	cert, pool := selfSigned(t, t.TempDir())
	cfg := testConfig(t, jwksFile(t, newSigner(t, "k1")))
	cfg.Plaintext, cfg.TLS = false, cert
	s, err := New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	e := &env{s: s}
	e.start(t)
	return e.addr, pool
}

func TestTLSVersions(t *testing.T) {
	t.Setenv("GODEBUG", "tls10server=1") // so that only the server's own minimum refuses TLS 1.1
	addr, pool := tlsServer(t)
	for _, c := range []struct {
		max uint16
		ok  bool
	}{{tls.VersionTLS11, false}, {tls.VersionTLS12, true}, {tls.VersionTLS13, true}} {
		conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS10, MaxVersion: c.max}) //nolint:gosec // offering TLS 1.0 is the test
		if err == nil {
			_ = conn.Close()
		}
		if (err == nil) != c.ok {
			t.Errorf("TLS up to %x: %v, want success %v", c.max, err, c.ok)
		}
	}
}

// TestNoCleartextHTTP2OverTLS sends the HTTP/2 preface inside a TLS
// connection that negotiated HTTP/1.1: it must not be served as HTTP/2.
func TestNoCleartextHTTP2OverTLS(t *testing.T) {
	addr, pool := tlsServer(t)
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, NextProtos: []string{"http/1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(conn)
	if !strings.HasPrefix(string(b), "HTTP/1.1 ") {
		t.Errorf("answer %q, want an HTTP/1.1 refusal", b)
	}
}

// TestCreateOutlivesItsClient closes the client's connection while the
// create runs: the create must still complete.
func TestCreateOutlivesItsClient(t *testing.T) {
	var gate gateStore
	e := newEnv(t, func(st storage.Store) storage.Store { gate = newGateStore(st); return gate })
	e.start(t)
	tok := e.sig.token(t, "admin")
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, putHead("detached", tok)+"Content-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
		t.Fatal(err)
	}
	<-gate.entered
	_ = c.Close()
	time.Sleep(100 * time.Millisecond) // net/http cancels the request's context
	close(gate.release)
	e.logs.waitFor(t, 1, `"msg":"database created"`, `"database":"detached"`)
}

// closingStore records Close.
type closingStore struct {
	storage.Store
	closed atomic.Bool
}

func (s *closingStore) Close() error { s.closed.Store(true); return nil }

func TestServeClosesTheStore(t *testing.T) {
	var cs *closingStore
	e := newEnv(t, func(st storage.Store) storage.Store { cs = &closingStore{Store: st}; return cs })
	if err := e.start(t)(); err != nil {
		t.Fatal(err)
	}
	if !cs.closed.Load() {
		t.Error("Serve returned without closing the store")
	}
}
