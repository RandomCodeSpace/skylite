package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/RandomCodeSpace/skylite/internal/auth"
)

// hold sends the header block of a POST to path on "d" as subject, with a
// two-byte body it never sends, so that the request holds its places in
// the bounds until the connection closes.
func (e *env) hold(t *testing.T, path, subject string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if _, err := io.WriteString(c, "POST /v1/databases/d/"+path+" HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer "+e.sig.token(t, subject)+
		"\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	return c
}

// admitted waits until b counts n requests, and n of subject's if subject
// is set.
func (e *env) admitted(t *testing.T, b *bound, subject string, n int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		e.s.admMu.Lock()
		got := b.n
		if subject != "" {
			got = b.by[testIssuer+"\x00"+subject]
		}
		e.s.admMu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests admitted, want %d", got, n)
		}
	}
}

func (a answer) overloaded(t *testing.T, what string) {
	t.Helper()
	if a.status != http.StatusTooManyRequests || a.code() != "overloaded" || a.last()["error"].(map[string]any)["not_executed"] != true {
		t.Errorf("%s: %d %v, want 429 overloaded, not executed", what, a.status, a.frames)
	}
}

// TestStatelessBounds: a principal at its bound on stateless requests is
// refused with overloaded while another principal is served; the pod's
// bound refuses everyone; both recover once the requests end.
func TestStatelessBounds(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.stateless.pod, e.s.stateless.principal = 3, 2 })
	held := []net.Conn{e.hold(t, "query", "reader"), e.hold(t, "describe", "reader")}
	e.admitted(t, &e.s.stateless, "reader", 2)
	e.post(t, "query", "reader", q("SELECT 1")).overloaded(t, "the principal at its bound")
	if a := e.post(t, "query", "admin", q("SELECT 1")); a.status != http.StatusOK {
		t.Fatalf("another principal: %d %v", a.status, a.frames)
	}
	held = append(held, e.hold(t, "query", "writer"))
	e.admitted(t, &e.s.stateless, "", 3)
	e.post(t, "query", "admin", q("SELECT 1")).overloaded(t, "the pod at its bound")
	e.admitted(t, &e.s.requests, "", 3) // stateless requests count against the request bound too
	for _, c := range held {
		_ = c.Close()
	}
	e.admitted(t, &e.s.stateless, "", 0)
	e.admitted(t, &e.s.requests, "", 0)
	if a := e.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusOK {
		t.Fatalf("after release: %d %v", a.status, a.frames)
	}
	e.s.admMu.Lock()
	defer e.s.admMu.Unlock()
	if len(e.s.stateless.by) != 0 || len(e.s.requests.by) != 0 {
		t.Errorf("principals still counted: %v, %v", e.s.stateless.by, e.s.requests.by)
	}
}

// TestRequestBounds: the request bound counts every request but cancel,
// close and resolve, per principal and per pod.
func TestRequestBounds(t *testing.T) {
	e := newQueryEnv(t, func(e *env) { e.s.requests.pod, e.s.requests.principal = 2, 1 })
	held := e.hold(t, "channels", "writer") // an open
	e.admitted(t, &e.s.requests, "writer", 1)
	e.post(t, "query", "writer", q("SELECT 1")).overloaded(t, "a query at the principal's bound")
	e.post(t, "channels", "writer", `{"mode":"write"}`).overloaded(t, "an open at the principal's bound")
	second := e.hold(t, "query", "reader")
	e.admitted(t, &e.s.requests, "", 2)
	e.post(t, "channels", "admin", `{"mode":"write"}`).overloaded(t, "an open at the pod's bound")
	_, _ = held.Close(), second.Close()
	e.admitted(t, &e.s.requests, "", 0)
	e.open(t, "writer")
}

// TestControlCapacity: cancel and close have their own capacity, which
// neither a full request bound nor a full body share refuses, and resolve
// has its own apart from theirs.
func TestControlCapacity(t *testing.T) {
	e := newQueryEnv(t, func(e *env) {
		e.s.requests.pod = 1
		e.s.controls.pod, e.s.controls.principal = 2, 1
		e.s.resolves.pod = 1
	})
	tok := e.open(t, "writer")
	e.hold(t, "query", "reader")
	e.admitted(t, &e.s.requests, "", 1)
	e.s.bodyMu.Lock()
	e.s.bodyShare = 0 // no other body fits
	e.s.bodyMu.Unlock()
	e.post(t, "query", "admin", q("SELECT 1")).overloaded(t, "a query past the request bound")
	held := e.hold(t, "channel/close", "writer")
	e.admitted(t, &e.s.controls, "writer", 1)
	e.post(t, "channel/cancel", "writer", `{"channel":"`+tok+`","seq":1}`).overloaded(t, "a cancel at the principal's control bound")
	if a := e.post(t, "channel/close", "admin", `{"channel":"`+tok+`"}`); a.code() == "overloaded" {
		t.Errorf("another principal's close: %v", a.frames)
	}
	// resolve is not served on a leader yet, but is admitted to its own capacity.
	if a := e.post(t, "resolve", "writer", `{"channel":"`+tok+`","seq":1}`); a.code() != "not_serving" {
		t.Errorf("resolve with cancel and close at their bound: %v", a.frames)
	}
	_ = held.Close()
	e.admitted(t, &e.s.controls, "", 0)
	if a := e.post(t, "channel/close", "writer", `{"channel":"`+tok+`"}`); a.status != http.StatusOK {
		t.Errorf("a close with the body share full: %d %v", a.status, a.frames)
	}
	e.s.admMu.Lock()
	e.s.resolves.pod = 0
	e.s.admMu.Unlock()
	e.post(t, "resolve", "writer", `{"channel":"`+tok+`","seq":1}`).overloaded(t, "a resolve past its bound")
}

// TestPreAuthIdleLimit: an HTTP/2 connection that has carried no request
// with a verified token is closed after the header deadline while idle,
// not the idle timeout; one that has stays open.
func TestPreAuthIdleLimit(t *testing.T) {
	e := newEnv(t, nil)
	e.s.headerDeadline = 300 * time.Millisecond
	e.start(t)
	t.Run("no request", func(t *testing.T) {
		h := dialH2(t, e.addr)
		if !closedWithin(h, h, 5*time.Second) {
			t.Error("an idle connection with no request stayed open")
		}
	})
	t.Run("refused authentication", func(t *testing.T) {
		h := dialH2(t, e.addr)
		h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(h2Put("x", "")...), EndHeaders: true, EndStream: true}))
		if s := h.status(1, 3*time.Second); s != "401" {
			t.Fatalf("status %q, want 401", s)
		}
		if !closedWithin(h, h, 5*time.Second) {
			t.Error("a connection idle after a refused token stayed open")
		}
	})
	t.Run("verified token", func(t *testing.T) {
		h := dialH2(t, e.addr)
		h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: block(h2Put("y", e.sig.token(t, "admin"), "content-type", "application/json", "content-length", "2")...), EndHeaders: true}))
		h.must(t, h.fr.WriteData(1, true, []byte("{}")))
		if s := h.status(1, 3*time.Second); s != "200" {
			t.Fatalf("status %q, want 200", s)
		}
		if closedWithin(h, h, 4*e.s.headerDeadline) {
			t.Error("an authenticated connection was closed while idle")
		}
	})
	h1 := func(db, token string) (net.Conn, io.Reader, int) {
		resp, c, br := rawConn(t, e.addr, putHead(db, token)+"Content-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		return c, br, resp.StatusCode
	}
	// On HTTP/1.1 a refusal before the body is read closes the connection
	// itself (Connection: close), so the limit matters there only for a
	// connection that has carried a verified token: it stays open.
	t.Run("HTTP1 refused authentication", func(t *testing.T) {
		c, br, status := h1("x", "")
		if status != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401", status)
		}
		if !closedWithin(c, br, 5*time.Second) {
			t.Error("an HTTP/1.1 connection idle after a refused token stayed open")
		}
	})
	t.Run("HTTP1 verified token", func(t *testing.T) {
		c, br, status := h1("z", e.sig.token(t, "admin"))
		if status != http.StatusOK {
			t.Fatalf("status %d, want 200", status)
		}
		if closedWithin(c, br, 4*e.s.headerDeadline) {
			t.Error("an authenticated HTTP/1.1 connection was closed while idle")
		}
	})
}

// TestForwardedRequestsCounted: a forwarded request counts against the
// forwarding pod's bounds from its header block, before its hop, and
// against the owner's as any request there.
func TestForwardedRequestsCounted(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	hops := countHops(t, leader)
	pod := newPod(t, leader, p, nil, false, nil)
	pod.s.admMu.Lock()
	pod.s.stateless.principal = 1
	pod.s.admMu.Unlock()
	held := pod.hold(t, "query", "reader")
	pod.admitted(t, &pod.s.stateless, "reader", 1)
	pod.post(t, "query", "reader", q("SELECT 1")).overloaded(t, "on the forwarding pod")
	if n := hops.Load(); n != 0 {
		t.Errorf("%d requests forwarded, want none", n)
	}
	_ = held.Close()
	pod.admitted(t, &pod.s.stateless, "", 0)
	leader.s.admMu.Lock()
	leader.s.stateless.principal = 0
	leader.s.admMu.Unlock()
	pod.post(t, "query", "reader", q("SELECT 1")).overloaded(t, "on the owner, relayed")
	if n := hops.Load(); n != 1 {
		t.Errorf("%d requests forwarded, want 1", n)
	}
	leader.admitted(t, &leader.s.requests, "", 0)
	pod.admitted(t, &pod.s.requests, "", 0)
}

// TestRefreshWaitHoldsNoPlace: requests waiting for another request's
// refresh of an HTTP issuer's signing keys, which stalls, hold no place in
// the pod's bounds, so a request with a known key is admitted meanwhile;
// the waiters have a bound of their own, leave when their client goes
// away, and are served once the refresh ends.
func TestRefreshWaitHoldsNoPlace(t *testing.T) {
	k1, k2 := newSigner(t, "k1"), newSigner(t, "k2")
	var mu sync.Mutex
	jwks := jwksFile(t, k1)
	var stalling atomic.Bool
	stalled, release := make(chan struct{}, 1), make(chan struct{})
	var iss *httptest.Server
	iss = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jwks" {
			if stalling.Load() {
				stalled <- struct{}{}
				<-release
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": iss.URL, "jwks_uri": iss.URL + "/jwks"})
			return
		}
		mu.Lock()
		defer mu.Unlock()
		b, _ := os.ReadFile(jwks) //nolint:gosec // the test's own file
		_, _ = w.Write(b)
	}))
	t.Cleanup(iss.Close)
	unstall := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unstall) // before iss.Close, which waits for the stalled handler
	var skew atomic.Int64
	g, err := auth.New(auth.Config{
		Issuers: []auth.Issuer{{Issuer: iss.URL, Audience: testAudience}},
		Rules:   []auth.Rule{{Issuer: iss.URL, Subject: "reader", Database: "*", Role: auth.RoleRead}},
	}, iss.Client(), func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := newQueryEnv(t, func(e *env) {
		e.s.guard = g
		e.s.requests.pod, e.s.refreshes.pod = 2, 2
	})
	t.Cleanup(unstall) // and before the server stops
	send := func(k signer) <-chan answer {
		got := make(chan answer, 1)
		tok := k.tokenFrom(t, iss.URL, "reader")
		go func() {
			a, err := e.send("query", tok, q("SELECT 1"))
			if err != nil {
				t.Error(err)
			}
			got <- a
		}()
		return got
	}
	if a := <-send(k1); a.status != http.StatusOK {
		t.Fatalf("first request: %d %v", a.status, a.frames)
	}
	skew.Store(int64(2 * time.Minute)) // past the least interval between refreshes
	stalling.Store(true)
	fetcher := send(k2) // refreshes, and holds its place while it does
	<-stalled
	waiter := send(k2)
	h := dialH2(t, e.addr) // a waiter whose client goes away
	h.must(t, h.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, BlockFragment: block(h2Put("w", k2.tokenFrom(t, iss.URL, "reader"), "content-type", "application/json", "content-length", "2")...)}))
	e.admitted(t, &e.s.refreshes, "", 2)
	e.admitted(t, &e.s.requests, "", 1)
	(<-send(k2)).overloaded(t, "a third waiter")
	if a := <-send(k1); a.status != http.StatusOK {
		t.Fatalf("a known key while the refresh stalls: %d %v", a.status, a.frames)
	}
	_ = h.Close()
	e.admitted(t, &e.s.refreshes, "", 1)
	mu.Lock()
	jwks = jwksFile(t, k1, k2)
	mu.Unlock()
	unstall()
	for what, got := range map[string]<-chan answer{"the refreshing request": fetcher, "the waiting request": waiter} {
		if a := <-got; a.status != http.StatusOK {
			t.Errorf("%s: %d %v", what, a.status, a.frames)
		}
	}
	e.admitted(t, &e.s.refreshes, "", 0)
	e.admitted(t, &e.s.requests, "", 0)
}
