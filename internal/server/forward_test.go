package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/auth"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// peerConfig writes a self-signed peer certificate for skylite-peer, usable
// as server and client certificate and as its own CA.
func peerConfig(t *testing.T) *Peer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "skylite peer"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"skylite-peer"}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.Public(), k)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	p := &Peer{Listen: "127.0.0.1:0", CertFile: filepath.Join(dir, "peer.pem"), KeyFile: filepath.Join(dir, "peer.key"), ServerName: "skylite-peer"}
	p.CAFile = p.CertFile
	for path, b := range map[string]*pem.Block{p.CertFile: {Type: "CERTIFICATE", Bytes: der}, p.KeyFile: {Type: "PRIVATE KEY", Bytes: kb}} {
		if err := os.WriteFile(path, pem.EncodeToMemory(b), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

var peerEnvs atomic.Int64

// addPeer gives e, before it starts, a peer listener with p's certificate.
func addPeer(t *testing.T, e *env, p *Peer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	c := *p
	c.AddressEnv = fmt.Sprint("SKYLITE_TEST_PEER_", peerEnvs.Add(1))
	t.Setenv(c.AddressEnv, ln.Addr().String())
	e.s.cfg.Peer = &c
	if e.s.peer, err = newPeer(&c); err != nil {
		t.Fatal(err)
	}
	e.s.peerLn = ln
}

// newPeerLeader returns a started server with a peer listener that leads
// "d" on a holdStore, once its leader record is published.
func newPeerLeader(t *testing.T, p *Peer) (*env, *holdStore) {
	t.Helper()
	e, hs := newChanEnv(t, func(e *env) { addPeer(t, e, p) })
	waitRecord(t, e, func(r leaderRecord) bool { return r.Instance == e.s.instance && r.Address != "" })
	return e, hs
}

// newPod returns another started server of "d" on leader's store, or on st
// if it is set, with a peer listener; edit, if set, changes its
// configuration first.
func newPod(t *testing.T, leader *env, p *Peer, st storage.Store, followerReads bool, edit func(*Config)) *env {
	t.Helper()
	cfg := leader.s.cfg
	cfg.FollowerReads, cfg.Peer = followerReads, nil
	if edit != nil {
		edit(&cfg)
	}
	logs := &lockedBuffer{}
	if st == nil {
		st = leader.store
	}
	s, err := newServer(cfg, slog.New(slog.NewJSONHandler(logs, nil)), st)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{s: s, sig: leader.sig, logs: logs, store: leader.store}
	addPeer(t, e, p)
	if err := s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	e.halt = e.start(t)
	return e
}

// record reads "d"'s leader record.
func (e *env) record(t *testing.T) (leaderRecord, error) {
	t.Helper()
	keys, err := e.s.cfg.Storage.keys("d")
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := e.s.readRecord(context.Background(), keys.Leader())
	return r, err
}

func waitRecord(t *testing.T, e *env, cond func(leaderRecord) bool) leaderRecord {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if r, err := e.record(t); err == nil && cond(r) {
			return r
		}
		if time.Now().After(deadline) {
			r, err := e.record(t)
			t.Fatalf("leader record %+v, %v", r, err)
		}
	}
}

// writeRecord replaces "d"'s leader record with r.
func (e *env) writeRecord(t *testing.T, r leaderRecord) {
	t.Helper()
	keys, _ := e.s.cfg.Storage.keys("d")
	b, _ := json.Marshal(r)
	if err := e.store.Delete(context.Background(), keys.Leader()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Create(context.Background(), keys.Leader(), bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
}

// call sends a request to url and returns its answer and headers; a body
// cut short is returned as err with the frames read so far.
func call(c *http.Client, method, url, token, body string, hdr ...string) (answer, http.Header, error) {
	var rd io.Reader
	if method != http.MethodGet {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return answer{}, nil, err
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := c.Do(req)
	if err != nil {
		return answer{}, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, rerr := io.ReadAll(resp.Body)
	var frames []map[string]any
	for line := range bytes.Lines(b) {
		if f, err := parseFrames(line); err == nil {
			frames = append(frames, f...)
		}
	}
	return answer{resp.StatusCode, frames}, resp.Header, rerr
}

// peerURL is the URL of path on e's peer listener.
func (e *env) peerURL(path string) string {
	return "https://" + e.s.peerLn.Addr().String() + "/v1/databases/d" + path
}

func peerClient(e *env) *http.Client { return &http.Client{Transport: e.s.peer.transport} }

// Every request kind a pod without office gets goes to the leader in
// office, which answers it, and a PUT stays where it arrived.
func TestForwardEachKind(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	pod := newPod(t, leader, p, nil, false, nil)
	seen := func() int64 { return leader.s.peerSeen.Load() }

	if a := pod.post(t, "query", "reader", q("SELECT 7")); a.done() == nil || a.rows()[0] != "[7]" {
		t.Fatalf("query: %d %v", a.status, a.frames)
	}
	if a := pod.post(t, "describe", "reader", q("SELECT a FROM t")); a.done() == nil {
		t.Fatalf("describe: %d %v", a.status, a.frames)
	}
	tok := pod.open(t, "writer")
	if !strings.HasPrefix(tok, channelTokenPrefix+"."+leader.s.instance+".") {
		t.Fatalf("token %q is not the leader's", tok)
	}
	if a := pod.post(t, "channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)")); a.done() == nil {
		t.Fatalf("execute: %d %v", a.status, a.frames)
	}
	if a := pod.post(t, "channel/cancel", "writer", cancelBody(tok, 1)); a.done() == nil {
		t.Fatalf("cancel: %d %v", a.status, a.frames)
	}
	if a := pod.post(t, "channel/close", "writer", closeBody(tok)); a.done() == nil {
		t.Fatalf("close: %d %v", a.status, a.frames)
	}
	if st, ok := leader.chState(tok); ok && !st.closing {
		t.Error("the leader still holds the closed channel")
	}
	if a := pod.post(t, "resolve", "writer", cancelBody(tok, 1)); a.code() != "not_serving" || !strings.Contains(a.err()["message"].(string), "not served in this version") {
		t.Errorf("resolve: %d %v", a.status, a.frames)
	}
	a, h, err := call(h1Client(), http.MethodGet, "http://"+pod.addr+"/v1/databases/d", pod.sig.token(t, "reader"), "")
	if err != nil || a.code() != "not_serving" || h.Get("Skylite-Protocol") != "1" {
		t.Errorf("status: %d %v %v", a.status, a.frames, err)
	}
	if got := seen(); got != 8 {
		t.Errorf("the leader's peer listener got %d requests, want 8", got)
	}
	if r, _ := pod.put(t, h1Client(), "d", pod.sig.token(t, "admin")); r.StatusCode != http.StatusConflict {
		t.Errorf("PUT: %d", r.StatusCode)
	}
	if got := seen(); got != 8 {
		t.Errorf("a PUT was forwarded: %d", got)
	}
	if a := leader.post(t, "query", "reader", q("SELECT count(*) FROM t")); a.rows()[0] != "[1]" {
		t.Errorf("rows: %v", a.frames)
	}
}

// A fresh follower serves a read itself, and hands the leader one it is
// behind for.
func TestForwardFollowerFallback(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	rs := &replicaStore{Store: leader.store}
	pod := newPod(t, leader, p, rs, true, nil)
	waitReplica(t, pod.s.ready, 5*time.Second, "follower not ready")
	if a := pod.post(t, "query", "reader", q("SELECT 1")); a.done() == nil || leader.s.peerSeen.Load() != 0 {
		t.Fatalf("local follower read: %d %v, forwarded %d", a.status, a.frames, leader.s.peerSeen.Load())
	}
	a := pod.post(t, "query", "reader", `{"sql":"SELECT 1","min_txn":1000000}`)
	if a.code() != "behind" || leader.s.peerSeen.Load() != 1 || !strings.Contains(a.err()["message"].(string), "leader's head") && !strings.Contains(a.err()["message"].(string), "min_txn") {
		t.Errorf("behind read: %d %v, forwarded %d", a.status, a.frames, leader.s.peerSeen.Load())
	}
	pauseReplica(t, rs)
	waitReplica(t, func() bool { return !pod.s.dbs["d"].loop.State().Follower.Fresh() }, 5*time.Second, "follower stayed fresh")
	if a := pod.post(t, "query", "reader", q("SELECT 2")); a.done() == nil || leader.s.peerSeen.Load() != 2 {
		t.Errorf("stale follower read: %d %v, forwarded %d", a.status, a.frames, leader.s.peerSeen.Load())
	}
}

// The hop carries the body, the bearer token, the request id and the
// budget, drops hop-by-hop headers, and relays the answer as it came.
func TestForwardHopHeaders(t *testing.T) {
	p := peerConfig(t)
	pod, up := fakeOwner(t, p, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Skylite-Protocol", "1")
		w.Header().Set("Skylite-Request-Id", r.Header.Get("Skylite-Request-Id"))
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush() // no Content-Length: the relay ends the body
		_, _ = io.WriteString(w, `{"done":{"txn":5}}`+"\n")
	})
	body := `{"sql":"SELECT 1","timeout_ms":1000}`
	tok := pod.sig.token(t, "reader")
	a, h, err := call(h1Client(), http.MethodPost, "http://"+pod.addr+"/v1/databases/d/query", tok, body, "Connection", "X-Hop", "X-Hop", "1", "X-End", "2")
	if err != nil || a.done() == nil || num(a.done(), "txn") != 5 {
		t.Fatalf("relayed: %d %v %v", a.status, a.frames, err)
	}
	got := up.last()
	if got.body != body || got.h.Get("Authorization") != "Bearer "+tok || got.h.Get("Skylite-Forwarded") != "1" ||
		got.h.Get("X-Hop") != "" || got.h.Get("X-End") != "2" {
		t.Errorf("upstream got %q %v", got.body, got.h)
	}
	if id := got.h.Get("Skylite-Request-Id"); id == "" || h.Get("Skylite-Request-Id") != id || len(h.Values("Skylite-Protocol")) != 1 {
		t.Errorf("request id %q, answer headers %v", id, h)
	}
	var ms int64
	if _, err := fmt.Sscan(got.h.Get("Skylite-Budget-Ms"), &ms); err != nil || ms < 1 || ms > 1000 {
		t.Errorf("budget %q", got.h.Get("Skylite-Budget-Ms"))
	}
	if up.n.Load() != 1 {
		t.Errorf("%d deliveries", up.n.Load())
	}
}

// An answer cut after it started is cut for the client too, with no
// terminal frame added.
func TestForwardPartialAnswer(t *testing.T) {
	p := peerConfig(t)
	pod, _ := fakeOwner(t, p, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Skylite-Protocol", "1")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"columns":[{"name":"x","decltype":null}]}`+"\n")
		_ = http.NewResponseController(w).Flush()
		panic(http.ErrAbortHandler)
	})
	a, _, err := call(h1Client(), http.MethodPost, "http://"+pod.addr+"/v1/databases/d/query", pod.sig.token(t, "reader"), q("SELECT 1"))
	if err == nil || len(a.frames) != 1 || a.frames[0]["columns"] == nil {
		t.Errorf("cut answer: %d %v, %v", a.status, a.frames, err)
	}
}

// A hop that fails once the request may have arrived leaves it unsettled,
// and is never sent again; one that failed before is not executed.
func TestForwardHopFailure(t *testing.T) {
	p := peerConfig(t)
	var upstreamAnswer atomic.Bool
	pod, up := fakeOwner(t, p, func(w http.ResponseWriter, r *http.Request) {
		if upstreamAnswer.Load() {
			w.WriteHeader(http.StatusOK) // no Skylite-Protocol marker
			_, _ = io.WriteString(w, `{"done":{}}`+"\n")
			return
		}
		panic(http.ErrAbortHandler)
	})
	rec, _ := pod.record(t)
	tok := channelTokenPrefix + "." + rec.Instance + "." + rec.DB + ".10." + strings.Repeat("A", 43)
	check := func(path, subject, body, code string, notExecuted bool) {
		t.Helper()
		before := up.n.Load()
		a := pod.post(t, path, subject, body)
		if a.status != http.StatusServiceUnavailable || a.code() != code || a.err()["not_executed"] != notExecuted || a.err()["final"] != false {
			t.Errorf("%s: %d %v", path, a.status, a.frames)
		}
		time.Sleep(50 * time.Millisecond)
		if n := up.n.Load() - before; n != 1 && !notExecuted || n != 0 && notExecuted {
			t.Errorf("%s: %d deliveries", path, n)
		}
	}
	check("channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)"), "owner_unreachable", false)
	check("channels", "writer", `{"mode":"write"}`, "no_leader", false)
	upstreamAnswer.Store(true)
	check("channel/close", "writer", closeBody(tok), "owner_unreachable", false)

	// Nothing listens at the recorded address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec.Address = ln.Addr().String()
	_ = ln.Close()
	pod.writeRecord(t, rec)
	check("channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)"), "owner_unreachable", true)
	check("query", "reader", q("SELECT 1"), "no_leader", true)

	// No leader is recorded.
	rec.Address = ""
	pod.writeRecord(t, rec)
	check("channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)"), "owner_unreachable", true)
	check("channels", "writer", `{"mode":"write"}`, "no_leader", true)
}

// channel_lost needs proof: a later office than the channel's, or this
// process's own channel after it left office.
func TestForwardChannelLost(t *testing.T) {
	p := peerConfig(t)
	pod, up := fakeOwner(t, p, func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) })
	rec, _ := pod.record(t)
	rec.Head = 100
	pod.writeRecord(t, rec)
	token := func(instance, db string, head int) string {
		return fmt.Sprintf("%s.%s.%s.%d.%s", channelTokenPrefix, instance, db, head, strings.Repeat("A", 43))
	}
	for _, tc := range []struct {
		name, tok, code string
	}{
		{"older office", token("OTHER", rec.DB, 99), "channel_lost"},
		{"own process", token(pod.s.instance, rec.DB, 500), "channel_lost"},
		{"newer than the record", token("OTHER", rec.DB, 100), "owner_unreachable"},
		{"another database", token("OTHER", strings.Repeat("0", 32), 99), "owner_unreachable"},
		{"not a token", "w1.x", "invalid_request"},
		{"another token version", "w9.OTHER." + rec.DB + ".99.AAAA", "invalid_request"},
	} {
		if a := pod.post(t, "channel", "writer", ex(tc.tok, 1, "SELECT 1")); a.code() != tc.code || a.err()["not_executed"] != true {
			t.Errorf("%s: %d %v", tc.name, a.status, a.frames)
		}
	}
	if up.n.Load() != 0 {
		t.Errorf("%d deliveries", up.n.Load())
	}
}

// delivery is one request a fake owner received.
type delivery struct {
	h    http.Header
	body string
}

type upstream struct {
	n   atomic.Int64
	mu  sync.Mutex
	got []delivery
}

func (u *upstream) last() delivery {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.got[len(u.got)-1]
}

// fakeOwner starts a pod of "d" without office, a database that does not
// exist, and a peer server that answers with h, and records it as the
// leader.
func fakeOwner(t *testing.T, p *Peer, h http.HandlerFunc) (*env, *upstream) {
	t.Helper()
	e := newEnv(t, nil)
	e.s.cfg.Databases = []string{"d"}
	addPeer(t, e, p)
	if err := e.s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	e.halt = e.start(t)
	up := &upstream{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		up.mu.Lock()
		up.got = append(up.got, delivery{r.Header.Clone(), string(b)})
		up.mu.Unlock()
		up.n.Add(1)
		h(w, r)
	}))
	srv.TLS = e.s.peer.server.Clone()
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	e.writeRecord(t, leaderRecord{Instance: "FAKE", Address: srv.Listener.Addr().String(), DB: strings.Repeat("ab", 16), Epoch: 2, Head: 1})
	return e, up
}

// The pod-to-pod headers are refused from clients, and the peer listener
// takes only pods with a peer certificate.
func TestForwardRefusesForgedHop(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	tok := leader.sig.token(t, "writer")
	for _, hdr := range [][]string{{"Skylite-Forwarded", "1"}, {"Skylite-Budget-Ms", "5"}} {
		a, _, err := call(h1Client(), http.MethodPost, "http://"+leader.addr+"/v1/databases/d/channels", tok, `{"mode":"write"}`, hdr...)
		if err != nil || a.status != http.StatusBadRequest || a.code() != "invalid_request" {
			t.Errorf("%v: %d %v %v", hdr, a.status, a.frames, err)
		}
	}
	if leader.s.nChans != 0 {
		t.Error("a forged hop opened a channel")
	}
	if _, h, err := call(h1Client(), http.MethodPost, "http://"+leader.addr+"/v1/databases/d/query", tok, q("SELECT 1"), "Skylite-Request-Id", "CLIENTID"); err != nil || h.Get("Skylite-Request-Id") == "CLIENTID" {
		t.Errorf("a client chose its request id: %v %v", h, err)
	}
	// No client certificate: the TLS handshake fails.
	tr := leader.s.peer.transport.Clone()
	tr.TLSClientConfig.Certificates = nil
	if _, _, err := call(&http.Client{Transport: tr}, http.MethodPost, leader.peerURL("/channels"), tok, `{"mode":"write"}`, "Skylite-Forwarded", "1", "Skylite-Budget-Ms", "1000"); err == nil {
		t.Error("the peer listener served a client without a peer certificate")
	}
	for _, budget := range []string{"", "0", "x"} {
		if a, _, err := call(peerClient(leader), http.MethodPost, leader.peerURL("/channels"), tok, `{"mode":"write"}`, "Skylite-Forwarded", "1", "Skylite-Budget-Ms", budget); err != nil || a.code() != "invalid_request" {
			t.Errorf("hop with budget %q: %d %v %v", budget, a.status, a.frames, err)
		}
	}
}

// A forwarded request that reaches a pod that cannot serve it is refused,
// not forwarded again.
func TestForwardOneHop(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	pod := newPod(t, leader, p, nil, false, nil)
	tok := leader.open(t, "writer")
	for _, tc := range []struct{ path, body string }{{"/channels", `{"mode":"write"}`}, {"/channel", ex(tok, 1, "SELECT 1")}, {"/query", q("SELECT 1")}} {
		a, _, err := call(peerClient(pod), http.MethodPost, pod.peerURL(tc.path), pod.sig.token(t, "writer"), tc.body, "Skylite-Forwarded", "1", "Skylite-Budget-Ms", "1000")
		if err != nil || a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
			t.Errorf("%s: %d %v %v", tc.path, a.status, a.frames, err)
		}
	}
	a, _, err := call(peerClient(pod), http.MethodPost, pod.peerURL("/query"), pod.sig.token(t, "reader"), q("SELECT 1"), "Skylite-Forwarded", "1", "Skylite-Budget-Ms", "1000", "Skylite-Request-Id", "bad/id")
	if id, _ := a.err()["request_id"].(string); err != nil || a.code() != "not_serving" || id == "bad/id" || id == "" {
		t.Errorf("forwarded request id: %v %v", a.frames, err)
	}
	if a, _, err := call(h1Client(), http.MethodPost, "http://"+pod.addr+"/v1/databases/other/query", pod.sig.token(t, "reader"), q("SELECT 1")); err != nil || a.code() != "not_serving" {
		t.Errorf("unconfigured database: %d %v %v", a.status, a.frames, err)
	}
	if a := pod.post(t, "resolve", "writer", fmt.Sprintf(`{"channel":%q,"seq":1}`, strings.Repeat("A", controlBodyBytes))); a.code() != "request_too_large" {
		t.Errorf("large resolve: %d %v", a.status, a.frames)
	}
	if n := leader.s.peerSeen.Load(); n != 0 {
		t.Errorf("forwarded again: %d", n)
	}
}

// The pod that serves a forwarded request checks the bearer token and the
// role itself, and applies the budget.
func TestForwardFinalPodChecks(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	pod := newPod(t, leader, p, nil, false, func(c *Config) {
		c.Auth.Rules = append(append([]auth.Rule(nil), c.Auth.Rules...), auth.Rule{Issuer: testIssuer, Subject: "intruder", Database: "*", Role: auth.RoleWrite})
	})
	if a := pod.post(t, "channels", "intruder", `{"mode":"write"}`); a.status != http.StatusForbidden || a.code() != "forbidden" || leader.s.peerSeen.Load() != 1 {
		t.Errorf("role granted only by the forwarding pod: %d %v", a.status, a.frames)
	}
	if leader.s.nChans != 0 {
		t.Error("the leader opened a channel the role does not allow")
	}
	a, h, err := call(peerClient(leader), http.MethodPost, leader.peerURL("/query"), "not-a-jwt", q("SELECT 1"), "Skylite-Forwarded", "1", "Skylite-Budget-Ms", "1000")
	if err != nil || a.status != http.StatusUnauthorized || h.Get("WWW-Authenticate") != "Bearer" {
		t.Errorf("bad token through a peer: %d %v %v", a.status, a.frames, err)
	}
	start := time.Now()
	a, _, err = call(peerClient(leader), http.MethodPost, leader.peerURL("/query"), leader.sig.token(t, "reader"), fmt.Sprintf(`{"sql":%q,"timeout_ms":20000}`, cpuBound), "Skylite-Forwarded", "1", "Skylite-Budget-Ms", "200", "Skylite-Request-Id", "HOPID")
	if took := time.Since(start); err != nil || a.code() != "timeout" || a.err()["request_id"] != "HOPID" || took > 5*time.Second {
		t.Errorf("budget: %d %v %v after %v", a.status, a.frames, err, took)
	}
}

// A forwarded request running as its leader shuts down finishes and is
// relayed; then the record names the new leader, and a channel of the old
// one is lost wherever it is sent.
func TestForwardLeaderChange(t *testing.T) {
	p := peerConfig(t)
	leader, hs := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	pod := newPod(t, leader, p, nil, false, nil)
	tok := pod.open(t, "writer")
	g, release := hs.hold()
	got := pod.postAsync(t, "channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)"))
	g.wait(t)
	halted := make(chan error, 1)
	go func() { halted <- leader.halt() }()
	waitReplica(t, leader.s.stopping.Load, 5*time.Second, "the leader did not start shutting down")
	release()
	if a := <-got; a.done() == nil || num(a.done(), "txn") == 0 {
		t.Fatalf("in-flight write: %d %v", a.status, a.frames)
	}
	if err := <-halted; err != nil {
		t.Fatal(err)
	}
	pod.leader(t)
	rec := waitRecord(t, pod, func(r leaderRecord) bool { return r.Instance == pod.s.instance && r.Address != "" })
	old, _ := leader.record(t)
	if a := pod.post(t, "channel", "writer", ex(tok, 2, "INSERT INTO t VALUES (2)")); a.code() != "channel_lost" {
		t.Errorf("old channel on the new leader: %d %v", a.status, a.frames)
	}
	third := newPod(t, leader, p, nil, false, nil)
	if a := third.post(t, "channel", "writer", ex(tok, 2, "INSERT INTO t VALUES (2)")); a.code() != "channel_lost" || pod.s.peerSeen.Load() != 0 {
		t.Errorf("old channel through a third pod: %d %v (record %+v, was %+v)", a.status, a.frames, rec, old)
	}
	if next := third.open(t, "writer"); !strings.HasPrefix(next, channelTokenPrefix+"."+pod.s.instance+".") {
		t.Errorf("open went to %q", next)
	}
	if a := pod.post(t, "query", "reader", q("SELECT count(*) FROM t")); a.rows()[0] != "[1]" {
		t.Errorf("rows: %v", a.frames)
	}
}

// The record is published when office is announced, withdrawn when it
// ends, and never replaced by an older office's.
func TestLeaderRecord(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	rec, err := leader.record(t)
	st := leader.s.dbs["d"].loop.State()
	if err != nil || rec.Address != leader.s.peer.address || rec.Epoch != st.Office.Epoch || rec.Head != st.Office.Head || rec.DB != hex.EncodeToString(st.Office.DB[:]) {
		t.Fatalf("record %+v, %v; office %+v", rec, err, st.Office)
	}
	keys, _ := leader.s.cfg.Storage.keys("d")
	// A step-down withdraws it, and the next office publishes again.
	if err := leader.leader(t).Close(); err != nil {
		t.Fatal(err)
	}
	waitRecord(t, leader, func(r leaderRecord) bool { return r.Epoch == rec.Epoch && r.Address == "" })
	rec = waitRecord(t, leader, func(r leaderRecord) bool { return r.Epoch > rec.Epoch && r.Address != "" })
	older := rec
	older.Epoch, older.Instance = rec.Epoch-1, "OLD"
	if v, err := leader.s.publish(context.Background(), keys.Leader(), older); err != nil || v != "" {
		t.Errorf("older publish: %q, %v", v, err)
	}
	if err := leader.halt(); err != nil {
		t.Fatal(err)
	}
	if got, err := leader.record(t); err != nil || got.Address != "" || got.Instance != rec.Instance || got.Epoch != rec.Epoch {
		t.Errorf("after shutdown: %+v, %v", got, err)
	}
	// A withdrawal of a version another server replaced leaves it.
	newer := rec
	newer.Epoch, newer.Instance, newer.Address = rec.Epoch+1, "NEW", "10.0.0.1:1"
	_, v, _ := leader.s.readRecord(context.Background(), keys.Leader())
	if _, err := leader.s.publish(context.Background(), keys.Leader(), newer); err != nil {
		t.Fatal(err)
	}
	leader.s.withdraw(keys.Leader(), rec, v, leader.s.log)
	if got, _ := leader.record(t); got != newer {
		t.Errorf("stale withdrawal changed %+v", got)
	}

	// A standby publishes nothing.
	idle := newEnv(t, nil)
	idle.s.cfg.Databases = []string{"x"}
	addPeer(t, idle, p)
	if err := idle.s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	idle.halt = idle.start(t)
	time.Sleep(300 * time.Millisecond)
	xkeys, _ := idle.s.cfg.Storage.keys("x")
	if _, _, err := idle.s.readRecord(context.Background(), xkeys.Leader()); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("standby record: %v", err)
	}
}

// recordFailStore fails the writes of the leader record while fails is positive.
type recordFailStore struct {
	storage.Store
	fails atomic.Int64
}

func (s *recordFailStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if strings.HasSuffix(key, "/leader") && s.fails.Add(-1) >= 0 {
		return "", errors.New("leader record write failed by the test")
	}
	return s.Store.Create(ctx, key, body)
}

// A leader record that could not be written is written again.
func TestLeaderRecordRetry(t *testing.T) {
	p := peerConfig(t)
	var fs *recordFailStore
	e := newLeaderEnv(t, func(st storage.Store) storage.Store {
		fs = &recordFailStore{Store: st}
		fs.fails.Store(1)
		return fs
	}, func(e *env) { addPeer(t, e, p) })
	waitRecord(t, e, func(r leaderRecord) bool { return r.Instance == e.s.instance && r.Address != "" })
	if fs.fails.Load() >= 0 {
		t.Error("the failing write was never tried")
	}
}

func TestNewPeerRefuses(t *testing.T) {
	p := peerConfig(t)
	c := *p
	c.AddressEnv = "SKYLITE_TEST_PEER_UNSET"
	t.Setenv(c.AddressEnv, ":8444")
	if _, err := newPeer(&c); err == nil {
		t.Error("an address without a host was accepted")
	}
	t.Setenv(c.AddressEnv, "10.0.0.1:8444")
	if _, err := newPeer(&c); err != nil {
		t.Fatal(err)
	}
	c.CAFile = c.KeyFile
	if _, err := newPeer(&c); err == nil {
		t.Error("a CA file without a certificate was accepted")
	}
}
