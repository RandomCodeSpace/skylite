package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// resolution checks that a is a resolve answer r at txn.
func resolution(t *testing.T, what string, a answer, r string, txn int64) {
	t.Helper()
	if a.status != http.StatusOK || a.done()["resolution"] != r || num(a.done(), "txn") != txn {
		t.Errorf("%s: %d %v, want %s at %d", what, a.status, a.frames, r, txn)
	}
}

// head is the leader's head as a wire integer.
func (e *env) head(t *testing.T) int64 {
	t.Helper()
	return int64(e.leader(t).Head()) //nolint:gosec // small
}

// withHead returns tok with its open head replaced by o.
func withHead(tok string, o uint64) string {
	parts := strings.Split(tok, ".")
	parts[3] = fmt.Sprint(o)
	return strings.Join(parts, ".")
}

// TestResolveOnLeader: committed, pending while the channel is live or
// closing, not_committed once it has ended, and the refusals before
// acceptance, which change nothing.
func TestResolveOnLeader(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	res := func(tok string, seq int64) answer { return e.post(t, "resolve", "writer", cancelBody(tok, seq)) }
	a := e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	c1 := num(a.done(), "txn")
	resolution(t, "a commit of a live channel", res(tok, 1), "committed", c1)
	resolution(t, "a request not yet sent", res(tok, 2), "pending", c1)
	// A write that changed nothing creates no commit.
	if a := e.post(t, "channel", "writer", ex(tok, 2, "UPDATE t SET a = 1 WHERE 0")); a.done() == nil {
		t.Fatal(a.frames)
	}
	resolution(t, "a write that changed nothing, channel live", res(tok, 2), "pending", c1)

	// In its upload, and then closing: pending; once it lands, committed.
	g, release := hs.hold()
	defer release()
	got := e.postAsync(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (1)"))
	g.wait(t)
	resolution(t, "a write in its upload", res(tok, 3), "pending", c1)
	closed := e.postAsync(t, "channel/close", "writer", closeBody(tok))
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.closing })
	resolution(t, "a write in its upload on a closing channel", res(tok, 3), "pending", c1)
	release()
	c3 := num((<-got).done(), "txn")
	if c := <-closed; c.done() == nil || c3 != c1+1 {
		t.Fatalf("close %v, write at %d", c.frames, c3)
	}
	resolution(t, "the landed write", res(tok, 3), "committed", c3)
	resolution(t, "a write that changed nothing, channel ended", res(tok, 2), "not_committed", c3)
	resolution(t, "a request never sent, channel ended", res(tok, 4), "not_committed", c3)
	resolution(t, "the first write, channel ended", res(tok, 1), "committed", c1)
	resolution(t, "an altered secret", res(tok[:len(tok)-2]+"AA", 1), "not_committed", c3)

	// Refusals before acceptance.
	id := e.dbID(t)
	db := hex.EncodeToString(id[:])
	for _, tc := range []struct {
		name, subject, body, code string
		status                    int
	}{
		{"another database", "writer", cancelBody(strings.Replace(tok, db, strings.Repeat("0", 32), 1), 1), "invalid_request", 400},
		{"open head not a number", "writer", cancelBody(strings.Replace(withHead(tok, 1), ".1.", ".x.", 1), 1), "invalid_request", 400},
		{"no seq", "writer", closeBody(tok), "invalid_request", 400},
		{"seq 0", "writer", cancelBody(tok, 0), "invalid_request", 400},
		{"another field", "writer", fmt.Sprintf(`{"channel":%q,"seq":1,"op":"commit"}`, tok), "invalid_request", 400},
		{"read role only", "reader", cancelBody(tok, 1), "forbidden", 403},
		{"open head above the head", "writer", cancelBody(withHead(tok, uint64(c3)+5), 1), "behind", 503}, //nolint:gosec // small
	} {
		if a := e.post(t, "resolve", tc.subject, tc.body); a.status != tc.status || a.code() != tc.code || a.err()["not_executed"] != true || a.err()["final"] != false {
			t.Errorf("%s: %d %v, want %s", tc.name, a.status, a.frames, tc.code)
		}
	}
	if a := e.post(t, "resolve", "writer", fmt.Sprintf(`{"channel":%q,"seq":1,"x":%q}`, tok, strings.Repeat("a", 4100))); a.status != http.StatusRequestEntityTooLarge {
		t.Errorf("a resolve body over 4 KiB: %d %v", a.status, a.frames)
	}
}

// TestResolveBound: an open head more than the resolve bound below the head
// is unknown, at the bound it is answered.
func TestResolveBound(t *testing.T) {
	e, _ := newChanEnv(t, func(e *env) { e.s.limits.ResolveBound = 2 })
	tok := e.open(t, "writer")
	o := e.head(t)
	other := e.open(t, "writer")
	for i := range 3 {
		if a := e.post(t, "channel", "writer", ex(other, int64(i+1), "CREATE TABLE t"+fmt.Sprint(i)+" (a)")); a.done() == nil {
			t.Fatal(a.frames)
		}
		want := "pending"
		if i == 2 {
			want = "unknown"
		}
		resolution(t, fmt.Sprintf("%d commits above the open head", i+1), e.post(t, "resolve", "writer", cancelBody(tok, 1)), want, o+int64(i)+1)
	}
}

// TestResolveStoppedLeader: a leader that has stopped answers unknown,
// never from its table.
func TestResolveStoppedLeader(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	l := e.leader(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	done, we := e.s.resolve(context.Background(), e.s.dbs["d"], l, &wire.Request{Kind: wire.KindResolve, Channel: tok, Seq: 1})
	if we != nil || done.Resolution != wire.ResolutionUnknown {
		t.Errorf("%+v %v, want unknown", done, we)
	}
}

// TestResolveOnDeposedLeader: a leader deposed without knowing it answers
// behind for a channel of the leader that deposed it.
func TestResolveOnDeposedLeader(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	keys, _ := e.s.cfg.Storage.keys("d")
	o, _, err := engine.Takeover(context.Background(), e.store, keys, engine.Candidate{Holder: "usurper", TTL: time.Minute, Now: func() time.Time { return time.Now().Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	if a := e.post(t, "resolve", "writer", cancelBody(withHead(tok, o.Head), 1)); a.status != http.StatusServiceUnavailable || a.code() != "behind" {
		t.Errorf("a later office's channel: %d %v, want behind", a.status, a.frames)
	}
}

// landStore makes a commit's create land, then report an unknown outcome
// for it and every attempt after, while armed; with skip set, the create
// does not land at all.
type landStore struct {
	storage.Store
	armed, skip atomic.Bool
}

func (s *landStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if !s.armed.Load() || !strings.Contains(key, "/log/") {
		return s.Store.Create(ctx, key, body)
	}
	if !s.skip.Swap(true) {
		if _, err := s.Store.Create(ctx, key, body); err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("%w: injected", storage.ErrOutcomeUnknown)
}

// TestResolveUnknownThenSettled: a write whose outcome its leader could not
// learn stops that leader; once the server leads again, under a new epoch,
// resolve settles it: committed if its create landed, not_committed if not.
func TestResolveUnknownThenSettled(t *testing.T) {
	var ls *landStore
	e := newLeaderEnv(t, func(st storage.Store) storage.Store { ls = &landStore{Store: st}; return ls }, nil)
	e.exec(t, "CREATE TABLE t (a)")
	for _, landed := range []bool{true, false} {
		tok := e.open(t, "writer")
		l := e.leader(t)
		ls.skip.Store(!landed)
		ls.armed.Store(true)
		a := e.post(t, "channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)"))
		ls.armed.Store(false)
		ls.skip.Store(false)
		if a.code() != "commit_outcome_unknown" || a.err()["not_executed"] != false {
			t.Fatalf("landed %v: %d %v, want commit_outcome_unknown", landed, a.status, a.frames)
		}
		if l.Err() == nil {
			t.Fatal("the leader still serves after an unknown outcome")
		}
		landedAt := l.Head() + 1
		next := e.leader(t)
		r := e.post(t, "resolve", "writer", cancelBody(tok, 1))
		if landed {
			resolution(t, "a create that landed", r, "committed", int64(landedAt)) //nolint:gosec // small
		} else {
			resolution(t, "a create that did not land", r, "not_committed", int64(next.Head())) //nolint:gosec // small
		}
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[1]" {
		t.Errorf("rows %s, want the landed write alone", got)
	}
}

// TestResolveReadsHeaders: after a takeover whose manifest is above a
// channel's open head, a resolve reads the headers below the engine's index
// from storage, retired ones included, and settles from them.
func TestResolveReadsHeaders(t *testing.T) {
	e, _ := newChanEnv(t, func(e *env) { e.s.engine.CompactInterval, e.s.engine.Grace = 10*time.Millisecond, time.Millisecond })
	tok := e.open(t, "writer")
	c1 := num(e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)")).done(), "txn")
	other := e.open(t, "writer")
	for i := range 3 {
		e.post(t, "channel", "writer", ex(other, int64(i+1), "INSERT INTO t VALUES (1)"))
	}
	// Once a compaction has covered c1, take office again: the new leader's
	// index starts at its manifest.
	keys, _ := e.s.cfg.Storage.keys("d")
	db := e.dbID(t)
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		rc, _, err := e.store.Get(context.Background(), keys.Manifest())
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(rc)
		_ = rc.Close()
		m, derr := format.DecodeManifest(b, db)
		if err != nil || derr != nil {
			t.Fatal(err, derr)
		}
		if m.Txn >= uint64(c1) { //nolint:gosec // small
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no compaction covered %d", c1)
		}
	}
	if err := e.leader(t).Close(); err != nil {
		t.Fatal(err)
	}
	if l := e.leader(t); l.IndexFloor() < uint64(c1) { //nolint:gosec // small
		t.Fatalf("the new leader's index floor %d is below %d", l.IndexFloor(), c1)
	}
	resolution(t, "a commit below the index", e.post(t, "resolve", "writer", cancelBody(tok, 1)), "committed", c1)
	resolution(t, "a request never sent", e.post(t, "resolve", "writer", cancelBody(tok, 2)), "not_committed", e.head(t))
	e.s.chMu.Lock()
	tb := e.s.dbs["d"].chans
	n := len(tb.hdrs)
	e.s.chMu.Unlock()
	if n == 0 {
		t.Error("no header was read from storage")
	}
	// Every header up to the head reads from storage, the bumps included.
	l := e.leader(t)
	if found, bump, ok := e.s.headers(context.Background(), tb, l, wantID(tok, 1), 2, l.Head()); found != uint64(c1) || bump == 0 || !ok { //nolint:gosec // small
		t.Errorf("headers: found %d, bump %d, %v; want %d and a bump", found, bump, ok, c1)
	}
	if found, bump, ok := e.s.headers(context.Background(), tb, l, wantID(tok, 1), uint64(c1), uint64(c1)+3); found != 0 || bump != 0 || !ok { //nolint:gosec // small
		t.Errorf("headers above c1: found %d, bump %d, %v; want neither", found, bump, ok)
	}
}

// TestResolveAcrossLeaders: a write in transit while its leader shuts down
// completes there; the next leader, another server, settles the channel's
// requests through any pod, and answers not_committed for one never sent.
func TestResolveAcrossLeaders(t *testing.T) {
	p := peerConfig(t)
	leader, hs := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	pod := newPod(t, leader, p, nil, false, nil)
	tok := pod.open(t, "writer")
	res := func(seq int64) answer { return pod.post(t, "resolve", "writer", cancelBody(tok, seq)) }
	c1 := num(pod.post(t, "channel", "writer", ex(tok, 1, "INSERT INTO t VALUES (1)")).done(), "txn")
	resolution(t, "forwarded to the leader, committed", res(1), "committed", c1)
	resolution(t, "forwarded to the leader, live", res(2), "pending", c1)
	g, release := hs.hold()
	got := pod.postAsync(t, "channel", "writer", ex(tok, 2, "INSERT INTO t VALUES (2)"))
	g.wait(t)
	halted := make(chan error, 1)
	go func() { halted <- leader.halt() }()
	waitReplica(t, leader.s.stopping.Load, 5*time.Second, "the leader did not start shutting down")
	release()
	c2 := num((<-got).done(), "txn")
	if err := <-halted; err != nil || c2 != c1+1 {
		t.Fatalf("halt %v, write at %d", err, c2)
	}
	pod.leader(t)
	waitRecord(t, pod, func(r leaderRecord) bool { return r.Instance == pod.s.instance && r.Address != "" })
	h := pod.head(t)
	resolution(t, "the first write, on the next leader", res(1), "committed", c1)
	resolution(t, "the write in transit, on the next leader", res(2), "committed", c2)
	resolution(t, "a request never sent, on the next leader", res(3), "not_committed", h)
	third := newPod(t, leader, p, nil, false, nil)
	resolution(t, "through a third pod", third.post(t, "resolve", "writer", cancelBody(tok, 3)), "not_committed", h)
}

// TestStatus: the leader answers GET status; a GET with a body is refused.
func TestStatus(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	a, _, err := call(h1Client(), http.MethodGet, "http://"+e.addr+"/v1/databases/d", e.sig.token(t, "reader"), "")
	d := a.done()
	lim, _ := d["limits"].(map[string]any)
	if err != nil || a.status != http.StatusOK || num(d, "txn") != e.head(t) || num(d, "epoch") != int64(e.s.dbs["d"].loop.State().Office.Epoch) || !strings.HasPrefix(fmt.Sprint(d["sqlite_version"]), "3.") || num(lim, "resolve_bound") != 100000 { //nolint:gosec // small
		t.Errorf("status: %d %v %v", a.status, a.frames, err)
	}
	r := raw(t, e.addr, "GET /v1/databases/d HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer "+e.sig.token(t, "reader")+"\r\nContent-Length: 2\r\n\r\n{}")
	defer func() { _ = r.Body.Close() }()
	if r.StatusCode != http.StatusBadRequest || errorCode(frame(t, r.Body)) != "invalid_request" {
		t.Errorf("a GET with a body: %d", r.StatusCode)
	}
	if a, _, _ := call(h1Client(), http.MethodGet, "http://"+e.addr+"/v1/databases/d", e.sig.token(t, "nobody"), ""); a.status != http.StatusForbidden {
		t.Errorf("status without a role: %d %v", a.status, a.frames)
	}
}
