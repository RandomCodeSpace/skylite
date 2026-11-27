package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// standbyStore is a replicaStore that, while noOffice is set, refuses every
// write of the lease, so that its server stays a standby.
type standbyStore struct {
	replicaStore
	noOffice atomic.Bool
	noPeer   atomic.Bool // the peer record's writes fail
}

var errNoOffice = errors.New("test: this server takes no office")

func (s *standbyStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if s.noOffice.Load() && strings.HasSuffix(key, "/lease") || s.noPeer.Load() && strings.Contains(key, "_peers/") {
		return "", errNoOffice
	}
	return s.replicaStore.Create(ctx, key, body)
}

func (s *standbyStore) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if s.noOffice.Load() && strings.HasSuffix(key, "/lease") {
		return "", errNoOffice
	}
	return s.replicaStore.Replace(ctx, key, version, body)
}

// newStandby returns another started server of "d" on st over leader's
// store, with a peer listener and follower reads and no page cache, so that
// every page read reaches st, once its follower is fresh and, unless
// st.noPeer is set, its peer record is published.
func newStandby(t *testing.T, leader *env, p *Peer, st *standbyStore) *env {
	t.Helper()
	st.Store = leader.store
	cfg := leader.s.cfg
	cfg.FollowerReads, cfg.Peer = true, nil
	logs := &lockedBuffer{}
	s, err := newServer(cfg, slog.New(slog.NewJSONHandler(logs, nil)), st)
	if err != nil {
		t.Fatal(err)
	}
	s.engine.Cache = engine.NewCache(0)
	e := &env{s: s, sig: leader.sig, logs: logs, store: leader.store}
	addPeer(t, e, p)
	if err := s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	e.halt = e.start(t)
	waitReplica(t, e.s.ready, 5*time.Second, "the standby's follower did not become fresh")
	if !st.noPeer.Load() {
		e.peered(t)
	}
	return e
}

// peered waits until e's peer record is published.
func (e *env) peered(t *testing.T) {
	t.Helper()
	waitReplica(t, func() bool {
		_, _, err := e.s.readRecord(context.Background(), e.s.cfg.Storage.peerKey(e.s.instance))
		return err == nil && e.s.peered.Load()
	}, 5*time.Second, "the standby's peer record was not published")
}

// caughtUp waits until e's follower has applied transaction txn.
func (e *env) caughtUp(t *testing.T, txn int64) {
	t.Helper()
	waitReplica(t, func() bool {
		f := e.s.dbs["d"].loop.State().Follower
		return f != nil && f.Fresh() && int64(f.Txn()) >= txn //nolint:gosec // small
	}, 5*time.Second, "the follower did not catch up")
}

// openAt opens a read-only channel on e at min_txn and returns its token.
func (e *env) openAt(t *testing.T, minTxn int64) (string, answer) {
	t.Helper()
	a := e.post(t, "channels", "reader", fmt.Sprintf(`{"mode":"read_only","min_txn":%d}`, minTxn))
	c, _ := a.done()["channel"].(map[string]any)
	tok, _ := c["token"].(string)
	if a.status != http.StatusOK || !strings.HasPrefix(tok, readTokenPrefix+".") {
		t.Fatalf("read-only open: %d %v", a.status, a.frames)
	}
	return tok, a
}

// reads returns the follower's channel of tok on e, its state and whether
// it holds a transaction, and whether e's follower table has it.
func (e *env) reads(tok string) (wire.TxnState, bool, bool) {
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	t := e.s.dbs["d"].reads
	if t == nil || t.chans[tok] == nil {
		return "", false, false
	}
	ch := t.chans[tok]
	return ch.state, ch.tx != nil, true
}

// TestFollowerReadOnlyChannel: a standby owns a read-only channel opened on
// it, at its follower's snapshot; the leader routes the channel's requests
// there by the standby's peer record; a transaction reads the snapshot it
// pinned at its begin while the leader commits; writes are refused; and
// everything the channel held is released.
func TestFollowerReadOnlyChannel(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1)")
	head := leader.head(t)
	pod := newStandby(t, leader, p, &standbyStore{})
	tok, a := pod.openAt(t, head)
	if !strings.HasPrefix(tok, readTokenPrefix+"."+pod.s.instance+".") || num(a.done(), "txn") < head || !strings.Contains(tok, fmt.Sprintf(".%d.", num(a.done(), "txn"))) {
		t.Fatalf("open on the standby: %q %v", tok, a.frames)
	}
	if _, _, ok := pod.reads(tok); !ok {
		t.Fatal("the standby's follower table has no channel")
	}

	// Through the leader: routed one hop by the peer record.
	a = leader.post(t, "channel", "reader", sq(tok, 1, "begin", "execute", map[string]any{"sql": "SELECT count(*) FROM t"}))
	snap := num(a.done(), "txn")
	if a.done()["txn_state"] != "open" || snap < head || len(a.rows()) != 1 || a.rows()[0] != "[1]" {
		t.Fatalf("begin through the leader: %d %v", a.status, a.frames)
	}
	if st, open, _ := pod.reads(tok); st != wire.TxnOpen || !open {
		t.Fatalf("the standby's channel is %q, transaction held %t", st, open)
	}
	leader.exec(t, "INSERT INTO t VALUES (2)")
	next := leader.head(t)
	pod.caughtUp(t, next)
	post := func(body string) answer { return pod.post(t, "channel", "reader", body) }
	if a := post(ex(tok, 2, "SELECT count(*) FROM t")); len(a.rows()) != 1 || a.rows()[0] != "[1]" || num(a.done(), "txn") != snap || a.done()["txn_state"] != "open" {
		t.Errorf("a read in the transaction after a commit: %v", a.frames)
	}
	readonly(t, "a write in the transaction", post(ex(tok, 3, "INSERT INTO t VALUES (3)")), false, "open")
	if a := post(sq(tok, 4, "describe", "sql", "SELECT a FROM t")); num(a.done(), "txn") != snap {
		t.Errorf("describe in the transaction: %v", a.frames)
	}
	if a := post(sq(tok, 5, "ping")); num(a.done(), "txn") != snap || a.done()["txn_state"] != "open" {
		t.Errorf("ping: %v", a.frames)
	}
	if a := leader.post(t, "channel", "reader", sq(tok, 6, "commit")); a.done()["committed"] != true || a.done()["wrote"] != false || num(a.done(), "txn") != snap {
		t.Errorf("commit through the leader: %v", a.frames)
	}
	if a := post(ex(tok, 7, "SELECT count(*) FROM t", "min_txn", next)); len(a.rows()) != 1 || a.rows()[0] != "[2]" || num(a.done(), "txn") < next || a.done()["txn_state"] != "none" {
		t.Errorf("an autocommit read after the commit: %v", a.frames)
	}
	readonly(t, "an autocommit write", post(ex(tok, 8, "INSERT INTO t VALUES (3)")), true, "none")
	if a := post(sq(tok, 9, "ping")); num(a.done(), "txn") < next {
		t.Errorf("idle ping: %v", a.frames)
	}
	if c := leader.post(t, "channel/close", "reader", closeBody(tok)); num(c.done(), "last_seq") != 9 {
		t.Errorf("close through the leader: %v", c.frames)
	}
	if _, _, ok := pod.reads(tok); ok {
		t.Error("the closed channel stayed in the table")
	}
	if got := leader.count(t, "SELECT count(*) FROM t"); got != "[2]" {
		t.Errorf("rows %s", got)
	}
	// A token of this process that names no channel of its table.
	forged := strings.Join(append(strings.Split(tok, ".")[:4], strings.Repeat("A", 43)), ".")
	for _, e := range []*env{pod, leader} {
		if a := e.post(t, "channel", "reader", ex(forged, 1, "SELECT 1")); a.code() != "channel_lost" || a.err()["not_executed"] != true {
			t.Errorf("an unknown token at the leader %t: %d %v", e == leader, a.status, a.frames)
		}
	}
	pod.s.pool.mu.Lock()
	busy := len(pod.s.pool.busy)
	pod.s.pool.mu.Unlock()
	if busy != 0 {
		t.Errorf("%d principals hold connections", busy)
	}
	pod.noReadTxns(t)
}

// TestFollowerReadOnlyMinTxn: an open the follower cannot serve at min_txn
// within the catch-up limit goes to the leader, which owns that channel; a
// begin on a follower's channel above what it reaches is behind, not
// executed, and the channel takes the same sequence number once it has
// caught up.
func TestFollowerReadOnlyMinTxn(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	st := &standbyStore{}
	pod := newStandby(t, leader, p, st)
	tok, _ := pod.openAt(t, leader.head(t))
	release := pauseReplica(t, &st.replicaStore)
	leader.exec(t, "INSERT INTO t VALUES (1)")
	head := leader.head(t)
	a := pod.post(t, "channel", "reader", sq(tok, 1, "begin", "min_txn", head))
	if a.status != http.StatusServiceUnavailable || a.code() != "behind" || a.err()["not_executed"] != true {
		t.Fatalf("a begin above the follower: %d %v", a.status, a.frames)
	}
	if lt, _ := pod.openAt(t, head); !strings.HasPrefix(lt, readTokenPrefix+"."+leader.s.instance+".") {
		t.Errorf("an open above the follower went to %q", lt)
	}
	release()
	pod.caughtUp(t, head)
	if a := pod.post(t, "channel", "reader", sq(tok, 1, "begin", "min_txn", head)); a.done()["txn_state"] != "open" || num(a.done(), "txn") < head {
		t.Errorf("the begin after catching up: %d %v", a.status, a.frames)
	}
	pod.post(t, "channel/close", "reader", closeBody(tok))
	pod.noReadTxns(t)
}

// TestFollowerReadOnlyPeerRecord: until its peer record is published, which
// other pods route its channels by, a standby forwards read-only opens to
// the leader.
func TestFollowerReadOnlyPeerRecord(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	st := &standbyStore{}
	st.noPeer.Store(true)
	pod := newStandby(t, leader, p, st)
	if tok, _ := pod.openAt(t, 0); !strings.HasPrefix(tok, readTokenPrefix+"."+leader.s.instance+".") {
		t.Errorf("an open without a peer record went to %q", tok)
	}
	st.noPeer.Store(false)
	pod.peered(t)
	if tok, _ := pod.openAt(t, 0); !strings.HasPrefix(tok, readTokenPrefix+"."+pod.s.instance+".") {
		t.Errorf("an open with the peer record went to %q", tok)
	}
}

// TestFollowerReadOnlyOffice: a follower's channels survive a leader change
// on other pods, whose new leader routes to them, and end when their own
// server takes office, which releases what they held.
func TestFollowerReadOnlyOffice(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1)")
	st := &standbyStore{}
	st.noOffice.Store(true)
	pod := newStandby(t, leader, p, st)
	third := newPod(t, leader, p, nil, false, nil)
	tok, _ := pod.openAt(t, leader.head(t))
	if a := pod.post(t, "channel", "reader", sq(tok, 1, "begin")); a.done()["txn_state"] != "open" {
		t.Fatalf("begin: %v", a.frames)
	}
	if err := leader.halt(); err != nil {
		t.Fatal(err)
	}
	third.leader(t)
	waitRecord(t, third, func(r leaderRecord) bool { return r.Instance == third.s.instance && r.Address != "" })
	if a := third.post(t, "channel", "reader", ex(tok, 2, "SELECT count(*) FROM t")); len(a.rows()) != 1 || a.rows()[0] != "[1]" || a.done()["txn_state"] != "open" {
		t.Errorf("a read in the transaction after another pod took office: %d %v", a.status, a.frames)
	}
	if st, open, ok := pod.reads(tok); !ok || st != wire.TxnOpen || !open {
		t.Fatalf("the follower's channel after the leader change: %q %t %t", st, open, ok)
	}

	st.noOffice.Store(false)
	if err := third.halt(); err != nil {
		t.Fatal(err)
	}
	pod.leader(t)
	waitReplica(t, func() bool {
		pod.s.chMu.Lock()
		defer pod.s.chMu.Unlock()
		return pod.s.dbs["d"].reads == nil
	}, 5*time.Second, "the follower's table outlived its follower")
	waitReplica(t, func() bool {
		pod.s.pool.mu.Lock()
		defer pod.s.pool.mu.Unlock()
		return pod.s.pool.txns == 0 && len(pod.s.pool.busy) == 0
	}, 5*time.Second, "the ended transaction still holds its connection")
	if a := pod.post(t, "channel", "reader", ex(tok, 3, "SELECT 1")); a.code() != "channel_lost" {
		t.Errorf("the channel after its server took office: %d %v", a.status, a.frames)
	}
}

// TestFollowerReadOnlyCaps: a follower's read-only transactions count
// against the pod's read-only transaction places; with none left for the
// principal, a begin is refused overloaded before acceptance and an open
// goes to the leader.
func TestFollowerReadOnlyCaps(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	pod := newStandby(t, leader, p, &standbyStore{})
	pod.s.pool.mu.Lock()
	pod.s.pool.capacity, pod.s.pool.perPrincipal = 8, 2 // 1 read-only transaction a principal
	pod.s.pool.mu.Unlock()
	head := leader.head(t)
	held, _ := pod.openAt(t, head)
	other, _ := pod.openAt(t, head)
	if a := pod.post(t, "channel", "reader", sq(held, 1, "begin")); a.done()["txn_state"] != "open" {
		t.Fatalf("begin: %v", a.frames)
	}
	if a := pod.post(t, "channel", "reader", sq(other, 1, "begin")); a.status != http.StatusTooManyRequests || a.code() != "overloaded" || a.err()["not_executed"] != true {
		t.Errorf("a begin past the principal's places: %d %v", a.status, a.frames)
	}
	if lt, _ := pod.openAt(t, head); !strings.HasPrefix(lt, readTokenPrefix+"."+leader.s.instance+".") {
		t.Errorf("an open with no place left went to %q", lt)
	}
	pod.post(t, "channel", "reader", sq(held, 2, "commit"))
	if a := pod.post(t, "channel", "reader", sq(other, 1, "begin")); a.done()["txn_state"] != "open" {
		t.Errorf("a begin after the commit: %v", a.frames)
	}
	pod.post(t, "channel/close", "reader", closeBody(other))
	pod.post(t, "channel/close", "reader", closeBody(held))
	pod.noReadTxns(t)
}

// TestFollowerReadOnlyLimits: the read-only transaction idle limit aborts a
// follower's idle transaction.
func TestFollowerReadOnlyLimits(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	pod := newStandby(t, leader, p, &standbyStore{})
	pod.s.chMu.Lock()
	pod.s.limits.ReadTxnIdleMS = 300
	pod.s.chMu.Unlock()
	tok, _ := pod.openAt(t, leader.head(t))
	pod.post(t, "channel", "reader", sq(tok, 1, "begin"))
	waitReplica(t, func() bool { st, open, _ := pod.reads(tok); return st == wire.TxnAborted && !open }, 10*time.Second, "the idle transaction was not aborted")
	if a := pod.post(t, "channel", "reader", sq(tok, 2, "ping")); a.code() != "transaction_aborted" {
		t.Errorf("after the idle limit: %v", a.frames)
	}
	pod.noReadTxns(t)
}

// TestFollowerReadOnlyCompacted: compaction that overtakes an open
// transaction's snapshot aborts it with cause maintenance.
func TestFollowerReadOnlyCompacted(t *testing.T) {
	p := peerConfig(t)
	leader, _ := newPeerLeader(t, p)
	leader.exec(t, "CREATE TABLE t (a INTEGER); CREATE TABLE u (a INTEGER); INSERT INTO u VALUES (1)")
	var armed atomic.Bool
	st := &standbyStore{replicaStore: replicaStore{read: func(context.Context) error {
		if armed.CompareAndSwap(true, false) {
			return engine.ErrCompacted
		}
		return nil
	}}}
	pod := newStandby(t, leader, p, st)
	tok, _ := pod.openAt(t, leader.head(t))
	post := func(body string) answer { return pod.post(t, "channel", "reader", body) }
	if a := post(sq(tok, 1, "begin", "execute", map[string]any{"sql": "SELECT count(*) FROM t"})); a.done()["txn_state"] != "open" {
		t.Fatalf("begin: %v", a.frames)
	}
	armed.Store(true)
	a := post(ex(tok, 2, "SELECT count(*) FROM u")) // reads u's page from storage
	if armed.Load() || a.code() != "maintenance" || a.err()["txn_state"] != "aborted" || a.err()["final"] != true {
		t.Fatalf("a read compaction overtook: %d %v", a.status, a.frames)
	}
	if a := post(sq(tok, 3, "ping")); a.code() != "transaction_aborted" || a.err()["cause"] != "maintenance" {
		t.Errorf("after the abort: %v", a.frames)
	}
	if a := post(sq(tok, 4, "rollback")); a.done() == nil {
		t.Errorf("rollback: %v", a.frames)
	}
	pod.noReadTxns(t)
}

// TestReadTxnLifetimeWithinGrace: a follower's read-only transaction reads a
// snapshot no older than the follower's freshness bound when it begins, and
// lives at most the read-only transaction lifetime; compaction retires and
// deletes what that snapshot reads only after the grace period. A change
// that lets the transaction outlive the grace fails here.
func TestReadTxnLifetimeWithinGrace(t *testing.T) {
	life := time.Duration(wire.DefaultLimits().ReadTxnLifetimeMS) * time.Millisecond
	grace := cmp.Or(engineConfig().Grace, engine.DefaultGrace)
	if life+engine.FollowerFreshness >= grace {
		t.Errorf("read-only lifetime %v plus follower freshness %v reaches the grace period %v", life, engine.FollowerFreshness, grace)
	}
}
