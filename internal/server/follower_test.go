package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

type replicaStore struct {
	storage.Store
	pause atomic.Pointer[pollPause]
	read  func(context.Context) error // optional fault on a page read
}

type pollPause struct {
	entered, release chan struct{}
	once             sync.Once
}

func (s *replicaStore) Head(ctx context.Context, key string) (storage.Info, error) {
	if p := s.pause.Load(); p != nil && strings.HasSuffix(key, "/manifest") {
		p.once.Do(func() { close(p.entered) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return storage.Info{}, ctx.Err()
		}
	}
	return s.Store.Head(ctx, key)
}

func (s *replicaStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if s.read != nil && n == pageSize+4 { // one page and its CRC
		if err := s.read(ctx); err != nil {
			return nil, storage.Info{}, err
		}
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func newReplicaEnv(t *testing.T, leader *env, enabled bool, st *replicaStore) *env {
	t.Helper()
	cfg := leader.s.cfg
	cfg.FollowerReads = enabled
	st.Store = leader.store
	s, err := newServer(cfg, leader.s.log, st)
	if err != nil {
		t.Fatal(err)
	}
	s.engine.Cache = engine.NewCache(0)
	if err := s.addDatabases(); err != nil {
		t.Fatal(err)
	}
	e := &env{s: s, sig: leader.sig, logs: leader.logs, store: leader.store}
	e.halt = e.start(t)
	return e
}

func waitReplica(t *testing.T, condition func() bool, limit time.Duration, message string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func pauseReplica(t *testing.T, st *replicaStore) func() {
	t.Helper()
	p := &pollPause{entered: make(chan struct{}), release: make(chan struct{})}
	st.pause.Store(p)
	release := sync.OnceFunc(func() {
		st.pause.Store(nil)
		close(p.release)
	})
	t.Cleanup(release)
	select {
	case <-p.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("follower never attempted a poll")
	}
	return release
}

func TestReplicaReads(t *testing.T) {
	leader := newQueryEnv(t, nil)
	leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (42)")
	replica := newReplicaEnv(t, leader, true, &replicaStore{})
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	if replica.s.dbs["d"].leader() != nil {
		t.Fatal("replica is leading")
	}
	for _, tc := range []struct{ path, sql string }{
		{"query", "SELECT a FROM t"},
		{"describe", "SELECT a FROM t WHERE a = :x"},
		{"describe", "INSERT INTO t VALUES (:x) RETURNING a"},
	} {
		a := leader.post(t, tc.path, "reader", q(tc.sql))
		b := replica.post(t, tc.path, "reader", q(tc.sql))
		if a.status != http.StatusOK || a.done() == nil || b.status != a.status || !reflect.DeepEqual(a.frames, b.frames) {
			t.Errorf("%s %s: leader %d %v, replica %d %v", tc.path, tc.sql, a.status, a.frames, b.status, b.frames)
		}
	}
	if a := replica.post(t, "query", "scoped", q("SELECT a FROM t")); a.code() != "forbidden" {
		t.Errorf("unauthorized read: %d %v", a.status, a.frames)
	}
	if a := replica.post(t, "query", "reader", q("ATTACH ':memory:' AS m")); a.code() != "statement_forbidden" {
		t.Errorf("policy: %d %v", a.status, a.frames)
	}
}

func TestReplicaMinTxn(t *testing.T) {
	leader := newQueryEnv(t, nil)
	leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1)")
	st := &replicaStore{}
	replica := newReplicaEnv(t, leader, true, st)
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	release := pauseReplica(t, st)
	want := leader.leader(t).Head() + 1
	leader.exec(t, "INSERT INTO t VALUES (2)")
	body := fmt.Sprintf(`{"sql":"SELECT max(a) FROM t","min_txn":%d}`, want)
	got := replica.postAsync(t, "query", "reader", body)
	select {
	case a := <-got:
		t.Fatalf("request ended before the new generation could publish: %d %v", a.status, a.frames)
	case <-time.After(45 * time.Millisecond):
	}
	release()
	a := <-got
	if a.status != http.StatusOK || a.done()["txn"] != json.Number(fmt.Sprint(want)) || !reflect.DeepEqual(a.rows(), []string{`[2]`}) {
		t.Fatalf("expected transaction %d after catch-up, got %d %v", want, a.status, a.frames)
	}
	for _, tc := range []struct {
		timeout int
		low     time.Duration
		high    time.Duration
	}{
		{2000, 430 * time.Millisecond, 850 * time.Millisecond},
		{50, 35 * time.Millisecond, 260 * time.Millisecond},
	} {
		start := time.Now()
		a := replica.post(t, "query", "reader", fmt.Sprintf(`{"sql":"SELECT 1","min_txn":%d,"timeout_ms":%d}`, want+1, tc.timeout))
		if elapsed := time.Since(start); a.status != http.StatusServiceUnavailable || a.code() != "behind" || a.err()["not_executed"] != true || elapsed < tc.low || elapsed > tc.high {
			t.Errorf("timeout_ms=%d: got %d %v after %v", tc.timeout, a.status, a.frames, elapsed)
		}
	}
}

func TestReplicaFreshness(t *testing.T) {
	leader := newQueryEnv(t, nil)
	st := &replicaStore{}
	replica := newReplicaEnv(t, leader, true, st)
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	release := pauseReplica(t, st)
	waitReplica(t, func() bool { return !replica.s.ready() }, 2200*time.Millisecond, "stale replica stayed ready")
	if a := replica.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
		t.Errorf("stale read: %d %v", a.status, a.frames)
	}
	release()
	waitReplica(t, replica.s.ready, 2*time.Second, "fresh replica stayed unready after polling resumed")
}

func TestReplicaCompacted(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d_pins", count), func(t *testing.T) {
			leader := newQueryEnv(t, nil)
			leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1)")
			type fault struct{ hit, release chan struct{} }
			faults := make(chan fault, count)
			st := &replicaStore{read: func(ctx context.Context) error {
				select {
				case f := <-faults:
					close(f.hit)
					select {
					case <-f.release:
						return engine.ErrCompacted
					case <-ctx.Done():
						return ctx.Err()
					}
				default:
					return nil
				}
			}}
			replica := newReplicaEnv(t, leader, true, st)
			waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
			follower := replica.s.dbs["d"].loop.State().Follower
			armed := make([]fault, count)
			for i := range armed {
				armed[i] = fault{make(chan struct{}), make(chan struct{})}
				faults <- armed[i]
			}
			got := replica.postAsync(t, "query", "reader", `{"sql":"SELECT max(a) FROM t","timeout_ms":2000}`)
			for i, f := range armed {
				select {
				case <-f.hit:
				case a := <-got:
					t.Fatalf("pin %d was never attempted: %d %v", i+1, a.status, a.frames)
				case <-time.After(2 * time.Second):
					t.Fatalf("pin %d was never attempted", i+1)
				}
				leader.exec(t, fmt.Sprintf("INSERT INTO t VALUES (%d)", i+2))
				minTxn := leader.leader(t).Head()
				waitReplica(t, func() bool {
					ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
					defer cancel()
					_, err := follower.Pin(ctx, minTxn)
					return err == nil
				}, time.Second, "new generation was not published")
				close(f.release)
			}
			a := <-got
			if count == 1 {
				if a.status != http.StatusOK || a.done()["txn"] != json.Number(fmt.Sprint(leader.leader(t).Head())) || !reflect.DeepEqual(a.rows(), []string{`[2]`}) {
					t.Fatalf("retry did not read the new pin: %d %v", a.status, a.frames)
				}
			} else if a.status != http.StatusServiceUnavailable || a.code() != "not_serving" || a.err()["not_executed"] != true || a.err()["final"] != false || len(a.frames) != 1 {
				t.Fatalf("two compacted pins: %d %v", a.status, a.frames)
			}
		})
	}
}

// replicaResponse arms a storage fault when the first row has been sent.
type replicaResponse struct {
	*httptest.ResponseRecorder
	rowSent *atomic.Bool
}

func (w replicaResponse) Write(p []byte) (int, error) {
	if len(p) > 0 && p[0] == '[' {
		w.rowSent.Store(true)
	}
	return w.ResponseRecorder.Write(p)
}

func TestReplicaCompactedAfterRow(t *testing.T) {
	leader := newQueryEnv(t, nil)
	leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (2)")
	var rowSent atomic.Bool
	st := &replicaStore{read: func(context.Context) error {
		if rowSent.Load() {
			return engine.ErrCompacted
		}
		return nil
	}}
	replica := newReplicaEnv(t, leader, true, st)
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	req := httptest.NewRequest(http.MethodPost, "/v1/databases/d/query", strings.NewReader(q("SELECT 1 UNION ALL SELECT a FROM t")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", fmt.Sprint(req.ContentLength))
	req.Header.Set("Authorization", "Bearer "+replica.sig.token(t, "reader"))
	rec := httptest.NewRecorder()
	replica.s.ServeHTTP(replicaResponse{rec, &rowSent}, req)
	frames, err := parseFrames(rec.Body.Bytes())
	a := answer{rec.Code, frames}
	if err != nil || a.status != http.StatusOK || a.code() != "internal" || a.err()["not_executed"] != false || a.err()["final"] != false ||
		len(a.frames) != 3 || !reflect.DeepEqual(a.rows(), []string{`[1]`}) {
		t.Fatalf("compaction after first row: %d %v (%v)", a.status, a.frames, err)
	}
}

func TestReplicaReadinessAndWrites(t *testing.T) {
	leader := newQueryEnv(t, nil)
	leader.exec(t, "CREATE TABLE t (a INTEGER)")
	off := newReplicaEnv(t, leader, false, &replicaStore{})
	if off.s.ready() || off.s.dbs["d"].loop.State().Follower != nil {
		t.Error("standby with follower_reads off serves the database")
	}
	if a := off.post(t, "query", "reader", q("SELECT 1")); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
		t.Errorf("standby with follower_reads off: %d %v", a.status, a.frames)
	}
	on := newReplicaEnv(t, leader, true, &replicaStore{})
	waitReplica(t, on.s.ready, 2*time.Second, "enabled replica did not become ready")
	if off.s.ready() || off.s.dbs["d"].loop.State().Follower != nil {
		t.Error("disabled replica became ready or started polling")
	}
	if a := on.post(t, "channels", "writer", `{"mode":"write"}`); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
		t.Errorf("write open on replica: %d %v", a.status, a.frames)
	}
	if a := on.post(t, "channel", "writer", `{}`); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
		t.Errorf("channel request on replica: %d %v", a.status, a.frames)
	}
	if a := on.post(t, "query", "writer", q("INSERT INTO t VALUES (1)")); a.status != http.StatusConflict || a.code() != "needs_channel" {
		t.Errorf("write SQL on replica: %d %v", a.status, a.frames)
	}
	led := leader.s.dbs["d"]
	led.tail.Store(leader.leader(t))
	probe := &Server{dbs: map[string]*database{"follower": on.s.dbs["d"], "led": led}}
	if probe.ready() {
		t.Error("a fresh follower overrode the tail-bound veto")
	}
	on.s.stopping.Store(true)
	if on.s.ready() {
		t.Error("stopping replica reports ready")
	}
}

func TestReplicaConnectionLimit(t *testing.T) {
	leader := newQueryEnv(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	replica := newReplicaEnv(t, leader, true, &replicaStore{read: func(ctx context.Context) error {
		select {
		case entered <- struct{}{}:
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	defer close(release)
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	for range replica.s.pool.perPrincipal {
		replica.postAsync(t, "query", "reader", q("SELECT 1"))
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("read did not reach the page barrier")
		}
	}
	if a := replica.post(t, "query", "reader", `{"sql":"SELECT 1","timeout_ms":100}`); a.status != http.StatusTooManyRequests || a.code() != "overloaded" {
		t.Errorf("connection limit: %d %v", a.status, a.frames)
	}
}

func TestReplicaOfficeTransitions(t *testing.T) {
	leader := newQueryEnv(t, nil)
	replica := newReplicaEnv(t, leader, true, &replicaStore{})
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	d := replica.s.dbs["d"]
	initial := d.loop.State().Follower
	if err := leader.halt(); err != nil {
		t.Fatal(err)
	}
	l := replica.leader(t)
	if f := d.loop.State().Follower; f != nil {
		t.Error("replica still publishes a follower after promotion")
	}
	select {
	case <-initial.Done():
	default:
		t.Error("promoted replica did not stop its follower")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	waitReplica(t, replica.s.ready, 4*time.Second, "stepped-down leader did not become a fresh replica")
	if f := d.loop.State().Follower; f == nil || f == initial {
		t.Fatal("step-down did not create a new follower")
	}
}
