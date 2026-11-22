package server

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/ncruces/go-sqlite3/vfs"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

type replicaStore struct {
	storage.Store
	pause   atomic.Pointer[pollPause]
	read    func(context.Context) error // optional fault on a page read
	missing atomic.Bool                 // the manifest's Head answers not found
}

type pollPause struct {
	entered, release chan struct{}
	once             sync.Once
}

func (s *replicaStore) Head(ctx context.Context, key string) (storage.Info, error) {
	if s.missing.Load() && strings.HasSuffix(key, "/manifest") {
		return storage.Info{}, storage.ErrNotFound
	}
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

func TestReplicaFailureAfterRow(t *testing.T) {
	leader := newQueryEnv(t, nil)
	leader.exec(t, "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (2)")
	for _, tc := range []struct {
		name  string
		fault func(context.Context) error
		code  string
		class string
	}{
		{"compacted", func(context.Context) error { return engine.ErrCompacted }, "internal", "compacted"},
		{"storage", func(context.Context) error { return errors.New("injected read failure") }, "internal", "other"},
		{"broken", func(context.Context) error { panic("injected engine panic") }, "internal", "broken"},
		{"timeout", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, "timeout", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rowSent atomic.Bool
			st := &replicaStore{read: func(ctx context.Context) error {
				if rowSent.CompareAndSwap(true, false) {
					return tc.fault(ctx)
				}
				return nil
			}}
			replica := newReplicaEnv(t, leader, true, st)
			waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
			body := `{"sql":"SELECT 1 UNION ALL SELECT a FROM t","timeout_ms":200}`
			req := httptest.NewRequest(http.MethodPost, "/v1/databases/d/query", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Length", fmt.Sprint(req.ContentLength))
			req.Header.Set("Authorization", "Bearer "+replica.sig.token(t, "reader"))
			rec := httptest.NewRecorder()
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("handler panicked before writing the terminal frame: %v", p)
				}
			}()
			replica.s.ServeHTTP(replicaResponse{rec, &rowSent}, req)
			frames, err := parseFrames(rec.Body.Bytes())
			a := answer{rec.Code, frames}
			if err != nil || a.status != http.StatusOK || a.code() != tc.code || a.err()["not_executed"] != false || a.err()["final"] != false ||
				len(a.frames) != 3 || !reflect.DeepEqual(a.rows(), []string{`[1]`}) {
				t.Errorf("failure after first row: %d %v (%v)", a.status, a.frames, err)
			}
			if tc.class != "" && !strings.Contains(replica.logs.String(),
				`"msg":"statement failed","request_id":"`+rec.Header().Get("Skylite-Request-Id")+`","class":"`+tc.class+`"`) {
				t.Errorf("no internal error log with class %q", tc.class)
			}
			if next := replica.post(t, "query", "reader", q("SELECT 1")); next.done() == nil {
				t.Errorf("read after failure: %d %v", next.status, next.frames)
			}
		})
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

// A follower connection that broke, by a stack overflow or by exhausting
// its memory, is released: the answer ends with its terminal frame, its VFS
// is unregistered, and the next read works.
func TestReplicaBrokenConnection(t *testing.T) {
	leader := newQueryEnv(t, nil)
	leader.exec(t, overflowViews())
	replica := newReplicaEnv(t, leader, true, &replicaStore{})
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	for _, tc := range []struct{ name, sql string }{
		{"stack overflow", "SELECT * FROM v4"},
		// More than a follower connection's 64 MiB ceiling.
		{"out of memory", "SELECT count(*) FROM (SELECT randomblob(1000000) AS b FROM generate_series(1, 80) ORDER BY b)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := replica.post(t, "query", "reader", q(tc.sql))
			if a.status != http.StatusOK && a.status != http.StatusInternalServerError || a.code() != "internal" {
				t.Fatalf("%d %v, want internal", a.status, a.frames)
			}
			if !strings.Contains(replica.logs.String(), fmt.Sprintf(`"request_id":"%v","class":"broken"`, a.err()["request_id"])) {
				t.Errorf("the follower did not report the connection broken: %s", replica.logs.String())
			}
			for i := range 10000 {
				if vfs.Find(fmt.Sprint("skylite-follow-", i)) != nil {
					t.Errorf("skylite-follow-%d is still registered", i)
				}
			}
			if a := replica.post(t, "query", "reader", q("SELECT 1")); !reflect.DeepEqual(a.rows(), []string{`[1]`}) {
				t.Errorf("the next query: %d %v", a.status, a.frames)
			}
		})
	}
}

// A follower whose manifest went missing refuses reads as internal and
// logs them.
func TestReplicaInvalidFollower(t *testing.T) {
	leader := newQueryEnv(t, nil)
	st := &replicaStore{}
	replica := newReplicaEnv(t, leader, true, st)
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	f := replica.s.dbs["d"].loop.State().Follower
	st.missing.Store(true)
	waitReplica(t, func() bool { return !f.Fresh() }, 2*time.Second, "the follower did not notice the missing manifest")
	a := replica.post(t, "query", "reader", q("SELECT 1"))
	if a.status != http.StatusInternalServerError || a.code() != "internal" || a.err()["not_executed"] != true {
		t.Errorf("read on an invalid follower: %d %v", a.status, a.frames)
	}
	if !strings.Contains(replica.logs.String(), fmt.Sprintf(`"request_id":"%v","class":"other"`, a.err()["request_id"])) {
		t.Errorf("no internal error log: %s", replica.logs.String())
	}
}

type pinDeadlineContext struct {
	context.Context
	atDeadline func()
}

func (c pinDeadlineContext) Deadline() (time.Time, bool) {
	c.atDeadline()
	return c.Context.Deadline()
}

func TestReplicaCatchUpDoesNotReserveConnection(t *testing.T) {
	leader := newQueryEnv(t, nil)
	replica := newReplicaEnv(t, leader, true, &replicaStore{})
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	replica.s.pool.mu.Lock()
	replica.s.pool.capacity = 1
	replica.s.pool.mu.Unlock()
	f := replica.s.dbs["d"].loop.State().Follower
	ctx, cancel := context.WithCancel(t.Context())
	enter := make(chan struct{})
	resume := make(chan struct{})
	ctx = pinDeadlineContext{ctx, sync.OnceFunc(func() {
		close(enter)
		<-resume
	})}
	resumePin := sync.OnceFunc(func() {
		cancel()
		close(resume)
	})
	defer resumePin()
	answer := make(chan *wire.Error, 1)
	go func() {
		_, e := replica.s.followerStatement(ctx, nil, f, &wire.Request{MinTxn: 1 << 40}, "waiting", replica.s.log)
		answer <- e
	}()
	select {
	case <-enter:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not reach the catch-up wait")
	}
	if a := replica.post(t, "query", "reader", q("SELECT 1")); a.done() == nil {
		t.Errorf("catch-up request blocked the available slot: %d %v", a.status, a.frames)
	}
	resumePin()
	if e := <-answer; e == nil || e.Code != wire.CodeNotServing {
		t.Errorf("canceled catch-up: %v", e)
	}
}

func TestReplicaExpiredRequest(t *testing.T) {
	leader := newQueryEnv(t, nil)
	replica := newReplicaEnv(t, leader, true, &replicaStore{})
	waitReplica(t, replica.s.ready, 2*time.Second, "replica did not become ready")
	f := replica.s.dbs["d"].loop.State().Follower
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	for _, minTxn := range []int64{0, 1, 1 << 40} {
		want := wire.CodeTimeout
		if minTxn == 1<<40 {
			want = wire.CodeBehind
		}
		_, e := replica.s.followerStatement(ctx, &stream{}, f, &wire.Request{MinTxn: minTxn}, "reader", replica.s.log)
		if e == nil || e.Code != want {
			t.Errorf("expired request with min_txn=%d: %v, want %s", minTxn, e, want)
		}
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
