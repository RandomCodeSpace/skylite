package office_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/office"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// recStore wraps a store: rule, if set, runs before each create, replace and
// get and fails it with no effect when it returns an error; got, if set, runs
// after each get that succeeds, its body read in full; every lease write is
// recorded with whether stopped reported true when it was issued.
type recStore struct {
	storage.Store
	keys format.Keys

	mu      sync.Mutex
	rule    func(op, key string) error
	got     func(key string)
	stopped func() bool
	writes  []leaseWrite
}

type leaseWrite struct {
	lease     format.Lease
	afterStop bool
	at        time.Time
}

func (s *recStore) setRule(r func(op, key string) error) {
	s.mu.Lock()
	s.rule = r
	s.mu.Unlock()
}

func (s *recStore) setGot(f func(key string)) {
	s.mu.Lock()
	s.got = f
	s.mu.Unlock()
}

func (s *recStore) setStopped(f func() bool) {
	s.mu.Lock()
	s.stopped = f
	s.mu.Unlock()
}

func (s *recStore) leaseWrites() []leaseWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]leaseWrite(nil), s.writes...)
}

func (s *recStore) before(op, key string, body io.Reader) (io.Reader, error) {
	s.mu.Lock()
	rule, stopped := s.rule, s.stopped
	s.mu.Unlock()
	if rule != nil {
		if err := rule(op, key); err != nil {
			return nil, err
		}
	}
	if key != s.keys.Lease() {
		return body, nil
	}
	b, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	l, err := format.DecodeLease(b)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.writes = append(s.writes, leaseWrite{lease: l, afterStop: stopped != nil && stopped(), at: time.Now()})
	s.mu.Unlock()
	return bytes.NewReader(b), nil
}

func (s *recStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	body, err := s.before("create", key, body)
	if err != nil {
		return "", err
	}
	return s.Store.Create(ctx, key, body)
}

func (s *recStore) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	s.mu.Lock()
	rule, got := s.rule, s.got
	s.mu.Unlock()
	if rule != nil {
		if err := rule("get", key); err != nil {
			return nil, storage.Info{}, err
		}
	}
	rc, info, err := s.Store.Get(ctx, key)
	if err != nil || got == nil {
		return rc, info, err
	}
	b, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, storage.Info{}, err
	}
	got(key)
	return io.NopCloser(bytes.NewReader(b)), info, nil
}

func (s *recStore) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	body, err := s.before("replace", key, body)
	if err != nil {
		return "", err
	}
	return s.Store.Replace(ctx, key, version, body)
}

func isCommit(op, key string) bool { return op == "create" && strings.Contains(key, "/log/") }

// newDatabase creates a database in a local store at dir.
func newDatabase(t *testing.T, dir string) (*recStore, format.Keys) {
	t.Helper()
	fs, err := localfs.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	keys, err := format.NewKeys("dbs/test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Create(context.Background(), fs, keys, 4096, 64); err != nil && !errors.Is(err, engine.ErrExists) {
		t.Fatal(err)
	}
	return &recStore{Store: fs, keys: keys}, keys
}

func engineConfig() engine.Config {
	return engine.Config{WALBound: 64 << 20, TailBound: 1 << 15, MaxMemory: 64 << 20, TempBudget: 64 << 20, Cache: engine.NewCache(8 << 20)}
}

func holder(t *testing.T, pod string) string {
	t.Helper()
	h, err := office.NewHolder(pod)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// start runs a loop until the test ends; stop ends it and returns Run's
// error.
func start(t *testing.T, cfg office.Config) (l *office.Loop, stop func() error) {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	if cfg.Engine == (engine.Config{}) {
		cfg.Engine = engineConfig()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	l, err := office.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			runErr = <-done
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return l, stop
}

// waitFor waits up to d for the loop's state to satisfy ok.
func waitFor(t *testing.T, l *office.Loop, d time.Duration, what string, ok func(office.State) bool) office.State {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		s := l.State()
		if ok(s) {
			return s
		}
		select {
		case <-s.Changed:
		case <-timer.C:
			t.Fatalf("after %v: not %s (leader %v, reason %v)", d, what, s.Leader != nil, s.Reason)
		}
	}
}

func leading(s office.State) bool { return s.Leader != nil }

func readLease(t *testing.T, st storage.Store, keys format.Keys) (format.Lease, string) {
	t.Helper()
	rc, info, err := st.Get(context.Background(), keys.Lease())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	l, err := format.DecodeLease(b)
	if err != nil {
		t.Fatal(err)
	}
	return l, info.Version
}

func epoch(t *testing.T, st storage.Store, keys format.Keys) uint64 {
	t.Helper()
	rc, _, err := st.Get(context.Background(), keys.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	m, err := format.DecodeManifest(b, format.DatabaseID{})
	if err != nil {
		t.Fatal(err)
	}
	return m.Epoch
}

func commit(ctx context.Context, l *engine.Leader) (uint64, error) {
	c, err := l.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = c.Close() }()
	return c.Run(ctx, []byte("office-test-commit"), func(db *sqlite3.Conn) error {
		return db.Exec("BEGIN; CREATE TABLE IF NOT EXISTS t(x); INSERT INTO t VALUES (1); COMMIT")
	})
}

// A leader that keeps renewing is never replaced by a standby.
func TestNoFailoverWhileRenewing(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl = time.Second
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl})
	b, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: ttl})
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	var lead, standby *office.Loop
	for lead == nil {
		sa, sb := a.State(), b.State()
		switch {
		case sa.Leader != nil:
			lead, standby = a, b
		case sb.Leader != nil:
			lead, standby = b, a
		default:
			select {
			case <-sa.Changed:
			case <-sb.Changed:
			case <-timer.C:
				t.Fatal("no loop took office")
			}
		}
	}
	e := epoch(t, st, keys)
	ls, ss := lead.State(), standby.State()
	select {
	case <-ls.Changed:
		t.Fatalf("leader changed state: reason %v", lead.State().Reason)
	case <-ss.Changed:
		t.Fatal("standby changed state")
	case <-time.After(5 * ttl):
	}
	if got := epoch(t, st, keys); got != e {
		t.Fatalf("epoch moved from %d to %d", e, got)
	}
	if lead.State().Leader != ls.Leader || standby.State().Leader != nil {
		t.Fatal("leadership moved")
	}
}

// A leader whose renewal finds another holder's lease released or expired,
// by Retake's rules, takes it back and stays in office; a live one it leaves
// alone.
func TestRenewRetakesReleasedLease(t *testing.T) {
	const ttl = 3 * time.Second
	for _, tc := range []struct {
		name    string
		renewed func() int64
		retake  bool
	}{
		{"released", func() int64 { return 0 }, true},
		{"expired", func() int64 { return time.Now().Add(-2 * ttl).UnixMilli() }, true},
		{"live", func() int64 { return time.Now().UnixMilli() }, false},
		// Stamped by a clock more than a time to live ahead: expired to
		// Retake, though its LeaseHeldError expires in the future.
		{"future", func() int64 { return time.Now().Add(5 * ttl).UnixMilli() }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, keys := newDatabase(t, t.TempDir())
			h := holder(t, "a")
			a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: h, TTL: ttl, Renew: 50 * time.Millisecond})
			s := waitFor(t, a, 5*time.Second, "leading", leading)
			e := epoch(t, st, keys)

			cur, version := readLease(t, st, keys)
			other := format.Lease{Holder: "other", Epoch: cur.Epoch + 1, Renewed: tc.renewed()}
			b, err := format.EncodeLease(other)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.Store.Replace(context.Background(), keys.Lease(), version, bytes.NewReader(b)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				got, _ := readLease(t, st, keys)
				if got.Holder == h {
					if !tc.retake {
						t.Fatal("a live lease of another holder was taken back")
					}
					if got.Epoch != cur.Epoch {
						t.Fatalf("retaken at epoch %d, want %d", got.Epoch, cur.Epoch)
					}
					break
				}
				if time.Now().After(deadline) {
					if tc.retake {
						t.Fatalf("lease still %+v", got)
					}
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if now := a.State(); now.Leader != s.Leader || epoch(t, st, keys) != e {
				t.Fatalf("left office: reason %v", now.Reason)
			}
		})
	}
}

// fakeClock is the real clock moved forward by an offset the test sets.
type fakeClock struct{ off atomic.Int64 }

func (c *fakeClock) Now() time.Time          { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *fakeClock) advance(d time.Duration) { c.off.Add(int64(d)) }

// A standby waits while another holder's lease is live, and takes over
// within a renewal period of its expiry.
func TestStandbyTakesOverAfterExpiry(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl = 2 * time.Second
	// A holder that takes office and is never heard from again.
	if _, _, err := engine.Takeover(context.Background(), st, keys, engine.Candidate{Holder: "ghost", TTL: ttl, Now: time.Now}); err != nil {
		t.Fatal(err)
	}
	ghost := epoch(t, st, keys)
	var clock fakeClock
	b, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: ttl, Renew: 100 * time.Millisecond, Now: clock.Now})
	select {
	case <-b.State().Changed:
		t.Fatal("took office while the lease was live")
	case <-time.After(500 * time.Millisecond):
	}
	if got := epoch(t, st, keys); got != ghost {
		t.Fatalf("epoch moved from %d to %d under a live lease", ghost, got)
	}
	clock.advance(ttl)
	waitFor(t, b, time.Second, "leading after expiry", leading)
	if got := epoch(t, st, keys); got != ghost+1 {
		t.Fatalf("epoch %d, want %d", got, ghost+1)
	}
}

// A leader deposed by another candidacy leaves office at its next renewal,
// without releasing the lease, even while its lease writes fail.
func TestStepDownWhenDeposed(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl = time.Second
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl, Renew: 50 * time.Millisecond})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, st, keys)
	if s.Office.Epoch != e || s.Office.DB == (format.DatabaseID{}) {
		t.Fatalf("state office %+v, want epoch %d", s.Office, e)
	}

	// The usurper's clock is a time to live ahead, so a's lease reads as
	// expired to it, and its own reads as live to a for two.
	usurper := engine.Candidate{Holder: "usurper", TTL: ttl, Now: func() time.Time { return time.Now().Add(ttl) }}
	st.setRule(func(op, key string) error {
		if op == "replace" && key == keys.Lease() {
			return errors.New("injected lease write failure")
		}
		return nil
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, err := engine.Takeover(context.Background(), st.Store, keys, usurper)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("takeover: %v", err)
		}
	}
	down := waitFor(t, a, time.Second, "deposed", func(s office.State) bool { return s.Leader == nil })
	if down.Office != (engine.Office{}) {
		t.Errorf("state office %+v without a leader", down.Office)
	}
	if !errors.Is(down.Reason, office.ErrDeposed) {
		t.Fatalf("reason %v, want %v", down.Reason, office.ErrDeposed)
	}
	if !errors.Is(s.Leader.Err(), engine.ErrStopped) {
		t.Fatalf("old leader not closed: %v", s.Leader.Err())
	}
	if l, _ := readLease(t, st, keys); l.Holder != "usurper" || l.Epoch != e+1 || l.Renewed == 0 {
		t.Fatalf("lease %+v after stepping down", l)
	}
}

// A leader that fail-stops writes nothing more, its lease included, before
// a new candidacy, which then takes office at a new epoch. It leaves office
// whether or not the manifest can be read.
func TestFailStopRecandidacy(t *testing.T) {
	for _, readable := range []bool{true, false} {
		t.Run(fmt.Sprintf("manifest readable %v", readable), func(t *testing.T) {
			st, keys := newDatabase(t, t.TempDir())
			a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 3 * time.Second, Renew: 50 * time.Millisecond})
			s := waitFor(t, a, 5*time.Second, "leading", leading)
			e := epoch(t, st, keys)

			st.setStopped(func() bool { return s.Leader.Err() != nil })
			st.setRule(func(op, key string) error {
				switch {
				case isCommit(op, key):
					return fmt.Errorf("injected: %w", storage.ErrOutcomeUnknown)
				case !readable && op == "get" && key == keys.Manifest():
					return errors.New("injected read failure")
				}
				return nil
			})
			_, err := commit(context.Background(), s.Leader)
			if !errors.Is(err, engine.ErrUnknown) {
				t.Fatalf("commit: %v, want %v", err, engine.ErrUnknown)
			}
			down := waitFor(t, a, 5*time.Second, "stopped", func(s office.State) bool { return s.Leader == nil })
			if !errors.Is(down.Reason, engine.ErrStopped) {
				t.Fatalf("reason %v, want %v", down.Reason, engine.ErrStopped)
			}
			st.setRule(nil)
			waitFor(t, a, 5*time.Second, "leading again", leading)
			if got := epoch(t, st, keys); got <= e {
				t.Fatalf("epoch %d, want above %d", got, e)
			}
			after := 0
			for _, w := range st.leaseWrites() {
				if !w.afterStop {
					continue
				}
				after++
				if w.lease.Epoch <= e {
					t.Fatalf("lease write %+v after the leader at epoch %d stopped", w.lease, e)
				}
			}
			if after == 0 {
				t.Fatal("no lease write by the new candidacy")
			}
		})
	}
}

// On shutdown the loop drains running work before it closes the leader, so
// a commit in its upload completes, and then releases the lease.
func TestShutdownDrainsThenReleases(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	h := holder(t, "a")
	reached, release := make(chan struct{}), make(chan struct{})
	var once, released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	ran := make(chan error, 1)
	var txn uint64
	drained := make(chan struct{})
	a, stop := start(t, office.Config{
		Store: st, Keys: keys, Holder: h, TTL: 3 * time.Second, Renew: 50 * time.Millisecond,
		Drain: func(ctx context.Context) {
			close(drained)
			select {
			case err := <-ran:
				ran <- err
			case <-ctx.Done():
			}
		},
	})
	t.Cleanup(unblock) // runs before start's cleanup, so a failure cannot hang it
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, st, keys)
	st.setRule(func(op, key string) error {
		if isCommit(op, key) {
			once.Do(func() { close(reached); <-release })
		}
		return nil
	})
	go func() {
		var err error
		txn, err = commit(context.Background(), s.Leader)
		ran <- err
	}()
	select {
	case <-reached:
	case err := <-ran:
		t.Fatalf("commit ended before its upload: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the commit did not reach its upload")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	down := waitFor(t, a, time.Second, "shutting down", func(s office.State) bool { return s.Leader == nil })
	if !errors.Is(down.Reason, office.ErrShutdown) {
		t.Fatalf("reason %v, want %v", down.Reason, office.ErrShutdown)
	}
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not run")
	}
	time.Sleep(200 * time.Millisecond) // the loop must still be draining
	if err := s.Leader.Err(); err != nil {
		t.Fatalf("leader closed while a commit was in its upload: %v", err)
	}
	unblock()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
	if err := <-ran; err != nil || txn == 0 {
		t.Fatalf("commit: txn %d, %v", txn, err)
	}
	if !errors.Is(s.Leader.Err(), engine.ErrStopped) {
		t.Fatal("leader not closed")
	}
	if l, _ := readLease(t, st, keys); l.Holder != h || l.Epoch != e || l.Renewed != 0 {
		t.Fatalf("lease %+v, want released by %s at epoch %d", l, h, e)
	}
}

const childEnv = "SKYLITE_OFFICE_CHILD"

// A process holding office is killed with SIGKILL; a standby in another
// process takes over once its lease expires.
func TestFailoverOnSIGKILL(t *testing.T) {
	if dir := os.Getenv(childEnv); dir != "" {
		runChild(t, dir)
		return
	}
	// The child's directories are under this test's, which removes them: the
	// killed child never cleans up after itself.
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "store"), 0o700); err != nil {
		t.Fatal(err)
	}
	st, keys := newDatabase(t, filepath.Join(dir, "store"))
	const ttl, renew = time.Second, 100 * time.Millisecond

	cmd := exec.Command(os.Args[0], "-test.run=^TestFailoverOnSIGKILL$", "-test.count=1") //nolint:gosec // the test binary itself
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	lines := bufio.NewScanner(out)
	for lines.Scan() && lines.Text() != "leading" {
		t.Log("child:", lines.Text())
	}
	if lines.Text() != "leading" {
		t.Fatalf("the child did not take office: %v", lines.Err())
	}
	child := epoch(t, st, keys)

	b, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: ttl, Renew: renew})
	select {
	case <-b.State().Changed:
		t.Fatal("took office from a live process")
	case <-time.After(3 * ttl):
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	killed := time.Now()
	waitFor(t, b, ttl+5*renew+time.Second, "leading after the kill", leading)
	if got := epoch(t, st, keys); got != child+1 {
		t.Fatalf("epoch %d, want %d", got, child+1)
	}
	t.Logf("took over %v after the kill", time.Since(killed).Round(time.Millisecond))
}

// runChild leads the database under dir until killed.
func runChild(t *testing.T, dir string) {
	fs, err := localfs.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := format.NewKeys("dbs/test")
	if err != nil {
		t.Fatal(err)
	}
	l, err := office.New(office.Config{
		Store: fs, Keys: keys, Holder: holder(t, "child"), Dir: filepath.Join(dir, "child"), Engine: engineConfig(),
		TTL: time.Second, Renew: 100 * time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = l.Run(context.Background()) }()
	waitFor(t, l, 10*time.Second, "leading", leading)
	fmt.Println("leading")
	select {}
}

// slowLog delays every ranged read of a log object by d, as a remote store
// would.
type slowLog struct {
	*recStore
	d atomic.Int64
}

func (s *slowLog) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if strings.Contains(key, "/log/") {
		time.Sleep(time.Duration(s.d.Load()))
	}
	return s.recStore.GetRange(ctx, key, off, n)
}

// A leader whose Lead walks a log tail for longer than a time to live keeps
// its lease meanwhile, so a standby does not depose it before it serves.
func TestSlowLeadKeepsLease(t *testing.T) {
	rec, keys := newDatabase(t, t.TempDir())
	st := &slowLog{recStore: rec}
	// A tail of epoch bumps, which Lead walks.
	for range 5 {
		_, lease, err := engine.Takeover(context.Background(), st, keys, engine.Candidate{Holder: "prior", TTL: time.Second, Now: time.Now})
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	const ttl = time.Second
	st.d.Store(int64(150 * time.Millisecond))
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl})
	b, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: ttl})
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	var lead, standby *office.Loop
	for lead == nil {
		sa, sb := a.State(), b.State()
		switch {
		case sa.Leader != nil:
			lead, standby = a, b
		case sb.Leader != nil:
			lead, standby = b, a
		default:
			select {
			case <-sa.Changed:
			case <-sb.Changed:
			case <-timer.C:
				t.Fatalf("no loop took office: reasons %v, %v", sa.Reason, sb.Reason)
			}
		}
	}
	e := epoch(t, st, keys)
	ls, ss := lead.State(), standby.State()
	select {
	case <-ls.Changed:
		t.Fatalf("leader changed state: reason %v", lead.State().Reason)
	case <-ss.Changed:
		t.Fatalf("standby changed state: reason %v", standby.State().Reason)
	case <-time.After(2 * ttl):
	}
	if got := epoch(t, st, keys); got != e {
		t.Fatalf("epoch moved from %d to %d", e, got)
	}
}

// hangStore makes every manifest read wait for its context to end once hang
// is set, as a partition from the store would.
type hangStore struct {
	*recStore
	hang atomic.Bool
}

func (s *hangStore) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if s.hang.Load() && key == s.keys.Manifest() {
		<-ctx.Done()
		return nil, storage.Info{}, ctx.Err()
	}
	return s.recStore.Get(ctx, key)
}

// A leader that cannot read the manifest for a time to live, its reads
// hanging, leaves office without a lease write, while a standby that can
// read it takes office.
func TestUncheckedLeaderStepsDown(t *testing.T) {
	rec, keys := newDatabase(t, t.TempDir())
	st := &hangStore{recStore: rec}
	const ttl = time.Second
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl, Renew: 100 * time.Millisecond})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	rec.setStopped(st.hang.Load)
	st.hang.Store(true)
	other := &recStore{Store: rec.Store, keys: keys}
	b, _ := start(t, office.Config{Store: other, Keys: keys, Holder: holder(t, "b"), TTL: ttl, Renew: 100 * time.Millisecond})
	down := waitFor(t, a, ttl+time.Second, "stepped down", func(s office.State) bool { return s.Leader == nil })
	if !errors.Is(down.Reason, office.ErrUnchecked) {
		t.Fatalf("reason %v, want %v", down.Reason, office.ErrUnchecked)
	}
	if !errors.Is(s.Leader.Err(), engine.ErrStopped) {
		t.Fatalf("old leader not closed: %v", s.Leader.Err())
	}
	waitFor(t, b, 5*time.Second, "standby leading", leading)
	for _, w := range st.leaseWrites() {
		if w.afterStop && w.lease.Renewed == 0 {
			t.Fatalf("released %+v while unable to read the manifest", w.lease)
		}
	}
}

func TestFollowerStopsBeforeLead(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	ctx := t.Context()
	_, primary, err := engine.Takeover(ctx, st, keys, engine.Candidate{Holder: holder(t, "primary"), TTL: 5 * time.Second, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	started, resume := make(chan struct{}), make(chan struct{})
	office.SetLead(t, func(ctx context.Context, _ storage.Store, _ format.Keys, _ string, _ engine.Office, _ engine.Config) (*engine.Leader, error) {
		select {
		case started <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-resume:
			return nil, errors.New("injected Lead failure")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	l, stop := start(t, office.Config{
		Store: st, Keys: keys, Holder: holder(t, "standby"), TTL: 5 * time.Second, Renew: 200 * time.Millisecond,
		FollowerReads: true,
	})
	initial := waitFor(t, l, 2*time.Second, "follower published", func(s office.State) bool { return s.Follower != nil }).Follower
	if err := primary.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("standby did not start Lead")
	}
	if l.State().Follower != nil {
		t.Error("Lead started while the standby still published a follower")
	}
	select {
	case <-initial.Done():
	default:
		t.Error("Lead started before the follower stopped")
	}
	close(resume)
	next := waitFor(t, l, 2*time.Second, "follower restarted", func(s office.State) bool {
		return s.Follower != nil && s.Follower != initial
	}).Follower
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if l.State().Follower != nil {
		t.Error("shutdown left a follower published")
	}
	select {
	case <-next.Done():
	default:
		t.Error("shutdown did not wait for the follower")
	}
}

// A Lead that fails releases the lease and shows in State, and candidacies
// then back off, doubling.
func TestLeadFailureBacksOff(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	var mu sync.Mutex
	var swaps []time.Time
	st.setRule(func(op, key string) error {
		if op == "replace" && key == keys.Manifest() {
			mu.Lock()
			swaps = append(swaps, time.Now())
			mu.Unlock()
		}
		return nil
	})
	e := epoch(t, st, keys)
	// The renewal period lets the backoff double from 1 s to 2 s.
	a, _ := start(t, office.Config{
		Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 6 * time.Second, Renew: 2 * time.Second,
		Engine: engine.Config{WALBound: -1},
	})
	s := waitFor(t, a, 5*time.Second, "failed", func(s office.State) bool { return s.Reason != nil })
	if s.Leader != nil || !strings.Contains(s.Reason.Error(), "lead at epoch") {
		t.Fatalf("state after a failed Lead: leader %v, reason %v", s.Leader != nil, s.Reason)
	}
	deadline := time.Now().Add(6 * time.Second)
	for {
		mu.Lock()
		n := len(swaps)
		mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d candidacies in 6 s", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	g1, g2 := swaps[1].Sub(swaps[0]), swaps[2].Sub(swaps[1])
	mu.Unlock()
	if g1 < 900*time.Millisecond || g2 < 1800*time.Millisecond {
		t.Fatalf("candidacies %v and %v apart, want about 1 s and 2 s", g1, g2)
	}
	released := false
	for _, w := range st.leaseWrites() {
		if w.lease.Epoch == e+1 && w.lease.Renewed == 0 {
			released = true
		}
	}
	if !released {
		t.Fatalf("lease at epoch %d not released after the failed Lead", e+1)
	}
}

// A manifest read and a lease renewal that fail for a while depose nobody.
func TestTransientErrorsKeepOffice(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 3 * time.Second, Renew: 50 * time.Millisecond})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, st, keys)
	var failed atomic.Int64
	for _, fail := range []func(op, key string) bool{
		func(op, key string) bool { return op == "get" && key == keys.Manifest() },
		func(op, key string) bool { return op == "replace" && key == keys.Lease() },
	} {
		st.setRule(func(op, key string) error {
			if fail(op, key) {
				failed.Add(1)
				return errors.New("injected transient failure")
			}
			return nil
		})
		time.Sleep(300 * time.Millisecond)
	}
	st.setRule(nil)
	before, _ := readLease(t, st, keys)
	time.Sleep(200 * time.Millisecond)
	after, _ := readLease(t, st, keys)
	if failed.Load() == 0 || after.Renewed <= before.Renewed {
		t.Fatalf("%d failures injected; lease %+v then %+v", failed.Load(), before, after)
	}
	if now := a.State(); now.Leader != s.Leader || epoch(t, st, keys) != e {
		t.Fatalf("left office: reason %v", now.Reason)
	}
}

// DrainTimeout bounds a Drain that does not return, and a standby shuts down
// at once.
func TestDrainTimeout(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	h := holder(t, "a")
	quit := make(chan struct{}) // lets a failing test's cleanup end the drain
	a, stop := start(t, office.Config{
		Store: st, Keys: keys, Holder: h, TTL: 3 * time.Second, Renew: 50 * time.Millisecond,
		DrainTimeout: 300 * time.Millisecond, Drain: func(ctx context.Context) {
			select {
			case <-ctx.Done():
			case <-quit:
			}
		},
	})
	t.Cleanup(func() { close(quit) })
	waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, st, keys)
	_, stopB := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: 3 * time.Second, Renew: 50 * time.Millisecond})
	time.Sleep(200 * time.Millisecond)
	t0 := time.Now()
	if err := stopB(); err != nil || time.Since(t0) > time.Second {
		t.Fatalf("standby stop: %v after %v", err, time.Since(t0))
	}
	t0 = time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return: the drain is unbounded")
	}
	if d := time.Since(t0); d < 300*time.Millisecond || d > 2*time.Second {
		t.Fatalf("drain of %v, want about 300ms", d)
	}
	if l, _ := readLease(t, st, keys); l.Holder != h || l.Epoch != e || l.Renewed != 0 {
		t.Fatalf("lease %+v, want released by %s at epoch %d", l, h, e)
	}
}

// A shutdown after the leader stopped, before the loop noticed, writes no
// lease.
func TestShutdownAfterStopWritesNothing(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	// Renewals 3 s apart: the stop is not noticed before the shutdown.
	a, stop := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 9 * time.Second})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	st.setStopped(func() bool { return s.Leader.Err() != nil })
	st.setRule(func(op, key string) error {
		if isCommit(op, key) {
			return fmt.Errorf("injected: %w", storage.ErrOutcomeUnknown)
		}
		return nil
	})
	if _, err := commit(context.Background(), s.Leader); !errors.Is(err, engine.ErrUnknown) {
		t.Fatalf("commit: %v, want %v", err, engine.ErrUnknown)
	}
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, w := range st.leaseWrites() {
		if w.afterStop {
			t.Fatalf("lease write %+v after the leader stopped", w.lease)
		}
	}
}

// A candidacy that takes office as the context ends releases the lease.
func TestCancelledCandidacyReleases(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	e := epoch(t, st, keys)
	h := holder(t, "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var bumped atomic.Bool
	st.setRule(func(op, key string) error {
		if isCommit(op, key) {
			bumped.Store(true)
		}
		return nil
	})
	// The candidacy's last manifest read, after its bump, ends the context.
	st.setGot(func(key string) {
		if key == keys.Manifest() && bumped.Load() {
			cancel()
		}
	})
	l, err := office.New(office.Config{
		Store: st, Keys: keys, Holder: h, Dir: t.TempDir(), Engine: engineConfig(),
		TTL: 3 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
	if l.State().Leader != nil || epoch(t, st, keys) != e+1 {
		t.Fatalf("leader %v at epoch %d, want none at %d", l.State().Leader != nil, epoch(t, st, keys), e+1)
	}
	if got, _ := readLease(t, st, keys); got.Holder != h || got.Epoch != e+1 || got.Renewed != 0 {
		t.Fatalf("lease %+v, want released by %s at epoch %d", got, h, e+1)
	}
}

// A leader that fail-stops leaves State within about a second however long
// the renewal period, and the next candidacy waits the first backoff, the
// backoff having started again when the loop took office.
func TestFailStopNoticedWithinASecond(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	// The first candidacy fails, which doubles the backoff to 2 s.
	var failed atomic.Bool
	st.setRule(func(op, key string) error {
		if op == "get" && key == keys.Manifest() && failed.CompareAndSwap(false, true) {
			return errors.New("injected")
		}
		return nil
	})
	t0 := time.Now()
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 9 * time.Second})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	if d := time.Since(t0); !failed.Load() || d < 900*time.Millisecond {
		t.Fatalf("took office %v after starting, failed candidacy %v; want a failed candidacy and a backoff", d, failed.Load())
	}
	st.setRule(func(op, key string) error {
		if isCommit(op, key) {
			return fmt.Errorf("injected: %w", storage.ErrOutcomeUnknown)
		}
		return nil
	})
	if _, err := commit(context.Background(), s.Leader); !errors.Is(err, engine.ErrUnknown) {
		t.Fatalf("commit: %v, want %v", err, engine.ErrUnknown)
	}
	st.setRule(nil)
	down := waitFor(t, a, 1500*time.Millisecond, "stopped", func(s office.State) bool { return s.Leader == nil })
	if !errors.Is(down.Reason, engine.ErrStopped) {
		t.Fatalf("reason %v, want %v", down.Reason, engine.ErrStopped)
	}
	left := time.Now()
	waitFor(t, a, 5*time.Second, "leading again", leading)
	if d := time.Since(left); d < 700*time.Millisecond || d > 1700*time.Millisecond {
		t.Fatalf("took office again %v after leaving it, want a backoff of 1 s", d)
	}
}

// A standby reads only the lease while it is live, and takes office within
// 5 s of its release however long the renewal period.
func TestStandbyPollsTheLease(t *testing.T) {
	t.Run("reads", func(t *testing.T) {
		st, keys := newDatabase(t, t.TempDir())
		a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 3 * time.Second, Renew: 100 * time.Millisecond})
		waitFor(t, a, 5*time.Second, "leading", leading)
		other := &recStore{Store: st.Store, keys: keys}
		var manifests, leases atomic.Int64
		other.setRule(func(op, key string) error {
			switch {
			case op == "get" && key == keys.Manifest():
				manifests.Add(1)
			case op == "get" && key == keys.Lease():
				leases.Add(1)
			}
			return nil
		})
		start(t, office.Config{Store: other, Keys: keys, Holder: holder(t, "b"), TTL: 3 * time.Second, Renew: 100 * time.Millisecond})
		time.Sleep(700 * time.Millisecond)
		if m, l := manifests.Load(), leases.Load(); m != 1 || l < 4 {
			t.Fatalf("the standby read the manifest %d times and the lease %d", m, l)
		}
	})
	t.Run("release", func(t *testing.T) {
		st, keys := newDatabase(t, t.TempDir())
		a, stop := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 30 * time.Second})
		waitFor(t, a, 5*time.Second, "leading", leading)
		b, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: 30 * time.Second})
		time.Sleep(100 * time.Millisecond)
		if err := stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		waitFor(t, b, 5*time.Second+time.Second, "leading after the release", leading)
	})
}

func TestNewHolder(t *testing.T) {
	long := strings.Repeat("p", 253) // Kubernetes' longest pod name
	h1, h2 := holder(t, long), holder(t, long)
	if len(h1) != format.MaxHolderLen || h1 == h2 || !strings.HasPrefix(h1, long[:111]+"_") {
		t.Fatalf("holders %q and %q for a pod of 253 characters", h1, h2)
	}
	for _, pod := range []string{"", "a/b"} {
		if _, err := office.NewHolder(pod); err == nil {
			t.Fatalf("NewHolder(%q) succeeded", pod)
		}
	}
}

func TestNewChecksConfig(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	for _, tc := range []struct {
		ttl, renew, drain time.Duration
		ok                bool
	}{
		{time.Second, 333 * time.Millisecond, 0, true},
		{time.Second, 334 * time.Millisecond, 0, false},
		{time.Second, 0, -time.Second, false},
		{-time.Second, time.Millisecond, 0, false},
	} {
		_, err := office.New(office.Config{Store: st, Keys: keys, Holder: "h", Dir: t.TempDir(), TTL: tc.ttl, Renew: tc.renew, DrainTimeout: tc.drain})
		if (err == nil) != tc.ok {
			t.Errorf("TTL %v, renewal %v, drain timeout %v: %v", tc.ttl, tc.renew, tc.drain, err)
		}
	}
}

// hangN makes the next n reads of key wait for their context to end.
type hangN struct {
	*recStore
	key string
	n   atomic.Int64
}

func (s *hangN) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if key == s.key && s.n.Add(-1) >= 0 {
		<-ctx.Done()
		return nil, storage.Info{}, ctx.Err()
	}
	return s.recStore.Get(ctx, key)
}

// A leader at the longest renewal period whose manifest read hangs once
// keeps office: its lease outlives the hung renewal.
func TestOneHungRenewalKeepsOffice(t *testing.T) {
	rec, keys := newDatabase(t, t.TempDir())
	st := &hangN{recStore: rec, key: keys.Manifest()}
	const ttl = 1500 * time.Millisecond
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl, Renew: ttl / 3})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, rec, keys)
	other := &recStore{Store: rec.Store, keys: keys}
	b, _ := start(t, office.Config{Store: other, Keys: keys, Holder: holder(t, "b"), TTL: ttl, Renew: ttl / 3})
	st.n.Store(1)
	select {
	case <-s.Changed:
		t.Fatalf("left office after one hung renewal: reason %v", a.State().Reason)
	case <-time.After(2 * ttl):
	}
	if st.n.Load() > 0 {
		t.Fatal("no renewal hung")
	}
	if got := epoch(t, rec, keys); got != e || b.State().Leader != nil {
		t.Fatalf("epoch moved from %d to %d; standby leads: %v", e, got, b.State().Leader != nil)
	}
}

// The first renewal after Lead returns is due a renewal period after the
// last one while Lead ran, not a period after Lead returned.
func TestRenewalCadenceAcrossLead(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const renew = 500 * time.Millisecond
	office.SetLead(t, func(ctx context.Context, s storage.Store, k format.Keys, dir string, o engine.Office, cfg engine.Config) (*engine.Leader, error) {
		n := len(st.leaseWrites())
		l, err := engine.Lead(ctx, s, k, dir, o, cfg)
		// Return just before the next tick: after a renewal and most of a
		// period.
		for err == nil && ctx.Err() == nil && len(st.leaseWrites()) == n {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(renew * 9 / 10)
		return l, err
	})
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 3 * renew, Renew: renew})
	waitFor(t, a, 5*time.Second, "leading", leading)
	time.Sleep(2 * renew)
	w := st.leaseWrites()
	var gap time.Duration
	for i := 1; i < len(w); i++ {
		gap = max(gap, w[i].at.Sub(w[i-1].at))
	}
	if len(w) < 4 || gap > renew*3/2 {
		t.Fatalf("%d lease writes, at most %v apart; want renewals a period of %v apart", len(w), gap, renew)
	}
}

// A leader deposed while Lead runs cancels Lead, closes the leader Lead
// returns, and leaves office without another lease write.
func TestDeposedDuringLead(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	var cancelled atomic.Bool
	led := make(chan *engine.Leader, 1)
	office.SetLead(t, func(ctx context.Context, s storage.Store, k format.Keys, dir string, o engine.Office, cfg engine.Config) (*engine.Leader, error) {
		l, err := engine.Lead(ctx, s, k, dir, o, cfg)
		if err != nil {
			return nil, err
		}
		select {
		case led <- l:
		default:
		}
		// Return the leader once Lead is cancelled, as a Lead that finished
		// as the office was lost would.
		select {
		case <-ctx.Done():
			cancelled.Store(true)
		case <-time.After(5 * time.Second):
		}
		return l, nil
	})
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 3 * time.Second, Renew: 100 * time.Millisecond})
	var leader *engine.Leader
	select {
	case leader = <-led:
	case <-time.After(5 * time.Second):
		t.Fatal("Lead did not run")
	}
	n := len(st.leaseWrites())
	// A candidate whose time to live of 1 ms finds the live lease expired,
	// and which stamps it by the true clock.
	if _, _, err := engine.Takeover(context.Background(), st.Store, keys, engine.Candidate{Holder: "usurper", TTL: time.Millisecond, Now: time.Now}); err != nil {
		t.Fatalf("usurper: %v", err)
	}
	s := waitFor(t, a, 10*time.Second, "out of office", func(s office.State) bool { return s.Reason != nil })
	if s.Leader != nil || !errors.Is(s.Reason, office.ErrDeposed) {
		t.Fatalf("leader %v, reason %v; want none, deposed", s.Leader != nil, s.Reason)
	}
	if !cancelled.Load() {
		t.Error("Lead's context was not cancelled")
	}
	if !errors.Is(leader.Err(), engine.ErrStopped) {
		t.Errorf("the leader Lead returned is not closed: %v", leader.Err())
	}
	left := len(st.leaseWrites())
	time.Sleep(500 * time.Millisecond)
	w := st.leaseWrites()
	for _, w := range w[n:] {
		if w.lease.Renewed == 0 {
			t.Errorf("released %+v after being deposed", w.lease)
		}
	}
	if len(w) != left || a.State().Leader != nil {
		t.Errorf("%d lease writes after leaving office; leading %v", len(w)-left, a.State().Leader != nil)
	}
}

// A manifest read that fails once, more than a time to live into office,
// deposes nobody: the time since the last good read counts.
func TestReadFailureLateInOfficeKeepsOffice(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl = 600 * time.Millisecond
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl, Renew: ttl / 3})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	time.Sleep(ttl + ttl/2)
	var failed atomic.Bool
	st.setRule(func(op, key string) error {
		if op == "get" && key == keys.Manifest() && failed.CompareAndSwap(false, true) {
			return errors.New("injected")
		}
		return nil
	})
	select {
	case <-s.Changed:
		t.Fatalf("left office: reason %v", a.State().Reason)
	case <-time.After(ttl):
	}
	if !failed.Load() {
		t.Fatal("no manifest read failed")
	}
}

// A standby takes office within a poll of finding the lease stamped more
// than a time to live ahead, which engine.Lease judges free.
func TestStandbyTakesFutureStampedLease(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl = 3 * time.Second
	if _, _, err := engine.Takeover(context.Background(), st.Store, keys, engine.Candidate{Holder: "live", TTL: ttl, Now: time.Now}); err != nil {
		t.Fatal(err)
	}
	b, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "b"), TTL: ttl, Renew: 300 * time.Millisecond})
	time.Sleep(500 * time.Millisecond)
	if b.State().Leader != nil {
		t.Fatal("took office from a live lease")
	}
	// A holder whose clock is an hour ahead takes office and is never heard
	// from again.
	far := func() time.Time { return time.Now().Add(time.Hour) }
	if _, _, err := engine.Takeover(context.Background(), st.Store, keys, engine.Candidate{Holder: "skewed", TTL: ttl, Now: far}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, b, 2*time.Second, "leading", leading)
}

// sigHang makes every manifest read wait for its context to end once armed,
// and signals each.
type sigHang struct {
	*recStore
	arm atomic.Bool
	in  chan struct{}
}

func (s *sigHang) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if s.arm.Load() && key == s.keys.Manifest() {
		select {
		case s.in <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, storage.Info{}, ctx.Err()
	}
	return s.recStore.Get(ctx, key)
}

// A leader that fail-stops while a renewal's manifest read hangs leaves
// State within about a second, not when the read times out.
func TestFailStopNoticedDuringHungRenewal(t *testing.T) {
	rec, keys := newDatabase(t, t.TempDir())
	st := &sigHang{recStore: rec, in: make(chan struct{}, 1)}
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 9 * time.Second})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	st.arm.Store(true)
	select {
	case <-st.in:
	case <-time.After(5 * time.Second):
		t.Fatal("no renewal")
	}
	rec.setRule(func(op, key string) error {
		if isCommit(op, key) {
			return fmt.Errorf("injected: %w", storage.ErrOutcomeUnknown)
		}
		return nil
	})
	if _, err := commit(context.Background(), s.Leader); !errors.Is(err, engine.ErrUnknown) {
		t.Fatalf("commit: %v, want %v", err, engine.ErrUnknown)
	}
	down := waitFor(t, a, 1500*time.Millisecond, "stopped", func(s office.State) bool { return s.Leader == nil })
	if !errors.Is(down.Reason, engine.ErrStopped) {
		t.Fatalf("reason %v, want %v", down.Reason, engine.ErrStopped)
	}
}

// A standby whose lease read hangs gives it up after a renewal period, and
// takes office once the leader releases.
func TestStandbyLeaseReadTimesOut(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl, renew = 3 * time.Second, 300 * time.Millisecond
	a, stop := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl, Renew: renew})
	waitFor(t, a, 5*time.Second, "leading", leading)
	other := &hangN{recStore: &recStore{Store: st.Store, keys: keys}, key: keys.Lease()}
	b, _ := start(t, office.Config{Store: other, Keys: keys, Holder: holder(t, "b"), TTL: ttl, Renew: renew})
	time.Sleep(renew)
	other.n.Store(1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitFor(t, b, 2*time.Second, "leading after the release", leading)
	if other.n.Load() > 0 {
		t.Fatal("no lease read hung")
	}
}
