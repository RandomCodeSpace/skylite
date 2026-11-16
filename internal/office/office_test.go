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
// get and fails it with no effect when it returns an error; every lease write is
// recorded with whether stopped reported true when it was issued.
type recStore struct {
	storage.Store
	keys format.Keys

	mu      sync.Mutex
	rule    func(op, key string) error
	stopped func() bool
	writes  []leaseWrite
}

type leaseWrite struct {
	lease     format.Lease
	afterStop bool
}

func (s *recStore) setRule(r func(op, key string) error) {
	s.mu.Lock()
	s.rule = r
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
	s.writes = append(s.writes, leaseWrite{lease: l, afterStop: stopped != nil && stopped()})
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
	rule := s.rule
	s.mu.Unlock()
	if rule != nil {
		if err := rule("get", key); err != nil {
			return nil, storage.Info{}, err
		}
	}
	return s.Store.Get(ctx, key)
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
	cfg.Engine = engineConfig()
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
	c, err := l.Open()
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

// A leader whose renewal finds another holder's lease released or expired
// takes it back and stays in office; a live one it leaves alone.
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
// without releasing the lease.
func TestStepDownWhenDeposed(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	const ttl = time.Second
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: ttl, Renew: 50 * time.Millisecond})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, st, keys)

	// The usurper's clock is a time to live ahead, so a's lease reads as
	// expired to it, and its own reads as live to a for two.
	usurper := engine.Candidate{Holder: "usurper", TTL: ttl, Now: func() time.Time { return time.Now().Add(ttl) }}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, err := engine.Takeover(context.Background(), st, keys, usurper)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("takeover: %v", err)
		}
	}
	down := waitFor(t, a, time.Second, "deposed", func(s office.State) bool { return s.Leader == nil })
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
// even while the manifest cannot be read.
func TestFailStopRecandidacy(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	a, _ := start(t, office.Config{Store: st, Keys: keys, Holder: holder(t, "a"), TTL: 3 * time.Second, Renew: 50 * time.Millisecond})
	s := waitFor(t, a, 5*time.Second, "leading", leading)
	e := epoch(t, st, keys)

	st.setStopped(func() bool { return s.Leader.Err() != nil })
	st.setRule(func(op, key string) error {
		switch {
		case isCommit(op, key):
			return fmt.Errorf("injected: %w", storage.ErrOutcomeUnknown)
		case op == "get" && key == keys.Manifest():
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
}

// On shutdown the loop drains running work before it closes the leader, so
// a commit in its upload completes, and then releases the lease.
func TestShutdownDrainsThenReleases(t *testing.T) {
	st, keys := newDatabase(t, t.TempDir())
	h := holder(t, "a")
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
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
	}

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	down := waitFor(t, a, time.Second, "shutting down", func(s office.State) bool { return s.Leader == nil })
	if !errors.Is(down.Reason, office.ErrShutdown) {
		t.Fatalf("reason %v, want %v", down.Reason, office.ErrShutdown)
	}
	<-drained
	time.Sleep(200 * time.Millisecond) // the loop must still be draining
	if err := s.Leader.Err(); err != nil {
		t.Fatalf("leader closed while a commit was in its upload: %v", err)
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Run: %v", err)
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
	dir := t.TempDir()
	st, keys := newDatabase(t, dir)
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

// runChild leads the database at dir until killed.
func runChild(t *testing.T, dir string) {
	fs, err := localfs.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := format.NewKeys("dbs/test")
	if err != nil {
		t.Fatal(err)
	}
	l, err := office.New(office.Config{
		Store: fs, Keys: keys, Holder: holder(t, "child"), Dir: t.TempDir(), Engine: engineConfig(),
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
