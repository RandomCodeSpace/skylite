package engine

// Tests for Run's contract: its classes, its context, Close on a hung store,
// the shared index, and panics outside Run.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// hangKey marks a context whose storage calls a hangStore holds.
type hangKey struct{}

// hangStore wraps a store. Each call that hang selects waits, as a hung
// backend does, until its context ends or the test releases it.
type hangStore struct {
	storage.Store
	hang    func(ctx context.Context, op string) bool
	reached chan struct{}
	release chan struct{}
	once    sync.Once
	opened  sync.Once
}

func newHangStore(st storage.Store, hang func(ctx context.Context, op string) bool) *hangStore {
	return &hangStore{Store: st, hang: hang, reached: make(chan struct{}), release: make(chan struct{})}
}

func (s *hangStore) open() { s.opened.Do(func() { close(s.release) }) }

func (s *hangStore) wait(ctx context.Context, op, key string) error {
	if !s.hang(ctx, op) {
		return nil
	}
	s.once.Do(func() { close(s.reached) })
	select {
	case <-ctx.Done():
		return &storage.Error{Op: op, Key: key, Err: ctx.Err()}
	case <-s.release:
		return nil
	}
}

func (s *hangStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if err := s.wait(ctx, "getrange", key); err != nil {
		return nil, storage.Info{}, err
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func (s *hangStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if isCommit(key) {
		if err := s.wait(ctx, "create", key); err != nil {
			return "", err
		}
	}
	return s.Store.Create(ctx, key, body)
}

// readsHang holds the storage reads of Runs whose context carries hangKey.
func readsHang(ctx context.Context, op string) bool {
	return op == "getrange" && ctx.Value(hangKey{}) != nil
}

// hungReadSetup leads a database with table t, all of it in storage after a
// TRUNCATE checkpoint and nothing cached, so that every page a new
// connection reads is a storage read.
func hungReadSetup(t *testing.T, hang func(ctx context.Context, op string) bool) (*hangStore, *Leader, *Conn) {
	t.Helper()
	st, keys, o := newDatabase(t, 1024)
	hs := newHangStore(st, hang)
	t.Cleanup(hs.open)
	cfg := testConfig()
	cfg.Cache = NewCache(0)
	l := lead(t, hs, keys, o, cfg)
	w := open(t, l)
	mustRun(t, w, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)")
	mustRun(t, w, "INSERT INTO t SELECT value, randomblob(900) FROM generate_series(1, 50)")
	checkpoint(t, w, sqlite3.CHECKPOINT_TRUNCATE)
	return hs, l, w
}

// within waits for done for a few seconds; on a timeout it releases hs, so
// that cleanup does not hang, and fails.
func within[T any](t *testing.T, hs *hangStore, done <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-done:
		return v
	case <-time.After(5 * time.Second):
		hs.open()
		t.Fatalf("%s did not finish within 5s", what)
		panic("unreachable")
	}
}

// Covers: Reads, the statement deadline on page reads: cancelling a Run's
// context ends its read parked in storage, the Run is not committed, and
// the leader serves on.
func TestRunContextEndsStorageRead(t *testing.T) {
	hs, l, _ := hungReadSetup(t, readsHang)
	r := open(t, l)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), hangKey{}, true))
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, nil, func(db *sqlite3.Conn) error {
			_, err := query(db, "SELECT count(*) FROM t")
			return err
		})
		done <- err
	}()
	<-hs.reached
	cancel()
	if err := within(t, hs, done, "the cancelled Run"); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("the cancelled Run: %v", err)
	}
	if err := l.Err(); err != nil {
		t.Fatalf("leader: %v", err)
	}
	if got := queryConn(t, r, "SELECT count(*) FROM t"); got[0] != "50" {
		t.Fatalf("rows %v", got)
	}
}

// Covers: Reads, no storage read under the leader's lock: a reader parked in
// a storage read does not hold up an unrelated writer whose first write
// restarts the WAL and so advances the base version.
func TestReaderInStorageDoesNotBlockWriter(t *testing.T) {
	hs, l, w := hungReadSetup(t, readsHang)
	r := open(t, l)
	rdone := make(chan error, 1)
	go func() {
		_, err := r.Run(context.WithValue(context.Background(), hangKey{}, true), nil, func(db *sqlite3.Conn) error {
			_, err := query(db, "SELECT count(*) FROM t")
			return err
		})
		rdone <- err
	}()
	<-hs.reached
	wdone := make(chan error, 1)
	go func() {
		_, err := exec(w, "INSERT INTO t VALUES (1000, 'w')")
		wdone <- err
	}()
	if err := within(t, hs, wdone, "the writer"); err != nil {
		t.Fatalf("the writer: %v", err)
	}
	hs.open()
	if err := <-rdone; err != nil {
		t.Fatalf("the reader: %v", err)
	}
}

// Covers: Close "closes every connection once its Run has returned": with
// the store hung, Close cancels the storage call a Run waits in, a read or a
// commit's create, and returns.
func TestCloseCancelsHungStore(t *testing.T) {
	for _, op := range []string{"getrange", "create"} {
		t.Run(op, func(t *testing.T) {
			var armed atomic.Bool
			hs, l, w := hungReadSetup(t, func(ctx context.Context, o string) bool {
				return o == op && (ctx.Value(hangKey{}) != nil || op == "create" && armed.Load())
			})
			armed.Store(true)
			r := open(t, l)
			done := make(chan error, 1)
			go func() {
				c, sql := r, "SELECT count(*) FROM t"
				if op == "create" {
					c, sql = w, "INSERT INTO t VALUES (1000, 'w')"
				}
				_, err := c.Run(context.WithValue(context.Background(), hangKey{}, true), newID(), func(db *sqlite3.Conn) error {
					_, err := query(db, sql)
					return err
				})
				done <- err
			}()
			<-hs.reached
			closed := make(chan error, 1)
			go func() { closed <- l.Close() }()
			if err := within(t, hs, closed, "Close"); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := within(t, hs, done, "the Run"); !errors.Is(err, ErrNotCommitted) {
				t.Fatalf("the Run: %v", err)
			}
		})
	}
}

// Covers: Limits and size, "The measurement counts the Go-side indexes":
// connections share the leader's index, so a connection's first read does
// not cost memory in proportion to it.
func TestConnectionsShareTheIndex(t *testing.T) {
	st, keys, o := newDatabase(t, 1024)
	l := lead(t, st, keys, o, testConfig())
	w := open(t, l)
	mustRun(t, w, "CREATE TABLE big(k INTEGER PRIMARY KEY, v)")
	mustRun(t, w, "INSERT INTO big SELECT value, randomblob(900) FROM generate_series(1, 20000)")
	checkpoint(t, w, sqlite3.CHECKPOINT_TRUNCATE)
	const q = "SELECT length(v) FROM big WHERE k = 5"
	queryConn(t, w, q) // the pages are cached from here on
	l.mu.RLock()
	entries := len(l.tmpl.pages)
	l.mu.RUnlock()
	cs := make([]*Conn, 8)
	for i := range cs {
		cs[i] = open(t, l)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for _, c := range cs {
		queryConn(t, c, q)
	}
	runtime.ReadMemStats(&after)
	// A copy of the index costs more than 40 bytes an entry.
	per := (after.TotalAlloc - before.TotalAlloc) / uint64(len(cs))
	if per > uint64(entries)*8 {
		t.Fatalf("a connection's first read allocated %d bytes, with %d entries in the index", per, entries)
	}
	t.Logf("%d bytes per connection's first read; %d entries in the index", per, entries)
}

// runClass names the class of a Run's result, as Run documents them.
func runClass(txn uint64, err error) string {
	nc, unk := errors.Is(err, ErrNotCommitted), errors.Is(err, ErrUnknown)
	switch {
	case txn != 0 && !nc && !unk:
		return "committed"
	case err == nil:
		return "ok"
	case nc && !unk:
		return "not committed"
	case unk && !nc:
		return "unknown"
	}
	return fmt.Sprintf("unclassified (%d, %v)", txn, err)
}

// Covers: Commit outcomes, "A client is told exactly one of committed, not
// committed or unknown", for every exit of Run: each returns exactly one of
// the classes Run documents, with the reason it names.
func TestRunClasses(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(t *testing.T) (uint64, error)
		want string
		also []error
	}{
		{"read", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, nil, func(db *sqlite3.Conn) error { return db.Exec("SELECT * FROM t") })
		}, "ok", nil},
		{"commit", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return exec(c, "INSERT INTO t VALUES (1)")
		}, "committed", nil},
		{"commit, then an error", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			txn, err := c.Run(ctx, newID(), func(db *sqlite3.Conn) error {
				return errors.Join(db.Exec("INSERT INTO t VALUES (1)"), fmt.Errorf("%w: later", ErrNotCommitted))
			})
			if err == nil {
				t.Fatal("the later error was lost")
			}
			return txn, err
		}, "committed", nil},
		{"identifier too long", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, make([]byte, format.MaxCommitIDLen+1), func(*sqlite3.Conn) error { return nil })
		}, "not committed", nil},
		{"SQL error", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return exec(c, "INSERT INTO nosuch VALUES (1)")
		}, "not committed", []error{sqlite3.ERROR}},
		{"f's own error", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, nil, func(*sqlite3.Conn) error { return errInjected })
		}, "not committed", []error{errInjected}},
		{"no identifier", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, nil, func(db *sqlite3.Conn) error { return db.Exec("INSERT INTO t VALUES (1)") })
		}, "not committed", nil},
		{"connection closed", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			_ = c.Close()
			return exec(c, "SELECT 1")
		}, "not committed", nil},
		{"admission closed", func(t *testing.T) (uint64, error) {
			_, _, _, l, c := setup(t)
			l.cmu.Lock()
			l.admitClosed = true
			l.cmu.Unlock()
			return exec(c, "SELECT 1")
		}, "not committed", []error{ErrRetry}},
		{"panic, connection discarded", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, nil, func(*sqlite3.Conn) error { panic("injected") })
		}, "not committed", nil},
		{"unknown outcome", func(t *testing.T) (uint64, error) {
			st, _, _, _, c := setup(t)
			st.setRule(commitRules(fault{unknownNone: true}))
			return exec(c, "INSERT INTO t VALUES (1)")
		}, "unknown", []error{ErrStopped}},
		{"stopped before the Run by another's unknown outcome", func(t *testing.T) (uint64, error) {
			st, _, _, l, c := setup(t)
			st.setRule(commitRules(fault{unknownNone: true}))
			if _, err := exec(c, "INSERT INTO t VALUES (1)"); !errors.Is(err, ErrUnknown) {
				t.Fatalf("the stopping commit: %v", err)
			}
			if l.Err() == nil {
				t.Fatal("the leader serves on")
			}
			return exec(c, "SELECT 1")
		}, "not committed", []error{ErrStopped}},
		{"stopped during the Run by another's unknown outcome", func(t *testing.T) (uint64, error) {
			st, _, _, l, a := setup(t)
			b := open(t, l)
			entered, proceed := make(chan struct{}), make(chan struct{})
			type result struct {
				txn uint64
				err error
			}
			done := make(chan result, 1)
			go func() {
				txn, err := b.Run(ctx, newID(), func(db *sqlite3.Conn) error {
					close(entered)
					<-proceed
					return db.Exec("INSERT INTO t VALUES (2)")
				})
				done <- result{txn, err}
			}()
			<-entered
			st.setRule(commitRules(fault{unknownNone: true}))
			if _, err := exec(a, "INSERT INTO t VALUES (1)"); !errors.Is(err, ErrUnknown) {
				t.Fatalf("the stopping commit: %v", err)
			}
			close(proceed)
			r := <-done
			return r.txn, r.err
		}, "not committed", []error{ErrStopped}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn, err := tc.run(t)
			if got := runClass(txn, err); got != tc.want {
				t.Fatalf("class %s, want %s: %v", got, tc.want, err)
			}
			for _, e := range tc.also {
				if !errors.Is(err, e) {
					t.Fatalf("%v does not match %v", err, e)
				}
			}
		})
	}
}

// Covers: decision 6 for a panic outside Run: one in Open, here from a
// storage read, is returned as an error, leaves no connection registered,
// and either the leader serves on, with a TRUNCATE checkpoint that needs
// every reader slot, or it has stopped.
func TestPanicInOpen(t *testing.T) {
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Cache = NewCache(0)
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	l.cmu.Lock()
	n := len(l.conns)
	l.cmu.Unlock()
	st.setRule(func(op, key string) fault {
		if op == "getrange" {
			panic("store bug")
		}
		return fault{}
	})
	_, err := l.Open()
	st.setRule(nil)
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("Open: %v", err)
	}
	l.cmu.Lock()
	m := len(l.conns)
	l.cmu.Unlock()
	if m != n {
		t.Fatalf("%d connections registered after the failed Open, %d before", m, n)
	}
	if l.Err() != nil {
		t.Logf("the leader stopped: %v", l.Err())
		return
	}
	checkpoint(t, c, sqlite3.CHECKPOINT_TRUNCATE)
}
