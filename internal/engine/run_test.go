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
// backend does, until its context ends or the test releases it; with
// panics set, a call whose context ends panics instead.
type hangStore struct {
	storage.Store
	hang    func(ctx context.Context, op string) bool
	panics  bool
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
		if s.panics {
			panic("injected: a storage call ended by its context")
		}
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

// reached waits a few seconds for a call to reach hs, and fails if none
// does.
func reached(t *testing.T, hs *hangStore) {
	t.Helper()
	select {
	case <-hs.reached:
	case <-time.After(5 * time.Second):
		hs.open()
		t.Fatal("no storage call reached the hung store within 5s")
	}
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
// context ends its read parked in storage, of a commit object or of a base
// object, the Run is not committed, and the leader serves on.
func TestRunContextEndsStorageRead(t *testing.T) {
	for _, objects := range []string{"commit objects", "base objects"} {
		t.Run(objects, func(t *testing.T) {
			var hs *hangStore
			var l *Leader
			if objects == "commit objects" {
				hs, l, _ = hungReadSetup(t, readsHang)
			} else {
				ts, keys, o, _ := importDatabase(t, 50)
				hs = newHangStore(ts, readsHang)
				t.Cleanup(hs.open)
				cfg := testConfig()
				cfg.Cache = NewCache(0)
				l = lead(t, hs, keys, o, cfg)
			}
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
			reached(t, hs)
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
		})
	}
}

// spin is a statement that reads no storage and runs for some seconds, then
// ends by itself: a test that waits for it to be interrupted fails, rather
// than hangs, when the interrupt never reaches SQLite.
const spin = "SELECT count(*) FROM generate_series(1, 100000000)"

// Covers: Reads, the statement deadline on a statement that reads no
// storage: the Run's context ends it, and the Run is not committed.
func TestRunDeadlineEndsStatement(t *testing.T) {
	_, _, _, l, c := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Run(ctx, nil, func(db *sqlite3.Conn) error {
		_, err := query(db, spin)
		return err
	})
	if !errors.Is(err, ErrNotCommitted) || !errors.Is(err, sqlite3.INTERRUPT) {
		t.Fatalf("the Run past its deadline, after %v: %v", time.Since(start), err)
	}
	if err := l.Err(); err != nil {
		t.Fatalf("leader: %v", err)
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
	reached(t, hs)
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
			reached(t, hs)
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
	// Open and the first read together: a copy made at either is counted.
	const n = 8
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range n {
		queryConn(t, open(t, l), q)
	}
	runtime.ReadMemStats(&after)
	// A copy of the index costs more than 40 bytes an entry.
	per := (after.TotalAlloc - before.TotalAlloc) / n
	if per > uint64(entries)*8 {
		t.Fatalf("a connection's Open and first read allocated %d bytes, with %d entries in the index", per, entries)
	}
	t.Logf("%d bytes per connection's Open and first read; %d entries in the index", per, entries)
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

// Covers: decision 6's commit path for a writing statement closed before
// its end: SQLite commits an INSERT ... RETURNING closed after one step
// inside sqlite3_finalize, where it calls no WAL hook, and the Run reports
// that commit as any other, with the leader serving on, whatever f does
// next in the Run: a second write is refused as a second commit, a
// rollback or an open read touches nothing committed, an empty write
// transaction commits nothing, and a panic discards just the connection.
func TestReturningClosedEarly(t *testing.T) {
	for _, steps := range []string{"one step", "to its end"} {
		for _, then := range []string{"nothing", "a second write", "a rollback", "a read left open", "an empty write transaction", "a panic"} {
			t.Run(steps+", then "+then, func(t *testing.T) {
				st, keys, _, l, c := setup(t)
				other := open(t, l)
				want := l.Head() + 1
				txn, err := c.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
					s, _, err := db.Prepare("INSERT INTO t VALUES (5), (6) RETURNING x")
					if err != nil {
						return err
					}
					if !s.Step() {
						return errors.Join(errors.New("no row returned"), s.Close())
					}
					for steps == "to its end" && s.Step() {
					}
					if err := s.Close(); err != nil {
						return err
					}
					switch then {
					case "a second write":
						return db.Exec("INSERT INTO t VALUES (9)")
					case "a rollback":
						return db.Exec("BEGIN; ROLLBACK")
					case "an empty write transaction":
						return db.Exec("BEGIN IMMEDIATE; COMMIT")
					case "a read left open":
						if err := db.Exec("BEGIN"); err != nil {
							return err
						}
						_, err := query(db, "SELECT count(*) FROM t")
						return err
					case "a panic":
						panic("injected")
					}
					return nil
				})
				failed := then == "a second write" || then == "a panic"
				if txn != want || runClass(txn, err) != "committed" || (err != nil) != failed || l.Err() != nil {
					t.Fatalf("Run: %d, %v; leader: %v; want %d and serving", txn, err, l.Err(), want)
				}
				if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,5,6" {
					t.Fatalf("durable rows %s, want 0,5,6", got)
				}
				if got := queryConn(t, other, "SELECT count(*) FROM t"); got[0] != "3" {
					t.Fatalf("another connection reads %v rows, want 3", got)
				}
				later := c // discarded after the panic, in a transaction after the read
				if then == "a panic" || then == "a read left open" {
					later = other
				}
				if _, err := exec(later, "INSERT INTO t VALUES (7)"); err != nil {
					t.Fatalf("a later commit: %v", err)
				}
			})
		}
	}
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
		{"empty identifier", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, []byte{}, func(db *sqlite3.Conn) error { return db.Exec("INSERT INTO t VALUES (1)") })
		}, "not committed", nil},
		{"commit, then an error of the unknown class", func(t *testing.T) (uint64, error) {
			_, _, _, _, c := setup(t)
			return c.Run(ctx, newID(), func(db *sqlite3.Conn) error {
				return errors.Join(db.Exec("INSERT INTO t VALUES (1)"), fmt.Errorf("%w: later", ErrUnknown))
			})
		}, "committed", nil},
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
		{"unknown outcome, then a not-committed error", func(t *testing.T) (uint64, error) {
			st, _, _, _, c := setup(t)
			st.setRule(commitRules(fault{unknownNone: true}))
			return c.Run(ctx, newID(), func(db *sqlite3.Conn) error {
				return errors.Join(db.Exec("INSERT INTO t VALUES (1)"), fmt.Errorf("%w: later", ErrNotCommitted))
			})
		}, "unknown", nil},
		{"unknown outcome, then a panic that discards the connection", func(t *testing.T) (uint64, error) {
			st, _, _, _, c := setup(t)
			st.setRule(commitRules(fault{unknownNone: true}))
			txn, err := c.Run(ctx, newID(), func(db *sqlite3.Conn) error {
				panic(fmt.Sprintf("after: %v", db.Exec("INSERT INTO t VALUES (1)")))
			})
			if !strings.Contains(err.Error(), "discarded") || strings.Contains(err.Error(), "nothing of this Run") {
				t.Fatalf("not discarded, or said to have sent nothing: %v", err)
			}
			return txn, err
		}, "unknown", []error{ErrStopped}},
		{"unknown outcome, then a panic that leaves the connection unclosable", func(t *testing.T) (uint64, error) {
			st, _, _, _, c := setup(t)
			st.setRule(commitRules(fault{unknownNone: true}))
			txn, err := c.Run(ctx, newID(), func(db *sqlite3.Conn) error {
				e := db.Exec("INSERT INTO t VALUES (1)")
				if err := db.CreateFunction("boom", 0, 0, func(sqlite3.Context, ...sqlite3.Value) { panic("boom") }); err != nil {
					return errors.Join(e, err)
				}
				return errors.Join(e, db.Exec("SELECT boom()"))
			})
			if !strings.Contains(err.Error(), "could not be closed") || strings.Contains(err.Error(), "nothing of this Run") {
				t.Fatalf("not unclosable, or said to have sent nothing: %v", err)
			}
			return txn, err
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
// storage read inside sqlite3.Open, is returned as an error, leaves no
// connection registered, and stops the leader, because a connection that
// panicked inside sqlite3.Open cannot be closed.
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
	if !errors.Is(l.Err(), ErrStopped) {
		t.Fatalf("the leader serves on after a panic inside sqlite3.Open: %v", l.Err())
	}
}

// Covers: decision 10's steps 4 and 5 for a connection still opening:
// maintenance ends an Open parked in a storage read, as it ends a Run's,
// and finishes; the leader serves on.
func TestMaintenanceEndsHungOpen(t *testing.T) {
	var armed atomic.Bool
	hs, l, w := hungReadSetup(t, func(_ context.Context, op string) bool { return op == "getrange" && armed.Load() })
	armed.Store(true)
	opened := make(chan error, 1)
	go func() {
		c, err := l.Open()
		if err == nil {
			err = c.Close()
		}
		opened <- err
	}()
	reached(t, hs)
	startMaintenance(l)
	// SQLite reports the cancelled read as an I/O error, without its cause.
	if err := within(t, hs, opened, "the Open parked in a storage read"); !errors.Is(err, ErrRetry) || !errors.Is(err, ErrNotCommitted) || !errors.Is(err, sqlite3.IOERR) {
		t.Fatalf("Open ended by maintenance: %v, want ErrRetry and ErrNotCommitted wrapping SQLite's I/O error", err)
	}
	armed.Store(false)
	waitMaintenance(t, l)
	if got := queryConn(t, w, "SELECT count(*) FROM t"); got[0] != "50" || l.Err() != nil {
		t.Fatalf("after maintenance: %v; leader: %v", got, l.Err())
	}
}

// Covers: Open's retry class belongs only to an Open that fails after
// maintenance has cancelled its storage reads: one whose storage read fails
// with no maintenance running, and one that Close ends, are not told to
// retry, nor is one that panics after maintenance has cancelled it. An Open
// whose read fails for another reason during maintenance is told to retry
// too, as Open documents.
func TestOpenFailureNotRetry(t *testing.T) {
	t.Run("storage error", func(t *testing.T) {
		st, keys, o := newDatabase(t, 4096)
		cfg := testConfig()
		cfg.Cache = NewCache(0)
		l := lead(t, st, keys, o, cfg)
		mustRun(t, open(t, l), "CREATE TABLE t(x)")
		st.setRule(func(op, key string) fault {
			if op == "getrange" {
				return fault{noEffect: true, err: errInjected}
			}
			return fault{}
		})
		if _, err := l.Open(); err == nil || errors.Is(err, ErrRetry) {
			t.Fatalf("Open through a failed read: %v, want an error that does not match ErrRetry", err)
		}
	})
	t.Run("Close", func(t *testing.T) {
		var armed atomic.Bool
		hs, l, _ := hungReadSetup(t, func(_ context.Context, op string) bool { return op == "getrange" && armed.Load() })
		armed.Store(true)
		opened := make(chan error, 1)
		go func() {
			_, err := l.Open()
			opened <- err
		}()
		reached(t, hs)
		go func() { _ = l.Close() }() // Close waits for the Open
		if err := within(t, hs, opened, "the Open parked in a storage read"); err == nil || errors.Is(err, ErrRetry) {
			t.Fatalf("Open ended by Close: %v, want an error that does not match ErrRetry", err)
		}
	})
	t.Run("panic during maintenance", func(t *testing.T) {
		var armed atomic.Bool
		hs, l, _ := hungReadSetup(t, func(_ context.Context, op string) bool { return op == "getrange" && armed.Load() })
		hs.panics = true
		armed.Store(true)
		opened := make(chan error, 1)
		go func() {
			_, err := l.Open()
			opened <- err
		}()
		reached(t, hs)
		startMaintenance(l)
		if err := within(t, hs, opened, "the Open parked in a storage read"); err == nil || errors.Is(err, ErrRetry) || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("Open that panicked during maintenance: %v, want a panic that does not match ErrRetry", err)
		}
	})
}

// Covers: decision 10's steps 4 to 6 for an Open that begins after
// maintenance listed the connections: its setup reads from storage inside a
// read transaction, whose WAL read lock the TRUNCATE checkpoint waits on, so
// maintenance ends it too, and the Open is told to retry.
func TestMaintenanceEndsOpenAfterListing(t *testing.T) {
	var armed atomic.Bool
	var n atomic.Int64
	// Open's first storage read is inside sqlite3.Open; the second, in
	// setup, holds a WAL read lock.
	hs, l, w := hungReadSetup(t, func(_ context.Context, op string) bool {
		return op == "getrange" && armed.Load() && n.Add(1) >= 2
	})
	mustRun(t, w, "INSERT INTO t VALUES (1000, 'x')") // frames the checkpoint must backfill
	b := open(t, l)
	entered, hold, ran := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := b.Run(context.Background(), nil, func(*sqlite3.Conn) error {
			close(entered)
			<-hold
			return nil
		})
		ran <- err
	}()
	<-entered
	startMaintenance(l)
	for end := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		l.cmu.Lock()
		listed := b.maint
		l.cmu.Unlock()
		if listed {
			break
		}
		if time.Now().After(end) {
			close(hold)
			t.Fatal("maintenance did not list the running connection within 5s")
		}
	}
	armed.Store(true)
	opened := make(chan error, 1)
	go func() {
		c, err := l.Open()
		if err == nil {
			err = c.Close()
		}
		opened <- err
	}()
	// The Open parks in its setup read, or maintenance has ended it already.
	select {
	case <-hs.reached:
	case err := <-opened:
		opened <- err
	case <-time.After(5 * time.Second):
		t.Log("the Open reached neither the store nor its end within 5s")
	}
	close(hold)
	if err := <-ran; err != nil {
		t.Fatalf("the running connection's Run: %v", err)
	}
	if err := within(t, hs, opened, "an Open that began after the listing"); !errors.Is(err, ErrRetry) || !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Open ended by maintenance: %v, want ErrRetry and ErrNotCommitted", err)
	}
	armed.Store(false)
	waitMaintenance(t, l)
	if got := queryConn(t, w, "SELECT count(*) FROM t"); got[0] != "51" || l.Err() != nil {
		t.Fatalf("after maintenance: %v; leader: %v", got, l.Err())
	}
}

// Covers: decision 6 for a failure outside Run: an Open whose setup fails
// after maintenance listed its connection leaves maintenance nothing to
// roll back, and the leader serves on.
func TestOpenFailsDuringMaintenance(t *testing.T) {
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Cache = NewCache(0)
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	checkpoint(t, c, sqlite3.CHECKPOINT_TRUNCATE)
	g := newGate()
	t.Cleanup(g.open)
	var n atomic.Int64
	// Open's first storage read is inside sqlite3.Open; the second, in
	// setup, waits and fails.
	st.setRule(func(op, key string) fault {
		if op == "getrange" && n.Add(1) > 1 {
			return fault{before: g, noEffect: true}
		}
		return fault{}
	})
	errc := make(chan error, 1)
	go func() {
		_, err := l.Open()
		errc <- err
	}()
	select {
	case <-g.reached:
	case err := <-errc:
		t.Fatalf("Open returned before its read in setup: %v", err)
	}
	startMaintenance(l)
	for end := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		listed := false
		l.cmu.Lock()
		for _, x := range l.conns {
			listed = listed || x != c && x != l.mconn && x.maint
		}
		l.cmu.Unlock()
		if listed {
			break
		}
		if time.Now().After(end) {
			t.Fatal("maintenance did not list the opening connection")
		}
	}
	g.open()
	if err := <-errc; err == nil {
		t.Fatal("Open succeeded through a failed read")
	}
	st.setRule(nil)
	waitMaintenance(t, l)
	if err := l.Err(); err != nil {
		t.Fatalf("leader: %v", err)
	}
}

// Covers: Close "closes every connection": a statement f left open, against
// Run's rule, is finalized at Close, so the connection's read lock goes
// with it and a TRUNCATE checkpoint, which needs every reader gone,
// succeeds.
func TestCloseFinalizesLeakedStatements(t *testing.T) {
	_, _, _, l, c := setup(t)
	w := open(t, l)
	if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
		s, _, err := db.Prepare("SELECT x FROM t")
		if err != nil {
			return err
		}
		s.Step() // left open
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	checkpoint(t, w, sqlite3.CHECKPOINT_TRUNCATE)
	if err := l.Err(); err != nil {
		t.Fatalf("leader: %v", err)
	}
}

// Covers: decision 6 for a panic outside Run, in Close: the connection may
// keep its locks, so the leader stops. The panic is in a rollback hook f
// set, which SQLite calls when it closes a connection in a transaction.
func TestClosePanicStops(t *testing.T) {
	_, _, _, l, c := setup(t)
	if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
		db.RollbackHook(func() { panic("injected") })
		return db.Exec("BEGIN; SELECT * FROM t")
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); !errors.Is(err, ErrStopped) || !errors.Is(l.Err(), ErrStopped) {
		t.Fatalf("Close: %v; leader: %v", err, l.Err())
	}
}
