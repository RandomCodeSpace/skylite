package engine

// Tests for failures in and around the commit path: "Commit capture and
// failure" in record 0004, the fail-stop rules of decision 6 and the panic
// rule of decision 2.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// Covers: Commit capture and failure, "Failures after publication: an
// out-of-memory in the shared-memory index after a successful upload, with
// a second connection open, stops the whole database on that node, and the
// second connection never serves a read from the old image afterwards. The
// same for every other panic and unexpected SQLite error in the commit path
// once an upload may have started"; Commit outcomes, "an error in the
// commit path after an upload may have started ... stop[s] the node".
//
// An out-of-memory inside SQLite's shared-memory code cannot be injected
// from Go; ncruces reports it as a panic in Go once SQLite returns, which
// the "panic after upload" case stands for. The I/O error case makes SQLite
// itself fail the commit after the upload, through the last WAL write of
// its sector padding, and roll it back. A panic through db.Exec leaves the
// connection unclosable; the cases "in a step" panic inside a prepared
// statement's step, which leaves it closable, as ncruces's out-of-memory
// panic does. SQLite rolls a failed VACUUM back without calling the
// rollback hook; the cases "then" go on in the same Run with a commit of an
// empty write transaction, which must not complete the uploaded one.
func TestFailureAfterUpload(t *testing.T) {
	for _, tc := range []string{
		"panic after upload", "panic in create", "SQLite error after upload", "panic after upload, in a step", "panic in create, in a step", "SQLite error after upload, in VACUUM",
		"SQLite error after upload, in VACUUM, then BEGIN IMMEDIATE; COMMIT", "SQLite error after upload, in VACUUM, then UPDATE t SET x = x WHERE 0",
	} {
		t.Run(tc, func(t *testing.T) {
			how, then, _ := strings.Cut(tc, ", then ")
			how, step := strings.CutSuffix(how, ", in a step")
			how, vacuum := strings.CutSuffix(how, ", in VACUUM")
			st, keys, o := newDatabase(t, 4096)
			l := lead(t, st, keys, o, testConfig())
			w, other := open(t, l), open(t, l)
			mustRun(t, w, "BEGIN; CREATE TABLE t(x); INSERT INTO t VALUES (0); COMMIT")
			if got := strings.Join(queryConn(t, other, "SELECT x FROM t"), ","); got != "0" {
				t.Fatalf("rows %s", got)
			}
			fired := false
			switch how {
			case "panic after upload":
				l.afterUpload = func() { fired = true; panic("injected") }
			case "panic in create":
				st.setRule(commitRules(fault{drop: true}, fault{}))
				st.setRule(func(op, key string) fault {
					if op == "create" && isCommit(key) {
						fired = true
						panic("injected")
					}
					return fault{}
				})
			case "SQLite error after upload":
				l.psow = false // so SQLite pads the commit and writes the rest after its sync
				// The padding geometry depends on the connection's settings
				// at open: reopen the writer.
				_ = w.Close()
				w = open(t, l)
				armed := false
				l.afterUpload = func() { armed = true }
				l.fault = func(file, op string, off int64, n int) error {
					if armed && file == "wal" && op == "write" {
						fired = true
						return errInjected
					}
					return nil
				}
			}
			// After SQLite rolls the uploaded transaction back, and before
			// the Run ends, another connection must not commit on top of
			// the image that lacks it.
			var otherErr error
			txn, err := w.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
				var err error
				switch {
				case vacuum:
					err = db.Exec("VACUUM")
					l.afterUpload = nil
					if then != "" {
						// f ignores the VACUUM's error.
						err = db.Exec(then)
					}
				case step:
					_, err = query(db, "INSERT INTO t VALUES (1)")
				default:
					err = db.Exec("INSERT INTO t VALUES (1)")
				}
				if tc == "SQLite error after upload" {
					_, otherErr = exec(other, "INSERT INTO t VALUES (2)")
				}
				return err
			})
			if tc == "SQLite error after upload" && !errors.Is(otherErr, ErrStopped) {
				t.Fatalf("another connection wrote after the rollback of an uploaded commit: %v", otherErr)
			}
			if !fired {
				t.Fatal("the fault did not fire")
			}
			if err == nil || !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("commit: %d, %v; leader: %v; want the database stopped", txn, err, l.Err())
			}
			if errors.Is(err, ErrNotCommitted) {
				t.Fatalf("told not committed after the upload may have started: %v", err)
			}
			durableRows, want := rows(t, durable(t, st.Store, keys, 0)), "0,1"
			if vacuum {
				want = "0"
			}
			if how != "panic in create" && (txn != o.Head+2 || durableRows != want) {
				t.Fatalf("committed in storage: reported %d, durable rows %s", txn, durableRows)
			}
			if _, err := other.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
				_, err := query(db, "SELECT x FROM t")
				return err
			}); !errors.Is(err, ErrStopped) {
				t.Fatalf("the second connection served after the stop: %v", err)
			}
		})
	}
}

// landThenPanic is a store whose Create of a commit panics once armed,
// after the object landed.
type landThenPanic struct {
	storage.Store
	armed atomic.Bool
}

func (s *landThenPanic) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err == nil && isCommit(key) && s.armed.CompareAndSwap(true, false) {
		panic("store bug after the create landed")
	}
	return v, err
}

// Covers: decision 6, a panic in the commit path stops the database and the
// client is never told not committed for a transaction in the durable
// history, for a panic in the store's Create after the object landed. With
// the statement closed by defer, SQLite syncs the WAL again during the
// unresolved commit; with it left open or run through db.Exec, it does not.
func TestCreateLandsThenPanics(t *testing.T) {
	for _, how := range []string{"step, statement closed by defer", "step, statement left open", "exec"} {
		t.Run(how, func(t *testing.T) {
			st, keys, o := newDatabase(t, 4096)
			ps := &landThenPanic{Store: st}
			l := lead(t, ps, keys, o, testConfig())
			w, other := open(t, l), open(t, l)
			mustRun(t, w, "BEGIN; CREATE TABLE t(x); INSERT INTO t VALUES (0); COMMIT")
			ps.armed.Store(true)
			txn, err := w.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
				switch how {
				case "exec":
					return db.Exec("INSERT INTO t VALUES (1)")
				case "step, statement left open":
					s, _, err := db.Prepare("INSERT INTO t VALUES (1)")
					if err != nil {
						return err
					}
					s.Step()
					return s.Close()
				}
				_, err := query(db, "INSERT INTO t VALUES (1)")
				return err
			})
			if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
				t.Fatalf("durable rows %s", got)
			}
			if txn == 0 && !errors.Is(err, ErrUnknown) || errors.Is(err, ErrNotCommitted) || !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("Run: %d, %v; leader: %v; want committed or unknown, and the database stopped", txn, err, l.Err())
			}
			if _, err := exec(other, "SELECT 1"); !errors.Is(err, ErrStopped) {
				t.Fatalf("the second connection served after the stop: %v", err)
			}
		})
	}
}

// Covers: Commit capture and failure, "Fail-stop with a second writer:
// while the database stops on a node, another connection's upload, or the
// processing of its response, overlaps the shutdown. That writer's client
// is never told not committed while its create can still land, and if it
// lands it is in the history the oracle checks."
func TestStopDuringAnotherUpload(t *testing.T) {
	for _, how := range []string{"fail-stop", "close"} {
		t.Run(how, func(t *testing.T) {
			st, keys, _, l, w := setup(t)
			landed := newGate()
			st.setRule(commitRules(fault{after: landed, drop: true}, fault{}))
			type res struct {
				txn uint64
				err error
			}
			done := make(chan res)
			go func() {
				txn, err := exec(w, "INSERT INTO t VALUES (1)")
				done <- res{txn, err}
			}()
			<-landed.reached
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				if how == "close" {
					_ = l.Close()
				} else {
					_ = l.stop(errInjected)
				}
			}()
			if how == "fail-stop" {
				<-stopped
			}
			landed.open()
			r := <-done
			<-stopped
			if errors.Is(r.err, ErrNotCommitted) {
				t.Fatalf("told not committed for a create that landed: %v", r.err)
			}
			if r.txn == 0 && !errors.Is(r.err, ErrUnknown) {
				t.Fatalf("neither committed nor unknown: %v", r.err)
			}
			if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
				t.Fatalf("durable rows %s", got)
			}
		})
	}
}

// Covers: Commit capture and failure, "A panic in a connection outside the
// commit path, including one that holds the WAL write lock or is running a
// checkpoint: within a time bound, another connection completes a later
// write and a checkpoint, and a newly opened connection reads, with every
// result checked against the oracle"; decision 6, a panic outside the
// commit path discards the connection if it closes cleanly and otherwise
// stops the database. ncruces's out-of-memory panic, and a VFS panic inside
// a prepared statement's step, leave the connection closable; a VFS panic
// inside sqlite3_exec does not (TestExperimentPanickedConnectionKeepsLocks),
// and there the database stops and a new leader, after a takeover, does the
// same work. Also covers Limits and size, "Each connection's memory ceiling
// is set": exceeding it panics in ncruces. Every wait for a lock is bounded
// by the busy timeout, so leaked locks fail by assertion.
func TestPanicOutsideTheCommitPath(t *testing.T) {
	// Each value is under the length limit; together they pass the 16 MiB
	// ceiling.
	const oom = "SELECT randomblob(3000000), randomblob(3000000), randomblob(3000000), randomblob(3000000), randomblob(3000000), randomblob(3000000)"
	walWritePanics := func(file, op string, off int64, n int) error {
		if file == "wal" && op == "write" {
			panic("injected")
		}
		return nil
	}
	for _, tc := range []struct {
		name  string
		fault func(file, op string, off int64, n int) error
		f     func(db *sqlite3.Conn) error
		stops bool
	}{
		{"out of memory in Exec, holding the write lock", nil, func(db *sqlite3.Conn) error {
			if err := db.Exec("BEGIN IMMEDIATE; UPDATE t SET v = 'lost' WHERE k = 1"); err != nil {
				return err
			}
			return db.Exec(oom)
		}, false},
		{"out of memory in a step, holding the write lock", nil, func(db *sqlite3.Conn) error {
			if err := db.Exec("BEGIN IMMEDIATE; UPDATE t SET v = 'lost' WHERE k = 1"); err != nil {
				return err
			}
			_, err := query(db, oom)
			return err
		}, false},
		{"VFS panic in a step, holding the write lock", walWritePanics, func(db *sqlite3.Conn) error {
			if err := db.Exec("PRAGMA cache_size=5; BEGIN"); err != nil {
				return err
			}
			s, _, err := db.Prepare("UPDATE t SET v = 'spilled'")
			if err != nil {
				return err
			}
			s.Step()
			return s.Close()
		}, false},
		{"out of memory in a step whose statement is left open", nil, func(db *sqlite3.Conn) error {
			s, _, err := db.Prepare(oom)
			if err != nil {
				return err
			}
			s.Step()
			return s.Close()
		}, false},
		{"VFS panic in a checkpoint", func(file, op string, off int64, n int) error {
			if file == "main" && op == "write" {
				panic("injected")
			}
			return nil
		}, func(db *sqlite3.Conn) error {
			_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE)
			return err
		}, false},
		{"VFS panic in Exec, holding the write lock", walWritePanics, func(db *sqlite3.Conn) error {
			return db.Exec("PRAGMA cache_size=5; BEGIN; UPDATE t SET v = 'spilled'; COMMIT")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, keys, o := newDatabase(t, 1024)
			cfg := testConfig()
			cfg.MaxMemory = 16 << 20
			l := lead(t, st, keys, o, cfg)
			victim, w := open(t, l), open(t, l)
			or := newOracle(t)
			for _, sql := range []string{
				"CREATE TABLE t(k INTEGER PRIMARY KEY, v)",
				"INSERT INTO t SELECT value, printf('%0900d', value) FROM generate_series(1, 200)",
			} {
				mustRun(t, w, sql)
				or.exec(t, sql)
			}
			l.fault = tc.fault
			txn, err := victim.Run(context.Background(), newID(), tc.f)
			l.fault = nil
			if txn != 0 || !errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrUnknown) || !strings.Contains(err.Error(), "panic") {
				t.Fatalf("victim: %d, %v; want not committed, after a panic", txn, err)
			}
			if tc.stops {
				if !errors.Is(err, ErrStopped) {
					t.Fatalf("victim: %v, want the database stopped", err)
				}
				if _, err := exec(w, "SELECT 1"); !errors.Is(err, ErrStopped) {
					t.Fatalf("another connection served after the panic: %v", err)
				}
				_ = l.Close()
				l = lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
				w = open(t, l)
			} else {
				if errors.Is(err, ErrStopped) || l.Err() != nil {
					t.Fatalf("victim: %v; leader: %v; want only the connection discarded", err, l.Err())
				}
				if _, err := exec(victim, "SELECT 1"); !errors.Is(err, ErrNotCommitted) || !strings.Contains(err.Error(), "closed") {
					t.Fatalf("the discarded connection: %v", err)
				}
				l.cmu.Lock()
				_, registered := l.conns[victim.n]
				l.cmu.Unlock()
				if registered {
					t.Fatal("the discarded connection is still registered")
				}
			}
			sql := "UPDATE t SET v = 'after' WHERE k % 7 = 0"
			mustRun(t, w, sql)
			or.exec(t, sql)
			if _, err := w.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
				_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE)
				return err
			}); err != nil {
				t.Fatalf("TRUNCATE checkpoint: %v", err)
			}
			exp := mustDump(t, or.db)
			if got := dumpConn(t, open(t, l)); got != exp {
				t.Fatal("a new connection differs from the oracle")
			}
			if got := mustDump(t, durable(t, st.Store, keys, 0)); got != exp {
				t.Fatal("durable state differs from the oracle")
			}
		})
	}
}

// Covers: decision 6 for a panic outside the commit path on the leader's
// own connection, here in maintenance's checkpoint: that connection is never
// discarded, since maintenance needs it, so the database stops.
func TestPanicOnMaintenanceConn(t *testing.T) {
	_, _, _, l, c := setup(t)
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	l.fault = func(file, op string, off int64, n int) error {
		if file == "main" && op == "write" {
			panic("injected")
		}
		return nil
	}
	startMaintenance(l)
	waitMaintenance(t, l)
	if !errors.Is(l.Err(), ErrStopped) {
		_, err := exec(c, "SELECT 1")
		t.Fatalf("the leader serves on after a panic on its own connection; a client's read: %v", err)
	}
}

// Covers: Limits and size, "Each connection's memory ceiling is set", for
// the caps below it: a value or a statement longer than a quarter of the
// ceiling fails with SQLITE_TOOBIG instead of reaching the ceiling, and the
// connection serves on.
func TestValueAndStatementLimits(t *testing.T) {
	st, keys, o := newDatabase(t, 1024)
	cfg := testConfig()
	cfg.MaxMemory = 16 << 20
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	const limit = 4 << 20 // MaxMemory / 4
	for _, tc := range []struct {
		sql    string
		tooBig bool
	}{
		{fmt.Sprintf("SELECT length(randomblob(%d))", limit), false},
		{fmt.Sprintf("SELECT length(randomblob(%d))", limit+1), true},
		{"SELECT '" + strings.Repeat("x", limit) + "'", true},
	} {
		_, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
			_, err := query(db, tc.sql)
			return err
		})
		if tc.tooBig != errors.Is(err, sqlite3.TOOBIG) || !tc.tooBig && err != nil {
			t.Fatalf("%.40s: %v", tc.sql, err)
		}
	}
	if got := queryConn(t, c, "SELECT 1"); got[0] != "1" || l.Err() != nil {
		t.Fatalf("after the limits: %v, %v", got, l.Err())
	}
}

// Covers: Commit capture and failure, "The local WAL is on a real disk, and
// tests inject short writes, a full disk, and a failed truncate or fsync"
// (the truncate and the engine's own fsync are in TestFailedTruncationStops)
// and "A commit known not to have entered the durable history never
// reappears ... after every connection closes and the database reopens",
// for each failure of a WAL write or sync before the upload. A full disk is
// not reported as the temporary-file budget, also after the budget refused
// a statement on the same connection.
func TestLocalWALFailures(t *testing.T) {
	for _, tc := range []string{"frame write", "full disk", "full disk after a budget refusal", "sync"} {
		t.Run(tc, func(t *testing.T) {
			st, keys, o, l, c := setup(t)
			if tc == "full disk after a budget refusal" {
				l.cfg.TempBudget = 1 << 20
				if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
					_, err := query(db, "SELECT count(*) FROM (SELECT randomblob(1000) AS x FROM generate_series(1, 5000) ORDER BY x)")
					return err
				}); !errors.Is(err, sqlite3.FULL) {
					t.Fatalf("a sort past the budget: %v", err)
				}
				l.cfg.TempBudget = testConfig().TempBudget
			}
			full := strings.HasPrefix(tc, "full disk")
			l.fault = func(file, op string, off int64, n int) error {
				switch {
				case file != "wal":
				case tc == "frame write" && op == "write":
					return errInjected
				case full && op == "write":
					return sqlite3.FULL
				case tc == "sync" && op == "sync":
					return errInjected
				}
				return nil
			}
			st.resetCounts()
			txn, err := exec(c, "INSERT INTO t VALUES (1)")
			l.fault = nil
			if err == nil || txn != 0 || errors.Is(err, sqlite3.BUSY) || full && strings.Contains(err.Error(), "temporary") {
				t.Fatalf("commit: %d, %v", txn, err)
			}
			if _, creates, _ := st.counts(); creates != 0 {
				t.Fatalf("%d creates after a local failure", creates)
			}
			if err := l.Err(); err != nil {
				t.Fatalf("leader stopped: %v", err)
			}
			mustRun(t, c, "INSERT INTO t VALUES (2)")
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l2 := lead(t, st, keys, takeOffice(t, st.Store, keys), testConfig())
			if got := strings.Join(queryConn(t, open(t, l2), "SELECT x FROM t ORDER BY x"), ","); got != "0,2" {
				t.Fatalf("rows after reopening: %s", got)
			}
			_ = o
		})
	}
}
