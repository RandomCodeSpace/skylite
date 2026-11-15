package engine

// Tests for failures in and around the commit path: "Commit capture and
// failure" in record 0004, the fail-stop rules of decision 6 and the panic
// rule of decision 2.

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
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
// its sector padding, and roll it back.
func TestFailureAfterUpload(t *testing.T) {
	for _, tc := range []string{"panic after upload", "panic in create", "SQLite error after upload"} {
		t.Run(tc, func(t *testing.T) {
			st, keys, o := newDatabase(t, 4096)
			l := lead(t, st, keys, o, testConfig())
			w, other := open(t, l), open(t, l)
			mustRun(t, w, "BEGIN; CREATE TABLE t(x); INSERT INTO t VALUES (0); COMMIT")
			if got := strings.Join(queryConn(t, other, "SELECT x FROM t"), ","); got != "0" {
				t.Fatalf("rows %s", got)
			}
			fired := false
			switch tc {
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
			txn, err := w.Run(newID(), func(db *sqlite3.Conn) error {
				err := db.Exec("INSERT INTO t VALUES (1)")
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
			durableRows := rows(t, durable(t, st.Store, keys, 0))
			if tc != "panic in create" && (txn != o.Head+2 || durableRows != "0,1") {
				t.Fatalf("committed in storage: reported %d, durable rows %s", txn, durableRows)
			}
			if _, err := other.Run(nil, func(db *sqlite3.Conn) error {
				_, err := query(db, "SELECT x FROM t")
				return err
			}); !errors.Is(err, ErrStopped) {
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
// result checked against the oracle. ... If this fails, the fail-stop rule
// extends to every panic." It fails
// (TestExperimentPanickedConnectionKeepsLocks), so this test checks the
// extended rule: any panic stops the database on the node, the other
// connections stop serving, and a new leader, after a takeover, completes a
// write and a checkpoint and a new connection reads, all checked against
// the oracle. Also covers Limits and size, "Each connection's memory
// ceiling is set": exceeding it panics in ncruces.
func TestPanicStopsTheDatabase(t *testing.T) {
	for _, tc := range []string{"holding the write lock", "running a checkpoint", "out of memory"} {
		t.Run(tc, func(t *testing.T) {
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
			var f func(db *sqlite3.Conn) error
			switch tc {
			case "holding the write lock":
				l.fault = func(file, op string, off int64, n int) error {
					if file == "wal" && op == "write" {
						panic("injected")
					}
					return nil
				}
				f = func(db *sqlite3.Conn) error {
					return db.Exec("PRAGMA cache_size=5; BEGIN; UPDATE t SET v = 'spilled'; COMMIT")
				}
			case "running a checkpoint":
				l.fault = func(file, op string, off int64, n int) error {
					if file == "main" && op == "write" {
						panic("injected")
					}
					return nil
				}
				f = func(db *sqlite3.Conn) error {
					_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE)
					return err
				}
			case "out of memory":
				f = func(db *sqlite3.Conn) error {
					// Each value is under the length limit; together they
					// pass the 16 MiB ceiling.
					_, err := query(db, "SELECT randomblob(3000000), randomblob(3000000), randomblob(3000000), randomblob(3000000), randomblob(3000000), randomblob(3000000)")
					return err
				}
			}
			_, err := victim.Run(newID(), f)
			l.fault = nil
			if !errors.Is(err, ErrStopped) || !errors.Is(err, ErrNotCommitted) || !strings.Contains(err.Error(), "panic") {
				t.Fatalf("victim: %v, want the database stopped, nothing committed", err)
			}
			if _, err := exec(w, "SELECT 1"); !errors.Is(err, ErrStopped) {
				t.Fatalf("another connection served after the panic: %v", err)
			}
			_ = l.Close()

			deadline := time.AfterFunc(30*time.Second, func() { panic("no progress within 30s after the panic") })
			defer deadline.Stop()
			l2 := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
			w2 := open(t, l2)
			sql := "UPDATE t SET v = 'after' WHERE k % 7 = 0"
			mustRun(t, w2, sql)
			or.exec(t, sql)
			if _, err := w2.Run(nil, func(db *sqlite3.Conn) error {
				_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE)
				return err
			}); err != nil {
				t.Fatalf("TRUNCATE checkpoint: %v", err)
			}
			exp := mustDump(t, or.db)
			if got := dumpConn(t, open(t, l2)); got != exp {
				t.Fatal("a new connection differs from the oracle")
			}
			if got := mustDump(t, durable(t, st.Store, keys, 0)); got != exp {
				t.Fatal("durable state differs from the oracle")
			}
		})
	}
}

// Covers: Commit capture and failure, "The local WAL is on a real disk, and
// tests inject short writes, a full disk, and a failed truncate or fsync"
// (the truncate and the engine's own fsync are in TestFailedTruncationStops)
// and "A commit known not to have entered the durable history never
// reappears ... after every connection closes and the database reopens",
// for each failure of a WAL write or sync before the upload.
func TestLocalWALFailures(t *testing.T) {
	for _, tc := range []string{"frame write", "full disk", "sync"} {
		t.Run(tc, func(t *testing.T) {
			st, keys, o, l, c := setup(t)
			l.fault = func(file, op string, off int64, n int) error {
				switch {
				case file != "wal":
				case tc == "frame write" && op == "write":
					return errInjected
				case tc == "full disk" && op == "write":
					return sqlite3.FULL
				case tc == "sync" && op == "sync":
					return errInjected
				}
				return nil
			}
			st.resetCounts()
			txn, err := exec(c, "INSERT INTO t VALUES (1)")
			l.fault = nil
			if err == nil || txn != 0 || errors.Is(err, sqlite3.BUSY) {
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
