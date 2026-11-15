package engine

// Tests for "Limits and size" in record 0004, and the maintenance of
// decision 10 up to the point where compaction would begin.

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// waitMaintenance waits until no maintenance runs.
func waitMaintenance(t *testing.T, l *Leader) {
	t.Helper()
	stop := time.AfterFunc(30*time.Second, func() { panic("maintenance did not finish within 30s") })
	defer stop.Stop()
	l.cmu.Lock()
	defer l.cmu.Unlock()
	for l.maintaining && l.stopped == nil {
		l.cond.Wait()
	}
}

func startMaintenance(l *Leader) {
	l.cmu.Lock()
	l.startMaintenance()
	l.cmu.Unlock()
}

// Covers: Limits and size, "The local WAL bound is enforced while a
// transaction's frames spill, including at the commit frame: the
// transaction rolls back, nothing is uploaded, and the next transaction
// works"; decision 10, "The local WAL bound is enforced while frames
// spill".
func TestWALBound(t *testing.T) {
	const big = "INSERT INTO t SELECT value, printf('%0900d', value) FROM generate_series(1, 200)"
	// Measure the WAL a transaction needs, on a leader with room.
	st, keys, o := newDatabase(t, 1024)
	l := lead(t, st, keys, o, testConfig())
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)")
	mustRun(t, c, big)
	l.cmu.Lock()
	end := l.wal.end
	l.cmu.Unlock()

	for _, tc := range []struct {
		name  string
		bound int64
		sql   string
	}{
		{"while spilling", end / 2, "PRAGMA cache_size=5; " + big},
		{"at the commit frame", end - 1, big},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, keys, o := newDatabase(t, 1024)
			cfg := testConfig()
			cfg.WALBound = tc.bound
			l := lead(t, st, keys, o, cfg)
			c := open(t, l)
			mustRun(t, c, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)")
			st.resetCounts()
			txn, err := c.Run(newID(), func(db *sqlite3.Conn) error {
				err := db.Exec(tc.sql)
				if !db.GetAutocommit() {
					return fmt.Errorf("transaction left open: %w", err)
				}
				return err
			})
			if txn != 0 || !errors.Is(err, ErrNotCommitted) {
				t.Fatalf("over the bound: %d, %v", txn, err)
			}
			if _, creates, _ := st.counts(); creates != 0 {
				t.Fatalf("%d creates over the WAL bound", creates)
			}
			waitMaintenance(t, l)
			mustRun(t, c, "INSERT INTO t VALUES (1, 'next')")
			if got := mustQuery(t, durable(t, st.Store, keys, 0), "SELECT count(*) FROM t"); got[0] != "1" {
				t.Fatalf("durable rows %v", got)
			}
		})
	}
	// A transaction that alone passes the bound is too large.
	st, keys, o = newDatabase(t, 1024)
	cfg := testConfig()
	cfg.WALBound = 64 << 10
	l = lead(t, st, keys, o, cfg)
	c = open(t, l)
	mustRun(t, c, "CREATE TABLE t(v)")
	// The first attempt follows another commit in the WAL, so the bound
	// refuses it as retryable; after maintenance resets the WAL, it is the
	// only transaction there, and too large.
	if _, err := exec(c, "INSERT INTO t VALUES (zeroblob(200000))"); !errors.Is(err, ErrRetry) {
		t.Fatalf("a transaction over the WAL bound: %v", err)
	}
	waitMaintenance(t, l)
	if _, err := exec(c, "INSERT INTO t VALUES (zeroblob(200000))"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a transaction larger than the WAL bound: %v", err)
	}
}

// Covers: Limits and size, "The WAL grows under a long reader on the
// leader ... The leader refuses client writes at the bounds; once the
// reader ends, the engine's own TRUNCATE checkpoint resets the WAL, the
// base version advances, ... with client writes refused throughout; then
// writes resume"; Interleavings, "Maintenance while writes are refused";
// decision 10, steps 1, 5, 6, 7 (up to compaction) and 8, with steps 1 and
// 5 as ADR 0005 amends them: new reads are refused too, and running ones
// end.
func TestMaintenanceAfterLongReader(t *testing.T) {
	st, keys, o := newDatabase(t, 1024)
	cfg := testConfig()
	cfg.WALBound = 512 << 10
	l := lead(t, st, keys, o, cfg)
	w, reader := open(t, l), open(t, l)
	or := newOracle(t)
	apply := func(sql string) error {
		_, err := exec(w, sql)
		if err == nil {
			or.exec(t, sql)
		}
		return err
	}
	if err := apply("CREATE TABLE t(k INTEGER PRIMARY KEY, v)"); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Run(nil, func(db *sqlite3.Conn) error {
		if err := db.Exec("BEGIN"); err != nil {
			return err
		}
		_, err := query(db, "SELECT count(*) FROM t")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The reader holds every checkpoint back; the WAL grows to its bound.
	truncating := newGate()
	l.fault = func(file, op string, off int64, n int) error {
		if file == "wal" && op == "truncate" && off == 0 {
			truncating.wait()
		}
		return nil
	}
	var refused error
	for i := 0; refused == nil; i++ {
		refused = apply(fmt.Sprintf("INSERT INTO t VALUES (%d, printf('%%0900d', %d))", i, i))
		if i > 1000 {
			t.Fatal("the WAL bound never refused a write")
		}
	}
	if !errors.Is(refused, ErrRetry) || !errors.Is(refused, ErrNotCommitted) {
		t.Fatalf("at the WAL bound: %v", refused)
	}
	<-truncating.reached
	// Maintenance holds the TRUNCATE checkpoint: client writes are refused.
	if _, err := exec(open(t, l), "INSERT INTO t VALUES (-1, 'refused')"); !errors.Is(err, ErrRetry) {
		t.Fatalf("a write during maintenance: %v", err)
	}
	// So are reads, from admission closing to readmission (ADR 0005's
	// amendment of decision 10, steps 1 and 5).
	if _, err := open(t, l).Run(nil, func(db *sqlite3.Conn) error {
		_, err := query(db, "SELECT count(*) FROM t")
		return err
	}); !errors.Is(err, ErrRetry) || !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("a read during maintenance: %v", err)
	}
	truncating.open()
	waitMaintenance(t, l)
	l.fault = nil
	if l.base != l.Head() {
		t.Fatalf("base version %d after the TRUNCATE checkpoint, head %d", l.base, l.Head())
	}
	if _, err := reader.Run(nil, func(db *sqlite3.Conn) error { return nil }); !errors.Is(err, ErrRetry) {
		t.Fatalf("the long reader after maintenance: %v, want rolled back", err)
	}
	if err := apply("INSERT INTO t VALUES (100000, 'resumed')"); err != nil {
		t.Fatalf("writes did not resume: %v", err)
	}
	exp := mustDump(t, or.db)
	if got := mustDump(t, durable(t, st.Store, keys, 0)); got != exp {
		t.Fatal("durable state differs from the oracle")
	}
	if got := dumpConn(t, open(t, l)); got != exp {
		t.Fatal("the leader differs from the oracle")
	}
}

// Covers: Limits and size, "Maintenance with an idle open write
// transaction that holds the writer lock, and with a writer queued for the
// lock before admission closed: each is rolled back and its client told
// not committed with a retryable reason, the TRUNCATE checkpoint
// completes, and maintenance finishes, each within a time bound. A commit
// already in its upload when admission closes completes or fails by the
// normal rules, and a commit that reaches its commit point after creates
// were stopped fails there with nothing sent to storage. No acknowledged
// transaction is lost and no client is told not committed for a
// transaction in the history"; Interleavings, "Maintenance with an
// admitted writer (fourth review)"; decision 10, steps 2, 3 and 4.
func TestMaintenanceRollsBackWriters(t *testing.T) {
	t.Run("idle and queued writers", func(t *testing.T) {
		st, keys, _, l, w := setup(t)
		idle, queued := open(t, l), open(t, l)
		if _, err := idle.Run(nil, func(db *sqlite3.Conn) error {
			return db.Exec("BEGIN IMMEDIATE; INSERT INTO t VALUES (10)")
		}); err != nil {
			t.Fatal(err)
		}
		started := make(chan struct{})
		queuedDone := make(chan error)
		go func() {
			_, err := queued.Run(newID(), func(db *sqlite3.Conn) error {
				close(started)
				return db.Exec("BEGIN IMMEDIATE; INSERT INTO t VALUES (20); COMMIT")
			})
			queuedDone <- err
		}()
		<-started
		startMaintenance(l)
		if err := <-queuedDone; !errors.Is(err, ErrNotCommitted) || !errors.Is(err, ErrRetry) {
			t.Fatalf("queued writer: %v", err)
		}
		waitMaintenance(t, l)
		if _, err := idle.Run(nil, func(*sqlite3.Conn) error { return nil }); !errors.Is(err, ErrNotCommitted) || !errors.Is(err, ErrRetry) {
			t.Fatalf("idle writer: %v", err)
		}
		if l.base != l.Head() {
			t.Fatalf("base %d, head %d: the TRUNCATE checkpoint did not complete", l.base, l.Head())
		}
		mustRun(t, w, "INSERT INTO t VALUES (1)")
		if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
			t.Fatalf("durable rows %s", got)
		}
	})
	t.Run("commit in its upload", func(t *testing.T) {
		st, keys, _, l, w := setup(t)
		held := newGate()
		st.setRule(commitRules(fault{before: held}, fault{}))
		done := make(chan error)
		go func() {
			_, err := exec(w, "INSERT INTO t VALUES (1)")
			done <- err
		}()
		<-held.reached
		startMaintenance(l)
		held.open()
		if err := <-done; err != nil {
			t.Fatalf("the commit in its upload: %v", err)
		}
		waitMaintenance(t, l)
		if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
			t.Fatalf("durable rows %s", got)
		}
	})
	t.Run("commit point after creates stopped", func(t *testing.T) {
		st, keys, _, l, w := setup(t)
		l.cmu.Lock()
		l.noCreates = true
		l.cmu.Unlock()
		st.resetCounts()
		if _, err := exec(w, "INSERT INTO t VALUES (1)"); !errors.Is(err, ErrNotCommitted) || !errors.Is(err, ErrRetry) {
			t.Fatalf("commit after creates stopped: %v", err)
		}
		if _, creates, _ := st.counts(); creates != 0 {
			t.Fatalf("%d creates after creates stopped", creates)
		}
		l.cmu.Lock()
		l.noCreates = false
		l.cmu.Unlock()
		mustRun(t, w, "INSERT INTO t VALUES (2)")
		if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,2" {
			t.Fatalf("durable rows %s", got)
		}
	})
}

// Covers: Limits and size, "The tail bound counts epoch bumps": a log tail
// filled by takeovers refuses writes and starts maintenance. Clearing it
// needs a compaction, which is later work, so writes stay refused here.
func TestTailBoundCountsBumps(t *testing.T) {
	st, keys, _ := newDatabase(t, 4096)
	takeOffice(t, st.Store, keys)
	o := takeOffice(t, st.Store, keys) // bumps at 2, 3 and 4: a tail of 4 above manifest 0
	cfg := testConfig()
	cfg.TailBound = o.Head
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	if _, err := exec(c, "CREATE TABLE t(x)"); !errors.Is(err, ErrRetry) {
		t.Fatalf("commit at the tail bound: %v", err)
	}
	waitMaintenance(t, l)
	if _, err := exec(c, "CREATE TABLE t(x)"); !errors.Is(err, ErrRetry) || !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("commit after maintenance without compaction: %v", err)
	}
	if ids := commitIDs(t, st.Store, keys, o.DB, o.Head+1); len(ids) != 0 {
		t.Fatalf("objects above the tail bound: %v", ids)
	}
}

// Covers: Limits and size, "A transaction above the maximum size fails
// with a clear error and leaves nothing in storage. VACUUM on a database
// above the maximum commit size fails that way." The limit is lowered for
// the default run; SKYLITE_SOAK=1 runs it at 1 GiB.
func TestTransactionTooLarge(t *testing.T) {
	limit, blob := int64(256<<10), "zeroblob(300000)"
	if os.Getenv("SKYLITE_SOAK") == "" {
		old := maxCommitSize
		maxCommitSize = limit
		t.Cleanup(func() { maxCommitSize = old })
	} else {
		limit, blob = maxCommitSize, "zeroblob(600000000)), (zeroblob(600000000)"
	}
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	if os.Getenv("SKYLITE_SOAK") != "" {
		cfg.WALBound, cfg.MaxMemory = 3*limit, 4<<30
	}
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(v)")
	st.resetCounts()
	_, err := exec(c, "INSERT INTO t VALUES ("+blob+")")
	if !errors.Is(err, ErrTooLarge) || !errors.Is(err, ErrNotCommitted) || !strings.Contains(err.Error(), "commit object") {
		t.Fatalf("an oversized transaction: %v", err)
	}
	if _, creates, _ := st.counts(); creates != 0 {
		t.Fatalf("%d creates for an oversized transaction", creates)
	}
	if os.Getenv("SKYLITE_SOAK") != "" {
		return
	}
	// A database above the limit, built in commits below it: VACUUM
	// rewrites it in one transaction.
	for range 12 {
		mustRun(t, c, "INSERT INTO t VALUES (zeroblob(60000))")
	}
	mustRun(t, c, "DELETE FROM t WHERE rowid % 2 = 0")
	st.resetCounts()
	if _, err := exec(c, "VACUUM"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("VACUUM above the limit: %v", err)
	}
	if _, creates, _ := st.counts(); creates != 0 {
		t.Fatalf("%d creates for VACUUM", creates)
	}
	if got := mustQuery(t, durable(t, st.Store, keys, 0), "SELECT count(*) FROM t"); got[0] != "6" {
		t.Fatalf("durable rows %v", got)
	}
}

// Covers: Invariants, "Transaction numbers and epochs at their maximum
// value stop the database instead of wrapping" (for the leader, whose
// epoch is fixed, the transaction number).
func TestMaxTransactionNumberStops(t *testing.T) {
	_, _, _, l, c := setup(t)
	l.cmu.Lock()
	l.head = math.MaxUint64
	l.cmu.Unlock()
	if _, err := exec(c, "INSERT INTO t VALUES (1)"); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("commit at the last transaction number: %v", err)
	}
	if !errors.Is(l.Err(), ErrStopped) {
		t.Fatal("the leader did not stop at the last transaction number")
	}
}

// Covers: Commit outcomes, "A client is told exactly one of committed, not
// committed or unknown, and never not committed for a commit that is in the
// history", for a Run whose commit completed before maintenance interrupted
// the rest of it.
func TestMaintenanceAfterCommitInSameRun(t *testing.T) {
	st, keys, _, l, c := setup(t)
	txn, err := c.Run(newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("INSERT INTO t VALUES (1)"); err != nil {
			return err
		}
		startMaintenance(l)
		// Runs until maintenance interrupts it.
		_, err := query(db, "SELECT count(*) FROM generate_series(1, 1000000000000)")
		return err
	})
	if txn == 0 || errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Run: %d, %v; want the commit reported, not \"not committed\"", txn, err)
	}
	if err == nil {
		t.Fatal("the interrupted statement's error was lost")
	}
	waitMaintenance(t, l)
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
		t.Fatalf("durable rows %s", got)
	}
}
