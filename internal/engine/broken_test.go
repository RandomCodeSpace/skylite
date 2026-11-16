package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// overflowSQL overflows SQLite's C stack on this build while it compiles: the
// engine's connections keep SQLite's default limits, and a chain of 332
// operators already broke the stack (record 0007, The C stack).
var overflowSQL = "SELECT 1" + strings.Repeat(" + 1", 999)

// brokenDB is a led database with a table of 200 rows of 900 bytes, a
// writer and an oracle that has seen the same statements.
type brokenDB struct {
	st   *faultStore
	keys format.Keys
	l    *Leader
	w    *Conn
	or   *oracle
}

func brokenSetup(t *testing.T) brokenDB {
	t.Helper()
	st, keys, o := newDatabase(t, 1024)
	l := lead(t, st, keys, o, testConfig())
	w := open(t, l)
	or := newOracle(t)
	for _, sql := range []string{
		"CREATE TABLE t(k INTEGER PRIMARY KEY, v)",
		"INSERT INTO t SELECT value, printf('%0900d', value) FROM generate_series(1, 200)",
	} {
		mustRun(t, w, sql)
		or.exec(t, sql)
	}
	return brokenDB{st, keys, l, w, or}
}

// mustBreak checks that a Run that overflowed the stack discarded only its
// connection.
func mustBreak(t *testing.T, l *Leader, c *Conn, txn uint64, err error) {
	t.Helper()
	if txn != 0 || !errors.Is(err, ErrBroken) || !errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrStopped) || errors.Is(err, ErrUnknown) {
		t.Fatalf("Run: %d, %v; want not committed and broken", txn, err)
	}
	mustBeDiscarded(t, l, c)
}

// mustBeDiscarded checks that the leader serves on and c is gone.
func mustBeDiscarded(t *testing.T, l *Leader, c *Conn) {
	t.Helper()
	if err := l.Err(); err != nil {
		t.Fatalf("the leader stopped after a broken connection: %v", err)
	}
	if _, err := exec(c, "SELECT 1"); !errors.Is(err, ErrNotCommitted) || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("the broken connection ran again: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("closing the broken connection: %v", err)
	}
}

// mustServe checks that the leader serves after a broken connection: a
// write commits and advances Head, a TRUNCATE checkpoint, which needs every
// WAL read lock and the write lock, completes, and both a new connection and
// the durable state match the oracle.
func (d brokenDB) mustServe(t *testing.T) {
	t.Helper()
	l, w, or := d.l, d.w, d.or
	head := l.Head()
	sql := "UPDATE t SET v = 'after' WHERE k % 7 = 0"
	if txn := mustRun(t, w, sql); txn <= head || l.Head() != txn {
		t.Fatalf("the write after the break committed %d with Head %d, from %d", txn, l.Head(), head)
	}
	or.exec(t, sql)
	if _, err := w.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
		_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE)
		return err
	}); err != nil {
		t.Fatalf("TRUNCATE checkpoint after the break: %v", err)
	}
	exp := mustDump(t, or.db)
	if got := dumpConn(t, open(t, l)); got != exp {
		t.Fatal("a new connection differs from the oracle")
	}
	if got := mustDump(t, durable(t, d.st.Store, d.keys, 0)); got != exp {
		t.Fatal("durable state differs from the oracle")
	}
}

// Covers: record 0007, requirement 1, and decision 6 as amended: a stack
// overflow on a connection holding a read snapshot breaks only that
// connection. Its WAL read lock is released, which the TRUNCATE checkpoint
// in mustServe needs.
func TestStackOverflowOnAReader(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	r := open(t, l)
	txn, err := r.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("BEGIN"); err != nil {
			return err
		}
		if _, err := query(db, "SELECT count(*) FROM t"); err != nil {
			return err
		}
		return db.Exec(overflowSQL)
	})
	mustBreak(t, l, r, txn, err)
	d.mustServe(t)
}

// Covers: the same for a connection inside a write transaction that has
// spilled frames to the WAL: its writes are never visible, the write lock is
// released, and the next writer commits over the dead frames.
func TestStackOverflowInAWriteTransaction(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	v, r := open(t, l), open(t, l)
	txn, err := v.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("PRAGMA cache_size=5; BEGIN IMMEDIATE; UPDATE t SET v = 'lost'"); err != nil {
			return err
		}
		return db.Exec(overflowSQL)
	})
	mustBreak(t, l, v, txn, err)
	if got := queryConn(t, r, "SELECT count(*) FROM t WHERE v = 'lost'"); got[0] != "0" {
		t.Fatalf("%s rows of the broken transaction are visible", got[0])
	}
	d.mustServe(t)
}

// Covers: the same while maintenance waits for the connection's Run to
// return: the connection holds a read snapshot, maintenance has asked it to
// give the snapshot up, and its statement overflows the stack at run time,
// in a LIKE, before SQLite checks the interrupt. Maintenance's TRUNCATE
// checkpoint completes only once the read lock is released.
func TestStackOverflowDuringMaintenance(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	r := open(t, l)
	like := "SELECT (maint() || '" + strings.Repeat("a", 10000) + "b') LIKE '" + strings.Repeat("%a", 10000) + "'"
	txn, err := r.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.CreateFunction("maint", 0, 0, func(ctx sqlite3.Context, _ ...sqlite3.Value) {
			startMaintenance(l)
			for {
				l.cmu.Lock()
				asked := r.maint
				l.cmu.Unlock()
				if asked {
					break
				}
				runtime.Gosched()
			}
			ctx.ResultText("")
		}); err != nil {
			return err
		}
		if err := db.Exec("BEGIN"); err != nil {
			return err
		}
		if _, err := query(db, "SELECT count(*) FROM t"); err != nil {
			return err
		}
		_, err := query(db, like)
		return err
	})
	mustBreak(t, l, r, txn, err)
	waitMaintenance(t, l)
	d.mustServe(t)
}

// Covers: repeated breaks neither stop the leader nor use the log: 200
// overflows leave Head where it was, create nothing in storage, and leave the
// Go heap, the process's resident memory and its open files where they were
// after the first 20.
func TestRepeatedStackOverflows(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	head := l.Head()
	d.st.resetCounts()
	var heap, rss, fds int64
	for i := range 200 {
		if i == 20 {
			heap, rss, fds = resources(t)
		}
		c, err := l.Open(context.Background())
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		txn, err := exec(c, overflowSQL)
		mustBreak(t, l, c, txn, err)
	}
	h, r, f := resources(t)
	t.Logf("after 20 and 200 breaks: heap %d and %d bytes, resident %d and %d bytes, %d and %d open files", heap, h, rss, r, fds, f)
	if h-heap > 8<<20 || r-rss > 32<<20 || f > fds {
		t.Fatal("a broken connection's memory or files outlive it")
	}
	if _, creates, _ := d.st.counts(); creates != 0 || l.Head() != head {
		t.Fatalf("%d creates and Head %d from %d during the breaks", creates, l.Head(), head)
	}
	d.mustServe(t)
}

// resources reports the Go heap after a collection, the process's resident
// memory and its open files.
func resources(t *testing.T) (heap, rss, fds int64) {
	t.Helper()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Skipf("no /proc: %v", err)
	}
	var size, pages int64
	if _, err := fmt.Sscan(string(b), &size, &pages); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return int64(m.HeapAlloc), pages * int64(os.Getpagesize()), int64(len(ents)) //nolint:gosec // a heap size
}

// Covers: a Run whose function reports the connection broken, as a policy
// that contains an engine panic itself does, discards the connection the
// same way.
func TestRunReportsBroken(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	v := open(t, l)
	rolledBack := false
	txn, err := v.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("BEGIN IMMEDIATE; UPDATE t SET v = 'lost'"); err != nil {
			return err
		}
		// sqlite3_close would roll the transaction back and call this.
		db.RollbackHook(func() { rolledBack = true })
		return fmt.Errorf("contained: %w", ErrBroken)
	})
	mustBreak(t, l, v, txn, err)
	if rolledBack {
		t.Fatal("SQLite ran on the broken connection: it rolled the transaction back")
	}
	d.mustServe(t)
}

// Covers: a connection that breaks after its Run committed: the commit
// stands, and the Run still reports the connection broken and discarded
// (record 0007, requirement 1).
func TestStackOverflowAfterACommit(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	v := open(t, l)
	sql := "INSERT INTO t VALUES (1000, 'committed')"
	txn, err := v.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec(sql); err != nil {
			return err
		}
		return db.Exec(overflowSQL)
	})
	if txn == 0 || txn != l.Head() || !errors.Is(err, ErrBroken) || errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrUnknown) || errors.Is(err, ErrStopped) {
		t.Fatalf("Run: %d, %v; want committed with Head %d, then broken", txn, err, l.Head())
	}
	d.or.exec(t, sql)
	mustBeDiscarded(t, l, v)
	d.mustServe(t)
}

// Covers: decision 6 as amended: a broken connection that release cannot
// free stops the leader, as before, and release reports a connection it
// cannot reach instead of panicking.
func TestFailedReleaseStops(t *testing.T) {
	if release(nil) || release(new(sqlite3.Conn)) {
		t.Fatal("release freed a connection without an SQLite instance")
	}
	d := brokenSetup(t)
	l := d.l
	v := open(t, l)
	db := v.db
	t.Cleanup(func() { _ = db.Close() })
	txn, err := v.Run(context.Background(), newID(), func(*sqlite3.Conn) error {
		v.db = new(sqlite3.Conn) // release cannot free this one
		panic("injected")
	})
	if txn != 0 || !errors.Is(err, ErrStopped) || !errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrBroken) || l.Err() == nil {
		t.Fatalf("Run: %d, %v, leader %v; want not committed and the leader stopped", txn, err, l.Err())
	}
	if _, err := exec(d.w, "SELECT 1"); !errors.Is(err, ErrStopped) {
		t.Fatalf("another connection served after a failed release: %v", err)
	}
	// The same when the function reports the connection broken.
	d = brokenSetup(t)
	v = open(t, d.l)
	db2 := v.db
	t.Cleanup(func() { _ = db2.Close() })
	txn, err = v.Run(context.Background(), newID(), func(*sqlite3.Conn) error {
		v.db = new(sqlite3.Conn)
		return fmt.Errorf("the policy found the connection broken: %w", ErrBroken)
	})
	if txn != 0 || !errors.Is(err, ErrStopped) || !errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrBroken) || d.l.Err() == nil {
		t.Fatalf("Run reporting broken: %d, %v, leader %v; want not committed, not broken, and the leader stopped", txn, err, d.l.Err())
	}
}

// Covers: a Run that commits and then breaks, whose connection release
// cannot free, reports its commit and not a broken connection: the leader
// stopped instead of discarding it.
func TestFailedReleaseAfterACommit(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	v := open(t, l)
	db := v.db
	t.Cleanup(func() { _ = db.Close() })
	txn, err := v.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("INSERT INTO t VALUES (1000, 'committed')"); err != nil {
			return err
		}
		v.db = new(sqlite3.Conn) // release cannot free this one
		return fmt.Errorf("the policy found the connection broken: %w", ErrBroken)
	})
	if txn == 0 || errors.Is(err, ErrBroken) || errors.Is(err, ErrNotCommitted) || l.Err() == nil {
		t.Fatalf("Run: %d, %v, leader %v; want committed, not broken, and the leader stopped", txn, err, l.Err())
	}
}

// Covers: release closes the broken connection's files at once, not at a
// later garbage collection: its main file, WAL, shared-memory index and
// temporary files, whose bytes leave the temporary-file budget.
func TestStackOverflowReleasesFiles(t *testing.T) {
	d := brokenSetup(t)
	l := d.l
	before := dirFiles(t, l.dir)
	v := open(t, l)
	var during []string
	var used int64
	txn, err := v.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("PRAGMA cache_size=5; PRAGMA temp_store=FILE; BEGIN IMMEDIATE; UPDATE t SET v = 'lost'; CREATE TEMP TABLE x AS SELECT * FROM t; SAVEPOINT s; UPDATE t SET v = 'lost2'"); err != nil {
			return err
		}
		during, used = dirFiles(t, l.dir), l.tempUsed.Load()
		return db.Exec(overflowSQL)
	})
	// Before mustBreak, and before any collection, whose finalizers would
	// close leaked files.
	after, usedAfter := dirFiles(t, l.dir), l.tempUsed.Load()
	mustBreak(t, l, v, txn, err)
	if used == 0 || !slices.ContainsFunc(during, func(f string) bool { return strings.Contains(f, "temp-") }) || len(during) < len(before)+3 {
		t.Fatalf("the broken connection held no temporary files (%d bytes) or too few files: %q, from %q", used, during, before)
	}
	if usedAfter != 0 || !slices.Equal(after, before) {
		t.Fatalf("after the release: %d bytes of temporary files, open files %q, from %q", usedAfter, after, before)
	}
	if m, err := filepath.Glob(filepath.Join(l.dir, "temp-*")); err != nil || len(m) != 0 {
		t.Fatalf("temporary files left: %q, %v", m, err)
	}
}

// dirFiles lists, sorted, the files under dir that the process holds open.
func dirFiles(t *testing.T, dir string) []string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("no /proc: %v", err)
	}
	var out []string
	for _, e := range ents {
		if f, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(f, dir+"/") {
			out = append(out, f)
		}
	}
	slices.Sort(out)
	return out
}
