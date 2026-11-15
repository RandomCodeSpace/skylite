package engine

// Tests for "Commit capture and failure" and "Reads" in record 0004.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ncruces/go-sqlite3"
)

// oracle is a plain SQLite database that receives the transactions the
// durable history admitted.
type oracle struct{ db *sqlite3.Conn }

func newOracle(t *testing.T) *oracle {
	t.Helper()
	db, err := sqlite3.Open("file:" + filepath.Join(t.TempDir(), "oracle.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &oracle{db}
}

func (o *oracle) exec(t *testing.T, sql string) {
	t.Helper()
	if err := o.db.Exec(sql); err != nil {
		t.Fatalf("oracle: %s: %v", sql, err)
	}
}

// dump is every table's rows in rowid order.
func dump(db *sqlite3.Conn) (string, error) {
	tables, err := query(db, "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, tbl := range tables {
		rows, err := query(db, fmt.Sprintf("SELECT * FROM %q ORDER BY rowid", tbl))
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s: %d rows %x\n", tbl, len(rows), strings.Join(rows, "\n"))
	}
	return b.String(), nil
}

func mustDump(t testing.TB, db *sqlite3.Conn) string {
	t.Helper()
	d, err := dump(db)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func dumpConn(t *testing.T, c *Conn) string {
	t.Helper()
	var d string
	if _, err := c.Run(nil, func(db *sqlite3.Conn) (err error) {
		d, err = dump(db)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return d
}

// script is a sequence of transactions that exercises what the WAL can hold
// at a commit: spilled frames (a page cache of 10 pages), frames rewritten
// in place and the checksum rewrite that follows, savepoint rollbacks,
// frame numbers reused after a rollback, a WAL restarted by a writer after
// a checkpoint, a TRUNCATE checkpoint, a database that shrinks, VACUUM, and
// transactions that change nothing. commits says whether each one commits.
var script = []struct {
	sql    string
	commit bool
}{
	{"BEGIN; CREATE TABLE t(k INTEGER PRIMARY KEY, v); COMMIT", true},
	{"INSERT INTO t SELECT value, printf('%03000d', value) FROM generate_series(1, 300)", true},
	{"BEGIN; UPDATE t SET v = printf('%02000d', k) WHERE k % 2 = 0; UPDATE t SET v = printf('%01000d', k) WHERE k % 4 = 0; COMMIT", true},
	{"BEGIN; INSERT INTO t SELECT value, printf('%03000d', value) FROM generate_series(1000, 1200); SAVEPOINT s; DELETE FROM t WHERE k < 100; ROLLBACK TO s; RELEASE s; COMMIT", true},
	{"BEGIN; INSERT INTO t SELECT value, printf('%03000d', value) FROM generate_series(2000, 2300); ROLLBACK", false},
	{"INSERT INTO t VALUES (5000, 'after a rollback')", true},
	{"BEGIN; SELECT count(*) FROM t; COMMIT", false},
	{"checkpoint passive", false},
	{"INSERT INTO t VALUES (5001, 'restarts the WAL')", true},
	{"checkpoint truncate", false},
	{"DELETE FROM t WHERE k BETWEEN 1 AND 250", true},
	{"VACUUM", true},
	{"BEGIN IMMEDIATE; COMMIT", false},
	{"UPDATE t SET v = v", false},
	{"BEGIN; SAVEPOINT a; INSERT INTO t VALUES (6000, 'undone'); ROLLBACK TO a; COMMIT", true},
}

func runScript(t *testing.T, c *Conn, sql string) (uint64, error) {
	t.Helper()
	switch sql {
	case "checkpoint passive", "checkpoint truncate":
		mode := sqlite3.CHECKPOINT_PASSIVE
		if sql == "checkpoint truncate" {
			mode = sqlite3.CHECKPOINT_TRUNCATE
		}
		return c.Run(nil, func(db *sqlite3.Conn) error {
			_, _, err := db.WALCheckpoint("main", mode)
			return err
		})
	}
	return c.Run(newID(), func(db *sqlite3.Conn) error {
		if err := db.Exec("PRAGMA cache_size=10"); err != nil {
			return err
		}
		return db.Exec(sql)
	})
}

// Covers: Commit capture and failure, "Exactly one durable commit object
// per intended transaction, and no object for anything else, across WAL
// header syncs, checkpoint syncs, sector padding, spilled frames,
// overwritten frames, savepoint rollback, frame numbers reused after a
// rollback, dead frames past the commit frame and checksum-only rewrites
// after the commit frame. The WAL header sync after each WAL restart is not
// taken for a commit. The commit-frame layout is checked with and without
// powersafe overwrite"; Reads, "writer restart, ... TRUNCATE checkpoints";
// and Bootstrap, "a database created by the engine opens in WAL mode with
// no write to page 1 outside the WAL" (the main file refuses any write
// that is not a checkpoint's).
func TestCaptureOneObjectPerTransaction(t *testing.T) {
	for _, psow := range []bool{true, false} {
		t.Run(fmt.Sprintf("psow=%v", psow), func(t *testing.T) {
			st, keys, o := newDatabase(t, 1024)
			l := lead(t, st, keys, o, testConfig())
			l.psow = psow
			c := open(t, l)
			reader := open(t, l)
			or := newOracle(t)
			want := o.Head
			for i, step := range script {
				txn, err := runScript(t, c, step.sql)
				if err != nil {
					t.Fatalf("step %d, %s: %v", i, step.sql, err)
				}
				if step.commit {
					want++
					if !strings.HasPrefix(step.sql, "checkpoint") {
						or.exec(t, step.sql)
					}
					if txn != want {
						t.Fatalf("step %d, %s: committed at %d, want %d", i, step.sql, txn, want)
					}
				} else if txn != 0 {
					t.Fatalf("step %d, %s: committed at %d, want no commit", i, step.sql, txn)
				}
				if l.Head() != want {
					t.Fatalf("step %d: head %d, want %d", i, l.Head(), want)
				}
				exp := mustDump(t, or.db)
				if got := mustDump(t, durable(t, st.Store, keys, 0)); got != exp {
					t.Fatalf("step %d, %s: durable state differs from the oracle", i, step.sql)
				}
				if got := dumpConn(t, reader); got != exp {
					t.Fatalf("step %d, %s: the leader's reader differs from the oracle", i, step.sql)
				}
			}
			if ids := commitIDs(t, st.Store, keys, o.DB, o.Head+1); uint64(len(ids)) != want-o.Head {
				t.Fatalf("%d objects after the bump, want %d", len(ids), want-o.Head)
			}
		})
	}
}

// Covers: Commit capture and failure, "Automatic checkpointing runs while
// commits are being captured"; Reads, "Automatic checkpointing overlapping
// commit capture leaves the base version and the locally held pages
// correct".
func TestAutoCheckpointDuringCapture(t *testing.T) {
	st, keys, o := newDatabase(t, 1024)
	l := lead(t, st, keys, o, testConfig())
	c, r := open(t, l), open(t, l)
	or := newOracle(t)
	for _, sql := range []string{
		"CREATE TABLE t(k INTEGER PRIMARY KEY, v)",
		"INSERT INTO t SELECT value, printf('%0900d', value) FROM generate_series(1, 1500)",
	} {
		mustRun(t, c, sql)
		or.exec(t, sql)
	}
	base := l.base
	for i := range 40 {
		sql := fmt.Sprintf("UPDATE t SET v = printf('%%0800d', k + %d) WHERE k %% 40 = %d", i, i)
		mustRun(t, c, sql)
		or.exec(t, sql)
	}
	if l.base == base {
		t.Fatal("no WAL restart: automatic checkpointing did not run")
	}
	exp := mustDump(t, or.db)
	if got := dumpConn(t, r); got != exp {
		t.Fatal("leader reader differs from the oracle")
	}
	if got := mustDump(t, durable(t, st.Store, keys, 0)); got != exp {
		t.Fatal("durable state differs from the oracle")
	}
}

// Covers: Reads, "An upload held indefinitely while existing and newly
// opened connections read, both at first initialisation of the
// shared-memory index and with an established one, and ending in success,
// in a known failure and in unknown: no connection sees the transaction
// before its upload has succeeded, and none sees a failed one"; decision
// 5, "Visibility".
func TestUploadHeldReaders(t *testing.T) {
	for _, fresh := range []bool{true, false} {
		for _, end := range []string{"success", "known failure", "unknown"} {
			t.Run(fmt.Sprintf("fresh=%v/%s", fresh, end), func(t *testing.T) {
				st, keys, o := newDatabase(t, 4096)
				l := lead(t, st, keys, o, testConfig())
				w := open(t, l)
				first := "BEGIN; CREATE TABLE t(x); INSERT INTO t VALUES (0); COMMIT"
				before := "0"
				var existing *Conn
				if !fresh {
					mustRun(t, w, first)
					existing = open(t, l)
					if got := strings.Join(queryConn(t, existing, "SELECT x FROM t"), ","); got != before {
						t.Fatalf("before: %s", got)
					}
					first = "INSERT INTO t VALUES (1)"
				} else {
					before = "none"
				}
				held := newGate()
				f := fault{before: held}
				switch end {
				case "known failure":
					f.noEffect = true
				case "unknown":
					f.unknownNone = true
				}
				st.setRule(commitRules(f, fault{noEffect: f.noEffect, unknownNone: f.unknownNone}))
				done := make(chan error)
				go func() {
					_, err := exec(w, first)
					done <- err
				}()
				<-held.reached
				see := func(c *Conn) string {
					var out string
					_, err := c.Run(nil, func(db *sqlite3.Conn) error {
						r, err := query(db, "SELECT x FROM t")
						if err != nil && strings.Contains(err.Error(), "no such table") {
							out = "none"
							return nil
						}
						out = strings.Join(r, ",")
						return err
					})
					if err != nil {
						return "error: " + err.Error()
					}
					return out
				}
				readers := []*Conn{open(t, l)}
				if existing != nil {
					readers = append(readers, existing)
				}
				for _, r := range readers {
					if got := see(r); got != before {
						t.Fatalf("during the upload a reader saw %q, want %q", got, before)
					}
				}
				held.open()
				err := <-done
				readers = append(readers, nil)
				for i, r := range readers {
					if r == nil {
						if end == "unknown" {
							continue
						}
						r = open(t, l)
						readers[i] = r
					}
					got := see(r)
					switch end {
					case "success":
						want := "0"
						if !fresh {
							want = "0,1"
						}
						if err != nil || got != want {
							t.Fatalf("after success: %v, a reader saw %q", err, got)
						}
					case "known failure":
						if !errors.Is(err, ErrNotCommitted) || got != before {
							t.Fatalf("after a known failure: %v, a reader saw %q", err, got)
						}
					case "unknown":
						if !errors.Is(err, ErrUnknown) || !strings.Contains(got, "stopped") {
							t.Fatalf("after unknown: %v, a reader saw %q", err, got)
						}
					}
				}
			})
		}
	}
}

// Covers: Reads, "Reads against the oracle through partial backfill, failed
// page writes during a checkpoint, ..., checkpoints held back by readers,
// writer restart, RESTART and TRUNCATE checkpoints ..., with cache
// eviction, return each page as of the base version, or as written locally
// by a checkpoint since then. A required local page that has been removed
// makes the read fail; it never falls back to older base bytes"; "The size
// reported for the main file is the size of the checkpointed image"; Limits
// and size, "The same with a RESTART checkpoint in place of TRUNCATE shows
// that it does not advance the base version".
func TestReadsThroughCheckpoints(t *testing.T) {
	st, keys, o := newDatabase(t, 1024)
	cfg := testConfig()
	cfg.Cache = NewCache(0) // every base page read goes to storage
	l := lead(t, st, keys, o, cfg)
	w, snap := open(t, l), open(t, l)
	or := newOracle(t)
	apply := func(sql string) {
		t.Helper()
		mustRun(t, w, sql)
		or.exec(t, sql)
	}
	ckpt := func(mode sqlite3.CheckpointMode) error {
		_, err := w.Run(nil, func(db *sqlite3.Conn) error {
			_, _, err := db.WALCheckpoint("main", mode)
			return err
		})
		return err
	}
	check := func(what string) {
		t.Helper()
		exp := mustDump(t, or.db)
		if got := dumpConn(t, open(t, l)); got != exp {
			t.Fatalf("%s: a new connection differs from the oracle", what)
		}
		if got := mustDump(t, durable(t, st.Store, keys, 0)); got != exp {
			t.Fatalf("%s: durable state differs from the oracle", what)
		}
	}
	apply("CREATE TABLE t(k INTEGER PRIMARY KEY, v)")
	apply("INSERT INTO t SELECT value, printf('%0700d', value) FROM generate_series(1, 400)")

	// A reader holds a snapshot; checkpoints cannot pass it.
	var held string
	if _, err := snap.Run(nil, func(db *sqlite3.Conn) (err error) {
		if err := db.Exec("BEGIN"); err != nil {
			return err
		}
		held, err = dump(db)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	apply("UPDATE t SET v = printf('%0600d', k * 2) WHERE k % 3 = 0")
	if err := ckpt(sqlite3.CHECKPOINT_PASSIVE); err != nil {
		t.Fatal(err)
	}
	check("checkpoint held back by a reader")
	if got := dumpConn(t, snap); got != held {
		t.Fatal("the held snapshot changed")
	}
	if _, err := snap.Run(nil, func(db *sqlite3.Conn) error { return db.Exec("COMMIT") }); err != nil {
		t.Fatal(err)
	}
	apply("DELETE FROM t WHERE k > 350")

	// A checkpoint whose page writes fail part of the way.
	writes := 0
	l.fault = func(file, op string, off int64, n int) error {
		if file == "main" && op == "write" {
			if writes++; writes > 3 {
				return errInjected
			}
		}
		return nil
	}
	if err := ckpt(sqlite3.CHECKPOINT_PASSIVE); err == nil || writes <= 3 {
		t.Fatalf("a checkpoint with failing page writes: %v after %d writes", err, writes)
	}
	l.fault = nil
	check("failed checkpoint writes")

	// RESTART backfills everything but does not restart the WAL: the base
	// version stays until the next writer restarts it.
	base := l.base
	if err := ckpt(sqlite3.CHECKPOINT_RESTART); err != nil {
		t.Fatal(err)
	}
	if l.base != base {
		t.Fatalf("a RESTART checkpoint advanced the base version from %d to %d", base, l.base)
	}
	pages := queryConn(t, w, "PRAGMA page_count")[0]
	if got := fmt.Sprint(l.size / 1024); got != pages {
		t.Fatalf("main file holds %s pages after a full checkpoint, the database %s", got, pages)
	}
	check("after RESTART")
	// The WAL is fully backfilled, so new readers read only the main file,
	// whose checkpointed pages are local. Removing them makes reads fail.
	if err := os.Truncate(l.path, 0); err != nil {
		t.Fatal(err)
	}
	if lost, err := l.Open(); err == nil {
		if _, err := lost.Run(nil, func(db *sqlite3.Conn) error {
			_, err := dump(db)
			return err
		}); err == nil {
			t.Fatal("a read served pages whose local copy was removed")
		}
		_ = lost.Close()
	}
	// The next writer restarts the WAL and advances the base version to
	// the last commit before the restart; the pages then come from storage.
	apply("INSERT INTO t VALUES (9000, 'writer restart')")
	if l.base != l.Head()-1 || l.base == base {
		t.Fatalf("base version %d after a writer restart, head %d", l.base, l.Head())
	}
	check("writer restart")
	apply("UPDATE t SET v = 'x' WHERE k < 50")
	if err := ckpt(sqlite3.CHECKPOINT_TRUNCATE); err != nil {
		t.Fatal(err)
	}
	if l.base != l.Head() {
		t.Fatalf("base version %d after TRUNCATE, head %d", l.base, l.Head())
	}
	check("TRUNCATE")
}

// Covers: Commit capture and failure, "Every journal-mode, locking-mode,
// synchronous and alternative-output setting a client can reach through
// SQL (pragmas, ATTACH, VACUUM INTO and any other route the lockdown leaves
// open) is either refused or leaves capture intact"; decision 5, "No SQL
// can bypass capture". This covers what the storage layer refuses itself;
// the SQL policy in internal/sqlpolicy refuses more.
func TestCaptureCannotBeBypassed(t *testing.T) {
	st, keys, _, l, c := setup(t)
	for _, sql := range []string{
		"PRAGMA journal_mode=DELETE", "PRAGMA journal_mode=TRUNCATE", "PRAGMA journal_mode=PERSIST",
		"PRAGMA journal_mode=MEMORY", "PRAGMA journal_mode=OFF", "PRAGMA main.journal_mode=delete",
		"PRAGMA locking_mode=EXCLUSIVE", "PRAGMA synchronous=OFF", "PRAGMA synchronous=NORMAL",
		"PRAGMA synchronous=1", "PRAGMA wal_autocheckpoint=0", "PRAGMA query_only=1",
		"VACUUM INTO '" + filepath.Join(t.TempDir(), "copy.db") + "'",
		"ATTACH 'file:" + l.path + "?vfs=" + l.vfs + "' AS again",
	} {
		if _, err := c.Run(nil, func(db *sqlite3.Conn) error { return db.Exec(sql) }); err == nil {
			t.Errorf("%s: accepted", sql)
		}
	}
	for _, sql := range []string{"PRAGMA journal_mode", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA synchronous"} {
		if _, err := c.Run(nil, func(db *sqlite3.Conn) error { return db.Exec(sql) }); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
		t.Fatalf("durable rows %s", got)
	}
	if got := queryConn(t, c, "PRAGMA synchronous"); got[0] != "2" {
		t.Fatalf("synchronous %v", got)
	}
}

// Covers: Commit capture and failure, "Bootstrap: ... a database already in
// object storage opens the same way. A main-file write that is not part of
// a checkpoint is an error, not a silent local change."
func TestMainFileRefusesOtherWrites(t *testing.T) {
	st, keys, o, l, c := setup(t)
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// A later leader opens the database already in storage, also WAL.
	l2 := lead(t, st, keys, takeOffice(t, st.Store, keys), testConfig())
	c2 := open(t, l2)
	if got := queryConn(t, c2, "PRAGMA journal_mode"); got[0] != "wal" {
		t.Fatalf("journal mode %v", got)
	}
	if got := strings.Join(queryConn(t, c2, "SELECT x FROM t ORDER BY x"), ","); got != "0,1" {
		t.Fatalf("rows %s", got)
	}
	l2.cmu.Lock()
	m := c2.main
	l2.cmu.Unlock()
	if _, err := m.WriteAt(make([]byte, 4096), 4096); err == nil {
		t.Fatal("a main-file write outside a checkpoint succeeded")
	}
	if err := m.Truncate(4096); err == nil {
		t.Fatal("a main-file truncate outside a checkpoint succeeded")
	}
	_ = o
}
