package engine

// Experiments on the SQLite-side assumptions of decision 5 of record 0004,
// run against SQLite 3.54.1 through ncruces/go-sqlite3 v0.37.0 and its own
// operating-system VFS, wrapped only to record what SQLite asks of its
// files. They check SQLite, not this package: the engine's own tests check
// the engine against the behaviour these pin down.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"
)

// recEvent is one call SQLite made on a recorded file.
type recEvent struct {
	file string // "main", "wal" or "other"
	op   string // "write", "sync", "truncate", "read", "delete"
	off  int64
	n    int
	// For a 24-byte write at a frame boundary of the WAL: the frame's page
	// number and its commit field (database size, 0 if not a commit).
	pgno, commit uint32
}

// recVFS wraps ncruces's operating-system VFS and records calls. fail, if
// set, may fail any call.
type recVFS struct {
	name string
	ps   int64 // page size, to find frame boundaries
	psow bool  // what the main file reports for powersafe overwrite

	mu     sync.Mutex
	events []recEvent
	fail   func(e recEvent) error
}

var recSeq atomic.Int64

func newRecVFS(t *testing.T, ps int64) *recVFS {
	v := &recVFS{name: fmt.Sprintf("rec-%d", recSeq.Add(1)), ps: ps, psow: true}
	vfs.Register(v.name, v)
	t.Cleanup(func() { vfs.Unregister(v.name) })
	return v
}

func (v *recVFS) record(e recEvent) error {
	v.mu.Lock()
	v.events = append(v.events, e)
	f := v.fail
	v.mu.Unlock()
	if f != nil {
		return f(e)
	}
	return nil
}

func (v *recVFS) take() []recEvent {
	v.mu.Lock()
	defer v.mu.Unlock()
	e := v.events
	v.events = nil
	return e
}

func (v *recVFS) base() vfs.VFSFilename { return vfs.Find("").(vfs.VFSFilename) }

func (v *recVFS) Open(string, vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	return nil, 0, sqlite3.CANTOPEN
}

func (v *recVFS) OpenFilename(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	f, flags, err := v.base().OpenFilename(name, flags)
	if err != nil {
		return nil, flags, err
	}
	kind := "other"
	switch {
	case flags&vfs.OPEN_MAIN_DB != 0:
		kind = "main"
	case flags&vfs.OPEN_WAL != 0:
		kind = "wal"
	}
	return &recFile{File: f, v: v, kind: kind}, flags, nil
}

func (v *recVFS) Delete(name string, dirSync bool) error {
	kind := "other"
	if strings.HasSuffix(name, "-wal") {
		kind = "wal"
	}
	if err := v.record(recEvent{file: kind, op: "delete"}); err != nil {
		return err
	}
	return v.base().Delete(name, dirSync)
}

func (v *recVFS) Access(name string, flags vfs.AccessFlag) (bool, error) {
	return v.base().Access(name, flags)
}

func (v *recVFS) FullPathname(name string) (string, error) { return v.base().FullPathname(name) }

type recFile struct {
	vfs.File
	v    *recVFS
	kind string
}

func (f *recFile) WriteAt(p []byte, off int64) (int, error) {
	e := recEvent{file: f.kind, op: "write", off: off, n: len(p)}
	if f.kind == "wal" && len(p) == 24 && off >= 32 && (off-32)%(f.v.ps+24) == 0 {
		e.pgno = binary.BigEndian.Uint32(p)
		e.commit = binary.BigEndian.Uint32(p[4:])
	}
	if err := f.v.record(e); err != nil {
		return 0, err
	}
	return f.File.WriteAt(p, off)
}

func (f *recFile) Sync(flags vfs.SyncFlag) error {
	if err := f.v.record(recEvent{file: f.kind, op: "sync"}); err != nil {
		return err
	}
	return f.File.Sync(flags)
}

func (f *recFile) Truncate(size int64) error {
	if err := f.v.record(recEvent{file: f.kind, op: "truncate", off: size}); err != nil {
		return err
	}
	return f.File.Truncate(size)
}

func (f *recFile) SharedMemory() vfs.SharedMemory {
	if s, ok := f.File.(vfs.FileSharedMemory); ok {
		return s.SharedMemory()
	}
	return nil
}

func (f *recFile) DeviceCharacteristics() vfs.DeviceCharacteristic {
	d := f.File.DeviceCharacteristics()
	if f.kind == "main" && !f.v.psow {
		d &^= vfs.IOCAP_POWERSAFE_OVERWRITE
	}
	return d
}

// openRec opens a WAL-mode database at a new path through v, with the given
// synchronous setting, and returns it and its path.
func openRec(t *testing.T, v *recVFS, sync string) (*sqlite3.Conn, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.db")
	return reopenRec(t, v, path, sync), path
}

func reopenRec(t *testing.T, v *recVFS, path, sync string) *sqlite3.Conn {
	t.Helper()
	c, err := sqlite3.Open("file:" + path + "?vfs=" + v.name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	mustExec(t, c, fmt.Sprintf("PRAGMA page_size=%d; PRAGMA journal_mode=wal; PRAGMA synchronous=%s; PRAGMA wal_autocheckpoint=0", v.ps, sync))
	return c
}

func mustExec(t *testing.T, c *sqlite3.Conn, sql string) {
	t.Helper()
	if err := c.Exec(sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func queryInt(t *testing.T, c *sqlite3.Conn, sql string) int64 {
	t.Helper()
	s, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer func() { _ = s.Close() }()
	if !s.Step() {
		t.Fatalf("%s: no row: %v", sql, s.Err())
	}
	return s.ColumnInt64(0)
}

// walSyncs returns, for each WAL sync in evs, the index of the last WAL
// frame-header write before it and whether that frame was a commit frame.
func walSyncs(evs []recEvent) (syncs int, afterCommit int) {
	lastCommit := false
	for _, e := range evs {
		if e.file != "wal" {
			continue
		}
		switch {
		case e.op == "write" && e.n == 24:
			lastCommit = e.commit != 0
		case e.op == "sync":
			syncs++
			if lastCommit {
				afterCommit++
			}
		}
	}
	return syncs, afterCommit
}

// Experiment: decision 5, capture point. With synchronous=FULL, SQLite syncs
// the WAL exactly once per commit, after the commit frame and before COMMIT
// returns; under NORMAL it does not sync at commit at all.
func TestExperimentOneWALSyncPerCommit(t *testing.T) {
	for _, mode := range []string{"FULL", "NORMAL"} {
		v := newRecVFS(t, 4096)
		c, _ := openRec(t, v, mode)
		mustExec(t, c, "CREATE TABLE t(x)") // first commit also writes the WAL header
		v.take()
		for i := range 5 {
			mustExec(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
			evs := v.take()
			syncs, after := walSyncs(evs)
			want := 1
			if mode == "NORMAL" {
				want = 0
			}
			if syncs != want || after != want {
				t.Fatalf("%s commit %d: %d WAL syncs, %d after a commit frame, want %d: %v", mode, i, syncs, after, want, evs)
			}
		}
	}
}

// Experiment: decision 5, "a transaction that changes no page creates no
// commit object". A read-only transaction, an empty write transaction, a
// write that matches no row and a savepoint rolled back before any write
// leave no frame and no sync.
func TestExperimentNoChangeWritesNoFrame(t *testing.T) {
	v := newRecVFS(t, 4096)
	c, _ := openRec(t, v, "FULL")
	mustExec(t, c, "CREATE TABLE t(x); INSERT INTO t VALUES (1)")
	for _, sql := range []string{
		"BEGIN; SELECT * FROM t; COMMIT",
		"BEGIN; COMMIT",
		"BEGIN IMMEDIATE; COMMIT",
		"BEGIN EXCLUSIVE; COMMIT",
		"UPDATE t SET x = 2 WHERE x = 99",
		"DELETE FROM t WHERE x = 99",
		"BEGIN; SAVEPOINT a; ROLLBACK TO a; COMMIT",
	} {
		v.take()
		mustExec(t, c, sql)
		for _, e := range v.take() {
			if e.file == "wal" && (e.op == "write" || e.op == "sync") {
				t.Errorf("%s: WAL %s at %d", sql, e.op, e.off)
			}
		}
	}
	// An update to the same value does not even make the page writable:
	// SQLite compares the cell and skips the write.
	v.take()
	mustExec(t, c, "UPDATE t SET x = x")
	if syncs, _ := walSyncs(v.take()); syncs != 0 {
		t.Errorf("same-value update: %d syncs", syncs)
	}
	// A transaction that made pages writable and then undid its change
	// still commits (inference in decision 5, checked here): its pages
	// are written back unchanged, with one commit frame and one sync.
	mustExec(t, c, "BEGIN; SAVEPOINT a; INSERT INTO t VALUES (3); ROLLBACK TO a; COMMIT")
	if syncs, after := walSyncs(v.take()); syncs != 1 || after != 1 {
		t.Errorf("undone change: %d syncs, %d after a commit frame", syncs, after)
	}
}

// Experiment: what arrives before the commit sync of a large transaction
// that spills the page cache, and with savepoints: spilled frames (no
// commit field), frames rewritten in place, and only then the commit frame
// and its sync. The sync always follows the transaction's single commit
// frame, and no frame of the transaction follows the sync except, without
// powersafe overwrite, the rest of the padding.
func TestExperimentSpillAndSavepoints(t *testing.T) {
	for _, psow := range []bool{true, false} {
		v := newRecVFS(t, 1024)
		v.psow = psow
		c, _ := openRec(t, v, "FULL")
		mustExec(t, c, "PRAGMA cache_size=10; CREATE TABLE t(x)")
		v.take()
		mustExec(t, c, `BEGIN;
			INSERT INTO t SELECT randomblob(900) FROM generate_series(1, 200);
			SAVEPOINT s;
			INSERT INTO t SELECT randomblob(900) FROM generate_series(1, 100);
			ROLLBACK TO s;
			UPDATE t SET x = randomblob(900) WHERE rowid % 3 = 0;
			COMMIT`)
		evs := v.take()
		var syncs, inPlace, afterSync int
		commitAt := map[int64]bool{}
		synced := false
		for _, e := range evs {
			if e.file != "wal" {
				continue
			}
			switch {
			case e.op == "sync":
				syncs++
				synced = true
			case e.op == "write" && synced:
				afterSync++
			case e.op == "write" && e.n == 24 && e.commit != 0:
				commitAt[e.off] = true // a checksum rewrite writes it again
			}
		}
		// Count in-place rewrites: a page-data write not preceded by the
		// header write of its own frame.
		var prev recEvent
		for _, e := range evs {
			if e.file == "wal" && e.op == "write" && e.n == 1024 && !(prev.n == 24 && prev.off+24 == e.off) {
				inPlace++
			}
			if e.file == "wal" {
				prev = e
			}
		}
		if syncs != 1 {
			t.Errorf("psow %v: %d WAL syncs, want 1", psow, syncs)
		}
		// Padding frames are commit frames too: one plus the padding.
		if commits := len(commitAt); psow && commits != 1 || !psow && commits < 1 {
			t.Errorf("psow %v: commit frames at %d offsets", psow, commits)
		}
		if inPlace == 0 {
			t.Errorf("psow %v: no frame was rewritten in place; the experiment does not cover it", psow)
		}
		if psow && afterSync != 0 || !psow && afterSync > 1 {
			t.Errorf("psow %v: %d WAL writes after the sync", psow, afterSync)
		}
		if got := queryInt(t, c, "SELECT count(*) FROM t"); got != 200 {
			t.Errorf("psow %v: %d rows", psow, got)
		}
	}
}

// Experiment: the WAL header sync after a WAL restart is not a commit sync:
// it comes after the header write at offset 0 and before any frame.
func TestExperimentRestartHeaderSync(t *testing.T) {
	v := newRecVFS(t, 4096)
	c, _ := openRec(t, v, "FULL")
	mustExec(t, c, "CREATE TABLE t(x); INSERT INTO t VALUES (1)")
	if _, _, err := c.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE); err != nil {
		t.Fatal(err)
	}
	v.take()
	mustExec(t, c, "INSERT INTO t VALUES (2)") // restarts the fully backfilled WAL
	evs := v.take()
	var wal []recEvent
	for _, e := range evs {
		if e.file == "wal" && e.op != "read" {
			wal = append(wal, e)
		}
	}
	if len(wal) < 4 || wal[0].op != "write" || wal[0].off != 0 || wal[0].n != 32 || wal[1].op != "sync" {
		t.Fatalf("restart did not begin with a header write and its sync: %v", wal)
	}
	if syncs, after := walSyncs(wal[2:]); syncs != 1 || after != 1 {
		t.Fatalf("after the header sync: %d syncs, %d after a commit frame: %v", syncs, after, wal)
	}
}

// Experiment: decision 5, known failure. An error from the commit sync
// reaches the caller and rolls the transaction back, and the connection
// serves on. Without truncating the WAL, the failed transaction reappears
// when SQLite next recovers the WAL, here a copy of the files taken as a
// crash would leave them; truncating the WAL to the end of the last good
// commit before returning the error prevents that. (Closing the last
// connection checkpoints only committed frames and deletes the WAL, so it
// does not bring the transaction back.)
func TestExperimentFailedSyncAndTruncate(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		v := newRecVFS(t, 4096)
		c, path := openRec(t, v, "FULL")
		mustExec(t, c, "CREATE TABLE t(x); INSERT INTO t VALUES (1)")
		size, err := os.Stat(path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		end := size.Size() // the WAL ends at the last good commit
		v.mu.Lock()
		v.fail = func(e recEvent) error {
			if e.file == "wal" && e.op == "sync" {
				if truncate {
					if err := os.Truncate(path+"-wal", end); err != nil {
						return err
					}
				}
				return errInjected
			}
			return nil
		}
		v.mu.Unlock()
		err = c.Exec("INSERT INTO t VALUES (2)")
		if err == nil {
			t.Fatalf("truncate %v: a failed sync committed", truncate)
		}
		v.mu.Lock()
		v.fail = nil
		v.mu.Unlock()
		if !c.GetAutocommit() {
			t.Fatalf("truncate %v: a transaction stayed open after the failed commit", truncate)
		}
		if got := queryInt(t, c, "SELECT count(*) FROM t"); got != 1 {
			t.Fatalf("truncate %v: %d rows after the failed commit", truncate, got)
		}
		// Copy the database and its WAL, as a crash leaves them, and open
		// the copy: recovery rebuilds the index from the WAL file.
		crash := filepath.Join(t.TempDir(), "x.db")
		for _, suffix := range []string{"", "-wal"} {
			b, err := os.ReadFile(path + suffix)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(crash+suffix, b, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		got := queryInt(t, reopenRec(t, v, crash, "FULL"), "SELECT count(*) FROM t")
		if got2 := queryInt(t, c, "SELECT count(*) FROM t"); got2 != 1 {
			t.Errorf("truncate %v: the live connection sees %d rows", truncate, got2)
		}
		switch {
		case truncate && got != 1:
			t.Errorf("with truncation, %d rows after reopening", got)
		case !truncate && got != 2:
			t.Errorf("without truncation, %d rows after reopening; the failed transaction was expected back", got)
		}
	}
}

// Experiment: decision 5, BUSY at the commit point. If the commit sync
// returns BUSY, an explicit COMMIT fails with the transaction still open
// and the write lock held, which is why the engine never returns BUSY
// there.
func TestExperimentBusyFromSyncKeepsTransactionOpen(t *testing.T) {
	v := newRecVFS(t, 4096)
	c, _ := openRec(t, v, "FULL")
	mustExec(t, c, "CREATE TABLE t(x)")
	mustExec(t, c, "BEGIN; INSERT INTO t VALUES (1)")
	v.mu.Lock()
	v.fail = func(e recEvent) error {
		if e.file == "wal" && e.op == "sync" {
			return sqlite3.BUSY
		}
		return nil
	}
	v.mu.Unlock()
	err := c.Exec("COMMIT")
	if !errors.Is(err, sqlite3.BUSY) {
		t.Fatalf("COMMIT: %v, want BUSY", err)
	}
	if c.GetAutocommit() {
		t.Fatal("BUSY from the sync rolled the transaction back; decision 5 says it does not")
	}
	v.mu.Lock()
	v.fail = nil
	v.mu.Unlock()
	mustExec(t, c, "ROLLBACK")
}

// Experiment: decision 7, what a TRUNCATE checkpoint asks of the main file:
// the frames' pages written between the checkpoint-start and -done file
// controls, then a truncate of the main file to the committed size and its
// sync, then the WAL truncated to zero bytes. A RESTART checkpoint leaves
// the WAL file in place.
func TestExperimentTruncateCheckpoint(t *testing.T) {
	for _, mode := range []sqlite3.CheckpointMode{sqlite3.CHECKPOINT_RESTART, sqlite3.CHECKPOINT_TRUNCATE} {
		v := newRecVFS(t, 4096)
		c, path := openRec(t, v, "FULL")
		mustExec(t, c, "CREATE TABLE t(x); INSERT INTO t SELECT randomblob(3000) FROM generate_series(1, 20)")
		v.take()
		if _, _, err := c.WALCheckpoint("main", mode); err != nil {
			t.Fatal(err)
		}
		evs := v.take()
		var mainWrites, walTrunc0 int
		mainTrunc := int64(-1)
		for _, e := range evs {
			switch {
			case e.file == "main" && e.op == "write":
				if mainTrunc >= 0 {
					t.Errorf("mode %d: main-file write after its truncate", mode)
				}
				mainWrites++
			case e.file == "main" && e.op == "truncate":
				mainTrunc = e.off
			case e.file == "wal" && e.op == "truncate" && e.off == 0:
				walTrunc0++
			case e.file == "wal" && e.op == "write":
				t.Errorf("mode %d: checkpoint wrote to the WAL", mode)
			}
		}
		pages := queryInt(t, c, "PRAGMA page_count")
		if mainWrites == 0 || mainTrunc != pages*4096 {
			t.Errorf("mode %d: %d main writes, main truncated to %d, want %d", mode, mainWrites, mainTrunc, pages*4096)
		}
		wantTrunc := 0
		if mode == sqlite3.CHECKPOINT_TRUNCATE {
			wantTrunc = 1
		}
		if walTrunc0 != wantTrunc {
			t.Errorf("mode %d: WAL truncated to zero %d times, want %d", mode, walTrunc0, wantTrunc)
		}
		if st, err := os.Stat(path + "-wal"); err != nil || (mode == sqlite3.CHECKPOINT_TRUNCATE) != (st.Size() == 0) {
			t.Errorf("mode %d: WAL file after the checkpoint: %v, %v", mode, st, err)
		}
	}
}

// Experiment: decision 10, admission. With PRAGMA query_only set, a write
// statement and BEGIN IMMEDIATE fail before taking the write lock, so
// another connection can still write.
func TestExperimentQueryOnlyRefusesWriteLock(t *testing.T) {
	v := newRecVFS(t, 4096)
	c, path := openRec(t, v, "FULL")
	mustExec(t, c, "CREATE TABLE t(x)")
	other := reopenRec(t, v, path, "FULL")
	mustExec(t, other, "PRAGMA query_only=1")
	for _, sql := range []string{"INSERT INTO t VALUES (1)", "BEGIN IMMEDIATE"} {
		if err := other.Exec(sql); !errors.Is(err, sqlite3.READONLY) {
			t.Errorf("%s under query_only: %v", sql, err)
		}
		mustExec(t, c, "INSERT INTO t VALUES (2)")
	}
}

// Experiment: decision 6, "Unproven: that discarding a connection after a
// panic releases its locks and leaves the shared-memory index consistent
// for the other connections." It does not, with ncruces v0.37.0 on Linux.
// A panic in a VFS call unwinds through sqlite3_exec, whose statement
// ncruces does not track, so the connection cannot be closed; and its
// shared-memory locks are OFD locks that the mapping of the -shm file keeps
// alive. Another connection then cannot write. So the engine stops the
// database on every panic, as the record says it must if this fails.
func TestExperimentPanickedConnectionKeepsLocks(t *testing.T) {
	v := newRecVFS(t, 1024)
	c, path := openRec(t, v, "FULL")
	mustExec(t, c, "CREATE TABLE t(x); INSERT INTO t SELECT randomblob(900) FROM generate_series(1, 100)")
	other := reopenRec(t, v, path, "FULL")
	if err := other.BusyTimeout(50 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	v.fail = func(e recEvent) error {
		if e.file == "wal" && e.op == "write" {
			panic("injected")
		}
		return nil
	}
	v.mu.Unlock()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		_ = c.Exec("PRAGMA cache_size=5; BEGIN; UPDATE t SET x = randomblob(900)")
	}()
	v.mu.Lock()
	v.fail = nil
	v.mu.Unlock()
	for s := range c.Stmts() {
		_ = s.Close()
	}
	if err := c.Close(); err == nil {
		t.Fatal("the panicked connection closed; the engine could discard it alone")
	}
	if err := other.Exec("INSERT INTO t VALUES (1)"); !errors.Is(err, sqlite3.BUSY) {
		t.Fatalf("another connection's write after the panic: %v, want BUSY from the leaked write lock", err)
	}
}
