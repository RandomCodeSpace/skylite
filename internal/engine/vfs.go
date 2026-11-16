package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"
)

// leaderVFS is the VFS a leader registers with ncruces. The main database
// file is virtual; the WAL and the shared-memory index are ordinary local
// files, opened through ncruces's own VFS, and SQLite's temporary files are
// local files under the leader's directory, within its budget.
type leaderVFS Leader

var errNoJournal = errors.New("engine: the database has no rollback journal; its journal mode cannot change")

func osVFS() vfs.VFSFilename { return vfs.Find("").(vfs.VFSFilename) }

func (v *leaderVFS) Open(string, vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	return nil, 0, sqlite3.CANTOPEN
}

func (v *leaderVFS) OpenFilename(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	l := (*Leader)(v)
	switch {
	case flags&vfs.OPEN_MAIN_DB != 0:
		return l.openMain(name, flags)
	case flags&vfs.OPEN_WAL != 0:
		return l.openWAL(name, flags)
	case flags&(vfs.OPEN_MAIN_JOURNAL|vfs.OPEN_SUPER_JOURNAL) != 0:
		return nil, flags, errNoJournal
	}
	// Temporary databases, statement journals and the like: local, and
	// not part of the database.
	f, err := os.CreateTemp(l.dir, "temp-")
	if err != nil {
		return nil, flags, err
	}
	return &tempFile{File: f, l: l}, flags, nil
}

// tempFile is one of SQLite's temporary files. It lives in the leader's
// directory, so a start wipes what a crash left, and its size counts
// against the leader's TempBudget: a write past the budget fails with
// SQLITE_FULL. Only its own connection opens it, so it needs no locks.
type tempFile struct {
	*os.File
	l    *Leader
	held int64 // the bytes counted for it: its size
}

func (f *tempFile) WriteAt(p []byte, off int64) (int, error) {
	if end := off + int64(len(p)); end > f.held {
		if !f.l.chargeTemp(end - f.held) {
			f.l.tempRefused.Add(1)
			return 0, sqlite3.FULL
		}
		f.held = end
	}
	return f.File.WriteAt(p, off)
}

func (f *tempFile) Truncate(size int64) error {
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	if size < f.held {
		f.l.tempUsed.Add(size - f.held)
		f.held = size
	}
	return nil
}

func (f *tempFile) Close() error {
	f.l.tempUsed.Add(-f.held)
	f.held = 0
	return errors.Join(f.File.Close(), os.Remove(f.Name()))
}

// Sync does nothing: no temporary file is read after a crash.
func (f *tempFile) Sync(vfs.SyncFlag) error { return nil }

func (f *tempFile) Size() (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (f *tempFile) Lock(vfs.LockLevel) error                        { return nil }
func (f *tempFile) Unlock(vfs.LockLevel) error                      { return nil }
func (f *tempFile) CheckReservedLock() (bool, error)                { return false, nil }
func (f *tempFile) SectorSize() int                                 { return 4096 }
func (f *tempFile) DeviceCharacteristics() vfs.DeviceCharacteristic { return 0 }

// chargeTemp counts n more bytes of temporary files, unless that would pass
// the budget.
func (l *Leader) chargeTemp(n int64) bool {
	for {
		used := l.tempUsed.Load()
		if used+n > l.cfg.TempBudget {
			return false
		}
		if l.tempUsed.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

func (l *Leader) openMain(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	if name.String() != l.path {
		return nil, flags, fmt.Errorf("engine: only %s opens through this VFS", l.path)
	}
	n, _ := strconv.ParseInt(name.URIParameter("conn"), 10, 64)
	l.cmu.Lock()
	c := l.conns[n]
	ok := l.stopped == nil && c != nil && c.main == nil
	l.cmu.Unlock()
	if !ok {
		return nil, flags, errors.New("engine: the main database opens only for a connection of the leader")
	}
	f, flags, err := osVFS().OpenFilename(name, flags)
	if err != nil {
		return nil, flags, err
	}
	m := &mainFile{File: f, l: l, c: c}
	l.cmu.Lock()
	c.main = m
	l.cmu.Unlock()
	return m, flags, nil
}

func (l *Leader) openWAL(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	db := name.DatabaseFile()
	for {
		u, ok := db.(vfs.FileUnwrap)
		if !ok {
			break
		}
		db = u.Unwrap()
	}
	m, ok := db.(*mainFile)
	if !ok || m.l != l {
		return nil, flags, errors.New("engine: a WAL without its main database")
	}
	f, flags, err := osVFS().OpenFilename(name, flags)
	if err != nil {
		return nil, flags, err
	}
	return &walFile{File: f, l: l, c: m.c}, flags, nil
}

func (v *leaderVFS) Delete(name string, dirSync bool) error {
	l := (*Leader)(v)
	if name == l.path+"-wal" {
		// SQLite deletes the WAL only after a checkpoint of all of it with
		// no other connection open: the WAL restarts.
		l.cmu.Lock()
		l.wal = walState{start: -1}
		l.cmu.Unlock()
		if err := l.advanceBase(); err != nil {
			return err
		}
	}
	return osVFS().Delete(name, dirSync)
}

func (v *leaderVFS) Access(name string, flags vfs.AccessFlag) (bool, error) {
	return osVFS().Access(name, flags)
}

func (v *leaderVFS) FullPathname(name string) (string, error) { return name, nil }

// mainFile is the virtual main database file of one connection. A page
// reads as the last checkpoint wrote it since the base version, held in the
// local file, or else as of the base version, from storage through the
// leader's index, which every connection shares. The local file also
// carries SQLite's file locks and the shared-memory index.
type mainFile struct {
	vfs.File // the local file
	l        *Leader
	c        *Conn

	inCkpt   bool // between the checkpoint-start and -done file controls
	ckptDone bool // a checkpoint finished; it may now truncate the file
}

// fetch reads page pgno from s, the source locate names for it. It uses only
// the reader's store, keys and cache, which never change, so it is safe while
// the index changes; the leader reads that way outside its lock.
func (r *Reader) fetch(ctx context.Context, s source, pgno uint32) ([]byte, error) {
	switch {
	case s.c != nil:
		return r.commitPage(ctx, s.c, s.e)
	case s.base.Txn != 0:
		return r.basePage(ctx, s.base, pgno)
	}
	return make([]byte, r.a.m.PageSize), nil
}

func (f *mainFile) ReadAt(p []byte, off int64) (int, error) {
	l := f.l
	ctx := f.c.readContext()
	n := 0
	for n < len(p) {
		pos := off + int64(n)
		m, src, err := f.readLocal(p[n:], pos)
		if err != nil {
			return n, err
		}
		if src != nil {
			pgno := uint32(pos/l.ps) + 1 //nolint:gosec // bounded by the file size
			page, err := l.tmpl.fetch(ctx, *src, pgno)
			var stale *staleError
			if errors.As(err, &stale) {
				l.mu.RLock()
				ours := stale.explainedBy(l.tmpl.a)
				l.mu.RUnlock()
				if ours {
					// The leader's own compaction moved the base past the
					// object after this read located it: locate again.
					continue
				}
				// Otherwise only a later leader's compaction retires a
				// commit or deletes a base object: this leader is deposed.
				err = l.stop(fmt.Errorf("%w: %w", ErrFenced, stale))
			}
			if err != nil {
				return n, err
			}
			copy(p[n:n+m], page[pos%l.ps:])
		}
		n += m
	}
	return n, nil
}

// readLocal reads the part of p at pos that lies in pos's page, under l.mu:
// zeros past the base's limit, or the page as a checkpoint wrote it to the
// local file. For a page that storage holds as of the base version, it
// returns where to read it instead, which the caller does outside the lock.
// Past the end of the file it clears p and returns io.EOF.
func (f *mainFile) readLocal(p []byte, pos int64) (int, *source, error) {
	l := f.l
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.err(); err != nil {
		return 0, nil, err
	}
	l.ovMu.Lock()
	size, limit := l.size, l.baseLimit
	pgno := uint32(pos/l.ps) + 1 //nolint:gosec // bounded by the file size
	local := l.overlay[pgno]
	l.ovMu.Unlock()
	if pos >= size {
		clear(p)
		return 0, nil, io.EOF
	}
	m := int(min(int64(len(p)), l.ps-pos%l.ps, size-pos))
	switch {
	case local:
		if _, err := f.File.ReadAt(p[:m], pos); err != nil {
			return 0, nil, fmt.Errorf("engine: page %d checkpointed locally is missing: %w", pgno, err)
		}
	case pgno > limit:
		clear(p[:m])
	default:
		s := l.tmpl.locate(pgno)
		return m, &s, nil
	}
	return m, nil, nil
}

// WriteAt accepts only a checkpoint's page writes: they go to the local
// file and are kept until the base version advances.
func (f *mainFile) WriteAt(p []byte, off int64) (int, error) {
	l := f.l
	if !f.inCkpt || int64(len(p)) != l.ps || off%l.ps != 0 {
		return 0, fmt.Errorf("engine: main-file write of %d bytes at %d outside a checkpoint", len(p), off)
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.err(); err != nil {
		return 0, err
	}
	if err := l.injected("main", "write", off, len(p)); err != nil {
		return 0, err
	}
	l.ovMu.Lock()
	defer l.ovMu.Unlock()
	n, err := f.File.WriteAt(p, off)
	if err != nil {
		return n, err
	}
	l.overlay[uint32(off/l.ps)+1] = true //nolint:gosec // bounded by the database size
	l.size = max(l.size, off+int64(len(p)))
	return n, nil
}

// Truncate accepts only the truncation a checkpoint makes once it has
// copied the whole WAL.
func (f *mainFile) Truncate(size int64) error {
	l := f.l
	if !f.ckptDone || size%l.ps != 0 {
		return fmt.Errorf("engine: main-file truncate to %d outside a checkpoint", size)
	}
	f.ckptDone = false
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.err(); err != nil {
		return err
	}
	l.ovMu.Lock()
	defer l.ovMu.Unlock()
	pages := uint32(size / l.ps) //nolint:gosec // bounded by the database size
	for p := range l.overlay {
		if p > pages {
			delete(l.overlay, p)
		}
	}
	l.size, l.baseLimit = size, min(l.baseLimit, pages)
	return nil
}

func (f *mainFile) Sync(vfs.SyncFlag) error { return f.l.err() }

func (f *mainFile) Size() (int64, error) {
	if err := f.l.err(); err != nil {
		return 0, err
	}
	f.l.ovMu.Lock()
	defer f.l.ovMu.Unlock()
	return f.l.size, nil
}

func (f *mainFile) Lock(lock vfs.LockLevel) error {
	if err := f.l.err(); err != nil {
		return err
	}
	return f.File.Lock(lock)
}

func (f *mainFile) Close() error {
	f.l.cmu.Lock()
	if f.c.main == f {
		f.c.main = nil
	}
	f.l.cmu.Unlock()
	return f.File.Close()
}

func (f *mainFile) SharedMemory() vfs.SharedMemory {
	return f.File.(vfs.FileSharedMemory).SharedMemory()
}

func (f *mainFile) DeviceCharacteristics() vfs.DeviceCharacteristic {
	if f.l.psow {
		return vfs.IOCAP_POWERSAFE_OVERWRITE
	}
	return 0
}

// CommitPhaseTwo runs when SQLite has completed a commit on the connection,
// in sqlite3_step or, for a statement closed before its end, in
// sqlite3_finalize, which calls no WAL hook. SQLite sends it for no commit
// that fails, but for every commit that ends a write transaction, an empty
// one included; the commit hook stops the leader before any commit other
// than the uploaded one gets here.
func (f *mainFile) CommitPhaseTwo() error {
	l, c := f.l, f.c
	if l.beforePhaseTwo != nil {
		l.beforePhaseTwo(c)
	}
	l.cmu.Lock()
	defer l.cmu.Unlock()
	if c.uploaded == 0 {
		return nil
	}
	c.done, c.uploaded, c.inCommit = c.uploaded, 0, false
	l.uploading = false
	l.cond.Broadcast()
	if c.retired {
		// Only a later leader's compaction retires a commit the leader had
		// not resolved yet, so the leader is deposed (decision 6).
		_ = l.stopLocked(fmt.Errorf("%w: own commit %d already retired", ErrFenced, c.done))
	}
	return nil
}

func (f *mainFile) CheckpointStart() { f.inCkpt, f.ckptDone = true, false }
func (f *mainFile) CheckpointDone()  { f.inCkpt, f.ckptDone = false, true }

// Pragma refuses the settings that would route a write around capture or
// replace the WAL hook, which checkpoints. The SQL policy refuses more;
// this holds whatever it allows.
func (f *mainFile) Pragma(name, value string) (string, error) {
	v := strings.ToLower(strings.Trim(value, `'"`))
	switch {
	case value == "":
	case name == "journal_mode" && v == "wal":
	case name == "synchronous" && (v == "full" || v == "2"):
	case name == "journal_mode", name == "locking_mode", name == "synchronous",
		name == "wal_autocheckpoint":
		return "", fmt.Errorf("engine: PRAGMA %s cannot be changed on this database", name)
	}
	return "", sqlite3.NOTFOUND
}

// walFile is a connection's handle on the local WAL. It watches what SQLite
// writes, captures each commit at the WAL sync that ends it, and observes
// the WAL restarting.
type walFile struct {
	vfs.File
	l *Leader
	c *Conn
}

func (w *walFile) ReadAt(p []byte, off int64) (int, error) {
	if err := w.l.err(); err != nil {
		return 0, err
	}
	return w.File.ReadAt(p, off)
}

func (w *walFile) WriteAt(p []byte, off int64) (int, error) {
	l := w.l
	l.cmu.Lock()
	err := l.walWrite(w, p, off)
	l.cmu.Unlock()
	if err != nil {
		return 0, err
	}
	if off == 0 {
		// A WAL header: SQLite is starting the WAL from its beginning.
		if err := l.advanceBase(); err != nil {
			return 0, err
		}
	}
	if err := l.injected("wal", "write", off, len(p)); err != nil {
		return 0, err
	}
	return w.File.WriteAt(p, off)
}

// walWrite checks and records a WAL write, with l.cmu held.
func (l *Leader) walWrite(w *walFile, p []byte, off int64) error {
	if l.stopped != nil {
		return l.stopped
	}
	t := &l.wal
	end := off + int64(len(p))
	if w.c.uploaded != 0 {
		// The rest of an uploaded commit's padding, written after its sync.
		t.hwm = max(t.hwm, end)
		return nil
	}
	switch {
	case off == 0:
		if len(p) != walHeaderSize {
			return fmt.Errorf("engine: WAL write of %d bytes at 0", len(p))
		}
		// A WAL that starts again while a commit is uploaded and not yet
		// completed: wait for SQLite to complete it, or for the leader to
		// stop if SQLite rolled it back without the rollback hook. A healthy
		// commit never waits here: its connection keeps a WAL read mark
		// below the new frames until phase two, so no checkpoint can reset
		// the WAL first.
		// shortcut: the wait ignores this connection's context and holds the
		// WAL write lock until the failed Run returns or the leader closes;
		// the database is already stopping, and the server bounds every Run
		// with its statement time limit. Wake on the context if a caller
		// without such a limit appears.
		for l.uploading && l.stopped == nil {
			l.cond.Wait()
		}
		if l.stopped != nil {
			return l.stopped
		}
		t.start, t.end = -1, walHeaderSize
	case off < t.end:
		return l.stopLocked(fmt.Errorf("engine: WAL write at %d over committed frames ending at %d", off, t.end))
	case t.start < 0:
		if len(p) != frameHeaderSize || (off-walHeaderSize)%(l.ps+frameHeaderSize) != 0 {
			return fmt.Errorf("engine: WAL write of %d bytes at %d does not start a frame", len(p), off)
		}
		t.start = off
	}
	t.writer = w
	switch {
	case t.start >= 0 && walHeaderSize+end-t.start > l.cfg.WALBound:
		// The transaction alone passes the bound. No maintenance would make
		// room for it, so none starts and no other transaction is touched.
		err := fmt.Errorf("%w: %w: the transaction alone passes the local WAL bound of %d bytes", ErrNotCommitted, ErrTooLarge, l.cfg.WALBound)
		w.c.outcome = err
		return err
	case end > l.cfg.WALBound:
		l.startMaintenance()
		err := fmt.Errorf("%w: %w: the local WAL would pass its bound of %d bytes", ErrNotCommitted, ErrRetry, l.cfg.WALBound)
		w.c.outcome = err
		return err
	}
	t.hwm = max(t.hwm, end)
	return nil
}

func (w *walFile) Sync(flags vfs.SyncFlag) error {
	l := w.l
	l.cmu.Lock()
	t := l.wal
	stopped := l.stopped
	if stopped == nil && w.c.inCommit {
		// A panic left the connection's commit path unresolved, and SQLite
		// is committing again: that commit's create may have taken effect.
		stopped = l.stopLocked(errors.New("engine: a WAL sync during a commit whose create may have taken effect"))
	}
	l.cmu.Unlock()
	if stopped != nil {
		return stopped
	}
	if err := l.injected("wal", "sync", 0, 0); err != nil {
		return err
	}
	if t.writer != w || t.start < 0 {
		return w.File.Sync(flags)
	}
	cp, err := l.parse(w, t.start, t.hwm)
	if err != nil {
		return l.fail(w, t.start, fmt.Errorf("%w: %w", ErrNotCommitted, err))
	}
	if cp == nil {
		// No commit frame: the sync of a new WAL header.
		return w.File.Sync(flags)
	}
	return l.commit(w, cp)
}

func (w *walFile) Truncate(size int64) error {
	l := w.l
	l.cmu.Lock()
	err := l.stopped
	switch {
	case err != nil:
	case size == 0:
		l.wal = walState{start: -1}
	case size < l.wal.end:
		err = l.stopLocked(fmt.Errorf("engine: WAL truncated to %d, within committed frames", size))
	default:
		l.wal.hwm = min(l.wal.hwm, size)
	}
	l.cmu.Unlock()
	if err == nil {
		err = l.injected("wal", "truncate", size, 0)
	}
	if err != nil {
		return err
	}
	if err := w.File.Truncate(size); err != nil {
		return err
	}
	if size == 0 {
		// A TRUNCATE checkpoint emptied a fully checkpointed WAL.
		return l.advanceBase()
	}
	return nil
}
