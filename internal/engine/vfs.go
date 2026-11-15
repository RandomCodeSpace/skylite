package engine

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"
)

// leaderVFS is the VFS a leader registers with ncruces. The main database
// file is virtual; the WAL, the shared-memory index and SQLite's temporary
// files are ordinary local files, opened through ncruces's own VFS.
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
	return osVFS().OpenFilename(name, flags)
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
// connection's reader. The local file also carries SQLite's file locks and
// the shared-memory index.
type mainFile struct {
	vfs.File // the local file
	l        *Leader
	c        *Conn

	r        *Reader // as of base
	base     uint64
	inCkpt   bool // between the checkpoint-start and -done file controls
	ckptDone bool // a checkpoint finished; it may now truncate the file
}

func (f *mainFile) ReadAt(p []byte, off int64) (int, error) {
	l := f.l
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.err(); err != nil {
		return 0, err
	}
	if f.r == nil || f.base != l.base {
		f.r, f.base = l.tmpl.clone(), l.base
	}
	n := 0
	for n < len(p) {
		pos := off + int64(n)
		l.ovMu.Lock()
		size, limit := l.size, l.baseLimit
		pgno := uint32(pos/l.ps) + 1 //nolint:gosec // bounded by the file size
		local := l.overlay[pgno]
		l.ovMu.Unlock()
		if pos >= size {
			clear(p[n:])
			return n, io.EOF
		}
		in := pos % l.ps
		m := int(min(int64(len(p)-n), l.ps-in, size-pos))
		switch {
		case local:
			if _, err := f.File.ReadAt(p[n:n+m], pos); err != nil {
				return n, fmt.Errorf("engine: page %d checkpointed locally is missing: %w", pgno, err)
			}
		case pgno > limit:
			clear(p[n : n+m])
		default:
			page, err := f.r.ReadPage(l.ctx, pgno)
			if err != nil {
				return n, err
			}
			copy(p[n:n+m], page[in:])
		}
		n += m
	}
	return n, nil
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

func (f *mainFile) CheckpointStart() { f.inCkpt, f.ckptDone = true, false }
func (f *mainFile) CheckpointDone()  { f.inCkpt, f.ckptDone = false, true }

// Pragma refuses the settings that would route a write around capture or
// take away the hook that completes a commit. The SQL policy refuses more;
// this holds whatever it allows.
func (f *mainFile) Pragma(name, value string) (string, error) {
	v := strings.ToLower(strings.Trim(value, `'"`))
	switch {
	case value == "":
	case name == "journal_mode" && v == "wal":
	case name == "synchronous" && (v == "full" || v == "2"):
	case name == "query_only" && f.c.internal:
	case name == "journal_mode", name == "locking_mode", name == "synchronous",
		name == "wal_autocheckpoint", name == "query_only":
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
	if end > l.cfg.WALBound {
		l.startMaintenance()
		err := fmt.Errorf("%w: %w: the local WAL would pass its bound of %d bytes", ErrNotCommitted, ErrRetry, l.cfg.WALBound)
		if t.start == walHeaderSize {
			err = fmt.Errorf("%w: %w: the transaction alone passes the local WAL bound of %d bytes", ErrNotCommitted, ErrTooLarge, l.cfg.WALBound)
		}
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
