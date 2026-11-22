package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

const followerFilename = "snapshot"

var readConnID atomic.Uint64

// ReadConn is a SQLite connection on one durable follower snapshot. Run
// and Close are serialized. The caller must close its statements before
// closing the connection.
type ReadConn struct {
	mu     sync.Mutex
	db     *sqlite3.Conn
	file   *snapshotFile
	name   string
	txn    uint64
	broken bool // a Run panicked or reported ErrBroken; Close releases db
}

func openReadConn(ctx context.Context, r Reader) (*ReadConn, error) {
	f := &snapshotFile{reader: r, ctx: ctx}
	name := fmt.Sprintf("skylite-follow-%d", readConnID.Add(1))
	vfs.Register(name, snapshotVFS{f})
	// mode=ro selects OPEN_READONLY. Use OpenContext so the initial header
	// reads and SQLite setup have the same deadline as admission.
	db, err := sqlite3.OpenContext(sqlite3.WithMaxMemory(ctx, 64<<20),
		"file:"+followerFilename+"?mode=ro&immutable=1&vfs="+name+"&_pragma=temp_store(MEMORY)")
	if err == nil {
		err = ctx.Err()
	}
	if f.readErr != nil {
		err = f.readErr // SQLite reports a failed read as a bare I/O error
	}
	if err != nil {
		if db != nil && !closeQuietly(db) {
			release(db)
		}
		vfs.Unregister(name)
		return nil, err
	}
	f.ctx = nil
	return &ReadConn{db: db, file: f, name: name, txn: r.Txn()}, nil
}

// Txn returns the snapshot's durable transaction number.
func (c *ReadConn) Txn() uint64 { return c.txn }

// Run uses ctx for SQLite interrupts and reads from storage. The callback
// uses SQLite synchronously and must not retain the connection. SQL
// admission policy is the caller's responsibility. A panic in fn, or an
// error of fn's that wraps ErrBroken, breaks the connection: Run then fails
// with ErrBroken, later Runs refuse, and Close frees it without SQLite.
func (c *ReadConn) Run(ctx context.Context, fn func(*sqlite3.Conn) error) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.db == nil:
		return sqlite3.MISUSE
	case c.broken:
		return fmt.Errorf("%w: the follower connection broke earlier", ErrBroken)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.file.ctx, c.file.readErr = ctx, nil
	defer func() { c.file.ctx, c.file.readErr = nil, nil }()
	old := c.db.SetInterrupt(ctx)
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: panic: %v", ErrBroken, p)
		}
		if c.broken = errors.Is(err, ErrBroken); !c.broken {
			c.db.SetInterrupt(old)
		}
	}()
	err = fn(c.db)
	if errors.Is(err, ErrBroken) {
		return err
	}
	if c.file.readErr != nil {
		return c.file.readErr
	}
	if err == nil {
		return ctx.Err()
	}
	return err
}

// Close releases the connection and its snapshot.
func (c *ReadConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil
	}
	// As the leader's discard: a broken connection, or one whose close
	// fails, is released without running SQLite.
	var err error
	if (c.broken || !closeQuietly(c.db)) && !release(c.db) {
		err = errors.New("engine: the follower connection could not be released")
	}
	vfs.Unregister(c.name)
	c.db, c.file = nil, nil
	return err
}

type snapshotVFS struct{ file *snapshotFile }

func (v snapshotVFS) Open(name string, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	if name != followerFilename || flags&vfs.OPEN_MAIN_DB == 0 || flags&vfs.OPEN_READONLY == 0 ||
		flags&(vfs.OPEN_READWRITE|vfs.OPEN_CREATE) != 0 {
		return nil, 0, sqlite3.CANTOPEN
	}
	return v.file, flags, nil
}

func (snapshotVFS) Delete(string, bool) error { return sqlite3.READONLY }
func (snapshotVFS) FullPathname(name string) (string, error) {
	return name, nil
}

func (snapshotVFS) Access(name string, flags vfs.AccessFlag) (bool, error) {
	return name == followerFilename && flags != vfs.ACCESS_READWRITE, nil
}

// Only this connection uses the Reader. ReadPage may re-anchor it, but
// the file's transaction, page size and length cannot change.
type snapshotFile struct {
	reader  Reader
	ctx     context.Context
	readErr error
}

func (f *snapshotFile) ReadAt(p []byte, off int64) (int, error) {
	ps := int64(f.reader.PageSize())
	size, _ := f.Size()
	if off < 0 {
		return 0, sqlite3.IOERR_READ
	}
	n := 0
	for n < len(p) && off < size {
		pgno := uint32(off/ps) + 1 //nolint:gosec // bounded by the file size
		start := int(off % ps)
		chunk := p[n : n+min(len(p)-n, int(ps)-start)]
		if pgno == format.LockPage(f.reader.PageSize()) {
			clear(chunk)
		} else {
			page, err := f.reader.ReadPage(f.ctx, pgno)
			if err != nil {
				// Keep the original cause even if SQLite discards it or a
				// caller ignores a statement's error.
				if f.readErr == nil {
					f.readErr = err
				}
				return n, err
			}
			if pgno == 1 {
				// ReadPage returns an owned copy. Present a rollback-mode
				// header whose page count and validity match this snapshot.
				page[18], page[19] = 1, 1
				binary.BigEndian.PutUint32(page[28:32], f.reader.Size())
				copy(page[92:96], page[24:28])
			}
			copy(chunk, page[start:])
		}
		n += len(chunk)
		off += int64(len(chunk))
	}
	if n < len(p) {
		clear(p[n:])
		return n, io.EOF
	}
	return n, nil
}

func (f *snapshotFile) Size() (int64, error) {
	return int64(f.reader.Size()) * int64(f.reader.PageSize()), nil
}

func (*snapshotFile) Lock(level vfs.LockLevel) error {
	if level > vfs.LOCK_SHARED {
		return sqlite3.READONLY
	}
	return nil
}

func (*snapshotFile) Close() error                                    { return nil }
func (*snapshotFile) WriteAt([]byte, int64) (int, error)              { return 0, sqlite3.READONLY }
func (*snapshotFile) Truncate(int64) error                            { return sqlite3.READONLY }
func (*snapshotFile) Sync(vfs.SyncFlag) error                         { return nil }
func (*snapshotFile) Unlock(vfs.LockLevel) error                      { return nil }
func (*snapshotFile) CheckReservedLock() (bool, error)                { return false, nil }
func (*snapshotFile) SectorSize() int                                 { return 4096 }
func (*snapshotFile) DeviceCharacteristics() vfs.DeviceCharacteristic { return 0 }
