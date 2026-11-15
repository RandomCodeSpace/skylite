package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// Errors a commit, a connection or the leader report. A commit's outcome is
// exactly one of: committed (Run returns its transaction number), not
// committed (the error wraps ErrNotCommitted) or unknown (it wraps
// ErrUnknown). Decision 6 of record 0004 defines them.
var (
	// ErrNotCommitted means the transaction is not in the durable history
	// and never will be.
	ErrNotCommitted = errors.New("engine: transaction not committed")
	// ErrUnknown means the transaction may be in the durable history, now
	// or later. The leader has stopped.
	ErrUnknown = errors.New("engine: commit outcome unknown")
	// ErrRetry, with ErrNotCommitted, means the transaction was refused or
	// rolled back while the leader brings its local WAL or its log tail
	// back under their bounds, and can be retried.
	ErrRetry = errors.New("engine: writes are refused for now; retry")
	// ErrTooLarge, with ErrNotCommitted, means the transaction does not fit
	// in one commit object or in the local WAL bound.
	ErrTooLarge = errors.New("engine: transaction too large")
	// ErrFenced means another candidacy's epoch bump holds the leader's next
	// transaction number. The leader has stopped.
	ErrFenced = errors.New("engine: fenced by another leader's epoch bump")
	// ErrStopped means the leader no longer serves the database on this
	// node: it failed, was fenced or was closed. It serves again only
	// through a new takeover.
	ErrStopped = errors.New("engine: the leader has stopped")
)

const (
	busyTimeout         = 10 * time.Second
	autoCheckpointPages = 1000 // SQLite's default wal_autocheckpoint
	maintenanceBackoff  = 10 * time.Millisecond
)

// Config sets a leader's local limits.
type Config struct {
	// WALBound is the largest the local WAL file may grow, in bytes. A
	// write past it rolls its transaction back and starts maintenance.
	WALBound int64
	// TailBound is how many commits, epoch bumps included, the log may
	// hold above the manifest's transaction number. Once it holds that
	// many, commits are refused and maintenance starts.
	TailBound uint64
	// MaxMemory is each connection's SQLite memory ceiling in bytes.
	MaxMemory int64
	// Cache holds pages read from storage; it may be shared.
	Cache *Cache
}

// Leader serves one database on this node while it holds office: SQLite in
// WAL mode with a main file served from object storage, and every commit
// created in the commit log before SQLite reports it. The package
// documentation describes the mechanism.
type Leader struct {
	st     storage.Store
	keys   format.Keys
	office Office
	cfg    Config
	dir    string // this database's local directory, wiped at start
	path   string // the local path SQLite knows the database by
	vfs    string // the name the VFS is registered under
	ps     int64  // page size

	ctx    context.Context // ends at Close; every storage call uses it
	cancel context.CancelFunc
	wg     sync.WaitGroup // the maintenance goroutine

	psow bool // what the main file reports for powersafe overwrite; tests clear it
	// afterUpload, if set, runs after a commit resolved as committed and
	// before SQLite is told. Tests use it to pause or kill there.
	afterUpload func()
	// fault, if set, runs before each write, sync and truncate of a local
	// file ("wal" or "main"), the engine's own WAL truncation included; an
	// error it returns fails the call. Tests use it for disk failures.
	fault func(file, op string, off int64, n int) error

	// mu orders main-file reads against advances of the base version.
	mu   sync.RWMutex
	base uint64  // the base version (decision 7)
	tmpl *Reader // the base version's index; main-file handles clone it

	// ovMu guards what checkpoints wrote to the main file since base.
	ovMu      sync.Mutex
	overlay   map[uint32]bool // pages held in the local file
	size      int64           // main file size in bytes
	baseLimit uint32          // highest page the base may still supply

	// cmu guards the commit state, the connections and maintenance.
	cmu         sync.Mutex
	cond        *sync.Cond // signalled when uploading or stopped changes
	stopped     error
	head        uint64 // the leader's last commit
	headSize    uint32 // database size in pages at head
	manifestTxn uint64 // the manifest's transaction number, for the tail bound
	pending     []*commit
	wal         walState
	uploading   bool // a commit is between its first create and its local completion
	conns       map[int64]*Conn
	nextConn    int64
	admitClosed bool // client writes refused (decision 10, step 1)
	noCreates   bool // creates not begun (decision 10, step 2)
	maintaining bool
	scratch     *os.File // the commit object being uploaded
	mconn       *Conn    // the leader's own connection, for maintenance
}

// walState follows the write transaction in the local WAL. Only the
// connection holding SQLite's write lock writes the WAL, so one state
// serves every handle.
type walState struct {
	start  int64 // offset of the open transaction's first frame; -1 if none
	end    int64 // end of the last committed frame; 0 before the WAL header
	hwm    int64 // end of the furthest write since end
	writer *walFile
}

// Lead serves the database as the holder of office. dir is a local
// directory for cache files: Lead keeps this database's files in a
// subdirectory named after its identifier, deletes whatever that
// subdirectory held, and deletes it again at Close. Lead reads the manifest
// and the log tail up to office.Head, never the database's pages.
func Lead(ctx context.Context, st storage.Store, keys format.Keys, dir string, office Office, cfg Config) (*Leader, error) {
	if cfg.WALBound <= 0 || cfg.TailBound == 0 || cfg.MaxMemory <= 0 || cfg.Cache == nil {
		return nil, errors.New("engine: Config needs a WAL bound, a tail bound, a memory ceiling and a cache")
	}
	tmpl, err := Open(ctx, st, keys, cfg.Cache, office.Head)
	if err != nil {
		return nil, err
	}
	m := tmpl.a.m
	if m.DB != office.DB || m.Epoch != office.Epoch || m.Nonce != office.Nonce {
		return nil, fmt.Errorf("%w: manifest of %x at epoch %d does not carry office %x at epoch %d", ErrFenced, m.DB, m.Epoch, office.DB, office.Epoch)
	}
	if err := checkBump(ctx, st, keys, office); err != nil {
		return nil, err
	}

	l := &Leader{
		st: st, keys: keys, office: office, cfg: cfg,
		dir:  filepath.Join(dir, hex.EncodeToString(office.DB[:])),
		ps:   int64(tmpl.PageSize()),
		psow: true,
		base: office.Head, tmpl: tmpl,
		overlay: map[uint32]bool{}, size: int64(tmpl.Size()) * int64(tmpl.PageSize()), baseLimit: tmpl.Size(),
		head: office.Head, headSize: tmpl.Size(), manifestTxn: m.Txn,
		wal:   walState{start: -1},
		conns: map[int64]*Conn{},
	}
	l.cond = sync.NewCond(&l.cmu)
	l.path = filepath.Join(l.dir, "db")
	if err := os.RemoveAll(l.dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		return nil, err
	}
	if l.scratch, err = os.OpenFile(filepath.Join(l.dir, "commit"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600); err != nil {
		return nil, err
	}
	var name [8]byte
	if _, err := rand.Read(name[:]); err != nil {
		return nil, err
	}
	l.vfs = "skylite-" + hex.EncodeToString(name[:])
	l.ctx, l.cancel = context.WithCancel(context.WithoutCancel(ctx))
	vfs.Register(l.vfs, (*leaderVFS)(l))
	if l.mconn, err = l.Open(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}

// checkBump checks that office.Head holds office's own epoch bump.
func checkBump(ctx context.Context, st storage.Store, keys format.Keys, office Office) error {
	rc, _, err := st.GetRange(ctx, keys.Commit(office.DB, office.Head), 0, format.CommitHeaderSize)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return err
	}
	h, err := format.DecodeCommitHeader(b, office.DB)
	if err == nil {
		err = h.CheckKey(office.DB, office.Head)
	}
	switch {
	case err != nil:
		return fmt.Errorf("engine: commit %d: %w", office.Head, err)
	case !h.EpochBump || h.Epoch != office.Epoch:
		return fmt.Errorf("%w: commit %d is not the epoch bump of epoch %d", ErrFenced, office.Head, office.Epoch)
	}
	return nil
}

// Head is the transaction number of the leader's last commit.
func (l *Leader) Head() uint64 {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.head
}

// Err is nil while the leader serves, and otherwise wraps ErrStopped and
// says why it stopped.
func (l *Leader) Err() error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.stopped
}

// Close stops serving, closes every connection once its Run has returned,
// and deletes the database's local files.
func (l *Leader) Close() error {
	l.cmu.Lock()
	l.stopLocked(errors.New("engine: leader closed"))
	conns := make([]*Conn, 0, len(l.conns))
	for _, c := range l.conns {
		conns = append(conns, c)
	}
	l.cmu.Unlock()
	l.cancel()
	l.wg.Wait()
	for _, c := range conns {
		_ = c.Close()
	}
	vfs.Unregister(l.vfs)
	if l.scratch != nil {
		_ = l.scratch.Close()
	}
	return os.RemoveAll(l.dir)
}

// stop is fail-stop: from now on the leader refuses every storage-layer
// call, so no connection serves from its local image again.
func (l *Leader) stop(cause error) error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.stopLocked(cause)
}

func (l *Leader) stopLocked(cause error) error {
	if l.stopped == nil {
		l.stopped = fmt.Errorf("%w: %w", ErrStopped, cause)
		l.cond.Broadcast()
	}
	return l.stopped
}

func (l *Leader) err() error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.stopped
}

// Open opens a new SQLite connection to the database.
func (l *Leader) Open() (*Conn, error) {
	l.cmu.Lock()
	if l.stopped != nil {
		defer l.cmu.Unlock()
		return nil, l.stopped
	}
	l.nextConn++
	c := &Conn{l: l, n: l.nextConn}
	l.conns[c.n] = c
	l.cmu.Unlock()

	u := url.URL{Scheme: "file", Path: l.path, RawQuery: url.Values{"vfs": {l.vfs}, "conn": {fmt.Sprint(c.n)}}.Encode()}
	db, err := sqlite3.OpenContext(sqlite3.WithMaxMemory(context.Background(), l.cfg.MaxMemory), u.String())
	if err == nil {
		c.db = db
		err = c.setup()
	}
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		l.cmu.Lock()
		delete(l.conns, c.n)
		l.cmu.Unlock()
		return nil, err
	}
	return c, nil
}

// Conn is one SQLite connection to the leader's database. It is safe for
// concurrent use, one Run at a time.
type Conn struct {
	l  *Leader
	n  int64
	mu sync.Mutex // held by Run, Close and maintenance
	db *sqlite3.Conn

	// Guarded by l.cmu.
	main      *mainFile
	id        []byte // the commit identifier for this Run
	idUsed    bool
	inCommit  bool   // from the first create until the commit completes locally
	uploaded  uint64 // committed in storage, not yet completed by SQLite
	done      uint64 // committed during this Run
	outcome   error  // why a commit during this Run failed
	retired   bool   // the uploaded commit was found retired: the leader is deposed
	interrupt context.CancelFunc
	maint     bool  // maintenance asked this connection to give up its transaction
	aborted   error // maintenance rolled this connection's transaction back
	queryOnly bool
	internal  bool // the engine is running its own pragma
}

func (c *Conn) setup() error {
	c.internal = true
	defer func() { c.internal = false }()
	if err := c.db.Exec("PRAGMA synchronous=FULL"); err != nil {
		return err
	}
	if mode, err := c.pragma("journal_mode"); err != nil || mode != "wal" {
		return fmt.Errorf("engine: database not in WAL mode (%q): %v", mode, err)
	}
	if s, err := c.pragma("synchronous"); err != nil || s != "2" {
		return fmt.Errorf("engine: synchronous is %q, want FULL: %v", s, err)
	}
	if err := c.db.BusyTimeout(busyTimeout); err != nil {
		return err
	}
	// Any panic stops the database (see finish), and exceeding the memory
	// ceiling panics in ncruces. Capping values and statements well below
	// the ceiling turns the commonest way there, one huge value, into an
	// SQLITE_TOOBIG error.
	limit := int(min(1e9, c.l.cfg.MaxMemory/4))
	c.db.Limit(sqlite3.LIMIT_LENGTH, limit)
	c.db.Limit(sqlite3.LIMIT_SQL_LENGTH, limit)
	c.db.WALHook(c.walHook)
	c.db.RollbackHook(c.rollbackHook)
	return nil
}

func (c *Conn) pragma(name string) (string, error) {
	s, _, err := c.db.Prepare("PRAGMA " + name)
	if err != nil {
		return "", err
	}
	defer func() { _ = s.Close() }()
	if !s.Step() {
		return "", s.Err()
	}
	return s.ColumnText(0), nil
}

// Run runs f on the connection. f may run any SQL, but must not keep db or
// statements beyond its return. id is the commit identifier, at most
// format.MaxCommitIDLen bytes, of the one transaction f may commit; a
// commit without one, or a second commit in one Run, is refused as not
// committed. A transaction that changes no page commits nothing and needs
// none. Run returns the transaction number of the commit f made, or 0, and
// f's error together with the commit's outcome if it was not committed. A
// nonzero transaction number is committed whatever the error says; the error
// then reports what failed later in the Run, and never matches
// ErrNotCommitted.
func (c *Conn) Run(id []byte, f func(db *sqlite3.Conn) error) (txn uint64, err error) {
	if len(id) > format.MaxCommitIDLen {
		return 0, fmt.Errorf("engine: commit identifier of %d bytes", len(id))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.l
	ictx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l.cmu.Lock()
	switch {
	case l.stopped != nil:
		err = l.stopped
	case c.db == nil:
		err = fmt.Errorf("%w: connection closed", ErrStopped)
	case c.aborted != nil:
		err = c.aborted
	}
	if err != nil {
		l.cmu.Unlock()
		return 0, err
	}
	c.id, c.idUsed, c.done, c.outcome = bytes.Clone(id), false, 0, nil
	c.interrupt = cancel
	closed := l.admitClosed
	l.cmu.Unlock()
	if err := c.admit(!closed); err != nil {
		return 0, err
	}
	c.db.SetInterrupt(ictx)

	defer func() {
		p := recover()
		txn, err = c.finish(p, err)
	}()
	return 0, f(c.db)
}

// admit sets query_only to match admission, so that a client write is
// refused before it takes SQLite's write lock (decision 10, step 1).
func (c *Conn) admit(open bool) error {
	if c.queryOnly != open {
		return nil
	}
	c.internal = true
	defer func() { c.internal = false }()
	v := "1"
	if open {
		v = "0"
	}
	if err := c.db.Exec("PRAGMA query_only=" + v); err != nil {
		return err
	}
	c.queryOnly = !open
	return nil
}

// finish ends a Run: it applies the rules for a panic or an error in the
// commit path and reports the commit's outcome.
//
// Any panic stops the database on this node, not only one in the commit
// path. Decision 6 left discarding just the panicked connection to a test,
// and it fails: a panic that unwinds through SQLite leaves a statement
// ncruces cannot finalize, so the connection cannot be closed, and its
// shared-memory locks outlive its files because the mapping keeps the file
// description open (TestExperimentPanickedConnectionKeepsLocks). The record
// extends fail-stop to every panic in that case.
func (c *Conn) finish(p any, ferr error) (uint64, error) {
	l := c.l
	if p == nil {
		c.db.SetInterrupt(context.Background())
	}
	l.cmu.Lock()
	inCommit, uploaded, done, outcome, maint := c.inCommit, c.uploaded, c.done, c.outcome, c.maint
	c.interrupt, c.id = nil, nil
	switch {
	case p != nil || inCommit:
		// The Run ended in a panic, or the commit path ended without SQLite
		// completing a commit whose create may have taken effect.
		cause := fmt.Errorf("%w: a commit's create may have taken effect", ErrUnknown)
		if !inCommit {
			cause = fmt.Errorf("%w: nothing of this Run was sent to storage", ErrNotCommitted)
		}
		if uploaded != 0 {
			cause = fmt.Errorf("transaction %d is committed in storage but SQLite did not complete it", uploaded)
			done = uploaded
		}
		outcome = l.stopLocked(fmt.Errorf("%w (panic: %v, error: %v)", cause, p, ferr))
		c.inCommit, c.uploaded = false, 0
		l.uploading = false
		l.cond.Broadcast()
	}
	l.cmu.Unlock()
	if p == nil && maint {
		ferr = errors.Join(ferr, c.rollbackForMaintenance())
	}
	if ferr != nil && outcome == nil && !errors.Is(ferr, ErrNotCommitted) && (maint || c.queryOnly && errors.Is(ferr, sqlite3.READONLY)) {
		// Interrupted by maintenance, or refused by closed admission.
		ferr = fmt.Errorf("%w: %w: %w", ErrNotCommitted, ErrRetry, ferr)
	}
	if outcome != nil {
		ferr = errors.Join(outcome, ferr)
	}
	if done != 0 && errors.Is(ferr, ErrNotCommitted) {
		// The Run's commit stands: what failed after it must not read as its
		// outcome.
		ferr = fmt.Errorf("transaction %d committed, then: %s", done, ferr)
	}
	return done, ferr
}

// walHook runs after SQLite completes a commit on this connection.
func (c *Conn) walHook(db *sqlite3.Conn, schema string, pages int) error {
	l := c.l
	l.cmu.Lock()
	var deposed bool
	if schema == "main" && c.uploaded != 0 {
		c.done, c.uploaded, c.inCommit = c.uploaded, 0, false
		deposed = c.retired
		l.uploading = false
		l.cond.Broadcast()
	}
	l.cmu.Unlock()
	if deposed {
		// Only a later leader's compaction retires a commit the leader had
		// not resolved yet, so the leader is deposed (decision 6).
		_ = l.stop(fmt.Errorf("%w: own commit %d already retired", ErrFenced, c.done))
		return nil
	}
	if schema == "main" && pages >= autoCheckpointPages {
		_, _, _ = db.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE)
	}
	return nil
}

// rollbackHook runs when SQLite rolls a transaction back on this
// connection. After an upload that succeeded, that leaves the local image
// without a durable commit: fail-stop.
func (c *Conn) rollbackHook() {
	l := c.l
	l.cmu.Lock()
	defer l.cmu.Unlock()
	if c.uploaded != 0 {
		c.outcome = l.stopLocked(fmt.Errorf("SQLite rolled back transaction %d after storage committed it", c.uploaded))
	}
}

// Close closes the connection, rolling back an open transaction.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.l
	l.cmu.Lock()
	delete(l.conns, c.n)
	l.cmu.Unlock()
	if c.db == nil {
		return nil
	}
	err := c.db.Close()
	c.db = nil
	return err
}

// rollbackForMaintenance rolls back the open transaction of a connection
// maintenance asked to give it up, with c.mu held. The connection is then
// refused until closed: its client was in a transaction that no longer
// exists.
func (c *Conn) rollbackForMaintenance() error {
	l := c.l
	l.cmu.Lock()
	c.maint = false
	l.cmu.Unlock()
	if c.db == nil || c.db.TxnState("main") == sqlite3.TXN_NONE {
		return nil
	}
	retry := fmt.Errorf("%w: %w: transaction rolled back for maintenance", ErrNotCommitted, ErrRetry)
	err := c.db.Exec("ROLLBACK")
	l.cmu.Lock()
	c.aborted = retry
	l.cmu.Unlock()
	return errors.Join(retry, err)
}

// startMaintenance begins decision 10's maintenance, with l.cmu held:
// admission closes and no new create begins until it ends.
func (l *Leader) startMaintenance() {
	if l.maintaining || l.stopped != nil {
		return
	}
	l.maintaining, l.admitClosed, l.noCreates = true, true, true
	l.wg.Add(1)
	go l.maintain()
}

// maintain runs decision 10's steps 3 to 8. Compaction, retirement and
// garbage collection, the second half of step 7, are not part of this
// version: maintenance stops where compaction would begin, and writes are
// readmitted only while the tail is under its bound.
func (l *Leader) maintain() {
	defer l.wg.Done()
	// Step 3: a commit already in its upload completes or fails.
	l.cmu.Lock()
	for l.uploading && l.stopped == nil {
		l.cond.Wait()
	}
	var conns []*Conn
	for _, c := range l.conns {
		if c != l.mconn {
			c.maint = true
			if c.interrupt != nil {
				c.interrupt()
			}
			conns = append(conns, c)
		}
	}
	stopped := l.stopped
	l.cmu.Unlock()
	if stopped != nil {
		return
	}
	// Steps 4 and 5: every other transaction, writing, queued or reading,
	// is rolled back; a running one when its Run returns.
	for _, c := range conns {
		c.mu.Lock()
		l.cmu.Lock()
		maint := c.maint
		l.cmu.Unlock()
		if maint {
			_ = c.rollbackForMaintenance()
		}
		c.mu.Unlock()
	}
	// Step 6: a TRUNCATE checkpoint, retried after BUSY. Seeing the WAL
	// truncated to zero bytes advances the base version (step 7).
	for {
		if l.err() != nil {
			return
		}
		_, err := l.mconn.Run(nil, func(db *sqlite3.Conn) error {
			_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE)
			return err
		})
		if err == nil {
			break
		}
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(maintenanceBackoff):
		}
	}
	// Step 7 continues with compaction up to the new base version: later
	// work. Step 8: readmit writes once both bounds are clear.
	l.cmu.Lock()
	defer l.cmu.Unlock()
	l.maintaining = false
	l.cond.Broadcast()
	if l.head-l.manifestTxn < l.cfg.TailBound {
		l.admitClosed, l.noCreates = false, false
	}
}

// advanceBase moves the base version to the head when SQLite restarts the
// WAL from its beginning: the main file then holds exactly the head's
// state, so the pages checkpoints wrote are dropped and read from storage
// as of the new base version (decision 7).
func (l *Leader) advanceBase() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cmu.Lock()
	cs, head, size := l.pending, l.head, l.headSize
	l.pending = nil
	l.cmu.Unlock()
	for _, c := range cs {
		if c.entries == nil && !c.h.EpochBump || c.h.Txn != l.tmpl.Txn()+1 {
			return l.stop(fmt.Errorf("engine: cannot index commit %d at base %d", c.h.Txn, l.tmpl.Txn()))
		}
		l.tmpl.add(c)
	}
	l.base = head
	l.ovMu.Lock()
	defer l.ovMu.Unlock()
	clear(l.overlay)
	l.size, l.baseLimit = int64(size)*l.ps, size
	if err := os.Truncate(l.path, 0); err != nil {
		return l.stop(err)
	}
	return nil
}

// clone returns a reader at the same state as r, built without I/O.
func (r *Reader) clone() *Reader {
	n := &Reader{st: r.st, keys: r.keys, cache: r.cache}
	n.reset(r.a, r.commits)
	return n
}

func (l *Leader) injected(file, op string, off int64, n int) error {
	if l.fault == nil {
		return nil
	}
	return l.fault(file, op, off, n)
}

// The transaction number never wraps (invariant 1).
var errMaxTxn = fmt.Errorf("engine: transaction number %d reached", uint64(math.MaxUint64))
