package engine

import (
	"bytes"
	"cmp"
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
	"reflect"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
	// ErrTailBound, with ErrNotCommitted, means the log above the manifest
	// is at the leader's tail bound. Reads and writes are refused until a
	// compaction clears it, and the error also matches ErrRetry. If the
	// leader cannot compact, because the new manifest would pass its
	// maximum size, it does not match ErrRetry and says why: the refusal
	// lasts for the life of the leader.
	ErrTailBound = errors.New("engine: the log tail is at its bound until a compaction clears it")
	// ErrBroken means the Run panicked outside the commit path, as on a
	// stack overflow in SQLite, or its function reported the connection
	// broken by returning an error that wraps ErrBroken. The engine has
	// discarded the connection without SQLite's cooperation and released its
	// locks; nothing of the Run's open transaction is visible. With
	// ErrNotCommitted the Run committed nothing; with a nonzero txn its
	// commit stands and the connection broke after it. The break does not
	// stop the leader (record 0004, decision 6); a connection that could not
	// be discarded stops it instead, and its Run does not report ErrBroken.
	ErrBroken = errors.New("engine: connection broken and discarded")
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
	// hold above the manifest's transaction number, at most 32,768. Once it
	// holds that many, or 2,097,152 page entries, commits are refused and
	// maintenance starts, which compacts. Those are half of what a reader
	// walks (record 0004 decision 10). Compaction starts in the background
	// at half of either, or at CompactBytes.
	TailBound uint64
	// CompactBytes starts a compaction once the commit objects above the
	// manifest hold this many bytes; 256 MiB if 0.
	CompactBytes int64
	// CompactInterval starts a compaction once this long has passed since
	// the last one and the log above the manifest is not empty; a minute if
	// 0.
	CompactInterval time.Duration
	// Grace is how long a manifest has been current before the commits it
	// covers are retired, and how long base objects found to be garbage
	// wait before they are deleted, so that readers on older manifests
	// usually finish; five minutes if 0. Safety does not depend on it.
	Grace time.Duration
	// MaxMemory is each connection's SQLite memory ceiling in bytes.
	MaxMemory int64
	// TempBudget is the most bytes SQLite's temporary files (sorts,
	// temporary tables, statement journals) may hold at once over all the
	// leader's connections. A write past it fails its statement with
	// SQLITE_FULL, which can make SQLite roll back the whole transaction;
	// the budget is shared, so another connection's sort can cause that.
	TempBudget int64
	// Cache holds pages read from storage; it may be shared.
	Cache *Cache
	// Seen is what this node read of the manifest before it took office,
	// oldest first. Retirement measures the grace period of a manifest
	// from the first time this node read it, so the period need not start
	// again with each new leader.
	Seen []Seen
}

// Seen is a manifest's database and transaction number and a time the
// manifest was current: a read of it from storage had begun by then.
type Seen struct {
	DB  format.DatabaseID
	Txn uint64
	At  time.Time
}

// Leader serves one database on this node while it holds office: SQLite in
// WAL mode with a main file served from object storage, and every commit
// created in the commit log before SQLite reports it, as record 0004
// decisions 5 to 7 and 10 describe.
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
	// beforePhaseTwo, if set, runs when SQLite sends a connection's phase
	// two, before the engine handles it. Tests use it to pause there.
	beforePhaseTwo func(*Conn)
	// fault, if set, runs before each write, sync and truncate of a local
	// file ("wal" or "main"), the engine's own WAL truncation included; an
	// error it returns fails the call. Tests use it for disk failures.
	fault func(file, op string, off int64, n int) error

	// tempUsed is the bytes SQLite's temporary files hold; tempRefused
	// counts the writes refused for passing TempBudget.
	tempUsed, tempRefused atomic.Int64

	// mu orders main-file reads against advances of the base version. No
	// storage read is made while it is held.
	mu   sync.RWMutex
	base uint64  // the base version (decision 7)
	tmpl *Reader // the base version's index, shared by every connection

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
	tailPages   int    // page entries in the log above the manifest
	pending     []*commit
	wal         walState
	uploading   bool // a commit is between its first create and its local completion; one at a time
	snapshots   int  // Snapshot calls whose start runs; no create begins meanwhile
	// ids and bumps index the headers of the log above the manifest, which
	// this version keeps whole (record 0005, requirement 11): each commit
	// identifier's transaction, and the epoch bumps in order.
	ids         map[string]uint64
	bumps       []uint64
	conns       map[int64]*Conn
	nextConn    int64
	admitClosed bool // client writes refused (decision 10, step 1)
	noCreates   bool // creates not begun (decision 10, step 2)
	maintaining bool
	scratch     *os.File // the commit object being uploaded
	mconn       *Conn    // the leader's own connection, for maintenance
	// tailBytes is the size of the commit objects above the manifest.
	tailBytes int64
	// compactRefused is why the leader cannot compact: the new manifest
	// would pass its maximum size.
	compactRefused error
	// indexFloor: ids and bumps cover every number above it to the head.
	indexFloor uint64

	// cctx ends at Close and at fail-stop: compaction, retirement and
	// collection issue no write after the leader stops.
	cctx  context.Context
	cstop context.CancelFunc
	wake  chan struct{} // wakes the compaction loop

	// cmpMu is held by one compaction, retirement or collection at a time
	// and guards what follows.
	cmpMu          sync.Mutex
	lastWrite      time.Time // the last manifest write attempt
	failures       int       // compaction attempts that failed since the last success
	lastCompact    time.Time
	retiredThrough uint64     // every commit up to it was read as retired or as a bump
	covered        []coverage // manifests known current, oldest first
	garbage        []string   // base keys found garbage at garbageAt
	garbageAt      time.Time
	collectWanted  bool // a compaction published since the last collection looked
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
// and, unless office's takeover read it and the manifest has not changed
// since, the log tail up to office.Head; never the database's pages.
func Lead(ctx context.Context, st storage.Store, keys format.Keys, dir string, office Office, cfg Config) (*Leader, error) {
	if cfg.WALBound <= 0 || cfg.TailBound == 0 || cfg.TailBound > uint64(maxTail/2) || cfg.MaxMemory <= 0 || cfg.TempBudget <= 0 || cfg.Cache == nil || //nolint:gosec // maxTail is a small positive constant
		cfg.CompactBytes < 0 || cfg.CompactInterval < 0 || cfg.Grace < 0 {
		return nil, fmt.Errorf("engine: Config needs a WAL bound, a tail bound from 1 to %d, a memory ceiling, a temporary-file budget and a cache, and no negative compaction setting", maxTail/2)
	}
	tmpl, err := openTail(ctx, st, keys, cfg.Cache, office)
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
		head: office.Head, headSize: tmpl.Size(), manifestTxn: m.Txn, tailPages: entryCount(tmpl.commits),
		wal:        walState{start: -1},
		conns:      map[int64]*Conn{},
		ids:        map[string]uint64{},
		indexFloor: m.Txn, retiredThrough: m.RetiredThrough,
		wake: make(chan struct{}, 1),
	}
	for _, c := range tmpl.commits {
		l.indexLocked(c.h)
		l.tailBytes += c.h.Size()
	}
	// A tail already at its bound refuses reads and writes from the start,
	// until the compaction the loop starts at once clears it: the base
	// version is the head.
	l.admitClosed = l.tailFullLocked()
	l.noCreates = l.admitClosed
	l.cond = sync.NewCond(&l.cmu)
	l.path = filepath.Join(l.dir, "db")
	if err := os.RemoveAll(l.dir); err != nil { //nolint:gosec // the caller's directory and a hex database identifier
		return nil, err
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil { //nolint:gosec // as above
		return nil, err
	}
	if l.scratch, err = os.OpenFile(filepath.Join(l.dir, "commit"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600); err != nil { //nolint:gosec // as above
		return nil, err
	}
	var name [8]byte
	if _, err := rand.Read(name[:]); err != nil {
		return nil, err
	}
	l.vfs = "skylite-" + hex.EncodeToString(name[:])
	l.ctx, l.cancel = context.WithCancel(context.WithoutCancel(ctx))
	l.cctx, l.cstop = context.WithCancel(l.ctx)
	vfs.Register(l.vfs, (*leaderVFS)(l))
	if l.mconn, err = l.Open(ctx); err != nil {
		_ = l.Close()
		return nil, err
	}
	// The manifest Lead read covers the log up to it from now on, and the
	// manifests this node read before covered the log up to theirs since.
	// One of another database, or with no time or a time still to come, is
	// no read of this log, and is ignored.
	now := time.Now()
	for _, s := range cfg.Seen {
		if s.DB == m.DB && !s.At.IsZero() && !s.At.After(now) && s.Txn <= m.Txn && (len(l.covered) == 0 || s.Txn >= l.covered[len(l.covered)-1].txn && !s.At.Before(l.covered[len(l.covered)-1].at)) {
			l.covered = append(l.covered, coverage{s.Txn, s.At})
		}
	}
	l.covered = append(l.covered, coverage{m.Txn, time.Now()})
	l.lastCompact = time.Now()
	l.cmu.Lock()
	l.wakeLocked()
	l.cmu.Unlock()
	l.wg.Add(1)
	go l.compactLoop()
	return l, nil
}

// openTail opens the database as of office.Head, from the commits office's
// takeover walked if the manifest is still the one it last read, and
// otherwise by a walk of its own. It takes the walked commits once: copies
// of office then carry none.
func openTail(ctx context.Context, st storage.Store, keys format.Keys, cache *Cache, office Office) (*Reader, error) {
	t := office.tail
	if t == nil || len(t.commits) == 0 {
		return Open(ctx, st, keys, cache, office.Head)
	}
	cs, version := t.commits, t.version
	*t = tail{}
	r := &Reader{st: st, keys: keys, cache: cache}
	a, err := r.readManifest(ctx, anchor{}, nil)
	if err != nil {
		return nil, err
	}
	if a.version != version || cs[len(cs)-1].h.Txn != office.Head {
		return Open(ctx, st, keys, cache, office.Head)
	}
	r.reset(a, cs)
	return r, nil
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

// Snapshot runs start, which must begin a read transaction on one of the
// leader's connections, and returns the transaction number of the snapshot
// that read holds (record 0005, requirement 12). The number is exact:
// Snapshot first waits until no commit is between its create and SQLite's
// completion of it, so the head is the newest commit SQLite has published,
// and while start runs no create begins, so neither changes before the read
// has taken its snapshot. That wait lasts for each commit it meets, a
// create of up to createAttempts attempts and SQLite's completion; a writer
// queued behind that commit can begin its create first, so the wait can
// span several commits. It ends with ctx's error when ctx is done. Commits wait while start runs, so start should only begin the read,
// with a statement that reads little, such as PRAGMA schema_version inside
// BEGIN; its storage reads end with its Run's context. start must not wait
// for SQLite's write lock: a writer holding it may be waiting for start to
// end before its create, so a BEGIN IMMEDIATE would wait out the busy
// timeout. A write transaction's number is Head() once BEGIN IMMEDIATE has
// returned: no commit lands while it holds the lock, and SQLite publishes a
// commit before it releases it.
func (l *Leader) Snapshot(ctx context.Context, start func() error) (uint64, error) {
	defer context.AfterFunc(ctx, func() {
		l.cmu.Lock()
		defer l.cmu.Unlock()
		l.cond.Broadcast()
	})()
	l.cmu.Lock()
	for l.uploading && l.stopped == nil && ctx.Err() == nil {
		l.cond.Wait()
	}
	if err := cmp.Or(l.stopped, ctx.Err()); err != nil {
		l.cmu.Unlock()
		return 0, err
	}
	l.snapshots++
	l.cmu.Unlock()
	defer func() {
		l.cmu.Lock()
		defer l.cmu.Unlock()
		if l.snapshots--; l.snapshots == 0 {
			l.cond.Broadcast()
		}
	}()
	if err := start(); err != nil {
		return 0, err
	}
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.head, nil
}

// Lookup answers a resolve (record 0005, decision 4) from the leader's
// headers, in one observation ordered against every head advance: txn is
// the transaction above `above` that carries commit identifier id, and
// firstBump the first epoch bump above `above`, each 0 if there is none,
// and head is the leader's head. Every value of above is valid. ok is false
// if the headers held do not cover every transaction from above+1 to head.
// This version holds the headers above the manifest it took office on and
// of each commit since, and once a compaction has passed them drops those
// more than 65,536 numbers below the head, so ok is false for `above` below
// that point; a caller must read that as unknown, never as not committed.
// It reads no header from storage. Lookup
// answers from memory on a stopped leader too: the caller does decision
// 4's step 1 itself, with Err. A caller that also reads a table of its own
// channels reads it before Lookup: a channel it found absent can then send
// no commit above head.
func (l *Leader) Lookup(id []byte, above uint64) (txn, firstBump, head uint64, ok bool) {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	if t := l.ids[string(id)]; t > above {
		txn = t
	}
	if i := sort.Search(len(l.bumps), func(i int) bool { return l.bumps[i] > above }); i < len(l.bumps) {
		firstBump = l.bumps[i]
	}
	return txn, firstBump, l.head, above >= l.indexFloor
}

// indexLocked adds a commit's header to the lookup index, with l.cmu held.
func (l *Leader) indexLocked(h format.CommitHeader) {
	switch {
	case h.EpochBump:
		l.bumps = append(l.bumps, h.Txn)
	case len(h.CommitID) > 0:
		l.ids[string(h.CommitID)] = h.Txn
	}
}

// Err is nil while the leader serves, and otherwise wraps ErrStopped and
// says why it stopped.
func (l *Leader) Err() error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.stopped
}

// Admits returns the error a client connection's Run would fail with now,
// before it reaches SQLite: the leader stopped, its tail bound, or
// maintenance; nil if it would run.
func (l *Leader) Admits() error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	switch {
	case l.stopped != nil:
		return fmt.Errorf("%w: %w", ErrNotCommitted, l.stopped)
	case l.admitClosed && l.tailFullLocked():
		return l.tailErrLocked()
	case l.admitClosed:
		return fmt.Errorf("%w: %w: maintenance is running", ErrNotCommitted, ErrRetry)
	}
	return nil
}

// Close stops serving, closes every connection once its Run has returned,
// and deletes the database's local files.
func (l *Leader) Close() error {
	l.cmu.Lock()
	_ = l.stopLocked(errors.New("engine: leader closed"))
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

// stopLocked stops the leader, with l.cmu held, and returns why it
// stopped. That error matches ErrStopped only: the cause is kept as text, so
// that it never gives another Run's error the class of the Run that caused
// the stop.
func (l *Leader) stopLocked(cause error) error {
	if l.stopped == nil {
		l.stopped = fmt.Errorf("%w: %v", ErrStopped, cause) //nolint:errorlint // see above
		if l.cstop != nil {
			l.cstop()
		}
		l.cond.Broadcast()
		for _, c := range l.conns {
			c.endLocked()
		}
	}
	return l.stopped
}

func (l *Leader) err() error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.stopped
}

// Open opens a new SQLite connection to the database. ctx bounds the Open:
// its storage reads and SQLite's set-up statements end when ctx is done, and
// a panic in them discards the connection if SQLite then closes it, and
// otherwise stops the leader (record 0007, requirement 5). An
// Open that fails without a panic after maintenance has cancelled its
// storage reads fails with an error that matches ErrNotCommitted and
// ErrRetry, whatever ended it: SQLite reports a failed read as an I/O error
// without its cause, so a storage error during maintenance is told to retry
// too. One that fails without a panic on a leader that has stopped fails
// with an error that matches ErrStopped, for the same reason.
func (l *Leader) Open(ctx context.Context) (_ *Conn, err error) {
	l.cmu.Lock()
	if l.stopped != nil {
		defer l.cmu.Unlock()
		return nil, l.stopped
	}
	l.nextConn++
	c := &Conn{l: l, n: l.nextConn, abort: make(chan struct{})}
	c.mu.Lock()
	defer c.mu.Unlock()
	l.conns[c.n] = c
	// ictx ends Open's set-up statements when ctx ends or at Close; octx
	// ends its storage reads then and at maintenance too.
	ictx, icancel := context.WithCancel(ctx)
	defer icancel()
	defer context.AfterFunc(l.ctx, icancel)()
	octx, cancel := context.WithCancel(ictx)
	defer cancel()
	c.ctx, c.interrupt = octx, cancel
	if l.maintaining {
		// Maintenance may have listed the connections already, and a read in
		// setup holds a WAL read lock its checkpoint waits on.
		cancel()
	}
	l.cmu.Unlock()
	defer func() {
		l.cmu.Lock()
		c.ctx, c.interrupt = nil, nil
		l.cmu.Unlock()
		p := recover()
		if p != nil {
			err = fmt.Errorf("engine: opening a connection panicked: %v", p)
		}
		if err == nil {
			return
		}
		switch stopped := l.err(); {
		case p == nil && stopped != nil:
			// SQLite reports the stop as an I/O error without its cause.
			err = fmt.Errorf("%w: the Open failed: %w", stopped, err)
		case p == nil && octx.Err() != nil && ictx.Err() == nil:
			err = fmt.Errorf("%w: %w: maintenance interrupted the Open: %w", ErrNotCommitted, ErrRetry, err)
		}
		// A connection that panicked inside sqlite3.Open cannot be closed.
		// One that is not closed may hold locks the others wait for.
		if c.db == nil && p != nil || c.db != nil && !closeQuietly(c.db) {
			_ = l.stop(fmt.Errorf("engine: a connection that failed to open could not be closed: %w", err))
		}
		c.db = nil // maintenance may already hold c
		l.cmu.Lock()
		delete(l.conns, c.n)
		l.cmu.Unlock()
	}()

	u := url.URL{Scheme: "file", Path: l.path, RawQuery: url.Values{"vfs": {l.vfs}, "conn": {fmt.Sprint(c.n)}}.Encode()}
	if c.db, err = sqlite3.OpenContext(sqlite3.WithMaxMemory(ictx, l.cfg.MaxMemory), u.String()); err != nil {
		return nil, err
	}
	c.db.SetInterrupt(ictx)
	if err := c.setup(); err != nil {
		return nil, err
	}
	c.db.SetInterrupt(context.Background())
	return c, nil
}

// closeQuietly closes db and reports whether that worked without an error
// or a panic.
func closeQuietly(db *sqlite3.Conn) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return db.Close() == nil
}

// release frees db without running SQLite, whose state a panic may have
// left half-updated; after a stack overflow the instance's stack pointer is
// past the end of its stack, so no call into it returns. It closes every
// file the instance holds through the VFS, which releases the main file's
// lock and closes the WAL, the shared-memory index and temporary files, and
// then unmaps the instance's memory. The index is mapped into that memory,
// and the mapping keeps the index's open file description, and the locks on
// it, alive until it goes. It reports whether it freed db; db must not be
// used again.
//
// shortcut: ncruces v0.37.0 has no API to drop an instance, so release
// reaches the connection's unexported wrapper by reflection and calls its
// Close; the broken-connection tests fail if an upgrade changes that, and
// release then reports false, which stops the leader as before.
func release(db *sqlite3.Conn) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	f := reflect.ValueOf(db).Elem().FieldByName("wrp")
	if f.Kind() != reflect.Pointer || f.IsNil() {
		return false
	}
	w, ok := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface().(io.Closer) //nolint:gosec // see the shortcut above
	if ok {
		_ = w.Close()
	}
	return ok
}

// Conn is one SQLite connection to the leader's database. It is safe for
// concurrent use, one Run at a time.
type Conn struct {
	l  *Leader
	n  int64
	mu sync.Mutex // held by Open, Run, Close and maintenance
	db *sqlite3.Conn
	// tempSeen is l.tempRefused when the running Run began.
	tempSeen int64

	// Guarded by l.cmu.
	ctx       context.Context // the running Run's; its storage reads use it
	main      *mainFile
	id        []byte // the commit identifier for this Run
	idUsed    bool
	inCommit  bool   // from the first create until the commit completes locally
	uploaded  uint64 // committed in storage, not yet completed by SQLite
	done      uint64 // committed during this Run
	outcome   error  // why a commit during this Run failed
	retired   bool   // the uploaded commit was found retired: the leader is deposed
	interrupt context.CancelFunc
	maint     bool          // maintenance asked this connection to give up its transaction
	aborted   error         // maintenance rolled this connection's transaction back
	abort     chan struct{} // closed by endLocked
}

// Aborted returns a channel that is closed once the engine has ended the
// connection's transaction, or will refuse it, without its client asking:
// maintenance rolled it back (record 0004 decision 10, steps 4 and 5), or
// the leader stopped. The connection's next Run fails and says why. Close,
// and a discard after a panic, close it too. A server that holds its own
// write lock for the transaction waits on it to release that lock and
// report the failure (record 0005, requirement 15). SQLite itself rolls an
// explicit transaction back when it interrupts a write statement in it, as
// at the end of a Run's context: the channel stays open then, the Run's
// error is the signal, and the connection's transaction state is NONE.
func (c *Conn) Aborted() <-chan struct{} { return c.abort }

// endLocked closes the Aborted channel, once, with l.cmu held.
func (c *Conn) endLocked() {
	select {
	case <-c.abort:
	default:
		close(c.abort)
	}
}

func (c *Conn) setup() error {
	if err := c.db.Exec("PRAGMA synchronous=FULL"); err != nil {
		return err
	}
	if mode, err := c.pragma("journal_mode"); err != nil || mode != "wal" {
		return errors.Join(fmt.Errorf("engine: database not in WAL mode (%q)", mode), err)
	}
	if s, err := c.pragma("synchronous"); err != nil || s != "2" {
		return errors.Join(fmt.Errorf("engine: synchronous is %q, want FULL", s), err)
	}
	if err := c.db.BusyTimeout(busyTimeout); err != nil {
		return err
	}
	// Exceeding the memory ceiling panics in ncruces, and a panic costs the
	// connection (see finish), or, in Open, the database when SQLite cannot
	// close the connection. Capping values and statements well below the
	// ceiling turns the commonest way there, one huge value, into an
	// SQLITE_TOOBIG error.
	limit := int(min(1e9, c.l.cfg.MaxMemory/4))
	c.db.Limit(sqlite3.LIMIT_LENGTH, limit)
	c.db.Limit(sqlite3.LIMIT_SQL_LENGTH, limit)
	c.db.WALHook(c.walHook)
	c.db.RollbackHook(c.rollbackHook)
	c.db.CommitHook(c.commitHook)
	return nil
}

// readContext is the context for the connection's storage reads: its
// running Run's, or the leader's outside one.
func (c *Conn) readContext() context.Context {
	l := c.l
	l.cmu.Lock()
	defer l.cmu.Unlock()
	if c.ctx != nil {
		return c.ctx
	}
	return l.ctx
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
// statements beyond its return, and must not replace db's interrupt
// context. ctx ends f's statements when it is done, page reads from storage
// included; Close, and maintenance, end them too. id is the commit
// identifier, at most format.MaxCommitIDLen bytes, of the one transaction f
// may commit; a commit without one, or a second commit in one Run, is
// refused as not committed. A transaction that changes no page commits
// nothing and needs none. f may close a writing statement before stepping
// it to its end, as with an INSERT ... RETURNING whose rows it does not all
// read: in autocommit SQLite then commits it at the close, and the Run
// reports that commit as any other.
//
// Every Run ends in exactly one of these classes:
//   - txn is nonzero: f's commit is in the durable history. A non-nil error
//     then means committed-then-failed: it reports what failed later in the
//     Run and matches neither ErrNotCommitted nor ErrUnknown, so a caller
//     must look at txn before err. It matches ErrBroken if the connection
//     broke after the commit.
//   - txn is 0 and err is nil: f succeeded and committed nothing.
//   - err matches ErrNotCommitted: the Run committed nothing, and nothing
//     of it can enter the history. It also matches ErrStopped if the leader
//     stopped, ErrBroken if the connection broke and was discarded, ErrRetry
//     if a retry can succeed, ErrTailBound at the tail bound, ErrTooLarge if
//     no retry can, and wraps f's own error, if any. A transaction f left
//     open stays open for the next Run unless maintenance rolled it back,
//     which the error says, or SQLite did, which it does not: SQLite can
//     roll the whole transaction back after SQLITE_FULL (the temporary-file
//     budget's included), SQLITE_IOERR, SQLITE_NOMEM or an interrupt, so f
//     reads db.GetAutocommit() after a failed statement (record 0005,
//     decision 6).
//   - err matches ErrUnknown: f's commit may be in the history, now or
//     later. The leader has stopped.
func (c *Conn) Run(ctx context.Context, id []byte, f func(db *sqlite3.Conn) error) (txn uint64, err error) {
	if len(id) > format.MaxCommitIDLen {
		return 0, fmt.Errorf("%w: a commit identifier of %d bytes, above %d", ErrNotCommitted, len(id), format.MaxCommitIDLen)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.l
	ictx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(l.ctx, cancel)()

	l.cmu.Lock()
	switch {
	case l.stopped != nil:
		err = fmt.Errorf("%w: %w", ErrNotCommitted, l.stopped)
	case c.db == nil:
		err = fmt.Errorf("%w: the connection is closed", ErrNotCommitted)
	case l.admitClosed && c != l.mconn && l.tailFullLocked():
		err = l.tailErrLocked()
	case c.aborted != nil:
		err = c.aborted
	case l.admitClosed && c != l.mconn:
		// From decision 10's step 1 to its step 8, reads are refused as
		// well as writes (amended by ADR 0005), before any reaches SQLite.
		err = fmt.Errorf("%w: %w: maintenance is running", ErrNotCommitted, ErrRetry)
	}
	if err != nil {
		l.cmu.Unlock()
		return 0, err
	}
	c.id, c.idUsed, c.done, c.outcome = bytes.Clone(id), false, 0, nil
	c.interrupt, c.ctx = cancel, ictx
	l.cmu.Unlock()
	c.tempSeen = l.tempRefused.Load()
	c.db.SetInterrupt(ictx)

	defer func() {
		p := recover()
		txn, err = c.finish(p, err)
	}()
	return 0, f(c.db)
}

// finish ends a Run: it applies the rules for a panic or an error in the
// commit path and reports the Run's class.
//
// A panic in the commit path stops the database on this node (decision 6).
// A panic outside it, or an error of f's that wraps ErrBroken, breaks the
// connection: release discards it without running SQLite, and the Run
// fails with ErrBroken. Only the leader's own connection, and a connection
// release cannot free, stop the database instead.
func (c *Conn) finish(p any, ferr error) (uint64, error) {
	l := c.l
	if p == nil {
		c.db.SetInterrupt(context.Background())
	}
	l.cmu.Lock()
	inCommit := c.inCommit
	c.interrupt, c.id, c.ctx = nil, nil, nil
	l.cmu.Unlock()
	broken := p != nil || errors.Is(ferr, ErrBroken)
	// The leader's own connection is never discarded: maintenance needs it.
	discarded := broken && !inCommit && c != l.mconn && c.discard(true)
	if !discarded && errors.Is(ferr, ErrBroken) {
		// ErrBroken says the connection was discarded; this one was not.
		ferr = errors.New(ferr.Error())
	}
	l.cmu.Lock()
	uploaded, done, outcome, maint := c.uploaded, c.done, c.outcome, c.maint
	unsent := "nothing of this Run was sent to storage"
	if done != 0 {
		unsent = fmt.Sprintf("nothing of this Run after transaction %d was sent to storage", done)
	}
	// An earlier commit of this Run whose outcome is unknown keeps that
	// outcome: nothing below may say the Run sent nothing.
	unknown := errors.Is(outcome, ErrUnknown)
	switch {
	case discarded:
		why := "its function reported it broken"
		if p != nil {
			why = fmt.Sprintf("panic: %v", p)
		}
		note := fmt.Errorf("%w: %w: %s; the connection was closed and discarded (%s)", ErrNotCommitted, ErrBroken, unsent, why)
		if unknown {
			note = fmt.Errorf("%w: the connection was closed and discarded (%s)", ErrBroken, why)
		}
		outcome = errors.Join(outcome, note)
	case broken || inCommit:
		// The Run broke a connection that could not be discarded, or the
		// commit path ended without SQLite completing a commit whose create
		// may have taken effect.
		cause := fmt.Errorf("%w: a commit's create may have taken effect", ErrUnknown)
		switch {
		case !inCommit && unknown:
			cause = errors.New("the connection could not be discarded")
		case !inCommit:
			cause = fmt.Errorf("%w: %s, but the connection could not be discarded", ErrNotCommitted, unsent)
		}
		if uploaded != 0 {
			cause = fmt.Errorf("transaction %d is committed in storage but SQLite did not complete it", uploaded)
			done = uploaded
		}
		if p != nil {
			cause = fmt.Errorf("%w (panic: %v)", cause, p)
		}
		cause = errors.Join(cause, ferr)
		_ = l.stopLocked(cause)
		outcome = errors.Join(outcome, fmt.Errorf("%w: %w", ErrStopped, cause))
		c.inCommit, c.uploaded = false, 0
		l.uploading = false
		l.cond.Broadcast()
	}
	l.cmu.Unlock()
	if p == nil && maint {
		ferr = errors.Join(ferr, c.rollbackForMaintenance())
	}
	if ferr != nil && outcome == nil && !errors.Is(ferr, ErrNotCommitted) && maint {
		// Interrupted by maintenance.
		l.cmu.Lock()
		ferr = fmt.Errorf("%w: %w", l.maintRefusalLocked(), ferr)
		l.cmu.Unlock()
	}
	if errors.Is(ferr, sqlite3.FULL) && l.tempRefused.Load() != c.tempSeen {
		// shortcut: the count is leader-wide, so a full local disk during
		// another connection's refusal reads as the budget; a per-connection
		// count needs the connection in the temporary file's open call.
		ferr = fmt.Errorf("engine: SQLite's temporary files would pass the leader's budget of %d bytes: %w", l.cfg.TempBudget, ferr)
	}
	return classify(done, errors.Join(outcome, ferr), l.err())
}

// classify gives a Run's result exactly one of the classes Run documents.
// err holds f's error and the Run's own commit outcome; stopped is why the
// leader stopped, if it has.
func classify(done uint64, err, stopped error) (uint64, error) {
	switch {
	case err == nil:
	case done != 0:
		if errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrUnknown) {
			// The Run's commit stands: what failed after it must not read
			// as its outcome. A broken connection is still reported broken:
			// finish reports ErrBroken only for a discarded one.
			if errors.Is(err, ErrBroken) {
				err = fmt.Errorf("transaction %d committed, then: %w: %s", done, ErrBroken, err) //nolint:errorlint // must not match either class
			} else {
				err = fmt.Errorf("transaction %d committed, then: %s", done, err) //nolint:errorlint // must not match either class
			}
		}
	case errors.Is(err, ErrUnknown):
		if errors.Is(err, ErrNotCommitted) {
			// A failure after the commit whose outcome is unknown.
			err = fmt.Errorf("%w: %s", ErrUnknown, err) //nolint:errorlint // must not match ErrNotCommitted
		}
	case errors.Is(err, ErrNotCommitted):
	case stopped != nil:
		// The leader stopped under the Run: SQLite reports only an I/O
		// error. No create of it was sent, or its outcome would be set.
		err = fmt.Errorf("%w: %w: %w", ErrNotCommitted, stopped, err)
	default:
		err = fmt.Errorf("%w: nothing of this Run was sent to storage: %w", ErrNotCommitted, err)
	}
	return done, err
}

// discard ends the connection, at Close or once it is broken, and reports
// whether its locks and memory are released; the connection then leaves the
// leader. A connection that is not broken is closed through SQLite, each
// statement and then the connection under recover; a statement whose Close
// panics counts as closed, since ncruces finalizes it first and then
// re-reports its out-of-memory error that way. A broken connection, or one
// whose close fails, is released instead.
func (c *Conn) discard(broken bool) bool {
	if !broken {
		for _, s := range slices.Collect(c.db.Stmts()) {
			func() {
				defer func() { _ = recover() }()
				_ = s.Close()
			}()
		}
		broken = !closeQuietly(c.db)
	}
	if broken && !release(c.db) {
		return false
	}
	c.db = nil
	c.l.cmu.Lock()
	delete(c.l.conns, c.n)
	c.endLocked()
	c.l.cmu.Unlock()
	return true
}

// walHook runs after SQLite completes a commit on this connection.
func (c *Conn) walHook(db *sqlite3.Conn, schema string, pages int) error {
	if schema == "main" && pages >= autoCheckpointPages {
		_, _, _ = db.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE)
	}
	return nil
}

// commitHook runs when SQLite begins a commit of a write transaction on
// this connection, an empty one included, and never between a commit's
// upload and its completion. An uploaded commit still pending here was
// rolled back without the rollback hook, as a failed VACUUM is, and the
// commit-phase-two file control of this commit must not complete it:
// fail-stop, and refuse this commit.
func (c *Conn) commitHook() bool {
	l := c.l
	l.cmu.Lock()
	defer l.cmu.Unlock()
	if c.uploaded != 0 {
		c.outcome = l.stopLocked(fmt.Errorf("SQLite began another commit before completing transaction %d", c.uploaded))
		return false
	}
	return true
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
	if w := l.wal.writer; w != nil && w.c == c {
		// The frames since start are dead, and SQLite rewrites their
		// checksums only at a commit: the next sync must not parse them.
		l.wal.start = -1
	}
}

// Close closes the connection, rolling back an open transaction.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.l
	l.cmu.Lock()
	delete(l.conns, c.n)
	c.endLocked()
	l.cmu.Unlock()
	if c.db == nil {
		return nil
	}
	if !c.discard(false) {
		// The connection may still hold locks that the others wait for.
		c.db = nil
		return l.stop(errors.New("engine: a connection could not be closed"))
	}
	return nil
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
	err := c.db.Exec("ROLLBACK")
	l.cmu.Lock()
	c.aborted = fmt.Errorf("%w: transaction rolled back for maintenance", l.maintRefusalLocked())
	c.endLocked()
	aborted := c.aborted
	l.cmu.Unlock()
	return errors.Join(aborted, err)
}

// maintRefusalLocked is the refusal of a transaction that maintenance
// ended, with l.cmu held: at the tail bound, the tail bound's.
func (l *Leader) maintRefusalLocked() error {
	if l.tailFullLocked() {
		return l.tailErrLocked()
	}
	return fmt.Errorf("%w: %w", ErrNotCommitted, ErrRetry)
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

// maintain runs decision 10's steps 3 to 8. Step 7 compacts only at the
// tail bound; after the WAL bound, the compaction loop folds the new base
// version once writes are readmitted. Retirement follows on the loop's
// schedule, after the grace period. Writes are readmitted only while the
// tail is under its bound; if the compaction failed, the loop retries it and
// readmits once the tail is clear.
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
		_, err := l.mconn.Run(l.ctx, nil, func(db *sqlite3.Conn) error {
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
	// Step 7 continues with compaction up to the new base version.
	l.cmu.Lock()
	full := l.tailFullLocked()
	l.cmu.Unlock()
	if full {
		l.cmpMu.Lock()
		_ = l.compactLocked()
		l.cmpMu.Unlock()
	}
	// Step 8: readmit writes once both bounds are clear.
	l.cmu.Lock()
	defer l.cmu.Unlock()
	l.maintaining = false
	l.cond.Broadcast()
	if !l.tailFullLocked() {
		l.admitClosed, l.noCreates = false, false
	}
}

// tailFullLocked reports, with l.cmu held, whether the log above the
// manifest is at the tail bound: TailBound commits, or half the page
// entries a reader indexes.
func (l *Leader) tailFullLocked() bool {
	return l.head-l.manifestTxn >= l.cfg.TailBound || l.tailPages >= maxTailPages/2
}

// tailErrLocked is the refusal at the tail bound, with l.cmu held.
func (l *Leader) tailErrLocked() error {
	if l.compactRefused != nil {
		return fmt.Errorf("%w: %w: %d commits and %d page entries above the manifest at %d, bounds %d and %d, and the leader cannot compact: %w",
			ErrNotCommitted, ErrTailBound, l.head-l.manifestTxn, l.tailPages, l.manifestTxn, l.cfg.TailBound, maxTailPages/2, l.compactRefused)
	}
	return fmt.Errorf("%w: %w: %w: %d commits and %d page entries above the manifest at %d, bounds %d and %d",
		ErrNotCommitted, ErrTailBound, ErrRetry, l.head-l.manifestTxn, l.tailPages, l.manifestTxn, l.cfg.TailBound, maxTailPages/2)
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

func (l *Leader) injected(file, op string, off int64, n int) error {
	if l.fault == nil {
		return nil
	}
	return l.fault(file, op, off, n)
}

// maxTxn is the largest transaction number: it never wraps (invariant 1),
// and it fits the client protocol's signed 64-bit integers (ADR 0005,
// requirement 13).
const maxTxn = math.MaxInt64

var errMaxTxn = fmt.Errorf("engine: transaction number %d reached", maxTxn)
