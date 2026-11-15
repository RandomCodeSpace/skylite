package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// gate pauses a store call until the test releases it. Reached is closed
// when a call arrives at it.
type gate struct {
	reached chan struct{}
	release chan struct{}
	once    sync.Once
	opened  sync.Once
}

func newGate() *gate { return &gate{reached: make(chan struct{}), release: make(chan struct{})} }

func (g *gate) wait() {
	g.once.Do(func() { close(g.reached) })
	<-g.release
}

func (g *gate) open() { g.opened.Do(func() { close(g.release) }) }

// fault says what one store call does. The zero fault passes it through.
type fault struct {
	before, after *gate // pause before the call takes effect, or after
	noEffect      bool  // fail it with no effect
	unknownNone   bool  // fail it with an unknown outcome, but with no effect
	drop          bool  // let it take effect and return an unknown outcome
	err           error // with noEffect: the error to return
	// late: return an unknown outcome at once; the call takes effect only
	// when the gate opens, as a request still in flight would.
	late *gate
}

// faultStore wraps a store. Each call asks rule what to do; it also counts
// requests, creates and the bytes uploaded.
type faultStore struct {
	storage.Store
	dir string

	late sync.WaitGroup

	mu       sync.Mutex
	rule     func(op, key string) fault
	requests int
	creates  int
	uploaded int64
}

func newFaultStore(t *testing.T) (*faultStore, format.Keys) {
	t.Helper()
	dir := t.TempDir()
	fs, err := localfs.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	keys, err := format.NewKeys("dbs/test")
	if err != nil {
		t.Fatal(err)
	}
	s := &faultStore{Store: fs, dir: dir}
	t.Cleanup(s.late.Wait)
	return s, keys
}

func (s *faultStore) setRule(r func(op, key string) fault) {
	s.mu.Lock()
	s.rule = r
	s.mu.Unlock()
}

func (s *faultStore) counts() (requests, creates int, uploaded int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.creates, s.uploaded
}

func (s *faultStore) resetCounts() {
	s.mu.Lock()
	s.requests, s.creates, s.uploaded = 0, 0, 0
	s.mu.Unlock()
}

func (s *faultStore) fault(op, key string) fault {
	s.mu.Lock()
	s.requests++
	if op == "create" {
		s.creates++
	}
	r := s.rule
	s.mu.Unlock()
	if r == nil {
		return fault{}
	}
	return r(op, key)
}

var errNoEffect = errors.New("injected failure with no effect")

func (s *faultStore) mutate(ctx context.Context, op, key string, body io.Reader, do func(context.Context, io.Reader) (string, error)) (string, error) {
	f := s.fault(op, key)
	if f.before != nil {
		f.before.wait()
	}
	if f.noEffect || f.unknownNone {
		if body != nil {
			_, _ = io.Copy(io.Discard, body)
		}
		err := f.err
		if err == nil {
			err = errNoEffect
		}
		if f.unknownNone {
			err = fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, err)
		}
		return "", &storage.Error{Op: op, Key: key, Err: err}
	}
	if body != nil {
		body = &countReader{r: body, s: s}
	}
	if f.late != nil {
		b, _ := io.ReadAll(body)
		s.late.Go(func() {
			f.late.wait()
			_, _ = do(context.WithoutCancel(ctx), bytes.NewReader(b))
		})
		return "", &storage.Error{Op: op, Key: key, Err: fmt.Errorf("%w: injected request still in flight", storage.ErrOutcomeUnknown)}
	}
	v, err := do(ctx, body)
	if f.after != nil {
		f.after.wait()
	}
	if f.drop {
		return "", &storage.Error{Op: op, Key: key, Err: fmt.Errorf("%w: injected lost response", storage.ErrOutcomeUnknown)}
	}
	return v, err
}

type countReader struct {
	r io.Reader
	s *faultStore
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.s.mu.Lock()
	c.s.uploaded += int64(n)
	c.s.mu.Unlock()
	return n, err
}

func (s *faultStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.mutate(ctx, "create", key, body, func(ctx context.Context, b io.Reader) (string, error) { return s.Store.Create(ctx, key, b) })
}

func (s *faultStore) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.mutate(ctx, "replace", key, body, func(ctx context.Context, b io.Reader) (string, error) { return s.Store.Replace(ctx, key, version, b) })
}

func (s *faultStore) read(op, key string) (fault, error) {
	f := s.fault(op, key)
	if f.before != nil {
		f.before.wait()
	}
	if f.noEffect {
		return f, &storage.Error{Op: op, Key: key, Err: errNoEffect}
	}
	return f, nil
}

func (s *faultStore) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	f, err := s.read("get", key)
	if err != nil {
		return nil, storage.Info{}, err
	}
	rc, info, err := s.Store.Get(ctx, key)
	if f.after != nil {
		f.after.wait() // the body is open, as of the version it reports
	}
	return rc, info, err
}

func (s *faultStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if _, err := s.read("getrange", key); err != nil {
		return nil, storage.Info{}, err
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func (s *faultStore) Head(ctx context.Context, key string) (storage.Info, error) {
	if _, err := s.read("head", key); err != nil {
		return storage.Info{}, err
	}
	return s.Store.Head(ctx, key)
}

func (s *faultStore) List(ctx context.Context, prefix, after string, limit int) ([]storage.Entry, error) {
	if _, err := s.read("list", prefix); err != nil {
		return nil, err
	}
	return s.Store.List(ctx, prefix, after, limit)
}

// isCommit reports whether key is a commit-log key.
func isCommit(key string) bool { return strings.Contains(key, "/log/") }

// takeOffice does what a takeover does, directly with package format: it
// swaps the manifest to the next epoch with a new nonce and creates the
// epoch bump after the head. st must be the inner store.
func takeOffice(t testing.TB, st storage.Store, keys format.Keys) Office {
	t.Helper()
	ctx := context.Background()
	rc, info, err := st.Get(ctx, keys.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	m, err := format.DecodeManifest(b, format.DatabaseID{})
	if err != nil {
		t.Fatal(err)
	}
	m.Epoch++
	if _, err := rand.Read(m.Nonce[:]); err != nil {
		t.Fatal(err)
	}
	nb, err := format.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Replace(ctx, keys.Manifest(), info.Version, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
	r, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	head := r.Txn() + 1
	createBump(t, st, keys, m.DB, head, m.Epoch, r.Size(), r.PageSize())
	return Office{DB: m.DB, Epoch: m.Epoch, Head: head, Nonce: m.Nonce}
}

// createBump creates an epoch bump at txn, as a candidate would.
func createBump(t testing.TB, st storage.Store, keys format.Keys, db format.DatabaseID, txn, epoch uint64, size, ps uint32) {
	t.Helper()
	var bump bytes.Buffer
	h := format.CommitHeader{DB: db, PageSize: ps, Txn: txn, Epoch: epoch, DBSize: size, EpochBump: true}
	if err := format.EncodeCommit(&bump, h, nil, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(context.Background(), keys.Commit(db, txn), &bump); err != nil {
		t.Fatalf("epoch bump at %d: %v", txn, err)
	}
}

func testConfig() Config {
	return Config{WALBound: 64 << 20, TailBound: 1 << 15, MaxMemory: 64 << 20, TempBudget: 64 << 20, Cache: NewCache(8 << 20)}
}

// newDatabase creates a database in a new store and takes office for it.
func newDatabase(t *testing.T, ps uint32) (*faultStore, format.Keys, Office) {
	t.Helper()
	st, keys := newFaultStore(t)
	if _, err := Create(context.Background(), st.Store, keys, ps, 64); err != nil {
		t.Fatal(err)
	}
	return st, keys, takeOffice(t, st.Store, keys)
}

func lead(t *testing.T, st storage.Store, keys format.Keys, o Office, cfg Config) *Leader {
	t.Helper()
	l, err := Lead(context.Background(), st, keys, t.TempDir(), o, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func open(t *testing.T, l *Leader) *Conn {
	t.Helper()
	c, err := l.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

var idSeq struct {
	sync.Mutex
	n int
}

// newID returns a fresh 24-byte commit identifier.
func newID() []byte {
	idSeq.Lock()
	defer idSeq.Unlock()
	idSeq.n++
	return fmt.Appendf(nil, "commit-%017d", idSeq.n)
}

// exec runs sql on c as one Run with a new commit identifier.
func exec(c *Conn, sql string) (uint64, error) {
	return c.Run(context.Background(), newID(), func(db *sqlite3.Conn) error { return db.Exec(sql) })
}

func mustRun(t *testing.T, c *Conn, sql string) uint64 {
	t.Helper()
	txn, err := exec(c, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return txn
}

// query returns the rows of sql as strings, one per row, columns joined by
// "|".
func query(db *sqlite3.Conn, sql string) ([]string, error) {
	s, _, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	var rows []string
	for s.Step() {
		cols := make([]string, s.ColumnCount())
		for i := range cols {
			cols[i] = s.ColumnText(i)
		}
		rows = append(rows, strings.Join(cols, "|"))
	}
	return rows, s.Err()
}

func queryConn(t *testing.T, c *Conn, sql string) []string {
	t.Helper()
	var rows []string
	if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) (err error) {
		rows, err = query(db, sql)
		return err
	}); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rows
}

// durable opens the database as stored, at txn (0 for the head), through a
// fresh Reader, copies its pages into a local file presented as a
// rollback-journal database, and opens that with SQLite. It checks
// integrity_check.
func durable(t testing.TB, st storage.Store, keys format.Keys, txn uint64) *sqlite3.Conn {
	t.Helper()
	db, err := durableErr(t, st, keys, txn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func durableErr(t testing.TB, st storage.Store, keys format.Keys, txn uint64) (*sqlite3.Conn, error) {
	ctx := context.Background()
	r, err := Open(ctx, st, keys, NewCache(0), txn)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(t.TempDir(), "durable.db")
	f, err := os.Create(path) //nolint:gosec // the test's own file
	if err != nil {
		return nil, err
	}
	for p := uint32(1); p <= r.Size(); p++ {
		page, err := r.ReadPage(ctx, p)
		if err != nil {
			return nil, err
		}
		if p == 1 {
			page[18], page[19] = 1, 1 // rollback journal: no WAL for a copy
		}
		if _, err := f.Write(page); err != nil {
			return nil, err
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	db, err := sqlite3.Open("file:" + path)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = db.Close() })
	rows, err := query(db, "PRAGMA integrity_check")
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 || rows[0] != "ok" {
		return nil, fmt.Errorf("integrity_check at %d: %v", r.Txn(), rows)
	}
	return db, nil
}

func mustQuery(t testing.TB, db *sqlite3.Conn, sql string) []string {
	t.Helper()
	rows, err := query(db, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rows
}

// commitIDs reads the commit identifiers of the log from first to the
// head, "bump" for an epoch bump.
func commitIDs(t testing.TB, st storage.Store, keys format.Keys, db format.DatabaseID, first uint64) []string {
	t.Helper()
	var ids []string
	for txn := first; ; txn++ {
		rc, _, err := st.GetRange(context.Background(), keys.Commit(db, txn), 0, format.CommitHeaderSize)
		if errors.Is(err, storage.ErrNotFound) {
			return ids
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		h, err := format.DecodeCommitHeader(b, db)
		if err != nil {
			t.Fatal(err)
		}
		if h.EpochBump {
			ids = append(ids, "bump")
		} else {
			ids = append(ids, string(h.CommitID))
		}
	}
}
