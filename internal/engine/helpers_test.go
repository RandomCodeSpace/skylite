package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

var errInjected = errors.New("injected failure")

// Retries in tests do not wait; TestCreateWaits checks the waits.
func init() { createWait = func(context.Context, int) error { return nil } }

// testStore wraps a store, counts requests and the body bytes read, and
// lets a test fail or delay any request.
type testStore struct {
	storage.Store
	dir string // the localfs directory newStore made

	mu       sync.Mutex
	requests int
	bytes    int64
	// inject, if set, is called before every request. A nil error lets it
	// through. Otherwise the request fails with that error: without
	// reaching the inner store, or, if land is set, after the inner store
	// performed it.
	inject func(ctx context.Context, op, key string) (land bool, err error)
}

func newStore(t *testing.T) (*testStore, format.Keys) {
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
	return &testStore{Store: fs, dir: dir}, keys
}

func (s *testStore) setInject(f func(ctx context.Context, op, key string) (bool, error)) {
	s.mu.Lock()
	s.inject = f
	s.mu.Unlock()
}

func (s *testStore) counts() (int, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.bytes
}

func (s *testStore) resetCounts() {
	s.mu.Lock()
	s.requests, s.bytes = 0, 0
	s.mu.Unlock()
}

// before counts a request and runs the injector. It returns the error to
// fail with and whether the inner store runs first.
func (s *testStore) before(ctx context.Context, op, key string) (bool, error) {
	s.mu.Lock()
	s.requests++
	f := s.inject
	s.mu.Unlock()
	if f == nil {
		return false, nil
	}
	return f(ctx, op, key)
}

type countingReader struct {
	io.ReadCloser
	s *testStore
}

func (r countingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.s.mu.Lock()
	r.s.bytes += int64(n)
	r.s.mu.Unlock()
	return n, err
}

func (s *testStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	land, err := s.before(ctx, "create", key)
	if err != nil && !land {
		return "", err
	}
	v, ierr := s.Store.Create(ctx, key, body)
	if err != nil {
		return "", err
	}
	return v, ierr
}

func (s *testStore) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	land, err := s.before(ctx, "replace", key)
	if err != nil && !land {
		return "", err
	}
	v, ierr := s.Store.Replace(ctx, key, version, body)
	if err != nil {
		return "", err
	}
	return v, ierr
}

// body wraps a read's body: counted, or failing if the injector returned
// errBody.
func (s *testStore) body(rc io.ReadCloser, info storage.Info, err, injected error) (io.ReadCloser, storage.Info, error) {
	if err != nil {
		return nil, info, err
	}
	if injected != nil {
		return failingBody{rc}, info, nil
	}
	return countingReader{rc, s}, info, nil
}

func (s *testStore) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	_, err := s.before(ctx, "get", key)
	if err != nil && !errors.Is(err, errBody) {
		return nil, storage.Info{}, err
	}
	rc, info, ierr := s.Store.Get(ctx, key)
	return s.body(rc, info, ierr, err)
}

func (s *testStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	_, err := s.before(ctx, "getrange", key)
	if err != nil && !errors.Is(err, errBody) {
		return nil, storage.Info{}, err
	}
	rc, info, ierr := s.Store.GetRange(ctx, key, off, n)
	return s.body(rc, info, ierr, err)
}

func (s *testStore) Head(ctx context.Context, key string) (storage.Info, error) {
	if _, err := s.before(ctx, "head", key); err != nil {
		return storage.Info{}, err
	}
	return s.Store.Head(ctx, key)
}

func (s *testStore) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if _, err := s.before(ctx, "list", prefix); err != nil {
		return nil, err
	}
	return s.Store.List(ctx, prefix, startAfter, limit)
}

func (s *testStore) Delete(ctx context.Context, key string) error {
	land, err := s.before(ctx, "delete", key)
	if err != nil && !land {
		return err
	}
	ierr := s.Store.Delete(ctx, key)
	if err != nil {
		return err
	}
	return ierr
}

// history writes a database's objects with package format, as a leader,
// compactor and collector would, and keeps the content of every page at
// every transaction as the model the reader is checked against. Its methods
// are safe for concurrent use with readers of the model.
type history struct {
	t    testing.TB
	st   storage.Store // the inner store: fixture writes are not counted
	keys format.Keys
	rng  *rand.Rand
	db   format.DatabaseID

	mu      sync.Mutex
	ps, gs  uint32
	epoch   uint64
	states  [][][]byte // states[txn][p-1] is page p after txn; states[0] is empty
	headers map[uint64]format.CommitHeader
	retired map[uint64]bool
	m       format.Manifest
	mv      string            // the manifest's version
	bases   map[string]uint64 // every base key written, to its transaction
}

func newHistory(t testing.TB, st storage.Store, keys format.Keys, seed uint64, ps, gs uint32) *history {
	t.Helper()
	h := &history{
		t: t, st: st, keys: keys, rng: rand.New(rand.NewPCG(seed, 1)), //nolint:gosec // seeded so failures reproduce
		ps: ps, gs: gs, epoch: 1,
		states:  [][][]byte{nil},
		headers: map[uint64]format.CommitHeader{},
		retired: map[uint64]bool{},
		bases:   map[string]uint64{},
	}
	for h.db == (format.DatabaseID{}) {
		h.db = format.DatabaseID(h.bytes(16))
	}
	h.commit(1, map[uint32][]byte{1: h.bytes(int(ps))})
	h.m = format.Manifest{DB: h.db, PageSize: ps, GroupSize: gs, Epoch: 1, Nonce: [16]byte(h.bytes(16))}
	h.mv = h.create(keys.Manifest(), h.encodeManifest(h.m))
	return h
}

func (h *history) bytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(h.rng.Uint32()) //nolint:gosec // truncation intended
	}
	return b
}

func (h *history) create(key string, b []byte) string {
	h.t.Helper()
	v, err := h.st.Create(context.Background(), key, bytes.NewReader(b))
	if err != nil {
		h.t.Fatalf("fixture create %s: %v", key, err)
	}
	return v
}

func (h *history) replace(key, version string, b []byte) string {
	h.t.Helper()
	v, err := h.st.Replace(context.Background(), key, version, bytes.NewReader(b))
	if err != nil {
		h.t.Fatalf("fixture replace %s: %v", key, err)
	}
	return v
}

func (h *history) encodeManifest(m format.Manifest) []byte {
	h.t.Helper()
	b, err := format.EncodeManifest(m)
	if err != nil {
		h.t.Fatal(err)
	}
	return b
}

// head is the last transaction number.
func (h *history) head() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return uint64(len(h.states) - 1) //nolint:gosec // never negative
}

func (h *history) manifest() (format.Manifest, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.m, h.mv
}

// state is the content of every page after txn.
func (h *history) state(txn uint64) [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.states[txn]
}

// encodeCommit builds the commit after the head with the given pages and
// database size, without writing it.
func (h *history) encodeCommit(txn uint64, db format.DatabaseID, epoch uint64, size uint32, pages map[uint32][]byte, bump bool) ([]byte, format.CommitHeader) {
	h.t.Helper()
	pgnos := make([]uint32, 0, len(pages))
	for p := range pages {
		pgnos = append(pgnos, p)
	}
	slices.Sort(pgnos)
	var body bytes.Buffer
	for _, p := range pgnos {
		body.Write(pages[p])
	}
	ch := format.CommitHeader{DB: db, PageSize: h.ps, Txn: txn, Epoch: epoch, DBSize: size, PageCount: uint32(len(pgnos)), EpochBump: bump} //nolint:gosec // small test databases
	if !bump {
		ch.CommitID = h.bytes(24)
	}
	var out bytes.Buffer
	if err := format.EncodeCommit(&out, ch, pgnos, &body); err != nil {
		h.t.Fatal(err)
	}
	dh, err := format.DecodeCommitHeader(out.Bytes()[:format.CommitHeaderSize], db)
	if err != nil {
		h.t.Fatal(err)
	}
	return out.Bytes(), dh
}

// commit writes the next commit: the database becomes size pages, and the
// given pages are written.
func (h *history) commit(size uint32, pages map[uint32][]byte) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	txn := uint64(len(h.states))
	b, ch := h.encodeCommit(txn, h.db, h.epoch, size, pages, false)
	h.create(h.keys.Commit(h.db, txn), b)
	prev := h.states[txn-1]
	next := make([][]byte, size)
	zero := make([]byte, h.ps)
	for i := range next {
		if i < len(prev) {
			next[i] = prev[i]
		} else {
			next[i] = zero
		}
	}
	for p, body := range pages {
		next[p-1] = body
	}
	h.states = append(h.states, next)
	h.headers[txn] = ch
	return txn
}

// randomCommit writes a commit that grows, shrinks or keeps the database
// size and writes random pages.
func (h *history) randomCommit(maxSize uint32) uint64 {
	size := uint32(len(h.state(h.head()))) //nolint:gosec // small test databases
	switch h.rng.IntN(4) {
	case 0:
		size = min(maxSize, size+1+h.rng.Uint32N(h.gs*2))
	case 1:
		size = max(1, size-h.rng.Uint32N(min(size, h.gs*2)))
	}
	pages := map[uint32][]byte{}
	for range 1 + h.rng.IntN(int(min(size, 6))) {
		pages[1+h.rng.Uint32N(size)] = h.bytes(int(h.ps))
	}
	return h.commit(size, pages)
}

// bump writes an epoch-bump commit at epoch e, which may be lower than
// earlier epochs.
func (h *history) bump(e uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	txn := uint64(len(h.states))
	size := uint32(len(h.states[txn-1])) //nolint:gosec // small test databases
	b, ch := h.encodeCommit(txn, h.db, e, size, nil, true)
	h.create(h.keys.Commit(h.db, txn), b)
	h.states = append(h.states, h.states[txn-1])
	h.headers[txn] = ch
	h.epoch = e
	return txn
}

// groupAt is the content of group g after txn, zeros past the database.
func (h *history) groupAt(txn uint64, g uint32) [][]byte {
	st := h.states[txn]
	out := make([][]byte, h.gs)
	zero := make([]byte, h.ps)
	for i := range out {
		p := uint64(g)*uint64(h.gs) + uint64(i)
		if p < uint64(len(st)) {
			out[i] = st[p]
		} else {
			out[i] = zero
		}
	}
	return out
}

func equalPages(a, b [][]byte) bool {
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func allZeros(pages [][]byte) bool {
	for _, p := range pages {
		if !bytes.Equal(p, make([]byte, len(p))) {
			return false
		}
	}
	return true
}

// compactObjects writes the base objects a compaction to t needs and
// returns the manifest that would publish them, without publishing it.
func (h *history) compactObjects(t uint64) format.Manifest {
	h.mu.Lock()
	defer h.mu.Unlock()
	size := uint32(len(h.states[t])) //nolint:gosec // small test databases
	n := format.GroupCount(size, h.gs)
	m := h.m
	m.Txn, m.DBSize, m.Groups = t, size, make([]format.GroupRef, n)
	tag := h.rng.Uint64()
	for g := range n {
		want := h.groupAt(t, g)
		if g < uint32(len(h.m.Groups)) { //nolint:gosec // small test databases
			ref := h.m.Groups[g]
			old := make([][]byte, h.gs)
			if ref.Txn == 0 {
				for i := range old {
					old[i] = make([]byte, h.ps)
				}
			} else {
				old = h.groupAt(ref.Txn, g)
			}
			if equalPages(old, want) {
				m.Groups[g] = ref
				continue
			}
		}
		if allZeros(want) && h.rng.IntN(2) == 0 {
			continue // the zero reference
		}
		bh := format.BaseHeader{DB: h.db, PageSize: h.ps, GroupSize: h.gs, Group: g, Txn: t, Tag: tag, DBSize: size}
		var body, out bytes.Buffer
		for _, p := range want {
			body.Write(p)
		}
		if err := format.EncodeBase(&out, bh, &body); err != nil {
			h.t.Fatal(err)
		}
		key := h.keys.Base(h.db, g, t, tag)
		h.create(key, out.Bytes())
		h.bases[key] = t
		m.Groups[g] = format.GroupRef{Txn: t, Tag: tag}
	}
	return m
}

// publish replaces the manifest.
func (h *history) publish(m format.Manifest) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mv = h.replace(h.keys.Manifest(), h.mv, h.encodeManifest(m))
	h.m = m
}

// compact folds the log up to t into the base and publishes it.
func (h *history) compact(t uint64) { h.publish(h.compactObjects(t)) }

// swapEpoch publishes an epoch swap: epoch plus one, a new nonce.
func (h *history) swapEpoch() {
	m, _ := h.manifest()
	m.Epoch++
	m.Nonce = [16]byte(h.bytes(16))
	h.publish(m)
}

// retire replaces commit k, which the manifest must cover, with its
// retired form. Epoch bumps and retired commits are left alone.
func (h *history) retire(k uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := h.headers[k]
	if ch.EpochBump || h.retired[k] || k > h.m.Txn {
		return
	}
	key := h.keys.Commit(h.db, k)
	info, err := h.st.Head(context.Background(), key)
	if err != nil {
		h.t.Fatal(err)
	}
	b, err := ch.Retire()
	if err != nil {
		h.t.Fatal(err)
	}
	h.replace(key, info.Version, b)
	h.retired[k] = true
}

// retireAll retires every commit the manifest covers.
func (h *history) retireAll() {
	m, _ := h.manifest()
	for k := uint64(1); k <= m.Txn; k++ {
		h.retire(k)
	}
}

// collect deletes every base object the garbage rule allows: unreferenced
// by the manifest, at or below its transaction number.
func (h *history) collect() {
	h.mu.Lock()
	defer h.mu.Unlock()
	live := map[string]bool{}
	for g, ref := range h.m.Groups {
		if ref.Txn != 0 {
			live[h.keys.Base(h.db, uint32(g), ref.Txn, ref.Tag)] = true //nolint:gosec // small test databases
		}
	}
	for key, t := range h.bases {
		if !live[key] && t <= h.m.Txn {
			if err := h.st.Delete(context.Background(), key); err != nil {
				h.t.Fatal(err)
			}
			delete(h.bases, key)
		}
	}
}

// checkReader reads every page of r and compares it with the model. If
// behind is set, ErrCompacted is accepted in place of a page.
func (h *history) checkReader(ctx context.Context, r *Reader, behind bool) error {
	want := h.state(r.Txn())
	if r.Size() != uint32(len(want)) { //nolint:gosec // small test databases
		return fmt.Errorf("size %d at transaction %d, want %d", r.Size(), r.Txn(), len(want))
	}
	for p := range r.Size() {
		got, err := r.ReadPage(ctx, p+1)
		if behind && errors.Is(err, ErrCompacted) {
			continue
		}
		if err != nil {
			return fmt.Errorf("page %d at transaction %d: %w", p+1, r.Txn(), err)
		}
		if !bytes.Equal(got, want[p]) {
			return fmt.Errorf("page %d at transaction %d: wrong content", p+1, r.Txn())
		}
	}
	if _, err := r.ReadPage(ctx, r.Size()+1); !errors.Is(err, ErrPageRange) {
		return fmt.Errorf("page past the end at transaction %d: %w", r.Txn(), err)
	}
	return nil
}
