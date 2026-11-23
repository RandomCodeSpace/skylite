package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

func TestCacheEvicts(t *testing.T) {
	page := func(b byte) []byte { return bytes.Repeat([]byte{b}, 9) }
	entry := entrySize("a", page(1)) // each entry, with its key and overhead
	c := NewCache(3 * entry)
	c.put("a", page(1))
	c.put("b", page(2))
	c.put("c", page(3))
	if _, ok := c.get("a"); !ok { // a is now the most recent
		t.Fatal("a evicted early")
	}
	c.put("d", page(4))
	if _, ok := c.get("b"); ok {
		t.Fatal("the least recently used entry was kept")
	}
	for _, k := range []string{"a", "c", "d"} {
		if b, ok := c.get(k); !ok || len(b) != 9 {
			t.Fatalf("%s missing", k)
		}
	}
	if c.used > c.budget {
		t.Fatalf("%d bytes used of %d", c.used, c.budget)
	}
	c.put("big", make([]byte, 3*entry))
	if _, ok := c.get("big"); ok || c.used != 3*entry {
		t.Fatal("an entry larger than the budget was cached")
	}
	exact := NewCache(entry)
	exact.put("a", page(1))
	if _, ok := exact.get("a"); !ok {
		t.Fatal("an entry the size of the budget was not cached")
	}
	z := NewCache(0)
	z.put("a", page(1))
	if _, ok := z.get("a"); ok {
		t.Fatal("a zero budget cached")
	}
}

// TestSlowFetchDoesNotBlockCachedReads holds one reader's fetch inside the
// store and reads a cached page through another reader meanwhile.
func TestSlowFetchDoesNotBlockCachedReads(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	cache := NewCache(1 << 20)
	a, err := Open(ctx, st, keys, cache, 0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(ctx, st, keys, cache, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadPage(ctx, 1); err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	st.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op == "getrange" && key == keys.Commit(h.db, 5) {
			close(held)
			<-release
		}
		return false, nil
	})
	done := make(chan error)
	go func() {
		_, err := b.ReadPage(ctx, 10)
		done <- err
	}()
	<-held
	got, err := a.ReadPage(ctx, 1)
	if err != nil || !bytes.Equal(got, h.state(6)[0]) {
		t.Fatalf("cached read during a slow fetch: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentReaders runs many readers, one advancing reader and a
// writer that commits, compacts, retires and collects, with a cache small
// enough to evict constantly. Run it with -race.
func TestConcurrentReaders(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 31, 512, 4)
	for range 10 {
		h.randomCommit(32)
	}
	cache := NewCache(8 * 600)
	var stop atomic.Bool
	var wg sync.WaitGroup
	fail := make(chan error, 16)
	check := func(r *Reader, p uint32) error {
		got, err := r.ReadPage(ctx, p)
		if errors.Is(err, ErrCompacted) {
			return nil
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(got, h.state(r.Txn())[p-1]) {
			return errors.New("wrong page content")
		}
		return nil
	}
	for i := range 8 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 2)) //nolint:gosec // only picks pages
			for !stop.Load() {
				r, err := Open(ctx, st, keys, cache, 0)
				if err != nil {
					fail <- err
					return
				}
				for range 20 {
					if err := check(r, 1+rng.Uint32N(r.Size())); err != nil {
						fail <- err
						return
					}
				}
			}
		})
	}
	wg.Go(func() {
		r, err := Open(ctx, st, keys, cache, 0)
		if err != nil {
			fail <- err
			return
		}
		for !stop.Load() {
			if err := r.Advance(ctx); err != nil {
				fail <- err
				return
			}
			for p := uint32(1); p <= r.Size(); p++ {
				if err := check(r, p); err != nil {
					fail <- err
					return
				}
			}
		}
	})
	for i := range 60 {
		h.randomCommit(32)
		if i%10 == 9 {
			m, _ := h.manifest()
			h.compact(m.Txn + (h.head()-m.Txn)/2 + 1)
			h.retireAll()
			h.collect()
		}
	}
	stop.Store(true)
	wg.Wait()
	close(fail)
	for err := range fail {
		t.Fatal(err)
	}
}

// errBody makes the store return a body whose reads fail.
var errBody = errors.New("injected body failure")

type failingBody struct{ io.ReadCloser }

func (failingBody) Read([]byte) (int, error) { return 0, errBody }

// TestStorageFailures runs one scenario (open, read every page, compaction
// and retirement underneath, read again, advance, read again) once for
// every request it makes, with that request failing: with an error, by
// timing out until its context ends, or with a body that fails. Every
// failure must surface as an error from the call that made the request;
// the same call then succeeds, and the scenario ends with every page right.
func TestStorageFailures(t *testing.T) {
	walkReads(t, 1, firstRead) // requests one at a time, so that each can be failed in turn
	// The two states of storage the scenario sees, built once.
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 41, 512, 4)
	for range 5 {
		h.randomCommit(16)
	}
	h.compact(3)
	h.commit(16, map[uint32][]byte{1: h.bytes(512)})
	snapA := snapshot(t, st)
	before := h.head()
	h.commit(16, map[uint32][]byte{2: h.bytes(512)})
	h.compact(before + 1)
	h.retireAll()
	h.collect()
	snapB := snapshot(t, st)

	run := func(t *testing.T, failAt int, mode string) int {
		ctx := context.Background()
		st := &testStore{Store: openCopy(t, snapA)}
		n, fired := 0, false
		var cancel context.CancelFunc
		st.inject = func(ctx context.Context, op, key string) (bool, error) {
			n++
			if n-1 != failAt {
				return false, nil
			}
			fired = true
			switch mode {
			case "timeout":
				cancel()
				<-ctx.Done()
				return false, &storage.Error{Op: op, Key: key, Err: ctx.Err()}
			case "body":
				if op == "get" || op == "getrange" {
					return false, errBody
				}
			}
			return false, &storage.Error{Op: op, Key: key, Err: errInjected}
		}
		do := func(what string, f func(ctx context.Context) error) {
			t.Helper()
			for attempt := 0; ; attempt++ {
				var opCtx context.Context
				opCtx, cancel = context.WithCancel(ctx)
				fired = false
				err := f(opCtx)
				cancel()
				if err == nil {
					return
				}
				want := errInjected
				switch {
				case mode == "timeout":
					want = context.Canceled
				case mode == "body" && !errors.Is(err, errInjected):
					want = errBody
				}
				if !fired || attempt > 0 || !errors.Is(err, want) {
					t.Fatalf("request %d, %s: %s: %v", failAt, mode, what, err)
				}
			}
		}
		var r *Reader
		readAll := func(behind bool) {
			t.Helper()
			for p := uint32(1); p <= r.Size(); p++ {
				do("read", func(ctx context.Context) error {
					got, err := r.ReadPage(ctx, p)
					switch {
					case behind && errors.Is(err, ErrCompacted):
						return nil
					case err != nil:
						return err
					case !bytes.Equal(got, h.state(r.Txn())[p-1]):
						t.Fatalf("request %d, %s: page %d at %d: wrong content", failAt, mode, p, r.Txn())
					}
					return nil
				})
			}
		}
		do("open", func(ctx context.Context) (err error) {
			r, err = Open(ctx, st, keys, NewCache(4*600), 0)
			return err
		})
		readAll(false)
		st.Store = openCopy(t, snapB)
		readAll(true)
		do("advance", func(ctx context.Context) error { return r.Advance(ctx) })
		if r.Txn() != h.head() {
			t.Fatalf("request %d, %s: advanced to %d, head %d", failAt, mode, r.Txn(), h.head())
		}
		readAll(false)
		return n
	}

	total := run(t, -1, "")
	t.Logf("the scenario makes %d requests", total)
	if total < 20 {
		t.Fatalf("the scenario made only %d requests", total)
	}
	for _, mode := range []string{"error", "timeout", "body"} {
		for i := range total {
			run(t, i, mode)
		}
	}
}

// snapshot copies a store's directory, version tokens included.
func snapshot(t *testing.T, st *testStore) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "snap")
	if err := os.CopyFS(dir, os.DirFS(st.dir)); err != nil {
		t.Fatal(err)
	}
	return dir
}

func openCopy(t *testing.T, snap string) storage.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.CopyFS(dir, os.DirFS(snap)); err != nil {
		t.Fatal(err)
	}
	fs, err := localfs.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	return fs
}

// TestCacheHeap fills a cache through a reader and measures the heap it
// holds: within 5% of its budget at every page size, with entries much
// smaller than the budget so that rounding to whole entries does not count.
func TestCacheHeap(t *testing.T) {
	ctx := context.Background()
	for _, ps := range []uint32{512, 4096, 8192, 65536} {
		st, keys := newStore(t)
		h := newHistory(t, st.Store, keys, 11, ps, 4)
		const n = 400
		pages := map[uint32][]byte{}
		for p := uint32(1); p <= n; p++ {
			pages[p] = h.bytes(int(ps))
		}
		h.commit(n, pages)
		h.compact(2)
		h.commit(n, map[uint32][]byte{2: h.bytes(int(ps))})
		budget := int64(200) * int64(ps)
		c := NewCache(budget)
		r, err := Open(ctx, st, keys, c, 0)
		if err != nil {
			t.Fatal(err)
		}
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		for p := uint32(1); p <= n; p++ {
			if _, err := r.ReadPage(ctx, p); err != nil {
				t.Fatal(err)
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&m1)
		held := int64(m1.HeapAlloc) - int64(m0.HeapAlloc) //nolint:gosec // heap sizes fit
		t.Logf("page size %d: budget %d, accounted %d, heap held %d (%.1f%%), %d entries", ps, budget, c.used, held, 100*float64(held)/float64(budget), len(c.m))
		if held > budget+budget/20 {
			t.Errorf("page size %d: the cache holds %d bytes of heap for a budget of %d", ps, held, budget)
		}
		runtime.KeepAlive(r)
	}
}
