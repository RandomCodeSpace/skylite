package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// cacheBudgets are the budgets every model run is repeated with: none, a
// few pages, everything.
var cacheBudgets = map[string]int64{"none": 0, "tiny": 3 * 600, "large": 64 << 20}

// geometries are the page and group sizes the model runs rotate through.
var geometries = [][2]uint32{{512, 4}, {512, 1}, {1024, 16}, {4096, 2}}

// TestReadAgainstModel generates random histories (commits that grow and
// shrink the database, epoch bumps with any epoch, compactions, epoch
// swaps, retirement and garbage collection), and checks readers opened at
// many transaction numbers, readers left behind and advancing readers
// against the model.
func TestReadAgainstModel(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4, 5, 6}
	if testing.Short() {
		seeds = seeds[:2]
	}
	for name, budget := range cacheBudgets {
		for _, seed := range seeds {
			t.Run(name, func(t *testing.T) {
				geo := geometries[seed%uint64(len(geometries))]
				if err := runModel(t, seed, budget, geo[0], geo[1]); err != nil {
					t.Fatalf("seed %d, cache %s, page size %d, group size %d: %v", seed, name, geo[0], geo[1], err)
				}
			})
		}
	}
}

func runModel(t *testing.T, seed uint64, budget int64, ps, gs uint32) error {
	ctx := context.Background()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, seed, ps, gs)
	cache := NewCache(budget)
	var old []*Reader
	for step := range 120 {
		switch x := h.rng.IntN(20); {
		case x < 12:
			h.randomCommit(40)
		case x < 14:
			e := h.epoch + 1
			if h.rng.IntN(3) == 0 {
				e = 1 + h.rng.Uint64N(e)
			}
			h.bump(e)
		case x < 16:
			m, _ := h.manifest()
			if head := h.head(); head > m.Txn {
				h.compact(m.Txn + 1 + h.rng.Uint64N(head-m.Txn))
			}
		case x < 17:
			h.swapEpoch()
		case x < 19:
			m, _ := h.manifest()
			for range 3 {
				h.retire(1 + h.rng.Uint64N(m.Txn+1))
			}
		default:
			h.collect()
		}
		if step%4 != 3 {
			continue
		}

		m, _ := h.manifest()
		head := h.head()
		for _, txn := range []uint64{0, m.Txn, head, m.Txn + h.rng.Uint64N(head-m.Txn+1)} {
			if txn == 0 && m.Txn == 0 {
				continue // 0 asks for the head
			}
			r, err := Open(ctx, st, keys, cache, txn)
			if err != nil {
				return err
			}
			want := txn
			if txn == 0 {
				want = head
			}
			if r.Txn() != want {
				return fmt.Errorf("opened at %d for %d, head %d", r.Txn(), txn, head)
			}
			if err := h.checkReader(ctx, r, false); err != nil {
				return err
			}
			if h.rng.IntN(3) == 0 {
				old = append(old, r)
			}
		}
		if m.Txn > 1 {
			if _, err := Open(ctx, st, keys, cache, m.Txn-1); !errors.Is(err, ErrCompacted) {
				return fmt.Errorf("open below the base: %w", err)
			}
		}
		if _, err := Open(ctx, st, keys, cache, head+1); !errors.Is(err, ErrBeyondHead) {
			return fmt.Errorf("open beyond the head: %w", err)
		}

		// Readers left behind serve their own state or fail; advancing
		// readers reach the head.
		for i, r := range old {
			if err := h.checkReader(ctx, r, true); err != nil {
				return fmt.Errorf("reader left at %d: %w", r.Txn(), err)
			}
			if i%2 == 0 {
				if err := r.Advance(ctx); err != nil {
					return fmt.Errorf("advance from %d: %w", r.Txn(), err)
				}
				if r.Txn() != head {
					return fmt.Errorf("advanced to %d, head %d", r.Txn(), head)
				}
				if err := h.checkReader(ctx, r, false); err != nil {
					return fmt.Errorf("advanced reader: %w", err)
				}
			}
		}
		if len(old) > 6 {
			old = old[len(old)-6:]
		}
	}
	return nil
}

// TestBoundedWork opens a large synthetic database and reads one page. The
// requests and bytes are bounded by what the record and the format say a
// reader needs: the manifest, the header and page table of every commit
// above it, a probe for the name after the head and one for the name above
// that, and one page record.
// None of it depends on how many groups the database has.
func TestBoundedWork(t *testing.T) {
	ctx := context.Background()
	for _, groups := range []uint32{2000, 8000} {
		st, keys := newStore(t)
		h := newHistory(t, st.Store, keys, 7, 512, 1)
		const size = 40
		h.commit(size, map[uint32][]byte{1: h.bytes(512), size: h.bytes(512)})
		for range 50 {
			h.randomCommit(size)
		}
		h.compact(h.head())
		h.retireAll()
		// The manifest references many groups whose objects this test does
		// not write: a reader must not touch them.
		m, _ := h.manifest()
		m.DBSize = groups
		for g := uint32(len(m.Groups)); g < groups; g++ { //nolint:gosec // at most groups
			m.Groups = append(m.Groups, format.GroupRef{Txn: m.Txn, Tag: uint64(g)})
		}
		h.publish(m)
		tail := 300
		var tableBytes int64
		for range tail {
			txn := h.commit(groups, map[uint32][]byte{1 + h.rng.Uint32N(20): h.bytes(512), 1 + h.rng.Uint32N(20): h.bytes(512)})
			tableBytes += h.headers[txn].TableSize()
		}
		mb, _ := format.EncodeManifest(m)

		st.resetCounts()
		r, err := Open(ctx, st, keys, NewCache(0), 0)
		if err != nil {
			t.Fatal(err)
		}
		reqs, n := st.counts()
		wantReqs := 1 + 2*tail + 2
		wantBytes := int64(len(mb)) + int64(tail)*format.CommitHeaderSize + tableBytes
		if reqs != wantReqs || n != wantBytes {
			t.Fatalf("%d groups: open made %d requests for %d bytes, want %d for %d", groups, reqs, n, wantReqs, wantBytes)
		}
		st.resetCounts()
		if _, err := Open(ctx, st, keys, NewCache(0), h.head()+1); !errors.Is(err, ErrBeyondHead) {
			t.Fatal(err)
		}
		if reqs, _ = st.counts(); reqs != 1+2*tail+1 {
			t.Fatalf("%d groups: open beyond the head made %d requests", groups, reqs)
		}
		for _, p := range []uint32{30, 2} { // a base page, then a commit page
			st.resetCounts()
			if _, err := r.ReadPage(ctx, p); err != nil {
				t.Fatal(err)
			}
			reqs, n = st.counts()
			if reqs > 1 || n > 512+4 {
				t.Fatalf("page %d: %d requests for %d bytes", p, reqs, n)
			}
		}

		// Advancing reads only the new commits, and the manifest's version.
		for range 5 {
			h.commit(groups, map[uint32][]byte{3: h.bytes(512)})
		}
		st.resetCounts()
		if err := r.Advance(ctx); err != nil {
			t.Fatal(err)
		}
		reqs, n = st.counts()
		if reqs != 1+2*5+2 || n != 5*(format.CommitHeaderSize+format.PageEntrySize) {
			t.Fatalf("advance made %d requests for %d bytes", reqs, n)
		}
	}
}

// TestAdvanceKeepsCache: pages unchanged by the new commits stay cached.
func TestAdvanceKeepsCache(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 8, 512, 4)
	h.commit(12, map[uint32][]byte{2: h.bytes(512), 9: h.bytes(512)})
	h.compact(2)
	h.commit(12, map[uint32][]byte{3: h.bytes(512)})
	r, err := Open(ctx, st, keys, NewCache(1<<20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.checkReader(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	h.commit(12, map[uint32][]byte{2: h.bytes(512)})
	if err := r.Advance(ctx); err != nil {
		t.Fatal(err)
	}
	st.resetCounts()
	for _, p := range []uint32{1, 3, 9} {
		if _, err := r.ReadPage(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if reqs, _ := st.counts(); reqs != 0 {
		t.Fatalf("%d requests for cached pages", reqs)
	}
	if err := h.checkReader(ctx, r, false); err != nil {
		t.Fatal(err)
	}
}

// TestFallingBehind anchors readers on a manifest and runs a compaction,
// retirement and garbage collection underneath them, checking every reader
// after every storage step. A reader at a transaction the new base covers
// fails with ErrCompacted; one at or above it re-anchors; none serves
// anything but its own state.
func TestFallingBehind(t *testing.T) {
	for name, budget := range cacheBudgets {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, keys := newStore(t)
			h := newHistory(t, st.Store, keys, 9, 512, 4)
			for range 6 {
				h.randomCommit(24)
			}
			h.compact(3)
			for range 6 {
				h.randomCommit(24)
			}
			cache := NewCache(budget)
			var readers []*Reader
			for txn := uint64(3); txn <= h.head(); txn++ {
				r, err := Open(ctx, st, keys, cache, txn)
				if err != nil {
					t.Fatal(err)
				}
				if txn%2 == 0 { // half of them have read everything already
					if err := h.checkReader(ctx, r, false); err != nil {
						t.Fatal(err)
					}
				}
				readers = append(readers, r)
			}
			target := uint64(9)
			check := func(stage string) {
				t.Helper()
				m, _ := h.manifest()
				for _, r := range readers {
					err := h.checkReader(ctx, r, true)
					if err != nil {
						t.Fatalf("%s: reader at %d: %v", stage, r.Txn(), err)
					}
					if r.Txn() >= m.Txn {
						// It may have to re-anchor, but it serves everything.
						if err := h.checkReader(ctx, r, false); err != nil {
							t.Fatalf("%s: reader at %d above the base at %d: %v", stage, r.Txn(), m.Txn, err)
						}
					}
				}
			}
			m := h.compactObjects(target)
			check("output written")
			h.publish(m)
			check("manifest published")
			for k := uint64(1); k <= target; k++ {
				h.retire(k)
				check(fmt.Sprint("retired ", k))
			}
			h.collect()
			check("collected")
			h.swapEpoch()
			check("epoch swap")
			for _, r := range readers {
				if err := r.Advance(ctx); err != nil {
					t.Fatal(err)
				}
				if err := h.checkReader(ctx, r, false); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestRetiredDuringWalk runs a compaction to 5 and the retirement of what
// it covers while Open walks the log: before it reads a commit's header, or
// between that header and its page table. The reader re-anchors on the new
// manifest when it may, and refuses a transaction the new base covers.
func TestRetiredDuringWalk(t *testing.T) {
	for _, nth := range []int{1, 2} {
		for _, at := range []uint64{0, 5, 4} {
			t.Run(fmt.Sprint(nth, "/", at), func(t *testing.T) {
				ctx := context.Background()
				st, keys, h := corruptFixture(t)
				// Opening at 4 never reads commit 5, so race it on commit 4.
				n, watch := 0, uint64(5)
				if at == 4 {
					watch = 4
				}
				st.setInject(func(_ context.Context, op, key string) (bool, error) {
					if op == "getrange" && key == keys.Commit(h.db, watch) {
						if n++; n == nth {
							h.compact(5)
							h.retireAll()
						}
					}
					return false, nil
				})
				r, err := Open(ctx, st, keys, NewCache(0), at)
				if at == 4 {
					if !errors.Is(err, ErrCompacted) {
						t.Fatalf("open at 4: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := h.checkReader(ctx, r, false); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestShrinkDoesNotWalkTheIndex indexes a commit of 400,000 pages and then
// 1,000 commits that each shrink the database by one page, and counts the
// work: no shrinking commit touches the index, and each pushes one floor
// and pops at most one. The pages above the smallest size are gone even
// after the database grows back.
func TestShrinkDoesNotWalkTheIndex(t *testing.T) {
	const pages, shrinks = 400000, 1000
	r := &Reader{pages: make(map[uint32]pageLoc, pages), baseLimit: pages}
	c := &commit{h: format.CommitHeader{Txn: 1, DBSize: pages}, entries: make([]format.PageEntry, pages)}
	for i := range c.entries {
		c.entries[i].Pgno = uint32(i + 1) //nolint:gosec // below pages
	}
	r.add(c)
	pops := 0
	for i := range uint32(shrinks) {
		before := len(r.floors)
		r.add(&commit{h: format.CommitHeader{Txn: uint64(i) + 2, DBSize: pages - 1 - i}})
		pops += before + 1 - len(r.floors)
		if len(r.pages) != pages {
			t.Fatalf("commit %d changed the index to %d entries", i+2, len(r.pages))
		}
	}
	if pops > shrinks || len(r.floors) != 1 {
		t.Fatalf("%d floors popped, %d left", pops, len(r.floors))
	}
	r.add(&commit{h: format.CommitHeader{Txn: shrinks + 2, DBSize: pages}}) // grows back, writes nothing
	floors := slices.Clone(r.floors)
	r.reset(anchor{m: format.Manifest{DBSize: pages}}, r.commits)
	if !slices.Equal(r.floors, floors) {
		t.Fatalf("rebuilt floors %v, want %v", r.floors, floors)
	}
	for p, want := range map[uint32]bool{1: true, pages - shrinks: true, pages - shrinks + 1: false, pages: false} {
		if s := r.locate(p); (s.c != nil) != want || s.base.Txn != 0 {
			t.Errorf("page %d read from commit: %v, want %v", p, s.c != nil, want)
		}
	}
}

// TestAddRefusesAnyCommitButTheNext: indexing must follow the log.
func TestAddRefusesAnyCommitButTheNext(t *testing.T) {
	r := &Reader{pages: map[uint32]pageLoc{}, txn: 4}
	for _, txn := range []uint64{4, 6, 0} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("commit %d indexed after 4", txn)
				}
			}()
			r.add(&commit{h: format.CommitHeader{Txn: txn, DBSize: 1}})
		}()
	}
	r.txn = math.MaxUint64
	defer func() {
		if recover() == nil {
			t.Error("a commit indexed after the largest transaction number")
		}
	}()
	r.add(&commit{h: format.CommitHeader{Txn: 0, DBSize: 1}})
}

// TestPageOwnership: changing a returned page, fetched or cached, changes
// nothing the reader or its cache serves.
func TestPageOwnership(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	r, err := Open(ctx, st, keys, NewCache(1<<20), 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		for p := uint32(1); p <= r.Size(); p++ {
			page, err := r.ReadPage(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			for i := range page {
				page[i] ^= 0xff
			}
		}
	}
	if err := h.checkReader(ctx, r, false); err != nil {
		t.Fatal(err)
	}
}

// TestNilCache: a nil cache caches nothing.
func TestNilCache(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	r, err := Open(ctx, st, keys, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.checkReader(ctx, r, false); err != nil {
		t.Fatal(err)
	}
}

// TestCacheKeyedByVersion shares a cache between a reader that read a page
// of commit 5 and one that pinned a different object at the same key: each
// reads the version it pinned.
func TestCacheKeyedByVersion(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	cache := NewCache(1 << 20)
	a, err := Open(ctx, st, keys, cache, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.checkReader(ctx, a, false); err != nil {
		t.Fatal(err)
	}
	y := h.bytes(512)
	b5, _ := h.encodeCommit(5, h.db, 1, 12, map[uint32][]byte{6: h.bytes(512), 10: y}, false)
	h.overwrite(keys.Commit(h.db, 5), b5)
	b, err := Open(ctx, st, keys, cache, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.ReadPage(ctx, 10); err != nil || !bytes.Equal(got, y) {
		t.Fatalf("page 10 of the replaced commit: %v", err)
	}
}

// TestAdvanceAfterEpochSwap: an epoch swap is read once; the next Advance
// costs the manifest's Head and the two probes above the head.
func TestAdvanceAfterEpochSwap(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	r, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	h.swapEpoch()
	for i, want := range []int{4, 3, 3} {
		st.resetCounts()
		if err := r.Advance(ctx); err != nil {
			t.Fatal(err)
		}
		if reqs, _ := st.counts(); reqs != want {
			t.Fatalf("advance %d made %d requests, want %d", i, reqs, want)
		}
	}
	if err := h.checkReader(ctx, r, false); err != nil {
		t.Fatal(err)
	}
}

// TestReadersShareGroupReferences: readers of one manifest hold one copy of
// its group references, and a reader that moves to a newer manifest shares
// the newer copy.
func TestReadersShareGroupReferences(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	a, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(ctx, st, keys, NewCache(0), 5)
	if err != nil {
		t.Fatal(err)
	}
	if a.a.groups != b.a.groups || a.a.m.Groups != nil {
		t.Fatal("two readers of one manifest hold separate group references")
	}
	h.compact(6)
	if err := a.Advance(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	if a.a.groups == b.a.groups || a.a.groups != c.a.groups {
		t.Fatal("readers of the new manifest hold separate group references")
	}
	for _, r := range []*Reader{a, b, c} {
		if err := h.checkReader(ctx, r, false); err != nil {
			t.Fatal(err)
		}
	}
}

// TestLockPage grows a database past SQLite's lock page and reads the
// pages around it from the log, from the base, and from the base and the
// log: the lock page reads as zeros and its neighbours as written.
func TestLockPage(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	const ps, gs = 512, 4096
	lock := format.LockPage(ps)
	h := newHistory(t, st.Store, keys, 5, ps, gs)
	size := lock + 10
	x, y, z := h.bytes(ps), h.bytes(ps), h.bytes(ps)
	h.commit(size, map[uint32][]byte{lock - 1: x, lock + 1: y})
	zero := make([]byte, ps)
	check := func(r *Reader, label string, more map[uint32][]byte) {
		t.Helper()
		want := map[uint32][]byte{1: h.state(r.Txn())[0], lock - 1: x, lock: zero, lock + 1: y, lock + 2: zero, size: zero}
		for p, b := range more {
			want[p] = b
		}
		for p, b := range want {
			got, err := r.ReadPage(ctx, p)
			if err != nil || !bytes.Equal(got, b) {
				t.Fatalf("%s: page %d: %v", label, p, err)
			}
		}
	}
	r, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	check(r, "log", nil)

	// Compact to 2 by hand: only the groups holding written pages get
	// objects, not the hundreds of zero groups the model compactor writes.
	m, _ := h.manifest()
	m.Txn, m.DBSize, m.Groups = 2, size, make([]format.GroupRef, format.GroupCount(size, gs))
	for _, p := range []uint32{1, lock - 1, lock + 1} {
		g := (p - 1) / gs
		h.mu.Lock()
		pages := bytes.Join(h.groupAt(2, g), nil)
		h.mu.Unlock()
		var out bytes.Buffer
		bh := format.BaseHeader{DB: h.db, PageSize: ps, GroupSize: gs, Group: g, Txn: 2, Tag: 7, DBSize: size}
		if err := format.EncodeBase(&out, bh, bytes.NewReader(pages)); err != nil {
			t.Fatal(err)
		}
		h.create(keys.Base(h.db, g, 2, 7), out.Bytes())
		m.Groups[g] = format.GroupRef{Txn: 2, Tag: 7}
	}
	h.publish(m)
	h.retireAll()
	if r, err = Open(ctx, st, keys, NewCache(0), 0); err != nil {
		t.Fatal(err)
	}
	check(r, "base", nil)

	h.commit(size+5, map[uint32][]byte{lock + 3: z})
	if err := r.Advance(ctx); err != nil {
		t.Fatal(err)
	}
	check(r, "base and log", map[uint32][]byte{lock + 3: z, size + 5: zero})
}
