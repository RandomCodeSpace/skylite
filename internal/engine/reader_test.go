package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// cacheBudgets are the budgets every model run is repeated with: none, a
// few pages, everything.
var cacheBudgets = map[string]int64{"none": 0, "tiny": 3 * 600, "large": 64 << 20}

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
				if err := runModel(t, seed, budget); err != nil {
					t.Fatalf("seed %d, cache %s: %v", seed, name, err)
				}
			})
		}
	}
}

func runModel(t *testing.T, seed uint64, budget int64) error {
	ctx := context.Background()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, seed, 512, 4)
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
// above it, one probe for the name after the head, and one page record.
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
		wantReqs := 1 + 2*tail + 1
		wantBytes := int64(len(mb)) + int64(tail)*format.CommitHeaderSize + tableBytes
		if reqs != wantReqs || n != wantBytes {
			t.Fatalf("%d groups: open made %d requests for %d bytes, want %d for %d", groups, reqs, n, wantReqs, wantBytes)
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
		if reqs != 1+2*5+1 || n != 5*(format.CommitHeaderSize+format.PageEntrySize) {
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
