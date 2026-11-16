package engine

// Tests for decision 8 of record 0004: compaction, retirement and garbage
// collection. The tests here drive the compaction attempt, the retire and
// the garbage rule directly against the model in helpers_test.go; the
// leader's tests are in compact_leader_test.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// adopt makes a manifest the compaction published the model's.
func (h *history) adopt(a anchor) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.m, h.mv = a.manifest(), a.version
}

// office is the model's manifest as an Office.
func (h *history) office() Office {
	m, _ := h.manifest()
	return Office{DB: m.DB, Epoch: m.Epoch, Nonce: m.Nonce}
}

// compactModel runs one compaction attempt to target through st, from the
// manifest in storage, and adopts what it publishes.
func compactModel(ctx context.Context, h *history, st storage.Store, target uint64) (anchor, error) {
	r, err := Open(ctx, st, h.keys, nil, target)
	if err != nil {
		return anchor{}, err
	}
	k := &compaction{st: st, keys: h.keys, a: r.a, r: r, retired: r.a.m.RetiredThrough}
	a, err := k.run(ctx)
	if err == nil {
		h.adopt(a)
	}
	return a, err
}

// collectModel deletes what the garbage rule makes garbage, at once.
func collectModel(t testing.TB, ctx context.Context, h *history, st storage.Store) []string {
	t.Helper()
	keys, err := findGarbage(ctx, st, h.keys, h.office(), maxGarbage)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if err := st.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

// referenced returns the base keys manifest m references.
func referenced(keys format.Keys, m format.Manifest) []string {
	var out []string
	for g, r := range m.Groups {
		if r.Txn != 0 {
			out = append(out, keys.Base(m.DB, uint32(g), r.Txn, r.Tag)) //nolint:gosec // small test databases
		}
	}
	return out
}

// checkReferenced fails unless every base object the current manifest
// references exists.
func checkReferenced(t testing.TB, ctx context.Context, h *history, st storage.Store) {
	t.Helper()
	m, _ := h.manifest()
	for _, k := range referenced(h.keys, m) {
		if _, err := st.Head(ctx, k); err != nil {
			t.Fatalf("manifest at %d references %s: %v", m.Txn, k, err)
		}
	}
}

// checkStates opens a reader at every transaction from the manifest's to
// the head, every step-th, and compares it with the model.
func checkStates(t testing.TB, ctx context.Context, h *history, st storage.Store, step uint64) {
	t.Helper()
	m, _ := h.manifest()
	head := h.head()
	for txn := max(m.Txn, 1); txn <= head; txn += step {
		r, err := Open(ctx, st, h.keys, nil, txn)
		if err != nil {
			t.Fatalf("open at %d over a manifest at %d: %v", txn, m.Txn, err)
		}
		if err := h.checkReader(ctx, r, false); err != nil {
			t.Fatal(err)
		}
	}
	if m.Txn > 1 {
		if _, err := Open(ctx, st, h.keys, nil, m.Txn-1); !errors.Is(err, ErrCompacted) {
			t.Fatalf("open below the manifest at %d: %v", m.Txn, err)
		}
	}
}

// Covers: decision 8, compaction, against the model: random histories that
// grow, shrink and bump, compacted to random targets, retired and
// collected, read at every transaction the manifest still serves; readers
// that began before each step read their state or report it compacted, and
// the manifest never references a missing object.
func TestCompactionOracle(t *testing.T) {
	ctx := context.Background()
	for seed := range uint64(12) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			st, keys := newStore(t)
			ps, gs := uint32(512), []uint32{1, 2, 4, 8}[seed%4]
			h := newHistory(t, st.Store, keys, seed, ps, gs)
			var readers []*Reader
			for step := range 60 {
				for range 1 + h.rng.IntN(4) {
					if h.rng.IntN(10) == 0 {
						h.bump(h.epoch + 1)
					} else {
						h.randomCommit(6 * gs)
					}
				}
				if h.rng.IntN(3) == 0 {
					r, err := Open(ctx, st, keys, nil, 0)
					if err != nil {
						t.Fatal(err)
					}
					if readers = append(readers, r); len(readers) > 6 {
						readers = readers[1:]
					}
				}
				m, _ := h.manifest()
				if head := h.head(); head > m.Txn && h.rng.IntN(2) == 0 {
					target := m.Txn + 1 + h.rng.Uint64N(head-m.Txn)
					if _, err := compactModel(ctx, h, st, target); err != nil {
						t.Fatalf("step %d: compaction to %d: %v", step, target, err)
					}
				}
				if h.rng.IntN(3) == 0 {
					m, _ := h.manifest()
					for k := m.RetiredThrough + 1; k <= m.Txn; k++ {
						if ok, err := retireCommit(ctx, st, keys, h.db, k); err != nil || !ok {
							t.Fatalf("retire %d: %v, %v", k, ok, err)
						}
					}
				}
				if h.rng.IntN(3) == 0 {
					collectModel(t, ctx, h, st)
				}
				checkReferenced(t, ctx, h, st)
				for _, r := range readers {
					if err := h.checkReader(ctx, r, true); err != nil {
						t.Fatalf("step %d: a reader at %d: %v", step, r.Txn(), err)
					}
				}
			}
			checkStates(t, ctx, h, st, 1)
		})
	}
}

// Covers: decision 8, "a compaction rewrites only changed groups": a commit
// that writes one group gets one new object and the other references carry
// forward; a commit that shrinks the database rewrites the group the new
// size cuts and drops the groups above it; one that grows it again with
// unwritten pages reads them as zeros.
func TestCompactionRewritesChangedGroups(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	const gs = 4
	h := newHistory(t, st.Store, keys, 1, 512, gs)
	pages := map[uint32][]byte{}
	for p := uint32(1); p <= 5*gs; p++ {
		pages[p] = h.bytes(512)
	}
	h.commit(5*gs, pages)
	if _, err := compactModel(ctx, h, st, h.head()); err != nil {
		t.Fatal(err)
	}
	before, _ := h.manifest()
	h.commit(5*gs, map[uint32][]byte{2*gs + 1: h.bytes(512)}) // group 2
	if _, err := compactModel(ctx, h, st, h.head()); err != nil {
		t.Fatal(err)
	}
	after, _ := h.manifest()
	for g := range after.Groups {
		if same := after.Groups[g] == before.Groups[g]; same == (g == 2) {
			t.Fatalf("group %d: %v before, %v after", g, before.Groups[g], after.Groups[g])
		}
	}
	// Cut group 2 after its first page and drop 3 and 4, then grow back:
	// pages above 2*gs+1 read as zeros.
	h.commit(2*gs+1, map[uint32][]byte{2*gs + 1: h.bytes(512)})
	h.commit(4*gs, map[uint32][]byte{2*gs + 1: h.bytes(512)})
	if _, err := compactModel(ctx, h, st, h.head()); err != nil {
		t.Fatal(err)
	}
	last, _ := h.manifest()
	if len(last.Groups) != 4 || last.Groups[0] != after.Groups[0] || last.Groups[1] != after.Groups[1] ||
		last.Groups[2] == after.Groups[2] || last.Groups[3].Txn != last.Txn {
		t.Fatalf("after the shrink: %v, before it %v", last.Groups, after.Groups)
	}
	checkStates(t, ctx, h, st, 1)
}

// Covers: Limits and size, "Databases above 1 GiB work ... the reader, the
// commit log, compaction and retirement must all handle that page": a
// database past SQLite's lock page, written on both sides of it, compacted
// and retired, reads the lock page as zeros and its neighbours as written.
func TestCompactionAroundLockPage(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	const ps = 512
	lock := format.LockPage(ps)
	h := newHistory(t, st.Store, keys, 1, ps, 64)
	want := map[uint32][]byte{lock - 1: h.bytes(ps), lock + 1: h.bytes(ps)}
	h.commit(lock+2, want)
	if _, err := compactModel(ctx, h, st, h.head()); err != nil {
		t.Fatal(err)
	}
	for k := uint64(1); k <= h.head(); k++ {
		if _, err := retireCommit(ctx, st, keys, h.db, k); err != nil {
			t.Fatal(err)
		}
	}
	r, err := Open(ctx, st, keys, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []uint32{lock - 1, lock, lock + 1, lock + 2} {
		got, err := r.ReadPage(ctx, p)
		if err != nil {
			t.Fatalf("page %d: %v", p, err)
		}
		if w, ok := want[p]; ok && !bytes.Equal(got, w) || !ok && !allZeros([][]byte{got}) {
			t.Fatalf("page %d holds the wrong content", p)
		}
	}
}

// Covers: Limits and size, "A compaction that would produce a manifest
// above the maximum size is refused": the attempt fails before it writes
// anything.
func TestCompactionManifestTooLarge(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 1, 512, 1)
	h.commit(64, map[uint32][]byte{64: h.bytes(512)})
	old := maxManifestSize
	maxManifestSize = 84 + 16*63 + 4 // one group short
	t.Cleanup(func() { maxManifestSize = old })
	st.resetCounts()
	if _, err := compactModel(ctx, h, st, h.head()); !errors.Is(err, format.ErrManifestTooLarge) {
		t.Fatalf("an oversized manifest: %v", err)
	}
	if list, err := st.List(ctx, keys.BasePrefix(h.db), "", 10); err != nil || len(list) != 0 {
		t.Fatalf("base objects after a refused compaction: %v, %v", list, err)
	}
}

// reload makes the manifest in storage the model's.
func (h *history) reload(ctx context.Context) {
	h.t.Helper()
	r := &Reader{st: h.st, keys: h.keys}
	a, err := r.readManifest(ctx, anchor{}, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	h.adopt(a)
}

// fixedTags makes drawTag return tags in order, then fail.
func fixedTags(t *testing.T, tags ...uint64) {
	t.Helper()
	old := drawTag
	t.Cleanup(func() { drawTag = old })
	drawTag = func() (uint64, error) {
		if len(tags) == 0 {
			return 0, errors.New("no more test tags")
		}
		tag := tags[0]
		tags = tags[1:]
		return tag, nil
	}
}

// faultHistory is a model whose fixture writes bypass st's faults, with a
// tail of commits over two groups above an empty manifest.
func faultHistory(t *testing.T) (*faultStore, format.Keys, *history) {
	t.Helper()
	st, keys := newFaultStore(t)
	h := newHistory(t, st.Store, keys, 7, 512, 4)
	for range 6 {
		h.randomCommit(8)
	}
	return st, keys, h
}

// nth returns a rule that gives fault f to the first n calls of op on keys
// that match, and passes every other call through.
func nth(op string, match func(string) bool, n int, f fault) func(string, string) fault {
	seen := 0
	return func(o, key string) fault {
		if o == op && match(key) && seen < n {
			seen++
			return f
		}
		return fault{}
	}
}

func isBase(key string) bool { return bytes.Contains([]byte(key), []byte("/base/")) }

// Covers: Fencing, takeover and garbage collection, sticky uncertainty for a
// base create and a manifest replacement, and Commit outcomes, "A manifest
// replacement with an unknown outcome is resolved as decision 6 says ... for
// compaction": a lost response is resolved by reading the attempt's own
// bytes; attempts with no effect after an unknown one resolve nothing, so
// the group moves to a new tag, or the replacement is unresolved; a
// replacement still in flight lands later on output that still exists.
func TestCompactionUnknownOutcomes(t *testing.T) {
	ctx := context.Background()
	isManifest := func(k string) bool { return bytes.HasSuffix([]byte(k), []byte("/manifest")) }

	t.Run("base create lost response", func(t *testing.T) {
		st, keys, h := faultHistory(t)
		st.setRule(nth("create", isBase, 1, fault{drop: true}))
		a, err := compactModel(ctx, h, st, h.head())
		if err != nil {
			t.Fatal(err)
		}
		list, _ := st.Store.List(ctx, keys.BasePrefix(h.db), "", 100)
		if len(list) != len(referenced(keys, a.manifest())) {
			t.Fatalf("%d base objects for %d references", len(list), len(referenced(keys, a.manifest())))
		}
		checkStates(t, ctx, h, st.Store, 1)
	})

	t.Run("base create unknown, then no effect", func(t *testing.T) {
		st, keys, h := faultHistory(t)
		fixedTags(t, 1, 2, 3, 4, 5, 6, 7, 8)
		late := newGate()
		first := true
		st.setRule(func(op, key string) fault {
			if op != "create" || !bytes.HasSuffix([]byte(key), []byte("-0000000000000001")) {
				return fault{}
			}
			if first {
				first = false
				return fault{late: late}
			}
			return fault{unknownNone: true}
		})
		a, err := compactModel(ctx, h, st, h.head())
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range referenced(keys, a.manifest()) {
			if bytes.HasSuffix([]byte(k), []byte("-0000000000000001")) {
				t.Fatalf("the manifest references %s, whose create never resolved", k)
			}
		}
		late.open()
		st.late.Wait() // the delayed create lands: garbage for good
		garbage := collectModel(t, ctx, h, st.Store)
		if len(garbage) != 1 || !bytes.HasSuffix([]byte(garbage[0]), []byte("-0000000000000001")) {
			t.Fatalf("garbage %v", garbage)
		}
		checkReferenced(t, ctx, h, st.Store)
		checkStates(t, ctx, h, st.Store, 1)
	})

	t.Run("manifest replacement lost response", func(t *testing.T) {
		st, _, h := faultHistory(t)
		st.setRule(nth("replace", isManifest, 1, fault{drop: true}))
		if _, err := compactModel(ctx, h, st, h.head()); err != nil {
			t.Fatal(err)
		}
		checkStates(t, ctx, h, st.Store, 1)
	})

	t.Run("manifest replacement unknown, then no effect", func(t *testing.T) {
		st, keys, h := faultHistory(t)
		st.setRule(func(op, key string) fault {
			if op == "replace" && isManifest(key) {
				return fault{unknownNone: true}
			}
			return fault{}
		})
		target := h.head()
		if _, err := compactModel(ctx, h, st, target); !errors.Is(err, errCompactUnresolved) {
			t.Fatalf("an unresolved replacement: %v", err)
		}
		// Its output is kept while the manifest is below its target.
		if g := collectModel(t, ctx, h, st.Store); len(g) != 0 {
			t.Fatalf("collected %v below the output's transaction", g)
		}
		if list, _ := st.Store.List(ctx, keys.BasePrefix(h.db), "", 100); len(list) == 0 {
			t.Fatal("the output is gone")
		}
	})

	t.Run("manifest replacement in flight lands later", func(t *testing.T) {
		st, _, h := faultHistory(t)
		late := newGate()
		first := true
		st.setRule(func(op, key string) fault {
			if op != "replace" || !isManifest(key) {
				return fault{}
			}
			if first {
				first = false
				return fault{late: late}
			}
			return fault{unknownNone: true}
		})
		if _, err := compactModel(ctx, h, st, h.head()); !errors.Is(err, errCompactUnresolved) {
			t.Fatalf("an unresolved replacement: %v", err)
		}
		collectModel(t, ctx, h, st.Store)
		late.open()
		st.late.Wait()
		h.reload(ctx)
		if m, _ := h.manifest(); m.Txn != h.head() {
			t.Fatalf("the delayed replacement did not land: manifest at %d", m.Txn)
		}
		checkReferenced(t, ctx, h, st.Store)
		checkStates(t, ctx, h, st.Store, 1)
	})
}

// Covers: Fencing, takeover and garbage collection, "A tag collision is
// forced while a create is unknown: with identical bytes the attempt
// references the object and the manifest references correct bytes that are
// not deleted while referenced; with different bytes the attempt moves the
// group to a new tag", and the sequential variant of the third review: an
// earlier attempt's output at the same key is adopted only on its bytes.
func TestCompactionTagCollision(t *testing.T) {
	ctx := context.Background()
	isManifest := func(k string) bool { return bytes.HasSuffix([]byte(k), []byte("/manifest")) }
	for _, same := range []bool{true, false} {
		t.Run(fmt.Sprint("identical bytes ", same), func(t *testing.T) {
			st, keys, h := faultHistory(t)
			target := h.head()
			// Attempt Q creates its output at tag 9 and then fails, with no
			// effect, to replace the manifest.
			fixedTags(t, 9, 9, 9, 9)
			st.setRule(func(op, key string) fault {
				if op == "replace" && isManifest(key) {
					return fault{noEffect: true}
				}
				return fault{}
			})
			if _, err := compactModel(ctx, h, st, target); err == nil || errors.Is(err, errCompactUnresolved) {
				t.Fatalf("attempt Q: %v", err)
			}
			list, _ := st.Store.List(ctx, keys.BasePrefix(h.db), "", 100)
			if !same {
				for _, e := range list {
					if err := st.Delete(ctx, e.Key); err != nil {
						t.Fatal(err)
					}
					if _, err := st.Store.Create(ctx, e.Key, bytes.NewReader([]byte("other bytes"))); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Attempt P draws the same tag, its first create's outcome is
			// unknown, and it then finds the key taken.
			fixedTags(t, 9, 9, 9, 9, 10, 11, 12, 13)
			late := newGate()
			st.setRule(nth("create", isBase, 1, fault{late: late}))
			a, err := compactModel(ctx, h, st, target)
			if err != nil {
				t.Fatal(err)
			}
			late.open()
			st.late.Wait()
			for _, k := range referenced(keys, a.manifest()) {
				if hasTag9 := bytes.HasSuffix([]byte(k), []byte("-0000000000000009")); hasTag9 != same {
					t.Fatalf("identical bytes %v, and the manifest references %s", same, k)
				}
			}
			collectModel(t, ctx, h, st.Store)
			checkReferenced(t, ctx, h, st.Store)
			checkStates(t, ctx, h, st.Store, 1)
		})
	}
}

// Covers: decision 8, "An attempt whose replacement is known not to have
// taken effect, or whose predecessor is gone, ends. Its output is left to
// the garbage rule", and Fencing, takeover and garbage collection, both
// base-deletion schedules: an attempt whose predecessor an epoch swap
// replaced ends without publishing; its output is not garbage while the
// manifest is below its target, even after another compaction below it,
// and a later attempt at the same target publishes with new tags; only then
// is the first output collected, and the manifest never references a
// missing object, also with a delete delayed past the later publication.
func TestCompactionLostPredecessor(t *testing.T) {
	ctx := context.Background()
	st, keys, h := faultHistory(t)
	target := h.head()
	before := newGate()
	st.setRule(nth("replace", func(k string) bool { return k == keys.Manifest() }, 1, fault{before: before}))
	done := make(chan error, 1)
	go func() {
		_, err := compactModel(ctx, h, st, target)
		done <- err
	}()
	<-before.reached
	h.swapEpoch()
	before.open()
	if err := <-done; !errors.Is(err, errCompactLost) {
		t.Fatalf("an attempt whose predecessor was replaced: %v", err)
	}
	firstOutput, _ := st.Store.List(ctx, keys.BasePrefix(h.db), "", 100)
	if len(firstOutput) == 0 {
		t.Fatal("no output")
	}
	if _, err := compactModel(ctx, h, st, target-2); err != nil {
		t.Fatal(err)
	}
	if g := collectModel(t, ctx, h, st.Store); len(g) != 0 {
		t.Fatalf("collected %v below the output's transaction", g)
	}
	// A delete of the first output, delayed past the later publication.
	late := newGate()
	st.setRule(func(op, key string) fault {
		if op == "delete" {
			return fault{late: late}
		}
		return fault{}
	})
	if _, err := compactModel(ctx, h, st, target); err != nil {
		t.Fatal(err)
	}
	garbage, err := findGarbage(ctx, st, keys, h.office(), maxGarbage)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range garbage {
		_ = st.Delete(ctx, k)
	}
	if _, err := compactModel(ctx, h, st, h.randomCommit(8)); err != nil {
		t.Fatal(err)
	}
	late.open()
	st.late.Wait()
	for _, e := range firstOutput {
		if _, err := st.Store.Head(ctx, e.Key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("the lost attempt's output %s: %v", e.Key, err)
		}
	}
	checkReferenced(t, ctx, h, st.Store)
	checkStates(t, ctx, h, st.Store, 1)
}

// Covers: decision 8, "A retire whose outcome is unknown needs no
// resolution for safety ... A retire is confirmed by a read of the object
// that returns the retired form", and sticky uncertainty for a retire: a
// lost response is confirmed by the read after it, an unknown outcome with
// no effect is not, and a delayed retire confirms later; epoch bumps are
// never retired and read as confirmed.
func TestRetireOutcomes(t *testing.T) {
	ctx := context.Background()
	st, keys, h := faultHistory(t)
	bump := h.bump(2)
	h.randomCommit(8)
	if _, err := compactModel(ctx, h, st, h.head()); err != nil {
		t.Fatal(err)
	}
	late := newGate()
	for _, tc := range []struct {
		k    uint64
		f    fault
		want bool
	}{
		{2, fault{drop: true}, true},
		{3, fault{unknownNone: true}, false},
		{4, fault{late: late}, false},
		{bump, fault{}, true},
	} {
		st.setRule(nth("replace", isCommit, 1, tc.f))
		if ok, err := retireCommit(ctx, st, keys, h.db, tc.k); ok != tc.want || err != nil {
			t.Fatalf("retire %d: %v, %v; want %v", tc.k, ok, err, tc.want)
		}
	}
	st.setRule(nil)
	late.open()
	st.late.Wait()
	for k := uint64(1); k <= h.head(); k++ {
		ok, err := retireCommit(ctx, st, keys, h.db, k)
		if err != nil || !ok {
			t.Fatalf("retire %d: %v, %v", k, ok, err)
		}
		hd, _, err := readHeader(ctx, st.Store, keys, h.db, k)
		if err != nil || hd.Retired == hd.EpochBump {
			t.Fatalf("commit %d: retired %v, bump %v, %v", k, hd.Retired, hd.EpochBump, err)
		}
	}
}

// Covers: Interleavings considered, "Two compaction attempts by one leader
// overlap, in every order": attempts that read the same manifest, at the
// same and at different targets, each paused before its replacement, land
// at most one; the other ends, and its output is collected only once the
// manifest passes its target.
func TestCompactionOverlappingAttempts(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ p, q uint64 }{{0, 0}, {0, 2}, {2, 0}} {
		t.Run(fmt.Sprint(tc.p, tc.q), func(t *testing.T) {
			st, keys, h := faultHistory(t)
			head := h.head()
			r1, _ := Open(ctx, st, keys, nil, head-tc.p)
			r2, _ := Open(ctx, st, keys, nil, head-tc.q)
			gates := []*gate{newGate(), newGate()}
			calls := 0
			st.setRule(func(op, key string) fault {
				if op == "replace" && key == keys.Manifest() && calls < 2 {
					calls++
					return fault{before: gates[calls-1]}
				}
				return fault{}
			})
			type result struct {
				a   anchor
				err error
			}
			done := make(chan result, 2)
			for i, r := range []*Reader{r1, r2} {
				go func() {
					k := &compaction{st: st, keys: keys, a: r.a, r: r}
					a, err := k.run(ctx)
					done <- result{a, err}
				}()
				<-gates[i].reached
			}
			gates[1].open()
			first := <-done
			gates[0].open()
			second := <-done
			if first.err != nil || !errors.Is(second.err, errCompactLost) {
				t.Fatalf("overlapping attempts: %v, then %v", first.err, second.err)
			}
			h.adopt(first.a)
			collectModel(t, ctx, h, st.Store)
			checkReferenced(t, ctx, h, st.Store)
			if _, err := compactModel(ctx, h, st, h.randomCommit(8)); err != nil {
				t.Fatal(err)
			}
			collectModel(t, ctx, h, st.Store)
			list, _ := st.Store.List(ctx, keys.BasePrefix(h.db), "", 100)
			m, _ := h.manifest()
			if len(list) != len(referenced(keys, m)) {
				t.Fatalf("%d base objects for %d references once the manifest passed both targets", len(list), len(referenced(keys, m)))
			}
			checkStates(t, ctx, h, st.Store, 1)
		})
	}
}

// Covers: Invariants, "A read transaction that meets ... a changed version
// token on a log object it indexed ... fails or restarts", for the
// compactor: a commit it indexed and then finds retired, which only a newer
// manifest explains, ends the attempt as stale, and nothing is published.
func TestCompactionMeetsRetiredCommit(t *testing.T) {
	ctx := context.Background()
	st, keys, h := faultHistory(t)
	r, err := Open(ctx, st, keys, nil, h.head())
	if err != nil {
		t.Fatal(err)
	}
	m, mv := h.manifest()
	h.compact(h.head()) // another actor's compaction, then its retirement
	h.retireAll()
	k := &compaction{st: st, keys: keys, a: r.a, r: r}
	var stale *staleError
	if _, err := k.run(ctx); !errors.As(err, &stale) {
		t.Fatalf("an attempt over a retired commit: %v", err)
	}
	if now, v := h.manifest(); v == mv || now.Txn == m.Txn {
		t.Fatal("the other compaction did not publish")
	}
}
