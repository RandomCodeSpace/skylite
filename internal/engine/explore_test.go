package engine

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// exploreHistory builds a database with a base, a tail, retired commits,
// collected bases, and commits that shrink and grow it.
func exploreHistory(t *testing.T, st *testStore, keys format.Keys, seed uint64, ps, gs uint32) *history {
	t.Helper()
	h := newHistory(t, st.Store, keys, seed, ps, gs)
	for range 24 {
		switch x := h.rng.IntN(20); {
		case x < 12:
			h.randomCommit(30)
		case x < 13:
			h.bump(h.epoch + 1)
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
	}
	return h
}

// A perturbation changes storage as a leader, compactor or collector would.
type perturbation func(h *history)

func compactHead(h *history) {
	if m, _ := h.manifest(); h.head() > m.Txn {
		h.compact(h.head())
	}
}

func compactMidRetireCollect(h *history) {
	if m, _ := h.manifest(); h.head() > m.Txn {
		h.compact(m.Txn + 1 + h.rng.Uint64N(h.head()-m.Txn))
	}
	h.retireAll()
	h.collect()
}

var perturbations = map[string]perturbation{
	"commit":       func(h *history) { h.randomCommit(30) },
	"bump":         func(h *history) { h.bump(h.epoch + 1) },
	"swap":         func(h *history) { h.swapEpoch() },
	"compact head": compactHead,
	"compact head, retire, collect": func(h *history) {
		compactHead(h)
		h.retireAll()
		h.collect()
	},
	"compact mid, retire, collect": compactMidRetireCollect,
	"two commits, compact, retire, collect": func(h *history) {
		h.randomCommit(30)
		h.randomCommit(30)
		if m, _ := h.manifest(); h.head() > m.Txn+1 {
			h.compact(h.head() - 1)
		}
		h.retireAll()
		h.collect()
	},
}

// explore runs one scenario: readers opened at the head, at the manifest's
// transaction, in the middle of the tail and at the tail's end, each read
// whole, advanced, read, advanced again and read. Before request i of the
// scenario it runs acts[i]. Every page read must be the model's page at the
// reader's transaction or fail with ErrCompacted, Open must reach what it
// was asked for, and Advance at least the head when it began. It returns
// the number of requests the scenario made.
func explore(t *testing.T, seed uint64, ps, gs uint32, budget int64, acts map[int][]perturbation) int {
	t.Helper()
	walkReads(t, 1, firstRead) // requests one at a time, in an order the acts can name
	ctx := context.Background()
	st, keys := newStore(t)
	h := exploreHistory(t, st, keys, seed, ps, gs)
	n := 0
	st.setInject(func(context.Context, string, string) (bool, error) {
		for _, p := range acts[n] {
			p(h)
		}
		n++
		return false, nil
	})
	m, _ := h.manifest()
	head := h.head()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d, page size %d, group size %d, budget %d, acts %d: %s", seed, ps, gs, budget, len(acts), fmt.Sprintf(format, args...))
	}
	for _, target := range []uint64{0, m.Txn, min(head, m.Txn+(head-m.Txn)/2+1), head} {
		if target == 0 && m.Txn == 0 {
			continue
		}
		before := h.head()
		r, err := Open(ctx, st, keys, NewCache(budget), target)
		switch {
		case errors.Is(err, ErrCompacted) && target != 0:
			continue
		case err != nil:
			fail("open %d: %v", target, err)
		case target != 0 && r.Txn() != target, target == 0 && r.Txn() < before:
			fail("opened at %d for %d, the head was %d", r.Txn(), target, before)
		}
		if err := h.checkReader(ctx, r, true); err != nil {
			fail("opened at %d: %v", target, err)
		}
		for range 2 {
			before := h.head()
			if err := r.Advance(ctx); err != nil {
				fail("advance: %v", err)
			}
			if r.Txn() < before {
				fail("advanced to %d, the head was %d", r.Txn(), before)
			}
			if err := h.checkReader(ctx, r, true); err != nil {
				fail("advanced to %d: %v", r.Txn(), err)
			}
		}
	}
	return n
}

// exploreAll runs the explorer exhaustively, as the review of the read path
// did: every request, every cache budget, more seeds. It takes hours.
var exploreAll = flag.Bool("engine.explore", false, "perturb storage before every request of the exploration scenarios")

type exploreConfig struct {
	ps, gs uint32
	budget int64
}

// exploreConfigs are the geometries and cache budgets explored: each
// geometry with one budget by default, with all of them with
// -engine.explore.
func exploreConfigs() []exploreConfig {
	geos := [][2]uint32{{512, 1}, {512, 4}, {1024, 16}}
	budgets := []int64{cacheBudgets["none"], cacheBudgets["tiny"], cacheBudgets["large"]}
	var out []exploreConfig
	for i, geo := range geos {
		for j, b := range budgets {
			if *exploreAll || i == j {
				out = append(out, exploreConfig{geo[0], geo[1], b})
			}
		}
	}
	return out
}

// TestExploreEveryRequest runs each perturbation before requests of the
// scenario, on random histories, geometries and cache budgets: before four
// requests drawn at random from the scenario by default, before every one
// with -engine.explore.
func TestExploreEveryRequest(t *testing.T) {
	seeds, points := []uint64{100}, 4
	if *exploreAll {
		seeds = []uint64{100, 101, 102, 103, 104, 105}
	}
	runs := 0
	for _, seed := range seeds {
		for _, c := range exploreConfigs() {
			total := explore(t, seed, c.ps, c.gs, c.budget, nil)
			rng := rand.New(rand.NewPCG(seed, uint64(c.gs))) //nolint:gosec // seeded so failures reproduce
			for _, name := range slices.Sorted(maps.Keys(perturbations)) {
				for i := range total {
					if !*exploreAll && rng.IntN(total) >= points {
						continue
					}
					explore(t, seed, c.ps, c.gs, c.budget, map[int][]perturbation{i: {perturbations[name]}})
					runs++
				}
			}
		}
	}
	t.Logf("%d runs", runs)
}

// TestExploreThreePoints spreads a compaction, a retirement and a
// collection, or other combinations, over three requests of the scenario.
func TestExploreThreePoints(t *testing.T) {
	n := 8
	if *exploreAll {
		n = 300
	}
	runs := 0
	for _, seed := range []uint64{1000, 1001, 1002} {
		for _, c := range exploreConfigs() {
			total := explore(t, seed, c.ps, c.gs, c.budget, nil)
			rng := rand.New(rand.NewPCG(seed, uint64(c.gs))) //nolint:gosec // seeded so failures reproduce
			for range n {
				i := rng.IntN(total)
				j := i + rng.IntN(total-i)
				k := j + rng.IntN(total-j)
				acts := map[int][]perturbation{}
				add := func(at int, p perturbation) { acts[at] = append(acts[at], p) }
				switch rng.IntN(3) {
				case 0:
					add(i, compactHead)
					add(j, func(h *history) { h.retireAll() })
					add(k, func(h *history) { h.collect() })
				case 1:
					add(i, perturbations["commit"])
					add(j, compactMidRetireCollect)
					add(k, perturbations["swap"])
				default:
					add(i, compactMidRetireCollect)
					add(j, perturbations["commit"])
					add(k, perturbations["compact head, retire, collect"])
				}
				explore(t, seed, c.ps, c.gs, c.budget, acts)
				runs++
			}
		}
	}
	t.Logf("%d runs", runs)
}
