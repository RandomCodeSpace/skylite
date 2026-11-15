package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

// raceCheck checks the end of a race between candidates: no failed
// candidate holds office, no failure other than losing, the invariants, at
// most one office not fenced, and every office's leader either commits or
// is fenced.
func raceCheck(t *testing.T, e *env, label string, cs []*candidate) {
	t.Helper()
	for _, c := range cs {
		if c.err != nil && !errors.Is(c.err, ErrLost) && !errors.Is(c.err, ErrLeaseHeld) {
			t.Fatalf("%s: candidate failed with no fault injected: %v", label, c.err)
		}
	}
	os := append(offices(t, cs...), e.l.o)
	ls := []*leader{e.l}
	if err := e.inv.check(os, ls, true); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	at := e.l.o
	at.Head = e.l.head // the old leader's next name follows its own commits
	if u := unfenced(t, e.g.Store, e.keys, append(offices(t, cs...), at)); len(u) > 1 {
		t.Fatalf("%s: %d offices not fenced: %+v", label, len(u), u)
	}
	slices.SortFunc(os, func(a, b Office) int { return int(a.Epoch) - int(b.Epoch) }) //nolint:gosec // small epochs
	for _, o := range os {
		l := newLeader(e.h, e.g, o)
		l.commit(bg)
		ls = append(ls, l)
	}
	if err := e.inv.check(os, ls, true); err != nil {
		t.Fatalf("%s: after the offices commit: %v", label, err)
	}
}

// TestTwoCandidatesEveryPairOfBoundaries runs two candidates and the leader
// in office: A is paused at each of its storage calls, B at each of its,
// with the old leader committing between phases, then both run to the end.
func TestTwoCandidatesEveryPairOfBoundaries(t *testing.T) {
	n := soloCalls(t)
	for i := 0; i <= n; i++ {
		for j := 0; j <= n; j++ {
			e := newEnv(t, uint64(i*100+j)) //nolint:gosec // small
			a := e.g.candidate(t, e.keys, e.cand("a"))
			b := e.g.candidate(t, e.keys, e.cand("b"))
			a.run(i)
			e.l.commit(bg)
			b.run(j)
			e.l.commit(bg)
			a.finish()
			e.l.commit(bg)
			b.finish()
			e.l.commit(bg)
			raceCheck(t, e, fmt.Sprintf("A at %d, B at %d", i, j), []*candidate{a, b})
		}
	}
}

// TestThreeCandidatesEveryPairOfBoundaries pauses A and B at every pair of
// their storage calls while C runs a whole takeover, then resumes A and B.
func TestThreeCandidatesEveryPairOfBoundaries(t *testing.T) {
	n := soloCalls(t)
	for i := 0; i <= n; i++ {
		for j := 0; j <= n; j++ {
			e := newEnv(t, uint64(i*100+j)) //nolint:gosec // small
			a := e.g.candidate(t, e.keys, e.cand("a"))
			b := e.g.candidate(t, e.keys, e.cand("b"))
			c := e.g.candidate(t, e.keys, e.cand("c"))
			a.run(i)
			e.l.commit(bg)
			b.run(j)
			c.finish()
			e.l.commit(bg)
			a.finish()
			b.finish()
			e.l.commit(bg)
			raceCheck(t, e, fmt.Sprintf("A at %d, B at %d", i, j), []*candidate{a, b, c})
		}
	}
}

// TestRandomSchedules runs the leader in office and three candidates, each
// of which leads once it takes office, under a seeded random schedule. The
// candidates start at random steps. At every step one actor's call is
// passed, failed with no effect, landed with its response lost, or held
// back to land at a later step, at a fault rate the seed sets, and the
// invariants are checked after every step. A failure names its seed.
func TestRandomSchedules(t *testing.T) {
	for seed := uint64(1); seed <= 90; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { randomSchedule(t, seed) })
	}
}

func randomSchedule(t *testing.T, seed uint64) {
	rng := seeded(seed)
	faults := []int{8, 25, 50}[seed%3] // percent of mutations not simply passed
	e := newEnv(t, seed)
	var mu sync.Mutex
	offs := []Office{e.l.o}
	leaders := []*leader{e.l}
	lead := func(ctx context.Context, l *leader) {
		for k := range 8 {
			if l.commit(ctx); l.done {
				return
			}
			if k%4 == 3 && !l.compact(ctx) {
				return
			}
		}
	}
	type scheduled struct {
		*actor
		start int
	}
	actors := []scheduled{{e.g.spawn(t, func(ctx context.Context) { lead(ctx, e.l) }), 0}}
	for _, name := range []string{"a", "b", "c"} {
		a := e.g.spawn(t, func(ctx context.Context) {
			o, _, err := Takeover(ctx, e.g, e.keys, e.cand(name))
			if err != nil {
				return
			}
			l := newLeader(e.h, e.g, o)
			mu.Lock()
			offs = append(offs, o)
			leaders = append(leaders, l)
			mu.Unlock()
			lead(ctx, l)
		})
		actors = append(actors, scheduled{a, rng.IntN(80)})
	}
	check := func(step int, final bool) {
		mu.Lock()
		os, ls := slices.Clone(offs), slices.Clone(leaders)
		mu.Unlock()
		if err := e.inv.check(os, ls, final); err != nil {
			t.Fatalf("seed %d, step %d: %v", seed, step, err)
		}
	}
	for step := 0; ; step++ {
		var live []*actor
		waiting := false
		for _, a := range actors {
			switch {
			case a.finished():
			case a.start > step:
				waiting = true
			default:
				live = append(live, a.actor)
			}
		}
		if len(live) == 0 && !waiting {
			break
		}
		if rng.IntN(10) == 0 {
			_, _ = e.g.land()
		}
		if len(live) == 0 {
			continue
		}
		a := live[rng.IntN(len(live))]
		c := a.next()
		mutation := c.op == "create" || c.op == "replace" || c.op == "delete"
		act := pass
		switch r := rng.IntN(100); {
		case mutation && r < faults/3:
			act = fail
		case mutation && r < 2*faults/3:
			act = drop
		case mutation && r < faults:
			act = late
		case !mutation && r < faults/4:
			act = fail
		}
		a.step(act)
		check(step, false)
	}
	e.g.landAll()
	check(-1, true)
}
