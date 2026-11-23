package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// The tests in this file replay the sequences of the record's
// "Interleavings considered" that involve takeover, the lease or fencing.
// Each newEnv starts with leader A (e.l) in office: its bump is 5 and its
// last commit 6, the manifest is at 3, and A's next name is 7. The record's
// numbers are shifted accordingly.

// leaderActor runs one commit of l as an actor and returns the actor and
// the key of the commit it builds.
func leaderActor(t *testing.T, e *env, l *leader) (*actor, string) {
	a := e.g.spawn(t, func(ctx context.Context) { l.commit(ctx) })
	return a, e.keys.Commit(e.h.db, l.head+1)
}

func isRetired(t *testing.T, e *env, txn uint64) (retired, bump bool) {
	t.Helper()
	b, err := get(bg, e.g, e.keys.Commit(e.h.db, txn), 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	h, err := format.DecodeCommitHeader(b[:format.CommitHeaderSize], e.h.db)
	if err != nil {
		t.Fatal(err)
	}
	return h.Retired, h.EpochBump
}

// TestInterleavingLostResponseCommit: A creates its next commit and loses
// the response; B takes over past it, compacts and retires it; A's repeat
// fails its precondition and reads its own retired commit: committed.
func TestInterleavingLostResponseCommit(t *testing.T) {
	e := newEnv(t, 20)
	a, key := leaderActor(t, e, e.l)
	a.until("create", key)
	a.step(drop)
	a.until("create", key)
	b, _ := mustTakeover(t, e, "b")
	if b.Head != 8 {
		t.Fatalf("B bumped at %d, want 8", b.Head)
	}
	e.h.compact(8)
	e.h.retireAll()
	if r, _ := isRetired(t, e, 7); !r {
		t.Fatal("A's commit not retired")
	}
	a.finish()
	if got := e.l.told[e.lastID(7)]; got != committed {
		t.Fatalf("A told %v", got)
	}
	e.check([]Office{e.l.o, b}, []*leader{e.l}, true)
}

// lastID is the commit identifier of the commit at txn.
func (e *env) lastID(txn uint64) string {
	e.t.Helper()
	b, err := get(bg, e.g, e.keys.Commit(e.h.db, txn), 1<<40)
	if err != nil {
		e.t.Fatal(err)
	}
	h, err := format.DecodeCommitHeader(b[:format.CommitHeaderSize], e.h.db)
	if err != nil {
		e.t.Fatal(err)
	}
	return string(h.CommitID)
}

// TestInterleavingGarbageCollectedTakeoverCommit: A pauses before its next
// create; B bumps that name, commits, compacts and collects; the bump is
// never retired, and A's create finds it: fenced, not committed.
func TestInterleavingGarbageCollectedTakeoverCommit(t *testing.T) {
	e := newEnv(t, 21)
	a, key := leaderActor(t, e, e.l)
	a.until("create", key)
	b, _ := mustTakeover(t, e, "b")
	nb := newLeader(e.h, e.g, b)
	if out := nb.commit(bg); out != committed {
		t.Fatal(out)
	}
	e.h.sync()
	e.h.compact(8)
	e.h.retireAll()
	e.h.collect()
	if r, bump := isRetired(t, e, 7); r || !bump {
		t.Fatalf("object at 7: retired %v, bump %v", r, bump)
	}
	a.finish()
	if !e.l.done || len(e.l.told) != 2 {
		t.Fatal("A not fenced")
	}
	for _, out := range e.l.told {
		if out == unknownOutcome {
			t.Fatal("A told unknown")
		}
	}
	e.check([]Office{e.l.o, b}, []*leader{e.l, nb}, true)
}

// TestInterleavingFreedName: candidate C is paused before its bump at A's
// next name; A creates that name and loses the response; B takes over,
// folds and retires A's commit; C's create fails, its walk on finds a
// retired commit above its manifest and it gives up; A's repeat finds its
// own retired commit: committed.
func TestInterleavingFreedName(t *testing.T) {
	e := newEnv(t, 22)
	key := e.keys.Commit(e.h.db, 7)
	c := e.g.candidate(t, e.keys, e.cand("c"))
	c.until("create", key)
	a, _ := leaderActor(t, e, e.l)
	a.until("create", key)
	a.step(drop)
	a.until("create", key)
	b, _ := mustTakeover(t, e, "b")
	e.h.compact(b.Head)
	e.h.retireAll()
	c.finish()
	if !errors.Is(c.err, ErrLost) {
		t.Fatalf("C: %v", c.err)
	}
	a.finish()
	if got := e.l.told[e.lastID(7)]; got != committed {
		t.Fatalf("A told %v", got)
	}
	e.check(append(offices(t, c), e.l.o, b), []*leader{e.l}, true)
}

// TestInterleavingRecycledName is blocker 1: a follower and a paused
// candidate on a manifest whose next names are folded and retired under
// them. The candidate gives up at its re-read, or, if it passed the re-read,
// when its create fails and its walk on meets a retired commit. The
// follower restarts from the current manifest and serves a state that
// existed.
func TestInterleavingRecycledName(t *testing.T) {
	for _, passedReread := range []bool{false, true} {
		e := newEnv(t, 23)
		e.h.compact(6) // the base is complete to 6
		key := e.keys.Commit(e.h.db, 7)
		c := e.g.candidate(t, e.keys, e.cand("c"))
		if passedReread {
			c.until("create", key)
		} else {
			c.until("getrange", key) // its walk finds 7 absent next
			c.step(pass)
			if n := c.next(); n == nil || n.op != "get" || n.key != e.keys.Manifest() {
				t.Fatalf("candidate not at its re-read: %+v", n)
			}
		}
		var r *Reader
		var ferr error
		f := e.g.spawn(t, func(ctx context.Context) { r, ferr = Open(ctx, e.g, e.keys, NewCache(1<<20), 0) })
		f.until("getrange", key) // F has read the manifest at 6
		x := e.l.commit(bg)
		y := e.l.commit(bg)
		if x != committed || y != committed {
			t.Fatal("A's commits")
		}
		e.h.sync()
		b, _ := mustTakeover(t, e, "b")
		e.h.compact(b.Head)
		e.h.retireAll()
		c.finish()
		if !errors.Is(c.err, ErrLost) {
			t.Fatalf("C (passed its re-read %v): %v", passedReread, c.err)
		}
		f.finish()
		if ferr != nil || r.Txn() != b.Head {
			t.Fatalf("follower: %v", ferr)
		}
		if err := e.h.checkReader(bg, r, false); err != nil {
			t.Fatal(err)
		}
		e.check([]Office{e.l.o, b}, []*leader{e.l}, true)
	}
}

// TestInterleavingUnknownCompactionSwap is blocker 2 as it meets a
// takeover: A's compaction replacement is outstanding. If a candidate swaps
// from the version it was conditioned on, it can never land; if it lands
// first, the candidate swaps from it and carries its references.
func TestInterleavingUnknownCompactionSwap(t *testing.T) {
	for _, landsFirst := range []bool{false, true} {
		e := newEnv(t, 24)
		_, v1 := e.h.manifest()
		m6 := e.h.compactObjects(6)
		body := e.h.encodeManifest(m6)
		a := e.g.spawn(t, func(ctx context.Context) {
			_, _ = e.g.Replace(ctx, e.keys.Manifest(), v1, bytes.NewReader(body))
		})
		a.until("replace", e.keys.Manifest())
		a.step(late)
		a.finish()
		if landsFirst {
			if _, err := e.g.land(); err != nil {
				t.Fatal(err)
			}
		}
		d, _ := mustTakeover(t, e, "d")
		_, err := e.g.land()
		if !landsFirst && !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("outstanding replacement after the swap: %v", err)
		}
		m := manifestOf(t, e)
		switch {
		case !d.Holds(m):
			t.Fatal("manifest not the candidate's")
		case landsFirst && (m.Txn != 6 || len(m.Groups) != len(m6.Groups)):
			t.Fatalf("swap from the compacted manifest: %+v", m)
		case !landsFirst && m.Txn != 3:
			t.Fatalf("swap from v1 at %d", m.Txn)
		}
		if landsFirst {
			for g := range m.Groups {
				if m.Groups[g] != m6.Groups[g] {
					t.Fatalf("group %d not carried", g)
				}
			}
		}
		e.check([]Office{e.l.o, d}, nil, true)
	}
}

// TestInterleavingRetireRacesTakeoverWalk: a candidate walking the log
// meets a commit that a newer candidacy folded and retired, before its
// header read and between its header and table reads. It re-reads the
// manifest and gives up.
func TestInterleavingRetireRacesTakeoverWalk(t *testing.T) {
	walkReads(t, 1, format.CommitHeaderSize) // the table in a read of its own
	for _, betweenReads := range []bool{false, true} {
		e := newEnv(t, 25)
		key := e.keys.Commit(e.h.db, 4)
		c := e.g.candidate(t, e.keys, e.cand("c"))
		c.until("getrange", key)
		if betweenReads {
			c.step(pass)
			if n := c.next(); n.op != "getrange" || n.key != key {
				t.Fatalf("not at the table read: %+v", n)
			}
		}
		b, _ := mustTakeover(t, e, "b")
		e.h.compact(b.Head)
		e.h.retireAll()
		c.finish()
		if !errors.Is(c.err, ErrLost) {
			t.Fatalf("between reads %v: %v", betweenReads, c.err)
		}
		e.check([]Office{e.l.o, b}, nil, true)
	}
}

// TestInterleavingTakeoverBetweenCompactionAndRetirement: a candidate
// swaps from the manifest a compaction just published, before that
// compaction's retirements; the old leader's retires land at or below it.
func TestInterleavingTakeoverBetweenCompactionAndRetirement(t *testing.T) {
	e := newEnv(t, 26)
	e.h.compact(6)
	c, _ := mustTakeover(t, e, "c")
	e.h.retireAll() // the old leader's collector, still running
	if c.Head != 7 {
		t.Fatalf("bump at %d", c.Head)
	}
	nc := newLeader(e.h, e.g, c)
	if out := nc.commit(bg); out != committed {
		t.Fatal(out)
	}
	e.check([]Office{e.l.o, c}, []*leader{nc}, true)
}

// TestInterleavingTwoCandidates covers the record's "Two candidates".
func TestInterleavingTwoCandidates(t *testing.T) {
	mk := func(t *testing.T) (*env, *candidate, *candidate) {
		e := newEnv(t, 27)
		return e, e.g.candidate(t, e.keys, e.cand("a")), e.g.candidate(t, e.keys, e.cand("b"))
	}
	t.Run("same manifest", func(t *testing.T) {
		e, a, b := mk(t)
		a.until("replace", e.keys.Manifest())
		b.until("replace", e.keys.Manifest())
		a.finish()
		b.finish()
		if a.err != nil || !errors.Is(b.err, ErrLost) {
			t.Fatalf("a %v, b %v", a.err, b.err)
		}
		e.check(offices(t, a, b), nil, true)
	})
	t.Run("in turn, first stopped at its re-read", func(t *testing.T) {
		e, a, b := mk(t)
		a.until("getrange", e.keys.Commit(e.h.db, 7))
		a.step(pass) // 7 absent: next is the re-read
		b.finish()
		a.finish()
		if b.err != nil || !errors.Is(a.err, ErrLost) {
			t.Fatalf("a %v, b %v", a.err, b.err)
		}
		e.check(offices(t, a, b), nil, true)
	})
	t.Run("in turn, first stopped at its final check", func(t *testing.T) {
		e, a, b := mk(t)
		a.until("create", e.keys.Commit(e.h.db, 7))
		a.step(pass) // its bump lands; next is the final check
		b.finish()
		a.finish()
		if b.err != nil || !errors.Is(a.err, ErrLost) || b.o.Head != 8 {
			t.Fatalf("a %v, b %+v %v", a.err, b.o, b.err)
		}
		e.check(offices(t, a, b), nil, true)
	})
	t.Run("first one's bump lands first", func(t *testing.T) {
		e, a, b := mk(t)
		key := e.keys.Commit(e.h.db, 7)
		a.until("create", key)
		b.until("create", key)
		a.finish()
		b.finish()
		if !errors.Is(a.err, ErrLost) || b.err != nil || b.o.Head != 8 {
			t.Fatalf("a %v, b %+v %v", a.err, b.o, b.err)
		}
		e.check(offices(t, a, b), nil, true)
	})
	t.Run("second one's bump lands first", func(t *testing.T) {
		e, a, b := mk(t)
		key := e.keys.Commit(e.h.db, 7)
		a.until("create", key)
		b.until("create", key)
		b.finish()
		a.finish()
		if !errors.Is(a.err, ErrLost) || b.err != nil || b.o.Head != 7 {
			t.Fatalf("a %v, b %+v %v", a.err, b.o, b.err)
		}
		e.check(offices(t, a, b), nil, true)
	})
}

// TestInterleavingCandidateCrashes: a candidate stops for good at each step
// of its takeover. The old leader keeps committing until a bump fences it
// or its compaction reads a higher epoch; a further takeover always
// restores service.
func TestInterleavingCandidateCrashes(t *testing.T) {
	type crash struct {
		name    string
		at      func(e *env, c *candidate)
		swapped bool // the manifest is the crashed candidate's
		bumped  bool // its bump is in the log
	}
	key := func(e *env) string { return e.keys.Commit(e.h.db, 7) }
	crashes := []crash{
		{"after the lease", func(e *env, c *candidate) { c.until("replace", e.keys.Manifest()) }, false, false},
		{"after the swap", func(e *env, c *candidate) { c.until("getrange", key(e)) }, true, false},
		{"with the swap unknown", func(e *env, c *candidate) {
			c.until("replace", e.keys.Manifest())
			c.step(late)
		}, true, false},
		{"during the walk", func(e *env, c *candidate) { c.until("getrange", e.keys.Commit(e.h.db, 5)) }, true, false},
		{"with its bump created", func(e *env, c *candidate) {
			c.until("create", key(e))
			c.step(pass)
		}, true, true},
		{"with its bump in flight", func(e *env, c *candidate) {
			c.until("create", key(e))
			c.step(late)
		}, true, true},
	}
	for _, cr := range crashes {
		t.Run(cr.name, func(t *testing.T) {
			e := newEnv(t, 28)
			c := e.g.candidate(t, e.keys, e.cand("c"))
			cr.at(e, c)
			e.g.landAll() // whatever it had in flight lands now
			want := committed
			if cr.bumped {
				want = notCommitted
			}
			if out := e.l.commit(bg); out != want {
				t.Fatalf("old leader's commit: %v, want %v", out, want)
			}
			e.h.sync()
			if !cr.bumped && e.l.compact(bg) == cr.swapped {
				t.Fatalf("old leader at its compaction stays in office: %v", !cr.swapped)
			}
			e.h.sync()
			d, _ := mustTakeover(t, e, "d")
			nd := newLeader(e.h, e.g, d)
			if out := nd.commit(bg); out != committed {
				t.Fatal(out)
			}
			e.check([]Office{e.l.o, d}, []*leader{e.l, nd}, true)
		})
	}
}

// TestInterleavingLateCandidate: candidate C passes its re-read and is
// paused before its bump; E swaps a higher epoch and walks; C's bump then
// wins the leader's next name and fences it. C fails its final check and
// never serves; E walks past C's bump and restores service.
func TestInterleavingLateCandidate(t *testing.T) {
	e := newEnv(t, 29)
	key := e.keys.Commit(e.h.db, 7)
	c := e.g.candidate(t, e.keys, e.cand("c"))
	c.until("create", key)
	d := e.g.candidate(t, e.keys, e.cand("e"))
	d.until("create", key)
	c.finish()
	if !errors.Is(c.err, ErrLost) {
		t.Fatalf("late candidate: %v", c.err)
	}
	if out := e.l.commit(bg); out != notCommitted {
		t.Fatalf("live leader not fenced by the late bump: %v", out)
	}
	d.finish()
	if d.err != nil || d.o.Head != 8 {
		t.Fatalf("further takeover: %+v %v", d.o, d.err)
	}
	e.h.sync()
	nd := newLeader(e.h, e.g, d.o)
	if out := nd.commit(bg); out != committed {
		t.Fatal(out)
	}
	e.check(append(offices(t, c, d), e.l.o), []*leader{e.l, nd}, true)
}

// TestBumpsLoseToLeader: an old leader that ignores the lease wins every
// name the candidate tries, three times. The candidate walks on each time,
// has one bump in the log in the end, and the old leader's last commit
// stays below it.
func TestBumpsLoseToLeader(t *testing.T) {
	e := newEnv(t, 30)
	c := e.g.candidate(t, e.keys, e.cand("c"))
	for txn := uint64(7); txn < 10; txn++ {
		if c.until("create", e.keys.Commit(e.h.db, txn)) == nil {
			t.Fatalf("no bump attempt at %d: %v", txn, c.err)
		}
		if out := e.l.commit(bg); out != committed {
			t.Fatal(out)
		}
	}
	c.finish()
	if c.err != nil || c.o.Head != 10 {
		t.Fatalf("%+v %v", c.o, c.err)
	}
	if out := e.l.commit(bg); out != notCommitted || e.l.head != 9 {
		t.Fatalf("old leader after the bump: %v at %d", out, e.l.head)
	}
	e.h.sync()
	e.check(append(offices(t, c), e.l.o), []*leader{e.l}, true)
}

// TestLeaderResponseAfterBump: the leader's create lands, and its response
// arrives only after a candidate's bump occupies the next name. The leader
// reports committed and commits nothing further.
func TestLeaderResponseAfterBump(t *testing.T) {
	e := newEnv(t, 31)
	a, key := leaderActor(t, e, e.l)
	a.until("create", key)
	a.step(hold)
	b, _ := mustTakeover(t, e, "b")
	a.finish()
	if e.l.head != 7 || e.l.told[e.lastID(7)] != committed {
		t.Fatalf("leader head %d", e.l.head)
	}
	if out := e.l.commit(bg); out != notCommitted || e.l.head != 7 || b.Head != 8 {
		t.Fatalf("leader after the bump: %v at %d", out, e.l.head)
	}
	e.check([]Office{e.l.o, b}, []*leader{e.l}, true)
}

// TestCandidatePausedAtEachStep: a candidate paused at each of its storage
// calls, for long enough that another takes office, commits, compacts and
// retires, never takes office.
func TestCandidatePausedAtEachStep(t *testing.T) {
	n := soloCalls(t)
	for k := 1; k < n; k++ { // at n it has finished
		e := newEnv(t, 32)
		c := e.g.candidate(t, e.keys, e.cand("c"))
		c.run(k)
		b, _ := mustTakeover(t, e, "b")
		nb := newLeader(e.h, e.g, b)
		nb.commit(bg)
		nb.commit(bg)
		e.h.sync() // the model cannot follow commits retired before it saw them
		if !nb.compact(bg) {
			t.Fatal("compaction")
		}
		c.finish()
		if c.err == nil {
			t.Fatalf("paused at call %d: took office %+v", k, c.o)
		}
		e.h.sync()
		e.check(append(offices(t, c), e.l.o, b), []*leader{nb}, true)
	}
}

// soloCalls is the number of storage calls of one takeover in newEnv.
func soloCalls(t *testing.T) int {
	e := newEnv(t, 33)
	if _, _, err := Takeover(bg, e.g, e.keys, e.cand("x")); err != nil {
		t.Fatal(err)
	}
	return len(e.g.takeTrace())
}
