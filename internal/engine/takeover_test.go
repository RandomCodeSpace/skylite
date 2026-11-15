package engine

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

var bg = context.Background()

func mustTakeover(t *testing.T, e *env, holder string) (Office, *Lease) {
	t.Helper()
	o, l, err := Takeover(bg, e.g, e.keys, e.cand(holder))
	if err != nil {
		t.Fatalf("takeover by %s: %v", holder, err)
	}
	e.h.sync()
	return o, l
}

func manifestOf(t testing.TB, e *env) format.Manifest {
	t.Helper()
	e.h.sync()
	m, _ := e.h.manifest()
	return m
}

func TestTakeoverAlone(t *testing.T) {
	e := newEnv(t, 1)
	before := manifestOf(t, e)
	o, l := mustTakeover(t, e, "b")
	m := manifestOf(t, e)
	switch {
	case o.DB != e.h.db || o.Epoch != before.Epoch+1 || o.Head != 7:
		t.Fatalf("office %+v after manifest epoch %d", o, before.Epoch)
	case !o.Holds(m) || m.Txn != before.Txn || m.RetiredThrough != before.RetiredThrough || len(m.Groups) != len(before.Groups):
		t.Fatalf("manifest after swap %+v, before %+v", m, before)
	case l == nil:
		t.Fatal("no lease")
	}
	for g := range m.Groups {
		if m.Groups[g] != before.Groups[g] {
			t.Fatalf("group %d reference changed by the swap", g)
		}
	}
	if out := e.l.commit(bg); out != notCommitted || !e.l.done {
		t.Fatalf("deposed leader's next commit: %v", out)
	}
	nl := newLeader(e.h, e.g, o)
	if out := nl.commit(bg); out != committed {
		t.Fatalf("new leader's commit: %v", out)
	}
	e.check([]Office{e.l.o, o}, []*leader{e.l, nl}, true)
}

func TestHolds(t *testing.T) {
	o := Office{DB: format.DatabaseID{1}, Epoch: 4, Head: 9, Nonce: [16]byte{7}}
	m := format.Manifest{DB: o.DB, Epoch: 4, Nonce: o.Nonce, Txn: 20}
	if !o.Holds(m) {
		t.Fatal("own manifest not held")
	}
	for name, f := range map[string]func(*format.Manifest){
		"higher epoch": func(m *format.Manifest) { m.Epoch = 5 },
		"lower epoch":  func(m *format.Manifest) { m.Epoch = 3 },
		"other nonce":  func(m *format.Manifest) { m.Nonce[0] = 8 },
		"other db":     func(m *format.Manifest) { m.DB[0] = 2 },
	} {
		n := m
		f(&n)
		if o.Holds(n) {
			t.Errorf("%s: held", name)
		}
	}
}

func TestRecognise(t *testing.T) {
	e := newEnv(t, 2)
	o := e.l.o
	built, bh := e.l.build()
	retired, err := bh.Retire()
	if err != nil {
		t.Fatal(err)
	}
	enc := func(txn uint64, db format.DatabaseID, epoch uint64, bump bool) []byte {
		e.h.mu.Lock()
		defer e.h.mu.Unlock()
		pages := map[uint32][]byte{1: e.h.bytes(512)}
		if bump {
			pages = nil
		}
		b, _ := e.h.encodeCommit(txn, db, epoch, e.l.size, pages, bump)
		return b
	}
	txn := bh.Txn
	other := format.DatabaseID{9, 9}
	sameID := bytes.Clone(built)
	sameID[len(sameID)-1] ^= 1 // another page body under the same header
	cases := []struct {
		name  string
		found []byte
		want  Found
	}{
		{"own live", built, FoundOwn},
		{"own retired", retired, FoundOwn},
		{"bump lower epoch", enc(txn, o.DB, o.Epoch-1, true), FoundBump},
		{"bump higher epoch", enc(txn, o.DB, o.Epoch+1, true), FoundBump},
		{"bump at own epoch", enc(txn, o.DB, o.Epoch, true), FoundUnknown},
		{"bump of another database", enc(txn, other, o.Epoch+1, true), FoundUnknown},
		{"bump for another name", enc(txn+1, o.DB, o.Epoch+1, true), FoundUnknown},
		{"truncated bump", enc(txn, o.DB, o.Epoch+1, true)[:format.CommitHeaderSize-1], FoundUnknown},
		{"bump with trailing bytes", append(enc(txn, o.DB, o.Epoch+1, true), 0), FoundUnknown},
		{"commit at own epoch, other identifier", enc(txn, o.DB, o.Epoch, false), FoundUnknown},
		{"commit at another epoch", enc(txn, o.DB, o.Epoch+1, false), FoundUnknown},
		{"own header, other bytes", sameID, FoundUnknown},
		{"commit of another database", enc(txn, other, o.Epoch, false), FoundUnknown},
		{"absent", nil, FoundUnknown},
		{"garbage", bytes.Repeat([]byte{0xa5}, 300), FoundUnknown},
	}
	for _, c := range cases {
		got, err := o.Recognise(built, c.found)
		if got != c.want || (got == FoundUnknown) != (err != nil) {
			t.Errorf("%s: %v, %v; want %v", c.name, got, err, c.want)
		}
	}
	bump := enc(txn, o.DB, o.Epoch, true)
	if got, err := o.Recognise(bump, bump); got != FoundUnknown || err == nil {
		t.Errorf("a bump as the leader's own commit: %v", got)
	}
	if got, _ := (Office{DB: o.DB, Epoch: o.Epoch + 1, Head: o.Head, Nonce: o.Nonce}).Recognise(built, built); got != FoundUnknown {
		t.Errorf("a commit of another office recognised: %v", got)
	}
}

// TestDeposedLeaderRecognition: a deposed leader's own commit, live and then
// retired, is committed; the bump after it fences it.
func TestDeposedLeaderRecognition(t *testing.T) {
	e := newEnv(t, 3)
	a := e.g.spawn(t, func(ctx context.Context) { e.l.commit(ctx) })
	c := a.until("create", e.keys.Commit(e.h.db, 7))
	a.step(drop) // lands; A sees unknown
	if a.until("create", c.key) == nil {
		t.Fatal("leader did not repeat its create")
	}
	b, _ := mustTakeover(t, e, "b")
	a.finish()
	if out := e.l.told; len(out) != 2 {
		t.Fatalf("told %v", out)
	}
	for _, out := range e.l.told {
		if out != committed && out != notCommitted {
			t.Fatalf("outcome %v", out)
		}
	}
	if b.Head != 8 {
		t.Fatalf("bump at %d, want 8 after the landed commit 7", b.Head)
	}
	nb := newLeader(e.h, e.g, b)
	nb.commit(bg)
	e.h.sync()
	e.h.compact(9)
	e.h.retireAll()
	// The deposed leader's commit 7 is now retired; reading it again still
	// says committed, and its next name holds the bump that fenced it.
	got, err := get(bg, e.g, e.keys.Commit(e.h.db, 7))
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := format.DecodeCommitHeader(got[:format.CommitHeaderSize], e.h.db); !h.Retired {
		t.Fatal("commit 7 not retired")
	}
	l := e.l
	l.head, l.done = 6, false
	if out := l.commit(bg); out == committed {
		t.Fatal("a second, different commit at 7 was accepted")
	}
	e.check([]Office{e.l.o, b}, []*leader{nb}, true)
}

func TestTakeoverReadsNoContent(t *testing.T) {
	var counts []int
	for _, size := range []uint32{8, 200} {
		st, keys := newMemStore(t)
		g := &gated{Store: st}
		h := newHistory(t, st, keys, 4, 512, 4)
		h.commit(size, map[uint32][]byte{size: h.bytes(512)})
		h.compact(2)
		h.commit(size, map[uint32][]byte{1: h.bytes(512)})
		h.commit(size, map[uint32][]byte{2: h.bytes(512)})
		g.takeTrace()
		if _, _, err := Takeover(bg, g, keys, Candidate{Holder: "a", TTL: time.Minute, Now: newClock(0).now}); err != nil {
			t.Fatal(err)
		}
		trace := g.takeTrace()
		for _, c := range trace {
			op, key, _ := strings.Cut(c, " ")
			switch {
			case op == "list":
				t.Fatalf("takeover listed %s", key)
			case key == keys.Manifest(), key == keys.Lease():
			case strings.HasPrefix(key, keys.CommitPrefix(h.db)):
				if _, txn, err := keys.ParseCommit(key); err != nil || txn <= 2 {
					t.Fatalf("takeover read %s, at or below the manifest", key)
				}
			default:
				t.Fatalf("takeover read %s", key)
			}
		}
		counts = append(counts, len(trace))
	}
	if counts[0] != counts[1] {
		t.Fatalf("requests %v differ with database size", counts)
	}
}

func TestTakeoverLimits(t *testing.T) {
	t.Run("epoch", func(t *testing.T) {
		e := newEnv(t, 5)
		m, mv := e.h.manifest()
		m.Epoch = math.MaxUint64
		e.h.replace(e.keys.Manifest(), mv, e.h.encodeManifest(m))
		e.g.takeTrace()
		if _, _, err := Takeover(bg, e.g, e.keys, e.cand("b")); !errors.Is(err, ErrExhausted) {
			t.Fatalf("takeover at the maximum epoch: %v", err)
		}
		for _, c := range e.g.takeTrace() {
			if !strings.HasPrefix(c, "get ") {
				t.Fatalf("takeover at the maximum epoch wrote: %s", c)
			}
		}
	})
	t.Run("transaction", func(t *testing.T) {
		st, keys := newMemStore(t)
		g := &gated{Store: st}
		h := newHistory(t, st, keys, 6, 512, 4)
		m, mv := h.manifest()
		m.Txn, m.RetiredThrough = math.MaxUint64-1, 0
		m.DBSize, m.Groups = 1, make([]format.GroupRef, 1)
		h.replace(keys.Manifest(), mv, h.encodeManifest(m))
		b, _ := h.encodeCommit(math.MaxUint64, h.db, 1, 1, map[uint32][]byte{1: h.bytes(512)}, false)
		h.create(keys.Commit(h.db, math.MaxUint64), b)
		if _, _, err := Takeover(bg, g, keys, Candidate{Holder: "a", TTL: time.Minute, Now: newClock(0).now}); !errors.Is(err, ErrExhausted) {
			t.Fatalf("takeover with the head at the maximum: %v", err)
		}
		for _, c := range g.takeTrace() {
			if strings.HasPrefix(c, "create "+keys.CommitPrefix(h.db)) {
				t.Fatalf("bump attempted past the maximum: %s", c)
			}
		}
	})
}

// TestTakeoverRejectsBadLog: a candidate's walk rejects what a reader
// rejects, and never bumps after it.
func TestTakeoverRejectsBadLog(t *testing.T) {
	cases := map[string]func(e *env) []byte{
		"garbage": func(*env) []byte { return bytes.Repeat([]byte{1}, 200) },
		"another database": func(e *env) []byte {
			b, _ := e.h.encodeCommit(7, format.DatabaseID{3}, e.l.o.Epoch, 8, map[uint32][]byte{1: make([]byte, 512)}, false)
			return b
		},
		"another name": func(e *env) []byte {
			b, _ := e.h.encodeCommit(8, e.h.db, e.l.o.Epoch, 8, map[uint32][]byte{1: make([]byte, 512)}, false)
			return b
		},
		"bad checksum": func(e *env) []byte {
			b, _ := e.h.encodeCommit(7, e.h.db, e.l.o.Epoch, 8, map[uint32][]byte{1: make([]byte, 512)}, false)
			b[30] ^= 1
			return b
		},
		"bad record": func(e *env) []byte {
			b, _ := e.h.encodeCommit(7, e.h.db, e.l.o.Epoch, 8, map[uint32][]byte{1: make([]byte, 512)}, false)
			b[format.CommitHeaderSize+1] ^= 1 // the page table
			return b
		},
		"retired above the manifest": func(e *env) []byte {
			_, h := e.h.encodeCommit(7, e.h.db, e.l.o.Epoch, 8, map[uint32][]byte{1: make([]byte, 512)}, false)
			b, err := h.Retire()
			if err != nil {
				e.t.Fatal(err)
			}
			return b
		},
		"bump of the wrong size": func(e *env) []byte {
			b, _ := e.h.encodeCommit(7, e.h.db, e.l.o.Epoch+5, 3, nil, true)
			return b
		},
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 7)
			e.h.create(e.keys.Commit(e.h.db, 7), bad(e))
			o, l, err := Takeover(bg, e.g, e.keys, e.cand("b"))
			if errors.Is(err, ErrLost) || err == nil || o != (Office{}) || l != nil {
				t.Fatalf("takeover over a bad commit: %+v, %v", o, err)
			}
			for _, c := range e.g.takeTrace() {
				if strings.HasPrefix(c, "create "+e.keys.CommitPrefix(e.h.db)) {
					t.Fatalf("bumped after a bad commit: %s", c)
				}
			}
		})
	}
}

// TestLostResponseSwap: the epoch swap's response is lost, with the write
// landed and not landed. Both resolve to office.
func TestLostResponseSwap(t *testing.T) {
	for _, act := range []action{drop, late} {
		e := newEnv(t, 8)
		c := e.g.candidate(t, e.keys, e.cand("b"))
		if c.until("replace", e.keys.Manifest()) == nil {
			t.Fatal("no swap")
		}
		c.step(act)
		if c.until("replace", e.keys.Manifest()) == nil {
			t.Fatal("swap not repeated after an unknown outcome")
		}
		c.finish()
		if c.err != nil {
			t.Fatalf("action %d: %v", act, c.err)
		}
		e.g.landAll() // the late swap, conditioned on the old version, fails
		e.h.sync()
		e.check([]Office{e.l.o, c.o}, nil, true)
		if !c.o.Holds(manifestOf(t, e)) {
			t.Fatalf("action %d: manifest not the candidate's", act)
		}
	}
}

// TestLostResponseBump: the bump's response is lost, landed and not.
func TestLostResponseBump(t *testing.T) {
	for _, act := range []action{drop, late} {
		e := newEnv(t, 9)
		c := e.g.candidate(t, e.keys, e.cand("b"))
		key := e.keys.Commit(e.h.db, 7)
		if c.until("create", key) == nil {
			t.Fatal("no bump")
		}
		c.step(act)
		if c.until("create", key) == nil {
			t.Fatal("bump not repeated at the same name after an unknown outcome")
		}
		c.finish()
		if c.err != nil || c.o.Head != 7 {
			t.Fatalf("action %d: %+v %v", act, c.o, c.err)
		}
		e.g.landAll()
		e.h.sync()
		e.check([]Office{e.l.o, c.o}, nil, true)
	}
}

// TestLostResponseLease: the lease write's response is lost, landed and
// not, at acquisition, renewal and release.
func TestLostResponseLease(t *testing.T) {
	for _, act := range []action{drop, late} {
		e := newEnv(t, 10)
		key := e.keys.Lease()
		c := e.g.candidate(t, e.keys, e.cand("b"))
		if c.until("replace", key) == nil {
			t.Fatal("no lease write")
		}
		c.step(act)
		c.finish()
		if c.err != nil {
			t.Fatalf("acquire, action %d: %v", act, c.err)
		}
		e.g.landAll()
		for _, op := range []func(*Lease, context.Context) error{(*Lease).Renew, (*Lease).Release} {
			a := e.g.spawn(t, func(ctx context.Context) {
				if err := op(c.l, ctx); err != nil {
					t.Errorf("action %d: %v", act, err)
				}
			})
			if a.until("replace", key) == nil {
				t.Fatal("no lease write")
			}
			a.step(act)
			a.finish()
			e.g.landAll()
		}
		b, _ := get(bg, e.g, key)
		if l, err := format.DecodeLease(b); err != nil || l.Holder != "b" || l.Renewed != 0 {
			t.Fatalf("lease after release: %+v %v", l, err)
		}
	}
}

// TestUnresolvedSwap: every attempt at the swap is unknown or has no
// effect, so the candidate never serves; the swap lands later, which leaves
// the manifest at an epoch nobody serves, and the leader below it leaves
// office at its next compaction. A further takeover restores service.
func TestUnresolvedSwap(t *testing.T) {
	e := newEnv(t, 11)
	c := e.g.candidate(t, e.keys, e.cand("b"))
	acts := []action{late, fail, fail, fail, fail}
	for _, act := range acts {
		if c.until("replace", e.keys.Manifest()) == nil {
			t.Fatal("swap not repeated")
		}
		c.step(act)
	}
	c.finish()
	if !errors.Is(c.err, storage.ErrOutcomeUnknown) {
		t.Fatalf("unresolved swap: %v", c.err)
	}
	if ok, err := e.g.land(); !ok || err != nil {
		t.Fatalf("late swap did not land: %v", err)
	}
	if out := e.l.commit(bg); out != committed {
		t.Fatalf("the old leader commits until it compacts: %v", out)
	}
	e.h.sync()
	if e.l.compact(bg) {
		t.Fatal("the old leader stayed in office after reading a higher epoch")
	}
	d, _ := mustTakeover(t, e, "d")
	if d.Epoch != e.l.o.Epoch+2 {
		t.Fatalf("epoch %d after an unserved epoch", d.Epoch)
	}
	e.check([]Office{e.l.o, d}, []*leader{e.l}, true)
}

// TestUnresolvedBump: the first bump attempt is unknown and the rest have
// no effect: the candidate gives up and never serves. The bump lands later
// and fences the leader at the head; the next takeover walks past it.
func TestUnresolvedBump(t *testing.T) {
	e := newEnv(t, 12)
	c := e.g.candidate(t, e.keys, e.cand("b"))
	key := e.keys.Commit(e.h.db, 7)
	for _, act := range []action{late, fail, fail, fail, fail} {
		if c.until("create", key) == nil {
			t.Fatal("bump not repeated at the same name")
		}
		c.step(act)
	}
	c.finish()
	if !errors.Is(c.err, storage.ErrOutcomeUnknown) {
		t.Fatalf("unresolved bump: %v", c.err)
	}
	if ok, err := e.g.land(); !ok || err != nil {
		t.Fatalf("late bump did not land: %v", err)
	}
	if out := e.l.commit(bg); out != notCommitted {
		t.Fatalf("leader not fenced by the late bump: %v", out)
	}
	e.h.sync()
	d, _ := mustTakeover(t, e, "d")
	if d.Head != 8 {
		t.Fatalf("next takeover bumped at %d", d.Head)
	}
	e.check([]Office{e.l.o, d}, []*leader{e.l}, true)
}

// TestStickyBump: an unknown bump attempt followed by attempts with no
// effect is never resolved as failed; a precondition failure that finds
// another object at the name resolves it, and the candidate walks on.
func TestStickyBump(t *testing.T) {
	e := newEnv(t, 13)
	c := e.g.candidate(t, e.keys, e.cand("b"))
	key := e.keys.Commit(e.h.db, 7)
	c.until("create", key)
	c.step(late)
	c.until("create", key)
	c.step(fail)
	c.until("create", key)
	if out := e.l.commit(bg); out != committed { // the old leader ignores the lease
		t.Fatal(out)
	}
	c.finish()
	if c.err != nil || c.o.Head != 8 {
		t.Fatalf("%+v %v", c.o, c.err)
	}
	if ok, err := e.g.land(); !ok || !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("late bump at an occupied name: %v", err)
	}
	e.h.sync()
	e.check([]Office{e.l.o, c.o}, []*leader{e.l}, true)
}

// TestStickySwap: an unknown swap attempt, then the manifest is replaced by
// another candidacy: the version is gone for good, so the candidate loses.
func TestStickySwap(t *testing.T) {
	e := newEnv(t, 14)
	c := e.g.candidate(t, e.keys, e.cand("b"))
	c.until("replace", e.keys.Manifest())
	c.step(late)
	c.until("replace", e.keys.Manifest())
	d, _ := mustTakeover(t, e, "d")
	c.finish()
	if !errors.Is(c.err, ErrLost) {
		t.Fatalf("candidate after its swap's version was replaced: %v", c.err)
	}
	if ok, err := e.g.land(); !ok || !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("late swap: %v", err)
	}
	e.check([]Office{e.l.o, d}, nil, true)
}

// TestSwapBothUnknown: two candidates read the same manifest and build
// swaps identical apart from the nonce; both see an unknown outcome, and
// only the one whose swap landed proceeds.
func TestSwapBothUnknown(t *testing.T) {
	e := newEnv(t, 15)
	a := e.g.candidate(t, e.keys, e.cand("a"))
	b := e.g.candidate(t, e.keys, e.cand("b"))
	a.until("replace", e.keys.Manifest())
	b.until("replace", e.keys.Manifest())
	a.step(drop)
	b.step(drop) // its condition fails inside the store; it sees unknown too
	a.finish()
	b.finish()
	if a.err != nil || !errors.Is(b.err, ErrLost) {
		t.Fatalf("a: %v, b: %v", a.err, b.err)
	}
	e.h.sync()
	e.check(offices(t, a, b), nil, true)
}

func TestLease(t *testing.T) {
	e := newEnv(t, 16)
	clk := newClock(0)
	at := func(holder string) Candidate { return Candidate{Holder: holder, TTL: time.Minute, Now: clk.now} }
	clk.advance(1000 * time.Hour) // the setup's lease, stamped by another clock, is long expired
	a, la, err := Takeover(bg, e.g, e.keys, at("a"))
	if err != nil {
		t.Fatal(err)
	}
	e.h.sync()
	e.g.takeTrace()
	if _, _, err := Takeover(bg, e.g, e.keys, at("b")); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("takeover under a live lease: %v", err)
	}
	for _, c := range e.g.takeTrace() {
		if !strings.HasPrefix(c, "get ") {
			t.Fatalf("a candidate refused the lease wrote: %s", c)
		}
	}
	clk.advance(50 * time.Second)
	if err := la.Renew(bg); err != nil {
		t.Fatal(err)
	}
	clk.advance(50 * time.Second) // 100s after acquisition, 50s after renewal
	if _, _, err := Takeover(bg, e.g, e.keys, at("b")); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("takeover under a renewed lease: %v", err)
	}
	// The same holder takes its own lease back without waiting.
	a2, la2, err := Takeover(bg, e.g, e.keys, at("a"))
	if err != nil || a2.Epoch != a.Epoch+1 {
		t.Fatalf("own holder's next candidacy: %+v %v", a2, err)
	}
	if err := la.Renew(bg); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("renewal of a replaced lease: %v", err)
	}
	if err := la2.Release(bg); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Takeover(bg, e.g, e.keys, at("b")); err != nil {
		t.Fatalf("takeover after a release: %v", err)
	}
	if err := la2.Release(bg); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("release of a taken lease: %v", err)
	}
	if _, _, err := Takeover(bg, e.g, e.keys, Candidate{Holder: "c", Now: clk.now}); err == nil {
		t.Fatal("a candidate without a time to live")
	}
}

// TestLeaseFailStop: a fail-stopped leader neither renews nor releases; its
// lease expires and only then does another candidate take over.
func TestLeaseFailStop(t *testing.T) {
	e := newEnv(t, 17)
	clk := newClock(0)
	clk.advance(1000 * time.Hour)
	at := func(holder string) Candidate { return Candidate{Holder: holder, TTL: time.Minute, Now: clk.now} }
	a, _, err := Takeover(bg, e.g, e.keys, at("a"))
	if err != nil {
		t.Fatal(err)
	}
	e.h.sync()
	la := newLeader(e.h, e.g, a)
	e.h.create(e.keys.Commit(e.h.db, a.Head+1), func() []byte { b, _ := e.h.encodeCommit(a.Head+1, e.h.db, a.Epoch+9, la.size, nil, true); return b }())
	if out := la.commit(bg); out != notCommitted || !la.done {
		t.Fatalf("fenced leader: %v", out)
	}
	e.g.takeTrace()
	clk.advance(59 * time.Second)
	if _, _, err := Takeover(bg, e.g, e.keys, at("b")); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("before expiry: %v", err)
	}
	clk.advance(time.Second)
	if _, _, err := Takeover(bg, e.g, e.keys, at("b")); err != nil {
		t.Fatalf("after expiry: %v", err)
	}
}

func TestLeaseUnreadable(t *testing.T) {
	for name, body := range map[string]string{
		"garbage":        "not a lease",
		"newer version":  `{"format":"skylite-lease","version":2,"anything":true}`,
		"non-canonical":  `{"format":"skylite-lease","version":1,"holder":"x", "epoch":1,"renewed":0}`,
		"invalid holder": `{"format":"skylite-lease","version":1,"holder":"a b","epoch":1,"renewed":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 18)
			info, err := e.g.Head(bg, e.keys.Lease())
			if err != nil {
				t.Fatal(err)
			}
			e.h.replace(e.keys.Lease(), info.Version, []byte(body))
			e.g.takeTrace()
			if _, _, err := Takeover(bg, e.g, e.keys, e.cand("b")); err == nil {
				t.Fatal("took an unreadable lease")
			}
			for _, c := range e.g.takeTrace() {
				if !strings.HasPrefix(c, "get ") {
					t.Fatalf("wrote over an unreadable lease: %s", c)
				}
			}
		})
	}
}

// TestLeaseRace: two candidates find the lease expired and replace it from
// the same version; one wins and the other stops before the manifest.
func TestLeaseRace(t *testing.T) {
	e := newEnv(t, 19)
	a := e.g.candidate(t, e.keys, e.cand("a"))
	b := e.g.candidate(t, e.keys, e.cand("b"))
	a.until("replace", e.keys.Lease())
	b.until("replace", e.keys.Lease())
	a.finish()
	b.finish()
	if a.err != nil || !errors.Is(b.err, ErrLeaseHeld) {
		t.Fatalf("a: %v, b: %v", a.err, b.err)
	}
	e.h.sync()
	e.check(offices(t, a, b), nil, true)
}
