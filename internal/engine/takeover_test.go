package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
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
	if b, err := get(bg, e.g, e.keys.Lease(), 1<<40); err != nil {
		t.Fatal(err)
	} else if ls, err := format.DecodeLease(b); err != nil || ls.Holder != "b" || ls.Epoch != o.Epoch {
		t.Fatalf("lease %+v, %v; want holder b at epoch %d", ls, err, o.Epoch)
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

// TestTakeoverCompacted takes over a database whose base has several groups:
// the epoch swap writes every group reference back unchanged, a reader reads
// the base through the swapped manifest, and the new leader commits.
func TestTakeoverCompacted(t *testing.T) {
	st, keys := newMemStore(t)
	h := newHistory(t, st, keys, 11, 512, 4)
	pages := map[uint32][]byte{}
	for p := range uint32(16) {
		pages[p+1] = h.bytes(512)
	}
	h.commit(16, pages)
	h.commit(16, map[uint32][]byte{6: h.bytes(512)})
	h.compact(h.head())
	h.commit(16, map[uint32][]byte{11: h.bytes(512)})
	before, _ := h.manifest()
	if len(before.Groups) != 4 {
		t.Fatalf("fixture has %d groups, want 4", len(before.Groups))
	}
	g := &gated{Store: st, manifest: keys.Manifest()}
	e := &env{t: t, g: g, keys: keys, h: h, inv: newInvariants(st, keys, h.db), clk: newClock(0)}
	e.inv.g = g
	o, _ := mustTakeover(t, e, "c")
	m := manifestOf(t, e)
	if !o.Holds(m) || m.Txn != before.Txn || !slices.Equal(m.Groups, before.Groups) {
		t.Fatalf("manifest after swap %+v, before %+v", m, before)
	}
	r, err := Open(bg, st, keys, nil, o.Head-1)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.checkReader(bg, r, false); err != nil {
		t.Fatal(err)
	}
	nl := newLeader(h, g, o)
	if out := nl.commit(bg); out != committed {
		t.Fatalf("new leader's commit: %v", out)
	}
	e.check([]Office{o}, []*leader{nl}, true)
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
		{"own retired with trailing bytes", append(bytes.Clone(retired), 0), FoundUnknown},
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
	if got, _ := (Office{DB: o.DB, Epoch: o.Epoch, Head: bh.Txn, Nonce: o.Nonce}).Recognise(built, built); got != FoundUnknown {
		t.Errorf("a commit at the office's bump recognised: %v", got)
	}
}

// TestRecogniseProperty: random corruptions of the objects that can be at a
// leader's next name are FoundOwn only as built or its retired form, and
// FoundBump only as a valid bump of this database at this name at another
// epoch.
func TestRecogniseProperty(t *testing.T) {
	e := newEnv(t, 110)
	o := e.l.o
	built, bh := e.l.build()
	retired, err := bh.Retire()
	if err != nil {
		t.Fatal(err)
	}
	e.h.mu.Lock()
	bump, _ := e.h.encodeCommit(bh.Txn, o.DB, o.Epoch+1, e.l.size, nil, true)
	lowBump, _ := e.h.encodeCommit(bh.Txn, o.DB, o.Epoch-1, e.l.size, nil, true)
	e.h.mu.Unlock()
	rng := seeded(7)
	bases := [][]byte{built, retired, bump, lowBump}
	for i := range 200000 {
		x := bytes.Clone(bases[rng.IntN(len(bases))])
		switch rng.IntN(4) {
		case 0:
			x[rng.IntN(len(x))] ^= byte(1 << rng.IntN(8))
		case 1:
			x = x[:rng.IntN(len(x)+1)]
		case 2:
			x = append(x, byte(rng.IntN(256))) //nolint:gosec // below 256
		case 3:
			j, k := rng.IntN(len(x)), rng.IntN(len(x))
			x[j], x[k] = x[k], x[j]
		}
		want := FoundUnknown
		h, herr := format.DecodeCommitHeader(x[:min(len(x), format.CommitHeaderSize)], o.DB)
		switch {
		case bytes.Equal(x, built), bytes.Equal(x, retired):
			want = FoundOwn
		case herr == nil && len(x) == format.CommitHeaderSize && h.EpochBump && h.Txn == bh.Txn && h.Epoch != o.Epoch:
			want = FoundBump
		}
		if got, err := o.Recognise(built, x); got != want || (got == FoundUnknown) != (err != nil) {
			t.Fatalf("corruption %d of %d bytes: %v, %v; want %v", i, len(x), got, err, want)
		}
	}
}

// TestRecogniseShortFound: a found object of every length up to one byte
// past a header, with no capacity beyond its length, is FoundUnknown with
// an error, never a panic.
func TestRecogniseShortFound(t *testing.T) {
	e := newEnv(t, 322)
	built, _ := e.l.build()
	for n := range format.CommitHeaderSize + 2 {
		found := make([]byte, n)
		copy(found, built)
		if got, err := e.l.o.Recognise(built, found); got != FoundUnknown || err == nil {
			t.Fatalf("%d bytes found: %v, %v", n, got, err)
		}
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
	got, err := get(bg, e.g, e.keys.Commit(e.h.db, 7), 1<<40)
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
		"commit at the candidate's epoch": func(e *env) []byte {
			b, _ := e.h.encodeCommit(7, e.h.db, e.l.o.Epoch+1, e.l.size, nil, true)
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
		b, _ := get(bg, e.g, key, 1<<40)
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
	d, dl := mustTakeover(t, e, "d")
	c.finish()
	if !errors.Is(c.err, ErrLost) {
		t.Fatalf("candidate after its swap's version was replaced: %v", c.err)
	}
	if ok, err := e.g.land(); !ok || !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("late swap: %v", err)
	}
	if b, _ := get(bg, e.g, e.keys.Lease(), 1<<40); !bytes.Equal(b, leaseBytes(t, dl)) {
		t.Fatal("the losing candidate's release changed the winner's lease")
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
	_, _, err = Takeover(bg, e.g, e.keys, at("b"))
	var held *LeaseHeldError
	if !errors.As(err, &held) || !errors.Is(err, ErrLeaseHeld) || held.Holder != "a" || !held.Expires.Equal(clk.now().Add(time.Minute)) {
		t.Fatalf("takeover under a live lease: %v", err)
	}
	for _, c := range e.g.takeTrace() {
		if !strings.HasPrefix(c, "get ") {
			t.Fatalf("a candidate refused the lease wrote: %s", c)
		}
	}
	for range 2 { // the clock stands still, and every write still has bytes of its own
		prev := la.l.Renewed
		if err := la.Renew(bg); err != nil || la.l.Renewed != prev+1 {
			t.Fatalf("renewal at the same clock reading: %d after %d, %v", la.l.Renewed, prev, err)
		}
		if tr := e.g.takeTrace(); len(tr) != 1 || tr[0] != "replace "+e.keys.Lease() {
			t.Fatalf("an uncontended renewal made %v", tr)
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

// TestLeaseUnreadable: a lease object that is not a lease of any version is
// replaced like an expired one; a lease of a newer version is refused and
// never overwritten.
func TestLeaseUnreadable(t *testing.T) {
	for name, c := range map[string]struct {
		body     string
		replaced bool
	}{
		"garbage":        {"not a lease", true},
		"newer version":  {`{"format":"skylite-lease","version":2,"anything":true}`, false},
		"non-canonical":  {`{"format":"skylite-lease","version":1,"holder":"x", "epoch":1,"renewed":0}`, true},
		"invalid holder": {`{"format":"skylite-lease","version":1,"holder":"a b","epoch":1,"renewed":0}`, true},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 18)
			info, err := e.g.Head(bg, e.keys.Lease())
			if err != nil {
				t.Fatal(err)
			}
			e.h.replace(e.keys.Lease(), info.Version, []byte(c.body))
			e.g.takeTrace()
			o, l, err := Takeover(bg, e.g, e.keys, e.cand("b"))
			if c.replaced {
				if err != nil || !bytes.Equal(must(get(bg, e.g, e.keys.Lease(), 1<<40)), leaseBytes(t, l)) {
					t.Fatalf("takeover over an undecodable lease: %+v %v", o, err)
				}
				return
			}
			if err == nil || errors.Is(err, ErrLeaseHeld) {
				t.Fatalf("takeover over a newer lease: %v", err)
			}
			for _, c := range e.g.takeTrace() {
				if !strings.HasPrefix(c, "get ") {
					t.Fatalf("wrote over a newer lease: %s", c)
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

func leaseBytes(t testing.TB, l *Lease) []byte {
	t.Helper()
	b, err := format.EncodeLease(l.l)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

// TestLeaseClockAhead: a lease renewed more than its time to live ahead of
// a node's clock is expired for that node; one exactly that far ahead is
// not.
func TestLeaseClockAhead(t *testing.T) {
	for _, c := range []struct {
		ahead time.Duration
		held  bool
	}{
		{time.Minute, true},
		{time.Minute + time.Millisecond, false},
		{100 * 365 * 24 * time.Hour, false},
	} {
		e := newEnv(t, 104)
		clk := newClock(0)
		clk.advance(1000 * time.Hour)
		ahead := newClock(0)
		ahead.advance(1000*time.Hour + c.ahead)
		if _, _, err := Takeover(bg, e.g, e.keys, Candidate{Holder: "a", TTL: time.Minute, Now: ahead.now}); err != nil {
			t.Fatal(err)
		}
		e.h.sync()
		_, _, err := Takeover(bg, e.g, e.keys, Candidate{Holder: "b", TTL: time.Minute, Now: clk.now})
		if errors.Is(err, ErrLeaseHeld) != c.held || (!c.held && err != nil) {
			t.Fatalf("lease %v ahead: %v", c.ahead, err)
		}
	}
}

// TestLeaseOwnUnresolvedWrite: a renewal or release that follows a renewal
// whose outcome stayed unknown, and landed, finds the holder's own earlier
// write and succeeds.
func TestLeaseOwnUnresolvedWrite(t *testing.T) {
	for _, release := range []bool{false, true} {
		e := newEnv(t, 103)
		clk := newClock(time.Millisecond)
		clk.advance(1000 * time.Hour)
		_, l, err := Takeover(bg, e.g, e.keys, Candidate{Holder: "a", TTL: time.Minute, Now: clk.now})
		if err != nil {
			t.Fatal(err)
		}
		key := e.keys.Lease()
		a := e.g.spawn(t, func(ctx context.Context) { err = l.Renew(ctx) })
		for _, act := range []action{drop, fail, fail, fail, fail} {
			if a.until("replace", key) == nil {
				t.Fatal("renewal not repeated")
			}
			a.step(act)
		}
		a.finish()
		if !errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Fatalf("unresolved renewal: %v", err)
		}
		op := l.Renew
		if release {
			op = l.Release
		}
		if err := op(bg); err != nil {
			t.Fatalf("release %v after an unresolved renewal: %v", release, err)
		}
		if ls, err := format.DecodeLease(must(get(bg, e.g, key, 1<<40))); err != nil || ls != l.l || (ls.Renewed == 0) != release {
			t.Fatalf("lease %+v, %v; the handle has %+v", ls, err, l.l)
		}
	}
}

// TestLeaseRenewedDuringWalk: a candidate renews its lease once a third of
// its time to live has passed since its last lease write.
func TestLeaseRenewedDuringWalk(t *testing.T) {
	setup := func(t *testing.T) (*env, Candidate) {
		e := newEnv(t, 105)
		for range 30 {
			if out := e.l.commit(bg); out != committed {
				t.Fatal(out)
			}
		}
		e.h.sync()
		e.g.takeTrace()
		clk := newClock(5 * time.Second) // each reading is 5s after the last
		clk.advance(1000 * time.Hour)
		return e, Candidate{Holder: "b", TTL: time.Minute, Now: clk.now}
	}
	t.Run("renewed", func(t *testing.T) {
		e, c := setup(t)
		if _, _, err := Takeover(bg, e.g, e.keys, c); err != nil {
			t.Fatal(err)
		}
		writes, gap, maxGap, last := 0, 0, 0, ""
		for _, call := range e.g.takeTrace() {
			switch op, key, _ := strings.Cut(call, " "); {
			case key == e.keys.Lease() && op != "get":
				writes, gap = writes+1, 0
			case op == "getrange" && strings.HasPrefix(key, e.keys.CommitPrefix(e.h.db)) && key != last:
				last, gap = key, gap+1
				maxGap = max(maxGap, gap)
			}
		}
		// The walk reads the clock once per commit, and a renewal reads it
		// once more: 20s pass every four commits.
		if writes < 8 || maxGap > 4 {
			t.Fatalf("%d lease writes, up to %d commits read between them", writes, maxGap)
		}
	})
}

// TestLeaseTakenByLoserKeepsWinner: x, whose clock is two hours ahead, takes
// w's lease before w's swap lands, loses its own swap and releases the
// lease. w's walk then finds the lease taken when it renews, and takes
// office anyway, because it still holds the manifest.
func TestLeaseTakenByLoserKeepsWinner(t *testing.T) {
	e := newEnv(t, 303)
	for range 30 {
		if out := e.l.commit(bg); out != committed {
			t.Fatal(out)
		}
	}
	e.h.sync()
	wc := newClock(5 * time.Second) // each reading is 5s after the last
	wc.advance(1000 * time.Hour)
	xc := newClock(0)
	xc.advance(1002 * time.Hour)
	w := e.g.candidate(t, e.keys, Candidate{Holder: "w", TTL: time.Minute, Now: wc.now})
	if w.until("replace", e.keys.Manifest()) == nil {
		t.Fatal("w never swaps")
	}
	x := e.g.candidate(t, e.keys, Candidate{Holder: "x", TTL: time.Minute, Now: xc.now})
	if x.until("replace", e.keys.Manifest()) == nil {
		t.Fatal("x never swaps")
	}
	w.step(pass) // w's swap lands
	x.finish()
	if ls, err := format.DecodeLease(must(get(bg, e.g, e.keys.Lease(), 1<<40))); !errors.Is(x.err, ErrLost) || err != nil || ls.Holder != "x" || ls.Renewed != 0 {
		t.Fatalf("x: %v; lease %+v, %v; want x's lease released", x.err, ls, err)
	}
	e.g.takeTrace()
	w.finish()
	if w.err != nil {
		t.Fatalf("w, which holds the manifest at its epoch, gave up: %v", w.err)
	}
	if !slices.Contains(e.g.takeTrace(), "replace "+e.keys.Lease()) {
		t.Fatal("w's walk never tried to renew the lease")
	}
	if ls, err := format.DecodeLease(must(get(bg, e.g, e.keys.Lease(), 1<<40))); err != nil || ls.Holder != "w" || ls.Epoch != w.o.Epoch {
		t.Fatalf("lease %+v, %v; want w's at epoch %d", ls, err, w.o.Epoch)
	}
	if err := w.l.Renew(bg); err != nil {
		t.Fatalf("w cannot renew the lease of its office: %v", err)
	}
	e.check(append(offices(t, w, x), e.l.o), []*leader{e.l}, true)
}

// TestFailedTakeBackKeepsWinner: x takes w's lease, which is a time to live
// old by x's clock, and is still a candidate with an unexpired lease when w,
// which won the swap, finishes. w's take-back fails, and w takes office
// anyway.
func TestFailedTakeBackKeepsWinner(t *testing.T) {
	e := newEnv(t, 305)
	wc := newClock(7 * time.Second)
	wc.advance(1000 * time.Hour)
	xc := newClock(0)
	xc.advance(1000*time.Hour + 75*time.Second)
	w := e.g.candidate(t, e.keys, Candidate{Holder: "w", TTL: time.Minute, Now: wc.now})
	if w.until("replace", e.keys.Manifest()) == nil {
		t.Fatal("w never swaps")
	}
	x := e.g.candidate(t, e.keys, Candidate{Holder: "x", TTL: time.Minute, Now: xc.now})
	if x.until("replace", e.keys.Manifest()) == nil {
		t.Fatalf("x never swaps: %v", x.err)
	}
	w.finish()
	if w.err != nil {
		t.Fatalf("w, which holds the manifest at its epoch, gave up: %v", w.err)
	}
	if err := w.l.Renew(bg); !errors.Is(err, ErrLeaseHeld) || !w.l.taken {
		t.Fatalf("Renew: %v, taken %v; want the take-back to have failed", err, w.l.taken)
	}
}

// TestFailedRenewalTriedOncePerThird: a renewal that fails is tried again
// only once a third of the lease's time to live has passed since the last
// try, not at every step of the walk, and its failure does not end the
// takeover.
func TestFailedRenewalTriedOncePerThird(t *testing.T) {
	e := newEnv(t, 304)
	for range 30 {
		if out := e.l.commit(bg); out != committed {
			t.Fatal(out)
		}
	}
	e.h.sync()
	clk := newClock(5 * time.Second)
	clk.advance(1000 * time.Hour)
	c := Candidate{Holder: "w", TTL: time.Minute, Now: clk.now}
	k := e.g.candidate(t, e.keys, c)
	if k.until("getrange", e.keys.Commit(e.h.db, 8)) == nil {
		t.Fatal("no walk")
	}
	var tries []time.Time // the clock at the first write of each try
	writes, inTry := 0, false
	for call := k.next(); call != nil; call = k.next() {
		if call.op != "replace" || call.key != e.keys.Lease() {
			inTry = false
			k.step(pass)
			continue
		}
		if !inTry {
			clk.mu.Lock()
			tries = append(tries, clk.t)
			clk.mu.Unlock()
		}
		writes, inTry = writes+1, true
		k.step(fail) // every lease write from here on fails with no effect
	}
	if k.err != nil {
		t.Fatalf("takeover with every renewal failing: %v", k.err)
	}
	if len(tries) < 3 || writes > createAttempts*len(tries) {
		t.Fatalf("%d lease writes in %d renewal tries", writes, len(tries))
	}
	for i := 1; i < len(tries); i++ {
		if d := tries[i].Sub(tries[i-1]); d < c.TTL/3 {
			t.Fatalf("renewal try %d came %v after the one before", i, d)
		}
	}
	e.check(append(offices(t, k), e.l.o), []*leader{e.l}, true)
}

// TestLeaseReadBound: a candidate reads at most MaxLeaseSize+1 bytes of an
// oversized lease object, and replaces it like one that does not decode.
func TestLeaseReadBound(t *testing.T) {
	st, keys := newMemStore(t)
	gc := &getCounter{Store: st, key: keys.Lease()}
	e := newEnvOn(t, gc, keys, 301)
	info, err := e.g.Head(bg, keys.Lease())
	if err != nil {
		t.Fatal(err)
	}
	e.h.replace(keys.Lease(), info.Version, bytes.Repeat([]byte{'x'}, 4<<20))
	gc.n = 0
	if _, _, err := Takeover(bg, e.g, keys, e.cand("b")); err != nil {
		t.Fatal(err)
	}
	if gc.n > int64(format.MaxLeaseSize+1) {
		t.Fatalf("read %d bytes of the lease", gc.n)
	}
}

// TestLeaseReadBackBound: a candidate whose lease replace fails because an
// oversized object replaced the lease first reads at most MaxLeaseSize+1
// bytes of it back, and gives up.
func TestLeaseReadBackBound(t *testing.T) {
	st, keys := newMemStore(t)
	gc := &getCounter{Store: st, key: keys.Lease()}
	e := newEnvOn(t, gc, keys, 302)
	k := e.g.candidate(t, keys, e.cand("b"))
	if k.until("replace", keys.Lease()) == nil {
		t.Fatal("no lease replace")
	}
	info, err := e.g.Head(bg, keys.Lease())
	if err != nil {
		t.Fatal(err)
	}
	e.h.replace(keys.Lease(), info.Version, bytes.Repeat([]byte{'x'}, 4<<20))
	gc.mu.Lock()
	gc.n = 0
	gc.mu.Unlock()
	k.finish()
	if !errors.Is(k.err, ErrLeaseHeld) || gc.n > int64(format.MaxLeaseSize+1) {
		t.Fatalf("read %d bytes of the lease back: %v", gc.n, k.err)
	}
}

// TestLeaseHeldErrorIs: a LeaseHeldError matches ErrLeaseHeld through a
// wrap and no other error, and errors.As recovers it.
func TestLeaseHeldErrorIs(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &LeaseHeldError{Holder: "x"})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatal("does not match ErrLeaseHeld")
	}
	for _, target := range []error{ErrLost, ErrCorrupt, ErrExhausted, storage.ErrOutcomeUnknown, storage.ErrNotFound, storage.ErrPreconditionFailed, context.Canceled, errors.New(ErrLeaseHeld.Error())} {
		if errors.Is(err, target) {
			t.Errorf("matches %v", target)
		}
	}
	var held *LeaseHeldError
	if !errors.As(err, &held) || held.Holder != "x" {
		t.Fatalf("errors.As: %+v", held)
	}
}

// TestLeaseHeldErrorMessages: a released lease may be taken now, not at the
// time to live after time 0, and a lease taken by another candidacy of the
// same holder is not reported as another holder's.
func TestLeaseHeldErrorMessages(t *testing.T) {
	st, keys := newMemStore(t)
	wc, xc := newClock(time.Second), newClock(0)
	xc.advance(2 * time.Hour)
	w, err := acquireLease(bg, st, keys, Candidate{Holder: "w", TTL: time.Minute, Now: wc.now}, 2)
	if err != nil {
		t.Fatal(err)
	}
	x, err := acquireLease(bg, st, keys, Candidate{Holder: "x", TTL: time.Minute, Now: xc.now}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Release(bg); err != nil {
		t.Fatal(err)
	}
	var held *LeaseHeldError
	err = w.Renew(bg)
	wc.mu.Lock()
	now := wc.t
	wc.mu.Unlock()
	if !errors.As(err, &held) || held.Holder != "x" || !held.Expires.Equal(now) {
		t.Fatalf("renewing over a released lease: %v; want it free at %s", err, now)
	}
	a := Candidate{Holder: "a", TTL: time.Minute, Now: xc.now}
	a5, err := acquireLease(bg, st, keys, a, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireLease(bg, st, keys, a, 6); err != nil {
		t.Fatal(err)
	}
	err = a5.Renew(bg)
	if !errors.Is(err, ErrLeaseHeld) || strings.Contains(err.Error(), "another holder") || !strings.Contains(err.Error(), `"a"`) {
		t.Fatalf("renewing a lease the same holder's next candidacy took: %v", err)
	}
}

// TestTakeoverReleasesLease: a candidate whose swap definitely did not land
// releases its lease; one whose swap may still land leaves it to expire.
func TestTakeoverReleasesLease(t *testing.T) {
	for _, c := range []struct {
		acts     []action
		released bool
	}{
		{[]action{fail, fail, fail, fail, fail}, true},
		{[]action{late, fail, fail, fail, fail}, false},
	} {
		e := newEnv(t, 20)
		k := e.g.candidate(t, e.keys, e.cand("b"))
		for _, act := range c.acts {
			if k.until("replace", e.keys.Manifest()) == nil {
				t.Fatal("swap not repeated")
			}
			k.step(act)
		}
		k.finish()
		if k.err == nil || errors.Is(k.err, storage.ErrOutcomeUnknown) == c.released {
			t.Fatalf("swap %v: %v", c.acts, k.err)
		}
		ls, err := format.DecodeLease(must(get(bg, e.g, e.keys.Lease(), 1<<40)))
		if err != nil || ls.Holder != "b" || (ls.Renewed == 0) != c.released {
			t.Fatalf("swap %v: lease %+v %v", c.acts, ls, err)
		}
		e.g.landAll()
	}
}

// TestBeforeErrorKeepsUnknown: a bump attempt is unknown, and the re-read
// before the next attempt finds another candidacy's manifest. The error
// says both, because the first attempt can still land, and it does.
func TestBeforeErrorKeepsUnknown(t *testing.T) {
	e := newEnv(t, 102)
	key := e.keys.Commit(e.h.db, 7)
	c := e.g.candidate(t, e.keys, e.cand("c"))
	c.until("create", key)
	c.step(late)
	d := e.g.candidate(t, e.keys, e.cand("d"))
	d.until("create", key) // d has swapped the manifest and not bumped yet
	c.finish()
	if !errors.Is(c.err, storage.ErrOutcomeUnknown) || !errors.Is(c.err, ErrLost) {
		t.Fatalf("candidate whose re-read failed after an unknown bump: %v", c.err)
	}
	if ok, err := e.g.land(); !ok || err != nil {
		t.Fatalf("the late bump did not land: %v", err)
	}
	d.finish()
	e.h.sync()
	e.check(offices(t, c, d), nil, true)
}

// getCounter counts the body bytes read by Get from one key.
type getCounter struct {
	storage.Store
	key string

	mu sync.Mutex
	n  int64
}

func (s *getCounter) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	rc, info, err := s.Store.Get(ctx, key)
	if err != nil || key != s.key {
		return rc, info, err
	}
	return countedBody{rc, s}, info, nil
}

type countedBody struct {
	io.ReadCloser
	s *getCounter
}

func (r countedBody) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.s.mu.Lock()
	r.s.n += int64(n)
	r.s.mu.Unlock()
	return n, err
}

// TestLostBumpReadsLittle: a candidate whose bump loses to a 2 MB commit
// reads back at most one byte more than its bump.
func TestLostBumpReadsLittle(t *testing.T) {
	st, keys := newMemStore(t)
	gc := &getCounter{Store: st}
	e := newEnvOn(t, gc, keys, 101)
	key := e.keys.Commit(e.h.db, 7)
	gc.key = key
	c := e.g.candidate(t, e.keys, e.cand("c"))
	if c.until("create", key) == nil {
		t.Fatal("no bump")
	}
	pages := map[uint32][]byte{}
	for p := uint32(1); p <= 4000; p++ {
		pages[p] = e.h.bytes(512)
	}
	big, _ := e.h.encodeCommit(7, e.h.db, e.l.o.Epoch, 4000, pages, false)
	e.h.create(key, big) // the old leader's commit takes the name
	c.finish()
	if c.err != nil || c.o.Head != 8 {
		t.Fatalf("%+v %v", c.o, c.err)
	}
	if gc.n > format.CommitHeaderSize+1 {
		t.Fatalf("the candidate read %d bytes of the %d-byte commit that beat its bump", gc.n, len(big))
	}
}

// lagStore hides one key from ranged reads only, as a lagging replica
// would, and cancels the operation after too many creates.
type lagStore struct {
	storage.Store
	hide    string
	creates int
	cancel  func()
}

func (s *lagStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if key == s.hide {
		return nil, storage.Info{}, &storage.Error{Op: "getrange", Key: key, Err: storage.ErrNotFound}
	}
	return s.Store.GetRange(ctx, key, off, n)
}

func (s *lagStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if s.creates++; s.creates > 10 {
		s.cancel()
	}
	return s.Store.Create(ctx, key, body)
}

// TestTakeoverNoProgress: storage whose ranged reads do not see a name
// that creates do breaks decision 3. The candidate's walk then never
// reaches the name its bump lost to, and it gives up instead of trying the
// same name again.
func TestTakeoverNoProgress(t *testing.T) {
	e := newEnv(t, 108)
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	ls := &lagStore{Store: e.g.Store, hide: e.keys.Commit(e.h.db, 6), cancel: cancel}
	if _, _, err := Takeover(ctx, ls, e.keys, e.cand("x")); !errors.Is(err, ErrCorrupt) || ls.creates != 1 {
		t.Fatalf("%d creates: %v", ls.creates, err)
	}
}

// TestTakeoverEmptyLog: a database whose manifest is at 0 and whose first
// commit is missing has no history to bump after.
func TestTakeoverEmptyLog(t *testing.T) {
	st, keys := newMemStore(t)
	g := &gated{Store: st}
	h := newHistory(t, st, keys, 21, 512, 4)
	if err := st.Delete(bg, keys.Commit(h.db, 1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Takeover(bg, g, keys, Candidate{Holder: "a", TTL: time.Minute, Now: newClock(0).now}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeover of an empty log: %v", err)
	}
	for _, c := range g.takeTrace() {
		if strings.HasPrefix(c, "create "+keys.CommitPrefix(h.db)) {
			t.Fatalf("bumped an empty log: %s", c)
		}
	}
}

// TestAbsentAfterPreconditionFailure: a create that fails its precondition
// at a key a read then finds absent breaks decision 3.
func TestAbsentAfterPreconditionFailure(t *testing.T) {
	ts, keys := newStore(t)
	newHistory(t, ts, keys, 22, 512, 4)
	ts.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op == "create" && key == keys.Lease() {
			return false, &storage.Error{Op: op, Key: key, Err: storage.ErrPreconditionFailed}
		}
		return false, nil
	})
	if _, _, err := Takeover(bg, ts, keys, Candidate{Holder: "a", TTL: time.Minute, Now: newClock(0).now}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lease absent after a precondition failure: %v", err)
	}
}

// TestLostSwapReadsLittle: a candidate whose swap loses to a compaction that
// grew the manifest reads back at most one byte more than its swap, and
// releases its lease, which nobody has taken since.
func TestLostSwapReadsLittle(t *testing.T) {
	st, keys := newMemStore(t)
	gc := &getCounter{Store: st, key: keys.Manifest()}
	e := newEnvOn(t, gc, keys, 24)
	e.g.manifest = "" // its record of replacements would read the manifest too
	before := len(must(get(bg, st, keys.Manifest(), 1<<40)))
	c := e.g.candidate(t, e.keys, e.cand("c"))
	if c.until("replace", e.keys.Manifest()) == nil {
		t.Fatal("no swap")
	}
	m, _ := e.h.manifest()
	m.DBSize, m.Groups = 4000, make([]format.GroupRef, format.GroupCount(4000, m.GroupSize))
	e.h.publish(m)
	gc.mu.Lock()
	gc.n = 0
	gc.mu.Unlock()
	c.finish()
	if !errors.Is(c.err, ErrLost) {
		t.Fatalf("swap after a compaction: %v", c.err)
	}
	if gc.n > int64(before)+1 {
		t.Fatalf("the candidate read %d bytes of a %d-byte manifest after its %d-byte swap lost", gc.n, len(must(get(bg, st, keys.Manifest(), 1<<40))), before)
	}
	if ls, err := format.DecodeLease(must(get(bg, st, keys.Lease(), 1<<40))); err != nil || ls.Holder != "c" || ls.Renewed != 0 {
		t.Fatalf("lease after a lost swap: %+v %v", ls, err)
	}
}

// TestLeaseOwnUnresolvedCreate: an earlier candidacy of the same holder at
// the same epoch created the lease, with an outcome that stayed unknown. The
// next candidacy finds that lease after its own create fails, and takes it.
func TestLeaseOwnUnresolvedCreate(t *testing.T) {
	st, keys := newMemStore(t)
	g := &gated{Store: st}
	newHistory(t, st, keys, 23, 512, 4)
	c := Candidate{Holder: "a", TTL: time.Minute, Now: newClock(time.Millisecond).now}
	first := g.candidate(t, keys, c)
	for _, act := range []action{late, fail, fail, fail, fail} {
		if first.until("create", keys.Lease()) == nil {
			t.Fatal("lease create not repeated")
		}
		first.step(act)
	}
	first.finish()
	if !errors.Is(first.err, storage.ErrOutcomeUnknown) {
		t.Fatalf("unresolved lease create: %v", first.err)
	}
	second := g.candidate(t, keys, c)
	if second.until("create", keys.Lease()) == nil {
		t.Fatal("no lease create")
	}
	if ok, err := g.land(); !ok || err != nil {
		t.Fatalf("the first create did not land: %v", err)
	}
	second.finish()
	if second.err != nil || !bytes.Equal(must(get(bg, st, keys.Lease(), 1<<40)), leaseBytes(t, second.l)) {
		t.Fatalf("second candidacy: %v", second.err)
	}
}

// TestLeaseReadFailure: a failed read of the lease ends the candidacy before
// any write.
func TestLeaseReadFailure(t *testing.T) {
	ts, keys := newStore(t)
	newHistory(t, ts, keys, 25, 512, 4)
	var writes []string
	ts.setInject(func(_ context.Context, op, key string) (bool, error) {
		switch {
		case op == "get" && key == keys.Lease():
			return false, &storage.Error{Op: op, Key: key, Err: errInjected}
		case op == "create" || op == "replace":
			writes = append(writes, op+" "+key)
		}
		return false, nil
	})
	if _, _, err := Takeover(bg, ts, keys, Candidate{Holder: "a", TTL: time.Minute, Now: newClock(0).now}); !errors.Is(err, errInjected) || len(writes) != 0 {
		t.Fatalf("takeover after a failed lease read: %v, wrote %v", err, writes)
	}
}

// TestLeaseRewrittenUnderWriter: storage rewrites the lease before every
// replace of it. A rewrite that names the writer's holder and epoch is
// adopted once, not again; one that does not decode is another holder's,
// whose lease may be retried at once.
func TestLeaseRewrittenUnderWriter(t *testing.T) {
	for _, garbage := range []bool{false, true} {
		ts, keys := newStore(t)
		newHistory(t, ts, keys, 26, 512, 4)
		clk := newClock(0)
		_, l, err := Takeover(bg, ts, keys, Candidate{Holder: "a", TTL: time.Minute, Now: clk.now})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(bg)
		replaces := 0
		ts.setInject(func(ctx context.Context, op, key string) (bool, error) {
			if op != "replace" || key != keys.Lease() {
				return false, nil
			}
			if replaces++; replaces > 10 {
				cancel()
			}
			body := []byte("not a lease")
			if !garbage {
				body = leaseBytes(t, &Lease{l: format.Lease{Holder: "a", Epoch: l.l.Epoch, Renewed: int64(replaces)}})
			}
			info, err := ts.Store.Head(ctx, key)
			if err == nil {
				_, err = ts.Store.Replace(ctx, key, info.Version, bytes.NewReader(body))
			}
			return false, err
		})
		err = l.Renew(ctx)
		cancel()
		var held *LeaseHeldError
		switch {
		case !errors.As(err, &held):
			t.Fatalf("garbage %v: %v", garbage, err)
		case garbage && (held.Holder != "" || !held.Expires.Equal(clk.now()) || replaces != 1):
			t.Fatalf("an undecodable lease found after %d replaces: %+v", replaces, held)
		case !garbage && replaces != 2:
			t.Fatalf("own holder and epoch adopted for %d replaces", replaces)
		}
	}
}

// retakeSetup: a takes office, and b, a time to live later, takes the lease
// for the next epoch without swapping the manifest, as a candidacy that has
// not swapped yet, or has lost, does.
func retakeSetup(t *testing.T, seed uint64) (*testStore, format.Keys, *clock, Office, *Lease, *Lease) {
	t.Helper()
	ts, keys := newStore(t)
	newHistory(t, ts, keys, seed, 512, 4)
	clk := newClock(0)
	o, la, err := Takeover(bg, ts, keys, Candidate{Holder: "a", TTL: time.Minute, Now: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Minute)
	lb, err := acquireLease(bg, ts, keys, Candidate{Holder: "b", TTL: time.Minute, Now: clk.now}, o.Epoch+1)
	if err != nil {
		t.Fatal(err)
	}
	return ts, keys, clk, o, la, lb
}

func storedLease(t *testing.T, st storage.Store, keys format.Keys) format.Lease {
	t.Helper()
	l, err := format.DecodeLease(must(get(bg, st, keys.Lease(), 1<<40)))
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// checkRetaken: la holds the lease at office o's epoch, the manifest still
// carries o, and la can renew.
func checkRetaken(t *testing.T, st storage.Store, keys format.Keys, o Office, la *Lease) {
	t.Helper()
	a, err := (&Reader{st: st, keys: keys}).readManifest(bg, anchor{}, nil)
	if err != nil || !o.Holds(a.m) {
		t.Fatalf("manifest %+v, %v; want office at epoch %d", a.m, err, o.Epoch)
	}
	if ls := storedLease(t, st, keys); ls != la.l || ls.Holder != "a" || ls.Epoch != o.Epoch {
		t.Fatalf("lease %+v; the handle has %+v, office epoch %d", ls, la.l, o.Epoch)
	}
	if err := la.Renew(bg); err != nil {
		t.Fatalf("renewal after a retake: %v", err)
	}
}

func TestRetakeAfterCandidateReleased(t *testing.T) {
	ts, keys, _, o, la, lb := retakeSetup(t, 27)
	if err := lb.Release(bg); err != nil {
		t.Fatal(err)
	}
	if err := la.Renew(bg); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("renewal of a released foreign lease: %v", err)
	}
	if err := la.Retake(bg); err != nil {
		t.Fatalf("retake of a released lease: %v", err)
	}
	checkRetaken(t, ts, keys, o, la)
}

func TestRetakeAfterExpiry(t *testing.T) {
	ts, keys, clk, o, la, _ := retakeSetup(t, 28)
	clk.advance(time.Minute)
	if err := la.Renew(bg); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("renewal of an expired foreign lease: %v", err)
	}
	if err := la.Retake(bg); err != nil {
		t.Fatalf("retake of an expired lease: %v", err)
	}
	checkRetaken(t, ts, keys, o, la)
}

func TestRetakeRefusesLiveHolder(t *testing.T) {
	ts, keys, _, _, la, lb := retakeSetup(t, 29)
	var writes []string
	ts.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op == "create" || op == "replace" || op == "delete" {
			writes = append(writes, op+" "+key)
		}
		return false, nil
	})
	err := la.Retake(bg)
	var held *LeaseHeldError
	if !errors.As(err, &held) || !errors.Is(err, ErrLeaseHeld) || held.Holder != "b" || !held.Expires.Equal(time.UnixMilli(lb.l.Renewed).Add(time.Minute)) {
		t.Fatalf("retake of a live foreign lease: %v", err)
	}
	if len(writes) != 0 {
		t.Fatalf("a refused retake wrote %v", writes)
	}
	if ls := storedLease(t, ts, keys); ls != lb.l {
		t.Fatalf("lease %+v; want b's %+v", ls, lb.l)
	}
}

func TestRetakeAdoptsOwnUnknownWrite(t *testing.T) {
	ts, keys, _, o, la, lb := retakeSetup(t, 30)
	if err := lb.Release(bg); err != nil {
		t.Fatal(err)
	}
	replaces := 0
	ts.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op != "replace" || key != keys.Lease() {
			return false, nil
		}
		if replaces++; replaces == 1 { // lands, but its outcome is unknown
			return true, &storage.Error{Op: op, Key: key, Err: storage.ErrOutcomeUnknown}
		}
		return false, nil
	})
	if err := la.Retake(bg); err != nil || replaces != 2 {
		t.Fatalf("retake whose write landed with an unknown outcome: %v after %d replaces", err, replaces)
	}
	ts.setInject(nil)
	checkRetaken(t, ts, keys, o, la)
}

// TestRetakeRefusesDeposed: b takes over from a and releases its lease; a,
// deposed, does not take the lease back.
func TestRetakeRefusesDeposed(t *testing.T) {
	ts, keys := newStore(t)
	newHistory(t, ts, keys, 31, 512, 4)
	clk := newClock(0)
	_, la, err := Takeover(bg, ts, keys, Candidate{Holder: "a", TTL: time.Minute, Now: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Minute)
	_, lb, err := Takeover(bg, ts, keys, Candidate{Holder: "b", TTL: time.Minute, Now: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	if err := lb.Release(bg); err != nil {
		t.Fatal(err)
	}
	var writes []string
	ts.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op == "create" || op == "replace" || op == "delete" {
			writes = append(writes, op+" "+key)
		}
		return false, nil
	})
	if err := la.Retake(bg); !errors.Is(err, ErrLost) || errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("retake by a deposed leader: %v", err)
	}
	if len(writes) != 0 {
		t.Fatalf("a deposed leader's retake wrote %v", writes)
	}
	if ls := storedLease(t, ts, keys); ls != lb.l {
		t.Fatalf("lease %+v; want b's released %+v", ls, lb.l)
	}
}
