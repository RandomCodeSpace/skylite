package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// action is what the gated store does with one call an actor makes.
type action int

const (
	pass action = iota // perform the call
	fail               // fail it with no effect
	drop               // perform a mutation and report an unknown outcome
	late               // report an unknown outcome now; the mutation lands only when the test lands it
	hold               // perform a mutation, then wait for the test to deliver the response
)

// gated wraps a store. A call made with an actor's context waits until the
// test grants it, with an action; every other call passes straight through.
// Interleavings are therefore set by the test, never by the scheduler.
type gated struct {
	storage.Store

	manifest string // the key whose replacements are recorded, if set

	mu       sync.Mutex
	pending  []func() error // late mutations, in the order they were sent
	trace    []string       // every call, as "op key"
	replaced [][2][]byte    // every replacement of manifest that landed: the bytes before and after
}

type actorKey struct{}

// actor runs one function whose storage calls the test steps through.
type actor struct {
	t     testing.TB
	calls chan *call
	done  chan struct{}
	cur   *call // the call the actor waits at, if next has seen it
}

// call is one storage call waiting for the test. Op is the store method in
// lower case, or the method followed by "-response" for a held mutation
// whose response is waiting to be delivered.
type call struct {
	op, key string
	reply   chan action
}

func unknownErr(op, key string) error {
	return &storage.Error{Op: op, Key: key, Err: fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, errInjected)}
}

func (g *gated) gate(ctx context.Context, op, key string) (action, error) {
	g.mu.Lock()
	g.trace = append(g.trace, op+" "+key)
	g.mu.Unlock()
	a, _ := ctx.Value(actorKey{}).(*actor)
	if a == nil {
		return pass, nil
	}
	c := &call{op: op, key: key, reply: make(chan action, 1)}
	select {
	case a.calls <- c:
	case <-ctx.Done():
		return pass, ctx.Err()
	}
	select {
	case act := <-c.reply:
		return act, nil
	case <-ctx.Done():
		return pass, ctx.Err()
	}
}

// mutate runs a mutation under the action the test chose.
func (g *gated) mutate(ctx context.Context, op, key string, do func(context.Context) error) error {
	act, err := g.gate(ctx, op, key)
	if err != nil {
		return &storage.Error{Op: op, Key: key, Err: err}
	}
	switch act {
	case fail:
		return &storage.Error{Op: op, Key: key, Err: errInjected}
	case drop:
		_ = do(ctx)
		return unknownErr(op, key)
	case late:
		g.mu.Lock()
		g.pending = append(g.pending, func() error { return do(context.Background()) })
		g.mu.Unlock()
		return unknownErr(op, key)
	case hold:
		err := do(ctx)
		if _, gerr := g.gate(ctx, op+"-response", key); gerr != nil {
			return &storage.Error{Op: op, Key: key, Err: fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, gerr)}
		}
		return err
	}
	return do(ctx)
}

func (g *gated) read(ctx context.Context, op, key string) error {
	act, err := g.gate(ctx, op, key)
	switch {
	case err != nil:
		return &storage.Error{Op: op, Key: key, Err: err}
	case act != pass:
		return &storage.Error{Op: op, Key: key, Err: errInjected}
	}
	return nil
}

func (g *gated) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	var v string
	err = g.mutate(ctx, "create", key, func(ctx context.Context) (err error) {
		v, err = g.Store.Create(ctx, key, bytes.NewReader(b))
		return err
	})
	return v, err
}

func (g *gated) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	var v string
	err = g.mutate(ctx, "replace", key, func(ctx context.Context) (err error) {
		if key != g.manifest {
			v, err = g.Store.Replace(ctx, key, version, bytes.NewReader(b))
			return err
		}
		// Versions are never reused, so if the replacement lands, old is
		// what it replaced.
		old, info, rerr := readObject(ctx, g.Store, key, format.MaxManifestSize+1)
		v, err = g.Store.Replace(ctx, key, version, bytes.NewReader(b))
		if err == nil && rerr == nil && info.Version == version {
			g.mu.Lock()
			g.replaced = append(g.replaced, [2][]byte{old, b})
			g.mu.Unlock()
		}
		return err
	})
	return v, err
}

func (g *gated) Delete(ctx context.Context, key string) error {
	return g.mutate(ctx, "delete", key, func(ctx context.Context) error { return g.Store.Delete(ctx, key) })
}

func (g *gated) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if err := g.read(ctx, "get", key); err != nil {
		return nil, storage.Info{}, err
	}
	return g.Store.Get(ctx, key)
}

func (g *gated) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if err := g.read(ctx, "getrange", key); err != nil {
		return nil, storage.Info{}, err
	}
	return g.Store.GetRange(ctx, key, off, n)
}

func (g *gated) Head(ctx context.Context, key string) (storage.Info, error) {
	if err := g.read(ctx, "head", key); err != nil {
		return storage.Info{}, err
	}
	return g.Store.Head(ctx, key)
}

func (g *gated) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if err := g.read(ctx, "list", prefix); err != nil {
		return nil, err
	}
	return g.Store.List(ctx, prefix, startAfter, limit)
}

// land performs the oldest late mutation, if any, and returns its result.
func (g *gated) land() (bool, error) {
	g.mu.Lock()
	if len(g.pending) == 0 {
		g.mu.Unlock()
		return false, nil
	}
	f := g.pending[0]
	g.pending = g.pending[1:]
	g.mu.Unlock()
	return true, f()
}

// landAll performs every late mutation, oldest first.
func (g *gated) landAll() {
	for ok, _ := g.land(); ok; ok, _ = g.land() {
	}
}

// takeTrace returns the calls made since the last takeTrace.
func (g *gated) takeTrace() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	t := g.trace
	g.trace = nil
	return t
}

// spawn starts fn as an actor. Its storage calls through g wait for the
// test; cancelling at cleanup releases an actor the test abandoned.
func (g *gated) spawn(t testing.TB, fn func(ctx context.Context)) *actor {
	ctx, cancel := context.WithCancel(context.Background())
	a := &actor{t: t, calls: make(chan *call), done: make(chan struct{})}
	ctx = context.WithValue(ctx, actorKey{}, a)
	go func() {
		defer close(a.done)
		fn(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-a.done
	})
	return a
}

// next returns the call the actor waits at, or nil once it has finished.
func (a *actor) next() *call {
	if a.cur == nil {
		select {
		case a.cur = <-a.calls:
		case <-a.done:
			return nil
		}
	}
	return a.cur
}

// step grants the actor's next call with act and returns it, or nil if the
// actor had finished. It returns once the actor waits at its following call
// or has finished, so that only one actor runs at a time.
func (a *actor) step(act action) *call {
	c := a.next()
	if c != nil {
		a.cur = nil
		c.reply <- act
		a.next()
	}
	return c
}

// run grants up to n calls with pass.
func (a *actor) run(n int) {
	for range n {
		if a.step(pass) == nil {
			return
		}
	}
}

// until passes calls until the actor waits at op on key, and returns that
// call ungranted, or nil if the actor finished first.
func (a *actor) until(op, key string) *call {
	for c := a.next(); c != nil; c = a.next() {
		if c.op == op && c.key == key {
			return c
		}
		a.step(pass)
	}
	return nil
}

// finish passes every call until the actor returns, and fails the test if
// it never does.
func (a *actor) finish() {
	a.t.Helper()
	for n := 0; a.step(pass) != nil; n++ {
		if n == 10000 {
			a.t.Fatal("an actor made 10000 storage calls without finishing")
		}
	}
	<-a.done
}

// finished reports whether the actor has returned, waiting for it to reach
// its next call or its end.
func (a *actor) finished() bool { return a.next() == nil }

// candidate is an actor running Takeover.
type candidate struct {
	*actor
	o   Office
	l   *Lease
	err error
}

func (g *gated) candidate(t testing.TB, keys format.Keys, c Candidate) *candidate {
	k := &candidate{}
	k.actor = g.spawn(t, func(ctx context.Context) { k.o, k.l, k.err = Takeover(ctx, g, keys, c) })
	return k
}

// clock is a test clock that advances by step on every reading, so that a
// candidate reading it after another stamped a lease sees that lease
// expired when step is at least the lease's time to live.
type clock struct {
	mu   sync.Mutex
	t    time.Time
	step time.Duration
}

func newClock(step time.Duration) *clock {
	return &clock{t: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC), step: step}
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(c.step)
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// outcome is what a client of the simulated leader is told.
type outcome int

const (
	committed outcome = iota
	notCommitted
	unknownOutcome
)

// leader is a simulated leader in office: it creates ordinary commits after
// its head and resolves each create with Office.Recognise, as decision 6
// says. Creating commits is the commit path's work, not this package's; the
// simulation exists to test recognition of loss of office.
type leader struct {
	h    *history
	st   storage.Store
	o    Office
	head uint64
	size uint32
	done bool // fenced or stopped: it commits nothing more

	told map[string]outcome // commit identifier to what its client was told
}

// newLeader starts a leader in office o; it reads its bump from the inner
// store, so it may run in an actor.
func newLeader(h *history, st storage.Store, o Office) *leader {
	l := &leader{h: h, st: st, o: o, head: o.Head, told: map[string]outcome{}}
	b, err := get(context.Background(), h.st, h.keys.Commit(o.DB, o.Head))
	var bh format.CommitHeader
	if err == nil {
		bh, err = format.DecodeCommitHeader(b[:format.CommitHeaderSize], o.DB)
	}
	if err != nil {
		h.t.Errorf("leader at epoch %d: its bump: %v", o.Epoch, err)
		l.done = true
	}
	l.size = bh.DBSize
	return l
}

// build returns the next commit, which writes page 1.
func (l *leader) build() ([]byte, format.CommitHeader) {
	l.h.mu.Lock()
	defer l.h.mu.Unlock()
	return l.h.encodeCommit(l.head+1, l.o.DB, l.o.Epoch, l.size, map[uint32][]byte{1: l.h.bytes(int(l.h.ps))}, false)
}

// commit tries one commit and returns what its client is told.
func (l *leader) commit(ctx context.Context) outcome {
	if l.done {
		return notCommitted
	}
	b, ch := l.build()
	out := l.create(ctx, ch.Txn, b)
	l.told[string(ch.CommitID)] = out
	switch out {
	case committed:
		l.head++
	default:
		// Fenced, or an indeterminate commit: either way it fail-stops.
		// A commit known not to have been sent anywhere would release the
		// binding; this simulation never builds a second object.
		l.done = true
	}
	return out
}

func (l *leader) create(ctx context.Context, txn uint64, b []byte) outcome {
	key := l.h.keys.Commit(l.o.DB, txn)
	unknown := false
	for range createAttempts {
		_, err := l.st.Create(ctx, key, bytes.NewReader(b))
		switch {
		case err == nil:
			return committed
		case errors.Is(err, storage.ErrPreconditionFailed):
			found, rerr := get(ctx, l.st, key)
			if rerr != nil && !errors.Is(rerr, storage.ErrNotFound) {
				unknown = true
				continue
			}
			switch f, _ := l.o.Recognise(b, found); f {
			case FoundOwn:
				return committed
			case FoundBump:
				return notCommitted
			default:
				return unknownOutcome
			}
		case errors.Is(err, storage.ErrOutcomeUnknown):
			unknown = true
		}
		if ctx.Err() != nil {
			break
		}
	}
	if unknown {
		return unknownOutcome
	}
	return notCommitted
}

// compact publishes a manifest at the leader's head, conditioned on the one
// it read, and retires what it covers. It publishes no base objects, so no
// reader may run against a database it compacted. It reports false when the
// leader must leave office: the manifest is not its own, or its replacement
// has no definite success.
func (l *leader) compact(ctx context.Context) bool {
	key := l.h.keys.Manifest()
	b, info, err := readObject(ctx, l.st, key, format.MaxManifestSize+1)
	if err != nil {
		return true
	}
	m, err := format.DecodeManifest(b, l.o.DB)
	if err != nil || !l.o.Holds(m) {
		return false
	}
	if m.Txn >= l.head {
		return true
	}
	from := m.RetiredThrough
	m.Txn, m.DBSize, m.Groups = l.head, l.size, make([]format.GroupRef, format.GroupCount(l.size, m.GroupSize))
	next, err := format.EncodeManifest(m)
	if err != nil {
		return false
	}
	own, err := settle(ctx, nil,
		func() error {
			_, err := l.st.Replace(ctx, key, info.Version, bytes.NewReader(next))
			return err
		},
		func() (bool, error) {
			b, err := get(ctx, l.st, key)
			return bytes.Equal(b, next), err
		})
	if err != nil || !own {
		return false
	}
	for k := from + 1; k <= m.Txn; k++ {
		ck := l.h.keys.Commit(l.o.DB, k)
		b, info, err := readObject(ctx, l.st, ck, format.MaxCommitSize+1)
		if err != nil {
			continue
		}
		h, err := format.DecodeCommitHeader(b[:format.CommitHeaderSize], l.o.DB)
		if err != nil || h.EpochBump || h.Retired {
			continue
		}
		r, err := h.Retire()
		if err != nil {
			continue
		}
		_, _ = l.st.Replace(ctx, ck, info.Version, bytes.NewReader(r))
	}
	return true
}

// sync brings the history's model up to date after writers other than the
// history: it re-reads the manifest and appends the state of every commit
// above the model's head.
func (h *history) sync() {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.Background()
	b, info, err := readObject(ctx, h.st, h.keys.Manifest(), format.MaxManifestSize+1)
	if err != nil {
		h.t.Fatal(err)
	}
	if h.m, err = format.DecodeManifest(b, h.db); err != nil {
		h.t.Fatal(err)
	}
	h.mv = info.Version
	for txn := uint64(len(h.states)); ; txn++ {
		b, err := get(ctx, h.st, h.keys.Commit(h.db, txn))
		if errors.Is(err, storage.ErrNotFound) {
			return
		}
		if err != nil {
			h.t.Fatal(err)
		}
		pages := map[uint32][]byte{}
		ch, _, err := format.DecodeCommit(bytes.NewReader(b), h.db, func(p uint32, body []byte) error {
			pages[p] = bytes.Clone(body)
			return nil
		})
		if err != nil {
			h.t.Fatalf("sync commit %d: %v", txn, err)
		}
		prev := h.states[txn-1]
		next := make([][]byte, ch.DBSize)
		for i := range next {
			if i < len(prev) {
				next[i] = prev[i]
			} else {
				next[i] = make([]byte, h.ps)
			}
		}
		for p, body := range pages {
			next[p-1] = body
		}
		h.states = append(h.states, next)
		h.headers[txn] = ch
	}
}

// invariants checks storage against decision 4 of the record and the
// takeover rules, from the inner store, and remembers what it has seen so
// that the manifest is also checked against its own past.
type invariants struct {
	st   storage.Store
	g    *gated // if set, every replacement of the manifest through it is checked
	keys format.Keys
	db   format.DatabaseID
	seen int // replacements of g already checked

	epoch, txn, retired uint64
	nonces              map[uint64][16]byte
}

func newInvariants(st storage.Store, keys format.Keys, db format.DatabaseID) *invariants {
	return &invariants{st: st, keys: keys, db: db, nonces: map[uint64][16]byte{}}
}

// check checks storage, offices that were granted and what leaders told
// their clients. notCommitted is checked only when final is set, once no
// request is left that could still land.
func (v *invariants) check(offices []Office, leaders []*leader, final bool) error {
	ctx := context.Background()
	b, err := get(ctx, v.st, v.keys.Manifest())
	if err != nil {
		return err
	}
	m, err := format.DecodeManifest(b, v.db)
	if err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	switch n, ok := v.nonces[m.Epoch]; {
	case m.Epoch < v.epoch, m.Txn < v.txn, m.RetiredThrough < v.retired:
		return fmt.Errorf("manifest went back: epoch %d txn %d retired %d after %d %d %d", m.Epoch, m.Txn, m.RetiredThrough, v.epoch, v.txn, v.retired)
	case ok && n != m.Nonce:
		return fmt.Errorf("epoch %d has two writer nonces", m.Epoch)
	}
	v.epoch, v.txn, v.retired, v.nonces[m.Epoch] = m.Epoch, m.Txn, m.RetiredThrough, m.Nonce
	if v.g != nil {
		v.g.mu.Lock()
		rs := v.g.replaced[v.seen:]
		v.seen = len(v.g.replaced)
		v.g.mu.Unlock()
		for _, r := range rs {
			if err := v.checkReplace(r[0], r[1]); err != nil {
				return err
			}
		}
	}

	var keys []string
	for after := ""; ; {
		es, err := v.st.List(ctx, v.keys.CommitPrefix(v.db), after, storage.MaxListLimit)
		if err != nil {
			return err
		}
		for _, e := range es {
			keys = append(keys, e.Key)
		}
		if len(es) < storage.MaxListLimit {
			break
		}
		after = es[len(es)-1].Key
	}
	bumps := map[uint64]uint64{} // epoch to transaction
	ids := map[string]uint64{}
	var prev format.CommitHeader
	for i, key := range keys {
		txn := uint64(i) + 1 //nolint:gosec // never negative
		if key != v.keys.Commit(v.db, txn) {
			return fmt.Errorf("log is not contiguous: %s at position %d", key, txn)
		}
		b, err := get(ctx, v.st, key)
		if err != nil {
			return err
		}
		h, _, err := format.DecodeCommit(bytes.NewReader(b), v.db, func(uint32, []byte) error { return nil })
		if errors.Is(err, format.ErrRetired) {
			if txn > m.Txn {
				return fmt.Errorf("commit %d retired above manifest %d", txn, m.Txn)
			}
			err = nil
		}
		if err == nil {
			err = h.CheckKey(v.db, txn)
		}
		if err != nil {
			return fmt.Errorf("commit %d: %w", txn, err)
		}
		if txn > 1 && h.Epoch < prev.Epoch {
			return fmt.Errorf("commit %d at epoch %d follows an object at epoch %d", txn, h.Epoch, prev.Epoch)
		}
		switch {
		case h.EpochBump:
			if t, ok := bumps[h.Epoch]; ok {
				return fmt.Errorf("epoch %d has two bumps, at %d and %d", h.Epoch, t, txn)
			}
			bumps[h.Epoch] = txn
			if h.DBSize != prev.DBSize {
				return fmt.Errorf("bump %d has size %d after %d", txn, h.DBSize, prev.DBSize)
			}
		case txn > 1 && h.Epoch != prev.Epoch:
			return fmt.Errorf("commit %d at epoch %d follows an object at epoch %d", txn, h.Epoch, prev.Epoch)
		case txn > 1:
			ids[string(h.CommitID)] = txn
		}
		prev = h
	}
	if m.Txn > uint64(len(keys)) {
		return fmt.Errorf("manifest at %d beyond the head %d", m.Txn, len(keys))
	}

	seen := map[uint64]bool{}
	for _, o := range offices {
		switch n, ok := v.nonces[o.Epoch]; {
		case seen[o.Epoch]:
			return fmt.Errorf("two offices at epoch %d", o.Epoch)
		case bumps[o.Epoch] != o.Head:
			return fmt.Errorf("office at epoch %d has head %d, its bump is at %d", o.Epoch, o.Head, bumps[o.Epoch])
		case ok && n != o.Nonce:
			return fmt.Errorf("office at epoch %d has another nonce than its manifest", o.Epoch)
		}
		seen[o.Epoch] = true
	}
	for _, l := range leaders {
		for id, out := range l.told {
			txn, in := ids[id]
			switch {
			case out == committed && !in:
				return fmt.Errorf("leader at epoch %d acknowledged a commit that is not in the log", l.o.Epoch)
			case out == notCommitted && in && final:
				return fmt.Errorf("leader at epoch %d told not committed for commit %d", l.o.Epoch, txn)
			}
			for epoch, bump := range bumps {
				if out == committed && epoch > l.o.Epoch && txn > bump {
					return fmt.Errorf("leader at epoch %d acknowledged commit %d above the bump at %d of epoch %d", l.o.Epoch, txn, bump, epoch)
				}
			}
		}
		if l.head > uint64(len(keys)) {
			return fmt.Errorf("leader at epoch %d has head %d beyond the log", l.o.Epoch, l.head)
		}
	}
	return nil
}

// checkReplace checks one replacement of the manifest against invariant 1:
// it keeps the epoch and the nonce, or it is an epoch swap, to the epoch
// plus one with a new nonce and every other field carried forward.
func (v *invariants) checkReplace(old, next []byte) error {
	a, err := format.DecodeManifest(old, v.db)
	if err != nil {
		return fmt.Errorf("replaced manifest: %w", err)
	}
	b, err := format.DecodeManifest(next, v.db)
	if err != nil {
		return fmt.Errorf("replacing manifest: %w", err)
	}
	switch {
	case b.Epoch == a.Epoch:
		if b.Nonce != a.Nonce {
			return fmt.Errorf("a replacement at epoch %d changed the nonce", a.Epoch)
		}
	case b.Epoch != a.Epoch+1 || b.Nonce == a.Nonce:
		return fmt.Errorf("manifest swapped from epoch %d to %d, nonce kept: %v", a.Epoch, b.Epoch, b.Nonce == a.Nonce)
	default:
		a.Epoch, a.Nonce = b.Epoch, b.Nonce
		if !reflect.DeepEqual(a, b) {
			return fmt.Errorf("the swap to epoch %d changed more than the epoch and the nonce", b.Epoch)
		}
	}
	return nil
}

// unfenced returns the offices whose next name, the one after Head, holds
// no epoch bump.
func unfenced(t testing.TB, st storage.Store, keys format.Keys, offices []Office) []Office {
	t.Helper()
	var out []Office
	for _, o := range offices {
		b, err := get(context.Background(), st, keys.Commit(o.DB, o.Head+1))
		if err == nil {
			h, err := format.DecodeCommitHeader(b[:format.CommitHeaderSize], o.DB)
			if err != nil {
				t.Fatal(err)
			}
			if h.EpochBump {
				continue
			}
		}
		out = append(out, o)
	}
	return out
}

// newMemStore opens localfs in a temporary directory in memory where the
// system has one (/dev/shm), else in t.TempDir. These tests replay
// thousands of schedules, and on a disk every write's fsyncs cost tens of
// milliseconds; durability is not what they check.
func newMemStore(t *testing.T) (storage.Store, format.Keys) {
	t.Helper()
	dir := t.TempDir()
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		if d, err := os.MkdirTemp("/dev/shm", "skylite-engine-"); err == nil {
			dir = d
			t.Cleanup(func() { _ = os.RemoveAll(d) })
		}
	}
	fs, err := localfs.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	keys, err := format.NewKeys("dbs/test")
	if err != nil {
		t.Fatal(err)
	}
	return fs, keys
}

// env is one database under a gated store, with a leader in office.
type env struct {
	t    testing.TB
	g    *gated
	keys format.Keys
	h    *history
	inv  *invariants
	clk  *clock
	l    *leader // the leader in office when the test starts
}

// newEnv creates a database through the history (commit 1 at epoch 1),
// writes commits 2 to 4, compacts through 3 and gives office to leader "l",
// which commits once more: its bump is 5 and the log runs to 6.
func newEnv(t *testing.T, seed uint64) *env {
	t.Helper()
	st, keys := newMemStore(t)
	return newEnvOn(t, st, keys, seed)
}

// newEnvOn is newEnv on store st.
func newEnvOn(t *testing.T, st storage.Store, keys format.Keys, seed uint64) *env {
	t.Helper()
	g := &gated{Store: st, manifest: keys.Manifest()}
	h := newHistory(t, st, keys, seed, 512, 4)
	for range 3 {
		h.randomCommit(8)
	}
	h.compact(3)
	e := &env{t: t, g: g, keys: keys, h: h, inv: newInvariants(st, keys, h.db), clk: newClock(0)}
	e.inv.g = g
	o, _, err := Takeover(context.Background(), g, keys, e.cand("l"))
	if err != nil {
		t.Fatal(err)
	}
	h.sync()
	e.l = newLeader(h, g, o)
	if out := e.l.commit(context.Background()); out != committed {
		t.Fatalf("setup commit: %v", out)
	}
	h.sync()
	g.takeTrace()
	return e
}

// cand describes a candidate with a clock of its own, fixed two lease
// lifetimes after the previous candidate's. It sees every earlier lease
// expired, every earlier candidate sees its lease more than a lifetime ahead
// and so expired too, and it never renews its lease during a walk.
func (e *env) cand(holder string) Candidate {
	e.clk.advance(2 * time.Minute)
	now := e.clk.now()
	return Candidate{Holder: holder, TTL: time.Minute, Now: func() time.Time { return now }}
}

func (e *env) check(offices []Office, leaders []*leader, final bool) {
	e.t.Helper()
	if err := e.inv.check(offices, leaders, final); err != nil {
		e.t.Fatal(err)
	}
}

// offices returns the offices of the candidates that obtained one, and
// checks that a candidate that failed holds none.
func offices(t testing.TB, cs ...*candidate) []Office {
	t.Helper()
	var out []Office
	for _, c := range cs {
		switch {
		case c.err == nil:
			out = append(out, c.o)
		case c.o != Office{} || c.l != nil:
			t.Fatalf("a failed candidate holds office %+v: %v", c.o, c.err)
		}
	}
	return out
}

// seeded returns a generator for a test's schedule.
func seeded(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, 2)) } //nolint:gosec // reproducible schedules
