package engine

// Compaction, retirement and garbage collection: record 0004, decision 8.
//
// A compaction attempt folds the commits after the manifest's transaction
// number, up to the leader's base version, into new base group objects and
// replaces the manifest by compare-and-swap on the token it read. It rewrites
// only the groups those commits changed and carries every other reference
// forward. Its memory is a second index of the log it folds, which the tail
// bound limits, a flag per group, the new manifest, and the pages of two
// groups; never the database.
//
// Retirement replaces each commit a manifest covers, once that manifest has
// been current for the grace period, with its retired form. Garbage
// collection deletes base objects that the one garbage rule makes garbage,
// a grace period after it found them.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// DefaultGrace is the grace period a leader uses when its Config sets none:
// commits are retired no sooner than this after a manifest covering them
// became current, and garbage is deleted no sooner than this after it was
// found.
const DefaultGrace = 5 * time.Minute

const (
	defaultCompactBytes    = 256 << 20
	defaultCompactInterval = time.Minute
	// passBatch is how many commits one retirement pass reads, and how many
	// garbage keys one collection pass deletes, before the pass releases
	// the compaction lock to a compaction or maintenance step 7.
	passBatch = 1024
	// maxGarbage is how many garbage keys one collection holds between
	// finding and deleting them; the rest wait for the next collection.
	maxGarbage = 1 << 16
)

// Variables so that tests can change them.
var (
	// compactPace is the least time between two of a leader's manifest
	// writes: Google Cloud Storage takes one write per second to one name
	// (record 0004, decision 10).
	compactPace = time.Second
	// compactTick is how often the leader looks for due compaction,
	// retirement and collection work without being woken.
	compactTick = time.Second
	// maxManifestSize is format.MaxManifestSize.
	maxManifestSize = format.MaxManifestSize
	// retireWidth is how many commits retirement reads and replaces at
	// once: one at a time, three round trips each, it fell behind 26
	// commits a second on Azure.
	retireWidth = 32
	// drawTag draws a compaction attempt's tag.
	drawTag = func() (uint64, error) {
		var b [8]byte
		_, err := rand.Read(b[:])
		return binary.LittleEndian.Uint64(b[:]), err
	}
)

// errCompactLost means the manifest a compaction attempt read was replaced
// before its own replacement could land, so it never will.
var errCompactLost = errors.New("engine: the manifest was replaced before the compaction's replacement")

// errCompactUnresolved means a compaction's replacement of the manifest has
// no definite answer: it may still land.
var errCompactUnresolved = errors.New("engine: the compaction's replacement of the manifest has no definite outcome")

// compaction is one compaction attempt: it read manifest a, and r is the
// state at its target, anchored on a.
type compaction struct {
	st      storage.Store
	keys    format.Keys
	a       anchor
	r       *Reader
	retired uint64 // every commit up to it was read as retired or as a bump
}

// run creates the attempt's base objects and replaces the manifest. It
// returns the new manifest's anchor, or an error: errCompactLost if the
// manifest it read was replaced first, errCompactUnresolved if its own
// replacement may still land, format.ErrManifestTooLarge before it writes
// anything if the new manifest would pass maxManifestSize, a *staleError if
// an object it reads has changed or gone, and otherwise an error after which
// a new attempt may start at once.
func (k *compaction) run(ctx context.Context) (anchor, error) {
	m := k.a.manifest()
	t, size := k.r.txn, k.r.size
	if t <= m.Txn {
		return anchor{}, fmt.Errorf("engine: compaction to %d over a manifest at %d", t, m.Txn)
	}
	n := format.GroupCount(size, m.GroupSize)
	changed := k.changed(n)
	nm := m
	nm.Txn, nm.DBSize = t, size
	nm.RetiredThrough = max(m.RetiredThrough, min(k.retired, m.Txn))
	nm.Groups = make([]format.GroupRef, n)
	for g := range nm.Groups {
		if !changed[g] && g < len(m.Groups) {
			nm.Groups[g] = m.Groups[g]
		}
	}
	// The new manifest's size does not depend on its references: refuse
	// before writing anything.
	b, err := format.EncodeManifest(nm)
	if err == nil && len(b) > maxManifestSize {
		err = fmt.Errorf("%w: %d bytes", format.ErrManifestTooLarge, len(b))
	}
	if err != nil {
		return anchor{}, err
	}
	for g := range n {
		if changed[g] {
			if nm.Groups[g], err = k.group(ctx, g); err != nil {
				return anchor{}, err
			}
		}
	}
	if b, err = format.EncodeManifest(nm); err != nil {
		return anchor{}, err
	}
	var version string
	key := k.keys.Manifest()
	own, err := settle(ctx, nil,
		func() error {
			v, err := k.st.Replace(ctx, key, k.a.version, bytes.NewReader(b))
			version = v
			return err
		},
		func() (bool, error) {
			got, info, err := readObject(ctx, k.st, key, len(b)+1)
			version = info.Version
			return bytes.Equal(got, b), err
		})
	switch {
	case errors.Is(err, storage.ErrOutcomeUnknown) || errors.Is(err, ErrCorrupt):
		return anchor{}, fmt.Errorf("%w: %w", errCompactUnresolved, err)
	case err != nil:
		return anchor{}, fmt.Errorf("engine: compaction to %d: replacing the manifest: %w", t, err)
	case !own:
		return anchor{}, errCompactLost
	}
	return newAnchor(nm, version), nil
}

// changed reports, for each of the n groups of the new manifest, whether a
// commit after the manifest changed it: wrote one of its pages, or made the
// database smaller than one of them.
func (k *compaction) changed(n uint32) []bool {
	ch := make([]bool, n)
	gs := k.a.m.GroupSize
	prev := k.a.m.DBSize
	for _, c := range k.r.commits {
		for _, e := range c.entries {
			if g := (e.Pgno - 1) / gs; g < n {
				ch[g] = true
			}
		}
		if c.h.DBSize < prev {
			for g := c.h.DBSize / gs; g <= (prev-1)/gs && g < n; g++ {
				ch[g] = true
			}
		}
		prev = c.h.DBSize
	}
	return ch
}

// group creates the base object of group g at the target and returns its
// reference. It references an object only if its create succeeded, or
// failed its precondition and a read then returned the bytes it meant to
// write; otherwise it draws a new tag and tries again under the new key.
func (k *compaction) group(ctx context.Context, g uint32) (format.GroupRef, error) {
	pages, err := k.pages(ctx, g)
	if err != nil {
		return format.GroupRef{}, err
	}
	m := k.a.m
	var last error
	for range createAttempts {
		tag, err := drawTag()
		if err != nil {
			return format.GroupRef{}, err
		}
		h := format.BaseHeader{DB: m.DB, PageSize: m.PageSize, GroupSize: m.GroupSize, Group: g, Txn: k.r.txn, Tag: tag, DBSize: k.r.size}
		var out bytes.Buffer
		out.Grow(int(h.Size()))
		if err := format.EncodeBase(&out, h, bytes.NewReader(pages)); err != nil {
			return format.GroupRef{}, err
		}
		obj, key := out.Bytes(), k.keys.Base(m.DB, g, k.r.txn, tag)
		own, err := settle(ctx, nil,
			func() error {
				_, err := k.st.Create(ctx, key, bytes.NewReader(obj))
				return err
			},
			func() (bool, error) {
				got, _, err := readObject(ctx, k.st, key, len(obj)+1)
				return bytes.Equal(got, obj), err
			})
		if err == nil && own {
			return format.GroupRef{Txn: k.r.txn, Tag: tag}, nil
		}
		if err := ctx.Err(); err != nil {
			return format.GroupRef{}, err
		}
		last = cmp.Or(err, fmt.Errorf("engine: base object %s holds other bytes", key))
	}
	return format.GroupRef{}, fmt.Errorf("engine: compaction to %d: no base object of group %d resolved: %w", k.r.txn, g, last)
}

// pages returns the pages of group g at the target, concatenated, zeros past
// the database. A page the predecessor's base supplies comes from one read
// of its group object; the pages one commit supplies, from one ranged read
// of that commit.
func (k *compaction) pages(ctx context.Context, g uint32) ([]byte, error) {
	m := k.a.m
	ps := int64(m.PageSize)
	buf := make([]byte, int64(m.GroupSize)*ps)
	first := uint64(g)*uint64(m.GroupSize) + 1
	type run struct {
		c     *commit
		es    []format.PageEntry // ascending
		slots []uint32           // where each goes in the group
	}
	var runs []*run
	byCommit := map[*commit]*run{}
	var base format.BaseHeader
	var baseSlots []uint32
	for i := range m.GroupSize {
		p := first + uint64(i)
		if p > uint64(k.r.size) {
			break
		}
		s := k.r.locate(uint32(p)) //nolint:gosec // at most the database size
		switch {
		case s.c != nil:
			r := byCommit[s.c]
			if r == nil {
				r = &run{c: s.c}
				byCommit[s.c] = r
				runs = append(runs, r)
			}
			r.es, r.slots = append(r.es, s.e), append(r.slots, i)
		case s.base.Txn != 0:
			base = s.base
			baseSlots = append(baseSlots, i)
		}
	}
	if base.Txn != 0 {
		if err := k.basePages(ctx, base, baseSlots, buf); err != nil {
			return nil, err
		}
	}
	for _, r := range runs {
		lo, hi := r.es[0], r.es[len(r.es)-1]
		off := int64(lo.Offset)                                                                //nolint:gosec // DecodeCommitTable bounds it by MaxCommitSize
		b, info, err := k.r.getRange(ctx, r.c.key, off, int64(hi.Offset)+int64(hi.Length)-off) //nolint:gosec // as above
		switch {
		case errors.Is(err, storage.ErrNotFound):
			return nil, fmt.Errorf("%w: commit %d deleted", ErrCorrupt, r.c.h.Txn)
		case err != nil:
			return nil, err
		case info.Version != r.c.version:
			return nil, &staleError{txn: r.c.h.Txn}
		}
		for i, e := range r.es {
			at := int64(e.Offset) - off //nolint:gosec // as above
			if at+int64(e.Length) > int64(len(b)) {
				return nil, fmt.Errorf("%w: commit %d ends before page %d", ErrCorrupt, r.c.h.Txn, e.Pgno)
			}
			body, err := format.DecodeCommitPage(r.c.h, e, b[at:at+int64(e.Length)])
			if err != nil {
				return nil, fmt.Errorf("engine: commit %d: %w", r.c.h.Txn, err)
			}
			copy(buf[int64(r.slots[i])*ps:], body)
		}
	}
	return buf, nil
}

// basePages copies the given slots of base object h into buf, from one
// read of the whole object.
func (k *compaction) basePages(ctx context.Context, h format.BaseHeader, slots []uint32, buf []byte) error {
	key := k.keys.Base(h.DB, h.Group, h.Txn, h.Tag)
	rc, info, err := k.st.Get(ctx, key)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return &staleError{group: h.Group, ref: format.GroupRef{Txn: h.Txn, Tag: h.Tag}}
	case err != nil:
		return err
	}
	b, err := readBody(rc, info, 0, h.Size())
	if err != nil {
		return err
	}
	if int64(len(b)) != h.Size() || info.Size != h.Size() {
		return fmt.Errorf("%w: base object %s is %d bytes, want %d", ErrCorrupt, key, info.Size, h.Size())
	}
	got, pages, err := format.DecodeBase(b, h.DB)
	if err == nil {
		err = got.CheckKey(h.DB, h.Group, h.Txn, h.Tag)
	}
	if err == nil && (got.PageSize != h.PageSize || got.GroupSize != h.GroupSize) {
		err = fmt.Errorf("%w: page size %d and group size %d", ErrCorrupt, got.PageSize, got.GroupSize)
	}
	if err != nil {
		return fmt.Errorf("engine: base object %s: %w", key, err)
	}
	for _, i := range slots {
		copy(buf[int64(i)*int64(h.PageSize):], pages[i])
	}
	return nil
}

// readHeader reads and checks the header of commit txn of database db.
func readHeader(ctx context.Context, st storage.Store, keys format.Keys, db format.DatabaseID, txn uint64) (format.CommitHeader, string, error) {
	rc, info, err := st.GetRange(ctx, keys.Commit(db, txn), 0, format.CommitHeaderSize)
	if err != nil {
		return format.CommitHeader{}, "", err
	}
	b, err := readBody(rc, info, 0, format.CommitHeaderSize)
	if err != nil {
		return format.CommitHeader{}, "", err
	}
	h, err := format.DecodeCommitHeader(b, db)
	if err == nil {
		err = h.CheckKey(db, txn)
	}
	if err != nil {
		return format.CommitHeader{}, "", fmt.Errorf("engine: commit %d: %w", txn, err)
	}
	return h, info.Version, nil
}

// retireCommit retires commit k of database db, which a manifest read from
// storage, or published with a definite success, covers. It reads the
// commit and, if it is neither retired nor an epoch bump, replaces it with
// its retired form, conditioned on the version it read; the outcome of that
// replace needs no resolution. It reports whether a read showed the commit
// retired or an epoch bump.
func retireCommit(ctx context.Context, st storage.Store, keys format.Keys, db format.DatabaseID, k uint64) (bool, error) {
	h, version, err := readHeader(ctx, st, keys, db, k)
	if err != nil || h.Retired || h.EpochBump {
		return err == nil, err
	}
	b, err := h.Retire()
	if err != nil {
		return false, err
	}
	_, _ = st.Replace(ctx, keys.Commit(db, k), version, bytes.NewReader(b))
	h, _, err = readHeader(ctx, st, keys, db, k)
	return err == nil && h.Retired, err
}

// findGarbage lists the base objects of office o's database and returns at
// most limit keys that the garbage rule makes garbage, judged after each page
// of the listing by a manifest read begun after that page was listed: keys
// under the manifest's identifier, at or below its transaction number, that
// it does not reference. A manifest that office o does not hold ends it with
// ErrFenced.
func findGarbage(ctx context.Context, st storage.Store, keys format.Keys, o Office, limit int) ([]string, error) {
	var out []string
	var m format.Manifest
	var mv, after string
	for len(out) < limit {
		page, err := st.List(ctx, keys.BasePrefix(o.DB), after, storage.MaxListLimit)
		if err != nil || len(page) == 0 {
			return out, err
		}
		info, err := st.Head(ctx, keys.Manifest())
		if err != nil {
			return out, err
		}
		if info.Version != mv {
			b, info, err := readObject(ctx, st, keys.Manifest(), format.MaxManifestSize+1)
			if err != nil {
				return out, err
			}
			if m, err = format.DecodeManifest(b, o.DB); err != nil {
				return out, fmt.Errorf("engine: manifest: %w", err)
			}
			if !o.Holds(m) {
				return out, fmt.Errorf("%w: the manifest is at epoch %d, this leader's is %d", ErrFenced, m.Epoch, o.Epoch)
			}
			mv = info.Version
		}
		for _, e := range page {
			db, g, t, tag, err := keys.ParseBase(e.Key)
			if err != nil || db != m.DB || t > m.Txn {
				continue
			}
			if int64(g) >= int64(len(m.Groups)) || m.Groups[g] != (format.GroupRef{Txn: t, Tag: tag}) {
				if out = append(out, e.Key); len(out) == limit {
					break
				}
			}
		}
		if len(page) < storage.MaxListLimit {
			break
		}
		after = page[len(page)-1].Key
	}
	return out, nil
}

// coverage is a manifest transaction number the leader knows to have been
// current, read from storage or published by itself, and when it learned it.
type coverage struct {
	txn uint64
	at  time.Time
}

// compactLoop runs compaction, retirement and collection while the leader
// serves: when a commit wakes it, and every compactTick.
func (l *Leader) compactLoop() {
	defer l.wg.Done()
	tick := time.NewTicker(compactTick)
	defer tick.Stop()
	for {
		select {
		case <-l.cctx.Done():
			return
		case <-l.wake:
		case <-tick.C:
		}
		l.tend()
	}
}

// wakeLocked wakes the compaction loop, with l.cmu held, if the log above
// the manifest has reached a compaction threshold.
func (l *Leader) wakeLocked() {
	if l.compactWantedLocked() {
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
}

// compactWantedLocked reports, with l.cmu held, whether the log above the
// manifest has reached half the tail bound, a quarter of the page entries a
// reader indexes, or the configured bytes.
func (l *Leader) compactWantedLocked() bool {
	tail := l.head - l.manifestTxn
	return tail > 0 && l.compactRefused == nil &&
		(2*tail >= l.cfg.TailBound || 4*l.tailPages >= maxTailPages || l.tailBytes >= l.compactBytes() || l.tailFullLocked())
}

func (l *Leader) compactBytes() int64 { return cmp.Or(l.cfg.CompactBytes, defaultCompactBytes) }

func (l *Leader) grace() time.Duration { return cmp.Or(l.cfg.Grace, DefaultGrace) }

// tend does the compaction, retirement and collection work that is due.
func (l *Leader) tend() {
	l.cmpMu.Lock()
	defer l.cmpMu.Unlock()
	l.cmu.Lock()
	due := l.compactWantedLocked() || l.head > l.manifestTxn && l.compactRefused == nil &&
		time.Since(l.lastCompact) >= cmp.Or(l.cfg.CompactInterval, defaultCompactInterval)
	l.cmu.Unlock()
	if due {
		_ = l.compactLocked()
	}
	l.retireLocked()
	l.collectLocked()
}

// compactLocked runs one compaction attempt up to the base version, with
// l.cmpMu held, and installs its manifest. If the base version is behind
// the head it first tries a checkpoint that waits for nothing, so that the
// base version can advance without maintenance. An attempt that finds the
// manifest replaced, or that cannot resolve its own replacement, stops the
// leader: it is deposed, or may have two manifests.
func (l *Leader) compactLocked() error {
	ctx := l.cctx
	l.mu.RLock()
	t := l.tmpl.txn
	l.mu.RUnlock()
	if t < l.Head() {
		l.tryCheckpoint()
	}
	l.mu.RLock()
	a, t := l.tmpl.a, l.tmpl.txn
	cs := slices.Clone(l.tmpl.commits)
	l.mu.RUnlock()
	if t <= a.m.Txn {
		return nil
	}
	// Paced, and backing off after failed attempts, each of which leaves
	// base objects for the garbage rule.
	if d := time.Until(l.lastWrite.Add(compactPace << min(l.failures, 6))); d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	info, err := l.st.Head(ctx, l.keys.Manifest())
	if err != nil {
		return err
	}
	if info.Version != a.version {
		return l.stop(fmt.Errorf("%w: the manifest changed under the leader's own at %d", ErrFenced, a.m.Txn))
	}
	r := &Reader{st: l.st, keys: l.keys}
	r.reset(a, cs)
	k := &compaction{st: l.st, keys: l.keys, a: a, r: r, retired: l.retiredThrough}
	n, err := k.run(ctx)
	l.lastWrite = time.Now()
	l.failures++
	var stale *staleError
	switch {
	case err == nil:
		l.failures = 0
		l.install(n)
		return nil
	case errors.Is(err, format.ErrManifestTooLarge):
		l.cmu.Lock()
		l.compactRefused = err
		l.cmu.Unlock()
	case errors.Is(err, errCompactLost), errors.As(err, &stale):
		return l.stop(fmt.Errorf("%w: compaction to %d: %w", ErrFenced, t, err))
	case errors.Is(err, errCompactUnresolved):
		return l.stop(err)
	}
	return err
}

// install makes a published manifest the leader's: the base version's
// index is re-anchored on it, the tail above it is counted again, the
// header index is trimmed, and writes refused at the tail bound outside
// maintenance are readmitted once the tail is under it.
func (l *Leader) install(a anchor) {
	now := time.Now()
	l.mu.Lock()
	l.tmpl.reset(a, l.tmpl.commits)
	l.cmu.Lock()
	l.manifestTxn = a.m.Txn
	l.tailPages, l.tailBytes = 0, 0
	for _, c := range append(slices.Clip(l.tmpl.commits), l.pending...) {
		l.tailPages += int(c.h.PageCount)
		l.tailBytes += c.h.Size()
	}
	l.trimIndexLocked()
	if !l.maintaining && l.admitClosed && !l.tailFullLocked() {
		l.admitClosed, l.noCreates = false, false
	}
	l.cmu.Unlock()
	l.mu.Unlock()
	l.covered = append(l.covered, coverage{a.m.Txn, now})
	l.lastCompact, l.collectWanted = now, true
}

// trimIndexLocked drops, with l.cmu held, the headers more than maxTail
// numbers below the head from the lookup index, once the manifest has
// passed them.
func (l *Leader) trimIndexLocked() {
	floor := l.indexFloor
	if l.head > uint64(maxTail) { //nolint:gosec // a small positive constant
		floor = max(floor, min(l.manifestTxn, l.head-uint64(maxTail))) //nolint:gosec // as above
	}
	if floor == l.indexFloor {
		return
	}
	for id, t := range l.ids {
		if t <= floor {
			delete(l.ids, id)
		}
	}
	i := sort.Search(len(l.bumps), func(i int) bool { return l.bumps[i] > floor })
	l.bumps = append([]uint64(nil), l.bumps[i:]...)
	l.indexFloor = floor
}

// tryCheckpoint runs a TRUNCATE checkpoint on the leader's own connection
// that waits for no lock: if no reader uses the WAL and no writer holds its
// lock, the WAL is reset and the base version advances to the head.
func (l *Leader) tryCheckpoint() {
	_, _ = l.mconn.Run(l.cctx, nil, func(db *sqlite3.Conn) error {
		if err := db.BusyTimeout(0); err != nil {
			return err
		}
		_, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE)
		return errors.Join(err, db.BusyTimeout(busyTimeout))
	})
}

// retireLimit is the highest transaction number of a manifest the leader
// knows was current at least the grace period ago, or 0.
func (l *Leader) retireLimit(now time.Time) uint64 {
	i := 0
	for i < len(l.covered) && now.Sub(l.covered[i].at) >= l.grace() {
		i++
	}
	if i == 0 {
		return 0
	}
	l.covered = l.covered[i-1:]
	return l.covered[0].txn
}

// retireLocked retires, with l.cmpMu held, up to passBatch commits above
// the retired-through cursor that a manifest has covered for the grace
// period, retireWidth at a time, and advances the cursor over each one a
// read shows retired or a bump, up to the first that it does not.
func (l *Leader) retireLocked() {
	limit := l.retireLimit(time.Now())
	for done := uint64(0); done < passBatch && l.retiredThrough < limit && l.cctx.Err() == nil; {
		n := min(uint64(retireWidth), limit-l.retiredThrough, passBatch-done) //nolint:gosec // a small positive constant
		ok := make([]bool, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				r, err := retireCommit(l.cctx, l.st, l.keys, l.office.DB, l.retiredThrough+1+i)
				ok[i] = err == nil && r
			})
		}
		wg.Wait()
		for _, r := range ok {
			if !r {
				return
			}
			l.retiredThrough++
		}
		done += n
	}
}

// collectLocked deletes, with l.cmpMu held, up to passBatch keys of the
// garbage it found at least the grace period ago, and then, once none is
// left, after a compaction, looks for more. A delete's outcome needs no
// resolution: what it removes is garbage for good. Finding garbage holds the
// lock for one listing of up to maxGarbage keys.
func (l *Leader) collectLocked() {
	now := time.Now()
	if len(l.garbage) > 0 {
		if now.Sub(l.garbageAt) < l.grace() {
			return
		}
		for range min(passBatch, len(l.garbage)) {
			if l.cctx.Err() != nil {
				return
			}
			_ = l.st.Delete(l.cctx, l.garbage[0])
			l.garbage = l.garbage[1:]
		}
		if len(l.garbage) > 0 {
			return
		}
		l.garbage = nil
	}
	if !l.collectWanted {
		return
	}
	found, err := findGarbage(l.cctx, l.st, l.keys, l.office, maxGarbage)
	switch {
	case errors.Is(err, ErrFenced):
		_ = l.stop(err)
		return
	case err != nil:
		return
	}
	l.garbage, l.garbageAt, l.collectWanted = found, now, len(found) == maxGarbage
}
