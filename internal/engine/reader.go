// Package engine is skylite's storage engine: it keeps a database in object
// storage as decision record 0004 (docs/content/docs/decisions/0004-storage-engine.md)
// lays it out, in the objects package format encodes. This package so far
// creates a database (Create) and reads its pages as of a transaction
// number (Open, Reader).
//
// # Reading
//
// A Reader reads one state of the database: the state after one
// transaction number. Open reads the manifest, which names the base, the
// group objects the database's pages were last compacted into, current to
// the manifest's transaction number. It then reads the commit log above
// that number by direct reads of consecutive names, the header and then the
// page table of each commit, up to the requested transaction or, for the
// head, until a name is absent. The log is never listed. From those it
// keeps an index from page number to the newest commit that holds the page;
// it takes memory in proportion to the pages in the log above the manifest,
// which compaction keeps short, not to the size of the database.
//
// ReadPage serves a page from the newest commit at or below the reader's
// transaction that holds it, else from the page's group object in the base,
// else as zeros: a group the manifest gives no object, or a page that was
// beyond the database size at some commit after its last write, reads as
// zeros. Each page costs one ranged read of its record, verified by the
// record's checksum, which binds it to its page, object and database. Every
// header read is checked against its key, every object against the database
// identifier, and every log object's version token is pinned when it is
// indexed and checked on every later read of it that reaches storage; a
// cache hit returns the bytes of the version the reader pinned.
//
// Looking for the head, a reader probes one name above the first absent one:
// a present name there is a gap in the log. A reader walks at most maxTail
// commits and indexes at most maxTailPages page entries above the manifest,
// and fails closed with ErrLimit beyond them. Readers whose manifests hold
// equal group references share one copy of them.
//
// # Falling behind
//
// Compaction folds the log into a newer base and then retires the commits
// it covers and deletes base objects no manifest references any more. A
// reader that finds a retired commit above its manifest's transaction
// number, a log object whose version changed, or a missing base object
// re-reads the manifest. A newer manifest at or below the reader's
// transaction re-anchors it: the same state, built from the newer base and
// the commits above it that the reader has already indexed. A newer
// manifest above the reader's transaction means that state can no longer be
// built, and the read fails with ErrCompacted; Advance moves the reader on.
// A changed or missing object that no newer manifest explains is
// ErrCorrupt. Every re-anchoring needs a manifest with a higher transaction
// number than the last, so it cannot repeat without bound.
package engine

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"unique"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

var (
	// ErrCorrupt means storage holds something that breaks a rule relating
	// objects to each other: a gap in the log, a changed or missing object
	// no newer manifest explains, a manifest that went backwards or that
	// disagrees with the commit at its transaction number, or an object
	// whose size or page size contradicts its header or manifest. Errors the
	// decoders of package format find wrap their own classes instead.
	ErrCorrupt = errors.New("engine: storage contradicts the log or the manifest")
	// ErrCompacted means the requested transaction is below the base of the
	// current manifest, so its state can no longer be built.
	ErrCompacted = errors.New("engine: transaction is below the current base")
	// ErrBeyondHead means the requested transaction is not in the log.
	ErrBeyondHead = errors.New("engine: transaction is beyond the head of the log")
	// ErrPageRange means a page number is 0 or above the database size.
	ErrPageRange = errors.New("engine: page number out of range")
	// ErrLimit means the log above the manifest has more commits or page
	// entries than a reader indexes, or reaches the largest transaction
	// number. The reader fails closed; a compaction brings the log back
	// within the limits.
	ErrLimit = errors.New("engine: the log above the manifest is beyond the reader's limits")
)

// The reader's limits on the log above the manifest, record 0004 decision
// 10: twice the leader's tail bound of 32,768 commits and 2,097,152 page
// entries, so that the commit that crosses the leader's bound and the epoch
// bumps of failed takeovers still fit. Variables so that tests can lower
// them.
var (
	maxTail      = 1 << 16
	maxTailPages = 1 << 22
)

// Reader reads the pages of one database as of one transaction number. A
// Reader is not safe for concurrent use; many Readers may share a Store and
// a Cache.
type Reader struct {
	st    storage.Store
	keys  format.Keys
	cache *Cache

	a       anchor
	commits []*commit // the commits after a.m.Txn up to txn, in order
	txn     uint64
	size    uint32 // database size in pages at txn
	// baseLimit is the highest page the base may still supply: pages above
	// it were beyond the database size at some commit after a.m.Txn.
	baseLimit uint32
	pages     map[uint32]pageLoc // newest commit holding each page
	// floors are the running minima of the database size after a.m.Txn:
	// each is the smallest size of any commit from its transaction to txn,
	// so they rise with the transaction number.
	floors []floor
}

type floor struct {
	txn  uint64
	size uint32
}

// anchor is a manifest and the version token it was read at. Its group
// references are interned, so anchors with equal references share one copy;
// m.Groups is nil.
type anchor struct {
	m       format.Manifest
	groups  unique.Handle[string] // per group, its transaction and tag, little-endian
	version string
}

func newAnchor(m format.Manifest, version string) anchor {
	b := make([]byte, 0, 16*len(m.Groups))
	for _, g := range m.Groups {
		b = binary.LittleEndian.AppendUint64(b, g.Txn)
		b = binary.LittleEndian.AppendUint64(b, g.Tag)
	}
	m.Groups = nil
	return anchor{m: m, groups: unique.Make(string(b)), version: version}
}

func (a anchor) groupCount() uint32 { return uint32(len(a.groups.Value()) / 16) } //nolint:gosec // at most MaxPages groups

// group returns the reference of group g, which is below groupCount.
func (a anchor) group(g uint32) format.GroupRef {
	s := a.groups.Value()[16*int(g):]
	return format.GroupRef{Txn: binary.LittleEndian.Uint64([]byte(s[:8])), Tag: binary.LittleEndian.Uint64([]byte(s[8:16]))}
}

// commit is one indexed commit-log object.
type commit struct {
	key     string
	version string // pinned when indexed
	h       format.CommitHeader
	entries []format.PageEntry
}

type pageLoc struct {
	c *commit
	i int // index into c.entries
}

// staleError reports that an object the reader relies on has changed or
// gone: the log object at txn retired or replaced, or the base object ref
// of group, deleted. Only a newer manifest that covers it explains that.
type staleError struct {
	txn   uint64 // 0 for a base object
	group uint32
	ref   format.GroupRef
}

func (e *staleError) Error() string {
	if e.txn != 0 {
		return fmt.Sprintf("engine: commit %d changed", e.txn)
	}
	return fmt.Sprintf("engine: base object %d-%d-%016x missing", e.group, e.ref.Txn, e.ref.Tag)
}

func (e *staleError) explainedBy(a anchor) bool {
	if e.txn != 0 {
		return a.m.Txn >= e.txn
	}
	return e.group >= a.groupCount() || a.group(e.group) != e.ref
}

// Open opens the database under keys' prefix for reading as of transaction
// txn, or as of the head of the log if txn is 0. A txn below the current
// manifest's is ErrCompacted, and one above the head ErrBeyondHead. cache
// may be shared between Readers; a nil cache caches nothing.
func Open(ctx context.Context, st storage.Store, keys format.Keys, cache *Cache, txn uint64) (*Reader, error) {
	r := &Reader{st: st, keys: keys, cache: cache}
	a, err := r.readManifest(ctx, anchor{}, nil)
	if err != nil {
		return nil, err
	}
	if txn != 0 && txn < a.m.Txn {
		return nil, fmt.Errorf("%w: transaction %d, base at %d", ErrCompacted, txn, a.m.Txn)
	}
	a, cs, err := r.walk(ctx, a, nil, txn)
	if err != nil {
		return nil, err
	}
	r.reset(a, cs)
	if r.txn == 0 {
		return nil, fmt.Errorf("%w: database %x has no commit", ErrCorrupt, a.m.DB)
	}
	return r, nil
}

// Txn is the transaction number the reader reads as of.
func (r *Reader) Txn() uint64 { return r.txn }

// Size is the database size in pages as of Txn.
func (r *Reader) Size() uint32 { return r.size }

// PageSize is the database's page size in bytes.
func (r *Reader) PageSize() uint32 { return r.a.m.PageSize }

// ReadPage returns page pgno, from 1 to Size, as of Txn. The caller owns
// the returned slice. On an error the reader stays at Txn and can be used
// again.
func (r *Reader) ReadPage(ctx context.Context, pgno uint32) ([]byte, error) {
	if pgno == 0 || pgno > r.size {
		return nil, fmt.Errorf("%w: page %d of %d at transaction %d", ErrPageRange, pgno, r.size, r.txn)
	}
	for {
		body, err := r.readPage(ctx, pgno)
		var stale *staleError
		if !errors.As(err, &stale) {
			if err != nil {
				return nil, err
			}
			return bytes.Clone(body), nil
		}
		a, err := r.reanchor(ctx, r.a, stale, r.commits)
		if err != nil {
			return nil, err
		}
		if a.m.Txn > r.txn {
			return nil, fmt.Errorf("%w: transaction %d, base now at %d", ErrCompacted, r.txn, a.m.Txn)
		}
		r.reset(a, r.commits)
	}
}

// Advance moves the reader to the head of the log. It reads only the
// commits it has not indexed yet, and the manifest only if its version
// changed; pages still current stay cached. On an error the reader stays
// where it was.
func (r *Reader) Advance(ctx context.Context) error {
	a := r.a
	info, err := r.st.Head(ctx, r.keys.Manifest())
	if err != nil {
		return err
	}
	if info.Version != a.version {
		if a, err = r.readManifest(ctx, a, r.commits); err != nil {
			return err
		}
	}
	a, cs, err := r.walk(ctx, a, slices.Clip(after(r.commits, a.m.Txn)), 0)
	if err != nil {
		return err
	}
	if a.version != r.a.version {
		r.reset(a, cs)
		return nil
	}
	for _, c := range cs[len(r.commits):] {
		r.add(c)
	}
	return nil
}

// reset rebuilds the index from anchor a and the commits after it.
func (r *Reader) reset(a anchor, cs []*commit) {
	r.a, r.commits, r.pages, r.floors = a, nil, make(map[uint32]pageLoc), nil
	r.txn, r.size, r.baseLimit = a.m.Txn, a.m.DBSize, a.m.DBSize
	for _, c := range after(cs, a.m.Txn) {
		r.add(c)
	}
}

// add indexes c, which must be the commit after r.txn.
func (r *Reader) add(c *commit) {
	if r.txn == math.MaxUint64 || c.h.Txn != r.txn+1 {
		panic(fmt.Sprintf("engine: commit %d indexed after transaction %d", c.h.Txn, r.txn))
	}
	// Pages beyond a smaller size are gone; if the database grows again they
	// read as zeros until a commit writes them. The floors find the smallest
	// size since a commit without visiting the index.
	i := len(r.floors)
	for i > 0 && r.floors[i-1].size >= c.h.DBSize {
		i--
	}
	r.floors = append(r.floors[:i], floor{c.h.Txn, c.h.DBSize})
	r.baseLimit = min(r.baseLimit, c.h.DBSize)
	for i, e := range c.entries {
		r.pages[e.Pgno] = pageLoc{c, i}
	}
	r.commits = append(r.commits, c)
	r.txn, r.size = c.h.Txn, c.h.DBSize
}

// current reports whether page pgno as commit txn wrote it is still the
// page: no commit after txn made the database smaller than pgno.
func (r *Reader) current(pgno uint32, txn uint64) bool {
	i, _ := slices.BinarySearchFunc(r.floors, txn+1, func(f floor, t uint64) int { return cmp.Compare(f.txn, t) })
	return i == len(r.floors) || r.floors[i].size >= pgno
}

// after returns the commits of cs, which are in order, above txn.
func after(cs []*commit, txn uint64) []*commit {
	for len(cs) > 0 && cs[0].h.Txn <= txn {
		cs = cs[1:]
	}
	return cs
}

// at returns the commit of cs, which are consecutive, at txn, or nil.
func at(cs []*commit, txn uint64) *commit {
	if len(cs) == 0 || txn < cs[0].h.Txn || txn-cs[0].h.Txn >= uint64(len(cs)) {
		return nil
	}
	return cs[txn-cs[0].h.Txn]
}

func entryCount(cs []*commit) int {
	n := 0
	for _, c := range cs {
		n += len(c.entries)
	}
	return n
}

// walk reads the commits after those in cs (after a's transaction if cs is
// empty) up to target, or up to the head if target is 0, and returns the
// anchor and commits to index. A retired or changed commit sends it to a
// newer manifest.
func (r *Reader) walk(ctx context.Context, a anchor, cs []*commit, target uint64) (anchor, []*commit, error) {
	entries := entryCount(cs)
	var retried uint64 // the name Head and GetRange last disagreed on
	for {
		last := a.m.Txn + uint64(len(cs))
		if target != 0 && last >= target {
			return a, cs, nil
		}
		if last >= math.MaxUint64-1 {
			return anchor{}, nil, fmt.Errorf("%w: transaction numbers end at %d", ErrLimit, last)
		}
		next := last + 1
		prevSize := a.m.DBSize
		if len(cs) > 0 {
			prevSize = cs[len(cs)-1].h.DBSize
		}
		c, err := r.fetchCommit(ctx, a.m, next, prevSize)
		var stale *staleError
		switch {
		case err == nil:
			if len(cs) >= maxTail || entries+len(c.entries) > maxTailPages {
				return anchor{}, nil, fmt.Errorf("%w: more than %d commits or %d page entries above the manifest at %d",
					ErrLimit, maxTail, maxTailPages, a.m.Txn)
			}
			cs = append(cs, c)
			entries += len(c.entries)
		case errors.Is(err, storage.ErrNotFound):
			// One name above the first absent one tells a gap from the head.
			probe := target
			if target == 0 {
				probe = next + 1
			}
			if probe != next {
				_, err = r.st.Head(ctx, r.keys.Commit(a.m.DB, probe))
			}
			switch {
			case probe == next || errors.Is(err, storage.ErrNotFound):
				if target == 0 {
					return a, cs, nil
				}
				return anchor{}, nil, fmt.Errorf("%w: transaction %d, head at %d", ErrBeyondHead, target, last)
			case err != nil:
				return anchor{}, nil, err
			}
			// The log is a contiguous prefix, so next was created before
			// probe, which exists: still absent now, it is a gap.
			_, err = r.st.Head(ctx, r.keys.Commit(a.m.DB, next))
			switch {
			case err == nil && retried != next:
				retried = next
			case err == nil:
				return anchor{}, nil, fmt.Errorf("%w: commit %d exists to Head and is absent to a read", ErrCorrupt, next)
			case errors.Is(err, storage.ErrNotFound):
				return anchor{}, nil, fmt.Errorf("%w: commit %d missing below commit %d", ErrCorrupt, next, probe)
			default:
				return anchor{}, nil, err
			}
		case errors.As(err, &stale):
			if a, err = r.reanchor(ctx, a, stale, cs); err != nil {
				return anchor{}, nil, err
			}
			if target != 0 && a.m.Txn > target {
				return anchor{}, nil, fmt.Errorf("%w: transaction %d, base now at %d", ErrCompacted, target, a.m.Txn)
			}
			cs = after(cs, a.m.Txn)
			entries = entryCount(cs)
		default:
			return anchor{}, nil, err
		}
	}
}

// fetchCommit reads and checks the header and page table of commit txn,
// which follows a commit of prevSize pages.
func (r *Reader) fetchCommit(ctx context.Context, m format.Manifest, txn uint64, prevSize uint32) (*commit, error) {
	key := r.keys.Commit(m.DB, txn)
	b, info, err := r.getRange(ctx, key, 0, format.CommitHeaderSize)
	if err != nil {
		return nil, err
	}
	h, err := format.DecodeCommitHeader(b, m.DB)
	if err == nil {
		err = h.CheckKey(m.DB, txn)
	}
	if err != nil {
		return nil, fmt.Errorf("engine: commit %d: %w", txn, err)
	}
	switch {
	case h.Retired:
		return nil, &staleError{txn: txn}
	case h.PageSize != m.PageSize:
		return nil, fmt.Errorf("%w: commit %d has page size %d, database %d", ErrCorrupt, txn, h.PageSize, m.PageSize)
	case info.Size != h.Size():
		return nil, fmt.Errorf("%w: commit %d is %d bytes, header says %d", ErrCorrupt, txn, info.Size, h.Size())
	case h.EpochBump && h.DBSize != prevSize:
		return nil, fmt.Errorf("%w: epoch bump %d has database size %d after %d", ErrCorrupt, txn, h.DBSize, prevSize)
	}
	c := &commit{key: key, version: info.Version, h: h}
	if h.EpochBump {
		return c, nil
	}
	b, info, err = r.getRange(ctx, key, format.CommitHeaderSize, h.TableSize())
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return nil, fmt.Errorf("%w: commit %d deleted", ErrCorrupt, txn)
	case err != nil:
		return nil, err
	case info.Version != c.version:
		return nil, &staleError{txn: txn}
	}
	if c.entries, err = format.DecodeCommitTable(h, b); err != nil {
		return nil, fmt.Errorf("engine: commit %d: %w", txn, err)
	}
	return c, nil
}

// source is where a page is read from: a commit's entry, a base object, or
// neither, for zeros.
type source struct {
	c    *commit
	e    format.PageEntry
	base format.BaseHeader // Txn is 0 if the page is not in a base object
}

// locate finds where page pgno, which is in range, is read from. It reads
// only the index, never storage.
func (r *Reader) locate(pgno uint32) source {
	if loc, ok := r.pages[pgno]; ok && r.current(pgno, loc.c.h.Txn) {
		return source{c: loc.c, e: loc.c.entries[loc.i]}
	}
	m := r.a.m
	if pgno > r.baseLimit {
		return source{}
	}
	g := (pgno - 1) / m.GroupSize
	ref := r.a.group(g)
	if ref.Txn == 0 {
		return source{}
	}
	return source{base: format.BaseHeader{DB: m.DB, PageSize: m.PageSize, GroupSize: m.GroupSize, Group: g, Txn: ref.Txn, Tag: ref.Tag}}
}

// readPage returns page pgno, which is in range, possibly shared with the
// cache.
func (r *Reader) readPage(ctx context.Context, pgno uint32) ([]byte, error) {
	switch s := r.locate(pgno); {
	case s.c != nil:
		return r.commitPage(ctx, s.c, s.e)
	case s.base.Txn != 0:
		return r.basePage(ctx, s.base, pgno)
	}
	return make([]byte, r.a.m.PageSize), nil
}

func (r *Reader) basePage(ctx context.Context, h format.BaseHeader, pgno uint32) ([]byte, error) {
	off, n, err := h.PageRange(pgno)
	if err != nil {
		return nil, err
	}
	key := r.keys.Base(h.DB, h.Group, h.Txn, h.Tag)
	// A base key's content never changes, so its pages need no version.
	ck := cacheKey(key, "", off)
	if body, ok := r.cache.get(ck); ok {
		return body, nil
	}
	b, info, err := r.getRange(ctx, key, off, n)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return nil, &staleError{group: h.Group, ref: format.GroupRef{Txn: h.Txn, Tag: h.Tag}}
	case err != nil:
		return nil, err
	case info.Size != h.Size():
		return nil, fmt.Errorf("%w: base object %s is %d bytes, want %d", ErrCorrupt, key, info.Size, h.Size())
	}
	body, err := format.DecodeBasePage(h, pgno, b)
	if err != nil {
		return nil, fmt.Errorf("engine: base object %s: %w", key, err)
	}
	r.cache.put(ck, body)
	return body, nil
}

func (r *Reader) commitPage(ctx context.Context, c *commit, e format.PageEntry) ([]byte, error) {
	off := int64(e.Offset) //nolint:gosec // DecodeCommitTable bounds it by MaxCommitSize
	ck := cacheKey(c.key, c.version, off)
	if body, ok := r.cache.get(ck); ok {
		return body, nil
	}
	b, info, err := r.getRange(ctx, c.key, off, int64(e.Length))
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return nil, fmt.Errorf("%w: commit %d deleted", ErrCorrupt, c.h.Txn)
	case err != nil:
		return nil, err
	case info.Version != c.version:
		return nil, &staleError{txn: c.h.Txn}
	}
	body, err := format.DecodeCommitPage(c.h, e, b)
	if err != nil {
		return nil, fmt.Errorf("engine: commit %d: %w", c.h.Txn, err)
	}
	r.cache.put(ck, body)
	return body, nil
}

func cacheKey(key, version string, off int64) string {
	return key + "\x00" + version + "\x00" + strconv.FormatInt(off, 10)
}

// readManifest reads the manifest. With a zero prev it discovers the
// database; otherwise the manifest must be a successor of prev's and agree
// with cs, the commits the reader has indexed after prev's transaction.
func (r *Reader) readManifest(ctx context.Context, prev anchor, cs []*commit) (anchor, error) {
	rc, info, err := r.st.Get(ctx, r.keys.Manifest())
	if err != nil {
		return anchor{}, err
	}
	b, err := readBody(rc, info, 0, format.MaxManifestSize+1)
	if err != nil {
		return anchor{}, err
	}
	m, err := format.DecodeManifest(b, prev.m.DB)
	if err != nil {
		return anchor{}, fmt.Errorf("engine: manifest: %w", err)
	}
	if prev.version != "" && (m.PageSize != prev.m.PageSize || m.GroupSize != prev.m.GroupSize || m.Txn < prev.m.Txn) {
		return anchor{}, fmt.Errorf("%w: manifest at %d (page size %d, group size %d) replaced one at %d (%d, %d)",
			ErrCorrupt, m.Txn, m.PageSize, m.GroupSize, prev.m.Txn, prev.m.PageSize, prev.m.GroupSize)
	}
	if c := at(cs, m.Txn); c != nil && c.h.DBSize != m.DBSize {
		return anchor{}, fmt.Errorf("%w: manifest at %d has database size %d, the commit %d", ErrCorrupt, m.Txn, m.DBSize, c.h.DBSize)
	}
	return newAnchor(m, info.Version), nil
}

// reanchor reads the manifest that must explain stale, which a reader
// anchored on a found with cs indexed above a.
func (r *Reader) reanchor(ctx context.Context, a anchor, stale *staleError, cs []*commit) (anchor, error) {
	n, err := r.readManifest(ctx, a, cs)
	if err != nil {
		return anchor{}, err
	}
	// Only a compaction changes what the manifest covers, and it raises
	// the transaction number; requiring that bounds re-anchoring.
	if n.m.Txn <= a.m.Txn || !stale.explainedBy(n) {
		return anchor{}, fmt.Errorf("%w: %v, and the manifest is at %d", ErrCorrupt, stale, n.m.Txn) //nolint:errorlint // wrapping stale would make callers re-anchor again
	}
	return n, nil
}

// getRange reads n bytes at off of key, fewer only if the object ends
// sooner.
func (r *Reader) getRange(ctx context.Context, key string, off, n int64) ([]byte, storage.Info, error) {
	rc, info, err := r.st.GetRange(ctx, key, off, n)
	if err != nil {
		return nil, storage.Info{}, err
	}
	b, err := readBody(rc, info, off, n)
	if err != nil {
		return nil, storage.Info{}, err
	}
	return b, info, nil
}

// readBody reads and closes the body of a read of n bytes at off of the
// object info describes. A body that ends before the object does is a
// failed read, never a short object: its error is the storage's, or
// io.ErrUnexpectedEOF.
func readBody(rc io.ReadCloser, info storage.Info, off, n int64) ([]byte, error) {
	defer func() { _ = rc.Close() }()
	b := make([]byte, min(n, max(0, info.Size-off)))
	got, err := io.ReadFull(rc, b)
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("engine: body ended after %d of %d bytes: %w", got, len(b), io.ErrUnexpectedEOF)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}
