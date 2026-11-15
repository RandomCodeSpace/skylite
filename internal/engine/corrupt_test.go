package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"testing/iotest"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// corruptFixture is a database with a base at 3 and commits 4 to 6:
//
//	1  page 1 (size 1)       4  pages 5, 6
//	2  pages 1-12 (size 12)  5  pages 6, 10
//	3  pages 2, 7            6  page 11
//
// so page 1 is read from the base and page 10 from commit 5.
func corruptFixture(t *testing.T) (*testStore, format.Keys, *history) {
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 21, 512, 4)
	all := map[uint32][]byte{}
	for p := uint32(1); p <= 12; p++ {
		all[p] = h.bytes(512)
	}
	h.commit(12, all)
	h.commit(12, map[uint32][]byte{2: h.bytes(512), 7: h.bytes(512)})
	h.compact(3)
	h.commit(12, map[uint32][]byte{5: h.bytes(512), 6: h.bytes(512)})
	h.commit(12, map[uint32][]byte{6: h.bytes(512), 10: h.bytes(512)})
	h.commit(12, map[uint32][]byte{11: h.bytes(512)})
	return st, keys, h
}

func (h *history) read(key string) []byte {
	h.t.Helper()
	b, err := get(context.Background(), h.st, key, 1<<40)
	if err != nil {
		h.t.Fatal(err)
	}
	return b
}

// overwrite puts b at key, whatever is there.
func (h *history) overwrite(key string, b []byte) {
	h.t.Helper()
	info, err := h.st.Head(context.Background(), key)
	if err != nil {
		h.create(key, b)
		return
	}
	h.replace(key, info.Version, b)
}

func (h *history) flip(key string, off int) {
	h.t.Helper()
	b := h.read(key)
	b[off] ^= 0x10
	h.overwrite(key, b)
}

func (h *history) baseKey(g uint32) string {
	m, _ := h.manifest()
	return h.keys.Base(h.db, g, m.Groups[g].Txn, m.Groups[g].Tag)
}

// TestCorruptionIsNeverServed presents every corruption and misplacement
// the format and the record let a reader detect, and checks that it is
// reported and that nothing but the model's pages is ever returned.
func TestCorruptionIsNeverServed(t *testing.T) {
	other := format.DatabaseID{0xee}
	log := func(h *history, txn uint64) string { return h.keys.Commit(h.db, txn) }
	cases := []struct {
		name    string
		at      uint64 // transaction to open at; 0 for the head
		before  func(h *history)
		after   func(h *history) // after Open, before the reads
		advance bool
		want    error
	}{
		{name: "commit of another database", before: func(h *history) {
			b, _ := h.encodeCommit(5, other, 1, 12, map[uint32][]byte{6: h.bytes(512)}, false)
			h.overwrite(log(h, 5), b)
		}, want: format.ErrMisplaced},
		{name: "commit under another key", before: func(h *history) {
			h.overwrite(log(h, 5), h.read(log(h, 6)))
		}, want: format.ErrMisplaced},
		{name: "truncated commit", before: func(h *history) {
			b := h.read(log(h, 5))
			h.overwrite(log(h, 5), b[:len(b)-10])
		}, want: ErrCorrupt},
		{name: "truncated commit header", before: func(h *history) {
			h.overwrite(log(h, 5), h.read(log(h, 5))[:100])
		}, want: format.ErrInvalid},
		{name: "commit of an unknown version", before: func(h *history) {
			h.flip(log(h, 5), 9)
		}, want: format.ErrVersion},
		{name: "flipped bit in a commit header", before: func(h *history) {
			h.flip(log(h, 5), 20)
		}, want: format.ErrChecksum},
		{name: "flipped bit in a page table", before: func(h *history) {
			h.flip(log(h, 5), format.CommitHeaderSize+3)
		}, want: format.ErrChecksum},
		{name: "flipped bit in a commit page", before: func(h *history) {
			h.flip(log(h, 5), len(h.read(log(h, 5)))-10)
		}, want: format.ErrChecksum},
		{name: "flipped bit in a page record checksum", before: func(h *history) {
			h.flip(log(h, 5), len(h.read(log(h, 5)))-2)
		}, want: format.ErrChecksum},
		{name: "flipped bit in a base page", before: func(h *history) {
			h.flip(h.baseKey(0), format.BaseHeaderSize+100)
		}, want: format.ErrChecksum},
		{name: "base object under another key", before: func(h *history) {
			h.overwrite(h.baseKey(0), h.read(h.baseKey(1)))
		}, want: format.ErrChecksum},
		{name: "base object of another database", before: func(h *history) {
			m, _ := h.manifest()
			bh := format.BaseHeader{DB: other, PageSize: 512, GroupSize: 4, Group: 0, Txn: m.Groups[0].Txn, Tag: m.Groups[0].Tag, DBSize: 12}
			var out bytes.Buffer
			if err := format.EncodeBase(&out, bh, bytes.NewReader(bytes.Join(h.groupAt(3, 0), nil))); err != nil {
				h.t.Fatal(err)
			}
			h.overwrite(h.baseKey(0), out.Bytes())
		}, want: format.ErrChecksum},
		{name: "truncated base object", before: func(h *history) {
			b := h.read(h.baseKey(2))
			h.overwrite(h.baseKey(2), b[:len(b)-600])
		}, want: ErrCorrupt},
		{name: "manifest references a missing base object", before: func(h *history) {
			if err := h.st.Delete(context.Background(), h.baseKey(0)); err != nil {
				h.t.Fatal(err)
			}
		}, want: ErrCorrupt},
		{name: "flipped bit in the manifest", before: func(h *history) {
			h.flip(h.keys.Manifest(), 30)
		}, want: format.ErrChecksum},
		{name: "not a manifest", before: func(h *history) {
			h.overwrite(h.keys.Manifest(), h.read(log(h, 1)))
		}, want: format.ErrNotFormat},
		{name: "gap in the log below the head", at: 6, before: func(h *history) {
			if err := h.st.Delete(context.Background(), log(h, 5)); err != nil {
				h.t.Fatal(err)
			}
		}, want: ErrCorrupt},
		{name: "gap in the log just below the head", before: func(h *history) {
			if err := h.st.Delete(context.Background(), log(h, 5)); err != nil {
				h.t.Fatal(err)
			}
		}, want: ErrCorrupt},
		{name: "retired commit above the manifest", before: func(h *history) {
			b, err := h.headers[5].Retire()
			if err != nil {
				h.t.Fatal(err)
			}
			h.overwrite(log(h, 5), b)
		}, want: ErrCorrupt},
		{name: "epoch bump with another database size", before: func(h *history) {
			b, _ := h.encodeCommit(7, h.db, 9, 11, nil, true)
			h.create(log(h, 7), b)
		}, want: ErrCorrupt},
		{name: "commit with another page size", before: func(h *history) {
			h.ps = 1024
			b, _ := h.encodeCommit(7, h.db, 1, 12, map[uint32][]byte{3: h.bytes(1024)}, false)
			h.ps = 512
			h.create(log(h, 7), b)
		}, want: ErrCorrupt},
		{name: "indexed commit deleted", after: func(h *history) {
			if err := h.st.Delete(context.Background(), log(h, 5)); err != nil {
				h.t.Fatal(err)
			}
		}, want: ErrCorrupt},
		{name: "indexed commit replaced by another", after: func(h *history) {
			b, _ := h.encodeCommit(5, h.db, 1, 12, map[uint32][]byte{6: h.bytes(512), 10: h.bytes(512)}, false)
			h.overwrite(log(h, 5), b)
		}, want: ErrCorrupt},
		{name: "indexed commit retired with no manifest covering it", after: func(h *history) {
			b, err := h.headers[5].Retire()
			if err != nil {
				h.t.Fatal(err)
			}
			h.overwrite(log(h, 5), b)
		}, want: ErrCorrupt},
		{name: "manifest went backwards", advance: true, after: func(h *history) {
			m, _ := h.manifest()
			m.Txn, m.Groups = 2, make([]format.GroupRef, 3)
			h.publish(m)
		}, want: ErrCorrupt},
		{name: "database recreated under the prefix", advance: true, after: func(h *history) {
			m, _ := h.manifest()
			m.DB = other
			h.publish(m)
		}, want: format.ErrMisplaced},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			st, keys, h := corruptFixture(t)
			if c.before != nil {
				c.before(h)
			}
			var errs []error
			r, err := Open(ctx, st, keys, NewCache(0), c.at)
			if err != nil {
				errs = append(errs, err)
			} else {
				if c.after != nil {
					c.after(h)
				}
				if c.advance {
					if err := r.Advance(ctx); err != nil {
						errs = append(errs, err)
					}
				}
				want := h.state(r.Txn())
				for p := uint32(1); p <= r.Size(); p++ {
					got, err := r.ReadPage(ctx, p)
					if err != nil {
						errs = append(errs, err)
					} else if !bytes.Equal(got, want[p-1]) {
						t.Fatalf("page %d at %d served with the wrong content", p, r.Txn())
					}
				}
			}
			for _, err := range errs {
				if errors.Is(err, c.want) {
					return
				}
			}
			t.Fatalf("want an error wrapping %v, got %v", c.want, errs)
		})
	}
}

// TestNoFirstCommit: a manifest with no commit 1 under it is corruption.
func TestNoFirstCommit(t *testing.T) {
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 22, 512, 4)
	if err := h.st.Delete(context.Background(), keys.Commit(h.db, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), st, keys, NewCache(0), 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v", err)
	}
}

// TestGapTwoAboveTheHead: the probe reaches one name above the first absent
// one, so a gap of two names is the head, and the prefix is served.
func TestGapTwoAboveTheHead(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	h.commit(12, map[uint32][]byte{3: h.bytes(512)})
	for _, txn := range []uint64{5, 6} {
		if err := h.st.Delete(ctx, keys.Commit(h.db, txn)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil || r.Txn() != 4 {
		t.Fatalf("open: %v", err)
	}
	if err := h.checkReader(ctx, r, false); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, st, keys, NewCache(0), 7); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open above the gap: %v", err)
	}
}

// shortStore ends the body of the reads on selects early: halfway with
// err, or cleanly before the first byte if err is nil.
type shortStore struct {
	storage.Store
	on  func(key string, off int64) bool
	err error
}

func (s shortStore) short(rc io.ReadCloser, info storage.Info, err error, key string, off int64) (io.ReadCloser, storage.Info, error) {
	if err != nil || !s.on(key, off) {
		return rc, info, err
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	if s.err == nil {
		return io.NopCloser(bytes.NewReader(nil)), info, nil
	}
	return io.NopCloser(io.MultiReader(bytes.NewReader(b[:len(b)/2]), iotest.ErrReader(s.err))), info, nil
}

func (s shortStore) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	rc, info, err := s.Store.Get(ctx, key)
	return s.short(rc, info, err, key, 0)
}

func (s shortStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	rc, info, err := s.Store.GetRange(ctx, key, off, n)
	return s.short(rc, info, err, key, off)
}

// TestShortBodyIsStorageError ends the body of each kind of read early,
// with an error or cleanly: the read fails with a storage error, never one
// that says storage is corrupt.
func TestShortBodyIsStorageError(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	for name, on := range map[string]func(string, int64) bool{
		"manifest": func(k string, _ int64) bool { return k == keys.Manifest() },
		"header":   func(k string, off int64) bool { return k == keys.Commit(h.db, 4) && off == 0 },
		"table":    func(k string, off int64) bool { return k == keys.Commit(h.db, 4) && off == format.CommitHeaderSize },
		"commit page": func(k string, off int64) bool {
			return k == keys.Commit(h.db, 5) && off > format.CommitHeaderSize+2*format.PageEntrySize
		},
		"base page": func(k string, off int64) bool { return k == h.baseKey(0) && off >= format.BaseHeaderSize },
	} {
		for _, end := range []error{io.ErrUnexpectedEOF, nil} {
			ss := shortStore{st, on, end}
			r, err := Open(ctx, ss, keys, NewCache(0), 0)
			for p := uint32(1); err == nil && p <= r.Size(); p++ {
				_, err = r.ReadPage(ctx, p)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrCorrupt) || errors.Is(err, format.ErrInvalid) || errors.Is(err, format.ErrChecksum) {
				t.Errorf("%s ending with %v: %v", name, end, err)
			}
		}
	}
}

// splitStore hides key from GetRange only.
type splitStore struct {
	storage.Store
	key   string
	reads *int
}

func (s splitStore) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, storage.Info, error) {
	if key == s.key {
		*s.reads++
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: storage.ErrNotFound}
	}
	return s.Store.GetRange(ctx, key, off, n)
}

// TestHeadAndReadDisagree: a commit that Head finds and GetRange does not is
// read again once, then reported as corruption.
func TestHeadAndReadDisagree(t *testing.T) {
	for _, at := range []uint64{0, 6} {
		st, keys, h := corruptFixture(t)
		reads := 0
		_, err := Open(context.Background(), splitStore{st, keys.Commit(h.db, 5), &reads}, keys, NewCache(0), at)
		if !errors.Is(err, ErrCorrupt) || reads != 2 {
			t.Fatalf("open at %d: %d reads of commit 5: %v", at, reads, err)
		}
	}
}

// TestTailLimits lowers the reader's limits below the fixture's tail of
// three commits and five page entries.
func TestTailLimits(t *testing.T) {
	ctx := context.Background()
	defer func(n, p int) { maxTail, maxTailPages = n, p }(maxTail, maxTailPages)
	for _, limits := range [][2]int{{2, 1 << 22}, {1 << 16, 4}} {
		maxTail, maxTailPages = limits[0], limits[1]
		st, keys, h := corruptFixture(t)
		if _, err := Open(ctx, st, keys, NewCache(0), 0); !errors.Is(err, ErrLimit) {
			t.Fatalf("limits %v: open at the head: %v", limits, err)
		}
		r, err := Open(ctx, st, keys, NewCache(0), 5)
		if err != nil {
			t.Fatalf("limits %v: %v", limits, err)
		}
		if err := r.Advance(ctx); !errors.Is(err, ErrLimit) || r.Txn() != 5 {
			t.Fatalf("limits %v: advance to %d: %v", limits, r.Txn(), err)
		}
		if err := h.checkReader(ctx, r, false); err != nil {
			t.Fatal(err)
		}
		h.compact(6)
		if err := r.Advance(ctx); err != nil || r.Txn() != 6 {
			t.Fatalf("limits %v: advance after compaction to %d: %v", limits, r.Txn(), err)
		}
	}

	// Commits left behind by a re-anchoring no longer count: with room for
	// two entries, a walk that indexed commit 4 and then re-anchors on a
	// manifest at 5 indexes commit 6.
	maxTail, maxTailPages = 1<<16, 2
	st, keys, h := corruptFixture(t)
	fired := false
	st.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op == "getrange" && key == keys.Commit(h.db, 5) && !fired {
			fired = true
			h.compact(5)
			h.retireAll()
		}
		return false, nil
	})
	if r, err := Open(ctx, st, keys, NewCache(0), 0); err != nil || r.Txn() != 6 {
		t.Fatalf("open across a re-anchoring: %v", err)
	}
}

// TestBaseObjectGoneWithItsGroup: a compaction that shrinks the database
// drops a group, and collection deletes its object; a reader that needed it
// finds its transaction compacted.
func TestBaseObjectGoneWithItsGroup(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	r, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	h.commit(4, map[uint32][]byte{1: h.bytes(512)})
	h.compact(7)
	h.collect()
	if _, err := r.ReadPage(ctx, 12); !errors.Is(err, ErrCompacted) {
		t.Fatalf("page 12 at 6: %v", err)
	}
}

// TestTxnLimit: a reader serves a manifest at the largest transaction
// numbers, and refuses to look for a name above them.
func TestTxnLimit(t *testing.T) {
	ctx := context.Background()
	for _, txn := range []uint64{math.MaxUint64, math.MaxUint64 - 1} {
		st, keys := newStore(t)
		h := newHistory(t, st.Store, keys, 23, 512, 4)
		m, _ := h.manifest()
		m.Txn, m.DBSize, m.Groups = txn, 1, []format.GroupRef{{}}
		h.publish(m)
		if _, err := Open(ctx, st, keys, NewCache(0), 0); !errors.Is(err, ErrLimit) {
			t.Fatalf("open the head above %d: %v", txn, err)
		}
		r, err := Open(ctx, st, keys, NewCache(0), txn)
		if err != nil || r.Txn() != txn {
			t.Fatalf("open at %d: %v", txn, err)
		}
		if err := r.Advance(ctx); !errors.Is(err, ErrLimit) {
			t.Fatalf("advance from %d: %v", txn, err)
		}
	}
}

// TestManifestReadIsBounded: a manifest object larger than the format
// allows is read no further than one byte past the limit.
func TestManifestReadIsBounded(t *testing.T) {
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 6, 512, 4)
	_, v := h.manifest()
	h.replace(keys.Manifest(), v, make([]byte, 2*format.MaxManifestSize))
	st.resetCounts()
	if _, err := Open(context.Background(), st, keys, NewCache(0), 0); err == nil {
		t.Fatal("an oversized manifest opened")
	}
	if _, n := st.counts(); n > format.MaxManifestSize+1 {
		t.Fatalf("read %d bytes of the manifest", n)
	}
}

// TestManifestReplacementChecked: a replacement manifest that changes the
// page or group size, or whose database size disagrees with the commit the
// reader indexed at its transaction number, fails Advance, and the reader
// stays where it was.
func TestManifestReplacementChecked(t *testing.T) {
	for name, change := range map[string]func(m *format.Manifest){
		"page size":     func(m *format.Manifest) { m.Txn, m.PageSize = 4, 1024 },
		"group size":    func(m *format.Manifest) { m.Txn, m.GroupSize, m.Groups = 4, 8, m.Groups[:2] },
		"database size": func(m *format.Manifest) { m.Txn, m.DBSize = 5, 11 },
	} {
		ctx := context.Background()
		st, keys, h := corruptFixture(t)
		r, err := Open(ctx, st, keys, NewCache(0), 0)
		if err != nil {
			t.Fatal(err)
		}
		m, _ := h.manifest()
		change(&m)
		h.publish(m)
		if err := r.Advance(ctx); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: advance: %v", name, err)
		}
		if err := h.checkReader(ctx, r, false); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// A base object that is gone while a newer manifest still references it is
// corruption, not "compacted", even if that manifest is above the reader.
func TestMissingReferencedBase(t *testing.T) {
	ctx := context.Background()
	st, keys, h := corruptFixture(t)
	r, err := Open(ctx, st, keys, NewCache(0), 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.st.Delete(ctx, h.baseKey(0)); err != nil {
		t.Fatal(err)
	}
	h.compact(6)
	if _, err := r.ReadPage(ctx, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("page 1: %v", err)
	}
}

// An anchor gives back the manifest it was built from, group references
// included, because a manifest is written back from it.
func TestAnchorManifest(t *testing.T) {
	_, _, h := corruptFixture(t)
	m, v := h.manifest()
	if len(m.Groups) == 0 {
		t.Fatal("the fixture's manifest has no groups")
	}
	if got := newAnchor(m, v).manifest(); !reflect.DeepEqual(got, m) {
		t.Fatalf("manifest\n got %+v\nwant %+v", got, m)
	}
}
