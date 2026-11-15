package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/format"
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
	b, err := get(context.Background(), h.st, key)
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
