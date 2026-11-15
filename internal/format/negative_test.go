package format

import (
	"bytes"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"testing"
)

type mutation struct {
	name string
	base func() []byte
	mut  func(b []byte) []byte
	want error
}

func set16(off int, v uint16) func([]byte) []byte {
	return func(b []byte) []byte { le.PutUint16(b[off:], v); return b }
}

func set32(off int, v uint32) func([]byte) []byte {
	return func(b []byte) []byte { le.PutUint32(b[off:], v); return b }
}

func set64(off int, v uint64) func([]byte) []byte {
	return func(b []byte) []byte { le.PutUint64(b[off:], v); return b }
}

func flip(off int) func([]byte) []byte {
	return func(b []byte) []byte { b[off] ^= 0x40; return b }
}

func cut(n int) func([]byte) []byte { return func(b []byte) []byte { return b[:n] } }

func setDB(off int, db DatabaseID) func([]byte) []byte {
	return func(b []byte) []byte { copy(b[off:], db[:]); return b }
}

// swap exchanges the n bytes at i and j.
func swap(i, j, n int) func([]byte) []byte {
	return func(b []byte) []byte {
		x := slices.Clone(b[i : i+n])
		copy(b[i:], b[j:j+n])
		copy(b[j:], x)
		return b
	}
}

// then applies m and then fix, so the mutation reaches the checks behind
// the checksums.
func then(m, fix func([]byte) []byte) func([]byte) []byte {
	return func(b []byte) []byte { return fix(m(b)) }
}

func TestCommitRejects(t *testing.T) {
	plain := func() []byte { return goldenCommit().encode(t) }
	bump := func() []byte { return goldenBump().encode(t) }
	lock := func() []byte { return lockCommit().encode(t) }
	const table = CommitHeaderSize
	const page0 = CommitHeaderSize + 2*PageEntrySize
	fixed := func(m func([]byte) []byte) func([]byte) []byte { return then(m, refixCommit) }
	entry := func(i int, field int, v uint32) func([]byte) []byte {
		return fixed(set32(table+i*PageEntrySize+field, v))
	}

	cases := []mutation{
		{"empty", plain, cut(0), ErrNotFormat},
		{"short magic", plain, cut(7), ErrNotFormat},
		{"other magic", plain, flip(7), ErrNotFormat},
		{"base object", func() []byte { h, p := goldenBase(); return encodeBase(t, h, p) }, nil, ErrNotFormat},
		{"magic only", plain, cut(8), ErrInvalid},
		{"half a version", plain, cut(9), ErrInvalid},
		{"version 2", plain, set16(8, 2), ErrVersion},
		{"version 0", plain, set16(8, 0), ErrVersion},
		{"version 2, nothing after it", plain, then(set16(8, 2), cut(10)), ErrVersion},
		{"version 1, nothing after it", plain, cut(10), ErrInvalid},
		{"truncated header", plain, cut(CommitHeaderSize - 1), ErrInvalid},
		{"header corrupt", plain, flip(20), ErrChecksum},
		{"database identifier corrupt", plain, flip(60), ErrChecksum},
		{"header checksum corrupt", plain, flip(141), ErrChecksum},
		{"codec set", plain, fixed(set16(10, 1<<1)), ErrVersion},
		{"unknown flag", plain, fixed(set16(10, 1<<15)), ErrVersion},
		{"page size not a power of two", plain, fixed(set32(12, 1000)), ErrInvalid},
		{"page size too small", plain, fixed(set32(12, 256)), ErrInvalid},
		{"page size too large", plain, fixed(set32(12, 131072)), ErrInvalid},
		{"transaction 0", plain, fixed(set64(16, 0)), ErrInvalid},
		{"epoch 0", plain, fixed(set64(24, 0)), ErrInvalid},
		{"database too large", plain, fixed(set32(32, math.MaxUint32)), ErrInvalid},
		{"more pages than database", plain, fixed(set32(32, 1)), ErrInvalid},
		{"table size mismatch", plain, fixed(set64(40, 48)), ErrInvalid},
		{"commit id too long", plain, fixed(func(b []byte) []byte { b[68] = MaxCommitIDLen + 1; return b }), ErrInvalid},
		{"commit id length 255", plain, fixed(func(b []byte) []byte { b[68] = 255; return b }), ErrInvalid},
		{"commit id padding", plain, fixed(func(b []byte) []byte { b[69+len("client-42")] = 1; return b }), ErrInvalid},
		{"commit id padding end", plain, fixed(func(b []byte) []byte { b[69+MaxCommitIDLen-1] = 1; return b }), ErrInvalid},
		{"reserved byte", plain, fixed(func(b []byte) []byte { b[135] = 1; return b }), ErrInvalid},
		{"last reserved byte", plain, fixed(func(b []byte) []byte { b[139] = 1; return b }), ErrInvalid},
		{"database identifier zero", plain, fixed(setDB(52, DatabaseID{})), ErrInvalid},
		{"other database", plain, fixed(setDB(52, otherDB)), ErrMisplaced},
		{"other database, bump", bump, fixed(setDB(52, otherDB)), ErrMisplaced},
		{"epoch bump with pages", plain, fixed(set16(10, flagEpochBump)), ErrInvalid},
		{"commit without pages", bump, fixed(set16(10, 0)), ErrInvalid},
		{"table corrupt", plain, flip(table + 1), ErrChecksum},
		{"truncated table", plain, cut(table + PageEntrySize + 3), ErrInvalid},
		{"pages out of order", plain, then(entry(0, 0, 3), entry(1, 0, 1)), ErrInvalid},
		{"duplicate page", plain, entry(1, 0, 1), ErrInvalid},
		{"page 0", plain, entry(0, 0, 0), ErrInvalid},
		{"page beyond database", plain, entry(1, 0, 4), ErrInvalid},
		{"lock page", lock, entry(0, 0, 16385), ErrInvalid},
		{"record length", plain, entry(0, 4, 512), ErrInvalid},
		{"record offset", plain, entry(1, 8, page0), ErrInvalid},
		{"page corrupt", plain, flip(page0 + 100), ErrChecksum},
		{"page checksum corrupt", plain, flip(page0 + 512), ErrChecksum},
		{"records swapped", plain, swap(page0, page0+516, 516), ErrChecksum},
		{"record of page 3 in both places", plain, func(b []byte) []byte { copy(b[page0:], b[page0+516:page0+1032]); return b }, ErrChecksum},
		{"record under another page number", plain, then(set32(table, 2), refixHeader), ErrChecksum},
		{"records of another transaction", plain, then(set64(16, 8), refixHeader), ErrChecksum},
		{"records of another epoch", plain, then(set64(24, 3), refixHeader), ErrChecksum},
		{"records of another database", plain, then(setDB(52, otherDB), refixHeader), ErrMisplaced},
		{"truncated page", plain, cut(page0 + 600), ErrInvalid},
		{"one byte short", plain, func(b []byte) []byte { return b[:len(b)-1] }, ErrInvalid},
		{"trailing byte", plain, func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
		{"trailing byte after bump", bump, func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := c.base()
			if c.mut != nil {
				b = c.mut(b)
			}
			_, _, err := DecodeCommit(bytes.NewReader(b), goldenDB, nil)
			wantClass(t, err, c.want)
			// The range decoders see the same failure in their own range.
			if len(b) <= CommitHeaderSize {
				_, err := DecodeCommitHeader(b, goldenDB)
				wantClass(t, err, c.want)
			}
		})
	}
}

// refixHeader recomputes a commit's table and header checksums but not its
// records', so records written for one header are presented under another.
func refixHeader(b []byte) []byte {
	n := CommitHeaderSize + PageEntrySize*int(le.Uint32(b[36:]))
	le.PutUint32(b[48:], checksum(b[CommitHeaderSize:n]))
	le.PutUint32(b[140:], checksum(b[:140]))
	return b
}

func TestCommitRangeDecodersReject(t *testing.T) {
	b := goldenCommit().encode(t)
	h, err := DecodeCommitHeader(b[:CommitHeaderSize], goldenDB)
	if err != nil {
		t.Fatal(err)
	}
	tb := b[CommitHeaderSize : CommitHeaderSize+h.TableSize()]
	entries, err := DecodeCommitTable(h, tb)
	if err != nil {
		t.Fatal(err)
	}
	e := entries[0]
	rec := b[e.Offset : e.Offset+uint64(e.Length)]

	built := h
	built.tableCRC = 0 // a header not produced by DecodeCommitHeader
	badHeader := h
	badHeader.PageSize = 3
	cases := []struct {
		name string
		err  func() error
		want error
	}{
		{"header with trailing bytes", func() error { _, err := DecodeCommitHeader(b[:CommitHeaderSize+1], goldenDB); return err }, ErrInvalid},
		{"header of another database", func() error { _, err := DecodeCommitHeader(b[:CommitHeaderSize], otherDB); return err }, ErrMisplaced},
		{"table with a hand-built header", func() error { _, err := DecodeCommitTable(built, tb); return err }, ErrChecksum},
		{"table with an invalid header", func() error { _, err := DecodeCommitTable(badHeader, tb); return err }, ErrInvalid},
		{"table too long", func() error {
			_, err := DecodeCommitTable(h, b[CommitHeaderSize:CommitHeaderSize+h.TableSize()+1])
			return err
		}, ErrInvalid},
		{"page too short", func() error { _, err := DecodeCommitPage(h, e, rec[:len(rec)-1]); return err }, ErrInvalid},
		{"page too long", func() error { _, err := DecodeCommitPage(h, e, b[e.Offset:e.Offset+uint64(e.Length)+1]); return err }, ErrInvalid},
		{"entry length", func() error { e := e; e.Length--; _, err := DecodeCommitPage(h, e, rec); return err }, ErrInvalid},
		{"page from the wrong range", func() error { _, err := DecodeCommitPage(h, e, b[e.Offset+1:e.Offset+1+uint64(e.Length)]); return err }, ErrChecksum},
		{"page with an invalid header", func() error { _, err := DecodeCommitPage(badHeader, e, rec); return err }, ErrInvalid},
	}
	// Entries DecodeCommitTable cannot return, and records of the wrong page.
	e1 := entries[1]
	rec1 := b[e1.Offset : e1.Offset+uint64(e1.Length)]
	withEntry := func(f func(*PageEntry)) PageEntry { x := e; f(&x); return x }
	lc := lockCommit()
	lb := lc.encode(t)
	lh, err := DecodeCommitHeader(lb[:CommitHeaderSize], goldenDB)
	if err != nil {
		t.Fatal(err)
	}
	lockEntry := PageEntry{Pgno: LockPage(65536), Length: 65540, Offset: CommitHeaderSize + 32}
	start := CommitHeaderSize + h.tableSize()
	for name, x := range map[string]PageEntry{
		"page 0":                 withEntry(func(e *PageEntry) { e.Pgno = 0 }),
		"page beyond database":   withEntry(func(e *PageEntry) { e.Pgno = 4 }),
		"offset in the header":   withEntry(func(e *PageEntry) { e.Offset = 0 }),
		"offset in the table":    withEntry(func(e *PageEntry) { e.Offset = start - 516 }),
		"offset one before":      withEntry(func(e *PageEntry) { e.Offset = start - 1 }),
		"offset one after":       withEntry(func(e *PageEntry) { e.Offset = start + 1 }),
		"offset mid record":      withEntry(func(e *PageEntry) { e.Offset = start + 258 }),
		"offset past last page":  withEntry(func(e *PageEntry) { e.Offset = start + 2*516 }),
		"offset at the end":      withEntry(func(e *PageEntry) { e.Offset = math.MaxUint64 }),
		"offset below the start": withEntry(func(e *PageEntry) { e.Offset = start - 2*516 }),
	} {
		cases = append(cases, struct {
			name string
			err  func() error
			want error
		}{"entry " + name, func() error { _, err := DecodeCommitPage(h, x, rec); return err }, ErrInvalid})
	}
	cases = append(cases, []struct {
		name string
		err  func() error
		want error
	}{
		{"entry lock page", func() error {
			_, err := DecodeCommitPage(lh, lockEntry, lb[lockEntry.Offset:lockEntry.Offset+65540])
			return err
		}, ErrInvalid},
		{"record of page 3 as page 1", func() error { _, err := DecodeCommitPage(h, e, rec1); return err }, ErrChecksum},
		{"record of page 1 as page 3", func() error { _, err := DecodeCommitPage(h, e1, rec); return err }, ErrChecksum},
		{"record of page 1 as page 2", func() error {
			_, err := DecodeCommitPage(h, withEntry(func(e *PageEntry) { e.Pgno = 2 }), rec)
			return err
		}, ErrChecksum},
		{"page 1 at page 3's position", func() error {
			_, err := DecodeCommitPage(h, withEntry(func(e *PageEntry) { e.Offset = e1.Offset }), rec1)
			return err
		}, ErrChecksum},
		{"record of another transaction", func() error { x := h; x.Txn++; _, err := DecodeCommitPage(x, e, rec); return err }, ErrChecksum},
		{"record of another epoch", func() error { x := h; x.Epoch++; _, err := DecodeCommitPage(x, e, rec); return err }, ErrChecksum},
		{"record of another database", func() error { x := h; x.DB = otherDB; _, err := DecodeCommitPage(x, e, rec); return err }, ErrChecksum},
		{"record of a base object", func() error {
			r := slices.Clone(rec)
			le.PutUint32(r[512:], recordCRC(baseMagic, h.DB, h.Txn, h.Epoch, uint64(e.Pgno), r[:512]))
			_, err := DecodeCommitPage(h, e, r)
			return err
		}, ErrChecksum},
	}...)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { wantClass(t, c.err(), c.want) })
	}
}

func TestEncodeCommitRejects(t *testing.T) {
	g := goldenCommit()
	l := lockCommit()
	with := func(c commitCase, f func(*commitCase)) commitCase { f(&c); return c }
	cases := []struct {
		name string
		c    commitCase
	}{
		{"database identifier zero", with(g, func(c *commitCase) { c.h.DB = DatabaseID{} })},
		{"page size", with(g, func(c *commitCase) { c.h.PageSize = 4095 })},
		{"transaction 0", with(g, func(c *commitCase) { c.h.Txn = 0 })},
		{"epoch 0", with(g, func(c *commitCase) { c.h.Epoch = 0 })},
		{"database too large", with(g, func(c *commitCase) { c.h.DBSize = MaxPages + 1 })},
		{"more pages than database", with(g, func(c *commitCase) { c.h.DBSize = 1 })},
		{"commit id too long", with(g, func(c *commitCase) { c.h.CommitID = make([]byte, MaxCommitIDLen+1) })},
		{"epoch bump with pages", with(g, func(c *commitCase) { c.h.EpochBump = true })},
		{"commit without pages", with(goldenBump(), func(c *commitCase) { c.h.EpochBump = false })},
		{"page count mismatch", with(g, func(c *commitCase) { c.pgnos = []uint32{1} })},
		{"more pages than the count", with(g, func(c *commitCase) { c.pgnos = []uint32{1, 2, 3}; c.pages = pattern(3*512, 1) })},
		{"epoch bump with one page", with(g, func(c *commitCase) { c.h.EpochBump, c.h.PageCount, c.pgnos = true, 1, []uint32{1} })},
		{"not ascending", with(g, func(c *commitCase) { c.pgnos = []uint32{3, 1} })},
		{"duplicate", with(g, func(c *commitCase) { c.pgnos = []uint32{1, 1} })},
		{"page 0", with(g, func(c *commitCase) { c.pgnos = []uint32{0, 1} })},
		{"beyond database", with(g, func(c *commitCase) { c.pgnos = []uint32{1, 4} })},
		{"lock page", with(l, func(c *commitCase) { c.pgnos = []uint32{16385, 16386} })},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := EncodeCommit(io.Discard, c.c.h, c.c.pgnos, bytes.NewReader(c.c.pages))
			wantClass(t, err, ErrInvalid)
		})
	}
}

// failWriter accepts n bytes and then fails.
type failWriter struct{ n int }

func (w *failWriter) Write(p []byte) (int, error) {
	if len(p) > w.n {
		k := w.n
		w.n = 0
		return k, errBoom
	}
	w.n -= len(p)
	return len(p), nil
}

// TestEncodersPassCallerErrors fails the writer after every possible number
// of bytes and the page source after every page boundary and inside a
// page, for objects larger than the encoders' buffer, and checks that the
// caller's error comes back and is in no format class.
func TestEncodersPassCallerErrors(t *testing.T) {
	big := commitCase{h: CommitHeader{DB: goldenDB, PageSize: 4096, Txn: 1, Epoch: 1, DBSize: 40, PageCount: 40}}
	for p := range uint32(40) {
		big.pgnos = append(big.pgnos, p+1)
	}
	big.pages = pattern(40*4096, 9)
	bigBase := BaseHeader{DB: goldenDB, PageSize: 4096, GroupSize: 32, Group: 0, Txn: 1, Epoch: 1, DBSize: 32}
	basePages := pattern(32*4096, 5)

	encoders := []struct {
		name  string
		size  int
		pages []byte
		page  int
		enc   func(w io.Writer, pages io.Reader) error
	}{
		{"commit", int(big.h.Size()), big.pages, 4096, func(w io.Writer, r io.Reader) error { return EncodeCommit(w, big.h, big.pgnos, r) }},
		{"small commit", int(goldenCommit().h.Size()), goldenCommit().pages, 512, func(w io.Writer, r io.Reader) error {
			g := goldenCommit()
			return EncodeCommit(w, g.h, g.pgnos, r)
		}},
		{"base", int(bigBase.Size()), basePages, 4096, func(w io.Writer, r io.Reader) error { return EncodeBase(w, bigBase, r) }},
	}
	for _, e := range encoders {
		t.Run(e.name, func(t *testing.T) {
			for n := 0; n < e.size; n += 1 + n/7 {
				wantForeign(t, e.enc(&failWriter{n: n}, bytes.NewReader(e.pages)), errBoom)
			}
			wantForeign(t, e.enc(&failWriter{n: e.size - 1}, bytes.NewReader(e.pages)), errBoom)
			if err := e.enc(&failWriter{n: e.size}, bytes.NewReader(e.pages)); err != nil {
				t.Fatalf("writer of exactly %d bytes: %v", e.size, err)
			}
			for n := 0; n < len(e.pages); n += e.page / 2 {
				wantForeign(t, e.enc(io.Discard, &errReader{data: e.pages[:n]}), errBoom)
				want := io.ErrUnexpectedEOF
				if n%e.page == 0 {
					want = io.EOF
				}
				wantForeign(t, e.enc(io.Discard, bytes.NewReader(e.pages[:n])), want)
			}
			// The source is read exactly as far as the object needs.
			src := bytes.NewReader(append(slices.Clone(e.pages), "rest"...))
			if err := e.enc(io.Discard, src); err != nil || src.Len() != len("rest") {
				t.Fatalf("%d bytes left unread, want 4: %v", src.Len(), err)
			}
		})
	}
}

func TestBaseRejects(t *testing.T) {
	golden := func() []byte { h, p := goldenBase(); return encodeBase(t, h, p) }
	fixed := func(m func([]byte) []byte) func([]byte) []byte { return then(m, refixBase) }
	headerOnly := func(m func([]byte) []byte) func([]byte) []byte {
		return then(m, func(b []byte) []byte { le.PutUint32(b[60:], checksum(b[:60])); return b })
	}
	const slot1 = BaseHeaderSize + 516
	cases := []mutation{
		{"empty", golden, cut(0), ErrNotFormat},
		{"other magic", golden, flip(7), ErrNotFormat},
		{"commit object", func() []byte { return goldenCommit().encode(t) }, nil, ErrNotFormat},
		{"magic only", golden, cut(8), ErrInvalid},
		{"half a version", golden, cut(9), ErrInvalid},
		{"version 2", golden, set16(8, 2), ErrVersion},
		{"version 2, nothing after it", golden, then(set16(8, 2), cut(10)), ErrVersion},
		{"header only", golden, cut(BaseHeaderSize), ErrInvalid},
		{"truncated header", golden, cut(BaseHeaderSize - 1), ErrInvalid},
		{"header corrupt", golden, flip(25), ErrChecksum},
		{"database identifier corrupt", golden, flip(45), ErrChecksum},
		{"header checksum corrupt", golden, flip(61), ErrChecksum},
		{"codec set", golden, fixed(set16(10, 1<<1)), ErrVersion},
		{"flag set", golden, fixed(set16(10, 1)), ErrVersion},
		{"database identifier zero", golden, fixed(setDB(40, DatabaseID{})), ErrInvalid},
		{"other database", golden, fixed(setDB(40, otherDB)), ErrMisplaced},
		{"records of another database", golden, headerOnly(setDB(40, otherDB)), ErrMisplaced},
		{"page size", golden, fixed(set32(12, 513)), ErrInvalid},
		{"group size 0", golden, fixed(set32(16, 0)), ErrInvalid},
		{"group size not a power of two", golden, fixed(set32(16, 3)), ErrInvalid},
		{"group too many bytes", golden, fixed(then(set32(12, 65536), set32(16, 2048))), ErrInvalid},
		{"group beyond largest database", golden, fixed(set32(20, 1<<30)), ErrInvalid},
		{"transaction 0", golden, fixed(set64(24, 0)), ErrInvalid},
		{"epoch 0", golden, fixed(set64(32, 0)), ErrInvalid},
		{"database too large", golden, fixed(set32(56, MaxPages+1)), ErrInvalid},
		{"group beyond the database", golden, fixed(set32(56, 2097152)), ErrInvalid},
		{"database ends at the group's first page", golden, fixed(set32(56, 2097153)), ErrInvalid}, // slots 1 and 2 hold data
		{"one byte short", golden, func(b []byte) []byte { return b[:len(b)-1] }, ErrInvalid},
		{"trailing byte", golden, func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
		{"page corrupt", golden, flip(slot1 + 7), ErrChecksum},
		{"page checksum corrupt", golden, flip(slot1 + 513), ErrChecksum},
		{"slots swapped", golden, swap(slot1, slot1+516, 516), ErrChecksum},
		{"records of another transaction", golden, headerOnly(set64(24, 10)), ErrChecksum},
		{"records of another epoch", golden, headerOnly(set64(32, 2)), ErrChecksum},
		{"records of another group", golden, headerOnly(then(set32(20, 524289), set32(56, MaxPages))), ErrChecksum},
		{"lock page not zero", golden, fixed(func(b []byte) []byte { b[BaseHeaderSize+3] = 1; return b }), ErrInvalid},
		{"page beyond the database not zero", golden, fixed(func(b []byte) []byte { b[BaseHeaderSize+3*516+511] = 1; return b }), ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := c.base()
			if c.mut != nil {
				b = c.mut(b)
			}
			_, _, err := DecodeBase(b, goldenDB)
			wantClass(t, err, c.want)
			if len(b) < BaseHeaderSize {
				_, err := DecodeBaseHeader(b, goldenDB)
				wantClass(t, err, c.want)
			}
		})
	}
}

// TestBasePadding is the review's case: at 512-byte pages in groups of 4,
// group 1073741823 holds pages 4294967293 to 4294967296, and the last two
// are beyond the largest database.
func TestBasePadding(t *testing.T) {
	h := BaseHeader{DB: goldenDB, PageSize: 512, GroupSize: 4, Group: 1073741823, Txn: 1, Epoch: 1, DBSize: MaxPages}
	zero := encodeBase(t, h, make([]byte, 4*512))
	for slot := range 4 {
		pages := make([]byte, 4*512)
		pages[slot*512+100] = 1
		err := EncodeBase(io.Discard, h, bytes.NewReader(pages))
		b := refixBase(func() []byte {
			b := slices.Clone(zero)
			b[BaseHeaderSize+slot*516+100] = 1
			return b
		}())
		_, _, derr := DecodeBase(b, goldenDB)
		if slot < 2 {
			if err != nil || derr != nil {
				t.Fatalf("slot %d: %v, %v", slot, err, derr)
			}
			continue
		}
		wantClass(t, err, ErrInvalid)
		wantClass(t, derr, ErrInvalid)
	}
	// Below the database size the same applies to every slot beyond it.
	h.DBSize = 4294967293
	for slot := 1; slot < 4; slot++ {
		pages := make([]byte, 4*512)
		pages[slot*512] = 1
		wantClass(t, EncodeBase(io.Discard, h, bytes.NewReader(pages)), ErrInvalid)
	}
	if err := EncodeBase(io.Discard, h, bytes.NewReader(pattern(512, 1))); err == nil {
		t.Fatal("short source accepted")
	}
	// The slot of page 2^32 has its own record identity, not page 0's.
	b := encodeBase(t, BaseHeader{DB: goldenDB, PageSize: 512, GroupSize: 4, Group: 1073741823, Txn: 1, Epoch: 1, DBSize: MaxPages}, make([]byte, 4*512))
	if want := record("SKYLITEB", goldenDB, 1, 1, 1<<32, make([]byte, 512)); !bytes.Equal(b[BaseHeaderSize+3*516:], want) {
		t.Fatal("record of page 4294967296 differs from the specified identity")
	}
	h.DBSize = 4294967292 // the group starts beyond it
	wantClass(t, EncodeBase(io.Discard, h, bytes.NewReader(make([]byte, 4*512))), ErrInvalid)
}

func TestBasePageRejects(t *testing.T) {
	h, pages := goldenBase()
	b := encodeBase(t, h, pages)
	lock := LockPage(h.PageSize)
	rec := func(pgno uint32) []byte {
		off, n, err := h.PageRange(pgno)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Clone(b[off : off+n])
	}
	dirtyLock := rec(lock)
	dirtyLock[0] = 1
	le.PutUint32(dirtyLock[512:], h.recordCRC(uint64(lock), dirtyLock[:512]))
	bad := h
	bad.GroupSize = 0
	with := func(f func(*BaseHeader)) BaseHeader { x := h; f(&x); return x }

	cases := []struct {
		name string
		err  func() error
		want error
	}{
		{"page before group", func() error { _, err := DecodeBasePage(h, lock-1, rec(lock)); return err }, ErrInvalid},
		{"page after group", func() error { _, err := DecodeBasePage(h, lock+4, rec(lock)); return err }, ErrInvalid},
		{"range outside group", func() error { _, _, err := h.PageRange(1); return err }, ErrInvalid},
		{"range with invalid header", func() error { _, _, err := bad.PageRange(lock); return err }, ErrInvalid},
		{"range with zero database", func() error {
			_, _, err := with(func(h *BaseHeader) { h.DB = DatabaseID{} }).PageRange(lock)
			return err
		}, ErrInvalid},
		{"range past the largest page", func() error {
			_, _, err := with(func(h *BaseHeader) { h.Group = 1073741823 }).PageRange(math.MaxUint32)
			return err
		}, ErrInvalid},
		{"record too short", func() error { _, err := DecodeBasePage(h, lock+1, rec(lock + 1)[1:]); return err }, ErrInvalid},
		{"record too long", func() error { _, err := DecodeBasePage(h, lock+1, append(rec(lock+1), 0)); return err }, ErrInvalid},
		{"record corrupt", func() error { r := rec(lock + 1); r[0] ^= 1; _, err := DecodeBasePage(h, lock+1, r); return err }, ErrChecksum},
		{"lock page not zero", func() error { _, err := DecodeBasePage(h, lock, dirtyLock); return err }, ErrInvalid},
		{"slot 2 as slot 1", func() error { _, err := DecodeBasePage(h, lock+1, rec(lock+2)); return err }, ErrChecksum},
		{"slot 1 as slot 2", func() error { _, err := DecodeBasePage(h, lock+2, rec(lock+1)); return err }, ErrChecksum},
		{"record of another transaction", func() error {
			_, err := DecodeBasePage(with(func(h *BaseHeader) { h.Txn++ }), lock+1, rec(lock+1))
			return err
		}, ErrChecksum},
		{"record of another epoch", func() error {
			_, err := DecodeBasePage(with(func(h *BaseHeader) { h.Epoch++ }), lock+1, rec(lock+1))
			return err
		}, ErrChecksum},
		{"record of another database", func() error {
			_, err := DecodeBasePage(with(func(h *BaseHeader) { h.DB = otherDB }), lock+1, rec(lock+1))
			return err
		}, ErrChecksum},
		{"record of another group at the same slot", func() error {
			x := with(func(h *BaseHeader) { h.Group++ })
			_, err := DecodeBasePage(x, lock+1+4, rec(lock+1))
			return err
		}, ErrChecksum},
		{"record of a commit object", func() error {
			r := rec(lock + 1)
			le.PutUint32(r[512:], recordCRC(commitMagic, h.DB, h.Txn, h.Epoch, uint64(lock+1), r[:512]))
			_, err := DecodeBasePage(h, lock+1, r)
			return err
		}, ErrChecksum},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantClass(t, c.err(), c.want)
		})
	}

	dirty := slices.Clone(pages)
	dirty[0] = 1
	wantClass(t, EncodeBase(io.Discard, h, bytes.NewReader(dirty)), ErrInvalid)
	wantClass(t, EncodeBase(io.Discard, bad, bytes.NewReader(pages)), ErrInvalid)
	wantClass(t, EncodeBase(io.Discard, with(func(h *BaseHeader) { h.DB = DatabaseID{} }), bytes.NewReader(pages)), ErrInvalid)
	wantClass(t, EncodeBase(io.Discard, with(func(h *BaseHeader) { h.DBSize = MaxPages + 1 }), bytes.NewReader(pages)), ErrInvalid)
	beyond := slices.Clone(pages)
	beyond[3*512+511] = 1
	wantClass(t, EncodeBase(io.Discard, h, bytes.NewReader(beyond)), ErrInvalid)
}

func TestCheckKey(t *testing.T) {
	c := goldenCommit().h
	if err := c.CheckKey(7); err != nil {
		t.Fatal(err)
	}
	wantClass(t, c.CheckKey(8), ErrMisplaced)
	wantClass(t, c.CheckKey(6), ErrMisplaced)
	b, _ := goldenBase()
	if err := b.CheckKey(524288, 9, 3); err != nil {
		t.Fatal(err)
	}
	wantClass(t, b.CheckKey(524289, 9, 3), ErrMisplaced)
	wantClass(t, b.CheckKey(524288, 8, 3), ErrMisplaced)
	wantClass(t, b.CheckKey(524288, 9, 4), ErrMisplaced)
}

func TestZeroDivisorsPanic(t *testing.T) {
	for name, f := range map[string]func(){
		"LockPage(0)":      func() { _ = LockPage(0) },
		"GroupCount(1, 0)": func() { _ = GroupCount(1, 0) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			f()
		}()
	}
	if LockPage(512) != 2097153 || LockPage(65536) != 16385 || GroupCount(0, 4) != 0 || GroupCount(5, 4) != 2 || GroupCount(8, 4) != 2 {
		t.Fatal("LockPage or GroupCount is wrong")
	}
}

func TestManifestRejects(t *testing.T) {
	golden := func() []byte { b, _ := EncodeManifest(goldenManifest()); return b }
	fixed := func(m func([]byte) []byte) func([]byte) []byte { return then(m, refixManifest) }
	const ref0 = manifestHeaderSize
	const bump0 = manifestHeaderSize + 3*groupRefSize
	cases := []mutation{
		{"empty", golden, cut(0), ErrNotFormat},
		{"other magic", golden, flip(7), ErrNotFormat},
		{"lease", func() []byte { b, _ := EncodeLease(goldenLease()); return b }, nil, ErrNotFormat},
		{"magic only", golden, cut(8), ErrInvalid},
		{"half a version", golden, cut(9), ErrInvalid},
		{"version 2, nothing after it", golden, then(set16(8, 2), cut(10)), ErrVersion},
		{"version 1, nothing after it", golden, cut(10), ErrInvalid},
		{"truncated header", golden, cut(manifestHeaderSize + 3), ErrInvalid},
		{"version 2", golden, set16(8, 2), ErrVersion},
		{"one byte short", golden, func(b []byte) []byte { return b[:len(b)-1] }, ErrInvalid},
		{"trailing byte", golden, func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
		{"group count", golden, fixed(set32(56, 2)), ErrInvalid},
		{"bump count", golden, fixed(set32(60, 1)), ErrInvalid},
		{"counts traded", golden, fixed(then(set32(56, 2), set32(60, 4))), ErrInvalid},
		{"corrupt", golden, flip(30), ErrChecksum},
		{"bump corrupt", golden, flip(bump0 + 9), ErrChecksum},
		{"checksum corrupt", golden, func(b []byte) []byte { b[len(b)-1] ^= 1; return b }, ErrChecksum},
		{"flags", golden, fixed(set16(10, 1)), ErrVersion},
		{"page size", golden, fixed(set32(12, 100)), ErrInvalid},
		{"group size not a power of two", golden, fixed(set32(16, 255)), ErrInvalid},
		{"group size 0", golden, fixed(set32(16, 0)), ErrInvalid},
		{"group too many bytes", golden, fixed(set32(16, 1<<15)), ErrInvalid},
		{"database too large", golden, fixed(set32(20, MaxPages+1)), ErrInvalid},
		{"groups do not cover the database", golden, fixed(set32(20, 769)), ErrInvalid},
		{"groups beyond the database", golden, fixed(set32(20, 512)), ErrInvalid},
		{"epoch 0", golden, fixed(set64(32, 0)), ErrInvalid},
		{"database identifier zero", golden, fixed(setDB(40, DatabaseID{})), ErrInvalid},
		{"other database", golden, fixed(setDB(40, otherDB)), ErrMisplaced},
		{"reference with transaction 0", golden, fixed(set64(ref0, 0)), ErrInvalid},
		{"reference with epoch 0", golden, fixed(set64(ref0+8, 0)), ErrInvalid},
		{"reference after manifest transaction", golden, fixed(set64(ref0, 10)), ErrInvalid},
		{"reference after manifest epoch", golden, fixed(set64(ref0+8, 4)), ErrInvalid},
		{"last reference after manifest epoch", golden, fixed(set64(ref0+2*16+8, 4)), ErrInvalid},
		{"bump 0", golden, fixed(set64(bump0, 0)), ErrInvalid},
		{"bump after manifest transaction", golden, fixed(set64(bump0+8, 10)), ErrInvalid},
		{"bump at manifest transaction", golden, fixed(set64(bump0+8, 9)), nil},
		{"duplicate bump", golden, fixed(set64(bump0+8, 2)), ErrInvalid},
		{"bumps descending", golden, fixed(set64(bump0, 9)), ErrInvalid},
		{"bumps before any base", golden, fixed(then(set64(24, 0), then(set32(20, 0), set32(56, 0)))), ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := c.base()
			if c.mut != nil {
				b = c.mut(b)
			}
			_, err := DecodeManifest(b, goldenDB)
			if c.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantClass(t, err, c.want)
		})
	}
	// A reader that does not know the identifier yet accepts any.
	if m, err := DecodeManifest(golden(), DatabaseID{}); err != nil || m.DB != goldenDB {
		t.Fatalf("decoded %x, %v", m.DB, err)
	}

	with := func(f func(*Manifest)) Manifest { m := goldenManifest(); f(&m); return m }
	for name, m := range map[string]Manifest{
		"database identifier": with(func(m *Manifest) { m.DB = DatabaseID{} }),
		"page size":           with(func(m *Manifest) { m.PageSize = 0 }),
		"group size":          with(func(m *Manifest) { m.GroupSize = 6 }),
		"database too large":  with(func(m *Manifest) { m.DBSize = MaxPages + 1 }),
		"epoch 0":             with(func(m *Manifest) { m.Epoch = 0 }),
		"too few groups":      with(func(m *Manifest) { m.Groups = m.Groups[:2] }),
		"too many groups":     with(func(m *Manifest) { m.Groups = append(m.Groups, GroupRef{}) }),
		"half a reference":    with(func(m *Manifest) { m.Groups[1].Txn = 1 }),
		"reference too new":   with(func(m *Manifest) { m.Groups[0].Txn = 10 }),
		"reference epoch new": with(func(m *Manifest) { m.Groups[0].Epoch = 4 }),
		"bump 0":              with(func(m *Manifest) { m.Bumps = []uint64{0, 2} }),
		"bump too new":        with(func(m *Manifest) { m.Bumps = []uint64{2, 10} }),
		"duplicate bump":      with(func(m *Manifest) { m.Bumps = []uint64{2, 2} }),
		"bumps descending":    with(func(m *Manifest) { m.Bumps = []uint64{8, 2} }),
	} {
		t.Run("encode "+name, func(t *testing.T) {
			_, err := EncodeManifest(m)
			wantClass(t, err, ErrInvalid)
			if errors.Is(err, ErrManifestTooLarge) {
				t.Fatal("ordinary invalid manifest reported as too large")
			}
		})
	}
}

// TestManifestSizeLimit fills a manifest with epoch bumps to the largest
// size the format allows, 68 + 8b bytes with no groups, and one bump more.
// rawManifest encodes m without validating it.
func rawManifest(m Manifest) []byte {
	b := make([]byte, manifestSize(int64(len(m.Groups)), int64(len(m.Bumps))))
	copy(b, "SKYLITEM")
	putN(b, 8, 2, 1)
	putN(b, 12, 4, uint64(m.PageSize))
	putN(b, 16, 4, uint64(m.GroupSize))
	putN(b, 20, 4, uint64(m.DBSize))
	putN(b, 24, 8, m.Txn)
	putN(b, 32, 8, m.Epoch)
	copy(b[40:], m.DB[:])
	putN(b, 56, 4, uint64(len(m.Groups)))
	putN(b, 60, 4, uint64(len(m.Bumps)))
	for i, r := range m.Groups {
		putN(b, 64+16*i, 8, r.Txn)
		putN(b, 72+16*i, 8, r.Epoch)
	}
	for i, t := range m.Bumps {
		putN(b, 64+16*len(m.Groups)+8*i, 8, t)
	}
	putN(b, len(b)-4, 4, uint64(crc32cBitwise(b[:len(b)-4])))
	return b
}

// TestHeaderRules checks each header field rule on its own, with every
// other field consistent, so no later check catches the value instead:
// headers decoded alone, PageRange, the encoders, and manifests whose
// group counts match their sizes.
func TestHeaderRules(t *testing.T) {
	commit := func(f func(*CommitHeader)) CommitHeader { h := goldenCommit().h; f(&h); return h }
	for name, h := range map[string]CommitHeader{
		"page size 256":           commit(func(h *CommitHeader) { h.PageSize = 256 }),
		"page size 768":           commit(func(h *CommitHeader) { h.PageSize = 768 }),
		"page size 131072":        commit(func(h *CommitHeader) { h.PageSize = 131072 }),
		"more pages than the db":  commit(func(h *CommitHeader) { h.PageCount, h.DBSize = 4, 3 }),
		"epoch bump with a page":  commit(func(h *CommitHeader) { h.EpochBump, h.PageCount = true, 1 }),
		"database too large":      commit(func(h *CommitHeader) { h.DBSize = MaxPages + 1 }),
		"commit without any page": commit(func(h *CommitHeader) { h.PageCount = 0 }),
	} {
		t.Run("commit "+name, func(t *testing.T) {
			_, err := DecodeCommitHeader(h.encode(0), goldenDB)
			wantClass(t, err, ErrInvalid)
			var pgnos []uint32
			for i := range h.PageCount {
				pgnos = append(pgnos, i+1)
			}
			wantClass(t, EncodeCommit(io.Discard, h, pgnos, bytes.NewReader(make([]byte, int(h.PageCount)*int(h.PageSize)))), ErrInvalid)
		})
	}

	base := func(f func(*BaseHeader)) BaseHeader { h, _ := goldenBase(); f(&h); return h }
	for name, h := range map[string]BaseHeader{
		"page size 256":                      base(func(h *BaseHeader) { h.PageSize = 256 }),
		"page size 768":                      base(func(h *BaseHeader) { h.PageSize = 768 }),
		"page size 131072":                   base(func(h *BaseHeader) { h.PageSize, h.Group = 131072, 1 }),
		"group of 128 MiB":                   base(func(h *BaseHeader) { h.PageSize, h.GroupSize, h.Group = 65536, 2048, 1 }),
		"group of 128 MiB at 512-byte pages": base(func(h *BaseHeader) { h.GroupSize, h.Group = 1<<18, 1 }),
	} {
		t.Run("base "+name, func(t *testing.T) {
			_, err := DecodeBaseHeader(h.encode(), goldenDB)
			wantClass(t, err, ErrInvalid)
			_, _, err = h.PageRange(h.Group*h.GroupSize + 1)
			wantClass(t, err, ErrInvalid)
		})
	}

	manifest := func(f func(*Manifest)) Manifest {
		m := Manifest{DB: goldenDB, PageSize: 4096, GroupSize: 256, Epoch: 1}
		f(&m)
		m.Groups = make([]GroupRef, GroupCount(m.DBSize, m.GroupSize))
		return m
	}
	for name, m := range map[string]Manifest{
		"page size 256":      manifest(func(m *Manifest) { m.PageSize = 256 }),
		"page size 768":      manifest(func(m *Manifest) { m.PageSize = 768 }),
		"page size 131072":   manifest(func(m *Manifest) { m.PageSize = 131072 }),
		"group of 128 MiB":   manifest(func(m *Manifest) { m.GroupSize = 1 << 15 }),
		"group size 3":       manifest(func(m *Manifest) { m.GroupSize = 3 }),
		"epoch 0":            manifest(func(m *Manifest) { m.Epoch = 0 }),
		"database too large": manifest(func(m *Manifest) { m.PageSize, m.GroupSize, m.DBSize = 512, 131072, MaxPages+1 }),
		"first bump 0":       manifest(func(m *Manifest) { m.Txn, m.Bumps = 5, []uint64{0} }),
	} {
		t.Run("manifest "+name, func(t *testing.T) {
			_, err := DecodeManifest(rawManifest(m), goldenDB)
			wantClass(t, err, ErrInvalid)
			_, err = EncodeManifest(m)
			wantClass(t, err, ErrInvalid)
		})
	}
	// The largest database is accepted at the same shape.
	m := manifest(func(m *Manifest) { m.PageSize, m.GroupSize, m.DBSize = 512, 131072, MaxPages })
	if _, err := DecodeManifest(rawManifest(m), goldenDB); err != nil {
		t.Fatal(err)
	}
}

func TestManifestSizeLimit(t *testing.T) {
	const most = (MaxManifestSize - 68) / 8
	m := Manifest{DB: goldenDB, PageSize: 4096, GroupSize: 256, Txn: most + 1, Epoch: 1, Bumps: make([]uint64, most)}
	for i := range m.Bumps {
		m.Bumps[i] = uint64(i) + 1
	}
	b, err := EncodeManifest(m)
	if err != nil || len(b) != 68+8*most || len(b) > MaxManifestSize || len(b)+8 <= MaxManifestSize {
		t.Fatalf("largest manifest: %d bytes, %v", len(b), err)
	}
	if got, err := DecodeManifest(b, goldenDB); err != nil || len(got.Bumps) != most {
		t.Fatalf("largest manifest does not decode: %v", err)
	}
	m.Bumps = append(m.Bumps, most+1)
	_, err = EncodeManifest(m)
	wantClass(t, err, ErrInvalid)
	if !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("encode: %v, want ErrManifestTooLarge", err)
	}
	// Decoding checks the size before the counts, the checksum or any field.
	big := append(slices.Clone(b), make([]byte, 8)...)
	_, err = DecodeManifest(big, goldenDB)
	wantClass(t, err, ErrInvalid)
	if !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("decode: %v, want ErrManifestTooLarge", err)
	}
	limit := make([]byte, MaxManifestSize+1)
	copy(limit, b[:10])
	if _, err := DecodeManifest(limit, goldenDB); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("decode one byte over the limit: %v, want ErrManifestTooLarge", err)
	}
	if _, err := DecodeManifest(limit[:MaxManifestSize], goldenDB); err == nil || errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("decode at the limit: %v, want an ordinary invalid manifest", err)
	}
	le.PutUint16(big[8:], 2)
	wantClass(t, func() error { _, err := DecodeManifest(big, goldenDB); return err }(), ErrVersion)
}

func TestLeaseRejects(t *testing.T) {
	s := layoutLease
	const holder = `"node-a_10.0.0.7"`
	const head = `{"format":"skylite-lease",`
	cases := []struct {
		name string
		in   string
		want error
	}{
		{"empty", "", ErrNotFormat},
		{"null", "null", ErrNotFormat},
		{"empty object", "{}", ErrNotFormat},
		{"other format", `{"format":"other","version":1}`, ErrNotFormat},
		{"manifest", string(readGolden(t, "manifest.bin")), ErrNotFormat},
		{"leading space", " " + s, ErrNotFormat},
		{"space in the format", strings.Replace(s, `"format":`, `"format": `, 1), ErrNotFormat},
		{"format only", head, ErrInvalid},
		{"no version", head + `"holder":"x"}`, ErrInvalid},
		{"version without digits", head + `"version":}`, ErrInvalid},
		{"version not a number", head + `"version":"1"}`, ErrInvalid},
		{"version negative", head + `"version":-1}`, ErrInvalid},
		{"version 2", strings.Replace(s, `"version":1`, `"version":2`, 1), ErrVersion},
		{"version 2, nothing after it", head + `"version":2`, ErrVersion},
		{"version 1, nothing after it", head + `"version":1`, ErrInvalid},
		{"version 10", strings.Replace(s, `"version":1`, `"version":10`, 1), ErrVersion},
		{"version 01", strings.Replace(s, `"version":1`, `"version":01`, 1), ErrVersion},
		{"version 0", strings.Replace(s, `"version":1`, `"version":0`, 1), ErrVersion},
		{"version beyond 64 bits", head + `"version":18446744073709551616,"epoch":"three"}`, ErrVersion},
		{"version beyond 32 bits", head + `"version":4294967297}`, ErrVersion},
		{"version with a sign", head + `"version":+1,"holder":"x","epoch":1,"renewed":0}`, ErrInvalid},
		{"version 2 with new fields", head + `"version":2,"epoch":"three"}`, ErrVersion},
		{"version 1.0", strings.Replace(s, `"version":1`, `"version":1.0`, 1), ErrInvalid},
		{"version 1e0", strings.Replace(s, `"version":1`, `"version":1e0`, 1), ErrInvalid},
		{"truncated", s[:len(s)-1], ErrInvalid},
		{"truncated in the holder", s[:55], ErrInvalid},
		{"truncated in the epoch", s[:strings.Index(s, `"epoch":`)+8], ErrInvalid},
		{"wrong field type", strings.Replace(s, `"epoch":3`, `"epoch":"3"`, 1), ErrInvalid},
		{"unknown field", strings.Replace(s, `"epoch":3`, `"epoch":3,"x":1`, 1), ErrInvalid},
		{"duplicate field", strings.Replace(s, `"epoch":3`, `"epoch":3,"epoch":3`, 1), ErrInvalid},
		{"fields reordered", head + `"version":1,"epoch":3,"holder":"x","renewed":0}`, ErrInvalid},
		{"whitespace", strings.Replace(s, `"epoch":3`, `"epoch": 3`, 1), ErrInvalid},
		{"empty holder", strings.Replace(s, holder, `""`, 1), ErrInvalid},
		{"longest holder", strings.Replace(s, holder, `"`+strings.Repeat("h", MaxHolderLen)+`"`, 1), nil},
		{"holder too long", strings.Replace(s, holder, `"`+strings.Repeat("h", MaxHolderLen+1)+`"`, 1), ErrInvalid},
		{"holder with a slash", strings.Replace(s, holder, `"node/a"`, 1), ErrInvalid},
		{"holder with a space", strings.Replace(s, holder, `"node a"`, 1), ErrInvalid},
		{"holder with an escape", strings.Replace(s, holder, `"node\u0041"`, 1), ErrInvalid},
		{"holder with an escaped quote", strings.Replace(s, holder, `"node\"a"`, 1), ErrInvalid},
		{"holder not ASCII", strings.Replace(s, holder, `"nodé"`, 1), ErrInvalid},
		{"holder with a colon", strings.Replace(s, holder, `"a:b"`, 1), ErrInvalid},
		{"holder with a plus", strings.Replace(s, holder, `"a+b"`, 1), ErrInvalid},
		{"epoch 0", strings.Replace(s, `"epoch":3`, `"epoch":0`, 1), ErrInvalid},
		{"epoch with a leading zero", strings.Replace(s, `"epoch":3`, `"epoch":03`, 1), ErrInvalid},
		{"epoch with a sign", strings.Replace(s, `"epoch":3`, `"epoch":+3`, 1), ErrInvalid},
		{"epoch largest", strings.Replace(s, `"epoch":3`, `"epoch":18446744073709551615`, 1), nil},
		{"epoch beyond 64 bits", strings.Replace(s, `"epoch":3`, `"epoch":18446744073709551616`, 1), ErrInvalid},
		{"epoch missing", strings.Replace(s, `"epoch":3`, `"epoch":`, 1), ErrInvalid},
		{"renewed 0", strings.Replace(s, `1794744000500`, `0`, 1), nil},
		{"renewed 00", strings.Replace(s, `1794744000500`, `00`, 1), ErrInvalid},
		{"renewed largest", strings.Replace(s, `1794744000500`, `9223372036854775807`, 1), nil},
		{"renewed beyond 63 bits", strings.Replace(s, `1794744000500`, `9223372036854775808`, 1), ErrInvalid},
		{"renewed negative", strings.Replace(s, `1794744000500`, `-1`, 1), ErrInvalid},
		{"renewed with a fraction", strings.Replace(s, `1794744000500`, `1794744000500.0`, 1), ErrInvalid},
		{"renewed as a time", strings.Replace(s, `1794744000500`, `"2026-11-15T12:00:00.5Z"`, 1), ErrInvalid},
		{"renewed missing", strings.Replace(s, `,"renewed":1794744000500`, ``, 1), ErrInvalid},
		{"trailing data", s + " ", ErrInvalid},
		{"trailing object", s + "{}", ErrInvalid},
		{"no closing brace", s[:len(s)-1] + " ", ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeLease([]byte(c.in))
			if c.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantClass(t, err, c.want)
		})
	}

	g := goldenLease()
	for name, l := range map[string]Lease{
		"empty holder":      {Holder: "", Epoch: 1},
		"holder too long":   {Holder: strings.Repeat("h", MaxHolderLen+1), Epoch: 1},
		"holder with quote": {Holder: `a"b`, Epoch: 1},
		"holder not ASCII":  {Holder: "\xff", Epoch: 1},
		"epoch 0":           {Holder: g.Holder},
		"renewed negative":  {Holder: g.Holder, Epoch: 1, Renewed: -1},
	} {
		t.Run("encode "+name, func(t *testing.T) {
			_, err := EncodeLease(l)
			wantClass(t, err, ErrInvalid)
		})
	}
}

func TestKeysReject(t *testing.T) {
	for _, p := range []string{
		"", "/", "a/", "/a", "a//b", ".", "..", "a.", "a/./b", "a/../b", "a b", "a\x00", "ä", "a:b", "a\\b",
		strings.Repeat("a", MaxKeyLen-longestSuffix+1),
		strings.Repeat("a/", MaxKeySegments-2) + "a",
	} {
		if _, err := NewKeys(p); err == nil {
			t.Errorf("NewKeys(%q) accepted", p)
		} else {
			wantClass(t, err, ErrInvalid)
		}
	}
	for _, p := range []string{"a", "tenant-1/db.main_2", "..a", "a..b", strings.Repeat("a/", MaxKeySegments-3) + "a", strings.Repeat("a", MaxKeyLen-longestSuffix)} {
		k, err := NewKeys(p)
		if err != nil {
			t.Errorf("NewKeys(%q): %v", p, err)
			continue
		}
		if key := k.Base(math.MaxUint32, math.MaxUint64, math.MaxUint64); !validKey(key) {
			t.Errorf("longest key %q is invalid", key)
		}
	}

	k, err := NewKeys("db")
	if err != nil {
		t.Fatal(err)
	}
	if got := k.Commit(42); got != "db/log/00000000000000000042" {
		t.Errorf("Commit(42) = %q", got)
	}
	if got := k.Base(3, 42, 7); got != "db/base/0000000003-00000000000000000042-00000000000000000007" {
		t.Errorf("Base(3, 42, 7) = %q", got)
	}
	if k.Manifest() != "db/manifest" || k.Lease() != "db/lease" {
		t.Errorf("Manifest, Lease = %q, %q", k.Manifest(), k.Lease())
	}

	commit := k.Commit(42)
	for _, key := range []string{
		"", "db/log/", "db/log/42", commit[:len(commit)-1], commit + "0", commit + "/x",
		"db/log/0000000000000000004a", "db/log/+0000000000000000042", "db/log/ 0000000000000000042",
		"db/log/99999999999999999999", "other/log/00000000000000000042", "db/base/00000000000000000042",
		"dbx/log/00000000000000000042", "db//log/00000000000000000042",
	} {
		if _, err := k.ParseCommit(key); err == nil {
			t.Errorf("ParseCommit(%q) accepted", key)
		} else {
			wantClass(t, err, ErrInvalid)
		}
	}
	base := k.Base(3, 42, 7)
	for _, key := range []string{
		"", "db/base/", base[:len(base)-1], base + "0", commit,
		strings.Replace(base, "-", "_", 1),
		"db/base/4294967296-00000000000000000042-00000000000000000007",
		"db/base/0000000003-99999999999999999999-00000000000000000007",
		"db/base/0000000003-00000000000000000042-99999999999999999999",
		"db/base/000000000a-00000000000000000042-00000000000000000007",
		"db/base/0000000003-00000000000000000042-0000000000000000000x",
		"db/base/00000000003-0000000000000000042-00000000000000000007",
	} {
		if _, _, _, err := k.ParseBase(key); err == nil {
			t.Errorf("ParseBase(%q) accepted", key)
		} else {
			wantClass(t, err, ErrInvalid)
		}
	}
}
