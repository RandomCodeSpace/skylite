package format

import (
	"bytes"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
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
		{"version 2", plain, set16(8, 2), ErrVersion},
		{"version 0", plain, set16(8, 0), ErrVersion},
		{"truncated header", plain, cut(CommitHeaderSize - 1), ErrInvalid},
		{"header corrupt", plain, flip(20), ErrChecksum},
		{"header checksum corrupt", plain, flip(125), ErrChecksum},
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
		{"commit id too long", plain, fixed(func(b []byte) []byte { b[52] = MaxCommitIDLen + 1; return b }), ErrInvalid},
		{"commit id padding", plain, fixed(func(b []byte) []byte { b[53+len("client-42")] = 1; return b }), ErrInvalid},
		{"reserved byte", plain, fixed(func(b []byte) []byte { b[120] = 1; return b }), ErrInvalid},
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
			_, _, err := DecodeCommit(bytes.NewReader(b), nil)
			wantClass(t, err, c.want)
			// The range decoders see the same failure in their own range.
			if len(b) < CommitHeaderSize {
				_, err := DecodeCommitHeader(b)
				wantClass(t, err, c.want)
			}
		})
	}
}

func TestCommitRangeDecodersReject(t *testing.T) {
	b := goldenCommit().encode(t)
	h, err := DecodeCommitHeader(b[:CommitHeaderSize])
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
		{"header with trailing bytes", func() error { _, err := DecodeCommitHeader(b[:CommitHeaderSize+1]); return err }, ErrInvalid},
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
		{"page size", with(g, func(c *commitCase) { c.h.PageSize = 4095 })},
		{"transaction 0", with(g, func(c *commitCase) { c.h.Txn = 0 })},
		{"epoch 0", with(g, func(c *commitCase) { c.h.Epoch = 0 })},
		{"database too large", with(g, func(c *commitCase) { c.h.DBSize = MaxPages + 1 })},
		{"more pages than database", with(g, func(c *commitCase) { c.h.DBSize = 1 })},
		{"commit id too long", with(g, func(c *commitCase) { c.h.CommitID = make([]byte, MaxCommitIDLen+1) })},
		{"epoch bump with pages", with(g, func(c *commitCase) { c.h.EpochBump = true })},
		{"commit without pages", with(goldenBump(), func(c *commitCase) { c.h.EpochBump = false })},
		{"page count mismatch", with(g, func(c *commitCase) { c.pgnos = []uint32{1} })},
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
	err := EncodeCommit(io.Discard, g.h, g.pgnos, bytes.NewReader(g.pages[:700]))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short page source: %v", err)
	}
}

func TestBaseRejects(t *testing.T) {
	golden := func() []byte { h, p := goldenBase(); return encodeBase(t, h, p) }
	fixed := func(m func([]byte) []byte) func([]byte) []byte { return then(m, refixBase) }
	const slot1 = BaseHeaderSize + 516
	cases := []mutation{
		{"empty", golden, cut(0), ErrNotFormat},
		{"other magic", golden, flip(7), ErrNotFormat},
		{"commit object", func() []byte { return goldenCommit().encode(t) }, nil, ErrNotFormat},
		{"magic only", golden, cut(9), ErrInvalid},
		{"version 2", golden, set16(8, 2), ErrVersion},
		{"header only", golden, cut(BaseHeaderSize), ErrInvalid},
		{"truncated header", golden, cut(BaseHeaderSize - 1), ErrInvalid},
		{"header corrupt", golden, flip(25), ErrChecksum},
		{"codec set", golden, fixed(set16(10, 1<<1)), ErrVersion},
		{"flag set", golden, fixed(set16(10, 1)), ErrVersion},
		{"reserved byte", golden, fixed(func(b []byte) []byte { b[50] = 1; return b }), ErrInvalid},
		{"page size", golden, fixed(set32(12, 513)), ErrInvalid},
		{"group size 0", golden, fixed(set32(16, 0)), ErrInvalid},
		{"group size not a power of two", golden, fixed(set32(16, 3)), ErrInvalid},
		{"group too many bytes", golden, fixed(then(set32(12, 65536), set32(16, 2048))), ErrInvalid},
		{"group beyond largest database", golden, fixed(set32(20, 1<<30)), ErrInvalid},
		{"transaction 0", golden, fixed(set64(24, 0)), ErrInvalid},
		{"epoch 0", golden, fixed(set64(32, 0)), ErrInvalid},
		{"one byte short", golden, func(b []byte) []byte { return b[:len(b)-1] }, ErrInvalid},
		{"trailing byte", golden, func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
		{"page corrupt", golden, flip(slot1 + 7), ErrChecksum},
		{"page checksum corrupt", golden, flip(slot1 + 513), ErrChecksum},
		{"lock page not zero", golden, fixed(func(b []byte) []byte { b[BaseHeaderSize+3] = 1; return b }), ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := c.base()
			if c.mut != nil {
				b = c.mut(b)
			}
			_, _, err := DecodeBase(b)
			wantClass(t, err, c.want)
			if len(b) < BaseHeaderSize {
				_, err := DecodeBaseHeader(b)
				wantClass(t, err, c.want)
			}
		})
	}
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
	le.PutUint32(dirtyLock[512:], checksum(dirtyLock[:512]))
	bad := h
	bad.GroupSize = 0

	cases := []struct {
		name string
		err  func() error
		want error
	}{
		{"page before group", func() error { _, err := DecodeBasePage(h, lock-1, rec(lock)); return err }, ErrInvalid},
		{"page after group", func() error { _, err := DecodeBasePage(h, lock+4, rec(lock)); return err }, ErrInvalid},
		{"range outside group", func() error { _, _, err := h.PageRange(1); return err }, ErrInvalid},
		{"range with invalid header", func() error { _, _, err := bad.PageRange(lock); return err }, ErrInvalid},
		{"record too short", func() error { _, err := DecodeBasePage(h, lock+1, rec(lock + 1)[1:]); return err }, ErrInvalid},
		{"record corrupt", func() error { r := rec(lock + 1); r[0] ^= 1; _, err := DecodeBasePage(h, lock+1, r); return err }, ErrChecksum},
		{"lock page not zero", func() error { _, err := DecodeBasePage(h, lock, dirtyLock); return err }, ErrInvalid},
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
	if err := EncodeBase(io.Discard, h, bytes.NewReader(pages[:100])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short page source: %v", err)
	}
}

func TestManifestRejects(t *testing.T) {
	golden := func() []byte { b, _ := EncodeManifest(goldenManifest()); return b }
	fixed := func(m func([]byte) []byte) func([]byte) []byte { return then(m, refixManifest) }
	const ref0 = manifestHeaderSize
	cases := []mutation{
		{"empty", golden, cut(0), ErrNotFormat},
		{"other magic", golden, flip(7), ErrNotFormat},
		{"lease", func() []byte { b, _ := EncodeLease(goldenLease()); return b }, nil, ErrNotFormat},
		{"truncated header", golden, cut(manifestHeaderSize + 3), ErrInvalid},
		{"version 2", golden, set16(8, 2), ErrVersion},
		{"one byte short", golden, func(b []byte) []byte { return b[:len(b)-1] }, ErrInvalid},
		{"trailing byte", golden, func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
		{"group count", golden, fixed(set32(40, 2)), ErrInvalid},
		{"corrupt", golden, flip(30), ErrChecksum},
		{"flags", golden, fixed(set16(10, 1)), ErrVersion},
		{"page size", golden, fixed(set32(12, 100)), ErrInvalid},
		{"group size not a power of two", golden, fixed(set32(16, 255)), ErrInvalid},
		{"group size 0", golden, fixed(set32(16, 0)), ErrInvalid},
		{"group too many bytes", golden, fixed(set32(16, 1<<15)), ErrInvalid},
		{"database too large", golden, fixed(set32(20, MaxPages+1)), ErrInvalid},
		{"groups do not cover the database", golden, fixed(set32(20, 769)), ErrInvalid},
		{"groups beyond the database", golden, fixed(set32(20, 512)), ErrInvalid},
		{"epoch 0", golden, fixed(set64(32, 0)), ErrInvalid},
		{"reference with transaction 0", golden, fixed(set64(ref0, 0)), ErrInvalid},
		{"reference with epoch 0", golden, fixed(set64(ref0+8, 0)), ErrInvalid},
		{"reference after manifest transaction", golden, fixed(set64(ref0, 10)), ErrInvalid},
		{"reference after manifest epoch", golden, fixed(set64(ref0+8, 4)), ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := c.base()
			if c.mut != nil {
				b = c.mut(b)
			}
			_, err := DecodeManifest(b)
			wantClass(t, err, c.want)
		})
	}

	with := func(f func(*Manifest)) Manifest { m := goldenManifest(); f(&m); return m }
	for name, m := range map[string]Manifest{
		"page size":           with(func(m *Manifest) { m.PageSize = 0 }),
		"group size":          with(func(m *Manifest) { m.GroupSize = 6 }),
		"database too large":  with(func(m *Manifest) { m.DBSize = MaxPages + 1 }),
		"epoch 0":             with(func(m *Manifest) { m.Epoch = 0 }),
		"too few groups":      with(func(m *Manifest) { m.Groups = m.Groups[:2] }),
		"too many groups":     with(func(m *Manifest) { m.Groups = append(m.Groups, GroupRef{}) }),
		"half a reference":    with(func(m *Manifest) { m.Groups[1].Txn = 1 }),
		"reference too new":   with(func(m *Manifest) { m.Groups[0].Txn = 10 }),
		"reference epoch new": with(func(m *Manifest) { m.Groups[0].Epoch = 4 }),
	} {
		t.Run("encode "+name, func(t *testing.T) {
			_, err := EncodeManifest(m)
			wantClass(t, err, ErrInvalid)
		})
	}
}

func TestLeaseRejects(t *testing.T) {
	good, err := EncodeLease(goldenLease())
	if err != nil {
		t.Fatal(err)
	}
	s := string(good)
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
		{"version 2", strings.Replace(s, `"version":1`, `"version":2`, 1), ErrVersion},
		{"version 2 with new fields", `{"format":"skylite-lease","version":2,"epoch":"three"}`, ErrVersion},
		{"too large", s[:len(s)-1] + strings.Repeat(" ", MaxLeaseSize) + "}", ErrInvalid},
		{"truncated", s[:len(s)-2], ErrInvalid},
		{"version not a number", `{"format":"skylite-lease","version":"1"}`, ErrInvalid},
		{"wrong field type", strings.Replace(s, `"epoch":3`, `"epoch":"3"`, 1), ErrInvalid},
		{"unknown field", strings.Replace(s, `"epoch":3`, `"epoch":3,"x":1`, 1), ErrInvalid},
		{"duplicate field", strings.Replace(s, `"epoch":3`, `"epoch":3,"epoch":3`, 1), ErrInvalid},
		{"whitespace", strings.Replace(s, `"epoch":3`, `"epoch": 3`, 1), ErrInvalid},
		{"other time zone", strings.Replace(s, `12:00:00.5Z`, `13:00:00.5+01:00`, 1), ErrInvalid},
		{"empty holder", strings.Replace(s, `"node-a/10.0.0.7"`, `""`, 1), ErrInvalid},
		{"holder too long", strings.Replace(s, `"node-a/10.0.0.7"`, `"`+strings.Repeat("h", MaxHolderLen+1)+`"`, 1), ErrInvalid},
		{"holder not UTF-8", strings.Replace(s, `node-a`, "node-\xff", 1), ErrInvalid},
		{"epoch 0", strings.Replace(s, `"epoch":3`, `"epoch":0`, 1), ErrInvalid},
		{"trailing data", s + " ", ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeLease([]byte(c.in))
			wantClass(t, err, c.want)
		})
	}

	g := goldenLease()
	for name, l := range map[string]Lease{
		"empty holder":    {Holder: "", Epoch: 1},
		"holder too long": {Holder: strings.Repeat("h", MaxHolderLen+1), Epoch: 1},
		"holder not UTF8": {Holder: "\xff", Epoch: 1},
		"epoch 0":         {Holder: g.Holder},
		"year 10000":      {Holder: g.Holder, Epoch: 1, Renewed: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
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
