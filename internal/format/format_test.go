package format

import (
	"bytes"
	"errors"
	"flag"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	seedFlag   = flag.Uint64("seed", 0, "seed for randomised tests; 0 picks one from the clock")
	updateFlag = flag.Bool("update", false, "rewrite the golden files in testdata")
)

func newRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d (rerun with -seed=%d)", seed, seed)
	return rand.New(rand.NewPCG(seed, 0)) //nolint:gosec // seeded and logged so failures reproduce
}

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, (n+7)&^7)
	for i := 0; i < len(b); i += 8 {
		le.PutUint64(b[i:], r.Uint64())
	}
	return b[:n]
}

var classes = []error{ErrNotFormat, ErrVersion, ErrChecksum, ErrInvalid, ErrMisplaced}

// checkClass fails unless err wraps exactly one error class.
func checkClass(t *testing.T, err error) {
	t.Helper()
	n := 0
	for _, c := range classes {
		if errors.Is(err, c) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("error %v wraps %d error classes, want 1", err, n)
	}
}

func wantClass(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
	checkClass(t, err)
}

// wantForeign fails unless err is the caller's own error and in no class.
func wantForeign(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
	for _, c := range classes {
		if errors.Is(err, c) {
			t.Fatalf("error %v wraps %v", err, c)
		}
	}
}

var (
	goldenDB = DatabaseID{0x5b, 0x1e, 0x3c, 0x00, 0x91, 0x2a, 0x4f, 0x77, 0x08, 0xd3, 0x6e, 0xa5, 0x10, 0xc4, 0x2b, 0xf9}
	otherDB  = DatabaseID{15: 1}
)

func randDB(r *rand.Rand) DatabaseID {
	var db DatabaseID
	copy(db[:], randBytes(r, 16))
	db[r.IntN(16)] |= 1
	return db
}

// Fixtures shared by the golden, negative and fuzz tests.

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

type commitCase struct {
	h     CommitHeader
	pgnos []uint32
	pages []byte
}

func (c commitCase) encode(t testing.TB) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeCommit(&buf, c.h, c.pgnos, bytes.NewReader(c.pages)); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func goldenCommit() commitCase {
	return commitCase{
		h:     CommitHeader{DB: goldenDB, PageSize: 512, Txn: 7, Epoch: 2, DBSize: 3, PageCount: 2, CommitID: []byte("client-42")},
		pgnos: []uint32{1, 3},
		pages: append(pattern(512, 1), pattern(512, 3)...),
	}
}

func goldenBump() commitCase {
	return commitCase{h: CommitHeader{DB: goldenDB, PageSize: 4096, Txn: 8, Epoch: 3, DBSize: 3, EpochBump: true}}
}

// lockCommit is a commit at 65536-byte pages around the lock page, 16385.
func lockCommit() commitCase {
	return commitCase{
		h:     CommitHeader{DB: goldenDB, PageSize: 65536, Txn: 1, Epoch: 1, DBSize: 16386, PageCount: 2},
		pgnos: []uint32{16384, 16386},
		pages: make([]byte, 2*65536),
	}
}

// goldenBase is the group holding the lock page at 512-byte pages
// (2097153) in slot 0; slot 3 is beyond the database size.
func goldenBase() (BaseHeader, []byte) {
	h := BaseHeader{DB: goldenDB, PageSize: 512, GroupSize: 4, Group: 524288, Txn: 9, Epoch: 3, DBSize: 2097155}
	pages := slices.Concat(make([]byte, 512), pattern(512, 2), pattern(512, 4), make([]byte, 512))
	return h, pages
}

func encodeBase(t testing.TB, h BaseHeader, pages []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeBase(&buf, h, bytes.NewReader(pages)); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func goldenManifest() Manifest {
	return Manifest{
		DB: goldenDB, PageSize: 4096, GroupSize: 256, DBSize: 600, Txn: 9, Epoch: 3,
		Groups: []GroupRef{{Txn: 9, Epoch: 3}, {}, {Txn: 5, Epoch: 2}},
		Bumps:  []uint64{2, 8},
	}
}

func goldenLease() Lease {
	return Lease{Holder: "node-a_10.0.0.7", Epoch: 3, Renewed: time.Date(2026, 11, 15, 12, 0, 0, 500_000_000, time.UTC).UnixMilli()}
}

// refixCommit recomputes every checksum it can locate from the header's
// page size and page count, each record's from the page number in its
// table entry, so mutations reach the checks behind them.
func refixCommit(b []byte) []byte {
	if len(b) < CommitHeaderSize {
		return b
	}
	var db DatabaseID
	copy(db[:], b[52:68])
	txn, epoch := le.Uint64(b[16:]), le.Uint64(b[24:])
	n := uint64(le.Uint32(b[36:]))
	end := CommitHeaderSize + n*PageEntrySize
	if uint64(len(b)) >= end {
		le.PutUint32(b[48:], checksum(b[CommitHeaderSize:end]))
		if ps := uint64(le.Uint32(b[12:])); ps <= MaxPageSize {
			off := end
			for i := uint64(0); i < n && off+ps+recordTrailer <= uint64(len(b)); i++ {
				pgno := uint64(le.Uint32(b[CommitHeaderSize+i*PageEntrySize:]))
				le.PutUint32(b[off+ps:], recordCRC(commitMagic, db, txn, epoch, pgno, b[off:off+ps]))
				off += ps + recordTrailer
			}
		}
	}
	le.PutUint32(b[140:], checksum(b[:140]))
	return b
}

func refixBase(b []byte) []byte {
	if len(b) < BaseHeaderSize {
		return b
	}
	le.PutUint32(b[60:], checksum(b[:60]))
	var db DatabaseID
	copy(db[:], b[40:56])
	txn, epoch := le.Uint64(b[24:]), le.Uint64(b[32:])
	pgno := uint64(le.Uint32(b[20:]))*uint64(le.Uint32(b[16:])) + 1
	if ps := uint64(le.Uint32(b[12:])); ps <= MaxPageSize {
		for off := uint64(BaseHeaderSize); off+ps+recordTrailer <= uint64(len(b)); off += ps + recordTrailer {
			le.PutUint32(b[off+ps:], recordCRC(baseMagic, db, txn, epoch, pgno, b[off:off+ps]))
			pgno++
		}
	}
	return b
}

func refixManifest(b []byte) []byte {
	if len(b) >= 4 {
		le.PutUint32(b[len(b)-4:], checksum(b[:len(b)-4]))
	}
	return b
}

func readGolden(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name)) //nolint:gosec // fixed names
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestGolden fails if the byte format changes. Run with -update only for a
// deliberate format change, which also needs a new Version once released.
// TestLayout checks the same objects against an independent statement of
// the layout.
func TestGolden(t *testing.T) {
	bh, bpages := goldenBase()
	manifest, err := EncodeManifest(goldenManifest())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := EncodeLease(goldenLease())
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"commit.bin":      goldenCommit().encode(t),
		"commit-bump.bin": goldenBump().encode(t),
		"base.bin":        encodeBase(t, bh, bpages),
		"manifest.bin":    manifest,
		"lease.json":      lease,
	}
	for name, got := range files {
		path := filepath.Join("testdata", name)
		if *updateFlag {
			if err := os.WriteFile(path, got, 0o600); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if want := readGolden(t, name); !bytes.Equal(got, want) {
			t.Errorf("%s: encoding changed (%d bytes, golden has %d)", name, len(got), len(want))
		}
	}

	// The golden bytes also decode to the values they were built from.
	for _, c := range []struct {
		name string
		want commitCase
	}{{"commit.bin", goldenCommit()}, {"commit-bump.bin", goldenBump()}} {
		var pages []byte
		h, entries, err := DecodeCommit(bytes.NewReader(readGolden(t, c.name)), goldenDB, func(_ uint32, p []byte) error {
			pages = append(pages, p...)
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		h.tableCRC = 0
		if !reflect.DeepEqual(h, c.want.h) || !slices.Equal(pgnosOf(entries), c.want.pgnos) || !bytes.Equal(pages, c.want.pages) {
			t.Errorf("%s: decoded %+v %v", c.name, h, pgnosOf(entries))
		}
	}
	gh, gpages, err := DecodeBase(readGolden(t, "base.bin"), goldenDB)
	if err != nil || gh != bh || !bytes.Equal(slices.Concat(gpages...), bpages) {
		t.Errorf("base.bin: decoded %+v, %v", gh, err)
	}
	if m, err := DecodeManifest(readGolden(t, "manifest.bin"), goldenDB); err != nil || !reflect.DeepEqual(m, goldenManifest()) {
		t.Errorf("manifest.bin: decoded %+v, %v", m, err)
	}
	if l, err := DecodeLease(readGolden(t, "lease.json")); err != nil || l != goldenLease() {
		t.Errorf("lease.json: decoded %+v, %v", l, err)
	}
}

func pgnosOf(entries []PageEntry) []uint32 {
	var p []uint32
	for _, e := range entries {
		p = append(p, e.Pgno)
	}
	return p
}

func randPageSize(r *rand.Rand) uint32 { return MinPageSize << r.IntN(8) }

func randCommit(r *rand.Rand) commitCase {
	ps := randPageSize(r)
	lock := LockPage(ps)
	h := CommitHeader{DB: randDB(r), PageSize: ps, Txn: 1 + r.Uint64N(math.MaxUint64), Epoch: 1 + r.Uint64N(math.MaxUint64)}
	if n := r.IntN(MaxCommitIDLen + 1); n > 0 {
		h.CommitID = randBytes(r, n)
	}
	if r.IntN(8) == 0 {
		h.EpochBump = true
		h.DBSize = r.Uint32N(MaxPages + 1)
		return commitCase{h: h}
	}
	switch r.IntN(3) {
	case 0:
		h.DBSize = 1 + r.Uint32N(64)
	case 1:
		h.DBSize = lock + r.Uint32N(4) // straddles the lock page
	default:
		h.DBSize = 1 + r.Uint32N(MaxPages)
	}
	set := map[uint32]bool{}
	if h.DBSize > lock {
		set[lock-1], set[lock+1] = true, true
	}
	want := 1 + r.IntN(min(8, int(h.DBSize)))
	for len(set) < want {
		if p := 1 + r.Uint32N(h.DBSize); p != lock {
			set[p] = true
		}
	}
	var pgnos []uint32
	for p := range set {
		if p <= h.DBSize {
			pgnos = append(pgnos, p)
		}
	}
	slices.Sort(pgnos)
	h.PageCount = uint32(len(pgnos)) //nolint:gosec // at most 10
	return commitCase{h: h, pgnos: pgnos, pages: randBytes(r, len(pgnos)*int(ps))}
}

func TestCommitRoundTrip(t *testing.T) {
	r := newRand(t)
	for range 300 {
		c := randCommit(r)
		b := c.encode(t)
		if int64(len(b)) != c.h.Size() {
			t.Fatalf("%+v: encoded %d bytes, Size says %d", c.h, len(b), c.h.Size())
		}

		var pages []byte
		var pgnos []uint32
		h, entries, err := DecodeCommit(bytes.NewReader(b), c.h.DB, func(p uint32, page []byte) error {
			pgnos = append(pgnos, p)
			pages = append(pages, page...)
			return nil
		})
		if err != nil {
			t.Fatalf("%+v: %v", c.h, err)
		}
		h2 := h
		h2.tableCRC = 0
		if !reflect.DeepEqual(h2, c.h) {
			t.Fatalf("header %+v, want %+v", h, c.h)
		}
		if !slices.Equal(pgnos, c.pgnos) || !bytes.Equal(pages, c.pages) {
			t.Fatalf("%+v: pages differ", c.h)
		}

		// The range-read decoders agree with the whole-object decoder.
		rh, err := DecodeCommitHeader(b[:CommitHeaderSize], c.h.DB)
		if err != nil || !reflect.DeepEqual(rh, h) {
			t.Fatalf("range header %+v, %v; want %+v", rh, err, h)
		}
		re, err := DecodeCommitTable(rh, b[CommitHeaderSize:CommitHeaderSize+rh.TableSize()])
		if err != nil || !slices.Equal(re, entries) {
			t.Fatalf("range table %v, %v; want %v", re, err, entries)
		}
		for i, e := range re {
			body, err := DecodeCommitPage(rh, e, b[e.Offset:e.Offset+uint64(e.Length)])
			if err != nil || !bytes.Equal(body, c.pages[i*int(c.h.PageSize):(i+1)*int(c.h.PageSize)]) {
				t.Fatalf("range page %d: %v", e.Pgno, err)
			}
		}

		// Decoded values re-encode to the same bytes.
		if again := (commitCase{h, pgnos, pages}).encode(t); !bytes.Equal(again, b) {
			t.Fatalf("%+v: re-encoding differs", c.h)
		}
	}
}

func randBase(r *rand.Rand) (BaseHeader, []byte) {
	ps := randPageSize(r)
	gs := uint32(1) << r.IntN(bitsLen((128<<10)/ps)) // at most 128 KiB per group, to stay fast
	h := BaseHeader{DB: randDB(r), PageSize: ps, GroupSize: gs, Txn: 1 + r.Uint64N(math.MaxUint64), Epoch: 1 + r.Uint64N(math.MaxUint64)}
	switch r.IntN(3) {
	case 0:
		h.Group = (LockPage(ps) - 1) / gs
	case 1:
		h.Group = (MaxPages - 1) / gs // the last group
	default:
		h.Group = r.Uint32N((MaxPages-1)/gs + 1)
	}
	first := uint64(h.Group)*uint64(gs) + 1
	h.DBSize = uint32(first + r.Uint64N(MaxPages-first+1)) //nolint:gosec // at most MaxPages
	if r.IntN(2) == 0 {
		h.DBSize = uint32(min(first+r.Uint64N(uint64(gs)), MaxPages)) //nolint:gosec // at most MaxPages
	}
	pages := randBytes(r, int(gs*ps))
	for i := range gs {
		p := first + uint64(i)
		if r.IntN(4) == 0 || p == uint64(LockPage(ps)) || p > uint64(h.DBSize) {
			clear(pages[i*ps : (i+1)*ps])
		}
	}
	return h, pages
}

func bitsLen(x uint32) int {
	n := 0
	for ; x > 0; x >>= 1 {
		n++
	}
	return n
}

func TestBaseRoundTrip(t *testing.T) {
	r := newRand(t)
	for range 300 {
		h, pages := randBase(r)
		b := encodeBase(t, h, pages)
		if int64(len(b)) != h.Size() {
			t.Fatalf("%+v: encoded %d bytes, Size says %d", h, len(b), h.Size())
		}
		gh, gp, err := DecodeBase(b, h.DB)
		if err != nil || gh != h || !bytes.Equal(slices.Concat(gp...), pages) {
			t.Fatalf("%+v: decoded %+v, %v", h, gh, err)
		}
		rh, err := DecodeBaseHeader(b[:BaseHeaderSize], h.DB)
		if err != nil || rh != h {
			t.Fatalf("range header %+v, %v", rh, err)
		}
		// Single pages via the range read, using a header built as a reader
		// would from the manifest, without reading the object's header.
		fromManifest := h
		fromManifest.DBSize = 0
		first := h.Group*h.GroupSize + 1
		for i := range h.GroupSize {
			pgno := first + i
			if uint64(pgno) > MaxPages || pgno < first {
				break
			}
			off, n, err := fromManifest.PageRange(pgno)
			if err != nil {
				t.Fatal(err)
			}
			body, err := DecodeBasePage(fromManifest, pgno, b[off:off+n])
			if err != nil || !bytes.Equal(body, gp[i]) {
				t.Fatalf("page %d: %v", pgno, err)
			}
		}
		if again := encodeBase(t, gh, slices.Concat(gp...)); !bytes.Equal(again, b) {
			t.Fatalf("%+v: re-encoding differs", h)
		}
	}
}

func randManifest(r *rand.Rand) Manifest {
	ps := randPageSize(r)
	gs := uint32(1) << r.IntN(bitsLen(MaxGroupBytes/ps))
	m := Manifest{DB: randDB(r), PageSize: ps, GroupSize: gs, Epoch: 1 + r.Uint64N(math.MaxUint64)}
	if r.IntN(4) != 0 {
		m.Txn = 1 + r.Uint64N(math.MaxUint64)
	}
	m.DBSize = gs*r.Uint32N(2000) + r.Uint32N(gs) // at most 2^17*2000, far below MaxPages
	if r.IntN(10) == 0 {
		m.DBSize = MaxPages - r.Uint32N(gs) // the largest databases, only with large groups
		if GroupCount(m.DBSize, gs) > 1<<16 {
			m.DBSize = 0
		}
	}
	for range GroupCount(m.DBSize, gs) {
		var g GroupRef
		if m.Txn > 0 && r.IntN(10) != 0 {
			g = GroupRef{Txn: 1 + r.Uint64N(m.Txn), Epoch: 1 + r.Uint64N(m.Epoch)}
		}
		m.Groups = append(m.Groups, g)
	}
	if m.Txn > 0 {
		for range r.IntN(300) {
			m.Bumps = append(m.Bumps, 1+r.Uint64N(m.Txn))
		}
		if r.IntN(2) == 0 {
			m.Bumps = append(m.Bumps, m.Txn) // the newest possible bump
		}
		slices.Sort(m.Bumps)
		m.Bumps = slices.Compact(m.Bumps)
	}
	return m
}

func TestManifestRoundTrip(t *testing.T) {
	r := newRand(t)
	for range 300 {
		m := randManifest(r)
		b, err := EncodeManifest(m)
		if err != nil {
			t.Fatalf("%+v: %v", m, err)
		}
		got, err := DecodeManifest(b, m.DB)
		if err != nil || !reflect.DeepEqual(got, m) {
			t.Fatalf("decoded %+v, %v; want %+v", got, err, m)
		}
		if again, _ := EncodeManifest(got); !bytes.Equal(again, b) {
			t.Fatal("manifest encoding is not deterministic")
		}
	}
}

const holderChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

func TestLeaseRoundTrip(t *testing.T) {
	r := newRand(t)
	for range 300 {
		h := make([]byte, 1+r.IntN(MaxHolderLen))
		for i := range h {
			h[i] = holderChars[r.IntN(len(holderChars))]
		}
		l := Lease{Holder: string(h), Epoch: 1 + r.Uint64N(math.MaxUint64), Renewed: r.Int64()}
		switch r.IntN(4) {
		case 0:
			l.Epoch, l.Renewed = math.MaxUint64, math.MaxInt64
		case 1:
			l.Epoch, l.Renewed = 1, 0
		}
		b, err := EncodeLease(l)
		if err != nil {
			t.Fatalf("%q: %v", l.Holder, err)
		}
		if len(b) > MaxLeaseSize {
			t.Fatalf("lease of %d bytes", len(b))
		}
		if got, err := DecodeLease(b); err != nil || got != l {
			t.Fatalf("decoded %+v, %v; want %+v", got, err, l)
		}
	}
	longest := Lease{Holder: strings.Repeat("h", MaxHolderLen), Epoch: math.MaxUint64, Renewed: math.MaxInt64}
	if b, err := EncodeLease(longest); err != nil || len(b) != MaxLeaseSize {
		t.Fatalf("longest lease is %d bytes, %v; MaxLeaseSize is %d", len(b), err, MaxLeaseSize)
	}
}

// validKey checks the storage interface's key rules independently of keys.go.
func validKey(k string) bool {
	segs := strings.Split(k, "/")
	if len(k) > MaxKeyLen || len(segs) > MaxKeySegments {
		return false
	}
	for _, s := range segs {
		if s == "" || s == "." || s == ".." || strings.HasSuffix(s, ".") ||
			strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-") != "" {
			return false
		}
	}
	return true
}

func TestKeysRoundTrip(t *testing.T) {
	r := newRand(t)
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
	for range 300 {
		var segs []string
		for range 1 + r.IntN(MaxKeySegments-2) {
			s := []byte{chars[r.IntN(len(chars)-3)]}
			for range r.IntN(30) {
				s = append(s, chars[r.IntN(len(chars))])
			}
			if s[len(s)-1] == '.' {
				s[len(s)-1] = '_'
			}
			segs = append(segs, string(s))
		}
		prefix := strings.Join(segs, "/")
		k, err := NewKeys(prefix)
		if len(prefix) > MaxKeyLen-longestSuffix {
			if err == nil {
				t.Fatalf("NewKeys accepted a %d-byte prefix", len(prefix))
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", prefix, err)
		}
		txn, epoch, group := r.Uint64(), r.Uint64(), r.Uint32()
		for _, key := range []string{k.Commit(txn), k.Base(group, txn, epoch), k.Manifest(), k.Lease()} {
			if !validKey(key) {
				t.Fatalf("built invalid key %q", key)
			}
		}
		if got, err := k.ParseCommit(k.Commit(txn)); err != nil || got != txn {
			t.Fatalf("ParseCommit = %d, %v; want %d", got, err, txn)
		}
		g, tx, e, err := k.ParseBase(k.Base(group, txn, epoch))
		if err != nil || g != group || tx != txn || e != epoch {
			t.Fatalf("ParseBase = %d %d %d, %v", g, tx, e, err)
		}
		a, b := r.Uint64(), r.Uint64()
		if (a < b) != (k.Commit(a) < k.Commit(b)) {
			t.Fatalf("commit keys for %d and %d sort out of order", a, b)
		}
	}
}

// TestHugeLengthsAllocateLittle gives decoders length fields that claim
// gigabytes with only a few bytes present.
func TestHugeLengthsAllocateLittle(t *testing.T) {
	huge := CommitHeader{DB: goldenDB, PageSize: 65536, Txn: 1, Epoch: 1, DBSize: MaxPages, PageCount: MaxPages - 1}
	hdr := huge.encode(0)
	stream := append(slices.Clone(hdr), make([]byte, 100)...)
	small := make([]byte, 100)

	m := make([]byte, manifestHeaderSize+4+100)
	copy(m, manifestMagic)
	le.PutUint16(m[8:], Version)
	le.PutUint32(m[56:], math.MaxUint32)
	le.PutUint32(m[60:], math.MaxUint32)

	bh := BaseHeader{DB: goldenDB, PageSize: 65536, GroupSize: 1024, Group: 0, Txn: 1, Epoch: 1, DBSize: 1}.encode()

	cases := []struct {
		name  string
		claim int64
		run   func() error
	}{
		{"DecodeCommit", huge.Size(), func() error { _, _, err := DecodeCommit(bytes.NewReader(stream), goldenDB, nil); return err }},
		{"DecodeCommitTable", huge.TableSize(), func() error { _, err := DecodeCommitTable(huge, small); return err }},
		{"DecodeManifest", (groupRefSize + bumpSize) * math.MaxUint32, func() error { _, err := DecodeManifest(m, goldenDB); return err }},
		{"DecodeBase", MaxGroupBytes, func() error { _, _, err := DecodeBase(bh, goldenDB); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := c.run()
			runtime.ReadMemStats(&after)
			wantClass(t, err, ErrInvalid)
			if got := after.TotalAlloc - before.TotalAlloc; got > 64<<10 {
				t.Fatalf("allocated %d bytes for a claim of %d", got, c.claim)
			}
		})
	}
}

// errReader fails after its data.
type errReader struct{ data []byte }

var errBoom = errors.New("boom")

func (e *errReader) Read(p []byte) (int, error) {
	if len(e.data) == 0 {
		return 0, errBoom
	}
	n := copy(p, e.data)
	e.data = e.data[n:]
	return n, nil
}

func TestDecodeCommitPassesReadErrors(t *testing.T) {
	b := goldenCommit().encode(t)
	for n := range len(b) + 1 {
		_, _, err := DecodeCommit(&errReader{data: b[:n]}, goldenDB, nil)
		if err != errBoom { //nolint:errorlint // must be returned unchanged
			t.Fatalf("after %d bytes: %v, want the reader's error unchanged", n, err)
		}
	}
	_, _, err := DecodeCommit(bytes.NewReader(b), goldenDB, func(uint32, []byte) error { return errBoom })
	if err != errBoom { //nolint:errorlint // must be returned unchanged
		t.Fatalf("callback error changed: %v", err)
	}
}
