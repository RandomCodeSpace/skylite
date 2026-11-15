package format

import (
	"bytes"
	"reflect"
	"slices"
	"testing"
)

// This file states each layout a second time, from the specification and
// literal values, independently of the encoders and of hash/crc32, so
// that an encoder and decoder that share a bug still fail.

// crc32cBitwise computes CRC-32C one bit at a time over the concatenation
// of parts.
func crc32cBitwise(parts ...[]byte) uint32 {
	crc := ^uint32(0)
	for _, p := range parts {
		for _, c := range p {
			crc ^= uint32(c)
			for range 8 {
				if crc&1 != 0 {
					crc = crc>>1 ^ 0x82F63B78
				} else {
					crc >>= 1
				}
			}
		}
	}
	return ^crc
}

// putN stores the n-byte little-endian integer v at b[off:].
func putN(b []byte, off, n int, v uint64) {
	for i := range n {
		b[off+i] = byte(v >> (8 * i)) //nolint:gosec // truncation intended
	}
}

// identity is the 48-byte record identity a page record's checksum covers;
// x is the commit's epoch or the base object's tag.
func identity(magic string, db DatabaseID, txn, x, pgno uint64) []byte {
	b := make([]byte, 48)
	copy(b, magic)
	copy(b[8:], db[:])
	putN(b, 24, 8, txn)
	putN(b, 32, 8, x)
	putN(b, 40, 8, pgno)
	return b
}

// record is a page body followed by its checksum.
func record(magic string, db DatabaseID, txn, x, pgno uint64, body []byte) []byte {
	b := append(slices.Clone(body), 0, 0, 0, 0)
	putN(b, len(body), 4, uint64(crc32cBitwise(identity(magic, db, txn, x, pgno), body)))
	return b
}

var layoutDB = DatabaseID{0x5b, 0x1e, 0x3c, 0x00, 0x91, 0x2a, 0x4f, 0x77, 0x08, 0xd3, 0x6e, 0xa5, 0x10, 0xc4, 0x2b, 0xf9}

func TestCRC32CBitwise(t *testing.T) {
	if got := crc32cBitwise([]byte("123456789")); got != 0xE3069283 {
		t.Fatalf("check value %#x, want 0xe3069283", got)
	}
}

// layoutCommit is the golden commit: page size 512, transaction 7, epoch
// 2, database size 3, pages 1 and 3, commit identifier "client-42".
func layoutCommit() []byte {
	p1, p3 := pattern(512, 1), pattern(512, 3)
	b := make([]byte, 144+2*16)
	copy(b[0:], "SKYLITEC")
	putN(b, 8, 2, 1)
	putN(b, 10, 2, 0)
	putN(b, 12, 4, 512)
	putN(b, 16, 8, 7)
	putN(b, 24, 8, 2)
	putN(b, 32, 4, 3)
	putN(b, 36, 4, 2)
	putN(b, 40, 8, 32)
	copy(b[52:], layoutDB[:])
	b[68] = 9
	copy(b[69:], "client-42")
	// Page table at 144; records start at 144 + 32 = 176.
	putN(b, 144, 4, 1)
	putN(b, 148, 4, 516)
	putN(b, 152, 8, 176)
	putN(b, 160, 4, 3)
	putN(b, 164, 4, 516)
	putN(b, 168, 8, 692)
	putN(b, 48, 4, uint64(crc32cBitwise(b[144:176])))
	putN(b, 140, 4, uint64(crc32cBitwise(b[:140])))
	b = append(b, record("SKYLITEC", layoutDB, 7, 2, 1, p1)...)
	return append(b, record("SKYLITEC", layoutDB, 7, 2, 3, p3)...)
}

// layoutRetired is the golden commit's retired form: its header with flag
// bit 4 set and the header CRC recomputed, every other byte the same.
func layoutRetired() []byte {
	b := make([]byte, 144)
	copy(b[0:], "SKYLITEC")
	putN(b, 8, 2, 1)
	putN(b, 10, 2, 0x10)
	putN(b, 12, 4, 512)
	putN(b, 16, 8, 7)
	putN(b, 24, 8, 2)
	putN(b, 32, 4, 3)
	putN(b, 36, 4, 2)
	putN(b, 40, 8, 32)
	copy(b[52:], layoutDB[:])
	b[68] = 9
	copy(b[69:], "client-42")
	putN(b, 48, 4, uint64(crc32cBitwise(layoutCommit()[144:176])))
	putN(b, 140, 4, uint64(crc32cBitwise(b[:140])))
	return b
}

// layoutBump is the golden epoch bump: page size 4096, transaction 8, epoch
// 3, database size 3.
func layoutBump() []byte {
	b := make([]byte, 144)
	copy(b[0:], "SKYLITEC")
	putN(b, 8, 2, 1)
	putN(b, 10, 2, 1)
	putN(b, 12, 4, 4096)
	putN(b, 16, 8, 8)
	putN(b, 24, 8, 3)
	putN(b, 32, 4, 3)
	putN(b, 48, 4, uint64(crc32cBitwise(nil)))
	copy(b[52:], layoutDB[:])
	putN(b, 140, 4, uint64(crc32cBitwise(b[:140])))
	return b
}

// layoutBase is the golden group: page size 512, group size 4, group
// 524288 (pages 2097153-2097156, the first the lock page), transaction 9,
// tag 0x9e3779b97f4a7c15, database size 2097155.
func layoutBase() []byte {
	const tag = 0x9e3779b97f4a7c15
	b := make([]byte, 64)
	copy(b[0:], "SKYLITEB")
	putN(b, 8, 2, 1)
	putN(b, 12, 4, 512)
	putN(b, 16, 4, 4)
	putN(b, 20, 4, 524288)
	putN(b, 24, 8, 9)
	putN(b, 32, 8, tag)
	copy(b[40:], layoutDB[:])
	putN(b, 56, 4, 2097155)
	putN(b, 60, 4, uint64(crc32cBitwise(b[:60])))
	zero := make([]byte, 512)
	b = append(b, record("SKYLITEB", layoutDB, 9, tag, 2097153, zero)...)
	b = append(b, record("SKYLITEB", layoutDB, 9, tag, 2097154, pattern(512, 2))...)
	b = append(b, record("SKYLITEB", layoutDB, 9, tag, 2097155, pattern(512, 4))...)
	return append(b, record("SKYLITEB", layoutDB, 9, tag, 2097156, zero)...)
}

// layoutManifest is the golden manifest: page size 4096, group size 256,
// database size 600 (3 groups), transaction 9, retired through 6, epoch 3,
// writer nonce a7035ec19b24006fd8114ae2730cb596, groups
// {9, 0x9e3779b97f4a7c15} {0, 0} {5, 0}.
func layoutManifest() []byte {
	b := make([]byte, 84+3*16+4)
	copy(b[0:], "SKYLITEM")
	putN(b, 8, 2, 1)
	putN(b, 12, 4, 4096)
	putN(b, 16, 4, 256)
	putN(b, 20, 4, 600)
	putN(b, 24, 8, 9)
	putN(b, 32, 8, 3)
	copy(b[40:], layoutDB[:])
	copy(b[56:], []byte{0xa7, 0x03, 0x5e, 0xc1, 0x9b, 0x24, 0x00, 0x6f, 0xd8, 0x11, 0x4a, 0xe2, 0x73, 0x0c, 0xb5, 0x96})
	putN(b, 72, 8, 6)
	putN(b, 80, 4, 3)
	putN(b, 84, 8, 9)
	putN(b, 92, 8, 0x9e3779b97f4a7c15)
	putN(b, 116, 8, 5)
	putN(b, 132, 4, uint64(crc32cBitwise(b[:132])))
	return b
}

// layoutCreation is the first manifest of a database: page size 4096,
// group size 256, database size 0, transaction 0, retired through 0, epoch
// 1, the golden writer nonce, no groups.
func layoutCreation() []byte {
	b := make([]byte, 84+4)
	copy(b[0:], "SKYLITEM")
	putN(b, 8, 2, 1)
	putN(b, 12, 4, 4096)
	putN(b, 16, 4, 256)
	putN(b, 32, 8, 1)
	copy(b[40:], layoutDB[:])
	copy(b[56:], []byte{0xa7, 0x03, 0x5e, 0xc1, 0x9b, 0x24, 0x00, 0x6f, 0xd8, 0x11, 0x4a, 0xe2, 0x73, 0x0c, 0xb5, 0x96})
	putN(b, 84, 4, uint64(crc32cBitwise(b[:84])))
	return b
}

const layoutLease = `{"format":"skylite-lease","version":1,"holder":"node-a_10.0.0.7","epoch":3,"renewed":1794744000500}`

func TestLayout(t *testing.T) {
	bh, bpages := goldenBase()
	manifest, err := EncodeManifest(goldenManifest())
	if err != nil {
		t.Fatal(err)
	}
	creation, err := EncodeManifest(creationManifest())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := EncodeLease(goldenLease())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name          string
		want, encoded []byte
	}{
		{"commit.bin", layoutCommit(), goldenCommit().encode(t)},
		{"commit-bump.bin", layoutBump(), goldenBump().encode(t)},
		{"commit-retired.bin", layoutRetired(), goldenRetired(t)},
		{"base.bin", layoutBase(), encodeBase(t, bh, bpages)},
		{"manifest.bin", layoutManifest(), manifest},
		{"", layoutCreation(), creation},
		{"lease.json", []byte(layoutLease), lease},
	} {
		if !bytes.Equal(c.encoded, c.want) {
			t.Errorf("%s: encoder output differs from the specified layout at byte %d", c.name, firstDiff(c.encoded, c.want))
		}
		if c.name == "" {
			continue // no golden file
		}
		if !bytes.Equal(readGolden(t, c.name), c.want) {
			t.Errorf("%s: golden file differs from the specified layout", c.name)
		}
	}

	// The decoders read the specified bytes as the values they were built from.
	for _, c := range []struct {
		b    []byte
		want commitCase
	}{{layoutCommit(), goldenCommit()}, {layoutBump(), goldenBump()}} {
		var pages []byte
		h, entries, err := DecodeCommit(bytes.NewReader(c.b), layoutDB, func(_ uint32, p []byte) error {
			pages = append(pages, p...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		h.tableCRC = 0
		if !reflect.DeepEqual(h, c.want.h) || !slices.Equal(pgnosOf(entries), c.want.pgnos) || !bytes.Equal(pages, c.want.pages) {
			t.Errorf("decoded %+v %v", h, pgnosOf(entries))
		}
	}
	commit := layoutCommit()
	h, err := DecodeCommitHeader(commit[:144], layoutDB)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := DecodeCommitTable(h, commit[144:176])
	if err != nil || !reflect.DeepEqual(entries, []PageEntry{{1, 516, 176}, {3, 516, 692}}) {
		t.Fatalf("table %v, %v", entries, err)
	}
	if body, err := DecodeCommitPage(h, entries[1], commit[692:1208]); err != nil || !bytes.Equal(body, pattern(512, 3)) {
		t.Fatalf("page 3: %v", err)
	}

	rh, err := DecodeCommitHeader(layoutRetired(), layoutDB)
	want := goldenCommit().h
	want.Retired = true
	rh.tableCRC = 0
	if err != nil || !reflect.DeepEqual(rh, want) {
		t.Errorf("retired header: decoded %+v, %v", rh, err)
	}

	gh, gpages, err := DecodeBase(layoutBase(), layoutDB)
	if err != nil || gh != bh || !bytes.Equal(slices.Concat(gpages...), bpages) {
		t.Errorf("base: decoded %+v, %v", gh, err)
	}
	fromManifest := BaseHeader{DB: layoutDB, PageSize: 512, GroupSize: 4, Group: 524288, Txn: 9, Tag: 0x9e3779b97f4a7c15}
	if body, err := DecodeBasePage(fromManifest, 2097155, layoutBase()[64+2*516:64+3*516]); err != nil || !bytes.Equal(body, pattern(512, 4)) {
		t.Errorf("base page 2097155: %v", err)
	}
	if m, err := DecodeManifest(layoutManifest(), layoutDB); err != nil || !reflect.DeepEqual(m, goldenManifest()) {
		t.Errorf("manifest: decoded %+v, %v", m, err)
	}
	if m, err := DecodeManifest(layoutCreation(), layoutDB); err != nil || !reflect.DeepEqual(m, creationManifest()) {
		t.Errorf("creation manifest: decoded %+v, %v", m, err)
	}
	if l, err := DecodeLease([]byte(layoutLease)); err != nil || l != goldenLease() {
		t.Errorf("lease: decoded %+v, %v", l, err)
	}
	k, err := NewKeys("tenant-1/db")
	if err != nil {
		t.Fatal(err)
	}
	for got, want := range map[string]string{
		k.Commit(layoutDB, 42):                                           "tenant-1/db/5b1e3c00912a4f7708d36ea510c42bf9/log/00000000000000000042",
		k.Base(layoutDB, 3, 42, 0x9e3779b97f4a7c15):                      "tenant-1/db/5b1e3c00912a4f7708d36ea510c42bf9/base/0000000003-00000000000000000042-9e3779b97f4a7c15",
		k.Base(layoutDB, 0, 1, 0):                                        "tenant-1/db/5b1e3c00912a4f7708d36ea510c42bf9/base/0000000000-00000000000000000001-0000000000000000",
		k.Commit(DatabaseID{15: 1}, 18446744073709551615):                "tenant-1/db/00000000000000000000000000000001/log/18446744073709551615",
		k.Base(DatabaseID{0: 0xff}, 4294967295, 1, 18446744073709551615): "tenant-1/db/ff000000000000000000000000000000/base/4294967295-00000000000000000001-ffffffffffffffff",
		k.Manifest(): "tenant-1/db/manifest",
		k.Lease():    "tenant-1/db/lease",
	} {
		if got != want {
			t.Errorf("key %q, want %q", got, want)
		}
	}
	const extreme = `{"format":"skylite-lease","version":1,"holder":"x","epoch":18446744073709551615,"renewed":0}`
	if l, err := DecodeLease([]byte(extreme)); err != nil || l != (Lease{Holder: "x", Epoch: 1<<64 - 1}) {
		t.Errorf("extreme lease: decoded %+v, %v", l, err)
	}
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}
