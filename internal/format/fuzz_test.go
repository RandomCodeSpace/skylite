package format

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

// Every fuzz target checks that the decoder never panics, that each error
// wraps exactly one class, and that whatever decodes re-encodes to the same
// bytes, so no two encodings mean the same thing. Targets over checksummed
// input take a fix flag that recomputes the checksums first, so the fuzzer
// also reaches the checks behind them.

func FuzzDecodeCommit(f *testing.F) {
	for _, name := range []string{"commit.bin", "commit-bump.bin", "commit-retired.bin"} {
		f.Add(readGolden(f, name), false)
	}
	f.Add([]byte(commitMagic), true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix {
			b = refixCommit(slices.Clone(b))
		}
		var pages []byte
		h, entries, err := DecodeCommit(bytes.NewReader(b), goldenDB, func(_ uint32, p []byte) error {
			pages = append(pages, p...)
			return nil
		})
		if errors.Is(err, ErrRetired) {
			if !h.Retired || h.EpochBump || entries != nil || pages != nil || !bytes.Equal(h.encode(h.tableCRC), b) {
				t.Fatalf("retired commit decoded as %+v %v", h, entries)
			}
			return
		}
		if err != nil {
			checkClass(t, err)
			return
		}
		if h.Retired {
			t.Fatal("retired commit decoded without ErrRetired")
		}
		if again := (commitCase{h, pgnosOf(entries), pages}).encode(t); !bytes.Equal(again, b) {
			t.Fatal("decoded commit re-encodes differently")
		}
	})
}

func FuzzDecodeCommitHeader(f *testing.F) {
	for _, name := range []string{"commit.bin", "commit-bump.bin", "commit-retired.bin"} {
		f.Add(readGolden(f, name)[:CommitHeaderSize], false)
	}
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix && len(b) >= CommitHeaderSize {
			b = slices.Clone(b)
			le.PutUint32(b[140:], checksum(b[:140]))
		}
		h, err := DecodeCommitHeader(b, goldenDB)
		if err != nil {
			checkClass(t, err)
			return
		}
		if !bytes.Equal(h.encode(h.tableCRC), b) {
			t.Fatal("decoded header re-encodes differently")
		}
		if h.EpochBump && h.CommitID != nil {
			t.Fatal("accepted an epoch bump with a commit identifier")
		}
		// The retired form differs from the header in bit 4 and the CRC
		// alone, and the table and page decoders refuse it.
		r, err := h.Retire()
		if h.EpochBump {
			if err == nil {
				t.Fatal("an epoch bump was retired")
			}
			checkClass(t, err)
			return
		}
		want := slices.Clone(b)
		want[10] |= 1 << 4
		le.PutUint32(want[140:], crc32cBitwise(want[:140]))
		if err != nil || !bytes.Equal(r, want) {
			t.Fatalf("retired form differs: %v", err)
		}
		rh, err := DecodeCommitHeader(r, goldenDB)
		if err != nil || !rh.Retired {
			t.Fatalf("retired header decodes as %+v, %v", rh, err)
		}
		if _, err := DecodeCommitTable(rh, nil); !errors.Is(err, ErrRetired) {
			t.Fatalf("table of a retired commit: %v", err)
		}
	})
}

func FuzzDecodeCommitTable(f *testing.F) {
	g := readGolden(f, "commit.bin")
	f.Add(uint32(512), uint32(3), uint32(2), g[CommitHeaderSize:CommitHeaderSize+2*PageEntrySize])
	f.Add(uint32(65536), uint32(16386), uint32(1), []byte{})
	f.Fuzz(func(t *testing.T, ps, dbSize, count uint32, table []byte) {
		h := CommitHeader{DB: goldenDB, PageSize: ps, Txn: 1, Epoch: 1, DBSize: dbSize, PageCount: count, tableCRC: checksum(table)}
		entries, err := DecodeCommitTable(h, table)
		if err != nil {
			checkClass(t, err)
			return
		}
		var again []byte
		for i, e := range entries {
			again = append(again, make([]byte, PageEntrySize)...)
			putEntry(again[i*PageEntrySize:], h.entry(uint32(i), e.Pgno)) //nolint:gosec // i < count
		}
		if !bytes.Equal(again, table) {
			t.Fatal("decoded table re-encodes differently")
		}
	})
}

// FuzzDecodeCommitPage hands DecodeCommitPage arbitrary entries for the
// golden commit's header shape. With fix set, the record's checksum is
// recomputed for the page number given, so the entry checks are reached.
func FuzzDecodeCommitPage(f *testing.F) {
	g := readGolden(f, "commit.bin")
	page0 := CommitHeaderSize + 2*PageEntrySize
	f.Add(uint32(3), uint32(2), uint32(1), uint64(page0), uint32(516), g[page0:page0+516], false)
	f.Add(uint32(3), uint32(2), uint32(3), uint64(page0+516), uint32(516), g[page0+516:page0+2*516], false)
	f.Add(uint32(3), uint32(2), uint32(3), uint64(page0), uint32(516), g[page0:page0+516], false)
	f.Add(uint32(5), uint32(4), uint32(0), uint64(page0), uint32(516), g[page0:page0+516], true)
	f.Fuzz(func(t *testing.T, dbSize, count, pgno uint32, offset uint64, length uint32, rec []byte, fix bool) {
		h := CommitHeader{DB: goldenDB, PageSize: 512, Txn: 7, Epoch: 2, DBSize: dbSize, PageCount: count}
		if fix && len(rec) >= recordTrailer {
			rec = slices.Clone(rec)
			n := len(rec) - recordTrailer
			le.PutUint32(rec[n:], crc32cBitwise(identity("SKYLITEC", goldenDB, 7, 2, uint64(pgno)), rec[:n]))
		}
		body, err := DecodeCommitPage(h, PageEntry{Pgno: pgno, Length: length, Offset: offset}, rec)
		if err != nil {
			checkClass(t, err)
			return
		}
		start := uint64(CommitHeaderSize) + 16*uint64(count)
		switch {
		case pgno == 0 || pgno > dbSize || pgno == LockPage(512):
			t.Fatalf("accepted page %d in a database of %d pages", pgno, dbSize)
		case offset < start || (offset-start)%516 != 0 || (offset-start)/516 >= uint64(count):
			t.Fatalf("accepted offset %d, not a record position", offset)
		case len(rec) != 516 || length != 516 || !bytes.Equal(body, rec[:512]):
			t.Fatal("accepted a record of the wrong length")
		case le.Uint32(rec[512:]) != crc32cBitwise(identity("SKYLITEC", goldenDB, 7, 2, uint64(pgno)), body):
			t.Fatalf("accepted a record that is not page %d of this commit", pgno)
		}
	})
}

func FuzzDecodeBase(f *testing.F) {
	f.Add(readGolden(f, "base.bin"), false)
	f.Add([]byte(baseMagic), true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix {
			b = refixBase(slices.Clone(b))
		}
		h, pages, err := DecodeBase(b, goldenDB)
		if err != nil {
			checkClass(t, err)
			return
		}
		if !bytes.Equal(encodeBase(t, h, slices.Concat(pages...)), b) {
			t.Fatal("decoded group re-encodes differently")
		}
	})
}

func FuzzDecodeBaseHeader(f *testing.F) {
	f.Add(readGolden(f, "base.bin")[:BaseHeaderSize], false)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix && len(b) >= BaseHeaderSize {
			b = slices.Clone(b)
			le.PutUint32(b[60:], checksum(b[:60]))
		}
		h, err := DecodeBaseHeader(b, goldenDB)
		if err != nil {
			checkClass(t, err)
			return
		}
		if !bytes.Equal(h.encode(), b) {
			t.Fatal("decoded header re-encodes differently")
		}
	})
}

// FuzzDecodeBasePage hands DecodeBasePage arbitrary groups and pages. With
// fix set, the record's checksum is recomputed for the page number given.
func FuzzDecodeBasePage(f *testing.F) {
	g := readGolden(f, "base.bin")
	f.Add(uint32(512), uint32(4), uint32(524288), uint32(2097154), uint64(goldenTag), g[BaseHeaderSize+516:BaseHeaderSize+2*516], false)
	f.Add(uint32(512), uint32(4), uint32(524288), uint32(2097153), uint64(goldenTag), g[BaseHeaderSize:BaseHeaderSize+516], false)
	f.Add(uint32(512), uint32(4), uint32(524288), uint32(2097155), uint64(goldenTag), g[BaseHeaderSize+516:BaseHeaderSize+2*516], false)
	f.Add(uint32(512), uint32(4), uint32(524288), uint32(2097154), uint64(0), g[BaseHeaderSize+516:BaseHeaderSize+2*516], false)
	f.Add(uint32(512), uint32(4), uint32(1073741823), uint32(4294967294), uint64(0), g[BaseHeaderSize+516:BaseHeaderSize+2*516], true)
	f.Fuzz(func(t *testing.T, ps, gs, group, pgno uint32, tag uint64, rec []byte, fix bool) {
		h := BaseHeader{DB: goldenDB, PageSize: ps, GroupSize: gs, Group: group, Txn: 9, Tag: tag}
		if fix && len(rec) >= recordTrailer {
			rec = slices.Clone(rec)
			n := len(rec) - recordTrailer
			le.PutUint32(rec[n:], crc32cBitwise(identity("SKYLITEB", goldenDB, 9, tag, uint64(pgno)), rec[:n]))
		}
		body, err := DecodeBasePage(h, pgno, rec)
		if err != nil {
			checkClass(t, err)
			return
		}
		first := uint64(group)*uint64(gs) + 1
		switch p := uint64(pgno); {
		case p < first || p >= first+uint64(gs) || p > MaxPages:
			t.Fatalf("accepted page %d outside group %d", pgno, group)
		case len(rec) != int(ps)+4 || !bytes.Equal(body, rec[:ps]):
			t.Fatal("accepted a record of the wrong length")
		case le.Uint32(rec[ps:]) != crc32cBitwise(identity("SKYLITEB", goldenDB, 9, tag, p), body):
			t.Fatalf("accepted a record that is not page %d of this group object", pgno)
		case pgno == LockPage(ps) && !allZero(body):
			t.Fatal("accepted a nonzero lock page")
		}
	})
}

func FuzzDecodeManifest(f *testing.F) {
	f.Add(readGolden(f, "manifest.bin"), false)
	creation, _ := EncodeManifest(creationManifest())
	f.Add(creation, false)
	f.Add([]byte(manifestMagic), true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix {
			b = refixManifest(slices.Clone(b))
		}
		m, err := DecodeManifest(b, goldenDB)
		if err != nil {
			checkClass(t, err)
			return
		}
		again, err := EncodeManifest(m)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded manifest re-encodes differently: %v", err)
		}
		if m.RetiredThrough > m.Txn {
			t.Fatalf("accepted retired through %d above transaction %d", m.RetiredThrough, m.Txn)
		}
		for g, r := range m.Groups {
			if r.Txn == 0 && r.Tag != 0 || r.Txn > m.Txn {
				t.Fatalf("accepted group %d reference %+v in a manifest at %d", g, r, m.Txn)
			}
		}
	})
}

func FuzzDecodeLease(f *testing.F) {
	f.Add(readGolden(f, "lease.json"))
	f.Add([]byte(`{"format":"skylite-lease","version":2}`))
	f.Add([]byte(`{"format":"skylite-lease","version":1,"holder":"x","epoch":18446744073709551615,"renewed":0}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		l, err := DecodeLease(b)
		if err != nil {
			checkClass(t, err)
			return
		}
		again, err := EncodeLease(l)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded lease re-encodes differently: %v", err)
		}
	})
}

func FuzzKeys(f *testing.F) {
	const id = "5b1e3c00912a4f7708d36ea510c42bf9"
	f.Add("db", "db/"+id+"/log/00000000000000000042")
	f.Add("tenant/db", "tenant/db/"+id+"/base/0000000003-00000000000000000042-9e3779b97f4a7c15")
	f.Add("tenant/db", "tenant/db/"+id+"/base/0000000003-00000000000000000042-0000000000000000")
	f.Add("tenant/db", "tenant/db/"+id+"/base/0000000003-00000000000000000042-9E3779B97F4A7C15")
	f.Add("tenant/db", "tenant/db/"+id+"/base/0000000003-00000000000000000042-9e3779b97f4a7c1")
	f.Add("tenant/db", "tenant/db/"+id+"/base/0000000003-00000000000000000042-00000000000000000007")
	f.Add("db", "db/5B1E3C00912A4F7708D36EA510C42BF9/log/00000000000000000042")
	f.Add("db", "db/00000000000000000000000000000000/log/00000000000000000042")
	f.Add("db", "db/log/00000000000000000042")
	f.Add("a.", "a./manifest")
	f.Fuzz(func(t *testing.T, prefix, key string) {
		k, err := NewKeys(prefix)
		if err != nil {
			checkClass(t, err)
			return
		}
		if longest := k.Base(goldenDB, 1<<32-1, 1<<64-1, 1<<64-1); !validKey(longest) {
			t.Fatalf("prefix %q accepted but builds invalid key %q", prefix, longest)
		}
		if db, txn, err := k.ParseCommit(key); err == nil {
			if db == (DatabaseID{}) || k.Commit(db, txn) != key {
				t.Fatalf("ParseCommit accepted %q, which Commit does not build", key)
			}
		} else {
			checkClass(t, err)
		}
		if db, g, txn, tag, err := k.ParseBase(key); err == nil {
			if db == (DatabaseID{}) || k.Base(db, g, txn, tag) != key {
				t.Fatalf("ParseBase accepted %q, which Base does not build", key)
			}
		} else {
			checkClass(t, err)
		}
	})
}
