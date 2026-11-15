package format

import (
	"bytes"
	"slices"
	"testing"
)

// Every fuzz target checks that the decoder never panics, that each error
// wraps exactly one class, and that whatever decodes re-encodes to the same
// bytes, so no two encodings mean the same thing. Targets over checksummed
// input take a fix flag that recomputes the checksums first, so the fuzzer
// also reaches the checks behind them.

func FuzzDecodeCommit(f *testing.F) {
	for _, name := range []string{"commit.bin", "commit-bump.bin"} {
		f.Add(readGolden(f, name), false)
	}
	f.Add([]byte(commitMagic), true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix {
			b = refixCommit(slices.Clone(b))
		}
		var pages []byte
		h, entries, err := DecodeCommit(bytes.NewReader(b), func(_ uint32, p []byte) error {
			pages = append(pages, p...)
			return nil
		})
		if err != nil {
			checkClass(t, err)
			return
		}
		if again := (commitCase{h, pgnosOf(entries), pages}).encode(t); !bytes.Equal(again, b) {
			t.Fatal("decoded commit re-encodes differently")
		}
	})
}

func FuzzDecodeCommitHeader(f *testing.F) {
	for _, name := range []string{"commit.bin", "commit-bump.bin"} {
		f.Add(readGolden(f, name)[:CommitHeaderSize], false)
	}
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix && len(b) >= CommitHeaderSize {
			b = slices.Clone(b)
			le.PutUint32(b[124:], checksum(b[:124]))
		}
		h, err := DecodeCommitHeader(b)
		if err != nil {
			checkClass(t, err)
			return
		}
		if !bytes.Equal(h.encode(h.tableCRC), b) {
			t.Fatal("decoded header re-encodes differently")
		}
	})
}

func FuzzDecodeCommitTable(f *testing.F) {
	g := readGolden(f, "commit.bin")
	f.Add(uint32(512), uint32(3), uint32(2), g[CommitHeaderSize:CommitHeaderSize+2*PageEntrySize])
	f.Add(uint32(65536), uint32(16386), uint32(1), []byte{})
	f.Fuzz(func(t *testing.T, ps, dbSize, count uint32, table []byte) {
		h := CommitHeader{PageSize: ps, Txn: 1, Epoch: 1, DBSize: dbSize, PageCount: count, tableCRC: checksum(table)}
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

func FuzzDecodeCommitPage(f *testing.F) {
	g := readGolden(f, "commit.bin")
	page0 := CommitHeaderSize + 2*PageEntrySize
	f.Add(uint32(512), uint32(516), g[page0:page0+516])
	f.Fuzz(func(t *testing.T, ps, length uint32, rec []byte) {
		h := CommitHeader{PageSize: ps, Txn: 1, Epoch: 1, DBSize: 1, PageCount: 1}
		body, err := DecodeCommitPage(h, PageEntry{Pgno: 1, Length: length}, rec)
		if err != nil {
			checkClass(t, err)
			return
		}
		if !bytes.Equal(body, rec[:ps]) || le.Uint32(rec[ps:]) != checksum(body) {
			t.Fatal("accepted a record that does not verify")
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
		h, pages, err := DecodeBase(b)
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
		h, err := DecodeBaseHeader(b)
		if err != nil {
			checkClass(t, err)
			return
		}
		if !bytes.Equal(h.encode(), b) {
			t.Fatal("decoded header re-encodes differently")
		}
	})
}

func FuzzDecodeBasePage(f *testing.F) {
	g := readGolden(f, "base.bin")
	f.Add(uint32(512), uint32(4), uint32(524288), uint32(2097154), g[BaseHeaderSize+516:BaseHeaderSize+2*516])
	f.Add(uint32(512), uint32(4), uint32(524288), uint32(2097153), g[BaseHeaderSize:BaseHeaderSize+516])
	f.Fuzz(func(t *testing.T, ps, gs, group, pgno uint32, rec []byte) {
		h := BaseHeader{PageSize: ps, GroupSize: gs, Group: group, Txn: 1, Epoch: 1}
		body, err := DecodeBasePage(h, pgno, rec)
		if err != nil {
			checkClass(t, err)
			return
		}
		if off, n, err := h.PageRange(pgno); err != nil || n != int64(len(rec)) || off < BaseHeaderSize || off+n > h.Size() {
			t.Fatalf("accepted page %d outside its range: %d %d %v", pgno, off, n, err)
		}
		if le.Uint32(rec[ps:]) != checksum(body) {
			t.Fatal("accepted a record that does not verify")
		}
	})
}

func FuzzDecodeManifest(f *testing.F) {
	f.Add(readGolden(f, "manifest.bin"), false)
	f.Add([]byte(manifestMagic), true)
	f.Fuzz(func(t *testing.T, b []byte, fix bool) {
		if fix {
			b = refixManifest(slices.Clone(b))
		}
		m, err := DecodeManifest(b)
		if err != nil {
			checkClass(t, err)
			return
		}
		again, err := EncodeManifest(m)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded manifest re-encodes differently: %v", err)
		}
	})
}

func FuzzDecodeLease(f *testing.F) {
	f.Add(readGolden(f, "lease.json"))
	f.Add([]byte(`{"format":"skylite-lease","version":2}`))
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
	f.Add("db", "db/log/00000000000000000042")
	f.Add("tenant/db", "tenant/db/base/0000000003-00000000000000000042-00000000000000000007")
	f.Add("a.", "a./manifest")
	f.Fuzz(func(t *testing.T, prefix, key string) {
		k, err := NewKeys(prefix)
		if err != nil {
			checkClass(t, err)
			return
		}
		if longest := k.Base(1<<32-1, 1<<64-1, 1<<64-1); !validKey(longest) {
			t.Fatalf("prefix %q accepted but builds invalid key %q", prefix, longest)
		}
		if txn, err := k.ParseCommit(key); err == nil {
			if k.Commit(txn) != key {
				t.Fatalf("ParseCommit accepted %q, which Commit does not build", key)
			}
		} else {
			checkClass(t, err)
		}
		if g, txn, epoch, err := k.ParseBase(key); err == nil {
			if k.Base(g, txn, epoch) != key {
				t.Fatalf("ParseBase accepted %q, which Base does not build", key)
			}
		} else {
			checkClass(t, err)
		}
	})
}
