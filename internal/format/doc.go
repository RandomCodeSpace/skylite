// Package format defines the objects that make up one database in object
// storage: the commit log, the base, the manifest and the lease. It
// encodes, decodes and validates them and names their keys. It does no I/O
// beyond the readers and writers it is given.
//
// This comment is the specification. A reader in another language should
// need nothing else.
//
// # Conventions
//
// All integers are unsigned and little-endian. Offsets are in bytes from
// the start of the object. Page numbers start at 1, as in SQLite. Every
// checksum is CRC-32C (Castagnoli polynomial, reflected, initial value and
// final XOR 0xFFFFFFFF, the variant used by iSCSI, ext4 and Google Cloud
// Storage), stored as a 4-byte integer. CRC-32C is in the Go standard
// library, runs in hardware on amd64 and arm64, and detects all burst
// errors up to 32 bits; the checksums detect corruption and truncation,
// not tampering. A page record is a page body followed by the CRC-32C of
// that body, so a record can be verified on its own.
//
// Supported page sizes are SQLite's: powers of two from 512 to 65536. A
// database holds at most 4294967294 pages (SQLite's limit): 16 TiB at
// 4096-byte pages. SQLite's lock page is page 2^30/pageSize + 1, the page
// holding byte offset 2^30; SQLite never writes it.
//
// # Errors and what a reader must reject
//
// Decoders return errors that wrap exactly one of ErrNotFormat (the bytes
// do not start with the expected magic), ErrVersion (a version, flag or
// codec this reader does not know), ErrChecksum, or ErrInvalid (anything
// else: truncation, trailing bytes, an out-of-range field, nonzero
// padding). Binary objects are checked in this order: magic, version,
// length, checksum, flags, fields. A reader must reject everything this
// specification does not allow; there are no optional or ignorable parts.
// Every accepted object re-encodes to exactly its input bytes.
//
// No decoder allocates in proportion to a length field before checking it
// against the bytes present. DecodeCommit reads from a stream, so it reads
// the page table with a buffer that grows with the bytes that arrive.
//
// # Versioning
//
// Every object carries the format version, 1. A change to any layout or
// rule here bumps it, and readers reject versions they do not know. Page
// compression will not need a new layout: the codec field in the flags
// will name it, and the stored record length in the commit page table
// already allows records of any length. Version 1 writers write codec 0
// (uncompressed) and version 1 readers reject any other codec with
// ErrVersion. Base group objects have no per-page lengths, so compressing
// them will need a new version with an offset table.
//
// # Commit object
//
// One object per committed transaction; creating it is the commit. Layout:
//
//	header      CommitHeaderSize (128) bytes
//	page table  PageEntrySize (16) bytes per page
//	records     one per page, in page table order
//
// Header:
//
//	off  size  field
//	0    8     magic "SKYLITEC"
//	8    2     version, 1
//	10   2     flags: bit 0 epoch bump; bits 1-3 codec, 0; other bits 0
//	12   4     page size
//	16   8     transaction number, at least 1
//	24   8     epoch of the writer, at least 1
//	32   4     database size in pages after this commit, at most 4294967294
//	36   4     page count n, at most the database size
//	40   8     page table size, 16n
//	48   4     CRC-32C of the page table
//	52   1     commit identifier length, at most 64
//	53   64    commit identifier, then zeros to fill the field
//	117  7     reserved, zero
//	124  4     CRC-32C of bytes 0-123
//
// The commit identifier is opaque bytes supplied by the client, used to
// find out later whether a commit whose response was lost happened.
//
// Page table entry i, at offset 128 + 16i:
//
//	off  size  field
//	0    4     page number
//	4    4     record length: page size + 4 (codec 0)
//	8    8     record offset: 128 + 16n + i*(page size + 4)
//
// Page numbers are strictly ascending, at least 1, at most the database
// size, and never the lock page. Records are contiguous and the object
// ends at the end of the last record, so its size is
// 128 + 16n + n*(page size + 4), known before any page body is written.
//
// An epoch bump has flag bit 0 set, no pages, and carries the database
// size of the commit before it. Any other commit has at least one page.
//
// Reading a page takes three range reads at most: the header, then the
// page table, then the record. Each is verified on its own: the header by
// its own CRC, the table by the CRC in the header, the record by its
// trailing CRC.
//
// # Base group object
//
// The base holds the database's pages in groups of G consecutive pages; G
// is a power of two, and G * page size is at most 64 MiB. Group g holds
// pages g*G+1 through (g+1)*G, and its first page must not exceed
// 4294967294. Each group object is immutable and its key is never reused.
// Layout:
//
//	header   BaseHeaderSize (64) bytes
//	records  G records of page size + 4 bytes; record i holds page g*G+1+i
//
// Header:
//
//	off  size  field
//	0    8     magic "SKYLITEB"
//	8    2     version, 1
//	10   2     flags: bits 1-3 codec, 0; other bits 0
//	12   4     page size
//	16   4     group size G
//	20   4     group number g
//	24   8     transaction number the group is current to, at least 1
//	32   8     epoch of the writer, at least 1
//	40   20    reserved, zero
//	60   4     CRC-32C of bytes 0-59
//
// Every slot is present. Pages beyond the database size, pages never
// written and the lock page are stored as zeros; the lock page must be
// zeros. Page p of group g is at offset 64 + (p - g*G - 1)*(page size + 4),
// computable from the manifest alone, so one range read fetches and
// verifies it. The whole object, 64 + G*(page size + 4) bytes, is one read.
//
// # Manifest
//
// One small object, replaced only by compare-and-swap. Binary, because a
// large database's manifest has hundreds of thousands of fixed-size
// entries that JSON would at least double in size and make slow to parse. Layout:
//
//	off  size  field
//	0    8     magic "SKYLITEM"
//	8    2     version, 1
//	10   2     flags, 0
//	12   4     page size
//	16   4     group size G
//	20   4     database size in pages at the transaction below
//	24   8     transaction number the base is current to; 0 for none
//	32   8     epoch of the writer, at least 1
//	40   4     group count, exactly ceil(database size / G)
//	44   16    per group, in group order: transaction number, epoch
//	...  4     CRC-32C of every byte before it
//
// Entry g names the object for group g by the transaction number it is
// current to and its writer's epoch: both zero means the group has no
// object and reads as zeros; otherwise the transaction number is at most
// the manifest's and the epoch at most the manifest's. Because entries are
// indexed by group, duplicate and out-of-range groups cannot be expressed.
// The encoding has no free choices, so equal manifests are equal bytes.
//
// The size is 48 + 16*ceil(database size / G) bytes. With G = 256 at
// 4096-byte pages (1 MiB groups, the suggested default) a 100 GiB database
// has 102400 groups and a 1638448-byte manifest: 16 bytes per MiB of
// data. Larger groups shrink the manifest and the object count and make
// read-ahead coarser; smaller groups make compaction rewrite less when
// writes are scattered, since a single changed page rewrites its whole
// group.
//
// # Lease
//
// One small JSON object, replaced by compare-and-swap, exactly as
// json.Marshal writes it in this field order, with no whitespace:
//
//	{"format":"skylite-lease","version":1,"holder":"...","epoch":3,"renewed":"2026-11-15T12:00:00.5Z"}
//
// holder is 1 to 256 bytes of UTF-8, epoch is at least 1, and renewed is
// an RFC 3339 time in UTC with trailing zero fractional digits removed,
// refreshed by the holder. The whole lease is at most 4096 bytes. The lease
// has no checksum: nothing safety-critical reads it, and a damaged lease
// fails to parse. A lease that does not start with {"format":"skylite-lease",
// is ErrNotFormat; a version other than 1 is ErrVersion; any other
// deviation from the canonical form is ErrInvalid.
//
// # Keys
//
// Keys follow the storage interface's rules: ASCII, at most 1024 bytes, at
// most 32 segments separated by "/", each segment one or more of A-Z a-z
// 0-9 . _ -, not "." or "..", not ending in ".". A database lives under a
// prefix of one or more segments, at most 966 bytes and 30 segments, so
// that every key below fits:
//
//	<prefix>/log/<txn>                  commit object
//	<prefix>/base/<group>-<txn>-<epoch> base group object
//	<prefix>/manifest                   manifest
//	<prefix>/lease                      lease
//
// txn and epoch are 20 decimal digits and group is 10, zero-padded, so
// byte order is numeric order: listing <prefix>/log/ starting after the
// key of transaction n yields the commits after n in order. Parsers accept
// only what the builders produce.
//
// A base object's key holds its group, the transaction number it is
// current to and its writer's epoch, so no two objects share a key. An
// object the manifest does not name is garbage if its transaction number
// is at most the manifest's; one with a higher transaction number may
// belong to a compaction in progress.
package format
