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
// All integers are unsigned and little-endian unless stated otherwise.
// Offsets are in bytes from the start of the object. Page numbers start at
// 1, as in SQLite. Every checksum is CRC-32C (Castagnoli polynomial,
// reflected, initial value and final XOR 0xFFFFFFFF, the variant used by
// iSCSI, ext4 and Google Cloud Storage), stored as a 4-byte integer.
// CRC-32C is in the Go standard library, runs in hardware on amd64 and
// arm64, and detects all burst errors up to 32 bits; the checksums detect
// corruption, truncation and misplaced data, not tampering.
//
// Supported page sizes are SQLite's: powers of two from 512 to 65536. A
// database holds at most 4294967294 pages (SQLite's limit): 16 TiB at
// 4096-byte pages. SQLite's lock page is page 2^30/pageSize + 1, the page
// holding byte offset 2^30; SQLite never writes it.
//
// Every database has a database identifier: 16 bytes, generated once when
// the database is created, never all zeros. The manifest and the header
// of every commit and base group object carry it, and every page record's
// checksum covers it, so an object or record copied from one database to
// another is rejected.
//
// # Page records
//
// A page record is a page body followed by a 4-byte checksum. The checksum
// is the CRC-32C of this 48-byte record identity followed by the body:
//
//	off  size  field
//	0    8     magic of the object holding the record: "SKYLITEC" or "SKYLITEB"
//	8    16    database identifier
//	24   8     transaction number of the object
//	32   8     epoch of the object
//	40   8     page number
//
// The identity is not stored; a reader rebuilds it from what it expects
// the record to be. A record verifies only as the page, object and
// database it was written for, so a record fetched from the wrong range,
// a stale object or another database fails its checksum, with ErrChecksum.
// A reader cannot tell such a record from a damaged one, and does not need
// to: both mean these bytes are not the page asked for, and both are
// handled by fetching again or failing. Storing the identity in each
// record would cost 48 bytes per page to tell the two apart.
//
// # Errors and what a reader must reject
//
// Decoders return errors that wrap exactly one of five classes:
// ErrNotFormat (the bytes do not start with the expected magic),
// ErrVersion (a version, flag or codec this reader does not know),
// ErrChecksum, ErrInvalid (anything else that breaks a rule here:
// truncation, trailing bytes, an out-of-range field, nonzero padding) and
// ErrMisplaced (a well-formed object that belongs somewhere else: to
// another database, or under another key). A manifest larger than
// MaxManifestSize is ErrManifestTooLarge, which is in the ErrInvalid class
// and can be told apart from other invalid manifests.
//
// A binary object is checked part by part from the front: the header (the
// whole manifest is one part), then the page table, then each record. A
// header is checked in this order: magic, version (as soon as its two
// bytes are present), length, checksum, flags, fields, database
// identifier. A reader must reject everything this specification does not
// allow; there are no optional or ignorable parts. Every accepted object
// re-encodes to exactly its input bytes.
//
// Decoders also take the identifier of the database the caller expects,
// and reject an object of another with ErrMisplaced. A header does not say
// which key it was fetched under, so the caller must check that too:
// CommitHeader.CheckKey compares a commit's transaction number with its
// key's, BaseHeader.CheckKey a group object's group, transaction number
// and epoch with its key's, and both return ErrMisplaced on a mismatch.
// Callers must call them for every object they fetch by key.
//
// Errors that do not come from the bytes are not format errors and wrap
// none of the classes. DecodeCommit returns errors from its reader and its
// callback unchanged. The encoders return errors from their writer
// unchanged, and errors from their page source wrapped with the page or
// slot being read; a source that ends early gives io.EOF or
// io.ErrUnexpectedEOF wrapped in the same way. An encoder reads exactly
// the page bodies it needs and nothing after them, so a source may be a
// longer stream the caller keeps reading; checking for extra bytes would
// consume a byte that belongs to the caller, and block on a stream that
// has no end yet.
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
// # Assumptions
//
// The format rests on these and does not check them:
//
//   - An epoch belongs to exactly one leader candidacy: no two writers ever
//     write with the same epoch.
//   - Commit transaction numbers are contiguous from 1, and each number is
//     created at most once, by create-if-absent.
//
// So there is no predecessor digest and no writer-instance field. A key
// in the log names at most one object ever, so there is no second history
// for a digest to detect, and a writer is identified by its epoch, which
// no other writer shares. If storage broke create-if-absent, or two
// writers shared an epoch, the format would not notice.
//
// Epochs need not be non-decreasing along the commit log, and readers must
// not require it: a paused candidate can leave an epoch bump with a lower
// epoch after commits of a higher one.
//
// # Rules for the life of a database
//
//   - The page size and the group size never change for the life of a
//     database identifier.
//   - The manifest's transaction number and epoch never decrease when it
//     is replaced, and a writer replaces only a manifest whose epoch is at
//     most its own.
//   - A compaction's output objects carry a transaction number strictly
//     greater than that of the manifest they were built from, so a
//     compaction never writes under the key of an object a manifest it
//     read names, and an unreferenced object can be judged from its key
//     and a manifest; see Garbage collection.
//   - A group's content at a transaction number is fixed by the log, and
//     the encoding has no free choices, so every group object written
//     under one key has the same bytes; a retried compaction rewrites a
//     key with what is already there.
//
// # Commit object
//
// One object per committed transaction; creating it is the commit. Layout:
//
//	header      CommitHeaderSize (144) bytes
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
//	52   16    database identifier
//	68   1     commit identifier length, at most 64
//	69   64    commit identifier, then zeros to fill the field
//	133  7     reserved, zero
//	140  4     CRC-32C of bytes 0-139
//
// The commit identifier is opaque bytes supplied by the client, used to
// find out later whether a commit whose response was lost happened. It can
// be found only while its commit object is still in the log: once garbage
// collection deletes the object, a search for the identifier finds
// nothing, and not finding it means the outcome is unknown, not that the
// commit did not happen.
//
// Page table entry i, at offset 144 + 16i:
//
//	off  size  field
//	0    4     page number
//	4    4     record length: page size + 4 (codec 0)
//	8    8     record offset: 144 + 16n + i*(page size + 4)
//
// Page numbers are strictly ascending, at least 1, at most the database
// size, and never the lock page. Records are contiguous and the object
// ends at the end of the last record, so its size is
// 144 + 16n + n*(page size + 4), known before any page body is written.
// Record i is the record of the page in entry i; its identity carries
// "SKYLITEC" and the commit's database identifier, transaction number and
// epoch.
//
// An epoch bump has flag bit 0 set, no pages, and carries the database
// size of the commit before it. Any other commit has at least one page.
//
// Reading a page takes three range reads at most: the header, then the
// page table, then the record. Each is verified on its own: the header by
// its own CRC, the table by the CRC in the header, the record by its
// checksum. A reader checks a page entry it is given against the header
// before using it: a valid page number and an offset that is a record
// position. Such an entry must come from the page table of the same
// object.
//
// # Base group object
//
// The base holds the database's pages in groups of G consecutive pages; G
// is a power of two, and G * page size is at most 64 MiB. Group g holds
// pages g*G+1 through (g+1)*G, computed with 64-bit arithmetic. Each group
// object is immutable and its key is never reused for different bytes.
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
//	40   16    database identifier
//	56   4     database size in pages after that transaction, at most 4294967294
//	60   4     CRC-32C of bytes 0-59
//
// The group's first page, g*G+1, is at most the database size, so a group
// object holds at least one page of the database. Every slot is present.
// Pages never written are stored as zeros. The lock page, and every slot
// whose page number is beyond the database size (and so every slot beyond
// page 4294967294), must be zeros. Record i's identity carries
// "SKYLITEB", the group object's database identifier, transaction number
// and epoch, and page number g*G+1+i.
//
// Page p of group g is at offset 64 + (p - g*G - 1)*(page size + 4),
// computable from the manifest alone, so one range read fetches and
// verifies it: the manifest gives everything its identity needs. Such a
// read can check that the lock page is zeros but not the database-size
// rule, which needs the header. The whole object, 64 + G*(page size + 4)
// bytes, is one read.
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
//	20   4     database size in pages at the transaction below, at most 4294967294
//	24   8     transaction number the base is current to; 0 for none
//	32   8     epoch of the writer, at least 1
//	40   16    database identifier
//	56   4     group count, exactly ceil(database size / G)
//	60   4     epoch-bump count b
//	64   16    per group, in group order: transaction number, epoch
//	...  8     per epoch bump, ascending: its transaction number
//	...  4     CRC-32C of every byte before it
//
// Entry g names the object for group g by the transaction number it is
// current to and its writer's epoch: both zero means the group has no
// object and reads as zeros; otherwise the transaction number is at most
// the manifest's and the epoch at most the manifest's. Because entries are
// indexed by group, duplicate and out-of-range groups cannot be expressed.
//
// The epoch-bump list holds the transaction number of every epoch-bump
// commit at or below the manifest's transaction number: strictly
// ascending, each at least 1 and at most the manifest's transaction
// number. Garbage collection deletes a commit object only if its number is
// at or below the manifest's and not in this list; compaction maintains the
// list from the commits it folds. The list is kept for the life of the
// database, 8 bytes per takeover attempt that created a bump.
//
// The encoding has no free choices, so equal manifests are equal bytes.
// Its size is 68 + 16*ceil(database size / G) + 8b bytes, and at most
// MaxManifestSize (16 MiB). Encoders refuse to write and decoders refuse
// to read a larger one, with ErrManifestTooLarge. The limit is 16 times
// below the smallest single-request upload limit of the backends (Azure
// Blob Storage's 256 MiB for older service versions, 5000 MiB now; S3's 5
// GiB; Google Cloud Storage and the filesystem allow more), so a manifest
// is always one upload.
//
// With G = 256 at 4096-byte pages (1 MiB groups, the suggested default) a
// 100 GiB database has 102400 groups and a 1638468-byte manifest, 16 bytes
// per MiB of data, leaving room for 1892343 epoch bumps. Larger groups
// shrink the manifest and the object count and make read-ahead coarser;
// smaller groups make compaction rewrite less when writes are scattered,
// since a single changed page rewrites its whole group.
//
// # Supported sizes
//
// The format can represent databases of 4294967294 pages (16 TiB at
// 4096-byte pages). What is supported is less, set by two limits:
//
//   - The manifest: at most 1048571 groups with no epoch bumps, each bump
//     taking the space of half a group. At the default 1 MiB groups that
//     is a database just under 1 TiB (1048571 MiB). A 16 TiB database
//     needs groups of 32 MiB or more (524288 groups, an 8 MiB manifest).
//   - The page table: decoders accept a commit of up to 4294967294 pages,
//     a 68719476704-byte table, and the format sets no lower limit.
//     DecodeCommit holds the table and its decoded entries, 32 bytes per
//     page, so the reader's memory bounds a transaction: one that writes
//     1 GiB at 4096-byte pages has 262144 pages and a 4 MiB table.
//
// # Lease
//
// One small JSON object, replaced by compare-and-swap. Its only accepted
// form is
//
//	{"format":"skylite-lease","version":1,"holder":"H","epoch":E,"renewed":R}
//
// with exactly these fields in this order, no whitespace anywhere, and H,
// E and R replaced as follows. H, the holder, is 1 to 128 characters of
// A-Z a-z 0-9 . _ -, so it needs no escaping. E, the epoch the holder
// leads or intends to lead at, is from 1 to 18446744073709551615. R, the
// time the holder last refreshed the lease, is milliseconds since the Unix
// epoch (1970-01-01T00:00:00Z, ignoring leap seconds), from 0 to
// 9223372036854775807. Both are written in decimal ASCII digits with no
// sign, no leading zeros (zero is "0"), no fraction and no exponent. The
// longest lease is MaxLeaseSize (237) bytes. For example:
//
//	{"format":"skylite-lease","version":1,"holder":"node-a_10.0.0.7","epoch":3,"renewed":1794744000500}
//	{"format":"skylite-lease","version":1,"holder":"x","epoch":18446744073709551615,"renewed":0}
//
// A lease that does not start with {"format":"skylite-lease", is
// ErrNotFormat. One that continues with "version": and a run of digits
// other than exactly 1 is ErrVersion, whatever follows, since a later
// version may change everything after it. Any other deviation is
// ErrInvalid. The lease has no checksum, so damage is detected only if it
// leaves the lease no longer in this form; nothing safety-critical reads
// it.
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
// only what the builders produce. A commit object's header must carry its
// key's transaction number, and a base object's header its key's group,
// transaction number and epoch.
//
// # Garbage collection
//
// A commit object is garbage when its transaction number is at or below
// the manifest's and is not in the manifest's epoch-bump list.
//
// A base object the manifest names is live. One it does not name is
// garbage, judged against that manifest, exactly when:
//
//   - its epoch is lower than the manifest's: its writer can no longer
//     replace the manifest, so nothing will ever name it; or
//   - it carries the manifest's epoch and the compaction that wrote it has
//     ended. Only the writer of that epoch knows when that is, so this is
//     the engine's rule; the key alone does not decide it.
//
// One with a higher epoch than the manifest's is never garbage on that
// manifest's evidence: it may belong to a candidate that has not yet
// replaced the manifest. For example, with the manifest at transaction 9
// and epoch 3, a group object at transaction 9 and epoch 4 is kept. It
// cannot come from a compaction built from that manifest, whose outputs
// carry transaction numbers above 9, but whatever wrote it, its epoch
// alone keeps it.
package format
