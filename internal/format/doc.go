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
// of every commit and base group object carry it, the key of every commit
// and base group object contains it, and every page record's checksum
// covers it, so an object or record copied from one database to another is
// rejected.
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
//	32   8     epoch of a commit object; tag of a base group object
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
// another database, or under another key). Two ErrInvalid errors can be
// told apart from the rest of their class with errors.Is, because callers
// report them: ErrManifestTooLarge, for a manifest larger than
// MaxManifestSize, and ErrCommitTooLarge, for a commit object larger than
// MaxCommitSize. The one other outcome is ErrRetired, which is not a format
// error and wraps no class: the bytes are a valid retired commit, and the
// caller asked for its page table or pages; see Retired commit.
//
// A binary object is checked part by part from the front: the header (the
// whole manifest is one part), then the page table, then each record. A
// header is checked in this order: magic, version (as soon as its two
// bytes are present, before any later byte is read), length, checksum,
// flags, fields, database identifier. A reader must reject everything this
// specification does not allow; there are no optional or ignorable parts.
// Every accepted object re-encodes to exactly its input bytes.
//
// Decoders also take the identifier of the database the caller expects,
// and reject an object of another with ErrMisplaced. The one exception is
// discovery: DecodeManifest given the all-zero identifier accepts any, and
// the caller learns the identifier from it. The manifest is the only
// object a reader can find without knowing the identifier, so trust in a
// discovered identifier rests on the key the manifest was read from, and
// every read after discovery passes the identifier it learned.
//
// A header does not say which key it was fetched under, so the caller must
// check that too: CommitHeader.CheckKey compares a commit's database
// identifier and transaction number with its key's, BaseHeader.CheckKey a
// group object's database identifier, group, transaction number and tag
// with its key's, and both return ErrMisplaced on a mismatch. Callers must
// call them for every object they fetch by key.
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
// the page table with a buffer that grows with the bytes that arrive, and
// the commit size limit, checked from the header, bounds it.
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
// # What the decoders check and what the engine must
//
// The decoders check what the bytes of one object, its expected database
// identifier and, through CheckKey, its key can show: everything this
// specification states as a layout or a field rule. The rules below
// relate objects to each other or to the history of storage, so no codec
// can check them. They are the engine's obligations, and the format's
// correctness rests on them:
//
//   - An epoch belongs to exactly one leader candidacy: no two writers ever
//     write with the same epoch.
//   - Commit transaction numbers are contiguous from 1. A log key is
//     created once, by create-if-absent, and may be replaced once, by the
//     retired form of the commit it holds, with a replace conditional on
//     the version read; it is never deleted. So a log key names one commit
//     for the life of the database, first whole and then perhaps retired.
//   - An epoch bump carries the database size of the commit before it.
//   - An epoch bump is never retired, and a commit is retired only once
//     its transaction number is at or below that of a manifest read from
//     storage, or of a manifest replacement of the collector's own that
//     succeeded.
//   - The page size and the group size never change for the life of a
//     database identifier.
//   - A manifest replacement obeys these against the manifest it
//     replaces: the database identifier, page size and group size are
//     unchanged; the transaction number, retired-through number and epoch
//     never decrease; a writer replaces only a manifest whose epoch is at
//     most its own; and the bytes change. An epoch swap, by a candidate
//     taking over, raises the epoch by one, sets a new random writer nonce
//     and carries every group reference and the retired-through number
//     unchanged; a compaction, by the leader, keeps the epoch and nonce of
//     that leader's swap and raises the transaction number.
//   - Every commit object at or below the manifest's retired-through
//     number is in its retired form or is an epoch bump.
//   - A manifest carries forward references its predecessor had and adds
//     references to objects its own compaction attempt created, and
//     nothing else. So it never references another attempt's object, and
//     never brings back a reference a predecessor dropped.
//   - Each compaction attempt draws a random 64-bit tag and writes its
//     output under keys carrying that tag and a transaction number strictly
//     greater than that of the manifest it was built from, so no two
//     attempts share a base key. An attempt that cannot confirm a create
//     as its own moves that group to a new tag; it never adopts an object
//     it did not create.
//   - Every create of one base key carries the same bytes: the key belongs
//     to one attempt, which binds it to one encoding of the group's content
//     at that transaction number.
//   - Base objects are deleted only as Garbage collection describes.
//   - Every object fetched by key is checked with CheckKey, and every read
//     after the manifest's discovery passes the identifier it learned.
//
// So there is no predecessor digest and no writer-instance field. A log
// key names one commit ever, so there is no second history for a digest
// to detect, and a writer is identified by its epoch, which no other
// writer shares. If storage broke create-if-absent, or two writers shared
// an epoch, the format would not notice.
//
// Epochs need not be non-decreasing along the commit log, and readers must
// not require it: a paused candidate can leave an epoch bump with a lower
// epoch after commits of a higher one.
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
//	10   2     flags: bit 0 epoch bump; bits 1-3 codec, 0; bit 4 retired; other bits 0
//	12   4     page size
//	16   8     transaction number, at least 1
//	24   8     epoch of the writer, at least 1
//	32   4     database size in pages after this commit, at most 4294967294
//	36   4     page count n, at most the database size
//	40   8     page table size, 16n
//	48   4     CRC-32C of the page table
//	52   16    database identifier
//	68   1     commit identifier length, at most 64; 0 in an epoch bump
//	69   64    commit identifier, then zeros to fill the field
//	133  7     reserved, zero
//	140  4     CRC-32C of bytes 0-139
//
// The commit identifier is an opaque byte string of up to 64 bytes, composed
// by the server, not supplied by the client; in this version it is a
// 24-byte value. It is used to find out later whether a commit whose
// response was lost happened, and is compared as exact bytes. Length 0
// means none. The retired form keeps it, so it stays in the log for the
// life of the database. An epoch bump has none: a header with bit 0 set
// and a non-zero identifier length is ErrInvalid.
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
// An epoch bump has flag bit 0 set and no pages. Any other commit has at
// least one page. A commit object is at most MaxCommitSize (1 GiB) bytes,
// computed from the header with 64-bit arithmetic as above; a larger one is
// ErrCommitTooLarge, from the encoder and from every decoder of its header,
// before any of its page table is read. Bit 4 is clear in every commit
// object written by EncodeCommit.
//
// Reading a page takes three range reads at most: the header, then the
// page table, then the record. Each is verified on its own: the header by
// its own CRC, the table by the CRC in the header, the record by its
// checksum. A reader checks a page entry it is given against the header
// before using it: a valid page number and an offset that is a record
// position. Such an entry must come from the page table of the same
// object.
//
// # Retired commit
//
// Garbage collection replaces a commit whose pages the base covers with
// its retired form: the commit's 144-byte header with flag bit 4 set and
// the CRC at offset 140 recomputed, and nothing after it. Every other
// header byte keeps its value: page size, transaction number, epoch,
// database size, page count, page table size and checksum, database
// identifier and commit identifier. So the retired form is a pure function
// of the commit's header bytes, and a header with bit 4 set describes, in
// every other field, the commit it retired. An epoch bump is never retired:
// a header with bits 0 and 4 both set is ErrInvalid.
//
// A retired object is exactly 144 bytes; DecodeCommit rejects bytes after
// the header with ErrInvalid. DecodeCommitHeader decodes a retired header
// and reports it with Retired set. DecodeCommitTable and DecodeCommitPage,
// given a retired header, and DecodeCommit, given a retired object, return
// ErrRetired, DecodeCommit together with the header. An empty table and no
// pages would be mistaken for an epoch bump or an empty commit, and both
// would let a caller that ignores Retired apply nothing and carry on, so
// they are not returned. A reader that needs the pages of a retired commit
// must go to the base; how the engine handles that is not part of the
// format. CommitHeader.Retire produces the retired form from a decoded
// header.
//
// # Base group object
//
// The base holds the database's pages in groups of G consecutive pages; G
// is a power of two, and G * page size is at most 64 MiB. Group g holds
// pages g*G+1 through (g+1)*G, computed with 64-bit arithmetic. Each group
// object is immutable, and its key belongs to the one compaction attempt
// whose random tag it carries. Layout:
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
//	32   8     tag of the compaction attempt that wrote it, any value
//	40   16    database identifier
//	56   4     database size in pages after that transaction, at most 4294967294
//	60   4     CRC-32C of bytes 0-59
//
// The group's first page, g*G+1, is at most the database size, so a group
// object holds at least one page of the database. Every slot is present
// and is a full record: a body of page size bytes followed by its record
// checksum. The body of a page never written is zeros, still followed by
// the checksum of its identity and that zero body. The lock page, and every
// slot whose page number is beyond the database size (and so every slot
// beyond page 4294967294), must have a zero body. Record i's identity carries
// "SKYLITEB", the group object's database identifier, transaction number
// and tag, and page number g*G+1+i.
//
// The tag is a random 64-bit number the attempt draws from a
// cryptographically secure source. Zero is a valid tag. It makes the
// object's key, and through the record checksums its bytes, unique to one
// attempt, so a group written by one attempt never verifies as another's.
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
//	56   16    writer nonce, not all zeros
//	72   8     retired-through transaction number, at most the one at 24
//	80   4     group count, exactly ceil(database size / G)
//	84   16    per group, in group order: transaction number, tag
//	...  4     CRC-32C of every byte before it
//
// Entry g names the object for group g by the transaction number it is
// current to and the tag of the attempt that wrote it: the base key
// <group>-<txn>-<tag>. Transaction number 0 means the group has no object
// and reads as zeros; the tag must then be 0 too (a non-zero tag with
// transaction number 0 is ErrInvalid), so there is one encoding of no
// object. Otherwise the transaction number is at most the manifest's, and
// the tag may be any value, 0 included. Because entries are indexed by
// group, duplicate and out-of-range groups cannot be expressed.
//
// The retired-through number says that every commit object at or below it
// is in its retired form or is an epoch bump, so a leader taking office
// resumes retirement just above it. It is 0 when the database is created,
// at most the manifest's transaction number (a larger one is ErrInvalid),
// and never decreases across replacements. Only the bound is the format's
// to check; that the claim is true and never decreases is the engine's
// obligation, like the other rules relating objects to each other. No
// reader relies on it for correctness.
//
// The writer nonce is 16 random bytes, set by a candidate when it swaps the
// manifest to a new epoch and kept by that leader's later manifests, so a
// candidate can tell its own swap from another's at the same epoch.
//
// The first manifest of a database has transaction number 0,
// retired-through 0, epoch 1, a new writer nonce, database size 0 and so no
// group entries: 88 bytes. Its creator writes it after the commit at
// transaction number 1, epoch 1, which holds the empty database.
//
// The encoding has no free choices, so equal manifests are equal bytes.
// Its size is 88 + 16*ceil(database size / G) bytes, and at most
// MaxManifestSize (16 MiB). Encoders refuse to write and decoders refuse
// to read a larger one, with ErrManifestTooLarge. The limit is 16 times
// below the smallest single-request upload limit of the backends (Azure
// Blob Storage's 256 MiB for older service versions, 5000 MiB now; S3's 5
// GiB; Google Cloud Storage and the filesystem allow more), so a manifest
// is always one upload.
//
// With G = 256 at 4096-byte pages (1 MiB groups, the suggested default) a
// 100 GiB database has 102400 groups and a 1638488-byte manifest, 16 bytes
// per MiB of data. Larger groups shrink the manifest and the object count
// and make read-ahead coarser; smaller groups make compaction rewrite less
// when writes are scattered, since a single changed page rewrites its
// whole group.
//
// # Supported sizes
//
// The format can represent databases of 4294967294 pages (16 TiB at
// 4096-byte pages). What is supported is less, set by two limits.
//
// The manifest holds at most 1048570 groups, a manifest of 16777208 bytes,
// 8 bytes below MaxManifestSize. At the default 1 MiB groups that is a
// database of 1048570 MiB, just under 1 TiB. A 16 TiB database needs
// groups of 32 MiB or more (524288 groups, an 8388696-byte manifest).
//
// A commit object is at most MaxCommitSize, 1 GiB, and this is the
// largest transaction this version supports. A commit of n pages is
// 144 + n*(page size + 20) bytes, so a commit holds at most:
//
//	page size  pages    page data
//	512        2018311  985.5 MiB
//	1024       1028488  1004.4 MiB
//	2048       519217   1014.1 MiB
//	4096       260870   1019.0 MiB
//	8192       130752   1021.5 MiB
//	16384      65456    1022.8 MiB
//	32768      32748    1023.4 MiB
//	65536      16378    1023.6 MiB
//
// No commit object is exactly 1 GiB, since 2^30 - 144 is not a multiple of
// any page size plus 20, so at each page size the limit is the page count
// tabled. The limit bounds a decoder's memory: DecodeCommit holds the page
// table and its decoded entries, 32 bytes per page, at most about 62 MiB
// at 512-byte pages. It also keeps a commit within one upload on every
// backend at its current single-request limit, the smallest of which is
// the Azurite emulator's 2 GiB. Azure Blob Storage service versions before
// 2019-12-12 accept only 256 MiB in one request, so the engine must not
// use them for commits.
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
// prefix of one or more segments, at most 937 bytes and 29 segments, so
// that every key below fits (the longest, a base key, adds 87 bytes and 3
// segments):
//
//	<prefix>/<id>/log/<txn>                commit object
//	<prefix>/<id>/base/<group>-<txn>-<tag> base group object
//	<prefix>/manifest                      manifest
//	<prefix>/lease                         lease
//
// id is the database identifier as 32 lowercase hexadecimal characters,
// byte 0 first; uppercase, another length and the all-zero identifier are
// invalid. txn is 20 decimal digits and group is 10, zero-padded, so byte
// order is numeric order. tag is the base object's tag as 16 lowercase
// hexadecimal characters, zero-padded, most significant digit first;
// uppercase and another length are invalid. Parsers accept only what the
// builders produce. A commit object's header must carry its key's database
// identifier and transaction number, and a base object's header its key's
// database identifier, group, transaction number and tag. Because every
// log and base key carries the identifier, a request an actor issues for
// one database, however late it lands, cannot touch the objects of a
// database later created under the same prefix with a new identifier.
//
// # Garbage collection
//
// Garbage collection deletes no commit object. It retires a commit once its
// transaction number is at or below a manifest's, by replacing the object,
// conditional on the version it read, with the commit's retired form. An
// epoch bump is never retired. The manifest's transaction number never
// decreases, so once a commit may be retired it may be retired forever, and
// a retire whose outcome is unknown needs no resolution.
//
// Base objects are the only objects deleted, under one rule: a base object
// is garbage when a manifest read from authoritative storage does not
// reference it and has a transaction number at or above the transaction
// number in the object's key. An unreferenced object whose key's
// transaction number is above the manifest's is kept. The rule holds
// because a manifest carries forward references its predecessor had and
// adds references to objects its own attempt created, and nothing else,
// and an attempt publishes by replacing a manifest whose transaction
// number is below that of its output: once a manifest at or above it has
// been current, no later manifest can first reference the object. A
// delete whose outcome is unknown therefore needs no resolution.
//
// A base key can reappear after its deletion, through a delayed create of
// its own attempt, with identical bytes; it is then garbage under the same
// rule. Nothing is ever conditioned on a base object's version. These are
// the engine's rules: the format records the transaction numbers, tags and
// references they need and does not enforce them.
package format
