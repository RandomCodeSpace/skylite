package format

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	commitMagic = "SKYLITEC"

	// CommitHeaderSize is the size of the fixed header at the front of a commit object.
	CommitHeaderSize = 144
	// PageEntrySize is the size of one page table entry.
	PageEntrySize = 16
	// MaxCommitIDLen is the longest client-supplied commit identifier.
	MaxCommitIDLen = 64

	// MaxCommitSize is the largest commit object, and so the largest
	// transaction this version supports; see the package documentation.
	MaxCommitSize = 1 << 30

	flagEpochBump = 1 << 0
	flagRetired   = 1 << 4
)

// CommitHeader is the fixed-size front of a commit object.
type CommitHeader struct {
	DB        DatabaseID
	PageSize  uint32
	Txn       uint64
	Epoch     uint64
	DBSize    uint32 // database size in pages after this commit
	PageCount uint32
	EpochBump bool
	// Retired reports a retired commit: this header and nothing after it.
	// The other fields keep the values of the commit it retired.
	Retired  bool
	CommitID []byte // opaque, at most MaxCommitIDLen bytes; nil when absent

	tableCRC uint32 // set by DecodeCommitHeader, checked by DecodeCommitTable
}

// PageEntry locates one page record within a commit object.
type PageEntry struct {
	Pgno   uint32
	Length uint32 // record length: stored body plus its 4-byte CRC-32C
	Offset uint64
}

// TableSize is the size of the page table, which starts at CommitHeaderSize.
// For a retired header it is the size of the retired commit's table, which
// the retired object no longer holds.
func (h CommitHeader) TableSize() int64 { return int64(h.PageCount) * PageEntrySize }

// Size is the size of the whole commit object. For a retired header it is
// the size of the commit it retired; the retired object is
// CommitHeaderSize bytes.
func (h CommitHeader) Size() int64 {
	return CommitHeaderSize + h.TableSize() + int64(h.PageCount)*h.recordLen()
}

func (h CommitHeader) recordLen() int64 { return int64(h.PageSize) + recordTrailer }

func (h CommitHeader) validate() error {
	if err := checkDB(h.DB); err != nil {
		return err
	}
	switch {
	case !validPageSize(h.PageSize):
		return invalid("page size %d", h.PageSize)
	case h.Txn == 0:
		return invalid("transaction number 0")
	case h.Epoch == 0:
		return invalid("epoch 0")
	case h.DBSize > MaxPages:
		return invalid("database size %d pages exceeds %d", h.DBSize, uint32(MaxPages))
	case h.PageCount > h.DBSize:
		return invalid("%d pages in a database of %d pages", h.PageCount, h.DBSize)
	case h.Size() > MaxCommitSize:
		return fmt.Errorf("%w: %d bytes", ErrCommitTooLarge, h.Size())
	case h.EpochBump && h.Retired:
		return invalid("retired epoch bump")
	case h.EpochBump && h.PageCount != 0:
		return invalid("epoch bump with %d pages", h.PageCount)
	case !h.EpochBump && h.PageCount == 0:
		return invalid("commit without pages")
	case len(h.CommitID) > MaxCommitIDLen:
		return invalid("commit identifier of %d bytes", len(h.CommitID))
	}
	return nil
}

// CheckKey returns an ErrMisplaced error unless h belongs under the commit
// key of database db and transaction txn, as Keys.ParseCommit returns them.
// Callers must check every commit header they decode against the key they
// fetched it under.
func (h CommitHeader) CheckKey(db DatabaseID, txn uint64) error {
	if h.DB != db || h.Txn != txn {
		return misplaced("commit of database %x transaction %d under the key of database %x transaction %d", h.DB, h.Txn, db, txn)
	}
	return nil
}

// checkPgno checks the page following prev (0 for the first page).
func (h CommitHeader) checkPgno(prev, pgno uint32) error {
	switch {
	case pgno <= prev:
		return invalid("page %d after page %d", pgno, prev)
	case pgno > h.DBSize:
		return invalid("page %d beyond database size %d", pgno, h.DBSize)
	case pgno == LockPage(h.PageSize):
		return invalid("page %d is the lock page", pgno)
	}
	return nil
}

func (h CommitHeader) tableSize() uint64 { return uint64(h.PageCount) * PageEntrySize }

func (h CommitHeader) recordCRC(pgno uint32, body []byte) uint32 {
	return recordCRC(commitMagic, h.DB, h.Txn, h.Epoch, uint64(pgno), body)
}

func (h CommitHeader) entry(i, pgno uint32) PageEntry {
	return PageEntry{
		Pgno:   pgno,
		Length: h.PageSize + recordTrailer,
		Offset: CommitHeaderSize + h.tableSize() + uint64(i)*uint64(h.PageSize+recordTrailer),
	}
}

func putEntry(b []byte, e PageEntry) {
	le.PutUint32(b[0:], e.Pgno)
	le.PutUint32(b[4:], e.Length)
	le.PutUint64(b[8:], e.Offset)
}

func (h CommitHeader) encode(tableCRC uint32) []byte {
	b := make([]byte, CommitHeaderSize)
	copy(b, commitMagic)
	le.PutUint16(b[8:], Version)
	var flags uint16
	if h.EpochBump {
		flags |= flagEpochBump
	}
	if h.Retired {
		flags |= flagRetired
	}
	le.PutUint16(b[10:], flags)
	le.PutUint32(b[12:], h.PageSize)
	le.PutUint64(b[16:], h.Txn)
	le.PutUint64(b[24:], h.Epoch)
	le.PutUint32(b[32:], h.DBSize)
	le.PutUint32(b[36:], h.PageCount)
	le.PutUint64(b[40:], h.tableSize())
	le.PutUint32(b[48:], tableCRC)
	copy(b[52:], h.DB[:])
	b[68] = byte(len(h.CommitID)) //nolint:gosec // validate bounds it by MaxCommitIDLen
	copy(b[69:], h.CommitID)
	le.PutUint32(b[140:], checksum(b[:140]))
	return b
}

// EncodeCommit writes a commit object to w. pgnos lists the pages in
// ascending order and pages supplies their bodies, h.PageSize bytes each, in
// the same order. Only one page body is held in memory at a time, and the
// object is exactly h.Size() bytes. h.PageCount must equal len(pgnos), and
// h.Retired must be false; Retire writes the retired form.
// EncodeCommit reads exactly len(pgnos)*h.PageSize bytes from pages and
// nothing after them. Errors from w are returned unchanged; errors reading
// pages are wrapped with the page number. Neither is a format class.
func EncodeCommit(w io.Writer, h CommitHeader, pgnos []uint32, pages io.Reader) error {
	if err := h.validate(); err != nil {
		return err
	}
	if h.Retired {
		return invalid("retired commit given to EncodeCommit")
	}
	if int64(len(pgnos)) != int64(h.PageCount) {
		return invalid("%d page numbers for a page count of %d", len(pgnos), h.PageCount)
	}
	var prev uint32
	for _, p := range pgnos {
		if err := h.checkPgno(prev, p); err != nil {
			return err
		}
		prev = p
	}

	// The table CRC goes in the header, so the table is generated twice
	// rather than held in memory.
	var e [PageEntrySize]byte
	var tableCRC uint32
	for i := range h.PageCount {
		putEntry(e[:], h.entry(i, pgnos[i]))
		tableCRC = crc32.Update(tableCRC, castagnoli, e[:])
	}

	bw := bufio.NewWriterSize(w, 64<<10)
	if _, err := bw.Write(h.encode(tableCRC)); err != nil {
		return err
	}
	for i := range h.PageCount {
		putEntry(e[:], h.entry(i, pgnos[i]))
		if _, err := bw.Write(e[:]); err != nil {
			return err
		}
	}
	rec := make([]byte, h.recordLen())
	body := rec[:h.PageSize]
	for _, p := range pgnos {
		if _, err := io.ReadFull(pages, body); err != nil {
			return fmt.Errorf("format: reading page %d: %w", p, err)
		}
		le.PutUint32(rec[h.PageSize:], h.recordCRC(p, body))
		if _, err := bw.Write(rec); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// Retire returns the retired form of the commit h describes: its header
// with the retired flag set and the header checksum recomputed, every other
// field unchanged. h must come from DecodeCommitHeader or DecodeCommit,
// because the retired form keeps the page table checksum, which only they
// carry. An epoch bump is never retired. A retired h gives the same bytes.
func (h CommitHeader) Retire() ([]byte, error) {
	h.Retired = true
	if err := h.validate(); err != nil {
		return nil, err
	}
	return h.encode(h.tableCRC), nil
}

// DecodeCommitHeader decodes the first CommitHeaderSize bytes of a commit
// object of database db. A retired commit's header decodes with Retired set.
func DecodeCommitHeader(b []byte, db DatabaseID) (CommitHeader, error) {
	if err := checkHeader(b, commitMagic, CommitHeaderSize); err != nil {
		return CommitHeader{}, err
	}
	f := le.Uint16(b[10:])
	if f&^(flagEpochBump|flagRetired) != 0 {
		return CommitHeader{}, fmt.Errorf("%w: flags %#x", ErrVersion, f)
	}
	idLen := int(b[68])
	if idLen > MaxCommitIDLen {
		return CommitHeader{}, invalid("commit identifier of %d bytes", idLen)
	}
	if !allZero(b[69+idLen : 140]) {
		return CommitHeader{}, invalid("nonzero padding")
	}
	h := CommitHeader{
		PageSize:  le.Uint32(b[12:]),
		Txn:       le.Uint64(b[16:]),
		Epoch:     le.Uint64(b[24:]),
		DBSize:    le.Uint32(b[32:]),
		PageCount: le.Uint32(b[36:]),
		EpochBump: f&flagEpochBump != 0,
		Retired:   f&flagRetired != 0,
		tableCRC:  le.Uint32(b[48:]),
	}
	copy(h.DB[:], b[52:68])
	if idLen > 0 {
		h.CommitID = append([]byte(nil), b[69:69+idLen]...)
	}
	if err := h.validate(); err != nil {
		return CommitHeader{}, err
	}
	if ts := le.Uint64(b[40:]); ts != h.tableSize() {
		return CommitHeader{}, invalid("table size %d for %d pages", ts, h.PageCount)
	}
	if h.DB != db {
		return CommitHeader{}, misplaced("commit of database %x, want %x", h.DB, db)
	}
	return h, nil
}

// DecodeCommitTable decodes the page table, the h.TableSize() bytes that
// follow the header. h must come from DecodeCommitHeader. A retired h gives
// ErrRetired.
func DecodeCommitTable(h CommitHeader, b []byte) ([]PageEntry, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	if h.Retired {
		return nil, ErrRetired
	}
	if int64(len(b)) != h.TableSize() {
		return nil, invalid("page table is %d bytes, want %d", len(b), h.TableSize())
	}
	if checksum(b) != h.tableCRC {
		return nil, fmt.Errorf("%w: page table", ErrChecksum)
	}
	entries := make([]PageEntry, h.PageCount)
	var prev uint32
	for i := range h.PageCount {
		x := b[uint64(i)*PageEntrySize:]
		e := PageEntry{Pgno: le.Uint32(x), Length: le.Uint32(x[4:]), Offset: le.Uint64(x[8:])}
		if err := h.checkPgno(prev, e.Pgno); err != nil {
			return nil, err
		}
		if e != h.entry(i, e.Pgno) {
			return nil, invalid("page %d: offset %d length %d", e.Pgno, e.Offset, e.Length)
		}
		entries[i] = e
		prev = e.Pgno
	}
	return entries, nil
}

// DecodeCommitPage verifies one page record, the e.Length bytes at e.Offset,
// and returns the page body, which aliases b. h must come from
// DecodeCommitHeader and e from DecodeCommitTable for the same object. A
// record of another page, object or database fails its checksum. A retired
// h gives ErrRetired.
func DecodeCommitPage(h CommitHeader, e PageEntry, b []byte) ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	if h.Retired {
		return nil, ErrRetired
	}
	if err := h.checkPgno(0, e.Pgno); err != nil {
		return nil, err
	}
	start, rl := CommitHeaderSize+h.tableSize(), uint64(h.PageSize)+recordTrailer
	if e.Offset < start || (e.Offset-start)%rl != 0 || (e.Offset-start)/rl >= uint64(h.PageCount) {
		return nil, invalid("page %d: offset %d is not a record position", e.Pgno, e.Offset)
	}
	if int64(e.Length) != h.recordLen() || int64(len(b)) != h.recordLen() {
		return nil, invalid("page %d record is %d bytes, entry says %d, want %d", e.Pgno, len(b), e.Length, h.recordLen())
	}
	body := b[:h.PageSize]
	if le.Uint32(b[h.PageSize:]) != h.recordCRC(e.Pgno, body) {
		return nil, fmt.Errorf("%w: page %d", ErrChecksum, e.Pgno)
	}
	return body, nil
}

// DecodeCommit reads and verifies a whole commit object of database db from
// r: header, page table, every page, and that nothing follows the last page.
// It calls fn, if not nil, with each page in order; the page slice is reused
// after fn returns. Errors from r and from fn are returned unchanged, not
// wrapped in a format class. It judges the magic and version from the first
// ten bytes before reading more. A retired commit, which must end at its
// header, gives its header, no entries and ErrRetired.
func DecodeCommit(r io.Reader, db DatabaseID, fn func(pgno uint32, page []byte) error) (CommitHeader, []PageEntry, error) {
	hb := make([]byte, CommitHeaderSize)
	if n, err := io.ReadFull(r, hb[:versionEnd]); err != nil {
		if short(err) {
			err = checkMagicVersion(hb[:n], commitMagic)
		}
		return CommitHeader{}, nil, err
	}
	if err := checkMagicVersion(hb[:versionEnd], commitMagic); err != nil {
		return CommitHeader{}, nil, err
	}
	if n, err := io.ReadFull(r, hb[versionEnd:]); err != nil {
		if short(err) {
			err = invalid("header truncated at %d bytes", versionEnd+n)
		}
		return CommitHeader{}, nil, err
	}
	h, err := DecodeCommitHeader(hb, db)
	if err != nil {
		return CommitHeader{}, nil, err
	}
	if h.Retired {
		if err := atEOF(r); err != nil {
			return CommitHeader{}, nil, err
		}
		return h, nil, ErrRetired
	}
	// ReadAll grows with the bytes actually present, not with TableSize.
	tb, err := io.ReadAll(io.LimitReader(r, h.TableSize()))
	if err != nil {
		return CommitHeader{}, nil, err
	}
	entries, err := DecodeCommitTable(h, tb)
	if err != nil {
		return CommitHeader{}, nil, err
	}
	rec := make([]byte, h.recordLen())
	for _, e := range entries {
		if _, err := io.ReadFull(r, rec); err != nil {
			if short(err) {
				err = invalid("truncated in page %d", e.Pgno)
			}
			return CommitHeader{}, nil, err
		}
		body, err := DecodeCommitPage(h, e, rec)
		if err != nil {
			return CommitHeader{}, nil, err
		}
		if fn != nil {
			if err := fn(e.Pgno, body); err != nil {
				return CommitHeader{}, nil, err
			}
		}
	}
	if err := atEOF(r); err != nil {
		return CommitHeader{}, nil, err
	}
	return h, entries, nil
}

func short(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func atEOF(r io.Reader) error {
	var one [1]byte
	switch _, err := io.ReadFull(r, one[:]); {
	case err == nil:
		return invalid("trailing bytes")
	case errors.Is(err, io.EOF):
		return nil
	default:
		return err
	}
}
