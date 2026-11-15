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
	CommitHeaderSize = 128
	// PageEntrySize is the size of one page table entry.
	PageEntrySize = 16
	// MaxCommitIDLen is the longest client-supplied commit identifier.
	MaxCommitIDLen = 64

	flagEpochBump = 1 << 0
)

// CommitHeader is the fixed-size front of a commit object.
type CommitHeader struct {
	PageSize  uint32
	Txn       uint64
	Epoch     uint64
	DBSize    uint32 // database size in pages after this commit
	PageCount uint32
	EpochBump bool
	CommitID  []byte // opaque, at most MaxCommitIDLen bytes; nil when absent

	tableCRC uint32 // set by DecodeCommitHeader, checked by DecodeCommitTable
}

// PageEntry locates one page record within a commit object.
type PageEntry struct {
	Pgno   uint32
	Length uint32 // record length: stored body plus its 4-byte CRC-32C
	Offset uint64
}

// TableSize is the size of the page table, which starts at CommitHeaderSize.
func (h CommitHeader) TableSize() int64 { return int64(h.PageCount) * PageEntrySize }

// Size is the size of the whole commit object.
func (h CommitHeader) Size() int64 {
	return CommitHeaderSize + h.TableSize() + int64(h.PageCount)*h.recordLen()
}

func (h CommitHeader) recordLen() int64 { return int64(h.PageSize) + recordTrailer }

func (h CommitHeader) validate() error {
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
	case h.EpochBump && h.PageCount != 0:
		return invalid("epoch bump with %d pages", h.PageCount)
	case !h.EpochBump && h.PageCount == 0:
		return invalid("commit without pages")
	case len(h.CommitID) > MaxCommitIDLen:
		return invalid("commit identifier of %d bytes", len(h.CommitID))
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
	if h.EpochBump {
		le.PutUint16(b[10:], flagEpochBump)
	}
	le.PutUint32(b[12:], h.PageSize)
	le.PutUint64(b[16:], h.Txn)
	le.PutUint64(b[24:], h.Epoch)
	le.PutUint32(b[32:], h.DBSize)
	le.PutUint32(b[36:], h.PageCount)
	le.PutUint64(b[40:], h.tableSize())
	le.PutUint32(b[48:], tableCRC)
	b[52] = byte(len(h.CommitID)) //nolint:gosec // validate bounds it by MaxCommitIDLen
	copy(b[53:], h.CommitID)
	le.PutUint32(b[124:], checksum(b[:124]))
	return b
}

// EncodeCommit writes a commit object to w. pgnos lists the pages in
// ascending order and pages supplies their bodies, h.PageSize bytes each, in
// the same order. Only one page body is held in memory at a time, and the
// object is exactly h.Size() bytes. h.PageCount must equal len(pgnos).
func EncodeCommit(w io.Writer, h CommitHeader, pgnos []uint32, pages io.Reader) error {
	if err := h.validate(); err != nil {
		return err
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
		le.PutUint32(rec[h.PageSize:], checksum(body))
		if _, err := bw.Write(rec); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// DecodeCommitHeader decodes the first CommitHeaderSize bytes of a commit object.
func DecodeCommitHeader(b []byte) (CommitHeader, error) {
	if err := checkHeader(b, commitMagic, CommitHeaderSize); err != nil {
		return CommitHeader{}, err
	}
	if f := le.Uint16(b[10:]); f&^flagEpochBump != 0 {
		return CommitHeader{}, fmt.Errorf("%w: flags %#x", ErrVersion, f)
	}
	idLen := int(b[52])
	if idLen > MaxCommitIDLen {
		return CommitHeader{}, invalid("commit identifier of %d bytes", idLen)
	}
	if !allZero(b[53+idLen : 124]) {
		return CommitHeader{}, invalid("nonzero padding")
	}
	h := CommitHeader{
		PageSize:  le.Uint32(b[12:]),
		Txn:       le.Uint64(b[16:]),
		Epoch:     le.Uint64(b[24:]),
		DBSize:    le.Uint32(b[32:]),
		PageCount: le.Uint32(b[36:]),
		EpochBump: le.Uint16(b[10:])&flagEpochBump != 0,
		tableCRC:  le.Uint32(b[48:]),
	}
	if idLen > 0 {
		h.CommitID = append([]byte(nil), b[53:53+idLen]...)
	}
	if err := h.validate(); err != nil {
		return CommitHeader{}, err
	}
	if ts := le.Uint64(b[40:]); ts != h.tableSize() {
		return CommitHeader{}, invalid("table size %d for %d pages", ts, h.PageCount)
	}
	return h, nil
}

// DecodeCommitTable decodes the page table, the h.TableSize() bytes that
// follow the header. h must come from DecodeCommitHeader.
func DecodeCommitTable(h CommitHeader, b []byte) ([]PageEntry, error) {
	if err := h.validate(); err != nil {
		return nil, err
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
// and returns the page body, which aliases b.
func DecodeCommitPage(h CommitHeader, e PageEntry, b []byte) ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	if int64(e.Length) != h.recordLen() || int64(len(b)) != h.recordLen() {
		return nil, invalid("page %d record is %d bytes, entry says %d, want %d", e.Pgno, len(b), e.Length, h.recordLen())
	}
	body := b[:h.PageSize]
	if le.Uint32(b[h.PageSize:]) != checksum(body) {
		return nil, fmt.Errorf("%w: page %d", ErrChecksum, e.Pgno)
	}
	return body, nil
}

// DecodeCommit reads and verifies a whole commit object from r: header,
// page table, every page, and that nothing follows the last page. It calls
// fn, if not nil, with each page in order; the page slice is reused after fn
// returns.
func DecodeCommit(r io.Reader, fn func(pgno uint32, page []byte) error) (CommitHeader, []PageEntry, error) {
	hb := make([]byte, CommitHeaderSize)
	if n, err := io.ReadFull(r, hb); err != nil {
		if short(err) {
			_, err = DecodeCommitHeader(hb[:n])
		}
		return CommitHeader{}, nil, err
	}
	h, err := DecodeCommitHeader(hb)
	if err != nil {
		return CommitHeader{}, nil, err
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
