package format

import (
	"bufio"
	"fmt"
	"io"
)

const (
	baseMagic = "SKYLITEB"

	// BaseHeaderSize is the size of the fixed header at the front of a base group object.
	BaseHeaderSize = 64
)

// BaseHeader is the fixed-size front of a base group object. Group g holds
// pages g*GroupSize+1 through (g+1)*GroupSize.
type BaseHeader struct {
	DB        DatabaseID
	PageSize  uint32
	GroupSize uint32 // pages per group
	Group     uint32
	Txn       uint64 // transaction number the group is current to
	Epoch     uint64 // epoch of the writer
	DBSize    uint32 // database size in pages after transaction Txn
}

// Size is the size of the whole group object.
func (h BaseHeader) Size() int64 {
	return BaseHeaderSize + int64(h.GroupSize)*h.recordLen()
}

func (h BaseHeader) recordLen() int64 { return int64(h.PageSize) + recordTrailer }

func (h BaseHeader) first() uint64 { return uint64(h.Group)*uint64(h.GroupSize) + 1 }

// validate checks the fields a reader can take from a manifest entry, all
// but DBSize.
func (h BaseHeader) validate() error {
	if err := checkDB(h.DB); err != nil {
		return err
	}
	switch {
	case !validPageSize(h.PageSize):
		return invalid("page size %d", h.PageSize)
	case !validGroupSize(h.PageSize, h.GroupSize):
		return invalid("group size %d at page size %d", h.GroupSize, h.PageSize)
	case h.first() > MaxPages:
		return invalid("group %d starts beyond page %d", h.Group, uint32(MaxPages))
	case h.Txn == 0:
		return invalid("transaction number 0")
	case h.Epoch == 0:
		return invalid("epoch 0")
	}
	return nil
}

// validateObject checks every field of a header read from or written to
// an object.
func (h BaseHeader) validateObject() error {
	if err := h.validate(); err != nil {
		return err
	}
	switch {
	case h.DBSize > MaxPages:
		return invalid("database size %d pages exceeds %d", h.DBSize, uint32(MaxPages))
	case h.first() > uint64(h.DBSize):
		return invalid("group %d starts beyond the database size %d", h.Group, h.DBSize)
	}
	return nil
}

// CheckKey returns an ErrMisplaced error unless h belongs under the base
// key of database db, group, txn and epoch, as Keys.ParseBase returns them.
// Callers must check every base header they decode against the key they
// fetched it under.
func (h BaseHeader) CheckKey(db DatabaseID, group uint32, txn, epoch uint64) error {
	if h.DB != db || h.Group != group || h.Txn != txn || h.Epoch != epoch {
		return misplaced("database %x group %d at transaction %d epoch %d under the key of database %x group %d at transaction %d epoch %d",
			h.DB, h.Group, h.Txn, h.Epoch, db, group, txn, epoch)
	}
	return nil
}

func (h BaseHeader) recordCRC(pgno uint64, body []byte) uint32 {
	return recordCRC(baseMagic, h.DB, h.Txn, h.Epoch, pgno, body)
}

// checkSlot checks that the body of the slot for page p is zeros if p is
// the lock page or beyond the database size.
func (h BaseHeader) checkSlot(p uint64, body []byte) error {
	switch {
	case p > uint64(h.DBSize) && !allZero(body):
		return invalid("page %d beyond the database size %d is not zero", p, h.DBSize)
	case p == uint64(LockPage(h.PageSize)) && !allZero(body):
		return invalid("lock page %d is not zero", p)
	}
	return nil
}

// PageRange returns the byte range [off, off+n) of page pgno's record in
// the group object h describes. h may be built from a manifest: DB,
// PageSize, GroupSize, Group, Txn and Epoch must be set, DBSize is not
// used, and the header need not have been read.
func (h BaseHeader) PageRange(pgno uint32) (off, n int64, err error) {
	if err := h.validate(); err != nil {
		return 0, 0, err
	}
	p := uint64(pgno)
	if p < h.first() || p >= h.first()+uint64(h.GroupSize) || p > MaxPages {
		return 0, 0, invalid("page %d is not in group %d", pgno, h.Group)
	}
	return BaseHeaderSize + int64(p-h.first())*h.recordLen(), h.recordLen(), nil
}

func (h BaseHeader) encode() []byte {
	b := make([]byte, BaseHeaderSize)
	copy(b, baseMagic)
	le.PutUint16(b[8:], Version)
	le.PutUint32(b[12:], h.PageSize)
	le.PutUint32(b[16:], h.GroupSize)
	le.PutUint32(b[20:], h.Group)
	le.PutUint64(b[24:], h.Txn)
	le.PutUint64(b[32:], h.Epoch)
	copy(b[40:], h.DB[:])
	le.PutUint32(b[56:], h.DBSize)
	le.PutUint32(b[60:], checksum(b[:60]))
	return b
}

// EncodeBase writes a base group object to w. pages supplies all
// h.GroupSize page bodies of h.PageSize bytes in page order, zeros for pages
// that were never written. The lock page and every page beyond h.DBSize
// must be zeros. The object is exactly h.Size() bytes. EncodeBase reads
// exactly h.GroupSize*h.PageSize bytes from pages and nothing after them.
// Errors from w are returned unchanged; errors reading pages are wrapped
// with the slot number. Neither is a format class.
func EncodeBase(w io.Writer, h BaseHeader, pages io.Reader) error {
	if err := h.validateObject(); err != nil {
		return err
	}
	bw := bufio.NewWriterSize(w, 64<<10)
	if _, err := bw.Write(h.encode()); err != nil {
		return err
	}
	rec := make([]byte, h.recordLen())
	body := rec[:h.PageSize]
	for i := range h.GroupSize {
		if _, err := io.ReadFull(pages, body); err != nil {
			return fmt.Errorf("format: reading slot %d: %w", i, err)
		}
		p := h.first() + uint64(i)
		if err := h.checkSlot(p, body); err != nil {
			return err
		}
		le.PutUint32(rec[h.PageSize:], h.recordCRC(p, body))
		if _, err := bw.Write(rec); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// DecodeBaseHeader decodes the first BaseHeaderSize bytes of a base group
// object of database db.
func DecodeBaseHeader(b []byte, db DatabaseID) (BaseHeader, error) {
	if err := checkHeader(b, baseMagic, BaseHeaderSize); err != nil {
		return BaseHeader{}, err
	}
	if f := le.Uint16(b[10:]); f != 0 {
		return BaseHeader{}, fmt.Errorf("%w: flags %#x", ErrVersion, f)
	}
	h := BaseHeader{
		PageSize:  le.Uint32(b[12:]),
		GroupSize: le.Uint32(b[16:]),
		Group:     le.Uint32(b[20:]),
		Txn:       le.Uint64(b[24:]),
		Epoch:     le.Uint64(b[32:]),
		DBSize:    le.Uint32(b[56:]),
	}
	copy(h.DB[:], b[40:56])
	if err := h.validateObject(); err != nil {
		return BaseHeader{}, err
	}
	if h.DB != db {
		return BaseHeader{}, misplaced("group object of database %x, want %x", h.DB, db)
	}
	return h, nil
}

// DecodeBasePage verifies the record of page pgno, the bytes PageRange
// names, and returns the page body, which aliases b. A record of another
// page, object or database fails its checksum. It checks that the lock
// page is zeros; it cannot check pages beyond the database size, since h
// may not carry it, so only DecodeBase does.
func DecodeBasePage(h BaseHeader, pgno uint32, b []byte) ([]byte, error) {
	_, n, err := h.PageRange(pgno)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != n {
		return nil, invalid("page %d record is %d bytes, want %d", pgno, len(b), n)
	}
	body := b[:h.PageSize]
	if le.Uint32(b[h.PageSize:]) != h.recordCRC(uint64(pgno), body) {
		return nil, fmt.Errorf("%w: page %d", ErrChecksum, pgno)
	}
	if pgno == LockPage(h.PageSize) && !allZero(body) {
		return nil, invalid("lock page %d is not zero", pgno)
	}
	return body, nil
}

// DecodeBase verifies a whole base group object of database db and returns
// its header and all GroupSize page bodies in page order, aliasing b.
func DecodeBase(b []byte, db DatabaseID) (BaseHeader, [][]byte, error) {
	h, err := DecodeBaseHeader(b[:min(len(b), BaseHeaderSize)], db)
	if err != nil {
		return BaseHeader{}, nil, err
	}
	if int64(len(b)) != h.Size() {
		return BaseHeader{}, nil, invalid("group object is %d bytes, want %d", len(b), h.Size())
	}
	pages := make([][]byte, h.GroupSize)
	for i := range h.GroupSize {
		off := BaseHeaderSize + int64(i)*h.recordLen()
		rec := b[off : off+h.recordLen()]
		body := rec[:h.PageSize]
		p := h.first() + uint64(i)
		if le.Uint32(rec[h.PageSize:]) != h.recordCRC(p, body) {
			return BaseHeader{}, nil, fmt.Errorf("%w: slot %d", ErrChecksum, i)
		}
		if err := h.checkSlot(p, body); err != nil {
			return BaseHeader{}, nil, err
		}
		pages[i] = body
	}
	return h, pages, nil
}
