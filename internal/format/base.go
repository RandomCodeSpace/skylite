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
	PageSize  uint32
	GroupSize uint32 // pages per group
	Group     uint32
	Txn       uint64 // transaction number the group is current to
	Epoch     uint64 // epoch of the writer
}

// Size is the size of the whole group object.
func (h BaseHeader) Size() int64 {
	return BaseHeaderSize + int64(h.GroupSize)*h.recordLen()
}

func (h BaseHeader) recordLen() int64 { return int64(h.PageSize) + recordTrailer }

func (h BaseHeader) first() uint64 { return uint64(h.Group)*uint64(h.GroupSize) + 1 }

func (h BaseHeader) validate() error {
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

// lockSlot reports the slot of the lock page, if the group holds it.
func (h BaseHeader) lockSlot() (uint32, bool) {
	lp := uint64(LockPage(h.PageSize))
	if lp < h.first() || lp >= h.first()+uint64(h.GroupSize) {
		return 0, false
	}
	return uint32(lp - h.first()), true //nolint:gosec // less than GroupSize
}

// PageRange returns the byte range [off, off+n) of page pgno's record in
// the group object h describes. h may be built from a manifest entry; the
// header need not have been read.
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
	le.PutUint32(b[60:], checksum(b[:60]))
	return b
}

// EncodeBase writes a base group object to w. pages supplies all
// h.GroupSize page bodies of h.PageSize bytes in page order, zeros for pages
// that are beyond the database size or were never written. The lock page,
// if the group holds it, must be zeros. The object is exactly h.Size() bytes.
func EncodeBase(w io.Writer, h BaseHeader, pages io.Reader) error {
	if err := h.validate(); err != nil {
		return err
	}
	lock, hasLock := h.lockSlot()
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
		if hasLock && i == lock && !allZero(body) {
			return invalid("lock page %d is not zero", LockPage(h.PageSize))
		}
		le.PutUint32(rec[h.PageSize:], checksum(body))
		if _, err := bw.Write(rec); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// DecodeBaseHeader decodes the first BaseHeaderSize bytes of a base group object.
func DecodeBaseHeader(b []byte) (BaseHeader, error) {
	if err := checkHeader(b, baseMagic, BaseHeaderSize); err != nil {
		return BaseHeader{}, err
	}
	if f := le.Uint16(b[10:]); f != 0 {
		return BaseHeader{}, fmt.Errorf("%w: flags %#x", ErrVersion, f)
	}
	if !allZero(b[40:60]) {
		return BaseHeader{}, invalid("nonzero reserved bytes")
	}
	h := BaseHeader{
		PageSize:  le.Uint32(b[12:]),
		GroupSize: le.Uint32(b[16:]),
		Group:     le.Uint32(b[20:]),
		Txn:       le.Uint64(b[24:]),
		Epoch:     le.Uint64(b[32:]),
	}
	if err := h.validate(); err != nil {
		return BaseHeader{}, err
	}
	return h, nil
}

// DecodeBasePage verifies the record of page pgno, the bytes PageRange
// names, and returns the page body, which aliases b.
func DecodeBasePage(h BaseHeader, pgno uint32, b []byte) ([]byte, error) {
	_, n, err := h.PageRange(pgno)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != n {
		return nil, invalid("page %d record is %d bytes, want %d", pgno, len(b), n)
	}
	body := b[:h.PageSize]
	if le.Uint32(b[h.PageSize:]) != checksum(body) {
		return nil, fmt.Errorf("%w: page %d", ErrChecksum, pgno)
	}
	if pgno == LockPage(h.PageSize) && !allZero(body) {
		return nil, invalid("lock page %d is not zero", pgno)
	}
	return body, nil
}

// DecodeBase verifies a whole base group object and returns its header and
// all GroupSize page bodies in page order, aliasing b.
func DecodeBase(b []byte) (BaseHeader, [][]byte, error) {
	h, err := DecodeBaseHeader(b[:min(len(b), BaseHeaderSize)])
	if err != nil {
		return BaseHeader{}, nil, err
	}
	if int64(len(b)) != h.Size() {
		return BaseHeader{}, nil, invalid("group object is %d bytes, want %d", len(b), h.Size())
	}
	lock, hasLock := h.lockSlot()
	pages := make([][]byte, h.GroupSize)
	for i := range h.GroupSize {
		off := BaseHeaderSize + int64(i)*h.recordLen()
		rec := b[off : off+h.recordLen()]
		body := rec[:h.PageSize]
		if le.Uint32(rec[h.PageSize:]) != checksum(body) {
			return BaseHeader{}, nil, fmt.Errorf("%w: slot %d", ErrChecksum, i)
		}
		if hasLock && i == lock && !allZero(body) {
			return BaseHeader{}, nil, invalid("lock page %d is not zero", LockPage(h.PageSize))
		}
		pages[i] = body
	}
	return h, pages, nil
}
