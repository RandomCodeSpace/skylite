package format

import "fmt"

const (
	manifestMagic      = "SKYLITEM"
	manifestHeaderSize = 44
	groupRefSize       = 16
)

// Manifest names the base object of every group and the state the base is
// current to.
type Manifest struct {
	PageSize  uint32
	GroupSize uint32
	DBSize    uint32 // database size in pages at Txn
	Txn       uint64 // transaction number the base is current to; 0 before the first compaction
	Epoch     uint64 // epoch of the writer
	// Groups has exactly GroupCount(DBSize, GroupSize) entries; entry g is group g.
	Groups []GroupRef
}

// GroupRef names a base group object by the transaction number it is
// current to and its writer's epoch. The zero GroupRef means the group has
// no object and all its pages read as zeros.
type GroupRef struct {
	Txn   uint64
	Epoch uint64
}

// GroupCount is the number of groups that hold a database of dbSize pages.
// groupSize must not be 0.
func GroupCount(dbSize, groupSize uint32) uint32 {
	n := dbSize / groupSize
	if dbSize%groupSize != 0 {
		n++
	}
	return n
}

func (m Manifest) validate() error {
	switch {
	case !validPageSize(m.PageSize):
		return invalid("page size %d", m.PageSize)
	case !validGroupSize(m.PageSize, m.GroupSize):
		return invalid("group size %d at page size %d", m.GroupSize, m.PageSize)
	case m.DBSize > MaxPages:
		return invalid("database size %d pages exceeds %d", m.DBSize, uint32(MaxPages))
	case m.Epoch == 0:
		return invalid("epoch 0")
	case int64(len(m.Groups)) != int64(GroupCount(m.DBSize, m.GroupSize)):
		return invalid("%d groups for %d pages in groups of %d", len(m.Groups), m.DBSize, m.GroupSize)
	}
	for g, r := range m.Groups {
		switch {
		case (r.Txn == 0) != (r.Epoch == 0):
			return invalid("group %d: transaction %d epoch %d", g, r.Txn, r.Epoch)
		case r.Txn > m.Txn:
			return invalid("group %d: transaction %d after manifest transaction %d", g, r.Txn, m.Txn)
		case r.Epoch > m.Epoch:
			return invalid("group %d: epoch %d after manifest epoch %d", g, r.Epoch, m.Epoch)
		}
	}
	return nil
}

// EncodeManifest returns the encoding of m. Equal manifests encode to equal bytes.
func EncodeManifest(m Manifest) ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, manifestHeaderSize+groupRefSize*len(m.Groups)+4)
	copy(b, manifestMagic)
	le.PutUint16(b[8:], Version)
	le.PutUint32(b[12:], m.PageSize)
	le.PutUint32(b[16:], m.GroupSize)
	le.PutUint32(b[20:], m.DBSize)
	le.PutUint64(b[24:], m.Txn)
	le.PutUint64(b[32:], m.Epoch)
	le.PutUint32(b[40:], GroupCount(m.DBSize, m.GroupSize))
	for g, r := range m.Groups {
		x := b[manifestHeaderSize+g*groupRefSize:]
		le.PutUint64(x, r.Txn)
		le.PutUint64(x[8:], r.Epoch)
	}
	n := len(b) - 4
	le.PutUint32(b[n:], checksum(b[:n]))
	return b, nil
}

// DecodeManifest decodes and validates a whole manifest.
func DecodeManifest(b []byte) (Manifest, error) {
	if len(b) < len(manifestMagic) || string(b[:len(manifestMagic)]) != manifestMagic {
		return Manifest{}, ErrNotFormat
	}
	if len(b) < manifestHeaderSize+4 {
		return Manifest{}, invalid("manifest truncated at %d bytes", len(b))
	}
	if v := le.Uint16(b[8:]); v != Version {
		return Manifest{}, fmt.Errorf("%w: version %d", ErrVersion, v)
	}
	count := le.Uint32(b[40:])
	if want := int64(manifestHeaderSize) + groupRefSize*int64(count) + 4; int64(len(b)) != want {
		return Manifest{}, invalid("manifest is %d bytes, %d groups need %d", len(b), count, want)
	}
	n := len(b) - 4
	if le.Uint32(b[n:]) != checksum(b[:n]) {
		return Manifest{}, fmt.Errorf("%w: manifest", ErrChecksum)
	}
	if f := le.Uint16(b[10:]); f != 0 {
		return Manifest{}, fmt.Errorf("%w: flags %#x", ErrVersion, f)
	}
	m := Manifest{
		PageSize:  le.Uint32(b[12:]),
		GroupSize: le.Uint32(b[16:]),
		DBSize:    le.Uint32(b[20:]),
		Txn:       le.Uint64(b[24:]),
		Epoch:     le.Uint64(b[32:]),
	}
	if count > 0 {
		m.Groups = make([]GroupRef, count) // bounded by len(b), checked above
		for g := range m.Groups {
			x := b[manifestHeaderSize+g*groupRefSize:]
			m.Groups[g] = GroupRef{Txn: le.Uint64(x), Epoch: le.Uint64(x[8:])}
		}
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
