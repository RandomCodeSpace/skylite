package format

import "fmt"

// MaxManifestSize is the largest encoded manifest. It is far below every
// backend's single-request upload limit; see the package documentation for
// what it allows.
const MaxManifestSize = 16 << 20

const (
	manifestMagic      = "SKYLITEM"
	manifestHeaderSize = 64
	groupRefSize       = 16
	bumpSize           = 8
)

// Manifest names the base object of every group and the state the base is
// current to.
type Manifest struct {
	DB        DatabaseID
	PageSize  uint32
	GroupSize uint32
	DBSize    uint32 // database size in pages at Txn
	Txn       uint64 // transaction number the base is current to; 0 before the first compaction
	Epoch     uint64 // epoch of the writer
	// Groups has exactly GroupCount(DBSize, GroupSize) entries; entry g is group g.
	Groups []GroupRef
	// Bumps lists the transaction numbers of every epoch-bump commit at or
	// below Txn, strictly ascending.
	Bumps []uint64
}

// GroupRef names a base group object by the transaction number it is
// current to and its writer's epoch. The zero GroupRef means the group has
// no object and all its pages read as zeros.
type GroupRef struct {
	Txn   uint64
	Epoch uint64
}

// GroupCount is the number of groups that hold a database of dbSize pages.
// It panics if groupSize is zero.
func GroupCount(dbSize, groupSize uint32) uint32 {
	n := dbSize / groupSize
	if dbSize%groupSize != 0 {
		n++
	}
	return n
}

func manifestSize(groups, bumps int64) int64 {
	return manifestHeaderSize + groupRefSize*groups + bumpSize*bumps + 4
}

func (m Manifest) validate() error {
	if err := checkDB(m.DB); err != nil {
		return err
	}
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
	var prev uint64
	for _, t := range m.Bumps {
		if t <= prev || t > m.Txn {
			return invalid("epoch bump %d after %d in a manifest at transaction %d", t, prev, m.Txn)
		}
		prev = t
	}
	return nil
}

// EncodeManifest returns the encoding of m. Equal manifests encode to equal
// bytes. A manifest larger than MaxManifestSize fails with
// ErrManifestTooLarge.
func EncodeManifest(m Manifest) ([]byte, error) {
	if n := manifestSize(int64(len(m.Groups)), int64(len(m.Bumps))); n > MaxManifestSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrManifestTooLarge, n)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, manifestSize(int64(len(m.Groups)), int64(len(m.Bumps))))
	copy(b, manifestMagic)
	le.PutUint16(b[8:], Version)
	le.PutUint32(b[12:], m.PageSize)
	le.PutUint32(b[16:], m.GroupSize)
	le.PutUint32(b[20:], m.DBSize)
	le.PutUint64(b[24:], m.Txn)
	le.PutUint64(b[32:], m.Epoch)
	copy(b[40:], m.DB[:])
	le.PutUint32(b[56:], uint32(len(m.Groups))) //nolint:gosec // bounded by MaxManifestSize
	le.PutUint32(b[60:], uint32(len(m.Bumps)))  //nolint:gosec // bounded by MaxManifestSize
	x := b[manifestHeaderSize:]
	for _, r := range m.Groups {
		le.PutUint64(x, r.Txn)
		le.PutUint64(x[8:], r.Epoch)
		x = x[groupRefSize:]
	}
	for _, t := range m.Bumps {
		le.PutUint64(x, t)
		x = x[bumpSize:]
	}
	n := len(b) - 4
	le.PutUint32(b[n:], checksum(b[:n]))
	return b, nil
}

// DecodeManifest decodes and validates a whole manifest of database db. A
// reader that does not know the database identifier yet, because the
// manifest is where it learns it, passes the zero DatabaseID and any
// identifier is accepted.
func DecodeManifest(b []byte, db DatabaseID) (Manifest, error) {
	if err := checkMagicVersion(b, manifestMagic); err != nil {
		return Manifest{}, err
	}
	if len(b) > MaxManifestSize {
		return Manifest{}, fmt.Errorf("%w: %d bytes", ErrManifestTooLarge, len(b))
	}
	if len(b) < manifestHeaderSize+4 {
		return Manifest{}, invalid("manifest truncated at %d bytes", len(b))
	}
	groups, bumps := le.Uint32(b[56:]), le.Uint32(b[60:])
	if want := manifestSize(int64(groups), int64(bumps)); int64(len(b)) != want {
		return Manifest{}, invalid("manifest is %d bytes, %d groups and %d bumps need %d", len(b), groups, bumps, want)
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
	copy(m.DB[:], b[40:56])
	// Both counts are bounded by len(b), checked above.
	x := b[manifestHeaderSize:]
	if groups > 0 {
		m.Groups = make([]GroupRef, groups)
		for g := range m.Groups {
			m.Groups[g] = GroupRef{Txn: le.Uint64(x), Epoch: le.Uint64(x[8:])}
			x = x[groupRefSize:]
		}
	}
	if bumps > 0 {
		m.Bumps = make([]uint64, bumps)
		for i := range m.Bumps {
			m.Bumps[i] = le.Uint64(x)
			x = x[bumpSize:]
		}
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	if db != (DatabaseID{}) && m.DB != db {
		return Manifest{}, misplaced("manifest of database %x, want %x", m.DB, db)
	}
	return m, nil
}
