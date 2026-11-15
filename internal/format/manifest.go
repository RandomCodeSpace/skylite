package format

import "fmt"

// MaxManifestSize is the largest encoded manifest. It is far below every
// backend's single-request upload limit; see the package documentation for
// what it allows.
const MaxManifestSize = 16 << 20

const (
	manifestMagic      = "SKYLITEM"
	manifestHeaderSize = 84
	groupRefSize       = 16
)

// Manifest names the base object of every group and the state the base is
// current to.
type Manifest struct {
	DB        DatabaseID
	PageSize  uint32
	GroupSize uint32
	DBSize    uint32 // database size in pages at Txn
	Txn       uint64 // transaction number the base is current to; 0 before the first compaction
	// RetiredThrough is at most Txn: every commit-log object at or below
	// it is retired or is an epoch bump. 0 when the database is created.
	RetiredThrough uint64
	Epoch          uint64 // epoch of the writer
	// Nonce is random, never all zeros, set by the candidate that swapped
	// the manifest to Epoch and kept by that leader's later manifests.
	Nonce [16]byte
	// Groups has exactly GroupCount(DBSize, GroupSize) entries; entry g is group g.
	Groups []GroupRef
}

// GroupRef names a base group object by the transaction number it is
// current to and the tag of the compaction attempt that wrote it. Txn 0
// means the group has no object and all its pages read as zeros; Tag is
// then 0 too, so the zero GroupRef is the only such entry.
type GroupRef struct {
	Txn uint64
	Tag uint64
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

func manifestSize(groups int64) int64 { return manifestHeaderSize + groupRefSize*groups + 4 }

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
	case m.RetiredThrough > m.Txn:
		return invalid("retired through %d after transaction %d", m.RetiredThrough, m.Txn)
	case m.Epoch == 0:
		return invalid("epoch 0")
	case m.Nonce == [16]byte{}:
		return invalid("writer nonce is zero")
	case int64(len(m.Groups)) != int64(GroupCount(m.DBSize, m.GroupSize)):
		return invalid("%d groups for %d pages in groups of %d", len(m.Groups), m.DBSize, m.GroupSize)
	}
	for g, r := range m.Groups {
		switch {
		case r.Txn == 0 && r.Tag != 0:
			return invalid("group %d: no object but tag %016x", g, r.Tag)
		case r.Txn > m.Txn:
			return invalid("group %d: transaction %d after manifest transaction %d", g, r.Txn, m.Txn)
		}
	}
	return nil
}

// EncodeManifest returns the encoding of m. Equal manifests encode to equal
// bytes. A manifest larger than MaxManifestSize fails with
// ErrManifestTooLarge.
func EncodeManifest(m Manifest) ([]byte, error) {
	if n := manifestSize(int64(len(m.Groups))); n > MaxManifestSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrManifestTooLarge, n)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, manifestSize(int64(len(m.Groups))))
	copy(b, manifestMagic)
	le.PutUint16(b[8:], Version)
	le.PutUint32(b[12:], m.PageSize)
	le.PutUint32(b[16:], m.GroupSize)
	le.PutUint32(b[20:], m.DBSize)
	le.PutUint64(b[24:], m.Txn)
	le.PutUint64(b[32:], m.Epoch)
	copy(b[40:], m.DB[:])
	copy(b[56:], m.Nonce[:])
	le.PutUint64(b[72:], m.RetiredThrough)
	le.PutUint32(b[80:], uint32(len(m.Groups))) //nolint:gosec // bounded by MaxManifestSize
	x := b[manifestHeaderSize:]
	for _, r := range m.Groups {
		le.PutUint64(x, r.Txn)
		le.PutUint64(x[8:], r.Tag)
		x = x[groupRefSize:]
	}
	n := len(b) - 4
	le.PutUint32(b[n:], checksum(b[:n]))
	return b, nil
}

// DecodeManifest decodes and validates a whole manifest of database db.
// Passing the zero DatabaseID is discovery: any identifier is accepted, and
// the caller learns it from the result. Trust in a discovered identifier
// rests on the key the manifest was read from; every later read pins it.
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
	groups := le.Uint32(b[80:])
	if want := manifestSize(int64(groups)); int64(len(b)) != want {
		return Manifest{}, invalid("manifest is %d bytes, %d groups need %d", len(b), groups, want)
	}
	n := len(b) - 4
	if le.Uint32(b[n:]) != checksum(b[:n]) {
		return Manifest{}, fmt.Errorf("%w: manifest", ErrChecksum)
	}
	if f := le.Uint16(b[10:]); f != 0 {
		return Manifest{}, fmt.Errorf("%w: flags %#x", ErrVersion, f)
	}
	m := Manifest{
		PageSize:       le.Uint32(b[12:]),
		GroupSize:      le.Uint32(b[16:]),
		DBSize:         le.Uint32(b[20:]),
		Txn:            le.Uint64(b[24:]),
		RetiredThrough: le.Uint64(b[72:]),
		Epoch:          le.Uint64(b[32:]),
	}
	copy(m.DB[:], b[40:56])
	copy(m.Nonce[:], b[56:72])
	// The count is bounded by len(b), checked above.
	x := b[manifestHeaderSize:]
	if groups > 0 {
		m.Groups = make([]GroupRef, groups)
		for g := range m.Groups {
			m.Groups[g] = GroupRef{Txn: le.Uint64(x), Tag: le.Uint64(x[8:])}
			x = x[groupRefSize:]
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
