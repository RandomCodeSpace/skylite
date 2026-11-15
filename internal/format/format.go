package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Version is the only format version this package reads and writes.
const Version = 1

// Limits shared by every object type.
const (
	MinPageSize   = 512
	MaxPageSize   = 65536
	MaxPages      = 4294967294 // SQLite's largest page count, and so the largest page number
	MaxGroupBytes = 64 << 20   // largest group size times page size
)

// Decoders return errors that wrap exactly one of these, so callers can
// tell the classes apart with errors.Is. Errors from a caller's reader,
// writer or callback are not format errors; see the package documentation.
var (
	ErrNotFormat = errors.New("format: not a skylite object")
	ErrVersion   = errors.New("format: unsupported version or feature")
	ErrChecksum  = errors.New("format: checksum mismatch")
	ErrInvalid   = errors.New("format: structurally invalid")
	// ErrMisplaced means a well-formed object that belongs to another
	// database, or whose header disagrees with the key it was fetched under.
	ErrMisplaced = errors.New("format: object belongs elsewhere")
)

// ErrManifestTooLarge is the ErrInvalid error for a manifest larger than
// MaxManifestSize. It wraps ErrInvalid, so it is in that class, and
// errors.Is tells it apart from other invalid manifests.
var ErrManifestTooLarge = fmt.Errorf("%w: manifest larger than %d bytes", ErrInvalid, MaxManifestSize)

// DatabaseID identifies one database. It is generated once, when the
// database is created, and is never all zeros.
type DatabaseID [16]byte

// recordTrailer is the CRC-32C that follows every stored page body.
const recordTrailer = 4

var (
	le         = binary.LittleEndian
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

func checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// recordIdentitySize is the size of the identity a page record's checksum
// covers before the body: magic, database, transaction, epoch, page number.
const recordIdentitySize = 8 + 16 + 8 + 8 + 8

// recordCRC is the checksum stored after a page body: the CRC-32C of the
// record identity followed by the body.
func recordCRC(magic string, db DatabaseID, txn, epoch, pgno uint64, body []byte) uint32 {
	var id [recordIdentitySize]byte
	copy(id[:], magic)
	copy(id[8:], db[:])
	le.PutUint64(id[24:], txn)
	le.PutUint64(id[32:], epoch)
	le.PutUint64(id[40:], pgno)
	return crc32.Update(checksum(id[:]), castagnoli, body)
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}

func misplaced(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrMisplaced}, args...)...)
}

func validPageSize(ps uint32) bool {
	return ps >= MinPageSize && ps <= MaxPageSize && ps&(ps-1) == 0
}

func validGroupSize(ps, gs uint32) bool {
	return gs != 0 && gs&(gs-1) == 0 && uint64(gs)*uint64(ps) <= MaxGroupBytes
}

// LockPage returns the page number of SQLite's lock page for the given page
// size: the page holding byte offset 2^30, which SQLite never writes. It
// panics if pageSize is zero.
func LockPage(pageSize uint32) uint32 { return 1<<30/pageSize + 1 }

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// checkHeader checks a fixed-size header: magic, version, size and the
// trailing CRC-32C over everything before it, in that order.
func checkHeader(b []byte, magic string, size int) error {
	if err := checkMagicVersion(b, magic); err != nil {
		return err
	}
	if len(b) != size {
		return invalid("header is %d bytes, want %d", len(b), size)
	}
	if got, want := le.Uint32(b[size-4:]), checksum(b[:size-4]); got != want {
		return fmt.Errorf("%w: header", ErrChecksum)
	}
	return nil
}

// checkMagicVersion checks the 8-byte magic and, as soon as its two bytes
// are present, the version that follows it.
func checkMagicVersion(b []byte, magic string) error {
	if len(b) < len(magic) || string(b[:len(magic)]) != magic {
		return ErrNotFormat
	}
	if len(b) < len(magic)+2 {
		return invalid("truncated at %d bytes", len(b))
	}
	if v := le.Uint16(b[len(magic):]); v != Version {
		return fmt.Errorf("%w: version %d", ErrVersion, v)
	}
	return nil
}

func checkDB(db DatabaseID) error {
	if db == (DatabaseID{}) {
		return invalid("database identifier is zero")
	}
	return nil
}
