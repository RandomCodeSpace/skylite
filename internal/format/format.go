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
// tell the classes apart with errors.Is.
var (
	ErrNotFormat = errors.New("format: not a skylite object")
	ErrVersion   = errors.New("format: unsupported version or feature")
	ErrChecksum  = errors.New("format: checksum mismatch")
	ErrInvalid   = errors.New("format: structurally invalid")
)

// recordTrailer is the CRC-32C that follows every stored page body.
const recordTrailer = 4

var (
	le         = binary.LittleEndian
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

func checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}

func validPageSize(ps uint32) bool {
	return ps >= MinPageSize && ps <= MaxPageSize && ps&(ps-1) == 0
}

func validGroupSize(ps, gs uint32) bool {
	return gs != 0 && gs&(gs-1) == 0 && uint64(gs)*uint64(ps) <= MaxGroupBytes
}

// LockPage returns the page number of SQLite's lock page for the given page
// size: the page holding byte offset 2^30, which SQLite never writes.
// pageSize must not be 0.
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
	if len(b) < len(magic) || string(b[:len(magic)]) != magic {
		return ErrNotFormat
	}
	if len(b) < len(magic)+2 {
		return invalid("header truncated at %d bytes", len(b))
	}
	if v := le.Uint16(b[len(magic):]); v != Version {
		return fmt.Errorf("%w: version %d", ErrVersion, v)
	}
	if len(b) != size {
		return invalid("header is %d bytes, want %d", len(b), size)
	}
	if got, want := le.Uint32(b[size-4:]), checksum(b[:size-4]); got != want {
		return fmt.Errorf("%w: header", ErrChecksum)
	}
	return nil
}
