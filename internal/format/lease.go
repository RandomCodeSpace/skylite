package format

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
)

// MaxHolderLen is the longest lease holder identity.
const MaxHolderLen = 128

// The canonical lease is these literals with the holder and two integers
// between them.
const (
	leaseHead    = `{"format":"skylite-lease","version":1,"holder":"`
	leaseEpoch   = `","epoch":`
	leaseRenewed = `,"renewed":`
	leaseEnd     = `}`

	// leaseFormat is how every lease starts; leaseVersion follows it.
	leaseFormat  = `{"format":"skylite-lease",`
	leaseVersion = `"version":`
)

// MaxLeaseSize is the largest encoded lease.
const MaxLeaseSize = len(leaseHead) + MaxHolderLen + len(leaseEpoch) + 20 + len(leaseRenewed) + 19 + len(leaseEnd)

// Lease says who intends to lead and at which epoch. Nothing
// safety-critical reads it.
type Lease struct {
	Holder  string // 1 to MaxHolderLen characters of A-Z a-z 0-9 . _ -
	Epoch   uint64 // epoch the holder leads or intends to lead at, at least 1
	Renewed int64  // milliseconds since the Unix epoch, at least 0; refreshed by the holder
}

func (l Lease) validate() error {
	if l.Holder == "" || len(l.Holder) > MaxHolderLen {
		return invalid("holder of %d bytes", len(l.Holder))
	}
	for i := range len(l.Holder) {
		switch c := l.Holder[i]; {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '.', c == '_', c == '-':
		default:
			return invalid("holder byte %#x", c)
		}
	}
	switch {
	case l.Epoch == 0:
		return invalid("epoch 0")
	case l.Renewed < 0:
		return invalid("renewal time %d", l.Renewed)
	}
	return nil
}

// EncodeLease returns the canonical encoding of l.
func EncodeLease(l Lease) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, 0, MaxLeaseSize)
	b = append(b, leaseHead...)
	b = append(b, l.Holder...)
	b = append(b, leaseEpoch...)
	b = strconv.AppendUint(b, l.Epoch, 10)
	b = append(b, leaseRenewed...)
	b = strconv.AppendInt(b, l.Renewed, 10)
	return append(b, leaseEnd...), nil
}

// DecodeLease decodes a lease. Only the exact bytes EncodeLease produces are
// accepted.
func DecodeLease(b []byte) (Lease, error) {
	rest, ok := bytes.CutPrefix(b, []byte(leaseFormat))
	if !ok {
		return Lease{}, ErrNotFormat
	}
	if rest, ok = bytes.CutPrefix(rest, []byte(leaseVersion)); !ok {
		return Lease{}, invalid("lease without a version")
	}
	n := digits(rest)
	if n == 0 {
		return Lease{}, invalid("lease version is not a number")
	}
	if string(rest[:n]) != "1" {
		return Lease{}, fmt.Errorf("%w: lease version %.20s", ErrVersion, rest[:n])
	}
	// Checked before any field is copied or parsed, so an oversized object
	// costs no allocation in proportion to its size.
	if len(b) > MaxLeaseSize {
		return Lease{}, invalid("lease is %d bytes; the longest is %d", len(b), MaxLeaseSize)
	}
	rest, ok = bytes.CutPrefix(b, []byte(leaseHead))
	if !ok {
		return Lease{}, invalid("lease is not in canonical form")
	}
	var l Lease
	i := bytes.IndexByte(rest, '"')
	if i < 0 {
		return Lease{}, invalid("lease holder is not terminated")
	}
	l.Holder, rest = string(rest[:i]), rest[i:]
	var renewed uint64
	if rest, ok = bytes.CutPrefix(rest, []byte(leaseEpoch)); ok {
		l.Epoch, rest, ok = cutUint(rest)
	}
	if ok {
		rest, ok = bytes.CutPrefix(rest, []byte(leaseRenewed))
	}
	if ok {
		renewed, rest, ok = cutUint(rest)
	}
	if !ok || renewed > math.MaxInt64 || string(rest) != leaseEnd {
		return Lease{}, invalid("lease is not in canonical form")
	}
	l.Renewed = int64(renewed)
	if err := l.validate(); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// digits returns the number of leading ASCII digits in b.
func digits(b []byte) int {
	n := 0
	for n < len(b) && '0' <= b[n] && b[n] <= '9' {
		n++
	}
	return n
}

// cutUint parses the canonical decimal integer at the front of b: no sign,
// no leading zero unless it is 0, at most math.MaxUint64.
func cutUint(b []byte) (uint64, []byte, bool) {
	n := digits(b)
	if n == 0 || (n > 1 && b[0] == '0') {
		return 0, b, false
	}
	v, err := strconv.ParseUint(string(b[:n]), 10, 64)
	return v, b[n:], err == nil
}
