package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	// MaxLeaseSize is the largest encoded lease.
	MaxLeaseSize = 4096
	// MaxHolderLen is the longest lease holder identity, in bytes.
	MaxHolderLen = 256

	leaseFormat = "skylite-lease"
)

// leasePrefix is how every encoded lease starts.
var leasePrefix = []byte(`{"format":"` + leaseFormat + `",`)

// Lease says who intends to lead and at which epoch. Nothing
// safety-critical reads it.
type Lease struct {
	Holder  string    // opaque, 1 to MaxHolderLen bytes of UTF-8
	Epoch   uint64    // epoch the holder leads or intends to lead at
	Renewed time.Time // refreshed by the holder; encoded in UTC
}

type leaseJSON struct {
	Format  string    `json:"format"`
	Version int       `json:"version"`
	Holder  string    `json:"holder"`
	Epoch   uint64    `json:"epoch"`
	Renewed time.Time `json:"renewed"`
}

func (l Lease) validate() error {
	switch {
	case l.Holder == "" || len(l.Holder) > MaxHolderLen:
		return invalid("holder of %d bytes", len(l.Holder))
	case !utf8.ValidString(l.Holder):
		return invalid("holder is not UTF-8")
	case l.Epoch == 0:
		return invalid("epoch 0")
	}
	return nil
}

// EncodeLease returns the JSON encoding of l.
func EncodeLease(l Lease) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(leaseJSON{leaseFormat, Version, l.Holder, l.Epoch, l.Renewed.UTC()})
	if err != nil {
		return nil, invalid("%v", err)
	}
	return b, nil
}

// DecodeLease decodes a lease. Only the exact bytes EncodeLease produces are
// accepted.
func DecodeLease(b []byte) (Lease, error) {
	if !bytes.HasPrefix(b, leasePrefix) {
		return Lease{}, ErrNotFormat
	}
	if len(b) > MaxLeaseSize {
		return Lease{}, invalid("lease of %d bytes", len(b))
	}
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		return Lease{}, invalid("%v", err)
	}
	if probe.Version != Version {
		return Lease{}, fmt.Errorf("%w: version %d", ErrVersion, probe.Version)
	}
	var j leaseJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return Lease{}, invalid("%v", err)
	}
	l := Lease{Holder: j.Holder, Epoch: j.Epoch, Renewed: j.Renewed}
	// Re-encoding rejects every non-canonical form at once: unknown or
	// duplicate fields, whitespace, other time zones, escapes.
	if c, err := EncodeLease(l); err != nil {
		return Lease{}, err
	} else if !bytes.Equal(c, b) {
		return Lease{}, invalid("lease is not in canonical form")
	}
	return l, nil
}
