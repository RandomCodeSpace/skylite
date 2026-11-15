package format

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Storage key constraints.
const (
	MaxKeyLen      = 1024
	MaxKeySegments = 32
)

const (
	txnDigits   = 20 // math.MaxUint64 has 20 decimal digits
	groupDigits = 10 // math.MaxUint32 has 10
	baseNameLen = groupDigits + 1 + txnDigits + 1 + txnDigits
	// longestSuffix is the longest part a Keys method appends to the prefix.
	longestSuffix = len("/base/") + baseNameLen
)

// Keys builds and parses the object keys of one database, all under one
// prefix. Use NewKeys; the zero Keys is not usable.
type Keys struct{ prefix string }

// NewKeys returns the keys for the database stored under prefix, which is
// one or more key segments separated by "/". The prefix must leave room for
// every key built from it: at most MaxKeySegments-2 segments and
// MaxKeyLen-58 bytes.
func NewKeys(prefix string) (Keys, error) {
	if len(prefix) > MaxKeyLen-longestSuffix {
		return Keys{}, invalid("key prefix of %d bytes", len(prefix))
	}
	segs := strings.Split(prefix, "/")
	if len(segs) > MaxKeySegments-2 {
		return Keys{}, invalid("key prefix of %d segments", len(segs))
	}
	for _, s := range segs {
		if !validSegment(s) {
			return Keys{}, invalid("key prefix segment %q", s)
		}
	}
	return Keys{prefix}, nil
}

func validSegment(s string) bool {
	if s == "" || s == "." || s == ".." || s[len(s)-1] == '.' {
		return false
	}
	for i := range len(s) {
		switch c := s[i]; {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// Manifest is the key of the manifest.
func (k Keys) Manifest() string { return k.prefix + "/manifest" }

// Lease is the key of the lease.
func (k Keys) Lease() string { return k.prefix + "/lease" }

// CommitPrefix is the prefix every commit log key starts with. Listing it
// with start-after Commit(n) yields the commits after n in order.
func (k Keys) CommitPrefix() string { return k.prefix + "/log/" }

// Commit is the key of the commit log entry for transaction txn.
func (k Keys) Commit(txn uint64) string { return fmt.Sprintf("%s%020d", k.CommitPrefix(), txn) }

// BasePrefix is the prefix every base group key starts with.
func (k Keys) BasePrefix() string { return k.prefix + "/base/" }

// Base is the key of the base object for group, current to txn and written at epoch.
func (k Keys) Base(group uint32, txn, epoch uint64) string {
	return fmt.Sprintf("%s%010d-%020d-%020d", k.BasePrefix(), group, txn, epoch)
}

// ParseCommit returns the transaction number in a key Commit built.
func (k Keys) ParseCommit(key string) (uint64, error) {
	s, ok := strings.CutPrefix(key, k.CommitPrefix())
	if !ok {
		return 0, invalid("key %q is not a commit key", key)
	}
	txn, ok := parseDigits(s, txnDigits)
	if !ok {
		return 0, invalid("key %q is not a commit key", key)
	}
	return txn, nil
}

// ParseBase returns the group, transaction number and epoch in a key Base built.
func (k Keys) ParseBase(key string) (group uint32, txn, epoch uint64, err error) {
	s, ok := strings.CutPrefix(key, k.BasePrefix())
	if !ok || len(s) != baseNameLen || s[groupDigits] != '-' || s[groupDigits+1+txnDigits] != '-' {
		return 0, 0, 0, invalid("key %q is not a base key", key)
	}
	g, ok1 := parseDigits(s[:groupDigits], groupDigits)
	t, ok2 := parseDigits(s[groupDigits+1:groupDigits+1+txnDigits], txnDigits)
	e, ok3 := parseDigits(s[groupDigits+2+txnDigits:], txnDigits)
	if !ok1 || !ok2 || !ok3 || g > math.MaxUint32 {
		return 0, 0, 0, invalid("key %q is not a base key", key)
	}
	return uint32(g), t, e, nil
}

// parseDigits parses exactly width decimal digits that fit in a uint64.
func parseDigits(s string, width int) (uint64, bool) {
	if len(s) != width {
		return 0, false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}
