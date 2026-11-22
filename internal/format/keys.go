package format

import (
	"encoding/hex"
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
	tagHexLen   = 16 // a uint64 in hexadecimal
	idHexLen    = 2 * len(DatabaseID{})
	baseNameLen = groupDigits + 1 + txnDigits + 1 + tagHexLen
	// longestSuffix is the longest part a Keys method appends to the prefix:
	// "/<identifier>/base/<group>-<txn>-<tag>".
	longestSuffix = 1 + idHexLen + len("/base/") + baseNameLen
	// suffixSegments is the most segments a Keys method appends.
	suffixSegments = 3
)

// Keys builds and parses the object keys of the databases under one prefix.
// Use NewKeys; the zero Keys is not usable.
type Keys struct{ prefix string }

// NewKeys returns the keys for the database stored under prefix, which is
// one or more key segments separated by "/". The prefix must leave room for
// every key built from it: at most MaxKeySegments-3 segments and
// MaxKeyLen-87 bytes.
func NewKeys(prefix string) (Keys, error) {
	if len(prefix) > MaxKeyLen-longestSuffix {
		return Keys{}, invalid("key prefix of %d bytes", len(prefix))
	}
	segs := strings.Split(prefix, "/")
	if len(segs) > MaxKeySegments-suffixSegments {
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

// Leader is the key of the server's leader record, which routes requests
// to the pod in office (ADR 0005 decision 7). The engine never reads it.
func (k Keys) Leader() string { return k.prefix + "/leader" }

// db is the start of every log and base key of database db. A key built
// for the zero DatabaseID is invalid, and the parsers reject it.
func (k Keys) db(db DatabaseID) string { return k.prefix + "/" + hex.EncodeToString(db[:]) }

// CommitPrefix is the prefix every commit log key of database db starts with.
func (k Keys) CommitPrefix(db DatabaseID) string { return k.db(db) + "/log/" }

// Commit is the key of the commit log entry of database db for transaction txn.
func (k Keys) Commit(db DatabaseID, txn uint64) string {
	return fmt.Sprintf("%s%020d", k.CommitPrefix(db), txn)
}

// BasePrefix is the prefix every base group key of database db starts with.
func (k Keys) BasePrefix(db DatabaseID) string { return k.db(db) + "/base/" }

// Base is the key of the base object of database db for group, current to
// txn and written by the compaction attempt with the given tag.
func (k Keys) Base(db DatabaseID, group uint32, txn, tag uint64) string {
	return fmt.Sprintf("%s%010d-%020d-%016x", k.BasePrefix(db), group, txn, tag)
}

// cutDB parses "<prefix>/<identifier><kind>" from the front of key and
// returns the identifier and the rest.
func (k Keys) cutDB(key, kind string) (DatabaseID, string, bool) {
	var db DatabaseID
	s, ok := strings.CutPrefix(key, k.prefix+"/")
	if !ok || len(s) < idHexLen {
		return db, "", false
	}
	id := s[:idHexLen]
	// The round trip rejects uppercase digits, which hex.Decode accepts.
	if _, err := hex.Decode(db[:], []byte(id)); err != nil || hex.EncodeToString(db[:]) != id || db == (DatabaseID{}) {
		return db, "", false
	}
	rest, ok := strings.CutPrefix(s[idHexLen:], kind)
	return db, rest, ok
}

// ParseCommit returns the database identifier and transaction number in a
// key Commit built.
func (k Keys) ParseCommit(key string) (DatabaseID, uint64, error) {
	db, s, ok := k.cutDB(key, "/log/")
	if !ok {
		return DatabaseID{}, 0, invalid("key %.80q is not a commit key", key)
	}
	txn, ok := parseDigits(s, txnDigits)
	if !ok {
		return DatabaseID{}, 0, invalid("key %.80q is not a commit key", key)
	}
	return db, txn, nil
}

// ParseBase returns the database identifier, group, transaction number and
// tag in a key Base built.
func (k Keys) ParseBase(key string) (db DatabaseID, group uint32, txn, tag uint64, err error) {
	db, s, ok := k.cutDB(key, "/base/")
	if !ok || len(s) != baseNameLen || s[groupDigits] != '-' || s[groupDigits+1+txnDigits] != '-' {
		return DatabaseID{}, 0, 0, 0, invalid("key %.80q is not a base key", key)
	}
	g, ok1 := parseDigits(s[:groupDigits], groupDigits)
	t, ok2 := parseDigits(s[groupDigits+1:groupDigits+1+txnDigits], txnDigits)
	e, ok3 := parseHex(s[groupDigits+2+txnDigits:])
	if !ok1 || !ok2 || !ok3 || g > math.MaxUint32 {
		return DatabaseID{}, 0, 0, 0, invalid("key %.80q is not a base key", key)
	}
	return db, uint32(g), t, e, nil
}

// parseHex parses exactly tagHexLen lowercase hexadecimal digits.
func parseHex(s string) (uint64, bool) {
	if len(s) != tagHexLen {
		return 0, false
	}
	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(s, 16, 64)
	return v, err == nil
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
