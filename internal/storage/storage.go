// Package storage defines the object storage interface that every skylite
// storage backend implements, the errors it returns and the key rules it
// enforces.
//
// Object storage is skylite's only durable state. A stale leader is stopped by
// conditional writes: a commit is a create-if-absent of a new object, and a
// lease or manifest is updated by compare-and-swap on a version token. The
// contract below exists so that those two operations mean the same thing on
// every backend. A backend is accepted only if it passes the conformance suite
// in package storagetest; this documentation is the specification that suite
// checks.
//
// # Keys
//
// A key is a flat, UTF-8 string. "/" separates segments by convention only:
// there are no directories, and "a/b" and "a/b/c" can both exist. Every
// backend calls [ValidateKey] on every key it receives, before any other work,
// and returns its error unchanged inside an [*Error]. A key is rejected with
// [ErrInvalidKey] if it:
//
//   - is empty or longer than [MaxKeyLen] bytes;
//   - is not valid UTF-8;
//   - starts or ends with "/", or contains an empty segment ("a//b");
//   - contains a "." or ".." segment;
//   - contains a backslash;
//   - contains a control character (unicode.IsControl: C0 including NUL, DEL,
//     and C1), or U+FFFE or U+FFFF; XML 1.0, which the Azure and S3 listing
//     APIs use, cannot carry these.
//
// Keys are compared and sorted as byte strings. Keys that differ only in case
// or in Unicode normalisation are different keys, and a backend must keep them
// apart even if the underlying system would not.
//
// # Writes
//
// There is no unconditional put. The only writes are:
//
//   - [Store.Create]: write the object only if no object exists at the key.
//   - [Store.Replace]: write the object only if an object exists at the key and
//     its current version token equals the one given (compare-and-swap).
//
// A failed condition returns [ErrPreconditionFailed]. Replace on a missing key
// is a precondition failure, not [ErrNotFound]. Replace with an empty version
// always fails its precondition.
//
// Writes are atomic. A concurrent or later reader sees the whole old object,
// the whole new object, or not-found; never a partial object. A write that
// fails before it commits (the reader returns an error, the context is
// cancelled, the process is killed, the upload fails) leaves no visible
// object and no visible change, and does not stop a later Create of the same key from
// succeeding. Backends whose service keeps partial upload state against a key
// (for example uncommitted blocks that make a later "If-None-Match: *" fail)
// must not leave such state behind, or must make it irrelevant to later
// writes. The one exception is an error from the commit step itself, such as
// a response lost after the service committed the write, or a failed fsync
// of the directory after the file was linked into place: then the write may
// have taken effect, and a caller that must know reads the object back.
//
// Among any number of concurrent Create calls for one absent key, exactly one
// succeeds; among concurrent Replace calls with the same current version,
// exactly one succeeds. This holds across goroutines, processes and hosts that
// share the store, to the extent stated by each backend's documentation.
//
// A write returns only after the object is durable in the backend: for a
// cloud service, after the service has acknowledged the upload; for a local
// filesystem, after the data and the directory entry have been fsynced.
//
// # Version tokens
//
// A version token is an opaque, non-empty string. Every successful write to a
// key produces a token that differs from every token that key has had before,
// including when the content is identical and including after the key was
// deleted and created again. Callers compare tokens only for equality and
// never parse them. Create, Replace, Get, GetRange and Head all return the
// token of the object they wrote or observed.
//
// A hash of the content, such as an MD5-based ETag, is therefore not a valid
// token on its own: two writes of the same bytes would share it, and a
// compare-and-swap could succeed against an object rewritten in between.
//
// # Reads
//
// [Store.Get] returns the whole object. [Store.GetRange] returns length bytes
// starting at offset, clipped to the object:
//
//   - offset < 0 or length < 0 fails with [ErrInvalidArgument];
//   - the body holds bytes [offset, min(offset+length, size));
//   - offset at or past the end, or length 0, gives an empty body and no error;
//   - length may run past the end, up to math.MaxInt64, and is clipped;
//   - a zero-length object behaves like any other object of size 0.
//
// An HTTP range cannot express zero bytes, and services answer a range that
// starts at or past the end with 416; a backend handles both and returns an
// empty body with the object's Info.
//
// Both return the object's [Info] (size and version token) for the version the
// body comes from, so a caller can tell whether the object changed between two
// reads. A missing key fails with [ErrNotFound]. The caller must close the
// body. Errors while reading the body, including the end of the context the
// body was opened with, are returned by the body's Read.
//
// [Store.Head] returns the same [Info] without a body.
//
// # Listing
//
// [Store.List] returns the objects whose keys start with prefix and sort after
// startAfter, in ascending byte order of the full key, at most limit of them.
// limit must be between 1 and [MaxListLimit]. A page shorter than limit is the
// last page; a backend whose service returns short pages must keep fetching
// until it has limit entries or the listing ends. To get the next page, pass
// the last key of the previous page as startAfter. prefix and startAfter need
// not be valid keys; an empty prefix lists everything and an empty startAfter
// starts at the beginning.
//
// Listing is consistent with completed writes and deletes: an object whose
// Create or Replace has returned is listed, and one whose Delete has returned
// is not. A listing made with several pages while other writers are active
// sees, on each page, the state at the time that page was fetched.
//
// # Delete
//
// [Store.Delete] removes the object unconditionally. Deleting a missing key
// succeeds.
//
// # Errors
//
// Every error returned by a [Store] method is an [*Error] carrying the
// operation and the key (empty for List). It wraps one of [ErrNotFound],
// [ErrPreconditionFailed], [ErrInvalidKey] or [ErrInvalidArgument], the
// context's error when the context ended, the error returned by the caller's
// reader, or the backend's own error. Test with [errors.Is]. Errors never
// contain object contents.
//
// # Concurrency and cancellation
//
// A Store is safe for concurrent use by many goroutines. Every method honours
// its context: a context that is already done fails the call, and a context
// that ends during a write fails the write with no visible effect.
//
// # Implementing a backend
//
// Call [ValidateKey] first in every method that takes a key. Return every
// error as an [*Error] with Op set to "create", "replace", "get" (for Get and
// GetRange), "head", "list" or "delete". Document what the backend does not
// support, as package localfs does. Then call storagetest.Run from the
// backend's tests with a function that returns a new, empty store; the
// backend is done when every subtest passes.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxKeyLen is the maximum key length in bytes. 1024 is the smallest limit
// among Azure Blob Storage, Amazon S3 and Google Cloud Storage.
const MaxKeyLen = 1024

// MaxListLimit is the largest page size [Store.List] accepts. It is the page
// size Amazon S3 returns at most, so every backend can fill a page with one
// service call.
const MaxListLimit = 1000

var (
	// ErrNotFound means no object exists at the key.
	ErrNotFound = errors.New("object not found")
	// ErrPreconditionFailed means a conditional write's condition did not hold:
	// the key already exists (Create), or it is missing or has a different
	// version token (Replace).
	ErrPreconditionFailed = errors.New("precondition failed")
	// ErrInvalidKey means the key breaks the rules in [ValidateKey].
	ErrInvalidKey = errors.New("invalid key")
	// ErrInvalidArgument means an argument other than the key is out of range,
	// such as a negative offset or a list limit outside 1..[MaxListLimit].
	ErrInvalidArgument = errors.New("invalid argument")
)

// Error is the error type returned by every [Store] method.
type Error struct {
	Op  string // "create", "replace", "get" (Get and GetRange), "head", "list" or "delete"
	Key string // the key, or empty for list
	Err error
}

func (e *Error) Error() string { return fmt.Sprintf("storage: %s %q: %v", e.Op, e.Key, e.Err) }

func (e *Error) Unwrap() error { return e.Err }

// Info describes one version of an object.
type Info struct {
	Size    int64  // length in bytes
	Version string // version token, see the package documentation
}

// Entry is one object in a listing.
type Entry struct {
	Key  string
	Size int64
}

// Store is an object store. The package documentation is its contract.
type Store interface {
	// Create writes the object at key only if no object exists there, and
	// returns its version token.
	Create(ctx context.Context, key string, body io.Reader) (version string, err error)
	// Replace writes the object at key only if the current version token is
	// version, and returns the new version token.
	Replace(ctx context.Context, key, version string, body io.Reader) (newVersion string, err error)
	// Get returns the whole object.
	Get(ctx context.Context, key string) (io.ReadCloser, Info, error)
	// GetRange returns up to length bytes of the object starting at offset.
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, Info, error)
	// Head returns the object's size and version token.
	Head(ctx context.Context, key string) (Info, error)
	// List returns up to limit entries with the given prefix whose keys sort
	// after startAfter, in byte order.
	List(ctx context.Context, prefix, startAfter string, limit int) ([]Entry, error)
	// Delete removes the object at key. Deleting a missing key succeeds.
	Delete(ctx context.Context, key string) error
}

// ValidateKey returns an error wrapping [ErrInvalidKey] if key breaks the key
// rules in the package documentation, and nil otherwise.
func ValidateKey(key string) error {
	reason := ""
	switch {
	case key == "":
		reason = "empty"
	case len(key) > MaxKeyLen:
		reason = "longer than 1024 bytes"
	case !utf8.ValidString(key):
		reason = "not valid UTF-8"
	case strings.ContainsRune(key, '\\'):
		reason = "contains a backslash"
	case strings.ContainsFunc(key, func(r rune) bool { return unicode.IsControl(r) || r == 0xFFFE || r == 0xFFFF }):
		reason = "contains a control character, U+FFFE or U+FFFF"
	default:
		for seg := range strings.SplitSeq(key, "/") {
			switch seg {
			case "":
				reason = "has an empty segment or a leading or trailing slash"
			case ".", "..":
				reason = "has a . or .. segment"
			}
			if reason != "" {
				break
			}
		}
	}
	if reason == "" {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidKey, reason)
}
