// Package storage defines the object storage interface that every skylite
// storage backend implements, the errors it returns and the key rules it
// enforces.
//
// Object storage is skylite's only durable state. A stale leader is stopped by
// conditional writes: a commit is a create-if-absent of a new object, and a
// lease or manifest is updated by compare-and-swap on a version token. The
// contract below exists so that those two operations mean the same thing on
// every backend. This documentation is the specification; package storagetest
// checks the parts of it that can be checked portably, and each backend
// carries its own tests for the rest (see "Implementing a backend").
//
// # Keys
//
// skylite generates its own keys, so the key domain is small enough that
// every backend can use a key as its native object name without encoding.
// Every backend calls [ValidateKey] on every key it receives, before any other
// work and before any I/O, and returns its error unchanged inside an
// [*Error], so an invalid key fails with [ErrInvalidKey] whatever the other
// arguments and the context are. A key is valid if and only if:
//
//   - it is 1 to [MaxKeyLen] bytes long;
//   - it consists of 1 to [MaxKeySegments] segments separated by "/", so it
//     has no leading or trailing "/" and no empty segment;
//   - every segment is one or more of the characters A-Z, a-z, 0-9, ".", "_"
//     and "-", and does not end with "." (which also excludes "." and "..";
//     Azure Blob Storage advises against names ending in a dot).
//
// "/" separates segments by convention only: there are no directories, and
// "a/b" and "a/b/c" can both exist. Keys are case-sensitive and are compared
// and sorted as byte strings. Keys that differ only in case are different
// keys, and a backend must keep them apart even if the underlying system
// would not.
//
// # Mutations
//
// There is no unconditional put. The only mutations are:
//
//   - [Store.Create]: write the object only if no object exists at the key.
//   - [Store.Replace]: write the object only if an object exists at the key and
//     its current version token equals the one given (compare-and-swap).
//   - [Store.Delete]: remove the object unconditionally. Deleting a missing
//     key succeeds.
//
// A failed condition returns [ErrPreconditionFailed]. Replace on a missing key
// is a precondition failure, not [ErrNotFound]. Replace with an empty version
// always fails its precondition. A Replace conditioned on a version that a
// completed Delete removed fails, even if it started before the Delete.
//
// Writes are atomic. A new version becomes visible at one instant and
// complete: a concurrent or later reader sees the whole old object, the whole
// new object, or not-found; never a partial object, and never the body of one
// version with the [Info] of another. Not-found is seen only before a Create
// or after a Delete: a Replace is a single atomic overwrite, so a key being
// replaced never reads as missing and a Create on it never succeeds. A
// backend does not implement Replace as a delete followed by a create.
//
// Reads are strongly consistent with completed mutations, on every client: a
// Get, GetRange, Head or List that starts after a mutation returned success,
// on any client of the store, observes that mutation or a later one. skylite
// relies on this for correctness, so a backend never answers from a cache or
// a replica that can lag behind the store.
//
// A write reads the caller's body to its end before it publishes anything.
// Until then, nothing about the write is visible to readers, listings or
// other writers, and a write that stops there (the body's Read fails, the
// context ends, the process is killed, the upload fails) does not stop a later
// Create of the same key from succeeding. While it reads the body, a write
// must not hold anything that blocks other mutations: cancellation cannot
// interrupt a Read of the caller's body (see below), so such a hold would let
// one stalled caller stop every writer.
//
// Among any number of concurrent Create calls for one absent key, exactly one
// succeeds; among concurrent Replace calls with the same current version,
// exactly one succeeds. This holds across goroutines, clients, processes and
// hosts that share the store, to the extent stated by each backend's
// documentation.
//
// A mutation returns success only after its effect is durable in the backend:
// for a cloud service, after the service has acknowledged it; for a local
// filesystem, after the data and the directory have been fsynced. For Delete
// this means a later crash or power loss cannot bring the object back, also
// when the key was already missing on entry, since another caller's Delete of
// it may have unlinked it without making that durable.
//
// # Outcome of a failed mutation
//
// One rule covers every error from Create, Replace and Delete, whatever its
// cause, cancellation included:
//
//   - nil means the mutation took effect and is durable;
//   - an error wrapping [ErrOutcomeUnknown] means it may or may not have taken
//     effect, now, later or after a crash (see below);
//   - any other error means it had no effect, visible now or after a crash.
//
// An unknown outcome is not resolved by reading the key once. A request to a
// cloud service can still be in flight after the call has returned, so a read
// can see the old state and the original request can commit afterwards. Reading
// tells the caller only this: finding content that only this write could have
// produced confirms that it took effect; finding older content, other content
// or no object does not establish that it failed. A failed write returns no
// version token, so a caller that resolves an unknown outcome by reading
// content is responsible for that content being unique to its write, for
// example by including a random value, or its own identity and a sequence
// number. Otherwise equal content proves nothing: another writer may have
// written the same bytes. The caller then does one of two things. Either it
// repeats the same conditional write until it gets a definite answer: success,
// or [ErrPreconditionFailed] followed by a read that shows whose content is
// there. Or it arranges that the outstanding request can no longer matter, for
// example by making sure its condition can never hold again. Delete has no
// condition: repeating it until it succeeds leaves the key missing when it
// returns, but the first request may still remove an object created at the key
// afterwards, so a caller does not create a key again after a Delete of it
// whose outcome was unknown, unless that removal would not matter. Create has
// the same gap: its condition, that the key is absent, holds again whenever
// the key is deleted. Finding out what the key holds now, by a definite answer
// or a read, does not stop an earlier Create attempt that is still in flight:
// if the key later becomes absent again, that attempt can still succeed. So a
// caller that deletes a key, or lets it be deleted, first makes sure that no
// earlier Create of it whose outcome was unknown can still land, unless that
// would not matter.
//
// Publication begins when a write has read its body to its end, or when a
// Delete sends the request or makes the system call that removes the object.
// Before publication begins a backend can always undo its work, so it never
// returns [ErrOutcomeUnknown]. After it has begun, a backend returns
// [ErrOutcomeUnknown] for any error unless it knows the mutation had no
// effect. Examples: the context ended, or the connection failed, after the
// request that commits the write was sent and before its response arrived; a
// local link, rename or unlink succeeded and the following directory fsync
// failed.
//
// Cancellation is a case of this rule, not an exception to it. A context that
// is done when a call starts fails the call. A context that ends before
// publication begins fails the mutation with the context's error and no
// effect. A context that ends after publication has begun gives success, an
// error with no effect, or an error wrapping both [ErrOutcomeUnknown] and the
// context's error. Cancellation cannot interrupt a call that is blocked
// inside the caller's own body Read: a backend sees the context end only when
// that Read returns, so a caller that needs a bounded write gives it a reader
// that returns.
//
// A backend whose SDK retries a request decides the outcome of the whole call,
// not of its last attempt. A precondition failure returned after a retry of an
// attempt whose response was lost does not prove that the first attempt had no
// effect: it may have committed, and the retry then failed its condition
// against it, or against a later write. Such a backend may turn that failure
// into success only by a per-write identity: a value unique to this one
// write (for example a random value in the object's metadata) that it sent
// with the write and then reads back from the stored object. Finding its own
// identity means the write took effect, at the version that carries it;
// anything else is [ErrOutcomeUnknown], never [ErrPreconditionFailed]. It never
// compares payloads: equal content does not identify a write, because another
// writer may have stored the same bytes in between. A backend that attaches no
// such identity returns [ErrOutcomeUnknown] for every precondition failure
// that follows a retry.
//
// The portable suite cannot lose a response, make an SDK retry or observe a
// request that is still in flight after its call returned. A backend that
// returns a definite error and lets the request commit afterwards passes it.
// Each cloud backend therefore carries controlled tests of its own: lost
// responses and retries, classified as above; and requests still pending when
// the call returns, showing that every definite error it returns leaves no
// effect, then or later.
//
// # Staged uploads
//
// The portable suite cannot force a backend onto its service's staged upload
// path (S3 multipart upload, an Azure block list, a GCS resumable upload). A
// backend therefore does one of two things. Either it publishes every object
// with one atomic request and rejects a body larger than that request's limit
// with [ErrInvalidArgument] and no effect, documenting the limit; or it
// carries its own tests showing that an upload interrupted inside the staged
// path leaves nothing visible and does not stop a later Create of the key.
//
// # Version tokens
//
// A version token is an opaque, non-empty string that identifies exactly one
// stored version of its key. Every successful write to a key produces a token
// that differs from every token that key has had before, including when the
// content is identical (an empty body included) and including after the key was
// deleted and created again, so a token never repeats for a key. Callers
// compare tokens only for equality and never parse them. Create, Replace, Get,
// GetRange and Head all return the token of the object they wrote or observed.
//
// A hash of the content, such as an MD5-based ETag, is therefore not a valid
// token on its own: two writes of the same bytes would share it, and a
// compare-and-swap could succeed against an object rewritten in between.
//
// Implementation note for S3: an S3 ETag reflects the content, so it cannot be
// the token by itself. Reading the object's version id and then writing with
// an If-Match condition on the ETag does not close the gap either: another
// writer can rewrite identical content between the read and the conditional
// write, the ETag still matches, and the compare-and-swap succeeds against a
// version it never saw. The token and the write condition must name the same
// per-write identity.
//
// Implementation note for Azure Blob Storage: the service documents that a
// blob's ETag changes on every write, but not that an ETag never recurs for a
// blob name across a delete and a re-create. An Azure backend that uses the
// ETag as the token must justify that it never repeats for a key, or use
// another mechanism, such as a per-write identity stored with the blob and
// compared under a blob lease.
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
// body comes from, and every byte of the body comes from that version, also for
// a bounded range; so a caller can tell whether the object changed between two
// reads. A missing key fails with [ErrNotFound]. The caller must close the
// body. Errors while reading the body, including the end of the context the
// body was opened with, are returned by the body's Read; once that context has
// ended, Read fails with an error err for which errors.Is(err, ctx.Err()) is
// true.
//
// An open body belongs to one version: every byte it returns comes from the
// version its Info describes, whenever it is read. If that version is
// replaced or deleted before the body has been read to its end, the body's
// Read either keeps returning that version's bytes or fails with an error; it
// never returns bytes of another version. (A cloud backend cannot promise to
// finish such a read: a dropped connection cannot be resumed against a version
// that no longer exists.)
//
// [Store.Head] returns the same [Info] without a body.
//
// # Listing
//
// [Store.List] returns the objects whose keys start with prefix and sort after
// startAfter, in ascending byte order of the full key, at most limit of them.
// limit must be between 1 and [MaxListLimit]. A result shorter than limit
// means there are no more entries. Services may return fewer entries than
// requested, and Azure Blob Storage and Google Cloud Storage document that
// they do; a backend on such a service keeps requesting until it has limit
// entries or the listing ends. That is each such backend's obligation, and
// its own tests force its service to return short pages. To get the next
// page, pass the last key of the previous page as startAfter. prefix and
// startAfter need not be valid keys; an empty prefix lists everything and an
// empty startAfter starts at the beginning.
//
// Listing is consistent with completed mutations: an object whose Create or
// Replace returned before List was called is listed with its new size, and
// one whose Delete returned before is not. A listing is not a snapshot. Each
// entry reports a state its key actually had at some moment during the call.
// A key that exists, unchanged, for the whole call appears exactly once. A key
// created, replaced or deleted during the call is listed in the state it had
// before or after that change, which for a create or a delete may be absent.
// Entries are in strictly ascending key order with no duplicates. Nothing
// above the storage layer may rely on a listing, or on a sequence of pages,
// being a point-in-time view.
//
// # Errors
//
// Every error returned by a [Store] method is an [*Error] carrying the
// operation and the key (empty for List). It wraps one of [ErrNotFound],
// [ErrPreconditionFailed], [ErrInvalidKey] or [ErrInvalidArgument], the
// context's error when the context ended, a [*BodyError] holding the error
// returned by the caller's body reader, or the backend's own error, and
// additionally [ErrOutcomeUnknown] when the rule above requires it. Test with
// [errors.Is] and [errors.As].
//
// [errors.Is] and [errors.As] do not look inside a [*BodyError]: the caller's
// reader may return any error, including one of this package's sentinels or a
// context error, and that must not change how the write is classified. A
// write whose reader returned [ErrOutcomeUnknown] or [ErrPreconditionFailed]
// is a write that failed reading its body, with no effect. To inspect the
// reader's error, get the [*BodyError] with [errors.As] and look at its Err
// field.
//
// Error text never contains object contents or credentials (access keys,
// tokens, signed URLs). A body reader's error is wrapped in a [*BodyError],
// whose text is fixed, because the caller's reader may put anything in its
// error. A backend whose SDK puts contents or credentials in its error
// strings redacts them before returning the error. Errors from the operating
// system keep their text.
//
// # Concurrency
//
// A Store is safe for concurrent use by many goroutines.
//
// # Implementing a backend
//
// Call [ValidateKey] first in every method that takes a key. Return every
// error as an [*Error] with Op set to "create", "replace", "get" (for Get and
// GetRange), "head", "list" or "delete", wrap body reader errors in a
// [*BodyError], and document what the backend does not support, as package
// localfs does. Then call storagetest.Run from the backend's tests.
//
// Passing storagetest.Run is necessary, not sufficient. The suite runs its
// clients in one process and cannot crash the backend, cut its network or
// inspect its service, so each backend also tests, itself: races between
// processes or hosts; the critical interleavings of its own conditional writes,
// forced with its own barriers or hooks (the suite's race tests only stress the
// scheduler); a process killed during and just after publication; lost
// responses and SDK retries, and that it classifies them as the outcome rule
// requires; requests still pending when a call returns, and that a definite
// error leaves no effect, then or later; page filling, if its service can
// return pages shorter than requested; its staged upload path (see above) or
// its size limit; that durability holds where it can be observed, such as the
// directory fsync of a local backend; and that its SDK's errors are redacted.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// MaxKeyLen is the maximum key length in bytes. 1024 is the smallest limit
// among Azure Blob Storage, Amazon S3 and Google Cloud Storage.
const MaxKeyLen = 1024

// MaxKeySegments is the maximum number of "/"-separated segments in a key,
// well inside Azure Blob Storage's limit of 254.
const MaxKeySegments = 32

// MaxListLimit is the largest page size [Store.List] accepts. It is the most
// entries Amazon S3 returns in one response. A backend whose service returns
// fewer keeps requesting until the page is full; see the package
// documentation.
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
	// such as a negative offset, a list limit outside 1..[MaxListLimit] or a
	// body larger than the backend accepts.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrOutcomeUnknown means a mutation failed after its publication began
	// and may or may not have taken effect; see the package documentation.
	ErrOutcomeUnknown = errors.New("outcome unknown")
)

// Error is the error type returned by every [Store] method.
type Error struct {
	Op  string // "create", "replace", "get" (Get and GetRange), "head", "list" or "delete"
	Key string // the key, or empty for list
	Err error
}

func (e *Error) Error() string { return fmt.Sprintf("storage: %s %q: %v", e.Op, e.Key, e.Err) }

func (e *Error) Unwrap() error { return e.Err }

// BodyError holds an error returned by the caller's body reader in Err. Its
// text is fixed, because the reader's error may carry object contents. It has
// no Unwrap method on purpose: whatever the reader returned, [errors.Is] on
// the write's error never matches it, so a reader cannot make a write look
// like it failed for any other reason. Inspect Err directly.
type BodyError struct{ Err error }

func (e *BodyError) Error() string { return "reading the body failed" }

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
	if key == "" || len(key) > MaxKeyLen {
		return fmt.Errorf("%w: length must be 1 to %d bytes", ErrInvalidKey, MaxKeyLen)
	}
	if strings.Count(key, "/") >= MaxKeySegments {
		return fmt.Errorf("%w: more than %d segments", ErrInvalidKey, MaxKeySegments)
	}
	for seg := range strings.SplitSeq(key, "/") {
		if seg == "" || seg[len(seg)-1] == '.' {
			return fmt.Errorf("%w: empty segment, leading or trailing slash, or segment ending in a dot", ErrInvalidKey)
		}
		if strings.ContainsFunc(seg, func(r rune) bool {
			return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-'
		}) {
			return fmt.Errorf("%w: character outside A-Z a-z 0-9 . _ -", ErrInvalidKey)
		}
	}
	return nil
}
