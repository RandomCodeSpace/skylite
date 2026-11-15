// Package storagetest is the portable part of the conformance suite for
// storage.Store backends.
//
// A backend's tests call [Run] with a function that creates a new, empty
// namespace and returns a way to open independent clients onto it. Every
// subtest gets its own namespace.
//
// The suite covers what can be checked through the interface from one
// process: key validation, conditional writes and version tokens, range
// reads, listing order, selectors that are not keys, pagination (including a
// listing longer than storage.MaxListLimit) and listing while other keys come
// and go, every read path on its own after another client's mutation, failed
// and cancelled writes (including a context cancelled, or a reader failing, at
// the very end of the body), contexts cancelled or past their deadline, what
// readers and other writers on the same and on other clients see while a
// write is stopped before and inside its body, open bodies across a Replace or
// Delete, error types, operations, keys and texts, the precedence of an
// invalid key over every other argument, that a failed write returns no
// token, reader errors that are storage sentinels, and races between
// independent clients: Create against Create, Replace against Replace,
// Replace against Delete and Create against Delete.
//
// Every test of conditional writes (the race tests included), version
// tokens, deletion, open bodies, range reads and the end of a body runs at
// three body sizes, each in its own namespace: empty, small (11 bytes) and
// large (17 MiB and 3 bytes, larger than any cloud SDK's default chunk; see
// largeSize). The concurrent readers see versions of all three sizes, and a
// write in progress is stopped after a large prefix and after a short read.
// After a mutation the suite reads the key back through the writing client
// and through another client that read the key before the mutation, with Head
// first, then a bounded GetRange, Get and List; that ordering keeps only Head
// from being refreshed by another path. FreshReads covers the others: there
// each read path is the first call, after another client's mutation, of its
// own client, which has read the old state through every path.
//
// The race tests are scheduling stress tests. Their gated bodies prove that
// the racing calls were in progress at the same time, not that they met
// inside a backend's critical window between checking a condition and
// publishing; a Delete started during a Replace may well run before or after
// that window. Each backend therefore forces its critical interleavings
// itself, with its own barriers or hooks, as package localfs does.
//
// It cannot cover races between processes or hosts, a process killed during
// or just after publication, durability across a crash or power loss, lost
// responses and SDK retries and their classification, a request still in
// flight after its call returned (a backend that returns a definite error and
// lets the request commit later passes the suite), short pages returned by the
// service, an interrupted staged upload, a size limit, or redaction of a
// service SDK's errors. Each backend tests those itself, as the storage package
// documentation requires. Passing [Run] is necessary for a backend to be
// accepted, not sufficient.
//
// Counted on localfs with a wrapper that counts calls and body bytes, a full
// run makes about 5,800 storage requests outside ListDuringWrites and
// ConcurrentReadsSeeWholeVersions, sends about 6.9 GiB of bodies to writes
// and reads about 2.0 GiB. The large rounds of ConcurrentCreate and
// ConcurrentReplace (16 writers, 10 rounds, 17 MiB each) send 5.3 GiB of
// that. ListDuringWrites lists for as long as its writer needs to make 240
// writes, so its count follows the backend's speed (about 8,000 requests on
// localfs). The four readers of ConcurrentReadsSeeWholeVersions read as fast
// as the backend lets them while it makes 41 writes of 0.2 GiB, at most 2,000
// reads each (on localfs about 10,000 requests and 0.3 to 0.5 GiB).
//
// Every wait that involves a write stopped in its body, or calls racing one,
// is bounded by a timer, not by the backend honouring its context: for the
// write to reach its stop, for calls that the stopped write must not block,
// for the write and the calls racing it to return once released, and for the
// goroutines a test leaves behind when it ends. Every other call is made
// directly, so a backend that hangs in one hangs the test until go test's own
// timeout.
//
// The suite tests itself: selftest_test.go runs it, with a shorter large
// size, against a catalogue of wrong backends, each a thin wrapper around
// package localfs that breaks one rule, and requires every one of them to be
// rejected by the subtest it names, and against correct but slow or fragile
// backends that must pass. A wrong backend whose fault shows only under an
// interleaving forces that interleaving with a barrier of its own, so that its
// rejection does not depend on scheduling.
package storagetest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// Run runs the conformance suite. newNamespace is called once per subtest, and
// once per body size for the subtests that run at several. It creates a new,
// empty namespace (a directory, or a container and prefix) and returns open,
// which opens a new client onto that namespace each time it is called. Clients
// must be independent: they may share nothing in the process that the
// backend's correctness depends on, so that two clients behave like two
// processes or hosts. Register cleanup with the t given to newNamespace.
func Run(t *testing.T, newNamespace func(t *testing.T) (open func() storage.Store)) {
	run(t, func(t reporter) func() storage.Store { return newNamespace(t.(*testing.T)) })
}

// reporter is the part of *testing.T the suite uses. It is an interface so
// that the suite's own tests can run the suite and observe it fail.
type reporter interface {
	Helper()
	Error(args ...any)
	Errorf(format string, args ...any)
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Cleanup(f func())
}

// subtest runs fn as the subtest name of t.
func subtest(t reporter, name string, fn func(t reporter)) {
	if tt, ok := t.(*testing.T); ok {
		tt.Run(name, func(t *testing.T) { fn(t) })
		return
	}
	t.(interface{ run(string, func(reporter)) }).run(name, fn)
}

// namespaces creates a new namespace for t and returns a way to open clients
// onto it.
type namespaces = func(t reporter) (open func() storage.Store)

// tests lists the suite's subtests in the order they run.
var tests = []struct {
	name string
	fn   func(t reporter, newNamespace namespaces)
}{
	{"InvalidKeys", one(testInvalidKeys)},
	{"ValidKeys", one(testValidKeys)},
	{"NotFound", one(testNotFound)},
	{"CreateExisting", sized(testCreateExisting)},
	{"Replace", sized(testReplace)},
	{"VersionTokens", sized(testVersionTokens)},
	{"Delete", sized(testDelete)},
	{"FreshReads", sized(testFreshReads)},
	{"RangeReads", sized(testRangeReads)},
	{"ListOrder", one(testListOrder)},
	{"ListPagination", one(testListPagination)},
	{"ListLimits", one(testListLimits)},
	{"ListBeyondMaxLimit", one(testListBeyondMaxLimit)},
	{"ListDuringWrites", multi(testListDuringWrites)},
	{"ConcurrentCreate", sized(testConcurrentCreate)},
	{"ConcurrentReplace", sized(testConcurrentReplace)},
	{"ReplaceDeleteRace", sized(testReplaceDeleteRace)},
	{"CreateDeleteRace", sized(testCreateDeleteRace)},
	{"ConcurrentReadsSeeWholeVersions", multi(testConcurrentReads)},
	{"OpenBodyKeepsItsVersion", sized(testOpenBodies)},
	{"WriteInProgress", multi(testWriteInProgress)},
	{"EndOfBody", sized(testEndOfBody)},
	{"CancelledContext", one(testCancelledContext)},
	{"BodyHonoursContext", one(testBodyHonoursContext)},
	{"Errors", one(testErrors)},
	{"BodyErrorText", one(testBodyErrorText)},
}

// smallSize is a body smaller than any buffer a backend is likely to have.
const smallSize = 11

// largeSize is the size of the suite's large bodies: 17 MiB and 3 bytes. It
// is larger than the largest default chunk of the cloud SDKs (the Google Cloud
// Storage writer uploads in 16 MiB chunks; the S3 transfer manager uses 5 MiB
// parts; the Azure Blob Storage SDK uploads streams in 1 MiB blocks and
// downloads in 4 MiB blocks) and than every I/O buffer, and it is odd, so no
// power-of-two buffer or chunk size, nor 5 MiB, divides it and every chunked
// path ends on a partial chunk. The suite's own tests shorten it.
var largeSize = 17<<20 + 3

type bodySize struct {
	name string
	n    int
}

func sizes() []bodySize {
	return []bodySize{{"empty", 0}, {"small", smallSize}, {"large", largeSize}}
}

func one(fn func(reporter, storage.Store)) func(reporter, namespaces) {
	return func(t reporter, ns namespaces) { fn(t, ns(t)()) }
}

func multi(fn func(reporter, func() storage.Store)) func(reporter, namespaces) {
	return func(t reporter, ns namespaces) { fn(t, ns(t)) }
}

// sized runs fn once per body size, as a subtest named after the size, each
// in its own namespace.
func sized(fn func(t reporter, open func() storage.Store, n int)) func(reporter, namespaces) {
	return func(t reporter, ns namespaces) {
		for _, size := range sizes() {
			subtest(t, size.name, func(t reporter) { fn(t, ns(t), size.n) })
		}
	}
}

func run(t reporter, newNamespace namespaces) {
	for _, tc := range tests {
		subtest(t, tc.name, func(t reporter) { tc.fn(t, newNamespace) })
	}
}

var ctx = context.Background()

// content returns n bytes made from tag: a block of 8,191 pseudo-random bytes
// seeded by tag, repeated. Contents made from different tags differ, and as
// 8,191 is prime, no shift by an amount that is not a multiple of it (any
// power of two, any chunk size) maps a content onto itself.
func content(tag string, n int) string {
	block := make([]byte, 8191)
	_, _ = rand.NewChaCha8(sha256.Sum256([]byte(tag))).Read(block)
	return strings.Repeat(string(block), n/len(block)+1)[:n]
}

func create(t reporter, s storage.Store, key, content string) string {
	t.Helper()
	v, err := s.Create(ctx, key, strings.NewReader(content))
	if err != nil {
		t.Fatalf("Create(%q): %v", key, err)
	}
	if v == "" {
		t.Fatalf("Create(%q) returned an empty version token", key)
	}
	return v
}

func replace(t reporter, s storage.Store, key, version, content string) string {
	t.Helper()
	v, err := s.Replace(ctx, key, version, strings.NewReader(content))
	if err != nil {
		t.Fatalf("Replace(%q): %v", key, err)
	}
	if v == "" {
		t.Fatalf("Replace(%q) returned an empty version token", key)
	}
	return v
}

func readAll(t reporter, body io.ReadCloser) string {
	t.Helper()
	defer func() { _ = body.Close() }()
	b, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(b)
}

func get(t reporter, s storage.Store, key string) (string, storage.Info) {
	t.Helper()
	body, info, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return readAll(t, body), info
}

// expectError checks that err is the *storage.Error of operation op on key
// and that it wraps want.
func expectError(t reporter, err error, op, key string, want error) {
	t.Helper()
	se, ok := err.(*storage.Error) //nolint:errorlint // the contract requires the returned error itself to be a *storage.Error
	switch {
	case err == nil:
		t.Fatalf("%s %q succeeded, want an error wrapping %v", op, key, want)
	case !ok:
		t.Fatalf("%s %q error %v is a %T, want a *storage.Error", op, key, err, err)
	case se.Op != op || se.Key != key:
		t.Fatalf("%s %q error %v has Op %q and Key %q", op, key, err, se.Op, se.Key)
	case !errors.Is(err, want):
		t.Fatalf("%s %q error = %v, want it to wrap %v", op, key, err, want)
	}
}

// expectWriteError is expectError for a Create or Replace, which must also
// have returned no version token.
func expectWriteError(t reporter, version string, err error, op, key string, want error) {
	t.Helper()
	expectError(t, err, op, key, want)
	noToken(t, version, err, op, key)
}

// noToken checks that a write that failed with err returned no version token.
func noToken(t reporter, version string, err error, op, key string) {
	t.Helper()
	if err != nil && version != "" {
		t.Fatalf("%s %q failed with %v and returned the version token %q; a failed write returns none", op, key, err, version)
	}
}

// sentinels are the errors a caller's body reader may return without changing
// how the write is classified.
var sentinels = []error{
	storage.ErrNotFound, storage.ErrPreconditionFailed, storage.ErrInvalidKey, storage.ErrInvalidArgument,
	storage.ErrOutcomeUnknown, context.Canceled, context.DeadlineExceeded,
}

// expectBodyError checks that err is the *storage.Error of write op on key
// failing because its body reader returned readerErr: it holds readerErr in a
// *storage.BodyError, errors.Is matches none of the sentinels, and the write
// returned no version token.
func expectBodyError(t reporter, version string, err error, op, key string, readerErr error) {
	t.Helper()
	noToken(t, version, err, op, key)
	se, ok := err.(*storage.Error) //nolint:errorlint // the contract requires the returned error itself to be a *storage.Error
	switch {
	case err == nil:
		t.Fatalf("%s %q whose body reader failed with %q succeeded", op, key, readerErr)
	case !ok:
		t.Fatalf("%s %q error %v is a %T, want a *storage.Error", op, key, err, err)
	case se.Op != op || se.Key != key:
		t.Fatalf("%s %q error %v has Op %q and Key %q", op, key, err, se.Op, se.Key)
	case !errors.Is(readerError(err), readerErr):
		t.Fatalf("%s %q whose body reader failed with %q: error %v does not hold the reader's error in a *storage.BodyError", op, key, readerErr, err)
	}
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			t.Fatalf("%s %q whose body reader failed with %q: errors.Is(error, %q) is true, but only the reader returned it", op, key, readerErr, sentinel)
		}
	}
}

// expectContent checks through s that key holds content at version (any
// version if ""). It asks Head first, so that no other read can have
// refreshed what Head returns, then reads a bounded range, the whole object
// and a listing.
func expectContent(t reporter, s storage.Store, key, content, version string) {
	t.Helper()
	head, err := s.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head(%q): %v", key, err)
	}
	want := storage.Info{Size: int64(len(content)), Version: version}
	if version == "" {
		want.Version = head.Version
	}
	if head != want {
		t.Fatalf("Head(%q) = %+v, want %+v", key, head, want)
	}
	off, n := len(content)/3, len(content)/3+1
	body, info, err := s.GetRange(ctx, key, int64(off), int64(n))
	if err != nil {
		t.Fatalf("GetRange(%q, %d, %d): %v", key, off, n, err)
	}
	if got := readAll(t, body); got != content[off:min(off+n, len(content))] || info != want {
		t.Fatalf("GetRange(%q, %d, %d) = %d bytes %.40q, %+v; want bytes [%d, %d) of the content, %+v", key, off, n, len(got), got, info, off, min(off+n, len(content)), want)
	}
	got, info := get(t, s, key)
	if got != content || info != want {
		t.Fatalf("Get(%q) = %d bytes %.40q, %+v; want %d bytes %.40q, %+v", key, len(got), got, info, len(content), content, want)
	}
	entries, err := s.List(ctx, key, "", 1)
	if err != nil {
		t.Fatalf("List(%q, \"\", 1): %v", key, err)
	}
	if wantEntry := (storage.Entry{Key: key, Size: want.Size}); len(entries) != 1 || entries[0] != wantEntry {
		t.Fatalf("List(%q, \"\", 1) = %+v, want [%+v]", key, entries, wantEntry)
	}
}

// expectMissing checks through s, with every read, that key does not exist.
func expectMissing(t reporter, s storage.Store, key string) {
	t.Helper()
	_, err := s.Head(ctx, key)
	expectError(t, err, "head", key, storage.ErrNotFound)
	_, _, err = s.GetRange(ctx, key, 0, 1)
	expectError(t, err, "get", key, storage.ErrNotFound)
	_, _, err = s.Get(ctx, key)
	expectError(t, err, "get", key, storage.ErrNotFound)
	entries, err := s.List(ctx, key, "", 1)
	if err != nil {
		t.Fatalf("List(%q, \"\", 1): %v", key, err)
	}
	if len(entries) > 0 && entries[0].Key == key {
		t.Fatalf("List(%q, \"\", 1) = %+v, which lists the missing key", key, entries)
	}
}

// checkPage checks that entries can be the result of List(prefix, startAfter,
// limit): at most limit entries, each with the prefix and sorting after
// startAfter, in strictly ascending order.
func checkPage(t reporter, entries []storage.Entry, prefix, startAfter string, limit int) {
	t.Helper()
	if len(entries) > limit {
		t.Fatalf("List(%q, %q, %d) returned %d entries", prefix, startAfter, limit, len(entries))
	}
	last := startAfter
	for i, e := range entries {
		if !strings.HasPrefix(e.Key, prefix) || e.Key <= last {
			t.Fatalf("List(%q, %q, %d) entry %d is %.40q after %.40q: entries must have the prefix and sort, strictly ascending, after startAfter", prefix, startAfter, limit, i, e.Key, last)
		}
		last = e.Key
	}
}

func listAll(t reporter, s storage.Store, prefix string) []storage.Entry {
	t.Helper()
	entries, err := s.List(ctx, prefix, "", storage.MaxListLimit)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	checkPage(t, entries, prefix, "", storage.MaxListLimit)
	return entries
}

func keys(entries []storage.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Key
	}
	return out
}

var invalidKeys = []struct{ name, key string }{
	{"empty", ""},
	{"only slash", "/"},
	{"leading slash", "/a"},
	{"trailing slash", "a/"},
	{"empty segment", "a//b"},
	{"dot", "."},
	{"dotdot", ".."},
	{"dot segment", "a/./b"},
	{"dotdot segment", "a/../b"},
	{"traversal", "../../etc/passwd"},
	{"trailing dot", "a."},
	{"segment ending in a dot", "a./b"},
	{"space", "a b"},
	{"backslash", `a\b`},
	{"NUL", "a\x00b"},
	{"newline", "a\nb"},
	{"tilde", "a~"},
	{"percent", "%2e"},
	{"colon", "a:b"},
	{"plus", "a+b"},
	{"non-ASCII", "é"},
	{"private use", "a"},
	{"invalid UTF-8", "a\xffb"},
	{"too long", strings.Repeat("a", storage.MaxKeyLen+1)},
	{"too many segments", strings.Repeat("a/", storage.MaxKeySegments) + "a"},
}

// readerFunc is an io.Reader made of a function.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// testInvalidKeys checks that every method rejects every invalid key before
// it does anything else: before it reads the body, before it looks at the
// context, whether cancelled or past its deadline, and before it looks at
// any other argument, also one that is invalid itself.
func testInvalidKeys(t reporter, s storage.Store) {
	v := create(t, s, "valid", "x")
	body := readerFunc(func([]byte) (int, error) {
		t.Error("a write with an invalid key read its body")
		return 0, io.EOF
	})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancelExpired := context.WithDeadline(ctx, time.Now().Add(-time.Hour))
	defer cancelExpired()
	for _, tc := range invalidKeys {
		subtest(t, tc.name, func(t reporter) {
			if err := storage.ValidateKey(tc.key); !errors.Is(err, storage.ErrInvalidKey) {
				t.Errorf("ValidateKey error = %v, want ErrInvalidKey", err)
			}
			for _, c := range []context.Context{ctx, cancelled, expired} {
				ops := []struct {
					op   string
					call func() (string, error)
				}{
					{"create", func() (string, error) { return s.Create(c, tc.key, body) }},
					{"replace", func() (string, error) { return s.Replace(c, tc.key, v, body) }},
					{"replace", func() (string, error) { return s.Replace(c, tc.key, "", body) }},
					{"get", func() (string, error) { _, _, err := s.Get(c, tc.key); return "", err }},
					{"get", func() (string, error) { _, _, err := s.GetRange(c, tc.key, 0, 1); return "", err }},
					{"get", func() (string, error) { _, _, err := s.GetRange(c, tc.key, -1, 1); return "", err }},
					{"get", func() (string, error) { _, _, err := s.GetRange(c, tc.key, 0, -1); return "", err }},
					{"head", func() (string, error) { _, err := s.Head(c, tc.key); return "", err }},
					{"delete", func() (string, error) { return "", s.Delete(c, tc.key) }},
				}
				for _, o := range ops {
					version, err := o.call()
					expectWriteError(t, version, err, o.op, tc.key, storage.ErrInvalidKey)
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, storage.ErrPreconditionFailed) {
						t.Fatalf("%s error = %v, want ErrInvalidKey and not the error of another argument or of the context", o.op, err)
					}
				}
			}
		})
	}
	if got := keys(listAll(t, s, "")); !slices.Equal(got, []string{"valid"}) {
		t.Fatalf("after invalid-key operations List = %q, want only %q", got, "valid")
	}
}

// validKeys sort awkwardly, look like paths, differ only in case, or reach
// the length and segment limits.
var validKeys = []string{
	"a", "a/b", "a.b", "a/b/c", "a0", "a-b", "a_b", "b", "Z", "z", "A",
	"..a", ".a/b", "...a", "a/..b/c", "-", "_", "Ab", "aB", "AB", "ab", "x.y.z",
	strings.Repeat("k", storage.MaxKeyLen),
	strings.Repeat("s/", storage.MaxKeySegments-1) + "s",
	strings.Repeat(strings.Repeat("m", 31)+"/", storage.MaxKeySegments-1) + strings.Repeat("m", 32),
}

func testValidKeys(t reporter, s storage.Store) {
	if k := validKeys[len(validKeys)-1]; len(k) != storage.MaxKeyLen || strings.Count(k, "/") != storage.MaxKeySegments-1 {
		t.Fatalf("fixture: longest key has %d bytes and %d segments", len(k), strings.Count(k, "/")+1)
	}
	for i, k := range validKeys {
		if err := storage.ValidateKey(k); err != nil {
			t.Fatalf("ValidateKey(%.40q): %v", k, err)
		}
		create(t, s, k, fmt.Sprint(i))
	}
	for i, k := range validKeys {
		expectContent(t, s, k, fmt.Sprint(i), "")
	}
	want := slices.Clone(validKeys)
	slices.Sort(want)
	if got := keys(listAll(t, s, "")); !slices.Equal(got, want) {
		t.Fatalf("List = %q, want %q", got, want)
	}
}

func testNotFound(t reporter, s storage.Store) {
	create(t, s, "other", "x")
	expectMissing(t, s, "missing")
	_, _, err := s.GetRange(ctx, "missing", 0, 0)
	expectError(t, err, "get", "missing", storage.ErrNotFound)
	v, err := s.Replace(ctx, "missing", "", strings.NewReader("x"))
	expectWriteError(t, v, err, "replace", "missing", storage.ErrPreconditionFailed)
	expectMissing(t, s, "missing")
}

// testCreateExisting creates a key that another client has seen missing and
// then creates it again through both clients.
func testCreateExisting(t reporter, open func() storage.Store, n int) {
	w, r := open(), open()
	first, second := content("first", n), content("second", n)
	expectMissing(t, r, "k")
	v := create(t, w, "k", first)
	expectContent(t, r, "k", first, v)
	expectContent(t, w, "k", first, v)
	for _, s := range []storage.Store{w, r} {
		v, err := s.Create(ctx, "k", strings.NewReader(second))
		expectWriteError(t, v, err, "create", "k", storage.ErrPreconditionFailed)
	}
	expectContent(t, r, "k", first, v)
	expectContent(t, w, "k", first, v)
}

// testReplace replaces a key that another client has read, replaces it with
// stale versions and replaces a missing key, through both clients.
func testReplace(t reporter, open func() storage.Store, n int) {
	w, r := open(), open()
	both := []storage.Store{r, w}
	first, second, third := content("one", n), content("two", n), content("three", n)
	v1 := create(t, w, "k", first)
	expectContent(t, r, "k", first, v1)
	v2 := replace(t, w, "k", v1, second)
	if v2 == v1 {
		t.Fatalf("Replace returned the old version token %q", v1)
	}
	for _, s := range both {
		expectContent(t, s, "k", second, v2)
	}
	for _, stale := range []string{v1, "", "not-a-version", v2 + "x"} {
		for _, s := range both {
			v, err := s.Replace(ctx, "k", stale, strings.NewReader(third))
			expectWriteError(t, v, err, "replace", "k", storage.ErrPreconditionFailed)
		}
	}
	for _, s := range both {
		expectContent(t, s, "k", second, v2)
	}
	expectMissing(t, r, "missing")
	for _, s := range both {
		for _, version := range []string{v2, ""} {
			v, err := s.Replace(ctx, "missing", version, strings.NewReader(third))
			expectWriteError(t, v, err, "replace", "missing", storage.ErrPreconditionFailed)
		}
	}
	for _, s := range both {
		expectMissing(t, s, "missing")
	}
	v3 := replace(t, r, "k", v2, first)
	for _, s := range []storage.Store{w, r} {
		expectContent(t, s, "k", first, v3)
	}
	// A Replace that changes the size, so that a stale size shows.
	resized := content("resized", n/2+1)
	v4 := replace(t, w, "k", v3, resized)
	for _, s := range both {
		expectContent(t, s, "k", resized, v4)
	}
}

// testVersionTokens writes a key with identical content in a row, returns to
// earlier content (A, B, A), passes through an empty body, deletes the key and
// creates it again, alternating clients. Every token must be new for the key,
// and straight after each write every earlier token must fail a Replace,
// through both clients.
func testVersionTokens(t reporter, open func() storage.Store, n int) {
	w, r := open(), open()
	clients := []storage.Store{w, r}
	a, b := content("same", n), content("other", n)
	var seen []string
	v := ""
	staleFail := func() {
		t.Helper()
		for i, old := range seen {
			if old == v {
				continue
			}
			s := clients[i%2]
			got, err := s.Replace(ctx, "k", old, strings.NewReader("stale"))
			expectWriteError(t, got, err, "replace", "k", storage.ErrPreconditionFailed)
		}
	}
	write := func(i int, c string) {
		t.Helper()
		if v == "" {
			v = create(t, clients[i%2], "k", c)
		} else {
			v = replace(t, clients[i%2], "k", v, c)
		}
		if slices.Contains(seen, v) {
			t.Fatalf("write %d returned the version token %q, which the key had before", i, v)
		}
		seen = append(seen, v)
		staleFail()
		for _, s := range clients {
			expectContent(t, s, "k", c, v)
		}
	}
	for i, c := range []string{a, a, b, a, "", a, "", "", b} {
		write(i, c)
	}
	if err := w.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	v = ""
	staleFail()
	for _, s := range clients {
		expectMissing(t, s, "k")
	}
	for i, c := range []string{a, b, a, a} {
		write(i+1, c)
	}
	// Tokens of different keys are not required to differ, only to be stable.
	other := create(t, w, "other", a)
	expectContent(t, r, "other", a, other)
}

// testDelete deletes a key that another client has read, deletes it again
// and deletes a key that never existed, then creates the key again and
// deletes it a second time.
func testDelete(t reporter, open func() storage.Store, n int) {
	w, r := open(), open()
	c := content("x", n)
	v := create(t, w, "k", c)
	create(t, w, "k2", content("y", n))
	expectContent(t, r, "k", c, v)
	if err := w.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, s := range []storage.Store{r, w} {
		expectMissing(t, s, "k")
		if got := keys(listAll(t, s, "")); !slices.Equal(got, []string{"k2"}) {
			t.Fatalf("List after Delete = %q, want [k2]", got)
		}
	}
	if err := r.Delete(ctx, "k"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if err := w.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("Delete of a missing key: %v", err)
	}
	again := content("again", n)
	v = create(t, r, "k", again)
	expectContent(t, w, "k", again, v)
	expectContent(t, r, "k", again, v)
	// A second Delete of the key, once it has been created again.
	if err := w.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete of the key created again: %v", err)
	}
	for _, s := range []storage.Store{r, w} {
		expectMissing(t, s, "k")
	}
}

// testFreshReads checks every read path on its own. Four clients read the key
// through every path; another client creates, replaces or deletes it; then
// each of the four makes one read, its first call after that mutation, each
// through another path: Head, Get, a bounded GetRange and List. A client that
// answers one path from what it read before, and refreshes it only when
// another path is used, fails here. The replacements change the size, so that
// a stale size shows.
func testFreshReads(t reporter, open func() storage.Store, n int) {
	w := open()
	readers := openClients(open, 4)
	prime := func(c, version string) {
		t.Helper()
		for _, r := range readers {
			if version == "" {
				expectMissing(t, r, "k")
			} else {
				expectContent(t, r, "k", c, version)
			}
		}
	}
	c1, c2, c3 := content("first", n), content("second", n/2+1), content("third", n)
	prime("", "")
	v := create(t, w, "k", c1)
	expectFirstReads(t, readers, "after a Create by another client", "k", c1, v)
	prime(c1, v)
	v = replace(t, w, "k", v, c2)
	expectFirstReads(t, readers, "after a Replace by another client", "k", c2, v)
	prime(c2, v)
	v = replace(t, w, "k", v, c3)
	expectFirstReads(t, readers, "after a second Replace by another client", "k", c3, v)
	prime(c3, v)
	if err := w.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	expectFirstReads(t, readers, "after a Delete by another client", "k", "", "")
}

// expectFirstReads checks that key holds content at version, or is missing if
// version is "", with one read on each of four clients: Head, Get, a bounded
// GetRange and List, in that order of clients.
func expectFirstReads(t reporter, clients []storage.Store, when, key, content, version string) {
	t.Helper()
	want := storage.Info{Size: int64(len(content)), Version: version}
	info, err := clients[0].Head(ctx, key)
	switch {
	case version == "":
		expectError(t, err, "head", key, storage.ErrNotFound)
	case err != nil || info != want:
		t.Fatalf("%s, Head(%q) = %+v, %v; want %+v", when, key, info, err, want)
	}
	body, info, err := clients[1].Get(ctx, key)
	if version == "" {
		expectError(t, err, "get", key, storage.ErrNotFound)
	} else if err != nil {
		t.Fatalf("%s, Get(%q): %v", when, key, err)
	} else if got := readAll(t, body); got != content || info != want {
		t.Fatalf("%s, Get(%q) = %d bytes, %+v; want %d bytes, %+v", when, key, len(got), info, len(content), want)
	}
	off, length := len(content)/3, len(content)/3+1
	body, info, err = clients[2].GetRange(ctx, key, int64(off), int64(length))
	if version == "" {
		expectError(t, err, "get", key, storage.ErrNotFound)
	} else if err != nil {
		t.Fatalf("%s, GetRange(%q, %d, %d): %v", when, key, off, length, err)
	} else if got := readAll(t, body); got != content[off:min(off+length, len(content))] || info != want {
		t.Fatalf("%s, GetRange(%q, %d, %d) = %d bytes, %+v; want bytes [%d, %d) of the content, %+v", when, key, off, length, len(got), info, off, min(off+length, len(content)), want)
	}
	entries, err := clients[3].List(ctx, key, "", 1)
	if err != nil {
		t.Fatalf("%s, List(%q, \"\", 1): %v", when, key, err)
	}
	if version == "" {
		if len(entries) > 0 && entries[0].Key == key {
			t.Fatalf("%s, List(%q, \"\", 1) = %+v, which lists the deleted key", when, key, entries)
		}
	} else if wantEntry := (storage.Entry{Key: key, Size: want.Size}); len(entries) != 1 || entries[0] != wantEntry {
		t.Fatalf("%s, List(%q, \"\", 1) = %+v, want [%+v]", when, key, entries, wantEntry)
	}
}

func testRangeReads(t reporter, open func() storage.Store, n int) {
	s := open()
	c := content("range", n)
	v := create(t, s, "k", c)
	want := storage.Info{Size: int64(n), Version: v}
	size := int64(n)
	cases := [][2]int64{
		{0, size},
		{0, 1},
		{2, 3},
		{size - 1, 1},
		{0, 0},
		{size / 2, 0},
		{size - 2, 100},
		{0, math.MaxInt64},
		{size - 1, math.MaxInt64},
		{size, 0},
		{size, 5},
		{size + 1, 1},
		{1 << 40, 1},
		{math.MaxInt64, math.MaxInt64},
		{size / 3, size/3 + 1},
		{1<<20 + 7, 2 << 20},
	}
	for _, tc := range cases {
		offset, length := tc[0], tc[1]
		if offset < 0 {
			continue // a case near the end of an object too short for it
		}
		start := min(offset, size)
		end := start + min(length, size-start)
		body, info, err := s.GetRange(ctx, "k", offset, length)
		if err != nil {
			t.Fatalf("GetRange(%d, %d): %v", offset, length, err)
		}
		if got := readAll(t, body); got != c[start:end] {
			t.Errorf("GetRange(%d, %d) = %d bytes %.16q, want bytes [%d, %d) of the object", offset, length, len(got), got, start, end)
		}
		if info != want {
			t.Errorf("GetRange(%d, %d) info = %+v, want %+v", offset, length, info, want)
		}
	}
	for _, bad := range [][2]int64{{-1, 1}, {0, -1}, {-1, -1}} {
		_, _, err := s.GetRange(ctx, "k", bad[0], bad[1])
		expectError(t, err, "get", "k", storage.ErrInvalidArgument)
	}
	expectContent(t, s, "k", c, v)
}

// expectList is the reference listing: the keys with prefix after startAfter,
// sorted bytewise, at most limit.
func expectList(all []string, prefix, startAfter string, limit int) []string {
	var out []string
	for _, k := range all {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out[:min(len(out), limit)]
}

func testListOrder(t reporter, s storage.Store) {
	all := []string{
		"a", "a/b", "a.b", "a/b/c", "a0", "a/b0", "a/c", "ab", "b", "B", "a-", "a_",
		"A", "Z", "_", "-", "0", "9", "a/b_", "a/b-", "a/b.c", "z",
	}
	sizeOf := map[string]int64{}
	for i, k := range all {
		create(t, s, k, strings.Repeat("x", i))
		sizeOf[k] = int64(i)
	}
	cases := []struct {
		prefix, startAfter string
		limit              int
	}{
		{"", "", 1000},
		{"a", "", 1000},
		{"a/", "", 1000},
		{"a/b", "", 1000},
		{"a/b/", "", 1000},
		{"A", "", 1000},
		{"nothing", "", 1000},
		{"", "a/b", 1000},
		{"", "a/a", 1000},
		{"", "a", 3},
		{"a", "a/b", 2},
		{"", "z", 1000},
		{"", "_", 1000},
		{"a/", "zzz", 1000},
		// Selectors that are not valid keys.
		{"a//", "", 1000},
		{"", "/", 1000},
		{"a/b.", "", 1000},
		{"a/", "a/", 1000},
		{"", "a//", 1000},
		{"é", "", 1000},
		{"a\xff", "", 1000},
		{"", "a\xff", 1000},
		{"", "\xff", 1000},
		{strings.Repeat("a", storage.MaxKeyLen+1), "", 1000},
	}
	for _, tc := range cases {
		entries, err := s.List(ctx, tc.prefix, tc.startAfter, tc.limit)
		if err != nil {
			t.Fatalf("List(%q, %q, %d): %v", tc.prefix, tc.startAfter, tc.limit, err)
		}
		want := expectList(all, tc.prefix, tc.startAfter, tc.limit)
		if got := keys(entries); !slices.Equal(got, want) {
			t.Errorf("List(%.40q, %.40q, %d) = %q, want %q", tc.prefix, tc.startAfter, tc.limit, got, want)
		}
		for _, e := range entries {
			if e.Size != sizeOf[e.Key] {
				t.Errorf("List entry %q size = %d, want %d", e.Key, e.Size, sizeOf[e.Key])
			}
		}
	}
}

func testListPagination(t reporter, s storage.Store) {
	all := map[string]bool{}
	for i := range 25 {
		k := fmt.Sprintf("p/%02d", i)
		create(t, s, k, k)
		all[k] = true
	}
	create(t, s, "o", "outside the prefix")
	create(t, s, "q", "outside the prefix")
	const limit = 7
	var got []string
	after := ""
	for page := 0; ; page++ {
		if page > len(all)/limit+2 {
			t.Fatalf("the listing did not end after %d pages of %d; got %q", page, limit, got)
		}
		entries, err := s.List(ctx, "p/", after, limit)
		if err != nil {
			t.Fatal(err)
		}
		checkPage(t, entries, "p/", after, limit)
		got = append(got, keys(entries)...)
		if len(entries) < limit {
			break
		}
		after = entries[len(entries)-1].Key
		if page == 0 {
			// Writes between pages: behind the cursor (not seen), ahead of it
			// (seen), and a delete ahead of it (not seen).
			create(t, s, "p/03x", "behind")
			create(t, s, "p/10x", "ahead")
			all["p/10x"] = true
			if err := s.Delete(ctx, "p/12"); err != nil {
				t.Fatal(err)
			}
			delete(all, "p/12")
		}
	}
	want := make([]string, 0, len(all))
	for k := range all {
		want = append(want, k)
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("paged listing = %q, want %q", got, want)
	}
}

func testListLimits(t reporter, s storage.Store) {
	for _, k := range []string{"x/1", "x/2", "x/3"} {
		create(t, s, k, k)
	}
	for _, limit := range []int{0, -1, storage.MaxListLimit + 1} {
		_, err := s.List(ctx, "", "", limit)
		expectError(t, err, "list", "", storage.ErrInvalidArgument)
	}
	page, err := s.List(ctx, "x/", "", 3)
	if err != nil || len(page) != 3 {
		t.Fatalf("List limit 3 = %v, %v; want 3 entries", page, err)
	}
	page, err = s.List(ctx, "x/", page[2].Key, 3)
	if err != nil || len(page) != 0 {
		t.Fatalf("List after the last key = %v, %v; want no entries", page, err)
	}
	page, err = s.List(ctx, "x/", "", 1)
	if err != nil || len(page) != 1 || page[0].Key != "x/1" {
		t.Fatalf("List limit 1 = %v, %v; want [x/1]", page, err)
	}
}

// testListBeyondMaxLimit lists a prefix holding one object more than
// storage.MaxListLimit, and the whole namespace, so that a first page must be
// full and the rest must come on a second page. A backend that cuts listings
// short fails here; one that does it silently would cut recovery and garbage
// collection short too. It makes storage.MaxListLimit+4 Creates and four
// Lists.
func testListBeyondMaxLimit(t reporter, s storage.Store) {
	const n = storage.MaxListLimit + 1
	inside := make([]string, n)
	for i := range inside {
		inside[i] = fmt.Sprintf("b/%04d", i)
	}
	all := append([]string{"a", "b", "b0"}, inside...)
	createAll(t, s, all)
	slices.Sort(all)
	expectPage := func(prefix, startAfter string, want []string) {
		t.Helper()
		entries, err := s.List(ctx, prefix, startAfter, storage.MaxListLimit)
		if err != nil {
			t.Fatalf("List(%q, %q, %d): %v", prefix, startAfter, storage.MaxListLimit, err)
		}
		if got := keys(entries); !slices.Equal(got, want) {
			t.Fatalf("List(%q, %q, %d) = %d keys %s, want %d keys %s", prefix, startAfter, storage.MaxListLimit, len(got), describe(got), len(want), describe(want))
		}
		for _, e := range entries {
			if e.Size != 1 {
				t.Fatalf("List entry %q size = %d, want 1", e.Key, e.Size)
			}
		}
	}
	expectPage("b/", "", inside[:storage.MaxListLimit])
	expectPage("b/", inside[storage.MaxListLimit-1], inside[storage.MaxListLimit:])
	expectPage("", "", all[:storage.MaxListLimit])
	expectPage("", all[storage.MaxListLimit-1], all[storage.MaxListLimit:])
}

// testListDuringWrites lists a prefix, in one call and in pages, while
// another client creates and deletes keys between the listed ones. A listing
// is not a snapshot, but a key that exists unchanged for the whole call must
// appear exactly once, entries must be strictly ascending, and a key that
// comes and goes may appear only as it was while it existed. A backend that
// fills a page from several service calls goes wrong only when the keys
// before its position change between two of them, so the other client writes
// for as long as the listings run, and the listings run for at least 20
// rounds and until 240 of those writes have been made. It makes 60 Creates
// and at least 20 rounds of one full and about ten paged Lists; the number of
// Creates and Deletes beside them depends on how fast the backend writes.
func testListDuringWrites(t reporter, open func() storage.Store) {
	const stableKeys, minListRounds, minWrites, pageLimit = 60, 20, 4 * 60, 7
	w, r := open(), open()
	stable := make([]string, stableKeys)
	for i := range stable {
		stable[i] = fmt.Sprintf("s/%03d", 2*i)
	}
	createAll(t, w, stable)
	done := make(chan struct{})
	finished := make(chan struct{})
	stop := sync.OnceFunc(func() {
		close(done)
		select {
		case <-finished:
		case <-time.After(completionTimeout):
			t.Errorf("the writer did not return within %v of the end of the listings", completionTimeout)
		}
	})
	defer stop() // also when a check calls Fatal
	var churned atomic.Int64
	go func() {
		defer close(finished)
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			// One pass creates every odd key, the next deletes them, both in a
			// scattered order: a key lives for about stableKeys writes, long
			// enough to be listed, and the number of keys before any position
			// changes with every write.
			k := fmt.Sprintf("s/%03d", 2*(i*37%stableKeys)+1)
			if i/stableKeys%2 == 0 {
				if _, err := w.Create(ctx, k, strings.NewReader("yy")); err != nil {
					t.Errorf("Create(%q): %v", k, err)
					return
				}
			} else if err := w.Delete(ctx, k); err != nil {
				t.Errorf("Delete(%q): %v", k, err)
				return
			}
			churned.Add(1)
		}
	}()
	check := func(how string, entries []storage.Entry) {
		t.Helper()
		listed := 0
		for _, e := range entries {
			var i int
			if _, err := fmt.Sscanf(e.Key, "s/%03d", &i); err != nil || e.Key != fmt.Sprintf("s/%03d", i) {
				t.Fatalf("%s listed %q, which was never written", how, e.Key)
			}
			if want := int64(1 + i%2); e.Size != want {
				t.Fatalf("%s listed %q with size %d; it only ever had size %d", how, e.Key, e.Size, want)
			}
			if i%2 == 0 {
				listed++
			}
		}
		if listed != stableKeys {
			t.Fatalf("%s listed %d of the %d keys that existed, unchanged, throughout", how, listed, stableKeys)
		}
	}
	churning := func() bool {
		select {
		case <-finished:
			return false
		default:
			return true
		}
	}
	for round := 0; round < minListRounds || churned.Load() < minWrites && churning(); round++ {
		entries, err := r.List(ctx, "s/", "", storage.MaxListLimit)
		if err != nil {
			t.Fatal(err)
		}
		checkPage(t, entries, "s/", "", storage.MaxListLimit)
		check("one List call", entries)
		var paged []storage.Entry
		after := ""
		for page := 0; ; page++ {
			if page > 2*stableKeys/pageLimit+2 {
				t.Fatalf("the paged listing did not end after %d pages", page)
			}
			entries, err := r.List(ctx, "s/", after, pageLimit)
			if err != nil {
				t.Fatal(err)
			}
			checkPage(t, entries, "s/", after, pageLimit)
			paged = append(paged, entries...)
			if len(entries) < pageLimit {
				break
			}
			after = entries[len(entries)-1].Key
		}
		check("a paged listing", paged)
	}
	stop()
	if churned.Load() == 0 {
		t.Fatal("no key was created or deleted while the listings ran; the test proved nothing")
	}
}

// createAll creates every key with a one-byte body, eight at a time, because a
// thousand sequential round trips are slow against an emulator.
func createAll(t reporter, s storage.Store, keys []string) {
	t.Helper()
	work := make(chan string)
	errs := make(chan error, len(keys))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for k := range work {
				if _, err := s.Create(ctx, k, strings.NewReader("x")); err != nil {
					errs <- fmt.Errorf("Create(%q): %w", k, err)
				}
			}
		})
	}
	for _, k := range keys {
		work <- k
	}
	close(work)
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

// describe shortens a long key list for an error message.
func describe(keys []string) string {
	if len(keys) <= 6 {
		return fmt.Sprintf("%q", keys)
	}
	return fmt.Sprintf("%q ... %q", keys[:3], keys[len(keys)-3:])
}

// writers race in each of rounds rounds; several rounds make a race window
// that one round misses likely to be hit.
const writers, rounds = 16, 10

// overlapTimeout bounds every wait for a backend: for a write to reach a
// point in its body, and for a call that a write in progress must not block.
// The suite's own tests shorten it.
var overlapTimeout = 10 * time.Second

// completionTimeout bounds the wait for a call to return once nothing the
// test controls holds it any more: a write whose body has been released, a
// call racing it, and the goroutines a test leaves behind. It is long, as it
// may cover a whole large upload.
var completionTimeout = 2 * time.Minute

// await waits up to completionTimeout for done to be closed and fails the
// test if it is not.
func await(t reporter, done <-chan struct{}, what string) {
	t.Helper()
	timer := time.NewTimer(completionTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("%s did not return within %v of being released", what, completionTimeout)
	}
}

func openClients(open func() storage.Store, n int) []storage.Store {
	out := make([]storage.Store, n)
	for i := range out {
		out[i] = open()
	}
	return out
}

// payload is distinct per writer, unless n is 0.
func payload(i, n int) string { return content(fmt.Sprint("writer ", i), n) }

// gatedBody is a write body that reads from r but stops once it has returned
// at bytes: its next Read closes reached and waits for release. With at 0 it
// stops before its first byte, where a write has begun and cannot have
// published anything; with at > 0 it stops inside the body, where a write has
// read part of it and still must not have published anything.
type gatedBody struct {
	r       io.Reader
	at, n   int
	reached chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func newGatedBody(r io.Reader, at int, release <-chan struct{}) *gatedBody {
	return &gatedBody{r: r, at: at, reached: make(chan struct{}), release: release}
}

func (g *gatedBody) Read(p []byte) (int, error) {
	if g.n >= g.at {
		g.once.Do(func() { close(g.reached); <-g.release })
	} else if len(p) > g.at-g.n {
		p = p[:g.at-g.n]
	}
	n, err := g.r.Read(p)
	g.n += n
	return n, err
}

// background tracks the goroutines a test starts and the gates their writes
// may be stopped at. When the test ends, failing or not, it opens every gate
// and then waits up to completionTimeout for every goroutine, so that no write
// is left blocked and a backend that never returns fails the test instead of
// hanging it.
type background struct {
	gates []func()
	wg    sync.WaitGroup
}

func newBackground(t reporter) *background {
	b := &background{}
	t.Cleanup(func() {
		b.openAll()
		done := make(chan struct{})
		go func() { b.wg.Wait(); close(done) }()
		timer := time.NewTimer(completionTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Errorf("a call the test started did not return within %v of its body being released", completionTimeout)
		}
	})
	return b
}

// gate returns a new gate and the function that opens it.
func (b *background) gate() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	open := sync.OnceFunc(func() { close(ch) })
	b.gates = append(b.gates, open)
	return ch, open
}

func (b *background) openAll() {
	for _, open := range b.gates {
		open()
	}
}

// within runs fn on another goroutine and waits up to overlapTimeout for it,
// whether or not the backend honours a context. If fn has not returned by
// then, a write stopped in its body is blocking it: within opens every gate,
// waits up to completionTimeout for fn and fails the test.
func within(t reporter, bg *background, what string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	bg.wg.Go(func() { done <- fn() })
	timer := time.NewTimer(overlapTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		bg.openAll()
		var err error
		select {
		case err = <-done:
		case <-time.After(completionTimeout):
			err = fmt.Errorf("nothing within %v", completionTimeout)
		}
		t.Fatalf("%s did not return within %v while a write was reading its body (it returned %v once the write was released); a write must not block other mutations while it reads its body", what, overlapTimeout, err)
		return nil
	}
}

// pending is a write running in the background with a gated body.
type pending struct {
	body    *gatedBody // stops before its first byte
	release func()
	done    chan struct{}
	version string
	err     error
}

// start runs write in the background with a body that stops before the first
// byte of r, and waits until the write is stopped there. If the write returns
// before reading its body, start fails the test unless mayReturnEarly is set.
func start(t reporter, bg *background, mayReturnEarly bool, r io.Reader, write func(body io.Reader) (string, error)) *pending {
	t.Helper()
	ch, release := bg.gate()
	p := &pending{body: newGatedBody(r, 0, ch), release: release, done: make(chan struct{})}
	bg.wg.Go(func() {
		defer close(p.done)
		p.version, p.err = write(p.body)
	})
	timer := time.NewTimer(overlapTimeout)
	defer timer.Stop()
	select {
	case <-p.body.reached:
	case <-p.done:
		if !mayReturnEarly {
			t.Fatalf("write returned %v before reading its body", p.err)
		}
	case <-timer.C:
		t.Fatalf("write did not start reading its body within %v", overlapTimeout)
	}
	return p
}

// finish releases the body and waits up to completionTimeout for the write
// to return.
func (p *pending) finish(t reporter) (string, error) {
	t.Helper()
	p.release()
	await(t, p.done, "a write")
	return p.version, p.err
}

// race runs fn(i, body) for i in [0, writers) at once, each with payload(i,
// n), and returns the index of the only call that returned nil. Every other
// call must fail with the *storage.Error of op on key wrapping
// ErrPreconditionFailed. The bodies are gated: no write is released until
// every write is blocked reading its body, which proves that all of them were
// in progress at the same time, though not that they met inside the
// backend's critical window.
func race(t reporter, op, key string, n int, fn func(i int, body io.Reader) (string, error)) (winner int, version string) {
	t.Helper()
	bg := newBackground(t)
	release, open := bg.gate()
	versions := make([]string, writers)
	errs := make([]error, writers)
	bodies := make([]*gatedBody, writers)
	returned := make(chan int, writers)
	for i := range writers {
		bodies[i] = newGatedBody(strings.NewReader(payload(i, n)), 0, release)
		bg.wg.Go(func() {
			versions[i], errs[i] = fn(i, bodies[i])
			returned <- i
		})
	}
	timeout := time.After(overlapTimeout)
	for i := range writers {
		select {
		case <-bodies[i].reached:
		case j := <-returned:
			t.Fatalf("writer %d returned %v before every writer was reading its body", j, errs[j])
		case <-timeout:
			t.Fatalf("writer %d did not start reading its body within %v; writes must not block each other while reading their bodies", i, overlapTimeout)
		}
	}
	open()
	timer := time.NewTimer(completionTimeout)
	defer timer.Stop()
	for range writers {
		select {
		case <-returned:
		case <-timer.C:
			t.Fatalf("a writer did not return within %v of its body being released", completionTimeout)
		}
	}
	winner = -1
	for i, err := range errs {
		switch {
		case err == nil && winner >= 0:
			t.Fatalf("writers %d and %d both succeeded", winner, i)
		case err == nil:
			winner = i
		default:
			expectWriteError(t, versions[i], err, op, key, storage.ErrPreconditionFailed)
		}
	}
	if winner < 0 {
		t.Fatal("no writer succeeded")
	}
	return winner, versions[winner]
}

// testConcurrentCreate races Create calls for one absent key, each from its
// own client.
func testConcurrentCreate(t reporter, open func() storage.Store, n int) {
	clients := openClients(open, writers)
	for r := range rounds {
		key := fmt.Sprintf("k%d", r)
		winner, v := race(t, "create", key, n, func(i int, body io.Reader) (string, error) {
			return clients[i].Create(ctx, key, body)
		})
		expectContent(t, clients[(winner+1)%writers], key, payload(winner, n), v)
	}
}

// testConcurrentReplace races Replace calls with the same current version,
// each from its own client. Every round's token must be new for the key.
func testConcurrentReplace(t reporter, open func() storage.Store, n int) {
	clients := openClients(open, writers)
	v := create(t, clients[0], "k", content("initial", n))
	seen := []string{v}
	for range rounds {
		from := v
		var winner int
		winner, v = race(t, "replace", "k", n, func(i int, body io.Reader) (string, error) {
			return clients[i].Replace(ctx, "k", from, body)
		})
		if slices.Contains(seen, v) {
			t.Fatalf("Replace returned the version token %q, which the key had before", v)
		}
		seen = append(seen, v)
		expectContent(t, clients[(winner+1)%writers], "k", payload(winner, n), v)
	}
}

// deleteNow deletes key and fails the test if that does not return within
// overlapTimeout, as it would not if a write in progress blocked it.
func deleteNow(t reporter, bg *background, s storage.Store, key string) {
	t.Helper()
	if err := within(t, bg, "Delete", func() error { return s.Delete(ctx, key) }); err != nil {
		t.Fatalf("Delete(%q) while a write was reading its body: %v", key, err)
	}
}

// deleteDuring starts Delete on another goroutine and returns a channel that
// receives its result. It is a scheduling stress test, nothing more: the
// Delete is started just before the write it races is released, so the two
// usually overlap, but nothing places the Delete inside the write's critical
// window, and a round may well run them one after the other. wait waits up
// to completionTimeout for the Delete's result.
func deleteDuring(t reporter, bg *background, s storage.Store, key string) (wait func() error) {
	result := make(chan error, 1)
	bg.wg.Go(func() { result <- s.Delete(ctx, key) })
	return func() error {
		t.Helper()
		timer := time.NewTimer(completionTimeout)
		defer timer.Stop()
		select {
		case err := <-result:
			return err
		case <-timer.C:
			t.Fatalf("a Delete racing a write did not return within %v", completionTimeout)
			return nil
		}
	}
}

// testReplaceDeleteRace checks that a Replace cannot bring back an object
// that a Delete removed. First a Delete, through the Replace's own client and
// then through another, completes while a Replace conditioned on the deleted
// version is reading its body, so the Replace must fail; then Delete and
// Replace are run at about the same time, and either order must leave the key
// missing.
func testReplaceDeleteRace(t reporter, open func() storage.Store, n int) {
	a, b := open(), open()
	bg := newBackground(t)
	old := content("old", n)
	for _, deleter := range []storage.Store{a, b} {
		v := create(t, a, "k", old)
		p := start(t, bg, false, strings.NewReader(content("resurrected", n)), func(body io.Reader) (string, error) {
			return a.Replace(ctx, "k", v, body)
		})
		deleteNow(t, bg, deleter, "k")
		v, err := p.finish(t)
		expectWriteError(t, v, err, "replace", "k", storage.ErrPreconditionFailed)
		expectMissing(t, a, "k")
		expectMissing(t, b, "k")
	}

	for r := range rounds {
		key := fmt.Sprintf("r%d", r)
		v := create(t, a, key, old)
		p := start(t, bg, false, strings.NewReader(content("new", n)), func(body io.Reader) (string, error) {
			return a.Replace(ctx, key, v, body)
		})
		deleted := deleteDuring(t, bg, b, key)
		nv, err := p.finish(t)
		if derr := deleted(); derr != nil {
			t.Fatalf("Delete racing a Replace: %v", derr)
		}
		if err != nil {
			expectWriteError(t, nv, err, "replace", key, storage.ErrPreconditionFailed)
		}
		expectMissing(t, a, key)
	}
}

// testCreateDeleteRace runs Create against Delete. A Create of an absent key
// is not disturbed by a Delete of that missing key, through the Create's own
// client or another. When the key exists, a Create that succeeds must leave
// its object in place and one that fails must leave the key missing.
func testCreateDeleteRace(t reporter, open func() storage.Store, n int) {
	a, b := open(), open()
	bg := newBackground(t)
	created, old, nw := content("created", n), content("old", n), content("new", n)
	for i, deleter := range []storage.Store{a, b} {
		key := fmt.Sprintf("k%d", i)
		p := start(t, bg, false, strings.NewReader(created), func(body io.Reader) (string, error) {
			return a.Create(ctx, key, body)
		})
		deleteNow(t, bg, deleter, key)
		v, err := p.finish(t)
		if err != nil {
			t.Fatalf("Create of an absent key after a Delete of it: %v", err)
		}
		expectContent(t, b, key, created, v)
	}

	for r := range rounds {
		key := fmt.Sprintf("c%d", r)
		create(t, a, key, old)
		p := start(t, bg, true, strings.NewReader(nw), func(body io.Reader) (string, error) {
			return a.Create(ctx, key, body)
		})
		deleted := deleteDuring(t, bg, b, key)
		v, err := p.finish(t)
		if derr := deleted(); derr != nil {
			t.Fatalf("Delete racing a Create: %v", derr)
		}
		if err == nil {
			expectContent(t, b, key, nw, v)
			continue
		}
		expectWriteError(t, v, err, "create", key, storage.ErrPreconditionFailed)
		expectMissing(t, b, key)
	}
}

// testConcurrentReads replaces an object repeatedly, cycling through the body
// sizes, while readers on other clients, and one on the writer's client,
// read it with Get, an unbounded and a bounded GetRange, and Head and List.
// Every byte a read returns must belong to the version its Info names, that
// Info must be the one the write of that version returned, and that version
// must have been current at some moment while the call ran; the key must be
// listed exactly once, with a size it had while the List call ran. While the
// readers run, every Replace is held before the first byte of its body until
// a read that began while it was held has returned, so that reads overlap the
// Replaces whatever the scheduling, and an honest backend never fails for
// lack of overlap. A body may fail once a Replace of its version has
// begun, as the contract allows; a body of a version that no Replace has
// started on must not fail, and once the Replaces are over every read must
// succeed. Every Replace must return a new token.
func testConcurrentReads(t reporter, open func() storage.Store) {
	// idleReads and maxReads stop a reader whose reads fail at once from
	// spinning: while no Replace is in progress, a reader makes idleReads reads
	// and then waits for the next Replace to start, and it stops after
	// maxReads reads in all. Once every reader has stopped, the Replaces are no
	// longer held.
	const readers, replacements, idleReads, maxReads = 4, 40, 10, 2000
	all := sizes()
	sizeOf := func(n int64) int64 { return int64(all[n%int64(len(all))].n) }
	replacement := func(n int) string { return content(fmt.Sprint("replacement ", n), all[n%len(all)].n) }
	rangeOff, rangeLen := int64(3), int64(largeSize/2)
	// clip returns the part of version n that a read of length bytes from
	// offset returns.
	clip := func(n int, offset, length int64) (int64, int64) {
		size := sizeOf(int64(n))
		start := min(offset, size)
		return start, start + min(length, size-start)
	}
	wholeSums := make([][sha256.Size]byte, replacements+1)
	rangeSums := make([][sha256.Size]byte, replacements+1)
	for n := range wholeSums {
		c := replacement(n)
		start, end := clip(n, rangeOff, rangeLen)
		wholeSums[n], rangeSums[n] = sha256.Sum256([]byte(c)), sha256.Sum256([]byte(c[start:end]))
	}
	clients := openClients(open, readers)
	writer := clients[0]
	versions := make([]string, replacements+1) // versions[n] is the token the write of replacement(n) returned
	versions[0] = create(t, writer, "k", replacement(0))
	// started and completed count Replace calls. Version n can be current only
	// from when started reaches n until completed reaches n+1, so a call that
	// began when completed was c and returned when started was s can see only
	// versions c to s. held is the number of the Replace held before its body,
	// or 0, and overlapped receives the held value a read saw when it began.
	// begun[n] is closed when Replace n starts.
	var started, completed, held atomic.Int64
	overlapped := make(chan int64, 1)
	begun := make([]chan struct{}, replacements+2)
	for n := range begun {
		begun[n] = make(chan struct{})
	}
	hadSize := func(from, to, size int64) bool {
		for n := from; n <= to; n++ {
			if sizeOf(n) == size {
				return true
			}
		}
		return false
	}
	type seen struct {
		info             storage.Info
		from, to         int64 // completed when the call began, started when it returned
		head             bool  // Head, no body
		offset, length   int64 // the range asked for
		read             int64 // bytes the body returned
		sum              [sha256.Size]byte
		startedAtFailure int64 // started when the body failed, or -1
	}
	observed := make([][]seen, readers)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i, s := range clients {
		wg.Go(func() {
			idle := 0
			for j := 0; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				if c := completed.Load(); started.Load() == c {
					if idle == idleReads {
						select {
						case <-done:
							return
						case <-begun[c+1]:
						}
						idle = 0
					}
					idle++
				}
				during := held.Load()
				o := seen{from: completed.Load(), length: math.MaxInt64, startedAtFailure: -1}
				var body io.ReadCloser
				var err error
				switch j % 4 {
				case 0:
					body, o.info, err = s.Get(ctx, "k")
				case 1:
					body, o.info, err = s.GetRange(ctx, "k", 0, o.length)
				case 2:
					o.offset, o.length = rangeOff, rangeLen
					body, o.info, err = s.GetRange(ctx, "k", o.offset, o.length)
				case 3:
					o.head = true
					o.info, err = s.Head(ctx, "k")
					if err == nil {
						from := completed.Load()
						var entries []storage.Entry
						entries, err = s.List(ctx, "k", "", 2)
						to := started.Load()
						if err == nil && (len(entries) != 1 || entries[0].Key != "k" || !hadSize(from, to, entries[0].Size)) {
							t.Errorf("List(\"k\", \"\", 2) while k was replaced = %+v, want k once, with the size of one of replacements %d to %d, which were current while the call ran", entries, from, to)
							return
						}
					}
				}
				o.to = started.Load()
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				if body != nil {
					h := sha256.New()
					o.read, err = io.Copy(h, body)
					_ = body.Close()
					h.Sum(o.sum[:0])
					if err != nil {
						o.startedAtFailure = started.Load()
					}
				}
				observed[i] = append(observed[i], o)
				if during > 0 {
					select {
					case overlapped <- during:
					default:
					}
				}
				if len(observed[i]) == maxReads {
					return
				}
			}
		})
	}
	readersDone := make(chan struct{})
	go func() { wg.Wait(); close(readersDone) }()
	stop := sync.OnceFunc(func() { close(done) })
	defer func() { // also when the writer calls Fatal
		stop()
		select {
		case <-readersDone:
		case <-time.After(completionTimeout):
			t.Errorf("a reader did not return within %v of the end of the test", completionTimeout)
		}
	}()
	bg := newBackground(t)
	for n := 1; n <= replacements; n++ {
		started.Add(1)
		close(begun[n])
		p := start(t, bg, false, strings.NewReader(replacement(n)), func(body io.Reader) (string, error) {
			return writer.Replace(ctx, "k", versions[n-1], body)
		})
		held.Store(int64(n))
		timer := time.NewTimer(overlapTimeout)
		for waiting := true; waiting; {
			select {
			case m := <-overlapped:
				waiting = m != int64(n)
			case <-readersDone:
				waiting = false
			case <-timer.C:
				t.Fatalf("no read that began while Replace %d was reading its body returned within %v; reads must not wait for a write in progress", n, overlapTimeout)
			}
		}
		timer.Stop()
		held.Store(0)
		v, err := p.finish(t)
		if err != nil {
			t.Fatalf("Replace %d: %v", n, err)
		}
		if v == "" {
			t.Fatalf("Replace %d returned an empty version token", n)
		}
		versions[n] = v
		completed.Add(1)
	}
	stop()
	await(t, readersDone, "a reader")
	byVersion := make(map[string]int, len(versions))
	for n, v := range versions {
		if m, ok := byVersion[v]; ok {
			t.Fatalf("the writes of replacements %d and %d returned the same version token %q", m, n, v)
		}
		byVersion[v] = n
	}
	for _, obs := range observed {
		for _, o := range obs {
			n, ok := byVersion[o.info.Version]
			if !ok {
				t.Fatalf("read version %q, which no write returned", o.info.Version)
			}
			if int64(n) < o.from || int64(n) > o.to {
				t.Fatalf("a read that began once replacement %d was written and returned before replacement %d was started saw replacement %d (version %q), which was not current at any moment while it ran", o.from, o.to+1, n, o.info.Version)
			}
			if size := sizeOf(int64(n)); o.info.Size != size {
				t.Fatalf("read version %q of replacement %d with size %d, want %d", o.info.Version, n, o.info.Size, size)
			}
			if o.head {
				continue
			}
			start, end := clip(n, o.offset, o.length)
			want := rangeSums[n]
			if o.offset == 0 {
				want = wholeSums[n]
			}
			if o.startedAtFailure >= 0 {
				want = sha256.Sum256([]byte(replacement(n)[start:min(end, start+o.read)]))
			}
			switch {
			case o.read > end-start || o.sum != want:
				t.Fatalf("read %d bytes from offset %d with the Info of replacement %d (version %q): not bytes of that version", o.read, o.offset, n, o.info.Version)
			case o.startedAtFailure < 0 && o.read != end-start:
				t.Fatalf("read %d bytes from offset %d of replacement %d and no error, want %d", o.read, o.offset, n, end-start)
			case o.startedAtFailure >= 0 && o.startedAtFailure <= int64(n):
				t.Fatalf("a body of replacement %d failed before any Replace of that version had started", n)
			}
		}
	}
	for _, s := range clients {
		expectContent(t, s, "k", replacement(replacements), versions[replacements])
	}
}

// openedRead is how much of an open body testOpenBodies reads before it
// changes the object: past the first Read, short of the first chunk a
// backend would fetch separately (any chunk of 1 KiB or more).
const openedRead = 1021

// testOpenBodies opens Get and bounded GetRange bodies, reads the start of
// half of them, replaces or deletes the object from another client, and only
// then reads the rest. Each must return bytes of the version its Info
// describes, or fail.
func testOpenBodies(t reporter, open func() storage.Store, n int) {
	old, repl := content("old", n), content("new", n)
	off, length := n/5, n/2+1
	s, other := open(), open()
	for _, change := range []string{"Replace", "Delete"} {
		subtest(t, change, func(t reporter) {
			key := strings.ToLower(change)
			v := create(t, s, key, old)
			want := storage.Info{Size: int64(n), Version: v}
			type opened struct {
				name    string
				body    io.ReadCloser
				info    storage.Info
				start   []byte
				content string
			}
			var bodies []*opened
			defer func() {
				for _, b := range bodies {
					_ = b.body.Close()
				}
			}()
			for _, readFirst := range []bool{false, true} {
				for _, name := range []string{"Get", "GetRange"} {
					b := &opened{name: name, content: old}
					var err error
					if name == "Get" {
						b.body, b.info, err = s.Get(ctx, key)
					} else {
						b.body, b.info, err = s.GetRange(ctx, key, int64(off), int64(length))
						b.content = old[off:min(off+length, n)]
					}
					if err != nil {
						t.Fatal(err)
					}
					bodies = append(bodies, b)
					if readFirst {
						b.name += fmt.Sprintf(" read %d bytes before the change", openedRead)
						b.start = readStart(t, b.body)
					}
				}
			}
			if change == "Replace" {
				replace(t, other, key, v, repl)
			} else if err := other.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			for _, b := range bodies {
				expectSnapshot(t, b.name, b.body, b.info, b.start, b.content, want)
			}
		})
	}
}

// readStart reads up to openedRead bytes of body.
func readStart(t reporter, body io.Reader) []byte {
	t.Helper()
	b := make([]byte, openedRead)
	n, err := io.ReadFull(body, b)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("reading the start of a body: %v", err)
	}
	return b[:n]
}

func expectSnapshot(t reporter, name string, body io.Reader, info storage.Info, start []byte, content string, want storage.Info) {
	t.Helper()
	if info != want {
		t.Errorf("%s info = %+v, want %+v", name, info, want)
	}
	rest, err := io.ReadAll(body)
	b := append(start, rest...)
	if !strings.HasPrefix(content, string(b)) {
		t.Errorf("%s body opened before the change returned %d bytes, not all of them from the version it was opened at", name, len(b))
	}
	if err == nil && len(b) != len(content) {
		t.Errorf("%s body returned %d bytes and no error, want %d", name, len(b), len(content))
	}
}

var errReader = errors.New("storagetest: reader failed")

// readerError returns the error that the caller's body reader returned, if err
// holds one in a *storage.BodyError, and nil otherwise.
func readerError(err error) error {
	var be *storage.BodyError
	if errors.As(err, &be) {
		return be.Err
	}
	return nil
}

// testWriteInProgress stops a Create or Replace twice inside its body: before
// the first byte, and after a prefix. The prefix is either largeSize bytes,
// past every chunk a backend may upload separately, or a short read: a first
// Read that returns five bytes and no error, which a backend must not take
// for the whole body. At each stop it checks with Get, Head and List, from the
// writer's client and from another one, that nothing of the write is visible,
// and requires mutations through the writer's own client, of an unrelated key
// and of the key itself, to complete. Then it lets the write succeed (the body
// ends with io.EOF), fail by a reader error or fail by cancellation, each from
// the second stop, and checks the outcome and that a failed write does not
// block the next one.
func testWriteInProgress(t reporter, open func() storage.Store) {
	w, r := open(), open()
	prefixes := []struct{ name, prefix string }{
		{"", content("prefix", largeSize)},
		{" after a short read", "short"},
	}
	for _, op := range []string{"Create", "Replace"} {
		for _, stop := range prefixes {
			for _, outcome := range []string{"succeeds", "fails", "cancelled"} {
				subtest(t, op+" "+outcome+stop.name, func(t reporter) {
					bg := newBackground(t)
					key := strings.ReplaceAll(strings.ToLower(op+"-"+outcome+stop.name), " ", "-")
					prefix := stop.prefix
					var old string
					if op == "Replace" {
						old = create(t, w, key, "old")
					}
					before := listAll(t, r, "")
					unchanged := func(when string) {
						t.Helper()
						for _, s := range []storage.Store{w, r} {
							if op == "Create" {
								expectMissing(t, s, key)
							} else {
								expectContent(t, s, key, "old", old)
							}
							if got := listAll(t, s, ""); !slices.Equal(got, before) {
								t.Fatalf("%s: List = %+v, want %+v", when, got, before)
							}
						}
					}
					cctx, cancel := context.WithCancel(ctx)
					defer cancel()
					var rest io.Reader
					switch outcome {
					case "succeeds":
						rest = strings.NewReader("new")
					case "fails":
						rest = iotest.ErrReader(errReader)
					case "cancelled": // the body keeps returning data, so only the store can stop the write
						rest = io.LimitReader(readerFunc(func(p []byte) (int, error) { clear(p); return len(p), nil }), 64<<20)
					}
					inner, openInner := bg.gate()
					body := newGatedBody(io.MultiReader(strings.NewReader(prefix), rest), len(prefix), inner)
					p := start(t, bg, false, body, func(body io.Reader) (string, error) {
						if op == "Create" {
							return w.Create(cctx, key, body)
						}
						return w.Replace(cctx, key, old, body)
					})
					unchanged("while the write is stopped before its body")
					mutateDuring(t, bg, w, key, key+"-other1", op == "Replace")
					p.release()
					timer := time.NewTimer(overlapTimeout)
					defer timer.Stop()
					select {
					case <-body.reached:
					case <-p.done:
						t.Fatalf("%s returned %v before reading its whole body", op, p.err)
					case <-timer.C:
						t.Fatalf("%s did not read the first %d bytes of its body within %v", op, len(prefix), overlapTimeout)
					}
					unchanged("while the write is stopped inside its body")
					mutateDuring(t, bg, w, key, key+"-other2", op == "Replace")
					if outcome == "cancelled" {
						cancel()
						unchanged("after the write's context was cancelled")
					}
					openInner()
					await(t, p.done, op)
					v, err := p.version, p.err
					switch outcome {
					case "succeeds":
						if err != nil {
							t.Fatalf("%s: %v", op, err)
						}
						expectContent(t, r, key, prefix+"new", v)
						return
					case "fails":
						expectBodyError(t, v, err, strings.ToLower(op), key, errReader)
					case "cancelled":
						expectWriteError(t, v, err, strings.ToLower(op), key, context.Canceled)
					}
					if errors.Is(err, storage.ErrOutcomeUnknown) {
						t.Fatalf("%s failed before publication, but its error %v wraps ErrOutcomeUnknown", op, err)
					}
					unchanged("after the write failed")
					if op == "Create" {
						v = create(t, w, key, "after")
					} else {
						v = replace(t, w, key, old, "after")
					}
					expectContent(t, r, key, "after", v)
				})
			}
		}
	}
}

// mutateDuring runs mutations through s, a client whose write is stopped in
// its body: Create, Replace and Delete of an unrelated key, and a mutation of
// the write's own key that cannot change it (Create when it exists, Delete
// when it does not). Each must return, with the right result, within
// overlapTimeout. The unrelated key is gone again afterwards.
func mutateDuring(t reporter, bg *background, s storage.Store, key, unrelated string, keyExists bool) {
	t.Helper()
	var v string
	err := within(t, bg, "Create of an unrelated key on the writing client", func() (err error) {
		v, err = s.Create(ctx, unrelated, strings.NewReader("unrelated"))
		return err
	})
	if err != nil {
		t.Fatalf("Create(%q) on a client whose write is reading its body: %v", unrelated, err)
	}
	err = within(t, bg, "Replace of an unrelated key on the writing client", func() error {
		_, err := s.Replace(ctx, unrelated, v, strings.NewReader("unrelated, replaced"))
		return err
	})
	if err != nil {
		t.Fatalf("Replace(%q) on a client whose write is reading its body: %v", unrelated, err)
	}
	if err := within(t, bg, "Delete of an unrelated key on the writing client", func() error { return s.Delete(ctx, unrelated) }); err != nil {
		t.Fatalf("Delete(%q) on a client whose write is reading its body: %v", unrelated, err)
	}
	if keyExists {
		var v string
		err := within(t, bg, "Create of the key being replaced, on the writing client", func() (err error) {
			v, err = s.Create(ctx, key, strings.NewReader("x"))
			return err
		})
		expectWriteError(t, v, err, "create", key, storage.ErrPreconditionFailed)
	} else if err := within(t, bg, "Delete of the key being created, on the writing client", func() error { return s.Delete(ctx, key) }); err != nil {
		t.Fatalf("Delete(%q) of the key being created, on the same client: %v", key, err)
	}
}

// testEndOfBody ends a Create or Replace exactly at the end of its body, in
// two ways that leave the backend no moment between the body's end and its
// commit: the body cancels the write's context as it returns io.EOF, or its
// last Read returns the last bytes together with an error. An error that does
// not wrap ErrOutcomeUnknown must leave the key exactly as it was, content and
// version, or absent; the new content may be there only after success or an
// error wrapping ErrOutcomeUnknown. A body whose reader failed was never read
// to its end, so that write must fail with the reader's error and no effect.
func testEndOfBody(t reporter, open func() storage.Store, n int) {
	s, other := open(), open()
	data := content("new content", n)
	for _, op := range []string{"Create", "Replace"} {
		for _, end := range []string{"cancel at EOF", "error with the last bytes"} {
			subtest(t, op+" "+end, func(t reporter) {
				key := strings.ReplaceAll(strings.ToLower(op+"-"+end), " ", "-")
				var old string
				if op == "Replace" {
					old = create(t, s, key, "old")
				}
				cctx, cancel := context.WithCancel(ctx)
				defer cancel()
				src := strings.NewReader(data)
				body := readerFunc(func(p []byte) (int, error) {
					m, _ := src.Read(p)
					if src.Len() > 0 {
						return m, nil
					}
					if end == "cancel at EOF" {
						cancel()
						return m, io.EOF
					}
					return m, errReader
				})
				var v string
				var err error
				if op == "Create" {
					v, err = s.Create(cctx, key, body)
				} else {
					v, err = s.Replace(cctx, key, old, body)
				}
				unknown := errors.Is(err, storage.ErrOutcomeUnknown)
				switch {
				case end == "error with the last bytes":
					expectBodyError(t, v, err, strings.ToLower(op), key, errReader)
				case err != nil:
					expectWriteError(t, v, err, strings.ToLower(op), key, context.Canceled)
				}
				newEntry := []storage.Entry{{Key: key, Size: int64(n)}}
				var oldEntries []storage.Entry
				if op == "Replace" {
					oldEntries = []storage.Entry{{Key: key, Size: 3}}
				}
				switch {
				case err == nil:
					expectContent(t, other, key, data, v)
				case unknown:
					body, info, gerr := other.Get(ctx, key)
					got := "absent"
					if gerr == nil {
						got = readAll(t, body)
					} else if !errors.Is(gerr, storage.ErrNotFound) {
						t.Fatalf("Get(%q): %v", key, gerr)
					}
					isOld := op == "Create" && got == "absent" || op == "Replace" && got == "old" && info.Version == old
					if got != data && !isOld {
						t.Fatalf("after %s failed with %v the key holds %d bytes at %q: neither the old state nor the new content", op, err, len(got), info.Version)
					}
				default:
					if op == "Create" {
						expectMissing(t, other, key)
						expectMissing(t, s, key)
					} else {
						expectContent(t, other, key, "old", old)
						expectContent(t, s, key, "old", old)
					}
					if got := listAll(t, other, key); !slices.Equal(got, oldEntries) {
						t.Fatalf("after %s failed with %v, which does not wrap ErrOutcomeUnknown, List = %+v, want %+v", op, err, got, oldEntries)
					}
					return
				}
				if got := listAll(t, other, key); err == nil && !slices.Equal(got, newEntry) {
					t.Fatalf("after %s succeeded List = %+v, want %+v", op, got, newEntry)
				}
			})
		}
	}
}

// testCancelledContext calls every method with a context that is cancelled
// and with one whose deadline has passed, and requires each call to fail
// with the context's error and no effect.
func testCancelledContext(t reporter, s storage.Store) {
	v := create(t, s, "k", "content")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancelExpired := context.WithDeadline(ctx, time.Now().Add(-time.Hour))
	defer cancelExpired()
	for _, cctx := range []context.Context{cancelled, expired} {
		ops := []struct {
			op, key string
			call    func() (string, error)
		}{
			{"create", "new", func() (string, error) { return s.Create(cctx, "new", strings.NewReader("x")) }},
			{"replace", "k", func() (string, error) { return s.Replace(cctx, "k", v, strings.NewReader("x")) }},
			{"get", "k", func() (string, error) { _, _, err := s.Get(cctx, "k"); return "", err }},
			{"get", "k", func() (string, error) { _, _, err := s.GetRange(cctx, "k", 0, 1); return "", err }},
			{"head", "k", func() (string, error) { _, err := s.Head(cctx, "k"); return "", err }},
			{"list", "", func() (string, error) { _, err := s.List(cctx, "", "", 10); return "", err }},
			{"delete", "k", func() (string, error) { return "", s.Delete(cctx, "k") }},
		}
		for _, o := range ops {
			version, err := o.call()
			expectWriteError(t, version, err, o.op, o.key, cctx.Err())
			if errors.Is(err, storage.ErrOutcomeUnknown) {
				t.Errorf("%s with a context that had ended error = %v, want no ErrOutcomeUnknown", o.op, err)
			}
		}
	}
	expectContent(t, s, "k", "content", v)
	expectMissing(t, s, "new")
}

// testBodyHonoursContext opens a Get body and a bounded GetRange body of a
// large object, cancels their context and requires reading them to fail with
// the context's error.
func testBodyHonoursContext(t reporter, s storage.Store) {
	data := content("big", largeSize)
	v := create(t, s, "k", data)
	expectContent(t, s, "k", data, v)
	for _, name := range []string{"Get", "GetRange"} {
		cctx, cancel := context.WithCancel(ctx)
		var body io.ReadCloser
		var err error
		if name == "Get" {
			body, _, err = s.Get(cctx, "k")
		} else {
			body, _, err = s.GetRange(cctx, "k", 1, int64(largeSize/2))
		}
		if err != nil {
			cancel()
			t.Fatalf("%s: %v", name, err)
		}
		cancel()
		_, err = io.Copy(io.Discard, body)
		_ = body.Close()
		if !errors.Is(err, cctx.Err()) {
			t.Fatalf("reading a %s body after its context was cancelled returned %v, want an error wrapping %v", name, err, cctx.Err())
		}
	}
}

func testErrors(t reporter, s storage.Store) {
	const secret1, secret2 = "secret-content-one", "secret-content-two"
	v := create(t, s, "dir/k", secret1)
	createVersion, createErr := s.Create(ctx, "dir/k", strings.NewReader(secret2))
	replaceVersion, replaceErr := s.Replace(ctx, "dir/k", v+"stale", strings.NewReader(secret2))
	noToken(t, createVersion, createErr, "create", "dir/k")
	noToken(t, replaceVersion, replaceErr, "replace", "dir/k")
	_, _, getErr := s.Get(ctx, "dir/missing")
	_, _, rangeErr := s.GetRange(ctx, "dir/k", -1, 1)
	_, headErr := s.Head(ctx, "dir/missing")
	_, listErr := s.List(ctx, "", "", 0)
	deleteErr := s.Delete(ctx, "dir/../k")
	cases := []struct {
		err     error
		op, key string
		is      error
	}{
		{createErr, "create", "dir/k", storage.ErrPreconditionFailed},
		{replaceErr, "replace", "dir/k", storage.ErrPreconditionFailed},
		{getErr, "get", "dir/missing", storage.ErrNotFound},
		{rangeErr, "get", "dir/k", storage.ErrInvalidArgument},
		{headErr, "head", "dir/missing", storage.ErrNotFound},
		{listErr, "list", "", storage.ErrInvalidArgument},
		{deleteErr, "delete", "dir/../k", storage.ErrInvalidKey},
	}
	for _, tc := range cases {
		expectError(t, tc.err, tc.op, tc.key, tc.is)
		msg := tc.err.Error()
		if !strings.Contains(msg, tc.key) {
			t.Errorf("%s error %q does not name the key %q", tc.op, msg, tc.key)
		}
		if strings.Contains(msg, "secret-content") {
			t.Errorf("%s error %q contains object contents", tc.op, msg)
		}
	}
}

// testBodyErrorText fails writes with body readers that return a secret
// payload and then fail, with an error whose text is a secret or with each
// storage sentinel and each context error. The reader's error must be held in
// a *storage.BodyError, neither the payload nor the reader's error text may
// reach the error message, and errors.Is on the write's error must not match
// the reader's error: a reader that returns ErrOutcomeUnknown or
// ErrPreconditionFailed has not made the write's outcome unknown or its
// condition fail.
func testBodyErrorText(t reporter, s storage.Store) {
	const secretPayload, secretError = "secret-payload-7d1e95", "secret-error-9f2c41" //nolint:gosec // test data, not a credential
	v := create(t, s, "k", "x")
	for _, readerErr := range append([]error{errors.New(secretError)}, sentinels...) {
		body := func() io.Reader {
			return io.MultiReader(strings.NewReader(secretPayload), iotest.ErrReader(readerErr))
		}
		createVersion, createErr := s.Create(ctx, "new", body())
		replaceVersion, replaceErr := s.Replace(ctx, "k", v, body())
		for _, c := range []struct {
			op, key, version string
			err              error
		}{{"create", "new", createVersion, createErr}, {"replace", "k", replaceVersion, replaceErr}} {
			expectBodyError(t, c.version, c.err, c.op, c.key, readerErr)
			if msg := c.err.Error(); strings.Contains(msg, "secret-") {
				t.Errorf("%s error %q contains the body or the reader's error text", c.op, msg)
			}
		}
	}
	expectContent(t, s, "k", "x", v)
	expectMissing(t, s, "new")
}
