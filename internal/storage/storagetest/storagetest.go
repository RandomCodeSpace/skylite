// Package storagetest is the portable part of the conformance suite for
// storage.Store backends.
//
// A backend's tests call [Run] with a function that creates a new, empty
// namespace and returns a way to open independent clients onto it. Every
// subtest gets its own namespace.
//
// The suite covers what can be checked through the interface from one
// process: key validation, conditional writes and version tokens, range
// reads, listing order and pagination (including a listing longer than
// storage.MaxListLimit), failed and cancelled writes (including a context
// cancelled, or a reader failing, at the very end of the body), what readers
// and other writers on the same and on other clients see while a write is
// stopped before and inside its body, open bodies across a Replace or Delete,
// error contents, reader errors that are storage sentinels, and races between
// independent clients: Create against Create, Replace against Replace,
// Replace against Delete and Create against Delete.
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
// responses and SDK retries and their classification, short pages returned
// by the service, an interrupted staged upload, a size limit, or redaction of
// a service SDK's errors. Each backend tests those itself, as the storage
// package documentation requires. Passing [Run] is necessary for a backend to
// be accepted, not sufficient.
//
// A full run makes about 2,300 storage requests and writes about 40 MiB,
// most of it in ListBeyondMaxLimit (1,004 one-byte Creates, made eight at a
// time), ConcurrentCreate, ConcurrentReplace and WriteInProgress. On top of
// that ConcurrentReadsSeeWholeVersions makes 41 writes while four readers
// read as fast as the backend lets them, at most 2,000 reads each (on
// localfs about 1,000 reads in all, about 100 MiB). Every wait for a backend
// is bounded by a timer, not by the backend honouring its context.
//
// The suite tests itself: selftest_test.go runs it against a catalogue of
// wrong backends, each a thin wrapper around package localfs that breaks one
// rule, and requires every one of them to be rejected by the subtest it
// names, and against correct but slow or fragile backends that must pass.
package storagetest

import (
	"bytes"
	"context"
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

// Run runs the conformance suite. newNamespace is called once per subtest. It
// creates a new, empty namespace (a directory, or a container and prefix) and
// returns open, which opens a new client onto that namespace each time it is
// called. Clients must be independent: they may share nothing in the process
// that the backend's correctness depends on, so that two clients behave like
// two processes or hosts. Register cleanup with the t given to newNamespace.
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

// tests lists the suite's subtests in the order they run.
var tests = []struct {
	name string
	fn   func(t reporter, open func() storage.Store)
}{
	{"InvalidKeys", one(testInvalidKeys)},
	{"ValidKeys", one(testValidKeys)},
	{"NotFound", one(testNotFound)},
	{"CreateExisting", one(testCreateExisting)},
	{"Replace", one(testReplace)},
	{"VersionTokens", one(testVersionTokens)},
	{"Delete", one(testDelete)},
	{"RangeReads", one(testRangeReads)},
	{"ZeroLength", one(testZeroLength)},
	{"LargeObject", one(testLargeObject)},
	{"ListOrder", one(testListOrder)},
	{"ListPagination", one(testListPagination)},
	{"ListLimits", one(testListLimits)},
	{"ListBeyondMaxLimit", one(testListBeyondMaxLimit)},
	{"ConcurrentCreate", testConcurrentCreate},
	{"ConcurrentReplace", testConcurrentReplace},
	{"ReplaceDeleteRace", testReplaceDeleteRace},
	{"CreateDeleteRace", testCreateDeleteRace},
	{"ConcurrentReadsSeeWholeVersions", testConcurrentReads},
	{"OpenBodyKeepsItsVersion", testOpenBodies},
	{"WriteInProgress", testWriteInProgress},
	{"EndOfBody", testEndOfBody},
	{"CancelledContext", one(testCancelledContext)},
	{"BodyHonoursContext", one(testBodyHonoursContext)},
	{"Errors", one(testErrors)},
	{"BodyErrorText", one(testBodyErrorText)},
}

func one(fn func(reporter, storage.Store)) func(reporter, func() storage.Store) {
	return func(t reporter, open func() storage.Store) { fn(t, open()) }
}

func run(t reporter, newNamespace func(t reporter) (open func() storage.Store)) {
	for _, tc := range tests {
		subtest(t, tc.name, func(t reporter) { tc.fn(t, newNamespace(t)) })
	}
}

var ctx = context.Background()

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

// expectContent checks that key holds content at version (any version if "").
func expectContent(t reporter, s storage.Store, key, content, version string) {
	t.Helper()
	got, info := get(t, s, key)
	if got != content {
		t.Fatalf("Get(%q) = %d bytes %.40q, want %d bytes %.40q", key, len(got), got, len(content), content)
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("Get(%q) size = %d, want %d", key, info.Size, len(content))
	}
	if version != "" && info.Version != version {
		t.Fatalf("Get(%q) version = %q, want %q", key, info.Version, version)
	}
	head, err := s.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head(%q): %v", key, err)
	}
	if head != info {
		t.Fatalf("Head(%q) = %+v, Get returned %+v", key, head, info)
	}
}

func expectMissing(t reporter, s storage.Store, key string) {
	t.Helper()
	if _, err := s.Head(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Head(%q) error = %v, want ErrNotFound", key, err)
	}
	if _, _, err := s.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get(%q) error = %v, want ErrNotFound", key, err)
	}
}

func listAll(t reporter, s storage.Store, prefix string) []storage.Entry {
	t.Helper()
	entries, err := s.List(ctx, prefix, "", storage.MaxListLimit)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
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
	{"non-ASCII", "\u00e9"},
	{"private use", "a\ue000"},
	{"invalid UTF-8", "a\xffb"},
	{"too long", strings.Repeat("a", storage.MaxKeyLen+1)},
	{"too many segments", strings.Repeat("a/", storage.MaxKeySegments) + "a"},
}

// readerFunc is an io.Reader made of a function.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// testInvalidKeys checks that every method rejects every invalid key before
// it does anything else: before it reads the body and before it looks at the
// context.
func testInvalidKeys(t reporter, s storage.Store) {
	v := create(t, s, "valid", "x")
	body := readerFunc(func([]byte) (int, error) {
		t.Error("a write with an invalid key read its body")
		return 0, io.EOF
	})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, tc := range invalidKeys {
		subtest(t, tc.name, func(t reporter) {
			if err := storage.ValidateKey(tc.key); !errors.Is(err, storage.ErrInvalidKey) {
				t.Errorf("ValidateKey error = %v, want ErrInvalidKey", err)
			}
			for _, c := range []context.Context{ctx, cancelled} {
				ops := map[string]func() error{
					"Create":   func() error { _, err := s.Create(c, tc.key, body); return err },
					"Replace":  func() error { _, err := s.Replace(c, tc.key, v, body); return err },
					"Get":      func() error { _, _, err := s.Get(c, tc.key); return err },
					"GetRange": func() error { _, _, err := s.GetRange(c, tc.key, 0, 1); return err },
					"Head":     func() error { _, err := s.Head(c, tc.key); return err },
					"Delete":   func() error { return s.Delete(c, tc.key) },
				}
				for name, op := range ops {
					if err := op(); !errors.Is(err, storage.ErrInvalidKey) || errors.Is(err, context.Canceled) {
						t.Errorf("%s error = %v, want ErrInvalidKey (context done: %v)", name, err, c.Err() != nil)
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
	if _, _, err := s.GetRange(ctx, "missing", 0, 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetRange error = %v, want ErrNotFound", err)
	}
	if _, _, err := s.GetRange(ctx, "missing", 0, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("zero-length GetRange error = %v, want ErrNotFound", err)
	}
	if _, err := s.Replace(ctx, "missing", "", strings.NewReader("x")); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("Replace of a missing key error = %v, want ErrPreconditionFailed", err)
	}
	expectMissing(t, s, "missing")
}

func testCreateExisting(t reporter, s storage.Store) {
	v := create(t, s, "k", "first")
	if _, err := s.Create(ctx, "k", strings.NewReader("second")); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("second Create error = %v, want ErrPreconditionFailed", err)
	}
	expectContent(t, s, "k", "first", v)
}

func testReplace(t reporter, s storage.Store) {
	v1 := create(t, s, "k", "one")
	v2 := replace(t, s, "k", v1, "two!")
	if v2 == v1 {
		t.Fatalf("Replace returned the old version token %q", v1)
	}
	expectContent(t, s, "k", "two!", v2)
	if got := listAll(t, s, "k"); !slices.Equal(got, []storage.Entry{{Key: "k", Size: 4}}) {
		t.Fatalf("List after Replace = %+v, want k with the new size 4", got)
	}
	for _, stale := range []string{v1, "", "not-a-version", v2 + "x"} {
		if _, err := s.Replace(ctx, "k", stale, strings.NewReader("three")); !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("Replace with version %q error = %v, want ErrPreconditionFailed", stale, err)
		}
	}
	expectContent(t, s, "k", "two!", v2)
	if _, err := s.Replace(ctx, "missing", v2, strings.NewReader("x")); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("Replace of a missing key error = %v, want ErrPreconditionFailed", err)
	}
	expectMissing(t, s, "missing")
	v3 := replace(t, s, "k", v2, "")
	expectContent(t, s, "k", "", v3)
}

func testVersionTokens(t reporter, s storage.Store) {
	seen := map[string]bool{}
	record := func(v string) {
		t.Helper()
		if seen[v] {
			t.Fatalf("version token %q was returned twice for one key", v)
		}
		seen[v] = true
	}
	v := create(t, s, "k", "same")
	record(v)
	for range 3 {
		v = replace(t, s, "k", v, "same")
		record(v)
		expectContent(t, s, "k", "same", v)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	v = create(t, s, "k", "same")
	record(v)
	expectContent(t, s, "k", "same", v)
	for old := range seen {
		if old == v {
			continue
		}
		if _, err := s.Replace(ctx, "k", old, strings.NewReader("same")); !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("Replace with an earlier version token error = %v, want ErrPreconditionFailed", err)
		}
	}
	// Tokens of different keys are not required to differ, only to be stable.
	w := create(t, s, "other", "same")
	expectContent(t, s, "other", "same", w)
}

func testDelete(t reporter, s storage.Store) {
	create(t, s, "k", "x")
	create(t, s, "k2", "y")
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	expectMissing(t, s, "k")
	if got := keys(listAll(t, s, "")); !slices.Equal(got, []string{"k2"}) {
		t.Fatalf("List after Delete = %q, want [k2]", got)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if err := s.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("Delete of a missing key: %v", err)
	}
	v := create(t, s, "k", "again")
	expectContent(t, s, "k", "again", v)
}

func testRangeReads(t reporter, s storage.Store) {
	const content = "0123456789"
	v := create(t, s, "k", content)
	cases := []struct {
		offset, length int64
		want           string
	}{
		{0, 10, content},
		{0, 1, "0"},
		{2, 3, "234"},
		{9, 1, "9"},
		{0, 0, ""},
		{5, 0, ""},
		{8, 100, "89"},
		{0, 1<<63 - 1, content},
		{9, 1<<63 - 1, "9"},
		{10, 0, ""},
		{10, 5, ""},
		{11, 1, ""},
		{1 << 40, 1, ""},
		{1<<63 - 1, 1<<63 - 1, ""},
	}
	for _, tc := range cases {
		body, info, err := s.GetRange(ctx, "k", tc.offset, tc.length)
		if err != nil {
			t.Fatalf("GetRange(%d, %d): %v", tc.offset, tc.length, err)
		}
		if got := readAll(t, body); got != tc.want {
			t.Errorf("GetRange(%d, %d) = %q, want %q", tc.offset, tc.length, got, tc.want)
		}
		if info.Size != int64(len(content)) || info.Version != v {
			t.Errorf("GetRange(%d, %d) info = %+v, want size %d version %q", tc.offset, tc.length, info, len(content), v)
		}
	}
	for _, bad := range [][2]int64{{-1, 1}, {0, -1}, {-1, -1}} {
		if _, _, err := s.GetRange(ctx, "k", bad[0], bad[1]); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Errorf("GetRange(%d, %d) error = %v, want ErrInvalidArgument", bad[0], bad[1], err)
		}
	}
}

func testZeroLength(t reporter, s storage.Store) {
	v := create(t, s, "empty", "")
	expectContent(t, s, "empty", "", v)
	for _, r := range [][2]int64{{0, 0}, {0, 10}, {1, 1}} {
		body, info, err := s.GetRange(ctx, "empty", r[0], r[1])
		if err != nil {
			t.Fatalf("GetRange(%d, %d): %v", r[0], r[1], err)
		}
		if got := readAll(t, body); got != "" || info.Size != 0 || info.Version != v {
			t.Fatalf("GetRange(%d, %d) = %q, %+v; want empty, size 0, version %q", r[0], r[1], got, info, v)
		}
	}
	entries := listAll(t, s, "")
	if len(entries) != 1 || entries[0] != (storage.Entry{Key: "empty", Size: 0}) {
		t.Fatalf("List = %+v, want one empty object", entries)
	}
	if _, err := s.Create(ctx, "empty", strings.NewReader("")); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("Create over a zero-length object error = %v, want ErrPreconditionFailed", err)
	}
}

func testLargeObject(t reporter, s storage.Store) {
	data := make([]byte, 5<<20+3)
	r := rand.NewChaCha8([32]byte{1})
	_, _ = r.Read(data)
	v, err := s.Create(ctx, "big", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	got, info := get(t, s, "big")
	if got != string(data) || info.Size != int64(len(data)) || info.Version != v {
		t.Fatalf("Get returned %d bytes, info %+v; want the %d bytes written at version %q", len(got), info, len(data), v)
	}
	const off, n = 1<<20 + 7, 2 << 20
	body, _, err := s.GetRange(ctx, "big", off, n)
	if err != nil {
		t.Fatal(err)
	}
	if part := readAll(t, body); part != string(data[off:off+n]) {
		t.Fatalf("GetRange(%d, %d) returned different bytes (%d of them)", off, n, len(part))
	}
	body, _, err = s.GetRange(ctx, "big", int64(len(data))-10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if tail := readAll(t, body); tail != string(data[len(data)-10:]) {
		t.Fatalf("tail GetRange returned %q", tail)
	}
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
	sizes := map[string]int64{}
	for i, k := range all {
		content := strings.Repeat("x", i)
		create(t, s, k, content)
		sizes[k] = int64(len(content))
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
	}
	for _, tc := range cases {
		entries, err := s.List(ctx, tc.prefix, tc.startAfter, tc.limit)
		if err != nil {
			t.Fatalf("List(%q, %q, %d): %v", tc.prefix, tc.startAfter, tc.limit, err)
		}
		want := expectList(all, tc.prefix, tc.startAfter, tc.limit)
		if got := keys(entries); !slices.Equal(got, want) {
			t.Errorf("List(%q, %q, %d) = %q, want %q", tc.prefix, tc.startAfter, tc.limit, got, want)
		}
		for _, e := range entries {
			if e.Size != sizes[e.Key] {
				t.Errorf("List entry %q size = %d, want %d", e.Key, e.Size, sizes[e.Key])
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
		entries, err := s.List(ctx, "p/", after, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > limit {
			t.Fatalf("page %d has %d entries, limit %d", page, len(entries), limit)
		}
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
		if _, err := s.List(ctx, "", "", limit); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Errorf("List limit %d error = %v, want ErrInvalidArgument", limit, err)
		}
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

func openClients(open func() storage.Store, n int) []storage.Store {
	out := make([]storage.Store, n)
	for i := range out {
		out[i] = open()
	}
	return out
}

// payload is distinct per writer and large enough to take a few reads.
func payload(i int) string {
	return fmt.Sprintf("writer %d\n", i) + strings.Repeat(fmt.Sprint(i%10), 64<<10)
}

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
// and then waits for every goroutine, so that no write is left blocked.
type background struct {
	gates []func()
	wg    sync.WaitGroup
}

func newBackground(t reporter) *background {
	b := &background{}
	t.Cleanup(b.stop)
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

func (b *background) stop() {
	b.openAll()
	b.wg.Wait()
}

// within runs fn on another goroutine and waits up to overlapTimeout for it,
// whether or not the backend honours a context. If fn has not returned by
// then, a write stopped in its body is blocking it: within opens every gate,
// waits for fn and fails the test.
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
		<-done
		t.Fatalf("%s did not return within %v while a write was reading its body; a write must not block other mutations while it reads its body", what, overlapTimeout)
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

// finish releases the body and waits for the write to return.
func (p *pending) finish() (string, error) {
	p.release()
	<-p.done
	return p.version, p.err
}

// race runs fn(i, body) for i in [0, writers) at once and returns the index of
// the only call that returned nil. Every other call must fail with
// ErrPreconditionFailed. The bodies are gated: no write is released until
// every write is blocked reading its body, which proves that all of them were
// in progress at the same time, though not that they met inside the
// backend's critical window.
func race(t reporter, fn func(i int, body io.Reader) (string, error)) (winner int, version string) {
	t.Helper()
	release := make(chan struct{})
	open := sync.OnceFunc(func() { close(release) })
	var wg sync.WaitGroup
	defer wg.Wait()
	defer open()
	versions := make([]string, writers)
	errs := make([]error, writers)
	bodies := make([]*gatedBody, writers)
	returned := make(chan int, writers)
	for i := range writers {
		bodies[i] = newGatedBody(strings.NewReader(payload(i)), 0, release)
		wg.Go(func() {
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
	wg.Wait()
	winner = -1
	for i, err := range errs {
		switch {
		case err == nil && winner >= 0:
			t.Fatalf("writers %d and %d both succeeded", winner, i)
		case err == nil:
			winner = i
		case !errors.Is(err, storage.ErrPreconditionFailed):
			t.Fatalf("writer %d error = %v, want nil or ErrPreconditionFailed", i, err)
		}
	}
	if winner < 0 {
		t.Fatal("no writer succeeded")
	}
	return winner, versions[winner]
}

// testConcurrentCreate races Create calls for one absent key, each from its
// own client.
func testConcurrentCreate(t reporter, open func() storage.Store) {
	clients := openClients(open, writers)
	for r := range rounds {
		key := fmt.Sprintf("k%d", r)
		winner, v := race(t, func(i int, body io.Reader) (string, error) {
			return clients[i].Create(ctx, key, body)
		})
		expectContent(t, clients[(winner+1)%writers], key, payload(winner), v)
	}
}

// testConcurrentReplace races Replace calls with the same current version,
// each from its own client.
func testConcurrentReplace(t reporter, open func() storage.Store) {
	clients := openClients(open, writers)
	v := create(t, clients[0], "k", "initial")
	for range rounds {
		from := v
		var winner int
		winner, v = race(t, func(i int, body io.Reader) (string, error) {
			return clients[i].Replace(ctx, "k", from, body)
		})
		expectContent(t, clients[(winner+1)%writers], "k", payload(winner), v)
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
// window, and a round may well run them one after the other.
func deleteDuring(bg *background, s storage.Store, key string) <-chan error {
	result := make(chan error, 1)
	bg.wg.Go(func() { result <- s.Delete(ctx, key) })
	return result
}

// testReplaceDeleteRace checks that a Replace cannot bring back an object
// that a Delete removed. First a Delete, through the Replace's own client and
// then through another, completes while a Replace conditioned on the deleted
// version is reading its body, so the Replace must fail; then Delete and
// Replace are run at about the same time, and either order must leave the key
// missing.
func testReplaceDeleteRace(t reporter, open func() storage.Store) {
	a, b := open(), open()
	bg := newBackground(t)
	for _, deleter := range []storage.Store{a, b} {
		v := create(t, a, "k", "old")
		p := start(t, bg, false, strings.NewReader("resurrected"), func(body io.Reader) (string, error) {
			return a.Replace(ctx, "k", v, body)
		})
		deleteNow(t, bg, deleter, "k")
		if _, err := p.finish(); !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("Replace conditioned on a version that a completed Delete removed: error = %v, want ErrPreconditionFailed", err)
		}
		expectMissing(t, a, "k")
		expectMissing(t, b, "k")
	}

	for r := range rounds {
		key := fmt.Sprintf("r%d", r)
		v := create(t, a, key, "old")
		p := start(t, bg, false, strings.NewReader("new"), func(body io.Reader) (string, error) {
			return a.Replace(ctx, key, v, body)
		})
		deleted := deleteDuring(bg, b, key)
		_, err := p.finish()
		if derr := <-deleted; derr != nil {
			t.Fatalf("Delete racing a Replace: %v", derr)
		}
		if err != nil && !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("Replace racing a Delete: error = %v, want nil or ErrPreconditionFailed", err)
		}
		expectMissing(t, a, key)
	}
}

// testCreateDeleteRace runs Create against Delete. A Create of an absent key
// is not disturbed by a Delete of that missing key, through the Create's own
// client or another. When the key exists, a Create that succeeds must leave
// its object in place and one that fails must leave the key missing.
func testCreateDeleteRace(t reporter, open func() storage.Store) {
	a, b := open(), open()
	bg := newBackground(t)
	for i, deleter := range []storage.Store{a, b} {
		key := fmt.Sprintf("k%d", i)
		p := start(t, bg, false, strings.NewReader("created"), func(body io.Reader) (string, error) {
			return a.Create(ctx, key, body)
		})
		deleteNow(t, bg, deleter, key)
		v, err := p.finish()
		if err != nil {
			t.Fatalf("Create of an absent key after a Delete of it: %v", err)
		}
		expectContent(t, b, key, "created", v)
	}

	for r := range rounds {
		key := fmt.Sprintf("c%d", r)
		create(t, a, key, "old")
		p := start(t, bg, true, strings.NewReader("new"), func(body io.Reader) (string, error) {
			return a.Create(ctx, key, body)
		})
		deleted := deleteDuring(bg, b, key)
		v, err := p.finish()
		if derr := <-deleted; derr != nil {
			t.Fatalf("Delete racing a Create: %v", derr)
		}
		switch {
		case err == nil:
			expectContent(t, b, key, "new", v)
		case errors.Is(err, storage.ErrPreconditionFailed):
			expectMissing(t, b, key)
		default:
			t.Fatalf("Create racing a Delete: error = %v, want nil or ErrPreconditionFailed", err)
		}
	}
}

// replacement returns the content of the n-th version in
// testConcurrentReads: it names n, and its size alternates.
func replacement(n int) string {
	size := 256 << 10
	if n%2 == 1 {
		size = 64 << 10
	}
	head := fmt.Sprintf("replacement %d\n", n)
	return head + strings.Repeat(string(rune('a'+n%26)), size-len(head))
}

// testConcurrentReads replaces an object repeatedly while readers on other
// clients, and one on the writer's client, check that every byte a read
// returns belongs to the version its Info names, and that this Info is the
// one the write of that version returned. A body may fail once a Replace of
// its version has begun, as the contract allows; a body of a version that no
// Replace has started on must not fail, and once the Replaces are over every
// read must succeed.
func testConcurrentReads(t reporter, open func() storage.Store) {
	// maxReads stops a reader whose reads fail at once from spinning.
	const readers, replacements, maxReads = 4, 40, 2000
	clients := make([]storage.Store, readers)
	clients[0] = open()
	for i := 1; i < readers; i++ {
		clients[i] = open()
	}
	writer := clients[0]
	versions := make([]string, replacements+1) // versions[n] is the token the write of replacement(n) returned
	versions[0] = create(t, writer, "k", replacement(0))
	// started and completed count Replace calls. A read that began when
	// completed was c and ended when started was s overlapped a Replace if
	// and only if s > c.
	var started, completed, overlapping atomic.Int64
	type seen struct {
		n                int    // the version the bytes name, or -1 if the body failed before its first line
		short            string // the bytes, if n is -1
		info             storage.Info
		startedAtFailure int64 // started when the body failed, or -1
	}
	observed := make([][]seen, readers)
	done := make(chan struct{})
	var wg sync.WaitGroup
	stop := sync.OnceFunc(func() { close(done); wg.Wait() })
	defer stop() // also when replace calls Fatal
	for i, s := range clients {
		wg.Go(func() {
			for j := 0; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				c := completed.Load()
				var body io.ReadCloser
				var info storage.Info
				var err error
				if j%2 == 0 {
					body, info, err = s.Get(ctx, "k")
				} else {
					body, info, err = s.GetRange(ctx, "k", 0, math.MaxInt64)
				}
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				b, err := io.ReadAll(body)
				_ = body.Close()
				if started.Load() > c {
					overlapping.Add(1)
				}
				o := seen{n: -1, info: info, startedAtFailure: -1}
				if err != nil {
					o.startedAtFailure = started.Load()
				}
				if line, _, ok := strings.Cut(string(b), "\n"); ok {
					if _, serr := fmt.Sscanf(line, "replacement %d", &o.n); serr != nil || o.n < 0 || o.n > replacements || !strings.HasPrefix(replacement(o.n), string(b)) {
						t.Errorf("read %d bytes starting %.16q with info %+v: not bytes of one version", len(b), b, info)
						return
					}
				} else if err == nil || len(b) > 32 {
					t.Errorf("read %d bytes starting %.16q with info %+v and error %v: not bytes of one version", len(b), b, info, err)
					return
				} else {
					o.short = string(b)
				}
				if err == nil && int64(len(b)) != info.Size {
					t.Errorf("read %d bytes and no error, but info %+v", len(b), info)
					return
				}
				observed[i] = append(observed[i], o)
				if len(observed[i]) == maxReads {
					return
				}
			}
		})
	}
	for n := 1; n <= replacements; n++ {
		started.Add(1)
		versions[n] = replace(t, writer, "k", versions[n-1], replacement(n))
		completed.Add(1)
	}
	stop()
	byVersion := make(map[string]int, len(versions))
	for n, v := range versions {
		byVersion[v] = n
	}
	for _, obs := range observed {
		for _, o := range obs {
			n, ok := byVersion[o.info.Version]
			switch {
			case !ok:
				t.Fatalf("read version %q, which no write returned", o.info.Version)
			case o.n >= 0 && o.n != n:
				t.Fatalf("read bytes of replacement %d with version %q, which the write of replacement %d returned", o.n, o.info.Version, n)
			case o.n < 0 && !strings.HasPrefix(replacement(n), o.short):
				t.Fatalf("read %q with version %q of replacement %d", o.short, o.info.Version, n)
			case o.info.Size != int64(len(replacement(n))):
				t.Fatalf("read version %q of replacement %d with size %d, want %d", o.info.Version, n, o.info.Size, len(replacement(n)))
			case o.startedAtFailure >= 0 && o.startedAtFailure <= int64(n):
				t.Fatalf("a body of replacement %d failed before any Replace of that version had started", n)
			}
		}
	}
	if overlapping.Load() == 0 {
		t.Fatal("no read overlapped a Replace; the test proved nothing")
	}
	for _, s := range clients {
		expectContent(t, s, "k", replacement(replacements), versions[replacements])
	}
}

// testOpenBodies opens a Get and a GetRange body, replaces or deletes the
// object from another client, and only then reads the bodies. Each must
// return bytes of the version its Info describes, or fail.
func testOpenBodies(t reporter, open func() storage.Store) {
	old, repl := strings.Repeat("old-", 32<<10), strings.Repeat("NEW!", 32<<10)
	const off, n = 1000, 50000
	s, other := open(), open()
	for _, change := range []string{"Replace", "Delete"} {
		subtest(t, change, func(t reporter) {
			key := strings.ToLower(change)
			v := create(t, s, key, old)
			want := storage.Info{Size: int64(len(old)), Version: v}
			whole, wholeInfo, err := s.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			part, partInfo, err := s.GetRange(ctx, key, off, n)
			if err != nil {
				t.Fatal(err)
			}
			if change == "Replace" {
				replace(t, other, key, v, repl)
			} else if err := other.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			expectSnapshot(t, "Get", whole, wholeInfo, old, want)
			expectSnapshot(t, "GetRange", part, partInfo, old[off:off+n], want)
		})
	}
}

func expectSnapshot(t reporter, name string, body io.ReadCloser, info storage.Info, content string, want storage.Info) {
	t.Helper()
	defer func() { _ = body.Close() }()
	if info != want {
		t.Errorf("%s info = %+v, want %+v", name, info, want)
	}
	b, err := io.ReadAll(body)
	if !strings.HasPrefix(content, string(b)) {
		t.Errorf("%s body opened before the change returned %d bytes starting %.16q: not the version it was opened at", name, len(b), b)
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

// prefixLen is how much of its body testWriteInProgress lets a write read
// before stopping it a second time.
const prefixLen = 128 << 10

// testWriteInProgress stops a Create or Replace twice inside its body: before
// the first byte, and after a prefix of prefixLen bytes. At each stop it
// checks with Get, Head and List, from the writer's client and from another
// one, that nothing of the write is visible, and requires mutations through
// the writer's own client, of an unrelated key and of the key itself, to
// complete. Then it lets the write succeed, fail by a reader error or fail by
// cancellation, each from the second stop, and checks the outcome and that a
// failed write does not block the next one.
func testWriteInProgress(t reporter, open func() storage.Store) {
	w, r := open(), open()
	for _, op := range []string{"Create", "Replace"} {
		for _, outcome := range []string{"succeeds", "fails", "cancelled"} {
			subtest(t, op+" "+outcome, func(t reporter) {
				bg := newBackground(t)
				key := strings.ToLower(op + "-" + outcome)
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
				prefix := strings.Repeat("p", prefixLen)
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
				body := newGatedBody(io.MultiReader(strings.NewReader(prefix), rest), prefixLen, inner)
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
					t.Fatalf("%s did not read the first %d bytes of its body within %v", op, prefixLen, overlapTimeout)
				}
				unchanged("while the write is stopped inside its body")
				mutateDuring(t, bg, w, key, key+"-other2", op == "Replace")
				if outcome == "cancelled" {
					cancel()
					unchanged("after the write's context was cancelled")
				}
				openInner()
				<-p.done
				v, err := p.version, p.err
				switch {
				case outcome == "succeeds":
					if err != nil {
						t.Fatalf("%s: %v", op, err)
					}
					expectContent(t, r, key, prefix+"new", v)
					return
				case outcome == "fails" && !errors.Is(readerError(err), errReader):
					t.Fatalf("%s with a failing body: error = %v, want the reader's error in a *storage.BodyError", op, err)
				case outcome == "cancelled" && !errors.Is(err, context.Canceled):
					t.Fatalf("%s cancelled while reading its body: error = %v, want context.Canceled", op, err)
				case errors.Is(err, storage.ErrOutcomeUnknown):
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
		err := within(t, bg, "Create of the key being replaced, on the writing client", func() error {
			_, err := s.Create(ctx, key, strings.NewReader("x"))
			return err
		})
		if !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("Create(%q) of the key being replaced, on the same client: error = %v, want ErrPreconditionFailed", key, err)
		}
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
func testEndOfBody(t reporter, open func() storage.Store) {
	s, other := open(), open()
	for _, op := range []string{"Create", "Replace"} {
		for _, end := range []string{"cancel at EOF", "error with the last bytes"} {
			subtest(t, op+" "+end, func(t reporter) {
				key := strings.ReplaceAll(strings.ToLower(op+"-"+end), " ", "-")
				var old string
				if op == "Replace" {
					old = create(t, s, key, "old")
				}
				const content = "new content"
				cctx, cancel := context.WithCancel(ctx)
				defer cancel()
				src := strings.NewReader(content)
				body := readerFunc(func(p []byte) (int, error) {
					n, _ := src.Read(p)
					if src.Len() > 0 {
						return n, nil
					}
					if end == "cancel at EOF" {
						cancel()
						return n, io.EOF
					}
					return n, errReader
				})
				var v string
				var err error
				if op == "Create" {
					v, err = s.Create(cctx, key, body)
				} else {
					v, err = s.Replace(cctx, key, old, body)
				}
				unknown := errors.Is(err, storage.ErrOutcomeUnknown)
				if end == "error with the last bytes" && (!errors.Is(readerError(err), errReader) || unknown) {
					t.Fatalf("%s whose body failed with its last bytes: error = %v, want the reader's error in a *storage.BodyError and no ErrOutcomeUnknown", op, err)
				}
				newEntry := []storage.Entry{{Key: key, Size: int64(len(content))}}
				var oldEntries []storage.Entry
				if op == "Replace" {
					oldEntries = []storage.Entry{{Key: key, Size: 3}}
				}
				switch {
				case err == nil:
					expectContent(t, other, key, content, v)
				case unknown:
					body, info, gerr := other.Get(ctx, key)
					got := "absent"
					if gerr == nil {
						got = readAll(t, body)
					} else if !errors.Is(gerr, storage.ErrNotFound) {
						t.Fatalf("Get(%q): %v", key, gerr)
					}
					isOld := op == "Create" && got == "absent" || op == "Replace" && got == "old" && info.Version == old
					if got != content && !isOld {
						t.Fatalf("after %s failed with %v the key holds %q at %q: neither the old state nor the new content", op, err, got, info.Version)
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

func testCancelledContext(t reporter, s storage.Store) {
	v := create(t, s, "k", "content")
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	ops := map[string]func() error{
		"Create": func() error { _, err := s.Create(cctx, "new", strings.NewReader("x")); return err },
		"Replace": func() error {
			_, err := s.Replace(cctx, "k", v, strings.NewReader("x"))
			return err
		},
		"Get":      func() error { _, _, err := s.Get(cctx, "k"); return err },
		"GetRange": func() error { _, _, err := s.GetRange(cctx, "k", 0, 1); return err },
		"Head":     func() error { _, err := s.Head(cctx, "k"); return err },
		"List":     func() error { _, err := s.List(cctx, "", "", 10); return err },
		"Delete":   func() error { return s.Delete(cctx, "k") },
	}
	for name, op := range ops {
		if err := op(); !errors.Is(err, context.Canceled) || errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Errorf("%s with a cancelled context error = %v, want context.Canceled and no ErrOutcomeUnknown", name, err)
		}
	}
	expectContent(t, s, "k", "content", v)
	expectMissing(t, s, "new")
}

func testBodyHonoursContext(t reporter, s storage.Store) {
	create(t, s, "k", strings.Repeat("x", 8<<20))
	cctx, cancel := context.WithCancel(ctx)
	body, _, err := s.Get(cctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	cancel()
	if _, err := io.ReadAll(body); err == nil {
		t.Fatal("reading a body after its context was cancelled succeeded")
	}
}

func testErrors(t reporter, s storage.Store) {
	const secret1, secret2 = "secret-content-one", "secret-content-two"
	v := create(t, s, "dir/k", secret1)
	_, createErr := s.Create(ctx, "dir/k", strings.NewReader(secret2))
	_, replaceErr := s.Replace(ctx, "dir/k", v+"stale", strings.NewReader(secret2))
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
		var se *storage.Error
		if !errors.As(tc.err, &se) {
			t.Errorf("%s error %v is not a *storage.Error", tc.op, tc.err)
			continue
		}
		if se.Op != tc.op || se.Key != tc.key || !errors.Is(tc.err, tc.is) {
			t.Errorf("%s error = {Op: %q, Key: %q, Err: %v}, want Op %q, Key %q, wrapping %v", tc.op, se.Op, se.Key, se.Err, tc.op, tc.key, tc.is)
		}
		msg := tc.err.Error()
		if !strings.Contains(msg, tc.key) {
			t.Errorf("%s error %q does not name the key %q", tc.op, msg, tc.key)
		}
		if strings.Contains(msg, "secret-content") {
			t.Errorf("%s error %q contains object contents", tc.op, msg)
		}
	}
}

// testBodyErrorText fails writes with body readers whose errors are object
// contents, each storage sentinel and each context error. The reader's error
// must be held in a *storage.BodyError, its text must not reach the error
// message, and errors.Is on the write's error must not match it: a reader
// that returns ErrOutcomeUnknown or ErrPreconditionFailed has not made the
// write's outcome unknown or its condition fail.
func testBodyErrorText(t reporter, s storage.Store) {
	const secret = "secret-payload-9f2c41"
	sentinels := []error{
		storage.ErrNotFound, storage.ErrPreconditionFailed, storage.ErrInvalidKey, storage.ErrInvalidArgument,
		storage.ErrOutcomeUnknown, context.Canceled, context.DeadlineExceeded,
	}
	v := create(t, s, "k", "x")
	for _, readerErr := range append([]error{errors.New(secret)}, sentinels...) {
		_, createErr := s.Create(ctx, "new", iotest.ErrReader(readerErr))
		_, replaceErr := s.Replace(ctx, "k", v, io.MultiReader(strings.NewReader("y"), iotest.ErrReader(readerErr)))
		for op, err := range map[string]error{"Create": createErr, "Replace": replaceErr} {
			if !errors.Is(readerError(err), readerErr) {
				t.Errorf("%s with a body reader failing with %q: error %v does not hold the reader's error in a *storage.BodyError", op, readerErr, err)
			}
			for _, sentinel := range sentinels {
				if errors.Is(err, sentinel) {
					t.Errorf("%s with a body reader failing with %q: errors.Is(error, %q) is true, but only the reader returned it", op, readerErr, sentinel)
				}
			}
			if err != nil && strings.Contains(err.Error(), secret) {
				t.Errorf("%s error %q contains the reader's error text", op, err)
			}
		}
	}
	expectContent(t, s, "k", "x", v)
	expectMissing(t, s, "new")
}
