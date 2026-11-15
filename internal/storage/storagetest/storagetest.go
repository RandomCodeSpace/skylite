// Package storagetest is the portable part of the conformance suite for
// storage.Store backends.
//
// A backend's tests call [Run] with a function that creates a new, empty
// namespace and returns a way to open independent clients onto it. Every
// subtest gets its own namespace.
//
// The suite covers what can be checked through the interface from one
// process: key validation, conditional writes and version tokens, range
// reads, listing order and pagination, failed and cancelled writes, what
// readers on other clients see while a write is in progress, open bodies
// across a Replace or Delete, error contents, and races between independent
// clients: Create against Create, Replace against Replace, Replace against
// Delete and Create against Delete. Every race test proves that its calls
// overlapped, so a serial run cannot pass it.
//
// It cannot cover races between processes or hosts, a process killed during
// or just after publication, durability across a crash or power loss, an
// interrupted staged upload, a size limit, or redaction of a service SDK's
// errors. Each backend tests those itself, as the storage package
// documentation requires. Passing [Run] is necessary for a backend to be
// accepted, not sufficient.
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
	one := func(fn func(*testing.T, storage.Store)) func(*testing.T, func() storage.Store) {
		return func(t *testing.T, open func() storage.Store) { fn(t, open()) }
	}
	tests := []struct {
		name string
		fn   func(t *testing.T, open func() storage.Store)
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
		{"ConcurrentCreate", testConcurrentCreate},
		{"ConcurrentReplace", testConcurrentReplace},
		{"ReplaceDeleteRace", testReplaceDeleteRace},
		{"CreateDeleteRace", testCreateDeleteRace},
		{"ConcurrentReadsSeeWholeVersions", testConcurrentReads},
		{"OpenBodyKeepsItsVersion", testOpenBodies},
		{"WriteInProgress", testWriteInProgress},
		{"CancelledContext", one(testCancelledContext)},
		{"BodyHonoursContext", one(testBodyHonoursContext)},
		{"Errors", one(testErrors)},
		{"BodyErrorText", one(testBodyErrorText)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newNamespace(t)) })
	}
}

var ctx = context.Background()

func create(t *testing.T, s storage.Store, key, content string) string {
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

func replace(t *testing.T, s storage.Store, key, version, content string) string {
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

func readAll(t *testing.T, body io.ReadCloser) string {
	t.Helper()
	defer func() { _ = body.Close() }()
	b, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(b)
}

func get(t *testing.T, s storage.Store, key string) (string, storage.Info) {
	t.Helper()
	body, info, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return readAll(t, body), info
}

// expectContent checks that key holds content at version (any version if "").
func expectContent(t *testing.T, s storage.Store, key, content, version string) {
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

func expectMissing(t *testing.T, s storage.Store, key string) {
	t.Helper()
	if _, err := s.Head(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Head(%q) error = %v, want ErrNotFound", key, err)
	}
	if _, _, err := s.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get(%q) error = %v, want ErrNotFound", key, err)
	}
}

func listAll(t *testing.T, s storage.Store, prefix string) []storage.Entry {
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
func testInvalidKeys(t *testing.T, s storage.Store) {
	v := create(t, s, "valid", "x")
	body := readerFunc(func([]byte) (int, error) {
		t.Error("a write with an invalid key read its body")
		return 0, io.EOF
	})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, tc := range invalidKeys {
		t.Run(tc.name, func(t *testing.T) {
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

func testValidKeys(t *testing.T, s storage.Store) {
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

func testNotFound(t *testing.T, s storage.Store) {
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

func testCreateExisting(t *testing.T, s storage.Store) {
	v := create(t, s, "k", "first")
	if _, err := s.Create(ctx, "k", strings.NewReader("second")); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("second Create error = %v, want ErrPreconditionFailed", err)
	}
	expectContent(t, s, "k", "first", v)
}

func testReplace(t *testing.T, s storage.Store) {
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

func testVersionTokens(t *testing.T, s storage.Store) {
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

func testDelete(t *testing.T, s storage.Store) {
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

func testRangeReads(t *testing.T, s storage.Store) {
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

func testZeroLength(t *testing.T, s storage.Store) {
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

func testLargeObject(t *testing.T, s storage.Store) {
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

func testListOrder(t *testing.T, s storage.Store) {
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

func testListPagination(t *testing.T, s storage.Store) {
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

func testListLimits(t *testing.T, s storage.Store) {
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

// writers race in each of rounds rounds; several rounds make a race window
// that one round misses likely to be hit.
const writers, rounds = 16, 10

// overlapTimeout bounds how long a test waits for calls that must overlap.
const overlapTimeout = 10 * time.Second

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

// gatedBody is a write body whose first Read closes started and waits for
// release before reading from r. A write blocked in it has begun and cannot
// have published anything.
type gatedBody struct {
	r       io.Reader
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func newGatedBody(r io.Reader, release <-chan struct{}) *gatedBody {
	return &gatedBody{r: r, started: make(chan struct{}), release: release}
}

func (g *gatedBody) Read(p []byte) (int, error) {
	g.once.Do(func() { close(g.started); <-g.release })
	return g.r.Read(p)
}

// pending is a write running in the background with a gated body.
type pending struct {
	body    *gatedBody
	release func()
	done    chan struct{}
	version string
	err     error
}

// start runs write in the background with a gated body over r and waits
// until the write is blocked reading it. If the write returns before reading
// its body, start fails the test unless mayReturnEarly is set.
func start(t *testing.T, mayReturnEarly bool, r io.Reader, write func(body io.Reader) (string, error)) *pending {
	t.Helper()
	ch := make(chan struct{})
	p := &pending{body: newGatedBody(r, ch), release: sync.OnceFunc(func() { close(ch) }), done: make(chan struct{})}
	t.Cleanup(p.release)
	go func() {
		defer close(p.done)
		p.version, p.err = write(p.body)
	}()
	select {
	case <-p.body.started:
	case <-p.done:
		if !mayReturnEarly {
			t.Fatalf("write returned %v before reading its body", p.err)
		}
	case <-time.After(overlapTimeout):
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
// in progress at the same time.
func race(t *testing.T, fn func(i int, body io.Reader) (string, error)) (winner int, version string) {
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
		bodies[i] = newGatedBody(strings.NewReader(payload(i)), release)
		wg.Go(func() {
			versions[i], errs[i] = fn(i, bodies[i])
			returned <- i
		})
	}
	timeout := time.After(overlapTimeout)
	for i := range writers {
		select {
		case <-bodies[i].started:
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
func testConcurrentCreate(t *testing.T, open func() storage.Store) {
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
func testConcurrentReplace(t *testing.T, open func() storage.Store) {
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

// deleteNow deletes key and fails the test if that takes longer than
// overlapTimeout, as it would if a write in progress blocked it.
func deleteNow(t *testing.T, s storage.Store, key string) {
	t.Helper()
	dctx, cancel := context.WithTimeout(ctx, overlapTimeout)
	defer cancel()
	if err := s.Delete(dctx, key); err != nil {
		t.Fatalf("Delete(%q) while a write was reading its body: %v", key, err)
	}
}

// deleteDuring starts Delete on another goroutine, waits until it is about
// to be called, and returns a channel that receives its result.
func deleteDuring(s storage.Store, key string) <-chan error {
	calling := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(calling)
		result <- s.Delete(ctx, key)
	}()
	<-calling
	return result
}

// testReplaceDeleteRace checks that a Replace cannot bring back an object
// that a Delete removed. First a Delete completes while a Replace conditioned
// on the deleted version is reading its body, so the Replace must fail; then
// Delete and Replace race for the commit, and either order must leave the
// key missing.
func testReplaceDeleteRace(t *testing.T, open func() storage.Store) {
	a, b := open(), open()
	v := create(t, a, "k", "old")
	p := start(t, false, strings.NewReader("resurrected"), func(body io.Reader) (string, error) {
		return a.Replace(ctx, "k", v, body)
	})
	deleteNow(t, b, "k")
	if _, err := p.finish(); !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("Replace conditioned on a version that a completed Delete removed: error = %v, want ErrPreconditionFailed", err)
	}
	expectMissing(t, a, "k")
	expectMissing(t, b, "k")

	for r := range rounds {
		key := fmt.Sprintf("r%d", r)
		v := create(t, a, key, "old")
		p := start(t, false, strings.NewReader("new"), func(body io.Reader) (string, error) {
			return a.Replace(ctx, key, v, body)
		})
		deleted := deleteDuring(b, key)
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

// testCreateDeleteRace races Create against Delete. A Create of an absent key
// is not disturbed by a Delete of that missing key. When the key exists, a
// Create that succeeds must leave its object in place and one that fails
// must leave the key missing.
func testCreateDeleteRace(t *testing.T, open func() storage.Store) {
	a, b := open(), open()
	p := start(t, false, strings.NewReader("created"), func(body io.Reader) (string, error) {
		return a.Create(ctx, "k", body)
	})
	deleteNow(t, b, "k")
	v, err := p.finish()
	if err != nil {
		t.Fatalf("Create of an absent key after a Delete of it: %v", err)
	}
	expectContent(t, b, "k", "created", v)

	for r := range rounds {
		key := fmt.Sprintf("c%d", r)
		create(t, a, key, "old")
		p := start(t, true, strings.NewReader("new"), func(body io.Reader) (string, error) {
			return a.Create(ctx, key, body)
		})
		deleted := deleteDuring(b, key)
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
// clients, and one on the writer's client, check that every read is one
// whole version and that the Info returned with it is the one the write of
// that version returned.
func testConcurrentReads(t *testing.T, open func() storage.Store) {
	const readers, replacements = 4, 40
	writer := open()
	versions := make([]string, replacements+1) // versions[n] is the token the write of replacement(n) returned
	versions[0] = create(t, writer, "k", replacement(0))
	var phase atomic.Int64 // odd while a Replace is in progress
	var during atomic.Int64
	type seen struct {
		n       int
		version string
	}
	observed := make([][]seen, readers)
	done := make(chan struct{})
	var wg sync.WaitGroup
	stop := sync.OnceFunc(func() { close(done); wg.Wait() })
	defer stop() // also when replace calls Fatal
	for i := range readers {
		s := writer
		if i > 0 {
			s = open()
		}
		wg.Go(func() {
			for j := 0; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				before := phase.Load()
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
				if err != nil {
					t.Errorf("reading body: %v", err)
					return
				}
				var n int
				if _, err := fmt.Sscanf(string(b), "replacement %d\n", &n); err != nil || n < 0 || n > replacements || string(b) != replacement(n) || int64(len(b)) != info.Size {
					t.Errorf("read %d bytes starting %.16q with info %+v: not one whole version", len(b), b, info)
					return
				}
				observed[i] = append(observed[i], seen{n, info.Version})
				if before%2 == 1 && phase.Load() == before {
					during.Add(1)
				}
			}
		})
	}
	for n := 1; n <= replacements; n++ {
		phase.Add(1)
		versions[n] = replace(t, writer, "k", versions[n-1], replacement(n))
		phase.Add(1)
	}
	stop()
	for _, obs := range observed {
		for _, o := range obs {
			if o.version != versions[o.n] {
				t.Fatalf("read replacement %d with version %q, but its write returned %q", o.n, o.version, versions[o.n])
			}
		}
	}
	if during.Load() == 0 {
		t.Fatal("no read ran while a Replace was in progress; the test proved nothing")
	}
}

// testOpenBodies opens a Get and a GetRange body, replaces or deletes the
// object from another client, and only then reads the bodies. Each must
// return bytes of the version its Info describes, or fail.
func testOpenBodies(t *testing.T, open func() storage.Store) {
	old, repl := strings.Repeat("old-", 32<<10), strings.Repeat("NEW!", 32<<10)
	const off, n = 1000, 50000
	s, other := open(), open()
	for _, change := range []string{"Replace", "Delete"} {
		t.Run(change, func(t *testing.T) {
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

func expectSnapshot(t *testing.T, name string, body io.ReadCloser, info storage.Info, content string, want storage.Info) {
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

// testWriteInProgress blocks a Create or Replace inside its body's Read and
// checks from the writer's client and from another one that nothing of the
// write is visible, both before it is released and, for a write that is
// about to fail or be cancelled, after the failure is decided. Then it checks
// the outcome and that a failed write does not block the next one.
func testWriteInProgress(t *testing.T, open func() storage.Store) {
	w, r := open(), open()
	for _, op := range []string{"Create", "Replace"} {
		for _, outcome := range []string{"succeeds", "fails", "cancelled"} {
			t.Run(op+" "+outcome, func(t *testing.T) {
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
				var rest io.Reader
				switch outcome {
				case "succeeds":
					rest = strings.NewReader("new")
				case "fails":
					rest = io.MultiReader(strings.NewReader(strings.Repeat("f", 1<<20)), iotest.ErrReader(errReader))
				case "cancelled": // the body keeps returning data, so only the store can stop the write
					rest = io.LimitReader(readerFunc(func(p []byte) (int, error) { clear(p); return len(p), nil }), 64<<20)
				}
				p := start(t, false, rest, func(body io.Reader) (string, error) {
					if op == "Create" {
						return w.Create(cctx, key, body)
					}
					return w.Replace(cctx, key, old, body)
				})
				unchanged("while the write reads its body")
				if outcome == "cancelled" {
					cancel()
					unchanged("after the write's context was cancelled")
				}
				v, err := p.finish()
				switch {
				case outcome == "succeeds":
					if err != nil {
						t.Fatalf("%s: %v", op, err)
					}
					expectContent(t, r, key, "new", v)
					return
				case outcome == "fails" && !errors.Is(err, errReader):
					t.Fatalf("%s with a failing body: error = %v, want the reader's error", op, err)
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

func testCancelledContext(t *testing.T, s storage.Store) {
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

func testBodyHonoursContext(t *testing.T, s storage.Store) {
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

func testErrors(t *testing.T, s storage.Store) {
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

// testBodyErrorText fails writes with a body reader whose error text is
// object contents, and checks that the text does not reach the error message
// while the reader's error stays reachable.
func testBodyErrorText(t *testing.T, s storage.Store) {
	const secret = "secret-payload-9f2c41"
	leak := errors.New(secret)
	_, createErr := s.Create(ctx, "k", iotest.ErrReader(leak))
	v := create(t, s, "k", "x")
	_, replaceErr := s.Replace(ctx, "k", v, io.MultiReader(strings.NewReader("y"), iotest.ErrReader(leak)))
	for op, err := range map[string]error{"Create": createErr, "Replace": replaceErr} {
		var be *storage.BodyError
		if !errors.Is(err, leak) || !errors.As(err, &be) {
			t.Errorf("%s error %v does not wrap the reader's error in a *storage.BodyError", op, err)
		}
		if err != nil && strings.Contains(err.Error(), secret) {
			t.Errorf("%s error %q contains the reader's error text", op, err)
		}
	}
	expectContent(t, s, "k", "x", v)
}
