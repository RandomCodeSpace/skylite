// Package storagetest is the conformance suite for storage.Store backends.
//
// A backend's tests call [Run] with a function that returns a new, empty
// store. Every point of the contract in package storage is a subtest; a
// backend is accepted only if all of them pass.
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// Run runs the conformance suite. newStore must return a new, empty store and
// register its cleanup with t.Cleanup.
func Run(t *testing.T, newStore func(t *testing.T) storage.Store) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s storage.Store)
	}{
		{"InvalidKeys", testInvalidKeys},
		{"ValidKeys", testValidKeys},
		{"NotFound", testNotFound},
		{"CreateExisting", testCreateExisting},
		{"Replace", testReplace},
		{"VersionTokens", testVersionTokens},
		{"Delete", testDelete},
		{"RangeReads", testRangeReads},
		{"ZeroLength", testZeroLength},
		{"LargeObject", testLargeObject},
		{"ListOrder", testListOrder},
		{"ListPagination", testListPagination},
		{"ListLimits", testListLimits},
		{"ConcurrentCreate", testConcurrentCreate},
		{"ConcurrentReplace", testConcurrentReplace},
		{"ConcurrentReadsSeeWholeObjects", testConcurrentReads},
		{"FailedReader", testFailedReader},
		{"CancelledDuringWrite", testCancelledDuringWrite},
		{"CancelledContext", testCancelledContext},
		{"BodyHonoursContext", testBodyHonoursContext},
		{"Errors", testErrors},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore(t)) })
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

func expectEmpty(t *testing.T, s storage.Store) {
	t.Helper()
	if entries := listAll(t, s, ""); len(entries) != 0 {
		t.Fatalf("List returned %v, want an empty store", entries)
	}
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
	{"leading dot segment", "./a"},
	{"inner dot segment", "a/./b"},
	{"trailing dot segment", "a/."},
	{"dotdot", ".."},
	{"leading dotdot segment", "../a"},
	{"inner dotdot segment", "a/../b"},
	{"trailing dotdot segment", "a/.."},
	{"traversal", "../../etc/passwd"},
	{"NUL", "a\x00b"},
	{"newline", "a\nb"},
	{"tab", "a\tb"},
	{"escape", "a\x1bb"},
	{"DEL", "a\x7fb"},
	{"C1 control", "a\u0085b"},
	{"U+FFFE", "a\ufffeb"},
	{"U+FFFF", "a\uffffb"},
	{"backslash", `a\b`},
	{"backslash traversal", `..\a`},
	{"invalid UTF-8", "a\xffb"},
	{"truncated UTF-8", "a\xe6\x97"},
	{"too long", strings.Repeat("a", storage.MaxKeyLen+1)},
	{"too long multi-byte", strings.Repeat("\u00e9", storage.MaxKeyLen/2) + "a"},
}

func testInvalidKeys(t *testing.T, s storage.Store) {
	v := create(t, s, "valid", "x")
	for _, tc := range invalidKeys {
		t.Run(tc.name, func(t *testing.T) {
			if err := storage.ValidateKey(tc.key); !errors.Is(err, storage.ErrInvalidKey) {
				t.Errorf("ValidateKey error = %v, want ErrInvalidKey", err)
			}
			ops := map[string]func() error{
				"Create": func() error { _, err := s.Create(ctx, tc.key, strings.NewReader("x")); return err },
				"Replace": func() error {
					_, err := s.Replace(ctx, tc.key, v, strings.NewReader("x"))
					return err
				},
				"Get":      func() error { _, _, err := s.Get(ctx, tc.key); return err },
				"GetRange": func() error { _, _, err := s.GetRange(ctx, tc.key, 0, 1); return err },
				"Head":     func() error { _, err := s.Head(ctx, tc.key); return err },
				"Delete":   func() error { return s.Delete(ctx, tc.key) },
			}
			for name, op := range ops {
				if err := op(); !errors.Is(err, storage.ErrInvalidKey) {
					t.Errorf("%s error = %v, want ErrInvalidKey", name, err)
				}
			}
		})
	}
	if got := keys(listAll(t, s, "")); !slices.Equal(got, []string{"valid"}) {
		t.Fatalf("after invalid-key operations List = %q, want only %q", got, "valid")
	}
}

// validKeys sort awkwardly, look like paths, or differ only in case or
// Unicode normalisation.
var validKeys = []string{
	"a", "a/b", "a.b", "a/b/c", "a0", "a-b", "a~", "b", "Z", "z",
	"..a", "a..", ".a/b.", "...", "a/..b/c", "a b", "%2e%2e",
	"\u00e9",  // e acute, NFC
	"e\u0301", // e acute, NFD
	"\u65e5\u672c/\u8a9e", "\u65e5\u672c\u8a9e", "emoji/\U0001f642", "\ufffd", "\U0010ffff",
	`~!@#$%^&*()_+=-[]{};':",<>?|` + "`",
	strings.Repeat("k", storage.MaxKeyLen),
	strings.Repeat("\u00e9", storage.MaxKeyLen/2),
	strings.Repeat("s/", storage.MaxKeyLen/2-1) + "s",
}

func testValidKeys(t *testing.T, s storage.Store) {
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
		"a", "a/b", "a.b", "a/b/c", "a0", "a/b0", "a/c", "ab", "b", "B", "a-",
		"\u65e5\u672c/\u8a9e", "\u65e5\u672c", "\u65e5", "\u00e9", "e\u0301", "e", "\U0010ffff", "~",
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
		{"\u65e5", "", 1000},
		{"nothing", "", 1000},
		{"", "a/b", 1000},
		{"", "a/a", 1000},
		{"", "a", 3},
		{"a", "a/b", 2},
		{"", "\U0010ffff", 1000},
		{"", "~", 1000},
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

// payload is distinct per writer and large enough to take a few reads.
func payload(i int) string {
	return fmt.Sprintf("writer %d\n", i) + strings.Repeat(fmt.Sprint(i%10), 64<<10)
}

// race runs fn(i) for i in [0, writers) at once and returns the index of the
// only call that returned nil. Every other call must fail with
// ErrPreconditionFailed.
func race(t *testing.T, fn func(i int) (string, error)) (winner int, version string) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	versions := make([]string, writers)
	errs := make([]error, writers)
	for i := range writers {
		wg.Go(func() {
			<-start
			versions[i], errs[i] = fn(i)
		})
	}
	close(start)
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

func testConcurrentCreate(t *testing.T, s storage.Store) {
	for r := range rounds {
		key := fmt.Sprintf("k%d", r)
		winner, v := race(t, func(i int) (string, error) {
			return s.Create(ctx, key, strings.NewReader(payload(i)))
		})
		expectContent(t, s, key, payload(winner), v)
	}
}

func testConcurrentReplace(t *testing.T, s storage.Store) {
	v := create(t, s, "k", "initial")
	for range rounds {
		from := v
		var winner int
		winner, v = race(t, func(i int) (string, error) {
			return s.Replace(ctx, "k", from, strings.NewReader(payload(i)))
		})
		expectContent(t, s, "k", payload(winner), v)
	}
}

// testConcurrentReads replaces an object between two contents of different
// sizes while readers check that every read is one whole version.
func testConcurrentReads(t *testing.T, s storage.Store) {
	contents := []string{strings.Repeat("a", 256<<10), strings.Repeat("b", 64<<10)}
	v := create(t, s, "k", contents[0])
	done := make(chan struct{})
	var wg sync.WaitGroup
	defer func() { close(done); wg.Wait() }() // also when replace calls Fatal
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				body, info, err := s.Get(ctx, "k")
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				b, err := io.ReadAll(body)
				_ = body.Close()
				if err != nil {
					t.Errorf("reading body: %v", err)
					return
				}
				if string(b) != contents[0] && string(b) != contents[1] || int64(len(b)) != info.Size {
					t.Errorf("read %d bytes starting %.8q with info %+v: not one whole version", len(b), b, info)
					return
				}
			}
		})
	}
	for i := range 40 {
		v = replace(t, s, "k", v, contents[(i+1)%2])
	}
}

var errReader = errors.New("storagetest: reader failed")

// failingReader returns n bytes and then fails with errReader.
type failingReader struct{ n int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, errReader
	}
	k := min(len(p), r.n)
	for i := range k {
		p[i] = 'f'
	}
	r.n -= k
	return k, nil
}

func testFailedReader(t *testing.T, s storage.Store) {
	_, err := s.Create(ctx, "k", &failingReader{n: 1 << 20})
	if !errors.Is(err, errReader) {
		t.Fatalf("Create with a failing reader error = %v, want the reader's error", err)
	}
	expectMissing(t, s, "k")
	expectEmpty(t, s)
	v := create(t, s, "k", "after")
	expectContent(t, s, "k", "after", v)

	_, err = s.Replace(ctx, "k", v, &failingReader{n: 1 << 20})
	if !errors.Is(err, errReader) {
		t.Fatalf("Replace with a failing reader error = %v, want the reader's error", err)
	}
	expectContent(t, s, "k", "after", v)
	v = replace(t, s, "k", v, "replaced")
	expectContent(t, s, "k", "replaced", v)
}

// cancellingReader cancels its context after 256 KiB and then keeps
// returning data, up to 64 MiB, so only the store can stop the write.
type cancellingReader struct {
	cancel context.CancelFunc
	n      int
}

func (r *cancellingReader) Read(p []byte) (int, error) {
	if r.n >= 64<<20 {
		return 0, io.EOF
	}
	if r.n >= 256<<10 {
		r.cancel()
	}
	k := min(len(p), 64<<20-r.n)
	for i := range k {
		p[i] = 'c'
	}
	r.n += k
	return k, nil
}

func testCancelledDuringWrite(t *testing.T, s storage.Store) {
	cctx, cancel := context.WithCancel(ctx)
	_, err := s.Create(cctx, "k", &cancellingReader{cancel: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Create cancelled during the write error = %v, want context.Canceled", err)
	}
	expectMissing(t, s, "k")
	expectEmpty(t, s)
	v := create(t, s, "k", "after")
	expectContent(t, s, "k", "after", v)

	cctx, cancel = context.WithCancel(ctx)
	_, err = s.Replace(cctx, "k", v, &cancellingReader{cancel: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Replace cancelled during the write error = %v, want context.Canceled", err)
	}
	expectContent(t, s, "k", "after", v)
	v = replace(t, s, "k", v, "replaced")
	expectContent(t, s, "k", "replaced", v)
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
		if err := op(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context error = %v, want context.Canceled", name, err)
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
