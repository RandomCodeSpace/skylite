package s3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// endpointEnv names the S3 endpoint of the emulator, for example
// http://127.0.0.1:9000. `make integration-s3` sets it. Without it the tests
// that need a service are skipped.
const endpointEnv = "SKYLITE_TEST_S3_ENDPOINT"

// RustFS's documented default development credentials. They are public and
// only work against the emulator.
const (
	devAccessKey = "rustfsadmin"
	devSecretKey = "rustfsadmin"
	testRegion   = "us-east-1"
)

func testEndpoint(t testing.TB) string {
	t.Helper()
	ep := os.Getenv(endpointEnv)
	if ep == "" {
		t.Skipf("%s is not set; run `make integration-s3` or point it at an S3 emulator", endpointEnv)
	}
	return ep
}

func devCredentials() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: devAccessKey, SecretAccessKey: devSecretKey, Source: "test"}, nil
	})
}

// bucket is a new, empty bucket on the emulator, with a client that bypasses
// the backend.
type bucket struct {
	name  string
	admin *awss3.Client
	cfg   Config
}

// newBucket creates a bucket and removes it, with everything in it, when t
// ends.
func newBucket(t testing.TB) *bucket {
	t.Helper()
	ep := testEndpoint(t)
	var b [8]byte
	_, _ = rand.Read(b[:])
	name := "skylite-test-" + hex.EncodeToString(b[:])
	admin := awss3.New(awss3.Options{Region: testRegion, Credentials: devCredentials(), BaseEndpoint: aws.String(ep), UsePathStyle: true})
	ctx := context.Background()
	if _, err := admin.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		p := awss3.NewListObjectsV2Paginator(admin, &awss3.ListObjectsV2Input{Bucket: aws.String(name)})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				t.Errorf("cleaning up bucket %s: %v", name, err)
				return
			}
			for _, o := range page.Contents {
				if _, err := admin.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(name), Key: o.Key}); err != nil {
					t.Errorf("cleaning up bucket %s: %v", name, err)
					return
				}
			}
		}
		if _, err := admin.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
			t.Errorf("DeleteBucket %s: %v", name, err)
		}
	})
	cfg := Config{Bucket: name, Region: testRegion, Credentials: devCredentials(), Endpoint: ep, UsePathStyle: true, SpoolDir: t.TempDir()}
	return &bucket{name: name, admin: admin, cfg: cfg}
}

// store opens a new client onto the bucket through New.
func (b *bucket) store(t testing.TB) *Store {
	t.Helper()
	s, err := New(b.cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// storeVia opens a new client onto the bucket whose requests pass through rt.
func (b *bucket) storeVia(t testing.TB, rt http.RoundTripper) *Store {
	t.Helper()
	s, err := newStore(b.cfg, &http.Client{Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// raw returns the stored bytes and metadata of key, read past the backend.
func (b *bucket) raw(t testing.TB, key string) ([]byte, map[string]string) {
	t.Helper()
	out, err := b.admin.GetObject(context.Background(), &awss3.GetObjectInput{Bucket: aws.String(b.name), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("raw GetObject(%q): %v", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return data, out.Metadata
}

// exists reports whether key exists, asking past the backend.
func (b *bucket) exists(t testing.TB, key string) bool {
	t.Helper()
	_, err := b.admin.HeadObject(context.Background(), &awss3.HeadObjectInput{Bucket: aws.String(b.name), Key: aws.String(key)})
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return false
	}
	if err != nil {
		t.Fatalf("raw HeadObject(%q): %v", key, err)
	}
	return true
}

func TestNewNeedsBucketRegionAndCredentials(t *testing.T) {
	for _, cfg := range []Config{
		{Region: testRegion, Credentials: devCredentials()},
		{Bucket: "b", Credentials: devCredentials()},
		{Bucket: "b", Region: testRegion},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
}

// TestListStart checks listStart against the definition it must meet: for
// every key k, k > s if and only if k > listStart(s), and listStart(s) is
// printable ASCII; if it reports false, no key sorts after s.
func TestListStart(t *testing.T) {
	keyBytes := "-./09AZ_az"
	others := " !+:@[\\^`{~\x00\x7f\xc3\xff"
	r := mrand.New(mrand.NewPCG(1, 2)) //nolint:gosec // seeded so a failure can be replayed
	word := func(alphabet string, n int) string {
		var b strings.Builder
		for range n {
			b.WriteByte(alphabet[r.IntN(len(alphabet))])
		}
		return b.String()
	}
	keys := []string{strings.Repeat("a", storage.MaxKeyLen), strings.Repeat("a", storage.MaxKeyLen-1) + "b"}
	for range 3000 {
		keys = append(keys, word(keyBytes, 1+r.IntN(4)))
	}
	selectors := []string{"", "\xff", "a\xff", "é", "a b", strings.Repeat("a", storage.MaxKeyLen+1), strings.Repeat("a", storage.MaxKeyLen) + "\xff", strings.Repeat("a", storage.MaxKeyLen-1) + "\xff"}
	for range 3000 {
		selectors = append(selectors, word(keyBytes+others, r.IntN(5)))
	}
	for _, s := range selectors {
		got, ok := listStart(s)
		for _, k := range keys {
			if want := k > s; want != (ok && k > got) {
				t.Fatalf("listStart(%q) = %q, %v: key %q sorts after the selector: %v, after the result: %v", s, got, ok, k, want, ok && k > got)
			}
		}
		if ok && strings.ContainsFunc(got, func(c rune) bool { return c < ' ' || c > '~' }) {
			t.Fatalf("listStart(%q) = %q, which is not printable ASCII", s, got)
		}
	}
}

func TestIsETag(t *testing.T) {
	for v, want := range map[string]bool{
		`"d41d8cd98f00b204e9800998ecf8427e"`:   true,
		`"9b2cf535f27731c974343645a3985328-3"`: true,
		`""`:                                   true,
		``:                                     false,
		`*`:                                    false,
		`"`:                                    false,
		`W/"abc"`:                              false,
		`"abc"x`:                               false,
		`"a"b"`:                                false,
		"\"a\r\nb\"":                           false,
		`"a b"`:                                false,
		"\"\xff\"":                             false,
		`not-a-version`:                        false,
	} {
		if got := isETag(v); got != want {
			t.Errorf("isETag(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestContentRange(t *testing.T) {
	for _, tc := range []struct {
		h            string
		start, total int64
		ok           bool
	}{
		{"bytes 0-42/43", 0, 43, true},
		{"bytes 5-9/10", 5, 10, true},
		{fmt.Sprintf("bytes %d-%d/%d", int64(math.MaxInt64-2), int64(math.MaxInt64-2), int64(math.MaxInt64)), math.MaxInt64 - 2, math.MaxInt64, true},
		{"", 0, 0, false},
		{"bytes */10", 0, 0, false},
		{"bytes 0--1/0", 0, 0, false},
		{"bytes 5-4/10", 0, 0, false},
		{"bytes 0-10/10", 0, 0, false},
	} {
		start, total, err := contentRange(tc.h)
		if (err == nil) != tc.ok || tc.ok && (start != tc.start || total != tc.total) {
			t.Errorf("contentRange(%q) = %d, %d, %v; want %d, %d, ok %v", tc.h, start, total, err, tc.start, tc.total, tc.ok)
		}
	}
}

// TestCallsWithoutService checks what every call decides before it sends
// anything: invalid keys and ended contexts, against a store whose every
// request fails the test.
func TestCallsWithoutService(t *testing.T) {
	noRequest := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		return nil, errors.New("no request expected")
	})
	s, err := newStore(Config{Bucket: "b", Region: testRegion, Credentials: devCredentials(), Endpoint: "http://127.0.0.1:1", UsePathStyle: true, SpoolDir: t.TempDir()}, &http.Client{Transport: noRequest})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, version := range []string{"", "*", `W/"x"`, "\"a\"b", "x"} {
		v, err := s.Replace(ctx, "k", version, strings.NewReader("body"))
		if !errors.Is(err, storage.ErrPreconditionFailed) || v != "" {
			t.Errorf("Replace with version %q = %q, %v; want ErrPreconditionFailed and no request", version, v, err)
		}
	}
	if _, err := s.List(ctx, "é", "", 10); err != nil {
		t.Errorf("List with a prefix no key can have: %v", err)
	}
	if _, err := s.List(ctx, "", "\xff", 10); err != nil {
		t.Errorf("List after every key: %v", err)
	}
	old := maxBody
	maxBody = 10
	defer func() { maxBody = old }()
	if _, err := s.Create(ctx, "k", strings.NewReader(strings.Repeat("x", 11))); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Errorf("Create of a body over the limit: %v, want ErrInvalidArgument", err)
	}
	if entries, _ := os.ReadDir(s.spoolDir); len(entries) > 0 {
		t.Errorf("spool directory holds %d files after the writes, want none", len(entries))
	}
}

// TestSpoolFileUnlinked checks that the spool file has no name while the body
// is being read, so a process killed during a write leaves no file behind.
func TestSpoolFileUnlinked(t *testing.T) {
	noService := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		closeBody(r)
		return nil, errors.New("no service")
	})
	s, err := newStore(Config{Bucket: "b", Region: testRegion, Credentials: devCredentials(), Endpoint: "http://127.0.0.1:1", UsePathStyle: true, SpoolDir: t.TempDir()}, &http.Client{Transport: noService})
	if err != nil {
		t.Fatal(err)
	}
	src := strings.NewReader("body")
	reads := 0
	_, _ = s.Create(context.Background(), "k", readerFunc(func(p []byte) (int, error) {
		reads++
		if entries, err := os.ReadDir(s.spoolDir); err != nil || len(entries) > 0 {
			t.Errorf("spool directory holds %d entries while the body is read (%v), want none", len(entries), err)
		}
		return src.Read(p)
	}))
	if reads == 0 {
		t.Fatal("the body was never read")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestStoredFormat checks the format the package comment documents: the
// caller's bytes, "SKYLS3T1" and 24 random bytes that also appear, in hex,
// as the skylite-id metadata; and that writes of the same bytes get different
// tokens.
func TestStoredFormat(t *testing.T) {
	b := newBucket(t)
	s := b.store(t)
	ctx := context.Background()
	v1, err := s.Create(ctx, "k", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	data, meta := b.raw(t, "k")
	if len(data) != 5+trailerLen || string(data[:5]) != "hello" || string(data[5:5+len(trailerMagic)]) != trailerMagic {
		t.Fatalf("stored bytes = %q, want the body, %q and %d random bytes", data, trailerMagic, idLen)
	}
	if id := hex.EncodeToString(data[5+len(trailerMagic):]); meta[metaID] != id {
		t.Fatalf("metadata %s = %q, want the trailer's random bytes %q", metaID, meta[metaID], id)
	}
	v2, err := s.Replace(ctx, "k", v1, strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if v1 == v2 {
		t.Fatalf("two writes of the same bytes returned the same token %q", v1)
	}
	if info, err := s.Head(ctx, "k"); err != nil || info != (storage.Info{Size: 5, Version: v2}) {
		t.Fatalf("Head = %+v, %v; want size 5 and version %q", info, err, v2)
	}
	if entries, err := os.ReadDir(s.spoolDir); err != nil || len(entries) > 0 {
		t.Fatalf("spool directory holds %d files after the writes (%v), want none", len(entries), err)
	}
}

// TestForeignObjects checks that objects this backend did not write are
// rejected, not misread: without the metadata, or shorter than the trailer.
func TestForeignObjects(t *testing.T) {
	b := newBucket(t)
	s := b.store(t)
	ctx := context.Background()
	put := func(key, body string, meta map[string]string) {
		t.Helper()
		if _, err := b.admin.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(b.name), Key: aws.String(key), Body: strings.NewReader(body), Metadata: meta}); err != nil {
			t.Fatal(err)
		}
	}
	put("plain", strings.Repeat("p", 100), nil)
	put("short", "tiny", map[string]string{metaID: strings.Repeat("ab", idLen)})
	for _, key := range []string{"plain", "short"} {
		_, err := s.Head(ctx, key)
		expectForeign(t, "Head", key, err)
		_, _, err = s.Get(ctx, key)
		expectForeign(t, "Get", key, err)
		_, _, err = s.GetRange(ctx, key, 1, 2)
		expectForeign(t, "GetRange", key, err)
	}
	if _, err := s.List(ctx, "", "", 10); !errors.Is(err, errForeign) {
		t.Fatalf("List over an object shorter than the trailer: %v, want an error", err)
	}
}

func expectForeign(t *testing.T, op, key string, err error) {
	t.Helper()
	if !errors.Is(err, errForeign) {
		t.Fatalf("%s(%q) of an object this backend did not write: %v, want errForeign", op, key, err)
	}
	for _, sentinel := range []error{storage.ErrNotFound, storage.ErrPreconditionFailed, storage.ErrInvalidKey, storage.ErrInvalidArgument, storage.ErrOutcomeUnknown} {
		if errors.Is(err, sentinel) {
			t.Fatalf("%s(%q) error %v wraps %v", op, key, err, sentinel)
		}
	}
}

// TestSizeLimit checks the documented single-request limit, lowered for the
// test: a body one byte over it fails with ErrInvalidArgument, sends nothing
// and leaves the key free; a body at the limit is written.
func TestSizeLimit(t *testing.T) {
	b := newBucket(t)
	f := newFaults()
	s := b.storeVia(t, f)
	old := maxBody
	maxBody = 1000
	defer func() { maxBody = old }()
	ctx := context.Background()
	_, err := s.Create(ctx, "k", strings.NewReader(strings.Repeat("x", 1001)))
	if !errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("Create of 1001 bytes over a limit of 1000: %v, want ErrInvalidArgument", err)
	}
	if n := f.count(http.MethodPut); n != 0 {
		t.Fatalf("the rejected Create sent %d PUT requests", n)
	}
	v, err := s.Create(ctx, "k", strings.NewReader(strings.Repeat("y", 1000)))
	if err != nil {
		t.Fatal(err)
	}
	if info, err := s.Head(ctx, "k"); err != nil || info != (storage.Info{Size: 1000, Version: v}) {
		t.Fatalf("Head = %+v, %v", info, err)
	}
}

// TestEmulatorConditionsUnderConcurrency is evidence about the service, not
// the backend: many independent clients race conditional writes on one key,
// and exactly one must win each round, with its own bytes stored.
func TestEmulatorConditionsUnderConcurrency(t *testing.T) {
	const clients, rounds = 32, 30
	b := newBucket(t)
	stores := make([]*Store, clients)
	for i := range stores {
		stores[i] = b.store(t)
	}
	ctx := context.Background()
	race := func(write func(i int) (string, error)) (int, string) {
		t.Helper()
		versions, errs := make([]string, clients), make([]error, clients)
		start := make(chan struct{})
		done := make(chan struct{})
		for i := range clients {
			go func() {
				<-start
				versions[i], errs[i] = write(i)
				done <- struct{}{}
			}()
		}
		close(start)
		for range clients {
			<-done
		}
		winner := -1
		for i, err := range errs {
			switch {
			case err == nil && winner >= 0:
				t.Fatalf("writers %d and %d both succeeded", winner, i)
			case err == nil:
				winner = i
			case !errors.Is(err, storage.ErrPreconditionFailed):
				t.Fatalf("writer %d: %v", i, err)
			}
		}
		if winner < 0 {
			t.Fatal("no writer succeeded")
		}
		return winner, versions[winner]
	}
	check := func(key string, winner int, version string) {
		t.Helper()
		body, info, err := stores[(winner+1)%clients].Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(body)
		_ = body.Close()
		if string(got) != fmt.Sprint("writer ", winner) || info.Version != version {
			t.Fatalf("%s holds %q at %q, want the winner's %q at %q", key, got, info.Version, fmt.Sprint("writer ", winner), version)
		}
	}
	for r := range rounds {
		key := fmt.Sprint("c", r)
		w, v := race(func(i int) (string, error) {
			return stores[i].Create(ctx, key, strings.NewReader(fmt.Sprint("writer ", i)))
		})
		check(key, w, v)
	}
	v, err := stores[0].Create(ctx, "r", strings.NewReader("initial"))
	if err != nil {
		t.Fatal(err)
	}
	seen := []string{v}
	for range rounds {
		from := v
		var w int
		w, v = race(func(i int) (string, error) {
			return stores[i].Replace(ctx, "r", from, strings.NewReader(fmt.Sprint("writer ", i)))
		})
		if slices.Contains(seen, v) {
			t.Fatalf("Replace returned the earlier token %q", v)
		}
		seen = append(seen, v)
		check("r", w, v)
	}
}
