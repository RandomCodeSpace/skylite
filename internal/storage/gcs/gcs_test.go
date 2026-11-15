package gcs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"google.golang.org/api/googleapi"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

var bg = context.Background()

// The tests below without an emulator check the pieces the outcome rule and
// listing rest on. The rest put a fault-injecting http.RoundTripper between
// the client and the emulator named by SKYLITE_TEST_GCS_ENDPOINT, and skip
// without one.

func TestCommits(t *testing.T) {
	for _, tc := range []struct {
		method, url, contentRange string
		want                      bool
	}{
		{"GET", "/download/storage/v1/b/b/o/k?alt=media", "", false},
		{"GET", "/storage/v1/b/b/o", "", false},
		{"HEAD", "/b/k", "", false},
		{"POST", "/upload/storage/v1/b/b/o?uploadType=resumable&name=k", "", false},
		{"POST", "/upload/storage/v1/b/b/o?uploadType=multipart&name=k", "", true},
		{"POST", "/upload/storage/v1/b/b/o?uploadType=media&name=k", "", true},
		{"POST", "/upload/storage/v1/b/b/o?uploadType=resumable&upload_id=x", "bytes 0-16777215/*", false},
		{"POST", "/upload/storage/v1/b/b/o?uploadType=resumable&upload_id=x", "bytes 16777216-16777216/16777217", true},
		{"POST", "/upload/storage/v1/b/b/o?uploadType=resumable&upload_id=x", "bytes */16777216", true},
		{"PUT", "/upload/storage/v1/b/b/o?upload_id=x", "bytes 0-16777215/*", false},
		{"PUT", "/upload/storage/v1/b/b/o?upload_id=x", "bytes 16777216-16777216/16777217", true},
		{"PUT", "/upload/storage/v1/b/b/o?upload_id=x", "bytes */16777216", true},
		{"PUT", "/upload/storage/v1/b/b/o?upload_id=x", "", true},
		{"DELETE", "/storage/v1/b/b/o/k", "", true},
		{"PATCH", "/storage/v1/b/b/o/k", "", true},
	} {
		r, err := http.NewRequest(tc.method, "http://h"+tc.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		if tc.contentRange != "" {
			r.Header.Set("Content-Range", tc.contentRange)
		}
		if got := commits(r); got != tc.want {
			t.Errorf("commits(%s %s, Content-Range %q) = %v, want %v", tc.method, tc.url, tc.contentRange, got, tc.want)
		}
	}
}

// TestWriteError checks the classification of a failed write: 412 is a
// precondition failure, other 4xx but 408 and a connection that was never made
// mean no effect, and everything else after the commit request started is an
// unknown outcome. Before it started every error is definite.
func TestWriteError(t *testing.T) {
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	reset := &urlError{err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}}
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		err     error
		sent    bool
		is      []error
		unknown bool
	}{
		{"412", bg, &googleapi.Error{Code: 412}, true, []error{storage.ErrPreconditionFailed}, false},
		{"412 not sent", bg, &googleapi.Error{Code: 412}, false, []error{storage.ErrPreconditionFailed}, false},
		{"403", bg, &googleapi.Error{Code: 403}, true, nil, false},
		{"429", bg, &googleapi.Error{Code: 429}, true, nil, false},
		{"408", bg, &googleapi.Error{Code: 408}, true, nil, true},
		{"500", bg, &googleapi.Error{Code: 500}, true, nil, true},
		{"503", bg, &googleapi.Error{Code: 503}, true, nil, true},
		{"503 before the commit", bg, &googleapi.Error{Code: 503}, false, nil, false},
		{"reset", bg, reset, true, nil, true},
		{"reset before the commit", bg, reset, false, nil, false},
		{"dial", bg, &urlError{err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}, true, nil, false},
		{"cancelled", cancelled, context.Canceled, true, []error{context.Canceled}, true},
		{"cancelled before the commit", cancelled, context.Canceled, false, []error{context.Canceled}, false},
	} {
		err := writeError(tc.ctx, "create", "k", tc.err, tc.sent)
		var se *storage.Error
		if !errors.As(err, &se) || se.Op != "create" || se.Key != "k" {
			t.Errorf("%s: %v is not the *storage.Error of create \"k\"", tc.name, err)
		}
		if got := errors.Is(err, storage.ErrOutcomeUnknown); got != tc.unknown {
			t.Errorf("%s: %v wraps ErrOutcomeUnknown = %v, want %v", tc.name, err, got, tc.unknown)
		}
		for _, want := range tc.is {
			if !errors.Is(err, want) {
				t.Errorf("%s: %v does not wrap %v", tc.name, err, want)
			}
		}
		if !slices.Contains(tc.is, storage.ErrPreconditionFailed) && errors.Is(err, storage.ErrPreconditionFailed) {
			t.Errorf("%s: %v wraps ErrPreconditionFailed", tc.name, err)
		}
	}
}

// urlError is a stand-in *url.Error whose text carries a resumable session URL.
type urlError struct{ err error }

func (u *urlError) Error() string {
	return `Put "http://h/upload/storage/v1/b/b/o?uploadType=resumable&upload_id=secret-session": ` + u.err.Error()
}
func (u *urlError) Unwrap() error { return u.err }

func TestRedact(t *testing.T) {
	err := writeError(bg, "create", "k", &urlError{err: syscall.ECONNRESET}, true)
	if strings.Contains(err.Error(), "secret-session") || strings.Contains(err.Error(), "upload_id") {
		t.Fatalf("error %q contains the upload session", err)
	}
	if !strings.Contains(err.Error(), "/upload/storage/v1/b/b/o?REDACTED") {
		t.Fatalf("error %q lost the URL's path", err)
	}
}

func TestListStart(t *testing.T) {
	for _, tc := range []struct {
		after, want string
		ok          bool
	}{
		{"", "", true},
		{"a/b", "a/b", true},
		{"a\xff", "b", true},
		{"ab\xc3\xa9z", "ac", true},
		{"\xff", "", false},
		{"é", "", false},
		{"a\x7f\x7f\xff", "b", true},
		{"\x7f\xff", "", false},
	} {
		got, ok := listStart(tc.after)
		if got != tc.want || ok != tc.ok {
			t.Errorf("listStart(%q) = %q, %v; want %q, %v", tc.after, got, ok, tc.want, tc.ok)
		}
	}
}

// noIO is a transport that fails the test on any request.
func noIO(t *testing.T) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		return nil, errors.New("no I/O expected")
	})
}

// TestReplaceTokens checks that a Replace whose token cannot be a generation
// this store returned fails its precondition without a request and without
// reading its body: a token is compared as a string, so "007" is not "7", and
// generation 0 would mean "does not exist".
func TestReplaceTokens(t *testing.T) {
	s := openStore(t, Config{Bucket: "b", Endpoint: "http://127.0.0.1:1/storage/v1/"}, noIO(t))
	body := iotest.ErrReader(errors.New("the body was read"))
	for _, v := range []string{"", "0", "-1", "007", "+7", "7x", "x", "9223372036854775808"} {
		got, err := s.Replace(bg, "k", v, body)
		if !errors.Is(err, storage.ErrPreconditionFailed) || got != "" {
			t.Errorf("Replace with token %q = %q, %v; want ErrPreconditionFailed", v, got, err)
		}
	}
}

// TestEmulatorHostWithCredentials checks that Open refuses to send
// credentials while the client would redirect them to STORAGE_EMULATOR_HOST.
func TestEmulatorHostWithCredentials(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")
	if _, err := Open(bg, Config{Bucket: "b"}); err == nil {
		t.Fatal("Open with STORAGE_EMULATOR_HOST set and no endpoint succeeded")
	}
}

// kind names the role of a request in the JSON API.
func kind(r *http.Request) string {
	q := r.URL.Query()
	cr := r.Header.Get("Content-Range")
	switch {
	case cr != "" && strings.HasSuffix(cr, "/*"):
		return "chunk"
	case cr != "":
		return "final"
	case r.Method == http.MethodPost && q.Get("uploadType") == "resumable":
		return "session"
	case r.Method == http.MethodPost:
		return "single"
	case r.Method == http.MethodDelete:
		return "delete"
	case q.Get("alt") == "media":
		return "media"
	case strings.HasSuffix(r.URL.Path, "/o"):
		return "list"
	}
	return "meta"
}

// commitKinds are the requests that can make a mutation take effect.
var commitKinds = []string{"single", "final", "delete"}

// faults is an http.RoundTripper between a Store and the emulator. It records
// the kind of every request and hands each one to hook, if set, instead of
// forwarding it.
type faults struct {
	next  http.RoundTripper
	mu    sync.Mutex
	hook  func(*http.Request) (*http.Response, error)
	kinds []string
}

func (f *faults) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	h := f.hook
	f.kinds = append(f.kinds, kind(r))
	f.mu.Unlock()
	if h != nil {
		return h(r)
	}
	return f.next.RoundTrip(r)
}

// set installs hook (nil forwards everything) and forgets the requests seen.
func (f *faults) set(hook func(*http.Request) (*http.Response, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook, f.kinds = hook, nil
}

// count returns the number of requests of the given kinds since the last set.
func (f *faults) count(kinds ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, k := range f.kinds {
		if slices.Contains(kinds, k) {
			n++
		}
	}
	return n
}

type fault func(r *http.Request, next http.RoundTripper) (*http.Response, error)

// once applies fault to the first request of one of kinds and forwards all
// others.
func (f *faults) once(fault fault, kinds ...string) {
	var done atomic.Bool
	f.set(func(r *http.Request) (*http.Response, error) {
		if slices.Contains(kinds, kind(r)) && done.CompareAndSwap(false, true) {
			return fault(r, f.next)
		}
		return f.next.RoundTrip(r)
	})
}

var errReset = &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}

func closeBody(r *http.Request) {
	if r.Body != nil {
		_ = r.Body.Close()
	}
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// lose forwards the request and loses the response: the service applied it,
// the client sees a connection reset.
func lose(r *http.Request, next http.RoundTripper) (*http.Response, error) {
	resp, err := next.RoundTrip(r)
	if err == nil {
		drain(resp)
	}
	return nil, errReset
}

// refuse fails the request before anything is sent, as a refused connection.
func refuse(r *http.Request, _ http.RoundTripper) (*http.Response, error) {
	closeBody(r)
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

func injected(r *http.Request, code int) *http.Response {
	return &http.Response{
		StatusCode: code, Status: fmt.Sprint(code, " ", http.StatusText(code)), Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:  http.Header{"Content-Type": {"application/json"}},
		Body:    io.NopCloser(strings.NewReader(fmt.Sprintf(`{"error":{"code":%d,"message":"injected"}}`, code))),
		Request: r,
	}
}

// status answers code without forwarding the request.
func status(code int) fault {
	return func(r *http.Request, _ http.RoundTripper) (*http.Response, error) {
		closeBody(r)
		return injected(r, code), nil
	}
}

// statusAfter forwards the request and answers code in place of the service's
// response.
func statusAfter(code int) fault {
	return func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		resp, err := next.RoundTrip(r)
		if err == nil {
			drain(resp)
		}
		return injected(r, code), nil
	}
}

// stall forwards the request and holds the response until the call's context
// ends.
func stall(r *http.Request, next http.RoundTripper) (*http.Response, error) {
	resp, err := next.RoundTrip(r)
	if err == nil {
		drain(resp)
	}
	<-r.Context().Done()
	return nil, r.Context().Err()
}

// cut sends the first 64 KiB of the request body and then breaks the
// connection.
func cut(r *http.Request, next http.RoundTripper) (*http.Response, error) {
	r2 := r.Clone(r.Context())
	r2.Body = &cutBody{rc: r.Body, left: 64 << 10}
	return next.RoundTrip(r2)
}

type cutBody struct {
	rc   io.ReadCloser
	left int
}

func (c *cutBody) Read(p []byte) (int, error) {
	if c.left <= 0 {
		return 0, &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
	}
	n, err := c.rc.Read(p[:min(len(p), c.left)])
	c.left -= n
	return n, err
}

func (c *cutBody) Close() error { return c.rc.Close() }

// fixture opens a store whose requests pass through faults and a second,
// clean store on the same bucket for checking what the service holds.
func fixture(t *testing.T) (*faults, *Store, *Store) {
	t.Helper()
	ep := endpoint(t)
	bucket := newBucket(t)
	f := &faults{next: newTransport(t)}
	s := openStore(t, Config{Bucket: bucket, Endpoint: ep}, f)
	check := openStore(t, Config{Bucket: bucket, Endpoint: ep}, newTransport(t))
	t.Cleanup(func() { noClientGoroutines(t) })
	return f, s, check
}

// noClientGoroutines fails t if a goroutine of the client library is still
// running once every call has returned. Such a goroutine may take a moment to
// see that its upload is over, so it waits up to two seconds.
func noClientGoroutines(t *testing.T) {
	t.Helper()
	var stacks string
	for range 40 {
		buf := make([]byte, 1<<20)
		stacks = ""
		for g := range strings.SplitSeq(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(g, "cloud.google.com/go/storage.") || strings.Contains(g, "google.golang.org/api/internal/gensupport.") {
				stacks += g + "\n\n"
			}
		}
		if stacks == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("client goroutines outlived the calls that started them:\n%s", stacks)
}

// data returns n pseudo-random bytes made from tag.
func data(tag string, n int) []byte {
	b := make([]byte, n)
	seed := sha256.Sum256([]byte(tag))
	_, _ = rand.NewChaCha8(seed).Read(b)
	return b
}

func expectObject(t *testing.T, s *Store, key string, want []byte) string {
	t.Helper()
	body, info, err := s.Get(bg, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer func() { _ = body.Close() }()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("reading %q: %v", key, err)
	}
	if !bytes.Equal(got, want) || info.Size != int64(len(want)) {
		t.Fatalf("%q holds %d bytes (Info %+v), want %d other bytes", key, len(got), info, len(want))
	}
	return info.Version
}

func expectAbsent(t *testing.T, s *Store, key string) {
	t.Helper()
	if _, err := s.Head(bg, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Head(%q) = %v, want ErrNotFound", key, err)
	}
}

func expectUnknown(t *testing.T, err error, op, key string) {
	t.Helper()
	var se *storage.Error
	if !errors.As(err, &se) || se.Op != op || se.Key != key || !errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("%s %q error = %v, want the *storage.Error of %s %q wrapping ErrOutcomeUnknown", op, key, err, op, key)
	}
	if errors.Is(err, storage.ErrPreconditionFailed) || errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("%s %q error = %v wraps another sentinel as well", op, key, err)
	}
	if strings.Contains(err.Error(), "upload_id") {
		t.Fatalf("%s %q error %q names an upload session", op, key, err)
	}
}

func expectDefinite(t *testing.T, err error, op, key string) {
	t.Helper()
	var se *storage.Error
	if !errors.As(err, &se) || se.Op != op || se.Key != key {
		t.Fatalf("%s %q error = %v, want a *storage.Error of %s %q", op, key, err, op, key)
	}
	if errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("%s %q error = %v wraps ErrOutcomeUnknown, but the mutation had no effect", op, key, err)
	}
}

// mutation is a Create, Replace or Delete prepared on a fresh key.
type mutation struct {
	name, op string
	// prepare writes what the key holds before the mutation and returns the
	// mutation, which runs through s, and the content it would leave (nil
	// for a Delete).
	prepare func(t *testing.T, s *Store, key string) (run func(ctx context.Context) error, old, new []byte)
}

func writeOf(op string, n int) mutation {
	return mutation{fmt.Sprintf("%s of %d bytes", op, n), op, func(t *testing.T, s *Store, key string) (func(context.Context) error, []byte, []byte) {
		t.Helper()
		nw := data(key+" new", n)
		if op == "create" {
			return func(ctx context.Context) error {
				v, err := s.Create(ctx, key, bytes.NewReader(nw))
				noTokenOnError(t, v, err)
				return err
			}, nil, nw
		}
		old := data(key+" old", 11)
		v, err := s.Create(bg, key, bytes.NewReader(old))
		if err != nil {
			t.Fatal(err)
		}
		return func(ctx context.Context) error {
			v, err := s.Replace(ctx, key, v, bytes.NewReader(nw))
			noTokenOnError(t, v, err)
			return err
		}, old, nw
	}}
}

func noTokenOnError(t *testing.T, v string, err error) {
	if err != nil && v != "" {
		t.Errorf("a write failed with %v and returned the token %q", err, v)
	}
}

var deletion = mutation{"delete", "delete", func(t *testing.T, s *Store, key string) (func(context.Context) error, []byte, []byte) {
	old := data(key+" old", 11)
	if _, err := s.Create(bg, key, bytes.NewReader(old)); err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context) error { return s.Delete(ctx, key) }, old, nil
}}

// mutations cover each commit request: a single-request upload, the last
// request of a resumable session, which here carries 1 MiB, and a DELETE.
var mutations = []mutation{
	writeOf("create", 11), writeOf("create", 1<<20), writeOf("create", ChunkSize+1<<20),
	writeOf("replace", 1<<20), writeOf("replace", ChunkSize+1<<20),
	deletion,
}

// expectState checks that key holds content, or is missing if content is nil.
func expectState(t *testing.T, s *Store, key string, content []byte) {
	t.Helper()
	if content == nil {
		expectAbsent(t, s, key)
	} else {
		expectObject(t, s, key, content)
	}
}

// TestCommitFaults injects one fault into the commit request of every kind of
// mutation. The contract: after its commit request may have reached the
// service, a mutation without a definite answer returns an error wrapping
// ErrOutcomeUnknown (a lost response, a reset or a stall past the deadline, a
// 500 or 503 whether the service applied the request or not, a body cut in the
// middle); one whose request was never sent returns an error without it and
// has no effect. In every case the client must not retry the commit request:
// a retry of an applied Create or Replace would fail its precondition and hide
// the success, and a retried Delete could remove a later object.
func TestCommitFaults(t *testing.T) {
	f, s, check := fixture(t)
	type result int
	const (
		unknownApplied result = iota // ErrOutcomeUnknown, and the service applied the mutation
		unknownNotApplied
		definite // an error without ErrOutcomeUnknown and no effect
	)
	for _, fc := range []struct {
		name  string
		fault fault
		want  result
	}{
		{"response lost after the effect", lose, unknownApplied},
		{"request not sent", refuse, definite},
		{"500 before the effect", status(500), unknownNotApplied},
		{"500 after the effect", statusAfter(500), unknownApplied},
		{"503 before the effect", status(503), unknownNotApplied},
		{"503 after the effect", statusAfter(503), unknownApplied},
		{"slow response past the deadline", stall, unknownApplied},
		{"connection cut in the middle of the body", cut, unknownNotApplied},
	} {
		for i, m := range mutations {
			if fc.name == "connection cut in the middle of the body" && (m.op == "delete" || strings.HasSuffix(m.name, " 11 bytes")) {
				continue // no body long enough to cut
			}
			t.Run(fc.name+"/"+m.name, func(t *testing.T) {
				key := fmt.Sprintf("k%d-%s", i, strings.ReplaceAll(fc.name, " ", "-"))
				f.set(nil)
				run, old, nw := m.prepare(t, s, key)
				f.once(fc.fault, commitKinds...)
				ctx, cancel := context.WithTimeout(bg, 3*time.Second)
				defer cancel()
				if fc.name != "slow response past the deadline" {
					ctx = bg
				}
				err := run(ctx)
				if n := f.count(commitKinds...); n != 1 {
					t.Fatalf("%d commit requests, want 1: the client must not retry a commit request", n)
				}
				switch fc.want {
				case unknownApplied:
					expectUnknown(t, err, m.op, key)
					expectState(t, check, key, nw)
				case unknownNotApplied:
					expectUnknown(t, err, m.op, key)
					expectState(t, check, key, old)
				case definite:
					expectDefinite(t, err, m.op, key)
					expectState(t, check, key, old)
				}
				if fc.name == "slow response past the deadline" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error %v does not wrap the context's error", err)
				}
			})
		}
	}
}

// TestRetries checks which requests the client retries on its own. It may
// retry those that publish nothing and carry none of the body: opening a
// resumable session, and reads. Each fails once, is retried, and the call
// succeeds. It must not retry a chunk of a resumable session: the emulator
// stores a retried chunk that it had already stored a second time, so a chunk
// that fails fails the write, definitely and with no effect, and a later
// Create of the key succeeds.
func TestRetries(t *testing.T) {
	f, s, check := fixture(t)
	body := data("staged", ChunkSize+1)
	for _, fc := range []struct {
		name  string
		fault fault
	}{
		{"503", status(503)},
		{"response lost", lose},
		{"connection cut in the body", cut},
	} {
		for _, k := range []string{"session", "chunk"} {
			if k == "session" && fc.name == "connection cut in the body" {
				continue // the request that opens a session has no body to cut
			}
			t.Run(fc.name+" on "+k, func(t *testing.T) {
				key := strings.ReplaceAll(fc.name+"-"+k, " ", "-")
				f.once(fc.fault, k)
				v, err := s.Create(bg, key, bytes.NewReader(body))
				if k == "chunk" {
					expectDefinite(t, err, "create", key)
					if n, c := f.count(k), f.count(commitKinds...); n != 1 || c != 0 {
						t.Fatalf("%d chunk and %d commit requests, want 1 and 0: the client must not retry a chunk", n, c)
					}
					expectAbsent(t, check, key)
					v, err = s.Create(bg, key, bytes.NewReader(body))
				} else if n := f.count(k); n != 2 {
					t.Fatalf("%d %s requests, want 2: one failed and one retried", n, k)
				}
				if err != nil {
					t.Fatalf("Create: %v", err)
				}
				if got := expectObject(t, check, key, body); got != v {
					t.Fatalf("Create returned token %q, the object has %q", v, got)
				}
			})
		}
	}
	v, err := s.Create(bg, "read", bytes.NewReader(body[:100]))
	if err != nil {
		t.Fatal(err)
	}
	for _, fc := range []struct {
		name  string
		fault fault
	}{{"503", status(503)}, {"response lost", lose}} {
		for _, k := range []string{"meta", "media", "list"} {
			t.Run(fc.name+" on "+k, func(t *testing.T) {
				f.once(fc.fault, k)
				switch k {
				case "meta":
					if info, err := s.Head(bg, "read"); err != nil || info.Version != v || info.Size != 100 {
						t.Fatalf("Head = %+v, %v", info, err)
					}
				case "media":
					expectObject(t, s, "read", body[:100])
				case "list":
					if e, err := s.List(bg, "read", "", 10); err != nil || len(e) != 1 || e[0].Key != "read" {
						t.Fatalf("List = %+v, %v", e, err)
					}
				}
				if n := f.count(k); n != 2 {
					t.Fatalf("%d %s requests, want 2", n, k)
				}
			})
		}
	}
}

// TestReadFaults checks that a read past its deadline fails with the
// context's error, and that a body whose connection breaks after the object
// was replaced fails instead of returning bytes of the new version.
func TestReadFaults(t *testing.T) {
	f, s, check := fixture(t)
	old := data("old", 4<<20)
	v, err := s.Create(bg, "k", bytes.NewReader(old))
	if err != nil {
		t.Fatal(err)
	}
	f.once(stall, "meta")
	ctx, cancel := context.WithTimeout(bg, time.Second)
	_, err = s.Head(ctx, "k")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("Head past its deadline = %v, want context.DeadlineExceeded", err)
	}
	// The first download response breaks after 64 KiB; by then the object has
	// been replaced, so the client's resumed download must not find it.
	replaced := make(chan struct{})
	f.once(func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		resp, err := next.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		resp.Body = &brokenAfter{rc: resp.Body, left: 64 << 10, before: func() { <-replaced }}
		return resp, nil
	}, "media")
	body, info, err := s.Get(bg, "k")
	if err != nil || info.Version != v {
		t.Fatalf("Get = %+v, %v", info, err)
	}
	defer func() { _ = body.Close() }()
	if _, err := check.Replace(bg, "k", v, bytes.NewReader(data("new", 4<<20))); err != nil {
		t.Fatal(err)
	}
	close(replaced)
	got, err := io.ReadAll(body)
	if err == nil {
		t.Fatalf("a body whose version was replaced while its connection broke returned %d bytes and no error", len(got))
	}
	if !bytes.HasPrefix(old, got) {
		t.Fatalf("the body returned %d bytes that are not all of the version it was opened at", len(got))
	}
	if n := f.count("media"); n != 2 {
		t.Fatalf("%d downloads, want 2: the first and the client's resumed one, at the old generation", n)
	}
}

// brokenAfter returns left bytes of rc, waits for before, then fails.
type brokenAfter struct {
	rc     io.ReadCloser
	left   int
	before func()
}

func (b *brokenAfter) Read(p []byte) (int, error) {
	if b.left <= 0 {
		b.before()
		return 0, errReset
	}
	n, err := b.rc.Read(p[:min(len(p), b.left)])
	b.left -= n
	return n, err
}

func (b *brokenAfter) Close() error { return b.rc.Close() }

// TestShortPages makes the service return pages of one or two entries. The
// contract requires List to keep requesting until it has limit entries or the
// listing ends, and to list nothing at or before startAfter.
func TestShortPages(t *testing.T) {
	f, s, _ := fixture(t)
	var all []string
	for i := range 25 {
		k := fmt.Sprintf("p/%02d", i)
		if _, err := s.Create(bg, k, strings.NewReader(k)); err != nil {
			t.Fatal(err)
		}
		all = append(all, k)
	}
	if _, err := s.Create(bg, "q", strings.NewReader("q")); err != nil {
		t.Fatal(err)
	}
	for _, page := range []int{1, 2} {
		f.set(func(r *http.Request) (*http.Response, error) {
			if kind(r) == "list" {
				r = r.Clone(r.Context())
				q := r.URL.Query()
				q.Set("maxResults", fmt.Sprint(page))
				r.URL.RawQuery = q.Encode()
			}
			return f.next.RoundTrip(r)
		})
		for _, tc := range []struct {
			after string
			limit int
			want  []string
		}{
			{"", 7, all[:7]},
			{"p/06", 7, all[7:14]},
			{"p/20", 7, all[21:]},
			{"p/24", 7, nil},
			{"", 1000, all},
		} {
			entries, err := s.List(bg, "p/", tc.after, tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(entries))
			for i, e := range entries {
				got[i] = e.Key
				if e.Size != int64(len(e.Key)) {
					t.Fatalf("entry %q has size %d", e.Key, e.Size)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("pages of %d: List(p/, %q, %d) = %q, want %q", page, tc.after, tc.limit, got, tc.want)
			}
		}
		if n := f.count("list"); n < 25/page {
			t.Fatalf("pages of %d: only %d list requests; the service's page size was not forced", page, n)
		}
	}
}

// TestChunkMultiples writes bodies that are an exact multiple of ChunkSize and
// one byte either side, which the portable suite does not, and checks the
// request plan: one commit request each, a single-request upload below
// ChunkSize bytes and a resumable session from ChunkSize on.
func TestChunkMultiples(t *testing.T) {
	f, s, check := fixture(t)
	for _, tc := range []struct {
		n             int
		single, final int
	}{
		{ChunkSize - 1, 1, 0},
		{ChunkSize, 0, 1},
		{ChunkSize + 1, 0, 1},
		{2*ChunkSize - 1, 0, 1},
		{2 * ChunkSize, 0, 1},
		{2*ChunkSize + 1, 0, 1},
	} {
		key := fmt.Sprint("m", tc.n)
		body := data(key, tc.n)
		f.set(nil)
		v, err := s.Create(bg, key, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Create of %d bytes: %v", tc.n, err)
		}
		if got := expectObject(t, check, key, body); got != v {
			t.Fatalf("%d bytes: Create returned token %q, the object has %q", tc.n, v, got)
		}
		if s, fi := f.count("single"), f.count("final"); s != tc.single || fi != tc.final {
			t.Fatalf("%d bytes: %d single-request uploads and %d final chunks, want %d and %d", tc.n, s, fi, tc.single, tc.final)
		}
		body = data(key+" replaced", tc.n)
		if v, err = s.Replace(bg, key, v, bytes.NewReader(body)); err != nil {
			t.Fatalf("Replace of %d bytes: %v", tc.n, err)
		}
		if got := expectObject(t, check, key, body); got != v {
			t.Fatalf("%d bytes: Replace returned token %q, the object has %q", tc.n, v, got)
		}
	}
}

// TestInterruptedStagedUpload stops a resumable upload after its first chunk
// has been sent, by a failing body and by cancellation. The contract: no
// effect, an error without ErrOutcomeUnknown, nothing visible, and a later
// Create of the key succeeds.
func TestInterruptedStagedUpload(t *testing.T) {
	f, s, check := fixture(t)
	errBody := errors.New("body failed")
	for _, how := range []string{"body fails", "cancelled"} {
		t.Run(how, func(t *testing.T) {
			key := strings.ReplaceAll(how, " ", "-")
			f.set(nil)
			ctx, cancel := context.WithCancel(bg)
			defer cancel()
			stop := iotest.ErrReader(errBody)
			if how == "cancelled" {
				stop = readerFunc(func([]byte) (int, error) { cancel(); return 0, nil })
			}
			v, err := s.Create(ctx, key, io.MultiReader(bytes.NewReader(data(key, ChunkSize+1<<20)), stop))
			expectDefinite(t, err, "create", key)
			if v != "" {
				t.Fatalf("a failed Create returned the token %q", v)
			}
			var be *storage.BodyError
			if how == "body fails" && (!errors.As(err, &be) || !errors.Is(be.Err, errBody)) {
				t.Fatalf("Create error %v does not hold the body's error", err)
			}
			if how == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("Create error %v does not wrap context.Canceled", err)
			}
			if f.count("chunk") == 0 || f.count(commitKinds...) != 0 {
				t.Fatalf("requests %q: want a chunk sent and no commit request", f.kinds)
			}
			expectAbsent(t, check, key)
			if e, err := check.List(bg, key, "", 10); err != nil || len(e) != 0 {
				t.Fatalf("List after the interrupted upload = %+v, %v", e, err)
			}
			body := data(key+" again", 10)
			if _, err := s.Create(bg, key, bytes.NewReader(body)); err != nil {
				t.Fatalf("Create after the interrupted upload: %v", err)
			}
			expectObject(t, check, key, body)
		})
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// holdCommits holds every commit request until n of them have arrived, so
// that n conditional writes meet at the service at once.
func (f *faults) holdCommits(t *testing.T, n int) {
	var arrived atomic.Int64
	all := make(chan struct{})
	f.set(func(r *http.Request) (*http.Response, error) {
		if slices.Contains(commitKinds, kind(r)) {
			if arrived.Add(1) == int64(n) {
				close(all)
			}
			select {
			case <-all:
			case <-time.After(30 * time.Second):
				t.Errorf("only %d of %d commit requests arrived", arrived.Load(), n)
				closeBody(r)
				return nil, errors.New("held too long")
			}
		}
		return f.next.RoundTrip(r)
	})
}

// TestForcedInterleavings makes conditional writes meet at the service: the
// commit requests of eight writers are held until all have arrived, then
// released together. Exactly one write must succeed and the others fail their
// precondition, definitely. Then a Replace or a Create is held at its commit
// request while another client deletes the key or creates it, and must see the
// result of that change.
func TestForcedInterleavings(t *testing.T) {
	f, s, check := fixture(t)
	const n = 8
	for _, tc := range []struct {
		name string
		size int
	}{{"single-request uploads", 11}, {"resumable uploads", ChunkSize + 1}} {
		for _, op := range []string{"create", "replace"} {
			t.Run(op+" with "+tc.name, func(t *testing.T) {
				key := strings.ReplaceAll(op+"-"+tc.name, " ", "-")
				var v string
				if op == "replace" {
					var err error
					if v, err = check.Create(bg, key, strings.NewReader("old")); err != nil {
						t.Fatal(err)
					}
				}
				f.holdCommits(t, n)
				versions, errs := make([]string, n), make([]error, n)
				var wg sync.WaitGroup
				for i := range n {
					wg.Go(func() {
						body := bytes.NewReader(data(fmt.Sprint(key, i), tc.size))
						if op == "create" {
							versions[i], errs[i] = s.Create(bg, key, body)
						} else {
							versions[i], errs[i] = s.Replace(bg, key, v, body)
						}
					})
				}
				wg.Wait()
				winner := -1
				for i, err := range errs {
					switch {
					case err == nil && winner >= 0:
						t.Fatalf("writers %d and %d both succeeded", winner, i)
					case err == nil:
						winner = i
					case !errors.Is(err, storage.ErrPreconditionFailed) || errors.Is(err, storage.ErrOutcomeUnknown):
						t.Fatalf("writer %d: %v, want a definite precondition failure", i, err)
					}
				}
				if winner < 0 {
					t.Fatal("no writer succeeded")
				}
				if got := expectObject(t, check, key, data(fmt.Sprint(key, winner), tc.size)); got != versions[winner] {
					t.Fatalf("the winner's token is %q, the object's %q", versions[winner], got)
				}
			})
		}
	}
	// held runs write with its commit request held until change has returned.
	held := func(t *testing.T, write func() error, change func() error) error {
		t.Helper()
		arrived, release := make(chan struct{}), make(chan struct{})
		f.set(func(r *http.Request) (*http.Response, error) {
			if slices.Contains(commitKinds, kind(r)) {
				close(arrived)
				<-release
			}
			return f.next.RoundTrip(r)
		})
		done := make(chan error, 1)
		go func() { done <- write() }()
		select {
		case <-arrived:
		case err := <-done:
			t.Fatalf("the write returned %v before its commit request arrived", err)
		}
		if err := change(); err != nil {
			t.Fatal(err)
		}
		close(release)
		return <-done
	}
	t.Run("replace held while the key is deleted", func(t *testing.T) {
		v, err := check.Create(bg, "rd", strings.NewReader("old"))
		if err != nil {
			t.Fatal(err)
		}
		err = held(t, func() error { _, err := s.Replace(bg, "rd", v, strings.NewReader("new")); return err },
			func() error { return check.Delete(bg, "rd") })
		if !errors.Is(err, storage.ErrPreconditionFailed) || errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Fatalf("Replace of a deleted version = %v, want a definite precondition failure", err)
		}
		expectAbsent(t, check, "rd")
	})
	t.Run("create held while another client creates the key", func(t *testing.T) {
		err := held(t, func() error { _, err := s.Create(bg, "cc", bytes.NewReader(data("mine", ChunkSize+1))); return err },
			func() error { _, err := check.Create(bg, "cc", strings.NewReader("theirs")); return err })
		if !errors.Is(err, storage.ErrPreconditionFailed) || errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Fatalf("Create of a key created meanwhile = %v, want a definite precondition failure", err)
		}
		expectObject(t, check, "cc", []byte("theirs"))
	})
	t.Run("create held while the existing key is deleted", func(t *testing.T) {
		if _, err := check.Create(bg, "cd", strings.NewReader("old")); err != nil {
			t.Fatal(err)
		}
		err := held(t, func() error { _, err := s.Create(bg, "cd", strings.NewReader("new")); return err },
			func() error { return check.Delete(bg, "cd") })
		if err != nil {
			t.Fatalf("Create whose condition held when it was evaluated: %v", err)
		}
		expectObject(t, check, "cd", []byte("new"))
	})
}

// TestPendingCommit holds a commit request in the transport past the end of
// the call's context and lets it reach the service only after the call has
// returned. The call must report an unknown outcome, not a definite error,
// because the write takes effect afterwards. A commit request that was never
// sent gives a definite error, and nothing appears later.
func TestPendingCommit(t *testing.T) {
	f, s, check := fixture(t)
	late := make(chan error, 1)
	f.once(func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		closeBody(r)
		r2 := r.Clone(bg)
		r2.Body = io.NopCloser(bytes.NewReader(body))
		<-r.Context().Done()
		go func() {
			time.Sleep(500 * time.Millisecond)
			resp, err := next.RoundTrip(r2)
			if err == nil {
				drain(resp)
			}
			late <- err
		}()
		return nil, r.Context().Err()
	}, commitKinds...)
	ctx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	_, err := s.Create(ctx, "late", strings.NewReader("late"))
	expectUnknown(t, err, "create", "late")
	expectAbsent(t, check, "late")
	if err := <-late; err != nil {
		t.Fatal(err)
	}
	expectObject(t, check, "late", []byte("late"))

	f.once(refuse, commitKinds...)
	_, err = s.Create(bg, "never", strings.NewReader("never"))
	expectDefinite(t, err, "create", "never")
	time.Sleep(time.Second)
	expectAbsent(t, check, "never")
}
