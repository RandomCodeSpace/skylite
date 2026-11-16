package gcs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
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

// TestWriteError checks the classification of a failed write: 412 for the
// write's condition is a precondition failure, other 4xx but 408 and a
// connection that was never made mean no effect, and everything else after
// the commit request started is an unknown outcome. Before it started every
// error is definite. Once net/http may have sent the commit request twice,
// every error is an unknown outcome.
func TestWriteError(t *testing.T) {
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	reset := &urlError{err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}}
	dial := &urlError{err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}
	reason := func(r ...string) *googleapi.Error {
		ge := &googleapi.Error{Code: 412, Message: "the service's message"}
		for _, r := range r {
			ge.Errors = append(ge.Errors, googleapi.ErrorItem{Reason: r})
		}
		return ge
	}
	for _, tc := range []struct {
		name         string
		ctx          context.Context
		err          error
		sent, resent bool
		is           []error
		unknown      bool
	}{
		{"412", bg, &googleapi.Error{Code: 412}, true, false, []error{storage.ErrPreconditionFailed}, false},
		{"412 not sent", bg, &googleapi.Error{Code: 412}, false, false, []error{storage.ErrPreconditionFailed}, false},
		{"412 conditionNotMet", bg, reason("conditionNotMet"), true, false, []error{storage.ErrPreconditionFailed}, false},
		{"412 orgPolicyConstraintFailed", bg, reason("orgPolicyConstraintFailed"), true, false, nil, false},
		{"412 conditionNotMet and another reason", bg, reason("conditionNotMet", "objectNotSoftDeleted"), true, false, nil, false},
		{"403", bg, &googleapi.Error{Code: 403}, true, false, nil, false},
		{"429", bg, &googleapi.Error{Code: 429}, true, false, nil, false},
		{"408", bg, &googleapi.Error{Code: 408}, true, false, nil, true},
		{"500", bg, &googleapi.Error{Code: 500}, true, false, nil, true},
		{"503", bg, &googleapi.Error{Code: 503}, true, false, nil, true},
		{"503 before the commit", bg, &googleapi.Error{Code: 503}, false, false, nil, false},
		{"reset", bg, reset, true, false, nil, true},
		{"reset before the commit", bg, reset, false, false, nil, false},
		{"dial", bg, dial, true, false, nil, false},
		{"cancelled", cancelled, context.Canceled, true, false, []error{context.Canceled}, true},
		{"cancelled before the commit", cancelled, context.Canceled, false, false, []error{context.Canceled}, false},
		{"412 after a re-send", bg, reason("conditionNotMet"), true, true, nil, true},
		{"403 after a re-send", bg, &googleapi.Error{Code: 403}, true, true, nil, true},
		{"dial after a re-send", bg, dial, true, true, nil, true},
	} {
		err := writeError(tc.ctx, "create", "k", tc.err, tc.sent, tc.resent)
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
		if ge := (*googleapi.Error)(nil); errors.As(tc.err, &ge) && ge.Message != "" && !errors.Is(err, storage.ErrPreconditionFailed) && !strings.Contains(err.Error(), ge.Message) {
			t.Errorf("%s: %v lost the service's message", tc.name, err)
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
	err := writeError(bg, "create", "k", &urlError{err: syscall.ECONNRESET}, true, false)
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
	// Without the check Open fails too where no credentials can be found, so
	// the test requires the check's own error.
	if _, err := Open(bg, Config{Bucket: "b"}); err == nil || !strings.Contains(err.Error(), "STORAGE_EMULATOR_HOST is set") {
		t.Fatalf("Open with STORAGE_EMULATOR_HOST set and no endpoint = %v, want it refused", err)
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
	return jsonResponse(r, code, fmt.Sprintf(`{"error":{"code":%d,"message":"injected"}}`, code))
}

func jsonResponse(r *http.Request, code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code, Status: fmt.Sprint(code, " ", http.StatusText(code)), Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:  http.Header{"Content-Type": {"application/json"}},
		Body:    io.NopCloser(strings.NewReader(body)),
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
// ends, or for ten seconds, so that a call that ignores its context fails its
// test instead of hanging it.
func stall(r *http.Request, next http.RoundTripper) (*http.Response, error) {
	resp, err := next.RoundTrip(r)
	if err == nil {
		drain(resp)
	}
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-time.After(10 * time.Second):
		return nil, errors.New("stall: the request's context did not end")
	}
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
				start := time.Now()
				err := run(ctx)
				if took := time.Since(start); fc.name == "slow response past the deadline" && took > 5*time.Second {
					t.Fatalf("the call past its 3 s deadline returned after %v: it ignored its context", took)
				}
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

// TestReadFaults checks that a Head, Get, GetRange or List whose response is
// held past its deadline fails promptly with the context's error, and that a body whose connection breaks after the object
// was replaced fails instead of returning bytes of the new version.
func TestReadFaults(t *testing.T) {
	f, s, check := fixture(t)
	old := data("old", 4<<20)
	v, err := s.Create(bg, "k", bytes.NewReader(old))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, kind string
		call       func(ctx context.Context) error
	}{
		{"Head", "meta", func(ctx context.Context) error { _, err := s.Head(ctx, "k"); return err }},
		{"Get", "media", func(ctx context.Context) error { _, _, err := s.Get(ctx, "k"); return err }},
		{"GetRange", "media", func(ctx context.Context) error { _, _, err := s.GetRange(ctx, "k", 1, 10); return err }},
		{"List", "list", func(ctx context.Context) error { _, err := s.List(ctx, "", "", 10); return err }},
	} {
		f.once(stall, tc.kind)
		ctx, cancel := context.WithTimeout(bg, time.Second)
		start := time.Now()
		err := tc.call(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrOutcomeUnknown) || time.Since(start) > 5*time.Second {
			t.Fatalf("%s past its 1 s deadline = %v after %v, want context.DeadlineExceeded promptly", tc.name, err, time.Since(start))
		}
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

// TestBodyAfterCancel reads the start of a body, cancels its context and
// reads again. The contract: once the context has ended, a Read that would
// return more data fails with an error that wraps the context's error, even
// when the connection still has data buffered.
func TestBodyAfterCancel(t *testing.T) {
	_, s, _ := fixture(t)
	if _, err := s.Create(bg, "k", bytes.NewReader(data("k", 1<<20))); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Get", "GetRange"} {
		ctx, cancel := context.WithCancel(bg)
		var body io.ReadCloser
		var err error
		if name == "Get" {
			body, _, err = s.Get(ctx, "k")
		} else {
			body, _, err = s.GetRange(ctx, "k", 1, 1<<19)
		}
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if _, err := io.ReadFull(body, make([]byte, 10)); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		n, err := body.Read(make([]byte, 1000))
		_ = body.Close()
		if n != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: Read after cancel = %d bytes, %v; want 0 bytes and context.Canceled", name, n, err)
		}
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
			// The cancelling reader keeps returning no data and no error, so only
			// the store's own check of its context ends the write.
			var v string
			var err error
			done := make(chan struct{})
			go func() {
				defer close(done)
				v, err = s.Create(ctx, key, io.MultiReader(bytes.NewReader(data(key, ChunkSize+1<<20)), stop))
			}()
			select {
			case <-done:
			case <-time.After(time.Minute):
				t.Fatal("Create did not return within a minute of the body failing or its context ending")
			}
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

// TestListPageRefused makes the service refuse the second page of a listing.
// List must return the error, not the entries it had so far: a result
// shorter than limit reads as the end of the listing.
func TestListPageRefused(t *testing.T) {
	f, s, _ := fixture(t)
	for i := range 3 {
		if _, err := s.Create(bg, fmt.Sprint("p/", i), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
	}
	var lists atomic.Int64
	f.set(func(r *http.Request) (*http.Response, error) {
		if kind(r) == "list" {
			if lists.Add(1) == 2 {
				closeBody(r)
				return injected(r, 403), nil
			}
			r = r.Clone(r.Context())
			q := r.URL.Query()
			q.Set("maxResults", "1")
			r.URL.RawQuery = q.Encode()
		}
		return f.next.RoundTrip(r)
	})
	if e, err := s.List(bg, "p/", "", 10); err == nil || lists.Load() != 2 {
		t.Fatalf("List whose second page was refused = %+v, %v after %d list requests; want an error after 2", e, err, lists.Load())
	}
}

// TestBodyDataWithEOF writes a body whose last Read returns its last bytes
// together with io.EOF; they must be part of the object.
func TestBodyDataWithEOF(t *testing.T) {
	_, s, check := fixture(t)
	body := data("eof", 1000)
	if _, err := s.Create(bg, "eof", iotest.DataErrReader(bytes.NewReader(body))); err != nil {
		t.Fatal(err)
	}
	expectObject(t, check, "eof", body)
}

// h2Server is an HTTP/2 server, framed by hand, that answers uploads like the
// JSON API, except that it resets the stream of the first commit request it
// receives with PROTOCOL_ERROR after reading all of it. That code does not
// tell the client whether the server processed the stream, and net/http
// re-sends the request on its own, below the store's transport. The server
// answers the re-sent commit request with second, or, if redial is set,
// stops listening before the reset: net/http does not reuse a connection
// after a PROTOCOL_ERROR reset, so the re-send has to dial and is refused.
//
// If hold is set, the server allows one stream per connection, closes hold
// when the request for object "hold" arrives and answers it 404 only once the
// first commit request has arrived, closing release unless the test closed it
// first; it resets that commit once resume is closed. acks, if set, receives a value for each SETTINGS
// ACK of the client.
type h2Server struct {
	ln      net.Listener
	second  int
	redial  bool
	hold    chan struct{}
	release chan struct{}
	resume  chan struct{}
	acks    chan struct{}
	mu      sync.Mutex
	commits int
}

func (s *h2Server) accept() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.serve(c)
	}
}

func (s *h2Server) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

func (s *h2Server) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	if _, err := io.ReadFull(c, make([]byte, len(http2.ClientPreface))); err != nil {
		return
	}
	fr := http2.NewFramer(c, c)
	dec := hpack.NewDecoder(4096, nil)
	if s.hold != nil {
		_ = fr.WriteSettings(http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 1})
	} else {
		_ = fr.WriteSettings()
	}
	respond := func(id uint32, status int, body string, header ...string) {
		var hb bytes.Buffer
		enc := hpack.NewEncoder(&hb)
		_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: strconv.Itoa(status)})
		for i := 0; i+1 < len(header); i += 2 {
			_ = enc.WriteField(hpack.HeaderField{Name: header[i], Value: header[i+1]})
		}
		_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: hb.Bytes(), EndHeaders: true, EndStream: body == ""})
		if body != "" {
			_ = fr.WriteData(id, true, []byte(body))
		}
	}
	requests := map[uint32]*http.Request{}
	// done answers the request on stream id, whose body has been read, and
	// reports whether to keep the connection.
	done := func(id uint32) bool {
		r := requests[id]
		delete(requests, id)
		switch kind(r) {
		case "session":
			respond(id, 200, "", "location", "https://"+s.ln.Addr().String()+"/upload/storage/v1/b/b/o?uploadType=resumable&upload_id=x")
			return true
		case "chunk":
			cr := strings.TrimSuffix(r.Header.Get("Content-Range"), "/*")
			respond(id, 200, "", "x-http-status-code-override", "308", "range", "bytes=0-"+cr[strings.IndexByte(cr, '-')+1:])
			return true
		case "single", "final", "delete":
		case "meta":
			if s.hold != nil && strings.HasSuffix(r.URL.Path, "/o/hold") {
				close(s.hold)
				// Its own framer, so the loop goes on reading; a tls.Conn writes each frame whole.
				go func() {
					<-s.release
					var hb bytes.Buffer
					_ = hpack.NewEncoder(&hb).WriteField(hpack.HeaderField{Name: ":status", Value: "404"})
					_ = http2.NewFramer(c, nil).WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: hb.Bytes(), EndHeaders: true, EndStream: true})
				}()
				return true
			}
			fallthrough
		default:
			respond(id, 404, `{"error":{"code":404,"message":"not found"}}`, "content-type", "application/json")
			return true
		}
		s.mu.Lock()
		s.commits++
		n := s.commits
		s.mu.Unlock()
		if n > 1 {
			respond(id, s.second, fmt.Sprintf(`{"error":{"code":%d,"message":"second","errors":[{"reason":"conditionNotMet"}]}}`, s.second), "content-type", "application/json")
			return true
		}
		if s.release != nil {
			select {
			case <-s.release:
			default:
				close(s.release)
			}
			<-s.resume
		}
		if s.redial {
			_ = s.ln.Close()
		}
		_ = fr.WriteRSTStream(id, http2.ErrCodeProtocol)
		return true
	}
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			return
		}
		var end uint32
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				_ = fr.WriteSettingsAck()
			} else if s.acks != nil {
				s.acks <- struct{}{}
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				_ = fr.WritePing(true, f.Data)
			}
		case *http2.HeadersFrame:
			fields, err := dec.DecodeFull(f.HeaderBlockFragment())
			if err != nil {
				return
			}
			r := &http.Request{Header: http.Header{}, URL: &url.URL{}}
			for _, h := range fields {
				switch h.Name {
				case ":method":
					r.Method = h.Value
				case ":path":
					if r.URL, err = url.Parse(h.Value); err != nil {
						return
					}
				default:
					r.Header.Add(h.Name, h.Value)
				}
			}
			requests[f.StreamID] = r
			if f.StreamEnded() {
				end = f.StreamID
			}
		case *http2.DataFrame:
			if n := f.Header().Length; n > 0 {
				_ = fr.WriteWindowUpdate(0, n)
				if !f.StreamEnded() {
					_ = fr.WriteWindowUpdate(f.StreamID, n)
				}
			}
			if f.StreamEnded() && requests[f.StreamID] != nil {
				end = f.StreamID
			}
		}
		if end != 0 && !done(end) {
			return
		}
	}
}

// TestHTTP2Resend runs every kind of commit request against h2Server through
// net/http's own HTTP/2 transport. The server reset the first attempt without
// saying whether it applied it, so whatever the re-send gets back, a 412, a
// 403 or a refused connection, the call's outcome is unknown.
func TestHTTP2Resend(t *testing.T) {
	ts := httptest.NewUnstartedServer(nil)
	ts.StartTLS()
	cert, roots := ts.TLS.Certificates[0], ts.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	ts.Close()
	for _, tc := range []struct {
		name, op string
		second   int
		redial   bool
		run      func(s *Store) error
	}{
		{"create of 11 bytes", "create", 412, false, func(s *Store) error { _, err := s.Create(bg, "k", strings.NewReader("hello world")); return err }},
		{"replace of 11 bytes", "replace", 412, false, func(s *Store) error { _, err := s.Replace(bg, "k", "7", strings.NewReader("hello world")); return err }},
		{"create of ChunkSize+1 bytes", "create", 412, false, func(s *Store) error {
			_, err := s.Create(bg, "k", bytes.NewReader(make([]byte, ChunkSize+1)))
			return err
		}},
		{"delete", "delete", 403, false, func(s *Store) error { return s.Delete(bg, "k") }},
		{"create re-sent on a refused connection", "create", 0, true, func(s *Store) error { _, err := s.Create(bg, "k", strings.NewReader("hello world")); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ln.Close() })
			srv := &h2Server{ln: ln, second: tc.second, redial: tc.redial}
			go srv.accept()
			var dials atomic.Int64
			tr := &http.Transport{
				TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
				ForceAttemptHTTP2: true,
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				},
			}
			t.Cleanup(tr.CloseIdleConnections)
			s := openStore(t, Config{Bucket: "b", Endpoint: "https://" + ln.Addr().String() + "/storage/v1/"}, tr)
			err = tc.run(s)
			if n, d := srv.commitCount(), dials.Load(); tc.redial && (n != 1 || d != 2) || !tc.redial && n != 2 {
				t.Fatalf("the server saw %d commit requests over %d connections; net/http did not re-send as expected", n, d)
			}
			expectUnknown(t, err, tc.op, "k")
		})
	}
}

// TestHTTP2ResendOnAnotherConnection re-sends the commit request on a second
// HTTP/2 connection, one dialled for a Head that gave up during the dial, for
// which net/http does not call GetConn. In the first order the first attempt
// goes out on that connection and the re-send on the first connection, now
// free. In the reverse order the held Head ends before the mutation, so the
// first attempt takes the first connection and the re-send the dialled one,
// with no GetConn between the two attempts.
func TestHTTP2ResendOnAnotherConnection(t *testing.T) {
	ts := httptest.NewUnstartedServer(nil)
	ts.StartTLS()
	cert, roots := ts.TLS.Certificates[0], ts.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	ts.Close()
	for _, tc := range []struct {
		op     string
		second int
		run    func(s *Store) error
	}{
		{"create", 412, func(s *Store) error { _, err := s.Create(bg, "k", strings.NewReader("hello world")); return err }},
		{"replace", 412, func(s *Store) error { _, err := s.Replace(bg, "k", "7", strings.NewReader("hello world")); return err }},
		{"delete", 403, func(s *Store) error { return s.Delete(bg, "k") }},
	} {
		for _, reverse := range []bool{false, true} {
			name := tc.op
			if reverse {
				name += " in reverse order"
			}
			t.Run(name, func(t *testing.T) {
				ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = ln.Close() })
				srv := &h2Server{ln: ln, second: tc.second, hold: make(chan struct{}), release: make(chan struct{}), resume: make(chan struct{}), acks: make(chan struct{}, 4)}
				go srv.accept()
				var dials atomic.Int64
				dialling, dial := make(chan struct{}), make(chan struct{})
				tr := &http.Transport{
					TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
					ForceAttemptHTTP2: true,
					DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
						if dials.Add(1) == 2 {
							close(dialling)
							<-dial
						}
						return (&net.Dialer{}).DialContext(ctx, network, addr)
					},
				}
				t.Cleanup(tr.CloseIdleConnections)
				tr.CloseIdleConnections() // installs net/http's HTTP/2 in TLSNextProto
				pooled, h2 := make(chan struct{}, 2), tr.TLSNextProto["h2"]
				tr.TLSNextProto["h2"] = func(authority string, c *tls.Conn) http.RoundTripper {
					rt := h2(authority, c) // returns once c is in the HTTP/2 pool
					pooled <- struct{}{}
					return rt
				}
				s := openStore(t, Config{Bucket: "b", Endpoint: "https://" + ln.Addr().String() + "/storage/v1/"}, tr)
				held := make(chan struct{})
				go func() { _, _ = s.Head(bg, "hold"); close(srv.resume); close(held) }()
				<-srv.hold
				<-srv.acks // the first connection takes one stream, now busy
				ctx, cancel := context.WithCancel(bg)
				head := make(chan struct{})
				go func() { _, _ = s.Head(ctx, "k"); close(head) }()
				<-dialling
				cancel()
				<-head
				close(dial) // the dial goes on for no request; its connection joins the pool
				<-srv.acks
				<-pooled
				<-pooled // both connections are in the pool; the ack alone can come first
				if reverse {
					close(srv.release) // the held Head ends: the first connection is free again
					<-held
					time.Sleep(100 * time.Millisecond) // the client returns the stream before the Head's caller sees it end
				}
				err = tc.run(s)
				if n, d := srv.commitCount(), dials.Load(); n != 2 || d != 2 {
					t.Fatalf("the server saw %d commit requests over %d connections, want 2 over 2", n, d)
				}
				expectUnknown(t, err, tc.op, "k")
			})
		}
	}
}

// adc points Application Default Credentials at a made-up authorized_user
// file whose token endpoint is a local server running token, and returns a
// service transport that answers every request itself and counts them.
// Nothing leaves the machine.
func adc(t *testing.T, token http.HandlerFunc) (tokens, requests *atomic.Int64, service http.RoundTripper) {
	t.Helper()
	tokens, requests = new(atomic.Int64), new(atomic.Int64)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens.Add(1)
		w.Header().Set("Content-Type", "application/json")
		token(w, r)
	}))
	t.Cleanup(ts.Close)
	file := filepath.Join(t.TempDir(), "adc.json")
	cred := `{"type":"authorized_user","client_id":"id","client_secret":"not-a-secret","refresh_token":"not-a-token","token_uri":"` + ts.URL + `"}`
	if err := os.WriteFile(file, []byte(cred), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	return tokens, requests, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		closeBody(r)
		if r.Method == http.MethodDelete {
			return jsonResponse(r, 204, ""), nil
		}
		return jsonResponse(r, 200, `{"name":"k","generation":"7","size":"11"}`), nil
	})
}

func grantToken(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, `{"access_token":"not-a-token","token_type":"Bearer","expires_in":3600}`)
}

// TestTokenFailure checks that a mutation whose credentials cannot be
// fetched fails definitely: no request reached the service.
func TestTokenFailure(t *testing.T) {
	tokens, requests, service := adc(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"invalid_grant"}`)
	})
	s := openStore(t, Config{Bucket: "b"}, service)
	_, err := s.Create(bg, "k", strings.NewReader("hello world"))
	expectDefinite(t, err, "create", "k")
	expectDefinite(t, s.Delete(bg, "k"), "delete", "k")
	if tokens.Load() == 0 || requests.Load() != 0 {
		t.Fatalf("%d token requests and %d service requests, want some and none", tokens.Load(), requests.Load())
	}
}

// TestOpenContext checks that the Store keeps working after the context given
// to Open has ended: credentials that fetch tokens later must not use it.
func TestOpenContext(t *testing.T) {
	tokens, _, service := adc(t, grantToken)
	ctx, cancel := context.WithCancel(bg)
	s, err := open(ctx, Config{Bucket: "b"}, service)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Head(bg, "k"); err != nil {
		t.Fatalf("Head after Open's context ended: %v", err)
	}
	if _, err := s.Create(bg, "k", strings.NewReader("hello world")); err != nil {
		t.Fatalf("Create after Open's context ended: %v", err)
	}
	if tokens.Load() == 0 {
		t.Fatal("no token was fetched")
	}
}

// TestResponseWithoutGeneration answers every request with a success that
// carries no generation. A write must not return a token for it, since it
// cannot tell what it published, and a Head must not return version "0".
func TestResponseWithoutGeneration(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
	}{{"204 without a body", 204, ""}, {"200 with an empty object", 200, "{}"}} {
		s := openStore(t, Config{Bucket: "b", Endpoint: "http://127.0.0.1:1/storage/v1/"}, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			closeBody(r)
			return jsonResponse(r, tc.code, tc.body), nil
		}))
		v, err := s.Create(bg, "k", strings.NewReader("hello world"))
		if v != "" {
			t.Errorf("%s: Create returned the token %q", tc.name, v)
		}
		expectUnknown(t, err, "create", "k")
		if info, err := s.Head(bg, "k"); err == nil {
			t.Errorf("%s: Head = %+v, want an error", tc.name, info)
		}
	}
}

// TestSmallWriteMemory checks that a small write does not allocate a whole
// chunk: the client buffers ChunkSize bytes per upload unless it is told the
// body is smaller.
func TestSmallWriteMemory(t *testing.T) {
	s := openStore(t, Config{Bucket: "b", Endpoint: "http://127.0.0.1:1/storage/v1/"}, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_, _ = io.Copy(io.Discard, r.Body)
		closeBody(r)
		return jsonResponse(r, 200, `{"name":"k","generation":"7","size":"11"}`), nil
	}))
	const n = 10
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range n {
		if _, err := s.Create(bg, "k", strings.NewReader("hello world")); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / n; per > ChunkSize/4 {
		t.Fatalf("an 11-byte Create allocates %d bytes", per)
	}
}

const (
	envKillPoint  = "SKYLITE_TEST_GCS_KILL_POINT"
	envKillBucket = "SKYLITE_TEST_GCS_KILL_BUCKET"
)

func TestMain(m *testing.M) {
	if point := os.Getenv(envKillPoint); point != "" {
		os.Exit(killHelper(point))
	}
	os.Exit(m.Run())
}

// killBody is what the helper of TestKilled writes: one chunk and a byte, so
// that it goes through a resumable session.
func killBody() []byte { return data("killed", ChunkSize+1) }

// killHelper creates key "k" with killBody and, at point, prints "at <point>"
// and stops, to be killed: "chunk" once the first chunk has been stored,
// "commit sent" once the commit request has been written but before its
// response is read, "committed" once its response has arrived.
func killHelper(point string) int {
	next := http.DefaultTransport
	stop := func() {
		fmt.Println("at " + point)
		select {}
	}
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch k := kind(r); {
		case k == "chunk" && point == "chunk", slices.Contains(commitKinds, k) && point == "committed":
			resp, err := next.RoundTrip(r)
			if err == nil {
				stop()
			}
			return resp, err
		case slices.Contains(commitKinds, k) && point == "commit sent":
			wrote, once := make(chan struct{}), new(sync.Once)
			r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{
				WroteRequest: func(httptrace.WroteRequestInfo) { once.Do(func() { close(wrote) }) },
			}))
			go func() {
				if resp, err := next.RoundTrip(r); err == nil {
					drain(resp)
				}
			}()
			<-wrote
			stop()
		}
		return next.RoundTrip(r)
	})
	s, err := open(bg, Config{Bucket: os.Getenv(envKillBucket), Endpoint: os.Getenv(endpointEnv)}, base)
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}
	_, err = s.Create(bg, "k", bytes.NewReader(killBody()))
	fmt.Println("finished without stopping:", err)
	return 1
}

// TestKilled kills a process inside a Create, during and just after
// publication, and checks that the key is whole or absent and that a later
// Create succeeds when it is absent.
func TestKilled(t *testing.T) {
	ep := endpoint(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ point, want string }{
		{"chunk", "absent"},
		{"commit sent", "whole or absent"},
		{"committed", "whole"},
	} {
		t.Run(tc.point, func(t *testing.T) {
			bucket := newBucket(t)
			cmd := exec.Command(exe, "-test.run=^$") //nolint:gosec // re-executes this test binary
			cmd.Env = append(os.Environ(), envKillPoint+"="+tc.point, envKillBucket+"="+bucket)
			cmd.Stderr = os.Stderr
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if line != "at "+tc.point+"\n" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("helper printed %q, %v; want at %s", line, err, tc.point)
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			time.Sleep(time.Second) // let the emulator finish a request the helper had sent
			s := openStore(t, Config{Bucket: bucket, Endpoint: ep}, newTransport(t))
			_, err = s.Head(bg, "k")
			absent := errors.Is(err, storage.ErrNotFound)
			switch {
			case !absent && err != nil:
				t.Fatal(err)
			case absent && tc.want == "whole", !absent && tc.want == "absent":
				t.Fatalf("after the kill at %s the key is absent = %v, want %s", tc.point, absent, tc.want)
			case absent:
				body := data("after the kill", 11)
				if _, err := s.Create(bg, "k", bytes.NewReader(body)); err != nil {
					t.Fatalf("Create after the kill: %v", err)
				}
				expectObject(t, s, "k", body)
			default:
				expectObject(t, s, "k", killBody())
			}
		})
	}
}
