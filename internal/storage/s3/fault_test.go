package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// faults is an http.RoundTripper between the SDK and the emulator that
// applies the first rule matching each request and passes the rest through.
type faults struct {
	next  http.RoundTripper
	mu    sync.Mutex
	rules []*rule
	seen  []string // method of every request, in order
	wg    sync.WaitGroup
}

// rule applies do to the next times requests with this method (any method if
// empty) and, if list is set, only to ListObjectsV2. times < 0 means every one.
type rule struct {
	method string
	list   bool
	times  int
	do     func(f *faults, req *http.Request) (*http.Response, error)
}

func newFaults(rules ...*rule) *faults {
	return &faults{next: http.DefaultTransport.(*http.Transport).Clone(), rules: rules}
}

func (f *faults) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.seen = append(f.seen, req.Method)
	var do func(*faults, *http.Request) (*http.Response, error)
	for _, r := range f.rules {
		if r.times != 0 && (r.method == "" || r.method == req.Method) && r.list == req.URL.Query().Has("list-type") {
			r.times--
			do = r.do
			break
		}
	}
	f.mu.Unlock()
	if do == nil {
		return f.next.RoundTrip(req)
	}
	return do(f, req)
}

// count returns how many requests with method were sent, retries included.
func (f *faults) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.seen {
		if m == method {
			n++
		}
	}
	return n
}

// close waits for requests still being delivered and closes idle connections.
func (f *faults) close() {
	f.wg.Wait()
	f.next.(*http.Transport).CloseIdleConnections()
}

func connReset(op string) error {
	return &net.OpError{Op: op, Net: "tcp", Err: syscall.ECONNRESET}
}

func closeBody(req *http.Request) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
}

// refuse fails the request before anything is sent, as a refused connection.
func refuse(_ *faults, req *http.Request) (*http.Response, error) {
	closeBody(req)
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

// lose delivers the request and loses the response, as a connection reset
// after the service has answered.
func lose(f *faults, req *http.Request) (*http.Response, error) {
	resp, err := f.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil, connReset("read")
}

// loseThen is lose, then calls fn before the SDK sees the lost response.
func loseThen(fn func()) func(*faults, *http.Request) (*http.Response, error) {
	return func(f *faults, req *http.Request) (*http.Response, error) {
		resp, err := lose(f, req)
		fn()
		return resp, err
	}
}

// before runs fn and then delivers the request.
func before(fn func()) func(*faults, *http.Request) (*http.Response, error) {
	return func(f *faults, req *http.Request) (*http.Response, error) {
		fn()
		return f.next.RoundTrip(req)
	}
}

// cutBody sends the request but cuts the connection after 1,000 bytes of its
// body.
func cutBody(f *faults, req *http.Request) (*http.Response, error) {
	req.Body = &failingBody{r: req.Body, left: 1000}
	return f.next.RoundTrip(req)
}

type failingBody struct {
	r    io.ReadCloser
	left int
}

func (b *failingBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, connReset("write")
	}
	n, err := b.r.Read(p[:min(len(p), b.left)])
	b.left -= n
	return n, err
}

func (b *failingBody) Close() error { return b.r.Close() }

// status answers with an S3 error response with code and status, without
// delivering the request if after is false, and after delivering it and
// discarding the service's response if it is true.
func status(code int, s3Code string, after bool) func(*faults, *http.Request) (*http.Response, error) {
	return func(f *faults, req *http.Request) (*http.Response, error) {
		if after {
			resp, err := f.next.RoundTrip(req)
			if err != nil {
				return nil, err
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		} else {
			closeBody(req)
		}
		body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected</Message></Error>`, s3Code)
		return &http.Response{
			StatusCode: code, Status: fmt.Sprint(code, " ", http.StatusText(code)), Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header:        http.Header{"Content-Type": {"application/xml"}},
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)), Request: req,
		}, nil
	}
}

// stallAfter delivers the request, discards the response and waits for the
// request's context to end, as a response slower than the deadline.
func stallAfter(f *faults, req *http.Request) (*http.Response, error) {
	resp, err := f.next.RoundTrip(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// stallBefore waits for the request's context to end without sending
// anything, as a connection that never gets going.
func stallBefore(_ *faults, req *http.Request) (*http.Response, error) {
	closeBody(req)
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// deliverLate takes the whole request and reports it written, as net/http
// does once the last byte is on the connection, then waits for the context
// to end, and only after that delivers the request, released by release: a
// request still in flight when its call returned.
func deliverLate(release <-chan struct{}) func(*faults, *http.Request) (*http.Response, error) {
	return func(f *faults, req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		closeBody(req)
		if err != nil {
			return nil, err
		}
		if tr := httptrace.ContextClientTrace(req.Context()); tr != nil && tr.WroteRequest != nil {
			tr.WroteRequest(httptrace.WroteRequestInfo{})
		}
		late := req.Clone(context.WithoutCancel(req.Context()))
		late.Body = io.NopCloser(bytes.NewReader(body))
		f.wg.Go(func() {
			<-release
			if resp, err := f.next.RoundTrip(late); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
}

var contentsRE = regexp.MustCompile(`(?s)<Contents>.*?</Contents>`)

// shortPage delivers a ListObjectsV2 request and keeps only the first keep
// entries of the response, marking it truncated if it dropped any.
func shortPage(keep int) func(*faults, *http.Request) (*http.Response, error) {
	return func(f *faults, req *http.Request) (*http.Response, error) {
		resp, err := f.next.RoundTrip(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			return resp, err
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		n := 0
		body = contentsRE.ReplaceAllFunc(body, func(m []byte) []byte {
			if n++; n > keep {
				return nil
			}
			return m
		})
		if n > keep {
			body = regexp.MustCompile(`<IsTruncated>false</IsTruncated>`).ReplaceAll(body, []byte(`<IsTruncated>true</IsTruncated>`))
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		resp.Header.Del("Content-Length")
		return resp, nil
	}
}

// expectUnknown checks that err is a write error wrapping ErrOutcomeUnknown
// and no other storage sentinel.
func expectUnknown(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want it to wrap ErrOutcomeUnknown", err)
	}
	expectOnlySentinel(t, err, nil)
}

// expectDefinite checks that err is an error that does not wrap
// ErrOutcomeUnknown and wraps no storage sentinel other than want (nil for
// none).
func expectDefinite(t *testing.T, err error, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("call succeeded, want an error")
	}
	if errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("error = %v wraps ErrOutcomeUnknown, but the call definitely had no effect", err)
	}
	if want != nil && !errors.Is(err, want) {
		t.Fatalf("error = %v, want it to wrap %v", err, want)
	}
	expectOnlySentinel(t, err, want)
}

func expectOnlySentinel(t *testing.T, err, want error) {
	t.Helper()
	var se *storage.Error
	if !errors.As(err, &se) || se.Key == "" && se.Op != "list" {
		t.Fatalf("error %v is not a *storage.Error with its key", err)
	}
	for _, s := range []error{storage.ErrNotFound, storage.ErrPreconditionFailed, storage.ErrInvalidKey, storage.ErrInvalidArgument} {
		if s != want && errors.Is(err, s) { //nolint:errorlint // compares two sentinels, not an error
			t.Fatalf("error = %v wraps %v", err, s)
		}
	}
	if strings.Contains(err.Error(), devSecretKey) || strings.Contains(err.Error(), devAccessKey) {
		t.Fatalf("error text %q contains a credential", err)
	}
}

// expectHolds checks through a client without faults that key holds content
// at version.
func expectHolds(t *testing.T, s storage.Store, key, content, version string) {
	t.Helper()
	body, info, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	got, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(got) != content || info != (storage.Info{Size: int64(len(content)), Version: version}) {
		t.Fatalf("Get(%q) = %q, %+v, %v; want %q at version %q", key, got, info, err, content, version)
	}
}

func expectMissing(t *testing.T, s storage.Store, key string) {
	t.Helper()
	if _, err := s.Head(context.Background(), key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Head(%q) = %v, want ErrNotFound", key, err)
	}
}

var ctx = context.Background()

// A large body takes the SDK's Expect: 100-continue path (over 2 MiB) as well.
var bodies = map[string]string{"small": "payload", "large": strings.Repeat("0123456789abcdef", 3<<16)}

// TestLostResponse loses the response to a mutation that the service applied.
// The contract: the outcome is unknown unless the backend establishes it. An
// SDK retry would fail its condition against this write's own attempt, or
// leave an attempt pending after the call returned, so the backend sends no
// retry; the per-write identity turns the write into success at the version
// that carries it, and if the identity cannot be found the outcome is
// unknown. Delete has no identity, so its outcome is unknown.
func TestLostResponse(t *testing.T) {
	for name, content := range bodies {
		t.Run("Create "+name, func(t *testing.T) {
			b := newBucket(t)
			f := newFaults(&rule{method: http.MethodPut, times: 1, do: lose})
			defer f.close()
			v, err := b.storeVia(t, f).Create(ctx, "k", strings.NewReader(content))
			if err != nil {
				t.Fatalf("Create whose first response was lost: %v, want success", err)
			}
			if f.count(http.MethodPut) != 1 || f.count(http.MethodHead) != 1 {
				t.Fatalf("requests %v, want one PUT, not retried, and a HEAD that found the write", f.seen)
			}
			expectHolds(t, b.store(t), "k", content, v)
		})
		t.Run("Replace "+name, func(t *testing.T) {
			b := newBucket(t)
			v1, err := b.store(t).Create(ctx, "k", strings.NewReader("old"))
			if err != nil {
				t.Fatal(err)
			}
			f := newFaults(&rule{method: http.MethodPut, times: 1, do: lose})
			defer f.close()
			v2, err := b.storeVia(t, f).Replace(ctx, "k", v1, strings.NewReader(content))
			if err != nil {
				t.Fatalf("Replace whose first response was lost: %v, want success", err)
			}
			if f.count(http.MethodPut) != 1 {
				t.Fatalf("requests %v, want one PUT, not retried", f.seen)
			}
			expectHolds(t, b.store(t), "k", content, v2)
			_, err = b.store(t).Replace(ctx, "k", v1, strings.NewReader("stale"))
			expectDefinite(t, err, storage.ErrPreconditionFailed)
		})
	}
	t.Run("Replace overtaken before the identity check", func(t *testing.T) {
		// The write commits, its response is lost, and another client replaces
		// the key before the identity check: unknown, never a precondition
		// failure.
		b := newBucket(t)
		other := b.store(t)
		v1, err := other.Create(ctx, "k", strings.NewReader("old"))
		if err != nil {
			t.Fatal(err)
		}
		var otherVersion string
		f := newFaults(&rule{method: http.MethodPut, times: 1, do: loseThen(func() { //nolint:bodyclose // a fault rule, not a response
			body, info, err := other.Get(ctx, "k")
			if err != nil {
				t.Error(err)
				return
			}
			_ = body.Close()
			if otherVersion, err = other.Replace(ctx, "k", info.Version, strings.NewReader("other")); err != nil {
				t.Error(err)
			}
		})})
		defer f.close()
		_, err = b.storeVia(t, f).Replace(ctx, "k", v1, strings.NewReader("mine"))
		expectUnknown(t, err)
		expectHolds(t, other, "k", "other", otherVersion)
	})
	t.Run("Create, the response and the check lost", func(t *testing.T) {
		b := newBucket(t)
		f := newFaults(&rule{method: http.MethodPut, times: -1, do: lose}, &rule{method: http.MethodHead, times: -1, do: refuse})
		defer f.close()
		_, err := b.storeVia(t, f).Create(ctx, "k", strings.NewReader("payload"))
		expectUnknown(t, err)
		if f.count(http.MethodPut) != 1 {
			t.Fatalf("requests %v, want one PUT, not retried", f.seen)
		}
		if !b.exists(t, "k") {
			t.Fatal("the first attempt was delivered but the key is missing")
		}
	})
	t.Run("Delete", func(t *testing.T) {
		// A retry that succeeded would hide the first attempt, which must not
		// be pending when a caller creates the key again after a nil Delete.
		b := newBucket(t)
		if _, err := b.store(t).Create(ctx, "k", strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		f := newFaults(&rule{method: http.MethodDelete, times: 1, do: lose})
		defer f.close()
		expectUnknown(t, b.storeVia(t, f).Delete(ctx, "k"))
		if f.count(http.MethodDelete) != 1 {
			t.Fatalf("requests %v, want one DELETE, not retried", f.seen)
		}
		expectMissing(t, b.store(t), "k")
	})
}

// TestRequestNotSent fails requests before any byte is sent. The contract: a
// write that definitely had no effect does not return ErrOutcomeUnknown, and
// leaves the key free for a later Create. The SDK retries refused connections.
func TestRequestNotSent(t *testing.T) {
	b := newBucket(t)
	f := newFaults(&rule{method: http.MethodPut, times: 3, do: refuse}, &rule{method: http.MethodDelete, times: 3, do: refuse})
	defer f.close()
	s := b.storeVia(t, f)
	_, err := s.Create(ctx, "k", strings.NewReader("payload"))
	expectDefinite(t, err, nil)
	if f.count(http.MethodPut) != 3 {
		t.Fatalf("requests %v, want 3 PUT attempts", f.seen)
	}
	expectMissing(t, b.store(t), "k")
	v, err := s.Create(ctx, "k", strings.NewReader("after"))
	if err != nil {
		t.Fatal(err)
	}
	expectDefinite(t, s.Delete(ctx, "k"), nil)
	expectHolds(t, b.store(t), "k", "after", v)

	// One refusal: the SDK's retry delivers the write.
	f2 := newFaults(&rule{method: http.MethodPut, times: 1, do: refuse})
	defer f2.close()
	v, err = b.storeVia(t, f2).Create(ctx, "k2", strings.NewReader("retried"))
	if err != nil {
		t.Fatalf("Create after one refused connection: %v", err)
	}
	expectHolds(t, b.store(t), "k2", "retried", v)
}

// TestBodyCut cuts the connection in the middle of the request body. S3
// never stores part of a PutObject, so the contract requires a definite
// error, nothing visible, and the key free for a later Create; one cut is
// retried by the SDK from the spool file. Then it cuts a Get's response body.
func TestBodyCut(t *testing.T) {
	content := strings.Repeat("c", 100<<10)
	b := newBucket(t)
	f := newFaults(&rule{method: http.MethodPut, times: 3, do: cutBody})
	defer f.close()
	s := b.storeVia(t, f)
	_, err := s.Create(ctx, "k", strings.NewReader(content))
	expectDefinite(t, err, nil)
	expectMissing(t, b.store(t), "k")
	v, err := s.Create(ctx, "k", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Create after the cut Creates: %v", err)
	}
	expectHolds(t, b.store(t), "k", content, v)

	f2 := newFaults(&rule{method: http.MethodPut, times: 1, do: cutBody})
	defer f2.close()
	v, err = b.storeVia(t, f2).Replace(ctx, "k", v, strings.NewReader(content+"2"))
	if err != nil {
		t.Fatalf("Replace after one cut body: %v", err)
	}
	expectHolds(t, b.store(t), "k", content+"2", v)

	// A read whose response is cut in the middle of its body: the body's Read
	// fails after a prefix of the version; it never ends as if it were whole.
	f3 := newFaults(&rule{method: http.MethodGet, times: 1, do: cutResponse})
	defer f3.close()
	body, _, err := b.storeVia(t, f3).Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	_ = body.Close()
	if err == nil || len(got) != 1000 || !strings.HasPrefix(content+"2", string(got)) {
		t.Fatalf("Get whose response was cut after 1000 bytes: read %d bytes, %v; want that prefix and an error", len(got), err)
	}
}

// cutResponse delivers the request and cuts the connection after 1,000 bytes
// of the response body.
func cutResponse(f *faults, req *http.Request) (*http.Response, error) {
	resp, err := f.next.RoundTrip(req)
	if err == nil {
		resp.Body = &failingBody{r: resp.Body, left: 1000}
	}
	return resp, err
}

// TestDeadline lets the context's deadline pass while a request is
// outstanding. The contract: once the request may have reached the service,
// the error wraps both ErrOutcomeUnknown and the context's error; before
// that, it wraps only the context's error and the write has no effect, then
// or later. A request still in flight when the call returned may commit
// afterwards, which is what the unknown outcome allows.
func TestDeadline(t *testing.T) {
	deadline := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, 500*time.Millisecond)
	}
	t.Run("response slower than the deadline", func(t *testing.T) {
		b := newBucket(t)
		f := newFaults(&rule{method: http.MethodPut, times: -1, do: stallAfter}, &rule{method: http.MethodDelete, times: -1, do: stallAfter})
		defer f.close()
		s := b.storeVia(t, f)
		c, cancel := deadline()
		defer cancel()
		_, err := s.Create(c, "k", strings.NewReader("payload"))
		expectUnknown(t, err)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want it to wrap context.DeadlineExceeded", err)
		}
		if !b.exists(t, "k") {
			t.Fatal("the delivered Create is missing")
		}
		c, cancel = deadline()
		defer cancel()
		err = s.Delete(c, "k")
		expectUnknown(t, err)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Delete error = %v, want it to wrap context.DeadlineExceeded", err)
		}
	})
	t.Run("request never sent", func(t *testing.T) {
		b := newBucket(t)
		f := newFaults(&rule{method: http.MethodPut, times: -1, do: stallBefore})
		defer f.close()
		c, cancel := deadline()
		defer cancel()
		_, err := b.storeVia(t, f).Create(c, "k", strings.NewReader("payload"))
		expectDefinite(t, err, context.DeadlineExceeded)
		time.Sleep(time.Second)
		expectMissing(t, b.store(t), "k")
	})
	t.Run("request still in flight", func(t *testing.T) {
		b := newBucket(t)
		release := make(chan struct{})
		f := newFaults(&rule{method: http.MethodPut, times: 1, do: deliverLate(release)}) //nolint:bodyclose // a fault rule, not a response
		defer f.close()
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce() // before f.close, so a failed check does not hang
		c, cancel := deadline()
		defer cancel()
		_, err := b.storeVia(t, f).Create(c, "k", strings.NewReader("late"))
		expectUnknown(t, err)
		expectMissing(t, b.store(t), "k")
		releaseOnce()
		f.wg.Wait()
		info, err := b.store(t).Head(ctx, "k")
		if err != nil || info.Size != 4 {
			t.Fatalf("after the late delivery Head = %+v, %v; want the write's 4 bytes", info, err)
		}
	})
}

// TestServerErrors answers mutations with 500 and 503, before and after the
// service applied them. A 5xx does not say whether the mutation took effect,
// so the contract allows only success established by the write's identity, or
// ErrOutcomeUnknown; never a definite error. The SDK retries 5xx, but not for
// a mutation: the first attempt might still be pending, or have applied.
func TestServerErrors(t *testing.T) {
	for _, c := range []struct {
		code   int
		s3Code string
	}{{500, "InternalError"}, {503, "SlowDown"}} {
		t.Run(fmt.Sprint(c.code), func(t *testing.T) {
			b := newBucket(t)
			check := b.store(t)
			for _, after := range []bool{false, true} {
				key := fmt.Sprint("k-", after)
				v0, err := check.Create(ctx, key, strings.NewReader("old"))
				if err != nil {
					t.Fatal(err)
				}
				f := newFaults(&rule{method: http.MethodPut, times: -1, do: status(c.code, c.s3Code, after)}, &rule{method: http.MethodDelete, times: -1, do: status(c.code, c.s3Code, after)}) //nolint:bodyclose // fault rules, not responses
				s := b.storeVia(t, f)
				v, createErr := s.Create(ctx, key+"-new", strings.NewReader("new"))
				v2, replaceErr := s.Replace(ctx, key, v0, strings.NewReader("replaced"))
				deleteErr := s.Delete(ctx, key)
				f.close()
				if n := f.count(http.MethodPut); n != 2 {
					t.Fatalf("answered %d (applied: %v): requests %v, want one PUT per write, not retried", c.code, after, f.seen)
				}
				expectUnknown(t, deleteErr)
				if !after {
					// Nothing was written, but the backend cannot know that.
					expectUnknown(t, createErr)
					expectMissing(t, check, key+"-new")
					expectUnknown(t, replaceErr)
					expectHolds(t, check, key, "old", v0)
					continue
				}
				// Applied: the identity check finds each write.
				if createErr != nil || replaceErr != nil {
					t.Fatalf("answered %d after applying: Create %v, Replace %v; want success through the identity check", c.code, createErr, replaceErr)
				}
				expectHolds(t, check, key+"-new", "new", v)
				expectMissing(t, check, key)
				if v2 == "" || v2 == v0 {
					t.Fatalf("Replace returned token %q after %q", v2, v0)
				}
			}
			// Answered after the service applied it, and the identity check
			// fails too.
			f := newFaults(&rule{method: http.MethodPut, times: -1, do: status(c.code, c.s3Code, true)}, &rule{method: http.MethodHead, times: -1, do: refuse}) //nolint:bodyclose // fault rules, not responses
			_, err := b.storeVia(t, f).Create(ctx, "always-after", strings.NewReader("x"))
			f.close()
			expectUnknown(t, err)
			if !b.exists(t, "always-after") {
				t.Fatal("the applied Create is missing")
			}
		})
	}
}

// undecodable delivers the request and corrupts a header of the service's
// successful answer, so the SDK fails to decode it: the write was applied but
// the call sees an error.
func undecodable(f *faults, req *http.Request) (*http.Response, error) {
	resp, err := f.next.RoundTrip(req)
	if err == nil {
		resp.Header.Set("X-Amz-Server-Side-Encryption-Bucket-Key-Enabled", "not-a-bool")
	}
	return resp, err
}

// TestUndecodableSuccess answers an applied write with a 2xx that the SDK
// cannot decode. The contract: the write took effect, so the error, if any,
// must wrap ErrOutcomeUnknown; the identity check turns it into success.
func TestUndecodableSuccess(t *testing.T) {
	b := newBucket(t)
	f := newFaults(&rule{method: http.MethodPut, times: 1, do: undecodable})
	defer f.close()
	v, err := b.storeVia(t, f).Create(ctx, "k", strings.NewReader("applied"))
	if err != nil {
		t.Fatalf("Create answered with an undecodable 200: %v, want success through the identity check", err)
	}
	expectHolds(t, b.store(t), "k", "applied", v)
	f2 := newFaults(&rule{method: http.MethodPut, times: 1, do: undecodable}, &rule{method: http.MethodHead, times: -1, do: refuse})
	defer f2.close()
	_, err = b.storeVia(t, f2).Replace(ctx, "k", v, strings.NewReader("applied again"))
	expectUnknown(t, err)
}

// TestBodyEndsWithContext ends the body and the context together. Nothing has
// been sent, so the contract requires the context's error and no effect. The
// SDK fails a call whose context has ended before it sends anything, and this
// test holds it to that.
func TestBodyEndsWithContext(t *testing.T) {
	b := newBucket(t)
	f := newFaults()
	defer f.close()
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	src := strings.NewReader("body")
	body := readerFunc(func(p []byte) (int, error) {
		n, _ := src.Read(p)
		if src.Len() == 0 {
			cancel()
			return n, io.EOF
		}
		return n, nil
	})
	_, err := b.storeVia(t, f).Create(c, "k", body)
	expectDefinite(t, err, context.Canceled)
	if len(f.seen) != 0 {
		t.Fatalf("requests %v, want none", f.seen)
	}
	expectMissing(t, b.store(t), "k")
}

type readerFunc func([]byte) (int, error)

func (r readerFunc) Read(p []byte) (int, error) { return r(p) }

// TestShortPages serves listings in pages of two entries. The contract: List
// keeps requesting until it has limit entries or the listing ends, in order
// and without duplicates. A page that claims more entries but holds none is
// an error, not the end of the listing.
func TestShortPages(t *testing.T) {
	b := newBucket(t)
	w := b.store(t)
	var want []string
	for i := range 25 {
		k := fmt.Sprintf("p/%02d", i)
		if _, err := w.Create(ctx, k, strings.NewReader(k)); err != nil {
			t.Fatal(err)
		}
		want = append(want, k)
	}
	f := newFaults(&rule{list: true, times: -1, do: shortPage(2)}) //nolint:bodyclose // fault rules, not responses
	defer f.close()
	s := b.storeVia(t, f)
	for _, tc := range []struct {
		startAfter string
		limit      int
		want       []string
	}{
		{"", 7, want[:7]},
		{"p/06", 7, want[7:14]},
		{"", 1000, want},
		{"p/22", 5, want[23:]},
	} {
		before := f.count(http.MethodGet)
		entries, err := s.List(ctx, "p/", tc.startAfter, tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, len(entries))
		for i, e := range entries {
			got[i] = e.Key
			if e.Size != 4 {
				t.Fatalf("entry %q size %d, want 4", e.Key, e.Size)
			}
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Fatalf("List(p/, %q, %d) in short pages = %q, want %q", tc.startAfter, tc.limit, got, tc.want)
		}
		if n, min := f.count(http.MethodGet)-before, (len(tc.want)+1)/2; n < min {
			t.Fatalf("List made %d requests, want at least %d pages of two", n, min)
		}
	}
	empty := newFaults(&rule{list: true, times: -1, do: shortPage(0)}) //nolint:bodyclose // fault rules, not responses
	defer empty.close()
	if _, err := b.storeVia(t, empty).List(ctx, "p/", "", 10); err == nil || errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("List whose page claims more but is empty: %v, want an error", err)
	}
}

// TestSDKRetries shows what the SDK retries on its own, per operation, and
// that each retry is classified as the outcome rule requires: a definite 4xx
// is not retried, 409 ConditionalRequestConflict and 400 RequestTimeout are
// (both say the write had no effect), and so are reads answered with 5xx or a
// reset connection, whose body and Info still come from one response. A
// mutation is not retried after a 5xx or a lost response (TestServerErrors,
// TestLostResponse), and is after a request that was never sent completely
// (TestRequestNotSent, TestBodyCut).
func TestSDKRetries(t *testing.T) {
	b := newBucket(t)
	plain := b.store(t)
	v, err := plain.Create(ctx, "k", strings.NewReader("content"))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("412 is not retried", func(t *testing.T) {
		f := newFaults()
		defer f.close()
		_, err := b.storeVia(t, f).Create(ctx, "k", strings.NewReader("again"))
		expectDefinite(t, err, storage.ErrPreconditionFailed)
		if f.count(http.MethodPut) != 1 || len(f.seen) != 1 {
			t.Fatalf("requests %v, want one PUT", f.seen)
		}
	})
	t.Run("missing key on Replace is not retried", func(t *testing.T) {
		f := newFaults()
		defer f.close()
		_, err := b.storeVia(t, f).Replace(ctx, "missing", v, strings.NewReader("x"))
		expectDefinite(t, err, storage.ErrPreconditionFailed)
		if len(f.seen) != 1 {
			t.Fatalf("requests %v, want one PUT", f.seen)
		}
	})
	for _, c := range []struct {
		code   int
		s3Code string
	}{{409, "ConditionalRequestConflict"}, {400, "RequestTimeout"}} {
		t.Run(c.s3Code, func(t *testing.T) {
			f := newFaults(&rule{method: http.MethodPut, times: 1, do: status(c.code, c.s3Code, false)}) //nolint:bodyclose // fault rules, not responses
			defer f.close()
			key := "retried-" + c.s3Code
			v, err := b.storeVia(t, f).Create(ctx, key, strings.NewReader("x"))
			if err != nil || f.count(http.MethodPut) != 2 {
				t.Fatalf("Create answered %d %s once: %v after requests %v, want success after a retry", c.code, c.s3Code, err, f.seen)
			}
			expectHolds(t, plain, key, "x", v)
			// Every attempt: no effect, and the error says so.
			f = newFaults(&rule{method: http.MethodPut, times: -1, do: status(c.code, c.s3Code, false)}) //nolint:bodyclose // fault rules, not responses
			defer f.close()
			_, err = b.storeVia(t, f).Create(ctx, key+"-always", strings.NewReader("x"))
			expectDefinite(t, err, nil)
			if f.count(http.MethodPut) != 3 {
				t.Fatalf("requests %v, want 3 PUT attempts", f.seen)
			}
		})
	}
	reads := []struct {
		name   string
		method string
		list   bool
		call   func(s *Store) error
	}{
		{"Get", http.MethodGet, false, func(s *Store) error {
			body, info, err := s.Get(ctx, "k")
			if err != nil {
				return err
			}
			defer func() { _ = body.Close() }()
			got, err := io.ReadAll(body)
			if err == nil && (string(got) != "content" || info.Version != v) {
				err = fmt.Errorf("got %q at %q", got, info.Version)
			}
			return err
		}},
		{"GetRange", http.MethodGet, false, func(s *Store) error {
			body, _, err := s.GetRange(ctx, "k", 1, 3)
			if err != nil {
				return err
			}
			defer func() { _ = body.Close() }()
			got, err := io.ReadAll(body)
			if err == nil && string(got) != "ont" {
				err = fmt.Errorf("got %q", got)
			}
			return err
		}},
		{"Head", http.MethodHead, false, func(s *Store) error {
			info, err := s.Head(ctx, "k")
			if err == nil && info.Version != v {
				err = fmt.Errorf("got %+v", info)
			}
			return err
		}},
		{"List", http.MethodGet, true, func(s *Store) error {
			entries, err := s.List(ctx, "k", "", 1)
			if err == nil && (len(entries) != 1 || entries[0].Size != 7) {
				err = fmt.Errorf("got %+v", entries)
			}
			return err
		}},
	}
	for _, r := range reads {
		for _, fault := range []struct {
			name string
			do   func(*faults, *http.Request) (*http.Response, error)
		}{{"503", status(503, "SlowDown", false)}, {"500", status(500, "InternalError", true)}, {"reset", lose}} { //nolint:bodyclose // fault rules, not responses
			t.Run(r.name+" "+fault.name, func(t *testing.T) {
				f := newFaults(&rule{method: r.method, list: r.list, times: 1, do: fault.do})
				defer f.close()
				if err := r.call(b.storeVia(t, f)); err != nil {
					t.Fatalf("%s retried after %s: %v", r.name, fault.name, err)
				}
				if f.count(r.method) != 2 {
					t.Fatalf("requests %v, want 2 %s attempts", f.seen, r.method)
				}
			})
		}
	}
}

// TestInterleavings forces the interleavings of the backend's conditional
// writes with other clients' mutations between the end of the body and the
// conditional request: the service evaluates the condition, so each must
// fail definitely with ErrPreconditionFailed and leave the other client's
// state.
func TestInterleavings(t *testing.T) {
	b := newBucket(t)
	other := b.store(t)
	t.Run("Delete before Replace", func(t *testing.T) {
		v, err := other.Create(ctx, "rd", strings.NewReader("old"))
		if err != nil {
			t.Fatal(err)
		}
		f := newFaults(&rule{method: http.MethodPut, times: 1, do: before(func() { //nolint:bodyclose // fault rules, not responses
			if err := other.Delete(ctx, "rd"); err != nil {
				t.Error(err)
			}
		})})
		defer f.close()
		_, err = b.storeVia(t, f).Replace(ctx, "rd", v, strings.NewReader("resurrected"))
		expectDefinite(t, err, storage.ErrPreconditionFailed)
		expectMissing(t, other, "rd")
	})
	t.Run("Create before Create", func(t *testing.T) {
		var v string
		f := newFaults(&rule{method: http.MethodPut, times: 1, do: before(func() { //nolint:bodyclose // fault rules, not responses
			var err error
			if v, err = other.Create(ctx, "cc", strings.NewReader("first")); err != nil {
				t.Error(err)
			}
		})})
		defer f.close()
		_, err := b.storeVia(t, f).Create(ctx, "cc", strings.NewReader("second"))
		expectDefinite(t, err, storage.ErrPreconditionFailed)
		expectHolds(t, other, "cc", "first", v)
	})
	t.Run("Replace before Replace", func(t *testing.T) {
		v, err := other.Create(ctx, "rr", strings.NewReader("old"))
		if err != nil {
			t.Fatal(err)
		}
		var v2 string
		f := newFaults(&rule{method: http.MethodPut, times: 1, do: before(func() { //nolint:bodyclose // fault rules, not responses
			if v2, err = other.Replace(ctx, "rr", v, strings.NewReader("theirs")); err != nil {
				t.Error(err)
			}
		})})
		defer f.close()
		_, err = b.storeVia(t, f).Replace(ctx, "rr", v, strings.NewReader("mine"))
		expectDefinite(t, err, storage.ErrPreconditionFailed)
		expectHolds(t, other, "rr", "theirs", v2)
	})
}

// TestSecretsNotInErrors signs with wrong credentials whose secret is
// distinctive and checks that no error text carries the secret or the access
// key.
func TestSecretsNotInErrors(t *testing.T) {
	b := newBucket(t)
	const access, secret = "AKIDSKYLITETESTKEY01", "skylite-test-secret-4f1c9a" //nolint:gosec // made up, to be searched for in error text
	cfg := b.cfg
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: access, SecretAccessKey: secret}, nil
	})
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, e1 := s.Create(ctx, "k", strings.NewReader("x"))
	_, e2 := s.Replace(ctx, "k", `"abc"`, strings.NewReader("x"))
	_, _, e3 := s.Get(ctx, "k")
	_, e4 := s.Head(ctx, "k")
	_, e5 := s.List(ctx, "", "", 1)
	e6 := s.Delete(ctx, "k")
	for i, err := range []error{e1, e2, e3, e4, e5, e6} {
		if err == nil {
			t.Fatalf("call %d with wrong credentials succeeded", i)
		}
		if msg := err.Error(); strings.Contains(msg, secret) || strings.Contains(msg, access) {
			t.Fatalf("error %q contains a credential", msg)
		}
	}
}

// TestNoGoroutineOutlivesCalls runs every operation, a stalled write and a
// body closed early, and checks that once the connections are idle and
// closed no goroutine started by them is left.
func TestNoGoroutineOutlivesCalls(t *testing.T) {
	b := newBucket(t)
	f := newFaults(&rule{method: http.MethodPut, times: 1, do: stallAfter})
	s := b.storeVia(t, f)
	baseline := runtime.NumGoroutine()
	c, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := s.Create(c, "stalled", strings.NewReader("x"))
	cancel()
	expectUnknown(t, err)
	v, err := s.Create(ctx, "k", strings.NewReader(strings.Repeat("x", 1<<20)))
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = body.Read(make([]byte, 10))
	_ = body.Close()
	if _, err := s.Replace(ctx, "k", v, strings.NewReader("y")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(ctx, "", "", 10); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	f.close()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseline {
		buf := make([]byte, 1<<16)
		t.Fatalf("%d goroutines, %d before the calls:\n%s", n, baseline, buf[:runtime.Stack(buf, true)])
	}
}

// TestWriteMemoryBound writes a 64 MiB body that is never in memory as a
// whole and checks that the write allocated far less than the body: the
// body goes to the spool file and from there to the service.
func TestWriteMemoryBound(t *testing.T) {
	const size = 64 << 20
	b := newBucket(t)
	s := b.store(t)
	zeros := io.LimitReader(zeroReader{}, size)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	v, err := s.Create(ctx, "big", zeros)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 8<<20 {
		t.Fatalf("writing %d bytes allocated %d bytes", size, alloc)
	} else {
		t.Logf("writing %d bytes allocated %d bytes", size, alloc)
	}
	if info, err := s.Head(ctx, "big"); err != nil || info != (storage.Info{Size: size, Version: v}) {
		t.Fatalf("Head = %+v, %v", info, err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }
