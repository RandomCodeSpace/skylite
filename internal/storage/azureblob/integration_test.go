//go:build integration

package azureblob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/storagetest"
)

// endpointEnv names the Azurite blob endpoint, for example
// http://127.0.0.1:10000/devstoreaccount1. `make integration` sets it.
const endpointEnv = "SKYLITE_TEST_AZURITE_BLOB_ENDPOINT"

// Azurite's documented default development account. The key is public and
// only works against the emulator.
const (
	azuriteAccount = "devstoreaccount1"
	azuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" //nolint:gosec // Azurite's public development key
)

// newContainer creates a container unique to the test, removes it after the
// test and returns a SAS URL for it. A SAS signs only its own query
// parameters, so the fault injector may rewrite the others, and its signature
// is a secret that errors must not show.
func newContainer(t *testing.T) string {
	t.Helper()
	ep := os.Getenv(endpointEnv)
	if ep == "" {
		t.Skipf("%s is not set; run `make integration` or point it at an Azurite blob endpoint", endpointEnv)
	}
	cred, err := service.NewSharedKeyCredential(azuriteAccount, azuriteKey)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.NewClientWithSharedKeyCredential(ep, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	name := "azureblob-" + hex.EncodeToString(b)
	if _, err := svc.CreateContainer(context.Background(), name, nil); err != nil {
		t.Fatal(redact(err))
	}
	t.Cleanup(func() { _, _ = svc.DeleteContainer(context.Background(), name, nil) })
	qp, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPSandHTTP,
		ExpiryTime:    time.Now().Add(24 * time.Hour),
		Permissions:   (&sas.ContainerPermissions{Read: true, Add: true, Create: true, Write: true, Delete: true, List: true}).String(),
		ContainerName: name,
	}.SignWithSharedKey(cred)
	if err != nil {
		t.Fatal(err)
	}
	return ep + "/" + name + "?" + qp.Encode()
}

// newClient returns a container client with its own transport, wrapped by
// wrap if it is not nil. Read retries wait 10 ms, so retried reads stay fast.
func newClient(t *testing.T, sasURL string, wrap func(http.RoundTripper) http.RoundTripper) (*container.Client, *http.Transport) {
	t.Helper()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// A mutation is never retried, so the transport must drop an idle
	// connection before Azurite does, after 5 s (see TestIdleConnection).
	tr.IdleConnTimeout = 2 * time.Second
	var rt http.RoundTripper = tr
	if wrap != nil {
		rt = wrap(tr)
	}
	c, err := container.NewClientWithNoCredential(sasURL, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: &http.Client{Transport: rt},
		Retry:     policy.RetryOptions{RetryDelay: 10 * time.Millisecond, MaxRetryDelay: 50 * time.Millisecond},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return c, tr
}

func newStore(t *testing.T, sasURL string, wrap func(http.RoundTripper) http.RoundTripper) *Store {
	t.Helper()
	c, tr := newClient(t, sasURL, wrap)
	t.Cleanup(tr.CloseIdleConnections)
	return New(c, t.TempDir())
}

// requestCount wraps a transport and counts every HTTP request through it.
type requestCount struct {
	next http.RoundTripper
	n    *atomic.Int64
}

func (c requestCount) RoundTrip(req *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.next.RoundTrip(req)
}

func TestConformance(t *testing.T) {
	var requests atomic.Int64
	start := time.Now()
	t.Cleanup(func() {
		t.Logf("%d HTTP requests to Azurite in %v", requests.Load(), time.Since(start).Round(time.Second))
	})
	count := func(next http.RoundTripper) http.RoundTripper { return requestCount{next, &requests} }
	storagetest.Run(t, func(t *testing.T) func() storage.Store {
		u := newContainer(t)
		spool := t.TempDir()
		// Every client gets its own transport, so clients share no connections.
		var mu sync.Mutex
		var transports []*http.Transport
		t.Cleanup(func() {
			mu.Lock()
			defer mu.Unlock()
			for _, tr := range transports {
				tr.CloseIdleConnections()
			}
		})
		return func() storage.Store {
			c, tr := newClient(t, u, count)
			mu.Lock()
			transports = append(transports, tr)
			mu.Unlock()
			return New(c, spool)
		}
	})
}

// injector sits between the SDK and Azurite. fault, when set, handles every
// request instead of passing it on; requests counts each request it sees by
// kind.
type injector struct {
	next  http.RoundTripper
	mu    sync.Mutex
	fault func(req *http.Request, next http.RoundTripper) (*http.Response, error)
	seen  map[string]int
}

func (in *injector) RoundTrip(req *http.Request) (*http.Response, error) {
	in.mu.Lock()
	in.seen[kind(req)]++
	fault := in.fault
	in.mu.Unlock()
	if fault != nil {
		return fault(req, in.next)
	}
	return in.next.RoundTrip(req)
}

// set installs fault and resets the request counts.
func (in *injector) set(fault func(req *http.Request, next http.RoundTripper) (*http.Response, error)) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.fault = fault
	in.seen = map[string]int{}
}

func (in *injector) count(k string) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.seen[k]
}

// kind names the Blob service operation of req.
func kind(req *http.Request) string {
	switch {
	case req.URL.Query().Get("comp") == "list":
		return "list"
	case req.Method == http.MethodPut:
		return "put"
	case req.Method == http.MethodDelete:
		return "delete"
	case req.Method == http.MethodHead:
		return "head"
	default:
		return "get"
	}
}

// faultStore returns a Store on a new container whose requests go through an
// injector, and a plain Store on the same container to check the outcome.
func faultStore(t *testing.T) (*Store, *injector, *Store, string) {
	t.Helper()
	u := newContainer(t)
	in := &injector{seen: map[string]int{}}
	s := newStore(t, u, func(next http.RoundTripper) http.RoundTripper { in.next = next; return in })
	return s, in, newStore(t, u, nil), u
}

// faultFunc handles a request in place of the transport.
type faultFunc func(req *http.Request, next http.RoundTripper) (*http.Response, error)

// response is a synthetic service response with an error code and no body.
func response(req *http.Request, status int, code string) *http.Response {
	h := http.Header{}
	h.Set("x-ms-error-code", code)
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: h, Body: http.NoBody, Request: req,
	}
}

// errInjected is a transport error that is not a dial error, so it proves
// nothing about the request whatever the trace shows.
var errInjected error = &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer (injected)")}

// lostResponse sends the request and drops the service's answer.
func lostResponse(req *http.Request, next http.RoundTripper) (*http.Response, error) {
	resp, err := next.RoundTrip(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	return nil, errInjected
}

// statusAfter sends the request and replaces the answer with status.
func statusAfter(status int, code string) faultFunc {
	return func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		resp, err := next.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		return response(req, status, code), nil
	}
}

// statusBefore reads the whole request, as a server does, and answers status
// without passing it on, reporting in the request's trace the one connection
// that a server's answer arrives on.
func statusBefore(status int, code string) faultFunc {
	return func(req *http.Request, _ http.RoundTripper) (*http.Response, error) {
		if tr := httptrace.ContextClientTrace(req.Context()); tr != nil && tr.GotConn != nil {
			tr.GotConn(httptrace.GotConnInfo{})
		}
		if req.Body != nil {
			_, _ = io.Copy(io.Discard, req.Body)
			_ = req.Body.Close()
		}
		return response(req, status, code), nil
	}
}

// refused dials a closed local port for the request: the dial fails with
// net/http's dial error, so nothing is sent.
func refused(t *testing.T) faultFunc {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		tr := next.(*http.Transport).Clone()
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
		defer tr.CloseIdleConnections()
		return tr.RoundTrip(req)
	}
}

// noTrace sends the request with a context that carries no client trace, as
// a RoundTripper that is not net/http's may, and answers with the context's
// error once the call's context has ended.
func noTrace(req *http.Request, next http.RoundTripper) (*http.Response, error) {
	resp, err := next.RoundTrip(req.Clone(context.Background()))
	if err == nil {
		_ = resp.Body.Close()
	}
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// noTraceAnswer sends the request with a context that carries no client
// trace, as noTrace does, and passes the service's answer on, changed by alter.
func noTraceAnswer(alter func(*http.Response)) faultFunc {
	return func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		resp, err := next.RoundTrip(req.Clone(context.Background()))
		if err == nil {
			alter(resp)
		}
		return resp, err
	}
}

// failBody replaces the answer's body with one that fails, as when the
// connection breaks while the body is read.
func failBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
}

// unsent fails the request without reading its body or sending it.
func unsent(*http.Request, http.RoundTripper) (*http.Response, error) { return nil, errInjected }

// slowBound is the longest slowResponse holds a request whose context does
// not end: a call that takes that long has ignored its context.
const slowBound = 5 * time.Second

// slowResponse sends the request and then waits for its context to end, or
// fails it after slowBound.
func slowResponse(req *http.Request, next http.RoundTripper) (*http.Response, error) {
	resp, err := next.RoundTrip(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-time.After(slowBound):
		return nil, errInjected
	}
}

// cutBody sends the request with a body that fails after n bytes, as when the
// connection breaks in the middle of the body.
func cutBody(n int64) faultFunc {
	return func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		r := req.Clone(req.Context())
		r.Body = io.NopCloser(io.MultiReader(io.LimitReader(req.Body, n), iotestErr{}))
		r.GetBody = nil
		return next.RoundTrip(r)
	}
}

type iotestErr struct{}

func (iotestErr) Read([]byte) (int, error) { return 0, errInjected }

func content(tag string, n int) string {
	s := strings.Repeat(tag+"-", n/(len(tag)+1)+1)
	return s[:n]
}

// Sizes for the fault tests: one body held in memory, one spooled to a file.
var faultSizes = []int{11, memLimit + 3}

// TestMutationFaults injects a fault into the one request of each mutation
// and checks the outcome rule of the storage contract: nil means the mutation
// took effect; an error wrapping ErrOutcomeUnknown means it may or may not
// have; any other error means it had no effect. The SDK must not retry a
// mutation, so each sends exactly one request: a retry after a lost response
// could meet the write's own effect and turn a success into a precondition
// failure, or remove a later Create of a deleted key.
func TestMutationFaults(t *testing.T) {
	type outcome int
	const (
		noEffect     outcome = iota // a definite error, and the store unchanged
		unknownDone                 // ErrOutcomeUnknown, and the effect applied
		unknownNoEff                // ErrOutcomeUnknown, and the store unchanged
	)
	cases := []struct {
		name  string
		fault func(t *testing.T) faultFunc
		// want per operation: create, replace, delete.
		want     [3]outcome
		deadline bool // the call's context has a short deadline, which the error must wrap
	}{
		// The service applied the mutation and its answer was lost: only
		// ErrOutcomeUnknown is allowed.
		{"LostResponse", func(*testing.T) faultFunc {
			return lostResponse
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		// A 5xx says nothing definite about the effect, before or after it.
		{"500AfterEffect", func(*testing.T) faultFunc {
			return statusAfter(500, "InternalError")
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		{"503AfterEffect", func(*testing.T) faultFunc {
			return statusAfter(503, "ServerBusy")
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		{"500BeforeEffect", func(*testing.T) faultFunc {
			return statusBefore(500, "InternalError")
		}, [3]outcome{unknownNoEff, unknownNoEff, unknownNoEff}, false},
		{"503BeforeEffect", func(*testing.T) faultFunc {
			return statusBefore(503, "ServerBusy")
		}, [3]outcome{unknownNoEff, unknownNoEff, unknownNoEff}, false},
		// A 408 is a timeout, which says nothing definite either.
		{"408AfterEffect", func(*testing.T) faultFunc {
			return statusAfter(408, "OperationTimedOut")
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		{"408BeforeEffect", func(*testing.T) faultFunc {
			return statusBefore(408, "OperationTimedOut")
		}, [3]outcome{unknownNoEff, unknownNoEff, unknownNoEff}, false},
		// Any other 4xx is a refusal, a definite error; a 409 is a
		// precondition failure only with the code BlobAlreadyExists.
		{"400", func(*testing.T) faultFunc {
			return statusBefore(400, "InvalidHeaderValue")
		}, [3]outcome{noEffect, noEffect, noEffect}, false},
		{"403", func(*testing.T) faultFunc {
			return statusBefore(403, "AuthorizationFailure")
		}, [3]outcome{noEffect, noEffect, noEffect}, false},
		{"409LeaseIdMissing", func(*testing.T) faultFunc {
			return statusBefore(409, "LeaseIdMissing")
		}, [3]outcome{noEffect, noEffect, noEffect}, false},
		// A success status other than the one the request expects (201 for
		// Put Blob, 202 for Delete Blob), as a non-conforming proxy may send
		// after the effect, is no definite error.
		{"200AfterEffect", func(*testing.T) faultFunc {
			return statusAfter(200, "")
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		// The deadline passes after the request was sent: unknown, and the
		// error wraps the context's error.
		{"SlowResponse", func(*testing.T) faultFunc {
			return slowResponse
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, true},
		// The same through a transport that reports no connection trace: the
		// SDK returns the context's error in place of the transport's.
		{"NoTraceDeadline", func(*testing.T) faultFunc {
			return noTrace
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, true},
		// The service applied the request and answered it, through a transport
		// that reports no connection trace, and then the answer's body fails
		// or a header of its success does not parse: unknown.
		{"NoTraceBodyFails", func(*testing.T) faultFunc {
			return noTraceAnswer(failBody)
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		{"NoTraceBadDate", func(*testing.T) faultFunc {
			return noTraceAnswer(func(resp *http.Response) { resp.Header.Set("Date", "not a date") })
		}, [3]outcome{unknownDone, unknownDone, unknownDone}, false},
		// The connection was refused, net/http's dial error, so nothing was
		// sent: a definite error.
		{"Refused", refused, [3]outcome{noEffect, noEffect, noEffect}, false},
		// The request failed before its body was read: nothing reached the
		// service, a definite error. A Delete has no body, and the error
		// proves nothing about whether it was sent: unknown.
		{"Unsent", func(*testing.T) faultFunc {
			return unsent
		}, [3]outcome{noEffect, noEffect, unknownNoEff}, false},
		// The connection broke in the middle of the body: the service never
		// got the whole request, a definite error. (A Delete has no body; it
		// is sent whole, and its answer lost, as in LostResponse.)
		{"CutInBody", func(*testing.T) faultFunc {
			return cutBody(5)
		}, [3]outcome{noEffect, noEffect, unknownDone}, false},
	}
	ops := [3]string{"create", "replace", "delete"}
	for _, tc := range cases {
		for _, n := range faultSizes {
			for i, op := range ops {
				if op == "delete" && n != faultSizes[0] {
					continue // a Delete has no body
				}
				t.Run(fmt.Sprintf("%s/%s/%d", tc.name, op, n), func(t *testing.T) {
					s, in, check, _ := faultStore(t)
					old, next := content("old", n), content("new", n)
					var v string
					if op != "create" {
						var err error
						if v, err = check.Create(context.Background(), "k", strings.NewReader(old)); err != nil {
							t.Fatal(err)
						}
					}
					f := tc.fault(t)
					if tc.name == "CutInBody" && op == "delete" {
						f = lostResponse
					}
					in.set(f)
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					if tc.deadline {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
					}
					defer cancel()
					start := time.Now()
					var err error
					switch op {
					case "create":
						_, err = s.Create(ctx, "k", strings.NewReader(next))
					case "replace":
						_, err = s.Replace(ctx, "k", v, strings.NewReader(next))
					case "delete":
						err = s.Delete(ctx, "k")
					}
					sent := in.count(map[string]string{"create": "put", "replace": "put", "delete": "delete"}[op])
					in.set(nil)
					want := tc.want[i]
					expectStorageError(t, err, op, "k")
					unknown := errors.Is(err, storage.ErrOutcomeUnknown)
					if unknown != (want != noEffect) {
						t.Fatalf("error %v: wraps ErrOutcomeUnknown = %v, want %v", err, unknown, want != noEffect)
					}
					if errors.Is(err, storage.ErrPreconditionFailed) {
						t.Fatalf("error %v wraps ErrPreconditionFailed", err)
					}
					if tc.deadline && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("error %v does not wrap context.DeadlineExceeded", err)
					}
					if d := time.Since(start); tc.deadline && d >= slowBound {
						t.Fatalf("returned after %v: the call ignored its context's deadline", d)
					}
					if sent != 1 {
						t.Fatalf("%d requests sent, want 1: the SDK must not retry a mutation", sent)
					}
					// The effect, or its absence, as the store shows it.
					applied := want == unknownDone
					switch {
					case op == "delete" && applied:
						expectMissing(t, check, "k")
					case op == "delete", op == "replace" && !applied:
						expectContent(t, check, "k", old)
					case op == "create" && !applied:
						expectMissing(t, check, "k")
					default:
						expectContent(t, check, "k", next)
					}
				})
			}
		}
	}
}

// TestPendingRequestAfterReturn cancels a write while the transport is still
// sending its body, then lets the transport go on. The call must return a
// definite error (the context's), and the request it leaves behind must never
// take effect: neither then nor later, and a later Create of the key succeeds.
func TestPendingRequestAfterReturn(t *testing.T) {
	for _, n := range faultSizes {
		for _, op := range []string{"create", "replace"} {
			t.Run(fmt.Sprintf("%s/%d", op, n), func(t *testing.T) {
				s, in, check, _ := faultStore(t)
				old := content("old", n)
				var v string
				if op == "replace" {
					var err error
					if v, err = check.Create(context.Background(), "k", strings.NewReader(old)); err != nil {
						t.Fatal(err)
					}
				}
				stalled, release := make(chan struct{}), make(chan struct{})
				var sent sync.WaitGroup
				sent.Add(1)
				type result struct {
					resp *http.Response
					err  error
				}
				results := make(chan result, 1)
				in.set(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
					r := req.Clone(req.Context())
					r.Body = io.NopCloser(io.MultiReader(io.LimitReader(req.Body, 5), &gate{stalled, release}, req.Body))
					r.GetBody = nil
					// The request goes on in the background, as a pending
					// request does in net/http after its call returned.
					go func() {
						defer sent.Done()
						resp, err := next.RoundTrip(r.WithContext(context.WithoutCancel(req.Context()))) //nolint:bodyclose // closed below, after sent.Wait
						results <- result{resp, err}
					}()
					select {
					case res := <-results:
						return res.resp, res.err
					case <-req.Context().Done():
						return nil, req.Context().Err()
					}
				})
				ctx, cancel := context.WithCancel(context.Background())
				go func() { <-stalled; cancel() }()
				var err error
				if op == "create" {
					_, err = s.Create(ctx, "k", strings.NewReader(content("new", n)))
				} else {
					_, err = s.Replace(ctx, "k", v, strings.NewReader(content("new", n)))
				}
				expectStorageError(t, err, op, "k")
				if errors.Is(err, storage.ErrOutcomeUnknown) || !errors.Is(err, context.Canceled) {
					t.Fatalf("error %v: want a definite error wrapping context.Canceled", err)
				}
				close(release)
				sent.Wait()
				select {
				case res := <-results:
					if res.err == nil {
						_ = res.resp.Body.Close()
						t.Fatalf("the request left behind completed with status %d", res.resp.StatusCode)
					}
				default: // the call itself took the result
				}
				in.set(nil)
				if op == "create" {
					expectMissing(t, check, "k")
					if _, err := check.Create(context.Background(), "k", strings.NewReader("later")); err != nil {
						t.Fatalf("a later Create failed: %v", err)
					}
				} else {
					expectContent(t, check, "k", old)
				}
			})
		}
	}
}

// gate blocks its first Read until release is closed, after closing stalled.
type gate struct {
	stalled, release chan struct{}
}

func (g *gate) Read([]byte) (int, error) {
	close(g.stalled)
	<-g.release
	return 0, io.EOF
}

// TestReadRetries checks that reads keep the SDK's retries: a read is
// repeated after a 503 or a lost response and succeeds, since repeating a
// read has no effect. (TestMutationFaults checks that mutations are not
// repeated.)
func TestReadRetries(t *testing.T) {
	for _, fault := range []string{"503", "lost"} {
		t.Run(fault, func(t *testing.T) {
			s, in, check, _ := faultStore(t)
			v, err := check.Create(context.Background(), "k", strings.NewReader("0123456789"))
			if err != nil {
				t.Fatal(err)
			}
			once := func() faultFunc {
				var failed atomic.Bool
				return func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
					if failed.Swap(true) {
						return next.RoundTrip(req)
					}
					if fault == "503" {
						return statusAfter(503, "ServerBusy")(req, next)
					}
					return lostResponse(req, next)
				}
			}
			in.set(once())
			body, info, err := s.Get(context.Background(), "k")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got := readAll(t, body); got != "0123456789" || info != (storage.Info{Size: 10, Version: v}) {
				t.Fatalf("Get = %q, %+v", got, info)
			}
			expectRequests(t, in, "get", 2)
			in.set(once())
			body, info, err = s.GetRange(context.Background(), "k", 2, 3)
			if err != nil {
				t.Fatalf("GetRange: %v", err)
			}
			if got := readAll(t, body); got != "234" || info != (storage.Info{Size: 10, Version: v}) {
				t.Fatalf("GetRange = %q, %+v", got, info)
			}
			expectRequests(t, in, "get", 2)
			in.set(once())
			if info, err := s.Head(context.Background(), "k"); err != nil || info != (storage.Info{Size: 10, Version: v}) {
				t.Fatalf("Head = %+v, %v", info, err)
			}
			expectRequests(t, in, "head", 2)
			in.set(once())
			if entries, err := s.List(context.Background(), "", "", 10); err != nil || !slices.Equal(entries, []storage.Entry{{Key: "k", Size: 10}}) {
				t.Fatalf("List = %v, %v", entries, err)
			}
			expectRequests(t, in, "list", 2)
		})
	}
}

// TestReadDeadline: a read whose response is slower than its context's
// deadline fails with an error wrapping context.DeadlineExceeded, and never
// with ErrOutcomeUnknown, which is for mutations only.
func TestReadDeadline(t *testing.T) {
	s, in, check, _ := faultStore(t)
	if _, err := check.Create(context.Background(), "k", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	in.set(slowResponse)
	for _, op := range []string{"get", "head", "list"} {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		start := time.Now()
		var err error
		key := "k"
		switch op {
		case "get":
			_, _, err = s.Get(ctx, "k")
		case "head":
			_, err = s.Head(ctx, "k")
		case "list":
			_, err = s.List(ctx, "", "", 1)
			key = ""
		}
		cancel()
		if d := time.Since(start); d >= slowBound {
			t.Fatalf("%s returned after %v: it ignored its context's deadline", op, d)
		}
		expectStorageError(t, err, op, key)
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Fatalf("%s: error %v, want one wrapping context.DeadlineExceeded and not ErrOutcomeUnknown", op, err)
		}
	}
}

// TestBodyAfterCancel: once the context of an open body has ended, a Read
// that would return more data fails with the context's error, also when the
// response is small enough that the transport already holds all of it.
func TestBodyAfterCancel(t *testing.T) {
	s, _, check, _ := faultStore(t)
	if _, err := check.Create(context.Background(), "k", strings.NewReader("0123456789a")); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"get", "range"} {
		ctx, cancel := context.WithCancel(context.Background())
		var body io.ReadCloser
		var err error
		if op == "get" {
			body, _, err = s.Get(ctx, "k")
		} else {
			body, _, err = s.GetRange(ctx, "k", 1, 5)
		}
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		n, err := body.Read(make([]byte, 64))
		_ = body.Close()
		if n != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: Read after cancel = %d bytes, %v; want 0 bytes and context.Canceled", op, n, err)
		}
	}
}

// TestShortPages makes the service return at most 2 entries per List Blobs
// response, as Azure may at a partition boundary. List must keep requesting
// until it has limit entries or the listing ends, with and without prefix and
// startAfter. A second listing longer than storage.MaxListLimit, in pages of
// 400, must come back whole in pages of the requested limit.
func TestShortPages(t *testing.T) {
	s, in, check, _ := faultStore(t)
	var all []string
	for i := range 11 {
		all = append(all, fmt.Sprintf("p/%02d", i))
	}
	all = append(all, "a", "q")
	for _, k := range all {
		if _, err := check.Create(context.Background(), k, strings.NewReader(k)); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(all)
	shortPages := func(max string) faultFunc {
		return func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
			if kind(req) == "list" {
				r := req.Clone(req.Context())
				q := r.URL.Query()
				q.Set("maxresults", max)
				r.URL.RawQuery = q.Encode()
				return next.RoundTrip(r)
			}
			return next.RoundTrip(req)
		}
	}
	for _, tc := range []struct {
		prefix, startAfter string
		limit              int
	}{{"", "", 5}, {"", "", 1000}, {"p/", "", 7}, {"p/", "p/03", 4}, {"", "p/09", 3}, {"p/", "a", 20}, {"p/", "p/10", 5}} {
		in.set(shortPages("2"))
		got, err := s.List(context.Background(), tc.prefix, tc.startAfter, tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, k := range all {
			if strings.HasPrefix(k, tc.prefix) && k > tc.startAfter && len(want) < tc.limit {
				want = append(want, k)
			}
		}
		if keys := entryKeys(got); !slices.Equal(keys, want) {
			t.Fatalf("List(%q, %q, %d) = %q, want %q", tc.prefix, tc.startAfter, tc.limit, keys, want)
		}
		if in.count("list") < 2 && len(want) > 2 {
			t.Fatalf("List(%q, %q, %d) made %d requests: the pages were not short", tc.prefix, tc.startAfter, tc.limit, in.count("list"))
		}
	}

	// A later page that keeps failing, after the read retries, fails the
	// whole List: the entries of the pages before are not a listing.
	var firstPage atomic.Bool
	in.set(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		if kind(req) == "list" && firstPage.Swap(true) {
			return response(req, http.StatusServiceUnavailable, "ServerBusy"), nil
		}
		return shortPages("2")(req, next)
	})
	if got, err := s.List(context.Background(), "", "", 5); err == nil || got != nil {
		t.Fatalf("List with a failing second page = %q, %v; want no entries and an error", entryKeys(got), err)
	}

	// More keys than storage.MaxListLimit, in service pages of 400.
	in.set(nil)
	var many []string
	for i := range storage.MaxListLimit + 3 {
		many = append(many, fmt.Sprintf("m/%04d", i))
	}
	createParallel(t, check, many)
	in.set(shortPages("400"))
	var got []string
	after := ""
	for {
		page, err := s.List(context.Background(), "m/", after, storage.MaxListLimit)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, entryKeys(page)...)
		if len(page) < storage.MaxListLimit {
			break
		}
		after = page[len(page)-1].Key
	}
	if !slices.Equal(got, many) {
		t.Fatalf("paged listing returned %d keys, want %d in order", len(got), len(many))
	}
	if in.count("list") < 4 {
		t.Fatalf("%d List Blobs requests, want at least 4 with pages of 400", in.count("list"))
	}
}

func createParallel(t *testing.T, s *Store, keys []string) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, len(keys))
	sem := make(chan struct{}, 8)
	for _, k := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := s.Create(context.Background(), k, strings.NewReader(k)); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestRangeGrewAfter416 answers the first ranged read with 416, as the
// service does when the offset is past the end, although the blob is longer:
// it grew after that read. GetRange must not return an empty body for an
// offset inside the object; it reads the blob's size and reads the range
// again.
func TestRangeGrewAfter416(t *testing.T) {
	s, in, check, _ := faultStore(t)
	v, err := check.Create(context.Background(), "k", strings.NewReader("0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	var first atomic.Bool
	in.set(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		if kind(req) == "get" && !first.Swap(true) {
			return response(req, http.StatusRequestedRangeNotSatisfiable, "InvalidRange"), nil
		}
		return next.RoundTrip(req)
	})
	body, info, err := s.GetRange(context.Background(), "k", 5, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, body); got != "567" || info != (storage.Info{Size: 10, Version: v}) {
		t.Fatalf("GetRange = %q, %+v, want \"567\" and size 10", got, info)
	}
	expectRequests(t, in, "get", 2)
	expectRequests(t, in, "head", 1)
}

// TestRedaction: errors never contain the SAS signature, from a transport
// failure (whose text quotes the request URL) or from a service response
// (whose body can quote the signature).
func TestRedaction(t *testing.T) {
	s, in, _, u := faultStore(t)
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	sig := parsed.Query().Get("sig")
	if sig == "" {
		t.Fatal("no signature in the SAS URL")
	}
	secrets := []string{sig, url.QueryEscape(sig), "sig="}
	check := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: no error", what)
		}
		for _, secret := range secrets {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("%s: error %q contains the SAS signature", what, err)
			}
		}
	}
	errs := func(what string) {
		_, err := s.Create(context.Background(), "k", strings.NewReader("x"))
		check(what+" create", err)
		_, err = s.Replace(context.Background(), "k", `"v"`, strings.NewReader("x"))
		check(what+" replace", err)
		_, _, err = s.Get(context.Background(), "k")
		check(what+" get", err)
		_, _, err = s.GetRange(context.Background(), "k", 0, 1)
		check(what+" range", err)
		_, _, err = s.GetRange(context.Background(), "k", 0, 0)
		check(what+" empty range", err)
		_, err = s.Head(context.Background(), "k")
		check(what+" head", err)
		_, err = s.List(context.Background(), "", "", 1)
		check(what+" list", err)
		check(what+" delete", s.Delete(context.Background(), "k"))
	}
	in.set(refused(t))
	errs("refused")
	// A wrong signature: Azurite answers 403 AuthenticationFailed.
	in.set(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		r := req.Clone(req.Context())
		q := r.URL.Query()
		q.Set("sig", strings.Repeat("A", 43)+"=")
		r.URL.RawQuery = q.Encode()
		return next.RoundTrip(r)
	})
	errs("forbidden")
}

// TestSizeLimit lowers the Put Blob limit: a body one byte over it fails with
// ErrInvalidArgument before any request, a body at the limit is written.
func TestSizeLimit(t *testing.T) {
	s, in, check, _ := faultStore(t)
	defer func(old int64) { maxBody = old }(maxBody)
	maxBody = memLimit + 10
	in.set(nil)
	_, err := s.Create(context.Background(), "k", strings.NewReader(content("x", int(maxBody)+1)))
	expectStorageError(t, err, "create", "k")
	if !errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("error %v, want ErrInvalidArgument", err)
	}
	expectRequests(t, in, "put", 0)
	expectMissing(t, check, "k")
	if _, err := s.Create(context.Background(), "k", strings.NewReader(content("x", int(maxBody)))); err != nil {
		t.Fatal(err)
	}
	expectContent(t, check, "k", content("x", int(maxBody)))
}

// TestSpool: a body longer than memLimit goes through a file in the spool
// directory, and every write, failed or not, leaves no file there and no
// descriptor open on one.
func TestSpool(t *testing.T) {
	u := newContainer(t)
	in := &injector{seen: map[string]int{}}
	c, tr := newClient(t, u, func(next http.RoundTripper) http.RoundTripper { in.next = next; return in })
	t.Cleanup(tr.CloseIdleConnections)
	dir := t.TempDir()
	s := New(c, dir)
	big := content("big", memLimit+1)

	missing := New(c, filepath.Join(dir, "missing"))
	if _, err := missing.Create(context.Background(), "small", strings.NewReader("x")); err != nil {
		t.Fatalf("a body held in memory does not need the spool directory: %v", err)
	}
	_, err := missing.Create(context.Background(), "big", strings.NewReader(big))
	expectStorageError(t, err, "create", "big")
	if errors.Is(err, storage.ErrOutcomeUnknown) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a spooled write without its directory: %v, want a definite error wrapping fs.ErrNotExist", err)
	}

	clean := func(what string) {
		t.Helper()
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Fatalf("after %s: spool directory has %d entries (%v)", what, len(entries), err)
		}
		fds, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			return // no /proc: the directory check above is all
		}
		for _, fd := range fds {
			if target, _ := os.Readlink(filepath.Join("/proc/self/fd", fd.Name())); strings.HasPrefix(target, dir) {
				t.Fatalf("after %s: descriptor %s still open on %s", what, fd.Name(), target)
			}
		}
	}
	v, err := s.Create(context.Background(), "k", strings.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	clean("a successful Create")
	_, err = s.Replace(context.Background(), "k", v, io.MultiReader(strings.NewReader(big), iotestErr{}))
	var be *storage.BodyError
	if !errors.As(err, &be) {
		t.Fatalf("Replace with a failing body: %v", err)
	}
	clean("a body error")
	ctx, cancel := context.WithCancel(context.Background())
	// The body cancels the context and returns nothing; read again, it
	// fails, so a write that ignores the context fails with a body error
	// instead of reading forever.
	var reads int
	_, err = s.Replace(ctx, "k", v, io.MultiReader(strings.NewReader(big), readerFunc(func([]byte) (int, error) {
		if reads++; reads == 1 {
			cancel()
			return 0, nil
		}
		return 0, errInjected
	}), strings.NewReader("tail")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Replace cancelled in its body: %v", err)
	}
	clean("a cancelled write")
	in.set(lostResponse)
	_, err = s.Replace(context.Background(), "k", v, strings.NewReader(big))
	if !errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("Replace with a lost response: %v", err)
	}
	clean("a lost response")
	in.set(nil)
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// TestNotETag: a version that is not a quoted entity tag, "*" included, which
// would match any blob, fails its precondition without a request.
func TestNotETag(t *testing.T) {
	s, in, check, _ := faultStore(t)
	v, err := check.Create(context.Background(), "k", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	in.set(nil)
	for _, bad := range []string{"*", "", strings.Trim(v, `"`), `W/` + v, v + `, "x"`, `"a b"`} {
		_, err := s.Replace(context.Background(), "k", bad, strings.NewReader("y"))
		expectStorageError(t, err, "replace", "k")
		if !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Fatalf("Replace with version %q: %v, want ErrPreconditionFailed", bad, err)
		}
	}
	expectRequests(t, in, "put", 0)
	expectContent(t, check, "k", "x")
}

// TestWriteToken: a successful Create or Replace sends one request, its Put
// Blob, and returns the ETag of that response. A token read afterwards could
// belong to a later writer's version.
func TestWriteToken(t *testing.T) {
	s, in, _, _ := faultStore(t)
	var v string
	for _, op := range []string{"create", "replace"} {
		var putETag string
		in.set(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
			resp, err := next.RoundTrip(req)
			if err == nil && kind(req) == "put" {
				putETag = resp.Header.Get("ETag")
			}
			return resp, err
		})
		var err error
		if op == "create" {
			v, err = s.Create(context.Background(), "k", strings.NewReader("a"))
		} else {
			v, err = s.Replace(context.Background(), "k", v, strings.NewReader("b"))
		}
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		for _, k := range []string{"get", "head", "list"} {
			expectRequests(t, in, k, 0)
		}
		expectRequests(t, in, "put", 1)
		if v == "" || v != putETag {
			t.Fatalf("%s returned token %q, want the Put Blob response's ETag %q", op, v, putETag)
		}
	}
}

// TestMissingContainer deletes the container. A 404 or 409 is the contract's
// answer only with the blob's own error code, so every call then fails with a
// definite error that is none of ErrNotFound, ErrPreconditionFailed and
// ErrOutcomeUnknown.
func TestMissingContainer(t *testing.T) {
	s, _, _, u := faultStore(t)
	v, err := s.Create(context.Background(), "k", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	cred, err := container.NewSharedKeyCredential(azuriteAccount, azuriteKey)
	if err != nil {
		t.Fatal(err)
	}
	base, _, _ := strings.Cut(u, "?")
	admin, err := container.NewClientWithSharedKeyCredential(base, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Delete(context.Background(), nil); err != nil {
		t.Fatal(redact(err))
	}
	ctx := context.Background()
	for op, call := range map[string]func() error{
		"create":  func() error { _, err := s.Create(ctx, "k2", strings.NewReader("y")); return err },
		"replace": func() error { _, err := s.Replace(ctx, "k", v, strings.NewReader("y")); return err },
		"delete":  func() error { return s.Delete(ctx, "k") },
		"get":     func() error { _, _, err := s.Get(ctx, "k"); return err },
		"range":   func() error { _, _, err := s.GetRange(ctx, "k", 0, 1); return err },
		"head":    func() error { _, err := s.Head(ctx, "k"); return err },
		"list":    func() error { _, err := s.List(ctx, "", "", 1); return err },
	} {
		err := call()
		if err == nil || errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrPreconditionFailed) || errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Errorf("%s without the container: %v, want a definite error of no contract kind", op, err)
		}
	}
}

// TestIdleConnection: Azurite closes a connection after 5 s idle, and a
// mutation is never retried, so a write sent on a pooled connection while
// Azurite closes it fails. newClient's transport drops idle connections
// sooner. Many clients each write after about 6 s idle, around the time
// Azurite closes their connections, and each Create of an existing key must
// fail its precondition, as on a fresh connection.
func TestIdleConnection(t *testing.T) {
	u := newContainer(t)
	if _, err := newStore(t, u, nil).Create(context.Background(), "k", strings.NewReader("old")); err != nil {
		t.Fatal(err)
	}
	body := content("new", 1<<20)
	const n = 48
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		s := newStore(t, u, nil)
		wg.Go(func() {
			if _, errs[i] = s.Head(context.Background(), "k"); errs[i] != nil {
				return
			}
			time.Sleep(5960*time.Millisecond + time.Duration(i)*2*time.Millisecond)
			_, errs[i] = s.Create(context.Background(), "k", strings.NewReader(body))
		})
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Errorf("client %d: %v, want ErrPreconditionFailed", i, err)
		}
	}
}

func expectStorageError(t *testing.T, err error, op, key string) {
	t.Helper()
	var se *storage.Error
	if !errors.As(err, &se) || se.Op != op || se.Key != key {
		t.Fatalf("error %v (%T), want a *storage.Error with Op %q and Key %q", err, err, op, key)
	}
}

func expectRequests(t *testing.T, in *injector, k string, n int) {
	t.Helper()
	if got := in.count(k); got != n {
		t.Fatalf("%d %s requests, want %d", got, k, n)
	}
}

func expectMissing(t *testing.T, s *Store, key string) {
	t.Helper()
	if _, err := s.Head(context.Background(), key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Head(%q) = %v, want ErrNotFound", key, err)
	}
}

func expectContent(t *testing.T, s *Store, key, want string) {
	t.Helper()
	body, info, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if got := readAll(t, body); got != want || info.Size != int64(len(want)) {
		t.Fatalf("Get(%q) = %d bytes (size %d), want %d bytes", key, len(got), info.Size, len(want))
	}
}

func readAll(t *testing.T, body io.ReadCloser) string {
	t.Helper()
	defer func() { _ = body.Close() }()
	var b bytes.Buffer
	if _, err := io.Copy(&b, body); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func entryKeys(entries []storage.Entry) []string {
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	return keys
}
