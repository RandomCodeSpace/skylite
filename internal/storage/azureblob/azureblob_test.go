package azureblob

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

func TestIsETag(t *testing.T) {
	for v, want := range map[string]bool{
		`"0x8DC2A1B2C3D4E5F"`: true,
		`"x"`:                 true,
		``:                    false,
		`*`:                   false,
		`"`:                   false,
		`""`:                  false,
		`0x8DC2A1B2C3D4E5F`:   false,
		`W/"x"`:               false,
		`"x", "y"`:            false,
		`"a b"`:               false,
		"\"a\nb\"":            false,
		`"x"x`:                false,
	} {
		if got := isETag(v); got != want {
			t.Errorf("isETag(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestRedact(t *testing.T) {
	const sig = "sig=c2VjcmV0LXNpZ25hdHVyZQ%3D%3D"
	transport := errors.New(`Put "http://127.0.0.1:1/c/k?se=2026-01-01&sp=rw&` + sig + `": dial tcp 127.0.0.1:1: connect: connection refused`)
	if got := redact(transport).Error(); strings.Contains(got, "sig=") || !strings.Contains(got, "connection refused") {
		t.Errorf("redact(transport error) = %q", got)
	}
	resp := &azcore.ResponseError{StatusCode: http.StatusForbidden, ErrorCode: "AuthenticationFailed", RawResponse: &http.Response{}}
	if got := redact(resp).Error(); got != "azure: HTTP 403 AuthenticationFailed" {
		t.Errorf("redact(response error) = %q", got)
	}
}

type failingCredential struct{}

func (failingCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, errors.New("token endpoint unavailable")
}

// httpCredential asks a token endpoint with the call's context, as azidentity's
// credentials do, and returns a token only if the endpoint answers 200.
type httpCredential struct{ url string }

func (c httpCredential) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return azcore.AccessToken{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return azcore.AccessToken{}, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return azcore.AccessToken{}, errors.New("token endpoint answered " + resp.Status)
	}
	return azcore.AccessToken{Token: "t", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// closedURL returns the URL of a local port that refuses connections.
func closedURL(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return "http://" + addr
}

// TestCredentialFailure: a mutation whose request never reaches the container
// fails with a definite error, for a Delete and an empty body too: a
// credential that gets no token, with or without asking a token endpoint
// (which shares the call's connection trace), or a token followed by a refused
// connection to the container.
//
// A token from a token endpoint followed by a refused connection used to be
// definite too; it is now unknown: the credential's connection in the shared
// trace, then the container's dial, look like a re-send.
func TestCredentialFailure(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	tokens := func(status int) string {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		t.Cleanup(ts.Close)
		return ts.URL
	}
	for name, tc := range map[string]struct {
		cred    azcore.TokenCredential
		blob    string
		unknown bool
	}{
		"no request":                    {failingCredential{}, srv.URL, false},
		"token 500":                     {httpCredential{tokens(http.StatusInternalServerError)}, srv.URL, false},
		"token refused":                 {httpCredential{closedURL(t)}, srv.URL, false},
		"container closed":              {tokenCredential{}, closedURL(t), false},
		"token asked, container closed": {httpCredential{tokens(http.StatusOK)}, closedURL(t), true},
	} {
		ctx := context.Background()
		for op, call := range map[string]func(*Store) error{
			"delete":        func(s *Store) error { return s.Delete(ctx, "k") },
			"create empty":  func(s *Store) error { _, err := s.Create(ctx, "k", strings.NewReader("")); return err },
			"create":        func(s *Store) error { _, err := s.Create(ctx, "k", strings.NewReader("x")); return err },
			"replace empty": func(s *Store) error { _, err := s.Replace(ctx, "k", `"v"`, strings.NewReader("")); return err },
		} {
			// A client per call: the client caches the token, and only a
			// call that asks for one has the credential's connection.
			c, err := container.NewClient(tc.blob+"/c", tc.cred, &container.ClientOptions{
				ClientOptions: azcore.ClientOptions{InsecureAllowCredentialWithHTTP: true},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = call(New(c, t.TempDir()))
			// A body the transport never read stays definite.
			unknown := tc.unknown && op != "create"
			if err == nil || errors.Is(err, storage.ErrOutcomeUnknown) != unknown {
				t.Errorf("%s, %s: %v, want an error, wrapping ErrOutcomeUnknown: %v", name, op, err, unknown)
			}
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("%d requests reached the server", n)
	}
}

// rtFunc is an http.RoundTripper made of a function.
type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// tokenCredential returns a token without a request.
type tokenCredential struct{}

func (tokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "t", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// TestAnsweredBehindCredential: azcore's bearer token policy drops a response
// whose body fails and returns the body's error alone, so with a token
// credential and a transport that reports no connection trace nothing but
// that error is left. The server answered the request, which it may have
// applied, so every mutation's outcome must be unknown.
func TestAnsweredBehindCredential(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"e"`)
		w.Header().Set("Content-Length", "10") // and no body, so reading it fails
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	noTrace := rtFunc(func(r *http.Request) (*http.Response, error) { return tr.RoundTrip(r.Clone(context.Background())) })
	c, err := container.NewClient(srv.URL+"/c", tokenCredential{}, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		InsecureAllowCredentialWithHTTP: true,
		Transport:                       &http.Client{Transport: noTrace},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := New(c, t.TempDir())
	ctx := context.Background()
	for op, call := range map[string]func() error{
		"create":       func() error { _, err := s.Create(ctx, "k", strings.NewReader("x")); return err },
		"create empty": func() error { _, err := s.Create(ctx, "k", strings.NewReader("")); return err },
		"replace":      func() error { _, err := s.Replace(ctx, "k", `"v"`, strings.NewReader("x")); return err },
		"delete":       func() error { return s.Delete(ctx, "k") },
	} {
		if err := call(); !errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Errorf("%s: %v, want an error wrapping ErrOutcomeUnknown", op, err)
		}
	}
}

// mutations calls Create, Replace and Delete on s, with a body and without,
// and returns their errors by name.
func mutations(s *Store) map[string]error {
	ctx := context.Background()
	errs := map[string]error{}
	_, errs["create"] = s.Create(ctx, "k", strings.NewReader("x"))
	_, errs["create empty"] = s.Create(ctx, "k", strings.NewReader(""))
	_, errs["replace"] = s.Replace(ctx, "k", `"v"`, strings.NewReader("x"))
	_, errs["replace empty"] = s.Replace(ctx, "k", `"v"`, strings.NewReader(""))
	errs["delete"] = s.Delete(ctx, "k")
	return errs
}

// stripTrace sends a request through tr with a context that carries no
// client trace, as a RoundTripper that is not net/http's may.
func stripTrace(tr http.RoundTripper) http.RoundTripper {
	return rtFunc(func(r *http.Request) (*http.Response, error) { return tr.RoundTrip(r.Clone(context.Background())) })
}

// TestRefused: a refused connection to the container is net/http's dial
// error, and with the client trace reaching net/http the trace shows a
// connection asked for and none handed over, so nothing was sent and every
// mutation's error is definite. Through a transport that strips the trace the
// same dial error is unknown: without the trace the backend cannot tell it
// from a re-send, by net/http, of a first attempt that took effect. A write
// whose body the transport never read stays definite by the body rule.
func TestRefused(t *testing.T) {
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	for name, rt := range map[string]http.RoundTripper{"plain": tr, "trace stripped": stripTrace(tr)} {
		c, err := container.NewClientWithNoCredential(closedURL(t)+"/c", &container.ClientOptions{
			ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: rt}},
		})
		if err != nil {
			t.Fatal(err)
		}
		for op, err := range mutations(New(c, t.TempDir())) {
			wantUnknown := name == "trace stripped" && op != "create" && op != "replace"
			if err == nil || errors.Is(err, storage.ErrOutcomeUnknown) != wantUnknown {
				t.Errorf("%s, %s: %v, want an error, wrapping ErrOutcomeUnknown: %v", name, op, err, wantUnknown)
			}
		}
	}
}

// outcome names the kind of a mutation's result.
func outcome(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, storage.ErrOutcomeUnknown):
		return "unknown"
	case errors.Is(err, storage.ErrPreconditionFailed):
		return "precondition"
	default:
		return "definite"
	}
}

// traced returns a Store whose transport reads the request's body, reports
// attempts in the request's trace and then answers.
func traced(t *testing.T, attempts func(*httptrace.ClientTrace), answer rtFunc) *Store {
	t.Helper()
	rt := rtFunc(func(r *http.Request) (*http.Response, error) {
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		attempts(httptrace.ContextClientTrace(r.Context()))
		return answer(r)
	})
	c, err := container.NewClientWithNoCredential("http://blob.example.test/c", &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: rt}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return New(c, t.TempDir())
}

// answer returns a transport answer with status and the Azure error code.
func answer(status int, code string) rtFunc {
	return func(r *http.Request) (*http.Response, error) {
		h := http.Header{}
		h.Set("x-ms-error-code", code)
		return &http.Response{StatusCode: status, Header: h, Body: http.NoBody, Request: r}, nil
	}
}

// opError returns a transport that fails with net/http's error for op.
func opError(op string) rtFunc {
	return func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: op, Net: "tcp", Err: errors.New("connection refused")}
	}
}

// conflict answers as the service does when a precondition fails: 412 to a
// Replace, 409 BlobAlreadyExists to a Create, and 409 LeaseIdMissing to a
// Delete.
func conflict(r *http.Request) (*http.Response, error) {
	switch {
	case r.Method == http.MethodPut && r.Header.Get("If-Match") != "":
		return answer(http.StatusPreconditionFailed, "ConditionNotMet")(r)
	case r.Method == http.MethodDelete:
		return answer(http.StatusConflict, "LeaseIdMissing")(r)
	}
	return answer(http.StatusConflict, "BlobAlreadyExists")(r)
}

// nonRetriable fails the way azcore's bearer token policy does after a 401
// challenge it cannot satisfy.
func nonRetriable(*http.Request) (*http.Response, error) {
	return nil, nonRetriableError{}
}

type nonRetriableError struct{}

func (nonRetriableError) Error() string { return "authentication challenge failed" }
func (nonRetriableError) NonRetriable() {}

func noAttempt(*httptrace.ClientTrace) {}

func oneGot(tr *httptrace.ClientTrace) { tr.GotConn(httptrace.GotConnInfo{}) }

func getOnly(tr *httptrace.ClientTrace) { tr.GetConn("blob.example.test:80") }

// all returns the same outcome for every mutation of mutations.
func all(o string) map[string]string {
	return map[string]string{"create": o, "create empty": o, "replace": o, "replace empty": o, "delete": o}
}

// TestTraceRules: an answer that proves no effect counts only when the trace
// saw exactly one connection handed to the request and none asked for after
// it, and net/http's dial error only when the trace saw a connection asked
// for and none handed over; any other transport error is unknown.
func TestTraceRules(t *testing.T) {
	refusedOne := map[string]string{"create": "precondition", "create empty": "precondition", "replace": "precondition", "replace empty": "precondition", "delete": "definite"}
	notFound := map[string]string{"create": "definite", "create empty": "definite", "replace": "precondition", "replace empty": "precondition", "delete": "success"}
	for name, tc := range map[string]struct {
		attempts func(*httptrace.ClientTrace)
		answer   rtFunc
		want     map[string]string
	}{
		"one GotConn, refused":          {oneGot, conflict, refusedOne},
		"one GotConn, 404 BlobNotFound": {oneGot, answer(http.StatusNotFound, "BlobNotFound"), notFound},
		"one GotConn, read fails":       {oneGot, opError("read"), all("unknown")},
		"one GotConn, dial fails":       {oneGot, opError("dial"), all("unknown")},
		"GetConn only, dial fails":      {getOnly, opError("dial"), all("definite")},
		"GetConn only, proxyconnect":    {getOnly, opError("proxyconnect"), all("definite")},
		"GetConn only, read fails":      {getOnly, opError("read"), all("unknown")},
		"GetConn only, write fails":     {getOnly, opError("write"), all("unknown")},
		"GetConn only, refused":         {getOnly, conflict, all("unknown")},
		"no trace, refused":             {noAttempt, conflict, all("unknown")},
		"no trace, 400":                 {noAttempt, answer(http.StatusBadRequest, "InvalidHeaderValue"), all("unknown")},
		"no trace, dial fails":          {noAttempt, opError("dial"), all("unknown")},
		"no trace, 404 BlobNotFound":    {noAttempt, answer(http.StatusNotFound, "BlobNotFound"), map[string]string{"create": "unknown", "create empty": "unknown", "replace": "unknown", "replace empty": "unknown", "delete": "success"}},
	} {
		for op, err := range mutations(traced(t, tc.attempts, tc.answer)) {
			if got := outcome(err); got != tc.want[op] {
				t.Errorf("%s, %s: %v, a %s outcome, want %s", name, op, err, got, tc.want[op])
			}
		}
	}
}

// TestResent: net/http re-sends a request on its own, for example over
// HTTP/2 after the server reset its stream with PROTOCOL_ERROR, so after a
// second attempt the answer may be to a re-send of a first attempt that took
// effect. A transport that reads the body, reports a second attempt in the
// request's trace and then fails a dial, or answers a precondition failure,
// must leave every mutation unknown.
func TestResent(t *testing.T) {
	dialFails := opError("dial")
	twoGot := func(tr *httptrace.ClientTrace) {
		tr.GotConn(httptrace.GotConnInfo{})
		tr.GotConn(httptrace.GotConnInfo{})
	}
	gotGet := func(tr *httptrace.ClientTrace) {
		tr.GotConn(httptrace.GotConnInfo{})
		tr.GetConn("blob.example.test:80")
	}
	for name, tc := range map[string]struct {
		attempts func(*httptrace.ClientTrace)
		answer   rtFunc
	}{
		"two GotConn, dial fails":           {twoGot, dialFails},
		"GetConn after GotConn, dial fails": {gotGet, dialFails},
		"two GotConn, refused":              {twoGot, conflict},
		// A failed 401 challenge after a re-send: azcore marks it non-retriable.
		"two GotConn, non-retriable":           {twoGot, nonRetriable},
		"GetConn after GotConn, non-retriable": {gotGet, nonRetriable},
		"GetConn after GotConn, refused":       {gotGet, conflict},
		// A missing blob is a Delete's effect, also after a re-send.
		"two GotConn, 404 BlobNotFound": {twoGot, answer(http.StatusNotFound, "BlobNotFound")},
	} {
		for op, err := range mutations(traced(t, tc.attempts, tc.answer)) {
			want := "unknown"
			if op == "delete" && name == "two GotConn, 404 BlobNotFound" {
				want = "success"
			}
			if got := outcome(err); got != want {
				t.Errorf("%s, %s: %v, a %s outcome, want %s", name, op, err, got, want)
			}
		}
	}
}

// proxyCredential asks its token endpoint through a proxy, as a credential
// does when HTTP_PROXY names one (http.ProxyFromEnvironment reads the
// environment once per process, so the proxy is named here), and falls back
// to its cached, still valid token when that fails, as azcore does when an
// early refresh fails.
type proxyCredential struct {
	proxy *url.URL
	calls *atomic.Int64
}

func (c proxyCredential) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.calls.Add(1)
	tr := &http.Transport{Proxy: http.ProxyURL(c.proxy)}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://login.example.test/token", nil)
	if err != nil {
		return azcore.AccessToken{}, err
	}
	if resp, err := tr.RoundTrip(req); err == nil {
		_ = resp.Body.Close()
	}
	return azcore.AccessToken{Token: "t", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// TestLostBehindProxyCredential: a credential's failed connection to a
// closed proxy leaves a connection attempt without a connection in the
// call's trace; the container's transport drops the trace, and the server
// applies the request and closes the connection without an answer. The
// error (EOF) proves nothing, so every mutation is unknown.
func TestLostBehindProxyCredential(t *testing.T) {
	var applied atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		applied.Add(1)
		panic(http.ErrAbortHandler) // close the connection without an answer
	}))
	defer srv.Close()
	t.Setenv("HTTP_PROXY", closedURL(t))
	proxy, err := url.Parse(os.Getenv("HTTP_PROXY"))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	c, err := container.NewClient(srv.URL+"/c", proxyCredential{proxy, &calls}, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		InsecureAllowCredentialWithHTTP: true,
		Transport:                       &http.Client{Transport: stripTrace(tr)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := New(c, t.TempDir())
	ctx := context.Background()
	for op, call := range map[string]func() error{
		"create":  func() error { _, err := s.Create(ctx, "k", strings.NewReader("x")); return err },
		"replace": func() error { _, err := s.Replace(ctx, "k", `"v"`, strings.NewReader("x")); return err },
		"delete":  func() error { return s.Delete(ctx, "k") },
	} {
		before := applied.Load()
		if err := call(); !errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Errorf("%s: %v, want an error wrapping ErrOutcomeUnknown", op, err)
		}
		if applied.Load() == before {
			t.Errorf("%s: the server did not get the request", op)
		}
	}
	if calls.Load() == 0 {
		t.Fatal("the credential was never asked")
	}
}

// TestSpoolMemory: a body of at most memLimit bytes stays in memory, and
// needs no spool directory.
func TestSpoolMemory(t *testing.T) {
	s := New(nil, "/nonexistent")
	for _, n := range []int{0, 1, memLimit} {
		u, err := s.spool(context.Background(), strings.NewReader(strings.Repeat("x", n)))
		if err != nil {
			t.Fatalf("%d bytes: %v", n, err)
		}
		if u.f != nil || u.size != int64(n) || u.end != (n == 0) {
			t.Fatalf("%d bytes: file %v, size %d, end %v", n, u.f, u.size, u.end)
		}
		u.release()
	}
}

// TestSpoolFile: a longer body goes to an unlinked file in the spool
// directory, all of it, and a body over maxBody fails with
// ErrInvalidArgument.
func TestSpoolFile(t *testing.T) {
	dir := t.TempDir()
	s := New(nil, dir)
	body := strings.Repeat("y", memLimit+5)
	u, err := s.spool(context.Background(), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); u.f == nil || len(entries) != 0 {
		t.Fatalf("file %v, %d entries in the spool directory, want an unlinked file", u.f, len(entries))
	}
	got, err := io.ReadAll(u)
	if err != nil || string(got) != body || !u.finish() {
		t.Fatalf("read back %d bytes, %v, finished %v", len(got), err, u.end)
	}
	if _, err := u.Read(make([]byte, 1)); !errors.Is(err, errFinished) {
		t.Fatalf("Read after finish: %v", err)
	}
	u.release()

	defer func(old int64) { maxBody = old }(maxBody)
	maxBody = memLimit + 4
	if _, err := s.spool(context.Background(), strings.NewReader(body)); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("body over maxBody: %v", err)
	}
}

// TestUploadEnd: the transport counts as having read the body only once it
// has read the last byte, not when the SDK seeks to the end to measure it.
func TestUploadEnd(t *testing.T) {
	s := New(nil, "")
	u, err := s.spool(context.Background(), strings.NewReader("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Read(make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	if u.finish() {
		t.Fatal("finish reports the whole body read after 2 of 3 bytes")
	}
}

// h2Reset serves HTTP/2 by hand. Of each two complete requests it takes the
// first as applied, runs onApply if set, and resets its stream with
// PROTOCOL_ERROR, after which net/http re-sends the request; it answers the
// second, the re-send, 409 BlobAlreadyExists, as the service answers a
// request that meets its own first attempt's effect.
func h2Reset(applied, answered *atomic.Int64, onApply func()) func(*http.Server, *tls.Conn, http.Handler) {
	return func(_ *http.Server, c *tls.Conn, _ http.Handler) {
		defer func() { _ = c.Close() }()
		if _, err := io.ReadFull(c, make([]byte, len(http2.ClientPreface))); err != nil {
			return
		}
		fr := http2.NewFramer(c, c)
		_ = fr.WriteSettings()
		handle := func(id uint32) {
			if (applied.Load()+answered.Load())%2 == 0 {
				applied.Add(1)
				if onApply != nil {
					onApply()
				}
				_ = fr.WriteRSTStream(id, http2.ErrCodeProtocol)
				return
			}
			answered.Add(1)
			var b bytes.Buffer
			enc := hpack.NewEncoder(&b)
			_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "409"})
			_ = enc.WriteField(hpack.HeaderField{Name: "x-ms-error-code", Value: "BlobAlreadyExists"})
			_ = enc.WriteField(hpack.HeaderField{Name: "content-length", Value: "0"})
			_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: b.Bytes(), EndStream: true, EndHeaders: true})
		}
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				return
			}
			switch f := f.(type) {
			case *http2.SettingsFrame:
				if !f.IsAck() {
					_ = fr.WriteSettingsAck()
				}
			case *http2.HeadersFrame:
				if f.StreamEnded() {
					handle(f.StreamID)
				}
			case *http2.DataFrame:
				if f.StreamEnded() {
					handle(f.StreamID)
				}
			}
		}
	}
}

// TestH2Resend: over HTTP/2 net/http re-sends, on its own, a request whose
// stream the server reset with PROTOCOL_ERROR, which the server may have
// applied. The re-send's 409, or its refused dial once the server has stopped
// listening, belongs to the second attempt, so every mutation must be
// unknown: with the trace intact because the trace shows the second attempt,
// and through a transport that strips the trace because nothing then proves a
// single attempt.
func TestH2Resend(t *testing.T) {
	for _, strip := range []bool{false, true} {
		for _, closeListener := range []bool{false, true} {
			var applied, answered atomic.Int64
			srv := httptest.NewUnstartedServer(http.NotFoundHandler())
			var onApply func()
			if closeListener {
				onApply = func() { _ = srv.Listener.Close() }
			}
			srv.TLS = &tls.Config{NextProtos: []string{"h2"}}
			srv.Config.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){"h2": h2Reset(&applied, &answered, onApply)}
			srv.StartTLS()
			tr := srv.Client().Transport.(*http.Transport).Clone()
			tr.ForceAttemptHTTP2 = true
			var rt http.RoundTripper = tr
			if strip {
				rt = stripTrace(tr)
			}
			c, err := container.NewClientWithNoCredential(srv.URL+"/c", &container.ClientOptions{
				ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: rt}},
			})
			if err != nil {
				t.Fatal(err)
			}
			s := New(c, t.TempDir())
			ctx := context.Background()
			ops := map[string]func() error{
				"create":       func() error { _, err := s.Create(ctx, "k", strings.NewReader("x")); return err },
				"create empty": func() error { _, err := s.Create(ctx, "k", strings.NewReader("")); return err },
				"replace":      func() error { _, err := s.Replace(ctx, "k", `"v"`, strings.NewReader("x")); return err },
				"delete":       func() error { return s.Delete(ctx, "k") },
			}
			if closeListener {
				ops = map[string]func() error{"create": ops["create"]} // the listener closes on the first
			}
			for op, call := range ops {
				a0, r0 := applied.Load(), answered.Load()
				err := call()
				wantAnswered := int64(1)
				if closeListener {
					wantAnswered = 0
				}
				if a, r := applied.Load()-a0, answered.Load()-r0; a != 1 || r != wantAnswered {
					t.Fatalf("strip %v, close %v, %s: %d applied, %d re-sends answered, want 1 and %d", strip, closeListener, op, a, r, wantAnswered)
				}
				if !errors.Is(err, storage.ErrOutcomeUnknown) {
					t.Errorf("strip %v, close %v, %s: %v, want an error wrapping ErrOutcomeUnknown", strip, closeListener, op, err)
				}
			}
			tr.CloseIdleConnections()
			srv.Close()
		}
	}
}
