package azureblob

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

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

// TestCredentialFailure: a credential that cannot get a token fails each
// mutation in the SDK before any request is sent, so the error is definite,
// for a Delete and an empty body too.
func TestCredentialFailure(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, err := container.NewClient(srv.URL+"/c", failingCredential{}, &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{InsecureAllowCredentialWithHTTP: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := New(c, t.TempDir())
	ctx := context.Background()
	for op, call := range map[string]func() error{
		"delete":        func() error { return s.Delete(ctx, "k") },
		"create empty":  func() error { _, err := s.Create(ctx, "k", strings.NewReader("")); return err },
		"create":        func() error { _, err := s.Create(ctx, "k", strings.NewReader("x")); return err },
		"replace empty": func() error { _, err := s.Replace(ctx, "k", `"v"`, strings.NewReader("")); return err },
	} {
		if err := call(); err == nil || errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Errorf("%s: %v, want a definite error", op, err)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("%d requests reached the server", n)
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
