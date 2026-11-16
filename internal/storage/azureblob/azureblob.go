// Package azureblob is a storage.Store on one container of Azure Blob Storage
// with a flat namespace: every object is a block blob named by its key.
//
// # Client and credentials
//
// [New] takes a [container.Client] that the caller has built for the
// container, so the package does not choose a credential. With Entra ID,
// including workload identity, build the client with [container.NewClient]
// and any azcore.TokenCredential; with a shared key or a SAS, use the other
// constructors of package container. The container must exist, and nothing
// else may write to it. The client's transport must be net/http's (the SDK's
// default or an *http.Client): the backend learns whether a request reached a
// connection from the response the SDK's pipeline returns and from net/http's
// client trace (see "Outcomes"). The trace names a request's first hop, so
// only a proxy that the environment names (http.ProxyFromEnvironment, as in
// the SDK's default transport) is recognised in it; through another proxy the
// backend classifies as if the transport reported no trace. The transport
// must also close an idle connection before the service, or anything between
// (a load balancer, a proxy), does: set http.Transport.IdleConnTimeout below
// the shortest idle timeout on the path (the SDK's default transport keeps
// idle connections 90 s; Azurite closes them after 5 s). A mutation is never
// retried, so one sent on a pooled connection that the other end is closing
// fails, often with an unknown outcome.
//
// # Writes
//
// Create and Replace each publish with a single Put Blob request; the backend
// never stages blocks. Create sends If-None-Match: *, so the service creates
// the blob only if none exists; Replace sends If-Match with the version token,
// so the service overwrites it only if its ETag still matches. Both conditions
// are evaluated by the service inside that one request, so among concurrent
// writers exactly one succeeds. Staged blocks are avoided because Azurite
// counts a blob that has only uncommitted blocks as existing for
// If-None-Match: *, so an interrupted staged upload could block a later
// Create, and the real service's behaviour there is not documented.
//
// Put Blob needs the body's length before it is sent, and a write must read
// its body to its end before it publishes anything, so every write first
// reads the whole body. Up to 4 MiB of it is held in memory; a longer body is
// written to a spool file in the directory given to [New] instead, so a write
// holds at most 4 MiB (plus a 32 KiB copy buffer) of its body in memory
// whatever its size. The spool file is unlinked right after it is created and
// closed when the write returns, on every path, so it never outlives the
// write, even if the process is killed (this relies on a Linux or macOS
// filesystem, where an open file can be unlinked). A write holds no lock
// while it reads its body, so it blocks no other call.
//
// A body longer than 5,000 MiB, the largest Put Blob the service accepts, is
// rejected with storage.ErrInvalidArgument before anything is sent.
//
// # Version tokens
//
// The version token is the blob's ETag, as the service returns it, quotes
// included. Replace sends it unchanged in If-Match, so the token and the write
// condition name the same thing. A version that is not a quoted entity tag
// ("*" and an empty string included) can never be a token this backend
// returned, so Replace fails its precondition without sending a request.
//
// The service documents that a blob's ETag changes on every write. The
// storage contract also needs an ETag never to recur for a key, also across a
// Delete and a later Create of the same name. The backend uses the ETag as the
// token on the assumption, not a documented guarantee, that the service never
// issues the same ETag twice for one blob name. That has not been verified
// against the service. If it proves false, the backend must change to a
// per-write identity stored with the blob and compared under a blob lease.
// Azurite derives its ETags from the time in
// milliseconds multiplied by a random factor, so on the emulator a repeat is
// possible, though unlikely, for two writes in the same millisecond.
//
// # Outcomes
//
// The SDK never retries a mutation: Create, Replace and Delete run with
// azcore retries disabled for that call, so the outcome of the call is the
// outcome of its one request, and a lost response is never turned into a
// precondition failure by a retry that meets the write's own effect. (net/http
// itself resends a request only when nothing of it was written to the
// connection.) A failed mutation is classified as follows:
//
//   - a response with status 409 BlobAlreadyExists or 412 (Create or Replace),
//     or 404 BlobNotFound (Replace), is storage.ErrPreconditionFailed;
//   - any other 4xx status except 408 is a definite error with no effect;
//   - if the transport did not read a write's body to its end, the service
//     cannot have applied it, and the error is definite;
//   - if the SDK's pipeline returned no response and either net/http's trace
//     reports a connection attempt to the container's endpoint, or the proxy
//     for it, that obtained no connection, or there was no such attempt and
//     the error is one azcore marks non-retriable (as its bearer token policy
//     marks a credential that got no token, and its refusal to send a token
//     over plain HTTP), the request never reached the container, and the
//     error is definite;
//   - anything else wraps storage.ErrOutcomeUnknown: a 5xx status, 408, a
//     2xx or 3xx status other than the one the request expects (from a
//     non-conforming proxy), any failure after a response arrived (a body
//     that fails, a header that does not parse), a lost response, a reset
//     connection, the context ending while the request is in flight, and any
//     other error for which the backend cannot rule out that the request was
//     sent.
//
// A credential that calls its token endpoint with the call's context shares
// its client trace, so only connections to the container's endpoint, or the
// proxy for it, count. azcore does not pass the response capture on to a
// credential, so a response always is the container's. Without a connection
// in the trace, the backend cannot always tell a request that was never sent
// from one that was sent and lost, and then reports unknown although nothing
// was sent: for Delete and an empty Create when the context ends while a
// credential waits for a token (azcore then returns the context's error
// alone); when a credential reaches its token endpoint through the
// container's proxy; and when the SDK fails before its transport with an
// error azcore does not mark non-retriable, for example a shared key that
// cannot sign the request.
//
// Once a mutation call returns, its request body refuses further reads, so a
// request that net/http is still sending when the call returns can never be
// completed and applied afterwards.
//
// Delete of a missing blob (404 BlobNotFound) succeeds. A blob that has
// snapshots cannot be deleted; this backend never creates snapshots.
//
// Reads (Get, GetRange, Head and List) keep the client's retry policy, since
// repeating them has no effect on the store.
//
// # Reads
//
// Get is one Get Blob request and GetRange one ranged Get Blob, so a body and
// its Info come from one response and therefore one version. A GetRange of
// length 0 is a Get Blob Properties request. The service answers a range
// starting at or past the end with 416; the backend then reads the blob's
// properties and returns an empty body if the offset is still at or past the
// end, or repeats the ranged read if the blob has grown in between.
//
// # Listing
//
// List uses List Blobs with the prefix and follows NextMarker until it has
// limit entries or the listing ends, since the service may return fewer
// entries than requested. List Blobs has no start-after parameter for a flat
// namespace, so the backend skips the entries up to startAfter itself:
// shortcut: a List with startAfter reads every key under the prefix up to
// startAfter, up to 5,000 per request; if that becomes a hot path, use the
// service's startFrom parameter once it is verified for flat namespaces.
//
// # Errors
//
// Errors from the service keep only the HTTP status and Azure's error code.
// Other SDK and transport errors keep their text with every URL query string
// removed, since a SAS URL carries its signature there.
//
// # Tested
//
// Only against Azurite 3.37.0, with its well-known development account and a
// container SAS, and token credentials against local HTTP servers: one that
// fails without a request, some that call a local token endpoint, and one
// that gets a token for a server whose answers' bodies fail.
// The real service, working Entra ID credentials, real latency and a proxy
// are not tested.
package azureblob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// memLimit is the most of a body a write holds in memory.
const memLimit = 4 << 20

// maxBody is the largest body Put Blob accepts (5,000 MiB). Tests lower it.
var maxBody int64 = 5000 << 20

// listPage is the most entries List Blobs returns in one response.
const listPage = 5000

// Store is a storage.Store on one Azure Blob Storage container.
type Store struct {
	c        *container.Client
	spoolDir string
	hops     []string // "host:port" of the container's endpoint, and of the proxy the environment names for it
}

// New returns a Store on the container that c addresses. Bodies longer than
// 4 MiB are spooled to files in spoolDir, or in os.TempDir if it is empty.
func New(c *container.Client, spoolDir string) *Store {
	s := &Store{c: c, spoolDir: spoolDir}
	if c == nil {
		return s
	}
	if u, err := url.Parse(c.URL()); err == nil {
		s.hops = []string{hostPort(u)}
		if p, err := http.ProxyFromEnvironment(&http.Request{URL: u}); err == nil && p != nil {
			s.hops = append(s.hops, hostPort(p))
		}
	}
	return s
}

// Create implements storage.Store.
func (s *Store) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	version, err := s.put(ctx, key, "", body)
	if err != nil {
		return "", &storage.Error{Op: "create", Key: key, Err: err}
	}
	return version, nil
}

// Replace implements storage.Store.
func (s *Store) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := check(ctx, key); err != nil {
		return "", &storage.Error{Op: "replace", Key: key, Err: err}
	}
	if !isETag(version) {
		return "", &storage.Error{Op: "replace", Key: key, Err: storage.ErrPreconditionFailed}
	}
	newVersion, err := s.put(ctx, key, version, body)
	if err != nil {
		return "", &storage.Error{Op: "replace", Key: key, Err: err}
	}
	return newVersion, nil
}

// put writes body to key with one Put Blob request, conditioned on the ETag
// ifMatch, or on the blob being absent if ifMatch is empty.
func (s *Store) put(ctx context.Context, key, ifMatch string, body io.Reader) (string, error) {
	if err := check(ctx, key); err != nil {
		return "", err
	}
	up, err := s.spool(ctx, body)
	if err != nil {
		return "", err
	}
	defer up.release()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cond := &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}
	if ifMatch != "" {
		cond = &blob.ModifiedAccessConditions{IfMatch: to.Ptr(azcore.ETag(ifMatch))}
	}
	tctx, conn := s.mutation(ctx)
	resp, err := s.c.NewBlockBlobClient(key).Upload(tctx, up, &blockblob.UploadOptions{
		AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: cond},
	})
	bodySent := up.finish()
	if err != nil {
		var re *azcore.ResponseError
		if errors.As(err, &re) && (re.StatusCode == http.StatusPreconditionFailed ||
			re.StatusCode == http.StatusConflict && re.ErrorCode == string(bloberror.BlobAlreadyExists) ||
			ifMatch != "" && re.StatusCode == http.StatusNotFound && re.ErrorCode == string(bloberror.BlobNotFound)) {
			return "", storage.ErrPreconditionFailed
		}
		return "", mutationError(ctx, err, conn.reached(err) && bodySent)
	}
	if resp.ETag == nil || *resp.ETag == "" {
		return "", fmt.Errorf("%w: azure: Put Blob succeeded without an ETag", storage.ErrOutcomeUnknown)
	}
	return string(*resp.ETag), nil
}

// Get implements storage.Store.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if err := check(ctx, key); err != nil {
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: err}
	}
	resp, err := s.c.NewBlobClient(key).DownloadStream(ctx, nil)
	if err != nil {
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: readError(ctx, err)}
	}
	if resp.ContentLength == nil || resp.ETag == nil || *resp.ETag == "" {
		_ = resp.Body.Close()
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: errors.New("azure: Get Blob response without Content-Length or ETag")}
	}
	return ctxBody{ctx, resp.Body}, storage.Info{Size: *resp.ContentLength, Version: string(*resp.ETag)}, nil
}

// GetRange implements storage.Store.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	rc, info, err := s.getRange(ctx, key, offset, length)
	if err != nil {
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: err}
	}
	return rc, info, nil
}

func (s *Store) getRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if err := check(ctx, key); err != nil {
		return nil, storage.Info{}, err
	}
	if offset < 0 || length < 0 {
		return nil, storage.Info{}, storage.ErrInvalidArgument
	}
	empty := io.NopCloser(strings.NewReader(""))
	if length == 0 {
		info, err := s.head(ctx, key)
		return empty, info, err
	}
	rng := blob.HTTPRange{Offset: offset, Count: length}
	if length > math.MaxInt64-offset {
		rng.Count = 0 // to the end
	}
	for {
		resp, err := s.c.NewBlobClient(key).DownloadStream(ctx, &blob.DownloadStreamOptions{Range: rng})
		var re *azcore.ResponseError
		if errors.As(err, &re) && re.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			info, err := s.head(ctx, key)
			if err != nil || info.Size <= offset {
				return empty, info, err
			}
			continue // the blob grew after the ranged read; read again
		}
		if err != nil {
			return nil, storage.Info{}, readError(ctx, err)
		}
		size, ok := rangeTotal(resp.ContentRange)
		if !ok || resp.ETag == nil || *resp.ETag == "" {
			_ = resp.Body.Close()
			return nil, storage.Info{}, errors.New("azure: ranged Get Blob response without Content-Range or ETag")
		}
		return ctxBody{ctx, resp.Body}, storage.Info{Size: size, Version: string(*resp.ETag)}, nil
	}
}

// rangeTotal returns the object size from a Content-Range "bytes a-b/size".
func rangeTotal(cr *string) (int64, bool) {
	if cr == nil {
		return 0, false
	}
	_, total, ok := strings.Cut(*cr, "/")
	n, err := strconv.ParseInt(total, 10, 64)
	return n, ok && err == nil && n >= 0
}

// Head implements storage.Store.
func (s *Store) Head(ctx context.Context, key string) (storage.Info, error) {
	err := check(ctx, key)
	var info storage.Info
	if err == nil {
		info, err = s.head(ctx, key)
	}
	if err != nil {
		return storage.Info{}, &storage.Error{Op: "head", Key: key, Err: err}
	}
	return info, nil
}

func (s *Store) head(ctx context.Context, key string) (storage.Info, error) {
	resp, err := s.c.NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return storage.Info{}, readError(ctx, err)
	}
	if resp.ContentLength == nil || resp.ETag == nil || *resp.ETag == "" {
		return storage.Info{}, errors.New("azure: Get Blob Properties response without Content-Length or ETag")
	}
	return storage.Info{Size: *resp.ContentLength, Version: string(*resp.ETag)}, nil
}

// List implements storage.Store.
func (s *Store) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.list(ctx, prefix, startAfter, limit)
	if err != nil {
		return nil, &storage.Error{Op: "list", Err: err}
	}
	return entries, nil
}

func (s *Store) list(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > storage.MaxListLimit {
		return nil, storage.ErrInvalidArgument
	}
	opts := &container.ListBlobsFlatOptions{MaxResults: to.Ptr(int32(limit))} //nolint:gosec // limit is at most MaxListLimit
	if startAfter >= prefix {
		opts.MaxResults = to.Ptr(int32(listPage)) // entries up to startAfter are read and skipped
	}
	if prefix != "" {
		opts.Prefix = &prefix
	}
	out := []storage.Entry{}
	last := startAfter
	pager := s.c.NewListBlobsFlatPager(opts)
	for len(out) < limit && pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, readError(ctx, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || item.Properties == nil || item.Properties.ContentLength == nil {
				return nil, errors.New("azure: List Blobs entry without a name or Content-Length")
			}
			name := *item.Name
			if name <= last {
				continue
			}
			out = append(out, storage.Entry{Key: name, Size: *item.Properties.ContentLength})
			last = name
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// Delete implements storage.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.delete(ctx, key); err != nil {
		return &storage.Error{Op: "delete", Key: key, Err: err}
	}
	return nil
}

func (s *Store) delete(ctx context.Context, key string) error {
	if err := check(ctx, key); err != nil {
		return err
	}
	tctx, conn := s.mutation(ctx)
	_, err := s.c.NewBlobClient(key).Delete(tctx, nil)
	if err == nil || bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil
	}
	return mutationError(ctx, err, conn.reached(err))
}

func check(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	return ctx.Err()
}

// isETag reports whether v is a strong entity tag: a quoted, non-empty
// string of visible ASCII characters other than the quote.
func isETag(v string) bool {
	if len(v) < 3 || v[0] != '"' || v[len(v)-1] != '"' {
		return false
	}
	for i := 1; i < len(v)-1; i++ {
		if v[i] <= ' ' || v[i] == '"' || v[i] > '~' {
			return false
		}
	}
	return true
}

// hostPort returns u's "host:port", as net/http's client trace names it.
func hostPort(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443", "socks5": "1080", "socks5h": "1080"}[u.Scheme]
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// connTrace records what one call's pipeline and net/http's client trace
// report about its request: the response the pipeline returned, if any, and
// connections to hops, the container's first hops. A credential shares the
// call's client trace, so connections to other hosts, such as its token
// endpoint, are left out; azcore does not pass the response capture on to a
// credential, so a captured response is always the container's.
type connTrace struct {
	hops             []string
	resp             *http.Response
	getConn, gotConn atomic.Bool
}

// reached reports whether the request that failed with err may have reached a
// connection to the container. A response returned by the pipeline means it
// did, whatever failed after it (reading the body, parsing a header). Else,
// with net/http's trace, it did if it obtained a connection. Without either,
// only an error that azcore marks non-retriable is known to come from before
// the transport: its bearer token policy marks so a credential's failure and
// the refusal to send a token over plain HTTP. Any other error may come from
// after it: http.Client wraps every transport error in a *url.Error, the
// bearer token policy drops a response whose body fails and returns the
// body's error alone, and the SDK returns the context's error in place of the
// transport's once the context has ended.
func (c *connTrace) reached(err error) bool {
	if c.resp != nil {
		return true
	}
	if c.getConn.Load() {
		return c.gotConn.Load()
	}
	var nr interface{ NonRetriable() }
	return !errors.As(err, &nr)
}

// mutation returns a context for one mutation request: SDK retries are off,
// and the returned trace records whether the request reached a connection.
func (s *Store) mutation(ctx context.Context) (context.Context, *connTrace) {
	c := &connTrace{hops: s.hops}
	ctx = policy.WithCaptureResponse(ctx, &c.resp)
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(hp string) {
			if slices.Contains(c.hops, hp) {
				c.getConn.Store(true)
			}
		},
		GotConn: func(httptrace.GotConnInfo) {
			if c.getConn.Load() {
				c.gotConn.Store(true)
			}
		},
	})
	return policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1}), c
}

// mutationError classifies the error of a mutation request that got no
// definite answer. reached reports whether the service can have received the
// whole request.
func mutationError(ctx context.Context, err error, reached bool) error {
	var re *azcore.ResponseError
	answered := errors.As(err, &re)
	cause := redact(err)
	if !answered && ctx.Err() != nil {
		cause = ctx.Err()
	}
	// Only a 4xx refuses the request; a 2xx or 3xx other than the one the
	// SDK expects comes from a peer that may have applied it.
	if !reached || answered && re.StatusCode >= 400 && re.StatusCode < 500 && re.StatusCode != http.StatusRequestTimeout {
		return cause
	}
	return fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, cause)
}

// readError maps the error of a read request.
func readError(ctx context.Context, err error) error {
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return storage.ErrNotFound
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return redact(err)
}

var queryString = regexp.MustCompile(`\?[^\s"'<>]*`)

// redact returns err's text without anything that can hold a secret: for a
// service response, its status and error code only (the body can quote the
// request's signature); otherwise the text with every query string removed.
func redact(err error) error {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return fmt.Errorf("azure: HTTP %d %s", re.StatusCode, re.ErrorCode)
	}
	return errors.New("azure: " + queryString.ReplaceAllString(err.Error(), "?[redacted]"))
}

// ctxBody fails reads once ctx is done, with ctx's error.
type ctxBody struct {
	ctx context.Context
	rc  io.ReadCloser
}

func (b ctxBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := b.rc.Read(p)
	if err != nil && err != io.EOF { //nolint:errorlint // io.Reader returns io.EOF itself, never wrapped
		if cerr := b.ctx.Err(); cerr != nil {
			err = cerr
		}
	}
	return n, err
}

func (b ctxBody) Close() error { return b.rc.Close() }

// upload is a write's body, read to its end, as the SDK sends it. It records
// whether the transport read it to its end, and refuses reads after finish.
type upload struct {
	mu   sync.Mutex
	r    io.ReadSeeker
	f    *os.File // the spool file, or nil when the body is in memory
	size int64
	off  int64
	end  bool // the transport has read the last byte
	done bool
}

var errFinished = errors.New("azureblob: the write has returned")

func (u *upload) Read(p []byte) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.done {
		return 0, errFinished
	}
	n, err := u.r.Read(p)
	u.off += int64(n)
	if n > 0 && u.off >= u.size {
		u.end = true
	}
	return n, err
}

func (u *upload) Seek(offset int64, whence int) (int64, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	pos, err := u.r.Seek(offset, whence)
	if err == nil {
		u.off = pos
	}
	return pos, err
}

// Close does nothing: the SDK may close the body, and the write closes the
// spool file itself, in release.
func (u *upload) Close() error { return nil }

// finish stops further reads and reports whether the whole body was read.
func (u *upload) finish() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.done = true
	return u.end
}

func (u *upload) release() {
	if u.f != nil {
		_ = u.f.Close()
	}
}

// spool reads body to its end and returns it as an upload: in memory if it
// is at most memLimit bytes long, else in an unlinked file in s.spoolDir.
func (s *Store) spool(ctx context.Context, body io.Reader) (*upload, error) {
	r := ctxReader{ctx, bodyReader{body}}
	head, err := io.ReadAll(io.LimitReader(r, memLimit+1))
	if err != nil {
		return nil, err
	}
	if len(head) <= memLimit {
		return &upload{r: bytes.NewReader(head), size: int64(len(head)), end: len(head) == 0}, nil
	}
	f, err := os.CreateTemp(s.spoolDir, "azureblob-*")
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		_ = f.Close()
		return nil, err
	}
	n, err := f.Write(head)
	if err == nil {
		var m int64
		m, err = io.Copy(f, io.LimitReader(r, maxBody-int64(n)+1))
		if err == nil && int64(n)+m > maxBody {
			err = fmt.Errorf("%w: body longer than %d bytes, the most one Put Blob accepts", storage.ErrInvalidArgument, maxBody)
		}
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &upload{r: f, f: f, size: st.Size()}, nil
}

// ctxReader fails reads once ctx is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// bodyReader wraps errors from the caller's body in a storage.BodyError.
type bodyReader struct{ r io.Reader }

func (b bodyReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err != nil && err != io.EOF { //nolint:errorlint // io.Reader returns io.EOF itself, never wrapped
		err = &storage.BodyError{Err: err}
	}
	return n, err
}
