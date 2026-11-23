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
// else may write to it. The client's transport must pass the request's
// context on to net/http, as the SDK's default transport, any
// *http.Transport and any *http.Client do: the backend proves that a request
// was sent once from net/http's client trace (see "Outcomes"). A transport
// that drops the context is not supported. Through it, a credential that
// fetches its token over HTTP with the call's context puts its own
// connections in the trace, and a failed mutation that took effect can then
// be reported as having none. The transport must also not send a request
// again on its own; net/http's own re-sends show in the trace. A refused
// connection is a definite error only if the transport returns net/http's
// dial error. The transport must also close an idle
// connection before the service, or anything between (a load balancer, a
// proxy), does: set http.Transport.IdleConnTimeout below
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
// The SDK does not retry a mutation: Create, Replace and Delete run with
// azcore retries disabled for that call (its bearer token policy still sends
// the request again after a 401 challenge it handled, which shows in the
// trace), so a lost response is never turned into a
// precondition failure by a retry that meets the write's own effect. net/http
// itself can send a request again, and an answer or a failed dial may belong
// to that second attempt, after a first that took effect: over HTTP/2 it
// re-sends a request whose stream the server reset with PROTOCOL_ERROR, which
// does not say whether the server processed it, and it retries on a new
// connection a request that it wrote nothing of on a reused one. The backend
// proves a single attempt from net/http's client trace of the call's context:
// the connections net/http hands the request (GotConn) and the connections it
// asks for (GetConn). A failed mutation is definite, with no effect, only
// when the error and that trace prove it:
//
//   - if the transport did not read a write's body to its end, no request
//     with the whole body was completed, so the service cannot have applied
//     it (a 409 or 412 that answers a re-send whose body was not read to its
//     end is therefore a definite error, but not
//     storage.ErrPreconditionFailed);
//   - the SDK failed before its transport with an error azcore marks
//     non-retriable, and the trace saw at most one connection handed to the
//     request and none asked for after it: its bearer token policy marks so,
//     before the transport, a credential that got no token and its refusal
//     to send a token over plain HTTP, and after it only a failed 401
//     challenge, which has no effect unless it answers a re-send;
//   - the trace saw exactly one connection handed to the request and none
//     asked for after it, and the service answered with a status that
//     refuses the request: 409 BlobAlreadyExists or 412 (Create or Replace),
//     or 404 BlobNotFound (Replace), is storage.ErrPreconditionFailed, and
//     any other 4xx except 408 is a definite error (the status comes from the
//     *azcore.ResponseError in the error). The backend assumes that a 4xx
//     other than 408 comes from the service, or from an intermediary that
//     refused the request itself, never from one that forwarded the request
//     and replaced the answer;
//   - the trace saw a connection asked for and none handed to the request,
//     and the error holds net/http's *net.OpError for a dial ("dial", or
//     "proxyconnect" through a proxy): no connection was made, so nothing of
//     the request was sent.
//
// A credential that calls its token endpoint with the call's context shares
// the trace, so its connections count too. With a transport that passes the
// context on, they can only turn an answer into unknown.
//
// Everything else wraps storage.ErrOutcomeUnknown: a lost response, a reset
// connection, EOF, a body that fails after a response arrived, a header that
// does not parse, the context ending, any other transport error, a 5xx, 408,
// and a 2xx or 3xx other than the one the request expects (from a
// non-conforming proxy). Some of these come from requests that were never
// sent, or that had no effect, which the contract allows when the backend
// cannot tell: for Delete and a write with an empty body when the context
// ends while a credential waits for a token, or before the transport dials
// (the SDK then returns the context's error alone); when a credential fails
// with an error azcore does not mark non-retriable, or with one it does mark
// after its own connections put more than one in the trace, or the SDK fails before
// its transport with such an error, for example a shared key that cannot sign
// the request; when a connection fails with an error other than net/http's
// dial error, such as a TLS handshake failure or any failure of a transport
// that hides net/http's errors; when a credential's own connection in the
// call's trace makes a refused connection look like a re-send; when net/http
// retries, on a new connection, a request it wrote nothing of on a reused one
// (an HTTP/2 connection found unusable, or one the server closed with
// GOAWAY), and the new dial is refused; and, through a transport that does
// not pass the request's context on, for a refused connection and for any
// 4xx answer.
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
// that gets a token for a server whose answers' bodies fail, and transports
// that refuse a connection, report a re-send in the trace, or drop the trace
// while a credential fails to reach a closed proxy, and a local HTTP/2 server
// that resets a request's stream with PROTOCOL_ERROR after applying it. The
// server's acceptance suite has been run once by hand against the real
// service with an Entra ID service principal; this package's own tests have
// not, and real throttling, HTTP/2 against the service and a real proxy are
// not tested.
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
	"os"
	"regexp"
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
}

// New returns a Store on the container that c addresses. Bodies longer than
// 4 MiB are spooled to files in spoolDir, or in os.TempDir if it is empty.
func New(c *container.Client, spoolDir string) *Store {
	return &Store{c: c, spoolDir: spoolDir}
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
	tctx, tries := mutation(ctx)
	resp, err := s.c.NewBlockBlobClient(key).Upload(tctx, up, &blockblob.UploadOptions{
		AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: cond},
	})
	bodySent := up.finish()
	if err != nil {
		var re *azcore.ResponseError
		if tries.single() && errors.As(err, &re) && (re.StatusCode == http.StatusPreconditionFailed ||
			re.StatusCode == http.StatusConflict && re.ErrorCode == string(bloberror.BlobAlreadyExists) ||
			ifMatch != "" && re.StatusCode == http.StatusNotFound && re.ErrorCode == string(bloberror.BlobNotFound)) {
			return "", storage.ErrPreconditionFailed
		}
		return "", mutationError(ctx, err, !bodySent, tries)
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
	tctx, tries := mutation(ctx)
	_, err := s.c.NewBlobClient(key).Delete(tctx, nil)
	// A missing blob is the effect of a Delete, also after a re-send.
	if err == nil || bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil
	}
	return mutationError(ctx, err, false, tries)
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

// attempts records net/http's attempts at one mutation request, from the
// client trace of the call's context. A transport that does not pass the
// request's context on to net/http leaves it empty.
type attempts struct {
	asked atomic.Bool  // a connection was asked for (GetConn)
	got   atomic.Int32 // connections handed to the request (GotConn)
	after atomic.Bool  // a connection was asked for after one was handed over
}

// single reports whether the trace saw exactly one attempt: one connection
// handed to the request and none asked for after it. GetConn runs before a
// dial, but HTTP/2 skips it for the first request on a new pooled connection,
// which may not be the request that dialled it, so a single attempt may lack
// it; a second GotConn, or a GetConn after a GotConn, is a re-send.
func (a *attempts) single() bool { return a.got.Load() == 1 && !a.after.Load() }

// unconnected reports whether the trace saw a connection asked for and none
// handed to the request.
func (a *attempts) unconnected() bool { return a.asked.Load() && a.got.Load() == 0 }

// mutation returns a context for one mutation request, with SDK retries off
// and a client trace that records net/http's attempts at the request.
func mutation(ctx context.Context) (context.Context, *attempts) {
	a := &attempts{}
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(string) {
			a.asked.Store(true)
			if a.got.Load() > 0 {
				a.after.Store(true)
			}
		},
		GotConn: func(httptrace.GotConnInfo) { a.got.Add(1) },
	})
	return policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1}), a
}

// mutationError classifies the error of a mutation request that got no
// precondition answer. unsent reports that the transport did not read the
// body to its end; tries holds the attempts the trace saw.
func mutationError(ctx context.Context, err error, unsent bool, tries *attempts) error {
	var re *azcore.ResponseError
	answered := errors.As(err, &re)
	cause := redact(err)
	if !answered && ctx.Err() != nil {
		cause = ctx.Err()
	}
	if unsent || noEffect(err, tries) {
		return cause
	}
	return fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, cause)
}

// noEffect reports whether err, with the attempts the trace saw, proves that
// its request had no effect: a 4xx answer other than 408 to a single attempt
// (a 2xx or 3xx other than the one the SDK expects comes from a peer that may
// have applied it), an error azcore marks non-retriable, which it raises
// before the transport or for a failed 401 challenge, to at most one attempt,
// or net/http's error for
// a dial when no connection was handed to the request.
func noEffect(err error, tries *attempts) bool {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return tries.single() && re.StatusCode >= 400 && re.StatusCode < 500 && re.StatusCode != http.StatusRequestTimeout
	}
	var nr interface{ NonRetriable() }
	if errors.As(err, &nr) {
		// A failed 401 challenge may answer a re-send of a request that took effect.
		return tries.got.Load() <= 1 && !tries.after.Load()
	}
	var oe *net.OpError
	return tries.unconnected() && errors.As(err, &oe) && (oe.Op == "dial" || oe.Op == "proxyconnect")
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
