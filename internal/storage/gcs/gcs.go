// Package gcs is a storage.Store on a Google Cloud Storage bucket, through the
// JSON API and the official client, cloud.google.com/go/storage.
//
// # Mapping
//
// A key is the object name, unchanged. The version token is the object's
// generation, in decimal. Google documents that a generation is unique to one
// version of an object and that a new object with the same name has a
// different generation, so a token never repeats for a key. The store never
// changes object metadata, so the metageneration plays no part.
//
// Create uploads with ifGenerationMatch=0 and Replace with
// ifGenerationMatch=<generation>; the service answers 412 with reason
// conditionNotMet when the condition does not hold. Exactly one of concurrent
// Creates of a key, or of Replaces from one version, succeeds across all
// processes and hosts that share the bucket if the service checks the
// condition and commits the object in one step. For a resumable upload
// (below) that is an assumption, observed on fake-gcs-server and not
// documented by Google; see Limits. A Replace token that is not a generation
// this store could have returned fails its precondition without a request.
// Delete is unconditional and treats 404 as success.
//
// # Uploads
//
// A write streams its body through the client's Writer in chunks of
// [ChunkSize] bytes. A body that ends within the first chunk is sent in one
// request (uploadType=multipart) once the body has ended. A longer body goes
// through a resumable upload session: every chunk but the last is sent while
// the body is read, and the object exists only once the last request of the
// session, sent after the body has ended, completes. Memory per write is bounded
// by one chunk plus a copy buffer: [ChunkSize] + 256 KiB. A body with a Len
// method (bytes.Reader, strings.Reader, bytes.Buffer) shorter than a chunk
// gets a chunk of its length plus one, which the client rounds up to a
// multiple of 256 KiB. There is no size limit beyond the service's own
// (5 TiB per object).
//
// The request that can make a write take effect is the commit request: the
// single-request upload, the last request of a resumable session (whose
// Content-Range names the total size), or the DELETE. The store marks, in its
// own http.RoundTripper, below authentication, when a call's commit request
// starts, so a failed token fetch is an error before that point. An error
// before that point means the call had no effect; an error after it wraps
// storage.ErrOutcomeUnknown unless the service answered 412 with reason
// conditionNotMet (precondition failed) or another 4xx other than 408, or the
// connection was never made. A 412 with another reason, such as
// orgPolicyConstraintFailed, is the service's error, with no effect.
//
// # Retries
//
// The client retries on its own. The store allows that only where a retry
// cannot change the outcome: reads, listings, and the request that opens a
// resumable session, which publishes nothing and carries none of the body.
// The client never retries a commit request, so a lost response is reported
// as storage.ErrOutcomeUnknown. net/http can still send a request twice on its
// own, below the store's transport: over HTTP/2 it re-sends a request whose
// stream the server reset with PROTOCOL_ERROR, which does not say whether the
// server processed it. The store counts the attempts net/http makes at a
// commit request (each connection it hands the request, and each connection
// it asks for after one); after more than one, every error of the call, a 412,
// another 4xx or a refused connection included, wraps
// storage.ErrOutcomeUnknown, because the answer may be to the re-send of an
// attempt that took effect. The client does not retry a chunk of a resumable
// session either: a retried chunk whose first attempt the emulator had stored
// was stored twice, giving an object longer than the body, and the service's
// documentation does not say how it treats a chunk it already has. A failed chunk fails the write, with no
// effect.
//
// # Configuration
//
// [Open] takes a bucket, which must exist, and an optional endpoint. The
// credentials keep the values of Open's context but not its cancellation, so
// the Store keeps working after that context ends. Without an endpoint the
// store uses Application Default Credentials with the
// devstorage.read_write scope: a GKE workload identity, a service-account key
// named by GOOGLE_APPLICATION_CREDENTIALS, or the metadata server. With an
// endpoint, such as an emulator's, it sends no credentials. The client reads
// STORAGE_EMULATOR_HOST from the environment; Open refuses to run with
// credentials while it is set, so that they are never sent to the emulator.
//
// # Limits
//
//   - Tested against the fake-gcs-server emulator only (see the package
//     tests), not against the real service.
//   - Exactly one winner among concurrent conditional writes of [ChunkSize]
//     bytes or more assumes that the service evaluates ifGenerationMatch at
//     the last request of a resumable session, as the emulator does. Google
//     does not document when it does; if it checked only when the session
//     opens, two writers could both succeed. This must be checked against the
//     real service before the server may use this backend.
//   - Version tokens of one bucket are not portable to another.
//   - The bucket must not have object versioning, retention policies or
//     holds that make a Delete fail; a Delete that the service refuses is
//     returned as an error.
package gcs

import (
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
	"sync/atomic"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// ChunkSize is the size of the chunks of a resumable upload, the client's
// default. A body of at most ChunkSize bytes is uploaded in one request.
const ChunkSize = 16 << 20

const copyBufferSize = 256 << 10

// Config configures a Store.
type Config struct {
	// Bucket is the name of an existing bucket.
	Bucket string
	// Endpoint, if set, is the JSON API base URL, for example
	// "http://127.0.0.1:4443/storage/v1/" for an emulator. Requests to it carry
	// no credentials. If empty, the store uses Google's endpoint and
	// Application Default Credentials.
	Endpoint string
}

// Store is a storage.Store on one bucket. It is safe for concurrent use.
type Store struct {
	client *gcs.Client
	bucket *gcs.BucketHandle
}

var _ storage.Store = (*Store)(nil)

// Open returns a Store on cfg.Bucket. It does not contact the service. Its
// HTTP transport, a clone of http.DefaultTransport, waits
// storage.ResponseHeaderTimeout for each response's headers.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	dt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("gcs: http.DefaultTransport is not an *http.Transport")
	}
	tr := dt.Clone()
	tr.ResponseHeaderTimeout = storage.ResponseHeaderTimeout
	return open(ctx, cfg, tr)
}

// open is Open with base carrying the HTTP requests, below authentication.
func open(ctx context.Context, cfg Config, base http.RoundTripper) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("gcs: no bucket")
	}
	opts := []option.ClientOption{gcs.WithJSONReads()}
	base = markCommits{base} // below authentication, so a failed token fetch sends no commit request
	if cfg.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(cfg.Endpoint))
	} else {
		if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
			return nil, errors.New("gcs: STORAGE_EMULATOR_HOST is set; set Config.Endpoint to use an emulator")
		}
		var err error
		// Some credentials fetch every later token with this context, so it
		// must not end with Open's.
		if base, err = htransport.NewTransport(context.WithoutCancel(ctx), base, option.WithScopes(gcs.ScopeReadWrite)); err != nil {
			return nil, fmt.Errorf("gcs: credentials: %w", redact(err))
		}
	}
	opts = append(opts, option.WithHTTPClient(&http.Client{Transport: base}))
	c, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: %w", redact(err))
	}
	return &Store{client: c, bucket: c.Bucket(cfg.Bucket)}, nil
}

// Close releases the client's resources.
func (s *Store) Close() error { return s.client.Close() }

// progress is what markCommits records of one Create, Replace or Delete. It
// travels in the call's context.
type progress struct {
	data   atomic.Bool  // a request carrying the body, or the commit request, has started
	commit atomic.Bool  // the commit request has started
	got    atomic.Int32 // connections net/http has handed the commit request
	again  atomic.Bool  // net/http has started a second attempt at the commit request
}

// resent reports whether net/http may have sent the commit request more than
// once. It re-sends a request on its own, below this store's transport, for
// example when an HTTP/2 server resets the stream with PROTOCOL_ERROR, which
// does not say whether the server processed it.
func (p *progress) resent() bool { return p.again.Load() }

type progressKey struct{}

// markCommits is the http.RoundTripper right above net/http's, below
// authentication.
type markCommits struct{ next http.RoundTripper }

func (m markCommits) RoundTrip(r *http.Request) (*http.Response, error) {
	if p, ok := r.Context().Value(progressKey{}).(*progress); ok {
		c := commits(r)
		if c || r.Header.Get("Content-Range") != "" {
			p.data.Store(true)
		}
		if c {
			p.commit.Store(true)
			// GotConn runs once per attempt that has a connection. GetConn runs
			// before a dial, but HTTP/2 skips it for the first request on a new
			// pooled connection, which may not be the request that dialled it.
			// So a second GotConn, or a GetConn after a GotConn, is a re-send.
			r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{
				GetConn: func(string) {
					if p.got.Load() > 0 {
						p.again.Store(true)
					}
				},
				GotConn: func(httptrace.GotConnInfo) {
					if p.got.Add(1) > 1 {
						p.again.Store(true)
					}
				},
			}))
		}
	}
	return m.next.RoundTrip(r)
}

// commits reports whether r can make a write or delete take effect: anything
// but a read, the request that opens a resumable session, or a chunk of one
// that is not its last (Content-Range "bytes a-b/*").
func commits(r *http.Request) bool {
	if cr := r.Header.Get("Content-Range"); cr != "" {
		return !strings.HasSuffix(cr, "/*")
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return false
	case http.MethodPost:
		return r.URL.Query().Get("uploadType") != "resumable"
	}
	return true
}

func fail(op, key string, err error) error { return &storage.Error{Op: op, Key: key, Err: err} }

// Create implements storage.Store.
func (s *Store) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("create", key, err)
	}
	return s.write(ctx, "create", key, gcs.Conditions{DoesNotExist: true}, body)
}

// Replace implements storage.Store.
func (s *Store) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("replace", key, err)
	}
	gen, err := strconv.ParseInt(version, 10, 64)
	if err != nil || gen <= 0 || strconv.FormatInt(gen, 10) != version {
		return "", fail("replace", key, storage.ErrPreconditionFailed)
	}
	return s.write(ctx, "replace", key, gcs.Conditions{GenerationMatch: gen}, body)
}

func (s *Store) write(ctx context.Context, op, key string, cond gcs.Conditions, body io.Reader) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fail(op, key, err)
	}
	p := new(progress)
	wctx, cancel := context.WithCancel(context.WithValue(ctx, progressKey{}, p))
	defer cancel()
	w := s.bucket.Object(key).If(cond).Retryer(gcs.WithErrorFunc(func(err error) bool {
		return !p.data.Load() && gcs.ShouldRetry(err)
	})).NewWriter(wctx)
	w.ChunkSize = ChunkSize
	if l, ok := body.(interface{ Len() int }); ok && l.Len() < ChunkSize {
		// The client allocates a whole chunk per upload; one byte more than the
		// body still sends it in one request. A wrong Len changes only that plan.
		w.ChunkSize = l.Len() + 1
	}
	w.ContentType = "application/octet-stream"
	// stop ends the upload and waits for the Writer's goroutine. Without an
	// end of the body no commit request is sent, so the write has no effect.
	stop := func(err error) (string, error) {
		cancel()
		_ = w.Close()
		return "", fail(op, key, err)
	}
	buf := make([]byte, copyBufferSize)
	for {
		n, rerr := body.Read(buf)
		if rerr != nil && rerr != io.EOF { //nolint:errorlint // io.Reader returns io.EOF itself
			return stop(&storage.BodyError{Err: rerr})
		}
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				cancel()
				_ = w.Close()
				return "", writeError(ctx, op, key, err, p.commit.Load(), p.resent())
			}
		}
		if err := ctx.Err(); err != nil {
			return stop(err)
		}
		if rerr == io.EOF { //nolint:errorlint // as above
			break
		}
	}
	if err := w.Close(); err != nil {
		return "", writeError(ctx, op, key, err, p.commit.Load(), p.resent())
	}
	if a := w.Attrs(); a != nil && a.Generation > 0 {
		return strconv.FormatInt(a.Generation, 10), nil
	}
	return "", fail(op, key, fmt.Errorf("%w: gcs: response without generation", storage.ErrOutcomeUnknown))
}

// writeError classifies err from a Create, Replace or Delete; sent reports
// whether its commit request had started, resent whether net/http may have
// sent it twice, in which case the answer may be to the second attempt.
func writeError(ctx context.Context, op, key string, err error, sent, resent bool) error {
	var ge *googleapi.Error
	isHTTP := errors.As(err, &ge)
	if isHTTP && ge.Code == http.StatusPreconditionFailed && !resent && conditionNotMet(ge) {
		return fail(op, key, storage.ErrPreconditionFailed)
	}
	definite := !sent || isHTTP && ge.Code >= 400 && ge.Code < 500 && ge.Code != http.StatusRequestTimeout
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		definite = true // the connection was never made, so nothing was sent
	}
	if resent {
		definite = false
	}
	e := redact(err)
	if cerr := ctx.Err(); cerr != nil {
		e = fmt.Errorf("%w: %w", cerr, e)
	}
	if !definite {
		e = fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, e)
	}
	return fail(op, key, e)
}

// conditionNotMet reports whether a 412 is the write's own condition failing.
// The service also answers 412 for reasons such as orgPolicyConstraintFailed.
func conditionNotMet(ge *googleapi.Error) bool {
	for _, e := range ge.Errors {
		if e.Reason != "conditionNotMet" {
			return false
		}
	}
	return true
}

// readError wraps err from a read, list or body Read: storage.ErrNotFound for
// a missing object, the context's error if it has ended.
func readError(ctx context.Context, err error) error {
	if errors.Is(err, gcs.ErrObjectNotExist) {
		return storage.ErrNotFound
	}
	e := redact(err)
	if cerr := ctx.Err(); cerr != nil {
		e = fmt.Errorf("%w: %w", cerr, e)
	}
	return e
}

// queryString matches the query of a URL. A resumable upload's session URL
// carries its upload_id there, which grants access to the upload.
var queryString = regexp.MustCompile(`\?[^\s"]*`)

// redact returns err's text without URL queries, as an error that wraps
// nothing. The client's error texts hold no object contents or credentials
// otherwise.
func redact(err error) error {
	return errors.New(queryString.ReplaceAllString(err.Error(), "?REDACTED"))
}

// Get implements storage.Store.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if err := storage.ValidateKey(key); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	return s.read(ctx, key, 0, -1)
}

// GetRange implements storage.Store.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if err := storage.ValidateKey(key); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	if offset < 0 || length < 0 {
		return nil, storage.Info{}, fail("get", key, fmt.Errorf("%w: offset %d, length %d", storage.ErrInvalidArgument, offset, length))
	}
	if length > math.MaxInt64-offset {
		length = -1 // to the end
	}
	return s.read(ctx, key, offset, length)
}

// read returns length bytes from offset, or the rest of the object if length
// is -1. HTTP cannot ask for zero bytes, so length 0 is a Head, and a range
// that starts at or past the end (416) is answered with a Head that shows the
// object still ends there; if it has grown since, the range is read again.
func (s *Store) read(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	for attempt := 0; ; attempt++ {
		if length == 0 {
			info, err := s.head(ctx, "get", key)
			if err != nil {
				return nil, storage.Info{}, err
			}
			return io.NopCloser(strings.NewReader("")), info, nil
		}
		r, err := s.bucket.Object(key).NewRangeReader(ctx, offset, length)
		var ge *googleapi.Error
		if errors.As(err, &ge) && ge.Code == http.StatusRequestedRangeNotSatisfiable && attempt < 3 {
			info, err := s.head(ctx, "get", key)
			if err != nil {
				return nil, storage.Info{}, err
			}
			if info.Size <= offset {
				return io.NopCloser(strings.NewReader("")), info, nil
			}
			continue
		}
		if err != nil {
			return nil, storage.Info{}, fail("get", key, readError(ctx, err))
		}
		if r.Attrs.Generation <= 0 || r.Attrs.Size < 0 {
			_ = r.Close()
			return nil, storage.Info{}, fail("get", key, errors.New("gcs: response without generation or size"))
		}
		info := storage.Info{Size: r.Attrs.Size, Version: strconv.FormatInt(r.Attrs.Generation, 10)}
		return &reader{ctx: ctx, r: r}, info, nil
	}
}

// reader is a Get or GetRange body. The client resumes a broken download
// only at the generation it started with, so every byte comes from one
// version.
type reader struct {
	ctx context.Context
	r   *gcs.Reader
}

func (b *reader) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := b.r.Read(p)
	if err != nil && err != io.EOF { //nolint:errorlint // io.Reader returns io.EOF itself
		err = readError(b.ctx, err)
	}
	return n, err
}

func (b *reader) Close() error { return b.r.Close() }

// Head implements storage.Store.
func (s *Store) Head(ctx context.Context, key string) (storage.Info, error) {
	if err := storage.ValidateKey(key); err != nil {
		return storage.Info{}, fail("head", key, err)
	}
	return s.head(ctx, "head", key)
}

func (s *Store) head(ctx context.Context, op, key string) (storage.Info, error) {
	if err := ctx.Err(); err != nil {
		return storage.Info{}, fail(op, key, err)
	}
	a, err := s.bucket.Object(key).Attrs(ctx)
	if err != nil {
		return storage.Info{}, fail(op, key, readError(ctx, err))
	}
	if a.Generation <= 0 || a.Size < 0 {
		return storage.Info{}, fail(op, key, errors.New("gcs: response without generation or size"))
	}
	return storage.Info{Size: a.Size, Version: strconv.FormatInt(a.Generation, 10)}, nil
}

// List implements storage.Store. The service may return short pages; the
// client's iterator keeps requesting pages until limit entries have been
// collected or the listing ends.
func (s *Store) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if limit < 1 || limit > storage.MaxListLimit {
		return nil, fail("list", "", fmt.Errorf("%w: limit %d is not between 1 and %d", storage.ErrInvalidArgument, limit, storage.MaxListLimit))
	}
	if err := ctx.Err(); err != nil {
		return nil, fail("list", "", err)
	}
	start, ok := listStart(startAfter)
	if !ok || !isASCII(prefix) {
		return nil, nil // keys are ASCII, so none can match
	}
	q := &gcs.Query{Prefix: prefix, StartOffset: start}
	if err := q.SetAttrSelection([]string{"Name", "Size"}); err != nil {
		return nil, fail("list", "", err)
	}
	it := s.bucket.Objects(ctx, q)
	it.PageInfo().MaxSize = min(limit+1, storage.MaxListLimit) // +1: StartOffset includes startAfter itself
	var out []storage.Entry
	for len(out) < limit {
		a, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fail("list", "", readError(ctx, err))
		}
		if a.Name > startAfter {
			out = append(out, storage.Entry{Key: a.Name, Size: a.Size})
		}
	}
	return out, nil
}

// listStart returns the service's inclusive StartOffset for the keys after
// startAfter, which need not be a key. Keys are ASCII, so if startAfter has a
// byte of 0x80 or more at position i, the keys after it are those at or after
// the first string past every string that begins with startAfter[:i]. ok is
// false if no key can sort after startAfter.
func listStart(startAfter string) (start string, ok bool) {
	i := strings.IndexFunc(startAfter, func(r rune) bool { return r >= 0x80 })
	if i < 0 {
		return startAfter, true
	}
	p := strings.TrimRight(startAfter[:i], "\x7f")
	if p == "" {
		return "", false
	}
	return p[:len(p)-1] + string(p[len(p)-1]+1), true
}

func isASCII(s string) bool { return !strings.ContainsFunc(s, func(r rune) bool { return r >= 0x80 }) }

// Delete implements storage.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return fail("delete", key, err)
	}
	if err := ctx.Err(); err != nil {
		return fail("delete", key, err)
	}
	p := new(progress)
	err := s.bucket.Object(key).Retryer(gcs.WithPolicy(gcs.RetryNever)).Delete(context.WithValue(ctx, progressKey{}, p))
	if err == nil || errors.Is(err, gcs.ErrObjectNotExist) {
		return nil
	}
	return writeError(ctx, "delete", key, err, p.commit.Load(), p.resent())
}
