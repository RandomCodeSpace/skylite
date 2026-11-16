// Package s3 is a storage.Store on Amazon S3 and S3-compatible object stores.
//
// It is a library package: nothing in the server uses it yet.
//
// # Stored format
//
// Every object this package writes is the caller's bytes followed by a
// 32-byte trailer: the 8 bytes "SKYLS3T1" and 24 bytes from crypto/rand that
// are new for every write. The same 24 bytes, as 48 lowercase hex digits, are
// stored in the object's user metadata under "skylite-id" (the header
// x-amz-meta-skylite-id). An object's size through the interface is its
// stored size minus 32. Reads never return trailer bytes, and Head, Get,
// GetRange and List never count them. Head, Get and GetRange reject an object
// without that metadata or shorter than the trailer with an error that wraps
// none of the storage sentinels, and List fails on an object shorter than the
// trailer; nothing else may write under the bucket.
//
// # Version tokens and conditions
//
// The version token is the object's ETag, as S3 returns it. S3 derives an
// ETag from the stored bytes, so on its own it would repeat when the same
// content is written twice. The random trailer makes the stored bytes, and so
// the ETag, new on every write. Create sends PutObject with If-None-Match: *
// and Replace sends it with If-Match: <token>, so each is one conditional
// request that S3 evaluates atomically, and the token a Replace is
// conditioned on names the same per-write identity that the token was read
// from. A Replace whose version is empty or not a quoted entity tag (such as
// "*", which would match any object) fails its precondition without a
// request. S3 answers If-Match on a missing key with 404, which Replace
// reports as a precondition failure.
//
// # Upload path
//
// A write copies the caller's body to a spool file in Config.SpoolDir, then
// sends it with one PutObject. The spool file is unlinked as soon as it is
// created where the operating system allows it (Linux and macOS), so a
// killed process leaves nothing behind, and it is removed when the call
// returns everywhere else. Memory per write is bounded by the 32 KiB copy
// buffer and the SDK's fixed per-request buffers; disk per write is the body
// size plus 32 bytes. Nothing is sent before the body has been read to its
// end, and S3 never stores part of a PutObject, so a write that fails before
// its request is complete leaves nothing behind, at the key or anywhere else.
// No staged (multipart) upload is used, so a body is limited to S3's
// single-request limit of 5 GiB minus the trailer, 5,368,709,088 bytes. A
// larger body fails with storage.ErrInvalidArgument and no effect, after at
// most that many bytes plus one have been read.
//
// # Retries and unknown outcomes
//
// The SDK's standard retryer runs for every operation, up to 3 attempts, with
// its client-side retry quota disabled and S3's 409
// ConditionalRequestConflict added to the codes it retries. Each attempt of a
// mutation is classified as it ends: an HTTP response below 500 other than
// 2xx is a definite answer with no effect, and so is a transport error before
// the whole request was written (seen through net/http/httptrace); a 2xx that
// failed to decode, a 5xx, or a transport error after the whole request was
// written may have taken effect. The SDK retries a mutation only while every
// attempt so far definitely had no effect, so when a call returns, none of
// its attempts can still be pending unless the call reports an unknown
// outcome. Reads are retried as the SDK decides.
//
// A write that fails definitely returns an error wrapping
// storage.ErrPreconditionFailed for a 412 (and for Replace a 404 NoSuchKey),
// or the SDK's error. A write whose attempt may have taken effect is resolved
// by its per-write identity: while its context is live it sends a HeadObject,
// and if the object carries this write's identity in its metadata the write
// succeeded, with that object's ETag as its token. Anything else wraps
// storage.ErrOutcomeUnknown. Delete is unconditional: if its attempt may have
// taken effect, it returns an error wrapping storage.ErrOutcomeUnknown.
//
// # Reads
//
// Get and GetRange send one ranged GetObject and return the size and ETag of
// the response the body comes from, so the body and its Info belong to one
// version. A range that starts in the trailer returns an empty body. S3
// answers a range that starts past the stored object with 416; GetRange then
// sends a HeadObject and returns an empty body if the offset is at or past
// the end of the version it finds, and tries again if the object has grown in
// between. GetRange of length 0 sends only a HeadObject. HeadObject cannot
// tell a missing bucket from a missing key, so Head reports both as
// storage.ErrNotFound.
//
// # Listing
//
// List sends ListObjectsV2 with the prefix, StartAfter and MaxKeys set to the
// entries still missing, and keeps requesting from the last key it got until
// it has limit entries or the service reports no more, so short pages are
// filled. A prefix that contains a byte keys cannot contain, or is longer
// than storage.MaxKeyLen, matches nothing and is answered without a request.
// A startAfter that keys cannot contain is replaced by one made of key
// characters that selects the same keys, so the service never sees bytes it
// may reject.
//
// # Limits
//
//   - The bucket must exist. Nothing else may write under it.
//   - Strong read-after-write consistency and conditional writes are taken
//     from the service; S3 has documented both since 2024. An S3-compatible
//     store that ignores If-None-Match or If-Match, or does not apply them
//     atomically, breaks the storage contract and is not supported.
//   - Bucket versioning is not needed. If it is enabled, old versions are kept
//     as the bucket's lifecycle rules decide.
//   - Tested against the RustFS emulator only (see the documentation site),
//     which rejects keys with a segment longer than 255 bytes; S3 does not.
package s3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

const (
	trailerMagic = "SKYLS3T1"
	idLen        = 24
	trailerLen   = len(trailerMagic) + idLen
	metaID       = "skylite-id"
)

// maxBody is the largest body a write accepts: S3's 5 GiB PutObject limit
// minus the trailer. Tests lower it.
var maxBody int64 = 5<<30 - int64(trailerLen)

var errForeign = errors.New("s3: object was not written by this backend")

// Config configures a Store.
type Config struct {
	// Bucket is the bucket that holds the objects. It must exist.
	Bucket string
	// Region is the bucket's region, used for request signing.
	Region string
	// Credentials signs every request.
	Credentials aws.CredentialsProvider
	// Endpoint is the base URL of an S3-compatible service or emulator, such
	// as http://127.0.0.1:9000. Empty means Amazon S3's own endpoints.
	Endpoint string
	// UsePathStyle addresses the bucket in the URL path instead of the host
	// name, as most emulators need.
	UsePathStyle bool
	// SpoolDir is the directory where write bodies are spooled before they
	// are sent. Empty means os.TempDir().
	SpoolDir string
}

// Store is a storage.Store on one S3 bucket.
type Store struct {
	client   *awss3.Client
	bucket   string
	spoolDir string
}

var _ storage.Store = (*Store)(nil)

// New returns a Store for cfg. It sends no request.
func New(cfg Config) (*Store, error) {
	return newStore(cfg, nil)
}

// newStore is New with the HTTP client tests put between the SDK and the
// service; nil means the SDK's default.
func newStore(cfg Config, httpClient awss3.HTTPClient) (*Store, error) {
	if cfg.Bucket == "" || cfg.Region == "" || cfg.Credentials == nil {
		return nil, errors.New("s3: Config needs a Bucket, a Region and Credentials")
	}
	o := awss3.Options{
		Region:       cfg.Region,
		Credentials:  cfg.Credentials,
		UsePathStyle: cfg.UsePathStyle,
		HTTPClient:   httpClient,
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.RateLimiter = ratelimit.None
			o.Retryables = append(o.Retryables, retry.RetryableErrorCode{
				Codes: map[string]struct{}{"ConditionalRequestConflict": {}},
			})
		}),
		// Reads stop before the trailer, so a whole-object checksum could never
		// be checked.
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	if cfg.Endpoint != "" {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	dir := cfg.SpoolDir
	if dir == "" {
		dir = os.TempDir()
	}
	return &Store{client: awss3.New(o), bucket: cfg.Bucket, spoolDir: dir}, nil
}

// Create implements storage.Store.
func (s *Store) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.write(ctx, key, "", body)
	if err != nil {
		return "", &storage.Error{Op: "create", Key: key, Err: err}
	}
	return v, nil
}

// Replace implements storage.Store.
func (s *Store) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := check(ctx, key); err != nil {
		return "", &storage.Error{Op: "replace", Key: key, Err: err}
	}
	if !isETag(version) {
		return "", &storage.Error{Op: "replace", Key: key, Err: storage.ErrPreconditionFailed}
	}
	v, err := s.write(ctx, key, version, body)
	if err != nil {
		return "", &storage.Error{Op: "replace", Key: key, Err: err}
	}
	return v, nil
}

// write spools body and puts it at key, if absent when ifMatch is empty and
// otherwise if its ETag is ifMatch.
func (s *Store) write(ctx context.Context, key, ifMatch string, body io.Reader) (string, error) {
	if err := check(ctx, key); err != nil {
		return "", err
	}
	f, size, id, err := s.spool(ctx, body)
	if err != nil {
		return "", err
	}
	defer func() {
		name := f.Name()
		_ = f.Close()
		_ = os.Remove(name) // already gone where the early unlink worked
	}()
	in := &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          io.NewSectionReader(f, 0, size),
		ContentLength: aws.Int64(size),
		Metadata:      map[string]string{metaID: id},
	}
	if ifMatch == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(ifMatch)
	}
	var a attempts
	out, err := s.client.PutObject(ctx, in, a.track)
	if err == nil {
		if v := aws.ToString(out.ETag); v != "" {
			return v, nil
		}
		err = errors.New("s3: PutObject returned no ETag")
		a.mayHaveApplied.Store(true) // it succeeded
	}
	if !a.mayHaveApplied.Load() {
		switch code := statusCode(err); {
		case code == http.StatusPreconditionFailed:
			return "", fmt.Errorf("%w: %w", storage.ErrPreconditionFailed, err)
		case code == http.StatusNotFound && ifMatch != "" && errorCode(err) == "NoSuchKey":
			return "", fmt.Errorf("%w: %w", storage.ErrPreconditionFailed, err)
		}
		return "", withContext(ctx, err)
	}
	if ctx.Err() == nil {
		if v, ok := s.owns(ctx, key, id); ok {
			return v, nil
		}
	}
	return "", unknown(ctx, err)
}

// owns reports whether key holds the object of the write with identity id,
// and returns its version token if it does.
func (s *Store) owns(ctx context.Context, key, id string) (string, bool) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil || out.Metadata[metaID] != id || aws.ToString(out.ETag) == "" {
		return "", false
	}
	return aws.ToString(out.ETag), true
}

// spool copies the body and a new trailer to an unlinked temporary file and
// returns it with its length and the write's identity in hex.
func (s *Store) spool(ctx context.Context, body io.Reader) (*os.File, int64, string, error) {
	f, err := os.CreateTemp(s.spoolDir, "skylite-s3-*")
	if err != nil {
		return nil, 0, "", err
	}
	_ = os.Remove(f.Name()) // the open descriptor keeps the data; fails harmlessly on Windows
	fail := func(err error) (*os.File, int64, string, error) {
		name := f.Name()
		_ = f.Close()
		_ = os.Remove(name)
		return nil, 0, "", err
	}
	n, err := io.Copy(f, io.LimitReader(ctxReader{ctx, bodyReader{body}}, maxBody+1))
	if err != nil {
		return fail(err)
	}
	if n > maxBody {
		return fail(fmt.Errorf("%w: body larger than %d bytes", storage.ErrInvalidArgument, maxBody))
	}
	trailer := make([]byte, trailerLen)
	copy(trailer, trailerMagic)
	_, _ = rand.Read(trailer[len(trailerMagic):]) // never fails; see crypto/rand.Read
	if _, err := f.Write(trailer); err != nil {
		return fail(err)
	}
	return f, n + int64(trailerLen), hex.EncodeToString(trailer[len(trailerMagic):]), nil
}

// Get implements storage.Store.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

// GetRange implements storage.Store.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.getRange(ctx, key, offset, length)
	if err != nil {
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: err}
	}
	return body, info, nil
}

func (s *Store) getRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if err := check(ctx, key); err != nil {
		return nil, storage.Info{}, err
	}
	if offset < 0 || length < 0 {
		return nil, storage.Info{}, storage.ErrInvalidArgument
	}
	if length == 0 {
		info, err := s.head(ctx, key)
		return http.NoBody, info, err
	}
	rng := fmt.Sprintf("bytes=%d-", offset)
	if length <= 1<<63-1-offset {
		rng += strconv.FormatInt(offset+length-1, 10)
	}
	for range 3 {
		out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Range: aws.String(rng)})
		if statusCode(err) == http.StatusRequestedRangeNotSatisfiable {
			info, err := s.head(ctx, key)
			if err != nil {
				return nil, storage.Info{}, err
			}
			if offset >= info.Size {
				return http.NoBody, info, nil
			}
			continue // the object grew after the GET; read it again
		}
		if err != nil {
			if statusCode(err) == http.StatusNotFound && errorCode(err) == "NoSuchKey" {
				return nil, storage.Info{}, fmt.Errorf("%w: %w", storage.ErrNotFound, err)
			}
			return nil, storage.Info{}, withContext(ctx, err)
		}
		start, total, err := contentRange(aws.ToString(out.ContentRange))
		if err == nil && start != offset {
			err = fmt.Errorf("s3: asked for a range from %d, got one from %d", offset, start)
		}
		var info storage.Info
		if err == nil {
			info, err = objectInfo(total, out.ETag, out.Metadata)
		}
		if err != nil {
			_ = out.Body.Close()
			return nil, storage.Info{}, err
		}
		if offset >= info.Size {
			_ = out.Body.Close()
			return http.NoBody, info, nil
		}
		return struct {
			io.Reader
			io.Closer
		}{ctxReader{ctx, io.LimitReader(out.Body, min(length, info.Size-offset))}, out.Body}, info, nil
	}
	return nil, storage.Info{}, errors.New("s3: the object kept changing while its range was read")
}

// contentRange parses "bytes <start>-<end>/<total>".
func contentRange(h string) (start, total int64, err error) {
	var end int64
	if _, err := fmt.Sscanf(h, "bytes %d-%d/%d", &start, &end, &total); err != nil || start < 0 || end < start || total <= end {
		return 0, 0, fmt.Errorf("s3: unexpected Content-Range %q", h)
	}
	return start, total, nil
}

// Head implements storage.Store.
func (s *Store) Head(ctx context.Context, key string) (storage.Info, error) {
	err := check(ctx, key)
	if err == nil {
		var info storage.Info
		if info, err = s.head(ctx, key); err == nil {
			return info, nil
		}
	}
	return storage.Info{}, &storage.Error{Op: "head", Key: key, Err: err}
}

func (s *Store) head(ctx context.Context, key string) (storage.Info, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if statusCode(err) == http.StatusNotFound {
			return storage.Info{}, fmt.Errorf("%w: %w", storage.ErrNotFound, err)
		}
		return storage.Info{}, withContext(ctx, err)
	}
	return objectInfo(aws.ToInt64(out.ContentLength), out.ETag, out.Metadata)
}

// objectInfo returns the Info of a stored object of size bytes, or errForeign
// if this backend did not write it.
func objectInfo(size int64, etag *string, meta map[string]string) (storage.Info, error) {
	id, err := hex.DecodeString(meta[metaID])
	if err != nil || len(id) != idLen || size < int64(trailerLen) || aws.ToString(etag) == "" {
		return storage.Info{}, errForeign
	}
	return storage.Info{Size: size - int64(trailerLen), Version: aws.ToString(etag)}, nil
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
	after, ok := listStart(startAfter)
	if !ok || len(prefix) > storage.MaxKeyLen || strings.ContainsFunc(prefix, func(r rune) bool { return r >= 0x80 || !isKeyByte(byte(r)) }) { //nolint:gosec // r < 0x80 when converted
		return nil, nil
	}
	var out []storage.Entry
	last := startAfter
	for len(out) < limit {
		in := &awss3.ListObjectsV2Input{Bucket: aws.String(s.bucket), MaxKeys: aws.Int32(int32(limit - len(out)))} //nolint:gosec // at most storage.MaxListLimit
		if prefix != "" {
			in.Prefix = aws.String(prefix)
		}
		if after != "" {
			in.StartAfter = aws.String(after)
		}
		page, err := s.client.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, withContext(ctx, err)
		}
		for _, o := range page.Contents {
			k := aws.ToString(o.Key)
			if !strings.HasPrefix(k, prefix) || k <= last {
				return nil, fmt.Errorf("s3: the listing returned %q after %q", k, last)
			}
			size := aws.ToInt64(o.Size) - int64(trailerLen)
			if size < 0 {
				return nil, fmt.Errorf("%w: %q", errForeign, k)
			}
			out = append(out, storage.Entry{Key: k, Size: size})
			last, after = k, k
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		if len(page.Contents) == 0 {
			return nil, errors.New("s3: the listing reported more entries but returned none")
		}
	}
	return out[:min(len(out), limit)], nil
}

// listStart returns a startAfter made only of key bytes that selects the same
// keys as s, or false if no key sorts after s.
func listStart(s string) (string, bool) {
	i := 0
	for i < len(s) && isKeyByte(s[i]) {
		i++
	}
	p := s[:i]
	if len(p) >= storage.MaxKeyLen {
		// A key that starts with p[:MaxKeyLen] is p[:MaxKeyLen], which sorts
		// before s.
		return p[:storage.MaxKeyLen], true
	}
	switch {
	case i == len(s):
		return s, true
	case s[i] < '-':
		// Below every key byte: the keys after s are the keys after p.
		return p, true
	case s[i] > 'z':
		// Above every key byte: the keys after s are those after p that do
		// not start with p, and no key starts with p+"~".
		if p == "" {
			return "", false
		}
		return p + "~", true
	default:
		// Printable, between key bytes: no key starts with s[:i+1].
		return s[:i+1], true
	}
}

func isKeyByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' || c == '/'
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
	var a attempts
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, a.track)
	switch {
	case err == nil:
		return nil
	case a.mayHaveApplied.Load():
		return unknown(ctx, err)
	default:
		return withContext(ctx, err)
	}
}

func check(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	return ctx.Err()
}

// isETag reports whether v is a strong entity tag: a quoted string of
// printable characters other than the quote. Every token this backend issues
// is one, and anything else could not match, or ("*") would match any object.
func isETag(v string) bool {
	if len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
		return false
	}
	for i := 1; i < len(v)-1; i++ {
		if v[i] <= ' ' || v[i] == '"' || v[i] >= 0x7f {
			return false
		}
	}
	return true
}

// attempts records whether any attempt of one SDK operation may have taken
// effect without a definite answer.
type attempts struct{ mayHaveApplied atomic.Bool }

// track is a per-operation SDK option that classifies every attempt,
// retries included, and lets the SDK retry only while every attempt so far
// definitely had no effect.
func (a *attempts) track(o *awss3.Options) {
	o.Retryer = definiteRetryer{o.Retryer, a}
	o.APIOptions = append(o.APIOptions, func(st *middleware.Stack) error {
		return st.Finalize.Insert(middleware.FinalizeMiddlewareFunc("SkyliteAttempt", a.attempt), "Retry", middleware.After)
	})
}

// definiteRetryer stops retrying once an attempt may have taken effect, so
// that no earlier attempt of a call that returned can still be pending.
type definiteRetryer struct {
	aws.Retryer
	a *attempts
}

func (r definiteRetryer) IsErrorRetryable(err error) bool {
	return !r.a.mayHaveApplied.Load() && r.Retryer.IsErrorRetryable(err)
}

func (a *attempts) attempt(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
	var wrote atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(i httptrace.WroteRequestInfo) {
		if i.Err == nil {
			wrote.Store(true)
		}
	}})
	out, md, err := next.HandleFinalize(ctx, in)
	if err != nil {
		code := statusCode(err)
		if code == 0 && wrote.Load() || code >= 500 || code >= 200 && code < 300 {
			a.mayHaveApplied.Store(true)
		}
	}
	return out, md, err
}

// statusCode returns the HTTP status of the response err came from, or 0.
func statusCode(err error) int {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// errorCode returns the S3 error code of err, or "".
func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// withContext makes err wrap the context's error if the context has ended.
func withContext(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil && !errors.Is(err, cerr) {
		return fmt.Errorf("%w: %w", cerr, err)
	}
	return err
}

func unknown(ctx context.Context, err error) error {
	return fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, withContext(ctx, err))
}

// ctxReader fails reads once ctx is done. A read blocked in net/http when ctx
// ends already fails with ctx's error.
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
