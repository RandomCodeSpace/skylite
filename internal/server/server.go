// Package server is skylite's HTTPS server: the client protocol of ADR 0005
// (docs/content/docs/decisions/0005-client-protocol.md) with the
// authentication of ADR 0006.
//
// This version serves one request, PUT /v1/databases/{db}, which creates a
// database. Every other path of the protocol is authenticated and then
// answered with not_serving: there is no leader, no forwarding and no
// cluster membership yet. Of the admission control of ADR 0005 decision 9,
// only the bound on HTTP connections is implemented.
package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/net/netutil"

	"github.com/RandomCodeSpace/skylite/internal/auth"
	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/gcs"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
	"github.com/RandomCodeSpace/skylite/internal/storage/s3"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// Limits of ADR 0005 decision 9 that wire.Limits does not carry.
const (
	headerDeadline = 10 * time.Second // header block read deadline
	bodyDeadline   = 30 * time.Second // body read deadline, from the end of the header block
	maxStreams     = 16               // concurrent streams per HTTP/2 connection
	maxConns       = 1024             // HTTP connections accepted
)

const (
	// idleTimeout closes a connection with no request running after this
	// long. ADR 0005 gives no value; 90 seconds is this server's choice.
	idleTimeout = 90 * time.Second
	// createTimeout bounds one database creation. It runs detached from the
	// request, so a client that goes away does not cut a create in half.
	createTimeout = 30 * time.Second
	// shutdownTimeout bounds the wait for running requests on shutdown.
	shutdownTimeout = 60 * time.Second
	// The page size and pages per base group of a new database.
	pageSize  = 4096
	groupSize = 256
)

// Server serves the client protocol. Create it with New.
type Server struct {
	cfg    Config
	log    *slog.Logger
	guard  *auth.Guard
	store  storage.Store
	cert   *tls.Certificate
	limits wire.Limits

	// The constants above, and the response deadline of limits, as set by
	// New; tests shorten them.
	headerDeadline, bodyDeadline, responseDeadline time.Duration
	idleTimeout, createTimeout, shutdownTimeout    time.Duration
	maxConns                                       int
}

// New validates cfg, loads the TLS certificate and the JWKS files, and opens
// the storage backend. A cloud backend takes its credentials from its SDK's default
// credential chain. A nil log discards log records. New sends no request.
func New(cfg Config, log *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s, err := newServer(cfg, log, nil)
	if err != nil {
		return nil, err
	}
	if s.store, err = openStore(context.Background(), cfg.Storage); err != nil {
		return nil, err
	}
	return s, nil
}

// newServer is New with the store given; New opens it after everything else
// has succeeded.
func newServer(cfg Config, log *slog.Logger, st storage.Store) (*Server, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	g, err := auth.New(cfg.Auth, nil, nil, log)
	if err != nil {
		return nil, err
	}
	if err := g.LoadKeyFiles(); err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	l := wire.DefaultLimits()
	s := &Server{
		cfg: cfg, log: log, guard: g, store: st, limits: l,
		headerDeadline: headerDeadline, bodyDeadline: bodyDeadline, responseDeadline: time.Duration(l.ResponseMS) * time.Millisecond,
		idleTimeout: idleTimeout, createTimeout: createTimeout, shutdownTimeout: shutdownTimeout, maxConns: maxConns,
	}
	if cfg.TLS != nil {
		c, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("server: tls: %w", err)
		}
		s.cert = &c
	}
	return s, nil
}

func openStore(ctx context.Context, c Storage) (storage.Store, error) {
	switch {
	case c.LocalFS != nil:
		return localfs.Open(c.LocalFS.Path)
	case c.S3 != nil:
		aws, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.S3.Region))
		if err != nil {
			return nil, fmt.Errorf("server: s3 credentials: %w", err)
		}
		return s3.New(s3.Config{Bucket: c.S3.Bucket, Region: c.S3.Region, Credentials: aws.Credentials, Endpoint: c.S3.Endpoint, UsePathStyle: c.S3.PathStyle})
	default:
		return gcs.Open(ctx, gcs.Config{Bucket: c.GCS.Bucket, Endpoint: c.GCS.Endpoint})
	}
}

// Serve serves at most 1,024 connections at a time from ln until ctx is
// done, then stops accepting connections, waits up to a minute for running
// requests, closes the store and returns. It returns nil after a clean
// shutdown, and an error if it had to close connections with requests still
// running.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	defer func() {
		if c, ok := s.store.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	var p http.Protocols
	p.SetHTTP1(true)
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: s.headerDeadline,
		IdleTimeout:       s.idleTimeout,
		MaxHeaderBytes:    int(s.limits.HeaderBlockBytes),
		HTTP2:             &http.HTTP2Config{MaxConcurrentStreams: maxStreams},
		Protocols:         &p,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
	ln = netutil.LimitListener(ln, s.maxConns)
	serve := func() error { return srv.Serve(ln) }
	if s.cert != nil {
		p.SetHTTP2(true)
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{*s.cert}, MinVersion: tls.VersionTLS12}
		serve = func() error { return srv.ServeTLS(ln, "", "") }
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	errc := make(chan error, 1)
	go func() { errc <- serve() }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	err := srv.Shutdown(sctx)
	if err != nil {
		_ = srv.Close()
		err = fmt.Errorf("server: requests still running after %v were cut: %w", s.shutdownTimeout, err)
	}
	if serr := <-errc; err == nil && !errors.Is(serr, http.ErrServerClosed) {
		err = serr
	}
	return err
}

// ServeHTTP answers one request with ADR 0005's headers and one NDJSON
// terminal frame.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	// The body read deadline runs from the end of the header block, and also
	// bounds a body a refusal leaves unread.
	_ = rc.SetReadDeadline(time.Now().Add(s.bodyDeadline))
	id := rand.Text()
	h := w.Header()
	h.Set("Skylite-Protocol", "1")
	h.Set("Skylite-Request-Id", id)
	h.Set("Content-Type", "application/x-ndjson")
	h.Set("Cache-Control", "no-store")
	log := s.log.With(slog.String("request_id", id))
	done, e, bodyRead := s.handle(w, r, log)
	status := http.StatusOK
	if e != nil {
		e.RequestID = id
		status = e.Code.Status()
		if e.Code == wire.CodeUnauthenticated {
			h.Set("WWW-Authenticate", "Bearer")
		}
		// Without it, net/http reads up to 256 KiB of the unread body
		// before it writes the answer.
		if !bodyRead && r.ProtoMajor == 1 {
			h.Set("Connection", "close")
		}
		log.LogAttrs(r.Context(), slog.LevelInfo, "request refused", slog.String("method", r.Method), slog.String("code", string(e.Code)))
	}
	// The frame is encoded before the status is sent, so that a frame the
	// codec refuses becomes an internal error rather than a half answer.
	var b bytes.Buffer
	out, err := wire.NewWriter(&b, wire.DefaultFrameBytes, s.limits)
	if err == nil {
		if e != nil {
			err = out.Error(e)
		} else {
			err = out.Done(done)
		}
		if err != nil {
			log.LogAttrs(r.Context(), slog.LevelError, "response not encoded", slog.String("error", err.Error()))
			status = http.StatusInternalServerError
			ie := wire.NewError(wire.CodeInternal, "the server could not encode its answer")
			ie.NotExecuted, ie.RequestID = false, id
			b.Reset()
			err = out.Error(ie)
		}
	}
	if err != nil { // only limits Validate refuses, which New's are not
		b.Reset()
		status = http.StatusInternalServerError
	}
	_ = rc.SetWriteDeadline(time.Now().Add(s.responseDeadline))
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}

// handle runs the checks before acceptance of ADR 0005 in their order, then
// serves the request. Admission (check 2) is not implemented. bodyRead
// reports whether the body was read to its end.
func (s *Server) handle(w http.ResponseWriter, r *http.Request, log *slog.Logger) (done *wire.Done, e *wire.Error, bodyRead bool) {
	kind, db, e := wire.Route(r.Method, r.URL.EscapedPath())
	if e != nil {
		return nil, e, false
	}
	if r.URL.RawQuery != "" {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the path must have no query"), false
	}
	// Authentication, from the headers only: the body is not read before it.
	token, err := auth.BearerToken(r.Header)
	var p *auth.Principal
	if err == nil {
		p, err = s.guard.Verify(r.Context(), token)
	}
	if err != nil {
		ae, ok := errors.AsType[*auth.Error](err)
		if !ok {
			ae = &auth.Error{Code: auth.CodeMalformed}
		}
		log.LogAttrs(r.Context(), slog.LevelInfo, "authentication refused", auth.RefusalAttrs(ae, token)...)
		return nil, wire.NewError(wire.CodeUnauthenticated, "the bearer token is missing, malformed, expired or unverifiable"), false
	}
	// Authorisation, before routing and before anything depends on whether
	// the database exists. Every request needs read; a PUT needs admin.
	need := auth.RoleRead
	if kind == wire.KindPut {
		need = auth.RoleAdmin
	}
	if !s.guard.Authorize(p, db, need) {
		attrs := auth.RefusalAttrs(&auth.Error{Code: auth.CodeForbidden, Issuer: p.Issuer, Subject: p.Subject}, token)
		attrs = append(attrs, slog.String("database", db), slog.String("role", string(need)))
		log.LogAttrs(r.Context(), slog.LevelInfo, "authorisation refused", attrs...)
		return nil, wire.NewError(wire.CodeForbidden, "the principal's role on this database name does not allow this request"), false
	}
	if kind != wire.KindPut {
		return nil, wire.NewError(wire.CodeNotServing, "this server serves only database creation (PUT) in this version"), false
	}
	body, e := s.readBody(w, r)
	if e != nil {
		return nil, e, false
	}
	if _, e := wire.Parse(kind, body, int(s.limits.BatchEntries)); e != nil {
		return nil, e, true
	}
	done, e = s.create(r.Context(), db, p, log)
	return done, e, true
}

// readBody applies ADR 0005's body checks to a PUT or POST and reads the
// complete body within the body read deadline.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, *wire.Error) {
	mt, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";")
	switch {
	case !strings.EqualFold(strings.TrimSpace(mt), "application/json"):
		return nil, wire.NewError(wire.CodeInvalidRequest, "Content-Type must be application/json")
	case len(r.TransferEncoding) > 0 || r.Header.Get("Transfer-Encoding") != "":
		return nil, wire.NewError(wire.CodeInvalidRequest, "Transfer-Encoding is not allowed; send Content-Length")
	case r.ContentLength < 0 || r.Header.Get("Content-Length") == "": // HTTP/1.1 reads a request without one as empty
		return nil, wire.NewError(wire.CodeInvalidRequest, "Content-Length is required")
	case !oneContentLength(r):
		return nil, wire.NewError(wire.CodeInvalidRequest, "Content-Length must appear once, with one value")
	case r.ContentLength > s.limits.RequestBodyBytes:
		return nil, wire.NewError(wire.CodeRequestTooLarge, fmt.Sprintf("the body exceeds %d bytes", s.limits.RequestBodyBytes))
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength+1))
	if err != nil || int64(len(body)) != r.ContentLength {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the body did not arrive complete, with exactly its Content-Length, within the body read deadline")
	}
	return body, nil
}

// oneContentLength reports whether the request has one Content-Length value
// and it is the one net/http parsed. HTTP/2 takes the first of several;
// HTTP/1.1 refuses differing ones itself and keeps one of equal ones.
func oneContentLength(r *http.Request) bool {
	v := r.Header.Values("Content-Length")
	if len(v) != 1 {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v[0]), 10, 64)
	return err == nil && n == r.ContentLength
}

// create creates database db. Only engine.ErrExists is database_exists; an
// outcome the store could not decide is internal, because the database may
// or may not have been created.
func (s *Server) create(ctx context.Context, db string, p *auth.Principal, log *slog.Logger) (*wire.Done, *wire.Error) {
	keys, err := s.cfg.Storage.keys(db)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.createTimeout)
		defer cancel()
		_, err = engine.Create(ctx, s.store, keys, pageSize, groupSize)
	}
	switch {
	case err == nil:
		log.LogAttrs(ctx, slog.LevelInfo, "database created", append(p.LogAttrs(), slog.String("database", db))...)
		return &wire.Done{Kind: wire.DonePut, Txn: 1, Epoch: 1}, nil
	case errors.Is(err, storage.ErrOutcomeUnknown):
		log.LogAttrs(ctx, slog.LevelError, "database create outcome unknown", slog.String("database", db), slog.String("error", err.Error()))
		e := wire.NewError(wire.CodeInternal, "the create's outcome is unknown: the database may or may not exist")
		e.NotExecuted = false // the database may have been created
		return nil, e
	case errors.Is(err, engine.ErrExists):
		return nil, wire.NewError(wire.CodeDatabaseExists, "the database exists")
	default:
		log.LogAttrs(ctx, slog.LevelError, "database create failed", slog.String("database", db), slog.String("error", err.Error()))
		return nil, wire.NewError(wire.CodeInternal, "the database could not be created")
	}
}
