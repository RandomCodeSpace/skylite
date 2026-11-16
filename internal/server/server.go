// Package server is skylite's HTTPS server: the client protocol of ADR 0005
// (docs/content/docs/decisions/0005-client-protocol.md) with the
// authentication of ADR 0006.
//
// This version serves one request, PUT /v1/databases/{db}, which creates a
// database. Every other path of the protocol is authenticated and then
// answered with not_serving: there is no leader, no forwarding and no
// cluster membership yet. Admission control (ADR 0005 decision 9) is not
// implemented.
package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

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
)

const (
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
}

// New validates cfg, loads the TLS certificate, and opens the storage
// backend. A cloud backend takes its credentials from its SDK's default
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
	s := &Server{cfg: cfg, log: log, guard: g, store: st, limits: wire.DefaultLimits()}
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

// Serve serves connections from ln until ctx is done, then stops accepting
// connections, waits up to a minute for running requests, closes the store
// and returns. It returns nil after a clean shutdown.
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
		ReadHeaderTimeout: headerDeadline,
		MaxHeaderBytes:    int(s.limits.HeaderBlockBytes),
		HTTP2:             &http.HTTP2Config{MaxConcurrentStreams: maxStreams},
		Protocols:         &p,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
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
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err := srv.Shutdown(sctx)
	if serr := <-errc; err == nil && !errors.Is(serr, http.ErrServerClosed) {
		err = serr
	}
	return err
}

// ServeHTTP answers one request with ADR 0005's headers and one NDJSON
// terminal frame.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := rand.Text()
	h := w.Header()
	h.Set("Skylite-Protocol", "1")
	h.Set("Skylite-Request-Id", id)
	h.Set("Content-Type", "application/x-ndjson")
	h.Set("Cache-Control", "no-store")
	log := s.log.With(slog.String("request_id", id))
	done, e := s.handle(w, r, log)
	out := wire.NewWriter(w, wire.DefaultFrameBytes, s.limits)
	if e != nil {
		e.RequestID = id
		if e.Code == wire.CodeUnauthenticated {
			h.Set("WWW-Authenticate", "Bearer")
		}
		w.WriteHeader(e.Code.Status())
		_ = out.Error(e)
		log.LogAttrs(r.Context(), slog.LevelInfo, "request refused", slog.String("method", r.Method), slog.String("code", string(e.Code)))
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Duration(s.limits.ResponseMS) * time.Millisecond))
	w.WriteHeader(http.StatusOK)
	_ = out.Done(done)
}

// handle runs the checks before acceptance of ADR 0005 in their order, then
// serves the request. Admission (check 2) is not implemented.
func (s *Server) handle(w http.ResponseWriter, r *http.Request, log *slog.Logger) (*wire.Done, *wire.Error) {
	kind, db, e := wire.Route(r.Method, r.URL.EscapedPath())
	if e != nil {
		return nil, e
	}
	// Authentication, from the headers only: the body is not read before it.
	token, err := auth.BearerToken(r.Header)
	var p *auth.Principal
	if err == nil {
		p, err = s.guard.Verify(r.Context(), token)
	}
	if err != nil {
		code := auth.CodeMalformed
		if ae, ok := errors.AsType[*auth.Error](err); ok {
			code = ae.Code
		}
		log.LogAttrs(r.Context(), slog.LevelInfo, "authentication refused", slog.String("auth_code", string(code)))
		return nil, wire.NewError(wire.CodeUnauthenticated, "the bearer token is missing, malformed, expired or unverifiable")
	}
	if kind != wire.KindPut {
		return nil, wire.NewError(wire.CodeNotServing, "this server serves only database creation (PUT) in this version")
	}
	// Authorisation, before anything depends on whether the database exists.
	if !s.guard.Authorize(p, db, auth.RoleAdmin) {
		return nil, wire.NewError(wire.CodeForbidden, "creating a database needs the admin role on its name")
	}
	body, e := s.readBody(w, r)
	if e != nil {
		return nil, e
	}
	if _, e := wire.Parse(kind, body, int(s.limits.BatchEntries)); e != nil {
		return nil, e
	}
	return s.create(r.Context(), db, log)
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
	case r.ContentLength > s.limits.RequestBodyBytes:
		return nil, wire.NewError(wire.CodeRequestTooLarge, fmt.Sprintf("the body exceeds %d bytes", s.limits.RequestBodyBytes))
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyDeadline))
	body, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength+1))
	if err != nil || int64(len(body)) != r.ContentLength {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the body did not arrive complete, with exactly its Content-Length, within the body read deadline")
	}
	return body, nil
}

// create creates database db. Only engine.ErrExists is database_exists; an
// outcome the store could not decide is internal, because the database may
// or may not have been created.
func (s *Server) create(ctx context.Context, db string, log *slog.Logger) (*wire.Done, *wire.Error) {
	keys, err := s.cfg.Storage.keys(db)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), createTimeout)
		defer cancel()
		_, err = engine.Create(ctx, s.store, keys, pageSize, groupSize)
	}
	switch {
	case err == nil:
		log.LogAttrs(ctx, slog.LevelInfo, "database created", slog.String("database", db))
		return &wire.Done{Kind: wire.DonePut, Txn: 1, Epoch: 1}, nil
	case errors.Is(err, storage.ErrOutcomeUnknown):
		log.LogAttrs(ctx, slog.LevelError, "database create outcome unknown", slog.String("database", db), slog.String("error", err.Error()))
		return nil, wire.NewError(wire.CodeInternal, "the create's outcome is unknown: the database may or may not exist")
	case errors.Is(err, engine.ErrExists):
		return nil, wire.NewError(wire.CodeDatabaseExists, "the database exists")
	default:
		log.LogAttrs(ctx, slog.LevelError, "database create failed", slog.String("database", db), slog.String("error", err.Error()))
		return nil, wire.NewError(wire.CodeInternal, "the database could not be created")
	}
}
