// Package server is skylite's HTTPS server: the client protocol of ADR 0005
// (docs/content/docs/decisions/0005-client-protocol.md) with the
// authentication of ADR 0006.
//
// This version serves PUT /v1/databases/{db}, which creates a database, the
// stateless query and describe requests on leaders and opt-in followers
// (see query.go), and write channels with autocommit writes, transactions
// and cancel on leaders (see channel.go). Each configured database has an
// office loop (internal/office). With a peer listener configured, a request
// this server cannot serve is forwarded one hop to the pod that can (see
// forward.go). resolve is authenticated, forwarded to the leader if this
// server does not lead, and answered there with not_serving; GET status is
// authenticated and answered with not_serving on every pod. Of the
// admission control of ADR 0005 decision 9, the bound
// on HTTP connections, the body share of the memory budget, the SQLite
// connection limits and the channel bounds are implemented.
package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/net/netutil"

	"github.com/RandomCodeSpace/skylite/internal/auth"
	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/office"
	"github.com/RandomCodeSpace/skylite/internal/sqlpolicy"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/azureblob"
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
	probeConns     = 64               // connections the readiness probe accepts
	terminalMargin = time.Second      // a query's write deadline past its statement deadline
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

// The memory budget of ADR 0005 decision 9 and what it is divided into:
// the body share; the channels' share, which holds a receipt for each
// channel, held result rows and retained replay data; and SQLite
// connections at their ceiling in what is left. Nothing else is charged;
// see docs/content/docs/server.md.
//
// shortcut: the budget is the decision's default without a container limit;
// derive it from the cgroup limit with the rest of admission.
const (
	memoryBudget = 2 << 30
	bodyShare    = memoryBudget / 2 // request bodies, charged at 8 × Content-Length
	channelShare = maxChannels*receiptBytes + heldShare + replayShare
	connCeiling  = 64 << 20 // SQLite connection memory ceiling
	maxConnLimit = 256      // SQLite connections per pod
	// maxDatabases is the SQLite connections the budget holds after the
	// body share and the channels' share: each led database's engine
	// connection is charged a ceiling, and one must stay for clients.
	maxDatabases = min(maxConnLimit, (memoryBudget-bodyShare-channelShare)/connCeiling)
)

// engineConfig is each leader's configuration. The tail bound is the
// engine's largest; the cache and the temporary-file budget are outside the
// memory budget (ADR 0005 decision 9, "the budget is the bound").
func engineConfig() engine.Config {
	return engine.Config{WALBound: 64 << 20, TailBound: 1 << 15, MaxMemory: connCeiling, TempBudget: 256 << 20, Cache: engine.NewCache(64 << 20)}
}

// Server serves the client protocol. Create it with New.
type Server struct {
	// probeLn and peerLn, if set, are the readiness probe's and the peer
	// listener's in place of cfg.ProbeListen and cfg.Peer.Listen; tests set
	// them to keep their ports bound.
	probeLn, peerLn net.Listener
	peer            *peer // nil without cfg.Peer
	cfg             Config
	log             *slog.Logger
	guard           *auth.Guard
	store           storage.Store
	cert            *tls.Certificate
	limits          wire.Limits

	policy   *sqlpolicy.Policy
	pool     *pool
	engine   engine.Config
	dbs      map[string]*database // the configured databases
	dir      string               // the leaders' local files
	stopping atomic.Bool          // Serve is shutting down: not ready
	instance string               // this process's instance identifier, which channel tokens name

	// chMu guards the channel tables, their channels and write locks, and
	// the counts below (channel.go).
	chMu         sync.Mutex
	nChans       int   // channels on this server, ended but unsettled ones included
	held, replay int64 // held result rows, retained replay data
	chl          chLimits
	afterCommit  func() error // nil; a test sets it to fail a write after its COMMIT

	bodyMu              sync.Mutex
	bodyUsed, bodyShare int64            // request bodies' share of the memory budget, charged at 8 × Content-Length
	bodyBy              map[string]int64 // the charge per principal, at most a quarter of the share
	statementTime       time.Duration

	// The constants above, and the response deadline of limits, as set by
	// New; tests shorten them.
	headerDeadline, bodyDeadline, responseDeadline time.Duration
	idleTimeout, createTimeout, shutdownTimeout    time.Duration
	hopSlack                                       time.Duration
	maxConns                                       int
}

// New validates cfg, loads the TLS certificate and the JWKS files, and opens
// the storage backend. S3 and GCS take their credentials from their SDKs'
// default credential chains; Azure reads its shared key from the configured
// file or environment variable. A nil log discards log records. New sends no
// request.
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
	if err := s.addDatabases(); err != nil {
		if c, ok := s.store.(io.Closer); ok {
			_ = c.Close()
		}
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
	stmt := time.Duration(l.StatementMS) * time.Millisecond
	// The policy's time limit is the statement time; the request's deadline
	// on the engine's Run context lowers it.
	policy, err := sqlpolicy.New(sqlpolicy.Limits{Timeout: stmt})
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg: cfg, log: log, guard: g, store: st, limits: l,
		policy: policy, pool: newPool(maxDatabases - len(cfg.Databases)), engine: engineConfig(), bodyShare: bodyShare, bodyBy: map[string]int64{}, statementTime: stmt,
		instance: rand.Text(), chl: defaultChLimits(),
		headerDeadline: headerDeadline, bodyDeadline: bodyDeadline, responseDeadline: time.Duration(l.ResponseMS) * time.Millisecond,
		idleTimeout: idleTimeout, createTimeout: createTimeout, shutdownTimeout: shutdownTimeout, hopSlack: hopSlack, maxConns: maxConns,
	}
	if cfg.TLS != nil {
		c, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("server: tls: %w", err)
		}
		s.cert = &c
	}
	if cfg.Peer != nil {
		if s.peer, err = newPeer(cfg.Peer); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// azureIdleConnTimeout drops idle connections before Azurite (5 s) or an
// intermediate proxy closes them; see package azureblob's documentation.
const azureIdleConnTimeout = 2 * time.Second

// maxAzureKeyFile bounds the shared key file; a key is 88 bytes of base64.
const maxAzureKeyFile = 4096

// azureStore wraps an azureblob.Store so closing the store closes the HTTP
// transport's idle connections.
type azureStore struct {
	storage.Store
	close func()
}

func (s azureStore) Close() error {
	s.close()
	return nil
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
	case c.GCS != nil:
		return gcs.Open(ctx, gcs.Config{Bucket: c.GCS.Bucket, Endpoint: c.GCS.Endpoint})
	default:
		return openAzure(c.Azure)
	}
}

func openAzure(a *Azure) (storage.Store, error) {
	var (
		key  *container.SharedKeyCredential
		cred azcore.TokenCredential
	)
	if a.Entra {
		c, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("server: azure entra credential: %w", err)
		}
		cred = detachedCredential{c}
	} else {
		k, err := readAzureSharedKey(a)
		if err != nil {
			return nil, err
		}
		if key, err = container.NewSharedKeyCredential(a.Account, k); err != nil {
			return nil, errors.New("server: azure shared key is not valid base64")
		}
	}
	dt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("server: azure client: http.DefaultTransport is not an *http.Transport")
	}
	tr := dt.Clone()
	tr.IdleConnTimeout = azureIdleConnTimeout
	tr.ResponseHeaderTimeout = storage.ResponseHeaderTimeout
	// A redirect would resend a signed request, body included, elsewhere.
	hc := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	opts := &container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}}
	var (
		c   *container.Client
		err error
	)
	if cred != nil {
		c, err = container.NewClient(a.containerURL(), cred, opts)
	} else {
		c, err = container.NewClientWithSharedKeyCredential(a.containerURL(), key, opts)
	}
	if err != nil {
		tr.CloseIdleConnections()
		return nil, fmt.Errorf("server: azure client: %w", err)
	}
	return azureStore{Store: azureblob.New(c, ""), close: tr.CloseIdleConnections}, nil
}

// detachedCredential fetches each token under a context that keeps the call's
// deadline and cancellation but none of its values: azureblob proves a request
// was sent once from net/http's client trace in the call's context, and a
// token request made under it would add its own connections to that trace.
type detachedCredential struct{ azcore.TokenCredential }

func (d detachedCredential) GetToken(ctx context.Context, o policy.TokenRequestOptions) (azcore.AccessToken, error) {
	dctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer context.AfterFunc(ctx, cancel)()
	if dl, ok := ctx.Deadline(); ok {
		var stop context.CancelFunc
		dctx, stop = context.WithDeadline(dctx, dl)
		defer stop()
	}
	return d.TokenCredential.GetToken(dctx, o)
}

func readAzureSharedKey(a *Azure) (string, error) {
	if a.SharedKeyFile != "" {
		// The errors leave out the path: a key pasted in its place would be logged.
		f, err := os.Open(a.SharedKeyFile) //nolint:gosec // the operator names the file
		if err != nil {
			var pe *fs.PathError
			if errors.As(err, &pe) {
				err = pe.Err
			}
			return "", fmt.Errorf("server: azure shared key file: %w", err)
		}
		defer func() { _ = f.Close() }()
		b, err := io.ReadAll(io.LimitReader(f, maxAzureKeyFile+1))
		if err != nil {
			var pe *fs.PathError
			if errors.As(err, &pe) {
				err = pe.Err
			}
			return "", fmt.Errorf("server: azure shared key file: %w", err)
		}
		if len(b) > maxAzureKeyFile {
			return "", fmt.Errorf("server: azure shared key file is larger than %d bytes", maxAzureKeyFile)
		}
		key := strings.TrimSpace(string(b))
		if key == "" {
			return "", errors.New("server: azure shared key file is empty")
		}
		return key, nil
	}
	key := strings.TrimSpace(os.Getenv(a.SharedKeyEnv))
	if key == "" {
		return "", fmt.Errorf("server: azure shared key: environment variable %s is empty or unset", a.SharedKeyEnv)
	}
	return key, nil
}

// Serve serves at most 1,024 connections at a time from ln until ctx is
// done. It runs an office loop for each configured database, and the
// readiness probe if one is configured. When ctx is done it reports itself
// not ready, stops accepting connections, waits up to a minute for running
// requests, then leaves office, releasing each lease, closes the store and
// returns. It returns nil after a clean shutdown, and an error if it had to
// close connections with requests still running.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	defer func() {
		if c, ok := s.store.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	defer s.startOffice()()
	sctx, stopSweep := context.WithCancel(context.Background())
	defer stopSweep()
	go s.sweep(sctx)
	if pln := s.probeLn; pln != nil || s.cfg.ProbeListen != "" {
		if pln == nil {
			var err error
			if pln, err = net.Listen("tcp", s.cfg.ProbeListen); err != nil {
				return fmt.Errorf("server: probe: %w", err)
			}
		}
		probe := &http.Server{Handler: http.HandlerFunc(s.readyz), ReadHeaderTimeout: s.headerDeadline, IdleTimeout: s.idleTimeout, ErrorLog: slog.NewLogLogger(s.log.Handler(), slog.LevelWarn)}
		go func() { _ = probe.Serve(netutil.LimitListener(pln, probeConns)) }()
		defer func() { _ = probe.Close() }()
	}
	var p http.Protocols
	p.SetHTTP1(true)
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: s.headerDeadline,
		IdleTimeout:       s.idleTimeout,
		MaxHeaderBytes:    int(s.limits.HeaderBlockBytes),
		// WriteByteTimeout closes an HTTP/2 connection whose client stops
		// reading: a frame write blocked in TCP ends no other way.
		HTTP2:     &http.HTTP2Config{MaxConcurrentStreams: maxStreams, WriteByteTimeout: s.responseDeadline},
		Protocols: &p,
		ErrorLog:  slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
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
	// The peer listener serves forwarded requests: only from a pod that
	// presents a peer certificate, and never forwarded again.
	var peerSrv *http.Server
	if s.peer != nil {
		pln := s.peerLn
		if pln == nil {
			var err error
			if pln, err = net.Listen("tcp", s.cfg.Peer.Listen); err != nil {
				return fmt.Errorf("server: peer: %w", err)
			}
		}
		peerSrv = &http.Server{
			Handler:   http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.serve(w, r, true) }),
			TLSConfig: s.peer.server.Clone(), ReadHeaderTimeout: s.headerDeadline, IdleTimeout: s.idleTimeout,
			MaxHeaderBytes: int(s.limits.HeaderBlockBytes), HTTP2: srv.HTTP2, ErrorLog: srv.ErrorLog,
		}
		go func() { _ = peerSrv.ServeTLS(netutil.LimitListener(pln, s.maxConns), "", "") }()
	}
	errc := make(chan error, 1)
	go func() { errc <- serve() }()
	select {
	case err := <-errc:
		if peerSrv != nil {
			_ = peerSrv.Close()
		}
		return err
	case <-ctx.Done():
	}
	s.stopping.Store(true)
	dctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	perr := make(chan error, 1)
	go func() {
		if peerSrv == nil {
			perr <- nil
			return
		}
		err := peerSrv.Shutdown(dctx)
		if err != nil {
			_ = peerSrv.Close()
		}
		perr <- err
	}()
	err := srv.Shutdown(dctx)
	if err != nil {
		_ = srv.Close()
		err = fmt.Errorf("server: requests still running after %v were cut: %w", s.shutdownTimeout, err)
	}
	if s.peer != nil {
		s.peer.transport.CloseIdleConnections()
	}
	if pe := <-perr; pe != nil && err == nil {
		err = fmt.Errorf("server: forwarded requests still running after %v were cut: %w", s.shutdownTimeout, pe)
	}
	if serr := <-errc; err == nil && !errors.Is(serr, http.ErrServerClosed) {
		err = serr
	}
	return err
}

// database is a configured database: its office loop, the leader that last
// met a tail bound it cannot compact past, which leaves this server not
// ready while it leads, and the channel table of its current leader, guarded
// by Server.chMu.
type database struct {
	loop  *office.Loop
	tail  atomic.Pointer[engine.Leader]
	chans *table
}

// leader returns the database's leader while this server serves it, or nil.
func (d *database) leader() *engine.Leader {
	if d == nil {
		return nil
	}
	l, _ := d.serving()
	return l
}

// serving reads the leader and follower from the same office state.
func (d *database) serving() (*engine.Leader, *engine.Follower) {
	state := d.loop.State()
	if state.Leader != nil && state.Leader.Err() != nil {
		return nil, nil
	}
	return state.Leader, state.Follower
}

// addDatabases creates an office loop for each configured database, on the
// server's store. The leaders' files go under a directory of the system's
// temporary directory that Lead creates and Serve removes; New creates
// nothing.
func (s *Server) addDatabases() error {
	s.dbs = map[string]*database{}
	if len(s.cfg.Databases) == 0 {
		return nil
	}
	pod, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("server: host name: %w", err)
	}
	holder, err := office.NewHolder(pod)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}
	s.dir = filepath.Join(os.TempDir(), "skylite-"+holder)
	for _, name := range s.cfg.Databases {
		keys, err := s.cfg.Storage.keys(name)
		var loop *office.Loop
		if err == nil {
			loop, err = office.New(office.Config{Store: s.store, Keys: keys, Holder: holder, Dir: s.dir, Engine: s.engine, FollowerReads: s.cfg.FollowerReads, Logger: s.log.With(slog.String("database", name))})
		}
		if err != nil {
			return fmt.Errorf("server: database %s: %w", name, err)
		}
		s.dbs[name] = &database{loop: loop}
	}
	return nil
}

// startOffice runs the office loops. The function it returns stops them,
// which releases their leases, and removes the leaders' directory.
func (s *Server) startOffice() (stop func()) {
	lctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for name, d := range s.dbs {
		wg.Go(func() { _ = d.loop.Run(lctx) })
		if s.peer != nil {
			wg.Go(func() { s.announce(lctx, name, d) })
		}
	}
	return func() {
		cancel()
		wg.Wait()
		if s.dir != "" {
			_ = os.RemoveAll(s.dir)
		}
	}
}

// ready reports whether this server serves any database, unless it is
// shutting down or a leader is at a tail bound it cannot compact past.
func (s *Server) ready() bool {
	if s.stopping.Load() {
		return false
	}
	serves := false
	for _, d := range s.dbs {
		l, f := d.serving()
		if l != nil && d.tail.Load() == l {
			return false
		}
		serves = serves || l != nil || f != nil && f.Fresh()
	}
	return serves
}

// readyz answers the readiness probe.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path != "/readyz" || r.Method != http.MethodGet:
		http.NotFound(w, r)
	case s.ready():
		_, _ = io.WriteString(w, "ready\n")
	default:
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}
}

// ServeHTTP answers one request with ADR 0005's headers and NDJSON frames,
// ending with one terminal frame.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.serve(w, r, false) }

// serve is ServeHTTP for a request from a client, or forwarded by another
// pod (hop), whose answer it relays.
func (s *Server) serve(w http.ResponseWriter, r *http.Request, hop bool) {
	arrived := time.Now()
	rc := http.NewResponseController(w)
	// The body read deadline runs from the end of the header block, and also
	// bounds a body a refusal leaves unread.
	_ = rc.SetReadDeadline(arrived.Add(s.bodyDeadline))
	id := rand.Text()
	if v := r.Header.Get(requestIDHeader); hop && validRequestID(v) {
		id = v // assigned by the pod that received the request (ADR 0005)
	}
	h := w.Header()
	h.Set("Skylite-Protocol", "1")
	h.Set("Skylite-Request-Id", id)
	h.Set("Content-Type", "application/x-ndjson")
	h.Set("Cache-Control", "no-store")
	log := s.log.With(slog.String("request_id", id))
	st := &stream{w: w, rc: rc}
	var b bytes.Buffer
	out, err := wire.NewWriter(st, wire.DefaultFrameBytes, s.limits)
	if err != nil { // only limits Validate refuses, which New's are not
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	st.out = out
	done, e, bodyRead := s.handle(st, r, id, log, hop, arrived)
	if st.relayed {
		return
	}
	status := http.StatusOK
	if e != nil {
		if e.RequestID == "" { // a replayed receipt keeps its request's own
			e.RequestID = id
		}
		status = e.Code.Status() // used only if the response has not started
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
	// A response that has not started is encoded before its status is sent,
	// so that a frame the codec refuses becomes an internal error rather
	// than a half answer. One that has started gets an internal error frame.
	started := st.started
	if !started {
		st.buf = &b
	}
	_ = rc.SetWriteDeadline(time.Now().Add(s.responseDeadline))
	if e != nil {
		err = out.Error(e)
	} else {
		err = out.Done(done)
	}
	if err != nil {
		log.LogAttrs(r.Context(), slog.LevelError, "response not encoded", slog.String("error", err.Error()))
		ie := wire.NewError(wire.CodeInternal, "the server could not encode its answer")
		ie.NotExecuted, ie.RequestID = false, id
		if !started {
			status = http.StatusInternalServerError
			b.Reset()
		}
		_ = out.Error(ie)
	}
	if !started {
		w.WriteHeader(status)
		_, _ = w.Write(b.Bytes())
	}
}

// stream is a response body. Frames written before the terminal one start
// the response with status 200; until then, and for the terminal frame of
// a response that has not started, they go to buf.
type stream struct {
	w        http.ResponseWriter
	rc       *http.ResponseController
	out      *wire.Writer
	buf      *bytes.Buffer
	started  bool
	relayed  bool      // forward relayed another pod's answer
	deadline time.Time // the write deadline from the start, if set
}

// start sends status 200, if not already sent, with the write deadline.
func (st *stream) start() {
	if st.started {
		return
	}
	if !st.deadline.IsZero() {
		_ = st.rc.SetWriteDeadline(st.deadline)
	}
	st.w.WriteHeader(http.StatusOK)
	st.started = true
}

func (st *stream) Write(p []byte) (int, error) {
	if st.buf != nil {
		return st.buf.Write(p)
	}
	st.start()
	return st.w.Write(p)
}

// handle runs the checks before acceptance of ADR 0005 in their order, then
// serves the request with request id id, or forwards it. Admission (check
// 2) is not implemented. bodyRead reports whether the body was read to its
// end. hop is set for a request another pod forwarded, which arrived then.
func (s *Server) handle(st *stream, r *http.Request, id string, log *slog.Logger, hop bool, arrived time.Time) (done *wire.Done, e *wire.Error, bodyRead bool) {
	kind, db, e := wire.Route(r.Method, r.URL.EscapedPath())
	if e != nil {
		return nil, e, false
	}
	if r.URL.RawQuery != "" {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the path must have no query"), false
	}
	// The pod-to-pod headers are trusted only on the peer listener.
	var budget int64
	if hop {
		var err error
		if budget, err = strconv.ParseInt(r.Header.Get(budgetHeader), 10, 64); err != nil || budget < 1 {
			return nil, wire.NewError(wire.CodeInvalidRequest, "a forwarded request needs Skylite-Budget-Ms, a positive integer"), false
		}
	} else if r.Header.Values(forwardedHeader) != nil || r.Header.Values(budgetHeader) != nil {
		return nil, wire.NewError(wire.CodeInvalidRequest, "Skylite-Forwarded and Skylite-Budget-Ms are sent only between pods"), false
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
	authorize := func(need auth.Role) *wire.Error {
		if s.guard.Authorize(p, db, need) {
			return nil
		}
		attrs := auth.RefusalAttrs(&auth.Error{Code: auth.CodeForbidden, Issuer: p.Issuer, Subject: p.Subject}, token)
		attrs = append(attrs, slog.String("database", db), slog.String("role", string(need)))
		log.LogAttrs(r.Context(), slog.LevelInfo, "authorisation refused", attrs...)
		return wire.NewError(wire.CodeForbidden, "the principal's role on this database name does not allow this request")
	}
	if e := authorize(need); e != nil {
		return nil, e, false
	}
	// Route before reading the body what this server can neither serve nor
	// forward. Only stateless reads can use a follower; a request another
	// pod forwarded is never forwarded again.
	d := s.dbs[db]
	var leader *engine.Leader
	var follower *engine.Follower
	if d != nil {
		leader, follower = d.serving()
	}
	forward := !hop && s.peer != nil && d != nil && kind != wire.KindPut && leader == nil
	stateless := kind == wire.KindQuery || kind == wire.KindDescribe
	switch {
	case kind == wire.KindStatus || kind == wire.KindResolve && !forward:
		return nil, wire.NewError(wire.CodeNotServing, "resolve and status are not served in this version"), false
	case kind != wire.KindPut && leader == nil && !forward && (follower == nil || !stateless):
		return nil, wire.NewError(wire.CodeNotServing, "this server does not lead the database or serve this request on a follower"), false
	}
	who := principal(p)
	release, e := s.chargeBody(r, who)
	if e != nil {
		return nil, e, false
	}
	defer release()
	limit := s.limits.RequestBodyBytes
	if kind == wire.KindClose || kind == wire.KindCancel || kind == wire.KindResolve {
		limit = controlBodyBytes
	}
	body, e := s.readBody(r, limit)
	if e != nil {
		return nil, e, false
	}
	req, e := wire.Parse(kind, body, int(s.limits.BatchEntries))
	if e != nil {
		return nil, e, true
	}
	if budget > 0 && budget < s.statementTime.Milliseconds() && (req.TimeoutMS == 0 || budget < req.TimeoutMS) {
		req.TimeoutMS = budget // the forwarding pod's budget only lowers the statement time
	}
	// Writes need the write role (ADR 0006): a write open, and every
	// request on a write channel but close and cancel, which only stop
	// work, checked again on each, since a token's claims can change.
	if kind == wire.KindChannel || kind == wire.KindOpen && req.Mode == wire.ModeWrite {
		if e := authorize(auth.RoleWrite); e != nil {
			return nil, e, true
		}
	}
	// Check 8 before the local lookup, on a leader too: a leader deposed
	// without noticing yet must not answer channel_lost for a channel of
	// the leader in office. A token naming another process goes there.
	if !hop && s.peer != nil && leader != nil && (kind == wire.KindChannel || kind == wire.KindCancel || kind == wire.KindClose) {
		inst, _, _ := strings.Cut(strings.TrimPrefix(req.Channel, channelTokenPrefix+"."), ".")
		forward = inst != s.instance
	}
	if forward && (!stateless || follower == nil) {
		return nil, s.forward(st, r, kind, db, req, body, id, arrived, log), true
	}
	switch kind {
	case wire.KindPut:
		done, e = s.create(r.Context(), db, p, log)
	case wire.KindOpen:
		done, e = s.openChannel(d, leader, req, who)
	case wire.KindChannel:
		done, e = s.channelRequest(r.Context(), st, d, leader, req, body, who, id, log)
	case wire.KindClose:
		done, e = s.closeChannel(d, leader, req, who)
	case wire.KindCancel:
		done, e = s.cancelRequest(d, leader, req, who, id)
	default:
		done, e = s.statement(r.Context(), st, d, leader, follower, req, who, log)
		// A follower that cannot serve the read before its answer starts
		// hands it to the leader.
		if forward && e != nil && !st.started && (e.Code == wire.CodeNotServing || e.Code == wire.CodeBehind) {
			return nil, s.forward(st, r, kind, db, req, body, id, arrived, log), true
		}
	}
	return done, e, true
}

// principal identifies an authenticated principal in the per-principal
// bounds: its issuer and subject.
func principal(p *auth.Principal) string { return p.Issuer + "\x00" + p.Subject }

// chargeBody charges a body's 8 × Content-Length to the body share of the
// memory budget before it is read (ADR 0005, checks 3 and 5), and returns
// the function that releases it. One principal, who, may hold a quarter of
// the share, as of the SQLite connections. A body over its limit is left
// to readBody.
func (s *Server) chargeBody(r *http.Request, who string) (release func(), e *wire.Error) {
	n := 8 * min(max(r.ContentLength, 0), s.limits.RequestBodyBytes)
	s.bodyMu.Lock()
	defer s.bodyMu.Unlock()
	if s.bodyUsed+n > s.bodyShare || s.bodyBy[who]+n > s.bodyShare/4 {
		return nil, wire.NewError(wire.CodeOverloaded, "the request bodies' share of the memory budget, or the principal's part of it, is full; retry later")
	}
	s.bodyUsed += n
	s.bodyBy[who] += n
	return func() {
		s.bodyMu.Lock()
		s.bodyUsed -= n
		if s.bodyBy[who] -= n; s.bodyBy[who] == 0 {
			delete(s.bodyBy, who)
		}
		s.bodyMu.Unlock()
	}, nil
}

// readBody applies ADR 0005's body checks to a PUT or POST, whose body may
// have limit bytes, and reads the complete body within the body read
// deadline.
func (s *Server) readBody(r *http.Request, limit int64) ([]byte, *wire.Error) {
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
	case r.ContentLength > limit:
		return nil, wire.NewError(wire.CodeRequestTooLarge, fmt.Sprintf("the body exceeds %d bytes", limit))
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
