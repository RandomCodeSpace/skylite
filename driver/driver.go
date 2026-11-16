// Package skylite is the Go database/sql driver for skylite, speaking the
// client protocol of ADR 0005 (docs/content/docs/decisions/0005-client-protocol.md)
// over HTTPS, with bearer tokens as ADR 0006 requires.
//
// This version serves reads only: statements outside a transaction that
// the server's stateless query endpoint accepts as pure reads. A statement
// that needs a write channel fails with ErrWritesUnsupported, and BeginTx
// fails with ErrTransactionsUnsupported. Nothing is retried.
//
// Open a database with a Connector, which carries the token source:
//
//	c, err := skylite.NewConnector(skylite.Config{...})
//	if err != nil { ... }
//	db := sql.OpenDB(c)
//
// db.Close closes the Connector, which releases the idle connections of
// the driver's own HTTP client.
//
// The driver is also registered as "skylite", but a DSN cannot carry a
// token source, so sql.Open("skylite", dsn) fails on first use.
package skylite

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

func init() {
	sql.Register("skylite", Driver{})
}

// TokenSource supplies the bearer token, a JWT, of each request. The driver
// calls it before every request and never logs, stores or echoes the token.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// TokenFunc adapts a function to a TokenSource.
type TokenFunc func(ctx context.Context) (string, error)

// Token calls f.
func (f TokenFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

// Config configures a Connector.
type Config struct {
	// Endpoint is the server's base URL: https://host[:port][/path]. It may
	// not carry user information, a query, a fragment, an empty host, or a
	// "." or ".." path segment.
	Endpoint string
	// Database is the database name: 1 to 63 characters from a-z, 0-9 and
	// -, starting with a letter or digit.
	Database string
	// Token supplies the bearer token of each request. Required.
	Token TokenSource
	// TLSConfig is the TLS configuration of the driver's own HTTP client;
	// nil means the system roots. It cannot be combined with HTTPClient.
	TLSConfig *tls.Config
	// AllowPlaintext permits an http:// Endpoint. Without it only https://
	// is accepted.
	AllowPlaintext bool
	// StatementTimeout, when set, lowers the server's statement time limit
	// for each query: from 1 ms to 2^31-1 ms. Zero leaves the server's
	// limit. The caller's context bounds the request on the client side.
	StatementTimeout time.Duration
	// ResponseHeaderTimeout bounds the wait for a response's headers on the
	// driver's own HTTP client; zero means 60 s, above the server's default
	// statement time limit of 30 s. It cannot be combined with HTTPClient.
	ResponseHeaderTimeout time.Duration
	// HTTPClient replaces the driver's HTTP client. The driver uses a copy
	// whose redirect policy refuses every redirect, and never closes its
	// connections.
	HTTPClient *http.Client
}

// defaultResponseHeaderTimeout is Config.ResponseHeaderTimeout's default.
const defaultResponseHeaderTimeout = 60 * time.Second

// maxFrame is the largest frame the driver reads: a row can be as large as
// the result limit, plus its newline.
var maxFrame = int(wire.DefaultLimits().ResultBytes) + 1

// errRedirect refuses every redirect, as ADR 0005 decision 7 requires: a
// request and its token go only to the configured endpoint.
var errRedirect = errors.New("skylite: the server answered with a redirect, which the driver never follows")

func refuseRedirect(*http.Request, []*http.Request) error { return errRedirect }

// Connector opens connections to one database. It holds the read-your-writes
// watermark of ADR 0005 decision 5, shared by every connection of the
// sql.DB that uses it.
type Connector struct {
	query, describe string // request URLs
	token           TokenSource
	client          *http.Client
	timeoutMS       int64
	own             bool         // client is the driver's own, not Config.HTTPClient
	watermark       atomic.Int64 // the highest txn seen
}

// NewConnector validates cfg and returns a Connector. It sends nothing.
// Its errors never hold the endpoint or anything else from cfg.
func NewConnector(cfg Config) (*Connector, error) {
	u, err := url.Parse(cfg.Endpoint)
	switch {
	case err != nil || u.Hostname() == "" || u.Opaque != "":
		return nil, errors.New("skylite: Endpoint must be an absolute URL with a host")
	case u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(cfg.Endpoint, "#"):
		return nil, errors.New("skylite: Endpoint may not carry user information, a query or a fragment")
	case u.Scheme == "http" && !cfg.AllowPlaintext:
		return nil, errors.New("skylite: Endpoint is plain http; set AllowPlaintext to permit it")
	case u.Scheme != "https" && u.Scheme != "http":
		return nil, errors.New("skylite: Endpoint must be https:// (or http:// with AllowPlaintext)")
	case !wire.ValidDatabaseName(cfg.Database):
		return nil, errors.New("skylite: Database must be 1 to 63 characters from a-z, 0-9 and -, starting with a letter or digit")
	case cfg.Token == nil:
		return nil, errors.New("skylite: Token is required")
	case (cfg.TLSConfig != nil || cfg.ResponseHeaderTimeout != 0) && cfg.HTTPClient != nil:
		return nil, errors.New("skylite: TLSConfig and ResponseHeaderTimeout configure the driver's own HTTP client; set them or HTTPClient, not both")
	case cfg.ResponseHeaderTimeout < 0:
		return nil, errors.New("skylite: ResponseHeaderTimeout is negative")
	case cfg.StatementTimeout != 0 && (cfg.StatementTimeout < time.Millisecond || cfg.StatementTimeout > math.MaxInt32*time.Millisecond):
		return nil, errors.New("skylite: StatementTimeout must be 0 or from 1 ms to 2^31-1 ms")
	}
	for seg := range strings.SplitSeq(u.EscapedPath(), "/") {
		if s, _ := url.PathUnescape(seg); s == "." || s == ".." {
			return nil, errors.New(`skylite: Endpoint's path may not have a "." or ".." segment`)
		}
	}
	var client http.Client
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
	} else {
		t, ok := http.DefaultTransport.(*http.Transport)
		if ok {
			t = t.Clone() // keeps ForceAttemptHTTP2
		} else { // replaced, for example by instrumentation
			t = &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true}
		}
		t.TLSClientConfig = cfg.TLSConfig.Clone()
		t.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
		if t.ResponseHeaderTimeout == 0 {
			t.ResponseHeaderTimeout = defaultResponseHeaderTimeout
		}
		client.Transport = t
	}
	client.CheckRedirect = refuseRedirect
	base := strings.TrimRight(u.Scheme+"://"+u.Host+u.EscapedPath(), "/")
	return &Connector{
		query:     base + wire.Path(wire.KindQuery, cfg.Database),
		describe:  base + wire.Path(wire.KindDescribe, cfg.Database),
		token:     cfg.Token,
		client:    &client,
		timeoutMS: cfg.StatementTimeout.Milliseconds(),
		own:       cfg.HTTPClient == nil,
	}, nil
}

// Close closes the idle connections of the driver's own HTTP client, never
// those of a Config.HTTPClient. sql.DB.Close calls it.
func (c *Connector) Close() error {
	if c.own {
		c.client.CloseIdleConnections()
	}
	return nil
}

// Connect returns a connection. It sends nothing: every request is a
// separate HTTP request on the Connector's client.
func (c *Connector) Connect(context.Context) (driver.Conn, error) { return &conn{c: c}, nil }

// Driver returns Driver{}.
func (c *Connector) Driver() driver.Driver { return Driver{} }

// raise raises the watermark to txn, never lowering it.
func (c *Connector) raise(txn int64) {
	for {
		w := c.watermark.Load()
		if txn <= w || c.watermark.CompareAndSwap(w, txn) {
			return
		}
	}
}

// Driver is the driver registered as "skylite". Its Open always fails,
// because a DSN cannot carry a token source; use NewConnector.
type Driver struct{}

// ErrNoDSN is Driver.Open's error.
var ErrNoDSN = errors.New("skylite: a DSN cannot carry a token source; use NewConnector and sql.OpenDB")

// Open returns ErrNoDSN. It never echoes the DSN.
func (Driver) Open(string) (driver.Conn, error) { return nil, ErrNoDSN }

// ErrWritesUnsupported is matched, with errors.Is, by the error of a
// statement the server refused with needs_channel: one that is not a pure
// read. This driver version has no write channels. It wraps
// errors.ErrUnsupported.
var ErrWritesUnsupported = fmt.Errorf("skylite: writes are not supported in this driver version: %w", errors.ErrUnsupported)

// ErrTransactionsUnsupported is BeginTx's error: this driver version has no
// channels, so no transactions. It wraps errors.ErrUnsupported.
var ErrTransactionsUnsupported = fmt.Errorf("skylite: transactions are not supported in this driver version: %w", errors.ErrUnsupported)

// Code is an ADR 0005 error code. The list is closed in protocol version 1;
// an Error may still carry a code outside it from a newer server.
type Code = wire.Code

// The error codes of ADR 0005.
const (
	CodeInvalidRequest       = wire.CodeInvalidRequest
	CodeUnauthenticated      = wire.CodeUnauthenticated
	CodeForbidden            = wire.CodeForbidden
	CodeStatementForbidden   = wire.CodeStatementForbidden
	CodeDatabaseNotFound     = wire.CodeDatabaseNotFound
	CodeDatabaseExists       = wire.CodeDatabaseExists
	CodeNeedsChannel         = wire.CodeNeedsChannel
	CodeChannelBusy          = wire.CodeChannelBusy
	CodeInFlight             = wire.CodeInFlight
	CodeStaleSequence        = wire.CodeStaleSequence
	CodeSequenceGap          = wire.CodeSequenceGap
	CodeSequenceConflict     = wire.CodeSequenceConflict
	CodeChannelLost          = wire.CodeChannelLost
	CodeRequestTooLarge      = wire.CodeRequestTooLarge
	CodeTooManyChannels      = wire.CodeTooManyChannels
	CodeOverloaded           = wire.CodeOverloaded
	CodeInvalidState         = wire.CodeInvalidState
	CodeTransactionAborted   = wire.CodeTransactionAborted
	CodeBusy                 = wire.CodeBusy
	CodeTimeout              = wire.CodeTimeout
	CodeCancelled            = wire.CodeCancelled
	CodeResultTooLarge       = wire.CodeResultTooLarge
	CodeSQLError             = wire.CodeSQLError
	CodeCommitFailed         = wire.CodeCommitFailed
	CodeCommitOutcomeUnknown = wire.CodeCommitOutcomeUnknown
	CodeNoLeader             = wire.CodeNoLeader
	CodeNotServing           = wire.CodeNotServing
	CodeOwnerUnreachable     = wire.CodeOwnerUnreachable
	CodeBehind               = wire.CodeBehind
	CodeMaintenance          = wire.CodeMaintenance
	CodeInternal             = wire.CodeInternal
)

// Error is an error the server answered: ADR 0005's error object. Use
// errors.As to read it. Zero fields were absent.
type Error struct {
	Code               Code
	NotExecuted        bool // this delivery had no effect
	Final              bool // this answer is the request's settled outcome
	Message            string
	RequestID          string
	SQLiteCode         int64
	SQLiteExtendedCode int64
	Txn                int64
	Epoch              int64
}

func (e *Error) Error() string {
	return fmt.Sprintf("skylite: %s: %s (request %s)", e.Code, e.Message, e.RequestID)
}

// serverError converts an error frame. needs_channel also matches
// ErrWritesUnsupported.
func serverError(we *wire.Error) error {
	e := &Error{
		Code: we.Code, NotExecuted: we.NotExecuted, Final: we.Final, Message: we.Message, RequestID: we.RequestID,
		SQLiteCode: we.SQLiteCode, SQLiteExtendedCode: we.SQLiteExtendedCode, Txn: we.Txn, Epoch: we.Epoch,
	}
	if e.Code == CodeNeedsChannel {
		return fmt.Errorf("%w: %w", ErrWritesUnsupported, e)
	}
	return e
}
