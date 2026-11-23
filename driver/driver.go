// Package skylite is the Go database/sql driver for skylite, speaking the
// client protocol of ADR 0005 (docs/content/docs/decisions/0005-client-protocol.md)
// over HTTPS, with bearer tokens as ADR 0006 requires.
//
// A query outside a transaction goes to the server's stateless query
// endpoint; one the server refuses there as not a pure read, every Exec,
// and every statement of a transaction go on the connection's write
// channel, or for a read-only transaction its read-only channel, which the
// driver opens on first use. A request on a channel whose response is lost
// is sent again with the same sequence number and the same body, which the
// server answers from its record: that is the protocol's replay. A request
// the server refused before it ran, for a reason that passes (busy,
// overloaded, no leader and the like), is sent again after a jittered
// pause. An autocommit write or a commit whose channel was lost is settled
// with resolve. An outcome the driver cannot learn is an error matching
// ErrOutcomeUnknown, never a success, and the connection is then
// discarded.
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
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
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
var errRedirect = errors.New("the server answered with a redirect, which the driver never follows")

func refuseRedirect(*http.Request, []*http.Request) error { return errRedirect }

// Connector opens connections to one database. It holds the read-your-writes
// watermark of ADR 0005 decision 5, shared by every connection of the
// sql.DB that uses it.
type Connector struct {
	base, db  string // the endpoint, without a trailing /, and the database
	token     TokenSource
	client    *http.Client
	timeoutMS int64
	own       bool          // client is the driver's own, not Config.HTTPClient
	watermark atomic.Int64  // the highest txn seen
	abandons  chan struct{} // abandonLater's slots
	closing   sync.Mutex    // serialises Close's wait for every slot
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
			// net/http's DefaultTransport settings, so idle connections close.
			t = &http.Transport{
				Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true,
				DialContext:  (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second,
			}
		}
		t.TLSClientConfig = cfg.TLSConfig.Clone()
		t.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
		if t.ResponseHeaderTimeout == 0 {
			t.ResponseHeaderTimeout = defaultResponseHeaderTimeout
		}
		client.Transport = t
	}
	client.CheckRedirect = refuseRedirect
	return &Connector{
		abandons:  make(chan struct{}, abandonLimit),
		base:      strings.TrimRight(u.Scheme+"://"+u.Host+u.EscapedPath(), "/"),
		db:        cfg.Database,
		token:     cfg.Token,
		client:    &client,
		timeoutMS: cfg.StatementTimeout.Milliseconds(),
		own:       cfg.HTTPClient == nil,
	}, nil
}

// Close waits for the cancels and closes of abandoned channels still
// running, each bounded by abandonWait, so that the server releases what
// those channels hold at once, and closes the idle connections of the
// driver's own HTTP client, never those of a Config.HTTPClient. sql.DB.Close
// calls it. An HTTP/2 connection a query still uses then closes after its
// idle timeout, 90 seconds by default. An abandon that starts while Close
// waits sends nothing. Close must not be called from the token source: an
// abandon waiting on it holds a slot, so Close would never return.
func (c *Connector) Close() error {
	c.closing.Lock()
	for range abandonLimit { // holding every slot, no abandon runs
		c.abandons <- struct{}{}
	}
	for range abandonLimit {
		<-c.abandons
	}
	c.closing.Unlock()
	if c.own {
		c.client.CloseIdleConnections()
	}
	return nil
}

// Connect returns a connection. It sends nothing: every request is a
// separate HTTP request on the Connector's client, and the connection's
// write channel is opened by its first request that needs one.
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

// ErrOutcomeUnknown is matched, with errors.Is, by the *Error of a request
// on a write channel whose outcome the driver could not learn, resolve
// included: it may or may not have taken effect, a commit included. Do not
// run it again without checking. The error also matches the context's error when the
// caller's context ended, and ErrTransactionLost when the request was a
// statement inside a transaction. It never matches driver.ErrBadConn.
var ErrOutcomeUnknown = errors.New("skylite: the outcome of the request is unknown")

// ErrTransactionLost is matched by the *Error of every statement and the
// commit of a transaction whose channel was lost or abandoned: nothing is
// sent for them, the transaction never commits, and its Rollback succeeds.
// It is also matched by the error of a commit whose channel was lost and
// that resolve proved did not commit; that error's NotExecuted is true.
var ErrTransactionLost = errors.New("skylite: the transaction was lost with its channel and will not commit")

// ErrResultUnavailable is matched by the *Error of a request whose
// response was lost and whose rows the server could not keep for its
// replay, or of an autocommit write whose channel was lost and that resolve
// found committed, at Txn. Committed reports whether it was an autocommit
// write, which committed; inside a transaction, the statement's effects are
// in the transaction, which stays open.
var ErrResultUnavailable = errors.New("skylite: the request ran but its result is unavailable")

// ErrReadOnlyUnsupported wraps errors.ErrUnsupported.
//
// Deprecated: BeginTx serves read-only transactions on a read-only channel
// and no longer returns it.
var ErrReadOnlyUnsupported = fmt.Errorf("skylite: read-only transactions are not supported by this server version: %w", errors.ErrUnsupported)

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

// Error is an error the server answered, ADR 0005's error object, or the
// driver's error for an outcome it could not learn (ErrOutcomeUnknown),
// a lost transaction (ErrTransactionLost) or a lost result
// (ErrResultUnavailable), which errors.Is matches. Use errors.As to read
// it. Zero fields were absent; Code is empty when no server answer gave one.
type Error struct {
	Code               Code
	NotExecuted        bool // this delivery had no effect
	Final              bool // this answer is the request's settled outcome
	Message            string
	RequestID          string
	ChannelID          string // the write channel's identifier, not its token
	Seq                int64  // the request's sequence number on the channel
	SQLiteCode         int64
	SQLiteExtendedCode int64
	Txn                int64
	Epoch              int64
	Cause              string // "maintenance" on a transaction_aborted that maintenance caused

	committed bool
	is        []error // the sentinels and the context's error it matches
}

func (e *Error) Error() string {
	s := "skylite: "
	if e.Code != "" {
		s += string(e.Code) + ": "
	}
	s += e.Message
	if e.RequestID != "" {
		s += " (request " + e.RequestID + ")"
	}
	if e.ChannelID != "" {
		s += fmt.Sprintf(" (channel %s, seq %d)", e.ChannelID, e.Seq)
	}
	return s
}

// Unwrap returns the sentinels and the context's error that e matches.
func (e *Error) Unwrap() []error { return e.is }

// Committed reports, for an error matching ErrResultUnavailable, whether
// the request committed.
func (e *Error) Committed() bool { return e.committed }

// Retryable reports whether the transaction can be run again from its
// start: maintenance ended it, and nothing of it committed (ADR 0005
// decision 9).
func (e *Error) Retryable() bool { return e.Code == CodeTransactionAborted && e.Cause == "maintenance" }

// serverError converts an error frame.
func serverError(we *wire.Error) *Error {
	return &Error{
		Code: we.Code, NotExecuted: we.NotExecuted, Final: we.Final, Message: we.Message, RequestID: we.RequestID,
		ChannelID: we.ChannelID, Seq: we.Seq, SQLiteCode: we.SQLiteCode, SQLiteExtendedCode: we.SQLiteExtendedCode,
		Txn: we.Txn, Epoch: we.Epoch, Cause: we.Cause,
	}
}
