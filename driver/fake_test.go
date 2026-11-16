package skylite

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

const testToken = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.c2VjcmV0LXNpZ25hdHVyZQ" //nolint:gosec // the fake server's token, not a credential

// call is one request the fake server received.
type call struct {
	kind  wire.Kind
	body  string
	req   *wire.Request
	proto int // HTTP major version
}

// fake is an in-process skylite server built from internal/wire: it routes,
// checks the bearer token and the request headers, parses the body as the
// real server does, sends the response headers ADR 0005 requires, and lets
// each test answer with a wire.Writer.
type fake struct {
	srv    *httptest.Server
	hits   atomic.Int32
	conns  atomic.Int32 // connections accepted
	closed atomic.Int32 // connections closed
	mu     sync.Mutex
	calls  []call
	answer func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request)
}

func newFake(t *testing.T, tlsOn bool, answer func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request)) *fake {
	t.Helper()
	f := &fake{answer: answer}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	f.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			f.conns.Add(1)
		case http.StateClosed, http.StateHijacked:
			f.closed.Add(1)
		}
	}
	if tlsOn {
		f.srv.EnableHTTP2 = true
		f.srv.StartTLS()
	} else {
		f.srv.Start()
	}
	t.Cleanup(f.srv.Close)
	return f
}

func refuse(w http.ResponseWriter, ww *wire.Writer, e *wire.Error) {
	if e.RequestID == "" {
		e.RequestID = w.Header().Get("Skylite-Request-Id")
	}
	if e.Code == wire.CodeUnauthenticated {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.WriteHeader(e.Code.Status())
	_ = ww.Error(e)
}

func (f *fake) serve(w http.ResponseWriter, r *http.Request) {
	n := f.hits.Add(1)
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Skylite-Protocol", "1")
	w.Header().Set("Skylite-Request-Id", fmt.Sprintf("req-%d", n))
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	ww, _ := wire.NewWriter(w, wire.DefaultFrameBytes, wire.DefaultLimits())
	kind, db, rerr := wire.Route(r.Method, r.URL.Path)
	if rerr != nil {
		refuse(w, ww, rerr)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+testToken {
		refuse(w, ww, wire.NewError(wire.CodeUnauthenticated, "the bearer token is missing or invalid"))
		return
	}
	if db != "library" {
		refuse(w, ww, wire.NewError(wire.CodeForbidden, "no role"))
		return
	}
	switch {
	case r.Header.Get("Content-Type") != "application/json":
		refuse(w, ww, wire.NewError(wire.CodeInvalidRequest, "Content-Type must be application/json"))
		return
	case len(r.TransferEncoding) > 0 || r.Header.Get("Transfer-Encoding") != "":
		refuse(w, ww, wire.NewError(wire.CodeInvalidRequest, "Transfer-Encoding is not allowed; send Content-Length"))
		return
	case len(r.Header.Values("Content-Length")) != 1 || r.ContentLength != int64(len(body)):
		refuse(w, ww, wire.NewError(wire.CodeInvalidRequest, "Content-Length is required and must match the body"))
		return
	}
	req, perr := wire.Parse(kind, body, 10000)
	if perr != nil {
		refuse(w, ww, perr)
		return
	}
	c := call{kind: kind, body: string(body), req: req, proto: r.ProtoMajor}
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
	f.answer(w, ww, c, r)
}

func (f *fake) lastCalls() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

// config returns a Config for the fake, trusting its certificate.
func (f *fake) config() Config {
	cfg := Config{
		Endpoint: f.srv.URL,
		Database: "library",
		Token:    TokenFunc(func(context.Context) (string, error) { return testToken, nil }),
	}
	if f.srv.TLS != nil {
		pool := x509.NewCertPool()
		pool.AddCert(f.srv.Certificate())
		cfg.TLSConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	} else {
		cfg.AllowPlaintext = true
	}
	return cfg
}

func openDB(t *testing.T, cfg Config) *sql.DB {
	t.Helper()
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// queryDone is a query's terminal frame at txn.
func queryDone(txn int64) *wire.Done {
	return &wire.Done{Kind: wire.DoneQuery, Txn: txn, ReadOnly: true}
}

// answerRows answers a query with cols, rows and a done frame at txn, and a
// describe with cols, unnamed parameters and txn; each answer starts with a
// keepalive, which the driver must skip.
func answerRows(cols []wire.Column, rows [][]any, params int, txn int64) func(http.ResponseWriter, *wire.Writer, call, *http.Request) {
	return func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
		_ = ww.Keepalive()
		_ = ww.Columns(cols)
		if c.kind == wire.KindDescribe {
			_ = ww.Params(make([]string, params))
			_ = ww.Done(&wire.Done{Kind: wire.DoneDescribe, Txn: txn, ParamCount: int64(params)})
			return
		}
		for _, r := range rows {
			_ = ww.Row(r)
		}
		_ = ww.Done(queryDone(txn))
	}
}
