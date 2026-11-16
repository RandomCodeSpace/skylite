package skylite

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"io"
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
// checks the bearer token, parses the body as the real server does, and
// lets each test answer with a wire.Writer.
type fake struct {
	srv    *httptest.Server
	hits   atomic.Int32
	mu     sync.Mutex
	calls  []call
	answer func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request)
}

func newFake(t *testing.T, tlsOn bool, answer func(w http.ResponseWriter, ww *wire.Writer, c call, r *http.Request)) *fake {
	t.Helper()
	f := &fake{answer: answer}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
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
		e.RequestID = "req-1"
	}
	w.WriteHeader(e.Code.Status())
	_ = ww.Error(e)
}

func (f *fake) serve(w http.ResponseWriter, r *http.Request) {
	f.hits.Add(1)
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Skylite-Protocol", "1")
	w.Header().Set("Content-Type", "application/x-ndjson")
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
// describe with cols, unnamed parameters and txn.
func answerRows(cols []wire.Column, rows [][]any, params int, txn int64) func(http.ResponseWriter, *wire.Writer, call, *http.Request) {
	return func(_ http.ResponseWriter, ww *wire.Writer, c call, _ *http.Request) {
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
