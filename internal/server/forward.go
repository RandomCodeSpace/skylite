package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// Forwarding (ADR 0005 decision 7): a request this server cannot serve goes,
// one hop and once, to the pod that can, over the peer listener's mutual
// TLS. The leader in office publishes a leader record in the store; a write
// channel lives on its leader, so the record also locates a channel's owner.

// The pod-to-pod headers of ADR 0005 decision 7.
const (
	forwardedHeader = "Skylite-Forwarded"
	budgetHeader    = "Skylite-Budget-Ms"
	requestIDHeader = "Skylite-Request-Id"
)

const (
	// hopSlack is how long past a forwarded request's budget the forwarding
	// pod waits for the answer, besides the response deadline: the engine's
	// bound R on a commit's outcome, 20 s at its default (ADR 0005 decision 7).
	hopSlack = 20 * time.Second
	// maxLeaderRecord bounds a leader record's read.
	maxLeaderRecord = 4096
	// publishRetry is the wait before a failed leader record write is tried again.
	publishRetry = time.Second
)

// leaderRecord says which server process leads a database and where its
// peer listener is. A withdrawn record keeps its office and loses its
// address.
type leaderRecord struct {
	Instance string `json:"instance"` // the process's identifier, which its channel tokens name
	Address  string `json:"address"`  // host:port of its peer listener; empty once withdrawn
	DB       string `json:"db"`       // the database identifier, in hexadecimal
	Epoch    uint64 `json:"epoch"`
	Head     uint64 `json:"head"` // the transaction number of the office's epoch bump
}

var errBadRecord = errors.New("server: the leader record is not valid")

// peer is this server's side of pod-to-pod traffic.
type peer struct {
	address   string      // where other pods reach this one
	server    *tls.Config // the peer listener's, requiring a peer certificate
	transport *http.Transport
}

func newPeer(c *Peer) (*peer, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("server: peer certificate: %w", err)
	}
	b, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("server: peer ca_file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, errors.New("server: peer ca_file holds no PEM certificate")
	}
	addr := os.Getenv(c.AddressEnv)
	if h, _, err := net.SplitHostPort(addr); err != nil || h == "" {
		return nil, fmt.Errorf("server: peer address: environment variable %s does not hold a host:port", c.AddressEnv)
	}
	return &peer{
		address: addr,
		server:  &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13},
		// No proxy: a forwarded request carries the caller's bearer token,
		// and goes only to a pod that proves it holds a peer certificate.
		transport: &http.Transport{
			TLSClientConfig:   &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: c.ServerName, MinVersion: tls.VersionTLS13},
			ForceAttemptHTTP2: true,
			DialContext:       (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			IdleConnTimeout:   idleTimeout,
		},
	}, nil
}

// readRecord reads the leader record at key and its version. A record that
// does not decode is errBadRecord, with its version.
func (s *Server) readRecord(ctx context.Context, key string) (leaderRecord, string, error) {
	rc, info, err := s.store.Get(ctx, key)
	if err != nil {
		return leaderRecord{}, "", err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, maxLeaderRecord+1))
	if err != nil {
		return leaderRecord{}, "", err
	}
	var rec leaderRecord
	if len(b) > maxLeaderRecord || json.Unmarshal(b, &rec) != nil {
		return leaderRecord{}, info.Version, errBadRecord
	}
	return rec, info.Version, nil
}

// announce keeps database name's leader record in step with d's office
// loop until ctx ends: it publishes the record once the loop reports this
// server leading, and withdraws it when the loop stops reporting it or ctx
// ends.
func (s *Server) announce(ctx context.Context, name string, d *database) {
	keys, err := s.cfg.Storage.keys(name)
	if err != nil {
		return // Validate checked every database's keys
	}
	key := keys.Leader()
	log := s.log.With(slog.String("database", name))
	var mine leaderRecord
	var version string // mine's version while it is in the store
	var handled uint64 // the epoch published, or left to a later office's record
	for {
		st := d.loop.State()
		var retry <-chan time.Time
		switch {
		case st.Leader != nil && handled != st.Office.Epoch:
			rec := leaderRecord{Instance: s.instance, Address: s.peer.address, DB: hex.EncodeToString(st.Office.DB[:]), Epoch: st.Office.Epoch, Head: st.Office.Head}
			v, err := s.publish(ctx, key, rec)
			if err != nil {
				log.LogAttrs(ctx, slog.LevelWarn, "leader record not published", slog.String("error", err.Error()))
				retry = time.After(publishRetry)
				break
			}
			handled, mine, version = rec.Epoch, rec, v
		case st.Leader == nil:
			handled = 0
			if version != "" {
				s.withdraw(key, mine, version, log)
				version = ""
			}
		}
		select {
		case <-ctx.Done():
			if version != "" {
				s.withdraw(key, mine, version, log)
			}
			return
		case <-st.Changed:
		case <-retry:
		}
	}
}

// publish writes rec at key unless the record there is of a later office.
// It returns the version written, or "" if it left the record alone.
func (s *Server) publish(ctx context.Context, key string, rec leaderRecord) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.createTimeout)
	defer cancel()
	b, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	cur, version, err := s.readRecord(ctx, key)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return s.store.Create(ctx, key, bytes.NewReader(b))
	case err != nil && !errors.Is(err, errBadRecord):
		return "", err
	case err == nil && cur.Epoch > rec.Epoch:
		return "", nil
	}
	return s.store.Replace(ctx, key, version, bytes.NewReader(b))
}

// withdraw removes the address from the record rec this server published
// at version, unless another server has written the record since.
func (s *Server) withdraw(key string, rec leaderRecord, version string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), s.createTimeout)
	defer cancel()
	rec.Address = ""
	b, err := json.Marshal(rec)
	if err == nil {
		_, err = s.store.Replace(ctx, key, version, bytes.NewReader(b))
	}
	if err != nil && !errors.Is(err, storage.ErrPreconditionFailed) {
		log.LogAttrs(ctx, slog.LevelWarn, "leader record not withdrawn", slog.String("error", err.Error()))
	}
}

// forward sends the request, of kind on database db with its parsed body
// req and exact body (both nil for a GET), one hop to the pod that serves
// it, and relays that pod's answer. It returns nil once it has relayed an
// answer, even one cut short, and otherwise the refusal: no_leader or
// owner_unreachable, not executed only when nothing reached the other pod,
// or channel_lost when the records show the token's channel has ended. It
// never sends a request twice.
func (s *Server) forward(st *stream, r *http.Request, kind wire.Kind, db string, req *wire.Request, body []byte, id string, arrived time.Time, log *slog.Logger) *wire.Error {
	owner := kind == wire.KindChannel || kind == wire.KindCancel || kind == wire.KindClose
	unreachable := func(why string) *wire.Error {
		if owner {
			return wire.NewError(wire.CodeOwnerUnreachable, "the pod that owns the channel could not be reached: "+why)
		}
		return wire.NewError(wire.CodeNoLeader, "the database's leader could not be reached: "+why)
	}
	keys, err := s.cfg.Storage.keys(db)
	var rec leaderRecord
	if err == nil {
		ctx, cancel := context.WithTimeout(r.Context(), s.createTimeout)
		rec, _, err = s.readRecord(ctx, keys.Leader())
		cancel()
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		log.LogAttrs(r.Context(), slog.LevelWarn, "leader record not read", slog.String("error", err.Error()))
	}
	addr := rec.Address
	if owner {
		// The token is w1.<instance>.<database id>.<open head>.<secret>.
		parts := strings.Split(req.Channel, ".")
		if len(parts) != 5 || parts[0] != channelTokenPrefix {
			return wire.NewError(wire.CodeInvalidRequest, "the token does not name this database")
		}
		head, herr := strconv.ParseUint(parts[3], 10, 64)
		switch {
		case parts[1] == s.instance:
			// This process holds no office, so its write channels ended
			// when it left office.
			return wire.NewError(wire.CodeChannelLost, "this server no longer leads the database, and its channels have ended")
		case parts[1] != rec.Instance:
			// A channel opened before a later office's epoch bump ended
			// with its own office.
			if parts[2] == rec.DB && herr == nil && head < rec.Head {
				return wire.NewError(wire.CodeChannelLost, "a later leader took office after the channel opened, so the channel has ended")
			}
			addr = ""
		}
	}
	if addr == "" {
		return unreachable("no leader is recorded")
	}

	// The budget: what is left of the statement time, which the owner's
	// own limits can only lower.
	limit := s.statementTime
	if req != nil && req.TimeoutMS > 0 {
		limit = min(limit, time.Duration(req.TimeoutMS)*time.Millisecond)
	}
	remaining := max(limit-time.Since(arrived), time.Millisecond)
	deadline := time.Now().Add(remaining + hopSlack + s.responseDeadline)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	// Once the headers may have gone, the other pod may have accepted it.
	var sent atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }})
	var hopErr error
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "https", Host: addr})
			pr.Out.Body, pr.Out.ContentLength, pr.Out.GetBody = nil, 0, nil // no GetBody: the transport never resends
			if body != nil {
				pr.Out.Body, pr.Out.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			}
			pr.Out.Header.Set(forwardedHeader, "1")
			pr.Out.Header.Set(budgetHeader, strconv.FormatInt(max(remaining.Milliseconds(), 1), 10))
			pr.Out.Header.Set(requestIDHeader, id)
		},
		Transport:     s.peer.transport,
		FlushInterval: -1,
		ModifyResponse: func(res *http.Response) error {
			if res.Header.Get("Skylite-Protocol") != "1" {
				return errors.New("the answer has no Skylite-Protocol marker")
			}
			return nil
		},
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) { hopErr = err },
		ErrorLog:     slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
	_ = st.rc.SetWriteDeadline(deadline.Add(s.responseDeadline))
	// The relayed answer carries the other pod's headers, which hold the
	// same request id; a refusal here keeps this server's.
	h := st.w.Header()
	own := h.Clone()
	clear(h)
	// A failure after the relay has started panics with
	// http.ErrAbortHandler, which aborts the response: no terminal frame.
	rp.ServeHTTP(st.w, r.WithContext(ctx))
	if hopErr == nil {
		st.relayed = true
		return nil
	}
	maps.Copy(h, own)
	e := unreachable("the hop failed")
	e.NotExecuted = !sent.Load()
	log.LogAttrs(r.Context(), slog.LevelWarn, "forward failed", slog.Bool("sent", sent.Load()), slog.String("error", hopErr.Error()))
	return e
}

// validRequestID reports whether a forwarded request id looks like one this
// server draws.
func validRequestID(v string) bool {
	if v == "" || len(v) > 64 {
		return false
	}
	for i := range len(v) {
		if c := v[i]; (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}
