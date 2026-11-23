package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// resolve of ADR 0005 decision 4, and GET status, on the leader. A resolve
// answers from one observation: the channel's entry in the leader's table,
// read before the engine's Lookup, which reads the head and the headers of
// the commits above the engine's index floor in one step; then the leader
// must not have stopped. The headers from the channel's open head up to the
// floor, if any, are read from storage, where they never change.

// Header reads from storage for resolves (ADR 0005 decision 9): in flight
// per database and per pod.
const (
	headerReadsDB  = 16
	headerReadsPod = 64
	// headerRetry is the wait before a resolve observes again after a
	// header read failed.
	headerRetry = 50 * time.Millisecond
)

// header is a commit header read from storage for resolves, kept in its
// leader's table, or being read: done is closed once id, bump and err are
// set. A read that failed leaves the table, so that a later resolve reads
// the header again.
type header struct {
	done chan struct{}
	id   []byte
	bump bool
	err  error
}

// resolve answers a resolve on d, which leader leads.
func (s *Server) resolve(ctx context.Context, d *database, leader *engine.Leader, req *wire.Request) (*wire.Done, *wire.Error) {
	answer := func(r wire.Resolution, txn uint64) *wire.Done {
		return &wire.Done{Kind: wire.DoneResolve, Resolution: r, Txn: int64(txn)} //nolint:gosec // transaction numbers stay below 2^63 (ADR 0005, requirement 13)
	}
	t := s.table(d, leader)
	if t == nil { // step 1
		return answer(wire.ResolutionUnknown, leader.Head()), nil
	}
	o, ok := openHead(req.Channel, t)
	if !ok {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the token does not name this database")
	}
	limit := s.statementTime
	if req.TimeoutMS > 0 {
		limit = min(limit, time.Duration(req.TimeoutMS)*time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	id := commitID(req.Channel, req.Seq)
	for {
		// Step 2, the observation: the table first, so that a channel found
		// absent can send no commit above the head Lookup reads; then the
		// leader still serves, so that no commit of it is unknown.
		s.chMu.Lock()
		_, live := t.chans[req.Channel] // live, or ended but unsettled
		s.chMu.Unlock()
		txn, bump, h, _ := leader.Lookup(id, o)
		floor := leader.IndexFloor()
		switch {
		case leader.Err() != nil:
			return answer(wire.ResolutionUnknown, h), nil
		case o > h: // step 3
			return nil, wire.NewError(wire.CodeBehind, fmt.Sprintf("the leader's head, transaction %d, is below the channel's open head %d", h, o))
		case h-o > uint64(s.limits.ResolveBound): //nolint:gosec // a positive limit
			return answer(wire.ResolutionUnknown, h), nil // step 4
		case txn != 0:
			return answer(wire.ResolutionCommitted, txn), nil
		}
		if o < floor {
			// Step 5: the headers above o up to the floor are not held.
			found, below, ok := s.headers(ctx, t, leader, id, o, floor)
			switch {
			case found != 0:
				return answer(wire.ResolutionCommitted, found), nil
			case !ok:
				select {
				case <-ctx.Done():
					return answer(wire.ResolutionUnknown, h), nil
				case <-time.After(headerRetry):
				}
				continue
			}
			bump = max(bump, below)
		}
		// Step 6.
		switch {
		case bump != 0:
			return answer(wire.ResolutionNotCommitted, h), nil
		case live:
			return answer(wire.ResolutionPending, h), nil
		}
		return answer(wire.ResolutionNotCommitted, h), nil
	}
}

// openHead returns the open head that token carries, if it is a channel
// token that names t's database. A read-only channel never commits, and
// its resolve answers what it would for a write channel's request that
// never committed.
func openHead(token string, t *table) (uint64, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 5 || parts[0] != channelTokenPrefix && parts[0] != readTokenPrefix || parts[2] != hex.EncodeToString(t.office.DB[:]) {
		return 0, false
	}
	o, err := strconv.ParseUint(parts[3], 10, 64)
	return o, err == nil
}

// headers reads the headers of transactions o+1 to f, which are at most
// leader's head, from storage, newest first, through the header read
// limits, reusing those t holds or another resolve is reading. It returns
// the transaction that carries id, or 0, a nonzero number if one of them is
// an epoch bump, and false if a read failed or ctx ended first.
func (s *Server) headers(ctx context.Context, t *table, leader *engine.Leader, id []byte, o, f uint64) (found, bump uint64, ok bool) {
	s.chMu.Lock()
	if bound := uint64(s.limits.ResolveBound); len(t.hdrs) > int(bound) && f > bound { //nolint:gosec // a positive limit
		// No resolve within the bound needs a header that far below.
		for n := range t.hdrs {
			if n <= f-bound {
				delete(t.hdrs, n)
			}
		}
	}
	s.chMu.Unlock()
	hs := make([]*header, 0, f-o)
	for n := f; n > o; n-- {
		s.chMu.Lock()
		h := t.hdrs[n]
		start := h == nil
		if start {
			h = &header{done: make(chan struct{})}
			t.hdrs[n] = h
		}
		s.chMu.Unlock()
		if start && !s.readHeader(ctx, t, leader, n, h) {
			return 0, 0, false
		}
		hs = append(hs, h)
	}
	for i, h := range hs {
		select {
		case <-h.done:
		case <-ctx.Done():
			return 0, 0, false
		}
		n := f - uint64(i) //nolint:gosec // i is below f-o
		switch {
		case h.err != nil:
			return 0, 0, false
		case h.bump:
			bump = n
		case string(h.id) == string(id):
			found = n
		}
	}
	return found, bump, true
}

// readHeader starts the read of header n into h, once it holds a place in
// the database's and the pod's header read limits, and reports whether it
// started; one that did not leaves t.
func (s *Server) readHeader(ctx context.Context, t *table, leader *engine.Leader, n uint64, h *header) bool {
	fail := func(err error) bool {
		s.chMu.Lock()
		delete(t.hdrs, n)
		s.chMu.Unlock()
		h.err = err
		close(h.done)
		return false
	}
	select {
	case t.hdrSem <- struct{}{}:
	case <-ctx.Done():
		return fail(ctx.Err())
	}
	select {
	case s.hdrSem <- struct{}{}:
	case <-ctx.Done():
		<-t.hdrSem
		return fail(ctx.Err())
	}
	go func() {
		id, bump, err := leader.Header(ctx, n)
		<-s.hdrSem
		<-t.hdrSem
		if err != nil {
			fail(err)
			return
		}
		h.id, h.bump = id, bump
		close(h.done)
	}()
	return true
}

// status answers GET status on d, which leader leads.
func (s *Server) status(d *database, leader *engine.Leader) (*wire.Done, *wire.Error) {
	t := s.table(d, leader)
	if t == nil {
		return nil, wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	}
	l := s.limits
	return &wire.Done{Kind: wire.DoneStatus, Txn: int64(leader.Head()), Epoch: int64(t.office.Epoch), SQLiteVersion: sqliteVersion(), Limits: &l}, nil //nolint:gosec // as in resolve
}

// sqliteVersion is the SQLite library's version.
var sqliteVersion = sync.OnceValue(func() string {
	db, err := sqlite3.Open(":memory:")
	if err != nil {
		return "unknown"
	}
	defer func() { _ = db.Close() }()
	st, _, err := db.Prepare("SELECT sqlite_version()")
	if err != nil {
		return "unknown"
	}
	defer func() { _ = st.Close() }()
	if !st.Step() {
		return "unknown"
	}
	return st.ColumnText(0)
})
