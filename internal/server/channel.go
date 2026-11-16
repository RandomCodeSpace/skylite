package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// Write channels of ADR 0005 decision 2, on the leader: open, close, and
// the sequenced execute and ping, with the sequence table, receipts and
// replay of decision 3. Every execute is an autocommit write: it waits for
// the database's write lock, then runs BEGIN IMMEDIATE, the statement and
// COMMIT on a fresh connection, which is closed afterwards. Transactions,
// cancel, batch, channel describe, read-only channels and resolve are not
// built: begin, batch and describe are refused with not_serving before
// acceptance, and commit and rollback, which a channel with no transaction
// open accepts, record invalid_state as the state machine says.

// Channel bounds of ADR 0005 decision 9, and this server's shares of the
// memory budget for channels: a receipt per channel, held result rows and
// retained replay data. See the memory budget in server.go.
const (
	maxChannels          = 16384    // channels per pod
	maxChannelsDB        = 4096     // per pod and database
	maxChannelsPrincipal = 256      // per pod, database and principal
	receiptBytes         = 2 << 10  // reserved per channel
	heldShare            = 64 << 20 // held results, per pod
	// replayShare is the retained replay data per pod and per database.
	// ADR 0005 allows 256 MiB per pod when the budget has room; this
	// server's budget gives it 32 MiB.
	replayShare        = 32 << 20
	maxWaiters         = 64      // writers waiting for the lock, per database
	maxWaitersBy       = 16      // per database and principal
	controlBodyBytes   = 4 << 10 // body limit of cancel, close and resolve
	sweepInterval      = time.Second
	channelTokenPrefix = "w1"
)

// chLimits are the channel bounds the limits object does not report; tests
// lower them.
type chLimits struct {
	channels, perDB, perPrincipal int
	waiters, waitersBy            int
	held, replay                  int64
	sweep                         time.Duration
}

func defaultChLimits() chLimits {
	return chLimits{maxChannels, maxChannelsDB, maxChannelsPrincipal, maxWaiters, maxWaitersBy, heldShare, replayShare, sweepInterval}
}

// table holds the write channels of one leader, the database's leader on
// this server at one epoch. Every field of a table and of its channels is
// guarded by Server.chMu.
type table struct {
	leader *engine.Leader
	office engine.Office
	chans  map[string]*channel // by token
	by     map[string]int      // channels per principal
	replay int64               // retained replay data
	lock   writeLock
}

// channel is one write channel.
type channel struct {
	t                *table
	token, id, who   string
	last             int64 // the last accepted sequence number
	sum              [sha256.Size]byte
	running, closing bool
	ended            bool          // refuses everything; leaves t once nothing runs
	receipt          *wire.Receipt // the outcome of last; nil while it runs, or if last is 0
	frames           bool          // last's response had frames before its terminal frame
	data             []byte        // last's retained replay data, charged
	lost             *wire.Error   // the unrecorded end of a running request, for waiting closes
	finished         chan struct{} // closed when the running request has finished
	cancel           context.CancelCauseFunc
	idle             time.Time // when the channel last became idle
}

// errClosed is the cause of a running request's interruption by close.
var errClosed = errors.New("server: the channel was closed")

// writeLock is a database's write lock, granted first in, first out.
type writeLock struct {
	held  bool
	queue []*waiter
	by    map[string]int // waiting per principal
}

type waiter struct {
	who   string
	ready chan struct{}
}

// errWaiters refuses a writer when a waiting-writer bound is reached.
var errWaiters = errors.New("server: too many writers wait for the write lock")

// table returns the channel table of leader, which leads d, or nil if leader
// has stopped or no longer leads d. A table of a stopped leader is ended.
func (s *Server) table(d *database, leader *engine.Leader) *table {
	st := d.loop.State()
	if st.Leader != leader || leader.Err() != nil {
		return nil
	}
	s.chMu.Lock()
	defer s.chMu.Unlock()
	if t := d.chans; t != nil {
		if t.leader == leader {
			return t
		}
		if t.leader.Err() == nil {
			return nil // leader is older than the table's, which is live
		}
		s.endTable(t)
	}
	d.chans = &table{leader: leader, office: st.Office, chans: map[string]*channel{}, by: map[string]int{}, lock: writeLock{by: map[string]int{}}}
	return d.chans
}

// endTable ends every channel of t, with s.chMu held.
func (s *Server) endTable(t *table) {
	for _, ch := range t.chans {
		s.end(ch)
	}
}

// end ends ch, with s.chMu held. It leaves its table at once if nothing
// runs on it, and otherwise when the running request has finished: until
// then it is ended but unsettled.
func (s *Server) end(ch *channel) {
	ch.ended = true
	if ch.running {
		return
	}
	t := ch.t
	if t.chans[ch.token] != ch {
		return
	}
	delete(t.chans, ch.token)
	if t.by[ch.who]--; t.by[ch.who] == 0 {
		delete(t.by, ch.who)
	}
	s.nChans--
	s.dropData(ch)
}

// dropData releases ch's retained replay data, with s.chMu held.
func (s *Server) dropData(ch *channel) {
	n := int64(len(ch.data))
	s.replay -= n
	ch.t.replay -= n
	ch.data = nil
}

// sweep ends the channel tables of leaders that stopped or left office and
// the channels idle past the channel idle limit, every chl.sweep until ctx
// is done.
func (s *Server) sweep(ctx context.Context) {
	tick := time.NewTicker(s.chl.sweep)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		idle := time.Duration(s.limits.ChannelIdleMS) * time.Millisecond
		for _, d := range s.dbs {
			cur := d.leader()
			s.chMu.Lock()
			if t := d.chans; t != nil && t.leader != cur {
				s.endTable(t)
				d.chans = nil
			} else if t != nil {
				for _, ch := range t.chans {
					if !ch.running && time.Since(ch.idle) > idle {
						s.end(ch)
					}
				}
			}
			s.chMu.Unlock()
		}
	}
}

// openChannel serves an open on d, which leader leads.
func (s *Server) openChannel(d *database, leader *engine.Leader, req *wire.Request, who string) (*wire.Done, *wire.Error) {
	if req.Mode != wire.ModeWrite {
		return nil, wire.NewError(wire.CodeNotServing, "read-only channels are not served in this version; open a write channel")
	}
	t := s.table(d, leader)
	if t == nil {
		return nil, wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	}
	s.chMu.Lock()
	defer s.chMu.Unlock()
	if s.nChans >= s.chl.channels || len(t.chans) >= s.chl.perDB || t.by[who] >= s.chl.perPrincipal {
		return nil, wire.NewError(wire.CodeTooManyChannels, "a channel bound of this server, its database or the principal was reached; close a channel")
	}
	head := leader.Head()
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	// The token names its database and this server process, carries the
	// open head, and holds 256 random bits (ADR 0005 decision 2).
	token := strings.Join([]string{channelTokenPrefix, s.instance, hex.EncodeToString(t.office.DB[:]), fmt.Sprint(head), base64.RawURLEncoding.EncodeToString(secret)}, ".")
	ch := &channel{t: t, token: token, id: hex.EncodeToString(id), who: who, idle: time.Now()}
	t.chans[token] = ch
	t.by[who]++
	s.nChans++
	return &wire.Done{Kind: wire.DoneOpen, Txn: int64(head), Channel: &wire.Channel{ //nolint:gosec // transaction numbers and epochs stay below 2^63 (ADR 0005, requirement 13)
		Token: token, ID: ch.id, Mode: wire.ModeWrite, Epoch: int64(t.office.Epoch), Limits: s.limits, //nolint:gosec // as above
	}}, nil
}

// lookup returns the live channel that token names on t for who, with
// s.chMu held, or the refusal: checks 7 to 9 of ADR 0005. In this version
// only the leader in office holds write channels, so a channel of another
// server process ended when that process left office.
func (s *Server) lookup(t *table, token, who string) (*channel, *wire.Error) {
	parts := strings.Split(token, ".")
	if len(parts) != 5 || parts[0] != channelTokenPrefix || parts[2] != hex.EncodeToString(t.office.DB[:]) {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the token does not name this database")
	}
	ch := t.chans[token]
	if parts[1] != s.instance || ch == nil || ch.who != who || ch.ended {
		return nil, wire.NewError(wire.CodeChannelLost, "the channel does not exist on this server, belongs to another principal, or has ended")
	}
	return ch, nil
}

// commitID is the commit identifier of sequence number seq on the channel
// of token: the first 16 bytes of SHA-256 of the whole token, then seq as 8
// bytes, big-endian (ADR 0005 decision 2).
func commitID(token string, seq int64) []byte {
	h := sha256.Sum256([]byte(token))
	return binary.BigEndian.AppendUint64(h[:16:16], uint64(seq)) //nolint:gosec // seq is from 1 to 2^63-1
}

// verdict is the sequence table's answer to a sequenced request.
type verdict int

const (
	accept verdict = iota
	replay
	refuse
)

// classify applies the sequence table of ADR 0005 decision 2 to seq with
// body digest sum, with s.chMu held.
func (ch *channel) classify(seq int64, sum [sha256.Size]byte) (verdict, *wire.Error) {
	switch {
	case seq == ch.last+1 && !ch.running:
		return accept, nil
	case seq == ch.last+1:
		return refuse, wire.NewError(wire.CodeChannelBusy, "another request on the channel is running")
	case seq == ch.last && sum != ch.sum:
		return refuse, wire.NewError(wire.CodeSequenceConflict, "the sequence number was accepted with a different body")
	case seq == ch.last && ch.running:
		return refuse, wire.NewError(wire.CodeInFlight, "this request is running")
	case seq == ch.last:
		return replay, nil
	case seq < ch.last:
		return refuse, wire.NewError(wire.CodeStaleSequence, fmt.Sprintf("the sequence number is below the last accepted, %d", ch.last))
	}
	return refuse, wire.NewError(wire.CodeSequenceGap, fmt.Sprintf("the sequence number is above the last accepted plus one, %d", ch.last+1))
}

// owned fills the fields of an answer from a channel's owner.
func owned(e *wire.Error, ch *channel, seq int64) *wire.Error {
	e.ChannelID, e.Seq, e.TxnState = ch.id, seq, wire.TxnNone
	return e
}

// replayed returns ch's recorded outcome as an answer, with s.chMu held, and
// the retained data to send before it.
func replayed(ch *channel) (*wire.Done, *wire.Error, []byte) {
	if r := ch.receipt; r.Error != nil {
		e := *r.Error
		return nil, &e, nil
	}
	d := *ch.receipt.Done
	d.DataOmitted = ch.frames && ch.data == nil
	return &d, nil, ch.data
}

// channelRequest serves a sequenced request on d, which leader leads; body
// is its exact body and id its request id.
func (s *Server) channelRequest(ctx context.Context, st *stream, d *database, leader *engine.Leader, req *wire.Request, body []byte, who, id string, log *slog.Logger) (*wire.Done, *wire.Error) {
	t := s.table(d, leader)
	if t == nil {
		return nil, wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	}
	seq, sum := req.Seq, sha256.Sum256(body)
	// check returns the channel and the sequence table's verdict, with
	// s.chMu held, or the refusal.
	check := func() (*channel, verdict, *wire.Error) {
		ch, e := s.lookup(t, req.Channel, who)
		switch {
		case e != nil:
			return nil, refuse, e
		case ch.closing:
			return nil, refuse, owned(wire.NewError(wire.CodeChannelLost, "the channel is closing"), ch, seq)
		}
		switch req.Op {
		case wire.OpBegin, wire.OpBatch, wire.OpDescribe:
			return nil, refuse, owned(wire.NewError(wire.CodeNotServing, "transactions, batch and describe on a channel are not served in this version"), ch, seq)
		}
		v, e := ch.classify(seq, sum)
		if e != nil {
			_ = owned(e, ch, seq)
		}
		return ch, v, e
	}
	s.chMu.Lock()
	ch, v, e := check()
	if v == replay {
		done, e, data := replayed(ch)
		s.chMu.Unlock()
		s.send(st, data)
		return done, e
	}
	s.chMu.Unlock()
	if e != nil {
		return nil, e
	}

	// Checks 11 and 12 for a request that would be accepted: the watermark,
	// and the fresh connection an execute writes on. The statement time
	// runs from here.
	limit := s.statementTime
	if req.TimeoutMS > 0 {
		limit = min(limit, time.Duration(req.TimeoutMS)*time.Millisecond)
	}
	deadline := time.Now().Add(limit)
	var c *pconn
	if req.Op == wire.OpExecute {
		if req.MinTxn > 0 && leader.Head() < uint64(req.MinTxn) { //nolint:gosec // min_txn is from 0 to 2^63-1
			return nil, owned(wire.NewError(wire.CodeBehind, fmt.Sprintf("the leader's head, transaction %d, is below min_txn %d", leader.Head(), req.MinTxn)), ch, seq)
		}
		octx, cancel := context.WithDeadline(ctx, deadline)
		var err error
		c, e, err = s.writeConn(octx, d, leader, who)
		cancel()
		if e != nil {
			logInternal(ctx, log, e, err)
			return nil, owned(e, ch, seq)
		}
	}

	// Acceptance, if the channel still allows it.
	s.chMu.Lock()
	ch, v, e = check()
	if v != accept {
		var done *wire.Done
		var data []byte
		if v == replay {
			done, e, data = replayed(ch)
		}
		s.chMu.Unlock()
		if c != nil {
			s.pool.release(who, c, false)
		}
		if v == replay {
			s.send(st, data)
		}
		return done, e
	}
	ch.last, ch.sum, ch.running, ch.receipt, ch.frames = seq, sum, true, nil, false
	ch.finished, ch.lost = make(chan struct{}), nil
	s.dropData(ch)
	// A dropped HTTP connection cancels nothing (ADR 0005 decision 6); a
	// close does.
	rctx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	ch.cancel = cancel
	s.chMu.Unlock()
	defer cancel(nil)
	st.start() // accepted: status 200 whatever follows

	var rec *wire.Receipt
	var lost *wire.Error
	var held *heldRows
	switch e := req.Validate(); {
	case e != nil:
		rec = &wire.Receipt{Error: e.Recorded(false)}
	case req.Op == wire.OpExecute:
		rec, held, lost = s.execute(rctx, d, t, c, req, deadline, limit, who, log)
		c = nil
	case req.Op == wire.OpPing:
		rec = &wire.Receipt{Done: &wire.Done{Kind: wire.DoneRollback, Txn: int64(leader.Head())}} //nolint:gosec // as in openChannel
	default: // commit and rollback with no transaction open
		rec = &wire.Receipt{Error: wire.NewError(wire.CodeInvalidState, "no transaction is open on the channel").Recorded(false)}
	}
	if c != nil {
		s.pool.release(who, c, false)
	}
	defer held.free()
	var data []byte
	if held != nil {
		data = held.b.Bytes()
	}
	switch {
	case rec == nil:
		_ = owned(lost, ch, seq)
	case rec.Done != nil:
		rec.Done.Seq, rec.Done.TxnState = seq, wire.TxnNone
	default:
		owned(rec.Error, ch, seq).RequestID = id
	}
	s.record(ch, rec, lost, data)
	if lost != nil {
		e := *lost
		return nil, &e
	}
	if rec.Error != nil {
		e := *rec.Error
		return nil, &e
	}
	done := *rec.Done
	s.send(st, data)
	return &done, nil
}

// record ends the running request of ch with its receipt, or with the
// unrecorded error lost, which ends the channel, and keeps data, the
// frames of its response before the terminal one, if they fit.
func (s *Server) record(ch *channel, rec *wire.Receipt, lost *wire.Error, data []byte) {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	ch.running, ch.idle = false, time.Now()
	if lost != nil {
		ch.lost = lost
		ch.ended = true
	} else {
		ch.receipt, ch.frames = rec, len(data) > 0
		n := int64(len(data))
		t := ch.t
		if rec.Done != nil && n > 0 && n <= s.limits.RetainedRequestBytes && t.replay+n <= s.chl.replay && s.replay+n <= s.chl.replay {
			ch.data = bytes.Clone(data)
			t.replay += n
			s.replay += n
		}
	}
	if ch.ended || ch.closing {
		s.end(ch)
	}
	close(ch.finished)
}

// send starts the response with status 200 and writes data, frames before
// the terminal frame, within the response deadline. A response that cannot
// be written is cut, and the driver replays.
func (s *Server) send(st *stream, data []byte) {
	st.start()
	if len(data) > 0 {
		_ = st.rc.SetWriteDeadline(time.Now().Add(s.responseDeadline))
		_, _ = st.Write(data)
	}
}

// closeChannel serves a close. A channel with a request running is closing
// until that request has finished; the answer waits for it.
func (s *Server) closeChannel(d *database, leader *engine.Leader, req *wire.Request, who string) (*wire.Done, *wire.Error) {
	t := s.table(d, leader)
	if t == nil {
		return nil, wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	}
	s.chMu.Lock()
	ch, e := s.lookup(t, req.Channel, who)
	if e != nil {
		s.chMu.Unlock()
		return nil, e
	}
	if ch.running {
		if !ch.closing {
			ch.closing = true
			ch.cancel(errClosed)
		}
		fin := ch.finished
		s.chMu.Unlock()
		<-fin
		s.chMu.Lock()
	}
	defer s.chMu.Unlock()
	s.end(ch)
	if ch.lost != nil {
		e := *ch.lost
		e.Seq = 0 // a close names no sequence number
		return nil, &e
	}
	done := &wire.Done{Kind: wire.DoneClose, LastSeq: ch.last}
	if ch.receipt != nil {
		r := *ch.receipt
		done.Outcome = &r
	}
	return done, nil
}

// writeConn reserves a SQLite connection for who and opens a fresh one on
// leader: check 12 of ADR 0005 for an execute.
func (s *Server) writeConn(ctx context.Context, d *database, leader *engine.Leader, who string) (*pconn, *wire.Error, error) {
	_, evict, ok := s.pool.take(nil, who)
	if !ok {
		return nil, wire.NewError(wire.CodeOverloaded, "the server's or the principal's SQLite connections are all in use; retry later"), nil
	}
	if evict != nil {
		_ = evict.ec.Close()
	}
	c, err := s.open(ctx, leader)
	if err != nil {
		s.pool.release(who, nil, false)
		return nil, s.refusal(ctx, d, leader, err), err
	}
	return c, nil, nil
}

// acquire waits for t's write lock for who until ctx ends, first in, first
// out.
func (s *Server) acquire(ctx context.Context, t *table, who string) error {
	s.chMu.Lock()
	l := &t.lock
	if !l.held {
		l.held = true
		s.chMu.Unlock()
		return nil
	}
	if len(l.queue) >= s.chl.waiters || l.by[who] >= s.chl.waitersBy {
		s.chMu.Unlock()
		return errWaiters
	}
	w := &waiter{who: who, ready: make(chan struct{})}
	l.queue = append(l.queue, w)
	l.by[who]++
	s.chMu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
	}
	s.chMu.Lock()
	defer s.chMu.Unlock()
	select {
	case <-w.ready: // granted as ctx ended: pass it on
		s.releaseLocked(t)
	default:
		l.queue = slices.DeleteFunc(l.queue, func(x *waiter) bool { return x == w })
		if l.by[who]--; l.by[who] == 0 {
			delete(l.by, who)
		}
	}
	return context.Cause(ctx)
}

// release gives t's write lock to its first waiter, or frees it.
func (s *Server) release(t *table) {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	s.releaseLocked(t)
}

func (s *Server) releaseLocked(t *table) {
	l := &t.lock
	if len(l.queue) == 0 {
		l.held = false
		return
	}
	w := l.queue[0]
	l.queue = l.queue[1:]
	if l.by[w.who]--; l.by[w.who] == 0 {
		delete(l.by, w.who)
	}
	close(w.ready)
}

// heldRows holds an autocommit write's frames until its commit has an
// outcome, charged to the held-result bounds: limit for the request, and
// the pod's share.
type heldRows struct {
	s     *Server
	b     bytes.Buffer
	limit int64
}

func (h *heldRows) Write(p []byte) (int, error) {
	n := int64(len(p))
	if int64(h.b.Len())+n > h.limit {
		return 0, wire.NewError(wire.CodeResultTooLarge, fmt.Sprintf("the write's result passes the held-result bound of %d bytes", h.limit))
	}
	h.s.chMu.Lock()
	if h.s.held+n > h.s.chl.held {
		h.s.chMu.Unlock()
		return 0, wire.NewError(wire.CodeResultTooLarge, "the server's held results are full; retry later")
	}
	h.s.held += n
	h.s.chMu.Unlock()
	return h.b.Write(p)
}

// free releases the charge; h may be nil.
func (h *heldRows) free() {
	if h == nil {
		return
	}
	h.s.chMu.Lock()
	h.s.held -= int64(h.b.Len())
	h.s.chMu.Unlock()
	h.b.Reset()
}

// execute runs an accepted execute as an autocommit write on c, a fresh
// connection it closes, within limit from now. It returns the receipt and
// the held frames, or the unrecorded error that ends the channel.
func (s *Server) execute(ctx context.Context, d *database, t *table, c *pconn, req *wire.Request, deadline time.Time, limit time.Duration, who string, log *slog.Logger) (*wire.Receipt, *heldRows, *wire.Error) {
	defer s.pool.release(who, c, false) // never reused after a write (ADR 0005 decision 6)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	recorded := func(e *wire.Error) *wire.Receipt { return &wire.Receipt{Error: e.Recorded(false)} }
	lctx, lcancel := context.WithTimeout(ctx, time.Duration(s.limits.LockWaitMS)*time.Millisecond)
	err := s.acquire(lctx, t, who)
	lcancel()
	switch {
	case err == nil:
	case errors.Is(context.Cause(ctx), errClosed):
		return recorded(wire.NewError(wire.CodeCancelled, "cancelled by close before it ran")), nil, nil
	case ctx.Err() != nil:
		return recorded(timeoutError(limit)), nil, nil
	case errors.Is(err, errWaiters):
		return recorded(wire.NewError(wire.CodeBusy, "too many writers are waiting for the write lock")), nil, nil
	default:
		return recorded(wire.NewError(wire.CodeBusy, fmt.Sprintf("the write lock was not granted within %d ms", s.limits.LockWaitMS))), nil, nil
	}
	defer s.release(t)
	held := &heldRows{s: s, limit: s.limits.HeldResultBytes}
	var done *wire.Done
	var e *wire.Error
	var snap uint64
	txn, err := c.ec.Run(ctx, commitID(req.Channel, req.Seq), func(db *sqlite3.Conn) error {
		var ferr error
		done, e, ferr = s.write(db, c, t.leader, req, held, &snap)
		return ferr
	})
	switch {
	case txn != 0:
		// Committed, whatever failed after the commit point.
		if err != nil {
			log.LogAttrs(ctx, slog.LevelWarn, "write committed, then failed", slog.String("class", engineClass(err)))
		}
		done.Txn = int64(txn) //nolint:gosec // as in openChannel
		return &wire.Receipt{Done: done}, held, nil
	case err == nil:
		done.Txn = int64(snap) //nolint:gosec // as above; the write changed no page
		return &wire.Receipt{Done: done}, held, nil
	}
	held.free() // nothing committed: no row is sent
	switch {
	case errors.Is(err, engine.ErrUnknown):
		log.LogAttrs(ctx, slog.LevelError, "write outcome unknown", slog.String("class", engineClass(err)))
		return nil, nil, wire.NewError(wire.CodeCommitOutcomeUnknown, "the commit's outcome is unknown; the server stopped serving the database; resolve the request")
	case errors.Is(err, engine.ErrTooLarge):
		return recorded(wire.NewError(wire.CodeCommitFailed, "the transaction is too large for one commit or for the leader's local WAL bound")), nil, nil
	case errors.Is(err, engine.ErrFenced):
		return recorded(wire.NewError(wire.CodeCommitFailed, "the leader was fenced by another leader's epoch")), nil, nil
	case errors.Is(err, engine.ErrTailBound), errors.Is(err, engine.ErrRetry):
		return recorded(s.engineError(d, t.leader, err)), nil, nil
	case errors.Is(err, engine.ErrBroken):
		log.LogAttrs(ctx, slog.LevelError, "write failed", slog.String("class", engineClass(err)))
		return recorded(wire.NewError(wire.CodeInternal, "the connection broke and was discarded")), nil, nil
	case errors.Is(err, engine.ErrStopped), t.leader.Err() != nil:
		// The leader stopped: the channel has ended, and nothing is recorded.
		l := wire.NewError(wire.CodeChannelLost, "the leader stopped serving the database while the request ran; it did not commit")
		l.NotExecuted = false
		return nil, nil, l
	case errors.Is(context.Cause(ctx), errClosed):
		return recorded(wire.NewError(wire.CodeCancelled, "cancelled by close before its commit point")), nil, nil
	case e != nil:
		return recorded(e), nil, nil
	case ctx.Err() != nil:
		return recorded(timeoutError(limit)), nil, nil
	}
	log.LogAttrs(ctx, slog.LevelError, "write failed", slog.String("class", engineClass(err)))
	return recorded(wire.NewError(wire.CodeInternal, "the write failed")), nil, nil
}

// write runs inside the Run of an execute: BEGIN IMMEDIATE, the client's
// statement with its frames held in h, then COMMIT, or ROLLBACK after a
// failure, so that a failed autocommit write leaves nothing (ADR 0005
// decision 6). snap is set to the head the write started from.
func (s *Server) write(db *sqlite3.Conn, c *pconn, leader *engine.Leader, req *wire.Request, h *heldRows, snap *uint64) (done *wire.Done, e *wire.Error, err error) {
	ictx := db.GetInterrupt()
	if err := c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("BEGIN IMMEDIATE") }); err != nil {
		return nil, nil, broken(err)
	}
	// No commit lands while this connection holds SQLite's write lock.
	*snap = leader.Head()
	out, err := wire.NewWriter(h, wire.DefaultFrameBytes, s.limits)
	if err == nil {
		total := db.TotalChanges()
		err = c.pc.Run(ictx, req.SQL, func(stmt *sqlite3.Stmt) error {
			if txnControl(req.SQL) {
				return wire.NewError(wire.CodeInvalidRequest, "transaction control is not SQL in this protocol; use a channel's begin, commit and rollback")
			}
			if err := bind(req, stmt); err != nil {
				return err
			}
			cols := columns(stmt)
			if len(cols) > 0 {
				if err := out.Columns(cols); err != nil {
					return err
				}
			}
			row := make([]any, len(cols))
			for stmt.Step() {
				for i := range row {
					row[i] = value(stmt, i)
				}
				if err := out.Row(row); err != nil {
					return err
				}
			}
			if err := stmt.Err(); err != nil {
				return err
			}
			var changes int64
			if db.TotalChanges() != total {
				changes = db.Changes()
			}
			done = &wire.Done{Kind: wire.DoneExecute, ReadOnly: stmt.ReadOnly(), RowsAffected: changes, LastInsertRowID: db.LastInsertRowID()}
			return nil
		})
	}
	if err == nil {
		err = c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("COMMIT") })
	}
	if err == nil {
		return done, nil, nil
	}
	rerr := c.pc.Privileged(func(db *sqlite3.Conn) error {
		if db.GetAutocommit() {
			return nil
		}
		return db.Exec("ROLLBACK")
	})
	e, berr := statementError(ictx, err)
	if berr == nil && rerr != nil {
		// A connection that cannot roll back is not trusted to leave nothing.
		berr = fmt.Errorf("%w: rollback failed", engine.ErrBroken)
	}
	if berr != nil {
		return nil, e, berr
	}
	return nil, e, errEnded
}
