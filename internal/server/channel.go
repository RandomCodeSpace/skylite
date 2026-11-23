package server

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// Write channels of ADR 0005 decision 2, on the leader: open, close,
// cancel, and the sequenced begin, execute, batch, describe, commit,
// rollback and ping, with the sequence table, receipts and replay of
// decision 3 and the channel state machine. An execute or a batch with no
// transaction open is an autocommit write: it waits for the database's
// write lock, then runs BEGIN IMMEDIATE, the statements and COMMIT on a
// fresh connection, which is closed afterwards. A begin waits for the lock
// and runs BEGIN IMMEDIATE on a fresh connection; the transaction holds
// both until its commit, its rollback, or its abort by SQLite, a
// transaction limit or the engine, and its connection is then closed. A
// describe with no transaction open runs on a pooled connection. Read-only
// channels are not built. Resolve is in resolve.go.

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
	channelTokenPrefix = "w1" // a write channel's token
	readTokenPrefix    = "r1" // a read-only channel's token
	causeMaintenance   = "maintenance"
)

// chLimits are the channel bounds the limits object does not report; tests
// lower them.
type chLimits struct {
	channels, perDB, perPrincipal int
	waiters, waitersBy            int
	held, replay                  int64
	sweep                         time.Duration
	resolveReads                  int // header reads one resolve may start (resolve.go)
}

func defaultChLimits() chLimits {
	return chLimits{maxChannels, maxChannelsDB, maxChannelsPrincipal, maxWaiters, maxWaitersBy, heldShare, replayShare, sweepInterval, headerReadsResolve}
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
	hdrs   map[uint64]*header // commit headers below the engine's index floor, read for resolves
	hdrSem chan struct{}      // header reads in flight for resolves
	// reading holds, per principal, the channel its one resolve that reads
	// headers from storage closes when it is done.
	reading map[string]chan struct{}
}

// channel is one write channel.
type channel struct {
	t                *table
	token, id, who   string
	ro               bool  // a read-only channel
	last             int64 // the last accepted sequence number
	sum              [sha256.Size]byte
	precancelled     bool // last was recorded as cancelled before it arrived, with no digest
	running, closing bool
	ended            bool          // refuses everything; leaves t once nothing runs
	receipt          *wire.Receipt // the outcome of last; nil while it runs, or if last is 0
	frames           bool          // last's response had frames before its terminal frame
	data             []byte        // last's retained replay data, charged
	run              *completion   // the completion of the last accepted request; nil if none was
	shut             chan struct{} // closed once a closing channel's transaction is rolled back and its lock released
	cancel           context.CancelCauseFunc
	idle             time.Time     // when the channel last became idle
	state            wire.TxnState // the transaction state after the last recorded request
	cause            string        // causeMaintenance if the engine aborted the transaction
	tx               *txn          // the open transaction; nil when none is open
}

// completion is the outcome of one accepted request, for the closes and
// cancels that wait for it. It is set before finished is closed and does
// not change after.
type completion struct {
	finished chan struct{} // closed when the request has finished
	receipt  *wire.Receipt // its receipt; nil if it ended with lost
	lost     *wire.Error   // its unrecorded end, which ended the channel
	state    wire.TxnState // the channel's transaction state after it
}

// txn is a channel's open write transaction. It holds its connection and
// the database's write lock from its begin until it ends.
type txn struct {
	c     *pconn
	snap  uint64    // the transaction number of its snapshot
	began time.Time // the transaction lifetime runs from here
}

// The causes of a running request's interruption: close, cancel, and the
// write transaction lifetime.
var (
	errClosed    = errors.New("server: the channel was closed")
	errCancelled = errors.New("server: the request was cancelled")
	errLifetime  = errors.New("server: the transaction lifetime was reached")
	// errReadLifetime is errLifetime of a read-only transaction.
	errReadLifetime = fmt.Errorf("%w: read-only", errLifetime)
)

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
	d.chans = &table{leader: leader, office: st.Office, chans: map[string]*channel{}, by: map[string]int{}, lock: writeLock{by: map[string]int{}}, hdrs: map[uint64]*header{}, hdrSem: make(chan struct{}, headerReadsDB), reading: map[string]chan struct{}{}}
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
	if ch.tx != nil && !ch.closing { // a waiting close drops it before its answer
		go s.dropTxn(ch)()
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
// the channels idle past the channel idle limit, and aborts the open write
// transactions past their idle limit or lifetime, every chl.sweep until ctx
// is done. The channel idle limit does not run while a transaction is open,
// and no timer acts on a running request: its context carries its limits.
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
			if s.sweepGap != nil {
				s.sweepGap()
			}
			s.chMu.Lock()
			// The leader is read with s.chMu held: a table that table
			// created for a leader taking office since is never ended here.
			cur := d.leader()
			if t := d.chans; t != nil && t.leader != cur {
				s.endTable(t)
				d.chans = nil
			} else if t != nil {
				now := time.Now()
				for _, ch := range t.chans {
					switch {
					case ch.running:
					case ch.tx != nil:
						txnIdle, life := s.txnLimits(ch.ro)
						if now.Sub(ch.idle) > txnIdle || now.Sub(ch.tx.began) > life {
							s.abortTxn(ch, "")
						}
					case now.Sub(ch.idle) > idle:
						s.end(ch)
					}
				}
			}
			s.chMu.Unlock()
		}
	}
}

// openChannel serves an open on d, which leader leads. In this version the
// leader owns read-only channels too.
func (s *Server) openChannel(d *database, leader *engine.Leader, req *wire.Request, who string) (*wire.Done, *wire.Error) {
	ro := req.Mode == wire.ModeReadOnly
	if head := leader.Head(); ro && head < uint64(req.MinTxn) { //nolint:gosec // min_txn is from 0 to 2^63-1
		return nil, wire.NewError(wire.CodeBehind, fmt.Sprintf("the leader's head, transaction %d, is below min_txn %d", head, req.MinTxn))
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
	prefix := channelTokenPrefix
	if ro {
		prefix = readTokenPrefix
	}
	token := strings.Join([]string{prefix, s.instance, hex.EncodeToString(t.office.DB[:]), fmt.Sprint(head), base64.RawURLEncoding.EncodeToString(secret)}, ".")
	ch := &channel{t: t, token: token, id: hex.EncodeToString(id), who: who, ro: ro, idle: time.Now(), state: wire.TxnNone}
	t.chans[token] = ch
	t.by[who]++
	s.nChans++
	return &wire.Done{Kind: wire.DoneOpen, Txn: int64(head), Channel: &wire.Channel{ //nolint:gosec // transaction numbers and epochs stay below 2^63 (ADR 0005, requirement 13)
		Token: token, ID: ch.id, Mode: req.Mode, Epoch: int64(t.office.Epoch), Limits: s.limits, //nolint:gosec // as above
	}}, nil
}

// lookup returns the live channel that token names on t for who, with
// s.chMu held, or the refusal: checks 7 to 9 of ADR 0005. With a peer
// listener, handle sends a token naming another server process to that
// process before this; without one, such a token matches no channel here
// and gets channel_lost.
func (s *Server) lookup(t *table, token, who string) (*channel, *wire.Error) {
	parts := strings.Split(token, ".")
	if len(parts) != 5 || parts[0] != channelTokenPrefix && parts[0] != readTokenPrefix || parts[2] != hex.EncodeToString(t.office.DB[:]) {
		return nil, wire.NewError(wire.CodeInvalidRequest, "the token does not name this database")
	}
	ch := t.chans[token]
	if ch == nil || ch.who != who || ch.ended {
		return nil, wire.NewError(wire.CodeChannelLost, "the channel does not exist on this server, belongs to another principal, or has ended")
	}
	return ch, nil
}

// tokenInstance returns the server process a channel token names, or "".
func tokenInstance(token string) string {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 3 || parts[0] != channelTokenPrefix && parts[0] != readTokenPrefix {
		return ""
	}
	return parts[1]
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
	case seq == ch.last && ch.precancelled:
		return replay, nil // whatever the body
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

// owned fills the fields of an answer from a channel's owner, with s.chMu
// held or by the request running on ch.
func owned(e *wire.Error, ch *channel, seq int64) *wire.Error {
	e.ChannelID, e.Seq, e.TxnState = ch.id, seq, ch.state
	return e
}

// ownedLocked is owned, taking s.chMu.
func (s *Server) ownedLocked(e *wire.Error, ch *channel, seq int64) *wire.Error {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	return owned(e, ch, seq)
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
func (s *Server) channelRequest(ctx context.Context, st *stream, d *database, leader *engine.Leader, req *wire.Request, body []byte, who, id string, log *slog.Logger) (_ *wire.Done, lostErr *wire.Error) {
	t := s.table(d, leader)
	if t == nil {
		return nil, wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	}
	seq, sum := req.Seq, sha256.Sum256(body)
	// check returns the channel and the sequence table's verdict, with
	// s.chMu held, or the refusal.
	check := func() (*channel, verdict, *wire.Error) {
		ch, e := s.lookup(t, req.Channel, who)
		if e != nil {
			return nil, refuse, e
		}
		ch.idle = time.Now() // idle time runs to the next request's arrival (ADR 0005)
		if ch.closing {
			return nil, refuse, owned(wire.NewError(wire.CodeChannelLost, "the channel is closing"), ch, seq)
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
	// A begin, execute or batch on an idle write channel starts a write
	// transaction on a fresh connection; on an idle read-only channel it
	// starts a read on a pooled connection, as a describe on any idle
	// channel does; any other request needs none.
	idle := v == accept && ch.state == wire.TxnNone
	starts := idle && (req.Op == wire.OpBegin || req.Op == wire.OpExecute || req.Op == wire.OpBatch)
	fresh := starts && !ch.ro
	pooled := starts && ch.ro || idle && req.Op == wire.OpDescribe
	s.chMu.Unlock()
	if e != nil {
		return nil, e
	}

	// Checks 11 and 12 for a request that would be accepted: the watermark,
	// maintenance and the connection. A fresh write reserves its connection
	// only once it holds the write lock. The statement time runs from here.
	limit := s.statementTime
	if req.TimeoutMS > 0 {
		limit = min(limit, time.Duration(req.TimeoutMS)*time.Millisecond)
	}
	deadline := time.Now().Add(limit)
	if starts && req.MinTxn > 0 && leader.Head() < uint64(req.MinTxn) { //nolint:gosec // min_txn is from 0 to 2^63-1
		return nil, s.ownedLocked(wire.NewError(wire.CodeBehind, fmt.Sprintf("the leader's head, transaction %d, is below min_txn %d", leader.Head(), req.MinTxn)), ch, seq)
	}
	if fresh {
		if err := leader.Admits(); err != nil {
			return nil, s.ownedLocked(s.engineError(d, leader, err), ch, seq)
		}
	}
	// A pooled connection, released after the request unless a read-only
	// transaction took it, which took then says, whatever ends the request;
	// keep says whether it goes back to the pool.
	var pc *pconn
	keep, took := false, false
	// A read-only begin first reserves a place among the read-only
	// transactions, which passes to its transaction if one begins.
	reserved := starts && ch.ro && req.Op == wire.OpBegin
	if reserved {
		if !s.pool.holdTxn(who) {
			return nil, s.ownedLocked(wire.NewError(wire.CodeOverloaded, "read-only transactions hold their share of the server's or the principal's SQLite connections; retry later"), ch, seq)
		}
		defer func() {
			if !took {
				s.pool.unholdTxn(who)
			}
		}()
	}
	if pooled {
		var e *wire.Error
		var err error
		if pc, e, err = s.conn(ctx, d, leader, who, reserved); e != nil {
			logInternal(ctx, log, e, err)
			return nil, s.ownedLocked(e, ch, seq)
		}
		defer func() {
			if !took {
				s.pool.release(who, pc, keep)
			}
		}()
		keep = true
	}

	// Acceptance, if the channel still allows it. Only a request changes
	// the state of an idle channel, so a fresh request still finds it idle.
	s.chMu.Lock()
	ch, v, e = check()
	if v != accept {
		var done *wire.Done
		var data []byte
		if v == replay {
			done, e, data = replayed(ch)
		}
		s.chMu.Unlock()
		if v == replay {
			s.send(st, data)
		}
		return done, e
	}
	ch.last, ch.sum, ch.precancelled, ch.running, ch.receipt, ch.frames = seq, sum, false, true, nil, false
	ch.run = &completion{finished: make(chan struct{})}
	s.dropData(ch)
	state, cause, tx := ch.state, ch.cause, ch.tx
	// A dropped HTTP connection cancels nothing (ADR 0005 decision 6); a
	// close or a cancel does.
	rctx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	ch.cancel = cancel
	s.chMu.Unlock()
	defer cancel(nil)
	// A panic after acceptance must not leave the channel running: it ends
	// it, as a request whose outcome is not known here.
	settled := false
	defer func() {
		if settled {
			return
		}
		p := recover()
		if p == nil {
			return
		}
		log.LogAttrs(ctx, slog.LevelError, "channel request panicked", slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
		l := owned(wire.NewError(wire.CodeChannelLost, "the request failed after acceptance and may have committed; the channel has ended"), ch, seq)
		l.NotExecuted, l.TxnState = false, wire.TxnNone
		s.record(ch, nil, l, nil, false, wire.TxnNone)
		e := *l
		lostErr = &e
	}()
	st.start() // accepted: status 200 whatever follows

	var rec *wire.Receipt
	var lost *wire.Error
	next := state
	// An autocommit write's frames are held until its commit has an
	// outcome; a transaction's stream as they are produced.
	var held *heldRows
	var sw *streamed
	statement := req.Op == wire.OpExecute || req.Op == wire.OpBatch
	switch {
	case statement && state == wire.TxnNone && !ch.ro:
		held = &heldRows{s: s, limit: s.limits.HeldResultBytes}
	case req.Op == wire.OpBegin && state == wire.TxnNone, statement && state != wire.TxnAborted, req.Op == wire.OpDescribe && state != wire.TxnAborted:
		sw = &streamed{st: st, limit: s.limits.RetainedRequestBytes, deadline: s.responseDeadline}
	}
	defer held.free()
	recorded := func(e *wire.Error, txnStatement bool) *wire.Receipt {
		return &wire.Receipt{Error: e.Recorded(txnStatement)}
	}
	switch e := req.Validate(); {
	case state == wire.TxnAborted && req.Op != wire.OpRollback:
		ae := wire.NewError(wire.CodeTransactionAborted, "the transaction was rolled back; only rollback is accepted")
		ae.Cause = cause
		rec = recorded(ae, false)
	case e != nil:
		rec = recorded(e, state == wire.TxnOpen && statement)
		if state == wire.TxnOpen && req.Op == wire.OpCommit { // a failed commit ends the transaction
			s.endTxn(ch)
			next = wire.TxnNone
		}
	case req.Op == wire.OpBegin && state == wire.TxnNone && ch.ro:
		rec, lost, next = s.readBegin(rctx, d, t, ch, pc, &took, req, sw, deadline, limit, log)
		keep = false
	case req.Op == wire.OpBegin && state == wire.TxnNone:
		rec, lost, next = s.begin(rctx, d, t, ch, req, sw, deadline, limit, log)
	case req.Op == wire.OpBegin:
		rec = recorded(wire.NewError(wire.CodeInvalidState, "a transaction is open on the channel"), false)
	case statement && state == wire.TxnNone && ch.ro:
		rec, lost, keep = s.read(rctx, d, t, pc, req, sw, deadline, limit, log)
	case statement && state == wire.TxnNone:
		rec, lost = s.execute(rctx, d, t, req, held, deadline, limit, who, log)
	case req.Op == wire.OpDescribe && state == wire.TxnNone:
		rec, lost, keep = s.describe(rctx, d, t, pc, req, sw, deadline, limit, log)
	case statement, req.Op == wire.OpDescribe:
		rec, lost, next = s.inTxn(rctx, d, t, ch, tx, req, sw, deadline, limit, log)
	case req.Op == wire.OpCommit && state == wire.TxnOpen && ch.ro:
		// A read-only transaction ends committed at its snapshot.
		s.endTxn(ch)
		rec, next = &wire.Receipt{Done: &wire.Done{Kind: wire.DoneCommit, Txn: int64(tx.snap)}}, wire.TxnNone //nolint:gosec // as in openChannel
	case req.Op == wire.OpCommit && state == wire.TxnOpen:
		rec, lost = s.commit(rctx, d, t, ch, tx, req, deadline, limit, log)
		next = wire.TxnNone
	case req.Op == wire.OpRollback && state != wire.TxnNone:
		s.endTxn(ch)
		rec, next = &wire.Receipt{Done: &wire.Done{Kind: wire.DoneRollback, Txn: int64(leader.Head())}}, wire.TxnNone //nolint:gosec // as in openChannel
	case req.Op == wire.OpPing:
		txn := leader.Head()
		if tx != nil {
			txn = tx.snap
		}
		rec = &wire.Receipt{Done: &wire.Done{Kind: wire.DoneRollback, Txn: int64(txn)}} //nolint:gosec // as above
	default: // commit and rollback with no transaction open
		rec = recorded(wire.NewError(wire.CodeInvalidState, "no transaction is open on the channel"), false)
	}
	var data []byte
	frames := false
	switch {
	case held != nil:
		data = held.b.Bytes()
		frames = len(data) > 0
	case sw != nil:
		data, frames = sw.kept.Bytes(), sw.n > 0
	}
	switch {
	case rec == nil:
		next = wire.TxnNone
		owned(lost, ch, seq).TxnState = next
	case rec.Done != nil:
		rec.Done.Seq, rec.Done.TxnState = seq, next
	default:
		owned(rec.Error, ch, seq).RequestID = id
		rec.Error.TxnState = next
	}
	s.record(ch, rec, lost, data, frames, next)
	settled = true
	if lost != nil {
		e := *lost
		return nil, &e
	}
	if rec.Error != nil {
		e := *rec.Error
		return nil, &e
	}
	done := *rec.Done
	if sw == nil { // streamed frames are sent already
		s.send(st, data)
	}
	return &done, nil
}

// record ends the running request of ch with its receipt, or with the
// unrecorded error lost, which ends the channel, and keeps data, the
// frames of its response before the terminal one, if they fit; frames says
// whether there were any. next is the channel's transaction state after it.
func (s *Server) record(ch *channel, rec *wire.Receipt, lost *wire.Error, data []byte, frames bool, next wire.TxnState) {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	ch.running, ch.idle, ch.state = false, time.Now(), next
	if next != wire.TxnAborted {
		ch.cause = ""
	}
	if lost != nil {
		ch.ended = true
	} else {
		if rec.Error != nil {
			rec.Error.Message = wire.CutMessage(rec.Error.Message, int(s.limits.ErrorMessageBytes))
		}
		ch.receipt, ch.frames = rec, frames
		n := int64(len(data))
		t := ch.t
		if rec.Done != nil && n > 0 && n <= s.limits.RetainedRequestBytes && t.replay+n <= s.chl.replay && s.replay+n <= s.chl.replay {
			ch.data = bytes.Clone(data)
			t.replay += n
			s.replay += n
		}
	}
	if ch.tx != nil && closed(ch.tx.c.ec.Aborted()) {
		// The engine ended the transaction as the request finished; watch
		// left it to this request.
		s.abortTxn(ch, causeMaintenance)
	}
	if ch.ended || ch.closing {
		s.end(ch)
	}
	ch.run.receipt, ch.run.lost, ch.run.state = rec, lost, ch.state
	if ch.ended {
		ch.run.state = wire.TxnNone
	}
	close(ch.run.finished)
}

// closed reports whether c is closed.
func closed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// dropTxn detaches ch's open transaction, if any, with s.chMu held, and
// returns the function that ends it: it closes the connection, which rolls
// back what is not committed, then releases the write lock.
func (s *Server) dropTxn(ch *channel) func() {
	tx, t, who, ro := ch.tx, ch.t, ch.who, ch.ro
	if tx == nil {
		return func() {}
	}
	ch.tx = nil
	return func() {
		s.pool.release(who, tx.c, false)
		if ro {
			s.pool.unholdTxn(who)
		} else {
			s.release(t)
		}
	}
}

// endTxn ends ch's open transaction from the request running on ch.
func (s *Server) endTxn(ch *channel) {
	s.chMu.Lock()
	drop := s.dropTxn(ch)
	s.chMu.Unlock()
	drop()
}

// abortTxn moves ch, which runs nothing, to the aborted state, with s.chMu
// held: its transaction is rolled back and the write lock released, and
// its next request other than rollback is answered transaction_aborted.
func (s *Server) abortTxn(ch *channel, cause string) {
	ch.state, ch.cause = wire.TxnAborted, cause
	if !ch.closing { // a waiting close drops it before its answer
		go s.dropTxn(ch)()
	}
}

// watch aborts tx, ch's open transaction, if the engine ends it while no
// request runs on ch: maintenance rolled it back, or the leader stopped
// (ADR 0005, requirement 15). The connection's close ends the wait.
func (s *Server) watch(ch *channel, tx *txn) {
	<-tx.c.ec.Aborted()
	s.chMu.Lock()
	defer s.chMu.Unlock()
	if ch.tx == tx && !ch.running {
		s.abortTxn(ch, causeMaintenance)
	}
}

// cancelRequest serves a cancel (ADR 0005 decision 6 and the channel state
// machine). A cancel of the next sequence number records it as cancelled
// before it arrived; one of the running request interrupts it before its
// commit point and answers once it has finished, with its receipt, and on a
// closing channel once the close has rolled back its transaction.
func (s *Server) cancelRequest(d *database, leader *engine.Leader, req *wire.Request, who, id string) (*wire.Done, *wire.Error) {
	t := s.table(d, leader)
	if t == nil {
		return nil, wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	}
	s.chMu.Lock()
	defer s.chMu.Unlock()
	ch, e := s.lookup(t, req.Channel, who)
	if e != nil {
		return nil, e
	}
	seq := req.Seq
	switch {
	case ch.closing && seq != ch.last:
		return nil, owned(wire.NewError(wire.CodeChannelLost, "the channel is closing"), ch, seq)
	case seq < ch.last:
		return nil, owned(wire.NewError(wire.CodeStaleSequence, fmt.Sprintf("the sequence number is below the last accepted, %d", ch.last)), ch, seq)
	case seq > ch.last+1:
		return nil, owned(wire.NewError(wire.CodeSequenceGap, fmt.Sprintf("the sequence number is above the last accepted plus one, %d", ch.last+1)), ch, seq)
	case seq == ch.last+1 && ch.running:
		return nil, owned(wire.NewError(wire.CodeChannelBusy, "another request on the channel is running"), ch, seq)
	case seq == ch.last+1:
		ce := owned(wire.NewError(wire.CodeCancelled, "cancelled before it arrived").Recorded(false), ch, seq)
		ce.RequestID = id
		ch.last, ch.sum, ch.precancelled = seq, [sha256.Size]byte{}, true
		ch.receipt, ch.frames, ch.idle = &wire.Receipt{Error: ce}, false, time.Now()
		s.dropData(ch)
	case ch.running:
		ch.cancel(errCancelled) // a close's interruption, if one came first, stands
		r := ch.run             // the next request may be accepted before this wakes
		s.chMu.Unlock()
		<-r.finished
		s.chMu.Lock()
		if shut := ch.shut; shut != nil {
			s.chMu.Unlock()
			<-shut
			s.chMu.Lock()
		}
		if r.lost != nil {
			e := *r.lost
			e.Seq = seq
			return nil, &e
		}
		o := *r.receipt
		return &wire.Done{Kind: wire.DoneCancel, Seq: seq, TxnState: r.state, Outcome: &o}, nil
	}
	r := *ch.receipt
	return &wire.Done{Kind: wire.DoneCancel, Seq: seq, TxnState: ch.state, Outcome: &r}, nil
}

// streamed writes a transaction's frames to the response as they are
// produced, and keeps them for replay while they fit in limit. A response
// that cannot be written stops being written; the statement goes on.
type streamed struct {
	st       *stream
	kept     bytes.Buffer
	limit, n int64
	deadline time.Duration // for each frame's write
	failed   bool
}

func (w *streamed) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	if !w.failed {
		_ = w.st.rc.SetWriteDeadline(time.Now().Add(w.deadline))
		_, err := w.st.Write(p)
		w.failed = err != nil
	}
	if int64(w.kept.Len()+len(p)) <= w.limit {
		w.kept.Write(p)
	} else {
		w.limit = -1 // kept whole or not at all
		w.kept.Reset()
	}
	return len(p), nil
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
// until that request has finished and the first close has rolled back its
// transaction; every close and cancel that joined it answers after that.
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
		r, shut, owner := ch.run, ch.shut, ch.shut == nil
		if owner { // this close rolls back the transaction, then wakes the other waiters
			shut = make(chan struct{})
			ch.closing, ch.shut = true, shut
			defer close(shut) // after drop
		}
		ch.cancel(errClosed) // the first interruption stands
		s.chMu.Unlock()
		<-r.finished
		if !owner {
			<-shut
		}
		s.chMu.Lock()
	}
	drop := s.dropTxn(ch) // an open transaction is rolled back before the answer
	defer drop()
	defer s.chMu.Unlock()
	s.end(ch)
	if r := ch.run; r != nil && r.lost != nil {
		e := *r.lost
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
// t's leader, for a write that holds t's write lock (ADR 0005 decision 9),
// waiting for it ahead of the stateless requests for at most the pool's
// maxWait, and only while stateless requests are turning those connections
// over: the lock is held throughout, so a wait for a connection that is not
// coming would stall every writer of the database. A write that gets none
// returns its receipt, a recorded overloaded when the pod's or who's
// connections stayed in use, or the unrecorded error that ends the channel.
func (s *Server) writeConn(ctx context.Context, d *database, t *table, who string, limit time.Duration, log *slog.Logger) (*pconn, *wire.Receipt, *wire.Error) {
	_, evict, ok := s.pool.await(ctx, nil, who, true, false)
	if !ok {
		e := wire.NewError(wire.CodeOverloaded, "the server's or the principal's SQLite connections are all in use; retry later")
		switch {
		case cancelled(ctx):
			e = wire.NewError(wire.CodeCancelled, "cancelled before it ran")
		case ctx.Err() != nil:
			e = s.timeout(ctx, limit)
		}
		return nil, &wire.Receipt{Error: e.Recorded(false)}, nil
	}
	if evict != nil {
		_ = evict.ec.Close()
	}
	c, err := s.open(ctx, t.leader)
	if err != nil {
		s.pool.release(who, nil, false)
		rec, lost := s.failure(ctx, d, t, err, nil, limit, false, log)
		return nil, rec, lost
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

// execute runs an accepted execute as an autocommit write on a fresh
// connection it closes, within limit from now, holding its frames in held,
// which it empties unless the write committed or changed nothing. It
// returns the receipt, or the unrecorded error that ends the channel.
func (s *Server) execute(ctx context.Context, d *database, t *table, req *wire.Request, held *heldRows, deadline time.Time, limit time.Duration, who string, log *slog.Logger) (*wire.Receipt, *wire.Error) {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if e := s.lock(ctx, t, who, limit); e != nil {
		return &wire.Receipt{Error: e}, nil
	}
	defer s.release(t)
	c, rec, lost := s.writeConn(ctx, d, t, who, limit, log)
	if c == nil {
		return rec, lost
	}
	defer s.pool.release(who, c, false) // never reused after a write (ADR 0005 decision 6)
	var done *wire.Done
	var e *wire.Error
	var snap uint64
	txn, err := c.ec.Run(ctx, commitID(req.Channel, req.Seq), func(db *sqlite3.Conn) error {
		var ferr error
		done, e, ferr = s.write(db, c, req, &req.Statement, true, true, held, &snap)
		return ferr
	})
	switch {
	case txn != 0:
		// Committed, whatever failed after the commit point.
		if err != nil {
			log.LogAttrs(ctx, slog.LevelWarn, "write committed, then failed", slog.String("class", engineClass(err)))
		}
		done.Txn = int64(txn) //nolint:gosec // as in openChannel
		return &wire.Receipt{Done: done}, nil
	case err == nil:
		done.Txn = int64(snap) //nolint:gosec // as above; the write changed no page
		return &wire.Receipt{Done: done}, nil
	}
	held.free() // nothing committed: no row is sent
	return s.failure(ctx, d, t, err, e, limit, false, log)
}

// begin runs an accepted begin: it waits for the write lock, then runs
// BEGIN IMMEDIATE and the first statement, if any, on a fresh connection,
// with its frames written to w. A transaction that began holds the
// connection and the lock as ch's open transaction; a begin that fails
// leaves neither, and no transaction (ADR 0005 decision 6).
func (s *Server) begin(ctx context.Context, d *database, t *table, ch *channel, req *wire.Request, w io.Writer, deadline time.Time, limit time.Duration, log *slog.Logger) (*wire.Receipt, *wire.Error, wire.TxnState) {
	began := time.Now()
	ctx, cancel := s.txnContext(ctx, ch, began, deadline)
	defer cancel()
	var c *pconn
	open, locked := false, false
	defer func() {
		if !open {
			if c != nil {
				s.pool.release(ch.who, c, false) // rolls back what began
			}
			if locked {
				s.release(t)
			}
		}
	}()
	if e := s.lock(ctx, t, ch.who, limit); e != nil {
		return &wire.Receipt{Error: e}, nil, wire.TxnNone
	}
	locked = true
	c, rec, lost := s.writeConn(ctx, d, t, ch.who, limit, log)
	if c == nil {
		return rec, lost, wire.TxnNone
	}
	var done *wire.Done
	var e *wire.Error
	var snap uint64
	auto := false
	_, err := c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		var ferr error
		done, e, ferr = s.write(db, c, req, req.Execute, true, false, w, &snap)
		auto = db.GetAutocommit()
		return ferr
	})
	if err != nil {
		rec, lost := s.failure(ctx, d, t, err, e, limit, false, log)
		return rec, lost, wire.TxnNone
	}
	if done == nil {
		done = &wire.Done{Kind: wire.DoneBegin}
	}
	done.Txn = int64(snap) //nolint:gosec // as in openChannel
	if auto {
		// The first statement ran but the transaction is gone (ADR 0007
		// requirement 8); the deferred release frees c and the lock.
		return &wire.Receipt{Done: done}, nil, wire.TxnAborted
	}
	tx := &txn{c: c, snap: snap, began: began}
	s.chMu.Lock()
	ch.tx = tx
	s.chMu.Unlock()
	open = true
	go s.watch(ch, tx)
	return &wire.Receipt{Done: done}, nil, wire.TxnOpen
}

// inTxn runs an accepted execute inside tx, ch's open transaction, with its
// frames written to w. It returns the receipt, or the unrecorded error
// that ends the channel, and the transaction state after it: aborted if
// SQLite rolled the transaction back, the engine ended it, or the
// transaction lifetime was reached, which roll back and release it.
func (s *Server) inTxn(ctx context.Context, d *database, t *table, ch *channel, tx *txn, req *wire.Request, w io.Writer, deadline time.Time, limit time.Duration, log *slog.Logger) (*wire.Receipt, *wire.Error, wire.TxnState) {
	ctx, cancel := s.txnContext(ctx, ch, tx.began, deadline)
	defer cancel()
	var done *wire.Done
	var e *wire.Error
	auto := false
	_, err := tx.c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		var ferr error
		done, e, ferr = s.write(db, tx.c, req, &req.Statement, false, false, w, nil)
		auto = db.GetAutocommit() // SQLite rolls back the whole transaction after some errors
		return ferr
	})
	if err == nil && !auto {
		done.Txn = int64(tx.snap) //nolint:gosec // as in openChannel
		return &wire.Receipt{Done: done}, nil, wire.TxnOpen
	}
	if err == nil {
		// The statement ran but the transaction is gone (ADR 0007
		// requirement 8): what it did is not the client's to commit.
		done.Txn = int64(tx.snap) //nolint:gosec // as above
		s.chMu.Lock()
		drop := s.dropTxn(ch)
		s.chMu.Unlock()
		drop()
		return &wire.Receipt{Done: done}, nil, wire.TxnAborted
	}
	// A failed describe ran nothing; a failed statement may have.
	rec, lost := s.failure(ctx, d, t, err, e, limit, req.Op != wire.OpDescribe, log)
	maint := errors.Is(err, engine.ErrRetry) || errors.Is(err, engine.ErrTailBound)
	if lost == nil && !auto && !maint && !errors.Is(err, engine.ErrBroken) && !errors.Is(context.Cause(ctx), errLifetime) {
		return rec, nil, wire.TxnOpen
	}
	s.chMu.Lock()
	drop := s.dropTxn(ch)
	if maint {
		ch.cause = causeMaintenance
	}
	s.chMu.Unlock()
	drop()
	if lost != nil {
		return nil, lost, wire.TxnNone
	}
	return rec, nil, wire.TxnAborted
}

// commit runs an accepted commit of tx, ch's open transaction, and ends the
// transaction whatever its outcome: a failed commit rolls it back, SQLite's
// deferred foreign key failure included (ADR 0005 decision 6). It returns
// the receipt, or the unrecorded error that ends the channel.
func (s *Server) commit(ctx context.Context, d *database, t *table, ch *channel, tx *txn, req *wire.Request, deadline time.Time, limit time.Duration, log *slog.Logger) (*wire.Receipt, *wire.Error) {
	ctx, cancel := s.txnContext(ctx, ch, tx.began, deadline)
	defer cancel()
	var e *wire.Error
	txn, err := tx.c.ec.Run(ctx, commitID(req.Channel, req.Seq), func(db *sqlite3.Conn) error {
		var ferr error
		_, e, ferr = s.write(db, tx.c, req, nil, false, true, nil, nil)
		return ferr
	})
	s.endTxn(ch)
	switch {
	case txn != 0:
		// Committed, whatever failed after the commit point.
		if err != nil {
			log.LogAttrs(ctx, slog.LevelWarn, "commit committed, then failed", slog.String("class", engineClass(err)))
		}
		return &wire.Receipt{Done: &wire.Done{Kind: wire.DoneCommit, Txn: int64(txn), Wrote: true}}, nil //nolint:gosec // as in openChannel
	case err == nil: // the transaction wrote nothing
		return &wire.Receipt{Done: &wire.Done{Kind: wire.DoneCommit, Txn: int64(tx.snap)}}, nil //nolint:gosec // as above
	}
	return s.failure(ctx, d, t, err, e, limit, false, log)
}

// txnLimits are the idle limit and the lifetime of a write transaction, or
// of a read-only one if ro.
func (s *Server) txnLimits(ro bool) (idle, life time.Duration) {
	if ro {
		return time.Duration(s.limits.ReadTxnIdleMS) * time.Millisecond, time.Duration(s.limits.ReadTxnLifetimeMS) * time.Millisecond
	}
	return time.Duration(s.limits.WriteTxnIdleMS) * time.Millisecond, time.Duration(s.limits.WriteTxnLifetimeMS) * time.Millisecond
}

// txnContext bounds a request of a transaction of ch that began at began:
// by the statement deadline, and by the transaction lifetime, which is its
// context's cause when it ends it.
func (s *Server) txnContext(ctx context.Context, ch *channel, began, deadline time.Time) (context.Context, context.CancelFunc) {
	_, life := s.txnLimits(ch.ro)
	cause := errLifetime
	if ch.ro {
		cause = errReadLifetime
	}
	lctx, lcancel := context.WithDeadlineCause(ctx, began.Add(life), cause)
	dctx, dcancel := context.WithDeadline(lctx, deadline)
	return dctx, func() { dcancel(); lcancel() }
}

// lock waits for t's write lock for who within the lock wait and ctx, and
// returns the recorded failure if it is not granted.
func (s *Server) lock(ctx context.Context, t *table, who string, limit time.Duration) *wire.Error {
	lctx, lcancel := context.WithTimeout(ctx, time.Duration(s.limits.LockWaitMS)*time.Millisecond)
	err := s.acquire(lctx, t, who)
	lcancel()
	switch {
	case err == nil:
		return nil
	case cancelled(ctx):
		return wire.NewError(wire.CodeCancelled, "cancelled before it ran").Recorded(false)
	case ctx.Err() != nil:
		return s.timeout(ctx, limit).Recorded(false)
	case errors.Is(err, errWaiters):
		return wire.NewError(wire.CodeBusy, "too many writers are waiting for the write lock").Recorded(false)
	}
	return wire.NewError(wire.CodeBusy, fmt.Sprintf("the write lock was not granted within %d ms", s.limits.LockWaitMS)).Recorded(false)
}

// cancelled reports whether a close or a cancel ended ctx.
func cancelled(ctx context.Context) bool {
	c := context.Cause(ctx)
	return errors.Is(c, errClosed) || errors.Is(c, errCancelled)
}

// timeout is the timeout error of a request whose ctx ended: the write
// transaction lifetime's, or the statement time limit's.
func (s *Server) timeout(ctx context.Context, limit time.Duration) *wire.Error {
	switch c := context.Cause(ctx); {
	case errors.Is(c, errReadLifetime):
		return wire.NewError(wire.CodeTimeout, fmt.Sprintf("the read-only transaction lifetime of %d ms was reached", s.limits.ReadTxnLifetimeMS))
	case errors.Is(c, errLifetime):
		return wire.NewError(wire.CodeTimeout, fmt.Sprintf("the write transaction lifetime of %d ms was reached", s.limits.WriteTxnLifetimeMS))
	}
	return timeoutError(limit)
}

// failure maps a Run of an accepted write that failed with err, and with e
// for the statement's own error if any, to its recorded failure, with the
// flags of a statement inside a transaction if txnStatement, or to the
// unrecorded error that ends the channel. ctx is the Run's.
func (s *Server) failure(ctx context.Context, d *database, t *table, err error, e *wire.Error, limit time.Duration, txnStatement bool, log *slog.Logger) (*wire.Receipt, *wire.Error) {
	recorded := func(e *wire.Error) *wire.Receipt { return &wire.Receipt{Error: e.Recorded(txnStatement)} }
	switch {
	case errors.Is(err, engine.ErrUnknown):
		log.LogAttrs(ctx, slog.LevelError, "write outcome unknown", slog.String("class", engineClass(err)))
		return nil, wire.NewError(wire.CodeCommitOutcomeUnknown, "the commit's outcome is unknown; the server stopped serving the database; resolve the request")
	case errors.Is(err, engine.ErrTooLarge):
		return recorded(wire.NewError(wire.CodeCommitFailed, "the transaction is too large for one commit or for the leader's local WAL bound")), nil
	case errors.Is(err, engine.ErrFenced):
		return recorded(wire.NewError(wire.CodeCommitFailed, "the leader was fenced by another leader's epoch")), nil
	case errors.Is(err, engine.ErrTailBound), errors.Is(err, engine.ErrRetry):
		return recorded(s.engineError(d, t.leader, err)), nil
	case errors.Is(err, engine.ErrBroken):
		log.LogAttrs(ctx, slog.LevelError, "write failed", slog.String("class", engineClass(err)))
		return recorded(wire.NewError(wire.CodeInternal, "the connection broke and was discarded")), nil
	case errors.Is(err, engine.ErrStopped), t.leader.Err() != nil:
		// The leader stopped: the channel has ended, and nothing is recorded.
		l := wire.NewError(wire.CodeChannelLost, "the leader stopped serving the database while the request ran; it did not commit")
		l.NotExecuted = false
		return nil, l
	case cancelled(ctx):
		return recorded(wire.NewError(wire.CodeCancelled, "cancelled before its commit point")), nil
	case errors.Is(context.Cause(ctx), errLifetime):
		return recorded(s.timeout(ctx, limit)), nil
	case e != nil:
		return recorded(e), nil
	case ctx.Err() != nil:
		return recorded(timeoutError(limit)), nil
	}
	log.LogAttrs(ctx, slog.LevelError, "write failed", slog.String("class", engineClass(err)))
	return recorded(wire.NewError(wire.CodeInternal, "the write failed")), nil
}

// write runs inside a Run: BEGIN IMMEDIATE if begin, setting snap to the
// head the transaction starts from; stmt of req, if not nil, with its frames
// written to w; then COMMIT if commit. A failure after a BEGIN IMMEDIATE
// or at a COMMIT rolls back, so that a failed autocommit write, begin or
// commit leaves nothing (ADR 0005 decision 6); inside a transaction a
// failed statement leaves what SQLite leaves.
func (s *Server) write(db *sqlite3.Conn, c *pconn, req *wire.Request, stmt *wire.Statement, begin, commit bool, w io.Writer, snap *uint64) (done *wire.Done, e *wire.Error, err error) {
	ictx := db.GetInterrupt()
	if begin {
		if err := c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("BEGIN IMMEDIATE") }); err != nil {
			return nil, nil, broken(err)
		}
		// No commit lands while this connection holds SQLite's write lock.
		*snap = c.leader.Head()
	}
	if stmt != nil {
		var out *wire.Writer
		out, err = wire.NewWriter(w, wire.DefaultFrameBytes, s.limits)
		switch {
		case err != nil:
		case req.Op == wire.OpBatch:
			done, err = batchOn(db, c, ictx, req, out, !commit)
		case req.Op == wire.OpDescribe:
			done, err = describeOn(c, ictx, stmt.SQL, out, !commit)
		default:
			done, err = statementOn(db, c, ictx, req, stmt, out, !commit)
		}
	}
	if err == nil && commit {
		err = c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("COMMIT") })
		if err == nil && s.afterCommit != nil {
			err = s.afterCommit()
		}
	}
	if err == nil {
		return done, nil, nil
	}
	var rerr error
	if begin || commit {
		rerr = c.pc.Privileged(func(db *sqlite3.Conn) error {
			if db.GetAutocommit() {
				return nil
			}
			return db.Exec("ROLLBACK")
		})
	}
	e, berr := entryError(ictx, err)
	if berr == nil && rerr != nil {
		// A connection that cannot roll back is not trusted to leave nothing.
		berr = fmt.Errorf("%w: rollback failed", engine.ErrBroken)
	}
	if berr != nil {
		return done, e, berr
	}
	return done, e, errEnded
}

// statementOn runs the client's statement stmt of req under the policy on
// c, writing its result to out; inTxn says it runs in the client's transaction.
func statementOn(db *sqlite3.Conn, c *pconn, ictx context.Context, req *wire.Request, stmt *wire.Statement, out *wire.Writer, inTxn bool) (done *wire.Done, err error) {
	total := db.TotalChanges()
	err = c.pc.Run(ictx, stmt.SQL, func(st *sqlite3.Stmt) error {
		// SAVEPOINT, RELEASE and ROLLBACK TO run inside the client's
		// transaction only (0007), not inside an autocommit write's own.
		if txnControl(stmt.SQL) && (!inTxn || !savepointSQL(stmt.SQL)) {
			return wire.NewError(wire.CodeInvalidRequest, "transaction control is not SQL in this protocol; use a channel's begin, commit and rollback")
		}
		if err := bind(req, stmt.Args, st); err != nil {
			return err
		}
		cols := columns(st)
		if len(cols) > 0 {
			if err := out.Columns(cols); err != nil {
				return err
			}
		}
		row := make([]any, len(cols))
		for st.Step() {
			for i := range row {
				row[i] = value(st, i)
			}
			if err := out.Row(row); err != nil {
				return err
			}
		}
		if err := st.Err(); err != nil {
			return err
		}
		var changes int64
		if db.TotalChanges() != total {
			changes = db.Changes()
		}
		done = &wire.Done{Kind: wire.DoneExecute, ReadOnly: st.ReadOnly(), RowsAffected: changes, LastInsertRowID: db.LastInsertRowID()}
		return nil
	})
	return done, err
}

// describe runs an accepted describe on an idle channel, on c, a pooled
// connection reserved before acceptance, with its frames written to w. It
// returns the receipt, or the unrecorded error that ends the channel, and
// whether c may go back to the pool.
func (s *Server) describe(ctx context.Context, d *database, t *table, c *pconn, req *wire.Request, w io.Writer, deadline time.Time, limit time.Duration, log *slog.Logger) (*wire.Receipt, *wire.Error, bool) {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var done *wire.Done
	var e *wire.Error
	trusted := false
	_, err := c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		ictx := db.GetInterrupt()
		out, err := wire.NewWriter(w, wire.DefaultFrameBytes, s.limits)
		if err == nil {
			done, err = describeOn(c, ictx, req.SQL, out, false)
		}
		if err == nil {
			return nil
		}
		var berr error
		e, berr = statementError(ictx, err)
		trusted = berr == nil // statementError's errEnded: not trusted again
		return cmp.Or(berr, errEnded)
	})
	if err == nil {
		done.Txn = int64(t.leader.Head()) //nolint:gosec // as in openChannel
		return &wire.Receipt{Done: done}, nil, true
	}
	rec, lost := s.failure(ctx, d, t, err, e, limit, false, log)
	return rec, lost, trusted && keepable(err)
}

// keepable reports whether a pooled connection whose Run failed with err may
// go back to the pool: the request recorded its own failure, and the engine
// neither broke nor refused the connection.
func keepable(err error) bool {
	keep := errors.Is(err, errEnded)
	for _, class := range []error{engine.ErrBroken, engine.ErrStopped, engine.ErrRetry, engine.ErrTailBound} {
		keep = keep && !errors.Is(err, class)
	}
	return keep
}

// entryError is statementError, with the failed entry of a batch named.
func entryError(ictx context.Context, err error) (*wire.Error, error) {
	e, berr := statementError(ictx, err)
	if be, ok := errors.AsType[*batchError](err); ok && e != nil {
		e.BatchIndex = &be.index
	}
	return e, berr
}

// startRead makes c refuse writes, with PRAGMA query_only, so that a write
// on a read-only channel fails with SQLITE_READONLY (ADR 0005 decision 5),
// begins a read transaction on it and returns the transaction number of its
// snapshot.
func startRead(c *pconn, ictx context.Context) (uint64, error) {
	if err := c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("PRAGMA query_only = ON; BEGIN") }); err != nil {
		return 0, broken(err)
	}
	snap, err := c.leader.Snapshot(ictx, func() error {
		return c.pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("PRAGMA main.schema_version") })
	})
	return snap, broken(err)
}

// readBegin runs an accepted begin on an idle read-only channel ch, on c, a
// pooled connection reserved before acceptance: a read transaction on a
// connection that refuses writes, then the first statement, if any, with
// its frames written to w. A transaction that began holds c as ch's open
// transaction, and *took is set with it; a begin that fails leaves no
// transaction, and c to be closed (ADR 0005 decision 6).
func (s *Server) readBegin(ctx context.Context, d *database, t *table, ch *channel, c *pconn, took *bool, req *wire.Request, w io.Writer, deadline time.Time, limit time.Duration, log *slog.Logger) (*wire.Receipt, *wire.Error, wire.TxnState) {
	began := time.Now()
	ctx, cancel := s.txnContext(ctx, ch, began, deadline)
	defer cancel()
	var done *wire.Done
	var e *wire.Error
	var snap uint64
	auto := false
	_, err := c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		ictx := db.GetInterrupt()
		var err error
		if snap, err = startRead(c, ictx); err == nil && req.Execute != nil {
			var out *wire.Writer
			if out, err = wire.NewWriter(w, wire.DefaultFrameBytes, s.limits); err == nil {
				done, err = statementOn(db, c, ictx, req, req.Execute, out, true)
			}
		}
		auto = db.GetAutocommit()
		if err == nil {
			return nil
		}
		var berr error
		e, berr = statementError(ictx, err)
		return cmp.Or(berr, errEnded)
	})
	if err != nil {
		rec, lost := s.failure(ctx, d, t, err, e, limit, false, log)
		return rec, lost, wire.TxnNone
	}
	if done == nil {
		done = &wire.Done{Kind: wire.DoneBegin}
	}
	done.Txn = int64(snap) //nolint:gosec // as in openChannel
	if auto {
		return &wire.Receipt{Done: done}, nil, wire.TxnAborted
	}
	tx := &txn{c: c, snap: snap, began: began}
	s.chMu.Lock()
	ch.tx, *took = tx, true // together: from here dropTxn alone releases c and its place
	s.chMu.Unlock()
	go s.watch(ch, tx)
	return &wire.Receipt{Done: done}, nil, wire.TxnOpen
}

// read runs an accepted execute or batch on an idle read-only channel, on c,
// a pooled connection reserved before acceptance, in a read transaction of
// its own on a connection that refuses writes, with its frames written to
// w. It returns the receipt, or the unrecorded error that ends the channel,
// and whether c may go back to the pool.
func (s *Server) read(ctx context.Context, d *database, t *table, c *pconn, req *wire.Request, w io.Writer, deadline time.Time, limit time.Duration, log *slog.Logger) (*wire.Receipt, *wire.Error, bool) {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var done *wire.Done
	var e *wire.Error
	var snap uint64
	clean, trusted := false, false
	_, err := c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		ictx := db.GetInterrupt()
		var err error
		if snap, err = startRead(c, ictx); err == nil {
			var out *wire.Writer
			out, err = wire.NewWriter(w, wire.DefaultFrameBytes, s.limits)
			switch {
			case err != nil:
			case req.Op == wire.OpBatch:
				done, err = batchOn(db, c, ictx, req, out, false)
			default:
				done, err = statementOn(db, c, ictx, req, &req.Statement, out, false)
			}
		}
		// The server's ROLLBACK ends the read, and the connection takes
		// writes again before it is pooled; one it fails on, as after an
		// interrupt, is closed instead, which ends the read too.
		clean = c.pc.Privileged(func(db *sqlite3.Conn) error {
			if !db.GetAutocommit() {
				if err := db.Exec("ROLLBACK"); err != nil {
					return err
				}
			}
			return db.Exec("PRAGMA query_only = OFF")
		}) == nil
		if err == nil {
			return nil
		}
		var berr error
		e, berr = entryError(ictx, err)
		trusted = berr == nil // as in describe
		return cmp.Or(berr, errEnded)
	})
	if err == nil {
		done.Txn = int64(snap) //nolint:gosec // as in openChannel
		return &wire.Receipt{Done: done}, nil, clean
	}
	rec, lost := s.failure(ctx, d, t, err, e, limit, false, log)
	return rec, lost, clean && trusted && keepable(err)
}

// describeOn writes the result columns and the parameter names of sql,
// compiled under the policy on c and never run, to out; inTxn says it is
// described inside the client's transaction, where SAVEPOINT, RELEASE and
// ROLLBACK TO are statements.
func describeOn(c *pconn, ictx context.Context, sql string, out *wire.Writer, inTxn bool) (done *wire.Done, err error) {
	err = c.pc.Run(ictx, sql, func(st *sqlite3.Stmt) error {
		if txnControl(sql) && (!inTxn || !savepointSQL(sql)) {
			return wire.NewError(wire.CodeInvalidRequest, "transaction control is not SQL in this protocol; use a channel's begin, commit and rollback")
		}
		names := make([]string, st.BindCount())
		for i := range names {
			names[i] = st.BindName(i + 1)
		}
		if err := out.Columns(columns(st)); err != nil {
			return err
		}
		if err := out.Params(names); err != nil {
			return err
		}
		done = &wire.Done{Kind: wire.DoneChannelDescribe, ParamCount: int64(len(names))}
		return nil
	})
	return done, err
}

// batchError is the failure of a batch's entry index.
type batchError struct {
	index int64
	err   error
}

func (e *batchError) Error() string { return fmt.Sprintf("batch entry %d: %v", e.index, e.err) }
func (e *batchError) Unwrap() error { return e.err }

// batchOn runs the entries of batch req on c in order under the policy,
// stopping at the first that fails, and writes the results of those that
// ran to out: every entry's, or, inTxn, after a failure, those of the
// entries before it, whose effects stay in the client's transaction (ADR
// 0005 decision 6). An entry may return no columns. A failure is a
// *batchError naming its entry; a statement of param_sets that does not
// compile fails at entry 0.
func batchOn(db *sqlite3.Conn, c *pconn, ictx context.Context, req *wire.Request, out *wire.Writer, inTxn bool) (*wire.Done, error) {
	var results []wire.Result
	var total int64
	// check runs before anything is bound or stepped.
	check := func(st *sqlite3.Stmt, sql string) error {
		switch {
		case txnControl(sql) && (!inTxn || !savepointSQL(sql)):
			return wire.NewError(wire.CodeInvalidRequest, "transaction control is not SQL in this protocol; use a channel's begin, commit and rollback")
		case st.ColumnCount() > 0:
			return wire.NewError(wire.CodeInvalidRequest, "a batch entry must return no columns")
		}
		return nil
	}
	entry := func(st *sqlite3.Stmt, a wire.Args) error {
		before := db.TotalChanges()
		if err := bind(req, a, st); err != nil {
			return err
		}
		for st.Step() { // no columns, so no rows
		}
		if err := st.Err(); err != nil {
			return err
		}
		var changes int64
		if db.TotalChanges() != before {
			changes = db.Changes()
		}
		results = append(results, wire.Result{RowsAffected: changes, LastInsertRowID: db.LastInsertRowID()})
		total += changes
		return nil
	}
	var err error
	if req.Statements == nil {
		err = c.pc.Run(ictx, req.SQL, func(st *sqlite3.Stmt) error {
			if err := check(st, req.SQL); err != nil {
				return err
			}
			for i, a := range req.ParamSets {
				if i > 0 {
					if err := errors.Join(st.Reset(), st.ClearBindings()); err != nil {
						return err
					}
				}
				if err := entry(st, a); err != nil {
					return err
				}
			}
			return nil
		})
	} else {
		for _, s := range req.Statements {
			if err = c.pc.Run(ictx, s.SQL, func(st *sqlite3.Stmt) error {
				if err := check(st, s.SQL); err != nil {
					return err
				}
				return entry(st, s.Args)
			}); err != nil {
				break
			}
		}
	}
	if err != nil {
		err = &batchError{index: int64(len(results)), err: err}
	}
	if (err == nil || inTxn) && len(results) > 0 {
		if werr := out.Results(results); err == nil {
			err = werr
		}
	}
	if err != nil {
		return nil, err
	}
	return &wire.Done{Kind: wire.DoneBatch, RowsAffected: total, LastInsertRowID: db.LastInsertRowID(), Entries: int64(len(results))}, nil
}
