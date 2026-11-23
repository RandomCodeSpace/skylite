package skylite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// The driver's own waits on a write channel, from ADR 0005's defaults.
const (
	// statementTime is ADR 0005's default statement time limit.
	statementTime = 30 * time.Second
	// resendWindow bounds one request on a channel, its resolve included: T
	// of ADR 0005 decision 7, the statement limit (30 s), the engine's
	// commit bound (20 s) and the response deadline (10 s).
	resendWindow = 60 * time.Second
	// commitWait bounds a Commit whose BeginTx context had no deadline, and
	// every Rollback: the statement time, which covers a commit's way to its
	// commit point and the engine's 20 s bound on learning its outcome.
	commitWait = statementTime
	// resolveWait is ADR 0005's driver wait for a resolution.
	resolveWait = 30 * time.Second
	// channelIdle is ADR 0005 decision 4's reopening rule: a channel with
	// no answer for longer is replaced before its next use, outside a
	// transaction.
	channelIdle = 60 * time.Second
	// abandonWait bounds the cancel and the close sent for a channel the
	// driver stops using.
	abandonWait = 5 * time.Second
	// abandonLimit bounds the cancels and closes a Connector sends in the
	// background at once.
	abandonLimit = 8
	// reissues bounds the reissues of a read-only statement of a transaction
	// whose replay omits its rows.
	reissues = 3
)

// channel is a connection's write channel.
type channel struct {
	token, id string
	epoch     int64
	seq       int64         // the last sequence number sent
	last      time.Time     // when the last answer arrived
	state     wire.TxnState // the transaction state of the last receipt
	limits    wire.Limits   // RequestBodyBytes and ResultBytes, as advertised
}

// answer is a channel request's whole response.
type answer struct {
	cols []wire.Column
	rows [][]driver.Value
	done *wire.Done
}

// do sends a sequenced request on the connection's channel, opening one
// first if needed, and reads its whole answer, keeping its rows if keep. A
// delivery without a settled answer, a transport failure, a cut or
// malformed response or a refusal before acceptance, is sent again with the
// same sequence number and body, which the server answers from its record
// (ADR 0005 decisions 2 and 3). A recorded transient refusal of a begin or
// an autocommit write (busy, overloaded or maintenance), which ran nothing,
// is sent again as a new request under the next sequence number. Every
// pause grows from 100 ms to 2 s, with jitter. by bounds the whole request:
// its token, its deliveries, their bodies and its resolve.
//
// After channel_lost or commit_outcome_unknown, an autocommit write or a
// commit is resolved (ADR 0005 decision 4): committed is a success, or for
// a write the committed, result-unavailable error; not_committed sends a
// write once more on a new channel as a new request, and fails a commit
// with ErrTransactionLost. Everything else leaves the outcome unknown: an
// unsettled resolve, any other request lost with its channel, a receipt
// that contradicts the request or itself (read-only with rows affected),
// an ended context and the end of by. A request refused with behind is sent
// again on a new channel once the close of the old one proves it was never
// accepted.
func (cn *conn) do(ctx context.Context, by time.Time, call *wire.Call, kind wire.DoneKind, keep bool) (*answer, error) {
	if err := cn.usable(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dctx, cancel := context.WithDeadline(ctx, by)
	if dl, ok := ctx.Deadline(); ok && !dl.After(by) { // ctx must end first, so that the error matches it
		cancel()
		dctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	var body []byte
	var last *wire.Error              // the last refusal, for the unknown outcome's error
	n, sent, again := 0, false, false // again: a write resolve proved not committed was sent once more
	for {
		if !sent {
			var err error
			if body, err = cn.prepare(dctx, call); err != nil {
				return nil, err // nothing of this request was accepted
			}
		}
		req, err := cn.c.request(dctx, wire.KindChannel, body)
		if !sent {
			if err == nil {
				err = dctx.Err() // ended before the first delivery
			}
			if err != nil {
				return nil, err // nothing sent, and the sequence number unused
			}
			cn.ch.seq, sent = call.Seq, true
		}
		var a *answer
		var e *wire.Error
		if err == nil {
			a, e, err = cn.deliver(req, kind, call.Seq, keep)
		}
		switch {
		case err != nil:
		case e == nil:
			if a.done.TxnState != cn.after(call.Op) || a.done.ReadOnly && a.done.RowsAffected > 0 {
				return nil, cn.unknown(ctx, call, nil)
			}
			cn.ch.last, cn.ch.state = time.Now(), a.done.TxnState
			cn.c.raise(a.done.Txn)
			return a, nil
		case e.Code == CodeChannelLost || e.Code == CodeCommitOutcomeUnknown:
			if !cn.resolvable(call) || ctx.Err() != nil {
				return nil, cn.unknown(ctx, call, e)
			}
			res, txn := cn.resolve(dctx, call)
			switch {
			case res == wire.ResolutionCommitted && call.Op == wire.OpCommit:
				return &answer{done: &wire.Done{Kind: wire.DoneCommit, Seq: call.Seq, TxnState: wire.TxnNone, Txn: txn, Wrote: true}}, nil
			case res == wire.ResolutionCommitted:
				return nil, &Error{
					Code: e.Code, Message: "the channel was lost and resolve found the write committed; its result is unavailable",
					Seq: call.Seq, Txn: txn, committed: true, is: []error{ErrResultUnavailable},
				}
			case res == wire.ResolutionNotCommitted && call.Op == wire.OpCommit:
				return nil, &Error{
					Code: e.Code, NotExecuted: true, Final: true, Seq: call.Seq,
					Message: "the channel was lost and resolve proved the commit did not happen: the transaction was rolled back",
					is:      []error{ErrTransactionLost},
				}
			case res == wire.ResolutionNotCommitted && !again && time.Now().Before(by):
				again, sent, last = true, false, nil // proven without effect: a new request, on a new channel
				continue
			case res == wire.ResolutionNotCommitted:
				return nil, &Error{
					Code: e.Code, NotExecuted: true, Final: true, Seq: call.Seq,
					Message: "the channel was lost and resolve proved the write did not commit; it had no effect",
				}
			}
			ue := cn.unknown(ctx, call, e)
			ue.Message += "; resolve answered " + string(res)
			return nil, ue
		case e.Final:
			if e.Seq != call.Seq || e.ChannelID != cn.ch.id || !cn.failedIn(call.Op, e.TxnState) {
				return nil, cn.unknown(ctx, call, nil)
			}
			cn.ch.last, cn.ch.state = time.Now(), e.TxnState
			cn.c.raise(e.Txn)
			if !transient(e) || call.Op != wire.OpBegin && (call.Op != wire.OpExecute || cn.inTx) {
				return nil, serverError(e)
			}
			sent, last = false, e // it ran nothing: a new request, under the next sequence number
		case e.Code == CodeBehind && call.MinTxn > 0 && cn.closeProves(dctx, call.Seq):
			sent = false // proven never accepted: send it again on a new channel
			last = e
		default:
			last = e
		}
		if !backoff(dctx, by, n) {
			return nil, cn.giveUp(ctx, call, last, sent)
		}
		n++
	}
}

// pause is the wait after the n-th refusal or failure of a request, from
// 0: 100 ms doubling to 2 s, less a random part of up to half, so that
// clients refused together come back apart.
func pause(n int) time.Duration {
	d := min(100*time.Millisecond<<min(n, 5), 2*time.Second)
	return d - rand.N(d/2+1) //nolint:gosec // jitter, not a secret
}

// backoff waits pause(n) and reports true, or reports false at once when
// the pause would pass by, if set, or ctx's deadline, and as soon as ctx
// ends.
func backoff(ctx context.Context, by time.Time, n int) bool {
	d := pause(n)
	if dl, ok := ctx.Deadline(); ok && (by.IsZero() || dl.Before(by)) {
		by = dl
	}
	if !by.IsZero() && time.Now().Add(d).After(by) {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// transient reports whether e refused a request without running it, for a
// reason that passes: a bound or a lock that frees, a leader or a pod that
// comes back, a follower that catches up, or maintenance that ends (ADR 0005
// decisions 4 and 9).
func transient(e *wire.Error) bool {
	switch e.Code {
	case CodeOverloaded, CodeBusy, CodeTooManyChannels, CodeNoLeader, CodeNotServing, CodeOwnerUnreachable, CodeBehind, CodeMaintenance:
		return e.NotExecuted
	}
	return false
}

// resolvable reports whether call is a request whose effect resolve can
// find: a commit or an autocommit execute, on a write channel.
func (cn *conn) resolvable(call *wire.Call) bool {
	return !cn.ro && (call.Op == wire.OpCommit || call.Op == wire.OpExecute && !cn.inTx)
}

// resolve settles call, whose channel was lost, within ctx and resolveWait.
// When the answer settles it, committed or not_committed, the channel,
// which has ended, is dropped, and after a commit the connection is
// discarded, as ADR 0005 decision 4 asks whatever the answer.
func (cn *conn) resolve(ctx context.Context, call *wire.Call) (wire.Resolution, int64) {
	rctx, cancel := context.WithTimeout(ctx, resolveWait)
	defer cancel()
	res, txn := cn.c.resolve(rctx, cn.ch.token, call.Seq)
	cn.c.raise(txn) // the commit, or the leader's head
	if res == wire.ResolutionCommitted || res == wire.ResolutionNotCommitted {
		cn.ch = nil
		cn.lost = cn.lost || call.Op == wire.OpCommit
	}
	return res, txn
}

// resolve asks the leader whether sequence number seq of the channel of
// token committed, and asks again after pending, a transient refusal or a
// failure, until it answers committed, not_committed or unknown or ctx
// ends. It returns the last resolution, unknown if none came, and its txn.
func (c *Connector) resolve(ctx context.Context, token string, seq int64) (wire.Resolution, int64) {
	res, txn := wire.ResolutionUnknown, int64(0)
	body, err := wire.AppendCall(nil, wire.KindResolve, &wire.Call{Channel: token, Seq: seq})
	if err != nil {
		return res, txn
	}
	for n := 0; ; n++ {
		f, err := c.post(ctx, wire.KindResolve, body, wire.DoneResolve)
		switch {
		case err != nil:
		case f.Kind == wire.FrameError:
			if !transient(f.Error) {
				return res, txn
			}
		case f.Done.Resolution != wire.ResolutionPending:
			return f.Done.Resolution, f.Done.Txn
		default:
			res, txn = wire.ResolutionPending, f.Done.Txn
		}
		if !backoff(ctx, time.Time{}, n) {
			return res, txn
		}
	}
}

// post sends a request whose answer is one terminal frame, and returns it.
func (c *Connector) post(ctx context.Context, kind wire.Kind, body []byte, done wire.DoneKind) (*wire.Frame, error) {
	req, err := c.request(ctx, kind, body)
	if err != nil {
		return nil, err
	}
	rs, err := c.roundTrip(req, done)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rs.body.Close() }()
	return rs.next()
}

// prepare readies the channel, gives call its sequence number and returns
// its body, refused locally when it passes the channel's request body limit.
func (cn *conn) prepare(ctx context.Context, call *wire.Call) ([]byte, error) {
	if err := cn.ready(ctx); err != nil {
		return nil, err
	}
	ch := cn.ch
	call.Channel, call.Seq = ch.token, ch.seq+1
	if call.Op == wire.OpBegin || call.Op == wire.OpExecute && !cn.inTx {
		call.MinTxn = cn.c.watermark.Load()
	}
	body, err := wire.AppendCall(nil, wire.KindChannel, call)
	if err != nil {
		return nil, fmt.Errorf("skylite: %w", err)
	}
	if n := int64(len(body)); n > ch.limits.RequestBodyBytes {
		return nil, fmt.Errorf("skylite: the request body of %d bytes passes the channel's request_body_bytes of %d; nothing was sent", n, ch.limits.RequestBodyBytes)
	}
	return body, nil
}

// usable refuses a request on a closed or discarded connection before
// anything is sent.
func (cn *conn) usable() error {
	switch {
	case cn.lost && cn.inTx:
		return &Error{Message: "the transaction's channel was lost; nothing was sent", is: []error{ErrTransactionLost}}
	case cn.closed || cn.lost:
		return driver.ErrBadConn // nothing sent
	}
	return nil
}

// after is the transaction state a success of op leaves.
func (cn *conn) after(op wire.Op) wire.TxnState {
	if op == wire.OpBegin || op == wire.OpExecute && cn.inTx {
		return wire.TxnOpen
	}
	return wire.TxnNone
}

// failedIn reports whether a recorded failure of op can leave state.
func (cn *conn) failedIn(op wire.Op, state wire.TxnState) bool {
	switch {
	case op == wire.OpExecute && cn.inTx:
		return state == wire.TxnOpen || state == wire.TxnAborted
	case op == wire.OpCommit || op == wire.OpRollback:
		return state == wire.TxnNone || state == wire.TxnOpen || state == wire.TxnAborted
	}
	return state == wire.TxnNone
}

// giveUp is the error of a request abandoned at the end of its time: its
// last refusal, or the caller's context's error once it ended, when its
// close proved it never accepted and nothing was sent since, otherwise an
// unknown outcome.
func (cn *conn) giveUp(ctx context.Context, call *wire.Call, last *wire.Error, sent bool) error {
	if !sent {
		if err := ctx.Err(); err != nil {
			return err
		}
		return serverError(last)
	}
	return cn.unknown(ctx, call, last)
}

// deliver sends one delivery of a request with sequence number seq and
// reads its answer: the rows, kept if keep, and done frame of a success, or
// the error frame. err is a failure that settles nothing.
func (cn *conn) deliver(req *http.Request, kind wire.DoneKind, seq int64, keep bool) (*answer, *wire.Error, error) {
	rs, err := cn.c.roundTrip(req, kind)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rs.body.Close() }()
	rs.r.LimitResult(cn.ch.limits.ResultBytes)
	a := &answer{}
	for {
		f, err := rs.next()
		if err != nil {
			return nil, nil, err
		}
		switch f.Kind {
		case wire.FrameColumns:
			a.cols = f.Columns
		case wire.FrameRow:
			if !keep {
				continue
			}
			row := make([]driver.Value, len(f.Row))
			for i, v := range f.Row {
				row[i] = columnValue(a.cols[i].DeclType, v)
			}
			a.rows = append(a.rows, row)
		case wire.FrameDone:
			if f.Done.Seq != seq {
				return nil, nil, fmt.Errorf("skylite: the answer is for sequence number %d, not %d", f.Done.Seq, seq)
			}
			a.done = f.Done
			return a, nil, nil
		case wire.FrameError:
			if f.Error.Seq != 0 && f.Error.Seq != seq {
				return nil, nil, fmt.Errorf("skylite: the answer is for sequence number %d, not %d", f.Error.Seq, seq)
			}
			return nil, f.Error, nil
		}
	}
}

// ready opens the connection's channel if it has none, a read-only one in
// a read-only transaction and a write one otherwise, and replaces one
// outside a transaction that has been idle past channelIdle or whose next
// sequence number would pass 2^62 (ADR 0005 decision 2). An open refused
// with a transient code is sent again after a pause, within ctx; a lost
// open is never sent again: it leaves only a channel that expires unused.
func (cn *conn) ready(ctx context.Context) error {
	if cn.ch != nil && (cn.inTx || time.Since(cn.ch.last) <= channelIdle && cn.ch.seq < 1<<62) {
		return nil
	}
	if cn.ch != nil {
		cn.c.abandonLater(cn.ch.token, 0) // the old channel's close must not hold up the caller's request
		cn.ch = nil
	}
	open := &wire.Call{Mode: wire.ModeWrite}
	if cn.ro {
		open = &wire.Call{Mode: wire.ModeReadOnly, MinTxn: cn.c.watermark.Load()}
	}
	body, err := wire.AppendCall(nil, wire.KindOpen, open)
	if err != nil {
		return fmt.Errorf("skylite: %w", err)
	}
	var f *wire.Frame
	for n := 0; ; n++ {
		if f, err = cn.c.post(ctx, wire.KindOpen, body, wire.DoneOpen); err != nil {
			return err
		}
		if f.Kind != wire.FrameError {
			break
		}
		if !transient(f.Error) || !backoff(ctx, time.Time{}, n) {
			return refused(ctx, f.Error)
		}
	}
	c := f.Done.Channel
	if c.Mode != open.Mode || c.Mode == wire.ModeWrite && c.Epoch < 1 {
		return errors.New("skylite: the open answer lacks the channel's mode, or a write channel's epoch")
	}
	cn.c.raise(f.Done.Txn)
	cn.ch = &channel{token: c.Token, id: c.ID, epoch: c.Epoch, last: time.Now(), state: wire.TxnNone, limits: c.Limits}
	return nil
}

// unknown abandons the channel after a request whose outcome the driver
// could not learn, and returns its error. The cancel and close that make
// sure nothing of it still runs are sent in the background; nothing waits
// for them. The connection is discarded. The error matches the context's
// error only when ctx, the caller's, ended.
func (cn *conn) unknown(ctx context.Context, call *wire.Call, last *wire.Error) *Error {
	ch := cn.ch
	cn.ch, cn.lost = nil, true
	cn.c.abandonLater(ch.token, call.Seq) // the request's context may have ended; the cancel and close outlive it
	e := &Error{
		Message: "the request was sent and its outcome is unknown", ChannelID: ch.id, Seq: call.Seq, Epoch: ch.epoch,
		is: []error{ErrOutcomeUnknown},
	}
	if last != nil {
		e.Code, e.RequestID, e.Message = last.Code, last.RequestID, e.Message+": the last answer was "+last.Message
	}
	if err := ctx.Err(); err != nil {
		e.is = append(e.is, err)
	}
	if cn.inTx && call.Op == wire.OpExecute {
		e.is = append(e.is, ErrTransactionLost)
	}
	return e
}

// closeProves closes the channel, which has no transaction, and reports
// whether the close's answer proves that sequence number seq was never
// accepted: its last_seq is below seq (ADR 0005 decision 4, case 4). The
// channel is dropped only then.
func (cn *conn) closeProves(ctx context.Context, seq int64) bool {
	body, err := wire.AppendCall(nil, wire.KindClose, &wire.Call{Channel: cn.ch.token})
	if err != nil {
		return false
	}
	req, err := cn.c.request(ctx, wire.KindClose, body)
	if err != nil {
		return false
	}
	rs, err := cn.c.roundTrip(req, wire.DoneClose)
	if err != nil {
		return false
	}
	defer func() { _ = rs.body.Close() }()
	if f, err := rs.next(); err != nil || f.Kind != wire.FrameDone || f.Done.LastSeq >= seq {
		return false
	}
	cn.ch = nil
	return true
}

// abandonLater runs abandon in the background, at most abandonLimit at
// once. With none free it sends nothing: the server closes the channel, and
// rolls back what it holds, once its idle limit passes.
func (c *Connector) abandonLater(token string, seq int64) {
	select {
	case c.abandons <- struct{}{}:
		go func() {
			defer func() { <-c.abandons }()
			c.abandon(token, seq)
		}()
	default:
	}
}

// abandon cancels sequence number seq of the channel of token, unless seq
// is 0, then closes the channel. The cancel may take half of abandonWait,
// and the close the rest; Connector.Close waits for both. Their answers
// change nothing the driver reports, so they are not read.
func (c *Connector) abandon(token string, seq int64) {
	post := func(kind wire.Kind, call *wire.Call, by time.Time) {
		ctx, cancel := context.WithDeadline(context.Background(), by)
		defer cancel()
		body, err := wire.AppendCall(nil, kind, call)
		if err != nil {
			return
		}
		req, err := c.request(ctx, kind, body)
		if err != nil {
			return
		}
		if resp, err := c.client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}
	start := time.Now()
	if seq > 0 {
		post(wire.KindCancel, &wire.Call{Channel: token, Seq: seq}, start.Add(abandonWait/2))
	}
	post(wire.KindClose, &wire.Call{Channel: token}, start.Add(abandonWait))
}

// Begin begins a transaction; database/sql calls BeginTx.
func (cn *conn) Begin() (driver.Tx, error) {
	return cn.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx begins a transaction: a write transaction on the connection's
// write channel or, with opts.ReadOnly, a read-only one on its read-only
// channel, which reads one snapshot and holds no lock. Every isolation
// level that SQLite's serializable isolation satisfies is accepted and
// served at serializable; sql.LevelLinearizable and values outside
// database/sql's levels are refused.
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if lvl := sql.IsolationLevel(opts.Isolation); lvl < sql.LevelDefault || lvl > sql.LevelSerializable {
		return nil, fmt.Errorf("skylite: isolation level %v is not supported; transactions are serializable", lvl)
	}
	if opts.ReadOnly {
		cn.ch, cn.other, cn.ro = cn.other, cn.ch, true
	}
	if _, err := cn.do(ctx, time.Now().Add(resendWindow), &wire.Call{Op: wire.OpBegin}, wire.DoneBegin, false); err != nil {
		cn.writes()
		return nil, err
	}
	cn.inTx = true
	by, _ := ctx.Deadline()
	return tx{cn, by}, nil
}

// writes puts the write channel back in use after a read-only transaction.
func (cn *conn) writes() {
	if cn.ro {
		cn.ch, cn.other, cn.ro = cn.other, cn.ch, false
	}
}

// tx is the open transaction of its connection, and the deadline of the
// context that began it, if any.
type tx struct {
	cn *conn
	by time.Time
}

// Commit commits, bounded by the deadline of BeginTx's context or, without
// one, by commitWait: database/sql passes Commit no context. A failed
// commit has rolled the transaction back. A commit whose channel was lost
// is resolved: it succeeds if it committed, and matches ErrTransactionLost
// if it did not; otherwise its error matches ErrOutcomeUnknown. A read-only
// transaction has nothing to commit, so its commit succeeds then.
func (t tx) Commit() error {
	defer t.cn.ended()
	ctx, by := context.Background(), time.Now().Add(commitWait)
	if !t.by.IsZero() { // the caller's deadline: its end is the caller's
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, t.by)
		defer cancel()
		by = t.by
	}
	_, err := t.cn.do(ctx, by, &wire.Call{Op: wire.OpCommit}, wire.DoneCommit, false)
	if t.cn.ro && errors.Is(err, ErrOutcomeUnknown) {
		return nil
	}
	return err
}

// Rollback rolls back, bounded by commitWait. When the transaction is lost,
// or the rollback's own outcome is unknown, it succeeds without an answer:
// its commit is never sent, and the server rolls it back when the abandoned
// channel closes or its idle limit passes.
func (t tx) Rollback() error {
	defer t.cn.ended()
	if t.cn.lost {
		return nil
	}
	_, err := t.cn.do(context.Background(), time.Now().Add(commitWait), &wire.Call{Op: wire.OpRollback}, wire.DoneRollback, false)
	if errors.Is(err, ErrOutcomeUnknown) {
		return nil
	}
	return err
}

// ended ends the connection's transaction. Unless the channel's last
// receipt left it with no transaction, as after a commit or rollback that
// failed before it was sent or was answered transaction_aborted, it closes
// the channel, which rolls back what the server still holds, and discards
// the connection, so that no later borrower inherits that transaction.
func (cn *conn) ended() {
	cn.inTx = false
	if cn.ch != nil && cn.ch.state != wire.TxnNone {
		cn.c.abandon(cn.ch.token, 0)
		cn.ch, cn.lost = nil, true
	}
	cn.writes()
}

// refused is the error of a stateless request or an open refused with e,
// which ran nothing; it also matches the context's error once ctx ended.
func refused(ctx context.Context, e *wire.Error) *Error {
	se := serverError(e)
	if err := ctx.Err(); err != nil {
		se.is = append(se.is, err)
	}
	return se
}
