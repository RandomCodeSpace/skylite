package skylite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// The driver's own waits on a write channel, from ADR 0005's defaults.
const (
	// resendWindow bounds the resends of one request: T of ADR 0005
	// decision 7, the statement limit (30 s), the engine's commit bound
	// (20 s) and the response deadline (10 s).
	resendWindow = 60 * time.Second
	// commitWait bounds Commit and Rollback, which take no context.
	commitWait = resendWindow
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

// do sends a sequenced request on the connection's write channel, opening
// one first if needed, and reads its whole answer, keeping its rows if
// keep. A delivery without a settled answer, a transport failure, a cut or
// malformed response or a refusal before acceptance, is sent again with the
// same sequence number and body, which the server answers from its record
// (ADR 0005 decisions 2 and 3). by bounds the whole request: its token, its
// deliveries and their bodies. channel_lost, commit_outcome_unknown, a
// receipt that contradicts the request or itself (read-only with rows
// affected), an ended context and the end of by leave the outcome unknown.
// A request refused with behind is sent again on a new channel once the
// close of the old one proves it was never accepted.
func (cn *conn) do(ctx context.Context, by time.Time, call *wire.Call, kind wire.DoneKind, keep bool) (*answer, error) {
	if err := cn.usable(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dctx, cancel := context.WithDeadline(ctx, by)
	defer cancel()
	var body []byte
	var last *wire.Error // the last refusal, for the unknown outcome's error
	wait, sent := 100*time.Millisecond, false
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
			return nil, cn.unknown(ctx, call, e)
		case e.Final:
			if e.Seq != call.Seq || e.ChannelID != cn.ch.id || !cn.failedIn(call.Op, e.TxnState) {
				return nil, cn.unknown(ctx, call, nil)
			}
			cn.ch.last, cn.ch.state = time.Now(), e.TxnState
			return nil, serverError(e)
		case e.Code == CodeBehind && call.MinTxn > 0 && cn.closeProves(dctx, call.Seq):
			sent = false // proven never accepted: send it again on a new channel
			last = e
		default:
			last = e
		}
		if time.Now().Add(wait).After(by) {
			return nil, cn.giveUp(ctx, call, last, sent)
		}
		select {
		case <-dctx.Done():
			return nil, cn.giveUp(ctx, call, last, sent)
		case <-time.After(wait):
		}
		wait = min(2*wait, 2*time.Second)
	}
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

// ready opens the connection's write channel if it has none, and replaces
// one outside a transaction that has been idle past channelIdle or whose
// next sequence number would pass 2^62 (ADR 0005 decision 2). An open is
// never sent again: a lost open leaves only a channel that expires unused.
func (cn *conn) ready(ctx context.Context) error {
	if cn.ch != nil && (cn.inTx || time.Since(cn.ch.last) <= channelIdle && cn.ch.seq < 1<<62) {
		return nil
	}
	if cn.ch != nil {
		cn.c.abandonLater(cn.ch.token, 0) // the old channel's close must not hold up the caller's request
		cn.ch = nil
	}
	body, err := wire.AppendCall(nil, wire.KindOpen, &wire.Call{Mode: wire.ModeWrite})
	if err != nil {
		return fmt.Errorf("skylite: %w", err)
	}
	req, err := cn.c.request(ctx, wire.KindOpen, body)
	if err != nil {
		return err
	}
	rs, err := cn.c.roundTrip(req, wire.DoneOpen)
	if err != nil {
		return err
	}
	defer func() { _ = rs.body.Close() }()
	f, err := rs.next()
	if err != nil {
		return err
	}
	if f.Kind == wire.FrameError {
		return serverError(f.Error)
	}
	c := f.Done.Channel
	if c.Mode != wire.ModeWrite || c.Epoch < 1 {
		return errors.New("skylite: the open answer lacks a write channel's mode or epoch")
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
func (cn *conn) unknown(ctx context.Context, call *wire.Call, last *wire.Error) error {
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
// and the close the rest; Connector.Close ends both. Their answers change
// nothing the driver reports, so they are not read.
func (c *Connector) abandon(token string, seq int64) {
	post := func(kind wire.Kind, call *wire.Call, by time.Time) {
		ctx, cancel := context.WithDeadline(c.ctx, by)
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

// BeginTx begins a write transaction on the connection's channel. Every
// isolation level that SQLite's serializable isolation satisfies is
// accepted and served at serializable; sql.LevelLinearizable and values
// outside database/sql's levels are refused, and so is a read-only
// transaction, which needs a read-only channel that the server does not
// serve yet (ErrReadOnlyUnsupported).
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, ErrReadOnlyUnsupported
	}
	if lvl := sql.IsolationLevel(opts.Isolation); lvl < sql.LevelDefault || lvl > sql.LevelSerializable {
		return nil, fmt.Errorf("skylite: isolation level %v is not supported; transactions are serializable", lvl)
	}
	if _, err := cn.do(ctx, time.Now().Add(resendWindow), &wire.Call{Op: wire.OpBegin}, wire.DoneBegin, false); err != nil {
		return nil, err
	}
	cn.inTx = true
	return tx{cn}, nil
}

// tx is the open transaction of its connection.
type tx struct{ cn *conn }

// Commit commits, bounded by commitWait. A failed commit has rolled the
// transaction back; one whose outcome is unknown matches ErrOutcomeUnknown.
func (t tx) Commit() error {
	defer t.cn.ended()
	_, err := t.cn.do(context.Background(), time.Now().Add(commitWait), &wire.Call{Op: wire.OpCommit}, wire.DoneCommit, false)
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
}
