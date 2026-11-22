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
)

// channel is a connection's write channel.
type channel struct {
	token, id string
	epoch     int64
	seq       int64     // the last sequence number sent
	last      time.Time // when the last answer arrived
}

// answer is a channel request's whole response.
type answer struct {
	cols []wire.Column
	rows [][]driver.Value
	done *wire.Done
}

// do sends a sequenced request on the connection's write channel, opening
// one first if needed, and reads its whole answer. A delivery without a
// settled answer, a transport failure, a cut or malformed response or a
// refusal before acceptance, is sent again with the same sequence number
// and body, which the server answers from its record (ADR 0005 decisions 2
// and 3), until resendWindow ends. channel_lost, commit_outcome_unknown, an
// ended context and an exhausted window leave the outcome unknown.
func (cn *conn) do(ctx context.Context, call *wire.Call, kind wire.DoneKind) (*answer, error) {
	switch {
	case cn.lost && cn.inTx:
		return nil, &Error{Message: "the transaction's channel was lost; nothing was sent", is: []error{ErrTransactionLost}}
	case cn.closed || cn.lost:
		return nil, driver.ErrBadConn // nothing sent
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
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
	var last *wire.Error // the last refusal, for the unknown outcome's error
	start, wait := time.Now(), 100*time.Millisecond
	for sent := false; ; sent = true {
		req, err := cn.c.request(ctx, wire.KindChannel, body)
		if err != nil && !sent {
			return nil, err // nothing sent, and the sequence number unused
		}
		ch.seq = call.Seq
		var a *answer
		var e *wire.Error
		if err == nil {
			a, e, err = cn.deliver(req, kind, call.Seq)
		}
		switch {
		case err != nil:
		case e == nil:
			ch.last = time.Now()
			cn.c.raise(a.done.Txn)
			return a, nil
		case e.Final:
			ch.last = time.Now()
			return nil, serverError(e)
		case e.Code == CodeChannelLost || e.Code == CodeCommitOutcomeUnknown:
			return nil, cn.unknown(ctx, call, e)
		default:
			last = e
		}
		if time.Since(start)+wait > resendWindow {
			return nil, cn.unknown(ctx, call, last)
		}
		select {
		case <-ctx.Done():
			return nil, cn.unknown(ctx, call, last)
		case <-time.After(wait):
		}
		wait = min(2*wait, 2*time.Second)
	}
}

// deliver sends one delivery of a request with sequence number seq and
// reads its answer: the rows and done frame of a success, or the error
// frame. err is a failure that settles nothing.
func (cn *conn) deliver(req *http.Request, kind wire.DoneKind, seq int64) (*answer, *wire.Error, error) {
	rs, err := cn.c.roundTrip(req, kind)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rs.body.Close() }()
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
// one that has been idle past channelIdle outside a transaction. An open is
// never sent again: a lost open leaves only a channel that expires unused.
func (cn *conn) ready(ctx context.Context) error {
	if cn.ch != nil && (cn.inTx || time.Since(cn.ch.last) <= channelIdle) {
		return nil
	}
	if cn.ch != nil {
		cn.c.abandon(cn.ch.token, 0)
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
	cn.c.raise(f.Done.Txn)
	c := f.Done.Channel
	cn.ch = &channel{token: c.Token, id: c.ID, epoch: c.Epoch, last: time.Now()}
	return nil
}

// unknown abandons the channel after a request whose outcome the driver
// could not learn, and returns its error. The cancel and close that make
// sure nothing of it still runs are sent in the background; nothing waits
// for them. The connection is discarded.
func (cn *conn) unknown(ctx context.Context, call *wire.Call, last *wire.Error) error {
	ch := cn.ch
	cn.ch, cn.lost = nil, true
	go cn.c.abandon(ch.token, call.Seq) //nolint:gosec // the request's context may have ended; the cancel and close outlive it
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

// abandon cancels sequence number seq of the channel of token, unless seq
// is 0, then closes the channel, waiting up to abandonWait. Their answers
// change nothing the driver reports, so they are not read.
func (c *Connector) abandon(token string, seq int64) {
	ctx, cancel := context.WithTimeout(context.Background(), abandonWait)
	defer cancel()
	post := func(kind wire.Kind, call *wire.Call) {
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
	if seq > 0 {
		post(wire.KindCancel, &wire.Call{Channel: token, Seq: seq})
	}
	post(wire.KindClose, &wire.Call{Channel: token})
}

// Begin begins a transaction; database/sql calls BeginTx.
func (cn *conn) Begin() (driver.Tx, error) {
	return cn.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx begins a write transaction on the connection's channel. Every
// isolation level that SQLite's serializable isolation satisfies is
// accepted and served at serializable; sql.LevelLinearizable is refused,
// and so is a read-only transaction, which needs a read-only channel that
// the server does not serve yet (ErrReadOnlyUnsupported).
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, ErrReadOnlyUnsupported
	}
	if lvl := sql.IsolationLevel(opts.Isolation); lvl > sql.LevelSerializable {
		return nil, fmt.Errorf("skylite: isolation level %v is not supported; transactions are serializable", lvl)
	}
	if _, err := cn.do(ctx, &wire.Call{Op: wire.OpBegin}, wire.DoneBegin); err != nil {
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
	defer func() { t.cn.inTx = false }()
	ctx, cancel := context.WithTimeout(context.Background(), commitWait)
	defer cancel()
	_, err := t.cn.do(ctx, &wire.Call{Op: wire.OpCommit}, wire.DoneCommit)
	return err
}

// Rollback rolls back, bounded by commitWait. When the transaction is lost,
// or the rollback's own outcome is unknown, it succeeds without an answer:
// its commit is never sent, and the server rolls it back when the abandoned
// channel closes or its idle limit passes.
func (t tx) Rollback() error {
	defer func() { t.cn.inTx = false }()
	if t.cn.lost {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), commitWait)
	defer cancel()
	_, err := t.cn.do(ctx, &wire.Call{Op: wire.OpRollback}, wire.DoneRollback)
	if errors.Is(err, ErrOutcomeUnknown) {
		return nil
	}
	return err
}
