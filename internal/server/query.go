package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/sqlpolicy"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// The stateless requests of ADR 0005, query and describe. Each runs on a
// leader connection or a follower pin: a privileged BEGIN, the read's
// snapshot, the client's statement under the SQL policy (ADR 0007), and a
// privileged ROLLBACK.

// pconn is a pooled connection: an engine connection with the policy
// applied, of one leader.
type pconn struct {
	leader *engine.Leader
	ec     *engine.Conn
	pc     *sqlpolicy.Conn
}

// pool holds the server's SQLite connections for stateless requests. Each
// connection is charged a ceiling of the memory budget from its open to its
// close, idle or not; capacity is how many the budget holds after the body
// share and the leaders' own connections, and a principal may hold a
// quarter of them at once (ADR 0005 decision 9). An admission that finds
// the pool full closes an idle connection first. A stateless request that
// finds no room waits for it first in, first out, within the waiting
// bounds, for at most maxWait; a write that holds its write lock waits too,
// ahead of every stateless request, but only while stateless requests are
// turning over the connections it waits for.
type pool struct {
	mu           sync.Mutex
	capacity     int
	perPrincipal int
	open         int
	idle         []*pconn // most recently used last
	busy         map[string]int
	queue        []*connWaiter
	waiting      map[string]int // waiters per principal
	// When a stateless request last got a connection, in all and per
	// principal with one in use.
	read  time.Time
	reads map[string]time.Time
	// The waiting bounds, which tests lower.
	waiters, waitersBy int
	maxWait            time.Duration
}

// connWaiter is a request waiting for a connection; c and evict are what
// take gave it once ready is closed.
type connWaiter struct {
	leader   *engine.Leader
	who      string
	write    bool
	c, evict *pconn
	ready    chan struct{}
}

const (
	maxConnWaiters   = 256                    // stateless requests waiting for a connection, per pod
	maxConnWaitersBy = 64                     // per pod and principal
	maxConnWait      = 250 * time.Millisecond // and never past the statement time
)

func newPool(capacity int) *pool {
	return &pool{
		capacity: capacity, perPrincipal: max(1, capacity/4), busy: map[string]int{}, waiting: map[string]int{}, reads: map[string]time.Time{},
		waiters: maxConnWaiters, waitersBy: maxConnWaitersBy, maxWait: maxConnWait,
	}
}

// take reserves a connection for principal who on leader. It returns an
// idle connection of leader if there is one, and otherwise reserves room to
// open one, closing the least recently used idle connection (evict) if the
// pool is full. ok is false if who or the pool is at its limit.
func (p *pool) take(leader *engine.Leader, who string) (c, evict *pconn, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.takeLocked(leader, who, false)
}

// await is take that, when who or the pool is at its limit, waits for room
// first in, first out for at most maxWait within ctx. ok is false if the
// wait ends first or a waiting bound is reached. A write, which holds its
// write lock, waits ahead of every stateless request and past the waiting
// bounds, which the write locks bound to one per database; it waits only
// if a connection is likely to come (turningLocked), and is otherwise
// refused at once, since every other writer of its database waits behind it.
func (p *pool) await(ctx context.Context, leader *engine.Leader, who string, write bool) (c, evict *pconn, ok bool) {
	p.mu.Lock()
	if c, evict, ok = p.takeLocked(leader, who, write); ok || !write && (len(p.queue) >= p.waiters || p.waiting[who] >= p.waitersBy) || write && !p.turningLocked(who) {
		p.mu.Unlock()
		return c, evict, ok
	}
	w := &connWaiter{leader: leader, who: who, write: write, ready: make(chan struct{})}
	i := len(p.queue)
	if write { // behind the writes already waiting
		if i = slices.IndexFunc(p.queue, func(x *connWaiter) bool { return !x.write }); i < 0 {
			i = len(p.queue)
		}
	}
	p.queue = slices.Insert(p.queue, i, w)
	p.waiting[who]++
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, p.maxWait) // only once waiting: a free connection costs no timer
	defer cancel()
	select {
	case <-w.ready:
		return w.c, w.evict, true
	case <-ctx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-w.ready: // granted as ctx ended: the caller releases it as any other
		return w.c, w.evict, true
	default:
		p.queue = slices.DeleteFunc(p.queue, func(x *connWaiter) bool { return x == w })
		p.unwaitLocked(who)
		return nil, nil, false
	}
}

// turningLocked reports whether a stateless request got one of the
// connections who lacks within the last maxWait: one of who's own if who is
// at its limit, and any otherwise. Stateless requests are mostly short, so
// a recent one means a connection is about to be freed; connections held by
// transactions, or by reads older than the wait, promise nothing.
func (p *pool) turningLocked(who string) bool {
	at := p.read
	if p.busy[who] >= p.perPrincipal {
		at = p.reads[who]
	}
	return time.Since(at) < p.maxWait
}

func (p *pool) unwaitLocked(who string) {
	if p.waiting[who]--; p.waiting[who] == 0 {
		delete(p.waiting, who)
	}
}

// grantLocked gives room to the waiters that now fit, in their order; one
// that its principal's limit holds back does not hold back the others.
func (p *pool) grantLocked() {
	for i := 0; i < len(p.queue); {
		w := p.queue[i]
		c, evict, ok := p.takeLocked(w.leader, w.who, w.write)
		if !ok {
			i++
			continue
		}
		w.c, w.evict = c, evict
		p.queue = slices.Delete(p.queue, i, i+1)
		p.unwaitLocked(w.who)
		close(w.ready)
	}
}

func (p *pool) takeLocked(leader *engine.Leader, who string, write bool) (c, evict *pconn, ok bool) {
	if c, evict, ok = p.roomLocked(leader, who); ok && !write {
		p.read = time.Now()
		p.reads[who] = p.read
	}
	return c, evict, ok
}

func (p *pool) roomLocked(leader *engine.Leader, who string) (c, evict *pconn, ok bool) {
	if p.busy[who] >= p.perPrincipal {
		return nil, nil, false
	}
	for i := len(p.idle) - 1; i >= 0; i-- {
		if p.idle[i].leader == leader {
			c = p.idle[i]
			p.idle = slices.Delete(p.idle, i, i+1)
			p.busy[who]++
			return c, nil, true
		}
	}
	if p.open >= p.capacity {
		if len(p.idle) == 0 {
			return nil, nil, false
		}
		evict, p.idle = p.idle[0], p.idle[1:]
		p.open--
	}
	p.open++
	p.busy[who]++
	return nil, evict, true
}

// release ends who's use of c, which goes back to the pool if keep, and is
// otherwise closed. A nil c gives up the room take reserved.
func (p *pool) release(who string, c *pconn, keep bool) {
	p.mu.Lock()
	if p.busy[who]--; p.busy[who] == 0 {
		delete(p.busy, who)
		delete(p.reads, who)
	}
	if keep {
		p.idle = append(p.idle, c)
	} else {
		p.open--
	}
	p.grantLocked()
	p.mu.Unlock()
	if !keep && c != nil {
		_ = c.ec.Close()
	}
}

// conn returns a connection of leader for who, reset for its new holder, or
// the refusal before acceptance and the engine's error behind it, if any.
func (s *Server) conn(ctx context.Context, d *database, leader *engine.Leader, who string) (*pconn, *wire.Error, error) {
	c, refusal := s.reserve(ctx, leader, who)
	if refusal != nil {
		return nil, refusal, nil
	}
	refuse := func(err error) *wire.Error { return s.refusal(ctx, d, leader, err) }
	if c != nil {
		_, err := c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
			return broken(c.pc.Reset(db.GetInterrupt()))
		})
		switch {
		case err == nil:
			return c, nil, nil
		case !errors.Is(err, engine.ErrBroken) && !errors.Is(err, engine.ErrStopped):
			// Refused before the Reset ran, as at the tail bound or in
			// maintenance: the connection is as it was.
			s.pool.release(who, c, true)
			return nil, refuse(err), err
		}
		_ = c.ec.Close() // a failed Reset broke the connection; open another in its place
	}
	c, err := s.open(ctx, leader)
	if err != nil {
		s.pool.release(who, nil, false)
		return nil, refuse(err), err
	}
	return c, nil, nil
}

// reserve is check 12 of ADR 0005 for a stateless request: it waits for a
// connection for at most the pool's maxWait within ctx, and is refused
// overloaded if none comes.
func (s *Server) reserve(ctx context.Context, leader *engine.Leader, who string) (*pconn, *wire.Error) {
	c, evict, ok := s.pool.await(ctx, leader, who, false)
	if !ok {
		return nil, wire.NewError(wire.CodeOverloaded, "the server's or the principal's SQLite connections are all in use; retry later")
	}
	if evict != nil {
		_ = evict.ec.Close()
	}
	return c, nil
}

// refusal is the refusal before acceptance of a request whose connection
// could not be opened or reset under ctx.
func (s *Server) refusal(ctx context.Context, d *database, leader *engine.Leader, err error) *wire.Error {
	if ctx.Err() == context.DeadlineExceeded { //nolint:errorlint // context.Err returns the sentinel itself
		return timeoutError(0)
	}
	return s.engineError(d, leader, err)
}

// open opens an engine connection and applies the policy once, inside a Run
// right after the open (ADR 0007, requirement 5).
func (s *Server) open(ctx context.Context, leader *engine.Leader) (*pconn, error) {
	ec, err := leader.Open(ctx)
	if err != nil {
		return nil, err
	}
	c := &pconn{leader: leader, ec: ec}
	_, err = ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		var err error
		c.pc, err = s.policy.Apply(db.GetInterrupt(), db)
		return broken(err)
	})
	if err != nil {
		_ = ec.Close()
		return nil, err
	}
	return c, nil
}

// broken wraps engine.ErrBroken around a policy error that leaves the
// connection unusable, so that the engine releases it (ADR 0007,
// requirement 1).
func broken(err error) error {
	if pe, ok := errors.AsType[*sqlpolicy.Error](err); errors.Is(err, sqlpolicy.ErrBroken) || ok && pe.Reason == sqlpolicy.ReasonMemory {
		return fmt.Errorf("%w: %w", engine.ErrBroken, err)
	}
	return err
}

// errEnded ends a Run whose request failed with an error it recorded.
var errEnded = errors.New("server: the statement ended")

// statement serves a query or describe using the local leader or follower.
func (s *Server) statement(ctx context.Context, st *stream, d *database, leader *engine.Leader, follower *engine.Follower, req *wire.Request, who string, log *slog.Logger) (*wire.Done, *wire.Error) {
	limit := s.statementTime
	if req.TimeoutMS > 0 {
		limit = min(limit, time.Duration(req.TimeoutMS)*time.Millisecond)
	}
	// The statement time runs to the last row written, and its deadline is
	// on the Run's context, which page reads from storage use. The write
	// deadline is a margin later, so that the statement, not the stream,
	// ends first and its timeout frame is written.
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	deadline, _ := ctx.Deadline()
	st.deadline = deadline.Add(terminalMargin)
	if leader == nil {
		return s.followerStatement(ctx, st, follower, req, who, log)
	}
	c, e, err := s.conn(ctx, d, leader, who)
	if e != nil {
		logInternal(ctx, log, e, err)
		return nil, e
	}
	var done *wire.Done
	keep := true
	_, err = c.ec.Run(ctx, nil, func(db *sqlite3.Conn) error {
		var ferr error
		done, e, keep, ferr = s.run(db, c.pc, func(start func() error) (uint64, error) {
			return leader.Snapshot(db.GetInterrupt(), start)
		}, req, st)
		return ferr
	})
	switch {
	case err == nil:
	case errors.Is(err, engine.ErrBroken), errors.Is(err, engine.ErrStopped), errors.Is(err, engine.ErrRetry), errors.Is(err, engine.ErrTailBound):
		// The engine's class wins over the statement's own error: an
		// interrupt by maintenance or Close reads as SQLite's INTERRUPT.
		keep = false
		done, e = nil, s.engineError(d, leader, err)
	case errors.Is(err, errEnded):
	case ctx.Err() == context.DeadlineExceeded: //nolint:errorlint // context.Err returns the sentinel itself
		keep = false
		done, e = nil, timeoutError(limit)
	default:
		keep = false
		done, e = nil, s.engineError(d, leader, err)
	}
	logInternal(ctx, log, e, err)
	s.pool.release(who, c, keep)
	return done, e
}

func (s *Server) followerStatement(ctx context.Context, st *stream, f *engine.Follower, req *wire.Request, who string, log *slog.Logger) (*wire.Done, *wire.Error) {
	remaining := engine.FollowerMaxWait
	minTxn := uint64(req.MinTxn) //nolint:gosec // validated nonnegative by wire.Parse
	for attempt := 0; ; attempt++ {
		started := time.Now()
		pctx, cancel := context.WithTimeout(ctx, remaining)
		pin, err := f.Pin(pctx, minTxn)
		cancel()
		remaining -= time.Since(started)
		if err != nil {
			switch {
			case errors.Is(err, engine.ErrStale) || errors.Is(err, context.Canceled):
				return nil, followerUnavailable()
			case errors.Is(err, engine.ErrBehind) || errors.Is(err, context.DeadlineExceeded):
				if !f.Fresh() || attempt > 0 {
					return nil, followerUnavailable()
				}
				if req.MinTxn == 0 {
					return nil, timeoutError(0)
				}
				return nil, wire.NewError(wire.CodeBehind, "the follower did not reach min_txn within the catch-up budget")
			default:
				e := wire.NewError(wire.CodeInternal, "the follower could not validate its snapshot")
				logInternal(ctx, log, e, err)
				return nil, e
			}
		}
		done, e, err := s.runFollower(ctx, pin, req, st, who)
		switch {
		case err == nil, errors.Is(err, errEnded):
			return done, e
		case errors.Is(err, engine.ErrCompacted) && st.started:
			e = wire.NewError(wire.CodeInternal, "compaction ended the follower snapshot during the read")
		case errors.Is(err, engine.ErrCompacted):
			if attempt == 0 && ctx.Err() == nil && remaining > 0 {
				minTxn = pin.Txn() + 1
				continue
			}
			return nil, followerUnavailable()
		case errors.Is(ctx.Err(), context.Canceled) && !errors.Is(err, engine.ErrBroken):
			return nil, followerUnavailable() // the request was cut off: not a fault of this read
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			e = timeoutError(0)
		default:
			e = wire.NewError(wire.CodeInternal, "the follower read failed")
		}
		logInternal(ctx, log, e, err)
		return nil, e // a read has no effect, so not_executed stays true (0005)
	}
}

func followerUnavailable() *wire.Error {
	return wire.NewError(wire.CodeNotServing, "this server has no usable fresh follower snapshot for the database")
}

// runFollower serves the request on pin. A refusal for want of a SQLite
// slot is returned as e with a nil err.
func (s *Server) runFollower(ctx context.Context, pin *engine.ReadPin, req *wire.Request, st *stream, who string) (done *wire.Done, e *wire.Error, err error) {
	// Follower connections use the same slots as leader connections. Each
	// pin has its own SQLite connection and is discarded on release.
	if _, e := s.reserve(ctx, nil, who); e != nil {
		return nil, e, nil
	}
	defer s.pool.release(who, nil, false)
	c, err := pin.Open(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = c.Close() }()
	err = c.Run(ctx, func(db *sqlite3.Conn) error {
		pc, err := s.policy.Apply(db.GetInterrupt(), db)
		if err != nil {
			return broken(err)
		}
		snapshot := func(start func() error) (uint64, error) { return c.Txn(), start() }
		done, e, _, err = s.run(db, pc, snapshot, req, st)
		return err
	})
	return done, e, err
}

// logInternal logs an internal answer with the engine's class of err.
func logInternal(ctx context.Context, log *slog.Logger, e *wire.Error, err error) {
	if e != nil && e.Code == wire.CodeInternal {
		log.LogAttrs(ctx, slog.LevelError, "statement failed", slog.String("class", engineClass(err)))
	}
}

// run serves the request inside the engine's Run. It returns the answer, and
// the error that ends the Run: nil on success, errEnded after a failure,
// or one that wraps engine.ErrBroken. keep is false if c must be closed.
func (s *Server) run(db *sqlite3.Conn, pc *sqlpolicy.Conn, snapshot func(func() error) (uint64, error), req *wire.Request, st *stream) (done *wire.Done, e *wire.Error, keep bool, err error) {
	ictx := db.GetInterrupt() // the Run's context: its deadline, Close and maintenance interrupt it
	err = pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("BEGIN") })
	var snap uint64
	if err == nil {
		snap, err = snapshot(func() error {
			return pc.Privileged(func(db *sqlite3.Conn) error { return db.Exec("PRAGMA main.schema_version") })
		})
	}
	defer func() {
		// The server's ROLLBACK ends the read; a connection it fails on is
		// closed rather than pooled with a snapshot held.
		rerr := pc.Privileged(func(db *sqlite3.Conn) error {
			if db.GetAutocommit() {
				return nil
			}
			return db.Exec("ROLLBACK")
		})
		keep = keep && rerr == nil
	}()
	if err != nil {
		return nil, nil, false, broken(err)
	}
	if req.MinTxn > 0 && snap < uint64(req.MinTxn) {
		return nil, wire.NewError(wire.CodeBehind, fmt.Sprintf("the leader's state, transaction %d, is below min_txn %d", snap, req.MinTxn)), true, errEnded
	}
	total := db.TotalChanges()
	describe := req.Kind == wire.KindDescribe
	err = pc.Run(ictx, req.SQL, func(stmt *sqlite3.Stmt) error {
		// The policy has compiled the statement; nothing has been bound or
		// stepped (ADR 0005 decision 5, step 3).
		switch {
		case !describe && !stmt.ReadOnly():
			return wire.NewError(wire.CodeNeedsChannel, "the statement is not a pure read; send it on a write channel")
		case txnControl(req.SQL):
			return wire.NewError(wire.CodeInvalidRequest, "transaction control is not SQL in this protocol; use a channel's begin, commit and rollback")
		}
		cols := columns(stmt)
		if describe {
			names := make([]string, stmt.BindCount())
			for i := range names {
				names[i] = stmt.BindName(i + 1)
			}
			if err := st.out.Columns(cols); err != nil {
				return err
			}
			if err := st.out.Params(names); err != nil {
				return err
			}
			done = &wire.Done{Kind: wire.DoneDescribe, Txn: int64(snap), ParamCount: int64(len(names))} //nolint:gosec // transaction numbers stay below 2^63 (ADR 0005, requirement 13)
			return nil
		}
		st.start() // the statement started executing: status 200
		if err := bind(req, req.Args, stmt); err != nil {
			return err
		}
		if len(cols) > 0 {
			if err := st.out.Columns(cols); err != nil {
				return err
			}
		}
		row := make([]any, len(cols))
		for stmt.Step() {
			for i := range row {
				row[i] = value(stmt, i)
			}
			if err := st.out.Row(row); err != nil {
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
		done = &wire.Done{Kind: wire.DoneQuery, Txn: int64(snap), ReadOnly: true, RowsAffected: changes, LastInsertRowID: db.LastInsertRowID()} //nolint:gosec // as above
		return nil
	})
	if err == nil {
		return done, nil, true, nil
	}
	e, err = statementError(ictx, err)
	return nil, e, err == nil, cmp.Or(err, errEnded)
}

// statementError maps the failure of the client's statement to its answer,
// and returns an error that wraps engine.ErrBroken if the connection broke.
func statementError(ctx context.Context, err error) (*wire.Error, error) {
	if we, ok := errors.AsType[*wire.Error](err); ok {
		return we, nil
	}
	if pe, ok := errors.AsType[*sqlpolicy.Error](err); ok {
		switch pe.Reason {
		case sqlpolicy.ReasonMemory:
			return wire.NewError(wire.CodeInternal, "the statement exhausted its connection's memory; the connection was discarded"), broken(err)
		case sqlpolicy.ReasonTimeLimit:
			return timeoutError(0), nil
		case sqlpolicy.ReasonTransaction, sqlpolicy.ReasonStatementCount, sqlpolicy.ReasonNUL, sqlpolicy.ReasonSQLLength:
			return wire.NewError(wire.CodeInvalidRequest, "the SQL must be one statement, without NUL, within the length limit, and not transaction control ("+string(pe.Reason)+")"), nil
		}
		return wire.NewError(wire.CodeStatementForbidden, "the SQL policy refused the statement ("+string(pe.Reason)+")"), nil
	}
	if errors.Is(err, sqlpolicy.ErrBroken) {
		return wire.NewError(wire.CodeInternal, "the connection broke and was discarded"), broken(err)
	}
	if ctx.Err() == context.DeadlineExceeded { //nolint:errorlint // context.Err returns the sentinel itself
		return timeoutError(0), nil
	}
	if se, ok := errors.AsType[*sqlite3.Error](err); ok {
		e := wire.NewError(wire.CodeSQLError, se.Error())
		e.SQLiteCode, e.SQLiteExtendedCode = int64(se.Code()), int64(se.ExtendedCode())
		return e, nil
	}
	// ncruces reports an error by its code alone when SQLite's message is
	// the generic one, as for SQLITE_READONLY under PRAGMA query_only. Any
	// other bare code, SQLITE_CORRUPT or SQLITE_IOERR among them, is internal.
	if xc, ok := errors.AsType[sqlite3.ExtendedErrorCode](err); ok && errors.Is(xc, sqlite3.READONLY) {
		e := wire.NewError(wire.CodeSQLError, xc.Error())
		e.SQLiteCode, e.SQLiteExtendedCode = int64(xc.Code()), int64(xc)
		return e, nil
	}
	// The response could not be written, or the engine failed: nobody may
	// be reading, and the connection is not trusted again.
	return wire.NewError(wire.CodeInternal, "the statement failed"), errEnded
}

func timeoutError(limit time.Duration) *wire.Error {
	msg := "the statement time limit was reached"
	if limit > 0 {
		msg = fmt.Sprintf("the statement time limit of %d ms was reached", limit.Milliseconds())
	}
	return wire.NewError(wire.CodeTimeout, msg)
}

// engineError maps an engine failure of a stateless request to its answer.
// At a tail bound the leader cannot compact past, it marks d, which leaves
// the server not ready while leader leads; at one compaction is clearing,
// the refusal matches ErrRetry and marks nothing.
func (s *Server) engineError(d *database, leader *engine.Leader, err error) *wire.Error {
	switch {
	case errors.Is(err, engine.ErrTailBound) && !errors.Is(err, engine.ErrRetry):
		d.tail.Store(leader)
		return wire.NewError(wire.CodeMaintenance, fmt.Sprintf("the database's log is at its tail bound of %d commits and the leader cannot compact it, so it serves no reads or writes of it", s.engine.TailBound))
	case errors.Is(err, engine.ErrTailBound):
		return wire.NewError(wire.CodeMaintenance, fmt.Sprintf("the database's log is at its tail bound of %d commits until a compaction clears it; retry", s.engine.TailBound))
	case errors.Is(err, engine.ErrRetry):
		return wire.NewError(wire.CodeMaintenance, "the leader is in maintenance; retry")
	case errors.Is(err, engine.ErrStopped):
		return wire.NewError(wire.CodeNotServing, "the leader stopped serving the database")
	case errors.Is(err, engine.ErrBroken):
		return wire.NewError(wire.CodeInternal, "the connection broke and was discarded")
	}
	return wire.NewError(wire.CodeInternal, "the statement failed")
}

// engineClass names an engine error's class for the log, which carries no
// SQL text and no SQLite message (ADR 0005 decision 6).
func engineClass(err error) string {
	for _, c := range []struct {
		err  error
		name string
	}{{engine.ErrBroken, "broken"}, {engine.ErrCompacted, "compacted"}, {engine.ErrStopped, "stopped"}, {engine.ErrUnknown, "unknown"}, {engine.ErrNotCommitted, "not_committed"}} {
		if errors.Is(err, c.err) {
			return c.name
		}
	}
	return "other"
}

// columns returns the statement's result columns.
func columns(stmt *sqlite3.Stmt) []wire.Column {
	cols := make([]wire.Column, stmt.ColumnCount())
	for i := range cols {
		cols[i] = wire.Column{Name: stmt.ColumnName(i), DeclType: stmt.ColumnDeclType(i), Database: stmt.ColumnDatabaseName(i), Table: stmt.ColumnTableName(i), Origin: stmt.ColumnOriginName(i)}
	}
	return cols
}

// bind binds a's arguments of req, every parameter exactly once.
func bind(req *wire.Request, a wire.Args, stmt *sqlite3.Stmt) error {
	names := make([]string, stmt.BindCount())
	for i := range names {
		names[i] = stmt.BindName(i + 1)
	}
	return req.Bind(a, names, func(i int, v any) error {
		switch v := v.(type) {
		case nil:
			return stmt.BindNull(i)
		case int64:
			return stmt.BindInt64(i, v)
		case float64:
			return stmt.BindFloat(i, v)
		case string:
			return stmt.BindText(i, v)
		case []byte: // never nil: the codec decodes an empty BLOB to an empty slice
			return stmt.BindBlob(i, v)
		}
		return fmt.Errorf("server: %T is not a value", v)
	})
}

// value returns column i of the current row.
func value(stmt *sqlite3.Stmt, i int) any {
	switch stmt.ColumnType(i) {
	case sqlite3.INTEGER:
		return stmt.ColumnInt64(i)
	case sqlite3.FLOAT:
		return stmt.ColumnFloat(i)
	case sqlite3.TEXT:
		return stmt.ColumnText(i)
	case sqlite3.BLOB:
		return stmt.ColumnBlob(i, []byte{})
	}
	return nil
}

// txnControl reports whether sql is transaction control by its first
// keyword, after white space, comments, semicolons and EXPLAIN [QUERY
// PLAN]. The policy lets SAVEPOINT, RELEASE and ROLLBACK TO through inside
// a transaction, and a stateless request runs inside the server's own read
// transaction.
func txnControl(sql string) bool {
	w, _ := leadWord(sql)
	switch strings.ToUpper(w) {
	case "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return true
	}
	return false
}

// leadWord returns sql's first keyword after EXPLAIN [QUERY PLAN], and the
// rest of sql.
func leadWord(sql string) (word, rest string) {
	w, rest := firstWord(sql)
	if strings.EqualFold(w, "EXPLAIN") {
		if w, rest = firstWord(rest); strings.EqualFold(w, "QUERY") {
			_, rest = firstWord(rest) // PLAN
			w, rest = firstWord(rest)
		}
	}
	return w, rest
}

// savepointSQL reports whether sql is SAVEPOINT, RELEASE or ROLLBACK
// [TRANSACTION] TO by its first keywords, after EXPLAIN [QUERY PLAN].
func savepointSQL(sql string) bool {
	w, rest := leadWord(sql)
	switch strings.ToUpper(w) {
	case "SAVEPOINT", "RELEASE":
		return true
	case "ROLLBACK":
		if w, rest = firstWord(rest); strings.EqualFold(w, "TRANSACTION") {
			w, _ = firstWord(rest)
		}
		return strings.EqualFold(w, "TO")
	}
	return false
}

// firstWord returns the identifier characters at the start of s after white
// space, comments and semicolons, as SQLite's parser reads them, and the
// rest of s. SQLite's tokenizer reads a UTF-8 byte order mark as white space,
// and a vertical tab too once a run of white space has begun.
func firstWord(s string) (word, rest string) {
	for s != "" {
		switch {
		case strings.IndexByte(" \t\n\f\r", s[0]) >= 0:
			s = strings.TrimLeft(s, " \t\n\v\f\r")
			continue
		case s[0] == ';': // SQLite skips empty statements before the first
			s = s[1:]
			continue
		case strings.HasPrefix(s, "\xef\xbb\xbf"):
			s = s[3:]
			continue
		case strings.HasPrefix(s, "--"):
			// Up to the newline, which starts a run of white space.
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i:]
			} else {
				s = ""
			}
			continue
		case strings.HasPrefix(s, "/*"):
			_, s, _ = strings.Cut(s[2:], "*/")
			continue
		}
		break
	}
	i := 0
	for i < len(s) && (s[i] >= 0x80 || s[i] == '_' || s[i] == '$' || 'a' <= s[i]|0x20 && s[i]|0x20 <= 'z' || '0' <= s[i] && s[i] <= '9') {
		i++
	}
	return s[:i], s[i:]
}
