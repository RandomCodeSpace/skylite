package engine

// Tests for what the client protocol's server needs from the leader: the
// snapshot number of a read, the lookup of a resolve, and the end of a
// connection's transaction by maintenance (record 0005, requirements 11, 12
// and 15), and a bounded Open (record 0007, requirement 5).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// readAt begins a read on c through Snapshot, then counts t's rows in that
// read: pause runs in Snapshot's start after the read began, and before
// once Snapshot has returned, before the count.
func readAt(l *Leader, c *Conn, pause, before func()) (n uint64, count int, err error) {
	ctx := context.Background()
	_, err = c.Run(ctx, nil, func(db *sqlite3.Conn) error {
		var err error
		n, err = l.Snapshot(ctx, func() error {
			if err := db.Exec("BEGIN; PRAGMA schema_version"); err != nil {
				return err
			}
			pause()
			return nil
		})
		if err != nil {
			return err
		}
		before()
		rows, err := query(db, "SELECT count(*) FROM t")
		if err == nil {
			count, err = strconv.Atoi(rows[0])
		}
		return errors.Join(err, db.Exec("COMMIT"))
	})
	return n, count, err
}

// Covers: requirement 12 at both edges of a commit. A read that begins while
// a commit is between its create and SQLite's completion of it reports the
// commit only once SQLite has published it, and a commit whose create comes
// while a read begins waits until that read holds its snapshot.
func TestSnapshotWhileCommitPublishes(t *testing.T) {
	type result struct {
		n     uint64
		count int
		err   error
	}
	t.Run("commit created, not yet completed", func(t *testing.T) {
		_, _, o, l, w := setup(t)
		r := open(t, l)
		held := newGate()
		l.afterUpload = held.wait
		wrote := make(chan error, 1)
		go func() {
			_, err := exec(w, "INSERT INTO t VALUES (1)")
			wrote <- err
		}()
		<-held.reached
		done := make(chan result, 1)
		go func() {
			n, count, err := readAt(l, r, func() {}, func() {})
			done <- result{n, count, err}
		}()
		select {
		case res := <-done:
			held.open()
			t.Fatalf("Snapshot returned %d (%d rows) while a commit was between its create and its completion", res.n, res.count)
		case <-time.After(200 * time.Millisecond):
		}
		held.open()
		res := <-done
		if res.err != nil || res.n != o.Head+2 || uint64(res.count) != res.n-o.Head { //nolint:gosec // a small count
			t.Fatalf("Snapshot %d, %d rows, %v; want %d and %d rows", res.n, res.count, res.err, o.Head+2, 2)
		}
		if err := <-wrote; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("read beginning, commit waiting", func(t *testing.T) {
		_, _, o, l, w := setup(t)
		r := open(t, l)
		began, resume := make(chan struct{}), make(chan struct{})
		done := make(chan result, 1)
		var txn uint64
		wrote := make(chan struct{})
		go func() {
			// The count waits for the commit, so it holds one row only if
			// start began a read that lasts.
			n, count, err := readAt(l, r, func() {
				close(began)
				<-resume
			}, func() { <-wrote })
			done <- result{n, count, err}
		}()
		<-began
		go func() {
			defer close(wrote)
			var err error
			if txn, err = exec(w, "INSERT INTO t VALUES (1)"); err != nil {
				t.Error(err)
			}
		}()
		select {
		case <-wrote:
			close(resume)
			t.Fatalf("commit %d created while a Snapshot's read was beginning", txn)
		case <-time.After(200 * time.Millisecond):
		}
		close(resume)
		res := <-done
		if res.err != nil || res.n != o.Head+1 || res.count != 1 {
			t.Fatalf("Snapshot %d, %d rows, %v; want %d and 1 row", res.n, res.count, res.err, o.Head+1)
		}
		if txn != o.Head+2 {
			t.Fatalf("the waiting commit is %d, want %d", txn, o.Head+2)
		}
	})
}

// Covers: requirement 12 under concurrency. Every commit adds one row, so a
// read at transaction n holds exactly n - o.Head rows; readers check that
// against the number Snapshot reports while writers commit.
func TestSnapshotOracle(t *testing.T) {
	_, _, o, l, _ := setup(t)
	const writers, readers, commits = 3, 3, 60
	var wg sync.WaitGroup
	var writing atomic.Int32
	writing.Store(writers)
	for i := range writers {
		w := open(t, l)
		wg.Go(func() {
			defer writing.Add(-1)
			for j := range commits {
				if _, err := exec(w, fmt.Sprintf("INSERT INTO t VALUES (%d)", (i+1)*1000+j)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	var checked atomic.Int64
	for range readers {
		r := open(t, l)
		wg.Go(func() {
			for writing.Load() > 0 {
				// A short pause widens the window in which a commit could
				// slip between the read's snapshot and the number reported.
				n, count, err := readAt(l, r, func() { time.Sleep(100 * time.Microsecond) }, func() {})
				if err != nil {
					t.Error(err)
					return
				}
				if uint64(count) != n-o.Head { //nolint:gosec // a small count
					t.Errorf("Snapshot reported %d; its read holds %d rows, the state at %d", n, count, o.Head+uint64(count)) //nolint:gosec // as above
					return
				}
				checked.Add(1)
			}
		})
	}
	wg.Wait()
	if l.Head() != o.Head+1+writers*commits || checked.Load() == 0 {
		t.Fatalf("head %d after %d commits, %d reads checked", l.Head(), writers*commits, checked.Load())
	}
}

// parkPhaseTwo pauses c's phase two at the returned gate.
func parkPhaseTwo(l *Leader, c *Conn) *gate {
	g := newGate()
	l.beforePhaseTwo = func(x *Conn) {
		if x == c {
			g.wait()
		}
	}
	return g
}

// Covers: requirement 12 with a second writer. SQLite releases the write
// lock before it sends phase two, so a second writer can reach its commit
// while the first is created and not yet published. It waits for that phase
// two before its create, and a read that begins while it is between its
// create and its completion still waits for it.
func TestSnapshotSecondWriter(t *testing.T) {
	_, _, o, l, w1 := setup(t)
	w2, r := open(t, l), open(t, l)
	p2 := parkPhaseTwo(l, w1)
	d1, d2 := make(chan error, 1), make(chan error, 1)
	go func() { _, err := exec(w1, "INSERT INTO t VALUES (1)"); d1 <- err }()
	<-p2.reached
	held := newGate()
	l.afterUpload = held.wait
	go func() { _, err := exec(w2, "INSERT INTO t VALUES (2)"); d2 <- err }()
	// Without the wait the second create lands here, before the first's
	// phase two.
	select {
	case <-held.reached:
	case <-time.After(300 * time.Millisecond):
	}
	p2.open()
	if err := <-d1; err != nil {
		t.Fatal(err)
	}
	<-held.reached
	type result struct {
		n     uint64
		count int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		n, count, err := readAt(l, r, func() {}, func() {})
		done <- result{n, count, err}
	}()
	select {
	case res := <-done:
		held.open()
		t.Fatalf("Snapshot returned %d (%d rows) while the second commit was between its create and its completion", res.n, res.count)
	case <-time.After(200 * time.Millisecond):
	}
	held.open()
	res := <-done
	if res.err != nil || res.n != o.Head+3 || uint64(res.count) != res.n-o.Head { //nolint:gosec // a small count
		t.Fatalf("Snapshot %d, %d rows, %v; want %d and 3 rows", res.n, res.count, res.err, o.Head+3)
	}
	if err := <-d2; err != nil {
		t.Fatal(err)
	}
}

// Covers: decision 10, steps 2 and 3, with a second writer: one that reached
// its commit while the first commit was created and not yet published, and
// waited, is refused by maintenance that began meanwhile, and nothing of it
// is created.
func TestMaintenanceRefusesWaitingWriter(t *testing.T) {
	_, _, o, l, w1 := setup(t)
	w2 := open(t, l)
	p2 := parkPhaseTwo(l, w1)
	d1, d2 := make(chan error, 1), make(chan error, 1)
	go func() { _, err := exec(w1, "INSERT INTO t VALUES (1)"); d1 <- err }()
	<-p2.reached
	held := newGate()
	l.afterUpload = held.wait
	go func() { _, err := exec(w2, "INSERT INTO t VALUES (2)"); d2 <- err }()
	select {
	case <-held.reached:
	case <-time.After(300 * time.Millisecond):
	}
	startMaintenance(l)
	p2.open()
	held.open()
	if err := <-d1; err != nil {
		t.Fatal(err)
	}
	if err := <-d2; !errors.Is(err, ErrNotCommitted) || !errors.Is(err, ErrRetry) {
		t.Fatalf("the waiting writer after maintenance began: %v, want not committed and retry", err)
	}
	waitMaintenance(t, l)
	if h := l.Head(); h != o.Head+2 || l.Err() != nil {
		t.Fatalf("head %d, want %d; leader: %v", h, o.Head+2, l.Err())
	}
}

// Covers: a commit waiting for a Snapshot's start is refused if the leader
// stops meanwhile, and creates nothing.
func TestCommitWaitingForSnapshotStops(t *testing.T) {
	_, _, o, l, w := setup(t)
	resume := make(chan struct{})
	snap := make(chan error, 1)
	go func() {
		_, err := l.Snapshot(context.Background(), func() error { <-resume; return nil })
		snap <- err
	}()
	wrote := make(chan error, 1)
	waitUntil := func(cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			l.cmu.Lock()
			ok := cond()
			l.cmu.Unlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				close(resume)
				t.Fatal("timed out")
			}
		}
	}
	waitUntil(func() bool { return l.snapshots == 1 })
	go func() { _, err := exec(w, "INSERT INTO t VALUES (1)"); wrote <- err }()
	waitUntil(func() bool { return l.uploading })
	_ = l.stop(errors.New("test stop"))
	close(resume)
	if err := <-snap; err != nil {
		t.Fatal(err)
	}
	if err := <-wrote; !errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrUnknown) {
		t.Fatalf("a commit waiting for start when the leader stopped: %v, want not committed", err)
	}
	if h := l.Head(); h != o.Head+1 {
		t.Fatalf("head %d after the refused commit, want %d", h, o.Head+1)
	}
}

// Covers: Snapshot's edges. A start that fails returns its error and no
// number, a start that panics leaves no create waiting, and a stopped
// leader runs no start.
func TestSnapshotEdges(t *testing.T) {
	_, _, _, l, w := setup(t)
	fail := errors.New("start failed")
	if n, err := l.Snapshot(context.Background(), func() error { return fail }); n != 0 || !errors.Is(err, fail) {
		t.Fatalf("a failing start: %d, %v", n, err)
	}
	r := open(t, l)
	if _, err := r.Run(context.Background(), nil, func(*sqlite3.Conn) error {
		_, err := l.Snapshot(context.Background(), func() error { panic("boom") })
		return err
	}); err == nil {
		t.Fatal("a Run whose start panicked succeeded")
	}
	done := make(chan error, 1)
	go func() { _, err := exec(w, "INSERT INTO t VALUES (1)"); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a write after a start panicked: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = l.stop(errors.New("test stop"))
		t.Fatalf("a write hangs after a start panicked; after a stop: %v", <-done)
	}
	_ = l.stop(errors.New("test stop"))
	ran := false
	if n, err := l.Snapshot(context.Background(), func() error { ran = true; return nil }); n != 0 || !errors.Is(err, ErrStopped) || ran {
		t.Fatalf("a stopped leader: %d, %v, start ran: %v", n, err, ran)
	}
}

// Covers: Snapshot's wait for a commit ends with its context, and start
// does not run.
func TestSnapshotContext(t *testing.T) {
	_, _, _, l, w := setup(t)
	held := newGate()
	l.afterUpload = held.wait
	defer held.open()
	wrote := make(chan error, 1)
	go func() { _, err := exec(w, "INSERT INTO t VALUES (1)"); wrote <- err }()
	<-held.reached
	var ran atomic.Bool
	start := func() error { ran.Store(true); return nil }
	ctx, cancel := context.WithCancel(context.Background())
	snap := make(chan error, 1)
	go func() { _, err := l.Snapshot(ctx, start); snap <- err }()
	select {
	case err := <-snap:
		t.Fatalf("Snapshot returned during a commit's upload: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-snap:
		if !errors.Is(err, context.Canceled) || ran.Load() {
			t.Fatalf("Snapshot after its context ended: %v, start ran: %v", err, ran.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Snapshot's wait outlived its context")
	}
	if _, err := l.Snapshot(ctx, start); !errors.Is(err, context.Canceled) || ran.Load() {
		t.Fatalf("Snapshot with a done context: %v, start ran: %v", err, ran.Load())
	}
	held.open()
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}

// runID commits sql on c under commit identifier id.
func runID(t *testing.T, c *Conn, id []byte, sql string) uint64 {
	t.Helper()
	txn, err := c.Run(context.Background(), id, func(db *sqlite3.Conn) error { return db.Exec(sql) })
	if err != nil || txn == 0 {
		t.Fatalf("%s: %d, %v", sql, txn, err)
	}
	return txn
}

// Covers: requirement 11's headers come from the takeover walk as well as
// the leader's own commits: identifiers an earlier leader committed are
// found after a takeover, with the new leader's bump as the first above
// them, and so are the new leader's.
func TestLookupFindsPreviousLeadersIDs(t *testing.T) {
	st, keys, o, l, w := setup(t)
	ids := [][]byte{newID(), newID()}
	txns := []uint64{runID(t, w, ids[0], "INSERT INTO t VALUES (1)"), runID(t, w, ids[1], "INSERT INTO t VALUES (2)")}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	o2 := takeOffice(t, st.Store, keys)
	l2 := lead(t, st, keys, o2, testConfig())
	for i, id := range ids {
		if txn, bump, head, ok := l2.Lookup(id, o.Head); txn != txns[i] || bump != o2.Head || head != o2.Head || !ok {
			t.Fatalf("Lookup of the earlier leader's %q: %d, bump %d, head %d, %v; want %d, %d, %d", id, txn, bump, head, ok, txns[i], o2.Head, o2.Head)
		}
		// Only a commit above the given number is reported.
		if txn, _, _, _ := l2.Lookup(id, txns[i]); txn != 0 {
			t.Fatalf("Lookup of %q above its own commit %d found %d", id, txns[i], txn)
		}
	}
	id := newID()
	if txn, _, _, _ := l2.Lookup(id, o.Head); txn != 0 {
		t.Fatalf("an identifier never committed found at %d", txn)
	}
	own := runID(t, open(t, l2), id, "INSERT INTO t VALUES (3)")
	if txn, bump, head, _ := l2.Lookup(id, o2.Head); txn != own || bump != 0 || head != own {
		t.Fatalf("Lookup of the leader's own commit: %d, bump %d, head %d; want %d, 0, %d", txn, bump, head, own, own)
	}
}

// Covers: requirement 11's bump position: the first epoch bump strictly
// above the given number, whichever leaders created the bumps, and no
// answer for a range the headers held do not cover.
func TestLookupFirstBumpAbove(t *testing.T) {
	st, keys, o, l, w := setup(t)
	mustRun(t, w, "INSERT INTO t VALUES (1)")
	_ = l.Close()
	o2 := takeOffice(t, st.Store, keys)
	l2 := lead(t, st, keys, o2, testConfig())
	mustRun(t, open(t, l2), "INSERT INTO t VALUES (2)")
	_ = l2.Close()
	o3 := takeOffice(t, st.Store, keys)
	l3 := lead(t, st, keys, o3, testConfig())
	for _, tc := range []struct{ above, bump uint64 }{
		{0, o.Head}, {o.Head - 1, o.Head}, {o.Head, o2.Head}, {o2.Head - 1, o2.Head}, {o2.Head, o3.Head}, {o3.Head, 0}, {math.MaxUint64, 0},
	} {
		if _, bump, head, ok := l3.Lookup(nil, tc.above); bump != tc.bump || head != o3.Head || !ok {
			t.Fatalf("Lookup above %d: bump %d, head %d, %v; want bump %d, head %d", tc.above, bump, head, ok, tc.bump, o3.Head)
		}
	}
	// A manifest above a number leaves the headers below it out.
	l3.cmu.Lock()
	l3.manifestTxn = o.Head
	l3.cmu.Unlock()
	if _, _, _, ok := l3.Lookup(nil, o.Head-1); ok {
		t.Fatal("Lookup below the manifest's transaction reported its range covered")
	}
	if _, _, _, ok := l3.Lookup(nil, o.Head); !ok {
		t.Fatal("Lookup at the manifest's transaction reported its range not covered")
	}
}

// closed reports whether ch is closed within d.
func closed(ch <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return true
	default:
	}
	select {
	case <-ch:
		return true
	case <-t.C:
		return false
	}
}

// Covers: requirement 15's rolled-back transaction: maintenance that rolls
// back an idle connection's open transaction closes its Aborted channel at
// once, and its next Run says why; a connection with no transaction keeps
// its channel open; a stop closes every connection's channel.
func TestAbortedAfterMaintenance(t *testing.T) {
	_, _, _, l, w := setup(t)
	idle := open(t, l)
	if _, err := w.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
		return db.Exec("BEGIN IMMEDIATE; INSERT INTO t VALUES (1)")
	}); err != nil {
		t.Fatal(err)
	}
	if closed(w.Aborted(), 0) {
		t.Fatal("Aborted closed before maintenance")
	}
	startMaintenance(l)
	if !closed(w.Aborted(), 5*time.Second) {
		t.Fatal("maintenance rolled the transaction back without closing Aborted")
	}
	waitMaintenance(t, l)
	if _, err := w.Run(context.Background(), nil, func(*sqlite3.Conn) error { return nil }); !errors.Is(err, ErrNotCommitted) || !errors.Is(err, ErrRetry) {
		t.Fatalf("the next Run after Aborted: %v", err)
	}
	if closed(idle.Aborted(), 0) {
		t.Fatal("Aborted closed for a connection with no transaction")
	}
	_ = l.stop(errors.New("test stop"))
	if !closed(idle.Aborted(), 0) {
		t.Fatal("the leader stopped without closing Aborted")
	}
}

// Covers: requirement 15's channel ends with the connection: Close closes
// it, and so does a discard after a panic.
func TestAbortedAtClose(t *testing.T) {
	_, _, _, l, _ := setup(t)
	c := open(t, l)
	if err := c.Close(); err != nil || !closed(c.Aborted(), 0) {
		t.Fatalf("Close: %v; Aborted closed: %v", err, closed(c.Aborted(), 0))
	}
	p := open(t, l)
	if _, err := p.Run(context.Background(), nil, func(*sqlite3.Conn) error { panic("boom") }); err == nil || !closed(p.Aborted(), 0) {
		t.Fatalf("a Run that panicked: %v; Aborted closed: %v", err, closed(p.Aborted(), 0))
	}
}

// Covers: record 0007's requirement 5 for the engine's set-up: Open's
// context bounds it. An Open parked in a storage read ends when its context
// does, is not told to retry, and leaves the leader serving; one with a done
// context opens nothing.
func TestOpenContextBoundsSetup(t *testing.T) {
	var armed atomic.Bool
	hs, l, w := hungReadSetup(t, func(_ context.Context, op string) bool { return op == "getrange" && armed.Load() })
	armed.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	opened := make(chan error, 1)
	go func() {
		c, err := l.Open(ctx)
		if err == nil {
			err = errors.Join(errors.New("opened"), c.Close())
		}
		opened <- err
	}()
	reached(t, hs)
	cancel()
	if err := within(t, hs, opened, "the Open parked in a storage read"); err == nil || errors.Is(err, ErrRetry) || bytes.Contains([]byte(err.Error()), []byte("opened")) {
		t.Fatalf("Open ended by its context: %v, want a failure not told to retry", err)
	}
	armed.Store(false)
	if _, err := l.Open(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with a done context: %v", err)
	}
	l.cmu.Lock()
	n := len(l.conns)
	l.cmu.Unlock()
	if n != 2 || l.Err() != nil {
		t.Fatalf("%d connections after the failed Opens, want 2; leader: %v", n, l.Err())
	}
	if got := queryConn(t, w, "SELECT count(*) FROM t"); got[0] != "50" {
		t.Fatalf("after the failed Opens: %v", got)
	}
}

// Covers: record 0007's requirement 5 for SQLite's half of the set-up: Open's
// context interrupts the set-up statements, which load the schema. A schema
// large enough that loading it takes measurable time, and a deadline shorter
// than an unbounded Open, ends the Open with SQLite's interrupt.
func TestOpenContextInterruptsSetup(t *testing.T) {
	_, _, _, l, w := setup(t)
	var b strings.Builder
	b.WriteString("BEGIN;")
	for i := range 1000 {
		fmt.Fprintf(&b, "CREATE VIEW v%d AS SELECT x, x + %d FROM t WHERE x > %d;", i, i, i)
	}
	b.WriteString("COMMIT")
	mustRun(t, w, b.String())
	// The fastest of a few unbounded Opens: the first is slower, cold.
	d := time.Hour
	for range 3 {
		begin := time.Now()
		c, err := l.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		d = min(d, time.Since(begin))
		_ = c.Close()
	}
	var errs []error
	for range 10 {
		ctx, cancel := context.WithTimeout(context.Background(), d/2)
		c, err := l.Open(ctx)
		cancel()
		if errors.Is(err, sqlite3.INTERRUPT) {
			return
		}
		if err == nil {
			_ = c.Close()
		}
		errs = append(errs, err)
	}
	t.Fatalf("no Open with a deadline of %v, half the fastest unbounded Open, was interrupted: %v", d/2, errs)
}
