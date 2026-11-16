package engine

// Tests for "Commit outcomes" in record 0004, and the interleavings under
// "Interleavings considered" that involve a commit, an upload outcome or a
// fenced leader.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// commitRules makes the n-th create of a commit-log key (from 1) behave as
// faults[n-1], and every later one as the last fault.
func commitRules(faults ...fault) func(op, key string) fault {
	var mu sync.Mutex
	n := 0
	return func(op, key string) fault {
		if op != "create" || !isCommit(key) {
			return fault{}
		}
		mu.Lock()
		defer mu.Unlock()
		n++
		return faults[min(n, len(faults))-1]
	}
}

// retire replaces commit txn with its retired form, as garbage collection
// does.
func retire(t testing.TB, st storage.Store, keys format.Keys, db format.DatabaseID, txn uint64) {
	t.Helper()
	ctx := context.Background()
	key := keys.Commit(db, txn)
	rc, info, err := st.GetRange(ctx, key, 0, format.CommitHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	h, err := format.DecodeCommitHeader(b, db)
	if err != nil {
		t.Fatal(err)
	}
	r, err := h.Retire()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Replace(ctx, key, info.Version, bytes.NewReader(r)); err != nil {
		t.Fatal(err)
	}
}

// setup creates a database with table t and one row, led by a leader.
func setup(t *testing.T) (*faultStore, format.Keys, Office, *Leader, *Conn) {
	t.Helper()
	st, keys, o := newDatabase(t, 4096)
	l := lead(t, st, keys, o, testConfig())
	c := open(t, l)
	mustRun(t, c, "BEGIN; CREATE TABLE t(x); INSERT INTO t VALUES (0); COMMIT")
	return st, keys, o, l, c
}

func rows(t *testing.T, db *sqlite3.Conn) string {
	t.Helper()
	return strings.Join(mustQuery(t, db, "SELECT x FROM t ORDER BY x"), ",")
}

// Covers: Commit outcomes, "A create whose response is lost is repeated
// with identical bytes. A repeat that succeeds is reported committed. A
// repeat that fails its precondition reads the name: the leader's own
// commit ... reports committed"; and the lost-response interleaving with
// the name not yet retired.
func TestLostResponseIsRepeated(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first fault
	}{
		{"repeat succeeds", fault{unknownNone: true}},
		{"repeat meets own commit", fault{drop: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, keys, o, l, c := setup(t)
			st.setRule(commitRules(tc.first, fault{}))
			txn, err := exec(c, "INSERT INTO t VALUES (1)")
			if err != nil || txn != l.Head() || txn != o.Head+2 {
				t.Fatalf("commit at %d (head %d): %v", txn, l.Head(), err)
			}
			if _, creates, _ := st.counts(); creates < 2 {
				t.Fatalf("%d creates, want the repeat", creates)
			}
			if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
				t.Fatalf("durable rows %s", got)
			}
			mustRun(t, c, "INSERT INTO t VALUES (2)")
			if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1,2" {
				t.Fatalf("durable rows %s", got)
			}
		})
	}
}

// Covers: Commit outcomes, the leader's own commit "retired or not ...
// reports committed ... tested with the name's object retired before,
// during and after the read"; Interleavings, "The lost-response commit
// (first review)", "The freed-name sequence" and "A retire races a
// leader's read-back". A later leader's takeover bumps the next name and
// retires the commit; the old leader reports committed and commits nothing
// further. The commit is also an INSERT ... RETURNING closed after one
// step, which SQLite completes inside sqlite3_finalize.
func TestLostResponseRetiredByLaterLeader(t *testing.T) {
	for _, name := range []string{"before", "during", "after", "before, closed early"} {
		t.Run(name, func(t *testing.T) {
			when, early := strings.CutSuffix(name, ", closed early")
			st, keys, o, l, c := setup(t)
			txn := o.Head + 2
			landed, read := newGate(), newGate()
			st.setRule(func(op, key string) fault {
				switch {
				case op == "create" && key == keys.Commit(o.DB, txn):
					st.setRule(func(op, key string) fault {
						switch {
						case op == "get" && key == keys.Commit(o.DB, txn) && when == "before":
							return fault{before: read}
						case op == "get" && key == keys.Commit(o.DB, txn) && when == "during":
							return fault{after: read}
						}
						return fault{}
					})
					return fault{after: landed, drop: true}
				}
				return fault{}
			})
			type result struct {
				txn uint64
				err error
			}
			done := make(chan result)
			go func() {
				if !early {
					txn, err := exec(c, "INSERT INTO t VALUES (1)")
					done <- result{txn, err}
					return
				}
				txn, err := c.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
					s, _, err := db.Prepare("INSERT INTO t VALUES (1), (2) RETURNING x")
					if err != nil {
						return err
					}
					s.Step()
					return s.Close()
				})
				done <- result{txn, err}
			}()
			<-landed.reached
			// Candidate B takes over: its bump lands after A's commit.
			takeOffice(t, st.Store, keys)
			landed.open()
			if when != "after" {
				<-read.reached
				retire(t, st.Store, keys, o.DB, txn)
				read.open()
			}
			res := <-done
			if res.err != nil || res.txn != txn {
				t.Fatalf("commit: %d, %v; want committed at %d", res.txn, res.err, txn)
			}
			// Reading its own commit already retired, A knows it is deposed
			// and stops at once, before any later write meets the bump.
			if when == "before" && !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("a leader that read its commit retired still serves: %v", l.Err())
			}
			if when == "after" {
				retire(t, st.Store, keys, o.DB, txn)
			}
			// A commits nothing further.
			if _, err := exec(c, "INSERT INTO t VALUES (2)"); err == nil {
				t.Fatal("a deposed leader committed again")
			}
			if !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("leader still serving: %v", l.Err())
			}
			if ids := commitIDs(t, st.Store, keys, o.DB, txn); len(ids) != 2 || ids[1] != "bump" {
				t.Fatalf("log after %d: %v", txn-1, ids)
			}
		})
	}
}

// Covers: Commit outcomes, "an epoch bump fences the leader and reports not
// committed"; Interleavings, "The garbage-collected takeover commit (first
// review)": a takeover's bump already holds the leader's next name.
func TestFencedByEpochBump(t *testing.T) {
	st, keys, o, l, c := setup(t)
	takeOffice(t, st.Store, keys)
	_, err := exec(c, "INSERT INTO t VALUES (1)")
	if !errors.Is(err, ErrNotCommitted) || !errors.Is(err, ErrFenced) {
		t.Fatalf("commit: %v, want not committed and fenced", err)
	}
	if !errors.Is(l.Err(), ErrStopped) {
		t.Fatalf("leader still serving after being fenced: %v", l.Err())
	}
	if _, err := l.Open(); !errors.Is(err, ErrStopped) {
		t.Fatalf("open after fencing: %v", err)
	}
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0" {
		t.Fatalf("durable rows %s", got)
	}
	_ = o
}

// Covers: Commit outcomes, "A non-bump at the leader's next name whose
// epoch, transaction number, commit identifier or bytes are not the
// leader's, and a precondition failure followed by a read that finds
// nothing, each stop the node with unknown." (The transaction number is
// bound to the key and checked by CheckKey.)
func TestForeignObjectAtNextNameStops(t *testing.T) {
	id := []byte("commit-identifier-000001")
	// A retired object keeps only its header: the identifier and epoch are
	// all there is to tell another leader's commit from this one's.
	for _, tc := range []string{"identifier", "epoch", "bytes", "absent", "identifier, retired", "epoch, retired"} {
		t.Run(tc, func(t *testing.T) {
			st, keys, o, l, c := setup(t)
			txn := o.Head + 2
			if tc == "absent" {
				st.setRule(commitRules(fault{noEffect: true, err: storage.ErrPreconditionFailed}))
			} else {
				h := format.CommitHeader{DB: o.DB, PageSize: 4096, Txn: txn, Epoch: o.Epoch, DBSize: 2, PageCount: 1, CommitID: id}
				switch strings.TrimSuffix(tc, ", retired") {
				case "identifier":
					h.CommitID = []byte("someone-elses-identifier")
				case "epoch":
					h.Epoch++
				}
				var b bytes.Buffer
				if err := format.EncodeCommit(&b, h, []uint32{2}, bytes.NewReader(make([]byte, 4096))); err != nil {
					t.Fatal(err)
				}
				if _, err := st.Store.Create(context.Background(), keys.Commit(o.DB, txn), &b); err != nil {
					t.Fatal(err)
				}
				if strings.HasSuffix(tc, ", retired") {
					retire(t, st.Store, keys, o.DB, txn)
				}
			}
			_, err := c.Run(context.Background(), id, func(db *sqlite3.Conn) error { return db.Exec("INSERT INTO t VALUES (1)") })
			if !errors.Is(err, ErrUnknown) || errors.Is(err, ErrNotCommitted) {
				t.Fatalf("commit: %v, want unknown", err)
			}
			if !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("leader still serving: %v", l.Err())
			}
		})
	}
}

// Covers: Commit outcomes, "An upload whose every attempt definitely had no
// effect, followed by the WAL truncation: the client is told not
// committed, and a different transaction then succeeds on the same leader
// at the same transaction number"; Interleavings, "A definitely failed
// commit (fourth review)".
func TestDefinitelyFailedCommitReleasesNumber(t *testing.T) {
	st, keys, o, l, c := setup(t)
	walSize := func() int64 {
		fi, err := os.Stat(l.path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	before := walSize()
	st.setRule(commitRules(fault{noEffect: true}))
	txn, err := exec(c, "INSERT INTO t VALUES (1)")
	if txn != 0 || !errors.Is(err, ErrNotCommitted) || errors.Is(err, ErrUnknown) {
		t.Fatalf("commit: %d, %v; want not committed", txn, err)
	}
	// The failed transaction's frames are gone from the local WAL, where
	// SQLite's recovery could otherwise find them.
	if after := walSize(); after != before {
		t.Fatalf("WAL of %d bytes after the failed commit, %d before it", after, before)
	}
	if _, creates, _ := st.counts(); creates < createAttempts {
		t.Fatalf("%d creates, want %d attempts", creates, createAttempts)
	}
	if got := strings.Join(queryConn(t, c, "SELECT x FROM t"), ","); got != "0" {
		t.Fatalf("leader rows %s after a failed commit", got)
	}
	st.setRule(nil)
	if txn := mustRun(t, c, "INSERT INTO t VALUES (2)"); txn != o.Head+2 {
		t.Fatalf("next transaction at %d, want the released %d", txn, o.Head+2)
	}
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,2" {
		t.Fatalf("durable rows %s", got)
	}
	if err := l.Err(); err != nil {
		t.Fatal(err)
	}
}

// Covers: Commit outcomes, "The same with one attempt of unknown outcome
// among them: the leader never builds a different object for that number";
// "An indeterminate commit ... stop[s] the node for that database and
// discard[s] its local state. The node serves again only through takeover.
// A create left outstanding by such a node and landing later lands only at
// the head plus one"; Interleavings, "Sticky uncertainty in a commit (third
// review)" and "A delayed create lands after any amount of time".
func TestStickyUncertaintyStops(t *testing.T) {
	for _, tc := range []struct {
		name      string
		takeFirst bool // a takeover's bump takes the name before the late create lands
	}{{"late create lands", false}, {"takeover first", true}} {
		t.Run(tc.name, func(t *testing.T) {
			st, keys, o, l, c := setup(t)
			late := newGate()
			st.setRule(commitRules(fault{late: late}, fault{noEffect: true}))
			txn, err := exec(c, "INSERT INTO t VALUES (1)")
			if !errors.Is(err, ErrUnknown) || errors.Is(err, ErrNotCommitted) {
				t.Fatalf("commit: %d, %v; want unknown", txn, err)
			}
			if !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("leader still serving: %v", l.Err())
			}
			if _, err := exec(c, "INSERT INTO t VALUES (2)"); !errors.Is(err, ErrStopped) {
				t.Fatalf("after stopping: %v", err)
			}
			dir := l.dir
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("local state left after the stop: %v", err)
			}
			if tc.takeFirst {
				takeOffice(t, st.Store, keys)
				late.open()
				st.late.Wait()
			} else {
				late.open()
				st.late.Wait()
				takeOffice(t, st.Store, keys)
			}
			ids := commitIDs(t, st.Store, keys, o.DB, o.Head+2)
			want := []string{"bump"}
			if !tc.takeFirst {
				want = []string{ids[0], "bump"}
			}
			if !slices.Equal(ids, want) || !tc.takeFirst && ids[0] == "bump" {
				t.Fatalf("log after the leader's head: %v", ids)
			}
			wantRows := "0,1"
			if tc.takeFirst {
				wantRows = "0"
			}
			if got := rows(t, durable(t, st.Store, keys, 0)); got != wantRows {
				t.Fatalf("durable rows %s, want %s", got, wantRows)
			}
		})
	}
}

// Covers: Commit outcomes, "A leader whose create landed, and whose
// response arrives only after another candidate's bump occupies the next
// name, reports that commit committed and commits nothing further";
// decision 6, "Late acknowledgements are correct".
func TestLateAcknowledgementAfterBump(t *testing.T) {
	st, keys, o, l, c := setup(t)
	landed := newGate()
	st.setRule(commitRules(fault{after: landed}, fault{}))
	done := make(chan error)
	var txn uint64
	go func() {
		var err error
		txn, err = exec(c, "INSERT INTO t VALUES (1)")
		done <- err
	}()
	<-landed.reached
	takeOffice(t, st.Store, keys)
	landed.open()
	if err := <-done; err != nil || txn != o.Head+2 {
		t.Fatalf("commit: %d, %v; want committed at %d", txn, err, o.Head+2)
	}
	_, err := exec(c, "INSERT INTO t VALUES (2)")
	if !errors.Is(err, ErrFenced) || !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("next commit: %v, want fenced", err)
	}
	if !errors.Is(l.Err(), ErrStopped) {
		t.Fatal("leader still serving")
	}
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1" {
		t.Fatalf("durable rows %s", got)
	}
}

// Covers: Commit outcomes, "a failed WAL truncation ... stop[s] the node";
// Commit capture and failure, "a failed truncate or fsync".
func TestFailedTruncationStops(t *testing.T) {
	for _, op := range []string{"truncate", "sync"} {
		t.Run(op, func(t *testing.T) {
			st, keys, _, l, c := setup(t)
			st.setRule(commitRules(fault{noEffect: true}))
			var armed bool
			l.fault = func(file, o string, off int64, n int) error {
				if file == "wal" && o == "truncate" {
					armed = true
				}
				if armed && file == "wal" && o == op {
					return errInjected
				}
				return nil
			}
			_, err := exec(c, "INSERT INTO t VALUES (1)")
			if !errors.Is(err, ErrNotCommitted) || !errors.Is(l.Err(), ErrStopped) {
				t.Fatalf("commit: %v; leader: %v; want not committed and stopped", err, l.Err())
			}
			if got := rows(t, durable(t, st.Store, keys, 0)); got != "0" {
				t.Fatalf("durable rows %s", got)
			}
		})
	}
}

// Covers: Commit capture and failure, "No error from the commit point is
// turned into success. Every error code the storage layer can return
// surfaces to the caller as an error", and "The commit point never returns
// BUSY ... No failed COMMIT leaves a transaction open".
func TestCommitPointErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    fault
	}{
		{"no effect", fault{noEffect: true}},
		{"invalid argument", fault{noEffect: true, err: storage.ErrInvalidArgument}},
		{"context", fault{noEffect: true, err: context.DeadlineExceeded}},
		{"not found", fault{noEffect: true, err: storage.ErrNotFound}},
		{"unknown", fault{unknownNone: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, _, _, _, c := setup(t)
			st.setRule(commitRules(tc.f))
			for _, sql := range []string{"INSERT INTO t VALUES (1)", "BEGIN; INSERT INTO t VALUES (1); COMMIT"} {
				var open bool
				txn, err := c.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
					err := db.Exec(sql)
					open = !db.GetAutocommit()
					if open {
						_ = db.Exec("ROLLBACK")
					}
					return err
				})
				if err == nil || txn != 0 {
					t.Fatalf("%s: %d, %v; want an error", sql, txn, err)
				}
				if errors.Is(err, sqlite3.BUSY) || open {
					t.Fatalf("%s: BUSY or a transaction left open: %v", sql, err)
				}
				if errors.Is(err, ErrUnknown) {
					break // the leader has stopped
				}
			}
		})
	}
}

// Covers: decision 5, "A transaction that changes no page creates no commit
// object"; Commit capture and failure, "A write transaction that changes no
// page, and a read-only transaction, create no commit object and take no
// transaction number"; the commit identifier rules of this version.
func TestNoChangeCommitsNothing(t *testing.T) {
	st, keys, o, l, c := setup(t)
	head := l.Head()
	for _, sql := range []string{
		"SELECT * FROM t",
		"BEGIN; SELECT * FROM t; COMMIT",
		"BEGIN IMMEDIATE; COMMIT",
		"UPDATE t SET x = 5 WHERE x = 99",
		"UPDATE t SET x = x",
	} {
		// No identifier needed: nothing commits.
		txn, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error { return db.Exec(sql) })
		if err != nil || txn != 0 {
			t.Fatalf("%s: %d, %v", sql, txn, err)
		}
	}
	if l.Head() != head {
		t.Fatalf("head moved from %d to %d", head, l.Head())
	}
	// A change needs an identifier, and one identifier commits once.
	if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error { return db.Exec("INSERT INTO t VALUES (1)") }); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("commit without an identifier: %v", err)
	}
	txn, err := c.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		return db.Exec("INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)")
	})
	// The first commits; the second is refused, and the error says so
	// without reading as the Run's outcome.
	if txn != head+1 || err == nil || errors.Is(err, ErrNotCommitted) || !strings.Contains(err.Error(), "a second commit") {
		t.Fatalf("two commits with one identifier: %d, %v", txn, err)
	}
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,2" {
		t.Fatalf("durable rows %s", got)
	}
	if ids := commitIDs(t, st.Store, keys, o.DB, o.Head+1); len(ids) != 2 {
		t.Fatalf("log after the bump: %v", ids)
	}
}
