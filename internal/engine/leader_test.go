package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ncruces/go-sqlite3"
)

// TestLeaderCommits is the smallest round trip: commits through SQLite
// become commit objects at consecutive numbers after the epoch bump, and a
// fresh reader of storage sees them.
func TestLeaderCommits(t *testing.T) {
	st, keys, o := newDatabase(t, 4096)
	l := lead(t, st, keys, o, testConfig())
	c := open(t, l)
	if got := mustRun(t, c, "CREATE TABLE t(x)"); got != o.Head+1 {
		t.Fatalf("first commit at %d, want %d", got, o.Head+1)
	}
	for i := range 3 {
		if got := mustRun(t, c, "INSERT INTO t VALUES (1)"); got != o.Head+2+uint64(i) {
			t.Fatalf("commit at %d", got)
		}
	}
	db := durable(t, st.Store, keys, 0)
	if got := mustQuery(t, db, "SELECT count(*) FROM t"); !slices.Equal(got, []string{"3"}) {
		t.Fatalf("durable rows %v", got)
	}
	if got := queryConn(t, c, "SELECT count(*) FROM t"); !slices.Equal(got, []string{"3"}) {
		t.Fatalf("leader rows %v", got)
	}
}

// Covers: decision 7, leader readers see consistent snapshots, under
// -race: writers queue for the write lock and commit concurrently, readers
// check in each snapshot that the balances sum to the total and that the
// transfers they see never go backwards, and checkpoints of each mode run
// throughout. Afterwards the acknowledged numbers are exactly the log after
// the epoch bump and storage holds every transfer.
func TestLeaderConcurrency(t *testing.T) {
	const writers, transfers, readers, accounts = 4, 25, 3, 20
	st, keys, o := newDatabase(t, 1024)
	l := lead(t, st, keys, o, testConfig())
	c := open(t, l)
	mustRun(t, c, fmt.Sprintf("BEGIN; CREATE TABLE acct(id INTEGER PRIMARY KEY, bal); CREATE TABLE xfer(n INTEGER PRIMARY KEY, pad); INSERT INTO acct SELECT value, 100 FROM generate_series(1, %d); COMMIT", accounts))
	const total = 100 * accounts

	var (
		wg      sync.WaitGroup
		done    atomic.Bool
		mu      sync.Mutex
		txns    []uint64
		failure = make(chan error, writers+readers+1)
	)
	for w := range writers {
		wc := open(t, l)
		wg.Go(func() {
			for i := range transfers {
				from, to := 1+(w*7+i)%accounts, 1+(w*3+i*5+1)%accounts
				txn, err := exec(wc, fmt.Sprintf("BEGIN IMMEDIATE; UPDATE acct SET bal = bal - 7 WHERE id = %d; UPDATE acct SET bal = bal + 7 WHERE id = %d; INSERT INTO xfer(pad) VALUES (zeroblob(%d)); COMMIT", from, to, 100*i))
				if err != nil {
					failure <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
				mu.Lock()
				txns = append(txns, txn)
				mu.Unlock()
			}
		})
	}
	var rg sync.WaitGroup
	for r := range readers {
		rc := open(t, l)
		rg.Go(func() {
			last := 0
			for !done.Load() {
				if _, err := rc.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
					if err := db.Exec("BEGIN"); err != nil {
						return err
					}
					defer func() { _ = db.Exec("COMMIT") }()
					sum, err := query(db, "SELECT sum(bal) FROM acct")
					if err != nil {
						return err
					}
					n, err := query(db, "SELECT count(*) FROM xfer")
					if err != nil {
						return err
					}
					if sum[0] != strconv.Itoa(total) {
						return fmt.Errorf("reader %d: balances sum to %s", r, sum[0])
					}
					seen, _ := strconv.Atoi(n[0])
					if seen < last {
						return fmt.Errorf("reader %d: %d transfers after %d", r, seen, last)
					}
					last = seen
					return nil
				}); err != nil {
					failure <- err
					return
				}
			}
		})
	}
	cc := open(t, l)
	rg.Go(func() {
		modes := []sqlite3.CheckpointMode{sqlite3.CHECKPOINT_PASSIVE, sqlite3.CHECKPOINT_RESTART, sqlite3.CHECKPOINT_TRUNCATE}
		for i := 0; !done.Load(); i++ {
			if _, err := cc.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
				_, _, err := db.WALCheckpoint("main", modes[i%len(modes)])
				return err
			}); err != nil && !errors.Is(err, sqlite3.BUSY) {
				failure <- fmt.Errorf("checkpoint: %w", err)
				return
			}
		}
	})
	wg.Wait()
	done.Store(true)
	rg.Wait()
	close(failure)
	for err := range failure {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	slices.Sort(txns)
	for i, txn := range txns {
		if txn != o.Head+2+uint64(i) { //nolint:gosec // small
			t.Fatalf("acknowledged numbers %v, want %d.. without gaps", txns, o.Head+2)
		}
	}
	if l.Head() != txns[len(txns)-1] {
		t.Fatalf("head %d, last acknowledged %d", l.Head(), txns[len(txns)-1])
	}
	db := durable(t, st.Store, keys, 0)
	if got := mustQuery(t, db, "SELECT (SELECT sum(bal) FROM acct), (SELECT count(*) FROM xfer)"); got[0] != fmt.Sprintf("%d|%d", total, writers*transfers) {
		t.Fatalf("durable sum and transfers: %s", got[0])
	}
}

// Covers: decision 9's final check as Lead makes it: an office whose epoch
// or nonce the manifest does not carry, or whose head is not its own epoch
// bump, is fenced, and Lead refuses it.
func TestLeadChecksOffice(t *testing.T) {
	st, keys, o := newDatabase(t, 4096)
	l := lead(t, st, keys, o, testConfig())
	txn := mustRun(t, open(t, l), "CREATE TABLE t(x)")
	_ = l.Close()
	nonce := o.Nonce
	nonce[0] ^= 1
	o2 := o // filled in below, after a later takeover
	for _, tc := range []struct {
		name string
		o    func() Office
	}{
		{"another epoch", func() Office { return Office{DB: o.DB, Epoch: o.Epoch + 1, Head: o.Head, Nonce: o.Nonce} }},
		{"another nonce", func() Office { return Office{DB: o.DB, Epoch: o.Epoch, Head: o.Head, Nonce: nonce} }},
		{"head not a bump", func() Office { return Office{DB: o.DB, Epoch: o.Epoch, Head: txn, Nonce: o.Nonce} }},
		{"head an earlier epoch's bump", func() Office {
			o2 = takeOffice(t, st.Store, keys)
			return Office{DB: o.DB, Epoch: o2.Epoch, Head: o.Head, Nonce: o2.Nonce}
		}},
	} {
		if _, err := Lead(context.Background(), st, keys, t.TempDir(), tc.o(), testConfig()); !errors.Is(err, ErrFenced) {
			t.Fatalf("%s: %v, want fenced", tc.name, err)
		}
	}
	lead(t, st, keys, o2, testConfig())
}

// TestOpenFailsStoppedWhenTheLeaderStops: an Open whose storage reads meet
// a leader that stopped meanwhile, as one fenced by a later candidacy's
// manifest does, fails with ErrStopped, which a caller can tell from a
// failure of the database, not with the I/O error SQLite turns it into.
func TestOpenFailsStoppedWhenTheLeaderStops(t *testing.T) {
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Cache = NewCache(0) // every page read reaches storage
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	l.tryCheckpoint() // the base version is the head: pages come from the log
	gate := newGate()
	st.setRule(nth("getrange", isCommit, 1, fault{before: gate}))
	opened := make(chan error, 1)
	go func() {
		c, err := l.Open(context.Background())
		if err == nil {
			_ = c.Close()
		}
		opened <- err
	}()
	<-gate.reached
	_ = l.stop(fmt.Errorf("%w: the manifest changed under the leader", ErrFenced))
	gate.open()
	if err := <-opened; !errors.Is(err, ErrStopped) {
		t.Fatalf("Open across the leader's stop: %v", err)
	}
}
