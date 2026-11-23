package engine

// Tests for decision 8 of record 0004 on a leader: compaction while it
// serves, at its bounds, with faults, against a stale leader, and the grace
// period.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// fastCompaction removes the pace between manifest writes and makes the
// compaction loop look for work every few milliseconds.
func fastCompaction(t *testing.T) {
	t.Helper()
	pace, tick := compactPace, compactTick
	compactPace, compactTick = 0, 5*time.Millisecond
	t.Cleanup(func() { compactPace, compactTick = pace, tick })
}

// quietCompaction removes the pace between manifest writes and keeps the
// compaction loop asleep unless a commit wakes it: tests then run each
// compaction, retirement and collection themselves.
func quietCompaction(t *testing.T) {
	t.Helper()
	pace, tick := compactPace, compactTick
	compactPace, compactTick = 0, time.Hour
	t.Cleanup(func() { compactPace, compactTick = pace, tick })
}

// onTmpfs puts the test's directories on tmpfs where it is available, so
// that fsync costs nothing: these tests are about the log, not the disk.
func onTmpfs(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/shm"); err == nil {
		t.Setenv("TMPDIR", "/dev/shm")
	}
}

// blockCompaction makes every replacement of the manifest fail with no
// effect, so that no compaction publishes; takeOffice writes around it.
func blockCompaction(st *faultStore, keys format.Keys) {
	st.setRule(func(op, key string) fault {
		if op == "replace" && key == keys.Manifest() {
			return fault{noEffect: true}
		}
		return fault{}
	})
}

// eventually polls cond until it holds, for up to ten seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(end) {
			t.Fatalf("%s: not within 10s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// checkLeaderManifest fails unless every base object the manifest in
// storage references exists, and every commit up to its retired-through
// cursor reads as retired or as an epoch bump.
func checkLeaderManifest(t *testing.T, st storage.Store, keys format.Keys, db format.DatabaseID) format.Manifest {
	t.Helper()
	ctx := context.Background()
	b, _, err := readObject(ctx, st, keys.Manifest(), format.MaxManifestSize+1)
	if err != nil {
		t.Fatal(err)
	}
	m, err := format.DecodeManifest(b, db)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range referenced(keys, m) {
		if _, err := st.Head(ctx, k); err != nil {
			t.Fatalf("manifest at %d references %s: %v", m.Txn, k, err)
		}
	}
	for k := uint64(1); k <= m.RetiredThrough; k++ {
		h, _, err := readHeader(ctx, st, keys, db, k)
		if err != nil || !h.Retired && !h.EpochBump {
			t.Fatalf("commit %d at or below retired-through %d: retired %v, %v", k, m.RetiredThrough, h.Retired, err)
		}
	}
	return m
}

// compactNow runs one compaction attempt on l.
func compactNow(l *Leader) error {
	l.cmpMu.Lock()
	defer l.cmpMu.Unlock()
	return l.compactLocked()
}

// Covers: the reason for decision 8: a database runs well past the tail
// bound of 32,768 commits, compacting, retiring and collecting as it goes,
// with every commit durable: 100,000 commits, or SKYLITE_COMMITS.
func TestLeaderRunsPastTailBound(t *testing.T) {
	fastCompaction(t)
	n := 100_000
	if v, err := strconv.Atoi(os.Getenv("SKYLITE_COMMITS")); err == nil {
		n = v
	}
	onTmpfs(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace = time.Millisecond
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	start := time.Now()
	for i := range n {
		if _, err := exec(c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i)); err != nil {
			t.Fatalf("commit %d after %v: %v", i, time.Since(start), err)
		}
	}
	t.Logf("%d commits in %v", n, time.Since(start))
	l.cmu.Lock()
	head, mtxn := l.head, l.manifestTxn
	l.cmu.Unlock()
	if mtxn == 0 || head-mtxn > cfg.TailBound {
		t.Fatalf("head %d, manifest at %d", head, mtxn)
	}
	got := mustQuery(t, durable(t, st.Store, keys, 0), "SELECT count(*), sum(x) FROM t")
	if want := fmt.Sprintf("%d|%d", n, n*(n-1)/2); fmt.Sprint(got) != "["+want+"]" {
		t.Fatalf("durable rows %v, want %s", got, want)
	}
	l.cmpMu.Lock()
	retired := l.retiredThrough
	l.cmpMu.Unlock()
	if retired == 0 {
		t.Fatal("nothing retired")
	}
	t.Logf("head %d, manifest at %d, retired through %d", head, mtxn, retired)
}

// Covers: Reads, against the oracle while compaction runs: with one writer
// adding one row per commit, reads on the leader through Snapshot count
// exactly the rows of the number reported, and readers built from storage
// at a transaction the manifest still serves read its rows, while the
// leader compacts, retires and collects under a small tail bound. Writes
// and reads refused while maintenance runs are retried.
func TestLeaderCompactionReadsMatchOracle(t *testing.T) {
	fastCompaction(t)
	onTmpfs(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.TailBound, cfg.Grace = 64, time.Millisecond
	l := lead(t, st, keys, o, cfg)
	w := open(t, l)
	created := mustRun(t, w, "CREATE TABLE t(x)")
	readers := []*Conn{open(t, l), open(t, l)}
	stop := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for _, r := range readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				n, count, err := readAt(l, r, func() {}, func() {})
				switch {
				case errors.Is(err, ErrRetry):
				case err != nil:
					errs <- err
					return
				case uint64(count) != n-created: //nolint:gosec // a row count
					errs <- fmt.Errorf("a read at %d counted %d rows", n, count)
					return
				}
			}
		})
	}
	durableReads := 0
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			l.cmu.Lock()
			txn, head := max(l.manifestTxn, created+1), l.head
			l.cmu.Unlock()
			if txn > head {
				continue
			}
			db, err := durableErr(t, st.Store, keys, txn)
			switch {
			case errors.Is(err, ErrCompacted):
				continue
			case err != nil:
				errs <- fmt.Errorf("a reader at %d: %w", txn, err)
				return
			}
			got, err := query(db, "SELECT count(*) FROM t")
			_ = db.Close()
			if err != nil || got[0] != strconv.FormatUint(txn-created, 10) {
				errs <- fmt.Errorf("a reader at %d counted %v rows: %w", txn, got, err)
				return
			}
			durableReads++
		}
	})
	retries := 0
	for i := 0; i < 3000; {
		_, err := exec(w, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
		switch {
		case errors.Is(err, ErrRetry):
			retries++
			time.Sleep(time.Millisecond)
		case err != nil:
			t.Fatalf("commit %d: %v", i, err)
		default:
			i++
		}
		select {
		case err := <-errs:
			t.Fatal(err)
		default:
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	m := checkLeaderManifest(t, st.Store, keys, o.DB)
	t.Logf("manifest at %d, retired through %d, %d retried commits, %d durable reads", m.Txn, m.RetiredThrough, retries, durableReads)
	if m.Txn <= created || durableReads == 0 {
		t.Fatalf("manifest at %d after the table at %d, %d durable reads", m.Txn, created, durableReads)
	}
	if got := rows(t, durable(t, st.Store, keys, 0)); strings.Count(got, ",") != 2999 {
		t.Fatalf("%d durable rows", strings.Count(got, ",")+1)
	}
}

// Covers: Fencing, takeover and garbage collection, "After a takeover, a
// stale leader cannot ... make its compaction reachable, and cannot delete
// an object that any later manifest references", and "A leader that reads
// a manifest with a higher epoch than its own stops compacting and leaves
// office": a leader paused before its manifest replacement while another
// takes over, commits and compacts loses the replacement and stops; every
// acknowledged commit of both is in the history, and its output is
// collected only once the new leader's manifest passes its target.
func TestStaleLeaderCompaction(t *testing.T) {
	fastCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Hour, time.Hour
	a := lead(t, st, keys, o, cfg)
	ca := open(t, a)
	mustRun(t, ca, "CREATE TABLE t(x)")
	for i := range 20 {
		mustRun(t, ca, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	gate := newGate()
	st.setRule(nth("replace", func(k string) bool { return k == keys.Manifest() }, 1, fault{before: gate}))
	done := make(chan error, 1)
	go func() { done <- compactNow(a) }()
	<-gate.reached
	ctx := context.Background()
	staleOutput, err := st.Store.List(ctx, keys.BasePrefix(o.DB), "", 100)
	if err != nil || len(staleOutput) == 0 {
		t.Fatalf("the stale leader's output: %v, %v", staleOutput, err)
	}

	ob := takeOffice(t, st.Store, keys)
	b := lead(t, st, keys, ob, cfg)
	cb := open(t, b)
	mustRun(t, cb, "INSERT INTO t VALUES (100)")
	if err := compactNow(b); err != nil {
		t.Fatal(err)
	}
	gate.open()
	if err := <-done; !errors.Is(err, ErrStopped) || !errors.Is(a.Err(), ErrStopped) || !strings.Contains(a.Err().Error(), "fenced") {
		t.Fatalf("the stale leader's compaction: %v; leader: %v", err, a.Err())
	}
	if _, err := exec(ca, "INSERT INTO t VALUES (200)"); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("a commit on the stale leader: %v", err)
	}
	want := "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,100"
	if got := rows(t, durable(t, st.Store, keys, 0)); got != want {
		t.Fatalf("durable rows %s", got)
	}
	// The new leader's manifest is past the stale target: its collection
	// deletes the stale output, and nothing it references.
	b.cmpMu.Lock()
	b.cfg.Grace = time.Nanosecond
	b.collectLocked()
	b.collectLocked()
	b.cmpMu.Unlock()
	for _, e := range staleOutput {
		if _, err := st.Store.Head(ctx, e.Key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("the stale output %s: %v", e.Key, err)
		}
	}
	checkLeaderManifest(t, st.Store, keys, o.DB)
	if got := rows(t, durable(t, st.Store, keys, 0)); got != want {
		t.Fatalf("durable rows after collection %s", got)
	}
}

// Covers: Commit outcomes, "A leader whose compaction replacement has no
// definite answer leaves office, and its output is not deleted while the
// manifest is below the output's transaction number"; the replacement
// lands later, and the next leader serves every acknowledged commit.
func TestCompactionUnresolvedLeavesOffice(t *testing.T) {
	fastCompaction(t)
	ctx := context.Background()
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Hour, time.Hour
	a := lead(t, st, keys, o, cfg)
	ca := open(t, a)
	mustRun(t, ca, "CREATE TABLE t(x)")
	for i := range 10 {
		mustRun(t, ca, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	late := newGate()
	first := true
	st.setRule(func(op, key string) fault {
		if op != "replace" || key != keys.Manifest() {
			return fault{}
		}
		if first {
			first = false
			return fault{late: late}
		}
		return fault{unknownNone: true}
	})
	if err := compactNow(a); !errors.Is(err, ErrStopped) || !errors.Is(a.Err(), ErrStopped) {
		t.Fatalf("an unresolved compaction: %v; leader %v", err, a.Err())
	}
	output, _ := st.Store.List(ctx, keys.BasePrefix(o.DB), "", 100)
	if len(output) == 0 {
		t.Fatal("no output")
	}
	m := checkLeaderManifest(t, st.Store, keys, o.DB)
	if g, err := findGarbage(ctx, st.Store, keys, Office{DB: m.DB, Epoch: m.Epoch, Nonce: m.Nonce}, maxGarbage); err != nil || len(g) != 0 {
		t.Fatalf("garbage below the output's transaction: %v, %v", g, err)
	}
	late.open()
	st.late.Wait()
	if m := checkLeaderManifest(t, st.Store, keys, o.DB); m.Txn == 0 {
		t.Fatal("the delayed replacement did not land")
	}
	st.setRule(nil)
	b := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
	cb := open(t, b)
	mustRun(t, cb, "INSERT INTO t VALUES (100)")
	if err := compactNow(b); err != nil {
		t.Fatal(err)
	}
	if got := rows(t, durable(t, st.Store, keys, 0)); got != "0,1,2,3,4,5,6,7,8,9,100" {
		t.Fatalf("durable rows %s", got)
	}
}

// Covers: decision 8, "A leader that reads a manifest whose epoch is higher
// than its own stops compacting and leaves office": a candidate's epoch
// swap, with no bump yet, stops the leader at its next compaction, before
// it writes any base object.
func TestCompactionAfterEpochSwapLeavesOffice(t *testing.T) {
	fastCompaction(t)
	ctx := context.Background()
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.CompactInterval = time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	b, info, err := readObject(ctx, st.Store, keys.Manifest(), format.MaxManifestSize+1)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := format.DecodeManifest(b, o.DB)
	m.Epoch++
	m.Nonce[0]++
	nb, _ := format.EncodeManifest(m)
	if _, err := st.Store.Replace(ctx, keys.Manifest(), info.Version, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
	mustRun(t, c, "INSERT INTO t VALUES (1)") // no bump yet: the leader still commits
	if err := compactNow(l); !errors.Is(err, ErrStopped) || !strings.Contains(l.Err().Error(), "fenced") {
		t.Fatalf("compaction under a higher epoch: %v; leader %v", err, l.Err())
	}
	if list, _ := st.Store.List(ctx, keys.BasePrefix(o.DB), "", 10); len(list) != 0 {
		t.Fatalf("base objects written under a higher epoch: %v", list)
	}
}

// Covers: decision 8's grace period: a reader on the older manifest reads
// its state while commits the newer manifest covers stay unretired and
// dropped base objects stay; after the grace period they are retired and
// collected, and the reader re-anchors on the newer manifest or reports its
// transaction compacted, never wrong data.
func TestGraceKeepsOlderReaders(t *testing.T) {
	fastCompaction(t)
	ctx := context.Background()
	st, keys, o := newDatabase(t, 1024)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = 500*time.Millisecond, time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	for i := range 20 {
		mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	if err := compactNow(l); err != nil {
		t.Fatal(err)
	}
	m1 := checkLeaderManifest(t, st.Store, keys, o.DB)
	for i := range 20 {
		mustRun(t, c, fmt.Sprintf("UPDATE t SET x = x + 1 WHERE x = %d", i))
	}
	r, err := Open(ctx, st.Store, keys, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := make([][]byte, r.Size())
	for p := range want {
		if want[p], err = r.ReadPage(ctx, uint32(p)+1); err != nil { //nolint:gosec // a small database
			t.Fatal(err)
		}
	}
	if err := compactNow(l); err != nil {
		t.Fatal(err)
	}
	m2 := checkLeaderManifest(t, st.Store, keys, o.DB)
	if m2.Txn < r.Txn() {
		t.Fatalf("manifest at %d, reader at %d", m2.Txn, r.Txn())
	}
	dropped := referenced(keys, m1)
	// Within the grace period the reader on the older manifest reads its
	// state from the objects it was anchored on.
	for p := range want {
		got, err := r.ReadPage(ctx, uint32(p)+1) //nolint:gosec // as above
		if err != nil || !bytes.Equal(got, want[p]) {
			t.Fatalf("page %d within the grace period: %v", p+1, err)
		}
	}
	// Retirement and collection that run within the grace period leave
	// both alone.
	l.cmpMu.Lock()
	l.retireLocked()
	l.collectLocked()
	l.collectLocked()
	l.cmpMu.Unlock()
	if h, _, err := readHeader(ctx, st.Store, keys, o.DB, m2.Txn); err != nil || h.Retired {
		t.Fatalf("commit %d within the grace period: retired %v, %v", m2.Txn, h.Retired, err)
	}
	for _, k := range dropped {
		if _, err := st.Store.Head(ctx, k); err != nil {
			t.Fatalf("base object %s within the grace period: %v", k, err)
		}
	}
	eventually(t, "retirement and collection after the grace period", func() bool {
		h, _, err := readHeader(ctx, st.Store, keys, o.DB, m2.Txn)
		if err != nil || !h.Retired {
			return false
		}
		for _, k := range dropped {
			if _, err := st.Store.Head(ctx, k); !errors.Is(err, storage.ErrNotFound) && !referencedBy(keys, m2, k) {
				return false
			}
		}
		return true
	})
	for p := range want {
		got, err := r.ReadPage(ctx, uint32(p)+1) //nolint:gosec // as above
		switch {
		case errors.Is(err, ErrCompacted):
		case err != nil:
			t.Fatalf("page %d after the grace period: %v", p+1, err)
		case !bytes.Equal(got, want[p]):
			t.Fatalf("page %d after the grace period: wrong content", p+1)
		}
	}
	checkLeaderManifest(t, st.Store, keys, o.DB)
}

func referencedBy(keys format.Keys, m format.Manifest, key string) bool {
	for _, k := range referenced(keys, m) {
		if k == key {
			return true
		}
	}
	return false
}

// Covers: Fencing, takeover and garbage collection, "The retired-through
// cursor never decreases, is never above the manifest's transaction
// number, and never passes an unretired ordinary commit. A leader that
// takes office after its predecessor was deposed mid-retirement resumes
// from the cursor and leaves no unretired object at or below the
// manifest's transaction number."
func TestRetiredThroughCursor(t *testing.T) {
	fastCompaction(t)
	ctx := context.Background()
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Hour, time.Hour
	a := lead(t, st, keys, o, cfg)
	ca := open(t, a)
	mustRun(t, ca, "CREATE TABLE t(x)")
	for i := range 30 {
		mustRun(t, ca, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	if err := compactNow(a); err != nil {
		t.Fatal(err)
	}
	m := checkLeaderManifest(t, st.Store, keys, o.DB)
	// A retires the commits it may up to 12, and its retires above fail.
	st.setRule(func(op, key string) fault {
		if _, txn, err := keys.ParseCommit(key); op == "replace" && err == nil && txn > 12 {
			return fault{noEffect: true}
		}
		return fault{}
	})
	a.cmpMu.Lock()
	a.cfg.Grace = time.Nanosecond
	a.retireLocked()
	cursor := a.retiredThrough
	a.cmpMu.Unlock()
	if cursor != 12 || cursor >= m.Txn {
		t.Fatalf("cursor %d after the retires up to 12 below a manifest at %d", cursor, m.Txn)
	}
	mustRun(t, ca, "INSERT INTO t VALUES (100)")
	if err := compactNow(a); err != nil {
		t.Fatal(err)
	}
	m2 := checkLeaderManifest(t, st.Store, keys, o.DB)
	if m2.RetiredThrough != cursor || m2.RetiredThrough < m.RetiredThrough {
		t.Fatalf("published cursor %d, leader's %d, previous %d", m2.RetiredThrough, cursor, m.RetiredThrough)
	}
	// A is deposed mid-retirement; B resumes from the cursor.
	st.setRule(nil)
	cfg.Grace = time.Nanosecond
	b := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
	eventually(t, "the next leader's retirement", func() bool {
		b.cmpMu.Lock()
		defer b.cmpMu.Unlock()
		return b.retiredThrough >= m2.Txn
	})
	mustRun(t, open(t, b), "INSERT INTO t VALUES (200)")
	if err := compactNow(b); err != nil {
		t.Fatal(err)
	}
	m3 := checkLeaderManifest(t, st.Store, keys, o.DB)
	if m3.RetiredThrough < m2.Txn || m3.RetiredThrough > m3.Txn {
		t.Fatalf("cursor %d in a manifest at %d, predecessor at %d", m3.RetiredThrough, m3.Txn, m2.Txn)
	}
	for k := uint64(1); k <= m2.Txn; k++ {
		h, _, err := readHeader(ctx, st.Store, keys, o.DB, k)
		if err != nil || !h.Retired && !h.EpochBump {
			t.Fatalf("commit %d below a manifest at %d left unretired: %v", k, m2.Txn, err)
		}
	}
}

// Covers: decision 6, the leader's header index, once compaction moves the
// manifest: Lookup finds recent identifiers, and reports numbers more than
// the window below the head as not covered instead of answering wrongly.
func TestLookupAfterCompaction(t *testing.T) {
	fastCompaction(t)
	oldTail, oldPages := maxTail, maxTailPages
	maxTail, maxTailPages = 16, 1<<22
	t.Cleanup(func() { maxTail, maxTailPages = oldTail, oldPages })
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.TailBound, cfg.Grace = 8, time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	ids := map[uint64][]byte{}
	for i := 0; i < 60; {
		id := newID()
		txn, err := c.Run(context.Background(), id, func(db *sqlite3.Conn) error { return db.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i)) })
		switch {
		case errors.Is(err, ErrRetry):
			time.Sleep(time.Millisecond)
			continue
		case err != nil:
			t.Fatal(err)
		}
		ids[txn] = id
		i++
	}
	head := l.Head()
	for txn, id := range ids {
		got, _, h, ok := l.Lookup(id, txn-1)
		switch {
		case h != head:
			t.Fatalf("head %d, want %d", h, head)
		case ok && got != txn:
			t.Fatalf("identifier of %d found at %d", txn, got)
		case !ok && txn+uint64(maxTail) > head: //nolint:gosec // lowered above
			t.Fatalf("identifier of %d, within the window of head %d, not covered", txn, head)
		}
	}
	if _, _, _, ok := l.Lookup(nil, o.Head); ok {
		t.Fatal("the office's bump, far below the head, still covered")
	}
}

// Covers: Limits and size, "A compaction that would produce a manifest
// above the maximum size is refused, and the database reports the
// condition": at the tail bound the refusal names it and is not retryable.
func TestTailBoundCompactionRefused(t *testing.T) {
	fastCompaction(t)
	old := maxManifestSize
	maxManifestSize = 0
	t.Cleanup(func() { maxManifestSize = old })
	st, keys, _ := newDatabase(t, 4096)
	o := takeOffice(t, st.Store, keys)
	cfg := testConfig()
	cfg.TailBound = o.Head
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	eventually(t, "the refused compaction", func() bool {
		_, err := exec(c, "CREATE TABLE t(x)")
		return errors.Is(err, format.ErrManifestTooLarge)
	})
	_, err := exec(c, "CREATE TABLE t(x)")
	if !errors.Is(err, ErrTailBound) || errors.Is(err, ErrRetry) || !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("at the bound with compaction refused: %v", err)
	}
}

// Covers: decision 8, "Scheduling": a leader far below half its tail bound
// compacts once the commit objects above the manifest reach CompactBytes,
// or once CompactInterval has passed, and not otherwise.
func TestCompactionTriggers(t *testing.T) {
	fastCompaction(t)
	for _, tc := range []struct {
		name     string
		bytes    int64
		interval time.Duration
		want     bool
	}{
		{"bytes", 64 << 10, time.Hour, true},
		{"interval", 1 << 40, 50 * time.Millisecond, true},
		{"neither", 1 << 40, time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, keys, o := newDatabase(t, 4096)
			cfg := testConfig()
			cfg.CompactBytes, cfg.CompactInterval, cfg.Grace = tc.bytes, tc.interval, time.Hour
			l := lead(t, st, keys, o, cfg)
			c := open(t, l)
			mustRun(t, c, "CREATE TABLE t(x)")
			for i := range 20 {
				mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
			}
			compacted := func() bool {
				l.cmu.Lock()
				defer l.cmu.Unlock()
				return l.manifestTxn > 0
			}
			if tc.want {
				eventually(t, "a compaction", compacted)
				return
			}
			time.Sleep(200 * time.Millisecond)
			if compacted() {
				t.Fatal("compacted below every threshold")
			}
		})
	}
}

// swapEpochRaw replaces the manifest with one at the next epoch and a new
// nonce, as a candidate's swap does, without a bump.
func swapEpochRaw(t *testing.T, st storage.Store, keys format.Keys, db format.DatabaseID) {
	t.Helper()
	ctx := context.Background()
	b, info, err := readObject(ctx, st, keys.Manifest(), format.MaxManifestSize+1)
	if err != nil {
		t.Fatal(err)
	}
	m, err := format.DecodeManifest(b, db)
	if err != nil {
		t.Fatal(err)
	}
	m.Epoch++
	m.Nonce[0]++
	nb, err := format.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Replace(ctx, keys.Manifest(), info.Version, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
}

// Covers: decision 8, a collector that reads a manifest at a higher epoch
// than its leader's: the leader leaves office and deletes nothing.
func TestCollectorAfterEpochSwapLeavesOffice(t *testing.T) {
	fastCompaction(t)
	ctx := context.Background()
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Hour, time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	// The loop stays out: the collector looks once, after the swap.
	l.cmpMu.Lock()
	defer l.cmpMu.Unlock()
	mustRun(t, c, "CREATE TABLE t(x)")
	if err := l.compactLocked(); err != nil {
		t.Fatal(err)
	}
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	if err := l.compactLocked(); err != nil {
		t.Fatal(err)
	}
	before, _ := st.Store.List(ctx, keys.BasePrefix(o.DB), "", 100)
	swapEpochRaw(t, st.Store, keys, o.DB)
	l.collectLocked()
	if err := l.Err(); !errors.Is(err, ErrStopped) || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("a collector under a higher epoch: leader %v", err)
	}
	if after, _ := st.Store.List(ctx, keys.BasePrefix(o.DB), "", 100); len(after) != len(before) {
		t.Fatalf("%d base objects before, %d after", len(before), len(after))
	}
}

// Covers: decision 8, a leader's own read around its own compaction: a
// main-file read that located a page in a commit, and whose ranged read
// lands after the leader compacted past that commit and retired it, locates
// the page again in the new base instead of stopping the leader.
func TestLeaderReadAcrossOwnCompaction(t *testing.T) {
	fastCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Cache, cfg.Grace, cfg.CompactInterval = NewCache(0), time.Hour, time.Hour
	l := lead(t, st, keys, o, cfg)
	c, r := open(t, l), open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	for i := range 5 {
		mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	l.tryCheckpoint() // the base version is the head: every page is read from storage
	gate := newGate()
	st.setRule(nth("getrange", isCommit, 1, fault{before: gate}))
	got := make(chan string, 1)
	go func() {
		var out []string
		_, err := r.Run(context.Background(), nil, func(db *sqlite3.Conn) (err error) {
			out, err = query(db, "SELECT group_concat(x) FROM t")
			return err
		})
		got <- fmt.Sprint(out, err)
	}()
	<-gate.reached
	if err := compactNow(l); err != nil {
		t.Fatal(err)
	}
	l.cmpMu.Lock()
	l.cfg.Grace = time.Nanosecond
	l.retireLocked()
	retired := l.retiredThrough
	l.cmpMu.Unlock()
	if retired < l.Head() {
		t.Fatalf("retired through %d, head %d", retired, l.Head())
	}
	gate.open()
	if res := <-got; res != "[0,1,2,3,4] <nil>" || l.Err() != nil {
		t.Fatalf("a read across the leader's own compaction: %s; leader %v", res, l.Err())
	}
}

// Covers: decision 7, the base version advances only to a head SQLite
// completed: while a commit is uploaded and SQLite rolled it back without
// the rollback hook, a TRUNCATE checkpoint does not reset the WAL, so a read
// transaction begun before still reads its own snapshot; the leader stops
// when the writer's Run returns.
func TestCheckpointDuringRolledBackUpload(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace = time.Hour
	l := lead(t, st, keys, o, cfg)
	l.psow = false // the commit's padding is written after its sync, where the fault fails it
	w, other := open(t, l), open(t, l)
	mustRun(t, w, "BEGIN; CREATE TABLE a(x); CREATE TABLE t(x); COMMIT")
	mustRun(t, w, "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<2000) INSERT INTO a SELECT randomblob(500) FROM c")
	mustRun(t, w, "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<2000) INSERT INTO t SELECT i FROM c")
	mustRun(t, w, "DROP TABLE a")
	baseIsHead := func() bool {
		l.mu.RLock()
		defer l.mu.RUnlock()
		return l.base == l.Head()
	}
	l.tryCheckpoint()
	if !baseIsHead() {
		t.Fatal("the base version is not the head before the test")
	}
	want := fmt.Sprint(queryConn(t, w, "SELECT count(*), sum(x) FROM t"))
	started, goOn, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var got []string
	var rerr error
	go func() {
		defer close(done)
		_, rerr = other.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
			if err := db.Exec("BEGIN"); err != nil {
				return err
			}
			if _, err := query(db, "SELECT count(*) FROM sqlite_schema"); err != nil {
				return err
			}
			close(started)
			<-goOn
			var err error
			got, err = query(db, "SELECT count(*), sum(x) FROM t")
			return err
		})
	}()
	<-started
	armed, fired := false, false
	l.afterUpload = func() { armed = true }
	l.fault = func(file, op string, _ int64, _ int) error {
		if armed && file == "wal" && op == "write" {
			fired = true
			return errInjected
		}
		return nil
	}
	headBefore := l.Head()
	var moved bool
	_, _ = w.Run(context.Background(), newID(), func(db *sqlite3.Conn) error {
		err := db.Exec("VACUUM")
		l.afterUpload = nil
		l.tryCheckpoint()
		moved = baseIsHead()
		close(goOn)
		<-done
		return err
	})
	switch {
	case !fired || l.Head() == headBefore:
		t.Fatalf("no upload rolled back: fault fired %v, head %d before and %d after", fired, headBefore, l.Head())
	case moved:
		t.Fatalf("the base version advanced to the rolled-back commit %d", l.Head())
	case rerr != nil || fmt.Sprint(got) != want:
		t.Fatalf("a read transaction begun before: %v, %v; want %s", got, rerr, want)
	case !errors.Is(l.Err(), ErrStopped):
		t.Fatalf("the leader after a rolled-back upload: %v", l.Err())
	}
}

// Covers: decision 8, "What an attempt reads and holds": the replacement is
// conditioned on the token the attempt read first. A leader paused before
// its first base object, while another takes office and compacts, loses
// its replacement and leaves office.
func TestStaleLeaderPausedBeforeBaseCreate(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace = time.Hour
	a := lead(t, st, keys, o, cfg)
	ca := open(t, a)
	mustRun(t, ca, "CREATE TABLE t(x)")
	for i := range 20 {
		mustRun(t, ca, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	gate := newGate()
	st.setRule(nth("create", isBase, 1, fault{before: gate}))
	done := make(chan error, 1)
	go func() { done <- compactNow(a) }()
	<-gate.reached
	st.setRule(nil)
	b := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
	cb := open(t, b)
	mustRun(t, cb, "INSERT INTO t VALUES (100)")
	if err := compactNow(b); err != nil {
		t.Fatal(err)
	}
	mb := checkLeaderManifest(t, st.Store, keys, o.DB)
	gate.open()
	if err := <-done; !errors.Is(err, ErrStopped) || !strings.Contains(fmt.Sprint(a.Err()), "fenced") {
		t.Fatalf("the stale leader's compaction: %v; leader: %v", err, a.Err())
	}
	if m := checkLeaderManifest(t, st.Store, keys, o.DB); m.Txn != mb.Txn || m.Epoch != mb.Epoch || m.Nonce != mb.Nonce {
		t.Fatalf("the stale leader replaced the manifest at %d epoch %d with one at %d epoch %d", mb.Txn, mb.Epoch, m.Txn, m.Epoch)
	}
	want := "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,100"
	if got := rows(t, durable(t, st.Store, keys, 0)); got != want {
		t.Fatalf("durable rows %s", got)
	}
}

// Covers: decision 6 and decision 8, "A leader's own reads around its
// compaction": a deposed leader's main-file read that meets a commit a later
// leader retired, or a base object it deleted, returns no page, and the
// leader leaves office.
func TestDeposedLeaderReadMeetsRetiredOrDeleted(t *testing.T) {
	for _, tc := range []struct {
		name string
		base bool // the page comes from a base object, else from a commit
	}{{"retired commit", false}, {"deleted base object", true}} {
		t.Run(tc.name, func(t *testing.T) {
			quietCompaction(t)
			st, keys, o := newDatabase(t, 4096)
			cfg := testConfig()
			cfg.Cache, cfg.Grace = NewCache(0), time.Hour
			a := lead(t, st, keys, o, cfg)
			ca, ra := open(t, a), open(t, a)
			mustRun(t, ca, "CREATE TABLE t(x)")
			for i := range 5 {
				mustRun(t, ca, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
			}
			// The base version is the head: every page is read from storage,
			// from the commits, or from the base objects after a compaction.
			a.tryCheckpoint()
			if tc.base {
				if err := compactNow(a); err != nil {
					t.Fatal(err)
				}
			}
			b := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
			cb := open(t, b)
			mustRun(t, cb, "INSERT INTO t VALUES (100)")
			if err := compactNow(b); err != nil {
				t.Fatal(err)
			}
			b.cmpMu.Lock()
			b.cfg.Grace = time.Nanosecond
			b.retireLocked()
			b.collectLocked() // finds A's base objects
			b.collectLocked() // deletes them
			b.cmpMu.Unlock()
			res := make(chan string, 1)
			go func() {
				var out []string
				_, err := ra.Run(context.Background(), nil, func(db *sqlite3.Conn) (err error) {
					out, err = query(db, "SELECT group_concat(x) FROM t")
					return err
				})
				res <- fmt.Sprint(out, err)
			}()
			select {
			case got := <-res:
				if strings.HasSuffix(got, "<nil>") {
					t.Fatalf("the deposed leader's read returned %s", got)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the deposed leader's read did not end")
			}
			if err := a.Err(); !errors.Is(err, ErrStopped) || !strings.Contains(err.Error(), "fenced") {
				t.Fatalf("the deposed leader: %v", err)
			}
		})
	}
}

// Covers: decision 10, step 8: a compaction that publishes while
// maintenance runs readmits nothing; maintenance readmits when it ends.
func TestInstallDuringMaintenanceKeepsAdmissionClosed(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace = time.Hour
	l := lead(t, st, keys, o, cfg)
	h, c := open(t, l), open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	// h's Run holds its connection, so maintenance waits in steps 4 and 5.
	entered, release, held := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(held)
		_, _ = h.Run(context.Background(), nil, func(*sqlite3.Conn) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	var once sync.Once
	free := func() { once.Do(func() { close(release) }); <-held }
	t.Cleanup(free)
	l.cmu.Lock()
	before := l.manifestTxn
	l.cmu.Unlock()
	startMaintenance(l)
	if err := compactNow(l); err != nil {
		t.Fatal(err)
	}
	l.cmu.Lock()
	after, closed := l.manifestTxn, l.admitClosed
	l.cmu.Unlock()
	if after == before || !closed {
		t.Fatalf("a compaction during maintenance: manifest %d to %d, admission closed %v", before, after, closed)
	}
	if _, err := exec(c, "INSERT INTO t VALUES (2)"); !errors.Is(err, ErrRetry) {
		t.Fatalf("a write during maintenance: %v", err)
	}
	free()
	waitMaintenance(t, l)
	mustRun(t, c, "INSERT INTO t VALUES (3)")
}

// Covers: decision 10, step 7: maintenance at the tail bound compacts up to
// the new base version itself, with the compaction loop asleep, and
// readmits writes when it ends.
func TestMaintenanceCompactsAtTailBound(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace = time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	mustRun(t, c, "INSERT INTO t VALUES (1)")
	// The tail is at the bound with no commit to wake the loop.
	l.cmu.Lock()
	l.cfg.TailBound = l.head - l.manifestTxn
	l.cmu.Unlock()
	startMaintenance(l)
	waitMaintenance(t, l)
	l.cmu.Lock()
	mt, head, closed := l.manifestTxn, l.head, l.admitClosed
	l.cmu.Unlock()
	if mt != head || closed {
		t.Fatalf("after maintenance at the tail bound: manifest %d, head %d, admission closed %v", mt, head, closed)
	}
	mustRun(t, c, "INSERT INTO t VALUES (2)")
}

// Covers: decision 8, "What an attempt reads and holds": a commit a later
// leader retires under an attempt sends the leader out of office at that
// attempt, not at its next.
func TestStaleCommitUnderAttemptLeavesOffice(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace = time.Hour
	a := lead(t, st, keys, o, cfg)
	ca := open(t, a)
	mustRun(t, ca, "CREATE TABLE t(x)")
	for i := range 5 {
		mustRun(t, ca, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	gate := newGate()
	st.setRule(nth("getrange", isCommit, 1, fault{before: gate}))
	done := make(chan error, 1)
	go func() { done <- compactNow(a) }()
	<-gate.reached
	st.setRule(nil)
	b := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
	cb := open(t, b)
	mustRun(t, cb, "INSERT INTO t VALUES (100)")
	if err := compactNow(b); err != nil {
		t.Fatal(err)
	}
	b.cmpMu.Lock()
	b.cfg.Grace = time.Nanosecond
	b.retireLocked()
	b.cmpMu.Unlock()
	gate.open()
	if err := <-done; !errors.Is(err, ErrStopped) || !strings.Contains(fmt.Sprint(a.Err()), "fenced") {
		t.Fatalf("an attempt over a retired commit: %v; leader: %v", err, a.Err())
	}
}

// A collection pass deletes at most passBatch keys, and later passes
// finish the list.
func TestCollectorPassBound(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Nanosecond, time.Hour
	l := lead(t, st, keys, o, cfg)
	l.cmpMu.Lock()
	for i := range 2*passBatch + 10 {
		l.garbage = append(l.garbage, fmt.Sprintf("probe/none-%d", i))
	}
	l.garbageAt = time.Now().Add(-time.Hour)
	l.cmpMu.Unlock()
	for i, want := range []int{passBatch + 10, 10, 0} {
		l.tend()
		l.cmpMu.Lock()
		got := len(l.garbage)
		l.cmpMu.Unlock()
		if got != want {
			t.Fatalf("after pass %d: %d keys left, want %d", i+1, got, want)
		}
	}
}
