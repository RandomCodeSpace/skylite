package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// usurper is a candidate whose clock runs far enough ahead that every
// earlier lease reads as expired to it.
func usurper(name string, ahead time.Duration) Candidate {
	return Candidate{Holder: name, TTL: time.Minute, Now: func() time.Time { return time.Now().Add(ahead) }}
}

// tailDatabase creates a database whose log holds n commits above its
// manifest, each inserting one row, and closes their leader without a
// compaction.
func tailDatabase(t *testing.T, n int) (*faultStore, format.Keys) {
	t.Helper()
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.CompactInterval = time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	for i := range n - 1 {
		mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	_ = c.Close()
	_ = l.Close()
	return st, keys
}

// logReads records the ranged reads of commit-log objects, in the order
// they reach the store; it is safe for the walk's concurrent reads.
type logReads struct {
	mu   sync.Mutex
	keys []string
}

func (r *logReads) rule(op, key string) fault {
	if op == "getrange" && isCommit(key) {
		r.mu.Lock()
		r.keys = append(r.keys, key)
		r.mu.Unlock()
	}
	return fault{}
}

func (r *logReads) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := r.keys
	r.keys = nil
	return k
}

// TestTakeoverLongTail: a takeover over a long tail reads each commit once,
// with reads in flight up to the window, and hands what it read to Lead,
// which reads no commit again but the bump's header, and leads the same
// state that a fresh walk of the log builds.
func TestTakeoverLongTail(t *testing.T) {
	for _, window := range []int{1, 3, 32} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			walkReads(t, window, firstRead)
			const n = 300
			st, keys := tailDatabase(t, n)
			reads := &logReads{}
			st.setRule(reads.rule)
			o, _, err := Takeover(context.Background(), st, keys, usurper("b", 0))
			if err != nil {
				t.Fatal(err)
			}
			walked := reads.take()
			seen := map[string]int{}
			for _, k := range walked {
				seen[k]++
			}
			for txn := uint64(1); txn < o.Head; txn++ {
				if k := keys.Commit(o.DB, txn); seen[k] != 1 && txn > 2 {
					t.Fatalf("commit %d read %d times", txn, seen[k])
				}
			}
			// The names read above the head are the window's, at most.
			if extra := len(walked) - int(o.Head-1); extra > window+2 { //nolint:gosec // a short log
				t.Fatalf("%d reads for %d commits with a window of %d", len(walked), o.Head-1, window)
			}
			l := lead(t, st, keys, o, testConfig())
			// The bump's header, then the pages SQLite reads to open its
			// connection: a few, however long the tail.
			if got := reads.take(); len(got) == 0 || got[0] != keys.Commit(o.DB, o.Head) || len(got) > 4 {
				t.Fatalf("Lead read the log again: %v", got)
			}
			r, err := Open(context.Background(), st.Store, keys, nil, o.Head)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.commits) != len(l.tmpl.commits) {
				t.Fatalf("Lead indexed %d commits, a fresh walk %d", len(l.tmpl.commits), len(r.commits))
			}
			for i, c := range r.commits {
				lc := l.tmpl.commits[i]
				if !reflect.DeepEqual(lc.h, c.h) || lc.version != c.version || lc.key != c.key || !slices.Equal(lc.entries, c.entries) {
					t.Fatalf("commit %d: Lead has %+v, a fresh walk %+v", c.h.Txn, lc.h, c.h)
				}
			}
			if got := queryConn(t, open(t, l), "SELECT count(*) FROM t"); !slices.Equal(got, []string{fmt.Sprint(n - 1)}) {
				t.Fatalf("rows %v", got)
			}
		})
	}
}

// TestTakeoverInterrupted: a takeover cancelled in the middle of its walk
// creates no bump and takes no office, and the next one leads every row.
func TestTakeoverInterrupted(t *testing.T) {
	const n = 120
	st, keys := tailDatabase(t, n)
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	reads := 0
	st.setRule(func(op, key string) fault {
		mu.Lock()
		defer mu.Unlock()
		if op == "getrange" && isCommit(key) {
			if reads++; reads == n/2 {
				cancel()
			}
		}
		if op == "create" && isCommit(key) {
			t.Errorf("interrupted takeover created %s", key)
		}
		return fault{}
	})
	if o, _, err := Takeover(ctx, st, keys, usurper("a", 0)); err == nil {
		t.Fatalf("interrupted takeover took office %+v", o)
	}
	st.setRule(nil)
	o, _, err := Takeover(context.Background(), st, keys, usurper("b", 2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	l := lead(t, st, keys, o, testConfig())
	if got := queryConn(t, open(t, l), "SELECT count(*) FROM t"); !slices.Equal(got, []string{fmt.Sprint(n - 1)}) {
		t.Fatalf("rows %v", got)
	}
}

// TestTakeoverFencesConcurrentWriter: a leader that keeps committing while
// a candidate walks its tail is fenced by the bump, and the new leader
// holds exactly the commits the old one reported committed.
func TestTakeoverFencesConcurrentWriter(t *testing.T) {
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.CompactInterval = time.Hour
	old := lead(t, st, keys, o, cfg)
	c := open(t, old)
	mustRun(t, c, "CREATE TABLE t(x)")
	for i := range 100 {
		mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", -i-1))
	}
	var committed []uint64
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			txn, err := exec(c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
			if err != nil {
				done <- err
				return
			}
			committed = append(committed, txn)
		}
	}()
	// Let the writer get ahead of where the walk starts.
	eventually(t, "the writer commits", func() bool { return old.Head() > 120 })
	b, _, err := Takeover(context.Background(), st, keys, usurper("b", 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrNotCommitted) && !errors.Is(err, ErrUnknown) {
		t.Fatalf("the old leader's commit after the bump: %v", err)
	}
	if !errors.Is(old.Err(), ErrStopped) {
		t.Fatalf("the old leader is not stopped: %v", old.Err())
	}
	for _, txn := range committed {
		if txn >= b.Head {
			t.Fatalf("commit %d acknowledged at or above the bump at %d", txn, b.Head)
		}
	}
	l := lead(t, st, keys, b, cfg)
	got := queryConn(t, open(t, l), "SELECT count(*) FROM t WHERE x >= 0")
	if want := []string{fmt.Sprint(len(committed))}; !slices.Equal(got, want) {
		// An unknown outcome may have landed one more.
		t.Fatalf("rows %v, %d acknowledged", got, len(committed))
	}
}

// TestLeadIgnoresStaleTail: an office whose manifest another candidacy has
// replaced since its takeover does not lead from what its walk read.
func TestLeadIgnoresStaleTail(t *testing.T) {
	st, keys := tailDatabase(t, 50)
	b, _, err := Takeover(context.Background(), st, keys, usurper("b", 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Takeover(context.Background(), st, keys, usurper("c", 2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if l, err := Lead(context.Background(), st, keys, t.TempDir(), b, testConfig()); !errors.Is(err, ErrFenced) {
		if l != nil {
			_ = l.Close()
		}
		t.Fatalf("Lead after a later takeover: %v", err)
	}
}

// TestWalkReadsAhead: a reader's walk over a long tail reads every commit
// once, while a walk that finds the head at once reads only the next name
// and the probe above it.
func TestWalkReadsAhead(t *testing.T) {
	st, keys := tailDatabase(t, 200)
	o, _, err := Takeover(context.Background(), st, keys, usurper("b", 0))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var calls []string
	st.setRule(func(op, key string) fault {
		if strings.Contains(key, "/log/") {
			mu.Lock()
			calls = append(calls, op+" "+key)
			mu.Unlock()
		}
		return fault{}
	})
	take := func() []string {
		mu.Lock()
		defer mu.Unlock()
		c := calls
		calls = nil
		return c
	}
	r, err := Open(context.Background(), st, keys, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Txn() != o.Head {
		t.Fatalf("opened at %d, head %d", r.Txn(), o.Head)
	}
	seen := map[string]int{}
	for _, c := range take() {
		seen[c]++
	}
	for txn := uint64(1); txn <= o.Head; txn++ {
		if k := "getrange " + keys.Commit(o.DB, txn); seen[k] != 1 {
			t.Fatalf("%s %d times", k, seen[k])
		}
	}
	if err := r.Advance(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"getrange " + keys.Commit(o.DB, o.Head+1), "head " + keys.Commit(o.DB, o.Head+2)}
	if got := take(); !slices.Equal(got, want) {
		t.Fatalf("advance at the head: %v, want %v", got, want)
	}
}
