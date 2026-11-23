package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// TestRetirementKeepsUpWithWrites: under a sustained write rate, with each
// storage call on the log taking a few milliseconds, retirement keeps up
// with the commits the manifest covers: three round trips per commit, one
// commit at a time, would leave most of them live.
func TestRetirementKeepsUpWithWrites(t *testing.T) {
	fastCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Nanosecond, 10*time.Millisecond
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	var mu sync.Mutex
	var inFlight, most int
	st.setRule(func(op, key string) fault {
		switch {
		case !isCommit(key):
		case op == "create":
			time.Sleep(2 * time.Millisecond)
		case op == "replace":
			mu.Lock()
			inFlight++
			most = max(most, inFlight)
			mu.Unlock()
			time.Sleep(4 * time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
		case op == "getrange":
			time.Sleep(4 * time.Millisecond)
		}
		return fault{}
	})
	const n = 400
	for i := range n {
		mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	head := l.Head()
	// One at a time, a commit's retirement takes three 4 ms calls, against
	// one 2 ms create per commit written; keeping up needs retires in
	// parallel. A ratio of retired to written commits at this instant
	// depends on the runner's speed, so the test checks the parallelism.
	eventually(t, "retires in parallel", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return most > 1
	})
	st.setRule(nil)
	eventually(t, "retirement through the manifest", func() bool {
		l.cmu.Lock()
		m := l.manifestTxn
		l.cmu.Unlock()
		l.cmpMu.Lock()
		defer l.cmpMu.Unlock()
		return m >= head-1 && l.retiredThrough >= m
	})
	for k := uint64(1); k <= head-1; k++ {
		h, _, err := readHeader(context.Background(), st.Store, keys, o.DB, k)
		if err != nil || !h.Retired && !h.EpochBump {
			t.Fatalf("commit %d left live: %v", k, err)
		}
	}
}

// TestRetirementInFlight: one retirement pass works on retireWidth commits
// at once and advances the cursor over all it may retire.
func TestRetirementInFlight(t *testing.T) {
	quietCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.Grace, cfg.CompactInterval = time.Hour, time.Hour
	l := lead(t, st, keys, o, cfg)
	c := open(t, l)
	mustRun(t, c, "CREATE TABLE t(x)")
	for i := range 96 {
		mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	if err := compactNow(l); err != nil {
		t.Fatal(err)
	}
	m := checkLeaderManifest(t, st.Store, keys, o.DB)
	var mu sync.Mutex
	inFlight, most := 0, 0
	st.setRule(func(op, key string) fault {
		if op != "replace" || !isCommit(key) {
			return fault{}
		}
		mu.Lock()
		inFlight++
		most = max(most, inFlight)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return fault{}
	})
	l.cmpMu.Lock()
	l.cfg.Grace = time.Nanosecond
	l.retireLocked()
	got := l.retiredThrough
	l.cmpMu.Unlock()
	if got != m.Txn {
		t.Fatalf("one pass retired through %d of a manifest at %d", got, m.Txn)
	}
	if most < 8 {
		t.Fatalf("at most %d retires in flight, width %d", most, retireWidth)
	}
}

// TestGraceAcrossLeaderChange: a leader retires what a manifest covers once
// its node has known that manifest for the grace period, however recently
// it took office; a manifest it read too recently, or one it never read,
// starts no retirement, nor does a reading of another database or one with
// no time or a time to come, which delays nothing.
func TestGraceAcrossLeaderChange(t *testing.T) {
	cases := map[string]struct {
		seen func(db format.DatabaseID, txn uint64) []Seen
		want func(txn uint64) uint64
	}{
		"read an hour ago": {func(db format.DatabaseID, txn uint64) []Seen {
			return []Seen{{db, 1, time.Now().Add(-3 * time.Hour)}, {db, txn, time.Now().Add(-2 * time.Hour)}}
		}, func(txn uint64) uint64 { return txn }},
		"read a minute ago": {func(db format.DatabaseID, txn uint64) []Seen { return []Seen{{db, txn, time.Now().Add(-time.Minute)}} }, func(uint64) uint64 { return 0 }},
		"older one only": {func(db format.DatabaseID, txn uint64) []Seen {
			return []Seen{{db, txn / 2, time.Now().Add(-2 * time.Hour)}, {db, txn, time.Now()}}
		}, func(txn uint64) uint64 { return txn / 2 }},
		"never read": {func(format.DatabaseID, uint64) []Seen { return nil }, func(uint64) uint64 { return 0 }},
		"above the manifest": {func(db format.DatabaseID, txn uint64) []Seen {
			return []Seen{{db, txn + 5, time.Now().Add(-2 * time.Hour)}}
		}, func(uint64) uint64 { return 0 }},
		"out of order ignored": {func(db format.DatabaseID, txn uint64) []Seen {
			return []Seen{{db, txn, time.Now().Add(-time.Minute)}, {db, txn - 1, time.Now().Add(-2 * time.Hour)}}
		}, func(uint64) uint64 { return 0 }},
		"another database": {func(_ format.DatabaseID, txn uint64) []Seen {
			return []Seen{{format.DatabaseID{1}, txn, time.Now().Add(-2 * time.Hour)}}
		}, func(uint64) uint64 { return 0 }},
		"no time":      {func(db format.DatabaseID, txn uint64) []Seen { return []Seen{{db, txn, time.Time{}}} }, func(uint64) uint64 { return 0 }},
		"time to come": {func(db format.DatabaseID, txn uint64) []Seen { return []Seen{{db, txn, time.Now().Add(time.Hour)}} }, func(uint64) uint64 { return 0 }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			quietCompaction(t)
			st, keys, o := newDatabase(t, 4096)
			cfg := testConfig()
			cfg.Grace, cfg.CompactInterval = time.Hour, time.Hour
			a := lead(t, st, keys, o, cfg)
			c := open(t, a)
			mustRun(t, c, "CREATE TABLE t(x)")
			for i := range 20 {
				mustRun(t, c, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
			}
			if err := compactNow(a); err != nil {
				t.Fatal(err)
			}
			m := checkLeaderManifest(t, st.Store, keys, o.DB)
			_ = c.Close()
			_ = a.Close()
			cfg.Seen = tc.seen(o.DB, m.Txn)
			b := lead(t, st, keys, takeOffice(t, st.Store, keys), cfg)
			b.cmpMu.Lock()
			b.retireLocked()
			got := b.retiredThrough
			// A grace period after the takeover, its own read of the manifest
			// covers it all, whatever the node read before.
			later := b.retireLimit(time.Now().Add(cfg.Grace + time.Minute))
			b.cmpMu.Unlock()
			if later != m.Txn {
				t.Fatalf("retire limit %d a grace period on, want %d", later, m.Txn)
			}
			if want := tc.want(m.Txn); got != want {
				t.Fatalf("retired through %d, want %d, under a manifest at %d", got, want, m.Txn)
			}
			for k := uint64(1); k <= m.Txn; k++ {
				h, _, err := readHeader(context.Background(), st.Store, keys, o.DB, k)
				if err != nil {
					t.Fatal(err)
				}
				if !h.EpochBump && h.Retired != (k <= got) {
					t.Fatalf("commit %d: retired %v, bump %v, cursor at %d", k, h.Retired, h.EpochBump, got)
				}
			}
		})
	}
}
