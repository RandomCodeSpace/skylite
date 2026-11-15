package engine

import (
	"slices"
	"testing"
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
