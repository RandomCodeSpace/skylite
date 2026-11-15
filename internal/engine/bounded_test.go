package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// importDatabase builds a WAL-mode SQLite database of rows rows of 1000
// bytes locally, stores it as one commit compacted into base objects, and
// takes office for it. It returns the database size in bytes.
func importDatabase(t *testing.T, rows int) (*testStore, format.Keys, Office, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "import.db")
	db, err := sqlite3.Open("file:" + path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(fmt.Sprintf("PRAGMA page_size=4096; PRAGMA journal_mode=wal; CREATE TABLE t(k INTEGER PRIMARY KEY, v); INSERT INTO t SELECT value, randomblob(1000) FROM generate_series(1, %d)", rows)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pages := map[uint32][]byte{}
	for p := range len(b) / 4096 {
		pages[uint32(p+1)] = b[p*4096 : (p+1)*4096] //nolint:gosec // small test databases
	}
	ts, keys := newStore(t)
	h := newHistory(t, ts.Store, keys, 1, 4096, 64)
	h.compact(h.commit(uint32(len(pages)), pages)) //nolint:gosec // small test databases
	return ts, keys, takeOffice(t, ts.Store, keys), int64(len(b))
}

// Covers: Limits and size, "Capacity: on a database far larger than the
// pod's memory and disk budgets ..., page misses, partial checkpoints, the
// WAL reset ... stay within fixed budgets" (compaction and retirement are
// not in this change); and Bootstrap, "a database already in object
// storage opens the same way", without downloading it. The same workload
// runs on two databases sixteen times apart in size; each phase's requests
// and bytes read from storage, and the local disk the main file uses, stay
// under bounds that do not depend on the size.
func TestLeaderBoundedWork(t *testing.T) {
	type cost struct {
		requests int
		bytes    int64
	}
	// What one phase may cost: the workload's 20 lookups touch at most a
	// root-to-leaf path each, four pages deep, and every miss fetches one
	// page.
	const maxRequests, maxBytes = 200, 200 * 4096
	const maxLocal = 64 * 4096 // the pages a checkpoint of the workload writes
	for _, rows := range []int{2000, 32000} {
		t.Run(fmt.Sprint(rows, " rows"), func(t *testing.T) {
			ts, keys, o, size := importDatabase(t, rows)
			measure := func(name string, f func()) {
				t.Helper()
				ts.resetCounts()
				f()
				r, b := ts.counts()
				t.Logf("%s: %d requests, %d bytes of a %d-byte database", name, r, b, size)
				if r > maxRequests || b > maxBytes {
					t.Errorf("%s: %d requests and %d bytes, bound %d and %d", name, r, b, maxRequests, maxBytes)
				}
			}
			var l *Leader
			var c *Conn
			measure("open", func() {
				cfg := testConfig()
				cfg.Cache = NewCache(64 << 10) // far smaller than either database
				l = lead(t, ts, keys, o, cfg)
				c = open(t, l)
				_ = queryConn(t, c, "SELECT count(*) FROM sqlite_schema")
			})
			lookups := func(first int) {
				for i := range 20 {
					k := first + i*rows/20
					if got := queryConn(t, c, fmt.Sprintf("SELECT length(v) FROM t WHERE k = %d", k)); got[0] != "1000" {
						t.Fatalf("row %d: %v", k, got)
					}
				}
			}
			measure("page misses", func() { lookups(1) })
			measure("commit", func() {
				var ks []string
				for i := range 10 {
					ks = append(ks, fmt.Sprint(1+i*rows/10))
				}
				mustRun(t, c, "UPDATE t SET v = randomblob(1000) WHERE k IN ("+strings.Join(ks, ",")+")")
			})
			local := func() int64 {
				var st syscall.Stat_t
				if err := syscall.Stat(l.path, &st); err != nil {
					t.Fatal(err)
				}
				return st.Blocks * 512
			}
			measure("partial checkpoint", func() {
				// A reader's snapshot from before the commit keeps the first
				// checkpoint partial.
				reader := open(t, l)
				_ = queryConn(t, reader, "BEGIN; SELECT count(*) FROM t")
				mustRun(t, c, "UPDATE t SET v = randomblob(1000) WHERE k = 2")
				checkpoint(t, c, sqlite3.CHECKPOINT_PASSIVE)
				_ = queryConn(t, reader, "COMMIT")
				checkpoint(t, c, sqlite3.CHECKPOINT_PASSIVE)
			})
			if n := local(); n > maxLocal {
				t.Errorf("the main file holds %d bytes locally after a checkpoint, bound %d", n, maxLocal)
			}
			measure("WAL reset", func() { checkpoint(t, c, sqlite3.CHECKPOINT_TRUNCATE) })
			if n := local(); n != 0 {
				t.Errorf("the main file holds %d bytes locally after the WAL reset", n)
			}
			measure("page misses after the reset", func() { lookups(7) })
		})
	}
}

func checkpoint(t *testing.T, c *Conn, mode sqlite3.CheckpointMode) {
	t.Helper()
	if _, err := c.Run(nil, func(db *sqlite3.Conn) error {
		_, _, err := db.WALCheckpoint("main", mode)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
