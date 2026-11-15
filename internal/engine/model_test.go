package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// modelTxn returns a random transaction for table t(k, v): inserts,
// updates and deletes of values that span overflow pages, savepoints,
// rollbacks, transactions that change nothing, index changes and VACUUM.
// commit says whether it can commit.
func modelTxn(rng *rand.Rand) (sql string, commit bool) {
	if rng.IntN(25) == 0 {
		return "VACUUM", true
	}
	var b strings.Builder
	b.WriteString("BEGIN;")
	for range 1 + rng.IntN(4) {
		lo := 1 + rng.IntN(300)
		hi := lo + rng.IntN(40)
		width := rng.IntN(2500)
		switch rng.IntN(7) {
		case 0, 1:
			fmt.Fprintf(&b, "INSERT OR REPLACE INTO t SELECT value, printf('%%0%dd', value * %d) FROM generate_series(%d, %d);", width, rng.IntN(1000), lo, hi)
		case 2:
			fmt.Fprintf(&b, "UPDATE t SET v = printf('%%0%dd', k + %d) WHERE k BETWEEN %d AND %d;", width, rng.IntN(1000), lo, hi)
		case 3:
			fmt.Fprintf(&b, "DELETE FROM t WHERE k BETWEEN %d AND %d;", lo, hi)
		case 4:
			fmt.Fprintf(&b, "SAVEPOINT s; INSERT OR REPLACE INTO t VALUES (%d, 'undone'); ROLLBACK TO s; RELEASE s;", lo)
		case 5:
			if rng.IntN(2) == 0 {
				b.WriteString("CREATE INDEX IF NOT EXISTS tv ON t(substr(v, 1, 8));")
			} else {
				b.WriteString("DROP INDEX IF EXISTS tv;")
			}
		case 6:
			b.WriteString("SELECT count(*) FROM t;")
		}
	}
	if rng.IntN(8) == 0 {
		b.WriteString("ROLLBACK")
		return b.String(), false
	}
	b.WriteString("COMMIT")
	return b.String(), true
}

// Covers: the oracle rule of "What the engine's tests must prove": the
// oracle receives the transactions the durable history admitted, including
// ones accepted while their client was told unknown, and the answers are
// checked against that history; Commit outcomes, "A client is told exactly
// one of committed, not committed or unknown, and never not committed for a
// commit that is in the history"; Commit capture and failure, "No
// transaction is committed under two transaction numbers"; and Reads,
// leader connections against the oracle, with cache eviction.
//
// Each step is a random transaction on a random connection with a random
// store outcome for its create (success, a lost response, definite
// failures, sticky uncertainty, a request still in flight), a checkpoint, a
// read compared with the oracle, or a takeover by a new leader with an
// empty local directory. SKYLITE_SEED replays a run; SKYLITE_SOAK runs
// longer.
func TestModel(t *testing.T) {
	seed := uint64(time.Now().UnixNano()) //nolint:gosec // a clock reading
	if s := os.Getenv("SKYLITE_SEED"); s != "" {
		var err error
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d: replay with SKYLITE_SEED=%d", seed, seed)
		}
	})
	steps := 120
	if os.Getenv("SKYLITE_SOAK") != "" {
		steps = 3000
	}
	rng := rand.New(rand.NewPCG(seed, 2)) //nolint:gosec // seeded so failures reproduce

	st, keys, o := newDatabase(t, 1024)
	cfg := testConfig()
	cfg.Cache = NewCache(32 << 10)
	var (
		l     *Leader
		conns []*Conn
		ids   []string // the log from o.Head, as commitIDs reads it
	)
	takeover := func(o Office) {
		if l != nil {
			_ = l.Close()
		}
		l = lead(t, st, keys, o, cfg)
		conns = []*Conn{open(t, l), open(t, l), open(t, l)}
		ids = append(ids, "bump")
	}
	takeover(o)
	or := newOracle(t)
	const schema = "CREATE TABLE t(k INTEGER PRIMARY KEY, v)"
	id := newID()
	if _, err := conns[0].Run(context.Background(), id, func(db *sqlite3.Conn) error { return db.Exec(schema) }); err != nil {
		t.Fatal(err)
	}
	or.exec(t, schema)
	ids = append(ids, string(id))

	checkDurable := func(step int) {
		t.Helper()
		if got, want := mustDump(t, durable(t, st.Store, keys, 0)), mustDump(t, or.db); got != want {
			t.Fatalf("step %d: durable state differs from the oracle", step)
		}
	}
	seen := map[string]int{}
	for step := range steps {
		c := conns[rng.IntN(len(conns))]
		switch k := rng.IntN(20); {
		case k < 13:
			sql, commits := modelTxn(rng)
			var rule func(op, key string) fault
			var late *gate
			switch rng.IntN(10) {
			case 0:
				rule = commitRules(fault{drop: true}, fault{})
			case 1:
				rule = commitRules(fault{noEffect: true})
			case 2:
				rule = commitRules(fault{noEffect: true}, fault{noEffect: true}, fault{})
			case 3:
				rule = commitRules(fault{unknownNone: true}, fault{noEffect: true})
			case 4:
				late = newGate()
				rule = commitRules(fault{late: late}, fault{noEffect: true})
			}
			st.setRule(rule)
			id = newID()
			before := l.Head()
			txn, err := c.Run(context.Background(), id, func(db *sqlite3.Conn) error {
				if rng.IntN(3) == 0 {
					if err := db.Exec("PRAGMA cache_size=5"); err != nil {
						return err
					}
				}
				return db.Exec(sql)
			})
			st.setRule(nil)
			if late != nil {
				late.open()
				st.late.Wait()
			}
			switch {
			case err == nil && txn == 0:
				seen["nothing"]++
				if l.Head() != before {
					t.Fatalf("step %d: no transaction number, but the head moved", step)
				}
				if commits {
					or.exec(t, sql) // changes nothing, or the comparisons fail
				}
			case err == nil:
				seen["committed"]++
				if txn != before+1 || l.Head() != txn {
					t.Fatalf("step %d: committed as %d after %d, head %d", step, txn, before, l.Head())
				}
				or.exec(t, sql)
				ids = append(ids, string(id))
			case errors.Is(err, ErrNotCommitted):
				seen["not committed"]++
				if txn != 0 || l.Head() != before {
					t.Fatalf("step %d: not committed, but %d, head %d: %v", step, txn, l.Head(), err)
				}
				if l.Err() != nil {
					takeover(takeOffice(t, st.Store, keys))
				}
				checkDurable(step)
			case errors.Is(err, ErrUnknown):
				seen["unknown"]++
				if !errors.Is(l.Err(), ErrStopped) {
					t.Fatalf("step %d: unknown without a stop: %v", step, err)
				}
				if !commits {
					t.Fatalf("step %d: unknown for a rollback: %v", step, err)
				}
				// The history decides: the transaction is in it or not, whole.
				inner := strings.TrimSuffix(strings.TrimPrefix(sql, "BEGIN;"), "COMMIT")
				if sql == "VACUUM" {
					inner = "" // changes no row
				}
				or.exec(t, "SAVEPOINT u;"+inner)
				with := mustDump(t, or.db)
				or.exec(t, "ROLLBACK TO u")
				without := mustDump(t, or.db)
				switch got := mustDump(t, durable(t, st.Store, keys, 0)); {
				case got == with && len(commitIDs(t, st.Store, keys, o.DB, o.Head)) > len(ids):
					or.exec(t, inner)
					ids = append(ids, string(id))
				case got == without:
					seen["unknown, absent"]++
				default:
					t.Fatalf("step %d: durable state is neither with nor without the unknown transaction", step)
				}
				or.exec(t, "RELEASE u")
				takeover(takeOffice(t, st.Store, keys))
			default:
				t.Fatalf("step %d: %s: %v", step, sql, err)
			}
		case k < 15:
			mode := []sqlite3.CheckpointMode{sqlite3.CHECKPOINT_PASSIVE, sqlite3.CHECKPOINT_RESTART, sqlite3.CHECKPOINT_TRUNCATE}[rng.IntN(3)]
			if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error {
				_, _, err := db.WALCheckpoint("main", mode)
				return err
			}); err != nil && !errors.Is(err, sqlite3.BUSY) {
				t.Fatalf("step %d: checkpoint: %v", step, err)
			}
		case k < 19:
			if got := dumpConn(t, c); got != mustDump(t, or.db) {
				t.Fatalf("step %d: a leader connection differs from the oracle", step)
			}
		default:
			takeover(takeOffice(t, st.Store, keys))
			checkDurable(step)
		}
	}
	checkDurable(steps)
	t.Logf("outcomes: %v", seen)
	if got := commitIDs(t, st.Store, keys, o.DB, o.Head); strings.Join(got, "\n") != strings.Join(ids, "\n") {
		t.Fatalf("the log holds %d objects, the model %d", len(got), len(ids))
	}
	if got := dumpConn(t, open(t, l)); got != mustDump(t, or.db) {
		t.Fatal("a new connection differs from the oracle")
	}
}
