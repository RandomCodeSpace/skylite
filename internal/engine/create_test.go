package engine

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver" // the SQLite engine of record 0004, v0.37.0

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// TestCreateEmptyDatabaseIsValidSQLite creates a database, reads its page 1
// back through a Reader and hands it to real SQLite.
func TestCreateEmptyDatabaseIsValidSQLite(t *testing.T) {
	for _, ps := range []uint32{512, 1024, 4096, 16384, 65536} {
		ctx := context.Background()
		st, keys := newStore(t)
		gs := uint32(format.MaxGroupBytes) / ps
		if _, err := Create(ctx, st, keys, ps, gs); err != nil {
			t.Fatalf("page size %d: %v", ps, err)
		}
		r, err := Open(ctx, st, keys, NewCache(0), 0)
		if err != nil {
			t.Fatal(err)
		}
		if r.Txn() != 1 || r.Size() != 1 || r.PageSize() != ps {
			t.Fatalf("page size %d: reader at %d with %d pages of %d", ps, r.Txn(), r.Size(), r.PageSize())
		}
		page, err := r.ReadPage(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(page, emptyDatabase(ps)) {
			t.Fatalf("page size %d: page 1 is not the empty database", ps)
		}

		path := filepath.Join(t.TempDir(), "empty.db")
		if err := os.WriteFile(path, page, 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite3", "file:"+path)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		query := func(q string) (s string) {
			t.Helper()
			if err := db.QueryRow(q).Scan(&s); err != nil {
				t.Fatalf("page size %d: %s: %v", ps, q, err)
			}
			return s
		}
		for q, want := range map[string]string{
			"PRAGMA integrity_check":             "ok",
			"PRAGMA journal_mode":                "wal",
			"PRAGMA page_size":                   strconv.Itoa(int(ps)),
			"PRAGMA page_count":                  "1",
			"SELECT count(*) FROM sqlite_schema": "0",
		} {
			if got := query(q); got != want {
				t.Errorf("page size %d: %s = %q, want %q", ps, q, got, want)
			}
		}
		// It is an ordinary database: it takes a table and stays intact.
		if _, err := db.Exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		if got := query("PRAGMA integrity_check"); got != "ok" {
			t.Errorf("page size %d: integrity_check after a write = %q", ps, got)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// checkCreated checks that the prefix holds exactly database id, which a
// reader opens at its first commit, and that the manifest is the first one.
func checkCreated(t *testing.T, st storage.Store, keys format.Keys, id format.DatabaseID) {
	t.Helper()
	ctx := context.Background()
	b, err := get(ctx, st, keys.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	m, err := format.DecodeManifest(b, id)
	if err != nil {
		t.Fatal(err)
	}
	if m.Txn != 0 || m.RetiredThrough != 0 || m.Epoch != 1 || len(m.Groups) != 0 || m.DBSize != 0 {
		t.Fatalf("first manifest %+v", m)
	}
	r, err := Open(ctx, st, keys, NewCache(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	page, err := r.ReadPage(ctx, 1)
	if err != nil || r.Txn() != 1 || !bytes.Equal(page, emptyDatabase(m.PageSize)) {
		t.Fatalf("reader at %d: %v", r.Txn(), err)
	}
}

func TestCreateRace(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	const n = 8
	ids := make([]format.DatabaseID, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { ids[i], errs[i] = Create(ctx, st, keys, 4096, 256) })
	}
	wg.Wait()
	var winner format.DatabaseID
	wins := 0
	for i := range n {
		switch {
		case errs[i] == nil:
			wins++
			winner = ids[i]
		case !errors.Is(errs[i], ErrExists):
			t.Fatalf("creator %d: %v", i, errs[i])
		}
	}
	if wins != 1 {
		t.Fatalf("%d creators won", wins)
	}
	for i := range n {
		if ids[i] != winner {
			t.Fatalf("creator %d reports database %x, the winner is %x", i, ids[i], winner)
		}
	}
	checkCreated(t, st, keys, winner)

	// A later creator loses the same way.
	id, err := Create(ctx, st, keys, 4096, 256)
	if !errors.Is(err, ErrExists) || id != winner {
		t.Fatalf("late creator: %x, %v", id, err)
	}
}

// TestCreateUnknownOutcome makes the first, or every, attempt at each of the
// two creates end with an unknown outcome, landed or not.
func TestCreateUnknownOutcome(t *testing.T) {
	isManifest := func(key string) bool { return filepath.Base(key) == "manifest" }
	for _, step := range []string{"commit", "manifest"} {
		for _, land := range []bool{false, true} {
			for _, every := range []bool{false, true} {
				name := step
				if land {
					name += "/landed"
				}
				if every {
					name += "/every"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					st, keys := newStore(t)
					attempts := 0
					st.setInject(func(_ context.Context, op, key string) (bool, error) {
						if op != "create" || isManifest(key) != (step == "manifest") {
							return false, nil
						}
						attempts++
						if attempts == 1 || every {
							return land && attempts == 1, &storage.Error{Op: op, Key: key, Err: errors.Join(storage.ErrOutcomeUnknown, errInjected)}
						}
						return false, nil
					})
					id, err := Create(ctx, st, keys, 4096, 256)
					st.setInject(nil)
					if !every {
						if err != nil {
							t.Fatal(err)
						}
						checkCreated(t, st, keys, id)
						return
					}
					if !errors.Is(err, storage.ErrOutcomeUnknown) {
						t.Fatalf("unresolved create: %v", err)
					}
					if attempts != createAttempts {
						t.Fatalf("%d attempts", attempts)
					}
					// What is left is no database, or a whole one.
					_, merr := st.Head(ctx, keys.Manifest())
					switch {
					case step == "manifest" && land:
						r, err := Open(ctx, st, keys, NewCache(0), 0)
						if err != nil || r.Txn() != 1 {
							t.Fatalf("landed manifest: %v", err)
						}
					case !errors.Is(merr, storage.ErrNotFound):
						t.Fatalf("manifest after an unresolved create: %v", merr)
					default:
						if _, err := Open(ctx, st, keys, NewCache(0), 0); !errors.Is(err, storage.ErrNotFound) {
							t.Fatalf("open: %v", err)
						}
						id, err := Create(ctx, st, keys, 4096, 256)
						if err != nil {
							t.Fatal(err)
						}
						checkCreated(t, st, keys, id)
					}
				})
			}
		}
	}
}

// TestCreateLosesAfterUnknownOutcome: a creator's manifest create has an
// unknown outcome and did not land; another creator wins; the first
// creator's retry fails its precondition and reports the winner.
func TestCreateLosesAfterUnknownOutcome(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	var winner format.DatabaseID
	var werr error
	st.setInject(func(_ context.Context, op, key string) (bool, error) {
		if op == "create" && key == keys.Manifest() && winner == (format.DatabaseID{}) && werr == nil {
			st.setInject(nil)
			winner, werr = Create(ctx, st, keys, 4096, 256)
			return false, &storage.Error{Op: op, Key: key, Err: storage.ErrOutcomeUnknown}
		}
		return false, nil
	})
	id, err := Create(ctx, st, keys, 4096, 256)
	if werr != nil {
		t.Fatal(werr)
	}
	if !errors.Is(err, ErrExists) || id != winner {
		t.Fatalf("loser: %x, %v; winner %x", id, err, winner)
	}
	checkCreated(t, st, keys, winner)
}

// TestCreateNoEffectFailures: failures with no effect are retried.
func TestCreateNoEffectFailures(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	n := 0
	st.setInject(func(_ context.Context, op, _ string) (bool, error) {
		if op == "create" && n < 2 {
			n++
			return false, errInjected
		}
		return false, nil
	})
	id, err := Create(ctx, st, keys, 4096, 256)
	st.setInject(nil)
	if err != nil {
		t.Fatal(err)
	}
	checkCreated(t, st, keys, id)

	// A cancelled context stops the creator.
	st2, keys2 := newStore(t)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Create(cctx, st2, keys2, 4096, 256); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
}
