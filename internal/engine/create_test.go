package engine

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver" // the SQLite engine of record 0004, v0.37.0

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// TestCreateEmptyDatabaseIsValidSQLite creates a database at every page
// size, reads its first commit and page 1 back through a Reader and hands
// the page to real SQLite, which reads it without changing it and then
// writes to it.
func TestCreateEmptyDatabaseIsValidSQLite(t *testing.T) {
	for ps := uint32(format.MinPageSize); ps <= format.MaxPageSize; ps *= 2 {
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
		if h := r.commits[0].h; h.Epoch != 1 || h.EpochBump || len(h.CommitID) != 0 || h.PageCount != 1 || h.DBSize != 1 {
			t.Fatalf("page size %d: first commit %+v", ps, h)
		}
		page, err := r.ReadPage(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(page, emptyDatabase(ps)) {
			t.Fatalf("page size %d: page 1 is not the empty database", ps)
		}
		// WAL mode, and a change counter SQLite trusts the size beside.
		if page[18] != 2 || page[19] != 2 || !bytes.Equal(page[24:28], page[92:96]) || !bytes.Equal(page[24:32], []byte{0, 0, 0, 1, 0, 0, 0, 1}) {
			t.Fatalf("page size %d: header bytes 18-19 %v, 24-31 %v, 92-95 %v", ps, page[18:20], page[24:32], page[92:96])
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
			"PRAGMA quick_check":                 "ok",
			"PRAGMA journal_mode":                "wal",
			"PRAGMA page_size":                   strconv.Itoa(int(ps)),
			"PRAGMA page_count":                  "1",
			"PRAGMA encoding":                    "UTF-8",
			"PRAGMA schema_version":              "0",
			"SELECT count(*) FROM sqlite_schema": "0",
		} {
			if got := query(q); got != want {
				t.Errorf("page size %d: %s = %q, want %q", ps, q, got, want)
			}
		}
		if b, err := os.ReadFile(path); err != nil || !bytes.Equal(b, page) { //nolint:gosec // the test's own temporary file
			t.Fatalf("page size %d: reading changed the file: %v", ps, err)
		}
		// It is an ordinary database: it takes a table and stays intact.
		if _, err := db.Exec("CREATE TABLE t(x); INSERT INTO t VALUES (1); INSERT INTO t SELECT x+1 FROM t"); err != nil {
			t.Fatal(err)
		}
		if got := query("PRAGMA integrity_check"); got != "ok" {
			t.Errorf("page size %d: integrity_check after a write = %q", ps, got)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if b, err := os.ReadFile(path); err != nil || b[18] != 2 || b[19] != 2 { //nolint:gosec // the test's own temporary file
			t.Fatalf("page size %d: not in WAL mode after writing: %v", ps, err)
		}
	}
}

// checkCreated checks that the prefix holds exactly database id, which a
// reader opens at its first commit, and that the manifest is the first one.
func checkCreated(t *testing.T, st storage.Store, keys format.Keys, id format.DatabaseID) {
	t.Helper()
	ctx := context.Background()
	b, err := get(ctx, st, keys.Manifest(), 1<<40)
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

	// A failed read of the manifest stops the creator before it writes.
	st3, keys3 := newStore(t)
	creates := 0
	st3.setInject(func(_ context.Context, op, _ string) (bool, error) {
		if op == "create" {
			creates++
		}
		if op == "get" {
			return false, errInjected
		}
		return false, nil
	})
	if _, err := Create(ctx, st3, keys3, 4096, 256); !errors.Is(err, errInjected) || creates != 0 {
		t.Fatalf("create after a failed manifest read: %d creates: %v", creates, err)
	}

	// A cancelled context stops the creator.
	st2, keys2 := newStore(t)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Create(cctx, st2, keys2, 4096, 256); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
}

// TestCreateBadArguments: invalid page and group sizes fail before Create
// builds anything or sends a request.
func TestCreateBadArguments(t *testing.T) {
	for _, sizes := range [][2]uint32{{0, 4}, {1, 4}, {100, 4}, {3000, 4}, {1<<28 + 1, 4}, {1 << 17, 1}, {4096, 0}, {4096, 3}, {4096, 1 << 15}} {
		st, keys := newStore(t)
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		_, err := Create(context.Background(), st, keys, sizes[0], sizes[1])
		runtime.ReadMemStats(&m1)
		if !errors.Is(err, format.ErrInvalid) {
			t.Errorf("sizes %v: %v", sizes, err)
		}
		if reqs, _ := st.counts(); reqs != 0 || m1.TotalAlloc-m0.TotalAlloc > 1<<20 {
			t.Errorf("sizes %v: %d requests and %d bytes allocated", sizes, reqs, m1.TotalAlloc-m0.TotalAlloc)
		}
	}
}

// TestCreateOverExistingWritesNothing: Create on a prefix that holds a
// database reads its manifest and writes nothing.
func TestCreateOverExistingWritesNothing(t *testing.T) {
	ctx := context.Background()
	st, keys := newStore(t)
	id, err := Create(ctx, st, keys, 4096, 256)
	if err != nil {
		t.Fatal(err)
	}
	before, err := st.List(ctx, "dbs/test/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		st.resetCounts()
		got, err := Create(ctx, st, keys, 4096, 256)
		if !errors.Is(err, ErrExists) || got != id {
			t.Fatalf("%x, %v", got, err)
		}
		if reqs, _ := st.counts(); reqs != 1 {
			t.Fatalf("a losing create made %d requests", reqs)
		}
	}
	if after, err := st.List(ctx, "dbs/test/", "", 1000); err != nil || len(after) != len(before) {
		t.Fatalf("%d objects, then %d: %v", len(before), len(after), err)
	}
}

// TestCreateKeepsUnknownOutcome: an unknown outcome stays the answer after
// later attempts fail with no effect, and an invalid argument is not
// retried.
func TestCreateKeepsUnknownOutcome(t *testing.T) {
	for _, first := range []error{storage.ErrOutcomeUnknown, storage.ErrInvalidArgument} {
		st, keys := newStore(t)
		attempts := 0
		st.setInject(func(_ context.Context, op, key string) (bool, error) {
			if op != "create" {
				return false, nil
			}
			if attempts++; attempts == 1 {
				return false, &storage.Error{Op: op, Key: key, Err: first}
			}
			return false, &storage.Error{Op: op, Key: key, Err: errInjected}
		})
		_, err := Create(context.Background(), st, keys, 4096, 256)
		want := createAttempts
		if errors.Is(first, storage.ErrInvalidArgument) {
			want = 1
		}
		if !errors.Is(err, first) || attempts != want {
			t.Fatalf("%v first: %d attempts: %v", first, attempts, err)
		}
	}
}

// TestCreateWaits: Create waits between attempts, and the wait ends with
// the context.
func TestCreateWaits(t *testing.T) {
	defer func(w func(context.Context, int) error) { createWait = w }(createWait)
	var waits []int
	createWait = func(_ context.Context, attempt int) error {
		waits = append(waits, attempt)
		return nil
	}
	st, keys := newStore(t)
	n := 0
	st.setInject(func(_ context.Context, op, _ string) (bool, error) {
		if op == "create" && n < 3 {
			n++
			return false, errInjected
		}
		return false, nil
	})
	if _, err := Create(context.Background(), st, keys, 4096, 256); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(waits, []int{0, 1, 2}) {
		t.Fatalf("waits %v", waits)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backoff(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("backoff after cancel: %v", err)
	}
	createWait = backoff
	ctx, cancel = context.WithCancel(context.Background())
	st2, keys2 := newStore(t)
	st2.setInject(func(_ context.Context, op, _ string) (bool, error) {
		if op == "create" {
			cancel() // the first attempt fails, and the wait after it ends at once
			return false, errInjected
		}
		return false, nil
	})
	if _, err := Create(ctx, st2, keys2, 4096, 256); !errors.Is(err, context.Canceled) {
		t.Fatalf("create cancelled while waiting: %v", err)
	}
}
