package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

func TestFollowerPoll(t *testing.T) {
	ctx := t.Context()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 1, 512, 4)
	f := newFollower(ctx)
	var calls []string
	var absenceStart time.Time
	st.setInject(func(_ context.Context, op, key string) (bool, error) {
		calls = append(calls, op+" "+key)
		if op == "getrange" && key == keys.Commit(h.db, h.head()+1) {
			absenceStart = time.Now()
		}
		return false, nil
	})
	check := func(want ...string) {
		t.Helper()
		calls = nil
		if err := f.poll(st, keys, nil); err != nil {
			t.Fatal(err)
		}
		if f.current.observedAt.IsZero() || f.current.observedAt.After(absenceStart) {
			t.Fatal("freshness timestamp must precede the absence observation")
		}
		if !reflect.DeepEqual(calls, want) {
			t.Fatalf("poll calls %v, want %v", calls, want)
		}
	}
	manifest := keys.Manifest()
	log := func(txn uint64) string { return keys.Commit(h.db, txn) }

	check("get "+manifest, "getrange "+log(1), "getrange "+log(1), "getrange "+log(2), "head "+log(3))
	check("head "+manifest, "getrange "+log(2), "head "+log(3))

	old := f.current
	h.commit(2, map[uint32][]byte{1: h.bytes(512), 2: h.bytes(512)})
	check("head "+manifest, "getrange "+log(2), "getrange "+log(2), "getrange "+log(3), "head "+log(4))
	if old.Txn() != 1 || f.current.Txn() != 2 {
		t.Fatalf("published transactions old=%d new=%d", old.Txn(), f.current.Txn())
	}
	if err := h.checkReader(ctx, old, false); err != nil {
		t.Fatal(err)
	}
	h.swapEpoch()
	h.bump(2)
	check("head "+manifest, "get "+manifest, "getrange "+log(3), "getrange "+log(4), "head "+log(5))
}

func TestFollowerWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, stop := context.WithCancel(t.Context())
		defer stop()
		f := newFollower(ctx)
		f.current = &Reader{txn: 1, observedAt: time.Now()}

		pins := make(chan Reader, 1)
		go func() {
			r, _ := f.pin(ctx, 2)
			pins <- r
		}()
		synctest.Wait()
		f.mu.Lock()
		f.current = &Reader{txn: 2, observedAt: time.Now()}
		close(f.changed)
		f.changed = make(chan struct{})
		f.mu.Unlock()
		if r := <-pins; r.Txn() != 2 {
			t.Fatalf("waiter pinned transaction %d, want 2", r.Txn())
		}

		check := func(ctx context.Context, txn uint64, want error, elapsed time.Duration) {
			t.Helper()
			start := time.Now()
			if r, err := f.pin(ctx, txn); !errors.Is(err, want) || time.Since(start) != elapsed {
				t.Fatalf("pin(%d) = (%d, %v) after %v, want %v after %v", txn, r.Txn(), err, time.Since(start), want, elapsed)
			}
		}
		check(ctx, 3, ErrBehind, 500*time.Millisecond)
		short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		check(short, 3, context.DeadlineExceeded, 20*time.Millisecond)

		time.Sleep(2*time.Second - 520*time.Millisecond)
		check(ctx, 2, ErrBehind, 500*time.Millisecond)
		stop()
		check(t.Context(), 0, context.Canceled, 0)
	})
}

func startTestFollower(t *testing.T, st storage.Store, keys format.Keys, cache *Cache) *Follower {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	f := Follow(ctx, st, keys, cache)
	t.Cleanup(func() {
		cancel()
		<-f.Done()
	})
	return f
}

func openFollower(t *testing.T, f *Follower, minTxn uint64) *ReadConn {
	t.Helper()
	c, err := f.Open(t.Context(), minTxn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	if c.Txn() < minTxn {
		t.Fatalf("follower opened at %d below requested %d", c.Txn(), minTxn)
	}
	return c
}

func TestFollowerSQLite(t *testing.T) {
	st, keys := newStore(t)
	ctx := t.Context()
	id, err := Create(ctx, st.Store, keys, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	f := startTestFollower(t, st, keys, nil)
	empty := openFollower(t, f, 1)
	if err := empty.Run(ctx, func(db *sqlite3.Conn) error {
		if ro, _ := db.ReadOnly("main"); !ro {
			t.Fatal("main database is not open read-only")
		}
		if n := mustQuery(t, db, "SELECT count(*) FROM sqlite_schema"); !reflect.DeepEqual(n, []string{"0"}) {
			t.Fatalf("empty database schema has %v objects", n)
		}
		if err := db.Exec("CREATE TABLE prohibited(x)"); !errors.Is(err, sqlite3.READONLY) {
			t.Fatalf("write to follower: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	o := newOracle(t)
	l := lead(t, st.Store, keys, takeOffice(t, st.Store, keys), testConfig())
	writer := open(t, l)
	write := func(sql string) uint64 {
		t.Helper()
		o.exec(t, sql)
		return mustRun(t, writer, sql)
	}
	write("CREATE TABLE items(k INTEGER PRIMARY KEY, v TEXT)")
	txn := write("INSERT INTO items SELECT value, printf('%0900d',value) FROM generate_series(1,12)")
	slow := openFollower(t, f, txn)
	wantOld := mustDump(t, o.db)

	// Pause the first uncached data fetch from the insertion. Another
	// connection must be able to open and read a later schema before it ends.
	g := newGate()
	defer g.open()
	var blocked atomic.Bool
	st.setInject(func(ctx context.Context, op, key string) (bool, error) {
		if op == "getrange" && key == keys.Commit(id, txn) && blocked.CompareAndSwap(false, true) {
			close(g.reached)
			select {
			case <-g.release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		return false, nil
	})
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	type result struct {
		rows string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var res result
		res.err = slow.Run(readCtx, func(db *sqlite3.Conn) (err error) {
			res.rows, err = dump(db)
			return err
		})
		done <- res
	}()
	select {
	case <-g.reached:
	case res := <-done:
		t.Fatalf("slow read ended before the gate: %v", res.err)
	case <-readCtx.Done():
		t.Fatal(readCtx.Err())
	}

	write("ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'new'")
	latest := write("UPDATE items SET v = 'replaced' WHERE k > 0")
	current := openFollower(t, f, latest)
	if err := current.Run(ctx, func(db *sqlite3.Conn) error {
		if got, want := mustDump(t, db), mustDump(t, o.db); got != want {
			t.Fatalf("new snapshot differs from oracle at %d", current.Txn())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	g.open()
	select {
	case res := <-done:
		if res.err != nil || res.rows != wantOld || slow.Txn() != txn {
			t.Fatalf("old snapshot at %d diverged after publication: %v", slow.Txn(), res.err)
		}
	case <-readCtx.Done():
		t.Fatal(readCtx.Err())
	}
}

func TestFollowerResize(t *testing.T) {
	st, keys, office := newDatabase(t, 1024)
	l := lead(t, st, keys, office, testConfig())
	writer := open(t, l)
	ctx := t.Context()
	cache := NewCache(1 << 20)
	f := newFollower(ctx)
	o := newOracle(t)
	var counts []uint32
	for _, sql := range []string{
		"BEGIN; CREATE TABLE items(k, v); INSERT INTO items SELECT value, zeroblob(900) FROM generate_series(1,16); COMMIT",
		"BEGIN; DELETE FROM items WHERE k > 1; COMMIT",
		"VACUUM",
		"INSERT INTO items SELECT value, zeroblob(900) FROM generate_series(2,24)",
	} {
		txn := mustRun(t, writer, sql)
		o.exec(t, sql)
		if err := f.poll(st, keys, cache); err != nil {
			t.Fatal(err)
		}
		before, err := f.current.ReadPage(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		c := openFollower(t, f, txn)
		if err := c.Run(ctx, func(db *sqlite3.Conn) error {
			if got, want := mustDump(t, db), mustDump(t, o.db); got != want {
				t.Fatalf("snapshot differs from oracle after %s", sql)
			}
			if got := mustQuery(t, db, "PRAGMA page_count"); !reflect.DeepEqual(got, []string{fmt.Sprint(f.current.Size())}) {
				t.Fatalf("page count %v, pinned size %d", got, f.current.Size())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		after, err := f.current.ReadPage(ctx, 1)
		if err != nil || !bytes.Equal(before, after) || after[18] != 2 || after[19] != 2 {
			t.Fatalf("durable WAL header was changed by SQLite read: %v", err)
		}
		counts = append(counts, f.current.Size())
	}
	if counts[1] <= counts[2] || counts[2] >= counts[3] {
		t.Fatalf("expected shrink then regrow, page counts %v", counts)
	}
}

func TestFollowerCompaction(t *testing.T) {
	ctx := t.Context()
	st, keys, h := corruptFixture(t)
	f := newFollower(ctx)
	// Pin at 6 on a base of 3. Publication through 7 leaves that pin alone.
	if err := f.poll(st, keys, NewCache(0)); err != nil {
		t.Fatal(err)
	}
	pin, err := f.pin(ctx, 6)
	if err != nil {
		t.Fatal(err)
	}
	h.commit(12, map[uint32][]byte{1: h.bytes(512), 6: h.bytes(512)})
	if err := f.poll(st, keys, nil); err != nil {
		t.Fatal(err)
	}
	passed, err := f.pin(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}

	// Retire the log and delete the obsolete base. The new base at 6
	// explains both object losses and still permits reading transaction 6.
	h.compact(6)
	h.retireAll()
	h.collect()
	if err := h.checkReader(ctx, &pin, false); err != nil {
		t.Fatal(err)
	}
	if pin.Txn() != 6 || pin.a.m.Txn != 6 || f.current.a.m.Txn != 3 {
		t.Fatal("re-anchoring altered the published reader or moved the pin")
	}

	// Move past the second pin and verify the VFS preserves ErrCompacted.
	h.commit(12, map[uint32][]byte{1: h.bytes(512), 2: h.bytes(512)})
	h.compact(8)
	h.retireAll()
	h.collect()
	file := &snapshotFile{reader: passed, ctx: ctx}
	if n, err := file.ReadAt(make([]byte, 100), 0); n != 0 || !errors.Is(err, ErrCompacted) {
		t.Fatalf("read a compacted pin: %d, %v", n, err)
	}
	if err := f.poll(st, keys, nil); err != nil || f.current.Txn() != 8 {
		t.Fatalf("poll after compaction: %v, applied %d", err, f.current.Txn())
	}
}

func TestFollowerWalk(t *testing.T) {
	t.Run("retired table", func(t *testing.T) {
		st, keys := newStore(t)
		h := newHistory(t, st.Store, keys, 1, 512, 4)
		f := newFollower(t.Context())
		if err := f.poll(st, keys, nil); err != nil {
			t.Fatal(err)
		}
		h.randomCommit(8)
		h.randomCommit(8)
		calls := 0
		st.setInject(func(_ context.Context, op, key string) (bool, error) {
			if op == "getrange" && key == keys.Commit(h.db, 2) {
				calls++
				if calls == 2 {
					h.compact(2)
					h.retireAll()
				}
			}
			return false, nil
		})
		if err := f.poll(st, keys, nil); err != nil {
			t.Fatal(err)
		}
		if f.current.Txn() != 3 || f.current.a.m.Txn != 2 {
			t.Fatalf("published at %d on base %d", f.current.Txn(), f.current.a.m.Txn)
		}
		if err := h.checkReader(t.Context(), f.current, false); err != nil {
			t.Fatal(err)
		}
	})
	for _, nth := range []int{1, 2} {
		t.Run(fmt.Sprint("deleted on fetch ", nth), func(t *testing.T) {
			ctx := t.Context()
			st, keys := newStore(t)
			h := newHistory(t, st.Store, keys, 1, 512, 4)
			f := newFollower(ctx)
			if err := f.poll(st, keys, nil); err != nil {
				t.Fatal(err)
			}
			h.randomCommit(8)
			h.randomCommit(8)
			calls := 0
			st.setInject(func(_ context.Context, op, key string) (bool, error) {
				if op == "getrange" && key == keys.Commit(h.db, 2) {
					calls++
					if calls == nth {
						return false, st.Store.Delete(ctx, key)
					}
				}
				return false, nil
			})
			if err := f.poll(st, keys, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("poll with missing commit: %v", err)
			}
			if f.current.Txn() != 1 {
				t.Fatalf("failed walk published transaction %d", f.current.Txn())
			}
			if _, err := f.pin(ctx, 1); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("admission after detecting missing object: %v", err)
			}
		})
	}
}

func TestFollowerVFS(t *testing.T) {
	ctx := t.Context()
	st, keys := newStore(t)
	const ps, gs = 4096, 1024
	h := newHistory(t, st.Store, keys, 8, ps, gs)
	lock := format.LockPage(ps)
	m, _ := h.manifest()
	m.Txn, m.DBSize = 2, lock
	m.Groups = make([]format.GroupRef, format.GroupCount(m.DBSize, gs))
	// The lock page is the first page in this group. Its bytes are
	// synthesized even when a read of the backing object would fail.
	m.Groups[len(m.Groups)-1] = format.GroupRef{Txn: 2, Tag: 1}
	h.publish(m)
	r, err := Open(ctx, st, keys, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := &snapshotFile{reader: *r, ctx: ctx}
	st.resetCounts()
	buf := bytes.Repeat([]byte{0xff}, ps+4)
	if n, err := f.ReadAt(buf, 1<<30-2); n != ps+2 || !errors.Is(err, io.EOF) || !bytes.Equal(buf, make([]byte, len(buf))) {
		t.Fatalf("read straddling lock page and EOF: %d, %v", n, err)
	}
	if calls, _ := st.counts(); calls != 0 {
		t.Fatalf("lock page fetched from storage (%d calls)", calls)
	}

	v := snapshotVFS{f}
	for _, flags := range []vfs.OpenFlag{
		vfs.OPEN_MAIN_DB | vfs.OPEN_READWRITE,
		vfs.OPEN_MAIN_DB | vfs.OPEN_CREATE | vfs.OPEN_READONLY,
		vfs.OPEN_MAIN_JOURNAL | vfs.OPEN_READONLY,
		vfs.OPEN_WAL | vfs.OPEN_READONLY,
	} {
		if file, _, err := v.Open(followerFilename, flags); file != nil || !errors.Is(err, sqlite3.CANTOPEN) {
			t.Fatalf("open with flags %x: %v", flags, err)
		}
	}
	if _, err := f.WriteAt(buf, 0); !errors.Is(err, sqlite3.READONLY) {
		t.Fatalf("write: %v", err)
	}
	if err := f.Truncate(0); !errors.Is(err, sqlite3.READONLY) {
		t.Fatalf("truncate: %v", err)
	}
}

func TestFollowerHeader(t *testing.T) {
	ctx := t.Context()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 7, 512, 4)
	page := emptyDatabase(512)
	page[27], page[95] = 57, 42
	txn := h.commit(2, map[uint32][]byte{1: page, 2: bytes.Repeat([]byte{3}, 512)})
	r, err := Open(ctx, st, keys, NewCache(1<<20), txn)
	if err != nil {
		t.Fatal(err)
	}
	f := &snapshotFile{reader: *r, ctx: ctx}
	buf := make([]byte, 500)
	if n, err := f.ReadAt(buf, 17); n != len(buf) || err != nil {
		t.Fatalf("partial read: %d, %v", n, err)
	}
	if buf[1] != 1 || buf[2] != 1 || binary.BigEndian.Uint32(buf[11:15]) != 2 ||
		!bytes.Equal(buf[7:11], buf[75:79]) || !bytes.Equal(buf[495:], []byte{3, 3, 3, 3, 3}) {
		t.Fatal("page 1 header or cross-page bytes are incorrect")
	}
	original, err := r.ReadPage(ctx, 1)
	if err != nil || !bytes.Equal(original, page) {
		t.Fatalf("cached durable page 1 changed: %v", err)
	}
}

func TestFollowerReadCancellation(t *testing.T) {
	ctx := t.Context()
	st, keys := newStore(t)
	if _, err := Create(ctx, st.Store, keys, 512, 64); err != nil {
		t.Fatal(err)
	}
	f := newFollower(ctx)
	if err := f.poll(st, keys, nil); err != nil {
		t.Fatal(err)
	}
	c := openFollower(t, f, 1)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var observed context.Context
	st.setInject(func(ctx context.Context, op, key string) (bool, error) {
		if op == "getrange" && key == keys.Commit(f.current.a.m.DB, 1) {
			observed = ctx
			cancel()
			return false, ctx.Err()
		}
		return false, nil
	})
	err := c.Run(readCtx, func(db *sqlite3.Conn) error {
		_ = db.Exec("SELECT count(*) FROM sqlite_schema")
		return nil
	})
	if observed != readCtx || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read reached storage with its context=%t, error=%v", observed == readCtx, err)
	}
	st.setInject(nil)
	if err := c.Run(ctx, func(db *sqlite3.Conn) error {
		return db.Exec("SELECT count(*) FROM sqlite_schema")
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFollowerRefreshFailure(t *testing.T) {
	ctx := t.Context()
	st, keys := newStore(t)
	h := newHistory(t, st.Store, keys, 2, 512, 4)
	f := newFollower(ctx)
	if err := f.poll(st, keys, nil); err != nil {
		t.Fatal(err)
	}
	before := f.current
	st.setInject(func(context.Context, string, string) (bool, error) {
		return false, errInjected
	})
	if err := f.poll(st, keys, nil); !errors.Is(err, errInjected) {
		t.Fatalf("transient poll error: %v", err)
	}
	if r, err := f.pin(ctx, 1); err != nil || r.observedAt != before.observedAt {
		t.Fatalf("pin after transient failure at %v: %v", r.observedAt, err)
	}
	st.setInject(nil)

	m, _ := h.manifest()
	original := h.encodeManifest(m)
	m.DB[0] ^= 1
	h.overwrite(keys.Manifest(), h.encodeManifest(m))
	if err := f.poll(st, keys, nil); !errors.Is(err, format.ErrMisplaced) || f.current != before {
		t.Fatalf("poll after manifest identity change: %v", err)
	}
	if _, err := f.pin(ctx, 1); !errors.Is(err, format.ErrMisplaced) {
		t.Fatalf("pin after manifest identity change: %v", err)
	}
	h.overwrite(keys.Manifest(), original)
	if err := f.poll(st, keys, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pin(ctx, 1); err != nil {
		t.Fatal(err)
	}
}
