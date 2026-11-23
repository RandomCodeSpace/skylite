package engine

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestIndexFloorAndHeader: a leader that takes office on a manifest above
// commits has its index floor there, Lookup covers exactly the numbers at
// or above it, the floor does not fall, and Header reads every commit up to
// the head from storage, those below the floor and the bump included, and
// refuses numbers outside 1 to the head and an ended context.
func TestIndexFloorAndHeader(t *testing.T) {
	fastCompaction(t)
	st, keys, o := newDatabase(t, 4096)
	cfg := testConfig()
	cfg.CompactInterval, cfg.Grace = 10*time.Millisecond, time.Millisecond
	l := lead(t, st, keys, o, cfg)
	w := open(t, l)
	mustRun(t, w, "CREATE TABLE t(x)")
	ids := [][]byte{newID(), newID()}
	txns := []uint64{runID(t, w, ids[0], "INSERT INTO t VALUES (1)"), runID(t, w, ids[1], "INSERT INTO t VALUES (2)")}
	eventually(t, "a manifest above the commits", func() bool { return checkLeaderManifest(t, st, keys, o.DB).Txn >= txns[1] })
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	o2 := takeOffice(t, st.Store, keys)
	l2 := lead(t, st, keys, o2, testConfig())
	f := l2.IndexFloor()
	if f < txns[1] || f >= o2.Head {
		t.Fatalf("index floor %d, want the manifest, from %d to below the bump %d", f, txns[1], o2.Head)
	}
	if _, _, _, ok := l2.Lookup(nil, f); !ok {
		t.Errorf("Lookup at the floor %d not covered", f)
	}
	if _, _, _, ok := l2.Lookup(nil, f-1); ok {
		t.Errorf("Lookup below the floor %d covered", f)
	}
	own := runID(t, open(t, l2), newID(), "INSERT INTO t VALUES (3)")
	if g := l2.IndexFloor(); g < f {
		t.Errorf("index floor fell from %d to %d", f, g)
	}
	ctx := context.Background()
	for i, txn := range txns {
		if id, bump, err := l2.Header(ctx, txn); err != nil || bump || !bytes.Equal(id, ids[i]) {
			t.Errorf("Header(%d): %x, %v, %v; want %x", txn, id, bump, err, ids[i])
		}
	}
	if id, bump, err := l2.Header(ctx, o2.Head); err != nil || !bump || id != nil {
		t.Errorf("Header of the bump %d: %x, %v, %v", o2.Head, id, bump, err)
	}
	for _, txn := range []uint64{0, own + 1} {
		if _, _, err := l2.Header(ctx, txn); err == nil {
			t.Errorf("Header(%d) with head %d: no error", txn, own)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := l2.Header(cancelled, txns[0]); err == nil {
		t.Error("Header on an ended context: no error")
	}
}
