package engine

import (
	"context"
	"fmt"
)

// IndexFloor is the transaction number above which the headers Lookup
// answers from are complete up to the head: Lookup's ok is true exactly for
// an `above` at or above it. It never falls. A caller that reads it after a
// Lookup gets a floor at or above the one that Lookup used.
func (l *Leader) IndexFloor() uint64 {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return l.indexFloor
}

// Header reads the header of commit txn from storage, live or retired, and
// returns its commit identifier, nil for an epoch bump, and whether it is an
// epoch bump. txn must be from 1 to the head: every name up to the head is
// occupied for good, and its first object is its only one (record 0004
// decision 6), so the answer never changes. Header is for a resolve whose
// range reaches below IndexFloor (record 0005, decision 4, step 5). Its
// read ends with ctx and when the leader closes.
func (l *Leader) Header(ctx context.Context, txn uint64) (id []byte, bump bool, err error) {
	if head := l.Head(); txn == 0 || txn > head {
		return nil, false, fmt.Errorf("engine: commit %d is not from 1 to the head, %d", txn, head)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(l.ctx, cancel)()
	h, _, err := readHeader(ctx, l.st, l.keys, l.office.DB, txn)
	if err != nil {
		return nil, false, err
	}
	return h.CommitID, h.EpochBump, nil
}
