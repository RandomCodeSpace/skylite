package engine

import (
	"bytes"
	"fmt"

	"github.com/RandomCodeSpace/skylite/internal/format"
)

// Found is what a leader found at the name where it tried to create its next
// commit, after the create failed its precondition (decision 6).
type Found int

const (
	// FoundUnknown is anything that is neither of the others. An invariant
	// is broken: the leader stops as for an indeterminate commit, and the
	// client is told unknown.
	FoundUnknown Found = iota
	// FoundOwn is the leader's own commit, live or retired: committed.
	FoundOwn
	// FoundBump is another candidacy's epoch bump: the leader is fenced, the
	// commit is definitely not committed, and the leader fail-stops.
	FoundBump
)

// Recognise decides what found, the bytes read at the name of the commit the
// leader in office o built as built, means (decision 6, "Resolving a
// create"). The first object created at a leader's next name is its own
// commit or some candidate's epoch bump, and it is the only object that name
// will ever hold. So found is FoundOwn if it is built or built's retired
// form, and FoundBump if it is a valid epoch bump of database o.DB at that
// name with an epoch other than o.Epoch. Anything else, an absent object
// (found empty) included, is FoundUnknown, with an error saying why.
func (o Office) Recognise(built, found []byte) (Found, error) {
	if len(built) < format.CommitHeaderSize {
		return FoundUnknown, fmt.Errorf("%w: built commit of %d bytes", ErrCorrupt, len(built))
	}
	bh, err := format.DecodeCommitHeader(built[:format.CommitHeaderSize], o.DB)
	if err != nil {
		return FoundUnknown, fmt.Errorf("engine: built commit: %w", err)
	}
	if bh.EpochBump || bh.Retired || bh.Epoch != o.Epoch || bh.Txn <= o.Head {
		return FoundUnknown, fmt.Errorf("%w: commit %d at epoch %d is not a commit of office %d after %d", ErrCorrupt, bh.Txn, bh.Epoch, o.Epoch, o.Head)
	}
	if bytes.Equal(found, built) {
		return FoundOwn, nil
	}
	if retired, err := bh.Retire(); err == nil && bytes.Equal(found, retired) {
		return FoundOwn, nil
	}
	if len(found) < format.CommitHeaderSize {
		return FoundUnknown, fmt.Errorf("%w: %d bytes at commit %d after a precondition failure", ErrCorrupt, len(found), bh.Txn)
	}
	fh, err := format.DecodeCommitHeader(found[:format.CommitHeaderSize], o.DB)
	if err == nil {
		err = fh.CheckKey(o.DB, bh.Txn)
	}
	switch {
	case err != nil:
		return FoundUnknown, fmt.Errorf("engine: commit %d: %w", bh.Txn, err)
	case !fh.EpochBump:
		return FoundUnknown, fmt.Errorf("%w: commit %d holds a commit that is not this leader's", ErrCorrupt, bh.Txn)
	case fh.Epoch == o.Epoch:
		return FoundUnknown, fmt.Errorf("%w: commit %d holds an epoch bump at this leader's epoch %d", ErrCorrupt, bh.Txn, o.Epoch)
	case int64(len(found)) != fh.Size():
		return FoundUnknown, fmt.Errorf("%w: epoch bump %d is %d bytes", ErrCorrupt, bh.Txn, len(found))
	}
	return FoundBump, nil
}

// Holds reports whether manifest m still carries office o's database, epoch
// and writer nonce. A leader that reads a manifest it does not hold stops
// compacting and leaves office (decision 8); a candidate takes office only
// if it holds the manifest after its bump (decision 9).
func (o Office) Holds(m format.Manifest) bool {
	return m.DB == o.DB && m.Epoch == o.Epoch && m.Nonce == o.Nonce
}
