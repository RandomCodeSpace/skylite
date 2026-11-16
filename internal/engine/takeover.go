package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

var (
	// ErrLost means a candidacy ended without office because another
	// candidacy changed the manifest: the epoch swap was beaten, or the
	// manifest no longer carries the candidate's epoch and nonce. The
	// candidate never serves at that epoch; a node may start a new
	// candidacy. Lease.Retake returns it when the manifest has moved past
	// the lease's epoch: its leader is deposed.
	ErrLost = errors.New("engine: candidacy lost to another")
	// ErrExhausted means an epoch or a transaction number is at its maximum,
	// so the database cannot take another takeover or another commit. It
	// stops instead of wrapping (invariant 1).
	ErrExhausted = errors.New("engine: epoch or transaction number at its maximum")
)

// Candidate is a node attempting a takeover.
type Candidate struct {
	// Holder names the node in the lease: 1 to 128 of A-Z a-z 0-9 . _ -.
	// Two processes that run at once never share it.
	Holder string
	// TTL is how long a lease lasts after its last renewal.
	TTL time.Duration
	// Now is the clock the lease is stamped and judged by.
	Now func() time.Time
}

// Takeover runs one candidacy for the database under keys' prefix, as
// decision 9 of the storage engine record describes, and returns the
// resulting Office and the node's lease:
//
//  1. It reads the manifest and takes the lease for the manifest's epoch
//     plus one.
//  2. It swaps the manifest to that epoch with a new writer nonce,
//     conditioned on the version it read, and resolves the outcome as
//     decision 6 says, recognising its own swap by its bytes.
//  3. It walks the commit log from the manifest's transaction number to the
//     head. A retired commit or a log object whose version changes under it
//     sends it back to the manifest. Once a third of the lease's time to live
//     has passed since its last lease write, or its last attempt at one, it
//     tries to renew the lease; the outcome ends nothing, because only the
//     manifest decides whether the candidacy goes on.
//  4. Before every attempt at its epoch-bump commit it re-reads the manifest
//     and gives up unless the manifest still carries its epoch and nonce. It
//     creates the bump at the head plus one, recognising its own bump by its
//     bytes, which its epoch determines, reading back at most one byte more
//     than the bump. If another object holds the name, it walks on and tries
//     again; it moves only past a name another object occupies, so a
//     candidacy has at most one bump in the log, and a walk that does not
//     reach that name is corruption.
//  5. It reads the manifest again and takes office only if the manifest
//     still carries its epoch and nonce. If a renewal in step 3 found the
//     lease taken, it takes it back by step 1's rules, best effort.
//
// Another candidacy can take the lease while this one runs, so the returned
// Lease may be one that another holder holds: its Renew then reports
// ErrLeaseHeld.
//
// Every conditional write is repeated identically after an unknown outcome
// until it has a definite answer, up to a bound. Without a definite success
// for its swap or its bump the candidate gives up: Takeover returns an error,
// and one that wraps storage.ErrOutcomeUnknown means that write may still
// land. An error means the candidate never serves at that epoch. It holds no
// Office and gets no Lease. A candidacy whose swap definitely did not land
// releases the lease it took; any other leaves it to expire, or to be taken
// back by the same holder's next candidacy.
func Takeover(ctx context.Context, st storage.Store, keys format.Keys, c Candidate) (Office, *Lease, error) {
	r := &Reader{st: st, keys: keys} // for its manifest and log reads
	a, err := r.readManifest(ctx, anchor{}, nil)
	if err != nil {
		return Office{}, nil, err
	}
	m := a.manifest()
	if m.Epoch == math.MaxUint64 {
		return Office{}, nil, fmt.Errorf("%w: epoch %d", ErrExhausted, m.Epoch)
	}
	lease, err := acquireLease(ctx, st, keys, c, m.Epoch+1)
	if err != nil {
		return Office{}, nil, err
	}
	// release gives up a candidacy whose swap definitely did not land. Its
	// error is dropped: the lease decides only who attempts a takeover.
	release := func(err error) (Office, *Lease, error) {
		_ = lease.Release(ctx)
		return Office{}, nil, err
	}

	o := Office{DB: m.DB, Epoch: m.Epoch + 1}
	for o.Nonce == [16]byte{} {
		if _, err := rand.Read(o.Nonce[:]); err != nil {
			return release(err)
		}
	}
	m.Epoch, m.Nonce = o.Epoch, o.Nonce
	swap, err := format.EncodeManifest(m)
	if err != nil {
		return release(err)
	}
	key := keys.Manifest()
	own, err := settle(ctx, nil,
		func() error {
			_, err := st.Replace(ctx, key, a.version, bytes.NewReader(swap))
			return err
		},
		func() (bool, error) {
			b, _, err := readObject(ctx, st, key, len(swap)+1)
			return bytes.Equal(b, swap), err
		})
	switch {
	case errors.Is(err, storage.ErrOutcomeUnknown):
		return Office{}, nil, fmt.Errorf("engine: epoch swap to %d unresolved: %w", o.Epoch, err)
	case err != nil:
		return release(fmt.Errorf("engine: epoch swap to %d failed: %w", o.Epoch, err))
	case !own:
		// The version the swap was conditioned on is gone for good. If an
		// earlier attempt landed and was replaced since, the lease was
		// taken since too, and the release, conditioned on this
		// candidacy's version, changes nothing.
		return release(fmt.Errorf("%w: the manifest at epoch %d was replaced first", ErrLost, o.Epoch-1))
	}

	held := func() error {
		cur, err := r.readManifest(ctx, anchor{m: m}, nil)
		switch {
		case err != nil:
			return err
		case !o.Holds(cur.m):
			return fmt.Errorf("%w: the manifest is at epoch %d, this candidacy's is %d", ErrLost, cur.m.Epoch, o.Epoch)
		}
		return nil
	}
	head, size := m.Txn, m.DBSize
	var occupied uint64 // the name the last bump attempt found another object at
	for {
		if head, size, err = walk(ctx, r, lease, m, o.Epoch, head, size); err != nil {
			var stale *staleError
			if !errors.As(err, &stale) {
				return Office{}, nil, err
			}
			// Only a newer manifest can cover a commit above ours.
			if err := held(); err != nil {
				return Office{}, nil, err
			}
			return Office{}, nil, fmt.Errorf("%w: %v above manifest %d, which this candidacy still holds", ErrCorrupt, stale, m.Txn) //nolint:errorlint // stale is not the cause
		}
		if head < occupied {
			return Office{}, nil, fmt.Errorf("%w: commit %d is occupied, but the walk ends at %d", ErrCorrupt, occupied, head)
		}
		switch head {
		case 0:
			return Office{}, nil, fmt.Errorf("%w: database %x has no commit", ErrCorrupt, o.DB)
		case math.MaxUint64:
			return Office{}, nil, fmt.Errorf("%w: transaction %d", ErrExhausted, head)
		}
		bump, err := encodeBump(o, m.PageSize, head+1, size)
		if err != nil {
			return Office{}, nil, err
		}
		bkey := keys.Commit(o.DB, head+1)
		occupied = head + 1
		own, err := settle(ctx, held,
			func() error {
				_, err := st.Create(ctx, bkey, bytes.NewReader(bump))
				return err
			},
			func() (bool, error) {
				b, _, err := readObject(ctx, st, bkey, len(bump)+1)
				return bytes.Equal(b, bump), err
			})
		if err != nil {
			return Office{}, nil, fmt.Errorf("engine: epoch bump at %d: %w", head+1, err)
		}
		if own {
			break
		}
		// Another object holds head+1 for good; walk on past it.
	}
	if err := held(); err != nil {
		return Office{}, nil, err
	}
	o.Head = head + 1
	// Without a renewal in the walk the lease goes unchecked: this candidacy
	// wrote it less than a third of its time to live ago, so another holder
	// took it only with a clock off by most of the time to live or under the
	// same holder name, and such a holder can take any rewrite of it again.
	if lease.taken {
		// Taken during the walk, typically by a candidacy that lost and
		// released it: take it back by acquireLease's rules, best effort.
		if l, err := acquireLease(ctx, st, keys, c, o.Epoch); err == nil {
			lease = l
		}
	}
	return o, lease, nil
}

// walk reads the log upward from the commit after head, which has database
// size size, until a name is absent, and returns the last commit's number
// and size. Every object is checked as a reader checks it; one that carries
// epoch, which only this candidacy's bump may, breaks invariant 1. Between
// reads it tries to keep lease renewed.
func walk(ctx context.Context, r *Reader, lease *Lease, m format.Manifest, epoch, head uint64, size uint32) (uint64, uint32, error) {
	for head < math.MaxUint64 {
		lease.keep(ctx)
		c, err := r.fetchCommit(ctx, m, head+1, size)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			return head, size, nil
		case err != nil:
			return 0, 0, err
		case c.h.Epoch == epoch:
			return 0, 0, fmt.Errorf("%w: commit %d already carries epoch %d", ErrCorrupt, head+1, epoch)
		}
		head, size = head+1, c.h.DBSize
	}
	return head, size, nil
}

func encodeBump(o Office, pageSize uint32, txn uint64, size uint32) ([]byte, error) {
	var b bytes.Buffer
	h := format.CommitHeader{DB: o.DB, PageSize: pageSize, Txn: txn, Epoch: o.Epoch, DBSize: size, EpochBump: true}
	if err := format.EncodeCommit(&b, h, nil, bytes.NewReader(nil)); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// settle sends one conditional write until it has a definite answer, as
// decision 6 says, and reports whether it took effect. before, if set, runs
// before every attempt and ends the operation with its error. send makes one
// attempt. mine reads the key after a precondition failure and reports
// whether it holds this write's bytes; a definite answer either way.
//
// Uncertainty is sticky: once an attempt had an unknown outcome, attempts
// with no effect resolve nothing, and running out of attempts returns that
// unknown error. A key absent after a precondition failure breaks the
// storage requirements of decision 3: none of these keys is ever deleted.
func settle(ctx context.Context, before, send func() error, mine func() (bool, error)) (bool, error) {
	var unknown, last error
	for range createAttempts {
		if before != nil {
			if err := before(); err != nil {
				if unknown != nil {
					err = fmt.Errorf("%w, after an attempt whose outcome is unknown: %w", err, unknown)
				}
				return false, err
			}
		}
		err := send()
		if err == nil {
			return true, nil
		}
		if errors.Is(err, storage.ErrPreconditionFailed) {
			own, rerr := mine()
			switch {
			case rerr == nil:
				return own, nil
			case errors.Is(rerr, storage.ErrNotFound):
				return false, fmt.Errorf("%w: absent after a precondition failure: %w", ErrCorrupt, rerr)
			}
			err = rerr
		}
		if errors.Is(err, storage.ErrOutcomeUnknown) && unknown == nil {
			unknown = err
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	if unknown != nil {
		last = unknown
	}
	return false, last
}
