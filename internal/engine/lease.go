package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// ErrLeaseHeld means another holder holds the lease: its last renewal is
// younger than the lease's time to live, or it replaced this holder's lease.
// Every error that matches it is a *LeaseHeldError.
var ErrLeaseHeld = errors.New("engine: the lease is held by another holder")

// LeaseHeldError is the error of a lease that another holder, or an earlier
// candidacy of the same holder, holds. It matches ErrLeaseHeld.
type LeaseHeldError struct {
	// Holder is the holder the lease names; empty if it does not decode.
	Holder string
	// Expires is when the lease expires by this node's clock unless it is
	// renewed: a candidate may try again then.
	Expires time.Time
}

func (e *LeaseHeldError) Error() string {
	return fmt.Sprintf("%v: %q until %s", ErrLeaseHeld, e.Holder, e.Expires.UTC().Format(time.RFC3339Nano))
}

// Is makes a LeaseHeldError match ErrLeaseHeld.
func (e *LeaseHeldError) Is(target error) bool { return target == ErrLeaseHeld }

// Lease is a node's hold on a database's lease object, the one at
// format.Keys.Lease. Decisions 3 and 9 of the storage engine record give the
// lease one purpose: it decides which node attempts a takeover, and when, so
// that takeovers make progress. It is never a fence. Holding it gives no
// right to write, losing it takes none away, and no safety argument reads
// it; a node in office is stopped only by the conditional writes of the log
// and the manifest (see Office.Recognise and Office.Holds).
//
// A lease names its holder, the epoch the holder leads or intends to lead at,
// and the time of its last renewal by the holder's clock. Another node
// treats it as expired once its own clock is at least the time to live past
// that renewal, or more than the time to live before it, so clock skew
// between nodes moves only when takeovers start. Every write of the lease is
// conditional, so a holder never overwrites a lease it has not read. A write
// that fails its precondition and finds a lease with the writer's own holder
// and epoch finds the writer's own earlier write, whose outcome was unknown:
// holder names are unique among running processes, and each candidacy has
// its own epoch. The writer adopts its version and writes once more.
//
// A node that leaves office cleanly calls Release. A node that fail-stops
// (decision 6) does neither Renew nor Release: it issues no further write
// for the database, and its lease expires. A Lease is not safe for concurrent
// use.
type Lease struct {
	st  storage.Store
	key string
	ttl time.Duration
	now func() time.Time

	l       format.Lease // the lease as this holder last wrote it
	version string       // its version token
}

// acquireLease takes the lease for c.Holder at epoch: it creates it if
// absent, and otherwise replaces a lease of the same holder, an expired one
// or one that does not decode, conditioned on the version it read. An
// unexpired lease of another holder gives ErrLeaseHeld, and a lease of a
// newer format version an error; neither is overwritten.
func acquireLease(ctx context.Context, st storage.Store, keys format.Keys, c Candidate, epoch uint64) (*Lease, error) {
	if c.TTL <= 0 || c.Now == nil {
		return nil, errors.New("engine: a candidate needs a positive lease time to live and a clock")
	}
	l := &Lease{st: st, key: keys.Lease(), ttl: c.TTL, now: c.Now}
	b, info, err := readObject(ctx, st, l.key, format.MaxLeaseSize+1)
	create := errors.Is(err, storage.ErrNotFound)
	if err != nil && !create {
		return nil, err
	}
	if !create {
		cur, err := format.DecodeLease(b)
		switch {
		case errors.Is(err, format.ErrVersion):
			return nil, fmt.Errorf("engine: lease: %w", err)
		case err != nil:
			// Not a lease of any version; it is replaced like an expired one.
		case cur.Holder != c.Holder && !l.expired(cur):
			return nil, l.heldError(cur)
		}
		l.version = info.Version
	}
	if err := l.put(ctx, format.Lease{Holder: c.Holder, Epoch: epoch, Renewed: l.stamp(0)}, create); err != nil {
		return nil, err
	}
	return l, nil
}

// Renew refreshes the lease's renewal time. ErrLeaseHeld means another
// holder has taken it; the holder should then leave office, because a
// takeover is under way, although only the log and the manifest can depose
// it.
func (l *Lease) Renew(ctx context.Context) error {
	next := l.l
	next.Renewed = l.stamp(l.l.Renewed)
	return l.put(ctx, next, false)
}

// Release gives the lease up on a clean stop, by rewriting it with renewal
// time 0, which every other node reads as expired. ErrLeaseHeld means
// another holder had already taken it.
func (l *Lease) Release(ctx context.Context) error {
	next := l.l
	next.Renewed = 0
	return l.put(ctx, next, false)
}

// keep renews the lease once a third of its time to live has passed since
// this holder last wrote it, and returns only an ErrLeaseHeld from it: any
// other failure leaves the lease to the next renewal, because the lease
// decides only who attempts a takeover.
func (l *Lease) keep(ctx context.Context) error {
	if l.now().UnixMilli()-l.l.Renewed < l.ttl.Milliseconds()/3 {
		return nil
	}
	if err := l.Renew(ctx); errors.Is(err, ErrLeaseHeld) {
		return err
	}
	return nil
}

// expired reports whether lease cur has expired by this node's clock.
func (l *Lease) expired(cur format.Lease) bool {
	age := l.now().UnixMilli() - cur.Renewed
	return age >= l.ttl.Milliseconds() || age < -l.ttl.Milliseconds()
}

func (l *Lease) heldError(cur format.Lease) error {
	return &LeaseHeldError{Holder: cur.Holder, Expires: time.UnixMilli(cur.Renewed).Add(l.ttl)}
}

// stamp is the clock in milliseconds, kept above prev so that each of this
// holder's writes has bytes of its own.
func (l *Lease) stamp(prev int64) int64 {
	return max(l.now().UnixMilli(), prev+1, 0)
}

// put writes next, by create or by a replace conditioned on l.version, and
// resolves the outcome as decision 6 says: finding next's bytes on a
// read-back is success. Finding this holder's own earlier write, the same
// holder at the same epoch, adopts its version for one more write; finding
// anything else means another holder wrote the lease.
func (l *Lease) put(ctx context.Context, next format.Lease, create bool) error {
	body, err := format.EncodeLease(next)
	if err != nil {
		return err
	}
	for adopted := false; ; adopted = true {
		var version string
		var found format.Lease
		var ferr error
		own, err := settle(ctx, nil,
			func() (err error) {
				if create {
					version, err = l.st.Create(ctx, l.key, bytes.NewReader(body))
				} else {
					version, err = l.st.Replace(ctx, l.key, l.version, bytes.NewReader(body))
				}
				return err
			},
			func() (bool, error) {
				b, info, err := readObject(ctx, l.st, l.key, format.MaxLeaseSize+1)
				version = info.Version
				found, ferr = format.DecodeLease(b)
				return bytes.Equal(b, body), err
			})
		switch {
		case err != nil:
			return fmt.Errorf("engine: lease write unresolved: %w", err)
		case own:
			l.l, l.version = next, version
			return nil
		case ferr != nil:
			return &LeaseHeldError{Expires: l.now()}
		case !adopted && found.Holder == next.Holder && found.Epoch == next.Epoch:
			l.version, create = version, false
			continue
		}
		return l.heldError(found)
	}
}

// readObject reads at most n bytes of the object at key, with its version.
func readObject(ctx context.Context, st storage.Store, key string, n int) ([]byte, storage.Info, error) {
	rc, info, err := st.Get(ctx, key)
	if err != nil {
		return nil, storage.Info{}, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, int64(n)))
	if err != nil {
		return nil, storage.Info{}, err
	}
	return b, info, nil
}
