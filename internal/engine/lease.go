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

	self bool // Holder is the holder that got this error
}

func (e *LeaseHeldError) Error() string {
	until := e.Expires.UTC().Format(time.RFC3339Nano)
	if e.self {
		return fmt.Sprintf("engine: the lease is held by another candidacy of this holder, %q, until %s", e.Holder, until)
	}
	return fmt.Sprintf("%v: %q until %s", ErrLeaseHeld, e.Holder, until)
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
	st   storage.Store
	keys format.Keys
	ttl  time.Duration
	now  func() time.Time

	l       format.Lease // the lease as this holder last wrote it
	version string       // its version token
	tried   int64        // when keep last tried to renew, in milliseconds
	taken   bool         // keep found the lease taken by another
}

// acquireLease takes the lease for c.Holder at epoch by take's rules.
func acquireLease(ctx context.Context, st storage.Store, keys format.Keys, c Candidate, epoch uint64) (*Lease, error) {
	if c.TTL <= 0 || c.Now == nil {
		return nil, errors.New("engine: a candidate needs a positive lease time to live and a clock")
	}
	l := &Lease{st: st, keys: keys, ttl: c.TTL, now: c.Now}
	if err := l.take(ctx, c.Holder, epoch); err != nil {
		return nil, err
	}
	return l, nil
}

// take writes the lease for holder at epoch: it creates it if absent, and
// otherwise replaces a lease of the same holder, an expired one or one that
// does not decode, conditioned on the version it read. An unexpired lease of
// another holder gives ErrLeaseHeld, and a lease of a newer format version
// an error; neither is overwritten. If the write fails, the handle keeps the
// version of this holder's own last write, so that a later Renew or Release
// can never replace the lease it read here.
func (l *Lease) take(ctx context.Context, holder string, epoch uint64) error {
	prev := l.version
	b, info, err := readObject(ctx, l.st, l.keys.Lease(), format.MaxLeaseSize+1)
	create := errors.Is(err, storage.ErrNotFound)
	if err != nil && !create {
		return err
	}
	if !create {
		cur, err := format.DecodeLease(b)
		switch {
		case errors.Is(err, format.ErrVersion):
			return fmt.Errorf("engine: lease: %w", err)
		case err != nil:
			// Not a lease of any version; it is replaced like an expired one.
		case cur.Holder != holder && !l.expired(cur):
			return l.heldError(cur, holder)
		}
		l.version = info.Version
	}
	if err := l.put(ctx, format.Lease{Holder: holder, Epoch: epoch, Renewed: l.stamp(l.l.Renewed)}, create); err != nil {
		l.version = prev
		return err
	}
	return nil
}

// Renew refreshes the lease's renewal time. ErrLeaseHeld means another
// holder has taken it: a candidacy that is under way, or one that has
// since lost. Renew never takes the lease back; Retake does. The error
// does not depose the holder; only the log and the manifest can.
func (l *Lease) Renew(ctx context.Context) error {
	next := l.l
	next.Renewed = l.stamp(l.l.Renewed)
	return l.put(ctx, next, false)
}

// Retake takes the lease back for this holder at its epoch, by the rules of
// a candidate's lease step: it writes the lease if the stored one is this
// holder's, or another holder's that is released or expired, and returns
// ErrLeaseHeld, writing nothing, if another holder's lease is live. A leader
// calls it when Renew reports ErrLeaseHeld and the lease reads as released
// or expired. The epoch it writes is the lease's own, never another.
//
// It first reads the manifest and returns ErrLost, writing nothing, unless
// the manifest still carries the lease's epoch: a deposed leader must not
// hold back the next takeover. That check is not a fence. The manifest can
// move between the read and the lease write, so a deposed leader may still
// take the lease once; it remains deposed, because only the log and the
// manifest decide who leads. Retake gives no right to write. Renew does not
// read the manifest, so a lease retaken in that window is kept by Renew,
// and every other holder and candidate is refused it, until the holder reads
// the manifest: the caller must check the manifest on its renewal cadence,
// not only when Renew fails.
func (l *Lease) Retake(ctx context.Context) error {
	a, err := (&Reader{st: l.st, keys: l.keys}).readManifest(ctx, anchor{}, nil)
	if err != nil {
		return err
	}
	// Epochs are never reused, so the manifest carries this lease's epoch only
	// while the office that holds this lease leads.
	if a.m.Epoch != l.l.Epoch {
		return fmt.Errorf("%w: the manifest is at epoch %d, this lease's is %d", ErrLost, a.m.Epoch, l.l.Epoch)
	}
	return l.take(ctx, l.l.Holder, l.l.Epoch)
}

// Release gives the lease up on a clean stop, by rewriting it with renewal
// time 0, which every other node reads as expired. ErrLeaseHeld means
// another holder had already taken it.
func (l *Lease) Release(ctx context.Context) error {
	next := l.l
	next.Renewed = 0
	return l.put(ctx, next, false)
}

// keep renews the lease, best effort, once a third of its time to live has
// passed since this holder last wrote it or last tried to. Its failure,
// a taken lease included, ends nothing: the lease decides only who attempts
// a takeover, and the manifest decides whether a candidacy goes on.
func (l *Lease) keep(ctx context.Context) {
	now := l.now().UnixMilli()
	if now-max(l.l.Renewed, l.tried) < l.ttl.Milliseconds()/3 {
		return
	}
	l.tried = now
	if errors.Is(l.Renew(ctx), ErrLeaseHeld) {
		l.taken = true
	}
}

// expired reports whether lease cur has expired by this node's clock.
func (l *Lease) expired(cur format.Lease) bool {
	age := l.now().UnixMilli() - cur.Renewed
	return age >= l.ttl.Milliseconds() || age < -l.ttl.Milliseconds()
}

// heldError is the error of finding lease cur, which holder did not write.
func (l *Lease) heldError(cur format.Lease, holder string) error {
	expires := time.UnixMilli(cur.Renewed).Add(l.ttl)
	if cur.Renewed == 0 {
		expires = l.now() // released: free to take now
	}
	return &LeaseHeldError{Holder: cur.Holder, Expires: expires, self: cur.Holder == holder}
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
					version, err = l.st.Create(ctx, l.keys.Lease(), bytes.NewReader(body))
				} else {
					version, err = l.st.Replace(ctx, l.keys.Lease(), l.version, bytes.NewReader(body))
				}
				return err
			},
			func() (bool, error) {
				b, info, err := readObject(ctx, l.st, l.keys.Lease(), format.MaxLeaseSize+1)
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
		return l.heldError(found, next.Holder)
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
