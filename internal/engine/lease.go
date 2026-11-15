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
var ErrLeaseHeld = errors.New("engine: the lease is held by another holder")

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
// that renewal, so clock skew between nodes moves only when takeovers start.
// Every write of the lease is conditional, so a holder never overwrites a
// lease it has not read.
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
// absent, and otherwise replaces a lease of the same holder or an expired
// one, conditioned on the version it read. An unexpired lease of another
// holder gives ErrLeaseHeld, and a lease that does not decode, which may be
// a newer version's, an error; neither is overwritten.
func acquireLease(ctx context.Context, st storage.Store, keys format.Keys, c Candidate, epoch uint64) (*Lease, error) {
	if c.TTL <= 0 || c.Now == nil {
		return nil, errors.New("engine: a candidate needs a positive lease time to live and a clock")
	}
	l := &Lease{st: st, key: keys.Lease(), ttl: c.TTL, now: c.Now}
	b, info, err := readObject(ctx, st, l.key)
	create := errors.Is(err, storage.ErrNotFound)
	if err != nil && !create {
		return nil, err
	}
	if !create {
		cur, err := format.DecodeLease(b)
		if err != nil {
			return nil, fmt.Errorf("engine: lease: %w", err)
		}
		if cur.Holder != c.Holder && l.now().UnixMilli()-cur.Renewed < l.ttl.Milliseconds() {
			return nil, fmt.Errorf("%w: %s, renewed at %d", ErrLeaseHeld, cur.Holder, cur.Renewed)
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

// stamp is the clock in milliseconds, kept above prev so that each of this
// holder's writes has bytes of its own.
func (l *Lease) stamp(prev int64) int64 {
	return max(l.now().UnixMilli(), prev+1, 0)
}

// put writes next, by create or by a replace conditioned on l.version, and
// resolves the outcome as decision 6 says: finding next's bytes on a
// read-back is success, finding anything else means another holder wrote
// the lease.
func (l *Lease) put(ctx context.Context, next format.Lease, create bool) error {
	body, err := format.EncodeLease(next)
	if err != nil {
		return err
	}
	var version string
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
			b, info, err := readObject(ctx, l.st, l.key)
			version = info.Version
			return bytes.Equal(b, body), err
		})
	switch {
	case err != nil:
		return fmt.Errorf("engine: lease write unresolved: %w", err)
	case !own:
		return fmt.Errorf("%w: the lease changed under %s", ErrLeaseHeld, next.Holder)
	}
	l.l, l.version = next, version
	return nil
}

// readObject reads the whole object at key with its version.
func readObject(ctx context.Context, st storage.Store, key string) ([]byte, storage.Info, error) {
	rc, info, err := st.Get(ctx, key)
	if err != nil {
		return nil, storage.Info{}, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, format.MaxManifestSize+1))
	if err != nil {
		return nil, storage.Info{}, err
	}
	return b, info, nil
}
