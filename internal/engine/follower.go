package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// ErrBehind means the follower did not observe a fresh snapshot at or
// above the requested transaction within its catch-up budget.
var ErrBehind = errors.New("engine: follower is behind")

// ErrStale means the follower has no fresh observation of the log's end.
var ErrStale = fmt.Errorf("%w: no fresh snapshot", ErrBehind)

// FollowerMaxWait bounds the time spent waiting for a fresh snapshot.
const FollowerMaxWait = 500 * time.Millisecond

const (
	followerPollInterval = 250 * time.Millisecond
	followerFreshness    = 2 * time.Second
)

// Follower tails the durable history of one database. Its published
// Reader and index are immutable and may be shared by any number of pins.
type Follower struct {
	ctx  context.Context
	done chan struct{}

	mu      sync.Mutex
	current *Reader
	invalid error
	changed chan struct{}
}

// Follow starts polling the database under keys' prefix. Before closing
// the store, cancel ctx, wait on Done and close its ReadConns. Open waits
// at most 500 ms for the initial walk, then returns ErrBehind. cache may be
// nil.
func Follow(ctx context.Context, st storage.Store, keys format.Keys, cache *Cache) *Follower {
	f := newFollower(ctx)
	go f.follow(st, keys, cache)
	return f
}

func newFollower(ctx context.Context) *Follower {
	return &Follower{ctx: ctx, done: make(chan struct{}), changed: make(chan struct{})}
}

// Done closes when the follower stops polling.
func (f *Follower) Done() <-chan struct{} { return f.done }

// Fresh reports whether the follower can currently admit a snapshot.
func (f *Follower) Fresh() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ctx.Err() == nil && f.invalid == nil && fresh(f.current)
}

func fresh(r *Reader) bool {
	return r != nil && time.Since(r.observedAt) < followerFreshness
}

func (f *Follower) follow(st storage.Store, keys format.Keys, cache *Cache) {
	defer close(f.done)
	delay := followerPollInterval
	for f.ctx.Err() == nil {
		err := f.poll(st, keys, cache)
		if err == nil {
			delay = followerPollInterval
		}
		// Jitter by ±20% to spread requests from followers starting together.
		jitter := time.Duration(rand.Int64N(int64(delay / 5))) //nolint:gosec // scheduling jitter
		t := time.NewTimer(delay - delay/5 + 2*jitter)
		select {
		case <-f.ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		if err != nil {
			delay = min(delay*2, followerFreshness)
		}
	}
}

// poll is called only by the tailer. Only successful walks replace the
// published generation. Validation failures immediately suspend admissions.
func (f *Follower) poll(st storage.Store, keys format.Keys, cache *Cache) error {
	var next *Reader
	var err error
	if f.current == nil {
		next, err = Open(f.ctx, st, keys, cache, 0)
	} else {
		r := *f.current
		err = r.advance(f.ctx, true)
		next = &r
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case err == nil:
		f.current, f.invalid = next, nil
	case errors.Is(err, ErrCorrupt), errors.Is(err, ErrLimit),
		errors.Is(err, format.ErrChecksum), errors.Is(err, format.ErrMisplaced),
		errors.Is(err, format.ErrInvalid), errors.Is(err, format.ErrNotFormat), errors.Is(err, format.ErrVersion),
		f.current != nil && errors.Is(err, storage.ErrNotFound):
		f.invalid = err
	default:
		return err
	}
	close(f.changed)
	f.changed = make(chan struct{})
	return err
}

// Open pins a fresh snapshot at or above minTxn and opens a read-only
// SQLite connection on it. It waits at most 500 ms for the pin, subject to
// ctx. Each connection stays on its transaction until closed. Canceling
// the follower stops new admissions, and existing connections keep their
// pins.
func (f *Follower) Open(ctx context.Context, minTxn uint64) (*ReadConn, error) {
	p, err := f.Pin(ctx, minTxn)
	if err != nil {
		return nil, err
	}
	return p.Open(ctx)
}

// ReadPin is one immutable follower snapshot. It retains its transaction
// even if compaction later makes that transaction unreadable.
type ReadPin struct{ reader Reader }

// Txn returns the pin's durable transaction number.
func (p *ReadPin) Txn() uint64 { return p.reader.Txn() }

// Open opens a read-only SQLite connection on this pin, subject to ctx.
func (p *ReadPin) Open(ctx context.Context) (*ReadConn, error) {
	return openReadConn(ctx, p.reader)
}

// Pin waits for a fresh snapshot at or above minTxn, subject to ctx and
// FollowerMaxWait. Opening SQLite on the pin can use a separate budget.
func (f *Follower) Pin(ctx context.Context, minTxn uint64) (*ReadPin, error) {
	r, err := f.pin(ctx, minTxn)
	if err != nil {
		return nil, err
	}
	return &ReadPin{reader: r}, nil
}

func (f *Follower) pin(ctx context.Context, minTxn uint64) (Reader, error) {
	wait, cancel := context.WithTimeout(ctx, FollowerMaxWait)
	defer cancel()
	for {
		f.mu.Lock()
		r, invalid, changed := f.current, f.invalid, f.changed
		f.mu.Unlock()
		// A snapshot that already meets minTxn is returned even under an
		// expired ctx, so that the caller reports its own deadline.
		switch {
		case f.ctx.Err() != nil:
			return Reader{}, f.ctx.Err()
		case invalid != nil:
			return Reader{}, invalid
		case fresh(r) && r.Txn() >= minTxn:
			return *r, nil
		case ctx.Err() != nil:
			return Reader{}, ctx.Err()
		case wait.Err() != nil:
			if !fresh(r) {
				return Reader{}, ErrStale
			}
			return Reader{}, ErrBehind
		}

		select {
		case <-wait.Done():
		case <-f.ctx.Done():
		case <-changed:
		}
	}
}
