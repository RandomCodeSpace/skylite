package engine

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// ErrBehind means the follower did not observe a fresh snapshot at or
// above the requested transaction within its catch-up budget.
var ErrBehind = errors.New("engine: follower is behind")

const (
	followerPollInterval = 250 * time.Millisecond
	followerMaxWait      = 500 * time.Millisecond
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
// for the initial walk if the follower is still starting. cache may be nil.
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
	r, err := f.pin(ctx, minTxn)
	if err != nil {
		return nil, err
	}
	return openReadConn(ctx, r)
}

func (f *Follower) pin(ctx context.Context, minTxn uint64) (Reader, error) {
	wait, cancel := context.WithTimeout(ctx, followerMaxWait)
	defer cancel()
	for {
		f.mu.Lock()
		r, invalid, changed := f.current, f.invalid, f.changed
		f.mu.Unlock()
		switch {
		case ctx.Err() != nil:
			return Reader{}, ctx.Err()
		case f.ctx.Err() != nil:
			return Reader{}, f.ctx.Err()
		case wait.Err() != nil:
			return Reader{}, ErrBehind
		case invalid != nil:
			return Reader{}, invalid
		case r != nil && r.Txn() >= minTxn && time.Since(r.observedAt) < followerFreshness:
			return *r, nil
		}

		select {
		case <-wait.Done():
		case <-f.ctx.Done():
		case <-changed:
		}
	}
}
