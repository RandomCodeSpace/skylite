// Package office takes and keeps office for one database on this node: it
// runs candidacies until one succeeds, serves the database through an
// engine.Leader while the node holds office, and leaves office when the
// leader stops, the node is deposed or the caller shuts it down. It is the
// leader's lifecycle of record 0004 decisions 3, 6 and 9; it serves nothing
// itself.
package office

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// Defaults. The records fix no lease timing. The renewal period is a third
// of the time to live, the cadence at which a candidate renews during its
// walk, so a leader misses two renewals before its lease expires. The drain
// timeout is record 0005's deployment budget T at its defaults (decision 7):
// a statement time of 30 s, the engine's bound R of 20 s and a response
// deadline of 10 s.
const (
	DefaultTTL          = 60 * time.Second
	DefaultDrainTimeout = 60 * time.Second
	firstBackoff        = time.Second
	minWait             = 100 * time.Millisecond // a standby's shortest wait
)

// Reasons a loop stops leading, as State.Reason reports them.
var (
	// ErrDeposed means the manifest no longer carries this node's office:
	// another candidacy took it.
	ErrDeposed = errors.New("office: deposed by another candidacy")
	// ErrShutdown means the loop's context ended.
	ErrShutdown = errors.New("office: shut down")
)

// Config configures a Loop.
type Config struct {
	Store  storage.Store
	Keys   format.Keys
	Holder string // the lease's holder name; see NewHolder
	Dir    string // local directory for the leader's files
	Engine engine.Config

	TTL   time.Duration // lease time to live; DefaultTTL if 0
	Renew time.Duration // renewal period; TTL/3 if 0
	// Drain, if set, runs when the loop's context ends while it leads,
	// after State reports it not leading and before the leader closes. It
	// stops new work on the database and returns once the work already
	// running has ended or ctx is done. Closing the leader cancels every
	// Run still in progress, which can turn a commit in its upload into an
	// unknown outcome.
	Drain        func(ctx context.Context)
	DrainTimeout time.Duration    // bounds Drain; DefaultDrainTimeout if 0
	Now          func() time.Time // the lease clock; time.Now if nil
	Logger       *slog.Logger     // slog.Default() if nil
}

// State is a loop's leadership at one moment.
type State struct {
	// Leader serves the database while this node leads it, and is nil
	// otherwise: the database is ready on this node only while it is set.
	// A leader that has stopped (its Err is set) stays here until the
	// loop's next renewal finds it.
	Leader *engine.Leader
	// Reason is why the loop last stopped leading, nil if it never has.
	// It wraps engine.ErrStopped, ErrDeposed or ErrShutdown.
	Reason error
	// Changed is closed when the state changes.
	Changed <-chan struct{}
}

// Loop takes and keeps office for one database. Its methods are safe for
// concurrent use.
type Loop struct {
	cfg Config
	log *slog.Logger

	mu      sync.Mutex
	state   State
	changed chan struct{}
}

// NewHolder returns a holder name for this process: pod, then "_" and 16
// random hexadecimal digits, so that two processes never share one. pod is
// 1 to 111 of A-Z a-z 0-9 . _ -; a Kubernetes pod name of that length fits.
func NewHolder(pod string) (string, error) {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	h := pod + "_" + hex.EncodeToString(id[:])
	if _, err := format.EncodeLease(format.Lease{Holder: h, Epoch: 1}); err != nil || len(pod) == 0 {
		return "", fmt.Errorf("office: pod name %q does not fit a lease holder", pod)
	}
	return h, nil
}

// New returns a loop for cfg, with defaults filled in. Run starts it.
func New(cfg Config) (*Loop, error) {
	if cfg.TTL == 0 {
		cfg.TTL = DefaultTTL
	}
	if cfg.Renew == 0 {
		cfg.Renew = cfg.TTL / 3
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	switch {
	case cfg.Store == nil || cfg.Dir == "":
		return nil, errors.New("office: Config needs a store and a directory")
	case cfg.TTL < 0 || cfg.Renew <= 0 || cfg.Renew >= cfg.TTL || cfg.DrainTimeout < 0:
		return nil, errors.New("office: Config needs a renewal period below the lease's time to live")
	}
	if _, err := format.EncodeLease(format.Lease{Holder: cfg.Holder, Epoch: 1}); err != nil {
		return nil, fmt.Errorf("office: holder: %w", err)
	}
	return &Loop{
		cfg:     cfg,
		log:     cfg.Logger.With("holder", cfg.Holder),
		changed: make(chan struct{}),
	}, nil
}

// State returns the loop's current state.
func (l *Loop) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.state
	s.Changed = l.changed
	return s
}

func (l *Loop) set(leader *engine.Leader, reason error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state.Leader = leader
	if reason != nil {
		l.state.Reason = reason
	}
	close(l.changed)
	l.changed = make(chan struct{})
}

// Run runs candidacies until one takes office, keeps office, and starts
// again whenever it leaves office, until ctx ends. A standby never takes
// over a live lease: it tries again once a renewal period has passed or
// the lease can have expired, whichever is sooner. A leader, every renewal
// period, reads the manifest, then:
//   - leaves office if its leader has stopped, closing it and writing
//     nothing more, its lease included: a node that fail-stops issues no
//     further write for the database (record 0004 decision 6);
//   - leaves office, without releasing the lease, if the manifest no longer
//     carries its office;
//   - renews the lease, and takes it back with Retake if another holder's
//     lease there reads as released or expired.
//
// When ctx ends, a leader stops reporting itself in State, runs Drain, closes
// the leader and then releases the lease, unless the leader had stopped.
// Run returns the error of that release, if any.
func (l *Loop) Run(ctx context.Context) error {
	backoff := min(firstBackoff, l.cfg.Renew) // doubles to Renew while candidacies fail
	for {
		office, lease, wait := l.candidacy(ctx)
		if lease == nil {
			if wait == 0 {
				wait = backoff
				backoff = min(2*backoff, l.cfg.Renew)
			}
			if !sleep(ctx, wait) {
				return nil
			}
			continue
		}
		backoff = min(firstBackoff, l.cfg.Renew)
		leader, err := engine.Lead(ctx, l.cfg.Store, l.cfg.Keys, l.cfg.Dir, office, l.cfg.Engine)
		if err != nil {
			l.log.Error("office: lead failed", "epoch", office.Epoch, "err", err)
			_ = l.release(ctx, lease)
			if !sleep(ctx, l.cfg.Renew) {
				return nil
			}
			continue
		}
		l.log.Info("office: leading", "epoch", office.Epoch, "head", office.Head)
		l.set(leader, nil)
		if done, err := l.lead(ctx, office, lease, leader); done {
			return err
		}
		if !sleep(ctx, backoff) {
			return nil
		}
	}
}

// candidacy runs one takeover. Without office it returns a nil lease and
// how long to wait before the next attempt, 0 for the caller's backoff.
func (l *Loop) candidacy(ctx context.Context) (engine.Office, *engine.Lease, time.Duration) {
	c := engine.Candidate{Holder: l.cfg.Holder, TTL: l.cfg.TTL, Now: l.cfg.Now}
	office, lease, err := engine.Takeover(ctx, l.cfg.Store, l.cfg.Keys, c)
	var held *engine.LeaseHeldError
	switch {
	case err == nil && ctx.Err() != nil:
		_ = l.release(ctx, lease)
		return engine.Office{}, nil, 0
	case err == nil:
		return office, lease, 0
	case ctx.Err() != nil:
		return engine.Office{}, nil, 0
	case errors.As(err, &held):
		l.log.Debug("office: standby", "leader", held.Holder, "expires", held.Expires)
		// A lease released early is free at once, so do not sleep past a
		// renewal period on an estimate of its expiry.
		return engine.Office{}, nil, max(min(held.Expires.Sub(l.cfg.Now()), l.cfg.Renew), minWait)
	}
	l.log.Warn("office: candidacy failed", "err", err)
	return engine.Office{}, nil, 0
}

// lead keeps office until the leader stops, the node is deposed or ctx
// ends. It reports done when ctx ended, with the release's error.
func (l *Loop) lead(ctx context.Context, office engine.Office, lease *engine.Lease, leader *engine.Leader) (bool, error) {
	t := time.NewTicker(l.cfg.Renew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return true, l.shutdown(ctx, lease, leader)
		case <-t.C:
		}
		m, err := l.manifest(ctx)
		// Checked after the read, which takes time, and right before the
		// renewal: a leader that has stopped must not renew.
		if stopped := leader.Err(); stopped != nil {
			l.stepDown(leader, stopped)
			return false, nil
		}
		if err != nil {
			// The renewal waits for a manifest check: a lease retaken
			// while deposed must not be kept (Lease.Retake).
			l.log.Warn("office: manifest read failed", "epoch", office.Epoch, "err", err)
			continue
		}
		if !office.Holds(m) {
			l.stepDown(leader, fmt.Errorf("%w: the manifest is at epoch %d, this office's is %d", ErrDeposed, m.Epoch, office.Epoch))
			return false, nil
		}
		err = lease.Renew(ctx)
		var held *engine.LeaseHeldError
		switch {
		case err == nil:
			continue
		case errors.As(err, &held) && !held.Expires.After(l.cfg.Now()):
			err = lease.Retake(ctx)
		}
		switch {
		case err == nil:
			l.log.Info("office: lease taken back", "epoch", office.Epoch)
		case errors.Is(err, engine.ErrLost):
			l.stepDown(leader, fmt.Errorf("%w: %w", ErrDeposed, err))
			return false, nil
		case errors.As(err, &held):
			// A candidacy is under way; only the manifest can depose us.
			l.log.Info("office: lease held by a candidate", "candidate", held.Holder, "epoch", office.Epoch)
		case ctx.Err() == nil:
			l.log.Warn("office: lease renewal failed", "epoch", office.Epoch, "err", err)
		}
	}
}

// stepDown leaves office without a lease write.
func (l *Loop) stepDown(leader *engine.Leader, reason error) {
	l.log.Warn("office: leaving office", "reason", reason)
	l.set(nil, reason)
	_ = leader.Close()
}

// shutdown drains, closes the leader and releases the lease.
func (l *Loop) shutdown(ctx context.Context, lease *engine.Lease, leader *engine.Leader) error {
	l.set(nil, ErrShutdown)
	if l.cfg.Drain != nil {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.cfg.DrainTimeout)
		l.cfg.Drain(dctx)
		cancel()
	}
	stopped := leader.Err()
	_ = leader.Close()
	if stopped != nil {
		l.log.Warn("office: shut down after the leader stopped; lease left to expire", "reason", stopped)
		return nil
	}
	return l.release(ctx, lease)
}

// release gives the lease up, best effort and within its time to live,
// after which it expires anyway.
func (l *Loop) release(ctx context.Context, lease *engine.Lease) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.cfg.TTL)
	defer cancel()
	err := lease.Release(rctx)
	if err != nil {
		l.log.Warn("office: lease release failed", "err", err)
	}
	return err
}

// manifest reads the database's manifest.
func (l *Loop) manifest(ctx context.Context) (format.Manifest, error) {
	rc, _, err := l.cfg.Store.Get(ctx, l.cfg.Keys.Manifest())
	if err != nil {
		return format.Manifest{}, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, format.MaxManifestSize+1))
	if err != nil {
		return format.Manifest{}, err
	}
	return format.DecodeManifest(b, format.DatabaseID{})
}

// sleep waits d, and reports false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
