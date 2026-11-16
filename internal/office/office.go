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
	maxPoll             = 5 * time.Second        // a standby's longest wait between reads of the lease
	stopCheck           = time.Second            // how often a leader checks whether its leader has stopped
)

// Reasons a loop stops leading, as State.Reason reports them.
var (
	// ErrDeposed means the manifest no longer carries this node's office:
	// another candidacy took it.
	ErrDeposed = errors.New("office: deposed by another candidacy")
	// ErrUnchecked means the loop could not read the manifest for a time to
	// live, so it can no longer tell that it holds office: its lease has
	// expired, and a standby may have taken office.
	ErrUnchecked = errors.New("office: the manifest could not be read for a time to live")
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

	TTL time.Duration // lease time to live; DefaultTTL if 0
	// Renew is the renewal period, TTL/3 if 0. It is at most TTL/3, so that
	// a lease outlives one renewal that hangs: that renewal costs two
	// periods, its timeout and the wait for the next tick.
	Renew time.Duration
	// Drain, if set, runs when the loop's context ends while it leads,
	// after State reports it not leading and before the leader closes. It
	// stops new work on the database and returns once the work already
	// running has ended or ctx is done. Closing the leader cancels every
	// Run still in progress, which can turn a commit in its upload into an
	// unknown outcome.
	Drain func(ctx context.Context)
	// DrainTimeout bounds Drain, DefaultDrainTimeout if 0, by ending its
	// context: the drain is bounded only if Drain honours that context.
	DrainTimeout time.Duration
	Now          func() time.Time // the lease clock; time.Now if nil
	Logger       *slog.Logger     // slog.Default() if nil
}

// State is a loop's leadership at one moment.
type State struct {
	// Leader serves the database while this node leads it, and is nil
	// otherwise: the database is ready on this node only while it is set.
	// A leader that has stopped (its Err is set) stays here until the
	// loop notices, within about a second.
	Leader *engine.Leader
	// Reason is why the loop last stopped leading, or last failed to start
	// a leader after taking office; nil if neither has happened. It wraps
	// engine.ErrStopped, ErrDeposed, ErrUnchecked or ErrShutdown, or is the
	// error of engine.Lead.
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

// term is one stay in office.
type term struct {
	office  engine.Office
	lease   *engine.Lease
	checked time.Time // when the manifest was last seen to carry office
}

// NewHolder returns a holder name for this process: pod, cut to its first
// 111 characters, then "_" and 16 random hexadecimal digits, so that two
// processes never share one even when their pod names are cut. pod is 1 or
// more of A-Z a-z 0-9 . _ -, as a Kubernetes pod name is.
func NewHolder(pod string) (string, error) {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	h := pod[:min(len(pod), format.MaxHolderLen-1-2*len(id))] + "_" + hex.EncodeToString(id[:])
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
	case cfg.TTL < 0 || cfg.Renew <= 0 || cfg.Renew > cfg.TTL/3:
		return nil, errors.New("office: Config needs a positive renewal period of at most a third of the lease's time to live")
	case cfg.DrainTimeout < 0:
		return nil, errors.New("office: Config needs a drain timeout of 0 or more")
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
// again whenever it leaves office, until ctx ends. After a failed attempt,
// and after leaving office, it waits a backoff that doubles from a second,
// or the renewal period if that is shorter, up to the renewal period, and
// starts from there again once a leader is in office. A standby never
// takes over a live lease: it reads the lease, and only the lease, at most
// min(renewal period, 5 s) apart and under a timeout of one renewal period,
// and runs a candidacy once the lease reads as released or expired, or
// cannot be read.
//
// A node in office starts its leader with engine.Lead, which walks the log
// tail and can take longer than a time to live, and keeps renewing while it
// runs. Every renewal period, under a timeout of one renewal period, it
// reads the manifest, then:
//   - leaves office if its leader has stopped, closing it without a lease
//     write: a node that fail-stops issues no further write for the
//     database (record 0004 decision 6). That is best effort: a leader that
//     stops between this check and the lease write lets one renewal, or
//     the release on shutdown, out at its old epoch. That is harmless: the
//     lease is never a fence, and the write is no write of data;
//   - leaves office, without releasing the lease, if the manifest no longer
//     carries its office, or if it has not read the manifest for a time to
//     live (ErrUnchecked). A lease it retook as it was deposed is left to
//     expire, as a crashed leader's would: only the log and the manifest
//     decide who leads;
//   - renews the lease, and calls Retake if another holder holds it, which
//     takes it back only if it reads as released or expired.
//
// It also checks every second whether its leader has stopped, and ends a
// renewal whose read or write hangs once it has. If Lead fails, or the
// office is lost while it runs, the loop leaves office, closing any leader
// Lead returns; it releases the lease after a failed Lead.
//
// When ctx ends, a leader stops reporting itself in State, runs Drain, closes
// the leader and then releases the lease, unless the leader had stopped.
// Run returns the error of the release it makes when ctx ends with the
// lease in hand: in that shutdown, after a Lead that failed, or after a
// candidacy that took office as ctx ended. It returns nil otherwise.
func (l *Loop) Run(ctx context.Context) error {
	first := min(firstBackoff, l.cfg.Renew)
	backoff := first
	pause := func() bool {
		ok := sleep(ctx, backoff)
		backoff = min(2*backoff, l.cfg.Renew)
		return ok
	}
	for {
		t, held := l.candidacy(ctx)
		switch {
		case t != nil && ctx.Err() != nil:
			return l.release(ctx, t.lease)
		case ctx.Err() != nil:
			return nil
		case held != nil:
			if !l.standby(ctx, held.Expires) {
				return nil
			}
			continue
		case t == nil:
			if !pause() {
				return nil
			}
			continue
		}
		// One ticker for the whole term, so that a renewal is due a period
		// after the last one wherever Lead's end falls between them.
		renewal := time.NewTicker(l.cfg.Renew)
		leader, err := l.start(ctx, t, renewal.C)
		if err != nil {
			renewal.Stop()
			var rerr error
			if !errors.Is(err, ErrDeposed) && !errors.Is(err, ErrUnchecked) {
				rerr = l.release(ctx, t.lease)
			}
			l.set(nil, err)
			if ctx.Err() != nil {
				return rerr
			}
			if !pause() {
				return nil
			}
			continue
		}
		backoff = first
		l.log.Info("office: leading", "epoch", t.office.Epoch, "head", t.office.Head)
		l.set(leader, nil)
		done, err := l.lead(ctx, t, leader, renewal.C)
		renewal.Stop()
		if done {
			return err
		}
		if !pause() {
			return nil
		}
	}
}

// candidacy runs one takeover. It returns the term it starts, or the error
// of a lease another holder holds, or neither if it failed otherwise.
func (l *Loop) candidacy(ctx context.Context) (*term, *engine.LeaseHeldError) {
	c := engine.Candidate{Holder: l.cfg.Holder, TTL: l.cfg.TTL, Now: l.cfg.Now}
	office, lease, err := engine.Takeover(ctx, l.cfg.Store, l.cfg.Keys, c)
	var held *engine.LeaseHeldError
	switch {
	case err == nil:
		return &term{office: office, lease: lease, checked: time.Now()}, nil
	case ctx.Err() != nil:
		return nil, nil
	case errors.As(err, &held):
		l.log.Debug("office: standby", "leader", held.Holder, "expires", held.Expires)
		return nil, held
	}
	l.log.Warn("office: candidacy failed", "err", err)
	return nil, nil
}

// standby waits while another holder's lease, due to expire at expires, is
// live. It reads the lease at most a poll period apart, so that a release
// is seen within one, and reports false if ctx ended.
func (l *Loop) standby(ctx context.Context, expires time.Time) bool {
	poll := min(l.cfg.Renew, maxPoll)
	for {
		if !sleep(ctx, max(min(expires.Sub(l.cfg.Now()), poll), minWait)) {
			return false
		}
		now := l.cfg.Now()
		if !now.Before(expires) {
			return true
		}
		rctx, cancel := context.WithTimeout(ctx, l.cfg.Renew)
		b, err := l.read(rctx, l.cfg.Keys.Lease(), format.MaxLeaseSize+1)
		cancel()
		if err != nil {
			return true // the candidacy reports it
		}
		cur, err := format.DecodeLease(b)
		renewed := time.UnixMilli(cur.Renewed)
		// Free as engine.Lease judges it: released, or stamped more than a
		// time to live ahead of this clock.
		if err != nil || cur.Renewed == 0 || renewed.After(now.Add(l.cfg.TTL)) {
			return true
		}
		expires = renewed.Add(l.cfg.TTL)
	}
}

// lead is engine.Lead; tests replace it.
var lead = engine.Lead

// start runs engine.Lead for t's office and renews on each tick of renewal
// while it runs. It returns the leader, or why there is none: Lead's error,
// or the reason a renewal found to leave office, after closing any leader
// Lead returned.
func (l *Loop) start(ctx context.Context, t *term, renewal <-chan time.Time) (*engine.Leader, error) {
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		leader *engine.Leader
		err    error
	}
	done := make(chan result, 1)
	go func() {
		leader, err := lead(lctx, l.cfg.Store, l.cfg.Keys, l.cfg.Dir, t.office, l.cfg.Engine)
		done <- result{leader, err}
	}()
	var lost error
	for {
		select {
		case <-renewal:
			if lost == nil {
				if lost = l.renew(ctx, t, nil); lost != nil {
					cancel()
				}
			}
		case r := <-done:
			switch {
			case lost != nil:
				if r.leader != nil {
					_ = r.leader.Close()
				}
				l.log.Warn("office: leaving office", "reason", lost)
				return nil, lost
			case r.err != nil:
				err := fmt.Errorf("office: lead at epoch %d: %w", t.office.Epoch, r.err)
				if ctx.Err() != nil {
					l.log.Info("office: lead ended by shutdown", "err", err)
				} else {
					l.log.Error("office: lead failed", "err", err)
				}
				return nil, err
			}
			return r.leader, nil
		}
	}
}

// lead keeps office until the leader stops, the node is deposed or ctx
// ends, renewing on each tick of renewal. It reports done when ctx ended,
// with the release's error.
func (l *Loop) lead(ctx context.Context, t *term, leader *engine.Leader, renewal <-chan time.Time) (bool, error) {
	check := time.NewTicker(stopCheck)
	defer check.Stop()
	for {
		var reason error
		select {
		case <-ctx.Done():
			return true, l.shutdown(ctx, t.lease, leader)
		case <-check.C:
			reason = leader.Err()
		case <-renewal:
			reason = l.renew(ctx, t, leader)
		}
		if reason != nil {
			l.stepDown(leader, reason)
			return false, nil
		}
	}
}

// renew runs one renewal of t: it reads the manifest and, unless leader (nil
// while Lead runs) has stopped, renews the lease or takes it back. It
// returns why the node must leave office, nil while it keeps it.
func (l *Loop) renew(ctx context.Context, t *term, leader *engine.Leader) error {
	rctx, cancel := context.WithTimeout(ctx, l.cfg.Renew)
	defer cancel()
	if leader != nil {
		// A leader that stops ends a renewal that hangs, so that it leaves
		// State within about a second.
		go func() {
			check := time.NewTicker(stopCheck)
			defer check.Stop()
			for {
				select {
				case <-rctx.Done():
					return
				case <-check.C:
					if leader.Err() != nil {
						cancel()
						return
					}
				}
			}
		}()
	}
	// shortcut: reads the whole manifest every renewal; read it only when its version changed once the store can tell.
	m, err := l.manifest(rctx)
	// Checked after the read, which takes time, and right before the
	// renewal: a leader that has stopped must not renew.
	if leader != nil {
		if stopped := leader.Err(); stopped != nil {
			return stopped
		}
	}
	if err != nil {
		if since := time.Since(t.checked); since >= l.cfg.TTL {
			return fmt.Errorf("%w: last read %v ago: %w", ErrUnchecked, since.Round(time.Millisecond), err)
		}
		// The renewal waits for a manifest check: a lease retaken while
		// deposed must not be kept (Lease.Retake).
		l.log.Warn("office: manifest read failed", "epoch", t.office.Epoch, "err", err)
		return nil
	}
	t.checked = time.Now()
	if !t.office.Holds(m) {
		return fmt.Errorf("%w: the manifest is at epoch %d, this office's is %d", ErrDeposed, m.Epoch, t.office.Epoch)
	}
	err = t.lease.Renew(rctx)
	var held *engine.LeaseHeldError
	if errors.As(err, &held) {
		// Retake judges the lease itself, so a lease stamped far ahead by a
		// skewed clock is taken back too; a live one it leaves alone.
		if err = t.lease.Retake(rctx); err == nil {
			l.log.Info("office: lease taken back", "epoch", t.office.Epoch)
		}
	}
	switch {
	case err == nil:
	case errors.Is(err, engine.ErrLost):
		return fmt.Errorf("%w: %w", ErrDeposed, err)
	case errors.As(err, &held):
		// A candidacy is under way; only the manifest can depose us.
		l.log.Info("office: lease held by a candidate", "candidate", held.Holder, "epoch", t.office.Epoch)
	case ctx.Err() == nil:
		l.log.Warn("office: lease renewal failed", "epoch", t.office.Epoch, "err", err)
	}
	return nil
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

// manifest reads the database's manifest, bounded as engine's readers bound
// it: DecodeManifest refuses more than MaxManifestSize bytes.
func (l *Loop) manifest(ctx context.Context) (format.Manifest, error) {
	b, err := l.read(ctx, l.cfg.Keys.Manifest(), format.MaxManifestSize+1)
	if err != nil {
		return format.Manifest{}, err
	}
	return format.DecodeManifest(b, format.DatabaseID{})
}

// read reads at most limit bytes of the object at key.
func (l *Loop) read(ctx context.Context, key string, limit int) ([]byte, error) {
	rc, _, err := l.cfg.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(io.LimitReader(rc, int64(limit)))
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
