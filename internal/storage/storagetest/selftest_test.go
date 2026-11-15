//go:build linux || darwin

package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// recorder is a reporter that records failures instead of failing the test
// that runs it, so the suite can be run against a backend that must fail it.
// Like *testing.T, Fatal ends the calling goroutine, and each subtest runs on
// its own goroutine and runs its cleanups when it ends.
type recorder struct {
	name     string
	failures *failures
	cleanups []func()
}

type failures struct {
	mu   sync.Mutex
	list []string // "Subtest/path: message"
}

func (r *recorder) Helper() {}

func (r *recorder) Error(args ...any) { r.record(fmt.Sprint(args...)) }

func (r *recorder) Errorf(format string, args ...any) { r.record(fmt.Sprintf(format, args...)) }

func (r *recorder) Fatal(args ...any) { r.Error(args...); runtime.Goexit() }

func (r *recorder) Fatalf(format string, args ...any) { r.Errorf(format, args...); runtime.Goexit() }

func (r *recorder) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }

func (r *recorder) record(msg string) {
	r.failures.mu.Lock()
	defer r.failures.mu.Unlock()
	r.failures.list = append(r.failures.list, r.name+": "+msg)
}

func (r *recorder) run(name string, fn func(reporter)) {
	sub := &recorder{name: strings.TrimPrefix(r.name+"/"+name, "/"), failures: r.failures}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			for _, f := range slices.Backward(sub.cleanups) {
				f()
			}
		}()
		fn(sub)
	}()
	<-done
}

// runRecorded runs the suite against wrap(localfs clients) and returns the
// failures, each prefixed with the top-level subtest's path.
func runRecorded(t *testing.T, wrap func(ns *shared, s storage.Store) storage.Store) []string {
	r := &recorder{failures: &failures{}}
	run(r, func(rt reporter) func() storage.Store {
		dir := tempDir(t)
		ns := &shared{}
		return func() storage.Store {
			s, err := localfs.Open(dir)
			if err != nil {
				rt.Fatal(err)
			}
			rt.Cleanup(func() { _ = s.Close() })
			return wrap(ns, s)
		}
	})
	return r.failures.list
}

// tempDir returns a new directory, on tmpfs if /dev/shm exists: the
// catalogue runs the suite dozens of times, and on a disk localfs's fsyncs
// make that about four times slower.
func tempDir(t *testing.T) string {
	if dir, err := os.MkdirTemp("/dev/shm", "storagetest-"); err == nil {
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		return dir
	}
	return t.TempDir()
}

// shared is state that the clients of one namespace share. A correct backend
// has none; wrong backends use it to break a rule across clients.
type shared struct {
	lock sync.Mutex // held across a whole call by the cross-client lock backend
	mu   sync.Mutex // guards the fields below
	keys map[string]string
	gen  map[string]int
	last string
}

func (ns *shared) put(k, v string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.keys == nil {
		ns.keys = map[string]string{}
	}
	ns.keys[k] = v
}

func (ns *shared) get(k string) (string, bool) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	v, ok := ns.keys[k]
	return v, ok
}

// wrongBackends breaks one rule each. want is the top-level subtest that must
// reject it; others may reject it too.
var wrongBackends = []struct {
	name, want string
	wrap       func(ns *shared, s storage.Store) storage.Store
}{
	{"publishes a partial object before the body ends (Create)", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return earlyPublish{s, false} }},
	{"publishes a partial object before the body ends (Replace)", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return earlyPublish{s, true} }},
	{"holds an instance lock while reading the body", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return &instanceLock{Store: s} }},
	{"holds a cross-client lock while reading the body", "ConcurrentCreate", func(ns *shared, s storage.Store) storage.Store { return crossLock{s, ns} }},
	{"returns a plain error after committing, context cancelled at EOF", "EndOfBody", func(_ *shared, s storage.Store) storage.Store { return lateCancel{s} }},
	{"commits what it read when the reader fails at the last byte", "EndOfBody", func(_ *shared, s storage.Store) storage.Store { return commitOnReaderError{s} }},
	{"Delete returns success without the effect", "Delete", func(_ *shared, s storage.Store) storage.Store { return noopDelete{s} }},
	{"Replace returns success without the effect", "Replace", func(_ *shared, s storage.Store) storage.Store { return noopReplace{s} }},
	{"returns outcome-unknown before publication", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return unknownBeforePublication{s} }},
	{"returns the reader's error unwrapped", "BodyErrorText", func(_ *shared, s storage.Store) storage.Store { return rawReaderError{s} }},
	{"truncates listings at 100", "ListBeyondMaxLimit", func(_ *shared, s storage.Store) storage.Store { return listCap{s, 100} }},
	{"truncates listings one short of MaxListLimit", "ListBeyondMaxLimit", func(_ *shared, s storage.Store) storage.Store { return listCap{s, storage.MaxListLimit - 1} }},
	{"returns more entries than the limit", "ListLimits", func(_ *shared, s storage.Store) storage.Store { return listOverLimit{s} }},
	{"lists out of order", "ListOrder", func(_ *shared, s storage.Store) storage.Store { return listReversed{s} }},
	{"repeats the key at a page boundary", "ListPagination", func(_ *shared, s storage.Store) storage.Store { return listRepeat{s} }},
	{"skips the key after a page boundary", "ListPagination", func(_ *shared, s storage.Store) storage.Store { return listSkip{s} }},
	{"lists a deleted key", "Delete", func(ns *shared, s storage.Store) storage.Store { return listDeleted{s, ns} }},
	{"does not list the key created last", "ValidKeys", func(ns *shared, s storage.Store) storage.Store { return listMissesLast{s, ns} }},
	{"lists a stale size", "Replace", func(ns *shared, s storage.Store) storage.Store { return listStaleSize{s, ns} }},
	{"Create returns a token other than the stored one", "CreateExisting", func(_ *shared, s storage.Store) storage.Store { return wrongCreateToken{s} }},
	{"Head returns a stale token", "Replace", func(ns *shared, s storage.Store) storage.Store { return staleHead{s, ns} }},
	{"reuses tokens after delete and recreate", "VersionTokens", func(ns *shared, s storage.Store) storage.Store { return counterTokens{s, ns} }},
	{"uses a content hash as the token", "VersionTokens", func(_ *shared, s storage.Store) storage.Store { return contentTokens{s} }},
	{"keeps the token on an identical-content rewrite", "VersionTokens", func(_ *shared, s storage.Store) storage.Store { return dedupeReplace{s} }},
	{"lets two Creates of one key win across clients", "ConcurrentCreate", func(_ *shared, s storage.Store) storage.Store { return checkThenCreate{s} }},
	{"lets two Replaces of one version win across clients", "ConcurrentReplace", func(_ *shared, s storage.Store) storage.Store { return checkThenReplace{s} }},
	{"Replace resurrects a deleted object", "ReplaceDeleteRace", func(_ *shared, s storage.Store) storage.Store { return resurrectingReplace{s} }},
	{"Create overwrites an existing object", "CreateExisting", func(_ *shared, s storage.Store) storage.Store { return overwritingCreate{s} }},
	{"Replace of a missing key returns ErrNotFound", "NotFound", func(_ *shared, s storage.Store) storage.Store { return replaceNotFound{s} }},
	{"opens bodies lazily", "OpenBodyKeepsItsVersion", func(_ *shared, s storage.Store) storage.Store { return lazyBody{s} }},
	{"returns a body mixing two versions", "OpenBodyKeepsItsVersion", func(_ *shared, s storage.Store) storage.Store { return mixedBody{s} }},
	{"starts a range one byte late", "RangeReads", func(_ *shared, s storage.Store) storage.Store { return rangeShift{s, 1, 0} }},
	{"ends a range one byte late", "RangeReads", func(_ *shared, s storage.Store) storage.Store { return rangeShift{s, 0, 1} }},
	{"returns the last byte for a range starting at the end", "RangeReads", func(_ *shared, s storage.Store) storage.Store { return rangeAtEnd{s} }},
	{"bodies always fail", "ValidKeys", func(_ *shared, s storage.Store) storage.Store { return failingBody{s} }},
	{"bodies ignore their context", "BodyHonoursContext", func(_ *shared, s storage.Store) storage.Store { return bodyIgnoresContext{s} }},
	{"reads the body before validating the key", "InvalidKeys", func(_ *shared, s storage.Store) storage.Store { return validateAfterIO{s} }},
	{"checks the context before the key", "InvalidKeys", func(_ *shared, s storage.Store) storage.Store { return contextBeforeKey{s} }},
	{"keys differing only in case collide", "ValidKeys", func(_ *shared, s storage.Store) storage.Store { return caseFolding{s} }},
	{"error text contains body bytes", "Errors", func(_ *shared, s storage.Store) storage.Store { return leakyErrors{s} }},
	{"Delete fails on a missing key", "Delete", func(_ *shared, s storage.Store) storage.Store { return strictDelete{s} }},
	{"ignores an already-cancelled context", "CancelledContext", func(_ *shared, s storage.Store) storage.Store { return ignoresCancelled{s} }},
}

// TestSuiteRejectsWrongBackends runs the whole suite against every wrong
// backend and requires the subtest it names to fail.
func TestSuiteRejectsWrongBackends(t *testing.T) {
	// A wrong backend that blocks a call costs overlapTimeout per blocked call;
	// these backends are expected to fail, so a shorter wait is enough.
	saved := overlapTimeout
	overlapTimeout = 3 * time.Second
	t.Cleanup(func() { overlapTimeout = saved })
	for _, tc := range wrongBackends {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			failed := map[string]bool{}
			var top []string
			for _, f := range runRecorded(t, tc.wrap) {
				name, _, _ := strings.Cut(f, "/")
				name, _, _ = strings.Cut(name, ":")
				if !failed[name] {
					top = append(top, name)
				}
				failed[name] = true
			}
			if !failed[tc.want] {
				t.Fatalf("the suite did not reject it in %s; failing subtests: %q", tc.want, top)
			}
			t.Logf("%s: rejected by %q", tc.name, top)
		})
	}
}

// TestSuiteAcceptsCorrectBackends runs the whole suite against correct
// backends that are slow, or whose open bodies fail once their version is
// replaced, as the contract allows.
func TestSuiteAcceptsCorrectBackends(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(s storage.Store) storage.Store
	}{
		{"every read delayed", func(s storage.Store) storage.Store { return slowReads{s} }},
		{"every write delayed", func(s storage.Store) storage.Store { return slowWrites{s} }},
		{"bodies fail once their version is replaced", func(s storage.Store) storage.Store { return invalidatedBody{s} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if failures := runRecorded(t, func(_ *shared, s storage.Store) storage.Store { return tc.wrap(s) }); len(failures) > 0 {
				t.Fatalf("the suite rejected a correct backend:\n%s", strings.Join(failures, "\n"))
			}
		})
	}
}

// Helpers for the wrappers.

func fail(op, key string, err error) error { return &storage.Error{Op: op, Key: key, Err: err} }

// readBody reads body to its end the way a backend must: stopping when ctx
// ends and wrapping the reader's error in a *storage.BodyError.
func readBody(ctx context.Context, body io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	p := make([]byte, 32<<10)
	for {
		if err := ctx.Err(); err != nil {
			return buf.Bytes(), err
		}
		n, err := body.Read(p)
		buf.Write(p[:n])
		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return buf.Bytes(), &storage.BodyError{Err: err}
		}
	}
}

func write(ctx context.Context, s storage.Store, key, version string, replace bool, body io.Reader) (string, error) {
	if replace {
		return s.Replace(ctx, key, version, body)
	}
	return s.Create(ctx, key, body)
}

func opName(replace bool) string {
	if replace {
		return "replace"
	}
	return "create"
}

// Wrong backends. Each embeds a correct localfs Store and overrides what it
// breaks.

// earlyPublish publishes the first chunk of the body at once and replaces it
// with the whole body at the end.
type earlyPublish struct {
	storage.Store
	replace bool
}

func (s earlyPublish) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if s.replace {
		return s.Store.Create(ctx, key, body)
	}
	return s.write(ctx, key, "", body)
}

func (s earlyPublish) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if !s.replace {
		return s.Store.Replace(ctx, key, version, body)
	}
	return s.write(ctx, key, version, body)
}

func (s earlyPublish) write(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(s.replace), key, err)
	}
	p := make([]byte, 32<<10)
	n, err := body.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fail(opName(s.replace), key, &storage.BodyError{Err: err})
	}
	v, werr := write(ctx, s.Store, key, version, s.replace, bytes.NewReader(p[:n]))
	if werr != nil || errors.Is(err, io.EOF) {
		return v, werr
	}
	rest, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(s.replace), key, err)
	}
	return s.Store.Replace(ctx, key, v, bytes.NewReader(append(p[:n], rest...)))
}

// instanceLock serialises the mutations of one client, body reads included.
type instanceLock struct {
	storage.Store
	mu sync.Mutex
}

func (s *instanceLock) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Create(ctx, key, body)
}

func (s *instanceLock) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Replace(ctx, key, version, body)
}

func (s *instanceLock) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Delete(ctx, key)
}

// crossLock serialises the mutations of every client of a namespace.
type crossLock struct {
	storage.Store
	ns *shared
}

func (s crossLock) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	s.ns.lock.Lock()
	defer s.ns.lock.Unlock()
	return s.Store.Create(ctx, key, body)
}

func (s crossLock) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	s.ns.lock.Lock()
	defer s.ns.lock.Unlock()
	return s.Store.Replace(ctx, key, version, body)
}

func (s crossLock) Delete(ctx context.Context, key string) error {
	s.ns.lock.Lock()
	defer s.ns.lock.Unlock()
	return s.Store.Delete(ctx, key)
}

// lateCancel commits without its context and reports the context's error
// afterwards as if nothing had happened.
type lateCancel struct{ storage.Store }

func (s lateCancel) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s lateCancel) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s lateCancel) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	v, err := write(context.WithoutCancel(ctx), s.Store, key, version, replace, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", fail(opName(replace), key, err)
	}
	return v, nil
}

// commitOnReaderError commits whatever it read when the reader fails, and
// returns the reader's error.
type commitOnReaderError struct{ storage.Store }

func (s commitOnReaderError) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s commitOnReaderError) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s commitOnReaderError) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	var be *storage.BodyError
	if errors.As(err, &be) && len(data) > 0 {
		_, _ = write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
	}
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

type noopDelete struct{ storage.Store }

func (s noopDelete) Delete(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return fail("delete", key, err)
	}
	return ctx.Err()
}

type noopReplace struct{ storage.Store }

func (s noopReplace) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	info, err := s.Head(ctx, key)
	if err != nil || info.Version != version {
		return s.Store.Replace(ctx, key, version, body)
	}
	if _, err := readBody(ctx, body); err != nil {
		return "", fail("replace", key, err)
	}
	return version + "0", nil
}

// unknownBeforePublication adds ErrOutcomeUnknown to errors that happen while
// the body is read.
type unknownBeforePublication struct{ storage.Store }

func (s unknownBeforePublication) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	return v, s.mark(err)
}

func (s unknownBeforePublication) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	return v, s.mark(err)
}

func (unknownBeforePublication) mark(err error) error {
	var se *storage.Error
	if readerError(err) != nil || errors.Is(err, context.Canceled) {
		if errors.As(err, &se) {
			return fail(se.Op, se.Key, fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, se.Err))
		}
	}
	return err
}

// rawReaderError returns the reader's error itself instead of a BodyError.
type rawReaderError struct{ storage.Store }

func (s rawReaderError) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	return v, s.unwrap(err)
}

func (s rawReaderError) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	return v, s.unwrap(err)
}

func (rawReaderError) unwrap(err error) error {
	var se *storage.Error
	if r := readerError(err); r != nil && errors.As(err, &se) {
		return fail(se.Op, se.Key, r)
	}
	return err
}

type listCap struct {
	storage.Store
	max int
}

func (s listCap) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	return entries[:min(len(entries), s.max)], err
}

type listOverLimit struct{ storage.Store }

func (s listOverLimit) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if limit < storage.MaxListLimit {
		limit++
	}
	return s.Store.List(ctx, prefix, startAfter, limit)
}

type listReversed struct{ storage.Store }

func (s listReversed) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	slices.Reverse(entries)
	return entries, err
}

// listRepeat treats startAfter as inclusive.
type listRepeat struct{ storage.Store }

func (s listRepeat) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	if err != nil || startAfter == "" || !strings.HasPrefix(startAfter, prefix) {
		return entries, err
	}
	info, herr := s.Head(ctx, startAfter)
	if herr != nil {
		return entries, nil
	}
	entries = append([]storage.Entry{{Key: startAfter, Size: info.Size}}, entries...)
	return entries[:min(len(entries), limit)], nil
}

type listSkip struct{ storage.Store }

func (s listSkip) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if startAfter == "" {
		return s.Store.List(ctx, prefix, startAfter, limit)
	}
	entries, err := s.Store.List(ctx, prefix, startAfter, min(limit+1, storage.MaxListLimit))
	if len(entries) > 0 {
		entries = entries[1:]
	}
	return entries[:min(len(entries), limit)], err
}

// listDeleted keeps listing deleted keys, as a stale index would.
type listDeleted struct {
	storage.Store
	ns *shared
}

func (s listDeleted) Delete(ctx context.Context, key string) error {
	if info, err := s.Head(ctx, key); err == nil {
		s.ns.put(key, fmt.Sprint(info.Size))
	}
	return s.Store.Delete(ctx, key)
}

func (s listDeleted) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	if err != nil {
		return entries, err
	}
	s.ns.mu.Lock()
	for k := range s.ns.keys {
		if strings.HasPrefix(k, prefix) && k > startAfter && !slices.ContainsFunc(entries, func(e storage.Entry) bool { return e.Key == k }) {
			entries = append(entries, storage.Entry{Key: k})
		}
	}
	s.ns.mu.Unlock()
	slices.SortFunc(entries, func(a, b storage.Entry) int { return strings.Compare(a.Key, b.Key) })
	return entries[:min(len(entries), limit)], nil
}

// listMissesLast hides the key created last, as an eventually consistent
// index would.
type listMissesLast struct {
	storage.Store
	ns *shared
}

func (s listMissesLast) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err == nil {
		s.ns.mu.Lock()
		s.ns.last = key
		s.ns.mu.Unlock()
	}
	return v, err
}

func (s listMissesLast) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	s.ns.mu.Lock()
	last := s.ns.last
	s.ns.mu.Unlock()
	return slices.DeleteFunc(entries, func(e storage.Entry) bool { return e.Key == last }), err
}

// listStaleSize lists the size an object had when it was created.
type listStaleSize struct {
	storage.Store
	ns *shared
}

func (s listStaleSize) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if info, herr := s.Head(ctx, key); err == nil && herr == nil {
		s.ns.put(key, fmt.Sprint(info.Size))
	}
	return v, err
}

func (s listStaleSize) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	for i, e := range entries {
		if size, ok := s.ns.get(e.Key); ok {
			_, _ = fmt.Sscan(size, &entries[i].Size)
		}
	}
	return entries, err
}

type wrongCreateToken struct{ storage.Store }

func (s wrongCreateToken) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err != nil {
		return v, err
	}
	return v + "0", nil
}

// staleHead caches Head results per key.
type staleHead struct {
	storage.Store
	ns *shared
}

func (s staleHead) Head(ctx context.Context, key string) (storage.Info, error) {
	if v, ok := s.ns.get(key); ok {
		var info storage.Info
		_, _ = fmt.Sscan(v, &info.Size, &info.Version)
		return info, nil
	}
	info, err := s.Store.Head(ctx, key)
	if err == nil {
		s.ns.put(key, fmt.Sprint(info.Size, " ", info.Version))
	}
	return info, err
}

// counterTokens numbers the versions of a key from 1 and restarts after a
// Delete, so a recreated key repeats its old tokens.
type counterTokens struct {
	storage.Store
	ns *shared
}

func (s counterTokens) token(key, inner string) string {
	s.ns.mu.Lock()
	defer s.ns.mu.Unlock()
	if s.ns.keys == nil {
		s.ns.keys, s.ns.gen = map[string]string{}, map[string]int{}
	}
	for t, in := range s.ns.keys {
		if in == inner && strings.HasPrefix(t, key+"#") {
			return strings.TrimPrefix(t, key+"#")
		}
	}
	s.ns.gen[key]++
	t := fmt.Sprint(s.ns.gen[key])
	s.ns.keys[key+"#"+t] = inner
	return t
}

func (s counterTokens) inner(key, token string) string {
	v, _ := s.ns.get(key + "#" + token)
	return v
}

func (s counterTokens) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err != nil {
		return "", err
	}
	return s.token(key, v), nil
}

func (s counterTokens) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, s.inner(key, version), body)
	if err != nil {
		return "", err
	}
	return s.token(key, v), nil
}

func (s counterTokens) Delete(ctx context.Context, key string) error {
	err := s.Store.Delete(ctx, key)
	if err == nil {
		s.ns.mu.Lock()
		delete(s.ns.gen, key)
		for t := range s.ns.keys {
			if strings.HasPrefix(t, key+"#") {
				delete(s.ns.keys, t)
			}
		}
		s.ns.mu.Unlock()
	}
	return err
}

func (s counterTokens) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s counterTokens) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err == nil {
		info.Version = s.token(key, info.Version)
	}
	return body, info, err
}

func (s counterTokens) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	if err == nil {
		info.Version = s.token(key, info.Version)
	}
	return info, err
}

// contentTokens uses the content as the token, as an MD5 ETag would, and
// compares it on Replace.
type contentTokens struct{ storage.Store }

func (s contentTokens) current(ctx context.Context, key string) (string, string, error) {
	body, info, err := s.Store.Get(ctx, key)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = body.Close() }()
	b, err := io.ReadAll(body)
	return fmt.Sprintf("%x", sha256.Sum256(b)), info.Version, err
}

func (s contentTokens) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if _, err := s.Store.Create(ctx, key, body); err != nil {
		return "", err
	}
	token, _, err := s.current(ctx, key)
	return token, err
}

func (s contentTokens) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	token, inner, err := s.current(ctx, key)
	if err != nil || token != version {
		inner = ""
	}
	if _, err := s.Store.Replace(ctx, key, inner, body); err != nil {
		return "", err
	}
	token, _, err = s.current(ctx, key)
	return token, err
}

func (s contentTokens) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s contentTokens) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err == nil {
		info.Version, _, _ = s.current(ctx, key)
	}
	return body, info, err
}

func (s contentTokens) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	if err == nil {
		info.Version, _, err = s.current(ctx, key)
	}
	return info, err
}

// dedupeReplace skips a Replace whose content equals the current content.
type dedupeReplace struct{ storage.Store }

func (s dedupeReplace) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("replace", key, err)
	}
	if cur, info, err := s.Get(ctx, key); err == nil {
		b, _ := io.ReadAll(cur)
		_ = cur.Close()
		if info.Version == version && bytes.Equal(b, data) {
			return version, nil
		}
	}
	return s.Store.Replace(ctx, key, version, bytes.NewReader(data))
}

// checkThenCreate checks that the key is missing and then writes it
// unconditionally.
type checkThenCreate struct{ storage.Store }

func (s checkThenCreate) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("create", key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("create", key, err)
	}
	if _, err := s.Head(ctx, key); err == nil {
		return "", fail("create", key, storage.ErrPreconditionFailed)
	}
	time.Sleep(time.Millisecond) // the window a network round trip would leave
	return put(ctx, s.Store, key, data)
}

// put writes key unconditionally: it is the operation the contract forbids.
func put(ctx context.Context, s storage.Store, key string, data []byte) (string, error) {
	for {
		if info, err := s.Head(ctx, key); err == nil {
			if v, err := s.Replace(ctx, key, info.Version, bytes.NewReader(data)); err == nil {
				return v, nil
			}
			continue
		}
		if v, err := s.Create(ctx, key, bytes.NewReader(data)); err == nil || !errors.Is(err, storage.ErrPreconditionFailed) {
			return v, err
		}
	}
}

// checkThenReplace compares the version and then writes unconditionally.
type checkThenReplace struct{ storage.Store }

func (s checkThenReplace) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("replace", key, err)
	}
	if info, err := s.Head(ctx, key); err != nil || info.Version != version {
		return "", fail("replace", key, storage.ErrPreconditionFailed)
	}
	time.Sleep(time.Millisecond)
	return put(ctx, s.Store, key, data)
}

// resurrectingReplace checks the version when it starts and writes
// unconditionally when the body has been read.
type resurrectingReplace struct{ storage.Store }

func (s resurrectingReplace) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if info, err := s.Head(ctx, key); err != nil || info.Version != version {
		return s.Store.Replace(ctx, key, version, body)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("replace", key, err)
	}
	return put(ctx, s.Store, key, data)
}

type overwritingCreate struct{ storage.Store }

func (s overwritingCreate) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("create", key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("create", key, err)
	}
	return put(ctx, s.Store, key, data)
}

type replaceNotFound struct{ storage.Store }

func (s replaceNotFound) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	if _, herr := s.Head(ctx, key); errors.Is(err, storage.ErrPreconditionFailed) && errors.Is(herr, storage.ErrNotFound) {
		return "", fail("replace", key, storage.ErrNotFound)
	}
	return v, err
}

// lazyBody opens the object on the body's first Read, so it returns whatever
// version is current then, under the Info of the version that was current at
// Get.
type lazyBody struct{ storage.Store }

func (s lazyBody) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s lazyBody) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err != nil {
		return body, info, err
	}
	_ = body.Close()
	var lazy io.ReadCloser
	return readCloser{readerFunc(func(p []byte) (int, error) {
		if lazy == nil {
			var err error
			if lazy, _, err = s.Store.GetRange(ctx, key, offset, length); err != nil {
				return 0, err
			}
		}
		return lazy.Read(p)
	}), func() error {
		if lazy != nil {
			return lazy.Close()
		}
		return nil
	}}, info, nil
}

type readCloser struct {
	io.Reader
	close func() error
}

func (r readCloser) Close() error { return r.close() }

// mixedBody returns the first 4 KiB of a whole-object read from the version
// that was opened and the rest from whatever version is current when the
// first 4 KiB run out.
type mixedBody struct{ storage.Store }

func (s mixedBody) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s mixedBody) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	const head = 4 << 10
	if offset != 0 || length <= head {
		return s.Store.GetRange(ctx, key, offset, length)
	}
	first, info, err := s.Store.GetRange(ctx, key, 0, head)
	if err != nil {
		return first, info, err
	}
	var rest io.ReadCloser
	second := readerFunc(func(p []byte) (int, error) {
		if rest == nil {
			var err error
			if rest, _, err = s.Store.GetRange(ctx, key, head, length-head); err != nil {
				return 0, err
			}
		}
		return rest.Read(p)
	})
	return readCloser{io.MultiReader(first, second), func() error {
		if rest != nil {
			_ = rest.Close()
		}
		return first.Close()
	}}, info, nil
}

type rangeShift struct {
	storage.Store
	start, end int64
}

func (s rangeShift) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if offset >= 0 && length > 0 && length < 1<<62 {
		offset, length = offset+s.start, length+s.end
	}
	return s.Store.GetRange(ctx, key, offset, length)
}

type rangeAtEnd struct{ storage.Store }

func (s rangeAtEnd) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if info, err := s.Head(ctx, key); err == nil && offset == info.Size && info.Size > 0 && length > 0 {
		offset = info.Size - 1
	}
	return s.Store.GetRange(ctx, key, offset, length)
}

var errBody = errors.New("body read failed")

type failingBody struct{ storage.Store }

func (s failingBody) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s failingBody) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err != nil {
		return body, info, err
	}
	return readCloser{readerFunc(func([]byte) (int, error) { return 0, errBody }), body.Close}, info, nil
}

type bodyIgnoresContext struct{ storage.Store }

func (s bodyIgnoresContext) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if err := storage.ValidateKey(key); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	return s.Store.Get(context.WithoutCancel(ctx), key)
}

type validateAfterIO struct{ storage.Store }

func (s validateAfterIO) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("create", key, err)
	}
	return s.Store.Create(ctx, key, bytes.NewReader(data))
}

type contextBeforeKey struct{ storage.Store }

func (s contextBeforeKey) Head(ctx context.Context, key string) (storage.Info, error) {
	if err := ctx.Err(); err != nil {
		return storage.Info{}, fail("head", key, err)
	}
	return s.Store.Head(ctx, key)
}

// caseFolding lower-cases every key, as a case-insensitive store would.
type caseFolding struct{ storage.Store }

func (s caseFolding) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.Store.Create(ctx, strings.ToLower(key), body)
}

func (s caseFolding) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.Store.Replace(ctx, strings.ToLower(key), version, body)
}

func (s caseFolding) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.Store.Get(ctx, strings.ToLower(key))
}

func (s caseFolding) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	return s.Store.GetRange(ctx, strings.ToLower(key), offset, length)
}

func (s caseFolding) Head(ctx context.Context, key string) (storage.Info, error) {
	return s.Store.Head(ctx, strings.ToLower(key))
}

func (s caseFolding) Delete(ctx context.Context, key string) error {
	return s.Store.Delete(ctx, strings.ToLower(key))
}

// leakyErrors quotes the body in a precondition failure.
type leakyErrors struct{ storage.Store }

func (s leakyErrors) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("create", key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("create", key, err)
	}
	v, err := s.Store.Create(ctx, key, bytes.NewReader(data))
	if errors.Is(err, storage.ErrPreconditionFailed) {
		return "", fail("create", key, fmt.Errorf("%w: rejected %q", storage.ErrPreconditionFailed, data))
	}
	return v, err
}

type strictDelete struct{ storage.Store }

func (s strictDelete) Delete(ctx context.Context, key string) error {
	if _, err := s.Head(ctx, key); errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return s.Store.Delete(ctx, key)
}

type ignoresCancelled struct{ storage.Store }

func (s ignoresCancelled) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.Store.Create(context.WithoutCancel(ctx), key, body)
}

// Correct backends that must pass.

// slowDelay is long against a localfs call and against the gaps between
// Replace calls in testConcurrentReads, short enough to keep the run quick.
const slowDelay = 3 * time.Millisecond

type slowReads struct{ storage.Store }

func (s slowReads) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	time.Sleep(slowDelay)
	return s.Store.Get(ctx, key)
}

func (s slowReads) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	time.Sleep(slowDelay)
	return s.Store.GetRange(ctx, key, offset, length)
}

func (s slowReads) Head(ctx context.Context, key string) (storage.Info, error) {
	time.Sleep(slowDelay)
	return s.Store.Head(ctx, key)
}

func (s slowReads) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	time.Sleep(slowDelay)
	return s.Store.List(ctx, prefix, startAfter, limit)
}

type slowWrites struct{ storage.Store }

func (s slowWrites) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	time.Sleep(slowDelay)
	return s.Store.Create(ctx, key, body)
}

func (s slowWrites) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	time.Sleep(slowDelay)
	return s.Store.Replace(ctx, key, version, body)
}

func (s slowWrites) Delete(ctx context.Context, key string) error {
	time.Sleep(slowDelay)
	return s.Store.Delete(ctx, key)
}

// invalidatedBody fails a body's Read once the body's version is no longer
// current, as a cloud backend may when its connection drops.
type invalidatedBody struct{ storage.Store }

func (s invalidatedBody) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s invalidatedBody) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err != nil {
		return body, info, err
	}
	return readCloser{readerFunc(func(p []byte) (int, error) {
		if cur, err := s.Head(ctx, key); err != nil || cur.Version != info.Version {
			return 0, errors.New("version no longer available")
		}
		return body.Read(p)
	}), body.Close}, info, nil
}
