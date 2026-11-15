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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// TestMain shortens the suite's large bodies to 256 KiB and 3 bytes. The
// self-test runs the whole suite against three correct backends and one
// subtest against each of 93 wrong ones, and at the full size the large
// rounds of the race tests alone would write over 5 GiB each time. The shorter size
// still crosses every buffer of localfs and of the wrong backends below,
// whose size-dependent paths are set relative to largeSize; localfs's own
// conformance test runs the suite at the full size.
func TestMain(m *testing.M) {
	largeSize = 256<<10 + 3
	os.Exit(m.Run())
}

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

// runRecorded runs the suite's top-level subtest named only, or the whole
// suite if only is "", against wrap(localfs clients) and returns the
// failures, each prefixed with the top-level subtest's path.
func runRecorded(t *testing.T, only string, wrap func(ns *shared, s storage.Store) storage.Store) []string {
	r := &recorder{failures: &failures{}}
	newNamespace := func(rt reporter) func() storage.Store {
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
	}
	for _, tc := range tests {
		if only == "" || tc.name == only {
			subtest(r, tc.name, func(rt reporter) { tc.fn(rt, newNamespace) })
		}
	}
	return r.failures.list
}

// tempDir returns a new directory, on tmpfs if /dev/shm exists: the
// self-test runs the suite or one of its subtests nearly a hundred times, and
// on a disk localfs's fsyncs make that about four times slower.
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

	// The barriers of wrong backends whose fault shows only when two calls
	// meet inside them, so that the suite rejects them whatever the
	// scheduling.
	arrived   map[string]int           // calls on a key that have arrived at a hold point and not reached it
	reached   map[string]chan struct{} // closed when arrived[key] drops to 0
	changed   chan struct{}            // closed and replaced by every change
	lists     int                      // List calls in progress
	listWrite bool                     // a write has completed while a List was in progress
	reads     int                      // reads begun
	lastRead  int                      // the number of the latest-begun read that has returned
	bodies    int                      // writes reading their body
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

// arrive registers a call on key that will reach a hold point, before the
// call can be stopped anywhere, and returns reach. reach marks the call as
// there, or gone, and returns a channel that is closed once every call on key
// that arrived with it has reached the hold point or gone.
func (ns *shared) arrive(key string) (reach func() <-chan struct{}) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.arrived == nil {
		ns.arrived, ns.reached = map[string]int{}, map[string]chan struct{}{}
	}
	if ns.arrived[key] == 0 {
		ns.reached[key] = make(chan struct{})
	}
	ns.arrived[key]++
	ch := ns.reached[key]
	return sync.OnceValue(func() <-chan struct{} {
		ns.mu.Lock()
		defer ns.mu.Unlock()
		if ns.arrived[key]--; ns.arrived[key] == 0 {
			close(ch)
		}
		return ch
	})
}

// hold waits until ch is closed, or for overlapTimeout, so that a call that
// never comes cannot stop the others for good.
func hold(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(overlapTimeout):
	}
}

// changes returns a channel that the next call of change closes.
func (ns *shared) changes() <-chan struct{} {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.changed == nil {
		ns.changed = make(chan struct{})
	}
	return ns.changed
}

// change runs f, if it is not nil, under ns.mu and wakes everything waiting
// on changes.
func (ns *shared) change(f func()) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if f != nil {
		f()
	}
	if ns.changed != nil {
		close(ns.changed)
	}
	ns.changed = make(chan struct{})
}

// wrongBackends breaks one rule each. want is the top-level subtest that must
// reject it, and the only one the self-test runs against it.
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
	{"lets two Creates of one key win across clients", "ConcurrentCreate", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "create", always, checkThenPut} }},
	{"lets two Replaces of one version win across clients", "ConcurrentReplace", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "replace", always, checkThenPut} }},
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
	// Found by the third review.
	{"stores half of a large body (Create)", "CreateExisting", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "create", isLarge, truncate} }},
	{"lets two empty Creates of one key win across clients", "ConcurrentCreate", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "create", isEmpty, checkThenPut} }},
	{"a large Replace returns success without the effect", "Replace", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "replace", isLarge, noEffect} }},
	{"an empty Replace ignores its condition", "Replace", func(ns *shared, s storage.Store) storage.Store {
		return bySize{ns, s, "replace", isEmpty, unconditional}
	}},
	{"error text contains the body read before the reader failed", "BodyErrorText", func(_ *shared, s storage.Store) storage.Store { return leakReadBody{s} }},
	{"returns context errors without a *storage.Error", "CancelledContext", func(_ *shared, s storage.Store) storage.Store { return rawContextErrors{s} }},
	{"returns a *storage.BodyError without a *storage.Error", "BodyErrorText", func(_ *shared, s storage.Store) storage.Store { return rawBodyError{s} }},
	{"range bodies ignore their context", "BodyHonoursContext", func(_ *shared, s storage.Store) storage.Store { return rangeIgnoresContext{s} }},
	{"a bounded range of a large object reports its length as the size, and another token", "RangeReads", func(_ *shared, s storage.Store) storage.Store { return largeRangeInfo{s} }},
	{"Head adds a byte to a large object's size", "CreateExisting", func(_ *shared, s storage.Store) storage.Store { return largeHeadSize{s} }},
	{"rejects List selectors that are not valid keys", "ListOrder", func(_ *shared, s storage.Store) storage.Store { return strictSelectors{s} }},
	{"List adds a byte to a large object's size", "CreateExisting", func(_ *shared, s storage.Store) storage.Store { return largeListSize{s} }},
	{"Delete of an empty object returns success without the effect", "Delete", func(_ *shared, s storage.Store) storage.Store { return deleteBySize{s, isEmpty} }},
	{"Delete of a large object returns success without the effect", "Delete", func(_ *shared, s storage.Store) storage.Store { return deleteBySize{s, isLarge} }},
	{"Head answers from the client's last Get", "Replace", func(_ *shared, s storage.Store) storage.Store {
		return &cachedHead{Store: s, cache: map[string]storage.Info{}}
	}},
	{"a large body fetches its tail at the current version", "OpenBodyKeepsItsVersion", func(_ *shared, s storage.Store) storage.Store { return pagedBody{s} }},
	{"empty objects share one version token", "VersionTokens", func(_ *shared, s storage.Store) storage.Store { return emptyTokens{s} }},
	{"ignores startAfter", "ListPagination", func(_ *shared, s storage.Store) storage.Store { return listIgnoresStartAfter{s} }},
	// Found by the sweep.
	{"lets two large Creates of one key win across clients", "ConcurrentCreate", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "create", isLarge, checkThenPut} }},
	{"lets two large Replaces of one version win across clients", "ConcurrentReplace", func(ns *shared, s storage.Store) storage.Store {
		return bySize{ns, s, "replace", isLarge, checkThenPut}
	}},
	{"a large Replace ignores its condition", "Replace", func(ns *shared, s storage.Store) storage.Store {
		return bySize{ns, s, "replace", isLarge, unconditional}
	}},
	{"a large Create overwrites an existing object", "CreateExisting", func(ns *shared, s storage.Store) storage.Store {
		return bySize{ns, s, "create", isLarge, unconditional}
	}},
	{"an empty Replace resurrects a deleted object", "ReplaceDeleteRace", func(_ *shared, s storage.Store) storage.Store { return emptyResurrect{s} }},
	{"keeps the token on an identical large rewrite", "VersionTokens", func(ns *shared, s storage.Store) storage.Store { return bySize{ns, s, "replace", isLarge, keepToken} }},
	{"commits a large body without its context and reports the context's error", "EndOfBody", func(ns *shared, s storage.Store) storage.Store {
		return bySize{ns, s, "create", isLarge, commitWithoutContext}
	}},
	{"publishes the first half of a large body before the body ends", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return halfPublish{s} }},
	{"GetRange answers from the client's last Get", "Replace", func(_ *shared, s storage.Store) storage.Store {
		return &rangeCache{Store: s, objects: map[string]cachedObject{}}
	}},
	{"fills a page by position", "ListDuringWrites", func(ns *shared, s storage.Store) storage.Store { return listByPosition{s, ns} }},
	// Found by the fourth review.
	{"publishes a short first read as the whole body (Create)", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return shortRead{s, false} }},
	{"publishes a short first read as the whole body (Replace)", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return shortRead{s, true} }},
	{"Get, GetRange and List answer from the client's last Get until it calls Head", "FreshReads", func(_ *shared, s storage.Store) storage.Store {
		return &headRefreshed{Store: s, objects: map[string]cachedObject{}}
	}},
	{"gives content written again after other content its old token", "VersionTokens", func(ns *shared, s storage.Store) storage.Store { return hashCounterTokens{s, ns} }},
	{"returns a version token with a failed write", "CreateExisting", func(_ *shared, s storage.Store) storage.Store { return tokenOnFailure{s} }},
	{"checks GetRange's range before the key", "InvalidKeys", func(_ *shared, s storage.Store) storage.Store { return rangeBeforeKey{s} }},
	{"checks Replace's empty version before the key", "InvalidKeys", func(_ *shared, s storage.Store) storage.Store { return versionBeforeKey{s} }},
	{"Delete ignores a context whose deadline has passed", "CancelledContext", func(_ *shared, s storage.Store) storage.Store { return ignoresDeadline{s} }},
	{"a Get body fails after cancellation with an error other than the context's", "BodyHonoursContext", func(_ *shared, s storage.Store) storage.Store { return cancelledBodyError{s} }},
	// Found by the fourth sweep.
	{"checks Head's deadline before the key", "InvalidKeys", func(_ *shared, s storage.Store) storage.Store { return deadlineBeforeKey{s} }},
	{"a second Delete of a key created again does nothing", "Delete", func(ns *shared, s storage.Store) storage.Store { return deleteOnce{s, ns} }},
	{"Replace deletes the object and creates it again", "ConcurrentReadsSeeWholeVersions", func(ns *shared, s storage.Store) storage.Store { return deleteThenCreate{s, ns} }},
	// Found by the fifth review.
	{"reads wait while a write reads its body", "WriteInProgress", func(ns *shared, s storage.Store) storage.Store { return readsWaitForWrites{s, ns} }},
	{"List with an empty prefix reports the size a key was created with", "FreshReads", func(ns *shared, s storage.Store) storage.Store { return listCreatedSize{s, ns} }},
	{"drops the last byte of a body whose length is a multiple of 32 KiB", "ExactMultiples", func(_ *shared, s storage.Store) storage.Store { return exactMultiple{s} }},
	{"a failed write stops other clients' writes of the key", "WriteInProgress", func(ns *shared, s storage.Store) storage.Store { return &poisonOthers{Store: s, ns: ns} }},
	{"Replace of a missing key wraps ErrNotFound as well", "NotFound", func(_ *shared, s storage.Store) storage.Store { return replaceMissingNotFound{s} }},
	{"Get, GetRange and List answer a key seen deleted as missing until Head", "FreshReads", func(_ *shared, s storage.Store) storage.Store {
		return &tombstones{Store: s, seen: map[string]bool{}, gone: map[string]bool{}}
	}},
	{"Delete folds the key to lower case", "ValidKeys", func(_ *shared, s storage.Store) storage.Store { return deleteFoldsCase{s} }},
	{"closing a body before its end breaks the client's next read", "RangeReads", func(_ *shared, s storage.Store) storage.Store { return &closeUnread{Store: s} }},
	{"a failed Replace creates another object", "Replace", func(_ *shared, s storage.Store) storage.Store { return replaceGhost{s} }},
	{"a deadline passing during the body gives outcome-unknown", "WriteInProgress", func(_ *shared, s storage.Store) storage.Store { return deadlineUnknown{s} }},
	{"Create of an existing key with the same content succeeds", "CreateExisting", func(_ *shared, s storage.Store) storage.Store { return idempotentCreate{s} }},
}

// TestSuiteRejectsWrongBackends runs against every wrong backend the subtest
// it names, and requires that subtest to fail.
func TestSuiteRejectsWrongBackends(t *testing.T) {
	// A wrong backend that blocks a call costs overlapTimeout per blocked call;
	// these backends are expected to fail, so a shorter wait is enough.
	saved := overlapTimeout
	overlapTimeout = 3 * time.Second
	t.Cleanup(func() { overlapTimeout = saved })
	for _, tc := range wrongBackends {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if len(runRecorded(t, tc.want, tc.wrap)) == 0 {
				t.Fatalf("the suite did not reject it in %s", tc.want)
			}
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
			if failures := runRecorded(t, "", func(_ *shared, s storage.Store) storage.Store { return tc.wrap(s) }); len(failures) > 0 {
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

// Wrong backends that break a rule only for some body sizes, through one read
// path, on a client that had earlier state, or in an error's form.

func isEmpty(n int) bool { return n == 0 }

func isLarge(n int) bool { return n >= largeSize }

func always(int) bool { return true }

// bySize reads a write's whole body and then writes it with wrong if the
// operation is op and the body's length matches at, and correctly otherwise:
// a backend that takes another path for some sizes and gets only that path
// wrong. Every write arrives at ns's barrier for its key before it reads its
// body; a wrong write that holds there finds it in ctx under reachKey.
type bySize struct {
	ns *shared
	storage.Store
	op    string // "create" or "replace"
	at    func(n int) bool
	wrong func(ctx context.Context, s storage.Store, key, version string, replace bool, data []byte) (string, error)
}

type reachKey struct{}

func (s bySize) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s bySize) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s bySize) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	reach := s.ns.arrive(key)
	defer reach()
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	if opName(replace) == s.op && s.at(len(data)) {
		return s.wrong(context.WithValue(ctx, reachKey{}, reach), s.Store, key, version, replace, data)
	}
	reach()
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

// The wrong writes bySize uses.

func truncate(ctx context.Context, s storage.Store, key, version string, replace bool, data []byte) (string, error) {
	return write(ctx, s, key, version, replace, bytes.NewReader(data[:len(data)/2]))
}

// checkThenPut checks the condition and then writes unconditionally. Between
// the two it holds until every write of the key that was in progress when it
// started has checked its condition too, so that racing writes all pass the
// check and all write.
func checkThenPut(ctx context.Context, s storage.Store, key, version string, replace bool, data []byte) (string, error) {
	info, err := s.Head(ctx, key)
	if replace && (err != nil || info.Version != version) || !replace && err == nil {
		return "", fail(opName(replace), key, storage.ErrPreconditionFailed)
	}
	hold(ctx.Value(reachKey{}).(func() <-chan struct{})())
	return put(ctx, s, key, data)
}

func unconditional(ctx context.Context, s storage.Store, key, _ string, _ bool, data []byte) (string, error) {
	return put(ctx, s, key, data)
}

func noEffect(context.Context, storage.Store, string, string, bool, []byte) (string, error) {
	return "invented-version", nil
}

func keepToken(ctx context.Context, s storage.Store, key, version string, replace bool, data []byte) (string, error) {
	if cur, info, err := s.Get(ctx, key); err == nil {
		b, _ := io.ReadAll(cur)
		_ = cur.Close()
		if info.Version == version && bytes.Equal(b, data) {
			return version, nil
		}
	}
	return write(ctx, s, key, version, replace, bytes.NewReader(data))
}

func commitWithoutContext(ctx context.Context, s storage.Store, key, version string, replace bool, data []byte) (string, error) {
	v, err := write(context.WithoutCancel(ctx), s, key, version, replace, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", fail(opName(replace), key, err)
	}
	return v, nil
}

// emptyResurrect checks the version when a Replace starts and, if the body
// turns out to be empty, writes it unconditionally once it has been read.
type emptyResurrect struct{ storage.Store }

func (s emptyResurrect) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("replace", key, err)
	}
	info, herr := s.Head(ctx, key)
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("replace", key, err)
	}
	if len(data) > 0 || herr != nil || info.Version != version {
		return s.Store.Replace(ctx, key, version, bytes.NewReader(data))
	}
	return put(ctx, s.Store, key, data)
}

// halfPublish creates an object from the first half of a large body as soon
// as it has read it, and replaces it with the whole body at the end.
type halfPublish struct{ storage.Store }

func (s halfPublish) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("create", key, err)
	}
	first, err := readBody(ctx, io.LimitReader(body, int64(largeSize/2)))
	if err != nil {
		return "", fail("create", key, err)
	}
	if len(first) < largeSize/2 {
		return s.Store.Create(ctx, key, bytes.NewReader(first))
	}
	v, err := s.Store.Create(ctx, key, bytes.NewReader(first))
	if err != nil {
		return "", err
	}
	rest, err := readBody(ctx, body)
	if err != nil {
		return "", fail("create", key, err)
	}
	return s.Replace(ctx, key, v, bytes.NewReader(append(first, rest...)))
}

// leakReadBody puts the bytes it read before the reader failed into the
// error's text, keeping the *storage.BodyError.
type leakReadBody struct{ storage.Store }

func (s leakReadBody) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s leakReadBody) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s leakReadBody) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(replace), key, fmt.Errorf("%w after %q", err, data))
	}
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

// rawContextErrors returns Head's context errors without their
// *storage.Error.
type rawContextErrors struct{ storage.Store }

func (s rawContextErrors) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	var se *storage.Error
	if errors.As(err, &se) && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return info, se.Err
	}
	return info, err
}

// rawBodyError returns a Create's *storage.BodyError without its
// *storage.Error.
type rawBodyError struct{ storage.Store }

func (s rawBodyError) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	var be *storage.BodyError
	if errors.As(err, &be) {
		return "", be
	}
	return v, err
}

// rangeIgnoresContext honours a context when GetRange is called and then
// detaches the body from it.
type rangeIgnoresContext struct{ storage.Store }

func (s rangeIgnoresContext) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if err := storage.ValidateKey(key); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, storage.Info{}, fail("get", key, err)
	}
	return s.Store.GetRange(context.WithoutCancel(ctx), key, offset, length)
}

// largeRangeInfo returns, for a bounded range of a large object, the range's
// length as the size and another version token.
type largeRangeInfo struct{ storage.Store }

func (s largeRangeInfo) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err == nil && isLarge(int(info.Size)) && length < info.Size {
		info = storage.Info{Size: length, Version: "a-different-version"}
	}
	return body, info, err
}

// largeHeadSize adds a byte to the size Head reports for a large object.
type largeHeadSize struct{ storage.Store }

func (s largeHeadSize) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	if err == nil && isLarge(int(info.Size)) {
		info.Size++
	}
	return info, err
}

// largeListSize adds a byte to the size List reports for a large object.
type largeListSize struct{ storage.Store }

func (s largeListSize) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	for i := range entries {
		if isLarge(int(entries[i].Size)) {
			entries[i].Size++
		}
	}
	return entries, err
}

// strictSelectors rejects a prefix or startAfter that is not a valid key,
// after removing a trailing "/" from the prefix.
type strictSelectors struct{ storage.Store }

func (s strictSelectors) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	for _, selector := range []string{strings.TrimSuffix(prefix, "/"), startAfter} {
		if selector == "" {
			continue
		}
		if err := storage.ValidateKey(selector); err != nil {
			return nil, fail("list", "", err)
		}
	}
	return s.Store.List(ctx, prefix, startAfter, limit)
}

// listIgnoresStartAfter lists from the beginning whatever the cursor.
type listIgnoresStartAfter struct{ storage.Store }

func (s listIgnoresStartAfter) List(ctx context.Context, prefix, _ string, limit int) ([]storage.Entry, error) {
	return s.Store.List(ctx, prefix, "", limit)
}

// listByPosition fills a page in chunks of ten, asking each time for the
// entries after the first ones by their position, as a service paged by
// offset would: a key created or deleted ahead of the position in between
// shifts the rest. Once a Create or Delete through any client of the
// namespace has returned while a List was in progress, it holds between two
// chunks until a key has been created ahead of its position, looking again
// after every Create and Delete, for at most overlapTimeout: the next chunk
// then starts with an entry it has already returned.
type listByPosition struct {
	storage.Store
	ns *shared
}

func (s listByPosition) wrote() {
	s.ns.change(func() { s.ns.listWrite = s.ns.listWrite || s.ns.lists > 0 })
}

func (s listByPosition) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	defer s.wrote()
	return s.Store.Create(ctx, key, body)
}

func (s listByPosition) Delete(ctx context.Context, key string) error {
	defer s.wrote()
	return s.Store.Delete(ctx, key)
}

func (s listByPosition) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if limit < 1 || limit > storage.MaxListLimit {
		return s.Store.List(ctx, prefix, startAfter, limit)
	}
	s.ns.change(func() { s.ns.lists++ })
	defer s.ns.change(func() { s.ns.lists-- })
	var out []storage.Entry
	for len(out) < limit {
		changed := s.ns.changes()
		all, err := s.Store.List(ctx, prefix, startAfter, min(len(out)+10, limit))
		s.ns.mu.Lock()
		held := len(out) > 0 && s.ns.listWrite
		s.ns.mu.Unlock()
		if held {
			timeout := time.After(overlapTimeout)
		wait:
			for err == nil && (len(all) <= len(out) || all[len(out)].Key > out[len(out)-1].Key) {
				select {
				case <-changed:
				case <-timeout:
					break wait
				}
				changed = s.ns.changes()
				all, err = s.Store.List(ctx, prefix, startAfter, min(len(out)+10, limit))
			}
		}
		if err != nil {
			return nil, err
		}
		if len(all) <= len(out) {
			break
		}
		out = append(out, all[len(out):]...)
	}
	return out, nil
}

// deleteBySize returns success without deleting an object whose size matches
// at.
type deleteBySize struct {
	storage.Store
	at func(n int) bool
}

func (s deleteBySize) Delete(ctx context.Context, key string) error {
	if info, err := s.Head(ctx, key); err == nil && s.at(int(info.Size)) {
		return nil
	}
	return s.Store.Delete(ctx, key)
}

// cachedHead answers Head from the Info of the client's last Get of the key,
// and forgets it only when the client deletes the key.
type cachedHead struct {
	storage.Store
	mu    sync.Mutex
	cache map[string]storage.Info
}

func (s *cachedHead) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.Get(ctx, key)
	if err == nil {
		s.mu.Lock()
		s.cache[key] = info
		s.mu.Unlock()
	}
	return body, info, err
}

func (s *cachedHead) Head(ctx context.Context, key string) (storage.Info, error) {
	if err := storage.ValidateKey(key); err != nil {
		return storage.Info{}, fail("head", key, err)
	}
	if err := ctx.Err(); err != nil {
		return storage.Info{}, fail("head", key, err)
	}
	s.mu.Lock()
	info, ok := s.cache[key]
	s.mu.Unlock()
	if ok {
		return info, nil
	}
	return s.Store.Head(ctx, key)
}

func (s *cachedHead) Delete(ctx context.Context, key string) error {
	err := s.Store.Delete(ctx, key)
	if err == nil {
		s.mu.Lock()
		delete(s.cache, key)
		s.mu.Unlock()
	}
	return err
}

// rangeCache serves GetRange from the object the client last read whole with
// Get, and forgets it only on the client's own writes, so that another
// client's write leaves it stale.
type rangeCache struct {
	storage.Store
	mu      sync.Mutex
	objects map[string]cachedObject
}

type cachedObject struct {
	info storage.Info
	data []byte
}

func (s *rangeCache) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.objects)
}

func (s *rangeCache) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	defer s.forget()
	return s.Store.Create(ctx, key, body)
}

func (s *rangeCache) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	defer s.forget()
	return s.Store.Replace(ctx, key, version, body)
}

func (s *rangeCache) Delete(ctx context.Context, key string) error {
	defer s.forget()
	return s.Store.Delete(ctx, key)
}

func (s *rangeCache) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.Get(ctx, key)
	if err != nil {
		return body, info, err
	}
	var buf bytes.Buffer
	return readCloser{readerFunc(func(p []byte) (int, error) {
		n, err := body.Read(p)
		buf.Write(p[:n])
		if err == io.EOF { //nolint:errorlint // localfs returns io.EOF itself
			s.mu.Lock()
			s.objects[key] = cachedObject{info, bytes.Clone(buf.Bytes())}
			s.mu.Unlock()
		}
		return n, err
	}), body.Close}, info, nil
}

func (s *rangeCache) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	s.mu.Lock()
	c, ok := s.objects[key]
	s.mu.Unlock()
	if !ok || offset < 0 || length < 0 || ctx.Err() != nil || storage.ValidateKey(key) != nil {
		return s.Store.GetRange(ctx, key, offset, length)
	}
	start := min(offset, c.info.Size)
	return io.NopCloser(bytes.NewReader(c.data[start : start+min(length, c.info.Size-start)])), c.info, nil
}

// pagedBody fetches a body longer than a quarter of largeSize in two
// requests, the second when the first quarter has been read, without pinning
// it to the version of the first.
type pagedBody struct{ storage.Store }

func (s pagedBody) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s pagedBody) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	chunk := int64(largeSize / 4)
	if offset < 0 || length <= chunk {
		return s.Store.GetRange(ctx, key, offset, length)
	}
	first, info, err := s.Store.GetRange(ctx, key, offset, chunk)
	if err != nil || offset >= info.Size || info.Size-offset <= chunk {
		return first, info, err
	}
	var rest io.ReadCloser
	second := readerFunc(func(p []byte) (int, error) {
		if rest == nil {
			var err error
			if rest, _, err = s.Store.GetRange(ctx, key, offset+chunk, length-chunk); err != nil {
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

// emptyTokens gives every empty object the same version token, as a token
// derived from the content would, and maps it back on Replace.
type emptyTokens struct{ storage.Store }

const emptyToken = "empty-content-token"

func (s emptyTokens) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s emptyTokens) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s emptyTokens) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	if replace && version == emptyToken {
		if info, err := s.Store.Head(ctx, key); err == nil && info.Size == 0 {
			version = info.Version
		}
	}
	v, err := write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
	if err == nil && len(data) == 0 {
		v = emptyToken
	}
	return v, err
}

func (s emptyTokens) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s emptyTokens) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err == nil && info.Size == 0 {
		info.Version = emptyToken
	}
	return body, info, err
}

func (s emptyTokens) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	if err == nil && info.Size == 0 {
		info.Version = emptyToken
	}
	return info, err
}

// shortRead takes a first Read that returns 1 to smallSize bytes and no error
// for the whole body and publishes those bytes at once.
type shortRead struct {
	storage.Store
	replace bool
}

func (s shortRead) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if s.replace {
		return s.Store.Create(ctx, key, body)
	}
	return s.write(ctx, key, "", body)
}

func (s shortRead) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if !s.replace {
		return s.Store.Replace(ctx, key, version, body)
	}
	return s.write(ctx, key, version, body)
}

func (s shortRead) write(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(s.replace), key, err)
	}
	p := make([]byte, 32<<10)
	n, err := body.Read(p)
	if err == nil && n >= 1 && n <= smallSize {
		return write(ctx, s.Store, key, version, s.replace, bytes.NewReader(p[:n]))
	}
	rest := body
	if err != nil {
		rest = iotest.ErrReader(err)
	}
	return write(ctx, s.Store, key, version, s.replace, io.MultiReader(bytes.NewReader(p[:n]), rest))
}

// headRefreshed caches an object the client has read whole with Get and
// serves Get, GetRange and listed sizes from it; only a Head of the key, or a
// write through the client, refreshes it.
type headRefreshed struct {
	storage.Store
	mu      sync.Mutex
	objects map[string]cachedObject
}

func (s *headRefreshed) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
}

func (s *headRefreshed) cached(ctx context.Context, key string) (cachedObject, bool) {
	if ctx.Err() != nil || storage.ValidateKey(key) != nil {
		return cachedObject{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.objects[key]
	return c, ok
}

// serve returns the bytes [offset, offset+length) of c as a body that honours ctx.
func serve(ctx context.Context, c cachedObject, offset, length int64) io.ReadCloser {
	start := min(offset, c.info.Size)
	r := bytes.NewReader(c.data[start : start+min(length, c.info.Size-start)])
	return io.NopCloser(readerFunc(func(p []byte) (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return r.Read(p)
	}))
}

func (s *headRefreshed) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	defer s.forget(key)
	return s.Store.Create(ctx, key, body)
}

func (s *headRefreshed) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	defer s.forget(key)
	return s.Store.Replace(ctx, key, version, body)
}

func (s *headRefreshed) Delete(ctx context.Context, key string) error {
	defer s.forget(key)
	return s.Store.Delete(ctx, key)
}

func (s *headRefreshed) Head(ctx context.Context, key string) (storage.Info, error) {
	s.forget(key)
	return s.Store.Head(ctx, key)
}

func (s *headRefreshed) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if c, ok := s.cached(ctx, key); ok {
		return serve(ctx, c, 0, c.info.Size), c.info, nil
	}
	body, info, err := s.Store.Get(ctx, key)
	if err != nil {
		return body, info, err
	}
	var buf bytes.Buffer
	return readCloser{readerFunc(func(p []byte) (int, error) {
		n, err := body.Read(p)
		buf.Write(p[:n])
		if err == io.EOF { //nolint:errorlint // localfs returns io.EOF itself
			s.mu.Lock()
			s.objects[key] = cachedObject{info, bytes.Clone(buf.Bytes())}
			s.mu.Unlock()
		}
		return n, err
	}), body.Close}, info, nil
}

func (s *headRefreshed) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if c, ok := s.cached(ctx, key); ok && offset >= 0 && length >= 0 {
		return serve(ctx, c, offset, length), c.info, nil
	}
	return s.Store.GetRange(ctx, key, offset, length)
}

func (s *headRefreshed) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	for i, e := range entries {
		if c, ok := s.cached(ctx, e.Key); ok {
			entries[i].Size = c.info.Size
		}
	}
	return entries, err
}

// hashCounterTokens makes a token from a hash of the content and a count of
// the writes of that content in a row, which a Delete does not reset: a key
// written A, B, A gets the first A's token again, and a Replace with that
// token succeeds against the second A.
type hashCounterTokens struct {
	storage.Store
	ns *shared
}

func (s hashCounterTokens) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s hashCounterTokens) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s hashCounterTokens) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	sum := sha256.Sum256(data)
	hash := fmt.Sprintf("%x", sum[:8])
	s.ns.mu.Lock()
	defer s.ns.mu.Unlock()
	if s.ns.keys == nil {
		s.ns.keys, s.ns.gen = map[string]string{}, map[string]int{}
	}
	if inner, ok := s.ns.keys[key+"\x00token "+version]; replace && ok {
		version = inner
	}
	v, err := write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	if s.ns.keys[key+"\x00hash"] == hash {
		s.ns.gen[key]++
	} else {
		s.ns.keys[key+"\x00hash"], s.ns.gen[key] = hash, 1
	}
	token := fmt.Sprintf("%s-%d", hash, s.ns.gen[key])
	s.ns.keys[key+"\x00token "+token] = v
	s.ns.keys[key+"\x00inner "+v] = token
	return token, nil
}

func (s hashCounterTokens) token(key string, info storage.Info) storage.Info {
	if token, ok := s.ns.get(key + "\x00inner " + info.Version); ok {
		info.Version = token
	}
	return info
}

func (s hashCounterTokens) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

func (s hashCounterTokens) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	if err == nil {
		info = s.token(key, info)
	}
	return body, info, err
}

func (s hashCounterTokens) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	if err == nil {
		info = s.token(key, info)
	}
	return info, err
}

// tokenOnFailure returns a version token beside the error of a failed write.
type tokenOnFailure struct{ storage.Store }

func (s tokenOnFailure) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err != nil {
		return "a-token-on-failure", err
	}
	return v, nil
}

func (s tokenOnFailure) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	if err != nil {
		return "a-token-on-failure", err
	}
	return v, nil
}

// rangeBeforeKey checks GetRange's offset and length before its key.
type rangeBeforeKey struct{ storage.Store }

func (s rangeBeforeKey) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if offset < 0 || length < 0 {
		return nil, storage.Info{}, fail("get", key, storage.ErrInvalidArgument)
	}
	return s.Store.GetRange(ctx, key, offset, length)
}

// versionBeforeKey fails a Replace with an empty version before it checks
// the key.
type versionBeforeKey struct{ storage.Store }

func (s versionBeforeKey) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if version == "" {
		return "", fail("replace", key, storage.ErrPreconditionFailed)
	}
	return s.Store.Replace(ctx, key, version, body)
}

// deadlineBeforeKey checks Head's context for a passed deadline before the
// key.
type deadlineBeforeKey struct{ storage.Store }

func (s deadlineBeforeKey) Head(ctx context.Context, key string) (storage.Info, error) {
	if err := ctx.Err(); errors.Is(err, context.DeadlineExceeded) {
		return storage.Info{}, fail("head", key, err)
	}
	return s.Store.Head(ctx, key)
}

// ignoresDeadline lets a Delete whose context's deadline has passed go
// ahead, as a backend that checks only for cancellation would.
type ignoresDeadline struct{ storage.Store }

func (s ignoresDeadline) Delete(ctx context.Context, key string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		ctx = context.WithoutCancel(ctx)
	}
	return s.Store.Delete(ctx, key)
}

// cancelledBodyError fails a Get body whose context has ended with an error
// of its own instead of the context's.
type cancelledBodyError struct{ storage.Store }

func (s cancelledBodyError) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	body, info, err := s.Store.Get(ctx, key)
	if err != nil {
		return body, info, err
	}
	return readCloser{readerFunc(func(p []byte) (int, error) {
		if ctx.Err() != nil {
			return 0, errBody
		}
		return body.Read(p)
	}), body.Close}, info, nil
}

// deleteOnce does nothing on a Delete of a key that it has deleted before, as
// a backend that keeps a tombstone instead of removing it again would.
type deleteOnce struct {
	storage.Store
	ns *shared
}

func (s deleteOnce) Delete(ctx context.Context, key string) error {
	if _, ok := s.ns.get("deleted\x00" + key); ok && storage.ValidateKey(key) == nil && ctx.Err() == nil {
		return nil
	}
	err := s.Store.Delete(ctx, key)
	if err == nil {
		s.ns.put("deleted\x00"+key, "")
	}
	return err
}

// deleteThenCreate replaces an object by deleting it and creating it again,
// so that the key reads as missing in between. Once a read has begun while
// one of its Replace calls was in progress, that call holds after its Delete
// until a read that began after the Delete has returned, for at most
// overlapTimeout.
type deleteThenCreate struct {
	storage.Store
	ns *shared
}

// read numbers a read as it begins and returns the function that marks it
// returned.
func (s deleteThenCreate) read() (returned func()) {
	var n int
	s.ns.change(func() { s.ns.reads++; n = s.ns.reads })
	return func() { s.ns.change(func() { s.ns.lastRead = max(s.ns.lastRead, n) }) }
}

func (s deleteThenCreate) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	defer s.read()()
	return s.Store.Get(ctx, key)
}

func (s deleteThenCreate) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	defer s.read()()
	return s.Store.GetRange(ctx, key, offset, length)
}

func (s deleteThenCreate) Head(ctx context.Context, key string) (storage.Info, error) {
	defer s.read()()
	return s.Store.Head(ctx, key)
}

func (s deleteThenCreate) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	defer s.read()()
	return s.Store.List(ctx, prefix, startAfter, limit)
}

func (s deleteThenCreate) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("replace", key, err)
	}
	s.ns.mu.Lock()
	before := s.ns.reads
	s.ns.mu.Unlock()
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("replace", key, err)
	}
	if info, err := s.Store.Head(ctx, key); err != nil || info.Version != version {
		return "", fail("replace", key, storage.ErrPreconditionFailed)
	}
	if err := s.Delete(ctx, key); err != nil {
		return "", err
	}
	s.ns.mu.Lock()
	deleted := s.ns.reads
	s.ns.mu.Unlock()
	if deleted > before {
		timeout := time.After(overlapTimeout)
	wait:
		for {
			changed := s.ns.changes()
			s.ns.mu.Lock()
			seen := s.ns.lastRead > deleted
			s.ns.mu.Unlock()
			if seen {
				break
			}
			select {
			case <-changed:
			case <-timeout:
				break wait
			}
		}
	}
	return s.Create(ctx, key, bytes.NewReader(data))
}

// readsWaitForWrites makes every read wait while a write on any client of the
// namespace is reading its body. It blocks no mutation.
type readsWaitForWrites struct {
	storage.Store
	ns *shared
}

func (s readsWaitForWrites) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s readsWaitForWrites) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s readsWaitForWrites) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	s.ns.change(func() { s.ns.bodies++ })
	data, err := readBody(ctx, body)
	s.ns.change(func() { s.ns.bodies-- })
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

// wait returns once no write is reading its body, or ctx has ended.
func (s readsWaitForWrites) wait(ctx context.Context) {
	for {
		changed := s.ns.changes()
		s.ns.mu.Lock()
		busy := s.ns.bodies > 0
		s.ns.mu.Unlock()
		if !busy {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return
		}
	}
}

func (s readsWaitForWrites) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	s.wait(ctx)
	return s.Store.Get(ctx, key)
}

func (s readsWaitForWrites) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	s.wait(ctx)
	return s.Store.GetRange(ctx, key, offset, length)
}

func (s readsWaitForWrites) Head(ctx context.Context, key string) (storage.Info, error) {
	s.wait(ctx)
	return s.Store.Head(ctx, key)
}

func (s readsWaitForWrites) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	s.wait(ctx)
	return s.Store.List(ctx, prefix, startAfter, limit)
}

// listCreatedSize lists every key with the size it was created with when the
// prefix is empty, as a backend whose full listings come from a stale index
// would.
type listCreatedSize struct {
	storage.Store
	ns *shared
}

func (s listCreatedSize) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err == nil {
		if info, err := s.Head(ctx, key); err == nil {
			s.ns.put("created size\x00"+key, strconv.FormatInt(info.Size, 10))
		}
	}
	return v, err
}

func (s listCreatedSize) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	if prefix == "" {
		for i, e := range entries {
			if size, ok := s.ns.get("created size\x00" + e.Key); ok {
				entries[i].Size, _ = strconv.ParseInt(size, 10, 64)
			}
		}
	}
	return entries, err
}

// exactMultiple drops the last byte of a body whose length is a positive
// multiple of its 32 KiB buffer, as a chunked upload that mishandles a full
// last chunk would.
type exactMultiple struct{ storage.Store }

func (s exactMultiple) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s exactMultiple) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s exactMultiple) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	if len(data) > 0 && len(data)%(32<<10) == 0 {
		data = data[:len(data)-1]
	}
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

// poisonOthers marks a key when a write of it fails reading its body, and then
// fails every write of that key through another client with
// ErrPreconditionFailed.
type poisonOthers struct {
	storage.Store
	ns *shared
}

func (s *poisonOthers) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s *poisonOthers) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s *poisonOthers) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	me := fmt.Sprintf("%p", s)
	if who, ok := s.ns.get("poisoned\x00" + key); ok && who != me {
		return "", fail(opName(replace), key, storage.ErrPreconditionFailed)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		s.ns.put("poisoned\x00"+key, me)
		return "", fail(opName(replace), key, err)
	}
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

// replaceMissingNotFound fails a Replace of a missing key with an error that
// wraps ErrNotFound as well as ErrPreconditionFailed.
type replaceMissingNotFound struct{ storage.Store }

func (s replaceMissingNotFound) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	if errors.Is(err, storage.ErrPreconditionFailed) {
		if _, herr := s.Head(ctx, key); errors.Is(herr, storage.ErrNotFound) {
			return v, fail("replace", key, fmt.Errorf("%w: %w", storage.ErrPreconditionFailed, storage.ErrNotFound))
		}
	}
	return v, err
}

// tombstones remembers the keys its client has seen exist and then seen
// missing, and answers Get, GetRange and List as if they were still missing
// until the client calls Head on the key or writes it.
type tombstones struct {
	storage.Store
	mu         sync.Mutex
	seen, gone map[string]bool
}

// saw records what a read or write of key returned.
func (s *tombstones) saw(key string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil:
		s.seen[key] = true
		delete(s.gone, key)
	case errors.Is(err, storage.ErrNotFound) && s.seen[key]:
		s.gone[key] = true
	}
}

func (s *tombstones) isGone(ctx context.Context, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ctx.Err() == nil && s.gone[key]
}

func (s *tombstones) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	v, err := s.Store.Create(ctx, key, body)
	if err == nil {
		s.saw(key, nil)
	}
	return v, err
}

func (s *tombstones) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	if err == nil {
		s.saw(key, nil)
	}
	return v, err
}

func (s *tombstones) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if s.isGone(ctx, key) {
		return nil, storage.Info{}, fail("get", key, storage.ErrNotFound)
	}
	body, info, err := s.Store.Get(ctx, key)
	s.saw(key, err)
	return body, info, err
}

func (s *tombstones) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if offset >= 0 && length >= 0 && s.isGone(ctx, key) {
		return nil, storage.Info{}, fail("get", key, storage.ErrNotFound)
	}
	body, info, err := s.Store.GetRange(ctx, key, offset, length)
	s.saw(key, err)
	return body, info, err
}

func (s *tombstones) Head(ctx context.Context, key string) (storage.Info, error) {
	info, err := s.Store.Head(ctx, key)
	s.saw(key, err)
	return info, err
}

func (s *tombstones) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.Store.List(ctx, prefix, startAfter, limit)
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.DeleteFunc(entries, func(e storage.Entry) bool { return s.gone[e.Key] }), err
}

// deleteFoldsCase deletes the lower-case form of the key.
type deleteFoldsCase struct{ storage.Store }

func (s deleteFoldsCase) Delete(ctx context.Context, key string) error {
	if storage.ValidateKey(key) == nil {
		key = strings.ToLower(key)
	}
	return s.Store.Delete(ctx, key)
}

// closeUnread fails its client's next read, once, after a body was closed
// before its end, as a client whose connection is left in a bad state would.
type closeUnread struct {
	storage.Store
	broken atomic.Bool
}

func (s *closeUnread) check(op, key string) error {
	if s.broken.CompareAndSwap(true, false) {
		return fail(op, key, errors.New("connection reset"))
	}
	return nil
}

func (s *closeUnread) wrap(body io.ReadCloser, info storage.Info, err error) (io.ReadCloser, storage.Info, error) {
	if err != nil {
		return body, info, err
	}
	var ended atomic.Bool
	return readCloser{readerFunc(func(p []byte) (int, error) {
		n, err := body.Read(p)
		if err != nil {
			ended.Store(true)
		}
		return n, err
	}), func() error {
		if !ended.Load() {
			s.broken.Store(true)
		}
		return body.Close()
	}}, info, nil
}

func (s *closeUnread) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	if err := s.check("get", key); err != nil {
		return nil, storage.Info{}, err
	}
	return s.wrap(s.Store.Get(ctx, key))
}

func (s *closeUnread) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	if err := s.check("get", key); err != nil {
		return nil, storage.Info{}, err
	}
	return s.wrap(s.Store.GetRange(ctx, key, offset, length))
}

func (s *closeUnread) Head(ctx context.Context, key string) (storage.Info, error) {
	if err := s.check("head", key); err != nil {
		return storage.Info{}, err
	}
	return s.Store.Head(ctx, key)
}

func (s *closeUnread) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if err := s.check("list", ""); err != nil {
		return nil, err
	}
	return s.Store.List(ctx, prefix, startAfter, limit)
}

// replaceGhost creates another object beside the key when a Replace fails its
// condition, as a backend that stages the new version under its own name and
// does not remove it would.
type replaceGhost struct{ storage.Store }

func (s replaceGhost) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	v, err := s.Store.Replace(ctx, key, version, body)
	if errors.Is(err, storage.ErrPreconditionFailed) {
		_, _ = s.Create(ctx, key+"-staged", strings.NewReader("staged"))
	}
	return v, err
}

// deadlineUnknown returns ErrOutcomeUnknown when its context's deadline
// passes while it reads the body, before publication.
type deadlineUnknown struct{ storage.Store }

func (s deadlineUnknown) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	return s.write(ctx, key, "", false, body)
}

func (s deadlineUnknown) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	return s.write(ctx, key, version, true, body)
}

func (s deadlineUnknown) write(ctx context.Context, key, version string, replace bool, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail(opName(replace), key, err)
	}
	data, err := readBody(ctx, body)
	if errors.Is(err, context.DeadlineExceeded) {
		return "", fail(opName(replace), key, fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, err))
	}
	if err != nil {
		return "", fail(opName(replace), key, err)
	}
	return write(ctx, s.Store, key, version, replace, bytes.NewReader(data))
}

// idempotentCreate lets a Create of an existing key whose content equals the
// body succeed, with the existing token, as a backend that treats a repeated
// upload as done would.
type idempotentCreate struct{ storage.Store }

func (s idempotentCreate) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := storage.ValidateKey(key); err != nil {
		return "", fail("create", key, err)
	}
	data, err := readBody(ctx, body)
	if err != nil {
		return "", fail("create", key, err)
	}
	v, err := s.Store.Create(ctx, key, bytes.NewReader(data))
	if errors.Is(err, storage.ErrPreconditionFailed) && len(data) > 0 {
		if cur, info, gerr := s.Get(ctx, key); gerr == nil {
			b, rerr := io.ReadAll(cur)
			_ = cur.Close()
			if rerr == nil && bytes.Equal(b, data) {
				return info.Version, nil
			}
		}
	}
	return v, err
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
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if cur, err := s.Head(ctx, key); err != nil || cur.Version != info.Version {
			return 0, errors.New("version no longer available")
		}
		return body.Read(p)
	}), body.Close}, info, nil
}
