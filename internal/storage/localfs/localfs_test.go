//go:build linux || darwin

package localfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/storagetest"
)

// Helper processes: the test binary re-executes itself with these variables
// set, and TestMain runs helper instead of the tests.
const (
	envMode    = "LOCALFS_TEST_HELPER"
	envDir     = "LOCALFS_TEST_DIR"
	envID      = "LOCALFS_TEST_ID"
	envVersion = "LOCALFS_TEST_VERSION"
	envOp      = "LOCALFS_TEST_OP"
	envPoint   = "LOCALFS_TEST_POINT"
	helperKey  = "lease"
	rounds     = 10
)

func roundKey(r int) string { return fmt.Sprintf("lease%d", r) }

func TestMain(m *testing.M) {
	if mode := os.Getenv(envMode); mode != "" {
		os.Exit(helper(mode))
	}
	os.Exit(m.Run())
}

func helperPayload(id string) string { return id + "\n" + strings.Repeat(id, 64<<10) }

// helper opens the store, waits for stdin to close (the start signal) and
// runs its writes. For create and replace it writes the keys lease0 to
// lease<rounds-1> in turn, replace using the comma-separated versions in
// envVersion, and prints one line per key: "ok <version>" or "precondition".
func helper(mode string) int {
	s, err := Open(os.Getenv(envDir))
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	ctx := context.Background()
	if mode == "crash" {
		return crash(ctx, s, os.Getenv(envOp), os.Getenv(envPoint), os.Getenv(envVersion))
	}
	id := os.Getenv(envID)
	versions := strings.Split(os.Getenv(envVersion), ",")
	for r := range rounds {
		var v string
		switch mode {
		case "create":
			v, err = s.Create(ctx, roundKey(r), strings.NewReader(helperPayload(id)))
		case "replace":
			v, err = s.Replace(ctx, roundKey(r), versions[r], strings.NewReader(helperPayload(id)))
		default:
			err = fmt.Errorf("unknown mode %q", mode)
		}
		switch {
		case err == nil:
			fmt.Println("ok", v)
		case errors.Is(err, storage.ErrPreconditionFailed):
			fmt.Println("precondition")
		default:
			fmt.Println("error:", err)
			return 1
		}
	}
	return 0
}

var crashPayload = strings.Repeat("n", 64<<10)

// crash runs one mutation of helperKey and stops it at point, where it prints
// "at <point>" and sleeps until killed. Point "body" is inside the body's
// Read, after the store has written crashPayload; any other point is a hook
// point.
func crash(ctx context.Context, s *Store, op, point, version string) int {
	stop := func() {
		fmt.Println("at", point)
		time.Sleep(time.Hour)
	}
	hook = func(p string) error {
		if p == point {
			stop()
		}
		return nil
	}
	body := io.MultiReader(strings.NewReader(crashPayload), readerFunc(func([]byte) (int, error) {
		if point == "body" {
			stop()
		}
		return 0, io.EOF
	}))
	var err error
	switch op {
	case "create":
		_, err = s.Create(ctx, helperKey, body)
	case "replace":
		_, err = s.Replace(ctx, helperKey, version, body)
	case "delete":
		err = s.Delete(ctx, helperKey)
	}
	fmt.Println("finished without stopping:", err)
	return 1
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func helperCmd(t *testing.T, mode, dir, id, version string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^$") //nolint:gosec // re-executes this test binary
	cmd.Env = append(os.Environ(), envMode+"="+mode, envDir+"="+dir, envID+"="+id, envVersion+"="+version)
	cmd.Stderr = os.Stderr
	return cmd
}

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) func() storage.Store {
		dir := t.TempDir()
		return func() storage.Store { return open(t, dir) }
	})
}

// raceProcesses starts n helper processes, releases them together, checks
// that exactly one succeeded on each key and returns, per key, the winner's
// id and version token.
func raceProcesses(t *testing.T, mode, dir, versions string) (winners, winnerVersions []string) {
	const n = 8
	cmds := make([]*exec.Cmd, n)
	outs := make([]*bytes.Buffer, n)
	stdins := make([]io.WriteCloser, n)
	for i := range n {
		cmds[i] = helperCmd(t, mode, dir, fmt.Sprint(i), versions)
		outs[i] = &bytes.Buffer{}
		cmds[i].Stdout = outs[i]
		var err error
		if stdins[i], err = cmds[i].StdinPipe(); err != nil {
			t.Fatal(err)
		}
		if err := cmds[i].Start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range stdins {
		_ = w.Close()
	}
	winners, winnerVersions = make([]string, rounds), make([]string, rounds)
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("helper %d: %v, output %q", i, err, outs[i])
		}
		lines := strings.Split(strings.TrimSpace(outs[i].String()), "\n")
		if len(lines) != rounds {
			t.Fatalf("helper %d printed %q, want %d lines", i, outs[i], rounds)
		}
		for r, line := range lines {
			switch {
			case strings.HasPrefix(line, "ok ") && winners[r] != "":
				t.Fatalf("%s: helpers %s and %d both succeeded", roundKey(r), winners[r], i)
			case strings.HasPrefix(line, "ok "):
				winners[r], winnerVersions[r] = fmt.Sprint(i), strings.TrimPrefix(line, "ok ")
			case line != "precondition":
				t.Fatalf("helper %d printed %q for %s", i, line, roundKey(r))
			}
		}
	}
	for r, w := range winners {
		if w == "" {
			t.Fatalf("%s: no helper succeeded", roundKey(r))
		}
	}
	return winners, winnerVersions
}

func expectObject(t *testing.T, s *Store, key, content, version string) {
	t.Helper()
	body, info, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content || info.Version != version {
		t.Fatalf("%s is %.20q at version %q, want %.20q at version %q", key, got, info.Version, content, version)
	}
}

func TestMultiProcessCreate(t *testing.T) {
	dir := t.TempDir()
	winners, versions := raceProcesses(t, "create", dir, "")
	s := open(t, dir)
	for r := range rounds {
		expectObject(t, s, roundKey(r), helperPayload(winners[r]), versions[r])
	}
}

func TestMultiProcessReplace(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	initial := make([]string, rounds)
	for r := range rounds {
		var err error
		if initial[r], err = s.Create(context.Background(), roundKey(r), strings.NewReader("initial")); err != nil {
			t.Fatal(err)
		}
	}
	winners, versions := raceProcesses(t, "replace", dir, strings.Join(initial, ","))
	for r := range rounds {
		expectObject(t, s, roundKey(r), helperPayload(winners[r]), versions[r])
	}
}

// TestKilled kills a process inside a mutation, at each point where the
// mutation can stop, and checks that the store is consistent afterwards.
func TestKilled(t *testing.T) {
	cases := []struct{ op, point, want string }{
		{"create", "body", "missing"},
		{"create", "sync objects", "new"},
		{"replace", "body", "old"},
		{"replace", "locked", "old"},
		{"replace", "sync objects", "new"},
		{"delete", "locked", "old"},
		{"delete", "sync objects", "missing"},
	}
	for _, tc := range cases {
		t.Run(tc.op+" at "+tc.point, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			s := open(t, dir)
			var old string
			if tc.op != "create" {
				var err error
				if old, err = s.Create(ctx, helperKey, strings.NewReader("old")); err != nil {
					t.Fatal(err)
				}
			}
			cmd := helperCmd(t, "crash", dir, "0", old)
			cmd.Env = append(cmd.Env, envOp+"="+tc.op, envPoint+"="+tc.point)
			cmd.Stdin = strings.NewReader("")
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if line != "at "+tc.point+"\n" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("helper printed %q, %v; want at %s", line, err, tc.point)
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()

			// Readers see exactly the state the mutation had reached.
			current := ""
			switch tc.want {
			case "missing":
				if _, err := s.Head(ctx, helperKey); !errors.Is(err, storage.ErrNotFound) {
					t.Fatalf("Head error = %v, want ErrNotFound", err)
				}
			case "old":
				expectObject(t, s, helperKey, "old", old)
				current = old
			case "new":
				info, err := s.Head(ctx, helperKey)
				if err != nil || info.Version == old {
					t.Fatalf("Head = %+v, %v; want a new version", info, err)
				}
				expectObject(t, s, helperKey, crashPayload, info.Version)
				current = info.Version
			}
			if entries, err := s.List(ctx, "", "", 10); err != nil || (len(entries) == 1) != (current != "") {
				t.Fatalf("List = %v, %v; want %s", entries, err, tc.want)
			}
			// Create-if-absent is not blocked, the lock is free, and later
			// mutations behave.
			lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if _, err := s.Create(lctx, "fresh", strings.NewReader("x")); err != nil {
				t.Fatalf("Create of another key: %v", err)
			}
			if current == "" {
				if current, err = s.Create(lctx, helperKey, strings.NewReader("recreated")); err != nil {
					t.Fatalf("Create after the kill: %v", err)
				}
			}
			if _, err := s.Replace(lctx, helperKey, "stale", strings.NewReader("x")); !errors.Is(err, storage.ErrPreconditionFailed) {
				t.Fatalf("Replace with a stale version error = %v, want ErrPreconditionFailed", err)
			}
			next, err := s.Replace(lctx, helperKey, current, strings.NewReader("next"))
			if err != nil {
				t.Fatalf("Replace after the kill: %v", err)
			}
			if err := s.Delete(lctx, helperKey); err != nil {
				t.Fatalf("Delete after the kill: %v", err)
			}
			if _, err := s.Replace(lctx, helperKey, next, strings.NewReader("x")); !errors.Is(err, storage.ErrPreconditionFailed) {
				t.Fatalf("Replace of the deleted version error = %v, want ErrPreconditionFailed", err)
			}
			if _, err := s.Create(lctx, helperKey, strings.NewReader("again")); err != nil {
				t.Fatalf("Create after Delete: %v", err)
			}
		})
	}
}

// setHook installs a test hook for the duration of the test.
func setHook(t *testing.T, h func(point string) error) {
	hook = h
	t.Cleanup(func() { hook = nil })
}

// TestDeleteMissingSyncs: a Delete that finds the key missing still fsyncs
// objects/, because the process that unlinked it may have died before its
// fsync.
func TestDeleteMissingSyncs(t *testing.T) {
	s := open(t, t.TempDir())
	var points []string
	setHook(t, func(p string) error { points = append(points, p); return nil })
	if err := s.Delete(context.Background(), "missing"); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(points, "sync objects") {
		t.Fatalf("Delete of a missing key passed %q, want a sync of objects", points)
	}
}

// TestOutcome injects a failure at each hook point: before publication the
// error is plain and nothing changes; after it the error wraps
// ErrOutcomeUnknown and the mutation is visible.
func TestOutcome(t *testing.T) {
	ctx := context.Background()
	injected := errors.New("injected")
	cases := []struct {
		op, point string
		existing  bool
		published bool
	}{
		{"create", "sync objects", false, true},
		{"replace", "locked", true, false},
		{"replace", "sync objects", true, true},
		{"delete", "locked", true, false},
		{"delete", "sync objects", true, true},
		{"delete", "sync objects", false, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s at %s, existing %v", tc.op, tc.point, tc.existing), func(t *testing.T) {
			s := open(t, t.TempDir())
			var old string
			if tc.existing {
				var err error
				if old, err = s.Create(ctx, "k", strings.NewReader("old")); err != nil {
					t.Fatal(err)
				}
			}
			setHook(t, func(p string) error {
				if p == tc.point {
					return injected
				}
				return nil
			})
			var err error
			switch tc.op {
			case "create":
				_, err = s.Create(ctx, "k", strings.NewReader("new"))
			case "replace":
				_, err = s.Replace(ctx, "k", old, strings.NewReader("new"))
			case "delete":
				err = s.Delete(ctx, "k")
			}
			if !errors.Is(err, injected) || errors.Is(err, storage.ErrOutcomeUnknown) != tc.published {
				t.Fatalf("error = %v, want the injected error, wrapping ErrOutcomeUnknown: %v", err, tc.published)
			}
			hook = nil
			body, info, err := s.Get(ctx, "k")
			got := "missing"
			if err == nil {
				b, _ := io.ReadAll(body)
				_ = body.Close()
				got = string(b)
			}
			want := map[bool]string{true: "old", false: "missing"}[tc.existing]
			if tc.published {
				want = map[string]string{"create": "new", "replace": "new", "delete": "missing"}[tc.op]
			}
			if got != want || want == "old" && info.Version != old {
				t.Fatalf("after the failure the key holds %q at %q, want %q", got, info.Version, want)
			}
		})
	}
}

// TestForcedInterleavings stops a first mutation inside its critical section,
// after it took the lock (for Replace: after its version check and before its
// rename), starts a second mutation of the same key on another Store, waits
// until that one is blocked on the lock, and only then lets the first go on.
// The suite's race tests cannot force this window; the hook can.
func TestForcedInterleavings(t *testing.T) {
	cases := []struct {
		first, second string
		secondErr     error  // nil or the error the second mutation must return
		want          string // "first" (the first Replace's content) or "missing"
	}{
		{"replace", "delete", nil, "missing"},
		{"replace", "replace", storage.ErrPreconditionFailed, "first"},
		{"delete", "replace", storage.ErrPreconditionFailed, "missing"},
	}
	for _, tc := range cases {
		t.Run(tc.first+" then "+tc.second, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			a, b := open(t, dir), open(t, dir)
			v, err := a.Create(ctx, "k", strings.NewReader("old"))
			if err != nil {
				t.Fatal(err)
			}
			paused, waiting := make(chan struct{}), make(chan struct{})
			resume := make(chan struct{})
			resumeOnce := sync.OnceFunc(func() { close(resume) })
			var pauseOnce, waitOnce sync.Once
			setHook(t, func(p string) error {
				switch p {
				case "locked":
					pauseOnce.Do(func() { close(paused); <-resume })
				case "lock busy":
					waitOnce.Do(func() { close(waiting) })
				}
				return nil
			})
			mutate := func(s *Store, op, content string) error {
				if op == "delete" {
					return s.Delete(ctx, "k")
				}
				_, err := s.Replace(ctx, "k", v, strings.NewReader(content))
				return err
			}
			var wg sync.WaitGroup
			defer wg.Wait()
			defer resumeOnce()
			firstErr, secondErr := make(chan error, 1), make(chan error, 1)
			wg.Go(func() { firstErr <- mutate(a, tc.first, "first") })
			select {
			case <-paused:
			case err := <-firstErr:
				t.Fatalf("first %s returned %v without reaching its critical section", tc.first, err)
			}
			wg.Go(func() { secondErr <- mutate(b, tc.second, "second") })
			select {
			case <-waiting:
			case err := <-secondErr:
				t.Fatalf("second %s returned %v while the first %s held the lock", tc.second, err, tc.first)
			}
			resumeOnce()
			if err := <-firstErr; err != nil {
				t.Fatalf("first %s: %v", tc.first, err)
			}
			if err := <-secondErr; !errors.Is(err, tc.secondErr) || (tc.secondErr == nil) != (err == nil) {
				t.Fatalf("second %s error = %v, want %v", tc.second, err, tc.secondErr)
			}
			hook = nil
			body, _, err := a.Get(ctx, "k")
			switch {
			case tc.want == "missing" && !errors.Is(err, storage.ErrNotFound):
				t.Fatalf("Get error = %v, want ErrNotFound", err)
			case tc.want == "first" && err != nil:
				t.Fatalf("Get: %v", err)
			case tc.want == "first":
				got, _ := io.ReadAll(body)
				_ = body.Close()
				if string(got) != "first" {
					t.Fatalf("k holds %q, want %q", got, "first")
				}
			}
		})
	}
}

// TestValidationBeforeIO removes everything under the root and checks that
// invalid keys are still rejected as invalid, and that nothing was created.
func TestValidationBeforeIO(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	for _, name := range []string{lockName, tmpDir, objDir} {
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	const key = "a/../b"
	errs := map[string]error{}
	_, errs["Create"] = s.Create(ctx, key, strings.NewReader("x"))
	_, errs["Replace"] = s.Replace(ctx, key, "v", strings.NewReader("x"))
	_, _, errs["Get"] = s.Get(ctx, key)
	_, errs["Head"] = s.Head(ctx, key)
	errs["Delete"] = s.Delete(ctx, key)
	for op, err := range errs {
		if !errors.Is(err, storage.ErrInvalidKey) {
			t.Errorf("%s error = %v, want ErrInvalidKey", op, err)
		}
	}
	if got := files(t, dir); !slices.Equal(got, []string{"."}) {
		t.Fatalf("invalid-key calls created %q", got)
	}
}

func TestFailedWriteRemovesTempFile(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	ctx := context.Background()
	v, err := s.Create(ctx, "k", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	boom := iotest.ErrReader(errors.New("boom"))
	if _, err := s.Create(ctx, "k2", boom); err == nil {
		t.Fatal("Create with a failing reader succeeded")
	}
	if _, err := s.Replace(ctx, "k", v, boom); err == nil {
		t.Fatal("Replace with a failing reader succeeded")
	}
	if tmp, err := os.ReadDir(filepath.Join(dir, tmpDir)); err != nil || len(tmp) != 0 {
		t.Fatalf("after failed writes tmp/ holds %v, %v; want nothing", tmp, err)
	}
}

// files returns every path under dir, relative to dir.
func files(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPathTraversal(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	s := open(t, root)
	ctx := context.Background()
	for _, k := range []string{"../x", "../../x", "a/../../x", "/etc/x", `..\x`, "a/../x", "a/.."} {
		if _, err := s.Create(ctx, k, strings.NewReader("x")); !errors.Is(err, storage.ErrInvalidKey) {
			t.Errorf("Create(%q) error = %v, want ErrInvalidKey", k, err)
		}
	}
	pathLike := []string{"..a/b", "a/..b", "...x", "etc/passwd", "root/..x", "tmp/x", "objects/x"}
	for _, k := range pathLike {
		if _, err := s.Create(ctx, k, strings.NewReader(k)); err != nil {
			t.Fatalf("Create(%q): %v", k, err)
		}
	}
	got := files(t, parent)
	want := []string{".", "root", "root/objects", "root/tmp"}
	for _, k := range pathLike {
		want = append(want, filepath.Join("root", objPath(k)))
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("files under the parent directory:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestSymlinkEscape(t *testing.T) {
	ctx := context.Background()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret")
	if err := os.WriteFile(secret, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectOutsideUntouched := func(t *testing.T) {
		t.Helper()
		got := files(t, outside)
		if b, err := os.ReadFile(secret); err != nil || string(b) != "outside" || len(got) != 2 { //nolint:gosec // test-owned path
			t.Fatalf("outside directory changed: files %q, secret %q, %v", got, b, err)
		}
	}

	t.Run("object file", func(t *testing.T) {
		root := t.TempDir()
		s := open(t, root)
		rel, err := filepath.Rel(filepath.Join(root, objDir), secret)
		if err != nil {
			t.Fatal(err)
		}
		for key, target := range map[string]string{"abs": secret, "rel": rel} {
			if err := os.Symlink(target, filepath.Join(root, objPath(key))); err != nil {
				t.Fatal(err)
			}
			if body, _, err := s.Get(ctx, key); err == nil {
				b, _ := io.ReadAll(body)
				_ = body.Close()
				t.Fatalf("Get(%q) through a symlink out of the root succeeded: %q", key, b)
			}
			if _, err := s.Head(ctx, key); err == nil {
				t.Fatalf("Head(%q) through a symlink out of the root succeeded", key)
			}
			if _, err := s.Replace(ctx, key, "", strings.NewReader("overwritten")); err == nil {
				t.Fatalf("Replace(%q) through a symlink out of the root succeeded", key)
			}
			if _, err := s.Create(ctx, key, strings.NewReader("overwritten")); err == nil {
				t.Fatalf("Create(%q) over a symlink succeeded", key)
			}
			if _, err := s.List(ctx, "", "", 10); err == nil {
				t.Fatal("List with a symlink out of the root succeeded")
			}
			expectOutsideUntouched(t)
			if err := s.Delete(ctx, key); err != nil {
				t.Fatalf("Delete(%q): %v", key, err)
			}
			expectOutsideUntouched(t)
		}
	})

	for _, d := range []string{objDir, tmpDir} {
		t.Run(d+" directory", func(t *testing.T) {
			root := t.TempDir()
			if err := os.Symlink(outside, filepath.Join(root, d)); err != nil {
				t.Fatal(err)
			}
			if s, err := Open(root); err == nil {
				defer func() { _ = s.Close() }()
				if _, err := s.Create(ctx, "k", strings.NewReader("escaped")); err == nil {
					t.Fatal("Create through a symlinked directory succeeded")
				}
				if _, err := s.List(ctx, "", "", 10); err == nil && d == objDir {
					t.Fatal("List through a symlinked directory succeeded")
				}
			}
			expectOutsideUntouched(t)
		})
	}
}
