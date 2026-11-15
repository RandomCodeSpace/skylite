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
	"syscall"
	"testing"
	"testing/iotest"

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
	id := os.Getenv(envID)
	versions := strings.Split(os.Getenv(envVersion), ",")
	for r := range rounds {
		var v string
		switch mode {
		case "create":
			v, err = s.Create(ctx, roundKey(r), strings.NewReader(helperPayload(id)))
		case "replace":
			v, err = s.Replace(ctx, roundKey(r), versions[r], strings.NewReader(helperPayload(id)))
		case "hang":
			_, err = s.Create(ctx, helperKey, &hangingReader{})
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

// hangingReader returns 64 KiB, then on the next read reports "writing" on
// stdout, by which time the store has written that data, and blocks forever.
type hangingReader struct{ calls int }

func (r *hangingReader) Read(p []byte) (int, error) {
	r.calls++
	if r.calls == 1 {
		n := min(len(p), 64<<10)
		copy(p, bytes.Repeat([]byte("h"), n))
		return n, nil
	}
	fmt.Println("writing")
	select {}
}

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
	storagetest.Run(t, func(t *testing.T) storage.Store { return open(t, t.TempDir()) })
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

func TestKilledDuringWrite(t *testing.T) {
	dir := t.TempDir()
	cmd := helperCmd(t, "hang", dir, "0", "")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdin = strings.NewReader("")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if line != "writing\n" {
		_ = cmd.Process.Kill()
		t.Fatalf("helper printed %q, %v; want writing", line, err)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	// The write was in progress: its temporary file is still there.
	tmp, err := os.ReadDir(filepath.Join(dir, tmpDir))
	if err != nil || len(tmp) != 1 {
		t.Fatalf("tmp/ holds %v, %v; want the killed write's file", tmp, err)
	}
	s := open(t, dir)
	ctx := context.Background()
	if _, err := s.Head(ctx, helperKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Head after the killed write error = %v, want ErrNotFound", err)
	}
	if entries, err := s.List(ctx, "", "", 10); err != nil || len(entries) != 0 {
		t.Fatalf("List after the killed write = %v, %v; want empty", entries, err)
	}
	v, err := s.Create(ctx, helperKey, strings.NewReader("after"))
	if err != nil {
		t.Fatalf("Create after the killed write: %v", err)
	}
	expectObject(t, s, helperKey, "after", v)
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
	for _, k := range []string{"../x", "../../x", "a/../../x", "/etc/x", `..\x`, "a/../x"} {
		if _, err := s.Create(ctx, k, strings.NewReader("x")); !errors.Is(err, storage.ErrInvalidKey) {
			t.Errorf("Create(%q) error = %v, want ErrInvalidKey", k, err)
		}
	}
	pathLike := []string{"..a/b", "a/..b", "...", "etc/passwd", "root/..x", "%2e%2e/x"}
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
