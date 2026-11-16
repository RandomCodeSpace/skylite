package engine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/localfs"
)

// crashEnv carries the store directory, the local directory and the step
// at which the child of TestCrash stops, joined by "|".
const crashEnv = "SKYLITE_CRASH_CHILD"

// crashTarget is the transaction of the child that is killed: 1 creates
// the table, the others insert row i.
const crashTarget = 4

// crashStore pauses the child forever at a step of the target commit's
// create, or fails every create of it with no effect.
type crashStore struct {
	storage.Store
	step  string
	armed atomic.Bool
	pause func(string)
}

func (s *crashStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	if !isCommit(key) || !s.armed.Load() {
		return s.Store.Create(ctx, key, body)
	}
	switch s.step {
	case "before upload":
		s.pause(s.step)
	case "during upload":
		return s.Store.Create(ctx, key, &pausingReader{r: body, pause: func() { s.pause(s.step) }})
	case "after accepted":
		v, err := s.Store.Create(ctx, key, body)
		if err != nil {
			return v, err
		}
		s.pause(s.step)
	case "before truncate", "before truncate sync", "after not committed":
		_, _ = io.Copy(io.Discard, body)
		return "", &storage.Error{Op: "create", Key: key, Err: errNoEffect}
	}
	return s.Store.Create(ctx, key, body)
}

// pausingReader returns a few bytes, then pauses on the next read.
type pausingReader struct {
	r     io.Reader
	pause func()
	n     int
}

func (p *pausingReader) Read(b []byte) (int, error) {
	p.n++
	if p.n > 1 {
		p.pause()
	}
	return p.r.Read(b[:min(len(b), 100)])
}

// TestCrashChild is the process TestCrash kills. It commits transactions
// one at a time, printing "ack <i> <txn>" for each acknowledged one, and at
// transaction crashTarget prints "at <step>" and waits to be killed.
func TestCrashChild(t *testing.T) {
	env := os.Getenv(crashEnv)
	if env == "" {
		t.Skip("run by TestCrash")
	}
	args := strings.Split(env, "|")
	storeDir, local, step := args[0], args[1], args[2]
	fs, err := localfs.Open(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := format.NewKeys("dbs/test")
	if err != nil {
		t.Fatal(err)
	}
	pause := func(where string) {
		fmt.Printf("at %s\n", where)
		select {}
	}
	st := &crashStore{Store: fs, step: step, pause: pause}
	l, err := Lead(context.Background(), st, keys, local, takeOffice(t, fs, keys), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	c, err := l.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; ; i++ {
		target := i == crashTarget
		if target {
			st.armed.Store(true)
			switch step {
			case "after response":
				l.afterUpload = func() { pause(step) }
			case "before truncate", "before truncate sync":
				op := map[string]string{"before truncate": "truncate", "before truncate sync": "sync"}[step]
				var failed atomic.Bool
				l.fault = func(file, o string, off int64, n int) error {
					if file == "wal" && o == "truncate" {
						failed.Store(true)
					}
					if file == "wal" && o == op && failed.Load() {
						pause(step)
					}
					return nil
				}
			}
		}
		sql := fmt.Sprintf("INSERT INTO t VALUES (%d, printf('%%0800d', %d))", i, i)
		if i == 1 {
			sql = "CREATE TABLE t(k INTEGER PRIMARY KEY, v)"
		}
		txn, err := c.Run(context.Background(), fmt.Appendf(nil, "crash-%018d", i), func(db *sqlite3.Conn) error { return db.Exec(sql) })
		if target && step == "after not committed" && errors.Is(err, ErrNotCommitted) {
			pause(step)
		}
		if err != nil {
			t.Fatalf("transaction %d: %v", i, err)
		}
		fmt.Printf("ack %d %d\n", i, txn)
	}
}

// Covers: Commit capture and failure, "A process is killed at every step of
// a commit: before the upload, during it, after storage has accepted it but
// before the response, and after the response but before COMMIT returns.
// Every acknowledged write survives. A commit that storage accepted but
// that was never acknowledged may appear in the history, and if it does,
// it appears whole"; "A commit known not to have entered the durable
// history never reappears, after a crash ... and a crash at each step of
// the truncation"; and "A real process crash, not a simulated one, leaves
// the shared-memory file in place, and the restarted node still wipes and
// ignores it". The child is killed with SIGKILL; the restarted leader uses
// the same local directory and starts from object storage.
func TestCrash(t *testing.T) {
	if os.Getenv(crashEnv) != "" {
		t.Skip("in the child")
	}
	for _, step := range []string{"before upload", "during upload", "after accepted", "after response", "before truncate", "before truncate sync", "after not committed"} {
		t.Run(step, func(t *testing.T) {
			st, keys, _ := newDatabase(t, 4096)
			local := t.TempDir()
			cmd := osexec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1") //nolint:gosec // the test binary itself
			cmd.Env = append(os.Environ(), crashEnv+"="+st.dir+"|"+local+"|"+step)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			var acked []int
			var all []string
			sc := bufio.NewScanner(out)
			reached := false
			for !reached && sc.Scan() {
				line := sc.Text()
				all = append(all, line)
				if f := strings.Fields(line); len(f) == 3 && f[0] == "ack" {
					i, _ := strconv.Atoi(f[1])
					acked = append(acked, i)
				}
				reached = line == "at "+step
			}
			_ = cmd.Process.Signal(syscall.SIGKILL)
			_ = cmd.Wait()
			if !reached {
				t.Fatalf("the child did not reach %q:\n%s\n%s", step, strings.Join(all, "\n"), stderr.String())
			}
			if want := []int{1, 2, 3}; !slices.Equal(acked, want) {
				t.Fatalf("acknowledged %v, want %v", acked, want)
			}

			shms, _ := filepath.Glob(filepath.Join(local, "*", "*-shm"))
			if len(shms) != 1 {
				t.Fatalf("shared-memory files after the kill: %v", shms)
			}
			// Inode numbers are reused, so a marker beside the stale file
			// shows whether the restart wiped the directory.
			marker := filepath.Join(filepath.Dir(shms[0]), "marker")
			if err := os.WriteFile(marker, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			l, err := Lead(context.Background(), st, keys, local, takeOffice(t, st.Store, keys), testConfig())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the restarted leader kept the local directory with the stale shared-memory file: %v", err)
			}
			c := open(t, l)

			want := "2,3"
			landed := step == "after accepted" || step == "after response"
			if landed {
				want = "2,3,4"
			}
			check := func(db *sqlite3.Conn) {
				t.Helper()
				if got := strings.Join(mustQuery(t, db, "SELECT k FROM t ORDER BY k"), ","); got != want {
					t.Fatalf("rows %s, want %s", got, want)
				}
				if bad := mustQuery(t, db, "SELECT count(*) FROM t WHERE v != printf('%0800d', k)"); bad[0] != "0" {
					t.Fatalf("%s rows not whole", bad[0])
				}
			}
			check(durable(t, st.Store, keys, 0))
			if _, err := c.Run(context.Background(), nil, func(db *sqlite3.Conn) error { check(db); return nil }); err != nil {
				t.Fatal(err)
			}
			mustRun(t, c, "INSERT INTO t VALUES (100, 'after the restart')")
			if got := mustQuery(t, durable(t, st.Store, keys, 0), "SELECT count(*) FROM t WHERE k = 100"); got[0] != "1" {
				t.Fatal("no write after the restart")
			}
		})
	}
}
