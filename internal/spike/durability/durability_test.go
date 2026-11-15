//go:build integration

package durability

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

const (
	helperEnv          = "SKYLITE_SPIKE_HELPER"
	helperDirEnv       = "SKYLITE_SPIKE_HELPER_DIR"
	helperContainerEnv = "SKYLITE_SPIKE_HELPER_CONTAINER"
	helperSeedEnv      = "SKYLITE_SPIKE_HELPER_SEED"
	helperRoundEnv     = "SKYLITE_SPIKE_HELPER_ROUND"

	// killRoundsEnv sets the number of kill rounds; CI uses the default and a
	// local soak sets it higher.
	killRoundsEnv     = "SKYLITE_SPIKE_KILL_ROUNDS"
	defaultKillRounds = 20
	seedEnv           = "SKYLITE_SPIKE_SEED"

	bigRowBytes = 1<<20 + 512<<10
)

func envInt(t testing.TB, name string, def int64) int64 {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

func seededRand(t testing.TB) *rand.Rand {
	t.Helper()
	seed := envInt(t, seedEnv, time.Now().UnixNano())
	t.Logf("seed %d (set %s to replay)", seed, seedEnv)
	return rand.New(rand.NewPCG(uint64(seed), 0x5eed)) //nolint:gosec // seeded so a failing run can be replayed
}

func createSchema(ctx context.Context, l *Leader) error {
	return l.Exec(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY, writer TEXT NOT NULL);
			CREATE TABLE IF NOT EXISTS big (slot INTEGER PRIMARY KEY, data BLOB NOT NULL)`)
		return err
	})
}

func insertRow(ctx context.Context, id int64, writer string, big []byte) func(*sql.Tx) error {
	return func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO rows (id, writer) VALUES (?, ?)`, id, writer); err != nil {
			return err
		}
		if big != nil {
			_, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO big (slot, data) VALUES (?, ?)`, id%4, big)
			return err
		}
		return nil
	}
}

func rowIDs(db *sql.DB) (map[int64]bool, error) {
	rows, err := db.Query(`SELECT id FROM rows`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

func epochOf(db *sql.DB) (int64, error) {
	var e int64
	err := db.QueryRow(`SELECT v FROM spike_meta WHERE k = 'epoch'`).Scan(&e)
	return e, err
}

// l0Gaps reports any break in the L0 sequence. Retention may delete L0
// files that a compaction or snapshot covers, so the sequence must be
// contiguous from its first file and everything before that file covered.
func l0Gaps(ctx context.Context, az *azblob.Client, container string) ([]string, error) {
	c := NewClient(az, container, DBPrefix)
	var covered ltx.TXID
	for _, level := range []int{1, litestream.SnapshotLevel} {
		infos, err := c.list(ctx, level, 0)
		if err != nil {
			return nil, err
		}
		for _, info := range infos {
			covered = max(covered, info.MaxTXID)
		}
	}
	infos, err := c.list(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	var gaps []string
	for i, info := range infos {
		prev := covered
		if i > 0 {
			prev = infos[i-1].MaxTXID
		}
		if info.MinTXID != info.MaxTXID || info.MinTXID > prev+1 || (i > 0 && info.MinTXID != prev+1) {
			gaps = append(gaps, fmt.Sprintf("L0 file %s-%s follows %s", info.MinTXID, info.MaxTXID, prev))
		}
	}
	return gaps, nil
}

// TestHelperWriter is the child process of the kill tests. It takes office
// on an empty directory and writes increasing IDs until it is killed,
// printing "ack <id>" only after Exec has returned nil.
func TestHelperWriter(t *testing.T) {
	mode := os.Getenv(helperEnv)
	if mode == "" {
		t.Skip("helper process for the kill tests")
	}
	ctx := context.Background()
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		os.Exit(3)
	}
	seed, _ := strconv.ParseUint(os.Getenv(helperSeedEnv), 10, 64)
	rng := rand.New(rand.NewPCG(seed, 1)) //nolint:gosec // seeded by the parent so a failing run can be replayed
	az := newAzure(t, nil)

	l, attempts, err := TakeOffice(ctx, az, os.Getenv(helperContainerEnv), os.Getenv(helperDirEnv))
	if err != nil {
		fail("take office: %v", err)
	}
	if err := createSchema(ctx, l); err != nil {
		fail("schema: %v", err)
	}
	var next int64
	if err := l.SQL().QueryRow(`SELECT COALESCE(MAX(id), 0) + 1 FROM rows`).Scan(&next); err != nil {
		fail("next id: %v", err)
	}
	// Catch up on maintenance before writing, as a Store would on its
	// schedule. Without this, kills that land faster than a growing
	// compaction can finish starve it, and every restore re-reads an
	// ever longer L0 tail.
	if _, err := l.db.Compact(ctx, 1); err != nil && !errors.Is(err, litestream.ErrNoCompaction) {
		fail("compact: %v", err)
	}
	if round, _ := strconv.Atoi(os.Getenv(helperRoundEnv)); round%10 == 0 {
		if _, err := l.db.Snapshot(ctx); err != nil {
			fail("snapshot: %v", err)
		}
	}
	fmt.Printf("office epoch=%d next=%d attempts=%d\n", l.Epoch(), next, attempts)

	for id := next; ; id++ {
		var big []byte
		if rng.IntN(4) == 0 {
			big = randomBytes(bigRowBytes)
		}
		fmt.Printf("begin %d big=%t\n", id, big != nil)
		if mode == "ack-early" {
			// Negative control: acknowledge after the local commit and before
			// the upload, which is the bug the kill test must catch.
			tx, err := l.SQL().BeginTx(ctx, nil)
			if err == nil {
				err = insertRow(ctx, id, "w", big)(tx)
			}
			if err == nil {
				err = tx.Commit()
			}
			if err != nil {
				fail("local commit %d: %v", id, err)
			}
			fmt.Printf("ack %d\n", id)
			if err := l.Exec(ctx, func(*sql.Tx) error { return nil }); err != nil {
				fail("sync %d: %v", id, err)
			}
		} else {
			if err := l.Exec(ctx, insertRow(ctx, id, "w", big)); err != nil {
				fail("exec %d: %v", id, err)
			}
			fmt.Printf("ack %d\n", id)
		}
		if id%10 == 0 {
			// Compaction keeps restores short and puts level-1 writes under
			// the kill as well.
			if _, err := l.db.Compact(ctx, 1); err != nil {
				fail("compact: %v", err)
			}
		}
		if id%50 == 0 {
			// Puts snapshot writes under the kill as well.
			if _, err := l.db.Snapshot(ctx); err != nil {
				fail("snapshot: %v", err)
			}
		}
	}
}

type round struct {
	epoch    int64
	acked    []int64
	inflight int64 // the begun but unacknowledged ID when killed, or 0
	big      bool  // inflight write carried a large row
	office   bool
}

// killPolicy says what the parent waits for before the random delay and
// the kill: "" for process start, or a line prefix such as "office" or "ack".
type killPolicy struct {
	after    string
	maxDelay time.Duration
}

func runRound(t *testing.T, container, mode string, index int, rng *rand.Rand, policy killPolicy) round {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperWriter$", "-test.count=1") //nolint:gosec // re-executes this test binary as the helper
	cmd.Env = append(os.Environ(),
		helperEnv+"="+mode,
		helperDirEnv+"="+t.TempDir(),
		helperContainerEnv+"="+container,
		helperSeedEnv+"="+strconv.FormatUint(rng.Uint64(), 10),
		helperRoundEnv+"="+strconv.Itoa(index),
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var lines []string
	seen := make(chan string, 1024)
	eof := make(chan struct{})
	go func() {
		defer close(eof)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			mu.Lock()
			lines = append(lines, sc.Text())
			mu.Unlock()
			select {
			case seen <- sc.Text():
			default:
			}
		}
	}()

	if policy.after != "" {
		timeout := time.After(2 * time.Minute)
	wait:
		for {
			select {
			case line := <-seen:
				if strings.HasPrefix(line, policy.after) {
					break wait
				}
			case <-eof:
				_ = cmd.Wait()
				t.Fatalf("helper exited before %q: %s", policy.after, stderr.String())
			case <-timeout:
				_ = cmd.Process.Kill()
				t.Fatalf("helper did not print %q in time: %s", policy.after, stderr.String())
			}
		}
	}
	if policy.maxDelay > 0 {
		select {
		case <-time.After(time.Duration(rng.Int64N(int64(policy.maxDelay)))):
		case <-eof:
			_ = cmd.Wait()
			t.Fatalf("helper exited on its own: %s", stderr.String())
		}
	}
	_ = cmd.Process.Signal(syscall.SIGKILL)
	<-eof
	err = cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("helper ended with %v, not SIGKILL: %s", err, stderr.String())
	}

	var r round
	for _, line := range lines {
		var id int64
		var big bool
		switch {
		case strings.HasPrefix(line, "office "):
			r.office = true
			if _, err := fmt.Sscanf(line, "office epoch=%d", &r.epoch); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
		case strings.HasPrefix(line, "begin "):
			if _, err := fmt.Sscanf(line, "begin %d big=%t", &id, &big); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			r.inflight, r.big = id, big
		case strings.HasPrefix(line, "ack "):
			if _, err := fmt.Sscanf(line, "ack %d", &id); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			r.acked = append(r.acked, id)
			r.inflight, r.big = 0, false
		}
	}
	return r
}

// checkP1 restores the history on an empty directory and returns every way
// it disagrees with what the rounds acknowledged.
//
// Rule for unacknowledged IDs: an ID that is present but was never
// acknowledged is allowed only if it is the write a round had begun and not
// acknowledged when it was killed. Its upload may have completed before the
// kill landed; nothing else may appear.
func checkP1(ctx context.Context, t *testing.T, az *azblob.Client, container string, rounds []round) []string {
	t.Helper()
	var v []string
	start := time.Now()
	db, err := restoreFresh(ctx, t, az, container)
	if err != nil {
		return []string{fmt.Sprintf("restore: %v", err)}
	}
	var pages, pageSize int64
	_ = db.QueryRow(`PRAGMA page_count`).Scan(&pages)
	_ = db.QueryRow(`PRAGMA page_size`).Scan(&pageSize)
	t.Logf("final restore: %.1f MiB in %s", float64(pages*pageSize)/(1<<20), time.Since(start).Round(time.Millisecond))

	if err := integrityCheck(db); err != nil {
		v = append(v, err.Error())
	}
	present, err := rowIDs(db)
	if err != nil {
		return append(v, err.Error())
	}
	acked := map[int64]bool{}
	inflight := map[int64]bool{}
	for _, r := range rounds {
		for _, id := range r.acked {
			acked[id] = true
		}
		if r.inflight != 0 {
			inflight[r.inflight] = true
		}
	}
	for _, id := range slices.Sorted(maps.Keys(acked)) {
		if !present[id] {
			v = append(v, fmt.Sprintf("acknowledged id %d is missing", id))
		}
	}
	trailing := 0
	for _, id := range slices.Sorted(maps.Keys(present)) {
		switch {
		case acked[id]:
		case inflight[id]:
			trailing++
		default:
			v = append(v, fmt.Sprintf("id %d is present but was never acknowledged nor in flight", id))
		}
	}
	t.Logf("%d acknowledged ids, %d present, %d unacknowledged in-flight writes survived", len(acked), len(present), trailing)

	// Epochs: one bump per round that took office. A round killed after its
	// bump landed but before it printed leaves a jump the next round shows.
	final, err := epochOf(db)
	if err != nil {
		return append(v, fmt.Sprintf("epoch: %v", err))
	}
	var last int64
	lastRound := -1
	for i, r := range rounds {
		if !r.office {
			continue
		}
		if r.epoch <= last || r.epoch-last > int64(i-lastRound) {
			v = append(v, fmt.Sprintf("round %d epoch %d after epoch %d in round %d", i, r.epoch, last, lastRound))
		}
		last, lastRound = r.epoch, i
	}
	if final < last || final > int64(len(rounds)) {
		v = append(v, fmt.Sprintf("final epoch %d, last printed %d, %d rounds", final, last, len(rounds)))
	}

	gaps, err := l0Gaps(ctx, az, container)
	if err != nil {
		return append(v, err.Error())
	}
	return append(v, gaps...)
}

func runKillTest(t *testing.T, mode string, rounds int, policy func(*rand.Rand) killPolicy) []string {
	ctx := context.Background()
	rng := seededRand(t)
	az := newAzure(t, nil)
	container := newContainer(t, az)

	var results []round
	var bigKills, officeKills int
	for i := range rounds {
		r := runRound(t, container, mode, i, rng, policy(rng))
		if r.big {
			bigKills++
		}
		if !r.office {
			officeKills++
		}
		results = append(results, r)
	}
	t.Logf("%d rounds: %d killed before taking office was reported, %d killed during a large write", rounds, officeKills, bigKills)
	return checkP1(ctx, t, az, container, results)
}

// TestP1KillRestore kills a writer with SIGKILL at random moments, each
// round on a new empty directory, and checks that a restore holds every
// acknowledged write.
func TestP1KillRestore(t *testing.T) {
	azuriteEndpoint(t)
	rounds := int(envInt(t, killRoundsEnv, defaultKillRounds))
	violations := runKillTest(t, "writer", rounds, func(rng *rand.Rand) killPolicy {
		// A quarter of the kills count from process start, so they can land
		// in the restore or the epoch bump; the rest land among the writes.
		if rng.IntN(4) == 0 {
			return killPolicy{maxDelay: 400 * time.Millisecond}
		}
		return killPolicy{after: "office", maxDelay: time.Second}
	})
	for _, v := range violations {
		t.Error(v)
	}
}

// TestP1NegativeControl acknowledges before the upload and kills right
// after an acknowledgement. The P1 checks must report lost writes.
func TestP1NegativeControl(t *testing.T) {
	azuriteEndpoint(t)
	violations := runKillTest(t, "ack-early", 5, func(*rand.Rand) killPolicy {
		return killPolicy{after: "ack"}
	})
	lost := 0
	for _, v := range violations {
		if strings.Contains(v, "is missing") {
			lost++
		}
	}
	if lost == 0 {
		t.Fatalf("acknowledging before upload lost nothing; the P1 checks cannot see a lost write: %v", violations)
	}
	t.Logf("negative control: %d acknowledged writes lost, as expected", lost)
}

type staleCase struct {
	pendingUpload    bool // A committed and built its L0 file but had not uploaded it when paused
	resetPosition    bool // A's replica position was reset by an earlier failed sync
	maintenanceFirst bool // on resume A runs compaction, snapshot and retention before writing
	unsafe           bool // negative control: A's client has no fencing at all
}

// staleLeaderScenario pauses leader A, lets B take office, resumes A and
// returns every P2 violation it finds.
//
// A is held in-process rather than with SIGSTOP: with the monitors off,
// Litestream runs no goroutine of its own, so a leader the test does not
// call is exactly a paused process as far as storage can tell, and the test
// can then drive the compaction, snapshot and retention calls a store would
// make on A's behalf.
func staleLeaderScenario(t *testing.T, sc staleCase) []string {
	ctx := context.Background()
	az := newAzure(t, nil)
	container := newContainer(t, az)
	var v []string

	leaseA, err := AcquireLease(ctx, az, container, "a")
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := TakeOffice(ctx, az, container, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close(ctx) }()
	if err := createSchema(ctx, a); err != nil {
		t.Fatal(err)
	}
	want := map[int64]bool{}
	for id := int64(1); id <= 5; id++ {
		if err := a.Exec(ctx, insertRow(ctx, id, "a", nil)); err != nil {
			t.Fatal(err)
		}
		want[id] = true
	}
	if sc.pendingUpload {
		if err := commitLocal(ctx, a, insertRow(ctx, 6, "a", nil)); err != nil {
			t.Fatal(err)
		}
		if err := a.db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if sc.resetPosition {
		a.db.Replica.SetPos(ltx.Pos{})
	}
	a.client.unsafe = sc.unsafe

	// A is paused. B takes the lease and office and acknowledges writes.
	if _, err := AcquireLease(ctx, az, container, "b"); err != nil {
		t.Fatal(err)
	}
	b, _, err := TakeOffice(ctx, az, container, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close(ctx) }()
	for id := int64(101); id <= 105; id++ {
		if err := b.Exec(ctx, insertRow(ctx, id, "b", nil)); err != nil {
			t.Fatal(err)
		}
		want[id] = true
	}
	// An L1 file gives A's L0 retention something it could delete.
	if _, err := b.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	before := listing(t, az, container)

	// A resumes.
	writes := func() {
		for id := int64(201); id <= 204; id++ {
			err := a.Exec(ctx, insertRow(ctx, id, "a", nil))
			if err == nil {
				v = append(v, fmt.Sprintf("A acknowledged post-takeover write %d", id))
			} else if !sc.unsafe && !errors.Is(err, ErrFenced) {
				v = append(v, fmt.Sprintf("A write %d failed with %v, want ErrFenced", id, err))
			}
		}
		// Litestream's own retry path, bypassing the leader's latch check.
		if err := a.db.SyncAndWait(ctx); err == nil {
			t.Logf("A: Litestream SyncAndWait returned nil at local position %s with uploaded head %s", mustPos(a), a.client.Head())
		}
	}
	// The calls a litestream.Store makes on its schedule, snapshot first so
	// that a snapshot ending at A's own head is tried before anything else
	// has fenced A.
	maintenance := func() {
		_, errS := a.db.Snapshot(ctx)
		minTXID, errR := a.db.EnforceSnapshotRetention(ctx, time.Now())
		var errT error
		for _, level := range []int{0, 1} {
			errT = errors.Join(errT, a.db.EnforceRetentionByTXID(ctx, level, minTXID))
		}
		a.db.L0Retention = time.Nanosecond
		errL := a.db.EnforceL0RetentionByTime(ctx)
		_, errC := a.db.Compact(ctx, 1)
		t.Logf("A maintenance: snapshot=%v snapshot-retention=%v txid-retention=%v l0-retention=%v compact=%v", errS, errR, errT, errL, errC)
	}
	if sc.resetPosition {
		// Litestream's own view of a write after a position reset.
		if err := commitLocal(ctx, a, insertRow(ctx, 200, "a", nil)); err != nil {
			t.Fatal(err)
		}
		if err := a.db.SyncAndWait(ctx); err == nil {
			t.Logf("A: SyncAndWait returned nil for write 200 at local position %v although client head is %s; Litestream uploaded nothing", mustPos(a), a.client.Head())
		}
	}
	if sc.maintenanceFirst {
		maintenance()
		writes()
	} else {
		writes()
		maintenance()
	}
	if err := leaseA.Renew(ctx, az, container); err == nil {
		v = append(v, "A renewed a lease B had taken over")
	}
	if !sc.unsafe && a.Fenced() == nil {
		v = append(v, "A is not fenced")
	}

	after := listing(t, az, container)
	for key, etag := range before {
		if got, ok := after[key]; !ok {
			v = append(v, fmt.Sprintf("%s was deleted", key))
		} else if got != etag {
			v = append(v, fmt.Sprintf("%s was overwritten", key))
		}
	}
	for key := range after {
		if _, ok := before[key]; !ok {
			v = append(v, fmt.Sprintf("%s was added by A", key))
		}
	}

	// B is unharmed and can keep writing.
	if err := b.Exec(ctx, insertRow(ctx, 106, "b", nil)); err != nil {
		v = append(v, fmt.Sprintf("B write after A resumed: %v", err))
	} else {
		want[106] = true
	}
	return append(v, checkExactRestore(ctx, t, az, container, want, 2)...)
}

// checkExactRestore restores on an empty directory and compares the row IDs
// and epoch with what was acknowledged.
func checkExactRestore(ctx context.Context, t *testing.T, az *azblob.Client, container string, want map[int64]bool, epoch int64) []string {
	t.Helper()
	db, err := restoreFresh(ctx, t, az, container)
	if err != nil {
		return []string{fmt.Sprintf("restore: %v", err)}
	}
	var v []string
	if err := integrityCheck(db); err != nil {
		v = append(v, err.Error())
	}
	got, err := rowIDs(db)
	if err != nil {
		return append(v, err.Error())
	}
	for id := range want {
		if !got[id] {
			v = append(v, fmt.Sprintf("acknowledged id %d missing from restore", id))
		}
	}
	for id := range got {
		if !want[id] {
			v = append(v, fmt.Sprintf("unacknowledged id %d present in restore", id))
		}
	}
	if e, err := epochOf(db); err != nil || e != epoch {
		v = append(v, fmt.Sprintf("restored epoch %d (%v), want %d", e, err, epoch))
	}
	gaps, err := l0Gaps(ctx, az, container)
	if err != nil {
		return append(v, err.Error())
	}
	return append(v, gaps...)
}

func commitLocal(ctx context.Context, l *Leader, fn func(*sql.Tx) error) error {
	tx, err := l.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func mustPos(l *Leader) ltx.TXID {
	pos, _ := l.db.Pos()
	return pos.TXID
}

func TestP2StaleLeaderCannotCommit(t *testing.T) {
	azuriteEndpoint(t)
	for name, sc := range map[string]staleCase{
		"WritesFirstWithPendingUpload": {pendingUpload: true},
		"MaintenanceFirst":             {maintenanceFirst: true},
		"MaintenanceFirstWithPending":  {pendingUpload: true, maintenanceFirst: true},
		"PositionResetAfterError":      {resetPosition: true},
	} {
		t.Run(name, func(t *testing.T) {
			for _, v := range staleLeaderScenario(t, sc) {
				t.Error(v)
			}
		})
	}
}

// TestP2NegativeControl runs the stale-leader scenario with A's fencing
// switched off. The P2 checks must report the damage.
func TestP2NegativeControl(t *testing.T) {
	azuriteEndpoint(t)
	v := staleLeaderScenario(t, staleCase{pendingUpload: true, unsafe: true})
	if len(v) == 0 {
		t.Fatal("an unfenced stale leader did no detectable damage; the P2 checks are blind")
	}
	for _, line := range v {
		t.Logf("negative control: %s", line)
	}
}

// TestTakeoverRace lets B take office while A is live and writing.
//
// Even iterations model a leader that renews its lease every renewEvery and
// stops writing once the renewal fails; B must then win within a few
// attempts. Odd iterations model a leader that ignores the lease and writes
// back to back for ignoreFor: B loses slots for as long as A writes, which
// is recorded rather than asserted, because the lease is what provides
// liveness. Safety is asserted in both.
func TestTakeoverRace(t *testing.T) {
	azuriteEndpoint(t)
	ctx := context.Background()
	rng := seededRand(t)
	az := newAzure(t, nil)
	iterations := int(envInt(t, "SKYLITE_SPIKE_RACE_ROUNDS", 3))
	// Long enough that A keeps writing through B's first attempts.
	const renewEvery = 250 * time.Millisecond
	// Bounded because an unbounded writer also grows an uncompacted L0 tail
	// that makes each of B's restores slower; in CI that ran to 14,000
	// files and Azurite stopped accepting connections.
	const ignoreFor = 3 * time.Second
	for i := range iterations {
		honourLease := i%2 == 0
		container := newContainer(t, az)
		leaseA, err := AcquireLease(ctx, az, container, "a")
		if err != nil {
			t.Fatal(err)
		}
		a, _, err := TakeOffice(ctx, az, container, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := createSchema(ctx, a); err != nil {
			t.Fatal(err)
		}

		var ackedA []int64
		var aErr error
		aDone := make(chan struct{})
		var stop atomic.Bool
		var aStopped time.Time
		go func() {
			defer close(aDone)
			defer func() { aStopped = time.Now() }()
			renewed := time.Now()
			for id := int64(1); !stop.Load(); id++ {
				if !honourLease && time.Since(renewed) > ignoreFor {
					return
				}
				if honourLease && time.Since(renewed) > renewEvery {
					if aErr = leaseA.Renew(ctx, az, container); aErr != nil {
						return
					}
					renewed = time.Now()
				}
				if aErr = a.Exec(ctx, insertRow(ctx, id, "a", nil)); aErr != nil {
					return
				}
				ackedA = append(ackedA, id)
			}
		}()

		time.Sleep(50*time.Millisecond + time.Duration(rng.Int64N(int64(200*time.Millisecond))))
		// A renewal can win the CAS between B's read and write; B retries.
		for {
			_, err := AcquireLease(ctx, az, container, "b")
			if err == nil {
				break
			}
			if !bloberror.HasCode(err, bloberror.ConditionNotMet) {
				t.Fatal(err)
			}
		}
		b, attempts, errB := TakeOffice(ctx, az, container, t.TempDir())
		bOffice := time.Now()
		want := map[int64]bool{}
		epoch := int64(2)
		switch {
		case errB != nil && honourLease:
			stop.Store(true)
			<-aDone
			t.Fatalf("iteration %d: B could not take office although A honours the lease: %v", i, errB)
		case errB != nil:
			stop.Store(true)
			<-aDone
			t.Logf("iteration %d: A ignored the lease and acknowledged %d writes; B gave up after %d attempts: %v", i, len(ackedA), attempts, errB)
			epoch = 1
		default:
			for id := int64(1_000_001); id <= 1_000_020; id++ {
				if err := b.Exec(ctx, insertRow(ctx, id, "b", nil)); err != nil {
					t.Fatalf("iteration %d: B write %d: %v", i, id, err)
				}
				want[id] = true
			}
			select {
			case <-aDone:
			case <-time.After(30 * time.Second):
				stop.Store(true)
				<-aDone
				t.Errorf("iteration %d: A kept acknowledging writes after B took office", i)
			}
			if err := a.Exec(ctx, insertRow(ctx, 999_999, "a", nil)); !errors.Is(err, ErrFenced) {
				t.Errorf("iteration %d: A write after B took office = %v, want ErrFenced", i, err)
			}
			_ = b.Close(ctx)
			how := "by losing the lease"
			switch {
			case errors.Is(aErr, ErrFenced):
				how = "fenced by B's history"
			case !honourLease:
				how = fmt.Sprintf("after ignoring the lease for %s; B was in office %s after A stopped", ignoreFor, bOffice.Sub(aStopped).Round(time.Millisecond))
			}
			t.Logf("iteration %d: A acknowledged %d writes and stopped %s; B took office in %d attempt(s)", i, len(ackedA), how, attempts)
		}
		for _, id := range ackedA {
			want[id] = true
		}
		for _, v := range checkExactRestore(ctx, t, az, container, want, epoch) {
			t.Errorf("iteration %d: %s", i, v)
		}
		_ = a.Close(ctx)
	}
}

// TestMeasure reports, without asserting, the acknowledged-write latency and
// storage requests per commit against Azurite, and restore time. Emulator
// timings say nothing about real Azure.
func TestMeasure(t *testing.T) {
	ctx := context.Background()
	var requests atomic.Int64
	az := newAzure(t, &requests)
	container := newContainer(t, az)
	l, _, err := TakeOffice(ctx, az, container, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close(ctx) }()
	if err := createSchema(ctx, l); err != nil {
		t.Fatal(err)
	}

	const n = 200
	lat := make([]time.Duration, 0, n)
	before := requests.Load()
	for id := int64(1); id <= n; id++ {
		start := time.Now()
		if err := l.Exec(ctx, insertRow(ctx, id, "m", nil)); err != nil {
			t.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	perCommit := float64(requests.Load()-before) / n
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	q := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))].Round(10 * time.Microsecond) }
	t.Logf("%d small commits: p50 %s p90 %s p99 %s max %s; %.2f storage requests per acknowledged commit", n, q(.5), q(.9), q(.99), lat[len(lat)-1].Round(10*time.Microsecond), perCommit)

	before = requests.Load()
	start := time.Now()
	if err := l.Exec(ctx, insertRow(ctx, n+1, "m", randomBytes(bigRowBytes))); err != nil {
		t.Fatal(err)
	}
	t.Logf("one commit with a %d KiB row: %s, %d storage requests", bigRowBytes>>10, time.Since(start).Round(10*time.Microsecond), requests.Load()-before)

	// Grow the database, then time restores from L0 alone and after
	// compaction. CI keeps it small; set SKYLITE_SPIKE_MEASURE_MIB to grow it.
	for i := range envInt(t, "SKYLITE_SPIKE_MEASURE_MIB", 16) / 4 {
		if err := l.Exec(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO big (slot, data) VALUES (?, ?)`, 100+i, randomBytes(4<<20))
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	timeRestore := func(label string) {
		start := time.Now()
		db, err := restoreFresh(ctx, t, az, container)
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		var pages, pageSize int64
		_ = db.QueryRow(`PRAGMA page_count`).Scan(&pages)
		_ = db.QueryRow(`PRAGMA page_size`).Scan(&pageSize)
		t.Logf("restore %s: %.1f MiB in %s", label, float64(pages*pageSize)/(1<<20), elapsed.Round(time.Millisecond))
	}
	timeRestore("from L0 files only")
	if _, err := l.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	timeRestore("after compaction to L1")
}
