//go:build integration

package durability

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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

// roundIDs spaces the record IDs of kill rounds: round i writes
// (i+1)*roundIDs + seq, so an ID never depends on restored state and is never
// reused by a later round.
const roundIDs = 1_000_000

// tagBytes is the size of the content every record carries.
const tagBytes = 16

// record is what a restored row must hold for its ID.
type record struct {
	writer string
	big    bool
}

// payload derives n bytes from a record's identity, so restored content can
// be checked against the ID and writer alone.
func payload(writer string, id int64, n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8(sha256.Sum256(fmt.Appendf(nil, "%s/%d", writer, id))).Read(b)
	return b
}

func createSchema(ctx context.Context, l *Leader) error {
	return l.Exec(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY, writer TEXT NOT NULL, tag BLOB NOT NULL, big INTEGER NOT NULL);
			CREATE TABLE IF NOT EXISTS big (slot INTEGER PRIMARY KEY, id INTEGER NOT NULL, writer TEXT NOT NULL, data BLOB NOT NULL)`)
		return err
	})
}

// insertRow writes record id. A big record also replaces big slot id%4 with
// bigRowBytes derived from its identity, so the database stays small.
func insertRow(ctx context.Context, id int64, writer string, big bool) func(*sql.Tx) error {
	return func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO rows (id, writer, tag, big) VALUES (?, ?, ?, ?)`, id, writer, payload(writer, id, tagBytes), big); err != nil {
			return err
		}
		if big {
			_, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO big (slot, id, writer, data) VALUES (?, ?, ?, ?)`, id%4, id, writer, payload(writer, id, bigRowBytes))
			return err
		}
		return nil
	}
}

// readRecords returns every restored record and every way restored content
// disagrees with the identities: a tag that does not match its row, or a big
// slot that does not hold the bytes of the highest-numbered big record
// mapping to it.
func readRecords(db *sql.DB) (map[int64]record, []string, error) {
	got := map[int64]record{}
	var bad []string
	rows, err := db.Query(`SELECT id, writer, tag, big FROM rows`)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	latest := map[int64]int64{}
	for rows.Next() {
		var id int64
		var r record
		var tag []byte
		if err := rows.Scan(&id, &r.writer, &tag, &r.big); err != nil {
			return nil, nil, err
		}
		if !bytes.Equal(tag, payload(r.writer, id, tagBytes)) {
			bad = append(bad, fmt.Sprintf("record %d by %s: content does not match its identity", id, r.writer))
		}
		got[id] = r
		if r.big {
			latest[id%4] = max(latest[id%4], id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	slots, err := db.Query(`SELECT slot, id, writer, data FROM big WHERE slot < 4`)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = slots.Close() }()
	seen := map[int64]bool{}
	for slots.Next() {
		var slot, id int64
		var writer string
		var data []byte
		if err := slots.Scan(&slot, &id, &writer, &data); err != nil {
			return nil, nil, err
		}
		seen[slot] = true
		if !bytes.Equal(data, payload(writer, id, bigRowBytes)) {
			bad = append(bad, fmt.Sprintf("big slot %d: data does not match record %d by %s", slot, id, writer))
		}
		if id != latest[slot] || got[id].writer != writer {
			bad = append(bad, fmt.Sprintf("big slot %d holds record %d by %s, want latest big record %d", slot, id, writer, latest[slot]))
		}
	}
	for slot, id := range latest {
		if !seen[slot] {
			bad = append(bad, fmt.Sprintf("big slot %d is empty, want record %d", slot, id))
		}
	}
	return got, bad, slots.Err()
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

// holdPrefix is how many records a hold-mode child acknowledges before it
// starts the upload it holds; the first carries a large row, so the held
// compaction and snapshot files are large as well.
const holdPrefix = 3

// holdLevels maps the hold modes to the LTX level whose upload is held.
var holdLevels = map[string]int{"hold-l0": 0, "hold-l1": 1, "hold-snapshot": litestream.SnapshotLevel}

// TestHelperWriter is the child process of the kill tests. It takes office
// on an empty directory and writes records until it is killed, printing
// "ack <id>" only after Exec has returned nil. Round i writes IDs
// (i+1)*roundIDs + 1, + 2, and so on.
//
// Modes: "writer" writes until killed; "ack-early" acknowledges its first
// record after the local commit and then waits, before any upload, to be
// killed; the hold modes acknowledge holdPrefix records, start one upload at
// their level, print "uploading" once half of its body has been read by the
// HTTP transport, and wait there to be killed.
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
	index, err := strconv.Atoi(os.Getenv(helperRoundEnv))
	if err != nil {
		fail("round: %v", err)
	}
	az := newAzure(t, nil)

	l, attempts, err := TakeOffice(ctx, az, os.Getenv(helperContainerEnv), os.Getenv(helperDirEnv))
	if err != nil {
		fail("take office: %v", err)
	}
	if err := createSchema(ctx, l); err != nil {
		fail("schema: %v", err)
	}
	// Records already in the history. This only schedules maintenance;
	// record IDs never depend on restored state.
	var written int64
	if err := l.SQL().QueryRow(`SELECT COUNT(*) FROM rows`).Scan(&written); err != nil {
		fail("count: %v", err)
	}
	// Catch up on maintenance before writing, as a Store would on its
	// schedule. Without this, kills that land faster than a growing
	// compaction can finish starve it, and every restore re-reads an
	// ever longer L0 tail.
	if _, err := l.db.Compact(ctx, 1); err != nil && !errors.Is(err, litestream.ErrNoCompaction) {
		fail("compact: %v", err)
	}
	if index%10 == 0 {
		if _, err := l.db.Snapshot(ctx); err != nil {
			fail("snapshot: %v", err)
		}
	}
	fmt.Printf("office epoch=%d written=%d attempts=%d\n", l.Epoch(), written, attempts)

	write := func(seq int64, big bool) {
		id := int64(index+1)*roundIDs + seq
		fmt.Printf("begin %d big=%t\n", id, big)
		if mode == "ack-early" {
			// Negative control: acknowledge after the local commit and before
			// the upload, which is the bug the kill test must catch.
			if err := commitLocal(ctx, l, insertRow(ctx, id, "w", big)); err != nil {
				fail("local commit %d: %v", id, err)
			}
			fmt.Printf("ack %d\n", id)
			time.Sleep(time.Hour)
			fail("ack-early helper was not killed")
		}
		if err := l.Exec(ctx, insertRow(ctx, id, "w", big)); err != nil {
			fail("exec %d: %v", id, err)
		}
		fmt.Printf("ack %d\n", id)
	}

	if level, ok := holdLevels[mode]; ok {
		for seq := int64(1); seq <= holdPrefix; seq++ {
			write(seq, seq == 1)
		}
		l.client.uploadBody = func(lv int, minTXID, maxTXID ltx.TXID) {
			if lv == level {
				fmt.Printf("uploading level=%d %s-%s\n", lv, minTXID, maxTXID)
				time.Sleep(time.Hour)
			}
		}
		switch level {
		case 0:
			write(holdPrefix+1, true)
		case 1:
			_, err = l.db.Compact(ctx, 1)
		default:
			_, err = l.db.Snapshot(ctx)
		}
		fail("level %d upload was not held (err %v)", level, err)
	}

	for seq := int64(1); ; seq++ {
		write(seq, rng.IntN(4) == 0)
		n := written + seq
		if n%10 == 0 {
			// Compaction keeps restores short and puts level-1 writes under
			// the kill as well.
			if _, err := l.db.Compact(ctx, 1); err != nil {
				fail("compact: %v", err)
			}
		}
		if n%50 == 0 {
			// Puts snapshot writes under the kill as well.
			if _, err := l.db.Snapshot(ctx); err != nil {
				fail("snapshot: %v", err)
			}
		}
	}
}

type round struct {
	epoch    int64
	acked    map[int64]bool // acknowledged IDs, true if the record was big
	inflight int64          // the begun but unacknowledged ID when killed, or 0
	big      bool           // inflight write carried a large row
	office   bool
	held     string // the "uploading" line of a hold-mode child
}

// killPolicy says what the parent waits for before the random delay and
// the kill: "" for process start, or a line prefix such as "office", "ack"
// or "uploading".
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

	r := round{acked: map[int64]bool{}}
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
			if id != r.inflight {
				t.Fatalf("ack %d without its begin line", id)
			}
			r.acked[id] = r.big
			r.inflight, r.big = 0, false
		case strings.HasPrefix(line, "uploading "):
			r.held = line
		}
	}
	return r
}

// p1Result is what a restore on an empty directory held against what the
// rounds acknowledged.
type p1Result struct {
	present    map[int64]record
	missing    map[int64]bool // acknowledged IDs absent from the restore
	violations []string       // every other disagreement
}

// checkP1 restores the history on an empty directory and compares it with
// what the rounds acknowledged. It fails the test if the restore itself
// fails.
//
// Rule for unacknowledged records: a record that is present but was never
// acknowledged is allowed only if it is the write a round had begun and not
// acknowledged when it was killed, with the content that write carried. Its
// upload may have completed before the kill landed; nothing else may appear.
func checkP1(ctx context.Context, t *testing.T, az *azblob.Client, container string, rounds []round) p1Result {
	t.Helper()
	start := time.Now()
	db, err := restoreFresh(ctx, t, az, container)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	var pages, pageSize int64
	_ = db.QueryRow(`PRAGMA page_count`).Scan(&pages)
	_ = db.QueryRow(`PRAGMA page_size`).Scan(&pageSize)
	t.Logf("final restore: %.1f MiB in %s", float64(pages*pageSize)/(1<<20), time.Since(start).Round(time.Millisecond))

	res := p1Result{missing: map[int64]bool{}}
	if err := integrityCheck(db); err != nil {
		res.violations = append(res.violations, err.Error())
	}
	present, bad, err := readRecords(db)
	if err != nil {
		t.Fatalf("read restored records: %v", err)
	}
	res.present = present
	res.violations = append(res.violations, bad...)

	acked := map[int64]record{}
	inflight := map[int64]record{}
	for _, r := range rounds {
		for id, big := range r.acked {
			acked[id] = record{writer: "w", big: big}
		}
		if r.inflight != 0 {
			inflight[r.inflight] = record{writer: "w", big: r.big}
		}
	}
	for _, id := range slices.Sorted(maps.Keys(acked)) {
		got, ok := present[id]
		switch {
		case !ok:
			res.missing[id] = true
			res.violations = append(res.violations, fmt.Sprintf("acknowledged record %d is missing", id))
		case got != acked[id]:
			res.violations = append(res.violations, fmt.Sprintf("acknowledged record %d restored as %+v, want %+v", id, got, acked[id]))
		}
	}
	trailing := 0
	for _, id := range slices.Sorted(maps.Keys(present)) {
		if _, ok := acked[id]; ok {
			continue
		}
		if want, ok := inflight[id]; ok && present[id] == want {
			trailing++
			continue
		}
		res.violations = append(res.violations, fmt.Sprintf("record %d %+v is present but was never acknowledged nor in flight", id, present[id]))
	}
	t.Logf("%d acknowledged records, %d present, %d unacknowledged in-flight writes survived", len(acked), len(present), trailing)

	// Epochs: one bump per round that took office. A round killed after its
	// bump landed but before it printed leaves a jump the next round shows.
	final, err := epochOf(db)
	if err != nil {
		t.Fatalf("epoch: %v", err)
	}
	var last int64
	lastRound := -1
	for i, r := range rounds {
		if !r.office {
			continue
		}
		if r.epoch <= last || r.epoch-last > int64(i-lastRound) {
			res.violations = append(res.violations, fmt.Sprintf("round %d epoch %d after epoch %d in round %d", i, r.epoch, last, lastRound))
		}
		last, lastRound = r.epoch, i
	}
	if final < last || final > int64(len(rounds)) {
		res.violations = append(res.violations, fmt.Sprintf("final epoch %d, last printed %d, %d rounds", final, last, len(rounds)))
	}

	gaps, err := l0Gaps(ctx, az, container)
	if err != nil {
		t.Fatalf("l0 gaps: %v", err)
	}
	res.violations = append(res.violations, gaps...)
	return res
}

func runKillTest(t *testing.T, mode string, rounds int, policy func(*rand.Rand) killPolicy) ([]round, p1Result) {
	ctx := context.Background()
	rng := seededRand(t)
	az := newAzure(t, nil)
	container := newContainer(t, az)

	var results []round
	var acked, bigKills, officeKills int
	for i := range rounds {
		r := runRound(t, container, mode, i, rng, policy(rng))
		acked += len(r.acked)
		if r.big {
			bigKills++
		}
		if !r.office {
			officeKills++
		}
		results = append(results, r)
	}
	t.Logf("%d rounds: %d acknowledged records, %d rounds killed before taking office was reported, %d killed after beginning a large write and before its ack", rounds, acked, officeKills, bigKills)
	return results, checkP1(ctx, t, az, container, results)
}

// TestP1KillRestore kills a writer with SIGKILL at random moments, each
// round on a new empty directory, and checks that a restore holds every
// acknowledged write.
func TestP1KillRestore(t *testing.T) {
	azuriteEndpoint(t)
	rounds := int(envInt(t, killRoundsEnv, defaultKillRounds))
	results, res := runKillTest(t, "writer", rounds, func(rng *rand.Rand) killPolicy {
		// A quarter of the kills count from process start, so they can land
		// in the restore or the epoch bump; the rest land among the writes.
		if rng.IntN(4) == 0 {
			return killPolicy{maxDelay: 400 * time.Millisecond}
		}
		return killPolicy{after: "office", maxDelay: time.Second}
	})
	acked := 0
	for _, r := range results {
		acked += len(r.acked)
	}
	if acked == 0 {
		t.Error("no round acknowledged a record; the soak tested nothing")
	}
	for _, v := range res.violations {
		t.Error(v)
	}
}

// TestP1KillDuringUpload holds an upload of each kind part way through its
// request body, kills the writer there and checks that a restore holds
// exactly the acknowledged prefix. A new leader then takes office on the
// same history and writes once, which shows the killed upload left nothing
// that blocks the next writer.
func TestP1KillDuringUpload(t *testing.T) {
	azuriteEndpoint(t)
	for _, mode := range []string{"hold-l0", "hold-l1", "hold-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			az := newAzure(t, nil)
			container := newContainer(t, az)
			r := runRound(t, container, mode, 0, rand.New(rand.NewPCG(1, 1)), killPolicy{after: "uploading"}) //nolint:gosec // fixed seed, nothing random is used
			if len(r.acked) != holdPrefix || r.held == "" {
				t.Fatalf("helper acknowledged %d records and held %q before the kill, want %d and an upload", len(r.acked), r.held, holdPrefix)
			}
			t.Logf("killed holding %q after %d acknowledged records", r.held, len(r.acked))

			next, _, err := TakeOffice(ctx, az, container, t.TempDir())
			if err != nil {
				t.Fatalf("take office after the kill: %v", err)
			}
			id := int64(2*roundIDs + 1)
			if err := next.Exec(ctx, insertRow(ctx, id, "w", false)); err != nil {
				t.Fatalf("write after the kill: %v", err)
			}
			_ = next.Close(ctx)

			res := checkP1(ctx, t, az, container, []round{r, {office: true, epoch: next.Epoch(), acked: map[int64]bool{id: false}}})
			for _, v := range res.violations {
				t.Error(v)
			}
			if _, ok := res.present[r.inflight]; r.inflight != 0 && ok {
				t.Errorf("record %d, whose L0 upload was killed mid-body, is in the restore", r.inflight)
			}
		})
	}
}

// TestP1NegativeControl acknowledges each round's first record after the
// local commit and kills the writer before it uploads. The P1 checks must
// report that record missing in every round.
func TestP1NegativeControl(t *testing.T) {
	azuriteEndpoint(t)
	const rounds = 5
	results, res := runKillTest(t, "ack-early", rounds, func(*rand.Rand) killPolicy {
		return killPolicy{after: "ack"}
	})
	for i, r := range results {
		if len(r.acked) != 1 {
			t.Errorf("round %d acknowledged %d records, want 1", i, len(r.acked))
		}
		for id := range r.acked {
			if !res.missing[id] {
				t.Errorf("round %d: record %d was acknowledged before upload and the writer killed, yet the P1 checks did not report it missing", i, id)
			}
		}
	}
	t.Logf("negative control: %d of %d acknowledged records reported missing", len(res.missing), rounds)
}

type staleCase struct {
	pendingUpload bool   // A committed and built its L0 file but had not uploaded it when paused
	resetPosition bool   // A's replica position was reset by an earlier failed sync
	first         string // what A runs first on resume: "writes" (default), or the maintenance step "snapshot", "retention" or "compaction"
	unsafe        bool   // negative control: A's client has no fencing at all
	bSkipsCompact bool   // B does not compact, so A's compaction finds L0 files to compact
	wantFence     string // part of the reason A's client must latch with
}

// staleResult is what leader A did to storage after B took office.
type staleResult struct {
	ackedAfter  []int64         // A's post-takeover writes that were acknowledged
	bL0         map[string]bool // B's L0 blobs when A resumed
	overwritten []string        // blobs whose ETag changed while A ran
	deleted     []string        // blobs that disappeared while A ran
	added       []string        // blobs that appeared while A ran
	violations  []string        // every other P2 check that failed
}

// staleLeaderScenario pauses leader A, lets B take office, resumes A and
// reports what A did. Operational errors that are not A's doing, such as a
// failed restore or an unexpected lease error, fail the test.
//
// A is held in-process rather than with SIGSTOP: with the monitors off,
// Litestream runs no goroutine of its own, so a leader the test does not
// call is exactly a paused process as far as storage can tell, and the test
// can then drive the compaction, snapshot and retention calls a store would
// make on A's behalf.
func staleLeaderScenario(t *testing.T, sc staleCase) staleResult {
	ctx := context.Background()
	az := newAzure(t, nil)
	container := newContainer(t, az)
	var res staleResult

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
	want := map[int64]record{}
	for id := int64(1); id <= 5; id++ {
		if err := a.Exec(ctx, insertRow(ctx, id, "a", false)); err != nil {
			t.Fatal(err)
		}
		want[id] = record{writer: "a"}
	}
	if sc.pendingUpload {
		if err := commitLocal(ctx, a, insertRow(ctx, 6, "a", false)); err != nil {
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
	paused := listing(t, az, container)

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
		if err := b.Exec(ctx, insertRow(ctx, id, "b", false)); err != nil {
			t.Fatal(err)
		}
		want[id] = record{writer: "b"}
	}
	// An L1 file gives A's L0 retention something it could delete.
	if !sc.bSkipsCompact {
		if _, err := b.db.Compact(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	before := listing(t, az, container)
	res.bL0 = map[string]bool{}
	for key := range before {
		if _, ok := paused[key]; !ok && strings.HasPrefix(key, litestream.LTXLevelDir(DBPrefix, 0)+"/") {
			res.bL0[key] = true
		}
	}

	// A resumes.
	// fencedBy records a violation unless A's client is already latched by
	// the check this ordering names. It runs right after the operation that
	// is meant to fence A, so a later operation cannot supply the fence.
	maintenanceFirst := sc.first != "" && sc.first != "writes"
	fencedBy := func(op string) {
		switch fenced := a.Fenced(); {
		case sc.unsafe:
		case fenced == nil:
			res.violations = append(res.violations, fmt.Sprintf("A is not fenced after its %s", op))
		case !strings.Contains(fenced.Error(), sc.wantFence):
			res.violations = append(res.violations, fmt.Sprintf("A was fenced in its %s by %q, want the check %q", op, fenced, sc.wantFence))
		}
	}
	writes := func() {
		for id := int64(201); id <= 204; id++ {
			err := a.Exec(ctx, insertRow(ctx, id, "a", false))
			if err == nil {
				res.ackedAfter = append(res.ackedAfter, id)
			} else if !sc.unsafe && !errors.Is(err, ErrFenced) {
				res.violations = append(res.violations, fmt.Sprintf("A write %d failed with %v, want ErrFenced", id, err))
			}
			if id == 201 && !maintenanceFirst {
				fencedBy("first write")
			}
		}
		// Litestream's own retry path, bypassing the leader's latch check.
		if err := a.db.SyncAndWait(ctx); err == nil {
			t.Logf("A: Litestream SyncAndWait returned nil at local position %s with uploaded head %s", mustPos(a), a.client.Head())
		}
	}
	// The calls a litestream.Store makes on its schedule.
	steps := map[string]func() error{
		"snapshot": func() error {
			_, err := a.db.Snapshot(ctx)
			return err
		},
		"retention": func() error {
			minTXID, err := a.db.EnforceSnapshotRetention(ctx, time.Now())
			if sc.first == "retention" {
				fencedBy("snapshot retention")
			}
			for _, level := range []int{0, 1} {
				err = errors.Join(err, a.db.EnforceRetentionByTXID(ctx, level, minTXID))
			}
			a.db.L0Retention = time.Nanosecond
			return errors.Join(err, a.db.EnforceL0RetentionByTime(ctx))
		},
		"compaction": func() error {
			_, err := a.db.Compact(ctx, 1)
			return err
		},
	}
	order := []string{"snapshot", "retention", "compaction"}
	if i := slices.Index(order, sc.first); i > 0 {
		order = append([]string{sc.first}, slices.Delete(order, i, i+1)...)
	}
	maintenance := func() {
		for i, step := range order {
			t.Logf("A %s: %v", step, steps[step]())
			if i == 0 && maintenanceFirst && step != "retention" {
				fencedBy(step)
			}
		}
	}
	if sc.resetPosition {
		// Litestream's own view of a write after a position reset.
		if err := commitLocal(ctx, a, insertRow(ctx, 200, "a", false)); err != nil {
			t.Fatal(err)
		}
		if err := a.db.SyncAndWait(ctx); err == nil {
			t.Logf("A: SyncAndWait returned nil for write 200 at local position %v although client head is %s; Litestream uploaded nothing", mustPos(a), a.client.Head())
		}
	}
	phases := []func(){writes, maintenance}
	if maintenanceFirst {
		phases = []func(){maintenance, writes}
	}
	// A listing after each phase, so a blob A overwrites and later deletes
	// shows up as both.
	listings := []map[string]azcore.ETag{before}
	for _, phase := range phases {
		phase()
		listings = append(listings, listing(t, az, container))
	}
	switch err := leaseA.Renew(ctx, az, container); {
	case err == nil:
		res.violations = append(res.violations, "A renewed a lease B had taken over")
	case !bloberror.HasCode(err, bloberror.ConditionNotMet):
		t.Fatalf("A's lease renewal failed with %v, want ConditionNotMet", err)
	}
	if !sc.unsafe {
		switch fenced := a.Fenced(); {
		case fenced == nil:
			res.violations = append(res.violations, "A is not fenced")
		case !strings.Contains(fenced.Error(), sc.wantFence):
			res.violations = append(res.violations, fmt.Sprintf("A was fenced by %q, want the check %q", fenced, sc.wantFence))
		default:
			t.Logf("A fenced: %v", fenced)
		}
	}

	after := listings[len(listings)-1]
	overwritten := map[string]bool{}
	for i := 1; i < len(listings); i++ {
		for key, etag := range listings[i-1] {
			if got, ok := listings[i][key]; ok && got != etag {
				overwritten[key] = true
			}
		}
	}
	res.overwritten = slices.Sorted(maps.Keys(overwritten))
	for _, key := range slices.Sorted(maps.Keys(before)) {
		if _, ok := after[key]; !ok {
			res.deleted = append(res.deleted, key)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(after)) {
		if _, ok := before[key]; !ok {
			res.added = append(res.added, key)
		}
	}

	// B is unharmed and can keep writing.
	if err := b.Exec(ctx, insertRow(ctx, 106, "b", false)); err != nil {
		res.violations = append(res.violations, fmt.Sprintf("B write after A resumed: %v", err))
	} else {
		want[106] = record{writer: "b"}
	}
	res.violations = append(res.violations, checkExactRestore(ctx, t, az, container, want, 2)...)
	return res
}

// checkExactRestore restores on an empty directory and compares the records,
// their content and the epoch with what was acknowledged. It fails the test
// if the restore itself fails.
func checkExactRestore(ctx context.Context, t *testing.T, az *azblob.Client, container string, want map[int64]record, epoch int64) []string {
	t.Helper()
	db, err := restoreFresh(ctx, t, az, container)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	var v []string
	if err := integrityCheck(db); err != nil {
		v = append(v, err.Error())
	}
	got, bad, err := readRecords(db)
	if err != nil {
		t.Fatalf("read restored records: %v", err)
	}
	v = append(v, bad...)
	for _, id := range slices.Sorted(maps.Keys(want)) {
		if g, ok := got[id]; !ok {
			v = append(v, fmt.Sprintf("acknowledged record %d missing from restore", id))
		} else if g != want[id] {
			v = append(v, fmt.Sprintf("acknowledged record %d restored as %+v, want %+v", id, g, want[id]))
		}
	}
	for _, id := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[id]; !ok {
			v = append(v, fmt.Sprintf("unacknowledged record %d %+v present in restore", id, got[id]))
		}
	}
	if e, err := epochOf(db); err != nil || e != epoch {
		v = append(v, fmt.Sprintf("restored epoch %d (%v), want %d", e, err, epoch))
	}
	gaps, err := l0Gaps(ctx, az, container)
	if err != nil {
		t.Fatalf("l0 gaps: %v", err)
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

// Fence reasons, as Client words them.
const (
	fenceL0Conflict = "already holds another leader's file"
	fenceRemoteHead = "remote L0 head"
	fencePastHead   = "is past head"
	fenceAckCheck   = "is not the uploaded head"
)

func TestP2StaleLeaderCannotCommit(t *testing.T) {
	azuriteEndpoint(t)
	for name, sc := range map[string]staleCase{
		"WritesFirstWithPendingUpload": {pendingUpload: true, wantFence: fenceL0Conflict},
		"SnapshotFirst":                {first: "snapshot", wantFence: fenceRemoteHead},
		"SnapshotFirstWithPending":     {pendingUpload: true, first: "snapshot", wantFence: fencePastHead},
		"RetentionFirst":               {first: "retention", wantFence: fenceRemoteHead},
		"CompactionFirst":              {first: "compaction", bSkipsCompact: true, wantFence: fencePastHead},
		"PositionResetAfterError":      {resetPosition: true, wantFence: fenceAckCheck},
	} {
		t.Run(name, func(t *testing.T) {
			res := staleLeaderScenario(t, sc)
			for _, id := range res.ackedAfter {
				t.Errorf("A acknowledged post-takeover write %d", id)
			}
			for _, key := range res.overwritten {
				t.Errorf("%s was overwritten", key)
			}
			for _, key := range res.deleted {
				t.Errorf("%s was deleted", key)
			}
			for _, key := range res.added {
				t.Errorf("%s was added by A", key)
			}
			for _, v := range res.violations {
				t.Error(v)
			}
		})
	}
}

// TestP2NegativeControl runs the stale-leader scenario with A's fencing
// switched off. Each kind of damage the P2 checks look for must be seen.
func TestP2NegativeControl(t *testing.T) {
	azuriteEndpoint(t)
	res := staleLeaderScenario(t, staleCase{pendingUpload: true, unsafe: true})
	if len(res.ackedAfter) == 0 {
		t.Error("unfenced A acknowledged no post-takeover write")
	}
	var bOverwritten []string
	for _, key := range res.overwritten {
		if res.bL0[key] {
			bOverwritten = append(bOverwritten, key)
		}
	}
	if len(bOverwritten) == 0 {
		t.Errorf("unfenced A changed the ETag of none of B's %d L0 blobs (overwritten: %v)", len(res.bL0), res.overwritten)
	}
	if len(res.deleted) == 0 {
		t.Error("unfenced A deleted no blob")
	}
	var snapshots []string
	for _, key := range res.added {
		if strings.HasPrefix(key, litestream.LTXLevelDir(DBPrefix, litestream.SnapshotLevel)+"/") {
			snapshots = append(snapshots, key)
		}
	}
	if len(snapshots) == 0 {
		t.Errorf("unfenced A added no snapshot (added: %v)", res.added)
	}
	t.Logf("negative control: A acknowledged %d post-takeover writes %v", len(res.ackedAfter), res.ackedAfter)
	t.Logf("negative control: A overwrote %d of B's %d L0 blobs %v", len(bOverwritten), len(res.bL0), bOverwritten)
	t.Logf("negative control: A deleted %d blobs %v", len(res.deleted), res.deleted)
	t.Logf("negative control: A added %d blobs %v, %d of them snapshots", len(res.added), res.added, len(snapshots))
	for _, v := range res.violations {
		t.Logf("negative control: %s", v)
	}
}

// TestP2StaleMutationWindow measures the window between the client's
// admission checks and the storage request. Leader A passes the checks for
// one snapshot write, compaction write or retention delete and is paused
// there; B takes office, writes and runs maintenance; A's operation is then
// released. The test records exactly what A's operation added or removed
// and asserts what matters for the product: A acknowledges nothing after
// the takeover, B keeps working, and restores hold exactly the acknowledged
// records at B's epoch.
func TestP2StaleMutationWindow(t *testing.T) {
	azuriteEndpoint(t)
	for _, kind := range []string{"Snapshot", "Compaction", "RetentionDelete"} {
		t.Run(kind, func(t *testing.T) { staleWindowCase(t, kind) })
	}
}

func staleWindowCase(t *testing.T, kind string) {
	ctx := context.Background()
	az := newAzure(t, nil)
	container := newContainer(t, az)
	want := map[int64]record{}
	write := func(l *Leader, id int64, writer string, big bool) {
		t.Helper()
		if err := l.Exec(ctx, insertRow(ctx, id, writer, big)); err != nil {
			t.Fatalf("%s write %d: %v", writer, id, err)
		}
		want[id] = record{writer: writer, big: big}
	}

	if _, err := AcquireLease(ctx, az, container, "a"); err != nil {
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
	// A's history: a snapshot and an L1 file over the first records, then an
	// uncompacted L0 tail.
	for id := int64(1); id <= 4; id++ {
		write(a, id, "a", id == 1)
	}
	if _, err := a.db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for id := int64(5); id <= 8; id++ {
		write(a, id, "a", false)
	}
	headA := a.client.Head()

	// A passes its admission checks for one operation and pauses before the
	// storage request.
	wantOp, wantLevel := "write", litestream.SnapshotLevel
	switch kind {
	case "Compaction":
		wantLevel = 1
	case "RetentionDelete":
		wantOp, wantLevel = "delete", -1
		a.db.L0Retention = time.Nanosecond
	}
	pausedCh := make(chan struct{})
	release := make(chan struct{})
	var once, releaseOnce sync.Once
	releaseA := func() { releaseOnce.Do(func() { close(release) }) }
	a.client.beforeMutation = func(op string, level int) {
		if op == wantOp && level == wantLevel {
			once.Do(func() {
				close(pausedCh)
				<-release
			})
		}
	}
	var errA error
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		switch kind {
		case "Snapshot":
			_, errA = a.db.Snapshot(ctx)
		case "Compaction":
			_, errA = a.db.Compact(ctx, 1)
		case "RetentionDelete":
			errA = a.db.EnforceL0RetentionByTime(ctx)
		}
	}()
	// Runs before A is closed, so a failure below never leaves the worker
	// blocked holding the client's lock.
	defer func() {
		releaseA()
		<-doneA
	}()
	select {
	case <-pausedCh:
	case <-doneA:
		t.Fatalf("A's %s finished without reaching its storage request: %v", kind, errA)
	case <-time.After(time.Minute):
		t.Fatalf("A's %s did not reach its storage request", kind)
	}
	t.Logf("A paused in its %s at head %s, after its admission checks", kind, headA)

	// B takes office while A is paused, writes and runs maintenance.
	if _, err := AcquireLease(ctx, az, container, "b"); err != nil {
		t.Fatal(err)
	}
	b, _, err := TakeOffice(ctx, az, container, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close(ctx) }()
	maintainB := func(phase string, snapshotCutoff time.Time, l0Retention time.Duration) {
		t.Helper()
		if _, err := b.db.Compact(ctx, 1); err != nil {
			t.Errorf("B %s compaction: %v", phase, err)
		}
		if _, err := b.db.Snapshot(ctx); err != nil {
			t.Errorf("B %s snapshot: %v", phase, err)
		}
		minTXID, err := b.db.EnforceSnapshotRetention(ctx, snapshotCutoff)
		if err != nil {
			t.Errorf("B %s snapshot retention: %v", phase, err)
		}
		for _, level := range []int{0, 1} {
			if err := b.db.EnforceRetentionByTXID(ctx, level, minTXID); err != nil {
				t.Errorf("B %s level %d retention: %v", phase, level, err)
			}
		}
		b.db.L0Retention = l0Retention
		if err := b.db.EnforceL0RetentionByTime(ctx); err != nil {
			t.Errorf("B %s L0 retention: %v", phase, err)
		}
	}
	for id := int64(101); id <= 104; id++ {
		write(b, id, "b", id == 101)
	}
	// Litestream's default retention, as a Store would run it now: nothing
	// is old enough to delete yet, so A's stale delete meets the files B
	// still keeps.
	maintainB("first", time.Now().Add(-litestream.DefaultRetention), litestream.DefaultL0Retention)

	// A's operation completes.
	beforeRelease := listing(t, az, container)
	releaseA()
	select {
	case <-doneA:
	case <-time.After(time.Minute):
		t.Fatalf("A's %s did not finish after release", kind)
	}
	afterRelease := listing(t, az, container)
	var changes []string
	for _, key := range slices.Sorted(maps.Keys(beforeRelease)) {
		if got, ok := afterRelease[key]; !ok {
			changes = append(changes, "removed "+key)
		} else if got != beforeRelease[key] {
			changes = append(changes, "overwrote "+key)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(afterRelease)) {
		if _, ok := beforeRelease[key]; !ok {
			changes = append(changes, "added "+key)
		}
	}
	t.Logf("A's released %s returned %v and changed %d blobs: %v", kind, errA, len(changes), changes)
	// The case only means something if storage accepted exactly the stale
	// mutation it is about; a rejected or no-op request would also leave the
	// restores below intact.
	if errA != nil {
		t.Errorf("A's released %s = %v, want nil", kind, errA)
	}
	ltxKey := func(level int, minTXID, maxTXID uint64) string {
		return fmt.Sprintf("db/ltx/%d/%016x-%016x.ltx", level, minTXID, maxTXID)
	}
	var wantChanges []string
	switch kind {
	case "Snapshot":
		wantChanges = []string{"added " + ltxKey(litestream.SnapshotLevel, 1, 0xa)}
	case "Compaction":
		wantChanges = []string{"added " + ltxKey(1, 7, 0xa)}
	case "RetentionDelete":
		for txid := uint64(1); txid <= 6; txid++ {
			wantChanges = append(wantChanges, "removed "+ltxKey(0, txid, txid))
		}
	}
	if !slices.Equal(changes, wantChanges) {
		t.Errorf("A's released %s changed %v, want %v", kind, changes, wantChanges)
	}

	if err := a.Exec(ctx, insertRow(ctx, 301, "a", false)); !errors.Is(err, ErrFenced) {
		t.Errorf("A write after the takeover = %v, want ErrFenced", err)
	}
	for _, v := range checkExactRestore(ctx, t, az, container, want, 2) {
		t.Errorf("restore after A's %s: %s", kind, v)
	}

	// B keeps going, now with retention that deletes everything it may.
	for id := int64(105); id <= 108; id++ {
		write(b, id, "b", id == 105)
	}
	maintainB("second", time.Now(), time.Nanosecond)
	for _, v := range checkExactRestore(ctx, t, az, container, want, 2) {
		t.Errorf("restore after B's second maintenance: %s", v)
	}
	// B's retention has removed whatever A's stale operation left behind.
	wantFinal := []string{
		ltxKey(0, 0x13, 0x13),
		ltxKey(1, 7, 0xf),
		ltxKey(1, 0x10, 0x13),
		ltxKey(litestream.SnapshotLevel, 1, 0x13),
		"lease.json",
	}
	if got := slices.Sorted(maps.Keys(listing(t, az, container))); !slices.Equal(got, wantFinal) {
		t.Errorf("blobs after B's second maintenance = %v, want %v", got, wantFinal)
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
		aAcked := make(chan struct{})
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
				if aErr = a.Exec(ctx, insertRow(ctx, id, "a", false)); aErr != nil {
					return
				}
				ackedA = append(ackedA, id)
				if id == 1 {
					close(aAcked)
				}
			}
		}()

		// A failure below must not leave A writing into a container that
		// cleanup is deleting.
		t.Cleanup(func() {
			stop.Store(true)
			<-aDone
		})
		// B starts only once A has acknowledged a write and is still writing.
		select {
		case <-aAcked:
		case <-aDone:
			t.Fatalf("iteration %d: A stopped before acknowledging a write: %v", i, aErr)
		}
		time.Sleep(50*time.Millisecond + time.Duration(rng.Int64N(int64(200*time.Millisecond))))
		select {
		case <-aDone:
			t.Fatalf("iteration %d: A stopped before B started: %v", i, aErr)
		default:
		}
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
		want := map[int64]record{}
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
				if err := b.Exec(ctx, insertRow(ctx, id, "b", false)); err != nil {
					t.Fatalf("iteration %d: B write %d: %v", i, id, err)
				}
				want[id] = record{writer: "b"}
			}
			select {
			case <-aDone:
			case <-time.After(30 * time.Second):
				stop.Store(true)
				<-aDone
				t.Errorf("iteration %d: A kept acknowledging writes after B took office", i)
			}
			if err := a.Exec(ctx, insertRow(ctx, 999_999, "a", false)); !errors.Is(err, ErrFenced) {
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
			want[id] = record{writer: "a"}
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
		if err := l.Exec(ctx, insertRow(ctx, id, "m", false)); err != nil {
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
	if err := l.Exec(ctx, insertRow(ctx, n+1, "m", true)); err != nil {
		t.Fatal(err)
	}
	t.Logf("one commit with a %d KiB row: %s, %d storage requests", bigRowBytes>>10, time.Since(start).Round(10*time.Microsecond), requests.Load()-before)

	// Grow the database, then time restores from L0 alone and after
	// compaction. CI keeps it small; set SKYLITE_SPIKE_MEASURE_MIB to grow it.
	for i := range envInt(t, "SKYLITE_SPIKE_MEASURE_MIB", 16) / 4 {
		if err := l.Exec(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO big (slot, id, writer, data) VALUES (?, 0, 'm', ?)`, 100+i, randomBytes(4<<20))
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
