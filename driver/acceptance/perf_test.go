package acceptance

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// perfStorage is the server's store: a local one in dir, or, with
// SKYLITE_PERF_PREFIX set, that prefix of container SKYLITE_PERF_CONTAINER
// in the account at SKYLITE_TEST_AZURE_ACCOUNT_URL, with the Entra
// credential from the AZURE_* environment.
func perfStorage(dir string) map[string]any {
	prefix := os.Getenv("SKYLITE_PERF_PREFIX")
	if prefix == "" {
		return map[string]any{"localfs": map[string]any{"path": filepath.Join(dir, "store")}}
	}
	u, err := url.Parse(os.Getenv("SKYLITE_TEST_AZURE_ACCOUNT_URL"))
	if err != nil || u.Host == "" {
		panic("SKYLITE_PERF_PREFIX needs SKYLITE_TEST_AZURE_ACCOUNT_URL, https://<account>.blob.core.windows.net/")
	}
	account, _, _ := strings.Cut(u.Host, ".")
	return map[string]any{"prefix": prefix, "azure": map[string]any{
		"account": account, "container": os.Getenv("SKYLITE_PERF_CONTAINER"), "endpoint": "https://" + u.Host, "entra": true,
	}}
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[min(len(d)-1, int(float64(len(d))*p))]
}

func report(t *testing.T, name string, lat []time.Duration, elapsed time.Duration, errs int64) {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("PERF %-34s n=%-6d err=%-3d ops/s=%-8.1f p50=%-8v p95=%-8v p99=%-8v max=%v",
		name, len(lat), errs, float64(len(lat))/elapsed.Seconds(),
		pct(lat, .50).Round(100*time.Microsecond), pct(lat, .95).Round(100*time.Microsecond),
		pct(lat, .99).Round(100*time.Microsecond), pct(lat, 1).Round(100*time.Microsecond))
}

// run calls op from workers goroutines for dur, or n times in total if n > 0.
func run(t *testing.T, name string, workers int, dur time.Duration, n int64, op func(ctx context.Context, w int, i int64) error) {
	var (
		mu   sync.Mutex
		lat  []time.Duration
		errs atomic.Int64
		next atomic.Int64
		wg   sync.WaitGroup
	)
	stop := time.Now().Add(dur)
	start := time.Now()
	for w := range workers {
		wg.Go(func() {
			var mine []time.Duration
			for {
				i := next.Add(1)
				if (n > 0 && i > n) || (n == 0 && time.Now().After(stop)) {
					break
				}
				t0 := time.Now()
				if err := op(context.Background(), w, i); err != nil {
					if errs.Add(1) <= 3 {
						t.Logf("%s: %v", name, err)
					}
					continue
				}
				mine = append(mine, time.Since(t0))
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		})
	}
	wg.Wait()
	report(t, name, lat, time.Since(start), errs.Load())
}

func TestPerf(t *testing.T) {
	if os.Getenv("SKYLITE_PERF") != "1" {
		t.Skip("SKYLITE_PERF is not 1")
	}
	db := open()
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(128)
	db.SetMaxIdleConns(128)
	mustExec := func(q string, args ...any) {
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatal(q, err)
		}
	}
	mustExec(`CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, w INTEGER, v BLOB)`)
	payload := make([]byte, 256)
	_, _ = rand.Read(payload)

	var id atomic.Int64
	insert := func(ctx context.Context, w int, _ int64) error {
		_, err := db.ExecContext(ctx, `INSERT INTO kv (id, w, v) VALUES (?, ?, ?)`, id.Add(1), w, payload)
		return err
	}
	run(t, "autocommit insert, 1 worker", 1, 0, 200, insert)
	for _, w := range []int{4, 16, 64} {
		run(t, fmt.Sprintf("autocommit insert, %d workers", w), w, 15*time.Second, 0, insert)
	}

	txn := func(rows int) func(ctx context.Context, w int, _ int64) error {
		return func(ctx context.Context, w int, _ int64) error {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			for range rows {
				if _, err := tx.ExecContext(ctx, `INSERT INTO kv (id, w, v) VALUES (?, ?, ?)`, id.Add(1), w, payload); err != nil {
					_ = tx.Rollback()
					return err
				}
			}
			return tx.Commit()
		}
	}
	run(t, "txn of 100 inserts, 1 worker", 1, 0, 30, txn(100))
	run(t, "txn of 100 inserts, 8 workers", 8, 15*time.Second, 0, txn(100))

	big := make([]byte, 1<<20)
	_, _ = rand.Read(big)
	bulk := func(ctx context.Context, w int, _ int64) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for range 16 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO kv (id, w, v) VALUES (?, ?, ?)`, id.Add(1), w, big[:512<<10]); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	}
	run(t, "txn of 16 x 512 KiB rows (8 MiB)", 1, 0, 5, bulk)

	var rows int64
	if err := db.QueryRow(`SELECT count(*) FROM kv`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	t.Logf("PERF rows in kv: %d", rows)
	maxID := id.Load()
	read := func(ctx context.Context, _ int, i int64) error {
		var w int
		err := db.QueryRowContext(ctx, `SELECT w FROM kv WHERE id = ?`, 1+(i*7919)%maxID).Scan(&w)
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	run(t, "point select, 1 worker", 1, 0, 1000, read)
	for _, w := range []int{16, 32, 64} {
		run(t, fmt.Sprintf("point select, %d workers", w), w, 10*time.Second, 0, read)
	}
	scan := func(ctx context.Context, _ int, _ int64) error {
		var n, s int64
		return db.QueryRowContext(ctx, `SELECT count(*), sum(length(v)) FROM kv`).Scan(&n, &s)
	}
	run(t, "full scan count+sum(length), 1 worker", 1, 0, 20, scan)
}
