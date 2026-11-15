package durability

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
	_ "modernc.org/sqlite" // pure-Go driver registered as "sqlite"; Litestream uses the same one
)

// DBPrefix is the blob prefix the spike keeps the LTX history under.
const DBPrefix = "db"

// maxTakeOfficeAttempts bounds how often a candidate retries after losing
// the race for the next slot to another writer.
const maxTakeOfficeAttempts = 20

// Leader is the single writer of one database. Its working copy lives in a
// directory that must start empty; object storage holds the only durable copy.
type Leader struct {
	dir    string
	client *Client
	db     *litestream.DB
	sql    *sql.DB

	mu    sync.Mutex
	epoch int64
}

// TakeOffice restores the latest history into the empty directory dir and
// commits an epoch bump with put-if-absent. Once that upload succeeds, any
// older leader's next upload collides and fences it. If another writer wins
// the slot first, the candidate discards dir and starts again. It returns the
// number of attempts it needed.
func TakeOffice(ctx context.Context, az *azblob.Client, container, dir string) (*Leader, int, error) {
	for attempt := 1; ; attempt++ {
		l, err := takeOffice(ctx, NewClient(az, container, DBPrefix), dir)
		if err == nil {
			return l, attempt, nil
		}
		if !errors.Is(err, ErrFenced) || attempt == maxTakeOfficeAttempts {
			return nil, attempt, fmt.Errorf("take office (attempt %d): %w", attempt, err)
		}
		// dir is the caller's working directory for this database.
		if err := os.RemoveAll(dir); err != nil { //nolint:gosec // see above
			return nil, attempt, err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil { //nolint:gosec // see above
			return nil, attempt, err
		}
	}
}

func takeOffice(ctx context.Context, client *Client, dir string) (_ *Leader, err error) {
	path := filepath.Join(dir, "db.sqlite")

	// Restore to an explicit TXID so the head is known exactly, even if
	// another writer appends while the restore runs.
	var txid ltx.TXID
	plan, err := litestream.CalcRestorePlan(ctx, client, 0, time.Time{}, client.logger)
	switch {
	case err == nil:
		txid = plan[len(plan)-1].MaxTXID
		opt := litestream.NewRestoreOptions()
		opt.OutputPath = path
		opt.TXID = txid
		if err := litestream.NewReplicaWithClient(nil, client).Restore(ctx, opt); err != nil {
			return nil, fmt.Errorf("restore to %s: %w", txid, err)
		}
	case errors.Is(err, litestream.ErrTxNotAvailable), errors.Is(err, litestream.ErrNoSnapshots):
		// Empty history: this leader creates the database.
	default:
		return nil, fmt.Errorf("restore plan: %w", err)
	}
	client.setHead(txid)

	db := litestream.NewDB(path)
	db.Logger = client.logger
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		return nil, err
	}

	sqldb, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)")
	if err != nil {
		_ = db.Close(ctx)
		return nil, err
	}
	l := &Leader{dir: dir, client: client, db: db, sql: sqldb}
	defer func() {
		if err != nil {
			_ = l.Close(ctx)
		}
	}()

	err = l.Exec(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS spike_meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO spike_meta (k, v) VALUES ('epoch', 1) ON CONFLICT (k) DO UPDATE SET v = v + 1`); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT v FROM spike_meta WHERE k = 'epoch'`).Scan(&l.epoch)
	})
	if err != nil {
		return nil, err
	}
	return l, nil
}

// Epoch returns the epoch this leader committed when it took office.
func (l *Leader) Epoch() int64 { return l.epoch }

// Fenced returns a wrapped ErrFenced once this leader can no longer write.
func (l *Leader) Fenced() error { return l.client.Fenced() }

// SQL returns the leader's connection to its working copy, for reads.
func (l *Leader) SQL() *sql.DB { return l.sql }

// Exec runs fn in one write transaction and returns nil only after the
// resulting change file is confirmed in object storage. A nil return is the
// acknowledgement.
func (l *Leader) Exec(ctx context.Context, fn func(*sql.Tx) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.client.Fenced(); err != nil {
		return err
	}

	tx, err := l.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	if err := l.db.SyncAndWait(ctx); err != nil {
		return err
	}
	pos, err := l.db.Pos()
	if err != nil {
		return err
	}
	return l.client.confirmAck(pos.TXID)
}

// Close releases the working copy. It does not remove the directory.
func (l *Leader) Close(ctx context.Context) error {
	return errors.Join(l.sql.Close(), l.db.Close(ctx))
}
