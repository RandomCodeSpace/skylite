package acceptance

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

// TestPoolAfterFailedEnd fails the token refresh just before a commit or
// rollback is sent: the server's transaction must not stay open on a pooled
// connection, or the next autocommit write runs inside it and is lost.
func TestPoolAfterFailedEnd(t *testing.T) {
	for _, end := range []string{"commit", "rollback"} {
		t.Run(end, func(t *testing.T) {
			var reject atomic.Bool
			unavailable := errors.New("injected token refresh failure")
			connector, err := skylite.NewConnector(skylite.Config{
				Endpoint: endpoint, Database: "gorm", AllowPlaintext: true,
				Token: skylite.TokenFunc(func(context.Context) (string, error) {
					if reject.Swap(false) {
						return "", unavailable
					}
					return token, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			db := sql.OpenDB(connector)
			defer func() { _ = db.Close() }()
			db.SetMaxOpenConns(1)
			table := "pool_end_" + end
			if _, err := db.Exec("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec("INSERT INTO " + table + " VALUES (1)"); err != nil { //nolint:gosec // table is a constant prefix and the subtest name
				t.Fatal(err)
			}
			reject.Store(true)
			finish := tx.Commit
			if end == "rollback" {
				finish = tx.Rollback
			}
			if err = finish(); !errors.Is(err, unavailable) {
				t.Fatalf("expected injected failure, got %v", err)
			}
			res, err := db.Exec("INSERT INTO " + table + " VALUES (2)") //nolint:gosec // table is a constant prefix and the subtest name
			if err != nil {
				t.Fatalf("new autocommit write: %v", err)
			}
			affected, err := res.RowsAffected()
			if err != nil || affected != 1 {
				t.Fatalf("rows affected %d, err %v", affected, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			observer := open()
			defer func() { _ = observer.Close() }()
			var count int
			if err = observer.QueryRow("SELECT COUNT(*) FROM " + table + " WHERE id = 2").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("autocommit reported success and affected %d row, independent connection sees %d after pool close", affected, count)
			}
		})
	}
}

// TestPoolAfterAbortedCommit commits an aborted transaction: its
// connection must not reach the next borrower with the channel aborted.
func TestPoolAfterAbortedCommit(t *testing.T) {
	db := open()
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE pool_abort (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT INTO pool_abort VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT OR ROLLBACK INTO pool_abort VALUES (1)"); err == nil {
		t.Fatal("expected constraint violation")
	}
	if err = tx.Commit(); err == nil {
		t.Fatal("expected aborted transaction")
	}
	if _, err = db.Exec("INSERT INTO pool_abort VALUES (2)"); err != nil {
		t.Fatalf("new autocommit inherited the finished transaction: %v", err)
	}
}
