package acceptance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (rt roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return rt(req) }

// TestCancelledCreate cancels a Create after its INSERT reached the server
// and loses the answer, so that the driver reports an unknown outcome.
//
// In GORM's default transaction, database/sql rolls the transaction back
// when the context ends. When it does so before GORM's own cleanup, GORM's
// Rollback gets sql.ErrTxDone, and GORM v1.31.2's AddError joins the two
// as fmt.Errorf("%v; %w"): the unknown outcome survives only as text. No
// driver change can prevent it, because database/sql answers ErrTxDone
// without calling the driver. With SkipDefaultTransaction the INSERT is an
// autocommit write and the error matches ErrOutcomeUnknown.
func TestCancelledCreate(t *testing.T) {
	setup := open()
	defer func() { _ = setup.Close() }()
	if _, err := setup.Exec("CREATE TABLE cancelled_create(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for i, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("SkipDefaultTransaction=%t", skip), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			insert := int32(2) // begin, then the INSERT
			if skip {
				insert = 1
			}
			var deliveries atomic.Int32
			rt := roundTripper(func(r *http.Request) (*http.Response, error) {
				resp, err := http.DefaultTransport.RoundTrip(r)
				if err == nil && strings.HasSuffix(r.URL.Path, "/channel") && deliveries.Add(1) == insert {
					_ = resp.Body.Close() // the INSERT reached the server; its answer is lost
					cancel()
					return nil, ctx.Err()
				}
				return resp, err
			})
			connector, err := skylite.NewConnector(skylite.Config{
				Endpoint: endpoint, Database: "gorm", AllowPlaintext: true, HTTPClient: &http.Client{Transport: rt},
				Token: skylite.TokenFunc(func(context.Context) (string, error) { return token, nil }),
			})
			if err != nil {
				t.Fatal(err)
			}
			db := sql.OpenDB(connector)
			defer func() { _ = db.Close() }()
			g, err := gorm.Open(sqlite.Dialector{Conn: db}, &gorm.Config{TranslateError: true, SkipDefaultTransaction: skip, Logger: logger.Discard})
			if err != nil {
				t.Fatal(err)
			}
			if !skip {
				// Take the ordering in which database/sql rolls back first.
				err = g.Callback().Create().After("gorm:create").Before("gorm:commit_or_rollback_transaction").Register("test:await_rollback", func(g *gorm.DB) {
					if !errors.Is(g.Error, skylite.ErrOutcomeUnknown) {
						t.Errorf("the driver's error before GORM's cleanup: %v", g.Error)
					}
					for deadline := time.Now().Add(3 * time.Second); db.Stats().InUse > 0 && time.Now().Before(deadline); {
						time.Sleep(time.Millisecond)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			err = g.WithContext(ctx).Table("cancelled_create").Create(&struct{ ID int }{ID: i + 1}).Error
			unknown, canceled := errors.Is(err, skylite.ErrOutcomeUnknown), errors.Is(err, context.Canceled)
			switch {
			case skip && (!unknown || !canceled):
				t.Fatalf("autocommit Create: unknown %t, canceled %t: %v", unknown, canceled, err)
			case !skip && (unknown || !errors.Is(err, sql.ErrTxDone) || !strings.Contains(fmt.Sprint(err), "outcome is unknown")):
				t.Fatalf("GORM's join of the two errors changed; update go-driver.md: unknown %t: %v", unknown, err)
			}
		})
	}
}
