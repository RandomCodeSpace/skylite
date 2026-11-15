package sqlpolicy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// FuzzRun runs arbitrary SQL under the policy on a database file and checks
// what must hold whatever the SQL was: SQLite used no file but the
// database's own, no database is attached, and every setting the server owns
// is unchanged.
func FuzzRun(f *testing.F) {
	for _, r := range refusals {
		f.Add(r.sql)
	}
	for _, r := range engineRefusals {
		f.Add(r.sql)
	}
	for _, s := range allowed {
		f.Add(s)
	}
	for _, s := range append(hibernate, gorm...) {
		f.Add(s.sql)
	}
	f.Add("CREATE TEMP TABLE x AS SELECT randomblob(4000) FROM generate_series(1, 2000); SELECT * FROM x ORDER BY 1")
	f.Add("SELECT * FROM t, pragma_journal_mode")
	f.Add("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c")

	f.Fuzz(func(t *testing.T, sql string) {
		db := openDB(t, true, Limits{})
		db.mustRun(t, "CREATE TABLE t(a); INSERT INTO t VALUES (1)")
		before := db.owned(t)
		rec.take()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := db.conn.Run(ctx, strings.ReplaceAll(sql, "{dir}", db.dir), nil)

		var pe *Error
		if errors.As(err, &pe) && len(pe.SQL) > 64 {
			t.Errorf("refusal echoes %d bytes of SQL", len(pe.SQL))
		}
		db.checkFiles(t)
		if errors.Is(err, ErrBroken) || errors.As(err, &pe) && pe.Reason == ReasonMemory {
			return // the engine's state is unknown; only the files can be checked
		}
		if after := db.owned(t); !reflect.DeepEqual(before, after) {
			for k, v := range before {
				if after[k] != v {
					t.Errorf("%s changed from %q to %q", k, v, after[k])
				}
			}
		}
	})
}
