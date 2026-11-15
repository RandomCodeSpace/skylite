package sqlpolicy

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"
)

// recorder wraps the os VFS and records every name SQLite opens, probes or
// deletes through it. Temporary files are opened with an empty name.
type recorder struct {
	vfs.VFS
	mu    sync.Mutex
	names []string
}

func (r *recorder) add(name string) {
	r.mu.Lock()
	r.names = append(r.names, name)
	r.mu.Unlock()
}

func (r *recorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.names
	r.names = nil
	return n
}

func (r *recorder) Open(name string, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	r.add(name)
	return r.VFS.Open(name, flags)
}

func (r *recorder) OpenFilename(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	r.add(name.String())
	return r.VFS.(vfs.VFSFilename).OpenFilename(name, flags)
}

func (r *recorder) Delete(name string, syncDir bool) error {
	r.add(name)
	return r.VFS.Delete(name, syncDir)
}

func (r *recorder) Access(name string, flags vfs.AccessFlag) (bool, error) {
	r.add(name)
	return r.VFS.Access(name, flags)
}

const testVFS = "sqlpolicy-recorder"

var rec = &recorder{VFS: vfs.Find("os")}

func init() { vfs.Register(testVFS, rec) }

// testDB is a connection with a policy applied, in memory or in a file in
// WAL mode, which is the journal mode skylite's engine will use.
type testDB struct {
	raw  *sqlite3.Conn
	conn *Conn
	dir  string // a temporary directory; holds the database file unless in memory
	path string // the database file, or "" in memory
}

func openDB(t testing.TB, inFile bool, l Limits) *testDB {
	t.Helper()
	db := &testDB{dir: t.TempDir()}
	name := ":memory:"
	if inFile {
		db.path = filepath.Join(db.dir, "db")
		name = (&url.URL{Scheme: "file", OmitHost: true, Path: db.path, RawQuery: "vfs=" + testVFS}).String()
	}
	raw, err := sqlite3.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	db.raw = raw
	if inFile {
		// The server's own settings, so that a change by a client is visible:
		// WAL and synchronous FULL (decision record 0004), and values other
		// than SQLite's defaults for the rest.
		if err := raw.Exec(baseline); err != nil {
			t.Fatal(err)
		}
	}
	db.conn = apply(t, raw, l)
	t.Cleanup(func() {
		if err := db.conn.Close(); err != nil && !errors.Is(err, ErrBroken) {
			t.Error(err)
		}
	})
	rec.take()
	return db
}

const baseline = "PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA wal_autocheckpoint = 0; PRAGMA cache_size = -4000; PRAGMA application_id = 7; PRAGMA user_version = 3"

func apply(t testing.TB, raw *sqlite3.Conn, l Limits) *Conn {
	t.Helper()
	p, err := New(l)
	if err != nil {
		t.Fatal(err)
	}
	c, err := p.Apply(raw)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// both runs f against a database in memory and one in a file.
func both(t *testing.T, l Limits, f func(t *testing.T, db *testDB)) {
	for _, inFile := range []bool{false, true} {
		name := "memory"
		if inFile {
			name = "file"
		}
		t.Run(name, func(t *testing.T) { f(t, openDB(t, inFile, l)) })
	}
}

// run runs each statement in sql through Run in turn, and stops at the first
// error. Where Run refuses text after the first statement, it finds where
// that statement ends by preparing it again, under the policy, without
// stepping it.
func (db *testDB) run(sql string) error {
	rest := strings.ReplaceAll(sql, "{dir}", db.dir)
	for !blank(rest) {
		err := db.conn.Run(context.Background(), rest, nil)
		var pe *Error
		if !errors.As(err, &pe) || pe.Reason != ReasonStatementCount {
			return err
		}
		stmt, tail, perr := db.raw.Prepare(rest)
		if perr != nil || stmt == nil {
			return err
		}
		_ = stmt.Close()
		if err := db.conn.Run(context.Background(), rest[:len(rest)-len(tail)], nil); err != nil {
			return err
		}
		rest = tail
	}
	return nil
}

func (db *testDB) mustRun(t testing.TB, sql string) {
	t.Helper()
	if err := db.run(sql); err != nil {
		t.Fatalf("%.80s: %v", sql, err)
	}
}

// rows runs one statement with args and returns its rows as text.
func (db *testDB) rows(sql string, args ...any) ([][]string, error) {
	var out [][]string
	err := db.conn.Run(context.Background(), sql, func(s *sqlite3.Stmt) error {
		for i, a := range args {
			if err := bind(s, i+1, a); err != nil {
				return err
			}
		}
		for s.Step() {
			var row []string
			for i := range s.ColumnCount() {
				row = append(row, s.ColumnText(i))
			}
			out = append(out, row)
		}
		return s.Err()
	})
	return out, err
}

func wantRefused(t testing.TB, err error, reason Reason) {
	t.Helper()
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("got %v, want a refusal (%s)", err, reason)
	}
	if pe.Reason != reason {
		t.Fatalf("refused for %q, want %q: %v", pe.Reason, reason, err)
	}
}

// unrestricted runs f on db's connection with the authorizer suspended.
func (db *testDB) unrestricted(t testing.TB, f func()) {
	t.Helper()
	if err := db.conn.Privileged(func(*sqlite3.Conn) error { f(); return nil }); err != nil {
		t.Fatal(err)
	}
}

// server runs sql as the server's own statements.
func (db *testDB) server(t testing.TB, sql string) {
	t.Helper()
	if err := db.conn.Privileged(func(c *sqlite3.Conn) error { return c.Exec(sql) }); err != nil {
		t.Fatalf("%.80s: %v", sql, err)
	}
}

// ownedPragmas are pragmas whose values the server owns: every setting the
// build has except foreign_keys, which clients may set. Reading one returns
// its current value.
var ownedPragmas = []string{
	"analysis_limit", "application_id", "auto_vacuum", "automatic_index", "busy_timeout", "cache_size",
	"cache_spill", "cell_size_check", "checkpoint_fullfsync", "count_changes", "defer_foreign_keys",
	"empty_result_callbacks", "encoding", "full_column_names", "fullfsync", "hard_heap_limit",
	"ignore_check_constraints", "journal_mode", "journal_size_limit", "legacy_alter_table", "locking_mode",
	"max_page_count", "mmap_size", "page_size", "query_only", "read_uncommitted", "recursive_triggers",
	"reverse_unordered_selects", "secure_delete", "short_column_names", "soft_heap_limit", "synchronous",
	"temp_store", "temp_store_directory", "threads", "trusted_schema", "user_version", "wal_autocheckpoint",
	"writable_schema",
}

// owned returns every setting the server owns, the attached databases and
// the connection's limits, read without the authorizer.
func (db *testDB) owned(t testing.TB) map[string]string {
	t.Helper()
	m := map[string]string{}
	db.unrestricted(t, func() {
		read := func(key, sql string) {
			s, _, err := db.raw.Prepare(sql)
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			defer func() { _ = s.Close() }()
			var vals []string
			for s.Step() {
				for i := range s.ColumnCount() {
					vals = append(vals, s.ColumnText(i))
				}
			}
			if err := s.Err(); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			m[key] = strings.Join(vals, "|")
		}
		for _, p := range ownedPragmas {
			read(p, "PRAGMA "+p)
		}
		read("case_sensitive_like", "SELECT 'a' LIKE 'A'")
		read("integrity_check", "PRAGMA integrity_check")
		read("temp_store_file", "SELECT count(*) FROM pragma_database_list WHERE name = 'temp' AND file <> ''")
		read("attached", "SELECT group_concat(name) FROM pragma_database_list WHERE seq > 1")
		// Enforcing foreign keys is a client's choice (PRAGMA foreign_keys).
		for op := sqlite3.DBCONFIG_ENABLE_FKEY + 1; op <= sqlite3.DBCONFIG_REVERSE_SCANORDER; op++ {
			v, err := db.raw.Config(op)
			if err != nil {
				t.Fatal(err)
			}
			m[fmt.Sprint("dbconfig ", op)] = fmt.Sprint(v)
		}
		for id := sqlite3.LIMIT_LENGTH; id <= sqlite3.LIMIT_TRIGGER_STEPS; id++ {
			m[fmt.Sprint("limit ", id)] = fmt.Sprint(db.raw.Limit(id, -1))
		}
	})
	if db.path != "" {
		// With wal_autocheckpoint off, only a checkpoint changes the
		// database file's size.
		if fi, err := os.Stat(db.path); err == nil {
			m["file size"] = fmt.Sprint(fi.Size())
		}
	}
	return m
}

func (db *testDB) schema(t testing.TB) [][]string {
	t.Helper()
	var r [][]string
	db.unrestricted(t, func() {
		var err error
		r, err = db.rows("SELECT type, name, tbl_name, sql FROM sqlite_schema UNION ALL SELECT type, name, tbl_name, sql FROM sqlite_temp_schema ORDER BY 1, 2")
		if err != nil {
			t.Fatal(err)
		}
	})
	return r
}

// checkFiles fails unless SQLite opened, probed and deleted only the
// database's own files since the last call, and the directory holds nothing
// else.
func (db *testDB) checkFiles(t testing.TB) {
	t.Helper()
	names := rec.take()
	if db.path == "" {
		return
	}
	own := []string{db.path, db.path + "-journal", db.path + "-wal", db.path + "-shm"}
	for _, n := range names {
		if !slices.Contains(own, n) {
			t.Errorf("SQLite used the file %q", n)
		}
	}
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !slices.Contains(own, filepath.Join(db.dir, e.Name())) {
			t.Errorf("directory holds %q", e.Name())
		}
	}
}
