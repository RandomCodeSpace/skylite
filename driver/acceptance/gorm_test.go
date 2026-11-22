// Package acceptance runs GORM, through its sqlite dialector, on the Go
// driver against a skylite server built from this tree. It is its own
// module so that the driver's module gains no GORM dependency.
package acceptance

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

// endpoint and token reach the server TestMain started.
var endpoint, token string

func TestMain(m *testing.M) {
	code, err := serve(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "acceptance:", err)
		code = 1
	}
	os.Exit(code)
}

// serve builds skylite from this tree, serves database "gorm" from a local
// store with a key it generates, and runs the tests against it.
func serve(m *testing.M) (int, error) {
	dir, err := os.MkdirTemp("", "skylite-gorm-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	bin := filepath.Join(dir, "skylite")
	build := exec.Command("go", "build", "-o", bin, "./cmd/skylite") //nolint:gosec // a path in our own temporary directory
	build.Dir, build.Stdout, build.Stderr = "../..", os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		return 0, fmt.Errorf("building skylite: %w", err)
	}
	cfg, err := config(dir)
	if err != nil {
		return 0, err
	}
	cmd := exec.Command(bin, "serve", "-config", cfg) //nolint:gosec // the binary just built
	cmd.Env = append(os.Environ(), "TMPDIR="+dir)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	lines := bufio.NewScanner(stderr)
	for endpoint == "" && lines.Scan() {
		var l struct{ Msg, Listen string }
		if json.Unmarshal(lines.Bytes(), &l) == nil && l.Msg == "serving" {
			endpoint = "http://" + l.Listen
		}
		if os.Getenv("SKYLITE_LOG") != "" {
			fmt.Fprintln(os.Stderr, lines.Text())
		}
	}
	if endpoint == "" {
		return 0, errors.New("the server did not start")
	}
	go func() {
		for lines.Scan() {
			if os.Getenv("SKYLITE_LOG") != "" {
				fmt.Fprintln(os.Stderr, lines.Text())
			}
		}
	}()
	if err := create(); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// config writes a plaintext configuration with a JWKS file and a local
// store in dir, sets token to an admin token, and returns the path.
func config(dir string) (string, error) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	set, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: k.Public(), KeyID: "k1", Algorithm: string(jose.ES256), Use: "sig"}}})
	if err != nil {
		return "", err
	}
	js, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: k}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		return "", err
	}
	const iss = "https://issuer.invalid"
	token, err = jwt.Signed(js).Claims(map[string]any{"iss": iss, "sub": "gorm", "aud": "skylite", "exp": time.Now().Add(time.Hour).Unix()}).Serialize()
	if err != nil {
		return "", err
	}
	c, err := json.Marshal(map[string]any{
		"listen": "127.0.0.1:0", "plaintext": true, "databases": []string{"gorm"},
		"auth": map[string]any{
			"issuers": []any{map[string]any{"issuer": iss, "audience": "skylite", "jwks_file": filepath.Join(dir, "jwks.json")}},
			"rules":   []any{map[string]any{"issuer": iss, "subject": "gorm", "database": "*", "role": "admin"}},
		},
		"storage": map[string]any{"localfs": map[string]any{"path": filepath.Join(dir, "store")}},
	})
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "skylite.json")
	for p, b := range map[string][]byte{filepath.Join(dir, "jwks.json"): set, path: c} {
		if err := os.WriteFile(p, b, 0o600); err != nil {
			return "", err
		}
	}
	return path, os.MkdirAll(filepath.Join(dir, "store"), 0o700)
}

// create creates database "gorm" and waits until the server leads it.
func create() error {
	req, _ := http.NewRequest(http.MethodPut, endpoint+"/v1/databases/gorm", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT: HTTP %d", resp.StatusCode)
	}
	db := open()
	defer func() { _ = db.Close() }()
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		err := db.Ping()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the server does not lead the database: %w", err)
		}
	}
}

func open() *sql.DB {
	c, err := skylite.NewConnector(skylite.Config{
		Endpoint: endpoint, Database: "gorm", AllowPlaintext: true,
		Token: skylite.TokenFunc(func(context.Context) (string, error) { return token, nil }),
	})
	if err != nil {
		panic(err)
	}
	return sql.OpenDB(c)
}

// gormDB opens GORM's sqlite dialector on the driver's *sql.DB.
func gormDB(t *testing.T) *gorm.DB {
	t.Helper()
	sqlDB := open()
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(sqlite.Dialector{Conn: sqlDB}, &gorm.Config{Logger: logger.Discard, TranslateError: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type Author struct {
	ID      uint
	Name    string `gorm:"uniqueIndex;not null"`
	Books   []Book
	Profile Profile
}

type Profile struct {
	ID       uint
	AuthorID uint
	Bio      string
}

type Book struct {
	gorm.Model
	Title    string `gorm:"not null"`
	AuthorID uint
	Author   Author
	Tags     []Tag `gorm:"many2many:book_tags"`
	Pages    int
}

type Tag struct {
	ID   uint
	Name string `gorm:"uniqueIndex"`
}

func migrate(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.AutoMigrate(&Author{}, &Profile{}, &Book{}, &Tag{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
}

func TestAutoMigrate(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	migrate(t, db) // a second run finds every table and column
	for _, table := range []string{"authors", "profiles", "books", "tags", "book_tags"} {
		if !db.Migrator().HasTable(table) {
			t.Errorf("table %s is missing", table)
		}
	}
	if !db.Migrator().HasIndex(&Author{}, "Name") || !db.Migrator().HasColumn(&Book{}, "Pages") {
		t.Error("an index or column is missing")
	}
}

func TestCRUD(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	a := Author{Name: "Herbert"}
	if err := db.Create(&a).Error; err != nil || a.ID == 0 {
		t.Fatalf("Create: %v, id %d", err, a.ID)
	}
	b := Book{Title: "Dune", AuthorID: a.ID, Pages: 412}
	if err := db.Create(&b).Error; err != nil || b.ID == 0 || b.CreatedAt.IsZero() {
		t.Fatalf("Create book: %v, %+v", err, b)
	}
	var got Book
	if err := db.First(&got, b.ID).Error; err != nil || got.Title != "Dune" || got.Pages != 412 || !got.CreatedAt.Equal(b.CreatedAt) {
		t.Fatalf("First: %v, %+v", err, got)
	}
	if err := db.Model(&got).Updates(Book{Title: "Dune Messiah", Pages: 256}).Error; err != nil {
		t.Fatalf("Updates: %v", err)
	}
	if res := db.Model(&Book{}).Where("pages > ?", 1000).Update("pages", 1); res.Error != nil || res.RowsAffected != 0 {
		t.Fatalf("an update matching nothing: %v, %d rows", res.Error, res.RowsAffected)
	}
	var titles []string
	if err := db.Model(&Book{}).Where("author_id = ?", a.ID).Pluck("title", &titles).Error; err != nil || len(titles) != 1 || titles[0] != "Dune Messiah" {
		t.Fatalf("Pluck: %v, %v", err, titles)
	}
	if res := db.Delete(&got); res.Error != nil || res.RowsAffected != 1 {
		t.Fatalf("Delete: %v, %d rows", res.Error, res.RowsAffected)
	}
	if err := db.First(&Book{}, b.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("First of a soft-deleted book: %v; want ErrRecordNotFound", err)
	}
	var n int64
	if err := db.Unscoped().Model(&Book{}).Where("id = ?", b.ID).Count(&n).Error; err != nil || n != 1 {
		t.Fatalf("Unscoped count: %v, %d", err, n)
	}
	if err := db.Unscoped().Delete(&Book{}, b.ID).Error; err != nil {
		t.Fatalf("Unscoped Delete: %v", err)
	}
}

func TestAssociations(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	a := Author{
		Name:    "Le Guin",
		Profile: Profile{Bio: "Earthsea"},
		Books: []Book{
			{Title: "A Wizard of Earthsea", Tags: []Tag{{Name: "fantasy"}, {Name: "classic"}}},
			{Title: "The Dispossessed", Tags: []Tag{{Name: "sf"}}},
		},
	}
	if err := db.Create(&a).Error; err != nil {
		t.Fatalf("Create with associations: %v", err)
	}
	var got Author
	if err := db.Preload("Books.Tags").Preload("Profile").First(&got, a.ID).Error; err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if len(got.Books) != 2 || got.Profile.Bio != "Earthsea" || len(got.Books[0].Tags)+len(got.Books[1].Tags) != 3 {
		t.Fatalf("preloaded %+v", got)
	}
	book := got.Books[1]
	if err := db.Model(&book).Association("Tags").Append(&Tag{Name: "utopia"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if n := db.Model(&book).Association("Tags").Count(); n != 2 {
		t.Fatalf("tags %d; want 2", n)
	}
	var b Book
	if err := db.Joins("Author").First(&b, book.ID).Error; err != nil || b.Author.Name != "Le Guin" {
		t.Fatalf("Joins: %v, %+v", err, b.Author)
	}
}

func TestTransactions(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Author{Name: "Committed"}).Error; err != nil {
			return err
		}
		var n int64
		if err := tx.Model(&Author{}).Where("name = ?", "Committed").Count(&n).Error; err != nil || n != 1 {
			return fmt.Errorf("the transaction does not see its own write: %d, %w", n, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	errRollback := errors.New("roll back")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Author{Name: "Rolled back"}).Error; err != nil {
			return err
		}
		return errRollback
	}); !errors.Is(err, errRollback) {
		t.Fatalf("Transaction: %v", err)
	}
	var names []string
	if err := db.Model(&Author{}).Where("name IN ?", []string{"Committed", "Rolled back"}).Pluck("name", &names).Error; err != nil || len(names) != 1 || names[0] != "Committed" {
		t.Fatalf("authors %v, %v; want only the committed one", names, err)
	}

	// A manual transaction, with a failing statement, then a rollback.
	tx := db.Begin()
	if err := tx.Create(&Author{Name: "Committed"}).Error; !errors.Is(err, gorm.ErrDuplicatedKey) && !isConstraint(err) {
		t.Fatalf("duplicate in a transaction: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	t.Run("nested savepoints", func(t *testing.T) {
		// GORM's sqlite dialector ignores the errors of SAVEPOINT and
		// ROLLBACK TO, so a server that refuses them would make a nested
		// Transaction's rollback a silent no-op: probe the server first.
		tx := db.Begin()
		err := tx.Exec("SAVEPOINT probe").Error
		_ = tx.Rollback()
		var se *skylite.Error
		if errors.As(err, &se) && se.Code == skylite.CodeInvalidRequest && strings.Contains(se.Message, "transaction control") {
			t.Skip("the server refuses SAVEPOINT inside a channel transaction (" + se.Message + "); " +
				"GORM's nested Transaction then commits the inner work it was told to roll back, because the dialector ignores the error")
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&Author{Name: "Outer"}).Error; err != nil {
				return err
			}
			if err := tx.Transaction(func(tx2 *gorm.DB) error {
				if err := tx2.Create(&Author{Name: "Inner"}).Error; err != nil {
					return err
				}
				return errRollback
			}); !errors.Is(err, errRollback) {
				return fmt.Errorf("inner transaction: %w", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		var names []string
		db.Model(&Author{}).Where("name IN ?", []string{"Outer", "Inner"}).Pluck("name", &names)
		if len(names) != 1 || names[0] != "Outer" {
			t.Fatalf("authors %v; want Outer only", names)
		}
	})
}

func TestBatchCreate(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	a := Author{Name: "Batch"}
	if err := db.Create(&a).Error; err != nil {
		t.Fatal(err)
	}
	books := make([]Book, 250)
	for i := range books {
		books[i] = Book{Title: fmt.Sprintf("Volume %d", i), AuthorID: a.ID, Pages: i}
	}
	if err := db.CreateInBatches(&books, 100).Error; err != nil {
		t.Fatalf("CreateInBatches: %v", err)
	}
	for i, b := range books {
		if b.ID == 0 || i > 0 && b.ID <= books[i-1].ID {
			t.Fatalf("book %d has id %d after %d", i, b.ID, books[max(i-1, 0)].ID)
		}
	}
	var n int64
	if err := db.Model(&Book{}).Where("author_id = ?", a.ID).Count(&n).Error; err != nil || n != 250 {
		t.Fatalf("count %d, %v; want 250", n, err)
	}
}

func TestErrors(t *testing.T) {
	db := gormDB(t)
	migrate(t, db)
	if err := db.Create(&Author{Name: "Unique"}).Error; err != nil {
		t.Fatal(err)
	}
	err := db.Create(&Author{Name: "Unique"}).Error
	if !isConstraint(err) {
		t.Fatalf("duplicate: %v; want the server's constraint error", err)
	}
	var se *skylite.Error
	if errors.As(err, &se); se.SQLiteExtendedCode != 2067 { // SQLITE_CONSTRAINT_UNIQUE
		t.Fatalf("extended code %d; want 2067", se.SQLiteExtendedCode)
	}
	if err := db.First(&Author{}, "name = ?", "nobody").Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("First of nothing: %v", err)
	}
	if err := db.Exec("INSERT INTO no_such_table VALUES (1)").Error; !errors.As(err, &se) || se.Code != skylite.CodeSQLError {
		t.Fatalf("a missing table: %v", err)
	}
	if err := db.Exec("ATTACH 'x' AS y").Error; !errors.As(err, &se) || se.Code != skylite.CodeStatementForbidden {
		t.Fatalf("ATTACH: %v; want statement_forbidden", err)
	}
	// The dialector passes an unknown outcome through untranslated, as ADR
	// 0005 requires with TranslateError on.
	unknown := fmt.Errorf("x: %w", skylite.ErrOutcomeUnknown)
	if got := (sqlite.Dialector{}).Translate(unknown); !errors.Is(got, skylite.ErrOutcomeUnknown) {
		t.Fatalf("Translate(%v) = %v", unknown, got)
	}
}

// isConstraint reports whether err is the server's SQLITE_CONSTRAINT error.
func isConstraint(err error) bool {
	var se *skylite.Error
	return errors.As(err, &se) && se.Code == skylite.CodeSQLError && se.SQLiteCode == 19
}
