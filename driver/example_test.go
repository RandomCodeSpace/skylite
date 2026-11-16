package skylite_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

// The example compiles with the package's tests, so the documented usage
// stays valid; it has no output, so it does not run.
func ExampleNewConnector() {
	c, err := skylite.NewConnector(skylite.Config{
		Endpoint: "https://skylite.example.internal",
		Database: "library",
		Token: skylite.TokenFunc(func(context.Context) (string, error) {
			// A projected service account token, read again on every request.
			b, err := os.ReadFile("/var/run/secrets/tokens/skylite")
			return strings.TrimSpace(string(b)), err
		}),
	})
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer func() { _ = db.Close() }() // also closes c
	var title string
	if err := db.QueryRowContext(context.Background(), "SELECT title FROM books WHERE id > ?", 10).Scan(&title); err != nil {
		log.Fatal(err)
	}
	fmt.Println(title)
}
