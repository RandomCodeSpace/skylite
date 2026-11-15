// Command flightsql runs the Apache Arrow Go SQLite Flight SQL example server
// on a SQLite file. It exists only to test the Arrow Flight SQL JDBC driver
// against a real server (decision record 0003) and is not part of skylite.
package main

import (
	"database/sql"
	"flag"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/example"
	_ "modernc.org/sqlite" // registers the "sqlite" driver, as the example does
)

func main() {
	host := flag.String("host", "127.0.0.1", "address to bind to")
	port := flag.Int("port", 31337, "port to bind to")
	path := flag.String("db", "", "SQLite database file (required)")
	flag.Parse()
	if *path == "" {
		log.Fatal("-db is required")
	}

	// A file in WAL mode, so that a reader on one connection does not block on,
	// or see, another connection's uncommitted write. The example's own CreateDB
	// uses a shared-cache in-memory database with sample tables instead.
	db, err := sql.Open("sqlite", "file:"+*path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	srv, err := example.NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatal(err)
	}
	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))
	if err := server.Init(net.JoinHostPort(*host, strconv.Itoa(*port))); err != nil {
		log.Fatal(err)
	}
	server.SetShutdownOnSignals(os.Interrupt)
	log.Println("Flight SQL server listening on", server.Addr())
	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
