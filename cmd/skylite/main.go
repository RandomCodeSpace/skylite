// Command skylite is the skylite server binary.
//
// The project is pre-alpha: the server creates databases and serves
// nothing else yet.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/RandomCodeSpace/skylite/internal/server"
)

const usage = `skylite - a SQL database server where object storage is the source of truth.

Status: pre-alpha. The server authenticates requests and creates databases;
it serves no queries yet.

Usage:
  skylite <command>

Commands:
  serve -config <file>
             Run the HTTPS server with the JSON configuration in <file>. It
             logs JSON lines to standard error and stops on SIGTERM or
             SIGINT after running requests finish, waiting at most a minute.
             A second SIGTERM or SIGINT stops it at once.
  version    Print the skylite version and exit.
  help       Print this help text and exit. Also: -h, --help, or no command.

Exit status:
  0  success, or the server stopped cleanly
  1  the server could not start or failed, or cut running requests to stop
  2  unknown command or bad arguments

Documentation: https://randomcodespace.github.io/skylite/
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = io.WriteString(stdout, usage)
		return 0
	}
	switch args[0] {
	case "help", "-h", "--help":
		_, _ = io.WriteString(stdout, usage)
		return 0
	case "serve":
		return serve(args[1:], stderr)
	case "version":
		_, _ = fmt.Fprintf(stdout, "skylite %s\n", version())
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "skylite: unknown command %q\nRun 'skylite help' for usage.\n", args[0])
		return 2
	}
}

// serve runs the server until SIGTERM or SIGINT.
func serve(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", "", "path of the JSON configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *path == "" || fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "skylite serve: usage: skylite serve -config <file>")
		return 2
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	cfg, err := server.Load(*path)
	if err != nil {
		log.Error("skylite serve: " + err.Error())
		return 1
	}
	s, err := server.New(cfg, log)
	if err != nil {
		log.Error("skylite serve: " + err.Error())
		return 1
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Error("skylite serve: " + err.Error())
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		stop() // a second signal gets its default action and ends the process
	}()
	log.Info("serving", slog.String("version", version()), slog.String("listen", ln.Addr().String()))
	if err := s.Serve(ctx, ln); err != nil {
		log.Error("skylite serve: " + err.Error())
		return 1
	}
	log.Info("stopped")
	return 0
}

// version returns the module version stamped by the Go toolchain, so
// `go install ...@vX.Y.Z` reports vX.Y.Z without ldflags.
func version() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "devel"
}
