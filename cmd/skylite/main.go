// Command skylite is the skylite server binary.
//
// The project is pre-alpha: today the binary only reports its version and
// prints usage.
package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
)

const usage = `skylite - a SQL database server where object storage is the source of truth.

Status: pre-alpha. This binary does not run a server yet.

Usage:
  skylite <command>

Commands:
  version    Print the skylite version and exit.
  help       Print this help text and exit. Also: -h, --help, or no command.

Exit status:
  0  success
  2  unknown command

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
	case "version":
		_, _ = fmt.Fprintf(stdout, "skylite %s\n", version())
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "skylite: unknown command %q\nRun 'skylite help' for usage.\n", args[0])
		return 2
	}
}

// version returns the module version stamped by the Go toolchain, so
// `go install ...@vX.Y.Z` reports vX.Y.Z without ldflags.
func version() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "devel"
}
