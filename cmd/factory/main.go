// Command factory is the CLI for the agent factory. Every command is planned
// but not built yet, so it only prints usage and dispatches by name.
package main

import (
	"fmt"
	"io"
	"os"
	"slices"
)

var commands = []string{
	"add", "tick", "status", "answer", "retry", "stop", "replan", "check",
	"blueprint", "event", "heartbeat", "inbox", "query", "may-merge",
	"deployed", "issue", "agents", "launchd", "lease-ok", "repo-worker",
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		printUsage(stdout)
		return 0
	}
	name := args[0]
	if !slices.Contains(commands, name) {
		fmt.Fprintf(stderr, "factory: unknown command: %s (run factory --help)\n", name)
		return 2
	}
	fmt.Fprintf(stderr, "factory: not implemented yet: %s\n", name)
	return 1
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: factory <command> [arguments]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands (not implemented yet):")
	for _, name := range commands {
		fmt.Fprintf(w, "  %s\n", name)
	}
}
