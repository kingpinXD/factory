// Command factory is the CLI for the agent factory. It dispatches by name;
// commands without a handler are planned but not built yet.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/kingpinXD/factory/internal/blueprint"
)

var commands = []string{
	"add", "tick", "status", "answer", "retry", "stop", "replan", "check",
	"blueprint", "event", "heartbeat", "inbox", "query", "may-merge",
	"deployed", "issue", "agents", "launchd", "lease-ok", "repo-worker",
}

var handlers = map[string]func(args []string, stdout, stderr io.Writer) int{
	"check":     runCheck,
	"blueprint": runBlueprint,
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
	if h, ok := handlers[name]; ok {
		return h(args[1:], stdout, stderr)
	}
	fmt.Fprintf(stderr, "factory: not implemented yet: %s\n", name)
	return 1
}

// runCheck validates the brain's blueprint and prints each problem.
func runCheck(args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "usage: factory check")
		return 2
	}
	brain := blueprint.Brain()
	_, problems := blueprint.Check(brain, nil)
	for _, p := range problems {
		fmt.Fprintln(stdout, p)
	}
	if len(problems) > 0 {
		fmt.Fprintf(stderr, "factory: %s fails its check (%d problems)\n", blueprint.Path(brain), len(problems))
		return 1
	}
	fmt.Fprintf(stdout, "ok: %s\n", blueprint.Path(brain))
	return 0
}

// runBlueprint prints the brain's blueprint as JSON.
func runBlueprint(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || args[0] != "--json" {
		fmt.Fprintln(stderr, "usage: factory blueprint --json")
		return 2
	}
	b, err := blueprint.Load(blueprint.Path(blueprint.Brain()))
	if err != nil {
		fmt.Fprintf(stderr, "factory: %v\n", err)
		return 1
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(b); err != nil {
		fmt.Fprintf(stderr, "factory: %v\n", err)
		return 1
	}
	return 0
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: factory <command> [arguments]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, name := range commands {
		if _, ok := handlers[name]; ok {
			fmt.Fprintf(w, "  %s\n", name)
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Planned (not implemented yet):")
	for _, name := range commands {
		if _, ok := handlers[name]; !ok {
			fmt.Fprintf(w, "  %s\n", name)
		}
	}
}
