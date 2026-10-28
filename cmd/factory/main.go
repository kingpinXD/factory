// Command factory is the CLI for the agent factory. It dispatches by name.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/kingpinXD/factory/internal/blueprint"
)

var commands = []string{
	"add", "tick", "status", "answer", "retry", "stop", "review-asked", "replan", "check",
	"blueprint", "event", "heartbeat", "inbox", "query", "may-merge",
	"deployed", "issue", "agents", "launchd", "lease-ok", "repo-worker",
	"version", "helper", "z-runner", "z-attach", "z-preflight", "ui",
}

var handlers = map[string]func(args []string, stdout, stderr io.Writer) int{
	"check":        runCheck,
	"blueprint":    runBlueprint,
	"agents":       runAgents,
	"event":        runEvent,
	"inbox":        runInbox,
	"heartbeat":    runHeartbeat,
	"lease-ok":     runLeaseOK,
	"query":        runQuery,
	"tick":         runTick,
	"repo-worker":  runRepoWorker,
	"add":          runAdd,
	"may-merge":    runMayMerge,
	"deployed":     runDeployed,
	"replan":       runReplan,
	"answer":       runAnswer,
	"issue":        runIssue,
	"status":       runStatus,
	"stop":         runStop,
	"retry":        runRetry,
	"review-asked": runReviewAsked,
	"launchd":      runLaunchd,
	"version":      runVersion,
	"helper":       runHelper,
	"z-runner":     runZRunner,
	"z-attach":     runZAttach,
	"z-preflight":  runZPreflight,
	"ui":           runUI,
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
	if name == "--version" {
		name = "version"
	}
	if _, ok := handlers[name]; !ok {
		fmt.Fprintf(stderr, "factory: unknown command: %s (run factory --help)\n", name)
		return 2
	}
	if name != "ui" {
		useSecretsToken(blueprint.Brain())
	}
	return handlers[name](args[1:], stdout, stderr)
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

// readBuildInfo reads the binary's build info. Tests replace it.
var readBuildInfo = debug.ReadBuildInfo

// runVersion prints the module version and the git commit the binary was built from.
func runVersion(args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "usage: factory version")
		return 2
	}
	fmt.Fprintln(stdout, versionLine(readBuildInfo()))
	return 0
}

// versionLine formats build info as "factory <version> <commit>", with unknown for a missing part.
func versionLine(info *debug.BuildInfo, ok bool) string {
	if !ok {
		return "factory unknown unknown"
	}
	version, commit := "unknown", "unknown"
	if v := info.Main.Version; v != "" && v != "(devel)" {
		version = v
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && s.Value != "" {
			commit = s.Value
		}
	}
	return fmt.Sprintf("factory %s %s", version, commit)
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: factory <command> [arguments]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, name := range commands {
		fmt.Fprintf(w, "  %s\n", name)
	}
}
