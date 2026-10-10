package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/kingpinXD/factory/internal/reconcile"
)

// runStatus prints the factory's load and what waits on the user:
// factory status [--json].
func runStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	asJSON := fs.Bool("json", false, "print the report as JSON (its schema is in README.md)")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: factory status [--json]")
		return 2
	}
	d, err := newDeps(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory status: %v\n", err)
		return 1
	}
	rep, err := reconcile.Status(context.Background(), d)
	if err != nil {
		fmt.Fprintf(stderr, "factory status: %v\n", err)
		return 1
	}
	if !*asJSON {
		rep.WriteText(stdout)
		return 0
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fmt.Fprintf(stderr, "factory status: %v\n", err)
		return 1
	}
	return 0
}

// runStop cancels an entity and the work under it:
// factory stop <id> --reason "<why>".
func runStop(args []string, stdout, stderr io.Writer) int {
	const usage = `usage: factory stop <id> --reason "<why>"`
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	fs.SetOutput(stderr)
	reason := fs.String("reason", "", "why the work stops")
	var id string
	if len(args) > 0 {
		id, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || id == "" || *reason == "" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	return request(stdout, stderr, "stop", id, func(ctx context.Context, d reconcile.Deps) error {
		return reconcile.Stop(ctx, d, id, *reason)
	})
}

// runRetry returns an entity that waits on the user to the state it left:
// factory retry <id>.
func runRetry(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: factory retry <id>")
		return 2
	}
	return request(stdout, stderr, "retry", args[0], func(ctx context.Context, d reconcile.Deps) error {
		return reconcile.Retry(ctx, d, args[0])
	})
}

// request runs a user command that logs a request for the next tick, and
// prints that it did, or why the entity's machine refused it.
func request(stdout, stderr io.Writer, name, id string, ask func(context.Context, reconcile.Deps) error) int {
	d, err := newDeps(stdout)
	if err == nil {
		err = ask(context.Background(), d)
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory %s: %v\n", name, err)
		return 1
	}
	fmt.Fprintf(stdout, "%s: %s requested; the next tick acts on it\n", id, name)
	return 0
}
