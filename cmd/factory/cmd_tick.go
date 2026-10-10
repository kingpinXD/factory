package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/notify"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
)

// Deadlines: one adapter call, the account step, and one repo job (a clone
// of a large repo can take minutes).
const (
	callTimeout    = 30 * time.Second
	accountTimeout = 20 * time.Second
	repoJobTimeout = 30 * time.Minute
)

// newDeps returns what the tick and factory add act through. Sessions get
// this binary as FACTORY_BIN. Tests replace it.
var newDeps = func(out io.Writer) (reconcile.Deps, error) {
	brain := blueprint.Brain()
	cl, err := claude.New(brain)
	if err != nil {
		return reconcile.Deps{}, err
	}
	if cl.FactoryBin, err = os.Executable(); err != nil {
		return reconcile.Deps{}, err
	}
	d := reconcile.Deps{
		Brain:          brain,
		Now:            time.Now,
		GitHub:         gh.Client{Runner: proc.Exec{}},
		Claude:         cl,
		Notify:         notify.Slack{Brain: brain},
		GHToken:        os.Getenv("GH_TOKEN"),
		CallTimeout:    callTimeout,
		AccountTimeout: accountTimeout,
		Out:            out,
	}
	d.Account = reconcile.PlanAccount{Deps: d, Probe: reconcile.Probe{Runner: proc.Exec{}, Claude: cl.Bin, Home: cl.Home}}
	return d, nil
}

// runTick runs one tick: factory tick [--dry-run].
func runTick(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("tick", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dry := fs.Bool("dry-run", false, "print what a tick would do, and change nothing")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: factory tick [--dry-run]")
		return 2
	}
	d, err := newDeps(stdout)
	if err == nil {
		err = reconcile.Tick(context.Background(), d, *dry)
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory tick: %v\n", err)
		return 1
	}
	return 0
}

// runRepoWorker does the repo work the tick queued: factory repo-worker.
func runRepoWorker(args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "usage: factory repo-worker")
		return 2
	}
	g := git.Client{Runner: proc.Exec{}}
	if err := reconcile.RepoWorker(context.Background(), blueprint.Brain(), g, repoJobTimeout, stdout); err != nil {
		fmt.Fprintf(stderr, "factory repo-worker: %v\n", err)
		return 1
	}
	return 0
}

// runAdd adds an epic:
// factory add <input> [--set step=tier]... [--effort step=level]...
func runAdd(args []string, stdout, stderr io.Writer) int {
	const usage = `usage: factory add <issue-url | epic-url | plan-file | "text"> [--set step=tier]... [--effort step=level]...`
	ov := reconcile.Overrides{Tier: map[string]string{}, Effort: map[string]string{}}
	fs := flag.NewFlagSet("add", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Var(pairs(ov.Tier), "set", "step=tier: the tier a step runs on for this input")
	fs.Var(pairs(ov.Effort), "effort", "step=level: the effort a step runs at for this input")
	var input string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		input, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	if input == "" && fs.NArg() == 1 {
		input = fs.Arg(0)
	} else if fs.NArg() != 0 || input == "" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	d, err := newDeps(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory add: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := reconcile.Add(ctx, d, input, ov)
	if id != "" {
		fmt.Fprintf(stdout, "added %s\n", id)
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory add: %v\n", err)
		return 1
	}
	return 0
}

// pairs is a repeatable step=value flag.
type pairs map[string]string

func (p pairs) String() string { return "" }

func (p pairs) Set(s string) error {
	step, value, ok := strings.Cut(s, "=")
	if !ok || step == "" {
		return errors.New("want step=value")
	}
	p[step] = value
	return nil
}
