package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/reconcile"
)

// epicCommandTimeout bounds the GitHub reads one epic command makes.
const epicCommandTimeout = 2 * time.Minute

// runMayMerge exits 0 when a PR may merge as far as the factory's plans go,
// and 1 when it may not yet or the factory cannot tell, printing why:
// factory may-merge <pr-url>.
func runMayMerge(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: factory may-merge <pr-url>")
		return 2
	}
	d, err := newDeps(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory may-merge: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), epicCommandTimeout)
	defer cancel()
	ok, why, err := reconcile.MayMerge(ctx, d, args[0])
	switch {
	case err != nil:
		fmt.Fprintf(stderr, "factory may-merge: %v\n", err)
		return 1
	case !ok:
		fmt.Fprintf(stdout, "not yet: %s\n", why)
		return 1
	}
	fmt.Fprintln(stdout, strings.TrimSuffix("may merge: "+why, ": "))
	return 0
}

// runDeployed records that a merged PR is deployed, for the links that
// wait on that: factory deployed <pr-url>.
func runDeployed(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: factory deployed <pr-url>")
		return 2
	}
	d, err := newDeps(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory deployed: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), epicCommandTimeout)
	defer cancel()
	epics, err := reconcile.Deployed(ctx, d, args[0])
	if err != nil {
		fmt.Fprintf(stderr, "factory deployed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "recorded for %s\n", strings.Join(epics, ", "))
	return 0
}

// runReplan asks an epic's planner to re-check every issue, with a note:
// factory replan <epic-id> "<note>".
func runReplan(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintln(stderr, `usage: factory replan <epic-id> "<note>"`)
		return 2
	}
	d, err := newDeps(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory replan: %v\n", err)
		return 1
	}
	logged, err := reconcile.Replan(context.Background(), d, args[0], args[1])
	if err != nil {
		fmt.Fprintf(stderr, "factory replan: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "seq: %d\n", logged.Seq)
	return 0
}

// runAnswer gives the user's answer to a question or a result that waits on
// them: factory answer <epic-id> <issue> "<choice>".
func runAnswer(args []string, stdout, stderr io.Writer) int {
	if len(args) != 3 {
		fmt.Fprintln(stderr, `usage: factory answer <epic-id> <issue> "<choice>"`)
		return 2
	}
	d, err := newDeps(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory answer: %v\n", err)
		return 1
	}
	did, err := reconcile.AnswerTo(context.Background(), d, args[0], args[1], args[2])
	if err != nil {
		fmt.Fprintf(stderr, "factory answer: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, did)
	return 0
}
