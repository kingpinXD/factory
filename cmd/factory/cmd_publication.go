package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/kingpinXD/factory/internal/reconcile"
)

func runSettlePublication(args []string, stdout, stderr io.Writer) int {
	const usage = `usage: factory settle-publication <item> --write <key> --outcome landed|not-landed [--head <sha>] [--pr <N>]
  landed: --head <sha> for a push, or --pr <N> for a PR write.
  not-landed: confirm the write did not land; then retry.
  If the remote head differs, restore the expected head (delete the branch if it was expected absent) before settling not-landed.
  If the verified commit is present, settle landed with --head <sha>.`
	fs := flag.NewFlagSet("settle-publication", flag.ContinueOnError)
	fs.SetOutput(stderr)
	key := fs.String("write", "", "the unresolved publication write key")
	outcome := fs.String("outcome", "", "landed or not-landed")
	head := fs.String("head", "", "the observed pushed commit")
	pr := fs.Int("pr", 0, "the observed pull request number")
	var id string
	if len(args) > 0 {
		id, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || id == "" || *key == "" || (*outcome != "landed" && *outcome != "not-landed") || *pr < 0 || (*outcome == "not-landed" && (*head != "" || *pr != 0)) {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	d, err := newDeps(stdout)
	if err == nil {
		err = reconcile.SettlePublication(context.Background(), d, id, *key, *outcome, *head, *pr)
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory settle-publication: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%s: publication %s settled as %s; use factory retry %s to resume\n", id, *key, *outcome, id)
	return 0
}
