package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/reconcile"
)

func runCodexVerify(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("codex-verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brain := fs.String("brain", blueprint.Brain(), "selected brain")
	profile := fs.String("profile", "", "pinned Codex executor profile")
	contract := fs.Int("contract", 1, "Codex capability contract: 1 or 2")
	repo := fs.String("repo", "", "authorized registered private repository for contract 2")
	base := fs.String("base", "", "registered base branch for contract 2")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return 2
	}
	if *contract != 1 && *contract != 2 || *contract == 1 && (*repo != "" || *base != "") || *contract == 2 && (*repo == "" || *base == "") {
		fmt.Fprintln(stderr, "factory codex-verify: contract 2 requires an explicit authorized --repo and --base; contract 1 accepts neither")
		return 2
	}
	abs, err := filepath.Abs(*brain)
	if err != nil {
		fmt.Fprintf(stderr, "factory codex-verify: %v\n", err)
		return 1
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		fmt.Fprintf(stderr, "factory codex-verify: %v\n", err)
		return 1
	}
	b, err := blueprint.Load(blueprint.Path(canonical))
	if err != nil {
		fmt.Fprintf(stderr, "factory codex-verify: %v\n", err)
		return 1
	}
	selection, err := executor.Prepare(canonical, "codex-verification", b, executor.Options{Executor: "codex", ProfilePath: *profile})
	if err != nil {
		fmt.Fprintf(stderr, "factory codex-verify: %v\n", err)
		return 1
	}
	binary, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "factory codex-verify: %v\n", err)
		return 1
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	verifyErr := error(nil)
	if *contract == 1 {
		verifyErr = codex.VerifyAcceptance(ctx, canonical, binary, selection)
	} else {
		verifyErr = verifyContract2(ctx, canonical, binary, selection, codex.AcceptanceRepository{Repo: *repo, Base: *base})
	}
	if verifyErr != nil {
		fmt.Fprintf(stderr, "factory codex-verify: %v\n", verifyErr)
		return 1
	}
	fmt.Fprintln(stdout, "Codex runtime acceptance passed for the pinned profile. A live sample epic is a separate check.")
	return 0
}

func verifyContract2(ctx context.Context, brain, binary string, selection executor.Selection, target codex.AcceptanceRepository) (result error) {
	if _, err := codex.Contract2AcceptanceAuthority(ctx, brain, selection); err == nil {
		return nil
	}
	probe, err := codex.PrepareContract2Probe(ctx, brain, binary, selection, target)
	if probe != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			result = errors.Join(result, probe.Close(cleanup))
		}()
	}
	if err != nil {
		return err
	}
	ref, err := reconcile.PrepareAcceptanceRead(ctx, brain, binary, probe.ReadGrant())
	if err != nil {
		return err
	}
	read, err := reconcile.RunProgramRead(ctx, brain, binary, ref)
	if err != nil {
		return err
	}
	return probe.Complete(ctx, read)
}
