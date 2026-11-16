package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/reconcile"
)

// Credentials enter only the authenticated programme process after its durable claim.
func runEffectDispatch(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("effect-dispatch", flag.ContinueOnError)
	fs.SetOutput(stderr)
	claim := fs.String("claim", "", "")
	nonce := fs.String("nonce", "", "")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *claim == "" || *nonce == "" {
		fmt.Fprintln(stderr, "usage: factory effect-dispatch --claim <claim> --nonce <nonce>")
		return 2
	}
	ctx := context.Background()
	kind, err := reconcile.ProgramDispatchKind(ctx, blueprint.Brain(), *claim)
	if err != nil {
		fmt.Fprintf(stderr, "factory effect-dispatch: %v\n", err)
		return 1
	}
	var dispatcher interface {
		Bootstrap(context.Context) (reconcile.EffectProgramBootstrap, error)
		Run(context.Context) error
	}
	switch kind {
	case "normal_effect":
		dispatcher, err = reconcile.ClaimEffectDispatcher(ctx, blueprint.Brain(), *claim, *nonce)
	case "program_read":
		dispatcher, err = reconcile.ClaimProgrammeRead(ctx, blueprint.Brain(), *claim, *nonce)
	case "acceptance_read":
		dispatcher, err = reconcile.ClaimAcceptanceRead(ctx, blueprint.Brain(), *claim, *nonce)
	default:
		fmt.Fprintln(stderr, "factory effect-dispatch: unsupported selected process subject")
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory effect-dispatch: %v\n", err)
		return 1
	}
	bootstrap, err := dispatcher.Bootstrap(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "factory effect-dispatch: %v\n", err)
		return 1
	}
	if err := os.Setenv("GH_CONFIG_DIR", bootstrap.GHConfigDir); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	useSecretsToken(bootstrap.Brain)
	if err := dispatcher.Run(ctx); err != nil {
		fmt.Fprintf(stderr, "factory effect-dispatch: %v\n", err)
		return 1
	}
	return 0
}
