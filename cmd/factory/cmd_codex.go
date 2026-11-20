package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/reconcile"
	"io"
	"os"
)

func runCodexHost(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("codex-host", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brain := fs.String("brain", "", "owned brain")
	id := fs.String("session", "", "owned session")
	attempt := fs.String("attempt", "", "owned attempt")
	claim := fs.String("claim", "", "private one-use claim")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return 2
	}
	if (*attempt == "") != (*claim == "") {
		return 2
	}
	var err error
	if *attempt == "" {
		err = codex.RunHost(context.Background(), *brain, *id)
	} else {
		var binary string
		binary, err = os.Executable()
		if err == nil {
			var runtime *codex.Runtime
			runtime, err = reconcile.NewFactoryCodexRuntime(*brain, binary)
			if err == nil {
				err = runtime.RunAttemptHost(context.Background(), codex.AttemptRef{SessionID: *id, AttemptID: *attempt}, *claim)
			}
		}
	}
	if err != nil {
		if *attempt != "" {
			err = errors.Join(err, codex.RecordAttemptHostFailure(*brain, codex.AttemptRef{SessionID: *id, AttemptID: *attempt}, err))
			fmt.Fprintf(stderr, "factory codex-host: %s\n", codex.ControlFailureCode(err))
		} else {
			fmt.Fprintf(stderr, "factory codex-host: %v\n", err)
		}
		return 1
	}
	return 0
}
