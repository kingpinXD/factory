package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/webui"
)

func runUI(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runUIContext(ctx, args, stdout, stderr)
}

func runUIContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	const usage = "usage: factory ui [--listen 127.0.0.1:0] [--brain /absolute/path] [--read-only]"
	fs := flag.NewFlagSet("ui", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "127.0.0.1:0", "loopback address and port")
	brain := fs.String("brain", blueprint.Brain(), "absolute selected brain directory")
	readOnly := fs.Bool("read-only", false, "disable all factory mutations")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	host, _, err := net.SplitHostPort(*listen)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		fmt.Fprintln(stderr, "factory ui: --listen must be a numeric loopback address and port")
		return 2
	}
	if !filepath.IsAbs(*brain) {
		fmt.Fprintln(stderr, "factory ui: --brain must be absolute")
		return 2
	}
	*brain = filepath.Clean(*brain)
	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "factory ui: %v\n", err)
		return 1
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(stderr, "factory ui: %v\n", err)
		return 1
	}
	server, err := webui.New(webui.Config{Brain: *brain, Home: home, Executable: executable, Version: versionLine(readBuildInfo()), ReadOnly: *readOnly, Deps: func(out io.Writer) (reconcile.Deps, error) { return uiDepsForBrain(*brain, out) }})
	if err != nil {
		fmt.Fprintf(stderr, "factory ui: %v\n", err)
		return 1
	}
	defer server.Close()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "factory ui: %v\n", err)
		return 1
	}
	defer listener.Close()
	fmt.Fprintln(stdout, server.BootstrapURL(listener.Addr().String()))
	fmt.Fprintln(stderr, "Factory UI is local. Keep the authenticated URL private. Closing the UI leaves the manager running.")
	if err := server.Serve(ctx, listener); err != nil {
		fmt.Fprintf(stderr, "factory ui: %v\n", err)
		return 1
	}
	return 0
}

// uiRunner binds child processes without changing the long-lived server environment.
type uiRunner struct {
	brain, token string
	runner       proc.Runner
}

func (r uiRunner) Run(ctx context.Context, c proc.Cmd) ([]byte, error) {
	c.Env = append([]string{"FACTORY_TASK=", "FACTORY_BRAIN=" + r.brain, "BRAIN=" + r.brain, "GH_TOKEN=" + r.token}, c.Env...)
	return r.runner.Run(ctx, c)
}

func uiDepsForBrain(brain string, out io.Writer) (reconcile.Deps, error) {
	d, err := depsForBrain(brain, out)
	if err != nil {
		return d, err
	}
	token := os.Getenv("GH_TOKEN")
	if token == "" {
		if data, err := os.ReadFile(filepath.Join(brain, "connectors", "secrets.env")); err == nil {
			token = tokenIn(string(data))
		}
	}
	runner := uiRunner{brain: brain, token: token, runner: proc.Exec{}}
	d.GitHub = gh.Client{Runner: runner}
	d.Claude.Runner = runner
	d.GHToken = token
	d.Account = reconcile.PlanAccount{Deps: d, Probe: reconcile.Probe{Runner: runner, Claude: d.Claude.Bin, Home: d.Claude.Home}}
	return d, nil
}
