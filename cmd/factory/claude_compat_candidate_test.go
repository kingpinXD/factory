package main

import (
	"context"
	"fmt"
	"github.com/kingpinXD/factory/internal/notify"
	"github.com/kingpinXD/factory/internal/proc"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

// Preserve the real constructor before fakeDeps replaces the CLI variable.
var cliCompatProductionNewDeps = newDeps

func init() {
	cliCompatExplicitInstalled = true
	cliCompatCandidateDeps = func(d reconcile.Deps) reconcile.Deps {
		service := reconcile.NativeService(d.Brain, func(context.Context, helper.Job) error {
			return fmt.Errorf("forbidden native runtime fence in legacy CLI lane")
		})
		d.Helpers = &service
		d.NativePreflight = func(context.Context, executor.Selection) error {
			return fmt.Errorf("forbidden native preflight in Claude CLI lane")
		}
		d.StartHelpers = func(context.Context) error {
			jobs, err := (helper.Service{Brain: d.Brain}).Jobs()
			if err != nil {
				return err
			}
			if len(jobs) > 0 {
				return fmt.Errorf("forbidden broker process launch in Claude CLI lane")
			}
			return nil
		}
		return d
	}
}
func TestClaudeCompatCandidateCLISelection(t *testing.T) {
	if os.Getenv("FACTORY_CLAUDE_COMPAT_CAPTURE") != "1" {
		t.Skip("explicit compatibility capture opt-in required")
	}
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			x := cliCompatNew(t)
			step, args := x.intake(false, explicit)
			cliCompatRequire(t, len(step.Evidence.Calls) > 0, "no Claude launch evidence")
			count := 0
			for i, arg := range args {
				if arg == "--executor" {
					count++
					cliCompatRequire(t, i+1 < len(args) && args[i+1] == "claude", "incorrect explicit executor")
				}
			}
			want := 0
			if explicit {
				want = 1
			}
			cliCompatRequire(t, count == want, "wrong explicit flag count")
			jobs, err := (helper.Service{Brain: x.brain}).Jobs()
			if err != nil {
				t.Fatal(err)
			}
			cliCompatRequire(t, len(jobs) == 0, "Claude CLI queued a native backend job")
			// fakeDeps replaces production newDeps. No claim is made about real
			// adapter construction, broker subprocesses or live backend behavior.
		})
	}
}

func TestClaudeCompatCandidateProductionConstructor(t *testing.T) {
	if os.Getenv("FACTORY_CLAUDE_COMPAT_CAPTURE") != "1" {
		t.Skip("explicit compatibility capture opt-in required")
	}
	home, brain, bin := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("FACTORY_BRAIN", brain)
	t.Setenv("PATH", bin)
	t.Setenv("GH_TOKEN", "")
	inert := filepath.Join(bin, "claude")
	// LookPath needs an executable bit, not an executable program. This file
	// is never executed; the cancelled context also prevents broker exec.
	if err := os.WriteFile(inert, []byte("inert LookPath fixture; not executable content\n"), 0700); err != nil {
		t.Fatal(err)
	}
	d, err := cliCompatProductionNewDeps(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cliCompatRequire(t, d.Claude.Bin == inert && d.Claude.Home == home && d.Brain == brain, "constructor did not use owned fixture roots")
	_, ghExec := d.GitHub.Runner.(proc.Exec)
	_, claudeExec := d.Claude.Runner.(proc.Exec)
	_, account := d.Account.(reconcile.PlanAccount)
	_, slack := d.Notify.(notify.Slack)
	cliCompatRequire(t, ghExec && claudeExec && account && slack && d.Helpers != nil && d.StartHelpers != nil && d.NativePreflight != nil, "production adapter wiring missing")
	jobs, err := d.Helpers.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	cliCompatRequire(t, len(jobs) == 0, "constructor ledger not empty")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.StartHelpers(ctx); err != nil {
		t.Fatalf("empty actual StartHelpers: %v", err)
	}
	// Constructor plus empty startup is not full production subprocess proof.
	// Replace both proc.Exec adapters and every live service before any Tick.
	x := cliCompatNew(t)
	fake := &proc.Fake{Respond: x.respond}
	d.Brain = x.brain
	d.Claude.Brain = x.brain
	d.Claude.Home = x.home
	d.Claude.Bin = "/fake/claude"
	d.Claude.SocketDir = filepath.Join(x.home, "sockets")
	d.Claude.FactoryBin = "/nonexistent/factory"
	d.GitHub.Runner = fake
	d.Claude.Runner = fake
	d.Account = reconcile.NoAccount{}
	d.Notify = nil
	d.Post = nil
	d.GHToken = ""
	d.Now = func() time.Time { return x.now }
	d.Out = io.Discard
	d = cliCompatCandidateDeps(d)
	if err := reconcile.Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	overall, err := store.ReadOverall(x.brain)
	if err != nil {
		t.Fatal(err)
	}
	cliCompatRequire(t, len(overall.Errors) == 0, "isolated empty Tick recorded errors: %v", overall.Errors)
	cliCompatRequire(t, len(fake.Calls()) == 0, "empty Tick unexpectedly called an external adapter")
}
