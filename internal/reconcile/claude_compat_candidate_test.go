package reconcile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func init() {
	compatExplicitInstalled = true
	compatAdd = func(ctx context.Context, d Deps, input string, ov Overrides) (string, error) {
		if os.Getenv("FACTORY_CLAUDE_COMPAT_LANE") == "candidate-explicit" {
			return AddWithOptions(ctx, d, input, ov, executor.Options{Executor: "claude"})
		}
		return Add(ctx, d, input, ov)
	}
	compatCandidateDeps = func(d Deps) Deps {
		service := NativeService(d.Brain, func(context.Context, helper.Job) error {
			return fmt.Errorf("forbidden native runtime fence in legacy lane")
		})
		d.Helpers = &service
		d.NativePreflight = func(context.Context, executor.Selection) error {
			return fmt.Errorf("forbidden native preflight in Claude lane")
		}
		// Tick calls this callback even for Claude. Guard the process-launch
		// condition, not entry to the callback. Never invoke StartRunner here.
		d.StartHelpers = func(context.Context) error {
			jobs, err := (helper.Service{Brain: d.Brain}).Jobs()
			if err != nil {
				return err
			}
			if len(jobs) != 0 {
				return fmt.Errorf("forbidden broker process launch: Claude lane created %d native jobs", len(jobs))
			}
			return nil
		}
		return d
	}
}

type compatForbiddenBackend struct{ t *testing.T }

func (b compatForbiddenBackend) Run(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, error) {
	b.t.Error("native backend Run in Claude lane")
	return helper.RunResult{}, fmt.Errorf("forbidden native backend")
}
func (b compatForbiddenBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	b.t.Error("native backend Inspect in Claude lane")
	return helper.RunResult{}, false, fmt.Errorf("forbidden native backend")
}
func (b compatForbiddenBackend) Fence(context.Context, helper.Job) error {
	b.t.Error("native backend Fence in Claude lane")
	return fmt.Errorf("forbidden native backend")
}

func TestClaudeCompatCandidateIsolation(t *testing.T) {
	if os.Getenv("FACTORY_CLAUDE_COMPAT_CAPTURE") != "1" {
		t.Skip("explicit compatibility capture opt-in required")
	}
	// This exercises injected deps, not production newDeps or real processes.
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			c := &compatCapture{t: t, trace: compatTrace{Version: 1, Package: "reconcile"}, roots: map[string]string{}}
			x := c.newWorld("candidate_isolation")
			d := compatCandidateDeps(x.deps())
			preflights := 0
			d.NativePreflight = func(context.Context, executor.Selection) error {
				preflights++
				return fmt.Errorf("forbidden native preflight")
			}
			var id string
			var err error
			if explicit {
				id, err = AddWithOptions(context.Background(), d, "add compatibility flag", Overrides{}, executor.Options{Executor: "claude"})
			} else {
				id, err = Add(context.Background(), d, "add compatibility flag", Overrides{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = Tick(context.Background(), d, false); err != nil {
				t.Fatal(err)
			}
			x.check("no native preflight", preflights == 0)
			service := helper.Service{Brain: x.brain, Authority: NativeAuthority{Brain: x.brain}}
			jobs, err := service.Jobs()
			if err != nil {
				t.Fatal(err)
			}
			x.check("broker process would not launch without native jobs", len(jobs) == 0)
			// Drive the actual in-process broker with an empty ledger and sentinels
			// for every native backend method. This starts no broker subprocess.
			backend := compatForbiddenBackend{t}
			err = helper.Run(context.Background(), helper.RunnerDeps{Service: service, FactoryBin: "/nonexistent/factory", Now: func() time.Time { return x.now }, Backends: map[string]helper.Backend{"zed": backend, "claude": backend}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = os.Stat(filepath.Join(x.brain, "factory/executors", id, "selection.json"))
			x.check("Claude intake persists no native selection", os.IsNotExist(err))
			x.check("Claude launch remains present", len(x.called("claude --bg --model")) == 1)
		})
	}
}
