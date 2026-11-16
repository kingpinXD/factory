package zednative

import (
	"context"
	"github.com/kingpinXD/factory/internal/executor"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type testAuthority struct{ scope helper.Scope }

func (a testAuthority) Resolve(context.Context, helper.Request) (helper.Scope, error) {
	return a.scope, nil
}
func (a testAuthority) Check(context.Context, helper.Job) error { return nil }
func (a testAuthority) Refresh(context.Context, helper.Scope) (helper.Scope, error) {
	return a.scope, nil
}
func TestRunnerUsesPublicBackendAndUsageContracts(t *testing.T) {
	t.Setenv("GH_TOKEN", "synthetic-secret-never-log")
	b, spec, e := fixture(t)
	blueprintFixture := blueprint.Blueprint{SchemaVersion: 1, Values: blueprint.Values{Usage: blueprint.Usage{NearLimit: 80, Pause: 88, StaleAfter: blueprint.Duration(15 * time.Minute)}}, Components: map[string]blueprint.Component{
		"planner": {RunsAs: blueprint.RunsAsSession}, "orchestrator": {RunsAs: blueprint.RunsAsSession}, "babysitter": {RunsAs: blueprint.RunsAsSession}, "reviewer": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "reply"}}},
	}}
	data, err := yaml.Marshal(blueprintFixture)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(b.brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	ix := store.Index{}
	for id, kind := range map[string]string{"e1": "epic", "owner": "session", "work1": "work"} {
		dir := filepath.Join(b.brain, "factory", id)
		if err = privateDir(dir); err != nil {
			t.Fatal(err)
		}
		ix[id] = store.Entry{Kind: kind, Dir: dir, Epic: "e1"}
	}
	if err = store.WriteIndex(b.brain, ix); err != nil {
		t.Fatal(err)
	}
	service := helper.Service{Brain: b.brain, Authority: testAuthority{spec.Job.Scope}}
	accepted, err := service.Submit(context.Background(), spec.Job.Request)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise New's real stored-job reader, not the injected fixture reader.
	backend := New(b.brain)
	backend.engine = e
	backend.docker = fakeDocker
	service.FenceRuntime = backend.Fence
	selection, err := b.validate(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deps := helper.RunnerDeps{Service: service, Backends: map[string]helper.Backend{"zed": backend}, Quotas: map[string]helper.UsageReader{"zed": NewUsageReader(selection.Binaries["usage"], "factory-openai", "fixture-identity")}, FactoryBin: spec.FactoryBin, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	if err = helper.Run(ctx, deps); err != nil {
		job, _ := service.Job(accepted.JobID)
		r, _ := service.Result(accepted.JobID, 1)
		t.Fatalf("runner: %v state=%s hold=%s result=%s/%s", err, job.State, job.HoldReason, r.ErrorCode, r.ErrorText)
	}
	result, err := service.Result(accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "completed" || result.Outcome != "completed" || result.ConversationID != "native-session" || result.TurnID != "test-epoch:1" {
		t.Fatalf("public contract failed: %+v", result)
	}
	// Cleanup paths now refer to the broker-generated UUID rather than fixture j1.
	job, err := service.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := backend.directory(job)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(filepath.Dir(socketPath(dir))) })
}

type sinkFunc func(context.Context, string, map[string]string) error

func (f sinkFunc) Record(ctx context.Context, event string, fields map[string]string) error {
	return f(ctx, event, fields)
}

type continuationBackend struct {
	*Backend
	service helper.Service
	parent  string
	yield   bool
	yielded bool
	specs   []helper.RunSpec
}

func (b *continuationBackend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	if spec.Job.ID == b.parent {
		b.specs = append(b.specs, spec)
	}
	wrapped := sinkFunc(func(ctx context.Context, event string, fields map[string]string) error {
		if err := sink.Record(ctx, event, fields); err != nil {
			return err
		}
		if !b.yield || b.yielded || spec.Job.ID != b.parent || spec.Job.Round != 1 || event != helper.EventTurnIdentified {
			return nil
		}
		b.yielded = true
		child := spec.Job.Request
		child.RequestKey = "nested-child"
		child.ParentJobID = b.parent
		child.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "report.v9.md")
		child.Prompt = "WRITE_REPORT child"
		accepted, err := b.service.Submit(ctx, child)
		if err != nil {
			return err
		}
		return b.service.Yield(ctx, b.parent, []string{accepted.JobID})
	})
	return b.Backend.Run(ctx, spec, wrapped)
}
func TestPublicRunnerFileContinuationsKeepConversationAndReservation(t *testing.T) {
	for _, mode := range []string{"manual-fresh", "yield-retained"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GH_TOKEN", "synthetic-secret-never-log")
			initial, spec, e := fixture(t)
			blueprintFixture := blueprint.Blueprint{SchemaVersion: 1, Values: blueprint.Values{Usage: blueprint.Usage{NearLimit: 80, Pause: 88, StaleAfter: blueprint.Duration(15 * time.Minute)}}, Components: map[string]blueprint.Component{
				"planner": {RunsAs: blueprint.RunsAsSession}, "orchestrator": {RunsAs: blueprint.RunsAsSession}, "babysitter": {RunsAs: blueprint.RunsAsSession}, "reviewer": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "report.v<n>.md", Headings: []string{"## Result"}}}},
			}}
			data, err := yaml.Marshal(blueprintFixture)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(blueprint.Path(initial.brain), data, 0600); err != nil {
				t.Fatal(err)
			}
			ix := store.Index{}
			for id, kind := range map[string]string{"e1": "epic", "owner": "session", "work1": "work"} {
				dir := filepath.Join(initial.brain, "factory", id)
				if err = privateDir(dir); err != nil {
					t.Fatal(err)
				}
				ix[id] = store.Entry{Kind: kind, Dir: dir, Epic: "e1"}
			}
			if err = store.WriteIndex(initial.brain, ix); err != nil {
				t.Fatal(err)
			}
			service := helper.Service{Brain: initial.brain, Authority: testAuthority{spec.Job.Scope}}
			request := spec.Job.Request
			request.Prompt = "WRITE_REPORT initial"
			request.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "report.v1.md")
			accepted, err := service.Submit(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			native := New(initial.brain)
			native.engine = e
			native.docker = fakeDocker
			tracked := &continuationBackend{Backend: native, service: service, parent: accepted.JobID, yield: mode == "yield-retained"}
			service.FenceRuntime = tracked.Fence
			var selection executor.Selection
			if err = readPrivate(filepath.Join(spec.RunDir, "selection.json"), &selection); err != nil {
				t.Fatal(err)
			}
			deps := helper.RunnerDeps{Service: service, Backends: map[string]helper.Backend{"zed": tracked}, Quotas: map[string]helper.UsageReader{"zed": NewUsageReader(selection.Binaries["usage"], "factory-openai", "fixture-identity")}, FactoryBin: spec.FactoryBin, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
			ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
			defer cancel()
			if err = helper.Run(ctx, deps); err != nil {
				job, _ := service.Job(accepted.JobID)
				t.Fatalf("runner: %v state=%s hold=%s", err, job.State, job.HoldReason)
			}
			expectedOutput := request.OutputPath
			if mode == "manual-fresh" {
				first, err := service.Result(accepted.JobID, 1)
				if err != nil || first.OutputFile != request.OutputPath || first.State != "completed" {
					t.Fatalf("first publication: %+v %v", first, err)
				}
				expectedOutput = filepath.Join(spec.Job.Scope.OutputRoot, "report.v2.md")
				if _, err = service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "fresh-round", ExpectedRound: 1, Prompt: "WRITE_REPORT continued", OutputPath: expectedOutput}); err != nil {
					t.Fatal(err)
				}
				if err = helper.Run(ctx, deps); err != nil {
					t.Fatal(err)
				}
				firstBytes, err := os.ReadFile(request.OutputPath)
				if err != nil || string(firstBytes) != "## Result\nround 1\n" {
					t.Fatal("fresh continuation overwrote the first report")
				}
			} else {
				first, err := service.Result(accepted.JobID, 1)
				if err != nil || first.OutputFile != "" {
					t.Fatalf("yielded output was published: %+v %v", first, err)
				}
			}
			final, err := service.Result(accepted.JobID, 2)
			if err != nil || final.State != "completed" || final.OutputFile != expectedOutput {
				t.Fatalf("continued publication: %+v %v", final, err)
			}
			if len(tracked.specs) != 2 {
				t.Fatalf("parent executed %d rounds", len(tracked.specs))
			}
			if tracked.specs[0].OutputPath != request.OutputPath || tracked.specs[1].OutputPath != expectedOutput || tracked.specs[1].Job.Request.OutputPath != request.OutputPath {
				t.Fatal("current reservation or immutable request changed")
			}
			if tracked.specs[1].Job.ConversationID != "native-session" || final.ConversationID != "native-session" {
				t.Fatal("file continuation replaced conversation")
			}
			finalBytes, err := os.ReadFile(expectedOutput)
			if err != nil || !strings.Contains(string(finalBytes), "round 2") {
				t.Fatal("continuation did not write its assigned output")
			}
			jobs, err := service.Jobs()
			if err != nil {
				t.Fatal(err)
			}
			for _, job := range jobs {
				for round := 1; round <= job.Round; round++ {
					dir := filepath.Join(initial.brain, "factory", "helpers", "runs", job.ID, strconv.Itoa(round))
					t.Cleanup(func() { os.RemoveAll(filepath.Dir(socketPath(dir))) })
				}
			}
		})
	}
}
