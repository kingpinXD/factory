package zednative

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func TestPrelaunchMissingGitHubAuthenticationFencesAfterRestart(t *testing.T) {
	b, spec, e := fixture(t)
	spec.Env = nil
	b.docker = func(context.Context) (executor.Binary, string, error) {
		t.Fatal("Docker resolution reached after missing-auth rejection")
		return executor.Binary{}, "", nil
	}
	ctx, cancel := bounded(t)
	defer cancel()
	sink := &recordSink{}
	_, runErr := b.Run(ctx, spec, sink)
	if runErr == nil || !strings.Contains(runErr.Error(), "explicit container GitHub authentication required") {
		t.Fatalf("wrong rejection: %v", runErr)
	}
	if len(sink.events) != 0 {
		t.Fatalf("prelaunch rejection recorded native events: %v", sink.events)
	}
	for _, name := range []string{"identity.json", "native.lock", "result.json"} {
		if _, err := os.Lstat(filepath.Join(spec.RunDir, name)); !os.IsNotExist(err) {
			t.Fatalf("unexpected writer artifact %s: %v", name, err)
		}
	}
	if c, err := e.read(); err != nil || c != nil {
		t.Fatalf("container created during prelaunch rejection: %+v %v", c, err)
	}
	// A new backend has no in-memory knowledge of the returned Run call.
	restarted := New(b.brain)
	restarted.engine = e
	restarted.job = b.job
	restarted.docker = b.docker
	if err := restarted.Fence(ctx, spec.Job); err != nil {
		t.Fatalf("confirmed prelaunch rejection cannot be fenced after restart: %v; original Run error: %v", err, runErr)
	}
	var closed attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &closed); err != nil || !closed.Closed || !closed.HostStopped || !closed.ContainerRemoved {
		t.Fatalf("prelaunch closure not durable: %+v %v", closed, err)
	}
	restarted = New(b.brain)
	restarted.engine, restarted.job, restarted.docker = e, b.job, b.docker
	if err := restarted.Fence(ctx, spec.Job); err != nil {
		t.Fatalf("repeated prelaunch fence after closed-state reconstruction: %v", err)
	}
	result, identified, err := restarted.Inspect(ctx, spec, sink)
	if err != nil || !identified || result.Outcome != "failed" || result.ErrorCode != "native_prelaunch_failed" || result.ErrorText != "explicit container GitHub authentication required" || result.ConversationID != "" || result.TurnID != "" || result.ObservedModel != "" || len(sink.events) != 0 {
		t.Fatalf("wrong prelaunch inspection: %+v %v %v events=%v", result, identified, err, sink.events)
	}
}

func TestPrelaunchFenceStillRequiresPositiveEvidence(t *testing.T) {
	for _, prior := range []string{"missing", "unconfirmed-prelaunch", "container-intent", "native-intent", "zero-schema", "corrupt"} {
		t.Run(prior, func(t *testing.T) {
			b, spec, _ := fixture(t)
			file := filepath.Join(spec.RunDir, "adapter.json")
			if prior == "corrupt" || prior == "zero-schema" {
				data := "{"
				if prior == "zero-schema" {
					data = "{}"
				}
				if err := os.WriteFile(file, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			} else if prior != "missing" {
				var selection executor.Selection
				if err := readPrivate(filepath.Join(spec.RunDir, "selection.json"), &selection); err != nil {
					t.Fatal(err)
				}
				a := attempt{SchemaVersion: 1, Job: spec.Job, OutputPath: spec.OutputPath, Selection: selection, Token: "prior-attempt", StartedAtMS: time.Now().UnixMilli()}
				if prior == "container-intent" {
					a.ContainerPhase = "create_intent"
				} else if prior == "native-intent" {
					a.LaunchIntent = true
				}
				if err := saveAttempt(spec.RunDir, &a); err != nil {
					t.Fatal(err)
				}
			}
			before, readErr := os.ReadFile(file)
			ctx, cancel := bounded(t)
			defer cancel()
			if prior != "missing" {
				// A new validation error must not erase an earlier ambiguous launch.
				spec.Env = nil
				if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
					t.Fatal("prior attempt replayed")
				}
				after, err := os.ReadFile(file)
				if err != nil || string(after) != string(before) {
					t.Fatal("prelaunch failure replaced prior evidence")
				}
			} else if !os.IsNotExist(readErr) {
				t.Fatalf("expected missing attempt: %v", readErr)
			}
			if err := b.Fence(ctx, spec.Job); err == nil {
				t.Fatal("uncertain writer released without positive prelaunch evidence")
			}
		})
	}
}

func TestPublicRunnerPrelaunchFailureSettlesWithDiagnostic(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	b, spec, e := fixture(t)
	bp := blueprint.Blueprint{SchemaVersion: 1, Values: blueprint.Values{Usage: blueprint.Usage{NearLimit: 80, Pause: 88, StaleAfter: blueprint.Duration(15 * time.Minute)}}, Components: map[string]blueprint.Component{
		"planner": {RunsAs: blueprint.RunsAsSession}, "orchestrator": {RunsAs: blueprint.RunsAsSession}, "babysitter": {RunsAs: blueprint.RunsAsSession}, "reviewer": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "reply"}}},
	}}
	data, err := yaml.Marshal(bp)
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
	native := New(b.brain)
	native.engine = e
	native.docker = func(context.Context) (executor.Binary, string, error) {
		return executor.Binary{}, "", errors.New("unexpected Docker resolution")
	}
	service.FenceRuntime = native.Fence
	var selection executor.Selection
	if err = readPrivate(filepath.Join(spec.RunDir, "selection.json"), &selection); err != nil {
		t.Fatal(err)
	}
	deps := helper.RunnerDeps{Service: service, Backends: map[string]helper.Backend{"zed": native}, Quotas: map[string]helper.UsageReader{"zed": NewUsageReader(selection.Binaries["usage"], "factory-openai", "fixture-identity")}, FactoryBin: spec.FactoryBin, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runErr := helper.Run(ctx, deps)
	job, jobErr := service.Job(accepted.JobID)
	result, resultErr := service.Result(accepted.JobID, 1)
	if jobErr != nil || resultErr != nil {
		t.Fatalf("stored job/result: %v / %v", jobErr, resultErr)
	}
	if runErr != nil || job.State != "failed" || result.State != "failed" || result.Outcome != "failed" || !strings.Contains(result.ErrorText, "explicit container GitHub authentication required") {
		t.Fatalf("prelaunch rejection did not settle with its cause: runner=%v state=%s hold=%s result=%s/%s error=%s/%s", runErr, job.State, job.HoldReason, result.State, result.Outcome, result.ErrorCode, result.ErrorText)
	}
	if job.ConversationID != "" || job.TurnID != "" || result.ObservedModel != "" {
		t.Fatal("prelaunch failure invented native execution evidence")
	}
}

func TestPrelaunchRejectionDurableAndSanitized(t *testing.T) {
	for _, kind := range []string{"github", "npm", "repository", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			b, spec, e := fixture(t)
			want := "explicit container GitHub authentication required"
			spec.Env = []string{"NPM_TOKEN=private-prelaunch-secret"}
			switch kind {
			case "npm":
				npmFixture(t, spec.Cwd)
				putManifest(t, spec.Cwd, ".npmrc", "//registry.example.invalid/:_authToken="+"$"+"{NPM_TOKEN}\n")
				spec.Env = []string{"GH_TOKEN=private-prelaunch-secret"}
				want = "explicit container NPM_TOKEN required"
			case "repository":
				putManifest(t, spec.Cwd, "go.mod", "module fixture\n")
				want = "repository requirements rejected"
			case "duplicate":
				spec.Env = []string{"GH_TOKEN=private-prelaunch-secret", "GH_TOKEN=second-secret"}
				want = "duplicate environment input"
			}
			ctx, cancel := bounded(t)
			defer cancel()
			if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
				t.Fatal("preflight accepted")
			}
			var a attempt
			if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
				t.Fatal(err)
			}
			if a.PrelaunchError != want || a.ContainerPhase != "" || a.LaunchIntent || a.Git.Phase != "" {
				t.Fatalf("wrong durable rejection: %+v", a)
			}
			data, err := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
			if err != nil || strings.Contains(string(data), "private-prelaunch-secret") || strings.Contains(string(data), "second-secret") {
				t.Fatal("prelaunch evidence leaked environment values")
			}
			if c, err := e.read(); err != nil || c != nil {
				t.Fatal("preflight created a container")
			}
		})
	}
}

func TestPrelaunchRejectionSaveFailureRemainsUnknown(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Env = nil
	commits := 0
	b.checkpoint = func(string) {
		commits++
		if commits == 1 {
			file := filepath.Join(spec.RunDir, "adapter.json")
			if err := os.Remove(file); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(file, 0700); err != nil {
				t.Fatal(err)
			}
		}
	}
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err == nil || !strings.Contains(err.Error(), "rename") {
		t.Fatalf("rejection write failure not propagated: %v", err)
	}
	if err := b.Fence(ctx, spec.Job); err == nil {
		t.Fatal("failed rejection persistence released ownership")
	}
}

func TestPrelaunchFenceValidatesImmutableBinding(t *testing.T) {
	for _, closed := range []bool{false, true} {
		name := "open"
		if closed {
			name = "closed"
		}
		t.Run(name, func(t *testing.T) {
			for _, field := range []string{"attempt-request", "supplied-request", "stored-request", "attempt-route", "supplied-route", "stored-route", "attempt-selection", "frozen-selection", "current-selection", "component-route"} {
				t.Run(field, func(t *testing.T) {
					b, spec, _ := fixture(t)
					spec.Env = nil
					ctx, cancel := bounded(t)
					defer cancel()
					if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
						t.Fatal("missing auth accepted")
					}
					if closed {
						if err := b.Fence(ctx, spec.Job); err != nil {
							t.Fatal(err)
						}
					}
					var a attempt
					if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
						t.Fatal(err)
					}
					supplied, stored := spec.Job, spec.Job
					switch field {
					case "attempt-request":
						a.Job.Request.Prompt = "changed"
					case "supplied-request":
						supplied.Request.Prompt = "changed"
					case "stored-request":
						stored.Request.Prompt = "changed"
					case "attempt-route":
						a.Job.Route.Effort = "changed"
					case "supplied-route":
						supplied.Route.Effort = "changed"
					case "stored-route":
						stored.Route.Effort = "changed"
					case "attempt-selection":
						a.Selection.CreatedAt = a.Selection.CreatedAt.Add(time.Second)
					case "frozen-selection", "current-selection":
						file := filepath.Join(spec.RunDir, "selection.json")
						if field == "current-selection" {
							file = filepath.Join(b.brain, "factory", "executors", spec.Job.Scope.EpicID, "selection.json")
						}
						var selection executor.Selection
						if err := readPrivate(file, &selection); err != nil {
							t.Fatal(err)
						}
						selection.CreatedAt = selection.CreatedAt.Add(time.Second)
						if err := durable(file, selection); err != nil {
							t.Fatal(err)
						}
					case "component-route":
						a.Job.Request.Component, supplied.Request.Component, stored.Request.Component = "missing", "missing", "missing"
					}
					if err := saveAttempt(spec.RunDir, &a); err != nil {
						t.Fatal(err)
					}
					b.job = func(string) (helper.Job, error) { return stored, nil }
					if err := b.Fence(ctx, supplied); err == nil {
						t.Fatal("Fence released mismatched binding without Inspect")
					}
				})
			}
		})
	}
}

func TestPrelaunchFenceRejectsContradictoryEvidence(t *testing.T) {
	for _, field := range []string{"container", "container-phase", "container-id", "launch", "pid", "identity", "turn-intent", "turn", "conversation", "git", "shutdown", "artifact", "unrecognized-error"} {
		t.Run(field, func(t *testing.T) {
			b, spec, _ := fixture(t)
			spec.Env = nil
			ctx, cancel := bounded(t)
			defer cancel()
			if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
				t.Fatal("missing auth accepted")
			}
			var a attempt
			file := filepath.Join(spec.RunDir, "adapter.json")
			if err := readPrivate(file, &a); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "container":
				a.Container.Daemon = "prior-daemon"
			case "container-phase":
				a.ContainerPhase = "create_intent"
			case "container-id":
				a.ContainerID = "prior-container"
			case "launch":
				a.LaunchIntent = true
			case "pid":
				a.PID = 1
			case "identity":
				a.Identity = &identity{}
			case "turn-intent":
				a.TurnIntent = true
			case "turn":
				a.Turn = "prior:1"
			case "conversation":
				a.Conversation = "prior"
			case "git":
				a.Git.Phase = "prepared"
			case "shutdown":
				a.ShutdownAck = true
			case "artifact":
				if err := durable(filepath.Join(spec.RunDir, "identity.json"), identity{}); err != nil {
					t.Fatal(err)
				}
			case "unrecognized-error":
				a.PrelaunchError = "private-prelaunch-secret"
			}
			if err := saveAttempt(spec.RunDir, &a); err != nil {
				t.Fatal(err)
			}
			if err := b.Fence(ctx, spec.Job); err == nil {
				t.Fatal("contradictory rejection released ownership")
			}
		})
	}
}
