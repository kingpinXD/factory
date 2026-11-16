package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func nativeWorld(t *testing.T) (*world, helper.Service, helper.Request) {
	t.Helper()
	w := newWorld(t)
	real, err := filepath.EvalSymlinks(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	w.brain = real
	t.Setenv("FACTORY_BRAIN", real)
	w.epic("e1", "planned")
	w.set("e1", "e1-s1", "running", "e1-w1")
	w.item("e1", "e1-s1", "e1-w1", "implementing")
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 1, Key: leaseKey(1)})
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reqEvent := w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindWorktreeReady, Text: tree, TriggerRef: seqRef(reqEvent), Key: repoResultKey(reqEvent)})
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.DefaultProfile(b)
	profile.NativeWorker = "/fixture/native"
	profile.NativeUsage = "/fixture/usage"
	profile.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	profile.CredentialNamespace = "factory-openai"
	profile.IdentityBinding = "fixture-account"
	routes := map[string][]executor.Route{}
	for name, cs := range profile.Routes {
		c := cs[0]
		model := executor.NativeModel
		if c.Provider == "claude" {
			model = executor.ClaudeModel
		}
		routes[name] = []executor.Route{{Provider: c.Provider, Tier: c.Tier, Effort: c.Effort, Model: model}}
	}
	selection := executor.Selection{SchemaVersion: 1, Executor: "z", EpicID: "e1", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"worker": {Path: profile.NativeWorker, SHA256: strings.Repeat("a", 64)}, "usage": {Path: profile.NativeUsage, SHA256: strings.Repeat("b", 64)}}, ContainerImageID: profile.ContainerImage, CreatedAt: t0}
	if err = executor.WriteSelection(w.brain, selection); err != nil {
		t.Fatal(err)
	}
	service := NativeService(w.brain, func(context.Context, helper.Job) error { return nil })
	return w, service, helper.Request{SchemaVersion: 1, RequestKey: "review-1", OwnerID: "e1-s1", WorkID: "e1-w1", SetID: "e1-s1", Lease: 1, Component: "reviewer-code", Prompt: "review it"}
}
func TestNativeAuthorityRejectsStaleAndBroadScope(t *testing.T) {
	w, s, req := nativeWorld(t)
	ctx := context.Background()
	scope, err := s.Authority.Resolve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*helper.Request){func(r *helper.Request) { r.Lease = 2 }, func(r *helper.Request) { r.OwnerID = "e1" }, func(r *helper.Request) { r.WorkID = "" }, func(r *helper.Request) { r.SetID = "" }} {
		r := req
		change(&r)
		if _, err = s.Authority.Resolve(ctx, r); err == nil {
			t.Fatalf("accepted broadened/stale scope: %+v", r)
		}
	}
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	if err = s.Authority.Check(ctx, helper.Job{Request: req, Scope: scope}); err == nil {
		t.Fatal("old owner retained authority")
	}
	fresh, err := s.Authority.Refresh(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.OwnerID != "e1-w1" || fresh.SetID != "" || fresh.Lease != 0 || fresh.Generation == scope.Generation {
		t.Fatalf("refresh did not derive new owner: %+v", fresh)
	}
}
func TestNativeFencePrecedesLeasePublicationAndReopensNewGeneration(t *testing.T) {
	w, s, req := nativeWorld(t)
	ctx := context.Background()
	accepted, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	d := w.deps()
	d.Helpers = &s
	r, err := newRun(ctx, d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	set := r.ents[req.SetID]
	if err = r.fenceHelpers(set, "replace lease"); err != nil {
		t.Fatal(err)
	}
	retry := req
	retry.RequestKey = "old-generation"
	if _, err = s.Submit(ctx, retry); err == nil {
		t.Fatal("old generation admitted after fence")
	}
	set.st.Lease++
	// Reopen before the grant is persisted must not use the in-memory lease.
	if _, err = s.Authority.Resolve(ctx, helper.Request{OwnerID: req.OwnerID, WorkID: req.WorkID, SetID: req.SetID, Lease: 2, Component: req.Component}); err == nil {
		t.Fatal("unpersisted lease became authority")
	}
	if _, err = r.grantLease(set, "new owner generation"); err != nil {
		t.Fatal(err)
	}
	if err = r.reopenHelpers(set); err != nil {
		t.Fatal(err)
	}
	retry.RequestKey = "new-generation"
	retry.Lease = 2
	if _, err = s.Submit(ctx, retry); err != nil {
		t.Fatalf("new generation refused: %v", err)
	}
	old, err := s.Job(accepted.JobID)
	if err != nil || old.State != "cancelled" {
		t.Fatalf("old writer: %+v %v", old, err)
	}
}
func TestNativeReservedOutputCannotBeSpoofedWithoutEnvironment(t *testing.T) {
	w, s, req := nativeWorld(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(w.dir(req.WorkID), "implement.v1.md")
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// A terminal job cannot mask a missing reservation check through the active-job guard.
	if err = s.Cancel(context.Background(), accepted.JobID, "abandon"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FACTORY_HELPER_JOB", "")
	called := false
	_, err = s.WithUnmanagedEvent(context.Background(), req.WorkID, events.KindEnd, req.OutputPath, func() (events.Event, error) { called = true; return events.Event{}, nil })
	if err == nil || !strings.Contains(err.Error(), "publication belongs") || called {
		t.Fatalf("spoof accepted: called=%v err=%v", called, err)
	}
}
func TestNativePinnedSessionUsesIsolatedPromptsAndNoClaudeHelpers(t *testing.T) {
	w, _, _ := nativeWorld(t)
	ctx := context.Background()
	r, err := newRun(ctx, w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	owner := r.ents["e1"]
	spec, err := r.spec(owner, &SessionInputs{Component: "planner", Name: "factory:e1:planner"}, "task")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Model != "claude-opus-5-5" || spec.Agents != "{}" || spec.PromptFile != agents.PromptPathFor(filepath.Join(store.Factory(w.brain), "executors", "e1"), "planner") {
		t.Fatalf("unpinned spec: %+v", spec)
	}
	data, err := os.ReadFile(spec.PromptFile)
	if err != nil || !strings.Contains(string(data), "Top-level Claude transport") {
		t.Fatalf("real isolated prompt missing: %v", err)
	}
	put(t, filepath.Join(w.brain, "factory", "executors", "e1", "selection.json"), `{"schema_version":900}`)
	if _, err = r.spec(owner, &SessionInputs{Component: "planner"}, "task"); err == nil {
		t.Fatal("corrupt selection fell back to Claude")
	}
}
func TestOrphanExecutorIntakeIDsAreNeverReused(t *testing.T) {
	w := newWorld(t)
	put(t, filepath.Join(store.Factory(w.brain), "executors", "e2610098", "selection.json"), "partial")
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.newEpicID()
	if err != nil || id != "e2610099" {
		t.Fatalf("orphan id reused: %s %v", id, err)
	}
}

type dBackend struct {
	check func(context.Context, helper.RunSpec) error
}

func (b dBackend) Run(ctx context.Context, s helper.RunSpec, k helper.Sink) (helper.RunResult, error) {
	for _, ev := range []struct {
		name   string
		fields map[string]string
	}{{helper.EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "fixture"}}, {helper.EventConversationRecorded, map[string]string{"conversation_id": "conversation"}}, {helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": "turn-1"}}, {helper.EventNativeEnded, map[string]string{"turn_id": "turn-1"}}, {helper.EventCheckpointSaved, map[string]string{"conversation_id": "conversation", "turn_id": "turn-1"}}} {
		if err := k.Record(ctx, ev.name, ev.fields); err != nil {
			return helper.RunResult{}, err
		}
	}
	if b.check != nil {
		if err := b.check(ctx, s); err != nil {
			return helper.RunResult{}, err
		}
	}
	return helper.RunResult{Outcome: "completed", Reply: "reviewed", ConversationID: "conversation", TurnID: "turn-1", ObservedModel: executor.NativeModel}, nil
}
func (dBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	return helper.RunResult{}, false, nil
}
func (dBackend) Fence(context.Context, helper.Job) error { return nil }

type dUsage struct{}

func (dUsage) Read(context.Context) (helper.QuotaSnapshot, error) {
	yes := true
	now := time.Now()
	return helper.QuotaSnapshot{Provider: "zed", Known: true, AttributionKnown: true, IdentityBinding: "fixture-account", ExpectedIdentity: "fixture-account", OrdinaryUsageAllowed: &yes, ObservedAt: now, Windows: []helper.QuotaWindow{{Used: 5, ResetsAt: now.Add(time.Hour), Duration: 5 * time.Hour}, {Used: 5, ResetsAt: now.Add(24 * time.Hour), Duration: 7 * 24 * time.Hour}}}, nil
}
func TestNativePublicationContinuesWhileClaudeHeldAndListingFails(t *testing.T) {
	for _, state := range []string{statePaused, ""} {
		t.Run(fmt.Sprintf("account_%q", state), func(t *testing.T) {
			w, s, req := nativeWorld(t)
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			w.acct.state = state
			w.fake.Respond = func(c proc.Cmd) ([]byte, error) { return nil, errors.New("Claude listing unavailable") }
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"zed": dBackend{}}, Quotas: map[string]helper.UsageReader{"zed": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err = helper.Run(ctx, deps); err != nil {
				t.Fatal(err)
			}
			result, err := s.Result(a.JobID, 1)
			if err != nil || result.State != "completed" {
				t.Fatalf("native result blocked by Claude: %+v %v", result, err)
			}
			evs, err := events.Read(store.EventsPath(w.dir(req.OwnerID)))
			if err != nil {
				t.Fatal(err)
			}
			inbox := events.Inbox(evs)
			if len(inbox) != 1 || !strings.Contains(inbox[0].Text, a.JobID) {
				data, _ := json.Marshal(inbox)
				t.Fatalf("durable owner inbox missing: %s", data)
			}
			if err = helper.Run(ctx, deps); err != nil {
				t.Fatal(err)
			}
			if len(events.Inbox(w.log(req.OwnerID))) != 1 {
				t.Fatal("duplicate completion inbox")
			}
		})
	}
}

func intakeNativeProfile(t *testing.T, w *world) string {
	t.Helper()
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(w.brain, "AGENTS.md"), "## Model tiers\n\n| Tier | Claude | Zed |\n| --- | --- | --- |\n| ultra low | haiku | |\n| low | sonnet | |\n| medium | opus | openai-subscribed/gpt-6.1-sol |\n| high | claude-opus-5-5 | |\n")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.DefaultProfile(b)
	profile.NativeWorker = binary
	profile.NativeUsage = binary
	profile.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	profile.CredentialNamespace = "factory-openai"
	profile.IdentityBinding = "fixture-account"
	tools := t.TempDir()
	docker := filepath.Join(tools, "docker")
	put(t, docker, "#!/bin/sh\nprintf '%s\\n' '"+profile.ContainerImage+"'\n")
	if err = os.Chmod(docker, 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tools+string(os.PathListSeparator)+os.Getenv("PATH"))
	data, err := yaml.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(w.brain, "profile.yaml")
	put(t, path, string(data))
	return path
}
func TestNativeIntakePersistsSelectionAndPromptsBeforeIndex(t *testing.T) {
	w := newWorld(t)
	profile := intakeNativeProfile(t, w)
	d := w.deps()
	w.acct.state = statePaused
	preflight := 0
	d.NativePreflight = func(ctx context.Context, s executor.Selection) error {
		preflight++
		ix, err := store.ReadIndex(w.brain)
		if err != nil {
			return err
		}
		if _, ok := ix[s.EpicID]; ok {
			return fmt.Errorf("epic already published at preflight")
		}
		return nil
	}
	id, err := AddWithOptions(context.Background(), d, "offline task", Overrides{}, executor.Options{Executor: "z", ProfilePath: profile})
	if err != nil {
		t.Fatal(err)
	}
	if preflight != 1 {
		t.Fatalf("preflight called %d times", preflight)
	}
	selection, err := executor.ReadSelection(w.brain, id)
	if err != nil || selection.Executor != "z" {
		t.Fatalf("missing durable selection: %+v %v", selection, err)
	}
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ix[id]; !ok {
		t.Fatal("intake was not published")
	}
	if _, err = os.Stat(agents.PromptPathFor(filepath.Join(store.Factory(w.brain), "executors", id), "planner")); err != nil {
		t.Fatal(err)
	}
}
func TestNativePromptFailureLeavesUnpublishedReservedID(t *testing.T) {
	w := newWorld(t)
	profile := intakeNativeProfile(t, w)
	d := w.deps()
	w.acct.state = statePaused
	d.NativePreflight = func(context.Context, executor.Selection) error {
		return os.Remove(filepath.Join(w.brain, "factory", blueprint.RulesFile))
	}
	if id, err := AddWithOptions(context.Background(), d, "offline task", Overrides{}, executor.Options{Executor: "z", ProfilePath: profile}); err == nil || id != "" {
		t.Fatalf("partial intake published: %s %v", id, err)
	}
	ix, err := store.ReadIndex(w.brain)
	if err != nil || len(ix) != 0 {
		t.Fatalf("partial intake indexed: %v %v", ix, err)
	}
	if _, err = executor.ReadSelection(w.brain, "e2610091"); err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(w.brain, "factory", blueprint.RulesFile), "# Factory rules v1\n")
	d.NativePreflight = func(context.Context, executor.Selection) error { return nil }
	id, err := AddWithOptions(context.Background(), d, "offline task", Overrides{}, executor.Options{Executor: "z", ProfilePath: profile})
	if err != nil || id != "e2610092" {
		t.Fatalf("orphan ID reused: %s %v", id, err)
	}
}
func TestNativeIntakeRejectsGrantAndTierBeforePublication(t *testing.T) {
	for _, test := range []string{"grant", "tier"} {
		t.Run(test, func(t *testing.T) {
			w := newWorld(t)
			profile := intakeNativeProfile(t, w)
			d := w.deps()
			d.NativePreflight = func(context.Context, executor.Selection) error { return errors.New("grant unavailable") }
			ov := Overrides{}
			if test == "tier" {
				ov.Tier = map[string]string{"planner": "medium"}
			}
			_, err := AddWithOptions(context.Background(), d, "offline task", ov, executor.Options{Executor: "z", ProfilePath: profile})
			if err == nil || !strings.Contains(err.Error(), map[string]string{"grant": "grant unavailable", "tier": "pinned route"}[test]) {
				t.Fatalf("wrong refusal: %v", err)
			}
			ix, err := store.ReadIndex(w.brain)
			if err != nil || len(ix) != 0 {
				t.Fatalf("rejected intake indexed: %v %v", ix, err)
			}
		})
	}
}

func TestManagedAdmissionUsesStoredGenerationAndDoesNotTakeTickLock(t *testing.T) {
	w, s, req := nativeWorld(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	backend := dBackend{check: func(ctx context.Context, spec helper.RunSpec) error {
		j, err := s.ManagedAdmission(ctx, spec.Job.ID, spec.Job.Generation)
		if err != nil {
			return err
		}
		if j.ID != a.JobID {
			return fmt.Errorf("wrong job")
		}
		if _, err = s.ManagedAdmission(ctx, j.ID, j.Generation+1); err == nil {
			return fmt.Errorf("future generation admitted")
		}
		checked = true
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"zed": backend}, Quotas: map[string]helper.UsageReader{"zed": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	if err = helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("bound managed admission never exercised")
	}
	result, err := s.Result(a.JobID, 1)
	if err != nil || result.State != "completed" {
		t.Fatalf("tick/helper-ledger lock inversion or failed authority check: %+v %v", result, err)
	}
}
func TestEmptyNativeScopeRecoversOnlyAfterPersistedNewAuthority(t *testing.T) {
	for _, via := range []string{"reopen", "restart", "submit"} {
		t.Run(via, func(t *testing.T) {
			w, s, req := nativeWorld(t)
			d := w.deps()
			d.Helpers = &s
			r, err := newRun(context.Background(), d, false)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.loadBlueprint(); err != nil {
				t.Fatal(err)
			}
			r.attachMachines()
			set := r.ents[req.SetID]
			if err = r.fenceHelpers(set, "replace empty scope"); err != nil {
				t.Fatal(err)
			}
			if _, err = s.RecoverScope(context.Background(), req.WorkID); err == nil {
				t.Fatal("unchanged authority reopened")
			}
			set.st.Lease++
			if _, err = s.Submit(context.Background(), req); err == nil {
				t.Fatal("old owner admitted before stored grant")
			}
			if _, err = r.grantLease(set, "replacement"); err != nil {
				t.Fatal(err)
			}
			fresh := req
			fresh.Lease = 2
			switch via {
			case "reopen":
				if err = r.reopenHelpers(set); err != nil {
					t.Fatal(err)
				}
			case "restart":
				// Simulate loss of coordinator memory after grant, before reopen.
				restarted, err := newRun(context.Background(), d, false)
				if err != nil {
					t.Fatal(err)
				}
				if err = restarted.loadBlueprint(); err != nil {
					t.Fatal(err)
				}
				restarted.attachMachines()
				restarted.recoverHelperOwnership()
				if len(restarted.errs) > 0 {
					t.Fatal(restarted.errs)
				}
			case "submit": // Submit performs atomic matching closed-fence recovery.
			}
			if _, err = s.Submit(context.Background(), fresh); err != nil {
				t.Fatalf("new authority refused: %v", err)
			}
			if _, err = s.Submit(context.Background(), req); err == nil {
				t.Fatal("retired owner admitted")
			}
			if _, err = s.RecoverScope(context.Background(), req.WorkID); err != nil {
				t.Fatalf("same reopened generation was not idempotent: %v", err)
			}
		})
	}
}

func TestBabysitterArtifactsRetainHistoricalSetWithoutLeaseAuthority(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	req.OwnerID = req.WorkID
	req.SetID = ""
	req.Lease = 0
	req.Component = "implementor"
	req.OutputPath = filepath.Join(w.dir(req.WorkID), "implement.v1.md")
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Scope.SetID != "" || job.Scope.Lease != 0 || job.Scope.OwnerID != req.WorkID {
		t.Fatalf("historical directory granted a lease: %+v", job.Scope)
	}
	if set, err := (NativeAuthority{w.brain}).ArtifactSet(context.Background(), job.Scope); err != nil || set != "e1-s1" {
		t.Fatalf("wrong artifact hierarchy: %s %v", set, err)
	}
	stale := req
	stale.RequestKey = "old-owner"
	stale.OwnerID = "e1-s1"
	stale.SetID = "e1-s1"
	stale.Lease = 1
	stale.OutputPath = filepath.Join(w.dir(req.WorkID), "implement.v2.md")
	if _, err = s.Submit(context.Background(), stale); err == nil {
		t.Fatal("historical set authorized the retired owner")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend := dBackend{check: func(_ context.Context, spec helper.RunSpec) error {
		return os.WriteFile(spec.OutputPath, []byte("## Status\nDONE\n## TODOs\nnone\n## Judgment calls\nnone\n## Red tests\nnone\n## Files touched\nnone\n"), 0600)
	}}
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"zed": backend}, Quotas: map[string]helper.UsageReader{"zed": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(accepted.JobID, 1)
	if err != nil || result.State != "completed" || result.OutputFile != req.OutputPath {
		t.Fatalf("work-owned artifact was not published: %+v %v", result, err)
	}
}

type observedAccount struct{ called bool }

func (a *observedAccount) Step(context.Context, time.Time, bool) (string, error) {
	a.called = true
	return statePaused, nil
}
func TestNativeRunnerFailureDoesNotSkipClaudeAccountProtection(t *testing.T) {
	w := newWorld(t)
	d := w.deps()
	account := &observedAccount{}
	d.Account = account
	d.StartHelpers = func(context.Context) error { return errors.New("broker unavailable") }
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if !account.called {
		t.Fatal("native runner failure bypassed Claude account protection")
	}
	overall, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if len(overall.Errors) != 1 || !strings.Contains(overall.Errors[0], "broker unavailable") {
		t.Fatalf("broker failure lost: %+v", overall.Errors)
	}
}
