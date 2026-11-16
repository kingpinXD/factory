package reconcile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

// sampleCodexRuntime records manager routing only. It performs no model, CLI or
// containment operations; this fixture is not live executor acceptance.
type sampleCodexRuntime struct {
	brain    string
	now      *time.Time
	sessions []session.Session
	starts   []session.Spec
	posts    []string
}

func (f *sampleCodexRuntime) List(context.Context) (session.Inventory, error) {
	return session.Inventory{Sessions: slices.Clone(f.sessions), Complete: true}, nil
}
func (f *sampleCodexRuntime) Start(_ context.Context, spec session.Spec) error {
	selection, err := executor.ReadSelection(f.brain, spec.EpicID)
	if err != nil {
		return err
	}
	if err := b2WriteLegacyContract(f.brain, spec, selection); err != nil {
		return err
	}
	n := len(f.starts) + 1
	f.starts = append(f.starts, spec)
	f.sessions = append(f.sessions, session.Session{Ref: session.Ref{Executor: "codex", ID: fmt.Sprintf("sample-host-%d", n), ConversationID: fmt.Sprintf("sample-thread-%d", n), PID: 9000 + n, EpicID: spec.EpicID, Generation: spec.Generation, Name: spec.Name}, OwnerID: spec.Task, Cwd: spec.Cwd, Kind: "background", State: "working", StartedAt: f.now.UnixMilli()})
	return nil
}
func (f *sampleCodexRuntime) Post(_ context.Context, ref session.Ref, text string) error {
	f.posts = append(f.posts, ref.ID+":"+text)
	return nil
}
func (f *sampleCodexRuntime) Stop(_ context.Context, ref session.Ref) error {
	for at := range f.sessions {
		if f.sessions[at].ID == ref.ID {
			f.sessions[at].PID, f.sessions[at].State = 0, "stopped"
		}
	}
	return nil
}
func (f *sampleCodexRuntime) Resume(ctx context.Context, ref session.Ref, text string) error {
	return f.Post(ctx, ref, text)
}
func (*sampleCodexRuntime) Compact(context.Context, session.Ref) error { return nil }
func (*sampleCodexRuntime) Context(context.Context, session.Ref, int) (int, time.Time, error) {
	return 0, time.Time{}, nil
}
func (*sampleCodexRuntime) LimitHit(context.Context, session.Ref, time.Time) (time.Time, bool, bool, error) {
	return time.Time{}, false, true, nil
}
func (*sampleCodexRuntime) TurnEndedAt(context.Context, session.Ref) (time.Time, error) {
	return time.Time{}, nil
}
func (*sampleCodexRuntime) Models(context.Context, session.Ref) (map[string][]string, error) {
	return nil, nil
}

type sampleCodexQuota struct{ now *time.Time }

func (q sampleCodexQuota) Read(context.Context) (helper.QuotaSnapshot, error) {
	allowed := true
	return helper.QuotaSnapshot{Provider: "codex", Known: true, AttributionKnown: true, IdentityBinding: "sample-account", ExpectedIdentity: "sample-account", OrdinaryUsageAllowed: &allowed, ObservedAt: *q.now, Windows: []helper.QuotaWindow{{Used: 10, Duration: 5 * time.Hour, ResetsAt: q.now.Add(time.Hour)}, {Used: 10, Duration: 7 * 24 * time.Hour, ResetsAt: q.now.Add(24 * time.Hour)}}}, nil
}

func codexSampleWorld(t *testing.T) (*world, Deps, *sampleCodexRuntime) {
	t.Helper()
	w := newWorld(t)
	brain, err := filepath.EvalSymlinks(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	w.brain = brain
	w.home, err = filepath.EvalSymlinks(w.home)
	if err != nil {
		t.Fatal(err)
	}
	w.registryMerging(true)
	put(t, filepath.Join(w.brain, "AGENTS.md"), "## Model tiers\n\n| Tier | Claude | Codex |\n| --- | --- | --- |\n| medium | opus | gpt-6.1-sol |\n| low | sonnet | gpt-6.1-sol |\n")
	w.epic("e-codex", epicChecking)
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	p := executor.DefaultCodexProfile(b)
	p.CodexBinary, p.CodexExecServer = "/fixture/codex", "/fixture/codex-exec-server"
	p.CodexModelCatalog, p.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	p.CodexVersion, p.IdentityBinding = "0.162.1", "sample-account"
	p.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	p.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	tiers, err := blueprint.ReadTiers(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := executor.Resolve(p, tiers, b.Deny.Models)
	if err != nil {
		t.Fatal(err)
	}
	sel := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "e-codex", Profile: p, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: p.ContainerImage, CreatedAt: w.now}
	if err := executor.WriteSelection(w.brain, sel); err != nil {
		t.Fatal(err)
	}
	if _, _, err := agents.BuildFor(w.brain, filepath.Join(store.Factory(w.brain), "executors", sel.EpicID), b, nil); err != nil {
		t.Fatal(err)
	}
	fake := &sampleCodexRuntime{brain: w.brain, now: &w.now}
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"codex": fake}
	d.Quotas = map[string]helper.UsageReader{"codex": sampleCodexQuota{now: &w.now}}
	service := NativeService(w.brain, func(context.Context, helper.Job) error {
		t.Fatal("sample fixture unexpectedly launched a broker helper")
		return nil
	})
	d.Helpers = &service
	return w, d, fake
}

const codexSamplePlan = `uat: none
issues:
  - {ref: o/r#3, result: ready}
  - {ref: o/r#4, result: ready}
  - {ref: o/r#5, result: ready}
  - {ref: o/r#6, result: ready}
items:
  - {id: parse, repo: r, issue: o/r#3}
  - {id: positive, repo: r, issue: o/r#4}
  - {id: sum, repo: r, issue: o/r#5}
  - {id: compose, repo: r, issue: o/r#6}
sets:
  - {id: parse-set, items: [parse]}
  - {id: positive-set, items: [positive]}
  - {id: sum-set, items: [sum]}
  - {id: compose-set, items: [compose]}
links:
  - {from: parse, to: compose, gate: start, when: merged, rollback: none}
  - {from: positive, to: compose, gate: start, when: merged, rollback: none}
  - {from: sum, to: compose, gate: start, when: merged, rollback: none}
`

// This runs the real manager state machines with simulated model outputs,
// repository-worker responses and GitHub. It does not implement sample code or
// claim a provider-backed sample epic passed.
func TestCodexSampleEpicSimulationPreservesDependenciesAndMergeChecks(t *testing.T) {
	w, d, runtime := codexSampleWorld(t)
	tick := func() {
		t.Helper()
		if err := Tick(context.Background(), d, false); err != nil {
			t.Fatal(err)
		}
		w.noErrors()
		w.now = w.now.Add(time.Minute)
	}
	tick()
	if len(runtime.starts) != 1 || !strings.HasSuffix(runtime.starts[0].Name, ":planner") {
		t.Fatalf("Codex planner was not started: %+v", runtime.starts)
	}
	w.end("e-codex", "state-check.v1.md", stateCheck)
	w.end("e-codex", "epic.v1.yaml", codexSamplePlan)
	tick()
	if got := w.status("e-codex").State; got != epicPlanned {
		t.Fatalf("sample plan not applied: %s\n%s", got, w.out.String())
	}
	tick()
	compose := "e-codex-compose"
	assertCompositionHeld := func() {
		t.Helper()
		if got := w.status(compose).State; got != workQueued {
			t.Fatalf("composition started before every prerequisite merged: %s", got)
		}
		if len(w.kinds(compose, events.KindWorktreeReady)) != 0 || slices.ContainsFunc(runtime.starts, func(spec session.Spec) bool { return spec.Task == "e-codex-compose-set" }) {
			t.Fatal("composition acquired an early worktree or coordinator")
		}
	}
	assertCompositionHeld()
	for at, name := range []string{"parse", "positive", "sum", "compose"} {
		id := "e-codex-" + name
		if name != "compose" {
			assertCompositionHeld()
		} else if got := w.status(id).State; got != stateStarting {
			t.Fatalf("composition did not start after all three green merges: %s", got)
		}
		if got := w.status(id).State; got != stateStarting {
			t.Fatalf("independent item %s is %s", id, got)
		}
		worktree := w.readyWorktree(id)
		if err := os.MkdirAll(worktree, 0o700); err != nil {
			t.Fatal(err)
		}
		tick() // lets the selected Codex orchestrator observe its worktree
		w.append(w.dir(id), events.Event{Kind: events.KindStart})
		for _, output := range []struct{ name, text string }{
			{"explore.v1.md", "## Summary\n## Files\nnumbers.go\n## Baseline tests\n## Issue check\n"},
			{"plan.v1.md", "## Explore summary\n## TODOs\n## Assumptions\n"},
			{"rounds.v1.md", "## Round 1\n## Final Summary\n"},
			{"implement.v1.md", "## Status\n## TODOs\n## Judgment calls\n## Red tests\n## Files touched\n"},
			{"verify.v1.md", "## Verdict\npass\n## Checks\n## Review\n## Send back\n"},
			{"pr.v1.md", "## PR\n## Issue\n"},
		} {
			w.end(id, output.name, output.text)
		}
		number, sha := 30+at, fmt.Sprintf("%040x", 30+at)
		url := fmt.Sprintf("https://github.com/o/r/pull/%d", number)
		w.prs[git.Branch(id)] = gh.PR{ID: fmt.Sprintf("PR_%d", number), Number: number, URL: url, State: "OPEN", HeadRefName: git.Branch(id), HeadRefOid: sha, BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE", MergeStateStatus: "CLEAN", Author: gh.User{Login: "me"}, CreatedAt: w.now}
		w.pulls.checks[sha] = []gh.Check{{Name: "ci", State: "IN_PROGRESS", Required: true}}
		comment := gh.Comment{ID: int64(100 + at), Author: "alice", Body: "Check the sample edge case.", CreatedAt: w.now}
		w.pulls.threads[number] = []gh.Thread{{ID: fmt.Sprintf("sample-thread-%d", number), First: comment, Last: comment}}
		w.fullNext()
		tick()
		w.fullNext()
		tick()
		if got := w.status(id).State; got != workInReview {
			t.Fatalf("item %s did not reach review with pending CI: %s\n%s", id, got, w.out.String())
		}
		if w.prs[git.Branch(id)].State != "OPEN" {
			t.Fatal("pending CI was treated as green")
		}
		w.pulls.threads[number] = []gh.Thread{{ID: fmt.Sprintf("sample-thread-%d", number), IsResolved: true, First: comment, Last: gh.Comment{ID: int64(200 + at), Author: "me", Body: "The sample edge case is verified.", CreatedAt: w.now}}}
		w.pulls.checks[sha] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
		w.now = w.now.Add(16 * time.Minute)
		w.fullNext()
		tick()
		if w.prs[git.Branch(id)].State != "MERGED" {
			t.Fatalf("green exact-head PR %s was not merged\n%s", id, w.out.String())
		}
		w.fullNext()
		tick()
		if got := w.status(id).State; got != stateMerged {
			t.Fatalf("merged GitHub PR not recorded for %s: %s", id, got)
		}
		tick() // release any dependent set on the following manager pass
	}
	for range 3 {
		tick()
	}
	if got := w.status("e-codex").State; got != "done" {
		t.Fatalf("sample epic did not complete: %s\n%s", got, w.out.String())
	}
	for _, component := range []string{"planner", "orchestrator", "babysitter"} {
		if !slices.ContainsFunc(runtime.starts, func(spec session.Spec) bool { return strings.HasSuffix(spec.Name, ":"+component) }) {
			t.Errorf("sample did not route %s through Codex", component)
		}
	}
	for _, spec := range runtime.starts {
		if spec.Model != executor.CodexModel || spec.Effort == "" || spec.Agents != "{}" || spec.SelectionHash == "" || spec.InstructionsHash == "" || spec.Generation == "" {
			t.Errorf("unbound Codex session: %+v", spec)
		}
	}
	if len(w.called("claude")) != 0 || len(w.posts) != 0 || len(w.dms) != 0 {
		t.Fatalf("sample contacted Claude or posted review/notification messages: claude=%v posts=%v dms=%v", w.called("claude"), w.posts, w.dms)
	}
	for _, call := range w.calls() {
		if strings.Contains(call, "zed") || strings.Contains(call, "--add-reviewer") || strings.Contains(call, "requestReviews") {
			t.Fatalf("sample invoked another provider or requested reviews: %s", call)
		}
	}
	if len(w.called("gh pr merge")) != 4 {
		t.Fatalf("sample did not merge each PR once: %v", w.called("gh pr merge"))
	}
}

func TestRemovedExecutorHoldsOnlyItsSavedEpic(t *testing.T) {
	w, d, runtime := codexSampleWorld(t)
	w.epic("old-z", epicChecking)
	put(t, filepath.Join(store.Factory(w.brain), "executors", "old-z", "selection.json"), `{"schema_version":1,"executor":"z","epic_id":"old-z","created_at":"2026-10-09T12:00:00Z"}`)
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(runtime.starts) != 1 || runtime.starts[0].EpicID != "e-codex" {
		t.Fatalf("saved Z blocked Codex startup: %+v", runtime.starts)
	}
	report, err := Status(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, epic := range report.Epics {
		if epic.ID == "old-z" {
			found = true
			if !strings.Contains(epic.ExecutorHold, "unsupported executor") || !strings.Contains(epic.ExecutorHold, "old-z") {
				t.Fatalf("saved Z lacked an explicit executor hold: %+v", epic)
			}
		}
	}
	if !found {
		t.Fatal("saved Z epic disappeared from status")
	}
	for _, spec := range runtime.starts {
		if spec.EpicID == "old-z" {
			t.Fatal("saved Z started through another executor")
		}
	}
}
