package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func coordinatorFixture(t *testing.T) (string, session.Spec, store.Entry) {
	t.Helper()
	brain := testBrain(t)
	real, err := filepath.EvalSymlinks(brain)
	if err != nil {
		t.Fatal(err)
	}
	brain = real
	t.Setenv("FACTORY_BRAIN", brain)
	entry := store.Entry{Kind: blueprint.MachineEpic, Epic: "epic1", Dir: filepath.Join(brain, "factory", "work", "epic1")}
	if err = os.MkdirAll(entry.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteInputs(entry.Dir, &reconcile.EpicInputs{ID: "epic1", Kind: "text", RepoHint: "o/r"}); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteIndex(brain, store.Index{"epic1": entry}); err != nil {
		t.Fatal(err)
	}
	return brain, session.Spec{Name: "factory:epic1:planner", EpicID: "epic1", Task: "epic1", Generation: "epic1:0:planner", SelectionHash: strings.Repeat("a", 64)}, entry
}
func coordinatorRequest(op string, payload any) controlRequest {
	data, _ := json.Marshal(payload)
	return controlRequest{Op: op, Payload: data}
}
func TestCodexCoordinatorControlWritesOnlyOwnOutputs(t *testing.T) {
	brain, spec, entry := coordinatorFixture(t)
	ctx := context.Background()
	for _, name := range []string{"../selection.json", "events.jsonl", "status.json", "inputs.yaml", "unknown.md", "/tmp/outside.md"} {
		_, err := scopedCoordinatorControl(ctx, brain, spec, coordinatorRequest("file_write", coordinatorPayload{Path: name, Text: "forged"}))
		if err == nil {
			t.Fatalf("unsafe file accepted: %s", name)
		}
	}
	_, err := scopedCoordinatorControl(ctx, brain, spec, coordinatorRequest("file_write", coordinatorPayload{Path: "state-check.v1.md", Text: "## Input\nhello\n## Issues\nnone\n"}))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(entry.Dir, "state-check.v1.md"))
	if err != nil || !strings.Contains(string(got), "hello") {
		t.Fatalf("output=%q %v", got, err)
	}
	_, err = scopedCoordinatorControl(ctx, brain, spec, coordinatorRequest("event", coordinatorPayload{Kind: "end", Path: "state-check.v1.md"}))
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(brain, "outside")
	if err = os.WriteFile(outside, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(entry.Dir, "state-check.v2.md")); err != nil {
		t.Fatal(err)
	}
	_, err = scopedCoordinatorControl(ctx, brain, spec, coordinatorRequest("file_write", coordinatorPayload{Path: "state-check.v2.md", Text: "replace"}))
	if err == nil {
		t.Fatal("symlink output accepted")
	}
	got, _ = os.ReadFile(outside)
	if string(got) != "preserve" {
		t.Fatal("outside output changed")
	}
}
func TestCodexCoordinatorControlCannotReachOtherEpicOrProgramEvents(t *testing.T) {
	brain, spec, entry := coordinatorFixture(t)
	ix, _ := store.ReadIndex(brain)
	ix["foreign"] = store.Entry{Kind: "work", Epic: "other", Dir: entry.Dir}
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	for _, req := range []controlRequest{
		coordinatorRequest("file_write", coordinatorPayload{Target: "foreign", Path: "state-check.v1.md", Text: "forged"}),
		coordinatorRequest("event", coordinatorPayload{Kind: "transition", Text: "forged"}),
		coordinatorRequest("event", coordinatorPayload{Kind: "handover", Text: "forged"}),
		coordinatorRequest("slack", map[string]string{"text": "review"}),
		coordinatorRequest("issue_create", map[string]string{"repo": "foreign/repo", "key": "x", "title": "forged", "body": "forged"}),
	} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err == nil {
			t.Fatalf("unscoped control accepted: %s", req.Op)
		}
	}
}
func TestCodexCoordinatorControlRejectsStaleSetLease(t *testing.T) {
	brain, spec, epic := coordinatorFixture(t)
	set := store.Entry{Kind: "set", Epic: spec.EpicID, Dir: filepath.Join(epic.Dir, "sets", "s1")}
	work := store.Entry{Kind: "work", Epic: spec.EpicID, Dir: filepath.Join(set.Dir, "w1")}
	for _, entry := range []store.Entry{set, work} {
		if err := os.MkdirAll(entry.Dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.WriteInputs(work.Dir, &reconcile.WorkInputs{ID: "w1", Epic: spec.EpicID, Set: "s1", Repo: "o/r"}); err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(brain)
	ix["s1"] = set
	ix["w1"] = work
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	spec.Task = "s1"
	spec.Name = "factory:s1:orchestrator"
	spec.Generation = "s1:99:orchestrator"
	if _, err := coordinatorTarget(ix, spec, "w1"); err == nil || !strings.Contains(err.Error(), "lease changed") {
		t.Fatalf("stale lease accepted: %v", err)
	}
}

func TestCodexTextAndPlanPlannerChooseRegisteredReposBeforeIssueWrite(t *testing.T) {
	for _, kind := range []string{"text", "plan"} {
		t.Run(kind, func(t *testing.T) {
			brain, spec, entry := coordinatorFixture(t)
			if err := store.WriteInputs(entry.Dir, &reconcile.EpicInputs{ID: "epic1", Kind: kind, Input: "build the sample"}); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"one", "two"} {
				if err := os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
					t.Fatal(err)
				}
				text := "**Repo:** `o/" + name + "`\n**Path:** `/fixture/" + name + "`\n**Default branch:** `main`\n"
				if err := os.WriteFile(filepath.Join(brain, "repos", name+".md"), []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			old := coordinatorGitHub
			t.Cleanup(func() { coordinatorGitHub = old })
			writes := 0
			fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
				if c.Name != "gh" {
					t.Fatalf("unexpected process: %+v", c)
				}
				if len(c.Args) > 3 && c.Args[1] == "-X" && c.Args[2] == "POST" {
					files, err := os.ReadDir(filepath.Join(store.Factory(brain), "codex", "repository-choices"))
					if err != nil || len(files) != 1 {
						t.Fatalf("external write preceded durable choice: %v %v", files, err)
					}
					data, err := os.ReadFile(filepath.Join(store.Factory(brain), "codex", "repository-choices", files[0].Name()))
					if err != nil {
						t.Fatal(err)
					}
					var receipt coordinatorRepoChoice
					if err = strictInput(data, &receipt); err != nil {
						t.Fatal(err)
					}
					if receipt.Repo != "o/one" || receipt.Generation != spec.Generation || receipt.SelectionHash != spec.SelectionHash {
						t.Fatalf("choice binding missing: %+v", receipt)
					}
					writes++
					created := gh.Issue{ID: 123, Number: 7, State: "open", RepositoryURL: "https://api.github.com/" + strings.TrimSuffix(c.Args[3], "/issues")}
					for _, arg := range c.Args {
						if value, ok := strings.CutPrefix(arg, "title="); ok {
							created.Title = value
						}
						if value, ok := strings.CutPrefix(arg, "body="); ok {
							created.Body = value
						}
					}
					return json.Marshal(created)
				}
				if len(c.Args) > 1 && c.Args[1] == "user" {
					return []byte(`{"login":"fixture"}`), nil
				}
				return []byte(`[]`), nil
			}}
			coordinatorGitHub = func() gh.Client { return gh.Client{Runner: fake} }
			create := coordinatorRequest("issue_create", coordinatorIssue{Repo: "o/one", Key: "epic", Title: "sample", Body: "sample epic"})
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, create); err != nil {
				t.Fatal(err)
			}
			before := len(fake.Calls())
			conflict := coordinatorRequest("issue_create", coordinatorIssue{Repo: "o/two", Key: "epic", Title: "sample", Body: "sample epic"})
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, conflict); err == nil || !strings.Contains(err.Error(), "choice conflicts") {
				t.Fatalf("changed retry accepted: %v", err)
			}
			if writes != 1 || len(fake.Calls()) != before {
				t.Fatal("conflicting repository retry reached GitHub")
			}
			changed := "**Repo:** `o/one`\n**Path:** `/fixture/one`\n**Default branch:** `dev`\n"
			if err := os.WriteFile(filepath.Join(brain, "repos", "one.md"), []byte(changed), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, create); err == nil || !strings.Contains(err.Error(), "choice conflicts") {
				t.Fatalf("changed registered base reused old grant: %v", err)
			}
			if len(fake.Calls()) != before {
				t.Fatal("changed registry reached external write before revalidation")
			}

		})
	}
}
func TestCodexPlannerRejectsUnregisteredDeniedAndChangedBranchesBeforeWrite(t *testing.T) {
	brain, spec, _ := coordinatorFixture(t)
	if err := os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	registry := "**Repo:** `o/one`\n**Path:** `/fixture/one`\n**Default branch:** `main`\n"
	registryPath := filepath.Join(brain, "repos", "one.md")
	if err := os.WriteFile(registryPath, []byte(registry), 0600); err != nil {
		t.Fatal(err)
	}
	old := coordinatorGitHub
	t.Cleanup(func() { coordinatorGitHub = old })
	fake := &proc.Fake{}
	coordinatorGitHub = func() gh.Client { return gh.Client{Runner: fake} }
	test := func(repo, base string) {
		t.Helper()
		_, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("issue_create", coordinatorIssue{Repo: repo, Base: base, Key: "epic", Title: "test", Body: "test"}))
		if err == nil {
			t.Fatalf("disallowed repository reached write: %s@%s", repo, base)
		}
	}
	test("unregistered/repo", "")
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		t.Fatal(err)
	}
	writeBlueprint := func() {
		data, err := yaml.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(blueprint.Path(brain), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	b.Deny.Repos = []string{"one"}
	writeBlueprint()
	test("o/one", "")
	b.Deny.Repos = nil
	b.Deny.Branches = []blueprint.RepoBranch{{Repo: "one", Branch: "main"}}
	writeBlueprint()
	test("o/one", "")
	b.Deny.Branches = nil
	writeBlueprint()
	test("o/one", "env-prod")
	if len(fake.Calls()) != 0 {
		t.Fatalf("denial performed an external call: %+v", fake.Calls())
	}
}

func TestCodexObservationCheckDoesNotReacquireManagersTickLock(t *testing.T) {
	brain, spec, owner := coordinatorFixture(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.Profile{SchemaVersion: 1, Executor: "codex", CodexBinary: binary, CodexExecServer: binary, CodexVersion: "0.162.1", CodexModelCatalog: "/fixture/models.json", CodexModelCatalogSHA256: strings.Repeat("a", 64), ContainerImage: "sha256:" + strings.Repeat("a", 64), IdentityBinding: "fixture", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, MaxActiveHelpers: 1, Routes: map[string][]executor.Candidate{}}
	routes := map[string][]executor.Route{}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		profile.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "high"}}
		routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "high", Model: executor.CodexModel}}
	}
	selected := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: spec.EpicID, Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
	if err = executor.WriteSelection(brain, selected); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(selected)
	selectionHash := sha256.Sum256(encoded)
	spec.SelectionHash = hex.EncodeToString(selectionHash[:])
	sessionEntry := store.Entry{Kind: "session", Epic: spec.EpicID, Dir: filepath.Join(owner.Dir, "sessions", "planner")}
	if err = os.MkdirAll(sessionEntry.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteInputs(sessionEntry.Dir, reconcile.SessionInputs{Name: spec.Name, Component: "planner", Task: spec.Task}); err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(brain)
	ix["epic1-planner"] = sessionEntry
	if err = store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	if _, err = events.Append(store.EventsPath(owner.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err = events.Append(store.EventsPath(sessionEntry.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "running"}); err != nil {
		t.Fatal(err)
	}
	idHash := sha256.Sum256([]byte(spec.Name))
	id := hex.EncodeToString(idHash[:])
	process, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	record := codex.Record{Schema: 1, ID: id, Spec: spec, Selection: selected, State: "idle", Token: "fixture", Host: process, Deliveries: map[string]string{}}
	dir := filepath.Join(store.Factory(brain), "codex", "sessions", id)
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(record)
	if err = os.WriteFile(filepath.Join(dir, "state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	done := make(chan int, 1)
	go func() {
		done <- runCodexControl([]string{"--brain", brain, "--session", id, "--check"}, io.Discard, io.Discard)
	}()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("read-only manager check failed: %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("manager-owned host check waited for its caller's tick lock")
	}
}
func TestCodexCompactionCheckCannotBeCombinedWithToolGrant(t *testing.T) {
	for _, args := range [][]string{{"--session", "x", "--compact-check", "--guard"}, {"--session", "x", "--compact-check", "--check"}, {"--job", "x", "--compact-check"}} {
		if code := runCodexControl(args, io.Discard, io.Discard); code != 2 {
			t.Fatalf("combined compaction/tool authority accepted: %v => %d", args, code)
		}
	}
}

func TestCodexPlannerInstructionsAndUserQuestionsKeepScope(t *testing.T) {
	brain, spec, ep := coordinatorFixture(t)
	ix, _ := store.ReadIndex(brain)
	for _, id := range []string{"set1", "other"} {
		entry := store.Entry{Kind: blueprint.MachineSet, Epic: spec.EpicID, Dir: filepath.Join(ep.Dir, id)}
		if id == "other" {
			entry.Epic = "foreign"
		}
		if err := os.MkdirAll(entry.Dir, 0700); err != nil {
			t.Fatal(err)
		}
		ix[id] = entry
	}
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	req := coordinatorRequest("event", coordinatorPayload{Target: "set1", Kind: events.KindInstruction, Text: "Recheck the resolved blocker"})
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err != nil {
		t.Fatal(err)
	}
	evs, err := events.Read(store.EventsPath(ix["set1"].Dir))
	if err != nil || len(evs) != 1 || evs[0].Kind != events.KindInstruction {
		t.Fatalf("instruction=%+v %v", evs, err)
	}
	for _, op := range []string{"file_read", "inbox", "events"} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest(op, coordinatorPayload{Target: "set1", Path: "inputs.yaml"})); err == nil {
			t.Fatalf("instruction exception widened to %s", op)
		}
	}
	for _, op := range []string{"inbox", "events"} {
		t.Run("empty_path_"+op, func(t *testing.T) {
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest(op, coordinatorPayload{Target: "set1"})); err == nil {
				t.Fatalf("instruction exception widened to %s without a path", op)
			}
		})
	}
	t.Run("noninstruction_event", func(t *testing.T) {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("event", coordinatorPayload{Target: "set1", Kind: events.KindMessage, Text: "not an instruction"})); err == nil {
			t.Fatal("instruction exception widened to a message event")
		}
	})
	for _, target := range []string{"other", spec.Task} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("event", coordinatorPayload{Target: target, Kind: events.KindInstruction, Text: "bad"})); err == nil {
			t.Fatalf("instruction escaped to %s", target)
		}
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("event", coordinatorPayload{Kind: events.KindMessage, Text: "Which option?", To: events.RecipientUser})); err != nil {
		t.Fatal(err)
	}
	evs, err = events.Read(store.EventsPath(ep.Dir))
	if err != nil || len(evs) != 1 || evs[0].Recipient != events.RecipientUser {
		t.Fatalf("question recipient=%+v %v", evs, err)
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("events", coordinatorPayload{})); err != nil {
		t.Fatal(err)
	}
}
func TestCodexPlanInputOnlyReadsOwnedHashedSnapshot(t *testing.T) {
	brain, spec, ep := coordinatorFixture(t)
	data := []byte("exact accepted plan\n")
	hash := sha256.Sum256(data)
	full := filepath.Join(ep.Dir, "plan-input.yaml")
	if err := os.WriteFile(full, data, 0400); err != nil {
		t.Fatal(err)
	}
	in := &reconcile.EpicInputs{ID: spec.Task, Kind: "plan", Path: full, PlanInputSHA256: hex.EncodeToString(hash[:])}
	if err := store.WriteInputs(ep.Dir, in); err != nil {
		t.Fatal(err)
	}
	out, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", struct{}{}))
	if err != nil {
		t.Fatal(err)
	}
	if out.(map[string]string)["text"] != string(data) {
		t.Fatalf("plan=%v", out)
	}
	for _, payload := range []any{map[string]string{"path": "/tmp/host"}, map[string]string{"target": "another"}} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", payload)); err == nil {
			t.Fatal("plan input widened to caller path")
		}
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("file_write", coordinatorPayload{Path: "plan-input.yaml", Text: "forged"})); err == nil {
		t.Fatal("snapshot writable by planner")
	}
	in.Path = "/tmp/host"
	if err := store.WriteInputs(ep.Dir, in); err != nil {
		t.Fatal(err)
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", struct{}{})); err == nil {
		t.Fatal("external plan path accepted")
	}
}

func TestCodexGitHubReadsUseRegisteredScopeAndBoundedReadonlyCommands(t *testing.T) {
	brain, spec, _ := coordinatorFixture(t)
	if err := os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "repos", "sample.md"), []byte("**Repo:** `o/sample`\n**Path:** `/fixture/sample`\n**Default branch:** `main`\n"), 0600); err != nil {
		t.Fatal(err)
	}
	old := coordinatorGitHub
	t.Cleanup(func() { coordinatorGitHub = old })
	fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if c.Name != "gh" || len(c.Args) < 2 {
			t.Fatalf("unexpected command:%+v", c)
		}
		if c.Args[0] == "api" && (strings.Contains(c.Args[1], "graphql") || strings.Contains(strings.Join(c.Args, " "), "-X")) {
			t.Fatalf("mutation:%+v", c)
		}
		return []byte(`[]`), nil
	}}
	coordinatorGitHub = func() gh.Client { return gh.Client{Runner: fake} }
	for _, op := range []string{"github_prs", "github_commit", "github_file", "github_runs", "github_run", "github_job_log"} {
		_, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest(op, coordinatorRead{Repo: "o/sample", Number: 7, SHA: strings.Repeat("a", 40), Path: "README.md"}))
		if err != nil {
			t.Fatalf("%s:%v", op, err)
		}
	}
	before := len(fake.Calls())
	for _, p := range []coordinatorRead{{Repo: "o/foreign", Path: "README.md"}, {Repo: "o/sample", Path: "../secret"}, {Repo: "o/sample", Path: "README.md", Base: "unregistered"}} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("github_file", p)); err == nil {
			t.Fatalf("bad scope accepted:%+v", p)
		}
	}
	if len(fake.Calls()) != before {
		t.Fatal("denied read reached GitHub")
	}
	fake.Respond = func(proc.Cmd) ([]byte, error) { return []byte(strings.Repeat("x", controlLimit+1)), nil }
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("github_job_log", coordinatorRead{Repo: "o/sample", Number: 7})); err == nil || !strings.Contains(err.Error(), "output exceeds") {
		t.Fatalf("unbounded output:%v", err)
	}
}

func TestCodexWorkArtifactsStayInsideOwnedWork(t *testing.T) {
	brain, spec, ep := coordinatorFixture(t)
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		t.Fatal(err)
	}
	b.Components["explorer"] = blueprint.Component{RunsAs: blueprint.RunsAsHelper, Tier: "medium", Instructions: "components/planner.md", Outputs: blueprint.Outputs{{Path: "sets/<set-id>/<work-id>/explore.md"}, {Path: "sets/<set-id>/<work-id>/plan.md"}}}
	data, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	set := store.Entry{Kind: blueprint.MachineSet, Epic: spec.EpicID, Dir: filepath.Join(ep.Dir, "sets", "set1")}
	work := store.Entry{Kind: blueprint.MachineWork, Epic: spec.EpicID, Dir: filepath.Join(set.Dir, "work1")}
	if err := os.MkdirAll(work.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(work.Dir, &reconcile.WorkInputs{ID: "work1", Epic: spec.EpicID, Set: "set1"}); err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(brain)
	ix["set1"], ix["work1"] = set, work
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	spec.Task, spec.Name, spec.Generation = "set1", "factory:set1:orchestrator", "set1:0:orchestrator"
	for _, name := range []string{"handoff.md", "researcher_work1.md"} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("file_write", coordinatorPayload{Target: "work1", Path: name, Text: "owned"})); err != nil {
			t.Fatalf("%s:%v", name, err)
		}
	}
	for _, name := range []string{"explore.v1.md", "plan.v1.md"} {
		if err := os.WriteFile(filepath.Join(work.Dir, name), []byte("helper output"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("file_read", coordinatorPayload{Target: "work1", Path: name})); err != nil {
			t.Fatalf("%s:%v", name, err)
		}
	}
	if err := os.WriteFile(filepath.Join(ep.Dir, "DESIGN.md"), []byte("epic design"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("epic_file", map[string]string{"path": "DESIGN.md"})); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../other", "inputs.yaml", "events.jsonl", "PLAN.md"} {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("file_write", coordinatorPayload{Target: "work1", Path: name, Text: "forged"})); err == nil {
			t.Fatalf("unsafe artifact write:%s", name)
		}
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("epic_file", map[string]string{"path": "inputs.yaml"})); err == nil {
		t.Fatal("epic file read widened")
	}
}

func TestCodexOwnedChildHistorySurvivesCoordinatorRestart(t *testing.T) {
	brain, spec, ep := coordinatorFixture(t)
	set := store.Entry{Kind: blueprint.MachineSet, Epic: spec.EpicID, Dir: filepath.Join(ep.Dir, "sets", "set1")}
	work := store.Entry{Kind: blueprint.MachineWork, Epic: spec.EpicID, Dir: filepath.Join(set.Dir, "work1")}
	if err := os.MkdirAll(work.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(work.Dir, &reconcile.WorkInputs{ID: "work1", Epic: spec.EpicID, Set: "set1"}); err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(brain)
	ix["set1"], ix["work1"] = set, work
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendTo(brain, "work1", events.Event{Kind: events.KindEnd, File: filepath.Join(work.Dir, "explore.v3.md")}); err != nil {
		t.Fatal(err)
	}
	spec.Task, spec.Name, spec.Generation = "set1", "factory:set1:orchestrator", "set1:0:orchestrator"
	result, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("events", coordinatorPayload{Target: "work1"}))
	if err != nil {
		t.Fatal(err)
	}
	history := result.([]events.Event)
	if len(history) != 1 || filepath.Base(history[0].File) != "explore.v3.md" {
		t.Fatalf("lost version history:%+v", history)
	}
	if _, err := store.AppendTo(brain, "work1", events.Event{Kind: events.KindHandover}); err != nil {
		t.Fatal(err)
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("events", coordinatorPayload{Target: "work1"})); err == nil || !strings.Contains(err.Error(), "handed over") {
		t.Fatalf("handed-over history accepted:%v", err)
	}
}
