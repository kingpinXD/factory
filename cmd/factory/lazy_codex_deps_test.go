package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func lazyBrain(t *testing.T) string {
	t.Helper()
	brain := testBrain(t)
	data, err := os.ReadFile("../../internal/fsm/testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{"factory/blueprint.yaml": string(data), "AGENTS.md": "## Model tiers\n\n| Tier | Claude | Codex |\n| --- | --- | --- |\n| low | sonnet | gpt-6.1-sol |\n| medium | opus | gpt-6.1-sol |\n| high | opus | gpt-6.1-sol |\n"}
	for name, c := range b.Components {
		if c.Instructions == "" {
			continue
		}
		body := "# " + name + "\n"
		if c.RunsAs == blueprint.RunsAsHelper {
			body = fmt.Sprintf("---\nname: %s\ndescription: The %s.\ntools: %s\n---\n# %s\n", name, name, strings.Join(c.Tools, ", "), name)
		}
		files["factory/"+c.Instructions] = body
	}
	for file, data := range files {
		p := filepath.Join(brain, file)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return brain
}

func failingCodexConstructor(t *testing.T) *int {
	t.Helper()
	calls := 0
	old := newFactoryCodexRuntime
	newFactoryCodexRuntime = func(string, string) (*codex.Runtime, error) {
		calls++
		return nil, errors.New("injected Codex setup failure")
	}
	t.Cleanup(func() { newFactoryCodexRuntime = old })
	return &calls
}

// Keep the production dependency construction; replace only external adapters.
func lazyDepsCLI(t *testing.T) *proc.Fake {
	t.Helper()
	fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.Contains(strings.Join(c.Args, " "), "agents --json --all") {
			return []byte(`[{"name":"factory:e2610091:planner","id":"planner","pid":0,"state":"stopped"},{"name":"factory:e-claude:planner","id":"other","pid":0,"state":"stopped"}]`), nil
		}
		return nil, nil
	}}
	old := newDeps
	newDeps = func(out io.Writer) (reconcile.Deps, error) {
		d, err := depsForBrain(os.Getenv("FACTORY_BRAIN"), out)
		d.Now = func() time.Time { return time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) }
		d.Claude.Runner, d.GitHub.Runner = fake, fake
		d.Account = reconcile.NoAccount{}
		return d, err
	}
	t.Cleanup(func() { newDeps = old })
	return fake
}

func TestClaudeCommandsDoNotConstructCodex(t *testing.T) {
	lazyBrain(t)
	calls := failingCodexConstructor(t)
	fake := lazyDepsCLI(t)
	for _, args := range [][]string{{"tick"}, {"add", "fix the flag", "--executor", "claude"}} {
		if code, _, stderr := cli(t, args...); code != 0 {
			t.Errorf("%v: exit %d: %s", args, code, stderr)
		}
	}
	writer, err := issueWriter(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Comment(context.Background(), "o/r", 9, "ready"); err != nil {
		t.Fatal(err)
	}
	if *calls != 0 {
		t.Fatalf("Claude invoked Codex constructor %d times", *calls)
	}
	found := false
	for _, c := range fake.Calls() {
		if c.Name == "gh" && strings.Contains(strings.Join(c.Args, " "), "api -X POST repos/o/r/issues/9/comments") {
			found = true
		}
	}
	if !found {
		t.Fatalf("issue writer never wrote: %v", fake.Calls())
	}
	d, err := depsForBrain(os.Getenv("FACTORY_BRAIN"), io.Discard)
	if err != nil || d.Helpers != nil || d.StartHelpers != nil || d.Runtimes["codex"] != nil {
		t.Fatalf("Claude dependencies contain Codex: %+v, %v", d, err)
	}
}

func TestExplicitCodexAddConstructsBeforeSelection(t *testing.T) {
	brain := lazyBrain(t)
	calls := failingCodexConstructor(t)
	lazyDepsCLI(t)
	code, _, stderr := cli(t, "add", "fix it", "--executor", "codex")
	if code != 1 || !strings.Contains(stderr, "injected Codex setup failure") || *calls != 1 {
		t.Fatalf("Codex add: exit=%d calls=%d stderr=%s", code, *calls, stderr)
	}
	if entries, err := os.ReadDir(filepath.Join(store.Factory(brain), "executors")); err != nil && !errors.Is(err, os.ErrNotExist) || len(entries) != 0 {
		t.Fatalf("selection written before setup: %v, %v", entries, err)
	}
	if ix, err := store.ReadIndex(brain); err != nil || len(ix) != 0 {
		t.Fatalf("index published before setup: %v, %v", ix, err)
	}
}

func selectedSetupEpic(t *testing.T, brain, id, provider string) {
	t.Helper()
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	dir := filepath.Join(store.Factory(brain), "work", id)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(dir, reconcile.EpicInputs{ID: id, Input: "fix it", Kind: "text", AddedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(dir), events.Event{Kind: events.KindTransition, At: now, To: "checking", Trigger: "tick", TriggerRef: "fixture"}); err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	ix[id] = store.Entry{Kind: blueprint.MachineEpic, Epic: id, Dir: dir}
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	if provider != "codex" {
		return
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		t.Fatal(err)
	}
	p := executor.DefaultCodexProfile(b)
	p.CodexBinary, p.CodexExecServer = "/fixture/codex", "/fixture/exec"
	p.CodexModelCatalog, p.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	p.CodexVersion, p.IdentityBinding = "0.162.1", "fixture"
	p.ContainerImage, p.ModelEvidencePolicy = "sha256:"+strings.Repeat("a", 64), executor.CodexModelEvidencePolicy
	routes := map[string][]executor.Route{}
	for name, candidates := range p.Routes {
		c := candidates[0]
		routes[name] = []executor.Route{{Provider: "codex", Tier: c.Tier, Effort: c.Effort, Model: executor.CodexModel}}
	}
	s := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: id, Profile: p, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: p.ContainerImage, CreatedAt: now}
	if err := executor.WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
}

func TestCodexSetupErrorDoesNotBlockClaudeEpic(t *testing.T) {
	brain := lazyBrain(t)
	selectedSetupEpic(t, brain, "e-claude", "claude")
	selectedSetupEpic(t, brain, "e-codex", "codex")
	calls := failingCodexConstructor(t)
	lazyDepsCLI(t)
	d, err := newDeps(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconcile.Status(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	foundHold := false
	for _, epic := range report.Epics {
		if epic.ID == "e-codex" {
			foundHold = strings.Contains(epic.ExecutorHold, "Codex setup: injected Codex setup failure")
		}
	}
	if !foundHold {
		t.Error("status before tick omits the Codex setup error")
	}
	*calls = 0
	if code, _, stderr := cli(t, "tick"); code != 0 {
		t.Fatalf("mixed tick exit=%d: %s", code, stderr)
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ix["e-claude-planner"]; !ok {
		t.Fatal("Claude epic did not get its planner in the same tick")
	}
	if _, ok := ix["e-codex-planner"]; ok {
		t.Fatal("Codex launched despite setup failure")
	}
	st, err := store.ReadStatus(ix["e-codex"].Dir)
	if err != nil || !strings.Contains(st.ExecutorHold, "Codex setup: injected Codex setup failure") {
		t.Fatalf("missing named setup error: %+v, %v", st, err)
	}
	if *calls != 1 {
		t.Fatalf("constructor calls=%d, want 1", *calls)
	}
}

func TestRelativeClaudeBrainDoesNotConstructCodex(t *testing.T) {
	brain := lazyBrain(t)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, brain)
	if err != nil {
		t.Fatal(err)
	}
	calls := failingCodexConstructor(t)
	d, err := depsForBrain(relative, io.Discard)
	if err != nil || *calls != 0 || d.Brain != relative || d.Helpers != nil {
		t.Fatalf("relative Claude dependencies: calls=%d, err=%v", *calls, err)
	}
}

func TestCorruptSelectionDoesNotFallBackToClaude(t *testing.T) {
	brain := lazyBrain(t)
	selectedSetupEpic(t, brain, "e-claude", "claude")
	selectedSetupEpic(t, brain, "e-corrupt", "claude")
	dir := filepath.Join(store.Factory(brain), "executors", "e-corrupt")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "selection.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	calls := failingCodexConstructor(t)
	lazyDepsCLI(t)
	if code, _, stderr := cli(t, "tick"); code != 0 {
		t.Fatalf("mixed corrupt tick exit=%d: %s", code, stderr)
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ix["e-claude-planner"]; !ok {
		t.Fatal("Claude epic blocked by corrupt selection")
	}
	if _, ok := ix["e-corrupt-planner"]; ok {
		t.Fatal("corrupt selection silently became Claude")
	}
	st, err := store.ReadStatus(ix["e-corrupt"].Dir)
	if err != nil || !strings.Contains(st.ExecutorHold, "selection epic identity mismatch") {
		t.Fatalf("corrupt selection not reported: %+v, %v", st, err)
	}
	d, err := newDeps(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconcile.Status(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ep := range report.Epics {
		if ep.ID == "e-corrupt" {
			found = ep.Executor == "unknown" && strings.Contains(ep.ExecutorHold, "selection epic identity mismatch")
		}
	}
	if !found {
		t.Fatalf("status hides corrupt selection: %+v", report.Epics)
	}
	if *calls != 0 {
		t.Fatal("corrupt selection requested Codex setup")
	}
}
