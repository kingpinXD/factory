package agents

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"go.yaml.in/yaml/v3"
)

func nativeBrain(t *testing.T) (string, *blueprint.Blueprint, executor.Selection, string) {
	t.Helper()
	brain := t.TempDir()
	err := os.CopyFS(brain, os.DirFS("testdata/brain"))
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(brain, "AGENTS.md"), "## Model tiers\n\n| Tier | Claude | Zed |\n| --- | --- | --- |\n| high | claude-opus-5-5 | |\n| medium | opus | openai-subscribed/gpt-6.1-sol |\n| low | sonnet | |\n")
	var b blueprint.Blueprint
	data, err := os.ReadFile(filepath.Join(brain, "factory", "blueprint.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &b); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		c := b.Components[name]
		c.Tier = "high"
		b.Components[name] = c
	}
	p := executor.DefaultProfile(&b)
	p.NativeWorker = "/test/worker"
	p.NativeUsage = "/test/usage"
	p.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	p.CredentialNamespace = "test-factory"
	p.IdentityBinding = "test-grant"
	routes, err := executor.Resolve(p, mustTiers(t, brain), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := executor.Selection{SchemaVersion: 1, Executor: "z", EpicID: "e-test", Profile: p, Routes: routes, Binaries: map[string]executor.Binary{"worker": {Path: p.NativeWorker, SHA256: strings.Repeat("b", 64)}, "usage": {Path: p.NativeUsage, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: p.ContainerImage, CreatedAt: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)}
	if err := executor.WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(brain, "factory", "executors", s.EpicID)
	return brain, &b, s, out
}
func mustTiers(t *testing.T, brain string) blueprint.Tiers {
	t.Helper()
	v, err := blueprint.ReadTiers(brain)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBuildForRealNativeComponents(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	put(t, Path(brain), "live sentinel")
	put(t, PromptPath(brain, "planner"), "live prompt sentinel")
	helpers, sessions, err := BuildFor(brain, out, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(helpers) != 0 || len(sessions) != 3 {
		t.Fatalf("native helpers leaked: %v sessions %v", helpers, sessions)
	}
	if got := read(t, Path(brain)); got != "live sentinel" {
		t.Fatal(got)
	}
	if got := read(t, PromptPath(brain, "planner")); got != "live prompt sentinel" {
		t.Fatal(got)
	}
	if got := read(t, filepath.Join(out, "agents.json")); strings.TrimSpace(got) != "{}" {
		t.Fatal(got)
	}
	for name, c := range b.Components {
		if c.Placeholder || c.Instructions == "" {
			continue
		}
		prompt := read(t, PromptPathFor(out, name))
		for _, bad := range []string{"SendMessage", "Agent call", "Spawn in one"} {
			if strings.Contains(prompt, bad) {
				t.Errorf("%s retains %q", name, bad)
			}
		}
		for _, op := range []string{"submit", "continue", "result", "yield"} {
			want := "helper " + op
			if c.RunsAs == blueprint.RunsAsHelper {
				want = `factory_helper(op="` + op + `")`
			}
			if !strings.Contains(prompt, want) {
				t.Errorf("%s missing %s bridge", name, op)
			}
		}
		if c.RunsAs == blueprint.RunsAsHelper {
			for _, op := range []string{"inbox", "lease_check", "heartbeat", "intent", "done", "error", "brain_read"} {
				if !strings.Contains(prompt, op) {
					t.Errorf("%s missing %s", name, op)
				}
			}
			if strings.Contains(prompt, "factory inbox") || strings.Contains(prompt, "factory event") {
				t.Errorf("%s retains host controls", name)
			}
		}
	}
	reviewer := read(t, PromptPathFor(out, "plan-reviewer"))
	if !strings.Contains(reviewer, "same conversation") || !strings.Contains(reviewer, "Round 2") {
		t.Fatal("lost round-two review contract")
	}
	verifier := read(t, PromptPathFor(out, "verifier"))
	for _, name := range []string{"reviewer-code", "reviewer-arch-test", "reviewer-issue-completeness", "reviewer-red-team"} {
		if !strings.Contains(verifier, name) {
			t.Fatal("lost nested panel", name)
		}
	}
	// Rebuilds consume the immutable source snapshot, not later brain edits.
	before := read(t, PromptPathFor(out, "implementor"))
	put(t, filepath.Join(brain, "factory", "components", "implementor.md"), "changed live component")
	put(t, filepath.Join(brain, "factory", blueprint.RulesFile), "changed live rules")
	put(t, filepath.Join(brain, "AGENTS.md"), "changed live table")
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if after := read(t, PromptPathFor(out, "implementor")); after != before {
		t.Fatal("snapshot changed")
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(read(t, filepath.Join(out, "sources.json"))), &snapshot); err != nil {
		t.Fatal(err)
	}
}

func TestBuildForRejectsTierOverrideAndCorruption(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, map[string]string{"planner": "medium"}); err == nil {
		t.Fatal("accepted lower top tier")
	}
	put(t, filepath.Join(out, "selection.json"), `{"schema_version":99}`)
	if _, _, err := BuildFor(brain, out, b, nil); err == nil {
		t.Fatal("corrupt selection became legacy")
	}
}

func TestBuildForLegacyBytesMatchBuild(t *testing.T) {
	brain, b := testBrain(t)
	if _, _, err := Build(brain, b); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "isolated")
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := read(t, filepath.Join(out, "agents.json")), read(t, Path(brain)); got != want {
		t.Fatalf("legacy definitions changed\ngot: %s\nwant: %s", got, want)
	}
	for _, name := range []string{"planner", "uat"} {
		if read(t, PromptPathFor(out, name)) != read(t, PromptPath(brain, name)) {
			t.Fatal("legacy prompt changed", name)
		}
	}
}

func TestBuildForRejectsUnmappedHelperName(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	file := filepath.Join(brain, "factory", "components", "implementor.md")
	put(t, file, strings.Replace(read(t, file), "name: implementor", "name: reviewer-code", 1))
	if _, _, err := BuildFor(brain, out, b, nil); err == nil || !strings.Contains(err.Error(), "differs from component") {
		t.Fatalf("unmapped helper: %v", err)
	}
}

func TestHostControlTransformationPreservesActionText(t *testing.T) {
	for _, kind := range []string{"intent", "done", "error"} {
		input := "factory event <work-id> " + kind + " --text \"branch and SHA\""
		got := transformHelper(input)
		if !strings.Contains(got, "factory_helper "+kind) || !strings.Contains(got, "branch and SHA") {
			t.Fatalf("lost action payload: %s", got)
		}
	}
}

func TestBuildForRejectsSharedDirectoryAlias(t *testing.T) {
	brain, b := testBrain(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Join(brain, "factory"), alias); err != nil {
		t.Fatal(err)
	}
	if _, _, err := BuildFor(brain, alias, b, nil); err == nil || !strings.Contains(err.Error(), "isolated") {
		t.Fatalf("shared alias: %v", err)
	}
	if _, err := os.Stat(filepath.Join(brain, "factory", "sources.json")); !os.IsNotExist(err) {
		t.Fatal("shared files written")
	}
}

func TestBuildForIncompleteSelectionNeverLegacy(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if err := os.Remove(filepath.Join(out, "selection.json")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := BuildFor(brain, out, b, nil); err == nil {
		t.Fatal("incomplete selection became legacy")
	}
	if _, err := os.Stat(filepath.Join(out, "sources.json")); !os.IsNotExist(err) {
		t.Fatal("generated legacy snapshot for incomplete selection")
	}
}
