package agents

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
)

func TestLegacyPromptHashes(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	snapshot := sourceSnapshot{SchemaVersion: 1, Components: b.Components, Instructions: map[string]blueprint.Instructions{}, Rules: read(t, filepath.Join(brain, "factory", blueprint.RulesFile)), Bridge: read(t, "testdata/legacy-native-bridge.md")}
	for name, c := range b.Components {
		if c.Instructions == "" {
			continue
		}
		ins, err := blueprint.ReadInstructions(brain, c.Instructions)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.Instructions[name] = ins
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(out, "sources.json"), string(data)+"\n")
	before := read(t, filepath.Join(out, "sources.json"))
	want := map[string]string{
		"implementor":   "649c7470114cb155e385f005256fdc191877c80d6f3410d3d4b0d6598742a660",
		"plan-reviewer": "98da3e8fbc47e8eb81c03ac62e8085796280aebc3f3db5cc624c4ed2780af0fb",
		"verifier":      "f22fafe60c6cbdc29b8c68550d4e5b1eb93dfcc2167175e3064a447c698d7515",
		"pr-opener":     "73c58bbcbc0e2e40a455252e70f21b503ee62e9011c3f209d5ad072b4bc40e06",
		"orchestrator":  "edda760ae5230142474155a7a7c02036eac23d1f66a51cf28d92e8c0c0b33f19",
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, _, err := BuildFor(brain, out, b, nil); err != nil {
			t.Fatal(err)
		}
		for name, hash := range want {
			data, err := os.ReadFile(PromptPathFor(out, name))
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != hash {
				t.Errorf("existing %s snapshot changed: %s", name, got)
			}
		}
		if got := read(t, filepath.Join(out, "sources.json")); got != before {
			t.Fatal("existing source snapshot rewritten")
		}
	}
}

func TestRealNativePromptTransportABI(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	for name, c := range b.Components {
		if c.Placeholder || c.Instructions == "" {
			continue
		}
		prompt := read(t, PromptPathFor(out, name))
		if c.RunsAs == blueprint.RunsAsHelper {
			for _, op := range []string{"submit", "continue", "result", "yield", "inbox", "lease_check", "heartbeat", "intent", "done", "error", "brain_read"} {
				if !strings.Contains(prompt, `factory_helper(op="`+op+`")`) {
					t.Errorf("%s missing typed operation %s", name, op)
				}
			}
			for _, forbidden := range []string{"helper submit", "helper continue", "helper result", "helper yield", "factory issue", "factory query", "~/.agents/", "--ack"} {
				if strings.Contains(prompt, forbidden) {
					t.Errorf("%s retains %q", name, forbidden)
				}
			}
			for _, required := range []string{"{op,payload}", "Docker-only", "cannot invoke host FACTORY_BIN", "no start/end/ack", "AGENTS.md", "repos/", "docs/", "skills/", "features/", "canonical", "symlink", "connectors/", "auth", ".env", "bounded markdown"} {
				if !strings.Contains(prompt, required) {
					t.Errorf("%s missing transport constraint %q", name, required)
				}
			}
		} else {
			if !strings.Contains(prompt, "Top-level Claude uses the normal CLI") || !strings.Contains(prompt, "helper submit") {
				t.Errorf("%s lost host CLI", name)
			}
			if !strings.Contains(prompt, "~/.agents/") {
				t.Errorf("%s incorrectly transformed host knowledge reads", name)
			}
		}
		if strings.Count(prompt, "# Native helper bridge") != 1 {
			t.Errorf("%s duplicates workflow bridge", name)
		}
	}
	source := read(t, filepath.Join(out, "sources.json"))
	prompt := read(t, PromptPathFor(out, "implementor"))
	if !strings.Contains(prompt, `factory_helper(op="brain_read") for skills/implementor/SKILL.md`) {
		t.Fatal("component skill read did not use typed brain_read")
	}
	if !strings.Contains(prompt, `factory_helper(op="brain_read") for repos/<short-name>.md`) {
		t.Fatal("shared registry read did not use typed brain_read")
	}
	put(t, filepath.Join(brain, "factory", "components", "implementor.md"), "changed live component")
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if read(t, filepath.Join(out, "sources.json")) != source || read(t, PromptPathFor(out, "implementor")) != prompt {
		t.Fatal("transport update broke snapshot immutability")
	}
}

func TestManagedToolTextUsesOnlyAllowedOperations(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"submit": true, "continue": true, "result": true, "yield": true, "inbox": true, "lease_check": true, "heartbeat": true, "intent": true, "done": true, "error": true, "brain_read": true}
	calls := regexp.MustCompile(`factory_helper\(op="([^"]+)"\)`)
	for name, c := range b.Components {
		if c.RunsAs != blueprint.RunsAsHelper || c.Placeholder {
			continue
		}
		prompt := read(t, PromptPathFor(out, name))
		for _, match := range calls.FindAllStringSubmatch(prompt, -1) {
			if !allowed[match[1]] {
				t.Errorf("%s requests forbidden host operation %s", name, match[1])
			}
		}
	}
}

func TestManagedKnowledgeReferencesDoNotBroadenAllowlist(t *testing.T) {
	for _, path := range []string{"AGENTS.md", "repos/factory.md", "docs/lang.md", "skills/implementor/SKILL.md", "features/agent_factory/PLAN.md"} {
		got := managedToolText("Read ~/.agents/" + path)
		if !strings.Contains(got, `factory_helper(op="brain_read") for `+path) {
			t.Fatalf("allowed knowledge lost: %s", got)
		}
	}
	for _, path := range []string{"connectors/auth.md", "auth/account.md", ".env", "memory/tone_guide.md", "repos/../connectors/auth.md"} {
		got := managedToolText("Read ~/.agents/" + path)
		if strings.Contains(got, `factory_helper(op="brain_read")`) || strings.Contains(got, "~/.agents/") {
			t.Fatalf("forbidden knowledge became a brain read: %s", got)
		}
		if !strings.Contains(got, "supplied by the owning session") {
			t.Fatalf("missing owner handoff: %s", got)
		}
	}
}

func TestBuildForRejectsUnknownPromptProtocol(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(out, "sources.json")
	before := read(t, PromptPathFor(out, "implementor"))
	snapshot, err := readSources(file)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.PromptProtocol = 99
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	put(t, file, string(data))
	if _, _, err := BuildFor(brain, out, b, nil); err == nil || !strings.Contains(err.Error(), "prompt transport") {
		t.Fatalf("unsupported transport accepted: %v", err)
	}
	if got := read(t, PromptPathFor(out, "implementor")); got != before {
		t.Fatal("unsupported transport replaced existing prompt")
	}
}
