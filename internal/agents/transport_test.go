package agents

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
)

func TestManagedToolTextUsesOnlyAllowedOperations(t *testing.T) {
	brain, b, out := codexBrain(t)
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
	brain, b, out := codexBrain(t)
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
