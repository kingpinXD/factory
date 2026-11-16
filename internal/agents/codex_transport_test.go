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

func codexBrain(t *testing.T) (string, *blueprint.Blueprint, string) {
	t.Helper()
	brain := t.TempDir()
	if err := os.CopyFS(brain, os.DirFS("testdata/brain")); err != nil {
		t.Fatal(err)
	}
	var b blueprint.Blueprint
	data, err := os.ReadFile(filepath.Join(brain, "factory", "blueprint.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &b); err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(brain, "AGENTS.md"), "## Model tiers\n\n| Tier | Claude | Codex |\n| --- | --- | --- |\n| medium | opus | gpt-6.1-sol |\n| low | sonnet | gpt-6.1-sol |\n")
	p := executor.DefaultCodexProfile(&b)
	p.CodexBinary = "/test/codex"
	p.CodexExecServer = "/test/codex-exec-server"
	p.ContainerImage = "sha256:" + strings.Repeat("b", 64)
	p.CodexModelCatalog, p.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	p.CodexVersion = "0.162.1"
	p.IdentityBinding = "test-account"
	p.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	routes, err := executor.Resolve(p, mustTiers(t, brain), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "e-codex", Profile: p, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("a", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: p.ContainerImage, CreatedAt: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
	if err := executor.WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
	return brain, &b, filepath.Join(brain, "factory", "executors", s.EpicID)
}

func TestCodexBuildForUsesScopedToolsAndBroker(t *testing.T) {
	brain, b, out := codexBrain(t)
	helpers, sessions, err := BuildFor(brain, out, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(helpers) != 0 || len(sessions) != 3 || strings.TrimSpace(read(t, filepath.Join(out, "agents.json"))) != "{}" {
		t.Fatalf("Codex leaked native agent definitions: %v %v", helpers, sessions)
	}
	for name, c := range b.Components {
		if c.Placeholder || c.Instructions == "" {
			continue
		}
		prompt := read(t, PromptPathFor(out, name))
		for _, bad := range []string{"SendMessage", "Agent call", "`Agent`", "Top-level Claude transport", "factory event", "factory heartbeat", "factory inbox"} {
			if strings.Contains(prompt, bad) {
				t.Errorf("%s retains %q", name, bad)
			}
		}
		for _, want := range []string{"same reviewer job", "persisted terminal turn", "confirmed stopped writers", "Do not post reviews or review requests on Slack"} {
			if !strings.Contains(prompt, want) {
				t.Errorf("%s lost %q", name, want)
			}
		}
		if c.RunsAs == blueprint.RunsAsSession {
			for _, want := range []string{`factory_control(op="helper_submit")`, `factory_control(op="helper_continue")`, `factory_control(op="helper_result")`, `factory_control(op="helper_cancel")`, "payload:object", "agents.enabled=false", "lifecycle", "checkpoint", `event{kind:"message",text,to:"user"}`} {
				if !strings.Contains(prompt, want) {
					t.Errorf("coordinator %s lost %q", name, want)
				}
			}
			for _, bad := range []string{"factory_helper", "managed_files", "managed_terminal"} {
				if strings.Contains(prompt, bad) {
					t.Errorf("coordinator %s advertises unavailable %s", name, bad)
				}
			}
		} else if strings.Contains(prompt, "--ack") || strings.Contains(prompt, "factory_control operation") {
			t.Errorf("helper %s acquired coordinator controls", name)
		} else {
			for _, want := range []string{`factory_helper(op="submit")`, `factory_helper(op="continue")`, `factory_helper(op="result")`, `factory_helper(op="yield")`, "managed_files", "managed_terminal"} {
				if !strings.Contains(prompt, want) {
					t.Errorf("helper %s lost %s", name, want)
				}
			}
		}
	}
	planner := read(t, PromptPathFor(out, "planner"))
	if !strings.Contains(planner, "factory_control operation issue_create") || strings.Contains(planner, "Return any required issue changes to the owning session") {
		t.Fatal("planner lost scoped issue ownership")
	}
	verifier := read(t, PromptPathFor(out, "verifier"))
	for _, reviewer := range []string{"reviewer-code", "reviewer-arch-test", "reviewer-issue-completeness", "reviewer-red-team"} {
		if !strings.Contains(verifier, reviewer) {
			t.Errorf("nested verifier lost %s", reviewer)
		}
	}
}

func TestCodexBuildForRetainsAllSourceAndTransportBytes(t *testing.T) {
	brain, b, out := codexBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	before := read(t, PromptPathFor(out, "planner"))
	put(t, filepath.Join(brain, "AGENTS.md"), "changed model table")
	put(t, filepath.Join(brain, "factory", b.Components["planner"].Instructions), "changed procedure")
	put(t, filepath.Join(brain, "factory", blueprint.RulesFile), "changed rules")
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if read(t, PromptPathFor(out, "planner")) != before {
		t.Fatal("Codex prompt changed after live edits")
	}
	var snapshot sourceSnapshot
	if err := json.Unmarshal([]byte(read(t, filepath.Join(out, "sources.json"))), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Executor != "codex" || snapshot.PromptProtocol != CodexPromptProtocol || snapshot.Bridge != codexBridge || snapshot.HelperTransport != codexHelperTransport || snapshot.SessionTransport != codexSessionTransport {
		t.Fatalf("Codex transport bytes missing: %+v", snapshot)
	}
	if _, _, err := BuildFor(brain, out, b, map[string]string{"planner": "low"}); err == nil || !strings.Contains(err.Error(), "immutable native route") {
		t.Fatalf("override changed pinned route: %v", err)
	}
}

func TestCodexBuildForRejectsForeignOrIncompleteTransport(t *testing.T) {
	for _, mutate := range []func(*sourceSnapshot){
		func(s *sourceSnapshot) { s.Executor = "z" },
		func(s *sourceSnapshot) { s.PromptProtocol = 1 },
		func(s *sourceSnapshot) { s.PromptProtocol = 2 },
		func(s *sourceSnapshot) { s.PromptProtocol = CodexPromptProtocol + 1 },
		func(s *sourceSnapshot) { s.SessionTransport = "" },
		func(s *sourceSnapshot) { s.HelperTransport = "" },
	} {
		brain, b, out := codexBrain(t)
		if _, _, err := BuildFor(brain, out, b, nil); err != nil {
			t.Fatal(err)
		}
		var snapshot sourceSnapshot
		if err := json.Unmarshal([]byte(read(t, filepath.Join(out, "sources.json"))), &snapshot); err != nil {
			t.Fatal(err)
		}
		mutate(&snapshot)
		data, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "sources.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := BuildFor(brain, out, b, nil); err == nil || !strings.Contains(err.Error(), "Codex prompt transport") {
			t.Fatalf("foreign transport accepted: %v", err)
		}
	}
}

func TestCodexCoordinatorKnowledgeUsesCoordinatorAuthority(t *testing.T) {
	got := codexControlText("Read ~/.agents/repos/factory.md, then factory issue create.")
	if !strings.Contains(got, `factory_control(op="brain_read")`) || strings.Contains(got, `factory_helper(op="brain_read")`) || !strings.Contains(got, "factory_control operation issue_create") {
		t.Fatalf("coordinator knowledge/issue authority changed: %s", got)
	}
}

func TestCodexManagerRecoveryTextUsesAvailableControls(t *testing.T) {
	original := "Read /tmp/owned/checkpoint.v2.md, then `factory inbox owner`; read the tail of your events.jsonl."
	got := SessionText("codex", original)
	for _, bad := range []string{"factory inbox", "Read /tmp", "tail of your events.jsonl", "factory_helper"} {
		if strings.Contains(got, bad) {
			t.Fatalf("manager message retains %q: %s", bad, got)
		}
	}
	if !strings.Contains(got, `payload={path:"checkpoint.v2.md"}`) || !strings.Contains(got, `factory_control(op="events")`) {
		t.Fatal(got)
	}
	if SessionText("claude", original) != original || SessionText("z", original) != original {
		t.Fatal("legacy manager prompt changed")
	}
}

func TestCodexRecoveryContractsReachPinnedConversations(t *testing.T) {
	brain, b, out := codexBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	sources := filepath.Join(out, "sources.json")
	before := read(t, sources)
	for _, text := range []string{SessionText("codex", "Resume from your newest event."), read(t, PromptPathFor(out, "orchestrator")), read(t, PromptPathFor(out, "implementor"))} {
		for _, required := range []string{"reply-only reviewers", "omitted output_path", "path:filename", "descendants_stopped=true", "publication_settled=true", "fresh request key"} {
			if !strings.Contains(text, required) {
				t.Fatalf("missing current contract %q", required)
			}
		}
	}
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if read(t, sources) != before {
		t.Fatal("runtime correction rewrote pinned sources")
	}
	for _, provider := range []string{"claude", "z"} {
		if SessionText(provider, "resume") != "resume" {
			t.Fatal("legacy manager message changed")
		}
	}
}

func TestCodexScopeFactsCorrectionReachesExistingConversations(t *testing.T) {
	text := SessionText("codex", "Resume your current work.")
	for _, required := range []string{"stored_scope", "scope_matches_current", "even for a completed helper", "fresh job/key", "current broker authority", "null is unknown", "true grants no permission"} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing scope recovery instruction %q", required)
		}
	}
	for _, provider := range []string{"claude", "z"} {
		if SessionText(provider, "Resume your current work.") != "Resume your current work." {
			t.Fatal("legacy recovery instructions changed")
		}
	}
}

func TestCodexWorkflowCorrectionsExplainCurrentRoundAndBusyHold(t *testing.T) {
	text := SessionText("codex", "Resume work.")
	for _, required := range []string{"CURRENT stored helper round", "not the", "next round", "confirmed completion", "busy refusal", "no file/lifecycle action", "hold for terminal stopped/descendant/publication proof", "Never repeat"} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing workflow correction %q", required)
		}
	}
}

func TestCodexControlRecoveryGuidanceReachesImmutableConversations(t *testing.T) {
	brain, b, out := codexBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	sources := filepath.Join(out, "sources.json")
	before := read(t, sources)
	for _, text := range []string{SessionText("codex", "Resume."), read(t, PromptPathFor(out, "orchestrator")), read(t, PromptPathFor(out, "implementor"))} {
		text = strings.Join(strings.Fields(text), " ")
		for _, term := range []string{"memory/tone_guide.md", "RESPONSE read_only:true", "metadata and recovery facts only", "does not grant Continue/Cancel", "program-owned phase transition", "query{repo}", "match the work id", "fresh truthful", "even when no code changes", "exact"} {
			if !strings.Contains(text, term) {
				t.Fatalf("missing recovery instruction %q", term)
			}
		}
	}
	if read(t, sources) != before {
		t.Fatal("runtime guidance changed immutable sources")
	}
	for _, provider := range []string{"claude", "z"} {
		if SessionText(provider, "Resume.") != "Resume." {
			t.Fatal("legacy message changed")
		}
	}
}

func TestCodexRuntimeReadGuidanceSeparatesInputAndResponse(t *testing.T) {
	text := strings.Join(strings.Fields(SessionText("codex", "Resume.")), " ")
	for _, required := range []string{"INPUT is exactly {job_id,round}", "omit read_only from the request", "RESPONSE read_only:true", "github_base requires the registered work base", "branch/ref in sha", "failed read is unknown evidence", "no additional approval is needed"} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing current read contract %q", required)
		}
	}
	if strings.Contains(text, "helper_result: read_only=true") {
		t.Fatal("runtime still presents response field as input")
	}
	for _, provider := range []string{"claude", "z"} {
		if SessionText(provider, "Resume.") != "Resume." {
			t.Fatal("legacy prompt changed")
		}
	}
}
