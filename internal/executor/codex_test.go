package executor

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
)

func codexFixture(t *testing.T) (string, *blueprint.Blueprint, Profile, string) {
	t.Helper()
	brain := t.TempDir()
	if err := os.MkdirAll(filepath.Join(brain, "factory"), 0o700); err != nil {
		t.Fatal(err)
	}
	b := &blueprint.Blueprint{Components: map[string]blueprint.Component{}}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		b.Components[name] = blueprint.Component{RunsAs: blueprint.RunsAsSession, Tier: "high", Instructions: "components/" + name + ".md"}
	}
	b.Components["implementor"] = blueprint.Component{RunsAs: blueprint.RunsAsHelper, Tier: "medium", Instructions: "components/implementor.md"}
	profilePath := filepath.Join(brain, "profile.yaml")
	if err := os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("## Model tiers\n\n| Tier | Claude | Codex |\n| --- | --- | --- |\n| medium | opus | gpt-6.1-sol |\n| low | sonnet | gpt-6.1-sol |\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, c := range b.Components {
		c.Effort = "medium"
		b.Components[name] = c
	}
	p := DefaultCodexProfile(b)
	p.CodexBinary = filepath.Join(brain, "codex")
	p.CodexExecServer = filepath.Join(brain, "codex-exec-server")
	p.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	p.CodexModelCatalog = filepath.Join(brain, "models.json")
	catalog := []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
	if err := os.WriteFile(p.CodexModelCatalog, catalog, 0600); err != nil {
		t.Fatal(err)
	}
	p.CodexModelCatalogSHA256 = fmt.Sprintf("%x", sha256.Sum256(catalog))
	p.CodexVersion = "0.162.1"
	p.IdentityBinding = "test-subscription-account"
	p.ModelEvidencePolicy = CodexModelEvidencePolicy
	if err := os.WriteFile(p.CodexBinary, []byte("#!/bin/sh\nif [ \"$1\" = 'debug' ]; then printf '{\"models\":[{\"slug\":\"gpt-6.1-sol\",\"tool_mode\":\"code_mode_only\"}]}'; exit; fi\nif [ \"$1\" != '--version' ]; then exit 98; fi\nprintf 'codex-cli 0.162.1\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.CodexExecServer, []byte("Linux binary fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	saveProfile(t, profilePath, p)
	old := inspectImage
	inspectImage = func(image string) (string, error) {
		if image != p.ContainerImage {
			return "", errors.New("wrong image")
		}
		return p.ContainerImage, nil
	}
	t.Cleanup(func() { inspectImage = old })
	return brain, b, p, profilePath
}

func TestCodexPrepareSeparateImmutableSelection(t *testing.T) {
	brain, b, p, profilePath := codexFixture(t)
	s, err := Prepare(brain, "e-codex", b, Options{Executor: "codex", ProfilePath: profilePath})
	if err != nil {
		t.Fatal(err)
	}
	if s.Executor != "codex" || len(s.Binaries) != 2 || s.Binaries["codex"].Path != p.CodexBinary || s.ContainerImageID != p.ContainerImage {
		t.Fatalf("wrong Codex selection: %+v", s)
	}
	for name, routes := range s.Routes {
		if len(routes) != 1 || routes[0].Provider != "codex" || routes[0].Model != CodexModel || routes[0].Effort != "medium" {
			t.Fatalf("route %s: %+v", name, routes)
		}
	}
	if err := WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
	p.CodexVersion = "0.999.0"
	saveProfile(t, profilePath, p)
	if err := os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("changed live table"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSelection(brain, s.EpicID)
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("immutable Codex selection changed: %+v %v", got, err)
	}
	if err := CheckBinaries(s); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.CodexBinary, []byte("changed executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CheckBinaries(s); err == nil || !strings.Contains(err.Error(), "fingerprint changed") {
		t.Fatalf("changed Codex executable was accepted: %v", err)
	}
}

func TestCodexIntakeRejectsUnpinnedAndForeignSettings(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Profile)
		want   string
	}{
		"relative binary":      {func(p *Profile) { p.CodexBinary = "codex" }, "codex_binary"},
		"floating version":     {func(p *Profile) { p.CodexVersion = "latest" }, "codex_version"},
		"wrong version":        {func(p *Profile) { p.CodexVersion = "0.999.0" }, "version differs"},
		"missing identity":     {func(p *Profile) { p.IdentityBinding = "" }, "identity_binding"},
		"implicit evidence":    {func(p *Profile) { p.ModelEvidencePolicy = "" }, "model_evidence_policy"},
		"pretend attestation":  {func(p *Profile) { p.ModelEvidencePolicy = "observed-model" }, "model_evidence_policy"},
		"Z worker":             {func(p *Profile) { p.NativeWorker = "/a/zed-worker" }, "cannot contain Z"},
		"Z identity":           {func(p *Profile) { p.CredentialNamespace = "factory-z" }, "cannot contain Z"},
		"floating image":       {func(p *Profile) { p.ContainerImage = "image:latest" }, "pinned by sha256"},
		"relative exec-server": {func(p *Profile) { p.CodexExecServer = "codex-exec-server" }, "codex_exec_server"},
		"Claude coordinator": {func(p *Profile) {
			p.Routes["planner"] = []Candidate{{Provider: "claude", Tier: "medium", Effort: "high"}}
		}, "unsupported route planner"},
		"Z helper": {func(p *Profile) {
			p.Routes["implementor"] = []Candidate{{Provider: "zed", Tier: "medium", Effort: "high"}}
		}, "unsupported route implementor"},
		"unavailable tier": {func(p *Profile) { p.Routes["planner"] = []Candidate{{Provider: "codex", Tier: "high", Effort: "high"}} }, "unsupported route planner"},
		"implicit effort":  {func(p *Profile) { p.Routes["planner"] = []Candidate{{Provider: "codex", Tier: "medium"}} }, "explicit valid effort"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			brain, b, p, profilePath := codexFixture(t)
			tc.mutate(&p)
			saveProfile(t, profilePath, p)
			_, err := Prepare(brain, "e-codex", b, Options{Executor: "codex", ProfilePath: profilePath})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
			if _, err := os.Stat(selectionPath(brain, "e-codex")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid intake published a selection: %v", err)
			}
		})
	}
}

func TestCodexResolveUsesOnlySharedCodexColumn(t *testing.T) {
	_, _, p, _ := codexFixture(t)
	tiers := blueprint.Tiers{"medium": {"Claude": ClaudeModel, "Codex": CodexModel}, "low": {"Codex": CodexModel}}
	for _, mutation := range []string{"missing", "different", "denied"} {
		t.Run(mutation, func(t *testing.T) {
			copy := maps.Clone(tiers)
			copy["medium"] = maps.Clone(tiers["medium"])
			var deny []string
			switch mutation {
			case "missing":
				delete(copy["medium"], "Codex")
			case "different":
				copy["medium"]["Codex"] = "unapproved-model"
			case "denied":
				deny = []string{CodexModel}
			}
			if _, err := Resolve(p, copy, deny); err == nil {
				t.Fatal("accepted missing, different or denied Codex model")
			}
		})
	}
}

func TestExistingSelectionJSONHasNoCodexFields(t *testing.T) {
	for _, s := range []Selection{{SchemaVersion: 1, Executor: "claude", EpicID: "e-claude"}} {
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"codex_binary", "codex_exec_server", "codex_disabled_mcp", "codex_version", "model_evidence_policy"} {
			if strings.Contains(string(raw), key) {
				t.Fatalf("existing %s JSON acquired %s", s.Executor, key)
			}
		}
	}
}
