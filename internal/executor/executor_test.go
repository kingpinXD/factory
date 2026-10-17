package executor

import (
	"encoding/json"
	"errors"
	"github.com/kingpinXD/factory/internal/blueprint"
	"go.yaml.in/yaml/v3"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestResolvePinnedRoutes(t *testing.T) {
	p := Profile{SchemaVersion: 1, Executor: "z", MaxActiveHelpers: 1, Routes: map[string][]Candidate{
		"planner":      {{Provider: "claude", Tier: "high"}},
		"orchestrator": {{Provider: "claude", Tier: "high"}},
		"babysitter":   {{Provider: "claude", Tier: "high"}},
		"implementor":  {{Provider: "zed", Tier: "medium"}},
	}}
	tiers := blueprint.Tiers{"high": {"Claude": "claude-opus-5-5"}, "medium": {"Zed": "openai-subscribed/gpt-6.1-sol"}}
	got, err := Resolve(p, tiers, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got["implementor"][0].Provider != "zed" || got["planner"][0].Model != "claude-opus-5-5" {
		t.Fatal(got)
	}
	for _, model := range []string{"opus", "claude-opus-4-6", ""} {
		tiers["high"]["Claude"] = model
		if _, err := Resolve(p, tiers, nil); err == nil {
			t.Fatalf("accepted %q", model)
		}
	}
}

func fixture(t *testing.T) (string, *blueprint.Blueprint, Profile, string) {
	t.Helper()
	brain := t.TempDir()
	if err := os.MkdirAll(filepath.Join(brain, "factory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("## Model tiers\n\n| Tier | Claude | Zed |\n| --- | --- | --- |\n| high | claude-opus-5-5 | |\n| medium | opus | openai-subscribed/gpt-6.1-sol |\n| low | sonnet | |\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := &blueprint.Blueprint{Components: map[string]blueprint.Component{}}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		b.Components[name] = blueprint.Component{RunsAs: blueprint.RunsAsSession, Tier: "high", Instructions: "components/" + name + ".md"}
	}
	b.Components["implementor"] = blueprint.Component{RunsAs: blueprint.RunsAsHelper, Tier: "medium", Instructions: "components/implementor.md"}
	p := DefaultProfile(b)
	p.NativeWorker = filepath.Join(brain, "worker")
	p.NativeUsage = filepath.Join(brain, "usage")
	for _, name := range []string{p.NativeWorker, p.NativeUsage} {
		if err := os.WriteFile(name, []byte("#!/bin/sh\nexit 99\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	p.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	p.CredentialNamespace = "test-factory"
	p.IdentityBinding = "test-grant"
	path := filepath.Join(brain, "profile.yaml")
	saveProfile(t, path, p)
	old := inspectImage
	inspectImage = func(image string) (string, error) {
		if image != p.ContainerImage {
			return "", errors.New("wrong image")
		}
		return p.ContainerImage, nil
	}
	t.Cleanup(func() { inspectImage = old })
	return brain, b, p, path
}
func saveProfile(t *testing.T, path string, p Profile) {
	t.Helper()
	data, err := yaml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
func prepare(t *testing.T, brain string, b *blueprint.Blueprint, path, id string) Selection {
	t.Helper()
	s, err := Prepare(brain, id, b, Options{Executor: "z", ProfilePath: path})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPrepareImmutableSnapshotAndBinaries(t *testing.T) {
	brain, b, _, path := fixture(t)
	s := prepare(t, brain, b, path, "e-one")
	for _, name := range []string{"factory/index.json", "factory/executors/e-one/selection.json"} {
		if _, err := os.Stat(filepath.Join(brain, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Prepare published %s", name)
		}
	}
	if err := WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
	if err := WriteSelection(brain, s); err != nil {
		t.Fatal("idempotent", err)
	}
	// A live table edit cannot alter a published route.
	if err := os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("broken live table"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSelection(brain, s.EpicID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("snapshot changed: %+v", got)
	}
	changed := s
	changed.CreatedAt = s.CreatedAt.Add(time.Second)
	if err := WriteSelection(brain, changed); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("changed selection: %v", err)
	}
	if err := CheckBinaries(s); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.Profile.NativeWorker, []byte("changed executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CheckBinaries(s); err == nil || !strings.Contains(err.Error(), "fingerprint changed") {
		t.Fatalf("binary change: %v", err)
	}
}

func TestStrictProfile(t *testing.T) {
	_, _, p, path := fixture(t)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"unknown":             string(good) + "unknown: true\n",
		"duplicate":           string(good) + "executor: z\n",
		"multiple documents":  string(good) + "---\nexecutor: z\n",
		"duplicate component": strings.Replace(string(good), "routes:\n", "routes:\n    planner: [{provider: claude, tier: high}]\n", 1),
		"unknown candidate":   strings.Replace(string(good), "provider: zed", "provider: zed\n          unknown: true", 1),
		"scalar candidate":    strings.Replace(string(good), "- provider: zed", "provider: zed", 1),
		"empty":               "",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadProfile(path); err == nil {
				t.Fatal("accepted malformed profile")
			}
		})
	}
	mutations := map[string]func(*Profile){
		"schema":      func(p *Profile) { p.SchemaVersion = 2 },
		"executor":    func(p *Profile) { p.Executor = "codex" },
		"concurrency": func(p *Profile) { p.MaxActiveHelpers = 2 },
		"alternative": func(p *Profile) {
			p.Routes["implementor"] = append(p.Routes["implementor"], p.Routes["implementor"][0])
		},
		"helper Claude":   func(p *Profile) { p.Routes["implementor"] = []Candidate{{Provider: "claude", Tier: "medium"}} },
		"helper Codex":    func(p *Profile) { p.Routes["implementor"] = []Candidate{{Provider: "codex", Tier: "medium"}} },
		"top tier":        func(p *Profile) { p.Routes["planner"] = []Candidate{{Provider: "claude", Tier: "medium"}} },
		"missing top":     func(p *Profile) { delete(p.Routes, "babysitter") },
		"floating image":  func(p *Profile) { p.ContainerImage = "image:latest" },
		"relative worker": func(p *Profile) { p.NativeWorker = "worker" },
		"namespace":       func(p *Profile) { p.CredentialNamespace = "" },
		"identity":        func(p *Profile) { p.IdentityBinding = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			copy := p
			copy.Routes = maps.Clone(p.Routes)
			mutate(&copy)
			saveProfile(t, path, copy)
			if _, err := LoadProfile(path); err == nil {
				t.Fatal("accepted invalid profile")
			}
		})
	}
}

func TestPrepareValidation(t *testing.T) {
	cases := map[string]func(string, *blueprint.Blueprint, *Profile){
		"missing helper": func(_ string, _ *blueprint.Blueprint, p *Profile) { delete(p.Routes, "implementor") },
		"unknown component": func(_ string, _ *blueprint.Blueprint, p *Profile) {
			p.Routes["typo"] = []Candidate{{Provider: "zed", Tier: "medium"}}
		},
		"denied":         func(_ string, b *blueprint.Blueprint, _ *Profile) { b.Deny.Models = []string{"gpt-6.1"} },
		"binary missing": func(_ string, _ *blueprint.Blueprint, p *Profile) { p.NativeWorker += "-missing" },
		"binary not executable": func(_ string, _ *blueprint.Blueprint, p *Profile) {
			if err := os.Chmod(p.NativeWorker, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"missing model": func(brain string, _ *blueprint.Blueprint, _ *Profile) {
			if err := os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("## Model tiers\n| Tier | Claude |\n| --- | --- |\n| high | claude-opus-5-5 |\n| medium | opus |\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			brain, b, p, path := fixture(t)
			mutate(brain, b, &p)
			saveProfile(t, path, p)
			if _, err := Prepare(brain, "e-one", b, Options{Executor: "z", ProfilePath: path}); err == nil {
				t.Fatal("accepted invalid intake")
			}
		})
	}
}

func TestSelectionCorruptionNeverLegacy(t *testing.T) {
	brain, b, _, path := fixture(t)
	s := prepare(t, brain, b, path, "e-one")
	if _, err := ReadSelection(brain, s.EpicID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if err := WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(selectionPath(brain, s.EpicID))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"truncated": "{", "null": "null", "unknown": strings.Replace(string(good), `"schema_version": 1`, `"unknown": 1, "schema_version": 1`, 1),
		"duplicate":      strings.Replace(string(good), `"schema_version": 1`, `"schema_version": 1, "schema_version": 1`, 1),
		"future":         strings.Replace(string(good), `"schema_version": 1`, `"schema_version": 2`, 1),
		"wrong epic":     strings.Replace(string(good), `"epic_id": "e-one"`, `"epic_id": "e-other"`, 1),
		"model mismatch": strings.Replace(string(good), NativeModel, "other-model", 1),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(selectionPath(brain, s.EpicID), []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadSelection(brain, s.EpicID); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corruption became missing/legacy: %v", err)
			}
		})
	}
}

func TestActiveProfileCompatibility(t *testing.T) {
	brain, b, p, path := fixture(t)
	s := prepare(t, brain, b, path, "e-one")
	if err := WriteSelection(brain, s); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(brain, "epic-one")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	index := map[string]map[string]string{"e-one": {"kind": "epic", "dir": dir, "epic": "e-one"}}
	data, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "factory", "index.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = prepare(t, brain, b, path, "e-two")
	mutations := map[string]func(*Profile){
		"namespace": func(p *Profile) { p.CredentialNamespace = "other-namespace" },
		"identity":  func(p *Profile) { p.IdentityBinding = "other-grant" },
		"binary":    func(p *Profile) { p.NativeWorker = p.NativeUsage },
		"effort": func(p *Profile) {
			p.Routes = maps.Clone(p.Routes)
			p.Routes["implementor"] = []Candidate{{Provider: "zed", Tier: "medium", Effort: "high"}}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			copy := p
			mutate(&copy)
			saveProfile(t, path, copy)
			if _, err := Prepare(brain, "e-two", b, Options{Executor: "z", ProfilePath: path}); err == nil || !strings.Contains(err.Error(), "conflicts with active epic") {
				t.Fatalf("active mismatch: %v", err)
			}
		})
	}
	// Orphans are not active. A genuinely ended indexed epic releases compatibility.
	if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte(`{"end":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = prepare(t, brain, b, path, "e-two")
}

func TestLegacyPrepareUnchanged(t *testing.T) {
	for _, name := range []string{"", "claude"} {
		s, err := Prepare(t.TempDir(), "e-legacy", nil, Options{Executor: name})
		if err != nil || s.Executor != "claude" {
			t.Fatalf("%+v %v", s, err)
		}
	}
	if _, err := Prepare(t.TempDir(), "../escape", nil, Options{}); err == nil {
		t.Fatal("invalid ID")
	}
}

func TestPrepareProfileOverridesBlueprintDefaults(t *testing.T) {
	brain, b, _, path := fixture(t)
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		c := b.Components[name]
		c.Tier = "medium"
		b.Components[name] = c
	}
	s := prepare(t, brain, b, path, "e-one")
	if s.Routes["planner"][0].Model != ClaudeModel {
		t.Fatal("profile did not take precedence")
	}
}

func TestActiveStateErrorsFailClosed(t *testing.T) {
	brain, b, _, path := fixture(t)
	indexPath := filepath.Join(brain, "factory", "index.json")
	for _, data := range []string{"{", "null", `{"e-one":{"kind":"epic","dir":"relative"}}`} {
		if err := os.WriteFile(indexPath, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Prepare(brain, "e-two", b, Options{Executor: "z", ProfilePath: path}); err == nil {
			t.Fatalf("accepted bad active state: %s", data)
		}
	}
}
