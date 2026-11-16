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
	s, err := Prepare(brain, id, b, Options{Executor: "codex", ProfilePath: path})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPrepareImmutableSnapshotAndBinaries(t *testing.T) {
	brain, b, _, path := codexFixture(t)
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
	if err := os.WriteFile(s.Profile.CodexBinary, []byte("changed executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CheckBinaries(s); err == nil || !strings.Contains(err.Error(), "fingerprint changed") {
		t.Fatalf("binary change: %v", err)
	}
}

func TestStrictProfile(t *testing.T) {
	_, _, p, path := codexFixture(t)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"unknown":             string(good) + "unknown: true\n",
		"duplicate":           string(good) + "executor: codex\n",
		"multiple documents":  string(good) + "---\nexecutor: codex\n",
		"duplicate component": strings.Replace(string(good), "routes:\n", "routes:\n    planner: [{provider: codex, tier: medium, effort: medium}]\n", 1),
		"unknown candidate":   strings.Replace(string(good), "provider: codex", "provider: codex\n          unknown: true", 1),
		"scalar candidate":    strings.Replace(string(good), "- provider: codex", "provider: codex", 1),
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
		"executor":    func(p *Profile) { p.Executor = "z" },
		"concurrency": func(p *Profile) { p.MaxActiveHelpers = 2 },
		"alternative": func(p *Profile) {
			p.Routes["implementor"] = append(p.Routes["implementor"], p.Routes["implementor"][0])
		},
		"helper Claude":   func(p *Profile) { p.Routes["implementor"] = []Candidate{{Provider: "claude", Tier: "medium"}} },
		"implicit effort": func(p *Profile) { p.Routes["implementor"] = []Candidate{{Provider: "codex", Tier: "medium"}} },
		"top tier":        func(p *Profile) { p.Routes["planner"] = []Candidate{{Provider: "claude", Tier: "medium"}} },
		"missing top":     func(p *Profile) { delete(p.Routes, "babysitter") },
		"floating image":  func(p *Profile) { p.ContainerImage = "image:latest" },
		"relative binary": func(p *Profile) { p.CodexBinary = "codex" },
		"version":         func(p *Profile) { p.CodexVersion = "" },
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
			p.Routes["typo"] = []Candidate{{Provider: "codex", Tier: "medium", Effort: "medium"}}
		},
		"denied":         func(_ string, b *blueprint.Blueprint, _ *Profile) { b.Deny.Models = []string{"gpt-6.1"} },
		"binary missing": func(_ string, _ *blueprint.Blueprint, p *Profile) { p.CodexBinary += "-missing" },
		"binary not executable": func(_ string, _ *blueprint.Blueprint, p *Profile) {
			if err := os.Chmod(p.CodexBinary, 0o600); err != nil {
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
			brain, b, p, path := codexFixture(t)
			mutate(brain, b, &p)
			saveProfile(t, path, p)
			if _, err := Prepare(brain, "e-one", b, Options{Executor: "codex", ProfilePath: path}); err == nil {
				t.Fatal("accepted invalid intake")
			}
		})
	}
}

func TestSelectionCorruptionNeverLegacy(t *testing.T) {
	brain, b, _, path := codexFixture(t)
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
		"model mismatch": strings.Replace(string(good), CodexModel, "other-model", 1),
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
	brain, b, p, path := codexFixture(t)
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
		"disabled MCP": func(p *Profile) { p.CodexDisabledMCP = []string{"different"} },
		"identity":     func(p *Profile) { p.IdentityBinding = "other-grant" },
		"binary":       func(p *Profile) { p.CodexExecServer = p.CodexBinary },
		"effort": func(p *Profile) {
			p.Routes = maps.Clone(p.Routes)
			p.Routes["implementor"] = []Candidate{{Provider: "codex", Tier: "medium", Effort: "high"}}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			copy := p
			mutate(&copy)
			saveProfile(t, path, copy)
			if _, err := Prepare(brain, "e-two", b, Options{Executor: "codex", ProfilePath: path}); err == nil || !strings.Contains(err.Error(), "conflicts with active epic") {
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
	brain, b, _, path := codexFixture(t)
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		c := b.Components[name]
		c.Tier = "low"
		b.Components[name] = c
	}
	s := prepare(t, brain, b, path, "e-one")
	if s.Routes["planner"][0].Tier != "medium" {
		t.Fatal("profile did not take precedence")
	}
}

func TestActiveStateErrorsFailClosed(t *testing.T) {
	brain, b, _, path := codexFixture(t)
	indexPath := filepath.Join(brain, "factory", "index.json")
	for _, data := range []string{"{", "null", `{"e-one":{"kind":"epic","dir":"relative"}}`} {
		if err := os.WriteFile(indexPath, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Prepare(brain, "e-two", b, Options{Executor: "codex", ProfilePath: path}); err == nil {
			t.Fatalf("accepted bad active state: %s", data)
		}
	}
}

func TestSavedUnsupportedExecutorDoesNotBlockCodexIntake(t *testing.T) {
	brain, b, _, profile := codexFixture(t)
	oldPath := selectionPath(brain, "e-old-z")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o700); err != nil {
		t.Fatal(err)
	}
	old := `{"schema_version":1,"executor":"z","epic_id":"e-old-z","profile":{"schema_version":1,"executor":"z","native_worker":"/old/worker","native_usage":"/old/usage","credential_namespace":"old-z"},"created_at":"2026-10-16T00:00:00Z"}`
	if err := os.WriteFile(oldPath, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSelection(brain, "e-old-z"); !errors.Is(err, ErrUnsupportedExecutor) || !strings.Contains(err.Error(), `"z"`) || !strings.Contains(err.Error(), "e-old-z") {
		t.Fatalf("old epic did not report its unsupported executor: %v", err)
	}
	index, err := json.Marshal(map[string]map[string]string{"e-old-z": {"kind": "epic", "dir": filepath.Join(brain, "old-epic"), "epic": "e-old-z"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "factory", "index.json"), index, 0o600); err != nil {
		t.Fatal(err)
	}
	selection := prepare(t, brain, b, profile, "e-new-codex")
	if err := WriteSelection(brain, selection); err != nil {
		t.Fatalf("old unsupported epic blocked Codex publication: %v", err)
	}
	if got, err := ReadSelection(brain, "e-new-codex"); err != nil || got.Executor != "codex" {
		t.Fatalf("new Codex selection: %+v %v", got, err)
	}
	if _, err := Prepare(brain, "e-new-z", b, Options{Executor: "z", ProfilePath: profile}); !errors.Is(err, ErrUnsupportedExecutor) {
		t.Fatalf("new Z executor was accepted: %v", err)
	}
	if err := os.WriteFile(oldPath, []byte(`{"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(brain, "e-other-codex", b, Options{Executor: "codex", ProfilePath: profile}); err == nil || errors.Is(err, ErrUnsupportedExecutor) {
		t.Fatalf("corrupt indexed selection was ignored: %v", err)
	}
}
