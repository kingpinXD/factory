package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

var plannedCommands = []string{
	"add", "tick", "status", "answer", "retry", "stop", "review-asked", "replan", "check",
	"blueprint", "event", "heartbeat", "inbox", "query", "may-merge",
	"deployed", "issue", "agents", "launchd", "lease-ok", "repo-worker",
	"version", "helper", "z-runner", "z-attach", "z-preflight", "ui",
}

func TestRunHelpListsEveryCommand(t *testing.T) {
	cases := map[string][]string{
		"no arguments": nil,
		"-h":           {"-h"},
		"--help":       {"--help"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr); code != 0 {
				t.Fatalf("exit code = %d, want 0", code)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
			listed := map[string]bool{}
			for _, line := range strings.Split(stdout.String(), "\n") {
				listed[strings.TrimSpace(line)] = true
			}
			for _, cmd := range plannedCommands {
				if !listed[cmd] {
					t.Errorf("usage does not list %q:\n%s", cmd, stdout.String())
				}
			}
		})
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"frobnicate"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: frobnicate") {
		t.Errorf("stderr = %q, want it to name the unknown command", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

func TestEveryCommandHasAHandler(t *testing.T) {
	for _, cmd := range plannedCommands {
		if _, ok := handlers[cmd]; !ok {
			t.Errorf("%s has no handler", cmd)
		}
	}
	if len(handlers) != len(plannedCommands) {
		t.Errorf("%d handlers for %d commands", len(handlers), len(plannedCommands))
	}
}

func TestRunVersion(t *testing.T) {
	t.Setenv("FACTORY_BRAIN", t.TempDir())
	old := readBuildInfo
	readBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Main:     debug.Module{Version: "v1.2.3"},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}},
		}, true
	}
	t.Cleanup(func() { readBuildInfo = old })
	for _, name := range []string{"version", "--version"} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run([]string{name}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit code = %d, want 0", code)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
			if got, want := stdout.String(), "factory v1.2.3 abc123\n"; got != want {
				t.Errorf("stdout = %q, want %q", got, want)
			}
		})
	}
}

func TestRunVersionRejectsArguments(t *testing.T) {
	t.Setenv("FACTORY_BRAIN", t.TempDir())
	for _, name := range []string{"version", "--version"} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run([]string{name, "extra"}, &stdout, &stderr); code != 2 {
				t.Fatalf("exit code = %d, want 2", code)
			}
			if !strings.Contains(stderr.String(), "usage: factory version") {
				t.Errorf("stderr = %q, want the usage line", stderr.String())
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

func TestVersionLine(t *testing.T) {
	vcsTime := debug.BuildSetting{Key: "vcs.time", Value: "2026-10-12T00:00:00Z"}
	cases := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{"full", &debug.BuildInfo{Main: debug.Module{Version: "v1.2.3"}, Settings: []debug.BuildSetting{vcsTime, {Key: "vcs.revision", Value: "abc123"}}}, true, "factory v1.2.3 abc123"},
		{"no revision", &debug.BuildInfo{Main: debug.Module{Version: "v1.2.3"}, Settings: []debug.BuildSetting{vcsTime}}, true, "factory v1.2.3 unknown"},
		{"empty revision", &debug.BuildInfo{Main: debug.Module{Version: "v1.2.3"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: ""}}}, true, "factory v1.2.3 unknown"},
		{"devel version", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}}}, true, "factory unknown abc123"},
		{"empty version", &debug.BuildInfo{Main: debug.Module{Version: ""}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}}}, true, "factory unknown abc123"},
		{"no build info", nil, false, "factory unknown unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := versionLine(c.info, c.ok); got != c.want {
				t.Errorf("versionLine = %q, want %q", got, c.want)
			}
		})
	}
}

// testBrain points FACTORY_BRAIN at a brain holding the blueprint package's
// test fixture, the rules, the planner's instructions and a model tiers table.
func testBrain(t *testing.T) string {
	t.Helper()
	brain := t.TempDir()
	fixture, err := os.ReadFile("../../internal/blueprint/testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"factory/blueprint.yaml":        string(fixture),
		"factory/components/_rules.md":  "# Factory rules\n",
		"factory/components/planner.md": "# planner\n",
		"AGENTS.md":                     "## Model tiers\n\n| Tier | Claude |\n| --- | --- |\n| medium | opus |\n",
	}
	for name, content := range files {
		path := filepath.Join(brain, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FACTORY_BRAIN", brain)
	return brain
}

func TestRunCheck(t *testing.T) {
	brain := testBrain(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"check"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	if want := "ok: " + filepath.Join(brain, "factory", "blueprint.yaml") + " (5 machines, 26 states, 26 transitions)\n"; stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunCheckPrintsEachProblem(t *testing.T) {
	brain := testBrain(t)
	if err := os.Remove(filepath.Join(brain, "factory", "components", "planner.md")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("## Model tiers\n\n| Tier | Claude |\n| --- | --- |\n| medium | |\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"check"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	want := "instructions: component \"planner\": instructions components/planner.md do not exist\n" +
		"tier: component \"planner\": tier \"medium\" has no Claude model in AGENTS.md\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if !strings.Contains(stderr.String(), "fails its check (2 problems)") {
		t.Errorf("stderr = %q, want the problem count", stderr.String())
	}
}

func TestRunCheckRefusesABlueprintWithoutRedAfter(t *testing.T) {
	brain := testBrain(t)
	path := filepath.Join(brain, "factory", "blueprint.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	without := strings.Replace(string(data), "  red_after: 15m\n", "", 1)
	if without == string(data) {
		t.Fatal("the fixture has no red_after")
	}
	if err := os.WriteFile(path, []byte(without), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"check"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if want := "repair: values.red_after is 0s; want how long a base stays red before a repair starts\n"; stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunBlueprintJSON(t *testing.T) {
	testBrain(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"blueprint", "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", code, stderr.String())
	}
	var got struct {
		SchemaVersion int `json:"schema_version"`
		Tick          struct {
			Every string `json:"every"`
		} `json:"tick"`
		Machines map[string]json.RawMessage `json:"machines"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got.SchemaVersion != 1 || got.Tick.Every != "1m0s" || len(got.Machines) != 5 {
		t.Errorf("blueprint = %+v, want schema 1, a 1m tick and five machines", got)
	}
}

func TestRunBlueprintNeedsJSONFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"blueprint"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "usage: factory blueprint --json") {
		t.Errorf("stderr = %q, want the usage", stderr.String())
	}
}
