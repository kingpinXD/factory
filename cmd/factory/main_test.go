package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var plannedCommands = []string{
	"add", "tick", "status", "answer", "retry", "stop", "replan", "check",
	"blueprint", "event", "heartbeat", "inbox", "query", "may-merge",
	"deployed", "issue", "agents", "launchd", "lease-ok", "repo-worker",
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
	if want := "ok: " + filepath.Join(brain, "factory", "blueprint.yaml") + "\n"; stdout.String() != want {
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
