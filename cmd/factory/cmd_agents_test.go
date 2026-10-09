package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// agentsBrain is testBrain with a reviewer helper added to the blueprint.
func agentsBrain(t *testing.T) string {
	t.Helper()
	brain := testBrain(t)
	path := filepath.Join(brain, "factory", "blueprint.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	helper := "components:\n  reviewer: {runs_as: helper, tier: medium, effort: high, tools: [Bash, Read], instructions: components/reviewer.md, output: {path: reply}}\n"
	files := map[string]string{
		"factory/blueprint.yaml":         strings.Replace(string(data), "components:\n", helper, 1),
		"factory/components/reviewer.md": "---\nname: reviewer\ndescription: Reviews one change.\ntools: Bash, Read\n---\n# reviewer\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(brain, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return brain
}

func TestRunAgents(t *testing.T) {
	brain := agentsBrain(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"agents"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	want := "wrote " + filepath.Join(brain, "factory", "agents.json") + ": 1 helpers: reviewer\n" +
		"wrote " + filepath.Join(brain, "factory", ".prompts", "planner.md") + "\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	data, err := os.ReadFile(filepath.Join(brain, "factory", "agents.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]struct {
		Prompt string `json:"prompt"`
		Model  string `json:"model"`
		Effort string `json:"effort"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	r := got["reviewer"]
	if len(got) != 1 || r.Model != "opus" || r.Effort != "high" || r.Prompt != "# reviewer\n\n# Factory rules\n" {
		t.Errorf("agents.json = %+v, want only the reviewer on opus at high effort, its prompt ending with the rules", got)
	}
}

func TestRunAgentsRefusesAFailingBlueprint(t *testing.T) {
	brain := agentsBrain(t)
	if err := os.Remove(filepath.Join(brain, "factory", "components", "_rules.md")); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"agents"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if want := "instructions: components/_rules.md does not exist; every session and helper prompt ends with it\n"; stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if _, err := os.Stat(filepath.Join(brain, "factory", "agents.json")); !os.IsNotExist(err) {
		t.Errorf("agents.json was written for a failing blueprint")
	}
}

func TestRunAgentsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"agents", "extra"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "usage: factory agents") {
		t.Errorf("stderr = %q, want the usage", stderr.String())
	}
}
