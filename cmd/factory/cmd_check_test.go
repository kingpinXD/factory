package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCheckOutput(t *testing.T) {
	testBrain(t)
	dir := t.TempDir()
	cases := []struct {
		name      string
		component string
		file      string
		content   string
		code      int
		stdout    string
		stderr    string
	}{
		{
			name: "every heading", component: "planner", file: "state-check.v1.md",
			content: "## Input\n\n## Issues\n",
			code:    0, stdout: "ok: <dir>/state-check.v1.md\n",
		},
		{
			name: "a missing heading", component: "planner", file: "state-check.v2.md",
			content: "## Input\n\n## Issue\n",
			code:    1, stdout: "missing: ## Issues\n", stderr: "state-check.v2.md lacks 1 of planner's headings",
		},
		{
			name: "missing top-level keys", component: "planner", file: "epic.v1.yaml",
			content: "uat: none\nsets: []\n",
			code:    1, stdout: "missing: issues\nmissing: items\n", stderr: "epic.v1.yaml lacks 2 of planner's headings",
		},
		{
			name: "unknown component", component: "nope", file: "state-check.v1.md",
			content: "## Input\n",
			code:    1, stderr: `the blueprint has no component "nope"`,
		},
		{
			name: "file that is not an output", component: "planner", file: "plan.md",
			content: "## Input\n",
			code:    1, stderr: "planner: plan.md is not one of the component's outputs: state-check.v<n>.md, epic.v<n>.yaml",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, c.file)
			if err := os.WriteFile(path, []byte(c.content), 0o644); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			if code := run([]string{"check", "--output", c.component, path}, &stdout, &stderr); code != c.code {
				t.Fatalf("exit code = %d, want %d; stdout %q, stderr %q", code, c.code, stdout.String(), stderr.String())
			}
			if want := strings.ReplaceAll(c.stdout, "<dir>", dir); stdout.String() != want {
				t.Errorf("stdout = %q, want %q", stdout.String(), want)
			}
			if !strings.Contains(stderr.String(), c.stderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), c.stderr)
			}
		})
	}
}

func TestRunCheckUsage(t *testing.T) {
	for _, args := range [][]string{{"check", "extra"}, {"check", "--output", "planner"}, {"check", "--out", "planner", "f.md"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Errorf("%q: exit code = %d, want 2", args, code)
		}
		if !strings.Contains(stderr.String(), "usage: factory check [--output <component> <file>]") {
			t.Errorf("%q: stderr = %q, want the usage", args, stderr.String())
		}
	}
}
