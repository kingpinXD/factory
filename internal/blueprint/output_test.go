package blueprint

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCheckOutput(t *testing.T) {
	planner := fixture(t).Components["planner"]
	cases := []struct {
		name    string
		file    string
		content string
		missing []string
	}{
		{
			name:    "markdown with every heading",
			file:    "state-check.v1.md",
			content: "# State check\n\n## Input\n\nThe epic.\n\n## Issues  \n\n### kingpinXD/factory#12\n",
		},
		{
			name:    "markdown missing a heading",
			file:    "state-check.v2.md",
			content: "## Input\n\nThe epic.\n\n## Issue\n\n### Issues\n",
			missing: []string{"## Issues"},
		},
		{
			name:    "heading text only in a paragraph",
			file:    "state-check.v2.md",
			content: "## Input\n\nSee ## Issues below.\n",
			missing: []string{"## Issues"},
		},
		{
			name:    "yaml with every top-level key",
			file:    "epic.v3.yaml",
			content: "uat: none\nissues: []\nitems:\n  - {id: a}\n",
		},
		{
			name:    "yaml with a key only nested",
			file:    "epic.v3.yaml",
			content: "issues: []\nsets:\n  - {id: s1, items: [a]}\n",
			missing: []string{"items"},
		},
		{
			name:    "empty yaml",
			file:    "epic.v1.yaml",
			missing: []string{"issues", "items"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), c.file)
			put(t, path, c.content)
			missing, err := planner.CheckOutput(path)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(missing, c.missing) {
				t.Errorf("missing = %q, want %q", missing, c.missing)
			}
		})
	}
}

func TestCheckOutputUnversionedName(t *testing.T) {
	uat := fixture(t).Components["uat-runner"]
	for _, name := range []string{"result.md", "result.v2.md"} {
		path := filepath.Join(t.TempDir(), name)
		put(t, path, "## Result\npass\n")
		if missing, err := uat.CheckOutput(path); err != nil || missing != nil {
			t.Errorf("%s: missing = %q, err = %v; want neither", name, missing, err)
		}
	}
}

func TestCheckOutputErrors(t *testing.T) {
	b := fixture(t)
	withReviewer(b)
	dir := t.TempDir()
	put(t, filepath.Join(dir, "plan.md"), "## TODOs\n")
	put(t, filepath.Join(dir, "epic.v1.yaml"), "- not a mapping\n")
	cases := []struct {
		name      string
		component string
		file      string
		want      string
	}{
		{"file is not an output", "planner", "plan.md", "plan.md is not one of the component's outputs: state-check.v<n>.md, epic.v<n>.yaml"},
		{"component replies", "reviewer", "plan.md", "the component writes no output file"},
		{"file does not exist", "planner", "state-check.v1.md", "no such file or directory"},
		{"yaml is not a mapping", "planner", "epic.v1.yaml", "cannot unmarshal !!seq into map[string]interface {}"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := b.Components[c.component].CheckOutput(filepath.Join(dir, c.file))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to contain %q", err, c.want)
			}
		})
	}
}
