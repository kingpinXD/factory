package agents

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
)

const (
	tiers = "## Model tiers\n\n| Tier | Claude |\n| --- | --- |\n| low | sonnet |\n| medium | opus |\n"
	rules = "# Factory rules\n\nCheck `factory inbox <id>`.\n"
)

func put(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// testBrain makes a brain with a tiers table, the rules, two helpers, a
// placeholder helper, a session and a placeholder session, and returns their
// blueprint.
func testBrain(t *testing.T) (string, *blueprint.Blueprint) {
	t.Helper()
	brain := t.TempDir()
	files := map[string]string{
		"AGENTS.md":                      tiers,
		"factory/" + blueprint.RulesFile: rules,
		"factory/components/reviewer.md": "---\nname: reviewer\ndescription: Reviews one change.\ntools: Bash, Read\n---\n\n# reviewer\n\nRead `<worktree>` & report.\n",
		"factory/components/opener.md":   "---\nname: opener\ndescription: Opens the PR.\n---\n# opener\n",
		"factory/components/planner.md":  "# planner\n\nPlans the epic.\n\n",
		"factory/components/uat.md":      "# uat (placeholder)\n",
		"factory/components/later.md":    "---\nname: later\ndescription: Not built yet.\n---\n# later\n",
	}
	for name, content := range files {
		put(t, filepath.Join(brain, name), content)
	}
	b := &blueprint.Blueprint{Components: map[string]blueprint.Component{
		"manager":  {RunsAs: blueprint.RunsAsCode},
		"reviewer": {RunsAs: blueprint.RunsAsHelper, Tier: "medium", Effort: "high", Tools: []string{"Bash", "Read"}, Instructions: "components/reviewer.md"},
		"opener":   {RunsAs: blueprint.RunsAsHelper, Tier: "low", Instructions: "components/opener.md"},
		"later":    {RunsAs: blueprint.RunsAsHelper, Placeholder: true, Instructions: "components/later.md"},
		"planner":  {RunsAs: blueprint.RunsAsSession, Tier: "medium", Effort: "high", Instructions: "components/planner.md"},
		"uat":      {RunsAs: blueprint.RunsAsSession, Placeholder: true, Instructions: "components/uat.md"},
	}}
	return brain, b
}

func TestBuild(t *testing.T) {
	brain, b := testBrain(t)
	helpers, sessions, err := Build(brain, b)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"opener", "reviewer"}; !slices.Equal(helpers, want) {
		t.Errorf("helpers = %q, want %q", helpers, want)
	}
	if want := []string{"planner", "uat"}; !slices.Equal(sessions, want) {
		t.Errorf("sessions = %q, want %q", sessions, want)
	}

	raw := read(t, Path(brain))
	var got map[string]Agent
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("agents.json is not JSON: %v\n%s", err, raw)
	}
	want := map[string]Agent{
		"reviewer": {
			Description: "Reviews one change.",
			Prompt:      "# reviewer\n\nRead `<worktree>` & report.\n\n" + rules,
			Tools:       []string{"Bash", "Read"},
			Model:       "opus",
			Effort:      "high",
		},
		"opener": {Description: "Opens the PR.", Prompt: "# opener\n\n" + rules, Model: "sonnet"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("agents.json = %+v\nwant %+v", got, want)
	}
	if !strings.Contains(raw, "`<worktree>` & report") {
		t.Errorf("agents.json escapes the prompt's <, > or &:\n%s", raw)
	}
	var keys map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		t.Fatal(err)
	}
	if _, ok := keys["opener"]["tools"]; ok {
		t.Errorf("opener has a tools key; an entry without tools should leave it out, so it gets every tool")
	}
	if _, ok := keys["opener"]["effort"]; ok {
		t.Errorf("opener has an effort key; an entry without effort should leave it out")
	}

	if got, want := read(t, PromptPath(brain, "planner")), "# planner\n\nPlans the epic.\n\n"+rules; got != want {
		t.Errorf("planner prompt = %q, want %q", got, want)
	}
	if got, want := read(t, PromptPath(brain, "uat")), "# uat (placeholder)\n\n"+rules; got != want {
		t.Errorf("uat prompt = %q, want %q", got, want)
	}
	for _, dir := range []string{filepath.Join(brain, "factory"), promptsDir(brain)} {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp") {
				t.Errorf("temporary file left behind: %s", filepath.Join(dir, e.Name()))
			}
		}
	}
}

func TestBuildReplacesTheOldFiles(t *testing.T) {
	brain, b := testBrain(t)
	put(t, Path(brain), `{"stale": {}}`)
	put(t, PromptPath(brain, "planner"), "stale")
	if _, _, err := Build(brain, b); err != nil {
		t.Fatal(err)
	}
	if raw := read(t, Path(brain)); strings.Contains(raw, "stale") {
		t.Errorf("agents.json kept the old content:\n%s", raw)
	}
	if got := read(t, PromptPath(brain, "planner")); strings.Contains(got, "stale") {
		t.Errorf("planner prompt kept the old content: %q", got)
	}
}

func TestBuildErrors(t *testing.T) {
	cases := map[string]struct {
		remove string
		want   string
	}{
		"no rules":        {"factory/" + blueprint.RulesFile, "_rules.md: no such file or directory"},
		"no tiers":        {"AGENTS.md", "AGENTS.md: no such file or directory"},
		"no instructions": {"factory/components/opener.md", "components/opener.md: no such file or directory"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			brain, b := testBrain(t)
			if err := os.Remove(filepath.Join(brain, c.remove)); err != nil {
				t.Fatal(err)
			}
			_, _, err := Build(brain, b)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to contain %q", err, c.want)
			}
			if _, err := os.Stat(Path(brain)); !os.IsNotExist(err) {
				t.Errorf("agents.json was written despite the error")
			}
		})
	}
}
