package agents

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
)

func mustTiers(t *testing.T, brain string) blueprint.Tiers {
	t.Helper()
	v, err := blueprint.ReadTiers(brain)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBuildForRejectsTierOverrideAndCorruption(t *testing.T) {
	brain, b, out := codexBrain(t)
	if _, _, err := BuildFor(brain, out, b, map[string]string{"planner": "high"}); err == nil {
		t.Fatal("accepted unpinned top tier")
	}
	put(t, filepath.Join(out, "selection.json"), `{"schema_version":99}`)
	if _, _, err := BuildFor(brain, out, b, nil); err == nil {
		t.Fatal("corrupt selection became legacy")
	}
}

func TestBuildForLegacyBytesMatchBuild(t *testing.T) {
	brain, b := testBrain(t)
	if _, _, err := Build(brain, b); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "isolated")
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := read(t, filepath.Join(out, "agents.json")), read(t, Path(brain)); got != want {
		t.Fatalf("legacy definitions changed\ngot: %s\nwant: %s", got, want)
	}
	for _, name := range []string{"planner", "uat"} {
		if read(t, PromptPathFor(out, name)) != read(t, PromptPath(brain, name)) {
			t.Fatal("legacy prompt changed", name)
		}
	}
}

func TestBuildForRejectsUnmappedHelperName(t *testing.T) {
	brain, b, out := codexBrain(t)
	file := filepath.Join(brain, "factory", "components", "implementor.md")
	put(t, file, strings.Replace(read(t, file), "name: implementor", "name: reviewer-code", 1))
	if _, _, err := BuildFor(brain, out, b, nil); err == nil || !strings.Contains(err.Error(), "differs from component") {
		t.Fatalf("unmapped helper: %v", err)
	}
}

func TestHostControlTransformationPreservesActionText(t *testing.T) {
	for _, kind := range []string{"intent", "done", "error"} {
		input := "factory event <work-id> " + kind + " --text \"branch and SHA\""
		got := transformHelper(input)
		if !strings.Contains(got, "factory_helper "+kind) || !strings.Contains(got, "branch and SHA") {
			t.Fatalf("lost action payload: %s", got)
		}
	}
}

func TestBuildForRejectsSharedDirectoryAlias(t *testing.T) {
	brain, b := testBrain(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Join(brain, "factory"), alias); err != nil {
		t.Fatal(err)
	}
	if _, _, err := BuildFor(brain, alias, b, nil); err == nil || !strings.Contains(err.Error(), "isolated") {
		t.Fatalf("shared alias: %v", err)
	}
	if _, err := os.Stat(filepath.Join(brain, "factory", "sources.json")); !os.IsNotExist(err) {
		t.Fatal("shared files written")
	}
}

func TestBuildForIncompleteSelectionNeverLegacy(t *testing.T) {
	brain, b, out := codexBrain(t)
	if err := os.Remove(filepath.Join(out, "selection.json")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := BuildFor(brain, out, b, nil); err == nil {
		t.Fatal("incomplete selection became legacy")
	}
	if _, err := os.Stat(filepath.Join(out, "sources.json")); !os.IsNotExist(err) {
		t.Fatal("generated legacy snapshot for incomplete selection")
	}
}
