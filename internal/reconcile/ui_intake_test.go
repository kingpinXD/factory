package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/store"
)

func TestAddTypedTextNeverReadsAHostPlan(t *testing.T) {
	for _, exists := range []bool{true, false} {
		t.Run(map[bool]string{true: "existing", false: "missing"}[exists], func(t *testing.T) {
			w := newWorld(t)
			file := filepath.Join(t.TempDir(), "host-plan.yaml")
			if exists {
				put(t, file, "private host content")
			}
			id, err := AddTyped(context.Background(), w.deps(), UIIntake{Kind: "text", Value: file}, Overrides{})
			if err != nil {
				t.Fatal(err)
			}
			var in EpicInputs
			if err := store.ReadInputs(w.dir(id), &in); err != nil {
				t.Fatal(err)
			}
			if in.Kind != "text" || in.Input != file || in.Path != "" {
				t.Fatalf("text became a host file: %+v", in)
			}
		})
	}
}
func TestAddTypedGitHubKindMustMatch(t *testing.T) {
	w := newWorld(t)
	w.hub.issue("o/r", 12).SubIssues.Total = 1
	_, err := AddTyped(context.Background(), w.deps(), UIIntake{Kind: "issue", Value: "https://github.com/o/r/issues/12"}, Overrides{})
	if err == nil || !strings.Contains(err.Error(), "actually epic") {
		t.Fatalf("mismatched issue kind: %v", err)
	}
	if ix, _ := store.ReadIndex(w.brain); len(ix) != 0 {
		t.Fatalf("refused input created entities: %v", ix)
	}
}
func TestAddTypedUploadUsesOnlyServerPlanPath(t *testing.T) {
	w := newWorld(t)
	file := filepath.Join(t.TempDir(), "admitted.yaml")
	put(t, file, "# admitted plan")
	id, err := AddTyped(context.Background(), w.deps(), UIIntake{Kind: "plan_upload", PlanPath: file}, Overrides{})
	if err != nil {
		t.Fatal(err)
	}
	var in EpicInputs
	if err := store.ReadInputs(w.dir(id), &in); err != nil {
		t.Fatal(err)
	}
	if in.Kind != "plan" || in.Path != file {
		t.Fatalf("wrong upload: %+v", in)
	}
	for _, input := range []UIIntake{{Kind: "plan_upload", Value: file, PlanPath: file}, {Kind: "plan_upload", PlanPath: "relative.yaml"}, {Kind: "issue", Value: file}, {Kind: "text", Value: ""}} {
		if _, err := AddTyped(context.Background(), w.deps(), input, Overrides{}); err == nil {
			t.Fatalf("accepted invalid typed input: %+v", input)
		}
	}
}
func TestAddTypedNativeRefusesBeforeBlueprintWrite(t *testing.T) {
	w := newWorld(t)
	put(t, filepath.Join(w.brain, "factory", "executors", "e1", "selection.json"), `{"executor":"native"}`)
	_, err := AddTyped(context.Background(), w.deps(), UIIntake{Kind: "text", Value: "Build it"}, Overrides{})
	if err == nil || !strings.Contains(err.Error(), "unsupported executor") {
		t.Fatalf("native refusal: %v", err)
	}
	if len(w.fake.Calls()) != 0 {
		t.Fatalf("refusal ran external tools: %v", w.fake.Calls())
	}
	if _, err := os.Stat(blueprint.GoodPath(w.brain)); !os.IsNotExist(err) {
		t.Fatalf("refusal wrote blueprint fallback: %v", err)
	}
}
