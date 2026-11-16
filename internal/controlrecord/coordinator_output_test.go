package controlrecord

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCoordinatorOutputUsesActualComponentAndTarget(t *testing.T) {
	dir := brainForTest(t)
	b := &blueprint.Blueprint{Components: map[string]blueprint.Component{"planner": {Outputs: blueprint.Outputs{{Path: "epic.v1.yaml"}}}, "implementor": {Outputs: blueprint.Outputs{{Path: "implement.v1.md"}}}, "babysitter": {Outputs: blueprint.Outputs{{Path: "review.v1.md"}}}}}
	for _, tc := range []struct {
		component, kind, name string
		write, want           bool
	}{
		{"planner", blueprint.MachineEpic, "epic.v2.yaml", true, true},
		{"planner", blueprint.MachineEpic, "implement.v1.md", true, false},
		{"orchestrator", blueprint.MachineSet, "checkpoint.v2.md", true, true},
		{"orchestrator", blueprint.MachineWork, "handoff.md", true, true},
		{"orchestrator", blueprint.MachineSet, "handoff.md", true, false},
		{"orchestrator", blueprint.MachineWork, "implement.v1.md", false, true},
		{"orchestrator", blueprint.MachineWork, "implement.v1.md", true, false},
		{"babysitter-1", blueprint.MachineWork, "review.v2.md", true, true},
		{"planner", blueprint.MachineEpic, "inputs.yaml", false, true},
		{"planner", blueprint.MachineEpic, "inputs.yaml", true, false},
	} {
		t.Run(tc.component+"/"+tc.kind+"/"+tc.name, func(t *testing.T) {
			got, err := ResolveCoordinatorOutputBlueprint(b, store.Entry{Dir: dir, Kind: tc.kind}, session.Spec{Name: "factory:epic:" + tc.component}, tc.name, tc.write)
			if (err == nil) != tc.want || err == nil && got != filepath.Join(dir, tc.name) {
				t.Fatalf("policy result %q %v", got, err)
			}
		})
	}
}

func TestCoordinatorOutputRejectsTargetAndFileAliases(t *testing.T) {
	dir := brainForTest(t)
	b := &blueprint.Blueprint{}
	other := dir + "-alias"
	if err := os.Symlink(dir, other); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(other) })
	if _, err := ResolveCoordinatorOutputBlueprint(b, store.Entry{Dir: other}, session.Spec{}, "checkpoint.md", true); err == nil {
		t.Fatal("target directory alias accepted")
	}
	if err := os.Symlink(filepath.Join(dir, "foreign"), filepath.Join(dir, "checkpoint.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveCoordinatorOutputBlueprint(b, store.Entry{Dir: dir}, session.Spec{}, "checkpoint.md", true); err == nil {
		t.Fatal("output alias accepted")
	}
}
