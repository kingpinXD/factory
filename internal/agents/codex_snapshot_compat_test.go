package agents

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
)

func TestCodexProtocol3SnapshotRetainsPinnedPromptBytes(t *testing.T) {
	brain, b, out := codexBrain(t)
	selection, err := executor.ReadSelection(brain, "e-codex")
	if err != nil {
		t.Fatal(err)
	}
	old := sourceSnapshot{Executor: "codex", SchemaVersion: 1, PromptProtocol: 3, Components: map[string]blueprint.Component{}, Instructions: map[string]blueprint.Instructions{}, Rules: "Pinned rules.", HelperTransport: "Pinned helper transport v3.", SessionTransport: "helper_cancel uses {job_id,request:{schema_version:1,request_key,expected_round}}.", Bridge: "Pinned bridge v3."}
	for name := range selection.Routes {
		old.Components[name] = b.Components[name]
		old.Instructions[name] = blueprint.Instructions{Name: name, Body: "Pinned " + name + " procedure."}
	}
	data, err := json.MarshalIndent(old, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	sources := filepath.Join(out, "sources.json")
	put(t, sources, string(data))
	expected := map[string]string{
		"planner":  "Pinned planner procedure.\n\nPinned rules.\n\nhelper_cancel uses {job_id,request:{schema_version:1,request_key,expected_round}}.\n\nPinned bridge v3.",
		"explorer": "Pinned explorer procedure.\n\n# Factory rules (managed native helper)\n\nUse absolute task paths. The stored job is the authority for your owner, work item, set and lease.\n\nPinned helper transport v3.\n\nPinned bridge v3.",
	}
	before := map[string]os.FileInfo{}
	before[sources], err = os.Stat(sources)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range expected {
		file := PromptPathFor(out, name)
		put(t, file, text)
		before[file], err = os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
	}
	put(t, filepath.Join(brain, "factory", blueprint.RulesFile), "changed live rules")
	for run := 0; run < 2; run++ {
		if _, _, err := BuildFor(brain, out, b, nil); err != nil {
			t.Fatalf("pinned protocol 3 snapshot refused: %v", err)
		}
		if got := read(t, sources); got != string(data) {
			t.Fatal("older source snapshot was rewritten")
		}
		for name, want := range expected {
			if got := read(t, PromptPathFor(out, name)); got != want {
				t.Fatalf("protocol 3 %s prompt changed: %q", name, got)
			}
		}
		for file, prior := range before {
			after, err := os.Stat(file)
			if err != nil || !os.SameFile(prior, after) || !prior.ModTime().Equal(after.ModTime()) {
				t.Fatalf("immutable snapshot file was replaced or rewritten: %s %v", file, err)
			}
		}
	}
}
