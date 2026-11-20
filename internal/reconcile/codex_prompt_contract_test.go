package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
)

func TestCodexGeneratedCoordinatorPromptsAcceptContractTwo(t *testing.T) {
	w, _, _ := nativeBaseWorld(t)
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range b.Components {
		if component.Placeholder || component.Instructions == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Join("../agents/testdata/brain/factory", component.Instructions))
		if err != nil {
			t.Fatal(err)
		}
		put(t, filepath.Join(w.brain, "factory", component.Instructions), string(data))
	}
	data, err := os.ReadFile("../agents/testdata/brain/factory/" + blueprint.RulesFile)
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(w.brain, "factory", blueprint.RulesFile), string(data))
	out := filepath.Join(w.brain, "factory", "executors", "e1")
	if _, _, err := agents.BuildFor(w.brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	r.b = b
	r.ents["e1"].epic.PlanInputSHA256 = strings.Repeat("a", 64)
	for _, role := range []struct{ component, owner string }{
		{componentPlanner, "e1"},
		{componentOrchestrator, "e1-s1"},
		{babysitterComponent, "e1-w1"},
	} {
		t.Run(role.component, func(t *testing.T) {
			owner := r.ents[role.owner]
			static, err := os.ReadFile(agents.PromptPathFor(out, role.component))
			if err != nil {
				t.Fatal(err)
			}
			news := []delivery{{msg: events.Event{Seq: 1, Kind: events.KindInstruction, Sender: "factory", Text: "Read the current inbox and continue."}}}
			messages := map[string]string{
				"instructions":  string(static) + agents.CodexControlInstructions(2, false),
				"start":         coordinatorText("codex", r.startPrompt(owner, role.component)),
				"wake":          coordinatorText("codex", r.wakePrompt(owner, role.component, news)),
				"nudge":         coordinatorText("codex", r.nudgePrompt(owner, "its turn ended")),
				"resume":        coordinatorText("codex", r.resumePrompt(owner, role.component, news)),
				"compacted":     coordinatorText("codex", compactedPrompt(owner.id, filepath.Join(owner.entry.Dir, "checkpoint.v1.md"))),
				"no-checkpoint": coordinatorText("codex", compactedPrompt(owner.id, "")),
			}
			for name, text := range messages {
				t.Run(name, func(t *testing.T) {
					if err := agents.CheckCodexContractOperations(text, false); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
