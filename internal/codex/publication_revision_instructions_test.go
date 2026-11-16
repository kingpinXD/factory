package codex

import (
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/helper"
)

func TestPublicationRevisionInstructionsReserveFactoryPush(t *testing.T) {
	job := helper.Job{Request: helper.Request{Mode: helper.ModeExistingPRRevision, Component: "implementor", OwnerID: "w", WorkID: "w", RevisionPR: 7, RevisionRepo: "o/r", RevisionBranch: "factory/w", RevisionBase: "main"}, Scope: helper.Scope{CodexContract: 2, OwnerID: "w", WorkID: "w"}}
	job.Route.Provider = "codex"
	const snapshot = "Original work instructions.\n- **Commits:** commit after tests. Never push.\n\n## Status\n"
	got, err := helperInstructions(job, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Original work instructions.", "## Status", "commit verified fixes locally", "Never push or write to GitHub", "Factory imports the helper revision and pushes it after completion", "overrides publication actions in the component snapshot", `"head_branch":"factory/w"`, "managed_files/managed_terminal", "committed SHA"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing revision publication instruction %q", want)
		}
	}
	for _, bad := range []string{"Push only", "before the push", "after it with the actual commit", "pushed SHA", "as the push destination"} {
		if strings.Contains(got, bad) {
			t.Errorf("helper still owns publication: %q", bad)
		}
	}
	ordinary := job
	ordinary.Request.Mode = ""
	unchanged, err := helperInstructions(ordinary, snapshot)
	if err != nil || unchanged != snapshot {
		t.Fatalf("ordinary implementor snapshot changed: %q %v", unchanged, err)
	}
}
