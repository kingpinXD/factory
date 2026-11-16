package codex

import (
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/helper"
)

func TestPublicationOpenerInstructionsReserveProgramWrites(t *testing.T) {
	job := helper.Job{Request: helper.Request{Component: "pr-opener"}, Scope: helper.Scope{CodexContract: 2}}
	job.Route.Provider = "codex"
	const original = "Immutable component snapshot: open the PR."
	instructions, err := helperInstructions(job, original)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{original, "Authoritative publication procedure", "title and body only", "Never push, commit, or create a PR", "## PR", "## Checks", "## Reviewers", "```json\n{\"title\":\"...\",\"body\":\"...\"}\n```", "escape newlines", "verified commit", "ignores the opener workspace"} {
		if !strings.Contains(instructions, want) {
			t.Errorf("missing publication contract %q", want)
		}
	}
	for _, change := range []func(*helper.Job){
		func(j *helper.Job) { j.Route.Provider = "claude" },
		func(j *helper.Job) { j.Scope.CodexContract = 0 },
		func(j *helper.Job) { j.Request.Component = "verifier" },
	} {
		other := job
		change(&other)
		got, err := helperInstructions(other, original)
		if err != nil || got != original {
			t.Fatalf("unrelated helper instructions changed: %q, %v", got, err)
		}
	}
}
