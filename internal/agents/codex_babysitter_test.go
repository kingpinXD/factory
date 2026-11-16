package agents

import (
	"strings"
	"testing"
)

func TestCodexBabysitterBuildKeepsProcedureAndScopedPRActions(t *testing.T) {
	brain, b, out := codexBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	prompt := read(t, PromptPathFor(out, "babysitter"))
	for _, want := range []string{"existing_pr_revision", "github_pr", "github_threads", "github_reviews", "github_comments", "github_checks", `factory_control(op="pr_reply"`, `factory_control(op="pr_resolve"`, `kind:"thread"`, `kind:"review"`, `kind:"comment"`, "First.ID", "## Outcome", "## Comments", "## CI", "blocked_by", "conflict", "one pass", "Never merge", "Never send Slack"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("babysitter lost %q", want)
		}
	}
	for _, bad := range []string{"Push with `git -C", "DM each human", "merge `origin/<base>` into your worktree's branch"} {
		if strings.Contains(prompt, bad) {
			t.Errorf("coordinator retains unavailable action: %q", bad)
		}
	}
	source := read(t, PromptPathFor(out, "implementor"))
	if !strings.Contains(source, "Never push.") {
		t.Fatal("ordinary implementor acquired revision push authority")
	}
	changed, err := CodexExistingPRRevisionPrompt(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`factory_helper(op="lease_check")`, `factory_helper(op="intent")`, `factory_helper(op="done")`, "managed_files/managed_terminal", "existing PR head branch", "Never amend, force-push", "full"} {
		if !strings.Contains(changed, want) {
			t.Errorf("revision helper lost %q", want)
		}
	}
	if strings.Contains(changed, "Never push.") {
		t.Fatal("admitted revision helper still cannot push")
	}
	if _, err = CodexExistingPRRevisionPrompt("arbitrary component without commit policy"); err == nil {
		t.Fatal("unknown helper policy silently overridden")
	}
}
