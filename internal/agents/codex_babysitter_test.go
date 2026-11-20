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
	for _, want := range []string{"Factory imports", "local commit", "github_pr shows the reported commit"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("babysitter does not wait for Factory publication: %q", want)
		}
	}
	for _, bad := range []string{"helper records intent/done around its push", "before pushing", "actual pushed SHA"} {
		if strings.Contains(prompt, bad) {
			t.Errorf("babysitter still expects helper publication: %q", bad)
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
	for _, want := range []string{"commit verified fixes locally", "Factory imports", "Never push or write to GitHub", "managed_files/managed_terminal", "existing PR head branch", "Never amend", "full"} {
		if !strings.Contains(changed, want) {
			t.Errorf("revision helper lost %q", want)
		}
	}
	for _, bad := range []string{"commits to the bound existing PR head branch", "before the push", "pushed SHA"} {
		if strings.Contains(changed, bad) {
			t.Errorf("revision helper still owns publication: %q", bad)
		}
	}
	if _, err = CodexExistingPRRevisionPrompt("arbitrary component without commit policy"); err == nil {
		t.Fatal("unknown helper policy silently overridden")
	}
}
