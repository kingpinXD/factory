package agents

import (
	"fmt"
	"regexp"
	"strings"
)

// Only the Codex babysitter component invokes this adapter. Source snapshots stay unchanged.
func transformCodexBabysitter(body string) string {
	body = replaceCodexBabysitterSection(body, "- **Step 3:** do not create a worktree.", "- **Step 4:**", `- **Step 3:** use only the handed-over worktree. Delegate every repository edit,
  test, commit, base merge, conflict resolution and push to the contained implementor helper
  with mode="existing_pr_revision". Copy the actual work ID, owner ID, PR number, branch,
  base and worktree from the current instructions. Never create a worktree or another PR.
`)
	body = replaceCodexBabysitterSection(body, "- **Steps 4 and 5:**", "- **Step 6, CI:**", `- **Steps 4 and 5:** delegate fixes to the contained implementor helper in
  existing_pr_revision mode. Its verified report must name the tests and pushed commit.
  Reply and resolve through pr_reply and pr_resolve as described below. The host records
  intent/done for those actions. The helper records intent/done around its push.
`)
	body = replaceCodexBabysitterSection(body, "- **A conflict wake:**", "- **Step 7, loop control:**", `- **A conflict wake:** delegate the merge of origin/<base>, conflict resolution,
  verification, commit and push to the contained implementor in existing_pr_revision mode.
  Follow fix-conflicts Steps 2 and 3 inside that helper. A merely behind branch stays
  the program's job.
`)
	body = replaceCodexBabysitterSection(body, "- **Step 3, Slack:**", "- **Step 3a, PR list:**", "- **Step 3, Slack:** skip. Never send Slack messages or review requests.\n")
	body = strings.ReplaceAll(body, "merge `origin/<base>` into the worktree yourself before your next push", "delegate merging origin/<base> into the worktree to the existing_pr_revision helper before its next push")
	return body + "\n\n" + codexBabysitterTransport
}

func replaceCodexBabysitterSection(body, start, end, replacement string) string {
	first := strings.Index(body, start)
	if first < 0 {
		return body
	}
	last := strings.Index(body[first:], end)
	if last < 0 {
		return body
	}
	return body[:first] + replacement + body[first+last:]
}

// The broker must validate existing_pr_revision mode before applying this overlay.
func CodexExistingPRRevisionPrompt(prompt string) (string, error) {
	paragraph := regexp.MustCompile(`(?m)- \*\*Commits:\*\*[^\n]*(?:\n  [^\n]*)*`)
	if !paragraph.MatchString(prompt) {
		return "", fmt.Errorf("existing PR implementor has no recognized commit policy")
	}
	prompt = paragraph.ReplaceAllString(prompt, `- **Commits:** commit verified fixes in the contained worktree. This job is an
  existing_pr_revision admitted by the broker for its current babysitter. Push only new
  commits to the bound existing PR head branch, after factory_helper(op="lease_check")
  and an inbox read. Record factory_helper(op="intent") before the push and
  factory_helper(op="done") after it with the actual commit SHA. Never amend, force-push,
  create a PR, merge, request reviewers or send Slack messages.`)
	return prompt + "\n\n" + codexExistingPRTransport, nil
}

const codexBabysitterTransport = `# Codex babysitter operations

You are a coordinator with factory_control only. Read and evaluate every review thread,
review body and PR issue comment on the one owned PR. Use github_pr, github_threads,
github_reviews, github_comments and github_checks with {repo,number}; github_checks
also needs the current sha. Read your events for already answered review/comment IDs.
The original feature decisions, comment evaluation, CI repair, base-breakage handling,
blocked_by escalation, conflict repair and one-pass report requirements still apply.

For repository inspection, edits, tests, commits and pushes, submit the existing
implementor with helper_submit. Set mode="existing_pr_revision" in its request. Copy the current PR identity into
revision_pr, revision_repo, revision_branch and revision_base. The host must match
these immutable request fields to recorded work and a fresh GitHub PR before admission.
The broker admits this mode only for the current babysitter and handed-over work.
Include the exact current PR, branch, base, worktree, findings and required checks in
its task. Retain its job ID, finish the turn and consume the published helper result
when woken. Never substitute a pr-opener that creates a new PR. Never treat queued,
running, uncertain or failed helpers as finished. Use another scoped helper round when
verification finds a problem. The helper must verify its change and run the repo's full
required checks before pushing; its report names the actual pushed SHA and red checks.

Use factory_control(op="pr_reply",payload={repo,number,kind:"thread",thread_id,
comment_id,body}) for a review-thread reply. comment_id is that thread's First.ID,
not an arbitrary middle comment. For review bodies use kind:"review" and its review
ID as comment_id. For PR issue comments use kind:"comment" and its comment ID.
Review/comment replies appear as PR issue comments. Name the answered review/comment
in the body. The host verifies IDs against fresh data from this PR, records intent
and done, and recovers interrupted replies without duplication. Do not add your own
retry marker. Fixed replies name the helper's verified pushed SHA. Declined replies
explain why. Unsure replies state the best reading and uncertainty and stay open.

Use factory_control(op="pr_resolve",payload={repo,number,thread_id}) only after
replying. The host requires this current thread's last comment to be ours. Fetch
threads again before the report: every unresolved thread must end in our reply,
and every fixed or declined thread must be resolved. Review bodies and PR issue
comments have no resolve action; their replies still need done receipts. Preserve
## Outcome, ## Comments and ## CI in the pass report. End through event with its
exact output path and do not wait. Never merge, enqueue, enable auto-merge, request
reviewers, post review requests, DM anyone or use Slack.
`

const codexExistingPRTransport = `# Existing PR revision

The broker grants only the current existing PR revision. Work in the bound contained
worktree and use managed_files/managed_terminal for all repository access. Follow the
owning babysitter's supplied evaluated findings and the repo instructions. Repair
comments and fixable CI, or merge the bound base and resolve conflicts when requested.
Read relevant source first, make the change, run the repo's full required checks, and
verify the resulting diff. Do not work around a missing outside dependency. Return
NEEDS_CONTEXT with the blocker for the babysitter to answer on the PR.

Use only the existing PR head branch supplied by the broker as the push destination.
An adopted PR can have a different head branch from the local factory branch. Never
push to a guessed branch. Never create or edit a different PR, merge a PR, amend,
force-push, request reviewers, post reviews/replies or send Slack messages. The owning
babysitter performs replies and resolves through its structured PR controls.
Return the verified tests, pushed SHA and any unresolved failures in the reserved
report, preserving ## Status, ## TODOs, ## Judgment calls, ## Red tests and ## Files touched.
`
