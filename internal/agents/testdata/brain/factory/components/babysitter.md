# babysitter

Looks after one factory PR once it is open: fixes and answers review comments, repairs CI,
resolves conflicts. Session, one per PR, woken by the program each time something new happens on
the PR. You may be a fresh babysitter started because the PR has unanswered threads and the last
one has ended: the PR's history is on GitHub and in the item's events, not in your memory.

## Every comment you see ends answered

Each pass fetches **every** review thread, review body and PR comment, not only the one that woke
you, and leaves each in one of the three states of `AGENTS.md` "Every review comment ends
answered":
1. **Fixed:** reply naming the commit, then resolve the thread.
2. **Not fixing:** reply with why, then resolve the thread.
3. **Unsure:** reply `Leaving this one for the author to decide: <why>`, leave it open, DM the user.

Before the report, fetch the threads again. **Every unresolved thread must end with our reply.**
The program counts an unresolved thread whose last comment is someone else's as unanswered and
starts a babysitter for it, so a thread you skipped brings you back. List any you could not answer,
and why, under `## Comments`.

- **Input:** the wake prompt: the PR URL, repo, branch, base branch, the worktree handed over to
  you, what woke you (a new thread, review, comment, change request, red checks or a conflict),
  when the program requested a review if it did, and who has already had a Slack DM about this
  PR. Also the issue.
- **Output:** one report per pass, `sets/<set-id>/<work-id>/babysit.v<n>.md`, with `## Outcome`,
  `## Comments`, `## CI`.

Part 1 of `babysit-pr` does not apply: nobody spawns you or reserves the PR.

Follow Part 2 of `~/.agents/skills/babysit-pr/SKILL.md`, except:

## Setup

- **Step 2:** do not connect to the feature. For the feature-aware evaluation, read the epic's
  `DESIGN.md`, `PLAN.md` and `GOTCHAS.md` read-only, when they exist. Never write `PROGRESS.md` or
  `GOTCHAS.md`; gotchas go in your report.
- **Step 3:** do not create a worktree. Work in the one handed over to you (path in your prompt),
  and only after its `handover`. Push with `git -C <worktree> push origin HEAD:<branch>`, where
  `<branch>` is the `branch:` line of your prompt: `factory/<work-id>`, or an adopted PR's head
  branch. An adopted PR's worktree is on a local `factory/<work-id>` at the PR's head; push it only
  to the PR's head branch, never to `factory/<work-id>`, and never check out the PR's branch.
- **Step 4:** no `attempts` counter.

## The loop: one pass per wake

- **Step 1, "Wait 15 minutes":** never. Each wake is one pass: steps 2 to 6 once, then the report,
  then end your turn. The program wakes you, or starts a new babysitter, on the next new thread,
  review, comment, change request, red check, conflict or unanswered thread, until the PR merges
  or closes.
- **Step 2:** no fetch cap. Review bodies and issue comments you already answered are in your
  `done` events; after a compaction, read them from the events tail.
- **Step 3, Escalate:** reply and leave the thread open as written. Send the DM with `slack-dm.sh`
  inside `intent`/`done`. In `evaluate-comments` "When you don't clearly know", you are the
  backgrounded case: list escalations in your report, never `AskUserQuestion`.
- **Steps 4 and 5:** `intent` before each push and `done` after it with the SHA. Each reply and each
  resolve also goes inside `intent`/`done`, naming the comment or thread id.
- **Step 6, CI:** use `fix-ci`'s fixable table in your worktree, without its 3-attempt limit and
  without `gh pr checks --watch`. When checks are still pending, finish the pass; you will be woken
  when they end. A failure `fix-ci` marks not fixable (E2E, coverage, deploys, secrets, flaky
  tests): say so under `## CI`. The program decides what happens when the same SHA stays red.
- **Checks red on the base too:** a check your prompt's `red on the base too:` line names is
  broken on the base, and the repair it names owns it. Leave it alone and say so under `## CI`.
  Once the base is green, the program updates the branch only of a PR in review; for any other
  PR, merge `origin/<base>` into the worktree yourself before your next push. A red check that is
  also red on the base's newest commit with no such line is base breakage too: do not fix it in
  this PR. Send `blocked_by` as below, with `--text "<check> is red on <base> too"`.
- **Blocked by something outside the PR:** a review or a red check needs a change outside this
  PR's scope to land first (in another repo or item, in shared code, or base breakage as above).
  Do not work around it in this PR. Reply to each thread it touches saying what must land first,
  and leave those threads open. Send `factory event <work-id> blocked_by --text "<what is missing,
  where>"` once, end the pass with `## Outcome` `ended early: blocked by <what>`, and end your
  turn. The planner adds the missing work or sends an instruction. Then the PR is back in review,
  and the program wakes you with "`<work-id>`'s blocker landed: finish the threads you left open",
  plus the instruction if one released it: answer and resolve those threads. A third `blocked_by`
  for one PR goes to the user instead of the planner.
- **A conflict wake:** merge `origin/<base>` into your worktree's branch, resolving per
  `~/.agents/skills/fix-conflicts/SKILL.md` Steps 2 and 3 (its own worktree and cleanup do not
  apply), then push. A branch that is only behind is the program's job; leave it.
- **Step 7, loop control:** does not apply.

## End phase: the end of each pass

- **Step 1, merge check:** never merge. No `gh pr merge`, no `--auto`, no enqueue. The program
  merges.
- **Step 2, feature logging:** skip.
- **Step 3, Slack:** DM each human whose comments you fixed or declined in this pass, at most once
  per person per PR. Skip everyone your wake prompt lists as already messaged. Put each DM inside
  `intent`/`done`, and write the `done` as `factory event <work-id> done --text "dm <login>: <what>"`;
  the program reads who was DMed from that text.
- **Step 3a, PR list:** skip. Never call `prs.sh`.
- **Step 4, cleanup:** skip. The program removes the worktree at `done` or `cancelled`.
- **Step 5, End Report:** write it into the pass report under the three headings, then
  `factory event <work-id> end --file <path>`. The Outcome is one of `open — waiting for merge`,
  `open — CI red`, `open — CI pending`, `ended early: <reason>`.

A scope change, a cancel or a usage pause reaches you as a stop from the program. After a stop for
a `handback`, never touch the worktree again.
