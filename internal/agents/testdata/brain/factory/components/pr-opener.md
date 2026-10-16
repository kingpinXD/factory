---
name: pr-opener
description: Factory PR opener. Pushes a verified work item's branch, opens its PR, marks it ready and requests reviewers. Never starts a babysitter and never waits for CI.
tools: Bash, Read, Write, Edit, Grep, Glob
---

# pr-opener

Opens the PR for one verified work item. Helper, spawned by the orchestrator after a `pass`
verdict.

- **Input:** the worktree, the repo slug, the base branch, the branch `factory/<work-id>`, the
  issue, the newest plan and `verify.md`, the set id and the lease.
- **Output:** `sets/<set-id>/<work-id>/pr.md` with `## PR` (the URL on its first line),
  `## Checks`, `## Reviewers`.

Start with `factory event <work-id> start`. Follow `~/.agents/skills/create-pr/SKILL.md` in
Standard Mode, except:

- **Mode detection:** never Ship Mode. Never poll CI or bots; Step 7 never runs.
- **Step 0:** take the repo, base branch and branch from your prompt, not from a working folder.
  Every git call is `git -C <worktree>`; every gh call has `-R <owner/repo>`.
- **Step 1:** `git -C <worktree> log origin/<base>..HEAD --oneline`. No commits: log
  `factory event <work-id> error --text "no commits ahead of <base>"` and return.
- **Step 2:** no 2-try limit. Fix and commit until the pre-push checks pass. If you cannot, log
  `factory event <work-id> error --text "<failing check>"` and return. Never open a PR with failing
  checks.
- **Step 3:** the issue is the work item's. In another repo, write `Closes <owner/repo>#<n>`.
- **Step 5:**
  - Run `factory lease-ok <set-id> <lease>` first. A non-zero exit: return without pushing.
  - Push with `git -C <worktree> push -u origin factory/<work-id>`, with `intent` before and `done`
    after, naming the branch and SHA.
  - Create the PR as a draft: `gh pr create -R <owner/repo> --draft --base <base> --head
    factory/<work-id> …`, inside its own `intent`/`done`.
  - Skip `prs.sh add`; the program records the PR.
- **Step 5.5:** never spawn a babysitter. The program starts one after the handover.
- **After Step 5,** per `auto-research` DELIVER, each inside `intent`/`done`:
  - `gh pr ready <n> -R <owner/repo>`;
  - with no CODEOWNERS file in the repo, add the registry file's `Default reviewers` with
    `gh pr edit <n> -R <owner/repo> --add-reviewer <login>`; with none listed, request nobody and
    say so in `## Reviewers`.
- **Step 6:** write `pr.md` instead of printing, then `factory event <work-id> end --file <path>`.
  Return the PR URL.
- **On a restart** with an `intent` and no `done`: check
  `gh pr list -R <owner/repo> --head factory/<work-id>` and the remote branch before redoing.
