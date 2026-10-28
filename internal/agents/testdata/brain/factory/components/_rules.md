# Factory rules

Shared rules for every factory agent. The program appends this file to each component's
instructions, sessions and helpers alike. **Sessions** are the planner, the orchestrator, the
babysitter and the uat-runner. **Helpers** are the agents a session spawns. A rule marked for one
kind applies to that kind only.

Your id is the `factory-task:` value in your first prompt. A helper gets its session's id, the set
id, the lease and the work id in its task prompt. Your working folder is an empty run folder, not a
repo: every path you use is absolute.

## 1. The inbox

`factory inbox <id>` is the only place to check for updates. It returns the messages you have not
acknowledged, each with a `seq`. Its first line is your context use, e.g. `context: 54%`.

Check it:
- at the start of each step;
- after each helper returns;
- before any push or PR;
- after a compaction;
- at least every 10 minutes or every 20 tool calls during long work.

The same message may also reach you in a wake prompt or between tool calls. The `seq` tells you it
is the same one. Act on each `seq` once.

**Sessions act on a message, then acknowledge it by its own `seq`:**
`factory inbox <id> --ack <seq>`. Acks are per message: acking 7 does not acknowledge 5.
When acting changes anything outside your own files, log the intent first and name the message in
it (`factory event <id> intent --text "msg <seq>: <action>"`), then ack, then act, then log `done`.
A restart finds the `intent` and checks the outside world before redoing it, so one message never
runs twice.

**Helpers never ack.** Read your session's inbox read-only:
`factory inbox <session-id> --read-only`. If a message is there, stop at a safe point and return
early. Put the message's `seq` and text on the first line of your report. Your session acts on it.

**Never wait inside a session.** If something you need is not ready — CI, a review, another item,
an answer — log an event that says what, and end your turn. The program wakes you when it is ready.
No `sleep`, no polling loop, no `--watch`.

Stop, cancel and usage pauses never come through the inbox. The program stops your session.

## 2. Events

Only the program writes state changes. You report with `factory event <id> <kind>`.

- **`start`** when a step begins. **`end --file <output>`** when its output is complete:
  `factory event <id> end --file <path>`. The output must carry its required headings. After `end`
  the file never changes. A redo writes a new version (`explore.v2.md`) with its own `start` and
  `end`.
- **Every reader uses the newest ended version**: the file named by the newest `end` event, never
  a fixed name.
- **`intent` before every outside action, `done` after it**: a push, a PR create or edit, marking
  ready, a GitHub reply or resolve, a Slack DM. The text names the action well
  enough to check it later: branch and SHA, PR number, comment id, Slack login. On a restart, an
  `intent` with no `done` means: check GitHub or Slack first, and redo only what is not there.
- **`heartbeat`** during long work, at least every 10 minutes: `factory heartbeat <set-id> <lease>`.
  This applies to the orchestrator and every helper working for a set. Its reply also starts with
  your context use.
- **`error --text "<one line>"`** once per failed step, not per attempt inside a step. Use the same
  text for the same failure: the program counts repeats.
- Other kinds a component uses are named in its own file: `issue_stale`, `blocked_by`,
  `instruction`, `message`, `checkpoint`.

## 3. No waits for approval; questions

Skip every "wait for the user", "ask the user", "confirm with the user", "present for approval" and
`AskUserQuestion` in the skills you follow. `AGENTS.md`: with a `factory-task:` line, skip user
approval; the plan review stands in.

A "stop and tell the user" still stops. Log `factory event <id> error --text "<why>"`, then return
(helper) or end your turn (session).

Decide judgment calls yourself and write down what you assumed, in your output's assumptions
section or as a `Ruling:` in the tracking doc.

Ask only in the three cases `AGENTS.md` "No guesswork" names: no reasonable choice exists, a wrong
choice is destructive or hard to undo, or the choice decides which PR, repo or account to act on.
- **Session:** `factory event <id> message --to user --text "<question> — answers: <a> | <b>"`,
  then end your turn. Only this work waits (`waiting_user`); the program DMs the user, and the
  answer comes back as an inbox message.
- **Helper:** return `NEEDS_CONTEXT` with the question. Your session sends it.

## 4. Checkpoints (sessions only; helpers never checkpoint)

From 50% context, take a checkpoint at your next clean stop, whether or not the `checkpoint_due`
message has reached you. A clean stop is:
- no helper running;
- no outside action between its `intent` and its `done`;
- the current step's output either ended or not started.

There:
1. Write `checkpoint.v<n>.md` in your own folder: the planner in the epic folder, the orchestrator
   in `sets/<set-id>/`, the babysitter in `sets/<set-id>/<work-id>/`. Headings: `## Where I am`,
   `## Next`, `## Notes`. Write only what your events and outputs do not already hold.
2. `factory event <id> checkpoint --file <path>`.
3. Ack `checkpoint_due` if you have it.
4. End your turn.

The program then compacts you and posts: "You were compacted. Read <checkpoint path>, then
`factory inbox <id>`, then continue from `## Next`." You cannot compact yourself: never run
`/compact`, `/summarize` or the summarize skill.

## 5. After a compaction

1. If the message names a checkpoint, read it first.
2. Run `factory inbox <id>`. Every message you had not acknowledged comes again.
3. Re-read your entity's `inputs.yaml`, the tail of its `events.jsonl` (`tail -40`), and your
   newest ended outputs.
4. Continue from the checkpoint's `## Next`. With no checkpoint (Claude Code compacted you by
   itself at 60%), continue from the newest event.

Never run the summarize skill. A helper that Claude Code compacts re-reads the input files named in
its task prompt and its own output so far.

## 6. Skill limits that do not apply

Factory agents have no limits on what they may do. The only limits are time, retries and restarts.
They live in `blueprint.yaml` and the program enforces them: state timeouts, "no progress", restart
counts, and the same `error` three times → dead-letter. So these limits in the skills you follow do
not apply:

- **`implementor` stop list** ("When to stop": a restructure the plan did not foresee, reading file
  after file without getting closer, and the rest). The factory `implementor.md` names the few
  reasons to return early.
- **`implementor` 3-fix cap** ("After three failed fixes on the same check, stop: report
  `BLOCKED`"). Keep "one hypothesis, one change, then re-run". After three misses, change the
  approach, not the last patch, and keep going.
- **`implement` 2 send-backs** (step 4, "After two send-backs, treat it as `BLOCKED`"). Send back
  as often as the verifier fails.
- **`create-pr` 2 tries** (Step 2, "Attempt to fix (up to 2 tries)"). Fix until the pre-push checks
  pass.
- **`create-pr` 30-min CI wait** (Ship Mode, Step 7a). Never Ship Mode; never wait for CI.
- **`fix-ci` 3 attempts** ("up to 3 attempts", "Stop after 3 fix attempts on the same issue").
- **`review-fix` 3 failures** (Step 5, "Stop after 3 failures on the same issue") and its CI step's
  "Stops after 3 attempts".
- **`babysit-pr` fetch cap** ("max 2 fetches, 15 min apart", `attempts = 2`). Every wake is one
  pass, with no cap on passes.
- **`review-fix` minor-only rule** (Step 4, "Fix Minor issues only if trivial"). Fix every finding
  you agree with, whatever its severity.
- **`review-fix` no-refactor rule** (Step 4, "Do not refactor beyond what the suggestion asks
  for"). Change as much as the fix needs.

## 7. Git and GitHub

- Never run `checkout`, `switch`, `pull`, `reset` or `stash` in the user's clone or anywhere outside
  your worktree. Never set up a worktree: the program made yours on `factory/<work-id>` off the base
  branch. This replaces `implement` step 0 and the `AGENTS.md` IMPLEMENT recipe.
- Every git call names the worktree: `git -C <worktree> …`. Every gh call names the repo:
  `gh … -R <owner/repo>`. A `gh api` call names the repo in its path.
- Push only new commits to your PR's branch. Never force-push, never amend.
- Orchestrator and its helpers: run `factory lease-ok <set-id> <lease>` before each push. A non-zero
  exit means your lease is gone: do not push; end your turn or return.
- Never touch a worktree after its `handover`, unless it was handed to you (babysitter) or handed
  back (`handback`).
- Never merge: no `gh pr merge`, no `--auto`, no enqueue. The program merges factory PRs.
- GitHub issue writes go only through `factory issue create|update|close|comment|assign`. Never
  `gh issue create|edit|close|comment`.
- Never call `prs.sh`. The program keeps the PR list for factory PRs. Never start `/babysit-pr` or
  `/ask-reviews`. Never post review requests; the program posts them.
- Read the repo's own `CLAUDE.md` and `AGENTS.md` from the worktree (`<worktree>/CLAUDE.md`), and
  its registry file `~/.agents/repos/<short-name>.md`. Your run folder loads no project
  instructions.

## 8. Files

- Write only the files you own: your step's output, your checkpoints, and (orchestrator) each
  item's tracking doc and plan-review files.
- Never write `inputs.yaml`, `events.jsonl`, `status.json`, `index.json`, `STATUS`, `PROGRESS.md`,
  `GOTCHAS.md`, `RELAY.md`, `prs.md`, a statusline marker, or anything in another set or epic.
- Never connect to a feature (`connect-feat`, `init-feat`). The epic folder is not a session's
  workspace.
- Learn anything else from your prompt, your inbox, or `factory query items --repo <r> [--files …]`.
  Never read another epic's or set's working files. Re-reading your own entity's `inputs.yaml` and
  `events.jsonl` is fine.

## 9. Helpers and models

- Spawn only factory helpers, by these names: `explorer`, `plan-writer`, `plan-reviewer`,
  `implementor`, `verifier`, `pr-opener`, `reviewer-code`, `reviewer-red-team`,
  `reviewer-arch-test`, `reviewer-issue-completeness`. Never `general-purpose`, `Explore`, `Plan` or
  any other type. When a skill says to spawn one of those, use the factory helper named in your
  component file, or do the work yourself.
- Never pass `model` or `effort` on an Agent call. The factory sets both. Never use Fable.
- In a factory session every helper runs in the background, whether or not you ask: its result
  arrives as a task notification that starts your next turn. Put independent helpers in one
  message so they run in parallel. Never poll a helper; wait for its notification.
- Give each helper: your id (for its read-only inbox), the set id, the lease, the work id, the repo,
  the worktree path, its input files and its output path, all absolute.

## 10. Writing

`AGENTS.md` "How to write" holds for everything you write. Anything on GitHub or Slack also follows
`~/.agents/memory/tone_guide.md`.
