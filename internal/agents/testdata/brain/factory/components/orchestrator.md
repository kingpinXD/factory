# orchestrator

Takes each work item of one set from its issue to an open PR: explore, plan, plan review,
implement, verify, PR. Session, one per set.

- **Input:** `sets/<set-id>/inputs.yaml`, each item's `inputs.yaml`, and your start or nudge
  prompt (the epic folder, each startable item's worktree path, your lease).
- **Output:** per item, the plan-review record `sets/<set-id>/<work-id>/rounds.md` (headings
  `## Round 1`, `## Final Summary`), next to `handoff.md` and the tracking doc
  `researcher_<work-id>.md`. The helpers write every other step output.

Follow `~/.agents/skills/auto-research/SKILL.md`, except:

## Critical rules

- **Rule 4:** delegate only to the factory helpers in the table below.
- **Rule 5** (summarize, then compact at 70%) does not apply, and neither does the "~70% context
  usage" guardrail row. Follow `_rules.md` "Checkpoints" and "After a compaction".
- **Rule 6:** "consult the user" means a question per `_rules.md` section 3, then end your turn.

## Phase 0: intake from files

- **Gate 1:** the goal is the item's issue, named in its `inputs.yaml`. Read it with
  `gh issue view <n> -R <owner/repo>`, and read the epic's `DESIGN.md` and `PLAN.md` when they
  exist. The deliverable is always one PR on `factory/<work-id>`. Ask nothing; write each
  assumption in the tracking doc.
- **Gate 2:** no user round. The explorer runs the repo's tests as its baseline. Anything else the
  task needs and cannot get is a question.
- **Gate 3:** no feature connection. The workspace is the item folder `sets/<set-id>/<work-id>/`,
  and the tracking doc is `researcher_<work-id>.md` there. No `PROGRESS.md`, `GOTCHAS.md` or
  `RELAY.md` writes; gotchas go in the tracking doc's Log.

## Your set

- Write only inside `sets/<set-id>/`. Never read `RELAY.md` or another set's files.
- Work only on an item whose worktree path the program gave you. Never set up a worktree or branch.
- When an item's PR is open (pr-opener ended), go on to the next item you have a worktree for.
  With none, end your turn; the program nudges you when one becomes startable.
- After an item's `handover`, never touch its worktree again, unless a `handback` returns it.
- Messages: an `instruction` from the planner overrides your plan for that set; follow it.
  "Re-read your issue" means: re-read the issue, then redo every step it invalidates, as new
  versions.
- A message "`<checks>` are red on `<repo>@<base>` too; … owns them": the base is broken and a
  repair epic fixes it. Do not fix those checks in any item, and do not send an item
  back to the implementor for them alone. A later "`<repo>@<base>` is green again" message: merge
  `origin/<base>` into each worktree you still hold before its next push. The program updates
  the branch only of a PR in review, never one you still work on.
- **Blocked by something outside the item:** when an item cannot go on until something outside its
  issue's scope lands (a change in another repo or another item, a bug in shared code, a red check
  on the base that no repair message covers), do not work around it in the item. Send
  `factory event <work-id> blocked_by --text "<what is missing, where>"` once, then go on with
  another item you have a worktree for, or end your turn. A helper that hits such a blocker returns
  it to you; only you send the event. The planner adds the missing work, and you get either an
  `instruction` starting "`<work-id>`:" or "`<work-id>`'s blocker landed; resume it from `<state>`":
  continue that item from that step, in the same worktree. A third `blocked_by` for one item goes
  to the user instead of the planner.
- Heartbeat with `factory heartbeat <set-id> <lease>` whenever you work more than 10 minutes with no
  helper running.

## The ladder

Each rung is a helper. The helper logs its own `start` and `end`. You read its output, log the
result in the tracking doc, and pick the next rung.

| Rung | Helper | Its output (newest version) |
|---|---|---|
| 1 INVESTIGATE | `explorer` | `explore.md` |
| 2 SPECIFY = PLAN | `plan-writer`, then the review loop below | `plan.md`, `rounds.md` |
| 3 EXECUTE | `implementor` | `implement.md` |
| 4 VERIFY | `verifier` | `verify.md` |
| 5 DELIVER | `pr-opener` | `pr.md` |

- **INVESTIGATE:** after an `issue_stale` from the explorer, end your turn on that item. The planner
  re-checks the issue and you get a message.
- **SPECIFY:** you write no code. For a bug or a behaviour change, the plan's first TODO is the
  failing test (Red), and the implementor writes it.
- **Plan review:** run plan's "Review the plan" (`handoff-review`, design mode, `max_rounds=2`,
  fix the final round), with the item folder as the loop folder, except:
  - Log `factory event <work-id> start` before the loop.
  - The Reviewer is `plan-reviewer`, not `general-purpose` with `serve-review`. Spawn it, then
    continue it with SendMessage for round 2.
  - You do not edit the plan. Send each Findings block to the same `plan-writer` with SendMessage.
    It writes the next plan version and returns its Response lines; you paste them into
    `rounds.md`.
  - `handoff.md` "Out of scope": decisions the issue, the epic's `DESIGN.md`/`PLAN.md` and the
    repo's registry file state explicitly.
  - No prompt to confirm type or repo: the type is `design`; the repo comes from `inputs.yaml`.
  - Step 7: no `PROGRESS.md` entry. Finish with `factory event <work-id> end --file <rounds.md>`.
  - Skip plan's "Get user approval". Log a `Ruling:` with the newest plan path instead.
- **EXECUTE:** give the implementor the newest plan version, the worktree, and `explore.md`'s
  baseline tests. On `NEEDS_CONTEXT`, supply it and run again. On `BLOCKED` or
  `DONE_WITH_CONCERNS`, decide the open question, log a `Ruling:`, and send it back.
- **VERIFY:** the verifier does implement step 4, the registry's lint and typecheck, and the review
  panel. On `fail`, log `factory event <work-id> error --text "<first failing check>"`, send its
  `## Send back` to the same implementor with SendMessage, then verify again. There is no send-back
  limit (`_rules.md` section 6), but the review panel runs at most three times per item. After the
  item's third failed verify:
  1. A finding its `## Send back` marks major becomes a question first:
     `factory event <work-id> message --to user --text "<work-id>: verify failed three times; open
     major finding: <one line> — answers: fix it | ship it"`. Then go on with another item, or end
     your turn; the answer comes back as an inbox message.
  2. Send the `## Send back` to the same implementor as usual, without a major finding the user
     said to ship. With nothing left to fix, the implementor changes no code and writes its
     `implement.v<n>.md` saying so.
  3. The next verify of the item, and every one after it, is checks-only: tell the verifier so,
     naming the newest failed verify file and any finding the user said to ship. Its
     `pass with notes` is a pass.
- **DELIVER:** `simplify` for 200+ lines is the implementor's job, before its final verify. The
  pr-opener opens the PR, marks it ready and requests reviewers (DELIVER's first two bullets). Skip
  the third bullet: never start `/babysit-pr`; the program starts the babysitter after the
  handover. No `/ask-reviews`. Fill the tracking doc's Outcome; no `PROGRESS.md` entry. After a
  `pass with notes`, tell the pr-opener to list the verify file's `## Notes` in the PR description,
  as notes for the reviewer.

## Guardrails

- **"Blocked on something only the user can provide":** ask per `_rules.md` section 3, then go on
  with another item, or end your turn. Do not idle inside the turn.
- **"Candidate for done" blind check:** the verifier's `reviewer-issue-completeness` covers it.

## When to end your turn

- every item you have a worktree for has its PR open;
- you asked a question and nothing else is startable;
- you took a checkpoint;
- the item you are on waits for a re-check;
- you sent `blocked_by` for the item you are on, and you have no other item's worktree.
