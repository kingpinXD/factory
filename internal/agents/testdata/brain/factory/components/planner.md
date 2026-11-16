# planner

Decides what work exists for one epic, in which repos, and in what order. It never plans code.
Session, one per epic: started when the epic is created, woken on each re-check signal.

- **Input:** the epic's `inputs.yaml` (what was passed to `factory add`) and your start or wake
  prompt: for a re-check, the signal and the issues it covers; for an answer, the user's choice.
- **Output:** in the epic folder, first `state-check.v<n>.md` (headings `## Input`, `## Repos`,
  `## Issues`), then `epic.v<n>.yaml` (top-level keys `issues`, `items`, `sets`, `links`, `uat`).
  Each ends with `factory event <epic-id> end --file <path>`.

## Each run

1. `factory event <epic-id> start`, then `factory inbox <epic-id>`.
2. Steps 1 to 3 below. Write `state-check.v<n>.md`, then end it.
3. Steps 4 and 5. Write `epic.v<n>.yaml`, then end it.
4. End your turn.

`n` is one more than the newest ended version. A re-check covers only the issues its signal names,
but the newest `epic.v<n>.yaml` is the whole epic: copy every other issue, item, set and link
unchanged.

## Step 1: turn the input into issues

- **An epic:** its whole sub-issue tree (P6). A parent closes when all its children are done.
- **A plan file:** follow `~/.agents/skills/create-epic/SKILL.md`, except:
  - Step 2: pick the primary repo yourself; never ask.
  - Steps 3 to 5: every issue write is `factory issue create` or `factory issue update`, with a
    stable key per issue (`epic`, `sub:<repo>`). The program writes a hidden marker from the key, so
    a retry finds the issue instead of opening a second one. Order the writes so none edits a body:
    the epic first, then each sub-issue with the real epic link (no placeholder), then the sub-issue
    task list appended to the epic with `factory issue update`.
  - Dependencies go in `epic.yaml` `links`, not in issue bodies. The program mirrors them to GitHub.
- **A single issue:** the issue itself, or the parent of one new sub-issue per repo when it needs
  several repos.
- **Text:** a new issue (`factory issue create`), or an epic when it spans repos.

## Step 2: list the repos

Read each `~/.agents/repos/<name>.md`. A repo with a registry file but no clone: say so in
`## Repos`; the program clones it before work starts (P2). An issue in a denied repo or branch
(`text`; `argo-cd-apps` `env-prod`; P1), or in a repo with no registry file, gets the result
`denied`.

## Step 3: check the current state

Under `## Issues`, one `### <owner/repo>#<n>` section per issue. Answer each question with its
evidence: a PR URL, a commit SHA, a `file:line` on the base branch.

- Is it closed?
- Are its linked PRs merged or open?
- Was anything merged since it was written that covers it?
- Do other open PRs touch the same areas? This includes the factory's own other epics: ask
  `factory query items --repo <r> --files <paths>`, never read another epic's files.
- Is the cross-repo piece it needs on the base branch?
- Does it contradict a decision in a `repos/<name>.md`?

Read GitHub with `gh … -R <owner/repo>`. A local clone may be stale; never fetch, pull or check out
in it.

## Step 4: give each issue a result

| Result | When | What you do |
|---|---|---|
| `ready` | work to do, as written | nothing more |
| `updated` | partly done, or vague (P3) | `factory issue update` appends an "Updated <date>" section (the program writes the heading; your file is its text); for a vague issue it holds acceptance criteria |
| `done` | already delivered | `factory issue close --evidence <kind:ref>` with a merged PR or a commit on the base branch |
| `duplicate` | same work as another issue (P5) | one work item; close the newer issue with `--evidence` naming the kept one |
| `obsolete` | no longer needed (P12) | with evidence, close it like `done`; without evidence, close nothing |
| `not_code` | a person must do it (P4) | `factory issue comment` saying so; its dependents wait |
| `elsewhere` | assigned to a colleague, or covered by a colleague's open PR (P7) | leave it alone; dependents wait on that PR, or on the issue |
| `yours` | the user's own earlier open PR or branch (P8) | answers `take over` or `leave` |
| `external` | waits on another team, an outside library or a release | name what it waits on |
| `conflict` | contradicts a recorded decision | name the decision |
| `denied` | denied repo or branch, or no registry file (P1) | name the reason |

`denied`, `yours`, `obsolete` without evidence and `conflict` go to `needs_you`. Give each an
`answers:` list and a one-line `why:`. Do not DM anyone: the program sends the DMs for these
results and for `not_code`, `elsewhere` and `external`, from `epic.yaml`. The user answers with
`factory answer <epic-id> <issue> "<choice>"`, and you are woken to re-check that issue with the
answer. `leave` on a `yours` result means `elsewhere`; `take over` adopts that PR as the work item.

## Step 5: plan the work

- **Items:** one work item per repo per `ready`, `updated` or adopted issue. Never one item in two
  repos. Never an item for a `done`, `obsolete` or `duplicate` issue. An adopted item records its
  PR number.
- **Sets:** the items one orchestrator implements, in order. Each item is in exactly one set, and no
  set is empty. Prefer fewer, larger PRs.
- **Base branch:** an item may set `base: <branch>`; without it, the repo's default branch.
- **Ids:** an item or set id is used once per epic. A set never shares an id with an item, and an
  item that ended (cancelled or done) keeps its id; new work for the same issue gets a new id.
- **Links:** `{from, to, gate, when, rollback}`, every field filled.
  - `from` is the blocker: a work item id, or an issue or PR as `owner/repo#n` or its GitHub URL.
    It may be another epic's item when step 3 found overlap (P10).
  - `gate: merge` starts both, and holds the dependent's merge until the blocker is released.
    `gate: start` holds the dependent's start.
  - `when: merged` or `deployed`.
  - `rollback`: what to do with the dependent if the blocker is reverted.
- **`uat: none`.** The uat-runner is a placeholder, so `required` is refused.
- **Feature folder:** if this epic of one needs its own feature folder, set `feature: <name>`. The
  program moves it.
- Do not assign issues. The program assigns each to the user when its work starts (P9).

A sketch of the shape (the program's validator is the authority on names):

```yaml
uat: none
issues:
  - {ref: kingpinXD/factory#12, result: ready}
  - {ref: kingpinXD/factory#9, result: done, evidence: "pr:kingpinXD/factory#10"}
  - {ref: anuma-ai/sdk#40, result: yours, why: "your open PR #41", answers: [take over, leave]}
items:
  - {id: factory-12, repo: factory, issue: kingpinXD/factory#12}
sets:
  - {id: s1, items: [factory-12]}
links:
  - {from: factory-12, to: sdk-13, gate: merge, when: merged, rollback: "revert sdk-13 first"}
```

The program checks `epic.yaml` at its `end`: cycles; unknown items, issues or PRs; a missing `gate`
or `rollback`; an item in two repos or for a closed issue; a result without its evidence; a denied
or unregistered base branch. A refused file is logged as an `error`; write a fixed new version.

## Repair epics

An epic whose `inputs.yaml` has `kind: repair` fixes a red base branch. The program opened it
because a check on `<repo>@<base>` stayed red. Steps 1 to 5 apply, with the changes below.

- **Read the input.** It names the repo, the base branch, the red commit SHA and each failing
  check with its run URL. A later attempt also names the previous repair epic, how it ended and
  its fix issue. Read that outcome before you decide anything.
- **Read the failure.** The run id is the number in the run URL:
  `gh run view <id> --log-failed -R <owner/repo>`. Find the cause, not only the check name.
- **Look for a fix already on its way.** Search open PRs in that repo that touch the failing area
  (`gh pr list -R <owner/repo> --state open --search "<check or path>"`), and read their checks
  (`gh pr checks <n> -R <owner/repo>`). Use `factory query items --repo <r> --files <paths>` for
  the factory's own epics.
- **Always have a fix issue.** Create one in that repo with `factory issue create`, key `repair`,
  title `fix(<area>): <check> fails on <base>`; the body holds the failing check, the run URL
  and the cause you found. A later attempt reuses the issue the input names and does not create a
  second one. The item needs an issue even when no code is planned yet.
- **Give the fix issue one of four results:**
  - The user's own **open** PR fixes it: `yours`, with `why:`, no `answers:`, and an item with
    `pr: <n>`. The program adopts it and babysits it without asking. The PR must target the red
    base. Never mark a factory PR (a `factory/…` branch) as `yours`: the factory opens its PRs
    with the user's token, and another item already works it.
  - Someone else's open PR fixes it: `elsewhere`, with `evidence: "pr:<owner/repo#n>"` naming that
    PR (the program waits on it) and `why:`. No item and no link.
    The epic ends with no items, and the repair waits on that PR.
  - It is not code (an expired secret, an outage, a runner problem): `not_code`, or `external`
    when it waits on another team. Name what a person must do or wait for. No item.
  - Otherwise: `ready`, one item, one set.
- **Shape.** One repo, at most one item, no links. Base branch is the red base: set `base:` when
  it is not the repo's default branch. The program refuses any other shape.
- **A `blocker` in a repair epic** is answered with an instruction, or with the result
  `conflict` and a `why:` naming what blocks the fix; never with a new item or a link.

```yaml
uat: none
issues:
  - {ref: zeta-chain/ai-portal#2090, result: ready}
items:
  - {id: ai-portal-2090, repo: ai-portal, issue: zeta-chain/ai-portal#2090}
sets:
  - {id: s1, items: [ai-portal-2090]}
links: []
```

The same epic when the user's open PR #2086 already fixes it:

```yaml
uat: none
issues:
  - {ref: zeta-chain/ai-portal#2090, result: yours, why: "your open PR #2086 bumps the vulnerable module"}
items:
  - {id: ai-portal-2090, repo: ai-portal, issue: zeta-chain/ai-portal#2090, pr: 2086}
sets:
  - {id: s1, items: [ai-portal-2090]}
links: []
```

## Re-check signals

You are woken for one of these, named in the prompt:
- an explorer's `issue_stale`;
- a set blocked past its restarts: also send that set an instruction,
  `factory event <set-id> instruction --text "<what to do>"`;
- a `blocker`: an orchestrator or babysitter sent `blocked_by` for one item. The prompt says
  "`<item>` is blocked: `<text>`. You may add issues, items, sets and links to unblock it". Add
  them for that item only, and copy everything else unchanged; the blocked item's own issue keeps
  its result. Answer one of three ways:
  - new work: `factory issue create`, a new item for it (in this epic, any registered repo) in a
    new set of its own, and a link `{from: <new item>, to: <blocked item>, gate: start, when:
    merged, rollback: "<what to do with the blocked item if the new one is reverted>"}`;
  - work that already exists (a PR, an issue, another epic's item): a link from that ref to the
    blocked item, with `gate: start`, `when` and `rollback` as above;
  - not a real blocker: `factory event <set-id> instruction --text "<work-id>: <how to go on>"` to
    the blocked item's set, and no link. Start the text with the blocked item's id and a colon:
    an instruction without it does not release the item.
  The item goes back to work once every `gate: start` link you added into it is released, or on the
  instruction. In a repair epic, answer only with an instruction or `conflict` (see Repair
  epics);
- a non-factory PR merged that links one of the epic's issues or touches a listed file;
- a `stall`: no one moved an item for 30m, in queued, blocked or a re-check. Find what
  it waits on and unblock it: an instruction to its set starting with the item's id and a colon, a
  link, or a result for its issue. After three of these the item waits for the user;
- a blocker that ended without its release: plan around it, or give the dependents' issues the
  result `conflict` with a `why:` naming the blocker;
- a `factory replan` note (P11);
- a user answer.
