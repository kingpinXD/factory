# factory

An always-running pipeline that takes an issue, epic or plan to merged PRs, using Claude Code or Codex sessions.

The plan and its decisions live in the author's agent harness, not in this repo.

```sh
make build      # go build ./...
make test       # go test -race ./...
make install    # builds ~/.local/bin/factory
```

## Commands

Every command, in the order `factory --help` lists them. A comment says what the command does;
the line under it shows its arguments.

```sh
# Adds an epic from an issue, an epic, a plan file or a line of text.
factory add <issue-url | epic-url | plan-file | "text"> [--executor claude|codex] [--set step=tier]... [--effort step=level]...

# Runs one tick: one pass of the pipeline. After factory launchd install, a launchd job runs it every minute.
factory tick [--dry-run]

# Prints the factory's load and what waits on the user. The JSON is described below.
factory status [--json]

# Gives the user's answer to a question or a result that waits on them.
factory answer <epic-id> <issue> "<choice>"

# Asks the next tick to return <id>, which is in needs_you, to the state it left (in_review if it left merging). Refused when its PR is closed, or when it waits on your answer to a re-check: that takes factory answer.
factory retry <id>

# Asks the next tick to cancel <id> and the work under it: their PRs close and their worktrees and local branches are removed. An adopted PR stays open and keeps its work.
factory stop <id> --reason "<why>"

# Records that you asked for reviews of these factory PRs yourself, so the factory never posts a review request for them; status shows them as "asked by you". Running it again adds nothing; a PR that is not a factory item's is refused.
factory review-asked <pr-url>...

# Asks the next tick to re-check every issue of an epic, with the note for its planner.
factory replan <epic-id> "<note>"

# Checks the blueprint and prints each problem, or its machine, state and transition counts when it passes; --output instead checks one step's output file for its required headings.
factory check [--output <component> <file>]

# Prints the blueprint as JSON.
factory blueprint --json

# Logs one event. A message sent --to another id reaches that id's inbox.
factory event <id> <kind> [--file <path>] [--text "<one line>"] [--to user|<id>]

# Renews a set's lease, if <lease> is still its current one.
factory heartbeat <set-id> <lease>

# Prints the unacknowledged messages for <id>, or acknowledges one.
factory inbox <id> [--ack <seq>] [--read-only]

# Lists a repo's in-flight items: one block per item (id, epic, state, issue, files), then a final
# "<n> items" line ("0 items" when none match). --files keeps those touching the paths or whose files are unknown.
# --state keeps those in one of the given states (repeat it for more); an unknown state is an error that names
# the valid ones.
factory query items --repo <owner/repo> [--files <path>...] [--state <state>]...

# Exits 0 when the factory's plans let a PR merge, 1 when not yet or unknown, and prints why.
factory may-merge <pr-url>

# Records that a PR is deployed, in each open epic whose plan waits on that. It does not check that the PR merged.
factory deployed <pr-url>

# Makes one GitHub issue write. Run by a factory session, it is logged in that session's events.
factory issue create  --repo <owner/repo> --key <stable key> --title "<t>" --body-file <path> [--parent <number>]
factory issue update  --repo <owner/repo> --number <n> --append-file <path>
factory issue comment --repo <owner/repo> --number <n> --body-file <path>
factory issue close   --repo <owner/repo> --number <n> --evidence <kind:ref> [--comment-file <path>]
factory issue assign  --repo <owner/repo> --number <n>

# Builds agents.json and the session prompts from the blueprint, once it passes its check.
factory agents

# Installs or removes the launchd jobs that run tick and repo-worker every minute.
factory launchd install|remove

# Exits 0 while <lease> is the set's current lease.
factory lease-ok <set-id> <lease>

# Does the repo work the tick queued.
factory repo-worker

# Prints the module version and the git commit the binary was built from; a missing part prints unknown. factory --version does the same.
factory version

# Opens a local browser control room without starting a manager.
factory ui [--listen 127.0.0.1:0] [--brain /absolute/path] [--read-only]
```

## Executors

Claude is the default. To use Codex for an epic, run
`factory add <input> --executor codex`.

Codex needs Docker, a pinned profile at `<brain>/factory/executors/codex.yaml`,
a registered private repository, and a passing `factory codex-verify` for that
profile. Version 2 requires `--contract 2 --repo <owner/repo> --base <branch>`.
See [Codex setup and recovery](docs/codex-executor.md) for the profile and checks.

Codex helpers make local commits. Factory pushes the verified commit, opens and
readies the PR, publishes later fixes, and merges when the repository's checks
allow it.

An uncertain publication write can put the item in `needs_you`. Inspect the
remote branch or PR before recording what happened:

```sh
factory settle-publication <item> --write <key> --outcome landed --head <sha> # push
factory settle-publication <item> --write <key> --outcome landed --pr <N>     # PR write
factory settle-publication <item> --write <key> --outcome not-landed
factory retry <item>
```

Use the write key from the hold reason. Before choosing `not-landed` for a
conflicting remote head, restore the expected head, or delete the branch if it
was expected to be absent. Factory makes no further publication writes until
the hold is settled.

## `factory status --json`

One JSON object. Times are UTC RFC 3339; a list is `[]` when empty. `factory status` without
`--json` prints the same report as text, with ages counted to `at` and at most 10 items per section.

```jsonc
{
  "at": "2026-10-09T15:00:00Z",          // when the report was read
  "last_tick": "2026-10-09T14:59:00Z",   // left out when no tick ran, or after `factory launchd remove`
  "tick": 42,                            // the last finished tick's number
  "account": {                           // factory/account/status.json; {"state": ""} before the first tick
    "state": "near_limit",               // ok | near_limit | paused | resuming | stale
    "since": "…",
    "usage": {                           // the newest usage figure younger than usage.stale_after; left out when none
      "session_id": "…", "written_at": "…",
      "five_hour": {"used_percentage": 82, "resets_at": 1791561600},   // resets_at in Unix seconds
      "seven_day": {"used_percentage": 61, "resets_at": 1791817200}
    },
    "resets_at": "…", "weekly": true,    // the reset a pause waits for; left out when none
    "held": 10800000000000,              // time in this state while the factory was idle, not counted toward its timeout
                                         // nanoseconds; only holds that ended; left out at 0
    "held_since": "…",                   // when the current hold began, or `since` if it began in an earlier state
                                         // left out when none
    "probed_at": "…", "errors": ["…"]    // left out when none
  },
  "sessions": {"running": 2, "by_repo": {"o/r": 2}},   // sessions not stopped, dead or finished; a planner's are in no repo
  "waiting_on_user": [                   // entities in needs_you or waiting_user
    {"id": "e1-w2", "state": "needs_you", "since": "…", "why": "the question asked, or why it needs you"}
  ],
  "blocked": [                           // work items in blocked; the text shows each as "blocked: <why>"
    {"id": "e1-w3", "since": "…", "why": "the blocked_by text its session sent, or restarts used up"}
  ],
  "in_review": [{"repo": "o/r", "count": 1, "oldest": "e1-w1", "oldest_since": "…"}],
  "dead_letters": [{"id": "e1-w2", "error": "the error reported too often"}],
  "paused_repos": ["o/r@main"],          // red base branches a queued item waits on: nothing new starts there
  "repairs": [                           // base branches red since a full reconcile read them red, until they read green
    {"repo": "o/r", "base": "main", "red_since": "…",
     "checks": ["govulncheck"],          // the checks that failed on the newest red head
     "epic": "e2610131",                 // the open repair epic; "" when none is open
     "epic_state": "checking",           // its state; "" when none is open
     "attempts": 1,                      // its repair epics in the last 24h, which count toward values.repair_attempts
     "stopped": false,                   // a repair epic was stopped with factory stop: no more start
     "waiting_on": ""}                   // the open PR, owner/repo#n, a repair found fixing it elsewhere
  ],
  "epics": [{"id": "e1", "state": "running", "since": "…", "rechecks": 1}],   // open epics
  "agents": [                            // sessions not finished
    {"name": "factory:e1-s1:orchestrator",   // for claude attach <name> or claude logs <name>
     "id": "a1b2c3d4",                   // the short id they also take; "" until a tick saw it listed
     "task": "e1-s1", "state": "running",
     "context": 54,                      // percent of values.context.window; null until measured
     "unread": 2, "oldest_unread": "…"}  // messages to the task not acknowledged; oldest_unread left out at 0
  ],
  "open_prs": [                          // work items whose PR is open
    {"id": "e1-w1", "url": "…", "state": "in_review",
     "unanswered": 2,                    // unresolved threads whose last comment is not the user's
     "merge_hold": "…",                  // what kept it from merging at the last full reconcile; "" when nothing
     "review": "…",                      // "review requested in #<review_channel_name>", "asked you by DM", "asked by you", "wants a review", or ""
     "review_at": "…"}                   // when the review request was posted, DMed or marked asked; left out until then
  ],
  "errors": ["…"],                       // what the last tick went on past
  "problems": ["…"]                      // blueprint problems while the tick runs on the last good copy
}
```


## Helper CLI

Use the current executable through `FACTORY_BIN` and the selected `FACTORY_BRAIN`.
Every command also accepts `--brain <absolute-brain>`.

- `helper submit --request <json-file|->` accepts the versioned helper Request.
- `helper continue <job-id> --request <json-file|->` accepts ContinueRequest,
  including `expected_round` and explicit failed-child acknowledgments for recovery.
- `helper result <job-id> --round <n>` reads durable RoundResult, including
  nonterminal state. A nonterminal result is not successful completion.
- `helper yield <job-id> --request <json-file|->` accepts `{"children":["job-id"]}`.
- `helper cancel <job-id> --request <json-file|->` accepts `{"reason":"..."}`.
  Cancellation abandons the job; it is not a recoverable interrupt.
- `helper status [job-id]` prints redacted model/state/hold/attach information.
- `helper drain --brain <absolute-brain>` closes indexed Codex epics, fences
  their helper writers and finishes durable result delivery. It abandons queued
  and interrupted work; use recoverable interrupt instead when work must resume.
Successful submit/continue returns schema-version-1 Accepted JSON. Reads of
nonexistent jobs/rounds fail. Unknown fields, duplicate keys, trailing JSON and
oversized requests fail. Never resubmit with a different request key merely
because a runner restart or notification failed: admission is already durable.

### Managed host control

Managed helpers use `FACTORY_BIN helper control --job <job-id> --generation
<n> --brain <absolute-brain> --request -`. The envelope is `{"op":"...",
"payload":{...}}`. It has no caller-selected executable, owner or lease.

| Operation | Payload |
| --- | --- |
| `lease_check`, `inbox` | `{}` |
| `submit` | `request_key`, `component`, `prompt`, `output_path` |
| `continue` | `job_id`, `request` (the versioned ContinueRequest) |
| `result` | `job_id`, `round` |
| `yield` | `children` (every submitted child ID) |
| `heartbeat`, `intent`, `done`, `error` | optional one-line `text` and `key` |
| `brain_read` | `path` (canonical allowlisted markdown path), optional `max_bytes` |

Submit derives child authority from the stored parent. Continue/result cannot
address foreign jobs. Inbox is read-only. Managed start/end, inbox ack, login,
arbitrary host commands and cancellation are not managed helper operations.

`lease_check` returns exactly `schema_version`, `op`, `job_id`, `generation`,
`allowed`. Missing/stale authority or a revoking scope returns `allowed:false`.
Result reads include checksummed inline `reply` and `report` text; private host
file paths are blank. Each artifact is bounded to 384 KiB, and the full encoded
result must fit 1 MiB. Oversized results require an explicit future chunked
transport; they are not silently truncated or treated as completed delivery.

`brain_read.max_bytes` is a positive integer through 65536.
Omission defaults to 65536. Null, floating-point values, unknown keys and larger
bounds fail. Files exceeding the selected byte limit
fail rather than returning silently truncated knowledge.

The detached broker observes and publishes without consulting Claude listings or
Claude usage holds. Completion first persists a keyed owner inbox entry. Only
normal tick policy wakes the owner. Scope revocation closes admission
before fencing writers and before persisting a replacement lease/ownership event.
Legacy CLI start/end cannot publish a managed output reservation, even if a child
omits all managed environment variables.

### Ownership recovery and rollback

The broker owns managed admission and output-reservation guards. CLI callbacks
append only scoped events under those guards; they do not reenter Service or
acquire tick. The integration never parses or edits the private helper ledger.

Ownership changes fence each affected work ID before publishing its lease or
handover/handback event. Authority reads sync those events before returning a
new generation. `RecoverScope` then reopens only the matching stored generation,
including scopes fenced before their first job. Tick retries recovery after a
coordinator crash without replacing the writer or replaying its prompt. A valid
new-owner Submit can also recover the matching closed fence atomically. Unchanged,
revoking or uncertain scopes remain denied.

Babysitter file helpers use the stored item's historical set directory through
ArtifactResolver. That directory is pinned with the job for later rounds. It
never supplies a current set lease: work-owned helpers retain empty `set_id`
and lease zero. Old orchestrator requests remain unauthorized.

Codex coordinator recovery records process closure and uncertain delivery
outcomes before admitting a successor. An unreadable delivery outcome needs the
user's decision before it can be sent again. Account holds do not replace these
ownership checks. See [Codex recovery](docs/codex-executor.md) for details.

Epic-only helper grants are not enabled. The approved contract requires an
explicit component-specific grant, but neither the plan nor the blueprint names
one or assigns an epic helper a contained repository worktree. All helper
submissions therefore still require a work item. This is a scope restriction,
not an implicit grant to run against the brain directory.

Before rollback, stop new intake and factory scheduling, and stop the top-level
coordinator sessions using this brain. Then run `helper drain` with the **current**
factory binary and explicit brain. It holds tick while closing every indexed
Codex epic, fencing descendants and running a broker pass to finish result
publication/delivery. It starts no new inference. Failure to prove writer death
or account for any helper job fails the drain. Retrying is safe. Admission stays
closed for the drained epics; use a new epic for new work.

A successful drain reports `drained:true` for **managed helpers only** and
`downgrade_ready:false`. It does not stop coordinators, change launchd, select an
older binary, archive epics, or certify that an old binary understands Codex state.
Keep drained Codex epics in an archived brain; use a fresh, separately configured
legacy brain for an older factory. Do not point an older executable at active
Codex work or delete helper/executor records to make it appear drained. Never
overwrite a running binary during rollback.

### Remaining live acceptance gates

Codex admission requires a live `factory codex-verify` receipt for the pinned
profile and contract. Offline tests do not establish live readiness. A complete
sample epic is a separate check.

### Claude-only compatibility acceptance

Merging requires execution-path compatibility with a freshly frozen `origin/main`
revision, not just matching final code or passing tests. Record main and candidate
SHAs. Candidate flagless intake and explicit `--executor claude` must both match
main's flagless path using the same frozen brain/model settings and equivalent
isolated repositories/issues. Codex credentials, profiles, binaries and Docker
must not be required or invoked.

Deterministic differential tests replay identical agent/GitHub responses, clock
and quota observations, and failures against both revisions. Compare substantive
events, prompts, effective models/effort, delegation, repository actions and
ordering constraints. Map only incidental IDs, paths, timestamps and fixture
commit identities; reference behavior must come from main, never the candidate.
Cover success, quota holds, failing CI, requested changes, restart and checkpoints.

Paired live epics must also preserve issue-to-PR grouping, dependency releases,
explore/plan/review/implement/verify phases, review continuation, ownership
transfers, retries and merge gates. Claude wording and exact patches can vary,
but matching final output cannot excuse an unexplained workflow difference.
Retain the traces and artifacts used to compare each lane.
Verify existing legacy state can resume, Claude model/effort overrides retain
their behavior, and checkpoint, ownership, CI/review/dependency merge gates and
cleanup still work. Do not copy a live epic into a writable test environment.

After merge, build the exact merged revision and repeat the flagless and explicit
Claude smoke checks without Codex dependencies. This does not authorize replacing
the installed factory or services. Record actual results; these compatibility
checks remain pending until executed. Both trial modes obey their account limits.

### Licensing

The Go factory is MIT-licensed (`LICENSE`).

## Local browser UI

Run `factory ui` and open the authenticated loopback URL it prints. The one-use
fragment is exchanged for a local browser cookie and removed from the address bar.
Keep that startup URL private. The server binds a numeric loopback address only;
remote access is unsupported.

The UI shows jobs, current questions, delivery counts, dependencies, manager
freshness and cached Claude capacity. Job details include activity, validated
plans and explicit private inspection. Read errors retain the last good browser
observation. A running manager or delivered message does not establish useful
worker progress. Completed, cancelled and pending cleanup counts stay separate.

`--brain` binds this server to that absolute brain directory. It does not change
other processes or install a scheduler. `--read-only` disables job submission and
factory controls. Neither startup nor polling probes authentication, queries
GitHub or starts Claude. Submission loads runtime dependencies only when requested.
The browser submits Claude jobs. Select Codex through
`factory add <input> --executor codex`.

New jobs accept written text, GitHub issues/epics, or bounded UTF-8 plan uploads.
Text is always text, even when it resembles a host file path. Submitting may start
agents, create issues and PRs, and merge under the repository policy. Available
controls are scoped to the selected job or task and revalidated under the manager
lock. Cancellation previews descendant, PR, branch and worktree consequences;
adopted PR preservation is shown explicitly. Recording deployment records a fact
for the selected job and does not deploy software.

Action receipts distinguish queued requests from applied, refused, failed and
unknown outcomes. If a response is lost, the browser checks the same request id
without replaying intake. Private receipts and uploads are stored under
`<brain>/factory/ui/` with restrictive permissions. The browser stores only
unresolved request ids and their scopes for reload recovery, never tokens, form
text or diagnostic content. An unknown submission remains blocked for inspection;
check the saved receipt and any resulting job before submitting it again.

Browser answers require a fresh completed tick that advertises
`ui_action_protocol=1`. An older installed manager leaves Answer disabled. Building
or starting this UI does not upgrade the manager. Stop UI mutations before a
manager downgrade; do not rely on a previous compatible tick after downgrading.

Logs, conversations and output files load only on request and have bounded,
server-redacted previews. Selected diagnostic export previews the same evidence
before downloading JSON. It can still contain private code, prompts and issue
text. Nothing is uploaded. The UI provides no shell, process control, generic file
browser, force-merge or pause action. Closing the browser or stopping this HTTP
server leaves the manager, workers and submitted work running independently.

The frontend is embedded vanilla HTML/CSS/JavaScript with no build, external assets
or runtime packages. Run its regression checks with
`node --test internal/webui/frontend_test.mjs` in addition to the Go checks.
