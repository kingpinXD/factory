# Codex executor

The factory retains three independent execution choices. The default uses Claude.

The additive `codex` choice uses Codex app-server for every model role.

Codex admission remains held until the pinned configuration passes the live
acceptance command. Offline tests and a successful protocol round do not satisfy
that requirement. The model-backed cancellation, conversation continuation,
compaction and complete sample epic have not yet passed for this implementation.

## Configuration and intake

Use `factory add <input> --executor codex`. A profile defaults to
`<brain>/factory/executors/codex.yaml`; `--executor-profile <file>` selects another
profile. An existing epic retains its immutable selection and instruction snapshot.
Changing the live profile never changes that epic.

The profile schema is strict. It requires these fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | `1` |
| `executor` | `codex` |
| `max_active_helpers` | `1` |
| `codex_binary` | Absolute host Codex executable path |
| `codex_exec_server` | Absolute path to the Linux Codex executable mounted read-only into the worker container |
| `codex_model_catalog` | Absolute path to the host CLI bundled model catalog with only `gpt-6.1-sol.tool_mode` set to `direct` |
| `codex_model_catalog_sha256` | Exact SHA256 of that catalog file |
| `codex_version` | Exact shared version of the host and Linux executables |
| `codex_disabled_mcp` | Configured MCP names to disable individually; an unexpected enabled server holds admission |
| `container_image` | Image pinned by its SHA256 digest |
| `identity_binding` | Expected subscription account identity returned by the quota API |
| `model_evidence_policy` | `codex-configured-persisted-v1` |
| `routes` | One Codex candidate per enabled planner, orchestrator, babysitter and helper component |

Each route pins `provider: codex`, a supported tier and an explicit reasoning
effort. Models resolve through the shared brain's Codex model column. Intake saves
both executable hashes and the immutable container image ID. Z-specific worker and
credential settings are rejected in a Codex profile.

Obtain the catalog with the pinned executable's `debug models --bundled` command.
Change only the pinned model's `tool_mode` to `direct` and record the file hash.
Intake checks that every other bundled catalog value is unchanged. Runtime checks
the hash and effective catalog path before opening a conversation. This keeps the
same model while exposing the factory tools directly. The local code host remains
disabled. Failed acceptance attempts retain bounded protocol metadata in
`protocol-diagnostics.json`; text, tool arguments and provider error messages are
excluded.

Run `factory codex-verify --help` for the bounded acceptance command. Verification
uses the same pinned selection, remote execution transport and observation
contract as ordinary workers. It refuses inference when included quota is unknown
or held. A partial or failed run does not publish an acceptance receipt. There is
no environment variable or flag that marks unexecuted checks successful.

## Process and tool ownership

A detached factory host owns each coordinator app-server, event stream, command
socket and durable session record. Manager ticks read those records and send
bounded commands. They do not attach to the desktop daemon. Stable delivery keys
and persisted submission intent prevent an ambiguous acknowledgment from causing
an automatic replay. A prompt or compaction command is acknowledged only after its
payload reaches the durable host queue. The host submits it after the manager
releases its tick lock. Stop still waits for physical fencing. Recovery retries
only queued messages whose model submission provably never began.

Host subscription authentication remains with the host app-server. Its selected
execution environment connects to a Codex exec-server inside the attested Docker
container. Provider authentication files are not copied into the container.
The container receives only explicitly allowed repository credentials and tools.

Every remote filesystem or process request crosses an authenticated loopback
WebSocket proxy. The proxy acquires current factory authority before forwarding a
request and retains that guard until the execution server acknowledges it. Unknown
methods, revoked generations, malformed frames and unconfirmed ownership close
the connection. Reconnection does not bypass the guard.

Helpers use the existing broker. Their conversations retain stable job identities,
round numbers, request keys, leases, generations, parent yield and child completion
rules. Repository commands and files use managed tools in the contained worktree.
Reserved helper output is written through the authority-scoped host control path.
The output directory and factory ledgers are not writable container mounts.

Coordinator containers have a separate scratch workspace. Coordinator actions use
the `factory_control` tool. That tool binds lifecycle events, inbox acknowledgment,
knowledge reads, helper requests and permitted output files to the stored owner.
It does not expose a host shell.

## Saved results, recovery and quotas

Codex model evidence describes pinned configuration. It does not attest the model
that actually executed a turn. The factory keeps configured model, provider and
effort separate from the Z executor's observed-model field. Rerouting or conflicting
configuration holds the attempt.

A helper result requires an exact terminal turn and matching reply from a fresh
observation-only server. The factory records a hash of that reply. Publication
also requires container removal, exact owned process termination and the existing
private-Git verification and promotion. A completion notification alone supplies
none of those writer-stop guarantees.

The manager owns recovery. Status and browser inspection do not stop processes,
start servers or submit prompts. A stopped coordinator resumes the same saved
conversation in a new owned attempt. Missing turn identity, a pending ambiguous
delivery, stale process identity or uncertain fencing remains held.

Codex quota reads use the subscription account's own API. Unknown account binding,
permission, spend control or window values remain unknown. Included usage must be
explicitly allowed. New inference stops at the existing hold threshold; running
work pauses at the existing pause threshold. Purchased credits and direct API-key
authentication do not provide fallback capacity.

When Codex admission is held or paused, the manager and queued helper broker share
an hourly account-API retry schedule. The schedule is saved under
`factory/codex/quota/` for the pinned selection and usage policy. It survives
manager and broker restarts. These checks do not create a conversation or call a
model. Unknown or failed reads retry after five minutes and continue to deny work.
A reset timestamp alone never permits admission. A due read must freshly allow
ordinary usage and pass every reported usage window. Positive permission is not
saved for later launches.

Once fresh usage permits work, ordinary ticks resume quota-stopped coordinators
through the existing conversation, fencing and ownership checks. Queued helpers
return through normal broker admission. Failed, cancelled or ambiguous attempts
are not replayed. Running coordinator and helper checks remain independent of the
hourly admission schedule, so a new pause is still observed promptly. The factory
manager and helper runner must be running; this does not install a separate timer
or restart a stopped factory service. Status and dry-run inspection do not probe
usage or change the retry schedule.


Codex coordinator and nested helper requests omit `output_path` for reply-only
reviewers. File-producing helpers still require their reserved output path.
A known reply/file mismatch returns an unsuccessful tool result before admission,
so the same turn can correct it. Authority, storage and unknown transport failures
remain closed. Coordinator `file_write` returns `filename` for later checkpoint
and file operations; `path` identifies the absolute owned file.

Queued Codex work also requires a ready owning coordinator and its matching live
host at selection and execution claim. Recovery visible before that claim holds
work. A later owner failure or boundary still uses ordinary journal checks and
physical fencing. No lock covers network or model calls. Terminal Codex results
include `recovery` facts for stopped writers, descendants, settled publication and
proved absence of launch. A settled, fenced cancellation permits a fresh request
and output version under current authority. It never permits replaying the old
attempt or accepting its partial files. Unknown proof continues to block work.

Codex recovery facts also include the helper's immutable `stored_scope` and
nullable `scope_matches_current`. The comparison uses the current ledger scope;
it is not permission to continue. A closed matching scope still compares true.
False forbids continuing that old scope, including a completed helper. Fresh work
requires terminal stop/descendant/publication proof and current broker authority,
with a new request key and output version. Null means unknown. Corrupt ledgers
with missing scope barriers still fail closed instead of returning usable proof.

Plan reviewers can read the exact `handoff.md` from their own work item through
scoped artifact control. Existing declared outputs still cover `rounds.md`.
Relative repository paths keep their contained repository meaning; no broader
Markdown or write permission is added. Continuation `expected_round` means the
current stored round, not the next. A precise round mismatch is a corrective
refusal; idempotency conflicts and authority failures still reject normally.
Tracking writes and lifecycle events still wait while matching helpers are
unsettled. Their known busy refusal confirms no action occurred; it requires
settled stop/publication proof before a later request and never bypasses the guard.

Idle coordinator hosts do not poll running quota. Each new command checks fresh
admission, and active or compacting work retains independent running checks.
Current recovery instructions also accompany normal manager messages, so existing
immutable prompt snapshots are preserved.


Codex repository helpers configure their fresh private Git view before container
creation. They use only the host repository's configured author name and email,
real origin tracking commits and a normal fetch refspec. Missing or invalid author
configuration or an unavailable origin fails before launch. This does not copy
hooks or the full host configuration. Control-only sessions and the optional Z executor keep their paths.

For a canonical HTTPS GitHub origin, a private credential helper checks the exact
protocol, host and repository path before reading the existing runtime GitHub
token. It answers only credential reads. It never stores the token in Git
configuration, command arguments or the attempt journal. Foreign repositories and
descendant paths receive no credential. Existing attempts are observed or fenced;
preparation never changes their private Git setup after launch.

Codex knowledge reads also allow the exact writing guide `memory/tone_guide.md`.
Other files under `memory/` remain outside the allowlist. Legacy reads keep their
existing boundary.

A known busy publication refusal identifies one unsettled helper and its round.
The current epic planner may inspect a same-epic child's recovery metadata through
`helper_result`. Such a response is marked `read_only: true` and contains no
report, reply, output paths or arbitrary error text. The request is exactly
`{job_id,round}`; `read_only` is a response property. Extra request fields receive
corrective guidance after current caller authority is checked. It does not grant authority
to continue or cancel that helper. A later publication still passes the existing
write guard; unknown operations must not be replayed.

New set-owned Codex verifier rounds require the current `verifying` phase, and
PR-opener rounds require `pr_open`. The broker checks this before a new reservation
and again before a queued helper starts. Existing-key recovery, report inspection,
fencing and publication keep their original authority checks. After any failed
verification, the manager needs a fresh truthful implementation report before new
verification, even if the retained code requires no change. Keep the earlier
failure and reuse an existing PR only when its exact commit remains unchanged.

For an owned repository with its registered default base, a `github_base`
request using an unregistered head receives guidance to use the registered base
or `github_commit` with a branch/ref in `sha`. This correction does not expand
repository scope or nondefault-base handling. A failed allowed GitHub read returns
an unsuccessful tool reply with unknown evidence. It does not prove absence or
permit repetition of an uncertain write. Existing scoped helpers can still use
authorized contained read-only preflight under current authority. Local authority,
scope, input and storage failures retain their existing fatal behavior.

## Operator boundaries

The browser continues to display executor and hold information. Unsupported
non-Claude mutations remain disabled; use supported CLI controls. No installed
factory services are changed by building or testing this source.

Keep the optional Z source and its incomplete native acceptance gates separate.
Codex does not require Zed to be installed. Neither offline Codex checks nor its
eventual acceptance receipt certify the Z executor.

No review requests or review messages from this effort go to Slack. A disposable
sample epic must run through the factory; manually implementing its sample code
does not count as acceptance.

## Version 2 control recovery

The stored contract version selects one shared tool catalogue, decoder and runtime
instruction supplement. Request payloads cannot select a different catalogue.
Existing prompt snapshots remain immutable. Claude and the separate Z executor
keep their existing behavior.

Version 2 gives GitHub credentials and writes to Factory. Repository helpers make
local changes and commits. Coordinators submit declared proposal files through
`effect_prepare` and inspect the recorded operation through `effect_result`.
An operation also requires its actual selected source. A missing producer keeps
that operation unavailable; it does not enable the old direct-write path.

Each coordinator call retains its original canonical request and actual attempt,
thread, turn and call identity. Current caller authority is checked before an
input error can become corrective guidance. A final local authority check runs
before control enrollment or a domain mutation. Network and process work runs
outside the owner and helper locks.

Broker admissions, local output commits and programme-owned reads retain their
own factual receipts. Recovery reads those facts and completes the existing
control. It does not repeat a submit, continuation, append or uncertain GitHub
write. A cancellation receipt means that cancellation was requested. It does not
claim that the helper has stopped. A read which never started has a distinct
no-action receipt, with no fabricated process or result reference.

Output recovery preserves its original writer birth, reservation, source hash and
current owner revision. A work fence resumes its already admitted stop with the
same token. A pending coordinator restart can wait for an admitted output to
finish. It cannot grant new work or write authority to a changed owner.

Private source snapshots allow bounded canonical JSON expansion. Public requests
and ordinary effect records retain their existing limits. These controls are
source mechanisms. Full version 2 launch qualification still requires the actual
source producers, dependent workflow and live acceptance checks.
