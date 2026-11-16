# Claude compatibility driver

Status: the four explicitly owned Go harness paths can now replace older
tracked versions in a staged revision. This supersedes the prior byte-equality-
only harness policy, so ongoing regression checks can use an older committed
harness baseline. Original committed bytes/hash/mode remain recorded and
verified before overlay; replacements are explicit provenance, never trace
normalization. All 61 Python tests passed with `-B`.

The parent's independent comparison at
`/tmp/factory-claude-parent-run-ars3oppw/comparison` predates this driver policy
change. It is historical evidence, not verification of a future published HEAD.
The parent/CI owner should run the full workflow against the actual committed
`HEAD`. No working-repository commits, pushes, Go-owner edits or CI workflow
edits were performed for this fix. Live auth/models/Claude/Docker/GitHub and
native dispatch acceptance remain outside this deterministic driver task.

## Modes

From the existing worktree:

```sh
python3 -B scripts/claude-compat.py --compile-only --baseline 68e810b49b6f215df5b265d41a72fb3ebee9c91b --candidate HEAD --output NEW_ABSOLUTE_DIR
```

Replace `NEW_ABSOLUTE_DIR` with a nonexistent absolute directory whose parent
exists, outside every source checkout (including the primary clone). Keep output private and uncommitted. An existing
output is rejected, including failed or partially completed runs. Omitting
`--compile-only` still compiles only. There is no reuse-binaries or golden-refresh
mode. `--baseline` defaults to local `origin/main`; `--candidate` defaults to
`HEAD`. Both become full committed SHAs before staging. The baseline must be an
ancestor of the separately frozen local `origin/main`. `--refresh-main` explicitly
fetches `origin main` before pinning; otherwise no network fetch occurs.

For the now-authorized deterministic captures, replace `--compile-only` with
explicit `--run` and use a different new output directory. This builds all lanes afresh, then executes
captures and candidate-only sentinels. The main reference comes only from its
actual main binary. Nothing writes or refreshes a reference from candidate data.

The three frozen lanes are:

- `main-default`: committed main plus shared new harness only; flagless intake.
- `candidate-default`: committed candidate plus shared and candidate harness;
  flagless intake.
- `candidate-explicit`: the same candidate revision and harness; explicit
  `--executor claude` intake through the candidate bridge.

Each lane compiles both packages with exactly this command shape:

```text
go test -c -o ABSOLUTE_BINARY ./internal/reconcile
go test -c -o ABSOLUTE_BINARY ./cmd/factory
```

Compile-only never enters the binary-execution branch. It does not use
`go test -run '^$'`. A successful compile-only result records
`status: compiled-only-NOT-EXECUTED` and `compatibility_pass: false`.

In `--run` only, each binary runs from its corresponding **package source
folder**, not the binary folder:

```text
ABSOLUTE_BINARY -test.run=^TestClaudeCompatCapture$ -test.count=1 -test.timeout=90s
```

Candidate lanes additionally run:

```text
ABSOLUTE_BINARY -test.run=^TestClaudeCompatCandidate -test.count=1 -test.timeout=90s -test.v
```

The candidate selection must report a passing top-level sentinel. An absent or
all-skipped sentinel fails. Shared captures must produce both raw and normalized
artifacts. Main never receives a candidate-only overlay insertion or executor argument.
If pinned main already tracks a candidate-specific harness file after merge,
its staged copy uses the current agreed test under the same audited four-path
policy. Its original bytes stay in the source manifest. An absent candidate-
only file is never inserted into main. The driver still selects only the shared
capture test in main, never the candidate-guard prefix.

## Publication and CI contract

After the parent commits the tests, use the actual new `HEAD`, not the older
production commit plus an uncommitted overlay. Replace `--compile-only` with
`--run` and choose a fresh output directory. Main's and candidate's actual
committed production source stay unchanged. The same current shared harness
applies to both; differing older versions of the four named harness files may
be replaced only in the owned staged trees, with original/new provenance below.
Candidate-only files apply to candidate and, on main, only when already tracked
there. Existing fixtures do not receive this replacement permission.

The CI owner runs this on PRs and main pushes. Required inputs:

- Check out the intended candidate as `HEAD` and fetch the explicit baseline
  commit plus local `origin/main` history needed for ancestry checks.
- Set `CLAUDE_COMPAT_BASELINE_SHA` to the full baseline commit SHA. Select it
  explicitly for each event (PR base or intended pre-push production baseline),
  not from a moving implicit reference inside the driver. Reject a missing or
  all-zero SHA rather than claiming an initial-push comparison.
- Provide `RUNNER_TEMP`, a local Go toolchain, and the prewarmed module cache.
  Driver compilation stays offline/local-toolchain. Run Python commands with
  `-B` so dirty-source checks are not defeated by generated bytecode.
- Run `--candidate HEAD` and upload the output's manifests/logs/raw/normalized
  traces on failure or success. No golden refresh or live model services.

The job can allocate its new output from the environment without shell
substitution:

```python
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

baseline = os.environ["CLAUDE_COMPAT_BASELINE_SHA"]
if not re.fullmatch(r"[0-9a-f]{40}", baseline) or baseline == "0" * 40:
    raise SystemExit("explicit nonzero baseline SHA required")
parent = Path(tempfile.mkdtemp(prefix="claude-compat-", dir=os.environ["RUNNER_TEMP"]))
output = parent / "comparison"  # Must not already exist.
print("compatibility artifacts:", output, flush=True)
subprocess.run(
    [sys.executable, "-B", "scripts/claude-compat.py", "--run",
     "--baseline", baseline, "--candidate", "HEAD", "--output", str(output)],
    check=True,
)
```

The job's output may be long; sockets still use the separate short private
runtime tree. Artifact retention and runtime ownership remain unchanged.

## Source and environment safety

Run Python commands with `-B`, including unittest discovery, so importing the
driver or test module does not write bytecode into this source worktree. `-B`
prevents new bytecode writes; it does not delete existing cache files. Existing
generated files must be removed by their owner before staging. The driver still
rejects unagreed dirty files, including arbitrary `__pycache__` contents. There
is no new ignore rule or dirty-source exception. The parent owns removal of the
two bytecode files created by its earlier invocation.

The exact overlay inventory is frozen in `SHARED`, `CANDIDATE`, and `FIXTURES`
in the driver. Currently no optional fixture files are agreed (`FIXTURES = ()`).
Adding a fixture requires coordinating its exact path with owner A and updating
that constant; the driver does not glob extra fixtures into either revision.

Shared files:

- `internal/reconcile/claude_compat_test.go`
- `cmd/factory/claude_compat_test.go`

Candidate-only files:

- `internal/reconcile/claude_compat_candidate_test.go`
- `cmd/factory/claude_compat_candidate_test.go`

The replacement permission is exactly these four `.go` test paths, not a
pattern covering other tests, fixtures, scripts or production files. Current
working overlays must be regular mode `0644`, with exact declared inventory.
New paths are inserted; matching committed bytes/mode are reused without a
rewrite; differing committed versions of these four paths alone are staged as
`replaced_test_harness`. Prior regular executable mode is also audited when the
new staged mode becomes `0644`. Git symlinks and submodules remain forbidden.

Original tracked fixture behavior is stricter. An explicitly agreed fixture
under `internal/reconcile/testdata/claude_compat/` may be inserted if new or
reused if its committed bytes and mode `0644` exactly match. Different existing
fixture bytes or modes fail; they are never overwritten. Unagreed production
fixtures and ordinary existing test files are ineligible even if identical.
There is no general tracked-file overwrite or ignore rule.

Dirty working changes remain limited to the exact declared overlay paths and
the driver's three owned files. Production changes, unagreed files, renames,
deletions and conflicts fail. Git replacement objects remain disabled during
all original-blob verification. The optional fixture directory is checked even
if ignored; exact inventory is required, not a glob. No secrets or unrelated
runtime files are copied.

The driver uses `git -c tar.umask=0022 archive` without changing the checkout,
branches, or worktrees. The command-local tar umask keeps archived file modes
consistent with Git's tracked executable bit. It independently reads the committed file inventory and Git blobs.
Archive contents must match those blobs exactly. Archive links, submodules and
unsafe paths fail closed. It hashes staged untouched files before copying any
overlay. Exact tracked matches are left in place. New paths use exclusive
creation. For the four replaceable paths only, a separately created file is
atomically installed over that staged test copy, never over the checkout or a
Git blob. Full original-tree hashes/modes are verified before the first overlay.
Afterward the expected inventory changes only at declared overlay paths; every
other production/source entry must retain its original hash and mode. Original
source manifests and current overlay hashes stay independent.
Compilation and runtime must not modify the staged source. Overlay bytes must
still match the original snapshot before the result is written, including after
runtime retention checks. The current driver hash is checked too. A rebuilt result is
required whenever the overlay changes after an earlier compile. Do not cite an
old overlay hash as current-head verification.

Each invocation exclusively creates a short `fc<random>` directory under the
resolved `/tmp` parent. Each package/lane gets its own numbered child there,
with separate owned HOME, TMPDIR, brain, socket, config and cache directories.
Capture paths never derive from the possibly long source/output path. The
`world.listen` socket budget includes the longest Go `socks<uint32>` suffix and
a 20-byte PID plus `.sock`; its full encoded path must stay below 104 bytes.
This is a source/path-budget check, not a claim that socket tests have run.

Before and after compiler/capture processes, the driver checks UID, mode `0700`,
device and inode for its root and package-private directories. Symlinks and
foreign replacements fail the checks. These ownership checks are observations,
not authority to delete a pathname: another same-UID process can replace a real
directory after `lstat`, so path-based recursive cleanup is intentionally absent.

The driver **never recursively deletes its runtime tree**, on success or error.
It retains that private tree as an artifact for manual cleanup. The exact path
and original ownership are recorded in `runtime-ownership.json` and package
runtime manifests. On exit with matching ownership, `runtime-retention.json`
records `status: retained`, the identity and `manual_cleanup_required: true`;
the exact retained path is also printed. A successful `result.json` records
`runtime_status: retained` and the root's path/UID/mode/device/inode. If ownership
has changed, the driver raises an error without deleting either the original
or its replacement; the initial ownership record remains for inspection.
There is no cleanup subprocess, timer, recursive remover or cleanup-success
claim. Retention does not keep a helper or service running. Go's own test/temp
helpers may still clean their internal fixtures; the driver does not delete
the containing private runtime directories.

Retained artifacts consume disk until manually removed. Inspect the exact path
and its current ownership before any manual cleanup; never apply a wildcard or
global temp-directory sweep. Staged source, binaries, logs and raw evidence stay
in the requested output. The output build cache remains separate from capture
runtime directories. The driver constructs an environment from scratch. Ambient
API tokens, SSH sockets, proxy variables, Go flags, and credential paths are not
inherited. It does not read live brain files or credential stores.

Every driver Git command runs through the same clean-environment helper with
both `GIT_NO_REPLACE_OBJECTS=1` and `--no-replace-objects`. This includes fetch,
revision resolution, ancestry, archive, tree inventory and blob reads. Private
build/capture environments also set `GIT_NO_REPLACE_OBJECTS=1`. Caller-supplied
Git object-directory, replacement-ref-base and configuration-injection variables
are not inherited. A local `refs/replace/<original-SHA>` therefore cannot make
both archive and tree/blob queries agree on a replacement while the manifest
still names the original commit.

Compilation uses the local Go executable, `GOENV=off`, `GOTOOLCHAIN=local`,
`GOWORK=off`, `GOPROXY=off`, `GOSUMDB=off`, `GOFLAGS=-mod=readonly` and disabled
CGO. An existing local module cache is required: explicit `GOMODCACHE`, otherwise
`~/go/pkg/mod`. Its path is recorded. The build cache belongs to this output.
Missing modules/toolchains fail rather than download. Compiler commands have a
bounded timeout (`--timeout`, default 180 seconds). Capture commands have an
outer bound of at most 100 seconds. Timeout/interruption kills the process group.
This environment isolation is not an OS security sandbox for hostile Go tests;
review of owner A's credential-free fake boundaries remains necessary.

`provenance.json` is written before compilation. It preserves revision IDs,
ancestry, original source hashes/Git blob IDs/modes, overlay hashes and driver
hash. The original `sources.main` / `sources.candidate` records keep their
`sha256`, `mode`, and `git_blob`. Each originally committed one of the four Go
harness files additionally has `content_base64`: its exact original blob bytes,
not the replacement. This makes the original source recoverable after staging.

`provenance.json` and `result.json` now have manifest `schema_version: 2` for
this changed audit protocol. Trace schema version remains `1`.
`lanes.<lane>.overlay_disposition.<path>` is now an object, not a string. The
same objects are saved in that lane's `overlay-disposition.json`:

| Field | Meaning |
| --- | --- |
| `disposition` | `reused`, `inserted`, or `replaced_test_harness`. The last value is permitted only for the four exact paths. |
| `prior_sha256` | Original committed content hash, or `null` for insertion. |
| `new_sha256` | Current agreed overlay content hash. |
| `prior_mode` | Original regular-file mode as a JSON integer, or `null`. |
| `new_mode` | JSON integer `420` (`0644`). |
| `prior_git_blob` | Original Git blob ID, or `null` for insertion. |

An unchanged hash with a changed mode still records `replaced_test_harness`.
`staged` records the expected post-overlay hashes/modes; it does not replace the
original `sources` records. Candidate-only paths absent from main have no main
overlay record. Candidate-only paths already in main can be reused or replaced,
but their guard tests are never selected in main. Raw/normalized trace schemas
and timestamp identity rules are unchanged by this manifest protocol update.

`toolchain.json` records Python/Git/Go versions, executable hash and module
cache. `commands.jsonl` records each build/runtime argv, cwd and clean environment
before launch. Per-package logs preserve compiler/capture output. A successful
`result.json` adds staged manifests, binary hashes and, for runs, artifact hashes.
A failure leaves its partial artifacts for inspection, never as a passing result.
Fixture clocks come from owner A's fixed fixtures. The only nonfixed-clock
exception is the verified worker result described below; other timestamp values
are never removed or generically normalized.

## Mandatory raw envelope: integration contract for owner A

`FACTORY_CLAUDE_COMPAT_TRACE` names the normalized trace. Write the original raw
artifact to that exact path plus `.raw.json` **before** normalization. Both must
be regular files. The normalized trace follows the exact schema/inventories in
`interfaces.md`. The driver enforces all nine reconcile scenarios and the CLI
intake scenario, every named step, nested envelope types and nonempty assertions.
Each step needs actual evidence. Each scenario needs observed operations and
relationships; equally empty traces cannot pass.

The raw envelope's exact fields are:

```json
{
  "trace": {"schema_version": 1, "package": "cli", "scenarios": []},
  "roots": {
    "/absolute/owned/runtime/home": "<HOME>",
    "/absolute/lane/source": "<SOURCE>"
  },
  "identities": {},
  "cli_inputs": {
    "intake/defaults": ["add", "owner/repo#1"],
    "intake/overrides": ["add", "owner/repo#2", "--model", "opus"]
  }
}
```

The empty `scenarios` above illustrates only the envelope; it is deliberately not
a valid capture. `trace` must contain the complete original pre-normalization
trace. It must not already contain normalization tokens. No revision/lane
metadata belongs in either trace; that belongs in the driver's manifest.

`roots` is checked against the actual makers in shared `newWorld` and
`cliCompatNew`, not against a general "somewhere inside private" predicate.
The existing raw envelope is unchanged; no maker fields are trusted from it.
The driver's hashed, reviewed harness source defines this frozen layout:

- Exactly one `<SOURCE>` root, equal to the pinned source-tree root.
- A single direct `TMPDIR/TestClaudeCompatCapture<digits>` parent, as created
  by Go `testing.TempDir`. It has exact three-digit child indices.
- Reconcile: nine worlds in frozen scenario order. Odd indices `001` through
  `017` are `<BRAIN>`; the following even indices through `018` are `<HOME>`.
  Each scenario must use its own pair and not another world's roots.
- CLI: one world shared by both intake steps, with `<BRAIN>` at `001` and
  `<HOME>` at `002`.
- Reconcile additionally declares exactly nine distinct direct
  `TMPDIR/socks<digits>` children, matching `os.MkdirTemp("", "socks")`, as
  `<SOCKETS>`. CLI declares no socket root. Go's random uint32 suffix is at most
  ten decimal digits.

The exact token counts and parent/child shapes must match. No root may shadow
another. Clone children such as `home/src/r`, workspace descendants, a second
test parent, swapped roles, unused extra roots, `<WORKSPACE>` or `<EXECUTABLE>`
mappings fail against the current maker inventory. Although those latter tokens
exist in the broader normalization vocabulary, the actual frozen Go harness does
not emit them. Repository/worktree suffixes therefore remain visible after
normalization. The validated inventory is retained in a separate driver output
file, never added to trace/raw envelopes. Go cleanup may already have removed
fixture directories when verification runs; this contract derives their exact
layout from the pinned maker source, not a raw self-declared marker or signature.
Any new Go temp allocation or root-maker layout requires explicit coordination
with B before this contract can change.

`identities` is mandatory. It is `{}` or an object containing only the optional
`worker_event_times` and `program_event_times` arrays. Their entry schemas are
exact and different; no other identity transform is allowed. Never mask UUIDs,
leases, models, refs, roles, durations or unrelated status fields.

```json
{
  "worker_event_times": [
    {"scenario": "worker_failures", "step": "prepare_retry", "entity": "item-1", "seq": 12,
     "at": "2026-10-18T12:00:00.123456789Z"}
  ]
}
```

This narrow exception covers actual `RepoWorker` result timestamps assigned by
`events.Append` using `time.Now`, outside `Deps.Now`. Each entry must identify
exactly one original event in its scenario/step/entity snapshot. `seq` is a
positive JSON integer, not a boolean or float. The original `at` must match the
entry exactly and be RFC3339. The event must have sender `factory`, kind
`worktree_ready`, `done` or `error`, and key `repo:<trigger_ref>`. Its canonical
numeric `trigger_ref` must name exactly one earlier request in the same entity
log. That request must have kind `request`, recipient `repo-worker`, and request
`worktree` or `cleanup`. A success kind must match the requested operation.
Both array order and sequence numbers must put the request first. Result `at`
must not precede request `at`; comparison preserves nanosecond precision and
accounts for timezone offsets.

Every worker result in every captured snapshot needs its own mapping. Missing,
duplicate, absent-target, wrong-type, unknown-field or wrong-provenance mappings
fail closed. The driver copies the raw trace and changes **only** each verified
result event's `at` to `<WORKER_AT:entity:seq>`. Originals remain in the raw
artifact. The same timestamp appearing in a request, post, event text or state
record is not replaced. Preexisting worker tokens in raw evidence fail; a wrong
emitted token fails independent normalization comparison. Root normalization is
still separately scoped. No global timestamp substitution occurs.

### Additional source-verified program event clocks

Both pinned revisions call `run.append`/`once` without `At` at the source sites
enumerated in `interfaces.md`. The resulting wall-clock timestamps are outside
`Deps.Now`. Only these typed families are eligible:

| Family | Required kind/key and evidence |
| --- | --- |
| `checked_plan` | Epic `done`, `checked-plan:E`; earlier ended `epic.vN.yaml` in that epic directory; exact full plan bytes/hash and checked text. |
| `mirror` | Epic `done`, `mirror:E`; same ended plan, earlier applied-plan transition, and exact dependency-link count/text. |
| `assignment` | Work `done`, `assign:workID`; earlier start plus actual successful same-tick current-user/issue reads and, when needed, the matching assignment call. |
| `handover` | Work `handover`, `handover:T`; earlier `pr_open` transition and that item's ended `pr.md`, with the supported source reason. |
| `cancel_done` | Work/set/epic `done`, `cancel:T`; earlier cancelled transition caused by a stop request, with its exact reason. |
| `stop_propagation` | Child work/set `request`, `stop:parent:T`; cancelled parent transition/request, parent-child ownership, and exact stop payload. |
| `cancel_cleanup` | Work cleanup `request`, `repo:cleanup:cancel:T`; cancelled transition, earlier worktree request, unadopted work and exact recipient/text. |
| `cancel_unassign` | Work `done`, `cancel:T:unassign`; cancelled transition, prior factory assignment and same-tick issue-removal call. |
| `stop_refused` | Cancelled work `refused`, `refused:R`; earlier stop request, cancelled state and exact refused from/to/trigger/ref/text. |
| `cancel_message` | Set `message`, `cancel:T:tell`; owned cancelled work and its stop reason, plus the set's orchestrator session. |

Each entry is exactly:

```json
{"scenario":"worker_failures","step":"cleanup","entity":"e2-w1","seq":10,
 "at":"2026-10-19T03:07:33.23468Z","family":"cancel_done"}
```

The driver independently verifies these source-specific relationships from raw
evidence. Every eligible event in every snapshot needs a mapping. Entries must
resolve to unique contiguous-log event sequences with the original exact `at`.
They must have sender `factory` and the family's exact kind/key shape. Fixture
markers in `relations["fixture_event:entity#seq"]` or fixture-input labels make
an event ineligible even when its sender is forged as `factory`. Unknown
families, malformed entries, missing/duplicate targets, worker/program overlap,
and preexisting tokens in raw traces fail closed.

A supplies these observations through the existing `relations` map, without
new trace/raw envelope fields:

- `plan_content:entity#E`: complete bytes of the ended plan; its SHA-256 must
  match the original end event. The Python decoder accepts only the agreed
  flat flow-map item/link format, not arbitrary YAML; unsupported new shapes
  fail rather than being guessed. Full plan content is retained and compared.
- `event_tick:entity#seq`: the tick that actually appended the factory event.
- `tick_calls:N`: JSON `{ "start": I, "end": J }`, a bounded half-open range
  in that scenario's original command history.
- `read_call:I`: complete read-result JSON. Assignment verification requires
  `kind: me`/`issue`, exact call index/tick/repository/issue, successful original
  stdout and the corresponding unmodified `gh` argv. The `pr` field carries
  the issue number for issue reads. Wrong/stale/failed reads cannot authorize
  an assignment timestamp mapping.
- `index:entity` and `inputs:entity`: actual entity kind/ownership and persisted
  inputs used for work/set/session links and issue targeting.

Only a verified target event's `at` becomes `<PROGRAM_AT:entity:seq>`. Worker
and program verifiers both read original values before either set of tokens
can obscure a referenced timestamp. Raw artifacts retain all original clocks.
The same time in a post, event text, unknown event field or status record is
not changed. No statuses, durations, timeouts, models, efforts, leases, refs,
responses, decision payloads, dependencies or causal action ordering are erased
or relaxed. Referenced event timestamps are checked for causal order at exact
fractional-second precision. Native/live acceptance remains a separate question.

For reconcile, `cli_inputs` is exactly `{}`. For CLI, it has exactly the two keys
shown above and preserves the actual unnormalized input argv used for each
intake case. The input array excludes any executable name if runAdd receives
only arguments; preserve precisely the arguments passed to that function.
Use semantic action labels in normalized evidence `inputs`, not raw flags.

Only candidate-explicit may add exactly one adjacent `--executor`, `claude`
pair. Removing that pair must leave main's complete argument sequence unchanged,
apart from independently verified root substitutions. `--executor=claude`, a
second pair, different models, reordered arguments, or other flags fail. This
exception applies **only** to `cli_inputs`. Actual program subprocess argv,
environment arrays, launch-file contents, socket posts, events, state records,
leases, owners, roles, refs and models remain fully compared.

Independent normalization replaces only declared root prefixes at path
boundaries, including artifact map keys and embedded file contents. Path suffixes
are retained. A substitution that collapses map keys fails. All fields and array
order are preserved; JSON map ordering alone is ignored. Owner A must use the
same boundary rule and keep independent-entity observations under stable keys.
The driver never sorts causal actions or intersects scenario inventories.
`relations["read_call:<index>"]` remains its complete JSON string, including
`call_index`, `tick`, `kind`, `repo`, `pr`, `sha`, `stdout` and `error`. Root
substitution applies inside that string without dropping fields or parsing it
into a lossy summary. Response repository/ref/status/error differences and
repository/worktree path suffixes remain compared.
It re-normalizes the raw trace itself and compares it to the emitted trace
before comparing either candidate to main. A first differing JSON path reports
where comparison failed. Duplicate JSON keys and nonfinite numbers fail.

Personal repositories must have automatic review requests disabled in both
frozen Go fixture configurations. The driver additionally rejects observed
GitHub reviewer assignments and recognized Slack review-request commands instead
of deleting them from comparison. The guard covers `gh pr create --reviewer`,
its equals form and `-r` forms, `gh pr edit --add-reviewer` forms, REST POSTs to
`requested_reviewers`, and GraphQL `requestReviews`. Default API POST selection
from body fields is accounted for. Reading requested reviewers and authoring a
review (`gh pr review`, REST `/reviews`, GraphQL `addPullRequestReview`) are not
reviewer assignment and remain substantive trace operations. The Go harness must assert the promised causal
ordering and native-isolation checks; assertion labels alone are not proof.
`fakeDeps` does not establish production `newDeps` behavior. Fake Claude events
prove program behavior around delegated work, not actual model compliance.

## Deterministic verification

The authorized offline Python tests are in `scripts/test_claude_compat.py`.
Run them separately, without generating bytecode, with:

```sh
python3 -B -m unittest discover -s scripts -p test_claude_compat.py -v
```

The expected full scenario/step inventory is an independent literal in the
Python tests, not copied from the driver. Removing `usage_policy/reset` from the
driver is an explicit regression case. The tests also cover long-output socket
budgets, ownership/mode/symlink refusal, retained-directory/no-deletion behavior,
replacement-ref provenance, exact maker-root inventories and clone
shadow attacks, all reviewer assignment forms versus review authoring,
malformed/empty schemas, raw normalization,
model/effort/role/owner/lease/ref/dependency and ordering drift, exact CLI flag
rules, worker event/request timestamp provenance and tampering, archive/overlay
provenance, environment isolation, and mocked default-mode
proof that only six compiler invocations occur and no binaries execute.
The mocked proof is not real Go compilation or a compatibility result.
Future capture results and controlled mutations remain required. No live auth,
models, Claude, GitHub, Docker, CI, commits, pushes or review requests are part of
this implementation step.

The replacement-ref regression uses an isolated bare local Git fixture. It first
shows ordinary Git reading a substituted revision, then requires driver queries
to read the original committed bytes. The fixture uses local object/ref plumbing
only, with no remote, checkout or live repository changes. It is covered by the
Python suite; no remote GitHub interaction is involved.
