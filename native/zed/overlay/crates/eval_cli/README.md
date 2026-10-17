# eval-cli

Headless Rust binary for running Zed's agent in evaluation and benchmark
environments. It is designed for containerized harnesses such as
[Harbor](https://harborframework.com/) and Pier, where the repository is already
checked out and model API keys are provided via environment variables.

`eval-cli` uses the same `NativeAgent` + `AcpThread` pipeline as the production
Zed editor: a full agentic loop with tool calls, subagents, and retries, without
a GUI.

This directory also contains `zed_eval/`, the Python `zed-eval` package used to
build this binary, launch remote benchmark runs on Modal/Harbor/Pier, and fetch
results. For normal benchmark orchestration, start with
[`zed_eval/README.md`](zed_eval/README.md).

## Building

### Native, for local testing on the same OS

```sh
cargo build --release -p eval_cli
```

### Linux x86_64, for Harbor/Pier sandboxes

Harbor and Pier containers run Linux x86_64. From the repository root, use the
Docker-based build script:

```sh
crates/eval_cli/script/build-linux
```

This produces `target/eval-cli`, an x86_64 Linux ELF binary. You can also
specify a custom output path:

```sh
crates/eval_cli/script/build-linux --output ~/bin/eval-cli-linux
```

## Standalone usage

```sh
eval-cli \
  --workdir /testbed \
  --model anthropic/claude-sonnet-4-6 \
  --instruction "Fix the bug described in..." \
  --timeout 600 \
  --output-dir /logs/agent
```

`eval-cli` reads provider API keys from environment variables such as
`ANTHROPIC_API_KEY` and `OPENAI_API_KEY`. It writes `result.json`, `thread.md`,
and `thread.json` to the output directory.

To keep application data and configuration in a separate directory, add
`--user-data-dir /tmp/zed-eval-data` to the command above. The directory is
created if needed, with configuration under `config/` inside it. This does not
isolate native credential storage or all OS cache, log, and temporary paths.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Agent finished |
| 1 | Error, such as model/auth/runtime failure |
| 2 | Timeout |
| 3 | Interrupted by SIGTERM or SIGINT |

## Running benchmarks

Most benchmark runs should use the Python `zed-eval` CLI instead of invoking
`eval-cli` directly. From the repository root:

```sh
crates/eval_cli/script/install-zed-eval
zed-eval doctor --create-volume
zed-eval run rf --from local --n-tasks 2
```

For one-off source runs without installing the tool globally, use
`crates/eval_cli/script/zed-eval <args>`.

See [`zed_eval/README.md`](zed_eval/README.md) for supported benchmarks, remote
builds, Modal setup, reporting, rejudging, baselines, and Harbor/Pier installed
agent usage.

## Native subscription usage probe (local experiment)

`zed-native-usage-probe` is a separate executable. It does not initialize the
agent, models, telemetry, or ordinary eval-cli startup.

```sh
cargo build -p eval_cli --bin zed-native-usage-probe
zed-native-usage-probe --user-data-dir /absolute/path/to/disposable-probe-data
```

The data directory is required and must be an absolute, writable UTF-8 path.
Keep the real HOME unchanged. The directory isolates application data/config,
not native credential storage. Release-channel initialization selects the same
native credential provider as Zed. No credentials are exported or written.
`--help` exits before initialization or credential access.

The probe loads native persisted credentials and performs one fresh-only GET to
the first-party subscription usage endpoint. It does not discover models,
refresh tokens, sign in, retry, run inference, or enable paid fallback. Expired
credentials return `refresh_required` without an HTTP request. Headers and the
complete body share a 15-second deadline. Bodies larger than 64 KiB are rejected.

Stdout contains one sanitized JSON envelope. `status: "ok"` includes usage;
`status: "error"` includes a fixed error category. Exit status is zero only on
success. The CLI prints no identity, credentials, raw backend bodies, or provider
errors. Parse errors for command-line arguments follow clap's normal help output.

This is observation only, not an admission policy. Flags remain independent of
percentages. Missing windows mean unknown, and window durations are not assumed
to identify short/long buckets. `observed_at_ms` is the local observation time,
not a backend freshness guarantee. Account binding is request-scoped and response
identity is unverified unless both account IDs were present and matched. Native
endpoint access still requires a separately reviewed live proof. Even a successful
read does not prove model-to-quota attribution or safe production dispatch.

## Native lifecycle proof (local experiment)

`zed-native-lifecycle-probe` retains one native session independently of its
Unix-socket clients. This is not a production worker or an external Codex runner.
It registers only the native ChatGPT subscription provider. The model must be a
discovered `openai-subscribed/gpt-6.1-sol`; primary, summary and compaction settings
are pinned to that model with standard speed. Tools, MCP and subagents are off.
Loaded sessions with another effective model, profile or speed are refused.
The probe sets the global `disable_ai` switch before constructing its project.
This blocks background MCP startup from workspace settings, including commands and
HTTP servers. Explicit native submissions still run; ordinary eval-cli settings
are unchanged.

Required absolute flags are `--user-data-dir`, `--workdir`, and `--socket`.
`--load-session ID` loads saved native history without replaying a prompt.
Keep the original HOME for native subscription access. Use a persistent isolated
data directory, not the installed editor's data/config directory. `ZED_STATELESS`
is refused, including when it is present with a false-looking value. The data and
socket directories must be owned by the current user with mode `0700`; a new
immediate directory is created with that mode. Parents must already exist.
The socket is mode `0600`, and its path is limited to 103 bytes. Use a short path.
An existing endpoint is never removed or taken over. After confirming the old
process is dead, the caller must remove its stale socket before restarting.
The hidden CLI-only `--printenv` supports native shell environment capture.
It is not a socket operation.

Send UTF-8 JSONL frames, each at most 64 KiB including its newline:

```json
{"v":1,"id":"inspect-1","op":"status"}
{"v":1,"id":"prompt-1","op":"prompt","text":"Remember this nonce."}
{"v":1,"id":"stop-1","op":"interrupt"}
{"v":1,"id":"save-1","op":"checkpoint"}
{"v":1,"id":"quit-1","op":"shutdown"}
```

Unknown fields, versions and operations are rejected. Slash-command prompts are
refused. Replies have `v`, `id`, `ok` and either `result` or a fixed `error.code`.
Prompt acknowledgment means admission, not completion. It returns `turn_id`;
poll status for `last_turn_result`. Only `completed` is a successful native turn.
Status includes a process epoch, sequence, session/turn IDs, state, live native
Markdown transcript, effective model/profile/speed and observed tool count.
Transcripts above 96 KiB are explicitly marked `transcript_truncated`. Responses
are capped at 1 MiB. Treat both the socket and its transcript as private.

Identical admitted prompt IDs return the original acknowledgment. A changed
payload is rejected. The process retains at most 128 prompt admissions and rejects
new IDs when full. Status and termination controls do not consume this cache.
Control retries while a control is active return `busy`; polling status remains
available. There is no request replay or deduplication across process restarts.
The queue holds 32 requests plus its sender's reserved slot. There are at most
eight socket clients. Reads/writes have short deadlines; control replies have a
bounded wait. A disconnected client does not cancel an admitted native turn.

Interrupt drains native cancellation, ACP cancellation and the retained
Submission. A cancellation timeout holds the session and refuses further work or
successful shutdown. Checkpoint requires an idle/drained turn and awaits
`NativeAgent::flush_session`. Shutdown freezes admission, drains, awaits that same
barrier, attempts its acknowledgment and only then requests GPUI quit. A failed
save is never acknowledged as success. A failed shutdown save leaves admission
frozen but permits retrying shutdown. Abrupt death only recovers persisted history;
it does not recover an executing turn.

Offline fixtures use a fake native provider installed before constructing any
real provider. They do not authenticate or call a live model. In-memory native
reload tests verify policy checks, not process-restart durability. That proof and
live subscription/quota checks belong to the parent experiment.

## Factory-owned native credentials (opt-in experiment)

`zed-native-credential-setup` uses the existing native OAuth sign-in, including its
unchanged `openid profile email offline_access` scopes and callback configuration.
It does not import desktop tokens or use Go OAuth. The only accepted namespace is
`factory-openai`. It maps internally to the fixed Keychain URI
`https://factory.zed.native/openai-subscribed/v1`; the URI is not a public
namespace identifier or accepted CLI value. The normal IDE key is unchanged.
An isolated application data directory alone does **not** isolate credentials.

Build with the pinned toolchain (no installation):

```sh
CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=false \
CARGO_TARGET_DIR=/absolute/path/to/selected-cargo-target \
rustup run 1.98.1 cargo build --locked -p eval_cli \
  --bin zed-native-credential-setup --bin zed-native-usage-probe
```

For a user-authorized setup only, pass `--sign-in`. This is the only command that
opens a browser. These are invocation templates, not instructions to run live auth
during offline validation:

```text
zed-native-credential-setup --user-data-dir <absolute-private-data-directory> --credential-namespace factory-openai --sign-in
zed-native-credential-setup --user-data-dir <same-directory> --credential-namespace factory-openai
zed-native-usage-probe --user-data-dir <same-directory> --credential-namespace factory-openai --expected-identity <binding-from-authorized-setup>
```

Without `--sign-in`, setup only loads credentials and reports status. It never
refreshes or calls an account/model/usage endpoint. Successful JSON has `status:
"ok"`, `credential_namespace`, opaque `identity_binding`, and `refresh_required`.
Pin the binding from the authorized setup in the caller's private configuration.
Do not replace that pin automatically from later status or usage output. A new
sign-in rotates the binding even for the same account. Account changes require
explicit setup and reauthorization. No raw account, email, tokens, or OAuth grant
are returned. Missing setup is a fixed `missing_setup` error and must hold work.

Scoped usage requires both flags. It never signs in, refreshes, discovers models,
or falls back to the desktop key. Its success envelope retains `status` and
`usage`. `usage.identity_binding` appears only after the setup pin matches and the
response account matches the request account. A response without an account may
still return usage but has no binding; callers must treat it as unknown. This does
not establish model-to-quota attribution. All other quota/admission policy stays
with the caller. Legacy usage invocations without the new flags retain their
existing behavior and JSON. They are **not** the factory production path.

Factory credential reads have a five-second deadline and a fixed
`credential_load_timeout` error. The timed-out read future is dropped; it cannot
publish a late result into State. The OS Keychain operation itself may continue
on its background thread because native synchronous Keychain calls cannot be
forcibly cancelled. No write/delete follows a timed-out read. Setup waits at most
three minutes for sign-in, including persistence. CLI errors and panic output are
fixed categories, not provider errors, credentials, HTTP bodies, or headers.

Factory mutations use one process-held OS lock at the OS account's real home:
`~/.factory/native-auth/factory-openai/credential.lock`. It is independent of
`--user-data-dir`, brain selection and worker HOME overrides. The existing
`.factory` root is canonicalized, must be owned by the effective user, and must
not be writable by group/others; its permissions are not changed. The two auth
subdirectories require `0700`; the regular, single-link lock file requires `0600`.
Unsafe paths fail closed. Unix locking is supported; other platforms fail factory
mutations closed without changing normal IDE credentials. The lock file is never
removed or taken over and contains no credentials.

Setup persistence, refresh and deletion reload the native provider's stored grant
under this lock. The nonce and cached credential revision must still match.
`StaleGrant` requires reloading and rechecking the pinned identity, not replaying
old cached credentials. Explicit setup retains its loaded comparison until it
persists a newly randomized nonce. Refresh holds the lock from its pre-request
comparison through persistence or fatal deletion. Lock acquisition is bounded to
five seconds; scoped refresh is bounded to thirty seconds including lock,
HTTP, body reading and persistence. Its shared slot clears on timeout even when
all inference waiters were cancelled. Normal IDE refresh behavior is unchanged.

Before an external credential write/delete, the runtime creates and fsyncs
`mutation-intent.json` beside the lock and syncs the auth directory chain. The
intent contains only the operation/version and opaque grant, revision, and exact
intended-state digests. It never contains tokens, accounts, raw nonces, or credential
bytes. Identical writes and deletes of an already-absent grant skip the external
mutation and intent. Explicit setup still rotates the nonce, so it is not a no-op.

Timed-out external mutations retain the OS lock in a detached persistence task.
The durable intent also survives process death. A released lock, dead PID, elapsed
time, or native service error does not prove settlement. Before another actor can
load/use factory credentials, refresh, sign in/persist or delete, a fresh native
credential read must prove the exact intended state committed. Only that proof can
remove and fsync the intent. Delete settlement requires positively read absence.
Wrong values, malformed intents, read failures and missing proof return
`NativeAuthError::MutationUncertain` and keep the namespace held. No blind intent
erase, time-based unlock, desktop fallback or API fallback exists.

### Pending native mutation runbook

1. Keep the native runtime alive while `NativeCredentials::has_pending_mutation()`
   is true. It includes both in-process persistence and durable uncertain intent.
   Never unlink the lock or intent, overwrite the marker, or launch fresh sign-in
   as a way to bypass it. Process death does not establish service completion.
2. The native cleanup owner may call
   `settle_pending_mutation(&AsyncApp).await`. This only acquires the lock and
   reads native credentials to seek exact settlement proof; it never rewrites or
   deletes credentials. Live persistence may return `MutationLockTimeout`.
3. After a crash, invoking `zed-native-credential-setup` **without** `--sign-in`
   using the normal explicit data-directory and `factory-openai` namespace flags
   performs the same bounded native read/settlement check. It never opens a
   browser. On success, reload State and recheck the caller's pinned identity.
4. If `MutationUncertain` persists, leave work held. An intent may have been
   durably recorded before a request was issued, so exact settlement may never
   become provable. No automatic recovery/bypass is provided. Escalate for a
   separately reviewed, platform-specific service-settlement proof. Do not inspect,
   copy or export raw Keychain credentials or clear markers based on a timer.

Public lock lookup is now
`credential_lock_path() -> Result<PathBuf, NativeAuthError>`; lookup failure is a
fixed error, not an alternate lock location. `NativeCredentials::new` is unchanged.
Live Keychain ordering, power-loss recovery and OAuth grant independence still
require separately authorized acceptance testing. Offline tests use synthetic
credentials, temporary intent files, a fake service and a child that exits before
that service commits; they do not contact Keychain or OAuth.

Native integration APIs live in `openai_subscribed::native_auth`:

- Public `openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE` is
  `"factory-openai"`.
- `NativeCredentials::new(provider, FACTORY_CREDENTIAL_NAMESPACE)` validates the
  explicit namespace and wraps **all** native credential reads/writes/deletes.
- `State::new_native(http, wrapper, cx)` creates scoped state without discovery or
  implicit login. Keep one State for concurrent worker/inspection access.
- `State::load_task()`, `State::native_identity()`,
  `State::native_refresh_required()` expose loading and opaque status.
- `State::native_credentials()` returns the shared wrapper. Its `metadata()`
  exposes the namespace, random local nonce, and binding, never raw account/tokens.
- `State::discover_native_models(expected_identity, cx)` is the explicit managed
  startup catalog API. It returns `Task<Result<(), NativeAuthError>>`, checks the
  pin before and after discovery, and only publishes account-visible discovered
  models on success. Setup and usage do not call it.
- `State::read_native_usage(expected_identity, cx)` enforces the pin and checks
  binding again after the response. Normal native inference uses the same State's
  single-flight refresh. Refresh writes and fatal deletion stay in the wrapper's
  namespace. A refresh changing account is rejected before writing.
- `NativeCredentials::begin_authorized_setup()` is reserved for explicit setup,
  after loading and before `State::sign_in(cx)`. Do not call either from a worker.

`State::new` and its IDE discovery/sign-in behavior are unchanged. Lifecycle and
headless proof executables are not switched to factory credentials by this work.
A future worker must await loading, compare `native_identity()` to its pinned
expected identity before dispatch, and refuse missing/mismatched setup.

Separate keys and a local random binding do **not** prove independent server-side
OAuth grants. Desktop-to-factory and factory-to-desktop refresh, plus overlapping
access from separate processes, remain user-authorized live acceptance gates.
Single-flight refresh is only guaranteed within one shared State, not between
processes or independently constructed States. Do not enable production dispatch
until the live grant and concurrency gates pass; never substitute shared/API auth.
No real authentication, Keychain, browser, model, or quota calls are part of tests.


## Managed native runtime ABI v1

`zed-native-lifecycle-probe --capabilities` prints this fixed JSON before parsing
startup paths or constructing native state (including when other flags are present):

```json
{"schema_version":1,"native_runtime":"zed","managed":true,"containment":"docker","model":"openai-subscribed/gpt-6.1-sol","control_protocol":1}
```

Managed launch requires every flag below. All paths must be absolute. Proof mode
retains its existing flags, numeric turn IDs, and tool-free profile.

```text
--managed --user-data-dir ABS --workdir ABS --socket ABS
--job-id ID --generation N --instance-token NONCE
--identity-file ABS --result-file ABS --lock-file ABS
--credential-namespace factory-openai
--expected-identity OPAQUE --container-id FULL_CONTAINER_ID
--docker-bin ABS --factory-bin ABS --brain ABS --output-file ABS
--git-dir ABS --git-index ABS
```

`--load-session ID` remains available. Go must establish old host-process-group
and container writer death before loading a continuation. Native mode does not
create, inspect, stop or attest Docker containers. The passed full container ID,
Docker endpoint environment, binaries and mounts must already be attested by Go.
Only Git/temp child directories may be mounted from the private runtime tree;
never mount an identity/result/lock directory or native application data.
Git metadata must be separate from the assigned worktree, with its index inside
that private Git directory. Control paths must be canonical, private, distinct
and outside both writable mounts and the exact markdown output artifact.
The native data directory must not overlap the worktree or lie inside the Git mount.
Selected Docker/factory executables must also be outside model-writable worktree,
Git metadata and exact output paths; pinning a mutable executable is not containment.

### Identity, status and results

Before any credential loading, the process takes an exclusive non-takeover file
lock and atomically fsyncs a mode-0600 identity file under an owner-only mode-0700
directory. Existing launch identity/result files are ambiguous and are refused.
Lock files are never unlinked to acquire ownership. macOS process start time is
read from the OS; managed start identity is currently unsupported elsewhere.

Identity fields are exactly `schema_version`, `job_id`, `generation`,
`instance_token`, `pid`, `process_group`, `start_identity`, and `epoch`.
Go must not kill a recovered PID based only on these strings.

Managed status adds `managed:true`, `containment:"docker"`, `job_id`, `generation`,
`instance_token`, `identity_binding`, `container_id`, and the identity object.
The profile is `factory-managed-v1`; turn IDs are strings `epoch:counter` in
admission acknowledgments, status, checkpoint replies and result files.
`reply` is assembled only from current-turn assistant text blocks. It is not
parsed from user/tool Markdown. Status has an explicit `reply_truncated` display
flag. Persisted replies are complete or checkpoint fails; they are never silently
truncated into a successful result.

Successful checkpoint result:

```json
{"saved":true,"drained":true,"conversation_id":"native-session-id","turn_id":"epoch:1"}
```

Shutdown adds `"exiting":true`. It does not prove container death. Shutdown before
any new prompt (including a loaded session) drains and flushes without publishing
a turn result; its turn ID is `epoch:0`. Otherwise a matching private result
envelope is fsynced before either success acknowledgment:

```json
{"schema_version":1,"identity":{"schema_version":1,"job_id":"job","generation":1,"instance_token":"nonce","pid":123,"process_group":123,"start_identity":"opaque-os-start","epoch":"epoch"},"conversation_id":"native-session-id","turn_id":"epoch:1","observed_model":"openai-subscribed/gpt-6.1-sol","speed":"standard","outcome":"completed","reply":"current assistant reply","checkpoint":{"saved":true,"conversation_id":"native-session-id","turn_id":"epoch:1"},"recorded_at_ms":1,"error_code":""}
```

The values above are schema examples, not live process evidence. Outcomes are
`completed`, `interrupted`, `failed`, or `unknown`. Non-success uses fixed error
codes rather than provider bodies. Native save errors, result rename/sync errors,
model/profile/speed mismatch, or uncertain host-child termination cannot produce
checkpoint success. Managed startup requires an actual persistent connection at
`USER_DATA_DIR/threads/threads.db` before loading credentials. SQLite's default
in-memory fallback is rejected, including when that pathname is a directory.
Checkpoint and shutdown recheck the connection before native flush. Ordinary IDE
and test database fallback behavior is unchanged.
Managed status also reports `credential_mutation_pending`. While the retained
`NativeCredentials::has_pending_mutation()` is true, status is `held` and new
prompts, checkpoint and shutdown return `credential_mutation_pending`. No saved
result is published and the runtime stays alive so a detached credential write
can complete. Controls recheck after native flush, before result publication.
Shutdown also rechecks after response delivery before quitting. If a mutation
starts in that delivery interval, the runtime remains reachable with the pending
flag and control error; `exiting:true` alone is not evidence of process death.
A known completed mutation clears this temporary barrier; retry checkpoint or
shutdown without restarting. Interrupt can still drain the native turn.
The auth API includes unresolved durable mutation intent in this flag. Such intent
requires credential-owner recovery; do not delete the intent or treat process death
as proof that an external credential operation stopped. Lock location and real-HOME
resolution come from the auth owner's exported API, never a worker-specific path.
No automatic prompt replay or compaction is added.

### Managed tool boundary and host process inventory

The model receives exactly three tools:

- `managed_files`: read/list/find/grep/write/edit/create_directory/move/delete.
  Find and grep use literal substrings. Edits require one exact match. Directory
  deletion is limited to empty directories. File operations use anchored directory
  descriptors and no-follow traversal; writes stay in the worktree or exact output
  file. Atomic replacement does not mutate externally hard-linked inodes. Reads
  reject multi-link files, protected Git/credential names, and symlinks.
- `managed_terminal`: fixed Docker exec argv at the assigned cwd. The model can
  supply only command text and a bounded timeout, not a host executable, container,
  environment, or alternate cwd. Git directory/common-dir/worktree/index are fixed;
  `GIT_OPTIONAL_LOCKS=0` is explicit. Selected GitHub/toolchain environment is
  forwarded by key name, not secret values in process argv.
  Every execution first runs a fixed `factory_helper` `lease_check` with `{}`
  payload and the stored job/generation. No model-supplied precheck is trusted.
  The required positive receipt is exactly
  `{"schema_version":1,"op":"lease_check","job_id":"JOB","generation":N,"allowed":true}`.
  Missing, malformed, expired, wrong-scope or failed checks deny Docker execution.
  This receipt shape is frozen in the runtime ABI; the Go endpoint must adopt it.
  One activity barrier spans the check and exec; cancellation between them cannot
  start a new Docker command.
- `factory_helper`: `{op,payload}` through the fixed selected executable:
  `FACTORY_BIN helper control --job JOB --generation N --brain BRAIN --request -`.
  Ops: `submit`, `continue`, `result`, `yield`, `inbox`, `lease_check`, `heartbeat`,
  `intent`, `done`, `error`, `brain_read`. Caller-supplied owner/lease/runtime scope,
  arbitrary executables, managed start/end, and inbox acknowledgment are rejected.
  Go owns per-operation authority and payload validation. `brain_read` accepts only
  bounded relative markdown knowledge under AGENTS.md/repos/docs/skills/features;
  symlinks, hidden/auth/connector/runtime-journal paths and control directories are
  denied. Global native skill writing is never enabled.

Retained host process paths:

| Path | Boundary and completion policy |
| --- | --- |
| Native worker | GPUI/native agent, scoped HTTP and private SQLite; process identity and lock are retained through lifetime. |
| Selected Docker client | Fixed `exec` argv only. Cancellation/timeout kills and awaits the direct child. Container descendants require Go's separate container fence. |
| Selected factory control child | Fixed typed endpoint and stored job/generation. Bounded stdin/output/deadline; cancellation kills and awaits the direct child. Broker-created jobs remain Go-owned. |
| Native host Git/project probes | Disabled, not merely assumed read-only: managed projects have no visible worktrees and a closed native project filesystem. All Git work uses the container's private metadata. |
| Directory-shell probes, Node, extensions, LSP, tasks, formatters, native terminals/subagents, MCP | No workspace scanning, native host filesystem access, or managed registration/allowlist route. A global MCP maintenance guard remains on. |

Host-command execution is retained independently of a cancelled native waiter.
Interrupt/checkpoint/shutdown drain tracked command/file activity before saving.
If a child cannot be confirmed terminated, admission/result publication stay held.
An abrupt native death still requires Go to fence the owned host process group and
container; a native PID exit or socket acknowledgment alone is insufficient.

Managed initialization uses `NativeCredentials` and `State::new_native`, followed
by the explicit bounded identity-checked `discover_native_models`. The normal IDE
constructor and proof provider path are unchanged. Primary, summary and compaction
models are pinned in the native registry; loaded effective speed is checked too.
The closed native project filesystem also prevents automatic HOME skill reads.
Authoritative component/repo/brain instructions must come from Go's managed prompt
or the typed read-only bridge, not native workspace/profile discovery.

Offline tests use fake credentials, native models and command executors. They test
exact JSON fields, all bridge ops, policy/containment guards, lock/identity, native
save and result publication, cancelled-command drain and loaded-session guards.
They do not prove real Docker daemon/mount behavior, detached-writer fencing,
factory helper authority integration, OAuth grant independence or live inference.
Those acceptance gates remain with the parent after explicit setup and eligible quota.
