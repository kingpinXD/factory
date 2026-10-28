# Zed native source distribution

This snapshot supersedes the earlier tool-free phase-zero-only overlay. It includes
the auxiliary managed-runtime implementation, managed tools, persistent database
checks, factory credential setup and pending-mutation handling. Proof mode remains
available. Packaging is not a production-runtime acceptance result. The Go adapter
and reconcile integration are owned separately and are not changed here.

Live grant independence, real container fencing, integrated crash recovery and
production dispatch still require their acceptance gates. No login or live model,
quota, Docker or credential call is performed by packaging/build verification.

## License boundary

This subtree is **not MIT**. `LICENSE` is the upstream GPL text; this distribution
is GPL-3.0-or-later, with upstream notices and separately licensed portions retained.
`notices/LICENSE-GPL` and `notices/LICENSE-APACHE` are verbatim upstream texts.
The full upstream tree retains its per-crate licenses when reconstructed. Dependencies
have their own licenses; `Cargo.lock` pins dependencies but is not a license inventory.

The native executables are separate processes. The lifecycle proof uses JSONL over
a private Unix socket; the usage proof prints JSON. There is no Rust linking into
the Go factory. This process boundary is not a license-compatibility promise.
Future binary distribution needs a separate corresponding-source and license-compliance
review covering upstream, dependencies, notices and applicable source obligations.
This overlay/build recipe is not a claim that those obligations have been settled.
No native binaries are distributed here. Root MIT exclusion belongs to the integration
owner; the factory root README is deliberately unchanged by this task.

## Verify and build

Requires Python 3.9+, Git, an already-installed Rust 1.98.1 toolchain and platform
build prerequisites. On macOS these include Xcode, its Metal Toolchain and CMake.
No toolchain/component installation or default-toolchain change is performed.

From the factory worktree:

```sh
python3 native/zed/build.py --verify-only
python3 native/zed/test_distribution.py -v
```

To build, explicitly supply a local Zed checkout whose HEAD is the pinned commit
in `source-manifest.json`, and a **new** experiment output directory. The output
parent must already exist. There are no checkout, output, install or service defaults.
For example, replace the descriptive paths below with your selected locations:

```sh
python3 native/zed/build.py --checkout /absolute/path/to/pinned-zed \
  --output /absolute/path/to/new-experiment --jobs 2 --seconds 600
```

The script verifies the pinned manifest, every overlay/notice hash (including the
modified lockfile), and the selected checkout HEAD. It creates its own local bare
clone and detached temporary worktree inside the output directory. It checks every
baseline file hash and the absence of newly added files before copying the overlay.
The selected checkout's files, index, refs and worktree registrations are not changed.
Local changes in that checkout are ignored, not reset or packaged. The clone shares
upstream objects read-only; retain the selected repository while inspecting this output.

`--prepare-only` reconstructs and verifies source without invoking Rust.
`--cache-target` explicitly selects an existing writable Cargo target cache; otherwise
the cache is output-local. Do not share a target with another active build. Cache
reuse can still trigger substantial recompilation because source paths differ.
Debug builds can consume tens of GiB. Jobs default to two and are capped at four.
The total command deadline defaults to 600 seconds and is capped at 3600.
Timed-out commands receive process-group TERM, then KILL after a five-second grace.
Source, build log, repository metadata and partial cache are retained on failure.
There is no forced worktree removal, reset, source deletion or automatic retry.

The command is `rustup run 1.98.1 cargo build --locked --offline --jobs N --features util/debug-embed -p eval_cli --bins`.
`rustup run` avoids checkout-driven component synchronization. Cargo stays offline
unless `--allow-network` is explicitly selected; that permits dependency downloads,
not auth/model/quota requests. `--test` additionally runs eval_cli bin tests and
openai_subscribed library tests with bounded test threads. These use local fixtures;
the script never runs the built probes or signs in. This is a pinned build recipe,
not a bit-for-bit reproducible-binary claim.

`util/debug-embed` is mandatory for all four executables. Stable release channel
alone does not embed debug assets. Without this feature, copied debug binaries
search their executable/cwd ancestors for a checkout and can panic on missing
settings before login. Do not use the earlier unembedded output.

Successful builds copy `eval-cli`, `zed-native-usage-probe`,
`zed-native-lifecycle-probe` and `zed-native-credential-setup` into the selected output's `bin/`. No installed IDE,
PATH entry, symlink, service, launchd job or Docker resource is changed. No runtime,
credential, account or model calls are part of packaging/build verification.
Logs and local build output can contain machine paths; keep them private and outside
source distribution. `.verification/` and `__pycache__/` are ignored local artifacts.

## First-use Keychain enrollment: exact final executables

Choose the final experiment output first and keep its `bin/` executables at those
exact paths. The selected output binaries are copies, not the shared Cargo target.
Never enroll a cache executable and then copy or replace it as if authorization
must transfer. A moved, rebuilt or replaced executable may require enrollment again.
Record fingerprints and explicitly select the final paths in the caller's private
configuration. The build does not change any live selection or installed IDE.

Only a user-present, separately authorized step may run `--authorize-keychain`.
Setup, scoped usage and lifecycle each run this mode in their **own executable**;
they never exec the setup binary to enroll another identity. Inspect the mode
without touching credentials:

```sh
/absolute/path/to/final-output/bin/zed-native-credential-setup --authorize-keychain --help
/absolute/path/to/final-output/bin/zed-native-usage-probe --authorize-keychain --help
/absolute/path/to/final-output/bin/zed-native-lifecycle-probe --authorize-keychain --help
```

When the parent/user separately authorizes real enrollment, the invocation template
for each of those exact executables is:

```text
EXACT_EXECUTABLE --authorize-keychain --user-data-dir ABS_PRIVATE_DATA_DIR --credential-namespace factory-openai [--expected-identity PINNED_OPAQUE_BINDING]
```

This explicit mode performs a read-only native Keychain access with a 120-second
read deadline. It does not start OAuth, sign in, refresh, fetch usage, discover
models or run inference. `--user-data-dir` must be absolute but is not created or
accessed by enrollment. The optional expected identity verifies an existing pin;
success does not return a new binding or prove grant independence. Missing or
cancelled reads can both report `keychain_authorization_unavailable`; neither proves
successful enrollment or justifies blindly restarting OAuth. Pending mutation
intent remains a hold, not something enrollment settles or clears.

Enrollment strictly rejects normal startup flags such as `--sign-in`, `--managed`,
`--capabilities`, `--printenv`, unknown flags and `--authorize-keychain=false`.
Do not combine enrollment with ordinary setup or worker launch. Ordinary unattended
credential reads retain their five-second deadline. Dynamic-loader/cold executable
startup is separate from that native read deadline: use a bounded 60 seconds for
help-only metadata checks, not a ten-second startup assumption.

The final source also preserves error exit status rather than AppKit's zero-status
quit, while keeping the runtime alive for pending credential mutations. Offline
parser/exit fixtures test this; packaging does not claim real Keychain settlement.

**Acceptance distinction:** embedded assets, successful builds, help and parser
checks are offline evidence only. They do not establish successful Keychain
approval, OAuth sign-in, independent grants, refresh safety, quota admission or
production managed execution. Those remain separately authorized acceptance gates.
No real enrollment, login, Keychain, usage or inference is run during packaging.

## Offline relocated asset gate

Every full build also compiles the focused credential-setup test executable using
`cargo test --locked --offline --jobs N --features util/debug-embed -p eval_cli
--bin zed-native-credential-setup --no-run --message-format=json` (with offline
omitted only when explicitly requested). It copies the test executable away from
the source/cache into `OUTPUT/checks/zed-native-runtime-assets-test` and runs only
`tests::runtime_assets_load_without_native_startup`. Both executable and empty cwd
must have no ancestor providing Zed's assets. HOME is a separate empty directory.
The shared build deadline covers this gate too. Failure leaves source/logs intact
and does not populate `OUTPUT/bin` with a successful distribution.

This regression loads default settings, keymaps, semantic rules and an agent
prompt template. It initializes no GPUI runtime, credential provider, HTTP client,
model or native State. No `--check-runtime` public CLI or sign-in behavior changed.
Only this isolated test executable is run, never the four native entry points.

To repeat the asset-only check, use the absolute path to the selected output's
`checks/zed-native-runtime-assets-test` from its `empty-cwd/` directory:

```sh
/absolute/path/to/output/checks/zed-native-runtime-assets-test \
  --exact tests::runtime_assets_load_without_native_startup --test-threads=1
```

This checks asset portability, not authentication readiness or live inference.
Binaries in `OUTPUT/bin/` are distinct from old outputs and installed programs.
Consumers must explicitly select these new paths and hashes; no live selection
or immutable profile is rewritten by the build script.

See `PROVENANCE.md` for origin and modification scope, and the overlay's
`crates/eval_cli/README.md` for the managed ABI, pending-mutation recovery API, tool boundaries and remaining
acceptance limitations. Its upstream
root README manual-review notice is preserved and must not be removed by an agent.
