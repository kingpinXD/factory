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

The command is `rustup run 1.98.1 cargo build --locked --offline --jobs N -p eval_cli --bins`.
`rustup run` avoids checkout-driven component synchronization. Cargo stays offline
unless `--allow-network` is explicitly selected; that permits dependency downloads,
not auth/model/quota requests. `--test` additionally runs eval_cli bin tests and
openai_subscribed library tests with bounded test threads. These use local fixtures;
the script never runs the built probes or signs in. This is a pinned build recipe,
not a bit-for-bit reproducible-binary claim.

Successful builds copy `eval-cli`, `zed-native-usage-probe`,
`zed-native-lifecycle-probe` and `zed-native-credential-setup` into the selected output's `bin/`. No installed IDE,
PATH entry, symlink, service, launchd job or Docker resource is changed. No runtime,
credential, account or model calls are part of packaging/build verification.
Logs and local build output can contain machine paths; keep them private and outside
source distribution. `.verification/` and `__pycache__/` are ignored local artifacts.

See `PROVENANCE.md` for origin and modification scope, and the overlay's
`crates/eval_cli/README.md` for the managed ABI, pending-mutation recovery API, tool boundaries and remaining
acceptance limitations. Its upstream
root README manual-review notice is preserved and must not be removed by an agent.
