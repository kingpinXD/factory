# Frozen native source provenance

Upstream: zed-industries/zed, release v1.24.2.
Base commit: `fb69067729cb16a7f6135e2c1600c26a28c59659`.
Repository: https://github.com/zed-industries/zed.

This snapshot was copied from the local `feat/factory-native-probe` auxiliary
worktree at the base commit. Its modifications are uncommitted, not an upstream
release or a published fork. No new source commit was created. The auxiliary
checkout remains read-only for packaging. This snapshot replaces the obsolete
phase-zero-only overlay.

The freeze time and exact snapshot identifiers are in `source-manifest.json`.
The snapshot digest is SHA-256 over sorted UTF-8 lines of the form
`FILE_SHA256`, two spaces, relative path, newline. Auxiliary and packaged-overlay
digests are recorded separately. The manifest itself is hash-pinned in build.py.
These hashes detect accidental or partial tampering, not replacement of the whole
distribution by an attacker. Consumers should coordinate against the manifest
hash and auxiliary snapshot digest, not just the unchanged upstream commit.

There are 25 source files: eleven tracked modifications and fourteen previously
untracked Rust files. Each manifest entry records the upstream baseline SHA-256
(null for additions), auxiliary SHA-256 and distributed SHA-256. Files were copied
with tar and compared against the auxiliary hashes before and after copying.

One documentation-only privacy normalization is recorded: the crate README's
single personal CARGO_TARGET_DIR example becomes
`/absolute/path/to/selected-cargo-target`. Its original content hash is preserved,
not the personal path. Later overlay-only corrections are documented below. The
auxiliary hashes preserve the original snapshot. Generic `/home/user` strings in
upstream Rust test fixtures are synthetic, not personal source paths.

The distributed source includes:

- Cargo manifests/lockfile and four executable entry points: eval-cli,
  zed-native-lifecycle-probe, zed-native-usage-probe and zed-native-credential-setup.
- Managed launch identity, socket/control protocol, model/profile checks,
  exact-session persistence and acknowledged result/checkpoint behavior.
- Managed file/project filesystem and terminal/helper tools, plus their tests.
- Factory-scoped credential wrapper, native auth and tests, pending-mutation
  inspection/settlement API, and scoped usage entry point.
- Agent persistence changes and persistent-database startup/checkpoint checks.
- Native provider integration and upstream README/manual-review notice.

Implementation presence is not proof of live safety or production acceptance.
The packaged crate README describes remaining OAuth grant, Docker containment,
service-settlement and integration gates. Source and packaging tests do not
perform login, Keychain, model, quota or Docker calls.

`LICENSE` and notices/LICENSE-GPL are verbatim copies of upstream LICENSE-GPL;
notices/LICENSE-APACHE is a verbatim copy of upstream LICENSE-APACHE at the pin.
Upstream authors retain their copyrights. The packaging scripts, tests and docs
in this subtree are provided under GPL-3.0-or-later, not the factory's MIT license.
Upstream per-crate license files remain in the reconstructed tree. Apache-licensed
portions and third-party dependencies retain their own terms. No binary is shipped.
Future native binary distribution needs a separate corresponding-source/license
review; this source-only snapshot makes no license-compatibility promise.

## Historical asset-regression build (superseded output)

The earlier all-executable compile succeeded without util/debug-embed, but that
was not a valid portability check. A copied debug executable still searched for
checkout assets at runtime. Stable channel did not change that debug-build rule.
That earlier output is superseded by this freeze and must not be used for login.

The only auxiliary source change for this fix is one asset-only test in
crates/eval_cli/src/credential_probe.rs. It loads settings, keymaps, semantic rules
and an agent template without GPUI, credentials, HTTP, models or native startup.
No auth behavior or public CLI was changed. Upstream's mandatory manual-review
notice remains intact. Cargo util's existing debug-embed feature supplies the fix;
no util, Cargo manifest, Go adapter or reconcile source was modified.

Red: the focused test executable was built with the old package flags (Rust 1.98.1,
--locked --offline --jobs 2, no embedding feature), copied away from its source,
and run from an empty cwd with scratch HOME. It failed specifically on
settings/default.json before any native startup. Elapsed time for red build/check
was 371.41 seconds under a 600-second compilation bound. There was no timeout.

Green: all four executables and the focused test executable were built with
rustup run 1.98.1, --locked --offline --jobs 2 --features util/debug-embed and
CARGO_NET_GIT_FETCH_WITH_CLI=true. The relocated asset test passed. Cwd and HOME
remained empty. Measured total elapsed time was 501.02 seconds under the shared
900-second deadline. No timeout occurred. The timing tool reported 3971497984
bytes maximum resident size, not an aggregate memory cap. The selected Cargo
cache measured 61G before green and 79G afterward; retained green output was 1.6G.

Green output is ignored .verification/asset-relocation-green-20261016. The four
binaries are under bin/; the safe repeatable asset-only test executable is under
checks/. Reconstructed source matches all packaged hashes and auxiliary input
hashes were rechecked after build. The one README privacy normalization remains
the only packaged-versus-auxiliary difference. The regression source passed a
file-scoped Rust 1.98.1 rustfmt check. Packaging tests cover the mandatory feature.

No login, Keychain, usage, inference, Docker, IDE change, install, default toolchain
change, commit or push was performed. Asset success does not establish live auth
or production acceptance. Dependency/linker warnings remain outside this fix.

## Final explicit-enrollment refresh

This freeze supersedes the earlier asset-fix binary output for future enrollment.
The original output remains unchanged. The auxiliary auth owner changed exactly
five files: native_auth.rs and its tests, credential_probe.rs, lifecycle_probe.rs
and newnativeauth_usage.rs. They add explicit read-only exact-executable Keychain
enrollment and preserve emitted exit status without bypassing pending mutations.
The auxiliary source was only read during this refresh; all changes were copied
with tar. The manifest records the new auxiliary/overlay digests and freeze time.
The existing README privacy normalization remains the only distributed difference.

All four executables built with Rust 1.98.1, --locked --offline --jobs 2,
--features util/debug-embed and CARGO_NET_GIT_FETCH_WITH_CLI=true. Total build plus
relocated asset check took 488.16 seconds against a 900-second deadline. There was
no timeout. The selected shared cache remained separate from copied final binaries;
it measured 83G before and 96G afterward. Final output measured 1.6G. The timing
tool reported 3861594112 bytes maximum resident size, not an aggregate memory cap.

The new output is ignored .verification/keychain-enrollment-final-v1. Enroll only
the exact executables under its bin/ directory, not shared-target executables or
old output. binary-fingerprints.json records all four hashes; cli-metadata.json
records bounded help/parser results. No automatic enrollment is performed.

All 31 help/parser checks passed with a 60-second per-process bound. The copied
lifecycle worker's first --help took 13.256 seconds and its warm repeat took 0.027
seconds. eval-cli's first help took 13.549 seconds. Setup and usage help each took
about one second. All three enrollment --help routes exited zero without native
startup; forbidden combinations and unknown flags exited two. Empty cwd and
scratch HOME remained empty. Do not interpret a ten-second help timeout as proof
that native startup is unavailable.

The credential-setup test executable's ten offline tests passed, including the
relocated asset regression, strict enrollment parser and credential-free error-exit
subprocess. Packaging includes a regression for the new source and unchanged
five-second unattended versus 120-second explicit read deadlines. Reconstructed
source was compared with all 25 packaged inputs and auxiliary hashes after build.
The previous four copied asset-fix binaries retain their original fingerprints.

These are offline packaging/CLI checks, not real Keychain approval, sign-in,
provider/grant independence, refresh, usage, inference or managed acceptance.
No install, original output overwrite, live auth, IDE change, commit or push occurred.

## Accepted-socket correction

The packaged lifecycle handler now clears inherited nonblocking mode before setting
its existing socket timeouts. Real accepted-socket tests cover small and large JSONL
responses, pipelined status requests, delivery receipts and a non-draining client.
Only lifecycle_probe.rs and lifecycle_tests.rs differ from the auxiliary snapshot
for this correction. Their distributed hashes and the overlay inventory are updated;
auxiliary hashes remain historical. No enrolled executable or live runtime is changed.
