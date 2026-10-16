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
not the personal path. All other source files are byte-identical to the auxiliary
snapshot. All executable source is unchanged. Generic `/home/user` strings in
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

## Offline build verification of this freeze

All 25 reconstructed overlay files agreed byte-for-byte with the package in a
fresh owned detached worktree. The auxiliary inventory and all original input
hashes were rechecked after the build and were unchanged. All 15 packaging tests
passed. These are packaging tests, not the native runtime acceptance suite.

All four executables built successfully with Rust 1.98.1, Cargo --locked --offline,
CARGO_NET_GIT_FETCH_WITH_CLI=true and two jobs. The selected deadline was 600 seconds;
measured elapsed time was 523.63 seconds. No timeout occurred. The timing tool
reported 4011933696 bytes maximum resident size, not an aggregate memory cap.
The explicitly selected Cargo cache measured 38G before and 51G after the attempt.
The retained local verification output measured 1.5G. Output, logs and binaries
remain in ignored .verification/managed-refresh-build-20261016, not distribution.

The build emitted existing dead-code warnings, a macOS linker unwind-size warning,
and a future-Rust compatibility notice for block 0.1.6. No executable was run.
No installs, IDE changes, login, live inference, quota or Docker calls occurred.
