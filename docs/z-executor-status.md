# Z executor development status

Status at pause, 2026-10-28. The implementation is retained on the draft executor PR. It is not installed or accepted for unattended use. This note records completed work and remaining checks; it does not authorize a deployment or another live run.

## What is implemented

The opt-in `z` executor keeps Claude-only execution as the default. Claude coordinates the factory workflow, and GPT helpers run through the native Zed engine rather than Codex CLI or direct API billing. Go continues to own scheduling, quotas, leases, recovery and merging.

The branch contains immutable executor selections, a detached helper broker, bounded native control and debug inspection, container and private-Git isolation, ownership fencing, startup-failure recovery, coordinator request schemas, and the babysitter checkpoint recovery correction. The browser UI changes from main are integrated.

Native code is distributed as a pinned, separately licensed source overlay in `native/zed`. Its manifest and reconstruction script preserve the required upstream source and notices. The auxiliary native checkout's changed files were checked against that inventory; all are represented. Native executables and credentials are not part of this source distribution.

## Corrections discovered during live trials

### Truncated native status

On macOS, accepted sockets inherited the listener's nonblocking mode. Status responses could stop at the socket buffer boundary while the native worker remained alive. The handler now explicitly restores blocking mode before applying its existing read/write timeouts. Real accepted-socket tests cover backpressure, pipelined replies and a non-draining client.

A subsequent live explorer completed with a status response larger than the former cutoff. Its native result, saved checkpoint, shutdown acknowledgement, host termination, container removal and private-Git completion were recorded.

### Managed commands waiting forever for input

The host executor retained its child-stdin handle while waiting for output. With the pinned Unix pipe implementation, the asynchronous close operation only flushes. The factory CLI reads its request until EOF, so each side waited for the other.

The executor now drops stdin inside the concurrent writer future. Real-process tests cover empty and nonempty EOF, large duplex I/O, cancellation during a blocked write, timeout, output limits and exact child termination. An independent fixture watchdog handles failures without letting fallback cleanup conceal a production reaping regression.

This second correction has not yet passed a fresh model-backed sample run. The prior explorer explicitly reported missing context because command execution failed; it did not establish a passing baseline or implement the sample functions.

## Validation recorded before the pause

- Parent reran the final native executable suites: 109 tests passed.
- Parent reran the subscription fixtures: 60 tests passed.
- Parent reran distribution verification: 17 tests passed.
- Implementation evidence records old-code failures and EOF/serialized-I/O/watchdog mutations. Parent did not independently repeat all mutations for the final stdin correction.
- The last full parallel Go race run failed seven tests across `cmd/factory`, `internal/helper` and `internal/zednative`. Failures included context deadlines, observation deadlines and unexpectedly queued helper results. Their causes have not been isolated. Targeted reproduction and a serial full rerun remain pending; do not describe that run as green or dismiss its failures as flakes.
- The final review of the updated fixture watchdog remains unfinished after interruption.
- Hosted CI and the main-derived Claude process comparison passed for the preceding socket-fix revision. Those results do not certify later commits; inspect the current head's checks.

## Remaining release gates

1. Finish review and resolve or explain the Go test failures. Check the exact committed revision, including the main-derived Claude workflow comparison.
2. Validate the stdin-corrected native runtime against the real factory command boundary, then complete the disposable sample through implementation, dependencies, CI, merges and integration testing. Do not implement the sample manually.
3. Complete real nested-helper/broker-restart acceptance and the remaining process-observation checks.
4. Run the paired real Claude-only compatibility trials. Matching final functions alone does not prove matching delegation, phases, dependencies, ownership and recovery.
5. Make credential access suitable for unattended operation. Current experimental executables are ad-hoc signed. Repeated password prompts after rebuilds are a release blocker. Stable installation/signing or a narrowly scoped stable credential service remains a design decision, not an implemented feature.
6. Verify factory/desktop grant independence only under separately authorized conditions. Read-only Keychain approval and ordinary factory renewal do not prove it.
7. Run the exact merged-revision smoke before treating the release as deployed and accepted.

## Preservation and resumption

The latest disposable sample was stopped through its scoped launcher and reported clean quiescence. Older uncertain attempts must retain their ownership barriers; do not replay their turns, fabricate checkpoints or edit ledgers to clear them. Source tests and a successful transport round do not establish that the entire sample passed.

The three large experimental Rust target caches were removed at the user's request. Final staged executables, private evidence and source were preserved. Do not recreate those caches automatically. Plan disk use before any Rust rebuild.

Private plans, trial receipts and authentication evidence remain in the universal brain's feature workspace. They are intentionally not copied into this public repository. This source status note is the portable handoff. No reviewer requests, company Slack posts, merge or service restart are implied by publishing it.
