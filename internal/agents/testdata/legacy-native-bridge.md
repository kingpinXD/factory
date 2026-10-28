# Native helper bridge

This bridge governs dispatch and managed host control, including any referenced
skill instructions. It does not change the component's reasoning, review role,
output headings, code ownership or limits on merging.

Dispatch only through helper submit / helper continue / helper result / helper yield.
Top-level Claude sessions invoke these commands using the explicitly selected
FACTORY_BIN and FACTORY_BRAIN. Native helpers use the typed factory_helper tool.
Use the endpoint's provided strict request schema, never invented flags or fields.
Do not use native subagents, Claude helper definitions or manual messaging as fallback.
A submitted job pins its provider and model for all rounds. Model/effort overrides
are not dispatch arguments. Only the program selects routes.

Submit each independent reviewer with helper submit. Keep each returned job ID.
Read durable results with helper result; queued, running, waiting, blocked and
unknown are not success. Never poll or wait inside a native turn. Use helper yield
with every submitted child ID, then return immediately without claiming completion.
The runner ends/checkpoints your turn before admitting children. With one slot,
children run only after the parent releases its execution turn. The runner resumes
the same parent conversation once all children complete. A failed, interrupted,
recovery_required or unknown child never counts as a completed review.

For plan review Round 2, use helper continue on the same plan-reviewer job and
same conversation, with expected_round and the revised plan prompt. Do not submit
a replacement reviewer. A new published file version needs a fresh reserved output;
automatic continuation after yield retains the same unpublished output reservation.
Manual recovery must acknowledge every unsuccessful direct child and requires all
descendants confirmed stopped. Do not retry consequential commands from an uncertain turn.

For native helpers, factory_helper is the only host-control tool. Its strict typed
allowlist is: submit, continue, result, yield; inbox read (never ack); lease check;
heartbeat; scoped external-action intent; scoped external-action done; and error.
Owner, work, set, lease, paths, job and generation bindings come from stored authority,
not caller-supplied replacements. Never issue arbitrary host commands. Managed start
and end are program-owned and cannot be requested by helpers. Write only the exact
reserved output path supplied for this round, then return it (or the reply-only review).
Do not claim ownership of rounds.md when returning a reviewer result. No output is
published until native turn termination, descendant barriers and authority are verified.

For native helpers, at every step boundary and at least every ten minutes or twenty tool calls, read
inbox through factory_helper without ack. Return early with any message sequence
and text so the owning session can act. Heartbeat during long work. Before a push,
check the lease. Before every external action, record intent naming its branch/SHA,
PR/comment or Slack recipient; after it, record done. On restart, inspect the outside
world before repeating an intent without done. Report one error per failed step.
These controls remain available while waiting on quota. No helper may merge.

Managed terminal commands run only in the attested per-round container. Diagnostics,
LSP, tasks, formatters, extensions and native subagents are disabled. No host shell,
installed-factory fallback or credential probe is permitted. Preserve FACTORY_BIN,
FACTORY_BRAIN and the supplied job/generation binding. The expected identity and
credential namespace are pinned in the immutable selection; never infer or replace them.
