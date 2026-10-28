package agents

import (
	"fmt"
	"regexp"
	"strings"
)

// These adapters only change transport. The workflow is shared by both roles
// and the original instruction bodies remain in the immutable source snapshot.
var nativeToolCalls = strings.NewReplacer(
	"factory_helper inbox (read-only)", `factory_helper(op="inbox") (read-only)`,
	"factory_helper lease check", `factory_helper(op="lease_check")`,
	"factory_helper heartbeat", `factory_helper(op="heartbeat")`,
	"factory_helper intent", `factory_helper(op="intent")`,
	"factory_helper done", `factory_helper(op="done")`,
	"factory_helper error", `factory_helper(op="error")`,
	"helper submit", `factory_helper(op="submit")`,
	"helper continue", `factory_helper(op="continue")`,
	"helper result", `factory_helper(op="result")`,
	"helper yield", `factory_helper(op="yield")`,
)

var knowledgeReference = regexp.MustCompile(`~/.agents/(AGENTS\.md|(?:repos|docs|skills|features)/[A-Za-z0-9_<>./-]+\.md)`)
var otherBrainReference = regexp.MustCompile("~/.agents/[^`\\s]+")

func managedToolText(text string) string {
	text = strings.ReplaceAll(text, "GitHub issue writes go only through `factory issue create|update|close|comment|assign`. Never\n  `gh issue create|edit|close|comment`.", "Return any required issue changes to the owning session. Native helpers do not write issues through a host CLI or direct gh issue commands.")
	text = strings.ReplaceAll(text, "or `factory query items --repo <r> [--files …]`.", "or ask the owning session for unavailable query results.")
	text = nativeToolCalls.Replace(text)
	text = knowledgeReference.ReplaceAllStringFunc(text, func(reference string) string {
		path := strings.TrimPrefix(reference, "~/.agents/")
		if strings.Contains(path, "..") {
			return "approved context supplied by the owning session"
		}
		return `factory_helper(op="brain_read") for ` + path
	})
	// Non-allowlisted context (for example the shared tone guide in memory/)
	// must be injected by the owner, never read through a wider tool grant.
	return otherBrainReference.ReplaceAllString(text, "approved context supplied by the owning session")
}

func checkManagedToolText(prompt string) error {
	for _, bad := range []string{"helper submit", "helper continue", "helper result", "helper yield", "factory issue", "factory query", "~/.agents/", "--ack"} {
		if strings.Contains(prompt, bad) {
			return fmt.Errorf("untransformed native tool instruction %q", bad)
		}
	}
	return nil
}

const nativeToolTransport = `# Native helper transport

All dispatch, host control and brain reads MUST use the native factory_helper tool,
which accepts {op,payload}. The operation notation below means a tool invocation,
never a shell command. Use the tool's provided strict payload schema.
Dispatch operations are factory_helper(op="submit"), factory_helper(op="continue"),
factory_helper(op="result") and factory_helper(op="yield"). Controls are
factory_helper(op="inbox"), factory_helper(op="lease_check"),
factory_helper(op="heartbeat"), factory_helper(op="intent"),
factory_helper(op="done") and factory_helper(op="error").
The host derives scope, paths, owner, set, lease, job and generation from stored
binding. You cannot choose a host executable or broaden that binding.
The allowlist has no start/end/ack operations. Inbox is read-only; never request
acknowledgment or managed lifecycle publication through any tool.

Use factory_helper(op="brain_read") for bounded markdown knowledge only:
AGENTS.md, repos/, docs/, skills/ and features/. Paths must be canonical relative
to the selected brain with no symlink escape. Never request connectors/, auth
stores, .env files or production credentials. If required knowledge is outside
this allowlist, use approved injected context or return NEEDS_CONTEXT to the owner.
Do not look for brain paths inside the terminal container.

The terminal is Docker-only and cannot invoke host FACTORY_BIN. Never execute
factory commands in terminal or invent a container factory fallback. The runtime
alone invokes the fixed host control endpoint. No host shell, model/credential
probe or writable brain mount is available. Diagnostics, LSP, tasks, formatters,
extensions and native subagents are disabled. Preserve the supplied job/generation
binding; expected identity and credential namespace remain pinned by the selection.
`

const claudeCLITransport = `# Top-level Claude transport

Top-level Claude uses the normal CLI, not the native factory_helper tool.
Use helper submit, helper continue, helper result and helper yield through the
explicitly selected FACTORY_BIN and FACTORY_BRAIN. Normal inbox, lease, heartbeat
and event commands remain host CLI operations. Brain knowledge remains available
through normal host reads. Never substitute a Claude helper definition for a
native-routed job. Native helpers use their tool transport, not this host CLI.

Use the current FACTORY_BIN and FACTORY_BRAIN supplied in your factory instructions;
never use a different installed binary or brain. These commands run on the host:

"$FACTORY_BIN" helper submit --brain "$FACTORY_BRAIN" --request request.json
"$FACTORY_BIN" helper continue --brain "$FACTORY_BRAIN" --job "$JOB_ID" --request continue.json
"$FACTORY_BIN" helper result --brain "$FACTORY_BRAIN" --job "$JOB_ID" --round 1
"$FACTORY_BIN" helper yield --brain "$FACTORY_BRAIN" --job "$JOB_ID" --request yield.json

Write the JSON file before invoking its command; --request - reads JSON from stdin.
Use the returned job_id as JOB_ID, and the returned round for --round.
Examples below show field names and types, not authority to use sample values.
Copy owner_id, work_id, set_id and lease from the current factory instructions.
Never guess IDs or lease values. Replace OWNER_ID, WORK_ID, SET_ID and the sample
lease 1 with those actual values. Use lease 0 when set_id is empty; a nonempty
set_id requires its current positive lease. Preserve empty work_id or set_id when
supplied. parent_job_id is empty for a top-level dispatch; a nested dispatch must
use its actual executing parent job ID in the same scope.

Submit Request (request.json):
~~~json
{
  "schema_version": 1,
  "request_key": "review-round-1",
  "owner_id": "OWNER_ID",
  "work_id": "WORK_ID",
  "set_id": "SET_ID",
  "lease": 1,
  "parent_job_id": "",
  "component": "plan-reviewer",
  "prompt": "Review the supplied plan; return findings in your reply.",
  "output_path": ""
}
~~~

Choose component from the immutable selection and supply its actual task/context.
request_key must be a nonempty stable token of letters, digits, hyphen, underscore or dot
(not just . or ..), not a timestamp regenerated on retry. Use a distinct key for
each new logical request. After a lost acknowledgment, resend the identical JSON with the same request_key;
changing fields under that key conflicts. This applies to submit and continue.
For reply-only work, output_path is "". Otherwise use only the exact reserved output
path from the factory instructions; never invent a path or reuse a published one.

ContinueRequest (continue.json):
~~~json
{
  "schema_version": 1,
  "request_key": "review-round-2",
  "expected_round": 1,
  "prompt": "Review the revised plan in the same conversation.",
  "output_path": "",
  "acknowledged_children": []
}
~~~

expected_round is the job's current stopped round, not the next round. Continue the
returned job_id to retain the same conversation, only after a confirmed stopped
checkpoint. acknowledged_children must name exactly the failed, stopped children
being accepted for that round; use [] when none failed. Never acknowledge an active
or unknown child. Keep an unpublished output reservation; use a new reserved path
for a new published report version.

For yield.json the shape is {"children":["CHILD_JOB_ID"]}. Use actual child job IDs
and name every submitted child exactly once. Yield applies to the active parent
helper job, not the top-level Claude session. Native helpers use their tool for it.
The manager owns helper start/end through the runner and publishes durable results.
Do not emit those lifecycle events yourself. Never fall back to an external Claude Agent,
Claude helper definition or direct model call when native admission fails; report
the CLI error and keep the selected route.
`
