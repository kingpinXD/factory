package agents

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

const CodexPromptProtocol = 5

func CodexControlInstructions(version int, helper bool) string {
	if version != 2 {
		return ""
	}
	text := "\nThe stored Factory contract is version 2. The advertised catalogue defines the available operations. Factory owns GitHub credentials and all GitHub writes. Repository helpers write code and local commits in the contained workspace. Factory automatically publishes verified commits and babysitter revisions. PR openers write title and body only. An unavailable result is unknown evidence; it does not prove absence or authorize a repeated write. Do not request Slack reviews.\n"
	names := []string{toolcontract.FactoryControl}
	if helper {
		names = []string{toolcontract.FactoryHelper, toolcontract.ManagedFiles, toolcontract.ManagedTerminal}
	}
	for _, name := range names {
		text += "\n" + toolcontract.HelpForContract(name, version)
	}
	return text
}

var codexCheckpointRead = regexp.MustCompile(`Read ([^,\n]*checkpoint\.v[0-9]+\.md)`)

var codexHelperCall = regexp.MustCompile(`factory_helper\(op="([a-z_]+)"\)`)
var codexControlCommand = regexp.MustCompile(`\bfactory (event|inbox|heartbeat|lease-ok|query)\b`)
var codexIssueCommand = regexp.MustCompile(`\bfactory issue (create|update|close|comment|assign)\b`)

func codexControlText(text string) string {
	text = transformDispatch(text)
	text = strings.ReplaceAll(text, "Use helper submit, continue, result and yield through the selected FACTORY_BIN.", `Use factory_control(op="helper_submit"), factory_control(op="helper_continue"), factory_control(op="helper_result") and factory_control(op="helper_cancel"). Coordinators never yield as helper jobs.`)
	text = strings.ReplaceAll(text, "Never poll a helper. Yield to the runner after submitting jobs; durable results resume your conversation.", `Submit helpers through factory_control(op="helper_submit"), retain their job IDs, and finish your coordinator turn. Inspect their durable results when woken; coordinators do not yield as helper jobs.`)
	text = strings.ReplaceAll(text, "Do not poll or wait inside a helper\nturn. Yield with every child ID and return.", "Helpers with child jobs yield through their own broker transport and return. Coordinators retain job IDs and inspect results in later turns.")
	text = strings.ReplaceAll(text, "They use factory_helper for read-only inbox access.", "Their scoped helper transport reads inbox messages without acknowledging them.")
	text = managedKnowledgeText(nativeToolCalls.Replace(text))
	text = codexHelperCall.ReplaceAllStringFunc(text, func(call string) string {
		op := codexHelperCall.FindStringSubmatch(call)[1]
		switch op {
		case "submit", "continue", "result", "cancel":
			op = "helper_" + op
		case "brain_read":
		default:
			return call
		}
		return `factory_control(op="` + op + `")`
	})
	text = codexIssueCommand.ReplaceAllString(text, "factory_control operation issue_$1")
	text = codexControlCommand.ReplaceAllString(text, "factory_control operation $1")
	return strings.ReplaceAll(text, "factory_control operation lease-ok", "factory_control operation lease_check")
}

func codexPrompt(name, body string, snapshot sourceSnapshot, helper bool) string {
	if helper {
		if snapshot.PromptProtocol >= 5 {
			snapshot.Bridge = codexHelperBridge(snapshot.Bridge)
		}
		return managedToolText(transformHelper(body)) + "\n\n" + managedToolText(transformHelperRules(snapshot.Rules)) + "\n\n" + snapshot.HelperTransport + "\n\n" + snapshot.Bridge
	}
	if name == "babysitter" {
		body = transformCodexBabysitter(body)
	}
	return codexControlText(body) + "\n\n" + codexControlText(transformSessionRules(snapshot.Rules)) + "\n\n" + codexControlText(snapshot.SessionTransport) + "\n\n" + codexControlText(snapshot.Bridge)
}

func checkCodexPrompt(prompt string, helper bool) error {
	if helper {
		if err := checkManagedToolText(prompt); err != nil {
			return err
		}
	}
	if !helper {
		for _, bad := range []string{"factory_helper", "managed_files", "managed_terminal", "selected FACTORY_BIN"} {
			if strings.Contains(prompt, bad) {
				return fmt.Errorf("unavailable coordinator transport %q", bad)
			}
		}
	}
	for _, bad := range []string{"factory event", "factory inbox", "factory heartbeat", "factory lease-ok", "Top-level Claude transport", "normal CLI"} {
		if strings.Contains(prompt, bad) {
			return fmt.Errorf("untransformed Codex control instruction %q", bad)
		}
	}
	return nil
}

const codexHelperTransport = `# Codex helper transport

Use only the factory_helper dynamic tool for broker and scoped host controls.
It accepts {op,payload}. The operation notation below means a tool invocation,
never a shell command. Dispatch operations are factory_helper(op="submit"),
factory_helper(op="continue"), factory_helper(op="result") and
factory_helper(op="yield"). Controls are factory_helper(op="inbox"),
factory_helper(op="lease_check"), factory_helper(op="heartbeat"),
factory_helper(op="intent"), factory_helper(op="done") and
factory_helper(op="error"). The provided strict schema defines each payload.
The host derives owner, work, set, lease, job and generation from the stored
binding. Do not supply another executable or broaden that binding. Helpers never
acknowledge inbox messages or emit start/end lifecycle events.

Use factory_helper(op="brain_read") for bounded approved markdown knowledge:
AGENTS.md, repos/, docs/, skills/, features/ and exact memory/tone_guide.md. Use canonical relative paths,
without symlink escapes. Never request credentials, auth stores, .env files or
connector configuration. Return NEEDS_CONTEXT for unavailable knowledge.

All repository file access and commands must use managed_files and
managed_terminal through the factory's contained workspace. Never use a native
Codex shell, apply_patch, app, browser, MCP server or a different execution route
for writes. The runtime must prove that tool containment before admitting work;
these instructions do not establish that proof. Never copy authentication files.
`

const codexSessionTransport = `# Codex coordinator transport

This conversation runs in a factory-owned Codex app-server. Native Codex agents
are disabled with agents.enabled=false. All model delegation uses the program's
helper broker through factory_control, with {op,payload}:
factory_control(op="helper_submit"), factory_control(op="helper_continue"),
factory_control(op="helper_result") and factory_control(op="helper_cancel").
Coordinators are not helper jobs and must never call yield. Never call another
model, create native agents or substitute a Claude or Zed worker.

Use the factory_control dynamic tool for coordinator operations. Its envelope is
{op:string,payload:object}. The source procedure's notation
"factory_control operation event ..." means op="event" with the named
fields in the tool's strict payload object. It is not a shell command. Preserve
all original lifecycle, inbox acknowledgment, lease, checkpoint, issue-write,
query and intent/done requirements. Derive scope from the bound task and copy
exact IDs, lease values and paths from the current factory instructions. Never
guess scope, bypass an unavailable operation or execute a host factory binary.
Use brain_read{path,max_bytes} for approved knowledge, including exact memory/tone_guide.md for public writing. file_read/file_write use
{target,path:basename,text,append}; target is the bound owner unless explicitly
authorized otherwise. Write checkpoint.vN.md, then event{kind:"checkpoint",path}.
For lifecycle use event{target,kind,path,text,key}; for messages use
inbox{target,ack}; heartbeat/lease_check accept {target}. events{} returns your
bounded recent event history. User questions use event{kind:"message",text,to:"user"}.
Use plan_input{} for the immutable accepted input plan. Read epic feature notes
with epic_file{path:"DESIGN.md"|"PLAN.md"|"GOTCHAS.md"}.
helper_submit uses the helper request {schema_version:1,request_key,owner_id,
work_id,set_id,lease,component,prompt,output_path?}; no parent_job_id for a
coordinator. helper_continue uses {job_id,request:{schema_version:1,request_key,
expected_round,prompt,output_path?}}; helper_result uses {job_id,round};
helper_cancel uses {job_id,request:{reason:"why this helper is being cancelled"}}.
Retain job IDs and use a new output reservation for a new published round.
Reply-only reviewers must omit output_path in submit and continue; consume their
reply through the broker result, then write findings into the coordinator rounds.
Issue operations are issue_read/children/create/update/comment/close/assign with
{repo,number,key,title,body,parent,evidence} as required by the provided schema.
Use query{repo} for the scoped item summary. Issue-write intent/done belongs to
the program. Read approved knowledge through these tools, never host discovery.

Repository inspection, edits, tests, git operations and pushes belong to
contained broker helpers. Submit the selected explorer, implementor, verifier or
other scoped component through helper_submit and consume its durable result.
Coordinator task files use file_read/file_write; use a bound target and basename
for supplied output paths. Folder paths in manager messages are scope metadata,
not permission to read a host filesystem. Read history through events{}, never
by tailing events.jsonl. Use only factory_control; no host CLI, native shell,
apply_patch, app, browser, MCP server, alternate agent or credential discovery.

Read GitHub evidence through scoped github_pr, github_linked_prs, github_checks,
github_base, github_threads, github_reviews, github_comments, github_merged_prs,
github_prs, github_commit, github_file, github_runs, github_run and github_job_log.
Payloads use {repo,number?,sha?,base?,path?,since?} as appropriate; use the current
PR/head and registered base from the owning work. Unsupported or incomplete
reads are unknown, never success. pr_reply/pr_resolve handle current-PR comments
as described in the babysitter procedure. Never merge or request reviewers.
The runtime proves containment before admitting work; these instructions do not.
No credentials or writable shared brain may be mounted into the workspace.
`

const codexBridge = codexRecoveryContract + `# Codex factory bridge

The original component procedure, review responsibilities, output headings,
code ownership and merge restrictions still apply. Only the program selects
routes. The immutable selection pins Codex executable, version, account identity,
model, effort and model evidence policy across every round. Configuration is not
proof of the executed model. An observed reroute or unexpected configuration
holds work. No Claude, Zed, API-key or paid-credit fallback is permitted.

Use the program-owned broker for every helper. Submit independent reviewers and
retain their job IDs. Queued, running, waiting, blocked, unknown and
recovery_required results are not success. Do not poll or wait inside a helper
turn. Yield with every child ID and return. The runner ends the parent round
before admitting children, so nested reviews can progress with one active slot.
Continue the same reviewer job and conversation for round two using the exact
expected_round. Failed or uncertain children never count as completed reviews.
A new published file version needs a fresh output reservation. Do not reuse a
published output or repeat an external action from an uncertain turn.

The program owns helper lifecycle and output publication. Write only the exact
reserved output path for the active round. Publication requires a verified
persisted terminal turn, confirmed stopped writers and descendants, current
lease/generation and the selected model evidence policy. Turn completion alone
is insufficient. Observation-only recovery never submits another model turn.
No helper may merge. Check the lease before external actions and record scoped
intent/done. Helpers read inbox messages without acknowledging them.

Do not post reviews or review requests on Slack.
`

const codexRecoveryContract = `
Current broker contract: reply-only reviewers, including plan-reviewer and code
reviewers, require omitted output_path for submit and continue. File-producing
helpers require their exact reserved file path. A tool_error is an unsuccessful,
correctable request; it does not admit a helper or prove an outside action occurred.
file_write returns filename for subsequent file/event payloads. Publish checkpoints
with event{kind:"checkpoint",path:filename}; never pass the absolute path field.
For a cancelled helper, inspect its recovery proof. Only terminal cancelled results
with stopped=true, descendants_stopped=true and publication_settled=true permit a
fresh request key and new output version under current ownership. Preserve the old
job, conversation, reservation and partial files. no_launch=true means the broker
proved no launch; false can mean a launched interruption. Never continue or accept
partial output from a cancelled launched attempt. Unknown or unsettled proof still
blocks dispatch; report the exact missing fact rather than assume fencing.
Recovery stored_scope is the helper's immutable scope. scope_matches_current
compares it only to the current ledger scope, not admission authority. If false,
never Continue that old scope, even for a completed helper. Only a fresh job/key
and output version may follow terminal/stopped/descendants/publication proof and
current broker authority. Preserve the old job and accepted outputs; do not replay
or rebind it. null is unknown; true grants no permission, including when the ledger
scope is temporarily closed. Existing Continue checks always remain required.
expected_round means the CURRENT stored helper round from helper_result, not the
next round. Inspect current state and confirmed completion before continuing;
a mismatch never admits a new round. A busy refusal means no file/lifecycle action
occurred: hold for terminal stopped/descendant/publication proof. Never repeat
writes/events against unsettled or unknown helpers or bypass that guard.
Read the writing guide through brain_read{path:"memory/tone_guide.md"}; this exact
path is allowed, not the memory/ directory. A busy refusal names the blocking
job_id and round. The current epic planner may inspect that same-epic child.
helper_result INPUT is exactly {job_id,round}; omit read_only from the request.
Its RESPONSE read_only:true identifies metadata and recovery facts only. It does
not grant Continue/Cancel authority or permission to publish. After settlement,
a new normal publication request still rechecks current authority and the guard.
Unknown operations must never be replayed from this advice.
github_base requires the registered work base, not its head branch. github_commit
accepts a commit or branch/ref in sha. A failed read is unknown evidence, not proof
of absence or authority to repeat a write. Existing authorized contained read-only
preflight remains available under current authority; no additional approval is needed.
Before new verifier or pr-opener work, coordinators use query{repo} with the
registered repository from inputs.yaml, match the work id and read its state.
Wait for the program-owned phase transition. Verifier requires verifying; pr-opener requires
pr_open for set-owned work. Any failed verification requires a fresh truthful
implementation report even when no code changes, before new verification. Retain
historical failure and accepted reports. Reuse an existing PR only for the exact
unchanged commit; never launch a duplicate opener or create another PR.
`

// SessionText adapts manager-generated messages using the same coordinator transport.
func SessionText(provider, text string) string {
	if provider == "codex" {
		text = codexControlText(text)
		text = codexCheckpointRead.ReplaceAllStringFunc(text, func(read string) string {
			return fmt.Sprintf("Read the checkpoint through factory_control(op=\"file_read\",payload={path:%q})", filepath.Base(strings.TrimPrefix(read, "Read ")))
		})
		text = strings.ReplaceAll(text, "the tail of your events.jsonl", "your recent history through factory_control(op=\"events\")")
		text = strings.ReplaceAll(text, "GitHub or Slack", "the scoped GitHub evidence controls; Slack is unavailable")
		text = strings.ReplaceAll(text, "before pushing and answering review threads", "through the existing_pr_revision helper before answering review threads")
		return text + codexRecoveryContract + "\nUse factory_control payload objects for these manager instructions. Read task inputs through file_read{path:\"inputs.yaml\"}, checkpoints and declared outputs through file_read{path:basename}, history through events{}, and inbox messages through inbox{target,ack}. Absolute paths identify the bound task only. Delegate repository commands, tests and pushes to the scoped helpers. Never use a host shell or Slack.\n"
	}
	return text
}

func codexHelperBridge(bridge string) string {
	recovery := codexRecoveryContract
	start := strings.Index(recovery, "file_write returns filename")
	end := strings.Index(recovery, "For a cancelled helper")
	recovery = recovery[:start] + recovery[end:]
	start = strings.Index(recovery, "A busy refusal names")
	end = strings.Index(recovery, "github_base requires")
	recovery = recovery[:start] + recovery[end:]
	start = strings.Index(recovery, "Before new verifier")
	recovery = recovery[:start]
	return strings.Replace(bridge, codexRecoveryContract, recovery, 1)
}
