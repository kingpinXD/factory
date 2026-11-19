package toolcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/jsonobject"
	"slices"
	"strings"
)

const (
	FactoryControl       = "factory_control"
	FactoryHelper        = "factory_helper"
	ManagedFiles         = "managed_files"
	ManagedTerminal      = "managed_terminal"
	BrainReadLimit       = 64 * 1024
	TerminalLimit        = 128 * 1024
	StoredCanonicalLimit = 6 * ControlLimit
	EventTextLimit       = 16 * 1024
)

var coordinatorEvents = events.CoordinatorEventKinds()
var epicReferences = []string{"DESIGN.md", "PLAN.md", "GOTCHAS.md"}
var replyKinds = []string{"thread", "comment", "review"}

type Example struct {
	Operation string
	Payload   json.RawMessage
}
type definition struct {
	operation        string
	fields, required []string
	example          string
	input            func() Input
	first, last      int
	read, write      bool
}

func contract(op, fields, required, example string, input func() Input) definition {
	return definition{op, strings.Fields(fields), strings.Fields(required), example, input, 1, 2, false, false}
}
func emptyInput() Input    { return &Empty{} }
func textInput() Input     { return &TextInput{} }
func brainInput() Input    { return &BrainReadInput{} }
func controlInput() Input  { return &ControlInput{} }
func issueInput() Input    { return &IssueInput{} }
func githubInput() Input   { return &GitHubReadInput{} }
func prInput() Input       { return &PRActionInput{} }
func resultInput() Input   { return &ResultInput{} }
func continueInput() Input { return &ContinueInput{} }
func fileInput() Input     { return &FileInput{} }

func allDefinitions(tool string) []definition {
	switch tool {
	case ManagedFiles:
		return []definition{
			contract("read", "path", "path", `{"path":"numbers/parse.go"}`, fileInput),
			contract("write", "path text", "path text", `{"path":"implement.v1.md","text":""}`, fileInput),
			contract("append", "path text", "path text", `{"path":"implement.v1.md","text":"verified"}`, fileInput),
		}
	case ManagedTerminal:
		return []definition{contract("run", "command cwd", "command", `{"command":"go test ./..."}`, func() Input { return &TerminalInput{} })}
	case FactoryHelper:
		out := []definition{
			contract("lease_check", "", "", `{}`, emptyInput), contract("inbox", "", "", `{}`, emptyInput),
			contract("heartbeat", "text key", "text", `{"text":"working"}`, textInput),
			contract("intent", "text key", "text", `{"text":"local check","key":"check"}`, textInput),
			contract("done", "text key", "text", `{"text":"local check finished","key":"check"}`, textInput),
			contract("error", "text key", "text", `{"text":"check failed"}`, textInput),
			contract("brain_read", "path max_bytes", "path", `{"path":"repos/factory.md","max_bytes":65536}`, brainInput),
			contract("submit", "request_key component prompt output_path", "request_key component prompt", `{"request_key":"review-1","component":"plan-reviewer","prompt":"Review the plan."}`, func() Input { return &NestedSubmitInput{} }),
			contract("result", "job_id round", "job_id round", `{"job_id":"job-1","round":1}`, resultInput),
			contract("yield", "children", "children", `{"children":["job-1"]}`, func() Input { return &YieldInput{} }),
			contract("continue", "job_id request", "job_id request", `{"job_id":"job-1","request":{"schema_version":1,"request_key":"review-2","expected_round":1,"prompt":"Review the revision."}}`, continueInput),
		}
		for _, d := range allDefinitions(FactoryControl) {
			if d.read {
				d.first = 2
				out = append(out, d)
			}
		}
		return out
	case FactoryControl:
		out := []definition{
			contract("brain_read", "path max_bytes", "path", `{"path":"repos/factory.md"}`, brainInput),
			contract("plan_input", "", "", `{}`, emptyInput),
			contract("epic_file", "path", "path", `{"path":"PLAN.md"}`, func() Input { return &PathInput{} }),
			contract("helper_submit", "schema_version mode request_key owner_id work_id set_id lease parent_job_id component prompt output_path revision_pr revision_repo revision_branch revision_base", "schema_version request_key owner_id component prompt", `{"schema_version":1,"request_key":"explore-1","owner_id":"set-1","work_id":"work-1","set_id":"set-1","lease":1,"component":"explorer","prompt":"Explore the work.","output_path":"explore.v1.md"}`, func() Input { return &Request{} }),
			contract("helper_continue", "job_id request", "job_id request", `{"job_id":"job-1","request":{"schema_version":1,"request_key":"review-2","expected_round":1,"prompt":"Review the revision."}}`, continueInput),
			contract("helper_result", "job_id round", "job_id round", `{"job_id":"job-1","round":1}`, resultInput),
			contract("helper_cancel", "job_id request", "job_id request", `{"job_id":"job-1","request":{"reason":"task cancelled"}}`, func() Input { return &CancelInput{} }),
			contract("events", "target", "", `{}`, controlInput),
			contract("inbox", "target ack", "", `{"ack":1}`, controlInput),
			contract("lease_check", "target", "", `{}`, controlInput),
			contract("heartbeat", "target", "", `{}`, controlInput),
			contract("file_read", "target path", "path", `{"path":"inputs.yaml"}`, controlInput),
			contract("file_write", "target path text append", "path text", `{"path":"checkpoint.v1.md","text":"working"}`, controlInput),
			contract("event", "target kind path text key to", "kind", `{"kind":"message","text":"Question","to":"user"}`, controlInput),
			contract("query", "repo base", "repo", `{"repo":"owner/repo"}`, issueInput),
			readContract("issue_read", "repo base number", "repo number", `{"repo":"owner/repo","number":1}`, issueInput),
			readContract("issue_children", "repo base number", "repo number", `{"repo":"owner/repo","number":1}`, issueInput),
			writeContract("issue_create", "repo base key title body parent", "repo key title body", `{"repo":"owner/repo","key":"producer-1","title":"Produce fixture","body":"Create the required fixture."}`, issueInput),
			writeContract("issue_update", "repo base number body", "repo number body", `{"repo":"owner/repo","number":1,"body":"Updated requirements."}`, issueInput),
			writeContract("issue_comment", "repo base number body", "repo number body", `{"repo":"owner/repo","number":1,"body":"Evidence recorded."}`, issueInput),
			writeContract("issue_close", "repo base number evidence body", "repo number evidence", `{"repo":"owner/repo","number":1,"evidence":"https://github.com/owner/repo/pull/2"}`, issueInput),
			writeContract("issue_assign", "repo base number", "repo number", `{"repo":"owner/repo","number":1}`, issueInput),
			writeContract("pr_reply", "repo number kind thread_id comment_id body", "repo number kind comment_id body", `{"repo":"owner/repo","number":1,"kind":"thread","thread_id":"thread-1","comment_id":1,"body":"Fixed in commit."}`, prInput),
			writeContract("pr_resolve", "repo number thread_id", "repo number thread_id", `{"repo":"owner/repo","number":1,"thread_id":"thread-1"}`, prInput),
		}
		for _, op := range []string{"github_pr", "github_comments", "github_linked_prs", "github_threads", "github_reviews", "github_run", "github_job_log"} {
			out = append(out, readContract(op, "repo base number", "repo number", `{"repo":"owner/repo","number":1}`, githubInput))
		}
		out = append(out,
			readContract("github_checks", "repo base number sha", "repo number sha", `{"repo":"owner/repo","number":1,"sha":"abc123"}`, githubInput),
			readContract("github_base", "repo base", "repo", `{"repo":"owner/repo","base":"main"}`, githubInput),
			readContract("github_merged_prs", "repo base since", "repo since", `{"repo":"owner/repo","since":"2026-01-01T00:00:00Z"}`, githubInput),
			readContract("github_prs", "repo base", "repo", `{"repo":"owner/repo"}`, githubInput),
			readContract("github_commit", "repo base sha", "repo sha", `{"repo":"owner/repo","sha":"main"}`, githubInput),
			readContract("github_file", "repo base path", "repo path", `{"repo":"owner/repo","path":"README.md"}`, githubInput),
			readContract("github_runs", "repo base", "repo", `{"repo":"owner/repo"}`, githubInput),
		)
		return out
	}
	return nil
}
func readContract(op, fields, required, example string, input func() Input) definition {
	d := contract(op, fields, required, example, input)
	d.read = true
	return d
}
func writeContract(op, fields, required, example string, input func() Input) definition {
	d := contract(op, fields, required, example, input)
	d.write = true
	return d
}
func IsWriteOperationForContract(tool, operation string, version int) bool {
	d, ok := lookupForContract(tool, operation, version)
	return ok && d.write
}
func definitionsForContract(tool string, version int) []definition {
	if version != 1 && version != 2 {
		return nil
	}
	var out []definition
	for _, d := range allDefinitions(tool) {
		if d.first <= version && version <= d.last {
			out = append(out, d)
		}
	}
	return out
}
func definitions(tool string) []definition      { return definitionsForContract(tool, 1) }
func lookup(tool, op string) (definition, bool) { return lookupForContract(tool, op, 1) }
func lookupForContract(tool, op string, version int) (definition, bool) {
	for _, d := range definitionsForContract(tool, version) {
		if d.operation == op {
			return d, true
		}
	}
	return definition{}, false
}
func Operations(tool string) []string { return OperationsForContract(tool, 1) }
func OperationsForContract(tool string, version int) []string {
	var out []string
	for _, d := range definitionsForContract(tool, version) {
		out = append(out, d.operation)
	}
	return out
}
func IsReadOperationForContract(tool, operation string, version int) bool {
	definition, ok := lookupForContract(tool, operation, version)
	return ok && definition.read
}
func Examples(tool string) []Example { return ExamplesForContract(tool, 1) }
func ExamplesForContract(tool string, version int) []Example {
	var out []Example
	for _, d := range definitionsForContract(tool, version) {
		out = append(out, Example{d.operation, json.RawMessage(d.example)})
	}
	return out
}
func Help(tool string) string { return HelpForContract(tool, 1) }
func HelpForContract(tool string, version int) string {
	var out strings.Builder
	for _, ex := range ExamplesForContract(tool, version) {
		fmt.Fprintf(&out, "INPUT %s(op=%q,payload=%s)\n", tool, ex.Operation, ex.Payload)
	}
	return out.String()
}

func DecodeInput(tool, op string, data json.RawMessage) (Input, error) {
	return DecodeInputForContract(tool, op, 1, data)
}
func DecodeInputForContract(tool, op string, version int, data json.RawMessage) (Input, error) {
	return decodeInputForContract(tool, op, version, data, ControlLimit, DecodeStrict)
}

// DecodeStoredInputForContract accepts only previously authenticated canonical originals.
func DecodeStoredInputForContract(tool, op string, version int, data json.RawMessage) (Input, error) {
	return decodeInputForContract(tool, op, version, data, StoredCanonicalLimit, DecodeBroker)
}
func decodeInputForContract(tool, op string, version int, data json.RawMessage, limit int, decode func([]byte, any) error) (Input, error) {
	if len(data) == 0 {
		return nil, &ToolInputError{Message: "payload is required and must be an object"}
	}
	if len(data) > limit {
		return nil, &EncodingError{Cause: fmt.Errorf("request exceeds size limit")}
	}
	// The first pass catches ambiguity before shape correction can hide it.
	var object map[string]json.RawMessage
	if err := decode(data, &object); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return nil, &ToolInputError{Message: "payload must be an object"}
		}
		if err.Error() == "null request" {
			return nil, &ToolInputError{Message: "payload must be an object"}
		}
		return nil, &EncodingError{Cause: err}
	}
	d, ok := lookupForContract(tool, op, version)
	if !ok {
		return nil, &ToolInputError{Message: "unsupported tool operation"}
	}
	for k, v := range object {
		if !slices.Contains(d.fields, k) {
			return nil, &ToolInputError{Message: "unknown input field; follow the operation schema"}
		}
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return nil, &ToolInputError{Message: "null input field: " + k}
		}
	}
	for _, k := range d.required {
		if _, ok := object[k]; !ok {
			return nil, &ToolInputError{Message: "missing required field: " + k}
		}
	}
	in := d.input()
	if err := decode(data, in); err != nil {
		return nil, &ToolInputError{Message: "payload field types or nested fields do not match this operation"}
	}
	if err := validateInput(op, in, object, d.required); err != nil {
		return nil, &ToolInputError{Message: err.Error()}
	}
	return in, nil
}
func validateInput(op string, in Input, raw map[string]json.RawMessage, required []string) error {
	for _, k := range required {
		var text string
		if json.Unmarshal(raw[k], &text) == nil && text == "" && k != "text" && k != "body" {
			return fmt.Errorf("nonempty %s required", k)
		}
	}
	switch p := in.(type) {
	case *BrainReadInput:
		if p.MaxBytes != nil && (*p.MaxBytes < 1 || *p.MaxBytes > BrainReadLimit) {
			return fmt.Errorf("max_bytes must be between 1 and %d", BrainReadLimit)
		}
	case *ResultInput:
		if p.Round < 1 {
			return fmt.Errorf("positive round required")
		}
	case *ContinueInput:
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(raw["request"], &nested); err != nil {
			return fmt.Errorf("invalid continue request")
		}
		for _, value := range nested {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return fmt.Errorf("null continue field")
			}
		}
		if children, present := nested["acknowledged_children"]; present {
			if err := validateChildIDs(children); err != nil {
				return err
			}
		}
		return validateContinue(p.Request)
	case *YieldInput:
		return validateChildIDs(raw["children"])
	case *CancelInput:
		if p.Request.Reason == "" {
			return fmt.Errorf("cancellation reason required")
		}
	case *Request:
		if p.SchemaVersion != 1 {
			return fmt.Errorf("schema_version must be 1")
		}
		if p.Lease < 0 || p.RevisionPR < 0 {
			return fmt.Errorf("lease and revision_pr cannot be negative")
		}
		if p.Mode != "" && p.Mode != ModeExistingPRRevision {
			return fmt.Errorf("unsupported helper mode")
		}
	case *TextInput:
		if len(p.Text) > EventTextLimit || strings.ContainsAny(p.Text+p.Key, "\r\n") {
			return fmt.Errorf("invalid event payload")
		}
	case *FileInput:
		if p.Text != nil && len(*p.Text) > ControlLimit {
			return fmt.Errorf("text exceeds size limit")
		}
	case *TerminalInput:
		if len(p.Command) > TerminalLimit {
			return fmt.Errorf("command exceeds size limit")
		}
	case *PathInput:
		if !slices.Contains(epicReferences, p.Path) {
			return fmt.Errorf("unsupported epic reference")
		}
	case *ControlInput:
		if p.Ack < 0 {
			return fmt.Errorf("ack cannot be negative")
		}
		if len(p.Text) > ControlLimit {
			return fmt.Errorf("text exceeds size limit")
		}
		if op == "event" && !slices.Contains(coordinatorEvents, p.Kind) {
			return fmt.Errorf("unsupported coordinator event")
		}
		if p.To != "" && p.To != "user" {
			return fmt.Errorf("unsupported event recipient")
		}
	case *IssueInput:
		if err := validateRepo(p.Repo); err != nil {
			return err
		}
		if slices.Contains(required, "number") && p.Number < 1 {
			return fmt.Errorf("positive number required")
		}
		if len(p.Title) > 16384 || len(p.Body) > ControlLimit {
			return fmt.Errorf("issue payload exceeds limit")
		}
	case *GitHubReadInput:
		if err := validateRepo(p.Repo); err != nil {
			return err
		}
		if slices.Contains(required, "number") && p.Number < 1 {
			return fmt.Errorf("positive number required")
		}
		if op == "github_merged_prs" && p.Since.IsZero() {
			return fmt.Errorf("since timestamp required")
		}
	case *PRActionInput:
		if err := validateRepo(p.Repo); err != nil {
			return err
		}
		if p.Number < 1 {
			return fmt.Errorf("positive PR number required")
		}
		if op == "pr_reply" {
			if !slices.Contains(replyKinds, p.Kind) || p.CommentID < 1 || strings.TrimSpace(p.Body) == "" || len(p.Body) > 65536 {
				return fmt.Errorf("invalid PR reply")
			}
			if p.Kind == "thread" && p.ThreadID == "" {
				return fmt.Errorf("thread_id required")
			}
			if p.Kind != "thread" && p.ThreadID != "" {
				return fmt.Errorf("thread_id requires thread reply")
			}
		}
	}
	return nil
}
func validateContinue(r ContinueRequest) error {
	if r.SchemaVersion != 1 || r.RequestKey == "" || r.ExpectedRound < 1 || r.Prompt == "" {
		return fmt.Errorf("continue requires schema_version:1, request_key, current expected_round and prompt")
	}
	return nil
}
func validateRepo(repo string) error {
	if repo == "" || strings.Count(repo, "/") != 1 || strings.ContainsAny(repo, "\r\n\x00") {
		return fmt.Errorf("repository required")
	}
	return nil
}

func validateChildIDs(raw json.RawMessage) error {
	var ids []*string
	if err := json.Unmarshal(raw, &ids); err != nil || ids == nil {
		return fmt.Errorf("child IDs must be nonempty strings")
	}
	for _, id := range ids {
		if id == nil || *id == "" {
			return fmt.Errorf("child IDs must be nonempty strings")
		}
	}
	return nil
}

func DecodeRequestForContract(tool string, version int, data []byte) (Envelope, Input, error) {
	var envelope Envelope
	if _, err := jsonobject.Canonical(data, ControlLimit); err != nil {
		return envelope, nil, &EncodingError{Cause: err}
	}
	if err := DecodeStrict(data, &envelope); err != nil {
		return envelope, nil, &ToolInputError{Message: "request requires exactly op and payload"}
	}
	if envelope.Op == "" || len(envelope.Payload) == 0 {
		return envelope, nil, &ToolInputError{Message: "request requires op and payload"}
	}
	input, err := DecodeInputForContract(tool, envelope.Op, version, envelope.Payload)
	return envelope, input, err
}
