package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"io"

	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type ToolInputError = toolcontract.ToolInputError
type ToolFailure = toolcontract.ToolFailure

func toolInputError(message string) error { return &ToolInputError{Message: message} }

func decodeToolPayload(data json.RawMessage, target any) error {
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return toolInputError("payload must be an object matching this tool's schema")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return toolInputError("payload must match this tool's schema; remove unknown fields and use the advertised field types")
	}
	if d.Decode(new(any)) != io.EOF {
		return toolInputError("payload must contain one JSON object")
	}
	return nil
}

func replyHelperToolFailure(ctx context.Context, p *Protocol, event Message, message string) error {
	return p.Reply(ctx, event, map[string]any{"contentItems": []any{map[string]any{"type": "inputText", "text": message}}, "success": false}, nil)
}

func helperToolInstructions(output string) string {
	reserved := "This helper has no reserved output file. Return its report in the final reply."
	if output != "" {
		reserved = fmt.Sprintf("The reserved output for this round is %q. Write it only through managed_files(op=\"write\" or \"append\",payload={path:the exact reserved output path,text:\"...\"}). A terminal command cannot write that host path. Do not write another output version.", output)
	}
	return `
Managed tools accept {op,payload}. For commands use managed_terminal with op="run" and payload={"command":"...","cwd":"optional repository directory"}. There is no exec operation. Commands run only in the contained repository. Read task inputs and declared prior work/set artifacts through managed_files(op="read",payload={path:exact task artifact path}); never cat a brain path in a command. Repository files use managed_files with read/write/append and payload={path,text}, with text required for writes.
` + reserved + "\nRecoverable tool mistakes return success=false with instructions; correct the request and continue.\n"
}

func helperFileSchema() map[string]any {
	schema := map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string", "enum": []string{"read", "write", "append"}}, "payload": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string", "minLength": 1}, "text": map[string]any{"type": "string", "maxLength": 1 << 20}}, "required": []string{"path"}, "additionalProperties": false}}, "required": []string{"op", "payload"}, "additionalProperties": false}

	return schema
}

func helperTerminalSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string", "enum": []string{"run"}}, "payload": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string", "minLength": 1, "maxLength": 128 << 10}, "cwd": map[string]any{"type": "string"}}, "required": []string{"command"}, "additionalProperties": false}}, "required": []string{"op", "payload"}, "additionalProperties": false}
}

func helperControlSchema() map[string]any {
	str := func() map[string]any { return map[string]any{"type": "string"} }
	integer := func() map[string]any { return map[string]any{"type": "integer", "minimum": 1} }
	stringsSchema := map[string]any{"type": "array", "items": str()}
	expectedRound := integer()
	expectedRound["description"] = "CURRENT stored helper round from helper_result, not the next round; inspect current state and confirmed completion before continuing."
	request := map[string]any{"type": "object", "properties": map[string]any{"schema_version": map[string]any{"type": "integer", "enum": []int{1}}, "request_key": str(), "expected_round": expectedRound, "prompt": str(), "output_path": str(), "acknowledged_children": stringsSchema}, "required": []string{"schema_version", "request_key", "expected_round", "prompt"}, "additionalProperties": false}
	payload := map[string]any{"type": "object", "properties": map[string]any{
		"request_key": str(), "component": str(), "prompt": str(), "output_path": str(),
		"job_id": str(), "round": integer(), "request": request, "children": stringsSchema,
		"path": str(), "max_bytes": map[string]any{"type": "integer", "minimum": 1, "maximum": 65536},
		"text": str(), "key": str(),
	}, "additionalProperties": false}
	return map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string", "enum": []string{"lease_check", "inbox", "heartbeat", "intent", "done", "error", "brain_read", "submit", "result", "yield", "continue"}}, "payload": payload}, "required": []string{"op", "payload"}, "additionalProperties": false}
}

func ValidateHelperControlInput(data []byte) error {
	var request struct {
		Op      string          `json:"op"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := decodeToolPayload(data, &request); err != nil {
		return err
	}
	var target any
	switch request.Op {
	case "lease_check", "inbox":
		target = &struct{}{}
	case "heartbeat", "intent", "done", "error":
		target = &struct {
			Text string `json:"text"`
			Key  string `json:"key"`
		}{}
	case "brain_read":
		target = &struct {
			Path     string  `json:"path"`
			MaxBytes *uint64 `json:"max_bytes"`
		}{}
	case "submit":
		target = &struct {
			RequestKey string `json:"request_key"`
			Component  string `json:"component"`
			Prompt     string `json:"prompt"`
			OutputPath string `json:"output_path"`
		}{}
	case "result":
		target = &struct {
			JobID string `json:"job_id"`
			Round int    `json:"round"`
		}{}
	case "yield":
		target = &struct {
			Children []string `json:"children"`
		}{}
	case "continue":
		target = &struct {
			JobID   string                 `json:"job_id"`
			Request helper.ContinueRequest `json:"request"`
		}{}
	default:
		return toolInputError("unsupported factory_helper operation; use the advertised submit, continue, result, yield, brain_read, inbox, lease_check, heartbeat, intent, done or error operation")
	}
	if err := decodeToolPayload(request.Payload, target); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(request.Payload, &fields)
	required := []string{}
	switch request.Op {
	case "brain_read":
		required = []string{"path"}
	case "submit":
		required = []string{"request_key", "component", "prompt"}
	case "result":
		required = []string{"job_id", "round"}
	case "continue":
		required = []string{"job_id", "request"}
	case "yield":
		required = []string{"children"}
	case "heartbeat", "intent", "done", "error":
		required = []string{"text"}
	}
	for _, name := range required {
		v := bytes.TrimSpace(fields[name])
		if len(v) == 0 || bytes.Equal(v, []byte("null")) || bytes.Equal(v, []byte(`""`)) && name != "text" {
			return toolInputError("factory_helper payload is missing a required field; follow the operation's schema")
		}
	}
	if request.Op == "result" {
		var n int
		_ = json.Unmarshal(fields["round"], &n)
		if n < 1 {
			return toolInputError("result requires a positive round")
		}
	}
	if request.Op == "brain_read" && fields["max_bytes"] != nil {
		var n uint64
		if json.Unmarshal(fields["max_bytes"], &n) != nil || n < 1 || n > 65536 {
			return toolInputError("brain_read max_bytes must be between 1 and 65536")
		}
	}
	if request.Op == "continue" {
		var next helper.ContinueRequest
		_ = json.Unmarshal(fields["request"], &next)
		if next.SchemaVersion != 1 || next.RequestKey == "" || next.ExpectedRound < 1 || next.Prompt == "" {
			return toolInputError("continue requires request={schema_version:1,request_key,expected_round,prompt,output_path?,acknowledged_children?}")
		}
	}
	return nil
}

type ToolInvocation struct {
	Arguments json.RawMessage `json:"arguments"`
	CallID    string          `json:"callId"`
	Namespace *string         `json:"namespace"`
	ThreadID  string          `json:"threadId"`
	Tool      string          `json:"tool"`
	TurnID    string          `json:"turnId"`
}

// Only the outer binding is decoded here. Inner arguments follow caller authentication.
func DecodeToolInvocation(data []byte) (ToolInvocation, error) {
	var invocation ToolInvocation
	if err := toolcontract.DecodeStrict(data, &invocation); err != nil {
		return invocation, err
	}
	if invocation.CallID == "" || invocation.ThreadID == "" || invocation.TurnID == "" || invocation.Tool == "" || len(invocation.Arguments) == 0 {
		return invocation, ErrObservationUnknown
	}
	return invocation, nil
}
func ToolArguments(invocation ToolInvocation) (json.RawMessage, error) {
	data := bytes.TrimSpace(invocation.Arguments)
	if len(data) > 0 && data[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		data = []byte(value)
	}
	if len(data) == 0 || len(data) > toolcontract.ControlLimit {
		return nil, fmt.Errorf("invalid tool argument size")
	}
	return append(json.RawMessage(nil), data...), nil
}
func DecodeToolReply(data []byte) (json.RawMessage, error) {
	var reply json.RawMessage
	if err := toolcontract.DecodeStrict(data, &reply); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), reply...), nil
}

func dynamicToolsForContract(helper bool, version int) []any {
	if version != 2 {
		return dynamicTools(helper)
	}
	names := []string{toolcontract.FactoryControl}
	if helper {
		names = []string{toolcontract.FactoryHelper, toolcontract.ManagedFiles, toolcontract.ManagedTerminal}
	}
	tools := make([]any, 0, len(names))
	for _, name := range names {
		tools = append(tools, map[string]any{"type": "function", "name": name, "description": toolcontract.HelpForContract(name, version), "inputSchema": toolcontract.SchemaForContract(name, version)})
	}
	return tools
}
func helperToolInstructionsForContract(output string, version int) string {
	if version != 2 {
		return helperToolInstructions(output)
	}
	return helperToolInstructions(output) + agents.CodexControlInstructions(version, true)
}

func ValidateHelperControlInputForContract(data []byte, version int) error {
	if version != 2 {
		return ValidateHelperControlInput(data)
	}
	_, _, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryHelper, version, data)
	return err
}

func ValidateHelperControlIngress(ctx context.Context, brain string, call controlrecord.HelperCall) (helper.Job, error) {
	job, err := (helper.Service{Brain: brain}).Job(call.JobID)
	if err != nil {
		return helper.Job{}, err
	}
	if job.Generation != call.Generation || job.Round != call.Round || job.Scope.CodexContract != 2 || job.Route.Provider != "codex" || job.State != "running" && job.State != "yielding" {
		return helper.Job{}, ErrAttemptStale
	}
	if err := ValidateHelperControlCall(brain, call.JobID, call.Generation, call.Round, call.ThreadID, call.TurnID); err != nil {
		return helper.Job{}, err
	}
	backend, ok := NewBackend(brain).(*Backend)
	if !ok || backend.Containment == nil {
		return helper.Job{}, ErrAttemptIntegration
	}
	attempt, err := backend.read(job)
	if err != nil {
		return helper.Job{}, err
	}
	if attempt.Job.ID != job.ID || attempt.Job.Generation != job.Generation || attempt.Job.Round != job.Round || attempt.Job.Scope != job.Scope || attempt.Job.Request != job.Request || attempt.Job.Route != job.Route {
		return helper.Job{}, ErrAttemptStale
	}
	if alive, err := attempt.Process.Alive(); err != nil || !alive {
		return helper.Job{}, ErrObservationUnknown
	}
	if err := backend.Containment.Check(ctx, backend.owner(attempt)); err != nil {
		return helper.Job{}, err
	}
	return job, nil
}

func ValidateHelperAttemptCallLocal(brain string, job helper.Job, call controlrecord.HelperCall) error {
	if job.Scope.CodexContract != 2 || job.Route.Provider != "codex" || job.ID != call.JobID || job.Generation != call.Generation || job.Round != call.Round || job.ConversationID != call.ThreadID || job.TurnID != call.TurnID || call.ThreadID == "" || call.TurnID == "" || call.CallID == "" {
		return ErrAttemptStale
	}
	attempt, err := (&Backend{Brain: brain}).read(job)
	if err != nil {
		return err
	}
	if attempt.Job.ID != job.ID || attempt.Job.Generation != job.Generation || attempt.Job.Round != job.Round || attempt.Job.Scope != job.Scope || attempt.Job.Request != job.Request || attempt.Job.Route != job.Route || !attempt.TurnIntent || attempt.Closed || attempt.Fenced || attempt.Ended || attempt.Rerouted || attempt.PrelaunchError != "" || attempt.ThreadID != call.ThreadID || attempt.TurnID != call.TurnID {
		return ErrAttemptStale
	}
	return nil
}

func ManagedControlReply(receipt helper.ManagedControlReceipt) (json.RawMessage, error) {
	if receipt.Operation == "yield" {
		if !receipt.Yielded || receipt.Accepted != nil {
			return nil, ErrObservationUnknown
		}
		return json.Marshal(map[string]any{"schema_version": 1, "job_id": receipt.ParentID, "state": "yielding"})
	}
	if receipt.Accepted == nil || receipt.Operation != "submit" && receipt.Operation != "continue" {
		return nil, ErrObservationUnknown
	}
	return json.Marshal(receipt.Accepted)
}
