package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/kingpinXD/factory/internal/jsonobject"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type acceptanceReadDiagnostic struct {
	Reason              string               `json:"reason"`
	Tool                string               `json:"tool"`
	Operation           string               `json:"op"`
	PayloadBytes        int                  `json:"payload_bytes"`
	PayloadCanonical    bool                 `json:"payload_canonical"`
	PayloadSHA256       string               `json:"payload_sha256,omitempty"`
	ThreadMatched       *bool                `json:"thread_matched,omitempty"`
	TurnMatched         *bool                `json:"turn_matched,omitempty"`
	BindingMatched      *bool                `json:"binding_matched,omitempty"`
	GrantReloaded       *bool                `json:"grant_reloaded,omitempty"`
	GrantBindingMatched *bool                `json:"grant_binding_matched,omitempty"`
	WorktreeMatched     *bool                `json:"worktree_matched,omitempty"`
	SelectionMatched    *bool                `json:"selection_matched,omitempty"`
	DecodeErrorType     string               `json:"decode_error_type,omitempty"`
	DecodeErrorMessage  string               `json:"decode_error_message,omitempty"`
	Arguments           *acceptanceJSONShape `json:"arguments,omitempty"`
	Payload             *acceptanceJSONShape `json:"payload,omitempty"`
}

type acceptanceJSONShape struct {
	Type          string              `json:"type"`
	DecodedType   string              `json:"decoded_type,omitempty"`
	Keys          []acceptanceJSONKey `json:"keys,omitempty"`
	KeysTruncated bool                `json:"keys_truncated,omitempty"`
}

type acceptanceJSONKey struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func acceptanceJSONType(data []byte) string {
	data = bytes.TrimSpace(data)
	if !json.Valid(data) {
		return "other"
	}
	switch data[0] {
	case '{':
		return "object"
	case '[':
		return "array"
	case '"':
		return "string"
	case 't', 'f':
		return "bool"
	case 'n':
		return "null"
	default:
		return "number"
	}
}

func acceptanceShape(data []byte, transport bool) (*acceptanceJSONShape, map[string]json.RawMessage) {
	shape := &acceptanceJSONShape{Type: "other"}
	// A JSON string can escape each input byte as six transport bytes.
	if len(data) > 6*toolcontract.ControlLimit+2 {
		return shape, nil
	}
	shape.Type = acceptanceJSONType(data)
	if shape.Type == "string" {
		var decoded string
		_ = json.Unmarshal(data, &decoded)
		data = []byte(decoded)
		shape.DecodedType = "other"
		if len(data) <= toolcontract.ControlLimit {
			shape.DecodedType = acceptanceJSONType(data)
		}
	}
	kind := shape.Type
	if kind == "string" {
		kind = shape.DecodedType
	}
	if transport && shape.Type != "object" && shape.Type != "string" {
		shape.Type = "other"
	}
	if kind != "object" || len(data) > toolcontract.ControlLimit {
		return shape, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil {
		return shape, nil
	}
	names := make([]string, 0, len(object))
	for name := range object {
		names = append(names, name)
	}
	slices.Sort(names)
	if len(names) > 16 {
		names = names[:16]
		shape.KeysTruncated = true
	}
	for _, name := range names {
		short := []rune(name)
		if len(short) > 32 {
			short = short[:32]
		}
		shape.Keys = append(shape.Keys, acceptanceJSONKey{Name: string(short), Type: acceptanceJSONType(object[name])})
	}
	return shape, object
}

func (d *acceptanceReadDiagnostic) decodeFailure(err error) {
	d.DecodeErrorType = fmt.Sprintf("%T", err)
	var input *toolcontract.ToolInputError
	var encoding *toolcontract.EncodingError
	switch {
	case errors.As(err, &input):
		// DecodeRequestForContract's input errors contain fixed text and catalog field names only.
		d.DecodeErrorMessage = input.Message
	case errors.As(err, &encoding):
		d.DecodeErrorMessage = "invalid request encoding"
		if errors.Is(encoding, jsonobject.ErrObject) {
			d.DecodeErrorMessage = jsonobject.ErrObject.Error()
		} else {
			switch encoding.Error() {
			case "request exceeds size limit", "aliases forbidden", "duplicate or invalid field", "expected one JSON object", "null request":
				d.DecodeErrorMessage = encoding.Error()
			}
		}
	default:
		d.DecodeErrorMessage = "request decoding failed"
	}
}

func cloneAcceptanceShape(shape *acceptanceJSONShape) *acceptanceJSONShape {
	if shape == nil {
		return nil
	}
	copy := *shape
	copy.Keys = append([]acceptanceJSONKey(nil), shape.Keys...)
	return &copy
}

type acceptanceReadRefusal struct {
	Diagnostic acceptanceReadDiagnostic
	cause      error
}

func (e *acceptanceReadRefusal) Error() string {
	facts, _ := json.Marshal(e.Diagnostic)
	return "acceptance local control read refused: " + string(facts)
}

func (e *acceptanceReadRefusal) Unwrap() error { return e.cause }

func (d acceptanceReadDiagnostic) refuse(reason string, cause error) error {
	d.Reason = reason
	return &acceptanceReadRefusal{Diagnostic: d, cause: cause}
}

func matched(value bool) *bool { return &value }

func acceptanceReadFacts(call ToolInvocation, thread, turn string) acceptanceReadDiagnostic {
	d := acceptanceReadDiagnostic{Tool: "unknown", Operation: "unknown", ThreadMatched: matched(call.ThreadID == thread), TurnMatched: matched(call.TurnID == turn)}
	switch call.Tool {
	case toolcontract.FactoryControl, toolcontract.FactoryHelper, toolcontract.ManagedFiles, toolcontract.ManagedTerminal:
		d.Tool = call.Tool
	}
	var object map[string]json.RawMessage
	d.Arguments, object = acceptanceShape(call.Arguments, true)
	if object == nil {
		return d
	}
	var operation string
	_ = json.Unmarshal(object["op"], &operation)
	for _, op := range toolcontract.Operations(toolcontract.FactoryControl) {
		if operation == op {
			d.Operation = op
			break
		}
	}
	payload, present := object["payload"]
	if !present {
		return d
	}
	d.Payload, _ = acceptanceShape(payload, false)
	d.PayloadBytes = len(payload)
	if canonical, err := jsonobject.Canonical(payload, toolcontract.ControlLimit); err == nil {
		d.PayloadCanonical = true
		d.PayloadSHA256 = hash(canonical)
	}
	return d
}

func (p *Protocol) recordAcceptanceReadFailure(err error) {
	var refusal *acceptanceReadRefusal
	if !errors.As(err, &refusal) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.diagnostics.LocalControlRefusals) == diagnosticEventLimit {
		copy(p.diagnostics.LocalControlRefusals, p.diagnostics.LocalControlRefusals[1:])
		p.diagnostics.LocalControlRefusals = p.diagnostics.LocalControlRefusals[:diagnosticEventLimit-1]
	}
	p.diagnostics.LocalControlRefusals = append(p.diagnostics.LocalControlRefusals, refusal.Diagnostic)
}
