package toolcontract

import "slices"

func Schema(tool string) map[string]any { return SchemaForContract(tool, 1) }
func SchemaForContract(tool string, version int) map[string]any {
	var choices []any
	var operations []string
	for _, d := range definitionsForContract(tool, version) {
		choices = append(choices, operationSchema(d, tool))
		operations = append(operations, d.operation)
	}
	if len(choices) == 0 {
		return nil
	}
	// Expose the required envelope even when a provider ignores composition.
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"op": map[string]any{"type": "string", "enum": operations}, "payload": map[string]any{"type": "object"}},
		"required":             []string{"op", "payload"},
		"additionalProperties": false,
		"oneOf":                choices,
	}
}
func OperationSchema(tool, op string) map[string]any {
	d, ok := lookup(tool, op)
	if !ok {
		return nil
	}
	return operationSchema(d, tool)
}
func operationSchema(d definition, tool string) map[string]any {
	op := d.operation
	props := map[string]any{}
	for _, name := range d.fields {
		field := fieldSchema(name, op)
		if slices.Contains(d.required, name) && field["type"] == "string" && name != "text" && name != "body" {
			field["minLength"] = 1
		}
		if tool == ManagedFiles && name == "text" {
			field["maxLength"] = ControlLimit
		}
		if tool == FactoryHelper && name == "text" {
			field["maxLength"] = EventTextLimit
		}
		if op == "event" && name == "kind" {
			field["enum"] = slices.Clone(coordinatorEvents)
		}
		if op == "epic_file" && name == "path" {
			field["enum"] = slices.Clone(epicReferences)
		}
		if op == "pr_reply" && name == "kind" {
			field["enum"] = slices.Clone(replyKinds)
		}
		if op == "pr_reply" && name == "body" {
			field["maxLength"] = 65536
			field["minLength"] = 1
		}
		props[name] = field
	}
	payload := map[string]any{"type": "object", "properties": props, "required": append([]string{}, d.required...), "additionalProperties": false}
	return map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string", "enum": []string{op}}, "payload": payload}, "required": []string{"op", "payload"}, "additionalProperties": false}
}
func fieldSchema(name, op string) map[string]any {
	switch name {
	case "schema_version":
		return map[string]any{"type": "integer", "enum": []int{1}}
	case "round", "expected_round", "number", "comment_id":
		out := map[string]any{"type": "integer", "minimum": 1}
		if name == "expected_round" {
			out["description"] = "CURRENT stored round, not the next round"
		}
		return out
	case "ack", "lease", "revision_pr":
		return map[string]any{"type": "integer", "minimum": 0}
	case "max_bytes":
		return map[string]any{"type": "integer", "minimum": 1, "maximum": BrainReadLimit}
	case "append":
		return map[string]any{"type": "boolean"}
	case "children", "acknowledged_children":
		return map[string]any{"type": "array", "items": map[string]any{"type": "string", "minLength": 1}}
	case "request":
		if op == "helper_cancel" {
			return map[string]any{"type": "object", "properties": map[string]any{"reason": map[string]any{"type": "string", "minLength": 1}}, "required": []string{"reason"}, "additionalProperties": false}
		}
		props := map[string]any{}
		for _, n := range []string{"schema_version", "request_key", "expected_round", "prompt", "output_path", "acknowledged_children"} {
			props[n] = fieldSchema(n, op)
		}
		for _, n := range []string{"request_key", "prompt"} {
			props[n].(map[string]any)["minLength"] = 1
		}
		return map[string]any{"type": "object", "properties": props, "required": []string{"schema_version", "request_key", "expected_round", "prompt"}, "additionalProperties": false}
	case "since":
		return map[string]any{"type": "string", "format": "date-time"}
	case "command":
		return map[string]any{"type": "string", "maxLength": TerminalLimit, "description": "At most 128 KiB of UTF-8 bytes"}
	case "title":
		return map[string]any{"type": "string", "maxLength": 16384, "description": "At most 16384 UTF-8 bytes"}
	case "text", "body":
		return map[string]any{"type": "string", "maxLength": ControlLimit, "description": "The transport and UTF-8 byte limits also apply"}
	case "mode":
		return map[string]any{"type": "string", "enum": []string{"", ModeExistingPRRevision}}
	case "to":
		return map[string]any{"type": "string", "enum": []string{"", "user"}}
	default:
		return map[string]any{"type": "string"}
	}
}
