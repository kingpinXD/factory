package toolcontract

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestEveryOperationExampleMatchesItsInputAndSchema(t *testing.T) {
	expected := map[string]string{
		FactoryHelper: "lease_check inbox heartbeat intent done error brain_read submit result yield continue",
		ManagedFiles:  "read write append", ManagedTerminal: "run",
		FactoryControl: "brain_read plan_input epic_file helper_submit helper_continue helper_result helper_cancel events inbox lease_check heartbeat file_read file_write event query issue_read issue_children issue_create issue_update issue_comment issue_close issue_assign pr_reply pr_resolve github_pr github_comments github_linked_prs github_threads github_reviews github_run github_job_log github_checks github_base github_merged_prs github_prs github_commit github_file github_runs",
	}
	for tool, ops := range expected {
		if !reflect.DeepEqual(Operations(tool), strings.Fields(ops)) {
			t.Fatalf("%s operations changed: %v", tool, Operations(tool))
		}
		schema := Schema(tool)
		if len(schema["oneOf"].([]any)) != len(strings.Fields(ops)) {
			t.Fatalf("%s schema lost operation", tool)
		}
		for _, example := range Examples(tool) {
			t.Run(tool+"/"+example.Operation, func(t *testing.T) {
				if _, err := DecodeInput(tool, example.Operation, example.Payload); err != nil {
					t.Fatalf("generated example rejected: %s / %v", example.Payload, err)
				}
				if !strings.Contains(Help(tool), string(example.Payload)) {
					t.Fatal("help omitted current input example")
				}
				operation := OperationSchema(tool, example.Operation)
				props := operation["properties"].(map[string]any)
				if got := props["op"].(map[string]any)["enum"].([]string); len(got) != 1 || got[0] != example.Operation {
					t.Fatalf("wrong operation schema: %v", got)
				}
				payload := props["payload"].(map[string]any)
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(example.Payload, &fields); err != nil {
					t.Fatal(err)
				}
				for k := range fields {
					if _, ok := payload["properties"].(map[string]any)[k]; !ok {
						t.Fatalf("example field absent from schema: %s", k)
					}
				}
				for _, key := range payload["required"].([]string) {
					var missing map[string]json.RawMessage
					_ = json.Unmarshal(example.Payload, &missing)
					delete(missing, key)
					data, _ := json.Marshal(missing)
					if _, err := DecodeInput(tool, example.Operation, data); err == nil {
						t.Fatalf("required %s was not enforced", key)
					}
				}
				for name, property := range payload["properties"].(map[string]any) {
					field := property.(map[string]any)
					invalid := map[string]json.RawMessage{"string": json.RawMessage(`7`), "integer": json.RawMessage(`"wrong"`), "boolean": json.RawMessage(`"wrong"`), "object": json.RawMessage(`[]`), "array": json.RawMessage(`{}`)}[field["type"].(string)]
					var wrong map[string]json.RawMessage
					_ = json.Unmarshal(example.Payload, &wrong)
					wrong[name] = invalid
					data, _ := json.Marshal(wrong)
					if _, err := DecodeInput(tool, example.Operation, data); err == nil {
						t.Fatalf("schema type for %s is not enforced", name)
					}
					if enum, ok := field["enum"]; ok {
						switch enum.(type) {
						case []string:
							wrong[name] = json.RawMessage(`"__unsupported__"`)
						case []int:
							wrong[name] = json.RawMessage(`99999`)
						default:
							t.Fatalf("unexpected enum type for %s", name)
						}
						data, _ = json.Marshal(wrong)
						if _, err := DecodeInput(tool, example.Operation, data); err == nil {
							t.Fatalf("schema enum for %s is not enforced", name)
						}
					}
				}
				fields["read_only"] = json.RawMessage(`true`)
				withResponse, _ := json.Marshal(fields)
				if _, err := DecodeInput(tool, example.Operation, withResponse); err == nil {
					t.Fatal("response-only read_only accepted as input")
				}
			})
		}
	}
	if Schema("unknown") != nil || OperationSchema(FactoryHelper, "unknown") != nil {
		t.Fatal("unknown tool/operation acquired a schema")
	}
}
func TestInputValidationKeepsAmbiguousEncodingHard(t *testing.T) {
	for _, op := range []string{"submit", "unsupported"} {
		for _, data := range []string{`{"request_key":"a","request_key":"b","component":"x","prompt":"p"}`, `{} {}`, `{"request":{"x":1,"x":2}}`, `path: &a x\nother: *a`, `{true:1}`} {
			_, err := DecodeInput(FactoryHelper, op, []byte(data))
			var hard *EncodingError
			var correction *ToolInputError
			if !errors.As(err, &hard) || errors.As(err, &correction) {
				t.Fatalf("ambiguous input softened: %s / %v", data, err)
			}
		}
	}
}
func TestInputShapeAndBounds(t *testing.T) {
	cases := []struct{ tool, op, data, reason string }{
		{FactoryHelper, "brain_read", `{"path":"repo","max_bytes":0}`, "max_bytes"},
		{FactoryHelper, "brain_read", `{"path":"repo","max_bytes":65537}`, "max_bytes"},
		{FactoryHelper, "result", `{"job_id":"j","round":0}`, "round"},
		{FactoryControl, "helper_result", `{"job_id":"j","round":1,"read_only":true}`, "unknown"},
		{FactoryHelper, "submit", `{"request_key":"r","component":"x","prompt":"p","owner_id":"foreign"}`, "unknown"},
		{FactoryHelper, "continue", `{"job_id":"j","request":{"schema_version":1,"request_key":"r","expected_round":0,"prompt":"p"}}`, "current expected_round"},
		{FactoryHelper, "continue", `{"job_id":"j","request":{"schema_version":1,"request_key":"r","expected_round":1,"prompt":"p","output_path":null}}`, "null continue"},
		{ManagedFiles, "write", `{"path":"x"}`, "missing required field: text"},
		{ManagedFiles, "write", `{"path":"x","text":null}`, "null input field: text"},
		{ManagedTerminal, "run", `{"command":""}`, "command"},
		{FactoryControl, "epic_file", `{"path":"SECRET.md"}`, "epic reference"},
		{FactoryControl, "event", `{"kind":"transition"}`, "coordinator event"},
		{FactoryControl, "helper_submit", `{"schema_version":2,"request_key":"r","owner_id":"o","component":"x","prompt":"p"}`, "schema_version"},
		{FactoryControl, "helper_submit", `{"schema_version":1,"request_key":"r","owner_id":"o","component":"x","prompt":"p","mode":"other"}`, "mode"},
		{FactoryControl, "pr_reply", `{"repo":"o/r","number":1,"kind":"thread","comment_id":1,"body":"fixed"}`, "thread_id"},
	}
	for _, c := range cases {
		t.Run(c.tool+"/"+c.op+"/"+c.reason, func(t *testing.T) {
			_, err := DecodeInput(c.tool, c.op, []byte(c.data))
			var correction *ToolInputError
			if !errors.As(err, &correction) || !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("wrong input guard: %v", err)
			}
		})
	}
	if _, err := DecodeInput(ManagedFiles, "write", []byte(`{"path":"x","text":""}`)); err != nil {
		t.Fatalf("empty write was rejected: %v", err)
	}
	for _, n := range []int{1, 65536} {
		data, _ := json.Marshal(map[string]any{"path": "repo", "max_bytes": n})
		if _, err := DecodeInput(FactoryHelper, "brain_read", data); err != nil {
			t.Fatalf("allowed boundary %d: %v", n, err)
		}
	}
	data, _ := json.Marshal(map[string]string{"command": strings.Repeat("界", 50000)})
	if _, err := DecodeInput(ManagedTerminal, "run", data); err == nil {
		t.Fatal("UTF-8 byte limit ignored")
	}
	data, _ = json.Marshal(map[string]string{"command": strings.Repeat("x", TerminalLimit)})
	if _, err := DecodeInput(ManagedTerminal, "run", data); err != nil {
		t.Fatalf("allowed command boundary: %v", err)
	}
}
func TestSchemaUsesIndependentMapsAndBoundedInputHelp(t *testing.T) {
	if ControlLimit != 1048576 || BrainReadLimit != 65536 || TerminalLimit != 131072 {
		t.Fatal("contract byte bounds changed")
	}
	get := func() map[string]any {
		return OperationSchema(FactoryHelper, "brain_read")["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)["max_bytes"].(map[string]any)
	}
	if get()["minimum"] != 1 || get()["maximum"] != 65536 {
		t.Fatalf("read bound schema changed: %v", get())
	}
	get()["maximum"] = 9
	if get()["maximum"] != 65536 {
		t.Fatal("caller mutated shared schema state")
	}
	if strings.Contains(Help(FactoryControl), "RESPONSE") || strings.Contains(Help(FactoryControl), "read_only") {
		t.Fatal("response fields entered generated INPUT guidance")
	}
	data := []byte(`{"` + strings.Repeat("x", 4096) + `":1}`)
	_, err := DecodeInput(FactoryHelper, "submit", data)
	if err == nil || len(err.Error()) > 256 {
		t.Fatalf("unbounded correction: %v", err)
	}
	raw, err := json.Marshal(ToolFailure{ToolError: "invalid"})
	if err != nil || string(raw) != `{"tool_error":"invalid"}` {
		t.Fatalf("legacy failure wire changed: %s/%v", raw, err)
	}
}

func TestSchemaEnumMutationCannotChangeValidation(t *testing.T) {
	cases := []struct{ op, field, unsupported, payload string }{
		{"event", "kind", "transition", `{"kind":"transition"}`},
		{"epic_file", "path", "SECRET.md", `{"path":"SECRET.md"}`},
		{"pr_reply", "kind", "unsupported", `{"repo":"o/r","number":1,"kind":"unsupported","comment_id":1,"body":"fixed"}`},
	}
	for _, c := range cases {
		props := OperationSchema(FactoryControl, c.op)["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)
		props[c.field].(map[string]any)["enum"].([]string)[0] = c.unsupported
		if _, err := DecodeInput(FactoryControl, c.op, []byte(c.payload)); err == nil {
			t.Fatalf("schema mutation changed validation for %s", c.op)
		}
	}
}

func TestMissingPayloadIsShapeCorrectionAndMalformedStaysHard(t *testing.T) {
	for _, data := range [][]byte{nil, []byte{}} {
		_, err := DecodeInput(FactoryHelper, "lease_check", data)
		var input *ToolInputError
		var encoding *EncodingError
		if !errors.As(err, &input) || errors.As(err, &encoding) {
			t.Fatalf("omitted payload must be an input correction: %v", err)
		}
	}
	for _, data := range []string{" ", "{", `{} {}`, `{"children":["a"],"children":["b"]}`} {
		_, err := DecodeInput(FactoryHelper, "yield", []byte(data))
		var encoding *EncodingError
		if !errors.As(err, &encoding) {
			t.Fatalf("malformed/duplicate payload must stay hard: %q / %v", data, err)
		}
	}
}
func TestRawChildIDsMustBeNonemptyStrings(t *testing.T) {
	for _, tool := range []string{FactoryHelper, FactoryControl} {
		op := "continue"
		if tool == FactoryControl {
			op = "helper_continue"
		}
		for _, member := range []string{`null`, `7`, `true`, `{}`, `""`} {
			data := []byte(`{"job_id":"j","request":{"schema_version":1,"request_key":"r","expected_round":1,"prompt":"p","acknowledged_children":[` + member + `]}}`)
			_, err := DecodeInput(tool, op, data)
			var input *ToolInputError
			var encoding *EncodingError
			if !errors.As(err, &input) || errors.As(err, &encoding) {
				t.Errorf("continue child ID %s must reject as input: %v", member, err)
			}
		}
	}
	for _, member := range []string{`null`, `7`, `true`, `{}`, `""`} {
		_, err := DecodeInput(FactoryHelper, "yield", []byte(`{"children":[`+member+`]}`))
		var input *ToolInputError
		var encoding *EncodingError
		if !errors.As(err, &input) || errors.As(err, &encoding) {
			t.Errorf("yield child ID %s must reject as input: %v", member, err)
		}
	}
	for _, data := range []string{`{"children":[]}`, `{"children":["child-1","child-2"]}`} {
		if _, err := DecodeInput(FactoryHelper, "yield", []byte(data)); err != nil {
			t.Fatalf("valid child IDs rejected: %v", err)
		}
	}
	if _, err := DecodeInput(FactoryHelper, "continue", []byte(`{"job_id":"j","request":{"schema_version":1,"request_key":"r","expected_round":1,"prompt":"p","acknowledged_children":["child-1"]}}`)); err != nil {
		t.Fatalf("valid acknowledged child rejected: %v", err)
	}
}

func TestChildIDSchemaRequiresNonemptyStrings(t *testing.T) {
	yield := OperationSchema(FactoryHelper, "yield")["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)["children"].(map[string]any)
	next := OperationSchema(FactoryHelper, "continue")["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)["request"].(map[string]any)["properties"].(map[string]any)["acknowledged_children"].(map[string]any)
	for _, schema := range []map[string]any{yield, next} {
		items := schema["items"].(map[string]any)
		if items["type"] != "string" || items["minLength"] != 1 {
			t.Fatalf("child ID schema weakened: %v", items)
		}
	}
}
func TestLegacyDecodersKeepNullChildConversion(t *testing.T) {
	for _, decode := range []func([]byte, any) error{DecodeBroker, DecodeStrict} {
		var data struct {
			Children []string `json:"children"`
		}
		if err := decode([]byte(`{"children":[null]}`), &data); err != nil || len(data.Children) != 1 || data.Children[0] != "" {
			t.Fatalf("legacy child decoding changed: %v / %#v", err, data)
		}
	}
}
