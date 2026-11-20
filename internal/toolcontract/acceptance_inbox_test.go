package toolcontract

import (
	"errors"
	"reflect"
	"testing"
)

func TestContract2InboxDecodePreservesPayloadShape(t *testing.T) {
	for _, tc := range []struct {
		name, arguments, op, payload, errorKind, errorText string
		valid                                              bool
	}{
		{"omitted", `{"op":"inbox"}`, "inbox", ``, "input", "request requires op and payload", false},
		{"null", `{"op":"inbox","payload":null}`, "", ``, "encoding", "invalid bounded JSON object", false},
		{"empty", `{"op":"inbox","payload":{}}`, "inbox", `{}`, "", "", true},
		{"whitespace", "{\"op\":\"inbox\",\"payload\":{ \n\t }}", "inbox", "{ \n\t }", "", "", true},
		{"envelope order", ` { "payload" : { }, "op" : "inbox" } `, "inbox", `{ }`, "", "", true},
		{"empty known field", `{"op":"inbox","payload":{"target":""}}`, "inbox", `{"target":""}`, "", "", true},
		{"zero known field", `{"op":"inbox","payload":{"ack":0}}`, "inbox", `{"ack":0}`, "", "", true},
		{"known field order", `{"op":"inbox","payload":{ "target" : "", "ack" : 0 }}`, "inbox", `{ "target" : "", "ack" : 0 }`, "", "", true},
		{"unknown empty field", `{"op":"inbox","payload":{"extra":""}}`, "inbox", `{"extra":""}`, "input", "unknown input field; follow the operation schema", false},
		{"extra envelope field", `{"op":"inbox","payload":{},"extra":""}`, "inbox", `{}`, "input", "request requires exactly op and payload", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envelope, _, err := DecodeRequestForContract(FactoryControl, 2, []byte(tc.arguments))
			if (err == nil) != tc.valid || envelope.Op != tc.op || string(envelope.Payload) != tc.payload {
				t.Fatalf("payload=%q err=%v; want payload=%q valid=%v", envelope.Payload, err, tc.payload, tc.valid)
			}
			if err != nil {
				var input *ToolInputError
				var encoding *EncodingError
				if err.Error() != tc.errorText || tc.errorKind == "input" && !errors.As(err, &input) || tc.errorKind == "encoding" && !errors.As(err, &encoding) {
					t.Fatalf("error %T %v; want %s %q", err, err, tc.errorKind, tc.errorText)
				}
			}
		})
	}
}

func TestContract2InboxAdvertisedSchema(t *testing.T) {
	schema := SchemaForContract(FactoryControl, 2)
	choices := schema["oneOf"].([]any)
	var inbox map[string]any
	for _, choice := range choices {
		object := choice.(map[string]any)
		properties := object["properties"].(map[string]any)
		if reflect.DeepEqual(properties["op"].(map[string]any)["enum"], []string{"inbox"}) {
			inbox = object
		}
	}
	want := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"op", "payload"},
		"properties": map[string]any{
			"op": map[string]any{"type": "string", "enum": []string{"inbox"}},
			"payload": map[string]any{
				"type": "object", "additionalProperties": false, "required": []string{},
				"properties": map[string]any{
					"target": map[string]any{"type": "string"},
					"ack":    map[string]any{"type": "integer", "minimum": 0},
				},
			},
		},
	}
	if !reflect.DeepEqual(inbox, want) {
		t.Fatalf("advertised contract changed: %#v", inbox)
	}
}

func TestContract2InboxPlausibleRejectedShapes(t *testing.T) {
	for _, tc := range []struct{ name, arguments, kind, message string }{
		{"null payload", `{"op":"inbox","payload":null}`, "encoding", "invalid bounded JSON object"},
		{"encoded payload", `{"op":"inbox","payload":"{}"}`, "input", "payload must be an object"},
		{"top-level target", `{"op":"inbox","payload":{},"target":""}`, "input", "request requires exactly op and payload"},
		{"top-level ack", `{"op":"inbox","payload":{},"ack":0}`, "input", "request requires exactly op and payload"},
		{"top-level null metadata", `{"op":"inbox","payload":{},"metadata":null}`, "encoding", "invalid bounded JSON object"},
		{"tool wrapper", `{"factory_control":{"op":"inbox","payload":{}}}`, "input", "request requires exactly op and payload"},
		{"request wrapper", `{"request":{"op":"inbox","payload":{}}}`, "input", "request requires exactly op and payload"},
		{"input wrapper", `{"input":{"op":"inbox","payload":{}}}`, "input", "request requires exactly op and payload"},
		{"flattened ack", `{"op":"inbox","ack":0}`, "input", "request requires exactly op and payload"},
		{"nested null", `{"op":"inbox","payload":{"target":null}}`, "encoding", "invalid bounded JSON object"},
		// JSON Schema integer includes numeric representations with a zero fraction.
		{"advertised integer exponent", `{"op":"inbox","payload":{"ack":0e0}}`, "input", "payload field types or nested fields do not match this operation"},
		{"advertised integer fraction", `{"op":"inbox","payload":{"ack":0.0}}`, "input", "payload field types or nested fields do not match this operation"},
		// A surrogate pair is a valid JSON encoding of a string, but not YAML's.
		{"advertised escaped Unicode", `{"op":"inbox","payload":{"target":"\ud83d\ude00"}}`, "input", "request requires exactly op and payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := DecodeRequestForContract(FactoryControl, 2, []byte(tc.arguments))
			var encoding *EncodingError
			var input *ToolInputError
			if err == nil || err.Error() != tc.message || tc.kind == "encoding" && !errors.As(err, &encoding) || tc.kind == "input" && !errors.As(err, &input) {
				t.Fatalf("got %T %v; want %s %q", err, err, tc.kind, tc.message)
			}
		})
	}
}
