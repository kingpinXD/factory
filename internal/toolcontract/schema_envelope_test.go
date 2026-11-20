package toolcontract

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func TestAdvertisedSchemasHaveObjectEnvelope(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, tool := range []string{FactoryControl, FactoryHelper, ManagedFiles, ManagedTerminal} {
			t.Run(fmt.Sprintf("%s/v%d", tool, version), func(t *testing.T) {
				schema := SchemaForContract(tool, version)
				var keys []string
				for key := range schema {
					keys = append(keys, key)
				}
				slices.Sort(keys)
				t.Logf("root keys=%v type=%v", keys, schema["type"])
				if schema["type"] != "object" {
					t.Fatalf("advertised schema must have top-level type object: %v", schema["type"])
				}
				if !reflect.DeepEqual(schema["required"], []string{"op", "payload"}) || schema["additionalProperties"] != false {
					t.Fatal("envelope must require exactly op and payload")
				}
				props := schema["properties"].(map[string]any)
				if len(props) != 2 || !reflect.DeepEqual(props["op"], map[string]any{"type": "string", "enum": OperationsForContract(tool, version)}) || !reflect.DeepEqual(props["payload"], map[string]any{"type": "object"}) {
					t.Fatalf("invalid root properties: %#v", props)
				}
			})
		}
	}
}

func TestAdvertisedOperationBranchesAndRequests(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, tool := range []string{FactoryControl, FactoryHelper, ManagedFiles, ManagedTerminal} {
			choices := SchemaForContract(tool, version)["oneOf"].([]any)
			defs := definitionsForContract(tool, version)
			if len(choices) != len(defs) {
				t.Fatalf("%s/v%d lost operation constraints", tool, version)
			}
			for i, d := range defs {
				t.Run(fmt.Sprintf("%s/v%d/%s", tool, version, d.operation), func(t *testing.T) {
					// Composition must retain the complete operation schema, including
					// nested fields, required keys, enums and numeric/text bounds.
					if !reflect.DeepEqual(choices[i], operationSchema(d, tool)) {
						t.Fatal("advertisement lost operation-specific constraints")
					}
					request := []byte(fmt.Sprintf(`{"op":%q,"payload":%s}`, d.operation, d.example))
					envelope, input, err := DecodeRequestForContract(tool, version, request)
					if err != nil || input == nil || envelope.Op != d.operation {
						t.Fatalf("advertised example does not decode: %s: %v", request, err)
					}
					var payload map[string]json.RawMessage
					if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					for _, required := range d.required {
						original := payload[required]
						delete(payload, required)
						missing, err := json.Marshal(Envelope{Op: d.operation, Payload: mustSchemaJSON(t, payload)})
						if err != nil {
							t.Fatal(err)
						}
						_, _, err = DecodeRequestForContract(tool, version, missing)
						var inputError *ToolInputError
						if !errors.As(err, &inputError) || err.Error() != "missing required field: "+required {
							t.Fatalf("missing %s reached wrong guard: %v", required, err)
						}
						payload[required] = original
					}
				})
			}
		}
	}
}

func TestAdvertisedEnvelopeDecoderRemainsStrict(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, tool := range []string{FactoryControl, FactoryHelper, ManagedFiles, ManagedTerminal} {
			for _, tc := range []struct{ name, request, message string }{
				{"empty", `{}`, "request requires op and payload"},
				{"missing op", `{"payload":{}}`, "request requires op and payload"},
				{"missing payload", `{"op":"inbox"}`, "request requires op and payload"},
				{"extra key", `{"op":"inbox","payload":{},"extra":1}`, "request requires exactly op and payload"},
				{"non-object payload", `{"op":"inbox","payload":[]}`, "payload must be an object"},
			} {
				t.Run(fmt.Sprintf("%s/v%d/%s", tool, version, tc.name), func(t *testing.T) {
					_, _, err := DecodeRequestForContract(tool, version, []byte(tc.request))
					var inputError *ToolInputError
					if !errors.As(err, &inputError) || err.Error() != tc.message {
						t.Fatalf("wrong decoder guard: %T %v", err, err)
					}
				})
			}
		}
	}
}

func mustSchemaJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
