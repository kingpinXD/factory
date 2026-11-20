package codex

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestDynamicToolObjectEnvelopes(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, helper := range []bool{false, true} {
			tools := dynamicToolsForContract(helper, version)
			wantNames := []string{toolcontract.FactoryControl}
			if helper {
				wantNames = []string{toolcontract.FactoryHelper, toolcontract.ManagedFiles, toolcontract.ManagedTerminal}
			}
			var names []string
			for _, tool := range tools {
				entry := tool.(map[string]any)
				name := entry["name"].(string)
				names = append(names, name)
				t.Run(fmt.Sprintf("%s/v%d", name, version), func(t *testing.T) {
					if entry["type"] != "function" {
						t.Fatal("tool is not advertised as a function")
					}
					schema := entry["inputSchema"].(map[string]any)
					assertToolObjectEnvelope(t, schema)
					props := schema["properties"].(map[string]any)
					if !reflect.DeepEqual(props["op"], map[string]any{"type": "string", "enum": toolcontract.OperationsForContract(name, version)}) {
						t.Fatalf("op must enumerate the advertised contract: %#v", props["op"])
					}
					if version == 2 && (!reflect.DeepEqual(schema, toolcontract.SchemaForContract(name, version)) || entry["description"] != toolcontract.HelpForContract(name, version)) {
						t.Fatal("production tool diverged from catalogue schema/help")
					}
				})
			}
			if !reflect.DeepEqual(names, wantNames) {
				t.Fatalf("contract %d helper=%v tools=%v; want %v", version, helper, names, wantNames)
			}
		}
	}
}

func TestAcceptanceDynamicToolSchemas(t *testing.T) {
	for _, localRead := range []bool{false, true} {
		tools := acceptanceDynamicTools(localRead)
		wantLen := 1
		if localRead {
			wantLen = 2
		}
		if len(tools) != wantLen {
			t.Fatalf("localRead=%v advertised %d tools", localRead, len(tools))
		}
		probe := tools[0].(map[string]any)
		wantProbe := map[string]any{
			"type": "object", "additionalProperties": false, "required": []string{"op"},
			"properties": map[string]any{"op": map[string]any{"type": "string", "enum": []string{"write", "read", "writer"}}},
		}
		if probe["type"] != "function" || probe["name"] != "factory_acceptance" || !reflect.DeepEqual(probe["inputSchema"], wantProbe) {
			t.Fatal("acceptance probe must retain its separate op-only object contract")
		}
		if localRead {
			control := tools[1].(map[string]any)
			schema := control["inputSchema"].(map[string]any)
			assertToolObjectEnvelope(t, schema)
			if control["type"] != "function" || control["name"] != toolcontract.FactoryControl || !reflect.DeepEqual(schema, toolcontract.SchemaForContract(toolcontract.FactoryControl, 2)) {
				t.Fatal("qualification tool must use the production control catalogue")
			}
			if !strings.Contains(control["description"].(string), `{"op":"inbox","payload":{}}`) {
				t.Fatal("qualification description must name the exact required JSON")
			}
		}
	}
}

func assertToolObjectEnvelope(t *testing.T, schema map[string]any) {
	t.Helper()
	if schema["type"] != "object" || schema["additionalProperties"] != false || !reflect.DeepEqual(schema["required"], []string{"op", "payload"}) {
		t.Fatalf("tool requires a closed top-level object with op and payload: %#v", schema)
	}
	props := schema["properties"].(map[string]any)
	if len(props) != 2 || props["op"].(map[string]any)["type"] != "string" || props["payload"].(map[string]any)["type"] != "object" {
		t.Fatalf("invalid advertised root properties: %#v", props)
	}
}
