package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlTransportV2UsesOnlyTrustedCatalogue(t *testing.T) {
	for _, helper := range []bool{false, true} {
		tools := dynamicToolsForContract(helper, 2)
		for _, tool := range tools {
			entry := tool.(map[string]any)
			name := entry["name"].(string)
			if !reflect.DeepEqual(entry["inputSchema"], toolcontract.SchemaForContract(name, 2)) || entry["description"] != toolcontract.HelpForContract(name, 2) {
				t.Fatalf("%s advertised an independent contract", name)
			}
		}
	}
	for _, op := range []string{"pr_create", "pr_ready"} {
		if strings.Contains(toolcontract.HelpForContract(toolcontract.FactoryControl, 2), "op=\""+op+"\"") {
			t.Fatalf("advertised legacy write %s", op)
		}
	}
	for _, example := range toolcontract.ExamplesForContract(toolcontract.FactoryHelper, 2) {
		if _, err := toolcontract.DecodeInputForContract(toolcontract.FactoryHelper, example.Operation, 2, example.Payload); err != nil {
			t.Fatalf("catalogue example does not decode: %s %v", example.Operation, err)
		}
	}
}
func TestControlTransportV2PreservesInvocationAndUnwrapsArgumentsOnce(t *testing.T) {
	inner := ` {"payload":{"text":"<>&","since":"2026-11-12T00:00:00Z"},"op":"event"} `
	outer, _ := json.Marshal(ToolInvocation{Arguments: mustTransportJSON(t, inner), CallID: "actual-call", ThreadID: "actual-thread", TurnID: "actual-turn", Tool: toolcontract.FactoryControl})
	invocation, err := DecodeToolInvocation(outer)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ToolArguments(invocation)
	if err != nil || string(got) != inner {
		t.Fatalf("original argument bytes changed: %q %v", got, err)
	}
	duplicate := []byte(`{"arguments":{},"callId":"a","callId":"b","threadId":"t","turnId":"r","tool":"factory_control"}`)
	if _, err := DecodeToolInvocation(duplicate); err == nil {
		t.Fatal("ambiguous outer binding accepted")
	}
}
func mustTransportJSON(t *testing.T, text string) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestAttemptControlTransportV2ForwardsExactBindingWithoutCredentials(t *testing.T) {
	t.Setenv("GH_TOKEN", "transport-fixture-token")
	t.Setenv("GITHUB_TOKEN", "transport-fixture-token2")
	t.Setenv("NPM_TOKEN", "transport-fixture-token3")
	dir := t.TempDir()
	binary := filepath.Join(dir, "factory-transport")
	arguments := filepath.Join(dir, "arguments")
	script := "#!/bin/sh\nif [ -n \"${GH_TOKEN+x}${GITHUB_TOKEN+x}${NPM_TOKEN+x}\" ]; then exit 12; fi\nprintf '%s\\n' \"$@\" > '" + arguments + "'\ncat >/dev/null\nprintf '%s' '{\"ok\":true}'\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{Brain: dir, FactoryBin: binary}
	record := Record{Schema: 2, CodexContract: 2, ID: "actual-session", AttemptID: "actual-attempt"}
	params := json.RawMessage(`{"arguments":{"op":"inbox","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
	reply, err := runtime.attemptControlTool(context.Background(), record, params)
	if err != nil || string(reply) != `{"ok":true}` {
		t.Fatalf("exact transport failed: %s %v", reply, err)
	}
	data, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "codex-control\n--brain\n"+dir+"\n--session\nactual-session\n--attempt\nactual-attempt\n" {
		t.Fatalf("actual attempt binding changed: %s", data)
	}
}
func TestAttemptControlTransportV2KeepsSubprocessAndEncodingFailuresHard(t *testing.T) {
	for _, kind := range []string{"nonzero", "invalid", "duplicate", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			binary := filepath.Join(dir, "factory-transport")
			script := "#!/bin/sh\ncat >/dev/null\n"
			switch kind {
			case "nonzero":
				script += "printf '%s' '{\"tool_error\":\"do not reclassify stderr\"}' >&2\nexit 3\n"
			case "invalid":
				script += "printf '%s' '{'\n"
			case "duplicate":
				script += "printf '%s' '{\"x\":1,\"x\":2}'\n"
			case "oversized":
				script += "/usr/bin/head -c 1048577 /dev/zero\n"
			}
			if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			runtime := &Runtime{Brain: dir, FactoryBin: binary}
			record := Record{Schema: 2, CodexContract: 2, ID: "actual-session", AttemptID: "actual-attempt"}
			params := json.RawMessage(`{"arguments":{"op":"inbox","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
			if _, err := runtime.attemptControlTool(context.Background(), record, params); err == nil {
				t.Fatalf("%s subprocess became success", kind)
			}
		})
	}
}
