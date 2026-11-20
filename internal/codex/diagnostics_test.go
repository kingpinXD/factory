package codex

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProtocolDiagnosticsAreBoundedAndExcludePayloads(t *testing.T) {
	p := &Protocol{}
	for i := 0; i < diagnosticEventLimit+20; i++ {
		p.recordDiagnostic(Message{Method: "item/completed", Params: json.RawMessage(`{"item":{"type":"agentMessage","text":"private-reply"},"arguments":{"token":"private-token"},"turn":{"status":"failed","error":{"message":"private-provider-error"}}}`)})
	}
	p.recordDiagnostic(Message{Method: "private-unknown-method", Error: &RPCError{Code: -32000, Message: "private-account-error"}})
	snapshot := p.diagnosticSnapshot()
	if snapshot.EventsSeen != diagnosticEventLimit+21 || len(snapshot.Events) != diagnosticEventLimit {
		t.Fatalf("diagnostic bound lost: %+v", snapshot)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-") || !strings.Contains(string(data), `"error_code":-32000`) || !strings.Contains(string(data), `"status":"failed"`) {
		t.Fatalf("diagnostics lost safe shape or retained payload: %s", data)
	}
}
