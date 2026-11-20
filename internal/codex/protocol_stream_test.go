package codex

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func protocolFrames(messages []Message) string {
	var frames strings.Builder
	for _, message := range messages {
		data, _ := json.Marshal(message)
		frames.Write(data)
		frames.WriteByte('\n')
	}
	return frames.String()
}
func TestProtocolTextStreamCannotDisplaceManagedControl(t *testing.T) {
	var messages []Message
	for i := 0; i < 512; i++ {
		for _, method := range []string{"item/agentMessage/delta", "item/reasoning/summaryTextDelta", "item/reasoning/summaryPartAdded", "item/reasoning/textDelta"} {
			messages = append(messages, Message{Method: method, Params: json.RawMessage(`{"delta":"private text"}`)})
		}
	}
	controls := []Message{
		{ID: json.RawMessage(`7`), Method: "item/tool/call", Params: json.RawMessage(`{"tool":"managed_files"}`)},
		{Method: "item/completed", Params: json.RawMessage(`{"item":{"type":"agentMessage","text":"complete report"}}`)},
		{Method: "thread/tokenUsage/updated", Params: json.RawMessage(`{"tokenUsage":{"last":{"totalTokens":321}}}`)},
		{Method: "item/completed", Params: json.RawMessage(`{"item":{"type":"contextCompaction"}}`)},
		{Method: "model/rerouted", Params: json.RawMessage(`{"threadId":"thread"}`)},
		{Method: "turn/completed", Params: json.RawMessage(`{"turn":{"status":"completed"}}`)},
	}
	messages = append(messages, controls...)
	p := newProtocol(&startupReplyBuffer{}, strings.NewReader(protocolFrames(messages)))
	waitProtocolDone(t, p)
	if !errors.Is(p.failure(), io.EOF) {
		t.Fatalf("unused deltas killed owned protocol before control: %v", p.failure())
	}
	if len(p.events) != len(controls) {
		t.Fatalf("control queue contains %d events, want %d", len(p.events), len(controls))
	}
	for _, want := range controls {
		got := <-p.Events()
		if got.Method != want.Method || string(got.ID) != string(want.ID) || string(got.Params) != string(want.Params) {
			t.Fatalf("managed control changed: %+v", got)
		}
	}
	if p.diagnosticSnapshot().EventsSeen != uint64(len(messages)) {
		t.Fatal("stream diagnostics lost count")
	}
}
func TestProtocolRequestsAndLifecycleStillHaveHardBound(t *testing.T) {
	for _, method := range []string{"item/tool/call", "item/agentMessage/delta", "turn/completed"} {
		t.Run(method, func(t *testing.T) {
			var messages []Message
			for i := 0; i < 129; i++ {
				m := Message{Method: method, Params: json.RawMessage(`{}`)}
				if method != "turn/completed" {
					m.ID = json.RawMessage(`1`)
				}
				messages = append(messages, m)
			}
			p := newProtocol(&startupReplyBuffer{}, strings.NewReader(protocolFrames(messages)))
			waitProtocolDone(t, p)
			if p.failure() == nil || errors.Is(p.failure(), io.EOF) {
				t.Fatal("critical traffic was silently discarded")
			}
			if p.diagnosticSnapshot().Failure != "event_bound_exceeded" {
				t.Fatal("original protocol failure category lost")
			}
		})
	}
}

func TestProtocolDeltaMethodWithAnyRequestIdentityIsRetained(t *testing.T) {
	for _, id := range []string{"null", "0", `""`} {
		t.Run(id, func(t *testing.T) {
			message := Message{ID: json.RawMessage(id), Method: "item/agentMessage/delta", Params: json.RawMessage(`{}`)}
			p := newProtocol(&startupReplyBuffer{}, strings.NewReader(protocolFrames([]Message{message})))
			waitProtocolDone(t, p)
			select {
			case got := <-p.Events():
				if string(got.ID) != id {
					t.Fatalf("request identity changed: %+v", got)
				}
			default:
				t.Fatal("identified request silently discarded")
			}
		})
	}
}
