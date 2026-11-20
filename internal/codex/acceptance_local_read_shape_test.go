package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func localReadCall(arguments string, encoded bool) Message {
	if encoded {
		data, _ := json.Marshal(arguments)
		arguments = string(data)
	}
	return Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: json.RawMessage(fmt.Sprintf(`{"threadId":"thread","turnId":"turn","callId":"call","tool":"factory_control","arguments":%s}`, arguments))}
}

func TestAcceptanceLocalReadEmptyPayloadForms(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		for _, tc := range []struct{ name, arguments, reason string }{
			{"empty", `{"op":"inbox","payload":{}}`, ""},
			{"whitespace", "{\"op\":\"inbox\",\"payload\":{ \n\t }}", ""},
			{"envelope order", ` { "payload" : { }, "op" : "inbox" } `, ""},
			{"omitted", `{"op":"inbox"}`, "decode_request"},
			{"null", `{"op":"inbox","payload":null}`, "decode_request"},
			{"known empty field", `{"op":"inbox","payload":{"target":""}}`, "payload_not_empty"},
			{"known zero field", `{"op":"inbox","payload":{"ack":0}}`, "payload_not_empty"},
			{"unknown field", `{"op":"inbox","payload":{"secret":"PRIVATE_PAYLOAD"}}`, "decode_request"},
			{"extra envelope field", `{"op":"inbox","payload":{},"secret":"PRIVATE_ENVELOPE"}`, "decode_request"},
			{"array payload", `{"op":"inbox","payload":[]}`, "decode_request"},
			{"string payload", `{"op":"inbox","payload":""}`, "decode_request"},
			{"duplicate payload key", `{"op":"inbox","payload":{"ack":0,"ack":0}}`, "decode_request"},
			{"known nonzero field", `{"op":"inbox","payload":{"ack":1}}`, "payload_not_empty"},
			{"unknown op", `{"op":"PRIVATE_OPERATION","payload":{}}`, "decode_request"},
			{"other op", `{"op":"lease_check","payload":{}}`, "operation"},
		} {
			if tc.name == "duplicate payload key" && !encoded {
				tc.reason = "call_shape"
			}
			t.Run(fmt.Sprintf("encoded=%v/%s", encoded, tc.name), func(t *testing.T) {
				brain, ref, grant := readGrantFixture(t)
				if err := sealAcceptanceReadGrant(t.Context(), brain, ref, "complete"); err != nil {
					t.Fatal(err)
				}
				owner := Owner{ID: "acceptance-model", Generation: grant.Binding, Worktree: grant.Target.Worktree, Selection: grant.Selection}
				_, err := acceptanceControlRead(t.Context(), brain, ref, owner, localReadCall(tc.arguments, encoded), "thread", "turn")
				if tc.reason != "" {
					if !errors.Is(err, ErrObservationUnknown) || !strings.Contains(err.Error(), tc.reason) {
						t.Fatalf("want refusal %s, got %v", tc.reason, err)
					}
					if strings.Contains(err.Error(), "PRIVATE_") || strings.Contains(err.Error(), brain) {
						t.Fatalf("unsafe refusal: %v", err)
					}
					if _, statErr := os.Stat(filepath.Join(grant.Owner.Worktree, "local-control-inbox.jsonl")); !errors.Is(statErr, os.ErrNotExist) {
						t.Fatalf("rejected call wrote inbox: %v", statErr)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(grant.Owner.Worktree, "local-control-read.json"))
				if err != nil {
					t.Fatal(err)
				}
				var proof acceptanceLocalRead
				if err := json.Unmarshal(data, &proof); err != nil {
					t.Fatal(err)
				}
				original, err := controlrecord.ReadOriginal(brain, proof.Original)
				if err != nil || string(original.Payload) != "{}" {
					t.Fatalf("original payload=%s error=%v", original.Payload, err)
				}
			})
		}
	}
}

func TestAcceptanceLocalReadAuthorityRefusals(t *testing.T) {
	for _, reason := range []string{"context", "call_shape", "tool", "thread", "turn", "binding", "grant_reload", "worktree", "selection", "seal"} {
		t.Run(reason, func(t *testing.T) {
			brain, ref, grant := readGrantFixture(t)
			if reason != "seal" {
				if err := sealAcceptanceReadGrant(t.Context(), brain, ref, "complete"); err != nil {
					t.Fatal(err)
				}
			}
			owner := Owner{ID: "acceptance-model", Generation: grant.Binding, Worktree: grant.Target.Worktree, Selection: grant.Selection}
			event := localReadCall(`{"op":"inbox","payload":{}}`, false)
			ctx := t.Context()
			want := ErrObservationUnknown
			switch reason {
			case "context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "call_shape":
				event.Method = "wrong"
			case "tool":
				event.Params = json.RawMessage(strings.Replace(string(event.Params), "factory_control", "PRIVATE_TOOL", 1))
			case "thread":
				event.Params = json.RawMessage(strings.Replace(string(event.Params), `"threadId":"thread"`, `"threadId":"PRIVATE_THREAD"`, 1))
			case "turn":
				event.Params = json.RawMessage(strings.Replace(string(event.Params), `"turnId":"turn"`, `"turnId":"PRIVATE_TURN"`, 1))
			case "binding":
				owner.Generation = "PRIVATE_BINDING"
			case "grant_reload":
				ref.SHA256[0] ^= 1
			case "worktree":
				owner.Worktree = "PRIVATE_PATH"
			case "selection":
				owner.Selection.Profile.IdentityBinding = "PRIVATE_SELECTION"
			case "seal":
				want = ErrAttemptStale
			}
			_, err := acceptanceControlRead(ctx, brain, ref, owner, event, "thread", "turn")
			if !errors.Is(err, want) || !strings.Contains(err.Error(), reason) {
				t.Fatalf("want %s / %v got %v", reason, want, err)
			}
			if strings.Contains(err.Error(), "PRIVATE_") || strings.Contains(err.Error(), brain) {
				t.Fatalf("unsafe refusal: %v", err)
			}
			if _, err := os.Stat(filepath.Join(grant.Owner.Worktree, "local-control-inbox.jsonl")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refusal wrote inbox: %v", err)
			}
		})
	}
}

func TestAcceptanceLocalReadProtocolDiagnostics(t *testing.T) {
	for _, tc := range []struct{ name, arguments, reason string }{
		{"nonempty", `{"op":"inbox","payload":{"target":"PRIVATE_PAYLOAD"}}`, "payload_not_empty"},
		{"unknown field", `{"op":"inbox","payload":{"extra":"PRIVATE_PAYLOAD"}}`, "decode_request"},
		{"unknown operation", `{"op":"PRIVATE_OPERATION","payload":{}}`, "decode_request"},
		{"thread", `{"op":"inbox","payload":{}}`, "thread"},
		{"turn", `{"op":"inbox","payload":{}}`, "turn"},
		{"missing call id", `{"op":"inbox","payload":{}}`, "call_shape"},
		{"missing request id", `{"op":"inbox","payload":{}}`, "call_shape"},
		{"arguments", `"PRIVATE_ARGUMENTS"`, "decode_request"},
		{"binding", `{"op":"inbox","payload":{}}`, "binding"},
		{"selection", `{"op":"inbox","payload":{}}`, "selection"},
		{"reload", `{"op":"inbox","payload":{}}`, "grant_reload"},
		{"inbox io", `{"op":"inbox","payload":{}}`, "inbox"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brain, ref, grant := readGrantFixture(t)
			if err := sealAcceptanceReadGrant(t.Context(), brain, ref, "complete"); err != nil {
				t.Fatal(err)
			}
			owner := Owner{ID: "acceptance-model", Generation: grant.Binding, Worktree: grant.Target.Worktree, Selection: grant.Selection}
			event := localReadCall(tc.arguments, false)
			switch tc.name {
			case "thread":
				event.Params = json.RawMessage(strings.Replace(string(event.Params), `"threadId":"thread"`, `"threadId":"PRIVATE_THREAD"`, 1))
			case "turn":
				event.Params = json.RawMessage(strings.Replace(string(event.Params), `"turnId":"turn"`, `"turnId":"PRIVATE_TURN"`, 1))
			case "missing call id":
				event.Params = json.RawMessage(strings.Replace(string(event.Params), `"callId":"call",`, "", 1))
			case "missing request id":
				event.ID = nil
			case "binding":
				owner.Generation = "PRIVATE_BINDING"
			case "selection":
				owner.Selection.Profile.IdentityBinding = "PRIVATE_SELECTION"
			case "reload":
				ref.SHA256[0] ^= 1
			case "inbox io":
				if err := os.Mkdir(filepath.Join(grant.Owner.Worktree, "local-control-inbox.jsonl"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			p := &Protocol{events: make(chan Message, 1), done: make(chan struct{})}
			p.events <- event
			callback := false
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_, err := acceptanceWaitTurn(ctx, p, "thread", "turn", nil, "", 1, func(ctx context.Context, event Message) (json.RawMessage, error) {
				callback = true
				return acceptanceControlRead(ctx, brain, ref, owner, event, "thread", "turn")
			})
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("wanted %s; got %v", tc.reason, err)
			}
			if strings.Contains(err.Error(), "PRIVATE_") || strings.Contains(err.Error(), brain) {
				t.Fatalf("private error %v", err)
			}
			if (tc.reason == "thread" || tc.reason == "turn" || tc.reason == "call_shape") && callback {
				t.Fatal("wait guard delegated invalid call")
			}
			name := filepath.Join(grant.Owner.Worktree, "protocol-diagnostics.json")
			if err := writeJSON(name, []protocolDiagnostics{p.diagnosticSnapshot()}); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), brain) {
				t.Fatalf("private diagnostics %s", data)
			}
			var traces []struct {
				Refusals []acceptanceReadDiagnostic `json:"local_control_refusals"`
			}
			if err := json.Unmarshal(data, &traces); err != nil {
				t.Fatal(err)
			}
			if len(traces) != 1 || len(traces[0].Refusals) != 1 {
				t.Fatalf("missing refusal in proof %s", data)
			}
			facts := traces[0].Refusals[0]
			if facts.Reason != tc.reason || facts.Tool != "factory_control" {
				t.Fatalf("incorrect facts %s", data)
			}
			if tc.reason == "decode_request" && (facts.DecodeErrorType == "" || facts.DecodeErrorMessage == "" || facts.Arguments == nil) {
				t.Fatalf("decoder details absent from saved proof: %s", data)
			}
			if strings.Contains(tc.arguments, `"payload":{}`) && (!facts.PayloadCanonical || facts.PayloadBytes != 2 || facts.PayloadSHA256 != "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a") {
				t.Fatalf("incorrect payload facts %s", data)
			}
			if tc.reason == "thread" && (facts.ThreadMatched == nil || *facts.ThreadMatched) {
				t.Fatalf("thread mismatch absent %s", data)
			}
			if tc.reason == "turn" && (facts.TurnMatched == nil || *facts.TurnMatched) {
				t.Fatalf("turn mismatch absent %s", data)
			}
			if tc.reason == "binding" && (facts.BindingMatched == nil || *facts.BindingMatched) {
				t.Fatalf("binding mismatch absent %s", data)
			}
			if tc.reason == "grant_reload" && (facts.GrantReloaded == nil || *facts.GrantReloaded) {
				t.Fatalf("reload failure absent %s", data)
			}
			if tc.reason == "selection" && (facts.SelectionMatched == nil || *facts.SelectionMatched) {
				t.Fatalf("selection mismatch absent %s", data)
			}
		})
	}
}

func TestAcceptanceLocalReadDiagnosticBoundAndSnapshot(t *testing.T) {
	p := &Protocol{}
	for i := 0; i < diagnosticEventLimit+3; i++ {
		p.recordAcceptanceReadFailure(acceptanceReadDiagnostic{Reason: fmt.Sprint(i)}.refuse(fmt.Sprint(i), ErrObservationUnknown))
	}
	snapshot := p.diagnosticSnapshot()
	if len(snapshot.LocalControlRefusals) != diagnosticEventLimit || snapshot.LocalControlRefusals[0].Reason != "3" {
		t.Fatalf("unbounded diagnostics %+v", snapshot)
	}
	snapshot.LocalControlRefusals[0].Reason = "changed snapshot"
	if p.diagnosticSnapshot().LocalControlRefusals[0].Reason != "3" {
		t.Fatal("snapshot aliases protocol diagnostics")
	}
}

func TestAcceptanceLocalReadRejectsTrailingArguments(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		t.Run(fmt.Sprint(encoded), func(t *testing.T) {
			brain, ref, grant := readGrantFixture(t)
			owner := Owner{Generation: grant.Binding}
			_, err := acceptanceControlRead(t.Context(), brain, ref, owner, localReadCall(`{"op":"inbox","payload":{}} {}`, encoded), "thread", "turn")
			reason := "call_shape"
			if encoded {
				reason = "decode_request"
			}
			if !errors.Is(err, ErrObservationUnknown) || !strings.Contains(err.Error(), reason) {
				t.Fatalf("want %s, got %v", reason, err)
			}
			if _, err := os.Stat(filepath.Join(grant.Owner.Worktree, "local-control-inbox.jsonl")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected call wrote inbox: %v", err)
			}
		})
	}
}

func TestAcceptanceLocalReadPayloadDiagnosticCanonicalHash(t *testing.T) {
	first, _ := DecodeToolInvocation(localReadCall(`{"op":"inbox","payload":{ "ack":0,"target":"" }}`, false).Params)
	second, _ := DecodeToolInvocation(localReadCall(`{"payload":{"target":"","ack":0},"op":"inbox"}`, false).Params)
	a, b := acceptanceReadFacts(first, "thread", "turn"), acceptanceReadFacts(second, "thread", "turn")
	if !a.PayloadCanonical || !b.PayloadCanonical || a.PayloadSHA256 != b.PayloadSHA256 || a.PayloadBytes == b.PayloadBytes {
		t.Fatalf("payload diagnostics do not canonicalize hash and preserve byte length: %+v %+v", a, b)
	}
}

func TestAcceptanceLocalReadDecodeFailureStructure(t *testing.T) {
	for _, encoded := range []bool{false, true} {
		for _, tc := range []struct {
			name, arguments, kind, message, payloadType, payloadDecoded string
			keys                                                        []acceptanceJSONKey
		}{
			{"null payload", `{"op":"inbox","payload":null}`, "*toolcontract.EncodingError", "invalid bounded JSON object", "null", "", nil},
			{"encoded payload", `{"op":"inbox","payload":"{}"}`, "*toolcontract.ToolInputError", "payload must be an object", "string", "object", nil},
			{"encoded payload keys", `{"op":"inbox","payload":"{\"extra\":\"PRIVATE_VALUE\"}"}`, "*toolcontract.ToolInputError", "payload must be an object", "string", "object", []acceptanceJSONKey{{Name: "extra", Type: "string"}}},
			{"extra key", `{"op":"inbox","payload":{},"target":"PRIVATE_VALUE"}`, "*toolcontract.ToolInputError", "request requires exactly op and payload", "object", "", nil},
			{"nested null", `{"op":"inbox","payload":{"ack":null}}`, "*toolcontract.EncodingError", "invalid bounded JSON object", "object", "", []acceptanceJSONKey{{Name: "ack", Type: "null"}}},
			{"wrong op type", `{"op":123456789,"payload":{"extra":"PRIVATE_VALUE"}}`, "*toolcontract.ToolInputError", "request requires exactly op and payload", "object", "", []acceptanceJSONKey{{Name: "extra", Type: "string"}}},
			{"wrapped tool", `{"factory_control":{"op":"inbox","payload":{}}}`, "*toolcontract.ToolInputError", "request requires exactly op and payload", "", "", nil},
			{"wrapped request", `{"request":{"op":"inbox","payload":{}}}`, "*toolcontract.ToolInputError", "request requires exactly op and payload", "", "", nil},
			{"wrapped input", `{"input":{"op":"inbox","payload":{}}}`, "*toolcontract.ToolInputError", "request requires exactly op and payload", "", "", nil},
		} {
			t.Run(fmt.Sprintf("encoded=%v/%s", encoded, tc.name), func(t *testing.T) {
				brain, ref, grant := readGrantFixture(t)
				_, err := acceptanceControlRead(t.Context(), brain, ref, Owner{Generation: grant.Binding}, localReadCall(tc.arguments, encoded), "thread", "turn")
				var refusal *acceptanceReadRefusal
				if !errors.As(err, &refusal) || !errors.Is(err, ErrObservationUnknown) {
					t.Fatalf("expected bounded refusal, got %v", err)
				}
				d := refusal.Diagnostic
				if d.Reason != "decode_request" || d.DecodeErrorType != tc.kind || d.DecodeErrorMessage != tc.message {
					t.Fatalf("decoder detail: %+v", d)
				}
				wantType, wantDecoded := "object", ""
				if encoded {
					wantType, wantDecoded = "string", "object"
				}
				if d.Arguments == nil || d.Arguments.Type != wantType || d.Arguments.DecodedType != wantDecoded || len(d.Arguments.Keys) == 0 {
					t.Fatalf("argument structure missing: %+v", d.Arguments)
				}
				if tc.payloadType == "" {
					if d.Payload != nil {
						t.Fatalf("invented top-level payload for wrapper: %+v", d.Payload)
					}
				} else if d.Payload == nil || d.Payload.Type != tc.payloadType || d.Payload.DecodedType != tc.payloadDecoded || !reflect.DeepEqual(d.Payload.Keys, tc.keys) {
					t.Fatalf("payload structure: %+v", d.Payload)
				}
				p := &Protocol{}
				p.recordAcceptanceReadFailure(err)
				data, marshalErr := json.Marshal(p.diagnosticSnapshot())
				if marshalErr != nil || strings.Contains(string(data), "PRIVATE_VALUE") || strings.Contains(string(data), "123456789") || strings.Contains(err.Error(), "PRIVATE_VALUE") || strings.Contains(err.Error(), "123456789") {
					t.Fatalf("unsafe error/proof: %v %s %v", err, data, marshalErr)
				}
				if _, err := os.Stat(filepath.Join(grant.Owner.Worktree, "local-control-inbox.jsonl")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("rejected call wrote inbox: %v", err)
				}
			})
		}
	}
}

func TestAcceptanceReadStructureTypesAndBounds(t *testing.T) {
	call, err := DecodeToolInvocation(localReadCall(`{"op":"inbox","payload":{"z":null,"s":"PRIVATE_STRING","n":1e9999,"b":true,"a":["PRIVATE_ARRAY"],"o":{"PRIVATE_NESTED_KEY":"PRIVATE_OBJECT"}}}`, false).Params)
	if err != nil {
		t.Fatal(err)
	}
	d := acceptanceReadFacts(call, "thread", "turn")
	want := []acceptanceJSONKey{{Name: "a", Type: "array"}, {Name: "b", Type: "bool"}, {Name: "n", Type: "number"}, {Name: "o", Type: "object"}, {Name: "s", Type: "string"}, {Name: "z", Type: "null"}}
	if d.Payload == nil || !reflect.DeepEqual(d.Payload.Keys, want) {
		t.Fatalf("wrong shallow structure: %+v", d.Payload)
	}
	data, _ := json.Marshal(d)
	if strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), "1e9999") {
		t.Fatalf("value or nested key leaked: %s", data)
	}
	object := map[string]any{}
	var names []string
	for i := 19; i >= 0; i-- {
		name := fmt.Sprintf("%02d", i) + strings.Repeat("界", 40) + "PRIVATE_SUFFIX"
		names = append(names, name)
		object[name] = "PRIVATE_VALUE"
	}
	sort.Strings(names)
	arguments, _ := json.Marshal(map[string]any{"op": "inbox", "payload": object})
	call, _ = DecodeToolInvocation(localReadCall(string(arguments), true).Params)
	d = acceptanceReadFacts(call, "thread", "turn")
	if d.Payload == nil || len(d.Payload.Keys) != 16 || !d.Payload.KeysTruncated {
		t.Fatalf("unbounded keys: %+v", d.Payload)
	}
	for i, key := range d.Payload.Keys {
		if key.Name != string([]rune(names[i])[:32]) || key.Type != "string" {
			t.Fatalf("unsorted/unbounded key: %+v", key)
		}
	}
	// The same bounds apply at the argument object's top level.
	arguments, _ = json.Marshal(object)
	call, _ = DecodeToolInvocation(localReadCall(string(arguments), false).Params)
	d = acceptanceReadFacts(call, "thread", "turn")
	if d.Arguments == nil || len(d.Arguments.Keys) != 16 || !d.Arguments.KeysTruncated {
		t.Fatalf("unbounded arguments: %+v", d.Arguments)
	}
	for i, key := range d.Arguments.Keys {
		if key.Name != string([]rune(names[i])[:32]) {
			t.Fatalf("unsorted/unbounded argument key: %+v", key)
		}
	}
}

func TestAcceptanceReadArgumentJSONTypes(t *testing.T) {
	for _, tc := range []struct{ raw, kind, decoded string }{
		{`{}`, "object", ""}, {`"{}"`, "string", "object"}, {`"[]"`, "string", "array"},
		{`"null"`, "string", "null"}, {`"true"`, "string", "bool"}, {`"1e9999"`, "string", "number"},
		{`"\"PRIVATE_VALUE\""`, "string", "string"}, {`"PRIVATE_INVALID_JSON"`, "string", "other"},
		{`[]`, "other", ""}, {`123456789`, "other", ""}, {`false`, "other", ""}, {`null`, "other", ""},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			call, err := DecodeToolInvocation(localReadCall(tc.raw, false).Params)
			if err != nil {
				t.Fatal(err)
			}
			d := acceptanceReadFacts(call, "thread", "turn")
			if d.Arguments == nil || d.Arguments.Type != tc.kind || d.Arguments.DecodedType != tc.decoded {
				t.Fatalf("wrong JSON type: %+v", d.Arguments)
			}
			data, _ := json.Marshal(d)
			if strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), "123456789") {
				t.Fatalf("value leaked: %s", data)
			}
		})
	}
}

func TestAcceptanceReadStructureSnapshotIsolation(t *testing.T) {
	call, _ := DecodeToolInvocation(localReadCall(`{"op":"inbox","payload":{"ack":null}}`, false).Params)
	p := &Protocol{}
	p.recordAcceptanceReadFailure(acceptanceReadFacts(call, "thread", "turn").refuse("decode_request", ErrObservationUnknown))
	snapshot := p.diagnosticSnapshot()
	snapshot.LocalControlRefusals[0].Arguments.Keys[0].Name = "PRIVATE_MUTATION"
	snapshot.LocalControlRefusals[0].Payload.Keys[0].Name = "PRIVATE_MUTATION"
	snapshot.LocalControlRefusals[0].Payload.Type = "PRIVATE_MUTATION"
	data, _ := json.Marshal(p.diagnosticSnapshot())
	if strings.Contains(string(data), "PRIVATE_MUTATION") {
		t.Fatalf("snapshot aliases recorded structures: %s", data)
	}
}

func TestAcceptanceReadDecodeErrorDoesNotPrintParserContents(t *testing.T) {
	d := acceptanceReadDiagnostic{}
	d.decodeFailure(&toolcontract.EncodingError{Cause: errors.New("parser saw PRIVATE_VALUE at PRIVATE_PATH")})
	if d.DecodeErrorType != "*toolcontract.EncodingError" || d.DecodeErrorMessage != "invalid request encoding" {
		t.Fatalf("unsafe parser error: %+v", d)
	}
}
