package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAttemptHostErrorOutputIsPrivateBoundedAndRotated(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	for i := 0; i < 2000; i++ {
		if err := RecordAttemptHostFailure(r.Brain, ref, errors.New("private-token private-prompt")); err != nil {
			t.Fatal(err)
		}
	}
	dir, _ := r.attemptDirectory(ref)
	for _, name := range []string{"host-errors.log", "host-errors.log.1"} {
		filename := filepath.Join(dir, name)
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Lstat(filename)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 || len(data) > 64*1024 || len(data) == 0 {
			t.Fatalf("invalid log bounds: %s %d %v", name, len(data), info.Mode())
		}
		if bytes.Contains(data, []byte("private-")) || strings.Contains(string(data), r.Brain) {
			t.Fatalf("unsafe host output: %s", data)
		}
		if !bytes.Contains(data, []byte("host_failure")) {
			t.Fatalf("missing safe reason: %s", data)
		}
	}
	filename := filepath.Join(dir, "host-errors.log")
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(r.Brain, "outside-log")
	if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filename); err != nil {
		t.Fatal(err)
	}
	if err := RecordAttemptHostFailure(r.Brain, ref, errors.New("private-token")); err == nil {
		t.Fatal("followed aliased log")
	}
	data, _ := os.ReadFile(outside)
	if string(data) != "untouched" {
		t.Fatal("changed external file")
	}
}

func TestAttemptControlFailureRetainsSafeErrorOutput(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	record := claimedAttempt(t, r, ref)
	if err := os.WriteFile(r.FactoryBin, []byte("#!/bin/sh\ncat >/dev/null\nprintf 'private-token private-prompt' >&2\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	params := json.RawMessage(`{"arguments":{"op":"plan_input","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
	if _, err := r.attemptControlTool(context.Background(), record, params); err == nil {
		t.Fatal("failed child succeeded")
	}
	dir, _ := r.attemptDirectory(ref)
	data, err := os.ReadFile(filepath.Join(dir, "host-errors.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("control_process")) || bytes.Contains(data, []byte("private-")) || bytes.Contains(data, []byte("payload")) {
		t.Fatalf("unsafe host output: %s", data)
	}
}

func TestDynamicControlRefusalPreservesReasonCodeAsToolResult(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	record := claimedAttempt(t, r, ref)
	record.State, record.TurnID = "active", "turn"
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.FactoryBin, []byte("#!/bin/sh\ncat >/dev/null\nprintf '%s' '{\"tool_error\":\"current lease required\",\"code\":\"control_authority_invalid\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	output := &startupReplyBuffer{}
	protocol := &Protocol{input: output, writeGate: make(chan struct{}, 1), done: make(chan struct{})}
	params := json.RawMessage(`{"arguments":{"op":"plan_input","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
	if err := r.handleEvent(context.Background(), protocol, Owner{}, &record, Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: params}); err != nil {
		t.Fatal(err)
	}
	var reply Message
	if err := json.Unmarshal(output.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Error != nil || !bytes.Contains(reply.Result, []byte("control_authority_invalid")) || !bytes.Contains(reply.Result, []byte(`"success":false`)) {
		t.Fatalf("refusal lost code or became RPC error: %s", output.Bytes())
	}
}

func TestAttemptControlTransportFailureBindsDurableOriginal(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	record := claimedAttempt(t, r, ref)
	params := json.RawMessage(`{"arguments":{"op":"plan_input","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: ref.SessionID, AttemptID: ref.AttemptID}, ThreadID: "thread", TurnID: "turn", CallID: "call"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"plan_input","payload":{}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(r.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.FactoryBin, []byte("#!/bin/sh\ncat >/dev/null\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.attemptControlTool(context.Background(), record, params); err == nil {
		t.Fatal("failed child succeeded")
	}
	filename := filepath.Join(r.Brain, "factory/control-records/failure", filepath.Base(originalRef.Path))
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("control_process")) || !bytes.Contains(data, []byte("control_failed")) {
		t.Fatalf("missing linked transport reason: %s", data)
	}
}
