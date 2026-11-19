package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAttemptControlFailureRetainsSafeHostReason(t *testing.T) {
	for _, kind := range []string{"process", "reply"} {
		t.Run(kind, func(t *testing.T) {
			r, spec, _, _ := attemptWorld(t)
			ref := preparedAttempt(t, r, spec)
			record := claimedAttempt(t, r, ref)
			script := "#!/bin/sh\ncat >/dev/null\nprintf '%s' 'private-token private-prompt' >&2\n"
			if kind == "process" {
				script += "exit 7\n"
			} else {
				script += "printf '{'\n"
			}
			if err := os.WriteFile(r.FactoryBin, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			params := json.RawMessage(`{"arguments":{"op":"inbox","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
			if _, err := r.attemptControlTool(context.Background(), record, params); err == nil {
				t.Fatal("broken control transport succeeded")
			}
			directory, err := r.attemptDirectory(ref)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(directory, "control-failures", hash(params)+".json")
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatalf("failed dynamic request has no host reason: %v", err)
			}
			var failure struct {
				Operation, Stage, Reason string
				RequestSHA256            string `json:"request_sha256"`
			}
			if err := json.Unmarshal(data, &failure); err != nil {
				t.Fatal(err)
			}
			stage, reason := "control_process", "the scoped control process failed"
			if kind == "reply" {
				stage, reason = "control_reply", "the scoped control reply is invalid"
			}
			if failure.Operation != "inbox" || failure.Stage != stage || failure.Reason != reason || failure.RequestSHA256 != hash(params) {
				t.Fatalf("host reason is not actionable: %s", data)
			}
			if strings.Contains(string(data), "private-") || strings.Contains(string(data), "payload") || strings.Contains(string(data), string(params)) {
				t.Fatalf("host reason disclosed request or stderr: %s", data)
			}
			info, err := os.Stat(name)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("diagnostic permissions: %v %v", info, err)
			}
		})
	}
}

func TestControlFailurePreservesDetailedChildReasonAndRejectsAliases(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	record := claimedAttempt(t, r, ref)
	request := json.RawMessage(`{"arguments":{"op":"inbox","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
	if err := RecordControlFailure(r.Brain, ref, request, "reply_snapshot", os.ErrNotExist); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.FactoryBin, []byte("#!/bin/sh\ncat >/dev/null\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.attemptControlTool(context.Background(), record, request); err == nil {
		t.Fatal("failed child returned success")
	}
	directory, err := r.attemptDirectory(ref)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(directory, "control-failures", hash(request)+".json")
	var failure controlFailure
	if err := readJSON(name, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Stage != "reply_snapshot" || failure.Reason != "required control evidence is missing" {
		t.Fatalf("parent overwrote the useful child reason: %+v", failure)
	}
	outside := filepath.Join(r.Brain, "outside-diagnostic")
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Dir(name), filepath.Dir(name)+"-saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Dir(name)); err != nil {
		t.Fatal(err)
	}
	if err := RecordControlFailure(r.Brain, ref, request, "reply_snapshot", os.ErrNotExist); err == nil {
		t.Fatal("diagnostic followed an aliased directory")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("diagnostic escaped private attempt: %v %v", entries, err)
	}
}

func TestDynamicControlAuthorityFailureIsVisibleWithoutChangingAttempt(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	record := claimedAttempt(t, r, ref)
	record.State, record.TurnID = "active", "turn"
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	before, err := r.readAttempt(ref)
	if err != nil {
		t.Fatal(err)
	}
	params := json.RawMessage(`{"arguments":{"op":"inbox","payload":{}},"callId":"call","threadId":"wrong-thread","turnId":"turn","tool":"factory_control"}`)
	if err := r.handleEvent(context.Background(), nil, Owner{}, &record, Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: params}); err == nil {
		t.Fatal("foreign thread was accepted")
	}
	directory, _ := r.attemptDirectory(ref)
	var failure controlFailure
	if err := readJSON(filepath.Join(directory, "control-failures", hash(params)+".json"), &failure); err != nil {
		t.Fatal("dynamic failure left no reason", err)
	}
	if failure.Stage != "tool_dispatch" || failure.Reason != "the Codex session observation is unknown" {
		t.Fatalf("wrong ingress reason: %+v", failure)
	}
	after, err := r.readAttempt(ref)
	if err != nil || after.Revision != before.Revision || after.State != before.State || after.Stage != before.Stage {
		t.Fatalf("diagnostic changed lifecycle: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestDynamicControlUnscopedToolFailureIsVisible(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	record := claimedAttempt(t, r, ref)
	record.State, record.TurnID = "active", "turn"
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	params := json.RawMessage(`{"arguments":{"op":"inbox","payload":{}},"callId":"call","threadId":"thread","turnId":"turn","tool":"unscoped"}`)
	output := &startupReplyBuffer{}
	protocol := &Protocol{input: output, writeGate: make(chan struct{}, 1), done: make(chan struct{})}
	if err := r.handleEvent(context.Background(), protocol, Owner{}, &record, Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: params}); err != nil {
		t.Fatal(err)
	}
	var reply Message
	if err := json.Unmarshal(output.Bytes(), &reply); err != nil || reply.Error == nil {
		t.Fatalf("unscoped tool was not rejected: %s %v", output.Bytes(), err)
	}
	directory, _ := r.attemptDirectory(ref)
	var failure controlFailure
	if err := readJSON(filepath.Join(directory, "control-failures", hash(params)+".json"), &failure); err != nil {
		t.Fatal("unscoped failure left no reason", err)
	}
	if failure.Stage != "tool_scope" || failure.Reason != "the model requested an unscoped tool" {
		t.Fatalf("wrong unscoped-tool reason: %+v", failure)
	}
}

func TestControlEnrollmentFailureClassifiesCauseWithoutSecrets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  string
	}{
		{"sentinel", fmt.Errorf("private-token: %w", effects.ErrCorrupt), "effects.ErrCorrupt"},
		{"wrapped_type", fmt.Errorf("private-prompt: %w", &store.LockedError{Name: "private-token", PID: 123}), "*store.LockedError"},
		{"opaque", errors.New("private-token private-prompt"), "*errors.errorString"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, spec, _, _ := attemptWorld(t)
			ref := preparedAttempt(t, r, spec)
			request := []byte(`{"arguments":{"op":"issue_create","payload":{"repo":"o/r","key":"epic","title":"private-prompt","body":"private-token"}},"callId":"call","threadId":"thread","turnId":"turn","tool":"factory_control"}`)
			if err := RecordControlFailure(r.Brain, ref, request, "control_enrollment", tc.cause); err != nil {
				t.Fatal(err)
			}
			directory, err := r.attemptDirectory(ref)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(directory, "control-failures", hash(request)+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var failure struct{ Stage, Reason, Cause string }
			if err := json.Unmarshal(data, &failure); err != nil {
				t.Fatal(err)
			}
			if failure.Stage != "control_enrollment" || failure.Reason != "the scoped control operation failed" || failure.Cause != tc.want {
				t.Fatalf("failure has no safe cause: %s", data)
			}
			if strings.Contains(string(data), "private-") || strings.Contains(string(data), "payload") {
				t.Fatalf("failure leaked secrets: %s", data)
			}
			t.Logf("retained %s without error text or request data", failure.Cause)
		})
	}
}
