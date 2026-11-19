package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlV2LiveCorrectableInput(t *testing.T) {
	for _, tc := range []struct {
		name, op, want string
		planner        bool
		payload        any
	}{
		{"planner_target", "event", "target", true, map[string]any{"target": "codex-minmax-set", "kind": "instruction", "text": "continue"}},
		{"orchestrator_target", "event", "target", false, map[string]any{"target": "codex-minmax-set", "kind": "blocked_by", "text": "waiting"}},
		{"file_read_target", "file_read", "target", false, map[string]any{"target": "codex-minmax-set", "path": "impl.v1.md"}},
		{"events_target", "events", "target", false, map[string]any{"target": "codex-minmax-set"}},
		{"inbox_target", "inbox", "target", false, map[string]any{"target": "codex-minmax-set"}},
		{"bad_event", "event", "one line", false, map[string]any{"kind": "blocked_by", "text": "first\nsecond"}},
		{"absent_epic_file", "epic_file", "does not exist", false, map[string]any{"path": "DESIGN.md"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brain, record, _ := localReadAttempt(t, tc.planner)
			dispatchActivate(t, brain, record)
			ix, err := store.ReadIndex(brain)
			if err != nil {
				t.Fatal(err)
			}
			before, err := events.Read(store.EventsPath(ix[record.Spec.Task].Dir))
			if err != nil {
				t.Fatal(err)
			}
			data, id := writeInvocation(t, record, tc.name, tc.op, tc.payload)
			value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
			if err != nil {
				t.Fatalf("model input became a dynamic tool failure: %v", err)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var reply toolcontract.ToolFailure
			if json.Unmarshal(encoded, &reply) != nil || !strings.Contains(reply.ToolError, tc.want) {
				t.Fatalf("missing corrective reply: %s", encoded)
			}
			after, err := events.Read(store.EventsPath(ix[record.Spec.Task].Dir))
			if err != nil || len(after) != len(before) {
				t.Fatalf("invalid input appended an event: before=%d after=%d err=%v", len(before), len(after), err)
			}
			original, _, err := controlrecord.FindControlOriginal(brain, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := controlrecord.FindControlEventSource(brain, original); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid event retained a source: %v", err)
			}
			failure := filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID, "control-failures", fmt.Sprintf("%x.json", sha256.Sum256(data)))
			if _, err := os.Stat(failure); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corrected input retained a transport failure: %v", err)
			}
		})
	}
}

func TestControlV2LiveEventReplyAndReplay(t *testing.T) {
	for _, planner := range []bool{true, false} {
		kind := "blocked_by"
		if planner {
			kind = "error"
		}
		t.Run(kind, func(t *testing.T) {
			var brain string
			var record codex.Record
			target := "work1"
			if planner {
				brain, record, _ = localReadAttempt(t, true)
				dispatchActivate(t, brain, record)
				target = record.Spec.Task
			} else {
				brain, record, _, _ = liveExplorerAdmissionAttempt(t)
			}
			data, id := writeInvocation(t, record, "event-reply", "event", map[string]any{"target": target, "kind": kind, "text": "helper admission failed"})
			attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
			value, err := coordinatorControlV2(context.Background(), brain, attempt, data)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var event events.Event
			if json.Unmarshal(reply, &event) != nil || event.Seq < 1 || event.Kind != kind || event.Text != "helper admission failed" {
				t.Fatalf("missing appended event: %s", reply)
			}
			original, _, err := controlrecord.FindControlOriginal(brain, id)
			if err != nil {
				t.Fatal(err)
			}
			_, outcome, err := controlrecord.FindOutcome(brain, original)
			if err != nil || string(outcome.Reply) != string(reply) {
				t.Fatalf("reply differs from durable outcome: %s %v", outcome.Reply, err)
			}
			if !planner && outcome.Input.Target.WorkID != "work1" {
				t.Fatalf("blocked event was not bound to its work: %+v", outcome.Input.Target)
			}
			value, err = coordinatorControlV2(context.Background(), brain, attempt, data)
			if err != nil {
				t.Fatal(err)
			}
			replayed, _ := json.Marshal(value)
			if string(replayed) != string(reply) {
				t.Fatalf("replay changed reply: %s != %s", replayed, reply)
			}
			ix, err := store.ReadIndex(brain)
			if err != nil {
				t.Fatal(err)
			}
			log, err := events.Read(store.EventsPath(ix[target].Dir))
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, ev := range log {
				if ev.Key == event.Key {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("same call appended %d events", count)
			}
		})
	}
}

func TestControlV2LiveMissingEvidenceKeepsFailureStage(t *testing.T) {
	brain, record, _ := localReadAttempt(t, false)
	dispatchActivate(t, brain, record)
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.InputsPath(ix["work1"].Dir)); err != nil {
		t.Fatal(err)
	}
	data, _ := writeInvocation(t, record, "missing-target-evidence", "event", map[string]any{"target": "work1", "kind": "blocked_by", "text": "waiting"})
	value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
	if value != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing authoritative target evidence became corrective reply: %v %v", value, err)
	}
	name := filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID, "control-failures", fmt.Sprintf("%x.json", sha256.Sum256(data)))
	failure, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic struct{ Stage, Reason string }
	if err := json.Unmarshal(failure, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if diagnostic.Stage != "event_source_capture" || diagnostic.Reason != "required control evidence is missing" {
		t.Fatalf("failed event source step was hidden: %s", failure)
	}
	if strings.Contains(string(failure), brain) || strings.Contains(string(failure), "waiting") || strings.Contains(string(failure), "payload") {
		t.Fatalf("failure exposed path or payload: %s", failure)
	}
}

func TestControlV2LiveRelativeHelperOutput(t *testing.T) {
	brain, record, service, request := liveExplorerAdmissionAttempt(t)
	request.OutputPath = "explore.v1.md"
	data, id := writeInvocation(t, record, "relative-explorer", "helper_submit", request)
	value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
	if err != nil {
		t.Fatalf("relative output became dynamic tool failure: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var reply toolcontract.ToolFailure
	if json.Unmarshal(encoded, &reply) != nil || !strings.Contains(reply.ToolError, "absolute output_path") {
		t.Fatalf("relative output has no actionable correction: %s", encoded)
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := controlrecord.FindEnrollmentSnapshot(brain, original); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrected request left an enrolled control: %v", err)
	}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatalf("relative output admitted a job: %+v %v", jobs, err)
	}
}
