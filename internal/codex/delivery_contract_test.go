package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
)

func TestDurableDeliveryIdentityDistinguishesRepeatedText(t *testing.T) {
	r := New(t.TempDir(), "unused")
	record := fixtureRecord(t, r)
	first := hostRequest{Operation: "deliver", Key: "nudge-after-event-1", Text: "continue your work"}
	if err := r.queueCommand(&record, first); err != nil {
		t.Fatal(err)
	}
	if err := r.queueCommand(&record, first); err != nil {
		t.Fatal(err)
	}
	if len(record.Commands) != 1 {
		t.Fatal("retry duplicated queued delivery")
	}
	record.Commands = nil
	record.Deliveries[first.Key] = "complete"
	if err := r.queueCommand(&record, first); err != nil {
		t.Fatal(err)
	}
	if len(record.Commands) != 0 {
		t.Fatal("completed retry replayed")
	}
	second := first
	second.Key = "nudge-after-event-2"
	if err := r.queueCommand(&record, second); err != nil {
		t.Fatal(err)
	}
	if len(record.Commands) != 1 || record.Commands[0].Text != first.Text {
		t.Fatal("later identical nudge was lost")
	}
}

func TestExistingPRInstructionsRequireAuthoritativeModeBinding(t *testing.T) {
	original := "- **Commits:** commit after tests. Never push.\n"
	job := helper.Job{Request: helper.Request{Mode: helper.ModeExistingPRRevision, Component: "implementor", OwnerID: "w", WorkID: "w", RevisionPR: 7, RevisionRepo: "o/r", RevisionBranch: "factory/w", RevisionBase: "main"}, Route: executor.Route{Provider: "codex"}, Scope: helper.Scope{OwnerID: "w", WorkID: "w"}}
	text, err := helperInstructions(job, original)
	if err != nil || !strings.Contains(text, "Never push or write to GitHub") || !strings.Contains(text, `"head_branch":"factory/w"`) {
		t.Fatalf("overlay=%q err=%v", text, err)
	}
	job.Request.RevisionBranch = ""
	if _, err = helperInstructions(job, original); err == nil || !strings.Contains(err.Error(), "authoritative") {
		t.Fatalf("missing binding accepted: %v", err)
	}
	job.Request.Mode = ""
	text, err = helperInstructions(job, original)
	if err != nil || text != original {
		t.Fatal("ordinary helper push policy changed")
	}
}

func TestAcceptanceCompactionRequiresTerminalAfterSavedItem(t *testing.T) {
	for _, finish := range []string{"turn", "idle"} {
		t.Run(finish, func(t *testing.T) {
			p := &Protocol{events: make(chan Message, 4), done: make(chan struct{})}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			p.events <- Message{Method: "turn/started", Params: json.RawMessage(`{"threadId":"thread","turn":{"id":"compact"}}`)}
			p.events <- Message{Method: "item/completed", Params: json.RawMessage(`{"threadId":"thread","turnId":"compact","item":{"id":"summary","type":"contextCompaction"}}`)}
			result := make(chan error, 1)
			go func() { _, err := acceptanceWaitCompaction(ctx, p, "thread"); result <- err }()
			select {
			case err := <-result:
				t.Fatalf("saved item completed compaction early: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			event := Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"thread","turn":{"id":"compact","status":"completed"}}`)}
			if finish == "idle" {
				event = Message{Method: "thread/status/changed", Params: json.RawMessage(`{"threadId":"thread","status":{"type":"idle"}}`)}
			}
			p.events <- event
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResumeRetryUsesPersistedDeliveryIdentityAfterHostStarts(t *testing.T) {
	for _, resumeAttempt := range []bool{false, true} {
		t.Run(fmt.Sprintf("resume_existing_thread=%t", resumeAttempt), func(t *testing.T) { assertResumeRetryIdentity(t, resumeAttempt) })
	}
}

func assertResumeRetryIdentity(t *testing.T, resumeAttempt bool) {
	t.Helper()
	p, _ := processFixture(t, "eof")
	defer p.Close()
	identity, err := CaptureProcess(p.PID())
	if err != nil {
		t.Fatal(err)
	}
	r := New(t.TempDir(), "unused")
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	record := fixtureRecord(t, r)
	record.State = "starting"
	record.Host = identity
	record.Server = identity
	record.ResumeAttempt = resumeAttempt
	record.ResumeText = "read checkpoint"
	record.Spec.Prompt = record.ResumeText
	record.ResumeDeliveryKey = hash([]byte("deliver\x00checkpoint-event"))
	initial, err := initialHostCommand(record)
	if err != nil {
		t.Fatal(err)
	}
	record.Commands = []queuedCommand{{Operation: initial.Operation, Key: initial.Key, Text: initial.Text, State: "queued"}}
	if err = r.save(&record); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socketName(r.Brain, record.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	result := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		var request hostRequest
		if err = json.NewDecoder(conn).Decode(&request); err != nil {
			result <- err
			return
		}
		if request.Operation != "deliver" || request.Key != record.ResumeDeliveryKey {
			err = fmt.Errorf("resume retry changed persisted delivery identity: %+v", request)
		}
		if err == nil {
			err = r.queueCommand(&record, request)
		}
		message := ""
		if err != nil {
			message = err.Error()
		}
		if writeErr := json.NewEncoder(conn).Encode(map[string]string{"error": message}); writeErr != nil {
			err = writeErr
		}
		result <- err
	}()
	ref := session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, EpicID: record.Spec.EpicID, Generation: record.Spec.Generation, ConversationID: record.ThreadID, DeliveryKey: "checkpoint-event"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = r.Resume(ctx, ref, record.ResumeText); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if len(record.Commands) != 1 {
		t.Fatal("resume retry queued another turn")
	}
}

func TestHeldPrelaunchResumeUsesFreshInitialDelivery(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "factory")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(t.TempDir(), binary)
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	record := fixtureRecord(t, r)
	record.State = "held"
	record.ThreadID = ""
	record.TurnID = ""
	record.Deliveries = map[string]string{}
	record.Spec.Prompt = "original launch text"
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	ref := session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, EpicID: record.Spec.EpicID, Generation: "renewed-generation", DeliveryKey: "wake-transition"}
	if err := r.Resume(context.Background(), ref, "fresh wake message"); err != nil {
		t.Fatal(err)
	}
	resumed, err := r.read(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := initialHostCommand(resumed)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ResumeAttempt || resumed.ThreadID != "" || resumed.Spec.Prompt != "fresh wake message" || initial.Text != resumed.Spec.Prompt || initial.Key != hash([]byte("deliver\x00wake-transition")) || resumed.Spec.InstructionsHash != record.Spec.InstructionsHash {
		t.Fatalf("held launch lost fresh binding: record=%+v initial=%+v", resumed, initial)
	}
	if err = r.queueCommand(&resumed, initial); err != nil {
		t.Fatal(err)
	}
	if err = r.queueCommand(&resumed, initial); err != nil {
		t.Fatal(err)
	}
	if len(resumed.Commands) != 1 {
		t.Fatal("initial delivery retry duplicated")
	}
}
