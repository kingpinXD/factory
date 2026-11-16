package codex

import (
	"context"
	"encoding/json"
	"errors"
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

type capabilityFixture func(context.Context, executor.Selection) error

func (f capabilityFixture) Verify(ctx context.Context, s executor.Selection) error { return f(ctx, s) }

func fixtureRecord(t *testing.T, r *Runtime) Record {
	t.Helper()
	spec := session.Spec{Name: "factory:e1:planner", Task: "e1", EpicID: "e1", Generation: "e1:1:planner", Cwd: t.TempDir(), GHToken: "fixture-secret-must-never-persist"}
	return Record{Schema: 1, ID: hash([]byte(spec.Name)), Spec: spec, Selection: executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "e1"}, State: "active", Token: "fixture-capability", Deliveries: map[string]string{"initial": "complete"}, ThreadID: "same-thread", TurnID: "old-turn", Epoch: 1, StartedAt: time.Now().UTC()}
}

func TestSessionListIsPureAndDeadHostIsUnknown(t *testing.T) {
	p, _ := processFixture(t, "eof")
	identity, err := CaptureProcess(p.PID())
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	waitProcessGone(t, p.PID())
	r := New(t.TempDir(), "must-not-run")
	record := fixtureRecord(t, r)
	record.Host = identity
	record.Server = identity
	if err = r.save(&record); err != nil {
		t.Fatal(err)
	}
	inventory, err := r.List(context.Background())
	if err != nil || !inventory.Complete || len(inventory.Sessions) != 0 || len(inventory.Quarantined) != 1 || inventory.Lookup(record.Spec.Name).Presence != session.Quarantined {
		t.Fatalf("dead host lost its quarantine reservation: %+v %v", inventory, err)
	}
	after, err := r.read(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != "active" || after.Host != identity {
		t.Fatal("inspection changed session authority")
	}
	dir, _ := r.directory(record.ID)
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fixture-secret") {
		t.Fatal("GitHub credential persisted in session record")
	}
}

func TestResumeArchivesStoppedHostAndRetainsConversation(t *testing.T) {
	p, _ := processFixture(t, "eof")
	identity, err := CaptureProcess(p.PID())
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	waitProcessGone(t, p.PID())
	brain := t.TempDir()
	binary := filepath.Join(t.TempDir(), "factory")
	if err = os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(brain, binary)
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	record := fixtureRecord(t, r)
	record.State = "stopped"
	record.Host = identity
	record.Server = identity
	if err = r.save(&record); err != nil {
		t.Fatal(err)
	}
	ref := session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, ConversationID: record.ThreadID, EpicID: "e1", Generation: "e1:2:planner", DeliveryKey: "checkpoint-event"}
	if err = r.Resume(context.Background(), ref, "resume checkpoint"); err != nil {
		t.Fatal(err)
	}
	resumed, err := r.read(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ThreadID != "same-thread" || resumed.Epoch != 2 || !resumed.ResumeAttempt || resumed.State != "starting" || resumed.Spec.Generation != "e1:2:planner" || resumed.Host.PID != 0 || resumed.Token == record.Token || len(resumed.Commands) != 1 || resumed.Commands[0].Key != hash([]byte("deliver\x00checkpoint-event")) {
		t.Fatalf("wrong resume intent: %#v", resumed)
	}
	dir, _ := r.directory(record.ID)
	var archived Record
	if err = readJSON(filepath.Join(dir, "host-1.json"), &archived); err != nil {
		t.Fatal(err)
	}
	if archived.Host != identity || archived.State != "stopped" {
		t.Fatal("previous owner identity lost")
	}
}

func TestHelperFenceCannotBeClearedByStaleEventSave(t *testing.T) {
	b := &Backend{Brain: t.TempDir()}
	a := helperAttempt{Schema: 1, Job: helper.Job{ID: "job", Round: 1, Generation: 1, Route: executor.Route{Provider: "codex", Model: "pinned"}}, Selection: executor.Selection{Executor: "codex"}, PrelaunchError: ErrAcceptancePending.Error()}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	if err := b.Fence(context.Background(), a.Job); err != nil {
		t.Fatal(err)
	}
	stale := a
	stale.PrelaunchError = ""
	stale.ThreadID = "late-thread"
	if err := b.save(stale); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("late event cleared sealed attempt: %v", err)
	}
	stored, err := b.read(a.Job)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Closed || !stored.Fenced || stored.ThreadID != "" {
		t.Fatal("stale write changed fenced attempt")
	}
}

func TestPositivePrelaunchProofSettlesWithoutFabricatedCheckpoint(t *testing.T) {
	b := &Backend{Brain: t.TempDir()}
	a := helperAttempt{Schema: 1, Job: helper.Job{ID: "job", Round: 1, Generation: 1, Route: executor.Route{Provider: "codex", Model: "pinned"}}, Selection: executor.Selection{Executor: "codex"}, PrelaunchError: ErrAcceptancePending.Error()}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	result, known, err := b.Inspect(context.Background(), helper.RunSpec{Job: a.Job}, nil)
	if err != nil || !known || result.ErrorCode != "codex_prelaunch_failed" || result.ObservedModel != "" || result.TurnID != "" || result.ConversationID != "" {
		t.Fatalf("wrong positive no-launch settlement: %#v %v %v", result, known, err)
	}
	if err := writeJSON(filepath.Join(b.directory(a.Job), "remote", "containment.json"), map[string]bool{"uncertain": true}); err != nil {
		t.Fatal(err)
	}
	if err = b.Fence(context.Background(), a.Job); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("ambiguous containment accepted as no launch: %v", err)
	}
}

func TestStoppedTurnObservationNeverResumesOrStarts(t *testing.T) {
	observer := &fixtureCaller{thread: `{"thread":{"id":"same-thread","model":"pinned","modelProvider":"openai","reasoningEffort":"low"}}`, turns: `{"data":[{"id":"old-turn","status":"interrupted"}]}`}
	record := Record{ThreadID: "same-thread", TurnID: "old-turn", Spec: session.Spec{Model: "pinned", Effort: "low"}}
	if err := verifyStoppedTurn(context.Background(), observer, record); err != nil {
		t.Fatal(err)
	}
	if strings.Join(observer.methods, ",") != "thread/read,thread/turns/list" {
		t.Fatalf("observer issued inference: %v", observer.methods)
	}
}

func TestCorruptAndPartialSessionRecordsStayUnknown(t *testing.T) {
	r := New(t.TempDir(), "unused")
	record := fixtureRecord(t, r)
	record.Deliveries = nil
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	if _, err := r.read(record.ID); err == nil {
		t.Fatal("partial record accepted")
	}
	dir, _ := r.directory(record.ID)
	data, _ := json.Marshal(map[string]any{"schema": 1, "id": record.ID, "unexpected": true})
	if err := os.WriteFile(filepath.Join(dir, "state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.read(record.ID); err == nil {
		t.Fatal("corrupt record accepted")
	}
}

func TestRenewedGenerationFencesLiveHostBeforeSameThreadResume(t *testing.T) {
	p, _ := processFixture(t, "eof")
	identity, err := CaptureProcess(p.PID())
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "factory")
	if err = os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(t.TempDir(), binary)
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	record := fixtureRecord(t, r)
	record.State = "idle"
	record.Host, record.Server = identity, identity
	record.Commands = []queuedCommand{{Operation: "deliver", Key: "pending-inbox", Text: "retained message", State: "queued"}}
	if err = r.save(&record); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socketName(r.Brain, record.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stopped := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			stopped <- err
			return
		}
		defer conn.Close()
		var request hostRequest
		if err = json.NewDecoder(conn).Decode(&request); err != nil {
			stopped <- err
			return
		}
		if request.Operation != "stop" || request.Token != record.Token {
			stopped <- errors.New("old owner was not stopped")
			return
		}
		if err = p.Close(); err != nil {
			stopped <- err
			return
		}
		old := record
		old.State = "stopped"
		if err = r.save(&old); err != nil {
			stopped <- err
			return
		}
		err = json.NewEncoder(conn).Encode(map[string]string{"error": ""})
		stopped <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ref := session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, ConversationID: record.ThreadID, EpicID: record.Spec.EpicID, Generation: "e1:2:planner", DeliveryKey: "checkpoint-event"}
	if err = r.Resume(ctx, ref, "read renewed checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err = <-stopped; err != nil {
		t.Fatal(err)
	}
	resumed, err := r.read(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ThreadID != record.ThreadID || resumed.Spec.Generation != ref.Generation || resumed.Epoch != 2 || resumed.Host.PID != 0 || resumed.Token == record.Token || len(resumed.Commands) != 2 || resumed.Commands[0].Key != hash([]byte("deliver\x00checkpoint-event")) || resumed.Commands[1].Key != "pending-inbox" {
		t.Fatalf("unsafe ownership handover: %#v", resumed)
	}
}

func TestRenewedGenerationCannotTransferAnActiveTurn(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	record := fixtureRecord(t, r)
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	ref := session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, ConversationID: record.ThreadID, EpicID: record.Spec.EpicID, Generation: "new-generation", DeliveryKey: "checkpoint-event"}
	err := r.Resume(context.Background(), ref, "checkpoint")
	if err == nil || !strings.Contains(err.Error(), "idle checkpointed") {
		t.Fatalf("active owner changed: %v", err)
	}
}
