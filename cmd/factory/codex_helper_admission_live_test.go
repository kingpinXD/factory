package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func liveExplorerAdmissionAttempt(t *testing.T) (string, codex.Record, helper.Service, helper.Request) {
	t.Helper()
	brain, record, _, scope := localReadAttemptSetup(t, false, false)
	service := reconcile.NativeService(brain, nil)
	request := helper.Request{SchemaVersion: 1, RequestKey: "minmax-explore-1", OwnerID: "owner1", WorkID: "work1", SetID: "owner1", Lease: 1, Component: "explorer", Prompt: "Explore the MinMax implementation and tests."}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	work := ix["work1"]
	history := []events.Event{
		{Seq: 1, Kind: events.KindTransition, Sender: events.SenderProgram, To: "queued", Trigger: "file", TriggerRef: "create"},
		{Seq: 2, Kind: events.KindTransition, Sender: events.SenderProgram, From: "queued", To: "starting", Trigger: "tick", TriggerRef: "tick:3762"},
		{Seq: 3, Kind: events.KindDone, Sender: events.SenderProgram, Text: "assigned", Key: "assign:work1"},
		{Seq: 4, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: "worktree", Key: "repo:worktree:2"},
		{Seq: 5, Kind: events.KindWorktreeReady, Sender: events.SenderProgram, Text: scope.Worktree, TriggerRef: "4", Key: "repo:4"},
	}
	var raw []byte
	for _, event := range history {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, append(line, '\n')...)
	}
	if err := os.WriteFile(store.EventsPath(work.Dir), raw, 0600); err != nil {
		t.Fatal(err)
	}
	dispatchActivate(t, brain, record)
	owner, err := reconcile.ResolveOwnerAuthority(context.Background(), brain, "epic1", "owner1")
	if err != nil {
		t.Fatal(err)
	}
	var basis []map[string]any
	for _, id := range []string{"epic1", "owner1", "work1"} {
		entry := ix[id]
		inputs, err := os.ReadFile(store.InputsPath(entry.Dir))
		if err != nil {
			t.Fatal(err)
		}
		log, err := events.Read(store.EventsPath(entry.Dir))
		if err != nil {
			t.Fatal(err)
		}
		var refs []effects.EventRef
		for _, event := range log {
			if event.Sender == events.SenderProgram && (event.Kind == events.KindTransition && event.From == "" || event.Kind == events.KindHeartbeat && event.Key == "lease:1") {
				encoded, _ := json.Marshal(event)
				refs = append(refs, effects.EventRef{OwnerID: id, Sequence: event.Seq, SHA256: sha256.Sum256(encoded)})
			}
		}
		basis = append(basis, map[string]any{"owner_id": id, "kind": entry.Kind, "inputs": string(inputs), "inputs_sha256": fmt.Sprintf("%x", sha256.Sum256(inputs)), "events": refs})
	}
	acceptance, err := codex.Contract2AcceptanceAuthority(context.Background(), brain, record.Selection)
	if err != nil {
		t.Fatal(err)
	}
	activation := map[string]any{"version": 2, "epic_id": "epic1", "owner_id": "work1", "kind": work.Kind, "selection_hash": record.Spec.SelectionHash, "basis": basis, "after_seq": 5, "acceptance": acceptance, "work_generation": scope.Generation, "owner_activation": owner.Activation}
	encoded, _ := json.Marshal(activation)
	if _, err := events.Append(store.EventsPath(work.Dir), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "codex-authority", Request: "activate", Text: string(encoded)}); err != nil {
		t.Fatal(err)
	}
	request.OutputPath = filepath.Join(work.Dir, "explore.v1.md")
	return brain, record, service, request
}

func TestControlV2ExplorerAdmissionAfterSchedulerStart(t *testing.T) {
	brain, record, service, request := liveExplorerAdmissionAttempt(t)
	data, _ := writeInvocation(t, record, "explorer-submit", "helper_submit", request)
	var accepted helper.Accepted
	for i := range 2 {
		value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
		if err != nil {
			t.Fatalf("explorer admission attempt %d: %v", i+1, err)
		}
		reply, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var current helper.Accepted
		if err := json.Unmarshal(reply, &current); err != nil || current.JobID == "" || current.Round != 1 || current.State != "queued" {
			t.Fatalf("explorer admission did not return job ID: %s (%v)", reply, err)
		}
		if i == 1 && current != accepted {
			t.Fatalf("exact call retry changed admission: %+v != %+v", current, accepted)
		}
		accepted = current
	}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("explorer retry changed durable job count: %+v %v", jobs, err)
	}
	job, err := service.Job(accepted.JobID)
	if err != nil || job.Request != request || job.Scope.StepSlot == "" || job.Scope.CodexContract != 2 {
		t.Fatalf("explorer admission lost request or authority: %+v %v", job, err)
	}
}
