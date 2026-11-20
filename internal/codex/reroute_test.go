package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func TestActiveSteerRechecksAuthorityBeforeAnySubmission(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "factory")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(t.TempDir(), binary)
	record := fixtureRecord(t, r)
	err := r.handleHostCommand(context.Background(), nil, &record, hostRequest{Operation: "deliver", Key: "new-steer", Text: "inbox message"})
	if err == nil || !strings.Contains(err.Error(), "factory authority is unavailable") {
		t.Fatalf("stale authority reached submission: %v", err)
	}
	if _, exists := record.Deliveries["new-steer"]; exists {
		t.Fatal("unauthorized delivery created submission intent")
	}
}

func TestCoordinatorRerouteAbortsAndRevokesAuthority(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	record := fixtureRecord(t, r)
	err := r.handleEvent(context.Background(), nil, Owner{}, &record, Message{Method: "model/rerouted", Params: json.RawMessage(`{"threadId":"same-thread"}`)})
	if err == nil || !strings.Contains(err.Error(), "model rerouting") {
		t.Fatalf("reroute did not abort host: %v", err)
	}
	stored, err := r.read(record.ID)
	if err != nil || !stored.Rerouted {
		t.Fatalf("reroute not persisted: %+v %v", stored, err)
	}
	if _, err = ControlAuthority(r.Brain, record.ID); err == nil || !strings.Contains(err.Error(), "model rerouting") {
		t.Fatalf("rerouted coordinator retained authority: %v", err)
	}
}

func TestHelperRerouteAbortsBeforeMoreToolRequests(t *testing.T) {
	b := &Backend{Brain: t.TempDir()}
	a := helperAttempt{Schema: 1, Job: helper.Job{ID: "rerouted-helper", Round: 1, Generation: 1, Route: executor.Route{Provider: "codex", Model: "pinned"}}, Selection: executor.Selection{Executor: "codex"}, ThreadID: "thread", TurnID: "turn", TurnIntent: true}
	err := b.helperEvent(context.Background(), nil, &a, RemoteEnvironment{}, Message{Method: "model/rerouted", Params: json.RawMessage(`{"threadId":"thread"}`)})
	if err == nil || !strings.Contains(err.Error(), "model rerouted") {
		t.Fatalf("helper reroute continued: %v", err)
	}
	stored, err := b.read(a.Job)
	if err != nil || !stored.Rerouted {
		t.Fatalf("helper reroute not persisted: %+v %v", stored, err)
	}
	if err = ValidateHelperAttempt(b.Brain, a.Job); err == nil {
		t.Fatal("rerouted helper retained tool authority")
	}
}
