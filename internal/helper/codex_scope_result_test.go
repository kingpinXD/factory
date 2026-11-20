package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func assertResultScopeFacts(t *testing.T, s Service, id string, want Scope, comparison string) RoundResult {
	t.Helper()
	ledger := filepath.Join(s.root(), "ledger.json")
	before, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(ledger)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("metadata read changed ledger/reservations: %v", err)
	}
	data, _ := json.Marshal(result)
	var body struct {
		Recovery map[string]json.RawMessage `json:"recovery"`
	}
	if err = json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	var stored Scope
	if err = json.Unmarshal(body.Recovery["stored_scope"], &stored); err != nil || stored != want {
		t.Fatalf("immutable scope unavailable/changed: %s err=%v", data, err)
	}
	if string(body.Recovery["scope_matches_current"]) != comparison {
		t.Fatalf("scope comparison is not factual %s: %s", comparison, data)
	}
	return result
}
func TestCodexResultScopeFactsSurviveClosedAndRetiredScope(t *testing.T) {
	s, d, req := setupCodex(t)
	req.SetID = "set1"
	req.Lease = 1
	a := strictEmptyAuthority(s, req)
	s.Authority = a
	d.Service = s
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	original, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	first := assertResultScopeFacts(t, s, original.ID, original.Scope, "true")
	if first.State != "completed" {
		t.Fatal(first)
	}
	if err = s.Fence(context.Background(), req.WorkID, "coordinator boundary"); err != nil {
		t.Fatal(err)
	}
	assertResultScopeFacts(t, s, original.ID, original.Scope, "true")
	next := ContinueRequest{SchemaVersion: 1, RequestKey: "next-review", ExpectedRound: 1, Prompt: "second review"}
	if _, err = s.Continue(context.Background(), original.ID, next); err == nil || err.Error() != "authority admission closed" {
		t.Fatalf("equal closed scope granted permission: %v", err)
	}
	a.mu.Lock()
	a.current.Generation = "g2"
	a.mu.Unlock()
	if _, err = s.RecoverScope(context.Background(), req.WorkID); err != nil {
		t.Fatal(err)
	}
	current := assertResultScopeFacts(t, s, original.ID, original.Scope, "false")
	if _, err = s.Continue(context.Background(), original.ID, next); err == nil || err.Error() != "scope admission closed or stale" {
		t.Fatalf("retired scope was continued: %v", err)
	}
	kept, _ := s.Job(original.ID)
	if kept.Request != original.Request || kept.Scope != original.Scope || kept.Round != 1 || current.State != "completed" || current.TurnID != first.TurnID || current.ReplySHA256 != first.ReplySHA256 {
		t.Fatalf("scope facts changed successful old helper: %+v %+v", kept, current)
	}
}
func TestCodexResultScopeComparisonIncludesEveryScopeField(t *testing.T) {
	for _, field := range []string{"Generation", "Lease", "OwnerID", "SetID", "EpicID", "WorkID", "Worktree", "OutputRoot"} {
		t.Run(field, func(t *testing.T) {
			s, _, req := setupCodex(t)
			req.SetID = "set1"
			req.Lease = 1
			accepted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			job, _ := s.Job(accepted.JobID)
			changed := job.Scope
			switch field {
			case "Generation":
				changed.Generation = "g2"
			case "Lease":
				changed.Lease = 2
			case "OwnerID":
				changed.OwnerID = "owner2"
			case "SetID":
				changed.SetID = "set2"
			case "EpicID":
				changed.EpicID = "epic2"
			case "WorkID":
				changed.WorkID = "work2"
			case "Worktree":
				changed.Worktree += "-other"
			case "OutputRoot":
				changed.OutputRoot += "-other"
			}
			if err = s.transaction(context.Background(), true, func(db *ledger) error { db.Scopes[scopeKey(job.Scope)].Scope = changed; return nil }); err != nil {
				t.Fatal(err)
			}
			assertResultScopeFacts(t, s, job.ID, job.Scope, "false")
		})
	}
}
func TestCodexScopeUnknownIsNullAndMissingBarrierStillFailsClosed(t *testing.T) {
	data, _ := json.Marshal(RecoveryEvidence{})
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	if string(fields["scope_matches_current"]) != "null" {
		t.Fatalf("unknown scope proof is not explicit null: %s", data)
	}
	s, _, req := setupCodex(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	job, _ := s.Job(accepted.JobID)
	if err = s.transaction(context.Background(), true, func(db *ledger) error { delete(db.Scopes, scopeKey(job.Scope)); return nil }); err != nil {
		t.Fatal(err)
	}
	ledger := filepath.Join(s.root(), "ledger.json")
	before, _ := os.ReadFile(ledger)
	if _, err = s.Result(job.ID, 1); err == nil || err.Error() != "missing helper scope barrier" {
		t.Fatalf("corrupt ledger yielded scope permission: %v", err)
	}
	after, _ := os.ReadFile(ledger)
	if !bytes.Equal(before, after) {
		t.Fatal("failed scope read mutated ledger")
	}
}
