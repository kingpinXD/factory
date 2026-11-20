package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexToneGuideIsExactAndLegacyStaysClosed(t *testing.T) {
	s, _, record, _ := recoveryCoordinator(t)
	guide := filepath.Join(s.Brain, "memory/tone_guide.md")
	if err := os.MkdirAll(filepath.Dir(guide), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(guide, []byte("approved writing guide"), 0600); err != nil {
		t.Fatal(err)
	}
	code, data := recoveryCall(t, s, record, "brain_read", map[string]any{"path": "memory/tone_guide.md"})
	var got struct {
		Text string `json:"text"`
	}
	if code != 0 || json.Unmarshal(data, &got) != nil || got.Text != "approved writing guide" {
		t.Fatalf("exact tone guide denied: %d %s", code, data)
	}
	if _, err := brainRead(s.Brain, "memory/tone_guide.md", brainReadLimit); err == nil || err.Error() != "knowledge path not allowed" {
		t.Fatalf("legacy allowlist changed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(s.Brain, "memory/other.md"), []byte("outside the allowed guide"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"memory/other.md", "memory/tone_guide.md/child.md", "memory/.tone_guide.md", "memory/auth.md", "memory/../memory/tone_guide.md", guide} {
		if _, err := codexBrainRead(s.Brain, path, brainReadLimit); err == nil {
			t.Fatalf("neighbour/unsafe path allowed %q", path)
		}
	}
	if err := os.Remove(guide); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Brain, "AGENTS.md"), []byte("existing symlink target"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(s.Brain, "AGENTS.md"), guide); err != nil {
		t.Fatal(err)
	}
	if _, err := codexBrainRead(s.Brain, "memory/tone_guide.md", brainReadLimit); err == nil {
		t.Fatal("tone symlink accepted")
	}
}

func TestCodexHelperToneGuideUsesBoundControl(t *testing.T) {
	s, req, _, _ := recoveryCoordinator(t)
	dir := filepath.Join(s.Brain, "memory")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tone_guide.md"), []byte("writing guide"), 0600); err != nil {
		t.Fatal(err)
	}
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	binary, _ := os.Executable()
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
		result, err := recoveryCodexTurn(ctx, spec, sink)
		if err != nil {
			return result, err
		}
		if err := store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), map[string]any{"schema": 1, "job": spec.Job, "thread_id": result.ConversationID, "turn_id": result.TurnID, "turn_intent": true}); err != nil {
			return result, err
		}
		args := []string{"--brain", s.Brain, "--job", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--round", "1"}
		ledger := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
		before, _ := os.ReadFile(ledger)
		code, data := knowledgeCLI(t, args, knowledgeEnvelope("factory_helper", "brain_read", "memory/tone_guide.md", result.ConversationID, result.TurnID))
		var body struct {
			Text string `json:"text"`
		}
		if code != 0 || json.Unmarshal(data, &body) != nil || body.Text != "writing guide" {
			t.Errorf("bound helper guide denied: %d %s", code, data)
			return result, fmt.Errorf("bound helper guide denied: %d %s", code, data)
		}
		after, _ := os.ReadFile(ledger)
		if !bytes.Equal(before, after) {
			return result, fmt.Errorf("guide read changed ledger")
		}
		if code, _ := knowledgeCLI(t, args, knowledgeEnvelope("factory_helper", "brain_read", "memory/tone_guide.md", result.ConversationID, "stale")); code == 0 {
			return result, fmt.Errorf("stale helper read guide")
		}
		checked = true
		return result, nil
	}}}, Quotas: map[string]helper.UsageReader{"codex": &recoveryCodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(accepted.JobID, 1)
	if err != nil || !checked || result.State != "completed" {
		t.Fatalf("guide helper not completed: checked=%v result=%+v err=%v", checked, result, err)
	}
}

func TestCodexPlannerReadsOnlyChildRecoveryMetadata(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	planner := workflowPlanner(t, s, record)
	ledger := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
	read := func(wantSettled bool) {
		t.Helper()
		before, _ := os.ReadFile(ledger)
		code, data := recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1})
		var body map[string]json.RawMessage
		if code != 0 || json.Unmarshal(data, &body) != nil {
			t.Fatalf("planner cannot inspect named blocker: %d %s", code, data)
		}
		for _, field := range []string{"schema_version", "job_id", "round", "state", "recovery", "read_only"} {
			if _, ok := body[field]; !ok {
				t.Fatalf("missing %s: %s", field, data)
			}
		}
		if len(body) != 6 || string(body["read_only"]) != "true" {
			t.Fatalf("planner received excess content: %s", data)
		}
		if bytes.Contains(data, []byte("output_root")) || bytes.Contains(data, []byte("reply")) || bytes.Contains(data, []byte("error_text")) {
			t.Fatalf("planner received private output: %s", data)
		}
		var proof struct {
			Stopped bool `json:"stopped"`
			Settled bool `json:"publication_settled"`
		}
		if json.Unmarshal(body["recovery"], &proof) != nil || proof.Settled != wantSettled || wantSettled && !proof.Stopped {
			t.Fatalf("stale/fabricated recovery proof: %s", data)
		}
		after, _ := os.ReadFile(ledger)
		if !bytes.Equal(before, after) {
			t.Fatal("read-only inspection mutated ledger")
		}
	}
	read(false)
	for _, op := range []string{"helper_continue", "helper_cancel"} {
		if code, data := recoveryCall(t, s, planner, op, map[string]any{"job_id": accepted.JobID, "round": 1, "request": map[string]any{"reason": "forbidden"}}); code == 0 {
			t.Fatalf("planner mutated child via %s: %s", op, data)
		}
	}
	if code, data := recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 2}); code == 0 {
		t.Fatalf("unknown round accepted: %s", data)
	}
	planner.TurnID = "stale"
	if code, _ := recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1}); code == 0 {
		t.Fatal("stale planner read child")
	}
	planner.TurnID = "turn"
	if err := s.Cancel(context.Background(), accepted.JobID, "normal owning operator cancellation"); err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := helper.Run(ctx, helper.RunnerDeps{Service: s, FactoryBin: binary, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	read(true)
	ownCode, ownData := recoveryCall(t, s, record, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1})
	if ownCode != 0 || !bytes.Contains(ownData, []byte("reply")) {
		t.Fatalf("own-owner result changed: %d %s", ownCode, ownData)
	}
}

func TestCodexPhaseMismatchIsCorrectiveWithoutAdmission(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	ix, _ := store.ReadIndex(s.Brain)
	req.Component = "verifier"
	req.OutputPath = filepath.Join(ix[req.WorkID].Dir, "verify.v1.md")
	code, data := recoveryCall(t, s, record, "helper_submit", req)
	var failure codex.ToolFailure
	if code != 0 || json.Unmarshal(data, &failure) != nil || !strings.Contains(failure.ToolError, "verifying") || !strings.Contains(failure.ToolError, "not admitted") {
		t.Fatalf("phase mismatch was not precise/corrective: %d %s", code, data)
	}
	if jobs, _ := s.Jobs(); len(jobs) != 0 {
		t.Fatal("phase mismatch admitted helper", jobs)
	}
}

func TestCodexToneGuideKeepsFileAndAuthorityGuards(t *testing.T) {
	for _, kind := range []string{"missing", "symlink_file", "symlink_parent", "oversize", "nonregular", "stale_turn", "closed_session"} {
		t.Run(kind, func(t *testing.T) {
			s, _, record, rp := recoveryCoordinator(t)
			dir := filepath.Join(s.Brain, "memory")
			guide := filepath.Join(dir, "tone_guide.md")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if kind != "missing" {
				if err := os.WriteFile(guide, []byte("writing guide"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "symlink_file":
				os.Remove(guide)
				if err := os.Symlink(filepath.Join(t.TempDir(), "missing.md"), guide); err != nil {
					t.Fatal(err)
				}
			case "symlink_parent":
				os.Remove(guide)
				os.Remove(dir)
				if err := os.Symlink(t.TempDir(), dir); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.WriteFile(guide, []byte(strings.Repeat("x", brainReadLimit+1)), 0600); err != nil {
					t.Fatal(err)
				}
			case "nonregular":
				os.Remove(guide)
				if err := os.Mkdir(guide, 0700); err != nil {
					t.Fatal(err)
				}
			case "stale_turn":
				record.TurnID = "stale"
			case "closed_session":
				record.State = "stopped"
				if err := store.WriteJSON(rp, record); err != nil {
					t.Fatal(err)
				}
			}
			code, data := recoveryCall(t, s, record, "brain_read", map[string]any{"path": "memory/tone_guide.md"})
			if kind == "missing" {
				var failure codex.ToolFailure
				if code != 0 || json.Unmarshal(data, &failure) != nil || !strings.Contains(failure.ToolError, "does not exist") {
					t.Fatalf("missing guide was invented/fatal: %d %s", code, data)
				}
			} else if code == 0 {
				t.Fatalf("tone guide softened %s: %s", kind, data)
			}
		})
	}
}

func TestCodexPlannerChildInspectionKeepsRoleAndEpicBoundaries(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	planner := workflowPlanner(t, s, record)
	for _, kind := range []string{"wrong_role", "spoof_name", "non_epic_index", "wrong_epic", "wrong_task"} {
		t.Run(kind, func(t *testing.T) {
			ix, _ := store.ReadIndex(s.Brain)
			original := ix[planner.Spec.Task]
			spec := planner.Spec
			switch kind {
			case "wrong_role":
				spec.Name = "factory:epic1:orchestrator"
			case "spoof_name":
				spec.Name = "spoof:epic1:planner"
			case "non_epic_index":
				entry := original
				entry.Kind = "set"
				ix[spec.Task] = entry
				if err := store.WriteIndex(s.Brain, ix); err != nil {
					t.Fatal(err)
				}
				defer func() { ix[planner.Spec.Task] = original; store.WriteIndex(s.Brain, ix) }()
			case "wrong_epic":
				spec.EpicID = "other"
			case "wrong_task":
				spec.Task = req.OwnerID
			}
			_, err := scopedCoordinatorControl(context.Background(), s.Brain, spec, coordinatorRequest("helper_result", map[string]any{"job_id": accepted.JobID, "round": 1}))
			if kind == "wrong_task" { // Own-owner results retain their established full contract.
				if err != nil {
					t.Fatal("own-owner result denied", err)
				}
			} else if err == nil {
				t.Fatalf("%s granted cross-owner inspection", kind)
			}
		})
	}
}

func TestCodexPlannerCannotInspectForeignEpicHelper(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	planner := workflowPlanner(t, s, record)
	ix, _ := store.ReadIndex(s.Brain)
	epicDir := filepath.Join(s.Brain, "factory/work/foreign")
	setDir := filepath.Join(epicDir, "sets/foreign-set")
	workDir := filepath.Join(setDir, "foreign-work")
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string]store.Entry{"foreign": {Kind: blueprint.MachineEpic, Dir: epicDir, Epic: "foreign"}, "foreign-set": {Kind: blueprint.MachineSet, Dir: setDir, Epic: "foreign"}, "foreign-work": {Kind: blueprint.MachineWork, Dir: workDir, Epic: "foreign"}}
	inputs := map[string]any{"foreign": reconcile.EpicInputs{ID: "foreign", Kind: "text", Input: "other epic"}, "foreign-set": reconcile.SetInputs{ID: "foreign-set", Epic: "foreign", Items: []string{"foreign-work"}}, "foreign-work": reconcile.WorkInputs{ID: "foreign-work", Epic: "foreign", Set: "foreign-set", Repo: "other/repo", Base: "main", Clone: tree}}
	for id, entry := range entries {
		if err := os.MkdirAll(entry.Dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteInputs(entry.Dir, inputs[id]); err != nil {
			t.Fatal(err)
		}
		state := map[string]string{"foreign": "planned", "foreign-set": "running", "foreign-work": "implementing"}[id]
		if _, err := events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: state}); err != nil {
			t.Fatal(err)
		}
		ix[id] = entry
	}
	if _, err := events.Append(store.EventsPath(setDir), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 1, Key: "lease:1"}); err != nil {
		t.Fatal(err)
	}
	request, err := events.Append(store.EventsPath(workDir), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: "worktree"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(workDir), events.Event{Kind: events.KindWorktreeReady, Text: tree, TriggerRef: strconv.Itoa(request.Seq), Key: "repo:" + strconv.Itoa(request.Seq)}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteIndex(s.Brain, ix); err != nil {
		t.Fatal(err)
	}
	selected, err := executor.ReadSelection(s.Brain, "epic1")
	if err != nil {
		t.Fatal(err)
	}
	selected.EpicID = "foreign"
	if err := executor.WriteSelection(s.Brain, selected); err != nil {
		t.Fatal(err)
	}
	req.OwnerID = "foreign-set"
	req.SetID = "foreign-set"
	req.WorkID = "foreign-work"
	registerLegacyCoordinatorFixture(t, s.Brain, selected, session.Spec{Name: "factory:foreign-set:orchestrator", Task: req.OwnerID, EpicID: selected.EpicID, Generation: "foreign-set:1:orchestrator"})
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if code, data := recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1}); code == 0 {
		t.Fatalf("foreign epic recovery exposed: %s", data)
	}
}

func TestPlannerRecoveryProjectionNeverExposesPrivateContent(t *testing.T) {
	secret := "private-sentinel"
	result := helper.RoundResult{SchemaVersion: 1, JobID: "job", Round: 1, State: "unknown", ReplyFile: secret, ReplySHA256: secret, OutputFile: secret, OutputSHA256: secret, ErrorText: secret, ConfiguredModel: secret, ConversationID: secret, TurnID: secret, Recovery: &helper.RecoveryEvidence{StoredScope: helper.Scope{OutputRoot: secret, Worktree: secret}}}
	data, err := json.Marshal(plannerHelperRecovery(result))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(secret)) {
		t.Fatalf("private result content leaked: %s", data)
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(data, &body) != nil || len(body) != 6 || string(body["read_only"]) != "true" {
		t.Fatal("metadata envelope changed", string(data))
	}
	result.Recovery = nil
	projected := plannerHelperRecovery(result)
	if projected.Recovery != nil {
		t.Fatal("unknown recovery fabricated proof")
	}
}
