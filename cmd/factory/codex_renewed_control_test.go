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
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
)

func TestControlV2CurrentLeaseOrchestratorPlanInput(t *testing.T) {
	brain, record, _, _ := liveExplorerAdmissionAtLease(t, 2, true)
	data, id := writeInvocation(t, record, "lease-two-plan", "plan_input", map[string]any{})
	value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
	if err != nil {
		t.Fatalf("current set owner could not read its epic plan input: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var reply map[string]string
	want := "accepted plan <>&\n"
	if json.Unmarshal(encoded, &reply) != nil || reply["text"] != want || reply["sha256"] != fmt.Sprintf("%x", sha256.Sum256([]byte(want))) {
		t.Fatalf("current set owner plan_input did not return its exact snapshot: %s", encoded)
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, outcome, err := controlrecord.FindOutcome(brain, original); err != nil || string(outcome.Reply) != string(encoded) {
		t.Fatalf("plan input did not retain outcome: %s %v", outcome.Reply, err)
	}
}

func TestControlV2CurrentLeaseHelperAdmission(t *testing.T) {
	brain, record, service, request := liveExplorerAdmissionAtLease(t, 2, true)
	data, id := writeInvocation(t, record, "lease-two-helper", "helper_submit", request)
	value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
	if err != nil {
		t.Fatalf("current set owner could not submit helper: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var accepted helper.Accepted
	if json.Unmarshal(encoded, &accepted) != nil || accepted.JobID == "" || accepted.State != "queued" {
		t.Fatalf("current lease helper was not accepted: %s", encoded)
	}
	job, err := service.Job(accepted.JobID)
	if err != nil || job.Request != request || job.Scope.Lease != 2 || job.Scope.CodexContract != 2 {
		t.Fatalf("helper retained stale authority: %+v %v", job, err)
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, outcome, err := controlrecord.FindOutcome(brain, original); err != nil || string(outcome.Reply) != string(encoded) {
		t.Fatalf("accepted helper did not retain outcome: %s %v", outcome.Reply, err)
	}
}

func TestControlV2CurrentLeaseRejectsStaleAuthority(t *testing.T) {
	for _, change := range []string{"old_attempt_lease", "old_helper_lease", "changed_owner_inputs"} {
		t.Run(change, func(t *testing.T) {
			brain, record, service, request := liveExplorerAdmissionAtLease(t, 2, true)
			switch change {
			case "old_attempt_lease":
				record.Spec.Generation = "owner1:1:orchestrator"
				attemptDir := filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID)
				if err := store.WriteJSON(filepath.Join(attemptDir, "state.json"), record); err != nil {
					t.Fatal(err)
				}
				generation := fmt.Sprintf("%x", sha256.Sum256([]byte(record.Spec.Generation+"\x00"+record.AttemptID)))
				remote := workspace.RemoteOwner{ID: record.ID, Generation: generation, Worktree: record.Spec.Cwd, RunDir: filepath.Join(attemptDir, "remote"), OutputRoot: record.Spec.Cwd, Image: record.Selection.ContainerImageID, ExecBinary: record.Selection.Binaries["codex-executor"], CodexVersion: record.Selection.Profile.CodexVersion, ControlOnly: true, CodexContract: 2}
				controlFenceContainment(t, remote)
			case "old_helper_lease":
				request.Lease = 1
			case "changed_owner_inputs":
				ix, err := store.ReadIndex(brain)
				if err != nil {
					t.Fatal(err)
				}
				name := store.InputsPath(ix["owner1"].Dir)
				inputs, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, append(inputs, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			data, _ := writeInvocation(t, record, change, "helper_submit", request)
			value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
			if change == "old_attempt_lease" {
				if value != nil || !errors.Is(err, effects.ErrAuthority) || !strings.Contains(err.Error(), "coordinator lease changed") {
					t.Fatalf("old attempt was not fenced by its lease: %v %v", value, err)
				}
				jobs, err := service.Jobs()
				if err != nil || len(jobs) != 0 {
					t.Fatalf("old attempt admitted a helper: %+v %v", jobs, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("stale authority became a dynamic tool failure: %v", err)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var failure toolcontract.ToolFailure
			if json.Unmarshal(encoded, &failure) != nil || failure.ToolError == "" || failure.Code != "control_authority_invalid" {
				t.Fatalf("stale authority lacks its fixed refusal code: %s", encoded)
			}
			jobs, err := service.Jobs()
			if err != nil || len(jobs) != 0 {
				t.Fatalf("stale authority admitted a helper: %+v %v", jobs, err)
			}
		})
	}
}
