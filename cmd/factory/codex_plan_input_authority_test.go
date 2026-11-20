package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexPlanInputSetAndWorkReadOnlyOwnEpicSnapshot(t *testing.T) {
	for _, kind := range []string{blueprint.MachineSet, blueprint.MachineWork} {
		t.Run(kind, func(t *testing.T) {
			brain, spec, ep := coordinatorFixture(t)
			data := []byte("accepted immutable plan\n")
			digest := sha256.Sum256(data)
			filename := filepath.Join(ep.Dir, "plan-input.yaml")
			if err := os.WriteFile(filename, data, 0400); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteInputs(ep.Dir, &reconcile.EpicInputs{ID: spec.EpicID, Kind: "plan", Path: filename, PlanInputSHA256: hex.EncodeToString(digest[:])}); err != nil {
				t.Fatal(err)
			}
			ix, _ := store.ReadIndex(brain)
			ix["owner"] = store.Entry{Kind: kind, Epic: spec.EpicID, Dir: filepath.Join(ep.Dir, "owner")}
			if err := store.WriteIndex(brain, ix); err != nil {
				t.Fatal(err)
			}
			spec.Task = "owner"
			spec.Name = "factory:epic1:owner:orchestrator"
			out, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", struct{}{}))
			if err != nil {
				t.Fatalf("admitted %s cannot read its epic plan: %v", kind, err)
			}
			if out.(map[string]string)["text"] != string(data) {
				t.Fatalf("wrong snapshot: %v", out)
			}
			for _, payload := range []any{map[string]string{"target": "other"}, map[string]string{"path": filename}} {
				if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", payload)); err == nil {
					t.Fatal("caller selected a plan target")
				}
			}
			spec.EpicID = "foreign"
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", struct{}{})); err == nil {
				t.Fatal("foreign owner read plan")
			}
			spec.EpicID = "epic1"
			if err := os.Chmod(filename, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filename, []byte("tampered\n"), 0400); err != nil {
				t.Fatal(err)
			}
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("plan_input", struct{}{})); err == nil {
				t.Fatal("tampered plan accepted")
			}
		})
	}
}

func TestControlV2SetPlanInputThroughAdmittedAttempt(t *testing.T) {
	brain, record, _ := localReadAttempt(t, false)
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	plan := []byte("immutable plan for set owner\n")
	digest := sha256.Sum256(plan)
	filename := filepath.Join(ix[record.Spec.EpicID].Dir, "plan-input.yaml")
	if err := os.WriteFile(filename, plan, 0400); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(ix[record.Spec.EpicID].Dir, &reconcile.EpicInputs{ID: record.Spec.EpicID, Kind: "plan", Path: filename, PlanInputSHA256: hex.EncodeToString(digest[:])}); err != nil {
		t.Fatal(err)
	}
	dispatchActivate(t, brain, record)
	data, id := writeInvocation(t, record, "set-plan-input", "plan_input", struct{}{})
	value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
	if err != nil {
		t.Fatalf("admitted set received RPC error: %v", err)
	}
	encoded, _ := json.Marshal(value)
	var reply map[string]string
	if json.Unmarshal(encoded, &reply) != nil || reply["text"] != string(plan) || reply["sha256"] != hex.EncodeToString(digest[:]) {
		t.Fatalf("wrong plan reply: %s", encoded)
	}
	ref, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := controlrecord.FindOutcome(brain, ref); err != nil {
		t.Fatalf("plan read has no durable outcome: %v", err)
	}
}
