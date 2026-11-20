package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestCoordinatorContract2PlannerMixedCaseRepositoryReadAdmission(t *testing.T) {
	for _, tc := range []struct {
		op      string
		payload string
	}{
		{"github_base", `{"repo":"kingpinXD/factory-executor-smoke","base":"main"}`},
		{"github_prs", `{"repo":"kingpinXD/factory-executor-smoke"}`},
		{"github_commit", `{"repo":"kingpinXD/factory-executor-smoke","sha":"main"}`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			ctx := context.Background()
			brain, record, _ := localReadAttempt(t, true)
			registry := filepath.Join(brain, "repos", "factory-executor-smoke.md")
			if err := os.MkdirAll(filepath.Dir(registry), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(registry, []byte("# factory-executor-smoke\n- **Repo:** `kingpinXD/factory-executor-smoke`\n- **Default branch:** `main`\n"), 0600); err != nil {
				t.Fatal(err)
			}
			dispatchActivate(t, brain, record)
			id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: record.ID, AttemptID: record.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "planner-" + tc.op}
			request := []byte(`{"op":"` + tc.op + `","payload":` + tc.payload + `}`)
			if _, _, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryControl, 2, request); err != nil {
				t.Fatal(err)
			}
			original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, request, &id, nil)
			if err != nil {
				t.Fatal(err)
			}
			originalRef, err := controlrecord.StoreOriginal(brain, original)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := (reconcile.NativeAuthority{Brain: brain}).CaptureControlOutputWriter()
			if err != nil {
				t.Fatal(err)
			}
			witness, err := reconcile.CaptureCoordinatorReadSourceLocked(ctx, brain, originalRef, reconcile.ControlReadDirect, writer)
			if err != nil {
				t.Fatal(err)
			}
			pre, err := reconcile.ReadControlReadPreSourceLocal(ctx, brain, witness)
			if err != nil {
				t.Fatalf("planner read source failed before control enrollment: %v", err)
			}
			if pre.Coordinator == nil || pre.Helper != nil || pre.Coordinator.Work != nil || pre.Coordinator.Input.Repo != "kingpinXD/factory-executor-smoke" || pre.Coordinator.Input.Base != "main" {
				t.Fatalf("planner read lost registered repository or default base: %+v", pre)
			}
			input := pre.Coordinator.Ingress.Input
			if input.PayloadSHA256 != sha256.Sum256(original.Payload) || input.Target != (effects.ControlTarget{}) {
				t.Fatalf("derived read changed original payload or invented a work target: %+v", input)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			programme, err := reconcile.NewProgramEffects(brain, binary)
			if err != nil {
				t.Fatal(err)
			}
			control, err := programme.Service().EnrollControl(ctx, id, input)
			if err != nil {
				t.Fatal(err)
			}
			admission, err := reconcile.BindCoordinatorReadSourceLocked(ctx, brain, witness, control)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := reconcile.FindControlReadAdmissionLocal(ctx, brain, originalRef, reconcile.ControlReadDirect)
			if err != nil || !reflect.DeepEqual(retained, admission) {
				t.Fatalf("recovered read differs from admitted read: %+v %v", retained, err)
			}
			stored, err := controlrecord.ReadOriginal(brain, originalRef)
			if err != nil || !reflect.DeepEqual(stored, original) {
				t.Fatalf("default base changed the immutable request: %+v %v", stored, err)
			}

			fields := []string{"repo", "payload_hash"}
			if tc.op == "github_base" {
				fields = append(fields, "base")
			}
			for _, field := range fields {
				altered := pre
				coordinator := *pre.Coordinator
				altered.Coordinator = &coordinator
				switch field {
				case "repo":
					coordinator.Input.Repo = "kingpinXD/foreign"
				case "base":
					coordinator.Input.Base = "foreign"
				case "payload_hash":
					coordinator.Ingress.Input.PayloadSHA256 = effects.Digest{1}
				}
				data, err := json.Marshal(altered)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(witness.Path, data, 0600); err != nil {
					t.Fatal(err)
				}
				witness.SHA256 = sha256.Sum256(data)
				if _, err := reconcile.ReadControlReadPreSourceLocal(ctx, brain, witness); !errors.Is(err, effects.ErrEvidence) {
					t.Fatalf("changed %s accepted with a valid content hash: %v", field, err)
				}
			}
		})
	}
}
