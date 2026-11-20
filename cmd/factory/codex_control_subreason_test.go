package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlHelperAdmissionRefusalRetainsSubReason(t *testing.T) {
	for _, check := range []string{"scope", "output_path"} {
		t.Run(check, func(t *testing.T) {
			brain, record, service, request := liveExplorerAdmissionAttempt(t)
			if check == "scope" {
				request.WorkID = ""
			} else {
				request.OutputPath = "relative.md"
			}
			data, id := writeInvocation(t, record, "named-"+check, "helper_submit", request)
			output, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
			if err != nil {
				t.Fatal(err)
			}
			failure, ok := output.(toolcontract.ToolFailure)
			if !ok || failure.Code != "invalid_input" || !strings.Contains(failure.ToolError, check) {
				t.Fatalf("missing corrective check: %+v", output)
			}
			ref, _, err := controlrecord.FindControlOriginal(brain, id)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(brain, "factory/control-records/failure", filepath.Base(ref.Path))
			diagnostic, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			var saved map[string]any
			if err := json.Unmarshal(diagnostic, &saved); err != nil {
				t.Fatal(err)
			}
			if saved["sub_reason"] != check {
				t.Fatalf("missing fixed check: %s", diagnostic)
			}
			host, err := os.ReadFile(filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID, "host-errors.log"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(host), `"sub_reason":"`+check+`"`) {
				t.Fatalf("host omitted check: %s", host)
			}
			if _, _, err := controlrecord.FindOutcome(brain, ref); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("diagnostic became outcome: %v", err)
			}
			jobs, err := service.Jobs()
			if err != nil || len(jobs) != 0 {
				t.Fatalf("corrective refusal admitted job: %v %v", jobs, err)
			}
			if _, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data); err == nil {
				t.Fatalf("old original was replayed: %v", err)
			}
		})
	}
}

func TestCodexHelperInputCorrectionPreservesWrappedAdmission(t *testing.T) {
	for _, check := range []string{"output_path", "step"} {
		t.Run(check, func(t *testing.T) {
			ctx := context.Background()
			brain, record, service, request := liveExplorerAdmissionAttempt(t)
			if check == "output_path" {
				request.OutputPath = "relative.md"
			} else {
				request.Component = "implementor"
			}
			_, id := writeInvocation(t, record, "wrapped-"+check, "helper_submit", request)
			payload, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := json.Marshal(toolcontract.Envelope{Op: "helper_submit", Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, envelope, &id, nil)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := controlrecord.StoreOriginal(brain, original)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := (reconcile.NativeAuthority{Brain: brain}).CaptureControlOutputWriter()
			if err != nil {
				t.Fatal(err)
			}
			source, err := reconcile.BuildControlIngressLocked(ctx, brain, ref, writer)
			if err != nil {
				t.Fatal(err)
			}
			_, captureErr := service.CaptureControlAdmission(ctx, ref, source)
			if helper.ControlAdmissionSubReason(captureErr) != check {
				t.Fatalf("wrong fixture check: %v", captureErr)
			}
			for _, wrapped := range []error{fmt.Errorf("transport: %w", captureErr), errors.Join(errors.New("transport"), captureErr)} {
				converted := codexHelperInputError(wrapped)
				var input *toolcontract.ToolInputError
				if !errors.As(converted, &input) || helper.ControlAdmissionSubReason(converted) != check {
					t.Fatalf("wrapped correction lost: %v", converted)
				}
				if check == "output_path" && !errors.Is(converted, helper.ErrOutputPath) {
					t.Fatalf("output guard lost: %v", converted)
				}
				var phase *helper.PhaseMismatchError
				if check == "step" && !errors.As(converted, &phase) {
					t.Fatalf("typed phase guard lost: %v", converted)
				}
			}
		})
	}
}
