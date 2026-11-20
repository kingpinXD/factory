package main

import (
	"bytes"
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
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlAuthorityFailureCorrectionIsRestrictedToPreEffectStages(t *testing.T) {
	for _, tc := range []struct {
		stage string
		cause error
		code  string
	}{
		{"owner_authority_validation", effects.ErrAuthority, "control_authority_invalid"},
		{"helper_ingress_build", helper.ErrAuthority, "control_authority_invalid"},
		{"helper_admission_capture", effects.ErrEvidence, "control_evidence_invalid"},
		{"owner_authority", codex.ErrAcceptancePending, "control_qualification_required"},
		{"work_admission_check", helper.ErrControlAdmissionClosed, "control_admission_closed"},
		{"control_authority_recheck", codex.ErrAttemptStale, "control_authority_invalid"},
		{"control_ingress", effects.ErrAuthority, ""},
		{"helper_submit", helper.ErrAuthority, ""},
		{"program_write", effects.ErrAuthority, ""},
		{"control_enrollment", effects.ErrUnresolved, ""},
		{"owner_authority_validation", errors.New("private-prompt"), ""},
	} {
		t.Run(tc.stage+fmt.Sprintf("/%T", tc.cause), func(t *testing.T) {
			value, ok := controlAdmissionFailure(tc.stage, fmt.Errorf("private-token: %w", tc.cause))
			if ok != (tc.code != "") {
				t.Fatalf("correction=%v for %s: %+v", ok, tc.stage, value)
			}
			if ok && (value.Code != tc.code || value.ToolError == "") {
				t.Fatalf("missing fixed code: %+v", value)
			}
			_ = toolcontract.ToolFailure(value)
		})
	}
}

func TestControlV2HelperScopeFailureIsCorrectableAndRecorded(t *testing.T) {
	for _, field := range []string{"set_id", "work_id"} {
		t.Run(field, func(t *testing.T) {
			brain, record, service, request := liveExplorerAdmissionAttempt(t)
			if field == "set_id" {
				request.SetID = ""
			} else {
				request.WorkID = ""
			}
			data, id := writeInvocation(t, record, "invalid-"+field, "helper_submit", request)
			value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
			if err != nil {
				t.Fatalf("admission check became RPC error: %v", err)
			}
			encoded, _ := json.Marshal(value)
			var failure toolcontract.ToolFailure
			code := "invalid_input"
			if json.Unmarshal(encoded, &failure) != nil || failure.Code != code {
				t.Fatalf("missing fixed corrective code: %s", encoded)
			}
			ref, _, err := controlrecord.FindControlOriginal(brain, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := controlrecord.FindOutcome(brain, ref); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected request has outcome: %v", err)
			}
			diagnostic, err := os.ReadFile(filepath.Join(brain, "factory/control-records/failure", filepath.Base(ref.Path)))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(diagnostic, []byte(code)) || !bytes.Contains(diagnostic, []byte("helper_admission_capture")) {
				t.Fatalf("missing admission cause: %s", diagnostic)
			}
			jobs, err := service.Jobs()
			if err != nil || len(jobs) != 0 {
				t.Fatalf("refused call admitted helper: %+v %v", jobs, err)
			}
		})
	}
}

func TestCodexHostFailureRetainsSanitizedErrorOutput(t *testing.T) {
	brain, record, _ := localReadAttempt(t, false)
	var stdout, stderr bytes.Buffer
	code := runCodexHost([]string{"--brain", brain, "--session", record.ID, "--attempt", record.AttemptID, "--claim", "private-token"}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "control_authority_invalid") || strings.Contains(stderr.String(), "private-token") {
		t.Fatalf("unsafe or missing host error: %d %s", code, stderr.String())
	}
	filename := filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID, "host-errors.log")
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("control_authority_invalid")) || bytes.Contains(data, []byte("private-token")) {
		t.Fatalf("unsafe host log: %s", data)
	}
}

func TestControlV2PriorEnrollmentAuthorityFailureRemainsUncertain(t *testing.T) {
	brain, record, _, request := liveExplorerAdmissionAttempt(t)
	data, id := writeInvocation(t, record, "enrolled-no-outcome", "helper_submit", request)
	attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
	if _, err := coordinatorControlV2(context.Background(), brain, attempt, data); err != nil {
		t.Fatal(err)
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, err := controlrecord.FindOutcome(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(outcome.Path); err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	filename := store.InputsPath(ix[record.Spec.Task].Dir)
	before, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, append(before, []byte("\n# changed authority basis\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	value, err := coordinatorControlV2(context.Background(), brain, attempt, data)
	if err == nil || value != nil {
		t.Fatalf("prior admission mislabeled as a new refusal: %v %v", value, err)
	}
	if !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("wrong authority failure: %v", err)
	}
}
