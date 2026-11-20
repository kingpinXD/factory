package main

import (
	"errors"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// Only checks before effect execution can report a safely correctable refusal.
func controlAdmissionFailure(stage string, cause error) (failure toolcontract.ToolFailure, correctable bool) {
	switch stage {
	case "coordinator_authority", "control_authority_recheck", "owner_authority", "helper_ingress_build", "helper_admission_capture", "event_source_capture", "output_ingress_capture", "read_source_capture", "local_input", "reply_snapshot", "work_admission_check", "owner_authority_validation":
	default:
		return toolcontract.ToolFailure{}, false
	}
	check := helper.ControlAdmissionSubReason(cause)
	defer func() {
		if correctable && check != "" {
			failure.ToolError = check + ": " + failure.ToolError
		}
	}()
	var input *toolcontract.ToolInputError
	if errors.As(cause, &input) {
		return toolcontract.ToolFailure{Code: "invalid_input", ToolError: input.Message}, true
	}
	switch {
	case errors.Is(cause, effects.ErrAuthority), errors.Is(cause, helper.ErrAuthority), errors.Is(cause, codex.ErrAttemptStale):
		return toolcontract.ToolFailure{Code: "control_authority_invalid", ToolError: "The caller's authority is no longer current. Inspect lease_check and wait for the owning coordinator to restart under the current activation; this call did not execute an effect."}, true
	case errors.Is(cause, effects.ErrEvidence):
		switch check {
		case "scope":
			return toolcontract.ToolFailure{Code: "control_evidence_invalid", ToolError: "Inspect lease_check and the current work evidence. Copy owner_id, set_id, work_id and lease from the current owning set; this call did not execute an effect."}, true
		case "step":
			return toolcontract.ToolFailure{Code: "control_evidence_invalid", ToolError: "The work history cannot be validated. Read the work events and current state, then wait for program recovery before submitting again; this call did not execute an effect."}, true
		case "ingress_source":
			return toolcontract.ToolFailure{Code: "control_evidence_invalid", ToolError: "The coordinator's recorded source evidence is no longer valid. Inspect lease_check and wait for the owning coordinator to restart under current authority; this call did not execute an effect."}, true
		}
		return toolcontract.ToolFailure{Code: "control_evidence_invalid", ToolError: "Control admission evidence does not match the current work state. Inspect the current owner and work evidence before correcting the request; this call did not execute an effect."}, true
	case errors.Is(cause, codex.ErrAcceptancePending), errors.Is(cause, codex.ErrCapabilitiesUnverified):
		return toolcontract.ToolFailure{Code: "control_qualification_required", ToolError: "Current executor qualification is required. Wait for qualification and the owning coordinator's restart; this call did not execute an effect."}, true
	case errors.Is(cause, helper.ErrControlAdmissionClosed):
		return toolcontract.ToolFailure{Code: "control_admission_closed", ToolError: "Control admission is closed. Inspect helper_result and wait for confirmed settlement before submitting again; this call did not execute an effect."}, true
	}
	return toolcontract.ToolFailure{}, false
}
