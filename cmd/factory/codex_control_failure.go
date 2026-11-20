package main

import (
	"errors"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// Only checks before effect execution can report a safely correctable refusal.
func controlAdmissionFailure(stage string, cause error) (toolcontract.ToolFailure, bool) {
	switch stage {
	case "coordinator_authority", "control_authority_recheck", "owner_authority", "helper_ingress_build", "helper_admission_capture", "event_source_capture", "output_ingress_capture", "read_source_capture", "local_input", "reply_snapshot", "work_admission_check", "owner_authority_validation":
	default:
		return toolcontract.ToolFailure{}, false
	}
	var input *toolcontract.ToolInputError
	if errors.As(cause, &input) {
		return toolcontract.ToolFailure{Code: "invalid_input", ToolError: input.Message}, true
	}
	switch {
	case errors.Is(cause, effects.ErrAuthority), errors.Is(cause, helper.ErrAuthority), errors.Is(cause, codex.ErrAttemptStale):
		return toolcontract.ToolFailure{Code: "control_authority_invalid", ToolError: "The caller's authority is no longer current. Inspect lease_check and wait for the owning coordinator to restart under the current activation; this call did not execute an effect."}, true
	case errors.Is(cause, effects.ErrEvidence):
		return toolcontract.ToolFailure{Code: "control_evidence_invalid", ToolError: "Control admission evidence does not match the current work state. Inspect the current owner and work evidence before correcting the request; this call did not execute an effect."}, true
	case errors.Is(cause, codex.ErrAcceptancePending), errors.Is(cause, codex.ErrCapabilitiesUnverified):
		return toolcontract.ToolFailure{Code: "control_qualification_required", ToolError: "Current executor qualification is required. Wait for qualification and the owning coordinator's restart; this call did not execute an effect."}, true
	case errors.Is(cause, helper.ErrControlAdmissionClosed):
		return toolcontract.ToolFailure{Code: "control_admission_closed", ToolError: "Control admission is closed. Inspect helper_result and wait for confirmed settlement before submitting again; this call did not execute an effect."}, true
	}
	return toolcontract.ToolFailure{}, false
}
