package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type controlFailure struct {
	Schema        int       `json:"schema"`
	At            time.Time `json:"at"`
	RequestSHA256 string    `json:"request_sha256"`
	Operation     string    `json:"operation"`
	Stage         string    `json:"stage"`
	Reason        string    `json:"reason"`
	Cause         string    `json:"cause,omitempty"`
	Code          string    `json:"code"`
	SubReason     string    `json:"sub_reason,omitempty"`
}

// Retain the failure stage and a known reason, never request data or subprocess stderr.
func RecordControlFailure(brain string, attempt AttemptRef, request []byte, stage string, cause error) error {
	if cause == nil {
		return nil
	}
	r := New(brain, "")
	directory, err := r.attemptDirectory(attempt)
	if err != nil {
		return err
	}
	if err := privateAttemptParents(brain, directory, false); err != nil {
		return err
	}
	directory = filepath.Join(directory, "control-failures")
	if err := privateAttemptParents(brain, directory, true); err != nil {
		return err
	}
	name := filepath.Join(directory, hash(request)+".json")
	if stage == "control_process" || stage == "control_reply" || stage == "tool_dispatch" {
		var existing controlFailure
		if err := readJSON(name, &existing); err == nil && existing.RequestSHA256 == hash(request) {
			return nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	operation := "unknown"
	if invocation, err := DecodeToolInvocation(request); err == nil {
		if arguments, err := ToolArguments(invocation); err == nil {
			if envelope, _, err := toolcontract.DecodeRequestForContract(invocation.Tool, 2, arguments); err == nil {
				operation = envelope.Op
			}
		}
	}
	reason := "the scoped control operation failed"
	switch {
	case errors.Is(cause, context.Canceled):
		reason = "the scoped control operation was cancelled"
	case errors.Is(cause, context.DeadlineExceeded):
		reason = "the scoped control operation exceeded its deadline"
	case errors.Is(cause, os.ErrNotExist):
		reason = "required control evidence is missing"
	case errors.Is(cause, os.ErrPermission):
		reason = "control evidence is not accessible"
	case errors.Is(cause, controlrecord.ErrRecord):
		reason = "the control record is invalid"
	case errors.Is(cause, effects.ErrEvidence):
		reason = "control evidence does not match the recorded request"
	case errors.Is(cause, effects.ErrAuthority), errors.Is(cause, ErrAttemptStale):
		reason = "control authority is no longer valid"
	case errors.Is(cause, ErrAttemptConflict):
		reason = "the Codex attempt binding conflicts"
	case errors.Is(cause, ErrObservationUnknown):
		reason = "the Codex session observation is unknown"
	case stage == "control_process":
		reason = "the scoped control process failed"
	case stage == "control_reply":
		reason = "the scoped control reply is invalid"
	case stage == "tool_scope":
		reason = "the model requested an unscoped tool"
	}
	classification := ""
	if reason == "the scoped control operation failed" {
		classification = controlFailureCause(cause)
	}
	failure := controlFailure{SubReason: helper.ControlAdmissionSubReason(cause), Cause: classification, Code: ControlFailureCode(cause), Schema: 1, At: time.Now().UTC(), RequestSHA256: hash(request), Operation: operation, Stage: stage, Reason: reason}
	return errors.Join(writeJSON(name, failure), appendAttemptError(brain, attempt, failure), recordControlFailureOriginal(brain, attempt, request, stage, failure.Code, failure.SubReason))
}

// Names identify failures without retaining error text, paths or subprocess output.
func controlFailureCause(cause error) string {
	for _, known := range []struct {
		err  error
		name string
	}{
		{helper.ErrOutputPath, "helper.ErrOutputPath"},
		{helper.ErrConflict, "helper.ErrConflict"},
		{helper.ErrNotFound, "helper.ErrNotFound"},
		{helper.ErrAuthority, "helper.ErrAuthority"},
		{helper.ErrControlAdmissionClosed, "helper.ErrControlAdmissionClosed"},
		{helper.ErrControlAlreadyAdmitted, "helper.ErrControlAlreadyAdmitted"},
		{helper.ErrControlFenceUnstopped, "helper.ErrControlFenceUnstopped"},
		{effects.ErrConflict, "effects.ErrConflict"},
		{effects.ErrNotFound, "effects.ErrNotFound"},
		{effects.ErrBusy, "effects.ErrBusy"},
		{effects.ErrStale, "effects.ErrStale"},
		{effects.ErrUnresolved, "effects.ErrUnresolved"},
		{effects.ErrCorrupt, "effects.ErrCorrupt"},
	} {
		if errors.Is(cause, known.err) {
			return known.name
		}
	}
	for range 32 {
		next := errors.Unwrap(cause)
		if next == nil {
			break
		}
		cause = next
	}
	return reflect.TypeOf(cause).String()
}

func recordControlFailureOriginal(brain string, attempt AttemptRef, request []byte, stage, code, subReason string) error {
	invocation, err := DecodeToolInvocation(request)
	if err != nil || invocation.Tool != toolcontract.FactoryControl {
		return nil
	}
	payload, err := ToolArguments(invocation)
	if err != nil {
		return nil
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}, ThreadID: invocation.ThreadID, TurnID: invocation.TurnID, CallID: invocation.CallID}
	expected, err := controlrecord.NewOriginal(invocation.Tool, payload, &id, nil)
	if err != nil {
		return nil
	}
	ref, original, err := controlrecord.FindControlOriginal(brain, id)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, original) {
		return effects.ErrEvidence
	}
	return controlrecord.StoreFailure(brain, ref, stage, code, subReason)
}
