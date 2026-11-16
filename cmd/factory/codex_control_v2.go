package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func coordinatorControlV2(ctx context.Context, brain string, attempt codex.AttemptRef, data []byte) (any, error) {
	invocation, err := codex.DecodeToolInvocation(data)
	if err != nil {
		return nil, err
	}
	if invocation.Tool != toolcontract.FactoryControl {
		return nil, codex.ErrObservationUnknown
	}
	spec, err := codex.ValidateAttemptControlIngress(ctx, brain, attempt, invocation.ThreadID, invocation.TurnID)
	if err != nil {
		return nil, err
	}
	if err := reconcile.CoordinatorAuthority(brain, spec); err != nil {
		return nil, err
	}
	payload, err := codex.ToolArguments(invocation)
	if err != nil {
		return nil, err
	}
	envelope, input, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryControl, 2, payload)
	if err != nil {
		return correctedControlReply(err)
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}, ThreadID: invocation.ThreadID, TurnID: invocation.TurnID, CallID: invocation.CallID}
	original, err := controlrecord.NewOriginal(invocation.Tool, payload, &id, nil)
	if err != nil {
		return nil, err
	}
	writer, err := (reconcile.NativeAuthority{Brain: brain}).CaptureControlOutputWriter()
	if err != nil {
		return nil, err
	}
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	programme, err := reconcile.NewProgramEffects(brain, binary)
	if err != nil {
		return nil, err
	}
	release, err := controlLock(ctx, brain)
	if err != nil {
		return nil, err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	if err := recheckControlV2(ctx, brain, id, spec); err != nil {
		return nil, err
	}
	originalRef, err := controlrecord.StoreOriginal(brain, original)
	if err != nil {
		return nil, err
	}
	ownerAuthority, err := reconcile.ResolveOwnerAuthority(ctx, brain, spec.EpicID, spec.Task)
	if err != nil {
		return nil, err
	}
	service := helperService(brain)
	if _, record, err := controlrecord.FindOutcome(brain, originalRef); err == nil {
		if record.Input.Operation != original.Operation || record.Input.PayloadSHA256 != original.PayloadSHA256 {
			return nil, effects.ErrEvidence
		}
		if _, err := programme.Service().CompleteControl(ctx, id, record.Outcome); err != nil {
			return nil, err
		}
		return json.RawMessage(record.Reply), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var control effects.ControlReceiptRef
	var controlInput effects.ControlInput
	var witness controlrecord.ImmutableRef
	switch envelope.Op {
	case "helper_submit", "helper_continue", "helper_cancel":
		source, err := reconcile.BuildControlIngressLocked(ctx, brain, originalRef, writer)
		if err != nil {
			return nil, err
		}
		witness, err = service.CaptureControlAdmission(ctx, originalRef, source)
		if err != nil {
			return nil, err
		}
		controlInput = source.Input
	case "event", "heartbeat", "inbox":
		if envelope.Op == "inbox" && localControlV2(envelope.Op, input) {
			controlInput, err = localControlInputV2(ctx, brain, spec, original, input)
			if err != nil {
				return nil, err
			}
			break
		}
		witness, err = reconcile.CaptureControlEventSourceLocked(ctx, brain, originalRef, writer)
		if err != nil {
			return correctedControlReply(err)
		}
		source, err := reconcile.ReadControlEventSourceLocal(ctx, brain, witness)
		if err != nil {
			return nil, err
		}
		controlInput = source.Input
	case "file_write":
		var source helper.ControlIngressSource
		witness, source, err = reconcile.CaptureControlOutputIngressLocked(ctx, brain, originalRef, writer)
		if err != nil {
			return nil, err
		}
		controlInput = source.Input
	default:
		if toolcontract.IsReadOperationForContract(invocation.Tool, envelope.Op, 2) {
			witness, err = reconcile.CaptureCoordinatorReadSourceLocked(ctx, brain, originalRef, reconcile.ControlReadDirect, writer)
			if err != nil {
				return nil, err
			}
			pre, err := reconcile.ReadControlReadPreSourceLocal(ctx, brain, witness)
			if err != nil {
				return nil, err
			}
			if pre.Coordinator == nil {
				return nil, effects.ErrEvidence
			}
			controlInput = pre.Coordinator.Ingress.Input
		} else if !localControlV2(envelope.Op, input) {
			return nil, fmt.Errorf("%w: concrete %s control integration is required", reconcile.ErrCodexIntegration, envelope.Op)
		} else {
			controlInput, err = localControlInputV2(ctx, brain, spec, original, input)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := recheckControlV2(ctx, brain, id, spec); err != nil {
		return nil, err
	}
	if err := service.CheckWorkControlAdmission(ctx, controlInput.Target.WorkID); err != nil {
		return nil, err
	}
	var localProof controlrecord.ImmutableRef
	var localSnapshot controlrecord.ReplySnapshot
	if localControlV2(envelope.Op, input) {
		localProof, localSnapshot, err = controlrecord.FindReplySnapshot(brain, originalRef)
		if errors.Is(err, os.ErrNotExist) {
			value, readErr := readLocalControlV2(ctx, brain, spec, programme, envelope, input)
			if readErr != nil {
				value, readErr = correctedControlReply(readErr)
				if readErr != nil {
					return nil, readErr
				}
			}
			reply, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			if len(reply) > toolcontract.ControlLimit {
				return nil, fmt.Errorf("coordinator result exceeds transport limit")
			}
			localSnapshot = controlrecord.ReplySnapshot{Schema: 2, Original: originalRef, Input: controlInput, Reply: reply, ResponseSHA256: sha256.Sum256(reply)}
			localProof, err = controlrecord.StoreReplySnapshot(brain, localSnapshot)
		}
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(localSnapshot.Input, controlInput) {
			return nil, effects.ErrEvidence
		}
	}
	if err := recheckControlV2(ctx, brain, id, spec); err != nil {
		return nil, err
	}
	if err := service.CheckWorkControlAdmission(ctx, controlInput.Target.WorkID); err != nil {
		return nil, err
	}
	if err := reconcile.ValidateAdmittedOwnerAuthority(ctx, brain, ownerAuthority); err != nil {
		return nil, err
	}
	control, err = programme.Service().EnrollControl(ctx, id, controlInput)
	if err != nil {
		return nil, err
	}
	if _, record, err := controlrecord.FindOutcome(brain, originalRef); err == nil {
		if !reflect.DeepEqual(record.Input, controlInput) {
			return nil, effects.ErrEvidence
		}
		if _, err := programme.Service().CompleteControl(ctx, id, record.Outcome); err != nil {
			return nil, err
		}
		return json.RawMessage(record.Reply), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var proof controlrecord.ImmutableRef
	var outcome effects.ControlOutcome
	var reply json.RawMessage
	if toolcontract.IsReadOperationForContract(invocation.Tool, envelope.Op, 2) {
		admission, err := reconcile.BindCoordinatorReadSourceLocked(ctx, brain, witness, control)
		if err != nil {
			return nil, err
		}
		read, err := reconcile.PrepareProgramReadLocked(ctx, brain, admission)
		if err != nil {
			return nil, err
		}
		release()
		release = nil
		result, readErr := reconcile.RunProgramRead(ctx, brain, binary, read)
		release, err = controlLock(ctx, brain)
		if err != nil {
			return nil, err
		}
		retained, err := reconcile.FindControlReadAdmissionLocal(ctx, brain, originalRef, reconcile.ControlReadDirect)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(retained, admission) {
			return nil, effects.ErrEvidence
		}
		var noAction *reconcile.ProgramReadNoActionError
		var unavailable *gh.ProgramReadUnavailable
		if errors.As(readErr, &noAction) {
			proof, reply, err = codex.StoreControlReadNoActionProof(ctx, brain, originalRef, noAction.Reference)
		} else if readErr == nil || errors.As(readErr, &unavailable) && result != (codex.ProgramReadResultRef{}) {
			proof, reply, err = codex.StoreControlReadProof(ctx, brain, originalRef, result)
		} else {
			return nil, readErr
		}
		if err != nil {
			return nil, err
		}
		outcome = effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: original.Operation, RequestSHA256: original.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
	} else if envelope.Op == "file_write" {
		request, err := reconcile.BuildControlOutputLocked(ctx, brain, originalRef, &control)
		if err != nil {
			return nil, err
		}
		if request.Writer != writer {
			return nil, effects.ErrEvidence
		}
		prepared, err := service.PrepareControlOutputLocked(ctx, request)
		if err != nil {
			return nil, err
		}
		proof, err = service.CommitControlOutputLocked(ctx, prepared)
		if err != nil {
			return nil, err
		}
		final, _, err := service.ReadControlOutput(ctx, prepared)
		if err != nil {
			return nil, err
		}
		reply, err = json.Marshal(map[string]string{"path": final.Path, "filename": final.Caller.Coordinator.OutputName})
		if err != nil {
			return nil, err
		}
		outcome = effects.ControlOutcome{Kind: effects.ControlFile, File: &effects.FileReceipt{Path: final.Path, SHA256: final.FinalSHA256, PublicationSHA256: proof.SHA256}}

	} else if envelope.Op == "event" || envelope.Op == "heartbeat" || envelope.Op == "inbox" && !localControlV2(envelope.Op, input) {
		enrollment, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: originalRef, Source: witness, Receipt: control, Input: controlInput})
		if err != nil {
			return nil, err
		}
		result, err := reconcile.ApplyControlEventLocked(ctx, brain, witness, enrollment, writer)
		if err != nil {
			return nil, err
		}
		proof, outcome, reply = result.Proof, result.Outcome, result.Reply
	} else if envelope.Op == "helper_submit" || envelope.Op == "helper_continue" || envelope.Op == "helper_cancel" {
		enrollment, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: originalRef, Source: witness, Receipt: control, Input: controlInput})
		if err != nil {
			return nil, err
		}
		switch envelope.Op {
		case "helper_submit":
			_, err = service.SubmitControl(ctx, witness, enrollment)
		case "helper_continue":
			_, err = service.ContinueControl(ctx, witness, enrollment)
		case "helper_cancel":
			_, _, err = service.AdmitControlCancellation(ctx, witness)
		}
		if err != nil {
			return nil, err
		}
		var admission helper.ControlAdmissionReceipt
		proof, admission, err = service.LookupControlAdmission(ctx, witness)
		if err != nil {
			return nil, err
		}
		reply, err = codex.ControlAdmissionReply(admission)
		if err != nil {
			return nil, err
		}
		outcome = effects.ControlOutcome{Kind: effects.ControlBroker, Broker: &effects.BrokerAdmission{JobID: admission.Accepted.JobID, Round: admission.Accepted.Round, RequestSHA256: admission.RequestSHA256, ScopeSHA256: admission.ScopeSHA256}}
	} else {
		proof = localProof
		reply = localSnapshot.Reply
		outcome = effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: original.Operation, RequestSHA256: original.PayloadSHA256, ResponseSHA256: localSnapshot.ResponseSHA256}}

	}
	record := controlrecord.OutcomeRecord{Schema: 2, Original: originalRef, Input: controlInput, Outcome: outcome, Reply: reply, ReplySHA256: sha256.Sum256(reply), Proof: proof}
	if _, err := controlrecord.StoreOutcome(brain, record); err != nil {
		return nil, err
	}
	if _, err := programme.Service().CompleteControl(ctx, id, outcome); err != nil {
		return nil, err
	}
	return json.RawMessage(reply), nil
}
func recheckControlV2(ctx context.Context, brain string, id effects.ControlID, spec session.Spec) error {
	current, err := codex.ReadAttemptControlLocal(ctx, brain, id)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, spec) {
		return effects.ErrAuthority
	}
	return reconcile.CoordinatorAuthority(brain, current)
}
func localControlV2(op string, input toolcontract.Input) bool {
	switch op {
	case "brain_read", "plan_input", "epic_file", "events", "lease_check", "file_read", "query", "helper_result", "effect_result":
		return true
	case "inbox":
		p, ok := input.(*toolcontract.ControlInput)
		return ok && p.Ack == 0
	}
	return false
}
func localControlInputV2(ctx context.Context, brain string, spec session.Spec, original controlrecord.OriginalRequest, input toolcontract.Input) (effects.ControlInput, error) {
	result := effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if p, ok := input.(*toolcontract.ResultInput); ok {
		job, err := (helper.Service{Brain: brain}).Job(p.JobID)
		if err != nil {
			return result, err
		}
		if job.Scope.EpicID != spec.EpicID {
			return result, effects.ErrAuthority
		}
		workID := job.Scope.WorkID
		if job.Scope.OwnerID != spec.Task {
			ix, err := store.ReadIndex(brain)
			if err != nil {
				return result, err
			}
			owner, err := ix.Lookup(spec.Task)
			if err != nil || owner.Kind != blueprint.MachineEpic || spec.Task != spec.EpicID || spec.Name != "factory:"+spec.EpicID+":planner" {
				return result, effects.ErrAuthority
			}
			workID = ""
		}
		result.Target = effects.ControlTarget{WorkID: workID, JobID: job.ID, Round: p.Round}
		return result, nil
	}
	if p, ok := input.(*toolcontract.IssueInput); ok && original.Operation == "query" {
		ix, err := store.ReadIndex(brain)
		if err != nil {
			return result, err
		}
		if _, _, err := coordinatorRepo(brain, ix, spec, p.Repo, p.Base); err != nil {
			return result, err
		}
	}
	if p, ok := input.(*toolcontract.ControlInput); ok {
		ix, err := store.ReadIndex(brain)
		if err != nil {
			return result, err
		}
		targetID := p.Target
		if targetID == "" {
			targetID = spec.Task
		}
		target, err := reconcile.ResolveCoordinatorTarget(ix, spec, targetID)
		if err != nil {
			return result, err
		}
		if target.Kind == blueprint.MachineWork {
			result.Target.WorkID = targetID
		}
		if original.Operation == "file_read" {
			result.Target.Path, err = controlrecord.ResolveCoordinatorOutput(brain, target, spec, p.Path, false)
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}
func correctedControlReply(err error) (any, error) {
	var input *toolcontract.ToolInputError
	if errors.As(err, &input) {
		return toolcontract.ToolFailure{ToolError: input.Message}, nil
	}
	return nil, err
}

func readLocalControlV2(ctx context.Context, brain string, spec session.Spec, programme reconcile.ProgramEffects, envelope toolcontract.Envelope, input toolcontract.Input) (any, error) {
	if request, ok := input.(*toolcontract.EffectResultInput); ok {
		receipt, err := programme.Service().Result(ctx, request.SlotID)
		if err != nil {
			return nil, err
		}
		if receipt.Prepared.Slot.OwnerID != spec.Task || receipt.Prepared.Slot.EpicID != spec.EpicID {
			return nil, effects.ErrAuthority
		}
		return receipt, nil
	}
	return scopedCoordinatorControl(ctx, brain, spec, controlRequest{Op: envelope.Op, Payload: envelope.Payload})
}

func helperControlV2(ctx context.Context, brain, id string, generation, round int, data []byte) (any, error) {
	invocation, err := codex.DecodeToolInvocation(data)
	if err != nil {
		return nil, err
	}
	if invocation.Tool != toolcontract.FactoryHelper && invocation.Tool != toolcontract.ManagedFiles {
		return nil, codex.ErrObservationUnknown
	}
	call := controlrecord.HelperCall{JobID: id, Generation: generation, Round: round, ThreadID: invocation.ThreadID, TurnID: invocation.TurnID, CallID: invocation.CallID}
	job, err := codex.ValidateHelperControlIngress(ctx, brain, call)
	if err != nil {
		return nil, err
	}
	payload, err := codex.ToolArguments(invocation)
	if err != nil {
		return nil, err
	}
	envelope, input, err := toolcontract.DecodeRequestForContract(invocation.Tool, 2, payload)
	if err != nil {
		return correctedControlReply(err)
	}
	original, err := controlrecord.NewOriginal(invocation.Tool, payload, nil, &call)
	if err != nil {
		return nil, err
	}
	writer, err := (reconcile.NativeAuthority{Brain: brain}).CaptureControlOutputWriter()
	if err != nil {
		return nil, err
	}
	release, err := controlLock(ctx, brain)
	if err != nil {
		return nil, err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	service := helperService(brain)
	if err := recheckHelperV2(ctx, brain, service, job, call); err != nil {
		return nil, err
	}
	originalRef, err := controlrecord.StoreOriginal(brain, original)
	if err != nil {
		return nil, err
	}
	if invocation.Tool == toolcontract.FactoryHelper && (envelope.Op == "submit" || envelope.Op == "continue" || envelope.Op == "yield") {
		witness, err := service.CaptureManagedControl(ctx, originalRef)
		if err != nil {
			return correctedControlReply(codexHelperInputError(err))
		}
		_, receipt, err := service.LookupManagedControl(ctx, witness)
		if errors.Is(err, helper.ErrNotFound) {
			switch envelope.Op {
			case "submit":
				_, err = service.SubmitManagedControl(ctx, witness)
			case "continue":
				_, err = service.ContinueManagedControl(ctx, witness)
			case "yield":
				err = service.YieldManagedControl(ctx, witness)
			}
			if err != nil {
				return correctedControlReply(codexHelperInputError(err))
			}
			_, receipt, err = service.LookupManagedControl(ctx, witness)
		}
		if err != nil {
			return nil, err
		}
		reply, err := codex.ManagedControlReply(receipt)
		return json.RawMessage(reply), err
	}
	if toolcontract.IsReadOperationForContract(invocation.Tool, envelope.Op, 2) {
		if _, reply, err := codex.FindControlReadProof(ctx, brain, originalRef); err == nil {
			return json.RawMessage(reply), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		pre, err := reconcile.CaptureHelperReadSourceLocked(ctx, brain, originalRef, reconcile.ControlReadDirect)
		if err != nil {
			return nil, err
		}
		admission, err := reconcile.BindHelperReadSourceLocked(ctx, brain, pre)
		if err != nil {
			return nil, err
		}
		request, err := reconcile.PrepareProgramReadLocked(ctx, brain, admission)
		if err != nil {
			return nil, err
		}
		binary, err := os.Executable()
		if err != nil {
			return nil, err
		}
		release()
		release = nil
		result, readErr := reconcile.RunProgramRead(ctx, brain, binary, request)
		release, err = controlLock(ctx, brain)
		if err != nil {
			return nil, err
		}
		retained, err := reconcile.FindControlReadAdmissionLocal(ctx, brain, originalRef, reconcile.ControlReadDirect)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(retained, admission) {
			return nil, effects.ErrEvidence
		}
		var reply json.RawMessage
		var noAction *reconcile.ProgramReadNoActionError
		var unavailable *gh.ProgramReadUnavailable
		if errors.As(readErr, &noAction) {
			_, reply, err = codex.StoreControlReadNoActionProof(ctx, brain, originalRef, noAction.Reference)
		} else if readErr == nil || errors.As(readErr, &unavailable) && result != (codex.ProgramReadResultRef{}) {
			_, reply, err = codex.StoreControlReadProof(ctx, brain, originalRef, result)
		} else {
			return nil, readErr
		}
		return json.RawMessage(reply), err
	}
	if invocation.Tool == toolcontract.ManagedFiles && envelope.Op != "read" {
		ref, err := service.FindControlOutput(ctx, originalRef)
		if err == nil {
			prepared, committed, err := service.ReadControlOutput(ctx, ref)
			if err != nil {
				return nil, err
			}
			if committed == nil {
				return nil, &helper.ControlOutputPendingError{Path: prepared.Path, Output: ref}
			}
			if prepared.Original != originalRef || prepared.Caller.Helper == nil || prepared.Caller.Helper.JobID != job.ID {
				return nil, effects.ErrEvidence
			}
			return map[string]string{"path": prepared.Path}, nil
		}
		if !errors.Is(err, helper.ErrNotFound) {
			return nil, err
		}
		request, err := reconcile.BuildControlOutputLocked(ctx, brain, originalRef, nil)
		if err != nil {
			return correctedControlReply(err)
		}
		request.Writer = writer
		ref, err = service.PrepareControlOutputLocked(ctx, request)
		if err != nil {
			return correctedControlReply(err)
		}
		if _, err := service.CommitControlOutputLocked(ctx, ref); err != nil {
			return nil, err
		}
		return map[string]string{"path": request.Path}, nil
	}
	controlInput := effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: job.Scope.WorkID, JobID: job.ID, Round: job.Round}}
	if invocation.Tool == toolcontract.ManagedFiles {
		file, ok := input.(*toolcontract.FileInput)
		if !ok || envelope.Op != "read" {
			return nil, effects.ErrEvidence
		}
		controlInput.Target.Path, err = helperArtifactPath(ctx, brain, job, file.Path)
		if err != nil {
			return correctedControlReply(err)
		}
	} else {
		switch envelope.Op {
		case "brain_read", "inbox", "lease_check", "result":
		default:
			return nil, fmt.Errorf("%w: concrete helper %s receipt integration is required", reconcile.ErrCodexIntegration, envelope.Op)
		}
	}
	if _, snapshot, err := controlrecord.FindReplySnapshot(brain, originalRef); err == nil {
		if !reflect.DeepEqual(snapshot.Input, controlInput) {
			return nil, effects.ErrEvidence
		}
		return json.RawMessage(snapshot.Reply), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var value any
	if invocation.Tool == toolcontract.ManagedFiles {
		var content []byte
		content, err = readCoordinatorFile(controlInput.Target.Path, controlLimit)
		if errors.Is(err, os.ErrNotExist) {
			err = &codex.ToolInputError{Message: "task artifact does not exist yet; use an existing declared artifact path"}
		}
		value = map[string]string{"text": string(content)}
	} else {
		value, err = managedControl(ctx, service, id, generation, payload)
	}
	if err != nil {
		value, err = correctedControlReply(err)
		if err != nil {
			return nil, err
		}
	}
	reply, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(reply) > toolcontract.ControlLimit {
		return nil, fmt.Errorf("helper result exceeds transport limit")
	}
	if err := recheckHelperV2(ctx, brain, service, job, call); err != nil {
		return nil, err
	}
	snapshot := controlrecord.ReplySnapshot{Schema: 2, Original: originalRef, Input: controlInput, Reply: reply, ResponseSHA256: sha256.Sum256(reply)}
	if _, err := controlrecord.StoreReplySnapshot(brain, snapshot); err != nil {
		return nil, err
	}
	return json.RawMessage(reply), nil
}

func recheckHelperV2(ctx context.Context, brain string, service helper.Service, expected helper.Job, call controlrecord.HelperCall) error {
	_, err := service.WithManagedAdmission(ctx, call.JobID, call.Generation, func(job helper.Job) error {
		if job.ID != expected.ID || job.Scope != expected.Scope || job.Generation != call.Generation || job.Round != call.Round || job.ConversationID != call.ThreadID || job.TurnID != call.TurnID {
			return effects.ErrAuthority
		}
		return codex.ValidateHelperAttempt(brain, job)
	})
	return err
}
