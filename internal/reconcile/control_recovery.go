package reconcile

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
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type ControlOperationRecovery struct {
	programme ProgramEffects
	helpers   helper.Service
}

func NewControlOperationRecovery(p ProgramEffects) (*ControlOperationRecovery, error) {
	if p.Brain == "" || p.controls == nil || p.attemptGate == nil {
		return nil, ErrCodexIntegration
	}
	service := NativeService(p.Brain, func(ctx context.Context, j helper.Job) error {
		if j.Route.Provider != "codex" || j.Scope.CodexContract != 2 {
			return effects.ErrAuthority
		}
		return codex.NewBackend(p.Brain).Fence(ctx, j)
	})
	return &ControlOperationRecovery{programme: p, helpers: service}, nil
}

func (r *ControlOperationRecovery) RecoverSealedControls(ctx context.Context, key effects.AttemptKey) error {
	if r == nil || r.programme.attemptGate == nil || r.programme.controls == nil {
		return ErrCodexIntegration
	}
	seal, err := r.programme.attemptGate.SealedAttempt(ctx, key)
	if err != nil {
		return err
	}
	pending, err := r.programme.Service().PendingAttemptControls(ctx, key)
	if err != nil {
		return err
	}
	var holds []error
	for _, control := range pending {
		if err := r.recoverControl(ctx, control, &seal, nil); err != nil {
			holds = append(holds, err)
		}
	}
	return errors.Join(holds...)
}

func (r *ControlOperationRecovery) recoverControl(ctx context.Context, control effects.ControlState, seal *effects.AttemptSeal, work *effects.WorkIngressSeal) error {
	originalRef, original, err := controlrecord.FindControlOriginal(r.programme.Brain, control.Receipt.Control)
	if err != nil {
		return err
	}
	if original.Control == nil || *original.Control != control.Receipt.Control || original.Operation != control.Input.Operation || original.PayloadSHA256 != control.Input.PayloadSHA256 {
		return effects.ErrEvidence
	}
	if _, record, err := controlrecord.FindOutcome(r.programme.Brain, originalRef); err == nil {
		return r.completeControl(ctx, control, record)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(control.Actions) > 0 {
		return fmt.Errorf("%w: linked control actions require their actual domain disposition", ErrCodexIntegration)
	}
	if toolcontract.IsReadOperationForContract(original.Tool, original.Operation, 2) {
		return r.recoverReadControl(ctx, control, originalRef, seal, work)
	}
	switch original.Operation {
	case "helper_submit", "helper_continue", "helper_cancel":
		witness, _, err := helper.FindControlAdmissionWitnessLocal(r.programme.Brain, originalRef)
		if err != nil {
			return err
		}
		proof, admission, err := r.helpers.LookupControlAdmission(ctx, witness)
		if err == nil {
			reply, err := codex.ControlAdmissionReply(admission)
			if err != nil {
				return err
			}
			outcome := effects.ControlOutcome{Kind: effects.ControlBroker, Broker: &effects.BrokerAdmission{JobID: admission.Accepted.JobID, Round: admission.Accepted.Round, RequestSHA256: admission.RequestSHA256, ScopeSHA256: admission.ScopeSHA256}}
			return r.storeAndComplete(ctx, control, originalRef, outcome, reply, proof)
		}
		if !errors.Is(err, helper.ErrNotFound) {
			return err
		}
		return r.closeUnadmitted(ctx, control, originalRef, witness, seal, work)
	case "event", "heartbeat", "inbox":
		if original.Operation == "inbox" {
			_, decoded, err := controlrecord.DecodeInput(r.programme.Brain, originalRef)
			if err != nil {
				return err
			}
			p, ok := decoded.(*toolcontract.ControlInput)
			if !ok {
				return effects.ErrEvidence
			}
			if p.Ack < 1 {
				return r.recoverReplyControl(ctx, control, originalRef)
			}
		}
		return r.recoverEventControl(ctx, control, originalRef, seal, work)
	case "file_write":
		ref, err := r.helpers.FindControlOutput(ctx, originalRef)
		if errors.Is(err, helper.ErrNotFound) {
			witness, _, err := helper.FindControlOutputIngressLocal(r.programme.Brain, originalRef)
			if err != nil {
				return err
			}
			return r.closeUnadmitted(ctx, control, originalRef, witness, seal, work)
		}
		if err != nil {
			return err
		}
		prepared, commit, err := r.helpers.ReadControlOutput(ctx, ref)
		if err != nil {
			return err
		}
		if prepared.Original != originalRef || prepared.Caller.Coordinator == nil {
			return effects.ErrEvidence
		}
		if commit == nil {
			if err := (NativeAuthority{r.programme.Brain}).ValidateControlOutputWriterStopped(ctx, prepared.Writer); err != nil {
				return err
			}
		}
		release, err := waitLock(ctx, r.programme.Brain, "tick")
		if err != nil {
			return err
		}
		proof, err := r.helpers.RecoverControlOutputLocked(ctx, ref)
		release()
		if err != nil {
			return err
		}
		reply, err := json.Marshal(map[string]string{"path": prepared.Path, "filename": prepared.Caller.Coordinator.OutputName})
		if err != nil {
			return err
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlFile, File: &effects.FileReceipt{Path: prepared.Path, SHA256: prepared.FinalSHA256, PublicationSHA256: proof.SHA256}}
		return r.storeAndComplete(ctx, control, originalRef, outcome, reply, proof)
	default:
		return r.recoverReplyControl(ctx, control, originalRef)
	}
}

func (r *ControlOperationRecovery) recoverReadControl(ctx context.Context, control effects.ControlState, original controlrecord.OriginalRequestRef, seal *effects.AttemptSeal, work *effects.WorkIngressSeal) error {
	brain := r.programme.Brain
	if proof, reply, err := codex.FindControlReadProof(ctx, brain, original); err == nil {
		outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: control.Input.Operation, RequestSHA256: control.Input.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
		return r.storeAndComplete(ctx, control, original, outcome, reply, proof)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	admission, err := FindControlReadAdmissionLocal(ctx, brain, original, ControlReadDirect)
	if errors.Is(err, os.ErrNotExist) {
		pre, _, findErr := FindControlReadPreSourceLocal(ctx, brain, original, ControlReadDirect)
		if findErr != nil {
			if errors.Is(findErr, os.ErrNotExist) {
				return fmt.Errorf("%w: enrolled control requires its retained read pre-input source: %v", ErrCodexIntegration, findErr)
			}
			return findErr
		}
		actual, readErr := r.programme.Service().ReadControlReference(ctx, control.Receipt)
		if readErr != nil {
			return readErr
		}
		if actual != control.Input {
			return effects.ErrEvidence
		}
		enrollment, storeErr := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: pre, Receipt: control.Receipt, Input: actual})
		if storeErr != nil {
			return storeErr
		}
		admission, err = ReadCoordinatorReadPreAdmissionLocal(ctx, brain, pre, enrollment)
	}
	if err != nil {
		return err
	}
	if admission.Control == nil || admission.Control.Receipt != control.Receipt || admission.Control.Input != control.Input {
		return effects.ErrEvidence
	}
	request, err := LookupProgramReadForAdmission(ctx, brain, admission)
	if errors.Is(err, os.ErrNotExist) {
		pre, _, err := FindControlReadPreSourceLocal(ctx, brain, original, ControlReadDirect)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: enrolled control requires its retained read pre-input source: %v", ErrCodexIntegration, err)
			}
			return err
		}
		actual, err := r.programme.Service().ReadControlReference(ctx, control.Receipt)
		if err != nil {
			return err
		}
		if actual != control.Input {
			return effects.ErrEvidence
		}
		enrollment, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: pre, Receipt: control.Receipt, Input: actual})
		if err != nil {
			return err
		}
		release, err := waitLock(ctx, brain, "tick")
		if err != nil {
			return err
		}
		absent, err := CloseProgramReadInputAbsentLocked(ctx, brain, ProgramReadInputAbsenceRequest{PreSource: pre, Enrollment: &enrollment, Closure: ReadIngressClosure{Original: original, AttemptSeal: seal, WorkSeal: work}})
		release()
		if err != nil {
			return err
		}
		proof, reply, err := codex.StoreControlReadInputAbsentProof(ctx, brain, original, absent)
		if err != nil {
			return err
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: control.Input.Operation, RequestSHA256: control.Input.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
		return r.storeAndComplete(ctx, control, original, outcome, reply, proof)
	}
	if err != nil {
		return err
	}
	result, readErr := ReadProgramReadResult(ctx, brain, request)
	var noAction *ProgramReadNoActionError
	var unavailable *gh.ProgramReadUnavailable
	var proof controlrecord.ImmutableRef
	var reply json.RawMessage
	if errors.As(readErr, &noAction) {
		proof, reply, err = codex.StoreControlReadNoActionProof(ctx, brain, original, noAction.Reference)
	} else if readErr == nil || errors.As(readErr, &unavailable) && result != (codex.ProgramReadResultRef{}) {
		proof, reply, err = codex.StoreControlReadProof(ctx, brain, original, result)
	} else {
		var held *ProgramReadHeld
		if !errors.As(readErr, &held) {
			return readErr
		}
		release, lockErr := waitLock(ctx, brain, "tick")
		if lockErr != nil {
			return lockErr
		}
		ref, closeErr := CloseUnselectedProgramReadLocked(ctx, brain, request, ReadIngressClosure{Original: original, AttemptSeal: seal, WorkSeal: work})
		release()
		if closeErr != nil {
			return closeErr
		}
		proof, reply, err = codex.StoreControlReadNoActionProof(ctx, brain, original, ref)
	}
	if err != nil {
		return err
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: control.Input.Operation, RequestSHA256: control.Input.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
	return r.storeAndComplete(ctx, control, original, outcome, reply, proof)
}

func (r *ControlOperationRecovery) closeUnadmitted(ctx context.Context, control effects.ControlState, original controlrecord.OriginalRequestRef, witness controlrecord.ImmutableRef, seal *effects.AttemptSeal, work *effects.WorkIngressSeal) error {
	// E1 is observed outside the helper lock; its immutable snapshot carries exact enrollment identity.
	actual, err := r.programme.Service().ReadControlReference(ctx, control.Receipt)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, control.Input) {
		return effects.ErrEvidence
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(r.programme.Brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: witness, Receipt: control.Receipt, Input: actual})
	if err != nil {
		return err
	}
	request := helper.UnadmittedControlRequest{Original: original, Witness: witness, Enrollment: enrollment, AttemptSeal: seal, WorkSeal: work}
	release, err := waitLock(ctx, r.programme.Brain, "tick")
	if err != nil {
		return err
	}
	proof, err := r.helpers.CloseUnadmittedControlLocked(ctx, request)
	release()
	if err != nil {
		return err
	}
	reply, err := codex.UnadmittedControlReply()
	if err != nil {
		return err
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRejected, Rejection: &effects.GuardReceipt{Operation: control.Input.Operation, Code: helper.ControlNotAdmittedCode, EvidenceSHA256: proof.SHA256}}
	return r.storeAndComplete(ctx, control, original, outcome, reply, proof)
}
func (r *ControlOperationRecovery) storeAndComplete(ctx context.Context, control effects.ControlState, original controlrecord.OriginalRequestRef, outcome effects.ControlOutcome, reply json.RawMessage, proof controlrecord.ImmutableRef) error {
	record := controlrecord.OutcomeRecord{Schema: 2, Original: original, Input: control.Input, Outcome: outcome, Reply: reply, ReplySHA256: sha256.Sum256(reply), Proof: proof}
	if _, err := controlrecord.StoreOutcome(r.programme.Brain, record); err != nil {
		return err
	}
	return r.completeControl(ctx, control, record)
}
func (r *ControlOperationRecovery) completeControl(ctx context.Context, control effects.ControlState, record controlrecord.OutcomeRecord) error {
	if !reflect.DeepEqual(record.Input, control.Input) {
		return effects.ErrEvidence
	}
	release, err := waitLock(ctx, r.programme.Brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	_, err = r.programme.Service().CompleteControl(ctx, control.Receipt.Control, record.Outcome)
	return err
}

var _ codex.SealedControlRecovery = (*ControlOperationRecovery)(nil)

func (r *ControlOperationRecovery) RecoverWorkControls(ctx context.Context, scope helper.Scope, oldOwner, oldWork OwnerAuthority) error {
	return r.recoverWorkControlsUsing(ctx, scope, oldOwner, oldWork, codex.Contract2AcceptanceAuthority)
}
func (r *ControlOperationRecovery) recoverWorkControlsUsing(ctx context.Context, scope helper.Scope, oldOwner, oldWork OwnerAuthority, verify nativeAcceptance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || scope.CodexContract != 2 || scope.WorkID == "" {
		return effects.ErrAuthority
	}
	initial, err := r.programme.Service().PendingWorkControls(ctx, scope.WorkID)
	if err != nil {
		return err
	}
	var remaining []effects.ControlState
	var holds []error
	for _, control := range initial {
		found, err := r.settlePositiveControl(ctx, control, verify)
		if err != nil {
			holds = append(holds, err)
			continue
		}
		if !found {
			remaining = append(remaining, control)
		}
	}

	release, err := waitLock(ctx, r.programme.Brain, "tick")
	if err != nil {
		return err
	}
	err = validateWorkControlAuthorities(ctx, r.programme.Brain, scope, oldOwner, oldWork, verify)
	release()
	if err != nil {
		return err
	}
	closed, err := r.helpers.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil {
		token, outputs, found, observeErr := r.helpers.PendingControlOutputFence(ctx, scope)
		if errors.Is(observeErr, helper.ErrControlFenceUnstopped) && found {
			if err := r.helpers.ResumeControlOutputFence(ctx, scope, token, "control_transfer"); err != nil {
				return err
			}
			token, outputs, found, observeErr = r.helpers.PendingControlOutputFence(ctx, scope)
		}
		if observeErr != nil {
			return observeErr
		}
		if !found {
			return effects.ErrAuthority
		}
		if token != "" {
			for _, ref := range outputs {
				prepared, commit, err := r.helpers.ReadControlOutput(ctx, ref)
				if err != nil {
					return err
				}
				if commit != nil {
					_, _, proof, err := r.helpers.ReadControlOutputCommitLocal(ctx, ref)
					if err != nil {
						return err
					}
					release, err := waitLock(ctx, r.programme.Brain, "tick")
					if err != nil {
						return err
					}
					err = r.helpers.AdoptControlOutputCommitLocked(ctx, ref, proof)
					release()
					if err != nil {
						return err
					}
					continue
				}
				if caller := prepared.Caller.Coordinator; caller != nil {
					if caller.WorkScope == nil || caller.WorkAuthority == nil {
						return fmt.Errorf("%w: pending coordinator output lacks captured work scope", ErrCodexIntegration)
					}
					if caller.Authority != oldOwner || *caller.WorkAuthority != oldWork || caller.WorkScope.Authorization() != scope.Authorization() {
						return effects.ErrEvidence
					}
					if err := (NativeAuthority{r.programme.Brain}).validateCoordinatorOutput(ctx, *caller, prepared.Original, prepared.Path, true, verify); err != nil {
						return err
					}
				} else {
					saved, err := readHelperOutputSnapshotLocal(r.programme.Brain, prepared)
					if err != nil {
						return err
					}
					if saved.Authority != oldOwner || saved.WorkAuthority != oldWork || saved.Job.Scope.Authorization() != scope.Authorization() {
						return effects.ErrEvidence
					}
				}
				if err := (NativeAuthority{r.programme.Brain}).ValidateControlOutputWriterStopped(ctx, prepared.Writer); err != nil {
					return err
				}

				release, err := waitLock(ctx, r.programme.Brain, "tick")
				if err != nil {
					return err
				}
				err = validateWorkControlAuthorities(ctx, r.programme.Brain, scope, oldOwner, oldWork, verify)
				if err == nil {
					_, err = r.helpers.RecoverControlOutputForFenceLocked(ctx, ref, scope.WorkID, token)
				}
				release()
				if err != nil {
					return err
				}
			}
			release, err := waitLock(ctx, r.programme.Brain, "tick")
			if err != nil {
				return err
			}
			err = validateWorkControlAuthorities(ctx, r.programme.Brain, scope, oldOwner, oldWork, verify)
			if err == nil {
				closed, err = r.helpers.FinishControlOutputFenceLocked(ctx, scope.WorkID, token)
			}
			release()
			if err != nil {
				return err
			}
		}
	}
	actual, err := helper.ReadClosedScopeFenceLocal(ctx, r.programme.Brain, closed)
	if err != nil {
		return err
	}
	closedFenceToken := actual.Token
	matches := false
	for _, stored := range actual.Scopes {
		if stored.Authorization() == scope.Authorization() {
			matches = true
		}
	}
	if !matches || actual.WorkID != scope.WorkID {
		return effects.ErrAuthority
	}
	seal := effects.WorkIngressSeal{WorkID: scope.WorkID, OwnerID: oldOwner.OwnerID, OwnerRevision: oldOwner.Revision, Fence: effects.Artifact{Path: closed.Path, SHA256: closed.SHA256}}
	for _, control := range remaining {
		found, err := r.settlePositiveControl(ctx, control, verify)
		if err != nil {
			holds = append(holds, err)
			continue
		}
		if found {
			continue
		}
		release, err := waitLock(ctx, r.programme.Brain, "tick")
		if err != nil {
			return err
		}
		err = validateWorkControlAuthorities(ctx, r.programme.Brain, scope, oldOwner, oldWork, verify)
		if err == nil {
			err = r.validateWorkControlSource(ctx, control, scope, oldOwner, oldWork, verify)
		}
		if err == nil {
			current, readErr := r.helpers.ReadClosedScopeFence(ctx, scope.WorkID)
			if readErr != nil {
				err = readErr
			} else {
				actual, readErr := helper.ReadClosedScopeFenceLocal(ctx, r.programme.Brain, current)
				if readErr != nil {
					err = readErr
				} else if actual.Token != closedFenceToken || actual.WorkID != scope.WorkID || len(actual.Scopes) != 1 {
					err = effects.ErrAuthority
				} else {
					for _, currentScope := range actual.Scopes {
						if currentScope.Authorization() != scope.Authorization() {
							err = effects.ErrAuthority
						}
					}
				}
			}
		}
		release()
		if err != nil {
			holds = append(holds, err)
			continue
		}
		if control.Input.Operation == "event" || control.Input.Operation == "heartbeat" || control.Input.Operation == "inbox" {
			original, _, findErr := controlrecord.FindControlOriginal(r.programme.Brain, control.Receipt.Control)
			if findErr != nil {
				holds = append(holds, findErr)
				continue
			}
			err = r.recoverEventControlUsing(ctx, control, original, nil, &seal, verify)
		} else {
			err = r.recoverControl(ctx, control, nil, &seal)
		}
		if err != nil {
			holds = append(holds, err)
		}
	}

	return errors.Join(holds...)
}

func (r *ControlOperationRecovery) recoverEventControl(ctx context.Context, control effects.ControlState, original controlrecord.OriginalRequestRef, seal *effects.AttemptSeal, work *effects.WorkIngressSeal) error {
	return r.recoverEventControlUsing(ctx, control, original, seal, work, codex.Contract2AcceptanceAuthority)
}

func (r *ControlOperationRecovery) recoverEventControlUsing(ctx context.Context, control effects.ControlState, original controlrecord.OriginalRequestRef, seal *effects.AttemptSeal, work *effects.WorkIngressSeal, verify nativeAcceptance) error {
	brain := r.programme.Brain
	sourceRef, source, err := controlrecord.FindControlEventSource(brain, original)
	if err != nil {
		return err
	}
	if source.Input != control.Input {
		return effects.ErrEvidence
	}
	if _, err := readControlEventSourceLocal(ctx, brain, sourceRef, verify); err != nil {
		return err
	}
	// The receiving process is observed before taking the tick or event lock.
	if err := (NativeAuthority{Brain: brain}).ValidateControlOutputWriterStopped(ctx, source.Writer); err != nil {
		return err
	}
	actual, err := r.programme.Service().ReadControlReference(ctx, control.Receipt)
	if err != nil {
		return err
	}
	if actual != control.Input {
		return effects.ErrEvidence
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: sourceRef, Receipt: control.Receipt, Input: actual})
	if err != nil {
		return err
	}
	release, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return err
	}
	result, lookupErr := LookupControlEventLocked(ctx, brain, sourceRef)
	if lookupErr == nil {
		release()
		return r.storeAndComplete(ctx, control, original, result.Outcome, result.Reply, result.Proof)
	}
	if !errors.Is(lookupErr, os.ErrNotExist) {
		release()
		return lookupErr
	}
	proof, err := closeControlEventAbsentLocked(ctx, brain, sourceRef, enrollment, seal, work, verify)
	if errors.Is(err, events.ErrControlEventPresent) {
		result, err = LookupControlEventLocked(ctx, brain, sourceRef)
		release()
		if err != nil {
			return err
		}
		return r.storeAndComplete(ctx, control, original, result.Outcome, result.Reply, result.Proof)
	}
	release()
	if err != nil {
		return err
	}
	reply, err := codex.ControlEventAbsentReply()
	if err != nil {
		return err
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRejected, Rejection: &effects.GuardReceipt{Operation: control.Input.Operation, Code: controlrecord.ControlEventNotAppendedCode, EvidenceSHA256: proof.SHA256}}
	return r.storeAndComplete(ctx, control, original, outcome, reply, proof)
}

func (r *ControlOperationRecovery) recoverReplyControl(ctx context.Context, control effects.ControlState, originalRef controlrecord.OriginalRequestRef) error {
	proof, snapshot, err := controlrecord.FindReplySnapshot(r.programme.Brain, originalRef)
	if err == nil {
		if !reflect.DeepEqual(snapshot.Input, control.Input) {
			return effects.ErrEvidence
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: control.Input.Operation, RequestSHA256: control.Input.PayloadSHA256, ResponseSHA256: snapshot.ResponseSHA256}}
		return r.storeAndComplete(ctx, control, originalRef, outcome, snapshot.Reply, proof)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return fmt.Errorf("%w: missing retained domain receipt for %s", ErrCodexIntegration, control.Input.Operation)
}

func validateWorkControlAuthorities(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, verify nativeAcceptance) error {
	if scope.CodexContract != 2 || scope.WorkID == "" || scope.EpicID != oldOwner.EpicID || scope.EpicID != oldWork.EpicID || scope.OwnerID != oldOwner.OwnerID || scope.WorkID != oldWork.OwnerID || oldWork.Kind != blueprint.MachineWork {
		return effects.ErrAuthority
	}
	_, owner, err := nativeAuthority(ctx, brain, oldOwner.EpicID, oldOwner.OwnerID, false, &oldOwner, verify)
	if err != nil {
		return err
	}
	_, work, err := nativeAuthority(ctx, brain, oldWork.EpicID, oldWork.OwnerID, false, &oldWork, verify)
	if err != nil {
		return err
	}
	if work.WorkGeneration != scope.Generation {
		return effects.ErrAuthority
	}
	if scope.OwnerID == scope.WorkID {
		if oldOwner != oldWork || work.OwnerActivation != nil || scope.SetID != "" || scope.Lease != 0 {
			return effects.ErrAuthority
		}
		return nil
	}
	if oldOwner.Kind != blueprint.MachineSet || scope.SetID != scope.OwnerID || scope.Lease < 1 || work.OwnerActivation == nil || *work.OwnerActivation != oldOwner.Activation {
		return effects.ErrAuthority
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	entry, err := index.Lookup(scope.OwnerID)
	if err != nil {
		return err
	}
	log, err := nativeCompleteLog(entry.Dir)
	if err != nil {
		return err
	}
	var retained []events.Event
	for _, basis := range owner.Basis {
		if basis.OwnerID != scope.OwnerID {
			continue
		}
		for _, ref := range basis.Events {
			if ref.Sequence < 1 || ref.Sequence > len(log) || nativeEventRef(scope.OwnerID, log[ref.Sequence-1]) != ref {
				return effects.ErrEvidence
			}
			retained = append(retained, log[ref.Sequence-1])
		}
	}
	if grantedLease(retained) != scope.Lease {
		return effects.ErrAuthority
	}
	return nil
}

func (r *ControlOperationRecovery) validateWorkControlSource(ctx context.Context, control effects.ControlState, scope helper.Scope, oldOwner, oldWork OwnerAuthority, verify nativeAcceptance) error {
	brain := r.programme.Brain
	originalRef, original, err := controlrecord.FindControlOriginal(brain, control.Receipt.Control)
	if err != nil {
		return err
	}
	if original.Control == nil || *original.Control != control.Receipt.Control || control.Input.Operation != original.Operation || control.Input.PayloadSHA256 != original.PayloadSHA256 || control.Input.Target.WorkID != scope.WorkID {
		return effects.ErrEvidence
	}
	if toolcontract.IsReadOperationForContract(original.Tool, original.Operation, 2) {
		admission, err := findControlReadAdmissionLocal(ctx, brain, originalRef, ControlReadDirect, verify)
		if err == nil {
			if admission.Control == nil || admission.Control.Receipt != control.Receipt || admission.Control.Input != control.Input || admission.Authority != oldOwner || admission.Work == nil || admission.Work.Authorization() != scope.Authorization() {
				return effects.ErrEvidence
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		_, pre, err := findControlReadPreSourceLocal(ctx, brain, originalRef, ControlReadDirect, verify)
		if err != nil {
			return err
		}
		if pre.Coordinator == nil || pre.Helper != nil || pre.Coordinator.Ingress.Authority != oldOwner || pre.Coordinator.Ingress.Control != control.Receipt.Control || pre.Coordinator.Ingress.Input != control.Input || pre.Coordinator.Work == nil || pre.Coordinator.Work.Authorization() != scope.Authorization() {
			return effects.ErrEvidence
		}
		return nil
	}
	switch original.Operation {
	case "helper_submit", "helper_continue", "helper_cancel":
		_, witness, err := helper.FindControlAdmissionWitnessLocal(brain, originalRef)
		if err != nil {
			return err
		}
		if witness.Source.Authority != oldOwner || witness.Source.Input != control.Input || witness.Scope.Authorization() != scope.Authorization() {
			return effects.ErrEvidence
		}
		return nil
	case "event", "heartbeat", "inbox":
		sourceRef, _, err := controlrecord.FindControlEventSource(brain, originalRef)
		if err != nil {
			return err
		}
		source, err := readControlEventSourceLocal(ctx, brain, sourceRef, verify)
		if err != nil {
			return err
		}
		var policy controlEventPolicy
		if err := toolcontract.DecodeBroker(source.Policy, &policy); err != nil {
			return err
		}
		if source.Authority != oldOwner || source.Input != control.Input || policy.Scope == nil || policy.Scope.Authorization() != scope.Authorization() || policy.WorkAuthority == nil || *policy.WorkAuthority != oldWork {
			return effects.ErrEvidence
		}
		return nil
	case "file_write":
		_, source, err := helper.FindControlOutputIngressLocal(brain, originalRef)
		if err != nil {
			return err
		}
		if source.Source.Authority != oldOwner || source.Source.Input != control.Input {
			return effects.ErrEvidence
		}
		if source.WorkScope == nil || source.WorkAuthority == nil {
			return fmt.Errorf("%w: pending coordinator output lacks captured work scope", ErrCodexIntegration)
		}
		if source.WorkScope.Authorization() != scope.Authorization() || *source.WorkAuthority != oldWork {
			return effects.ErrEvidence
		}
		return validateCoordinatorOutputWorkLocal(ctx, brain, oldOwner, source.Target, source.TargetID, source.WorkScope, source.WorkAuthority, verify)
	default:
		return fmt.Errorf("%w: %s needs a captured work control source", ErrCodexIntegration, original.Operation)
	}
}

func (r *ControlOperationRecovery) settlePositiveControl(ctx context.Context, control effects.ControlState, verify nativeAcceptance) (bool, error) {
	brain := r.programme.Brain
	originalRef, original, err := controlrecord.FindControlOriginal(brain, control.Receipt.Control)
	if err != nil {
		return false, err
	}
	actual, err := r.programme.Service().ReadControlReference(ctx, control.Receipt)
	if err != nil {
		return false, err
	}
	if actual != control.Input || original.Control == nil || *original.Control != control.Receipt.Control || original.Operation != actual.Operation || original.PayloadSHA256 != actual.PayloadSHA256 {
		return false, effects.ErrEvidence
	}
	if _, record, err := controlrecord.FindOutcome(brain, originalRef); err == nil {
		return true, r.completeControl(ctx, control, record)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if proof, snapshot, err := controlrecord.FindReplySnapshot(brain, originalRef); err == nil {
		if snapshot.Input != actual {
			return true, effects.ErrEvidence
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: actual.Operation, RequestSHA256: actual.PayloadSHA256, ResponseSHA256: snapshot.ResponseSHA256}}
		return true, r.storeAndComplete(ctx, control, originalRef, outcome, snapshot.Reply, proof)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if toolcontract.IsReadOperationForContract(original.Tool, original.Operation, 2) {
		proof, reply, err := codex.FindControlReadProof(ctx, brain, originalRef)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: actual.Operation, RequestSHA256: actual.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
		return true, r.storeAndComplete(ctx, control, originalRef, outcome, reply, proof)
	}
	switch original.Operation {
	case "helper_submit", "helper_continue", "helper_cancel":
		witnessRef, witness, err := helper.FindControlAdmissionWitnessLocal(brain, originalRef)
		if err != nil {
			return false, err
		}
		if witness.Source.Input != actual {
			return false, effects.ErrEvidence
		}
		proof, admission, err := r.helpers.LookupControlAdmission(ctx, witnessRef)
		if errors.Is(err, helper.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		reply, err := codex.ControlAdmissionReply(admission)
		if err != nil {
			return true, err
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlBroker, Broker: &effects.BrokerAdmission{JobID: admission.Accepted.JobID, Round: admission.Accepted.Round, RequestSHA256: admission.RequestSHA256, ScopeSHA256: admission.ScopeSHA256}}
		return true, r.storeAndComplete(ctx, control, originalRef, outcome, reply, proof)
	case "file_write":
		ref, err := r.helpers.FindControlOutput(ctx, originalRef)
		if errors.Is(err, helper.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		prepared, commit, proof, err := r.helpers.ReadControlOutputCommitLocal(ctx, ref)
		if errors.Is(err, helper.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if prepared.Original != originalRef || prepared.Caller.Coordinator == nil || prepared.Caller.Coordinator.Control != control.Receipt || prepared.Path != actual.Target.Path || commit.FinalSHA256 != prepared.FinalSHA256 {
			return true, effects.ErrEvidence
		}
		release, err := waitLock(ctx, brain, "tick")
		if err != nil {
			return true, err
		}
		err = r.helpers.AdoptControlOutputCommitLocked(ctx, ref, proof)
		release()
		if err != nil {
			return true, err
		}
		reply, err := json.Marshal(map[string]string{"path": prepared.Path, "filename": prepared.Caller.Coordinator.OutputName})
		if err != nil {
			return true, err
		}
		outcome := effects.ControlOutcome{Kind: effects.ControlFile, File: &effects.FileReceipt{Path: prepared.Path, SHA256: prepared.FinalSHA256, PublicationSHA256: proof.SHA256}}
		return true, r.storeAndComplete(ctx, control, originalRef, outcome, reply, proof)
	case "event", "heartbeat", "inbox":
		sourceRef, _, err := controlrecord.FindControlEventSource(brain, originalRef)
		if err != nil {
			return false, err
		}
		source, err := readControlEventSourceLocal(ctx, brain, sourceRef, verify)
		if err != nil {
			return false, err
		}
		if source.Input != actual {
			return false, effects.ErrEvidence
		}
		release, err := waitLock(ctx, brain, "tick")
		if err != nil {
			return false, err
		}
		result, err := LookupControlEventLocked(ctx, brain, sourceRef)
		release()
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, r.storeAndComplete(ctx, control, originalRef, result.Outcome, result.Reply, result.Proof)
	default:
		return false, nil
	}
}
