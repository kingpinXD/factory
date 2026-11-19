package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type ControlReadEvidenceAuthority interface {
	ValidateControlReadInputAbsentLocal(context.Context, controlrecord.OriginalRequestRef, effects.ControlInput, ProgramReadInputAbsentRef) error
}

type ControlEventEvidenceAuthority interface {
	ValidateControlEventSourceLocal(context.Context, controlrecord.OriginalRequestRef, effects.ControlInput, controlrecord.ImmutableRef) error
}

type ControlWriteEvidenceAuthority interface {
	ValidateControlWriteOwnerLocal(context.Context, controlrecord.OwnerAuthority) error
}

type FactoryControlValidator struct {
	brain         string
	ReadEvidence  ControlReadEvidenceAuthority
	EventEvidence ControlEventEvidenceAuthority
	WriteEvidence ControlWriteEvidenceAuthority
}

func NewFactoryControlValidator(brain string) (*FactoryControlValidator, error) {
	root, err := filepath.EvalSymlinks(brain)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(root) {
		return nil, effects.ErrAuthority
	}
	return &FactoryControlValidator{brain: root}, nil
}

func (v *FactoryControlValidator) ValidateAttempt(ctx context.Context, key effects.AttemptKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.brain == "" {
		return effects.ErrAuthority
	}
	ref := AttemptRef{SessionID: key.SessionID, AttemptID: key.AttemptID}
	record, err := readAttemptRecord(v.brain, ref)
	if err != nil {
		return err
	}
	index, err := readAttemptIndex(v.brain, key.SessionID)
	if err != nil {
		return err
	}
	if index.CurrentAttempt != key.AttemptID || record.Spec.Name != index.Name || record.Spec.Task != index.OwnerID || record.Spec.EpicID != index.EpicID || (record.Stage != attemptPrepared && record.Stage != attemptClaimed) {
		return effects.ErrAuthority
	}
	return nil
}

func (v *FactoryControlValidator) ValidateAdmission(ctx context.Context, id effects.ControlID, input effects.ControlInput) error {
	if err := v.ValidateAttempt(ctx, id.Attempt); err != nil {
		return err
	}
	record, err := readAttemptRecord(v.brain, AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID})
	if err != nil {
		return err
	}
	if record.Stage != attemptClaimed || !record.Claimed || !record.EffectsInitialized || record.State != "active" || record.Rerouted || record.ThreadID != id.ThreadID || record.TurnID != id.TurnID || id.ThreadID == "" || id.TurnID == "" || id.CallID == "" {
		return effects.ErrAuthority
	}
	_, original, err := controlrecord.FindControlOriginal(v.brain, id)
	if err != nil {
		return err
	}
	if original.Control == nil || *original.Control != id || original.Helper != nil || original.Tool != toolcontract.FactoryControl || input.Operation != original.Operation || input.PayloadSHA256 != original.PayloadSHA256 {
		return effects.ErrEvidence
	}
	return nil
}

// ReadAttemptControlLocal rechecks a previously observed caller without process I/O.
func ReadAttemptControlLocal(ctx context.Context, brain string, id effects.ControlID) (session.Spec, error) {
	if err := ctx.Err(); err != nil {
		return session.Spec{}, err
	}
	index, err := readAttemptIndex(brain, id.Attempt.SessionID)
	if err != nil {
		return session.Spec{}, err
	}
	record, err := readAttemptRecord(brain, AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID})
	if err != nil {
		return session.Spec{}, err
	}
	if index.CurrentAttempt != id.Attempt.AttemptID || record.Spec.Name != index.Name || record.Spec.Task != index.OwnerID || record.Spec.EpicID != index.EpicID || record.Stage != attemptClaimed || !record.Claimed || !record.EffectsInitialized || record.State != "active" || record.Rerouted || record.ThreadID != id.ThreadID || record.TurnID != id.TurnID || id.CallID == "" || id.ThreadID == "" || id.TurnID == "" {
		return session.Spec{}, effects.ErrAuthority
	}
	return record.Spec, nil
}

func ReadRetainedControlAttemptSpec(ctx context.Context, brain string, id effects.ControlID) (session.Spec, error) {
	if err := ctx.Err(); err != nil {
		return session.Spec{}, err
	}
	record, err := readAttemptRecord(brain, AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID})
	if err != nil {
		return session.Spec{}, err
	}
	if record.Schema != 2 || record.CodexContract != 2 || record.attemptRef().effectsKey() != id.Attempt {
		return session.Spec{}, effects.ErrAuthority
	}
	return record.Spec, nil
}

func (v *FactoryControlValidator) ValidateOutcome(ctx context.Context, id effects.ControlID, input effects.ControlInput, outcome effects.ControlOutcome) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.brain == "" {
		return effects.ErrAuthority
	}
	originalRef, original, err := controlrecord.FindControlOriginal(v.brain, id)
	if err != nil {
		return err
	}
	if input.Operation != original.Operation || input.PayloadSHA256 != original.PayloadSHA256 {
		return effects.ErrEvidence
	}
	if outcome.Unprepared != nil {
		return v.validateUnpreparedLocal(ctx, id, input, outcome, originalRef)
	}
	record, err := controlrecord.ReadOutcome(v.brain, originalRef)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(record.Input, input) || !reflect.DeepEqual(record.Outcome, outcome) {
		return effects.ErrEvidence
	}
	ctx = controlrecord.WithOutcomeValidation(ctx, id)
	switch outcome.Kind {
	case effects.ControlWrite:
		return v.validateWriteOutcome(ctx, id, input, record)
	case effects.ControlRejected:
		if outcome.Rejection != nil && outcome.Rejection.Code == controlrecord.ControlEventNotAppendedCode {
			return v.validateControlEventOutcome(ctx, originalRef, input, record, true)
		}
		if outcome.Rejection == nil || outcome.Rejection.Operation != input.Operation || outcome.Rejection.Code != helper.ControlNotAdmittedCode || outcome.Rejection.EvidenceSHA256 != record.Proof.SHA256 {
			return effects.ErrEvidence
		}
		proof, err := helper.ReadUnadmittedControlLocal(ctx, v.brain, record.Proof, &AttemptJournalGate{brain: v.brain})
		if err != nil {
			return err
		}
		enrollment, err := controlrecord.ReadEnrollmentSnapshot(v.brain, proof.Request.Enrollment)
		if err != nil {
			return err
		}
		reply, err := UnadmittedControlReply()
		if err != nil {
			return err
		}
		if proof.Request.Original != originalRef || enrollment.Receipt.Control != id || !reflect.DeepEqual(enrollment.Input, input) || !bytes.Equal(record.Reply, reply) {
			return effects.ErrEvidence
		}
		return nil
	case effects.ControlBroker:
		if input.Operation != "helper_submit" && input.Operation != "helper_continue" && input.Operation != "helper_cancel" || outcome.Broker == nil {
			return effects.ErrEvidence
		}
		admission, err := helper.ReadControlAdmissionLocal(v.brain, record.Proof)
		if err != nil {
			return err
		}
		if admission.Original != originalRef || admission.Accepted.JobID != outcome.Broker.JobID || admission.Accepted.Round != outcome.Broker.Round || admission.RequestSHA256 != outcome.Broker.RequestSHA256 || admission.ScopeSHA256 != outcome.Broker.ScopeSHA256 || input.Target.WorkID != "" && input.Target.WorkID != admission.Job.Scope.WorkID || input.Target.JobID != "" && input.Target.JobID != admission.Job.ID {
			return effects.ErrEvidence
		}
		reply, err := ControlAdmissionReply(admission)
		if err != nil {
			return err
		}
		if !bytes.Equal(reply, record.Reply) {
			return effects.ErrEvidence
		}
		return nil
	case effects.ControlFile:
		if input.Operation != "file_write" || outcome.File == nil || outcome.File.Path != input.Target.Path || outcome.File.PublicationSHA256 != record.Proof.SHA256 {
			return effects.ErrEvidence
		}
		name := filepath.Join(v.brain, "factory", "helpers", "control-outputs", controlValueDigest(originalRef).String(), "commit.json")
		if record.Proof.Path != name {
			return effects.ErrEvidence
		}
		b, err := readProgramArtifact(ProgramArtifact{Path: name, SHA256: record.Proof.SHA256})
		if err != nil {
			return err
		}
		var commit helper.ControlOutputCommit
		if err := toolcontract.DecodeBroker(b, &commit); err != nil {
			return err
		}
		prepared, actual, err := (helper.Service{Brain: v.brain}).ReadControlOutput(ctx, commit.Prepared)
		if err != nil {
			return err
		}
		if actual == nil || !reflect.DeepEqual(*actual, commit) || prepared.Original != originalRef || prepared.Path != outcome.File.Path || prepared.FinalSHA256 != outcome.File.SHA256 || prepared.Caller.Coordinator == nil || prepared.Caller.Coordinator.Control.Control != id {
			return effects.ErrEvidence
		}
		return nil
	case effects.ControlRead:
		if outcome.Read == nil {
			return effects.ErrEvidence
		}
		if !localControlRead(input.Operation) {
			proof, reply, err := ReadControlReadProof(ctx, v.brain, record.Proof)
			if err != nil {
				return err
			}
			if proof.Original != originalRef || !bytes.Equal(reply, record.Reply) || outcome.Read.Operation != input.Operation || outcome.Read.RequestSHA256 != input.PayloadSHA256 || outcome.Read.ResponseSHA256 != effects.Digest(sha256.Sum256(reply)) {
				return effects.ErrEvidence
			}
			if err := validateControlReadSourceInput(ctx, v.brain, proof, &input); err != nil {
				return err
			}
			if proof.InputAbsent != nil {
				if v.ReadEvidence == nil {
					return effects.ErrAuthority
				}
				if err := v.ReadEvidence.ValidateControlReadInputAbsentLocal(ctx, originalRef, input, *proof.InputAbsent); err != nil {
					return err
				}
			}
			return nil
		}
		snapshot, err := controlrecord.ReadReplySnapshot(v.brain, record.Proof)
		if err != nil {
			return err
		}
		if snapshot.Original != originalRef || !reflect.DeepEqual(snapshot.Input, input) || !reflect.DeepEqual(snapshot.Reply, record.Reply) || snapshot.ResponseSHA256 != outcome.Read.ResponseSHA256 || outcome.Read.Operation != input.Operation || outcome.Read.RequestSHA256 != input.PayloadSHA256 {
			return effects.ErrEvidence
		}
		return nil
	case effects.ControlEvent:
		return v.validateControlEventOutcome(ctx, originalRef, input, record, false)
	default:
		return effects.ErrEvidence
	}
}

func localControlRead(op string) bool {
	switch op {
	case "brain_read", "plan_input", "epic_file", "events", "inbox", "lease_check", "file_read", "query", "helper_result", "effect_result":
		return true
	}
	return false
}

func (v *FactoryControlValidator) ResolveOutcome(ctx context.Context, id effects.ControlID, input effects.ControlInput, actions []effects.ActionReceiptRef) (effects.ControlOutcome, error) {
	if v == nil || v.brain == "" {
		return effects.ControlOutcome{}, effects.ErrAuthority
	}
	if err := ctx.Err(); err != nil {
		return effects.ControlOutcome{}, err
	}
	ref, _, err := controlrecord.FindControlOriginal(v.brain, id)
	if err != nil {
		return effects.ControlOutcome{}, err
	}
	record, err := controlrecord.ReadOutcome(v.brain, ref)
	if err != nil {
		return effects.ControlOutcome{}, err
	}
	if !reflect.DeepEqual(record.Outcome.Actions, actions) && !(len(record.Outcome.Actions) == 0 && len(actions) == 0) {
		return effects.ControlOutcome{}, effects.ErrEvidence
	}
	if err := v.ValidateOutcome(ctx, id, input, record.Outcome); err != nil {
		return effects.ControlOutcome{}, err
	}
	return record.Outcome, nil
}

func (v *FactoryControlValidator) validateUnpreparedLocal(ctx context.Context, id effects.ControlID, input effects.ControlInput, outcome effects.ControlOutcome, original controlrecord.OriginalRequestRef) error {
	proof := outcome.Unprepared
	if proof == nil || outcome.Kind != effects.ControlRejected || outcome.Rejection == nil || outcome.Rejection.Code != "not_prepared_before_ingress_closed" || outcome.Rejection.EvidenceSHA256 != controlValueDigest(*proof) || proof.Request.Control.Control != id || !reflect.DeepEqual(proof.Request.Input, input) || proof.Request.Original.Path != original.Path || proof.Request.Original.SHA256 != original.SHA256 || (proof.Request.AttemptSeal == nil) == (proof.Request.WorkSeal == nil) {
		return effects.ErrEvidence
	}
	if _, err := readProgramArtifact(ProgramArtifact{Path: proof.Request.Source.Path, SHA256: proof.Request.Source.SHA256}); err != nil {
		return err
	}
	if seal := proof.Request.AttemptSeal; seal != nil {
		gate, err := NewAttemptJournalGate(v.brain)
		if err != nil {
			return err
		}
		actual, err := gate.SealedAttempt(ctx, id.Attempt)
		if err != nil || actual != *seal {
			return effects.ErrEvidence
		}
		return nil
	}
	seal := proof.Request.WorkSeal
	closed, err := helper.ReadClosedScopeFenceLocal(ctx, v.brain, helper.ClosedScopeFenceRef{Path: seal.Fence.Path, SHA256: seal.Fence.SHA256})
	if err != nil {
		return err
	}
	if closed.WorkID != seal.WorkID || seal.WorkID != input.Target.WorkID || seal.OwnerID != proof.Request.Slot.OwnerID || seal.OwnerRevision != proof.Request.Slot.WorkRevision {
		return effects.ErrEvidence
	}
	for _, scope := range closed.Scopes {
		if scope.OwnerID != seal.OwnerID {
			return effects.ErrEvidence
		}
	}
	return nil
}

func controlValueDigest(v any) effects.Digest { b, _ := json.Marshal(v); return sha256.Sum256(b) }

var _ effects.ControlValidator = (*FactoryControlValidator)(nil)

type ControlReadProof struct {
	Schema      int                              `json:"schema"`
	Original    controlrecord.OriginalRequestRef `json:"original"`
	Result      ProgramReadResultRef             `json:"result"`
	Unavailable bool                             `json:"unavailable"`
	NoAction    *ProgramReadNoActionRef          `json:"no_action,omitempty"`
	InputAbsent *ProgramReadInputAbsentRef       `json:"input_absent,omitempty"`
}

func StoreControlReadNoActionProof(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, ref ProgramReadNoActionRef) (controlrecord.ImmutableRef, json.RawMessage, error) {
	proof := ControlReadProof{Schema: 2, Original: original, Unavailable: true, NoAction: &ref}
	if err := validateControlReadSource(ctx, brain, proof); err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	reply, err := controlReadNoActionReply()
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	name := controlReadProofPath(brain, original)
	if err := publishProgramReadArtifact(name, data); err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	return controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}, reply, nil
}

func StoreControlReadInputAbsentProof(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, ref ProgramReadInputAbsentRef) (controlrecord.ImmutableRef, json.RawMessage, error) {
	proof := ControlReadProof{Schema: 2, Original: original, Unavailable: true, InputAbsent: &ref}
	if err := validateControlReadSource(ctx, brain, proof); err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	reply, err := controlReadNoActionReply()
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	name := controlReadProofPath(brain, original)
	if err := publishProgramReadArtifact(name, data); err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	return controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}, reply, nil
}

func controlReadProofPath(brain string, original controlrecord.OriginalRequestRef) string {
	return filepath.Join(brain, "factory", "codex", "control-read-receipts", controlValueDigest(original).String()+".json")
}

func FindControlReadProof(ctx context.Context, brain string, original controlrecord.OriginalRequestRef) (controlrecord.ImmutableRef, json.RawMessage, error) {
	name := controlReadProofPath(brain, original)
	data, err := readProgramArtifactFile(name)
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}
	proof, reply, err := ReadControlReadProof(ctx, brain, ref)
	if err == nil && proof.Original != original {
		err = effects.ErrEvidence
	}
	return ref, reply, err
}

func StoreControlReadProof(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, result ProgramReadResultRef) (controlrecord.ImmutableRef, json.RawMessage, error) {
	proof := ControlReadProof{Schema: 2, Original: original, Result: result}
	evidence, err := ReadProgramReadEvidenceLocal(ctx, brain, result)
	var unavailable *gh.ProgramReadUnavailable
	if err != nil && !errors.As(err, &unavailable) {
		return controlrecord.ImmutableRef{}, nil, err
	}
	proof.Unavailable = unavailable != nil
	if err := validateControlReadSource(ctx, brain, proof); err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	reply, err := controlReadPublicReply(evidence)
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	name := controlReadProofPath(brain, original)
	if err := publishProgramReadArtifact(name, data); err != nil {
		return controlrecord.ImmutableRef{}, nil, err
	}
	return controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}, reply, nil
}

func ReadControlReadProof(ctx context.Context, brain string, ref controlrecord.ImmutableRef) (ControlReadProof, json.RawMessage, error) {
	var proof ControlReadProof
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "codex", "control-read-receipts") {
		return proof, nil, effects.ErrEvidence
	}
	data, err := readProgramArtifact(ProgramArtifact{Path: ref.Path, SHA256: ref.SHA256})
	if err != nil {
		return proof, nil, err
	}
	if err := toolcontract.DecodeBroker(data, &proof); err != nil {
		return proof, nil, err
	}
	if proof.Schema != 2 || ref.Path != controlReadProofPath(brain, proof.Original) || !bytes.Equal(data, mustControlJSON(proof)) {
		return proof, nil, effects.ErrEvidence
	}
	if err := validateControlReadSource(ctx, brain, proof); err != nil {
		return proof, nil, err
	}
	if proof.NoAction != nil || proof.InputAbsent != nil {
		reply, err := controlReadNoActionReply()
		return proof, reply, err
	}
	result, err := ReadProgramReadEvidenceLocal(ctx, brain, proof.Result)
	var unavailable *gh.ProgramReadUnavailable
	if err != nil && !errors.As(err, &unavailable) {
		return proof, nil, err
	}
	if proof.Unavailable != (unavailable != nil) {
		return proof, nil, effects.ErrEvidence
	}
	reply, err := controlReadPublicReply(result)
	return proof, reply, err
}

func validateControlReadSource(ctx context.Context, brain string, proof ControlReadProof) error {
	return validateControlReadSourceInput(ctx, brain, proof, nil)
}

func validateControlReadSourceInput(ctx context.Context, brain string, proof ControlReadProof, expected *effects.ControlInput) error {
	original, err := controlrecord.ReadOriginal(brain, proof.Original)
	if err != nil {
		return err
	}
	requestRef := ProgramReadRef{RequestID: proof.Result.RequestID, SHA256: proof.Result.RequestSHA256}
	var sourceBytes json.RawMessage
	if proof.InputAbsent != nil {
		if proof.Result != (ProgramReadResultRef{}) || proof.NoAction != nil || !proof.Unavailable {
			return effects.ErrEvidence
		}
		absent, err := ReadProgramReadInputAbsentLocal(ctx, brain, *proof.InputAbsent)
		if err != nil {
			return err
		}
		if absent.Original != proof.Original {
			return effects.ErrEvidence
		}
		sourceBytes = absent.Admission
	}
	if proof.NoAction != nil {
		if proof.Result != (ProgramReadResultRef{}) || !proof.Unavailable {
			return effects.ErrEvidence
		}
		noAction, err := ReadProgramReadNoActionLocal(ctx, brain, *proof.NoAction)
		if err != nil {
			return err
		}
		if noAction.Original != proof.Original {
			return effects.ErrEvidence
		}
		requestRef = noAction.Request
	}
	if proof.InputAbsent == nil {
		request, err := ReadProgramReadRequest(ctx, brain, requestRef)
		if err != nil {
			return err
		}
		if request.Kind != "program_read" {
			return effects.ErrEvidence
		}
		sourceBytes = request.Source
	}
	var source map[string]json.RawMessage
	if err := toolcontract.DecodeBroker(sourceBytes, &source); err != nil {
		return err
	}
	if original.Control != nil {
		var control struct {
			Receipt  effects.ControlReceiptRef
			Input    effects.ControlInput
			Original controlrecord.OriginalRequestRef
		}
		if err := toolcontract.DecodeBroker(source["Control"], &control); err != nil {
			return err
		}
		if control.Original != proof.Original || control.Receipt.Control != *original.Control || control.Input.Operation != original.Operation || control.Input.PayloadSHA256 != original.PayloadSHA256 || !bytes.Equal(bytes.TrimSpace(source["Helper"]), []byte("null")) {
			return effects.ErrEvidence
		}
		if expected != nil && control.Input != *expected {
			return effects.ErrEvidence
		}
		return nil
	}
	var helperSource struct {
		JobID                    string
		Generation, Round        int
		ThreadID, TurnID, CallID string
		Input                    effects.ControlInput
		Original                 controlrecord.OriginalRequestRef
	}
	if err := toolcontract.DecodeBroker(source["Helper"], &helperSource); err != nil {
		return err
	}
	if original.Helper == nil || *original.Helper != (controlrecord.HelperCall{JobID: helperSource.JobID, Generation: helperSource.Generation, Round: helperSource.Round, ThreadID: helperSource.ThreadID, TurnID: helperSource.TurnID, CallID: helperSource.CallID}) || helperSource.Original != proof.Original || helperSource.Input.Operation != original.Operation || helperSource.Input.PayloadSHA256 != original.PayloadSHA256 || !bytes.Equal(bytes.TrimSpace(source["Control"]), []byte("null")) {
		return effects.ErrEvidence
	}
	if expected != nil && helperSource.Input != *expected {
		return effects.ErrEvidence
	}
	return nil
}

func controlReadNoActionReply() (json.RawMessage, error) {
	return json.Marshal(ToolFailure{ToolError: "GitHub evidence is unavailable because the programme-owned read did not execute."})
}

func controlReadPublicReply(result ProgramReadResult) (json.RawMessage, error) {
	if result.Failure != "" {
		return json.Marshal(ToolFailure{ToolError: "GitHub evidence is unavailable because the programme-owned read failed."})
	}
	if result.Base != nil || len(result.ReadJSON) == 0 {
		return nil, effects.ErrEvidence
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(result.ReadJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	reply, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	if len(reply) > toolcontract.ControlLimit {
		return json.Marshal(ToolFailure{ToolError: "Read evidence exceeds the control reply limit."})
	}
	return reply, nil
}

func mustControlJSON(v any) []byte { data, _ := json.Marshal(v); return data }

func UnadmittedControlReply() (json.RawMessage, error) {
	return json.Marshal(ToolFailure{ToolError: "The request was not admitted before its original caller stopped."})
}

func ControlAdmissionReply(admission helper.ControlAdmissionReceipt) (json.RawMessage, error) {
	if admission.Cancellation == "requested" {
		return json.Marshal(struct {
			Schema int    `json:"schema_version"`
			JobID  string `json:"job_id"`
			State  string `json:"state"`
		}{1, admission.Job.ID, "cancel_requested"})
	}
	return json.Marshal(admission.Accepted)
}

func (v *FactoryControlValidator) validateControlEventOutcome(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, record controlrecord.OutcomeRecord, absent bool) error {
	if v.EventEvidence == nil {
		return effects.ErrAuthority
	}
	sourceRef, source, err := controlrecord.FindControlEventSource(v.brain, original)
	if err != nil {
		return err
	}
	if source.Input != input {
		return effects.ErrEvidence
	}
	if err := v.EventEvidence.ValidateControlEventSourceLocal(ctx, original, input, sourceRef); err != nil {
		return err
	}
	originalRecord, err := controlrecord.ReadOriginal(v.brain, original)
	if err != nil {
		return err
	}
	if absent {
		proof, _, err := ReadControlEventAbsenceLocal(ctx, v.brain, record.Proof)
		if err != nil {
			return err
		}
		if proof.Source != sourceRef || record.Outcome.Rejection == nil || record.Outcome.Rejection.Operation != input.Operation || record.Outcome.Rejection.EvidenceSHA256 != record.Proof.SHA256 {
			return effects.ErrEvidence
		}
		reply, err := ControlEventAbsentReply()
		if err != nil {
			return err
		}
		if !bytes.Equal(record.Reply, reply) {
			return effects.ErrEvidence
		}
		return nil
	}
	result, err := ReadControlEventResultLocal(ctx, v.brain, sourceRef, record.Proof)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(result.Outcome, record.Outcome) || !bytes.Equal(result.Reply, record.Reply) {
		return effects.ErrEvidence
	}
	enrollmentRef, _, err := controlrecord.FindEnrollmentSnapshot(v.brain, original)
	if err != nil {
		return err
	}
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(v.brain, enrollmentRef)
	if err != nil {
		return err
	}
	if enrollment.Source != sourceRef || enrollment.Original != original || enrollment.Input != input || enrollment.Receipt.Control != *originalRecord.Control {
		return effects.ErrEvidence
	}
	return nil
}
