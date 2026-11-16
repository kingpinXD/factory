package reconcile

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"go.yaml.in/yaml/v3"
)

type controlEventPolicy struct {
	TargetID      string          `json:"target_id"`
	Owner         store.Entry     `json:"owner"`
	Target        store.Entry     `json:"target"`
	TargetInputs  []byte          `json:"target_inputs,omitempty"`
	Blueprint     []byte          `json:"blueprint,omitempty"`
	Lease         int             `json:"lease"`
	HandedOver    bool            `json:"handed_over"`
	Scope         *helper.Scope   `json:"scope,omitempty"`
	WorkAuthority *OwnerAuthority `json:"work_authority,omitempty"`
}

func ResolveCoordinatorEvent(brain string, spec session.Spec, operation string, input toolcontract.ControlInput) (store.Entry, events.Event, error) {
	policy, event, err := resolveCoordinatorEvent(brain, spec, operation, input)
	return policy.Target, event, err
}
func resolveCoordinatorEvent(brain string, spec session.Spec, operation string, input toolcontract.ControlInput) (controlEventPolicy, events.Event, error) {
	var policy controlEventPolicy
	var event events.Event
	index, err := store.ReadIndex(brain)
	if err != nil {
		return policy, event, err
	}
	policy.Owner, err = index.Lookup(spec.Task)
	if err != nil || policy.Owner.Epic != spec.EpicID {
		return policy, event, fmt.Errorf("coordinator owner changed")
	}
	policy.TargetID = input.Target
	if policy.TargetID == "" {
		policy.TargetID = spec.Task
	}
	plannerInstruction := operation == "event" && input.Kind == events.KindInstruction && policy.Owner.Kind == blueprint.MachineEpic && strings.HasSuffix(spec.Name, ":planner")
	if plannerInstruction {
		policy.Target, err = index.Lookup(policy.TargetID)
		if err == nil && (policy.Target.Epic != spec.EpicID || policy.Target.Kind != blueprint.MachineSet || input.Path != "") {
			err = fmt.Errorf("planner instructions require a set in the same epic")
		}
	} else {
		policy.Target, err = ResolveCoordinatorTarget(index, spec, policy.TargetID)
	}
	if err != nil {
		return policy, event, err
	}
	if policy.TargetID != spec.Task && policy.Target.Kind == blueprint.MachineWork {
		policy.TargetInputs, err = os.ReadFile(filepath.Join(policy.Target.Dir, "inputs.yaml"))
		if err != nil {
			return policy, event, err
		}
		policy.Lease, err = Lease(policy.Owner.Dir)
		if err != nil {
			return policy, event, err
		}
	}
	switch operation {
	case "heartbeat":
		if policy.Target.Kind != blueprint.MachineSet || policy.TargetID != spec.Task {
			return policy, event, fmt.Errorf("heartbeat requires the owning set")
		}
		policy.Lease, err = Lease(policy.Target.Dir)
		if err != nil {
			return policy, event, err
		}
		if !strings.Contains(spec.Generation, ":"+strconv.Itoa(policy.Lease)+":") {
			return policy, event, fmt.Errorf("coordinator lease changed")
		}
		event = events.Event{Kind: events.KindHeartbeat, Sender: spec.Task, Lease: policy.Lease}
	case "event":
		if !slices.Contains(events.AgentEventKinds(), input.Kind) || input.Kind == events.KindInstruction && !plannerInstruction || input.Kind == events.KindIssueStale {
			return policy, event, fmt.Errorf("unsupported coordinator event")
		}
		if input.To != "" && input.To != events.RecipientUser {
			return policy, event, fmt.Errorf("unsupported event recipient")
		}
		if err := events.ValidateAgentEvent(input.Kind, input.Path, input.Text, input.To); err != nil {
			return policy, event, err
		}
		event = events.Event{Kind: input.Kind, Sender: spec.Task, Text: input.Text, Key: input.Key, Recipient: input.To}
		if input.Kind == events.KindCheckpoint && filepath.IsAbs(input.Path) && filepath.Clean(input.Path) == input.Path && input.Path == filepath.Join(policy.Target.Dir, filepath.Base(input.Path)) {
			full, err := controlrecord.ResolveCoordinatorOutput(brain, policy.Target, spec, filepath.Base(input.Path), true)
			if err != nil {
				return policy, event, err
			}
			if _, err := events.HashFile(full); err != nil {
				return policy, event, err
			}
			return policy, event, &codex.ToolInputError{Message: "use the returned filename for checkpoint events; no event was appended"}
		}
		if input.Path != "" {
			policy.Blueprint, err = os.ReadFile(blueprint.Path(brain))
			if err != nil {
				return policy, event, err
			}
			var model blueprint.Blueprint
			if err := yaml.Unmarshal(policy.Blueprint, &model); err != nil {
				return policy, event, err
			}
			event.File, err = controlrecord.ResolveCoordinatorOutputBlueprint(&model, policy.Target, spec, input.Path, true)
			if err != nil {
				return policy, event, err
			}
			event.SHA256, err = events.HashFile(event.File)
			if err != nil {
				return policy, event, err
			}
		}
	case "inbox":
		if input.Ack < 1 {
			return policy, event, fmt.Errorf("positive acknowledgement sequence required")
		}
	default:
		return policy, event, fmt.Errorf("unsupported coordinator operation")
	}
	return policy, event, nil
}

func CaptureControlEventSourceLocked(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter) (controlrecord.ImmutableRef, error) {
	return captureControlEventSourceLocked(ctx, brain, original, writer, codex.Contract2AcceptanceAuthority)
}
func captureControlEventSourceLocked(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter, verify nativeAcceptance) (controlrecord.ImmutableRef, error) {
	if ref, saved, err := controlrecord.FindControlEventSource(brain, original); err == nil {
		if _, err := readControlEventSourceLocal(ctx, brain, ref, verify); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		canonical, err := controlrecord.ReadOriginal(brain, original)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		spec, err := codex.ReadAttemptControlLocal(ctx, brain, *canonical.Control)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		if !reflect.DeepEqual(spec, saved.Spec) {
			return controlrecord.ImmutableRef{}, effects.ErrAuthority
		}
		if _, _, err := nativeAuthority(ctx, brain, saved.Authority.EpicID, saved.Authority.OwnerID, true, &saved.Authority, verify); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, saved.Input.Target.WorkID); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		return ref, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return controlrecord.ImmutableRef{}, err
	}
	ingress, decoded, err := currentControlIngressLocked(ctx, brain, original, writer, verify)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	input, ok := decoded.(*toolcontract.ControlInput)
	if !ok {
		return controlrecord.ImmutableRef{}, effects.ErrEvidence
	}
	policy, event, err := resolveCoordinatorEvent(brain, ingress.Spec, ingress.Input.Operation, *input)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	source := controlrecord.ControlEventSource{Schema: 2, Original: original, Authority: ingress.Authority, Spec: ingress.Spec, Input: ingress.Input, Writer: writer, LogPath: store.EventsPath(policy.Target.Dir), CallerKey: input.Key}
	source.Input.Target.Path = source.LogPath
	if policy.Target.Kind == blueprint.MachineWork {
		source.Input.Target.WorkID = policy.TargetID
		scope, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, policy.TargetID, verify)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		if scope.OwnerID != source.Authority.OwnerID || scope.EpicID != source.Authority.EpicID {
			return controlrecord.ImmutableRef{}, effects.ErrAuthority
		}
		policy.Scope = &scope
		authority, _, err := nativeAuthority(ctx, brain, scope.EpicID, scope.WorkID, true, nil, verify)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		policy.WorkAuthority = &authority
		if len(policy.TargetInputs) == 0 {
			policy.TargetInputs, err = os.ReadFile(filepath.Join(policy.Target.Dir, "inputs.yaml"))
			if err != nil {
				return controlrecord.ImmutableRef{}, err
			}
		}
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, source.Input.Target.WorkID); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	source.Policy, err = json.Marshal(policy)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if ingress.Input.Operation == "inbox" {
		log, err := events.LockControlEventLog(ctx, source.LogPath)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		message, err := log.Message(input.Ack)
		log.Close()
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		source.Message, err = json.Marshal(message)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
	} else {
		event.Key = controlrecord.ControlEventKey(ingress.Control)
		if err := validateControlEventReplySize(event); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		source.Event, err = json.Marshal(event)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
	}
	var ref controlrecord.ImmutableRef
	_, err = (helper.Service{Brain: brain}).WithUnmanagedEvent(ctx, policy.TargetID, event.Kind, event.File, func() (events.Event, error) {
		var err error
		ref, err = controlrecord.StoreControlEventSource(brain, source)
		return events.Event{}, err
	})
	return ref, err
}

func ReadControlEventSourceLocal(ctx context.Context, brain string, ref controlrecord.ImmutableRef) (controlrecord.ControlEventSource, error) {
	return readControlEventSourceLocal(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}
func readControlEventSourceLocal(ctx context.Context, brain string, ref controlrecord.ImmutableRef, verify nativeAcceptance) (controlrecord.ControlEventSource, error) {
	source, err := controlrecord.ReadControlEventSource(brain, ref)
	if err != nil {
		return source, err
	}
	original, decoded, err := controlrecord.DecodeInput(brain, source.Original)
	if err != nil {
		return source, err
	}
	input, ok := decoded.(*toolcontract.ControlInput)
	if !ok || original.Control == nil {
		return source, effects.ErrEvidence
	}
	actual, err := codex.ReadRetainedControlAttemptSpec(ctx, brain, *original.Control)
	if err != nil {
		return source, err
	}
	if !reflect.DeepEqual(actual, source.Spec) {
		return source, effects.ErrAuthority
	}
	if _, _, err := nativeAuthority(ctx, brain, source.Authority.EpicID, source.Authority.OwnerID, false, &source.Authority, verify); err != nil {
		return source, err
	}
	var policy controlEventPolicy
	if err := toolcontract.DecodeBroker(source.Policy, &policy); err != nil {
		return source, err
	}
	if err := validateControlEventPolicy(source, *input, policy, *original.Control); err != nil {
		return source, err
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return source, err
	}
	owner, err := index.Lookup(source.Spec.Task)
	if err != nil {
		return source, err
	}
	target, err := index.Lookup(policy.TargetID)
	if err != nil {
		return source, err
	}
	if owner != policy.Owner || target != policy.Target {
		return source, effects.ErrEvidence
	}
	if policy.Scope != nil {
		scope := policy.Scope.Authorization()
		if scope.CodexContract != 2 || scope.EpicID != source.Authority.EpicID || scope.OwnerID != source.Authority.OwnerID || scope.WorkID != source.Input.Target.WorkID || policy.WorkAuthority == nil || policy.WorkAuthority.OwnerID != scope.WorkID || policy.WorkAuthority.EpicID != scope.EpicID {
			return source, effects.ErrEvidence
		}
		_, work, err := nativeAuthority(ctx, brain, scope.EpicID, scope.WorkID, false, policy.WorkAuthority, verify)
		if err != nil {
			return source, err
		}
		if work.WorkGeneration != scope.Generation {
			return source, effects.ErrAuthority
		}
		if scope.OwnerID == scope.WorkID {
			if *policy.WorkAuthority != source.Authority || work.OwnerActivation != nil {
				return source, effects.ErrAuthority
			}
		} else if work.OwnerActivation == nil || *work.OwnerActivation != source.Authority.Activation {
			return source, effects.ErrAuthority
		}
	}
	return source, nil
}

func validateControlEventPolicy(source controlrecord.ControlEventSource, input toolcontract.ControlInput, policy controlEventPolicy, id effects.ControlID) error {
	target := input.Target
	if target == "" {
		target = source.Spec.Task
	}
	if policy.TargetID != target || policy.Target.Epic != source.Spec.EpicID || policy.Owner.Epic != source.Spec.EpicID || policy.Owner.Kind != source.Authority.Kind || source.LogPath != store.EventsPath(policy.Target.Dir) || !filepath.IsAbs(policy.Target.Dir) || filepath.Clean(policy.Target.Dir) != policy.Target.Dir {
		return effects.ErrEvidence
	}
	plannerInstruction := source.Input.Operation == "event" && input.Kind == events.KindInstruction && policy.Owner.Kind == blueprint.MachineEpic && strings.HasSuffix(source.Spec.Name, ":planner")
	if plannerInstruction {
		if policy.Target.Kind != blueprint.MachineSet || input.Path != "" {
			return effects.ErrAuthority
		}
	} else if target != source.Spec.Task {
		if policy.Owner.Kind != blueprint.MachineSet || policy.Target.Kind != blueprint.MachineWork || policy.HandedOver || !strings.Contains(source.Spec.Generation, ":"+strconv.Itoa(policy.Lease)+":") {
			return effects.ErrAuthority
		}
		var work WorkInputs
		if err := yaml.Unmarshal(policy.TargetInputs, &work); err != nil {
			return err
		}
		if work.ID != target || work.Epic != source.Spec.EpicID || work.Set != source.Spec.Task {
			return effects.ErrAuthority
		}
	}
	expected := effects.ControlTarget{Path: source.LogPath}
	if policy.Target.Kind == blueprint.MachineWork {
		expected.WorkID = target
		if policy.Scope == nil || policy.WorkAuthority == nil {
			return effects.ErrEvidence
		}
	} else if policy.Scope != nil || policy.WorkAuthority != nil {
		return effects.ErrEvidence
	}
	if source.Input.Target != expected {
		return effects.ErrEvidence
	}
	switch source.Input.Operation {
	case "inbox":
		var message events.Event
		if toolcontract.DecodeBroker(source.Message, &message) != nil || input.Ack < 1 || message.Seq != input.Ack || !events.IsMessage(message) {
			return effects.ErrEvidence
		}
	case "event", "heartbeat":
		var actual events.Event
		if err := toolcontract.DecodeBroker(source.Event, &actual); err != nil {
			return err
		}
		expected := events.Event{Sender: source.Spec.Task, Key: controlrecord.ControlEventKey(id)}
		if source.Input.Operation == "heartbeat" {
			if target != source.Spec.Task || policy.Target.Kind != blueprint.MachineSet || !strings.Contains(source.Spec.Generation, ":"+strconv.Itoa(policy.Lease)+":") {
				return effects.ErrAuthority
			}
			expected.Kind = events.KindHeartbeat
			expected.Lease = policy.Lease
		} else {
			if !slices.Contains(events.CoordinatorEventKinds(), input.Kind) || input.Kind == events.KindInstruction && !plannerInstruction || input.To != "" && input.To != events.RecipientUser {
				return effects.ErrAuthority
			}
			if err := events.ValidateAgentEvent(input.Kind, input.Path, input.Text, input.To); err != nil {
				return err
			}
			expected.Kind = input.Kind
			expected.Text = input.Text
			expected.Recipient = input.To
			if input.Path != "" {
				var model blueprint.Blueprint
				if err := yaml.Unmarshal(policy.Blueprint, &model); err != nil {
					return err
				}
				full, err := controlrecord.ResolveCoordinatorOutputPolicy(&model, policy.Target, source.Spec, input.Path, true)
				if err != nil {
					return err
				}
				if len(actual.SHA256) != 64 {
					return effects.ErrEvidence
				}
				if _, err := hex.DecodeString(actual.SHA256); err != nil {
					return effects.ErrEvidence
				}
				expected.File = full
				expected.SHA256 = actual.SHA256
			}
		}
		if !reflect.DeepEqual(actual, expected) || source.CallerKey != input.Key {
			return effects.ErrEvidence
		}
	default:
		return effects.ErrEvidence
	}
	return nil
}

type ControlEventResult = codex.ControlEventResult

func eventControlReference(ref events.EventReceiptRef) (controlrecord.ImmutableRef, error) {
	bytes, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(bytes) != 32 {
		return controlrecord.ImmutableRef{}, effects.ErrEvidence
	}
	var digest effects.Digest
	copy(digest[:], bytes)
	return controlrecord.ImmutableRef{Path: ref.Path, SHA256: digest}, nil
}
func controlEventBinding(source controlrecord.ControlEventSource) (events.Event, int, error) {
	return codex.ControlEventBinding(source)
}

func ReadControlEventResultLocal(ctx context.Context, brain string, sourceRef, proof controlrecord.ImmutableRef) (ControlEventResult, error) {
	return codex.ReadControlEventResultLocal(ctx, brain, sourceRef, proof)
}

func lookupControlEvent(log *events.ControlEventLog, source controlrecord.ControlEventSource) (events.EventReceiptRef, error) {
	event, ack, err := controlEventBinding(source)
	if err != nil {
		return events.EventReceiptRef{}, err
	}
	if ack > 0 {
		return log.LookupAckReceipt(ack)
	}
	return log.LookupEventReceipt(event, source.CallerKey)
}
func ApplyControlEventLocked(ctx context.Context, brain string, sourceRef, enrollmentRef controlrecord.ImmutableRef, writer controlrecord.LocalWriter) (ControlEventResult, error) {
	return applyControlEventLocked(ctx, brain, sourceRef, enrollmentRef, writer, codex.Contract2AcceptanceAuthority)
}
func applyControlEventLocked(ctx context.Context, brain string, sourceRef, enrollmentRef controlrecord.ImmutableRef, writer controlrecord.LocalWriter, verify nativeAcceptance) (ControlEventResult, error) {
	source, err := readControlEventSourceLocal(ctx, brain, sourceRef, verify)
	if err != nil {
		return ControlEventResult{}, err
	}
	if err := validateControlEventEnrollment(brain, sourceRef, enrollmentRef, source); err != nil {
		return ControlEventResult{}, err
	}
	original, _, err := controlrecord.DecodeInput(brain, source.Original)
	if err != nil {
		return ControlEventResult{}, err
	}
	spec, err := codex.ReadAttemptControlLocal(ctx, brain, *original.Control)
	if err != nil {
		return ControlEventResult{}, err
	}
	if !reflect.DeepEqual(spec, source.Spec) {
		return ControlEventResult{}, effects.ErrAuthority
	}
	if err := CoordinatorAuthority(brain, spec); err != nil {
		return ControlEventResult{}, err
	}
	if _, _, err := nativeAuthority(ctx, brain, source.Authority.EpicID, source.Authority.OwnerID, true, &source.Authority, verify); err != nil {
		return ControlEventResult{}, err
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, source.Input.Target.WorkID); err != nil {
		return ControlEventResult{}, err
	}
	var input toolcontract.ControlInput
	if err := toolcontract.DecodeBroker(original.Payload, &input); err != nil {
		return ControlEventResult{}, err
	}
	current, event, err := resolveCoordinatorEvent(brain, spec, source.Input.Operation, input)
	if err != nil {
		return ControlEventResult{}, err
	}
	if store.EventsPath(current.Target.Dir) != source.LogPath {
		return ControlEventResult{}, effects.ErrAuthority
	}
	expected, ack, err := controlEventBinding(source)
	if err != nil {
		return ControlEventResult{}, err
	}
	if ack == 0 {
		event.Key = controlrecord.ControlEventKey(*original.Control)
		if !reflect.DeepEqual(event, expected) {
			return ControlEventResult{}, effects.ErrStale
		}
	}
	var proof controlrecord.ImmutableRef
	_, err = (helper.Service{Brain: brain}).WithUnmanagedEvent(ctx, current.TargetID, event.Kind, event.File, func() (events.Event, error) {
		log, err := events.LockControlEventLog(ctx, source.LogPath)
		if err != nil {
			return events.Event{}, err
		}
		defer log.Close()
		if _, _, err := controlrecord.FindControlEventAbsence(brain, sourceRef); err == nil {
			return events.Event{}, effects.ErrAuthority
		} else if !errors.Is(err, os.ErrNotExist) {
			return events.Event{}, err
		}
		ref, err := lookupControlEvent(log, source)
		if errors.Is(err, os.ErrNotExist) {
			if writer != source.Writer || writer.PID != os.Getpid() {
				return events.Event{}, effects.ErrBusy
			}
			if ack == 0 {
				if err := validateControlEventReplySize(expected); err != nil {
					return events.Event{}, err
				}
			}
			if ack > 0 {
				message, messageErr := log.Message(ack)
				if messageErr != nil {
					return events.Event{}, messageErr
				}
				var captured events.Event
				if err := toolcontract.DecodeBroker(source.Message, &captured); err != nil {
					return events.Event{}, err
				}
				if !reflect.DeepEqual(message, captured) {
					return events.Event{}, effects.ErrEvidence
				}
				ref, err = log.AckWithReceipt(ack, spec.Task)
			} else {
				ref, err = log.AppendWithReceipt(expected, source.CallerKey)
			}
		}
		if err != nil {
			return events.Event{}, err
		}
		proof, err = eventControlReference(ref)
		return events.Event{}, err
	})
	if err != nil {
		return ControlEventResult{}, err
	}
	return ReadControlEventResultLocal(ctx, brain, sourceRef, proof)
}
func validateControlEventEnrollment(brain string, sourceRef, enrollmentRef controlrecord.ImmutableRef, source controlrecord.ControlEventSource) error {
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(brain, enrollmentRef)
	if err != nil {
		return err
	}
	original, err := controlrecord.ReadOriginal(brain, source.Original)
	if err != nil {
		return err
	}
	if original.Control == nil || enrollment.Source != sourceRef || enrollment.Original != source.Original || enrollment.Input != source.Input || enrollment.Receipt.Control != *original.Control {
		return effects.ErrEvidence
	}
	return nil
}
func LookupControlEventLocked(ctx context.Context, brain string, sourceRef controlrecord.ImmutableRef) (ControlEventResult, error) {
	source, err := controlrecord.ReadControlEventSource(brain, sourceRef)
	if err != nil {
		return ControlEventResult{}, err
	}
	log, err := events.LockControlEventLog(ctx, source.LogPath)
	if err != nil {
		return ControlEventResult{}, err
	}
	defer log.Close()
	ref, err := lookupControlEvent(log, source)
	if err != nil {
		return ControlEventResult{}, err
	}
	proof, err := eventControlReference(ref)
	if err != nil {
		return ControlEventResult{}, err
	}
	return ReadControlEventResultLocal(ctx, brain, sourceRef, proof)
}

func (a NativeAuthority) ValidateControlEventSourceLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, sourceRef controlrecord.ImmutableRef) error {
	return a.validateControlEventSourceLocal(ctx, original, input, sourceRef, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) validateControlEventSourceLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, sourceRef controlrecord.ImmutableRef, verify nativeAcceptance) error {
	source, err := readControlEventSourceLocal(ctx, a.Brain, sourceRef, verify)
	if err != nil {
		return err
	}
	if source.Original != original || source.Input != input {
		return effects.ErrEvidence
	}
	return nil
}

func validateControlEventClosure(ctx context.Context, brain string, source controlrecord.ControlEventSource, attempt *effects.AttemptSeal, work *effects.WorkIngressSeal) error {
	if (attempt == nil) == (work == nil) {
		return effects.ErrEvidence
	}
	original, err := controlrecord.ReadOriginal(brain, source.Original)
	if err != nil {
		return err
	}
	if attempt != nil {
		gate, err := codex.NewAttemptJournalGate(brain)
		if err != nil {
			return err
		}
		actual, err := gate.SealedAttempt(ctx, original.Control.Attempt)
		if err != nil || actual != *attempt {
			return effects.ErrEvidence
		}
		return nil
	}
	closed, err := helper.ReadClosedScopeFenceLocal(ctx, brain, helper.ClosedScopeFenceRef{Path: work.Fence.Path, SHA256: work.Fence.SHA256})
	if err != nil {
		return err
	}
	var policy controlEventPolicy
	if err := toolcontract.DecodeBroker(source.Policy, &policy); err != nil {
		return err
	}
	if policy.Scope == nil || work.WorkID != source.Input.Target.WorkID || closed.WorkID != work.WorkID || work.OwnerID != source.Authority.OwnerID || work.OwnerRevision != source.Authority.Revision {
		return effects.ErrEvidence
	}
	if len(closed.Scopes) != 1 {
		return effects.ErrEvidence
	}
	for _, actual := range closed.Scopes {
		if actual.Authorization() != policy.Scope.Authorization() {
			return effects.ErrEvidence
		}
	}
	return nil
}

func closeControlEventAbsentLocked(ctx context.Context, brain string, sourceRef, enrollmentRef controlrecord.ImmutableRef, attempt *effects.AttemptSeal, work *effects.WorkIngressSeal, verify nativeAcceptance) (controlrecord.ImmutableRef, error) {
	source, err := readControlEventSourceLocal(ctx, brain, sourceRef, verify)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if err := validateControlEventEnrollment(brain, sourceRef, enrollmentRef, source); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if ref, record, err := controlrecord.FindControlEventAbsence(brain, sourceRef); err == nil {
		if record.Enrollment != enrollmentRef {
			return controlrecord.ImmutableRef{}, effects.ErrEvidence
		}
		_, _, err := codex.ReadControlEventAbsenceLocal(ctx, brain, ref)
		return ref, err
	} else if !errors.Is(err, os.ErrNotExist) {
		return controlrecord.ImmutableRef{}, err
	}
	if err := validateControlEventClosure(ctx, brain, source, attempt, work); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	log, err := events.LockControlEventLog(ctx, source.LogPath)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	defer log.Close()
	if _, err := lookupControlEvent(log, source); err == nil {
		return controlrecord.ImmutableRef{}, events.ErrControlEventPresent
	} else if !errors.Is(err, os.ErrNotExist) {
		return controlrecord.ImmutableRef{}, err
	}
	event, ack, err := controlEventBinding(source)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	prefix, err := log.ObserveAbsence(event.Key, ack)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if ack > 0 {
		actual, err := log.Message(ack)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		var captured events.Event
		if err := toolcontract.DecodeBroker(source.Message, &captured); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		if !reflect.DeepEqual(actual, captured) {
			return controlrecord.ImmutableRef{}, effects.ErrEvidence
		}
	}
	bytes, err := hex.DecodeString(prefix.SHA256)
	if err != nil || len(bytes) != 32 {
		return controlrecord.ImmutableRef{}, effects.ErrEvidence
	}
	var digest effects.Digest
	copy(digest[:], bytes)
	return controlrecord.StoreControlEventAbsence(brain, controlrecord.ControlEventAbsence{Schema: 2, Source: sourceRef, Enrollment: enrollmentRef, AttemptSeal: attempt, WorkSeal: work, PrefixBytes: prefix.Bytes, PrefixSHA256: digest, Code: controlrecord.ControlEventNotAppendedCode})
}

func validateControlEventReplySize(event events.Event) error {
	event.Seq = math.MaxInt
	event.At = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// jsonReply appends one newline to the unchanged full Event response.
	if len(data)+1 > toolcontract.ControlLimit {
		return &codex.ToolInputError{Message: "event reply exceeds the 1 MiB limit; shorten text before retrying; no event was appended"}
	}
	return nil
}
