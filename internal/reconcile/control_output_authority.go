package reconcile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type ControlOutputWriterPendingError struct{ Writer controlrecord.LocalWriter }

func (*ControlOutputWriterPendingError) Error() string {
	return "original local output writer is still alive"
}

func (a NativeAuthority) CaptureControlOutputWriter() (controlrecord.LocalWriter, error) {
	writer, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		return controlrecord.LocalWriter{}, err
	}
	return controlrecord.LocalWriter{PID: writer.PID, Group: writer.Group, Birth: writer.Birth}, nil
}
func (a NativeAuthority) ValidateControlOutputWriterStopped(ctx context.Context, w controlrecord.LocalWriter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.PID <= 1 || w.Group <= 0 || w.Birth == "" {
		return codex.ErrObservationUnknown
	}
	alive, err := (codex.ProcessIdentity{PID: w.PID, Group: w.Group, Birth: w.Birth}).Alive()
	if err != nil {
		return err
	}
	if alive {
		return &ControlOutputWriterPendingError{Writer: w}
	}
	return nil
}

type coordinatorOutputSnapshot struct {
	Schema   int                              `json:"schema"`
	Original controlrecord.OriginalRequestRef `json:"original"`
	Caller   helper.CoordinatorOutputCaller   `json:"caller"`
	Input    effects.ControlInput             `json:"input"`
	Target   store.Entry                      `json:"target"`
}
type coordinatorBlueprintSnapshot struct {
	Schema int    `json:"schema"`
	Bytes  []byte `json:"bytes"`
}
type helperOutputSnapshot struct {
	WorkAuthority OwnerAuthority                   `json:"work_authority"`
	Schema        int                              `json:"schema"`
	Original      controlrecord.OriginalRequestRef `json:"original"`
	Caller        helper.HelperOutputCaller        `json:"caller"`
	Job           helper.Job                       `json:"job"`
	Authority     OwnerAuthority                   `json:"authority"`
	Path          string                           `json:"path"`
}

func outputSourcePath(brain string, original controlrecord.OriginalRequestRef, name string) string {
	return filepath.Join(brain, "factory", "effects", "control-outputs", effectDigest(original).String(), name+".json")
}

func BuildControlIngressLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter) (helper.ControlIngressSource, error) {
	return buildControlIngressLocked(ctx, brain, originalRef, writer, codex.Contract2AcceptanceAuthority)
}
func buildControlIngressLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter, verify nativeAcceptance) (helper.ControlIngressSource, error) {
	source, input, err := currentControlIngressLocked(ctx, brain, originalRef, writer, verify)
	if err != nil {
		return source, err
	}
	spec := source.Spec
	switch p := input.(type) {
	case *toolcontract.Request:
		if p.OwnerID != spec.Task {
			return source, effects.ErrAuthority
		}
		source.Input.Target.WorkID = p.WorkID
	case *toolcontract.ContinueInput:
		job, err := (helper.Service{Brain: brain}).Job(p.JobID)
		if err != nil {
			return source, err
		}
		if job.Scope.OwnerID != spec.Task || job.Scope.EpicID != spec.EpicID {
			return source, effects.ErrAuthority
		}
		source.Input.Target = effects.ControlTarget{WorkID: job.Scope.WorkID, JobID: job.ID, Round: job.Round}
	case *toolcontract.CancelInput:
		job, err := (helper.Service{Brain: brain}).Job(p.JobID)
		if err != nil {
			return source, err
		}
		if job.Scope.OwnerID != spec.Task || job.Scope.EpicID != spec.EpicID {
			return source, effects.ErrAuthority
		}
		source.Input.Target = effects.ControlTarget{WorkID: job.Scope.WorkID, JobID: job.ID, Round: job.Round}
	case *toolcontract.ControlInput:
		if source.Input.Operation != "file_write" {
			return source, ErrCodexIntegration
		}
		ix, err := store.ReadIndex(brain)
		if err != nil {
			return source, err
		}
		targetID := p.Target
		if targetID == "" {
			targetID = spec.Task
		}
		target, err := ResolveCoordinatorTarget(ix, spec, targetID)
		if err != nil {
			return source, err
		}
		path, err := controlrecord.ResolveCoordinatorOutput(brain, target, spec, p.Path, true)
		if err != nil {
			return source, err
		}
		source.Input.Target.Path = path
		if target.Kind == blueprint.MachineWork {
			source.Input.Target.WorkID = targetID
		}
	default:
		return source, ErrCodexIntegration
	}
	return source, nil
}

func currentControlIngressLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter, verify nativeAcceptance) (helper.ControlIngressSource, toolcontract.Input, error) {
	var source helper.ControlIngressSource
	original, err := controlrecord.ReadOriginal(brain, originalRef)
	if err != nil {
		return source, nil, err
	}
	if original.Control == nil || original.Helper != nil || original.Tool != toolcontract.FactoryControl {
		return source, nil, effects.ErrAuthority
	}
	spec, err := codex.ReadAttemptControlLocal(ctx, brain, *original.Control)
	if err != nil {
		return source, nil, err
	}
	authority, _, err := nativeAuthority(ctx, brain, spec.EpicID, spec.Task, true, nil, verify)
	if err != nil {
		return source, nil, err
	}
	_, input, err := controlrecord.DecodeInput(brain, originalRef)
	if err != nil {
		return source, nil, err
	}
	source = helper.ControlIngressSource{Schema: 2, Original: originalRef, Control: *original.Control, Input: effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256}, Authority: authority, Spec: spec, Writer: writer}
	if writer.PID != os.Getpid() || writer.Group <= 0 || writer.Birth == "" {
		return source, nil, effects.ErrAuthority
	}
	return source, input, nil
}

func (a NativeAuthority) ValidateControlIngress(ctx context.Context, source helper.ControlIngressSource) error {
	return a.validateControlIngress(ctx, source, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) validateControlIngress(ctx context.Context, source helper.ControlIngressSource, verify nativeAcceptance) error {
	original, err := controlrecord.ReadOriginal(a.Brain, source.Original)
	if err != nil {
		return err
	}
	if source.Schema != 2 || original.Control == nil || source.Control != *original.Control || source.Input.Operation != original.Operation || source.Input.PayloadSHA256 != original.PayloadSHA256 || source.Writer.PID <= 1 || source.Writer.Group <= 0 || source.Writer.Birth == "" {
		return effects.ErrEvidence
	}
	spec, err := codex.ReadAttemptControlLocal(ctx, a.Brain, source.Control)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(spec, source.Spec) || spec.Task != source.Authority.OwnerID || spec.EpicID != source.Authority.EpicID {
		return effects.ErrAuthority
	}
	_, _, err = nativeAuthority(ctx, a.Brain, source.Authority.EpicID, source.Authority.OwnerID, true, &source.Authority, verify)
	return err
}

func CaptureControlOutputIngressLocked(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter) (controlrecord.ImmutableRef, helper.ControlIngressSource, error) {
	return captureControlOutputIngressLocked(ctx, brain, original, writer, codex.Contract2AcceptanceAuthority)
}
func captureControlOutputIngressLocked(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, writer controlrecord.LocalWriter, verify nativeAcceptance) (controlrecord.ImmutableRef, helper.ControlIngressSource, error) {
	source, err := buildControlIngressLocked(ctx, brain, original, writer, verify)
	if err != nil {
		return controlrecord.ImmutableRef{}, source, err
	}
	_, decoded, err := controlrecord.DecodeInput(brain, original)
	if err != nil {
		return controlrecord.ImmutableRef{}, source, err
	}
	p, ok := decoded.(*toolcontract.ControlInput)
	if !ok || source.Input.Operation != "file_write" {
		return controlrecord.ImmutableRef{}, source, effects.ErrEvidence
	}
	targetID := p.Target
	if targetID == "" {
		targetID = source.Spec.Task
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return controlrecord.ImmutableRef{}, source, err
	}
	target, err := ResolveCoordinatorTarget(ix, source.Spec, targetID)
	if err != nil {
		return controlrecord.ImmutableRef{}, source, err
	}
	data, err := os.ReadFile(blueprint.Path(brain))
	if err != nil {
		return controlrecord.ImmutableRef{}, source, err
	}
	frozen := coordinatorBlueprintSnapshot{Schema: 2, Bytes: data}
	blueprintRef := controlrecord.ImmutableRef{Path: outputSourcePath(brain, original, "blueprint"), SHA256: effectDigest(frozen)}
	if err := controlSourceWriteImmutable(blueprintRef.Path, frozen); err != nil {
		return controlrecord.ImmutableRef{}, source, err
	}
	input := helper.ControlOutputIngress{Schema: 2, Source: source, Target: target, TargetID: targetID, OutputName: p.Path, Blueprint: blueprintRef}
	if target.Kind == blueprint.MachineWork {
		scope, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, targetID, verify)
		if err != nil {
			return controlrecord.ImmutableRef{}, source, err
		}
		scope = scope.Authorization()
		work, _, err := nativeAuthority(ctx, brain, scope.EpicID, scope.WorkID, true, nil, verify)
		if err != nil {
			return controlrecord.ImmutableRef{}, source, err
		}
		input.WorkScope, input.WorkAuthority = &scope, &work
		if err := validateCoordinatorOutputWorkLocal(ctx, brain, input.Source.Authority, target, targetID, input.WorkScope, input.WorkAuthority, verify); err != nil {
			return controlrecord.ImmutableRef{}, source, err
		}
	}
	service := helper.Service{Brain: brain, Authority: controlOutputIngressAuthority{NativeAuthority{brain}, verify}}
	ref, err := service.CaptureControlOutputIngressLocked(ctx, input)
	return ref, source, err
}

type controlOutputIngressAuthority struct {
	NativeAuthority
	verify nativeAcceptance
}

func (a controlOutputIngressAuthority) ValidateControlOutputIngress(ctx context.Context, input helper.ControlOutputIngress) error {
	return a.validateControlOutputIngress(ctx, input, a.verify)
}

func (a NativeAuthority) ValidateControlOutputIngress(ctx context.Context, input helper.ControlOutputIngress) error {
	return a.validateControlOutputIngress(ctx, input, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) validateControlOutputIngress(ctx context.Context, input helper.ControlOutputIngress, verify nativeAcceptance) error {
	if input.Schema != 2 {
		return effects.ErrEvidence
	}
	if err := a.validateControlIngress(ctx, input.Source, verify); err != nil {
		return err
	}
	original, decoded, err := controlrecord.DecodeInput(a.Brain, input.Source.Original)
	if err != nil {
		return err
	}
	p, ok := decoded.(*toolcontract.ControlInput)
	if !ok || original.Operation != "file_write" || input.OutputName != p.Path {
		return effects.ErrEvidence
	}
	targetID := p.Target
	if targetID == "" {
		targetID = input.Source.Spec.Task
	}
	if targetID != input.TargetID {
		return effects.ErrEvidence
	}
	ix, err := store.ReadIndex(a.Brain)
	if err != nil {
		return err
	}
	target, err := ResolveCoordinatorTarget(ix, input.Source.Spec, input.TargetID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(target, input.Target) {
		return effects.ErrAuthority
	}
	path, err := controlrecord.ResolveCoordinatorOutput(a.Brain, target, input.Source.Spec, input.OutputName, true)
	if err != nil || path != input.Source.Input.Target.Path {
		return effects.ErrAuthority
	}
	workID := ""
	if target.Kind == blueprint.MachineWork {
		workID = targetID
	}
	if input.Source.Input.Target.WorkID != workID {
		return effects.ErrEvidence
	}
	if err := validateCoordinatorOutputWorkLocal(ctx, a.Brain, input.Source.Authority, target, input.TargetID, input.WorkScope, input.WorkAuthority, verify); err != nil {
		return err
	}
	if target.Kind == blueprint.MachineWork {
		if err := a.validateCoordinatorOutputCurrentWork(ctx, input.Source.Spec, target, input.TargetID, input.WorkScope, input.WorkAuthority, verify); err != nil {
			return err
		}
	}

	var frozen coordinatorBlueprintSnapshot
	if input.Blueprint.Path != outputSourcePath(a.Brain, input.Source.Original, "blueprint") || controlSourceReadJSON(input.Blueprint.Path, &frozen) != nil || frozen.Schema != 2 || effectDigest(frozen) != input.Blueprint.SHA256 {
		return effects.ErrEvidence
	}
	b, err := blueprint.Parse(frozen.Bytes)
	if err != nil {
		return err
	}
	declared, err := controlrecord.ResolveCoordinatorOutputBlueprint(b, target, input.Source.Spec, input.OutputName, true)
	if err != nil || declared != path {
		return effects.ErrEvidence
	}
	return nil
}

// The receiving command fills Writer from its self observation made before tick.
func BuildControlOutputLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, control *effects.ControlReceiptRef) (helper.ControlOutputRequest, error) {
	return buildControlOutputLocked(ctx, brain, originalRef, control, codex.Contract2AcceptanceAuthority)
}
func buildControlOutputLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, control *effects.ControlReceiptRef, verify nativeAcceptance) (helper.ControlOutputRequest, error) {
	var req helper.ControlOutputRequest
	original, err := controlrecord.ReadOriginal(brain, originalRef)
	if err != nil {
		return req, err
	}
	if original.Control != nil {
		if control == nil || control.Control != *original.Control || original.Tool != toolcontract.FactoryControl || original.Operation != "file_write" {
			return req, effects.ErrAuthority
		}
		_, pre, err := helper.FindControlOutputIngressLocal(brain, originalRef)
		if err != nil {
			return req, err
		}
		if pre.Source.Control != control.Control {
			return req, effects.ErrEvidence
		}
		if err := (NativeAuthority{brain}).validateControlOutputIngress(ctx, pre, verify); err != nil {
			return req, err
		}
		input, err := (effects.Service{Brain: brain}).ReadControlReference(ctx, *control)
		if err != nil {
			return req, err
		}
		if !reflect.DeepEqual(input, pre.Source.Input) {
			return req, effects.ErrEvidence
		}
		_, decoded, err := controlrecord.DecodeInput(brain, originalRef)
		if err != nil {
			return req, err
		}
		p, ok := decoded.(*toolcontract.ControlInput)
		if !ok {
			return req, effects.ErrEvidence
		}
		spec, authority, targetID, target, blueprintRef := pre.Source.Spec, pre.Source.Authority, pre.TargetID, pre.Target, pre.Blueprint
		path := input.Target.Path
		caller := helper.CoordinatorOutputCaller{WorkScope: pre.WorkScope, WorkAuthority: pre.WorkAuthority, Writer: pre.Source.Writer, Authority: authority, Control: *control, Spec: spec, TargetID: targetID, OutputName: p.Path, Blueprint: blueprintRef}
		snapshot := coordinatorOutputSnapshot{Schema: 2, Original: originalRef, Caller: caller, Input: input, Target: target}
		enrollment := controlrecord.ImmutableRef{Path: outputSourcePath(brain, originalRef, "enrollment"), SHA256: effectDigest(snapshot)}
		if err := controlSourceWriteImmutable(enrollment.Path, snapshot); err != nil {
			return req, err
		}
		caller.Enrollment = enrollment
		return helper.ControlOutputRequest{Writer: pre.Source.Writer, Original: originalRef, Caller: helper.ControlOutputCaller{Coordinator: &caller}, Path: path, Text: p.Text, Append: p.Append}, nil
	}
	if control != nil || original.Helper == nil || original.Tool != toolcontract.ManagedFiles {
		return req, effects.ErrAuthority
	}
	call := original.Helper
	if err := codex.ValidateHelperControlCall(brain, call.JobID, call.Generation, call.Round, call.ThreadID, call.TurnID); err != nil {
		return req, err
	}
	service := helper.Service{Brain: brain, Authority: controlReadHelperAuthority{NativeAuthority{brain}, verify}}
	job, path, err := service.ControlOutputTargetLocked(ctx, *call)
	if err != nil {
		return req, err
	}
	if job.Scope.CodexContract != 2 {
		return req, effects.ErrAuthority
	}
	_, input, err := controlrecord.DecodeInput(brain, originalRef)
	if err != nil {
		return req, err
	}
	p, ok := input.(*toolcontract.FileInput)
	if !ok || p.Text == nil || (original.Operation != "write" && original.Operation != "append") {
		return req, effects.ErrEvidence
	}
	if p.Path != path && p.Path != filepath.Base(path) {
		return req, effects.ErrAuthority
	}
	caller := helper.HelperOutputCaller{Scope: job.Scope, JobID: job.ID, Generation: job.Generation, Round: job.Round, ThreadID: call.ThreadID, TurnID: call.TurnID, CallID: call.CallID}
	authority, _, err := nativeAuthority(ctx, brain, job.Scope.EpicID, job.Scope.OwnerID, true, nil, verify)
	if err != nil {
		return req, err
	}
	workAuthority, _, err := nativeAuthority(ctx, brain, job.Scope.EpicID, job.Scope.WorkID, true, nil, verify)
	if err != nil {
		return req, err
	}
	snapshot := helperOutputSnapshot{WorkAuthority: workAuthority, Schema: 2, Original: originalRef, Caller: caller, Job: job, Authority: authority, Path: path}
	caller.Source = controlrecord.ImmutableRef{Path: outputSourcePath(brain, originalRef, "helper"), SHA256: effectDigest(snapshot)}
	if err := controlSourceWriteImmutable(caller.Source.Path, snapshot); err != nil {
		return req, err
	}
	return helper.ControlOutputRequest{Original: originalRef, Caller: helper.ControlOutputCaller{Helper: &caller}, Path: path, Text: *p.Text, Append: original.Operation == "append"}, nil
}

func (a NativeAuthority) ValidateControlOutputRecovery(ctx context.Context, p helper.ControlOutputPrepared) error {
	return a.validateControlOutputRecovery(ctx, p, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) validateControlOutputRecovery(ctx context.Context, p helper.ControlOutputPrepared, verify nativeAcceptance) error {
	if (p.Caller.Coordinator == nil) == (p.Caller.Helper == nil) {
		return effects.ErrAuthority
	}
	if c := p.Caller.Coordinator; c != nil {
		if err := a.validateCoordinatorOutput(ctx, *c, p.Original, p.Path, true, verify); err != nil {
			return err
		}
		if _, _, err := nativeOutputRecoveryAuthority(ctx, a.Brain, c.Authority, verify); err != nil {
			return err
		}
		if c.WorkAuthority != nil {
			_, work, err := nativeOutputRecoveryAuthority(ctx, a.Brain, *c.WorkAuthority, verify)
			if err != nil {
				return err
			}
			if c.WorkScope == nil || work.WorkGeneration != c.WorkScope.Generation {
				return effects.ErrAuthority
			}
			if c.WorkScope.OwnerID == c.WorkScope.WorkID {
				if *c.WorkAuthority != c.Authority || work.OwnerActivation != nil {
					return effects.ErrAuthority
				}
			} else if work.OwnerActivation == nil || *work.OwnerActivation != c.Authority.Activation {
				return effects.ErrAuthority
			}
		}

		ix, err := store.ReadIndex(a.Brain)
		if err != nil {
			return err
		}
		_, err = ResolveCoordinatorTarget(ix, c.Spec, c.TargetID)
		return err
	}
	saved, err := readHelperOutputSnapshotLocal(a.Brain, p)
	if err != nil {
		return err
	}
	c := p.Caller.Helper
	if _, _, err := nativeOutputRecoveryAuthority(ctx, a.Brain, saved.Authority, verify); err != nil {
		return err
	}
	if saved.WorkAuthority.EpicID != saved.Job.Scope.EpicID || saved.WorkAuthority.OwnerID != saved.Job.Scope.WorkID {
		return effects.ErrEvidence
	}
	_, work, err := nativeOutputRecoveryAuthority(ctx, a.Brain, saved.WorkAuthority, verify)
	if err != nil {
		return err
	}
	if work.WorkGeneration != saved.Job.Scope.Generation {
		return effects.ErrAuthority
	}
	if c.Scope.OwnerID == c.Scope.WorkID {
		if saved.WorkAuthority != saved.Authority || work.OwnerActivation != nil {
			return effects.ErrAuthority
		}
	} else if work.OwnerActivation == nil || *work.OwnerActivation != saved.Authority.Activation {
		return effects.ErrAuthority
	}
	return nil
}

func (a NativeAuthority) ValidateCoordinatorOutput(ctx context.Context, c helper.CoordinatorOutputCaller, original controlrecord.OriginalRequestRef, path string) error {
	return a.validateCoordinatorOutput(ctx, c, original, path, false, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) ValidateRetainedCoordinatorOutput(ctx context.Context, c helper.CoordinatorOutputCaller, original controlrecord.OriginalRequestRef, path string) error {
	return a.validateCoordinatorOutput(ctx, c, original, path, true, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) validateCoordinatorOutput(ctx context.Context, c helper.CoordinatorOutputCaller, originalRef controlrecord.OriginalRequestRef, path string, retained bool, verify nativeAcceptance) error {
	original, input, err := controlrecord.DecodeInput(a.Brain, originalRef)
	if err != nil {
		return err
	}
	p, ok := input.(*toolcontract.ControlInput)
	if !ok || original.Control == nil || *original.Control != c.Control.Control || original.Operation != "file_write" || original.Tool != toolcontract.FactoryControl || c.Spec.Task != c.Authority.OwnerID || c.Spec.EpicID != c.Authority.EpicID || c.OutputName != p.Path {
		return effects.ErrEvidence
	}
	_, ingress, err := helper.FindControlOutputIngressLocal(a.Brain, originalRef)
	if err != nil {
		return err
	}
	if ingress.Source.Writer != c.Writer || !reflect.DeepEqual(ingress.WorkScope, c.WorkScope) || !reflect.DeepEqual(ingress.WorkAuthority, c.WorkAuthority) {
		return effects.ErrEvidence
	}
	var snapshot coordinatorOutputSnapshot
	if c.Enrollment.Path != outputSourcePath(a.Brain, originalRef, "enrollment") || controlSourceReadJSON(c.Enrollment.Path, &snapshot) != nil || effectDigest(snapshot) != c.Enrollment.SHA256 {
		return effects.ErrEvidence
	}
	withoutRef := c
	withoutRef.Enrollment = controlrecord.ImmutableRef{}
	if snapshot.Schema != 2 || snapshot.Original != originalRef || !reflect.DeepEqual(snapshot.Caller, withoutRef) || snapshot.Input.Operation != original.Operation || snapshot.Input.PayloadSHA256 != original.PayloadSHA256 || snapshot.Input.Target.Path != path {
		return effects.ErrEvidence
	}
	var frozen coordinatorBlueprintSnapshot
	if c.Blueprint.Path != outputSourcePath(a.Brain, originalRef, "blueprint") || controlSourceReadJSON(c.Blueprint.Path, &frozen) != nil || effectDigest(frozen) != c.Blueprint.SHA256 || frozen.Schema != 2 {
		return effects.ErrEvidence
	}
	b, err := blueprint.Parse(frozen.Bytes)
	if err != nil {
		return err
	}
	resolved, err := controlrecord.ResolveCoordinatorOutputBlueprint(b, snapshot.Target, c.Spec, c.OutputName, true)
	if err != nil || resolved != path {
		return effects.ErrEvidence
	}
	if _, _, err := nativeAuthority(ctx, a.Brain, c.Authority.EpicID, c.Authority.OwnerID, !retained, &c.Authority, verify); err != nil {
		return err
	}
	if err := validateCoordinatorOutputWorkLocal(ctx, a.Brain, c.Authority, snapshot.Target, c.TargetID, c.WorkScope, c.WorkAuthority, verify); err != nil {
		return err
	}
	if !retained && snapshot.Target.Kind == blueprint.MachineWork {
		if err := a.validateCoordinatorOutputCurrentWork(ctx, c.Spec, snapshot.Target, c.TargetID, c.WorkScope, c.WorkAuthority, verify); err != nil {
			return err
		}
	}

	if retained {
		return nil
	}
	// Physical liveness was observed before the caller entered tick/helper locks.
	// This bounded check retains the exact logical descriptor and output policy.
	current, err := blueprint.Load(blueprint.Path(a.Brain))
	if err != nil {
		return err
	}
	resolved, err = controlrecord.ResolveCoordinatorOutputBlueprint(current, snapshot.Target, c.Spec, c.OutputName, true)
	if err != nil || resolved != path {
		return effects.ErrAuthority
	}
	return CoordinatorAuthority(a.Brain, c.Spec)
}

func ResolveCoordinatorTarget(ix store.Index, spec session.Spec, id string) (store.Entry, error) {
	target, err := ix.Lookup(id)
	if err != nil {
		return target, err
	}
	if target.Epic != spec.EpicID {
		return target, fmt.Errorf("target belongs to another epic")
	}
	if id == spec.Task {
		return target, nil
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil {
		return target, err
	}
	if owner.Kind != blueprint.MachineSet || target.Kind != blueprint.MachineWork {
		return target, fmt.Errorf("target belongs to another coordinator")
	}
	var work WorkInputs
	if err := store.ReadInputs(target.Dir, &work); err != nil {
		return target, err
	}
	if work.Set != spec.Task {
		return target, fmt.Errorf("work belongs to another set")
	}
	lease, err := Lease(owner.Dir)
	if err != nil {
		return target, err
	}
	if !strings.Contains(spec.Generation, ":"+strconv.Itoa(lease)+":") {
		return target, fmt.Errorf("coordinator lease changed")
	}
	evs, err := events.Read(store.EventsPath(target.Dir))
	if err != nil {
		return target, err
	}
	handed := false
	for _, ev := range evs {
		if ev.Kind == events.KindHandover {
			handed = true
		}
		if ev.Kind == events.KindHandback {
			handed = false
		}
	}
	if handed {
		return target, fmt.Errorf("work handed over")
	}
	return target, nil
}

var _ helper.ControlOutputAuthority = NativeAuthority{}
var _ helper.ControlOutputWriterAuthority = NativeAuthority{}
var _ helper.ControlOutputRecoveryAuthority = NativeAuthority{}
var _ helper.ControlIngressAuthority = NativeAuthority{}

var _ helper.ControlOutputIngressAuthority = NativeAuthority{}

func (a NativeAuthority) SealedAttempt(ctx context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
	gate, err := codex.NewAttemptJournalGate(a.Brain)
	if err != nil {
		return effects.AttemptSeal{}, err
	}
	return gate.SealedAttempt(ctx, key)
}

func (a NativeAuthority) ValidateManagedControlCallerLocal(ctx context.Context, job helper.Job, call controlrecord.HelperCall) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return codex.ValidateHelperAttemptCallLocal(a.Brain, job, call)
}

func readHelperOutputSnapshotLocal(brain string, p helper.ControlOutputPrepared) (helperOutputSnapshot, error) {
	var saved helperOutputSnapshot
	c := p.Caller.Helper
	if c == nil {
		return saved, effects.ErrEvidence
	}
	if c.Source.Path != outputSourcePath(brain, p.Original, "helper") || c.Source.SHA256 == (effects.Digest{}) {
		return saved, effects.ErrEvidence
	}
	if err := controlSourceReadJSON(c.Source.Path, &saved); err != nil {
		return saved, err
	}
	if effectDigest(saved) != c.Source.SHA256 {
		return saved, effects.ErrEvidence
	}
	withoutSource := *c
	withoutSource.Source = controlrecord.ImmutableRef{}
	if saved.Schema != 2 || saved.Original != p.Original || saved.Caller != withoutSource || saved.Path != p.Path || saved.Job.Scope != c.Scope || saved.Job.ID != c.JobID || saved.Job.Generation != c.Generation || saved.Job.Round != c.Round {
		return saved, effects.ErrEvidence
	}
	original, err := controlrecord.ReadOriginal(brain, p.Original)
	if err != nil {
		return saved, err
	}
	if original.Helper == nil || *original.Helper != (controlrecord.HelperCall{JobID: c.JobID, Generation: c.Generation, Round: c.Round, ThreadID: c.ThreadID, TurnID: c.TurnID, CallID: c.CallID}) {
		return saved, effects.ErrEvidence
	}
	return saved, nil
}

func validateCoordinatorOutputWorkLocal(ctx context.Context, brain string, owner OwnerAuthority, target store.Entry, targetID string, scope *helper.Scope, work *OwnerAuthority, verify nativeAcceptance) error {
	if target.Kind != blueprint.MachineWork {
		if scope != nil || work != nil {
			return effects.ErrEvidence
		}
		return nil
	}
	if scope == nil || work == nil {
		return fmt.Errorf("%w: pending coordinator output lacks captured work scope", ErrCodexIntegration)
	}
	if scope.WorkID != targetID || scope.Authorization() != *scope {
		return effects.ErrEvidence
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	epic, err := index.Lookup(scope.EpicID)
	if err != nil {
		return err
	}
	if scope.OutputRoot != epic.Dir {
		return effects.ErrEvidence
	}
	return validateWorkControlAuthorities(ctx, brain, *scope, owner, *work, verify)
}

func (a NativeAuthority) validateCoordinatorOutputCurrentWork(ctx context.Context, spec session.Spec, target store.Entry, targetID string, scope *helper.Scope, work *OwnerAuthority, verify nativeAcceptance) error {
	if scope == nil || work == nil {
		return effects.ErrAuthority
	}
	fresh, err := a.refreshControlReadScope(ctx, targetID, verify)
	if err != nil {
		return err
	}
	if fresh.Authorization() != scope.Authorization() {
		return effects.ErrAuthority
	}
	if _, _, err := nativeAuthority(ctx, a.Brain, work.EpicID, work.OwnerID, true, work, verify); err != nil {
		return err
	}
	var inputs WorkInputs
	if err := store.ReadInputs(target.Dir, &inputs); err != nil {
		return err
	}
	_, err = controlReadRepository(a.Brain, spec, &fresh, toolcontract.GitHubReadInput{Repo: inputs.Repo, Base: inputs.Base})
	return err
}
