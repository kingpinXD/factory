package reconcile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func controlSourceReadJSON(name string, value any) error {
	return effectReadJSONBounded(name, value, controlSourceSnapshotLimit)
}

func controlSourceWriteImmutable(name string, value any) error {
	return effectWriteImmutableBounded(name, value, controlSourceSnapshotLimit)
}

type ControlReadPurpose string

const (
	ControlReadDirect       ControlReadPurpose = "direct"
	ControlReadActor        ControlReadPurpose = "actor"
	ControlReadIssue        ControlReadPurpose = "issue"
	ControlReadComments     ControlReadPurpose = "comments"
	ControlReadParent       ControlReadPurpose = "parent"
	ControlReadOwnedPR      ControlReadPurpose = "owned_pr"
	ControlReadOwnedThreads ControlReadPurpose = "owned_threads"
	ControlReadOwnedReviews ControlReadPurpose = "owned_reviews"
)

type ControlReadRequest struct {
	Original controlrecord.OriginalRequestRef
	Control  *effects.ControlReceiptRef
	Helper   *controlrecord.HelperCall
	Purpose  ControlReadPurpose
}

type controlReadSnapshot struct {
	Schema    int                              `json:"schema"`
	Original  controlrecord.OriginalRequestRef `json:"original"`
	Admission ProgramReadAdmission             `json:"admission"`
	Spec      *session.Spec                    `json:"spec,omitempty"`
	Job       *helper.Job                      `json:"job,omitempty"`
}

func controlReadSnapshotPath(brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose) string {
	return filepath.Join(brain, "factory", "effects", "control-reads", effectDigest(struct {
		Original controlrecord.OriginalRequestRef
		Purpose  ControlReadPurpose
	}{original, purpose}).String()+".json")
}

func buildControlReadLocked(ctx context.Context, brain string, req ControlReadRequest, verify nativeAcceptance) (ProgramReadAdmission, error) {
	var out ProgramReadAdmission
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if (req.Control == nil) == (req.Helper == nil) {
		return out, effects.ErrAuthority
	}
	original, err := controlrecord.ReadOriginal(brain, req.Original)
	if err != nil {
		return out, err
	}
	if req.Purpose != ControlReadDirect {
		return out, fmt.Errorf("%w: selected prerequisite source producer required for %s", ErrCodexIntegration, req.Purpose)
	}
	var pre ControlReadPreSource
	preName := controlReadPreSourcePath(brain, req.Original, req.Purpose)
	if err := controlSourceReadJSON(preName, &pre); err == nil {
		ref := controlrecord.ImmutableRef{Path: preName, SHA256: effectDigest(pre)}
		if req.Control != nil {
			return bindCoordinatorReadSourceLocked(ctx, brain, ref, *req.Control, verify)
		}
		if req.Helper == nil || original.Helper == nil || *req.Helper != *original.Helper {
			return out, effects.ErrAuthority
		}
		return bindHelperReadSourceLocked(ctx, brain, ref, verify)
	} else if !errors.Is(err, os.ErrNotExist) {
		return out, err
	}
	out = ProgramReadAdmission{Purpose: req.Purpose, Operation: original.Operation}
	snapshot := controlReadSnapshot{Schema: 2, Original: req.Original}
	if req.Control != nil {
		id := req.Control.Control
		if original.Control == nil || *original.Control != id || original.Helper != nil || original.Tool != toolcontract.FactoryControl {
			return out, effects.ErrAuthority
		}
		ref := codex.AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID}
		spec, err := codex.AttemptControlAuthority(brain, ref)
		if err != nil {
			return out, err
		}
		if err := codex.ValidateAttemptControlCall(brain, ref, id.ThreadID, id.TurnID); err != nil {
			return out, err
		}
		if err := CoordinatorAuthority(brain, spec); err != nil {
			return out, err
		}
		actual, err := (effects.Service{Brain: brain}).ReadControlReference(ctx, *req.Control)
		if err != nil {
			return out, err
		}
		if actual.Operation != original.Operation || actual.PayloadSHA256 != original.PayloadSHA256 {
			return out, effects.ErrEvidence
		}
		out.Authority, _, err = nativeAuthority(ctx, brain, spec.EpicID, spec.Task, true, nil, verify)
		if err != nil {
			return out, err
		}
		out.Control = &ProgramReadControlSource{Receipt: *req.Control, Input: actual, Original: req.Original}
		if actual.Target.WorkID != "" {
			scope, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, actual.Target.WorkID, verify)
			if err != nil {
				return out, err
			}
			if scope.OwnerID != spec.Task || scope.EpicID != spec.EpicID {
				return out, effects.ErrAuthority
			}
			out.Work = &scope
		}
		snapshot.Spec = &spec
	} else {
		call := req.Helper
		if original.Helper == nil || *original.Helper != *call || original.Control != nil || original.Tool != toolcontract.FactoryHelper {
			return out, effects.ErrAuthority
		}
		if err := codex.ValidateHelperControlCall(brain, call.JobID, call.Generation, call.Round, call.ThreadID, call.TurnID); err != nil {
			return out, err
		}
		service := helper.Service{Brain: brain, Authority: controlReadHelperAuthority{NativeAuthority{brain}, verify}}
		job, err := service.ManagedAdmission(ctx, call.JobID, call.Generation)
		if err != nil {
			return out, err
		}
		if job.Scope.CodexContract != 2 || job.Round != call.Round || job.ConversationID != call.ThreadID || job.TurnID != call.TurnID {
			return out, effects.ErrAuthority
		}
		out.Authority, _, err = nativeAuthority(ctx, brain, job.Scope.EpicID, job.Scope.OwnerID, true, nil, verify)
		if err != nil {
			return out, err
		}
		scope := job.Scope.Authorization()
		out.Work = &scope
		out.Helper = &ProgramReadHelperSource{JobID: call.JobID, Generation: call.Generation, Round: call.Round, ThreadID: call.ThreadID, TurnID: call.TurnID, CallID: call.CallID, Original: req.Original, Input: effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: scope.WorkID, JobID: call.JobID, Round: call.Round}}}
		snapshot.Job = &job
	}
	_, input, err := controlrecord.DecodeInput(brain, req.Original)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	read, err := controlDirectRead(original.Operation, input)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	spec := session.Spec{Task: out.Authority.OwnerID, EpicID: out.Authority.EpicID}
	if snapshot.Spec != nil {
		spec = *snapshot.Spec
	}
	out.Input, err = controlReadRepository(brain, spec, out.Work, read)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	snapshot.Admission = out
	if err := controlSourceWriteImmutable(controlReadSnapshotPath(brain, req.Original, req.Purpose), snapshot); err != nil {
		return ProgramReadAdmission{}, err
	}
	return out, nil
}

func (a NativeAuthority) refreshControlReadScope(ctx context.Context, id string, verify nativeAcceptance) (helper.Scope, error) {
	ix, err := store.ReadIndex(a.Brain)
	if err != nil {
		return helper.Scope{}, err
	}
	entry, err := ix.Lookup(id)
	if err != nil {
		return helper.Scope{}, err
	}
	it, err := loadEntity(id, entry)
	if err != nil || it.work == nil {
		return helper.Scope{}, effects.ErrAuthority
	}
	req := helper.Request{WorkID: id, Component: "implementor", OwnerID: id}
	if !handedOver(it) {
		set, err := ix.Lookup(it.work.Set)
		if err != nil {
			return helper.Scope{}, err
		}
		lease, err := Lease(set.Dir)
		if err != nil {
			return helper.Scope{}, err
		}
		req.OwnerID, req.SetID, req.Lease = it.work.Set, it.work.Set, lease
	}
	return a.resolveQualified(ctx, req, false, false, true, false, verify)
}

func controlDirectRead(op string, input toolcontract.Input) (toolcontract.GitHubReadInput, error) {
	switch p := input.(type) {
	case *toolcontract.GitHubReadInput:
		if strings.HasPrefix(op, "github_") {
			return *p, nil
		}
	case *toolcontract.IssueInput:
		if op == "issue_read" || op == "issue_children" {
			return toolcontract.GitHubReadInput{Repo: p.Repo, Base: p.Base, Number: p.Number}, nil
		}
	}
	return toolcontract.GitHubReadInput{}, effects.ErrAuthority
}

func controlReadRepository(brain string, spec session.Spec, scope *helper.Scope, input toolcontract.GitHubReadInput) (toolcontract.GitHubReadInput, error) {
	registered, err := repos.ByFullName(brain, input.Repo)
	if err != nil {
		return input, err
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return input, err
	}
	if slices.Contains(b.Deny.Repos, registered.Name) || slices.Contains(b.Deny.Repos, registered.FullName) {
		return input, effects.ErrAuthority
	}
	base := input.Base
	if base == "" {
		base = registered.Default
	}
	if !slices.Contains(registered.Branches, base) {
		return input, effects.ErrAuthority
	}
	for _, branch := range []string{registered.Default, base} {
		if slices.Contains(b.Deny.Branches, blueprint.RepoBranch{Repo: registered.Name, Branch: branch}) {
			return input, effects.ErrAuthority
		}
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return input, err
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil || owner.Epic != spec.EpicID {
		return input, effects.ErrAuthority
	}
	allowed := scope == nil && owner.Kind == blueprint.MachineEpic && spec.Task == spec.EpicID && spec.Name == "factory:"+spec.EpicID+":planner"
	for id, entry := range ix {
		if entry.Kind != blueprint.MachineWork || entry.Epic != spec.EpicID || scope != nil && id != scope.WorkID {
			continue
		}
		var work WorkInputs
		if err := store.ReadInputs(entry.Dir, &work); err != nil {
			return input, err
		}
		if (id == spec.Task || work.Set == spec.Task) && work.Repo == registered.FullName && work.Base == base {
			allowed = true
		}
	}
	if !allowed {
		return input, effects.ErrAuthority
	}
	input.Repo, input.Base = registered.FullName, base
	return input, nil
}

func validateControlReadLocal(ctx context.Context, brain string, a ProgramReadAdmission, verify nativeAcceptance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if (a.Control == nil) == (a.Helper == nil) || a.RepoRequest != nil || a.Purpose != ControlReadDirect {
		return effects.ErrEvidence
	}
	var originalRef controlrecord.OriginalRequestRef
	if a.Control != nil {
		originalRef = a.Control.Original
	} else {
		originalRef = a.Helper.Original
	}
	original, input, err := controlrecord.DecodeInput(brain, originalRef)
	if err != nil {
		return err
	}
	read, err := controlDirectRead(original.Operation, input)
	if err != nil {
		return err
	}
	var snapshot controlReadSnapshot
	if err := controlSourceReadJSON(controlReadSnapshotPath(brain, originalRef, a.Purpose), &snapshot); err != nil {
		return err
	}
	if snapshot.Schema != 2 || snapshot.Original != originalRef || !reflect.DeepEqual(snapshot.Admission, a) || a.Operation != original.Operation || read.Number != a.Input.Number || read.Path != a.Input.Path || read.SHA != a.Input.SHA || !read.Since.Equal(a.Input.Since) {
		return effects.ErrEvidence
	}
	if _, _, err := nativeAuthority(ctx, brain, a.Authority.EpicID, a.Authority.OwnerID, false, &a.Authority, verify); err != nil {
		return err
	}
	if a.Work != nil && (a.Work.CodexContract != 2 || a.Work.EpicID != a.Authority.EpicID || a.Work.OwnerID != a.Authority.OwnerID) {
		return effects.ErrAuthority
	}
	if a.Control != nil {
		if original.Control == nil || *original.Control != a.Control.Receipt.Control || original.Helper != nil || snapshot.Spec == nil || snapshot.Job != nil || snapshot.Spec.Task != a.Authority.OwnerID || snapshot.Spec.EpicID != a.Authority.EpicID || a.Control.Input.Operation != original.Operation || a.Control.Input.PayloadSHA256 != original.PayloadSHA256 || (a.Control.Input.Target.WorkID == "") != (a.Work == nil) || a.Work != nil && a.Work.WorkID != a.Control.Input.Target.WorkID {
			return effects.ErrEvidence
		}
	} else {
		call := original.Helper
		if call == nil || original.Control != nil || snapshot.Job == nil || snapshot.Spec != nil || call.JobID != a.Helper.JobID || call.Generation != a.Helper.Generation || call.Round != a.Helper.Round || call.ThreadID != a.Helper.ThreadID || call.TurnID != a.Helper.TurnID || call.CallID != a.Helper.CallID || a.Helper.Input.Operation != original.Operation || a.Helper.Input.PayloadSHA256 != original.PayloadSHA256 || a.Work == nil || snapshot.Job.Scope.Authorization() != a.Work.Authorization() {
			return effects.ErrEvidence
		}
	}
	return nil
}

type controlReadHelperAuthority struct {
	NativeAuthority
	verify nativeAcceptance
}

func (a controlReadHelperAuthority) Check(ctx context.Context, job helper.Job) error {
	return a.checkQualified(ctx, job, a.verify)
}

type CoordinatorReadPreSource struct {
	Ingress helper.ControlIngressSource  `json:"ingress"`
	Work    *helper.Scope                `json:"work,omitempty"`
	Input   toolcontract.GitHubReadInput `json:"input"`
}
type HelperReadPreSource struct {
	Caller    ProgramReadHelperSource      `json:"caller"`
	Authority OwnerAuthority               `json:"authority"`
	Job       helper.Job                   `json:"job"`
	Work      helper.Scope                 `json:"work"`
	Input     toolcontract.GitHubReadInput `json:"input"`
}
type ControlReadPreSource struct {
	Schema      int                              `json:"schema"`
	Original    controlrecord.OriginalRequestRef `json:"original"`
	Purpose     ControlReadPurpose               `json:"purpose"`
	Coordinator *CoordinatorReadPreSource        `json:"coordinator,omitempty"`
	Helper      *HelperReadPreSource             `json:"helper,omitempty"`
}

func controlReadPreSourcePath(brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose) string {
	return strings.TrimSuffix(controlReadSnapshotPath(brain, original, purpose), ".json") + ".pre.json"
}
func publishControlReadPreSource(ctx context.Context, brain string, pre ControlReadPreSource, verify nativeAcceptance) (controlrecord.ImmutableRef, error) {
	ref := controlrecord.ImmutableRef{Path: controlReadPreSourcePath(brain, pre.Original, pre.Purpose), SHA256: effectDigest(pre)}
	var existing ControlReadPreSource
	if err := controlSourceReadJSON(ref.Path, &existing); err == nil {
		oldRef := controlrecord.ImmutableRef{Path: ref.Path, SHA256: effectDigest(existing)}
		if _, _, err := readControlReadPreSourceLocal(ctx, brain, oldRef, verify); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		if pre.Coordinator != nil && existing.Coordinator != nil {
			pre.Coordinator.Ingress.Writer = existing.Coordinator.Ingress.Writer
		}
		if pre.Helper != nil && existing.Helper != nil {
			pre.Helper.Job.State = existing.Helper.Job.State
			pre.Helper.Job.HoldReason = existing.Helper.Job.HoldReason
		}
		if !reflect.DeepEqual(pre, existing) {
			return controlrecord.ImmutableRef{}, effects.ErrConflict
		}
		return oldRef, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return controlrecord.ImmutableRef{}, err
	}
	return ref, controlSourceWriteImmutable(ref.Path, pre)
}
func CaptureCoordinatorReadSourceLocked(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose, writer controlrecord.LocalWriter) (controlrecord.ImmutableRef, error) {
	return captureCoordinatorReadSourceLocked(ctx, brain, original, purpose, writer, codex.Contract2AcceptanceAuthority)
}
func captureCoordinatorReadSourceLocked(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose, writer controlrecord.LocalWriter, verify nativeAcceptance) (controlrecord.ImmutableRef, error) {
	if purpose != ControlReadDirect {
		return controlrecord.ImmutableRef{}, fmt.Errorf("%w: actual selected prerequisite source is required", ErrCodexIntegration)
	}
	source, input, err := currentControlIngressLocked(ctx, brain, original, writer, verify)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if err := CoordinatorAuthority(brain, source.Spec); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	read, err := controlDirectRead(source.Input.Operation, input)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	owner, err := ix.Lookup(source.Spec.Task)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	var work *helper.Scope
	if owner.Kind == blueprint.MachineWork {
		scope, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, source.Spec.Task, verify)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		if scope.OwnerID != source.Spec.Task || scope.EpicID != source.Spec.EpicID {
			return controlrecord.ImmutableRef{}, effects.ErrAuthority
		}
		work = &scope
		source.Input.Target.WorkID = scope.WorkID
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, source.Input.Target.WorkID); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	read, err = controlReadRepository(brain, source.Spec, work, read)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if err := codex.ValidateProgramReadInput(source.Input.Operation, read); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	return publishControlReadPreSource(ctx, brain, ControlReadPreSource{Schema: 2, Original: original, Purpose: purpose, Coordinator: &CoordinatorReadPreSource{Ingress: source, Work: work, Input: read}}, verify)
}

func CaptureHelperReadSourceLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, purpose ControlReadPurpose) (controlrecord.ImmutableRef, error) {
	return captureHelperReadSourceLocked(ctx, brain, originalRef, purpose, codex.Contract2AcceptanceAuthority)
}
func captureHelperReadSourceLocked(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, purpose ControlReadPurpose, verify nativeAcceptance) (controlrecord.ImmutableRef, error) {
	if purpose != ControlReadDirect {
		return controlrecord.ImmutableRef{}, fmt.Errorf("%w: actual selected prerequisite source is required", ErrCodexIntegration)
	}
	original, decoded, err := controlrecord.DecodeInput(brain, originalRef)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	call := original.Helper
	if call == nil || original.Control != nil || original.Tool != toolcontract.FactoryHelper {
		return controlrecord.ImmutableRef{}, effects.ErrAuthority
	}
	if err := codex.ValidateHelperControlCall(brain, call.JobID, call.Generation, call.Round, call.ThreadID, call.TurnID); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	read, err := controlDirectRead(original.Operation, decoded)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	service := helper.Service{Brain: brain, Authority: controlReadHelperAuthority{NativeAuthority{brain}, verify}}
	var pre HelperReadPreSource
	_, err = service.WithManagedAdmission(ctx, call.JobID, call.Generation, func(job helper.Job) error {
		if job.Scope.CodexContract != 2 || job.Round != call.Round || job.ConversationID != call.ThreadID || job.TurnID != call.TurnID {
			return effects.ErrAuthority
		}
		if err := codex.ValidateHelperAttempt(brain, job); err != nil {
			return err
		}
		pre.Job = job
		pre.Work = job.Scope.Authorization()
		var err error
		pre.Authority, _, err = nativeAuthority(ctx, brain, job.Scope.EpicID, job.Scope.OwnerID, true, nil, verify)
		if err != nil {
			return err
		}
		pre.Input, err = controlReadRepository(brain, session.Spec{Task: job.Scope.OwnerID, EpicID: job.Scope.EpicID}, &pre.Work, read)
		if err != nil {
			return err
		}
		return codex.ValidateProgramReadInput(original.Operation, pre.Input)
	})
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	pre.Caller = ProgramReadHelperSource{Original: originalRef, JobID: call.JobID, Generation: call.Generation, Round: call.Round, ThreadID: call.ThreadID, TurnID: call.TurnID, CallID: call.CallID, Input: effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: pre.Work.WorkID, JobID: call.JobID, Round: call.Round}}}
	return publishControlReadPreSource(ctx, brain, ControlReadPreSource{Schema: 2, Original: originalRef, Purpose: purpose, Helper: &pre}, verify)
}

func readControlReadPreSourceLocal(ctx context.Context, brain string, ref controlrecord.ImmutableRef, verify nativeAcceptance) (ControlReadPreSource, controlrecord.OriginalRequest, error) {
	var pre ControlReadPreSource
	var original controlrecord.OriginalRequest
	if err := ctx.Err(); err != nil {
		return pre, original, err
	}
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "effects", "control-reads") {
		return pre, original, effects.ErrEvidence
	}
	if err := controlSourceReadJSON(ref.Path, &pre); err != nil {
		return pre, original, err
	}
	if pre.Schema != 2 || pre.Purpose != ControlReadDirect || ref.Path != controlReadPreSourcePath(brain, pre.Original, pre.Purpose) || effectDigest(pre) != ref.SHA256 || (pre.Coordinator == nil) == (pre.Helper == nil) {
		return pre, original, effects.ErrEvidence
	}
	var input toolcontract.Input
	var err error
	original, input, err = controlrecord.DecodeInput(brain, pre.Original)
	if err != nil {
		return pre, original, err
	}
	read, err := controlDirectRead(original.Operation, input)
	if err != nil {
		return pre, original, err
	}
	var authority OwnerAuthority
	var actual toolcontract.GitHubReadInput
	if source := pre.Coordinator; source != nil {
		ingress := source.Ingress
		if original.Control == nil || original.Helper != nil || original.Tool != toolcontract.FactoryControl || ingress.Schema != 2 || ingress.Original != pre.Original || ingress.Control != *original.Control || ingress.Input.Operation != original.Operation || ingress.Input.PayloadSHA256 != original.PayloadSHA256 || ingress.Spec.Task != ingress.Authority.OwnerID || ingress.Spec.EpicID != ingress.Authority.EpicID || ingress.Writer.PID <= 1 || ingress.Writer.Group <= 0 || ingress.Writer.Birth == "" {
			return pre, original, effects.ErrEvidence
		}
		expected := effects.ControlTarget{}
		if source.Work != nil {
			if source.Work.CodexContract != 2 || source.Work.OwnerID != ingress.Authority.OwnerID || source.Work.EpicID != ingress.Authority.EpicID || source.Work.WorkID != ingress.Spec.Task {
				return pre, original, effects.ErrAuthority
			}
			expected.WorkID = source.Work.WorkID
		}
		if ingress.Input.Target != expected {
			return pre, original, effects.ErrEvidence
		}
		authority, actual = ingress.Authority, source.Input
	} else {
		source := pre.Helper
		call := original.Helper
		if call == nil || original.Control != nil || original.Tool != toolcontract.FactoryHelper || source.Caller.Original != pre.Original || source.Caller.JobID != call.JobID || source.Caller.Generation != call.Generation || source.Caller.Round != call.Round || source.Caller.ThreadID != call.ThreadID || source.Caller.TurnID != call.TurnID || source.Caller.CallID != call.CallID || source.Job.ID != call.JobID || source.Job.Generation != call.Generation || source.Job.Round != call.Round || source.Job.ConversationID != call.ThreadID || source.Job.TurnID != call.TurnID || source.Job.Scope.Authorization() != source.Work || source.Work.CodexContract != 2 || source.Authority.OwnerID != source.Work.OwnerID || source.Authority.EpicID != source.Work.EpicID {
			return pre, original, effects.ErrEvidence
		}
		expected := effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: source.Work.WorkID, JobID: call.JobID, Round: call.Round}}
		if source.Caller.Input != expected {
			return pre, original, effects.ErrEvidence
		}
		authority, actual = source.Authority, source.Input
	}
	if strings.ToLower(strings.TrimSpace(read.Repo)) != actual.Repo || read.Base != "" && read.Base != actual.Base || actual.Base == "" || read.Number != actual.Number || read.Path != actual.Path || read.SHA != actual.SHA || !read.Since.Equal(actual.Since) {
		return pre, original, effects.ErrEvidence
	}
	if _, _, err := nativeAuthority(ctx, brain, authority.EpicID, authority.OwnerID, false, &authority, verify); err != nil {
		return pre, original, err
	}
	return pre, original, nil
}

func ReadControlReadPreSourceLocal(ctx context.Context, brain string, ref controlrecord.ImmutableRef) (ControlReadPreSource, error) {
	pre, _, err := readControlReadPreSourceLocal(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
	return pre, err
}

func FindControlReadPreSourceLocal(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose) (controlrecord.ImmutableRef, ControlReadPreSource, error) {
	return findControlReadPreSourceLocal(ctx, brain, original, purpose, codex.Contract2AcceptanceAuthority)
}
func findControlReadPreSourceLocal(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose, verify nativeAcceptance) (controlrecord.ImmutableRef, ControlReadPreSource, error) {
	name := controlReadPreSourcePath(brain, original, purpose)
	var pre ControlReadPreSource
	if err := controlSourceReadJSON(name, &pre); err != nil {
		return controlrecord.ImmutableRef{}, pre, err
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: effectDigest(pre)}
	pre, _, err := readControlReadPreSourceLocal(ctx, brain, ref, verify)
	if err == nil && (pre.Original != original || pre.Purpose != purpose) {
		err = effects.ErrEvidence
	}
	return ref, pre, err
}

func FindControlReadAdmissionLocal(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose) (ProgramReadAdmission, error) {
	return findControlReadAdmissionLocal(ctx, brain, original, purpose, codex.Contract2AcceptanceAuthority)
}
func findControlReadAdmissionLocal(ctx context.Context, brain string, original controlrecord.OriginalRequestRef, purpose ControlReadPurpose, verify nativeAcceptance) (ProgramReadAdmission, error) {
	var snapshot controlReadSnapshot
	name := controlReadSnapshotPath(brain, original, purpose)
	if err := controlSourceReadJSON(name, &snapshot); err != nil {
		return ProgramReadAdmission{}, err
	}
	if snapshot.Original != original || snapshot.Admission.Purpose != purpose {
		return ProgramReadAdmission{}, effects.ErrEvidence
	}
	snapshot, err := readControlReadSnapshotLocal(ctx, brain, controlrecord.ImmutableRef{Path: name, SHA256: effectDigest(snapshot)}, verify)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	return snapshot.Admission, nil
}

func readControlReadSnapshotLocal(ctx context.Context, brain string, ref controlrecord.ImmutableRef, verify nativeAcceptance) (controlReadSnapshot, error) {
	var snapshot controlReadSnapshot
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	if err := controlSourceReadJSON(ref.Path, &snapshot); err != nil {
		return snapshot, err
	}
	if snapshot.Schema != 2 || ref.SHA256 != effectDigest(snapshot) || ref.Path != controlReadSnapshotPath(brain, snapshot.Original, snapshot.Admission.Purpose) {
		return controlReadSnapshot{}, effects.ErrEvidence
	}
	if err := validateControlReadLocal(ctx, brain, snapshot.Admission, verify); err != nil {
		return controlReadSnapshot{}, err
	}
	helperSource := snapshot.Admission.Helper
	if snapshot.Admission.Control != nil {
		if snapshot.Original != snapshot.Admission.Control.Original {
			return controlReadSnapshot{}, effects.ErrEvidence
		}
	} else if snapshot.Original != helperSource.Original {
		return controlReadSnapshot{}, effects.ErrEvidence
	}
	return snapshot, nil
}

func ReadCoordinatorReadPreAdmissionLocal(ctx context.Context, brain string, preRef, enrollmentRef controlrecord.ImmutableRef) (ProgramReadAdmission, error) {
	return readCoordinatorReadPreAdmissionLocal(ctx, brain, preRef, enrollmentRef, codex.Contract2AcceptanceAuthority)
}
func readCoordinatorReadPreAdmissionLocal(ctx context.Context, brain string, preRef, enrollmentRef controlrecord.ImmutableRef, verify nativeAcceptance) (ProgramReadAdmission, error) {
	pre, original, err := readControlReadPreSourceLocal(ctx, brain, preRef, verify)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	if pre.Coordinator == nil {
		return ProgramReadAdmission{}, effects.ErrAuthority
	}
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(brain, enrollmentRef)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	source := pre.Coordinator
	if enrollment.Source != preRef || enrollment.Original != pre.Original || enrollment.Receipt.Control != source.Ingress.Control || enrollment.Input != source.Ingress.Input {
		return ProgramReadAdmission{}, effects.ErrEvidence
	}
	return ProgramReadAdmission{Purpose: pre.Purpose, Authority: source.Ingress.Authority, Work: source.Work, Operation: original.Operation, Input: source.Input, Control: &ProgramReadControlSource{Original: pre.Original, Receipt: enrollment.Receipt, Input: enrollment.Input}}, nil
}
func ReadHelperReadPreAdmissionLocal(ctx context.Context, brain string, preRef controlrecord.ImmutableRef) (ProgramReadAdmission, error) {
	return readHelperReadPreAdmissionLocal(ctx, brain, preRef, codex.Contract2AcceptanceAuthority)
}
func readHelperReadPreAdmissionLocal(ctx context.Context, brain string, preRef controlrecord.ImmutableRef, verify nativeAcceptance) (ProgramReadAdmission, error) {
	pre, original, err := readControlReadPreSourceLocal(ctx, brain, preRef, verify)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	if pre.Helper == nil {
		return ProgramReadAdmission{}, effects.ErrAuthority
	}
	source := pre.Helper
	work := source.Work
	caller := source.Caller
	return ProgramReadAdmission{Purpose: pre.Purpose, Authority: source.Authority, Work: &work, Operation: original.Operation, Input: source.Input, Helper: &caller}, nil
}

func BindCoordinatorReadSourceLocked(ctx context.Context, brain string, preRef controlrecord.ImmutableRef, control effects.ControlReceiptRef) (ProgramReadAdmission, error) {
	return bindCoordinatorReadSourceLocked(ctx, brain, preRef, control, codex.Contract2AcceptanceAuthority)
}
func bindCoordinatorReadSourceLocked(ctx context.Context, brain string, preRef controlrecord.ImmutableRef, control effects.ControlReceiptRef, verify nativeAcceptance) (ProgramReadAdmission, error) {
	pre, _, err := readControlReadPreSourceLocal(ctx, brain, preRef, verify)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	if pre.Coordinator == nil {
		return ProgramReadAdmission{}, effects.ErrAuthority
	}
	source := pre.Coordinator
	spec, err := codex.ReadAttemptControlLocal(ctx, brain, source.Ingress.Control)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	if !reflect.DeepEqual(spec, source.Ingress.Spec) {
		return ProgramReadAdmission{}, effects.ErrAuthority
	}
	if err := CoordinatorAuthority(brain, spec); err != nil {
		return ProgramReadAdmission{}, err
	}
	if _, _, err := nativeAuthority(ctx, brain, source.Ingress.Authority.EpicID, source.Ingress.Authority.OwnerID, true, &source.Ingress.Authority, verify); err != nil {
		return ProgramReadAdmission{}, err
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, source.Ingress.Input.Target.WorkID); err != nil {
		return ProgramReadAdmission{}, err
	}
	input, err := (effects.Service{Brain: brain}).ReadControlReference(ctx, control)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	if control.Control != source.Ingress.Control || input != source.Ingress.Input {
		return ProgramReadAdmission{}, effects.ErrEvidence
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: pre.Original, Source: preRef, Receipt: control, Input: input})
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	admission, err := readCoordinatorReadPreAdmissionLocal(ctx, brain, preRef, enrollment, verify)
	if err != nil {
		return admission, err
	}
	snapshot := controlReadSnapshot{Schema: 2, Original: pre.Original, Admission: admission, Spec: &spec}
	if err := controlSourceWriteImmutable(controlReadSnapshotPath(brain, pre.Original, pre.Purpose), snapshot); err != nil {
		return ProgramReadAdmission{}, err
	}
	return admission, nil
}
func BindHelperReadSourceLocked(ctx context.Context, brain string, preRef controlrecord.ImmutableRef) (ProgramReadAdmission, error) {
	return bindHelperReadSourceLocked(ctx, brain, preRef, codex.Contract2AcceptanceAuthority)
}
func bindHelperReadSourceLocked(ctx context.Context, brain string, preRef controlrecord.ImmutableRef, verify nativeAcceptance) (ProgramReadAdmission, error) {
	admission, err := readHelperReadPreAdmissionLocal(ctx, brain, preRef, verify)
	if err != nil {
		return admission, err
	}
	pre, _, err := readControlReadPreSourceLocal(ctx, brain, preRef, verify)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	source := pre.Helper
	if err := codex.ValidateHelperControlCall(brain, source.Caller.JobID, source.Caller.Generation, source.Caller.Round, source.Caller.ThreadID, source.Caller.TurnID); err != nil {
		return ProgramReadAdmission{}, err
	}
	if _, _, err := nativeAuthority(ctx, brain, source.Authority.EpicID, source.Authority.OwnerID, true, &source.Authority, verify); err != nil {
		return ProgramReadAdmission{}, err
	}
	service := helper.Service{Brain: brain, Authority: controlReadHelperAuthority{NativeAuthority{brain}, verify}}
	_, err = service.WithManagedAdmission(ctx, source.Caller.JobID, source.Caller.Generation, func(job helper.Job) error {
		if job.ID != source.Job.ID || job.Generation != source.Job.Generation || job.Round != source.Job.Round || job.Scope != source.Job.Scope || job.Request != source.Job.Request || job.Route != source.Job.Route || job.ConversationID != source.Caller.ThreadID || job.TurnID != source.Caller.TurnID {
			return effects.ErrAuthority
		}
		return codex.ValidateHelperAttempt(brain, job)
	})
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	snapshot := controlReadSnapshot{Schema: 2, Original: pre.Original, Admission: admission, Job: &source.Job}
	if err := controlSourceWriteImmutable(controlReadSnapshotPath(brain, pre.Original, pre.Purpose), snapshot); err != nil {
		return ProgramReadAdmission{}, err
	}
	return admission, nil
}

func (a NativeAuthority) ValidateControlReadInputAbsentLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, ref codex.ProgramReadInputAbsentRef) error {
	return a.validateControlReadInputAbsentLocal(ctx, original, input, ref, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) validateControlReadInputAbsentLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, ref codex.ProgramReadInputAbsentRef, verify nativeAcceptance) error {
	if err := validateProgrammeReadInputAbsentLocal(ctx, a.Brain, ref, verify); err != nil {
		return err
	}
	record, err := codex.ReadProgramReadInputAbsentLocal(ctx, a.Brain, ref)
	if err != nil {
		return err
	}
	if record.Original != original || record.Enrollment == nil {
		return effects.ErrEvidence
	}
	admission, err := readCoordinatorReadPreAdmissionLocal(ctx, a.Brain, record.PreSource, *record.Enrollment, verify)
	if err != nil {
		return err
	}
	if admission.Control == nil || admission.Helper != nil || admission.Control.Original != original || admission.Control.Input != input {
		return effects.ErrEvidence
	}
	return nil
}
