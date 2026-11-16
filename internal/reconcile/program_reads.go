package reconcile

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
)

type ProgramReadAdmission struct {
	Authority   OwnerAuthority
	Purpose     ControlReadPurpose
	Operation   string
	Input       toolcontract.GitHubReadInput
	Work        *helper.Scope
	Control     *ProgramReadControlSource
	Helper      *ProgramReadHelperSource
	RepoRequest *effects.EventRef
	Program     *ProgramReadProgramSource `json:"Program,omitempty"`
}

type ProgramReadProgramSource struct {
	Event   effects.EventRef
	Purpose string
}

func prepareRegistryChecksLocked(ctx context.Context, brain string, selected effects.EventRef, verify nativeAcceptance) (codex.ProgramReadRef, error) {
	selection, candidate, _, err := readRegistryChecksSelectionLocalUsing(ctx, brain, selected, verify)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	if ref, found, err := registryChecksExistingInputLocal(ctx, brain, selected, selection, candidate, verify); err != nil || found {
		return ref, err
	}
	admission, err := registryChecksReadAdmissionLocalUsing(ctx, brain, selected, verify)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	return prepareProgramReadLocked(ctx, brain, admission, verify)
}

type ProgramReadControlSource struct {
	Receipt  effects.ControlReceiptRef
	Input    effects.ControlInput
	Original controlrecord.OriginalRequestRef
}

type ProgramReadHelperSource struct {
	JobID                    string
	Generation, Round        int
	ThreadID, TurnID, CallID string
	Input                    effects.ControlInput
	Original                 controlrecord.OriginalRequestRef
}

// Ordinary admission requires the original enrolled control-input witness.
// Typed read parameters alone cannot establish that caller's authority.
func PrepareProgramReadLocked(ctx context.Context, brain string, input ProgramReadAdmission) (codex.ProgramReadRef, error) {
	return prepareProgramReadLocked(ctx, brain, input, codex.Contract2AcceptanceAuthority)
}

func programReadRequestID(a ProgramReadAdmission) string {
	return effectDigest(struct {
		Domain string
		Source ProgramReadAdmission
	}{"factory-program-read-input-v2", a}).String()
}

func prepareProgramReadLocked(ctx context.Context, brain string, input ProgramReadAdmission, verify nativeAcceptance) (codex.ProgramReadRef, error) {
	var ref codex.ProgramReadRef
	err := withProgramReadAdmissionLocked(ctx, brain, input, nil, verify, func() error {
		var err error
		ref, err = persistProgrammeReadInputLocked(ctx, brain, input, verify)
		if err != nil {
			return err
		}
		return queueProgramReadLocked(ctx, brain, input, ref)
	})
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	return ref, nil
}

func persistProgrammeReadInputLocked(ctx context.Context, brain string, input ProgramReadAdmission, verify nativeAcceptance) (codex.ProgramReadRef, error) {
	var ref codex.ProgramReadRef
	if err := codex.ValidateProgramReadInput(input.Operation, input.Input); err != nil {
		return ref, err
	}
	id := programReadRequestID(input)
	ref, err := codex.ReadProgramReadRequestReference(ctx, brain, id)
	if errors.Is(err, os.ErrNotExist) {
		now := time.Now().UTC()
		deadline := now.Add(15 * time.Second)
		if input.Program != nil {
			if err := validateNewRegistryChecksReadLocalUsing(ctx, brain, input.Program.Event, verify); err != nil {
				return ref, err
			}
			deadline = now.Add(2 * time.Minute)
		} else {
			if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d
			}
		}
		if err := ctx.Err(); err != nil {
			return ref, err
		}
		if !deadline.After(now) {
			return ref, context.DeadlineExceeded
		}
		source, err := json.Marshal(input)
		if err != nil {
			return ref, err
		}
		return codex.PublishProgramReadRequestLocked(brain, codex.ProgramReadRequest{Schema: 2, RequestID: id, Kind: "program_read", Operation: input.Operation, Input: input.Input, Source: source, AdmittedAt: now, Deadline: deadline})
	}
	if err != nil {
		return ref, err
	}
	request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	if _, err := readProgrammeAdmission(ctx, brain, request, verify); err != nil {
		return codex.ProgramReadRef{}, err
	}
	if err := rejectProgramReadNoAction(ctx, brain, ref); err != nil {
		return codex.ProgramReadRef{}, err
	}
	return ref, nil
}

func LookupProgramReadForAdmission(ctx context.Context, brain string, a ProgramReadAdmission) (codex.ProgramReadRef, error) {
	return lookupProgramReadForAdmission(ctx, brain, a, codex.Contract2AcceptanceAuthority)
}
func lookupProgramReadForAdmission(ctx context.Context, brain string, a ProgramReadAdmission, verify nativeAcceptance) (codex.ProgramReadRef, error) {
	var ref codex.ProgramReadRef
	if err := validateProgrammeReadLocal(ctx, brain, a, verify); err != nil {
		return ref, err
	}
	id := programReadRequestID(a)
	ref, err := codex.ReadProgramReadRequestReference(ctx, brain, id)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	source, err := json.Marshal(a)
	if err != nil {
		return ref, err
	}
	if request.RequestID != id || !bytes.Equal(source, request.Source) {
		return ref, effects.ErrEvidence
	}
	if _, err := readProgrammeAdmission(ctx, brain, request, verify); err != nil {
		return codex.ProgramReadRef{}, err
	}
	return ref, nil
}

func readProgrammeAdmission(ctx context.Context, brain string, request codex.ProgramReadRequest, verify nativeAcceptance) (ProgramReadAdmission, error) {
	var admission ProgramReadAdmission
	if request.Kind != "program_read" || request.Grant != nil || len(request.Source) == 0 {
		return admission, effects.ErrEvidence
	}
	if err := toolcontract.DecodeBroker(request.Source, &admission); err != nil {
		return admission, err
	}
	canonical, err := json.Marshal(admission)
	if err != nil || !bytes.Equal(canonical, request.Source) || programReadRequestID(admission) != request.RequestID || request.Operation != admission.Operation || codex.ProgramProcessDigest(request.Input) != codex.ProgramProcessDigest(admission.Input) {
		return admission, effects.ErrEvidence
	}
	if err := validateProgrammeReadLocal(ctx, brain, admission, verify); err != nil {
		return admission, err
	}
	return admission, nil
}

func validateProgrammeReadLocal(ctx context.Context, brain string, a ProgramReadAdmission, verify nativeAcceptance) error {
	if a.Program == nil {
		return validateControlReadLocal(ctx, brain, a, verify)
	}
	if a.Control != nil || a.Helper != nil || a.RepoRequest != nil || a.Work == nil || a.Program.Purpose != "registry_checks" || a.Purpose != "registry_checks" || a.Operation != "registry_checks" {
		return effects.ErrEvidence
	}
	selection, candidate, policy, err := readRegistryChecksSelectionLocalUsing(ctx, brain, a.Program.Event, verify)
	if err != nil {
		return err
	}
	if a.Authority != candidate.Authority || *a.Work != candidate.Work || a.Input.Repo != policy.Repo || a.Input.SHA != candidate.Commit || codex.ValidateProgramReadInput(a.Operation, a.Input) != nil {
		return effects.ErrEvidence
	}
	if selection.ReadRecipe != nil && a.Input != selection.ReadRecipe.Input {
		return effects.ErrEvidence
	}
	return nil
}

func registryCheckOwnerForProcess(ctx context.Context, brain string, request codex.ProgramReadRequest, process codex.ProgramProcessRecord, verify nativeAcceptance) (workspace.RegistryCheckOwner, codex.ProgramArtifact, error) {
	var none workspace.RegistryCheckOwner
	admission, err := readProgrammeAdmission(ctx, brain, request, verify)
	if err != nil {
		return none, codex.ProgramArtifact{}, err
	}
	if admission.Program == nil {
		return none, codex.ProgramArtifact{}, effects.ErrEvidence
	}
	ref := codex.ProgramReadRef{RequestID: request.RequestID, SHA256: codex.ProgramProcessDigest(request)}
	dir, err := codex.RegistryCheckRunDirectory(brain, ref, process)
	if err != nil {
		return none, codex.ProgramArtifact{}, err
	}
	selected, candidate, policy, err := readRegistryChecksSelectionLocalUsing(ctx, brain, admission.Program.Event, verify)
	if err != nil {
		return none, codex.ProgramArtifact{}, err
	}
	receipt, err := codex.ReadHelperRevisionLocal(ctx, brain, candidate.Revision)
	if err != nil {
		return none, codex.ProgramArtifact{}, err
	}
	selection, err := NativeSelection(brain, candidate.Work.EpicID)
	if err != nil {
		return none, codex.ProgramArtifact{}, err
	}
	if selection.Executor != "codex" || process.OwnerID != candidate.Authority.OwnerID || process.LogicalRevision != candidate.Authority.Revision || !process.Deadline.Equal(request.Deadline) {
		return none, codex.ProgramArtifact{}, effects.ErrEvidence
	}
	artifact := func(ref codex.ProgramArtifact) workspace.RevisionArtifact {
		return workspace.RevisionArtifact{Path: ref.Path, SHA256: ref.SHA256.String()}
	}
	settlement := receipt.Settlement.Observation
	owner := workspace.RegistryCheckOwner{RequestID: request.RequestID, ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256.String(), RunDir: dir, Candidate: artifact(selected.Candidate), Input: artifact(receipt.Observation.Input), OldModelSettlement: workspace.RevisionArtifact{Path: settlement.ContainmentRef, SHA256: settlement.ContainmentSHA256}, Registry: artifact(selected.Policy), Commit: candidate.Commit, Tree: candidate.Tree, Image: selection.ContainerImageID, Commands: policy.Commands, NetworkPolicy: policy.NetworkPolicy, Deadline: process.Deadline}
	return owner, candidate.Revision, nil
}

// The caller holds tick. A helper callback never re-enters its ledger.
func withProgramReadAdmissionLocked(ctx context.Context, brain string, a ProgramReadAdmission, observed *codex.ProgramReadSourceObservation, verify nativeAcceptance, action func() error) error {
	if err := validateProgrammeReadLocal(ctx, brain, a, verify); err != nil {
		return err
	}
	if _, _, err := nativeAuthority(ctx, brain, a.Authority.EpicID, a.Authority.OwnerID, true, &a.Authority, verify); err != nil {
		return err
	}
	if a.Work != nil {
		if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, a.Work.WorkID); err != nil {
			return err
		}
	}
	if a.Program != nil {
		if err := validateRegistryChecksAdmissionLocalUsing(ctx, brain, a.Program.Event, verify); err != nil {
			return err
		}
		_, it, err := nativeOwner(brain, a.Work.EpicID, a.Work.WorkID)
		if err != nil {
			return err
		}
		if it.work == nil || a.Input.Base != it.work.Base || observed != nil {
			return effects.ErrAuthority
		}
		return action()
	}
	if a.Control != nil {
		id := a.Control.Receipt.Control
		ref := codex.AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID}
		spec, err := codex.AttemptControlAuthority(brain, ref)
		if err != nil {
			return err
		}
		if spec.EpicID != a.Authority.EpicID || spec.Task != a.Authority.OwnerID {
			return effects.ErrAuthority
		}
		if err := codex.ValidateAttemptControlCall(brain, ref, id.ThreadID, id.TurnID); err != nil {
			return err
		}
		if err := CoordinatorAuthority(brain, spec); err != nil {
			return err
		}
		if err := validateProgramReadCurrentTarget(ctx, brain, a, spec, verify); err != nil {
			return err
		}
		if observed != nil {
			if err := observed.ValidateControlLocal(ctx, brain, ref); err != nil {
				return err
			}
		}
		return action()
	}
	h := a.Helper
	if h == nil || a.Work == nil {
		return effects.ErrEvidence
	}
	if err := codex.ValidateHelperControlCall(brain, h.JobID, h.Generation, h.Round, h.ThreadID, h.TurnID); err != nil {
		return err
	}
	if err := validateProgramReadCurrentTarget(ctx, brain, a, session.Spec{Task: a.Authority.OwnerID, EpicID: a.Authority.EpicID}, verify); err != nil {
		return err
	}
	service := helper.Service{Brain: brain, Authority: controlReadHelperAuthority{NativeAuthority{brain}, verify}}
	_, err := service.WithManagedAdmission(ctx, h.JobID, h.Generation, func(job helper.Job) error {
		if job.Round != h.Round || job.ConversationID != h.ThreadID || job.TurnID != h.TurnID || job.Scope.Authorization() != a.Work.Authorization() {
			return effects.ErrAuthority
		}
		if err := codex.ValidateHelperAttempt(brain, job); err != nil {
			return err
		}
		if observed != nil {
			if err := observed.ValidateHelperLocal(ctx, brain, job); err != nil {
				return err
			}
		}
		return action()
	})
	return err
}

func validateProgramReadCurrentTarget(ctx context.Context, brain string, a ProgramReadAdmission, spec session.Spec, verify nativeAcceptance) error {
	target, err := controlReadRepository(brain, spec, a.Work, a.Input)
	if err != nil {
		return err
	}
	if codex.ProgramProcessDigest(target) != codex.ProgramProcessDigest(a.Input) {
		return effects.ErrAuthority
	}
	if a.Work != nil {
		current, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, a.Work.WorkID, verify)
		if err != nil {
			return err
		}
		if current.Authorization() != a.Work.Authorization() {
			return effects.ErrAuthority
		}
	}
	return nil
}

const programReadQueueRequest = "program-read"

func queueProgramReadLocked(ctx context.Context, brain string, a ProgramReadAdmission, ref codex.ProgramReadRef) error {
	guard, err := codex.AcquireProgramReadInputGuard(ctx, brain, ref.RequestID)
	if err != nil {
		return err
	}
	defer guard.Close()
	if err := guard.ValidateExistingLocal(ctx, ref); err != nil {
		return err
	}
	if err := rejectProgramReadNoAction(ctx, brain, ref); err != nil {
		return err
	}

	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	entry, err := ix.Lookup(a.Authority.OwnerID)
	if err != nil {
		return err
	}
	dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
	if err != nil {
		return err
	}
	expected := events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: programReadQueueRequest, Text: ref.RequestID, File: filepath.Join(dir, "request.json"), SHA256: ref.SHA256.String(), Key: programReadQueueRequest + ":" + ref.RequestID}
	logged, err := events.Read(store.EventsPath(entry.Dir))
	if err != nil {
		return err
	}
	found := false
	for _, event := range logged {
		if event.Key != expected.Key {
			continue
		}
		if found || event.Kind != expected.Kind || event.Sender != expected.Sender || event.Recipient != expected.Recipient || event.Request != expected.Request || event.Text != expected.Text || event.File != expected.File || event.SHA256 != expected.SHA256 || event.TriggerRef != "" {
			return effects.ErrEvidence
		}
		found = true
	}
	if !found {
		if _, err := events.Append(store.EventsPath(entry.Dir), expected); err != nil {
			return err
		}
	}
	return syncOwnership(&entity{entry: entry})
}

type ProgramReadStatus struct {
	NoAction              *codex.ProgramReadNoActionRef
	Stage                 string
	Process               codex.ProgramProcessRef
	Deadline, NextCheckAt time.Time
}

type ProgramReadHeld struct {
	Status ProgramReadStatus
	Cause  error
}

func (e *ProgramReadHeld) Error() string {
	return "programme read remains held until its actual process settles"
}
func (e *ProgramReadHeld) Unwrap() error { return e.Cause }

func programReadStatus(r codex.ProgramProcessRecord) ProgramReadStatus {
	return ProgramReadStatus{Stage: r.Stage, Process: codex.ProgramProcessRef{SubjectID: r.SubjectID, ClaimID: r.ClaimID, Revision: r.Revision, SHA256: codex.ProgramProcessDigest(r)}, Deadline: r.Deadline, NextCheckAt: r.NextCheckAt}
}

// ProgramDispatchKind selects a concrete claim facade. It grants no admission.
func ProgramDispatchKind(ctx context.Context, brain, claimID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r, err := effectReadProcess(brain, claimID)
	if err != nil {
		return "", err
	}
	selected, err := effectSelectedProcess(brain, r.SubjectID)
	if err != nil || selected.ClaimID != r.ClaimID || selected.EnrollmentSHA256 != r.EnrollmentSHA256 || codex.ProgramProcessDigest(selected) != codex.ProgramProcessDigest(r) {
		return "", effects.ErrEvidence
	}
	switch r.Subject.Kind {
	case "normal_effect", "program_read", "acceptance_read":
		return r.Subject.Kind, nil
	default:
		return "", effects.ErrEvidence
	}
}

func PrepareAcceptanceRead(ctx context.Context, brain, factoryBin string, grant codex.AcceptanceReadGrantRef) (codex.ProgramReadRef, error) {
	observed, err := codex.ObserveAcceptanceReadGrant(ctx, brain, grant)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	guard, err := codex.AcquireAcceptanceReadGuard(ctx, brain, grant, observed)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	defer guard.Release()
	enrollment, err := codex.ReadAcceptanceReadEnrollment(ctx, brain, grant)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	fingerprint, err := effectExecutableHash(factoryBin)
	if err != nil || fingerprint != enrollment.FactorySHA256 {
		return codex.ProgramReadRef{}, effects.ErrAuthority
	}
	return guard.PrepareReadInput(ctx)
}

func readProgramSubject(ctx context.Context, brain string, ref codex.ProgramReadRef) (codex.ProgramReadRequest, codex.ProgramProcessSubject, error) {
	request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
	if err != nil {
		return request, codex.ProgramProcessSubject{}, err
	}
	if request.Kind == "acceptance_read" {
		actual, err := codex.AcceptanceProgramReadRequest(ctx, brain, *request.Grant)
		if err != nil || codex.ProgramProcessDigest(actual) != codex.ProgramProcessDigest(request) {
			return request, codex.ProgramProcessSubject{}, effects.ErrEvidence
		}
		return request, codex.ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: request.Grant}, nil
	}
	return request, codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}, nil
}

func InspectProgramRead(ctx context.Context, brain string, ref codex.ProgramReadRef) (ProgramReadStatus, error) {
	return inspectProgramReadUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func inspectProgramReadUsing(ctx context.Context, brain string, ref codex.ProgramReadRef, verify nativeAcceptance) (ProgramReadStatus, error) {
	request, subject, err := readProgramSubject(ctx, brain, ref)
	if err != nil {
		return ProgramReadStatus{}, err
	}
	if request.Kind == "program_read" {
		noaction, err := codex.ProgramReadNoActionForRequest(ctx, brain, ref)
		if err == nil {
			return ProgramReadStatus{Stage: "unavailable", NoAction: &noaction, Deadline: request.Deadline}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return ProgramReadStatus{}, err
		}
	}
	id, err := codex.ProgramProcessSubjectID(subject)
	if err != nil {
		return ProgramReadStatus{}, err
	}
	r, err := effectSelectedProcess(brain, id)
	if errors.Is(err, os.ErrNotExist) {
		return ProgramReadStatus{Stage: "prepared", Deadline: request.Deadline}, nil
	}
	if err != nil {
		return ProgramReadStatus{}, err
	}
	if err := validateReadEnrollmentUsing(ctx, brain, request, r, verify); err != nil {
		return ProgramReadStatus{}, err
	}
	return programReadStatus(r), nil
}

func validateReadEnrollment(ctx context.Context, brain string, request codex.ProgramReadRequest, r codex.ProgramProcessRecord) error {
	return validateReadEnrollmentUsing(ctx, brain, request, r, codex.Contract2AcceptanceAuthority)
}
func validateReadEnrollmentUsing(ctx context.Context, brain string, request codex.ProgramReadRequest, r codex.ProgramProcessRecord, verify nativeAcceptance) error {
	if r.Deadline.After(request.Deadline) || r.AdmittedAt.Before(request.AdmittedAt) {
		return effects.ErrEvidence
	}
	if request.Kind == "program_read" {
		a, err := readProgrammeAdmission(ctx, brain, request, verify)
		if err != nil {
			return err
		}
		ref := codex.ProgramReadRef{RequestID: request.RequestID, SHA256: codex.ProgramProcessDigest(request)}
		mode := "read"
		if a.Program != nil {
			mode = "check"
			if !r.Deadline.Equal(request.Deadline) {
				return effects.ErrEvidence
			}
		}
		if r.Subject.Kind != "program_read" || r.Subject.ProgramRead == nil || *r.Subject.ProgramRead != ref || r.InputSHA256 != ref.SHA256 || r.OwnerID != a.Authority.OwnerID || r.LogicalRevision != a.Authority.Revision || r.Mode != mode {
			return effects.ErrEvidence
		}
		return nil
	}
	if request.Kind != "acceptance_read" || request.Grant == nil {
		return effects.ErrEvidence
	}
	enrollment, err := codex.ReadAcceptanceReadEnrollment(ctx, brain, *request.Grant)
	if err != nil || r.Subject.AcceptanceRead == nil || *r.Subject.AcceptanceRead != *request.Grant || r.InputSHA256 != request.Grant.SHA256 || r.OwnerID != enrollment.OwnerID || r.LogicalRevision != enrollment.LogicalRevision || r.FactorySHA256 != enrollment.FactorySHA256 {
		return effects.ErrAuthority
	}
	return nil
}

func ReadProgramReadResult(ctx context.Context, brain string, ref codex.ProgramReadRef) (codex.ProgramReadResultRef, error) {
	status, err := InspectProgramRead(ctx, brain, ref)
	if err != nil {
		return codex.ProgramReadResultRef{}, err
	}
	if status.NoAction != nil {
		return codex.ProgramReadResultRef{}, &ProgramReadNoActionError{Reference: *status.NoAction}
	}
	if status.Stage != "closed" {
		return codex.ProgramReadResultRef{}, &ProgramReadHeld{Status: status, Cause: effects.ErrBusy}
	}
	return codex.ReadProgramReadResultReference(ctx, brain, ref, status.Process)
}

func RunProgramRead(ctx context.Context, brain, factoryBin string, ref codex.ProgramReadRef) (codex.ProgramReadResultRef, error) {
	return runProgramRead(ctx, brain, factoryBin, ref, codex.Contract2AcceptanceAuthority)
}
func runProgramRead(ctx context.Context, brain, factoryBin string, ref codex.ProgramReadRef, verify nativeAcceptance) (codex.ProgramReadResultRef, error) {
	request, subject, err := readProgramSubject(ctx, brain, ref)
	if err != nil {
		return codex.ProgramReadResultRef{}, err
	}
	if request.Kind == "program_read" {
		if _, err := readProgrammeAdmission(ctx, brain, request, verify); err != nil {
			return codex.ProgramReadResultRef{}, err
		}
		if err := rejectProgramReadNoAction(ctx, brain, ref); err != nil {
			return codex.ProgramReadResultRef{}, err
		}
	}
	id, err := codex.ProgramProcessSubjectID(subject)
	if err != nil {
		return codex.ProgramReadResultRef{}, err
	}
	r, err := effectSelectedProcess(brain, id)
	nonce := ""
	if errors.Is(err, os.ErrNotExist) && request.Kind == "program_read" && !time.Now().Before(request.Deadline) {
		noaction, err := settleUnselectedProgrammeRead(ctx, brain, request, ref, verify, true)
		if err != nil {
			return codex.ProgramReadResultRef{}, err
		}
		return codex.ProgramReadResultRef{}, &ProgramReadNoActionError{Reference: noaction}
	}
	if errors.Is(err, os.ErrNotExist) {
		err = withReadSourceGuard(ctx, brain, request, true, verify, func() error {
			var e error
			r, e = effectSelectedProcess(brain, id)
			if !errors.Is(e, os.ErrNotExist) {
				return e
			}
			owner, revision, input, mode := "", "", ref.SHA256, "read"
			if request.Kind == "acceptance_read" {
				enrollment, e := codex.ReadAcceptanceReadEnrollment(ctx, brain, *request.Grant)
				if e != nil {
					return e
				}
				owner, revision, input, mode = enrollment.OwnerID, enrollment.LogicalRevision, request.Grant.SHA256, "fetch"
			} else {
				a, e := readProgrammeAdmission(ctx, brain, request, verify)
				if e != nil {
					return e
				}
				owner, revision = a.Authority.OwnerID, a.Authority.Revision
				if a.Program != nil {
					mode = "check"
				}
			}
			r, nonce, e = prepareProgramProcessLocked(ctx, brain, factoryBin, subject, owner, revision, input, mode, request.Deadline)
			if e == nil && mode == "check" {
				check, _, deriveErr := registryCheckOwnerForProcess(ctx, brain, request, r, verify)
				if deriveErr != nil {
					return deriveErr
				}
				e = workspace.NewRemoteWorkspace().PrepareRegistryCheckRecord(ctx, check)
			}
			return e
		})
	}
	if err != nil {
		if request.Kind == "program_read" && request.Operation != "registry_checks" && r.ClaimID == "" {
			noaction, closeErr := settleUnselectedProgrammeRead(ctx, brain, request, ref, verify, !time.Now().Before(request.Deadline))
			if closeErr == nil {
				return codex.ProgramReadResultRef{}, &ProgramReadNoActionError{Reference: noaction}
			}
			var held *ProgramReadHeld
			if errors.As(closeErr, &held) {
				return codex.ProgramReadResultRef{}, closeErr
			}
		}
		return codex.ProgramReadResultRef{}, err
	}
	if err := validateReadEnrollmentUsing(ctx, brain, request, r, verify); err != nil {
		return codex.ProgramReadResultRef{}, err
	}
	if r.Stage == "closed" {
		return finishProgramRead(ctx, brain, request, ref, r, verify)
	}
	if r.Stage == "prepared" && nonce == "" && time.Now().Before(r.Deadline) {
		return codex.ProgramReadResultRef{}, &ProgramReadHeld{Status: programReadStatus(r), Cause: effects.ErrBusy}
	}
	if r.Stage != "sealed" && time.Now().Before(r.Deadline) {
		current, waitErr := waitProgramProcess(ctx, brain, factoryBin, r, nonce)
		if waitErr != nil {
			if ctx.Err() != nil && time.Now().Before(r.Deadline) {
				return codex.ProgramReadResultRef{}, &ProgramReadHeld{Status: programReadStatus(current), Cause: waitErr}
			}
			if current.ClaimID != r.ClaimID || current.EnrollmentSHA256 != r.EnrollmentSHA256 {
				return codex.ProgramReadResultRef{}, waitErr
			}
			if current.Stage == "claimed" && time.Now().Before(current.Deadline) {
				alive, aliveErr := current.Host.Alive()
				if aliveErr != nil || alive {
					return codex.ProgramReadResultRef{}, &ProgramReadHeld{Status: programReadStatus(current), Cause: waitErr}
				}
			}
		}
		r = current
	}
	closed, err := settleProgramRead(ctx, brain, request, r, verify)
	if err != nil {
		return codex.ProgramReadResultRef{}, &ProgramReadHeld{Status: programReadStatus(closed), Cause: err}
	}
	return finishProgramRead(ctx, brain, request, ref, closed, verify)
}

func finishProgramRead(ctx context.Context, brain string, request codex.ProgramReadRequest, ref codex.ProgramReadRef, closed codex.ProgramProcessRecord, verify nativeAcceptance) (codex.ProgramReadResultRef, error) {
	status := programReadStatus(closed)
	if _, err := codex.ReadClosedProgramProcess(ctx, brain, status.Process); err != nil {
		return codex.ProgramReadResultRef{}, err
	}
	err := withReadSourceGuard(ctx, brain, request, false, verify, func() error {
		current, err := effectSelectedProcess(brain, closed.SubjectID)
		if err != nil {
			return err
		}
		if codex.ProgramProcessDigest(current) != codex.ProgramProcessDigest(closed) || current.Stage != "closed" {
			return effects.ErrStale
		}
		if err := validateReadEnrollmentUsing(ctx, brain, request, current, verify); err != nil {
			return err
		}
		dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
		if err != nil {
			return err
		}
		_, err = os.Lstat(filepath.Join(dir, "result.json"))
		if errors.Is(err, os.ErrNotExist) {
			if request.Operation == "registry_checks" {
				return effects.ErrEvidence
			}
			return codex.PublishProgramReadResult(brain, codex.ProgramReadResult{Schema: 2, Request: ref, Operation: request.Operation, Failure: "read_unavailable", ClaimID: closed.ClaimID, EnrollmentSHA256: closed.EnrollmentSHA256})
		}
		return err
	})
	if err != nil {
		return codex.ProgramReadResultRef{}, err
	}
	return codex.ReadProgramReadResultReference(ctx, brain, ref, status.Process)
}

func settleAcceptanceRead(ctx context.Context, brain string, request codex.ProgramReadRequest, expected codex.ProgramProcessRecord) (codex.ProgramProcessRecord, error) {
	return settleProgramRead(ctx, brain, request, expected, codex.Contract2AcceptanceAuthority)
}
func settleProgramRead(ctx context.Context, brain string, request codex.ProgramReadRequest, expected codex.ProgramProcessRecord, verify nativeAcceptance) (codex.ProgramProcessRecord, error) {
	sealed := expected
	err := withReadSourceGuard(ctx, brain, request, false, verify, func() error {
		current, err := effectSelectedProcess(brain, expected.SubjectID)
		if err != nil {
			return err
		}
		if current.ClaimID != expected.ClaimID || current.EnrollmentSHA256 != expected.EnrollmentSHA256 {
			return effects.ErrStale
		}
		if err := validateReadEnrollmentUsing(ctx, brain, request, current, verify); err != nil {
			return err
		}
		sealed = current
		if current.Stage == "sealed" && !current.NextCheckAt.IsZero() && time.Now().Before(current.NextCheckAt) {
			return effects.ErrBusy
		}
		sealed, err = sealProgramProcessLocked(ctx, brain, current.ClaimID)
		return err
	})
	if err != nil || sealed.Stage == "closed" {
		return sealed, err
	}
	fenceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	fenceErr := fenceProgramProcess(fenceCtx, brain, sealed)
	cancel()
	if fenceErr == nil && request.Operation == "registry_checks" {
		owner, revision, err := registryCheckOwnerForProcess(ctx, brain, request, sealed, verify)
		if err == nil {
			remote := workspace.NewRemoteWorkspace()
			// A sealed never-claimed record cannot start a programme writer. Its
			// missing domain metadata may be completed as actual not-started facts.
			if sealed.Host == (codex.ProcessIdentity{}) {
				err = remote.PrepareRegistryCheckRecord(ctx, owner)
			}
			if err == nil {
				var observed workspace.RegistryCheckObservation
				observed, err = remote.ObserveRegistryCheck(ctx, owner)
				if err == nil {
					err = publishRegistryCheckReadResult(ctx, brain, *sealed.Subject.ProgramRead, sealed, owner, revision, observed)
				}
			}
		}
		fenceErr = err
	}
	closed := sealed
	err = withReadSourceGuard(ctx, brain, request, false, verify, func() error {
		var err error
		if fenceErr != nil {
			closed, err = deferProgramProcessFenceLocked(ctx, brain, sealed)
			if err != nil {
				closed = sealed
			}
			return errors.Join(fenceErr, err)
		}
		closed, err = closeProgramProcessLocked(ctx, brain, sealed, "")
		return err
	})
	return closed, err
}

func withReadSourceGuard(ctx context.Context, brain string, request codex.ProgramReadRequest, current bool, verify nativeAcceptance, action func() error) error {
	if request.Kind == "acceptance_read" && request.Grant != nil {
		if current {
			observed, err := codex.ObserveAcceptanceReadGrant(ctx, brain, *request.Grant)
			if err != nil {
				return err
			}
			guard, err := codex.AcquireAcceptanceReadGuard(ctx, brain, *request.Grant, observed)
			if err != nil {
				return err
			}
			defer guard.Release()
			return action()
		}
		guard, err := codex.AcquireAcceptanceReadClosureGuard(ctx, brain, *request.Grant)
		if err != nil {
			return err
		}
		defer guard.Release()
		return action()
	}
	a, err := readProgrammeAdmission(ctx, brain, request, verify)
	if err != nil {
		return err
	}
	ref := codex.ProgramReadRef{RequestID: request.RequestID, SHA256: codex.ProgramProcessDigest(request)}
	if err := rejectProgramReadNoAction(ctx, brain, ref); err != nil {
		return err
	}
	var observation *codex.ProgramReadSourceObservation
	if current && a.Program == nil {
		if a.Control != nil {
			key := a.Control.Receipt.Control.Attempt
			observation, err = codex.ObserveProgramReadControl(ctx, brain, codex.AttemptRef{SessionID: key.SessionID, AttemptID: key.AttemptID})
		} else {
			job, e := (helper.Service{Brain: brain}).Job(a.Helper.JobID)
			if e != nil {
				return e
			}
			observation, err = codex.ObserveProgramReadHelper(ctx, brain, job)
		}
		if err != nil {
			return err
		}
	}
	unlock, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return err
	}
	defer unlock()
	if err := rejectProgramReadNoAction(ctx, brain, ref); err != nil {
		return err
	}
	if current {
		return withProgramReadAdmissionLocked(ctx, brain, a, observation, verify, func() error {
			guard, err := codex.AcquireProgramReadInputGuard(ctx, brain, ref.RequestID)
			if err != nil {
				return err
			}
			defer guard.Close()
			if err := guard.ValidateExistingLocal(ctx, ref); err != nil {
				return err
			}
			return action()
		})
	}

	if _, err := readProgrammeAdmission(ctx, brain, request, verify); err != nil {
		return err
	}
	return action()
}

type ClaimedAcceptanceRead struct {
	brain        string
	request      codex.ProgramReadRequest
	ref          codex.ProgramReadRef
	process      codex.ProgramProcessRecord
	bootstrapped bool
}

type ClaimedProgrammeRead struct {
	brain        string
	request      codex.ProgramReadRequest
	ref          codex.ProgramReadRef
	process      codex.ProgramProcessRecord
	bootstrapped bool
}

func ClaimProgrammeRead(ctx context.Context, brain, claimID, nonce string) (*ClaimedProgrammeRead, error) {
	return claimProgrammeRead(ctx, brain, claimID, nonce, codex.Contract2AcceptanceAuthority)
}
func claimProgrammeRead(ctx context.Context, brain, claimID, nonce string, verify nativeAcceptance) (*ClaimedProgrammeRead, error) {
	selected, err := effectReadProcess(brain, claimID)
	if err != nil || selected.Subject.Kind != "program_read" || selected.Subject.ProgramRead == nil {
		return nil, effects.ErrEvidence
	}
	ref := *selected.Subject.ProgramRead
	request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
	if err != nil {
		return nil, err
	}
	if err := validateReadEnrollmentUsing(ctx, brain, request, selected, verify); err != nil {
		return nil, err
	}
	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		return nil, err
	}
	err = withReadSourceGuard(ctx, brain, request, true, verify, func() error {
		var err error
		selected, err = claimProgramProcessLocked(ctx, brain, claimID, nonce, identity)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &ClaimedProgrammeRead{brain: brain, request: request, ref: ref, process: selected}, nil
}
func (r *ClaimedProgrammeRead) Bootstrap(ctx context.Context) (EffectProgramBootstrap, error) {
	return r.bootstrap(ctx, codex.Contract2AcceptanceAuthority)
}
func (r *ClaimedProgrammeRead) bootstrap(ctx context.Context, verify nativeAcceptance) (EffectProgramBootstrap, error) {
	var bootstrap EffectProgramBootstrap
	if r == nil || r.brain == "" || r.process.ClaimID == "" || r.request.Kind != "program_read" {
		return bootstrap, effects.ErrAuthority
	}
	err := withReadSourceGuard(ctx, r.brain, r.request, true, verify, func() error {
		var err error
		bootstrap, err = programActorBootstrapLocked(ctx, r.brain, r.process)
		return err
	})
	if err == nil {
		r.bootstrapped = true
	}
	return bootstrap, err
}
func (r *ClaimedProgrammeRead) Run(ctx context.Context) error {
	return r.run(ctx, codex.Contract2AcceptanceAuthority)
}
func (r *ClaimedProgrammeRead) run(ctx context.Context, verify nativeAcceptance) error {
	if r == nil || !r.bootstrapped {
		return effects.ErrAuthority
	}
	if _, err := readProgrammeAdmission(ctx, r.brain, r.request, verify); err != nil {
		return err
	}
	if _, err := programActorBootstrapLocked(ctx, r.brain, r.process); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, r.process.Deadline)
	defer cancel()
	if r.request.Operation == "registry_checks" {
		owner, revision, err := registryCheckOwnerForProcess(ctx, r.brain, r.request, r.process, verify)
		if err != nil {
			return err
		}
		observed, err := workspace.NewRemoteWorkspace().RunRegistryCheck(ctx, owner)
		if err != nil {
			return err
		}
		if _, err := programActorBootstrapLocked(ctx, r.brain, r.process); err != nil {
			return err
		}
		return publishRegistryCheckReadResult(ctx, r.brain, r.ref, r.process, owner, revision, observed)
	}
	value, readErr := (gh.Client{Runner: proc.Exec{}}).ProgramRead(ctx, r.request.Operation, r.request.Input)
	result := codex.ProgramReadResult{Schema: 2, Request: r.ref, Operation: r.request.Operation, ClaimID: r.process.ClaimID, EnrollmentSHA256: r.process.EnrollmentSHA256}
	if readErr != nil {
		result.Failure = "read_unavailable"
	} else {
		var output bytes.Buffer
		encoder := json.NewEncoder(&output)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			return err
		}
		data := bytes.TrimSuffix(output.Bytes(), []byte("\n"))
		if len(data) > toolcontract.ControlLimit {
			result.Failure = "read_unavailable"
			readErr = &gh.ProgramReadUnavailable{}
		} else {
			result.ReadJSON = data
		}
	}
	if _, err := programActorBootstrapLocked(ctx, r.brain, r.process); err != nil {
		return err
	}
	if err := codex.PublishProgramReadResult(r.brain, result); err != nil {
		return err
	}
	return gh.ProgramReadError(readErr)
}

func publishRegistryCheckReadResult(ctx context.Context, brain string, ref codex.ProgramReadRef, process codex.ProgramProcessRecord, owner workspace.RegistryCheckOwner, revision codex.ProgramArtifact, observed workspace.RegistryCheckObservation) error {
	check, err := codex.PublishRegistryCheckEvidence(ctx, brain, ref, process, owner, revision, observed)
	if err != nil {
		return err
	}
	return codex.PublishProgramReadResult(brain, codex.ProgramReadResult{Schema: 2, Request: ref, Operation: "registry_checks", Check: &check, ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256})
}

func ClaimAcceptanceRead(ctx context.Context, brain, claimID, nonce string) (*ClaimedAcceptanceRead, error) {
	selected, err := effectReadProcess(brain, claimID)
	if err != nil || selected.Subject.Kind != "acceptance_read" || selected.Subject.AcceptanceRead == nil {
		return nil, effects.ErrEvidence
	}
	grant := *selected.Subject.AcceptanceRead
	request, err := codex.AcceptanceProgramReadRequest(ctx, brain, grant)
	if err != nil {
		return nil, err
	}
	ref, err := acceptanceReadRequestReference(brain, request)
	if err != nil {
		return nil, err
	}
	if err := validateReadEnrollment(ctx, brain, request, selected); err != nil {
		return nil, err
	}
	observed, err := codex.ObserveAcceptanceReadGrant(ctx, brain, grant)
	if err != nil {
		return nil, err
	}
	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		return nil, err
	}
	guard, err := codex.AcquireAcceptanceReadGuard(ctx, brain, grant, observed)
	if err != nil {
		return nil, err
	}
	defer guard.Release()
	claimed, err := claimProgramProcessLocked(ctx, brain, claimID, nonce, identity)
	if err != nil {
		return nil, err
	}
	return &ClaimedAcceptanceRead{brain: brain, request: request, ref: ref, process: claimed}, nil
}

func acceptanceReadRequestReference(brain string, request codex.ProgramReadRequest) (codex.ProgramReadRef, error) {
	ref := codex.ProgramReadRef{RequestID: request.RequestID, SHA256: codex.ProgramProcessDigest(request)}
	if _, err := codex.ReadProgramReadRequest(context.Background(), brain, ref); err != nil {
		return codex.ProgramReadRef{}, err
	}
	return ref, nil
}

func (r *ClaimedAcceptanceRead) Bootstrap(ctx context.Context) (EffectProgramBootstrap, error) {
	if r == nil || r.request.Grant == nil || r.brain == "" || r.process.ClaimID == "" {
		return EffectProgramBootstrap{}, effects.ErrAuthority
	}
	observed, err := codex.ObserveAcceptanceReadGrant(ctx, r.brain, *r.request.Grant)
	if err != nil {
		return EffectProgramBootstrap{}, err
	}
	guard, err := codex.AcquireAcceptanceReadGuard(ctx, r.brain, *r.request.Grant, observed)
	if err != nil {
		return EffectProgramBootstrap{}, err
	}
	defer guard.Release()
	bootstrap, err := programActorBootstrapLocked(ctx, r.brain, r.process)
	if err == nil {
		r.bootstrapped = true
	}
	return bootstrap, err
}

func (r *ClaimedAcceptanceRead) Run(ctx context.Context) error {
	if r == nil || !r.bootstrapped {
		return effects.ErrAuthority
	}
	if _, err := programActorBootstrapLocked(ctx, r.brain, r.process); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, r.process.Deadline)
	defer cancel()
	result := codex.ProgramReadResult{Schema: 2, Request: r.ref, Operation: r.request.Operation, ClaimID: r.process.ClaimID, EnrollmentSHA256: r.process.EnrollmentSHA256}
	base, err := executeAcceptanceBase(ctx, r.brain, r.request)
	if err != nil {
		result.Failure = "read_unavailable"
	} else {
		result.Base = &base
	}
	if _, actualErr := programActorBootstrapLocked(ctx, r.brain, r.process); actualErr != nil {
		return actualErr
	}
	if publishErr := codex.PublishProgramReadResult(r.brain, result); publishErr != nil {
		return publishErr
	}
	return gh.ProgramReadError(err)
}

func executeAcceptanceBase(ctx context.Context, brain string, request codex.ProgramReadRequest) (codex.ProgramBaseImport, error) {
	target, err := codex.ReadAcceptanceReadTarget(ctx, brain, *request.Grant)
	if err != nil || request.Clone != target.Bare || request.Common != target.Bare || request.Worktree != target.Worktree || request.Input.Repo != target.Repository.Repo || request.Input.Base != target.Repository.Base {
		return codex.ProgramBaseImport{}, effects.ErrEvidence
	}
	repository, err := (gh.Client{Runner: proc.Exec{}}).ReadProgramRepository(ctx, target.Repository.Repo)
	if err != nil || !repository.Private {
		return codex.ProgramBaseImport{}, &gh.ProgramReadUnavailable{}
	}
	path := filepath.Join(filepath.Dir(target.Bare), "repository.json")
	if err := effectWriteImmutable(path, repository); err != nil {
		return codex.ProgramBaseImport{}, err
	}
	base, err := fetchProgramBase(ctx, request, target.AuthorName, target.AuthorMail, "https://github.com/"+target.Repository.Repo+".git")
	if err != nil {
		return base, err
	}
	base.Repository = codex.ProgramArtifact{Path: path, SHA256: codex.ProgramProcessDigest(repository)}
	return base, nil
}

// The remote is programme-derived. Tests call this private executor with an
// auth-required loopback target; no model tool accepts a remote URL or argv.
func fetchProgramBase(ctx context.Context, request codex.ProgramReadRequest, author, email, remote string) (codex.ProgramBaseImport, error) {
	credential, err := codex.ProgramGitCredentialHelper(request.Input.Repo)
	if err != nil {
		return codex.ProgramBaseImport{}, err
	}
	return fetchProgramBaseTransport(ctx, request, author, email, remote, credential)
}

func fetchProgramBaseTransport(ctx context.Context, request codex.ProgramReadRequest, author, email, remote, credential string) (codex.ProgramBaseImport, error) {
	var none codex.ProgramBaseImport
	if request.Kind != "acceptance_read" || request.Operation != "repo_base" || author == "" || email == "" || strings.ContainsAny(author+email, "\r\n\x00") {
		return none, effects.ErrEvidence
	}
	if _, err := os.Lstat(request.Clone); err == nil || !errors.Is(err, os.ErrNotExist) {
		return none, effects.ErrConflict
	}
	root := filepath.Dir(request.Clone)
	if err := effectMkdir(root); err != nil {
		return none, err
	}
	env := workspace.WithoutGitHubCredentials(os.Environ())
	filtered := env[:0]
	for _, item := range env {
		key, _, _ := strings.Cut(item, "=")
		if key != "HOME" && key != "XDG_CONFIG_HOME" && !strings.HasPrefix(key, "GIT_") {
			filtered = append(filtered, item)
		}
	}
	env = append(filtered, "HOME="+root, "XDG_CONFIG_HOME="+root, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=", "GH_TOKEN="+os.Getenv("GH_TOKEN"))
	run := func(args ...string) ([]byte, error) {
		command := exec.CommandContext(ctx, "git", append([]string{"-c", "core.hooksPath=/dev/null", "-c", "credential.helper=", "-c", "credential.helper=" + credential, "-c", "credential.useHttpPath=true", "-c", "http.followRedirects=false", "-c", "protocol.file.allow=never", "-c", "maintenance.auto=false", "-c", "gc.auto=0"}, args...)...)
		command.Env = env
		command.WaitDelay = 200 * time.Millisecond
		var out bytes.Buffer
		command.Stdout = &programReadOutput{out: &out, remaining: toolcontract.ControlLimit}
		command.Stderr = io.Discard
		if err := command.Run(); err != nil {
			return nil, &gh.ProgramReadUnavailable{}
		}
		return out.Bytes(), nil
	}
	for _, args := range [][]string{{"init", "--bare", "--template=", request.Clone}, {"-C", request.Clone, "config", "user.name", author}, {"-C", request.Clone, "config", "user.email", email}, {"-C", request.Clone, "remote", "add", "origin", remote}, {"-C", request.Clone, "fetch", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", "origin", "+refs/heads/" + request.Input.Base + ":refs/remotes/origin/" + request.Input.Base}} {
		if _, err := run(args...); err != nil {
			return none, err
		}
	}
	data, err := run("-C", request.Clone, "rev-parse", "--verify", "refs/remotes/origin/"+request.Input.Base+"^{commit}")
	if err != nil {
		return none, err
	}
	commit := strings.TrimSpace(string(data))
	if request.Input.SHA != "" {
		commit = request.Input.SHA
	}
	imported, err := workspace.ImportProgramCommit(ctx, request.Clone, request.Common, commit)
	if err != nil {
		return none, err
	}
	localEnv := workspace.WithoutGitHubCredentials(env)
	localEnv = append(localEnv, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	worktree, err := (git.Client{Runner: programGitRunner{env: localEnv}}).WorktreeFromCommit(ctx, request.Clone, commit, "acceptance")
	if err != nil || worktree != request.Worktree {
		return none, errors.Join(err, effects.ErrEvidence)
	}
	objects, err := readProgramDigest(imported.ObjectsSHA256)
	if err != nil {
		return none, err
	}
	content, err := readProgramDigest(imported.ContentSHA256)
	if err != nil {
		return none, err
	}
	return codex.ProgramBaseImport{Repo: request.Input.Repo, Base: request.Input.Base, Ref: "refs/heads/" + request.Input.Base, Commit: imported.Commit, Tree: imported.Tree, ObjectsDirectory: imported.ObjectsDirectory, ObjectsManifest: codex.ProgramArtifact{Path: imported.ObjectsManifest, SHA256: objects}, ContentManifest: codex.ProgramArtifact{Path: imported.ContentManifest, SHA256: content}}, nil
}

type programGitRunner struct{ env []string }

func (r programGitRunner) Run(ctx context.Context, cmd proc.Cmd) ([]byte, error) {
	if cmd.Name != "git" || cmd.Dir != "" || len(cmd.Env) != 0 {
		return nil, effects.ErrEvidence
	}
	command := exec.CommandContext(ctx, "git", append([]string{"-c", "core.hooksPath=/dev/null"}, cmd.Args...)...)
	command.Env = r.env
	command.WaitDelay = 200 * time.Millisecond
	var output bytes.Buffer
	command.Stdout = &programReadOutput{out: &output, remaining: toolcontract.ControlLimit}
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func readProgramDigest(value string) (effects.Digest, error) {
	var digest effects.Digest
	data, err := hex.DecodeString(value)
	if err != nil || len(data) != len(digest) {
		return digest, effects.ErrEvidence
	}
	copy(digest[:], data)
	return digest, nil
}

type programReadOutput struct {
	out       io.Writer
	remaining int
}

func (w *programReadOutput) Write(data []byte) (int, error) {
	if len(data) > w.remaining {
		return 0, fmt.Errorf("programme output exceeds limit")
	}
	w.remaining -= len(data)
	return w.out.Write(data)
}

// RepoWorker calls this pass once; it owns no additional scheduler or journal.
func runProgramReadJobs(ctx context.Context, brain string, out io.Writer) error {
	return runProgramReadJobsUsing(ctx, brain, out, codex.Contract2AcceptanceAuthority)
}
func runProgramReadJobsUsing(ctx context.Context, brain string, out io.Writer, verify nativeAcceptance) error {
	return runProgramReadJobsTraced(ctx, brain, out, verify, nil)
}

func runProgramReadJobsTraced(ctx context.Context, brain string, out io.Writer, verify nativeAcceptance, trace *registryCheckWorkTrace) error {
	rows, inventoryErr := codex.InspectProgramReadRequests(ctx, brain)
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	checks, err := prepareRegistryCheckWorkPass(ctx, brain, ix, rows, inventoryErr, out, verify, trace)
	if err != nil {
		return err
	}
	rows, inventoryErr = codex.InspectProgramReadRequests(ctx, brain)
	ids := make([]string, 0, len(ix))
	for id := range ix {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	expected := map[string]bool{}
	for _, id := range ids {
		logged, err := events.Read(store.EventsPath(ix[id].Dir))
		if err != nil {
			return err
		}
		for _, event := range logged {
			if event.Request != programReadQueueRequest {
				continue
			}
			if event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != events.RecipientRepoWorker || event.Text == "" {
				fmt.Fprintf(out, "%s: programme read queue evidence unavailable\n", id)
				continue
			}
			expected[event.Text] = true
		}
	}
	if inventoryErr != nil {
		if !errors.Is(inventoryErr, os.ErrNotExist) || len(expected) > 0 {
			fmt.Fprintf(out, "programme read inventory held: %v\n", inventoryErr)
		}
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if row.InputAbsent != nil {
			err := row.Err
			if err == nil {
				err = validateProgrammeReadInputAbsentLocal(ctx, brain, *row.InputAbsent, verify)
			}
			if err != nil {
				fmt.Fprintf(out, "%s: programme read input absence held: %v\n", row.RequestID, err)
			} else if expected[row.RequestID] {
				fmt.Fprintf(out, "%s: programme read queue contradicts input absence\n", row.RequestID)
			}
			delete(expected, row.RequestID)
			continue
		}
		delete(expected, row.RequestID)
		if row.Err != nil {
			fmt.Fprintf(out, "%s: programme read evidence held: %v\n", row.RequestID, row.Err)
			continue
		}
		if row.Request.Kind != "program_read" {
			continue
		}
		if row.Request.Operation == "registry_checks" {
			if checks.terminal[row.Reference] {
				continue
			}
			source, err := registryCheckRequestSource(row.Request)
			if err != nil || checks.invalid[source.OwnerID] || !checks.leaves[source] {
				continue
			}
		}

		unlock, err := waitLock(ctx, brain, "tick")
		if err != nil {
			return err
		}
		admission, err := readProgrammeAdmission(ctx, brain, row.Request, verify)
		if err == nil {
			err = queueProgramReadLocked(ctx, brain, admission, row.Reference)
		}
		unlock()
		if err != nil {
			fmt.Fprintf(out, "%s: programme read source held: %v\n", row.RequestID, err)
			continue
		}
		if _, err := runProgramRead(ctx, brain, binary, row.Reference, verify); err != nil {
			fmt.Fprintf(out, "%s: programme read held: %v\n", row.RequestID, err)
		}
	}
	missing := make([]string, 0, len(expected))
	for id := range expected {
		missing = append(missing, id)
	}
	sort.Strings(missing)
	for _, id := range missing {
		fmt.Fprintf(out, "%s: queued programme read input is unavailable\n", id)
	}
	return checks.recover(ctx, brain, out, verify, trace)
}
