package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/workspace"
)

type helperInputFailure struct {
	Cause  ProgramArtifact `json:"cause"`
	Input  ProgramArtifact `json:"input"`
	Reason string          `json:"reason"`
	Paths  []string        `json:"paths"`
}

type HelperRevisionObservation struct {
	Job          helper.Job
	Workspace    helper.ChildWorkspaceObservation
	Input, Final ProgramArtifact
}

type HelperRevisionReceipt struct {
	Observation HelperRevisionObservation
	Settlement  helper.ChildWorkspaceReceipt
}

type RevisionInputFailure struct {
	Version                     int
	Job                         helper.Job
	Attempt, Containment, Input ProgramArtifact
	Reason                      string
	Paths                       []string
}

func programmeRevisionRef(ref workspace.RevisionArtifact) (ProgramArtifact, error) {
	var sha effects.Digest
	if err := sha.UnmarshalText([]byte(ref.SHA256)); err != nil {
		return ProgramArtifact{}, err
	}
	if sha == (effects.Digest{}) || ref.Path == "" {
		return ProgramArtifact{}, ErrObservationUnknown
	}
	return ProgramArtifact{Path: ref.Path, SHA256: sha}, nil
}

func nativeRevisionRef(ref ProgramArtifact) workspace.RevisionArtifact {
	return workspace.RevisionArtifact{Path: ref.Path, SHA256: ref.SHA256.String()}
}

func (b *Backend) revisionWorkspace() (remoteWorkspace, error) {
	w, ok := b.Containment.(remoteWorkspace)
	if !ok || w.workspace == nil {
		return remoteWorkspace{}, ErrCapabilitiesUnverified
	}
	return w, nil
}

func (b *Backend) captureHelperInput(ctx context.Context, a *helperAttempt) error {
	owner := b.owner(*a)
	if owner.RevisionRole == "" {
		return nil
	}
	w, err := b.revisionWorkspace()
	if err != nil {
		return err
	}
	ref, err := w.workspace.RepositoryInputRef(ctx, remoteOwner(owner))
	if err != nil {
		return err
	}
	input, err := programmeRevisionRef(ref)
	if err != nil {
		return err
	}
	if a.RevisionInput != nil && *a.RevisionInput != input {
		return ErrObservationUnknown
	}
	a.RevisionInput = &input
	return b.save(*a)
}

// A selected immutable containment failure can survive a crash before its link
// reaches codex.json. Only that actual selected ref can repair the missing link.
func (b *Backend) recoverHelperInputFailure(ctx context.Context, a *helperAttempt) error {
	owner := b.owner(*a)
	if owner.RevisionRole != "verifier" {
		return nil
	}
	if a.PrelaunchError != "" && a.RevisionInputFailure == nil {
		return nil
	}
	w, err := b.revisionWorkspace()
	if err != nil {
		return err
	}
	failure, ref, err := w.workspace.RepositoryInputFailure(ctx, remoteOwner(owner))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	cause, err := programmeRevisionRef(ref)
	if err != nil {
		return err
	}
	input, err := programmeRevisionRef(failure.Input)
	if err != nil {
		return err
	}
	next := helperInputFailure{Cause: cause, Input: input, Reason: failure.Reason, Paths: failure.Paths}
	if a.RevisionInputFailure != nil && !reflect.DeepEqual(*a.RevisionInputFailure, next) {
		return ErrObservationUnknown
	}
	if a.RevisionInput != nil && *a.RevisionInput != input {
		return ErrObservationUnknown
	}
	if next.Reason == "not_restored" {
		if a.Process.PID <= 1 || a.Process.Group <= 1 || a.Process.Birth == "" || a.ThreadID == "" || a.TurnID == "" || !a.TurnIntent || !a.Ended || a.PrelaunchError != "" {
			return ErrObservationUnknown
		}
	} else {
		if a.Process != (ProcessIdentity{}) || a.ThreadID != "" || a.TurnID != "" || a.TurnIntent || a.Ended || a.Proof != nil {
			return ErrObservationUnknown
		}
		message := "revision input: " + next.Reason
		if a.PrelaunchError != "" && a.PrelaunchError != message {
			return ErrObservationUnknown
		}
		a.PrelaunchError = message
	}
	a.RevisionInput, a.RevisionInputFailure = &input, &next
	return b.save(*a)
}

func (b *Backend) fenceFailedHelperInput(ctx context.Context, a *helperAttempt) error {
	if a.RevisionInputFailure == nil || a.RevisionInput == nil || a.RevisionInputFailure.Input != *a.RevisionInput {
		return ErrObservationUnknown
	}
	launched := a.RevisionInputFailure.Reason == "not_restored"
	if !launched && (a.Process != (ProcessIdentity{}) || a.ThreadID != "" || a.TurnID != "" || a.TurnIntent || a.Ended || a.Proof != nil) {
		return ErrObservationUnknown
	}
	if launched && (a.Process.PID <= 1 || a.Process.Group <= 1 || a.Process.Birth == "" || a.ThreadID == "" || a.TurnID == "" || !a.TurnIntent || !a.Ended || a.PrelaunchError != "") {
		return ErrObservationUnknown
	}
	w, err := b.revisionWorkspace()
	if err != nil {
		return err
	}
	owner := remoteOwner(b.owner(*a))
	if err := w.workspace.Fence(ctx, owner); err != nil {
		return err
	}
	if launched {
		if err := stopOwnedProcess(ctx, a.Process); err != nil {
			return err
		}
	}
	if _, err := w.workspace.ObserveFailedRepositoryInput(ctx, owner, nativeRevisionRef(a.RevisionInputFailure.Cause)); err != nil {
		return err
	}
	a.Fenced = true
	return b.save(*a)
}

func helperRevisionFailedResult(a helperAttempt) helper.RunResult {
	return helper.RunResult{Outcome: "failed", ErrorCode: "codex_revision_input_failed", ErrorText: a.PrelaunchError}
}

func retainRevisionTurnProof(a helperAttempt, observed PersistedTurn) (PersistedTurn, error) {
	if a.Proof == nil {
		return observed, nil
	}
	if a.Proof.VerifiedAt.IsZero() || observed.VerifiedAt.IsZero() {
		return PersistedTurn{}, ErrObservationUnknown
	}
	comparison := observed
	comparison.VerifiedAt = a.Proof.VerifiedAt
	if !reflect.DeepEqual(*a.Proof, comparison) {
		return PersistedTurn{}, ErrObservationUnknown
	}
	return *a.Proof, nil
}

func (b *Backend) revisionFailureResult(ctx context.Context, a helperAttempt) (helper.RunResult, error) {
	if a.RevisionInputFailure == nil {
		return helperResult(a), nil
	}
	w, err := b.revisionWorkspace()
	if err != nil {
		return helper.RunResult{}, err
	}
	if a.RevisionInputFailure.Reason == "not_restored" {
		if a.Process.PID <= 1 || a.Process.Group <= 1 || syscall.Kill(a.Process.PID, 0) != syscall.ESRCH || syscall.Kill(-a.Process.Group, 0) != syscall.ESRCH {
			return helper.RunResult{}, ErrObservationUnknown
		}
	}
	closure, err := w.workspace.ObserveFailedRepositoryInput(ctx, remoteOwner(b.owner(a)), nativeRevisionRef(a.RevisionInputFailure.Cause))
	if err != nil {
		return helper.RunResult{}, err
	}
	containment, err := programmeRevisionRef(closure)
	if err != nil {
		return helper.RunResult{}, err
	}
	attemptName := filepath.Join(b.directory(a.Job), "codex.json")
	data, err := readProgramArtifactFile(attemptName)
	if err != nil {
		return helper.RunResult{}, err
	}
	attempt := ProgramArtifact{Path: attemptName, SHA256: sha256.Sum256(data)}
	proof := RevisionInputFailure{Version: 2, Job: a.Job, Attempt: attempt, Containment: containment, Input: a.RevisionInputFailure.Input, Reason: a.RevisionInputFailure.Reason, Paths: a.RevisionInputFailure.Paths}
	ref, err := saveHelperRevisionArtifact(helperRevisionFailurePath(b, a.Job, attempt.SHA256), proof)
	if err != nil {
		return helper.RunResult{}, err
	}
	if _, err := ReadRevisionInputFailureLocal(ctx, b.Brain, ref); err != nil {
		return helper.RunResult{}, err
	}
	result := helperResult(a)
	if a.PrelaunchError != "" {
		result = helperRevisionFailedResult(a)
	}
	result.RevisionInputFailure = &helper.RevisionInputFailureRef{Path: ref.Path, SHA256: ref.SHA256.String()}
	return result, nil
}

// ReadRevisionInputFailureLocal never probes a process, Git, a model or the
// helper ledger. Actual closure was observed before this immutable ref existed.
func ReadRevisionInputFailureLocal(ctx context.Context, brain string, ref ProgramArtifact) (RevisionInputFailure, error) {
	var out RevisionInputFailure
	if err := ctx.Err(); err != nil {
		return out, err
	}
	data, err := readProgramArtifact(ref)
	if err != nil {
		return out, err
	}
	if err := strictDecodeRevision(data, &out); err != nil {
		return out, err
	}
	j := out.Job
	if out.Version != 2 || j.ID == "" || filepath.Base(j.ID) != j.ID || j.ID == "." || j.ID == ".." || j.Round < 1 || j.Generation < 1 || j.Scope.CodexContract != 2 || j.Request.Component != "verifier" || j.Route.Provider != "codex" {
		return out, ErrObservationUnknown
	}
	b := &Backend{Brain: brain}
	if ref.Path != helperRevisionFailurePath(b, j, out.Attempt.SHA256) || out.Attempt.Path != helperRevisionPath(b, j, "codex.json") {
		return out, ErrObservationUnknown
	}
	if _, err := readProgramArtifact(out.Attempt); err != nil {
		return out, err
	}
	a, err := b.read(j)
	if err != nil {
		return out, err
	}
	routes := a.Selection.Routes[j.Request.Component]
	if a.Selection.Executor != "codex" || a.Selection.EpicID != j.Scope.EpicID || len(routes) != 1 || routes[0] != j.Route || j.Request.OwnerID != j.Scope.OwnerID || j.Request.WorkID != j.Scope.WorkID {
		return out, ErrObservationUnknown
	}
	if !reflect.DeepEqual(a.Job, out.Job) || !a.Closed || !a.Fenced || a.RevisionInputFailure == nil || a.RevisionInput == nil || *a.RevisionInput != out.Input || a.RevisionInputFailure.Input != out.Input || a.RevisionInputFailure.Reason != out.Reason || !reflect.DeepEqual(a.RevisionInputFailure.Paths, out.Paths) {
		return out, ErrObservationUnknown
	}
	if _, err := readProgramArtifact(a.RevisionInputFailure.Cause); err != nil {
		return out, err
	}
	if _, err := readProgramArtifact(out.Input); err != nil {
		return out, err
	}
	if _, err := readProgramArtifact(out.Containment); err != nil {
		return out, err
	}
	failure, err := workspace.ReadFailedRepositoryInputLocal(ctx, remoteOwner(b.owner(a)), nativeRevisionRef(a.RevisionInputFailure.Cause), nativeRevisionRef(out.Containment))
	if err != nil {
		return out, err
	}
	if failure.Reason != out.Reason || failure.Input != nativeRevisionRef(out.Input) || !reflect.DeepEqual(failure.Paths, out.Paths) {
		return out, ErrObservationUnknown
	}
	if out.Reason == "not_restored" {
		if _, _, err := b.readSettledChild(ctx, j); err != nil {
			return out, err
		}
	} else if a.Process != (ProcessIdentity{}) || a.ThreadID != "" || a.TurnID != "" || a.TurnIntent || a.Ended || a.Proof != nil || a.PrelaunchError != "revision input: "+out.Reason {
		return out, ErrObservationUnknown
	}
	return out, nil
}

func strictDecodeRevision(data []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ErrObservationUnknown
	}
	return nil
}

func saveHelperRevisionArtifact(path string, value any) (ProgramArtifact, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return ProgramArtifact{}, err
	}
	if err := publishProgramReadArtifact(path, data); err != nil {
		return ProgramArtifact{}, err
	}
	return ProgramArtifact{Path: path, SHA256: sha256.Sum256(data)}, nil
}

func helperRevisionPath(b *Backend, job helper.Job, name string) string {
	return filepath.Join(b.directory(job), name)
}

func helperRevisionFailurePath(b *Backend, job helper.Job, attemptSHA effects.Digest) string {
	return helperRevisionPath(b, job, "revision-input-failure-"+attemptSHA.String()+".json")
}

func revisionSettlement(o helper.ChildWorkspaceObservation) workspace.WorkspaceSettlement {
	return workspace.WorkspaceSettlement{OwnerSHA256: o.OwnerSHA256, ContainmentRef: o.ContainmentRef, ContainmentSHA256: o.ContainmentSHA256, PromotionRef: o.PromotionRef, PromotionSHA256: o.PromotionSHA256, PublishedGitSHA256: o.PublishedGitSHA256}
}

func (b *Backend) ObserveHelperRevision(ctx context.Context, jobID string, round int) (HelperRevisionObservation, error) {
	var out HelperRevisionObservation
	if jobID == "" || filepath.Base(jobID) != jobID || jobID == "." || jobID == ".." || round < 1 {
		return out, ErrObservationUnknown
	}
	var archived helperAttempt
	if err := readJSON(filepath.Join(b.directory(helper.Job{ID: jobID, Round: round}), "codex.json"), &archived); err != nil {
		return out, err
	}
	j := archived.Job
	if j.ID != jobID || j.Round != round || j.Scope.CodexContract != 2 || b.owner(archived).RevisionRole == "" {
		return out, ErrObservationUnknown
	}
	service := helper.Service{Brain: b.Brain}
	current, err := service.Job(jobID)
	if err != nil {
		return out, err
	}
	result, err := service.Result(jobID, round)
	if err != nil {
		return out, err
	}
	if current.ID != j.ID || current.Scope != j.Scope || current.Request != j.Request || current.Route != j.Route || current.Round < round || current.Round == round && current.Generation != j.Generation || result.JobID != j.ID || result.Round != round || result.State != "completed" || result.Recovery == nil || !result.Recovery.Stopped || !result.Recovery.DescendantsStopped || !result.Recovery.PublicationSettled || result.Recovery.StoredScope != j.Scope {
		return out, ErrObservationUnknown
	}
	observedWorkspace, err := b.ObserveChildWorkspace(ctx, j)
	if err != nil {
		return out, err
	}
	w, err := b.revisionWorkspace()
	if err != nil {
		return out, err
	}
	if _, _, err := w.workspace.ObserveRepositoryRevision(ctx, remoteOwner(b.owner(archived))); err != nil {
		return out, err
	}
	inputRef, finalRef, err := workspace.ReadRepositoryRevisionRefsLocal(ctx, remoteOwner(b.owner(archived)), revisionSettlement(observedWorkspace))
	if err != nil {
		return out, err
	}
	input, err := programmeRevisionRef(inputRef)
	if err != nil {
		return out, err
	}
	final, err := programmeRevisionRef(finalRef)
	if err != nil {
		return out, err
	}
	if archived.RevisionInput == nil || *archived.RevisionInput != input || archived.RevisionInputFailure != nil {
		return out, ErrObservationUnknown
	}
	return HelperRevisionObservation{Job: j, Workspace: observedWorkspace, Input: input, Final: final}, nil
}

func (b *Backend) CommitHelperRevisionLocked(ctx context.Context, observed HelperRevisionObservation) (ProgramArtifact, error) {
	var result ProgramArtifact
	if _, err := b.revisionWorkspace(); err != nil {
		return result, err
	}
	err := b.WithChildWorkspaceGuard(ctx, observed.Job, observed.Workspace, func(guard helper.ChildWorkspaceGuard) error {
		archived, _, err := b.readSettledChild(ctx, observed.Job)
		if err != nil || !reflect.DeepEqual(archived.Job, observed.Job) || archived.RevisionInput == nil || *archived.RevisionInput != observed.Input || archived.RevisionInputFailure != nil {
			return ErrObservationUnknown
		}
		settlement, err := guard.ValidateLocal(ctx)
		if err != nil {
			return err
		}
		actual, ok := guard.(childWorkspaceGuard)
		if !ok {
			return ErrCapabilitiesUnverified
		}
		remote, ok := actual.remote.(workspace.RepositoryRevisionGuard)
		if !ok {
			return ErrCapabilitiesUnverified
		}
		inputRef, finalRef, err := remote.RevisionRefsLocal(ctx)
		if err != nil {
			return err
		}
		if inputRef != nativeRevisionRef(observed.Input) || finalRef != nativeRevisionRef(observed.Final) {
			return ErrObservationUnknown
		}
		input, err := readProgramArtifact(observed.Input)
		if err != nil {
			return err
		}
		final, err := readProgramArtifact(observed.Final)
		if err != nil {
			return err
		}
		var inputFacts workspace.RepositoryInput
		var finalFacts workspace.RepositoryFinal
		if strictDecodeRevision(input, &inputFacts) != nil || strictDecodeRevision(final, &finalFacts) != nil || finalFacts.Input != inputRef {
			return ErrObservationUnknown
		}
		result, err = saveHelperRevisionArtifact(helperRevisionPath(b, observed.Job, "revision.json"), HelperRevisionReceipt{Observation: observed, Settlement: settlement})
		return err
	})
	return result, err
}

func ReadHelperRevisionLocal(ctx context.Context, brain string, ref ProgramArtifact) (HelperRevisionReceipt, error) {
	var out HelperRevisionReceipt
	if err := ctx.Err(); err != nil {
		return out, err
	}
	data, err := readProgramArtifact(ref)
	if err != nil {
		return out, err
	}
	if err := strictDecodeRevision(data, &out); err != nil {
		return out, err
	}
	j := out.Observation.Job
	if j.ID == "" || filepath.Base(j.ID) != j.ID || j.ID == "." || j.ID == ".." || j.Round < 1 || j.Generation < 1 || j.Scope.CodexContract != 2 || j.Route.Provider != "codex" {
		return out, ErrObservationUnknown
	}
	b := &Backend{Brain: brain}
	if ref.Path != helperRevisionPath(b, j, "revision.json") {
		return out, ErrObservationUnknown
	}
	a, attemptSHA, err := b.readSettledChild(ctx, j)
	if err != nil {
		return out, err
	}
	if b.owner(a).RevisionRole == "" || a.RevisionInput == nil || *a.RevisionInput != out.Observation.Input || a.RevisionInputFailure != nil {
		return out, ErrObservationUnknown
	}
	o := out.Observation.Workspace
	if !reflect.DeepEqual(a.Job, j) || o != out.Settlement.Observation || o.AttemptRef != filepath.Join(b.directory(j), "codex.json") || o.AttemptSHA256 != attemptSHA || o.JobID != j.ID || o.Generation != j.Generation || o.Round != j.Round || out.Settlement.FinalLocalSHA256 != hash([]byte("factory-child-workspace-2\x00"+attemptSHA+o.ContainmentSHA256+o.PromotionSHA256+o.PublishedGitSHA256)) {
		return out, ErrObservationUnknown
	}
	input, final, err := workspace.ReadRepositoryRevisionRefsLocal(ctx, remoteOwner(b.owner(a)), revisionSettlement(o))
	if err != nil {
		return out, err
	}
	if input != nativeRevisionRef(out.Observation.Input) || final != nativeRevisionRef(out.Observation.Final) {
		return out, ErrObservationUnknown
	}
	if _, err := readProgramArtifact(out.Observation.Input); err != nil {
		return out, err
	}
	if _, err := readProgramArtifact(out.Observation.Final); err != nil {
		return out, err
	}
	return out, nil
}

func ReadHelperRepositoryRevisionLocal(ctx context.Context, brain string, ref ProgramArtifact) (workspace.RepositoryInput, workspace.RepositoryFinal, error) {
	var input workspace.RepositoryInput
	var final workspace.RepositoryFinal
	receipt, err := ReadHelperRevisionLocal(ctx, brain, ref)
	if err != nil {
		return input, final, err
	}
	data, err := readProgramArtifact(receipt.Observation.Input)
	if err != nil {
		return input, final, err
	}
	if err := strictDecodeRevision(data, &input); err != nil {
		return input, final, err
	}
	data, err = readProgramArtifact(receipt.Observation.Final)
	if err != nil {
		return input, final, err
	}
	if err := strictDecodeRevision(data, &final); err != nil {
		return input, final, err
	}
	return input, final, nil
}

// This copy is for managed tool paths only. The original owner remains the
// authorization, containment and journal identity throughout its lifecycle.
func (b *Backend) helperToolOwner(ctx context.Context, a helperAttempt, environment RemoteEnvironment) (Owner, error) {
	owner := b.owner(a)
	if owner.CodexContract != 2 || owner.RevisionRole != "verifier" {
		return owner, nil
	}
	if err := ctx.Err(); err != nil {
		return Owner{}, err
	}
	if a.Job.ID == "" || filepath.Base(a.Job.ID) != a.Job.ID || a.Job.ID == "." || a.Job.ID == ".." || a.Job.Generation < 1 || a.Job.Round < 1 || a.RevisionInput == nil || a.RevisionInputFailure != nil {
		return Owner{}, ErrObservationUnknown
	}
	stored, err := b.read(a.Job)
	if err != nil || !reflect.DeepEqual(stored.Job, a.Job) || stored.RevisionInput == nil || *stored.RevisionInput != *a.RevisionInput || stored.RevisionInputFailure != nil {
		return Owner{}, ErrObservationUnknown
	}
	root := filepath.Join(owner.RunDir, "revision")
	cwd := filepath.Join(root, "workspace")
	if a.RevisionInput.Path != filepath.Join(root, "repository-input.json") || environment.Cwd != cwd || !reflect.DeepEqual(environment.Roots, []string{cwd}) {
		return Owner{}, ErrObservationUnknown
	}
	data, err := readProgramArtifact(*a.RevisionInput)
	if err != nil {
		return Owner{}, err
	}
	var input workspace.RepositoryInput
	if err := strictDecodeRevision(data, &input); err != nil {
		return Owner{}, err
	}
	ownerData, err := json.Marshal(remoteOwner(owner))
	if err != nil {
		return Owner{}, err
	}
	if input.Version != 2 || input.OwnerSHA256 != hash(ownerData) || input.SnapshotWorktree != cwd || input.SnapshotGit != filepath.Join(root, "source.git") || input.IndexTree != input.Tree {
		return Owner{}, ErrObservationUnknown
	}
	actual, err := filepath.EvalSymlinks(cwd)
	if err != nil || actual != cwd {
		return Owner{}, ErrObservationUnknown
	}
	owner.Worktree = cwd
	return owner, nil
}
