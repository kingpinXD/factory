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
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
)

// A candidate records a restored verifier input. It grants no check result or
// permission to publish that revision.
type VerifierCandidate = codex.VerifierCandidate
type RegistryChecksSelection = codex.RegistryChecksSelection

type VerifierCandidateObservation struct {
	WorkID     string
	Entry, End effects.EventRef
	Authority  OwnerAuthority
	Verifier   effects.Artifact
	Round      helper.RevisionRoundRef
	Revision   codex.HelperRevisionObservation
}

func verificationDirectory(brain, work string, entry, end int) string {
	return filepath.Join(store.Factory(brain), "publication", work, "verification", fmt.Sprintf("%d-%d", entry, end))
}

func verificationArtifact(path string, value any) (codex.ProgramArtifact, error) {
	if err := effectWriteImmutable(path, value); err != nil {
		return codex.ProgramArtifact{}, err
	}
	return codex.ProgramArtifact{Path: path, SHA256: effectDigest(value)}, nil
}

func readVerificationArtifact(ref codex.ProgramArtifact, value any) error {
	if ref.SHA256 == (effects.Digest{}) {
		return effects.ErrEvidence
	}
	if err := effectReadJSON(ref.Path, value); err != nil {
		return err
	}
	if effectDigest(value) != ref.SHA256 {
		return effects.ErrEvidence
	}
	return nil
}

func verifierEnd(e *entity, round helper.RevisionRoundRef) (events.Event, error) {
	var out events.Event
	key := fmt.Sprintf("helper:%s:%d:end", round.JobID, round.Round)
	for _, ev := range e.evs {
		if ev.Key != key {
			continue
		}
		if out.Seq != 0 || ev.Kind != events.KindEnd || ev.Sender != events.SenderProgram || ev.File != round.Output.Path || ev.SHA256 != round.Output.SHA256.String() || events.StepOf(ev.File) != verifyStep {
			return out, effects.ErrEvidence
		}
		out = ev
	}
	if out.Seq == 0 {
		return out, effects.ErrEvidence
	}
	started := false
	for _, ev := range e.evs[:out.Seq-1] {
		if ev.Key != fmt.Sprintf("helper:%s:%d:start", round.JobID, round.Round) {
			continue
		}
		if started || ev.Kind != events.KindStart || ev.Sender != events.SenderProgram || ev.File != out.File {
			return out, effects.ErrEvidence
		}
		started = true
	}
	if !started {
		return out, effects.ErrEvidence
	}
	return out, nil
}

func verifierEntry(e *entity, round helper.RevisionRoundRef, end events.Event) (effects.EventRef, error) {
	prefix := e.id + "#"
	if !strings.HasPrefix(round.Step.EntryRef, prefix) || round.Step.Component != "verifier" {
		return effects.EventRef{}, effects.ErrEvidence
	}
	seq, err := strconv.Atoi(strings.TrimPrefix(round.Step.EntryRef, prefix))
	if err != nil || seq < 1 || seq >= end.Seq || seq > len(e.evs) {
		return effects.EventRef{}, effects.ErrEvidence
	}
	ev := e.evs[seq-1]
	if ev.Seq != seq || ev.Kind != events.KindTransition || ev.Sender != events.SenderProgram || ev.To != "verifying" {
		return effects.EventRef{}, effects.ErrEvidence
	}
	for _, start := range e.evs[:end.Seq-1] {
		if start.Key == fmt.Sprintf("helper:%s:%d:start", round.JobID, round.Round) && start.Seq <= seq {
			return effects.EventRef{}, effects.ErrEvidence
		}
	}
	return nativeEventRef(e.id, ev), nil
}

// This preflight opens the helper ledger, so callers must not hold that ledger.
func verifierSource(ctx context.Context, brain string, output effects.Artifact, current bool, verify nativeAcceptance) (helper.PublishedRoundEvidence, *entity, OwnerAuthority, effects.EventRef, effects.EventRef, error) {
	var none helper.PublishedRoundEvidence
	var zero effects.EventRef
	if output.ProducerJobID == "" || output.Round < 1 {
		return none, nil, OwnerAuthority{}, zero, zero, effects.ErrEvidence
	}
	service := helper.Service{Brain: brain}
	round, err := service.ReadPublishedRound(ctx, output.ProducerJobID, output.Round)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	if round.Round.Output != output || round.Request.Component != "verifier" || round.Scope.CodexContract != 2 || round.Request.ParentJobID != "" {
		return none, nil, OwnerAuthority{}, zero, zero, effects.ErrEvidence
	}
	_, it, err := nativeOwner(brain, round.Scope.EpicID, round.Scope.WorkID)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	end, err := verifierEnd(it, round.Round)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	entry, err := verifierEntry(it, round.Round, end)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	owner, _, err := nativeAuthority(ctx, brain, round.Scope.EpicID, round.Scope.OwnerID, current, nil, verify)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	if current {
		job, err := service.Job(round.Round.JobID)
		if err != nil {
			return none, nil, OwnerAuthority{}, zero, zero, err
		}
		step, err := (NativeAuthority{brain}).resolveStepFactsQualified(ctx, round.Scope, "verifier", verify)
		if err != nil {
			return none, nil, OwnerAuthority{}, zero, zero, err
		}
		if it.cur.State != "verifying" || job.Round != round.Round.Round || job.Generation != round.Round.Generation || step != round.Round.Step {
			return none, nil, OwnerAuthority{}, zero, zero, effects.ErrStale
		}
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	component, ok := b.Components["verifier"]
	if !ok {
		return none, nil, OwnerAuthority{}, zero, zero, effects.ErrEvidence
	}
	missing, err := component.CheckOutput(output.Path)
	if err != nil || len(missing) != 0 {
		return none, nil, OwnerAuthority{}, zero, zero, effects.ErrEvidence
	}
	return round, it, owner, entry, nativeEventRef(it.id, end), nil
}

func observeVerifierCandidate(ctx context.Context, brain string, verifier effects.Artifact) (VerifierCandidateObservation, error) {
	return observeVerifierCandidateUsing(ctx, brain, verifier, codex.Contract2AcceptanceAuthority)
}

func observeVerifierCandidateUsing(ctx context.Context, brain string, verifier effects.Artifact, verify nativeAcceptance) (VerifierCandidateObservation, error) {
	var out VerifierCandidateObservation
	round, _, owner, entry, end, err := verifierSource(ctx, brain, verifier, true, verify)
	if err != nil {
		return out, err
	}
	pass, err := says(verifier.Path, verdictHeading, "pass")
	if err != nil || !pass {
		return out, effects.ErrEvidence
	}
	backend := codex.NewBackend(brain).(*codex.Backend)
	observed, err := backend.ObserveHelperRevision(ctx, round.Round.JobID, round.Round.Round)
	if err != nil {
		return out, err
	}
	if !verifierRevisionMatches(observed.Job, round) {
		return out, effects.ErrEvidence
	}
	return VerifierCandidateObservation{WorkID: round.Scope.WorkID, Entry: entry, End: end, Authority: owner, Verifier: verifier, Round: round.Round, Revision: observed}, nil
}

func verifierRevisionMatches(job helper.Job, round helper.PublishedRoundEvidence) bool {
	return job.ID == round.Round.JobID && job.Round == round.Round.Round && job.Generation == round.Round.Generation && job.Scope.Authorization() == round.Scope && job.Request == round.Request
}

func commitVerifierCandidateLocked(ctx context.Context, brain string, observed VerifierCandidateObservation) (codex.ProgramArtifact, error) {
	return commitVerifierCandidateLockedUsing(ctx, brain, observed, codex.Contract2AcceptanceAuthority)
}

func commitVerifierCandidateLockedUsing(ctx context.Context, brain string, observed VerifierCandidateObservation, verify nativeAcceptance) (codex.ProgramArtifact, error) {
	round, _, owner, entry, end, err := verifierSource(ctx, brain, observed.Verifier, true, verify)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	if observed.WorkID != round.Scope.WorkID || observed.Authority != owner || observed.Entry != entry || observed.End != end || observed.Round != round.Round || !verifierRevisionMatches(observed.Revision.Job, round) {
		return codex.ProgramArtifact{}, effects.ErrStale
	}
	revision, err := codex.NewBackend(brain).(*codex.Backend).CommitHelperRevisionLocked(ctx, observed.Revision)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	input, final, err := codex.ReadHelperRepositoryRevisionLocal(ctx, brain, revision)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	if input.Commit != final.Commit || input.Tree != final.Tree || input.IndexTree != final.IndexTree || input.ContentSHA256 != final.ContentSHA256 {
		return codex.ProgramArtifact{}, effects.ErrEvidence
	}
	next := VerifierCandidate{Version: 2, Authority: owner, Work: round.Scope, Entry: entry, End: end, Verifier: observed.Verifier, Round: round.Round, Revision: revision, Commit: input.Commit, Tree: input.Tree}
	path := filepath.Join(verificationDirectory(brain, next.Work.WorkID, entry.Sequence, end.Sequence), "candidate-"+effectDigest(next).String()+".json")
	return verificationArtifact(path, next)
}

func ObserveVerifierCandidate(ctx context.Context, brain string, verifier effects.Artifact) (codex.ProgramArtifact, error) {
	observed, err := observeVerifierCandidate(ctx, brain, verifier)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	unlock, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	defer unlock()
	return commitVerifierCandidateLocked(ctx, brain, observed)
}

func ReadVerifierCandidateLocal(ctx context.Context, brain string, ref codex.ProgramArtifact) (VerifierCandidate, error) {
	return readVerifierCandidateLocalUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func readVerifierCandidateLocalUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, verify nativeAcceptance) (VerifierCandidate, error) {
	var out VerifierCandidate
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := readVerificationArtifact(ref, &out); err != nil {
		return out, err
	}
	if out.Version != 2 || out.Work != out.Work.Authorization() || out.Work.CodexContract != 2 || out.Authority.EpicID != out.Work.EpicID || out.Authority.OwnerID != out.Work.OwnerID || out.Entry.OwnerID != out.Work.WorkID || out.End.OwnerID != out.Work.WorkID || out.Entry.Sequence < 1 || out.End.Sequence <= out.Entry.Sequence || out.Round.Output != out.Verifier || out.Round.JobID != out.Verifier.ProducerJobID || out.Round.Round != out.Verifier.Round || out.Round.Generation < 1 || out.Round.Step.Component != "verifier" || out.Round.Step.EntryRef != fmt.Sprintf("%s#%d", out.Work.WorkID, out.Entry.Sequence) || ref.Path != filepath.Join(verificationDirectory(brain, out.Work.WorkID, out.Entry.Sequence, out.End.Sequence), "candidate-"+ref.SHA256.String()+".json") {
		return out, effects.ErrEvidence
	}
	if _, _, err := nativeAuthority(ctx, brain, out.Authority.EpicID, out.Authority.OwnerID, false, &out.Authority, verify); err != nil {
		return out, err
	}
	receipt, err := codex.ReadHelperRevisionLocal(ctx, brain, out.Revision)
	if err != nil {
		return out, err
	}
	job := receipt.Observation.Job
	if job.ID != out.Round.JobID || job.Round != out.Round.Round || job.Generation != out.Round.Generation || job.Scope.Authorization() != out.Work || job.Request.Component != "verifier" || job.Request.ParentJobID != "" {
		return out, effects.ErrEvidence
	}
	input, final, err := codex.ReadHelperRepositoryRevisionLocal(ctx, brain, out.Revision)
	if err != nil {
		return out, err
	}
	if input.Commit != out.Commit || input.Tree != out.Tree || final.Commit != out.Commit || final.Tree != out.Tree || input.IndexTree != final.IndexTree || input.ContentSHA256 != final.ContentSHA256 {
		return out, effects.ErrEvidence
	}
	_, it, err := nativeOwner(brain, out.Work.EpicID, out.Work.WorkID)
	if err != nil {
		return out, err
	}
	end, err := verifierEnd(it, out.Round)
	if err != nil || nativeEventRef(it.id, end) != out.End {
		return out, effects.ErrEvidence
	}
	entry, err := verifierEntry(it, out.Round, end)
	if err != nil || entry != out.Entry {
		return out, effects.ErrEvidence
	}
	verdict, err := readVerifierVerdict(out.Verifier)
	if err != nil || verdict != "pass" {
		return out, effects.ErrEvidence
	}
	return out, nil
}

func SelectRegistryChecksLocked(ctx context.Context, brain string, ref codex.ProgramArtifact) (effects.EventRef, error) {
	return selectRegistryChecksLockedUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func selectRegistryChecksLockedUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, verify nativeAcceptance) (effects.EventRef, error) {
	candidate, err := readVerifierCandidateLocalUsing(ctx, brain, ref, verify)
	if err != nil {
		return effects.EventRef{}, err
	}
	_, retainedWork, err := nativeOwner(brain, candidate.Work.EpicID, candidate.Work.WorkID)
	if err != nil {
		return effects.EventRef{}, err
	}
	var retained effects.EventRef
	for _, ev := range retainedWork.evs {
		if err := ctx.Err(); err != nil {
			return effects.EventRef{}, err
		}
		if ev.Request != "registry-checks" {
			continue
		}
		var existing RegistryChecksSelection
		var sha effects.Digest
		if sha.UnmarshalText([]byte(ev.SHA256)) != nil || ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Recipient != events.RecipientRepoWorker {
			return effects.EventRef{}, effects.ErrEvidence
		}
		if err := readVerificationArtifact(codex.ProgramArtifact{Path: ev.File, SHA256: sha}, &existing); err != nil {
			return effects.EventRef{}, err
		}
		if existing.Version != 2 || ev.Key != "registry-checks:"+effectDigest(existing).String() {
			return effects.EventRef{}, effects.ErrEvidence
		}
		if existing.Candidate != ref {
			continue
		}
		if retained.Sequence != 0 {
			return effects.EventRef{}, fmt.Errorf("%w: registry checks require ordinary worker observation", effects.ErrBusy)
		}
		retained = nativeEventRef(retainedWork.id, ev)
	}
	if retained.Sequence != 0 {
		selected, _, _, err := readRegistryChecksSelectionLocalUsing(ctx, brain, retained, verify)
		if err != nil {
			return effects.EventRef{}, err
		}
		if selected.Retry != nil {
			return effects.EventRef{}, fmt.Errorf("%w: registry checks require ordinary worker observation", effects.ErrBusy)
		}
		if _, err := prepareRegistryChecksLocked(ctx, brain, retained, verify); err != nil {
			return retained, err
		}
		return retained, nil
	}
	round, it, owner, entry, end, err := verifierSource(ctx, brain, candidate.Verifier, true, verify)
	if err != nil {
		return effects.EventRef{}, err
	}
	if candidate.Authority != owner || candidate.Round != round.Round || candidate.Entry != entry || candidate.End != end {
		return effects.EventRef{}, effects.ErrStale
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, candidate.Work.WorkID); err != nil {
		return effects.EventRef{}, err
	}
	registered, err := repos.ByFullName(brain, it.work.Repo)
	if err != nil {
		return effects.EventRef{}, err
	}
	policy, err := repos.ReadCheckPolicy(brain, registered.Name)
	if err != nil {
		return effects.EventRef{}, err
	}
	recipe, err := captureRegistryCheckReadRecipeLocked(ctx, brain, candidate, policy, verify)
	if err != nil {
		return effects.EventRef{}, err
	}
	dir := verificationDirectory(brain, it.id, entry.Sequence, end.Sequence)
	policyRef, err := verificationArtifact(filepath.Join(dir, "policy-"+effectDigest(policy).String()+".json"), policy)
	if err != nil {
		return effects.EventRef{}, err
	}
	selection := RegistryChecksSelection{Version: 2, Candidate: ref, Policy: policyRef, ReadRecipe: recipe}
	return publishRegistryCheckSelectionLocked(ctx, brain, candidate, selection, verify)
}

func publishRegistryCheckSelectionLocked(ctx context.Context, brain string, candidate VerifierCandidate, selection RegistryChecksSelection, verify nativeAcceptance) (effects.EventRef, error) {
	dir := verificationDirectory(brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
	_, it, err := nativeOwner(brain, candidate.Work.EpicID, candidate.Work.WorkID)
	if err != nil {
		return effects.EventRef{}, err
	}
	source, err := verificationArtifact(filepath.Join(dir, "selection-"+effectDigest(selection).String()+".json"), selection)
	if err != nil {
		return effects.EventRef{}, err
	}
	key := "registry-checks:" + effectDigest(selection).String()
	want := events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: "registry-checks", File: source.Path, SHA256: source.SHA256.String(), Key: key}
	logged, err := events.Append(store.EventsPath(it.entry.Dir), want)
	if err != nil {
		return effects.EventRef{}, err
	}
	comparison := logged
	comparison.Seq, comparison.At = 0, want.At
	if !reflect.DeepEqual(comparison, want) {
		return effects.EventRef{}, effects.ErrConflict
	}
	f, err := filepath.EvalSymlinks(store.EventsPath(it.entry.Dir))
	if err != nil || f != store.EventsPath(it.entry.Dir) {
		return effects.EventRef{}, effects.ErrEvidence
	}
	file, err := os.Open(f)
	if err != nil {
		return effects.EventRef{}, err
	}
	err = file.Sync()
	closeErr := file.Close()
	if err != nil {
		return effects.EventRef{}, err
	}
	if closeErr != nil {
		return effects.EventRef{}, closeErr
	}
	if err := effectSyncDir(filepath.Dir(f)); err != nil {
		return effects.EventRef{}, err
	}
	sourceRef := nativeEventRef(it.id, logged)
	if _, err := prepareRegistryChecksLocked(ctx, brain, sourceRef, verify); err != nil {
		return sourceRef, err
	}
	return sourceRef, nil
}

// This retained reader never grants current admission or opens the helper
// ledger. A successor may read a completed check after its proposer has ended.
func ReadRegistryChecksSelectionLocal(ctx context.Context, brain string, ref effects.EventRef) (RegistryChecksSelection, VerifierCandidate, repos.CheckPolicy, error) {
	return readRegistryChecksSelectionLocalUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func readRegistryChecksSelectionLocalUsing(ctx context.Context, brain string, ref effects.EventRef, verify nativeAcceptance) (RegistryChecksSelection, VerifierCandidate, repos.CheckPolicy, error) {
	var selection RegistryChecksSelection
	var candidate VerifierCandidate
	var policy repos.CheckPolicy
	ev, err := (ProgramEffects{Brain: brain}).boundEffectEvent(ref)
	if err != nil {
		return selection, candidate, policy, err
	}
	var sourceSHA effects.Digest
	if sourceSHA.UnmarshalText([]byte(ev.SHA256)) != nil || ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Recipient != events.RecipientRepoWorker || ev.Request != "registry-checks" {
		return selection, candidate, policy, effects.ErrEvidence
	}
	source := codex.ProgramArtifact{Path: ev.File, SHA256: sourceSHA}
	if err := readVerificationArtifact(source, &selection); err != nil {
		return selection, candidate, policy, err
	}
	if selection.Version != 2 || ev.Key != "registry-checks:"+effectDigest(selection).String() {
		return selection, candidate, policy, effects.ErrEvidence
	}
	candidate, err = readVerifierCandidateLocalUsing(ctx, brain, selection.Candidate, verify)
	if err != nil {
		return selection, candidate, policy, err
	}
	dir := verificationDirectory(brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
	if ref.OwnerID != candidate.Work.WorkID || ref.Sequence <= candidate.End.Sequence || source.Path != filepath.Join(dir, "selection-"+source.SHA256.String()+".json") || selection.Policy.Path != filepath.Join(dir, "policy-"+selection.Policy.SHA256.String()+".json") {
		return selection, candidate, policy, effects.ErrEvidence
	}
	if err := readVerificationArtifact(selection.Policy, &policy); err != nil {
		return selection, candidate, policy, err
	}
	if selection.ReadRecipe != nil {
		if err := validateRegistryCheckReadRecipeLocal(ctx, brain, candidate, policy, selection.ReadRecipe, verify); err != nil {
			return selection, candidate, policy, err
		}
		return selection, candidate, policy, nil
	}
	_, it, err := nativeOwner(brain, candidate.Work.EpicID, candidate.Work.WorkID)
	if err != nil {
		return selection, candidate, policy, err
	}
	if it.work == nil || validateRetainedRegistryCheckPolicy(brain, it.work.Repo, policy) != nil {
		return selection, candidate, policy, effects.ErrEvidence
	}
	return selection, candidate, policy, nil
}

func validateRetainedRegistryCheckPolicy(brain, repo string, policy repos.CheckPolicy) error {
	var registrySHA effects.Digest
	name, markdown := strings.CutSuffix(filepath.Base(policy.RegistryPath), ".md")
	if !markdown || name == "" || strings.HasPrefix(name, "_") || strings.ContainsAny(name, `/\\`) || policy.RegistryPath != repos.Path(brain, name) {
		return effects.ErrEvidence
	}
	if policy.Version != 2 || repo == "" || !strings.EqualFold(policy.Repo, repo) || len(policy.Commands) == 0 || registrySHA.UnmarshalText([]byte(policy.RegistrySHA256)) != nil || policy.NetworkPolicy != "existing_contained" {
		return effects.ErrEvidence
	}
	for _, command := range policy.Commands {
		if strings.TrimSpace(command) == "" || strings.ContainsAny(command, "\r\n\x00") {
			return effects.ErrEvidence
		}
	}
	return nil
}

func validateNewRegistryCheckPolicy(brain, repo string, selected repos.CheckPolicy) error {
	registered, err := repos.ByFullName(brain, repo)
	if err != nil {
		return err
	}
	current, err := repos.ReadCheckPolicy(brain, registered.Name)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(selected, current) {
		return effects.ErrStale
	}
	return nil
}

// The caller owns tick, and must not hold a helper or E1 transaction.
func ValidateRegistryChecksAdmissionLocal(ctx context.Context, brain string, ref effects.EventRef) error {
	return validateRegistryChecksAdmissionLocalUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func validateRegistryChecksAdmissionLocalUsing(ctx context.Context, brain string, ref effects.EventRef, verify nativeAcceptance) error {
	_, candidate, _, err := readRegistryChecksSelectionLocalUsing(ctx, brain, ref, verify)
	if err != nil {
		return err
	}
	owner, _, err := nativeAuthority(ctx, brain, candidate.Work.EpicID, candidate.Work.OwnerID, true, &candidate.Authority, verify)
	if err != nil {
		return err
	}
	_, it, err := nativeOwner(brain, candidate.Work.EpicID, candidate.Work.WorkID)
	if err != nil {
		return err
	}
	step, err := (NativeAuthority{brain}).resolveStepQualified(ctx, candidate.Work, "verifier", verify)
	if err != nil {
		return err
	}
	if candidate.Authority != owner || it.cur.State != "verifying" || step != candidate.Round.Step || step.EntryRef != fmt.Sprintf("%s#%d", candidate.Entry.OwnerID, candidate.Entry.Sequence) {
		return effects.ErrStale
	}
	return nil
}

// Only first publication of a new immutable read invokes this check. A replay
// or already admitted claim retains its captured policy and execution deadline.
func ValidateNewRegistryChecksReadLocal(ctx context.Context, brain string, ref effects.EventRef) error {
	return validateNewRegistryChecksReadLocalUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func validateNewRegistryChecksReadLocalUsing(ctx context.Context, brain string, ref effects.EventRef, verify nativeAcceptance) error {
	if err := validateRegistryChecksAdmissionLocalUsing(ctx, brain, ref, verify); err != nil {
		return err
	}
	selection, candidate, policy, err := readRegistryChecksSelectionLocalUsing(ctx, brain, ref, verify)
	if err != nil {
		return err
	}
	_, it, err := nativeOwner(brain, candidate.Work.EpicID, candidate.Work.WorkID)
	if err != nil {
		return err
	}
	if it.work == nil {
		return effects.ErrEvidence
	}
	if err := validateRegistryCheckReadRecipeCurrentLocked(ctx, brain, candidate, policy, selection.ReadRecipe, verify); err != nil {
		return err
	}
	return validateNewRegistryCheckPolicy(brain, it.work.Repo, policy)
}
