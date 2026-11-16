package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/zednative"
)

type VerifiedRevision struct {
	Version      int
	Authority    OwnerAuthority
	Work         helper.Scope
	Entry        effects.EventRef
	Verifier     effects.Artifact
	Candidate    codex.ProgramArtifact
	Revision     codex.ProgramArtifact
	Checks       codex.ProgramReadResultRef
	Commit, Tree string
}

func registryCheckDisposition(input zednative.RepositoryInput, check zednative.RegistryCheckObservation) string {
	final := check.Final
	if check.Outcome != "completed" || check.ExitCode == nil || check.ExitState.Path == "" || check.Output.Path == "" || check.Content.Path == "" || final.Commit == "" || final.Tree == "" || final.IndexTree == "" || final.ContentSHA256 == "" || final.Manifest.Path == "" || final.Manifest.SHA256 == "" {
		return ""
	}
	if *check.ExitCode != 0 || final.Commit != input.Commit || final.Tree != input.Tree || final.IndexTree != input.IndexTree || final.ContentSHA256 != input.ContentSHA256 {
		return "checks_failed"
	}
	return "passed"
}

func verificationCheckFactsUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, checks codex.ProgramReadResultRef, verify nativeAcceptance) (VerifierCandidate, string, error) {
	candidate, err := readVerifierCandidateLocalUsing(ctx, brain, ref, verify)
	if err != nil {
		return candidate, "", err
	}
	chains, err := observeRegistryCheckChains(ctx, brain, candidate.Work.WorkID)
	if err != nil {
		return candidate, "", err
	}
	for {
		more, err := chains.advance(ctx)
		if err != nil {
			return candidate, "", err
		}
		if !more {
			break
		}
	}
	outcome, err := verificationCheckFactsFromChains(ctx, brain, ref, candidate, checks, chains)
	return candidate, outcome, err
}

func verificationCheckFactsFromChains(ctx context.Context, brain string, ref codex.ProgramArtifact, candidate VerifierCandidate, checks codex.ProgramReadResultRef, chains *registryCheckChains) (string, error) {
	chain := chains.chains[ref]
	if chain == nil {
		return "", effects.ErrEvidence
	}
	request, err := chains.snapshot.ReadRequestLocal(ctx, codex.ProgramReadRef{RequestID: checks.RequestID, SHA256: checks.RequestSHA256})
	if err != nil {
		return "", err
	}
	source, err := registryCheckRequestSource(request)
	if err != nil || source != chain.Leaf {
		return "", effects.ErrEvidence
	}
	check, err := chains.snapshot.ReadResultLocal(ctx, checks)
	if err != nil {
		return "", err
	}
	input, _, err := codex.ReadHelperRepositoryRevisionLocal(ctx, brain, candidate.Revision)
	if err != nil {
		return "", err
	}
	return registryCheckDisposition(input, check), nil
}

func CompleteVerifiedRevisionLocked(ctx context.Context, brain string, ref codex.ProgramArtifact, checks codex.ProgramReadResultRef) (codex.ProgramArtifact, error) {
	return completeVerifiedRevisionLockedUsing(ctx, brain, ref, checks, codex.Contract2AcceptanceAuthority)
}

func completeVerifiedRevisionLockedUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, checks codex.ProgramReadResultRef, verify nativeAcceptance) (codex.ProgramArtifact, error) {
	candidate, outcome, err := verificationCheckFactsUsing(ctx, brain, ref, checks, verify)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	if outcome != "passed" {
		return codex.ProgramArtifact{}, effects.ErrBusy
	}
	round, _, authority, entry, end, err := verifierSource(ctx, brain, candidate.Verifier, true, verify)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	if round.Scope != candidate.Work || round.Round != candidate.Round || authority != candidate.Authority || entry != candidate.Entry || end != candidate.End {
		return codex.ProgramArtifact{}, effects.ErrStale
	}
	next := VerifiedRevision{Version: 2, Authority: candidate.Authority, Work: candidate.Work, Entry: candidate.Entry, Verifier: candidate.Verifier, Candidate: ref, Revision: candidate.Revision, Checks: checks, Commit: candidate.Commit, Tree: candidate.Tree}
	name := filepath.Join(verificationDirectory(brain, candidate.Work.WorkID, entry.Sequence, end.Sequence), "verified-"+effectDigest(next).String()+".json")
	return verificationArtifact(name, next)
}

func ReadVerifiedRevisionLocal(ctx context.Context, brain string, ref codex.ProgramArtifact) (VerifiedRevision, error) {
	return readVerifiedRevisionLocalUsing(ctx, brain, ref, codex.Contract2AcceptanceAuthority)
}

func readVerifiedRevisionLocalUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, verify nativeAcceptance) (VerifiedRevision, error) {
	var out VerifiedRevision
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := readVerificationArtifact(ref, &out); err != nil {
		return out, err
	}
	candidate, outcome, err := verificationCheckFactsUsing(ctx, brain, out.Candidate, out.Checks, verify)
	if err != nil {
		return out, err
	}
	expected := VerifiedRevision{Version: 2, Authority: candidate.Authority, Work: candidate.Work, Entry: candidate.Entry, Verifier: candidate.Verifier, Candidate: out.Candidate, Revision: candidate.Revision, Checks: out.Checks, Commit: candidate.Commit, Tree: candidate.Tree}
	if outcome != "passed" || out != expected || ref.Path != filepath.Join(verificationDirectory(brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence), "verified-"+ref.SHA256.String()+".json") {
		return out, effects.ErrEvidence
	}
	return out, nil
}

func programmeArtifact(ref codex.ProgramArtifact) effects.Artifact {
	return effects.Artifact{Path: ref.Path, SHA256: ref.SHA256}
}

func programmeArtifactRef(ref effects.Artifact) (codex.ProgramArtifact, error) {
	if ref.ProducerJobID != "" || ref.Round != 0 || ref.ScopeSHA256 != (effects.Digest{}) {
		return codex.ProgramArtifact{}, effects.ErrEvidence
	}
	return codex.ProgramArtifact{Path: ref.Path, SHA256: ref.SHA256}, nil
}

func verificationDecisionFactsUsing(ctx context.Context, brain string, next helper.VerificationDecision, verify nativeAcceptance) (VerifierCandidate, error) {
	var none VerifierCandidate
	candidateRef, err := programmeArtifactRef(next.Candidate)
	if err != nil {
		return none, err
	}
	checksRef, err := programmeArtifactRef(next.Checks)
	if err != nil {
		return none, err
	}
	var checks codex.ProgramReadResultRef
	if err := readVerificationArtifact(checksRef, &checks); err != nil {
		return none, err
	}
	candidate, outcome, err := verificationCheckFactsUsing(ctx, brain, candidateRef, checks, verify)
	if err != nil {
		return none, err
	}
	dir := verificationDirectory(brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
	if next.Version != 2 || next.Work != candidate.Work || next.Entry != candidate.Entry || next.End != candidate.End || next.Round != candidate.Round || outcome == "" || next.Outcome != outcome || checksRef.Path != filepath.Join(dir, "checks-"+checksRef.SHA256.String()+".json") {
		return none, effects.ErrEvidence
	}
	if outcome == "checks_failed" {
		if next.Verified != nil {
			return none, effects.ErrEvidence
		}
		return candidate, nil
	}
	if next.Verified == nil {
		return none, effects.ErrEvidence
	}
	verifiedRef, err := programmeArtifactRef(*next.Verified)
	if err != nil {
		return none, err
	}
	verified, err := readVerifiedRevisionLocalUsing(ctx, brain, verifiedRef, verify)
	if err != nil || verified.Candidate != candidateRef || verified.Checks != checks {
		return none, effects.ErrEvidence
	}
	return candidate, nil
}

func (a NativeAuthority) ValidateVerificationDecisionLocal(ctx context.Context, scope helper.Scope, expected effects.EventRef, next helper.VerificationDecision, source effects.Artifact) error {
	return a.validateVerificationDecisionQualified(ctx, scope, expected, next, source, codex.Contract2AcceptanceAuthority)
}

func (a NativeAuthority) validateVerificationDecisionQualified(ctx context.Context, scope helper.Scope, expected effects.EventRef, next helper.VerificationDecision, source effects.Artifact, verify nativeAcceptance) error {
	ref, err := programmeArtifactRef(source)
	if err != nil {
		return err
	}
	var body helper.VerificationDecision
	if err := readVerificationArtifact(ref, &body); err != nil {
		return err
	}
	if !reflect.DeepEqual(body, next) || scope != scope.Authorization() || scope != next.Work || expected != next.Entry {
		return effects.ErrEvidence
	}
	candidate, err := verificationDecisionFactsUsing(ctx, a.Brain, next, verify)
	if err != nil {
		return err
	}
	if ref.Path != filepath.Join(verificationDirectory(a.Brain, scope.WorkID, expected.Sequence, next.End.Sequence), "decision-"+ref.SHA256.String()+".json") {
		return effects.ErrEvidence
	}
	step, err := a.resolveStepFactsQualified(ctx, scope, "verifier", verify)
	if err != nil {
		return err
	}
	owner, _, err := nativeAuthority(ctx, a.Brain, scope.EpicID, scope.OwnerID, true, nil, verify)
	if err != nil {
		return err
	}
	if owner != candidate.Authority || step != next.Round.Step || step.EntryRef != fmt.Sprintf("%s#%d", scope.WorkID, expected.Sequence) {
		return effects.ErrStale
	}
	return nil
}

var _ helper.VerificationDecisionAuthority = NativeAuthority{}

func ensureCandidateChecksLockedUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, verify nativeAcceptance) (codex.ProgramReadRef, error) {
	candidate, err := readVerifierCandidateLocalUsing(ctx, brain, ref, verify)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	chains, err := observeRegistryCheckChains(ctx, brain, candidate.Work.WorkID)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	for {
		more, err := chains.advance(ctx)
		if err != nil {
			return codex.ProgramReadRef{}, err
		}
		if !more {
			break
		}
	}
	if chain := chains.chains[ref]; chain != nil {
		return prepareRegistryChecksLocked(ctx, brain, chain.Leaf, verify)
	}
	source, err := selectRegistryChecksLockedUsing(ctx, brain, ref, verify)
	if err != nil {
		return codex.ProgramReadRef{}, err
	}
	return prepareRegistryChecksLocked(ctx, brain, source, verify)
}

func completeVerificationDecisionLockedUsing(ctx context.Context, brain string, ref codex.ProgramArtifact, service helper.Service, verify nativeAcceptance) (effects.EventRef, error) {
	var none effects.EventRef
	if service.Brain != brain {
		return none, effects.ErrAuthority
	}
	candidate, err := readVerifierCandidateLocalUsing(ctx, brain, ref, verify)
	if err != nil {
		return none, err
	}
	round, _, owner, entry, end, err := verifierSource(ctx, brain, candidate.Verifier, true, verify)
	if err != nil {
		return none, err
	}
	if round.Round != candidate.Round || round.Scope != candidate.Work || owner != candidate.Authority || entry != candidate.Entry || end != candidate.End {
		return none, effects.ErrStale
	}
	request, err := ensureCandidateChecksLockedUsing(ctx, brain, ref, verify)
	if err != nil {
		return none, err
	}
	status, err := inspectProgramReadUsing(ctx, brain, request, verify)
	if err != nil {
		return none, err
	}
	if status.Stage != "closed" || status.NoAction != nil {
		return none, effects.ErrBusy
	}
	checks, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, brain, request, status.Process)
	if err != nil {
		return none, err
	}
	_, outcome, err := verificationCheckFactsUsing(ctx, brain, ref, checks, verify)
	if err != nil {
		return none, err
	}
	if outcome == "" {
		return none, effects.ErrBusy
	}
	dir := verificationDirectory(brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
	checkArtifact, err := verificationArtifact(filepath.Join(dir, "checks-"+effectDigest(checks).String()+".json"), checks)
	if err != nil {
		return none, err
	}
	next := helper.VerificationDecision{Version: 2, Work: candidate.Work, Entry: entry, End: end, Round: round.Round, Outcome: outcome, Candidate: programmeArtifact(ref), Checks: programmeArtifact(checkArtifact)}
	if outcome == "passed" {
		verified, err := completeVerifiedRevisionLockedUsing(ctx, brain, ref, checks, verify)
		if err != nil {
			return none, err
		}
		artifact := programmeArtifact(verified)
		next.Verified = &artifact
	}
	source, err := verificationArtifact(filepath.Join(dir, "decision-"+effectDigest(next).String()+".json"), next)
	if err != nil {
		return none, err
	}
	return service.AppendVerificationDecision(ctx, entry, next, programmeArtifact(source))
}
