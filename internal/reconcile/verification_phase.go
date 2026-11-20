package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
)

type nativeVerificationCause struct {
	entry, end int
	start      int
	outcome    string
}

type nativeVerificationReplay struct {
	modernAfter int
	causes      map[int]nativeVerificationCause
}

func observeNativeVerificationReplay(ctx context.Context, brain string, work *entity, verify nativeAcceptance) (*nativeVerificationReplay, error) {
	out := &nativeVerificationReplay{causes: map[int]nativeVerificationCause{}}
	for _, event := range work.evs {
		if event.Recipient == codexAuthorityRecipient {
			out.modernAfter = event.Seq
			break
		}
	}
	if out.modernAfter == 0 {
		return nil, effects.ErrEvidence
	}
	var chains *registryCheckChains
	retained := map[OwnerAuthority]bool{}
	for _, transition := range work.evs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if transition.Seq <= out.modernAfter || transition.Kind != events.KindTransition || transition.From != "verifying" || transition.To != "implementing" && transition.To != "pr_open" {
			continue
		}
		seq, err := strconv.Atoi(transition.TriggerRef)
		if err != nil || seq < 1 || seq >= transition.Seq || seq > len(work.evs) {
			return nil, effects.ErrEvidence
		}
		if _, found := out.causes[seq]; found {
			return nil, effects.ErrConflict
		}
		event := work.evs[seq-1]
		if event.Kind == events.KindEnd {
			cause, err := nativeVerificationFailCause(work, event)
			if err != nil {
				return nil, err
			}
			out.causes[seq] = cause
			continue
		}
		if chains == nil {
			chains, err = observeRegistryCheckChains(ctx, brain, work.id)
			if err != nil {
				return nil, err
			}
			for {
				more, err := chains.advance(ctx)
				if err != nil {
					return nil, err
				}
				if !more {
					break
				}
			}
		}
		next, err := verificationDecisionEventBody(brain, work.id, event)
		if err != nil {
			return nil, err
		}
		candidateRef, err := programmeArtifactRef(next.Candidate)
		if err != nil {
			return nil, err
		}
		candidate, err := chains.snapshot.CandidateLocal(candidateRef)
		if err != nil {
			return nil, err
		}
		if !retained[candidate.Authority] {
			if _, _, err := nativeAuthority(ctx, brain, candidate.Authority.EpicID, candidate.Authority.OwnerID, false, &candidate.Authority, verify); err != nil {
				return nil, err
			}
			retained[candidate.Authority] = true
		}
		end, err := verifierEnd(work, candidate.Round)
		if err != nil || nativeEventRef(work.id, end) != candidate.End {
			return nil, effects.ErrEvidence
		}
		entry, err := verifierEntry(work, candidate.Round, end)
		if err != nil || entry != candidate.Entry {
			return nil, effects.ErrEvidence
		}
		if verdict, err := readVerifierVerdict(candidate.Verifier); err != nil || verdict != "pass" {
			return nil, effects.ErrEvidence
		}
		checkArtifact, err := programmeArtifactRef(next.Checks)
		if err != nil {
			return nil, err
		}
		var checks codex.ProgramReadResultRef
		if err := readVerificationArtifact(checkArtifact, &checks); err != nil {
			return nil, err
		}
		outcome, err := verificationCheckFactsFromChains(ctx, brain, candidateRef, candidate, checks, chains)
		if err != nil {
			return nil, err
		}
		dir := verificationDirectory(brain, work.id, candidate.Entry.Sequence, candidate.End.Sequence)
		if next.Version != 2 || next.Work != candidate.Work || next.Entry != candidate.Entry || next.End != candidate.End || next.Round != candidate.Round || outcome == "" || next.Outcome != outcome || checkArtifact.Path != filepath.Join(dir, "checks-"+checkArtifact.SHA256.String()+".json") {
			return nil, effects.ErrEvidence
		}
		if outcome == "passed" {
			if next.Verified == nil {
				return nil, effects.ErrEvidence
			}
			verifiedRef, err := programmeArtifactRef(*next.Verified)
			if err != nil {
				return nil, err
			}
			var verified VerifiedRevision
			if err := readVerificationArtifact(verifiedRef, &verified); err != nil {
				return nil, err
			}
			expected := VerifiedRevision{Version: 2, Authority: candidate.Authority, Work: candidate.Work, Entry: candidate.Entry, Verifier: candidate.Verifier, Candidate: candidateRef, Revision: candidate.Revision, Checks: checks, Commit: candidate.Commit, Tree: candidate.Tree}
			if verified != expected || verifiedRef.Path != filepath.Join(dir, "verified-"+verifiedRef.SHA256.String()+".json") {
				return nil, effects.ErrEvidence
			}
		} else if next.Verified != nil {
			return nil, effects.ErrEvidence
		}
		out.causes[seq] = nativeVerificationCause{entry: candidate.Entry.Sequence, end: candidate.End.Sequence, outcome: outcome}
	}
	return out, nil
}

func nativeVerificationFailCause(work *entity, end events.Event) (nativeVerificationCause, error) {
	var out nativeVerificationCause
	parts := strings.Split(end.Key, ":")
	var sha effects.Digest
	if end.Kind != events.KindEnd || end.Sender != events.SenderProgram || len(parts) != 4 || parts[0] != "helper" || parts[1] == "" || parts[3] != "end" || events.StepOf(end.File) != verifyStep || sha.UnmarshalText([]byte(end.SHA256)) != nil {
		return out, effects.ErrEvidence
	}
	round, err := strconv.Atoi(parts[2])
	if err != nil || round < 1 || strconv.Itoa(round) != parts[2] {
		return out, effects.ErrEvidence
	}
	key := fmt.Sprintf("helper:%s:%d:start", parts[1], round)
	for _, start := range work.evs[:end.Seq-1] {
		if start.Key != key {
			continue
		}
		if out.start != 0 || start.Kind != events.KindStart || start.Sender != events.SenderProgram || start.File != end.File {
			return out, effects.ErrEvidence
		}
		out.start = start.Seq
	}
	if out.start == 0 {
		return out, effects.ErrEvidence
	}
	verdict, err := readVerifierVerdictFile(end.File, sha)
	if err != nil || verdict != "fail" {
		return out, effects.ErrEvidence
	}
	out.end, out.outcome = end.Seq, "checks_failed"
	return out, nil
}

func nativeConsumedVerification(replay *nativeVerificationReplay, transition events.Event, entry string, after int) bool {
	seq, err := strconv.Atoi(transition.TriggerRef)
	if err != nil || seq <= after || seq >= transition.Seq {
		return false
	}
	cause, ok := replay.causes[seq]
	if !ok || cause.end <= nativeEntrySequence(entry) || cause.end >= transition.Seq || cause.entry != 0 && cause.entry != nativeEntrySequence(entry) || cause.start != 0 && cause.start <= nativeEntrySequence(entry) {
		return false
	}
	return transition.To == "pr_open" && cause.outcome == "passed" || transition.To == "implementing" && cause.outcome == "checks_failed"
}
