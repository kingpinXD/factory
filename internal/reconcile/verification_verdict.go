package reconcile

import (
	"context"
	"fmt"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
)

func (r *run) modernVerificationWork(e *entity) (bool, error) {
	if e.work == nil {
		return false, nil
	}
	_, _, found, err := nativeReadActivation(e)
	if found || err != nil {
		return true, err
	}
	owner := r.ents[e.work.Set]
	if owner != nil {
		_, _, found, err = nativeReadActivation(owner)
		if found || err != nil {
			return true, err
		}
	}
	selected, err := NativeSelection(r.d.Brain, e.entry.Epic)
	if err != nil {
		return true, err
	}
	if selected.Executor != "codex" {
		return false, nil
	}
	scope, err := (NativeAuthority{r.d.Brain}).refreshControlReadScope(r.ctx, e.id, codex.Contract2AcceptanceAuthority)
	if err != nil {
		return true, err
	}
	return scope.CodexContract == 2, nil
}

func currentVerifierSourceUsing(ctx context.Context, brain, work string, verify nativeAcceptance) (helper.PublishedRoundEvidence, *entity, OwnerAuthority, effects.EventRef, effects.EventRef, error) {
	var none helper.PublishedRoundEvidence
	var zero effects.EventRef
	a := NativeAuthority{brain}
	scope, err := a.refreshControlReadScope(ctx, work, verify)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	step, err := a.resolveStepFactsQualified(ctx, scope, "verifier", verify)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	round, err := (helper.Service{Brain: brain}).ReadCurrentRootPublishedRound(ctx, scope, step)
	if err != nil {
		return none, nil, OwnerAuthority{}, zero, zero, err
	}
	return verifierSource(ctx, brain, round.Round.Output, true, verify)
}

func (r *run) contract2Verdict(e *entity, expectedEnd events.Event, want string) (bool, string, error) {
	return r.contract2VerdictUsing(e, expectedEnd, want, r.verificationAcceptance())
}

func (r *run) verificationAcceptance() nativeAcceptance {
	if r.verificationQualifier == nil {
		return codex.Contract2AcceptanceAuthority
	}
	return r.verificationQualifier
}

func (r *run) contract2VerdictUsing(e *entity, expectedEnd events.Event, want string, verify nativeAcceptance) (bool, string, error) {
	round, work, _, entry, end, err := currentVerifierSourceUsing(r.ctx, r.d.Brain, e.id, verify)
	if err != nil {
		return false, "", err
	}
	if nativeEventRef(e.id, expectedEnd) != end {
		return false, "", effects.ErrStale
	}
	verdict, err := readVerifierVerdict(round.Round.Output)
	if err != nil {
		return false, "", err
	}
	if verdict == "fail" {
		return want == "fail", seqRef(expectedEnd), nil
	}
	key := fmt.Sprintf("verification-decision:%s#%d", work.id, entry.Sequence)
	var selected events.Event
	for _, event := range work.evs {
		if event.Key != key {
			continue
		}
		if selected.Seq != 0 {
			return false, "", effects.ErrConflict
		}
		selected = event
	}
	if selected.Seq == 0 || e.used(selected) {
		return false, "", nil
	}
	next, err := verificationDecisionEventUsing(r.ctx, r.d.Brain, work.id, selected, verify)
	if err != nil {
		return false, "", err
	}
	if next.Work != round.Scope || next.Entry != entry || next.End != end || next.Round != round.Round {
		return false, "", effects.ErrStale
	}
	passed := want == "pass" && next.Outcome == "passed" || want == "fail" && next.Outcome == "checks_failed"
	if !passed {
		return false, "", nil
	}
	if want == "pass" {
		if err := r.recordVerifiedPublication(e, selected, next); err != nil {
			return false, "", err
		}
	}
	return true, seqRef(selected), nil
}
