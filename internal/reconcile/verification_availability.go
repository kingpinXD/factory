package reconcile

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func (r *run) verificationCheckHold(work *entity) (*store.AvailabilityHold, error) {
	return r.verificationCheckHoldUsing(work, r.verificationAcceptance())
}

func (r *run) verificationCheckHoldUsing(work *entity, verify nativeAcceptance) (*store.AvailabilityHold, error) {
	if work.work == nil || work.cur.State != "verifying" {
		return nil, nil
	}
	modern, err := r.modernVerificationWork(work)
	if err != nil || !modern {
		return nil, err
	}
	round, actual, owner, entry, end, err := currentVerifierSourceUsing(r.ctx, r.d.Brain, work.id, verify)
	if err != nil {
		return nil, err
	}
	verdict, err := readVerifierVerdict(round.Round.Output)
	if err != nil || verdict != "pass" {
		return nil, err
	}
	hold := &store.AvailabilityHold{Scope: store.AvailabilityScope{Executor: "codex", OwnerID: round.Scope.OwnerID, WorkID: work.id, Generation: round.Scope.Generation}, Source: "verification-checks", SourceID: effectDigest(struct {
		Work       helper.Scope
		Entry, End effects.EventRef
	}{round.Scope, entry, end}).String(), From: r.now, Reason: "verification result pending"}
	key := fmt.Sprintf("verification-decision:%s#%d", work.id, entry.Sequence)
	found := false
	for _, event := range actual.evs {
		if event.Key != key {
			continue
		}
		if found {
			return hold, effects.ErrConflict
		}
		found = true
		decision, err := verificationDecisionEventUsing(r.ctx, r.d.Brain, work.id, event, verify)
		if err != nil {
			return hold, err
		}
		if decision.Work != round.Scope || decision.Entry != entry || decision.End != end || decision.Round != round.Round {
			return hold, effects.ErrStale
		}
	}
	if found {
		return nil, nil
	}
	candidate, err := protectedVerifierCandidateUsing(r.ctx, r.d.Brain, verificationObserveSource(round, owner, entry, end), verify)
	if errors.Is(err, os.ErrNotExist) {
		return hold, nil
	}
	if err != nil {
		return hold, err
	}
	chains, err := observeRegistryCheckChains(r.ctx, r.d.Brain, work.id)
	if err != nil {
		return hold, err
	}
	for {
		more, err := chains.advance(r.ctx)
		if err != nil {
			return hold, err
		}
		if !more {
			break
		}
	}
	chain := chains.chains[candidate]
	if chain == nil {
		return hold, effects.ErrEvidence
	}
	var request *codex.ProgramReadRef
	_, queueOwner, err := nativeOwner(r.d.Brain, round.Scope.EpicID, owner.OwnerID)
	if err != nil {
		return hold, err
	}
	for _, event := range queueOwner.evs {
		if event.Request != programReadQueueRequest {
			continue
		}
		ref, err := codex.ReadProgramReadRequestReference(r.ctx, r.d.Brain, event.Text)
		if err != nil {
			return hold, err
		}
		input, err := codex.ReadProgramReadRequest(r.ctx, r.d.Brain, ref)
		if err != nil {
			return hold, err
		}
		if input.Kind != "program_read" || input.Operation != "registry_checks" {
			continue
		}
		source, err := registryCheckRequestSource(input)
		if err != nil {
			return hold, err
		}
		if source != chain.Leaf {
			continue
		}
		if request != nil && *request != ref {
			return hold, effects.ErrConflict
		}
		copy := ref
		request = &copy
	}
	if request == nil {
		return hold, nil
	}
	metadata, err := observeRegistryCheckRetry(r.ctx, r.d.Brain, work.id, chain, request, verify)
	if err != nil {
		return hold, err
	}
	if metadata.cause != nil {
		if r.b == nil {
			return hold, effects.ErrEvidence
		}
		times := []time.Time{}
		for ref, node := range chains.nodes {
			if node.Cause == nil {
				continue
			}
			if ref.Sequence > len(actual.evs) || nativeEventRef(work.id, actual.evs[ref.Sequence-1]) != ref {
				return hold, effects.ErrEvidence
			}
			times = append(times, actual.evs[ref.Sequence-1].At)
		}
		budget, err := registryCheckRetryBudget(r.now, r.restartLimit("verifier"), times)
		if err != nil {
			return hold, err
		}
		if !budget.Allowed {
			hold.NextCheckAt = budget.NextCheckAt
			return hold, nil
		}
	}
	if metadata.selected != nil && !metadata.selected.NextCheckAt.IsZero() {
		hold.NextCheckAt = metadata.selected.NextCheckAt
		return hold, nil
	}
	input, err := chains.snapshot.ReadRequestLocal(r.ctx, *request)
	if err != nil {
		return hold, err
	}
	if r.now.Before(input.Deadline) {
		hold.NextCheckAt = input.Deadline
	}
	return hold, nil
}
