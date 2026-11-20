package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

// This permits retirement only. Admission always requires the exact current
// basis. No inputs, registrations, transfers or boundaries may change here.
func nativeLeaseOnlyAdvance(ix store.Index, owner *entity, old, current []nativeOwnerBasis) bool {
	setID := ""
	if owner.set != nil {
		setID = owner.id
	}
	if owner.work != nil && !handedOver(owner) {
		setID = owner.work.Set
	}
	if setID == "" || len(old) != len(current) {
		return false
	}
	advanced := false
	for i, before := range old {
		after := current[i]
		if reflect.DeepEqual(before, after) {
			continue
		}
		if before.OwnerID != setID || before.OwnerID != after.OwnerID || before.Kind != after.Kind || before.Inputs != after.Inputs || before.InputsSHA256 != after.InputsSHA256 || len(after.Events) <= len(before.Events) || !slices.Equal(before.Events, after.Events[:len(before.Events)]) {
			return false
		}
		entry, err := ix.Lookup(setID)
		if err != nil {
			return false
		}
		evs, err := nativeCompleteLog(entry.Dir)
		if err != nil {
			return false
		}
		lease := 0
		for j, ref := range after.Events {
			if ref.OwnerID != setID || ref.Sequence < 1 || ref.Sequence > len(evs) {
				return false
			}
			ev := evs[ref.Sequence-1]
			if nativeEventRef(setID, ev) != ref {
				return false
			}
			grant := ev.Sender == events.SenderProgram && ev.Kind == events.KindHeartbeat && ev.Key == leaseKey(ev.Lease) && ev.Lease > 0
			if j < len(before.Events) {
				if grant {
					lease = ev.Lease
				}
				continue
			}
			if !grant || lease < 1 || ev.Lease != lease+1 {
				return false
			}
			lease = ev.Lease
		}
		advanced = true
	}
	return advanced
}

func validateLeaseClose(ctx context.Context, brain string, request CoordinatorCloseRequest, verify nativeAcceptance) error {
	old, err := qualificationRetirementAuthority(ctx, brain, request.Authority, verify)
	if err != nil {
		return err
	}
	ix, owner, err := nativeOwner(brain, request.Authority.EpicID, request.Authority.OwnerID)
	if err != nil {
		return err
	}
	basis, err := nativeBasis(ix, owner)
	if err != nil {
		return err
	}
	if owner.set == nil || request.Work != nil || !nativeLeaseOnlyAdvance(ix, owner, old.Basis, basis) {
		return effects.ErrAuthority
	}
	// The exact grant is durable authorization to retire this lease's holder.
	for _, b := range basis {
		if b.OwnerID != owner.id {
			continue
		}
		for _, before := range old.Basis {
			if before.OwnerID == owner.id && slices.Contains(b.Events[len(before.Events):], request.Cause) {
				return nil
			}
		}
	}
	return effects.ErrAuthority
}

func validateRenewalClose(ctx context.Context, brain string, request CoordinatorCloseRequest, verify nativeAcceptance) error {
	if request.Mode == "lease_refresh" && request.Reason == "shutdown" {
		return validateLeaseClose(ctx, brain, request, verify)
	}
	if request.Mode == "qualification_refresh" && request.Reason == "shutdown" {
		return validateQualificationClose(ctx, brain, request, verify)
	}
	return effects.ErrAuthority
}

// A grant can arrive after the activation append but before successor selection.
// All intermediate renewals must retain the same durable closure proof.
func nativeRenewalAncestors(ix store.Index, owner *entity, activation nativeActivation, proof codex.AttemptClosureRef) (map[effects.EventRef]nativeActivation, error) {
	chain := map[effects.EventRef]nativeActivation{}
	if proof.ClosureSHA256 == "" {
		return nil, effects.ErrAuthority
	}
	for activation.Previous != nil && activation.RenewalClosure != nil && *activation.RenewalClosure == proof {
		ref := *activation.Previous
		if ref.OwnerID != owner.id || ref.Sequence < 1 || ref.Sequence > len(owner.evs) {
			return nil, effects.ErrAuthority
		}
		ev := owner.evs[ref.Sequence-1]
		var prior nativeActivation
		if nativeEventRef(owner.id, ev) != ref || json.Unmarshal([]byte(ev.Text), &prior) != nil || activation.SelectionHash != prior.SelectionHash {
			return nil, effects.ErrAuthority
		}
		if !reflect.DeepEqual(prior.Basis, activation.Basis) && !nativeLeaseOnlyAdvance(ix, owner, prior.Basis, activation.Basis) {
			return nil, effects.ErrAuthority
		}
		if activation.Acceptance != prior.Acceptance {
			retry, ok := qualificationRetry(owner, qualificationRef(prior, activation.Acceptance))
			if !ok || retry.Seq >= activation.AfterSeq+1 {
				return nil, effects.ErrAuthority
			}
		}
		if err := nativeValidateBasis(ix, prior.Basis); err != nil {
			return nil, err
		}
		chain[ref] = prior
		if prior.RenewalClosure == nil || *prior.RenewalClosure != proof {
			return chain, nil
		}
		activation = prior
	}
	return nil, effects.ErrAuthority
}

// Historical recovery accounting must not depend on today's qualification or
// latest lease. All authority here comes from the immutable selected transfer.
func (a *factoryAttemptAuthority) ValidateGenerationSuccessor(ctx context.Context, previous codex.AttemptRef, spec session.Spec) error {
	view, err := a.runtime.InspectHistoricalCoordinatorAttempt(ctx, previous)
	if err != nil {
		return err
	}
	ix, owner, err := nativeOwner(a.brain, view.EpicID, view.OwnerID)
	if err != nil || owner.set == nil || view.Stage != "closed" {
		return effects.ErrAuthority
	}
	id := sessionID(owner.id, componentOrchestrator)
	entry, err := ix.Lookup(id)
	if err != nil {
		return err
	}
	sess, err := loadEntity(id, entry)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("select:%s:%s", previous.SessionID, previous.AttemptID)
	ev, ok := sess.byKey(key)
	var intent CoordinatorSelectionIntent
	if !ok || ev.Kind != events.KindIntent || ev.Sender != events.SenderProgram || ev.Recipient != "codex-attempt" || ev.Request != "select" || json.Unmarshal([]byte(ev.Text), &intent) != nil {
		return effects.ErrAuthority
	}
	canonical, _ := json.Marshal(intent)
	if string(canonical) != ev.Text || intent.Version != 2 || intent.Previous.Attempt != previous || intent.Previous.ClosureSHA256 != view.ClosureSHA256 || intent.Mode != "process_replace" || !reflect.DeepEqual(intent.CloseRequest, view.CloseCause) || intent.Transition != nil || intent.TaskRestart != nil {
		return effects.ErrAuthority
	}
	if err := a.runtime.ValidateAttemptClosure(ctx, intent.Previous); err != nil {
		return err
	}
	current, err := nativeHistoricalActivation(ix, owner, intent.Authority)
	if err != nil || current.RenewalClosure == nil || *current.RenewalClosure != intent.Previous {
		return effects.ErrAuthority
	}
	ancestors, err := nativeRenewalAncestors(ix, owner, current, intent.Previous)
	if err != nil {
		return err
	}
	root := current
	for _, candidate := range ancestors {
		if root.AfterSeq > candidate.AfterSeq {
			root = candidate
		}
	}
	if !nativeLeaseOnlyAdvance(ix, owner, root.Basis, current.Basis) {
		return effects.ErrAuthority
	}
	oldLease, newLease := 0, 0
	for _, pair := range []struct {
		basis []nativeOwnerBasis
		lease *int
	}{{root.Basis, &oldLease}, {current.Basis, &newLease}} {
		for _, b := range pair.basis {
			if b.OwnerID != owner.id {
				continue
			}
			for _, ref := range b.Events {
				ev := owner.evs[ref.Sequence-1]
				if ev.Kind == events.KindHeartbeat && ev.Key == leaseKey(ev.Lease) {
					*pair.lease = ev.Lease
				}
			}
		}
	}
	if newLease <= oldLease || view.Generation != fmt.Sprintf("%s:%d:%s", owner.id, oldLease, sess.session.Component) || spec.Generation != fmt.Sprintf("%s:%d:%s", owner.id, newLease, sess.session.Component) {
		return effects.ErrAuthority
	}
	return nil
}

func nativeHistoricalActivation(ix store.Index, owner *entity, authority OwnerAuthority) (nativeActivation, error) {
	var rec nativeActivation
	if _, _, _, err := nativeReadActivation(owner); err != nil {
		return rec, err
	}
	ref := authority.Activation
	if authority.OwnerID != owner.id || authority.EpicID != owner.entry.Epic || authority.Kind != owner.entry.Kind || ref.OwnerID != owner.id || ref.Sequence < 1 || ref.Sequence > len(owner.evs) {
		return rec, effects.ErrAuthority
	}
	ev := owner.evs[ref.Sequence-1]
	if ev.Recipient != codexAuthorityRecipient || nativeEventRef(owner.id, ev) != ref || json.Unmarshal([]byte(ev.Text), &rec) != nil || nativeDigest(rec) != authority.Revision {
		return rec, effects.ErrAuthority
	}
	return rec, nativeValidateBasis(ix, rec.Basis)
}
