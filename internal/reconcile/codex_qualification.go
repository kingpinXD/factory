package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
)

const codexQualificationRef = "codex-qualification:"

func init() { needsUserReasons = append(needsUserReasons, (*run).codexQualificationChanged) }

func qualificationRef(old nativeActivation, next codex.Contract2AcceptanceRef) string {
	return codexQualificationRef + nativeDigest(old) + ":" + next.ReceiptSHA256
}

func qualificationRetry(owner *entity, ref string) (events.Event, bool) {
	seq := codexDeliveryRetry(owner, ref)
	ev, ok := eventBySeq(owner, fmt.Sprint(seq))
	return ev, ok && ev.Request == RequestRetry
}

func codexCoordinatorOwner(owner *entity) bool {
	return owner.epic != nil || owner.set != nil || owner.work != nil && handedOver(owner)
}

func (r *run) codexQualificationChanged(owner *entity) (string, bool) {
	if !codexCoordinatorOwner(owner) || r.providerForEpic(owner.entry.Epic) != "codex" {
		return "", false
	}
	old, previous, found, err := nativeReadActivation(owner)
	if err != nil || !found {
		return "", false
	}
	selected, err := NativeSelection(r.d.Brain, owner.entry.Epic)
	if err != nil {
		return "", false
	}
	next, err := r.verificationAcceptance()(r.ctx, r.d.Brain, selected)
	if err != nil || next == old.Acceptance {
		return "", false
	}
	ref := qualificationRef(old, next)
	_, released := qualificationRetry(owner, ref)
	if released {
		expected := OwnerAuthority{EpicID: owner.entry.Epic, OwnerID: owner.id, Kind: owner.entry.Kind, Revision: nativeDigest(old), Activation: nativeEventRef(owner.id, previous)}
		if _, err := qualificationRetirementAuthority(r.ctx, r.d.Brain, expected, r.verificationAcceptance()); err != nil {
			return ref + ":authority-changed", true
		}
		if runtime, ok := r.runtimeFor("codex").(*codex.Runtime); ok {
			_, name := r.sessionSlot(owner, sessionComponent(owner))
			view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
			if err != nil || view.Stage == "sealing" && !view.NextCheckAt.IsZero() && (view.RecoveryReason != "closure_unconfirmed" || !r.now.Before(view.NextCheckAt)) {
				return ref + ":closure-unconfirmed", true
			}
			if view.Stage == "closed" {
				if err := runtime.ValidateAttemptClosure(r.ctx, codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}); err != nil {
					return ref + ":closure-unconfirmed", true
				}
			}
			if view.Stage == "closed" && r.qualificationHelpersSettled(owner) != nil {
				return ref + ":helpers-unsettled", true
			}
		}
	}
	return ref, !released
}

// Retirement can revoke the exact historical owner after qualification changes.
// Its program-owned activation records the receipt verified before admission;
// revocation does not require that old binary to qualify again. It cannot
// authorize a control, helper, process claim or GitHub write.
func qualificationRetirementAuthority(ctx context.Context, brain string, expected OwnerAuthority, verify nativeAcceptance) (nativeActivation, error) {
	_, old, err := nativeAuthorityUsing(ctx, brain, expected.EpicID, expected.OwnerID, false, &expected, verify, nativeQualificationRetirementBasis)
	return old, err
}

func validateQualificationClose(ctx context.Context, brain string, request CoordinatorCloseRequest, verify nativeAcceptance) error {
	old, err := qualificationRetirementAuthority(ctx, brain, request.Authority, verify)
	if err != nil {
		return err
	}
	_, owner, err := nativeOwner(brain, request.Authority.EpicID, request.Authority.OwnerID)
	if err != nil || !codexCoordinatorOwner(owner) || request.Work != nil || request.Cause.OwnerID != owner.id {
		return effects.ErrAuthority
	}
	return validateQualificationCloseEvidence(owner, old, request)
}

func validateQualificationCloseEvidence(owner *entity, old nativeActivation, request CoordinatorCloseRequest) error {
	for _, move := range owner.evs {
		if move.Kind != events.KindTransition || move.Sender != events.SenderProgram || move.From != stateNeedsYou || move.Trigger != "user" || move.TriggerRef != fmt.Sprint(request.Cause.Sequence) {
			continue
		}
		for _, hold := range owner.evs {
			if hold.Seq >= move.Seq || hold.Kind != events.KindTransition || hold.Sender != events.SenderProgram || hold.To != stateNeedsYou || !strings.HasPrefix(hold.TriggerRef, codexQualificationRef+nativeDigest(old)+":") {
				continue
			}
			if retry, ok := qualificationRetry(owner, hold.TriggerRef); ok && nativeEventRef(owner.id, retry) == request.Cause {
				return nil
			}
		}
	}
	return effects.ErrAuthority
}

// The manager owns tick. It records shutdown here; the pre-tick close worker
// performs physical fencing and settlement before any replacement activation.
func (r *run) refreshCodexQualification(owner, sess *entity, runtime *codex.Runtime, verify nativeAcceptance) (bool, error) {
	if !codexCoordinatorOwner(owner) {
		return false, nil
	}
	old, previous, found, err := nativeReadActivation(owner)
	if err != nil || !found {
		return false, err
	}
	selected, err := NativeSelection(r.d.Brain, owner.entry.Epic)
	if err != nil {
		return true, err
	}
	next, err := verify(r.ctx, r.d.Brain, selected)
	if err != nil {
		return true, err
	}
	ix, current, err := nativeOwner(r.d.Brain, owner.entry.Epic, owner.id)
	if err != nil {
		return true, err
	}
	basis, err := nativeBasis(ix, current)
	if err != nil {
		return true, err
	}
	leaseChanged := nativeLeaseOnlyAdvance(ix, current, old.Basis, basis)
	if next == old.Acceptance && !leaseChanged {
		// A previous tick may have appended the parent activation before a
		// dependent work renewal failed. Finish it before selecting a successor.
		if owner.set != nil && old.Previous != nil {
			view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
			if err != nil {
				return true, err
			}
			if view.Stage == "closed" {
				return false, r.renewSetQualificationWork(owner, old, nativeEventRef(owner.id, previous), view, runtime, verify)
			}
		}
		return false, nil
	}
	retry, released := qualificationRetry(owner, qualificationRef(old, next))
	mode := "qualification_refresh"
	cause := nativeEventRef(owner.id, retry)
	if next == old.Acceptance && leaseChanged {
		mode, released = "lease_refresh", true
		for _, b := range basis {
			if b.OwnerID == owner.id {
				cause = b.Events[len(b.Events)-1]
			}
		}
	}
	if !released || owner.cur.State == stateNeedsYou {
		if owner.cur.State != stateNeedsYou {
			return true, fmt.Errorf("authority qualification changed")
		}
		return true, nil
	}
	expected := OwnerAuthority{EpicID: owner.entry.Epic, OwnerID: owner.id, Kind: owner.entry.Kind, Revision: nativeDigest(old), Activation: nativeEventRef(owner.id, previous)}
	if _, err = qualificationRetirementAuthority(r.ctx, r.d.Brain, expected, verify); err != nil {
		return true, err
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if err != nil {
		return true, err
	}
	if view.OwnerID != owner.id || view.EpicID != owner.entry.Epic || view.Name != sess.session.Name {
		return true, effects.ErrAuthority
	}
	if view.Stage == "sealing" {
		return true, nil
	}
	if view.Stage != "closed" {
		request := CoordinatorCloseRequest{Version: 2, Attempt: view.Ref, Authority: expected, Cause: cause, Mode: mode, Reason: "shutdown"}
		if err := validateRenewalClose(r.ctx, r.d.Brain, request, verify); err != nil {
			return true, err
		}
		data, err := json.Marshal(request)
		if err != nil {
			return true, err
		}
		_, err = appendCoordinatorEvent(r, sess, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "close", Text: string(data), Key: coordinatorCloseKey(request)})
		return true, err
	}
	if err := runtime.ValidateAttemptClosure(r.ctx, codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}); err != nil {
		return true, err
	}
	if view.SealReason != "shutdown" && view.SealReason != "host_failure" && view.SealReason != "unknown_submission" && view.SealReason != "prelaunch_failure" && view.SealReason != "quota_pause" {
		return true, effects.ErrAuthority
	}
	if err := r.qualificationHelpersSettled(owner); err != nil {
		return true, err
	}
	ix, current, err = nativeOwner(r.d.Brain, owner.entry.Epic, owner.id)
	if err != nil {
		return true, err
	}
	basis, err = nativeBasis(ix, current)
	if err != nil || !reflect.DeepEqual(old.Basis, basis) && !nativeLeaseOnlyAdvance(ix, current, old.Basis, basis) {
		return true, effects.ErrAuthority
	}
	old.RenewalClosure = &codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}
	old.Basis, old.Acceptance, old.AfterSeq = basis, next, len(current.evs)
	ref := nativeEventRef(owner.id, previous)
	old.Previous = &ref
	data, err := json.Marshal(old)
	if err != nil {
		return true, err
	}
	activation, err := r.append(owner, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: codexAuthorityRecipient, Request: "activate", Text: string(data), Key: "codex-authority:" + nativeDigest(old)})
	if err != nil {
		return true, err
	}
	if err := syncOwnership(owner); err != nil {
		return true, err
	}
	return false, r.renewSetQualificationWork(owner, old, nativeEventRef(owner.id, activation), view, runtime, verify)
}

func (r *run) renewSetQualificationWork(owner *entity, activation nativeActivation, parent effects.EventRef, closed codex.CoordinatorAttemptView, runtime *codex.Runtime, verify nativeAcceptance) error {
	if owner.set == nil || activation.Previous == nil || activation.RenewalClosure == nil {
		return nil
	}
	proof := codex.AttemptClosureRef{Attempt: closed.Ref, ClosureSHA256: closed.ClosureSHA256}
	if *activation.RenewalClosure != proof {
		return nil
	}
	if closed.Stage != "closed" || closed.OwnerID != owner.id || closed.EpicID != owner.entry.Epic {
		return effects.ErrAuthority
	}
	if err := runtime.ValidateAttemptClosure(r.ctx, proof); err != nil {
		return err
	}
	ix, currentOwner, err := nativeOwner(r.d.Brain, owner.entry.Epic, owner.id)
	if err != nil {
		return err
	}
	ancestors, err := nativeRenewalAncestors(ix, currentOwner, activation, proof)
	if err != nil {
		return err
	}
	currentParent, _, err := nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, nil, verify)
	if err != nil || currentParent.Activation != parent {
		return effects.ErrAuthority
	}
	if err := r.qualificationHelpersSettled(owner); err != nil {
		return err
	}
	items, err := r.itemsOf(owner)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.work == nil || item.work.Set != owner.id || item.entry.Epic != owner.entry.Epic || !item.open() || handedOver(item) {
			continue
		}
		_, current, err := nativeOwner(r.d.Brain, item.entry.Epic, item.id)
		if err != nil {
			return err
		}
		old, previous, found, err := nativeReadActivation(current)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		scope, err := (NativeAuthority{r.d.Brain}).resolveBase(r.ctx, helper.Request{WorkID: item.id, OwnerID: owner.id, SetID: owner.id, Lease: owner.st.Lease, Component: "implementor"}, false, false, false, false)
		if err != nil || scope.OwnerID != owner.id {
			return effects.ErrAuthority
		}
		if old.Acceptance == activation.Acceptance && old.OwnerActivation != nil && *old.OwnerActivation == parent {
			if scope.Generation != old.WorkGeneration {
				return effects.ErrAuthority
			}
			if _, _, err := nativeAuthority(r.ctx, r.d.Brain, item.entry.Epic, item.id, true, nil, verify); err != nil {
				return err
			}
			continue
		}
		if old.OwnerActivation == nil {
			return effects.ErrAuthority
		}
		oldParent, ancestor := ancestors[*old.OwnerActivation]
		if !ancestor || old.Acceptance != oldParent.Acceptance || old.SelectionHash != activation.SelectionHash {
			return effects.ErrAuthority
		}
		expected := OwnerAuthority{EpicID: item.entry.Epic, OwnerID: item.id, Kind: item.entry.Kind, Revision: nativeDigest(old), Activation: nativeEventRef(item.id, previous)}
		if _, err := qualificationRetirementAuthority(r.ctx, r.d.Brain, expected, verify); err != nil {
			return err
		}
		if err := r.qualificationHelpersSettled(current); err != nil {
			return err
		}
		previousRef := nativeEventRef(item.id, previous)
		old.Previous, old.OwnerActivation = &previousRef, &parent
		basis, err := nativeBasis(r.ix, current)
		if err != nil || !reflect.DeepEqual(old.Basis, basis) && !nativeLeaseOnlyAdvance(r.ix, current, old.Basis, basis) {
			return effects.ErrAuthority
		}
		if scope.Generation != old.WorkGeneration && !nativeLeaseOnlyAdvance(r.ix, current, old.Basis, basis) {
			return effects.ErrAuthority
		}
		old.Basis, old.WorkGeneration = basis, scope.Generation
		old.Acceptance, old.AfterSeq = activation.Acceptance, len(current.evs)
		data, err := json.Marshal(old)
		if err != nil {
			return err
		}
		item.evs = current.evs
		if _, err = r.append(item, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: codexAuthorityRecipient, Request: "activate", Text: string(data), Key: "codex-authority:" + nativeDigest(old)}); err != nil {
			return err
		}
		if err := syncOwnership(item); err != nil {
			return err
		}
		r.invalidateAvailability()
	}
	return nil
}

func (r *run) qualificationHelpersSettled(owner *entity) error {
	service := r.d.Helpers
	if service == nil {
		service = &helper.Service{Brain: r.d.Brain}
	}
	jobs, err := service.Jobs()
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.Scope.OwnerID != owner.id && job.Request.OwnerID != owner.id && (owner.work == nil || job.Scope.WorkID != owner.id && job.Request.WorkID != owner.id) {
			continue
		}
		result, err := service.Result(job.ID, job.Round)
		if err != nil {
			return err
		}
		if result.Recovery == nil || !result.Recovery.Stopped || !result.Recovery.DescendantsStopped || !result.Recovery.PublicationSettled {
			return fmt.Errorf("qualification renewal waits for settled helper %s round %d", job.ID, job.Round)
		}
	}
	return nil
}

type qualificationSettlementKey struct{}

type qualificationSettlement struct {
	RequestRef effects.EventRef
	Request    CoordinatorCloseRequest
}

// This capability travels only through the exact qualification shutdown. The
// validator also supplies the control being checked; neither value works alone.
func qualificationSettlementContext(ctx context.Context, ref effects.EventRef, request CoordinatorCloseRequest) context.Context {
	return context.WithValue(ctx, qualificationSettlementKey{}, qualificationSettlement{ref, request})
}

func qualificationSettlementAllowed(ctx context.Context, brain string, expected OwnerAuthority, verify nativeAcceptance) bool {
	retirement, ok := ctx.Value(qualificationSettlementKey{}).(qualificationSettlement)
	if !ok || retirement.Request.Authority.EpicID != expected.EpicID || retirement.Request.Mode != "qualification_refresh" && retirement.Request.Mode != "lease_refresh" || retirement.Request.Reason != "shutdown" {
		return false
	}
	control, ok := controlrecord.OutcomeValidation(ctx)
	attempt := retirement.Request.Attempt
	if !ok || control.Attempt != (effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}) {
		return false
	}
	originalRef, original, err := controlrecord.FindControlOriginal(brain, control)
	if err != nil || original.Control == nil || *original.Control != control {
		return false
	}
	enrollmentRef, _, err := controlrecord.FindEnrollmentSnapshot(brain, originalRef)
	if err != nil {
		return false
	}
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(brain, enrollmentRef)
	if err != nil || enrollment.Receipt.Control != control {
		return false
	}
	view, err := codex.New(brain, "").InspectHistoricalCoordinatorAttempt(ctx, attempt)
	if err != nil || view.Ref != attempt || view.OwnerID != retirement.Request.Authority.OwnerID || view.EpicID != retirement.Request.Authority.EpicID || view.Stage != "sealing" && view.Stage != "closed" || view.CloseCause == nil || *view.CloseCause != retirement.RequestRef {
		return false
	}
	event, err := (ProgramEffects{Brain: brain}).boundEffectEvent(retirement.RequestRef)
	data, marshalErr := json.Marshal(retirement.Request)
	if err != nil || marshalErr != nil || event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != "codex-attempt" || event.Request != "close" || event.Key != coordinatorCloseKey(retirement.Request) || event.Text != string(data) {
		return false
	}
	if validateRenewalClose(ctx, brain, retirement.Request, verify) != nil {
		return false
	}
	if expected == retirement.Request.Authority {
		return true
	}
	// A set's enrolled work-target outcome also retains its exact historical
	// work authority. This exception grants only validation inside that seal.
	if retirement.Request.Authority.Kind != blueprint.MachineSet || expected.Kind != blueprint.MachineWork || enrollment.Input.Target.WorkID != expected.OwnerID {
		return false
	}
	_, work, err := nativeOwner(brain, expected.EpicID, expected.OwnerID)
	if err != nil || work.work == nil || work.work.Set != retirement.Request.Authority.OwnerID || handedOver(work) {
		return false
	}
	oldWork, err := qualificationRetirementAuthority(ctx, brain, expected, verify)
	if err != nil || oldWork.OwnerActivation == nil || *oldWork.OwnerActivation != retirement.Request.Authority.Activation || oldWork.WorkGeneration == "" {
		return false
	}
	oldOwner, err := qualificationRetirementAuthority(ctx, brain, retirement.Request.Authority, verify)
	return err == nil && oldWork.Acceptance == oldOwner.Acceptance
}
