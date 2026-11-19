package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

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

func (r *run) codexQualificationChanged(owner *entity) (string, bool) {
	if owner.epic == nil || r.providerForEpic(owner.entry.Epic) != "codex" {
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
			_, name := r.sessionSlot(owner, componentPlanner)
			view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
			if err != nil || view.Stage == "sealing" && !view.NextCheckAt.IsZero() {
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
	if err != nil || owner.epic == nil || request.Work != nil || request.Cause.OwnerID != owner.id {
		return effects.ErrAuthority
	}
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
	if owner.epic == nil {
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
	if next == old.Acceptance {
		return false, nil
	}
	retry, released := qualificationRetry(owner, qualificationRef(old, next))
	if !released || owner.cur.State == stateNeedsYou {
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
	if view.Stage != "closed" {
		request := CoordinatorCloseRequest{Version: 2, Attempt: view.Ref, Authority: expected, Cause: nativeEventRef(owner.id, retry), Mode: "qualification_refresh", Reason: "shutdown"}
		if err := validateQualificationClose(r.ctx, r.d.Brain, request, verify); err != nil {
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
	if err := r.qualificationHelpersSettled(owner); err != nil {
		return true, err
	}
	ix, current, err := nativeOwner(r.d.Brain, owner.entry.Epic, owner.id)
	if err != nil {
		return true, err
	}
	basis, err := nativeBasis(ix, current)
	if err != nil || !reflect.DeepEqual(old.Basis, basis) {
		return true, effects.ErrAuthority
	}
	old.Acceptance, old.AfterSeq = next, len(current.evs)
	ref := nativeEventRef(owner.id, previous)
	old.Previous = &ref
	data, err := json.Marshal(old)
	if err != nil {
		return true, err
	}
	if _, err = r.append(owner, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: codexAuthorityRecipient, Request: "activate", Text: string(data), Key: "codex-authority:" + nativeDigest(old)}); err != nil {
		return true, err
	}
	return false, syncOwnership(owner)
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
		if job.Scope.OwnerID != owner.id && job.Request.OwnerID != owner.id {
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
	if !ok || retirement.Request.Authority != expected || retirement.Request.Mode != "qualification_refresh" || retirement.Request.Reason != "shutdown" {
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
	if err != nil || view.Ref != attempt || view.OwnerID != expected.OwnerID || view.EpicID != expected.EpicID || view.Stage != "sealing" && view.Stage != "closed" || view.CloseCause == nil || *view.CloseCause != retirement.RequestRef {
		return false
	}
	event, err := (ProgramEffects{Brain: brain}).boundEffectEvent(retirement.RequestRef)
	data, marshalErr := json.Marshal(retirement.Request)
	if err != nil || marshalErr != nil || event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != "codex-attempt" || event.Request != "close" || event.Key != coordinatorCloseKey(retirement.Request) || event.Text != string(data) {
		return false
	}
	return validateQualificationClose(ctx, brain, retirement.Request, verify) == nil
}
