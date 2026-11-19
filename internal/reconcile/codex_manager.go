package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type CoordinatorSelectionIntent struct {
	Version      int                     `json:"version"`
	Previous     codex.AttemptClosureRef `json:"previous"`
	Authority    OwnerAuthority          `json:"authority"`
	Mode         string                  `json:"mode"`
	CloseRequest *effects.EventRef       `json:"close_request,omitempty"`
	Transition   *effects.EventRef       `json:"transition,omitempty"`
	TaskRestart  *effects.EventRef       `json:"task_restart,omitempty"`
}

func (r *run) modernCoordinatorRuntime() (*codex.Runtime, nativeAcceptance, error) {
	runtime, ok := r.runtimeFor("codex").(*codex.Runtime)
	if !ok {
		return nil, nil, ErrCodexIntegration
	}
	authority, ok := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	if !ok || authority.runtime != runtime || authority.verify == nil {
		return nil, nil, ErrCodexIntegration
	}
	return runtime, authority.verify, nil
}

func coordinatorRegistrationKey(sess *entity) (string, error) {
	if sess == nil || sess.session == nil {
		return "", effects.ErrEvidence
	}
	for _, ev := range sess.evs {
		if ev.Kind != events.KindTransition {
			continue
		}
		if ev.Sender != events.SenderProgram || ev.From != "" || ev.TriggerRef != "create" {
			return "", effects.ErrEvidence
		}
		ref := nativeEventRef(sess.id, ev)
		return fmt.Sprintf("initial:%s:%d:%s", sess.id, ev.Seq, ref.SHA256.String()), nil
	}
	return "", effects.ErrEvidence
}

// This runs with tick held. Physical closure is exclusively the pre-tick phase.
func (r *run) manageCodexCoordinators() {
	if r.dry {
		return
	}
	if _, ok := r.runtimeFor("codex").(*codex.Runtime); !ok {
		return
	}

	for _, id := range r.order() {
		sess := r.ents[id]
		if sess.session == nil || r.providerForEpic(sess.entry.Epic) != "codex" {
			continue
		}
		owner := r.ents[sess.session.Task]
		if owner == nil || !owner.open() || !sess.open() || !r.needsSession(owner) {
			continue
		}
		if err := r.manageCodexCoordinator(owner, sess); err != nil {
			r.fail("%s: Codex manager: %v", id, err)
		}
	}
}

func (r *run) manageCodexCoordinator(owner, sess *entity) error {
	if r.dry || r.codexDeliveryHeld(owner) {
		return nil
	}
	if nativeBoundaryState(sess).pending() {
		return errNativeBoundary
	}
	if err := r.checkCoordinatorTerminalAdmission(owner); err != nil {
		return err
	}
	runtime, verify, err := r.modernCoordinatorRuntime()
	if err != nil {
		return err
	}
	if handled, err := r.refreshCodexQualification(owner, sess, runtime, verify); handled || err != nil {
		return err
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if errors.Is(err, os.ErrNotExist) {
		return r.prepareInitialCodexCoordinator(owner, sess, runtime, verify)
	}
	if err != nil {
		return err
	}
	if view.Name != sess.session.Name || view.OwnerID != owner.id || view.EpicID != owner.entry.Epic {
		return effects.ErrAuthority
	}
	if err = r.ensureSetWorkAuthorities(owner, verify); err != nil {
		return err
	}
	switch view.Stage {
	case "claimed", "sealing":
		return nil
	case "prepared":
		if view.Predecessor != (codex.AttemptRef{}) {
			if _, err = r.readCoordinatorSelectionLocked(sess, view.RecoveryKey, view.Predecessor, runtime, verify, true); err != nil {
				return err
			}
			proof, err := runtime.ValidateProcessRecovery(r.ctx, view.Predecessor, view.Ref, view.RecoveryKey)
			if err != nil {
				return err
			}
			if err = r.recordProcessRecoveryUsing(sess, proof, verify); err != nil {
				return err
			}
		}
		if !r.accountAllows(owner, false) {
			return nil
		}
		return r.launchCodexAttempt(sess, runtime, view.Ref)
	case "closed":
		closed := codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}
		if err = runtime.ValidateAttemptClosure(r.ctx, closed); err != nil {
			return err
		}
		mode, err := r.coordinatorReplacementMode(sess, view, runtime, verify)
		if err != nil {
			return err
		}
		if mode == "" {
			return nil
		}
		if !r.accountAllows(owner, false) {
			return nil
		}
		authority, _, err := nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, nil, verify)
		if err != nil {
			return err
		}
		intent := CoordinatorSelectionIntent{Version: 2, Previous: closed, Authority: authority, Mode: mode, CloseRequest: view.CloseCause}
		key := fmt.Sprintf("select:%s:%s", view.Ref.SessionID, view.Ref.AttemptID)
		data, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		if _, err = appendCoordinatorEvent(r, sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "select", Text: string(data), Key: key}); err != nil {
			return err
		}
		if _, err = r.readCoordinatorSelectionLocked(sess, key, view.Ref, runtime, verify, true); err != nil {
			return err
		}
		budget, err := r.processRecoveryBudget(owner)
		if err != nil {
			return err
		}
		if !budget.Allowed {
			return nil
		}
		prompt := r.resumePrompt(owner, sess.session.Component, r.unacknowledged(owner, sess))
		spec, err := r.spec(owner, sess.session, prompt)
		if err != nil {
			return err
		}
		next, found, err := runtime.AdoptPreparedCoordinatorAttempt(r.ctx, spec, key, &view.Ref)
		if err != nil {
			return err
		}
		if !found {
			next, err = runtime.SelectSuccessor(r.ctx, view.Ref, key, spec)
			if err != nil {
				return err
			}
		}
		proof, err := runtime.ValidateProcessRecovery(r.ctx, view.Ref, next, key)
		if err != nil {
			return err
		}
		if err = r.recordProcessRecoveryUsing(sess, proof, verify); err != nil {
			return err
		}
		return r.launchCodexAttempt(sess, runtime, next)
	default:
		return codex.ErrObservationUnknown
	}
}

func (r *run) prepareInitialCodexCoordinator(owner, sess *entity, runtime *codex.Runtime, verify nativeAcceptance) error {
	key, err := coordinatorRegistrationKey(sess)
	if err != nil {
		return err
	}
	if err = r.writeIndex(); err != nil {
		return err
	}
	if err = syncOwnership(sess); err != nil {
		return err
	}
	_, _, activated, err := nativeReadActivation(owner)
	if err != nil {
		return err
	}
	if !activated {
		inventory := r.inventory("codex")
		if !r.initialCodexSlotAvailable(runtime, inventory, sess.session.Name) {
			return codex.ErrObservationUnknown
		}
		service := r.d.Helpers
		if service == nil {
			service = &helper.Service{Brain: r.d.Brain}
		}
		jobs, err := service.Jobs()
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if job.Scope.OwnerID == owner.id || job.Request.OwnerID == owner.id {
				return fmt.Errorf("%w: prior helper execution requires transfer", ErrCodexIntegration)
			}
		}
		for _, ev := range owner.evs {
			if ev.Kind == events.KindStart || ev.Kind == events.KindEnd || ev.Kind == events.KindHandover || ev.Kind == events.KindHandback {
				return fmt.Errorf("%w: prior execution requires transfer", ErrCodexIntegration)
			}
		}
		if owner.work != nil {
			return fmt.Errorf("%w: work-owned first activation requires transfer", ErrCodexIntegration)
		}
		if owner.set != nil {
			grant := grantedLease(owner.evs)
			if grant == 0 && owner.st.Lease == 0 {
				owner.st.Lease = 1
				if _, err = r.grantLease(owner, "initial Codex lease for "+key); err != nil {
					return err
				}
			}
			if grantedLease(owner.evs) != 1 || owner.st.Lease != 1 {
				return effects.ErrAuthority
			}
			grants := 0
			for _, ev := range owner.evs {
				if ev.Kind != events.KindHeartbeat || ev.Sender != events.SenderProgram || !strings.HasPrefix(ev.Key, leasePrefix) {
					continue
				}
				if ev.Key != leaseKey(1) || ev.Lease != 1 || ev.Text != "initial Codex lease for "+key {
					return fmt.Errorf("%w: initial lease provenance requires transfer", ErrCodexIntegration)
				}
				grants++
			}
			if grants != 1 {
				return effects.ErrEvidence
			}

			if err = syncOwnership(owner); err != nil {
				return err
			}
		}
	}
	if err = r.ensureCodexAuthorityUsing(owner, verify); err != nil {
		return err
	}
	if owner.evs, err = nativeCompleteLog(owner.entry.Dir); err != nil {
		return err
	}
	if err = r.ensureSetWorkAuthorities(owner, verify); err != nil {
		return err
	}

	if !r.accountAllows(owner, false) {
		return nil
	}
	spec, err := r.spec(owner, sess.session, r.startPrompt(owner, sess.session.Component))
	if err != nil {
		return err
	}
	ref, found, err := runtime.AdoptPreparedCoordinatorAttempt(r.ctx, spec, key, nil)
	if err != nil {
		return err
	}
	if !found {
		inventory := r.inventory("codex")
		if !r.initialCodexSlotAvailable(runtime, inventory, sess.session.Name) {
			return codex.ErrObservationUnknown
		}
		ref, err = runtime.PrepareAttempt(r.ctx, spec, key)
		if err != nil {
			return err
		}
	}
	return r.launchCodexAttempt(sess, runtime, ref)
}

func (r *run) launchCodexAttempt(sess *entity, runtime *codex.Runtime, ref codex.AttemptRef) error {
	if r.codexLaunched == nil {
		r.codexLaunched = map[codex.AttemptRef]bool{}
	}
	if r.codexLaunched[ref] {
		return nil
	}
	r.codexLaunched[ref] = true
	err := r.act("launch selected Codex attempt for "+sess.session.Name, func(ctx context.Context) error { return runtime.LaunchAttempt(ctx, ref) })
	r.invalidateInventory("codex")
	return err
}

func (r *run) coordinatorReplacementMode(sess *entity, view codex.CoordinatorAttemptView, runtime *codex.Runtime, verify nativeAcceptance) (string, error) {
	if view.CloseCause == nil {
		switch view.SealReason {
		case "host_failure", "unknown_submission", "prelaunch_failure":
			return "process_replace", nil
		default:
			return "", effects.ErrEvidence
		}
	}
	ev, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(*view.CloseCause)
	if err != nil {
		return "", err
	}
	var request CoordinatorCloseRequest
	if toolcontract.DecodeBroker([]byte(ev.Text), &request) != nil || request.Attempt != view.Ref || request.Reason != view.SealReason {
		return "", effects.ErrEvidence
	}
	if err = validateCoordinatorCloseLocked(r.ctx, r.d.Brain, runtime, verify, sess.id, request, r.d.Now()); err != nil {
		return "", err
	}
	switch request.Mode {
	case "process_pause", "process_replace", "qualification_refresh":
		if request.Mode == "qualification_refresh" {
			return "process_replace", nil
		}
		return request.Mode, nil
	case "explicit_stop", "owner_transfer":
		return "", nil
	default:
		return "", effects.ErrEvidence
	}
}

func (r *run) readCoordinatorSelectionLocked(sess *entity, key string, previous codex.AttemptRef, runtime *codex.Runtime, verify nativeAcceptance, current bool) (CoordinatorSelectionIntent, error) {
	ev, ok := sess.byKey(key)
	if !ok {
		return CoordinatorSelectionIntent{}, effects.ErrEvidence
	}
	var intent CoordinatorSelectionIntent
	if ev.Kind != events.KindIntent || ev.Sender != events.SenderProgram || ev.Recipient != "codex-attempt" || ev.Request != "select" || toolcontract.DecodeBroker([]byte(ev.Text), &intent) != nil {
		return intent, effects.ErrEvidence
	}
	data, err := json.Marshal(intent)
	if err != nil || string(data) != ev.Text || intent.Version != 2 || intent.Previous.Attempt != previous || key != fmt.Sprintf("select:%s:%s", previous.SessionID, previous.AttemptID) {
		return intent, effects.ErrEvidence
	}
	old, err := runtime.InspectHistoricalCoordinatorAttempt(r.ctx, previous)
	if err != nil {
		return intent, err
	}
	if old.OwnerID != sess.session.Task || old.Name != sess.session.Name || old.EpicID != sess.entry.Epic || old.ClosureSHA256 != intent.Previous.ClosureSHA256 || !reflect.DeepEqual(old.CloseCause, intent.CloseRequest) {
		return intent, effects.ErrEvidence
	}
	if err = runtime.ValidateAttemptClosure(r.ctx, intent.Previous); err != nil {
		return intent, err
	}
	if intent.Transition != nil || intent.TaskRestart != nil {
		return intent, ErrCodexIntegration
	}
	if old.CloseCause == nil {
		if intent.Mode != "process_replace" {
			return intent, effects.ErrEvidence
		}
		switch old.SealReason {
		case "host_failure", "unknown_submission", "prelaunch_failure":
		default:
			return intent, effects.ErrEvidence
		}
	} else {
		requestEv, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(*old.CloseCause)
		if err != nil {
			return intent, err
		}
		var request CoordinatorCloseRequest
		if toolcontract.DecodeBroker([]byte(requestEv.Text), &request) != nil {
			return intent, effects.ErrEvidence
		}
		refresh := request.Mode == "qualification_refresh" && intent.Mode == "process_replace"
		if !refresh && (request.Mode != intent.Mode || request.Authority != intent.Authority) || request.Attempt != previous || request.Reason != old.SealReason {
			return intent, effects.ErrEvidence
		}
		if refresh {
			if err := validateQualificationClose(r.ctx, r.d.Brain, request, verify); err != nil {
				return intent, err
			}
			_, activation, err := nativeAuthority(r.ctx, r.d.Brain, old.EpicID, old.OwnerID, current, &intent.Authority, verify)
			if err != nil || activation.Previous == nil || *activation.Previous != request.Authority.Activation {
				return intent, effects.ErrAuthority
			}
		}
	}
	_, _, err = nativeAuthority(r.ctx, r.d.Brain, old.EpicID, old.OwnerID, current, &intent.Authority, verify)
	return intent, err
}

func (r *run) ensureSetWorkAuthorities(owner *entity, verify nativeAcceptance) error {
	if owner.set == nil {
		return nil
	}
	if _, _, err := nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, nil, verify); err != nil {
		return err
	}
	items, err := r.itemsOf(owner)
	if err != nil {
		return err
	}
	service := r.d.Helpers
	if service == nil {
		service = &helper.Service{Brain: r.d.Brain}
	}
	jobs, err := service.Jobs()
	if err != nil {
		return err
	}
	changed := false
	for _, it := range items {
		if it.work == nil || it.work.Set != owner.id || it.entry.Epic != owner.entry.Epic || !it.open() || handedOver(it) {
			continue
		}
		if _, ready := worktreeReady(it); !ready {
			continue
		}
		_, _, found, err := nativeReadActivation(it)
		if err != nil {
			return err
		}
		if !found {
			for _, job := range jobs {
				if job.Scope.WorkID == it.id || job.Request.WorkID == it.id {
					return fmt.Errorf("%w: prior work execution requires transfer", ErrCodexIntegration)
				}
			}
		}
		if err = r.ensureCodexAuthorityUsing(it, verify); err != nil {
			return err
		}
		changed = changed || !found
		if it.evs, err = nativeCompleteLog(it.entry.Dir); err != nil {
			return err
		}
	}
	if changed {
		r.invalidateAvailability()
	}
	return nil
}

func (r *run) initialCodexSlotAvailable(runtime *codex.Runtime, inventory session.Inventory, name string) bool {
	if !inventory.Complete {
		return false
	}
	lookup := inventory.Lookup(name)
	if lookup.Presence == session.Absent {
		return true
	}
	return lookup.Presence == session.Present && lookup.Session.Stopped() && runtime.ValidateLegacySettlement(r.ctx, lookup.Session.ID) == nil
}
