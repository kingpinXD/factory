package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type modernCoordinatorRecovery interface {
	RecoverAttempts(context.Context) error
	RecoverLegacy(context.Context) error
}

func recoverModernCoordinators(ctx context.Context, d Deps) error {
	runtime, ok := d.Runtimes["codex"].(modernCoordinatorRecovery)
	if !ok {
		return nil
	}
	ix, err := store.ReadIndex(d.Brain)
	if err != nil {
		return err
	}
	selected, selectionErr := indexedCodexSelection(d.Brain, ix)
	if !selected {
		return selectionErr
	}
	var requested error
	if actual, ok := d.Runtimes["codex"].(*codex.Runtime); ok {
		requested = errors.Join(recoverCoordinatorTerminalFences(ctx, d, actual), recoverRequestedCoordinators(ctx, d.Brain, actual))
	}
	return errors.Join(selectionErr, requested, runtime.RecoverAttempts(ctx))
}

func indexedCodexSelection(brain string, ix store.Index) (bool, error) {
	selected := false
	seen := map[string]bool{}
	var holds []error
	for _, id := range slices.Sorted(maps.Keys(ix)) {
		epic := ix[id].Epic
		if epic == "" || seen[epic] {
			continue
		}
		seen[epic] = true
		selection, err := NativeSelection(brain, epic)
		if errors.Is(err, executor.ErrUnsupportedExecutor) {
			continue
		}
		if err != nil {
			holds = append(holds, err)
			continue
		}
		if selection.Executor == "codex" {
			selected = true
		}
	}
	return selected, errors.Join(holds...)
}

type CoordinatorCloseRequest struct {
	Version   int              `json:"version"`
	Attempt   codex.AttemptRef `json:"attempt"`
	Authority OwnerAuthority   `json:"authority"`
	Work      *helper.Scope    `json:"work,omitempty"`
	Cause     effects.EventRef `json:"cause"`
	Reason    string           `json:"reason"`
	Mode      string           `json:"mode"`
}
type CoordinatorCloseRequestRef struct{ Event effects.EventRef }

type coordinatorQuotaCause struct {
	Version         int                   `json:"version"`
	Attempt         codex.AttemptRef      `json:"attempt"`
	SelectionSHA256 string                `json:"selection_sha256"`
	Policy          helper.QuotaPolicy    `json:"policy"`
	Admission       helper.CodexAdmission `json:"admission"`
}

func appendCoordinatorEvent(r *run, sess *entity, want events.Event) (effects.EventRef, error) {
	logged, err := r.append(sess, want)
	if err != nil {
		return effects.EventRef{}, err
	}
	comparison := logged
	comparison.Seq, comparison.At = 0, want.At
	if !reflect.DeepEqual(comparison, want) {
		return effects.EventRef{}, effects.ErrConflict
	}
	if err := syncOwnership(sess); err != nil {
		return effects.EventRef{}, err
	}
	return nativeEventRef(sess.id, logged), nil
}

func (r *run) recordCoordinatorQuotaLocked(sess *entity, view codex.CoordinatorAttemptView, q codexQuotaObservation) (effects.EventRef, error) {
	cause := coordinatorQuotaCause{Version: 2, Attempt: view.Ref, SelectionSHA256: q.SelectionSHA256, Policy: q.Policy, Admission: q.Admission}
	// A saved denial is only a scheduling fact, not a new provider snapshot.
	cause.Admission.Snapshot = helper.QuotaSnapshot{}
	data, err := json.Marshal(cause)
	if err != nil {
		return effects.EventRef{}, err
	}
	previous := "initial"
	for i := len(sess.evs) - 1; i >= 0; i-- {
		ev := sess.evs[i]
		if ev.Recipient != "codex-attempt" || ev.Request != "quota-observation" {
			continue
		}
		if ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram {
			return effects.EventRef{}, effects.ErrEvidence
		}
		if ev.Text == string(data) {
			return nativeEventRef(sess.id, ev), nil
		}
		previous = fmt.Sprint(ev.Seq)
		break
	}
	return appendCoordinatorEvent(r, sess, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "quota-observation", Text: string(data), Key: "codex-quota-observation:" + view.Ref.AttemptID + ":" + previous + ":" + effectDigest(cause).String()})
}

func (r *run) requestCoordinatorCloseLocked(sess *entity, mode, reason string, cause effects.EventRef) (CoordinatorCloseRequestRef, error) {
	runtime, ok := r.runtimeFor("codex").(*codex.Runtime)
	if !ok || sess == nil || sess.session == nil {
		return CoordinatorCloseRequestRef{}, ErrCodexIntegration
	}
	authority, ok := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	if !ok {
		return CoordinatorCloseRequestRef{}, ErrCodexIntegration
	}
	view, err := r.coordinatorAttemptView(sess, runtime)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	owner, _, err := nativeAuthority(r.ctx, r.d.Brain, view.EpicID, view.OwnerID, true, nil, authority.verify)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	request := CoordinatorCloseRequest{Version: 2, Attempt: view.Ref, Authority: owner, Cause: cause, Reason: reason, Mode: mode}
	if e := r.ents[view.OwnerID]; e != nil && e.work != nil {
		scope, err := (NativeAuthority{r.d.Brain}).refreshControlReadScope(r.ctx, e.id, authority.verify)
		if err != nil {
			return CoordinatorCloseRequestRef{}, err
		}
		request.Work = &scope
	}
	if err := validateCoordinatorCloseLocked(r.ctx, r.d.Brain, runtime, authority.verify, sess.id, request, r.d.Now()); err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	if view.CloseCause != nil {
		retained, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(*view.CloseCause)
		if err != nil {
			return CoordinatorCloseRequestRef{}, err
		}
		data, _ := json.Marshal(request)
		if retained.Text != string(data) || retained.Key != coordinatorCloseKey(request) {
			return CoordinatorCloseRequestRef{}, codex.ErrAttemptConflict
		}
		return CoordinatorCloseRequestRef{Event: *view.CloseCause}, nil
	}
	data, err := json.Marshal(request)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	ref, err := appendCoordinatorEvent(r, sess, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "close", Text: string(data), Key: coordinatorCloseKey(request)})
	return CoordinatorCloseRequestRef{Event: ref}, err
}

func coordinatorCloseKey(v CoordinatorCloseRequest) string {
	return fmt.Sprintf("close:%s:%s:%s#%d:%s", v.Attempt.SessionID, v.Attempt.AttemptID, v.Cause.OwnerID, v.Cause.Sequence, v.Reason)
}
func (r *run) coordinatorAttemptView(sess *entity, runtime *codex.Runtime) (codex.CoordinatorAttemptView, error) {
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if err != nil {
		return view, err
	}
	if view.Name != sess.session.Name || view.OwnerID != sess.session.Task || view.EpicID != sess.entry.Epic {
		return view, effects.ErrEvidence
	}
	return view, nil
}

func validateCoordinatorCloseLocked(ctx context.Context, brain string, runtime *codex.Runtime, verify nativeAcceptance, sessionID string, request CoordinatorCloseRequest, now time.Time) error {
	quota := request.Mode == "process_pause" && request.Reason == "quota_pause"
	supervisor := request.Mode == "process_replace" && request.Reason == "shutdown"
	terminal := request.Mode == "explicit_stop" && request.Reason == "cancelled"
	if request.Version != 2 || (!quota && !supervisor && !terminal) || !terminal && request.Cause.OwnerID != sessionID {
		return effects.ErrAuthority
	}
	view, err := runtime.InspectCoordinatorAttempt(ctx, request.Attempt.SessionID)
	if err != nil {
		return err
	}
	if view.Ref != request.Attempt || view.OwnerID != request.Authority.OwnerID || view.EpicID != request.Authority.EpicID {
		return codex.ErrAttemptStale
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	entry, err := ix.Lookup(sessionID)
	if err != nil {
		return err
	}
	registered, err := loadEntity(sessionID, entry)
	if err != nil {
		return err
	}
	if entry.Kind != blueprint.MachineSession || registered.session == nil || registered.session.Name != view.Name || registered.session.Task != view.OwnerID || entry.Epic != view.EpicID {
		return effects.ErrAuthority
	}

	if !view.NextCheckAt.IsZero() && now.Before(view.NextCheckAt) {
		return codex.ErrAttemptRecovery
	}
	sealed := view.Stage == "sealing" || view.Stage == "closed"
	if sealed {
		if view.SealReason != request.Reason || view.CloseCause == nil {
			return codex.ErrAttemptConflict
		}
		stored, err := (ProgramEffects{Brain: brain}).boundEffectEvent(*view.CloseCause)
		if err != nil {
			return err
		}
		data, _ := json.Marshal(request)
		if stored.Kind != events.KindRequest || stored.Sender != events.SenderProgram || stored.Recipient != "codex-attempt" || stored.Request != "close" || stored.Text != string(data) || stored.Key != coordinatorCloseKey(request) {
			return codex.ErrAttemptConflict
		}
	}
	if terminal {
		cause, err := (ProgramEffects{Brain: brain}).boundEffectEvent(request.Cause)
		if err != nil {
			return err
		}
		return validateTerminalCloseLocked(ctx, brain, runtime, verify, sessionID, request, cause, sealed)
	}
	_, _, err = nativeAuthority(ctx, brain, view.EpicID, view.OwnerID, !sealed, &request.Authority, verify)
	if err != nil {
		return err
	}
	selected, err := executor.ReadSelection(brain, view.EpicID)
	if err != nil {
		return err
	}
	ev, err := (ProgramEffects{Brain: brain}).boundEffectEvent(request.Cause)
	if err != nil {
		return err
	}
	if supervisor {
		return validateSupervisorCloseLocked(ctx, brain, runtime, verify, sessionID, request, ev, sealed, now)
	}
	if ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Recipient != "codex-attempt" || ev.Request != "quota-observation" {
		return effects.ErrEvidence
	}
	var cause coordinatorQuotaCause
	if toolcontract.DecodeBroker([]byte(ev.Text), &cause) != nil {
		return effects.ErrEvidence
	}
	canonical, err := json.Marshal(cause)
	if err != nil || string(canonical) != ev.Text || cause.Version != 2 || cause.Attempt != request.Attempt || cause.SelectionSHA256 != selectionHash(selected) || cause.SelectionSHA256 != view.SelectionHash || cause.Admission.Decision.State != "pause" || cause.Admission.NextCheckAt.IsZero() {
		return effects.ErrEvidence
	}
	if request.Work != nil {
		if request.Work.CodexContract != 2 || request.Work.OwnerID != view.OwnerID || request.Work.WorkID != view.OwnerID || request.Work.EpicID != view.EpicID {
			return effects.ErrAuthority
		}
		if !sealed {
			actual, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, view.OwnerID, verify)
			if err != nil {
				return err
			}
			if actual.Authorization() != request.Work.Authorization() {
				return effects.ErrAuthority
			}
		}
	}
	if sealed {
		return nil
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return err
	}
	policy := helper.QuotaPolicy{Hold: float64(b.Values.Usage.NearLimit), Pause: float64(b.Values.Usage.Pause), StaleAfter: time.Duration(b.Values.Usage.StaleAfter)}
	if cause.Policy != policy {
		return effects.ErrStale
	}
	current, err := helper.ReadCodexQuotaWaitLocal(ctx, brain, selected, policy, now)
	if err != nil {
		return err
	}
	if current.Decision != cause.Admission.Decision || !current.NextCheckAt.Equal(cause.Admission.NextCheckAt) || current.Decision.State != "pause" {
		return effects.ErrStale
	}
	return nil
}

type requestedCoordinatorAuthority struct {
	*factoryAttemptAuthority
	requestRef CoordinatorCloseRequestRef
	request    CoordinatorCloseRequest
	sessionID  string
}

func (a *requestedCoordinatorAuthority) WithClosureCauseGuard(ctx context.Context, ref codex.AttemptRef, reason string, close func(context.Context, *effects.EventRef) error) error {
	if ref != a.request.Attempt || reason != a.request.Reason {
		return codex.ErrAttemptStale
	}
	release, err := waitLock(ctx, a.brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	ev, err := (ProgramEffects{Brain: a.brain}).boundEffectEvent(a.requestRef.Event)
	if err != nil {
		return err
	}
	data, _ := json.Marshal(a.request)
	if ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Recipient != "codex-attempt" || ev.Request != "close" || ev.Text != string(data) || ev.Key != coordinatorCloseKey(a.request) {
		return effects.ErrEvidence
	}
	if err := validateCoordinatorCloseLocked(ctx, a.brain, a.runtime, a.verify, a.sessionID, a.request, time.Now().UTC()); err != nil {
		return err
	}
	view, err := a.runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil {
		return err
	}
	if view.CloseCause != nil && *view.CloseCause != a.requestRef.Event {
		return codex.ErrAttemptConflict
	}
	cause := a.requestRef.Event
	return close(ctx, &cause)
}

func recoverRequestedCoordinators(ctx context.Context, brain string, runtime *codex.Runtime) error {
	authority, ok := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	if !ok {
		return ErrCodexIntegration
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(ix))
	for id := range ix {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var holds []error
	for _, id := range ids {
		entry := ix[id]
		if entry.Kind != blueprint.MachineSession {
			continue
		}
		selected, err := NativeSelection(brain, entry.Epic)
		if err != nil {
			holds = append(holds, err)
			continue
		}
		if selected.Executor != "codex" {
			continue
		}
		evs, err := nativeCompleteLog(entry.Dir)
		if err != nil {
			holds = append(holds, err)
			continue
		}
		for _, ev := range evs {
			if ev.Recipient != "codex-attempt" || ev.Request != "close" {
				continue
			}
			var request CoordinatorCloseRequest
			if toolcontract.DecodeBroker([]byte(ev.Text), &request) != nil || ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Key != coordinatorCloseKey(request) {
				holds = append(holds, effects.ErrEvidence)
				continue
			}
			view, err := runtime.InspectCoordinatorAttempt(ctx, request.Attempt.SessionID)
			if err != nil {
				holds = append(holds, err)
				continue
			}
			// This old request can never resolve to the selected successor.
			if view.Ref != request.Attempt {
				continue
			}
			if view.Stage == "closed" {
				if view.CloseCause == nil || *view.CloseCause != nativeEventRef(id, ev) {
					holds = append(holds, codex.ErrAttemptConflict)
					continue
				}
				if err := validateCoordinatorCloseLocked(ctx, brain, runtime, authority.verify, id, request, time.Now().UTC()); err != nil {
					holds = append(holds, err)
					continue
				}
				if err := runtime.ValidateAttemptClosure(ctx, codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}); err != nil {
					holds = append(holds, err)
				}
				continue
			}
			if !view.NextCheckAt.IsZero() && time.Now().Before(view.NextCheckAt) {
				continue
			}
			copyRuntime := *runtime
			copyAuthority := *authority
			copyAuthority.runtime = &copyRuntime
			copyRuntime.AttemptAuthority = &requestedCoordinatorAuthority{factoryAttemptAuthority: &copyAuthority, requestRef: CoordinatorCloseRequestRef{Event: nativeEventRef(id, ev)}, request: request, sessionID: id}
			if err := copyRuntime.CloseAttempt(ctx, request.Attempt, request.Reason); err != nil {
				holds = append(holds, err)
			}
		}
	}
	return errors.Join(holds...)
}
