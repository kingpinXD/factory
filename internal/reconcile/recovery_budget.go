package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type processRecoveryReader interface {
	ValidateProcessRecovery(context.Context, codex.AttemptRef, codex.AttemptRef, string) (codex.ProcessRecoveryRef, error)
	ValidateHistoricalProcessRecovery(context.Context, codex.ProcessRecoveryRef) error
	InspectHistoricalCoordinatorAttempt(context.Context, codex.AttemptRef) (codex.CoordinatorAttemptView, error)
}

type processRecoveryBudget struct {
	Count       int
	Allowed     bool
	NextCheckAt time.Time
}

func recoveryEventKey(sessionID string, ref codex.ProcessRecoveryRef) string {
	return "recovery:" + sessionID + ":" + ref.Previous.AttemptID + ":" + ref.Successor.AttemptID
}

func (r *run) validateRecoveryEvent(owner, logged *entity, ev events.Event) (codex.ProcessRecoveryRef, error) {
	var ref codex.ProcessRecoveryRef
	if ev.Kind != events.KindRecovery || ev.Sender != events.SenderProgram || ev.At.IsZero() || ev.At.After(r.now) || toolcontract.DecodeBroker([]byte(ev.Text), &ref) != nil {
		return ref, fmt.Errorf("invalid process recovery event")
	}
	reader, ok := r.runtimeFor("codex").(processRecoveryReader)
	if !ok {
		return ref, ErrCodexIntegration
	}
	if err := reader.ValidateHistoricalProcessRecovery(r.ctx, ref); err != nil {
		return ref, err
	}
	old, err := reader.InspectHistoricalCoordinatorAttempt(r.ctx, ref.Previous)
	if err != nil {
		return ref, err
	}
	next, err := reader.InspectHistoricalCoordinatorAttempt(r.ctx, ref.Successor)
	if err != nil {
		return ref, err
	}
	if old.OwnerID != owner.id || old.EpicID != owner.entry.Epic || next.OwnerID != old.OwnerID || next.EpicID != old.EpicID || next.Name != old.Name || logged.session == nil || logged.session.Task != owner.id || logged.session.Name != old.Name || ev.Key != recoveryEventKey(logged.id, ref) {
		return ref, fmt.Errorf("process recovery event owner changed")
	}
	return ref, nil
}

func (r *run) recordProcessRecoveryLocked(sess *entity, ref codex.ProcessRecoveryRef) error {
	return r.recordProcessRecoveryUsing(sess, ref, codex.Contract2AcceptanceAuthority)
}
func (r *run) recordProcessRecoveryUsing(sess *entity, ref codex.ProcessRecoveryRef, verify nativeAcceptance) error {
	if sess.session == nil {
		return fmt.Errorf("process recovery session missing")
	}
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return fmt.Errorf("process recovery owner missing")
	}
	if _, _, err := nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, nil, verify); err != nil {
		return err
	}
	reader, ok := r.runtimeFor("codex").(processRecoveryReader)
	if !ok {
		return ErrCodexIntegration
	}
	actual, err := reader.ValidateProcessRecovery(r.ctx, ref.Previous, ref.Successor, ref.Key)
	if err != nil {
		return err
	}
	if actual != ref {
		return fmt.Errorf("process recovery proof changed")
	}
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	ev := events.Event{Kind: events.KindRecovery, Sender: events.SenderProgram, Text: string(data), Key: recoveryEventKey(sess.id, ref), At: r.now}
	if _, err := r.validateRecoveryEvent(owner, sess, ev); err != nil {
		return err
	}
	logged, err := r.append(sess, ev)
	if err != nil {
		return err
	}
	if logged.Kind != ev.Kind || logged.Sender != ev.Sender || logged.Text != ev.Text || logged.Key != ev.Key {
		return fmt.Errorf("process recovery key conflicts")
	}
	return syncOwnership(sess)
}

func (r *run) processRecoveryBudget(owner *entity) (processRecoveryBudget, error) {
	cutoff := r.now.Add(-time.Hour)
	restartSince := cutoff
	if left := lastLeft(owner, stateBlocked, stateNeedsYou); left > 0 {
		if ev, ok := eventBySeq(owner, fmt.Sprint(left)); ok {
			restartSince = later(restartSince, ev.At)
		}
	}
	var times []time.Time
	seen := map[string]bool{}
	logs := slices.Clone(r.logsOf(owner))
	for _, id := range r.order() {
		e := r.ents[id]
		if e.session != nil && e.session.Task == owner.id {
			logs = append(logs, e)
		}
	}
	for _, log := range logs {
		for _, ev := range log.evs {
			if !ev.At.After(cutoff) {
				continue
			}
			switch ev.Kind {
			case events.KindRestart:
				if ev.At.After(restartSince) {
					times = append(times, ev.At)
				}
			case events.KindRecovery:
				ref, err := r.validateRecoveryEvent(owner, log, ev)
				if err != nil {
					return processRecoveryBudget{}, err
				}
				key := ref.Previous.SessionID + ":" + ref.Previous.AttemptID + ":" + ref.Successor.AttemptID
				if !seen[key] {
					times = append(times, ev.At)
					seen[key] = true
				}
			}
		}
	}
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
	limit := r.restartLimit(sessionComponent(owner))
	if limit <= 0 {
		return processRecoveryBudget{Count: len(times)}, nil
	}
	out := processRecoveryBudget{Count: len(times), Allowed: len(times) < limit}
	if !out.Allowed {
		out.NextCheckAt = times[len(times)-limit].Add(time.Hour)
	}
	return out, nil
}
