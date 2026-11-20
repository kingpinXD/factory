package reconcile

import (
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

const claudeListingUnavailable = "session listing is unknown"

// A coordinator hold applies to its work, not every entity in the epic.
func (r *run) claudeClockOwner(e *entity) string {
	if e == nil || r.providerForEpic(e.entry.Epic) != "claude" {
		return ""
	}
	if e.session != nil {
		owner := r.ents[e.session.Task]
		if owner != nil && (owner.work == nil || r.claudeClockOwner(owner) == owner.id) {
			return owner.id
		}
		return ""
	}
	if e.work != nil {
		if (active(e) || e.cur.State == workPROpen) && !handedOver(e) && !skipsOrchestrator(e) {
			if _, ready := worktreeReady(e); ready {
				return e.work.Set
			}
			return ""
		}
		if !watched(e) {
			return ""
		}
	}
	return e.id
}

func (r *run) coordinatorHolds() []store.CoordinatorHold {
	if r.sup.coordinatorHolds == nil {
		r.sup.coordinatorHolds = slices.Clone(r.sup.prev.CoordinatorHolds)
	}
	return r.sup.coordinatorHolds
}

func (r *run) claudeCoordinatorHeld(e *entity) bool {
	holds := r.coordinatorHolds()
	if len(holds) == 0 {
		return false
	}
	owner := r.claudeClockOwner(e)
	return owner != "" && slices.ContainsFunc(holds, func(h store.CoordinatorHold) bool { return h.OwnerID == owner && h.To.IsZero() })
}

func (r *run) observeClaudeCoordinator(epic, name string, lookup session.Lookup) {
	if r.providerForEpic(epic) != "claude" {
		return
	}
	for _, owner := range r.ents {
		if owner.session != nil || !owner.open() || owner.entry.Epic != epic || sessionComponent(owner) == "" {
			continue
		}
		_, currentName := r.sessionSlot(owner, sessionComponent(owner))
		if currentName != name {
			continue
		}
		switch lookup.Presence {
		case session.Unknown:
			r.holdClaudeCoordinator(owner, claudeListingUnavailable)
		case session.Present:
			if lookup.Session.Live() {
				r.holdClaudeCoordinator(owner, "")
				return
			}
			fallthrough
		case session.Absent:
			for _, h := range r.coordinatorHolds() {
				if h.OwnerID == owner.id && h.To.IsZero() && h.Reason == claudeListingUnavailable {
					r.holdClaudeCoordinator(owner, "")
					break
				}
			}
		}
		return
	}
}

func (r *run) holdClaudeCoordinator(owner *entity, reason string) {
	if r.providerForEpic(owner.entry.Epic) != "claude" {
		return
	}
	holds := r.coordinatorHolds()
	open := -1
	for i, h := range holds {
		if h.OwnerID == owner.id && h.To.IsZero() {
			open = i
			break
		}
	}
	if reason == "" && open < 0 || reason != "" && open >= 0 {
		return
	}
	if reason != "" {
		holds = append(holds, store.CoordinatorHold{OwnerID: owner.id, From: r.now, Reason: reason})
	} else {
		holds[open].To = r.now
	}
	r.sup.coordinatorHolds = holds
	// The listing can be read after entity clocks run. Save its first unknown observation now.
	for _, e := range r.ents {
		if r.claudeClockOwner(e) == owner.id {
			e.cur = fsm.HoldClock(e.cur, r.now, r.held(e))
		}
	}
}

func (r *run) claudeHeldIntervals(e *entity) []store.Hold {
	holds := r.coordinatorHolds()
	if len(holds) == 0 {
		return nil
	}
	owner := r.claudeClockOwner(e)
	var out []store.Hold
	if owner != "" {
		for _, h := range holds {
			if h.OwnerID == owner {
				out = append(out, store.Hold{From: h.From, To: h.To})
			}
		}
	}
	return out
}

func (r *run) claudeListingHold(e *entity) (store.CoordinatorHold, bool) {
	holds := r.coordinatorHolds()
	if len(holds) == 0 {
		return store.CoordinatorHold{}, false
	}
	owner := r.claudeClockOwner(e)
	for _, h := range holds {
		if h.OwnerID == owner && owner != "" && h.To.IsZero() && h.Reason == claudeListingUnavailable {
			return h, true
		}
	}
	return store.CoordinatorHold{}, false
}

// Used by quiet-turn checks, whose legacy clock deliberately ignores account holds.
func (r *run) claudeQuietElapsed(e *entity, since time.Time) time.Duration {
	return store.EligibleElapsed(since, r.now, r.claudeHeldIntervals(e), nil, since).Eligible
}
