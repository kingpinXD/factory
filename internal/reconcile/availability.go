package reconcile

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type availabilityDomain struct {
	Scope         store.AvailabilityScope
	From, To      time.Time
	OwnerRevision string
}
type availabilityState struct {
	domains  map[string][]availabilityDomain
	managed  map[string]bool
	unknown  map[string]bool
	holds    []store.AvailabilityHold
	coverage []store.AvailabilityCoverage
	observed bool
}
type availabilityCause struct {
	source, id, reason string
	next               time.Time
}

// Timelines bind observed clock domains to retained activation events. They do
// not grant admission or infer a missing authority transfer.
func (r *run) prepareAvailability() {
	if r.availability != nil {
		return
	}
	a := &availabilityState{domains: map[string][]availabilityDomain{}, managed: map[string]bool{}, unknown: map[string]bool{}, holds: slices.Clone(r.sup.prev.AvailabilityHolds), coverage: slices.Clone(r.sup.prev.AvailabilityCoverage)}
	r.availability = a
	for _, id := range r.order() {
		e := r.ents[id]
		if e.session != nil || e.entry.Epic == "" || r.providerForEpic(e.entry.Epic) != "codex" {
			continue
		}
		domains, managed, err := r.availabilityDomains(e)
		a.domains[id], a.managed[id] = domains, managed
		if err != nil {
			a.unknown[id] = true
		}
	}
	for _, id := range r.order() {
		if e := r.ents[id]; e.session != nil {
			owner := e.session.Task
			a.domains[id] = a.domains[owner]
			a.managed[id] = a.managed[owner]
			a.unknown[id] = a.unknown[owner]
		}
	}
}

func (r *run) availabilityDomains(e *entity) ([]availabilityDomain, bool, error) {
	_, actual, err := nativeOwner(r.d.Brain, e.entry.Epic, e.id)
	if err != nil {
		return nil, true, err
	}
	_, _, found, err := nativeReadActivation(actual)
	if err != nil {
		return nil, true, err
	}
	if !found {
		if e.work != nil {
			if _, owner, err := nativeOwner(r.d.Brain, e.entry.Epic, e.work.Set); err == nil {
				_, _, modern, err := nativeReadActivation(owner)
				return nil, modern, err
			} else {
				return nil, true, err
			}
		}
		return nil, false, nil
	}
	selected, err := NativeSelection(r.d.Brain, e.entry.Epic)
	if err != nil {
		return nil, true, err
	}
	var out []availabilityDomain
	for i, ev := range actual.evs {
		if ev.Recipient != codexAuthorityRecipient {
			continue
		}
		prefix := *actual
		prefix.evs = actual.evs[:i+1]
		rec, _, _, err := nativeReadActivation(&prefix)
		if err != nil {
			return nil, true, err
		}
		if ev.At.IsZero() || ev.At.After(r.now) || rec.SelectionHash != selectionHash(selected) || nativeValidateBasis(r.ix, rec.Basis) != nil {
			return nil, true, fmt.Errorf("availability activation evidence changed")
		}
		scope := store.AvailabilityScope{Executor: "codex", OwnerID: e.id, Generation: nativeDigest(rec)}
		ownerRevision := scope.Generation
		if e.work != nil {
			if rec.WorkGeneration == "" {
				return nil, true, fmt.Errorf("availability work generation missing")
			}
			scope.WorkID = e.id
			scope.Generation = rec.WorkGeneration
			if ref := rec.OwnerActivation; ref != nil {
				_, owner, err := nativeOwner(r.d.Brain, e.entry.Epic, ref.OwnerID)
				if err != nil {
					return nil, true, err
				}
				if ref.Sequence < 1 || ref.Sequence > len(owner.evs) || nativeEventRef(owner.id, owner.evs[ref.Sequence-1]) != *ref {
					return nil, true, fmt.Errorf("availability owner activation changed")
				}
				owner.evs = owner.evs[:ref.Sequence]
				bound, selected, ok, err := nativeReadActivation(owner)
				if err != nil || !ok || nativeEventRef(owner.id, selected) != *ref || owner.set == nil || nativeValidateBasis(r.ix, bound.Basis) != nil {
					return nil, true, fmt.Errorf("availability work membership unproved")
				}
				var workInput WorkInputs
				var setInput SetInputs
				for _, basis := range rec.Basis {
					if basis.OwnerID == e.id {
						if err := yaml.Unmarshal([]byte(basis.Inputs), &workInput); err != nil {
							return nil, true, err
						}
					}
					if basis.OwnerID == owner.id {
						if err := yaml.Unmarshal([]byte(basis.Inputs), &setInput); err != nil {
							return nil, true, err
						}
					}
				}
				if workInput.ID != e.id || workInput.Set != owner.id || workInput.Epic != e.entry.Epic || setInput.ID != owner.id || setInput.Epic != e.entry.Epic || !slices.Contains(setInput.Items, e.id) {
					return nil, true, fmt.Errorf("availability retained membership unproved")
				}
				scope.OwnerID = owner.id
				ownerRevision = nativeDigest(bound)
			} else if !handedOver(&prefix) {
				return nil, true, fmt.Errorf("availability self ownership unproved")
			}
		}
		if len(out) > 0 {
			if ev.At.Before(out[len(out)-1].From) {
				return nil, true, fmt.Errorf("availability activation time reversed")
			}
			out[len(out)-1].To = ev.At
		}
		out = append(out, availabilityDomain{Scope: scope, From: ev.At, OwnerRevision: ownerRevision})
	}
	// A changed current basis has no newly proved scope until the ordinary
	// transfer producer records its activation. Never credit that unknown span.
	basis, err := nativeBasis(r.ix, actual)
	rec, _, _, readErr := nativeReadActivation(actual)
	if err != nil || readErr != nil || !reflect.DeepEqual(basis, rec.Basis) {
		return out, true, fmt.Errorf("availability current authority unproved")
	}
	return out, true, nil
}

func (r *run) observeAvailabilityHolds() {
	r.prepareAvailability()
	a := r.availability
	if a.observed {
		return
	}
	a.observed = true
	managed := false
	for _, value := range a.managed {
		managed = managed || value
	}
	if !managed {
		return
	}
	for _, id := range r.order() {
		e := r.ents[id]
		if !a.managed[id] || e.session != nil {
			continue
		}
		ds := a.domains[id]
		if len(ds) == 0 {
			continue
		}
		d := ds[len(ds)-1]
		causes := r.availabilityCauses(e, d)
		for _, h := range a.holds {
			if h.Scope == d.Scope && (h.Source == "" || h.SourceID == "" || h.Reason == "" || h.From.IsZero() || h.From.After(r.now) || !h.To.IsZero() && (h.To.Before(h.From) || h.To.After(r.now))) {
				a.unknown[id] = true
			}
		}
		for _, c := range a.coverage {
			if c.Scope == d.Scope && (c.From.IsZero() || c.From.After(r.now)) {
				a.unknown[id] = true
			}
		}

		for i := range a.holds {
			h := &a.holds[i]
			if h.Scope != d.Scope || !h.To.IsZero() || h.From.After(r.now) {
				continue
			}
			switch h.Source {
			case "authority", "quota", "capability", "runtime", "recovery_limit", "verification-checks":
			default:
				continue
			}
			same := false
			for _, c := range causes {
				if h.Source == c.source && h.SourceID == c.id && h.Reason == c.reason {
					h.NextCheckAt = c.next
					same = true
					break
				}
			}
			if !same {
				h.To = r.now
			}
		}
		for _, c := range causes {
			if !slices.ContainsFunc(a.holds, func(h store.AvailabilityHold) bool {
				return h.Scope == d.Scope && h.Source == c.source && h.SourceID == c.id && h.To.IsZero()
			}) {
				a.holds = append(a.holds, store.AvailabilityHold{Scope: d.Scope, Source: c.source, SourceID: c.id, From: r.now, Reason: c.reason, NextCheckAt: c.next})
			}
		}
		if !a.unknown[id] && !slices.ContainsFunc(a.coverage, func(c store.AvailabilityCoverage) bool { return c.Scope == d.Scope }) {
			a.coverage = append(a.coverage, store.AvailabilityCoverage{Scope: d.Scope, From: r.now})
		}
	}
	if !r.dry {
		overall := r.sup.prev
		overall.AvailabilityHolds = a.holds
		overall.AvailabilityCoverage = a.coverage
		if err := store.WriteOverall(r.d.Brain, overall); err != nil {
			r.fail("availability observation: %v", err)
			for id := range a.managed {
				a.unknown[id] = true
			}
		}
	}
}

func (r *run) availabilityCauses(e *entity, d availabilityDomain) []availabilityCause {
	var out []availabilityCause
	add := func(source, id, reason string, next time.Time) {
		out = append(out, availabilityCause{source, id, reason, next})
	}
	if r.availability.unknown[e.id] {
		add("authority", d.OwnerRevision, "authority scope unavailable", time.Time{})
	}
	if hold, _ := r.verificationCheckHold(e); hold != nil && hold.Scope == d.Scope {
		add(hold.Source, hold.SourceID, hold.Reason, hold.NextCheckAt)
	}
	q, ok := r.providerQuotas[e.entry.Epic]
	if !ok {
		add("quota", d.OwnerRevision, "quota observation unavailable", time.Time{})
	} else {
		if q.Admission.Decision.State != "allowed" {
			add("quota", q.SelectionSHA256+":"+nativeDigest(q.Policy), q.Admission.Decision.Reason, q.Admission.NextCheckAt)
		}
		if q.Capability.State != "allowed" {
			add("capability", q.SelectionSHA256, q.Capability.Reason, time.Time{})
		}
	}
	owner := r.ents[d.Scope.OwnerID]
	if owner == nil {
		add("runtime", d.OwnerRevision, "owner unavailable", time.Time{})
		return out
	}
	sess := r.ents[sessionID(owner.id, sessionComponent(owner))]
	if owner.work != nil {
		sess = r.babysitterOf(owner)
	}
	if sess == nil {
		add("runtime", d.OwnerRevision, "coordinator unavailable", time.Time{})
		return out
	}
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(sess.session.Name)))
	view, err := codex.New(r.d.Brain, "").InspectCoordinatorAttempt(r.ctx, id)
	if err != nil {
		add("runtime", id, "attempt observation unavailable", time.Time{})
		return out
	}
	if view.OwnerID != owner.id || view.EpicID != e.entry.Epic || view.Name != sess.session.Name {
		add("runtime", view.Ref.AttemptID, "attempt ownership changed", view.NextCheckAt)
		return out
	}
	if view.Stage == "closed" {
		budget, err := r.processRecoveryBudget(owner)
		if err != nil {
			add("recovery_limit", view.Ref.AttemptID, "recovery accounting unavailable", time.Time{})
		} else if !budget.Allowed {
			add("recovery_limit", view.Ref.AttemptID, "component recovery limit", budget.NextCheckAt)
		}
	}
	inv, known := r.inventories["codex"]
	if !known {
		add("runtime", view.Ref.AttemptID, "inventory unavailable", view.NextCheckAt)
		return out
	}
	lookup := inv.Lookup(sess.session.Name)
	switch lookup.Presence {
	case session.Unknown:
		add("runtime", view.Ref.AttemptID, "inventory unknown", view.NextCheckAt)
	case session.Quarantined:
		next := view.NextCheckAt
		if lookup.Reservation.AttemptID == view.Ref.AttemptID && lookup.Reservation.Generation == view.Generation {
			next = lookup.Reservation.NextCheckAt
		}
		add("runtime", view.Ref.AttemptID, lookup.Reservation.Reason, next)
	case session.Present:
		row := lookup.Session
		if row.AttemptID != view.Ref.AttemptID || row.ID != view.Ref.SessionID || row.Generation != view.Generation || view.Stage != "claimed" {
			add("runtime", view.Ref.AttemptID, "selected attempt unavailable", view.NextCheckAt)
		}
	case session.Absent:
		if view.Stage != "claimed" {
			add("runtime", view.Ref.AttemptID, "coordinator recovery pending", view.NextCheckAt)
		}
	}
	return out
}

func (r *run) managedClock(e *entity) bool {
	r.prepareAvailability()
	return r.availability.managed[e.id]
}

func (r *run) clockHeld(e *entity) bool {
	if !r.managedClock(e) {
		if e.entry.Kind == blueprint.MachineAccount {
			return r.sup.usage.idle
		}
		return !r.workAllowed(e)
	}
	a := r.availability
	if !a.observed || a.unknown[e.id] || len(a.domains[e.id]) == 0 {
		return true
	}
	scope := a.domains[e.id][len(a.domains[e.id])-1].Scope
	return slices.ContainsFunc(a.holds, func(h store.AvailabilityHold) bool { return h.Scope == scope && h.To.IsZero() })
}

func (r *run) eligibleElapsed(e *entity, since time.Time) store.ClockReading {
	r.prepareAvailability()
	a := r.availability
	if !a.managed[e.id] {
		return store.ClockReading{Eligible: r.legacyElapsed(e, since), Complete: true}
	}
	if since.IsZero() || since.After(r.now) || a.unknown[e.id] || !a.observed {
		return store.ClockReading{}
	}
	return eligibleDomainElapsed(a.domains[e.id], a.coverage, a.holds, since, r.now)
}

func eligibleDomainElapsed(ds []availabilityDomain, coverageRows []store.AvailabilityCoverage, holdRows []store.AvailabilityHold, since, now time.Time) store.ClockReading {
	end := now
	total := time.Duration(0)
	for i := len(ds) - 1; i >= 0; i-- {
		d := ds[i]
		to := d.To
		if to.IsZero() || to.After(now) {
			to = now
		}
		if !to.Equal(end) {
			break
		}
		from := later(since, d.From)
		coverage := time.Time{}
		for _, c := range coverageRows {
			if c.Scope == d.Scope && (coverage.IsZero() || c.From.Before(coverage)) {
				coverage = c.From
			}
		}
		if coverage.IsZero() {
			break
		}
		holds := []store.AvailabilityHold{}
		for _, h := range holdRows {
			if h.Scope == d.Scope {
				holds = append(holds, h)
			}
		}
		reading := store.EligibleElapsed(from, to, nil, holds, coverage)
		total += reading.Eligible
		if !reading.Complete {
			return store.ClockReading{Eligible: total}
		}
		if !from.After(since) {
			return store.ClockReading{Eligible: total, Complete: true}
		}
		end = from
	}
	return store.ClockReading{Eligible: total}
}

func (r *run) invalidateAvailability() {
	if r.availability != nil {
		r.sup.prev.AvailabilityHolds = r.availability.holds
		r.sup.prev.AvailabilityCoverage = r.availability.coverage
	}
	r.availability = nil
}
