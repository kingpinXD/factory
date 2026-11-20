package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type coordinatorSupervisionPolicy struct {
	Unacked, NoProgress, InboxCheck time.Duration
}

func (r *run) coordinatorSupervisionPolicy() coordinatorSupervisionPolicy {
	return coordinatorSupervisionPolicy{time.Duration(r.b.Values.Inbox.Unacked), time.Duration(r.b.Values.Supervision.NoProgress), time.Duration(r.b.Values.Inbox.CheckEvery)}
}

type supervisorStallInputs struct {
	Policy                                        coordinatorSupervisionPolicy
	SubjectID, SubjectState                       string
	Working, TurnEnded, HasSubject, InboxFree     bool
	InboxSequence                                 int
	InboxSince, ProgressSince, IdleSince          time.Time
	InboxEligible, ProgressEligible, IdleEligible time.Duration
}

func (r *run) supervisorStallInputs(owner, subject, sess *entity, s session.Session, turnEnded bool) supervisorStallInputs {
	in := supervisorStallInputs{Policy: r.coordinatorSupervisionPolicy(), Working: !turnEnded && s.State == listedWorking, TurnEnded: turnEnded, HasSubject: subject != nil}
	if subject != nil {
		in.SubjectID, in.SubjectState = subject.id, subject.cur.State
	}
	if in.Working {
		if inbox := events.Inbox(owner.evs); len(inbox) > 0 {
			in.InboxSequence, in.InboxSince = inbox[0].Seq, inbox[0].At
			in.InboxFree = !r.helperBusy(owner)
		}
		if subject != nil {
			in.ProgressSince = r.progress(owner, subject, sess)
		}
	} else if subject != nil && turnEnded {
		in.IdleSince = later(r.progress(owner, subject, sess), sess.cur.Since)
	}
	return in
}

func supervisorStall(in supervisorStallInputs) (stall, string, bool) {
	if in.Working {
		if in.InboxSequence > 0 && in.InboxFree && in.InboxEligible >= in.Policy.Unacked {
			return stall{fmt.Sprintf("message seq %d unacknowledged for %s", in.InboxSequence, in.InboxEligible.Round(time.Minute)), in.InboxSince, in.Policy.Unacked}, "unacked", true
		}
		if in.HasSubject && in.ProgressEligible >= in.Policy.NoProgress {
			return stall{fmt.Sprintf("no progress on %s (%s) for %s", in.SubjectID, in.SubjectState, in.ProgressEligible.Round(time.Minute)), in.ProgressSince, in.Policy.NoProgress}, "no_progress", true
		}
		return stall{}, "", false
	}
	if in.HasSubject && in.TurnEnded && in.IdleEligible >= in.Policy.InboxCheck {
		return stall{fmt.Sprintf("its turn ended with %s %s", in.SubjectID, in.SubjectState), in.IdleSince, in.Policy.NoProgress}, "idle", true
	}
	return stall{}, "", false
}

func (r *run) currentSupervisorReadings(owner, subject *entity, in supervisorStallInputs) supervisorStallInputs {
	if !in.InboxSince.IsZero() && in.InboxFree {
		in.InboxEligible = r.elapsed(owner, in.InboxSince)
	}
	if subject != nil && !in.ProgressSince.IsZero() {
		in.ProgressEligible = r.elapsed(subject, in.ProgressSince)
	}
	if subject != nil && !in.IdleSince.IsZero() {
		in.IdleEligible = r.elapsed(subject, in.IdleSince)
	}
	return in
}

type coordinatorClockSnapshot struct {
	ObservedAt time.Time
	Domains    map[string][]availabilityDomain
	Coverage   []store.AvailabilityCoverage
	Holds      []store.AvailabilityHold
}

func (r *run) captureCoordinatorClock(owner, subject *entity) (coordinatorClockSnapshot, error) {
	a := r.availability
	if a == nil || !a.observed {
		return coordinatorClockSnapshot{}, effects.ErrEvidence
	}
	out := coordinatorClockSnapshot{ObservedAt: r.now, Domains: map[string][]availabilityDomain{}}
	scopes := map[store.AvailabilityScope]bool{}
	for _, e := range []*entity{owner, subject} {
		if e == nil {
			continue
		}
		if !a.managed[e.id] || a.unknown[e.id] || r.clockHeld(e) || len(a.domains[e.id]) == 0 {
			return out, effects.ErrAuthority
		}
		out.Domains[e.id] = slices.Clone(a.domains[e.id])
		for _, d := range a.domains[e.id] {
			scopes[d.Scope] = true
		}
	}
	for _, c := range a.coverage {
		if scopes[c.Scope] {
			out.Coverage = append(out.Coverage, c)
		}
	}
	for _, h := range a.holds {
		if scopes[h.Scope] {
			out.Holds = append(out.Holds, h)
		}
	}
	return out, nil
}

func (c coordinatorClockSnapshot) elapsed(entityID string, since time.Time) time.Duration {
	return eligibleDomainElapsed(c.Domains[entityID], c.Coverage, c.Holds, since, c.ObservedAt).Eligible
}

// Only the old covered interval is compared. No unobserved suffix is credited.
func coordinatorClockRows(c coordinatorClockSnapshot, coverage []store.AvailabilityCoverage, holds []store.AvailabilityHold, anchors map[string][]time.Time, now time.Time) ([]string, error) {
	var rows []string
	add := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		rows = append(rows, string(b))
		return nil
	}
	for entityID, times := range anchors {
		ds, ok := c.Domains[entityID]
		if !ok || len(ds) == 0 {
			return nil, effects.ErrEvidence
		}
		for _, since := range times {
			if since.IsZero() || since.After(c.ObservedAt) {
				return nil, effects.ErrEvidence
			}
			for _, d := range ds {
				to := d.To
				if to.IsZero() || to.After(c.ObservedAt) {
					to = c.ObservedAt
				}
				from := later(since, d.From)
				if !from.Before(to) {
					continue
				}
				found := false
				for _, v := range coverage {
					if v.Scope != d.Scope {
						continue
					}
					if v.From.IsZero() || v.From.After(now) {
						return nil, effects.ErrEvidence
					}
					if v.From.After(c.ObservedAt) {
						continue
					}
					found = true
					if err := add(struct {
						Entity   string
						Scope    store.AvailabilityScope
						From, To time.Time
					}{entityID, d.Scope, later(from, v.From), to}); err != nil {
						return nil, err
					}
				}
				if !found {
					return nil, effects.ErrEvidence
				}
				for _, h := range holds {
					if h.Scope != d.Scope {
						continue
					}
					if h.Source == "" || h.SourceID == "" || h.Reason == "" || h.From.IsZero() || h.From.After(now) || !h.To.IsZero() && h.To.Before(h.From) {
						return nil, effects.ErrEvidence
					}
					end := h.To
					if end.IsZero() || end.After(to) {
						end = to
					}
					start := later(from, h.From)
					if !start.Before(end) {
						continue
					}
					if err := add(struct {
						Entity             string
						Scope              store.AvailabilityScope
						Source, ID, Reason string
						From, To           time.Time
					}{entityID, d.Scope, h.Source, h.SourceID, h.Reason, start, end}); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	slices.Sort(rows)
	return rows, nil
}

func (r *run) validateCoordinatorClock(c coordinatorClockSnapshot, anchors map[string][]time.Time, now time.Time) error {
	if c.ObservedAt.IsZero() || c.ObservedAt.After(now) || len(c.Domains) == 0 {
		return effects.ErrEvidence
	}
	r.prepareAvailability()
	a := r.availability
	scopes := map[store.AvailabilityScope]bool{}
	for id, ds := range c.Domains {
		if !a.managed[id] || a.unknown[id] || !reflect.DeepEqual(a.domains[id], ds) {
			return effects.ErrStale
		}
		for _, d := range ds {
			scopes[d.Scope] = true
		}
	}
	for _, h := range r.sup.prev.AvailabilityHolds {
		if scopes[h.Scope] && h.To.IsZero() {
			return effects.ErrAuthority
		}
	}
	old, err := coordinatorClockRows(c, c.Coverage, c.Holds, anchors, c.ObservedAt)
	if err != nil {
		return err
	}
	current, err := coordinatorClockRows(c, r.sup.prev.AvailabilityCoverage, r.sup.prev.AvailabilityHolds, anchors, now)
	if err != nil {
		return err
	}
	if !slices.Equal(old, current) {
		return effects.ErrStale
	}
	return nil
}

func coordinatorProgressInputs(owner, subject *entity) []effects.EventRef {
	var out []effects.EventRef
	seen := map[string]bool{}
	for _, e := range []*entity{owner, subject} {
		if e == nil || seen[e.id] {
			continue
		}
		seen[e.id] = true
		for _, ev := range e.evs {
			if ev.Sender == owner.id && (ev.Kind != events.KindHeartbeat || ev.Lease == owner.st.Lease) {
				out = append(out, nativeEventRef(e.id, ev))
			}
		}
	}
	return out
}
func coordinatorInboxInputs(owner *entity) []effects.EventRef {
	var out []effects.EventRef
	for _, ev := range events.Inbox(owner.evs) {
		out = append(out, nativeEventRef(owner.id, ev))
	}
	return out
}
func coordinatorViewSession(v codex.CoordinatorAttemptView) session.Session {
	state, status := listedWorking, "busy"
	if v.State == "idle" && !v.PendingCommands {
		state, status = listedTurnEnded, listedIdle
	}
	return session.Session{Ref: session.Ref{Executor: "codex", ID: v.Ref.SessionID, AttemptID: v.Ref.AttemptID, Name: v.Name, EpicID: v.EpicID, Generation: v.Generation}, OwnerID: v.OwnerID, State: state, Status: status}
}

func coordinatorClockAnchors(owner, subject *entity, in supervisorStallInputs, nudge time.Time) map[string][]time.Time {
	out := map[string][]time.Time{}
	if !in.InboxSince.IsZero() && in.InboxFree {
		out[owner.id] = append(out[owner.id], in.InboxSince)
	}
	if subject != nil {
		if !in.ProgressSince.IsZero() {
			out[subject.id] = append(out[subject.id], in.ProgressSince)
		}
		if !in.IdleSince.IsZero() {
			out[subject.id] = append(out[subject.id], in.IdleSince)
		}
		out[subject.id] = append(out[subject.id], nudge)
	}
	return out
}

func coordinatorSnapshotReadings(c coordinatorClockSnapshot, owner, subject *entity, in supervisorStallInputs) supervisorStallInputs {
	if !in.InboxSince.IsZero() && in.InboxFree {
		in.InboxEligible = c.elapsed(owner.id, in.InboxSince)
	}
	if subject != nil && !in.ProgressSince.IsZero() {
		in.ProgressEligible = c.elapsed(subject.id, in.ProgressSince)
	}
	if subject != nil && !in.IdleSince.IsZero() {
		in.IdleEligible = c.elapsed(subject.id, in.IdleSince)
	}
	return in
}

func canonicalCoordinatorSource(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	if len(raw) > 8<<20 {
		return "", effects.ErrEvidence
	}
	return string(raw), nil
}

func supervisorSourceKey(ref codex.AttemptRef, nudge effects.EventRef) string {
	return fmt.Sprintf("supervisor:%s:%s:%s:%d", ref.SessionID, ref.AttemptID, nudge.OwnerID, nudge.Sequence)
}

// These are exact bounded log references, not a comparison of event timestamps.
func sameCoordinatorReferences(a, b []effects.EventRef) bool { return slices.Equal(a, b) }

func validCoordinatorSupervisionPolicy(p coordinatorSupervisionPolicy) bool {
	return p.Unacked > 0 && p.NoProgress > 0 && p.InboxCheck > 0
}

type coordinatorSupervisorCause struct {
	Version                           int
	Attempt                           codex.AttemptRef
	Authority                         OwnerAuthority
	Work                              *helper.Scope
	SubjectID, ActiveSubjectID        string
	SubjectEntry, SessionEntry, Nudge effects.EventRef
	State                             string
	PendingCommands                   bool
	Policy                            coordinatorSupervisionPolicy
	Code                              string
	Since                             time.Time
	Window                            time.Duration
	Inbox, Progress                   []effects.EventRef
	Clock                             coordinatorClockSnapshot
}

func coordinatorSupervisorBinding(v coordinatorSupervisorCause) effects.Digest {
	v.Clock = coordinatorClockSnapshot{}
	return effectDigest(v)
}
func coordinatorSupervisorKey(v coordinatorSupervisorCause) string {
	return supervisorSourceKey(v.Attempt, v.Nudge) + ":" + coordinatorSupervisorBinding(v).String()
}

func (r *run) captureSupervisorCloseLocked(subject, sess *entity) (CoordinatorCloseRequestRef, error) {
	runtime, verify, err := r.modernCoordinatorRuntime()
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	owner := r.ents[sess.session.Task]
	if owner == nil || subject == nil {
		return CoordinatorCloseRequestRef{}, effects.ErrAuthority
	}
	if owner.work != nil {
		return CoordinatorCloseRequestRef{}, fmt.Errorf("%w: review-idle source integration pending", ErrCodexIntegration)
	}
	activeSubject := r.subject(owner)
	target := activeSubject
	if target == nil {
		target = owner
	}
	if subject != target {
		return CoordinatorCloseRequestRef{}, effects.ErrAuthority
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	if view.Stage != "claimed" || view.OwnerID != owner.id || view.Name != sess.session.Name || view.EpicID != owner.entry.Epic {
		return CoordinatorCloseRequestRef{}, codex.ErrAttemptStale
	}
	if !validCoordinatorSupervisionPolicy(r.coordinatorSupervisionPolicy()) || r.nativeBusy(owner.id) {
		return CoordinatorCloseRequestRef{}, effects.ErrAuthority
	}
	observed := coordinatorViewSession(view)
	ended := view.State == "idle" && !view.PendingCommands
	in := r.currentSupervisorReadings(owner, activeSubject, r.supervisorStallInputs(owner, activeSubject, sess, observed, ended))
	st, code, ok := supervisorStall(in)
	if !ok {
		return CoordinatorCloseRequestRef{}, effects.ErrStale
	}
	nudge, ok := lastAction(subject, st.since)
	if !ok || nudge.Kind != events.KindNudge || nudge.Sender != events.SenderProgram || r.elapsed(subject, nudge.At) < st.window {
		return CoordinatorCloseRequestRef{}, effects.ErrStale
	}
	clock, err := r.captureCoordinatorClock(owner, subject)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	authority, _, err := nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, nil, verify)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	source := coordinatorSupervisorCause{Version: 2, Attempt: view.Ref, Authority: authority, SubjectID: subject.id, SubjectEntry: nativeEventRef(subject.id, subject.enteredBy), SessionEntry: nativeEventRef(sess.id, sess.enteredBy), Nudge: nativeEventRef(subject.id, nudge), State: view.State, PendingCommands: view.PendingCommands, Policy: in.Policy, Code: code, Since: st.since, Window: st.window, Inbox: coordinatorInboxInputs(owner), Progress: coordinatorProgressInputs(owner, activeSubject), Clock: clock}
	if activeSubject != nil {
		source.ActiveSubjectID = activeSubject.id
	}
	key := coordinatorSupervisorKey(source)
	if prior, ok := sess.byKey(key); ok {
		var stored coordinatorSupervisorCause
		if toolcontract.DecodeBroker([]byte(prior.Text), &stored) != nil || coordinatorSupervisorBinding(stored) != coordinatorSupervisorBinding(source) {
			return CoordinatorCloseRequestRef{}, effects.ErrConflict
		}
		if err = r.validateSupervisorSource(runtime, verify, sess, stored, prior, false, r.d.Now()); err != nil {
			return CoordinatorCloseRequestRef{}, err
		}
		return r.requestCoordinatorCloseLocked(sess, "process_replace", "shutdown", nativeEventRef(sess.id, prior))
	}
	data, err := canonicalCoordinatorSource(source)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	event, err := appendCoordinatorEvent(r, sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "restart-observation", Text: data, Key: key})
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	return r.requestCoordinatorCloseLocked(sess, "process_replace", "shutdown", event)
}

func (r *run) validateSupervisorSource(runtime *codex.Runtime, verify nativeAcceptance, sess *entity, source coordinatorSupervisorCause, event events.Event, sealed bool, now time.Time) error {
	data, err := canonicalCoordinatorSource(source)
	if err != nil || data != event.Text || source.Version != 2 || event.Kind != events.KindIntent || event.Sender != events.SenderProgram || event.Recipient != "codex-attempt" || event.Request != "restart-observation" || event.Key != coordinatorSupervisorKey(source) || !event.At.Equal(source.Clock.ObservedAt) || !validCoordinatorSupervisionPolicy(source.Policy) {
		return effects.ErrEvidence
	}
	owner := r.ents[sess.session.Task]
	subject := r.ents[source.SubjectID]
	if owner == nil || subject == nil || source.Attempt.SessionID == "" || source.Authority.OwnerID != owner.id || source.Authority.EpicID != owner.entry.Epic || source.SessionEntry.OwnerID != sess.id || source.SubjectEntry.OwnerID != subject.id || source.Nudge.OwnerID != subject.id {
		return effects.ErrAuthority
	}
	// These exact historical references remain readable after a matching seal.
	for _, ref := range []effects.EventRef{source.SessionEntry, source.SubjectEntry, source.Nudge} {
		if _, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(ref); err != nil {
			return err
		}
	}
	for _, refs := range [][]effects.EventRef{source.Inbox, source.Progress} {
		for _, ref := range refs {
			if _, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(ref); err != nil {
				return err
			}
		}
	}
	nudge, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(source.Nudge)
	if err != nil || nudge.Kind != events.KindNudge || nudge.Sender != events.SenderProgram || nudge.At.After(source.Clock.ObservedAt) {
		return effects.ErrEvidence
	}
	if source.Clock.ObservedAt.IsZero() || source.Clock.ObservedAt.After(now) || source.Since.IsZero() || source.Window <= 0 {
		return effects.ErrEvidence
	}
	if sealed {
		return nil
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if err != nil {
		return err
	}
	if view.Ref != source.Attempt || view.Stage != "claimed" || view.State != source.State || view.PendingCommands != source.PendingCommands {
		return codex.ErrAttemptStale
	}
	if owner.work != nil {
		return fmt.Errorf("%w: review-idle source integration pending", ErrCodexIntegration)
	}
	active := r.subject(owner)
	target := active
	if target == nil {
		target = owner
	}
	activeID := ""
	if active != nil {
		activeID = active.id
	}
	if target != subject || activeID != source.ActiveSubjectID || nativeEventRef(subject.id, subject.enteredBy) != source.SubjectEntry || nativeEventRef(sess.id, sess.enteredBy) != source.SessionEntry || r.coordinatorSupervisionPolicy() != source.Policy || !sameCoordinatorReferences(coordinatorInboxInputs(owner), source.Inbox) || !sameCoordinatorReferences(coordinatorProgressInputs(owner, active), source.Progress) || r.nativeBusy(owner.id) {
		return effects.ErrStale
	}
	in := r.supervisorStallInputs(owner, active, sess, coordinatorViewSession(view), view.State == "idle" && !view.PendingCommands)
	anchors := coordinatorClockAnchors(owner, active, in, nudge.At)
	if active == nil {
		anchors[owner.id] = append(anchors[owner.id], nudge.At)
	}
	if err = r.validateCoordinatorClock(source.Clock, anchors, now); err != nil {
		return err
	}
	in = coordinatorSnapshotReadings(source.Clock, owner, active, in)
	st, code, ok := supervisorStall(in)
	if !ok || code != source.Code || !st.since.Equal(source.Since) || st.window != source.Window || source.Clock.elapsed(subject.id, nudge.At) < st.window {
		return effects.ErrStale
	}
	last, ok := lastAction(subject, st.since)
	if !ok || nativeEventRef(subject.id, last) != source.Nudge {
		return effects.ErrStale
	}
	_, _, err = nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, &source.Authority, verify)
	return err
}

func validateSupervisorCloseLocked(ctx context.Context, brain string, runtime *codex.Runtime, verify nativeAcceptance, sessionID string, request CoordinatorCloseRequest, event events.Event, sealed bool, now time.Time) error {
	var source coordinatorSupervisorCause
	if toolcontract.DecodeBroker([]byte(event.Text), &source) != nil || source.Attempt != request.Attempt || source.Authority != request.Authority || !reflect.DeepEqual(source.Work, request.Work) {
		return effects.ErrEvidence
	}
	r, err := readRun(ctx, Deps{Brain: brain, Now: func() time.Time { return now }, Runtimes: map[string]session.Runtime{"codex": runtime}})
	if err != nil {
		return err
	}
	sess := r.ents[sessionID]
	if sess == nil || sess.session == nil {
		return effects.ErrAuthority
	}
	return r.validateSupervisorSource(runtime, verify, sess, source, event, sealed, now)
}

type coordinatorWorkTimeoutCause struct {
	Version               int
	Attempt               codex.AttemptRef
	Authority             OwnerAuthority
	SubjectID             string
	Timeout, SessionEntry effects.EventRef
}

func workTimeoutSourceKey(source coordinatorWorkTimeoutCause) string {
	return "work-timeout:" + source.Attempt.AttemptID + ":" + source.SubjectID + ":" + fmt.Sprint(source.Timeout.Sequence)
}

func (r *run) captureWorkTimeoutCloseLocked(subject, sess *entity) (CoordinatorCloseRequestRef, error) {
	runtime, verify, err := r.modernCoordinatorRuntime()
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	if subject == nil || sess == nil || sess.session == nil {
		return CoordinatorCloseRequestRef{}, effects.ErrAuthority
	}
	owner := r.ents[sess.session.Task]
	if owner == nil || r.subject(owner) != subject {
		return CoordinatorCloseRequestRef{}, effects.ErrAuthority
	}
	view, err := r.coordinatorAttemptView(sess, runtime)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	authority, _, err := nativeAuthority(r.ctx, r.d.Brain, owner.entry.Epic, owner.id, true, nil, verify)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	source := coordinatorWorkTimeoutCause{Version: 2, Attempt: view.Ref, Authority: authority, SubjectID: subject.id, Timeout: nativeEventRef(subject.id, subject.enteredBy), SessionEntry: nativeEventRef(sess.id, sess.enteredBy)}
	data, err := canonicalCoordinatorSource(source)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	event := events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "work-timeout-observation", Text: data, Key: workTimeoutSourceKey(source)}
	if err := r.validateWorkTimeoutSource(runtime, sess, source, event, false); err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	ref, err := appendCoordinatorEvent(r, sess, event)
	if err != nil {
		return CoordinatorCloseRequestRef{}, err
	}
	return r.requestCoordinatorCloseLocked(sess, "process_replace", "shutdown", ref)
}

func (r *run) validateWorkTimeoutSource(runtime *codex.Runtime, sess *entity, source coordinatorWorkTimeoutCause, event events.Event, sealed bool) error {
	data, err := canonicalCoordinatorSource(source)
	if err != nil || data != event.Text || source.Version != 2 || event.Kind != events.KindIntent || event.Sender != events.SenderProgram || event.Recipient != "codex-attempt" || event.Request != "work-timeout-observation" || event.Key != workTimeoutSourceKey(source) {
		return effects.ErrEvidence
	}
	subject := r.ents[source.SubjectID]
	owner := r.ents[sess.session.Task]
	if subject == nil || owner == nil || source.Authority.OwnerID != owner.id || source.Authority.EpicID != owner.entry.Epic || source.Timeout.OwnerID != subject.id || source.SessionEntry.OwnerID != sess.id {
		return effects.ErrAuthority
	}
	timeout, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(source.Timeout)
	if err != nil {
		return err
	}
	if timeout.Kind != events.KindTransition || timeout.Sender != events.SenderProgram || timeout.Trigger != blueprint.TriggerTick || !strings.HasPrefix(timeout.TriggerRef, timeoutRef) || timeout.From == "" || timeout.From != timeout.To || timeout.Attempt <= 0 || timeout.At.After(event.At) {
		return effects.ErrEvidence
	}
	if _, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(source.SessionEntry); err != nil {
		return err
	}
	if sealed {
		return nil
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if err != nil {
		return err
	}
	if view.Ref != source.Attempt || (view.Stage != "claimed" && view.Stage != "prepared") {
		return codex.ErrAttemptStale
	}
	if r.subject(owner) != subject || nativeEventRef(subject.id, subject.enteredBy) != source.Timeout || nativeEventRef(sess.id, sess.enteredBy) != source.SessionEntry {
		return effects.ErrStale
	}
	state := subject.m.States[timeout.From]
	if state.Timeout <= 0 || state.OnTimeout != timeout.From {
		return effects.ErrEvidence
	}
	return nil
}

func validateWorkTimeoutCloseLocked(ctx context.Context, brain string, runtime *codex.Runtime, sessionID string, request CoordinatorCloseRequest, event events.Event, sealed bool, now time.Time) error {
	var source coordinatorWorkTimeoutCause
	if toolcontract.DecodeBroker([]byte(event.Text), &source) != nil || source.Attempt != request.Attempt || source.Authority != request.Authority || request.Work != nil || event.At.After(now) {
		return effects.ErrEvidence
	}
	r, err := readRun(ctx, Deps{Brain: brain, Now: func() time.Time { return now }, Runtimes: map[string]session.Runtime{"codex": runtime}})
	if err != nil {
		return err
	}
	sess := r.ents[sessionID]
	if sess == nil || sess.session == nil {
		return effects.ErrAuthority
	}
	return r.validateWorkTimeoutSource(runtime, sess, source, event, sealed)
}
