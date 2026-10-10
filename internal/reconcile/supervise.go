package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// Supervision: after the entities move, the tick checks that the work it
// expects to progress does. A session whose work stalls is nudged through
// its socket, then restarted (stopped; its machine resumes it with no
// flags). Nudges and restarts count: an item that uses up its budget is
// blocked and its epic re-checks it (escalation), and one that keeps doing
// so, or that reports the same error again and again (looping, not
// stalled), waits for the user. While the account holds work back nothing
// here acts, and the time held does not count on any clock.

// supervision is what one tick learns for supervising.
type supervision struct {
	// prev is the overall status.json as the last tick left it.
	prev store.Overall
	// holds are the account's holds, this tick's included.
	holds        []store.Hold
	unknown      int
	autoContinue bool
	// usage is the account step's reading, in the account step's run only.
	usage *usage
	// compactedAt is each live session's newest compaction, read from its
	// transcript this tick, by session entity id.
	compactedAt map[string]time.Time
}

// States supervision acts on by name.
const (
	sessionRunning    = "running"
	sessionTurnEnded  = "turn_ended"
	sessionCompacting = "compacting"
	sessionStopped    = "stopped"
	sessionDead       = "dead"
	sessionFinished   = "finished"
	stateNeedsYou     = "needs_you"
	stateBlocked      = "blocked"
	setRunning        = "running"
	// listedTurnEnded is the listed state of a session whose turn ended.
	listedTurnEnded = "done"
)

// maxHolds bounds the holds kept in status.json; older ones no longer
// overlap any clock that matters.
const maxHolds = 20

// updateHolds opens a hold on the first tick the account holds work back,
// and closes it on the first tick it does not.
func (r *run) updateHolds() {
	h := slices.Clone(r.sup.prev.Holds)
	open := len(h) > 0 && h[len(h)-1].To.IsZero()
	switch {
	case !r.accountOK && !open:
		h = append(h, store.Hold{From: r.now})
	case r.accountOK && open:
		h[len(h)-1].To = r.now
	}
	if len(h) > maxHolds {
		h = h[len(h)-maxHolds:]
	}
	r.sup.holds = h
}

// elapsed returns how long a clock started at since has run by now, without
// the time the account held work back.
func (r *run) elapsed(since time.Time) time.Duration {
	d := r.now.Sub(since)
	for _, h := range r.sup.holds {
		to := h.To
		if to.IsZero() {
			to = r.now
		}
		if from := later(h.From, since); to.After(from) {
			d -= to.Sub(from)
		}
	}
	return d
}

// supervised adds what supervision keeps to the overall status.
func (r *run) supervised(o *store.Overall) {
	o.Holds, o.UnknownListings, o.AutoContinue = r.sup.holds, r.sup.unknown, r.sup.autoContinue
	for _, k := range slices.Sorted(maps.Keys(r.bases)) {
		if res := r.bases[k]; res.err == nil && !res.green {
			o.PausedRepos = append(o.PausedRepos, k)
		}
	}
}

// supervise runs after the entities moved and before sessions are driven.
func (r *run) supervise() {
	r.countListing()
	r.checkAutoContinue()
	r.countOtherRestarts()
	r.dmNeedsYou()
	if !r.accountOK || !r.anySession() {
		return
	}
	if _, known := r.listing(); !known {
		return
	}
	for _, id := range r.order() {
		e := r.ents[id]
		switch {
		case e.session != nil:
			r.superviseSession(e)
		case e.epic != nil && slices.Contains(plannerStates, e.cur.State), e.set != nil && e.cur.State == setRunning:
			r.superviseOwner(e)
		}
	}
}

// anySession reports whether the factory ever started a session.
func (r *run) anySession() bool {
	return slices.ContainsFunc(r.order(), func(id string) bool { return r.ents[id].session != nil })
}

// expectsLiveSession reports whether a session record expects a process:
// then an empty listing is a failed one.
func (r *run) expectsLiveSession() bool {
	return slices.ContainsFunc(r.order(), func(id string) bool {
		e := r.ents[id]
		return e.session != nil && slices.Contains([]string{sessionStarting, sessionRunning, sessionTurnEnded, sessionCompacting}, e.cur.State)
	})
}

// countListing counts the ticks in a row with an unknown listing, and DMs
// once when they reach the blueprint's value.
func (r *run) countListing() {
	r.sup.unknown = r.sup.prev.UnknownListings
	if !r.listed {
		return
	}
	if r.sessionsKnown {
		r.sup.unknown = 0
		return
	}
	r.sup.unknown++
	if r.sup.unknown != r.b.Values.Supervision.UnknownListingsDM {
		return
	}
	text := fmt.Sprintf("factory: `claude agents` failed or came back empty %d ticks in a row; no session starts or moves until it answers.", r.sup.unknown)
	if err := r.act("DM the user: the session listing is unknown", func(ctx context.Context) error { return r.d.Notify.DM(ctx, text) }); err != nil {
		r.fail("%v", err)
	}
}

// checkAutoContinue reports whether the user settings turn on
// autoContinueAtUsageLimit: past a usage limit, factory work would go on
// with usage credits, and only user or managed settings can turn it off.
func (r *run) checkAutoContinue() {
	if r.d.Claude.Home == "" {
		return
	}
	var settings struct {
		AutoContinue bool `json:"autoContinueAtUsageLimit"`
	}
	data, err := os.ReadFile(filepath.Join(r.d.Claude.Home, ".claude", "settings.json"))
	if err == nil && json.Unmarshal(data, &settings) == nil {
		r.sup.autoContinue = settings.AutoContinue
	}
	if r.sup.autoContinue && !r.sup.prev.AutoContinue {
		r.say("warning: autoContinueAtUsageLimit is on in ~/.claude/settings.json: past a usage limit, factory work would go on with usage credits")
	}
}

// superviseOwner checks the session working for owner, a running set's
// orchestrator or a checking or planning epic's planner: its lease, and
// whether its work stalled.
func (r *run) superviseOwner(owner *entity) {
	sess := r.ents[sessionID(owner.id, sessionComponent(owner))]
	if sess == nil || !sess.open() || sess.cur.State == sessionCompacting {
		return
	}
	s, ok := r.findSession(sess.session.Name)
	if !ok || !s.Live() || len(r.undelivered(owner, sess)) > 0 {
		return
	}
	subject := r.subject(owner)
	if owner.set != nil && subject != nil {
		r.checkLease(owner, sess)
	}
	st, stalled := r.stalled(owner, subject, sess, s)
	if !stalled {
		return
	}
	if subject == nil {
		subject = owner
	}
	last, acted := lastAction(subject, st.since)
	switch {
	case !acted:
		r.nudge(owner, subject, s, st.why)
	case last.Kind == events.KindNudge && r.elapsed(last.At) >= st.window:
		r.restart(subject, sess, s, st.why)
	}
}

// subject returns what the owner's session works on now: a set's item
// being worked in a ready worktree it still holds, or a planner's epic.
func (r *run) subject(owner *entity) *entity {
	if owner.epic != nil {
		return owner
	}
	items, _ := r.itemsOf(owner)
	for _, it := range items {
		if _, ready := worktreeReady(it); ready && active(it) && !handedOver(it) {
			return it
		}
	}
	return nil
}

// stall is why a session's work stalled, since when, and how long after a
// nudge it is restarted.
type stall struct {
	why    string
	since  time.Time
	window time.Duration
}

// stalled reports whether the session's work stalled. A working session
// stalls when a message to it stays unacknowledged (unless a helper of its
// heartbeats: the helper's check points bound the delay) or when its work
// shows no progress; a session whose turn ended stalls when its work is
// open and nothing came for an inbox check interval.
func (r *run) stalled(owner, subject, sess *entity, s claude.Session) (stall, bool) {
	v := r.b.Values
	if s.State == listedWorking {
		unacked := time.Duration(v.Inbox.Unacked)
		if inbox := events.Inbox(owner.evs); len(inbox) > 0 && !r.helperBusy(owner) && r.elapsed(inbox[0].At) >= unacked {
			m := inbox[0]
			return stall{fmt.Sprintf("message seq %d unacknowledged for %s", m.Seq, r.elapsed(m.At).Round(time.Minute)), m.At, unacked}, true
		}
		if subject == nil {
			return stall{}, false
		}
		p := r.progress(owner, subject, sess)
		if r.elapsed(p) >= time.Duration(v.Supervision.NoProgress) {
			return stall{fmt.Sprintf("no progress on %s (%s) for %s", subject.id, subject.cur.State, r.elapsed(p).Round(time.Minute)), p, time.Duration(v.Supervision.NoProgress)}, true
		}
		return stall{}, false
	}
	if subject == nil || s.State != listedTurnEnded {
		return stall{}, false
	}
	base := later(r.progress(owner, subject, sess), sess.cur.Since)
	if r.elapsed(base) < time.Duration(v.Inbox.CheckEvery) {
		return stall{}, false
	}
	return stall{fmt.Sprintf("its turn ended with %s %s", subject.id, subject.cur.State), base, time.Duration(v.Supervision.NoProgress)}, true
}

// helperBusy reports whether someone in the set's session heartbeat under
// its lease within the last inbox check interval.
func (r *run) helperBusy(owner *entity) bool {
	if owner.set == nil {
		return false
	}
	for i := len(owner.evs) - 1; i >= 0; i-- {
		if ev := owner.evs[i]; ev.Kind == events.KindHeartbeat && ev.Sender != events.SenderProgram && ev.Lease == owner.st.Lease {
			return r.elapsed(ev.At) < time.Duration(r.b.Values.Inbox.CheckEvery)
		}
	}
	return false
}

// progress returns when the owner's session last showed progress on
// subject: an event it wrote to the owner's or the subject's log (a
// heartbeat only under the set's current lease), the subject entering its
// state, or the session (re)starting.
func (r *run) progress(owner, subject, sess *entity) time.Time {
	t := subject.cur.Since
	for _, e := range slices.Compact([]*entity{owner, subject}) {
		for _, ev := range e.evs {
			if ev.Sender == owner.id && (ev.Kind != events.KindHeartbeat || ev.Lease == owner.st.Lease) {
				t = later(t, ev.At)
			}
		}
	}
	return later(t, startedAt(sess))
}

// startedAt returns when the session last started or was resumed: its
// newest move into starting, or its first move.
func startedAt(sess *entity) time.Time {
	if start, ok := newestTransitionTo(sess, sessionStarting); ok {
		return start.At
	}
	for _, ev := range sess.evs {
		if ev.Kind == events.KindTransition {
			return ev.At
		}
	}
	return time.Time{}
}

// lastAction returns the newest nudge or restart logged on e at or after since.
func lastAction(e *entity, since time.Time) (events.Event, bool) {
	for i := len(e.evs) - 1; i >= 0; i-- {
		ev := e.evs[i]
		if ev.At.Before(since) {
			break
		}
		if ev.Kind == events.KindNudge || ev.Kind == events.KindRestart {
			return ev, true
		}
	}
	return events.Event{}, false
}

// nudge posts the session a reminder of its work, and counts it on subject.
func (r *run) nudge(owner, subject *entity, s claude.Session, why string) {
	text := r.nudgePrompt(owner, why)
	if err := r.act(fmt.Sprintf("nudge %s: %s", s.Name, why), func(ctx context.Context) error { return r.d.Claude.Post(ctx, s.PID, text) }); err != nil {
		r.fail("%v", err)
		return
	}
	r.logged(r.append(subject, events.Event{At: r.now, Kind: events.KindNudge, Sender: events.SenderProgram, Text: why, Key: "nudge:" + strconv.Itoa(r.n)}))
}

// restart stops the session and counts a restart on subject. Its machine
// then moves it to stopped and resumes it with no flags.
func (r *run) restart(subject, sess *entity, s claude.Session, why string) {
	key := "restart:" + strconv.Itoa(r.n)
	if err := r.stopSession(sess, s, key, fmt.Sprintf("restart %s: %s", s.Name, why)); err != nil {
		r.fail("%v", err)
		return
	}
	r.logged(r.append(subject, events.Event{At: r.now, Kind: events.KindRestart, Sender: events.SenderProgram, Text: why, Key: key}))
}

// stopSession stops a session and logs the stop in its log, so its machine
// moves to stopped.
func (r *run) stopSession(sess *entity, s claude.Session, key, what string) error {
	if err := r.act(what, func(ctx context.Context) error { return r.d.Claude.Stop(ctx, s.ID) }); err != nil {
		return err
	}
	_, err := r.append(sess, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: stopText, Key: "stop:" + key})
	return err
}

// nudgePrompt names why the session is nudged and what it works on.
func (r *run) nudgePrompt(owner *entity, why string) string {
	lead := fmt.Sprintf("Nudge: %s. Run `factory inbox %s`, then carry on from your newest event.", why, owner.id)
	if owner.set != nil {
		return r.orchestratorPrompt(owner, lead, r.workingItems(owner), r.deliveries(owner))
	}
	return r.plannerPrompt(owner, r.deliveries(owner)) + "\n" + lead + "\n"
}

// resumePrompt is what a stopped or dead session is woken with: all it
// works on, what is new, and the outside actions it logged an intent for
// and no done.
func (r *run) resumePrompt(owner *entity, component string, news []delivery) string {
	prompt := r.wakePrompt(owner, component, news)
	if component == componentOrchestrator {
		prompt = r.orchestratorPrompt(owner, "You were stopped and are resumed. Carry on from your newest event.", r.workingItems(owner), news)
	}
	return prompt + r.outboxSection(owner)
}

// outboxSection lists the intents the owner's session logged, in its own
// log or its items', with no done after them: after a restart each is
// checked against GitHub or Slack before it is redone.
func (r *run) outboxSection(owner *entity) string {
	logs := []*entity{owner}
	if owner.set != nil {
		items, _ := r.itemsOf(owner)
		logs = append(logs, items...)
	}
	var b strings.Builder
	for _, e := range logs {
		for _, ev := range openIntents(e, owner.id) {
			if b.Len() == 0 {
				b.WriteString("\nOutside actions you logged an intent for and no done: check each on GitHub or Slack before you redo it:\n")
			}
			fmt.Fprintf(&b, "- %s seq %d: %s\n", e.id, ev.Seq, ev.Text)
		}
	}
	return b.String()
}

// openIntents returns the intents sender logged in e that no later done
// closes, each done closing the oldest open intent.
func openIntents(e *entity, sender string) []events.Event {
	var open []events.Event
	for _, ev := range e.evs {
		switch {
		case ev.Sender != sender:
		case ev.Kind == events.KindIntent:
			open = append(open, ev)
		case ev.Kind == events.KindDone && len(open) > 0:
			open = open[1:]
		}
	}
	return open
}

// checkLease revokes the set's lease when nothing renewed it for the
// lease's length: the lease number goes up, so the old holder's next
// heartbeat and lease-ok fail and it pushes nothing. Its next prompt
// carries the new number.
func (r *run) checkLease(set, sess *entity) {
	lease := set.st.Lease
	renewed := r.leaseRenewed(set, sess)
	if lease == 0 || r.elapsed(renewed) < time.Duration(r.b.Values.Lease) {
		return
	}
	set.st.Lease++
	text := fmt.Sprintf("lease %d expired after %s with no heartbeat; lease %d granted", lease, r.elapsed(renewed).Round(time.Minute), set.st.Lease)
	r.say("%s: %s", set.id, text)
	r.logged(r.append(set, events.Event{At: r.now, Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: set.st.Lease, Text: text, Key: "lease:" + strconv.Itoa(set.st.Lease)}))
}

// leaseRenewed returns when the set's current lease was last renewed: a
// heartbeat under its number, or its orchestrator (re)starting with it.
func (r *run) leaseRenewed(set, sess *entity) time.Time {
	t := startedAt(sess)
	for _, ev := range set.evs {
		if ev.Kind == events.KindHeartbeat && ev.Lease == set.st.Lease {
			t = later(t, ev.At)
		}
	}
	return t
}

// superviseSession acts on one session: a finished one still idle is
// stopped; an automatic compaction gets its "you were compacted" message;
// on a full reconcile, a session that used a denied model is stopped.
func (r *run) superviseSession(sess *entity) {
	s, ok := r.findSession(sess.session.Name)
	if !ok || !s.Live() {
		return
	}
	if sess.cur.State == sessionFinished {
		if s.State == listedTurnEnded {
			if err := r.stopSession(sess, s, "finished", "stop "+s.Name+": its work is over"); err != nil {
				r.fail("%v", err)
			}
		}
		return
	}
	r.postCompactions(sess, s)
	if r.full {
		r.checkModels(sess, s)
	}
}

// checkModels stops a session whose transcript, or a helper's, shows a
// denied model, and logs an error on what it works on, which then waits for
// the user.
func (r *run) checkModels(sess *entity, s claude.Session) {
	ctx, cancel := r.call()
	models, err := r.d.Claude.Models(ctx, s.SessionID)
	cancel()
	if errors.Is(err, claude.ErrNoTranscript) {
		return
	}
	if err != nil {
		r.fail("%s: %v", sess.id, err)
		return
	}
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return
	}
	subject := r.subject(owner)
	if subject == nil {
		subject = owner
	}
	for _, file := range slices.Sorted(maps.Keys(models)) {
		for _, m := range models[file] {
			denied, ok := blueprint.DeniedModel(m, r.b.Deny.Models)
			key := "fable:" + s.SessionID + ":" + file
			if !ok || subject.has(key) || owner.has(key) {
				continue
			}
			if err := r.stopSession(sess, s, key, fmt.Sprintf("stop %s: it ran %s", s.Name, m)); err != nil {
				r.fail("%v", err)
				return
			}
			text := fmt.Sprintf("%s ran on %s, which is denied (%s); the session is stopped", strings.TrimSuffix(file, ".jsonl"), m, denied)
			r.logged(r.append(subject, events.Event{At: r.now, Kind: events.KindError, Sender: events.SenderProgram, Text: text, Key: key}))
		}
	}
}

// countOtherRestarts counts the restarts this tick made besides the
// supervisor's own: a state restarted by its timeout (whose session is then
// restarted too) and a dead session woken.
func (r *run) countOtherRestarts() {
	for _, id := range r.order() {
		e := r.ents[id]
		evs := e.evs
		for _, t := range evs {
			if t.Kind != events.KindTransition || !t.At.Equal(r.now) {
				continue
			}
			switch {
			case e.session == nil && t.From == t.To && strings.HasPrefix(t.TriggerRef, timeoutRef):
				r.logged(r.append(e, events.Event{At: r.now, Kind: events.KindRestart, Sender: events.SenderProgram, Text: t.From + " timed out", Key: "restart:timeout:" + seqRef(t)}))
				r.restartWorker(e, "restart:timeout:"+e.id+":"+seqRef(t))
			case e.session != nil && t.From == sessionDead && t.To == sessionStarting:
				owner := r.ents[e.session.Task]
				if owner == nil {
					continue
				}
				subject := r.subject(owner)
				if subject == nil {
					subject = owner
				}
				r.logged(r.append(subject, events.Event{At: r.now, Kind: events.KindRestart, Sender: events.SenderProgram, Text: e.session.Name + " was dead", Key: "restart:dead:" + e.id + ":" + seqRef(t)}))
			}
		}
	}
}

// timeoutRef starts the trigger_ref of a move by a state's timeout.
const timeoutRef = "timeout:"

// restartWorker stops the session working on e after e's state timed out:
// the orchestrator on the set's current item, or the epic's planner.
func (r *run) restartWorker(e *entity, key string) {
	owner := e
	if e.work != nil {
		owner = r.ents[e.work.Set]
	}
	if owner == nil || sessionComponent(owner) == "" || r.subject(owner) != e {
		return
	}
	sess := r.ents[sessionID(owner.id, sessionComponent(owner))]
	if sess == nil {
		return
	}
	s, ok := r.findSession(sess.session.Name)
	if !ok || !s.Live() {
		return
	}
	if err := r.stopSession(sess, s, key, fmt.Sprintf("restart %s: %s timed out in %s", s.Name, e.id, e.cur.State)); err != nil {
		r.fail("%v", err)
	}
}

// lastLeft returns the seq of e's newest move out of one of states, 0 for
// none: leaving blocked (a planner instruction) or needs_you (a retry)
// gives an entity a new budget.
func lastLeft(e *entity, states ...string) int {
	for i := len(e.evs) - 1; i >= 0; i-- {
		if ev := e.evs[i]; ev.Kind == events.KindTransition && ev.From != ev.To && slices.Contains(states, ev.From) {
			return ev.Seq
		}
	}
	return 0
}

// countedSince returns the events of e after seq that match, oldest first.
func countedSince(e *entity, seq int, match func(events.Event) bool) []events.Event {
	var out []events.Event
	for _, ev := range e.evs {
		if ev.Seq > seq && match(ev) {
			out = append(out, ev)
		}
	}
	return out
}

func isNudgeOrRestart(ev events.Event) bool {
	return ev.Kind == events.KindNudge || ev.Kind == events.KindRestart
}

// itemRestarts returns the nudges and restarts of a work item since it
// last left blocked or needs_you.
func itemRestarts(e *entity) []events.Event {
	return countedSince(e, lastLeft(e, stateBlocked, stateNeedsYou), isNudgeOrRestart)
}

// escalations counts e's moves into blocked because its restarts ran out,
// since the user last retried it: one planner run each.
func escalations(e *entity) int {
	return len(countedSince(e, lastLeft(e, stateNeedsYou), func(ev events.Event) bool {
		return ev.Kind == events.KindTransition && ev.To == stateBlocked && strings.HasPrefix(ev.TriggerRef, restartsRef)
	}))
}

// restartsRef starts the trigger_ref of a move into blocked because the
// entity used up its restarts or nudges.
const restartsRef = "restarts:"

// hourRestarts returns the restarts of the session working for owner in
// the last hour, since the owner last left blocked or needs_you: a set's
// orchestrator (logged on its items) or an epic's planner.
func (r *run) hourRestarts(owner *entity) []events.Event {
	logs := []*entity{owner}
	if owner.set != nil {
		items, _ := r.itemsOf(owner)
		logs = append(logs, items...)
	}
	since := r.now.Add(-time.Hour)
	if left := lastLeft(owner, stateBlocked, stateNeedsYou); left > 0 {
		if ev, ok := eventBySeq(owner, strconv.Itoa(left)); ok {
			since = later(since, ev.At)
		}
	}
	var out []events.Event
	for _, e := range logs {
		for _, ev := range e.evs {
			if ev.Kind == events.KindRestart && ev.At.After(since) {
				out = append(out, ev)
			}
		}
	}
	return out
}

// restartLimit is how many restarts per hour a session component gets.
func (r *run) restartLimit(component string) int {
	if n := r.b.Components[component].Limits.RestartsPerHour; n > 0 {
		return n
	}
	return r.b.Values.Supervision.RestartsPerComponentPerHour
}

// restartsExhausted: a work item used up its nudges and restarts, or a
// set's orchestrator was restarted too often in an hour, and the planner
// has not yet run for it as often as it may: the move to blocked is the
// escalation the epic re-checks.
func (r *run) restartsExhausted(e *entity) (bool, string, error) {
	ref, ok := r.restartsUsedUp(e)
	if !ok || escalations(e) >= r.b.Values.Supervision.PlannerRunsPerProblem {
		return false, "", nil
	}
	return true, ref, nil
}

// restartsUsedUp reports whether e used up its restarts, with the ref of
// the restart that used up the last one.
func (r *run) restartsUsedUp(e *entity) (string, bool) {
	var counted []events.Event
	switch {
	case e.work != nil:
		if counted = itemRestarts(e); len(counted) < r.b.Values.Supervision.RestartsPerItem {
			return "", false
		}
	case e.set != nil, e.epic != nil:
		if counted = r.hourRestarts(e); len(counted) < r.restartLimit(sessionComponent(e)) {
			return "", false
		}
	default:
		return "", false
	}
	last := counted[len(counted)-1]
	return fmt.Sprintf("%s%s:%d@%s", restartsRef, e.id, len(counted), last.At.Format(time.RFC3339)), true
}

// needsUserReasons are the reasons only the user can settle an entity's
// work. Each returns the ref its move to needs_you is logged under. Other
// parts of the tick add theirs from their own files with an init func.
var needsUserReasons = []func(r *run, e *entity) (ref string, ok bool){
	(*run).deadLettered,
	(*run).plannerRunsUsedUp,
	(*run).deniedModelUsed,
}

// needsUser: one of the reasons holds.
func (r *run) needsUser(e *entity) (bool, string, error) {
	for _, reason := range needsUserReasons {
		if ref, ok := reason(r, e); ok {
			return true, ref, nil
		}
	}
	return false, "", nil
}

// Refs of the moves to needs_you this file DMs about.
const (
	deadLetterRef  = "dead-letter:"
	plannerRunsRef = "planner-runs:"
	deniedModelRef = "denied-model:"
)

// deadLettered: an agent reported the same error the blueprint's number of
// times since the user last retried the entity: it loops, so restarting it
// does not help.
func (r *run) deadLettered(e *entity) (string, bool) {
	seen := map[string]int{}
	for _, ev := range countedSince(e, lastLeft(e, stateNeedsYou), func(ev events.Event) bool {
		return ev.Kind == events.KindError && ev.Sender != events.SenderProgram
	}) {
		if seen[ev.Text]++; seen[ev.Text] >= r.b.Values.Supervision.SameErrorDeadLetter {
			return deadLetterRef + seqRef(ev), true
		}
	}
	return "", false
}

// plannerRunsUsedUp: e used up its restarts again after the planner ran
// for it as often as it may; for an epic, its planner was restarted too
// often in an hour, since nothing above the planner can re-plan it. While
// e is blocked, its last planner run is still to come.
func (r *run) plannerRunsUsedUp(e *entity) (string, bool) {
	if e.cur.State == stateBlocked {
		return "", false
	}
	ref, ok := r.restartsUsedUp(e)
	if !ok || e.epic == nil && escalations(e) < r.b.Values.Supervision.PlannerRunsPerProblem {
		return "", false
	}
	return plannerRunsRef + strings.TrimPrefix(ref, restartsRef), true
}

// deniedModelUsed: a session working on e ran a denied model since the
// user last retried it.
func (r *run) deniedModelUsed(e *entity) (string, bool) {
	used := countedSince(e, lastLeft(e, stateNeedsYou), func(ev events.Event) bool {
		return ev.Kind == events.KindError && strings.HasPrefix(ev.Key, "fable:")
	})
	if len(used) == 0 {
		return "", false
	}
	return deniedModelRef + seqRef(used[len(used)-1]), true
}

// dmNeedsYou DMs the user once for each move into needs_you this file's
// reasons made, and for one made by a state's timeout.
func (r *run) dmNeedsYou() {
	for _, id := range r.order() {
		e := r.ents[id]
		t, ok := lastTransition(e.evs)
		if !ok || t.To != stateNeedsYou || e.cur.State != stateNeedsYou {
			continue
		}
		if why := r.needsYouWhy(e, t); why != "" {
			r.dm(e, "dm:needs-you:"+seqRef(t), fmt.Sprintf("factory: %s needs you: %s. When it is settled: factory retry %s, or factory stop %s --reason \"<why>\".", e.id, why, e.id, e.id))
		}
	}
}

// needsYouWhy says why e moved to needs_you, or "" when the move is not
// one this file DMs about.
func (r *run) needsYouWhy(e *entity, t events.Event) string {
	ref := t.TriggerRef
	switch {
	case strings.HasPrefix(ref, timeoutRef):
		return fmt.Sprintf("%s timed out after %s", t.From, time.Duration(e.m.States[t.From].Timeout))
	case strings.HasPrefix(ref, deadLetterRef):
		ev, _ := eventBySeq(e, strings.TrimPrefix(ref, deadLetterRef))
		return fmt.Sprintf("the same error %d times (dead-lettered): %s", r.b.Values.Supervision.SameErrorDeadLetter, ev.Text)
	case strings.HasPrefix(ref, plannerRunsRef) && e.epic != nil:
		return "its planner was restarted too often in an hour"
	case strings.HasPrefix(ref, plannerRunsRef):
		return fmt.Sprintf("it stalled again after %d planner runs", r.b.Values.Supervision.PlannerRunsPerProblem)
	case strings.HasPrefix(ref, deniedModelRef):
		ev, _ := eventBySeq(e, strings.TrimPrefix(ref, deniedModelRef))
		return ev.Text
	}
	return ""
}

// dm sends the user text once per key, logged in e's log: an intent before
// the DM and a done after it, as for every outside action.
func (r *run) dm(e *entity, key, text string) {
	if e.has(key) {
		return
	}
	r.logged(r.append(e, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Text: key, Key: key + ":intent"}))
	if err := r.act("DM the user about "+e.id, func(ctx context.Context) error { return r.d.Notify.DM(ctx, text) }); err != nil {
		r.fail("%v", err)
		return
	}
	r.logged(r.append(e, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: key, Key: key}))
}

// usedRef reports whether a transition of e names ref.
func usedRef(e *entity, ref string) bool {
	return slices.ContainsFunc(e.evs, func(ev events.Event) bool { return ev.Kind == events.KindTransition && ev.TriggerRef == ref })
}
