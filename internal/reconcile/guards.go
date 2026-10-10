package reconcile

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/store"
)

// guard reports whether a guard's condition holds for entity e this tick,
// and names the occurrence that made it hold: the causing event's seq, or
// what GitHub showed. An empty ref means the tick itself.
type guard func(r *run, e *entity) (ok bool, ref string, err error)

// guards are the guards this package implements, by blueprint name.
var guards = map[string]guard{
	// work
	"startable":            (*run).startable,
	"step_started":         (*run).stepStarted,
	"step_ended":           (*run).stepEnded,
	"verify_passed":        func(r *run, e *entity) (bool, string, error) { return r.verdict(e, "pass") },
	"verify_failed":        func(r *run, e *entity) (bool, string, error) { return r.verdict(e, "fail") },
	"question_asked":       (*run).questionAsked,
	"answered":             requested(RequestAnswer),
	"retry_requested":      requested(RequestRetry),
	"retry_allowed":        (*run).retryAllowed,
	"cancel_requested":     requested(RequestStop),
	"instruction_received": (*run).instructionReceived,
	"pr_recorded":          (*run).prRecorded,
	"pr_merged":            func(r *run, e *entity) (bool, string, error) { return prState(e, "MERGED") },
	"pr_closed_unmerged":   func(r *run, e *entity) (bool, string, error) { return prState(e, "CLOSED") },
	"cleaned_up":           (*run).cleanedUp,
	// set and epic
	"lease_taken":       (*run).leaseTaken,
	"session_started":   (*run).sessionStarted,
	"items_finished":    (*run).itemsFinished,
	"item_startable":    (*run).itemStartable,
	"nothing_startable": (*run).nothingStartable,
	"handed_back":       (*run).handedBack,
	// session
	"listed_working":  func(r *run, e *entity) (bool, string, error) { return r.listedState(e, listedWorking) },
	"turn_ended":      func(r *run, e *entity) (bool, string, error) { return r.listedState(e, "done") },
	"no_pid":          (*run).noPID,
	"factory_stopped": (*run).factoryStopped,
	"work_finished":   (*run).workFinished,
	"resume_allowed":  (*run).resumeAllowed,
	// supervision, context and the account
	"restarts_exhausted":      (*run).restartsExhausted,
	"needs_user":              (*run).needsUser,
	"checkpoint_ready":        (*run).checkpointReady,
	"compaction_recorded":     (*run).compactionRecorded,
	"usage_near_limit":        (*run).usageNearLimit,
	"usage_under_limit":       (*run).usageUnderLimit,
	"usage_pause":             (*run).usagePause,
	"usage_stale":             (*run).usageStale,
	"reset_passed":            (*run).resetPassed,
	"reset_passed_near_limit": (*run).resetPassedNearLimit,
	"all_resumed":             (*run).allResumed,
	// the planner flow and re-checks
	"state_check_ended":  (*run).stateCheckEnded,
	"epic_plan_ended":    (*run).epicPlanEnded,
	"work_started":       (*run).workStarted,
	"epic_idle":          (*run).epicIdle,
	"all_in_review":      (*run).allInReview,
	"uat_passed":         (*run).uatPassed,
	"recheck_signal":     (*run).recheckSignal,
	"recheck_ended":      (*run).recheckEnded,
	"recheck_started":    (*run).recheckStarted,
	"recheck_kept":       recheckKept,
	"recheck_dropped":    recheckDropped,
	"recheck_needs_user": recheckNeedsUser,
	"scope_changed":      (*run).scopeChanged,
	// merging
	"merge_ready":  (*run).mergeReady,
	"merge_failed": (*run).mergeFailed,
}

// pending are the guards later TODOs implement, each with what it needs.
// Until then each never holds, so its move never happens.
var pending = map[string]string{}

// registry returns every guard by name: the implemented ones, and for each
// pending one a guard that never holds.
func registry() map[string]guard {
	all := make(map[string]guard, len(guards)+len(pending))
	for name, g := range guards {
		all[name] = g
	}
	for name := range pending {
		all[name] = func(*run, *entity) (bool, string, error) { return false, "", nil }
	}
	return all
}

// guardFuncs adapts the registry to fsm for entity e. The guard that holds
// leaves its ref in *ref.
func (r *run) guardFuncs(e *entity, ref *string) map[string]fsm.GuardFunc {
	reg := registry()
	gs := make(map[string]fsm.GuardFunc, len(reg))
	for name, g := range reg {
		gs[name] = func(fsm.Cur, fsm.Event) (bool, error) {
			ok, why, err := g(r, e)
			if ok {
				*ref = why
			}
			return ok, err
		}
	}
	return gs
}

func seqRef(ev events.Event) string { return strconv.Itoa(ev.Seq) }

// States the tick acts on by name: entering them does something.
const (
	stateStarting    = "starting" // work: make the worktree
	stateWaitingUser = "waiting_user"
	stateMerged      = "merged"
	sessionStarting  = "starting" // session: resume or start it
)

// What a session listing, a PR and a verify output say.
const (
	listedWorking  = "working"
	prClosed       = "CLOSED"
	verdictHeading = "## Verdict"
)

// Names the tick writes and reads back in event logs.
const (
	// stopText is the text of the done event that logs the program
	// stopping a session.
	stopText = "stop"
	// worktreeRequest and cleanupRequest are the repo work requests.
	worktreeRequest = "worktree"
	cleanupRequest  = "cleanup"
	noWorktreeRef   = "no-worktree"
	verifyStep      = "verify"
)

// The components the tick starts sessions for.
const (
	componentPlanner      = "planner"
	componentOrchestrator = "orchestrator"
)

// finishedWork are the work states in which an item counts as merged or
// cancelled.
var finishedWork = []string{"merged", "done", "cancelled"}

// steps maps each active work state to the step whose end moves it on.
var steps = map[string]string{
	"exploring":    "explore",
	"planning":     "plan",
	"plan_review":  "rounds",
	"implementing": "implement",
	"verifying":    verifyStep,
}

// startable: the account is ok and the item can start now.
func (r *run) startable(e *entity) (bool, string, error) {
	if !r.accountOK {
		return false, "", nil
	}
	ok, err := r.canStart(e)
	return ok, "", err
}

// canStart reports whether a queued item may start: no item of its set is
// being worked, no earlier queued item of its set could start instead, and
// it is ready itself. An orchestrator works one item at a time, and moves to
// the next when the current one reaches pr_open.
func (r *run) canStart(it *entity) (bool, error) {
	if it.work == nil || it.cur.State != it.m.Start {
		return false, nil
	}
	items, err := r.setItems(it)
	if err != nil {
		return false, err
	}
	for _, s := range items {
		if s != it && active(s) {
			return false, nil
		}
	}
	for _, s := range items {
		if s == it {
			break
		}
		if s.cur.State == s.m.Start {
			if ok, err := r.ready(s); ok || err != nil {
				return false, err
			}
		}
	}
	return r.ready(it)
}

// active reports whether a work item is being worked: a state whose
// progress is events.
func active(it *entity) bool { return it.m.States[it.cur.State].Health == blueprint.HealthEvents }

// ready reports whether an item's own start conditions hold: its gate:
// start links are released and its repo's base branch is not red.
func (r *run) ready(it *entity) (bool, error) {
	if ok, err := linksReleased(r, it); !ok || err != nil {
		return false, err
	}
	return r.baseGreen(it.work.Repo, it.work.Base)
}

// setItems returns the items of its set, in the set's order; an item with
// no set is alone.
func (r *run) setItems(it *entity) ([]*entity, error) {
	if it.work.Set == "" {
		return []*entity{it}, nil
	}
	set := r.ents[it.work.Set]
	if set == nil || set.set == nil {
		return nil, fmt.Errorf("set %s of %s is not indexed", it.work.Set, it.id)
	}
	return r.itemsOf(set)
}

// itemsOf returns a set's items in its order, or an epic's items by id.
func (r *run) itemsOf(e *entity) ([]*entity, error) {
	var ids []string
	switch {
	case e.set != nil:
		ids = e.set.Items
	case e.epic != nil:
		for _, id := range r.order() {
			if it := r.ents[id]; it.work != nil && it.entry.Epic == e.id {
				ids = append(ids, id)
			}
		}
	}
	var items []*entity
	for _, id := range ids {
		it := r.ents[id]
		if it == nil || it.work == nil {
			return nil, fmt.Errorf("item %s of %s is not indexed", id, e.id)
		}
		items = append(items, it)
	}
	return items, nil
}

func (r *run) stepStarted(e *entity) (bool, string, error) {
	ev, ok := e.newest(events.KindStart, nil)
	return ok, seqRef(ev), nil
}

func (r *run) stepEnded(e *entity) (bool, string, error) {
	step, ok := steps[e.cur.State]
	if !ok {
		return false, "", fmt.Errorf("state %s has no step", e.cur.State)
	}
	end, ok, err := r.ended(e, step)
	return ok, seqRef(end), err
}

// ended returns the end event of step's newest output, when no move has
// used it yet, the file still has the hash it names and the headings its
// component requires. It may be older than the current state: a step can
// start and end between two ticks. A changed file or a missing heading is
// logged once as an error event.
func (r *run) ended(e *entity, step string) (events.Event, bool, error) {
	end, ok := events.NewestEnd(e.evs, step)
	if !ok || e.used(end) {
		return events.Event{}, false, nil
	}
	if r.dry {
		if sum, err := events.HashFile(end.File); err != nil || sum != end.SHA256 {
			return end, false, nil
		}
	} else if _, err := events.Output(store.EventsPath(e.entry.Dir), step); err != nil {
		return end, false, nil
	}
	c, ok := r.outputComponent(step)
	if !ok {
		return end, false, fmt.Errorf("no component writes %s", step)
	}
	missing, err := c.CheckOutput(end.File)
	if err != nil || len(missing) > 0 {
		why := fmt.Sprintf("%s lacks %s", end.File, strings.Join(missing, ", "))
		if err != nil {
			why = err.Error()
		}
		_, logErr := r.append(e, events.Event{Kind: events.KindError, Sender: events.SenderProgram, Text: why, Key: "headings:" + end.File + ":" + end.SHA256})
		return end, false, logErr
	}
	return end, true, nil
}

// outputComponent returns the component one of whose outputs is step's.
func (r *run) outputComponent(step string) (blueprint.Component, bool) {
	for _, c := range r.b.Components {
		for _, o := range c.Outputs {
			if o.Path != blueprint.OutputReply && events.StepOf(o.Path) == step {
				return c, true
			}
		}
	}
	return blueprint.Component{}, false
}

// verdict: the verify step ended and the first line under its ## Verdict
// starts with want.
func (r *run) verdict(e *entity, want string) (bool, string, error) {
	end, ok, err := r.ended(e, verifyStep)
	if !ok || err != nil {
		return false, "", err
	}
	got, err := firstLineUnder(end.File, verdictHeading)
	if err != nil {
		return false, "", err
	}
	return strings.HasPrefix(strings.ToLower(strings.Trim(got, "*_` ")), want), seqRef(end), nil
}

// firstLineUnder returns the first non-blank line after heading in the file.
func firstLineUnder(path, heading string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	in := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == heading:
			in = true
		case in && strings.HasPrefix(line, "#"):
			return "", sc.Err()
		case in && line != "":
			return line, nil
		}
	}
	return "", sc.Err()
}

// questionAsked: a question to the user that no move has used yet, the
// oldest first. It may be older than the current state, when a step ended in
// the same minute.
func (r *run) questionAsked(e *entity) (bool, string, error) {
	for _, ev := range e.evs {
		if ev.Kind == events.KindMessage && ev.Recipient == events.RecipientUser && !e.used(ev) {
			return true, seqRef(ev), nil
		}
	}
	return false, "", nil
}

// requested returns the guard that holds for a request of that name.
func requested(name string) guard {
	return func(_ *run, e *entity) (bool, string, error) {
		if e.req == nil || e.req.Request != name {
			return false, "", nil
		}
		return true, seqRef(*e.req), nil
	}
}

// retryAllowed: a retry was asked for, the item's PR is not closed, and it
// does not wait on the user's answer to a re-check, which factory answer
// gives.
func (r *run) retryAllowed(e *entity) (bool, string, error) {
	ok, ref, err := requested(RequestRetry)(r, e)
	return ok && e.st.PRState != prClosed && !awaitsAnswer(e), ref, err
}

// instructionReceived: an instruction to the entity in its current state;
// for a work item, also one to its set since the item was blocked. The
// planner sends that one during the re-check the block starts, so it can
// predate the item's return from that re-check.
func (r *run) instructionReceived(e *entity) (bool, string, error) {
	if ev, ok := e.newest(events.KindInstruction, nil); ok {
		return true, seqRef(ev), nil
	}
	if e.work == nil || r.ents[e.work.Set] == nil {
		return false, "", nil
	}
	set, since := r.ents[e.work.Set], blockedAt(e)
	for i := len(set.evs) - 1; i >= 0; i-- {
		ev := set.evs[i]
		if ev.Kind == events.KindInstruction && !ev.At.Before(since) {
			return true, set.id + "#" + seqRef(ev), nil
		}
	}
	return false, "", nil
}

// blockedAt returns when the item was last blocked: its newest escalation,
// not a return to blocked from a re-check or needs_you.
func blockedAt(it *entity) time.Time {
	for i := len(it.evs) - 1; i >= 0; i-- {
		if blockedPastRestarts(it.evs[i]) {
			return it.evs[i].At
		}
	}
	return it.cur.Since
}

// prRecorded: the item's PR is recorded and its worktree handed over. The
// handover names the occurrence: after a handback, the same PR moves the
// item to in_review again once the worktree is handed over again.
func (r *run) prRecorded(e *entity) (bool, string, error) {
	ho, ok := lastHandover(e)
	if e.st.PR == 0 || !ok {
		return false, "", nil
	}
	return true, fmt.Sprintf("pr:%d:%d", e.st.PR, ho.Seq), nil
}

// handedOver reports whether the item's worktree is with its babysitter.
func handedOver(e *entity) bool {
	_, ok := lastHandover(e)
	return ok
}

// lastHandover returns the item's newest handover, when it is newer than
// its newest handback.
func lastHandover(e *entity) (events.Event, bool) {
	for i := len(e.evs) - 1; i >= 0; i-- {
		switch e.evs[i].Kind {
		case events.KindHandover:
			return e.evs[i], true
		case events.KindHandback:
			return events.Event{}, false
		}
	}
	return events.Event{}, false
}

// prState: on a full reconcile, GitHub shows the item's PR in state. A
// closed PR moves its item once: from closed it goes on to needs_you, an
// open state, which the same closed PR must not move back.
func prState(e *entity, state string) (bool, string, error) {
	if e.pr == nil || e.pr.State != state {
		return false, "", nil
	}
	if state == "MERGED" {
		return true, "merged:" + e.pr.HeadRefOid, nil
	}
	ref := fmt.Sprintf("closed:%d", e.pr.Number)
	moved := slices.ContainsFunc(e.evs, func(ev events.Event) bool { return ev.Kind == events.KindTransition && ev.TriggerRef == ref })
	return !moved, ref, nil
}

// cleanedUp: the item never had a worktree, or the repo worker finished the
// cleanup asked for last.
func (r *run) cleanedUp(e *entity) (bool, string, error) {
	if _, ok := lastRepoRequest(e, worktreeRequest); !ok {
		return true, noWorktreeRef, nil
	}
	req, ok := lastRepoRequest(e, cleanupRequest)
	if !ok {
		return false, "", nil
	}
	res, ok := repoResult(e, req)
	return ok && res.Kind == events.KindDone, seqRef(res), nil
}

// leaseTaken: the set has a lease and its orchestrator was started.
func (r *run) leaseTaken(e *entity) (bool, string, error) {
	return e.st.Lease > 0 && r.ents[sessionID(e.id, componentOrchestrator)] != nil, "", nil
}

// sessionStarted: the session the entity's work needs is listed.
func (r *run) sessionStarted(e *entity) (bool, string, error) {
	_, ok := r.findSession(sessionName(e.id, sessionComponent(e)))
	return ok, "", nil
}

// itemsFinished: every item of the set or epic is merged or cancelled. An
// epic may have no items; a set without its items has not been made yet.
func (r *run) itemsFinished(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil || e.set != nil && len(items) == 0 {
		return false, "", err
	}
	for _, it := range items {
		if !slices.Contains(finishedWork, it.cur.State) {
			return false, "", nil
		}
	}
	return true, "", nil
}

// itemStartable: an item of the set or epic can start now.
func (r *run) itemStartable(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
		return false, "", err
	}
	for _, it := range items {
		if ok, err := r.canStart(it); ok || err != nil {
			return ok, "", err
		}
	}
	return false, "", nil
}

// nothingStartable: no item of the set is being worked and none can start.
func (r *run) nothingStartable(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
		return false, "", err
	}
	if slices.ContainsFunc(items, active) {
		return false, "", nil
	}
	ok, _, err := r.itemStartable(e)
	return !ok, "", err
}

// handedBack: an item of the set got its worktree back from its babysitter
// since the set entered its state, so its orchestrator has work again.
func (r *run) handedBack(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
		return false, "", err
	}
	for _, it := range items {
		hb, ok := eventBySeq(it, strconv.Itoa(lastHandback(it)))
		if ok && !hb.At.Before(e.cur.Since) {
			return true, it.id + "#" + seqRef(hb), nil
		}
	}
	return false, "", nil
}

// listedState: the session is listed with a pid, in state.
func (r *run) listedState(e *entity, state string) (bool, string, error) {
	s, ok := r.findSession(e.session.Name)
	return ok && s.Live() && s.State == state, "", nil
}

// noPID: the session has been listed with no pid, or not at all, for
// supervision.dead_after.
func (r *run) noPID(e *entity) (bool, string, error) {
	since := e.st.NoPIDSince
	dead := time.Duration(r.b.Values.Supervision.DeadAfter)
	return !since.IsZero() && r.now.Sub(since) >= dead, "", nil
}

// factoryStopped: the program logged a stop of the session in its current
// state, and it is listed with no pid.
func (r *run) factoryStopped(e *entity) (bool, string, error) {
	done, ok := e.newest(events.KindDone, func(ev events.Event) bool { return ev.Text == stopText })
	if !ok {
		return false, "", nil
	}
	s, listed := r.findSession(e.session.Name)
	return listed && !s.Live(), seqRef(done), nil
}

// workFinished: the session's owner is over, so nothing will wake it.
func (r *run) workFinished(e *entity) (bool, string, error) {
	owner := r.ents[e.session.Task]
	return owner == nil || !owner.open(), "", nil
}

// resumeAllowed: the session's owner has work for it and does not wait for
// the user, and the account is ok or asked for this resume after a pause.
// The account's status is read again last: another tick's account step may
// have paused it since this tick's.
func (r *run) resumeAllowed(e *entity) (bool, string, error) {
	owner := r.ents[e.session.Task]
	afterPause := e.req != nil && e.req.Request == RequestResume
	if owner == nil || owner.cur.State == stateNeedsYou || !r.accountOK && !afterPause {
		return false, "", nil
	}
	return r.needsSession(owner) && r.accountAllows(afterPause), "", nil
}
