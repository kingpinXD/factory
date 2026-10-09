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
	// session
	"listed_working":  func(r *run, e *entity) (bool, string, error) { return r.listedState(e, listedWorking) },
	"turn_ended":      func(r *run, e *entity) (bool, string, error) { return r.listedState(e, "done") },
	"no_pid":          (*run).noPID,
	"factory_stopped": (*run).factoryStopped,
	"work_finished":   (*run).workFinished,
	"resume_allowed":  (*run).resumeAllowed,
}

// pending are the guards later TODOs implement, each with what it needs.
// Until then each never holds, so its move never happens. Two parts of
// implemented guards wait too: linksReleased (TODO 11, gate: start links)
// and the account behind accountOK (TODO 10).
var pending = map[string]string{
	"restarts_exhausted":  "TODO 10: restart and nudge counts",
	"needs_user":          "TODO 10: dead-letters, planner runs used up, the same SHA red after a babysitter pass",
	"checkpoint_ready":    "TODO 10: checkpoints",
	"compaction_recorded": "TODO 10: compaction",
	"usage_near_limit":    "TODO 10: the account",
	"usage_under_limit":   "TODO 10: the account",
	"usage_pause":         "TODO 10: the account",
	"usage_stale":         "TODO 10: the account",
	"reset_passed":        "TODO 10: the account",
	"all_resumed":         "TODO 10: the account",
	"state_check_ended":   "TODO 11: the planner flow",
	"epic_plan_ended":     "TODO 11: the planner flow",
	"work_started":        "TODO 11: the planner flow",
	"epic_idle":           "TODO 11: the planner flow",
	"all_in_review":       "TODO 11: the planner flow",
	"uat_passed":          "TODO 11: the planner flow",
	"recheck_signal":      "TODO 11: re-checks",
	"recheck_ended":       "TODO 11: re-checks",
	"recheck_started":     "TODO 11: re-checks",
	"recheck_kept":        "TODO 11: re-checks",
	"recheck_dropped":     "TODO 11: re-checks",
	"scope_changed":       "TODO 11: re-checks",
	"merge_ready":         "TODO 12: merging",
	"merge_failed":        "TODO 12: merging",
}

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

// linksReleased reports whether every gate: start link into the item is
// released. TODO 11 reads them from epic.yaml; until then no item has links.
func linksReleased(*run, *entity) (bool, error) { return true, nil }

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

func (r *run) retryAllowed(e *entity) (bool, string, error) {
	ok, ref, err := requested(RequestRetry)(r, e)
	return ok && e.st.PRState != prClosed, ref, err
}

// instructionReceived: an instruction to the entity in its current state;
// for a work item, also one to its set since the item entered its state.
func (r *run) instructionReceived(e *entity) (bool, string, error) {
	if ev, ok := e.newest(events.KindInstruction, nil); ok {
		return true, seqRef(ev), nil
	}
	if e.work == nil || r.ents[e.work.Set] == nil {
		return false, "", nil
	}
	set := r.ents[e.work.Set]
	for i := len(set.evs) - 1; i >= 0; i-- {
		ev := set.evs[i]
		if ev.Kind == events.KindInstruction && !ev.At.Before(e.cur.Since) {
			return true, set.id + "#" + seqRef(ev), nil
		}
	}
	return false, "", nil
}

// prRecorded: the item's PR is recorded and its worktree handed over.
func (r *run) prRecorded(e *entity) (bool, string, error) {
	if e.st.PR == 0 || !handedOver(e) {
		return false, "", nil
	}
	return true, fmt.Sprintf("pr:%d", e.st.PR), nil
}

// handedOver reports whether the item's worktree is with its babysitter:
// its newest handover or handback is a handover.
func handedOver(e *entity) bool {
	for i := len(e.evs) - 1; i >= 0; i-- {
		switch e.evs[i].Kind {
		case events.KindHandover:
			return true
		case events.KindHandback:
			return false
		}
	}
	return false
}

// prState: on a full reconcile, GitHub shows the item's PR in state.
func prState(e *entity, state string) (bool, string, error) {
	if e.pr == nil || e.pr.State != state {
		return false, "", nil
	}
	if state == "MERGED" {
		return true, "merged:" + e.pr.HeadRefOid, nil
	}
	return true, fmt.Sprintf("closed:%d", e.pr.Number), nil
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

func (r *run) itemsFinished(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
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

// resumeAllowed: the account is ok and the session's owner has work for it.
func (r *run) resumeAllowed(e *entity) (bool, string, error) {
	owner := r.ents[e.session.Task]
	if !r.accountOK || owner == nil {
		return false, "", nil
	}
	return r.needsSession(owner), "", nil
}
