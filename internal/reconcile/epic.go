package reconcile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
)

// States of the epic and work machines the planner flow acts on by name.
const (
	epicChecking = "checking"
	epicPlanning = "planning"
	epicPlanned  = "planned"

	workQueued       = "queued"
	workImplementing = "implementing"
	workPROpen       = "pr_open"
	workInReview     = "in_review"
	workMerging      = "merging"
	workRechecking   = "rechecking"
	workNeedsYou     = "needs_you"
	workCancelled    = "cancelled"
	workClosed       = "closed"
	workBlocked      = "blocked"
)

// The planner's steps, by their output files, and the UAT result's.
const (
	stateCheckStep = "state-check"
	planStep       = "epic"
	uatStep        = "result"
	uatHeading     = "## Result"
	exploreStep    = "explore"
	prStep         = "pr"
)

// entityID is the id of an epic's plan item or set.
func entityID(epicID, planID string) string { return epicID + "-" + planID }

// planID is a work item's or set's id in its epic's plan.
func planID(e *entity) string { return strings.TrimPrefix(e.id, e.entry.Epic+"-") }

// appliedPlan returns the plan the epic acts on: the epic.v<n>.yaml whose
// end event the newest applying move named (planning → planned, or a
// re-check's end), and that end event. It is nil before the first plan.
func (r *run) appliedPlan(ep *entity) (*epic.Plan, events.Event, error) {
	for i := len(ep.evs) - 1; i >= 0; i-- {
		t := ep.evs[i]
		if t.Kind != events.KindTransition {
			continue
		}
		if end, ok := planEnd(ep, t.TriggerRef); ok {
			p, err := loadEnded(end)
			return p, end, err
		}
	}
	return nil, events.Event{}, nil
}

// planEnd returns the end event of an epic.yaml that ref, a seq, names.
func planEnd(ep *entity, ref string) (events.Event, bool) {
	ev, ok := eventBySeq(ep, ref)
	return ev, ok && ev.Kind == events.KindEnd && events.StepOf(ev.File) == planStep
}

// loadEnded reads the plan an end event names, refusing a file changed since.
func loadEnded(end events.Event) (*epic.Plan, error) {
	sum, err := events.HashFile(end.File)
	if err != nil {
		return nil, err
	}
	if sum != end.SHA256 {
		return nil, fmt.Errorf("%s changed after its end event seq %d", end.File, end.Seq)
	}
	return epic.Load(end.File)
}

// itemPlan returns the item's epic and the plan it applied; both are nil
// for an item no plan made.
func (r *run) itemPlan(it *entity) (*entity, *epic.Plan, error) {
	ep := r.ents[it.entry.Epic]
	if ep == nil || ep.epic == nil {
		return nil, nil, nil
	}
	p, _, err := r.appliedPlan(ep)
	return ep, p, err
}

// issueRef is the work item's issue.
func issueRef(it *entity) epic.Ref { return epic.Ref{Repo: it.work.Repo, Number: it.work.Issue} }

// checkedPlan returns the newest ended epic.yaml when the tick may act on
// it. Each version is checked once: one that passes is logged as checked;
// one that fails is logged as an error, and the planner is told why in its
// inbox, so it writes a fixed version.
func (r *run) checkedPlan(ep *entity) (events.Event, bool, error) {
	end, ok := events.NewestEnd(ep.evs, planStep)
	switch {
	case !ok || ep.has(refusedPlanKey(end)):
		return end, false, nil
	case ep.has(checkedPlanKey(end)):
		return end, true, nil
	}
	problems, err := r.planProblems(ep, end)
	if err != nil {
		return end, false, err
	}
	if len(problems) == 0 {
		_, err := r.append(ep, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "checked " + filepath.Base(end.File), Key: checkedPlanKey(end)})
		return end, err == nil, err
	}
	text := fmt.Sprintf("%s is refused: %s. Write a fixed new version and end it.", filepath.Base(end.File), strings.Join(problems, "; "))
	r.say("%s: %s", ep.id, text)
	if _, err := r.append(ep, events.Event{Kind: events.KindError, Sender: events.SenderProgram, Text: text, Key: refusedPlanKey(end)}); err != nil {
		return end, false, err
	}
	_, err = r.append(ep, events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: "tell-" + refusedPlanKey(end)})
	return end, false, err
}

func checkedPlanKey(end events.Event) string { return "checked-plan:" + seqRef(end) }

func refusedPlanKey(end events.Event) string { return "refused-plan:" + seqRef(end) }

// planProblems checks an ended epic.yaml: its hash and keys, the plan's own
// rules, and on GitHub that each issue and blocker exists and no item is
// for an issue already closed.
func (r *run) planProblems(ep *entity, end events.Event) ([]string, error) {
	sum, err := events.HashFile(end.File)
	if err != nil || sum != end.SHA256 {
		return []string{fmt.Sprintf("%s changed after its end event, or is gone", end.File)}, nil
	}
	missing, err := r.b.Components[componentPlanner].CheckOutput(end.File)
	if err != nil {
		return []string{err.Error()}, nil
	}
	if len(missing) > 0 {
		return []string{"it lacks the keys " + strings.Join(missing, ", ")}, nil
	}
	p, err := epic.Load(end.File)
	if err != nil {
		return []string{err.Error()}, nil
	}
	problems := p.Validate(epic.Env{
		Repo:      func(name string) (repos.Repo, error) { return repos.Read(r.d.Brain, name) },
		Deny:      r.b.Deny,
		Foreign:   func(id string) bool { return r.foreignItem(ep, id) != nil },
		UATRunner: !r.b.Components["uat-runner"].Placeholder,
	})
	if len(problems) > 0 {
		return problems, nil
	}
	return r.githubProblems(p)
}

// githubProblems reads each issue and blocker the plan names on GitHub: each
// must exist, and an item's issue must be open.
func (r *run) githubProblems(p *epic.Plan) ([]string, error) {
	var problems []string
	closed := map[string]bool{}
	check := func(ref epic.Ref) error {
		ctx, cancel := r.call()
		defer cancel()
		i, err := r.d.GitHub.Issue(ctx, ref.Repo, ref.Number)
		switch {
		case notFound(err):
			problems = append(problems, fmt.Sprintf("%s: no such issue or PR on GitHub", ref))
		case err != nil:
			return err
		case i.State == "closed":
			closed[strings.ToLower(ref.String())] = true
		}
		return nil
	}
	for _, i := range p.Issues {
		ref, _ := epic.ParseRef(i.Ref)
		if err := check(ref); err != nil {
			return nil, err
		}
	}
	for _, l := range p.Links {
		if _, local := p.Item(l.From); local {
			continue
		}
		if ref, err := epic.ParseRef(l.From); err == nil {
			if err := check(ref); err != nil {
				return nil, err
			}
		}
	}
	for _, it := range p.Items {
		if ref, _ := epic.ParseRef(it.Issue); closed[strings.ToLower(ref.String())] {
			problems = append(problems, fmt.Sprintf("item %s is for %s, which is closed on GitHub", it.ID, ref))
		}
	}
	return problems, nil
}

// notFound reports gh failing with HTTP 404.
func notFound(err error) bool { return err != nil && strings.Contains(err.Error(), "HTTP 404") }

// foreignItem returns another epic's work item with entity id id.
func (r *run) foreignItem(ep *entity, id string) *entity {
	if it := r.ents[id]; it != nil && it.work != nil && it.entry.Epic != ep.id {
		return it
	}
	return nil
}

// stateCheckEnded: the first check's state-check output ended. A re-check
// has a state to return to and moves on its plan's end instead.
func (r *run) stateCheckEnded(e *entity) (bool, string, error) {
	if len(e.cur.Prev) > 0 {
		return false, "", nil
	}
	end, ok, err := r.ended(e, stateCheckStep)
	return ok, seqRef(end), err
}

// epicPlanEnded: the newest epic.yaml ended and passes its check.
func (r *run) epicPlanEnded(e *entity) (bool, string, error) {
	end, ok, err := r.checkedPlan(e)
	return ok, seqRef(end), err
}

// recheckEnded: a re-check's epic.yaml, ended after the re-check began,
// passes its check.
func (r *run) recheckEnded(e *entity) (bool, string, error) {
	if newest, ok := events.NewestEnd(e.evs, planStep); len(e.cur.Prev) == 0 || !ok || newest.Seq < e.entered {
		return false, "", nil
	}
	end, ok, err := r.checkedPlan(e)
	return ok, seqRef(end), err
}

// setsOf returns the epic's sets.
func (r *run) setsOf(ep *entity) []*entity {
	var out []*entity
	for _, id := range r.order() {
		if s := r.ents[id]; s.set != nil && s.entry.Epic == ep.id {
			out = append(out, s)
		}
	}
	return out
}

// workStarted: a set of the epic left queued: an orchestrator holds it.
func (r *run) workStarted(e *entity) (bool, string, error) {
	return slices.ContainsFunc(r.setsOf(e), func(s *entity) bool { return s.cur.State != s.m.Start }), "", nil
}

// epicIdle: no open item can start, and none is being worked or in review.
func (r *run) epicIdle(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
		return false, "", err
	}
	for _, it := range items {
		if h := it.m.States[it.cur.State].Health; it.open() && (h == blueprint.HealthEvents || h == blueprint.HealthGitHub) {
			return false, "", nil
		}
	}
	ok, _, err := r.itemStartable(e)
	return !ok, "", err
}

// allInReview: every open item is in review or merging.
func (r *run) allInReview(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
		return false, "", err
	}
	for _, it := range items {
		if it.open() && it.cur.State != workInReview && it.cur.State != workMerging {
			return false, "", nil
		}
	}
	return true, "", nil
}

// uatPassed: the epic is done: every item merged or cancelled, and its plan
// has uat: none or uat/result.md says pass.
func (r *run) uatPassed(e *entity) (bool, string, error) {
	p, _, err := r.appliedPlan(e)
	if p == nil || err != nil {
		return false, "", err
	}
	passed := false
	if p.UAT != epic.UATNone {
		end, ok, err := r.ended(e, uatStep)
		if err != nil {
			return false, "", err
		}
		if ok {
			got, err := firstLineUnder(end.File, uatHeading)
			if err != nil {
				return false, "", err
			}
			passed = strings.HasPrefix(strings.ToLower(strings.Trim(got, "*_` ")), "pass")
		}
	}
	finished := func(id string) bool {
		it := r.ents[entityID(e.id, id)]
		return it != nil && slices.Contains(finishedWork, it.cur.State)
	}
	return p.Done(finished, passed), "", nil
}

// applyPlan makes the items and sets of the epic's plan that do not exist
// yet, and adds new items to sets that do. Items the plan dropped stay in
// their set, so the set ends only once they are cancelled. A set lists only
// items that exist: one that cannot be made is retried next tick.
func (r *run) applyPlan(ep *entity) error {
	p, _, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return err
	}
	var errs []error
	for _, s := range p.Sets {
		setID := entityID(ep.id, s.ID)
		setDir := filepath.Join(ep.entry.Dir, "sets", setID)
		var items []string
		for _, id := range s.Items {
			it, _ := p.Item(id)
			if err := r.ensureItem(ep, setID, setDir, it); err != nil {
				errs = append(errs, err)
				continue
			}
			items = append(items, entityID(ep.id, id))
		}
		errs = append(errs, r.ensureSet(ep, setID, setDir, items))
	}
	return errors.Join(errs...)
}

func (r *run) ensureSet(ep *entity, id, dir string, items []string) error {
	set := r.ents[id]
	if set == nil {
		_, err := r.create(id, store.Entry{Kind: blueprint.MachineSet, Dir: dir, Epic: ep.id}, &SetInputs{ID: id, Epic: ep.id, Items: items}, blueprint.TriggerFile)
		return err
	}
	for _, old := range set.set.Items {
		if !slices.Contains(items, old) {
			items = append(items, old)
		}
	}
	if slices.Equal(items, set.set.Items) {
		return nil
	}
	set.set.Items = items
	r.say("%s: items now %s", id, strings.Join(items, ", "))
	if r.dry {
		return nil
	}
	return store.WriteInputs(set.entry.Dir, set.set)
}

func (r *run) ensureItem(ep *entity, setID, setDir string, it epic.Item) error {
	id := entityID(ep.id, it.ID)
	if r.ents[id] != nil {
		return nil
	}
	repo, err := repos.Read(r.d.Brain, it.Repo)
	if err != nil {
		return err
	}
	ref, err := epic.ParseRef(it.Issue)
	if err != nil {
		return err
	}
	base := it.Base
	if base == "" {
		base = repo.Default
	}
	in := &WorkInputs{ID: id, Epic: ep.id, Set: setID, Repo: repo.FullName, Clone: repo.Path, Base: base, Issue: ref.Number, PR: it.PR}
	_, err = r.create(id, store.Entry{Kind: blueprint.MachineWork, Dir: filepath.Join(setDir, id), Epic: ep.id}, in, blueprint.TriggerFile)
	return err
}

// dmResults sends the user one DM per issue whose result waits on them,
// with its allowed answers, or that a person or someone else must move.
func (r *run) dmResults(ep *entity) error {
	p, _, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return err
	}
	var errs []error
	for _, i := range p.Issues {
		text := resultDM(ep.id, i)
		if text == "" {
			continue
		}
		key := fmt.Sprintf("dm-result:%s:%s:%s", i.Ref, i.Result, i.Why)
		errs = append(errs, r.once(ep, key, "DM the user "+i.Result+" for "+i.Ref, func(ctx context.Context) error { return r.d.Notify.DM(ctx, text) }))
	}
	return errors.Join(errs...)
}

// resultDM is the DM for an issue's result, or "" when it needs none.
func resultDM(epicID string, i epic.Issue) string {
	head := fmt.Sprintf("factory: %s %s is %s", epicID, i.Ref, i.Result)
	if i.Why != "" {
		head += ": " + i.Why
	}
	switch {
	case i.NeedsUser():
		return fmt.Sprintf("%s\nAnswer with: factory answer %s %s \"<%s>\"", head, epicID, i.Ref, strings.Join(i.Answers, " | "))
	case i.Result == epic.ResultNotCode:
		return head + "\nA person must do it; what waits on it waits until it is closed."
	case i.Result == epic.ResultElsewhere, i.Result == epic.ResultExternal:
		return head + "\nThe factory leaves it alone; what waits on it waits."
	}
	return ""
}

// mirror mirrors the links between the plan's items to GitHub, once per
// applied plan.
func (r *run) mirror(ep *entity) error {
	p, end, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return err
	}
	want := p.IssueLinks(func(id string) (epic.Ref, bool) {
		it := r.foreignItem(ep, id)
		if it == nil {
			return epic.Ref{}, false
		}
		return issueRef(it), it.work.Issue != 0
	})
	path := filepath.Join(ep.entry.Dir, epic.LinksFile)
	return r.once(ep, "mirror:"+seqRef(end), fmt.Sprintf("mirror %d blocked-by links of %s to GitHub", len(want), ep.id), func(ctx context.Context) error {
		return epic.MirrorLinks(ctx, r.d.GitHub, path, want)
	})
}

// once runs a side effect unless a done event with key says it ran, then
// logs that done event.
func (r *run) once(e *entity, key, what string, fn func(context.Context) error) error {
	if e.has(key) {
		return nil
	}
	if err := r.act(what, fn); err != nil {
		return err
	}
	_, err := r.append(e, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: what, Key: key})
	return err
}

// tell puts a message in the inbox of the orchestrator working the item,
// once; with none started yet there is nobody to tell, and a message would
// start one.
func (r *run) tell(it *entity, key, text string) error {
	set := r.ents[it.work.Set]
	if set == nil || r.ents[sessionID(set.id, componentOrchestrator)] == nil || set.has(key) {
		return nil
	}
	_, err := r.append(set, events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: key})
	return err
}

// writeInstructions keeps each instruction to a set as an unchanging file,
// sets/<set-id>/instructions/<seq>.md. Its text reaches the orchestrator in
// the prompt that wakes it.
func (r *run) writeInstructions(set *entity) error {
	if r.dry {
		return nil
	}
	for _, ev := range set.evs {
		if ev.Kind != events.KindInstruction {
			continue
		}
		path := filepath.Join(set.entry.Dir, "instructions", seqRef(ev)+".md")
		if _, err := os.Stat(path); err == nil {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := blueprint.WriteFile(path, []byte(ev.Text+"\n")); err != nil {
			return err
		}
	}
	return nil
}

// chainLength is how many items wait behind the entity's work in its
// epic's plan: for an item, its longest chain; for a set or an epic, the
// longest of its items'. TODO 10 resumes the longest chain first.
func (r *run) chainLength(e *entity) int {
	var ep *entity
	var items []string
	switch {
	case e.work != nil:
		ep, items = r.ents[e.entry.Epic], []string{planID(e)}
	case e.set != nil:
		ep = r.ents[e.entry.Epic]
		for _, id := range e.set.Items {
			items = append(items, strings.TrimPrefix(id, e.entry.Epic+"-"))
		}
	case e.epic != nil:
		ep = e
	}
	if ep == nil || ep.epic == nil {
		return 0
	}
	p, _, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return 0
	}
	if e.epic != nil {
		for _, it := range p.Items {
			items = append(items, it.ID)
		}
	}
	longest := 0
	for _, id := range items {
		longest = max(longest, p.LongestChain(id))
	}
	return longest
}

// prepare runs before an entity's moves each tick: an epic logs the
// re-check signals it has, then the entity settles.
func (r *run) prepare(e *entity) {
	if e.epic != nil {
		if err := r.noticeSignals(e); err != nil {
			r.fail("%s: %v", e.id, err)
		}
	}
	r.settle(e)
}

// settle runs the planner flow's actions that what the entity's log says
// calls for. Each is keyed by an event, so it runs once however often
// settle does: each tick, and after each move.
func (r *run) settle(e *entity) {
	var err error
	switch {
	case e.epic != nil:
		e.st.Rechecks = rechecks(e)
		if e.cur.State == epicChecking && len(e.cur.Prev) > 0 {
			if t, ok := eventBySeq(e, strconv.Itoa(e.entered)); ok {
				err = r.startRecheck(e, t)
			}
		}
		err = errors.Join(err, r.applyPlan(e), r.dmResults(e), r.mirror(e), r.moveEpic(e), r.cancelUnder(e))
	case e.set != nil:
		err = errors.Join(r.writeInstructions(e), r.cancelUnder(e))
	case e.work != nil:
		err = errors.Join(r.assign(e), r.handOver(e), r.handBack(e), r.reread(e), r.cancelItem(e), r.waitDM(e))
	}
	if err != nil {
		r.fail("%s: %v", e.id, err)
	}
}

// rechecks counts the epic's moves into checking for a re-check.
func rechecks(ep *entity) int {
	n := 0
	for _, ev := range ep.evs {
		if ev.Kind == events.KindTransition && ev.To == epicChecking && len(ev.Prev) > 0 {
			n++
		}
	}
	return n
}

// moveEpic moves an epic of one into features/<name>/ when its plan names a
// feature: only while the planner holds it (checking, planning, or planned
// before a set is claimed), under the tick lock, with the planner's turn
// over and no other session of the epic live. The old folder becomes a link
// to the new one, so the absolute paths its events name, and an append that
// raced the move, land in the new folder.
func (r *run) moveEpic(ep *entity) error {
	p, _, err := r.appliedPlan(ep)
	if p == nil || p.Feature == "" || err != nil {
		return err
	}
	from := ep.entry.Dir
	to := filepath.Join(r.d.Brain, "features", p.Feature)
	key := "moved:" + to
	if filepath.Dir(from) != filepath.Join(store.Factory(r.d.Brain), "work") || ep.has("refused-"+key) ||
		!slices.Contains([]string{epicChecking, epicPlanning, epicPlanned}, ep.cur.State) || r.epicBusy(ep) {
		return nil
	}
	err = r.act(fmt.Sprintf("move %s to %s", ep.id, to), func(context.Context) error { return moveDir(from, to) })
	if errors.Is(err, os.ErrExist) {
		text := fmt.Sprintf("features/%s exists already, so %s stays in %s: name another feature in the next epic.yaml", p.Feature, ep.id, from)
		if _, err := r.append(ep, events.Event{Kind: events.KindError, Sender: events.SenderProgram, Text: text, Key: "refused-" + key}); err != nil {
			return err
		}
		_, err = r.append(ep, events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: "tell-refused-" + key})
		return err
	}
	if err != nil || r.dry {
		return err
	}
	r.rebase(from, to)
	_, err = r.append(ep, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "moved to " + to, Key: key})
	return err
}

// moveDir renames from to to and leaves a link to it at from. A from that
// is that link already was moved by a tick that died before writing the
// index.
func moveDir(from, to string) error {
	if dst, err := os.Readlink(from); err == nil {
		if dst == to {
			return nil
		}
		return fmt.Errorf("%s links to %s, not %s", from, dst, to)
	}
	if _, err := os.Lstat(to); err == nil {
		return fmt.Errorf("%s: %w", to, os.ErrExist)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return os.Symlink(to, from)
}

// rebase points every entity in folder from, or below it, at the same place
// in folder to; the index is written when the tick ends.
func (r *run) rebase(from, to string) {
	for id, entry := range r.ix {
		rest, ok := strings.CutPrefix(entry.Dir, from)
		if !ok || rest != "" && !strings.HasPrefix(rest, string(filepath.Separator)) {
			continue
		}
		entry.Dir = to + rest
		r.ix[id] = entry
		if e := r.ents[id]; e != nil {
			e.entry = entry
		}
	}
	r.indexed = true
}

// epicBusy reports whether a session of the epic may be writing to its
// folder: its planner mid-turn, or any other of its sessions live. An
// unknown listing counts as busy.
func (r *run) epicBusy(ep *entity) bool {
	sessions, known := r.listing()
	if !known {
		return true
	}
	for _, id := range r.order() {
		sess := r.ents[id]
		if sess.session == nil || sess.entry.Epic != ep.id {
			continue
		}
		s, ok := claude.FindByName(sessions, sess.session.Name)
		if ok && s.Live() && (sess.session.Component != componentPlanner || s.State == listedWorking) {
			return true
		}
	}
	return false
}
