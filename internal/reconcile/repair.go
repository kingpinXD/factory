package reconcile

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

// Repairs. On each full reconcile the tick reads the base branch of every
// repo@base an open item or repair epic works on. A red read opens a repair
// episode, which closes only when the base reads SUCCESS, or when every
// check it lists passes on a newer head: a pending head changes nothing. An
// episode red for red_after with no repair epic open gets one, which the
// planner turns into a fix; one still red red_after after its repair epic
// ended gets another, up to repair_attempts.

// epicKindRepair is the kind of an epic the tick starts for a red base.
const epicKindRepair = "repair"

// baseGreenAgain is why a repair epic is stopped when its base turns green
// before its fix merged.
const baseGreenAgain = "base green again"

type repoBase struct{ repo, base string }

func (b repoBase) String() string { return b.repo + "@" + b.base }

func episodeBase(ep store.RepairEpisode) repoBase { return repoBase{ep.Repo, ep.Base} }

// episodeKey names one episode in the keys of what it does once.
func episodeKey(ep store.RepairEpisode) string {
	return episodeBase(ep).String() + ":" + ep.RedSince.UTC().Format(time.RFC3339)
}

// watchBases reads the base of every watched repo@base and opens, refreshes
// or closes its episode. An episode whose base nobody works on any more is
// dropped. A read error changes nothing.
func (r *run) watchBases() {
	watched := r.watchedBases()
	r.repairs = slices.DeleteFunc(r.repairs, func(ep store.RepairEpisode) bool { return !slices.Contains(watched, episodeBase(ep)) })
	for _, b := range watched {
		res, err := r.baseState(b.repo, b.base)
		if err != nil {
			r.fail("%s: %v", b, err)
			continue
		}
		r.readEpisode(b, res)
	}
	for i := range r.repairs {
		if err := r.refreshWaitingOn(&r.repairs[i]); err != nil {
			r.fail("%s: %v", episodeBase(r.repairs[i]), err)
		}
	}
}

// watchedBases returns, sorted, the repo@base of each open work item and
// open repair epic.
func (r *run) watchedBases() []repoBase {
	var out []repoBase
	for _, id := range r.order() {
		e := r.ents[id]
		var b repoBase
		switch {
		case !e.open():
			continue
		case e.work != nil:
			b = repoBase{e.work.Repo, e.work.Base}
		case isRepairEpic(e):
			b = repoBase{e.epic.Repo, e.epic.Base}
		default:
			continue
		}
		if !slices.Contains(out, b) {
			out = append(out, b)
		}
	}
	slices.SortFunc(out, func(a, b repoBase) int { return strings.Compare(a.String(), b.String()) })
	return out
}

// readEpisode applies one read of b's base to its episode.
func (r *run) readEpisode(b repoBase, res gh.BaseResult) {
	ep, open := r.openEpisode(b.repo, b.base)
	switch {
	case res.Red() && !open:
		red := res.RedChecks()
		r.repairs = append(r.repairs, store.RepairEpisode{Repo: b.repo, Base: b.base, RedSince: r.now, RedSHA: res.SHA, Checks: red})
		r.say("%s: red at %s (%s); repair episode opened", b, abbrev(res.SHA), checkNames(red))
	case res.Red():
		ep.RedSHA = res.SHA
		if red := res.RedChecks(); len(red) > 0 {
			ep.Checks = red
		}
	case open && greenAgain(*ep, res):
		r.closeEpisode(*ep)
	}
}

// greenAgain reports whether a read that is not red closes ep: the base
// reads SUCCESS, or every check ep lists passed on a head other than the
// red one.
func greenAgain(ep store.RepairEpisode, res gh.BaseResult) bool {
	if res.State == "SUCCESS" {
		return true
	}
	if res.SHA == ep.RedSHA || len(ep.Checks) == 0 {
		return false
	}
	for _, c := range ep.Checks {
		if !slices.ContainsFunc(res.Checks, func(n gh.BaseCheck) bool { return n.Name == c.Name && n.Success }) {
			return false
		}
	}
	return true
}

// closeEpisode ends ep, tells the work it held, and stops its open repair
// epic unless the repair's fix is merging or merged.
func (r *run) closeEpisode(ep store.RepairEpisode) {
	r.repairs = slices.DeleteFunc(r.repairs, func(o store.RepairEpisode) bool { return episodeBase(o) == episodeBase(ep) })
	r.closed = append(r.closed, ep)
	r.say("%s: green again; repair episode closed", episodeBase(ep))
	r.releaseHeld(ep)
	rep := r.openRepair(ep.Repo, ep.Base)
	if rep == nil {
		return
	}
	items, err := r.itemsOf(rep)
	if err != nil {
		r.fail("%s: %v", rep.id, err)
		return
	}
	merging := func(it *entity) bool {
		return slices.Contains([]string{workMerging, stateMerged, workDone}, it.cur.State)
	}
	if slices.ContainsFunc(items, merging) {
		return
	}
	stop := Request(RequestStop, workCancelled, blueprint.TriggerUser, baseGreenAgain)
	stop.At, stop.Sender, stop.Key = r.now, events.SenderProgram, "repair-stop:"+episodeKey(ep)
	if _, err := r.append(rep, stop); err != nil {
		r.fail("%s: %v", rep.id, err)
	}
}

// noteRepairs tells each orchestrator working on an item in a red
// repo@base, once per episode, to leave the base's red checks to the repair.
func (r *run) noteRepairs() {
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil || !active(it) {
			continue
		}
		ep, ok := r.episodeOf(it)
		if !ok {
			continue
		}
		text := fmt.Sprintf("%s are red on %s too; %s owns them; do not fix them in this item",
			checkNames(ep.Checks), episodeBase(*ep), repairOwner(r.repairID(ep.Repo, ep.Base)))
		if err := r.tell(it, "repair-note:"+episodeKey(*ep), text); err != nil {
			r.fail("%s: %v", it.id, err)
		}
	}
}

// releaseHeld tells each orchestrator that got ep's note that its base is
// green again, and brings each PR in review that only ep's checks hold up
// to date with the base, once per base commit.
func (r *run) releaseHeld(ep store.RepairEpisode) {
	b := episodeBase(ep)
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil || (repoBase{it.work.Repo, it.work.Base}) != b || r.isRepair(it) {
			continue
		}
		if set := r.ents[it.work.Set]; set != nil && set.has("repair-note:"+episodeKey(ep)) {
			text := fmt.Sprintf("%s is green again; merge origin/%s before your next push", b, ep.Base)
			if err := r.tell(it, "repair-green:"+episodeKey(ep), text); err != nil {
				r.fail("%s: %v", it.id, err)
			}
		}
		if err := r.updateHeldByBase(it, baseCheckNames(ep.Checks)); err != nil {
			r.fail("%s: %v", it.id, err)
		}
	}
}

// updateHeldByBase updates the branch of a watched PR in review whose red
// required checks are all among names.
func (r *run) updateHeldByBase(it *entity, names []string) error {
	if it.cur.State != workInReview || it.pr == nil || !watched(it) {
		return nil
	}
	rd, err := r.readMore(it)
	if err != nil || !redOnBase(failing(gating(rd.checks)), names) {
		return err
	}
	return r.updateBehind(it)
}

// episodeOf returns the open episode of the repo@base of it, a work item
// outside any repair epic.
func (r *run) episodeOf(it *entity) (*store.RepairEpisode, bool) {
	if r.isRepair(it) {
		return nil, false
	}
	return r.openEpisode(it.work.Repo, it.work.Base)
}

// baseRed returns the checks red on the base of it, a work item, that a
// repair owns: those of its repo@base's open episode, or of one closed this
// tick, until its PR is read again on the green base. A repair's own item
// owns its checks itself and gets none.
func (r *run) baseRed(it *entity) []string {
	if r.isRepair(it) {
		return nil
	}
	var names []string
	for _, ep := range slices.Concat(r.repairs, r.closed) {
		if ep.Repo == it.work.Repo && ep.Base == it.work.Base {
			names = append(names, baseCheckNames(ep.Checks)...)
		}
	}
	return names
}

// redOnBase reports whether there are red checks and the base has each.
func redOnBase(red, baseRed []string) bool {
	return len(red) > 0 && !slices.ContainsFunc(red, func(n string) bool { return !slices.Contains(baseRed, n) })
}

// repairID returns the id of repo@base's open repair epic, "" for none.
func (r *run) repairID(repo, base string) string {
	if rep := r.openRepair(repo, base); rep != nil {
		return rep.id
	}
	return ""
}

// repairOwner names what owns a base's red checks: repair epic id, or the
// repair still to start.
func repairOwner(id string) string {
	if id == "" {
		return "the factory's repair"
	}
	return "repair " + id
}

// waitingOnRepair is the merge hold of a PR red only where its base is.
func (r *run) waitingOnRepair(it *entity) string {
	if id := r.repairID(it.work.Repo, it.work.Base); id != "" {
		return "waiting on repair " + id
	}
	return "waiting on " + repoBase{it.work.Repo, it.work.Base}.String()
}

// refreshWaitingOn sets ep.WaitingOn to the PR its newest repair epic, now
// ended, found fixing the base elsewhere, while that PR is open.
func (r *run) refreshWaitingOn(ep *store.RepairEpisode) error {
	past := r.repairsOf(*ep)
	if len(past) == 0 || r.openRepair(ep.Repo, ep.Base) != nil {
		ep.WaitingOn = ""
		return nil
	}
	ref, ok := r.elsewherePR(past[len(past)-1])
	if !ok {
		ep.WaitingOn = ""
		return nil
	}
	ctx, cancel := r.call()
	defer cancel()
	pr, err := r.d.GitHub.PR(ctx, ref.Repo, ref.Number)
	if err != nil {
		return err
	}
	ep.WaitingOn = ""
	if pr.State == prOpen {
		ep.WaitingOn = ref.String()
	}
	return nil
}

// startRepairs starts a repair epic for each episode that is due one, or
// DMs once that its repairs gave up.
func (r *run) startRepairs() {
	for i := range r.repairs {
		if err := r.startRepair(&r.repairs[i]); err != nil {
			r.fail("%s: %v", episodeBase(r.repairs[i]), err)
		}
	}
}

// startRepair starts ep's next repair epic when none is open, the user
// stopped none, it waits on no PR, and red_after passed since it opened or
// its last repair ended. Past repair_attempts it DMs once instead.
func (r *run) startRepair(ep *store.RepairEpisode) error {
	if r.openRepair(ep.Repo, ep.Base) != nil {
		return nil
	}
	past := r.repairsOf(*ep)
	if slices.ContainsFunc(past, stoppedByUser) {
		ep.Stopped = true
	}
	if ep.Stopped || ep.WaitingOn != "" {
		return nil
	}
	since := ep.RedSince
	if n := len(past); n > 0 {
		end, _ := lastTransition(past[n-1].evs)
		since = later(since, end.At)
	}
	if r.now.Sub(since) < time.Duration(r.b.Values.RedAfter) {
		return nil
	}
	if r.attempts(past) >= r.b.Values.RepairAttempts {
		var ids []string
		for _, e := range past {
			ids = append(ids, e.id)
		}
		r.dm(past[len(past)-1], "repair-gave-up:"+episodeKey(*ep), fmt.Sprintf("factory: repair gave up on %s: %s", episodeBase(*ep), strings.Join(ids, ", ")))
		return nil
	}
	id := r.newEpicID()
	in := &EpicInputs{ID: id, Kind: epicKindRepair, Repo: ep.Repo, Base: ep.Base, RepoHint: ep.Repo, Input: r.repairText(*ep, past), AddedAt: r.now}
	dir := filepath.Join(store.Factory(r.d.Brain), "work", id)
	e, err := r.create(id, store.Entry{Kind: blueprint.MachineEpic, Dir: dir, Epic: id}, in, blueprint.TriggerTick)
	if err != nil {
		return err
	}
	r.dm(e, "repair-dm:"+id, fmt.Sprintf("factory: %s red since %s (%s); repair epic %s started",
		episodeBase(*ep), ep.RedSince.UTC().Format(time.RFC3339), checkNames(ep.Checks), id))
	return nil
}

// repairText is a repair epic's input: the red branch, commit and failing
// checks, and what each earlier repair of the episode came to.
func (r *run) repairText(ep store.RepairEpisode, past []*entity) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s is red since %s, at commit %s. These checks fail:\n", episodeBase(ep), ep.RedSince.UTC().Format(time.RFC3339), ep.RedSHA)
	for _, c := range ep.Checks {
		fmt.Fprintf(&b, "- %s: %s\n", c.Name, c.URL)
	}
	reuse := ""
	for i, e := range past {
		if i == 0 {
			b.WriteString("Earlier repairs of this red branch:\n")
		}
		issue, ok := r.fixIssue(e)
		if !ok {
			fmt.Fprintf(&b, "- %s ended %s with no fix issue\n", e.id, e.cur.State)
			continue
		}
		fmt.Fprintf(&b, "- %s ended %s; its fix issue %s\n", e.id, e.cur.State, resultLine(issue))
		reuse = issue.Ref
	}
	if reuse != "" {
		fmt.Fprintf(&b, "Reuse the fix issue %s.\n", reuse)
	}
	return b.String()
}

// fixIssue returns the issue a repair epic's plan fixes the base in: its
// plan's only issue.
func (r *run) fixIssue(ep *entity) (epic.Issue, bool) {
	p, _, err := r.appliedPlan(ep)
	if err != nil || p == nil || len(p.Issues) == 0 {
		return epic.Issue{}, false
	}
	return p.Issues[0], true
}

// elsewherePR returns the open PR a repair epic's plan found fixing the
// base: its fix issue's result is elsewhere, with evidence pr:<ref>.
func (r *run) elsewherePR(ep *entity) (epic.Ref, bool) {
	i, ok := r.fixIssue(ep)
	if !ok || i.Result != epic.ResultElsewhere || i.Evidence == "" {
		return epic.Ref{}, false
	}
	ev, err := epic.ParseEvidence(i.Evidence, ep.epic.Repo)
	return ev.Ref, err == nil && ev.Kind == epic.EvidencePR
}

// attempts counts the repair epics that count toward repair_attempts: all
// but those that found the fix in someone else's PR.
func (r *run) attempts(past []*entity) int {
	n := 0
	for _, e := range past {
		if _, elsewhere := r.elsewherePR(e); !elsewhere {
			n++
		}
	}
	return n
}

// stoppedByUser reports whether a repair epic ended on the user's factory
// stop.
func stoppedByUser(ep *entity) bool {
	if ep.cur.State != workCancelled {
		return false
	}
	t, _ := lastTransition(ep.evs)
	req, ok := eventBySeq(ep, t.TriggerRef)
	return ok && req.Kind == events.KindRequest && req.Request == RequestStop && req.Sender == senderUser
}

// repairsOf returns ep's repair epics: those for its repo@base added since
// it opened, oldest first.
func (r *run) repairsOf(ep store.RepairEpisode) []*entity {
	var out []*entity
	for _, id := range r.order() {
		e := r.ents[id]
		if isRepairEpic(e) && episodeBase(ep) == (repoBase{e.epic.Repo, e.epic.Base}) && !e.epic.AddedAt.Before(ep.RedSince) {
			out = append(out, e)
		}
	}
	slices.SortStableFunc(out, func(a, b *entity) int { return cmp.Compare(a.epic.AddedAt.UnixNano(), b.epic.AddedAt.UnixNano()) })
	return out
}

// openEpisode returns the open repair episode of repo@base.
func (r *run) openEpisode(repo, base string) (*store.RepairEpisode, bool) {
	i := slices.IndexFunc(r.repairs, func(ep store.RepairEpisode) bool { return ep.Repo == repo && ep.Base == base })
	if i < 0 {
		return nil, false
	}
	return &r.repairs[i], true
}

// openRepair returns the open repair epic of repo@base, nil for none.
func (r *run) openRepair(repo, base string) *entity {
	for _, id := range r.order() {
		if e := r.ents[id]; isRepairEpic(e) && e.epic.Repo == repo && e.epic.Base == base && e.open() {
			return e
		}
	}
	return nil
}

func isRepairEpic(e *entity) bool { return e.epic != nil && e.epic.Kind == epicKindRepair }

// isRepair reports whether e, an epic, set, work item or session, belongs
// to a repair epic.
func (r *run) isRepair(e *entity) bool {
	ep := r.ents[e.entry.Epic]
	return ep != nil && isRepairEpic(ep)
}

func checkNames(checks []gh.BaseCheck) string { return strings.Join(baseCheckNames(checks), ", ") }

func baseCheckNames(checks []gh.BaseCheck) []string {
	var names []string
	for _, c := range checks {
		names = append(names, c.Name)
	}
	return names
}
