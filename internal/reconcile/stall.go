package reconcile

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
)

// Stalls. On each full reconcile, after the PRs are watched, the tick looks
// for work that waits on a cause that is gone. An item the program sent to
// needs_you for a cause it can re-test — its head stayed red after a
// babysitter pass, or pr_open timed out — is released the way `factory
// retry` releases it once the cause is gone. Leaving needs_you resets the
// item's budgets, so releases have their own: releasesPerDay in the 24h
// before an escalation, after which it stays for the user.
//
// Then it looks for work that waits on no one. An open item in queued,
// in_review, blocked or rechecking that no one has moved for stall_after is
// stalled: its epic re-checks it and the user gets one DM. The other work
// states have their own timeout, and needs_you and waiting_user are the
// user's. An item stalled again after planner_runs_per_problem of those
// re-checks waits for the user.
//
// Last, the user gets one digest of the work that waited in its state
// longer than wait_dm, and a PR no one reviewed for wait_dm after its
// review request gets one reminder post.

// Key prefixes of a release request, by the move into needs_you it
// releases, and of its DM, by the request.
const (
	autoReleasePrefix   = "auto-release:"
	autoReleaseDMPrefix = "auto-release-dm:"
)

// releasesPerDay is how many times one item is released in 24h.
const releasesPerDay = 2

// checkStalls runs on a full reconcile, after the PRs are watched.
func (r *run) checkStalls() {
	r.releaseEscalations()
	r.dmReleases()
	r.findStalls()
	r.digestWaits()
	r.remindReviews()
}

// releaseEscalations releases each releasable item whose cause is gone. A
// head that is still the red one needs a read of its checks: at most
// stall_reads of those per full reconcile, the least recently read first.
func (r *run) releaseEscalations() {
	var reads []*entity
	for _, id := range r.order() {
		it := r.ents[id]
		if !releasable(it) {
			continue
		}
		sha, red := strings.CutPrefix(it.enteredBy.TriggerRef, redAfterPassRef)
		switch {
		case !red:
			if ok, _, _ := r.prRecorded(it); ok {
				r.releaseItem(it, fmt.Sprintf("its PR %s is recorded and handed to its babysitter", it.st.PRURL))
			}
		case it.pr == nil: // its PR was not read this reconcile
		case it.pr.HeadRefOid != sha:
			r.releaseItem(it, fmt.Sprintf("its head moved from %s to %s", abbrev(sha), abbrev(it.pr.HeadRefOid)))
		default:
			reads = append(reads, it)
		}
	}
	slices.SortStableFunc(reads, func(a, b *entity) int { return a.st.ChecksReadAt.Compare(b.st.ChecksReadAt) })
	for _, it := range reads[:min(len(reads), r.b.Values.StallReads)] {
		r.retestChecks(it)
	}
}

// releasable: a work item is in needs_you for a cause the program re-tests,
// it has no release for that move yet, and its releases are not used up.
func releasable(it *entity) bool {
	t := it.enteredBy
	return it.work != nil && it.cur.State == stateNeedsYou && retestable(t) &&
		!it.has(autoReleaseKey(t)) && !releasesUsedUp(it, t)
}

// retestable: the move into needs_you was made because a head stayed red
// after a babysitter pass, or because pr_open timed out.
func retestable(t events.Event) bool {
	return strings.HasPrefix(t.TriggerRef, redAfterPassRef) || t.From == workPROpen && strings.HasPrefix(t.TriggerRef, timeoutRef)
}

func autoReleaseKey(t events.Event) string { return autoReleasePrefix + seqRef(t) }

func isRelease(ev events.Event) bool {
	return ev.Kind == events.KindRequest && strings.HasPrefix(ev.Key, autoReleasePrefix)
}

// releasesUsedUp: the item was released releasesPerDay times in the 24h
// before move t.
func releasesUsedUp(it *entity, t events.Event) bool {
	return len(countedSince(it, 0, func(ev events.Event) bool {
		return isRelease(ev) && ev.Seq < t.Seq && ev.At.After(t.At.Add(-24*time.Hour))
	})) >= releasesPerDay
}

// retestChecks reads the checks on the item's red head: none red that a
// merge waits for, or each red on its base too, which a repair owns, is
// the cause gone.
func (r *run) retestChecks(it *entity) {
	it.st.ChecksReadAt = r.now
	var checks []gh.Check
	if err := r.read(func(ctx context.Context) (err error) {
		checks, err = r.d.GitHub.Checks(ctx, it.work.Repo, it.pr.Number, it.pr.HeadRefOid)
		return
	}); err != nil {
		r.fail("%s: %v", it.id, err)
		return
	}
	switch red := failing(gating(checks)); {
	case len(red) == 0:
		r.releaseItem(it, fmt.Sprintf("no required check is red on %s now", abbrev(it.pr.HeadRefOid)))
	case redOnBase(red, r.baseRed(it)):
		r.releaseItem(it, fmt.Sprintf("its red checks (%s) are red on the base too, which its repair owns", strings.Join(red, ", ")))
	}
}

// releaseItem logs the program's retry request for the item's move into
// needs_you; the next tick applies it.
func (r *run) releaseItem(it *entity, why string) {
	req := Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, why)
	req.At, req.Sender, req.Key = r.now, events.SenderProgram, autoReleaseKey(it.enteredBy)
	if _, err := r.append(it, req); err != nil {
		r.fail("%s: %v", it.id, err)
		return
	}
	r.say("%s: cause gone: %s", it.id, why)
}

// dmReleases DMs the user once for each release, so the DM that said the
// item needs them is not left standing. It runs on every full reconcile, so
// a DM that failed is sent on a later one.
func (r *run) dmReleases() {
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil {
			continue
		}
		for _, ev := range it.evs {
			if isRelease(ev) {
				r.dm(it, autoReleaseDMPrefix+seqRef(ev), fmt.Sprintf("factory: %s released: %s.", it.id, ev.Text))
			}
		}
	}
}

// releasedNote says, for a releasable cause, that the item stays for the
// user because it was released as often as it may be.
func releasedNote(e *entity, t events.Event) string {
	if e.work == nil || !retestable(t) || !releasesUsedUp(e, t) {
		return ""
	}
	return fmt.Sprintf("; it was released automatically %d times in 24h and the cause came back, so it stays for you", releasesPerDay)
}

// stallPrefix starts the key of a stall signal and its DM:
// stall:<item>:<seq of the item's move into the state it stalled in>.
const stallPrefix = signalStall + ":"

// stallsRef starts the ref of a move to needs_you of an item that stalled
// again after its stall re-checks: stalls:<item>:<seq of its state entry>.
const stallsRef = "stalls:"

// stallStates are the work states in which an item stalls.
var stallStates = []string{workQueued, workInReview, stateBlocked, workRechecking}

func init() { needsUserReasons = append(needsUserReasons, (*run).stallsUsedUp) }

// findStalls re-checks each stalled item once in its state: a DM, then a
// stall signal to its epic. An item that used up its stall re-checks is
// left to stallsUsedUp.
func (r *run) findStalls() {
	for _, id := range r.order() {
		it := r.ents[id]
		age, stalled := r.stalledFor(it)
		if !stalled || r.stallSignals(it) >= r.b.Values.Supervision.PlannerRunsPerProblem {
			continue
		}
		ep, key := r.ents[it.entry.Epic], fmt.Sprintf("%s%s:%d", stallPrefix, it.id, it.entered)
		if ep == nil || ep.has(key) {
			continue
		}
		age = age.Round(time.Minute)
		r.dm(it, key, fmt.Sprintf("factory: %s (%s) stalled in %s: no one has moved it for %s. %s re-checks it.", it.id, issueRef(it), it.cur.State, age, ep.id))
		sig := events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientRecheck,
			Request: signalStall, Text: issueRef(it).String(), Key: key}
		if _, err := r.append(ep, sig); err != nil {
			r.fail("%s: %v", ep.id, err)
			continue
		}
		r.say("%s: stalled in %s for %s; %s re-checks it", it.id, it.cur.State, age, ep.id)
	}
}

// stalledFor returns how long no one has moved the item, and whether it is
// stalled: in a state it stalls in, with no mover, for stall_after.
func (r *run) stalledFor(it *entity) (time.Duration, bool) {
	if it.work == nil || !slices.Contains(stallStates, it.cur.State) {
		return 0, false
	}
	age := r.elapsed(it, quietSince(it, it.cur.Since, r.now))
	return age, age >= time.Duration(r.b.Values.StallAfter) && r.mover(it) == ""
}

// quietSince returns the later of since and the item's newest event stamped
// before until, a tick's start: what a tick logs counts from the next one.
func quietSince(it *entity, since, until time.Time) time.Time {
	for _, ev := range it.evs {
		if ev.At.Before(until) {
			since = later(since, ev.At)
		}
	}
	return since
}

// stallSignals counts the item's stall signals since it last left
// needs_you.
func (r *run) stallSignals(it *entity) int {
	ep := r.ents[it.entry.Epic]
	if ep == nil {
		return 0
	}
	left := lastLeft(it, stateNeedsYou)
	return len(countedSince(ep, 0, func(ev events.Event) bool {
		seq, ok := strings.CutPrefix(ev.Key, stallPrefix+it.id+":")
		n, err := strconv.Atoi(seq)
		return ev.Kind == events.KindRequest && ok && err == nil && n > left
	}))
}

// stallsUsedUp: the item is stalled now, after as many stall re-checks as
// the planner may run for one problem since it last left needs_you. An item
// in a re-check stays there: a retry could not take it back to one.
func (r *run) stallsUsedUp(it *entity) (string, bool) {
	if it.work == nil || it.cur.State == workRechecking || r.stallSignals(it) < r.b.Values.Supervision.PlannerRunsPerProblem {
		return "", false
	}
	if _, stalled := r.stalledFor(it); !stalled {
		return "", false
	}
	return fmt.Sprintf("%s%s:%d", stallsRef, it.id, it.entered), true
}

// mover names who moves the work item next from where it waits, or "" for
// no one. It reads only what this tick read already: a session's listing,
// the merge hold watchPRs kept, the blockers and bases read so far.
func (r *run) mover(it *entity) string {
	ep := r.ents[it.entry.Epic]
	switch {
	case it.cur.State == stateNeedsYou, it.cur.State == stateWaitingUser, ep != nil && ep.cur.State == stateNeedsYou:
		return "you"
	case !r.workAllowed(it):
		return "the account"
	case r.working(r.babysitterOf(it)) || r.babysitterNeeded(it):
		return "its babysitter"
	case ep != nil && r.recheckComing(ep, it):
		return ep.id + "'s re-check"
	}
	switch it.cur.State {
	case workQueued:
		return r.queuedMover(it)
	case workInReview:
		if it.st.MergeHold != "" {
			return "its merge hold: " + it.st.MergeHold
		}
		if it.pr == nil || watched(it) && it.reads == nil {
			return "its PR, not read this reconcile"
		}
	case stateBlocked:
		if r.startWaits(it) {
			return "a gate: start link into it"
		}
		if wait, _ := r.waitsForSet(it); wait {
			return "an item of its set being worked"
		}
	}
	return ""
}

// queuedMover names who moves a queued item: a gate: start link into it;
// its red base; its set, while no orchestrator holds it; an item of its set
// being worked, or an earlier one ready to start first.
func (r *run) queuedMover(it *entity) string {
	switch {
	case r.startWaits(it):
		return "a gate: start link into it"
	case r.baseReadRed(it):
		return fmt.Sprintf("its base %s, red", repoBase{it.work.Repo, it.work.Base})
	}
	if set := r.ents[it.work.Set]; set != nil && (set.cur.State == set.m.Start || set.cur.State == setClaimed) {
		return fmt.Sprintf("its set %s, still %s", set.id, set.cur.State)
	}
	items, _ := r.setItems(it)
	for _, s := range items {
		if s != it && active(s) {
			return s.id + ", being worked"
		}
	}
	for _, s := range items {
		if s == it {
			break
		}
		if s.cur.State == s.m.Start && !r.startWaits(s) && !r.baseReadRed(s) {
			return s.id + ", ready to start first"
		}
	}
	return ""
}

// setClaimed is the set state in which an orchestrator holds its lease but
// is not listed yet.
const setClaimed = "claimed"

// recheckComing reports whether a re-check of the epic will take the item
// or runs: a signal no re-check took covers its issue; for an item blocked
// or in a re-check, also the epic's planner works or is due.
func (r *run) recheckComing(ep, it *entity) bool {
	ref := issueRef(it)
	if slices.ContainsFunc(pendingSignals(ep), func(sig events.Event) bool {
		return slices.ContainsFunc(signalCovers(sig), func(c string) bool { return refCovers(c, ref) })
	}) {
		return true
	}
	if it.cur.State != stateBlocked && it.cur.State != workRechecking {
		return false
	}
	return r.needsSession(ep) || r.working(r.ents[sessionID(ep.id, componentPlanner)])
}

// working reports whether a session is listed working; while the listing is
// unknown, it may be.
func (r *run) working(sess *entity) bool {
	if sess == nil || !sess.open() {
		return false
	}
	if _, known := r.listing(); !known {
		return true
	}
	ok, _, _ := r.listedWorking(sess)
	return ok
}

// startWaits reports whether a gate: start link into the item waits, as far
// as this tick read its blockers: one on GitHub not read yet waits.
func (r *run) startWaits(it *entity) bool {
	ep, p, err := r.itemPlan(it)
	if p == nil || err != nil {
		return err != nil
	}
	ok, err := p.Startable(planID(it), func(l epic.Link) (epic.Release, error) { return r.readRelease(ep, l) })
	return !ok || err != nil
}

// readRelease is release as far as this tick read the blockers: one on
// GitHub not read yet waits.
func (r *run) readRelease(ep *entity, l epic.Link) (epic.Release, error) {
	if ref, err := epic.ParseRef(l.From); err == nil && r.blockerItem(ep, l.From) == nil {
		if _, read := r.blockerReads[blockerKey(ref)]; !read {
			return epic.Waiting, nil
		}
	}
	return r.release(ep, l)
}

// baseReadRed reports whether this tick read the item's base red; a repair
// item starts on it.
func (r *run) baseReadRed(it *entity) bool {
	read, ok := r.bases[repoBase{it.work.Repo, it.work.Base}.String()]
	return ok && read.err == nil && read.res.Red() && !r.isRepair(it)
}

// waitDigestPrefix starts the key of a long-wait digest DM,
// wait-digest:<tick>, logged on the epic of the oldest wait.
const waitDigestPrefix = "wait-digest:"

// digestWaits DMs the user one digest of the open work items that waited in
// their state longer than wait_dm, with what each waits on: once a wait
// crossed wait_dm since the last digest, or wait_dm after it while any
// remain. The last digest is the newest digest done in any epic's log.
func (r *run) digestWaits() {
	limit := time.Duration(r.b.Values.WaitDM)
	var waits []*entity
	for _, id := range r.order() {
		if it := r.ents[id]; it.work != nil && it.open() && r.now.Sub(it.cur.Since) > limit {
			waits = append(waits, it)
		}
	}
	if len(waits) == 0 {
		return
	}
	slices.SortStableFunc(waits, func(a, b *entity) int { return a.cur.Since.Compare(b.cur.Since) })
	last := r.lastDigest()
	if !waits[len(waits)-1].cur.Since.Add(limit).After(last) && r.now.Sub(last) < limit {
		return
	}
	ep := r.ents[waits[0].entry.Epic]
	if ep == nil {
		return
	}
	lines := []string{"factory: long waits:"}
	for _, it := range waits {
		line := fmt.Sprintf(" - %s (%s) in %s for %s, waiting on %s", it.id, issueRef(it), it.cur.State,
			r.now.Sub(it.cur.Since).Round(time.Hour), cmp.Or(r.mover(it), "no one"))
		if from := r.deployedWait(it); from != "" {
			line += fmt.Sprintf("; once %s is deployed, run: factory deployed <its PR URL>", from)
		}
		lines = append(lines, line)
	}
	r.dm(ep, waitDigestPrefix+strconv.Itoa(r.n), strings.Join(lines, "\n"))
}

// lastDigest returns when the newest long-wait digest was sent, ended
// epics included.
func (r *run) lastDigest() time.Time {
	var last time.Time
	for _, id := range r.order() {
		ep := r.ents[id]
		if ep.epic == nil {
			continue
		}
		for _, ev := range ep.evs {
			if ev.Kind == events.KindDone && strings.HasPrefix(ev.Key, waitDigestPrefix) {
				last = later(last, ev.At)
			}
		}
	}
	return last
}

// deployedWait returns the blocker of a when: deployed link the item waits
// on, as far as this tick read the blockers, or "".
func (r *run) deployedWait(it *entity) string {
	ep, p, err := r.itemPlan(it)
	if p == nil || err != nil {
		return ""
	}
	for _, l := range p.LinksTo(planID(it)) {
		if rel, err := r.readRelease(ep, l); l.When == epic.WhenDeployed && rel == epic.Waiting && err == nil {
			return l.From
		}
	}
	return ""
}
