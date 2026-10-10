package reconcile

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

// recipientRecheck marks a re-check signal: a request in an epic's log that
// the tick reads as a reason to re-check, never applies as a move.
const recipientRecheck = "recheck"

// Re-check signals, by the Request of the signal's request event. A
// signal's Text is the issues it covers, space-separated, except for
// replan (its note; it covers every issue) and answer (the issue, then the
// user's choice).
const (
	signalStale   = "stale"   // an explorer's issue_stale
	signalBlocked = "blocked" // an item or set used up its restarts
	signalMerged  = "merged"  // a non-factory PR merged that links an issue or touches a listed file
	signalEnded   = "ended"   // a blocker ended without its release
	signalOverlap = "overlap" // another epic's explore ended with files overlapping an item's
	signalReplan  = "replan"  // factory replan
	signalAnswer  = "answer"  // factory answer to a result that waits on the user
)

// everyIssue, as a covered issue, is every issue of the epic.
const everyIssue = "*"

func signalKey(req events.Event) string {
	if req.Key != "" {
		return req.Key
	}
	return req.Request + ":" + seqRef(req)
}

func signalCovers(req events.Event) []string {
	switch req.Request {
	case signalReplan:
		return []string{everyIssue}
	case signalAnswer:
		ref, _, _ := strings.Cut(req.Text, " ")
		return []string{ref}
	}
	return strings.Fields(req.Text)
}

func consumedKey(req events.Event) string { return "signal:" + signalKey(req) }

// pendingSignals returns the epic's signals no re-check has taken yet,
// oldest first.
func pendingSignals(ep *entity) []events.Event {
	var out []events.Event
	for _, ev := range ep.evs {
		if ev.Kind == events.KindRequest && ev.Recipient == recipientRecheck && !ep.has(consumedKey(ev)) {
			out = append(out, ev)
		}
	}
	return out
}

// recheckSignal: a signal no re-check has taken yet. Its key is the move's
// ref; an escalation goes first, so its ref, the restarts ref of the move
// into blocked, names the planner run it gets (TODO 10 counts them).
func (r *run) recheckSignal(e *entity) (bool, string, error) {
	sigs := pendingSignals(e)
	if len(sigs) == 0 {
		return false, "", nil
	}
	if i := slices.IndexFunc(sigs, func(s events.Event) bool { return s.Request == signalBlocked }); i >= 0 {
		return true, signalKey(sigs[i]), nil
	}
	return true, signalKey(sigs[0]), nil
}

// startRecheck runs as the epic enters checking for a re-check, t being
// that move: it takes every pending signal, recording the issues each
// covers, and tells the planner what to re-check.
func (r *run) startRecheck(ep *entity, t events.Event) error {
	key := "recheck:" + seqRef(t)
	if ep.has(key) {
		return nil
	}
	var refs, lines []string
	for _, sig := range pendingSignals(ep) {
		covers := signalCovers(sig)
		if _, err := r.append(ep, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, TriggerRef: seqRef(t),
			Text: strings.Join(covers, " "), Key: consumedKey(sig)}); err != nil {
			return err
		}
		lines = append(lines, r.describe(ep, sig))
		for _, c := range covers {
			if !slices.Contains(refs, c) {
				refs = append(refs, c)
			}
		}
	}
	issues := strings.Join(refs, ", ")
	if slices.Contains(refs, everyIssue) {
		issues = "every issue of the epic"
	}
	text := fmt.Sprintf("Re-check: %s. Re-check only %s; copy everything else unchanged into the new epic.v<n>.yaml.", strings.Join(lines, "; "), issues)
	r.say("%s: %s", ep.id, text)
	_, err := r.append(ep, events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: key})
	return err
}

// describe says what a signal is, for the planner.
func (r *run) describe(ep *entity, sig events.Event) string {
	covers := strings.Join(signalCovers(sig), ", ")
	parts := strings.Split(signalKey(sig), ":")
	switch sig.Request {
	case signalStale:
		why := ""
		if it := r.ents[parts[1]]; it != nil {
			if ev, ok := eventBySeq(it, parts[2]); ok && ev.Text != "" {
				why = ": " + ev.Text
			}
		}
		return fmt.Sprintf("the explorer of %s says %s is stale%s", parts[1], covers, why)
	case signalBlocked:
		id := r.escalated(ep, signalKey(sig))
		set := id
		if e := r.ents[id]; e != nil && e.work != nil {
			set = e.work.Set
		}
		return fmt.Sprintf("%s used up its restarts (%s); also send %s an instruction", id, covers, set)
	case signalMerged:
		return fmt.Sprintf("%s merged, and it links or touches %s", strings.Join(parts[1:], ":"), covers)
	case signalEnded:
		return fmt.Sprintf("blocker %s ended without its release; it blocks %s", strings.Join(parts[1:], ":"), covers)
	case signalOverlap:
		return fmt.Sprintf("%s, of another epic, will change files that the work on %s changes", parts[1], covers)
	case signalReplan:
		return "factory replan: " + sig.Text
	case signalAnswer:
		ref, choice, _ := strings.Cut(sig.Text, " ")
		return fmt.Sprintf("the user answered %s: %s", ref, choice)
	}
	return sig.Request
}

// covered returns the issues the epic's running re-check covers.
func covered(ep *entity) []string {
	if ep.cur.State != epicChecking || len(ep.cur.Prev) == 0 {
		return nil
	}
	var refs []string
	for _, ev := range ep.since() {
		if ev.Kind == events.KindDone && strings.HasPrefix(ev.Key, "signal:") && ev.TriggerRef == strconv.Itoa(ep.entered) {
			refs = append(refs, strings.Fields(ev.Text)...)
		}
	}
	return refs
}

// covers reports whether the epic's running re-check covers the item's issue.
func (r *run) covers(ep, it *entity) bool {
	ref := issueRef(it)
	return slices.ContainsFunc(covered(ep), func(s string) bool {
		c, err := epic.ParseRef(s)
		return s == everyIssue || err == nil && c.Is(ref)
	})
}

// awaitsAnswer reports whether the item waits on the user after a re-check:
// it went to needs_you from rechecking. A new re-check does not take it
// again; its outcome moves it on.
func awaitsAnswer(it *entity) bool {
	n := len(it.cur.Prev)
	return it.cur.State == workNeedsYou && n > 0 && it.cur.Prev[n-1] == workRechecking
}

// recheckStarted: the epic's running re-check covers the item.
func (r *run) recheckStarted(it *entity) (bool, string, error) {
	ep := r.ents[it.entry.Epic]
	if it.work == nil || ep == nil || ep.epic == nil || awaitsAnswer(it) || !r.covers(ep, it) {
		return false, "", nil
	}
	return true, fmt.Sprintf("recheck:%s:%d", ep.id, ep.entered), nil
}

// recheckOutcome returns what the epic's plan says about an item a re-check
// holds, once that re-check has ended, and the plan's end seq as the ref.
// Until then, ok is false: the item stays held while a new epic.yaml is
// being written.
func (r *run) recheckOutcome(it *entity) (out epic.Outcome, ref string, ok bool, err error) {
	ep := r.ents[it.entry.Epic]
	if ep == nil || ep.epic == nil || it.cur.State != workRechecking && !awaitsAnswer(it) {
		return 0, "", false, nil
	}
	start := recheckStart(it)
	if !slices.ContainsFunc(ep.evs, func(t events.Event) bool {
		return t.Kind == events.KindTransition && t.From == epicChecking && t.Seq > start
	}) {
		return 0, "", false, nil
	}
	p, end, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return 0, "", false, err
	}
	return p.Outcome(planID(it)), "plan:" + seqRef(end), true, nil
}

// recheckStart returns the seq, in its epic's log, of the move into
// checking that began the re-check holding the item.
func recheckStart(it *entity) int {
	for i := len(it.evs) - 1; i >= 0; i-- {
		t := it.evs[i]
		if t.Kind == events.KindTransition && t.To == workRechecking {
			parts := strings.Split(t.TriggerRef, ":")
			n, _ := strconv.Atoi(parts[len(parts)-1])
			return n
		}
	}
	return 0
}

// The guards on a re-check's outcome for an item it held: its work goes
// on, it is dropped, or it waits on the user.
var (
	recheckKept      = recheckIs(epic.Work)
	recheckDropped   = recheckIs(epic.Drop)
	recheckNeedsUser = recheckIs(epic.Hold)
)

// recheckIs returns the guard that holds when the re-check's outcome for
// the item is want.
func recheckIs(want epic.Outcome) guard {
	return func(r *run, e *entity) (bool, string, error) {
		got, ref, ok, err := r.recheckOutcome(e)
		return ok && got == want, ref, err
	}
}

// issueResult is the plan's result for the item's issue.
func issueResult(p *epic.Plan, it *entity) string {
	i, _ := p.Issue(issueRef(it))
	return i.Result
}

// scopeChanged: the item came back to in_review from a re-check that marked
// its issue updated, and its worktree is with the babysitter.
func (r *run) scopeChanged(it *entity) (bool, string, error) {
	t, _ := lastTransition(it.evs)
	if t.From != workRechecking || !handedOver(it) {
		return false, "", nil
	}
	_, p, err := r.itemPlan(it)
	if p == nil || err != nil || issueResult(p, it) != epic.ResultUpdated {
		return false, "", err
	}
	return true, "scope:" + seqRef(t), nil
}

// reread tells the orchestrator to re-read an item's issue after each
// re-check that kept the item with its issue updated.
func (r *run) reread(it *entity) error {
	ep := r.ents[it.entry.Epic]
	if ep == nil {
		return nil
	}
	for _, t := range it.evs {
		seq, ok := strings.CutPrefix(t.TriggerRef, "plan:")
		key := fmt.Sprintf("reread:%s:%d", it.id, t.Seq)
		if t.Kind != events.KindTransition || !ok || slices.Contains([]string{workNeedsYou, workCancelled, workRechecking}, t.To) || r.told(it, key) {
			continue
		}
		end, ok := planEnd(ep, seq)
		if !ok {
			continue
		}
		p, err := loadEnded(end)
		if err != nil {
			return err
		}
		if issueResult(p, it) != epic.ResultUpdated {
			continue
		}
		text := fmt.Sprintf("%s: the planner updated issue %s. Re-read it, then redo every step it invalidates, as new versions.", it.id, issueRef(it))
		if err := r.tell(it, key, text); err != nil {
			return err
		}
	}
	return nil
}

// told reports whether a message with key went to the item's orchestrator.
func (r *run) told(it *entity, key string) bool {
	set := r.ents[it.work.Set]
	return set != nil && set.has(key)
}

// noticeSignals logs, as signals in the epic's log, each reason to re-check
// it that its items, sets, plan and GitHub show. Each has a once-only key.
// GitHub is read only on a full reconcile.
func (r *run) noticeSignals(ep *entity) error {
	if !ep.open() {
		return nil
	}
	items, err := r.itemsOf(ep)
	if err != nil {
		return err
	}
	var sigs []events.Event
	add := func(kind, key string, refs []string) {
		if len(refs) > 0 && !ep.has(key) && !slices.ContainsFunc(sigs, func(s events.Event) bool { return s.Key == key }) {
			sigs = append(sigs, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientRecheck,
				Request: kind, Text: strings.Join(refs, " "), Key: key})
		}
	}
	for _, it := range items {
		ref := []string{issueRef(it).String()}
		for _, ev := range it.evs {
			switch {
			case ev.Kind == events.KindIssueStale:
				add(signalStale, fmt.Sprintf("stale:%s:%d", it.id, ev.Seq), ref)
			case blockedPastRestarts(ev):
				add(signalBlocked, ev.TriggerRef, ref)
			}
		}
	}
	for _, set := range r.setsOf(ep) {
		for _, ev := range set.evs {
			if blockedPastRestarts(ev) {
				add(signalBlocked, ev.TriggerRef, r.issuesOf(set))
			}
		}
	}
	p, _, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return err
	}
	if err := r.noticeEnded(ep, p, add); err != nil {
		return err
	}
	if err := r.noticeOverlap(ep, items, add); err != nil {
		return err
	}
	if r.full {
		if err := r.noticeMerges(ep, p, items, add); err != nil {
			return err
		}
	}
	for _, sig := range sigs {
		r.say("%s: re-check signal %s", ep.id, sig.Key)
		if _, err := r.append(ep, sig); err != nil {
			return err
		}
	}
	return nil
}

// blockedPastRestarts reports an escalation: a move into blocked because
// the entity used up its restarts. Its ref is the signal's key.
func blockedPastRestarts(ev events.Event) bool {
	return ev.Kind == events.KindTransition && ev.To == workBlocked && strings.HasPrefix(ev.TriggerRef, restartsRef)
}

// escalated returns the item or set of the epic whose move into blocked
// has ref.
func (r *run) escalated(ep *entity, ref string) string {
	items, _ := r.itemsOf(ep)
	for _, e := range append(items, r.setsOf(ep)...) {
		if slices.ContainsFunc(e.evs, func(ev events.Event) bool { return blockedPastRestarts(ev) && ev.TriggerRef == ref }) {
			return e.id
		}
	}
	return ""
}

// issuesOf returns the issues of a set's items.
func (r *run) issuesOf(set *entity) []string {
	items, _ := r.itemsOf(set)
	var refs []string
	for _, it := range items {
		refs = append(refs, issueRef(it).String())
	}
	return refs
}

type addSignal func(kind, key string, refs []string)

// noticeEnded signals each blocker of the plan that ended without its
// release. A blocker on GitHub is read only on a full reconcile.
func (r *run) noticeEnded(ep *entity, p *epic.Plan, add addSignal) error {
	for _, l := range p.Links {
		key := "ended:" + l.From
		if ep.has(key) || !r.full && r.blockerItem(ep, l.From) == nil {
			continue
		}
		rel, err := r.release(ep, l)
		if err != nil {
			return err
		}
		if rel != epic.Ended {
			continue
		}
		var refs []string
		for _, id := range p.Dependents(l.From) {
			if it, ok := p.Item(id); ok {
				refs = append(refs, it.Issue)
			}
		}
		add(signalEnded, key, refs)
	}
	return nil
}

// noticeOverlap signals each item of another epic, in the same repo, whose
// explore output ended after one of this epic's items' and lists a file it
// lists too: the epic whose explore came first re-checks.
func (r *run) noticeOverlap(ep *entity, items []*entity, add addSignal) error {
	for _, x := range items {
		xEnd, xFiles, err := explored(x)
		if err != nil || xFiles == nil || !x.open() {
			if err != nil {
				return err
			}
			continue
		}
		for _, id := range r.order() {
			y := r.ents[id]
			if y.work == nil || y.entry.Epic == ep.id || !y.open() || !strings.EqualFold(y.work.Repo, x.work.Repo) {
				continue
			}
			yEnd, yFiles, err := explored(y)
			if err != nil {
				return err
			}
			if yFiles != nil && yEnd.At.After(xEnd.At) && epic.Overlap(xFiles, yFiles) {
				add(signalOverlap, fmt.Sprintf("overlap:%s:%d", y.id, yEnd.Seq), []string{issueRef(x).String()})
			}
		}
	}
	return nil
}

// explored returns an item's newest ended explore output and the files it
// lists; files is nil when it has none yet.
func explored(it *entity) (events.Event, []string, error) {
	end, ok := events.NewestEnd(it.evs, exploreStep)
	if !ok {
		return end, nil, nil
	}
	files, err := epic.ExploreFiles(end.File)
	if err != nil {
		return end, nil, err
	}
	return end, append([]string{}, files...), nil
}

// noticeMerges signals each pull request merged since the epic was added,
// in a repo of its items, from a branch that is not the factory's, that
// links one of its issues or touches a file one of its items' explore
// output lists.
func (r *run) noticeMerges(ep *entity, p *epic.Plan, items []*entity, add addSignal) error {
	var repos []string
	for _, it := range items {
		if it.open() && !slices.ContainsFunc(repos, func(s string) bool { return strings.EqualFold(s, it.work.Repo) }) {
			repos = append(repos, it.work.Repo)
		}
	}
	for _, repo := range repos {
		ctx, cancel := r.call()
		prs, err := r.d.GitHub.MergedPRs(ctx, repo, ep.epic.AddedAt)
		cancel()
		if err != nil {
			return err
		}
		for _, pr := range prs {
			if strings.HasPrefix(pr.HeadRefName, "factory/") {
				continue
			}
			refs, err := mergedCovers(p, items, repo, pr)
			if err != nil {
				return err
			}
			add(signalMerged, fmt.Sprintf("merged:%s#%d", strings.ToLower(repo), pr.Number), refs)
		}
	}
	return nil
}

// mergedCovers returns the epic's issues a merged PR links, and those of
// the open items in its repo whose explore output lists a file it changed.
func mergedCovers(p *epic.Plan, items []*entity, repo string, pr gh.MergedPR) ([]string, error) {
	var refs []string
	add := func(ref epic.Ref) {
		if s := ref.String(); !slices.Contains(refs, s) {
			refs = append(refs, s)
		}
	}
	for _, c := range pr.Closes {
		if ref, err := epic.ParseRef(c.URL); err == nil {
			if _, ok := p.Issue(ref); ok {
				add(ref)
			}
		}
	}
	var changed []string
	for _, f := range pr.Files {
		changed = append(changed, f.Path)
	}
	for _, it := range items {
		if !it.open() || !strings.EqualFold(it.work.Repo, repo) {
			continue
		}
		_, files, err := explored(it)
		if err != nil {
			return nil, err
		}
		if files != nil && epic.Overlap(files, changed) {
			add(issueRef(it))
		}
	}
	return refs, nil
}

// AnswerTo logs `factory answer <epic> <issue> "<choice>"`. The answer goes
// to the work on that issue when it asked the user a question; otherwise
// the issue's result must wait on the user and the choice be one of its
// answers, and the epic re-checks that issue with it. It returns what it did.
func AnswerTo(ctx context.Context, d Deps, epicID, issue, choice string) (string, error) {
	r, err := readRun(ctx, d)
	if err != nil {
		return "", err
	}
	ep := r.ents[epicID]
	if ep == nil || ep.epic == nil {
		return "", fmt.Errorf("%s is not an epic", epicID)
	}
	if !ep.open() {
		return "", fmt.Errorf("%s is %s: it takes no answers", epicID, ep.cur.State)
	}
	p, _, err := r.appliedPlan(ep)
	if err != nil {
		return "", err
	}
	items, err := r.itemsOf(ep)
	if err != nil {
		return "", err
	}
	ref, err := resolveIssue(p, items, issue)
	if err != nil {
		return "", err
	}
	for _, it := range items {
		if issueRef(it).Is(ref) && it.cur.State == stateWaitingUser {
			req := Request(RequestAnswer, blueprint.Previous, blueprint.TriggerUser, choice)
			req.Sender = "user"
			if _, err := events.Append(store.EventsPath(it.entry.Dir), req); err != nil {
				return "", err
			}
			return fmt.Sprintf("answered the question %s asked", it.id), nil
		}
	}
	if p == nil {
		return "", fmt.Errorf("%s has no plan yet, and no work on %s asked a question", epicID, ref)
	}
	i, ok := p.Issue(ref)
	switch {
	case !ok:
		return "", fmt.Errorf("%s is not an issue of %s", ref, epicID)
	case !i.NeedsUser():
		return "", fmt.Errorf("%s waits on no answer: its result is %s", ref, i.Result)
	}
	k := slices.IndexFunc(i.Answers, func(a string) bool { return strings.EqualFold(a, strings.TrimSpace(choice)) })
	if k < 0 {
		return "", fmt.Errorf("%q is not an answer for %s: want one of %s", choice, ref, strings.Join(i.Answers, " | "))
	}
	req := events.Event{Kind: events.KindRequest, Sender: "user", Recipient: recipientRecheck, Request: signalAnswer,
		Text: ref.String() + " " + i.Answers[k]}
	if _, err := events.Append(store.EventsPath(ep.entry.Dir), req); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s re-checks %s with your answer: %s", epicID, ref, i.Answers[k]), nil
}

// resolveIssue reads the issue argument of factory answer: owner/repo#n, a
// URL, or a bare number that one issue of the epic has.
func resolveIssue(p *epic.Plan, items []*entity, s string) (epic.Ref, error) {
	if ref, err := epic.ParseRef(s); err == nil {
		return ref, nil
	}
	n, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(s), "#"))
	if err != nil {
		return epic.Ref{}, fmt.Errorf("issue %q: want owner/repo#n, its URL or its number", s)
	}
	var found []epic.Ref
	have := func(ref epic.Ref) {
		if ref.Number == n && !slices.ContainsFunc(found, ref.Is) {
			found = append(found, ref)
		}
	}
	for _, it := range items {
		have(issueRef(it))
	}
	if p != nil {
		for _, i := range p.Issues {
			if ref, err := epic.ParseRef(i.Ref); err == nil {
				have(ref)
			}
		}
	}
	if len(found) != 1 {
		return epic.Ref{}, fmt.Errorf("issue #%d: %d issues of the epic have that number; name it as owner/repo#%d", n, len(found), n)
	}
	return found[0], nil
}

// Replan logs `factory replan <epic> "<note>"`: a signal to re-check every
// issue of the epic, with the note for the planner.
func Replan(ctx context.Context, d Deps, epicID, note string) (events.Event, error) {
	r, err := readRun(ctx, d)
	if err != nil {
		return events.Event{}, err
	}
	ep := r.ents[epicID]
	switch {
	case ep == nil || ep.epic == nil:
		return events.Event{}, fmt.Errorf("%s is not an epic", epicID)
	case !ep.open():
		return events.Event{}, fmt.Errorf("%s is %s: there is nothing to re-plan", epicID, ep.cur.State)
	case strings.TrimSpace(note) == "":
		return events.Event{}, fmt.Errorf("the note is empty: say what changed")
	}
	return events.Append(store.EventsPath(ep.entry.Dir), events.Event{Kind: events.KindRequest, Sender: "user", Recipient: recipientRecheck,
		Request: signalReplan, Text: note})
}
