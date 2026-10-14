package reconcile

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

var (
	redCI   = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	greenCI = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
)

// redEscalated puts item e1-w1 in needs_you the way the tick does: its head
// head1 went red, a babysitter pass ended, and the checks stayed red. Its PR
// is a draft, so nothing merges it once it is back in review.
func redEscalated(t *testing.T) *world {
	t.Helper()
	w := watching(t)
	w.pr(func(pr *gh.PR) { pr.IsDraft = true })
	w.listen(w.babysitter(1, sessionTurnEnded, 5400, listedTurnEnded).PID)
	w.pulls.checks[head1] = redCI
	w.tick(false)
	w.end(item, "babysit.v1.md", "## Outcome\nopen — CI red\n## Comments\nnone\n## CI\nci fails: not fixable here\n")
	w.now = w.now.Add(5 * time.Minute)
	w.fullNext()
	w.tick(false)
	if got := last(w.kinds(item, events.KindTransition)); got.To != stateNeedsYou || got.TriggerRef != redAfterPassRef+head1 {
		t.Fatalf("%s moved to %s on %q; want needs_you on red-after-pass", item, got.To, got.TriggerRef)
	}
	return w
}

// toNeedsYou logs a move of id from `from` into needs_you on ref, as if the
// tick made it.
func (w *world) toNeedsYou(id, from, ref string) {
	w.t.Helper()
	w.append(w.dir(id), events.Event{Kind: events.KindTransition, At: w.now, From: from, To: stateNeedsYou, Prev: []string{from},
		Trigger: "tick", TriggerRef: ref})
}

// releases returns the program's release requests in id's log.
func (w *world) releases(id string) []events.Event {
	return slices.DeleteFunc(w.kinds(id, events.KindRequest), func(ev events.Event) bool { return !strings.HasPrefix(ev.Key, autoReleasePrefix) })
}

// checksRead returns the commit of each read of a commit's checks so far.
func (w *world) checksRead() []string {
	var out []string
	for _, c := range w.fake.Calls() {
		if c.Name == "gh" && strings.Contains(strings.Join(c.Args, " "), "object(oid:$sha)") {
			out = append(out, field(c.Args, "sha="))
		}
	}
	return out
}

// fullTick advances the clock by d and runs a full reconcile; it returns the
// commits whose checks that tick read.
func (w *world) fullTick(d time.Duration) []string {
	w.t.Helper()
	before := len(w.checksRead())
	w.now = w.now.Add(d)
	w.fullNext()
	w.tick(false)
	return w.checksRead()[before:]
}

// anuma-marketing-site#412: a cancelled run left its head red, a pass ended,
// and it went to needs_you; then the run was re-run green.
func TestAnEscalationWhoseChecksTurnedGreenIsReleased(t *testing.T) {
	w := redEscalated(t)
	w.pulls.checks[head1] = rerunGreen
	w.fullTick(5 * time.Minute)
	const why = "no required check is red on 1111111 now"
	if got := w.releases(item); len(got) != 1 || got[0].Text != why || got[0].Sender != events.SenderProgram {
		t.Fatalf("releases = %+v, want one: %s", got, why)
	}
	if !strings.Contains(w.out.String(), item+": cause gone: "+why) {
		t.Errorf("the tick's report does not name the cause:\n%s", w.out.String())
	}
	rep, err := Status(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Waiting) != 1 || rep.Waiting[0].Why != "released automatically: "+why {
		t.Errorf("waiting on you = %+v", rep.Waiting)
	}
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := w.status(item).State; got != workInReview {
		t.Fatalf("%s is %s after the release, want in_review", item, got)
	}
	if got := w.dmsWith("e1-w1 released: " + why); len(got) != 1 {
		t.Errorf("DMs = %q, want one release DM", w.dms)
	}
	w.noErrors()
}

func TestARedAfterPassIsReleasedOnlyOnceItsCauseIsGone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*world)
		why    string
		reads  []string
	}{
		{name: "the same head, still red", change: func(*world) {}, reads: []string{head1}},
		{name: "a new head", change: func(w *world) {
			w.pr(func(pr *gh.PR) { pr.HeadRefOid = head2 })
			w.pulls.checks[head2] = redCI
		}, why: "its head moved from 1111111 to 2222222"},
		{name: "red on the base too", change: func(w *world) { w.red["o/r@main"] = true },
			why: "its red checks (ci) are red on the base too", reads: []string{head1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := redEscalated(t)
			tc.change(w)
			reads := w.fullTick(5 * time.Minute)
			if !slices.Equal(reads, tc.reads) {
				t.Errorf("checks read on %q, want %q", reads, tc.reads)
			}
			got := w.releases(item)
			if tc.why == "" {
				if len(got) != 0 {
					t.Errorf("released: %+v", got)
				}
				return
			}
			if len(got) != 1 || !strings.HasPrefix(got[0].Text, tc.why) {
				t.Errorf("releases = %+v, want one: %s", got, tc.why)
			}
		})
	}
}

// prOpenTimedOut puts item e1-w1 in needs_you because pr_open timed out,
// with its PR 5 open on GitHub and its worktree not handed over yet.
func prOpenTimedOut(t *testing.T) *world {
	t.Helper()
	w := newWorld(t)
	w.registryMerging(true)
	w.planned("e1", oneItem("[]"))
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree(item)
	w.prs[git.Branch(item)] = gh.PR{ID: "PR_5", Number: 5, URL: prURL, State: "OPEN", HeadRefName: git.Branch(item), HeadRefOid: head1,
		BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE", IsDraft: true, CreatedAt: w.now}
	w.pulls.checks[head1] = greenCI
	w.moveTo(item, workPROpen)
	w.toNeedsYou(item, workPROpen, timeoutRef+"3")
	return w
}

func TestAPROpenTimeoutIsReleasedOnceThePRIsRecordedAndHandedOver(t *testing.T) {
	w := prOpenTimedOut(t)
	if reads := w.fullTick(time.Minute); len(w.releases(item)) != 0 || len(reads) != 0 {
		t.Fatalf("released %+v with no handover (checks read on %q)", w.releases(item), reads)
	}
	w.append(w.dir(item), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture"})
	reads := w.fullTick(5 * time.Minute)
	const why = "its PR " + prURL + " is recorded and handed to its babysitter"
	if got := w.releases(item); len(got) != 1 || got[0].Text != why || len(reads) != 0 {
		t.Fatalf("releases = %+v, checks read on %q; want one, %s, and no read", got, reads, why)
	}
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	w.fullTick(5 * time.Minute)
	if got := w.moves(item); !slices.Equal(got[len(got)-2:], []string{"needs_you→pr_open", "pr_open→in_review"}) {
		t.Errorf("moves = %v, want back to pr_open, then in_review", got)
	}
}

func TestEscalationsTheProgramCannotReTestStay(t *testing.T) {
	for _, tc := range []struct{ name, from, ref string }{
		{name: "closed", from: workClosed, ref: "tick:3"},
		{name: "dead-letter", from: workInReview, ref: deadLetterRef + "4"},
		{name: "planner runs", from: workInReview, ref: plannerRunsRef + "e1-w1:6@2026-10-09T12:00:00Z"},
		{name: "denied model", from: workInReview, ref: deniedModelRef + "4"},
		{name: "a timeout out of another state", from: workInReview, ref: timeoutRef + "3"},
		{name: "waiting_user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := watching(t)
			w.pr(func(pr *gh.PR) { pr.IsDraft = true })
			w.tick(false)
			if tc.from == "" {
				w.moveTo(item, stateWaitingUser)
			} else {
				w.toNeedsYou(item, tc.from, tc.ref)
			}
			reads := w.fullTick(5 * time.Minute)
			if got := w.releases(item); len(got) != 0 || len(reads) != 0 {
				t.Errorf("releases %+v, checks read on %q; want neither", got, reads)
			}
		})
	}
}

func TestAnEscalationThatKeepsComingBackStaysTheThirdTime(t *testing.T) {
	w := redEscalated(t)
	w.pulls.checks[head1] = greenCI
	var reads []string
	for n := 1; n <= 3; n++ {
		if n > 1 {
			if got := w.status(item).State; got != workInReview {
				t.Fatalf("release %d: %s is %s, want in_review", n-1, item, got)
			}
			w.toNeedsYou(item, workInReview, redAfterPassRef+head1)
		}
		reads = w.fullTick(5 * time.Minute)
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if got := w.releases(item); len(got) != releasesPerDay {
		t.Errorf("%d releases in 24h, want %d", len(got), releasesPerDay)
	}
	if got := w.status(item).State; got != stateNeedsYou {
		t.Errorf("%s is %s after the third escalation, want needs_you", item, got)
	}
	if len(reads) != 0 {
		t.Errorf("checks read on %q for an item at the cap", reads)
	}
	if got := w.dmsWith("released automatically 2 times in 24h and the cause came back"); len(got) != 1 {
		t.Errorf("DMs = %q, want one that says the cause keeps coming back", w.dms)
	}
}

// manyItems is a plan with n items, w1…wn, in one set.
func manyItems(n int) string {
	var issues, items, ids []string
	for i := 1; i <= n; i++ {
		issues = append(issues, fmt.Sprintf("  - {ref: o/r#%d, result: ready}", 20+i))
		items = append(items, fmt.Sprintf("  - {id: w%d, repo: r, issue: o/r#%d}", i, 20+i))
		ids = append(ids, fmt.Sprintf("w%d", i))
	}
	return "uat: none\nissues:\n" + strings.Join(issues, "\n") + "\nitems:\n" + strings.Join(items, "\n") +
		"\nsets:\n  - {id: s1, items: [" + strings.Join(ids, ", ") + "]}\nlinks: []\n"
}

func TestAFullReconcileReadsTheLeastRecentlyReadEscalationsUpToStallReads(t *testing.T) {
	w := newWorld(t)
	w.registryMerging(true)
	w.planned("e1", manyItems(7))
	head := map[string]string{}
	for i := 1; i <= 7; i++ {
		id, sha, n := fmt.Sprintf("e1-w%d", i), fmt.Sprintf("%040d", i), 100+i
		head[sha] = id
		url := fmt.Sprintf("https://github.com/o/r/pull/%d", n)
		w.prs[git.Branch(id)] = gh.PR{ID: fmt.Sprintf("PR_%d", n), Number: n, URL: url, State: "OPEN", HeadRefName: git.Branch(id), HeadRefOid: sha,
			BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE"}
		st := w.status(id)
		st.PR, st.PRURL, st.PRState, st.HeadSHA = n, url, "OPEN", sha
		if err := store.WriteStatus(w.dir(id), st); err != nil {
			t.Fatal(err)
		}
		w.pulls.checks[sha] = redCI
		w.toNeedsYou(id, workInReview, redAfterPassRef+sha)
	}
	ids := func(reads []string) []string {
		var out []string
		for _, sha := range reads {
			out = append(out, head[sha])
		}
		slices.Sort(out)
		return out
	}
	if got := ids(w.fullTick(time.Minute)); !slices.Equal(got, []string{"e1-w1", "e1-w2", "e1-w3", "e1-w4", "e1-w5"}) {
		t.Fatalf("first full reconcile read %q", got)
	}
	if got := ids(w.fullTick(5 * time.Minute)); !slices.Equal(got, []string{"e1-w1", "e1-w2", "e1-w3", "e1-w6", "e1-w7"}) {
		t.Errorf("second full reconcile read %q, want e1-w6 and e1-w7 first", got)
	}
	for i := 1; i <= 7; i++ {
		if got := w.releases(fmt.Sprintf("e1-w%d", i)); len(got) != 0 {
			t.Errorf("released a red head: %+v", got)
		}
	}
}

// stuckInReview puts item e1-w1 in in_review with no one to move it: its PR
// is a draft, so nothing merges it, and its worktree came back from its
// babysitter, so nothing watches the PR. One full reconcile has run.
func stuckInReview(t *testing.T) *world {
	t.Helper()
	w := watching(t)
	w.pr(func(pr *gh.PR) { pr.IsDraft = true })
	w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindHandback, Sender: events.SenderProgram, Text: "fixture"})
	w.tick(false)
	return w
}

// stallSignals returns the stall signals in epic e1's log.
func (w *world) stallSignals() []events.Event {
	return slices.DeleteFunc(w.kinds("e1", events.KindRequest), func(ev events.Event) bool { return ev.Request != signalStall })
}

// stallKey is the key of the stall signal for id in its current state.
func (w *world) stallKey(id string) string {
	return fmt.Sprintf("%s%s:%d", stallPrefix, id, last(w.kinds(id, events.KindTransition)).Seq)
}

// ackAll acks every message in id's inbox, as its session does once it
// read them.
func (w *world) ackAll(id string) {
	w.t.Helper()
	for _, m := range events.Inbox(w.log(id)) {
		if err := events.Ack(store.EventsPath(w.dir(id)), m.Seq, id); err != nil {
			w.t.Fatal(err)
		}
	}
}

// recheckV ends epic e1's running re-check with plan as epic.v<v>.yaml, after
// its planner read what it was sent.
func (w *world) recheckV(v int, plan string) {
	w.t.Helper()
	w.ackAll("e1")
	w.end("e1", fmt.Sprintf("state-check.v%d.md", v), stateCheck)
	w.end("e1", fmt.Sprintf("epic.v%d.yaml", v), plan)
}

func TestAnItemNoOneMovesIsStalledAfterStallAfter(t *testing.T) {
	w := stuckInReview(t)
	w.fullTick(29 * time.Minute)
	if got := w.stallSignals(); len(got) != 0 || len(w.dmsWith("stalled")) != 0 {
		t.Fatalf("at 29m: stall signals %+v, DMs %q; want none", got, w.dms)
	}
	key := w.stallKey(item)
	w.fullTick(2 * time.Minute)
	got := w.stallSignals()
	if len(got) != 1 || got[0].Key != key || got[0].Recipient != recipientRecheck || !slices.Equal(signalCovers(got[0]), []string{"o/r#14"}) {
		t.Fatalf("stall signals = %+v, want one under %s covering o/r#14", got, key)
	}
	const dm = "factory: e1-w1 (o/r#14) stalled in in_review: no one has moved it for 31m0s. e1 re-checks it."
	if got := w.dmsWith("stalled"); !slices.Equal(got, []string{dm}) {
		t.Errorf("DMs = %q, want %q", got, dm)
	}
	w.fullTick(time.Minute)
	if got := w.status(item).State; got != workRechecking {
		t.Errorf("%s is %s after the stall signal, want rechecking", item, got)
	}
	if inbox := strings.Join(w.inbox("e1"), "\n"); !strings.Contains(inbox, "e1-w1 stalled in in_review: no one moved it for 31m0s") {
		t.Errorf("planner inbox %q does not say what stalled", inbox)
	}
	if got := w.stallSignals(); len(got) != 1 || len(w.dmsWith("stalled")) != 1 {
		t.Errorf("stall signals %+v, DMs %q; want one of each", got, w.dms)
	}
	w.noErrors()
}

func TestABlockedItemWhoseRecheckEndedWithNothingIsStalled(t *testing.T) {
	w := newWorld(t)
	w.blockW1(twoItems("[]"), workImplementing)
	w.recheckAs(twoItems("[]"))
	w.ackAll("e1")
	w.ticks(2)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Fatalf("e1-w1 is %s, want blocked; moves %v", got, w.moves("e1-w1"))
	}
	planner := w.listedSession("factory:e1:planner")
	w.setState(planner.Name, planner.PID, listedTurnEnded)
	w.fullTick(28 * time.Minute)
	if got := w.stallSignals(); len(got) != 0 {
		t.Fatalf("at 28m: stall signals %+v, want none", got)
	}
	key := w.stallKey("e1-w1")
	w.fullTick(2 * time.Minute)
	if got := w.stallSignals(); len(got) != 1 || got[0].Key != key {
		t.Fatalf("stall signals = %+v, want one under %s", got, key)
	}
}

func TestOnlyQueuedInReviewBlockedAndRecheckingWorkIsJudged(t *testing.T) {
	for _, state := range []string{workImplementing, workPROpen, workMerging, stateWaitingUser, stateNeedsYou} {
		t.Run("work "+state, func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.planned("e1", oneItem("[]"))
			w.moveTo("e1-s1", setRunning)
			w.moveTo(item, state)
			w.fullTick(45 * time.Minute)
			if got := w.stallSignals(); len(got) != 0 || len(w.dmsWith("stalled")) != 0 {
				t.Errorf("stall signals %+v, DMs %q; want none", got, w.dms)
			}
		})
	}
	w := newWorld(t)
	for i, state := range []string{epicPlanned, "running", "merging", "uat"} {
		ep := fmt.Sprintf("e%d", i+1)
		w.epic(ep, state)
		w.set(ep, ep+"-s1", map[bool]string{true: "queued", false: stateBlocked}[i%2 == 0])
	}
	w.fullTick(45 * time.Minute)
	for i := range 4 {
		if got := w.kinds(fmt.Sprintf("e%d", i+1), events.KindRequest); len(got) != 0 {
			t.Errorf("e%d has signals %+v, want none", i+1, got)
		}
	}
	if got := w.dmsWith("stalled"); len(got) != 0 {
		t.Errorf("DMs = %q, want none", got)
	}
}

func TestAQueuedItemBehindAnItemBeingWorkedIsNotStalled(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", sameSet)
	w.moveTo("e1-s1", setRunning)
	w.moveTo("e1-w1", workImplementing)
	w.fullTick(31 * time.Minute)
	if got := w.status("e1-w2").State; got != workQueued {
		t.Fatalf("e1-w2 is %s, want queued", got)
	}
	if got := w.stallSignals(); len(got) != 0 {
		t.Errorf("stall signals %+v, want none", got)
	}
}

// moverRun reads the world as a full reconcile's run, its account ok, then
// changes the run with adjust.
func moverRun(t *testing.T, w *world, adjust func(*run)) *run {
	t.Helper()
	r, err := readRun(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	r.useAccount(stateOK, nil)
	r.full = true
	if adjust != nil {
		adjust(r)
	}
	return r
}

func TestEachMoverKeepsAnItemOut(t *testing.T) {
	readPR := func(r *run) { pr := gh.PR{Number: 5, State: "OPEN"}; r.ents[item].pr = &pr }
	setStatus := func(w *world, id string, change func(*store.Status)) {
		st := w.status(id)
		change(&st)
		if err := store.WriteStatus(w.dir(id), st); err != nil {
			t.Fatal(err)
		}
	}
	queued := func(plan string) func(*world) {
		return func(w *world) {
			w.registry()
			w.planned("e1", plan)
			w.moveTo("e1-s1", setRunning)
		}
	}
	for _, tc := range []struct {
		name, id string
		world    func(*world)
		adjust   func(*run)
		want     string
	}{
		{name: "no one", world: func(w *world) {
			w.inReviewWith(oneItem("[]"), time.Minute)
			w.append(w.dir(item), events.Event{Kind: events.KindHandback})
		},
			adjust: readPR, want: ""},
		{name: "its PR not read", world: func(w *world) {
			w.inReviewWith(oneItem("[]"), time.Minute)
			w.append(w.dir(item), events.Event{Kind: events.KindHandback})
		}, want: "its PR, not read this reconcile"},
		{name: "a working babysitter", world: func(w *world) {
			w.inReviewWith(oneItem("[]"), time.Minute)
			w.babysitter(1, sessionRunning, 5300, listedWorking)
		}, adjust: readPR, want: "its babysitter"},
		{name: "a babysitter trigger", world: func(w *world) {
			w.inReviewWith(oneItem("[]"), time.Minute)
			w.append(w.dir(item), events.Event{Kind: events.KindRequest, Recipient: recipientBabysitter, Request: "comment", Text: "a new comment"})
		}, adjust: readPR, want: "its babysitter"},
		{name: "a merge hold", world: func(w *world) {
			w.inReviewWith(oneItem("[]"), time.Minute)
			w.append(w.dir(item), events.Event{Kind: events.KindHandback})
			setStatus(w, item, func(st *store.Status) { st.MergeHold = "it is not approved" })
		}, adjust: readPR, want: "its merge hold: it is not approved"},
		{name: "a waiting link", world: queued(oneItem("\n  - {from: o/r#99, to: w1, gate: start, when: merged, rollback: none}")),
			want: "a gate: start link into it"},
		{name: "a red base", world: queued(oneItem("[]")), adjust: func(r *run) {
			r.bases["o/r@main"] = baseRead{res: gh.BaseResult{Checks: []gh.BaseCheck{{Name: "ci", Red: true}}}}
		}, want: "its base o/r@main, red"},
		{name: "the account", world: queued(oneItem("[]")), adjust: func(r *run) { r.useAccount(stateNearLimit, nil) }, want: "the account"},
		{name: "its set not claimed", world: func(w *world) { w.registry(); w.planned("e1", oneItem("[]")) }, want: "its set e1-s1, still queued"},
		{name: "an item of its set being worked", id: "e1-w2", world: func(w *world) {
			queued(sameSet)(w)
			w.moveTo("e1-w1", workImplementing)
		}, want: "e1-w1, being worked"},
		{name: "an earlier item of its set ready", id: "e1-w2", world: func(w *world) {
			queued(sameSet)(w)
			w.moveTo("e1-w2", workQueued)
		}, want: "e1-w1, ready to start first"},
		{name: "you", world: func(w *world) {
			queued(oneItem("[]"))(w)
			w.toNeedsYou(item, workInReview, deadLetterRef+"4")
		}, want: "you"},
		{name: "a pending re-check", world: func(w *world) {
			queued(oneItem("[]"))(w)
			w.moveTo(item, stateBlocked)
			w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientRecheck, Request: signalBlocker, Text: "o/r#14", Key: "blocker:e1-w1:9"})
		}, want: "e1's re-check"},
		{name: "a working planner", world: func(w *world) {
			queued(oneItem("[]"))(w)
			w.moveTo(item, workRechecking)
			w.planner("e1", sessionRunning, 5400, listedWorking)
		}, want: "e1's re-check"},
		{name: "a planner whose turn ended", world: func(w *world) {
			queued(oneItem("[]"))(w)
			w.moveTo(item, workRechecking)
			w.planner("e1", sessionTurnEnded, 5400, listedTurnEnded)
		}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			tc.world(w)
			id := cmp.Or(tc.id, item)
			r := moverRun(t, w, tc.adjust)
			if got := r.mover(r.ents[id]); got != tc.want {
				t.Errorf("mover of %s (%s) = %q, want %q", id, r.ents[id].cur.State, got, tc.want)
			}
		})
	}
}

func TestFindingStallsCallsNothing(t *testing.T) {
	w := stuckInReview(t)
	w.planned("e2", oneItem("\n  - {from: o/r#99, to: w1, gate: start, when: merged, rollback: none}"))
	w.moveTo("e2-s1", setRunning)
	w.now = w.now.Add(31 * time.Minute)
	r := moverRun(t, w, func(r *run) {
		r.listing()
		pr := gh.PR{Number: 5, State: "OPEN"}
		r.ents[item].pr = &pr
	})
	before := len(w.fake.Calls())
	r.findStalls()
	if got := w.calls()[before:]; len(got) != 0 {
		t.Errorf("finding stalls called %q", got)
	}
	if out := w.out.String(); !strings.Contains(out, "e1-w1: stalled in in_review") || strings.Contains(out, "e2-w1: stalled") {
		t.Errorf("want e1-w1 stalled and e2-w1 waiting on its link:\n%s", out)
	}
}

func TestStallsAreCappedThenTheItemWaitsForYou(t *testing.T) {
	w := stuckInReview(t)
	for n := 1; n <= 3; n++ {
		w.fullTick(31 * time.Minute)
		if got := w.stallSignals(); len(got) != n {
			t.Fatalf("stall %d: %d stall signals, want %d", n, len(got), n)
		}
		w.fullTick(time.Minute)
		if got := w.status(item).State; got != workRechecking {
			t.Fatalf("stall %d: %s is %s, want rechecking", n, item, got)
		}
		if n == 1 {
			w.fullTick(31 * time.Minute)
			if got := w.status(item).State; got != workRechecking || len(w.stallSignals()) != 1 {
				t.Fatalf("a re-check its planner works on: %s is %s with %d stall signals", item, got, len(w.stallSignals()))
			}
		}
		w.recheckV(n+1, oneItem("[]"))
		w.fullTick(time.Minute)
		if got := w.status(item).State; got != workInReview {
			t.Fatalf("stall %d: %s is %s after its re-check, want in_review", n, item, got)
		}
	}
	w.fullTick(29 * time.Minute)
	if got := w.status(item).State; got != workInReview {
		t.Fatalf("%s is %s 29m after its third re-check, want in_review", item, got)
	}
	w.fullTick(2 * time.Minute)
	moved := last(w.kinds(item, events.KindTransition))
	if moved.To != stateNeedsYou || !strings.HasPrefix(moved.TriggerRef, stallsRef+item+":") {
		t.Fatalf("%s moved to %s on %q; want needs_you on stalls:", item, moved.To, moved.TriggerRef)
	}
	const why = "no one moved it in in_review for 30m0s after 3 stall re-checks"
	if got := w.dmsWith(why); len(got) != 1 {
		t.Errorf("DMs = %q, want one that says %q", w.dms, why)
	}
	w.fullTick(31 * time.Minute)
	if got := w.stallSignals(); len(got) != 3 || len(w.dmsWith(why)) != 1 {
		t.Errorf("%d stall signals and DMs %q after needs_you; want 3 and one", len(got), w.dms)
	}
}

// A capped item in a re-check that ended without moving it gets no more
// stall signals, and stays: from needs_you, a retry could not take it back.
func TestAStalledItemInARecheckStaysThereAtTheCap(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	w.ackAll("e1")
	w.moveTo("e1-s1", setRunning)
	w.append(w.dir(item), events.Event{Kind: events.KindTransition, At: w.now, From: workQueued, To: workRechecking, Prev: []string{workQueued},
		Trigger: "file", TriggerRef: "recheck:e1:999"})
	for n := 1; n <= 3; n++ {
		sig := w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientRecheck,
			Request: signalStall, Text: "o/r#14", Key: fmt.Sprintf("%s%s:%d", stallPrefix, item, 100+n)})
		w.append(w.dir("e1"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Key: consumedKey(sig)})
	}
	w.fullTick(31 * time.Minute)
	if got := w.status(item).State; got != workRechecking || len(w.stallSignals()) != 3 {
		t.Errorf("%s is %s with %d stall signals; want rechecking with 3", item, got, len(w.stallSignals()))
	}
}
