package reconcile

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

const (
	head9  = "9999999eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	prURL9 = "https://github.com/o/r/pull/9"
)

// threeItems is a plan whose w3 waits on w1 by a gate: start link and on
// w2's PR, o/r#9, by a gate: merge link.
const threeItems = `uat: none
issues:
  - {ref: o/r#12, result: ready}
  - {ref: o/r#13, result: ready}
  - {ref: o/r#14, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#12}
  - {id: w2, repo: r, issue: o/r#13}
  - {id: w3, repo: r, issue: o/r#14}
sets:
  - {id: s1, items: [w1]}
  - {id: s2, items: [w2]}
  - {id: s3, items: [w3]}
links:
  - {from: w1, to: w3, gate: start, when: merged, rollback: none}
  - {from: o/r#9, to: w3, gate: merge, when: merged, rollback: none}
`

// awaitingReview plans e1 with plan in o/r, which merges only approved
// PRs, and puts e1-w1 in in_review as toInReview does, its PR titled and
// past its quiet time.
func awaitingReview(t *testing.T, plan string) *world {
	w := newWorld(t)
	w.registry()
	w.planned("e1", plan)
	w.toInReview(20 * time.Minute)
	w.pr(func(pr *gh.PR) { pr.Title = "fix: retry the upload" })
	return w
}

// reviewing puts item id in in_review with PR n open, green and quiet on
// head, as toInReview does for e1-w1.
func (w *world) reviewing(id string, n int, url, head string) {
	w.t.Helper()
	w.append(w.dir(id), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree(id)
	w.prs[git.Branch(id)] = gh.PR{ID: "PR_" + id, Number: n, URL: url, Title: "feat: " + id, State: "OPEN", HeadRefName: git.Branch(id),
		HeadRefOid: head, BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE", MergeStateStatus: "CLEAN", Author: gh.User{Login: "me"}, CreatedAt: w.now}
	w.moveTo(id, workPROpen)
	w.append(w.dir(id), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture"})
	w.moveTo(id, workInReview)
	w.writeStatus(id, func(st *store.Status) {
		st.PR, st.PRURL, st.PRState, st.HeadSHA, st.HeadSeenAt = n, url, prOpen, head, w.now.Add(-20*time.Minute)
	})
	w.pulls.checks[head] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
}

// reviewRequests returns the review requests on id's log.
func (w *world) reviewRequests(id string) []events.Event {
	return slices.DeleteFunc(w.kinds(id, events.KindRequest), func(ev events.Event) bool { return ev.Request != requestReview })
}

// keyed reports whether id's log holds an event with key.
func (w *world) keyed(id, key string) bool {
	return slices.ContainsFunc(w.log(id), func(ev events.Event) bool { return ev.Key == key })
}

func TestAPRThatHoldsWorkAndWaitsOnlyOnApprovalGetsOneReviewRequest(t *testing.T) {
	w := awaitingReview(t, twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]"))
	w.tick(false)
	reqs := w.reviewRequests(item)
	want := `post a review request in C09QMJQ71FW for: https://github.com/o/r/pull/5 "fix: retry the upload"`
	if len(reqs) != 1 || reqs[0].Text != want || reqs[0].Recipient != recipientBabysitter {
		t.Fatalf("review requests = %+v, want one: %s", reqs, want)
	}
	if st := w.status(item); !st.WantsReview || st.MergeHold != "it is not approved" {
		t.Errorf("wants_review %v, hold %q", st.WantsReview, st.MergeHold)
	}
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	if !strings.Contains(args[len(args)-1], "woken by: "+want) {
		t.Errorf("babysitter prompt lacks the request:\n%s", args[len(args)-1])
	}
	if got := flagValue(args, "--mcp-config"); got != blueprint.MCPPath(w.brain, "default") {
		t.Errorf("--mcp-config = %q, want the default connectors config", got)
	}
	for i := range 10 {
		w.fullTickAt(time.Duration(i+1) * 5 * time.Minute)
	}
	if got := w.reviewRequests(item); len(got) != 1 {
		t.Errorf("review requests after ten more ticks = %d, want 1", len(got))
	}
	if got := w.status(item).WantsReview; !got {
		t.Errorf("wants_review went false while it waits")
	}
	w.noErrors()
}

func TestOneReviewRequestPerEpicListsEveryPRThatHoldsWork(t *testing.T) {
	w := awaitingReview(t, threeItems)
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.fullNext()
	w.tick(false)
	reqs := w.reviewRequests(item)
	want := `post a review request in C09QMJQ71FW for: https://github.com/o/r/pull/5 "fix: retry the upload", https://github.com/o/r/pull/9 "feat: e1-w2"`
	if len(reqs) != 1 || reqs[0].Text != want || reqs[0].Key != "review-request:o/r#5,o/r#9" {
		t.Fatalf("review requests on e1-w1 = %+v, want one listing both", reqs)
	}
	if got := w.reviewRequests("e1-w2"); len(got) != 0 {
		t.Errorf("review requests on e1-w2 = %+v, want none: e1-w1 carries it", got)
	}
	for _, id := range []string{item, "e1-w2"} {
		if !w.status(id).WantsReview {
			t.Errorf("%s does not want a review", id)
		}
	}
	if !w.keyed("e1-w2", "review-ask:o/r#9") {
		t.Errorf("e1-w2 is not marked asked for")
	}
}

// Without the per-item review-ask mark, the second request lists #5 again.
func TestAPRAskedForOnceIsNeverListedAgain(t *testing.T) {
	w := awaitingReview(t, threeItems)
	w.tick(false)
	if got := w.reviewRequests(item); len(got) != 1 || got[0].Key != "review-request:o/r#5" {
		t.Fatalf("review requests on e1-w1 = %+v, want one for #5", got)
	}
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.fullTickAt(5 * time.Minute)
	if got := w.reviewRequests(item); len(got) != 1 {
		t.Errorf("review requests on e1-w1 = %+v, want still one", got)
	}
	if got := w.reviewRequests("e1-w2"); len(got) != 1 || got[0].Key != "review-request:o/r#9" {
		t.Errorf("review requests on e1-w2 = %+v, want one for #9 alone", got)
	}
}

func TestNoReviewRequestUnlessOnlyTheApprovalHoldsWork(t *testing.T) {
	blocking := twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]")
	holdsAndHeld := twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}, {from: o/r#20, to: w1, gate: merge, when: merged, rollback: none}]")
	alice := gh.User{Login: "alice"}
	for _, tc := range []struct {
		name  string
		plan  string
		setup func(w *world)
		asked bool
	}{
		{name: "only the approval holds it", asked: true},
		{name: "may-merge holds it too", asked: true, plan: holdsAndHeld},
		{name: "red checks", setup: func(w *world) {
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
		}},
		{name: "an unresolved thread", setup: func(w *world) { w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "me")} }},
		{name: "the quiet time", setup: func(w *world) { w.recordPR(head1, w.now.Add(-time.Minute)) }},
		{name: "a change request", setup: func(w *world) {
			w.pr(func(pr *gh.PR) {
				pr.Reviews = []gh.Review{{Author: alice, State: "CHANGES_REQUESTED", SubmittedAt: t0}}
			})
		}},
		{name: "behind its base", setup: func(w *world) { w.pr(func(pr *gh.PR) { pr.MergeStateStatus = "BEHIND" }) }},
		{name: "waiting on a repair", setup: func(w *world) {
			w.baseIs("FAILURE", "aaa1111", red("govulncheck"))
			w.pulls.checks[head1] = []gh.Check{{Name: "govulncheck", State: "FAILURE", Required: true}}
		}},
		{name: "approved", setup: func(w *world) {
			w.pr(func(pr *gh.PR) {
				pr.Reviews = []gh.Review{{Author: alice, AuthorAssociation: "MEMBER", State: "APPROVED", SubmittedAt: t0}}
			})
		}},
		{name: "nothing waits on it", plan: twoItems("[]")},
		{name: "a link into an item that ended", setup: func(w *world) { w.moveTo("e1-w2", workCancelled) }},
		{name: "a repo that merges without approval, held by may-merge", plan: holdsAndHeld, setup: func(w *world) { w.registryMerging(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.plan
			if plan == "" {
				plan = blocking
			}
			w := awaitingReview(t, plan)
			if tc.setup != nil {
				tc.setup(w)
			}
			w.tick(false)
			got := w.reviewRequests(item)
			if asked := len(got) == 1; asked != tc.asked || len(got) > 1 {
				t.Errorf("review requests = %+v, want asked %v; hold %q", got, tc.asked, w.status(item).MergeHold)
			}
			if got := w.status(item).WantsReview; got != tc.asked {
				t.Errorf("wants_review = %v, want %v", got, tc.asked)
			}
		})
	}
}

func TestARepairPRWaitingOnApprovalGetsAReviewRequestWithNoLink(t *testing.T) {
	w := awaitingReview(t, oneItem("[]"))
	if err := store.WriteInputs(w.dir("e1"), &EpicInputs{ID: "e1", Kind: epicKindRepair, Repo: "o/r", Base: "main", Input: "o/r@main is red", AddedAt: t0}); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.reviewRequests(item); len(got) != 1 {
		t.Errorf("review requests = %+v, want one for the repair's PR", got)
	}
}

// passOf logs a babysitter pass on e1-w1: its start, the dones, its end.
func (w *world) passOf(n int, dones ...string) {
	w.t.Helper()
	w.append(w.dir(item), events.Event{Kind: events.KindStart, Sender: item})
	for _, text := range dones {
		w.append(w.dir(item), events.Event{Kind: events.KindDone, Sender: item, Text: text})
	}
	w.end(item, "babysit.v"+string(rune('0'+n))+".md", "## Outcome\nopen — waiting for merge\n## Comments\nnone\n## CI\ngreen\n")
}

const slackDone = "slack post 1791933491.139759 https://github.com/o/r/pull/5 https://github.com/o/r/pull/9"

func TestAPostedReviewRequestMarksEveryPRItNames(t *testing.T) {
	w := awaitingReview(t, threeItems)
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.fullNext()
	w.tick(false)
	w.passOf(1, slackDone)
	w.fullTickAt(5 * time.Minute)
	for _, key := range []string{"review-post:o/r#5"} {
		if !w.keyed(item, key) {
			t.Errorf("e1-w1 lacks %s", key)
		}
	}
	if !w.keyed("e1-w2", "review-post:o/r#9") {
		t.Errorf("e1-w2 lacks review-post:o/r#9")
	}
	if dms := w.dmsWith("review request"); len(dms) != 0 {
		t.Errorf("DMs %q, want none", dms)
	}
	rep, err := Status(t.Context(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(rep.PRs, func(p OpenPR) bool { return p.ID == "e1-w2" }); i < 0 || rep.PRs[i].Review != "review requested in C09QMJQ71FW" || rep.PRs[i].ReviewAt.IsZero() {
		t.Errorf("open PRs = %+v, want e1-w2's review posted", rep.PRs)
	}
}

func TestAPassThatEndsWithoutPostingDMsTheUserOnce(t *testing.T) {
	w := awaitingReview(t, threeItems)
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.fullNext()
	w.tick(false)
	w.passOf(1)
	w.fullTickAt(5 * time.Minute)
	dms := w.dmsWith("did not post the review request")
	want := "factory: e1-w1's babysitter did not post the review request; post it in C09QMJQ71FW yourself:\n - " + prURL + "\n - " + prURL9
	if len(dms) != 1 || dms[0] != want {
		t.Fatalf("DMs = %q, want one:\n%s", w.dms, want)
	}
	for i := range 5 {
		w.fullTickAt(time.Duration(i+2) * 5 * time.Minute)
	}
	if dms := w.dmsWith("did not post the review request"); len(dms) != 1 {
		t.Errorf("DMs = %q, want still one", dms)
	}
	if got := len(w.reviewRequests(item)) + len(w.reviewRequests("e1-w2")); got != 1 {
		t.Errorf("%d review requests, want the first only", got)
	}
}

// A pass under way when the request reaches the babysitter may end
// without reading it; only a pass that began after it counts.
func TestARequestPostedMidPassWaitsForTheNextPass(t *testing.T) {
	w := awaitingReview(t, twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]"))
	posted := w.listen(w.babysitter(1, sessionRunning, 5300, listedWorking).PID)
	w.append(w.dir(item), events.Event{Kind: events.KindStart, Sender: item})
	w.tick(false)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "post a review request in C09QMJQ71FW") {
		t.Fatalf("posts to the live babysitter = %q, want the request", got)
	}
	w.end(item, "babysit.v1.md", "## Outcome\nopen — waiting for merge\n## Comments\nnone\n## CI\ngreen\n")
	w.fullTickAt(5 * time.Minute)
	if dms := w.dmsWith("review request"); len(dms) != 0 {
		t.Fatalf("DMs = %q after the pass the request reached mid-way, want none", dms)
	}
	w.passOf(2, "slack post 1791933491.139759 "+prURL)
	w.fullTickAt(10 * time.Minute)
	w.fullTickAt(15 * time.Minute)
	if dms := w.dmsWith("review request"); len(dms) != 0 {
		t.Errorf("DMs = %q, want none ever", dms)
	}
	if !w.keyed(item, "review-post:o/r#5") {
		t.Errorf("e1-w1 is not marked posted")
	}
}

// A pass the request never reached cannot answer it: here the account
// holds the babysitter's work back, so the request is not posted to it yet.
func TestARequestNotYetGivenToTheBabysitterSendsNoDM(t *testing.T) {
	w := awaitingReview(t, twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]"))
	posted := w.listen(w.babysitter(1, sessionRunning, 5300, listedWorking).PID)
	w.accountIs(stateNearLimit)
	w.tick(false)
	if got := w.reviewRequests(item); len(got) != 1 {
		t.Fatalf("review requests = %+v, want one", got)
	}
	w.passOf(1)
	w.fullTickAt(5 * time.Minute)
	if got := posted(); len(got) != 0 {
		t.Fatalf("posts = %q, want none while the account holds work back", got)
	}
	if dms := w.dmsWith("review request"); len(dms) != 0 {
		t.Errorf("DMs = %q for a pass the request never reached", dms)
	}

	// The account frees up and the request reaches the babysitter; the pass
	// before that still does not count.
	w.accountIs(stateOK)
	w.fullTickAt(10 * time.Minute)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "post a review request") {
		t.Fatalf("posts = %q, want the request", got)
	}
	w.fullTickAt(15 * time.Minute)
	if dms := w.dmsWith("review request"); len(dms) != 0 {
		t.Errorf("DMs = %q for a pass that ended before the request reached it", dms)
	}
}

func TestACarrierThatLeavesReviewUnpostedDMsTheUserOnce(t *testing.T) {
	w := awaitingReview(t, twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]"))
	w.tick(false)
	w.moveTo(item, stateNeedsYou)
	w.fullTickAt(5 * time.Minute)
	w.fullTickAt(10 * time.Minute)
	if dms := w.dmsWith("did not post the review request"); len(dms) != 1 {
		t.Errorf("DMs = %q, want one", w.dms)
	}
}

func TestTheDependentStartsOnceTheReviewedPRMerges(t *testing.T) {
	w := awaitingReview(t, twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]"))
	w.tick(false)
	if got := w.status("e1-w2").State; got != workQueued {
		t.Fatalf("e1-w2 is %s, want queued behind e1-w1", got)
	}
	w.pr(func(pr *gh.PR) {
		pr.Reviews = []gh.Review{{Author: gh.User{Login: "alice"}, AuthorAssociation: "MEMBER", State: "APPROVED", SubmittedAt: t0}}
	})
	w.fullTickAt(5 * time.Minute)
	w.fullTickAt(10 * time.Minute)
	if got := w.status(item).State; got != stateMerged && got != workDone {
		t.Fatalf("e1-w1 is %s, want merged; moves %v", got, w.moves(item))
	}
	w.fullTickAt(15 * time.Minute)
	if got := w.status("e1-w2").State; got == workQueued {
		t.Errorf("e1-w2 is still queued once e1-w1 merged; moves %v", w.moves("e1-w2"))
	}
}
