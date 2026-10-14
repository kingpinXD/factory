package reconcile

import (
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
