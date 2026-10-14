package reconcile

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/notify"
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

// threeItemsWith is threeItems with links instead of its own.
func threeItemsWith(links string) string {
	return threeItems[:strings.Index(threeItems, "links:")] + "links: " + links + "\n"
}

// keyed reports whether id's log holds an event with key.
func (w *world) keyed(id, key string) bool {
	return slices.ContainsFunc(w.log(id), func(ev events.Event) bool { return ev.Key == key })
}

// forget drops the events of id's log whose key starts with prefix, as a
// tick that died before logging them would have left it.
func (w *world) forget(id, prefix string) {
	w.t.Helper()
	var b strings.Builder
	for _, ev := range w.log(id) {
		if strings.HasPrefix(ev.Key, prefix) {
			continue
		}
		line, err := json.Marshal(ev)
		if err != nil {
			w.t.Fatal(err)
		}
		b.Write(append(line, '\n'))
	}
	put(w.t, store.EventsPath(w.dir(id)), b.String())
}

// openPR returns factory status's line for id's open PR.
func (w *world) openPR(id string) OpenPR {
	w.t.Helper()
	rep, err := Status(w.t.Context(), w.deps())
	if err != nil {
		w.t.Fatal(err)
	}
	i := slices.IndexFunc(rep.PRs, func(p OpenPR) bool { return p.ID == id })
	if i < 0 {
		w.t.Fatalf("no open PR for %s in %+v", id, rep.PRs)
	}
	return rep.PRs[i]
}

// bothInReview is e1 with two items and no links, each with a PR held only
// by the approval: e1-w1's #5 and e1-w2's #9.
func bothInReview(t *testing.T) *world {
	w := awaitingReview(t, twoItems("[]"))
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.fullNext()
	return w
}

const postedBoth = "do the thing\n - " + prURL + "\n - " + prURL9

func TestAnEpicWaitingOnlyOnReviewsPostsOneRequestForAllItsPRs(t *testing.T) {
	w := bothInReview(t)
	w.moveTo("e1", "merging")
	w.tick(false)
	if want := []post{{"C09QMJQ71FW", postedBoth}}; !slices.Equal(w.posts, want) {
		t.Fatalf("posts = %q, want %q", w.posts, want)
	}
	for i := range 10 {
		w.fullTickAt(time.Duration(i+1) * 5 * time.Minute)
	}
	if len(w.posts) != 1 {
		t.Errorf("posts after ten more ticks = %q, want the first only", w.posts)
	}
	for id, key := range map[string]string{item: "review-post:o/r#5", "e1-w2": "review-post:o/r#9"} {
		if !w.keyed(id, key) {
			t.Errorf("%s lacks %s", id, key)
		}
		if !w.status(id).WantsReview {
			t.Errorf("%s does not want a review", id)
		}
	}
	if p := w.openPR("e1-w2"); p.Review != "review requested in #engineering" || !p.ReviewAt.Equal(t0) {
		t.Errorf("e1-w2's open PR = %+v, want its review requested at t0", p)
	}
	if len(w.dms) != 0 {
		t.Errorf("DMs = %q, want none", w.dms)
	}
	w.noErrors()
}

// e1-w2 dropped back to implementing after e1 reached merging: the epic
// stays in merging, but no longer waits only on reviews.
func TestAnEpicWithAnItemBackToWorkDoesNotWaitOnReviews(t *testing.T) {
	w := bothInReview(t)
	w.moveTo("e1", "merging")
	w.moveTo("e1-w2", workImplementing)
	w.tick(false)
	if got := w.status("e1").State; got != "merging" {
		t.Fatalf("e1 is %s, want still merging", got)
	}
	if len(w.posts) != 0 {
		t.Errorf("posts = %q, want none: e1-w2 is being worked", w.posts)
	}
}

func TestNoReviewRequestUnlessOnlyTheApprovalHoldsWork(t *testing.T) {
	blocking := twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]")
	holdsAndHeld := twoItems("[{from: w1, to: w2, gate: start, when: merged, rollback: none}, {from: o/r#20, to: w1, gate: merge, when: merged, rollback: none}]")
	// w3 has no link and is still queued, so the epic waits on more than
	// reviews.
	linkAndOther := threeItemsWith("[{from: w1, to: w2, gate: start, when: merged, rollback: none}]")
	alice := gh.User{Login: "alice"}
	for _, tc := range []struct {
		name   string
		plan   string
		setup  func(w *world)
		posted bool
	}{
		{name: "only the approval holds it", posted: true},
		{name: "may-merge holds it too", posted: true, plan: holdsAndHeld},
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
			w.baseIs("aaa1111", red("govulncheck"))
			w.pulls.checks[head1] = []gh.Check{{Name: "govulncheck", State: "FAILURE", Required: true}}
		}},
		{name: "approved", setup: func(w *world) { w.approve(item) }},
		{name: "nothing waits on it", plan: twoItems("[]")},
		{name: "a link into an item that ended", plan: linkAndOther, setup: func(w *world) { w.moveTo("e1-w2", workCancelled) }},
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
			want := []post{}
			if tc.posted {
				want = []post{{"C09QMJQ71FW", "do the thing\n - " + prURL}}
			}
			if !slices.Equal(w.posts, want) {
				t.Errorf("posts = %q, want %q; hold %q", w.posts, want, w.status(item).MergeHold)
			}
			if got := w.status(item).WantsReview; got != tc.posted {
				t.Errorf("wants_review = %v, want %v", got, tc.posted)
			}
		})
	}
}

func TestARepairPRWaitingOnApprovalIsPostedUnderTheRepairsTitle(t *testing.T) {
	w := awaitingReview(t, oneItem("[]"))
	if err := store.WriteInputs(w.dir("e1"), &EpicInputs{ID: "e1", Kind: epicKindRepair, Repo: "o/r", Base: "main", Input: "o/r@main is red", AddedAt: t0}); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if want := []post{{"C09QMJQ71FW", "Fix for red o/r@main\n - " + prURL}}; !slices.Equal(w.posts, want) {
		t.Errorf("posts = %q, want %q", w.posts, want)
	}
}

func TestAnIssueEpicIsPostedUnderItsIssueTitle(t *testing.T) {
	w := awaitingReview(t, oneItem("[]"))
	w.hub.issue("o/r", 40).Title = "Retry failed uploads"
	if err := store.WriteInputs(w.dir("e1"), &EpicInputs{ID: "e1", Kind: "issue", Input: "https://github.com/o/r/issues/40", AddedAt: t0}); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if want := []post{{"C09QMJQ71FW", "Retry failed uploads\n - " + prURL}}; !slices.Equal(w.posts, want) {
		t.Errorf("posts = %q, want %q", w.posts, want)
	}
}

func TestAReviewPostSlackRefusedDMsTheMessageOnce(t *testing.T) {
	w := bothInReview(t)
	w.postErr = fmt.Errorf("slack-post.sh: %w", notify.ErrNotPosted)
	w.tick(false)
	for i := range 3 {
		w.fullTickAt(time.Duration(i+1) * 5 * time.Minute)
	}
	if len(w.posts) != 1 {
		t.Errorf("posts = %q, want one try", w.posts)
	}
	want := "factory: the review request could not be posted in #engineering; post it yourself:\n" + postedBoth
	if !slices.Equal(w.dms, []string{want}) {
		t.Errorf("DMs = %q, want one:\n%s", w.dms, want)
	}
	if p := w.openPR(item); p.Review != "asked you by DM" {
		t.Errorf("e1-w1's review = %q, want asked you by DM", p.Review)
	}
}

func TestAReviewPostThatMayHaveLandedIsNotRetried(t *testing.T) {
	w := bothInReview(t)
	w.postErr = fmt.Errorf("slack-post.sh: %w", notify.ErrMaybePosted)
	w.tick(false)
	for i := range 3 {
		w.fullTickAt(time.Duration(i+1) * 5 * time.Minute)
	}
	if len(w.posts) != 1 {
		t.Errorf("posts = %q, want one try", w.posts)
	}
	want := "factory: a review request for " + prURL + ", " + prURL9 + " may not have gone out; check #engineering"
	if !slices.Equal(w.dms, []string{want}) {
		t.Errorf("DMs = %q, want one:\n%s", w.dms, want)
	}
}

// threeReviewing is bothInReview with a third PR, e1-w3's #11, whose
// checks are red: it does not want a review until they turn green.
func threeReviewing(t *testing.T) *world {
	w := awaitingReview(t, threeItemsWith("[]"))
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.reviewing("e1-w3", 11, prURL11, head11)
	w.pulls.checks[head11] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	w.fullNext()
	return w
}

const (
	head11  = "1111111eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	prURL11 = "https://github.com/o/r/pull/11"
)

// A tick that posted and died before logging it: the next one does not
// post again, and tells the user to check.
func TestATickThatDiedAfterPostingDoesNotPostAgain(t *testing.T) {
	w := threeReviewing(t)
	w.tick(false)
	if len(w.posts) != 1 {
		t.Fatalf("posts = %q, want one", w.posts)
	}
	w.forget("e1", "review-post-done:")
	w.forget(item, "review-post:")
	w.forget("e1-w2", "review-post:")
	w.fullTickAt(5 * time.Minute)
	w.fullTickAt(10 * time.Minute)
	if len(w.posts) != 1 {
		t.Errorf("posts = %q, want still one", w.posts)
	}
	want := "factory: a review request for " + prURL + ", " + prURL9 + " may not have gone out; check #engineering"
	if !slices.Equal(w.dms, []string{want}) {
		t.Errorf("DMs = %q, want one:\n%s", w.dms, want)
	}
	w.pulls.checks[head11] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	w.fullTickAt(15 * time.Minute)
	if want := []post{{"C09QMJQ71FW", "do the thing\n - " + prURL11}}; len(w.posts) != 2 || w.posts[1] != want[0] {
		t.Errorf("posts = %q, want #11 posted alone second", w.posts)
	}
}

// With the DMs failing too, nothing marks the items: the intent alone keeps
// them from a second post.
func TestADanglingIntentWhoseDMFailsStillBlocksASecondPost(t *testing.T) {
	w := bothInReview(t)
	w.tick(false)
	w.forget("e1", "review-post-done:")
	w.forget(item, "review-post:")
	w.forget("e1-w2", "review-post:")
	w.dmErr = errors.New("slack-dm.sh: exit status 1")
	w.fullTickAt(5 * time.Minute)
	w.fullTickAt(10 * time.Minute)
	if len(w.posts) != 1 {
		t.Errorf("posts = %q, want still one", w.posts)
	}
}

// A tick that logged the post and died before marking the items: the next
// one marks them and posts nothing.
func TestATickThatDiedBeforeMarkingTheItemsMarksThemNext(t *testing.T) {
	w := threeReviewing(t)
	w.tick(false)
	w.forget(item, "review-post:")
	w.forget("e1-w2", "review-post:")
	w.fullTickAt(5 * time.Minute)
	if len(w.posts) != 1 || len(w.dms) != 0 {
		t.Errorf("posts = %q, DMs %q, want one post and no DM", w.posts, w.dms)
	}
	for id, key := range map[string]string{item: "review-post:o/r#5", "e1-w2": "review-post:o/r#9"} {
		if !w.keyed(id, key) {
			t.Errorf("%s lacks %s", id, key)
		}
	}
	w.pulls.checks[head11] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	w.fullTickAt(10 * time.Minute)
	if want := (post{"C09QMJQ71FW", "do the thing\n - " + prURL11}); len(w.posts) != 2 || w.posts[1] != want {
		t.Errorf("posts = %q, want #11 posted alone second", w.posts)
	}
}

// #103's URL is a prefix of #1031's: an intent naming #1031 does not ask
// for #103.
func TestAnIntentAsksOnlyForThePRsItNamesWhole(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	url103, url1031 := "https://github.com/o/r/pull/103", "https://github.com/o/r/pull/1031"
	w.reviewing(item, 103, url103, head1)
	w.reviewing("e1-w2", 1031, url1031, head9)
	w.append(w.dir("e1"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: url1031, Key: "review-post-intent:e1:1"})
	w.append(w.dir("e1"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "1791933491.000001 " + url1031, Key: "review-post-done:e1:1"})
	w.fullNext()
	w.tick(false)
	if want := []post{{"C09QMJQ71FW", "do the thing\n - " + url103}}; !slices.Equal(w.posts, want) {
		t.Errorf("posts = %q, want %q", w.posts, want)
	}
}

// #32 asked a babysitter to post, logging review-ask on each PR's item, then
// DMed the user when it did not, logging review-dm:<refs> on the carrier.
func TestAPRAskedForByAnEarlierFactoryIsNotPostedAgain(t *testing.T) {
	for _, tc := range []struct {
		keys   []string
		review string
	}{
		{[]string{"review-ask:o/r#5", "review-dm:o/r#5"}, "asked you by DM"},
		{[]string{"review-ask:o/r#5"}, "wants a review"},
		{[]string{"review-dm:o/r#5,o/r#9"}, "asked you by DM"},
	} {
		t.Run(strings.Join(tc.keys, " "), func(t *testing.T) {
			w := awaitingReview(t, oneItem("[]"))
			for _, key := range tc.keys {
				w.append(w.dir(item), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: key, Key: key})
			}
			w.tick(false)
			if len(w.posts) != 0 {
				t.Errorf("posts = %q, want none", w.posts)
			}
			if p := w.openPR(item); p.Review != tc.review {
				t.Errorf("review = %q, want %s", p.Review, tc.review)
			}
		})
	}
}

// The user asked for a review of #5 by hand: the manager posts only #9.
func TestAPRTheUserMarkedAskedIsNotPosted(t *testing.T) {
	w := bothInReview(t)
	if _, err := ReviewAsked(t.Context(), w.deps(), []string{prURL}); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if want := []post{{"C09QMJQ71FW", "do the thing\n - " + prURL9}}; !slices.Equal(w.posts, want) {
		t.Errorf("posts = %q, want %q", w.posts, want)
	}
	if p := w.openPR(item); p.Review != "asked by you" || !p.ReviewAt.Equal(t0) {
		t.Errorf("e1-w1's open PR = %+v, want asked by you at t0", p)
	}
}

func TestTheBabysitterOfAPostedPRIsToldButNotWokenByIt(t *testing.T) {
	w := awaitingReview(t, oneItem("[]"))
	w.tick(false)
	if len(w.posts) != 1 {
		t.Fatalf("posts = %q, want one", w.posts)
	}
	w.fullTickAt(5 * time.Minute)
	if slices.ContainsFunc(w.calls(), func(c string) bool { return strings.Contains(c, "factory:e1-w1:babysitter") }) {
		t.Fatalf("a babysitter started for the post: %q", w.calls())
	}
	w.pulls.comments[5] = []gh.Comment{{ID: 31, Author: "carol", URL: prURL + "#issuecomment-31"}}
	w.fullTickAt(25 * time.Minute)
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	if want := "waiting on approval; review requested in #engineering 25m0s ago\n"; !strings.Contains(args[len(args)-1], want) {
		t.Errorf("babysitter prompt lacks %q:\n%s", want, args[len(args)-1])
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

// approve approves the PR of item id on GitHub.
func (w *world) approve(id string) {
	pr := w.prs[git.Branch(id)]
	pr.Reviews = []gh.Review{{Author: gh.User{Login: "alice"}, AuthorAssociation: "MEMBER", State: "APPROVED", SubmittedAt: t0}}
	w.prs[git.Branch(id)] = pr
}

const reminded = "Still waiting on a review:\n - " + prURL

func TestAPRNoPersonReviewedForADayGetsOneReminder(t *testing.T) {
	greptile := gh.Review{Author: gh.User{Login: "greptile-apps[bot]"}, AuthorAssociation: "NONE", State: "COMMENTED", SubmittedAt: t0}
	mine := gh.Review{Author: gh.User{Login: "me"}, AuthorAssociation: "OWNER", State: "COMMENTED", SubmittedAt: t0}
	alice := gh.Review{Author: gh.User{Login: "alice"}, AuthorAssociation: "MEMBER", State: "COMMENTED", SubmittedAt: t0}
	for _, tc := range []struct {
		name     string
		reviews  []gh.Review
		reminded bool
	}{
		{name: "no review", reminded: true},
		{name: "only a bot's", reviews: []gh.Review{greptile}, reminded: true},
		{name: "only its author's", reviews: []gh.Review{mine}, reminded: true},
		{name: "a person commented", reviews: []gh.Review{greptile, alice}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := awaitingReview(t, oneItem("[]"))
			w.tick(false)
			w.pr(func(pr *gh.PR) { pr.Reviews = tc.reviews })
			w.fullTickAt(23*time.Hour + 59*time.Minute)
			if len(w.posts) != 1 {
				t.Fatalf("posts before 24h = %q, want the request only", w.posts)
			}
			w.fullTickAt(24*time.Hour + time.Minute)
			w.fullTickAt(24*time.Hour + 6*time.Minute)
			want := []post{{"C09QMJQ71FW", "do the thing\n - " + prURL}}
			if tc.reminded {
				want = append(want, post{"C09QMJQ71FW", reminded})
			}
			if !slices.Equal(w.posts, want) {
				t.Errorf("posts = %q, want %q", w.posts, want)
			}
			if got := w.keyed(item, "review-reminder:o/r#5"); got != tc.reminded {
				t.Errorf("review-reminder:o/r#5 logged = %v, want %v", got, tc.reminded)
			}
			w.noErrors()
		})
	}
}

func TestAnApprovedPRGetsNoReminder(t *testing.T) {
	w := awaitingReview(t, oneItem("[]"))
	w.tick(false)
	w.pr(func(pr *gh.PR) { pr.IsDraft = true; pr.ReviewDecision = "APPROVED" })
	w.fullTickAt(25 * time.Hour)
	if len(w.posts) != 1 {
		t.Errorf("posts = %q, want the request only", w.posts)
	}
}

// A tick that posted the reminder and died before logging it: no second
// reminder follows.
func TestATickThatDiedAfterRemindingDoesNotRemindAgain(t *testing.T) {
	w := awaitingReview(t, oneItem("[]"))
	w.tick(false)
	w.fullTickAt(25 * time.Hour)
	if len(w.posts) != 2 {
		t.Fatalf("posts = %q, want the request and a reminder", w.posts)
	}
	w.forget("e1", "review-reminder-done:")
	w.forget(item, "review-reminder:")
	w.fullTickAt(25*time.Hour + 5*time.Minute)
	w.fullTickAt(25*time.Hour + 10*time.Minute)
	if len(w.posts) != 2 {
		t.Errorf("posts = %q, want still two", w.posts)
	}
	want := "factory: a review reminder for " + prURL + " may not have gone out; check #engineering"
	if got := w.dmsWith("review reminder"); !slices.Equal(got, []string{want}) {
		t.Errorf("DMs = %q, want one:\n%s", got, want)
	}
}

// A reminder Slack refused DMs the user the message once; one that may have
// landed DMs the user to check. Neither is posted again.
func TestAFailedReminderDMsTheUserOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		dm   string
	}{
		{name: "refused", err: notify.ErrNotPosted, dm: "factory: the review reminder could not be posted in #engineering; post it yourself:\n" + reminded},
		{name: "may have landed", err: notify.ErrMaybePosted, dm: "factory: a review reminder for " + prURL + " may not have gone out; check #engineering"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := awaitingReview(t, oneItem("[]"))
			w.tick(false)
			w.postErr = fmt.Errorf("slack-post.sh: %w", tc.err)
			for i := range 3 {
				w.fullTickAt(25*time.Hour + time.Duration(i)*5*time.Minute)
			}
			if len(w.posts) != 2 {
				t.Errorf("posts = %q, want the request and one reminder try", w.posts)
			}
			if got := w.dmsWith("review reminder"); !slices.Equal(got, []string{tc.dm}) {
				t.Errorf("DMs = %q, want one:\n%s", got, tc.dm)
			}
		})
	}
}

// e1-w1 was posted a day ago; e1-w2's checks just turned green. One tick
// posts e1-w2's request and e1-w1's reminder, each settling its own PR.
func TestARequestAndAReminderInOneTickSettleTheirOwnPRs(t *testing.T) {
	w := awaitingReview(t, twoItems("[]"))
	w.reviewing("e1-w2", 9, prURL9, head9)
	w.pulls.checks[head9] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	w.fullNext()
	w.tick(false)
	w.pulls.checks[head9] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	w.fullTickAt(25 * time.Hour)
	want := []post{{"C09QMJQ71FW", "do the thing\n - " + prURL}, {"C09QMJQ71FW", "do the thing\n - " + prURL9}, {"C09QMJQ71FW", reminded}}
	if !slices.Equal(w.posts, want) {
		t.Fatalf("posts = %q, want %q", w.posts, want)
	}
	for _, tc := range []struct {
		id, key string
		logged  bool
	}{
		{item, "review-reminder:o/r#5", true},
		{"e1-w2", "review-post:o/r#9", true},
		{"e1-w2", "review-reminder:o/r#9", false},
	} {
		if got := w.keyed(tc.id, tc.key); got != tc.logged {
			t.Errorf("%s has %s = %v, want %v", tc.id, tc.key, got, tc.logged)
		}
	}
	w.noErrors()
}
