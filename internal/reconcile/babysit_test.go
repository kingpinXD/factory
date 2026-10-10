package reconcile

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

// watching puts item e1-w1 in in_review as inReviewWith does, its head
// first seen a minute ago, so the quiet time holds any merge.
func watching(t *testing.T) *world {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), time.Minute)
	return w
}

// babysitter puts the item's nth babysitter session in state, listed with
// pid in listed, and returns its listing.
func (w *world) babysitter(n int, state string, pid int, listed string) claude.Session {
	w.t.Helper()
	w.put(babysitterID(item, n), blueprint.MachineSession, "e1", filepath.Join(w.dir(item), "sessions", babysitterSlot(n)), state,
		&SessionInputs{Name: babysitterName(item, n), Component: babysitterComponent, Task: item})
	return w.listSession(babysitterName(item, n), pid, listed)
}

// thread is an unresolved review thread whose last comment, id, is by who.
func thread(id string, last int64, who string) gh.Thread {
	return gh.Thread{ID: id, First: gh.Comment{ID: 1, Author: "alice"}, Last: gh.Comment{ID: last, Author: who, URL: "https://github.com/o/r/pull/5#c" + id}}
}

// toBabysitter returns the requests to the item's babysitter.
func (w *world) toBabysitter() []events.Event {
	return slices.DeleteFunc(w.kinds(item, events.KindRequest), func(ev events.Event) bool { return ev.Recipient != recipientBabysitter })
}

// delivered marks the item's babysitter requests delivered to its nth
// babysitter, as if it had been told of them.
func (w *world) delivered(n int) {
	w.t.Helper()
	st := w.status(babysitterID(item, n))
	for _, ev := range w.toBabysitter() {
		st.Delivered = append(st.Delivered, item+"#"+seqRef(ev))
	}
	if err := store.WriteStatus(w.dir(babysitterID(item, n)), st); err != nil {
		w.t.Fatal(err)
	}
}

func TestEachTriggerWakesTheBabysitterOnce(t *testing.T) {
	w := watching(t)
	s := w.babysitter(1, sessionTurnEnded, 5200, listedTurnEnded)
	posted := w.listen(s.PID)
	w.append(w.dir(item), events.Event{Kind: events.KindDone, Sender: item, Text: "dm alice: fixed 2 comments"})
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice"), {ID: "T2", IsResolved: true, Last: gh.Comment{ID: 21, Author: "dave"}}}
	w.pulls.comments[5] = []gh.Comment{{ID: 31, Author: "carol", URL: "https://github.com/o/r/pull/5#issuecomment-31"}, {ID: 32, Author: "me"}}
	w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	w.pr(func(pr *gh.PR) {
		pr.Mergeable = "CONFLICTING"
		pr.Reviews = []gh.Review{{Author: gh.User{Login: "bob"}, State: "COMMENTED", Body: "nit", SubmittedAt: t0}, {Author: gh.User{Login: "eve"}, State: "APPROVED", SubmittedAt: t0}}
	})
	w.tick(false)

	got := posted()
	if len(got) != 1 {
		t.Fatalf("posted %q, want one wake\n%s", got, w.out.String())
	}
	for _, want := range []string{
		"factory-task: e1-w1\n", "PR: " + prURL + "\n", "issue: o/r#14\n", "branch: factory/e1-w1\n",
		"worktree (handed over to you): " + filepath.Join(w.home, "src", "r-worktrees", "factory-e1-w1"),
		"alice commented on a review thread: https://github.com/o/r/pull/5#cT1",
		"bob reviewed it: commented", "carol commented on the PR: https://github.com/o/r/pull/5#issuecomment-31",
		"red checks on 1111111: ci", "a conflict with main at 3333333 (head 1111111)",
		"already DMed about this PR: alice\n",
	} {
		if !strings.Contains(got[0], want) {
			t.Errorf("wake lacks %q:\n%s", want, got[0])
		}
	}
	for _, not := range []string{"dave", "eve", "me commented"} {
		if strings.Contains(got[0], not) {
			t.Errorf("wake names %q, which needs no answer:\n%s", not, got[0])
		}
	}
	if n := len(w.toBabysitter()); n != 5 {
		t.Errorf("logged %d requests to the babysitter, want 5", n)
	}
	if st := w.status(item); st.Unanswered != 1 {
		t.Errorf("unanswered = %d, want 1", st.Unanswered)
	}

	w.now = w.now.Add(5 * time.Minute)
	w.fullNext()
	w.tick(false)
	if got := posted(); len(got) != 1 {
		t.Fatalf("woken again by the same things: %q", got[1:])
	}
	w.pulls.threads[5][0] = thread("T1", 12, "alice")
	w.fullNext()
	w.tick(false)
	got = posted()
	if len(got) != 2 || !strings.Contains(got[1], "alice commented on a review thread") || strings.Contains(got[1], "carol") {
		t.Errorf("after alice's second comment, posted %q; want one wake with only it", got)
	}
	if starts := w.called("claude --bg"); len(starts) != 0 {
		t.Errorf("started or resumed a babysitter while one is live: %q", starts)
	}
	w.noErrors()
}

func TestUnansweredThreadsStartABabysitterWhenNoneIsLive(t *testing.T) {
	w := watching(t)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.tick(false)
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	prompt := args[len(args)-1]
	for _, want := range []string{"factory-task: e1-w1\n", "PR: " + prURL, "woken by: alice commented on a review thread", "already DMed about this PR: nobody yet"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("start prompt lacks %q:\n%s", want, prompt)
		}
	}
	reserve := "renew " + prURL + " e1-w1 FACTORY_TASK=e1-w1 BRAIN=" + w.brain
	if got := w.pulls.prsMD; len(got) != 1 || !strings.HasSuffix(got[0], reserve) || !strings.Contains(got[0], filepath.Join(w.brain, "scripts", "harness", "prs.sh")) {
		t.Fatalf("prs.sh calls = %q, want the line reserved on the first start", got)
	}
	for range 2 {
		w.now = w.now.Add(5 * time.Minute)
		w.fullNext()
		w.tick(false)
	}
	if got := w.called("claude --bg"); len(got) != 1 {
		t.Errorf("babysitter starts = %q, want one", got)
	}
	if got := w.pulls.prsMD; len(got) != 3 {
		t.Errorf("prs.sh calls = %q, want the line renewed on each full reconcile while open", got)
	}
	w.noErrors()
}

func TestAFinishedBabysitterIsFollowedByANewOne(t *testing.T) {
	w := watching(t)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.babysitter(1, sessionFinished, 0, "stopped")
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter,
		Request: "thread", Text: "alice commented on a review thread", Key: "babysit:thread:T1:11"})
	w.delivered(1)
	w.tick(false)
	args := startArgs(t, w, "factory:e1-w1:babysitter-2")
	if prompt := args[len(args)-1]; !strings.Contains(prompt, "woken by: 1 unanswered review thread\n") {
		t.Errorf("prompt =\n%s\nwant it woken by the unanswered thread", prompt)
	}
	if got := w.called("claude --bg"); len(got) != 1 {
		t.Errorf("starts = %q, want only the new babysitter", got)
	}
	if got := w.status(babysitterID(item, 2)).State; got != sessionStarting && got != sessionRunning {
		t.Errorf("new babysitter is %s", got)
	}
	w.noErrors()
}

func TestADeadBabysitterIsResumedNotDoubled(t *testing.T) {
	w := watching(t)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	s := w.babysitter(1, sessionDead, 0, "stopped")
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter,
		Request: "thread", Text: "alice commented on a review thread", Key: "babysit:thread:T1:11"})
	w.delivered(1)
	w.tick(false)
	// The full reconcile counts the thread after the session's move; the
	// next tick resumes it.
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := w.called("claude --bg --model"); len(got) != 0 {
		t.Errorf("started a second babysitter: %q", got)
	}
	resumes := w.called("claude --bg --resume")
	if len(resumes) != 1 || !strings.HasPrefix(resumes[0], "claude --bg --resume "+s.SessionID+" factory-task: e1-w1") || !strings.Contains(resumes[0], "1 unanswered review thread") {
		t.Errorf("resumes = %q, want the dead babysitter resumed with no flags", resumes)
	}
}

func TestALiveBabysitterIsWokenNotDoubled(t *testing.T) {
	w := watching(t)
	s := w.babysitter(1, sessionRunning, 5300, listedWorking)
	posted := w.listen(s.PID)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.tick(false)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "alice commented on a review thread") {
		t.Errorf("posted %q, want one wake", got)
	}
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started or resumed a second babysitter: %q", got)
	}
}

func TestAThreadEndingInOurReplyStartsNothingAndHoldsTheMerge(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 12, "me")}
	w.tick(false)
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started a babysitter for an answered thread: %q", got)
	}
	if got := w.toBabysitter(); len(got) != 0 {
		t.Errorf("requests to the babysitter = %+v, want none", got)
	}
	if st := w.status(item); st.State != workInReview || st.Unanswered != 0 {
		t.Errorf("status = %s with %d unanswered, want in_review with none", st.State, st.Unanswered)
	}
	if got := w.called("gh pr merge"); len(got) != 0 {
		t.Errorf("merged with an unresolved thread: %q", got)
	}
}

func TestNothingIsWatchedAfterTheMerge(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.tick(false)
	w.now = w.now.Add(5 * time.Minute)
	w.fullNext()
	w.tick(false)
	if got := w.status(item).State; got != stateMerged && got != "done" {
		t.Fatalf("%s is %s, want merged", item, got)
	}
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.pulls.comments[5] = []gh.Comment{{ID: 31, Author: "carol"}}
	w.now = w.now.Add(5 * time.Minute)
	w.fullNext()
	w.tick(false)
	if got := w.toBabysitter(); len(got) != 0 {
		t.Errorf("requests after the merge = %+v", got)
	}
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started a babysitter after the merge: %q", got)
	}
	if got := w.pulls.prsMD; len(got) != 0 {
		t.Errorf("prs.sh calls after the merge: %q", got)
	}
}

func TestAClosedPRWaitsForTheUserWithOneDM(t *testing.T) {
	w := watching(t)
	w.pr(func(pr *gh.PR) { pr.State = "CLOSED" })
	w.tick(false)
	if got := w.moves(item); !slices.Equal(got[len(got)-2:], []string{"in_review→closed", "closed→needs_you"}) {
		t.Fatalf("moves = %v", got)
	}
	w.now = w.now.Add(5 * time.Minute)
	w.fullNext()
	w.tick(false)
	if got := w.moves(item); last(got) != "closed→needs_you" {
		t.Errorf("moves = %v, want it to stay in needs_you", got)
	}
	if got := w.dmsWith("e1-w1 needs you: its PR " + prURL + " was closed without merging"); len(got) != 1 {
		t.Errorf("DMs = %q, want one", w.dms)
	}
}

func TestTheSameHeadRedAfterAPassWaitsForTheUser(t *testing.T) {
	for _, tc := range []struct {
		name string
		// passFirst ends the babysitter's pass before the checks go red.
		passFirst, push, green bool
		want                   bool
	}{
		{name: "red, a pass, still red", want: true},
		{name: "a pass, then red", passFirst: true},
		{name: "red, a pass, a new head red too", push: true},
		{name: "red, a pass, green again", green: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := watching(t)
			w.babysitter(1, sessionTurnEnded, 5400, listedTurnEnded)
			pass := "## Outcome\nopen — CI red\n## Comments\nnone\n## CI\nci fails: not fixable here\n"
			if tc.passFirst {
				w.end(item, "babysit.v1.md", pass)
			}
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
			w.tick(false)
			if !tc.passFirst {
				w.end(item, "babysit.v1.md", pass)
			}
			if tc.push {
				w.pr(func(pr *gh.PR) { pr.HeadRefOid = head2 })
				w.pulls.checks[head2] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
			}
			if tc.green {
				w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
			}
			w.now = w.now.Add(5 * time.Minute)
			w.fullNext()
			w.tick(false)
			state, dms := w.status(item).State, w.dmsWith("checks on "+prURL+" stayed red after a babysitter pass, on 1111111")
			if !tc.want {
				if state == workNeedsYou || len(dms) != 0 {
					t.Errorf("state %s, DMs %q; want it left with the babysitter", state, dms)
				}
				return
			}
			if state != workNeedsYou || len(dms) != 1 {
				t.Errorf("state %s, DMs %q; want needs_you and one DM", state, w.dms)
			}
			if got := last(w.kinds(item, events.KindTransition)).TriggerRef; got != redAfterPassRef+head1 {
				t.Errorf("trigger_ref = %q", got)
			}
			// The user settles it now: nothing on the PR wakes the babysitter.
			before := len(w.toBabysitter())
			w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
			w.fullNext()
			w.tick(false)
			if got := w.toBabysitter(); len(got) != before {
				t.Errorf("requests to the babysitter in needs_you: %+v", got[before:])
			}
		})
	}
}

func TestTriggersLeaveOutWhatTheUserWrote(t *testing.T) {
	me := gh.User{Login: "me"}
	pr := gh.PR{HeadRefOid: head1, BaseRefOid: base1, Mergeable: "MERGEABLE", Reviews: []gh.Review{
		{Author: me, State: "COMMENTED", Body: "self note"},
		{Author: gh.User{Login: "bob"}, State: "CHANGES_REQUESTED"},
		{Author: gh.User{Login: "eve"}, State: "COMMENTED"},
	}}
	rd := &prReads{
		threads:  []gh.Thread{thread("T1", 12, "me"), {ID: "T2", IsResolved: true, Last: gh.Comment{ID: 5, Author: "x"}}, thread("T3", 13, "alice")},
		comments: []gh.Comment{{ID: 1, Author: "me"}, {ID: 2, Author: "greptile-apps[bot]"}},
		checks:   []gh.Check{{Name: "ci", State: "IN_PROGRESS"}, {Name: "lint", State: "SUCCESS"}},
	}
	var keys []string
	for _, ev := range triggers(pr, rd, "me") {
		keys = append(keys, ev.Key)
	}
	want := []string{"babysit:thread:T3:13", "babysit:review:bob:0001-01-01T00:00:00Z", "babysit:comment:2"}
	if !slices.Equal(keys, want) {
		t.Errorf("trigger keys = %q, want %q", keys, want)
	}
}

func TestDMedLogins(t *testing.T) {
	it := &entity{evs: []events.Event{
		{Kind: events.KindDone, Sender: "e1-w1", Text: "dm @alice: fixed 2"},
		{Kind: events.KindDone, Sender: "e1-w1", Text: "DM bob"},
		{Kind: events.KindDone, Sender: "e1-w1", Text: "dm alice again"},
		{Kind: events.KindIntent, Sender: "e1-w1", Text: "dm carol"},
		{Kind: events.KindDone, Sender: events.SenderProgram, Text: "dm dave"},
		{Kind: events.KindDone, Sender: "e1-w1", Text: "push abc"},
	}}
	if got := dmedLogins(it); !slices.Equal(got, []string{"alice", "bob"}) {
		t.Errorf("dmedLogins = %q", got)
	}
}

func TestABabysitterWaitsForTheHandover(t *testing.T) {
	w := newWorld(t)
	w.registryMerging(true)
	w.planned("e1", oneItem("[]"))
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree(item)
	w.prs[git.Branch(item)] = gh.PR{ID: "PR_5", Number: 5, URL: prURL, State: "OPEN", HeadRefName: git.Branch(item), HeadRefOid: head1,
		BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE"}
	w.moveTo(item, workPROpen)
	w.recordPR(head1, w.now)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.fullNext()
	w.tick(false)
	if got := slices.DeleteFunc(w.called("claude --bg"), func(c string) bool { return !strings.Contains(c, "factory:e1-w1:babysitter") }); len(got) != 0 || len(w.toBabysitter()) != 0 {
		t.Fatalf("babysat a worktree the orchestrator still holds: starts %q, requests %+v", got, w.toBabysitter())
	}
	w.append(w.dir(item), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture"})
	w.fullNext()
	w.tick(false)
	startArgs(t, w, "factory:e1-w1:babysitter")
}
