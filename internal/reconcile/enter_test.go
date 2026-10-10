package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

// failingDM fails its first fail DMs, as a Slack outage would, then
// delivers to the world.
type failingDM struct {
	w     *world
	fail  int
	tried int
}

func (f *failingDM) DM(ctx context.Context, text string) error {
	f.tried++
	if f.tried <= f.fail {
		return errors.New("slack: 503")
	}
	return f.w.DM(ctx, text)
}

// tickWith runs one tick on d, a minute after the last.
func (w *world) tickWith(d Deps) {
	w.t.Helper()
	if err := Tick(context.Background(), d, false); err != nil {
		w.t.Fatalf("tick: %v\n%s", err, w.out.String())
	}
	w.now = w.now.Add(time.Minute)
}

func TestAFailedQuestionDMIsSentOnALaterTick(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "exploring")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which DB?"})
	d := w.deps()
	d.Notify = &failingDM{w: w, fail: 1}
	w.tickWith(d)
	if st := w.status("e1-w3").State; st != stateWaitingUser || len(w.dms) != 0 {
		t.Fatalf("e1-w3 is %s with DMs %q, want waiting_user and the DM failed", st, w.dms)
	}
	for range 3 {
		w.tickWith(d)
	}
	if got := w.dmsWith("Which DB?"); len(got) != 1 {
		t.Errorf("DMs = %q, want the question once", w.dms)
	}
}

func TestAReturnToWaitingUserAsksNothingAgain(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "exploring")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which DB?"})
	w.tick(false)
	w.moveTo("e1-w3", workRechecking)
	w.moveTo("e1-w3", stateWaitingUser)
	w.tick(false)
	if len(w.dms) != 1 {
		t.Errorf("DMs = %q, want the question once", w.dms)
	}
	w.noErrors()
}

func TestAFailedDequeueOnARecheckIsTriedAgain(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.pulls.queues["o/r@main"] = true
	w.moveTo(item, workMerging)
	w.append(w.dir(item), events.Event{Kind: events.KindIntent, Text: "merge " + head1 + " by queue", Key: "merge:1:intent"})
	failed := false
	orig := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == "gh" && strings.Contains(strings.Join(c.Args, " "), "dequeuePullRequest(") && !failed {
			failed = true
			return nil, errors.New("HTTP 502: Bad Gateway")
		}
		return orig(c)
	}
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		w.tick(false)
		w.now = w.now.Add(time.Minute)
	}
	if got := w.status(item).State; got != workRechecking {
		t.Fatalf("%s is %s, want rechecking", item, got)
	}
	if got := w.called("gh api graphql -f query=mutation($id:ID!){dequeuePullRequest("); len(got) != 2 {
		t.Errorf("dequeues = %d, want the failed one and one more", len(got))
	}
}

func TestAFailedNeedsYouDMIsSentOnALaterTick(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(w *world)
		want string
	}{
		{"a closed PR", func(w *world) { w.pr(func(pr *gh.PR) { pr.State = "CLOSED" }) }, "was closed without merging"},
		{"red after a babysitter pass", func(w *world) {
			w.babysitter(1, sessionTurnEnded, 5400, listedTurnEnded)
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
			w.tick(false)
			w.end(item, "babysit.v1.md", "## Outcome\nopen — CI red\n## Comments\nnone\n## CI\nci fails\n")
			w.now = w.now.Add(5 * time.Minute)
			w.fullNext()
		}, "stayed red after a babysitter pass"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := watching(t)
			tc.make(w)
			d := w.deps()
			d.Notify = &failingDM{w: w, fail: 1}
			w.tickWith(d)
			if st := w.status(item).State; st != workNeedsYou || len(w.dms) != 0 {
				t.Fatalf("%s is %s with DMs %q, want needs_you and the DM failed", item, st, w.dms)
			}
			for range 2 {
				w.tickWith(d)
			}
			if got := w.dmsWith(tc.want); len(got) != 1 {
				t.Errorf("DMs = %q, want one saying it %s", w.dms, tc.want)
			}
		})
	}
}

// mergedWithWorktree puts e1-w1 in merged, with the worktree it was given,
// and nothing asked of the repo worker since: a tick that died between the
// move and its cleanup request leaves that.
func mergedWithWorktree(t *testing.T) *world {
	w := newWorld(t)
	w.working(stateMerged)
	return w
}

// cleanups returns the item's cleanup requests to the repo worker.
func (w *world) cleanups(id string) []events.Event {
	var out []events.Event
	for _, ev := range w.kinds(id, events.KindRequest) {
		if ev.Recipient == events.RecipientRepoWorker && ev.Request == cleanupRequest {
			out = append(out, ev)
		}
	}
	return out
}

// answerCleanup answers the item's newest cleanup request as the repo
// worker would: done, or an error.
func (w *world) answerCleanup(id string, ok bool) {
	w.t.Helper()
	reqs := w.cleanups(id)
	req := reqs[len(reqs)-1]
	res := events.Event{Kind: events.KindDone, Text: cleanupRequest, TriggerRef: seqRef(req), Key: repoResultKey(req)}
	if !ok {
		res.Kind, res.Text = events.KindError, "cleanup: git worktree remove: exit status 128"
	}
	w.append(w.dir(id), res)
}

func TestAMergedItemAsksForItsCleanupAfterACrash(t *testing.T) {
	w := mergedWithWorktree(t)
	w.tick(false)
	if got := w.cleanups("e1-w1"); len(got) != 1 {
		t.Fatalf("cleanup requests = %+v, want one", got)
	}
	w.answerCleanup("e1-w1", true)
	w.tick(false)
	if st := w.status("e1-w1").State; st != "done" {
		t.Errorf("e1-w1 is %s, want done once cleaned up", st)
	}
}

func TestAFailedCleanupIsAskedAgainWithBackoffThenDMs(t *testing.T) {
	w := mergedWithWorktree(t)
	w.tick(false)
	// The worker fails each cleanup; the tick asks again 1, 5, then 15
	// minutes after the failed one.
	var asked []time.Duration
	for m := 0; m <= 40; m++ {
		w.at(time.Duration(m) * time.Minute)
		if reqs := w.cleanups("e1-w1"); len(reqs) > len(asked) {
			asked = append(asked, reqs[len(reqs)-1].At.Sub(t0))
			if len(reqs) <= 3 {
				w.answerCleanup("e1-w1", false)
			}
		}
		w.tick(false)
	}
	want := []time.Duration{0, time.Minute, 6 * time.Minute, 21 * time.Minute}
	if len(asked) != len(want) {
		t.Fatalf("cleanups asked at %v, want %v", asked, want)
	}
	for i := range want {
		if asked[i] != want[i] {
			t.Errorf("cleanups asked at %v, want %v", asked, want)
			break
		}
	}
	if st := w.status("e1-w1").State; st != stateMerged {
		t.Errorf("e1-w1 is %s while its cleanup fails, want merged", st)
	}
	if got := w.dmsWith("e1-w1, merged, failed 3 times"); len(got) != 1 || len(w.dms) != 1 {
		t.Errorf("DMs = %q, want one after the third failure", w.dms)
	}
	w.answerCleanup("e1-w1", true)
	w.tick(false)
	if st := w.status("e1-w1").State; st != "done" {
		t.Errorf("e1-w1 is %s, want done once a cleanup succeeds", st)
	}
}

func TestATakenBackMergeIsNotReadAgain(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.moveTo(item, workMerging)
	w.append(w.dir(item), events.Event{Kind: events.KindIntent, Text: "merge " + head1 + " by squash", Key: "merge:1:intent"})
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status(item).State; got != workRechecking {
		t.Fatalf("%s is %s, want rechecking", item, got)
	}
	before := len(w.called("gh pr view 5"))
	for range 3 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if got := len(w.called("gh pr view 5")) - before; got != 0 {
		t.Errorf("read PR 5 %d more times on plain ticks; its auto-merge was found off once", got)
	}
}
