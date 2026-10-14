package reconcile

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

const redPass = "## Outcome\nopen — CI red\n## Comments\nnone\n## CI\ngovulncheck fails\n"

// inReviewOnRedBase puts e1-w1 in in_review as inReviewWith does, its
// head red on govulncheck like o/r@main, which is red on it since t0.
func inReviewOnRedBase(t *testing.T) *world {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.baseIs("FAILURE", "aaa1111", red("govulncheck"))
	w.pulls.checks[head1] = []gh.Check{{Name: "govulncheck", State: "FAILURE", Required: true}, {Name: "ci", State: "SUCCESS", Required: true}}
	return w
}

// messagesKeyed returns the messages on id's log whose key starts with prefix.
func (w *world) messagesKeyed(id, prefix string) []string {
	var out []string
	for _, ev := range w.kinds(id, events.KindMessage) {
		if strings.HasPrefix(ev.Key, prefix) {
			out = append(out, ev.Text)
		}
	}
	return out
}

func TestAPRRedOnlyWhereItsBaseIsRedIsLeftToTheRepair(t *testing.T) {
	w := inReviewOnRedBase(t)
	w.tick(false)
	if got := w.toBabysitter(); len(got) != 0 {
		t.Fatalf("requests to the babysitter = %+v, want none for a check red on the base too", got)
	}
	if got := w.status(item).MergeHold; got != "waiting on o/r@main" {
		t.Errorf("hold = %q before the repair epic", got)
	}
	w.fullTickAt(redAfter)
	id := w.repairEpics()[0]
	if got := w.status(item).MergeHold; got != "waiting on repair "+id {
		t.Errorf("hold = %q, want it to name %s", got, id)
	}

	// lint goes red too: it alone wakes the babysitter.
	w.pulls.checks[head1] = []gh.Check{{Name: "govulncheck", State: "FAILURE", Required: true}, {Name: "lint", State: "FAILURE", Required: true}, {Name: "ci", State: "SUCCESS", Required: true}}
	w.fullTickAt(redAfter + 5*time.Minute)
	if got := w.toBabysitter(); len(got) != 1 || got[0].Text != "red checks on 1111111: lint" {
		t.Fatalf("requests to the babysitter = %+v, want one naming only lint", got)
	}
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	if want := "red on the base too: govulncheck red on main; repair " + id + " owns them\n"; !strings.Contains(args[len(args)-1], want) {
		t.Errorf("prompt lacks %q:\n%s", want, args[len(args)-1])
	}

	// The pass ends; lint passes on a re-run and only govulncheck stays red.
	w.end(item, "babysit.v1.md", redPass)
	w.pulls.checks[head1] = []gh.Check{{Name: "govulncheck", State: "FAILURE", Required: true}, {Name: "lint", State: "SUCCESS", Required: true}, {Name: "ci", State: "SUCCESS", Required: true}}
	w.fullTickAt(redAfter + 10*time.Minute)
	if st, dms := w.status(item).State, w.dmsWith("stayed red"); st != workInReview || len(dms) != 0 {
		t.Errorf("state %s, DMs %q; want it left in in_review for the repair", st, dms)
	}
	if got := w.status(item).MergeHold; got != "waiting on repair "+id {
		t.Errorf("hold = %q after the pass", got)
	}
}

func TestARepairsOwnPRIsNotLeftToItself(t *testing.T) {
	w := inReviewOnRedBase(t)
	if err := store.WriteInputs(w.dir("e1"), &EpicInputs{ID: "e1", Kind: epicKindRepair, Repo: "o/r", Base: "main", Input: "o/r@main is red", AddedAt: t0}); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.toBabysitter(); len(got) != 1 || got[0].Text != "red checks on 1111111: govulncheck" {
		t.Fatalf("requests to the babysitter = %+v, want the red check", got)
	}
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	if prompt := args[len(args)-1]; strings.Contains(prompt, "red on the base too") {
		t.Errorf("the repair's babysitter is told a repair owns its checks:\n%s", prompt)
	}
	if got := w.status(item).MergeHold; got != "its required checks are not green on 1111111" {
		t.Errorf("hold = %q", got)
	}
	w.end(item, "babysit.v1.md", redPass)
	w.fullTickAt(5 * time.Minute)
	if st, dms := w.status(item).State, w.dmsWith("stayed red"); st != stateNeedsYou || len(dms) != 1 {
		t.Errorf("state %s, DMs %q; want needs_you and one DM", st, w.dms)
	}
}

func TestABabysitterWokenDuringARepairIsToldWhatTheRepairOwns(t *testing.T) {
	w := inReviewOnRedBase(t)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.tick(false)
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	prompt := args[len(args)-1]
	for _, want := range []string{"woken by: alice commented on a review thread", "red on the base too: govulncheck red on main; the factory's repair owns them\n"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
}

func TestOrchestratorsInARedRepoAreToldOnceAndPRsUpdatedOnceWhenGreen(t *testing.T) {
	w := inReviewOnRedBase(t)
	w.pr(func(pr *gh.PR) { pr.MergeStateStatus = "BLOCKED" })
	posted := w.listen(w.babysitter(1, sessionRunning, 5300, listedWorking).PID)
	// e2-s1 works on two items in o/r, e3-s1 on one in o/q; e4-s1 has no
	// orchestrator.
	w.epic("e2", "running")
	w.set("e2", "e2-s1", "running", "e2-w1", "e2-w2")
	w.item("e2", "e2-s1", "e2-w1", workImplementing)
	w.item("e2", "e2-s1", "e2-w2", "exploring")
	w.orchestrator("e2-s1", sessionRunning, 5400, listedWorking)
	w.epic("e3", "running")
	w.set("e3", "e3-s1", "running", "e3-w1")
	w.item("e3", "e3-s1", "e3-w1", workImplementing, func(in *WorkInputs) { in.Repo = "o/q" })
	w.orchestrator("e3-s1", sessionRunning, 5500, listedWorking)
	w.epic("e4", "running")
	w.set("e4", "e4-s1", "running", "e4-w1")
	w.item("e4", "e4-s1", "e4-w1", workImplementing)

	w.tick(false)
	w.fullTickAt(5 * time.Minute)
	note := "govulncheck are red on o/r@main too; the factory's repair owns them; do not fix them in this item"
	if got := w.messagesKeyed("e2-s1", "repair-note:"); !slices.Equal(got, []string{note}) {
		t.Errorf("e2-s1 notes = %q, want one", got)
	}
	for _, set := range []string{"e1-s1", "e3-s1", "e4-s1"} {
		if got := w.messagesKeyed(set, "repair-"); len(got) != 0 {
			t.Errorf("%s got %q", set, got)
		}
	}

	w.fullTickAt(redAfter)
	id := w.repairEpics()[0]
	w.planRepair(id, repairPlan)
	repairSet, repairItem := entityID(id, "s1"), entityID(id, "w1")
	w.moveTo(repairItem, workImplementing)
	w.orchestrator(repairSet, sessionRunning, 5600, listedWorking)
	w.fullTickAt(redAfter + time.Minute)
	if got := w.messagesKeyed(repairSet, "repair-"); len(got) != 0 {
		t.Errorf("the repair's own orchestrator got %q", got)
	}

	w.baseIs("SUCCESS", "bbb2222", passed("govulncheck"))
	w.fullTickAt(redAfter + 5*time.Minute)
	if got := w.messagesKeyed("e2-s1", "repair-green:"); !slices.Equal(got, []string{"o/r@main is green again; merge origin/main before your next push"}) {
		t.Errorf("e2-s1 green messages = %q, want one", got)
	}
	for _, set := range []string{"e1-s1", "e3-s1", "e4-s1", repairSet} {
		if got := w.messagesKeyed(set, "repair-green:"); len(got) != 0 {
			t.Errorf("%s got %q", set, got)
		}
	}
	update := "gh api -X PUT repos/o/r/pulls/5/update-branch -f expected_head_sha=" + head1
	if got := w.called("gh api -X PUT"); !slices.Equal(got, []string{update}) {
		t.Fatalf("updates = %q, want one", got)
	}

	// GitHub merged main into the branch; its new head is pending.
	w.pr(func(pr *gh.PR) { pr.HeadRefOid, pr.BaseRefOid = head2, base2 })
	w.pulls.checks[head2] = []gh.Check{{Name: "govulncheck", State: "IN_PROGRESS", Required: true}}
	w.fullTickAt(redAfter + 10*time.Minute)
	if got := w.called("gh api -X PUT"); len(got) != 1 {
		t.Errorf("updates = %q after a second green tick, want still one", got)
	}
	if got := w.messagesKeyed("e2-s1", "repair-"); len(got) != 2 {
		t.Errorf("e2-s1 messages = %q, want the note and one green", got)
	}
	if got := posted(); len(got) != 0 {
		t.Errorf("the babysitter was woken: %q", got)
	}
	if got := w.toBabysitter(); len(got) != 0 {
		t.Errorf("requests to the babysitter = %+v", got)
	}
	if got := slices.DeleteFunc(w.called("claude --bg"), func(c string) bool { return !strings.Contains(c, ":babysitter") }); len(got) != 0 {
		t.Errorf("babysitter starts = %q", got)
	}
}

func TestAPRRedBeyondItsBaseIsNotUpdatedWhenTheBaseTurnsGreen(t *testing.T) {
	w := inReviewOnRedBase(t)
	w.pulls.checks[head1] = append(w.pulls.checks[head1], gh.Check{Name: "lint", State: "FAILURE", Required: true})
	w.tick(false)
	w.baseIs("SUCCESS", "bbb2222", passed("govulncheck"))
	w.fullTickAt(5 * time.Minute)
	if eps := w.episodes(); len(eps) != 0 {
		t.Fatalf("episodes = %+v, want it closed", eps)
	}
	if got := w.called("gh api -X PUT"); len(got) != 0 {
		t.Errorf("updates = %q, want none for a PR red on lint", got)
	}
}
