package reconcile

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
)

// sameSet is a plan with two items in one set: an orchestrator works w1,
// then w2.
const sameSet = `uat: none
issues:
  - {ref: o/r#12, result: ready}
  - {ref: o/r#13, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#12}
  - {id: w2, repo: r, issue: o/r#13}
sets:
  - {id: s1, items: [w1, w2]}
links: []
`

// withBlocker returns plan with item w3, for o/r#14, in its own set, and a
// gate: start link from it to w1: the planner's answer to w1's blocked_by.
func withBlocker(plan string) string {
	plan = strings.Replace(plan, "items:\n", "  - {ref: o/r#14, result: ready}\nitems:\n", 1)
	plan = strings.Replace(plan, "sets:\n", "  - {id: w3, repo: r, issue: o/r#14}\nsets:\n", 1)
	return strings.Replace(plan, "links: []", "  - {id: s3, items: [w3]}\nlinks: [{from: w3, to: w1, gate: start, when: merged, rollback: revert w1 too}]", 1)
}

// blockedBy logs a blocked_by from item id's session, as `factory event`
// does.
func (w *world) blockedBy(id, text string) events.Event {
	return w.append(w.dir(id), events.Event{Kind: events.KindBlockedBy, Sender: "e1-s1", Text: text})
}

// ticks runs n ticks, a minute apart.
func (w *world) ticks(n int) {
	w.t.Helper()
	for range n {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
}

// recheckAs ends epic e1's re-check with plan as epic.v2.yaml.
func (w *world) recheckAs(plan string) {
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", plan)
}

// blockW1 plans e1 with plan, puts w1 in state with s1 running and its
// orchestrator listed, and has w1's session send blocked_by. Two ticks
// later w1 is in its re-check. It returns what is posted to the
// orchestrator.
func (w *world) blockW1(plan, state string) func() []string {
	w.t.Helper()
	w.registry()
	w.planned("e1", plan)
	w.tick(false)
	posted := w.listen(w.orchestrator("e1-s1", "turn_ended", 4401, "done").PID)
	w.moveTo("e1-s1", "running")
	w.moveTo("e1-w1", state)
	w.blockedBy("e1-w1", "needs the retry helper in o/r#14 first")
	w.ticks(2)
	if got := w.status("e1-w1").State; got != workRechecking {
		w.t.Fatalf("e1-w1 is %s, want rechecking; moves %v", got, w.moves("e1-w1"))
	}
	return posted
}

func TestBlockedByRechecksOnlyThatItem(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.moveTo("e1-w1", workImplementing)
	w.moveTo("e1-w2", workImplementing)
	ev := w.blockedBy("e1-w1", "needs the retry helper in o/r#14 first")
	w.tick(false)
	if got := w.moves("e1-w1"); last(got) != "implementing→blocked" {
		t.Fatalf("e1-w1 moves %v, want it blocked", got)
	}
	w.ticks(3)
	key := blockerRef + "e1-w1:" + seqRef(ev)
	if got := w.signals("e1"); !slices.Equal(got, []string{key}) {
		t.Errorf("signals = %v, want one %s", got, key)
	}
	if got := w.status("e1"); got.State != epicChecking || got.Rechecks != 1 {
		t.Errorf("e1 = %s with %d re-checks, want one re-check", got.State, got.Rechecks)
	}
	if got := w.moves("e1-w1"); !slices.Equal(got[len(got)-2:], []string{"implementing→blocked", "blocked→rechecking"}) {
		t.Errorf("e1-w1 moves %v, want blocked, then rechecking", got)
	}
	if got := w.status("e1-w2").State; got != workImplementing {
		t.Errorf("e1-w2 is %s, want it to run on", got)
	}
	inbox := strings.Join(w.inbox("e1"), "\n")
	for _, want := range []string{"e1-w1 is blocked: needs the retry helper in o/r#14 first. You may add issues, items, sets and links", "Re-check only o/r#12;"} {
		if !strings.Contains(inbox, want) {
			t.Errorf("planner inbox %q lacks %q", inbox, want)
		}
	}
	w.noErrors()
}

// The planner adds the missing work as w3 and links it to w1: w1 waits in
// blocked until w3 merges, then goes back to work, its set runs again and
// the orchestrator is told to resume it.
func TestABlockerThatMergesSendsTheItemBackToWork(t *testing.T) {
	w := newWorld(t)
	posted := w.blockW1(twoItems("[]"), workImplementing)
	w.recheckAs(withBlocker(twoItems("[]")))
	w.ticks(2)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Fatalf("e1-w1 is %s while w3 is open, want blocked; moves %v", got, w.moves("e1-w1"))
	}
	if got := w.status("e1-s1").State; got != stateBlocked {
		t.Fatalf("e1-s1 is %s with its only item blocked, want blocked", got)
	}
	w.moveTo("e1-w3", stateMerged)
	w.ticks(2)
	if got := w.moves("e1-w1"); last(got) != "blocked→implementing" {
		t.Errorf("e1-w1 moves %v, want back to implementing once w3 merged", got)
	}
	if got := w.moves("e1-s1"); last(got) != "blocked→running" {
		t.Errorf("e1-s1 moves %v, want running again", got)
	}
	resume := "e1-w1's blocker landed; resume it from implementing"
	if got := w.inbox("e1-s1"); !slices.Contains(got, resume) {
		t.Errorf("orchestrator inbox = %q, want the resume message", got)
	}
	if got := posted(); !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, resume) }) {
		t.Errorf("posted to the orchestrator: %q", got)
	}
	w.noErrors()
}

// While w1 is blocked its set starts w2. w3 merges while w2 is being
// worked: w1 waits until w2 reaches pr_open, so the orchestrator never has
// two items at once.
func TestAReleasedItemWaitsForItsSetsActiveItem(t *testing.T) {
	w := newWorld(t)
	w.blockW1(sameSet, workImplementing)
	if got := w.status("e1-w2").State; got != stateStarting {
		t.Fatalf("e1-w2 is %s while e1-w1 is blocked, want starting", got)
	}
	w.moveTo("e1-w2", workImplementing)
	w.recheckAs(withBlocker(sameSet))
	w.ticks(1)
	w.moveTo("e1-w3", stateMerged)
	w.ticks(2)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Fatalf("e1-w1 is %s while e1-w2 is implementing, want blocked", got)
	}
	w.moveTo("e1-w2", workPROpen)
	w.ticks(1)
	if got := w.moves("e1-w1"); last(got) != "blocked→implementing" {
		t.Errorf("e1-w1 moves %v, want back to implementing once e1-w2 reached pr_open", got)
	}
	w.noErrors()
}

// The planner answers with an instruction during the re-check, not a link:
// it releases w1 once the re-check ends.
func TestAnInstructionDuringTheBlockerRecheckReleasesTheItem(t *testing.T) {
	w := newWorld(t)
	w.blockW1(twoItems("[]"), workImplementing)
	w.now = w.now.Add(time.Minute)
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "e1-w1: use the old helper"})
	w.recheckAs(twoItems("[]"))
	w.ticks(3)
	want := []string{"implementing→blocked", "blocked→rechecking", "rechecking→blocked", "blocked→implementing"}
	if got := w.moves("e1-w1"); !slices.Equal(got[len(got)-4:], want) {
		t.Errorf("moves %v, want them to end %v", got, want)
	}
}

// An instruction to the set that does not start with the blocked item's id
// is about other work: it leaves the item blocked.
func TestAnInstructionForOtherWorkLeavesTheItemBlocked(t *testing.T) {
	w := newWorld(t)
	w.blockW1(twoItems("[]"), workImplementing)
	w.now = w.now.Add(time.Minute)
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "use the old helper"})
	w.recheckAs(twoItems("[]"))
	w.ticks(3)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Fatalf("e1-w1 is %s after an instruction that does not name it, want blocked; moves %v", got, w.moves("e1-w1"))
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "e1-w1: use the old helper"})
	w.ticks(1)
	if got := w.moves("e1-w1"); last(got) != "blocked→implementing" {
		t.Errorf("e1-w1 moves %v, want back to implementing on the instruction that names it", got)
	}
}

// The instruction answer waits, like a released link, until no other item
// of the set is being worked.
func TestAnInstructedItemWaitsForItsSetsActiveItem(t *testing.T) {
	w := newWorld(t)
	w.blockW1(sameSet, workImplementing)
	w.moveTo("e1-w2", workImplementing)
	w.now = w.now.Add(time.Minute)
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "e1-w1: use the old helper"})
	w.recheckAs(sameSet)
	w.ticks(3)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Fatalf("e1-w1 is %s while e1-w2 is implementing, want blocked; moves %v", got, w.moves("e1-w1"))
	}
	w.moveTo("e1-w2", workPROpen)
	w.ticks(1)
	if got := w.moves("e1-w1"); last(got) != "blocked→implementing" {
		t.Errorf("e1-w1 moves %v, want back to implementing once e1-w2 reached pr_open", got)
	}
}

// A babysitter's blocked_by on an in_review item: once its link releases it
// goes back to in_review at once, though another item of its set is being
// worked, and the orchestrator is told nothing.
func TestAnInReviewItemGoesBackToReviewAtOnce(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", sameSet)
	w.tick(false)
	w.orchestrator("e1-s1", "turn_ended", 4401, "done")
	w.listen(w.orchestrator("e1-s1", "turn_ended", 4401, "done").PID)
	w.moveTo("e1-s1", "running")
	w.listen(w.inReview("e1-w1", 5).PID)
	w.moveTo("e1-w2", workImplementing)
	w.blockedBy("e1-w1", "needs o/r#14's API first")
	w.ticks(2)
	if got := w.status("e1-w1").State; got != workRechecking {
		t.Fatalf("e1-w1 is %s, want rechecking; moves %v", got, w.moves("e1-w1"))
	}
	w.recheckAs(withBlocker(sameSet))
	w.ticks(1)
	w.moveTo("e1-w3", stateMerged)
	w.ticks(1)
	if got := w.moves("e1-w1"); last(got) != "blocked→in_review" {
		t.Errorf("e1-w1 moves %v, want back to in_review while e1-w2 is implementing", got)
	}
	if got := w.inbox("e1-s1"); slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, "blocker landed") }) {
		t.Errorf("orchestrator inbox = %q, want no resume message", got)
	}
	w.noErrors()
}

// An item blocked past its restarts leaves only on an instruction, even
// when its plan has a released gate: start link into it.
func TestAnEscalatedItemIsNotReleasedByItsLinks(t *testing.T) {
	w := newWorld(t)
	w.escalate(time.Time{})
	w.moveTo("e1-w2", stateMerged)
	w.recheckAs(twoItems("[{from: w2, to: w1, gate: start, when: merged, rollback: revert w1 too}]"))
	w.ticks(3)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Errorf("e1-w1 is %s, want blocked until the planner instructs its set; moves %v", got, w.moves("e1-w1"))
	}
}

// A gate: start link the plan had before the blocked_by, released already,
// does not release the item: the planner's re-check answers it first.
func TestAnOldReleasedLinkWaitsForTheBlockerRecheck(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[{from: w2, to: w1, gate: start, when: merged, rollback: revert w1 too}]"))
	w.tick(false)
	w.moveTo("e1-w2", stateMerged)
	w.moveTo("e1-w1", workImplementing)
	w.blockedBy("e1-w1", "needs o/r#14 first")
	w.ticks(2)
	if got := w.moves("e1-w1"); slices.Contains(got, "blocked→implementing") || last(got) != "blocked→rechecking" {
		t.Errorf("e1-w1 moves %v, want blocked until its re-check, then rechecking", got)
	}
	w.recheckAs(twoItems("[{from: w2, to: w1, gate: start, when: merged, rollback: revert w1 too}]"))
	w.ticks(3)
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Errorf("e1-w1 is %s once a re-check kept only the old link, want blocked; moves %v", got, w.moves("e1-w1"))
	}
}

// blockInReview puts e1-w1 in in_review with PR 5, a thread its babysitter
// answered and left open, and has the babysitter send blocked_by. Two ticks
// later e1-w1 is in its re-check. It returns what is posted to the
// babysitter.
func (w *world) blockInReview() func() []string {
	w.t.Helper()
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	posted := w.listen(w.inReview("e1-w1", 5).PID)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "me")}
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindBlockedBy, Sender: "e1-w1", Text: "needs o/r#14's API first"})
	w.ticks(2)
	if got := w.status("e1-w1").State; got != workRechecking {
		w.t.Fatalf("e1-w1 is %s, want rechecking; moves %v", got, w.moves("e1-w1"))
	}
	return posted
}

// landed returns the requests to e1-w1's babysitter that say its blocker
// landed.
func (w *world) landed() []string {
	var out []string
	for _, ev := range w.toBabysitter() {
		if strings.HasPrefix(ev.Text, "e1-w1's blocker landed") {
			out = append(out, ev.Text)
		}
	}
	return out
}

// The babysitter left its threads open when it sent blocked_by. Back in
// review, nothing on the PR is new, so the program wakes it, once.
func TestABabysitterIsWokenOnceItsBlockerLands(t *testing.T) {
	w := newWorld(t)
	posted := w.blockInReview()
	w.recheckAs(withBlocker(twoItems("[]")))
	w.ticks(1)
	w.moveTo("e1-w3", stateMerged)
	w.ticks(3)
	if got := w.moves("e1-w1"); last(got) != "blocked→in_review" {
		t.Fatalf("e1-w1 moves %v, want back to in_review", got)
	}
	want := "e1-w1's blocker landed: finish the threads you left open"
	if got := w.landed(); !slices.Equal(got, []string{want}) {
		t.Errorf("requests to the babysitter = %q, want one: %s", got, want)
	}
	if got := posted(); !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, want) }) {
		t.Errorf("posted to the babysitter: %q, want the wake", got)
	}
	w.noErrors()
}

// Released by an instruction, the babysitter also gets its text.
func TestAnInstructedInReviewItemPassesTheInstructionToItsBabysitter(t *testing.T) {
	w := newWorld(t)
	w.blockInReview()
	w.now = w.now.Add(time.Minute)
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "e1-w1: the API change is out of scope; say so"})
	w.recheckAs(twoItems("[]"))
	w.ticks(3)
	if got := w.moves("e1-w1"); last(got) != "blocked→in_review" {
		t.Fatalf("e1-w1 moves %v, want back to in_review", got)
	}
	want := "e1-w1's blocker landed: finish the threads you left open. Instruction: e1-w1: the API change is out of scope; say so"
	if got := w.landed(); !slices.Equal(got, []string{want}) {
		t.Errorf("requests to the babysitter = %q, want one: %s", got, want)
	}
}

// Only the item's own orchestrator or babysitter blocks it.
func TestBlockedByFromAnotherSessionIsIgnored(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.moveTo("e1-w1", workImplementing)
	for _, sender := range []string{"e1-s2", "e1-w2", "user"} {
		w.append(w.dir("e1-w1"), events.Event{Kind: events.KindBlockedBy, Sender: sender, Text: "not mine to say"})
	}
	w.ticks(2)
	if got := w.status("e1-w1").State; got != workImplementing {
		t.Fatalf("e1-w1 is %s after blocked_by from other sessions, want implementing; moves %v", got, w.moves("e1-w1"))
	}
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindBlockedBy, Sender: "e1-w1", Text: "needs o/r#14 first"})
	w.tick(false)
	if got := w.moves("e1-w1"); last(got) != "implementing→blocked" {
		t.Errorf("e1-w1 moves %v, want blocked by its own session", got)
	}
}

// Two blocked_by moves on one item, each answered by an instruction; the
// third goes to the user, with one DM, and starts no re-check.
func TestAThirdBlockedByGoesToTheUser(t *testing.T) {
	w := newWorld(t)
	w.blockW1(twoItems("[]"), workImplementing)
	for v := 2; v <= 3; v++ {
		w.now = w.now.Add(time.Minute)
		w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "e1-w1: go on"})
		w.end("e1", fmt.Sprintf("state-check.v%d.md", v), stateCheck)
		w.end("e1", fmt.Sprintf("epic.v%d.yaml", v), twoItems("[]"))
		w.ticks(3)
		if got := w.status("e1-w1").State; got != workImplementing {
			t.Fatalf("e1-w1 is %s after re-check %d, want implementing; moves %v", got, v, w.moves("e1-w1"))
		}
		if v == 2 {
			w.blockedBy("e1-w1", "a second blocker")
			w.ticks(2)
		}
	}
	w.blockedBy("e1-w1", "a third blocker")
	w.ticks(3)
	if got := w.moves("e1-w1"); last(got) != "implementing→needs_you" {
		t.Fatalf("e1-w1 moves %v, want needs_you on the third blocked_by", got)
	}
	want := "factory: e1-w1 needs you: its sessions sent blocked_by 3 times; the last: a third blocker. When it is settled: factory retry e1-w1, or factory stop e1-w1 --reason \"<why>\"."
	if got := w.dmsWith("sent blocked_by"); !slices.Equal(got, []string{want}) {
		t.Errorf("DMs = %q, want one:\n%s", got, want)
	}
	if got := w.signals("e1"); len(got) != 2 {
		t.Errorf("signals = %v, want the first two blockers only", got)
	}
}
