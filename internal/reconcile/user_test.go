package reconcile

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// refusedAs checks that err is a refusal whose text has want, and that the
// entity's log holds the request it refused and one refused event naming it.
func (w *world) refusedAs(id string, err error, want string) {
	w.t.Helper()
	var refused RefusedError
	if !errors.As(err, &refused) || !strings.Contains(err.Error(), want) {
		w.t.Fatalf("%s: err = %v, want a refusal with %q", id, err, want)
	}
	req := last(w.kinds(id, events.KindRequest))
	got := w.kinds(id, events.KindRefused)
	if len(got) != 1 || got[0].TriggerRef != seqRef(req) || !strings.Contains(got[0].Text, want) {
		w.t.Fatalf("%s: refused events = %+v, want one for request %d", id, got, req.Seq)
	}
}

func TestAStopOnAnEpicCancelsOnlyItsWork(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.planned("e2", oneItem("[]"))
	planner1 := w.planner("e1", "running", 4100, "working")
	orch1 := w.orchestrator("e1-s1", "running", 4200, "working")
	w.planner("e2", "running", 4300, "working")
	w.orchestrator("e2-s1", "running", 4400, "working")

	if err := Stop(context.Background(), w.deps(), "e1", "the plan changed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	for _, id := range []string{"e1", "e1-s1", "e1-s2", "e1-w1", "e1-w2"} {
		if got := w.status(id).State; got != workCancelled {
			t.Errorf("%s is %s, want cancelled", id, got)
		}
	}
	for _, id := range []string{"e2", "e2-s1", "e2-w1"} {
		if got := w.status(id).State; got == workCancelled {
			t.Errorf("%s is cancelled: the stop covers e1 only", id)
		}
	}
	if got := w.called("claude stop"); !slices.Equal(got, []string{"claude stop " + planner1.ID, "claude stop " + orch1.ID}) {
		t.Errorf("stops = %q, want e1's planner and orchestrator only", got)
	}
	if got := w.inbox("e1-s1"); !slices.ContainsFunc(got, func(s string) bool {
		return strings.Contains(s, "e1-w1 is cancelled (factory stop: the plan changed)")
	}) {
		t.Errorf("orchestrator inbox = %q, want the item's cancel with the stop's reason", got)
	}
	w.tick(false)
	for _, id := range []string{"e1-planner", "e1-s1-orchestrator"} {
		if got := w.status(id).State; got != "finished" {
			t.Errorf("%s is %s, want finished", id, got)
		}
	}
	if got := w.status("e2-planner").State; got != "running" {
		t.Errorf("e2-planner is %s, want running", got)
	}
	w.noErrors()
}

func TestAStopOnASetCancelsOnlyItsItems(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	if err := Stop(context.Background(), w.deps(), "e1-s1", "not this one"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status("e1-w1").State; got != workCancelled {
		t.Errorf("e1-w1 is %s, want cancelled", got)
	}
	for _, id := range []string{"e1", "e1-s2", "e1-w2"} {
		if got := w.status(id).State; got == workCancelled {
			t.Errorf("%s is cancelled; the stop covers e1-s1 only", id)
		}
	}
}

func TestAStopWrittenMidTickEndsCancelledTheNextTick(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	w.planner("e1", "running", 4100, "working")
	stopped := false
	respond := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && c.Args[0] == "agents" && !stopped {
			stopped = true
			if err := Stop(context.Background(), w.deps(), "e1-w1", "mid-tick"); err != nil {
				t.Errorf("stop: %v", err)
			}
		}
		return respond(c)
	}
	w.tick(false)
	if !stopped {
		t.Fatal("the tick listed no sessions, so the stop was not written mid-tick")
	}
	if got := w.status("e1-w1").State; got == workCancelled {
		t.Fatal("e1-w1 is cancelled by the tick that was running when the stop was written: the test proves nothing")
	}
	w.tick(false)
	if got := w.status("e1-w1").State; got != workCancelled {
		t.Errorf("e1-w1 is %s after the next tick, want cancelled", got)
	}
}

func TestRetryAndStopInAStateTheBlueprintRefuses(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	ctx := context.Background()

	// needs_you with its PR closed: retry_allowed does not hold.
	w.moveTo("e1-w1", workNeedsYou)
	st := w.status("e1-w1")
	st.PR, st.PRState = 31, prClosed
	if err := store.WriteStatus(w.dir("e1-w1"), st); err != nil {
		t.Fatal(err)
	}
	err := Retry(ctx, w.deps(), "e1-w1")
	w.refusedAs("e1-w1", err, "needs_you → previous on user: guard retry_allowed does not hold")
	if !strings.Contains(err.Error(), "unless its PR is closed") {
		t.Errorf("refusal %q lacks the blueprint's exits for needs_you", err)
	}
	w.tick(false)
	if got := w.status("e1-w1").State; got != workNeedsYou {
		t.Errorf("e1-w1 is %s, want needs_you still", got)
	}
	if got := w.kinds("e1-w1", events.KindRefused); len(got) != 1 {
		t.Errorf("refused events after a tick = %d, want the one the command logged", len(got))
	}

	// implementing has no move back on a retry.
	w.moveTo("e1-w2", "implementing")
	w.refusedAs("e1-w2", Retry(ctx, w.deps(), "e1-w2"), "implementing → previous on user: not an allowed move")

	// An ended set takes no stop.
	w.moveTo("e1-s2", "done")
	w.refusedAs("e1-s2", Stop(ctx, w.deps(), "e1-s2", "too late"), "done → cancelled on user: not an allowed move")
}

func TestARetryAllowedNowIsLeftForTheTick(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindTransition, From: "queued", To: workNeedsYou, Prev: []string{"implementing"}, Trigger: "tick", TriggerRef: "fixture"})
	if err := Retry(context.Background(), w.deps(), "e1-w1"); err != nil {
		t.Fatal(err)
	}
	if got := w.kinds("e1-w1", events.KindRefused); len(got) != 0 {
		t.Fatalf("refused = %+v", got)
	}
	w.tick(false)
	if got := w.status("e1-w1").State; got != "implementing" {
		t.Errorf("e1-w1 is %s, want back in implementing", got)
	}
}

func TestAnAnswerToAnEndedEpicIsRefused(t *testing.T) {
	w := newWorld(t)
	w.registry()
	plan := strings.Replace(oneItem("[]"), "  - {ref: o/r#14, result: ready}", "  - {ref: o/r#14, result: ready}\n  - {ref: o/r#30, result: yours, why: \"your PR\", answers: [take over, leave]}", 1)
	w.planned("e1", plan)
	w.moveTo("e1", "done")
	_, err := AnswerTo(context.Background(), w.deps(), "e1", "o/r#30", "leave")
	w.refusedAs("e1", err, "done → checking on user: not an allowed move")
	if got := w.signals("e1"); len(got) != 0 {
		t.Errorf("signals = %q, want none for a refused answer", got)
	}
}

func TestAnAnswerTakesAnEpicOutOfNeedsYou(t *testing.T) {
	w := newWorld(t)
	w.registry()
	plan := strings.Replace(oneItem("[]"), "  - {ref: o/r#14, result: ready}", "  - {ref: o/r#14, result: ready}\n  - {ref: o/r#30, result: yours, why: \"your PR\", answers: [take over, leave]}", 1)
	w.planned("e1", plan)
	w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, From: epicPlanned, To: stateNeedsYou, Prev: []string{epicPlanned}, Trigger: "tick", TriggerRef: "fixture"})
	if _, err := AnswerTo(context.Background(), w.deps(), "e1", "o/r#30", "leave"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status("e1").State; got != epicChecking {
		t.Fatalf("e1 is %s, want checking", got)
	}
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", strings.Replace(plan, `result: yours, why: "your PR", answers: [take over, leave]`, `result: elsewhere, why: "you keep it"`, 1))
	w.tick(false)
	if got := w.status("e1").State; got == stateNeedsYou || got == epicChecking {
		t.Errorf("e1 is %s after the re-check, want it back where it was before needs_you; moves %v", got, w.moves("e1"))
	}
	if got := w.moves("e1"); !slices.Contains(got, "needs_you→planned") {
		t.Errorf("moves = %v, want the answer to take e1 out of needs_you", got)
	}
}

func TestCancellingAnAdoptedPRInTheMergeQueueDequeuesIt(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(strings.Replace(oneItem("[]"), "{id: w1, repo: r, issue: o/r#14}", "{id: w1, repo: r, issue: o/r#14, pr: 5}", 1), 20*time.Minute)
	w.pulls.queues["o/r@main"] = true
	w.moveTo(item, workMerging)
	w.append(w.dir(item), events.Event{Kind: events.KindIntent, Text: "merge " + head1 + " by queue", Key: "merge:1:intent"})
	if err := Stop(context.Background(), w.deps(), item, "not needed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status(item).State; got != workCancelled {
		t.Fatalf("%s is %s", item, got)
	}
	if got := w.called("gh api graphql -f query=mutation($id:ID!){dequeuePullRequest("); len(got) != 1 {
		t.Errorf("dequeues = %q, want one", got)
	}
	if got := w.called("gh pr close"); len(got) != 0 {
		t.Errorf("closed the user's PR: %v", got)
	}
	if w.prs[git.Branch(item)].State != "OPEN" {
		t.Error("the adopted PR is not open")
	}
}
