package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

// oneItem is a plan for issue o/r#14 alone, with links.
func oneItem(links string) string {
	return `uat: none
issues:
  - {ref: o/r#14, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#14}
sets:
  - {id: s1, items: [w1]}
links: ` + links + "\n"
}

// dmsWith returns the DMs that contain text.
func (w *world) dmsWith(text string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, dm := range w.dms {
		if strings.Contains(dm, text) {
			out = append(out, dm)
		}
	}
	return out
}

func TestADependentOfANotCodeIssueWaitsUntilItIsClosedAsCompleted(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", `uat: none
issues:
  - {ref: o/r#14, result: ready}
  - {ref: o/r#20, result: not_code, why: "a person signs the contract"}
items:
  - {id: w1, repo: r, issue: o/r#14}
sets:
  - {id: s1, items: [w1]}
links:
  - {from: o/r#20, to: w1, gate: start, when: merged, rollback: none}
`)
	if got := w.dmsWith("e1 o/r#20 is not_code: a person signs the contract"); len(got) != 1 {
		t.Errorf("not_code DMs = %q", w.dms)
	}
	w.tick(false)
	if got := w.status("e1-w1").State; got != workQueued {
		t.Fatalf("e1-w1 is %s while o/r#20 is open, want queued", got)
	}
	w.hub.issue("o/r", 20).State, w.hub.issue("o/r", 20).StateReason = "closed", "not_planned"
	w.tick(false)
	if got := w.status("e1-w1").State; got != workQueued {
		t.Fatalf("e1-w1 is %s with o/r#20 closed as not planned, want queued", got)
	}
	w.hub.issue("o/r", 20).StateReason = "completed"
	w.tick(false)
	if got := w.status("e1-w1").State; got != stateStarting {
		t.Errorf("e1-w1 is %s once o/r#20 closed as completed, want starting", got)
	}
	if got := w.dmsWith("not_code"); len(got) != 1 {
		t.Errorf("not_code DMs = %d, want one", len(got))
	}
}

func TestADependentOfAColleaguesPR(t *testing.T) {
	setup := func(t *testing.T) *world {
		w := newWorld(t)
		w.registry()
		w.hub.issue("o/r", 50).PullRequest = &struct{}{}
		w.prs["alice/fix"] = gh.PR{Number: 50, State: "OPEN", HeadRefName: "alice/fix"}
		w.planned("e1", oneItem("\n  - {from: \"https://github.com/o/r/pull/50\", to: w1, gate: start, when: merged, rollback: none}"))
		w.tick(false)
		if got := w.status("e1-w1").State; got != workQueued {
			t.Fatalf("e1-w1 is %s while PR 50 is open, want queued", got)
		}
		return w
	}
	t.Run("released when it merges", func(t *testing.T) {
		w := setup(t)
		w.prs["alice/fix"] = gh.PR{Number: 50, State: "MERGED", HeadRefName: "alice/fix"}
		w.tick(false)
		if got := w.status("e1-w1").State; got != stateStarting {
			t.Errorf("e1-w1 is %s, want starting", got)
		}
	})
	t.Run("re-checked when it closes unmerged", func(t *testing.T) {
		w := setup(t)
		w.prs["alice/fix"] = gh.PR{Number: 50, State: "CLOSED", HeadRefName: "alice/fix"}
		w.fullNext()
		w.tick(false)
		if got := w.signals("e1"); !slices.Equal(got, []string{"ended:https://github.com/o/r/pull/50"}) {
			t.Fatalf("signals = %v", got)
		}
		if got := w.status("e1-w1").State; got != workRechecking {
			t.Errorf("e1-w1 is %s, want rechecking", got)
		}
		if got := w.inbox("e1"); len(got) != 1 || !strings.Contains(got[0], "blocker https://github.com/o/r/pull/50 ended without its release; it blocks o/r#14") {
			t.Errorf("planner inbox = %q", got)
		}
	})
}

func TestACancelledCrossEpicBlockerAsksTheUser(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.planned("e2", oneItem("\n  - {from: e1-w1, to: w1, gate: start, when: merged, rollback: none}"))
	w.tick(false)
	if got := w.status("e2-w1").State; got != workQueued {
		t.Fatalf("e2-w1 is %s, want queued behind e1-w1", got)
	}
	w.moveTo("e1-w1", workCancelled)
	w.tick(false)
	if got := w.status("e2-w1").State; got != workRechecking {
		t.Fatalf("e2-w1 is %s after its blocker was cancelled, want rechecking", got)
	}
	// The planner cannot plan around it: the issue waits on the user.
	w.end("e2", "state-check.v2.md", stateCheck)
	w.end("e2", "epic.v2.yaml", withResult(oneItem("[]"), "o/r#14", `conflict, why: "its blocker e1-w1 was cancelled", answers: [drop, wait]`))
	w.tick(false)
	if got := w.status("e2-w1").State; got != workNeedsYou {
		t.Fatalf("e2-w1 is %s, want needs_you", got)
	}
	dms := w.dmsWith("e2 o/r#14 is conflict")
	if len(dms) != 1 || !strings.Contains(dms[0], `factory answer e2 o/r#14 "<drop | wait>"`) {
		t.Fatalf("DMs = %q", w.dms)
	}

	// The user answers; the re-check that follows decides.
	if _, err := AnswerTo(context.Background(), w.deps(), "e2", "o/r#14", "maybe"); err == nil || err.Error() != `"maybe" is not an answer for o/r#14: want one of drop | wait` {
		t.Fatalf("a wrong answer: %v", err)
	}
	did, err := AnswerTo(context.Background(), w.deps(), "e2", "14", "Wait")
	if err != nil || did != "e2 re-checks o/r#14 with your answer: wait" {
		t.Fatalf("answer = %q, %v", did, err)
	}
	w.tick(false)
	if got := w.status("e2"); got.State != epicChecking || w.status("e2-w1").State != workNeedsYou {
		t.Fatalf("e2 is %s and e2-w1 %s, want a re-check with the item still waiting", got.State, w.status("e2-w1").State)
	}
	if got := w.inbox("e2"); !strings.Contains(strings.Join(got, "\n"), "the user answered o/r#14: wait") {
		t.Errorf("planner inbox = %q", got)
	}
	w.end("e2", "state-check.v3.md", stateCheck)
	w.end("e2", "epic.v3.yaml", oneItem("[]"))
	w.tick(false)
	moves := w.moves("e2-w1")
	if got := moves[len(moves)-3:]; !slices.Equal(got, []string{"needs_you→rechecking", "rechecking→queued", "queued→starting"}) {
		t.Errorf("e2-w1 moves = %v", moves)
	}
}

func TestAWaitOverTheDMTimeSendsOneDM(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("\n  - {from: w1, to: w2, gate: start, when: merged, rollback: none}"))
	w.tick(false)
	w.now = w.now.Add(23 * time.Hour)
	w.tick(false)
	if got := w.dmsWith("has waited"); len(got) != 0 {
		t.Fatalf("DMs after 23h = %q", got)
	}
	w.now = w.now.Add(2 * time.Hour)
	w.tick(false)
	w.now = w.now.Add(time.Hour)
	w.tick(false)
	got := w.dmsWith("has waited")
	if len(got) != 1 || !strings.HasPrefix(got[0], "factory: e1-w2 (o/r#13) has waited 25h0m0s on w1 (gate start, when merged).") {
		t.Errorf("wait DMs = %q, want one", got)
	}
}

func TestAnswerLeaveOnAYoursResultGoesToThePlanner(t *testing.T) {
	w := newWorld(t)
	w.registry()
	plan := strings.Replace(oneItem("[]"), "  - {ref: o/r#14, result: ready}", "  - {ref: o/r#14, result: ready}\n  - {ref: o/r#30, result: yours, why: \"your open PR #31\", answers: [take over, leave]}", 1)
	w.planned("e1", plan)
	if got := w.dmsWith("e1 o/r#30 is yours: your open PR #31"); len(got) != 1 {
		t.Fatalf("DMs = %q", w.dms)
	}
	if _, err := AnswerTo(context.Background(), w.deps(), "e1", "o/r#30", "leave"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status("e1").State; got != epicChecking {
		t.Fatalf("e1 is %s, want checking", got)
	}
	if got := w.status("e1-w1").State; got == workRechecking {
		t.Error("e1-w1 is held, but the answer covers only o/r#30")
	}
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", strings.Replace(plan, `result: yours, why: "your open PR #31", answers: [take over, leave]`, `result: elsewhere, why: "you keep it"`, 1))
	w.tick(false)
	if got := w.status("e1").State; got == epicChecking {
		t.Fatalf("e1 is still checking")
	}
	if got := w.dmsWith("e1 o/r#30 is elsewhere: you keep it"); len(got) != 1 {
		t.Errorf("DMs = %q", w.dms)
	}
	if _, err := AnswerTo(context.Background(), w.deps(), "e1", "o/r#30", "leave"); err == nil || err.Error() != "o/r#30 waits on no answer: its result is elsewhere" {
		t.Errorf("a second answer: %v", err)
	}
}

func TestAnAnswerToAQuestionGoesToTheWork(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	w.moveTo("e1-w1", stateWaitingUser)
	did, err := AnswerTo(context.Background(), w.deps(), "e1", "o/r#14", "use the v2 API")
	if err != nil || did != "answered the question e1-w1 asked" {
		t.Fatalf("answer = %q, %v", did, err)
	}
	if req := last(w.kinds("e1-w1", events.KindRequest)); req.Request != RequestAnswer || req.Text != "use the v2 API" || req.To != blueprint.Previous {
		t.Errorf("request = %+v", req)
	}
}

func TestReplanStartsOneRecheckOfEveryIssue(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	if _, err := Replan(context.Background(), w.deps(), "e1", " "); err == nil {
		t.Error("an empty note was taken")
	}
	if _, err := Replan(context.Background(), w.deps(), "e1", "the API moved to v2"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.tick(false)
	if got := w.status("e1").Rechecks; got != 1 {
		t.Fatalf("re-checks = %d, want 1", got)
	}
	for _, id := range []string{"e1-w1", "e1-w2"} {
		if got := w.status(id).State; got != workRechecking {
			t.Errorf("%s is %s, want rechecking", id, got)
		}
	}
	if got := w.inbox("e1"); len(got) != 1 || !strings.Contains(got[0], "factory replan: the API moved to v2. Re-check only every issue of the epic") {
		t.Errorf("planner inbox = %q", got)
	}
}

func TestOverlapRechecksTheEpicWhoseExploreCameFirst(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.planned("e2", oneItem("[]"))
	w.tick(false)
	w.moveTo("e1-w1", "planning")
	w.moveTo("e2-w1", "planning")
	w.end("e1-w1", "explore.md", "## Summary\n## Files\n- pkg/a.go\n## Baseline tests\n## Issue check\n")
	w.end("e2-w1", "explore.md", "## Summary\n## Files\n- pkg/\n- docs/b.md\n## Baseline tests\n## Issue check\n")
	w.tick(false)
	if got := w.signals("e1"); len(got) != 1 || !strings.HasPrefix(got[0], "overlap:e2-w1:") {
		t.Errorf("e1 signals = %v, want one overlap with e2-w1", got)
	}
	if got := w.signals("e2"); len(got) != 0 {
		t.Errorf("e2 signals = %v: its explore came second", got)
	}
	if got := w.status("e1-w1").State; got != workRechecking {
		t.Errorf("e1-w1 is %s", got)
	}
}

func TestInstructionsAreKeptAsNumberedFiles(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	ins := w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", Text: "do e1-w2 first"})
	w.tick(false)
	data, err := os.ReadFile(filepath.Join(w.dir("e1-s1"), "instructions", seqRef(ins)+".md"))
	if err != nil || string(data) != "do e1-w2 first\n" {
		t.Fatalf("instruction file = %q, %v", data, err)
	}
}

func TestMayMergeAndDeployed(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.planned("e2", oneItem("\n  - {from: e1-w1, to: w1, gate: merge, when: deployed, rollback: revert e2-w1}"))
	w.prs["factory/e1-w1"] = gh.PR{Number: 40, State: "OPEN", HeadRefName: "factory/e1-w1"}
	w.prs["factory/e2-w1"] = gh.PR{Number: 41, State: "OPEN", HeadRefName: "factory/e2-w1"}
	w.prs["factory/e9-w9"] = gh.PR{Number: 49, State: "OPEN", HeadRefName: "factory/e9-w9"}
	w.prs["alice/fix"] = gh.PR{Number: 50, State: "OPEN", HeadRefName: "alice/fix"}
	w.tick(false)
	w.fullNext()
	w.tick(false)
	ctx := context.Background()
	for _, tt := range []struct {
		pr, why string
		ok      bool
	}{
		{"https://github.com/o/r/pull/50", "not a factory PR", true},
		{"https://github.com/o/r/pull/49", "a factory branch, but no work item records it", false},
		{"https://github.com/o/r/pull/41", "e2-w1: waits on e1-w1 (gate merge, when deployed)", false},
		{"https://github.com/o/r/pull/40", "", true},
	} {
		ok, why, err := MayMerge(ctx, w.deps(), tt.pr)
		if err != nil || ok != tt.ok || why != tt.why {
			t.Errorf("may-merge %s = %v %q %v, want %v %q", tt.pr, ok, why, err, tt.ok, tt.why)
		}
	}
	w.moveTo("e1-w1", stateMerged)
	if ok, why, _ := MayMerge(ctx, w.deps(), "https://github.com/o/r/pull/41"); ok {
		t.Errorf("may-merge 41 = ok %q before e1-w1 is deployed", why)
	}
	if _, err := Deployed(ctx, w.deps(), "https://github.com/o/r/pull/50"); err == nil || err.Error() != "no open epic waits on o/r#50 being deployed" {
		t.Errorf("deployed 50: %v", err)
	}
	epics, err := Deployed(ctx, w.deps(), "o/r#40")
	if err != nil || !slices.Equal(epics, []string{"e2"}) {
		t.Fatalf("deployed 40 = %v, %v", epics, err)
	}
	if ok, why, _ := MayMerge(ctx, w.deps(), "https://github.com/o/r/pull/41"); !ok {
		t.Errorf("may-merge 41 = %q once e1-w1 is deployed", why)
	}
	// An adopted PR whose item is not made yet is held.
	w.planned("e3", strings.Replace(oneItem("[]"), "{id: w1, repo: r, issue: o/r#14}", "{id: w1, repo: r, issue: o/r#14, pr: 60}", 1))
	r, err := readRun(ctx, w.deps())
	if err != nil {
		t.Fatal(err)
	}
	delete(r.ents, "e3-w1")
	if got := r.adopter(epic.Ref{Repo: "o/r", Number: 60}); got != "e3" {
		t.Errorf("adopter of #60 = %q, want e3", got)
	}
}

func TestTheEpicMovesToItsFeatureFolderWhileAnAppendRaces(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.epic("e1", "checking")
	w.planner("e1", "turn_ended", 4300, "done")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.end("e1", "epic.v1.yaml", "feature: my-feature\n"+twoItems("[]"))
	old := w.epicDir("e1")

	const appends = 200
	var wg sync.WaitGroup
	errs := make(chan error, appends)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range appends {
			if _, err := store.AppendTo(w.brain, "e1", events.Event{Kind: events.KindHeartbeat, Sender: "racer"}); err != nil {
				errs <- err
			}
		}
	}()
	w.tick(false)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("an append failed: %v", err)
	}

	to := filepath.Join(w.brain, "features", "my-feature")
	if dst, err := os.Readlink(old); err != nil || dst != to {
		t.Fatalf("old folder links to %q, %v; want %s", dst, err, to)
	}
	if got := w.dir("e1"); got != to {
		t.Fatalf("index has e1 in %s", got)
	}
	if got := w.dir("e1-w2"); got != filepath.Join(to, "sets", "e1-s2", "e1-w2") {
		t.Errorf("index has e1-w2 in %s", got)
	}
	evs, err := events.Read(filepath.Join(to, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	beats := 0
	for i, ev := range evs {
		if ev.Seq != i+1 {
			t.Fatalf("seq %d at line %d: a gap", ev.Seq, i+1)
		}
		if ev.Kind == events.KindHeartbeat {
			beats++
		}
	}
	if beats != appends {
		t.Errorf("heartbeats in the new folder = %d, want %d", beats, appends)
	}
	if got := w.status("e1").State; got != epicPlanned {
		t.Errorf("e1 is %s", got)
	}
	w.tick(false)
	if got := w.kinds("e1", events.KindDone); !slices.ContainsFunc(got, func(ev events.Event) bool { return ev.Key == "moved:"+to }) {
		t.Errorf("no moved record: %+v", got)
	}
	w.noErrors()
}

func TestTheEpicDoesNotMoveWhileThePlannerWorks(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.epic("e1", "checking")
	w.planner("e1", "running", 4300, "working")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.end("e1", "epic.v1.yaml", "feature: my-feature\n"+twoItems("[]"))
	w.tick(false)
	if got := w.dir("e1"); got != w.epicDir("e1") {
		t.Errorf("moved to %s while the planner works", got)
	}
}
