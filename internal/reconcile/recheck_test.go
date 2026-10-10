package reconcile

import (
	"context"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
)

// signals returns the keys of the re-check signals in epic id's log.
func (w *world) signals(id string) []string {
	var out []string
	for _, ev := range w.kinds(id, events.KindRequest) {
		if ev.Recipient == recipientRecheck {
			out = append(out, signalKey(ev))
		}
	}
	return out
}

// withResult returns twoItems with issue ref's result replaced.
func withResult(plan, ref, result string) string {
	return strings.Replace(plan, "{ref: "+ref+", result: ready}", "{ref: "+ref+", result: "+result+"}", 1)
}

func TestAnIssueStaleRechecksOnlyThatIssue(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	posted := w.listen(w.orchestrator("e1-s1", "turn_ended", 4401, "done").PID)
	w.moveTo("e1-w1", "exploring")
	w.moveTo("e1-w2", "implementing")
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindIssueStale, Sender: "e1-s1", Text: "#50 fixed it already"})
	w.tick(false)
	if got := w.status("e1"); got.State != epicChecking || got.Rechecks != 1 {
		t.Fatalf("e1 = %s with %d re-checks, want one re-check", got.State, got.Rechecks)
	}
	if got := w.status("e1-w1").State; got != workRechecking {
		t.Errorf("e1-w1 is %s, want rechecking", got)
	}
	if got := w.status("e1-w2").State; got != workImplementing {
		t.Errorf("e1-w2 is %s, want it to run on", got)
	}
	if ok, why := w.mayMerge("e1-w1"); ok || why != "a re-check of its issue is running" {
		t.Errorf("may-merge e1-w1 while rechecking = %v %q, want held", ok, why)
	}
	inbox := w.inbox("e1")
	if len(inbox) != 1 || !strings.Contains(inbox[0], "the explorer of e1-w1 says o/r#12 is stale: #50 fixed it already") ||
		!strings.Contains(inbox[0], "Re-check only o/r#12;") {
		t.Fatalf("planner inbox = %q", inbox)
	}

	// The planner writes its re-check: the item stays held while
	// epic.v2.yaml is being written, and moves only at its end.
	w.end("e1", "state-check.v2.md", stateCheck)
	v2 := withResult(twoItems("[]"), "o/r#12", "updated")
	put(t, filepath.Join(w.epicDir("e1"), "epic.v2.yaml"), v2)
	w.tick(false)
	if got := w.status("e1-w1").State; got != workRechecking {
		t.Fatalf("e1-w1 is %s while epic.v2.yaml has no end, want rechecking", got)
	}
	w.end("e1", "epic.v2.yaml", v2)
	w.tick(false)
	if got := w.status("e1").State; got != epicPlanned {
		t.Errorf("e1 is %s, want back in planned", got)
	}
	if got := w.status("e1-w1").State; got != "exploring" {
		t.Errorf("e1-w1 is %s, want back in exploring", got)
	}
	if got := w.inbox("e1-s1"); len(got) != 1 || !strings.Contains(got[0], "e1-w1: the planner updated issue o/r#12. Re-read it") {
		t.Errorf("orchestrator inbox = %q", got)
	}
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "e1-w1: the planner updated issue o/r#12") {
		t.Errorf("posted to the orchestrator: %q", got)
	}
	w.tick(false)
	if got := w.status("e1").Rechecks; got != 1 {
		t.Errorf("re-checks = %d after the signal was taken, want 1", got)
	}
	w.noErrors()
}

func TestAnEscalationRechecksItsIssueUnderItsRestartsRef(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.moveTo("e1-w1", "implementing")
	w.moveTo("e1-s2", "running")
	// The set ran out of things to start (a file move), and the item used up
	// its restarts (the escalation): only the second is a signal.
	w.append(w.dir("e1-s2"), events.Event{Kind: events.KindTransition, At: w.now, From: "running", To: stateBlocked, Trigger: "file", TriggerRef: "7"})
	if _, err := Replan(context.Background(), w.deps(), "e1", "first"); err != nil {
		t.Fatal(err)
	}
	ref := restartsRef + "e1-w1:6@2026-10-09T12:00:00Z"
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindTransition, At: w.now, From: "implementing", To: stateBlocked, Trigger: "tick", TriggerRef: ref})
	w.tick(false)
	if got := last(w.kinds("e1", events.KindTransition)); got.To != epicChecking || got.TriggerRef != ref {
		t.Fatalf("e1's move = %s on %q, want checking under the escalation's ref %q", got.To, got.TriggerRef, ref)
	}
	inbox := strings.Join(w.inbox("e1"), "\n")
	if !strings.Contains(inbox, "e1-w1 used up its restarts (o/r#12); also send e1-s1 an instruction") || !strings.Contains(inbox, "factory replan: first") {
		t.Errorf("planner inbox = %q", inbox)
	}
	if got := w.signals("e1"); len(got) != 2 {
		t.Errorf("signals = %v, want the replan and the escalation, not the set's file move", got)
	}
}

func TestAColleagueMergeTouchingAListedFileStartsOneRecheck(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.moveTo("e1-w1", "implementing")
	w.end("e1-w1", "explore.md", "## Summary\n## Files\n- `pkg/a.go`\n## Baseline tests\n## Issue check\n")
	w.hub.merged["o/r"] = []gh.MergedPR{
		{Number: 90, HeadRefName: "fix/a", Files: []gh.File{{Path: "pkg/a.go"}}},
		{Number: 91, HeadRefName: "fix/docs", Files: []gh.File{{Path: "docs/x.md"}}},
		{Number: 92, HeadRefName: "factory/e9-w1", Files: []gh.File{{Path: "pkg/a.go"}}},
	}
	w.tick(false)
	if got := w.signals("e1"); len(got) != 0 {
		t.Fatalf("signals %v on a tick that is not a full reconcile", got)
	}
	w.fullNext()
	w.tick(false)
	if got := w.signals("e1"); !slices.Equal(got, []string{"merged:o/r#90"}) {
		t.Fatalf("signals = %v, want only the colleague merge touching pkg/a.go", got)
	}
	if got := w.status("e1-w1").State; got != workRechecking {
		t.Errorf("e1-w1 is %s, want rechecking", got)
	}
	if got := w.status("e1-w2").State; got == workRechecking {
		t.Error("e1-w2 is held, but the merge touched none of its files")
	}
	w.fullNext()
	w.tick(false)
	if got := w.status("e1").Rechecks; got != 1 {
		t.Errorf("re-checks = %d, want one", got)
	}
}

// A colleague's merge already in the item's base when its worktree was made
// changes nothing the item started from: only a later one re-checks it.
func TestOnlyAColleagueMergeAfterTheWorktreeWasMadeRechecks(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	req, _ := lastRepoRequest(&entity{evs: w.log("e1-w1")}, worktreeRequest)
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindWorktreeReady, At: t0.Add(10 * time.Minute), Text: "/wt", Key: repoResultKey(req)})
	w.moveTo("e1-w1", "implementing")
	w.end("e1-w1", "explore.md", "## Summary\n## Files\n- `pkg/a.go`\n## Baseline tests\n## Issue check\n")
	w.hub.merged["o/r"] = []gh.MergedPR{
		{Number: 90, HeadRefName: "fix/a", MergedAt: t0.Add(5 * time.Minute), Files: []gh.File{{Path: "pkg/a.go"}}},
		{Number: 93, HeadRefName: "fix/a-again", MergedAt: t0.Add(20 * time.Minute), Files: []gh.File{{Path: "pkg/a.go"}}},
	}
	w.fullNext()
	w.tick(false)
	if got := w.signals("e1"); !slices.Equal(got, []string{"merged:o/r#93"}) {
		t.Errorf("signals = %v, want only the merge after e1-w1's worktree was made", got)
	}
}

// inReview puts item id in in_review with PR n recorded on GitHub, its
// worktree ready and handed over, and a live babysitter, which it returns.
func (w *world) inReview(id string, n int) claude.Session {
	w.t.Helper()
	w.readyWorktree(id)
	w.prs[git.Branch(id)] = gh.PR{Number: n, URL: "https://github.com/o/r/pull/" + strconv.Itoa(n), State: "OPEN", HeadRefName: git.Branch(id),
		HeadRefOid: "abc1234", AutoMergeRequest: &struct{}{}, CreatedAt: w.now}
	w.moveTo(id, workPROpen)
	w.append(w.dir(id), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture"})
	w.moveTo(id, workInReview)
	w.put(id+"-babysitter", blueprint.MachineSession, "e1", filepath.Join(w.dir(id), "sessions", "babysitter"), "running",
		&SessionInputs{Name: "factory:" + id + ":babysitter", Component: babysitterComponent, Task: id})
	return w.listSession("factory:"+id+":babysitter", 5100, "working")
}

func TestARecheckMarkingAnInReviewItemDoneCancelsIt(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	sitter := w.inReview("e1-w1", 40)
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #12 again"); err != nil {
		t.Fatal(err)
	}
	w.fullNext()
	w.tick(false)
	if got := w.status("e1-w1"); got.State != workRechecking || got.PR != 40 {
		t.Fatalf("e1-w1 = %s with PR %d, want rechecking with PR 40", got.State, got.PR)
	}
	w.hub.issue("o/r", 12).State = "closed"
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", `uat: none
issues:
  - {ref: o/r#12, result: done, evidence: "commit:abc1234"}
  - {ref: o/r#13, result: ready}
items:
  - {id: w2, repo: r, issue: o/r#13}
sets:
  - {id: s2, items: [w2]}
links: []
`)
	w.tick(false)
	if got := w.status("e1-w1").State; got != workCancelled {
		t.Fatalf("e1-w1 is %s, want cancelled\n%s", got, w.out.String())
	}
	for _, want := range []string{
		"claude stop " + sitter.ID,
		"gh pr merge 40 -R o/r --disable-auto",
		"gh pr close 40 -R o/r --comment Closed by the factory: the planner's re-check found o/r#12 done.",
		"gh issue edit 12 -R o/r --remove-assignee @me",
	} {
		if len(w.called(want)) != 1 {
			t.Errorf("want one %q in %q", want, w.calls())
		}
	}
	reqs := w.kinds("e1-w1", events.KindRequest)
	if cleanup := last(reqs); cleanup.Request != cleanupRequest || cleanup.Text != cancelledCleanup {
		t.Fatalf("last request = %+v, want a cleanup of cancelled work", cleanup)
	}
	if got := w.inbox("e1-s1"); len(got) != 0 {
		t.Errorf("orchestrator inbox = %q: the set's orchestrator never started, so nobody is told", got)
	}
	if got := w.status("e1-w2").State; got != stateStarting {
		t.Errorf("e1-w2 is %s, want back in starting", got)
	}
	w.tick(false)
	if got := w.called("gh pr close"); len(got) != 1 {
		t.Errorf("closed again: %v", got)
	}

	// The repo worker removes the worktree and branch with no merged SHA:
	// it never checks the head the PR had.
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, 0, &w.out); err != nil {
		t.Fatal(err)
	}
	if got := w.called("git -C " + filepath.Join(w.home, "src", "r") + " cat-file"); len(got) != 0 {
		t.Errorf("cleanup checked a merged SHA: %v", got)
	}
	if done := last(w.log("e1-w1")); done.Kind != events.KindDone || done.Key != repoResultKey(last(reqs)) {
		t.Errorf("worker answer = %+v", done)
	}
	w.noErrors()
}

func TestCancellingAnAdoptedPRLeavesItOpen(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.prs["me/own"] = gh.PR{Number: 31, URL: "https://github.com/o/r/pull/31", State: "OPEN", HeadRefName: "me/own", AutoMergeRequest: &struct{}{}, Author: gh.User{Login: "me"}}
	w.plannedAdopting("e1", strings.Replace(twoItems("[]"), "{id: w1, repo: r, issue: o/r#12}", "{id: w1, repo: r, issue: o/r#12, pr: 31}", 1), "o/r#12", 31)
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.append(w.dir("e1-w1"), Request(RequestStop, workCancelled, blueprint.TriggerUser, "not needed"))
	w.tick(false)
	if got := w.status("e1-w1").State; got != workCancelled {
		t.Fatalf("e1-w1 is %s", got)
	}
	if got := w.called("gh pr close"); len(got) != 0 {
		t.Errorf("closed the user's PR: %v", got)
	}
	if got := w.called("gh pr merge"); len(got) != 0 {
		t.Errorf("touched the user's PR's merge: %v", got)
	}
	if got := w.called("gh api -X POST repos/o/r/issues/31/comments"); len(got) != 1 || !strings.Contains(got[0], "The factory stopped working on this PR (factory stop: not needed). It stays open.") {
		t.Errorf("comments = %v", got)
	}
	if got := w.dmsWith("It had adopted your PR https://github.com/o/r/pull/31, which stays open."); len(got) != 1 {
		t.Errorf("DMs = %q", w.dms)
	}
	if cleanup := last(w.kinds("e1-w1", events.KindRequest)); cleanup.Request != cleanupRequest || cleanup.Text == cancelledCleanup {
		t.Errorf("cleanup = %+v, want one that keeps work not in the PR", cleanup)
	}
}

func TestAReadyResultLeavesAnInReviewItemWithItsBabysitter(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.inReview("e1-w1", 41)
	if _, err := Replan(context.Background(), w.deps(), "e1", "nothing changed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", twoItems("[]"))
	w.tick(false)
	if got := last(w.moves("e1-w1")); got != "rechecking→in_review" {
		t.Errorf("last move = %s, want back in in_review", got)
	}
	if hb := w.kinds("e1-w1", events.KindHandback); len(hb) != 0 || len(w.called("claude stop")) != 0 {
		t.Errorf("handbacks %+v, stops %v: a ready result changes no scope", hb, w.called("claude stop"))
	}
}

func TestAnUpdatedResultAfterHandoverHandsTheWorktreeBack(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.inReview("e1-w1", 41)
	w.listen(w.orchestrator("e1-s1", "turn_ended", 4402, "done").PID)
	if _, err := Replan(context.Background(), w.deps(), "e1", "the API changed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", withResult(twoItems("[]"), "o/r#12", "updated"))
	w.tick(false)
	moves := w.moves("e1-w1")
	if got := moves[len(moves)-3:]; !slices.Equal(got, []string{"in_review→rechecking", "rechecking→in_review", "in_review→implementing"}) {
		t.Fatalf("moves = %v", moves)
	}
	if hb := w.kinds("e1-w1", events.KindHandback); len(hb) != 1 {
		t.Errorf("handbacks = %+v", hb)
	}
	if handedOver(&entity{evs: w.log("e1-w1")}) {
		t.Error("the worktree is still the babysitter's")
	}
	if got := w.called("claude stop"); len(got) != 1 {
		t.Errorf("stops = %v, want the babysitter stopped", got)
	}
	inbox := strings.Join(w.inbox("e1-s1"), "\n")
	for _, want := range []string{"e1-w1: the planner updated issue o/r#12. Re-read it", "its worktree " + filepath.Join(w.home, "src", "r-worktrees", "factory-e1-w1") + " is yours again"} {
		if !strings.Contains(inbox, want) {
			t.Errorf("orchestrator inbox lacks %q:\n%s", want, inbox)
		}
	}
}

func TestAnOrchestratorThatDiesBeforeHandoverStillGetsTheItemToReview(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.moveTo("e1-w1", workPROpen)
	w.prs["factory/e1-w1"] = gh.PR{Number: 42, State: "OPEN", HeadRefName: "factory/e1-w1", CreatedAt: w.now}
	w.fullNext()
	w.tick(false)
	if got := w.status("e1-w1").State; got != workPROpen {
		t.Fatalf("e1-w1 is %s with nothing handing it over, want pr_open", got)
	}
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "dead",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: componentOrchestrator, Task: "e1-s1"})
	w.fullNext()
	w.tick(false)
	if hb := w.kinds("e1-w1", events.KindHandover); len(hb) != 1 || hb[0].Text != "the orchestrator's session is dead" {
		t.Fatalf("handovers = %+v", hb)
	}
	if got := last(w.moves("e1-w1")); got != "pr_open→in_review" {
		t.Errorf("last move = %s, want pr_open→in_review", got)
	}
}

func TestTheProgramHandsOverWhenThePROpenerEnds(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.moveTo("e1-w1", workPROpen)
	w.prs["factory/e1-w1"] = gh.PR{Number: 43, State: "OPEN", HeadRefName: "factory/e1-w1", CreatedAt: w.now}
	w.end("e1-w1", "pr.md", "## PR\n## Checks\n## Reviewers\n")
	w.fullNext()
	w.tick(false)
	if hb := w.kinds("e1-w1", events.KindHandover); len(hb) != 1 || hb[0].Text != "the pr-opener ended" {
		t.Fatalf("handovers = %+v", hb)
	}
	if got := w.status("e1-w1").State; got != workInReview {
		t.Errorf("e1-w1 is %s, want in_review", got)
	}
}

// The epic's 2h checking timeout restarts the planner; the re-check it was
// running goes on, holding its items, and is neither ended nor begun again.
func TestACheckingTimeoutKeepsTheRecheckRunning(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status(item).State; got != workRechecking {
		t.Fatalf("%s is %s, want rechecking", item, got)
	}
	w.now = w.now.Add(2*time.Hour + time.Minute)
	w.fullNext()
	w.tick(false)
	if got := last(w.moves("e1")); got != "checking→checking" {
		t.Fatalf("e1's last move %s, want the timeout's restart; moves %v", got, w.moves("e1"))
	}
	if got := w.status(item).State; got != workRechecking {
		t.Errorf("%s is %s while the planner has not ended its re-check, want rechecking; moves %v", item, got, w.moves(item))
	}
	if got := w.called("gh pr merge 5"); len(got) != 0 {
		t.Errorf("merged while the re-check still runs: %q", got)
	}
	var asks []string
	for _, m := range w.inbox("e1") {
		if strings.HasPrefix(m, "Re-check:") {
			asks = append(asks, m)
		}
	}
	if len(asks) != 1 || w.status("e1").Rechecks != 1 {
		t.Errorf("planner asked %q, re-checks counted %d; want one of each", asks, w.status("e1").Rechecks)
	}
	w.recheckReady()
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := last(w.moves(item)); got != "rechecking→in_review" {
		t.Errorf("%s's last move %s once the re-check ended, want rechecking→in_review", item, got)
	}
}

// escalate blocks e1-w1 past its restarts at the world's time, after an
// instruction to its set at instructed, when that is set; the next tick
// starts the escalation's re-check.
func (w *world) escalate(instructed time.Time) {
	w.t.Helper()
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.moveTo("e1-w1", "implementing")
	if !instructed.IsZero() {
		w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: instructed, Text: "an old instruction"})
	}
	ref := restartsRef + "e1-w1:6@2026-10-09T12:00:00Z"
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindTransition, At: w.now, From: "implementing", To: stateBlocked,
		Prev: []string{"implementing"}, Trigger: "tick", TriggerRef: ref})
	w.tick(false)
	if got := w.status("e1-w1").State; got != workRechecking {
		w.t.Fatalf("e1-w1 is %s, want rechecking", got)
	}
}

// The planner instructs the escalated item's set during the re-check, then
// ends it: the item goes back to blocked, and the instruction releases it.
func TestAnInstructionSentDuringTheEscalationRecheckReleasesTheItem(t *testing.T) {
	w := newWorld(t)
	w.escalate(time.Time{})
	w.now = w.now.Add(time.Minute)
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", At: w.now, Text: "split the change"})
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", twoItems("[]"))
	for range 3 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	want := []string{"implementing→blocked", "blocked→rechecking", "rechecking→blocked", "blocked→implementing"}
	if got := w.moves("e1-w1"); !slices.Equal(got[len(got)-4:], want) {
		t.Errorf("moves %v, want them to end %v", got, want)
	}
}

// An instruction to the set from before the escalation is not the answer to
// it: the item stays blocked after the re-check.
func TestAnInstructionFromBeforeTheEscalationReleasesNothing(t *testing.T) {
	w := newWorld(t)
	w.escalate(t0.Add(-time.Minute))
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", twoItems("[]"))
	for range 3 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if got := w.status("e1-w1").State; got != stateBlocked {
		t.Errorf("e1-w1 is %s, want blocked until the planner instructs its set; moves %v", got, w.moves("e1-w1"))
	}
}

// After a handback the old pr-opener's end hands nothing over: the redo
// reaches pr_open with the orchestrator's new pr-opener still in the
// worktree, and only its own end hands the worktree over again.
func TestAnOldPROpenerEndDoesNotHandOverAfterAHandback(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	w.tick(false)
	w.readyWorktree(item)
	w.moveTo(item, workPROpen)
	w.prs[git.Branch(item)] = gh.PR{Number: 43, State: "OPEN", HeadRefName: git.Branch(item), CreatedAt: w.now}
	w.end(item, "pr.md", "## PR\n## Checks\n## Reviewers\n")
	w.fullNext()
	w.tick(false)
	if got := w.status(item).State; got != workInReview {
		t.Fatalf("%s is %s, want in_review after the first pr-opener ended", item, got)
	}
	w.append(w.dir(item), events.Event{Kind: events.KindHandback, Sender: events.SenderProgram, Text: "the scope changed"})
	w.moveTo(item, workImplementing)
	w.moveTo(item, workPROpen)
	w.fullNext()
	w.tick(false)
	if got := len(w.kinds(item, events.KindHandover)); got != 1 || w.status(item).State != workPROpen {
		t.Fatalf("%s is %s with %d handovers, want pr_open with the first one only", item, w.status(item).State, got)
	}
	w.end(item, "pr.v2.md", "## PR\n## Checks\n## Reviewers\n")
	w.fullNext()
	w.tick(false)
	if got := len(w.kinds(item, events.KindHandover)); got != 2 || w.status(item).State != workInReview {
		t.Errorf("%s is %s with %d handovers, want in_review after the new pr-opener ended", item, w.status(item).State, got)
	}
}

// An item a re-check result left waiting on the user moves on only by
// factory answer: a retry is refused with that command, and status shows
// the result and its answers.
func TestARetryOnAnItemAwaitingAnAnswerIsRefused(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.planned("e2", oneItem("\n  - {from: e1-w1, to: w1, gate: start, when: merged, rollback: none}"))
	w.tick(false)
	w.moveTo("e1-w1", workCancelled)
	w.tick(false)
	w.end("e2", "state-check.v2.md", stateCheck)
	w.end("e2", "epic.v2.yaml", withResult(oneItem("[]"), "o/r#14", `conflict, why: "its blocker e1-w1 was cancelled", answers: [drop, wait]`))
	w.tick(false)
	if got := w.status("e2-w1").State; got != stateNeedsYou {
		t.Fatalf("e2-w1 is %s, want needs_you", got)
	}
	const hint = `o/r#14 is conflict: its blocker e1-w1 was cancelled; answer with factory answer e2 o/r#14 "<drop | wait>"`
	err := Retry(context.Background(), w.deps(), "e2-w1")
	if err == nil || !strings.Contains(err.Error(), "guard retry_allowed does not hold") || !strings.HasSuffix(err.Error(), hint) {
		t.Errorf("retry = %v, want refused with %q", err, hint)
	}
	for range 3 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if got := w.status("e2-w1").State; got != stateNeedsYou {
		t.Errorf("e2-w1 is %s after a refused retry, want needs_you; moves %v", got, w.moves("e2-w1"))
	}
	rep, err := Status(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(rep.Waiting, func(x Waiting) bool { return x.ID == "e2-w1" }); i < 0 || rep.Waiting[i].Why != hint {
		t.Errorf("waiting on you = %+v, want e2-w1 with %q", rep.Waiting, hint)
	}
}

// An updated result for an item taken out of merging returns it to
// in_review and hands its worktree back; the orchestrator is told to
// re-read the issue, not only that the worktree is its own again.
func TestAnUpdatedResultWhileMergingTellsTheOrchestratorToReRead(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.orchestrator("e1-s1", "turn_ended", 4410, "done")
	w.moveTo(item, workMerging)
	w.append(w.dir(item), events.Event{Kind: events.KindIntent, Text: "merge " + head1 + " by squash", Key: "merge:1:intent"})
	if _, err := Replan(context.Background(), w.deps(), "e1", "the API changed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", withResult(oneItem("[]"), "o/r#14", "updated"))
	w.tick(false)
	w.tick(false)
	inbox := strings.Join(w.inbox("e1-s1"), "\n")
	if !strings.Contains(inbox, "the planner updated issue o/r#14. Re-read it") || !strings.Contains(inbox, "its worktree") {
		t.Errorf("orchestrator inbox:\n%s\nwant the re-read and the handback; moves %v", inbox, w.moves(item))
	}
}

// An item that waits on the user's answer to an earlier re-check is not
// held by a new one, but may-merge still says a re-check of its issue runs,
// and goes on saying it after checking's timeout restarts the planner.
func TestMayMergeHoldsThroughACheckingRestart(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #14"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", withResult(oneItem("[]"), "o/r#14", `conflict, why: "a decision says no", answers: [drop, keep]`))
	w.tick(false)
	if got := w.status(item).State; got != stateNeedsYou {
		t.Fatalf("%s is %s, want needs_you", item, got)
	}
	if _, err := Replan(context.Background(), w.deps(), "e1", "check again"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	for _, at := range []string{"while the re-check runs", "after checking restarted"} {
		ok, why, err := MayMerge(context.Background(), w.deps(), prURL)
		if err != nil || ok || why != item+": a re-check of its issue is running" {
			t.Errorf("may-merge %s = %v %q %v, want held by the re-check", at, ok, why, err)
		}
		w.now = w.now.Add(2*time.Hour + time.Minute)
		w.tick(false)
	}
}

// An item handed back for an updated issue comes back to in_review with its
// redone PR; the same updated result does not hand it back again.
func TestAnUpdatedItemBackInReviewIsNotHandedBackAgain(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	if _, err := Replan(context.Background(), w.deps(), "e1", "the API changed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", withResult(oneItem("[]"), "o/r#14", "updated"))
	w.tick(false)
	if got := w.status(item).State; got != workImplementing {
		t.Fatalf("%s is %s, want implementing after the handback", item, got)
	}
	w.moveTo(item, workPROpen)
	w.append(w.dir(item), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture"})
	w.moveTo(item, workInReview)
	w.now = w.now.Add(time.Minute)
	w.fullNext()
	w.tick(false)
	if hb := w.kinds(item, events.KindHandback); len(hb) != 1 {
		t.Errorf("handbacks = %d, want only the first; moves %v", len(hb), w.moves(item))
	}
	if got := w.status(item).State; got != workMerging {
		t.Errorf("%s is %s, want its redone PR merging", item, got)
	}
}
