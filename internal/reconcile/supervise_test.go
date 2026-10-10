package reconcile

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

func TestNoProgressIsNudgedThenRestarted(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4201, "working")
	posted := w.listen(s.PID)
	w.append(w.dir("e1-w1"), events.Event{At: w.now, Kind: events.KindIntent, Sender: "e1-s1", Text: "push abc to factory/e1-w1"})

	w.at(44 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 0 {
		t.Fatalf("nudged before no_progress: %q", got)
	}
	w.at(45 * time.Minute)
	w.tick(false)
	got := posted()
	if len(got) != 1 || !strings.Contains(got[0], "Nudge: no progress on e1-w1 (implementing) for 45m") || !strings.Contains(got[0], "factory inbox e1-s1") {
		t.Fatalf("posted %q, want one nudge", got)
	}
	if n := w.kinds("e1-w1", events.KindNudge); len(n) != 1 {
		t.Errorf("nudges = %+v, want one counted on the item", n)
	}

	w.at(89 * time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); len(got) != 0 {
		t.Fatalf("restarted before no_progress after the nudge: %q", got)
	}
	w.at(90 * time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); !reflect.DeepEqual(got, []string{"claude stop " + s.ID}) {
		t.Fatalf("stops = %q, want a restart", got)
	}
	if r := w.kinds("e1-w1", events.KindRestart); len(r) != 1 {
		t.Errorf("restarts = %+v, want one counted on the item", r)
	}
	w.at(91 * time.Minute)
	w.tick(false)
	resumes := w.called("claude --bg --resume " + s.SessionID)
	if len(resumes) != 1 {
		t.Fatalf("resumes = %q, want the restart's flagless resume", w.calls())
	}
	for _, want := range []string{"factory-task: e1-s1", "e1-w1 (implementing)", "lease: 3", "e1-w1 seq 4: push abc to factory/e1-w1"} {
		if !strings.Contains(resumes[0], want) {
			t.Errorf("resume prompt lacks %q:\n%s", want, resumes[0])
		}
	}
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got, []string{"→running", "running→stopped", "stopped→starting"}) {
		t.Errorf("session moves = %v", got)
	}
}

func TestATurnEndedSessionWithWorkIsNudged(t *testing.T) {
	w := newWorld(t)
	w.working("planning")
	s := w.orchestrator("e1-s1", "turn_ended", 4202, "done")
	posted := w.listen(s.PID)
	w.at(9 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 0 {
		t.Fatalf("nudged before an inbox check interval: %q", got)
	}
	w.at(10 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "its turn ended with e1-w1 planning") {
		t.Errorf("posted %q, want one nudge", got)
	}
}

func TestInReviewForDaysIsNeverRestarted(t *testing.T) {
	w := newWorld(t)
	w.working("in_review")
	s := w.orchestrator("e1-s1", "turn_ended", 4203, "done")
	posted := w.listen(s.PID)
	for _, d := range []time.Duration{time.Hour, 24 * time.Hour, 72 * time.Hour} {
		w.at(d)
		w.tick(false)
	}
	if got := posted(); len(got) != 0 {
		t.Errorf("posted %q to a session whose work waits on GitHub", got)
	}
	if got := w.called("claude stop"); len(got) != 0 {
		t.Errorf("stopped %q", got)
	}
	if n := append(w.kinds("e1-w1", events.KindNudge), w.kinds("e1-w1", events.KindRestart)...); len(n) != 0 {
		t.Errorf("counted %+v", n)
	}
}

func TestAnUnacknowledgedMessage(t *testing.T) {
	message := func(w *world) events.Event {
		return w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindInstruction, Sender: "e1", Text: "use the v2 API"})
	}
	t.Run("running, no helper heartbeat: nudged after 15 minutes", func(t *testing.T) {
		w := newWorld(t)
		w.working("implementing")
		s := w.orchestrator("e1-s1", "running", 4204, "working")
		posted := w.listen(s.PID)
		m := message(w)
		w.tick(false)
		w.at(14 * time.Minute)
		w.tick(false)
		if got := posted(); len(got) != 1 || !strings.Contains(got[0], "seq "+seqRef(m)+", instruction from e1: use the v2 API") {
			t.Fatalf("posted %q, want only the delivery", got)
		}
		w.at(15 * time.Minute)
		w.tick(false)
		if got := posted(); len(got) != 2 || !strings.Contains(got[1], "message seq "+seqRef(m)+" unacknowledged for 15m") {
			t.Errorf("posted %q, want a nudge naming the message", got)
		}
	})
	t.Run("a helper heartbeats through a 2h run: no nudge, no restart", func(t *testing.T) {
		w := newWorld(t)
		w.working("implementing")
		s := w.orchestrator("e1-s1", "running", 4205, "working")
		posted := w.listen(s.PID)
		message(w)
		for d := time.Duration(0); d <= 2*time.Hour; d += 5 * time.Minute {
			w.at(d)
			w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: "e1-s1", Lease: 1, At: w.now})
			w.tick(false)
		}
		if got := posted(); len(got) != 1 {
			t.Errorf("posted %q, want only the delivery", got)
		}
		if got := w.called("claude stop"); len(got) != 0 {
			t.Errorf("restarted %q", got)
		}
	})
	t.Run("a turn-ended session gets it in its wake", func(t *testing.T) {
		w := newWorld(t)
		w.working("implementing")
		s := w.orchestrator("e1-s1", "turn_ended", 4206, "done")
		posted := w.listen(s.PID)
		m := message(w)
		w.tick(false)
		if got := posted(); len(got) != 1 || !strings.Contains(got[0], "seq "+seqRef(m)+", instruction") {
			t.Fatalf("posted %q, want the message in the wake", got)
		}
		w.at(16 * time.Minute)
		w.tick(false)
		for _, n := range w.kinds("e1-w1", events.KindNudge) {
			if strings.Contains(n.Text, "unacknowledged") {
				t.Errorf("nudge %q: the unacknowledged rule is for a running session", n.Text)
			}
		}
	})
}

func TestAHeldBackSessionIsNudgedNotRestarted(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "turn_ended", 4207, "done")
	posted := w.listen(s.PID)
	w.at(10 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 1 {
		t.Fatalf("posted %q, want the turn-ended nudge", got)
	}
	w.acct.ok = false
	w.at(11 * time.Minute)
	w.tick(false)
	w.at(3*time.Hour + 11*time.Minute)
	w.tick(false)
	w.acct.ok = true
	w.at(3*time.Hour + 12*time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); len(got) != 0 {
		t.Errorf("restarted after a 3h hold: %q", got)
	}
	if st := w.status("e1-s1"); st.Lease != 1 {
		t.Errorf("lease = %d, want 1: its clock stood still while held", st.Lease)
	}
	w.at(3*time.Hour + 56*time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); len(got) != 1 {
		t.Errorf("stops = %q, want a restart once no_progress ran since the nudge, holds aside", got)
	}
}

func TestALeaseIsRenewedOnlyUnderItsNumber(t *testing.T) {
	for _, tc := range []struct {
		name      string
		heartbeat int
		want      int
	}{{"current lease", 2, 2}, {"old lease", 1, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.working("implementing")
			if err := store.WriteStatus(w.dir("e1-s1"), store.Status{State: "running", Since: t0, Lease: 2}); err != nil {
				t.Fatal(err)
			}
			w.orchestrator("e1-s1", "running", 4208, "working")
			w.at(20 * time.Minute)
			w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: "e1-s1", Lease: tc.heartbeat, At: w.now})
			w.append(w.dir("e1-w1"), events.Event{Kind: events.KindStart, Sender: "e1-s1", At: w.now})
			w.at(31 * time.Minute)
			w.tick(false)
			if st := w.status("e1-s1"); st.Lease != tc.want {
				t.Errorf("lease = %d, want %d", st.Lease, tc.want)
			}
		})
	}
}

func TestADeadSessionsResumeCountsAsARestart(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	w.orchestrator("e1-s1", "running", 0, "working")
	w.tick(false)
	w.at(time.Minute + 59*time.Second)
	w.tick(false)
	if got := w.moves("e1-s1-orchestrator"); len(got) != 1 {
		t.Fatalf("moves = %v before dead_after", got)
	}
	w.at(2 * time.Minute)
	w.tick(false)
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got, []string{"→running", "running→dead", "dead→starting"}) {
		t.Fatalf("moves = %v", got)
	}
	if r := w.kinds("e1-w1", events.KindRestart); len(r) != 1 || r[0].Text != "factory:e1-s1:orchestrator was dead" {
		t.Errorf("restarts = %+v", r)
	}
}

// counted logs n restarts on id, as the supervisor would, ago before now.
func (w *world) counted(id string, n int, ago time.Duration) {
	for i := range n {
		w.append(w.dir(id), events.Event{Kind: events.KindRestart, Sender: events.SenderProgram, Text: "no progress", At: w.now.Add(time.Duration(i)*time.Second - ago)})
	}
}

func TestUsedUpRestartsEscalateThenWaitForTheUser(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	w.counted("e1-w1", 5, 2*time.Hour)
	w.tick(false)
	if st := w.status("e1-w1"); st.State != "implementing" {
		t.Fatalf("blocked after 5 restarts")
	}
	w.counted("e1-w1", 1, 2*time.Hour)
	w.tick(false)
	moves := w.kinds("e1-w1", events.KindTransition)
	if last := moves[len(moves)-1]; last.To != "blocked" || !strings.HasPrefix(last.TriggerRef, "restarts:e1-w1:6@") {
		t.Fatalf("last move = %+v, want blocked on its restarts", last)
	}

	// The epic re-checks it: its move into blocked is a tick move.
	if last := moves[len(moves)-1]; last.Trigger != blueprint.TriggerTick {
		t.Errorf("move into blocked on %s, want tick", last.Trigger)
	}

	// The planner runs twice more for it; the item stalls a fourth time.
	for range 2 {
		w.append(w.dir("e1-w1"), events.Event{Kind: events.KindTransition, From: "blocked", To: "implementing", Trigger: "file", TriggerRef: "x", At: w.now})
		w.now = w.now.Add(time.Minute)
		w.counted("e1-w1", 6, 2*time.Hour)
		w.tick(false)
	}
	if got := len(w.kinds("e1-w1", events.KindTransition)); w.status("e1-w1").State != "blocked" || escalations(&entity{evs: w.log("e1-w1")}) != 3 {
		t.Fatalf("%d moves, state %s, want 3 escalations", got, w.status("e1-w1").State)
	}
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindTransition, From: "blocked", To: "implementing", Trigger: "file", TriggerRef: "y", At: w.now})
	w.now = w.now.Add(time.Minute)
	w.counted("e1-w1", 6, 2*time.Hour)
	w.tick(false)
	if st := w.status("e1-w1"); st.State != "needs_you" {
		t.Fatalf("state = %s, want needs_you after 3 planner runs", st.State)
	}
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "e1-w1 needs you: it stalled again after 3 planner runs") {
		t.Errorf("DMs = %q", w.dms)
	}
}

// loadRun returns a run over the world's brain, its entities loaded.
func loadRun(t *testing.T, w *world) *run {
	t.Helper()
	r, err := newRun(context.Background(), w.deps(), true)
	if err != nil {
		t.Fatal(err)
	}
	if r.b, err = blueprint.Load(shippedBlueprint); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	return r
}

func TestOrchestratorRestartsPerHourBlockTheSet(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	w.counted("e1-w1", 1, 0)
	w.tick(false)
	if st := w.status("e1-s1"); st.State != "running" {
		t.Fatalf("set is %s after one restart", st.State)
	}
	w.counted("e1-w1", 1, 0)
	w.tick(false)
	moves := w.kinds("e1-s1", events.KindTransition)
	if last := moves[len(moves)-1]; last.To != "blocked" || !strings.HasPrefix(last.TriggerRef, "restarts:e1-s1:2@") {
		t.Errorf("last set move = %+v, want blocked on its orchestrator's restarts", last)
	}
	// An hour on, the old restarts no longer count.
	w2 := newWorld(t)
	w2.working("implementing")
	w2.counted("e1-w1", 1, 0)
	w2.at(61 * time.Minute)
	w2.counted("e1-w1", 1, 0)
	w2.tick(false)
	if st := w2.status("e1-s1"); st.State != "running" {
		t.Errorf("set is %s with restarts an hour apart", st.State)
	}
}

func TestTheSameErrorThreeTimesIsDeadLettered(t *testing.T) {
	for _, tc := range []struct {
		name   string
		errors []string
		want   string
	}{
		{"same", []string{"go test fails in ./x", "go test fails in ./x", "go test fails in ./x"}, "needs_you"},
		{"different", []string{"go test fails in ./x", "go test fails in ./x", "vet fails"}, "implementing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.working("implementing")
			for _, text := range tc.errors {
				w.append(w.dir("e1-w1"), events.Event{At: w.now, Kind: events.KindError, Sender: "e1-s1", Text: text})
			}
			w.tick(false)
			w.tick(false)
			if st := w.status("e1-w1"); st.State != tc.want {
				t.Fatalf("state = %s, want %s", st.State, tc.want)
			}
			if tc.want == "needs_you" && (len(w.dms) != 1 || !strings.Contains(w.dms[0], "the same error 3 times (dead-lettered): go test fails in ./x")) {
				t.Errorf("DMs = %q, want one", w.dms)
			}
		})
	}
}

func TestTimeoutsRestartAndAreCounted(t *testing.T) {
	t.Run("exploring after 2h restarts the orchestrator", func(t *testing.T) {
		w := newWorld(t)
		w.working("exploring")
		s := w.orchestrator("e1-s1", "turn_ended", 4209, "done")
		w.at(2 * time.Hour)
		w.tick(false)
		if got := w.moves("e1-w1"); !reflect.DeepEqual(got, []string{"→exploring", "exploring→exploring"}) {
			t.Fatalf("moves = %v", got)
		}
		if r := w.kinds("e1-w1", events.KindRestart); len(r) != 1 || r[0].Text != "exploring timed out" {
			t.Errorf("restarts = %+v", r)
		}
		if got := w.called("claude stop"); !reflect.DeepEqual(got, []string{"claude stop " + s.ID}) {
			t.Errorf("stops = %q", got)
		}
	})
	t.Run("starting after 1h asks for the worktree again", func(t *testing.T) {
		w := newWorld(t)
		w.epic("e1", "running")
		w.set("e1", "e1-s1", "running", "e1-w1")
		w.item("e1", "e1-s1", "e1-w1", "starting")
		w.append(w.dir("e1-w1"), events.Event{At: w.now, Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
		w.at(59 * time.Minute)
		w.tick(false)
		w.at(time.Hour)
		w.tick(false)
		if got := w.moves("e1-w1"); !reflect.DeepEqual(got, []string{"→starting", "starting→starting"}) {
			t.Fatalf("moves = %v", got)
		}
		if reqs := w.kinds("e1-w1", events.KindRequest); len(reqs) != 2 {
			t.Errorf("worktree requests = %d, want a second one", len(reqs))
		}
		if r := w.kinds("e1-w1", events.KindRestart); len(r) != 1 {
			t.Errorf("restarts = %+v, want it counted", r)
		}
	})
	t.Run("pr_open after 1h waits for the user with a DM", func(t *testing.T) {
		w := newWorld(t)
		w.working("pr_open")
		w.at(time.Hour)
		w.tick(false)
		w.tick(false)
		if st := w.status("e1-w1"); st.State != "needs_you" {
			t.Fatalf("state = %s", st.State)
		}
		if len(w.dms) != 1 || !strings.Contains(w.dms[0], "e1-w1 needs you: pr_open timed out after 1h0m0s") {
			t.Errorf("DMs = %q", w.dms)
		}
	})
}

func TestARedBasePausesNewItemsInItsRepo(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	oneSet(w)
	w.red["o/r@main"] = true
	w.tick(false)
	if got := w.moves("e1-w1"); len(got) != 1 {
		t.Errorf("moves = %v, want e1-w1 held in queued", got)
	}
	if o, _ := store.ReadOverall(w.brain); !reflect.DeepEqual(o.PausedRepos, []string{"o/r@main"}) {
		t.Errorf("paused repos = %v", o.PausedRepos)
	}
	w.red["o/r@main"] = false
	w.at(time.Minute)
	w.tick(false)
	if got := w.moves("e1-w1"); len(got) != 2 {
		t.Errorf("moves = %v, want e1-w1 started once the base is green", got)
	}
	if o, _ := store.ReadOverall(w.brain); len(o.PausedRepos) != 0 {
		t.Errorf("paused repos = %v, want none", o.PausedRepos)
	}
}

func TestSixUnknownListingsInARowSendOneDM(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "running",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	for i := range 7 {
		w.at(time.Duration(i) * time.Minute)
		w.tick(false)
	}
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "6 ticks in a row") {
		t.Errorf("DMs = %q, want one at the sixth", w.dms)
	}
	if got := w.moves("e1-s1-orchestrator"); len(got) != 1 {
		t.Errorf("session moves = %v while the listing is unknown", got)
	}
	w.listSession("factory:e1-s1:orchestrator", 4210, "working")
	w.at(8 * time.Minute)
	w.tick(false)
	if o, _ := store.ReadOverall(w.brain); o.UnknownListings != 0 {
		t.Errorf("unknown listings = %d after a known one", o.UnknownListings)
	}
}

func TestAFinishedSessionStillIdleIsStopped(t *testing.T) {
	for _, tc := range []struct {
		listed string
		stops  int
	}{{"done", 1}, {"working", 0}} {
		w := newWorld(t)
		w.epic("e1", "done")
		w.planner("e1", "finished", 4211, tc.listed)
		w.tick(false)
		if got := w.called("claude stop"); len(got) != tc.stops {
			t.Errorf("listed %s: stops = %q, want %d", tc.listed, got, tc.stops)
		}
	}
}

func TestAHelperOnADeniedModelStopsItsSession(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4212, "working")
	main := w.transcript(s.SessionID, answer(t0, "claude-opus-5-5", 1000))
	put(t, strings.TrimSuffix(main, ".jsonl")+"/subagents/agent-a1.jsonl", answer(t0, "claude-fable-5-1", 500)+"\n")
	w.tick(false)
	if got := w.called("claude stop"); !reflect.DeepEqual(got, []string{"claude stop " + s.ID}) {
		t.Fatalf("stops = %q", got)
	}
	w.at(time.Minute)
	w.tick(false)
	if st := w.status("e1-w1"); st.State != "needs_you" {
		t.Fatalf("e1-w1 is %s, want needs_you", st.State)
	}
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "agent-a1 ran on claude-fable-5-1, which is denied (fable)") {
		t.Errorf("DMs = %q", w.dms)
	}

	// After the user's retry the same transcript is not flagged again.
	w.append(w.dir("e1-w1"), Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, ""))
	w.setState("factory:e1-s1:orchestrator", 4213, "working")
	for i := range 6 {
		w.at(time.Duration(2+i) * time.Minute)
		w.tick(false)
	}
	if st := w.status("e1-w1"); st.State != "implementing" {
		t.Errorf("e1-w1 is %s after the retry", st.State)
	}
	if got := w.called("claude stop"); len(got) != 1 {
		t.Errorf("stops = %q, want no second one", got)
	}
}

func TestADyingBabysitterWaitsForTheUser(t *testing.T) {
	w := watching(t)
	w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
	w.babysitter(1, sessionDead, 0, "stopped")
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter,
		Request: "thread", Text: "alice commented on a review thread", Key: "babysit:thread:T1:11"})
	w.delivered(1)
	for range 2 * 60 {
		w.tick(false)
		// It crashes as soon as it is resumed.
		w.setState(babysitterName(item, 1), 0, "stopped")
		w.now = w.now.Add(time.Minute)
	}
	if got := w.called("claude --bg --resume"); len(got) != 2 {
		t.Errorf("resumes = %d, want 2, the babysitter's restarts an hour", len(got))
	}
	if st := w.status(item).State; st != workNeedsYou {
		t.Errorf("%s is %s, want needs_you", item, st)
	}
	if got := w.dmsWith("its babysitter died and was resumed 2 times"); len(got) != 1 || len(w.dms) != 1 {
		t.Errorf("DMs = %q, want one about the babysitter", w.dms)
	}
}

func TestAPlannerTimingOutThreeTimesWaitsForTheUser(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "planning")
	w.planner("e1", "running", 4101, "working")
	for m := range 7 * 60 {
		// It keeps working, so it is never nudged, yet never ends a plan.
		if m%20 == 0 {
			w.append(w.dir("e1"), events.Event{At: w.now, Kind: events.KindIntent, Sender: "e1", Text: "comment on o/r#12"})
			w.append(w.dir("e1"), events.Event{At: w.now, Kind: events.KindDone, Sender: "e1", Text: "comment on o/r#12"})
		}
		w.tick(false)
		w.now = w.now.Add(time.Minute)
	}
	if got := w.called("claude stop"); len(got) != 3 {
		t.Errorf("stops = %q, want one per timeout, 3", got)
	}
	if st := w.status("e1").State; st != stateNeedsYou {
		t.Errorf("e1 is %s, want needs_you", st)
	}
	if got := w.dmsWith("e1 needs you: planning timed out 3 times"); len(got) != 1 || len(w.dms) != 1 {
		t.Errorf("DMs = %q, want one about the timeouts", w.dms)
	}
}

func TestThreeRefusedPlansWaitForTheUser(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.epic("e1", "checking")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.planner("e1", "running", 4101, "working")
	// The base branch is not in the registry: no new version fixes that.
	bad := strings.Replace(oneItem("[]"), "{id: w1, repo: r, issue: o/r#14}", "{id: w1, repo: r, issue: o/r#14, base: release}", 1)
	for n := 1; n <= 4; n++ {
		w.end("e1", fmt.Sprintf("epic.v%d.yaml", n), bad)
		w.tick(false)
		w.now = w.now.Add(time.Minute)
	}
	refused := 0
	for _, ev := range w.kinds("e1", events.KindError) {
		if strings.Contains(ev.Text, "is refused") {
			refused++
		}
	}
	if refused != 3 {
		t.Errorf("refused %d versions, want 3", refused)
	}
	if st := w.status("e1").State; st != stateNeedsYou {
		t.Errorf("e1 is %s, want needs_you", st)
	}
	if got := w.dmsWith("3 plan versions in a row were refused; the last: epic.v3.yaml is refused: item w1: base release is not a branch of r in the registry (main). When"); len(got) != 1 || len(w.dms) != 1 {
		t.Errorf("DMs = %q, want one naming the last refusal", w.dms)
	}
}

// snapshotState keeps every index.json and status.json under the brain but
// the account's, and returns a func that puts them back and removes those
// made since: what a tick killed after its side effects and before it saved
// leaves behind.
func snapshotState(t *testing.T, brain string) func() {
	t.Helper()
	keep := map[string][]byte{}
	walk := func(fn func(path string)) {
		filepath.WalkDir(brain, func(path string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && (d.Name() == "status.json" || d.Name() == "index.json") && !strings.Contains(path, "/account/") {
				fn(path)
			}
			return nil
		})
	}
	walk(func(p string) { keep[p], _ = os.ReadFile(p) })
	return func() {
		walk(func(p string) {
			if data, ok := keep[p]; ok {
				os.WriteFile(p, data, 0o644)
			} else {
				os.Remove(p)
			}
		})
	}
}

func TestAKilledTickKeepsTheLeaseItGranted(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.now = w.now.Add(time.Minute)
	restore := snapshotState(t, w.brain)
	// The orchestrator's index entry is written before it starts.
	indexed := false
	orig := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 1 && c.Args[0] == "--bg" && c.Args[1] == "--model" {
			ix, _ := store.ReadIndex(w.brain)
			_, indexed = ix["e1-s1-orchestrator"]
		}
		return orig(c)
	}
	w.tick(false)
	if got := w.called("claude --bg --model"); len(got) != 1 || !strings.Contains(got[0], "lease: 1") {
		t.Fatalf("starts = %q, want one with lease 1", got)
	}
	if !indexed {
		t.Errorf("the orchestrator started before its index entry was written")
	}
	restore()
	if lease, err := Lease(w.dir("e1-s1")); err != nil || lease != 1 {
		t.Errorf("lease after the kill = %d, %v; want 1, the one the orchestrator holds", lease, err)
	}
	for range 3 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if st := w.status("e1-s1"); st.Lease != 1 || st.State != setRunning {
		t.Errorf("set %s with lease %d, want running with lease 1", st.State, st.Lease)
	}
	if got := w.called("claude --bg --model"); len(got) != 1 {
		t.Errorf("starts = %q, want the orchestrator adopted, not started again", got)
	}
}

func TestAnOrchestratorStoppedWhileItsPROpensIsResumed(t *testing.T) {
	// A usage pause stops it; with no PR recorded yet, a dead one is
	// resumed too, since there is nothing to hand over.
	for _, state := range []string{sessionStopped, sessionDead} {
		t.Run(state, func(t *testing.T) {
			w := newWorld(t)
			w.working(workPROpen)
			w.orchestrator("e1-s1", state, 0, "stopped")
			w.tick(false)
			if got := w.called("claude --bg --resume"); len(got) != 1 {
				t.Errorf("resumes = %q, want the orchestrator back to finish its pr-opener", got)
			}
		})
	}
}

func TestAResumedSessionIsToldEveryUnacknowledgedMessage(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "turn_ended", 4209, "done")
	posted := w.listen(s.PID)
	m := w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindInstruction, Sender: "e1", Text: "use the v2 API"})
	w.tick(false)
	if got := posted(); len(got) != 1 {
		t.Fatalf("posted %q, want the message", got)
	}
	// It is stopped before it acts on the message.
	w.append(w.dir("e1-s1-orchestrator"), events.Event{At: w.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: stopText, Key: "stop:test"})
	w.setState("factory:e1-s1:orchestrator", 0, "stopped")
	w.at(time.Minute)
	w.tick(false)
	w.at(2 * time.Minute)
	w.tick(false)
	resumes := w.called("claude --bg --resume")
	if len(resumes) != 1 || !strings.Contains(resumes[0], "seq "+seqRef(m)+", instruction from e1: use the v2 API") {
		t.Errorf("resumes = %q, want the unacknowledged message in the prompt", resumes)
	}
}

func TestNothingStartsOrIsPostedWhileTheAccountHoldsWork(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "turn_ended", 4210, "done")
	posted := w.listen(s.PID)
	w.epic("e2", "checking")
	w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindInstruction, Sender: "e1", Text: "use the v2 API"})
	w.acct.ok = false
	w.tick(false)
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started %q while the account holds work", got)
	}
	if got := posted(); len(got) != 0 {
		t.Errorf("posted %q while the account holds work", got)
	}
}

func TestASessionWhoseOwnerWaitsForTheUserIsNotResumed(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	w.moveTo("e1-s1", stateNeedsYou)
	w.orchestrator("e1-s1", "stopped", 0, "stopped")
	w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindInstruction, Sender: "e1", Text: "use the v2 API"})
	w.tick(false)
	if got := w.called("claude --bg --resume"); len(got) != 0 {
		t.Errorf("resumed %q for a set that waits for the user", got)
	}
}

func TestAHeartbeatUnderAnOldLeaseDoesNotHoldTheNudge(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	if err := store.WriteStatus(w.dir("e1-s1"), store.Status{State: "running", Since: t0, Lease: 2}); err != nil {
		t.Fatal(err)
	}
	s := w.orchestrator("e1-s1", "running", 4211, "working")
	posted := w.listen(s.PID)
	m := w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindInstruction, Sender: "e1", Text: "use the v2 API"})
	for d := time.Duration(0); d <= 15*time.Minute; d += 5 * time.Minute {
		w.at(d)
		// A helper of the orchestrator that lost lease 1 heartbeats.
		w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: "e1-s1", Lease: 1, At: w.now})
		w.tick(false)
	}
	if got := posted(); len(got) != 2 || !strings.Contains(got[1], "message seq "+seqRef(m)+" unacknowledged for 15m") {
		t.Errorf("posted %q, want the delivery then a nudge", got)
	}
}

func TestTheProgramsOwnErrorsAreNotDeadLettered(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	for range 3 {
		w.append(w.dir("e1-w1"), events.Event{At: w.now, Kind: events.KindError, Sender: events.SenderProgram, Text: "explore.md lacks ## Files"})
	}
	w.tick(false)
	if st := w.status("e1-w1").State; st != "implementing" {
		t.Errorf("e1-w1 is %s, want implementing: the program's own errors are not the agent looping", st)
	}
}

func TestABabysitterDyingOnceAnHourStillWaitsForTheUser(t *testing.T) {
	w := watching(t)
	died := func(i int) {
		w.append(w.dir(item), events.Event{At: w.now.Add(time.Duration(i-7) * 61 * time.Minute), Kind: events.KindRestart, Sender: events.SenderProgram,
			Text: babysitterName(item, 1) + " was dead", Key: fmt.Sprintf("restart:dead:%s:%d", babysitterID(item, 1), i)})
	}
	for i := range 5 {
		died(i)
	}
	w.tick(false)
	if st := w.status(item).State; st != workInReview {
		t.Fatalf("%s is %s after 5 restarts an hour apart, want in_review", item, st)
	}
	died(5)
	w.tick(false)
	if st := w.status(item).State; st != workNeedsYou || len(w.dmsWith("its babysitter died and was resumed 6 times")) != 1 {
		t.Errorf("%s is %s with DMs %q, want needs_you after 6, the limit a work item has", item, st, w.dms)
	}
}

func TestOnlyTimeoutsSinceTheEpicEnteredItsStateCount(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "checking")
	long := w.now.Add(-3 * time.Hour)
	move := func(from, to, ref string) {
		w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, At: long, From: from, To: to, Trigger: "tick", TriggerRef: ref})
	}
	move("checking", "checking", "timeout:1")
	move("checking", "checking", "timeout:2")
	move("checking", "planning", "fixture")
	move("planning", "planning", "timeout:3")
	w.tick(false)
	if st := w.status("e1").State; st != epicPlanning {
		t.Errorf("e1 is %s, want planning: its one timeout in planning is under the limit", st)
	}
}
