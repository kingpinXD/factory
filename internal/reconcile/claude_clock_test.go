package reconcile

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestClaudeUnknownCoordinatorPausesTimeoutAcrossTicks(t *testing.T) {
	w := newWorld(t)
	w.working(stateStarting)
	failed := true
	original := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "agents" && failed {
			return nil, errors.New("listing unavailable")
		}
		return original(c)
	}
	w.at(5 * time.Minute)
	w.tick(false)
	if got := w.status("e1-w1").HeldSince; !got.Equal(t0.Add(5 * time.Minute)) {
		t.Fatalf("first refusal hold=%v, want first observed refusal at 5m", got)
	}
	w.at(66 * time.Minute)
	w.tick(false)
	st := w.status("e1-w1")
	if st.State != stateStarting || st.Attempt != 0 || len(w.kinds("e1-w1", events.KindRestart)) != 0 {
		t.Fatalf("unavailable coordinator spent timeout/restart budget: %+v", st)
	}
	r := inventoryRun(t, w, w.deps())
	if got := r.elapsed(r.ents["e1-w1"], t0); got != 5*time.Minute {
		t.Fatalf("reloaded supervision elapsed=%v, want 5m", got)
	}
	failed = false
	w.at(67 * time.Minute)
	w.tick(false)
	st = w.status("e1-w1")
	if !st.HeldSince.IsZero() || st.Held != 62*time.Minute {
		t.Fatalf("recovery did not close observed hold: %+v", st)
	}
	w.at(121 * time.Minute)
	w.tick(false)
	if got := w.status("e1-w1").Attempt; got != 0 {
		t.Fatalf("recovery immediately spent held time: attempt=%d", got)
	}
	w.at(123 * time.Minute)
	w.tick(false)
	if got := w.status("e1-w1").Attempt; got != 1 {
		t.Fatalf("eligible clock did not resume: attempt=%d", got)
	}
}

func TestClaudeCoordinatorListingHoldDoesNotPauseIndependentSet(t *testing.T) {
	w := newWorld(t)
	w.working(stateStarting)
	w.set("e1", "e1-s2", setRunning, "e1-w2")
	w.item("e1", "e1-s2", "e1-w2", stateStarting)
	w.append(w.dir("e1-w2"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w2")
	w.orchestrator("e1-s2", sessionRunning, 8502, listedWorking)
	r := inventoryRun(t, w, w.deps())
	// One owner was observed while the listing was unavailable; the other
	// has a known session. The refusal must only pause the first scope.
	r.observeClaudeCoordinator("e1", sessionName("e1-s1", componentOrchestrator), session.Lookup{Presence: session.Unknown})
	r.lookupSession("e1", sessionName("e1-s2", componentOrchestrator))
	r.now = t0.Add(61 * time.Minute)
	for _, id := range []string{"e1-w1", "e1-w2"} {
		r.tickEntity(r.ents[id])
	}
	if st := r.ents["e1-w1"].cur; st.Attempt != 0 || st.HeldSince.IsZero() {
		t.Fatalf("unknown owner did not retain hold: %+v", st)
	}
	if st := r.ents["e1-w2"].cur; st.Attempt != 1 || !st.HeldSince.IsZero() {
		t.Fatalf("known independent owner lost normal timeout: %+v", st)
	}
}

func TestClaudeFailedStartsKeepTimeoutAndRestartBudget(t *testing.T) {
	w := newWorld(t)
	w.working(stateStarting)
	w.listSession("someone-else", 9001, listedWorking)
	original := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "--bg" {
			return nil, errors.New("installed Claude cannot start")
		}
		return original(c)
	}
	w.tick(false)
	w.at(61 * time.Minute)
	w.tick(false)
	st := w.status("e1-w1")
	if st.State != stateStarting || st.Attempt != 1 || !st.HeldSince.IsZero() || st.Held != 0 {
		t.Fatalf("failed starts must retain normal starting timeout: %+v", st)
	}
	if got := w.moves("e1-w1"); strings.Join(got, ",") != "→starting,starting→starting" {
		t.Fatalf("timeout moves = %v, want the same moves as TestTimeoutsRestartAndAreCounted", got)
	}
	if restarts := w.kinds("e1-w1", events.KindRestart); len(restarts) != 1 || restarts[0].Text != "starting timed out" {
		t.Fatalf("restarts = %+v, want one counted starting timeout", restarts)
	}
	if len(w.called("claude agents")) != 2 || len(w.called("claude --bg")) == 0 || !strings.Contains(w.out.String(), "installed Claude cannot start") {
		t.Fatalf("fixture did not list healthily and fail starts: %s", w.out.String())
	}
}

func TestClaudeCoordinatorAndAccountHoldOverlapCountsOnce(t *testing.T) {
	w := newWorld(t)
	w.working("exploring")
	w.at(time.Hour)
	r := inventoryRun(t, w, w.deps())
	r.sup.holds = []store.Hold{{From: t0.Add(10 * time.Minute), To: t0.Add(30 * time.Minute), State: statePaused}}
	raw := `{"coordinator_holds":[{"owner_id":"e1-s1","from":"` + t0.Add(20*time.Minute).Format(time.RFC3339) + `","to":"` + t0.Add(40*time.Minute).Format(time.RFC3339) + `","reason":"session listing is unknown"}]}`
	if err := json.Unmarshal([]byte(raw), &r.sup.prev); err != nil {
		t.Fatal(err)
	}
	if got := r.elapsed(r.ents["e1-w1"], t0); got != 30*time.Minute {
		t.Fatalf("overlapping account/coordinator elapsed=%v, want 30m", got)
	}
	if got := r.digestAge(r.ents["e1-w1"]); got != time.Hour {
		t.Fatalf("display clock changed: %v", got)
	}
}

func TestClaudeNumberedBabysitterHoldExcludesPreparationRetry(t *testing.T) {
	w := newWorld(t)
	w.working(workInReview)
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindHandover})
	st := w.status("e1-w1")
	st.PRState = prOpen
	if err := store.WriteStatus(w.dir("e1-w1"), st); err != nil {
		t.Fatal(err)
	}
	w.babysitter(1, sessionFinished, 0, "stopped")
	w.babysitter(2, sessionRunning, 8501, listedWorking)
	r := inventoryRun(t, w, w.deps())
	r.inventories = map[string]session.Inventory{"claude": {Complete: false}}
	r.lookupSession("e1", babysitterName("e1-w1", 2))
	if !r.held(r.ents["e1-w1"]) {
		t.Fatal("numbered babysitter refusal did not hold its item")
	}
	r.ents["e1-w1"].cur.State = "queued"
	if r.held(r.ents["e1-w1"]) {
		t.Fatal("old babysitter hold froze a new preparation retry")
	}
}

func TestClaudeFailedWakeKeepsTimeoutAndRestartBudget(t *testing.T) {
	w := newWorld(t)
	w.working(stateStarting)
	w.orchestrator("e1-s1", sessionStarting, 0, "stopped")
	original := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 1 && c.Args[0] == "--bg" && c.Args[1] == "--resume" {
			return nil, errors.New("installed Claude cannot resume")
		}
		return original(c)
	}
	r := inventoryRun(t, w, w.deps())
	for _, at := range []time.Duration{0, 61 * time.Minute} {
		r.now = t0.Add(at)
		if err := r.wake(r.ents["e1-s1-orchestrator"]); err == nil || !strings.Contains(err.Error(), "installed Claude cannot resume") {
			t.Fatalf("resume error = %v, want fixture failure", err)
		}
	}
	r.tickEntity(r.ents["e1-w1"])
	r.countOtherRestarts()
	if st := r.ents["e1-w1"].cur; st.Attempt != 1 || !st.HeldSince.IsZero() || st.Held != 0 {
		t.Fatalf("failed resumes changed normal timeout: %+v", st)
	}
	if got := w.kinds("e1-w1", events.KindRestart); len(got) != 1 || got[0].Text != "starting timed out" {
		t.Fatalf("resume timeout restart budget = %+v, want one starting timeout", got)
	}
	if got := len(w.called("claude --bg --resume")); got != 2 {
		t.Fatalf("resume calls = %d, want two failures", got)
	}
}
