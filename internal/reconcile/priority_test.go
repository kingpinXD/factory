package reconcile

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

// makeRepair turns epic id into a repair of o/r@main.
func (w *world) makeRepair(id string) {
	w.t.Helper()
	in := w.epicInputs(id)
	in.Kind, in.Repo, in.Base = epicKindRepair, "o/r", "main"
	if err := store.WriteInputs(w.dir(id), &in); err != nil {
		w.t.Fatal(err)
	}
}

// accountIs makes the account step answer state, and the account's
// status.json say it.
func (w *world) accountIs(state string) {
	w.acct.state = state
	put(w.t, store.StatusPath(accountDir(w.brain)), `{"state": "`+state+`"}`+"\n")
}

// forRepairAndNot runs test once for a repair epic and once for a normal
// one; test makes epic e1 a repair when repair is set.
func forRepairAndNot(t *testing.T, test func(t *testing.T, repair bool)) {
	for _, repair := range []bool{true, false} {
		name := "normal epic"
		if repair {
			name = "repair epic"
		}
		t.Run(name, func(t *testing.T) { test(t, repair) })
	}
}

func TestARepairItemStartsOnARedBase(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	oneSet(w)
	w.makeRepair("e1")
	w.epic("e2", "running")
	w.set("e2", "e2-s1", "queued", "e2-w1")
	w.item("e2", "e2-s1", "e2-w1", "queued")
	w.red["o/r@main"] = true
	w.tick(false)
	if got := w.moves("e1-w1"); !reflect.DeepEqual(got, []string{"→queued", "queued→starting"}) {
		t.Errorf("repair item moves = %v, want it started on the red base\n%s", got, w.out.String())
	}
	if got := w.moves("e2-w1"); len(got) != 1 {
		t.Errorf("e2-w1 moves = %v, want it held in queued by the red base", got)
	}
}

func TestNearLimitTheRepairPlannerStartsAndANormalOneDoesNot(t *testing.T) {
	forRepairAndNot(t, func(t *testing.T, repair bool) {
		w := newWorld(t)
		w.epic("e1", "checking")
		if repair {
			w.makeRepair("e1")
		}
		w.accountIs(stateNearLimit)
		w.tick(false)
		if got := w.called("claude --bg"); (len(got) == 1 && strings.Contains(got[0], "factory:e1:planner")) != repair {
			t.Errorf("starts = %q, want the planner started only for a repair", got)
		}
	})
}

func TestNearLimitTheRepairItemAndOrchestratorStart(t *testing.T) {
	forRepairAndNot(t, func(t *testing.T, repair bool) {
		w := newWorld(t)
		w.listSession("someone-else", 1, "working")
		oneSet(w)
		if repair {
			w.makeRepair("e1")
		}
		w.accountIs(stateNearLimit)
		w.tick(false)
		if started := len(w.moves("e1-w1")) == 2; started != repair {
			t.Fatalf("e1-w1 moves = %v, want it started only for a repair", w.moves("e1-w1"))
		}
		if !repair {
			return
		}
		w.readyWorktree("e1-w1")
		w.at(time.Minute)
		w.tick(false)
		startArgs(t, w, "factory:e1-s1:orchestrator")
	})
}

func TestNearLimitTheRepairBabysitterIsWokenTwice(t *testing.T) {
	forRepairAndNot(t, func(t *testing.T, repair bool) {
		w := watching(t)
		if repair {
			w.makeRepair("e1")
		}
		w.accountIs(stateNearLimit)
		s := w.babysitter(1, sessionTurnEnded, 5300, listedTurnEnded)
		posted := w.listen(s.PID)
		w.pulls.threads[5] = []gh.Thread{thread("T1", 11, "alice")}
		w.tick(false)
		w.at(5 * time.Minute)
		w.pulls.threads[5][0] = thread("T1", 12, "alice")
		w.fullNext()
		w.tick(false)
		want := 0
		if repair {
			want = 2
		}
		if got := posted(); len(got) != want {
			t.Errorf("posted %q, want %d wakes", got, want)
		}
		if got := w.called("claude --bg"); len(got) != 0 {
			t.Errorf("started %q while a babysitter is live", got)
		}
	})
}

func TestNearLimitATurnEndedRepairSessionIsNudgedAndItsClocksRun(t *testing.T) {
	forRepairAndNot(t, func(t *testing.T, repair bool) {
		w := newWorld(t)
		w.working("planning")
		if repair {
			w.makeRepair("e1")
		}
		w.accountIs(stateNearLimit)
		s := w.orchestrator("e1-s1", "turn_ended", 4310, "done")
		posted := w.listen(s.PID)
		w.tick(false)
		w.at(10 * time.Minute)
		w.tick(false)
		if nudged := len(posted()) == 1; nudged != repair {
			t.Errorf("posted %q, want one nudge only for a repair", posted())
		}
		if held := !w.status("e1-w1").HeldSince.IsZero(); held == repair {
			t.Errorf("e1-w1 status = %+v, want its clock held only for a normal epic", w.status("e1-w1"))
		}
	})
}

func TestNearLimitARepairSessionWithNoProgressIsNudgedAndItsLeaseExpiresOnTime(t *testing.T) {
	forRepairAndNot(t, func(t *testing.T, repair bool) {
		w := newWorld(t)
		w.working("implementing")
		if repair {
			w.makeRepair("e1")
		}
		w.accountIs(stateNearLimit)
		s := w.orchestrator("e1-s1", "running", 4311, "working")
		posted := w.listen(s.PID)
		w.tick(false)
		w.at(31 * time.Minute)
		w.tick(false)
		wantLease := 1
		if repair {
			wantLease = 2
		}
		if st := w.status("e1-s1"); st.Lease != wantLease {
			t.Errorf("lease = %d at 31m, want %d", st.Lease, wantLease)
		}
		w.at(45 * time.Minute)
		w.tick(false)
		got := posted()
		if nudged := len(got) == 1 && strings.Contains(got[0], "no progress on e1-w1 (implementing) for 45m"); nudged != repair {
			t.Errorf("posted %q at 45m, want one no-progress nudge only for a repair", got)
		}
	})
}

func TestNothingOfARepairStartsOrResumesWhileTheAccountHoldsAllWork(t *testing.T) {
	for _, state := range []string{statePaused, stateStale, stateResuming} {
		t.Run(state, func(t *testing.T) {
			w := newWorld(t)
			w.working("implementing")
			w.makeRepair("e1")
			w.orchestrator("e1-s1", "stopped", 0, "stopped")
			w.set("e1", "e1-s2", "queued", "e1-w2")
			w.item("e1", "e1-s2", "e1-w2", "queued")
			w.epic("e2", "checking")
			w.makeRepair("e2")
			w.accountIs(state)
			w.tick(false)
			if got := w.called("claude --bg"); len(got) != 0 {
				t.Errorf("started or resumed %q while the account is %s", got, state)
			}
			if got := w.moves("e1-w2"); len(got) != 1 {
				t.Errorf("e1-w2 moves = %v, want it held in queued", got)
			}
		})
	}
}
