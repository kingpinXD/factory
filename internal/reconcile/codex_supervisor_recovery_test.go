package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func supervisorTickCall(t *testing.T, brain string, call func() error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	return call()
}

func supervisorPlannerFixture(t *testing.T) (*world, Deps, *codex.Runtime, string) {
	t.Helper()
	t.Setenv("FACTORY_E6_ADVANCE_TURNS", "1")
	w, manager, rt, _ := ordinaryManagerFixture(t)
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	selected.EpicID = "e2"
	if err = executor.WriteSelection(w.brain, selected); err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	b.Values.Inbox.Unacked = blueprint.Duration(100 * time.Millisecond)
	b.Values.Inbox.CheckEvery = blueprint.Duration(100 * time.Millisecond)
	b.Values.Supervision.NoProgress = blueprint.Duration(100 * time.Millisecond)
	raw, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	put(t, blueprint.Path(w.brain), string(raw))
	if err = manager.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	owner, err := manager.create("e2", store.Entry{Kind: blueprint.MachineEpic, Epic: "e2", Dir: w.epicDir("e2")}, &EpicInputs{ID: "e2", Kind: "text", Input: "actual offline planner supervision", AddedAt: manager.now}, blueprint.TriggerUser)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.writeIndex(); err != nil {
		t.Fatal(err)
	}
	id, name := manager.sessionSlot(owner, componentPlanner)
	if err = supervisorTickCall(t, w.brain, func() error { return manager.startSession(owner, componentPlanner, id, name) }); err != nil {
		t.Fatal(err)
	}
	view, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { managerCleanupOwnedChildren(t, w.brain, rt, name) })
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, view.Ref.SessionID); err == nil {
			return w, manager.d, rt, id
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual planner child did not become ready")
	return nil, Deps{}, nil, ""
}

// This calls the real normal observation and supervisor producers. It never
// sets observed/coverage or writes a synthetic availability interval.
func observedSupervisorRun(t *testing.T, d Deps) *run {
	t.Helper()
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	r.prepareAvailability()
	for _, id := range r.order() {
		if r.availability.managed[id] {
			r.codexQuota(r.ents[id], false)
			r.inventory("codex")
		}
	}
	r.observeAvailabilityHolds()
	return r
}

func genuineSupervisorRequest(t *testing.T) (*world, Deps, *codex.Runtime, string, CoordinatorCloseRequest) {
	t.Helper()
	w, d, rt, id := supervisorPlannerFixture(t)
	return w, d, rt, id, genuineSupervisorRequestFromPlanner(t, w, d, rt, id)
}

func genuineSupervisorRequestFromPlanner(t *testing.T, w *world, d Deps, rt *codex.Runtime, id string) CoordinatorCloseRequest {
	t.Helper()
	return genuineSupervisorRequestForState(t, w, d, rt, id, "idle")
}

func genuineSupervisorRequestForState(t *testing.T, w *world, d Deps, rt *codex.Runtime, id, state string) CoordinatorCloseRequest {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// Let the actual queued command and its guarded authority check finish
		// between ticks. This fixture does not run a tick every ten milliseconds
		// while the owned host waits for that same tick lock.
		view, err := rt.InspectCoordinatorAttemptByName(context.Background(), sessionName("e2", componentPlanner))
		if err != nil {
			t.Fatal(err)
		}
		if view.PendingCommands {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		var found *CoordinatorCloseRequest
		err = supervisorTickCall(t, w.brain, func() error {
			observing := time.Now()
			r := observedSupervisorRun(t, d)
			if elapsed := time.Since(observing); elapsed > time.Second {
				t.Logf("actual observation held tick for %s", elapsed)
			}
			supervising := time.Now()
			r.superviseOwner(r.ents["e2"])
			if elapsed := time.Since(supervising); elapsed > time.Second {
				t.Logf("actual supervisor held tick for %s", elapsed)
			}
			for _, ev := range r.ents[id].evs {
				if ev.Recipient != "codex-attempt" || ev.Request != "close" {
					continue
				}
				var request CoordinatorCloseRequest
				if json.Unmarshal([]byte(ev.Text), &request) != nil || request.Mode != "process_replace" {
					continue
				}
				sourceEvent, err := (ProgramEffects{Brain: w.brain}).boundEffectEvent(request.Cause)
				if err != nil {
					return err
				}
				var source coordinatorSupervisorCause
				if json.Unmarshal([]byte(sourceEvent.Text), &source) != nil {
					t.Fatal("actual supervisor source malformed")
				}
				view, err := rt.InspectCoordinatorAttemptByName(r.ctx, r.ents[id].session.Name)
				if err != nil {
					return err
				}
				if view.State == state && !view.PendingCommands && source.State == view.State && !source.PendingCommands {
					copy := request
					found = &copy
				}
			}
			saving := time.Now()
			if err := r.save(); err != nil {
				return err
			}
			if elapsed := time.Since(saving); elapsed > time.Second {
				t.Logf("actual save held tick for %s", elapsed)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if found != nil {
			return *found
		}
		time.Sleep(10 * time.Millisecond)
	}
	view, viewErr := rt.InspectCoordinatorAttemptByName(context.Background(), sessionName("e2", componentPlanner))
	var record codex.Record
	readErr := store.ReadJSON(filepath.Join(store.Factory(w.brain), "codex/sessions", view.Ref.SessionID, "attempts", view.Ref.AttemptID, "state.json"), &record)
	kind, stage := "", ""
	if record.Failure != nil {
		kind, stage = record.Failure.Kind, record.Failure.Stage
	}
	checkError, _ := os.ReadFile(filepath.Join(w.brain, "s6-check-error-"+view.Ref.AttemptID))
	trace, _ := os.ReadFile(filepath.Join(w.brain, "s6-check-trace-"+view.Ref.AttemptID))
	t.Logf("actual private authority child error: %s; trace=%s", checkError, trace)
	t.Fatalf("real normal supervisor did not nudge then record an idle restart; currentStage=%s state=%s pending=%v hold=%q failure=%s/%s viewErr=%v readErr=%v; out=%s", view.Stage, view.State, view.PendingCommands, record.Hold, kind, stage, viewErr, readErr, w.out.String())
	return CoordinatorCloseRequest{}
}

func TestSupervisorActualObservedSourceRevalidatesWithoutForcingFreshClock(t *testing.T) {
	w, d, rt, id, request := genuineSupervisorRequest(t)
	if request.Reason != "shutdown" {
		t.Fatal("healthy supervisor mislabeled host failure")
	}
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	if err := supervisorTickCall(t, w.brain, func() error {
		return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
	}); err != nil {
		t.Fatal("actual recorded eligibility did not revalidate", err)
	}
	fresh, err := readRun(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	fresh.prepareAvailability()
	if fresh.availability.observed {
		t.Fatal("fresh run fabricated normal observation")
	}
	history := w.log("e2")
	nudges, restarts := 0, 0
	for _, ev := range history {
		if ev.Kind == events.KindNudge {
			nudges++
		}
		if ev.Kind == events.KindRestart {
			restarts++
		}
	}
	if nudges == 0 || restarts != 0 {
		t.Fatal("pure process restart precharged item history", nudges, restarts)
	}
}

func TestSupervisorActualPendingSourceRejectsNewMessage(t *testing.T) {
	w, _, rt, id, request := genuineSupervisorRequest(t)
	if _, err := store.AppendTo(w.brain, "e2", events.Event{At: time.Now().UTC(), Kind: events.KindMessage, Sender: senderUser, Text: "new actual user inbox message", Key: "supervisor-new-user-message"}); err != nil {
		t.Fatal(err)
	}
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	err := supervisorTickCall(t, w.brain, func() error {
		return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
	})
	if err == nil {
		t.Fatal("new inbox source failed to revoke NEW supervisor seal")
	}
	view, err := rt.InspectCoordinatorAttemptByName(context.Background(), sessionName("e2", componentPlanner))
	if err != nil || view.Stage != "claimed" {
		t.Fatal("denied new source performed physical closure", err)
	}
}

func TestSupervisorActualSourceMissingCoverageHolds(t *testing.T) {
	w, _, rt, id, request := genuineSupervisorRequest(t)
	overall, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	overall.AvailabilityCoverage = nil
	if err = store.WriteOverall(w.brain, overall); err != nil {
		t.Fatal(err)
	}
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	err = supervisorTickCall(t, w.brain, func() error {
		return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
	})
	if err == nil {
		t.Fatal("missing actual stored coverage became a new stop grant")
	}
}

func TestSupervisorActualRequestClosesOutsideTickAndManagerSelectsSuccessor(t *testing.T) {
	w, d, rt, id, request := genuineSupervisorRequest(t)
	deadline := time.Now().Add(90 * time.Second)
	var view codex.CoordinatorAttemptView
	var lastError error
	changedAfterSeal := false
	for time.Now().Before(deadline) {
		var err error
		view, err = rt.InspectCoordinatorAttempt(context.Background(), request.Attempt.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Ref != request.Attempt {
			t.Fatal("closure replaced selected attempt")
		}
		if view.Stage == "closed" {
			break
		}
		if view.Stage == "sealing" && !changedAfterSeal {
			if _, err = store.AppendTo(w.brain, "e2", events.Event{At: time.Now().UTC(), Kind: events.KindMessage, Sender: senderUser, Text: "actual message after original request was sealed", Key: "supervisor-message-after-seal"}); err != nil {
				t.Fatal(err)
			}
			changedAfterSeal = true
		}
		if !view.NextCheckAt.IsZero() && time.Now().Before(view.NextCheckAt) {
			if err := makeFixtureCodexRecoveryDue(context.Background(), w.brain, rt, request.Attempt, func() error {
				return recoverRequestedCoordinators(context.Background(), w.brain, rt)
			}); err != nil {
				t.Fatal("actual supervisor hold did not become due fixture", err)
			}
			continue
		}
		// This is the ordinary pre-tick consumer. No tick lock surrounds its
		// actual stop/fence. The fixture advances retry time only after proving death.
		lastError = recoverRequestedCoordinators(context.Background(), w.brain, rt)
	}
	if view.Stage != "closed" {
		t.Fatal("actual supervisor closure did not settle", view.Stage, view.NextCheckAt, lastError)
	}
	if view.CloseCause == nil || view.SealReason != "shutdown" {
		t.Fatal("physical closure lost actual supervisor cause")
	}
	if err := rt.ValidateAttemptClosure(context.Background(), codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}); err != nil {
		t.Fatal(err)
	}
	rt.AttemptCapabilities = managerDenyCapabilities{}
	var selected codex.CoordinatorAttemptView
	if err := supervisorTickCall(t, w.brain, func() error {
		r := observedSupervisorRun(t, d)
		if err := r.manageCodexCoordinator(r.ents["e2"], r.ents[id]); err != nil && !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
			return err
		}
		var err error
		selected, err = rt.InspectCoordinatorAttempt(context.Background(), request.Attempt.SessionID)
		if err != nil {
			return err
		}
		return r.save()
	}); err != nil {
		t.Fatal(err)
	}
	if selected.Ref == request.Attempt || selected.Predecessor != request.Attempt || selected.Stage != "prepared" {
		t.Fatal("ordinary manager failed to select actual supervisor successor", selected.Stage)
	}
	if selected.Predecessor != view.Ref {
		t.Fatal("successor changed predecessor")
	}
	for _, ev := range w.log("e2") {
		if ev.Kind == events.KindRestart {
			t.Fatal("pure supervisor process replacement charged item restart")
		}
	}
	count := 0
	for _, ev := range w.log(id) {
		if ev.Kind == events.KindRecovery {
			count++
		}
	}
	if count != 1 {
		t.Fatal("supervisor process replacement count", count)
	}
	jobs, err := (helper.Service{Brain: w.brain}).Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatal("planner recovery fabricated helper execution", err)
	}
}

func TestSupervisorActualCurrentSourceGuardMatrix(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, scenario := range []string{"same_timestamp_progress", "closed_hold_overlap", "policy", "pending_command"} {
		t.Run(scenario, func(t *testing.T) {
			w, _, rt, id, request := genuineSupervisorRequest(t)
			event, err := (ProgramEffects{Brain: w.brain}).boundEffectEvent(request.Cause)
			if err != nil {
				t.Fatal(err)
			}
			var source coordinatorSupervisorCause
			if err = json.Unmarshal([]byte(event.Text), &source); err != nil {
				t.Fatal(err)
			}
			want := effects.ErrStale
			switch scenario {
			case "same_timestamp_progress":
				// This delayed event has a new real sequence at the old progress
				// timestamp. Comparing only progress time cannot detect it.
				if _, err = store.AppendTo(w.brain, "e2", events.Event{At: source.Since, Kind: events.KindHeartbeat, Sender: "e2", Lease: 0, Key: "supervisor-same-time-progress"}); err != nil {
					t.Fatal(err)
				}
			case "closed_hold_overlap":
				overall, err := store.ReadOverall(w.brain)
				if err != nil {
					t.Fatal(err)
				}
				domains := source.Clock.Domains["e2"]
				if len(domains) == 0 {
					t.Fatal("source omitted actual clock domain")
				}
				// Negative stored metadata overlaps already-credited time; it
				// must revoke a NEW seal even though the hold is now closed.
				overall.AvailabilityHolds = append(overall.AvailabilityHolds, store.AvailabilityHold{Scope: domains[len(domains)-1].Scope, Source: "runtime", SourceID: "unit-overlap-negative", Reason: "inspection_unknown", From: source.Clock.ObservedAt.Add(-50 * time.Millisecond), To: source.Clock.ObservedAt.Add(-10 * time.Millisecond)})
				if err = store.WriteOverall(w.brain, overall); err != nil {
					t.Fatal(err)
				}
			case "policy":
				b, err := blueprint.Load(blueprint.Path(w.brain))
				if err != nil {
					t.Fatal(err)
				}
				b.Values.Inbox.Unacked *= 2
				data, err := yaml.Marshal(b)
				if err != nil {
					t.Fatal(err)
				}
				put(t, blueprint.Path(w.brain), string(data))
			case "pending_command":
				want = codex.ErrAttemptStale
			}
			authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
			err = supervisorTickCall(t, w.brain, func() error {
				if scenario == "pending_command" {
					_, ref, err := codex.CoordinatorAttemptStartAuthority(w.brain, request.Attempt.SessionID)
					if err != nil {
						return err
					}
					if err = rt.Post(context.Background(), deliveryRef(ref, "supervisor-actual-new-command"), "actual new queued command after source capture"); err != nil {
						return err
					}
					view, err := rt.InspectCoordinatorAttemptByName(context.Background(), sessionName("e2", componentPlanner))
					if err != nil || !view.PendingCommands {
						t.Fatal("actual queued command was not retained", err)
					}
				}
				return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
			})
			if !errors.Is(err, want) {
				t.Fatal("current source did not fail at its intended guard", scenario, err, "want", want)
			}
			view, err := rt.InspectCoordinatorAttemptByName(context.Background(), sessionName("e2", componentPlanner))
			if err != nil || view.Stage != "claimed" || view.Ref != request.Attempt {
				t.Fatal("denied current source changed physical attempt", err)
			}
		})
	}
}
