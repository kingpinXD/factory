package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func TestTerminalStopActualRequestCapturesBeforeTransition(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, d, _, _ := supervisorPlannerFixture(t)
	if err := Stop(context.Background(), d, "e2", "UNIT actual public stop request"); err != nil {
		t.Fatal(err)
	}
	if err := supervisorTickCall(t, w.brain, func() error {
		r := observedSupervisorRun(t, d)
		r.applyRequests(r.ents["e2"])
		return r.save()
	}); err != nil {
		t.Fatal(err)
	}
	for _, ev := range w.log("e2") {
		if ev.Kind == events.KindIntent && ev.Sender == events.SenderProgram && ev.Recipient == "codex-attempt" && ev.Request == "terminal-transition" {
			if _, err := readCoordinatorTerminal(w.brain, "e2", ev); err != nil {
				t.Fatal("actual terminal source failed its typed reader", err)
			}
			for _, logged := range w.log(sessionID("e2", componentPlanner)) {
				if logged.Kind == events.KindRequest && logged.Request == "close" {
					return
				}
			}
			t.Fatal("actual source was written without the bound close request")
		}
	}
	t.Fatalf("actual public Stop did not capture its coordinator admission before invalidation: %s", w.out.String())
}

func TestTerminalStopActualPreTickClosesOriginalAndNeverSelectsSuccessor(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, d, rt, id := supervisorPlannerFixture(t)
	name := sessionName("e2", componentPlanner)
	original, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := nativeAuthority(context.Background(), w.brain, "e2", "e2", true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if err = Stop(context.Background(), d, "e2", "UNIT actual pre-tick stop"); err != nil {
		t.Fatal(err)
	}
	if err = supervisorTickCall(t, w.brain, func() error { r := observedSupervisorRun(t, d); r.applyRequests(r.ents["e2"]); return r.save() }); err != nil {
		t.Fatal(err)
	}
	rootHistory := w.log("e2")
	for _, ev := range rootHistory {
		if ev.Kind == events.KindTransition && ev.To == workCancelled {
			t.Fatal("root became terminal before actual physical closure")
		}
	}
	if _, _, err = nativeAuthority(context.Background(), w.brain, "e2", "e2", true, &old, b2Qualification); !errors.Is(err, errCoordinatorTerminalPending) {
		t.Fatal("pending terminal failed to deny new admission", err)
	}
	if _, _, err = nativeOutputRecoveryAuthority(context.Background(), w.brain, old, b2Qualification); err != nil {
		t.Fatal("pending terminal denied retained prepared-output policy", err)
	}
	var request CoordinatorCloseRequest
	found := false
	for _, ev := range w.log(id) {
		if ev.Request == "close" {
			if err = json.Unmarshal([]byte(ev.Text), &request); err != nil {
				t.Fatal(err)
			}
			found = true
		}
	}
	if !found || request.Mode != "explicit_stop" || request.Reason != "cancelled" || request.Attempt != original.Ref {
		t.Fatal("missing exact admitted Stop close request")
	}
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	if err = supervisorTickCall(t, w.brain, func() error {
		return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
	}); err != nil {
		t.Fatal("own admitted Stop denied itself", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	var view codex.CoordinatorAttemptView
	var last error
	for time.Now().Before(deadline) {
		view, err = rt.InspectCoordinatorAttemptByName(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if view.Ref != original.Ref {
			t.Fatal("Stop replaced original attempt")
		}
		if view.Stage == "closed" {
			break
		}
		if !view.NextCheckAt.IsZero() && time.Now().Before(view.NextCheckAt) {
			if err := makeFixtureCodexRecoveryDue(context.Background(), w.brain, rt, original.Ref, func() error {
				return recoverModernCoordinators(context.Background(), d)
			}); err != nil {
				t.Fatal("actual terminal hold did not become due fixture", err)
			}
			continue
		}
		last = recoverModernCoordinators(context.Background(), d)
	}
	if view.Stage != "closed" {
		t.Fatal("actual Stop closure did not settle", view.Stage, view.NextCheckAt, last)
	}
	if err = supervisorTickCall(t, w.brain, func() error {
		r := observedSupervisorRun(t, d)
		r.applyRequests(r.ents["e2"])
		r.manageCodexCoordinators()
		return r.save()
	}); err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	root, err := loadEntity("e2", ix["e2"])
	if err != nil || root.cur.State != workCancelled {
		t.Fatal("actual admitted Stop did not publish terminal transition", err)
	}
	current, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil || current.Ref != original.Ref || current.Stage != "closed" {
		t.Fatal("explicit Stop selected a successor", err)
	}
}

func TestTerminalStopReadyNoJobWorkCompletesActualClosedFence(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, r, rt, owner, item, _, name := normalManagerWorkFixture(t)
	rt.AttemptCapabilities = b2AttemptCaps{}
	parent, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err = rt.LaunchAttempt(context.Background(), parent.Ref); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err = codex.CoordinatorAttemptStartAuthority(w.brain, parent.Ref.SessionID); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, _, err = codex.CoordinatorAttemptStartAuthority(w.brain, parent.Ref.SessionID); err != nil {
		t.Fatal("actual shared parent did not initialize", err)
	}
	service := helper.Service{Brain: w.brain, Authority: &fenceUnitAuthority{NativeAuthority: NativeAuthority{w.brain}}}
	r.d.Helpers = &service
	if jobs, err := service.Jobs(); err != nil || len(jobs) != 0 {
		t.Fatal("fixture has unexpected helper execution", err)
	}
	if err = Stop(context.Background(), r.d, item.id, "UNIT actual ready no-job stop"); err != nil {
		t.Fatal(err)
	}
	if err = supervisorTickCall(t, w.brain, func() error {
		fresh := observedSupervisorRun(t, r.d)
		fresh.applyRequests(fresh.ents[item.id])
		records, err := terminalRecordsAt(w.brain, item.id, fresh.ix[item.id])
		if err != nil || len(records) != 1 {
			t.Fatal("actual ready no-job Stop was not captured", err, fresh.errs)
		}
		return fresh.save()
	}); err != nil {
		t.Fatal(err)
	}
	if err = recoverCoordinatorTerminalFences(context.Background(), r.d, rt); err != nil {
		t.Fatal("ordinary work control recovery failed before closed-state assertion", err)
	}
	ref, err := service.ReadClosedScopeFence(context.Background(), item.id)
	if err != nil {
		t.Fatal("actual helper fence did not reach closed", err)
	}
	closed, err := helper.ReadClosedScopeFenceLocal(context.Background(), w.brain, ref)
	if err != nil || len(closed.Scopes) != 1 || len(closed.Descendants) != 0 {
		t.Fatal("no actual no-job closed scope", err)
	}
	if err = supervisorTickCall(t, w.brain, func() error {
		fresh := observedSupervisorRun(t, r.d)
		records, err := terminalRecordsAt(w.brain, item.id, fresh.ix[item.id])
		if err != nil {
			return err
		}
		if len(records) != 1 {
			return errors.New("missing actual no-job terminal source")
		}
		return fresh.coordinatorTerminalComplete(records[0])
	}); err != nil {
		t.Fatal("actual closed work fence was not accepted by terminal completion", err)
	}
	current, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil || current.Ref != parent.Ref || current.Stage != "claimed" || current.OwnerID != owner.id {
		t.Fatal("single-work stop disturbed healthy shared coordinator", err)
	}
}
