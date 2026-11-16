package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

func terminalProspectivePlanFixture(t *testing.T) (*world, Deps, *run) {
	t.Helper()
	w, r, _, _ := ordinaryManagerFixture(t)
	w.registry()
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	selected.EpicID = "e3"
	if err = executor.WriteSelection(w.brain, selected); err != nil {
		t.Fatal(err)
	}
	if _, err = r.create("e3", store.Entry{Kind: blueprint.MachineEpic, Epic: "e3", Dir: w.epicDir("e3")}, &EpicInputs{ID: "e3", Kind: "text", Input: "UNIT actual first plan", AddedAt: r.now}, blueprint.TriggerUser); err != nil {
		t.Fatal(err)
	}
	if err = r.writeIndex(); err != nil {
		t.Fatal(err)
	}
	w.end("e3", "state-check.v1.md", stateCheck)
	w.end("e3", "epic.v1.yaml", twoItems("[]"))
	fresh, err := newRun(context.Background(), r.d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	return w, r.d, fresh
}

func TestTerminalMembershipFirstPlanPreservesWorkBeforeSet(t *testing.T) {
	w, _, r := terminalProspectivePlanFixture(t)
	if err := supervisorTickCall(t, w.brain, func() error { r.advance(r.ents["e3"]); return r.save() }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"e3-w1", "e3-w2", "e3-s1", "e3-s2"} {
		if r.ents[id] == nil {
			t.Fatalf("actual first plan did not create %s in its first application: %s", id, w.out.String())
		}
	}
	if !slices.Equal(r.ents["e3-s1"].set.Items, []string{"e3-w1"}) || !slices.Equal(r.ents["e3-s2"].set.Items, []string{"e3-w2"}) {
		t.Fatal("first-plan membership changed")
	}
	if len(r.errs) > 0 {
		t.Fatal(r.errs)
	}
}

func TestTerminalMembershipActualUnindexedRegistrationSurvivesMissingStatus(t *testing.T) {
	w, d, r := terminalProspectivePlanFixture(t)
	if err := supervisorTickCall(t, w.brain, func() error { r.advance(r.ents["e3"]); return nil }); err != nil {
		t.Fatal(err)
	}
	if r.ents["e3-s1"] == nil {
		t.Fatal("actual creation before index publication failed", r.errs)
	}
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := ix["e3-s1"]; found {
		t.Fatal("fixture published index before its interrupted save")
	}
	setDir := filepath.Join(w.epicDir("e3"), "sets/e3-s1")
	before, err := os.ReadFile(store.EventsPath(setDir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(store.StatusPath(setDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture did not stop before status publication", err)
	}
	fresh, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	if err = supervisorTickCall(t, w.brain, func() error {
		if err := fresh.applyPlan(fresh.ents["e3"]); err != nil {
			return err
		}
		return fresh.save()
	}); err != nil {
		t.Fatal("genuine unindexed first registration was not recovered", err)
	}
	after, err := os.ReadFile(store.EventsPath(setDir))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("retained registration was duplicated or replaced")
	}
	if !slices.Equal(fresh.ents["e3-s1"].set.Items, []string{"e3-w1"}) {
		t.Fatal("retained member was lost")
	}
}

func TestTerminalMembershipPartialParentIsHeldWithoutOverwrite(t *testing.T) {
	w, d, r := terminalProspectivePlanFixture(t)
	if err := supervisorTickCall(t, w.brain, func() error { r.advance(r.ents["e3"]); return nil }); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(w.epicDir("e3"), "sets/e3-s1")
	if err := os.Remove(store.InputsPath(dir)); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.EventsPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	err = supervisorTickCall(t, w.brain, func() error { return fresh.applyPlan(fresh.ents["e3"]) })
	if err == nil {
		t.Fatal("partial authoritative metadata was called prospective")
	}
	if _, err = os.Stat(store.InputsPath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery rewrote missing authoritative inputs", err)
	}
	after, err := os.ReadFile(store.EventsPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("partial parent history was overwritten")
	}
	if fresh.ents["e3-s2"] == nil {
		t.Fatal("independent known first-registration parent did not progress")
	}
}

func TestTerminalStopUnstartedWorkNamesMissingNoExecutionProof(t *testing.T) {
	w, d, r := terminalProspectivePlanFixture(t)
	if err := supervisorTickCall(t, w.brain, func() error { r.advance(r.ents["e3"]); return r.save() }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"e3-w1", "e3-w2"} {
		item := r.ents[id]
		if item == nil {
			t.Fatal("actual first plan did not register unstarted member")
		}
		if _, ready := worktreeReady(item); ready {
			t.Fatal("fixture manufactured worktree readiness")
		}
		if _, _, found, err := nativeReadActivation(item); err != nil || found {
			t.Fatal("fixture manufactured WORK activation", err)
		}
	}
	if err := Stop(context.Background(), d, "e3", "UNIT stop genuinely unstarted work"); err != nil {
		t.Fatal(err)
	}
	if err := supervisorTickCall(t, w.brain, func() error {
		fresh, err := newRun(context.Background(), d, false)
		if err != nil {
			return err
		}
		if err = fresh.loadBlueprint(); err != nil {
			return err
		}
		fresh.attachMachines()
		fresh.applyRequests(fresh.ents["e3"])
		if fresh.ents["e3"].cur.State == workCancelled {
			t.Fatal("missing no-execution closure proof became success")
		}
		if !strings.Contains(strings.Join(fresh.errs, "\n"), "terminal work scope") {
			t.Fatal("missing proof was not reported at actual source", fresh.errs)
		}
		return fresh.save()
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"e3-w1", "e3-w2"} {
		for _, ev := range w.log(id) {
			if ev.Kind == events.KindStart || ev.Recipient == codexAuthorityRecipient {
				t.Fatal("Stop forced work execution or activation")
			}
		}
	}
	for _, ev := range w.log("e3") {
		if ev.Request == "terminal-transition" {
			t.Fatal("unknown work cohort was captured as complete")
		}
	}
}
