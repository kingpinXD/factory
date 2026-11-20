package reconcile

import (
	"context"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

func TestProcessRecoveryBudgetCountsActualSelectionsOnce(t *testing.T) {
	w, _, _ := b2World(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	name := "factory:e1-s1:orchestrator"
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions/orchestrator"), sessionStarting, &SessionInputs{Name: name, Component: componentOrchestrator, Task: "e1-s1"})
	selected, err := NativeSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	prompt := filepath.Join(w.brain, "budget-prompt")
	put(t, prompt, "Offline accounting contract; no provider launch.")
	rt := b2Runtime(w.brain, binary)
	spec := session.Spec{Name: name, Task: "e1-s1", EpicID: "e1", Generation: "e1-s1:1:orchestrator", SelectionHash: selectionHash(selected), Cwd: w.dir("e1-s1"), PromptFile: prompt, Prompt: "fixture", Model: executor.CodexModel, Effort: "low"}
	old, err := rt.PrepareAttempt(context.Background(), spec, "accounting-initial")
	if err != nil {
		t.Fatal(err)
	}
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"codex": rt}
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	owner, sess := r.ents["e1-s1"], r.ents["e1-s1-orchestrator"]
	c := r.b.Components[componentOrchestrator]
	c.Limits.RestartsPerHour = 1
	r.b.Components[componentOrchestrator] = c
	if err = rt.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	next, err := rt.SelectSuccessor(context.Background(), old, "actual-manager-cause-1", spec)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := rt.ValidateProcessRecovery(context.Background(), old, next, "actual-manager-cause-1")
	if err != nil {
		t.Fatal(err)
	}
	if err = r.recordProcessRecoveryUsing(sess, proof, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if err = r.recordProcessRecoveryUsing(sess, proof, b2Qualification); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, ev := range w.log(sess.id) {
		if ev.Kind == events.KindRecovery {
			count++
		}
	}
	if count != 1 || len(itemRestarts(r.ents["e1-w1"])) != 0 || isNudgeOrRestart(events.Event{Kind: events.KindRecovery}) {
		t.Fatal("recovery duplicated or entered task-failure budget")
	}
	budget, err := r.processRecoveryBudget(owner)
	if err != nil || budget.Count != 1 || budget.Allowed || !budget.NextCheckAt.Equal(r.now.Add(time.Hour)) {
		t.Fatalf("actual rolling hold: %+v %v", budget, err)
	}
	if err = rt.CloseAttempt(context.Background(), next, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	newer, err := rt.SelectSuccessor(context.Background(), next, "actual-manager-cause-2", spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := rt.ValidateProcessRecovery(context.Background(), next, newer, "actual-manager-cause-2")
	if err != nil {
		t.Fatal(err)
	}
	if err = r.recordProcessRecoveryUsing(sess, second, b2Qualification); err != nil {
		t.Fatal(err)
	}
	budget, err = r.processRecoveryBudget(owner)
	if err != nil || budget.Count != 2 || budget.Allowed {
		t.Fatalf("two actual recoveries lost historical first: %+v %v", budget, err)
	}
	w.append(w.dir(owner.id), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	if err = r.recordProcessRecoveryUsing(sess, second, b2Qualification); err == nil {
		t.Fatal("changed B2 authority recorded new recovery")
	}
	budget, err = r.processRecoveryBudget(owner)
	if err != nil || budget.Count != 2 {
		t.Fatalf("current authority change erased historical accounting: %+v %v", budget, err)
	}
	r.now = r.now.Add(time.Hour)
	budget, err = r.processRecoveryBudget(owner)
	if err != nil || budget.Count != 0 || !budget.Allowed || !budget.NextCheckAt.IsZero() {
		t.Fatalf("rolling expiry failed without budget reset: %+v %v", budget, err)
	}
}
