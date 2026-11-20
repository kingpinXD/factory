package reconcile

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
)

func boundaryRun(t *testing.T, w *world, s helper.Service) *run {
	t.Helper()
	d := w.deps()
	d.Helpers = &s
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	r.useAccount(stateOK, nil)
	return r
}

type boundaryBackend struct {
	dBackend
	fence func(context.Context, helper.Job) error
}

func (b boundaryBackend) Fence(ctx context.Context, j helper.Job) error { return b.fence(ctx, j) }

func TestNativeBoundaryUnconfirmedDeathPreventsCompactTransitionAndCoordinatorStop(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.orchestrator(req.SetID, sessionTurnEnded, 9412, listedTurnEnded)
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend := boundaryBackend{dBackend: dBackend{check: func(ctx context.Context, _ helper.RunSpec) error { close(started); <-ctx.Done(); return ctx.Err() }}, fence: func(context.Context, helper.Job) error { return errors.New("descendant death unconfirmed") }}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	go func() { done <- helper.Run(ctx, deps) }()
	defer func() { cancel(); <-done }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("native turn did not start")
	}
	s.FenceRuntime = func(context.Context, helper.Job) error { return errors.New("descendant death unconfirmed") }
	r := boundaryRun(t, w, s)
	sess := r.ents[sessionID(req.SetID, componentOrchestrator)]
	if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:unconfirmed"); err == nil || moved || !strings.Contains(err.Error(), "descendant death unconfirmed") {
		t.Fatalf("unsafe compact transition: %v %v", moved, err)
	}
	if sess.cur.State != sessionTurnEnded {
		t.Fatalf("compaction state persisted through unconfirmed fencing: %s", sess.cur.State)
	}
	if calls := w.called("claude stop"); len(calls) != 0 {
		t.Fatalf("coordinator stopped before descendant barrier: %v", calls)
	}
	recovered := boundaryRun(t, w, s)
	recovered.recoverHelperOwnership()
	if !nativeBoundaryState(recovered.ents[sess.id]).pending() {
		t.Fatal("uncertain boundary was cleared after crash")
	}
	if _, err = s.RecoverScope(ctx, req.WorkID); err == nil {
		t.Fatal("revoking or uncertain scope reopened")
	}
	fresh := req
	fresh.RequestKey = "unsafe-replacement"
	if _, err = s.Submit(ctx, fresh); err == nil {
		t.Fatal("uncertain writer admitted replacement")
	}
}
