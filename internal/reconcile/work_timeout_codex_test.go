package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type timeoutHelperAuthority struct{ scope helper.Scope }

func (a timeoutHelperAuthority) Resolve(context.Context, helper.Request) (helper.Scope, error) {
	return a.scope, nil
}
func (a timeoutHelperAuthority) Check(context.Context, helper.Job) error { return nil }
func (a timeoutHelperAuthority) Refresh(context.Context, helper.Scope) (helper.Scope, error) {
	return a.scope, nil
}

type timeoutRunningBackend struct {
	started chan struct{}
	fences  atomic.Int32
}

func (b *timeoutRunningBackend) Run(ctx context.Context, _ helper.RunSpec, _ helper.Sink) (helper.RunResult, error) {
	close(b.started)
	<-ctx.Done()
	return helper.RunResult{}, ctx.Err()
}
func (b *timeoutRunningBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	return helper.RunResult{}, false, nil
}
func (b *timeoutRunningBackend) Fence(context.Context, helper.Job) error { b.fences.Add(1); return nil }

func TestCodexWorkTimeoutReplacesOnlyCoordinator(t *testing.T) {
	ctx := context.Background()
	w, rt, spec := factoryAttemptGuardFixture(t)
	ref, err := rt.PrepareAttempt(ctx, spec, "work-timeout")
	if err != nil {
		t.Fatal(err)
	}
	d := w.deps()
	d.Now = time.Now
	d.Runtimes = map[string]session.Runtime{"codex": rt}
	d.Quotas = map[string]helper.UsageReader{"codex": b2CodexUsage{}}
	r, err := readRun(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	r.dry = false
	it, owner, sess := r.ents["e1-w1"], r.ents["e1-s1"], r.ents["e1-s1-orchestrator"]
	if err := r.ensureSetWorkAuthorities(owner, b2Qualification); err != nil {
		t.Fatal(err)
	}
	tree, ok := worktreeReady(it)
	if !ok {
		t.Fatal("missing worktree")
	}
	authority := timeoutHelperAuthority{scope: helper.Scope{EpicID: "e1", OwnerID: owner.id, WorkID: it.id, SetID: owner.id, Lease: 1, Generation: "timeout-test", Worktree: tree.Text, OutputRoot: owner.entry.Dir}}
	var fences atomic.Int32
	service := helper.Service{Brain: w.brain, Authority: authority, FenceRuntime: func(context.Context, helper.Job) error { fences.Add(1); return nil }}
	r.d.Helpers = &service
	request := helper.Request{SchemaVersion: 1, RequestKey: "running", OwnerID: owner.id, WorkID: it.id, SetID: owner.id, Lease: 1, Component: "reviewer-code", Prompt: "review"}
	running, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	backend := &timeoutRunningBackend{started: make(chan struct{})}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- helper.Run(runCtx, helper.RunnerDeps{Service: service, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-backend.started:
	case <-time.After(3 * time.Second):
		t.Fatal("helper did not start")
	}
	request.RequestKey = "queued"
	queued, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	original := map[string]helper.Job{}
	for _, accepted := range []helper.Accepted{running, queued} {
		job, err := service.Job(accepted.JobID)
		if err != nil {
			t.Fatal(err)
		}
		original[job.ID] = job
	}
	// The lookup is simulated; the attempt journal and close/successor operations are real.
	r.inventories = map[string]session.Inventory{"codex": {Complete: true, Sessions: []session.Session{{Ref: session.Ref{Executor: "codex", ID: ref.SessionID, AttemptID: ref.AttemptID, Name: spec.Name, EpicID: "e1", Generation: spec.Generation, PID: 42}, OwnerID: owner.id, State: listedWorking}}}}
	r.now = time.Now().UTC()
	next := it.cur
	next.Since = r.now
	next.Attempt++
	if moved, err := r.move(it, next, blueprint.TriggerTick, timeoutRef+"work-timeout"); err != nil || !moved {
		t.Fatalf("timeout transition: %v %v", moved, err)
	}
	if r.subject(owner) != it {
		t.Fatalf("fixture subject: %+v", r.subject(owner))
	}
	lookup := r.lookupSession("e1", spec.Name)
	if lookup.Presence != session.Present || !lookup.Session.Live() {
		t.Fatalf("fixture session: %+v", lookup)
	}
	r.countOtherRestarts()
	if fences.Load() != 0 {
		t.Fatalf("work timeout fenced helpers %d times", fences.Load())
	}
	for _, accepted := range []helper.Accepted{running, queued} {
		j, err := service.Job(accepted.JobID)
		if err != nil {
			t.Fatal(err)
		}
		want := "queued"
		if accepted.JobID == running.JobID {
			want = "running"
		}
		if j.State != want {
			t.Fatalf("timeout changed helper %s: %s want %s", j.ID, j.State, want)
		}
	}
	if nativeBoundaryState(sess).pending() {
		t.Fatal("timeout left an ownership boundary")
	}
	if len(r.errs) > 0 {
		t.Fatal(r.errs)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := recoverRequestedCoordinators(ctx, w.brain, rt); err != nil {
			t.Fatal(err)
		}
	}
	closed, err := rt.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil || closed.Stage != "closed" {
		t.Fatalf("coordinator did not close: %+v %v", closed, err)
	}
	rt.AttemptCapabilities = managerDenyCapabilities{}
	for range 2 {
		fresh, err := readRun(ctx, r.d)
		if err != nil {
			t.Fatal(err)
		}
		fresh.dry = false
		fresh.now = time.Now().UTC()
		err = fresh.manageCodexCoordinator(fresh.ents[owner.id], fresh.ents[sess.id])
		if err != nil && !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
			t.Fatal(err)
		}
	}
	successor, err := rt.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil || successor.Ref == ref || successor.Predecessor != ref || successor.Stage != "prepared" {
		t.Fatalf("successor: %+v %v", successor, err)
	}
	attempts, err := os.ReadDir(filepath.Join(store.Factory(w.brain), "codex/sessions", ref.SessionID, "attempts"))
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts: %d %v", len(attempts), err)
	}
	for _, accepted := range []helper.Accepted{running, queued} {
		job, err := service.Job(accepted.JobID)
		if err != nil || job != original[accepted.JobID] {
			t.Fatalf("replacement changed helper: %+v want %+v: %v", job, original[accepted.JobID], err)
		}
	}

	if fences.Load() != 0 || backend.fences.Load() != 0 {
		t.Fatal("replacement fenced a helper")
	}
	fresh, err := readRun(ctx, r.d)
	if err != nil {
		t.Fatal(err)
	}
	if nativeBoundaryState(fresh.ents[sess.id]).pending() {
		t.Fatal("replacement left ownership boundary")
	}
}

func TestCodexWorkTimeoutRequiresTimeoutTransition(t *testing.T) {
	ctx := context.Background()
	w, runtime, spec := factoryAttemptGuardFixture(t)
	ref, err := runtime.PrepareAttempt(ctx, spec, "no-timeout")
	if err != nil {
		t.Fatal(err)
	}
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"codex": runtime}
	r, err := readRun(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	r.dry = false
	subject, sess := r.ents["e1-w1"], r.ents["e1-s1-orchestrator"]
	if _, err := r.captureWorkTimeoutCloseLocked(subject, sess); err == nil {
		t.Fatal("ordinary transition authorized a timeout close")
	}
	view, err := runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil || view.Stage != "prepared" || view.CloseCause != nil {
		t.Fatalf("invalid timeout changed attempt: %+v %v", view, err)
	}
	for _, event := range w.log(sess.id) {
		if event.Recipient == "codex-attempt" && event.Request == "close" {
			t.Fatal("invalid timeout recorded a close")
		}
	}
}
