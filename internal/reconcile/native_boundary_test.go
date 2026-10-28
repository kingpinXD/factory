package reconcile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
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

func TestNativeTimeoutRestartFencesBeforeStopAndRenewsAuthority(t *testing.T) {
	w, s, req := nativeWorld(t)
	coord := w.orchestrator(req.SetID, sessionRunning, 9401, listedWorking)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	started, gone, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var fenced atomic.Bool
	backend := boundaryBackend{dBackend: dBackend{check: func(ctx context.Context, spec helper.RunSpec) error {
		close(started)
		defer close(gone)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}, fence: func(ctx context.Context, _ helper.Job) error {
		select {
		case <-gone:
			fenced.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"zed": backend}, Quotas: map[string]helper.UsageReader{"zed": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	go func() { done <- helper.Run(ctx, deps) }()
	defer func() { unblock(); cancel(); <-done }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("helper did not reach admitted turn")
	}
	old, err := s.ManagedAdmission(ctx, accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	s.FenceRuntime = func(ctx context.Context, j helper.Job) error {
		if _, err := s.ManagedAdmission(ctx, j.ID, j.Generation); err == nil {
			return errors.New("fencer ran before admission closed")
		}
		unblock()
		select {
		case <-gone:
			fenced.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	respond := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "stop" {
			if !fenced.Load() {
				t.Error("coordinator stopped before native descendant death was confirmed")
			}
			if _, err := s.ManagedAdmission(context.Background(), old.ID, old.Generation); err == nil {
				t.Error("old native generation still admitted at coordinator stop")
			}
		}
		return respond(c)
	}
	r := boundaryRun(t, w, s)
	r.restartWorker(r.ents[req.WorkID], "restart:timeout:fixture")
	if len(r.errs) > 0 {
		t.Fatal(r.errs)
	}
	if _, err = s.ManagedAdmission(ctx, old.ID, old.Generation); err == nil {
		t.Error("stopped coordinator retained admitted native generation")
	}
	// Discard the tick's in-memory view, as on a coordinator crash.
	restarted := boundaryRun(t, w, s)
	restarted.recoverHelperOwnership()
	if _, err = s.RecoverScope(ctx, req.WorkID); err == nil {
		t.Error("stopped boundary reopened before replacement authority")
	}
	if moved, err := restarted.try(restarted.ents[sessionID(req.SetID, componentOrchestrator)], sessionStopped, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("stop not recorded: %v %v", moved, err)
	}
	sess := restarted.ents[sessionID(req.SetID, componentOrchestrator)]
	if moved, err := restarted.try(sess, sessionStarting, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("coordinator not resumed: %v %v", moved, err)
	}
	if calls := w.called("claude --bg --resume " + coord.SessionID); len(calls) != 1 {
		t.Fatalf("resume calls: %v", calls)
	}
	lease, err := Lease(w.dir(req.SetID))
	if err != nil {
		t.Fatal(err)
	}
	if lease <= req.Lease {
		t.Errorf("timeout resume reused old lease %d", lease)
	}
	freshScope, err := s.Authority.Refresh(ctx, old.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if freshScope.Generation == old.Scope.Generation {
		t.Error("timeout resume reused old scope generation")
	}
	stale := req
	stale.RequestKey = "old-coordinator"
	if _, err = s.Submit(ctx, stale); err == nil {
		t.Error("old lease accepted after timeout stop and resume")
	}
	fresh := req
	fresh.RequestKey = "resumed-coordinator"
	fresh.Lease = lease
	if _, err = s.Submit(ctx, fresh); err != nil {
		t.Fatalf("fresh persisted lease refused: %v", err)
	}
}

func TestNativeCompactTransitionFencesAdmissionAfterCheckpointCheck(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.orchestrator(req.SetID, sessionTurnEnded, 9402, listedTurnEnded)
	file := filepath.Join(w.dir(req.SetID), "checkpoint.md")
	put(t, file, "## Where I am\n## Next\n## Notes\n")
	w.append(w.dir(req.SetID), events.Event{Kind: events.KindCheckpoint, Sender: req.SetID, File: file, At: w.now.Add(time.Second)})
	r := boundaryRun(t, w, s)
	sess := r.ents[sessionID(req.SetID, componentOrchestrator)]
	ready, ref, err := r.checkpointReady(sess)
	if err != nil || !ready {
		t.Fatalf("checkpoint not initially ready: %v %v", ready, err)
	}
	// Admission races the observational nativeBusy check, before the transition.
	admitted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	respond := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "stop" {
			stale := req
			stale.RequestKey = "during-compact-stop"
			if _, err := s.Submit(context.Background(), stale); err == nil {
				t.Error("native helper admitted during actual compact stop")
			}
		}
		return respond(c)
	}
	next := fsm.Cur{State: sessionCompacting, Since: r.now, Attempt: sess.cur.Attempt, Prev: sess.cur.Prev}
	if moved, err := r.move(sess, next, blueprint.TriggerTick, ref); err != nil || !moved {
		t.Fatalf("compact transition: %v %v", moved, err)
	}
	job, err := s.Job(admitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != "cancelled" {
		t.Errorf("racing helper was not fenced: %s", job.State)
	}
	// A coordinator restart must not reopen a compacting scope.
	restarted := boundaryRun(t, w, s)
	restarted.recoverHelperOwnership()
	stale := req
	stale.RequestKey = "after-compact-transition"
	if _, err = s.Submit(context.Background(), stale); err == nil {
		t.Error("old lease Submit succeeded while coordinator compacting")
	}
	oldScope := job.Scope
	if _, err = s.Authority.Refresh(context.Background(), oldScope); err == nil {
		t.Error("compacting authority was refreshable for reopening")
	}
	current := w.listedSession("factory:" + req.SetID + ":orchestrator")
	posted := w.listen(current.PID)
	resumed := boundaryRun(t, w, s)
	sess = resumed.ents[sessionID(req.SetID, componentOrchestrator)]
	next = fsm.Cur{State: sessionRunning, Since: resumed.now.Add(time.Minute), Attempt: sess.cur.Attempt, Prev: sess.cur.Prev}
	if moved, err := resumed.move(sess, next, blueprint.TriggerTick, "compacted:fixture"); err != nil || !moved {
		t.Fatalf("compaction completion: %v %v", moved, err)
	}
	messages := posted()
	if len(messages) != 1 {
		t.Error("compacted coordinator did not get its checkpoint prompt")
	}
	lease, err := Lease(w.dir(req.SetID))
	if err != nil {
		t.Fatal(err)
	}
	if lease <= req.Lease {
		t.Errorf("compaction reused lease %d", lease)
	}
	if len(messages) == 1 && !strings.Contains(messages[0], fmt.Sprintf("lease: %d", lease)) {
		t.Errorf("checkpoint prompt did not carry fresh authority: %s", messages[0])
	}
	if _, err = s.Submit(context.Background(), stale); err == nil {
		t.Error("old lease admitted after compaction completion")
	}
	fresh := req
	fresh.RequestKey = "after-compact"
	fresh.Lease = lease
	if _, err = s.Submit(context.Background(), fresh); err != nil {
		t.Fatalf("new compaction authority refused: %v", err)
	}
}

func TestNativeAccountStopDoesNotRevokeIndependentHelpers(t *testing.T) {
	w, s, req := nativeWorld(t)
	coord := w.orchestrator(req.SetID, sessionRunning, 9403, listedWorking)
	r := boundaryRun(t, w, s)
	sess := r.ents[sessionID(req.SetID, componentOrchestrator)]
	scope, err := s.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.stopSession(sess, coord, "account:pause", "pause Claude account"); err != nil {
		t.Fatal(err)
	}
	w.moveTo(sess.id, sessionStopped)
	if _, err = s.Submit(context.Background(), req); err != nil {
		t.Fatalf("Claude hold leaked into native admission: %v", err)
	}
	after, err := s.Authority.Refresh(context.Background(), scope)
	if err != nil || after != scope {
		t.Fatalf("account stop changed native authority: %+v %v", after, err)
	}
}

func stoppedBoundary(t *testing.T) (*world, helper.Service, helper.Request, *run, *entity) {
	t.Helper()
	w, s, req := nativeWorld(t)
	w.orchestrator(req.SetID, sessionRunning, 9410, listedWorking)
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	r := boundaryRun(t, w, s)
	r.restartWorker(r.ents[req.WorkID], "restart:timeout:crash-fixture")
	if len(r.errs) > 0 {
		t.Fatal(r.errs)
	}
	r = boundaryRun(t, w, s)
	sess := r.ents[sessionID(req.SetID, componentOrchestrator)]
	if moved, err := r.try(sess, sessionStopped, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("stop boundary: %v %v", moved, err)
	}
	return w, s, req, r, sess
}
func TestNativeRestartRecoversCrashAfterLeaseGrantWithoutGrantingTwice(t *testing.T) {
	w, s, req, r, sess := stoppedBoundary(t)
	b := nativeBoundaryState(sess)
	if !b.pending() {
		t.Fatal("restart intent not retained through stop")
	}
	// Simulate the crash point after the real grant write but before ready.
	owner := r.ents[req.SetID]
	owner.st.Lease++
	marker := nativeBoundaryPrefix + sess.id + ":" + seqRef(b.intent)
	if _, err := r.grantLease(owner, marker); err != nil {
		t.Fatal(err)
	}
	if err := syncOwnership(owner); err != nil {
		t.Fatal(err)
	}
	fresh := req
	fresh.Lease = 2
	fresh.RequestKey = "fresh-after-grant"
	if _, err := s.Submit(context.Background(), fresh); err == nil {
		t.Fatal("unsatisfied boundary reopened merely because lease changed")
	}
	recovered := boundaryRun(t, w, s)
	recovered.recoverHelperOwnership()
	sess = recovered.ents[sess.id]
	if moved, err := recovered.try(sess, sessionStarting, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("recovery wake: %v %v", moved, err)
	}
	lease, err := Lease(w.dir(req.SetID))
	if err != nil || lease != 2 {
		t.Fatalf("grant was repeated or lost: %d %v", lease, err)
	}
	if _, err = s.Submit(context.Background(), req); err == nil {
		t.Fatal("old lease admitted after crash recovery")
	}
	if _, err = s.Submit(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if calls := w.called("claude --bg --resume"); len(calls) != 1 {
		t.Fatalf("wake replayed: %v", calls)
	}
	recovered.recoverHelperOwnership()
	if after, err := Lease(w.dir(req.SetID)); err != nil || after != lease {
		t.Fatalf("completed boundary regenerated authority: %d %v", after, err)
	}
}

type failBoundaryRefresh struct{ helper.Authority }

func (f failBoundaryRefresh) Refresh(context.Context, helper.Scope) (helper.Scope, error) {
	return helper.Scope{}, errors.New("crash before scope reopen")
}
func TestNativeRestartRecoversReadyRecordBeforeReopenAndResume(t *testing.T) {
	w, s, req, r, sess := stoppedBoundary(t)
	broken := s
	broken.Authority = failBoundaryRefresh{s.Authority}
	r.d.Helpers = &broken
	// Starting is logged; the new lease and ready marker persist, then reopening
	// fails before the coordinator has been resumed.
	if moved, err := r.try(sess, sessionStarting, blueprint.TriggerTick); err == nil || !moved || !strings.Contains(err.Error(), "crash before scope reopen") {
		t.Fatalf("missed crash point: %v %v", moved, err)
	}
	b := nativeBoundaryState(sess)
	if b.ready.Seq == 0 || b.delivered.Seq != 0 {
		t.Fatalf("incorrect durable boundary phase: %+v", b)
	}
	if calls := w.called("claude --bg --resume"); len(calls) != 0 {
		t.Fatalf("coordinator resumed through failed reopen: %v", calls)
	}
	recovered := boundaryRun(t, w, s)
	recovered.recoverHelperOwnership()
	if len(recovered.errs) != 0 {
		t.Fatal(recovered.errs)
	}
	after := nativeBoundaryState(recovered.ents[sess.id])
	if after.ready.Seq != b.ready.Seq || after.delivered.Seq == 0 {
		t.Fatalf("recovery changed fresh authority or lost wake: %+v", after)
	}
	if lease, err := Lease(w.dir(req.SetID)); err != nil || lease != 2 {
		t.Fatalf("recovery did not preserve recorded lease: %d %v", lease, err)
	}
	if calls := w.called("claude --bg --resume"); len(calls) != 1 {
		t.Fatalf("coordinator wake count: %v", calls)
	}
	recovered.recoverHelperOwnership()
	if calls := w.called("claude --bg --resume"); len(calls) != 1 {
		t.Fatalf("acknowledged wake replayed: %v", calls)
	}
	fresh := req
	fresh.Lease = 2
	fresh.RequestKey = "after-recovered-resume"
	if _, err := s.Submit(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCompactionRecoversCrashAfterRunningTransitionBeforeEntryAction(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.orchestrator(req.SetID, sessionTurnEnded, 9411, listedTurnEnded)
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	r := boundaryRun(t, w, s)
	sess := r.ents[sessionID(req.SetID, componentOrchestrator)]
	if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:fixture"); err != nil || !moved {
		t.Fatalf("begin compact: %v %v", moved, err)
	}
	// The transition was persisted, but its afterCompaction action never ran.
	w.append(w.dir(sess.id), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: sessionCompacting, To: sessionRunning, At: w.now.Add(time.Minute), Trigger: blueprint.TriggerTick, TriggerRef: "compacted:crash-fixture"})
	current := w.listedSession(sess.session.Name)
	posted := w.listen(current.PID)
	recovered := boundaryRun(t, w, s)
	recovered.recoverHelperOwnership()
	if len(recovered.errs) > 0 {
		t.Fatal(recovered.errs)
	}
	b := nativeBoundaryState(recovered.ents[sess.id])
	if b.pending() || b.delivered.Seq == 0 {
		t.Fatalf("compaction boundary was not recovered: %+v", b)
	}
	if lease, err := Lease(w.dir(req.SetID)); err != nil || lease != 2 {
		t.Fatalf("missing recorded compaction lease: %d %v", lease, err)
	}
	recovered.recoverHelperOwnership()
	if messages := posted(); len(messages) != 1 {
		t.Fatalf("compaction recovery delivered %d messages", len(messages))
	}
	fresh := req
	fresh.Lease = 2
	fresh.RequestKey = "compact-recovered"
	if _, err := s.Submit(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(context.Background(), req); err == nil {
		t.Fatal("pre-compaction lease survived recovery")
	}
}

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
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"zed": backend}, Quotas: map[string]helper.UsageReader{"zed": dUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
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

func TestNativeWorkOwnedCompactionUsesRecordedGenerationWithoutSetLease(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	w.moveTo(req.WorkID, workInReview)
	req.OwnerID = req.WorkID
	req.SetID = ""
	req.Lease = 0
	id := sessionID(req.WorkID, babysitterComponent)
	name := sessionName(req.WorkID, babysitterComponent)
	w.put(id, blueprint.MachineSession, "e1", filepath.Join(w.dir(req.WorkID), "sessions", "babysitter"), sessionTurnEnded, &SessionInputs{Name: name, Component: babysitterComponent, Task: req.WorkID})
	w.listSession(name, 9413, listedTurnEnded)
	admitted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.Job(admitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	r := boundaryRun(t, w, s)
	sess := r.ents[id]
	if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:work-owned"); err != nil || !moved {
		t.Fatalf("work-owned compact: %v %v", moved, err)
	}
	current := w.listedSession(name)
	posted := w.listen(current.PID)
	r = boundaryRun(t, w, s)
	sess = r.ents[id]
	if moved, err := r.move(sess, fsm.Cur{State: sessionRunning, Since: r.now.Add(time.Minute)}, blueprint.TriggerTick, "compacted:work-owned"); err != nil || !moved {
		t.Fatalf("work-owned finish: %v %v", moved, err)
	}
	after, err := s.Authority.Refresh(context.Background(), old.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation == old.Scope.Generation || after.SetID != "" || after.Lease != 0 {
		t.Fatalf("bad work-owned boundary authority: %+v", after)
	}
	if len(posted()) != 1 {
		t.Fatal("work-owned checkpoint prompt missing")
	}
	fresh := req
	fresh.RequestKey = "work-owned-new-generation"
	next, err := s.Submit(context.Background(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(next.JobID)
	if err != nil || job.Scope.Generation != after.Generation {
		t.Fatalf("new job did not pin recorded boundary: %+v %v", job, err)
	}
}

func TestNativeBoundaryRecoveryDoesNotResumeThroughUnknownClaudeListing(t *testing.T) {
	w, s, req, _, sess := stoppedBoundary(t)
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "agents" {
			return nil, errors.New("listing unavailable")
		}
		return nil, fmt.Errorf("unexpected side effect: %v", c.Args)
	}
	r := boundaryRun(t, w, s)
	r.recoverHelperOwnership()
	if !nativeBoundaryState(r.ents[sess.id]).pending() {
		t.Fatal("unknown listing completed pending authority")
	}
	lease, err := Lease(w.dir(req.SetID))
	if err != nil || lease != 1 {
		t.Fatalf("unknown listing minted lease: %d %v", lease, err)
	}
	if calls := w.called("claude --bg --resume"); len(calls) != 0 {
		t.Fatalf("resumed without coordinator observation: %v", calls)
	}
}

func TestNativeRestartRecoversLostCoordinatorStopAcknowledgment(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.orchestrator(req.SetID, sessionRunning, 9414, listedWorking)
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	respond := w.fake.Respond
	lost := false
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		out, err := respond(c)
		if !lost && c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "stop" {
			lost = true
			return nil, errors.New("lost stop acknowledgment")
		}
		return out, err
	}
	r := boundaryRun(t, w, s)
	r.restartWorker(r.ents[req.WorkID], "restart:timeout:lost-stop")
	if len(r.errs) != 1 || !strings.Contains(r.errs[0], "lost stop acknowledgment") {
		t.Fatalf("stop failure lost: %v", r.errs)
	}
	sessID := sessionID(req.SetID, componentOrchestrator)
	if !nativeBoundaryState(r.ents[sessID]).pending() {
		t.Fatal("stop uncertainty lost durable intent")
	}
	recovered := boundaryRun(t, w, s)
	recovered.recoverHelperOwnership()
	sess := recovered.ents[sessID]
	if moved, err := recovered.try(sess, sessionStopped, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("stopped coordinator not recovered: %v %v", moved, err)
	}
	if moved, err := recovered.try(sess, sessionStarting, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("recovered coordinator not resumed: %v %v", moved, err)
	}
	if lease, err := Lease(w.dir(req.SetID)); err != nil || lease != 2 {
		t.Fatalf("wrong recovered lease: %d %v", lease, err)
	}
	if _, err := s.Submit(context.Background(), req); err == nil {
		t.Fatal("old lease survived lost-stop recovery")
	}
}
func TestNativeEmptyCheckpointBoundaryFencesBeforeFirstHelper(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.orchestrator(req.SetID, sessionTurnEnded, 9415, listedTurnEnded)
	r := boundaryRun(t, w, s)
	id := sessionID(req.SetID, componentOrchestrator)
	sess := r.ents[id]
	if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:empty-scope"); err != nil || !moved {
		t.Fatalf("empty scope compact: %v %v", moved, err)
	}
	if _, err := s.Submit(context.Background(), req); err == nil {
		t.Fatal("first old-lease helper admitted during compaction")
	}
	current := w.listedSession(sess.session.Name)
	posted := w.listen(current.PID)
	r = boundaryRun(t, w, s)
	sess = r.ents[id]
	if moved, err := r.move(sess, fsm.Cur{State: sessionRunning, Since: r.now.Add(time.Minute)}, blueprint.TriggerTick, "compacted:empty-scope"); err != nil || !moved {
		t.Fatalf("empty scope finish: %v %v", moved, err)
	}
	if len(posted()) != 1 {
		t.Fatal("new coordinator authority not delivered")
	}
	fresh := req
	fresh.Lease = 2
	fresh.RequestKey = "first-helper-new-lease"
	if _, err := s.Submit(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
}
func TestNativeDeadCoordinatorWakeFencesAndRenewsButAccountResumeDoesNot(t *testing.T) {
	for _, tc := range []struct {
		name, state, listed string
		renew               bool
	}{{"dead", sessionDead, listedWorking, true}, {"account", sessionStopped, "stopped", false}} {
		t.Run(tc.name, func(t *testing.T) {
			w, s, req := nativeWorld(t)
			w.orchestrator(req.SetID, tc.state, 0, tc.listed)
			admitted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			r := boundaryRun(t, w, s)
			sess := r.ents[sessionID(req.SetID, componentOrchestrator)]
			if moved, err := r.try(sess, sessionStarting, blueprint.TriggerTick); err != nil || !moved {
				t.Fatalf("wake: %v %v", moved, err)
			}
			lease, err := Lease(w.dir(req.SetID))
			if err != nil {
				t.Fatal(err)
			}
			job, err := s.Job(admitted.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.renew && (lease != 2 || job.State != "cancelled") {
				t.Fatalf("dead replacement reused authority: lease=%d state=%s", lease, job.State)
			}
			if !tc.renew && (lease != 1 || job.State != "queued") {
				t.Fatalf("account resume revoked native work: lease=%d state=%s", lease, job.State)
			}
		})
	}
}

func TestNativePlannerCheckpointDoesNotFenceSetOwnedHelpers(t *testing.T) {
	w, s, req := nativeWorld(t)
	w.planner("e1", sessionTurnEnded, 9416, listedTurnEnded)
	admitted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.Job(admitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	r := boundaryRun(t, w, s)
	sess := r.ents[sessionID("e1", componentPlanner)]
	if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:planner"); err != nil || !moved {
		t.Fatalf("planner compact: %v %v", moved, err)
	}
	current, err := s.Job(admitted.JobID)
	if err != nil || current.State != "queued" {
		t.Fatalf("planner checkpoint revoked another coordinator's helper: %+v %v", current, err)
	}
	after, err := s.Authority.Refresh(context.Background(), old.Scope)
	if err != nil || after != old.Scope {
		t.Fatalf("planner checkpoint changed set authority: %+v %v", after, err)
	}
	fresh := req
	fresh.RequestKey = "independent-during-planner-checkpoint"
	if _, err = s.Submit(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCompactionCrashDefersDeliveryThroughHoldUnknownListingAndDelayedEligibility(t *testing.T) {
	for _, tc := range []struct{ name, tick, disk string }{
		{"near_limit", stateNearLimit, stateNearLimit}, {"paused", statePaused, statePaused}, {"stale", stateStale, stateStale}, {"unknown_tick", "", stateOK}, {"account_changed_after_tick", stateOK, stateNearLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, s, req := nativeWorld(t)
			w.orchestrator(req.SetID, sessionTurnEnded, 9420, listedTurnEnded)
			admitted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			r := boundaryRun(t, w, s)
			id := sessionID(req.SetID, componentOrchestrator)
			sess := r.ents[id]
			if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:hold-crash"); err != nil || !moved {
				t.Fatalf("compact: %v %v", moved, err)
			}
			// Crash after the actual compact operation and persisted running transition,
			// before afterCompaction can deliver the carry-on prompt.
			w.append(w.dir(id), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: sessionCompacting, To: sessionRunning, At: w.now.Add(time.Minute), Trigger: blueprint.TriggerTick, TriggerRef: "compacted:hold-crash"})
			current := w.listedSession(sess.session.Name)
			posted := w.listen(current.PID)
			setPolicy := func(state string) {
				t.Helper()
				if err := os.MkdirAll(accountDir(w.brain), 0700); err != nil {
					t.Fatal(err)
				}
				if err := store.WriteJSON(store.StatusPath(accountDir(w.brain)), AccountStatus{State: state}); err != nil {
					t.Fatal(err)
				}
			}
			setPolicy(tc.disk)
			held := boundaryRun(t, w, s)
			held.useAccount(tc.tick, nil)
			held.recoverHelperOwnership()
			if messages := posted(); len(messages) != 0 {
				t.Errorf("carry-on posted while Claude held: %v", messages)
			}
			if b := nativeBoundaryState(held.ents[id]); b.delivered.Seq != 0 {
				t.Error("held recovery acknowledged undelivered carry-on")
			}
			if calls := w.called("claude --bg --resume"); len(calls) != 1 {
				t.Errorf("held recovery resumed Claude: %v", calls)
			}

			// Broker observation/result persistence is independent of the Claude hold.
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			notices := 0
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = helper.Run(ctx, helper.RunnerDeps{Service: s, FactoryBin: binary, Notify: func(context.Context, helper.Job, helper.RoundResult) error { notices++; return nil }})
			cancel()
			if err != nil || notices != 1 {
				t.Fatalf("native observation blocked by Claude hold: notices=%d err=%v", notices, err)
			}
			result, err := s.Result(admitted.JobID, 1)
			if err != nil || result.State != "cancelled" {
				t.Fatalf("native result lost while held: %+v %v", result, err)
			}
			if len(events.Inbox(w.log(req.SetID))) != 1 {
				t.Fatal("native owner notice was not durably persisted during hold")
			}

			setPolicy(stateOK)
			respond := w.fake.Respond
			w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
				if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "agents" {
					return nil, errors.New("listing unavailable")
				}
				return respond(c)
			}
			unknown := boundaryRun(t, w, s)
			unknown.recoverHelperOwnership()
			if messages := posted(); len(messages) != 0 {
				t.Errorf("carry-on posted with unknown listing: %v", messages)
			}
			if b := nativeBoundaryState(unknown.ents[id]); b.delivered.Seq != 0 {
				t.Error("unknown listing acknowledged delivery")
			}
			if calls := w.called("claude --bg --resume"); len(calls) != 1 {
				t.Errorf("unknown listing resumed Claude: %v", calls)
			}

			// Eligibility can disappear again before the next retry; preserve the pending
			// delivery and recorded generation rather than acknowledging or regranting.
			w.fake.Respond = respond
			setPolicy(stateNearLimit)
			delayed := boundaryRun(t, w, s)
			delayed.useAccount(stateNearLimit, nil)
			delayed.recoverHelperOwnership()
			if messages := posted(); len(messages) != 0 {
				t.Errorf("delayed hold posted carry-on: %v", messages)
			}
			if b := nativeBoundaryState(delayed.ents[id]); b.delivered.Seq != 0 {
				t.Error("delayed hold acknowledged carry-on")
			}
			setPolicy(stateOK)
			eligible := boundaryRun(t, w, s)
			eligible.recoverHelperOwnership()
			if len(eligible.errs) > 0 {
				t.Fatal(eligible.errs)
			}
			b := nativeBoundaryState(eligible.ents[id])
			if b.pending() || b.delivered.Seq == 0 {
				t.Fatalf("eligible retry did not finish delivery: %+v", b)
			}
			if messages := posted(); len(messages) != 1 || !strings.Contains(messages[0], "lease: 2") {
				t.Fatalf("eligible carry-on count/content: %v", messages)
			}
			if lease, err := Lease(w.dir(req.SetID)); err != nil || lease != 2 {
				t.Fatalf("delayed recovery repeated lease grant: %d %v", lease, err)
			}
			again := boundaryRun(t, w, s)
			again.recoverHelperOwnership()
			if messages := posted(); len(messages) != 1 {
				t.Fatalf("acknowledged carry-on replayed: %v", messages)
			}
		})
	}
}

func TestNativeCompactionLiveDeliveryRechecksAccountPolicyAfterTransitionDecision(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(fmt.Sprintf("repair_%v", repair), func(t *testing.T) {
			w, s, req := nativeWorld(t)
			if repair {
				var in EpicInputs
				if err := store.ReadInputs(w.dir("e1"), &in); err != nil {
					t.Fatal(err)
				}
				in.Kind = epicKindRepair
				if err := store.WriteInputs(w.dir("e1"), in); err != nil {
					t.Fatal(err)
				}
			}
			w.orchestrator(req.SetID, sessionTurnEnded, 9421, listedTurnEnded)
			r := boundaryRun(t, w, s)
			id := sessionID(req.SetID, componentOrchestrator)
			sess := r.ents[id]
			if moved, err := r.move(sess, fsm.Cur{State: sessionCompacting, Since: r.now}, blueprint.TriggerTick, "checkpoint:live-policy"); err != nil || !moved {
				t.Fatalf("compact: %v %v", moved, err)
			}
			current := w.listedSession(sess.session.Name)
			posted := w.listen(current.PID)
			r = boundaryRun(t, w, s)
			sess = r.ents[id]
			// The tick was eligible when it decided the transition; the account step
			// published a newer hold before the live-session delivery side effect.
			if err := os.MkdirAll(accountDir(w.brain), 0700); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteJSON(store.StatusPath(accountDir(w.brain)), AccountStatus{State: stateNearLimit}); err != nil {
				t.Fatal(err)
			}
			if moved, err := r.move(sess, fsm.Cur{State: sessionRunning, Since: r.now.Add(time.Minute)}, blueprint.TriggerTick, "compacted:live-policy"); err != nil || !moved {
				t.Fatalf("finish transition: %v %v", moved, err)
			}
			b := nativeBoundaryState(sess)
			if !repair {
				if messages := posted(); len(messages) != 0 {
					t.Errorf("live session bypassed fresh account hold: %v", messages)
				}
				if b.delivered.Seq != 0 {
					t.Error("held live delivery acknowledged")
				}
			} else {
				if messages := posted(); len(messages) != 1 || b.delivered.Seq == 0 {
					t.Fatalf("existing near-limit repair exception was lost: %v", messages)
				}
			}
		})
	}
}
