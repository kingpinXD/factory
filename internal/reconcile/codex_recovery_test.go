package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type splitRecoveryRuntime struct {
	*sampleCodexRuntime
	brain string
	calls []string
}

func (r *splitRecoveryRuntime) Recover(ctx context.Context) error {
	return fmt.Errorf("mixed recovery must not run under tick")
}
func (r *splitRecoveryRuntime) RecoverAttempts(ctx context.Context) error {
	release, err := store.Lock(r.brain, "tick")
	if err != nil {
		return fmt.Errorf("modern recovery ran under tick: %w", err)
	}
	release()
	r.calls = append(r.calls, "modern")
	return nil
}
func (r *splitRecoveryRuntime) RecoverLegacy(ctx context.Context) error {
	release, err := store.Lock(r.brain, "tick")
	if err == nil {
		release()
		return fmt.Errorf("legacy recovery moved outside tick")
	}
	r.calls = append(r.calls, "legacy")
	return nil
}
func TestFactoryModernRecoveryRunsBeforeActualTickLock(t *testing.T) {
	w, d, base := codexSampleWorld(t)
	runtime := &splitRecoveryRuntime{sampleCodexRuntime: base, brain: w.brain}
	d.Runtimes["codex"] = runtime
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(runtime.calls) != "[modern legacy]" {
		t.Fatalf("actual Tick recovery order=%v; log=%s", runtime.calls, w.out.String())
	}
}
func TestFactoryDryTickDoesNotRecoverEitherRuntimeLane(t *testing.T) {
	w, d, base := codexSampleWorld(t)
	runtime := &splitRecoveryRuntime{sampleCodexRuntime: base, brain: w.brain}
	d.Runtimes["codex"] = runtime
	if err := Tick(context.Background(), d, true); err != nil {
		t.Fatal(err)
	}
	if len(runtime.calls) != 0 {
		t.Fatal(runtime.calls)
	}
}

func recoveryRealCoordinator(t *testing.T) (*world, *run, *codex.Runtime, codex.AttemptRef) {
	t.Helper()
	w, _, _ := b2World(t)
	runtime, ref := b2ReadyCoordinator(t, w)
	initializeReadFixtureSessionRegistration(t, w)
	manager, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	manager.attachMachines()
	b2Activate(t, manager, "e1-s1")
	runtime.AttemptAuthority = &factoryAttemptAuthority{brain: w.brain, runtime: runtime, verify: b2Qualification}
	manager.d.Runtimes = map[string]session.Runtime{"codex": runtime}
	manager.now = time.Now().UTC()
	manager.d.Now = time.Now
	manager.d.Quotas = map[string]helper.UsageReader{"codex": b2CodexUsage{}}
	return w, manager, runtime, ref
}

func TestFactoryAutomaticRealFailedAttemptClosesOutsideTick(t *testing.T) {
	w, _, runtime, ref := recoveryRealCoordinator(t)
	var v codex.Record
	if err := store.ReadJSON(filepath.Join(w.brain, "factory/codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"), &v); err != nil {
		t.Fatal(err)
	}
	// Kill only the genuinely owned host group, retaining its real journal.
	if err := syscall.Kill(-v.Host.Group, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		alive, err := v.Host.Alive()
		if err != nil && !errors.Is(err, codex.ErrObservationUnknown) {
			t.Fatal(err)
		}
		if err == nil && !alive {
			break
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := runtime.RecoverAttempts(ctx); err != nil {
		t.Fatal("actual pre-tick failure could not enter closure", err)
	}
	view, err := runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil || view.Ref != ref || view.Stage != "closed" && view.Stage != "sealing" {
		t.Fatal(view, err)
	}
	if view.Stage == "sealing" {
		if view.NextCheckAt.IsZero() {
			t.Fatal("physical hold not scheduled")
		}
		before, err := os.ReadFile(filepath.Join(w.brain, "factory/codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.RecoverAttempts(ctx); err != nil {
			t.Fatal(err)
		}
		after, _ := os.ReadFile(filepath.Join(w.brain, "factory/codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"))
		if string(before) != string(after) {
			t.Fatal("before-due retry changed persisted hold")
		}
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal("physical recovery retained tick", err)
	}
	release()
}

func pauseRequestFixture(t *testing.T) (*world, *run, *codex.Runtime, codex.AttemptRef, CoordinatorCloseRequestRef) {
	t.Helper()
	w, r, runtime, ref := recoveryRealCoordinator(t)
	r.d.Quotas = map[string]helper.UsageReader{"codex": coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		q, err := (b2CodexUsage{}).Read(context.Background())
		q.Windows[0].Used = 100
		return q, err
	})}
	r.pauseCodexSessions()
	sess := r.ents["e1-s1-orchestrator"]
	var request CoordinatorCloseRequestRef
	for _, ev := range sess.evs {
		if ev.Recipient == "codex-attempt" && ev.Request == "close" {
			request.Event = nativeEventRef(sess.id, ev)
		}
	}
	if request.Event.Sequence == 0 {
		t.Fatalf("actual pause request missing: %s errors=%v", w.out.String(), r.errs)
	}
	return w, r, runtime, ref, request
}

func TestFactoryQuotaRequestClosesActualLivingHostOnNextPass(t *testing.T) {
	w, r, runtime, ref, _ := pauseRequestFixture(t)
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "claimed" {
		t.Fatal("pause producer performed physical stop", view, err)
	}
	before := nativeBoundaryState(r.ents["e1-s1-orchestrator"])
	if before.intent.Seq != 0 {
		t.Fatal("pause invented ownership transfer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	err = recoverRequestedCoordinators(ctx, w.brain, runtime)
	view, readErr := runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if readErr != nil || view.Ref != ref || view.SealReason != "quota_pause" || view.Stage != "closed" && view.Stage != "sealing" {
		t.Fatal("request failed before exact actual seal", view, err, readErr)
	}
	if err != nil && !errors.Is(err, codex.ErrObservationUnknown) {
		t.Fatal(err)
	}
	if view.Stage == "sealing" && view.NextCheckAt.IsZero() {
		t.Fatal("real physical hold lost due time")
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal("request retained tick during physical work", err)
	}
	release()
	// The actual old owner activation, work lease and native boundary stay unchanged.
	fresh, err := newRun(context.Background(), r.d, false)
	if err != nil {
		t.Fatal(err)
	}
	if nativeBoundaryState(fresh.ents["e1-s1-orchestrator"]).intent.Seq != 0 {
		t.Fatal("process pause created native transfer")
	}
}

func preparedPauseRequestFixture(t *testing.T) (*world, *run, *codex.Runtime, codex.AttemptRef, CoordinatorCloseRequestRef, CoordinatorCloseRequest) {
	t.Helper()
	w, runtime, spec := factoryAttemptGuardFixture(t)
	ref, err := runtime.PrepareAttempt(context.Background(), spec, "prepared-pause")
	if err != nil {
		t.Fatal(err)
	}
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"codex": runtime}
	d.Now = time.Now
	d.Quotas = map[string]helper.UsageReader{"codex": coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		q, e := (b2CodexUsage{}).Read(context.Background())
		q.Windows[0].Used = 100
		return q, e
	})}
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	r.pauseCodexSessions()
	for _, ev := range r.ents["e1-s1-orchestrator"].evs {
		if ev.Recipient != "codex-attempt" || ev.Request != "close" {
			continue
		}
		var request CoordinatorCloseRequest
		if err := json.Unmarshal([]byte(ev.Text), &request); err != nil {
			t.Fatal(err)
		}
		return w, r, runtime, ref, CoordinatorCloseRequestRef{Event: nativeEventRef("e1-s1-orchestrator", ev)}, request
	}
	t.Fatalf("prepared pause request absent: %v", r.errs)
	return nil, nil, nil, codex.AttemptRef{}, CoordinatorCloseRequestRef{}, CoordinatorCloseRequest{}
}

func makeActualQuotaAllowed(t *testing.T, r *run, request CoordinatorCloseRequest) {
	t.Helper()
	selected, err := executor.ReadSelection(r.d.Brain, request.Authority.EpicID)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(request.Cause)
	if err != nil {
		t.Fatal(err)
	}
	var cause coordinatorQuotaCause
	if err := json.Unmarshal([]byte(ev.Text), &cause); err != nil {
		t.Fatal(err)
	}
	at := cause.Admission.NextCheckAt
	reader := coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		q, e := (b2CodexUsage{}).Read(context.Background())
		q.ObservedAt = at
		for i := range q.Windows {
			q.Windows[i].ResetsAt = at.Add(q.Windows[i].Duration)
		}
		return q, e
	})
	actual := helper.ReadCodexAdmissionQuota(context.Background(), r.d.Brain, selected, cause.Policy, reader, func() time.Time { return at })
	if actual.Decision.State != "allowed" {
		t.Fatal("ordinary due fake-account refresh did not supersede pause", actual)
	}
}

func TestFactoryQuotaRequestRequiresCurrentBeforeDueSharedFact(t *testing.T) {
	w, r, runtime, ref, _, request := preparedPauseRequestFixture(t)
	authority := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	ev, err := (ProgramEffects{Brain: w.brain}).boundEffectEvent(request.Cause)
	if err != nil {
		t.Fatal(err)
	}
	var cause coordinatorQuotaCause
	if err := json.Unmarshal([]byte(ev.Text), &cause); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{cause.Admission.NextCheckAt, cause.Admission.NextCheckAt.Add(time.Nanosecond)} {
		if err := validateCoordinatorCloseLocked(context.Background(), w.brain, runtime, authority.verify, "e1-s1-orchestrator", request, at); !errors.Is(err, helper.ErrAuthority) {
			t.Fatal("due/past quota authorized NEW stop", at, err)
		}
	}
	makeActualQuotaAllowed(t, r, request)
	if err := recoverRequestedCoordinators(context.Background(), w.brain, runtime); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("removed shared denial remained newstop permission", err)
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "prepared" || view.SealReason != "" {
		t.Fatal("superseded pause wrote seal", view, err)
	}
}

type afterSealControlRecovery struct {
	codex.SealedControlRecovery
	after func()
}

func (r afterSealControlRecovery) RecoverSealedControls(ctx context.Context, key effects.AttemptKey) error {
	r.after()
	return r.SealedControlRecovery.RecoverSealedControls(ctx, key)
}

func TestFactoryAdmittedPauseCompletesAfterActualAllowedRefresh(t *testing.T) {
	w, r, runtime, ref, _, request := preparedPauseRequestFixture(t)
	called := false
	runtime.ControlRecovery = afterSealControlRecovery{SealedControlRecovery: runtime.ControlRecovery, after: func() {
		called = true
		view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
		if err != nil || view.Stage != "sealing" || view.SealReason != "quota_pause" {
			t.Fatal("quota changed before actual admitted seal", view, err)
		}
		release, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal("postseal domain phase retained tick", err)
		}
		release()
		makeActualQuotaAllowed(t, r, request)
	}}
	if err := recoverRequestedCoordinators(context.Background(), w.brain, runtime); err != nil {
		t.Fatal("new allowed quota stranded actual sealed close", err)
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if !called || err != nil || view.Stage != "closed" || view.SealReason != "quota_pause" {
		t.Fatal(called, view, err)
	}
	if err := runtime.ValidateAttemptClosure(context.Background(), codex.AttemptClosureRef{Attempt: ref, ClosureSHA256: view.ClosureSHA256}); err != nil {
		t.Fatal(err)
	}
}

func TestFactorySecondSameReasonRequestCannotOwnExistingSeal(t *testing.T) {
	w, r, runtime, ref, firstRef, first := preparedPauseRequestFixture(t)
	interrupted := errors.New("actual post-seal domain boundary interrupted")
	originalRecovery := runtime.ControlRecovery
	runtime.ControlRecovery = sealedControlRecoveryForRequestTest(func(context.Context, effects.AttemptKey) error { return interrupted })
	if err := recoverRequestedCoordinators(context.Background(), w.brain, runtime); !errors.Is(err, interrupted) {
		t.Fatal(err)
	}
	runtime.ControlRecovery = originalRecovery
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "sealing" || view.SealReason != "quota_pause" {
		t.Fatal("actual original seal absent", view, err)
	}
	ev, err := (ProgramEffects{Brain: w.brain}).boundEffectEvent(first.Cause)
	if err != nil {
		t.Fatal(err)
	}
	var old coordinatorQuotaCause
	if err := json.Unmarshal([]byte(ev.Text), &old); err != nil {
		t.Fatal(err)
	}
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	at := old.Admission.NextCheckAt
	reader := coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		q, e := (b2CodexUsage{}).Read(context.Background())
		q.ObservedAt = at
		for i := range q.Windows {
			q.Windows[i].ResetsAt = at.Add(q.Windows[i].Duration)
		}
		q.Windows[0].Used = 100
		return q, e
	})
	next := helper.ReadCodexAdmissionQuota(context.Background(), w.brain, selected, old.Policy, reader, func() time.Time { return at })
	if next.Decision.State != "pause" || !next.NextCheckAt.After(old.Admission.NextCheckAt) {
		t.Fatal("real shared schedule did not observe next pause", next)
	}
	sess := r.ents["e1-s1-orchestrator"]
	cause, err := r.recordCoordinatorQuotaLocked(sess, view, codexQuotaObservation{SelectionSHA256: selectionHash(selected), Policy: old.Policy, Admission: next})
	if err != nil {
		t.Fatal(err)
	}
	if cause == first.Cause {
		t.Fatal("second actual observation reused old event")
	}
	if _, err := r.requestCoordinatorCloseLocked(sess, "process_pause", "quota_pause", cause); !errors.Is(err, codex.ErrAttemptConflict) {
		t.Fatal("second same-reason request adopted original seal", err)
	}
	// A new runtime consumes the same durable request and actual seal after restart.
	second := first
	second.Cause = cause
	makeActualQuotaAllowed(t, r, second)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFactoryCodexRuntime(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	reopened.AttemptAuthority.(*factoryAttemptAuthority).verify = b2Qualification
	if err := recoverRequestedCoordinators(context.Background(), w.brain, reopened); err != nil {
		t.Fatal("exact admitted request failed after new runtime", err)
	}
	closed, err := reopened.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || closed.Stage != "closed" || closed.CloseCause == nil || *closed.CloseCause != firstRef.Event {
		t.Fatal(closed, err)
	}
	// The immutable first request owns this exact closure; a future successor is separate.
}

type sealedControlRecoveryForRequestTest func(context.Context, effects.AttemptKey) error

func (f sealedControlRecoveryForRequestTest) RecoverSealedControls(ctx context.Context, key effects.AttemptKey) error {
	return f(ctx, key)
}

func TestFactoryCapabilityOnlyHoldDoesNotRequestLivingStop(t *testing.T) {
	w, r, runtime, ref := recoveryRealCoordinator(t)
	r.d.ExecutorAdmission = func(context.Context, executor.Selection) error { return errors.New("actual unit capability hold") }
	r.pauseCodexSessions()
	for _, ev := range r.ents["e1-s1-orchestrator"].evs {
		if ev.Recipient == "codex-attempt" && ev.Request == "close" {
			t.Fatal("capability hold became stop permission")
		}
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "claimed" || view.SealReason != "" {
		t.Fatal(view, err)
	}
	_, _, err = codex.CoordinatorAttemptStartAuthority(w.brain, ref.SessionID)
	if err != nil {
		t.Fatal("healthy admitted host lost actual readiness", err)
	}
}

type blockingRecoveryContainment struct {
	codex.Containment
	entered chan struct{}
	release chan struct{}
}

func (c blockingRecoveryContainment) Fence(ctx context.Context, owner codex.Owner) error {
	if err := c.Containment.Fence(ctx, owner); err != nil {
		return err
	}
	close(c.entered)
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestFactoryRequestedPhysicalFenceDoesNotHoldTick(t *testing.T) {
	w, _, runtime, ref, _ := pauseRequestFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	runtime.Containment = blockingRecoveryContainment{Containment: runtime.Containment, entered: entered, release: release}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	done := make(chan error, 1)
	recoverOnce := func() { done <- recoverRequestedCoordinators(ctx, w.brain, runtime) }
	go recoverOnce()
	select {
	case <-entered:
	case err := <-done:
		if !errors.Is(err, codex.ErrObservationUnknown) {
			t.Fatal("unexpected pre-fence closure failure", err)
		}
		view, readErr := runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
		if readErr != nil || view.NextCheckAt.IsZero() || view.Stage != "sealing" {
			t.Fatal("unknown physical closure not retained", view, readErr)
		}
		t.Logf("same request waits for actual persisted physical due time %s", view.NextCheckAt)
		timer := time.NewTimer(time.Until(view.NextCheckAt))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		go recoverOnce()
		select {
		case <-entered:
		case err := <-done:
			t.Fatal("ordinary same-request due retry did not reach actual fence", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		close(release)
		t.Fatal("blocked physical fence owns tick", err)
	}
	unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	view, err := runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil || view.Stage != "closed" {
		t.Fatal("actual physical request did not close", view, err)
	}
}

func TestFactoryUnselectedCodexRuntimeDoesNotRecoverRetainedState(t *testing.T) {
	for _, kind := range []string{"default-claude", "retained-z"} {
		t.Run(kind, func(t *testing.T) {
			var w *world
			if kind == "retained-z" {
				w = newWorld(t)
				w.epic("e1", "planned")
				put(t, filepath.Join(w.brain, "factory/executors/e1/selection.json"), `{"schema_version":1,"executor":"z","epic_id":"e1","created_at":"2026-10-09T12:00:00Z"}`)
			} else {
				w = newWorld(t)
				w.epic("e1", "planned")
			}
			directory := filepath.Join(w.brain, "factory/codex/sessions", strings.Repeat("a", 64))
			if err := os.MkdirAll(directory, 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(directory, "retained-private-state")
			if err := os.WriteFile(sentinel, []byte("unselected retained evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			runtime := &splitRecoveryRuntime{sampleCodexRuntime: &sampleCodexRuntime{}, brain: w.brain}
			d := w.deps()
			d.Runtimes = map[string]session.Runtime{"codex": runtime}
			if err := Tick(context.Background(), d, false); err != nil {
				t.Fatal(err)
			}
			if len(runtime.calls) != 0 {
				t.Fatal("unselected Codex runtime recovered", kind, runtime.calls)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "unselected retained evidence" {
				t.Fatal("unselected evidence changed", err)
			}
		})
	}
}

func TestFactoryKnownSelectedRecoverySurvivesOtherCorruptSelection(t *testing.T) {
	w, d, base := codexSampleWorld(t)
	w.epic("e-bad", "planned")
	put(t, filepath.Join(w.brain, "factory/executors/e-bad/selection.json"), `{"executor":"invalid"}`)
	runtime := &splitRecoveryRuntime{sampleCodexRuntime: base, brain: w.brain}
	d.Runtimes["codex"] = runtime
	if err := recoverModernCoordinators(context.Background(), d); err == nil {
		t.Fatal("corrupt independent selection disappeared")
	}
	if fmt.Sprint(runtime.calls) != "[modern]" {
		t.Fatal("known selected owner did not recover", runtime.calls)
	}
}
