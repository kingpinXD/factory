package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func renewedQualification(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
	ref, err := b2Qualification(ctx, brain, selected)
	ref.ReceiptSHA256 = strings.Repeat("c", 64)
	return ref, err
}

func TestCodexQualificationChangeWaitsForExplicitRetry(t *testing.T) {
	w, r, _ := b2World(t)
	owner := r.ents["e1"]
	b2Activate(t, r, owner.id)
	owner.evs = w.log(owner.id)
	r.verificationQualifier = renewedQualification
	r.tickEntity(owner)
	if owner.cur.State != stateNeedsYou {
		t.Fatalf("changed receipt leaves epic in %s, want needs_you", owner.cur.State)
	}
	if why := r.needsYouWhy(owner, owner.enteredBy); !strings.Contains(why, "factory retry e1") || !strings.Contains(why, "verification changed") {
		t.Fatalf("waiting_on_user reason omits receipt change or retry: %q", why)
	}
	report, err := Status(r.ctx, w.deps())
	if err != nil || len(report.Waiting) == 0 || !strings.Contains(report.Waiting[0].Why, "verification changed") {
		t.Fatalf("factory status omits qualification wait: %+v %v", report.Waiting, err)
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", "e1", true, nil, renewedQualification); err == nil || !strings.Contains(err.Error(), "qualification changed") {
		t.Fatalf("changed proof admitted active owner: %v", err)
	}
	// A request alone has no successful transition and cannot release the hold.
	if _, err := r.append(owner, Request(RequestRetry, "previous", "user", "")); err != nil {
		t.Fatal(err)
	}
	if _, held := r.codexQualificationChanged(owner); !held {
		t.Fatal("unapplied retry cleared changed-qualification hold")
	}
	if err := Retry(r.ctx, w.deps(), owner.id); err != nil {
		t.Fatal(err)
	}
	owner.evs = w.log(owner.id)
	if err := r.applyRequest(owner, owner.evs[len(owner.evs)-1]); err != nil {
		t.Fatal(err)
	}
	if _, held := r.codexQualificationChanged(owner); held {
		t.Fatal("successful user retry did not release receipt hold")
	}
}

func TestCodexQualificationRetirementCannotAuthorizeChangedInputsOrEmptyBinding(t *testing.T) {
	for _, changed := range []string{"inputs", "binding"} {
		t.Run(changed, func(t *testing.T) {
			w, r, _ := b2World(t)
			b2Activate(t, r, "e1")
			authority, _, err := nativeAuthority(r.ctx, w.brain, "e1", "e1", true, nil, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			verify := nativeAcceptance(renewedQualification)
			if _, err := qualificationRetirementAuthority(r.ctx, w.brain, authority, verify); err != nil {
				t.Fatal("same-binding owner cannot be retired", err)
			}
			if changed == "inputs" {
				data, err := os.ReadFile(store.InputsPath(w.dir("e1")))
				if err != nil {
					t.Fatal(err)
				}
				put(t, store.InputsPath(w.dir("e1")), string(data)+"\n")
			} else {
				verify = func(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
					ref, err := renewedQualification(ctx, brain, selected)
					ref.Binding = ""
					return ref, err
				}
			}
			if _, err := qualificationRetirementAuthority(r.ctx, w.brain, authority, verify); err == nil {
				t.Fatal("retirement accepted changed " + changed)
			}
		})
	}
}

type qualificationEventAuthority struct {
	NativeAuthority
	verify nativeAcceptance
}

func (a qualificationEventAuthority) ValidateControlEventSourceLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, source controlrecord.ImmutableRef) error {
	return a.validateControlEventSourceLocal(ctx, original, input, source, a.verify)
}

func qualificationCompletedEvent(t *testing.T, r *run, runtime *codex.Runtime, attempt codex.AttemptRef) (effects.ControlID, effects.ControlInput, ControlEventResult) {
	t.Helper()
	record := settlementAttemptRecord(t, r.d.Brain, attempt)
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "completed-before-reverification"}
	payload, _ := json.Marshal(map[string]any{"op": "event", "payload": map[string]any{"kind": "message", "text": "retained completed planner event"}})
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, payload, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(r.d.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := (NativeAuthority{r.d.Brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	validator := runtime.Effects.Controls.(*codex.FactoryControlValidator)
	validator.EventEvidence = qualificationEventAuthority{NativeAuthority{r.d.Brain}, b2Qualification}
	var source controlrecord.ControlEventSource
	var result ControlEventResult
	if err := managerTickCall(t, r.d.Brain, func() error {
		sourceRef, err := captureControlEventSourceLocked(r.ctx, r.d.Brain, ref, writer, b2Qualification)
		if err != nil {
			return err
		}
		source, err = controlrecord.ReadControlEventSource(r.d.Brain, sourceRef)
		if err != nil {
			return err
		}
		receipt, err := runtime.Effects.EnrollControl(r.ctx, id, source.Input)
		if err != nil {
			return err
		}
		enrollment, err := controlrecord.StoreEnrollmentSnapshot(r.d.Brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: ref, Source: sourceRef, Receipt: receipt, Input: source.Input})
		if err != nil {
			return err
		}
		result, err = applyControlEventLocked(r.ctx, r.d.Brain, sourceRef, enrollment, writer, b2Qualification)
		if err != nil {
			return err
		}
		if _, err := controlrecord.StoreOutcome(r.d.Brain, controlrecord.OutcomeRecord{Schema: 2, Original: ref, Input: source.Input, Outcome: result.Outcome, Reply: result.Reply, ReplySHA256: sha256.Sum256(result.Reply), Proof: result.Proof}); err != nil {
			return err
		}
		_, err = runtime.Effects.CompleteControl(r.ctx, id, result.Outcome)
		return err
	}); err != nil {
		t.Fatal("complete actual planner event before re-verification", err)
	}
	return id, source.Input, result
}

func qualificationPendingEvent(t *testing.T, r *run, attempt codex.AttemptRef) {
	t.Helper()
	record := settlementAttemptRecord(t, r.d.Brain, attempt)
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "pending-before-reverification"}
	payload, _ := json.Marshal(map[string]any{"op": "event", "payload": map[string]any{"kind": "message", "text": "retained pending planner event"}})
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, payload, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(r.d.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(ref)
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestControlEventReceivingDomainChild$")
	command.Env = append(os.Environ(), "FACTORY_EVENT_SOURCE_CHILD="+r.d.Brain, "FACTORY_EVENT_SOURCE_INPUT="+string(input), "FACTORY_EVENT_SOURCE_CUT=after-receipt")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		_ = stdin.Close()
	})
	var source controlrecord.ImmutableRef
	if err := json.NewDecoder(stdout).Decode(&source); err != nil {
		t.Fatal("receiving child failed before durable append", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	if err == nil {
		t.Fatal("receiving child exited without kill")
	}
	if _, _, err := controlrecord.FindOutcome(r.d.Brain, ref); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture did not retain a pending control", err)
	}
}

func TestCodexPlannerRetryClosesActiveAttemptBeforeReceiptRenewal(t *testing.T) {
	testCodexPlannerRetryClosesActiveAttempt(t, renewedQualification)
}

func TestCodexPlannerRetryAfterBinaryUpgradeRetiresOldBinding(t *testing.T) {
	testCodexPlannerRetryClosesActiveAttempt(t, func(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
		ref, err := renewedQualification(ctx, brain, selected)
		ref.Binding = strings.Repeat("d", 64)
		return ref, err
	})
}

func TestCodexPlannerRetryRecoversUnconfirmedClosure(t *testing.T) {
	testCodexPlannerRetryClosesActiveAttempt(t, renewedQualification, "scheduled")
}

func TestCodexPlannerRetryAfterBinaryUpgradeRecoversUnconfirmedClosure(t *testing.T) {
	for _, retryAgain := range []bool{false, true} {
		name := "scheduled"
		if retryAgain {
			name = "second_retry"
		}
		t.Run(name, func(t *testing.T) {
			testCodexPlannerRetryClosesActiveAttempt(t, func(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
				ref, err := renewedQualification(ctx, brain, selected)
				ref.Binding = strings.Repeat("d", 64)
				return ref, err
			}, name)
		})
	}
}

func TestCodexQualificationRetryKeepsRealRecoveryHeld(t *testing.T) {
	for _, reason := range []string{"effects_unsettled", "integration_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			testCodexPlannerRetryClosesActiveAttempt(t, renewedQualification, reason)
		})
	}
}

type qualificationUnconfirmedContainment struct {
	codex.Containment
	pending bool
}

func (c *qualificationUnconfirmedContainment) Fence(ctx context.Context, owner codex.Owner) error {
	if c.pending {
		return codex.ErrObservationUnknown
	}
	return c.Containment.Fence(ctx, owner)
}

func testCodexPlannerRetryClosesActiveAttempt(t *testing.T, qualify nativeAcceptance, delayed ...string) {
	t.Helper()
	if isolatedParallelTest(t) {
		return
	}
	w, r, runtime, _ := ordinaryManagerFixture(t)
	owner := r.ents["e1"]
	// Start from the real planner entry rather than the generic world's planned epic.
	history := w.log(owner.id)
	history[0].To = epicChecking
	var raw []byte
	for _, event := range history {
		line, _ := json.Marshal(event)
		raw = append(raw, append(line, '\n')...)
	}
	put(t, store.EventsPath(owner.entry.Dir), string(raw))
	owner.evs, owner.cur.State = history, epicChecking
	owner.enteredBy = history[0]
	id, name := r.sessionSlot(owner, componentPlanner)
	if err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentPlanner, id, name) }); err != nil {
		t.Fatal(err)
	}
	sess := r.ents[id]
	before, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		record := settlementAttemptRecord(t, w.brain, before.Ref)
		if record.Claimed && record.State == "active" && record.TurnID != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual planner did not begin a turn: state=%s claimed=%v turn=%q", record.State, record.Claimed, record.TurnID)
		}
		time.Sleep(5 * time.Millisecond)
	}
	completedID, completedInput, completedEvent := qualificationCompletedEvent(t, r, runtime, before.Ref)
	if len(delayed) != 0 {
		if strings.HasPrefix(delayed[0], "issue_") {
			qualificationFailedIssue(t, r, runtime, before.Ref, delayed[0])
		} else {
			qualificationPendingEvent(t, r, before.Ref)
		}
		runtime.ControlRecovery.(*ControlOperationRecovery).verify = qualify
	}
	oldAuthority, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	r.verificationQualifier = qualify
	runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = qualify
	validator := runtime.Effects.Controls.(*codex.FactoryControlValidator)
	validator.EventEvidence = qualificationEventAuthority{NativeAuthority{w.brain}, qualify}
	validator.WriteEvidence = qualificationWriteAuthority{w.brain, qualify}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, false, &oldAuthority, qualify); err == nil || !strings.Contains(err.Error(), "qualification changed") {
		t.Fatal("changed proof accepted retained authority outside sealed retirement", err)
	}
	spec, err := codex.AttemptControlAuthority(w.brain, before.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.AttemptAuthority.(*factoryAttemptAuthority).ValidatePrepare(r.ctx, spec); err == nil || !strings.Contains(err.Error(), "qualification changed") {
		t.Fatal("changed proof authorized old coordinator preparation", err)
	}
	if err := validator.ValidateOutcome(r.ctx, completedID, completedInput, completedEvent.Outcome); err == nil || !strings.Contains(err.Error(), "qualification changed") {
		t.Fatal("completed old event accepted changed proof outside sealed retirement", err)
	}
	owner.evs = w.log(owner.id)
	r.tickEntity(owner)
	if owner.cur.State != stateNeedsYou {
		t.Fatalf("active planner receipt change stayed %s", owner.cur.State)
	}
	if err := managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) }); err != nil {
		t.Fatal(err)
	}
	still, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || still.Ref != before.Ref || still.Stage != "claimed" {
		t.Fatal("hold adopted or replaced active attempt", still, err)
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, &oldAuthority, qualify); err == nil {
		t.Fatal("old active-turn authority accepted new receipt")
	}
	if err := Retry(r.ctx, w.deps(), owner.id); err != nil {
		t.Fatal(err)
	}
	owner.evs = w.log(owner.id)
	r.applyRequests(owner)
	missing := *runtime
	missing.Brain = filepath.Join(w.brain, "missing-attempt-metadata")
	r.d.Runtimes["codex"] = &missing
	if ref, held := r.codexQualificationChanged(owner); !held || !strings.HasSuffix(ref, ":closure-unconfirmed") {
		t.Fatal("missing earlier attempt metadata did not retain a user hold", ref)
	}
	if _, err := r.refreshCodexQualification(owner, sess, &missing, qualify); err == nil {
		t.Fatal("missing attempt metadata permitted renewal")
	}
	r.d.Runtimes["codex"] = runtime
	if err := managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) }); err != nil {
		t.Fatal("queue qualification stop", err)
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, qualify); err == nil {
		t.Fatal("receipt renewed before actual closure")
	}
	var stop events.Event
	for _, event := range sess.evs {
		if event.Recipient == "codex-attempt" && event.Request == "close" {
			stop = event
		}
	}
	var stopRequest CoordinatorCloseRequest
	if err := json.Unmarshal([]byte(stop.Text), &stopRequest); err != nil {
		t.Fatal(err)
	}
	retiring := qualificationSettlementContext(r.ctx, nativeEventRef(sess.id, stop), stopRequest)
	if err := validator.ValidateOutcome(retiring, completedID, completedInput, completedEvent.Outcome); err == nil {
		t.Fatal("retirement context accepted an unsealed active control")
	}
	annotated := controlrecord.WithOutcomeValidation(retiring, completedID)
	if _, _, err := nativeAuthority(annotated, w.brain, "e1", owner.id, true, &oldAuthority, qualify); err == nil {
		t.Fatal("retirement context admitted new work on active turn")
	}
	if len(delayed) != 0 {
		containment := &qualificationUnconfirmedContainment{Containment: runtime.Containment, pending: true}
		runtime.Containment = containment
		if err := recoverRequestedCoordinators(r.ctx, w.brain, runtime); !errors.Is(err, codex.ErrObservationUnknown) {
			t.Fatal("first closure did not retain an unconfirmed seal", err)
		}
		containment.pending = false
		view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
		if err != nil || view.Stage != "sealing" || !view.NextCheckAt.After(time.Now()) {
			t.Fatal("unconfirmed closure lacks scheduled recovery", view, err)
		}
		previousNow := r.now
		r.now = view.NextCheckAt.Add(time.Second)
		if ref, held := r.codexQualificationChanged(owner); !held || !strings.HasSuffix(ref, ":closure-unconfirmed") {
			t.Fatal("overdue closure without a fresh recovery schedule hid its hold", ref)
		}
		r.now = previousNow
		if delayed[0] == "second_retry" {
			old, _, _, err := nativeReadActivation(owner)
			if err != nil {
				t.Fatal(err)
			}
			next, err := qualify(r.ctx, w.brain, executor.Selection{})
			if err != nil {
				t.Fatal(err)
			}
			// Retain the transition emitted by older binaries before this fix.
			if _, err := r.move(owner, fsm.Cur{State: stateNeedsYou, Prev: []string{owner.cur.State}}, "tick", qualificationRef(old, next)+":closure-unconfirmed"); err != nil {
				t.Fatal(err)
			}
			if err := Retry(r.ctx, w.deps(), owner.id); err != nil {
				t.Fatal(err)
			}
			owner.evs = w.log(owner.id)
			r.applyRequests(owner)
			if err := managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) }); err != nil {
				t.Fatal("resume retained close after second retry", err)
			}
		}
		r.tickEntity(owner)
		if owner.cur.State == stateNeedsYou {
			t.Fatal("scheduled closure returned epic to needs_you", owner.enteredBy.TriggerRef)
		}
		if err := makeFixtureCodexRecoveryDue(r.ctx, w.brain, runtime, before.Ref, func() error { return recoverModernCoordinators(r.ctx, r.d) }); err != nil {
			t.Fatal("wait for actual fixture processes to stop", err)
		}
		if delayed[0] == "issue_prepared" || delayed[0] == "issue_intent" {
			qualificationIssueRemainsUnknown(t, r, runtime, before.Ref, name)
			return
		}
		if delayed[0] == "effects_unsettled" || delayed[0] == "integration_unavailable" {
			runtime.ControlRecovery = sealedControlRecoveryForRequestTest(func(context.Context, effects.AttemptKey) error {
				if delayed[0] == "integration_unavailable" {
					return codex.ErrAttemptIntegration
				}
				return &effects.AttemptSettlementHeldError{Attempt: completedID.Attempt}
			})
			if err := recoverModernCoordinators(r.ctx, r.d); err == nil {
				t.Fatal("genuine recovery hold disappeared")
			}
			view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
			if err != nil || view.RecoveryReason != delayed[0] || !view.NextCheckAt.After(time.Now()) {
				t.Fatal("genuine recovery hold lost its reason or schedule", view, err)
			}
			r.tickEntity(owner)
			if owner.cur.State != stateNeedsYou {
				t.Fatal("genuine held recovery did not request user action")
			}
			return
		}
		if err := recoverModernCoordinators(r.ctx, r.d); err != nil {
			t.Fatal("due recovery tick cannot finish retained close", err)
		}
	} else {
		recoverOnce := func() error { return recoverRequestedCoordinators(r.ctx, w.brain, runtime) }
		if err := recoverOnce(); errors.Is(err, codex.ErrObservationUnknown) {
			if err := makeFixtureCodexRecoveryDue(r.ctx, w.brain, runtime, before.Ref, recoverOnce); err != nil {
				t.Fatal("wait for actual fixture processes to stop", err)
			}
			if err := recoverOnce(); err != nil {
				t.Fatal("finish actual planner closure", err)
			}
		} else if err != nil {
			t.Fatal("physically retire old planner", err)
		}
	}
	closed, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || closed.Stage != "closed" {
		t.Fatal("old planner not closed", closed, err)
	}
	if err := runtime.ValidateAttemptClosure(r.ctx, codex.AttemptClosureRef{Attempt: before.Ref, ClosureSHA256: closed.ClosureSHA256}); err != nil {
		t.Fatal("closure lacks durable fencing/settlement", err)
	}
	if err := validator.ValidateOutcome(retiring, completedID, completedInput, completedEvent.Outcome); err != nil {
		t.Fatal("sealed retirement cannot validate completed old event", err)
	}
	for _, changed := range []string{"attempt", "call"} {
		other := completedID
		if changed == "attempt" {
			other.Attempt.AttemptID = strings.Repeat("e", 64)
		} else {
			other.CallID = "not-enrolled-in-sealed-attempt"
		}
		wrong := controlrecord.WithOutcomeValidation(retiring, other)
		if _, _, err := nativeAuthority(wrong, w.brain, "e1", owner.id, false, &oldAuthority, qualify); err == nil {
			t.Fatal("retirement accepted unrelated " + changed)
		}
	}
	closedRecord := settlementAttemptRecord(t, w.brain, before.Ref)
	wantControls := uint64(1)
	if len(delayed) != 0 {
		wantControls++
	}
	if closedRecord.Closure.Settlement.Journal.ControlCount != wantControls {
		t.Fatal("closure did not retain the completed control")
	}
	unconfirmed := *runtime
	unconfirmed.Effects = nil
	r.d.Runtimes["codex"] = &unconfirmed
	if ref, held := r.codexQualificationChanged(owner); !held || !strings.HasSuffix(ref, ":closure-unconfirmed") {
		t.Fatal("invalid durable closure did not retain a user hold", ref)
	}
	r.d.Runtimes["codex"] = runtime
	eventCount := 0
	for _, event := range w.log(owner.id) {
		if event.Kind == events.KindMessage && event.Text == "retained completed planner event" {
			eventCount++
		}
	}
	if eventCount != 1 {
		t.Fatalf("retirement replayed completed control: %d messages", eventCount)
	}
	// Stop at preparation: the child fixture pins the original test receipt.
	runtime.AttemptCapabilities = managerDenyCapabilities{}
	err = managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) })
	if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("renewed planner did not reach normal successor preparation", err)
	}
	next, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || next.Ref == before.Ref || next.Stage != "prepared" {
		t.Fatal("distinct successor was not prepared", next, err)
	}
	owner.evs = w.log(owner.id)
	activation, _, _, err := nativeReadActivation(owner)
	if err != nil || activation.Previous == nil || *activation.Previous != oldAuthority.Activation {
		t.Fatal("renewal did not chain old activation", err)
	}
	expectedAcceptance, err := qualify(r.ctx, w.brain, executor.Selection{})
	if err != nil || activation.Acceptance != expectedAcceptance {
		t.Fatal("successor did not activate under the newly verified binding", activation.Acceptance, err)
	}
	r.tickEntity(owner)
	if owner.cur.State == stateNeedsYou {
		t.Fatal("retry returned the renewed epic to needs_you", owner.enteredBy.TriggerRef)
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, qualify); err != nil {
		t.Fatal("renewed planner authority unusable", err)
	}
	count := len(owner.evs)
	_ = managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) })
	if len(w.log(owner.id)) != count {
		t.Fatal("repeated tick duplicated qualification activation")
	}
	if len(r.errs) != 0 {
		t.Fatal("follow-up manager tick failed", r.errs)
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, &oldAuthority, qualify); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatal("old authority accepted after renewal", err)
	}
}
