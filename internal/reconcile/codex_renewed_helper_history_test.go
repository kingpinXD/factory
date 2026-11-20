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

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type renewedHelperHistoryAuthority struct{ renewedControlAuthority }

func (a renewedHelperHistoryAuthority) Refresh(ctx context.Context, old helper.Scope) (helper.Scope, error) {
	return a.refreshQualified(ctx, old, b2Qualification)
}

func TestNativeLeaseRenewedHelperHistory(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, prior := range []string{"none", "reserved_fresh_output", "reserved_same_output", "reserved_restart_escalation", "reserved_live_item33_history"} {
		t.Run(prior, func(t *testing.T) {
			w, r, rt, owner, item, id, name := normalManagerWorkFixture(t)
			service := helper.Service{Brain: w.brain, Authority: renewedHelperHistoryAuthority{renewedControlAuthority{b2OfflineAuthority{NativeAuthority{w.brain}}}}}
			if _, err := r.move(item, fsm.Cur{State: "starting"}, "tick", "tick:1"); err != nil {
				t.Fatal(err)
			}
			req := helper.Request{SchemaVersion: 1, RequestKey: "old-explore", OwnerID: owner.id, SetID: owner.id, WorkID: item.id, Lease: 1, Component: "explorer", Prompt: "Explore the work", OutputPath: filepath.Join(item.entry.Dir, "explore.v1.md")}
			oldScope, err := service.Authority.Resolve(r.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			var oldJob string
			if strings.HasPrefix(prior, "reserved_") {
				accepted, err := service.Submit(r.ctx, req)
				if err != nil {
					t.Fatal("old helper submit", err)
				}
				oldJob = accepted.JobID
				if err := service.Fence(r.ctx, item.id, "old generation closed"); err != nil {
					t.Fatal("old helper fence", err)
				}
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				flushCtx, cancel := context.WithTimeout(r.ctx, time.Second)
				err = helper.Run(flushCtx, helper.RunnerDeps{Service: service, FactoryBin: binary, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }})
				cancel()
				if err != nil && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("flush cancelled helper", err)
				}
				result, err := service.Result(oldJob, 1)
				if err != nil || result.Recovery == nil || !result.Recovery.PublicationSettled {
					t.Fatal("old helper not settled", result, err)
				}
			}
			// Exercise the actual timeout constructor, rather than hand-writing its result.
			timeoutCount := 2
			if prior == "reserved_live_item33_history" {
				appendLiveStepManagerHistory(t, r, item, owner)
				timeoutCount = 0
			}
			if prior == "reserved_restart_escalation" {
				timeoutCount = r.b.Values.Supervision.RestartsPerItem
			}
			for n := 1; n <= timeoutCount; n++ {
				next, fired := fsm.Timeout(item.m, item.cur, r.now.Add(time.Duration(n)*24*time.Hour), false)
				if !fired {
					t.Fatal("timeout did not fire")
				}
				if _, err := r.move(item, next, "tick", "timeout:"+seqRef(events.Event{Seq: n})); err != nil {
					t.Fatal(err)
				}
				if prior == "reserved_restart_escalation" {
					timeout := item.evs[len(item.evs)-1]
					if _, err := r.append(item, events.Event{At: r.now, Kind: events.KindRestart, Sender: events.SenderProgram, Text: "starting timed out", Key: "restart:timeout:" + seqRef(timeout)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if prior == "reserved_restart_escalation" {
				ok, ref, err := r.restartsExhausted(item)
				if err != nil || !ok || !strings.HasPrefix(ref, restartsRef) {
					t.Fatal("real restart guard did not fire", ref, err)
				}
				appendRenewedHelperHistoryMove(t, r, item, stateBlocked, "tick", ref)
				instruction, err := r.append(item, events.Event{At: r.now, Kind: events.KindInstruction, Sender: owner.id, Text: "Resume explorer after recovery"})
				if err != nil {
					t.Fatal(err)
				}
				ok, ref, err = r.instructionReceived(item)
				if err != nil || !ok || ref != seqRef(instruction) {
					t.Fatal("real instruction guard did not fire", ref, err)
				}
				appendRenewedHelperHistoryMove(t, r, item, "previous", "file", ref)
				if item.cur.State != "starting" {
					t.Fatal("instruction did not return to starting", item.cur)
				}

				// The old restarts have left the manager's hourly process budget.
				r.now = r.now.Add(2 * time.Hour)
			}

			owner.st.Lease = 2
			if _, err := r.grantLease(owner, "T18 expiry with earlier explorer state"); err != nil {
				t.Fatal(err)
			}
			manage := func() error {
				return managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, r.ents[id]) })
			}
			if err := manage(); err != nil {
				t.Fatal(err)
			}
			if err := recoverRequestedCoordinators(r.ctx, w.brain, rt); err != nil {
				t.Fatal(err)
			}
			if err := manage(); err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
				t.Fatal("successor not selected", err, "owner", owner.cur, "work", item.cur, w.out.String())
			}
			rt.AttemptCapabilities = b2AttemptCaps{}
			r.codexLaunched = nil
			if err := manage(); err != nil {
				t.Fatal(err)
			}
			view, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			var record codex.Record
			for {
				record = settlementAttemptRecord(t, w.brain, view.Ref)
				if record.Claimed && record.State == "active" && record.TurnID != "" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("successor not active")
				}
				time.Sleep(5 * time.Millisecond)
			}
			req.Lease = 2
			req.RequestKey = "explore-resume-lease2-1"
			req.OutputPath = filepath.Join(item.entry.Dir, "explore.v2.md")
			if prior == "reserved_same_output" {
				req.OutputPath = filepath.Join(item.entry.Dir, "explore.v1.md")
			}
			currentScope, err := service.Authority.Resolve(r.ctx, req)
			if err != nil || currentScope.Generation == oldScope.Generation || currentScope.Lease != 2 {
				t.Fatal("scope was not renewed", currentScope, err)
			}
			_, activation, err := nativeAuthority(r.ctx, w.brain, "e1", item.id, true, nil, b2Qualification)
			if err != nil || activation.OwnerActivation == nil || activation.WorkGeneration != currentScope.Generation || activation.Previous == nil {
				t.Fatal("work activation was not renewed", activation, err)
			}
			if record.Spec.Generation != owner.id+":2:"+componentOrchestrator {
				t.Fatal("old coordinator generation", record.Spec.Generation)
			}
			if _, err := os.Stat(filepath.Join(item.entry.Dir, "explore.v1.md")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("old reserved output unexpectedly exists", err)
			}

			payload, _ := json.Marshal(req)
			wire, _ := json.Marshal(toolcontract.Envelope{Op: "helper_submit", Payload: payload})
			caller := effects.ControlID{Attempt: effects.AttemptKey{SessionID: view.Ref.SessionID, AttemptID: view.Ref.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "t18-helper-submit"}
			original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, wire, &caller, nil)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := controlrecord.StoreOriginal(w.brain, original)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
			if err != nil {
				t.Fatal(err)
			}
			source, err := buildControlIngressLocked(r.ctx, w.brain, ref, writer, b2Qualification)
			if err != nil {
				t.Fatal("ingress", err)
			}
			witness, err := service.CaptureControlAdmission(r.ctx, ref, source)
			if err != nil {
				t.Fatalf("renewed capture: evidence=%v: %v", errors.Is(err, effects.ErrEvidence), err)
			}
			t.Logf("capture succeeded with prior=%s old_job=%s", prior, oldJob)
			if _, err := helper.ReadControlAdmissionWitnessLocal(w.brain, witness); err != nil {
				t.Fatal(err)
			}
			enrolled, err := rt.Effects.EnrollControl(r.ctx, caller, source.Input)
			if err != nil {
				t.Fatal("renewed control enrollment", err)
			}
			enrollment, err := controlrecord.StoreEnrollmentSnapshot(w.brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: ref, Source: witness, Receipt: enrolled, Input: source.Input})
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := service.SubmitControl(r.ctx, witness, enrollment)
			if prior == "reserved_same_output" {
				if err == nil || !strings.Contains(err.Error(), "output already reserved") {
					t.Fatal("old output reservation bypassed", accepted, err)
				}
			} else {
				if err != nil || accepted.JobID == oldJob {
					t.Fatal("fresh submission failed or adopted old job", accepted, err)
				}
				job, err := service.Job(accepted.JobID)
				if err != nil || job.Scope.Authorization() != currentScope.Authorization() || job.Request != req {
					t.Fatal("new job authority mismatch", job, err)
				}
			}
			if oldJob != "" {
				old, err := service.Job(oldJob)
				if err != nil || old.Scope.Authorization() != oldScope.Authorization() || old.State != "cancelled" || old.Round != 1 || old.Generation != 1 {
					t.Fatal("old job adopted or replayed", old, err)
				}
			}
			jobs, err := service.Jobs()
			want := 1
			if prior == "reserved_fresh_output" || prior == "reserved_restart_escalation" || prior == "reserved_live_item33_history" {
				want = 2
			}
			if err != nil || len(jobs) != want {
				t.Fatal("wrong number of admissions", jobs, err)
			}
			// Changing evidence is still refused after closed-scope recovery.
			mismatched := source
			mismatched.Input.PayloadSHA256 = effects.Digest{99}
			if _, err := service.CaptureControlAdmission(r.ctx, ref, mismatched); !errors.Is(err, helper.ErrConflict) {
				t.Fatal("changed payload evidence accepted", err)
			}

			// A corrupt historical scheduler cause still fails in step replay,
			// even though current ingress/lease/activation evidence is valid.
			logPath := store.EventsPath(item.entry.Dir)
			before, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			history := w.log(item.id)
			altered := false
			var raw []byte
			for _, ev := range history {
				if ev.Kind == events.KindTransition && ev.From == "queued" && ev.To == "starting" {
					ev.TriggerRef = "tick:0"
					altered = true
				}
				line, err := json.Marshal(ev)
				if err != nil {
					t.Fatal(err)
				}
				raw = append(raw, append(line, '\n')...)
			}
			if !altered {
				t.Fatal("missing historical scheduler transition")
			}
			if err := os.WriteFile(logPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			badSource, err := buildControlIngressLocked(r.ctx, w.brain, ref, writer, b2Qualification)
			if err != nil {
				t.Fatal("corruption should be caught by step check, not ingress", err)
			}
			_, err = service.CaptureControlAdmission(r.ctx, ref, badSource)
			if !errors.Is(err, effects.ErrEvidence) || !strings.Contains(err.Error(), "work step cause missing or unsupported") {
				t.Fatal("invalid historical cause accepted or failed wrong guard", err)
			}
			if err := os.WriteFile(logPath, before, 0600); err != nil {
				t.Fatal(err)
			}

		})
	}
}

// Record already-settled history without rerunning its recheck side effects.
// The caller obtains the cause from the real guard before each FSM move.
func appendRenewedHelperHistoryMove(t *testing.T, r *run, item *entity, to, trigger, ref string) {
	t.Helper()
	guards := map[string]fsm.GuardFunc{}
	for _, tr := range item.m.Transitions {
		guards[tr.Guard] = func(fsm.Cur, fsm.Event) (bool, error) { return true, nil }
	}
	next, err := fsm.Apply(item.m, item.cur, fsm.Event{To: to, Trigger: trigger, At: r.now}, guards)
	if err != nil {
		t.Fatal(err)
	}
	event, err := r.append(item, events.Event{At: r.now, Kind: events.KindTransition, Sender: events.SenderProgram, From: item.cur.State, To: next.State, Trigger: trigger, TriggerRef: ref, Prev: next.Prev, Attempt: next.Attempt, Key: events.Key(item.id, item.cur.State, next.State, ref, next.Attempt)})
	if err != nil {
		t.Fatal(err)
	}
	item.cur, item.entered, item.enteredBy = next, event.Seq, event
}
