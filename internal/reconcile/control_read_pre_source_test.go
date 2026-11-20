package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func coordinatorPreOriginal(t *testing.T, w *world, req ControlReadRequest, call string) controlrecord.OriginalRequestRef {
	t.Helper()
	id := req.Control.Control
	id.CallID = call
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"github_pr","payload":{"repo":"o/r","number":12}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
func TestControlReadPreSourceCapturesBeforeEnrollmentAndBindsActualGlobalSetRead(t *testing.T) {
	w, runtime, attempt, old := realReadSourceFixture(t)
	original := coordinatorPreOriginal(t, w, old, "pre-source-global")
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := captureCoordinatorReadSourceLocked(context.Background(), w.brain, original, ControlReadDirect, writer, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	pre, _, err := readControlReadPreSourceLocal(context.Background(), w.brain, ref, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if pre.Coordinator == nil || pre.Helper != nil || pre.Coordinator.Work != nil || pre.Coordinator.Ingress.Input.Target.WorkID != "" || pre.Coordinator.Ingress.Writer != writer {
		t.Fatal("set read guessed an item or command writer")
	}
	pending, err := (effects.Service{Brain: w.brain}).PendingAttemptControls(context.Background(), pre.Coordinator.Ingress.Control.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range pending {
		if control.Receipt.Control == pre.Coordinator.Ingress.Control {
			t.Fatal("pre-source enrolled the control")
		}
	}
	control, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(context.Background(), pre.Coordinator.Ingress.Control, pre.Coordinator.Ingress.Input)
	if err != nil {
		t.Fatal(err)
	}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	admission, err := bindCoordinatorReadSourceLocked(context.Background(), w.brain, ref, control, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	if admission.Control == nil || admission.Control.Receipt != control || admission.Control.Original != original || admission.Work != nil {
		t.Fatal("binding invented receipt or work")
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(w.brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: ref, Receipt: control, Input: pre.Coordinator.Ingress.Input})
	if err != nil {
		t.Fatal(err)
	}
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	retained, err := readCoordinatorReadPreAdmissionLocal(context.Background(), w.brain, ref, enrollment, b2Qualification)
	if err != nil || !reflect.DeepEqual(retained, admission) {
		t.Fatalf("retained source needed the live proposer or changed admission: %v", err)
	}
	if _, err := bindCoordinatorReadSourceLocked(context.Background(), w.brain, ref, control, b2Qualification); err == nil {
		t.Fatal("closed old caller gained new binding authority")
	}
}
func TestControlReadPreSourcePreservesExplicitCompatibilityTargetAndRefusesRebinding(t *testing.T) {
	w, _, _, req := realReadSourceFixture(t)
	existing, err := buildControlReadLocked(context.Background(), w.brain, req, b2Qualification)
	if err != nil || existing.Work == nil || existing.Work.WorkID != "e1-w1" {
		t.Fatal("genuine existing E1 work target changed", err)
	}
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := captureCoordinatorReadSourceLocked(context.Background(), w.brain, req.Original, ControlReadDirect, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bindCoordinatorReadSourceLocked(context.Background(), w.brain, ref, *req.Control, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("existing E1 work input replaced global pre-source: %v", err)
	}
	if _, err := buildControlReadLocked(context.Background(), w.brain, req, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("compatibility wrapper ignored contradictory real pre-source: %v", err)
	}
}
func TestControlReadHelperPreSourceUsesActualJobAndNoCoordinatorEnrollment(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, service, job, req := realHelperReadSourceFixture(t)
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := captureHelperReadSourceLocked(context.Background(), w.brain, req.Original, ControlReadDirect, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	pre, _, err := readControlReadPreSourceLocal(context.Background(), w.brain, ref, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if pre.Coordinator != nil || pre.Helper == nil || pre.Helper.Job.ID != job.ID || pre.Helper.Caller.Generation != job.Generation || pre.Helper.Caller.Round != job.Round {
		t.Fatal("helper pre-source invented coordinator identity")
	}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	admission, err := bindHelperReadSourceLocked(context.Background(), w.brain, ref, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	if admission.Control != nil || admission.Helper == nil || admission.Helper.Original != req.Original || admission.Work == nil || admission.Work.Authorization() != job.Scope.Authorization() {
		t.Fatal("helper read binding invented scope")
	}
	if err := validateControlReadLocal(context.Background(), w.brain, admission, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Job(job.ID); err != nil {
		t.Fatal(err)
	}
	// No fake helper E1 control was created by either source phase.
	if pre.Helper.Caller.Input.Target.JobID != job.ID {
		t.Fatal("helper identity not retained")
	}
	if _, err := ReadHelperReadPreAdmissionLocal(context.Background(), w.brain, ref); err == nil {
		t.Fatal("bounded test qualifier leaked into production acceptance")
	}
}

func TestControlReadNoActionReceiptCompletesActualControlWithoutInventingResult(t *testing.T) {
	w, runtime, attempt, admission, request := noactionReadFixture(t)
	var unsettled *effects.AttemptSettlementHeldError
	closeErr := runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	if errors.Is(closeErr, codex.ErrObservationUnknown) {
		statePath := filepath.Join(w.brain, "factory/codex/sessions", attempt.SessionID, "attempts", attempt.AttemptID, "state.json")
		var observed codex.Record
		if err := store.ReadJSON(statePath, &observed); err != nil {
			t.Fatal(err)
		}
		if observed.AttemptID != attempt.AttemptID || observed.Stage != "sealing" || observed.Closure != nil || observed.Recovery == nil || observed.Recovery.Reason != "closure_unconfirmed" || observed.Recovery.Failures != 1 || !observed.Recovery.NextCheckAt.After(time.Now()) {
			t.Fatal("unexpected physical hold", observed.Recovery)
		}
		if alive, err := observed.Host.Alive(); err != nil || alive {
			t.Fatal("original writer not actually stopped", alive, err)
		}
		if err := runtime.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		var beforeDue codex.Record
		if err := store.ReadJSON(statePath, &beforeDue); err != nil || beforeDue.Revision != observed.Revision {
			t.Fatal("same attempt advanced before actual due", err)
		}
		if err := makeFixtureCodexRecoveryDue(context.Background(), w.brain, runtime, attempt, func() error {
			return runtime.Recover(context.Background())
		}); err != nil {
			t.Fatal("actual control-read hold did not become due fixture", err)
		}
		closeErr = runtime.Recover(context.Background())
		var afterDue codex.Record
		if err := store.ReadJSON(statePath, &afterDue); err != nil || afterDue.AttemptID != attempt.AttemptID || afterDue.Stage != "sealing" || !reflect.DeepEqual(afterDue.Seal, observed.Seal) || afterDue.Recovery == nil || afterDue.Recovery.Reason != "effects_unsettled" {
			t.Fatal("ordinary due recovery did not reach actual unsettled E1", afterDue.Recovery, err)
		}
	}
	if closeErr != nil && !errors.As(closeErr, &unsettled) {
		t.Fatal(closeErr)
	}
	seal, err := runtime.SealedAttempt(context.Background(), admission.Control.Receipt.Control.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	noAction, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, request, ReadIngressClosure{Original: admission.Control.Original, AttemptSeal: &seal}, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	proof, reply, err := codex.StoreControlReadNoActionProof(context.Background(), w.brain, admission.Control.Original, noAction)
	if err != nil {
		t.Fatal(err)
	}
	record, readReply, err := codex.ReadControlReadProof(context.Background(), w.brain, proof)
	if err != nil || record.NoAction == nil || *record.NoAction != noAction || record.Result != (codex.ProgramReadResultRef{}) || !record.Unavailable || !bytes.Equal(reply, readReply) {
		t.Fatalf("no-action proof invented result: %+v %s %v", record, readReply, err)
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: admission.Operation, RequestSHA256: admission.Control.Input.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
	if _, err := controlrecord.StoreOutcome(w.brain, controlrecord.OutcomeRecord{Schema: 2, Original: admission.Control.Original, Input: admission.Control.Input, Outcome: outcome, Reply: reply, ReplySHA256: sha256.Sum256(reply), Proof: proof}); err != nil {
		t.Fatal(err)
	}
	validator, err := codex.NewFactoryControlValidator(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := codex.NewAttemptJournalGate(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	service := effects.Service{Brain: w.brain, Controls: validator, AttemptGate: gate}
	for i := 0; i < 2; i++ {
		if _, err := service.CompleteControl(context.Background(), admission.Control.Receipt.Control, outcome); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := codex.StoreControlReadProof(context.Background(), w.brain, admission.Control.Original, codex.ProgramReadResultRef{}); err == nil {
		t.Fatal("missing result treated as factual read result")
	}
}

func (a outputFixtureAuthority) ValidateControlReadInputAbsentLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, ref codex.ProgramReadInputAbsentRef) error {
	return a.validateControlReadInputAbsentLocal(ctx, original, input, ref, b2Qualification)
}

func TestControlReadInputAbsenceCompleteRequiresActualRetainedSourceProjection(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, mutation := range []string{"valid", "canonical-admission", "read-target", "request-id"} {
		t.Run(mutation, func(t *testing.T) {
			fixture := actualInputPreSourceFixture(t, "c2-absence-complete-"+mutation)
			fixture.seal(t)
			absent, err := fixture.closeInput(t)
			if err != nil {
				t.Fatal(err)
			}
			original := fixture.request.Closure.Original
			if mutation != "valid" {
				record, err := codex.ReadProgramReadInputAbsentLocal(context.Background(), fixture.world.brain, absent)
				if err != nil {
					t.Fatal(err)
				}
				admission := fixture.admission
				switch mutation {
				case "canonical-admission":
					admission.Input.Number++
				case "read-target":
					admission.Input.Repo = "o/foreign"
				}
				record.Admission, err = json.Marshal(admission)
				if err != nil {
					t.Fatal(err)
				}
				record.AdmissionSHA256 = sha256.Sum256(record.Admission)
				record.RequestID = programReadRequestID(admission)
				if mutation == "request-id" {
					record.RequestID = effectDigest(struct{ Domain, Original string }{"wrong-read-domain", record.RequestID}).String()
				}
				guard, err := codex.AcquireProgramReadInputGuard(context.Background(), fixture.world.brain, record.RequestID)
				if err != nil {
					t.Fatal(err)
				}
				// Adversarial low-level storage proves the neutral codec is not the full C2 policy.
				absent, err = codex.PublishProgramReadInputAbsentLocked(context.Background(), fixture.world.brain, guard, record)
				guard.Close()
				if err != nil {
					t.Fatal("candidate was not neutral-reader-valid", err)
				}
				if _, err := codex.ReadProgramReadInputAbsentLocal(context.Background(), fixture.world.brain, absent); err != nil {
					t.Fatal(err)
				}
			}
			proof, reply, err := codex.StoreControlReadInputAbsentProof(context.Background(), fixture.world.brain, original, absent)
			if err != nil {
				t.Fatal(err)
			}
			input := fixture.admission.Control.Input
			outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: input.Operation, RequestSHA256: input.PayloadSHA256, ResponseSHA256: sha256.Sum256(reply)}}
			if _, err := controlrecord.StoreOutcome(fixture.world.brain, controlrecord.OutcomeRecord{Schema: 2, Original: original, Input: input, Outcome: outcome, Reply: reply, ReplySHA256: sha256.Sum256(reply), Proof: proof}); err != nil {
				t.Fatal(err)
			}
			validator, err := codex.NewFactoryControlValidator(fixture.world.brain)
			if err != nil {
				t.Fatal(err)
			}
			gate, err := codex.NewAttemptJournalGate(fixture.world.brain)
			if err != nil {
				t.Fatal(err)
			}
			service := effects.Service{Brain: fixture.world.brain, Controls: validator, AttemptGate: gate}
			if _, err := service.CompleteControl(context.Background(), fixture.admission.Control.Receipt.Control, outcome); !errors.Is(err, effects.ErrAuthority) {
				t.Fatalf("missing concrete source policy did not hold: %v", err)
			}
			validator.ReadEvidence = outputFixtureAuthority{NativeAuthority{fixture.world.brain}}
			tick, err := store.Lock(fixture.world.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := store.Lock(fixture.world.brain, "helper-ledger")
			if err != nil {
				tick()
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = service.CompleteControl(ctx, fixture.admission.Control.Receipt.Control, outcome)
			cancel()
			ledger()
			tick()
			if mutation == "valid" {
				if err != nil {
					t.Fatal("real retained source did not complete", err)
				}
			} else if !errors.Is(err, effects.ErrEvidence) {
				t.Fatalf("changed retained source completed: %v", err)
			}
		})
	}
}
