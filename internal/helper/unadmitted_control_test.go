package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// This fixed seal isolates helper-ledger predicates; reconcile tests stop real A1 attempts.
type sealedAdmissionFixture struct {
	admissionFixtureAuthority
	seal effects.AttemptSeal
}

func (a sealedAdmissionFixture) SealedAttempt(_ context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
	if key != a.seal.Attempt {
		return effects.AttemptSeal{}, ErrConflict
	}
	return a.seal, nil
}
func admissionEnrollmentFixture(t *testing.T, s Service, witness controlrecord.ImmutableRef) (controlrecord.ImmutableRef, effects.AttemptSeal) {
	t.Helper()
	w, err := ReadControlAdmissionWitnessLocal(s.Brain, witness)
	if err != nil {
		t.Fatal(err)
	}
	// This domain-only receipt does not qualify an E1 producer or A1 closure.
	receipt := effects.ControlReceiptRef{Control: w.Source.Control, Sequence: 1, Revision: 1, SHA256: effects.Digest{1}}
	ref, err := controlrecord.StoreEnrollmentSnapshot(s.Brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: w.Original, Source: witness, Receipt: receipt, Input: w.Source.Input})
	if err != nil {
		t.Fatal(err)
	}
	return ref, effects.AttemptSeal{Attempt: w.Source.Control.Attempt, Revision: 2, SHA256: effects.Digest{2}}
}
func unadmittedBrokerFixture(t *testing.T, op string) (Service, UnadmittedControlRequest, *stepTestAuthority) {
	t.Helper()
	s, d, req, a := setupStep(t)
	var input any = req
	if op != "helper_submit" {
		first := mustSubmit(t, s, req)
		d.Backends["codex"] = backendFunc{run: recordCodexTurn}
		runBounded(t, d)
		a.step(StepBasis{EntryRef: "work1#8", ContinuationOf: "work1#1"})
		if op == "helper_continue" {
			input = toolcontract.ContinueInput{JobID: first.JobID, Request: ContinueRequest{SchemaVersion: 1, RequestKey: "negative-race-next", ExpectedRound: 1, Prompt: "actual next round", AcknowledgedChildren: []string{}}}
		} else {
			input = toolcontract.CancelInput{JobID: first.JobID, Request: toolcontract.CancelRequest{Reason: "stop"}}
		}
	}
	original := admissionOriginal(t, s, "no-admission-"+op, op, input)
	witness, err := captureAdmissionFixture(t, s, original)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, seal := admissionEnrollmentFixture(t, s, witness)
	s.Authority = sealedAdmissionFixture{admissionFixtureAuthority: admissionFixtureAuthority{Authority: s.Authority, brain: s.Brain}, seal: seal}
	return s, UnadmittedControlRequest{Original: original, Witness: witness, Enrollment: enrollment, AttemptSeal: &seal}, a
}
func TestUnadmittedBrokerChecksCompleteLedgerAndPermanentlyClosesExactOriginal(t *testing.T) {
	for _, op := range []string{"helper_submit", "helper_continue", "helper_cancel"} {
		t.Run(op, func(t *testing.T) {
			s, req, _ := unadmittedBrokerFixture(t, op)
			ref, err := s.CloseUnadmittedControlLocked(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := ReadUnadmittedControlLocal(context.Background(), s.Brain, ref, s.Authority.(effects.AttemptGate))
			if err != nil || proof.Code != ControlNotAdmittedCode {
				t.Fatalf("wrong domain proof: %+v %v", proof, err)
			}
			var admissionErr error
			switch op {
			case "helper_submit":
				_, admissionErr = s.SubmitControl(context.Background(), req.Witness, req.Enrollment)
			case "helper_continue":
				_, admissionErr = s.ContinueControl(context.Background(), req.Witness, req.Enrollment)
			case "helper_cancel":
				_, _, admissionErr = s.AdmitControlCancellation(context.Background(), req.Witness)
			}
			if !errors.Is(admissionErr, ErrControlAdmissionClosed) {
				t.Fatalf("closed original admitted again: %v", admissionErr)
			}
			again, err := s.CloseUnadmittedControlLocked(context.Background(), req)
			if err != nil || again != ref {
				t.Fatalf("negative receipt replay changed: %v", err)
			}
			releases := []func() error{}
			for _, lock := range []string{"tick", "helper-ledger", "effects"} {
				release, err := store.Lock(s.Brain, lock)
				if err != nil {
					t.Fatal(err)
				}
				releases = append(releases, release)
			}
			_, err = ReadUnadmittedControlLocal(context.Background(), s.Brain, ref, s.Authority.(effects.AttemptGate))
			for _, release := range releases {
				if err := release(); err != nil {
					t.Fatal(err)
				}
			}
			if err != nil {
				t.Fatal("local reader reentered a programme lock", err)
			}
		})
	}
}
func TestUnadmittedBrokerExistingFactsAlwaysTakePositivePath(t *testing.T) {
	for _, op := range []string{"helper_submit", "helper_continue", "helper_cancel"} {
		t.Run(op, func(t *testing.T) {
			s, req, _ := unadmittedBrokerFixture(t, op)
			w, err := ReadControlAdmissionWitnessLocal(s.Brain, req.Witness)
			if err != nil {
				t.Fatal(err)
			}
			switch op {
			case "helper_submit":
				_, err = s.SubmitControl(context.Background(), req.Witness, req.Enrollment)
			case "helper_continue":
				_, err = s.ContinueControl(context.Background(), req.Witness, req.Enrollment)
			case "helper_cancel":
				_, _, err = s.AdmitControlCancellation(context.Background(), req.Witness)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.CloseUnadmittedControlLocked(context.Background(), req); !errors.Is(err, ErrControlAlreadyAdmitted) {
				t.Fatalf("actual %s fact allowed a negative proof: %v", w.Operation, err)
			}
			if _, _, err := s.LookupControlAdmission(context.Background(), req.Witness); err != nil {
				t.Fatal("actual positive receipt lost", err)
			}
			if _, err := os.Stat(unadmittedControlName(s.Brain, req.Original)); !os.IsNotExist(err) {
				t.Fatalf("positive fact published a negative receipt: %v", err)
			}
		})
	}
}
func TestUnadmittedBrokerRequestKeyConflictAndOccupiedRootSuppressNegative(t *testing.T) {
	for _, kind := range []string{"key-conflict", "root-slot"} {
		t.Run(kind, func(t *testing.T) {
			s, req, _ := unadmittedBrokerFixture(t, "helper_submit")
			w, err := ReadControlAdmissionWitnessLocal(s.Brain, req.Witness)
			if err != nil {
				t.Fatal(err)
			}
			other := *w.Submit
			other.Prompt += " unrelated changed payload"
			if kind == "root-slot" {
				other.RequestKey += "-other"
			}
			if _, err := s.Submit(context.Background(), other); err != nil {
				t.Fatal(err)
			}
			_, err = s.CloseUnadmittedControlLocked(context.Background(), req)
			want := ErrConflict
			if kind == "root-slot" {
				want = ErrControlAlreadyAdmitted
			}
			if !errors.Is(err, want) {
				t.Fatalf("wrong %s precedence: %v", kind, err)
			}
		})
	}
}
func TestUnadmittedBrokerMissingOrCorruptLedgerNeverMeansAbsence(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			s, req, _ := unadmittedBrokerFixture(t, "helper_submit")
			name := filepath.Join(s.root(), "ledger.json")
			if kind == "missing" {
				if err := os.Rename(name, name+".retained"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(name, []byte(`{`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.CloseUnadmittedControlLocked(context.Background(), req); err == nil {
				t.Fatal("unknown ledger became negative evidence")
			}
		})
	}
}
func TestUnadmittedBrokerRacesActualContinueWithoutDoubleDisposition(t *testing.T) {
	for n := 0; n < 8; n++ {
		s, req, _ := unadmittedBrokerFixture(t, "helper_continue")
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var admissionErr, negativeErr error
		go func() {
			defer wg.Done()
			<-start
			_, admissionErr = s.ContinueControl(context.Background(), req.Witness, req.Enrollment)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, negativeErr = s.CloseUnadmittedControlLocked(context.Background(), req)
		}()
		close(start)
		wg.Wait()
		admitted := admissionErr == nil && errors.Is(negativeErr, ErrControlAlreadyAdmitted)
		closed := negativeErr == nil && errors.Is(admissionErr, ErrControlAdmissionClosed)
		if !admitted && !closed {
			t.Fatalf("race had ambiguous dispositions: admission=%v negative=%v", admissionErr, negativeErr)
		}
		if err := s.transaction(context.Background(), false, func(db *ledger) error {
			w, err := ReadControlAdmissionWitnessLocal(s.Brain, req.Witness)
			if err != nil {
				return err
			}
			job := db.Jobs[w.Continue.JobID]
			if admitted && (job.Job.Round != 2 || len(job.ContinueKeys) != 1) || closed && (job.Job.Round != 1 || len(job.ContinueKeys) != 0) {
				t.Fatal("race replayed or lost the actual continuation")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
