package helper

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
)

func TestControlAdmissionRecoversOnlySettledFreshFence(t *testing.T) {
	for _, state := range []string{"settled", "revoking", "unchanged_generation", "target_mismatch"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			s, d, req, a := setupStep(t)
			req.Component = "explorer"
			req.OutputPath = filepath.Join(a.scope.OutputRoot, "explore.v1.md")
			initial := admissionOriginal(t, s, "initial", "helper_submit", req)
			witness, err := captureAdmissionFixture(t, s, initial)
			if err != nil {
				t.Fatal(err)
			}
			initialWitness, err := ReadControlAdmissionWitnessLocal(s.Brain, witness)
			if err != nil {
				t.Fatal(err)
			}
			first := mustSubmit(t, s, req)
			if _, _, err := controlrecord.FindOutcome(s.Brain, initial); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old captured original unexpectedly has an outcome: %v", err)
			}
			fenceCalls := 0
			if state == "revoking" {
				if err := s.transaction(ctx, true, func(db *ledger) error {
					r := db.Jobs[first.JobID]
					r.Rounds[1].Phase = "launch_attempted"
					setState(r, "unknown", "launch_unconfirmed")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				s.FenceRuntime = func(context.Context, Job) error {
					fenceCalls++
					return errors.New("stop unconfirmed")
				}
			}
			err = s.Fence(ctx, req.OwnerID, "retire old owner")
			if state == "revoking" {
				if err == nil {
					t.Fatal("uncertain writer closed fence")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if state != "revoking" {
				if err := flush(ctx, d); err != nil {
					t.Fatal(err)
				}
			}
			jobsBeforeCapture, err := s.Jobs()
			if err != nil || len(jobsBeforeCapture) != 1 {
				t.Fatalf("invalid prior helper fixture: %+v %v", jobsBeforeCapture, err)
			}
			if state == "revoking" && fenceCalls != 1 {
				t.Fatal("fixture did not attempt physical fence")
			}
			beforeFenceCalls := fenceCalls
			before, err := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			if state != "unchanged_generation" {
				a.fakeAuthority.mu.Lock()
				a.scope.Generation = "renewed-generation"
				a.fakeAuthority.mu.Unlock()
			}
			req.RequestKey = "fresh-admission"
			req.OutputPath = filepath.Join(a.scope.OutputRoot, "explore.v2.md")
			ref := admissionOriginal(t, s, "fresh-admission", "helper_submit", req)
			if state == "target_mismatch" {
				var original controlrecord.OriginalRequest
				original, err = controlrecord.ReadOriginal(s.Brain, ref)
				if err != nil {
					t.Fatal(err)
				}
				source := initialWitness.Source
				source.Original, source.Control = ref, *original.Control
				source.Input.PayloadSHA256 = original.PayloadSHA256
				source.Input.Target.WorkID = "other-work"
				s.Authority = admissionFixtureAuthority{Authority: s.Authority, brain: s.Brain}
				_, err = s.CaptureControlAdmission(ctx, ref, source)
			} else {
				_, err = captureAdmissionFixture(t, s, ref)
			}
			if fenceCalls != beforeFenceCalls {
				t.Fatal("capture replayed a physical fence")
			}
			if state == "settled" {
				if err != nil {
					t.Fatalf("settled fresh owner cannot capture: %v", err)
				}
				next := mustSubmit(t, s, req)
				if next.JobID == first.JobID {
					t.Fatal("old generation helper adopted")
				}
				old, err := s.Job(first.JobID)
				if err != nil || old.State != "cancelled" || old.Scope.Generation == a.scope.Generation {
					t.Fatalf("old helper changed: %+v %v", old, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe fence admitted")
				}
				check := "scope_recovery"
				if state == "revoking" {
					check = "ledger"
				}
				if state == "target_mismatch" {
					check = "scope"
				}
				if ControlAdmissionSubReason(err) != check {
					t.Fatalf("wrong refusal check: %s %v", ControlAdmissionSubReason(err), err)
				}
				jobs, err := s.Jobs()
				if err != nil || len(jobs) != 1 || jobs[0].ID != first.JobID {
					t.Fatalf("refusal replayed or adopted helper: %+v %v", jobs, err)
				}
				if state == "revoking" && jobs[0].State != "unknown" {
					t.Fatalf("uncertain old helper settled without evidence: %+v", jobs[0])
				}
				after, err := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("refusal changed ledger: %v", err)
				}
			}
		})
	}
}
