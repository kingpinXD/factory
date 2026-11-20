package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
)

func TestControlAdmissionFailureNamesCheckAndPreservesCause(t *testing.T) {
	for _, check := range []string{"original", "ingress_source", "scope", "step", "output_path", "admission_artifact", "ledger"} {
		t.Run(check, func(t *testing.T) {
			s, _, req, a := setupStep(t)
			req.Component = "implementor"
			req.OutputPath = filepath.Join(a.scope.OutputRoot, "implement.v1.md")
			ref := admissionOriginal(t, s, "diagnostic", "helper_submit", req)
			witnessRef, err := captureAdmissionFixture(t, s, ref)
			if err != nil {
				t.Fatal(err)
			}
			w, err := ReadControlAdmissionWitnessLocal(s.Brain, witnessRef)
			if err != nil {
				t.Fatal(err)
			}
			s.Authority = admissionDiagnosticAuthority{admissionFixtureAuthority{Authority: a, brain: s.Brain}, check}
			switch check {
			case "original":
				ref.SHA256[0] ^= 1
			case "output_path":
				req.OutputPath = "relative.md"
				ref = admissionOriginal(t, s, "bad-output", "helper_submit", req)
				original, err := controlrecord.ReadOriginal(s.Brain, ref)
				if err != nil {
					t.Fatal(err)
				}
				w.Source.Original, w.Source.Control = ref, *original.Control
				w.Source.Input.PayloadSHA256 = original.PayloadSHA256
			case "admission_artifact":
				if err := os.WriteFile(witnessRef.Path, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "ledger":
				if err := os.WriteFile(s.root()+"/ledger.json", []byte(`{`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = s.CaptureControlAdmission(context.Background(), ref, w.Source)
			var named interface{ AdmissionSubReason() string }
			if !errors.As(err, &named) || named.AdmissionSubReason() != check {
				t.Fatalf("want check %s, got %v", check, err)
			}
			if check == "ingress_source" || check == "scope" || check == "step" {
				if !errors.Is(err, effects.ErrEvidence) {
					t.Fatalf("lost evidence cause: %v", err)
				}
			}
			if check == "output_path" && !errors.Is(err, ErrOutputPath) {
				t.Fatalf("lost output cause: %v", err)
			}
		})
	}
}

type admissionDiagnosticAuthority struct {
	admissionFixtureAuthority
	check string
}

func (a admissionDiagnosticAuthority) ValidateControlIngress(ctx context.Context, s ControlIngressSource) error {
	if a.check == "ingress_source" {
		return effects.ErrEvidence
	}
	return a.admissionFixtureAuthority.ValidateControlIngress(ctx, s)
}
func (a admissionDiagnosticAuthority) Resolve(ctx context.Context, r Request) (Scope, error) {
	if a.check == "scope" {
		return Scope{}, effects.ErrEvidence
	}
	return a.Authority.Resolve(ctx, r)
}
func (a admissionDiagnosticAuthority) ResolveStep(ctx context.Context, s Scope, c string) (StepBasis, error) {
	if a.check == "step" {
		return StepBasis{}, effects.ErrEvidence
	}
	return a.admissionFixtureAuthority.ResolveStep(ctx, s, c)
}

func TestControlAdmissionFailurePreservesTypedStepError(t *testing.T) {
	s, _, req, a := setupStep(t)
	a.stepErr = &PhaseMismatchError{Component: req.Component, ActualPhase: "waiting", RequiredPhase: "review"}
	_, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "typed-step", "helper_submit", req))
	var phase *PhaseMismatchError
	if !errors.As(err, &phase) || phase != a.stepErr || ControlAdmissionSubReason(err) != "step" {
		t.Fatalf("typed correction lost: %v", err)
	}
}
