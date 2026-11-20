package reconcile

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
)

func TestControlRecoverySettlesActualPreparedOutputWhileIndependentDomainHolds(t *testing.T) {
	w, _, attempt, ref, service := preparedRealCoordinatorOutput(t)
	prepared, _, err := service.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := NewControlOperationRecovery(p)
	if err != nil {
		t.Fatal(err)
	}
	recovery.helpers = service // Exact bounded B2 qualification fixture, actual domain policy otherwise.
	if err := recovery.RecoverSealedControls(context.Background(), effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}); !errors.Is(err, ErrCodexIntegration) {
		t.Fatalf("unimplemented independent read domain did not remain explicit: %v", err)
	}
	record, err := controlrecord.ReadOutcome(w.brain, prepared.Original)
	if err != nil || record.Outcome.Kind != effects.ControlFile {
		t.Fatalf("independent read hold stranded admitted local output: %+v %v", record, err)
	}
	old, err := os.Stat(prepared.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.RecoverSealedControls(context.Background(), effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}); !errors.Is(err, ErrCodexIntegration) {
		t.Fatal(err)
	}
	latest, err := os.Stat(prepared.Path)
	if err != nil || !os.SameFile(old, latest) {
		t.Fatal("concrete retry renamed admitted output again", err)
	}
}

func TestControlRecoveryClosesActualEnrollmentWithoutDomainReplay(t *testing.T) {
	w, service, req := enrolledOutputCut(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := NewControlOperationRecovery(p)
	if err != nil {
		t.Fatal(err)
	}
	recovery.helpers = service
	if err := recovery.RecoverSealedControls(context.Background(), req.AttemptSeal.Attempt); !errors.Is(err, ErrCodexIntegration) {
		t.Fatal("expected only independent read integration hold", err)
	}
	record, err := controlrecord.ReadOutcome(w.brain, req.Original)
	if err != nil || record.Outcome.Rejection == nil || record.Outcome.Rejection.Code != helper.ControlNotAdmittedCode {
		t.Fatalf("concrete recovery did not record truthful no-admission: %+v %v", record, err)
	}
	if _, err := service.FindControlOutput(context.Background(), req.Original); !errors.Is(err, helper.ErrNotFound) {
		t.Fatalf("concrete negative recovery prepared output: %v", err)
	}
}
