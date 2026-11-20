package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAttemptManagerReadsActualClosureAndSuccessor(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	view, err := r.InspectCoordinatorAttempt(context.Background(), old.SessionID)
	if err != nil || view.Ref != old || view.Stage != "prepared" || view.OwnerID != spec.Task || view.Generation != spec.Generation || view.ClosureSHA256 != "" {
		t.Fatalf("prepared view: %+v %v", view, err)
	}
	if err = r.ValidateAttemptClosure(context.Background(), AttemptClosureRef{Attempt: old, ClosureSHA256: old.AttemptID}); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("prepared certified closed: %v", err)
	}
	if err = r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	view, err = r.InspectCoordinatorAttempt(context.Background(), old.SessionID)
	if err != nil || view.Stage != "closed" || view.ClosureSHA256 == "" {
		t.Fatalf("closed view: %+v %v", view, err)
	}
	closed := AttemptClosureRef{Attempt: old, ClosureSHA256: view.ClosureSHA256}
	if err = r.ValidateAttemptClosure(context.Background(), closed); err != nil {
		t.Fatal(err)
	}
	next, err := r.SelectSuccessor(context.Background(), old, "actual-recovery", spec)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := r.ValidateProcessRecovery(context.Background(), old, next, "actual-recovery")
	if err != nil || proof.Previous != old || proof.Successor != next || proof.ClosureSHA256 != closed.ClosureSHA256 {
		t.Fatalf("recovery: %+v %v", proof, err)
	}
	view, err = r.InspectCoordinatorAttempt(context.Background(), old.SessionID)
	if err != nil || view.Ref != next || view.Predecessor != old || view.RecoveryKey != "actual-recovery" {
		t.Fatalf("successor view: %+v %v", view, err)
	}
	if _, err = r.ValidateProcessRecovery(context.Background(), old, next, "foreign"); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("foreign key: %v", err)
	}
	closed.ClosureSHA256 = old.AttemptID
	if err = r.ValidateAttemptClosure(context.Background(), closed); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("foreign closure: %v", err)
	}
	if err = r.CloseAttempt(context.Background(), next, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	newer, err := r.SelectSuccessor(context.Background(), next, "second-recovery", spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.ValidateProcessRecovery(context.Background(), old, next, "actual-recovery"); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("old successor accepted current: %v", err)
	}
	if _, err = r.ValidateProcessRecovery(context.Background(), next, newer, "second-recovery"); err != nil {
		t.Fatal(err)
	}
	if err = r.ValidateHistoricalProcessRecovery(context.Background(), proof); err != nil {
		t.Fatalf("genuine historical recovery lost: %v", err)
	}
	proof.Key = "foreign"
	if err = r.ValidateHistoricalProcessRecovery(context.Background(), proof); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("foreign history: %v", err)
	}
}

func TestAttemptManagerMissingReadCreatesNothing(t *testing.T) {
	brain := t.TempDir()
	r := New(brain, "")
	if _, err := r.InspectCoordinatorAttempt(context.Background(), hash([]byte("missing"))); !os.IsNotExist(err) {
		t.Fatalf("missing view: %v", err)
	}
	if _, err := os.Stat(filepath.Join(brain, "factory")); !os.IsNotExist(err) {
		t.Fatalf("read created state: %v", err)
	}
}
