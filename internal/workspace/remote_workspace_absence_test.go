package workspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoteWorkspaceWriterAbsenceRefusesSurvivingContainerWithoutStoppingIt(t *testing.T) {
	w, owner, engine, _ := remoteFixture(t, true)
	if _, err := w.Prepare(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if err := w.ObserveWriterAbsence(t.Context(), owner); err == nil {
		t.Fatal("surviving container appeared absent")
	}
	stored, err := engine.read()
	if err != nil || stored == nil || !stored.View.State.Running {
		t.Fatalf("observation stopped container: %+v %v", stored, err)
	}
	if err := w.Fence(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if err := w.ObserveWriterAbsence(t.Context(), owner); err != nil {
		t.Fatalf("verified removed container refused: %v", err)
	}
	// A failed read stays unknown even if an earlier observation found absence.
	if err := os.WriteFile(engine.path(), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.ObserveWriterAbsence(t.Context(), owner); err == nil {
		t.Fatal("Docker lookup failure became absence")
	}
}

func TestRemoteWorkspaceWriterAbsenceAcceptsOnlyPositiveNoLaunchEvidence(t *testing.T) {
	w, owner, _, _ := remoteFixture(t, true)
	owner.Env = nil
	for _, phase := range []string{"prepared", "running", "fenced"} {
		if err := durable(filepath.Join(owner.RunDir, "containment.json"), remoteAttempt{SchemaVersion: 1, Owner: owner, Phase: phase, Fenced: phase == "fenced"}); err != nil {
			t.Fatal(err)
		}
		err := w.ObserveWriterAbsence(context.Background(), owner)
		if (err == nil) != (phase != "running") {
			t.Fatalf("phase %s: %v", phase, err)
		}
	}
	if err := os.Remove(filepath.Join(owner.RunDir, "containment.json")); err != nil {
		t.Fatal(err)
	}
	if err := w.ObserveWriterAbsence(t.Context(), owner); err == nil {
		t.Fatal("missing containment became no-launch evidence")
	}
}
