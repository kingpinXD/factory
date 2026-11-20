package codex

import (
	"context"
	"testing"

	"github.com/kingpinXD/factory/internal/session"
)

type generationTestAuthority struct {
	*attemptTestAuthority
	allowed bool
	calls   int
}

func (a *generationTestAuthority) ValidateGenerationSuccessor(context.Context, AttemptRef, session.Spec) error {
	a.calls++
	if !a.allowed {
		return ErrAttemptConflict
	}
	return nil
}

func TestAttemptGenerationSuccessorRequiresTransferAuthority(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	authority := r.AttemptAuthority.(*attemptTestAuthority)
	old := preparedAttempt(t, r, spec)
	ctx := context.Background()
	if err := r.CloseAttempt(ctx, old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	spec.Generation += "-new"
	if _, err := r.SelectSuccessor(ctx, old, "new-generation", spec); err == nil {
		t.Fatal("generation changed without transfer capability")
	}
	transfer := &generationTestAuthority{attemptTestAuthority: authority}
	r.AttemptAuthority = transfer
	if _, err := r.SelectSuccessor(ctx, old, "new-generation", spec); err == nil || transfer.calls == 0 {
		t.Fatal("denied transfer selected successor", err)
	}
	transfer.allowed = true
	wrong := spec
	wrong.Cwd = t.TempDir()
	if _, err := r.SelectSuccessor(ctx, old, "new-generation", wrong); err == nil {
		t.Fatal("generation grant changed another logical field")
	}
	next, err := r.SelectSuccessor(ctx, old, "new-generation", spec)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := r.ValidateProcessRecovery(ctx, old, next, "new-generation")
	if err != nil {
		t.Fatal(err)
	}
	transfer.allowed = false
	if err := r.ValidateHistoricalProcessRecovery(ctx, proof); err == nil {
		t.Fatal("historical proof skipped transfer validation")
	}
}
