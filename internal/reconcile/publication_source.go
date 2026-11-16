package reconcile

import (
	"context"
	"fmt"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
)

// Called inside the existing helper transaction. This validates retained local
// source facts; ordinary authority and cancellation checks remain the caller's.
func (a NativeAuthority) ValidateRevisionInputFailureLocal(ctx context.Context, job helper.Job, ref helper.RevisionInputFailureRef) error {
	var sha effects.Digest
	if err := sha.UnmarshalText([]byte(ref.SHA256)); err != nil {
		return err
	}
	failure, err := codex.ReadRevisionInputFailureLocal(ctx, a.Brain, codex.ProgramArtifact{Path: ref.Path, SHA256: sha})
	if err != nil {
		return err
	}
	actual := failure.Job
	if actual.ID != job.ID || actual.Generation != job.Generation || actual.Round != job.Round || actual.Scope != job.Scope || actual.Request != job.Request || actual.Route != job.Route || job.Scope.CodexContract != 2 || job.Request.Component != "verifier" || job.Route.Provider != "codex" {
		return fmt.Errorf("revision input failure differs from actual helper job")
	}
	return nil
}

var _ helper.RevisionInputFailureAuthority = NativeAuthority{}
