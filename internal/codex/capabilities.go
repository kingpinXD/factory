package codex

import (
	"context"
	"errors"

	"github.com/kingpinXD/factory/internal/executor"
)

var ErrAcceptancePending = errors.New("Codex live containment acceptance is pending")

// CapabilityVerifier binds admission to evidence for the pinned runtime. The
// default remains closed until the model-backed containment, cancellation,
// continuation and compaction checks have actually completed.
type CapabilityVerifier interface {
	Verify(context.Context, executor.Selection) error
}

func CheckCapabilities(ctx context.Context, brain string, selection executor.Selection) error {
	return NewAcceptanceVerifier(brain).Verify(ctx, selection)
}
