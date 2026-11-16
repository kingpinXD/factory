package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
)

func NewFactoryCodexRuntime(brain, factoryBin string) (*codex.Runtime, error) {
	programme, err := NewProgramEffects(brain, factoryBin)
	if err != nil {
		return nil, err
	}
	runtime := codex.New(programme.Brain, programme.FactoryBin)
	capabilities, ok := runtime.Capabilities.(codex.AttemptCapabilities)
	if !ok || capabilities == nil {
		return nil, codex.ErrAttemptIntegration
	}
	service := programme.Service()
	recovery, err := NewControlOperationRecovery(programme)
	if err != nil {
		return nil, err
	}
	runtime.AttemptCapabilities = capabilities
	runtime.Effects = &service
	runtime.ControlRecovery = recovery
	runtime.AttemptAuthority = &factoryAttemptAuthority{brain: programme.Brain, runtime: runtime, verify: codex.Contract2AcceptanceAuthority}
	return runtime, nil
}

type factoryAttemptAuthority struct {
	brain   string
	runtime *codex.Runtime
	verify  nativeAcceptance
}

// The manager already owns tick for preparation and launch.
func (a *factoryAttemptAuthority) ValidatePrepare(ctx context.Context, spec session.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a == nil || a.runtime == nil || a.verify == nil {
		return codex.ErrAttemptIntegration
	}
	if err := CoordinatorAuthority(a.brain, spec); err != nil {
		return err
	}
	owner, sess, _, err := coordinatorAuthorityEntities(a.brain, spec)
	if err != nil {
		return err
	}
	component := sess.session.Component
	if sess.session.Task != owner.id || spec.Generation != fmt.Sprintf("%s:%d:%s", owner.id, owner.st.Lease, component) || spec.Cwd != owner.entry.Dir || spec.PromptFile != agents.PromptPathFor(filepath.Join(a.brain, "factory", "executors", spec.EpicID), component) {
		return effects.ErrAuthority
	}
	selected, err := executor.ReadSelection(a.brain, spec.EpicID)
	if err != nil {
		return err
	}
	routes := selected.Routes[component]
	if len(routes) != 1 || routes[0].Provider != coordinatorProvider(selected) || spec.Model != routes[0].Model || spec.Effort != routes[0].Effort {
		return effects.ErrAuthority
	}
	_, _, err = nativeAuthority(ctx, a.brain, spec.EpicID, spec.Task, true, nil, a.verify)
	return err
}

func (a *factoryAttemptAuthority) WithClaimGuard(ctx context.Context, ref codex.AttemptRef, spec session.Spec, claim func(context.Context) error) error {
	if a == nil || a.runtime == nil {
		return codex.ErrAttemptIntegration
	}
	release, err := waitLock(ctx, a.brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	if err := a.runtime.ValidatePreparedCoordinatorAttempt(ctx, ref, spec); err != nil {
		return err
	}
	if err := a.ValidatePrepare(ctx, spec); err != nil {
		return err
	}
	return claim(ctx)
}

func (a *factoryAttemptAuthority) WithClosureGuard(ctx context.Context, ref codex.AttemptRef, close func(context.Context) error) error {
	if a == nil || a.runtime == nil || a.verify == nil {
		return codex.ErrAttemptIntegration
	}
	release, err := waitLock(ctx, a.brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	view, err := a.runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil {
		return err
	}
	if view.Ref != ref {
		return codex.ErrAttemptStale
	}
	if view.Stage != "sealing" {
		return fmt.Errorf("%w: automatic closure cause is not supplied", ErrCodexIntegration)
	}
	if _, err := a.runtime.SealedAttempt(ctx, effects.AttemptKey{SessionID: ref.SessionID, AttemptID: ref.AttemptID}); err != nil {
		return err
	}
	if _, _, err := nativeAuthority(ctx, a.brain, view.EpicID, view.OwnerID, false, nil, a.verify); err != nil {
		return err
	}
	return close(ctx)
}

var _ codex.AttemptAuthority = (*factoryAttemptAuthority)(nil)
var _ codex.AttemptClaimAuthority = (*factoryAttemptAuthority)(nil)

func (a *factoryAttemptAuthority) WithClosureCauseGuard(ctx context.Context, ref codex.AttemptRef, reason string, close func(context.Context, *effects.EventRef) error) error {
	if a == nil || a.runtime == nil || a.verify == nil {
		return codex.ErrAttemptIntegration
	}
	switch reason {
	case "host_failure", "unknown_submission", "prelaunch_failure":
	default:
		return fmt.Errorf("%w: automatic closure cause is not supplied", ErrCodexIntegration)
	}
	observed, err := a.runtime.ObserveCoordinatorRecovery(ctx, ref)
	if err != nil {
		return err
	}
	release, err := waitLock(ctx, a.brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	if err := a.runtime.ValidateCoordinatorRecoveryLocal(ctx, ref, reason, observed); err != nil {
		return err
	}
	view, err := a.runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil {
		return err
	}
	if !view.NextCheckAt.IsZero() && time.Now().Before(view.NextCheckAt) {
		return codex.ErrAttemptRecovery
	}
	if _, _, err := nativeAuthority(ctx, a.brain, view.EpicID, view.OwnerID, false, nil, a.verify); err != nil {
		return err
	}
	return close(ctx, nil)
}

var _ codex.AttemptCauseAuthority = (*factoryAttemptAuthority)(nil)
