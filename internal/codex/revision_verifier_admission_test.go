package codex

import (
	"context"
	"errors"
	"testing"

	"github.com/kingpinXD/factory/internal/helper"
)

func (a revisionBrokerAuthority) CheckVerifierAdmissionLocal(ctx context.Context, scope helper.Scope, step helper.StepBasis, rounds []helper.PublishedRoundEvidence) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope != scope.Authorization() || scope != a.scope.Authorization() || step.Component != "verifier" {
		return helper.ErrAuthority
	}
	actual, err := a.ResolveStep(ctx, scope, "verifier")
	if err != nil {
		return err
	}
	if actual != step {
		return helper.ErrAuthority
	}
	for _, round := range rounds {
		if round.Scope != scope || round.Round.Step != step {
			return helper.ErrAuthority
		}
	}
	return nil
}

func TestRevisionBrokerVerifierAdmissionFixtureRequiresExactTuple(t *testing.T) {
	for _, fault := range []string{"exact", "foreign-scope", "changed-step", "wrong-component", "round-scope", "round-step", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			b, attempt := revisionBackendFixture(t)
			a := revisionBrokerAuthority{brain: b.Brain, scope: attempt.Job.Scope}
			scope := a.scope.Authorization()
			step, err := a.ResolveStep(context.Background(), scope, "verifier")
			if err != nil {
				t.Fatal(err)
			}
			rounds := []helper.PublishedRoundEvidence{{Scope: scope, Round: helper.RevisionRoundRef{Step: step}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "foreign-scope":
				scope.Generation += "-foreign"
				rounds[0].Scope = scope
			case "changed-step":
				step.EntryRef += "-foreign"
				rounds[0].Round.Step = step
			case "wrong-component":
				step.Component = "implementor"
			case "round-scope":
				rounds[0].Scope.Generation += "-foreign"
			case "round-step":
				rounds[0].Round.Step.EntryRef += "-foreign"
			case "cancelled":
				cancel()
			}
			err = a.CheckVerifierAdmissionLocal(ctx, scope, step, rounds)
			if fault == "exact" {
				if err != nil {
					t.Fatal("exact private fixture tuple refused", err)
				}
			} else if fault == "cancelled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("cancelled private fixture accepted", err)
				}
			} else if !errors.Is(err, helper.ErrAuthority) {
				t.Fatal("foreign private fixture tuple accepted", err)
			}
		})
	}
}
