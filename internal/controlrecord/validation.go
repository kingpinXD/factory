package controlrecord

import (
	"context"

	"github.com/kingpinXD/factory/internal/effects"
)

type outcomeValidationKey struct{}

// WithOutcomeValidation identifies a factual validation. It grants no authority.
func WithOutcomeValidation(ctx context.Context, id effects.ControlID) context.Context {
	return context.WithValue(ctx, outcomeValidationKey{}, id)
}

func OutcomeValidation(ctx context.Context) (effects.ControlID, bool) {
	id, ok := ctx.Value(outcomeValidationKey{}).(effects.ControlID)
	return id, ok
}
