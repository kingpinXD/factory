package helper

import (
	"context"
	"fmt"
)

// ReadCurrentRootPublishedRound reads the current broker binding, not admission.
func (s Service) ReadCurrentRootPublishedRound(ctx context.Context, scope Scope, step StepBasis) (PublishedRoundEvidence, error) {
	var out PublishedRoundEvidence
	scope = scope.Authorization()
	if scope.CodexContract != 2 || !validToken(scope.WorkID) || !validStep(step, step.Component) {
		return out, fmt.Errorf("invalid current root published round")
	}
	err := s.transaction(ctx, false, func(db *ledger) error {
		slot := stepSlot(scope, step.EntryRef, step.Component)
		id := db.DispatchSlots[slot]
		if id == "" {
			return fmt.Errorf("%w: current root dispatch", ErrNotFound)
		}
		r, err := lookup(db, id)
		if err != nil {
			return err
		}
		if r.Job.Request.ParentJobID != "" || r.Job.Scope.Authorization() != scope || r.Job.Request.Component != step.Component {
			return fmt.Errorf("%w: current root dispatch binding", ErrConflict)
		}
		q := r.Rounds[r.Job.Round]
		if q == nil || q.JobGeneration != r.Job.Generation || q.Step == nil || *q.Step != step || q.StepSlot != slot {
			return fmt.Errorf("%w: current root round binding", ErrConflict)
		}
		out, err = publishedRoundLocked(ctx, db, id, r.Job.Round)
		return err
	})
	return out, err
}
