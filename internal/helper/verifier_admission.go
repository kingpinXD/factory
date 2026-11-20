package helper

import (
	"context"
	"sort"
)

type VerifierAdmissionAuthority interface {
	CheckVerifierAdmissionLocal(context.Context, Scope, StepBasis, []PublishedRoundEvidence) error
}

// The caller owns the helper transaction. The authority receives the complete
// current-root projection and must not open the helper ledger again.
func (s Service) checkVerifierAdmissionLocked(ctx context.Context, db *ledger, scope Scope, step *StepBasis, component, parent string) error {
	if scope.CodexContract != 2 || component != "verifier" || parent != "" {
		return nil
	}
	authority, ok := s.Authority.(VerifierAdmissionAuthority)
	if !ok || step == nil || !validStep(*step, component) {
		return ErrAuthority
	}
	ids := make([]string, 0, len(db.Jobs))
	for id := range db.Jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var rounds []PublishedRoundEvidence
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		r := db.Jobs[id]
		if r.Job.Scope.Authorization() != scope.Authorization() || r.Job.Request.Component != component || r.Job.Request.ParentJobID != "" {
			continue
		}
		q := r.Rounds[r.Job.Round]
		if q == nil || q.Step == nil || !validStep(*q.Step, component) {
			return ErrAuthority
		}
		if q.Step.EntryRef != step.EntryRef {
			continue
		}
		if *q.Step != *step {
			return ErrAuthority
		}
		if q.Result.State != "completed" {
			continue
		}
		round, err := publishedRoundLocked(ctx, db, id, r.Job.Round)
		if err != nil {
			return err
		}
		rounds = append(rounds, round)
	}
	return authority.CheckVerifierAdmissionLocal(ctx, scope.Authorization(), *step, rounds)
}
