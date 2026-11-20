package helper

import "context"

func (s Service) ReadClosedControlOutputFence(ctx context.Context, scope Scope, token string) (ClosedScopeFenceRef, error) {
	var ref ClosedScopeFenceRef
	expected := scope.Authorization()
	if expected.CodexContract != 2 || !validRetainedFenceScope(expected) || !validToken(token) {
		return ref, ErrAuthority
	}
	err := s.transaction(ctx, false, func(db *ledger) error {
		closed, err := closedScopeFenceSnapshot(db, expected.WorkID)
		if err != nil {
			return err
		}
		if closed.Token != token || len(closed.Scopes) != 1 || closed.Scopes[scopeKey(expected)].Authorization() != expected {
			return ErrAuthority
		}
		pending, err := s.pendingControlOutputs(ctx, db, expected.WorkID)
		if err != nil {
			return err
		}
		if len(pending) != 0 {
			return &PendingControlOutputsError{WorkID: expected.WorkID, Token: token, Outputs: pending}
		}
		ref, err = s.publishClosedScopeFence(closed)
		return err
	})
	return ref, err
}
