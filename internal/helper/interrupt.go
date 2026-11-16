package helper

import (
	"context"
	"fmt"
	"strings"
)

// Interrupt journals an operator request; it does not cancel/abandon the job or
// claim the writer stopped. Run (or recovery Inspect after a broker restart)
// confirms the saved conversation and every writer's death before publishing
// recovery_required. The caller must ensure the selected runner is started.
func (s Service) Interrupt(ctx context.Context, id string, expectedRound int, reason string) (Accepted, error) {
	var out Accepted
	if s.Authority == nil {
		return out, ErrAuthority
	}
	if !validToken(id) || expectedRound < 1 || strings.TrimSpace(reason) == "" || len(reason) > 512 || strings.ContainsAny(reason, "\r\n\x00") {
		return out, fmt.Errorf("invalid interrupt request")
	}
	err := s.transaction(ctx, true, func(db *ledger) error {
		r, err := lookup(db, id)
		if err != nil {
			return err
		}
		if err = s.check(ctx, db, r.Job); err != nil {
			return err
		}
		if r.Job.Round != expectedRound {
			return ErrConflict
		}
		q := r.Rounds[r.Job.Round]
		if q.InterruptReason != "" {
			out = accepted(r.Job)
			return nil
		}
		if (r.Job.State != "running" && r.Job.State != "yielding" && r.Job.State != "unknown") || q.Stopped || r.Job.ConversationID == "" || r.Job.TurnID == "" {
			return fmt.Errorf("interrupt requires an identified active turn")
		}
		q.InterruptReason = reason
		r.Job.HoldReason = "interrupt_requested"
		out = accepted(r.Job)
		return nil
	})
	return out, err
}
