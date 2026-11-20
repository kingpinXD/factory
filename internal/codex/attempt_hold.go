package codex

import (
	"context"
	"errors"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
)

type attemptRecoveryHeldError struct{ cause error }

func (e *attemptRecoveryHeldError) Error() string { return "Codex attempt recovery held" }
func (e *attemptRecoveryHeldError) Unwrap() error { return e.cause }

type attemptRecoveryHold struct {
	Reason      string    `json:"reason"`
	Failures    int       `json:"failures"`
	NextCheckAt time.Time `json:"next_check_at"`
}

func validAttemptHold(h *attemptRecoveryHold) bool {
	if h == nil {
		return true
	}
	if h.Failures < 1 || h.NextCheckAt.IsZero() {
		return false
	}
	switch h.Reason {
	case "effects_unsettled", "integration_unavailable", "closure_unconfirmed":
		return true
	}
	return false
}
func knownAttemptHold(err error) string {
	var settlement *effects.AttemptSettlementHeldError
	switch {
	case errors.As(err, &settlement):
		return "effects_unsettled"
	case errors.Is(err, ErrAttemptIntegration):
		return "integration_unavailable"
	case errors.Is(err, ErrObservationUnknown):
		return "closure_unconfirmed"
	}
	return ""
}
func (r *Runtime) recordAttemptHold(ref AttemptRef, reason string) error {
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if v.AttemptID != ref.AttemptID || v.Stage == attemptClosed {
		return ErrAttemptStale
	}
	_, err = r.mutateAttempt(ref, v.Revision, "recovery", func(n *Record) error {
		failures := 1
		if n.Recovery != nil {
			failures = n.Recovery.Failures + 1
		}
		minutes := 1
		for i := 1; i < failures && minutes < 60; i++ {
			minutes *= 2
		}
		if minutes > 60 {
			minutes = 60
		}
		n.Recovery = &attemptRecoveryHold{Reason: reason, Failures: failures, NextCheckAt: time.Now().UTC().Add(time.Duration(minutes) * time.Minute)}
		return nil
	})
	return err
}
func (r *Runtime) recoverAttempt(ctx context.Context, v Record) error {
	if v.Stage == attemptClosed || v.Recovery != nil && time.Now().Before(v.Recovery.NextCheckAt) {
		return nil
	}
	observed, err := r.ObserveCoordinatorRecovery(ctx, v.attemptRef())
	if errors.Is(err, ErrAttemptRecovery) {
		return nil
	}
	if err != nil {
		return err
	}
	err = r.CloseAttempt(ctx, v.attemptRef(), observed.Reason())
	if err == nil {
		return nil
	}
	var held *attemptRecoveryHeldError
	if errors.As(err, &held) {
		return nil
	}
	return err
}
