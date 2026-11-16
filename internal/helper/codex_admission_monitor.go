package helper

import (
	"context"

	"github.com/kingpinXD/factory/internal/executor"
)

type codexAdmissionKey struct {
	id                string
	generation, round int
}

type codexAdmissionState struct {
	key    codexAdmissionKey
	ready  map[codexAdmissionKey]CodexAdmission
	flight chan CodexAdmission
	ctx    context.Context
	cancel context.CancelFunc
}

// admission keeps queued Codex reads separate from the active quota monitor.
// A single bounded flight preserves fast cancellation and authority controls.
func (m *quotaMonitor) admission(ctx context.Context, j Job) QuotaDecision {
	if j.Route.Provider != "codex" {
		return m.decision(ctx, j)
	}
	pending := QuotaDecision{"unknown", "usage_refresh_pending"}
	s := &m.admissionState
	now := m.deps.Now()
	key := codexAdmissionKey{j.ID, j.Generation, j.Round}
	if s.ready == nil {
		s.ready = map[codexAdmissionKey]CodexAdmission{}
	}
	for owner, result := range s.ready {
		expired := now.Sub(result.Snapshot.ObservedAt) > m.policy.StaleAfter
		if result.Decision.State != "allowed" {
			expired = !now.Before(result.NextCheckAt)
		}
		if expired {
			delete(s.ready, owner)
		}
	}
	if s.flight != nil {
		select {
		case result := <-s.flight:
			if s.ctx.Err() != nil {
				result = CodexAdmission{Decision: QuotaDecision{"unknown", "usage_read_failed"}, NextCheckAt: now.Add(codexUnknownRetry)}
			}
			s.cancel()
			s.cancel = nil
			s.ctx = nil
			s.flight = nil
			s.ready[s.key] = result
		default:
		}
	}
	if result, ok := s.ready[key]; ok {
		if result.Decision.State != "allowed" {
			return result.Decision
		}
		delete(s.ready, key)
		snapshot := result.Snapshot
		decision := quotaForJob(m.deps, j, snapshot, m.policy, now)
		if decision.State != "allowed" {
			return decision
		}
		active := &quotaState{snapshot: snapshot, loaded: true}
		m.schedule(active, decision, now)
		m.states[j.Route.Provider] = active
		return decision
	}
	if s.flight != nil {
		return pending
	}
	selection, err := executor.ReadSelection(m.deps.Service.Brain, j.Scope.EpicID)
	if err != nil {
		return QuotaDecision{"unknown", "selection_unavailable"}
	}
	reader := m.deps.Quotas["codex"]
	if reader == nil {
		return QuotaDecision{"unknown", "missing_provider_reader"}
	}
	if ctx.Err() != nil {
		return pending
	}
	s.key = key
	s.ctx, s.cancel = context.WithTimeout(ctx, m.readTimeout)
	s.flight = make(chan CodexAdmission, 1)
	readCtx, ch := s.ctx, s.flight
	go func() {
		ch <- ReadCodexAdmissionQuota(readCtx, m.deps.Service.Brain, selection, m.policy, reader, m.deps.Now)
	}()
	return pending
}
