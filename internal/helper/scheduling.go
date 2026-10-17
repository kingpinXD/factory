package helper

import (
	"context"
	"errors"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

const (
	controlInterval          = 50 * time.Millisecond
	retryMinimum             = time.Second
	retryMaximum             = 30 * time.Second
	recoveryOperationTimeout = 10 * time.Second
	quotaReadTimeout         = 20 * time.Second
	defaultQuotaProbe        = 4 * time.Minute
	defaultQuotaNearProbe    = time.Minute
)

func nextBackoff(previous time.Duration) time.Duration {
	if previous < retryMinimum {
		return retryMinimum
	}
	if previous >= retryMaximum/2 {
		return retryMaximum
	}
	return previous * 2
}

type recoveryRetry struct {
	at    time.Time
	delay time.Duration
}
type recoveryAttemptKey struct {
	id         string
	generation int
}
type recoveryOutcome struct {
	key recoveryAttemptKey
	err error
}
type recoverySchedule struct {
	attempts map[recoveryAttemptKey]recoveryRetry
	flight   chan recoveryOutcome
	cancel   context.CancelFunc
}

func (r *recoverySchedule) close() {
	if r.cancel != nil {
		r.cancel()
	}
}

// Reconcile one due attempt at a time, without blocking fast local controls or
// another work item's active turn. This path only observes/fences existing
// attempts; it never submits a queued or recovery_required prompt.
func (r *recoverySchedule) tick(ctx context.Context, d RunnerDeps) error {
	if r.attempts == nil {
		r.attempts = map[recoveryAttemptKey]recoveryRetry{}
	}
	if r.flight != nil {
		select {
		case result := <-r.flight:
			r.cancel()
			r.cancel = nil
			r.flight = nil
			if result.err != nil && !errors.Is(result.err, ErrConflict) {
				return result.err
			}
			if retry, ok := r.attempts[result.key]; ok {
				retry.delay = nextBackoff(retry.delay)
				retry.at = d.Now().Add(retry.delay)
				r.attempts[result.key] = retry
			}
		default:
		}
	}
	jobs, err := d.Service.Jobs()
	if err != nil {
		return err
	}
	seen := map[recoveryAttemptKey]bool{}
	now := d.Now()
	var due *Job
	for _, j := range jobs {
		if j.State != "unknown" {
			continue
		}
		key := recoveryAttemptKey{j.ID, j.Generation}
		seen[key] = true
		retry, ok := r.attempts[key]
		if !ok {
			r.attempts[key] = recoveryRetry{now.Add(retryMinimum), retryMinimum}
			continue
		}
		if due == nil && !now.Before(retry.at) {
			copy := j
			due = &copy
		}
	}
	for key := range r.attempts {
		if !seen[key] {
			delete(r.attempts, key)
		}
	}
	if r.flight != nil || due == nil || ctx.Err() != nil {
		return nil
	}
	job := *due
	ch := make(chan recoveryOutcome, 1)
	recoveryCtx, cancel := context.WithCancel(ctx)
	r.flight = ch
	r.cancel = cancel
	go func() {
		err := recoverAttempt(recoveryCtx, d, job)
		ch <- recoveryOutcome{recoveryAttemptKey{job.ID, job.Generation}, err}
	}()
	return nil
}

type quotaRead struct {
	snapshot QuotaSnapshot
	err      error
}
type quotaState struct {
	snapshot      QuotaSnapshot
	loaded        bool
	next          time.Time
	retry         time.Duration
	flight        chan quotaRead
	flightContext context.Context
	cancel        context.CancelFunc
	timedOut      bool
}
type quotaMonitor struct {
	deps                     RunnerDeps
	policy                   QuotaPolicy
	every, near, readTimeout time.Duration
	states                   map[string]*quotaState
}

func newQuotaMonitor(d RunnerDeps, p QuotaPolicy, every, near time.Duration) *quotaMonitor {
	if d.Now == nil {
		d.Now = time.Now
	}
	if every <= 0 {
		every = defaultQuotaProbe
	}
	if near <= 0 {
		near = defaultQuotaNearProbe
	}
	every = max(every, retryMinimum)
	near = max(near, retryMinimum)
	return &quotaMonitor{deps: d, policy: p, every: every, near: near, readTimeout: quotaReadTimeout, states: map[string]*quotaState{}}
}
func (m *quotaMonitor) close() {
	for _, s := range m.states {
		if s.cancel != nil {
			s.cancel()
		}
	}
}
func quotaForJob(d RunnerDeps, j Job, s QuotaSnapshot, p QuotaPolicy, now time.Time) QuotaDecision {
	selection, err := executor.ReadSelection(d.Service.Brain, j.Scope.EpicID)
	if err != nil {
		return QuotaDecision{"unknown", "selection_unavailable"}
	}
	if s.Provider != j.Route.Provider {
		return QuotaDecision{"unknown", "provider_mismatch"}
	}
	if selection.Profile.IdentityBinding == "" || s.ExpectedIdentity != selection.Profile.IdentityBinding || s.IdentityBinding != selection.Profile.IdentityBinding {
		return QuotaDecision{"unknown", "pinned_identity_mismatch"}
	}
	return DecideQuota(s, p, now)
}
func (m *quotaMonitor) cached(j Job, s *quotaState, now time.Time) QuotaDecision {
	if !s.loaded {
		return QuotaDecision{"unknown", "usage_refresh_pending"}
	}
	return quotaForJob(m.deps, j, s.snapshot, m.policy, now)
}
func (m *quotaMonitor) failed(s *quotaState, now time.Time) {
	s.retry = nextBackoff(s.retry)
	s.next = now.Add(s.retry)
}
func (m *quotaMonitor) schedule(s *quotaState, decision QuotaDecision, now time.Time) {
	s.retry = 0
	interval := m.every
	if decision.State != "allowed" {
		interval = m.near
	}
	s.next = now.Add(interval)
	// A reset never supplies usage evidence. Refresh at reset/staleness boundaries,
	// while the unchanged cached snapshot still decides eligibility on every tick.
	boundary := s.snapshot.ObservedAt.Add(m.policy.StaleAfter)
	if boundary.After(now) && boundary.Before(s.next) {
		s.next = boundary
	}
	for _, w := range s.snapshot.Windows {
		if w.ResetsAt.After(now) && w.ResetsAt.Before(s.next) {
			s.next = w.ResetsAt
		}
	}
	if !boundary.After(now) || s.next.Before(now.Add(retryMinimum)) {
		s.next = now.Add(retryMinimum)
	}
}

// decision never waits for provider I/O. The sole owner is the broker goroutine;
// readers publish values through one buffered channel per provider. A reader
// which ignores cancellation keeps that one flight occupied rather than causing
// unbounded replacement goroutines. Its last snapshot still expires normally.
func (m *quotaMonitor) decision(ctx context.Context, j Job) QuotaDecision {
	provider := j.Route.Provider
	reader := m.deps.Quotas[provider]
	if reader == nil {
		return QuotaDecision{"unknown", "missing_provider_reader"}
	}
	now := m.deps.Now()
	s := m.states[provider]
	if s == nil {
		s = &quotaState{}
		m.states[provider] = s
	}
	if s.flight != nil {
		select {
		case result := <-s.flight:
			if s.flightContext.Err() != nil {
				result.err = s.flightContext.Err()
			}
			s.cancel()
			s.flight = nil
			s.flightContext = nil
			s.cancel = nil
			if result.err == nil {
				s.snapshot = result.snapshot
				s.loaded = true
			}
			decision := m.cached(j, s, now)
			if result.err != nil || decision.State == "unknown" {
				if !s.timedOut {
					m.failed(s, now)
				}
			} else {
				m.schedule(s, decision, now)
			}
			s.timedOut = false
		default:
			if s.flightContext.Err() != nil && !s.timedOut {
				s.timedOut = true
				m.failed(s, now)
			}
		}
	}
	decision := m.cached(j, s, now)
	if s.flight == nil && !now.Before(s.next) && ctx.Err() == nil {
		readCtx, cancel := context.WithTimeout(ctx, m.readTimeout)
		ch := make(chan quotaRead, 1)
		s.flight = ch
		s.flightContext = readCtx
		s.cancel = cancel
		s.timedOut = false
		go func() {
			snapshot, err := reader.Read(readCtx)
			if readCtx.Err() != nil {
				err = readCtx.Err()
			}
			ch <- quotaRead{snapshot, err}
		}()
	}
	return decision
}
