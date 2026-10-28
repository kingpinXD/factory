package helper

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type schedulingClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *schedulingClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *schedulingClock) Advance(d time.Duration) { c.mu.Lock(); c.at = c.at.Add(d); c.mu.Unlock() }
func probeCadence(t *testing.T, s Service, every, near time.Duration) {
	t.Helper()
	b, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	b.Values.Usage.ProbeEvery = blueprint.Duration(every)
	b.Values.Usage.ProbeEveryNearLimit = blueprint.Duration(near)
	data, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(s.Brain), data, 0600); err != nil {
		t.Fatal(err)
	}
}
func TestRunningBrokerRetriesTransientFenceAndInspectWithoutReplay(t *testing.T) {
	for _, failure := range []string{"fence", "inspect"} {
		t.Run(failure, func(t *testing.T) {
			s, d, req := setup(t)
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			var runs, fences, inspections, effects atomic.Int32
			var recoveryAuthorized, reset atomic.Bool
			identified := make(chan RunResult, 1)
			recovered := make(chan struct{})
			var notice sync.Once
			var saved RunResult
			d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
				used := 10.0
				if reset.Load() {
					used = 0
				}
				return quota(time.Now(), used), nil
			})
			d.Notify = func(_ context.Context, _ Job, r RoundResult) error {
				if r.State == "recovery_required" {
					notice.Do(func() { close(recovered) })
				}
				return nil
			}
			d.Backends["zed"] = backendFunc{
				run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
					runs.Add(1)
					if spec.Resume {
						if !recoveryAuthorized.Load() || spec.Prompt != "explicit recovery" || spec.Job.ConversationID != saved.ConversationID {
							t.Error("automatic replay or lost conversation")
						}
						return recordTurn(ctx, spec, k)
					}
					result, err := beginTurn(ctx, spec, k)
					if err != nil {
						return result, err
					}
					saved = result
					saved.Outcome = "interrupted"
					effects.Add(1)
					identified <- result
					<-ctx.Done()
					return RunResult{}, ctx.Err()
				},
				fence: func(context.Context, Job) error {
					if fences.Add(1) == 1 && failure == "fence" {
						return errors.New("temporary daemon failure")
					}
					return nil
				},
				inspect: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, bool, error) {
					n := inspections.Add(1)
					if n == 1 && failure == "inspect" {
						return RunResult{}, false, errors.New("temporary result read failure")
					}
					if spec.Job.ID != a.JobID || spec.Job.Round != 1 {
						t.Error("reconciled another attempt")
					}
					if err := k.Record(ctx, EventCheckpointSaved, map[string]string{"conversation_id": saved.ConversationID, "turn_id": saved.TurnID}); err != nil {
						return saved, false, err
					}
					return saved, true, nil
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			done := make(chan error, 1)
			go func() { defer close(done); done <- Run(ctx, d) }()
			defer func() { cancel(); <-done }()
			select {
			case <-identified:
			case <-ctx.Done():
				t.Fatal("turn did not start")
			}
			if _, err = s.Interrupt(context.Background(), a.JobID, 1, "operator"); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() bool { r, e := s.Result(a.JobID, 1); return e == nil && r.State == "unknown" })
			var locked *store.LockedError
			if err = Run(context.Background(), d); !errors.As(err, &locked) {
				t.Fatalf("original broker lost ownership: %v", err)
			}
			reset.Store(true)
			select {
			case <-recovered:
			case <-time.After(4 * time.Second):
				t.Fatalf("same broker never retried %s: fence=%d inspect=%d run=%d", failure, fences.Load(), inspections.Load(), runs.Load())
			}
			r, err := s.Result(a.JobID, 1)
			if err != nil || r.State != "recovery_required" || r.ConversationID != saved.ConversationID || runs.Load() != 1 || effects.Load() != 1 {
				t.Fatalf("%+v run=%d effects=%d err=%v", r, runs.Load(), effects.Load(), err)
			}
			recoveryAuthorized.Store(true)
			if _, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "recover", ExpectedRound: 1, Prompt: "explicit recovery"}); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("same broker did not finish continuation")
			}
			if runs.Load() != 2 || effects.Load() != 1 {
				t.Fatalf("run=%d effects=%d", runs.Load(), effects.Load())
			}
		})
	}
}

type schedulingAuthority struct {
	Authority
	checks atomic.Int32
}

func (a *schedulingAuthority) Check(ctx context.Context, j Job) error {
	a.checks.Add(1)
	return a.Authority.Check(ctx, j)
}
func TestFreshQuotaCacheIsReusedAcrossFastControlTicks(t *testing.T) {
	s, d, req := setup(t)
	probeCadence(t, s, time.Second, time.Second)
	authority := &schedulingAuthority{Authority: s.Authority}
	s.Authority = authority
	d.Service = s
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	var reads atomic.Int32
	started := make(chan struct{})
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) { reads.Add(1); return quota(clock.Now(), 10), nil })
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		r, err := beginTurn(ctx, spec, k)
		if err != nil {
			return r, err
		}
		close(started)
		<-ctx.Done()
		return RunResult{}, ctx.Err()
	}}
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	defer func() { cancel(); <-done }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("turn did not start")
	}
	baseline := authority.checks.Load()
	eventually(t, func() bool { return authority.checks.Load() >= baseline+4 })
	if reads.Load() != 1 {
		t.Fatalf("fresh cache launched %d usage reads during local controls", reads.Load())
	}
	clock.Advance(time.Second)
	eventually(t, func() bool { return reads.Load() == 2 })
	baseline = authority.checks.Load()
	eventually(t, func() bool { return authority.checks.Load() >= baseline+4 })
	if reads.Load() != 2 {
		t.Fatalf("refresh was not scheduled: reads=%d", reads.Load())
	}
}
func TestSlowQuotaRefreshDoesNotDelayInterruptOrAuthorityObservation(t *testing.T) {
	for _, control := range []string{"interrupt", "authority"} {
		t.Run(control, func(t *testing.T) {
			s, d, req := setup(t)
			probeCadence(t, s, time.Second, time.Second)
			clock := &schedulingClock{at: time.Now().UTC()}
			d.Now = clock.Now
			var reads atomic.Int32
			refreshStarted := make(chan struct{})
			release := make(chan struct{})
			cancelled := make(chan struct{})
			identified := make(chan RunResult, 1)
			d.Quotas["zed"] = usageFunc(func(ctx context.Context) (QuotaSnapshot, error) {
				if reads.Add(1) > 1 {
					close(refreshStarted)
					select {
					case <-release:
					case <-ctx.Done():
						return QuotaSnapshot{}, ctx.Err()
					}
				}
				return quota(clock.Now(), 10), nil
			})
			d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				r, err := beginTurn(ctx, spec, k)
				if err != nil {
					return r, err
				}
				identified <- r
				<-ctx.Done()
				close(cancelled)
				return RunResult{}, ctx.Err()
			}}
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			done := make(chan error, 1)
			go func() { done <- Run(ctx, d) }()
			defer func() { cancel(); close(release); <-done }()
			select {
			case <-identified:
			case <-ctx.Done():
				t.Fatal("turn did not start")
			}
			clock.Advance(time.Second)
			select {
			case <-refreshStarted:
			case <-ctx.Done():
				t.Fatal("scheduled refresh did not start")
			}
			if control == "interrupt" {
				if _, err = s.Interrupt(context.Background(), a.JobID, 1, "operator"); err != nil {
					t.Fatal(err)
				}
			} else {
				authority := s.Authority.(*fakeAuthority)
				authority.mu.Lock()
				authority.deny = true
				authority.mu.Unlock()
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("slow usage I/O blocked local control")
			}
			if reads.Load() != 2 {
				t.Fatalf("more than one refresh in flight: %d", reads.Load())
			}
		})
	}
}

func quotaReply(t *testing.T, ctx context.Context, m *quotaMonitor, j Job) QuotaDecision {
	t.Helper()
	var decision QuotaDecision
	eventually(t, func() bool {
		decision = m.decision(ctx, j)
		state := m.states[j.Route.Provider]
		return state != nil && state.flight == nil && !state.next.IsZero()
	})
	return decision
}
func TestRecoveryBackoffIsBoundedPerAttemptAndNeverRunsPrompt(t *testing.T) {
	s, d, req := setup(t)
	j, _ := preparedAttempt(t, s, req, true)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	fences, inspections := 0, 0
	d.Backends["zed"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) {
		t.Error("replayed prompt")
		return RunResult{}, nil
	}, fence: func(ctx context.Context, job Job) error {
		fences++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded fence")
		}
		if job.ID != j.ID || job.Generation != j.Generation {
			t.Error("different attempt")
		}
		return nil
	}, inspect: func(ctx context.Context, _ RunSpec, _ Sink) (RunResult, bool, error) {
		inspections++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded inspection")
		}
		return RunResult{}, false, nil
	}}
	if err := recoverAttempts(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	schedule := recoverySchedule{}
	defer schedule.close()
	if err := schedule.tick(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	for _, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		before := fences
		for i := 0; i < 20; i++ {
			if err := schedule.tick(context.Background(), d); err != nil {
				t.Fatal(err)
			}
		}
		clock.Advance(delay - time.Nanosecond)
		if err := schedule.tick(context.Background(), d); err != nil {
			t.Fatal(err)
		}
		if fences != before {
			t.Fatalf("retry before %s: %d -> %d", delay, before, fences)
		}
		clock.Advance(time.Nanosecond)
		if err := schedule.tick(context.Background(), d); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool {
			if err := schedule.tick(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			return schedule.flight == nil
		})
		if fences != before+1 || inspections != fences {
			t.Fatalf("retry at %s: fence=%d inspect=%d", delay, fences, inspections)
		}
	}
	result, err := s.Result(j.ID, 1)
	if err != nil || result.State != "unknown" {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestQuotaRefreshUsesConfiguredNormalAndNearIntervals(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(a.JobID)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	var reads atomic.Int32
	var used atomic.Int32
	used.Store(20)
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		reads.Add(1)
		return quota(clock.Now(), float64(used.Load())), nil
	})
	monitor := newQuotaMonitor(d, QuotaPolicy{80, 88, 15 * time.Minute}, 4*time.Minute, time.Minute)
	defer monitor.close()
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" {
		t.Fatal(got)
	}
	clock.Advance(4*time.Minute - time.Nanosecond)
	monitor.decision(context.Background(), j)
	if reads.Load() != 1 {
		t.Fatal("fresh cache ignored")
	}
	used.Store(80)
	clock.Advance(time.Nanosecond)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "hold" || reads.Load() != 2 {
		t.Fatalf("%+v reads=%d", got, reads.Load())
	}
	clock.Advance(time.Minute - time.Nanosecond)
	monitor.decision(context.Background(), j)
	if reads.Load() != 2 {
		t.Fatal("near-limit interval ignored")
	}
	used.Store(88)
	clock.Advance(time.Nanosecond)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "pause" || reads.Load() != 3 {
		t.Fatalf("%+v reads=%d", got, reads.Load())
	}
}
func TestQuotaCacheExpiresAtExistingStaleAndResetBoundaries(t *testing.T) {
	for _, boundary := range []string{"stale", "reset"} {
		t.Run(boundary, func(t *testing.T) {
			s, d, req := setup(t)
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			j, _ := s.Job(a.JobID)
			clock := &schedulingClock{at: time.Now().UTC()}
			d.Now = clock.Now
			initial := quota(clock.Now(), 10)
			policy := QuotaPolicy{80, 88, 10 * time.Second}
			interval := 10 * time.Second
			if boundary == "reset" {
				interval = 5 * time.Second
				initial.Windows[0].ResetsAt = clock.Now().Add(interval)
			}
			started, release := make(chan struct{}), make(chan struct{})
			var reads atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d.Quotas["zed"] = usageFunc(func(ctx context.Context) (QuotaSnapshot, error) {
				if reads.Add(1) == 1 {
					return initial, nil
				}
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return QuotaSnapshot{}, ctx.Err()
				}
				return quota(clock.Now(), 10), nil
			})
			monitor := newQuotaMonitor(d, policy, 4*time.Minute, time.Minute)
			defer monitor.close()
			if got := quotaReply(t, ctx, monitor, j); got.State != "allowed" {
				t.Fatal(got)
			}
			clock.Advance(interval)
			got := monitor.decision(ctx, j)
			if boundary == "stale" && got.State != "allowed" {
				t.Fatal("changed exact staleness threshold", got)
			}
			if boundary == "reset" && got.State != "unknown" {
				t.Fatal("reset guessed eligibility", got)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("boundary refresh not scheduled")
			}
			clock.Advance(time.Nanosecond)
			if got = monitor.decision(ctx, j); got.State != "unknown" {
				t.Fatal("expired snapshot authorized work", got)
			}
			close(release)
			if got = quotaReply(t, ctx, monitor, j); got.State != "allowed" {
				t.Fatal("fresh boundary evidence not admitted", got)
			}
		})
	}
}
func TestQuotaReadFailureUsesOnlyStillFreshEvidenceAndBacksOff(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(a.JobID)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	var calls atomic.Int32
	var unattributed atomic.Bool
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		if calls.Add(1) == 1 {
			return quota(clock.Now(), 10), nil
		}
		if unattributed.Load() {
			q := quota(clock.Now(), 10)
			q.AttributionKnown = false
			return q, nil
		}
		return QuotaSnapshot{}, errors.New("temporary usage failure")
	})
	monitor := newQuotaMonitor(d, QuotaPolicy{80, 88, 15 * time.Minute}, time.Second, time.Second)
	defer monitor.close()
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" {
		t.Fatal(got)
	}
	clock.Advance(time.Second)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" || calls.Load() != 2 {
		t.Fatalf("%+v calls=%d", got, calls.Load())
	}
	for i := 0; i < 20; i++ {
		monitor.decision(context.Background(), j)
	}
	if calls.Load() != 2 {
		t.Fatal("retry busy spin")
	}
	clock.Advance(time.Second)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" || calls.Load() != 3 {
		t.Fatalf("%+v calls=%d", got, calls.Load())
	}
	clock.Advance(2*time.Second - time.Nanosecond)
	monitor.decision(context.Background(), j)
	if calls.Load() != 3 {
		t.Fatal("read backoff ignored")
	}
	unattributed.Store(true)
	clock.Advance(time.Nanosecond)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "unknown" || got.Reason != "identity_or_attribution" {
		t.Fatal("inferred attribution from previous snapshot", got)
	}
}
func TestQuotaTimedOutReaderRemainsSingleFlightAndLateSuccessIsDiscarded(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(a.JobID)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	initial := clock.Now()
	var calls, active atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Quotas["zed"] = usageFunc(func(ctx context.Context) (QuotaSnapshot, error) {
		n := active.Add(1)
		defer active.Add(-1)
		if n != 1 {
			t.Error("concurrent provider usage reads")
		}
		call := calls.Add(1)
		if call == 2 {
			close(started)
			<-release
		} else if call > 2 {
			<-ctx.Done()
			return QuotaSnapshot{}, ctx.Err()
		}
		return quota(clock.Now(), 10), nil
	})
	monitor := newQuotaMonitor(d, QuotaPolicy{80, 88, 15 * time.Minute}, time.Second, time.Second)
	monitor.readTimeout = 200 * time.Millisecond
	defer monitor.close()
	if got := quotaReply(t, ctx, monitor, j); got.State != "allowed" {
		t.Fatal(got)
	}
	clock.Advance(time.Second)
	monitor.decision(ctx, j)
	<-started
	flight := monitor.states["zed"].flight
	select {
	case <-monitor.states["zed"].flightContext.Done():
	case <-time.After(time.Second):
		t.Fatal("usage read was not bounded")
	}
	monitor.decision(ctx, j)
	clock.Advance(20 * time.Minute)
	for i := 0; i < 100; i++ {
		if got := monitor.decision(ctx, j); got.State != "unknown" {
			t.Fatal("stale evidence authorized work", got)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("unresponsive reader spawned replacements", calls.Load())
	}
	close(release)
	eventually(t, func() bool { return len(flight) == 1 })
	if got := monitor.decision(ctx, j); got.State != "unknown" {
		t.Fatal("late timed-out response accepted", got)
	}
	if !monitor.states["zed"].snapshot.ObservedAt.Equal(initial) {
		t.Fatal("late response replaced trusted snapshot")
	}
}

func TestRecoveryIOIsSingleFlightAndLateFailureCannotReviveCancellation(t *testing.T) {
	s, d, req := setup(t)
	j, _ := preparedAttempt(t, s, req, true)
	if err := s.transaction(context.Background(), true, func(db *ledger) error { setState(db.Jobs[j.ID], "unknown", "unconfirmed"); return nil }); err != nil {
		t.Fatal(err)
	}
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	entered := make(chan struct{})
	var calls atomic.Int32
	d.Backends["zed"] = backendFunc{fence: func(ctx context.Context, _ Job) error { calls.Add(1); close(entered); <-ctx.Done(); return ctx.Err() }}
	schedule := recoverySchedule{}
	defer schedule.close()
	if err := schedule.tick(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := schedule.tick(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 20; i++ {
			if err := schedule.tick(context.Background(), d); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery I/O blocked fast scheduler ticks")
	}
	if calls.Load() != 1 {
		t.Fatal("overlapping recovery attempts", calls.Load())
	}
	s.FenceRuntime = func(context.Context, Job) error { return nil }
	if err := s.Cancel(context.Background(), j.ID, "explicit abandonment"); err != nil {
		t.Fatal(err)
	}
	schedule.close()
	eventually(t, func() bool {
		if err := schedule.tick(context.Background(), d); err != nil {
			t.Fatal(err)
		}
		return schedule.flight == nil
	})
	result, err := s.Result(j.ID, 1)
	if err != nil || result.State != "cancelled" {
		t.Fatalf("late failure revived cancelled attempt: %+v %v", result, err)
	}
}
func TestQuotaRefreshFloorNeverAssumesEligibilityAcrossReset(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(a.JobID)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	initial := quota(clock.Now(), 10)
	initial.Windows[0].ResetsAt = clock.Now().Add(10 * time.Millisecond)
	var calls atomic.Int32
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		if calls.Add(1) == 1 {
			return initial, nil
		}
		return quota(clock.Now(), 10), nil
	})
	monitor := newQuotaMonitor(d, QuotaPolicy{80, 88, 15 * time.Minute}, time.Nanosecond, time.Nanosecond)
	defer monitor.close()
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" {
		t.Fatal(got)
	}
	clock.Advance(10 * time.Millisecond)
	if got := monitor.decision(context.Background(), j); got.State != "unknown" {
		t.Fatal("reset assumed permission", got)
	}
	if calls.Load() != 1 {
		t.Fatal("subsecond provider probe", calls.Load())
	}
	clock.Advance(time.Second - 10*time.Millisecond)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" || calls.Load() != 2 {
		t.Fatalf("%+v calls=%d", got, calls.Load())
	}
}

func TestReadFailureCannotExtendSnapshotFreshness(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(a.JobID)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	initial := clock.Now()
	var calls atomic.Int32
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		if calls.Add(1) == 1 {
			return quota(clock.Now(), 10), nil
		}
		return QuotaSnapshot{}, errors.New("usage unavailable")
	})
	monitor := newQuotaMonitor(d, QuotaPolicy{80, 88, 10 * time.Second}, time.Second, time.Second)
	defer monitor.close()
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" {
		t.Fatal(got)
	}
	clock.Advance(time.Second)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "allowed" {
		t.Fatal("discarded still-fresh fallback", got)
	}
	clock.Advance(9 * time.Second)
	if got := monitor.decision(context.Background(), j); got.State != "allowed" {
		t.Fatal("changed exact staleness threshold", got)
	}
	clock.Advance(time.Nanosecond)
	if got := quotaReply(t, context.Background(), monitor, j); got.State != "unknown" || got.Reason != "stale_or_clock" {
		t.Fatal("failed refresh extended stale permission", got)
	}
	if !monitor.states["zed"].snapshot.ObservedAt.Equal(initial) {
		t.Fatal("failed refresh rewrote observation time")
	}
}
