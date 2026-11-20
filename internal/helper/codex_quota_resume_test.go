package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

func admissionSelection() executor.Selection {
	return executor.Selection{Executor: "codex", EpicID: "epic", Profile: executor.Profile{IdentityBinding: "account-binding"}}
}
func admissionSnapshot(now time.Time, primary, weekly float64) QuotaSnapshot {
	q := quota(now, primary)
	q.Provider = "codex"
	q.Windows[1].Used = weekly
	return q
}
func TestCodexAdmissionHourlyResume(t *testing.T) {
	for _, window := range []string{"primary", "weekly", "permission"} {
		t.Run(window, func(t *testing.T) {
			brain := t.TempDir()
			clock := &schedulingClock{at: time.Now().UTC()}
			selection := admissionSelection()
			var reads atomic.Int32
			reader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
				n := reads.Add(1)
				q := admissionSnapshot(clock.Now(), 10, 10)
				if n <= 2 {
					switch window {
					case "primary":
						q.Windows[0].Used = 100
					case "weekly":
						q.Windows[1].Used = 100
					case "permission":
						no := false
						q.OrdinaryUsageAllowed = &no
					}
				}
				return q, nil
			})
			check := func() CodexAdmission {
				t.Helper()
				got := ReadCodexAdmissionQuota(context.Background(), brain, selection, QuotaPolicy{80, 88, 15 * time.Minute}, reader, clock.Now)
				return got
			}
			if got := check(); got.Decision.State != "pause" {
				t.Fatal(got)
			}
			clock.Advance(59 * time.Minute)
			if got := check(); got.Decision.State != "pause" || reads.Load() != 1 {
				t.Fatalf("early probe or lost denial: %+v reads=%d", got, reads.Load())
			}
			clock.Advance(time.Minute)
			if got := check(); got.Decision.State != "pause" || reads.Load() != 2 {
				t.Fatalf("hourly probe missing: %+v reads=%d", got, reads.Load())
			}
			clock.Advance(time.Hour)
			if got := check(); got.Decision.State != "allowed" || reads.Load() != 3 {
				t.Fatalf("fresh reset not admitted: %+v reads=%d", got, reads.Load())
			}
			if got := check(); got.Decision.State != "allowed" || reads.Load() != 4 {
				t.Fatalf("positive permission cached: %+v reads=%d", got, reads.Load())
			}
		})
	}
}
func TestCodexAdmissionConcurrentAndRestart(t *testing.T) {
	brain := t.TempDir()
	clock := &schedulingClock{at: time.Now().UTC()}
	sel := admissionSelection()
	var reads atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	reader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
		reads.Add(1)
		close(started)
		<-release
		return admissionSnapshot(clock.Now(), 100, 10), nil
	})
	done := make(chan CodexAdmission, 1)
	go func() {
		done <- ReadCodexAdmissionQuota(context.Background(), brain, sel, QuotaPolicy{80, 88, time.Minute}, reader, clock.Now)
	}()
	select {
	case <-started:
	case got := <-done:
		t.Fatalf("read never started: %+v", got)
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := ReadCodexAdmissionQuota(context.Background(), brain, sel, QuotaPolicy{80, 88, time.Minute}, reader, clock.Now)
			if got.Decision.State == "allowed" {
				t.Error("concurrent probe admitted")
			}
		}()
	}
	wg.Wait()
	close(release)
	if got := <-done; got.Decision.State != "pause" {
		t.Fatal(got)
	}
	if reads.Load() != 1 {
		t.Fatal(reads.Load())
	}
	clock.Advance(59 * time.Minute)
	freshReader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
		t.Error("restart lost schedule")
		return QuotaSnapshot{}, nil
	})
	if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, QuotaPolicy{80, 88, time.Minute}, freshReader, clock.Now); got.Decision.State != "pause" {
		t.Fatal(got)
	}
}
func TestCodexAdmissionUnknownAndCorruptionRecover(t *testing.T) {
	for _, kind := range []string{"read_error", "wrong_account", "stale", "future", "reset", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			brain := t.TempDir()
			clock := &schedulingClock{at: time.Now().UTC()}
			sel := admissionSelection()
			policy := QuotaPolicy{80, 88, time.Minute}
			var reads atomic.Int32
			reader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
				n := reads.Add(1)
				q := admissionSnapshot(clock.Now(), 10, 10)
				if n == 1 {
					switch kind {
					case "read_error":
						return q, errors.New("unavailable")
					case "wrong_account":
						q.IdentityBinding = "foreign"
						q.ExpectedIdentity = "foreign"
					case "stale":
						q.ObservedAt = q.ObservedAt.Add(-2 * time.Minute)
					case "future":
						q.ObservedAt = q.ObservedAt.Add(time.Minute)
					case "reset":
						q.Windows[0].ResetsAt = clock.Now()
					}
				}
				return q, nil
			})
			if kind == "corrupt" {
				rp := codexQuotaPath(brain, sel, policy)
				if err := os.MkdirAll(filepath.Dir(rp), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(rp, []byte("{broken"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "unknown" {
				t.Fatalf("unknown input admitted: %+v", got)
			}
			before := reads.Load()
			clock.Advance(4 * time.Minute)
			if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "unknown" || reads.Load() != before {
				t.Fatalf("unknown retry not bounded %+v", got)
			}
			clock.Advance(time.Minute)
			if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "allowed" {
				t.Fatalf("unknown never recovered: %+v", got)
			}
		})
	}
}

func TestCodexAdmissionPendingClaimAndFutureStateRecover(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(map[bool]string{false: "crashed_pending_read", true: "future_state"}[future], func(t *testing.T) {
			brain := t.TempDir()
			clock := &schedulingClock{at: time.Now().UTC()}
			sel := admissionSelection()
			policy := QuotaPolicy{80, 88, time.Minute}
			at := clock.Now()
			if future {
				at = at.Add(24 * time.Hour)
			}
			wait := codexQuotaWait{Decision: QuotaDecision{"unknown", "usage_refresh_pending"}, CheckedAt: at, NextCheckAt: at.Add(5 * time.Minute)}
			rp := codexQuotaPath(brain, sel, policy)
			if err := os.MkdirAll(filepath.Dir(rp), 0700); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteJSON(rp, wait); err != nil {
				t.Fatal(err)
			}
			var reads atomic.Int32
			reader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
				reads.Add(1)
				return admissionSnapshot(clock.Now(), 10, 10), nil
			})
			if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "unknown" || reads.Load() != 0 {
				t.Fatalf("unresolved claim admitted: %+v", got)
			}
			clock.Advance(5 * time.Minute)
			if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "allowed" || reads.Load() != 1 {
				t.Fatalf("claim never retried: %+v", got)
			}
		})
	}
}
func TestCodexAdmissionSelectionAndPolicyDoNotSharePermission(t *testing.T) {
	brain := t.TempDir()
	clock := &schedulingClock{at: time.Now().UTC()}
	sel := admissionSelection()
	policy := QuotaPolicy{80, 88, time.Minute}
	var reads atomic.Int32
	reader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
		reads.Add(1)
		return admissionSnapshot(clock.Now(), 85, 10), nil
	})
	if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "hold" {
		t.Fatal(got)
	}
	policy.Hold = 90
	policy.Pause = 95
	if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "allowed" || reads.Load() != 2 {
		t.Fatalf("changed policy inherited hold: %+v", got)
	}
	sel.Profile.IdentityBinding = "other"
	if got := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, clock.Now); got.Decision.State != "unknown" || reads.Load() != 3 {
		t.Fatalf("changed account borrowed permission: %+v", got)
	}
}
func codexAdmissionReply(t *testing.T, m *quotaMonitor, j Job) QuotaDecision {
	t.Helper()
	var result QuotaDecision
	eventually(t, func() bool {
		result = m.admission(context.Background(), j)
		return m.admissionState.flight == nil && result.Reason != "usage_refresh_pending"
	})
	return result
}
func TestCodexHourlyAdmissionDoesNotThrottleActiveMonitor(t *testing.T) {
	s, d, req := setupCodex(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(a.JobID)
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	var reads, used atomic.Int32
	used.Store(85)
	d.Quotas["codex"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		reads.Add(1)
		return admissionSnapshot(clock.Now(), float64(used.Load()), 10), nil
	})
	m := newQuotaMonitor(d, QuotaPolicy{80, 88, 15 * time.Minute}, time.Minute, time.Minute)
	defer m.close()
	if got := codexAdmissionReply(t, m, j); got.State != "hold" {
		t.Fatal(got)
	}
	// Active work has its own fresh observations even during an admission hold.
	if got := quotaReply(t, context.Background(), m, j); got.State != "hold" {
		t.Fatal(got)
	}
	clock.Advance(time.Minute)
	used.Store(100)
	if got := quotaReply(t, context.Background(), m, j); got.State != "pause" {
		t.Fatalf("active pause throttled: %+v", got)
	}
	before := reads.Load()
	clock.Advance(20 * time.Minute)
	if got := m.admission(context.Background(), j); got.State != "hold" || reads.Load() != before {
		t.Fatalf("expired denial changed running semantics: %+v", got)
	}
	clock.Advance(39 * time.Minute)
	used.Store(10)
	if got := codexAdmissionReply(t, m, j); got.State != "allowed" {
		t.Fatalf("due fresh admission failed: %+v", got)
	}
	// A second dispatch must not borrow the first one's positive permission.
	used.Store(100)
	if got := codexAdmissionReply(t, m, j); got.State != "pause" {
		t.Fatalf("stale positive permission reused: %+v", got)
	}
}
func TestCodexQueuedRunnerResumesAfterHourlyCheck(t *testing.T) {
	s, d, req := setupCodex(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.RequestKey, req.Prompt = "queued-second", "second review"
	b, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	clock := &schedulingClock{at: time.Now().UTC()}
	d.Now = clock.Now
	var reads, used, runs atomic.Int32
	used.Store(100)
	d.Quotas["codex"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		reads.Add(1)
		return admissionSnapshot(clock.Now(), float64(used.Load()), 10), nil
	})
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		runs.Add(1)
		return recordCodexTurn(ctx, spec, k)
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	eventually(t, func() bool {
		j, e := s.Job(a.JobID)
		return e == nil && j.State == "queued" && j.HoldReason == "provider_denied_or_exhausted"
	})
	if reads.Load() != 1 || runs.Load() != 0 {
		t.Fatalf("held job launched reads=%d runs=%d", reads.Load(), runs.Load())
	}
	clock.Advance(59 * time.Minute)
	used.Store(10)
	j, _ := s.Job(a.JobID)
	if j.State != "queued" {
		t.Fatal(j.State)
	}
	clock.Advance(time.Minute)
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("due queued work did not resume")
	}
	for _, id := range []string{a.JobID, b.JobID} {
		result, err := s.Result(id, 1)
		if err != nil || result.State != "completed" || runs.Load() != 2 || reads.Load() != 3 {
			t.Fatalf("resume result=%+v reads=%d runs=%d err=%v", result, reads.Load(), runs.Load(), err)
		}
	}
}

func TestCodexAdmissionCompletedReadBelongsToQueuedJob(t *testing.T) {
	s, d, req := setupCodex(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.RequestKey = "second-job"
	req.Prompt = "second review"
	b, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ja, _ := s.Job(a.JobID)
	jb, _ := s.Job(b.JobID)
	m := newQuotaMonitor(d, QuotaPolicy{80, 88, 15 * time.Minute}, time.Minute, time.Minute)
	defer m.close()
	m.admission(context.Background(), ja)
	eventually(t, func() bool { return len(m.admissionState.flight) == 1 })
	// B may poll after A's read finishes; it must not discard A's result.
	m.admission(context.Background(), jb)
	if got := m.admission(context.Background(), ja); got.State != "allowed" {
		t.Fatalf("another job discarded ready admission: %+v", got)
	}
	if got := codexAdmissionReply(t, m, jb); got.State != "allowed" {
		t.Fatalf("second job never admitted: %+v", got)
	}
}

func TestCodexAdmissionBrokenLockIsNotPendingRead(t *testing.T) {
	brain := t.TempDir()
	if err := os.MkdirAll(filepath.Join(brain, "factory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "factory", ".lock"), []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	reader := usageFunc(func(context.Context) (QuotaSnapshot, error) {
		t.Fatal("broken lock admitted provider read")
		return QuotaSnapshot{}, nil
	})
	got := ReadCodexAdmissionQuota(context.Background(), brain, admissionSelection(), QuotaPolicy{80, 88, time.Minute}, reader, time.Now)
	if got.Decision.State != "unknown" || got.Decision.Reason != "usage_schedule_unwritable" {
		t.Fatalf("unavailable schedule mistaken for ordinary contention: %+v", got)
	}
}
