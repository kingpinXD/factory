package helper

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunnerRetainsCancellationCauseForBackendDiagnostics(t *testing.T) {
	for _, reason := range []string{"quota_interrupted", "authority_revoked"} {
		t.Run(reason, func(t *testing.T) {
			s, d, req := setup(t)
			d.Now = time.Now
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			j, err := s.Job(a.JobID)
			if err != nil {
				t.Fatal(err)
			}
			var paused atomic.Bool
			d.Quotas["codex"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
				used := 10.0
				if paused.Load() {
					used = 100
				}
				return quota(time.Now(), used), nil
			})
			observed := make(chan string, 1)
			d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				if _, err := beginTurn(ctx, spec, k); err != nil {
					return RunResult{}, err
				}
				if reason == "quota_interrupted" {
					paused.Store(true)
				} else {
					authority := s.Authority.(*fakeAuthority)
					authority.mu.Lock()
					authority.deny = true
					authority.mu.Unlock()
				}
				<-ctx.Done()
				cause := ""
				if context.Cause(ctx) != nil {
					cause = context.Cause(ctx).Error()
				}
				observed <- cause
				return RunResult{}, ctx.Err()
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			monitor := testExecutionQuota(t, ctx, d, j, QuotaPolicy{80, 88, 15 * time.Minute})
			if err := execute(ctx, d, j, monitor, nil); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-observed:
				if got != reason {
					t.Fatalf("runner lost cancellation cause: got %q want %q", got, reason)
				}
			default:
				t.Fatal("backend did not observe cancellation")
			}
		})
	}
}
