package helper

import (
	"context"
	"errors"
	"testing"
)

func TestPublicCancelFenceFailurePreservesNativeRecovery(t *testing.T) {
	for _, kind := range []string{"startup", "prelaunch"} {
		t.Run(kind, func(t *testing.T) {
			attempt := prelaunchAttempt
			if kind == "startup" {
				attempt = startupAttempt
			}
			s, d, j, proof := attempt(t)
			ctx := context.Background()

			transient := errors.New("transient fence failure")
			publicFences := 0
			s.FenceRuntime = func(ctx context.Context, job Job) error {
				publicFences++
				// The public cancellation path, not the fixture, must persist fencing.
				current, err := s.Job(job.ID)
				if err != nil || current.State != "unknown" || current.HoldReason != "fencing" {
					t.Fatalf("Cancel did not persist fencing: %+v %v", current, err)
				}
				if err := s.transaction(ctx, true, func(db *ledger) error {
					db.Jobs[job.ID].Rounds[1].Result.ErrorText = "cancellation remains pending"
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				return transient
			}
			if err := s.Cancel(ctx, j.ID, "operator_cancelled"); !errors.Is(err, transient) {
				t.Fatalf("Cancel error: %v", err)
			}
			job, err := s.Job(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := s.Result(j.ID, 1)
			want := "fencing"
			if err != nil || job.State != "unknown" || job.HoldReason != want || result.State != "unknown" || result.ErrorCode != want || result.ErrorText != "cancellation remains pending" {
				t.Errorf("public Cancel lost pending state: job=%+v result=%+v err=%v", job, result, err)
			}
			if err := s.transaction(ctx, false, func(db *ledger) error {
				if db.Jobs[j.ID].Rounds[1].Stopped {
					t.Error("failed fence confirmed writer stopped")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			// Reconstruct the broker from the durable public Cancel result.
			d.Service = Service{Brain: s.Brain, Authority: s.Authority}
			fences, inspections := 0, 0
			d.Backends["zed"] = backendFunc{
				fence: func(context.Context, Job) error { fences++; return nil },
				inspect: func(ctx context.Context, _ RunSpec, sink Sink) (RunResult, bool, error) {
					inspections++
					if kind == "startup" {
						if err := sink.Record(ctx, EventProcessOwned, startupProcessEvidence()); err != nil {
							return RunResult{}, false, err
						}
					}
					return proof, true, nil
				},
			}
			if err := recoverAttempts(ctx, d); err != nil {
				t.Fatal(err)
			}
			s = Service{Brain: s.Brain, Authority: s.Authority}
			assertCancellationMarker(t, s, j.ID, "fencing")
			if publicFences != 1 || fences != 1 || inspections != 1 {
				t.Fatalf("missing mandatory operations: public=%d recovery=%d inspect=%d", publicFences, fences, inspections)
			}
			if err := s.transaction(ctx, false, func(db *ledger) error {
				if !db.Jobs[j.ID].Rounds[1].Stopped {
					t.Error("successful recovery fence not recorded")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Cancel(ctx, j.ID, "operator_cancelled"); err != nil {
				t.Fatal(err)
			}
			job, err = s.Job(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			result, err = s.Result(j.ID, 1)
			if err != nil || job.State != "cancelled" || job.HoldReason != "operator_cancelled" || result.State != "cancelled" || result.ErrorCode != "operator_cancelled" || result.ErrorText != "" {
				t.Fatalf("legitimate Cancel did not complete: job=%+v result=%+v err=%v", job, result, err)
			}
		})
	}
}
