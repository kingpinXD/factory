package helper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/store"
)

func prelaunchAttempt(t *testing.T) (Service, RunnerDeps, Job, RunResult) {
	t.Helper()
	s, d, req := setup(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.transaction(context.Background(), true, func(db *ledger) error {
		r := db.Jobs[accepted.JobID]
		r.Rounds[1].Phase = "launch_attempted"
		setState(r, "running", "")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	j, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	result := RunResult{Outcome: "failed", ErrorCode: "native_prelaunch_failed", ErrorText: "explicit container GitHub authentication required"}
	return s, d, j, result
}

func TestNativePrelaunchSettlementRequiresFencedInspection(t *testing.T) {
	for _, kind := range []string{"valid", "prior-conversation", "claude", "run-only", "unfenced", "run-error", "process", "phase", "conflict", "model", "turn", "conversation", "outcome", "error-code", "empty-diagnostic", "interrupt", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			s, _, j, result := prelaunchAttempt(t)
			inspection := true
			var runErr, fenceErr error
			switch kind {
			case "claude":
				j.Route.Provider = "claude"
			case "run-only":
				inspection = false
			case "unfenced":
				fenceErr = errors.New("writer still uncertain")
			case "run-error":
				runErr = errors.New("unconfirmed transport")
			case "model":
				result.ObservedModel = j.Route.Model
			case "turn":
				result.TurnID = "invented"
			case "conversation":
				result.ConversationID = "invented"
			case "outcome":
				result.Outcome = "completed"
			case "error-code":
				result.ErrorCode = "native_failed"
			case "empty-diagnostic":
				result.ErrorText = ""
			case "revoked":
				s.Authority.(*fakeAuthority).deny = true
			}
			if err := s.transaction(context.Background(), true, func(db *ledger) error {
				r := db.Jobs[j.ID]
				q := r.Rounds[1]
				switch kind {
				case "prior-conversation":
					r.Job.ConversationID = "previous-conversation"
				case "process":
					q.Evidence[EventProcessOwned+":recorded"] = "true"
				case "phase":
					q.Phase = EventProcessOwned
				case "conflict":
					q.Conflicted = true
				case "interrupt":
					q.InterruptReason = "operator"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := finish(context.Background(), s, j, result, runErr, fenceErr, "", inspection); err != nil {
				t.Fatal(err)
			}
			stored, err := s.Result(j.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			want := "unknown"
			if kind == "valid" || kind == "prior-conversation" {
				want = "failed"
			}
			if kind == "revoked" {
				want = "interrupted"
			}
			if stored.State != want {
				t.Fatalf("state=%s want=%s result=%+v", stored.State, want, stored)
			}
			if want == "failed" && (stored.ErrorCode != "native_prelaunch_failed" || stored.ErrorText != "explicit container GitHub authentication required" || stored.ObservedModel != "" || stored.TurnID != "" || stored.ConversationID != "") {
				t.Fatalf("wrong prelaunch failure: %+v", stored)
			}
			if kind == "prior-conversation" {
				job, err := s.Job(j.ID)
				if err != nil || job.ConversationID != "previous-conversation" {
					t.Fatal("prior conversation history lost", err)
				}
			}
		})
	}
}

func TestNativePrelaunchRecoveryAfterRestart(t *testing.T) {
	s, d, j, result := prelaunchAttempt(t)
	d.Service = Service{Brain: s.Brain, Authority: s.Authority}
	fences, inspects := 0, 0
	d.Backends["zed"] = backendFunc{
		fence: func(context.Context, Job) error { fences++; return nil },
		inspect: func(context.Context, RunSpec, Sink) (RunResult, bool, error) {
			inspects++
			return result, true, nil
		},
	}
	if err := recoverAttempts(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	got, err := d.Service.Result(j.ID, 1)
	if err != nil || got.State != "failed" || got.ErrorText != result.ErrorText || fences != 1 || inspects != 1 {
		t.Fatalf("recovery: %+v %v fences=%d inspects=%d", got, err, fences, inspects)
	}
}

func assertCancellationMarker(t *testing.T, s Service, id, marker string) {
	t.Helper()
	job, err := s.Job(id)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(id, 1)
	if err != nil || job.State != "unknown" || job.HoldReason != marker || result.State != "unknown" || result.ErrorCode != marker || result.ErrorText != "cancellation remains pending" {
		t.Fatalf("lost cancellation marker %s: job=%+v result=%+v err=%v", marker, job, result, err)
	}
}

func TestNativePrelaunchCancellationSurvivesTwoRecoveryPasses(t *testing.T) {
	for _, marker := range []string{"cancel_requested", "fencing"} {
		t.Run(marker, func(t *testing.T) {
			for _, failure := range []string{"fence", "inspect-error", "inspect-false", "timeout"} {
				t.Run(failure, func(t *testing.T) {
					s, d, j, result := prelaunchAttempt(t)
					if err := s.transaction(context.Background(), true, func(db *ledger) error {
						r := db.Jobs[j.ID]
						setState(r, "unknown", marker)
						r.Rounds[1].Result.ErrorText = "cancellation remains pending"
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					for pass := 0; pass < 2; pass++ {
						d.Service = Service{Brain: s.Brain, Authority: s.Authority}
						d.Backends["zed"] = backendFunc{
							fence: func(context.Context, Job) error {
								if pass == 0 && failure == "fence" {
									return errors.New("uncertain writer")
								}
								return nil
							},
							inspect: func(context.Context, RunSpec, Sink) (RunResult, bool, error) {
								if pass == 0 && failure == "inspect-error" {
									return RunResult{}, false, errors.New("uncertain result")
								}
								return result, pass != 0 || failure != "inspect-false", nil
							},
						}
						if pass == 0 && failure == "timeout" {
							unlock, err := store.Lock(s.Brain, "helper-ledger")
							if err != nil {
								t.Fatal(err)
							}
							// Hold the ledger beyond the caller deadline to exercise the
							// fresh commit context in the deferred timeout handler.
							released := make(chan error, 1)
							time.AfterFunc(80*time.Millisecond, func() { released <- unlock() })
							ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
							err = recoverAttempt(ctx, d, j)
							cancel()
							if unlockErr := <-released; err != nil || unlockErr != nil {
								t.Fatalf("timeout reconciliation: %v unlock=%v", err, unlockErr)
							}
						} else if err := recoverAttempts(context.Background(), d); err != nil {
							t.Fatal(err)
						}
						assertCancellationMarker(t, d.Service, j.ID, marker)
					}
					d.Service.FenceRuntime = func(context.Context, Job) error { return nil }
					if err := d.Service.Cancel(context.Background(), j.ID, "operator_cancelled"); err != nil {
						t.Fatal(err)
					}
					got, err := s.Job(j.ID)
					if err != nil || got.State != "cancelled" {
						t.Fatalf("cancellation did not finish: %+v %v", got, err)
					}
				})
			}
		})
	}
}

func TestNativePrelaunchCancellationWhileInspectInflight(t *testing.T) {
	for _, marker := range []string{"cancel_requested", "fencing"} {
		t.Run(marker, func(t *testing.T) {
			s, d, j, result := prelaunchAttempt(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			d.Backends["zed"] = backendFunc{inspect: func(ctx context.Context, _ RunSpec, _ Sink) (RunResult, bool, error) {
				close(entered)
				select {
				case <-release:
					return result, true, nil
				case <-ctx.Done():
					return RunResult{}, false, ctx.Err()
				}
			}}
			done := make(chan error, 1)
			go func() { done <- recoverAttempts(ctx, d) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("inspection did not start")
			}
			// Persist the same checkpoint as Cancel/stopJobs while inspection is
			// suspended. Cancellation continuation resumes only after finish.
			if err := s.transaction(ctx, true, func(db *ledger) error {
				r := db.Jobs[j.ID]
				setState(r, "unknown", marker)
				r.Rounds[1].Result.ErrorText = "cancellation remains pending"
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("inspection did not finish")
			}
			assertCancellationMarker(t, s, j.ID, marker)
			s.FenceRuntime = func(context.Context, Job) error { return nil }
			if err := s.Cancel(ctx, j.ID, "operator_cancelled"); err != nil {
				t.Fatal(err)
			}
			got, err := s.Job(j.ID)
			if err != nil || got.State != "cancelled" {
				t.Fatalf("cancellation did not finish: %+v %v", got, err)
			}
		})
	}
}
