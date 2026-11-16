package helper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/store"
)

func startupAttempt(t *testing.T) (Service, RunnerDeps, Job, RunResult) {
	t.Helper()
	s, d, j, result := prelaunchAttempt(t)
	if err := (journalSink{service: s, job: j}).Record(context.Background(), EventProcessOwned, startupProcessEvidence()); err != nil {
		t.Fatal(err)
	}
	result.ErrorCode = "native_startup_failed"
	result.ErrorText = "native worker exited before conversation acknowledgment"
	return s, d, j, result
}

func startupProcessEvidence() map[string]string {
	return map[string]string{"pid": "42", "process_group": "42", "start_identity": "1793163631:411146", "generation": "1", "epoch": "startup-epoch"}
}

func TestNativeStartupSettlementRequiresExactProcessOnly(t *testing.T) {
	for _, kind := range []string{"valid", "model", "turn", "conversation", "extra-journal", "missing-process", "missing-generation", "wrong-generation", "missing-epoch", "bad-pid", "bad-group", "current-conversation", "round-result", "ended", "diagnostic", "run-only", "unfenced", "run-error", "interrupt", "claude", "zero-pid", "empty-start", "invalid-epoch", "error-code", "outcome"} {
		t.Run(kind, func(t *testing.T) {
			s, _, j, result := startupAttempt(t)
			inspection := true
			var runErr, fenceErr error
			switch kind {
			case "error-code":
				result.ErrorCode = "native_failed"
			case "outcome":
				result.Outcome = "completed"
			case "model":
				result.ObservedModel = j.Route.Model
			case "turn":
				result.TurnID = "invented"
			case "conversation":
				result.ConversationID = "invented"
			case "diagnostic":
				result.ErrorText = "untrusted provider detail"
			case "run-only":
				inspection = false
			case "unfenced":
				fenceErr = errors.New("unconfirmed death")
			case "run-error":
				runErr = errors.New("unconfirmed Run")
			case "claude":
				j.Route.Provider = "claude"
			}
			if err := s.transaction(context.Background(), true, func(db *ledger) error {
				r := db.Jobs[j.ID]
				q := r.Rounds[1]
				switch kind {
				case "zero-pid":
					q.Evidence[EventProcessOwned+":pid"] = "0"
					q.Evidence[EventProcessOwned+":process_group"] = "0"
				case "empty-start":
					q.Evidence[EventProcessOwned+":start_identity"] = ""
				case "invalid-epoch":
					q.Evidence[EventProcessOwned+":epoch"] = "invalid:epoch"
				case "extra-journal":
					q.Evidence[EventConversationRecorded+":recorded"] = "true"
				case "missing-process":
					q.Evidence = map[string]string{}
				case "missing-generation":
					delete(q.Evidence, EventProcessOwned+":generation")
				case "wrong-generation":
					q.Evidence[EventProcessOwned+":generation"] = "2"
				case "missing-epoch":
					delete(q.Evidence, EventProcessOwned+":epoch")
				case "bad-pid":
					q.Evidence[EventProcessOwned+":pid"] = "not-a-pid"
				case "bad-group":
					q.Evidence[EventProcessOwned+":process_group"] = "43"
				case "current-conversation":
					r.Job.ConversationID = "prior-session"
				case "round-result":
					q.Result.ConversationID = "prior-session"
				case "ended":
					q.Ended = true
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
			got, err := s.Result(j.ID, 1)
			want := "unknown"
			if kind == "valid" {
				want = "failed"
			}
			if err != nil || got.State != want {
				t.Fatalf("startup settlement state=%s want=%s result=%+v err=%v", got.State, want, got, err)
			}
			if want == "failed" && (got.ErrorCode != result.ErrorCode || got.ErrorText != result.ErrorText || got.ObservedModel != "" || got.TurnID != "" || got.ConversationID != "") {
				t.Fatalf("startup failure changed its proof or diagnosis: %+v", got)
			}
		})
	}
}

func TestNativeStartupInspectionRejectsJournalIdentityMismatch(t *testing.T) {
	for _, field := range []string{"pid", "process_group", "start_identity", "generation", "epoch"} {
		t.Run(field, func(t *testing.T) {
			s, d, j, result := startupAttempt(t)
			if err := s.transaction(context.Background(), true, func(db *ledger) error {
				db.Jobs[j.ID].Rounds[1].Evidence[EventProcessOwned+":"+field] = "different"
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			d.Backends["zed"] = backendFunc{inspect: func(ctx context.Context, _ RunSpec, sink Sink) (RunResult, bool, error) {
				if err := sink.Record(ctx, EventProcessOwned, startupProcessEvidence()); err != nil {
					return RunResult{}, false, err
				}
				return result, true, nil
			}}
			if err := recoverAttempts(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			got, err := s.Result(j.ID, 1)
			if err != nil || got.State != "unknown" {
				t.Fatalf("mismatched process proof settled: %+v %v", got, err)
			}
		})
	}
}

func assertStartupCancellation(t *testing.T, s Service, id, hold, code string) {
	t.Helper()
	job, err := s.Job(id)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(id, 1)
	if err != nil || job.State != "unknown" || job.HoldReason != hold || result.State != "unknown" || result.ErrorCode != code || result.ErrorText != "preserve this cancellation" {
		t.Fatalf("startup recovery erased cancellation: hold=%q code=%q job=%+v result=%+v err=%v", hold, code, job, result, err)
	}
}

func TestNativeStartupCancellationAcrossRecoveryWrites(t *testing.T) {
	for _, marker := range []string{"cancel_requested", "fencing"} {
		for _, slot := range []string{"both", "hold-only", "code-only"} {
			for _, failure := range []string{"fence", "inspect", "timeout", "finish"} {
				t.Run(marker+"/"+slot+"/"+failure, func(t *testing.T) {
					s, d, j, result := startupAttempt(t)
					hold, code := marker, marker
					if slot == "hold-only" {
						code = "operator_diagnostic"
					}
					if slot == "code-only" {
						hold = "operator_diagnostic"
					}
					if err := s.transaction(context.Background(), true, func(db *ledger) error {
						r := db.Jobs[j.ID]
						setState(r, "unknown", marker)
						r.Job.HoldReason = hold
						r.Rounds[1].Result.ErrorCode = code
						r.Rounds[1].Result.ErrorText = "preserve this cancellation"
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
							inspect: func(ctx context.Context, _ RunSpec, sink Sink) (RunResult, bool, error) {
								if pass == 0 && failure == "inspect" {
									return RunResult{}, false, errors.New("uncertain result")
								}
								if err := sink.Record(ctx, EventProcessOwned, startupProcessEvidence()); err != nil {
									return RunResult{}, false, err
								}
								return result, true, nil
							},
						}
						if pass == 0 && failure == "timeout" {
							unlock, err := store.Lock(s.Brain, "helper-ledger")
							if err != nil {
								t.Fatal(err)
							}
							// Exercise the deferred timeout commit while the caller cannot
							// acquire the ledger, rather than fabricating a probe error.
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
						assertStartupCancellation(t, d.Service, j.ID, hold, code)
					}
				})
			}
		}
	}
}

func TestNativeStartupCancellationDuringInspection(t *testing.T) {
	for _, marker := range []string{"cancel_requested", "fencing"} {
		t.Run(marker, func(t *testing.T) {
			s, d, j, result := startupAttempt(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			d.Backends["zed"] = backendFunc{inspect: func(ctx context.Context, _ RunSpec, sink Sink) (RunResult, bool, error) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return RunResult{}, false, ctx.Err()
				}
				if err := sink.Record(ctx, EventProcessOwned, startupProcessEvidence()); err != nil {
					return RunResult{}, false, err
				}
				return result, true, nil
			}}
			done := make(chan error, 1)
			go func() { done <- recoverAttempts(ctx, d) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("inspection did not start")
			}
			if err := s.transaction(ctx, true, func(db *ledger) error {
				r := db.Jobs[j.ID]
				setState(r, "unknown", marker)
				r.Rounds[1].Result.ErrorText = "preserve this cancellation"
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
			assertStartupCancellation(t, s, j.ID, marker, marker)
		})
	}
}
