package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestLiveCancellationInspectsSavedInterruptionWithoutBrokerRestart(t *testing.T) {
	for _, cause := range []string{"operator", "quota", "runner_shutdown"} {
		t.Run(cause, func(t *testing.T) {
			s, d, req := setup(t)
			req.Component = "implementor"
			req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			identified := make(chan RunResult, 1)
			var quotaPaused, stopped, removed atomic.Bool
			var runs, inspections atomic.Int32
			d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
				used := 10.0
				if quotaPaused.Load() {
					used = 95
				}
				return quota(time.Now(), used), nil
			})
			savedPath := filepath.Join(s.Brain, "native-saved-interruption.json")
			d.Backends["zed"] = backendFunc{
				run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
					runs.Add(1)
					if spec.Resume {
						if spec.Job.ConversationID == "" || spec.Prompt != "recover explicitly\n\nAuthoritative output path: "+req.OutputPath {
							return RunResult{}, errors.New("replayed original prompt")
						}
						if err := os.WriteFile(spec.OutputPath, []byte("## Result\nrecovered\n"), 0600); err != nil {
							return RunResult{}, err
						}
						return recordTurn(ctx, spec, k)
					}
					result, err := beginTurn(ctx, spec, k)
					if err != nil {
						return result, err
					}
					result.Outcome = "interrupted"
					if err = appendSynced(filepath.Join(s.Brain, "cancelled-tool-effect"), "one command\n"); err != nil {
						return result, err
					}
					data, err := json.Marshal(result)
					if err != nil {
						return result, err
					}
					if err = durableWrite(savedPath, data, durableOps{}); err != nil {
						return result, err
					}
					identified <- result
					<-ctx.Done()
					// Like the transport, Run returns cancellation rather than a saved result.
					// The checkpoint is discoverable only through Inspect after fencing.
					return RunResult{}, ctx.Err()
				},
				fence: func(ctx context.Context, _ Job) error {
					if ctx.Err() != nil {
						t.Error("fence inherited cancellation")
					}
					stopped.Store(true)
					removed.Store(true)
					return nil
				},
				inspect: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, bool, error) {
					inspections.Add(1)
					if ctx.Err() != nil {
						t.Error("inspect inherited cancellation")
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Error("inspection is not bounded")
					}
					if !stopped.Load() || !removed.Load() {
						return RunResult{}, false, errors.New("inspection before writer death")
					}
					data, err := os.ReadFile(savedPath)
					if err != nil {
						return RunResult{}, false, err
					}
					var result RunResult
					if err = strictDecode(data, &result); err != nil {
						return result, false, err
					}
					if spec.Job.Round != 1 || spec.OutputPath != req.OutputPath {
						t.Error("inspection lost attempt/output binding")
					}
					for _, event := range []struct {
						name   string
						fields map[string]string
					}{
						{EventNativeEnded, map[string]string{"turn_id": result.TurnID}},
						{EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID}},
					} {
						if err = k.Record(ctx, event.name, event.fields); err != nil {
							return result, false, err
						}
					}
					return result, true, nil
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			done := make(chan error, 1)
			go func() { defer close(done); done <- Run(ctx, d) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("broker did not stop")
				}
			}()
			var original RunResult
			select {
			case original = <-identified:
			case <-ctx.Done():
				t.Fatal("turn not identified")
			}
			switch cause {
			case "operator":
				if _, err = s.Interrupt(context.Background(), a.JobID, 1, "attach"); err != nil {
					t.Fatal(err)
				}
			case "quota":
				quotaPaused.Store(true)
			case "runner_shutdown":
				cancel()
			}
			eventually(t, func() bool {
				r, e := s.Result(a.JobID, 1)
				return e == nil && (r.State == "recovery_required" || r.State == "unknown")
			})
			result, err := s.Result(a.JobID, 1)
			if err != nil || result.State != "recovery_required" || result.ConversationID != original.ConversationID || result.TurnID != original.TurnID || inspections.Load() != 1 {
				t.Fatalf("broker=%+v inspect=%d host_stopped=%v container_removed=%v err=%v", result, inspections.Load(), stopped.Load(), removed.Load(), err)
			}
			if cause == "runner_shutdown" {
				if result.ErrorCode != "runner_cancelled" {
					t.Fatalf("incorrect cancellation cause: %+v", result)
				}
				select {
				case err = <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("cancelled broker did not exit")
				}
				return
			}
			quotaPaused.Store(false)
			if _, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "recovery", ExpectedRound: 1, Prompt: "recover explicitly"}); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("live broker did not finish explicit continuation")
			}
			result, err = s.Result(a.JobID, 2)
			if err != nil || result.State != "completed" || result.ConversationID != original.ConversationID || runs.Load() != 2 || inspections.Load() != 1 {
				t.Fatalf("%+v runs=%d inspections=%d err=%v", result, runs.Load(), inspections.Load(), err)
			}
			effect, err := os.ReadFile(filepath.Join(s.Brain, "cancelled-tool-effect"))
			if err != nil || string(effect) != "one command\n" {
				t.Fatalf("replayed command: %q %v", effect, err)
			}
		})
	}
}
func TestLiveCancellationKeepsUnknownWithoutInspectionProof(t *testing.T) {
	for _, failure := range []string{"missing", "error", "missing_checkpoint", "wrong_turn", "uncertain_death"} {
		t.Run(failure, func(t *testing.T) {
			s, d, req := setup(t)
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			identified := make(chan RunResult, 1)
			var saved RunResult
			var calls atomic.Int32
			d.Backends["zed"] = backendFunc{
				run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
					result, err := beginTurn(ctx, spec, k)
					if err != nil {
						return result, err
					}
					saved = result
					identified <- result
					<-ctx.Done()
					return RunResult{}, ctx.Err()
				},
				fence: func(context.Context, Job) error {
					if failure == "uncertain_death" {
						return errors.New("writers unconfirmed")
					}
					return nil
				},
				inspect: func(ctx context.Context, _ RunSpec, k Sink) (RunResult, bool, error) {
					calls.Add(1)
					result := saved
					result.Outcome = "interrupted"
					switch failure {
					case "missing":
						return result, false, nil
					case "error":
						return result, true, errors.New("unverified native result")
					case "wrong_turn":
						result.TurnID = "foreign-turn"
					case "missing_checkpoint":
						return result, true, nil
					}
					err := k.Record(ctx, EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID})
					return result, err == nil, err
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			done := make(chan error, 1)
			go func() { defer close(done); done <- Run(ctx, d) }()
			defer func() { cancel(); <-done }()
			select {
			case <-identified:
			case <-ctx.Done():
				t.Fatal("turn not identified")
			}
			if _, err = s.Interrupt(context.Background(), a.JobID, 1, "attach"); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() bool { r, e := s.Result(a.JobID, 1); return e == nil && r.State == "unknown" })
			if _, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "unsafe", ExpectedRound: 1, Prompt: "resume"}); err == nil {
				t.Fatal("unverified interruption was resumable")
			}
			if failure == "uncertain_death" && calls.Load() != 0 {
				t.Fatal("inspected before confirmed death")
			}
		})
	}
}
func TestExplorerStartIsProgramOwnedAndPrecedesExternalWork(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "explorer"
	req.SetID = "set1"
	req.Lease = 1
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "explore.v1.md")
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(s.Brain)
	path := store.EventsPath(ix[req.WorkID].Dir)
	if log, err := events.Read(path); err != nil || len(log) != 0 {
		t.Fatal("submission published execution start", log, err)
	}
	calls := 0
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		calls++
		log, err := events.Read(path)
		if err != nil {
			return RunResult{}, err
		}
		if len(log) != 1 {
			t.Errorf("native external work began without exactly one start: %+v", log)
		} else {
			start := log[0]
			if start.Kind != events.KindStart || start.Sender != events.SenderProgram || start.Text != "explorer" || start.File != req.OutputPath || start.SHA256 != "" || start.Lease != 1 || start.Key != "helper:"+a.JobID+":1:start" || start.Seq != 1 || start.At.IsZero() {
				t.Errorf("wrong start schema/component identity: %+v", start)
			}
		}
		if err = os.WriteFile(spec.OutputPath, []byte("## Result\nexplored\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordTurn(ctx, spec, k)
	}}
	runBounded(t, d)
	runBounded(t, d)
	log, err := events.Read(path)
	if err != nil || len(log) != 2 || log[0].Kind != events.KindStart || log[1].Kind != events.KindEnd || log[0].Seq >= log[1].Seq || calls != 1 {
		t.Fatalf("lifecycle=%+v calls=%d err=%v", log, calls, err)
	}
}
func TestStartFailurePreventsNativeSideEffectAndCanRetry(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "explorer"
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "explore.v1.md")
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(s.Brain)
	path := store.EventsPath(ix[req.WorkID].Dir)
	if err = os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		calls.Add(1)
		if err := os.WriteFile(spec.OutputPath, []byte("## Result\nexplored\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordTurn(ctx, spec, k)
	}}
	j, err := s.Job(a.JobID)
	if err != nil {
		t.Fatal(err)
	}
	d.Now = time.Now
	if err = execute(context.Background(), d, j, testExecutionQuota(t, context.Background(), d, j, QuotaPolicy{80, 88, time.Minute}), nil); err == nil || calls.Load() != 0 {
		t.Fatalf("failed start allowed native side effects: calls=%d err=%v", calls.Load(), err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	if calls.Load() != 1 {
		t.Fatal("failed admission replayed work")
	}
}

func TestWorkflowStartWaitsForQuotaAndAuthority(t *testing.T) {
	for _, hold := range []string{"quota", "authority"} {
		t.Run(hold, func(t *testing.T) {
			s, d, req := setup(t)
			req.Component = "explorer"
			req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "explore.v1.md")
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				calls.Add(1)
				return recordTurn(ctx, spec, k)
			}}
			if hold == "quota" {
				d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) { return quota(time.Now(), 95), nil })
				ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
				defer cancel()
				if err = Run(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			} else {
				s.Authority.(*fakeAuthority).deny = true
				d.Now = time.Now
				j, err := s.Job(a.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if err = execute(context.Background(), d, j, testExecutionQuota(t, context.Background(), d, j, QuotaPolicy{80, 88, time.Minute}), nil); !errors.Is(err, ErrAuthority) {
					t.Fatal(err)
				}
			}
			ix, _ := store.ReadIndex(s.Brain)
			log, err := events.Read(store.EventsPath(ix[req.WorkID].Dir))
			if err != nil || len(log) != 0 || calls.Load() != 0 {
				t.Fatalf("held execution emitted start/work: %+v calls=%d err=%v", log, calls.Load(), err)
			}
		})
	}
}
