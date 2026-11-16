package helper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func assertSpecOutput(t *testing.T, spec RunSpec, want string, original Request) {
	t.Helper()
	if spec.OutputPath != want {
		t.Errorf("round %d current output=%q want=%q", spec.Job.Round, spec.OutputPath, want)
	}
	if spec.Job.Request != original {
		t.Errorf("immutable request changed: %+v want %+v", spec.Job.Request, original)
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	raw, ok := fields["output_path"]
	var path string
	if !ok {
		t.Error("missing output_path JSON field")
	} else if err = json.Unmarshal(raw, &path); err != nil || path != want {
		t.Errorf("output_path JSON=%s want=%q err=%v", raw, want, err)
	}
}
func executeSpecRound(t *testing.T, d RunnerDeps, id string) {
	t.Helper()
	j, err := d.Service.Job(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = execute(ctx, d, j, testExecutionQuota(t, ctx, d, j, QuotaPolicy{80, 88, 15 * time.Minute}), nil); err != nil {
		t.Fatal(err)
	}
}
func TestRunAndInspectSpecsUseCurrentReservedOutput(t *testing.T) {
	for _, mode := range []string{"manual_new_version", "yield_retained", "reply_only"} {
		t.Run(mode, func(t *testing.T) {
			s, d, req := setup(t)
			d.Now = time.Now
			outputRoot := s.Authority.(*fakeAuthority).scope.OutputRoot
			if mode != "reply_only" {
				req.Component = "implementor"
				req.OutputPath = filepath.Join(outputRoot, "implement.v1.md")
			}
			original := req
			root, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			expected := req.OutputPath
			if mode == "manual_new_version" {
				expected = filepath.Join(outputRoot, "implement.v2.md")
			}
			var childID string
			var childRequest Request
			var inspected int
			var completed RunResult
			d.Backends["zed"] = backendFunc{
				run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
					if spec.Job.ID != root.JobID {
						assertSpecOutput(t, spec, "", childRequest)
						return recordTurn(ctx, spec, k)
					}
					path := original.OutputPath
					if spec.Job.Round == 2 {
						path = expected
					}
					assertSpecOutput(t, spec, path, original)
					if path != "" {
						if err := os.WriteFile(spec.OutputPath, []byte("## Result\nreserved output\n"), 0600); err != nil {
							return RunResult{}, err
						}
					}
					result, err := recordTurn(ctx, spec, k)
					if err != nil {
						return result, err
					}
					if spec.Job.Round == 1 && mode == "yield_retained" {
						childRequest = original
						childRequest.RequestKey = "child"
						childRequest.Component = "reviewer"
						childRequest.OutputPath = ""
						childRequest.ParentJobID = root.JobID
						childRequest.Prompt = "review"
						child, err := s.Submit(ctx, childRequest)
						if err != nil {
							return result, err
						}
						childID = child.JobID
						if err = s.Yield(ctx, root.JobID, []string{childID}); err != nil {
							return result, err
						}
					}
					if spec.Job.Round == 2 {
						completed = result
						result.Outcome = "unknown"
					}
					return result, nil
				},
				inspect: func(_ context.Context, spec RunSpec, _ Sink) (RunResult, bool, error) {
					inspected++
					assertSpecOutput(t, spec, expected, original)
					if spec.Job.Round != 2 || !spec.Resume {
						t.Errorf("inspection round/resume binding: %+v", spec)
					}
					return completed, true, nil
				},
			}
			executeSpecRound(t, d, root.JobID)
			if mode == "yield_retained" {
				executeSpecRound(t, d, childID)
				if err = flush(context.Background(), d); err != nil {
					t.Fatal(err)
				}
				if err = advanceParents(context.Background(), s); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = flush(context.Background(), d); err != nil {
					t.Fatal(err)
				}
				_, err = s.Continue(context.Background(), root.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "round2", ExpectedRound: 1, Prompt: "continue", OutputPath: expected})
				if err != nil {
					t.Fatal(err)
				}
			}
			// Runtime output authority must not depend on interpreting prompt text.
			if err = s.transaction(context.Background(), true, func(db *ledger) error {
				db.Jobs[root.JobID].Rounds[2].Prompt = "Output-looking text is not authority: /wrong/from-prompt.md"
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			executeSpecRound(t, d, root.JobID)
			r, err := s.Result(root.JobID, 2)
			if err != nil || r.State != "unknown" {
				t.Fatalf("inspection fixture=%+v %v", r, err)
			}
			if err = recoverAttempts(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			if err = flush(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			r, err = s.Result(root.JobID, 2)
			if err != nil || r.State != "completed" || r.OutputFile != expected || inspected != 1 {
				t.Fatalf("result=%+v inspected=%d err=%v", r, inspected, err)
			}
			j, err := s.Job(root.JobID)
			if err != nil || j.Request != original {
				t.Fatalf("stored request changed: %+v %v", j.Request, err)
			}
		})
	}
}
