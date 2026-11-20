package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func (a *stepTestAuthority) CheckVerifierAdmissionLocal(ctx context.Context, scope Scope, step StepBasis, rounds []PublishedRoundEvidence) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	expected, denied := a.scope.Authorization(), a.deny
	a.mu.Unlock()
	if denied || scope != scope.Authorization() || scope != expected || step.Component != "verifier" {
		return ErrAuthority
	}
	actual, err := a.ResolveStep(ctx, scope, "verifier")
	if err != nil {
		return err
	}
	if actual != step {
		return ErrAuthority
	}
	for _, round := range rounds {
		if round.Scope != scope || round.Round.Step != step {
			return ErrAuthority
		}
	}
	return nil
}

func TestStepVerifierAdmissionFixtureRequiresExactTuple(t *testing.T) {
	for _, fault := range []string{"exact", "foreign-scope", "changed-step", "wrong-component", "round-scope", "round-step", "cancelled", "denied"} {
		t.Run(fault, func(t *testing.T) {
			_, _, _, a := setupStep(t)
			scope := a.scope.Authorization()
			step, err := a.ResolveStep(context.Background(), scope, "verifier")
			if err != nil {
				t.Fatal(err)
			}
			rounds := []PublishedRoundEvidence{{Scope: scope, Round: RevisionRoundRef{Step: step}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "foreign-scope":
				scope.Generation += "-foreign"
				rounds[0].Scope = scope
			case "changed-step":
				step.EntryRef += "-foreign"
				rounds[0].Round.Step = step
			case "wrong-component":
				step.Component = "implementor"
			case "round-scope":
				rounds[0].Scope.Generation += "-foreign"
			case "round-step":
				rounds[0].Round.Step.EntryRef += "-foreign"
			case "cancelled":
				cancel()
			case "denied":
				a.mu.Lock()
				a.deny = true
				a.mu.Unlock()
			}
			err = a.CheckVerifierAdmissionLocal(ctx, scope, step, rounds)
			if fault == "exact" {
				if err != nil {
					t.Fatal("exact private fixture tuple refused", err)
				}
			} else if fault == "cancelled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("cancelled private fixture accepted", err)
				}
			} else if !errors.Is(err, ErrAuthority) {
				t.Fatal("foreign private fixture tuple accepted", err)
			}
		})
	}
}

type verifierProjectionAuthority struct {
	*stepTestAuthority
	mu     sync.Mutex
	rounds []PublishedRoundEvidence
}

func (a *verifierProjectionAuthority) CheckVerifierAdmissionLocal(_ context.Context, scope Scope, step StepBasis, rounds []PublishedRoundEvidence) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, round := range rounds {
		if round.Scope != scope || round.Round.Step != step {
			return ErrAuthority
		}
	}
	a.rounds = append([]PublishedRoundEvidence(nil), rounds...)
	return nil
}

func verifierProjectionFixture(t *testing.T) (Service, RunnerDeps, Request) {
	t.Helper()
	s, d, req, original := setupStep(t)
	a := &verifierProjectionAuthority{stepTestAuthority: original}
	s.Authority = a
	d.Service = s
	b, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	b.Components["verifier"] = blueprint.Component{RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "verify.v<n>.md", Headings: []string{"## Result"}}}}
	raw, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(s.Brain), raw, 0600); err != nil {
		t.Fatal(err)
	}
	sel, err := executor.ReadSelection(s.Brain, original.scope.EpicID)
	if err != nil {
		t.Fatal(err)
	}
	sel.Routes["verifier"] = sel.Routes["implementor"]
	sel.Profile.Routes["verifier"] = sel.Profile.Routes["implementor"]
	raw, err = json.Marshal(sel)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.Factory(s.Brain), "executors", sel.EpicID, "selection.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	req.Component = "verifier"
	req.OutputPath = filepath.Join(original.scope.OutputRoot, "verify.v1.md")
	return s, d, req
}

func TestVerifierAdmissionKeepsActualUnpublishedOutputRecovery(t *testing.T) {
	s, d, request := verifierProjectionFixture(t)
	accepted := mustSubmit(t, s, request)
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		body := "Missing the required heading."
		if spec.Job.Round > 1 {
			body = "## Result\nActual repaired output.\n"
		}
		if err := os.WriteFile(spec.OutputPath, []byte(body), 0600); err != nil {
			return RunResult{}, err
		}
		return recordCodexTurn(ctx, spec, sink)
	}}
	runBounded(t, d)
	failed, err := s.Result(accepted.JobID, 1)
	if err != nil || failed.State != "failed" || failed.ErrorCode != "output_validation_failed" || failed.OutputFile != "" {
		t.Fatalf("fixture did not fail through actual output validator: %+v %v", failed, err)
	}
	next, err := s.Continue(context.Background(), accepted.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "retain-unpublished-verifier", ExpectedRound: 1, Prompt: "Repair the reserved output."})
	if err != nil || next.Round != 2 {
		t.Fatal("failed unpublished verifier was treated as published FAIL", next, err)
	}
	runBounded(t, d)
	published, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 2)
	if err != nil || published.Round.Output.Path != request.OutputPath {
		t.Fatal("actual repair lost original output reservation", published, err)
	}
}

func TestVerifierAdmissionWaitsForActualPublicationAndDelivery(t *testing.T) {
	for _, cut := range []string{"result-stored", "delivery"} {
		t.Run(cut, func(t *testing.T) {
			s, d, request := verifierProjectionFixture(t)
			accepted := mustSubmit(t, s, request)
			d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
				if err := os.WriteFile(spec.OutputPath, []byte("## Result\nActual broker output.\n"), 0600); err != nil {
					return RunResult{}, err
				}
				return recordCodexTurn(ctx, spec, sink)
			}}
			if cut == "result-stored" {
				d.Service.testCheckpoint = func(at string) {
					if at == "result_stored" {
						panic("actual publication cut")
					}
				}
			} else {
				d.Notify = func(context.Context, Job, RoundResult) error { panic("actual publication cut") }
			}
			func() {
				defer func() {
					if recover() != "actual publication cut" {
						t.Fatal("did not reach actual publication cut")
					}
				}()
				runBounded(t, d)
			}()
			continuation := ContinueRequest{SchemaVersion: 1, RequestKey: "after-actual-publication", ExpectedRound: 1, Prompt: "Continue only after publication.", OutputPath: filepath.Join(filepath.Dir(request.OutputPath), "verify.v2.md")}
			_, err := s.Continue(context.Background(), accepted.JobID, continuation)
			if err == nil || !strings.Contains(err.Error(), "published round completion evidence unavailable") {
				t.Fatal("incomplete completed projection admitted a new round", err)
			}
			d.Service.testCheckpoint = nil
			d.Notify = func(context.Context, Job, RoundResult) error { return nil }
			runBounded(t, d)
			if _, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1); err != nil {
				t.Fatal("ordinary flush did not settle publication", err)
			}
			if next, err := s.Continue(context.Background(), accepted.JobID, continuation); err != nil || next.Round != 2 {
				t.Fatal("actual published non-FAIL source stayed blocked", next, err)
			}
		})
	}
}

func TestVerifierAdmissionLeavesLegacyAndNestedRolesUnchanged(t *testing.T) {
	s := Service{}
	for _, test := range []struct {
		scope  Scope
		role   string
		parent string
	}{
		{Scope{}, "verifier", ""},
		{Scope{CodexContract: 1}, "verifier", ""},
		{Scope{CodexContract: 2}, "implementor", ""},
		{Scope{CodexContract: 2}, "verifier", "actual-parent"},
	} {
		if err := s.checkVerifierAdmissionLocked(context.Background(), nil, test.scope, nil, test.role, test.parent); err != nil {
			t.Fatal("unrelated role required verifier projection", err)
		}
	}
	if err := s.checkVerifierAdmissionLocked(context.Background(), nil, Scope{CodexContract: 2}, nil, "verifier", ""); !errors.Is(err, ErrAuthority) {
		t.Fatal("schema2 root verifier omitted required authority", err)
	}
}
