package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type bindingAuthority struct {
	helper.Authority
	contract int
}

type bindingAuthorityKey struct{}

func (a bindingAuthority) Resolve(ctx context.Context, req helper.Request) (helper.Scope, error) {
	scope, err := a.Authority.Resolve(ctx, req)
	scope.CodexContract = a.contract
	mode, _ := ctx.Value(bindingAuthorityKey{}).(int)
	switch mode {
	case 1:
		scope.Lease++
	case 2:
		scope.Generation = "foreign-generation"
	}
	return scope, err
}
func (a bindingAuthority) Check(ctx context.Context, job helper.Job) error {
	scope, err := a.Resolve(ctx, job.Request)
	if err != nil {
		return err
	}
	if scope.Authorization() != job.Scope.Authorization() {
		return fmt.Errorf("stored logical authority changed")
	}
	return nil
}
func (a bindingAuthority) ResolveStep(_ context.Context, scope helper.Scope, component string) (helper.StepBasis, error) {
	return helper.StepBasis{EntryRef: scope.WorkID + "#1", Component: component}, nil
}

type bindingUsage struct{ provider string }

func (q bindingUsage) Read(ctx context.Context) (helper.QuotaSnapshot, error) {
	snapshot, err := (&controlUsage{}).Read(ctx)
	snapshot.Provider = q.provider
	return snapshot, err
}

func bindingFixture(t *testing.T, provider string) (helper.Service, helper.RunnerDeps, helper.Request) {
	t.Helper()
	s, d, req := controlFixture(t)
	contract := 0
	if provider == "codex" {
		contract = 2
		old, err := executor.ReadSelection(s.Brain, "epic1")
		if err != nil {
			t.Fatal(err)
		}
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		profile := executor.Profile{SchemaVersion: 1, Executor: "codex", CodexBinary: binary, CodexExecServer: binary, CodexVersion: "0.162.1", CodexModelCatalog: "/fixture/models.json", CodexModelCatalogSHA256: strings.Repeat("a", 64), ContainerImage: "sha256:" + strings.Repeat("a", 64), IdentityBinding: "fixture-account", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, MaxActiveHelpers: 1, Routes: map[string][]executor.Candidate{}}
		routes := map[string][]executor.Route{}
		for name := range old.Routes {
			profile.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "high"}}
			routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "high", Model: executor.CodexModel}}
		}
		selection := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "epic1", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("a", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("b", 64)}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
		data, err := json.Marshal(selection)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(s.Brain, "factory", "executors", "epic1", "selection.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		// The contract2 projection below tests the broker, not genuine B2 qualification.
		registerLegacyCoordinatorFixture(t, s.Brain, selection, session.Spec{Name: "factory:" + req.OwnerID + ":orchestrator", Task: req.OwnerID, EpicID: selection.EpicID, Generation: fmt.Sprintf("%s:%d:orchestrator", req.OwnerID, req.Lease)})
	}
	s.Authority = bindingAuthority{Authority: s.Authority, contract: contract}
	d.Service = s
	d.Quotas = map[string]helper.UsageReader{provider: bindingUsage{provider}}
	return s, d, req
}

type bindingSink struct{ helper.Sink }

func (s bindingSink) Record(ctx context.Context, event string, fields map[string]string) error {
	if event == helper.EventCheckpointSaved {
		return nil
	}
	return s.Sink.Record(ctx, event, fields)
}

func bindingTurn(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	writer := sink
	if spec.Job.Route.Provider == "codex" {
		writer = bindingSink{sink}
	}
	result, err := controlTurn(ctx, spec, writer, true)
	if err != nil || spec.Job.Route.Provider != "codex" {
		return result, err
	}
	result.ObservedModel = ""
	result.EvidencePolicy = executor.CodexModelEvidencePolicy
	result.ConfiguredModel, result.ConfiguredEffort = spec.Job.Route.Model, spec.Job.Route.Effort
	sum := sha256.Sum256([]byte(result.Reply))
	result.PersistedReplySHA256 = hex.EncodeToString(sum[:])
	err = sink.Record(ctx, helper.EventPersistedTurnVerified, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID, "reply_sha256": result.PersistedReplySHA256, "configured_model": result.ConfiguredModel, "configured_effort": result.ConfiguredEffort, "evidence_policy": result.EvidencePolicy})
	return result, err
}

func bindingControl(ctx context.Context, service helper.Service, job helper.Job, op string, payload any) (any, error) {
	data, err := json.Marshal(coordinatorRequest(op, payload))
	if err != nil {
		return nil, err
	}
	return managedControl(ctx, service, job.ID, job.Generation, data)
}

func TestBrokerBindingManagedChildren(t *testing.T) {
	for _, provider := range []string{"codex"} {
		for _, operation := range []string{"submit", "result", "continue"} {
			t.Run(provider+"/"+operation, func(t *testing.T) {
				s, d, req := bindingFixture(t, provider)
				root, err := s.Submit(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				var child helper.Accepted
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				failures := make(chan error, 1)
				run := func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
					result, err := bindingTurn(ctx, spec, sink)
					if err != nil || spec.Job.ID != root.JobID {
						return result, err
					}
					if provider == "codex" && spec.Job.Scope.StepSlot == "" {
						return result, fmt.Errorf("parent has no real root step slot")
					}
					switch spec.Job.Round {
					case 1:
						input := toolcontract.NestedSubmitInput{RequestKey: "nested", Component: "reviewer-code", Prompt: "review child"}
						if operation == "submit" {
							value, e := bindingControl(ctx, s, spec.Job, "submit", input)
							err = e
							if err == nil {
								child = value.(helper.Accepted)
							}
						} else {
							child, err = s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, input)
						}
						if err != nil {
							return result, fmt.Errorf("bound nested submit: %w", err)
						}
						j, e := s.Job(child.JobID)
						if e != nil || j.Request.ParentJobID != root.JobID || j.Scope.StepSlot != "" || j.Scope.Authorization() != spec.Job.Scope.Authorization() {
							return result, fmt.Errorf("child binding differs: %+v %v", j, e)
						}
						return result, s.Yield(ctx, root.JobID, []string{child.JobID})
					case 2:
						if operation == "result" {
							_, err = bindingControl(ctx, s, spec.Job, "result", map[string]any{"job_id": child.JobID, "round": 1})
						}
						if operation == "continue" {
							_, err = bindingControl(ctx, s, spec.Job, "continue", map[string]any{"job_id": child.JobID, "request": helper.ContinueRequest{SchemaVersion: 1, RequestKey: "child-next", ExpectedRound: 1, Prompt: "review again"}})
							if err == nil {
								err = s.Yield(ctx, root.JobID, []string{child.JobID})
							}
						}
					}
					return result, err
				}
				d.Backends = map[string]helper.Backend{provider: controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
					result, err := run(ctx, spec, sink)
					if err != nil {
						failures <- err
						cancel()
					}
					return result, err
				}}}
				runErr := helper.Run(ctx, d)
				select {
				case err := <-failures:
					t.Fatalf("bound frontdoor failed: %v", err)
				default:
				}
				if runErr != nil {
					t.Fatal(runErr)
				}
				last := 2
				if operation == "continue" {
					last = 3
				}
				got, err := s.Result(root.JobID, last)
				if err != nil || got.State != "completed" {
					first, _ := s.Result(root.JobID, 1)
					t.Fatalf("parent did not complete actual round %d: %+v %v; first=%+v", last, got, err, first)
				}
			})
		}
	}
}

func TestBrokerBindingCoordinatorCannotClaimNestedParent(t *testing.T) {
	s, d, req := bindingFixture(t, "codex")
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var checked bool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Backends = map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
		result, err := bindingTurn(ctx, spec, sink)
		if err != nil || spec.Job.ID != root.JobID {
			return result, err
		}
		spoof := req
		spoof.RequestKey, spoof.ParentJobID = "coordinator-spoof", root.JobID
		bound := session.Spec{Task: req.OwnerID, EpicID: "epic1", Generation: "owner1:1:orchestrator", Name: "factory:owner1:orchestrator"}
		_, e := scopedCoordinatorControl(ctx, s.Brain, bound, coordinatorRequest("helper_submit", spoof))
		checked = true
		if e == nil || !strings.Contains(e.Error(), "parent_job_id") {
			t.Errorf("coordinator spoof not rejected at parent guard: %v", e)
			cancel()
		}
		return result, nil
	}}}
	_ = helper.Run(ctx, d)
	if !checked {
		t.Fatal("coordinator frontdoor was not checked")
	}
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("coordinator changed admitted graph: %+v %v", jobs, err)
	}
}

func TestBrokerBindingManagedAuthorityAndPayloadGuards(t *testing.T) {
	for _, provider := range []string{"codex"} {
		t.Run(provider, func(t *testing.T) {
			s, d, req := bindingFixture(t, provider)
			root, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			otherReq := req
			otherReq.RequestKey, otherReq.Component = "other-root", "reviewer-arch-test"
			other, err := s.Submit(context.Background(), otherReq)
			if err != nil {
				t.Fatal(err)
			}
			var childID string
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			failures := make(chan error, 1)
			run := func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
				result, err := bindingTurn(ctx, spec, sink)
				if err != nil || spec.Job.ID != root.JobID || spec.Job.Round != 1 {
					return result, err
				}
				valid := toolcontract.NestedSubmitInput{RequestKey: "one-child", Component: "reviewer-code", Prompt: "scoped child"}
				data, _ := json.Marshal(coordinatorRequest("submit", valid))
				if _, err := managedControl(ctx, s, spec.Job.ID, spec.Job.Generation+1, data); err == nil || !strings.Contains(err.Error(), "active admitted generation") {
					return result, fmt.Errorf("wrong execution generation accepted: %v", err)
				}
				for _, mode := range []int{1, 2} {
					foreign := context.WithValue(ctx, bindingAuthorityKey{}, mode)
					_, err := bindingControl(foreign, s, spec.Job, "submit", map[string]string{"unknown": "must not reach decoder"})
					if err == nil || !strings.Contains(err.Error(), "stored logical authority changed") {
						return result, fmt.Errorf("foreign lease/generation reached payload: %v", err)
					}
				}
				if _, err := bindingControl(ctx, s, spec.Job, "submit", map[string]string{"request_key": "forged", "component": "reviewer-code", "prompt": "x", "parent_job_id": "forged"}); err == nil || !strings.Contains(err.Error(), "unknown field") {
					return result, fmt.Errorf("nested payload chose parent: %v", err)
				}
				for _, op := range []string{"result", "continue"} {
					payload := map[string]any{"job_id": other.JobID, "round": 1}
					if op == "continue" {
						payload = map[string]any{"job_id": other.JobID, "request": helper.ContinueRequest{SchemaVersion: 1, ExpectedRound: 1, RequestKey: "foreign", Prompt: "x"}}
					}
					if _, err := bindingControl(ctx, s, spec.Job, op, payload); err == nil || !strings.Contains(err.Error(), "foreign") {
						return result, fmt.Errorf("foreign %s accepted: %v", op, err)
					}
				}
				type answer struct {
					value any
					err   error
				}
				answers := make(chan answer, 2)
				for range 2 {
					go func() {
						value, err := bindingControl(ctx, s, spec.Job, "submit", valid)
						answers <- answer{value, err}
					}()
				}
				for range 2 {
					got := <-answers
					if got.err != nil {
						return result, got.err
					}
					accepted := got.value.(helper.Accepted)
					if childID != "" && accepted.JobID != childID {
						return result, fmt.Errorf("concurrent same-key admission duplicated child")
					}
					childID = accepted.JobID
				}
				jobs, err := s.Jobs()
				if err != nil || len(jobs) != 3 {
					return result, fmt.Errorf("guarded requests changed graph: %d jobs %v", len(jobs), err)
				}
				return result, s.Yield(ctx, root.JobID, []string{childID})
			}
			d.Backends = map[string]helper.Backend{provider: controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
				result, err := run(ctx, spec, sink)
				if err != nil {
					failures <- err
					cancel()
				}
				return result, err
			}}}
			runErr := helper.Run(ctx, d)
			select {
			case err := <-failures:
				t.Fatal(err)
			default:
			}
			if runErr != nil {
				t.Fatal(runErr)
			}
			for id, round := range map[string]int{root.JobID: 2, other.JobID: 1, childID: 1} {
				got, err := s.Result(id, round)
				if err != nil || got.State != "completed" {
					t.Fatalf("actual result not completed: %+v %v", got, err)
				}
			}
		})
	}
}
