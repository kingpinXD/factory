package helper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"go.yaml.in/yaml/v3"
)

func publishedRoundFixture(t *testing.T) (Service, RunnerDeps, Accepted) {
	t.Helper()
	s, d, req, auth := setupStep(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(auth.scope.OutputRoot, "implement.v1.md")
	accepted := mustSubmit(t, s, req)
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		if err := os.WriteFile(spec.OutputPath, []byte("## Result\nActual broker output.\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordCodexTurn(ctx, spec, sink)
	}}
	runBounded(t, d)
	return s, d, accepted
}

func TestPublishedRoundReadsActualGenerationStepAndStableResult(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	got, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	result.Recovery = nil
	raw, _ := json.Marshal(result)
	if got.Round.Generation != job.Generation || got.Round.Round != 1 || got.Round.JobID != job.ID || got.Round.ResultSHA256 != sha256.Sum256(raw) || got.Round.Output.Path != result.OutputFile || got.Round.Output.ProducerJobID != job.ID || got.Round.Output.Round != 1 || got.Round.Step.Component != "implementor" || got.Scope != job.Scope.Authorization() {
		t.Fatalf("actual broker source lost: %+v", got)
	}
	if StepSlotID(got.Scope, got.Round.Step.EntryRef, got.Round.Step.Component) != job.Scope.StepSlot {
		t.Fatal("step wrapper changed the existing identity recipe")
	}
	// Dynamic recovery projections are not part of the stored result core.
	again, err := s.ReadPublishedRound(context.Background(), job.ID, 1)
	if err != nil || again.Round.ResultSHA256 != got.Round.ResultSHA256 {
		t.Fatal("stable result changed with read projection", err)
	}
}

func TestPublishedRoundRefusesMissingGenerationAndUnsettledFacts(t *testing.T) {
	for _, fault := range []string{"generation", "step", "delivery", "output"} {
		t.Run(fault, func(t *testing.T) {
			s, _, accepted := publishedRoundFixture(t)
			if fault == "output" {
				result, err := s.Result(accepted.JobID, 1)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(result.OutputFile, []byte("changed source"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := s.transaction(context.Background(), true, func(db *ledger) error {
				q := db.Jobs[accepted.JobID].Rounds[1]
				switch fault {
				case "generation":
					q.JobGeneration = 0
				case "step":
					q.StepSlot = "foreign"
				case "delivery":
					q.Delivered = false
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			_, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
			if err == nil {
				t.Fatal("unproved broker source was consumed")
			}
			if fault == "generation" && !strings.Contains(err.Error(), "generation") {
				t.Fatalf("wrong generation guard: %v", err)
			}
			if fault == "output" && !strings.Contains(err.Error(), "output changed") {
				t.Fatalf("wrong output guard: %v", err)
			}
		})
	}
}

func TestPublishedRoundKeepsHistoricalGenerationAndRefusesMissingHistory(t *testing.T) {
	s, d, accepted := publishedRoundFixture(t)
	first, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Continue(context.Background(), accepted.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "actual-second-round", ExpectedRound: 1, Prompt: "Write the next genuine report.", OutputPath: filepath.Join(first.Scope.OutputRoot, "implement.v2.md")})
	if err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	second, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 2)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if retained.Round != first.Round || retained.Round.Generation == second.Round.Generation {
		t.Fatal("historical source was reconstructed from current job")
	}
	if err := s.transaction(context.Background(), true, func(db *ledger) error { db.Jobs[accepted.JobID].Rounds[1].JobGeneration = 0; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatalf("missing historical generation was guessed: %v", err)
	}
}

func TestPublishedRoundKeepsHistoricalFactsWhileLaterRoundQueued(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	before, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Continue(context.Background(), accepted.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "later-queued-round", ExpectedRound: 1, Prompt: "Next genuine report.", OutputPath: filepath.Join(before.Scope.OutputRoot, "implement.v2.md")})
	if err != nil {
		t.Fatal(err)
	}
	after, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatalf("later admission erased historical completed source: %v", err)
	}
	if after.Round != before.Round {
		t.Fatal("historical facts changed after later admission")
	}
}

func TestPublishedRoundIgnoresChildAdmittedByLaterRootRound(t *testing.T) {
	s, d, accepted := publishedRoundFixture(t)
	first, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Continue(context.Background(), accepted.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "later-round-with-child", ExpectedRound: 1, Prompt: "Next real round.", OutputPath: filepath.Join(first.Scope.OutputRoot, "implement.v2.md")})
	if err != nil {
		t.Fatal(err)
	}
	child := make(chan string, 1)
	release := make(chan struct{})
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		if spec.Job.ID == accepted.JobID && spec.Job.Round == 2 {
			admitted, err := s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, toolcontract.NestedSubmitInput{RequestKey: "later-child", Component: "reviewer", Prompt: "Review the later round."})
			if err != nil {
				return RunResult{}, err
			}
			child <- admitted.JobID
			select {
			case <-ctx.Done():
				return RunResult{}, ctx.Err()
			case <-release:
			}
		}
		return recordCodexTurn(ctx, spec, sink)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	t.Cleanup(func() { cancel(); close(release); <-done })
	select {
	case <-ctx.Done():
		t.Fatal("real later child was not admitted")
	case id := <-child:
		actual, err := s.Job(id)
		if err != nil || actual.Request.ParentJobID != accepted.JobID || actual.State != "queued" {
			t.Fatalf("not an actual later child: %+v %v", actual, err)
		}
	}
	retained, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatalf("later-round child erased earlier completed source: %v", err)
	}
	if retained.Round != first.Round {
		t.Fatal("later child changed earlier source")
	}
}

// This tests ledger/event serialization. Its private authority does not qualify
// any real check, container, owner activation or publication.
type decisionTestAuthority struct {
	*stepTestAuthority
	calls int
	fail  bool
}

func (a *decisionTestAuthority) ValidateVerificationDecisionLocal(_ context.Context, scope Scope, expected effects.EventRef, next VerificationDecision, source effects.Artifact) error {
	a.calls++
	if a.fail || scope != a.scope.Authorization() || next.Entry != expected || source.SHA256 == (effects.Digest{}) {
		return ErrAuthority
	}
	return nil
}
func (a *decisionTestAuthority) ValidateRevisionPhaseLocal(_ context.Context, scope Scope, expected effects.EventRef, next RevisionPhase, source effects.Artifact) error {
	a.calls++
	if a.fail || scope != a.scope.Authorization() || expected.OwnerID != scope.WorkID || source.SHA256 == (effects.Digest{}) {
		return ErrAuthority
	}
	return nil
}
func decisionFixture(t *testing.T) (Service, RunnerDeps, *decisionTestAuthority, VerificationDecision) {
	t.Helper()
	s, d, req, base := setupStep(t)
	a := &decisionTestAuthority{stepTestAuthority: base}
	s.Authority, d.Service.Authority = a, a
	b, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	b.Components["verifier"] = blueprint.Component{RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "verify.v<n>.md", Headings: []string{"## Result"}}}}
	raw, _ := yaml.Marshal(b)
	if err := os.WriteFile(blueprint.Path(s.Brain), raw, 0600); err != nil {
		t.Fatal(err)
	}
	selected, err := executor.ReadSelection(s.Brain, "epic1")
	if err != nil {
		t.Fatal(err)
	}
	selected.Routes["verifier"] = selected.Routes["implementor"]
	selected.Profile.Routes["verifier"] = selected.Profile.Routes["implementor"]
	raw, _ = json.Marshal(selected)
	if err := os.WriteFile(filepath.Join(store.Factory(s.Brain), "executors", "epic1", "selection.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	path, err := scopeEventsPath(s.Brain, a.scope)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := events.Append(path, events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "verifying"})
	if err != nil {
		t.Fatal(err)
	}
	a.step(StepBasis{EntryRef: "work1#1"})
	req.Component, req.OutputPath = "verifier", filepath.Join(a.scope.OutputRoot, "verify.v1.md")
	accepted := mustSubmit(t, s, req)
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		if err := os.WriteFile(spec.OutputPath, []byte("## Result\nActual broker output.\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordCodexTurn(ctx, spec, sink)
	}}
	runBounded(t, d)
	actual, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	log, err := events.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	var end events.Event
	for _, ev := range log {
		if ev.Kind == events.KindEnd {
			end = ev
		}
	}
	return s, d, a, VerificationDecision{Version: 2, Work: actual.Scope, Entry: programmeRef("work1", entry), End: programmeRef("work1", end), Round: actual.Round, Outcome: "checks_failed", Candidate: effects.Artifact{Path: "/private-fixture/candidate", SHA256: sha256.Sum256([]byte("candidate"))}, Checks: effects.Artifact{Path: "/private-fixture/checks", SHA256: sha256.Sum256([]byte("checks"))}}
}
func programmeBodyFixture(t *testing.T, s Service, scope Scope, name string, body any) effects.Artifact {
	t.Helper()
	dir := filepath.Join(store.Factory(s.Brain), "publication", scope.WorkID, "fixture")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return effects.Artifact{Path: path, SHA256: sha256.Sum256(raw)}
}
func TestVerificationDecisionConsumesCurrentActualRoundAndReplaysLostReply(t *testing.T) {
	s, _, a, next := decisionFixture(t)
	source := programmeBodyFixture(t, s, next.Work, "decision.json", next)
	ref, err := s.AppendVerificationDecision(context.Background(), next.Entry, next, source)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Sequence <= next.End.Sequence || a.calls != 1 {
		t.Fatal("did not consume actual completed source once")
	}
	// Existing decisions remain facts after the current helper projection changes.
	_, err = s.Continue(context.Background(), next.Round.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "later", ExpectedRound: 1, Prompt: "Later actual report.", OutputPath: filepath.Join(next.Work.OutputRoot, "verify.v2.md")})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.AppendVerificationDecision(context.Background(), next.Entry, next, source)
	if err != nil || got != ref || a.calls != 1 {
		t.Fatalf("lost reply replay changed source: %+v %v calls%d", got, err, a.calls)
	}
	changed := next
	changed.Outcome = "passed"
	changed.Verified = &next.Candidate
	other := programmeBodyFixture(t, s, next.Work, "different-decision.json", changed)
	if _, err := s.AppendVerificationDecision(context.Background(), next.Entry, changed, other); !errors.Is(err, ErrConflict) {
		t.Fatalf("decision key accepted a different body: %v", err)
	}
}
func TestVerificationDecisionRefusesRoundThatContinuedAfterPreflight(t *testing.T) {
	s, _, a, next := decisionFixture(t)
	source := programmeBodyFixture(t, s, next.Work, "decision.json", next)
	_, err := s.Continue(context.Background(), next.Round.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "wins-before-decision", ExpectedRound: 1, Prompt: "Later genuine report.", OutputPath: filepath.Join(next.Work.OutputRoot, "verify.v2.md")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadPublishedRound(context.Background(), next.Round.JobID, 1); err != nil {
		t.Fatal("historical factual read should remain available", err)
	}
	if _, err := s.AppendVerificationDecision(context.Background(), next.Entry, next, source); !errors.Is(err, ErrConflict) || a.calls != 0 {
		t.Fatalf("stale source consumed after real Continue: %v calls%d", err, a.calls)
	}
}
func TestVerificationDecisionRequiresTypedAuthorityAndExactProgrammeSource(t *testing.T) {
	for _, fault := range []string{"authority_missing", "authority_denied", "source_changed", "foreign_scope", "wrong_generation", "wrong_end", "wrong_end_event"} {
		t.Run(fault, func(t *testing.T) {
			s, _, a, next := decisionFixture(t)
			switch fault {
			case "authority_missing":
				s.Authority = a.stepTestAuthority
			case "authority_denied":
				a.fail = true
			case "foreign_scope":
				next.Work.Generation = "foreign"
			case "wrong_generation":
				next.Round.Generation++
			case "wrong_end":
				next.End.SHA256 = sha256.Sum256([]byte("foreign"))
			case "wrong_end_event":
				path, err := scopeEventsPath(s.Brain, next.Work)
				if err != nil {
					t.Fatal(err)
				}
				log, err := events.Read(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, ev := range log {
					if ev.Kind == events.KindStart {
						next.End = programmeRef(next.Work.WorkID, ev)
					}
				}
			}
			source := programmeBodyFixture(t, s, next.Work, "decision.json", next)
			if fault == "source_changed" {
				if err := os.WriteFile(source.Path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.AppendVerificationDecision(context.Background(), next.Entry, next, source); err == nil {
				t.Fatal("unproved decision consumed")
			}
		})
	}
}

func TestRevisionPhaseSeedAndActualReportUseExistingLedgerAndEventChain(t *testing.T) {
	s, d, req, base := setupStep(t)
	a := &decisionTestAuthority{stepTestAuthority: base}
	s.Authority, d.Service.Authority = a, a
	path, err := scopeEventsPath(s.Brain, a.scope)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := events.Append(path, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "revision-pass", Request: "select", Key: "actual-selected-pass"})
	if err != nil {
		t.Fatal(err)
	}
	selected := programmeRef("work1", pass)
	seed := RevisionPhase{Version: 2, Pass: selected, Work: a.scope.Authorization(), Role: "implementor", Cause: "selected"}
	source := programmeBodyFixture(t, s, seed.Work, "seed.json", seed)
	first, err := s.AppendRevisionPhase(context.Background(), selected, seed, source)
	if err != nil {
		t.Fatal(err)
	}
	a.step(StepBasis{EntryRef: fmt.Sprintf("work1#%d", first.Sequence)})
	req.Component, req.OutputPath = "implementor", filepath.Join(a.scope.OutputRoot, "implement.v1.md")
	accepted := mustSubmit(t, s, req)
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		if err := os.WriteFile(spec.OutputPath, []byte("## Result\nFresh actual report.\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordCodexTurn(ctx, spec, sink)
	}}
	runBounded(t, d)
	round, err := s.ReadPublishedRound(context.Background(), accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	next := RevisionPhase{Version: 2, Pass: selected, Work: round.Scope, Previous: &first, Role: "verifier", Cause: "report", Round: &round.Round}
	report := programmeBodyFixture(t, s, next.Work, "report.json", next)
	second, err := s.AppendRevisionPhase(context.Background(), first, next, report)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.AppendRevisionPhase(context.Background(), first, next, report)
	if err != nil || again != second {
		t.Fatal("phase replay changed exact source", err)
	}
	changed := next
	changed.Role = "implementor"
	changed.Cause = "verify_failed"
	foreign := programmeBodyFixture(t, s, next.Work, "changed.json", changed)
	if _, err := s.AppendRevisionPhase(context.Background(), first, changed, foreign); !errors.Is(err, ErrConflict) {
		t.Fatalf("phase child changed after commit: %v", err)
	}
	stale := next
	stale.Previous = &selected
	staleSource := programmeBodyFixture(t, s, next.Work, "stale.json", stale)
	if _, err := s.AppendRevisionPhase(context.Background(), selected, stale, staleSource); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale phase tip accepted: %v", err)
	}
}
