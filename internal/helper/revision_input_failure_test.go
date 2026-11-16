package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"go.yaml.in/yaml/v3"
)

// This authority isolates broker predicates. Codex tests exercise the real source reader.
type revisionPredicateAuthority struct {
	*stepTestAuthority
	ref    RevisionInputFailureRef
	denied bool
}

func (a revisionPredicateAuthority) ValidateRevisionInputFailureLocal(_ context.Context, j Job, ref RevisionInputFailureRef) error {
	if a.denied || ref != a.ref || j.Request.Component != "verifier" || j.Scope.CodexContract != 2 {
		return ErrAuthority
	}
	return nil
}
func revisionPredicateFixture(t *testing.T) (Service, Job, RunResult, revisionPredicateAuthority) {
	t.Helper()
	s, _, req, a := setupStep(t)
	bp, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	bp.Components["verifier"] = bp.Components[req.Component]
	data, err := yaml.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(s.Brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	selection, err := executor.ReadSelection(s.Brain, a.scope.EpicID)
	if err != nil {
		t.Fatal(err)
	}
	selection.Profile.Routes["verifier"] = selection.Profile.Routes[req.Component]
	selection.Routes["verifier"] = selection.Routes[req.Component]
	data, err = json.Marshal(selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Brain, "factory", "executors", a.scope.EpicID, "selection.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	req.Component = "verifier"
	accepted := mustSubmit(t, s, req)
	if err := s.transaction(context.Background(), true, func(db *ledger) error {
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
	authority := revisionPredicateAuthority{stepTestAuthority: a, ref: RevisionInputFailureRef{Path: filepath.Join(s.Brain, "retained-source-fixture"), SHA256: strings.Repeat("a", 64)}}
	s.Authority = authority
	result := RunResult{Outcome: "failed", ErrorCode: "codex_revision_input_failed", ErrorText: "selected revision input failed", RevisionInputFailure: &authority.ref}
	return s, j, result, authority
}

func TestRevisionFailureBrokerRequiresTypedSourceAndExactNoTurn(t *testing.T) {
	for _, kind := range []string{"valid", "missing-ref", "foreign-ref", "bad-hash", "source-unavailable", "model", "turn", "reply", "wrong-code", "fence", "cancel", "conflict", "run-error"} {
		t.Run(kind, func(t *testing.T) {
			s, j, result, a := revisionPredicateFixture(t)
			var runErr, fenceErr error
			switch kind {
			case "missing-ref":
				result.RevisionInputFailure = nil
			case "foreign-ref":
				ref := a.ref
				ref.Path += "foreign"
				result.RevisionInputFailure = &ref
			case "bad-hash":
				ref := a.ref
				ref.SHA256 = "invalid"
				result.RevisionInputFailure = &ref
			case "source-unavailable":
				a.denied = true
				s.Authority = a
			case "model":
				result.ConfiguredModel = j.Route.Model
			case "turn":
				result.TurnID = "invented"
			case "reply":
				result.Reply = "invented"
			case "wrong-code":
				result.ErrorCode = "codex_prelaunch_failed"
			case "fence":
				fenceErr = errors.New("writer not stopped")
			case "run-error":
				runErr = errors.New("transport uncertain")
			}
			if kind == "cancel" || kind == "conflict" {
				if err := s.transaction(context.Background(), true, func(db *ledger) error {
					r := db.Jobs[j.ID]
					if kind == "cancel" {
						setState(r, "unknown", "cancel_requested")
					} else {
						r.Rounds[1].Conflicted = true
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := finish(context.Background(), s, j, result, runErr, fenceErr, "", true); err != nil {
				t.Fatal(err)
			}
			got, err := s.Result(j.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "valid" {
				if got.State != "failed" || got.ErrorCode != result.ErrorCode || got.RevisionInputFailure == nil || *got.RevisionInputFailure != a.ref || got.Recovery == nil || !got.Recovery.NoLaunch {
					t.Fatalf("actual typed no-turn failure not retained: %+v", got)
				}
				a.denied = true
				s.Authority = a
				after, err := s.Result(j.ID, 1)
				if err != nil || after.Recovery.NoLaunch {
					t.Fatalf("unavailable retained source gained no-launch fact: %+v %v", after, err)
				}
				return
			}
			if got.State != "unknown" || got.RevisionInputFailure != nil || got.Recovery != nil && got.Recovery.NoLaunch {
				t.Fatalf("%s gained failed source authority: %+v", kind, got)
			}
			want := "revision_input_failure_unconfirmed"
			switch kind {
			case "missing-ref":
				want = "inspection_identity_unconfirmed"
			case "fence":
				want = "writer_death_unconfirmed"
			case "cancel":
				want = "cancel_requested"
			case "conflict":
				want = "journal_identity_conflict"
			case "run-error":
				want = "turn_result_unconfirmed"
			}
			if got.ErrorCode != want {
				t.Fatalf("%s reached wrong guard: %s want %s", kind, got.ErrorCode, want)
			}
		})
	}
}

func TestRevisionFailureLaunchedRequiresActualPersistedReplyTuple(t *testing.T) {
	for _, kind := range []string{"valid", "changed-reply", "changed-hash", "changed-turn", "wrong-model", "wrong-effort", "missing-persistence"} {
		t.Run(kind, func(t *testing.T) {
			s, j, failed, a := revisionPredicateFixture(t)
			var sink Sink = journalSink{service: s, job: j}
			if kind == "missing-persistence" {
				sink = skipPersistedSink{sink}
			}
			result, err := recordCodexTurn(context.Background(), RunSpec{Job: j}, sink)
			if err != nil {
				t.Fatal(err)
			}
			result.Outcome = failed.Outcome
			result.ErrorCode = failed.ErrorCode
			result.ErrorText = failed.ErrorText
			result.RevisionInputFailure = &a.ref
			j, err = s.Job(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "changed-reply":
				result.Reply += " changed"
			case "changed-hash":
				result.PersistedReplySHA256 = strings.Repeat("b", 64)
			case "changed-turn":
				result.TurnID = "foreign"
			case "wrong-model":
				result.ConfiguredModel = "foreign"
			case "wrong-effort":
				result.ConfiguredEffort = "low"
			}
			if err := finish(context.Background(), s, j, result, nil, nil, "", true); err != nil {
				t.Fatal(err)
			}
			got, err := s.Result(j.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "valid" {
				if got.State != "failed" || got.RevisionInputFailure == nil || got.PersistedReplySHA256 != result.PersistedReplySHA256 || got.Recovery == nil || got.Recovery.NoLaunch {
					t.Fatalf("real launched tuple not retained: %+v", got)
				}
			} else if got.State != "unknown" || got.ErrorCode != "revision_input_failure_unconfirmed" || got.RevisionInputFailure != nil {
				t.Fatalf("%s bypassed persisted tuple: %+v", kind, got)
			}
		})
	}
}
