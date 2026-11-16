package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestWorkControlResultLockedReadsActualPublishedRoundWithoutReentry(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	want, err := s.Result(accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want.State != "completed" || want.OutputFile == "" || want.Recovery == nil || !want.Recovery.Stopped || !want.Recovery.PublicationSettled {
		t.Fatalf("ordinary producer did not publish a stopped round: %+v", want)
	}
	before, err := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.transaction(context.Background(), false, func(db *ledger) error {
		raw, err := json.Marshal(db)
		if err != nil {
			return err
		}
		got, err := s.resultLocked(db, accepted.JobID, 1)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("held-ledger result differs from ordinary result: got=%+v want=%+v", got, want)
		}
		if db.Jobs[accepted.JobID].Rounds[1].Result.Recovery != nil {
			t.Fatal("read projection changed the retained result")
		}
		if _, err := s.resultLocked(db, accepted.JobID, 2); !errors.Is(err, ErrNotFound) || err.Error() != "helper: not found: round" {
			t.Fatalf("missing round changed its error: %v", err)
		}
		after, err := json.Marshal(db)
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, after) {
			t.Fatal("held-ledger result mutated retained facts")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("result lookup rewrote the ledger", err)
	}
}

func TestWorkControlActualReleasedRoundRetainsUnpublishedFrontier(t *testing.T) {
	actualWorkControlNested(t)
}

func actualWorkControlNested(t *testing.T) (Service, RunnerDeps, Scope, Accepted) {
	t.Helper()
	s, d, request, authority := setupStep(t)
	request.Component = "implementor"
	request.OutputPath = filepath.Join(authority.scope.OutputRoot, "implement.v1.md")
	root := mustSubmit(t, s, request)
	var child Accepted
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		result, err := recordCodexTurn(ctx, spec, sink)
		if err != nil {
			return result, err
		}
		if spec.Job.ID == root.JobID && spec.Job.Round == 1 {
			child, err = s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, toolcontract.NestedSubmitInput{RequestKey: "cohort-nested-review", Component: "reviewer", Prompt: "Review the parent's result."})
			if err != nil {
				return result, err
			}
			return result, s.Yield(ctx, root.JobID, []string{child.JobID})
		}
		if spec.OutputPath != "" {
			err = os.WriteFile(spec.OutputPath, []byte("## Result\nActual resumed parent report.\n"), 0600)
		}
		return result, err
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Run(ctx, d); err != nil {
		raw, readErr := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
		t.Fatalf("actual nested runner: %v; ledger read=%v facts=%s", err, readErr, raw)
	}
	if child.JobID == "" {
		t.Fatal("ordinary producer did not submit its child")
	}
	if err := s.transaction(context.Background(), false, func(db *ledger) error {
		parent := db.Jobs[root.JobID]
		if parent == nil || parent.Job.Round != 2 || parent.Job.State != "completed" {
			t.Fatalf("ordinary parent continuation did not complete: %+v", parent)
		}
		first, second := parent.Rounds[1], parent.Rounds[2]
		childRecord := db.Jobs[child.JobID]
		if first == nil || second == nil || childRecord == nil {
			t.Fatal("ordinary continuation lost retained rounds")
		}
		cq := childRecord.Rounds[childRecord.Job.Round]
		if first.JobGeneration != 1 || second.JobGeneration != 2 || cq.JobGeneration != 1 {
			t.Fatalf("actual producer did not retain typed generations: %d/%d/%d", first.JobGeneration, second.JobGeneration, cq.JobGeneration)
		}
		if first.Result.State != "waiting_children" || !first.Yield || !first.Stopped || first.Published || first.Delivered || first.Result.OutputFile != "" {
			t.Fatalf("released frontier was rewritten as published: %+v", first)
		}
		if !cq.Stopped || !cq.Published || !cq.Delivered || cq.Result.State != "completed" || !second.Published || !second.Delivered || !second.Stopped {
			t.Fatal("child or resumed parent publication did not complete")
		}
		wantPrompt := "Continue after your submitted children. Use these immutable result references:\n" +
			fmt.Sprintf("job=%s round=1 state=completed reply=%s sha256=%s\n", child.JobID, cq.Result.ReplyFile, cq.Result.ReplySHA256) +
			"\n\nAuthoritative output path: " + request.OutputPath
		if second.Prompt != wantPrompt || second.Output != first.Output || second.Output != request.OutputPath {
			t.Fatal("ordinary continuation did not bind exact child references and output")
		}
		old, err := s.resultLocked(db, root.JobID, 1)
		if err != nil {
			return err
		}
		current, err := s.resultLocked(db, root.JobID, 2)
		if err != nil {
			return err
		}
		if old.Recovery == nil || old.Recovery.PublicationSettled || !old.Recovery.Stopped || current.Recovery == nil || !current.Recovery.PublicationSettled {
			t.Fatalf("actual Result projection erased the frontier distinction: old=%+v current=%+v", old.Recovery, current.Recovery)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return s, d, authority.scope.Authorization(), root
}

func closeWorkControlCohort(t *testing.T, s Service, scope Scope) ClosedScopeFenceRef {
	t.Helper()
	if err := s.Fence(context.Background(), scope.WorkID, "UNIT complete cohort"); err != nil {
		t.Fatal(err)
	}
	ref, err := s.ReadClosedScopeFence(context.Background(), scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func requireWorkControlCapture(t *testing.T, s Service, scope Scope, ref ClosedScopeFenceRef) WorkControlCohort {
	t.Helper()
	before := rootRoundLedger(t, s)
	got, err := s.CaptureWorkControlCohort(context.Background(), scope, ref)
	if err != nil || len(got.PendingFiles) != 0 || len(got.PendingPublication) != 0 {
		t.Fatalf("actual complete capture: %+v %v", got, err)
	}
	if !bytes.Equal(before, rootRoundLedger(t, s)) {
		t.Fatal("capture changed ledger facts")
	}
	if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got.Membership); err != nil {
		t.Fatal("actual retained cohort refused", err)
	}
	return got.Membership
}

func TestWorkControlCohortActualPublishedRoundAndRetainedReopen(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	job, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	scope := job.Scope.Authorization()
	ref := closeWorkControlCohort(t, s, scope)
	got := requireWorkControlCapture(t, s, scope, ref)
	if len(got.Rounds) != 1 || len(got.Files) != 0 || got.Rounds[0].JobID != job.ID || got.Rounds[0].Generation != 1 {
		t.Fatalf("incomplete actual membership: %+v", got)
	}
	var p WorkControlRoundProof
	if err := readAdmissionArtifact(got.Rounds[0].Proof.Path, got.Rounds[0].Proof, &p); err != nil || p.Publication.Sequence == 0 || p.End == nil || p.Output == nil || p.Frontier != nil {
		t.Fatalf("genuine event/output references missing: %+v %v", p, err)
	}
	again := requireWorkControlCapture(t, s, scope, ref)
	if !reflect.DeepEqual(got, again) {
		t.Fatal("unchanged capture changed content-addressed references")
	}
	authority := s.Authority.(*stepTestAuthority)
	authority.scope.Generation = "g2"
	if err := s.Reopen(context.Background(), scope.WorkID, "g2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureWorkControlCohort(context.Background(), scope, ref); !errors.Is(err, ErrAuthority) {
		t.Fatal("old closed receipt granted new capture after reopen", err)
	}
	request := job.Request
	request.RequestKey, request.OutputPath = "new-generation-report", filepath.Join(scope.OutputRoot, "implement.v2.md")
	mustSubmit(t, s, request)
	if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got); err != nil {
		t.Fatal("later genuine admission invalidated retained facts", err)
	}
}

func TestWorkControlCohortActualNestedFrontierComplete(t *testing.T) {
	s, _, scope, root := actualWorkControlNested(t)
	ref := closeWorkControlCohort(t, s, scope)
	got := requireWorkControlCapture(t, s, scope, ref)
	if len(got.Rounds) != 3 {
		t.Fatalf("nested rounds omitted: %+v", got.Rounds)
	}
	for _, round := range got.Rounds {
		var p WorkControlRoundProof
		if err := readAdmissionArtifact(round.Proof.Path, round.Proof, &p); err != nil {
			t.Fatal(err)
		}
		if round.JobID == root.JobID && round.Round == 1 {
			if p.Frontier == nil || p.Publication != (effects.EventRef{}) || p.End != nil || p.Output != nil || p.Frontier.Successor.JobID != root.JobID || p.Frontier.Successor.Round != 2 || len(p.Frontier.Children) != 1 {
				t.Fatalf("released frontier fabricated publication or lost linkage: %+v", p)
			}
			if p.Frontier.Children[0].Proof.Path == "" || p.Frontier.Successor.Proof.Path == "" {
				t.Fatal("frontier links do not bind actual immutable proofs")
			}
		} else if p.Frontier != nil || p.Publication.Sequence == 0 {
			t.Fatal("published successor/child lost actual publication")
		}
	}
}

func TestWorkControlCohortActualCancelledPublicationMover(t *testing.T) {
	s, d, request, authority := setupStep(t)
	accepted := mustSubmit(t, s, request)
	scope := authority.scope.Authorization()
	old := closeWorkControlCohort(t, s, scope)
	cut, err := s.CaptureWorkControlCohort(context.Background(), scope, old)
	if err != nil || len(cut.PendingPublication) != 1 || cut.PendingPublication[0].JobID != accepted.JobID || len(cut.Membership.Rounds) != 0 {
		t.Fatalf("actual cancelled unpublished round hidden: %+v %v", cut, err)
	}
	if err := flush(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureWorkControlCohort(context.Background(), scope, old); !errors.Is(err, ErrAuthority) {
		t.Fatal("stale false-publication snapshot accepted for new capture", err)
	}
	current, err := s.ReadClosedScopeFence(context.Background(), scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	got := requireWorkControlCapture(t, s, scope, current)
	var p WorkControlRoundProof
	if len(got.Rounds) != 1 {
		t.Fatal(got)
	}
	if err := readAdmissionArtifact(got.Rounds[0].Proof.Path, got.Rounds[0].Proof, &p); err != nil || p.End != nil || p.Output != nil || p.Publication.Sequence == 0 {
		t.Fatal("cancelled round got a synthetic End/output", p, err)
	}
}

func TestWorkControlCohortActualOutputCommitBeforeLedgerAdoption(t *testing.T) {
	s, scope, prepared := pendingClosedOutputFixture(t)
	cut := s
	cut.testCheckpoint = func(at string) {
		if at == "control_output_receipt_published" {
			panic("actual commit before ledger adoption")
		}
	}
	func() {
		defer func() {
			if recover() != "actual commit before ledger adoption" {
				t.Fatal("did not cut after actual commit publication")
			}
		}()
		_, _ = cut.CommitControlOutputLocked(context.Background(), prepared)
	}()
	admitted, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	var pending *PendingControlOutputsError
	if _, err := s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, admitted.Token); !errors.As(err, &pending) || len(pending.Outputs) != 1 || pending.Outputs[0] != prepared {
		t.Fatal("unacknowledged real commit did not remain pending", err)
	}
	if _, err := s.CaptureWorkControlCohort(context.Background(), scope, ClosedScopeFenceRef{}); err == nil {
		t.Fatal("capture accepted an invented preclosure receipt")
	}
	_, _, proof, err := s.ReadControlOutputCommitLocal(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AdoptControlOutputCommitLocked(context.Background(), prepared, proof); err != nil {
		t.Fatal(err)
	}
	ref, err := s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, admitted.Token)
	if err != nil {
		t.Fatal(err)
	}
	got := requireWorkControlCapture(t, s, scope, ref)
	if len(got.Rounds) != 0 || len(got.Files) != 1 || got.Files[0] != (WorkControlFile{Prepared: prepared, Commit: proof}) {
		t.Fatalf("actual file history missing: %+v", got)
	}
	before := rootRoundLedger(t, s)
	name, _ := controlOutputPath(s.Brain, prepared.ID, "commit.json")
	if err := os.WriteFile(name, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got); err == nil {
		t.Fatal("changed actual commit accepted")
	}
	if !bytes.Equal(before, rootRoundLedger(t, s)) {
		t.Fatal("factual failure adopted or rewrote history")
	}
}

func TestWorkControlCohortMissingGenerationAndHistoricalTamperingHold(t *testing.T) {
	for _, fault := range []string{"generation", "round_digest", "proof_hash", "missing_member", "output", "message"} {
		t.Run(fault, func(t *testing.T) {
			s, _, accepted := publishedRoundFixture(t)
			job, _ := s.Job(accepted.JobID)
			scope := job.Scope.Authorization()
			ref := closeWorkControlCohort(t, s, scope)
			got := requireWorkControlCapture(t, s, scope, ref)
			switch fault {
			case "generation", "round_digest":
				if err := s.transaction(context.Background(), true, func(db *ledger) error {
					q := db.Jobs[accepted.JobID].Rounds[1]
					if fault == "generation" {
						q.JobGeneration = 0
					} else {
						q.Prompt += " changed"
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "proof_hash":
				got.Rounds[0].Proof.SHA256[0] ^= 1
			case "missing_member":
				got.Rounds = nil
			case "output":
				if err := os.WriteFile(job.Request.OutputPath, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "message":
				path, _ := entityPath(s.Brain, scope.OwnerID, scope.EpicID)
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got); err == nil {
				t.Fatal("changed retained evidence accepted", fault)
			}
			if fault == "generation" {
				if _, err := s.CaptureWorkControlCohort(context.Background(), scope, ref); err == nil || !strings.Contains(err.Error(), "generation") {
					t.Fatal("missing generation was guessed", err)
				}
			}
		})
	}
}

func TestWorkControlCohortActualEmptyAndMissingLedger(t *testing.T) {
	s, scope, token := closedOutputFixture(t)
	ref, err := s.ReadClosedControlOutputFence(context.Background(), scope, token)
	if err != nil {
		t.Fatal(err)
	}
	got := requireWorkControlCapture(t, s, scope, ref)
	if len(got.Rounds) != 0 || len(got.Files) != 0 {
		t.Fatal("empty initialized cohort fabricated members", got)
	}
	if err := os.Remove(filepath.Join(s.root(), "ledger.json")); err != nil {
		t.Fatal(err)
	}
	if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing ledger was called complete empty", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ReadWorkControlCohortLocal(cancelled, s.Brain, got); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled retained read proceeded", err)
	}
}

func TestWorkControlCohortActualRepeatedAndRecoveredFrontiers(t *testing.T) {
	for _, mode := range []string{"yield", "recovered"} {
		t.Run(mode, func(t *testing.T) {
			s, d, request, authority := setupStep(t)
			root := mustSubmit(t, s, request)
			backend := &releaseTestBackend{}
			backend.allow.Store(true)
			backend.run = func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
				if spec.Job.ID == root.JobID && spec.Job.Round <= 2 {
					child, err := s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, toolcontract.NestedSubmitInput{RequestKey: fmt.Sprintf("cohort-child-%d", spec.Job.Round), Component: "reviewer", Prompt: "Review this round."})
					if err != nil {
						return RunResult{}, err
					}
					if mode == "yield" {
						if err := s.Yield(ctx, spec.Job.ID, []string{child.JobID}); err != nil {
							return RunResult{}, err
						}
					}
				}
				return recordCodexTurn(ctx, spec, sink)
			}
			backend.inspect = func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, bool, error) {
				result, err := recordCodexTurn(ctx, spec, sink)
				return result, err == nil, err
			}
			d.Backends["codex"] = backend
			clock := &schedulingClock{at: time.Now()}
			d.Now = clock.Now
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			done := make(chan error, 1)
			go func() { defer close(done); done <- Run(ctx, d) }()
			defer func() { cancel(); <-done }()
			var runErr error
			eventually(t, func() bool {
				clock.Advance(time.Second)
				select {
				case runErr = <-done:
					return true
				default:
					return false
				}
			})
			if runErr != nil {
				var facts []string
				readErr := s.transaction(context.Background(), false, func(db *ledger) error {
					for _, r := range db.Jobs {
						q := r.Rounds[r.Job.Round]
						facts = append(facts, fmt.Sprintf("job=%s key=%s gen=%d round=%d state=%s hold=%s phase=%s stopped=%v ended=%v published=%v delivered=%v children=%v released=%v", r.Job.ID, r.Job.Request.RequestKey, r.Job.Generation, r.Job.Round, r.Job.State, r.Job.HoldReason, q.Phase, q.Stopped, q.Ended, q.Published, q.Delivered, q.Children, q.ChildRelease != nil))
					}
					return nil
				})
				t.Fatalf("actual repeated runner: %v observations=%d read=%v facts=%v", runErr, backend.observations.Load(), readErr, facts)
			}
			job, err := s.Job(root.JobID)
			if err != nil || job.Round != 3 || job.State != "completed" {
				t.Fatal("ordinary repeated frontier did not complete", job, err)
			}
			scope := authority.scope.Authorization()
			got := requireWorkControlCapture(t, s, scope, closeWorkControlCohort(t, s, scope))
			if len(got.Rounds) != 5 {
				t.Fatalf("repeated frontier rounds omitted: %+v", got.Rounds)
			}
			if mode == "recovered" {
				if backend.observations.Load() < 2 {
					t.Fatal("recovered fixture bypassed actual workspace guard calls")
				}
				if err := s.transaction(context.Background(), false, func(db *ledger) error {
					for n := 1; n <= 2; n++ {
						q := db.Jobs[root.JobID].Rounds[n]
						if q.Yield || q.ChildRelease == nil || !validChildReleaseAtGeneration(db.Jobs[root.JobID], n, q, q.JobGeneration) {
							t.Fatal("real release mover did not retain the guarded receipt")
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := s.transaction(context.Background(), true, func(db *ledger) error {
					db.Jobs[root.JobID].Rounds[1].ChildRelease.FrontierSHA256 = strings.Repeat("0", 64)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.CaptureWorkControlCohort(context.Background(), scope, got.Fence); err == nil {
					t.Fatal("changed recovered release receipt was accepted")
				}
			}
		})
	}
}

func TestWorkControlCohortFrontierPromptAndReceiptGuards(t *testing.T) {
	for _, fault := range []string{"trailing_prompt", "child_reference", "output", "release"} {
		t.Run(fault, func(t *testing.T) {
			s, _, scope, root := actualWorkControlNested(t)
			ref := closeWorkControlCohort(t, s, scope)
			if err := s.transaction(context.Background(), true, func(db *ledger) error {
				r := db.Jobs[root.JobID]
				switch fault {
				case "trailing_prompt":
					r.Rounds[2].Prompt += "\nUnrelated continuation."
				case "child_reference":
					r.Rounds[2].Prompt = strings.Replace(r.Rounds[2].Prompt, " round=1 ", " round=99 ", 1)
				case "output":
					r.Rounds[1].Output = filepath.Join(scope.OutputRoot, "foreign.md")
				case "release":
					r.Rounds[1].Yield = false
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CaptureWorkControlCohort(context.Background(), scope, ref); err == nil {
				t.Fatal("unproved frontier was called settled", fault)
			}
		})
	}
}

func TestWorkControlCohortHistoricalGenerationCannotExceedCurrent(t *testing.T) {
	s, d, accepted := publishedRoundFixture(t)
	job, _ := s.Job(accepted.JobID)
	_, err := s.Continue(context.Background(), job.ID, ContinueRequest{SchemaVersion: 1, RequestKey: "cohort-round-two", ExpectedRound: 1, Prompt: "A fresh actual report.", OutputPath: filepath.Join(job.Scope.OutputRoot, "implement.v2.md")})
	if err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	scope := job.Scope.Authorization()
	ref := closeWorkControlCohort(t, s, scope)
	if len(requireWorkControlCapture(t, s, scope, ref).Rounds) != 2 {
		t.Fatal("actual retained continuation round omitted")
	}
	if err := s.transaction(context.Background(), true, func(db *ledger) error {
		db.Jobs[job.ID].Rounds[1].JobGeneration = 3
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureWorkControlCohort(context.Background(), scope, ref); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatal("impossible historical generation accepted", err)
	}
}

func TestWorkControlCohortSiblingWritesDoNotChangeRelevantMembership(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	job, _ := s.Job(accepted.JobID)
	scope := job.Scope.Authorization()
	ref := closeWorkControlCohort(t, s, scope)
	before := requireWorkControlCapture(t, s, scope, ref)
	ix, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.Brain, "factory", "work2")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	ix["work2"] = store.Entry{Kind: "work", Dir: dir, Epic: scope.EpicID}
	if err := store.WriteIndex(s.Brain, ix); err != nil {
		t.Fatal(err)
	}
	request := job.Request
	request.WorkID, request.Component, request.OutputPath, request.RequestKey = "work2", "reviewer", "", "actual-sibling"
	mustSubmit(t, s, request)
	after := requireWorkControlCapture(t, s, scope, ref)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("unrelated queued sibling changed relevant membership")
	}
}

func TestWorkControlCohortActualFailedOutputHasNoSyntheticEnd(t *testing.T) {
	s, d, request, authority := setupStep(t)
	request.Component = "implementor"
	request.OutputPath = filepath.Join(authority.scope.OutputRoot, "implement.v1.md")
	accepted := mustSubmit(t, s, request)
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		if err := os.WriteFile(spec.OutputPath, []byte("Missing the required heading."), 0600); err != nil {
			return RunResult{}, err
		}
		return recordCodexTurn(ctx, spec, sink)
	}}
	runBounded(t, d)
	result, err := s.Result(accepted.JobID, 1)
	if err != nil || result.State != "failed" || result.ErrorCode != "output_validation_failed" || result.OutputFile != "" {
		t.Fatal("ordinary validator did not produce the intended failure", result, err)
	}
	scope := authority.scope.Authorization()
	got := requireWorkControlCapture(t, s, scope, closeWorkControlCohort(t, s, scope))
	var p WorkControlRoundProof
	if len(got.Rounds) != 1 {
		t.Fatal(got)
	}
	if err := readAdmissionArtifact(got.Rounds[0].Proof.Path, got.Rounds[0].Proof, &p); err != nil || p.End != nil || p.Output != nil || p.Publication.Sequence == 0 {
		t.Fatal("failed output got an invented successful End", p, err)
	}
}

func TestWorkControlCohortProofPathAndCurrentScopeAreExact(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	job, _ := s.Job(accepted.JobID)
	scope := job.Scope.Authorization()
	ref := closeWorkControlCohort(t, s, scope)
	got := requireWorkControlCapture(t, s, scope, ref)
	for _, fault := range []string{"owner", "generation", "work", "lease"} {
		t.Run(fault, func(t *testing.T) {
			wrong := scope
			switch fault {
			case "owner":
				wrong.OwnerID = "foreign"
			case "generation":
				wrong.Generation = "foreign"
			case "work":
				wrong.WorkID = "foreign"
			case "lease":
				wrong.SetID, wrong.Lease = "foreign", 1
			}
			if _, err := s.CaptureWorkControlCohort(context.Background(), wrong, ref); !errors.Is(err, ErrAuthority) {
				t.Fatal("wrong scope accepted for new capture", err)
			}
		})
	}
	round := got.Rounds[0]
	data, err := os.ReadFile(round.Proof.Path)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(round.Proof.Path), "other-proof.json")
	if err := os.WriteFile(alias, data, 0600); err != nil {
		t.Fatal(err)
	}
	got.Rounds[0].Proof.Path = alias
	if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got); !errors.Is(err, ErrConflict) {
		t.Fatal("same bytes at a foreign proof path accepted", err)
	}
}

func TestWorkControlCohortMissingPublicationCannotMintProof(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	job, _ := s.Job(accepted.JobID)
	scope := job.Scope.Authorization()
	ref := closeWorkControlCohort(t, s, scope)
	requireWorkControlCapture(t, s, scope, ref)
	path, err := entityPath(s.Brain, scope.OwnerID, scope.EpicID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureWorkControlCohort(context.Background(), scope, ref); !errors.Is(err, ErrConflict) {
		t.Fatal("missing genuine publication was accepted for new proof", err)
	}
}

func TestWorkControlCohortPreservesExistingLargeLedgerCodec(t *testing.T) {
	s, _, accepted := publishedRoundFixture(t)
	job, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	scope := job.Scope.Authorization()
	ref := closeWorkControlCohort(t, s, scope)
	requireWorkControlCapture(t, s, scope, ref)
	name := filepath.Join(s.root(), "ledger.json")
	actual := rootRoundLedger(t, s)
	padded := append(append([]byte(nil), actual...), bytes.Repeat([]byte(" "), (16<<20)+1)...)
	if err := os.WriteFile(name, padded, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(accepted.JobID, 1)
	if err != nil || result.State != "completed" || result.Recovery == nil || !result.Recovery.PublicationSettled {
		t.Fatalf("existing ledger codec rejected lawful whitespace: size=%d result=%+v err=%v", len(padded), result, err)
	}
	got, err := s.CaptureWorkControlCohort(context.Background(), scope, ref)
	if err != nil || len(got.PendingFiles) != 0 || len(got.PendingPublication) != 0 || len(got.Membership.Rounds) != 1 {
		t.Fatalf("existing valid ledger gained an unrelated size limit: size=%d capture=%+v err=%v", len(padded), got, err)
	}
	if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got.Membership); err != nil {
		t.Fatal("retained reader added a ledger bound", err)
	}
	if !bytes.Equal(padded, rootRoundLedger(t, s)) {
		t.Fatal("factual reader normalized or rewrote the existing ledger")
	}
}

func TestWorkControlCohortLedgerMetadataRefusesAliasesAndWrongMode(t *testing.T) {
	for _, fault := range []string{"mode", "symlink", "hardlink", "missing"} {
		t.Run(fault, func(t *testing.T) {
			s, scope, token := closedOutputFixture(t)
			ref, err := s.ReadClosedControlOutputFence(context.Background(), scope, token)
			if err != nil {
				t.Fatal(err)
			}
			got := requireWorkControlCapture(t, s, scope, ref)
			name := filepath.Join(s.root(), "ledger.json")
			switch fault {
			case "mode":
				err = os.Chmod(name, 0644)
			case "symlink":
				err = os.Rename(name, name+".actual")
				if err == nil {
					err = os.Symlink(name+".actual", name)
				}
			case "hardlink":
				err = os.Link(name, name+".alias")
			case "missing":
				err = os.Remove(name)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := ReadWorkControlCohortLocal(context.Background(), s.Brain, got); err == nil {
				t.Fatal("untrusted ledger metadata accepted", fault)
			}
		})
	}
}

func TestWorkControlCohortLedgerCanonicalParentMetadata(t *testing.T) {
	s, _, _ := closedOutputFixture(t)
	if err := requireWorkLedger(s); err != nil {
		t.Fatal("actual initialized private ledger refused", err)
	}
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "brain-alias")
	if err := os.Symlink(s.Brain, alias); err != nil {
		t.Fatal(err)
	}
	s.Brain = alias
	if err := requireWorkLedger(s); !errors.Is(err, ErrConflict) {
		t.Fatal("aliased canonical ledger parent accepted", err)
	}
}
