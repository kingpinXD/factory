package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// Unlike the normal backend fixture, this intentionally ends a turn without a
// successful save acknowledgment. A conversation identifier is not a save.
func uncheckpointedAttempt(t *testing.T, s Service, req Request) (Job, RunResult) {
	t.Helper()
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return uncheckpointedJob(t, s, a.JobID)
}
func uncheckpointedJob(t *testing.T, s Service, id string) (Job, RunResult) {
	t.Helper()
	ctx := context.Background()
	if err := s.transaction(ctx, true, func(db *ledger) error {
		r := db.Jobs[id]
		r.Rounds[r.Job.Round].Phase = "launch_attempted"
		setState(r, "running", "")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	j, err := s.Job(id)
	if err != nil {
		t.Fatal(err)
	}
	sink := journalSink{service: s, job: j}
	result, err := beginTurn(ctx, RunSpec{Job: j}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.Record(ctx, EventNativeEnded, map[string]string{"turn_id": result.TurnID}); err != nil {
		t.Fatal(err)
	}
	result.Outcome = "completed"
	j, err = s.Job(id)
	if err != nil {
		t.Fatal(err)
	}
	return j, result
}
func acknowledgeCheckpoint(t *testing.T, s Service, j Job, result RunResult) {
	t.Helper()
	if err := (journalSink{service: s, job: j}).Record(context.Background(), EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID}); err != nil {
		t.Fatal(err)
	}
}
func markCompleted(t *testing.T, s Service, j Job, result RunResult) {
	t.Helper()
	if err := s.transaction(context.Background(), true, func(db *ledger) error {
		r := db.Jobs[j.ID]
		q := r.Rounds[r.Job.Round]
		q.Stopped = true
		q.Result.ObservedModel = result.ObservedModel
		setState(r, "completed", "")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestFinishRequiresCheckpointBeforeSuccessOrYield(t *testing.T) {
	for _, kind := range []string{"success", "yield"} {
		t.Run(kind, func(t *testing.T) {
			s, d, req := setup(t)
			req.Component = "implementor"
			req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
			j, result := uncheckpointedAttempt(t, s, req)
			if err := os.WriteFile(req.OutputPath, []byte("## Result\nnot saved\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var childID string
			if kind == "yield" {
				child := req
				child.RequestKey = "child"
				child.Component = "reviewer"
				child.OutputPath = ""
				child.ParentJobID = j.ID
				a, err := s.Submit(context.Background(), child)
				if err != nil {
					t.Fatal(err)
				}
				childID = a.JobID
				if err = s.Yield(context.Background(), j.ID, []string{childID}); err != nil {
					t.Fatal(err)
				}
			}
			if err := finish(context.Background(), s, j, result, nil, nil, "", false); err != nil {
				t.Fatal(err)
			}
			r, err := s.Result(j.ID, 1)
			if err != nil || r.State != "unknown" || r.Outcome != "unknown" {
				t.Errorf("unsaved turn advanced: %+v %v", r, err)
			}
			if childID != "" {
				if err = s.transaction(context.Background(), false, func(db *ledger) error {
					if admissible(db, db.Jobs[childID]) {
						t.Error("child admitted without parent checkpoint")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err = flush(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			ix, _ := store.ReadIndex(s.Brain)
			log, err := events.Read(store.EventsPath(ix["work1"].Dir))
			if err != nil || len(log) != 0 {
				t.Errorf("unsaved output published: %+v %v", log, err)
			}
		})
	}
}
func TestManualContinuationRequiresMatchingCheckpoint(t *testing.T) {
	for _, kind := range []string{"missing", "wrong_turn", "wrong_conversation", "failed_ack", "valid"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := setup(t)
			j, result := uncheckpointedAttempt(t, s, req)
			switch kind {
			case "valid":
				acknowledgeCheckpoint(t, s, j, result)
			case "failed_ack":
				if err := (journalSink{service: s, job: j}).Record(context.Background(), EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID}); err == nil {
					t.Fatal("incomplete save acknowledgment accepted")
				}
			case "wrong_turn", "wrong_conversation":
				if err := s.transaction(context.Background(), true, func(db *ledger) error {
					q := db.Jobs[j.ID].Rounds[1]
					q.Evidence[EventCheckpointSaved+":conversation_id"] = result.ConversationID
					q.Evidence[EventCheckpointSaved+":turn_id"] = result.TurnID
					if kind == "wrong_turn" {
						q.Evidence[EventCheckpointSaved+":turn_id"] = "previous-turn"
					} else {
						q.Evidence[EventCheckpointSaved+":conversation_id"] = "another-conversation"
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			// Emulates an older completed record that lacked the save prerequisite.
			markCompleted(t, s, j, result)
			a, err := s.Continue(context.Background(), j.ID, ContinueRequest{SchemaVersion: 1, RequestKey: "next", ExpectedRound: 1, Prompt: "next round"})
			if kind == "valid" {
				if err != nil || a.Round != 2 {
					t.Fatalf("%+v %v", a, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "checkpoint") {
				t.Fatalf("continued unsaved conversation: %+v %v", a, err)
			}
		})
	}
}
func TestAutomaticContinuationRequiresParentAndChildCheckpoint(t *testing.T) {
	for _, missing := range []string{"parent", "child", "neither"} {
		t.Run(missing, func(t *testing.T) {
			s, _, req := setup(t)
			parent, pr := uncheckpointedAttempt(t, s, req)
			childReq := req
			childReq.RequestKey = "child"
			childReq.ParentJobID = parent.ID
			a, err := s.Submit(context.Background(), childReq)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Yield(context.Background(), parent.ID, []string{a.JobID}); err != nil {
				t.Fatal(err)
			}
			if missing != "parent" {
				acknowledgeCheckpoint(t, s, parent, pr)
			}
			child, cr := uncheckpointedJob(t, s, a.JobID)
			if missing != "child" {
				acknowledgeCheckpoint(t, s, child, cr)
			}
			markCompleted(t, s, child, cr)
			if err = s.transaction(context.Background(), true, func(db *ledger) error {
				p := db.Jobs[parent.ID]
				p.Rounds[1].Stopped = true
				setState(p, "waiting_children", "")
				db.Jobs[child.ID].Rounds[1].Published = true
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = advanceParents(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			p, err := s.Job(parent.ID)
			want := 1
			if missing == "neither" {
				want = 2
			}
			if err != nil || p.Round != want {
				t.Fatalf("missing=%s parent=%+v %v", missing, p, err)
			}
		})
	}
}
func TestAdmissionRequiresSavedParentAndPriorRound(t *testing.T) {
	for _, kind := range []string{"descendant", "continuation"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := setup(t)
			j, result := uncheckpointedAttempt(t, s, req)
			target := j.ID
			if kind == "descendant" {
				child := req
				child.RequestKey = "child"
				child.ParentJobID = j.ID
				a, err := s.Submit(context.Background(), child)
				if err != nil {
					t.Fatal(err)
				}
				target = a.JobID
				if err = s.Yield(context.Background(), j.ID, []string{target}); err != nil {
					t.Fatal(err)
				}
				if err = s.transaction(context.Background(), true, func(db *ledger) error {
					r := db.Jobs[j.ID]
					r.Rounds[1].Stopped = true
					setState(r, "waiting_children", "")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				markCompleted(t, s, j, result)
				if err := s.transaction(context.Background(), true, func(db *ledger) error {
					r := db.Jobs[j.ID]
					r.Job.Round = 2
					r.Job.Generation = 2
					r.Job.TurnID = ""
					r.Job.State = "queued"
					r.Rounds[2] = newRound(r.Job, "next round", "")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.transaction(context.Background(), false, func(db *ledger) error {
				if admissible(db, db.Jobs[target]) {
					t.Error("admitted writer without checkpoint")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPublicationRejectsPreviouslyCompletedUnsavedRound(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
	j, result := uncheckpointedAttempt(t, s, req)
	if err := os.WriteFile(req.OutputPath, []byte("## Result\nunsaved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	markCompleted(t, s, j, result)
	if err := s.transaction(context.Background(), true, func(db *ledger) error { return validateOutput(db, db.Jobs[j.ID]) }); err != nil {
		t.Fatal(err)
	}
	if err := flush(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	r, err := s.Result(j.ID, 1)
	if err != nil || r.State != "unknown" {
		t.Errorf("unsaved completed record accepted: %+v %v", r, err)
	}
	ix, _ := store.ReadIndex(s.Brain)
	log, err := events.Read(store.EventsPath(ix["work1"].Dir))
	if err != nil || len(log) != 0 {
		t.Errorf("unsaved end event: %+v %v", log, err)
	}
}
func TestInspectCompletionRequiresCheckpointAcknowledgment(t *testing.T) {
	for _, kind := range []string{"missing", "failed"} {
		t.Run(kind, func(t *testing.T) {
			s, d, req := setup(t)
			j, result := uncheckpointedAttempt(t, s, req)
			d.Backends["zed"] = backendFunc{inspect: func(ctx context.Context, _ RunSpec, k Sink) (RunResult, bool, error) {
				if kind == "failed" {
					if err := k.Record(ctx, EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID}); err == nil {
						t.Error("failed save accepted")
					}
				}
				return result, true, nil
			}}
			if err := recoverAttempts(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			r, err := s.Result(j.ID, 1)
			if err != nil || r.State != "unknown" {
				t.Fatalf("inspector fabricated saved completion: %+v %v", r, err)
			}
		})
	}
}
func TestParentReusesCompletedReviewerAcrossYieldRoundsPoolOne(t *testing.T) {
	s, d, req := setup(t)
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var active, maxActive atomic.Int32
	var childID, childConversation, parentConversation string
	parentRuns, childRuns := 0, 0
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		n := active.Add(1)
		defer active.Add(-1)
		if n != 1 {
			t.Errorf("pool of one admitted %d simultaneous turns", n)
		}
		if n > maxActive.Load() {
			maxActive.Store(n)
		}
		result, e := beginTurn(ctx, spec, k)
		if e != nil {
			return result, e
		}
		result.Outcome = "completed"
		if spec.Job.ID == root.JobID {
			parentRuns++
			if parentConversation == "" {
				parentConversation = result.ConversationID
			} else if result.ConversationID != parentConversation {
				return result, errors.New("parent conversation changed")
			}
			if spec.Job.Round == 1 {
				child := req
				child.RequestKey = "reviewer"
				child.ParentJobID = root.JobID
				child.Prompt = "review first version"
				a, e := s.Submit(ctx, child)
				if e != nil {
					return result, e
				}
				childID = a.JobID
			}
			if spec.Job.Round == 2 {
				cr := ContinueRequest{SchemaVersion: 1, RequestKey: "review-again", ExpectedRound: 1, Prompt: "review revised version"}
				a, e := s.Continue(ctx, childID, cr)
				if e != nil {
					return result, e
				}
				if a.JobID != childID || a.Round != 2 {
					return result, errors.New("reviewer replaced")
				}
				if _, e = s.Continue(ctx, childID, cr); e != nil {
					return result, e
				}
			}
			if spec.Job.Round < 3 {
				if e = s.Yield(ctx, root.JobID, []string{childID}); e != nil {
					return result, e
				}
			}
		} else {
			childRuns++
			if spec.Job.ID != childID {
				return result, errors.New("new reviewer job")
			}
			if childConversation == "" {
				childConversation = result.ConversationID
			} else if result.ConversationID != childConversation || !spec.Resume || spec.Prompt != "review revised version" {
				return result, errors.New("reviewer conversation or continuation lost")
			}
		}
		if e = k.Record(ctx, EventNativeEnded, map[string]string{"turn_id": result.TurnID}); e != nil {
			return result, e
		}
		e = k.Record(ctx, EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID})
		return result, e
	}}
	runBounded(t, d)
	parent, err := s.Job(root.JobID)
	if err != nil || parent.State != "completed" || parent.Round != 3 || parentRuns != 3 || childRuns != 2 || maxActive.Load() != 1 {
		t.Fatalf("parent=%+v parentRuns=%d childRuns=%d max=%d err=%v", parent, parentRuns, childRuns, maxActive.Load(), err)
	}
	child, err := s.Job(childID)
	if err != nil || child.Round != 2 || child.ConversationID != childConversation {
		t.Fatalf("child=%+v %v", child, err)
	}
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs=%+v %v", jobs, err)
	}
}

func TestFailedCheckpointAcknowledgmentCannotResumeAsSaved(t *testing.T) {
	s, _, req := setup(t)
	j, result := uncheckpointedAttempt(t, s, req)
	acknowledgeCheckpoint(t, s, j, result)
	// The evidence may be visible while the backend reports a lost/failed
	// acknowledgment. Visibility must not silently certify a saved conversation.
	if err := finish(context.Background(), s, j, result, errors.New("checkpoint acknowledgement failed"), nil, "", false); err != nil {
		t.Fatal(err)
	}
	r, err := s.Result(j.ID, 1)
	if err != nil || r.State != "unknown" {
		t.Errorf("failed acknowledgment became resolved: %+v %v", r, err)
	}
	if _, err = s.Continue(context.Background(), j.ID, ContinueRequest{SchemaVersion: 1, RequestKey: "unsafe-resume", ExpectedRound: 1, Prompt: "resume"}); err == nil {
		t.Error("failed acknowledgment allowed load-session continuation")
	}
}

func TestCheckpointDoesNotResolveUnknownOrUnendedResultOnCancellation(t *testing.T) {
	for _, kind := range []string{"unknown", "unended"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := setup(t)
			var j Job
			var result RunResult
			if kind == "unknown" {
				j, result = uncheckpointedAttempt(t, s, req)
				result.Outcome = "unknown"
			} else {
				j, result = preparedAttempt(t, s, req, false)
				result.Outcome = "completed"
			}
			acknowledgeCheckpoint(t, s, j, result)
			if err := finish(context.Background(), s, j, result, nil, nil, "runner_cancelled", false); err != nil {
				t.Fatal(err)
			}
			r, err := s.Result(j.ID, 1)
			if err != nil || r.State != "unknown" {
				t.Fatalf("checkpoint converted unresolved result: %+v %v", r, err)
			}
		})
	}
}
