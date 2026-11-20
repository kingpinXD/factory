package helper

import (
	"context"
	"encoding/json"
	"testing"
)

func TestContract2RoundsRetainActualGenerationAcrossContinueAndRetry(t *testing.T) {
	s, deps, request, authority := setupStep(t)
	deps.Backends["codex"] = backendFunc{run: recordCodexTurn}
	first := mustSubmit(t, s, request)
	assertGeneration := func(id string, round, generation int) {
		t.Helper()
		if err := s.transaction(context.Background(), false, func(db *ledger) error {
			q := db.Jobs[id].Rounds[round]
			if q.JobGeneration != generation {
				t.Fatalf("round %d retained generation %d, expected actual %d", round, q.JobGeneration, generation)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertGeneration(first.JobID, 1, 1)
	runBounded(t, deps)
	authority.step(StepBasis{EntryRef: "work1#8", ContinuationOf: "work1#1"})
	second, err := s.Continue(context.Background(), first.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "actual-generation-continue", ExpectedRound: 1, Prompt: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(second.JobID)
	if err != nil {
		t.Fatal(err)
	}
	assertGeneration(second.JobID, second.Round, job.Generation)
	assertGeneration(first.JobID, 1, 1)
	runBounded(t, deps)
	authority.step(StepBasis{EntryRef: "work1#15", RetryOf: "work1#8"})
	request.RequestKey = "actual-generation-retry"
	retry := mustSubmit(t, s, request)
	if retry.JobID == first.JobID {
		t.Fatal("retry reused old lineage")
	}
	job, err = s.Job(retry.JobID)
	if err != nil {
		t.Fatal(err)
	}
	assertGeneration(retry.JobID, retry.Round, job.Generation)
	assertGeneration(first.JobID, 1, 1)
	assertGeneration(second.JobID, second.Round, 2)
}

func TestRoundGenerationIsNeverBackfilledFromCurrentJob(t *testing.T) {
	for _, contract := range []int{0, 2} {
		job := Job{ID: "job", Generation: 27, Round: 3, Scope: Scope{CodexContract: contract}}
		q := newRound(job, "prompt", "")
		if contract == 2 && q.JobGeneration != 27 {
			t.Fatal("new round guessed generation from round number")
		}
		if contract == 0 && q.JobGeneration != 0 {
			t.Fatal("legacy round gained a generation field")
		}
		bytes, err := json.Marshal(q)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(bytes, &fields); err != nil {
			t.Fatal(err)
		}
		if contract == 0 {
			if _, ok := fields["job_generation"]; ok {
				t.Fatal("legacy encoding changed")
			}
		}
		delete(fields, "job_generation")
		bytes, err = json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		var old roundRecord
		if err := strictDecode(bytes, &old); err != nil {
			t.Fatal(err)
		}
		if old.JobGeneration != 0 {
			t.Fatal("old missing generation was guessed")
		}
	}
}
