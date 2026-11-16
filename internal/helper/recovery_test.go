package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func preparedAttempt(t *testing.T, s Service, req Request, ended bool) (Job, RunResult) {
	t.Helper()
	ctx := context.Background()
	a, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.transaction(ctx, true, func(db *ledger) error {
		r := db.Jobs[a.JobID]
		r.Rounds[1].Phase = "launch_attempted"
		setState(r, "running", "")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	j, err := s.Job(a.JobID)
	if err != nil {
		t.Fatal(err)
	}
	sink := journalSink{service: s, job: j}
	spec := RunSpec{Job: j}
	var result RunResult
	if ended {
		result, err = recordTurn(ctx, spec, sink)
	} else {
		result, err = beginTurn(ctx, spec, sink)
	}
	if err != nil {
		t.Fatal(err)
	}
	j, err = s.Job(a.JobID)
	if err != nil {
		t.Fatal(err)
	}
	return j, result
}
func TestInspectRecoveryRequiresIdentifiedFencedResult(t *testing.T) {
	for _, kind := range []string{"completed", "checkpointed", "false", "error", "wrong_turn", "wrong_model", "missing_end", "unfenced", "conflicting_evidence"} {
		t.Run(kind, func(t *testing.T) {
			s, d, req := setup(t)
			ended := kind != "checkpointed" && kind != "missing_end"
			j, result := preparedAttempt(t, s, req, ended)
			if kind == "checkpointed" {
				if err := (journalSink{service: s, job: j}).Record(context.Background(), EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID}); err != nil {
					t.Fatal(err)
				}
				result.Outcome = "recovery_required"
			}
			if kind == "missing_end" {
				acknowledgeCheckpoint(t, s, j, result)
				result.Outcome = "completed"
			}
			if kind == "wrong_turn" {
				result.TurnID = "foreign-turn"
			}
			if kind == "wrong_model" {
				result.ObservedModel = "foreign-model"
			}
			runs, inspects := 0, 0
			d.Backends["zed"] = backendFunc{
				run: func(context.Context, RunSpec, Sink) (RunResult, error) {
					runs++
					return RunResult{}, errors.New("prompt replay")
				},
				fence: func(context.Context, Job) error {
					if kind == "unfenced" {
						return errors.New("writer still alive")
					}
					return nil
				},
				inspect: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, bool, error) {
					inspects++
					if spec.Job.ID != j.ID || spec.RunDir != filepath.Join(s.root(), "runs", j.ID, "1") || spec.FactoryBin != d.FactoryBin || spec.Prompt != "review" {
						t.Error("inspection lost attempt binding")
					}
					switch kind {
					case "false":
						return result, false, nil
					case "error":
						return result, true, errors.New("unverified runtime")
					case "conflicting_evidence":
						if err := k.Record(ctx, EventProcessOwned, map[string]string{"pid": "999", "start_identity": "test-start", "process_group": "42"}); !errors.Is(err, ErrConflict) {
							t.Errorf("conflict: %v", err)
						}
					case "completed":
						if _, err := recordTurn(ctx, spec, k); err != nil {
							return RunResult{}, false, err
						}
					}
					return result, true, nil
				},
			}
			if err := recoverAttempts(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			r, err := s.Result(j.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			want := "unknown"
			if kind == "completed" {
				want = "completed"
			}
			if kind == "checkpointed" {
				want = "recovery_required"
			}
			if r.State != want || runs != 0 {
				t.Fatalf("%+v runs=%d", r, runs)
			}
			if kind == "unfenced" && inspects != 0 {
				t.Fatal("inspected unfenced writer")
			}
			if kind == "conflicting_evidence" {
				if err := recoverAttempts(context.Background(), d); err != nil {
					t.Fatal(err)
				}
				if inspects != 1 {
					t.Fatal("inspection erased conflicting evidence")
				}
			}
		})
	}
}
func TestInspectFalseCanLaterRecoverWithoutReplay(t *testing.T) {
	s, d, req := setup(t)
	j, result := preparedAttempt(t, s, req, true)
	identified := false
	calls := 0
	d.Backends["zed"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) {
		t.Error("replayed prompt")
		return RunResult{}, nil
	}, inspect: func(context.Context, RunSpec, Sink) (RunResult, bool, error) { calls++; return result, identified, nil }}
	if err := recoverAttempts(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	r, err := s.Result(j.ID, 1)
	if err != nil || r.State != "unknown" {
		t.Fatalf("%+v %v", r, err)
	}
	identified = true
	if err = recoverAttempts(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	r, err = s.Result(j.ID, 1)
	if err != nil || r.State != "completed" || calls != 2 {
		t.Fatalf("%+v calls=%d %v", r, calls, err)
	}
}
func TestSinkIdentityRetriesDoNotRegressAndConflictsStayUnknown(t *testing.T) {
	for _, event := range []string{EventProcessOwned, EventConversationRecorded, EventTurnRequested, EventTurnIdentified, EventNativeEnded, EventCheckpointSaved} {
		t.Run(event, func(t *testing.T) {
			s, _, req := setup(t)
			j, result := preparedAttempt(t, s, req, true)
			sink := journalSink{service: s, job: j}
			if err := sink.Record(context.Background(), EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID}); err != nil {
				t.Fatal(err)
			}
			fields := map[string]string{}
			switch event {
			case EventProcessOwned:
				fields = map[string]string{"pid": "42", "start_identity": "test-start", "process_group": "42"}
			case EventConversationRecorded:
				fields["conversation_id"] = result.ConversationID
			case EventTurnIdentified, EventNativeEnded:
				fields["turn_id"] = result.TurnID
			case EventCheckpointSaved:
				fields["conversation_id"] = result.ConversationID
				fields["turn_id"] = result.TurnID
			}
			fields["generation"] = strconv.Itoa(j.Generation)
			fields["attestation"] = "same-evidence"
			for i := 0; i < 2; i++ {
				if err := sink.Record(context.Background(), event, fields); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.transaction(context.Background(), false, func(db *ledger) error {
				q := db.Jobs[j.ID].Rounds[1]
				if q.Phase != EventCheckpointSaved {
					t.Error("retry regressed phase")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			fields["attestation"] = "conflicting-evidence"
			if err := sink.Record(context.Background(), event, fields); !errors.Is(err, ErrConflict) {
				t.Fatal(err)
			}
			r, err := s.Result(j.ID, 1)
			if err != nil || r.State != "unknown" || r.ErrorCode != "journal_identity_conflict" {
				t.Fatalf("%+v %v", r, err)
			}
		})
	}
}

type transferAuthority struct {
	mu            sync.Mutex
	current       Scope
	refreshes     int
	refreshedFrom Scope
	retiredJob    string
}

func (a *transferAuthority) Resolve(_ context.Context, r Request) (Scope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.current
	if r.OwnerID != s.OwnerID || r.WorkID != s.WorkID || r.SetID != s.SetID || r.Lease != s.Lease {
		return Scope{}, ErrAuthority
	}
	return s, nil
}
func (a *transferAuthority) Check(_ context.Context, j Job) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.current
	if j.ID == a.retiredJob || j.Scope != s || j.Request.OwnerID != s.OwnerID || j.Request.Lease != s.Lease || j.Request.SetID != s.SetID {
		return ErrAuthority
	}
	return nil
}
func (a *transferAuthority) Refresh(_ context.Context, old Scope) (Scope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refreshes++
	a.refreshedFrom = old
	if (old.EpicID != "" && old.EpicID != a.current.EpicID) || old.WorkID != a.current.WorkID {
		return Scope{}, ErrAuthority
	}
	return a.current, nil
}

func TestReopenRefreshesNewOwnerAndRejectsOldGenerationPublication(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "implementor"
	req.SetID = "set1"
	req.Lease = 1
	old := s.Authority.(*fakeAuthority).scope
	old.OwnerID = req.OwnerID
	old.WorkID = req.WorkID
	old.SetID = req.SetID
	old.Lease = req.Lease
	req.OutputPath = filepath.Join(old.OutputRoot, "implement.v1.md")
	authority := &transferAuthority{current: old}
	s.Authority = authority
	d.Service = s
	j, result := preparedAttempt(t, s, req, true)
	if err := os.WriteFile(req.OutputPath, []byte("## Result\nold generation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	fresh := old
	fresh.Generation = "g2"
	fresh.OwnerID = "owner2"
	fresh.Lease = 2
	authority.current = fresh
	authority.retiredJob = j.ID
	if _, err := authority.Resolve(context.Background(), req); !errors.Is(err, ErrAuthority) {
		t.Fatal("old request still authorized", err)
	}
	s.FenceRuntime = func(context.Context, Job) error { return errors.New("death unknown") }
	if err := s.Fence(context.Background(), "work1", "transfer"); err == nil {
		t.Fatal("uncertain fence succeeded")
	}
	if err := s.Reopen(context.Background(), "work1", "g2"); err == nil || authority.refreshes != 0 {
		t.Fatal("revoking scope refreshed/reopened")
	}
	s.FenceRuntime = func(context.Context, Job) error { return nil }
	if err := s.Fence(context.Background(), "work1", "confirmed transfer"); err != nil {
		t.Fatal(err)
	}
	if err := s.Reopen(context.Background(), "work1", "g2"); err != nil {
		t.Fatal(err)
	}
	if authority.refreshes != 1 || authority.refreshedFrom != old {
		t.Fatalf("refreshes=%d from=%+v", authority.refreshes, authority.refreshedFrom)
	}
	if _, err := s.Submit(context.Background(), req); err == nil {
		t.Fatal("old owner admitted after reopen")
	}
	if err := finish(context.Background(), s, j, result, nil, nil, "", false); err != nil {
		t.Fatal(err)
	}
	if err := flush(context.Background(), RunnerDeps{Service: s, Notify: d.Notify}); err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	log, err := events.Read(store.EventsPath(ix["work1"].Dir))
	if err != nil || len(log) != 0 {
		t.Fatalf("old generation published: %+v %v", log, err)
	}
	owner2 := filepath.Join(s.Brain, "factory", "owner2")
	if err = os.MkdirAll(owner2, 0700); err != nil {
		t.Fatal(err)
	}
	ix["owner2"] = store.Entry{Kind: "session", Epic: "epic1", Dir: owner2}
	if err = store.WriteIndex(s.Brain, ix); err != nil {
		t.Fatal(err)
	}
	req.OwnerID = "owner2"
	req.Lease = 2
	req.RequestKey = "new-owner"
	req.OutputPath = filepath.Join(fresh.OutputRoot, "implement.v2.md")
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Fencing a retired owner must not close the current owner's work barrier.
	if err = s.Fence(context.Background(), "owner1", "retired owner"); err != nil {
		t.Fatal(err)
	}
	d.Service = s
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if err := os.WriteFile(req.OutputPath, []byte("## Result\nnew owner\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordTurn(ctx, spec, k)
	}}
	runBounded(t, d)
	r, err := s.Result(a.JobID, 1)
	if err != nil || r.State != "completed" {
		t.Fatalf("%+v %v", r, err)
	}
	log, err = events.Read(store.EventsPath(ix["work1"].Dir))
	if err != nil || len(log) != 2 || log[0].Kind != events.KindStart || log[1].Kind != events.KindEnd || log[1].File != req.OutputPath {
		t.Fatalf("%+v %v", log, err)
	}
	if err = s.Fence(context.Background(), "work1", "close new generation"); err != nil {
		t.Fatal(err)
	}
	if err = s.Reopen(context.Background(), "work1", "g1"); err == nil {
		t.Fatal("retired generation reopened")
	}
}
