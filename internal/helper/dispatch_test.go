package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type stepTestAuthority struct {
	*fakeAuthority
	stepMu  sync.Mutex
	basis   StepBasis
	stepErr error
}

func (a *stepTestAuthority) ResolveStep(_ context.Context, _ Scope, component string) (StepBasis, error) {
	a.stepMu.Lock()
	defer a.stepMu.Unlock()
	b := a.basis
	b.Component = component
	return b, a.stepErr
}
func (a *stepTestAuthority) step(b StepBasis) { a.stepMu.Lock(); defer a.stepMu.Unlock(); a.basis = b }
func setupStep(t *testing.T) (Service, RunnerDeps, Request, *stepTestAuthority) {
	t.Helper()
	s, d, r := setupCodex(t)
	a := &stepTestAuthority{fakeAuthority: s.Authority.(*fakeAuthority), basis: StepBasis{EntryRef: "work1#1"}}
	a.scope.CodexContract = 2
	s.Authority = a
	d.Service = s
	return s, d, r, a
}
func mustSubmit(t *testing.T, s Service, r Request) Accepted {
	t.Helper()
	a, e := s.Submit(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func wantOccupied(t *testing.T, e error, id string, round int) {
	t.Helper()
	var occupied *DispatchOccupiedError
	if !errors.As(e, &occupied) || occupied.JobID != id || occupied.Round != round {
		t.Fatalf("wrong occupied step: %+v %v", occupied, e)
	}
}

func TestRootDispatchLostAckAndConcurrentChangedKeys(t *testing.T) {
	s, _, r, auth := setupStep(t)
	r.Component = "implementor"
	r.OutputPath = filepath.Join(auth.scope.OutputRoot, "implement.v1.md")
	// Lose the response after the real durable ledger commit.
	lost := s
	lost.testCheckpoint = func(at string) {
		if at == "submit_committed" {
			panic("transport loss after fsync")
		}
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("did not inject actual postcommit loss")
			}
		}()
		_, _ = lost.Submit(context.Background(), r)
	}()
	jobs, e := s.Jobs()
	if e != nil || len(jobs) != 1 {
		t.Fatal(jobs, e)
	}
	a := accepted(jobs[0])
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := r
			q.RequestKey = fmt.Sprintf("changed-%d", i)
			q.Prompt = "changed output and prompt"
			q.OutputPath = filepath.Join(auth.scope.OutputRoot, "implement.v2.md")
			_, e := s.Submit(context.Background(), q)
			wantOccupied(t, e, a.JobID, 1)
		}(i)
	}
	wg.Wait()
	jobs, e = s.Jobs()
	if e != nil || len(jobs) != 1 {
		t.Fatalf("duplicate after lost acknowledgement: %v %v", jobs, e)
	}
	data, e := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	var db ledger
	if e = json.Unmarshal(data, &db); e != nil {
		t.Fatal(e)
	}
	if len(db.Keys) != 1 || len(db.DispatchSlots) != 1 || len(db.Reservations) != 1 {
		t.Fatalf("orphan allocation: %+v", db)
	}
}

func TestRootDispatchContinuationKeepsHistoryAndActualRound(t *testing.T) {
	s, d, r, a := setupStep(t)
	first := mustSubmit(t, s, r)
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	a.step(StepBasis{EntryRef: "work1#8", ContinuationOf: "work1#1"})
	q := r
	q.RequestKey = "return-submit"
	_, e := s.Submit(context.Background(), q)
	wantOccupied(t, e, first.JobID, 1)
	second, e := s.Continue(context.Background(), first.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "return-continue", ExpectedRound: 1, Prompt: "provide a fresh truthful implementation report"})
	if e != nil || second.Round != 2 {
		t.Fatalf("continue: %+v %v", second, e)
	}
	q.RequestKey = "after-coordinator-replacement"
	_, e = s.Submit(context.Background(), q)
	wantOccupied(t, e, first.JobID, 2)
	a.stepMu.Lock()
	a.stepErr = errors.New("current phase has moved")
	a.stepMu.Unlock()
	if got := mustSubmit(t, s, r); got.JobID != first.JobID || got.Round != 1 {
		t.Fatalf("same key lost original acceptance: %+v", got)
	}
	a.stepMu.Lock()
	a.stepErr = nil
	a.stepMu.Unlock()
	q = r
	q.Prompt = "changed immutable request"
	if _, e = s.Submit(context.Background(), q); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if e = s.transaction(context.Background(), false, func(db *ledger) error {
		j := db.Jobs[first.JobID]
		if len(db.DispatchSlots) != 2 || j.InitialStep.EntryRef != "work1#1" || j.Rounds[1].Step.EntryRef != "work1#1" || j.Rounds[2].Step.EntryRef != "work1#8" || j.Job.Scope.StepSlot != j.Rounds[1].StepSlot {
			t.Fatal("historical binding changed")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	a.step(StepBasis{EntryRef: "work1#9"})
	q = r
	q.RequestKey = "fresh-verifier"
	fresh := mustSubmit(t, s, q)
	if fresh.JobID == first.JobID {
		t.Fatal("fresh verifier reused implementor lineage")
	}
}

func TestRootDispatchRejectsWrongLineageAndUnsettledRetry(t *testing.T) {
	s, d, r, a := setupStep(t)
	first := mustSubmit(t, s, r)
	a.step(StepBasis{EntryRef: "work1#2", ContinuationOf: "missing#1"})
	q := r
	q.RequestKey = "bad-lineage"
	if _, e := s.Submit(context.Background(), q); e == nil || !strings.Contains(e.Error(), "lineage") {
		t.Fatal(e)
	}
	a.step(StepBasis{EntryRef: "work1#3", RetryOf: "work1#1"})
	q.RequestKey = "premature-retry"
	if _, e := s.Submit(context.Background(), q); e == nil || !strings.Contains(e.Error(), "terminal and stopped") {
		t.Fatal(e)
	}
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	for _, field := range []string{"published", "delivered"} {
		if e := s.transaction(context.Background(), true, func(db *ledger) error {
			q := db.Jobs[first.JobID].Rounds[1]
			if field == "published" {
				q.Published = false
			} else {
				q.Delivered = false
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		q.RequestKey = "unsettled-" + field
		if _, e := s.Submit(context.Background(), q); e == nil || !strings.Contains(e.Error(), "publication") {
			t.Fatal(e)
		}
		if e := flush(context.Background(), d); e != nil {
			t.Fatal(e)
		}
	}
	q.RequestKey = "authorized-terminal-retry"
	next := mustSubmit(t, s, q)
	if next.JobID == first.JobID {
		t.Fatal("retry reused closed lineage")
	}
	a.step(StepBasis{EntryRef: "work1#3"})
	q.RequestKey = "retry-again"
	_, e := s.Submit(context.Background(), q)
	wantOccupied(t, e, next.JobID, 1)
}

func TestDispatchLedgerCorruptionRefusesWithoutRewriting(t *testing.T) {
	for _, kind := range []string{"schema", "index", "round", "nested"} {
		t.Run(kind, func(t *testing.T) {
			s, _, r, _ := setupStep(t)
			a := mustSubmit(t, s, r)
			file := filepath.Join(s.root(), "ledger.json")
			data, e := os.ReadFile(file)
			if e != nil {
				t.Fatal(e)
			}
			var db ledger
			if e = json.Unmarshal(data, &db); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "schema":
				db.SchemaVersion = 1
			case "index":
				db.DispatchSlots["orphan"] = a.JobID
			case "round":
				db.Jobs[a.JobID].Rounds[1].Step.EntryRef = "wrong#2"
			case "nested":
				db.Jobs[a.JobID].Job.Request.ParentJobID = a.JobID
			}
			bad, _ := json.Marshal(db)
			if e = os.WriteFile(file, bad, 0600); e != nil {
				t.Fatal(e)
			}
			if _, e = s.Jobs(); e == nil {
				t.Fatal("corrupt dispatch accepted")
			}
			after, _ := os.ReadFile(file)
			if string(after) != string(bad) {
				t.Fatal("read changed corrupt ledger")
			}
		})
	}
}

func TestDispatchAuthorizationAndLegacyJSON(t *testing.T) {
	scope := Scope{EpicID: "e", OwnerID: "o", WorkID: "w", Generation: "g", Lease: 2, CodexContract: 2, StepSlot: "slot"}
	other := scope
	other.StepSlot = "other"
	if scope.Authorization() != other.Authorization() {
		t.Fatal("slot changed authorization")
	}
	for _, change := range []func(*Scope){func(s *Scope) { s.OwnerID = "foreign" }, func(s *Scope) { s.Generation = "other" }, func(s *Scope) { s.Lease++ }, func(s *Scope) { s.CodexContract = 0 }} {
		other = scope
		change(&other)
		if scope.Authorization() == other.Authorization() {
			t.Fatal("authority field was erased")
		}
	}
	data, _ := json.Marshal(Scope{})
	if string(data) != `{"epic_id":"","owner_id":"","work_id":"","set_id":"","lease":0,"worktree":"","output_root":"","generation":""}` {
		t.Fatal("legacy scope JSON changed:", string(data))
	}
	if strings.Contains(string(data), "codex_contract") || strings.Contains(string(data), "step_slot") {
		t.Fatal(string(data))
	}
	for _, v := range []string{"work1#5", "work1:phase#15"} {
		if !validEntryRef(v) {
			t.Fatal(v)
		}
	}
	for _, v := range []string{"", "../entry", "x y", "x\n", strings.Repeat("a", 1025)} {
		if validEntryRef(v) {
			t.Fatal(v)
		}
	}
}

func TestSubmitManagedDerivesParentAtomicallyAndPreservesFanout(t *testing.T) {
	for _, provider := range []string{"zed", "codex"} {
		t.Run(provider, func(t *testing.T) {
			s, _, r := setup(t)
			if provider == "codex" {
				s, _, r, _ = setupStep(t)
			}
			a := mustSubmit(t, s, r)
			if e := s.transaction(context.Background(), true, func(db *ledger) error {
				j := db.Jobs[a.JobID]
				setState(j, "running", "")
				j.Rounds[1].Phase = "launch_attempted"
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			j, e := s.Job(a.JobID)
			if e != nil {
				t.Fatal(e)
			}
			for _, key := range []string{"first-area", "second-area"} {
				child, e := s.SubmitManaged(context.Background(), j.ID, j.Generation, toolcontract.NestedSubmitInput{RequestKey: key, Component: "reviewer", Prompt: key})
				if e != nil {
					t.Fatal(e)
				}
				c, e := s.Job(child.JobID)
				if e != nil || c.Request.ParentJobID != j.ID || c.Scope.StepSlot != "" || c.Scope.Authorization() != j.Scope.Authorization() {
					t.Fatal(c, e)
				}
			}
			if _, e = s.SubmitManaged(context.Background(), j.ID, j.Generation+1, toolcontract.NestedSubmitInput{RequestKey: "stale", Component: "reviewer", Prompt: "stale"}); e == nil {
				t.Fatal("stale generation admitted")
			}
			if provider == "codex" {
				q := r
				q.ParentJobID = j.ID
				q.RequestKey = "spoofed"
				if _, e = s.Submit(context.Background(), q); e == nil || !strings.Contains(e.Error(), "cannot claim") {
					t.Fatal(e)
				}
			}
			if e = s.Yield(context.Background(), j.ID, func() []string {
				jobs, _ := s.Jobs()
				ids := []string{}
				for _, child := range jobs {
					if child.Request.ParentJobID == j.ID {
						ids = append(ids, child.ID)
					}
				}
				return ids
			}()); e != nil {
				t.Fatal(e)
			}
			recovered, e := s.SubmitManaged(context.Background(), j.ID, j.Generation, toolcontract.NestedSubmitInput{RequestKey: "first-area", Component: "reviewer", Prompt: "first-area"})
			if e != nil || recovered.Round != 1 || recovered.State != "queued" {
				t.Fatal("yielding same-key recovery failed:", recovered, e)
			}
			if _, e = s.SubmitManaged(context.Background(), j.ID, j.Generation, toolcontract.NestedSubmitInput{RequestKey: "first-area", Component: "reviewer", Prompt: "changed"}); !errors.Is(e, ErrConflict) {
				t.Fatal("yielding changed-key payload:", e)
			}
			if e = s.transaction(context.Background(), false, func(db *ledger) error {
				q := db.Jobs[j.ID].Rounds[1]
				if len(db.Jobs) != 3 || len(q.Children) != 2 || !q.Yield {
					t.Fatal("same-key recovery changed graph")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if _, e = s.SubmitManaged(context.Background(), j.ID, j.Generation, toolcontract.NestedSubmitInput{RequestKey: "after-yield", Component: "reviewer", Prompt: "late"}); e == nil {
				t.Fatal("child admitted after yield")
			}
		})
	}
}

func TestSubmitManagedStopRaceLeavesOneCompleteFrontier(t *testing.T) {
	for i := 0; i < 12; i++ {
		s, _, r, _ := setupStep(t)
		a := mustSubmit(t, s, r)
		s.FenceRuntime = func(context.Context, Job) error { return nil }
		if e := s.transaction(context.Background(), true, func(db *ledger) error {
			p := db.Jobs[a.JobID]
			setState(p, "running", "")
			p.Rounds[1].Phase = "launch_attempted"
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		gate := make(chan struct{})
		go func() {
			defer wg.Done()
			<-gate
			_, _ = s.SubmitManaged(context.Background(), a.JobID, 1, toolcontract.NestedSubmitInput{RequestKey: "racing-child", Component: "reviewer", Prompt: "review"})
		}()
		go func() {
			defer wg.Done()
			<-gate
			if e := s.Cancel(context.Background(), a.JobID, "actual stop"); e != nil {
				t.Error(e)
			}
		}()
		close(gate)
		wg.Wait()
		if e := s.transaction(context.Background(), false, func(db *ledger) error {
			p := db.Jobs[a.JobID]
			if len(db.Jobs) > 2 || len(db.Keys) != len(db.Jobs) || len(p.Rounds[1].Children) != len(db.Jobs)-1 {
				t.Fatal("partial nested admission")
			}
			for _, j := range db.Jobs {
				if j.Job.State != "cancelled" {
					t.Fatalf("stop failed to include child: %+v", j.Job)
				}
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
}

func TestSubmitManagedActualRunnerKeepsSameRoleFanout(t *testing.T) {
	for _, provider := range []string{"zed", "codex"} {
		t.Run(provider, func(t *testing.T) {
			s, d, r := setup(t)
			if provider == "codex" {
				s, d, r, _ = setupStep(t)
			}
			a := mustSubmit(t, s, r)
			d.Backends[provider] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				if spec.Job.ID == a.JobID && spec.Job.Round == 1 {
					children := []string{}
					for _, key := range []string{"area-one", "area-two"} {
						child, e := s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, toolcontract.NestedSubmitInput{RequestKey: key, Component: "reviewer", Prompt: key})
						if e != nil {
							return RunResult{}, e
						}
						children = append(children, child.JobID)
					}
					if e := s.Yield(ctx, spec.Job.ID, children); e != nil {
						return RunResult{}, e
					}
				}
				if provider == "codex" {
					return recordCodexTurn(ctx, spec, k)
				}
				return recordTurn(ctx, spec, k)
			}}
			runBounded(t, d)
			j, e := s.Job(a.JobID)
			if e != nil || j.State != "completed" || j.Round != 2 {
				t.Fatal(j, e)
			}
			jobs, e := s.Jobs()
			if e != nil || len(jobs) != 3 {
				t.Fatal(jobs, e)
			}
			for _, j := range jobs {
				if j.State != "completed" {
					t.Fatal(j)
				}
			}
		})
	}
}

func TestRootDispatchRequiresTrustedStepAuthority(t *testing.T) {
	for _, contract := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(contract), func(t *testing.T) {
			s, _, r := setupCodex(t)
			a := s.Authority.(*fakeAuthority)
			a.scope.CodexContract = contract
			_, e := s.Submit(context.Background(), r)
			if e == nil {
				t.Fatal("unsupported/unqualified contract admitted")
			}
			expected := "unsupported helper execution contract"
			if contract == 2 {
				expected = "trusted dispatch step authority required"
			}
			if !strings.Contains(e.Error(), expected) {
				t.Fatal(e)
			}
			jobs, e := s.Jobs()
			if e != nil || len(jobs) != 0 {
				t.Fatal(jobs, e)
			}
		})
	}
}

func TestDispatchLaterRoundSlotCannotChange(t *testing.T) {
	s, d, r, a := setupStep(t)
	first := mustSubmit(t, s, r)
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	a.step(StepBasis{EntryRef: "work1#8", ContinuationOf: "work1#1"})
	if _, e := s.Continue(context.Background(), first.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "return", ExpectedRound: 1, Prompt: "fresh report"}); e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(s.root(), "ledger.json")
	data, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	var db ledger
	if e = json.Unmarshal(data, &db); e != nil {
		t.Fatal(e)
	}
	db.Jobs[first.JobID].Rounds[2].Step.EntryRef = "work1#99"
	data, _ = json.Marshal(db)
	if e = os.WriteFile(file, data, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Jobs(); e == nil || !strings.Contains(e.Error(), "corrupt helper dispatch round") {
		t.Fatal(e)
	}
}

func completedFanoutRetryFixture(t *testing.T, mode string) (Service, RunnerDeps, Request, *stepTestAuthority, string) {
	t.Helper()
	if mode == "release" {
		s, d, j, _, b, _ := releaseFixture(t)
		b.allow.Store(true)
		runBounded(t, d)
		return s, d, j.Request, s.Authority.(*stepTestAuthority), j.ID
	}
	s, d, r, a := setupStep(t)
	root := mustSubmit(t, s, r)
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if spec.Job.ID == root.JobID && spec.Job.Round == 1 {
			child, e := s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, toolcontract.NestedSubmitInput{RequestKey: "real-fanout-child", Component: "reviewer", Prompt: "review"})
			if e != nil {
				return RunResult{}, e
			}
			if e = s.Yield(ctx, spec.Job.ID, []string{child.JobID}); e != nil {
				return RunResult{}, e
			}
		}
		return recordCodexTurn(ctx, spec, k)
	}}
	runBounded(t, d)
	return s, d, r, a, root.JobID
}

func TestRootRetryAfterActualFanoutParentCompletion(t *testing.T) {
	for _, mode := range []string{"yield", "release"} {
		t.Run(mode, func(t *testing.T) {
			s, _, r, a, id := completedFanoutRetryFixture(t, mode)
			parent, e := s.Job(id)
			if e != nil || parent.State != "completed" || parent.Round != 2 {
				t.Fatal(parent, e)
			}
			if e = s.transaction(context.Background(), false, func(db *ledger) error {
				p := db.Jobs[id]
				old := p.Rounds[1]
				if old.Result.State != "waiting_children" || old.Published || old.Delivered {
					t.Fatal("waiting round was fabricated as published")
				}
				if (mode == "yield") != old.Yield || (mode == "release") != (old.ChildRelease != nil) {
					t.Fatal("wrong real child-release path")
				}
				for _, desc := range descendants(db, id) {
					child := db.Jobs[desc]
					q := child.Rounds[child.Job.Round]
					if !settled(child) || !q.Published || !q.Delivered || !checkpointed(q) {
						t.Fatal("actual current lineage not complete")
					}
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			a.step(StepBasis{EntryRef: "work1#90", RetryOf: "work1#1"})
			r.RequestKey = "authorized-after-fanout"
			next, e := s.Submit(context.Background(), r)
			if e != nil || next.JobID == id {
				t.Fatalf("settled %s parent could not retry: %+v %v", mode, next, e)
			}
			if e = s.transaction(context.Background(), false, func(db *ledger) error {
				q := db.Jobs[id].Rounds[1]
				if q.Published || q.Delivered || q.Result.State != "waiting_children" {
					t.Fatal("retry fabricated historical publication")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestRootRetryFanoutRequiresActualHistoricalAndCurrentSettlement(t *testing.T) {
	for _, mode := range []string{"yield", "release"} {
		t.Run(mode, func(t *testing.T) {
			for _, fault := range []string{"old-writer", "old-end", "old-checkpoint", "old-release", "unfinished-child", "child-publication", "child-delivery", "current-publication", "current-delivery"} {
				t.Run(fault, func(t *testing.T) {
					s, _, r, a, id := completedFanoutRetryFixture(t, mode)
					if e := s.transaction(context.Background(), true, func(db *ledger) error {
						p := db.Jobs[id]
						old := p.Rounds[1]
						child := db.Jobs[old.Children[0]]
						cq := child.Rounds[child.Job.Round]
						switch fault {
						case "old-writer":
							old.Stopped = false
						case "old-end":
							old.Ended = false
						case "old-checkpoint":
							delete(old.Evidence, EventPersistedTurnVerified+":reply_sha256")
						case "old-release":
							old.Yield = false
							old.ChildRelease = nil
						case "unfinished-child":
							setState(child, "queued", "")
							cq.Stopped = false
						case "child-publication":
							cq.Published = false
						case "child-delivery":
							cq.Delivered = false
						case "current-publication":
							p.Rounds[2].Published = false
						case "current-delivery":
							p.Rounds[2].Delivered = false
						}
						return nil
					}); e != nil {
						t.Fatal(e)
					}
					a.step(StepBasis{EntryRef: "work1#90", RetryOf: "work1#1"})
					r.RequestKey = "retry-with-unsettled-lineage"
					if _, e := s.Submit(context.Background(), r); e == nil || !(strings.Contains(e.Error(), "retry lineage") || strings.Contains(e.Error(), "corrupt recovered child release")) {
						t.Fatalf("%s %s permitted retry or hit wrong guard: %v", mode, fault, e)
					}
				})
			}
		})
	}
}

func failedChildWithoutCheckpoint(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
	conversation := spec.Job.ConversationID
	turn := fmt.Sprintf("codex-turn-%d", spec.Job.Round)
	for _, event := range []struct {
		name   string
		fields map[string]string
	}{
		{EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "owned-test"}},
		{EventConversationRecorded, map[string]string{"conversation_id": conversation}},
		{EventTurnRequested, nil}, {EventTurnIdentified, map[string]string{"turn_id": turn}},
		{EventNativeEnded, map[string]string{"turn_id": turn}},
	} {
		if e := sink.Record(ctx, event.name, event.fields); e != nil {
			return RunResult{}, e
		}
	}
	return RunResult{Outcome: "failed", Reply: "failed child review", ConversationID: conversation, TurnID: turn, EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: spec.Job.Route.Model, ConfiguredEffort: spec.Job.Route.Effort, PersistedReplySHA256: digestBytes([]byte("failed child review"))}, nil
}

func TestRootRetryAfterLaterAcknowledgedChildFailure(t *testing.T) {
	for _, mode := range []string{"yield", "release"} {
		t.Run(mode, func(t *testing.T) {
			for _, checkpoint := range []bool{true, false} {
				t.Run(fmt.Sprintf("checkpoint-%v", checkpoint), func(t *testing.T) {
					var s Service
					var d RunnerDeps
					var request Request
					var authority *stepTestAuthority
					var parentID, childID string
					var backend *releaseTestBackend
					ids := make(chan string, 1)
					if mode == "release" {
						var parent, child Job
						s, d, parent, child, backend, _ = releaseFixture(t)
						backend.allow.Store(true)
						request = parent.Request
						authority = s.Authority.(*stepTestAuthority)
						parentID = parent.ID
						childID = child.ID
						ids <- childID
					} else {
						s, d, request, authority = setupStep(t)
						parentID = mustSubmit(t, s, request).JobID
					}
					run := func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
						if spec.Job.ID == parentID {
							if spec.Job.Round == 1 {
								child, e := s.SubmitManaged(ctx, spec.Job.ID, spec.Job.Generation, toolcontract.NestedSubmitInput{RequestKey: "child-first-review", Component: "reviewer", Prompt: "review"})
								if e != nil {
									return RunResult{}, e
								}
								childID = child.JobID
								ids <- childID
								if e = s.Yield(ctx, spec.Job.ID, []string{childID}); e != nil {
									return RunResult{}, e
								}
							} else if spec.Job.Round == 2 {
								if _, e := s.Continue(ctx, childID, ContinueRequest{SchemaVersion: 1, RequestKey: "child-later-review", ExpectedRound: 1, Prompt: "review later change"}); e != nil {
									return RunResult{}, e
								}
								if e := s.Yield(ctx, spec.Job.ID, []string{childID}); e != nil {
									return RunResult{}, e
								}
							}
						}
						if spec.Job.ID != parentID && spec.Job.Round == 2 && !checkpoint {
							return failedChildWithoutCheckpoint(ctx, spec, sink)
						}
						result, e := recordCodexTurn(ctx, spec, sink)
						if spec.Job.ID != parentID && spec.Job.Round == 2 && e == nil {
							result.Outcome = "failed"
						}
						return result, e
					}
					if mode == "release" {
						backend.run = run
					} else {
						d.Backends["codex"] = backendFunc{run: run}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					done := make(chan error, 1)
					go func() { defer close(done); done <- Run(ctx, d) }()
					defer func() { cancel(); <-done }()
					var id string
					select {
					case id = <-ids:
					case <-ctx.Done():
						t.Fatal("child not admitted")
					}
					eventually(t, func() bool {
						parent, e := s.Job(parentID)
						if e != nil || parent.Round != 2 || parent.HoldReason != "child_recovery_required" {
							return false
						}
						ready := false
						e = s.transaction(context.Background(), false, func(db *ledger) error {
							child := db.Jobs[id]
							q := child.Rounds[child.Job.Round]
							ready = child.Job.State == "failed" && q.Published && q.Delivered && db.Jobs[parentID].Rounds[2].Published && db.Jobs[parentID].Rounds[2].Delivered
							return nil
						})
						return e == nil && ready
					})
					authority.step(StepBasis{EntryRef: "work1#90", RetryOf: "work1#1"})
					nextRequest := request
					nextRequest.RequestKey = "before-failed-child-ack"
					if _, e := s.Submit(context.Background(), nextRequest); e == nil || !strings.Contains(e.Error(), "retry lineage is not terminal and stopped") {
						t.Fatal("unacknowledged failure admitted new root:", e)
					}
					authority.step(StepBasis{EntryRef: "work1#1"})
					if _, e := s.Continue(context.Background(), parentID, ContinueRequest{SchemaVersion: 1, RequestKey: "missing-failed-child-ack", ExpectedRound: 2, Prompt: "continue"}); e == nil || !strings.Contains(e.Error(), "acknowledge all failed children exactly") {
						t.Fatal("missing acknowledgement accepted:", e)
					}
					if _, e := s.Continue(context.Background(), parentID, ContinueRequest{SchemaVersion: 1, RequestKey: "actual-failed-child-ack", ExpectedRound: 2, Prompt: "continue after failed review", AcknowledgedChildren: []string{id}}); e != nil {
						t.Fatal(e)
					}
					select {
					case e := <-done:
						if e != nil {
							t.Fatal(e)
						}
					case <-ctx.Done():
						t.Fatal("acknowledged parent did not finish")
					}
					parent, e := s.Job(parentID)
					if e != nil || parent.State != "completed" || parent.Round != 3 {
						t.Fatal(parent, e)
					}
					if e = s.transaction(context.Background(), false, func(db *ledger) error {
						p := db.Jobs[parentID]
						child := db.Jobs[id]
						cq := child.Rounds[child.Job.Round]
						if p.Rounds[1].Result.State != "waiting_children" || p.Rounds[1].Published || p.Rounds[1].Delivered || child.Job.State != "failed" || child.Job.Round != 2 || !cq.Published || !cq.Delivered || checkpointed(cq) != checkpoint || child.Rounds[1].Result.State != "completed" {
							t.Fatal("ordinary acknowledgement history was not retained")
						}
						return nil
					}); e != nil {
						t.Fatal(e)
					}
					authority.step(StepBasis{EntryRef: "work1#90", RetryOf: "work1#1"})
					nextRequest.RequestKey = "after-actual-failed-child-ack"
					next, e := s.Submit(context.Background(), nextRequest)
					if e != nil || next.JobID == parentID {
						t.Fatalf("ordinary acknowledged failed child prevented retry: %+v %v", next, e)
					}
				})
			}
		})
	}
}
