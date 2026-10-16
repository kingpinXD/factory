package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestBrokerInterruptPreservesConversationAndRequiresExplicitRecovery(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan RunResult, 1)
	var runs atomic.Int32
	var nativeSaved RunResult
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		runs.Add(1)
		if spec.Resume {
			if spec.Job.ConversationID == "" || spec.Prompt != "explicit recovery\n\nAuthoritative output path: "+req.OutputPath || spec.OutputPath != req.OutputPath {
				return RunResult{}, errors.New("lost recovery binding")
			}
			if err := os.WriteFile(spec.OutputPath, []byte("## Result\nrecovered\n"), 0600); err != nil {
				return RunResult{}, err
			}
			return recordTurn(ctx, spec, k)
		}
		result, err := beginTurn(ctx, spec, k)
		if err != nil {
			return result, err
		}
		if err = appendSynced(filepath.Join(s.Brain, "interrupt-command"), "command once\n"); err != nil {
			return result, err
		}
		started <- result
		<-ctx.Done()
		saveCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = k.Record(saveCtx, EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID})
		result.Outcome = "recovery_required"
		if err == nil {
			nativeSaved = result
		}
		return result, err
	}, inspect: func(context.Context, RunSpec, Sink) (RunResult, bool, error) {
		return nativeSaved, nativeSaved.ConversationID != "", nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	var original RunResult
	select {
	case original = <-started:
	case <-ctx.Done():
		t.Fatal("worker did not identify turn")
	}
	if _, err = s.Interrupt(context.Background(), accepted.JobID, 2, "stale round"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	intent, err := s.Interrupt(context.Background(), accepted.JobID, 1, "attach interrupt")
	if err != nil || intent.State != "running" {
		t.Fatalf("%+v %v", intent, err)
	}
	if _, err = s.Interrupt(context.Background(), accepted.JobID, 1, "retry"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { j, e := s.Job(accepted.JobID); return e == nil && j.State == "recovery_required" })
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	result, err := s.Result(accepted.JobID, 1)
	if err != nil || result.ConversationID != original.ConversationID || result.TurnID != original.TurnID || result.Outcome != "recovery_required" {
		t.Fatalf("%+v %v", result, err)
	}
	ix, _ := store.ReadIndex(s.Brain)
	log, err := events.Read(store.EventsPath(ix[req.WorkID].Dir))
	if err != nil || len(log) != 1 || log[0].Kind != events.KindStart {
		t.Fatalf("interrupted report published: %+v %v", log, err)
	}
	heldCtx, heldCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer heldCancel()
	if err = Run(heldCtx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if runs.Load() != 1 {
		t.Fatal("interrupt silently replayed prompt")
	}
	if _, err = s.Continue(context.Background(), accepted.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "recover", ExpectedRound: 1, Prompt: "explicit recovery"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Interrupt(context.Background(), accepted.JobID, 1, "stale attach"); !errors.Is(err, ErrConflict) {
		t.Fatal("old interrupt reached new round", err)
	}
	runBounded(t, d)
	result, err = s.Result(accepted.JobID, 2)
	if err != nil || result.State != "completed" || result.ConversationID != original.ConversationID || runs.Load() != 2 {
		t.Fatalf("%+v runs=%d %v", result, runs.Load(), err)
	}
	data, err := os.ReadFile(filepath.Join(s.Brain, "interrupt-command"))
	if err != nil || string(data) != "command once\n" {
		t.Fatalf("replayed command: %q %v", data, err)
	}
}
func TestInterruptUnknownDeathRetainsWriterBarrier(t *testing.T) {
	s, d, req := setup(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	var runs atomic.Int32
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		runs.Add(1)
		result, err := beginTurn(ctx, spec, k)
		if err != nil {
			return result, err
		}
		close(started)
		<-ctx.Done()
		return result, ctx.Err()
	}, fence: func(context.Context, Job) error { return errors.New("container or host death unproven") }}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	<-started
	if _, err = s.Interrupt(context.Background(), accepted.JobID, 1, "attach"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { r, e := s.Result(accepted.JobID, 1); return e == nil && r.State == "unknown" })
	replacement := req
	replacement.RequestKey = "replacement"
	next, err := s.Submit(context.Background(), replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.transaction(context.Background(), false, func(db *ledger) error {
		if db.Jobs[accepted.JobID].Rounds[1].Stopped || admissible(db, db.Jobs[next.JobID]) {
			t.Error("unknown death released writer")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if runs.Load() != 1 {
		t.Fatal("replacement writer started")
	}
}
func TestInterruptIntentRecoversThroughInspectAfterBrokerRestart(t *testing.T) {
	s, d, req := setup(t)
	j, result := preparedAttempt(t, s, req, false)
	if _, err := s.Interrupt(context.Background(), j.ID, j.Round, "attach before broker died"); err != nil {
		t.Fatal(err)
	}
	restarted := Service{Brain: s.Brain, Authority: s.Authority}
	d.Service = restarted
	d.Backends["zed"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) {
		t.Error("replayed prompt")
		return RunResult{}, nil
	}, inspect: func(ctx context.Context, _ RunSpec, k Sink) (RunResult, bool, error) {
		err := k.Record(ctx, EventCheckpointSaved, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID})
		result.Outcome = "interrupted"
		return result, err == nil, err
	}}
	if err := recoverAttempts(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	r, err := s.Result(j.ID, 1)
	if err != nil || r.State != "recovery_required" || r.ConversationID != result.ConversationID {
		t.Fatalf("%+v %v", r, err)
	}
}
