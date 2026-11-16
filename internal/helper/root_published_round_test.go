package helper

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func rootRoundLedger(t *testing.T, s Service) []byte {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func rootRoundRead(t *testing.T, s Service, scope Scope, step StepBasis) (PublishedRoundEvidence, error) {
	t.Helper()
	before := rootRoundLedger(t, s)
	result, err := s.ReadCurrentRootPublishedRound(context.Background(), scope, step)
	if !bytes.Equal(before, rootRoundLedger(t, s)) {
		t.Fatal("factual reader mutated helper ledger")
	}
	return result, err
}
func TestCurrentRootPublishedRoundActualContinuationKeepsInitialSlotHistorical(t *testing.T) {
	s, d, a := publishedRoundFixture(t)
	first, err := s.ReadPublishedRound(context.Background(), a.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rootRoundRead(t, s, first.Scope, first.Round.Step)
	if err != nil || !reflect.DeepEqual(got, first) {
		t.Fatalf("actual first root source %+v %v", got, err)
	}
	auth := s.Authority.(*stepTestAuthority)
	auth.step(StepBasis{EntryRef: "work1#8", ContinuationOf: first.Round.Step.EntryRef})
	next, err := s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "current-root-next", ExpectedRound: 1, Prompt: "Write the fresh implementation report.", OutputPath: filepath.Join(first.Scope.OutputRoot, "implement.v2.md")})
	if err != nil || next.Round != 2 {
		t.Fatal(next, err)
	}
	if _, err = rootRoundRead(t, s, first.Scope, first.Round.Step); !errors.Is(err, ErrConflict) {
		t.Fatalf("old slot relabelled as current: %v", err)
	}
	runBounded(t, d)
	second, err := s.ReadPublishedRound(context.Background(), a.JobID, 2)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(a.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Scope.StepSlot == StepSlotID(second.Scope, second.Round.Step.EntryRef, second.Round.Step.Component) {
		t.Fatal("fixture did not preserve distinct initial/current slots")
	}
	got, err = rootRoundRead(t, s, job.Scope, second.Round.Step)
	if err != nil || !reflect.DeepEqual(got, second) || got.Round.Generation != job.Generation {
		t.Fatalf("current round borrowed initial slot/history: %+v %v", got, err)
	}
	if _, err = rootRoundRead(t, s, second.Scope, first.Round.Step); !errors.Is(err, ErrConflict) {
		t.Fatalf("historical source accepted after real successor completion: %v", err)
	}
	retained, err := s.ReadPublishedRound(context.Background(), a.JobID, 1)
	if err != nil || !reflect.DeepEqual(retained, first) {
		t.Fatalf("public historical reader changed %+v %v", retained, err)
	}
	wrong := second.Round.Step
	wrong.ContinuationOf = "work1#2"
	if _, err = rootRoundRead(t, s, second.Scope, wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("same slot different full step accepted: %v", err)
	}
}
func TestCurrentRootPublishedRoundRejectsForeignAndMissingBinding(t *testing.T) {
	s, _, a := publishedRoundFixture(t)
	actual, err := s.ReadPublishedRound(context.Background(), a.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"generation", "owner", "work", "root", "entry", "component", "invalid"} {
		t.Run(field, func(t *testing.T) {
			scope, step := actual.Scope, actual.Round.Step
			switch field {
			case "generation":
				scope.Generation = "foreign"
			case "owner":
				scope.OwnerID = "foreign"
			case "work":
				scope.WorkID = "foreign"
			case "root":
				scope.OutputRoot = filepath.Join(scope.OutputRoot, "foreign")
			case "entry":
				step.EntryRef = "work1#99"
			case "component":
				step.Component = "verifier"
			case "invalid":
				step.EntryRef = ""
			}
			if _, err := rootRoundRead(t, s, scope, step); err == nil {
				t.Fatal("foreign current root binding accepted", field)
			}
		})
	}
	original := rootRoundLedger(t, s)
	for _, fault := range []string{"index", "generation", "slot", "output"} {
		t.Run(fault, func(t *testing.T) {
			defer func() {
				if e := os.WriteFile(filepath.Join(s.root(), "ledger.json"), original, 0600); e != nil {
					t.Error(e)
				}
			}()
			if fault == "output" {
				result, e := s.Result(a.JobID, 1)
				if e != nil {
					t.Fatal(e)
				}
				data, e := os.ReadFile(result.OutputFile)
				if e != nil {
					t.Fatal(e)
				}
				defer func() {
					if err := os.WriteFile(result.OutputFile, data, 0600); err != nil {
						t.Error(err)
					}
				}()
				if e = os.WriteFile(result.OutputFile, []byte("changed"), 0600); e != nil {
					t.Fatal(e)
				}
			} else if e := s.transaction(context.Background(), true, func(db *ledger) error {
				q := db.Jobs[a.JobID].Rounds[1]
				switch fault {
				case "index":
					db.DispatchSlots[q.StepSlot] = "missing-job"
				case "generation":
					q.JobGeneration = 0
				case "slot":
					q.StepSlot = "foreign"
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if _, err := rootRoundRead(t, s, actual.Scope, actual.Round.Step); err == nil {
				t.Fatal("corrupt current source accepted", fault)
			}
		})
	}
}
func TestCurrentRootPublishedRoundWaitsForActualPublicationAndDelivery(t *testing.T) {
	s, d, req, auth := setupStep(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(auth.scope.OutputRoot, "implement.v1.md")
	a := mustSubmit(t, s, req)
	step := StepBasis{EntryRef: "work1#1", Component: "implementor"}
	scope := auth.scope.Authorization()
	if _, err := rootRoundRead(t, s, scope, step); err == nil || !strings.Contains(err.Error(), "completion evidence unavailable") {
		t.Fatalf("queued root was published: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	active, runNext, stored, flushNext, delivering, deliveryNext := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		close(active)
		select {
		case <-runNext:
		case <-ctx.Done():
			return RunResult{}, ctx.Err()
		}
		if err := os.WriteFile(spec.OutputPath, []byte("## Result\nActual published output.\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordCodexTurn(ctx, spec, k)
	}}
	d.Service.testCheckpoint = func(at string) {
		if at == "result_stored" {
			close(stored)
			select {
			case <-flushNext:
			case <-ctx.Done():
			}
		}
	}
	d.Notify = func(ctx context.Context, _ Job, _ RoundResult) error {
		close(delivering)
		select {
		case <-deliveryNext:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	joined := false
	go func() { done <- Run(ctx, d) }()
	wait := func(stage <-chan struct{}) {
		t.Helper()
		select {
		case <-stage:
		case <-ctx.Done():
			t.Fatal("actual broker stage did not arrive", ctx.Err())
		}
	}
	t.Cleanup(func() {
		cancel()
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("actual broker did not join")
		}
	})
	wait(active)
	if _, err := rootRoundRead(t, s, scope, step); err == nil || !strings.Contains(err.Error(), "completion evidence unavailable") {
		t.Fatalf("active root was published: %v", err)
	}
	close(runNext)
	wait(stored)
	if _, err := rootRoundRead(t, s, scope, step); err == nil || !strings.Contains(err.Error(), "completion evidence unavailable") {
		t.Fatalf("unpublished stored result qualified: %v", err)
	}
	close(flushNext)
	wait(delivering)
	if _, err := rootRoundRead(t, s, scope, step); err == nil || !strings.Contains(err.Error(), "completion evidence unavailable") {
		t.Fatalf("undelivered result qualified: %v", err)
	}
	close(deliveryNext)
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	got, err := rootRoundRead(t, s, scope, step)
	if err != nil || got.Round.JobID != a.JobID || got.Round.Round != 1 {
		t.Fatalf("normal publication did not unlock factual source %+v %v", got, err)
	}
}
