package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type startTestAuthority struct {
	Authority
	denied bool
	checks int
}

func (a *startTestAuthority) CheckStart(context.Context, Job) error {
	a.checks++
	if a.denied {
		return ErrAuthority
	}
	return nil
}
func TestQueuedStartAuthorityIsRecheckedAtExecutionClaim(t *testing.T) {
	s, d, req := setup(t)
	a := &startTestAuthority{Authority: s.Authority}
	s.Authority = a
	d.Service = s
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	candidates, _, err := queuedCandidates(context.Background(), s)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidate %v %v", candidates, err)
	}
	a.denied = true
	ran := false
	d.Backends["codex"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) { ran = true; return RunResult{}, nil }}
	err = execute(context.Background(), d, candidates[0], nil, nil)
	if !errors.Is(err, ErrAuthority) || ran {
		t.Fatalf("revocation before claim ignored: err=%v ran=%v", err, ran)
	}
	job, _ := s.Job(accepted.JobID)
	if job.State != "queued" || a.checks < 2 {
		t.Fatalf("claim mutated held job: %+v checks=%d", job, a.checks)
	}
}
func TestTerminalRecoveryEvidenceDistinguishesLaunchAndSettlement(t *testing.T) {
	for _, kind := range []string{"prepared", "launched", "unpublished", "unknown", "descendant"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := setupCodex(t)
			accepted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Cancel(context.Background(), accepted.JobID, "restart coordinator boundary"); err != nil {
				t.Fatal(err)
			}
			err = s.transaction(context.Background(), true, func(db *ledger) error {
				r := db.Jobs[accepted.JobID]
				q := r.Rounds[1]
				q.Published, q.Delivered = true, true
				switch kind {
				case "launched":
					q.Phase = EventTurnIdentified
					q.Evidence[EventTurnRequested+":recorded"] = "true"
				case "unpublished":
					q.Published = false
				case "unknown":
					setState(r, "unknown", "writer_death_unconfirmed")
				case "descendant":
					child := *r
					child.Job.ID = "child"
					child.Job.Request.ParentJobID = r.Job.ID
					child.Rounds = map[int]*roundRecord{1: newRound(child.Job, "child", "")}
					setState(&child, "unknown", "fencing")
					db.Jobs["child"] = &child
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := s.Result(accepted.JobID, 1)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(result)
			proof := result.Recovery
			if proof == nil {
				t.Fatal("terminal result omits known fencing/recovery facts")
			}
			if proof.NoLaunch != (kind != "launched") || !proof.Stopped {
				t.Fatalf("wrong launch/stopped proof: %s %s", kind, data)
			}
			if proof.DescendantsStopped != (kind != "descendant") || proof.PublicationSettled != (kind != "unpublished" && kind != "unknown" && kind != "descendant") {
				t.Fatalf("unsafe recovery proof: %s %s", kind, data)
			}
		})
	}
}

func TestRecoveryAfterValidClaimKeepsJournalAndFenceBarrier(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			s, d, req := setupCodex(t)
			a := &startTestAuthority{Authority: s.Authority}
			s.Authority = a
			d.Service = s
			accepted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			job, _ := s.Job(accepted.JobID)
			claimed, release := make(chan struct{}), make(chan struct{})
			d.Service.testCheckpoint = func(stage string) {
				if stage == "execution_claimed" {
					close(claimed)
					<-release
				}
			}
			var runs, effects, fences atomic.Int32
			backend := backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
				runs.Add(1)
				if err := sink.Record(ctx, EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "fixture"}); err != nil {
					return RunResult{}, err
				}
				_, err := s.WithManagedAdmission(ctx, spec.Job.ID, spec.Job.Generation, func(Job) error { effects.Add(1); return nil })
				return RunResult{}, err
			}, fence: func(context.Context, Job) error {
				fences.Add(1)
				if uncertain {
					return errors.New("writer death uncertain")
				}
				return nil
			}}
			s.FenceRuntime = backend.Fence
			d.Service.FenceRuntime = backend.Fence
			d.Backends["codex"] = backend
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- execute(ctx, d, job, nil, nil) }()
			select {
			case <-claimed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Recovery becomes visible after a valid claim, before backend preparation.
			err = s.Fence(ctx, req.OwnerID, "restart coordinator boundary")
			if (err != nil) != uncertain {
				t.Fatalf("fence result %v", err)
			}
			close(release)
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = flush(ctx, d); err != nil {
				t.Fatal(err)
			}
			result, err := s.Result(job.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			want := "cancelled"
			if uncertain {
				want = "unknown"
			}
			if result.State != want || runs.Load() != 1 || effects.Load() != 0 || fences.Load() == 0 {
				t.Fatalf("post-claim barrier failed: result=%+v runs=%d effects=%d fences=%d", result, runs.Load(), effects.Load(), fences.Load())
			}
			if result.Recovery == nil || result.Recovery.NoLaunch || result.Recovery.PublicationSettled == uncertain {
				t.Fatalf("untruthful post-claim proof: %+v", result.Recovery)
			}
		})
	}
}

func TestClaimedWriterProcess(t *testing.T) {
	if os.Getenv("FACTORY_CLAIM_WRITER") != "1" {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}
func TestClaimedWriterIsPhysicallyFencedBeforeTerminalRecovery(t *testing.T) {
	for _, order := range []string{"runner-first", "cancel-first"} {
		t.Run(order, func(t *testing.T) {
			s, d, req := setupCodex(t)
			accepted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			job, _ := s.Job(accepted.JobID)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			returnRun := make(chan struct{})
			var releaseRun sync.Once
			defer releaseRun.Do(func() { close(returnRun) })
			processDone := make(chan struct{})
			var cmd *exec.Cmd
			var input io.WriteCloser
			var stop sync.Once
			fence := func(context.Context, Job) error {
				stop.Do(func() { _ = input.Close() })
				<-processDone
				if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
					return errors.New("physical writer not stopped")
				}
				return nil
			}
			backend := backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
				cmd = exec.Command(binary, "-test.run=^TestClaimedWriterProcess$")
				cmd.Env = append(os.Environ(), "FACTORY_CLAIM_WRITER=1")
				input, err = cmd.StdinPipe()
				if err != nil {
					return RunResult{}, err
				}
				if err = cmd.Start(); err != nil {
					return RunResult{}, err
				}
				if err = sink.Record(ctx, EventProcessOwned, map[string]string{"pid": strconv.Itoa(cmd.Process.Pid), "process_group": strconv.Itoa(cmd.Process.Pid), "start_identity": "owned-test-process"}); err != nil {
					_ = input.Close()
					_ = cmd.Wait()
					close(processDone)
					return RunResult{}, err
				}
				close(started)
				waitErr := cmd.Wait()
				close(processDone)
				if order == "cancel-first" {
					<-returnRun
				}
				return RunResult{}, waitErr
			}, fence: fence}
			s.FenceRuntime = fence
			d.Service = s
			d.Backends["codex"] = backend
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			executeFinished := make(chan struct{})
			s.testCheckpoint = func(stage string) {
				if order == "runner-first" && stage == "fence_revoking_committed" {
					// Let the runner observe revoked admission and physically stop the writer.
					select {
					case <-executeFinished:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if order == "cancel-first" && stage == "fence_stopping_complete" {
					// The cancellation is durable before Run can return to finish.
					releaseRun.Do(func() { close(returnRun) })
				}
			}
			go func() {
				done <- execute(ctx, d, job, nil, nil)
				close(executeFinished)
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = s.Fence(ctx, req.OwnerID, "restart coordinator boundary"); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = flush(ctx, d); err != nil {
				t.Fatal(err)
			}
			result, err := s.Result(job.ID, 1)
			wantState, wantCode := "cancelled", "restart coordinator boundary"
			if order == "runner-first" {
				wantState, wantCode = "interrupted", "authority_revoked"
			}
			if err != nil || result.State != wantState || result.ErrorCode != wantCode || result.Recovery == nil || !result.Recovery.Stopped || !result.Recovery.PublicationSettled || result.Recovery.NoLaunch {
				t.Fatalf("writer fence proof wrong: %+v recovery=%+v %v", result, result.Recovery, err)
			}
		})
	}
}
func TestQueuedStartAuthorityHoldsCandidateSelection(t *testing.T) {
	s, _, req := setupCodex(t)
	s.Authority = &startTestAuthority{Authority: s.Authority, denied: true}
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("start hold incorrectly prevented queue admission: %v", err)
	}
	candidates, pending, err := queuedCandidates(context.Background(), s)
	if err != nil || !pending || len(candidates) != 0 {
		t.Fatalf("held coordinator remained dispatch candidate: candidates=%+v pending=%v err=%v", candidates, pending, err)
	}
	job, err := s.Job(accepted.JobID)
	if err != nil || job.State != "queued" {
		t.Fatalf("selection rewrote held job: %+v err=%v", job, err)
	}
}
