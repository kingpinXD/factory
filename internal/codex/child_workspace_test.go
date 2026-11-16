package codex

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/zednative"
)

type childWorkspaceContainment struct {
	proof   zednative.WorkspaceSettlement
	changed bool
	guarded bool
}

func (*childWorkspaceContainment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	return RemoteEnvironment{}, ErrObservationUnknown
}
func (*childWorkspaceContainment) Check(context.Context, Owner) error   { return ErrObservationUnknown }
func (*childWorkspaceContainment) Fence(context.Context, Owner) error   { return nil }
func (*childWorkspaceContainment) Promote(context.Context, Owner) error { return nil }
func (c *childWorkspaceContainment) ObserveSettledWorkspace(context.Context, Owner) (zednative.WorkspaceSettlement, error) {
	return c.proof, nil
}
func (c *childWorkspaceContainment) WithSettledWorkspaceGuard(_ context.Context, _ Owner, p zednative.WorkspaceSettlement, fn func(zednative.WorkspaceSettlementGuard) error) error {
	if p != c.proof {
		return ErrObservationUnknown
	}
	c.guarded = true
	defer func() { c.guarded = false }()
	return fn(c)
}
func (c *childWorkspaceContainment) ValidateLocal(context.Context) (zednative.WorkspaceSettlement, error) {
	if !c.guarded {
		return zednative.WorkspaceSettlement{}, ErrObservationUnknown
	}
	p := c.proof
	if c.changed {
		p.OwnerSHA256 = strings.Repeat("e", 64)
	}
	return p, nil
}

func settledChildFixture(t *testing.T) (*Backend, helper.Job, helperAttempt, *childWorkspaceContainment) {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	job := helper.Job{ID: "child-workspace", Round: 1, Generation: 2, Scope: helper.Scope{Worktree: brain, OutputRoot: brain, OwnerID: "owner", EpicID: "epic", WorkID: "item"}, Route: executor.Route{Provider: "codex", Model: "gpt-6.1-sol", Effort: "high"}}
	b := &Backend{Brain: brain}
	name := filepath.Join(b.directory(job), "codex.json")
	cmd := exec.Command("/bin/sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := CaptureProcess(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	a := helperAttempt{Schema: 1, Job: job, Process: identity, ThreadID: "thread", TurnID: "turn", TurnIntent: true, Ended: true, Reply: "truthful checkpoint", Fenced: true, Closed: true}
	a.Proof = &PersistedTurn{Policy: EvidencePolicy, ConversationID: a.ThreadID, TurnID: a.TurnID, ConfiguredModel: job.Route.Model, ConfiguredEffort: job.Route.Effort, Reply: a.Reply, ReplySHA256: hash([]byte(a.Reply)), VerifiedAt: time.Now().UTC()}
	if err := writeJSON(name, a); err != nil {
		t.Fatal(err)
	}
	c := &childWorkspaceContainment{proof: zednative.WorkspaceSettlement{OwnerSHA256: strings.Repeat("a", 64), ContainmentRef: filepath.Join(b.directory(job), "remote", "containment.json"), ContainmentSHA256: strings.Repeat("b", 64), PromotionRef: filepath.Join(b.directory(job), "remote", "containment.json"), PromotionSHA256: strings.Repeat("c", 64), PublishedGitSHA256: strings.Repeat("d", 64)}}
	b.Containment = c
	return b, job, a, c
}

func TestChildWorkspaceBindsActualAttemptAndHoldsGuard(t *testing.T) {
	b, job, _, c := settledChildFixture(t)
	ctx := context.Background()
	proof, err := b.ObserveChildWorkspace(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if proof.AttemptRef != filepath.Join(b.directory(job), "codex.json") || len(proof.AttemptSHA256) != 64 || proof.JobID != job.ID || proof.Generation != 2 || proof.Round != 1 {
		t.Fatalf("unbound attempt %+v", proof)
	}
	called := false
	err = b.WithChildWorkspaceGuard(ctx, job, proof, func(g helper.ChildWorkspaceGuard) error {
		called = true
		if !c.guarded {
			t.Fatal("containment guard released early")
		}
		unlock, err := store.Lock(b.Brain, "codex-helper-"+job.ID+"-"+strconv.Itoa(job.Round))
		if err == nil {
			unlock()
			t.Fatal("attempt guard released early")
		}
		got, err := g.ValidateLocal(ctx)
		if err != nil {
			return err
		}
		if got.Observation != proof || len(got.FinalLocalSHA256) != 64 {
			t.Fatal("wrong final receipt")
		}
		return nil
	})
	if err != nil || !called {
		t.Fatal(err, called)
	}
	unlock, err := b.lockChildAttempt(ctx, job)
	if err != nil {
		t.Fatal("guard leaked", err)
	}
	unlock()
}

func TestChildWorkspaceRequiresExactTerminalEvidence(t *testing.T) {
	tests := map[string]func(*helperAttempt){
		"closed":       func(a *helperAttempt) { a.Closed = false },
		"fenced":       func(a *helperAttempt) { a.Fenced = false },
		"ended":        func(a *helperAttempt) { a.Ended = false },
		"rerouted":     func(a *helperAttempt) { a.Rerouted = true },
		"intent":       func(a *helperAttempt) { a.TurnIntent = false },
		"prelaunch":    func(a *helperAttempt) { a.PrelaunchError = "failure" },
		"thread":       func(a *helperAttempt) { a.ThreadID = "other" },
		"turn":         func(a *helperAttempt) { a.TurnID = "other" },
		"proof":        func(a *helperAttempt) { a.Proof = nil },
		"policy":       func(a *helperAttempt) { a.Proof.Policy = "wrong" },
		"verified":     func(a *helperAttempt) { a.Proof.VerifiedAt = time.Time{} },
		"model":        func(a *helperAttempt) { a.Proof.ConfiguredModel = "wrong" },
		"effort":       func(a *helperAttempt) { a.Proof.ConfiguredEffort = "wrong" },
		"reply":        func(a *helperAttempt) { a.Proof.Reply = "partial" },
		"reply-hash":   func(a *helperAttempt) { a.Proof.ReplySHA256 = strings.Repeat("f", 64) },
		"process":      func(a *helperAttempt) { a.Process = ProcessIdentity{} },
		"live-process": func(a *helperAttempt) { a.Process.PID = os.Getpid(); a.Process.Group = syscall.Getpgrp() },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			b, job, a, _ := settledChildFixture(t)
			change(&a)
			if err := writeJSON(filepath.Join(b.directory(job), "codex.json"), a); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ObserveChildWorkspace(context.Background(), job); err == nil {
				t.Fatal("incomplete evidence qualified")
			}
		})
	}
}

func TestChildWorkspaceRejectsChangedAttemptOrRemoteProof(t *testing.T) {
	for _, fault := range []string{"before", "final", "remote", "foreign-job", "foreign-generation", "foreign-round", "foreign-scope", "foreign-request", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			b, job, a, c := settledChildFixture(t)
			ctx := context.Background()
			proof, err := b.ObserveChildWorkspace(ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			change := func() {
				a.Output = "changed"
				if err := writeJSON(filepath.Join(b.directory(job), "codex.json"), a); err != nil {
					t.Fatal(err)
				}
			}
			switch fault {
			case "before":
				change()
			case "foreign-job":
				job.ID = "foreign"
			case "foreign-generation":
				job.Generation++
			case "foreign-round":
				job.Round++
			case "foreign-scope":
				job.Scope.OwnerID = "foreign"
			case "foreign-request":
				job.Request.Prompt = "foreign"
			}
			entered := false
			err = b.WithChildWorkspaceGuard(ctx, job, proof, func(g helper.ChildWorkspaceGuard) error {
				entered = true
				if fault == "final" {
					change()
				}
				if fault == "remote" {
					c.changed = true
				}
				if fault == "cancelled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				_, err := g.ValidateLocal(ctx)
				return err
			})
			if err == nil {
				t.Fatal("changed proof accepted")
			}
			if fault == "before" && entered {
				t.Fatal("changed attempt entered final commit")
			}
		})
	}
}

func TestChildWorkspaceAttemptWaitHonorsCallerDeadline(t *testing.T) {
	b, job, _, _ := settledChildFixture(t)
	unlock, err := b.lockChildAttempt(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = b.ObserveChildWorkspace(ctx, job)
	unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("attempt wait=%v", err)
	}
	if _, err = b.ObserveChildWorkspace(context.Background(), job); err != nil {
		t.Fatal("wait poisoned attempt", err)
	}
}

func TestChildWorkspaceRechecksAfterLedgerWait(t *testing.T) {
	b, job, a, _ := settledChildFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	proof, err := b.ObserveChildWorkspace(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	releaseLedger, err := store.Lock(b.Brain, "helper-ledger")
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- b.WithChildWorkspaceGuard(ctx, job, proof, func(g helper.ChildWorkspaceGuard) error {
			signalled := false
			for {
				unlock, err := store.Lock(b.Brain, "helper-ledger")
				if err == nil {
					defer unlock()
					_, err = g.ValidateLocal(ctx)
					return err
				}
				var busy *store.LockedError
				if !errors.As(err, &busy) {
					return err
				}
				if !signalled {
					close(waiting)
					signalled = true
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
		})
	}()
	select {
	case <-waiting:
	case <-ctx.Done():
		releaseLedger()
		t.Fatal(ctx.Err())
	}
	attemptUnlock, err := store.Lock(b.Brain, "codex-helper-"+job.ID+"-"+strconv.Itoa(job.Round))
	if err == nil {
		attemptUnlock()
		releaseLedger()
		t.Fatal("ledger wait released attempt guard")
	}
	a.Output = "changed while waiting for ledger"
	if err := writeJSON(filepath.Join(b.directory(job), "codex.json"), a); err != nil {
		releaseLedger()
		t.Fatal(err)
	}
	releaseLedger()
	if err := <-done; err == nil {
		t.Fatal("changed attempt crossed ledger wait")
	}
}

func TestChildWorkspaceGuardAndExistingFenceRemainLive(t *testing.T) {
	b, job, _, _ := settledChildFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	proof, err := b.ObserveChildWorkspace(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	guardDone := make(chan error, 1)
	go func() {
		guardDone <- b.WithChildWorkspaceGuard(ctx, job, proof, func(g helper.ChildWorkspaceGuard) error {
			close(entered)
			<-release
			_, err := g.ValidateLocal(ctx)
			return err
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	fenceDone := make(chan error, 1)
	go func() { fenceDone <- b.Fence(ctx, job) }()
	waitCtx, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	unlock, err := b.lockChildAttempt(waitCtx, job)
	stop()
	if err == nil {
		unlock()
		close(release)
		t.Fatal("concurrent guard lost its attempt lock")
	}
	close(release)
	if err := <-guardDone; err != nil {
		t.Fatal("guard", err)
	}
	if err := <-fenceDone; err != nil {
		t.Fatal("existing fence", err)
	}
}
