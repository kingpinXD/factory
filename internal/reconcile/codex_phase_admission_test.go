package reconcile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexDownstreamAdmissionRequiresCurrentPhase(t *testing.T) {
	for _, test := range []struct{ component, phase, file string }{{"verifier", "verifying", "verify.v1.md"}, {"pr-opener", "pr_open", "pr.v1.md"}} {
		t.Run(test.component, func(t *testing.T) {
			w, s, req, _, _ := codexStartFixture(t)
			req.Component = test.component
			req.OutputPath = filepath.Join(w.dir(req.WorkID), test.file)
			ctx := context.Background()
			if _, err := s.Submit(ctx, req); err == nil || !strings.Contains(err.Error(), test.phase) {
				t.Fatalf("downstream admitted while implementing: %v", err)
			}
			jobs, _ := s.Jobs()
			if len(jobs) != 0 {
				t.Fatal("phase refusal reserved a job")
			}
			w.moveTo(req.WorkID, test.phase)
			accepted, err := s.Submit(ctx, req)
			if err != nil {
				t.Fatal("matching phase denied", err)
			}
			job, _ := s.Job(accepted.JobID)
			start := s.Authority.(interface {
				CheckStart(context.Context, helper.Job) error
			})
			if err := start.CheckStart(ctx, job); err != nil {
				t.Fatal("matching queued phase refused", err)
			}
			w.moveTo(req.WorkID, "implementing")
			if err := s.Authority.Check(ctx, job); err != nil {
				t.Fatal("new phase guard leaked into existing-job authority", err)
			}
			if got, err := s.Submit(ctx, req); err != nil || got.JobID != accepted.JobID {
				t.Fatalf("exact-key recovery changed: %+v %v", got, err)
			}
			wrong := req
			wrong.Prompt = "changed"
			if _, err := s.Submit(ctx, wrong); !errors.Is(err, helper.ErrConflict) {
				t.Fatalf("phase masked key mismatch: %v", err)
			}
			ledger := filepath.Join(s.Brain, "factory/helpers/ledger.json")
			before, _ := os.ReadFile(ledger)
			wrong = req
			wrong.RequestKey = "fresh-wrong-phase"
			if _, err := s.Submit(ctx, wrong); err == nil || !strings.Contains(err.Error(), test.phase) {
				t.Fatalf("new key bypassed phase: %v", err)
			}
			after, _ := os.ReadFile(ledger)
			if !bytes.Equal(before, after) {
				t.Fatal("wrong phase mutated ledger")
			}
			if err := start.CheckStart(ctx, job); err == nil || !strings.Contains(err.Error(), test.phase) || strings.Contains(err.Error(), "not admitted") {
				t.Fatalf("queued helper started in wrong phase: %v", err)
			}
			if err := s.Cancel(ctx, accepted.JobID, "phase changed"); err != nil {
				t.Fatal("phase blocked fencing/cancel", err)
			}
		})
	}
}

func TestCodexPhaseAdmissionLeavesOtherComponentsUnchanged(t *testing.T) {
	_, s, req, _, _ := codexStartFixture(t)
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal("reviewer phase was restricted", err)
	}
}

func TestCodexPhaseGuardDoesNotBecomeCommonAuthority(t *testing.T) {
	w, s, req, _, _ := codexStartFixture(t)
	ctx := context.Background()
	scope, err := s.Authority.Resolve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	phase, ok := s.Authority.(interface {
		CheckNewRound(context.Context, helper.Job) error
	})
	if !ok {
		t.Fatal("missing new-round phase check")
	}
	job := helper.Job{ID: "known-job", Request: req, Scope: scope, Route: executor.Route{Provider: "codex"}, Round: 1}
	job.Request.Component = "verifier"
	var mismatch *helper.PhaseMismatchError
	if err := phase.CheckNewRound(ctx, job); !errors.As(err, &mismatch) || mismatch.ActualPhase != "implementing" || mismatch.RequiredPhase != "verifying" {
		t.Fatalf("wrong phase facts: %+v %v", mismatch, err)
	}
	for _, component := range []string{"reviewer-code", "reviewer-arch-test", "reviewer-issue-completeness", "reviewer-red-team", "implementor"} {
		nested := job
		nested.Request.Component = component
		nested.Request.ParentJobID = "executing-parent"
		if err := phase.CheckNewRound(ctx, nested); err != nil {
			t.Fatalf("nested %s got phase restriction: %v", component, err)
		}
	}
	workOwned := job
	workOwned.Scope.OwnerID = workOwned.Scope.WorkID
	workOwned.Scope.SetID = ""
	workOwned.Request.Mode = helper.ModeExistingPRRevision
	if err := phase.CheckNewRound(ctx, workOwned); err != nil {
		t.Fatal("work-owned babysitter path restricted", err)
	}
	if err := os.WriteFile(store.EventsPath(w.dir(req.WorkID)), []byte("invalid event storage"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := phase.CheckNewRound(ctx, job); err == nil || errors.As(err, &mismatch) {
		t.Fatalf("storage failure became correctable phase fact: %v", err)
	}
}
