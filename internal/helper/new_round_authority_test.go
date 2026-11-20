package helper

import (
	"bytes"
	"context"
	"errors"
	"github.com/kingpinXD/factory/internal/events"
	"os"
	"path/filepath"
	"testing"
)

var errNewRoundDenied = errors.New("new round phase denied")

type newRoundTestAuthority struct {
	Authority
	denied bool
	checks int
}

func (a *newRoundTestAuthority) CheckNewRound(context.Context, Job) error {
	a.checks++
	if a.denied {
		return errNewRoundDenied
	}
	return nil
}

func TestNewRoundAuthorityPreservesAdmissionAndContinuationPriority(t *testing.T) {
	s, d, req := setupCodex(t)
	base := s.Authority.(*fakeAuthority)
	a := &newRoundTestAuthority{Authority: s.Authority, denied: true}
	s.Authority = a
	d.Service = s
	ctx := context.Background()
	ledger := filepath.Join(s.root(), "ledger.json")
	if _, err := s.Submit(ctx, req); !errors.Is(err, errNewRoundDenied) {
		t.Fatalf("new admission ignored phase: %v", err)
	}
	if jobs, _ := s.Jobs(); len(jobs) != 0 {
		t.Fatal("phase denial admitted job", jobs)
	}
	a.denied = false
	accepted, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	checks := a.checks
	a.denied = true
	if got, err := s.Submit(ctx, req); err != nil || got.JobID != accepted.JobID || a.checks != checks {
		t.Fatalf("same key cannot recover after phase changes: %+v %v", got, err)
	}
	wrong := req
	wrong.Prompt = "changed"
	if _, err := s.Submit(ctx, wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("phase masked key conflict: %v", err)
	}
	base.deny = true
	if _, err := s.Submit(ctx, req); !errors.Is(err, ErrAuthority) {
		t.Fatalf("same key bypassed authority: %v", err)
	}
	base.deny = false
	a.denied = false
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	a.denied = true
	next := ContinueRequest{SchemaVersion: 1, RequestKey: "new-phase-round", ExpectedRound: 2, Prompt: "review again"}
	if _, err := s.Continue(ctx, accepted.JobID, next); !errors.Is(err, ErrConflict) || errors.Is(err, errNewRoundDenied) {
		t.Fatalf("phase masked current round mismatch: %v", err)
	}
	next.ExpectedRound = 1
	before, _ := os.ReadFile(ledger)
	if _, err := s.Continue(ctx, accepted.JobID, next); !errors.Is(err, errNewRoundDenied) {
		t.Fatalf("new continuation ignored phase: %v", err)
	}
	after, _ := os.ReadFile(ledger)
	if !bytes.Equal(before, after) {
		t.Fatal("phase denial mutated continuation/key/reservation")
	}
	a.denied = false
	second, err := s.Continue(ctx, accepted.JobID, next)
	if err != nil {
		t.Fatal(err)
	}
	a.denied = true
	checks = a.checks
	if got, err := s.Continue(ctx, accepted.JobID, next); err != nil || got.Round != second.Round || a.checks != checks {
		t.Fatalf("same continuation cannot recover after phase changes: %+v %v", got, err)
	}
	outstanding := next
	outstanding.RequestKey = "still-running-round"
	outstanding.ExpectedRound = 2
	if _, err := s.Continue(ctx, accepted.JobID, outstanding); err == nil || errors.Is(err, errNewRoundDenied) || err.Error() != "continuation requires confirmed stopped matching conversation and turn checkpoint" {
		t.Fatalf("phase masked active round guard: %v", err)
	}
	changed := next
	changed.Prompt = "changed"
	if _, err := s.Continue(ctx, accepted.JobID, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("phase masked continuation key conflict: %v", err)
	}
	base.deny = true
	if _, err := s.Continue(ctx, accepted.JobID, next); !errors.Is(err, ErrAuthority) {
		t.Fatalf("continued key bypassed authority: %v", err)
	}
	base.deny = false
	if _, err := s.Result(accepted.JobID, 1); err != nil {
		t.Fatal("phase broke existing result", err)
	}
	if err := s.Cancel(ctx, accepted.JobID, "normal cancellation despite phase"); err != nil {
		t.Fatal("phase broke cancellation", err)
	}
}

func TestManagedLifecycleBusyNamesTheExactUnsettledRound(t *testing.T) {
	s, _, req := setupCodex(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	_, err = s.WithUnmanagedEvent(context.Background(), req.OwnerID, "start", "", func() (events.Event, error) { called = true; return events.Event{}, nil })
	var busy *ManagedLifecycleBusyError
	if !errors.As(err, &busy) || busy.JobID != accepted.JobID || busy.Round != 1 || err.Error() != "managed lifecycle belongs to the runner" || called {
		t.Fatalf("wrong blocker/legacy bytes: %+v %v callback=%v", busy, err, called)
	}
}

func TestNewRoundAuthorityDoesNotMaskStorageFailures(t *testing.T) {
	s, _, req := setupCodex(t)
	a := &newRoundTestAuthority{Authority: s.Authority, denied: true}
	s.Authority = a
	if err := os.MkdirAll(s.root(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.root(), "ledger.json"), []byte("invalid ledger"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(context.Background(), req); err == nil || errors.Is(err, errNewRoundDenied) || a.checks != 0 {
		t.Fatalf("phase check masked storage failure: checks=%d err=%v", a.checks, err)
	}
}
