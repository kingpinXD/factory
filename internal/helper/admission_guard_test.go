package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
)

func TestManagedAdmissionOwnsCheckAndActionUntilFence(t *testing.T) {
	s, _, req := setup(t)
	j, _ := preparedAttempt(t, s, req, true)
	entered, release := make(chan struct{}), make(chan struct{})
	action := make(chan error, 1)
	go func() {
		_, err := s.WithManagedAdmission(context.Background(), j.ID, j.Generation, func(job Job) error {
			if job.ID != j.ID {
				return ErrConflict
			}
			close(entered)
			<-release
			return nil
		})
		action <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := s.Fence(ctx, req.WorkID, "transfer waits for admitted write"); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-action; err != nil {
		t.Fatal(err)
	}
	s.FenceRuntime = func(context.Context, Job) error { return nil }
	if err := s.Fence(context.Background(), req.WorkID, "transfer"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ManagedAdmission(context.Background(), j.ID, j.Generation); err == nil {
		t.Fatal("fenced managed writer admitted")
	}
}
func TestManagedAdmissionRejectsStaleBindingAndInterrupt(t *testing.T) {
	s, _, req := setup(t)
	j, _ := preparedAttempt(t, s, req, true)
	if _, err := s.ManagedAdmission(context.Background(), j.ID, j.Generation+1); err == nil {
		t.Fatal("wrong job generation admitted")
	}
	if _, err := s.Interrupt(context.Background(), j.ID, j.Round, "operator"); err != nil {
		t.Fatal(err)
	}
	called := false
	if _, err := s.WithManagedAdmission(context.Background(), j.ID, j.Generation, func(Job) error { called = true; return nil }); err == nil || called {
		t.Fatal("new action admitted after interrupt intent")
	}
}
func TestUnmanagedGuardExcludesReservationsAliasesAndConcurrentSubmit(t *testing.T) {
	s, _, req := setup(t)
	req.Component = "implementor"
	root := s.Authority.(*fakeAuthority).scope.OutputRoot
	req.OutputPath = filepath.Join(root, "implement.v1.md")
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(req.OutputPath, []byte("## Result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(root, "linked.md")
	if err := os.Link(req.OutputPath, hardlink); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{req.OutputPath, filepath.Join(alias, "implement.v1.md"), hardlink} {
		called := false
		_, err := s.WithUnmanagedEvent(context.Background(), "unrelated", events.KindEnd, file, func() (events.Event, error) { called = true; return events.Event{}, nil })
		if err == nil || called {
			t.Fatalf("reserved alias accepted: %s %v", file, err)
		}
	}
	// The same guard also covers an unreserved file until its manual append ends.
	s2, _, req2 := setup(t)
	req2.Component = "implementor"
	req2.OutputPath = filepath.Join(s2.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := s2.WithUnmanagedEvent(context.Background(), req2.WorkID, events.KindEnd, req2.OutputPath, func() (events.Event, error) {
			close(entered)
			<-release
			return events.Event{}, os.WriteFile(req2.OutputPath, []byte("manual report"), 0600)
		})
		done <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := s2.Submit(ctx, req2); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Submit(context.Background(), req2); err == nil {
		t.Fatal("submit claimed existing manual output")
	}
}
func TestGuardsRejectCorruptLedgerAndPreserveLegacyEvents(t *testing.T) {
	s, _, _ := setup(t)
	called := false
	if _, err := s.WithUnmanagedEvent(context.Background(), "legacy", events.KindEnd, "", func() (events.Event, error) { called = true; return events.Event{Seq: 1}, nil }); err != nil || !called {
		t.Fatal(err)
	}
	if err := durableWrite(filepath.Join(s.root(), "ledger.json"), []byte(`{"schema_version":9}`), durableOps{}); err != nil {
		t.Fatal(err)
	}
	called = false
	if _, err := s.WithUnmanagedEvent(context.Background(), "legacy", events.KindEnd, "", func() (events.Event, error) { called = true; return events.Event{}, nil }); err == nil || called {
		t.Fatal("corrupt ledger allowed publication")
	}
}
