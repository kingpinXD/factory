package helper

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func closedOutputFixture(t *testing.T) (Service, Scope, string) {
	t.Helper()
	s, _, _, authority := setupStep(t)
	scope := authority.scope.Authorization()
	admitted, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, admitted.Token); err != nil {
		t.Fatal(err)
	}
	return s, scope, admitted.Token
}

func TestClosedControlOutputFenceActualClosedNoJob(t *testing.T) {
	s, scope, token := closedOutputFixture(t)
	physical, err := s.ReadClosedScopeFence(context.Background(), scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	before := fenceAdmissionLedger(t, s)
	s.Authority = nil
	if err = os.Remove(scope.Worktree); err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(s.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	closed, err := s.ReadClosedControlOutputFence(context.Background(), scope, token)
	if err != nil || closed != physical {
		t.Fatalf("actual closed output proof: %+v %v", closed, err)
	}
	proof, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, closed)
	if err != nil || proof.Token != token || len(proof.Descendants) != 0 || len(proof.Scopes) != 1 {
		t.Fatalf("changed existing snapshot: %+v %v", proof, err)
	}
	if !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
		t.Fatal("readonly output proof changed ledger")
	}
	if _, _, found, err := s.PendingControlOutputFence(context.Background(), scope); !found || !errors.Is(err, ErrAuthority) {
		t.Fatalf("revoking reader accepted closed fence: found%v %v", found, err)
	}
}

func TestClosedControlOutputFenceRejectsWrongIdentityAndReopen(t *testing.T) {
	s, scope, token := closedOutputFixture(t)
	before := fenceAdmissionLedger(t, s)
	for _, change := range []struct {
		name string
		edit func(*Scope)
	}{
		{"work", func(s *Scope) { s.WorkID = "other-work" }},
		{"generation", func(s *Scope) { s.Generation = "other-generation" }},
		{"owner", func(s *Scope) { s.OwnerID = "other-owner" }},
		{"contract", func(s *Scope) { s.CodexContract = 1 }},
		{"lease", func(s *Scope) { s.SetID, s.Lease = "other-set", 1 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			wrong := scope
			change.edit(&wrong)
			if _, err := s.ReadClosedControlOutputFence(context.Background(), wrong, token); !errors.Is(err, ErrAuthority) {
				t.Fatalf("changed scope accepted: %v", err)
			}
		})
	}
	if _, err := s.ReadClosedControlOutputFence(context.Background(), scope, "different-token"); !errors.Is(err, ErrAuthority) {
		t.Fatalf("different token accepted: %v", err)
	}
	if !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
		t.Fatal("negative reads changed ledger")
	}
	authority := s.Authority.(*stepTestAuthority)
	authority.scope.Generation = "g2"
	if err := s.Reopen(context.Background(), scope.WorkID, "g2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadClosedControlOutputFence(context.Background(), scope, token); !errors.Is(err, ErrAuthority) {
		t.Fatalf("reopened old fence accepted: %v", err)
	}
	next := authority.scope.Authorization()
	admitted, err := s.AdmitControlOutputFence(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Token == token {
		t.Fatal("new generation did not replace original token")
	}
	if _, err = s.FinishControlOutputFenceLocked(context.Background(), next.WorkID, admitted.Token); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadClosedControlOutputFence(context.Background(), next, token); !errors.Is(err, ErrAuthority) {
		t.Fatalf("closed replacement accepted old token: %v", err)
	}
	if _, err = s.ReadClosedControlOutputFence(context.Background(), next, admitted.Token); err != nil {
		t.Fatal("actual replacement refused", err)
	}
}

func TestClosedControlOutputFenceRejectsConflictingClosedBlock(t *testing.T) {
	s, scope, token := closedOutputFixture(t)
	// Negative metadata fixture: isolate the block check while the actual
	// previously closed scope and token still match.
	if err := s.transaction(context.Background(), true, func(db *ledger) error {
		db.Blocks[scope.WorkID] = "revoking"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadClosedControlOutputFence(context.Background(), scope, token); !errors.Is(err, ErrAuthority) {
		t.Fatalf("nonclosed work block accepted: %v", err)
	}
}

func TestClosedControlOutputFencePreservesFalsePublicationSnapshots(t *testing.T) {
	s, deps, request, authority := setupStep(t)
	accepted := mustSubmit(t, s, request)
	if err := s.Fence(context.Background(), request.WorkID, "close before actual publication"); err != nil {
		t.Fatal(err)
	}
	scope := authority.scope.Authorization()
	first, err := s.ReadClosedScopeFence(context.Background(), scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	old, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, first)
	if err != nil || len(old.Descendants) != 1 || old.Descendants[0].JobID != accepted.JobID || old.Descendants[0].PublicationSettled {
		t.Fatalf("actual publication cut missing: %+v %v", old, err)
	}
	ref, err := s.ReadClosedControlOutputFence(context.Background(), scope, old.Token)
	if err != nil || ref != first {
		t.Fatalf("factual closed query invented publication: %+v %v", ref, err)
	}
	if err = flush(context.Background(), deps); err != nil {
		t.Fatal(err)
	}
	second, err := s.ReadClosedControlOutputFence(context.Background(), scope, old.Token)
	if err != nil || second == first {
		t.Fatalf("actual publication did not advance snapshot: %+v %v", second, err)
	}
	current, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, second)
	if err != nil || !current.Descendants[0].PublicationSettled {
		t.Fatalf("actual publication missing: %+v %v", current, err)
	}
	retained, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, first)
	if err != nil || !reflect.DeepEqual(old, retained) {
		t.Fatal("old factual snapshot changed", err)
	}
	again, err := s.ReadClosedControlOutputFence(context.Background(), scope, old.Token)
	if err != nil || again != second {
		t.Fatal("identical snapshot is not idempotent", err)
	}
}

type closedOutputAuthority struct {
	*stepTestAuthority
	caller CoordinatorOutputCaller
	path   string
}

func (a closedOutputAuthority) ValidateCoordinatorOutput(_ context.Context, caller CoordinatorOutputCaller, _ controlrecord.OriginalRequestRef, path string) error {
	if !reflect.DeepEqual(caller, a.caller) || path != a.path {
		return ErrAuthority
	}
	return nil
}
func (a closedOutputAuthority) ValidateRetainedCoordinatorOutput(ctx context.Context, caller CoordinatorOutputCaller, original controlrecord.OriginalRequestRef, path string) error {
	return a.ValidateCoordinatorOutput(ctx, caller, original, path)
}

func pendingClosedOutputFixture(t *testing.T) (Service, Scope, ControlOutputRef) {
	t.Helper()
	s, _, _, authority := setupStep(t)
	scope := authority.scope.Authorization()
	path := filepath.Join(scope.OutputRoot, "checkpoint.md")
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: "closed-output-session", AttemptID: "closed-output-attempt"}, ThreadID: "thread", TurnID: "turn", CallID: "file"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"file_write","payload":{"path":"checkpoint.md","text":"once"}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(s.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	birth, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil || strings.TrimSpace(string(birth)) == "" {
		t.Fatal("actual writer birth unavailable", err)
	}
	writer := controlrecord.LocalWriter{PID: os.Getpid(), Group: syscall.Getpgrp(), Birth: strings.TrimSpace(string(birth))}
	caller := CoordinatorOutputCaller{Writer: writer, Authority: controlrecord.OwnerAuthority{EpicID: scope.EpicID, OwnerID: scope.OwnerID, Kind: "set", Revision: "UNIT-closed-output", Activation: effects.EventRef{OwnerID: scope.OwnerID, Sequence: 1, SHA256: effects.Digest{1}}}, Control: effects.ControlReceiptRef{Control: id, Sequence: 1, Revision: 1, SHA256: effects.Digest{2}}, Spec: session.Spec{Task: scope.OwnerID, EpicID: scope.EpicID}, TargetID: scope.WorkID, OutputName: "checkpoint.md"}
	s.Authority = closedOutputAuthority{authority, caller, path}
	prepared, err := s.PrepareControlOutputLocked(context.Background(), ControlOutputRequest{Original: ref, Writer: writer, Caller: ControlOutputCaller{Coordinator: &caller}, Path: path, Text: "once"})
	if err != nil {
		t.Fatal("actual domain preparation", err)
	}
	return s, scope, prepared
}

func TestClosedControlOutputFenceActualPendingFileBlocksClosure(t *testing.T) {
	s, scope, prepared := pendingClosedOutputFixture(t)
	admitted, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	var pending *PendingControlOutputsError
	if _, err = s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, admitted.Token); !errors.As(err, &pending) || len(pending.Outputs) != 1 || pending.Outputs[0] != prepared {
		t.Fatalf("actual pending output did not block close: %+v %v", pending, err)
	}
	if _, err = s.ReadClosedControlOutputFence(context.Background(), scope, admitted.Token); !errors.Is(err, ErrAuthority) {
		t.Fatalf("revoking fence was called closed: %v", err)
	}
	// Negative metadata fixture: legal finalizers cannot close with a pending file.
	// Keep its real prepared history and change only the barrier state to isolate
	// the current reader's independent pending-output predicate.
	if err = s.transaction(context.Background(), true, func(db *ledger) error {
		db.Blocks[scope.WorkID] = "closed"
		db.Scopes[scopeKey(scope)].State = "closed"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadClosedControlOutputFence(context.Background(), scope, admitted.Token); !errors.As(err, &pending) || len(pending.Outputs) != 1 || pending.Outputs[0] != prepared {
		t.Fatalf("closed negative metadata hid pending output: %+v %v", pending, err)
	}
}

func TestClosedControlOutputFenceRejectsUnknownOutputHistory(t *testing.T) {
	s, scope, prepared := pendingClosedOutputFixture(t)
	admitted, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.transaction(context.Background(), true, func(db *ledger) error {
		db.Blocks[scope.WorkID] = "closed"
		db.Scopes[scopeKey(scope)].State = "closed"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	name, err := controlOutputPath(s.Brain, prepared.ID, "prepared.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(name, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadClosedControlOutputFence(context.Background(), scope, admitted.Token); !errors.Is(err, ErrConflict) {
		t.Fatalf("unknown output history accepted: %v", err)
	}
}
