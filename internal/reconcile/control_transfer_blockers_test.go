package reconcile

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
)

func TestControlTransferFiltersActualReadSourceByRetainedWorkScope(t *testing.T) {
	w, _, _, admission, request := noactionReadFixture(t)
	if admission.Work == nil {
		t.Fatal("actual fixture has no work source")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	read, err := codex.ReadProgramReadRequest(context.Background(), w.brain, request)
	if err != nil {
		t.Fatal(err)
	}
	physical, _, err := prepareProgramProcessLocked(context.Background(), w.brain, binary, codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &request}, admission.Authority.OwnerID, admission.Authority.Revision, request.SHA256, "read", read.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	scope := *admission.Work
	tick, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer tick()
	ledger, err := store.Lock(w.brain, "helper-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer ledger()
	blockers, err := p.transferWorkBlockersLocked(context.Background(), scope, b2Qualification)
	if err != nil || len(blockers.Actions) != 0 || len(blockers.Dispatchers) != 1 || blockers.Dispatchers[0].ClaimID != physical.ClaimID {
		t.Fatalf("actual scoped read missing: %+v %v", blockers, err)
	}
	// This query tests filtering only. It does not claim admission of a new work item.
	sibling := scope
	sibling.WorkID = "e1-w2"
	blockers, err = p.transferWorkBlockersLocked(context.Background(), sibling, b2Qualification)
	if err != nil || len(blockers.Dispatchers) != 0 {
		t.Fatalf("unrelated work query inherited dispatcher: %+v %v", blockers, err)
	}
	changed := scope
	changed.Generation = "changed-generation"
	if _, err := p.transferWorkBlockersLocked(context.Background(), changed, b2Qualification); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("foreign generation matched physical source: %v", err)
	}
	sealed, err := sealProgramProcessLocked(context.Background(), w.brain, physical.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closeProgramProcessLocked(context.Background(), w.brain, sealed, ""); err != nil {
		t.Fatal(err)
	}
	blockers, err = p.transferWorkBlockersLocked(context.Background(), scope, b2Qualification)
	if err != nil || len(blockers.Dispatchers) != 0 {
		t.Fatalf("actual local closed proof did not release: %+v %v", blockers, err)
	}
}
