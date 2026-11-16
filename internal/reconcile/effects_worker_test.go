package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
)

func TestEffectConcreteAuthorityRejectsUnadmittedPrepare(t *testing.T) {
	brain := e3Brain(t)
	bin, _ := os.Executable()
	p, err := NewProgramEffects(brain, bin)
	if err != nil {
		t.Fatal(err)
	}
	slot := effects.Slot{EpicID: "e1", OwnerID: "e1", WorkRevision: "rev", StepRef: "e1#1", Operation: effects.IssueComment, Target: effects.Target{Repo: "o/r", Number: 1}}
	_, err = p.Service().Prepare(context.Background(), effects.Origin{Kind: effects.OriginProgram, OwnerEvent: &effects.EventRef{OwnerID: "e1", Sequence: 1, SHA256: effects.Digest{1}}}, slot, effects.Proposal{Version: 2, Kind: effects.IssueComment, Payload: []byte(`{"node_id":"I_1","actor":"owner","body":"body"}`)}, effects.Artifact{Path: "/unadmitted", SHA256: effects.Digest{1}, ScopeSHA256: effects.Digest{1}})
	if !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("direct Prepare bypassed typed actual admission: %v", err)
	}
	var zero ClaimedEffectDispatcher
	if _, err := zero.Bootstrap(context.Background()); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("zero handle bootstrap: %v", err)
	}
	if err := zero.Run(context.Background()); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("zero handle run: %v", err)
	}
}
func TestEffectTransferIncludesPhysicalClaimsWithoutPendingActions(t *testing.T) {
	brain := e3Brain(t)
	r, _ := e3Prepare(t, brain, strings.Repeat("1", 64))
	bin, _ := os.Executable()
	p, err := NewProgramEffects(brain, bin)
	if err != nil {
		t.Fatal(err)
	}
	// Transfer cannot rely only on E1.Unsettled; the physical inventory is independent.
	blockers, err := p.TransferBlockersLocked(context.Background(), "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	if len(blockers.Actions) != 0 || len(blockers.Dispatchers) != 1 || blockers.Dispatchers[0].ClaimID != r.ClaimID {
		t.Fatalf("missing physical barrier: %+v", blockers)
	}
	sealed, err := sealProgramProcessLocked(context.Background(), brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closeProgramProcessLocked(context.Background(), brain, sealed, ""); err != nil {
		t.Fatal(err)
	}
	blockers, err = p.TransferBlockersLocked(context.Background(), "fixture-owner")
	if err != nil || len(blockers.Dispatchers) != 0 {
		t.Fatalf("actual closed proof did not release: %+v %v", blockers, err)
	}
}
func TestEffectPhysicalSpawnStripsCredentialEnvironment(t *testing.T) {
	brain := e3Brain(t)
	bin := filepath.Join(brain, "actor")
	script := "#!/bin/sh\nif [ -n \"$GH_TOKEN$GITHUB_TOKEN$SSH_AUTH_SOCK\" ] || [ \"$GH_CONFIG_DIR\" != /dev/null ]; then echo bad > \"$FACTORY_BRAIN/env-result\"; else echo clean > \"$FACTORY_BRAIN/env-result\"; fi\n"
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_TOKEN", "synthetic-private-token")
	t.Setenv("GITHUB_TOKEN", "synthetic-private-token")
	t.Setenv("SSH_AUTH_SOCK", "synthetic-socket")
	r, nonce, err := prepareProgramProcessLocked(context.Background(), brain, bin, codex.ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("1", 64)}, "fixture-owner", "revision-1", effects.Digest{1}, "execute", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := waitProgramProcess(context.Background(), brain, bin, r, nonce)
	if err != nil {
		t.Fatal(err)
	}
	result, err := os.ReadFile(filepath.Join(brain, "env-result"))
	if err != nil || string(result) != "clean\n" {
		t.Fatalf("programme spawn inherited credential capability: %q %v", result, err)
	}
	if actual.Stage != "prepared" {
		t.Fatal("environment fixture claimed a production authority")
	}
	sealed, err := sealProgramProcessLocked(context.Background(), brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closeProgramProcessLocked(context.Background(), brain, sealed, ""); err != nil {
		t.Fatal(err)
	}
}

func TestEffectQueueObserverBindsExactFrozenRecord(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "running")
	bin, _ := os.Executable()
	p, err := NewProgramEffects(w.brain, bin)
	if err != nil {
		t.Fatal(err)
	}
	slot := effects.Slot{EpicID: "e1", OwnerID: "e1", WorkRevision: "revision-1", StepRef: "e1#1", Operation: effects.IssueComment, Target: effects.Target{Repo: "o/r", Number: 1}}
	id, _ := slot.ID()
	admission := EffectAdmission{Version: 2, Authority: OwnerAuthority{EpicID: "e1", OwnerID: "e1", Kind: "epic", Revision: "revision-1"}, Slot: slot}
	source := effects.Artifact{Path: effectAdmissionPath(p.Brain, id), SHA256: effectDigest(admission), ScopeSHA256: effectDigest(admission.Authority)}
	if err := effectWriteImmutable(source.Path, admission); err != nil {
		t.Fatal(err)
	}
	r := effects.Receipt{Prepared: effects.Prepared{Slot: slot, SlotID: id, CapabilityVersion: 2, Source: source, SourceSHA256: source.SHA256.String()}}
	// This exercises only the local queue observer; it does not admit this unqualified source.
	req := w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: "effect", Text: id, Key: "effect:" + id})
	ref, err := p.effectQueue(r)
	if err != nil || ref.Sequence != req.Seq || ref.SHA256 != effectDigest(req) {
		t.Fatalf("queue reference mismatch: %+v %v", ref, err)
	}
	r.Prepared.Source.SHA256 = effects.Digest{9}
	if _, err := p.effectQueue(r); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("changed frozen source accepted: %v", err)
	}
}
func TestEffectTerminalWakeIsOnceAndNotPhaseCompletion(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "running")
	bin, _ := os.Executable()
	p, err := NewProgramEffects(w.brain, bin)
	if err != nil {
		t.Fatal(err)
	}
	r := effects.Receipt{State: effects.AppliedState, Prepared: effects.Prepared{SlotID: strings.Repeat("1", 64), Slot: effects.Slot{OwnerID: "e1"}}}
	before := len(w.log("e1"))
	for range 2 {
		if err := p.effectWakeLocked(r); err != nil {
			t.Fatal(err)
		}
	}
	evs := w.log("e1")
	if len(evs) != before+1 {
		t.Fatalf("duplicate terminal wake: %d", len(evs)-before)
	}
	wake := evs[len(evs)-1]
	if wake.Kind != events.KindMessage || wake.Sender != events.SenderProgram || wake.File != "" || wake.SHA256 != "" {
		t.Fatalf("terminal wake claimed phase completion: %+v", wake)
	}
	r.State = effects.UnresolvedState
	if err := p.effectWakeLocked(r); err != nil {
		t.Fatal(err)
	}
	if len(w.log("e1")) != len(evs) {
		t.Fatal("uncertain observation emitted a mover event")
	}
}

func TestEffectInventoryIgnoresUnpublishedAtomicTemporary(t *testing.T) {
	brain := e3Brain(t)
	r, _ := e3Prepare(t, brain, strings.Repeat("1", 64))
	rp, _ := codex.ProgramProcessPath(brain, r.SubjectID, r.ClaimID)
	temp := filepath.Join(filepath.Dir(rp), ".effect-tmp-privatefixture")
	if err := os.WriteFile(temp, []byte("unfinished"), 0600); err != nil {
		t.Fatal(err)
	}
	records, err := effectProcessInventory(brain)
	if err != nil || len(records) != 1 || records[0].ClaimID != r.ClaimID {
		t.Fatalf("unpublished temporary stalled inventory: %v %v", records, err)
	}
	if err := os.Chmod(temp, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := effectProcessInventory(brain); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("unsafe temporary ignored: %v", err)
	}
}
