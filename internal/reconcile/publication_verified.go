package reconcile

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

type VerifiedPublication struct {
	Commit, Tree, TrustedBare string
	Artifact                  codex.ProgramArtifact
}

func publicationVerifiedKey(decision events.Event) string {
	return "publication:verified:" + seqRef(decision)
}

func publicationVerifiedArtifact(brain, work string, next helper.VerificationDecision) (codex.ProgramArtifact, VerifiedRevision, error) {
	var verified VerifiedRevision
	if next.Outcome != "passed" || next.Verified == nil || next.Work.WorkID != work {
		return codex.ProgramArtifact{}, verified, effects.ErrEvidence
	}
	ref, err := programmeArtifactRef(*next.Verified)
	if err != nil {
		return ref, verified, err
	}
	if err := readVerificationArtifact(ref, &verified); err != nil {
		return ref, verified, err
	}
	if verified.Version != 2 || verified.Work != next.Work || verified.Entry != next.Entry || ref.Path != filepath.Join(verificationDirectory(brain, work, next.Entry.Sequence, next.End.Sequence), "verified-"+ref.SHA256.String()+".json") {
		return ref, verified, effects.ErrEvidence
	}
	for _, sha := range []string{verified.Commit, verified.Tree} {
		if _, err := hex.DecodeString(sha); err != nil || len(sha) != 40 && len(sha) != 64 || strings.ToLower(sha) != sha {
			return ref, verified, effects.ErrEvidence
		}
	}
	return ref, verified, nil
}

func publicationVerifiedReceipt(decision events.Event, ref codex.ProgramArtifact, commit string) events.Event {
	return events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Recipient: "publication", Request: "verified-commit", Key: publicationVerifiedKey(decision), TriggerRef: seqRef(decision), File: ref.Path, SHA256: ref.SHA256.String(), Text: commit}
}

func matchesPublicationVerifiedReceipt(event, expected events.Event) bool {
	return event.Kind == expected.Kind && event.Sender == expected.Sender && event.Recipient == expected.Recipient && event.Request == expected.Request && event.Key == expected.Key && event.TriggerRef == expected.TriggerRef && event.File == expected.File && event.SHA256 == expected.SHA256 && event.Text == expected.Text
}

func (r *run) recordVerifiedPublication(e *entity, decision events.Event, next helper.VerificationDecision) error {
	ref, verified, err := publicationVerifiedArtifact(r.d.Brain, e.id, next)
	if err != nil {
		return err
	}
	expected := publicationVerifiedReceipt(decision, ref, verified.Commit)
	if recorded, found := e.byKey(expected.Key); found {
		if !matchesPublicationVerifiedReceipt(recorded, expected) {
			return effects.ErrConflict
		}
		return nil
	}
	_, err = r.append(e, expected)
	return err
}

func publicationVerificationDecision(e *entity) (events.Event, events.Event, error) {
	for i := len(e.evs) - 1; i >= 0; i-- {
		transition := e.evs[i]
		if transition.Kind != events.KindTransition || transition.From != "verifying" || transition.To != workPROpen {
			continue
		}
		seq, err := strconv.Atoi(transition.TriggerRef)
		if transition.Sender != events.SenderProgram || err != nil || seq < 1 || seq >= transition.Seq || seq > len(e.evs) {
			return events.Event{}, transition, effects.ErrEvidence
		}
		return e.evs[seq-1], transition, nil
	}
	return events.Event{}, events.Event{}, fmt.Errorf("item %s has no consumed verified commit", e.id)
}

func publicationVerificationSourceUsing(ctx context.Context, brain string, e *entity, verify nativeAcceptance) (events.Event, helper.VerificationDecision, error) {
	var none helper.VerificationDecision
	decision, _, err := publicationVerificationDecision(e)
	if err != nil {
		return decision, none, err
	}
	replay, err := observeNativeVerificationReplay(ctx, brain, e, verify)
	if err != nil {
		return decision, none, err
	}
	if cause, ok := replay.causes[decision.Seq]; !ok || cause.outcome != "passed" {
		return decision, none, effects.ErrEvidence
	}
	next, err := verificationDecisionEventBody(brain, e.id, decision)
	return decision, next, err
}

// Existing pr_open items may have consumed verification before commit receipts
// were recorded. Their original proof can mint the same receipt during replay.
func (r *run) ensurePublicationVerified(e *entity) error {
	decision, next, err := publicationVerificationSourceUsing(r.ctx, r.d.Brain, e, r.verificationAcceptance())
	if err != nil {
		return err
	}
	return r.recordVerifiedPublication(e, decision, next)
}

func ReadPublicationVerified(ctx context.Context, brain, workID string) (VerifiedPublication, error) {
	return readPublicationVerifiedUsing(ctx, brain, workID, codex.Contract2AcceptanceAuthority)
}

func readPublicationVerifiedUsing(ctx context.Context, brain, workID string, verify nativeAcceptance) (VerifiedPublication, error) {
	var out VerifiedPublication
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return out, err
	}
	entry, err := ix.Lookup(workID)
	if err != nil {
		return out, err
	}
	if entry.Kind != blueprint.MachineWork {
		return out, effects.ErrEvidence
	}
	e, err := loadEntity(workID, entry)
	if err != nil {
		return out, err
	}
	decision, next, err := publicationVerificationSourceUsing(ctx, brain, e, verify)
	if err != nil {
		return out, err
	}
	ref, verified, err := publicationVerifiedArtifact(brain, workID, next)
	if err != nil {
		return out, err
	}
	expected := publicationVerifiedReceipt(decision, ref, verified.Commit)
	receipt, found := e.byKey(expected.Key)
	if !found || receipt.Seq <= decision.Seq || !matchesPublicationVerifiedReceipt(receipt, expected) {
		return out, effects.ErrEvidence
	}
	input, final, err := codex.ReadHelperRepositoryRevisionLocal(ctx, brain, verified.Revision)
	if err != nil {
		return out, err
	}
	if input.SnapshotGit == "" || input.Commit != verified.Commit || input.Tree != verified.Tree || final.Commit != verified.Commit || final.Tree != verified.Tree {
		return out, effects.ErrEvidence
	}
	return VerifiedPublication{Commit: verified.Commit, Tree: verified.Tree, TrustedBare: input.SnapshotGit, Artifact: ref}, nil
}
