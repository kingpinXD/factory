package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

const verificationObserveRequest = "verification-observe"

type VerificationObserveRequest struct {
	Version    int
	Authority  OwnerAuthority
	Work       helper.Scope
	Entry, End effects.EventRef
	Round      helper.RevisionRoundRef
	Verifier   effects.Artifact
}

func verificationObserveKey(next VerificationObserveRequest) string {
	return fmt.Sprintf("verification-observe:%s#%d:%d:%s", next.Work.WorkID, next.Entry.Sequence, next.End.Sequence, next.End.SHA256.String())
}

func verificationObserveSource(round helper.PublishedRoundEvidence, owner OwnerAuthority, entry, end effects.EventRef) VerificationObserveRequest {
	return VerificationObserveRequest{Version: 2, Authority: owner, Work: round.Scope, Entry: entry, End: end, Round: round.Round, Verifier: round.Round.Output}
}

func readVerificationObserveRequestUsing(ctx context.Context, brain, work string, event events.Event, verify nativeAcceptance) (VerificationObserveRequest, error) {
	var next VerificationObserveRequest
	var sha effects.Digest
	if event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != events.RecipientRepoWorker || event.Request != verificationObserveRequest || sha.UnmarshalText([]byte(event.SHA256)) != nil {
		return next, effects.ErrEvidence
	}
	if err := readVerificationArtifact(codex.ProgramArtifact{Path: event.File, SHA256: sha}, &next); err != nil {
		return next, err
	}
	if next.Version != 2 || next.Work != next.Work.Authorization() || next.Work.WorkID != work || next.Work.CodexContract != 2 || next.Entry.OwnerID != work || next.End.OwnerID != work || next.Entry.Sequence < 1 || next.End.Sequence <= next.Entry.Sequence || next.End.Sequence >= event.Seq || next.Round.Output != next.Verifier || next.Round.Step.EntryRef != fmt.Sprintf("%s#%d", work, next.Entry.Sequence) || next.Round.Step.Component != "verifier" || event.Key != verificationObserveKey(next) || event.File != filepath.Join(verificationDirectory(brain, work, next.Entry.Sequence, next.End.Sequence), "observe-"+sha.String()+".json") {
		return next, effects.ErrEvidence
	}
	if _, _, err := nativeAuthority(ctx, brain, next.Work.EpicID, next.Work.OwnerID, false, &next.Authority, verify); err != nil {
		return next, err
	}
	_, actual, err := nativeOwner(brain, next.Work.EpicID, work)
	if err != nil {
		return next, err
	}
	end, err := verifierEnd(actual, next.Round)
	if err != nil || nativeEventRef(work, end) != next.End {
		return next, effects.ErrEvidence
	}
	entry, err := verifierEntry(actual, next.Round, end)
	if err != nil || entry != next.Entry {
		return next, effects.ErrEvidence
	}
	verdict, err := readVerifierVerdict(next.Verifier)
	if err != nil || verdict != "pass" {
		return next, effects.ErrEvidence
	}
	return next, nil
}

func publishVerificationObserveLocked(ctx context.Context, brain string, work *entity, next VerificationObserveRequest, verify nativeAcceptance) (effects.EventRef, error) {
	var found effects.EventRef
	for _, event := range work.evs {
		if event.Key != verificationObserveKey(next) {
			continue
		}
		if found != (effects.EventRef{}) {
			return effects.EventRef{}, effects.ErrConflict
		}
		actual, err := readVerificationObserveRequestUsing(ctx, brain, work.id, event, verify)
		if err != nil || actual != next {
			return effects.EventRef{}, effects.ErrConflict
		}
		found = nativeEventRef(work.id, event)
	}
	if found != (effects.EventRef{}) {
		return found, nil
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, work.id); err != nil {
		return effects.EventRef{}, err
	}
	dir := verificationDirectory(brain, work.id, next.Entry.Sequence, next.End.Sequence)
	source, err := verificationArtifact(filepath.Join(dir, "observe-"+effectDigest(next).String()+".json"), next)
	if err != nil {
		return effects.EventRef{}, err
	}
	event, err := events.Append(store.EventsPath(work.entry.Dir), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: verificationObserveRequest, File: source.Path, SHA256: source.SHA256.String(), Key: verificationObserveKey(next)})
	if err != nil {
		return effects.EventRef{}, err
	}
	if err := syncOwnership(work); err != nil {
		return effects.EventRef{}, err
	}
	actual, err := readVerificationObserveRequestUsing(ctx, brain, work.id, event, verify)
	if err != nil || actual != next {
		return effects.EventRef{}, effects.ErrConflict
	}
	return nativeEventRef(work.id, event), nil
}

func protectedVerifierCandidateUsing(ctx context.Context, brain string, expected VerificationObserveRequest, verify nativeAcceptance) (codex.ProgramArtifact, error) {
	chains, err := observeRegistryCheckChains(ctx, brain, expected.Work.WorkID)
	if err != nil {
		return codex.ProgramArtifact{}, err
	}
	for {
		more, err := chains.advance(ctx)
		if err != nil {
			return codex.ProgramArtifact{}, err
		}
		if !more {
			break
		}
	}
	var out codex.ProgramArtifact
	for ref := range chains.chains {
		candidate, err := chains.snapshot.CandidateLocal(ref)
		if err != nil {
			return out, err
		}
		if candidate.Work != expected.Work || candidate.Authority != expected.Authority || candidate.Entry != expected.Entry || candidate.End != expected.End || candidate.Round != expected.Round || candidate.Verifier != expected.Verifier {
			continue
		}
		if out != (codex.ProgramArtifact{}) {
			return out, effects.ErrConflict
		}
		if _, err := readVerifierCandidateLocalUsing(ctx, brain, ref, verify); err != nil {
			return out, err
		}
		out = ref
	}
	if out == (codex.ProgramArtifact{}) {
		return out, os.ErrNotExist
	}
	return out, nil
}

func observeVerificationCandidatesUsing(ctx context.Context, d Deps, verify nativeAcceptance) ([]VerifierCandidateObservation, error) {
	r, err := newRun(ctx, d, true)
	if err != nil {
		return nil, err
	}
	var out []VerifierCandidateObservation
	for _, id := range r.order() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		work := r.ents[id]
		if work.work == nil || work.cur.State != "verifying" {
			continue
		}
		modern, err := r.modernVerificationWork(work)
		if err != nil || !modern {
			continue
		}
		round, _, owner, entry, end, err := currentVerifierSourceUsing(ctx, d.Brain, id, verify)
		if err != nil {
			continue
		}
		if verdict, err := readVerifierVerdict(round.Round.Output); err != nil || verdict != "pass" {
			continue
		}
		expected := verificationObserveSource(round, owner, entry, end)
		if _, err := protectedVerifierCandidateUsing(ctx, d.Brain, expected, verify); err == nil || !errors.Is(err, os.ErrNotExist) {
			continue
		}
		observation, err := observeVerifierCandidateUsing(ctx, d.Brain, round.Round.Output, verify)
		if err == nil {
			out = append(out, observation)
		}
	}
	return out, nil
}

func (r *run) prepareVerificationDecisionsLocked(observations []VerifierCandidateObservation) error {
	return r.prepareVerificationDecisionsLockedUsing(observations, r.verificationAcceptance())
}

func (r *run) prepareVerificationDecisionsLockedUsing(observations []VerifierCandidateObservation, verify nativeAcceptance) error {
	if r.dry {
		return nil
	}
	for _, id := range r.order() {
		work := r.ents[id]
		if work.work == nil || work.cur.State != "verifying" {
			continue
		}
		modern, err := r.modernVerificationWork(work)
		if err != nil || !modern {
			continue
		}
		if err := r.prepareVerificationWorkLockedUsing(work, observations, verify); err != nil {
			r.fail("%s: verification result pending: %v", work.id, err)
		}
		fresh, err := loadEntity(id, work.entry)
		if err != nil {
			return err
		}
		fresh.m = work.m
		r.ents[id] = fresh
	}
	return nil
}

func (r *run) prepareVerificationWorkLockedUsing(work *entity, observations []VerifierCandidateObservation, verify nativeAcceptance) error {
	round, current, owner, entry, end, err := currentVerifierSourceUsing(r.ctx, r.d.Brain, work.id, verify)
	if err != nil {
		return err
	}
	verdict, err := readVerifierVerdict(round.Round.Output)
	if err != nil || verdict != "pass" {
		return err
	}
	expected := verificationObserveSource(round, owner, entry, end)
	key := fmt.Sprintf("verification-decision:%s#%d", work.id, entry.Sequence)
	decisionFound := false
	for _, event := range current.evs {
		if event.Key != key {
			continue
		}
		if decisionFound {
			return effects.ErrConflict
		}
		decisionFound = true
		next, err := verificationDecisionEventUsing(r.ctx, r.d.Brain, work.id, event, verify)
		if err != nil {
			return err
		}
		if next.Work != expected.Work || next.Entry != entry || next.End != end || next.Round != round.Round {
			return effects.ErrStale
		}
	}
	if decisionFound {
		return nil
	}
	candidate, err := protectedVerifierCandidateUsing(r.ctx, r.d.Brain, expected, verify)
	if errors.Is(err, os.ErrNotExist) {
		var matched *VerifierCandidateObservation
		for i := range observations {
			observed := &observations[i]
			if observed.WorkID != work.id || observed.Entry != entry || observed.End != end || observed.Round != round.Round || observed.Authority != owner || observed.Verifier != round.Round.Output {
				continue
			}
			if matched != nil {
				return effects.ErrConflict
			}
			matched = observed
		}
		if matched == nil {
			_, err := publishVerificationObserveLocked(r.ctx, r.d.Brain, current, expected, verify)
			return err
		}
		if err := (helper.Service{Brain: r.d.Brain}).CheckWorkControlAdmission(r.ctx, work.id); err != nil {
			return err
		}
		candidate, err = commitVerifierCandidateLockedUsing(r.ctx, r.d.Brain, *matched, verify)
	}
	if err != nil {
		return err
	}
	service := NativeService(r.d.Brain, nil)
	if r.d.Helpers != nil {
		if r.d.Helpers.Brain != r.d.Brain {
			return effects.ErrAuthority
		}
		service = *r.d.Helpers
	}
	_, err = completeVerificationDecisionLockedUsing(r.ctx, r.d.Brain, candidate, service, verify)
	return err
}

func runVerificationObservationJobs(ctx context.Context, brain string, out io.Writer) error {
	return runVerificationObservationJobsUsing(ctx, brain, out, codex.Contract2AcceptanceAuthority)
}

func runVerificationObservationJobsUsing(ctx context.Context, brain string, out io.Writer, verify nativeAcceptance) error {
	r, err := newRun(ctx, Deps{Brain: brain, Now: time.Now}, true)
	if err != nil {
		return err
	}
	for _, id := range r.order() {
		work := r.ents[id]
		if work.entry.Kind != blueprint.MachineWork {
			continue
		}
		for _, request := range work.evs {
			if request.Request != verificationObserveRequest {
				continue
			}
			if err := runVerificationObservationJobUsing(ctx, brain, work, request, verify); err != nil {
				fmt.Fprintf(out, "%s: verification observation pending: %v\n", id, err)
			}
		}
	}
	return ctx.Err()
}

func runVerificationObservationJobUsing(ctx context.Context, brain string, work *entity, request events.Event, verify nativeAcceptance) error {
	expected, err := readVerificationObserveRequestUsing(ctx, brain, work.id, request, verify)
	if err != nil {
		return err
	}
	found := false
	for _, answer := range work.evs {
		if answer.Key != repoResultKey(request) {
			continue
		}
		if found {
			return effects.ErrConflict
		}
		found = true
		var sha effects.Digest
		if answer.Kind != events.KindDone || answer.Sender != events.SenderProgram || answer.TriggerRef != seqRef(request) || sha.UnmarshalText([]byte(answer.SHA256)) != nil {
			return effects.ErrEvidence
		}
		candidate, err := readVerifierCandidateLocalUsing(ctx, brain, codex.ProgramArtifact{Path: answer.File, SHA256: sha}, verify)
		if err != nil || candidate.Work != expected.Work || candidate.Authority != expected.Authority || candidate.Entry != expected.Entry || candidate.End != expected.End || candidate.Round != expected.Round || candidate.Verifier != expected.Verifier {
			return effects.ErrEvidence
		}
	}
	if found {
		return nil
	}
	candidate, err := protectedVerifierCandidateUsing(ctx, brain, expected, verify)
	var observed VerifierCandidateObservation
	if errors.Is(err, os.ErrNotExist) {
		observed, err = observeVerifierCandidateUsing(ctx, brain, expected.Verifier, verify)
	}
	if err != nil {
		return err
	}
	unlock, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return err
	}
	defer unlock()
	_, actual, err := nativeOwner(brain, expected.Work.EpicID, work.id)
	if err != nil || request.Seq > len(actual.evs) || !reflect.DeepEqual(actual.evs[request.Seq-1], request) {
		return effects.ErrStale
	}
	if candidate == (codex.ProgramArtifact{}) {
		if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, work.id); err != nil {
			return err
		}
		if observed.WorkID != work.id || observed.Entry != expected.Entry || observed.End != expected.End || observed.Round != expected.Round || observed.Verifier != expected.Verifier || observed.Authority != expected.Authority {
			return effects.ErrStale
		}
		candidate, err = commitVerifierCandidateLockedUsing(ctx, brain, observed, verify)
		if err != nil {
			return err
		}
		if _, err := ensureCandidateChecksLockedUsing(ctx, brain, candidate, verify); err != nil {
			return err
		}
	}
	want := events.Event{Kind: events.KindDone, Sender: events.SenderProgram, TriggerRef: seqRef(request), File: candidate.Path, SHA256: candidate.SHA256.String(), Key: repoResultKey(request)}
	answer, err := events.Append(store.EventsPath(actual.entry.Dir), want)
	if err != nil {
		return err
	}
	comparison := answer
	comparison.Seq, comparison.At = 0, want.At
	if !reflect.DeepEqual(comparison, want) {
		return effects.ErrConflict
	}
	return syncOwnership(actual)
}
