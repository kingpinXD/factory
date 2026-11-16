package codex

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/zednative"
)

type VerifierCandidate struct {
	Version      int
	Authority    controlrecord.OwnerAuthority
	Work         helper.Scope
	Entry, End   effects.EventRef
	Verifier     effects.Artifact
	Round        helper.RevisionRoundRef
	Revision     ProgramArtifact
	Commit, Tree string
}

type RegistryChecksSelection struct {
	Version           int
	Candidate, Policy ProgramArtifact
	Retry             *ProgramArtifact         `json:"Retry,omitempty"`
	ReadRecipe        *RegistryCheckReadRecipe `json:"ReadRecipe,omitempty"`
}

type RegistryCheckEvidence struct {
	Version     int
	Owner       zednative.RegistryCheckOwner
	Revision    ProgramArtifact
	Containment ProgramArtifact
}

func ReadRegistryCheckResultLocal(ctx context.Context, brain string, ref ProgramReadResultRef) (zednative.RegistryCheckObservation, error) {
	result, err := readProgramReadEvidence(ctx, brain, ref, true, true)
	if err != nil {
		return zednative.RegistryCheckObservation{}, err
	}
	var evidence RegistryCheckEvidence
	if result.Check == nil || readRegistryCheckArtifact(*result.Check, &evidence) != nil {
		return zednative.RegistryCheckObservation{}, ErrObservationUnknown
	}
	return zednative.ReadRegistryCheckLocal(ctx, zednative.RevisionArtifact{Path: evidence.Containment.Path, SHA256: evidence.Containment.SHA256.String()})
}

func readRegistryCheckArtifact(ref ProgramArtifact, value any) error {
	data, err := readProgramArtifact(ref)
	if err != nil {
		return err
	}
	if err := strictDecodeRevision(data, value); err != nil {
		return err
	}
	canonical, err := json.Marshal(value)
	if err != nil || string(canonical) != string(data) {
		return ErrObservationUnknown
	}
	return nil
}

func registryCheckEvent(brain string, ref effects.EventRef) (events.Event, error) {
	index, err := store.ReadIndex(brain)
	if err != nil {
		return events.Event{}, err
	}
	entry, err := index.Lookup(ref.OwnerID)
	if err != nil {
		return events.Event{}, err
	}
	log, err := events.Read(store.EventsPath(entry.Dir))
	if err != nil {
		return events.Event{}, err
	}
	if ref.Sequence < 1 || ref.Sequence > len(log) || log[ref.Sequence-1].Seq != ref.Sequence || ProgramProcessDigest(log[ref.Sequence-1]) != ref.SHA256 {
		return events.Event{}, ErrObservationUnknown
	}
	return log[ref.Sequence-1], nil
}

func readRegistryCheckDomainEvidence(ctx context.Context, brain string, request ProgramReadRequest, process ProgramProcessRecord, result ProgramReadResult, snapshot *RegistryCheckSourceSnapshot) (zednative.RegistryCheckObservation, error) {
	var none zednative.RegistryCheckObservation
	if request.Kind != "program_read" || request.Operation != "registry_checks" || process.Mode != "check" || !process.Deadline.Equal(request.Deadline) || result.Check == nil || result.Base != nil || len(result.ReadJSON) != 0 || result.Failure != "" {
		return none, ErrObservationUnknown
	}
	facts, err := snapshot.source(ctx, brain, request)
	if err != nil {
		return none, err
	}
	source, selected, candidate, policy, receipt := facts.source, facts.selection, facts.candidate, facts.policy, facts.receipt
	if process.OwnerID != source.Authority.OwnerID || process.LogicalRevision != source.Authority.Revision {
		return none, ErrObservationUnknown
	}
	dir, err := RegistryCheckRunDirectory(brain, result.Request, process)
	if err != nil || result.Check.Path != filepath.Join(dir, "check-result.json") {
		return none, ErrObservationUnknown
	}
	var evidence RegistryCheckEvidence
	if err := snapshot.artifact(*result.Check, &evidence); err != nil {
		return none, err
	}
	owner := evidence.Owner
	old := receipt.Settlement.Observation
	if evidence.Version != 2 || evidence.Revision != candidate.Revision || owner.RequestID != request.RequestID || owner.ClaimID != process.ClaimID || owner.EnrollmentSHA256 != process.EnrollmentSHA256.String() || owner.RunDir != dir || !owner.Deadline.Equal(process.Deadline) || owner.Candidate != nativeRevisionRef(selected.Candidate) || owner.Input != nativeRevisionRef(receipt.Observation.Input) || owner.Registry != nativeRevisionRef(selected.Policy) || owner.OldModelSettlement != (zednative.RevisionArtifact{Path: old.ContainmentRef, SHA256: old.ContainmentSHA256}) || owner.Commit != candidate.Commit || owner.Tree != candidate.Tree || !reflect.DeepEqual(owner.Commands, policy.Commands) || owner.NetworkPolicy != policy.NetworkPolicy || evidence.Containment.Path != filepath.Join(dir, "containment.json") {
		return none, ErrObservationUnknown
	}
	if owner.Image != facts.image {
		return none, ErrObservationUnknown
	}
	observation, err := zednative.ReadRegistryCheckLocal(ctx, nativeRevisionRef(evidence.Containment))
	if err != nil {
		return none, err
	}
	if observation.OwnerSHA256 != ProgramProcessDigest(owner).String() || observation.Commit != candidate.Commit || observation.Tree != candidate.Tree {
		return none, ErrObservationUnknown
	}
	return observation, nil
}

func PublishRegistryCheckEvidence(ctx context.Context, brain string, request ProgramReadRef, process ProgramProcessRecord, owner zednative.RegistryCheckOwner, revision ProgramArtifact, observation zednative.RegistryCheckObservation) (ProgramArtifact, error) {
	dir, err := RegistryCheckRunDirectory(brain, request, process)
	if err != nil || owner.RunDir != dir || owner.RequestID != request.RequestID || owner.ClaimID != process.ClaimID || owner.EnrollmentSHA256 != process.EnrollmentSHA256.String() || !owner.Deadline.Equal(process.Deadline) || observation.OwnerSHA256 != ProgramProcessDigest(owner).String() {
		return ProgramArtifact{}, ErrObservationUnknown
	}
	actual, err := zednative.ReadRegistryCheckLocal(ctx, observation.Containment)
	if err != nil || !reflect.DeepEqual(actual, observation) {
		return ProgramArtifact{}, ErrObservationUnknown
	}
	var sha effects.Digest
	if sha.UnmarshalText([]byte(observation.Containment.SHA256)) != nil {
		return ProgramArtifact{}, ErrObservationUnknown
	}
	return saveHelperRevisionArtifact(filepath.Join(dir, "check-result.json"), RegistryCheckEvidence{Version: 2, Owner: owner, Revision: revision, Containment: ProgramArtifact{Path: observation.Containment.Path, SHA256: sha}})
}

// RegistryCheckRunDirectory derives a domain path from immutable enrollment.
// It is not admission or a physical claim; the actual E6 source/claim guard is
// mandatory before preparing or executing anything at this path.
func RegistryCheckRunDirectory(brain string, request ProgramReadRef, process ProgramProcessRecord) (string, error) {
	id, err := ProgramProcessSubjectID(process.Subject)
	if err != nil || process.Schema != 1 || process.Subject.Kind != "program_read" || process.Subject.ProgramRead == nil || *process.Subject.ProgramRead != request || process.SubjectID != id || process.Mode != "check" || process.InputSHA256 != request.SHA256 || process.EnrollmentSHA256 != ProgramProcessEnrollmentDigest(process) || process.SelectionSHA256 != ProgramProcessDigest(ProgramProcessSelectionFor(process)) || process.AdmittedAt.IsZero() || !process.Deadline.After(process.AdmittedAt) || process.Deadline.Sub(process.AdmittedAt) > 2*time.Minute {
		return "", ErrObservationUnknown
	}
	if _, err := ProgramProcessPath(brain, id, process.ClaimID); err != nil {
		return "", err
	}
	dir, err := ProgramReadDirectory(brain, request.RequestID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "checks", process.ClaimID, process.EnrollmentSHA256.String()), nil
}
