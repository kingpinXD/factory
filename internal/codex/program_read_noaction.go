package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

const (
	ProgramReadNoActionClosed  = "read_not_selected_before_ingress_closed"
	ProgramReadNoActionExpired = "read_expired_before_selection"
)

type ProgramReadIngressClosure struct {
	Original    controlrecord.OriginalRequestRef `json:"original"`
	AttemptSeal *effects.AttemptSeal             `json:"attempt_seal,omitempty"`
	WorkSeal    *effects.WorkIngressSeal         `json:"work_seal,omitempty"`
}

type ProgramReadNoActionRef struct {
	Path   string         `json:"path"`
	SHA256 effects.Digest `json:"sha256"`
}

type ProgramReadNoAction struct {
	Schema      int                              `json:"schema"`
	Original    controlrecord.OriginalRequestRef `json:"original"`
	Request     ProgramReadRef                   `json:"request"`
	Closure     *ProgramReadIngressClosure       `json:"closure,omitempty"`
	Scope       *helper.Scope                    `json:"scope,omitempty"`
	SubjectID   string                           `json:"subject_id"`
	Enrollments []ProgramProcessRef              `json:"enrollments"`
	Code        string                           `json:"code"`
	ObservedAt  time.Time                        `json:"observed_at"`
}

func ProgramReadNoActionPath(brain string, ref ProgramReadRef) (string, error) {
	dir, err := ProgramReadDirectory(brain, ref.RequestID)
	if err != nil || ref.SHA256 == (effects.Digest{}) {
		return "", ErrObservationUnknown
	}
	return filepath.Join(dir, "no-action.json"), nil
}

// ProgramReadNoActionForRequest is a factual lookup, never an absence grant.
func ProgramReadNoActionForRequest(ctx context.Context, brain string, request ProgramReadRef) (ProgramReadNoActionRef, error) {
	name, err := ProgramReadNoActionPath(brain, request)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	data, err := readProgramArtifactFile(name)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	ref := ProgramReadNoActionRef{Path: name, SHA256: sha256.Sum256(data)}
	input, err := ReadProgramReadRequest(ctx, brain, request)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	if input.Operation == "registry_checks" {
		return RegistryCheckNoActionForRequest(ctx, brain, request)
	}
	actual, err := ReadProgramReadNoActionLocal(ctx, brain, ref)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	if actual.Request != request {
		return ProgramReadNoActionRef{}, ErrObservationUnknown
	}
	return ref, nil
}

// PublishProgramReadNoActionLocked follows the actual source/tick and physical
// subject guard. Only the reconcile producer observes selection exclusion.
func PublishProgramReadNoActionLocked(ctx context.Context, brain string, record ProgramReadNoAction) (ProgramReadNoActionRef, error) {
	if err := validateProgramReadNoAction(ctx, brain, record); err != nil {
		return ProgramReadNoActionRef{}, err
	}
	name, err := ProgramReadNoActionPath(brain, record.Request)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	if err := publishProgramReadArtifact(name, data); err != nil {
		return ProgramReadNoActionRef{}, err
	}
	return ProgramReadNoActionRef{Path: name, SHA256: sha256.Sum256(data)}, nil
}

// ReadProgramReadNoActionLocal checks bounded retained artifacts. It does not
// inspect selection, a live process, Git, or the current helper ledger.
func ReadProgramReadNoActionLocal(ctx context.Context, brain string, ref ProgramReadNoActionRef) (ProgramReadNoAction, error) {
	var record ProgramReadNoAction
	if err := ctx.Err(); err != nil {
		return record, err
	}
	data, err := readProgramArtifact(ProgramArtifact{Path: ref.Path, SHA256: ref.SHA256})
	if err != nil {
		return record, err
	}
	if err := toolcontract.DecodeBroker(data, &record); err != nil {
		return record, err
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, data) {
		return record, ErrObservationUnknown
	}
	name, err := ProgramReadNoActionPath(brain, record.Request)
	if err != nil || name != ref.Path {
		return record, ErrObservationUnknown
	}
	return record, validateProgramReadNoAction(ctx, brain, record)
}

// The complete admission policy remains with its reconcile/C2 consumer. This
// reader checks only the neutral identity witnesses in the bound source bytes.
func readProgramSourceWitness(request ProgramReadRequest) (controlrecord.OriginalRequestRef, controlrecord.OwnerAuthority, *helper.Scope, error) {
	return readProgramAdmissionWitness(request.Source)
}

func validateProgramReadNoAction(ctx context.Context, brain string, r ProgramReadNoAction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Schema != 2 || r.ObservedAt.IsZero() || r.Enrollments == nil {
		return ErrObservationUnknown
	}
	request, err := ReadProgramReadRequest(ctx, brain, r.Request)
	if err != nil {
		return err
	}
	if request.Kind != "program_read" {
		return ErrObservationUnknown
	}
	subject, err := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "program_read", ProgramRead: &r.Request})
	if err != nil || subject != r.SubjectID {
		return ErrObservationUnknown
	}
	witness, authority, scope, err := readProgramSourceWitness(request)
	if err != nil || witness != r.Original {
		return ErrObservationUnknown
	}
	original, err := controlrecord.ReadOriginal(brain, r.Original)
	if err != nil {
		return err
	}
	if original.Control != nil {
		if r.Scope != nil || scope != nil {
			return ErrObservationUnknown
		}
	} else if original.Helper != nil {
		if r.Scope == nil || scope == nil || *r.Scope != r.Scope.Authorization() || *r.Scope != scope.Authorization() {
			return ErrObservationUnknown
		}
	} else {
		return ErrObservationUnknown
	}
	switch r.Code {
	case ProgramReadNoActionExpired:
		if r.Closure != nil || r.ObservedAt.Before(request.Deadline) {
			return ErrObservationUnknown
		}
	case ProgramReadNoActionClosed:
		if r.Closure == nil {
			return ErrObservationUnknown
		}
		closureScope := scope
		if original.Control != nil && r.Closure.WorkSeal != nil {
			closureScope, err = readProgramCoordinatorWorkWitness(request.Source, r.Original, *original.Control)
			if err != nil {
				return err
			}
		}
		if err := validateProgramReadIngressClosure(ctx, brain, r.Original, authority, closureScope, *r.Closure); err != nil {
			return err
		}

	default:
		return ErrObservationUnknown
	}
	last := ""
	for _, ref := range r.Enrollments {
		if ref.SubjectID != r.SubjectID || ref.ClaimID <= last {
			return ErrObservationUnknown
		}
		process, err := readPreparedProgramReadEnrollmentLocal(ctx, brain, ref, r.Request)
		if err != nil {
			return err
		}
		if process.OwnerID != authority.OwnerID || process.LogicalRevision != authority.Revision {
			return ErrObservationUnknown
		}
		last = ref.ClaimID
	}
	return nil
}

func readPreparedProgramReadEnrollmentLocal(ctx context.Context, brain string, ref ProgramProcessRef, request ProgramReadRef) (ProgramProcessRecord, error) {
	var record ProgramProcessRecord
	if err := ctx.Err(); err != nil {
		return record, err
	}
	name, err := ProgramProcessPath(brain, ref.SubjectID, ref.ClaimID)
	if err != nil {
		return record, err
	}
	data, err := readProgramArtifact(ProgramArtifact{Path: name, SHA256: ref.SHA256})
	if err != nil {
		return record, err
	}
	if err := toolcontract.DecodeStrict(data, &record); err != nil {
		return record, err
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, data) || ValidateProgramProcessRecord(record) != nil || record.SubjectID != ref.SubjectID || record.ClaimID != ref.ClaimID || record.Revision != ref.Revision || record.Stage != "prepared" || record.Subject.Kind != "program_read" || record.Subject.ProgramRead == nil || *record.Subject.ProgramRead != request {
		return record, ErrObservationUnknown
	}
	return record, nil
}

type RegistryCheckNoAction struct {
	Schema            int
	Request           ProgramReadRef
	Selection         effects.EventRef
	Candidate, Policy ProgramArtifact
	Authority         controlrecord.OwnerAuthority
	Work              helper.Scope
	SubjectID         string
	Enrollments       []ProgramProcessRef
	Code              string
	ObservedAt        time.Time
}

func RegistryCheckNoActionForRequest(ctx context.Context, brain string, request ProgramReadRef) (ProgramReadNoActionRef, error) {
	name, err := ProgramReadNoActionPath(brain, request)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	data, err := readProgramArtifactFile(name)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	ref := ProgramReadNoActionRef{Path: name, SHA256: sha256.Sum256(data)}
	record, err := ReadRegistryCheckNoActionLocal(ctx, brain, ref)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	if record.Request != request {
		return ProgramReadNoActionRef{}, ErrObservationUnknown
	}
	return ref, nil
}
func ReadRegistryCheckNoActionLocal(ctx context.Context, brain string, ref ProgramReadNoActionRef) (RegistryCheckNoAction, error) {
	return readRegistryCheckNoActionLocal(ctx, brain, ref, nil)
}
func readRegistryCheckNoActionLocal(ctx context.Context, brain string, ref ProgramReadNoActionRef, snapshot *RegistryCheckSourceSnapshot) (RegistryCheckNoAction, error) {
	var record RegistryCheckNoAction
	if err := ctx.Err(); err != nil {
		return record, err
	}
	if err := snapshot.artifact(ProgramArtifact{Path: ref.Path, SHA256: ref.SHA256}, &record); err != nil {
		return record, err
	}
	name, err := ProgramReadNoActionPath(brain, record.Request)
	if err != nil || name != ref.Path {
		return record, ErrObservationUnknown
	}
	return record, validateRegistryCheckNoAction(ctx, brain, record, snapshot)
}
func validateRegistryCheckNoAction(ctx context.Context, brain string, record RegistryCheckNoAction, snapshot *RegistryCheckSourceSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.Schema != 2 || record.Enrollments == nil || record.ObservedAt.IsZero() || record.Code != ProgramReadNoActionExpired {
		return ErrObservationUnknown
	}
	request, err := snapshot.request(ctx, brain, record.Request)
	if err != nil {
		return err
	}
	facts, err := snapshot.source(ctx, brain, request)
	if err != nil {
		return err
	}
	subject, err := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "program_read", ProgramRead: &record.Request})
	if err != nil || subject != record.SubjectID || record.ObservedAt.Before(request.Deadline) || record.Selection != facts.source.Program.Event || record.Candidate != facts.selection.Candidate || record.Policy != facts.selection.Policy || record.Authority != facts.candidate.Authority || record.Work != facts.candidate.Work {
		return ErrObservationUnknown
	}
	last := ""
	for _, ref := range record.Enrollments {
		if ref.SubjectID != subject || ref.ClaimID <= last {
			return ErrObservationUnknown
		}
		process, err := ReadClosedProgramProcessLocal(ctx, brain, ref)
		if err != nil {
			return err
		}
		if process.Host != (ProcessIdentity{}) || process.ClosedProof.Kind != "never_claimed" || process.Mode != "check" || process.Subject.ProgramRead == nil || *process.Subject.ProgramRead != record.Request || process.InputSHA256 != record.Request.SHA256 || process.OwnerID != record.Authority.OwnerID || process.LogicalRevision != record.Authority.Revision || process.AdmittedAt.Before(request.AdmittedAt) || !process.Deadline.Equal(request.Deadline) {
			return ErrObservationUnknown
		}
		last = ref.ClaimID
	}
	return nil
}

// Only the actual RepoWorker exclusion producer may publish this factual arm.
func PublishRegistryCheckNoActionLocked(ctx context.Context, brain string, record RegistryCheckNoAction) (ProgramReadNoActionRef, error) {
	if err := validateRegistryCheckNoAction(ctx, brain, record, nil); err != nil {
		return ProgramReadNoActionRef{}, err
	}
	name, err := ProgramReadNoActionPath(brain, record.Request)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return ProgramReadNoActionRef{}, err
	}
	if err := publishProgramReadArtifact(name, data); err != nil {
		return ProgramReadNoActionRef{}, err
	}
	return ProgramReadNoActionRef{Path: name, SHA256: sha256.Sum256(data)}, nil
}
