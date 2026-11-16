package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
)

type ProgramReadInputAbsenceRequest struct {
	PreSource  controlrecord.ImmutableRef
	Enrollment *controlrecord.ImmutableRef
	Closure    ReadIngressClosure
}

func CloseProgramReadInputAbsentLocked(ctx context.Context, brain string, req ProgramReadInputAbsenceRequest) (codex.ProgramReadInputAbsentRef, error) {
	return closeProgramReadInputAbsentLocked(ctx, brain, req, codex.Contract2AcceptanceAuthority)
}

// The caller owns tick. Projection and closure facts are retained evidence;
// the RequestID guard excludes input publication through the final receipt.
func closeProgramReadInputAbsentLocked(ctx context.Context, brain string, req ProgramReadInputAbsenceRequest, verify nativeAcceptance) (codex.ProgramReadInputAbsentRef, error) {
	var none codex.ProgramReadInputAbsentRef
	admission, err := readProgrammeReadPreAdmission(ctx, brain, req.PreSource, req.Enrollment, verify)
	if err != nil {
		return none, err
	}
	source, err := json.Marshal(admission)
	if err != nil {
		return none, err
	}
	id := programReadRequestID(admission)
	guard, err := codex.AcquireProgramReadInputGuard(ctx, brain, id)
	if err != nil {
		return none, err
	}
	defer guard.Close()
	dir, err := codex.ProgramReadDirectory(brain, id)
	if err != nil {
		return none, err
	}
	if err := validateProgramReadPhysicalParents(brain, dir); err != nil {
		return none, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "request.json")); err == nil {
		return none, effects.ErrBusy
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	if err := programReadInputQueueAbsence(ctx, brain, admission, id, filepath.Join(dir, "request.json")); err != nil {
		return none, err
	}
	if err := programReadInputPhysicalAbsence(ctx, brain, id); err != nil {
		return none, err
	}
	original := req.Closure.Original
	if admission.Control != nil {
		if admission.Helper != nil || admission.Control.Original != original {
			return none, effects.ErrEvidence
		}
	} else if admission.Helper == nil || admission.Helper.Original != original {
		return none, effects.ErrEvidence
	}
	record := codex.ProgramReadInputAbsent{Schema: 2, Original: original, PreSource: req.PreSource, Enrollment: req.Enrollment, Admission: source, AdmissionSHA256: sha256.Sum256(source), RequestID: id, Closure: req.Closure, ObservedAt: time.Now().UTC()}
	if existing, err := codex.ProgramReadInputAbsentForRequestID(ctx, brain, id); err == nil {
		saved, err := codex.ReadProgramReadInputAbsentLocal(ctx, brain, existing)
		if err != nil {
			return none, err
		}
		record.ObservedAt = saved.ObservedAt
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	return codex.PublishProgramReadInputAbsentLocked(ctx, brain, guard, record)
}

func programReadInputPhysicalAbsence(ctx context.Context, brain, requestID string) error {
	root := filepath.Join(brain, "factory", "effects", "dispatch")
	if err := validateProgramReadPhysicalParents(brain, root); err != nil {
		return err
	}
	records, err := effectProcessInventory(brain)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Subject.Kind == "program_read" && record.Subject.ProgramRead != nil && record.Subject.ProgramRead.RequestID == requestID {
			return effects.ErrEvidence
		}
	}
	subjects, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// The inventory already checked every raw canonical record. Selectors are
	// independent evidence and must also resolve through their actual refs.
	for _, subject := range subjects {
		if err := ctx.Err(); err != nil {
			return err
		}
		selected := effectSelectedPath(brain, subject.Name())
		if _, err := os.Lstat(selected); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		record, err := effectSelectedProcess(brain, subject.Name())
		if err != nil {
			return err
		}
		if record.Subject.Kind == "program_read" && record.Subject.ProgramRead != nil && record.Subject.ProgramRead.RequestID == requestID {
			return effects.ErrEvidence
		}
	}
	return nil
}

func programReadInputQueueAbsence(ctx context.Context, brain string, a ProgramReadAdmission, id, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	owner, err := ix.Lookup(a.Authority.OwnerID)
	if err != nil {
		return err
	}
	if owner.Epic != a.Authority.EpicID || owner.Kind != a.Authority.Kind {
		return effects.ErrEvidence
	}
	log, err := nativeCompleteLog(owner.Dir)
	if err != nil {
		return err
	}
	for _, event := range log {
		if event.Key == programReadQueueRequest+":"+id || event.File == name || event.Request == programReadQueueRequest && event.Text == id {
			return effects.ErrEvidence
		}
	}
	return nil
}

func readProgrammeReadPreAdmission(ctx context.Context, brain string, pre controlrecord.ImmutableRef, enrollment *controlrecord.ImmutableRef, verify nativeAcceptance) (ProgramReadAdmission, error) {
	if enrollment != nil {
		return readCoordinatorReadPreAdmissionLocal(ctx, brain, pre, *enrollment, verify)
	}
	return readHelperReadPreAdmissionLocal(ctx, brain, pre, verify)
}

func validateProgrammeReadInputAbsentLocal(ctx context.Context, brain string, ref codex.ProgramReadInputAbsentRef, verify nativeAcceptance) error {
	record, err := codex.ReadProgramReadInputAbsentLocal(ctx, brain, ref)
	if err != nil {
		return err
	}
	admission, err := readProgrammeReadPreAdmission(ctx, brain, record.PreSource, record.Enrollment, verify)
	if err != nil {
		return err
	}
	actual, err := json.Marshal(admission)
	if err != nil {
		return err
	}
	if programReadRequestID(admission) != record.RequestID || !bytes.Equal(actual, record.Admission) || sha256.Sum256(actual) != record.AdmissionSHA256 {
		return effects.ErrEvidence
	}
	if admission.Control != nil {
		if admission.Control.Original != record.Original {
			return effects.ErrEvidence
		}
	} else if admission.Helper == nil || admission.Helper.Original != record.Original {
		return effects.ErrEvidence
	}
	return nil
}
