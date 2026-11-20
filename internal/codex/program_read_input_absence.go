package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type programReadInputLease struct {
	mu               sync.Mutex
	brain, requestID string
	release          func() error
}

type ProgramReadInputGuard struct{ lease *programReadInputLease }

func AcquireProgramReadInputGuard(ctx context.Context, brain, requestID string) (*ProgramReadInputGuard, error) {
	if _, err := ProgramReadDirectory(brain, requestID); err != nil {
		return nil, err
	}
	if canonical, err := filepath.EvalSymlinks(brain); err != nil || canonical != brain {
		return nil, ErrObservationUnknown
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		release, err := store.Lock(brain, "codex-program-read-input-"+requestID)
		if err == nil {
			if err := ctx.Err(); err != nil {
				release()
				return nil, err
			}
			return &ProgramReadInputGuard{lease: &programReadInputLease{brain: brain, requestID: requestID, release: release}}, nil
		}
		var busy *store.LockedError
		if !errors.As(err, &busy) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (g *ProgramReadInputGuard) Close() error {
	if g == nil || g.lease == nil {
		return nil
	}
	g.lease.mu.Lock()
	defer g.lease.mu.Unlock()
	if g.lease.release == nil {
		return nil
	}
	release := g.lease.release
	g.lease.release = nil
	return release()
}

func programReadInputAbsentPath(brain, requestID string) (string, error) {
	dir, err := ProgramReadDirectory(brain, requestID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "input-absent.json"), nil
}

func (g *ProgramReadInputGuard) rejectAbsenceLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.lease.release == nil {
		return ErrObservationUnknown
	}
	name, err := programReadInputAbsentPath(g.lease.brain, g.lease.requestID)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(name); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	// A present exclusion cannot become permission because its proof is corrupt.
	return ErrObservationUnknown
}

func (g *ProgramReadInputGuard) ValidateExistingLocal(ctx context.Context, ref ProgramReadRef) error {
	if g == nil || g.lease == nil {
		return ErrObservationUnknown
	}
	g.lease.mu.Lock()
	defer g.lease.mu.Unlock()
	if ref.RequestID != g.lease.requestID {
		return ErrObservationUnknown
	}
	if err := g.rejectAbsenceLocked(ctx); err != nil {
		return err
	}
	request, err := ReadProgramReadRequest(ctx, g.lease.brain, ref)
	if err != nil {
		return err
	}
	if request.Kind != "program_read" {
		return ErrObservationUnknown
	}
	return nil
}

func (g *ProgramReadInputGuard) publishRequest(ctx context.Context, request ProgramReadRequest) (ProgramReadRef, error) {
	if g == nil || g.lease == nil {
		return ProgramReadRef{}, ErrObservationUnknown
	}
	g.lease.mu.Lock()
	defer g.lease.mu.Unlock()
	if request.Kind != "program_read" || request.RequestID != g.lease.requestID {
		return ProgramReadRef{}, ErrObservationUnknown
	}
	if err := g.rejectAbsenceLocked(ctx); err != nil {
		return ProgramReadRef{}, err
	}
	return publishProgramReadRequest(g.lease.brain, request)
}

func validateProgramReadIngressClosure(ctx context.Context, brain string, originalRef controlrecord.OriginalRequestRef, authority controlrecord.OwnerAuthority, scope *helper.Scope, closure ProgramReadIngressClosure) error {
	if closure.Original != originalRef || (closure.AttemptSeal == nil) == (closure.WorkSeal == nil) {
		return ErrObservationUnknown
	}
	original, err := controlrecord.ReadOriginal(brain, originalRef)
	if err != nil {
		return err
	}
	if original.Control != nil && closure.WorkSeal != nil {
		if scope == nil || scope.CodexContract != 2 || scope.OwnerID != authority.OwnerID || scope.EpicID != authority.EpicID {
			return ErrObservationUnknown
		}
		seal := closure.WorkSeal
		if seal.WorkID != scope.WorkID || seal.OwnerID != authority.OwnerID || seal.OwnerRevision != authority.Revision {
			return ErrObservationUnknown
		}
		fence, err := helper.ReadClosedScopeFenceLocal(ctx, brain, helper.ClosedScopeFenceRef{Path: seal.Fence.Path, SHA256: seal.Fence.SHA256})
		if err != nil {
			return err
		}
		if fence.WorkID != scope.WorkID {
			return ErrObservationUnknown
		}
		matched := false
		for _, stored := range fence.Scopes {
			if stored.Authorization() == scope.Authorization() {
				matched = true
			}
		}
		if !matched {
			return ErrObservationUnknown
		}
		return nil
	}
	if original.Control != nil {
		seal := closure.AttemptSeal
		if seal == nil || seal.Attempt != original.Control.Attempt {
			return ErrObservationUnknown
		}
		actual, err := New(brain, "").SealedAttempt(ctx, seal.Attempt)
		if err != nil {
			return err
		}
		if actual != *seal {
			return ErrObservationUnknown
		}
		return nil
	}
	if original.Helper == nil || scope == nil {
		return ErrObservationUnknown
	}
	seal := closure.WorkSeal
	if seal == nil || seal.WorkID != scope.WorkID || seal.OwnerID != authority.OwnerID || seal.OwnerRevision != authority.Revision {
		return ErrObservationUnknown
	}
	fence, err := helper.ReadClosedScopeFenceLocal(ctx, brain, helper.ClosedScopeFenceRef{Path: seal.Fence.Path, SHA256: seal.Fence.SHA256})
	if err != nil {
		return err
	}
	if fence.WorkID != scope.WorkID {
		return ErrObservationUnknown
	}
	found := false
	for _, child := range fence.Descendants {
		if child.JobID != original.Helper.JobID {
			continue
		}
		if child.Generation != original.Helper.Generation || child.Round != original.Helper.Round || child.Scope.Authorization() != scope.Authorization() || !child.Stopped {
			return ErrObservationUnknown
		}
		found = true
	}
	if !found {
		return ErrObservationUnknown
	}
	return nil
}

type ProgramReadInputAbsentRef struct {
	Path   string         `json:"path"`
	SHA256 effects.Digest `json:"sha256"`
}

type ProgramReadInputAbsent struct {
	Schema          int                              `json:"schema"`
	Original        controlrecord.OriginalRequestRef `json:"original"`
	PreSource       controlrecord.ImmutableRef       `json:"pre_source"`
	Enrollment      *controlrecord.ImmutableRef      `json:"enrollment,omitempty"`
	Admission       json.RawMessage                  `json:"admission"`
	AdmissionSHA256 effects.Digest                   `json:"admission_sha256"`
	RequestID       string                           `json:"request_id"`
	Closure         ProgramReadIngressClosure        `json:"closure"`
	ObservedAt      time.Time                        `json:"observed_at"`
}

func ProgramReadInputAbsentForRequestID(ctx context.Context, brain, requestID string) (ProgramReadInputAbsentRef, error) {
	name, err := programReadInputAbsentPath(brain, requestID)
	if err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	data, err := readProgramArtifactFile(name)
	if err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	ref := ProgramReadInputAbsentRef{Path: name, SHA256: sha256.Sum256(data)}
	actual, err := ReadProgramReadInputAbsentLocal(ctx, brain, ref)
	if err != nil {
		return ProgramReadInputAbsentRef{}, fmt.Errorf("%w: invalid input exclusion: %v", ErrObservationUnknown, err)
	}
	if actual.RequestID != requestID {
		return ProgramReadInputAbsentRef{}, ErrObservationUnknown
	}
	return ref, nil
}

func ReadProgramReadInputAbsentLocal(ctx context.Context, brain string, ref ProgramReadInputAbsentRef) (ProgramReadInputAbsent, error) {
	var record ProgramReadInputAbsent
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
	name, err := programReadInputAbsentPath(brain, record.RequestID)
	if err != nil || name != ref.Path {
		return record, ErrObservationUnknown
	}
	return record, validateProgramReadInputAbsent(ctx, brain, record)
}

// Only the concrete source/absence transaction may call this with its held
// RequestID guard. This storage operation creates no request or process.
func PublishProgramReadInputAbsentLocked(ctx context.Context, brain string, guard *ProgramReadInputGuard, record ProgramReadInputAbsent) (ProgramReadInputAbsentRef, error) {
	if guard == nil || guard.lease == nil {
		return ProgramReadInputAbsentRef{}, ErrObservationUnknown
	}
	guard.lease.mu.Lock()
	defer guard.lease.mu.Unlock()
	if guard.lease.release == nil || guard.lease.brain != brain || guard.lease.requestID != record.RequestID {
		return ProgramReadInputAbsentRef{}, ErrObservationUnknown
	}
	if err := validateProgramReadInputAbsent(ctx, brain, record); err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	if err := programReadInputArtifactAbsence(ctx, brain, record.RequestID); err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	name, err := programReadInputAbsentPath(brain, record.RequestID)
	if err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	if err := publishProgramReadArtifact(name, data); err != nil {
		return ProgramReadInputAbsentRef{}, err
	}
	return ProgramReadInputAbsentRef{Path: name, SHA256: sha256.Sum256(data)}, nil
}

type readInputPreHeader struct {
	Schema      int                              `json:"schema"`
	Original    controlrecord.OriginalRequestRef `json:"original"`
	Purpose     string                           `json:"purpose"`
	Coordinator json.RawMessage                  `json:"coordinator,omitempty"`
	Helper      json.RawMessage                  `json:"helper,omitempty"`
}

func validateProgramReadInputAbsent(ctx context.Context, brain string, r ProgramReadInputAbsent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Schema != 2 || !programHex(r.RequestID, 64) || r.ObservedAt.IsZero() || len(r.Admission) == 0 || sha256.Sum256(r.Admission) != r.AdmissionSHA256 {
		return ErrObservationUnknown
	}
	originalRef, authority, scope, err := readProgramAdmissionWitness(r.Admission)
	if err != nil || originalRef != r.Original {
		return ErrObservationUnknown
	}
	original, err := controlrecord.ReadOriginal(brain, r.Original)
	if err != nil {
		return err
	}
	pre, err := readProgramReadPreHeader(brain, r.PreSource)
	if err != nil {
		return err
	}
	if pre.Original != r.Original {
		return ErrObservationUnknown
	}
	var admission map[string]json.RawMessage
	if err := toolcontract.DecodeBroker(r.Admission, &admission); err != nil {
		return err
	}
	var purpose, operation string
	if toolcontract.DecodeBroker(admission["Purpose"], &purpose) != nil || toolcontract.DecodeBroker(admission["Operation"], &operation) != nil || purpose != pre.Purpose || operation != original.Operation {
		return ErrObservationUnknown
	}
	if original.Control != nil {
		if r.Enrollment == nil || len(pre.Coordinator) == 0 || len(pre.Helper) != 0 || scope != nil {
			return ErrObservationUnknown
		}
		enrolled, err := controlrecord.ReadEnrollmentSnapshot(brain, *r.Enrollment)
		if err != nil {
			return err
		}
		var caller struct {
			Receipt  effects.ControlReceiptRef
			Input    effects.ControlInput
			Original controlrecord.OriginalRequestRef
		}
		if err := toolcontract.DecodeBroker(admission["Control"], &caller); err != nil {
			return err
		}
		if enrolled.Original != r.Original || enrolled.Source != r.PreSource || enrolled.Receipt != caller.Receipt || enrolled.Input != caller.Input || caller.Original != r.Original {
			return ErrObservationUnknown
		}
		if r.Closure.WorkSeal != nil {
			scope, err = readProgramCoordinatorWorkWitness(r.Admission, r.Original, *original.Control)
			if err != nil {
				return err
			}
			if enrolled.Input.Target.WorkID != scope.WorkID {
				return ErrObservationUnknown
			}
		}

	} else if original.Helper != nil {
		if r.Enrollment != nil || len(pre.Helper) == 0 || len(pre.Coordinator) != 0 || scope == nil {
			return ErrObservationUnknown
		}
		var caller struct {
			JobID                    string
			Generation, Round        int
			ThreadID, TurnID, CallID string
			Input                    effects.ControlInput
			Original                 controlrecord.OriginalRequestRef
		}
		if err := toolcontract.DecodeBroker(admission["Helper"], &caller); err != nil {
			return err
		}
		call := original.Helper
		if caller.Original != r.Original || caller.JobID != call.JobID || caller.Generation != call.Generation || caller.Round != call.Round || caller.ThreadID != call.ThreadID || caller.TurnID != call.TurnID || caller.CallID != call.CallID || caller.Input.Operation != original.Operation || caller.Input.PayloadSHA256 != original.PayloadSHA256 {
			return ErrObservationUnknown
		}
	} else {
		return ErrObservationUnknown
	}
	return validateProgramReadIngressClosure(ctx, brain, r.Original, authority, scope, r.Closure)
}

func readProgramReadPreHeader(brain string, ref controlrecord.ImmutableRef) (readInputPreHeader, error) {
	var out readInputPreHeader
	// The full pre-source target policy is validated by its C2 owner. This
	// reader checks only the fixed immutable artifact and neutral caller tags.
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "effects", "control-reads") || !programHex(strings.TrimSuffix(filepath.Base(ref.Path), ".pre.json"), 64) || !strings.HasSuffix(ref.Path, ".pre.json") {
		return out, ErrObservationUnknown
	}
	if err := verifyProgramManifest(ProgramArtifact{Path: ref.Path, SHA256: ref.SHA256}, 16<<20); err != nil {
		return out, err
	}
	data, err := os.ReadFile(ref.Path)
	if err != nil {
		return out, err
	}
	if len(data) > 16<<20 || sha256.Sum256(data) != ref.SHA256 {
		return out, ErrObservationUnknown
	}
	if err := toolcontract.DecodeBroker(data, &out); err != nil {
		return out, err
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, data) || out.Schema != 2 || out.Purpose == "" || (len(out.Coordinator) == 0) == (len(out.Helper) == 0) || bytes.Equal(out.Coordinator, []byte("null")) || bytes.Equal(out.Helper, []byte("null")) {
		return out, ErrObservationUnknown
	}
	return out, nil
}

func programReadInputArtifactAbsence(ctx context.Context, brain, requestID string) error {
	dir, err := ProgramReadDirectory(brain, requestID)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil || canonical != dir {
		return ErrObservationUnknown
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 || len(entries) > 8192 {
		return ErrObservationUnknown
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Name() == "input-absent.json" {
			if _, err := ProgramReadInputAbsentForRequestID(ctx, brain, requestID); err != nil {
				return err
			}
			continue
		}
		if !strings.HasPrefix(entry.Name(), ".codex-") {
			return ErrObservationUnknown
		}
		st, err := os.Lstat(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		identity, ok := st.Sys().(*syscall.Stat_t)
		if !ok || identity.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			return ErrObservationUnknown
		}
	}
	return nil
}
