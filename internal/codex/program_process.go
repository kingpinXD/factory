package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type ProgramReadRef struct {
	RequestID string         `json:"request_id"`
	SHA256    effects.Digest `json:"sha256"`
}
type AcceptanceReadGrantRef struct {
	Binding string         `json:"binding"`
	ProofID string         `json:"proof_id"`
	SHA256  effects.Digest `json:"sha256"`
}
type ProgramProcessSubject struct {
	Kind           string                  `json:"kind"`
	EffectSlotID   string                  `json:"effect_slot_id,omitempty"`
	ProgramRead    *ProgramReadRef         `json:"program_read,omitempty"`
	AcceptanceRead *AcceptanceReadGrantRef `json:"acceptance_read,omitempty"`
}
type ProgramProcessRef struct {
	SubjectID string         `json:"subject_id"`
	ClaimID   string         `json:"claim_id"`
	Revision  uint64         `json:"revision"`
	SHA256    effects.Digest `json:"sha256"`
}
type ProgramProcessSelection struct {
	Schema           int            `json:"schema"`
	Revision         uint64         `json:"revision"`
	SubjectID        string         `json:"subject_id"`
	ClaimID          string         `json:"claim_id"`
	EnrollmentSHA256 effects.Digest `json:"enrollment_sha256"`
}
type ProgramProcessClosedProof struct {
	Kind             string          `json:"kind"`
	SubjectID        string          `json:"subject_id"`
	ClaimID          string          `json:"claim_id"`
	SealedRevision   uint64          `json:"sealed_revision"`
	EnrollmentSHA256 effects.Digest  `json:"enrollment_sha256"`
	SelectionSHA256  effects.Digest  `json:"selection_sha256"`
	Host             ProcessIdentity `json:"host"`
	Deadline         time.Time       `json:"deadline"`
	DispatchID       string          `json:"dispatch_id,omitempty"`
}
type ProgramProcessRecord struct {
	Schema            int                        `json:"schema"`
	Revision          uint64                     `json:"revision"`
	Subject           ProgramProcessSubject      `json:"subject"`
	SubjectID         string                     `json:"subject_id"`
	ClaimID           string                     `json:"claim_id"`
	NonceSHA256       effects.Digest             `json:"nonce_sha256"`
	OwnerID           string                     `json:"owner_id"`
	LogicalRevision   string                     `json:"logical_revision"`
	InputSHA256       effects.Digest             `json:"input_sha256"`
	FactorySHA256     effects.Digest             `json:"factory_sha256"`
	Mode              string                     `json:"mode"`
	AdmittedAt        time.Time                  `json:"admitted_at"`
	Deadline          time.Time                  `json:"deadline"`
	EnrollmentSHA256  effects.Digest             `json:"enrollment_sha256"`
	SelectionRevision uint64                     `json:"selection_revision"`
	SelectionSHA256   effects.Digest             `json:"selection_sha256"`
	Stage             string                     `json:"stage"`
	NextCheckAt       time.Time                  `json:"next_check_at,omitzero"`
	Host              ProcessIdentity            `json:"host"`
	SealedRevision    uint64                     `json:"sealed_revision,omitempty"`
	ClosedProof       *ProgramProcessClosedProof `json:"closed_proof,omitempty"`
}

func ProgramProcessDigest(v any) effects.Digest {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("invalid programme process value: %v", err))
	}
	return sha256.Sum256(b)
}
func programHex(s string, n int) bool {
	if len(s) != n || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func ProgramProcessSubjectID(s ProgramProcessSubject) (string, error) {
	switch s.Kind {
	case "normal_effect":
		if !programHex(s.EffectSlotID, 64) || s.ProgramRead != nil || s.AcceptanceRead != nil {
			return "", ErrObservationUnknown
		}
		return s.EffectSlotID, nil
	case "program_read":
		if s.EffectSlotID != "" || s.ProgramRead == nil || s.AcceptanceRead != nil || !programHex(s.ProgramRead.RequestID, 64) || s.ProgramRead.SHA256 == (effects.Digest{}) {
			return "", ErrObservationUnknown
		}
		b, _ := json.Marshal(s.ProgramRead)
		sum := sha256.Sum256(append([]byte("factory-program-read-v2\x00"), b...))
		return hex.EncodeToString(sum[:]), nil
	case "acceptance_read":
		r := s.AcceptanceRead
		if s.EffectSlotID != "" || s.ProgramRead != nil || r == nil || !programHex(r.Binding, 64) || r.ProofID == "" || filepath.Base(r.ProofID) != r.ProofID || r.ProofID == "." || r.ProofID == ".." || strings.ContainsAny(r.ProofID, "\\\x00\r\n") || r.SHA256 == (effects.Digest{}) {
			return "", ErrObservationUnknown
		}
		b, _ := json.Marshal(r)
		sum := sha256.Sum256(append([]byte("factory-acceptance-read-v2\x00"), b...))
		return hex.EncodeToString(sum[:]), nil
	}
	return "", ErrObservationUnknown
}

func ProgramProcessLimit(subject ProgramProcessSubject, mode string) (time.Duration, error) {
	switch subject.Kind {
	case "normal_effect":
		switch mode {
		case "execute":
			return 30 * time.Second, nil
		case "observe":
			return 15 * time.Second, nil
		}
	case "program_read", "acceptance_read":
		switch mode {
		case "check":
			if subject.Kind == "program_read" {
				return 2 * time.Minute, nil
			}
		case "read":
			return 15 * time.Second, nil
		case "fetch":
			if subject.Kind == "acceptance_read" {
				return 8 * time.Minute, nil
			}
			return 30 * time.Minute, nil
		}
	}
	return 0, ErrObservationUnknown
}

func ProgramProcessEnrollmentDigest(r ProgramProcessRecord) effects.Digest {
	return ProgramProcessDigest(struct {
		Subject                  ProgramProcessSubject
		SubjectID, ClaimID       string
		Nonce                    effects.Digest
		OwnerID, LogicalRevision string
		Input, Factory           effects.Digest
		Mode                     string
		AdmittedAt, Deadline     time.Time
	}{r.Subject, r.SubjectID, r.ClaimID, r.NonceSHA256, r.OwnerID, r.LogicalRevision, r.InputSHA256, r.FactorySHA256, r.Mode, r.AdmittedAt, r.Deadline})
}
func ProgramProcessSelectionFor(r ProgramProcessRecord) ProgramProcessSelection {
	return ProgramProcessSelection{Schema: 1, Revision: r.SelectionRevision, SubjectID: r.SubjectID, ClaimID: r.ClaimID, EnrollmentSHA256: r.EnrollmentSHA256}
}

func ValidateProgramProcessRecord(r ProgramProcessRecord) error {
	id, err := ProgramProcessSubjectID(r.Subject)
	parts := strings.Split(r.ClaimID, ".")
	limit, e := ProgramProcessLimit(r.Subject, r.Mode)
	if err != nil || e != nil || r.Schema != 1 || r.Revision == 0 || id != r.SubjectID || len(parts) != 2 || parts[0] != id || !programHex(parts[1], 32) || r.OwnerID == "" || r.LogicalRevision == "" || r.NonceSHA256 == (effects.Digest{}) || r.InputSHA256 == (effects.Digest{}) || r.FactorySHA256 == (effects.Digest{}) || r.AdmittedAt.IsZero() || !r.Deadline.After(r.AdmittedAt) || r.Deadline.Sub(r.AdmittedAt) > limit || r.SelectionRevision == 0 || r.EnrollmentSHA256 != ProgramProcessEnrollmentDigest(r) || r.SelectionSHA256 != ProgramProcessDigest(ProgramProcessSelectionFor(r)) {
		return ErrObservationUnknown
	}
	// Revision identifies lifecycle stages. Sealed retry metadata uses full-record CAS.
	if r.Stage != "sealed" && !r.NextCheckAt.IsZero() {
		return ErrObservationUnknown
	}
	hostEmpty := r.Host == (ProcessIdentity{})
	switch r.Stage {
	case "prepared":
		if r.Revision != 1 || !hostEmpty || r.SealedRevision != 0 || r.ClosedProof != nil {
			return ErrObservationUnknown
		}
	case "claimed":
		if r.Revision != 2 || hostEmpty || r.Host.PID <= 1 || r.Host.Group != r.Host.PID || r.Host.Birth == "" || r.SealedRevision != 0 || r.ClosedProof != nil {
			return ErrObservationUnknown
		}
	case "sealed":
		expected := uint64(3)
		if hostEmpty {
			expected = 2
		}
		if r.Revision != expected || r.SealedRevision != r.Revision || r.ClosedProof != nil {
			return ErrObservationUnknown
		}
	case "closed":
		p := r.ClosedProof
		if p == nil || r.SealedRevision < 2 || r.Revision != r.SealedRevision+1 || p.SubjectID != r.SubjectID || p.ClaimID != r.ClaimID || p.SealedRevision != r.SealedRevision || p.EnrollmentSHA256 != r.EnrollmentSHA256 || p.SelectionSHA256 != r.SelectionSHA256 || p.Host != r.Host || !p.Deadline.Equal(r.Deadline) {
			return ErrObservationUnknown
		}
		switch p.Kind {
		case "never_claimed":
			if !hostEmpty || r.SealedRevision != 2 || p.DispatchID != "" {
				return ErrObservationUnknown
			}
		case "owned_group_gone":
			if r.SealedRevision != 3 || hostEmpty || r.Host.PID <= 1 || r.Host.Group != r.Host.PID || r.Host.Birth == "" {
				return ErrObservationUnknown
			}
		default:
			return ErrObservationUnknown
		}
		if r.Subject.Kind != "normal_effect" && p.DispatchID != "" {
			return ErrObservationUnknown
		}
	default:
		return ErrObservationUnknown
	}
	if r.Subject.Kind == "program_read" && r.InputSHA256 != r.Subject.ProgramRead.SHA256 {
		return ErrObservationUnknown
	}
	if r.Subject.Kind == "acceptance_read" && r.InputSHA256 != r.Subject.AcceptanceRead.SHA256 {
		return ErrObservationUnknown
	}
	if r.ClosedProof != nil && r.ClosedProof.DispatchID != "" && !programHex(r.ClosedProof.DispatchID, 64) {
		return ErrObservationUnknown
	}
	if !hostEmpty && (r.Host.PID <= 1 || r.Host.Group != r.Host.PID || r.Host.Birth == "") {
		return ErrObservationUnknown
	}
	return nil
}

func ProgramProcessPath(brain, subjectID, claimID string) (string, error) {
	if !filepath.IsAbs(brain) || !programHex(subjectID, 64) {
		return "", ErrObservationUnknown
	}
	parts := strings.Split(claimID, ".")
	if len(parts) != 2 || parts[0] != subjectID || !programHex(parts[1], 32) {
		return "", ErrObservationUnknown
	}
	return filepath.Join(brain, "factory", "effects", "dispatch", subjectID, claimID+".json"), nil
}

// ReadClosedProgramProcess checks the actual immutable record, never a caller flag.
func ReadClosedProgramProcess(ctx context.Context, brain string, ref ProgramProcessRef) (ProgramProcessRecord, error) {
	record, err := ReadClosedProgramProcessLocal(ctx, brain, ref)
	if err != nil {
		return record, err
	}
	if record.ClosedProof.Kind == "owned_group_gone" {
		if err := VerifyOwnedProcessGroupGone(record.Host); err != nil {
			return record, err
		}
	}
	if err := ctx.Err(); err != nil {
		return record, err
	}
	return record, nil
}

// ReadClosedProgramProcessLocal validates immutable historical evidence only.
// It grants no process admission and does not establish current group absence.
func ReadClosedProgramProcessLocal(ctx context.Context, brain string, ref ProgramProcessRef) (ProgramProcessRecord, error) {
	var record ProgramProcessRecord
	if err := ctx.Err(); err != nil {
		return record, err
	}
	p, err := ProgramProcessPath(brain, ref.SubjectID, ref.ClaimID)
	if err != nil {
		return record, err
	}
	canonical, err := filepath.EvalSymlinks(p)
	if err != nil || canonical != p {
		return record, ErrObservationUnknown
	}
	st, err := os.Lstat(p)
	if err != nil {
		return record, err
	}
	identity, ok := st.Sys().(*syscall.Stat_t)
	if !ok || identity.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 1<<20 {
		return record, ErrObservationUnknown
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return record, err
	}
	if err := toolcontract.DecodeStrict(b, &record); err != nil {
		return record, err
	}
	encoded, _ := json.Marshal(record)
	if !bytes.Equal(b, encoded) || ref.SHA256 != effects.Digest(sha256.Sum256(b)) || ref.Revision != record.Revision || ref.SubjectID != record.SubjectID || ref.ClaimID != record.ClaimID || record.Stage != "closed" || ValidateProgramProcessRecord(record) != nil {
		return record, ErrObservationUnknown
	}
	if err := ctx.Err(); err != nil {
		return record, err
	}
	return record, nil
}

func (r ProgramProcessRef) String() string { return fmt.Sprintf("%s@%d", r.ClaimID, r.Revision) }
