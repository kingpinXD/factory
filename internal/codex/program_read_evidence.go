package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/zednative"
)

type ProgramArtifact struct {
	Path   string         `json:"path"`
	SHA256 effects.Digest `json:"sha256"`
}

type ProgramReadResultRef struct {
	RequestID      string            `json:"request_id"`
	RequestSHA256  effects.Digest    `json:"request_sha256"`
	ResponseSHA256 effects.Digest    `json:"response_sha256"`
	Process        ProgramProcessRef `json:"process"`
}

type ProgramBaseImport struct {
	Repo             string          `json:"repo"`
	Base             string          `json:"base"`
	Ref              string          `json:"ref"`
	Commit           string          `json:"commit"`
	Tree             string          `json:"tree"`
	ObjectsDirectory string          `json:"objects_directory"`
	ObjectsManifest  ProgramArtifact `json:"objects_manifest"`
	ContentManifest  ProgramArtifact `json:"content_manifest"`
	Repository       ProgramArtifact `json:"repository"`
}

type ProgramReadRequest struct {
	Schema     int                          `json:"schema"`
	RequestID  string                       `json:"request_id"`
	Kind       string                       `json:"kind"`
	Operation  string                       `json:"operation"`
	Input      toolcontract.GitHubReadInput `json:"input"`
	Source     json.RawMessage              `json:"source,omitempty"`
	Grant      *AcceptanceReadGrantRef      `json:"grant,omitempty"`
	Clone      string                       `json:"clone,omitempty"`
	Worktree   string                       `json:"worktree,omitempty"`
	Common     string                       `json:"common,omitempty"`
	AdmittedAt time.Time                    `json:"admitted_at"`
	Deadline   time.Time                    `json:"deadline"`
}

type ProgramReadResult struct {
	Schema           int                `json:"schema"`
	Request          ProgramReadRef     `json:"request"`
	Operation        string             `json:"operation"`
	ReadJSON         json.RawMessage    `json:"read_json,omitempty"`
	Base             *ProgramBaseImport `json:"base,omitempty"`
	Check            *ProgramArtifact   `json:"check,omitempty"`
	Failure          string             `json:"failure,omitempty"`
	ClaimID          string             `json:"claim_id"`
	EnrollmentSHA256 effects.Digest     `json:"enrollment_sha256"`
}

func ProgramReadDirectory(brain, requestID string) (string, error) {
	if !filepath.IsAbs(brain) || filepath.Clean(brain) != brain || !programHex(requestID, 64) {
		return "", ErrObservationUnknown
	}
	return filepath.Join(store.Factory(brain), "codex", "program-reads", requestID), nil
}

func ValidateProgramReadInput(operation string, input toolcontract.GitHubReadInput) error {
	if operation == "registry_checks" {
		if input.Repo == "" || input.Base == "" || (!programHex(input.SHA, 40) && !programHex(input.SHA, 64)) || input.Number != 0 || input.Path != "" || !input.Since.IsZero() {
			return fmt.Errorf("registry checks require only their registered repository, base and exact revision")
		}
		return nil
	}
	if internal, err := gh.ValidateProgramInternalReadInput(operation, input); internal {
		return err
	}
	if operation == "repo_base" {
		if input.Repo == "" || input.Base == "" || input.Number != 0 || input.Path != "" || !input.Since.IsZero() {
			return fmt.Errorf("base fetch requires only its registered repository, base and selected commit")
		}
		if input.SHA != "" && !programHex(input.SHA, 40) && !programHex(input.SHA, 64) {
			return fmt.Errorf("base fetch commit must be exact")
		}
		return nil
	}
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	// time.Time does not honor omitempty. This is input validation only, not
	// the canonical payload witness used to bind an actual control source.
	if input.Since.IsZero() {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(b, &fields); err != nil {
			return err
		}
		delete(fields, "since")
		b, err = json.Marshal(fields)
		if err != nil {
			return err
		}
	}
	decoded, err := toolcontract.DecodeInput(toolcontract.FactoryControl, operation, b)
	if err != nil {
		return err
	}
	if _, ok := decoded.(*toolcontract.GitHubReadInput); ok {
		return nil
	}
	if operation == "issue_read" || operation == "issue_children" {
		return nil
	}
	return fmt.Errorf("operation is not a programme read")
}

func validateProgramReadRequest(r ProgramReadRequest) error {
	if r.Schema != 2 || !programHex(r.RequestID, 64) || r.AdmittedAt.IsZero() || !r.Deadline.After(r.AdmittedAt) || ValidateProgramReadInput(r.Operation, r.Input) != nil {
		return ErrObservationUnknown
	}
	if r.Kind == "acceptance_read" {
		if r.Grant == nil || len(r.Source) != 0 || r.Operation != "repo_base" {
			return ErrObservationUnknown
		}
	} else if r.Kind != "program_read" || r.Grant != nil || len(r.Source) == 0 || !json.Valid(r.Source) {
		return ErrObservationUnknown
	}
	limit := 15 * time.Second
	if r.Operation == "registry_checks" {
		if r.Kind != "program_read" || r.Deadline.Sub(r.AdmittedAt) != 2*time.Minute {
			return ErrObservationUnknown
		}
		limit = 2 * time.Minute
	}
	if r.Operation == "repo_base" {
		limit = 30 * time.Minute
		if r.Kind == "acceptance_read" {
			limit = 8 * time.Minute
		}
		for _, p := range []string{r.Clone, r.Worktree, r.Common} {
			if !filepath.IsAbs(p) || filepath.Clean(p) != p {
				return ErrObservationUnknown
			}
		}
	} else if r.Clone != "" || r.Worktree != "" || r.Common != "" {
		return ErrObservationUnknown
	}
	if r.Deadline.Sub(r.AdmittedAt) > limit {
		return ErrObservationUnknown
	}
	return nil
}

// PublishProgramReadRequestLocked stores input only. Its caller holds the real
// source admission guard; the file alone cannot authorize a process claim.
func PublishProgramReadRequestLocked(brain string, request ProgramReadRequest) (ProgramReadRef, error) {
	var ref ProgramReadRef
	if err := validateProgramReadRequest(request); err != nil {
		return ref, err
	}
	if request.Kind == "program_read" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		guard, err := AcquireProgramReadInputGuard(ctx, brain, request.RequestID)
		if err != nil {
			return ref, err
		}
		defer guard.Close()
		return guard.publishRequest(ctx, request)
	}
	return publishProgramReadRequest(brain, request)
}

func publishProgramReadRequest(brain string, request ProgramReadRequest) (ProgramReadRef, error) {
	var ref ProgramReadRef
	dir, err := ProgramReadDirectory(brain, request.RequestID)
	if err != nil {
		return ref, err
	}
	b, err := json.Marshal(request)
	if err != nil {
		return ref, err
	}
	ref = ProgramReadRef{RequestID: request.RequestID, SHA256: sha256.Sum256(b)}
	if err := publishProgramReadArtifact(filepath.Join(dir, "request.json"), b); err != nil {
		return ProgramReadRef{}, err
	}
	return ref, nil
}

func ReadProgramReadRequest(ctx context.Context, brain string, ref ProgramReadRef) (ProgramReadRequest, error) {
	if err := ctx.Err(); err != nil {
		return ProgramReadRequest{}, err
	}
	if _, err := ProgramReadDirectory(brain, ref.RequestID); err != nil {
		return ProgramReadRequest{}, err
	}
	if ref.SHA256 == (effects.Digest{}) {
		return ProgramReadRequest{}, fmt.Errorf("invalid programme artifact reference")
	}
	request, actual, err := readProgramReadRequestFile(ctx, brain, ref.RequestID)
	if err != nil {
		return request, err
	}
	if actual != ref {
		return request, fmt.Errorf("programme artifact changed")
	}
	return request, nil
}

func ReadProgramReadRequestReference(ctx context.Context, brain, requestID string) (ProgramReadRef, error) {
	_, ref, err := readProgramReadRequestFile(ctx, brain, requestID)
	return ref, err
}

func readProgramReadRequestFile(ctx context.Context, brain, requestID string) (ProgramReadRequest, ProgramReadRef, error) {
	var request ProgramReadRequest
	if err := ctx.Err(); err != nil {
		return request, ProgramReadRef{}, err
	}
	dir, err := ProgramReadDirectory(brain, requestID)
	if err != nil {
		return request, ProgramReadRef{}, err
	}
	data, err := readProgramArtifactFile(filepath.Join(dir, "request.json"))
	if err != nil {
		return request, ProgramReadRef{}, err
	}
	if err := toolcontract.DecodeBroker(data, &request); err != nil {
		return request, ProgramReadRef{}, err
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, data) || request.RequestID != requestID || validateProgramReadRequest(request) != nil {
		return request, ProgramReadRef{}, ErrObservationUnknown
	}
	return request, ProgramReadRef{RequestID: requestID, SHA256: ProgramProcessDigest(request)}, nil
}

func PublishProgramReadResult(brain string, result ProgramReadResult) error {
	if result.Schema != 2 || !programHex(result.Request.RequestID, 64) || result.Request.SHA256 == (effects.Digest{}) || result.ClaimID == "" || result.EnrollmentSHA256 == (effects.Digest{}) {
		return ErrObservationUnknown
	}
	if err := validateProgramReadResult(result); err != nil {
		return err
	}
	dir, _ := ProgramReadDirectory(brain, result.Request.RequestID)
	b, err := encodeProgramReadResult(result)
	if err != nil {
		return err
	}
	return publishProgramReadArtifact(filepath.Join(dir, "result.json"), b)
}

// Result data keeps its bounded provider JSON without expanding HTML escapes.
// This private result encoding does not change any input or effect digest.
func encodeProgramReadResult(result ProgramReadResult) ([]byte, error) {
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte{'\n'}), nil
}

func decodeProgramReadResult(data []byte) (ProgramReadResult, error) {
	var result ProgramReadResult
	if len(data) > maxFrame {
		return result, ErrObservationUnknown
	}
	if err := toolcontract.DecodeBroker(data, &result); err != nil {
		return result, err
	}
	canonical, err := encodeProgramReadResult(result)
	if err != nil || !bytes.Equal(canonical, data) {
		return result, ErrObservationUnknown
	}
	return result, nil
}

func validateProgramReadResult(r ProgramReadResult) error {
	n := 0
	if len(r.ReadJSON) != 0 {
		if !json.Valid(r.ReadJSON) || len(r.ReadJSON) > toolcontract.ControlLimit {
			return ErrObservationUnknown
		}
		n++
	}
	if r.Base != nil {
		n++
	}
	if r.Check != nil {
		if r.Operation != "registry_checks" || r.Check.Path == "" || r.Check.SHA256 == (effects.Digest{}) {
			return ErrObservationUnknown
		}
		n++
	}
	if r.Failure != "" {
		if r.Failure != "read_unavailable" {
			return ErrObservationUnknown
		}
		n++
	}
	if n != 1 || r.Operation == "registry_checks" && r.Check == nil {
		return ErrObservationUnknown
	}
	return nil
}

// ReadProgramReadResultReference derives a result reference from the actual
// immutable output and genuine closed physical proof. It does not infer success
// from a missing output or from a process exit alone.
func ReadProgramReadResultReference(ctx context.Context, brain string, request ProgramReadRef, process ProgramProcessRef) (ProgramReadResultRef, error) {
	return readProgramReadResultReference(ctx, brain, request, process, false)
}

func ReadProgramReadResultReferenceLocal(ctx context.Context, brain string, request ProgramReadRef, process ProgramProcessRef) (ProgramReadResultRef, error) {
	return readProgramReadResultReference(ctx, brain, request, process, true)
}

func readProgramReadResultReference(ctx context.Context, brain string, request ProgramReadRef, process ProgramProcessRef, local bool) (ProgramReadResultRef, error) {
	dir, err := ProgramReadDirectory(brain, request.RequestID)
	if err != nil {
		return ProgramReadResultRef{}, err
	}
	data, err := readAcceptanceArtifact(filepath.Join(dir, "result.json"))
	if err != nil {
		return ProgramReadResultRef{}, err
	}
	ref := ProgramReadResultRef{RequestID: request.RequestID, RequestSHA256: request.SHA256, ResponseSHA256: sha256.Sum256(data), Process: process}
	if local {
		_, err = ReadProgramReadEvidenceLocal(ctx, brain, ref)
	} else {
		_, err = ReadProgramReadEvidence(ctx, brain, ref)
	}
	if err != nil {
		var unavailable *gh.ProgramReadUnavailable
		if errors.As(err, &unavailable) {
			return ref, err
		}
		return ProgramReadResultRef{}, err
	}
	return ref, nil
}

// ReadProgramReadEvidence performs full local Git verification and must run
// outside owner, helper, effects and physical locks. Admission uses a separate
// bounded immutable qualification reader, not this explicit completion check.
func ReadProgramReadEvidence(ctx context.Context, brain string, ref ProgramReadResultRef) (ProgramReadResult, error) {
	return readProgramReadEvidence(ctx, brain, ref, false, true)
}

// ReadProgramReadEvidenceLocal validates retained fixed-read evidence only.
// The consumer separately validates its actual retained admission and Original.
func ReadProgramReadEvidenceLocal(ctx context.Context, brain string, ref ProgramReadResultRef) (ProgramReadResult, error) {
	return readProgramReadEvidence(ctx, brain, ref, true, false)
}

func readProgramReadEvidence(ctx context.Context, brain string, ref ProgramReadResultRef, local, checks bool) (ProgramReadResult, error) {
	return readProgramReadEvidenceUsing(ctx, brain, ref, local, checks, nil)
}

func readProgramReadEvidenceUsing(ctx context.Context, brain string, ref ProgramReadResultRef, local, checks bool, snapshot *RegistryCheckSourceSnapshot) (ProgramReadResult, error) {
	var result ProgramReadResult
	request, err := snapshot.request(ctx, brain, ProgramReadRef{RequestID: ref.RequestID, SHA256: ref.RequestSHA256})
	if err != nil {
		return result, err
	}
	if local && (request.Kind != "program_read" || request.Operation == "repo_base" || request.Operation == "registry_checks" && !checks) {
		return result, ErrObservationUnknown
	}
	var process ProgramProcessRecord
	if local {
		process, err = ReadClosedProgramProcessLocal(ctx, brain, ref.Process)
	} else {
		process, err = ReadClosedProgramProcess(ctx, brain, ref.Process)
	}
	if err != nil || process.Deadline.After(request.Deadline) || process.AdmittedAt.Before(request.AdmittedAt) {
		return result, ErrObservationUnknown
	}
	if request.Kind == "program_read" {
		if process.Subject.ProgramRead == nil || *process.Subject.ProgramRead != (ProgramReadRef{RequestID: ref.RequestID, SHA256: ref.RequestSHA256}) {
			return result, ErrObservationUnknown
		}
	} else {
		if process.Subject.AcceptanceRead == nil || request.Grant == nil || *process.Subject.AcceptanceRead != *request.Grant {
			return result, ErrObservationUnknown
		}
		enrollment, err := ReadAcceptanceReadEnrollment(ctx, brain, *request.Grant)
		if err != nil || process.OwnerID != enrollment.OwnerID || process.LogicalRevision != enrollment.LogicalRevision || process.FactorySHA256 != enrollment.FactorySHA256 || process.AdmittedAt.Before(enrollment.Request.AdmittedAt) || ProgramProcessDigest(request) != ProgramProcessDigest(enrollment.Request) {
			return result, ErrObservationUnknown
		}
	}
	dir, _ := ProgramReadDirectory(brain, ref.RequestID)
	b, err := readProgramArtifact(ProgramArtifact{Path: filepath.Join(dir, "result.json"), SHA256: ref.ResponseSHA256})
	if err != nil {
		return result, err
	}
	if snapshot != nil {
		snapshot.artifacts[ProgramArtifact{Path: filepath.Join(dir, "result.json"), SHA256: ref.ResponseSHA256}] = b
	}
	result, err = decodeProgramReadResult(b)
	if err != nil {
		return result, err
	}
	if result.Schema != 2 || result.Request != (ProgramReadRef{RequestID: ref.RequestID, SHA256: ref.RequestSHA256}) || result.Operation != request.Operation || result.ClaimID != process.ClaimID || result.EnrollmentSHA256 != process.EnrollmentSHA256 || validateProgramReadResult(result) != nil {
		return result, ErrObservationUnknown
	}
	if result.Failure != "" {
		return result, &gh.ProgramReadUnavailable{}
	}
	if request.Operation == "registry_checks" {
		if process.ClosedProof.Kind != "owned_group_gone" && process.ClosedProof.Kind != "never_claimed" {
			return result, ErrObservationUnknown
		}
		observation, err := readRegistryCheckDomainEvidence(ctx, brain, request, process, result, snapshot)
		if err != nil {
			return result, err
		}
		if process.ClosedProof.Kind == "never_claimed" && observation.Outcome != "not_started" {
			return result, ErrObservationUnknown
		}
		if snapshot != nil {
			snapshot.results[ref] = observation
		}
		return result, nil
	}
	if process.ClosedProof.Kind != "owned_group_gone" || process.Host.PID <= 1 {
		return result, ErrObservationUnknown
	}
	if result.Check != nil {
		return result, ErrObservationUnknown
	}
	if request.Operation != "repo_base" {
		if result.Base != nil {
			return result, ErrObservationUnknown
		}
		return result, nil
	}
	if result.Base == nil || result.Base.Repo != request.Input.Repo || result.Base.Base != request.Input.Base || result.Base.Ref != "refs/heads/"+request.Input.Base || result.Base.ObjectsDirectory != filepath.Join(request.Common, "objects") || request.Input.SHA != "" && result.Base.Commit != request.Input.SHA {
		return result, ErrObservationUnknown
	}
	manifestDir := dir
	if request.Grant != nil {
		target, err := ReadAcceptanceReadTarget(ctx, brain, *request.Grant)
		if err != nil {
			return result, err
		}
		manifestDir = filepath.Dir(target.Bare)
		if err := ValidateProgramPrivateRepository(*result.Base, filepath.Join(manifestDir, "repository.json")); err != nil {
			return result, err
		}
	}
	if result.Base.ObjectsManifest.Path != filepath.Join(manifestDir, "program-objects.json") || result.Base.ContentManifest.Path != filepath.Join(manifestDir, "program-content.raw") {
		return result, ErrObservationUnknown
	}
	verified, err := zednative.VerifyProgramCommit(ctx, request.Common, result.Base.Commit)
	if err != nil || verified.Commit != result.Base.Commit || verified.Tree != result.Base.Tree || verified.ObjectsDirectory != result.Base.ObjectsDirectory || verified.ObjectsSHA256 != result.Base.ObjectsManifest.SHA256.String() || verified.ContentSHA256 != result.Base.ContentManifest.SHA256.String() {
		return result, ErrObservationUnknown
	}
	if err := verifyProgramManifest(result.Base.ObjectsManifest, maxFrame); err != nil {
		return result, err
	}
	if err := verifyProgramManifest(result.Base.ContentManifest, zednative.ProgramGitManifestLimit); err != nil {
		return result, err
	}
	return result, nil
}

func ValidateProgramPrivateRepository(base ProgramBaseImport, path string) error {
	if base.Repository.Path != path {
		return ErrObservationUnknown
	}
	data, err := readProgramArtifact(base.Repository)
	if err != nil {
		return err
	}
	var repository gh.ProgramRepository
	if err := toolcontract.DecodeStrict(data, &repository); err != nil {
		return err
	}
	canonical, _ := json.Marshal(repository)
	if !bytes.Equal(canonical, data) || !strings.EqualFold(repository.FullName, base.Repo) || repository.NodeID == "" || strings.TrimSpace(repository.NodeID) != repository.NodeID || len(repository.NodeID) > 256 || strings.ContainsAny(repository.NodeID, "\r\n\x00") || !repository.Private {
		return ErrObservationUnknown
	}
	return nil
}

func verifyProgramManifest(ref ProgramArtifact, limit int64) error {
	if !filepath.IsAbs(ref.Path) || filepath.Clean(ref.Path) != ref.Path || ref.SHA256 == (effects.Digest{}) {
		return ErrObservationUnknown
	}
	real, err := filepath.EvalSymlinks(ref.Path)
	if err != nil || real != ref.Path {
		return ErrObservationUnknown
	}
	f, err := os.OpenFile(ref.Path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > limit {
		return ErrObservationUnknown
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(f, limit+1)); err != nil || hex.EncodeToString(h.Sum(nil)) != ref.SHA256.String() {
		return ErrObservationUnknown
	}
	return nil
}

func publishProgramReadArtifact(name string, b []byte) error {
	if len(b) > maxFrame || !json.Valid(b) {
		return ErrObservationUnknown
	}
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(dir); err != nil || real != dir {
		return ErrObservationUnknown
	}
	if _, err := os.Lstat(name); err == nil {
		expected := ProgramArtifact{Path: name, SHA256: sha256.Sum256(b)}
		if _, err := readProgramArtifact(expected); err != nil {
			return err
		}
		return syncExistingProgramRead(name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeRecordBytes(name, b)
}

func syncExistingProgramRead(name string) error {
	f, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	f, err = os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// ProgramGitCredentialHelper returns fixed code, never a credential. Only an
// already claimed programme actor may install it in its isolated fetch command.
func ProgramGitCredentialHelper(repo string) (string, error) {
	path := githubCredentialPath("https://github.com/" + repo + ".git")
	if path == "" || strings.TrimSuffix(path, ".git") != repo {
		return "", fmt.Errorf("canonical GitHub repository required")
	}
	return githubCredentialHelper(path), nil
}

func readProgramArtifact(ref ProgramArtifact) ([]byte, error) {
	if ref.SHA256 == (effects.Digest{}) {
		return nil, fmt.Errorf("invalid programme artifact reference")
	}
	b, err := readProgramArtifactFile(ref.Path)
	if err != nil {
		return nil, err
	}
	if effects.Digest(sha256.Sum256(b)) != ref.SHA256 {
		return nil, fmt.Errorf("programme artifact changed")
	}
	return b, nil
}
func readProgramArtifactFile(name string) ([]byte, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return nil, fmt.Errorf("invalid programme artifact reference")
	}
	st, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return nil, fmt.Errorf("aliased programme artifact")
	}
	return readAcceptanceArtifact(name)
}

type ProgramReadInventoryEntry struct {
	RequestID   string
	Reference   ProgramReadRef
	Request     ProgramReadRequest
	Err         error
	InputAbsent *ProgramReadInputAbsentRef
}

// InspectProgramReadRequests observes the private namespace. An indexed row or
// a self-computed hash grants no source or process authority.
func InspectProgramReadRequests(ctx context.Context, brain string) ([]ProgramReadInventoryEntry, error) {
	sample, err := ProgramReadDirectory(brain, strings.Repeat("0", 64))
	if err != nil {
		return nil, err
	}
	root := filepath.Dir(sample)
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	st, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if canonical != root || !st.IsDir() || st.Mode().Perm() != 0700 {
		return nil, ErrObservationUnknown
	}
	directory, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	var entries []ProgramReadInventoryEntry
	budget := maxFrame
	for {
		if err := ctx.Err(); err != nil {
			return entries, err
		}
		rows, err := directory.ReadDir(1)
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return entries, err
		}
		for _, row := range rows {
			budget -= len(row.Name()) + 1
			if budget < 0 {
				return entries, fmt.Errorf("programme inventory exceeds private artifact bound")
			}
			item := ProgramReadInventoryEntry{RequestID: row.Name()}
			if !row.IsDir() || !programHex(row.Name(), 64) {
				item.Err = ErrObservationUnknown
				entries = append(entries, item)
				continue
			}
			ref, e := ReadProgramReadRequestReference(ctx, brain, row.Name())
			if e == nil {
				item.Reference = ref
				item.Request, e = ReadProgramReadRequest(ctx, brain, ref)
			} else if errors.Is(e, os.ErrNotExist) {
				absent, absentErr := ProgramReadInputAbsentForRequestID(ctx, brain, row.Name())
				if absentErr == nil {
					if absenceErr := programReadInputArtifactAbsence(ctx, brain, row.Name()); absenceErr != nil {
						e = absenceErr
					} else {
						item.InputAbsent = &absent
						e = nil
					}
				} else if !errors.Is(absentErr, os.ErrNotExist) {
					e = absentErr
				}
			}

			item.Err = e
			entries = append(entries, item)
		}
	}
}

// ProgramReadSourceObservation binds an external containment check to the exact
// local source subsequently checked under its admission guard.
type ProgramReadSourceObservation struct {
	brain   string
	attempt AttemptRef
	job     helper.Job
	source  effects.Digest
	owner   effects.Digest
}

func ObserveProgramReadControl(ctx context.Context, brain string, ref AttemptRef) (*ProgramReadSourceObservation, error) {
	return observeProgramReadControl(ctx, New(brain, ""), ref)
}

func observeProgramReadControl(ctx context.Context, runtime *Runtime, ref AttemptRef) (*ProgramReadSourceObservation, error) {
	if runtime == nil || runtime.Containment == nil {
		return nil, ErrObservationUnknown
	}
	if _, err := AttemptControlAuthority(runtime.Brain, ref); err != nil {
		return nil, err
	}
	_, record, err := runtime.readCurrent(ref.SessionID)
	if err != nil || record.attemptRef() != ref {
		return nil, ErrAttemptStale
	}
	owner, err := attemptOwner(runtime, record)
	if err != nil {
		return nil, err
	}
	if err := runtime.Containment.Check(ctx, owner); err != nil {
		return nil, err
	}
	return &ProgramReadSourceObservation{brain: runtime.Brain, attempt: ref, source: ProgramProcessDigest(record), owner: ProgramProcessDigest(owner)}, nil
}

func ObserveProgramReadHelper(ctx context.Context, brain string, job helper.Job) (*ProgramReadSourceObservation, error) {
	return observeProgramReadHelper(ctx, &Backend{Brain: brain, Containment: remoteWorkspaceDefault()}, job)
}

func observeProgramReadHelper(ctx context.Context, backend *Backend, job helper.Job) (*ProgramReadSourceObservation, error) {
	if backend == nil || backend.Containment == nil {
		return nil, ErrObservationUnknown
	}
	if err := ValidateHelperAttempt(backend.Brain, job); err != nil {
		return nil, err
	}
	record, err := backend.read(job)
	if err != nil {
		return nil, err
	}
	owner := backend.owner(record)
	if err := backend.Containment.Check(ctx, owner); err != nil {
		return nil, err
	}
	return &ProgramReadSourceObservation{brain: backend.Brain, job: job, source: ProgramProcessDigest(record), owner: ProgramProcessDigest(owner)}, nil
}

func (o *ProgramReadSourceObservation) ValidateControlLocal(ctx context.Context, brain string, ref AttemptRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil || o.brain != brain || o.attempt != ref || !ref.valid() || o.job.ID != "" {
		return ErrAttemptStale
	}
	if _, err := AttemptControlAuthority(brain, ref); err != nil {
		return err
	}
	runtime := New(brain, "")
	_, record, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	owner, err := attemptOwner(runtime, record)
	if err != nil {
		return err
	}
	if record.attemptRef() != ref || ProgramProcessDigest(record) != o.source || ProgramProcessDigest(owner) != o.owner {
		return ErrAttemptStale
	}
	return nil
}

func (o *ProgramReadSourceObservation) ValidateHelperLocal(ctx context.Context, brain string, job helper.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil || o.brain != brain || o.attempt != (AttemptRef{}) || o.job.ID == "" || ProgramProcessDigest(o.job) != ProgramProcessDigest(job) {
		return ErrAttemptStale
	}
	if err := ValidateHelperAttempt(brain, job); err != nil {
		return err
	}
	backend := &Backend{Brain: brain}
	record, err := backend.read(job)
	if err != nil {
		return err
	}
	if ProgramProcessDigest(record) != o.source || ProgramProcessDigest(backend.owner(record)) != o.owner {
		return ErrAttemptStale
	}
	return nil
}

func readProgramAdmissionWitness(admission json.RawMessage) (controlrecord.OriginalRequestRef, controlrecord.OwnerAuthority, *helper.Scope, error) {
	var original controlrecord.OriginalRequestRef
	var authority controlrecord.OwnerAuthority
	var scope *helper.Scope
	var fields map[string]json.RawMessage
	if err := toolcontract.DecodeBroker(admission, &fields); err != nil {
		return original, authority, scope, err
	}
	present := func(raw json.RawMessage) bool { return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) }
	control, child := present(fields["Control"]), present(fields["Helper"])
	if control == child || present(fields["RepoRequest"]) {
		return original, authority, scope, ErrObservationUnknown
	}
	if err := toolcontract.DecodeStrict(fields["Authority"], &authority); err != nil {
		return original, authority, scope, err
	}
	source := fields["Control"]
	if child {
		source = fields["Helper"]
	}
	var caller map[string]json.RawMessage
	if err := toolcontract.DecodeBroker(source, &caller); err != nil {
		return original, authority, scope, err
	}
	if err := toolcontract.DecodeStrict(caller["Original"], &original); err != nil {
		return original, authority, scope, err
	}
	if child {
		if err := toolcontract.DecodeStrict(fields["Work"], &scope); err != nil || scope == nil {
			return original, authority, scope, ErrObservationUnknown
		}
	}
	return original, authority, scope, nil
}

func readProgramCoordinatorWorkWitness(source json.RawMessage, original controlrecord.OriginalRequestRef, control effects.ControlID) (*helper.Scope, error) {
	var fields map[string]json.RawMessage
	if err := toolcontract.DecodeBroker(source, &fields); err != nil {
		return nil, err
	}
	var scope *helper.Scope
	if err := toolcontract.DecodeBroker(fields["Work"], &scope); err != nil || scope == nil {
		return nil, ErrObservationUnknown
	}
	var caller struct {
		Receipt  effects.ControlReceiptRef
		Input    effects.ControlInput
		Original controlrecord.OriginalRequestRef
	}
	if err := toolcontract.DecodeBroker(fields["Control"], &caller); err != nil {
		return nil, err
	}
	if caller.Original != original || caller.Receipt.Control != control || caller.Input.Target.WorkID != scope.WorkID || scope.WorkID == "" {
		return nil, ErrObservationUnknown
	}
	return scope, nil
}
