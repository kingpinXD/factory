package reconcile

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/zednative"
)

const controlSourceSnapshotLimit int64 = 16 << 20

func effectReadJSON(path string, v any) error {
	return effectReadJSONBounded(path, v, 1<<20)
}

func effectReadJSONBounded(path string, v any, limit int64) error {
	if limit != 1<<20 && limit != controlSourceSnapshotLimit {
		return effects.ErrEvidence
	}

	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if canonical != path {
		return effects.ErrEvidence
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > limit {
		return effects.ErrEvidence
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return effects.ErrEvidence
	}
	if limit == 1<<20 {
		if err := decodeEffectJSON(b, v); err != nil {
			return err
		}
	} else if err := toolcontract.DecodeBroker(b, v); err != nil {
		return err
	}

	canonicalBytes, err := json.Marshal(v)
	if err != nil || !bytes.Equal(canonicalBytes, b) {
		return effects.ErrEvidence
	}
	return nil
}
func effectDirectory(path string, mode os.FileMode) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return effects.ErrEvidence
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(path, current), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		err := os.Mkdir(current, mode)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		st, err := os.Lstat(current)
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return effects.ErrEvidence
		}
	}
	st, err := os.Lstat(path)
	if err != nil || st.Mode().Perm()&0022 != 0 {
		return effects.ErrEvidence
	}
	return nil
}
func effectMkdir(path string) error {
	if err := effectDirectory(path, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil || st.Mode().Perm()&0077 != 0 {
		return effects.ErrEvidence
	}
	return nil
}
func effectSyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func effectWriteJSON(path string, value any) error {
	if err := effectMkdir(filepath.Dir(path)); err != nil {
		return err
	}
	if st, err := os.Lstat(path); err == nil {
		if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return effects.ErrEvidence
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil || len(b) > 1<<20 {
		return effects.ErrEvidence
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".effect-tmp-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return effectSyncDir(filepath.Dir(path))
}
func effectWriteImmutable(path string, value any) error {
	return effectWriteImmutableBounded(path, value, 1<<20)
}

func effectWriteImmutableBounded(path string, value any, limit int64) error {
	if limit != 1<<20 && limit != controlSourceSnapshotLimit {
		return effects.ErrEvidence
	}

	if err := effectMkdir(filepath.Dir(path)); err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil || int64(len(b)) > limit {
		return effects.ErrEvidence
	}
	if st, err := os.Lstat(path); err == nil {
		if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return effects.ErrEvidence
		}
		current, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if !bytes.Equal(current, b) {
			return effects.ErrConflict
		}
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		e = f.Sync()
		f.Close()
		if e != nil {
			return e
		}
		return effectSyncDir(filepath.Dir(path))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".effect-tmp-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(name, path); errors.Is(err, os.ErrExist) {
		return effectWriteImmutableBounded(path, value, limit)
	} else if err != nil {
		return err
	}
	if err := os.Remove(name); err != nil {
		return err
	}
	return effectSyncDir(filepath.Dir(path))
}
func effectProcessTemporary(name string) (bool, error) {
	if !strings.HasPrefix(filepath.Base(name), ".effect-tmp-") {
		return false, nil
	}
	st, err := os.Lstat(name)
	if err != nil {
		return false, err
	}
	identity, ok := st.Sys().(*syscall.Stat_t)
	if !ok || identity.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return false, effects.ErrEvidence
	}
	return true, nil
}

func effectExecutableHash(path string) (effects.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return effects.Digest{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > 256<<20 {
		return effects.Digest{}, effects.ErrEvidence
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return effects.Digest{}, err
	}
	var digest effects.Digest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
func effectSubjectLock(ctx context.Context, brain, subjectID string) (func() error, error) {
	if _, err := codex.ProgramProcessPath(brain, subjectID, subjectID+"."+strings.Repeat("0", 32)); err != nil {
		return nil, err
	}
	if err := effectDirectory(filepath.Join(brain, "factory", ".lock"), 0755); err != nil {
		return nil, err
	}
	return waitLock(ctx, brain, "effect-process-"+subjectID)
}
func effectSelectedPath(brain, subjectID string) string {
	return filepath.Join(brain, "factory", "effects", "dispatch", subjectID, "selected.json")
}
func effectReadProcess(brain, claimID string) (codex.ProgramProcessRecord, error) {
	var r codex.ProgramProcessRecord
	parts := strings.Split(claimID, ".")
	if len(parts) != 2 {
		return r, effects.ErrEvidence
	}
	path, err := codex.ProgramProcessPath(brain, parts[0], claimID)
	if err != nil {
		return r, err
	}
	if err := effectReadJSON(path, &r); err != nil {
		return r, err
	}
	if r.ClaimID != claimID || r.SubjectID != parts[0] || codex.ValidateProgramProcessRecord(r) != nil {
		return r, effects.ErrEvidence
	}
	return r, nil
}
func effectWriteProcess(brain string, r codex.ProgramProcessRecord) error {
	if err := codex.ValidateProgramProcessRecord(r); err != nil {
		return err
	}
	path, err := codex.ProgramProcessPath(brain, r.SubjectID, r.ClaimID)
	if err != nil {
		return err
	}
	return effectWriteJSON(path, r)
}
func effectSelectedProcess(brain, subjectID string) (codex.ProgramProcessRecord, error) {
	var selected codex.ProgramProcessSelection
	if err := effectReadJSON(effectSelectedPath(brain, subjectID), &selected); err != nil {
		return codex.ProgramProcessRecord{}, err
	}
	r, err := effectReadProcess(brain, selected.ClaimID)
	if err != nil {
		return r, err
	}
	if selected != codex.ProgramProcessSelectionFor(r) || codex.ProgramProcessDigest(selected) != r.SelectionSHA256 {
		return r, effects.ErrEvidence
	}
	return r, nil
}

// The concrete caller holds its tick or acceptance-grant guard through this CAS.
func prepareProgramProcessLocked(ctx context.Context, brain, factoryBin string, subject codex.ProgramProcessSubject, ownerID, logicalRevision string, input effects.Digest, mode string, deadline time.Time) (codex.ProgramProcessRecord, string, error) {
	return prepareProgramProcessWithSelectionWriterLocked(ctx, brain, factoryBin, subject, ownerID, logicalRevision, input, mode, deadline, effectWriteJSON)
}

func prepareProgramProcessWithSelectionWriterLocked(ctx context.Context, brain, factoryBin string, subject codex.ProgramProcessSubject, ownerID, logicalRevision string, input effects.Digest, mode string, deadline time.Time, writeSelection func(string, any) error) (codex.ProgramProcessRecord, string, error) {
	var none codex.ProgramProcessRecord
	id, err := codex.ProgramProcessSubjectID(subject)
	if err != nil {
		return none, "", err
	}
	unlock, err := effectSubjectLock(ctx, brain, id)
	if err != nil {
		return none, "", err
	}
	defer unlock()
	if subject.Kind == "program_read" && mode == "check" && subject.ProgramRead != nil {
		if err := rejectProgramReadNoAction(ctx, brain, *subject.ProgramRead); err != nil {
			return none, "", err
		}
	}
	var selectionRevision uint64 = 1
	old, err := effectSelectedProcess(brain, id)
	if err == nil {
		if old.Stage != "closed" {
			return old, "", effects.ErrBusy
		}
		if _, err := codex.ReadClosedProgramProcess(ctx, brain, codex.ProgramProcessRef{SubjectID: old.SubjectID, ClaimID: old.ClaimID, Revision: old.Revision, SHA256: codex.ProgramProcessDigest(old)}); err != nil {
			return none, "", err
		}
		selectionRevision = old.SelectionRevision + 1
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, "", err
	}
	limit, err := codex.ProgramProcessLimit(subject, mode)
	if err != nil {
		return none, "", err
	}
	admitted := time.Now().UTC()
	bound := admitted.Add(limit)
	check := subject.Kind == "program_read" && mode == "check"
	if check {
		if subject.ProgramRead == nil || input != subject.ProgramRead.SHA256 {
			return none, "", effects.ErrEvidence
		}
		request, err := codex.ReadProgramReadRequest(ctx, brain, *subject.ProgramRead)
		if err != nil {
			return none, "", err
		}
		if request.Kind != "program_read" || request.Operation != "registry_checks" || request.Grant != nil || !deadline.Equal(request.Deadline) {
			return none, "", effects.ErrEvidence
		}
	}
	if !deadline.IsZero() && deadline.Before(bound) {
		bound = deadline
	}
	if d, ok := ctx.Deadline(); !check && ok && d.Before(bound) {
		bound = d
	}
	if !bound.After(admitted) {
		return none, "", context.DeadlineExceeded
	}
	random := make([]byte, 16)
	nonceBytes := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return none, "", err
	}
	if _, err := rand.Read(nonceBytes); err != nil {
		return none, "", err
	}
	nonce := hex.EncodeToString(nonceBytes)
	binary, err := effectExecutableHash(factoryBin)
	if err != nil {
		return none, "", err
	}
	if check {
		if err := ctx.Err(); err != nil {
			return none, "", err
		}
	}
	r := codex.ProgramProcessRecord{Schema: 1, Revision: 1, Subject: subject, SubjectID: id, ClaimID: id + "." + hex.EncodeToString(random), NonceSHA256: sha256.Sum256([]byte(nonce)), OwnerID: ownerID, LogicalRevision: logicalRevision, InputSHA256: input, FactorySHA256: binary, Mode: mode, AdmittedAt: admitted, Deadline: bound, SelectionRevision: selectionRevision, Stage: "prepared"}
	r.EnrollmentSHA256 = codex.ProgramProcessEnrollmentDigest(r)
	r.SelectionSHA256 = codex.ProgramProcessDigest(codex.ProgramProcessSelectionFor(r))
	if err := effectWriteProcess(brain, r); err != nil {
		return none, "", err
	}
	if err := writeSelection(effectSelectedPath(brain, id), codex.ProgramProcessSelectionFor(r)); err != nil {
		return none, "", err
	}
	return r, nonce, nil
}

func claimProgramProcessLocked(ctx context.Context, brain, claimID, nonce string, identity codex.ProcessIdentity) (codex.ProgramProcessRecord, error) {
	var none codex.ProgramProcessRecord
	if len(nonce) != 64 || identity.PID != os.Getpid() || identity.PID <= 1 || identity.Group != identity.PID || identity.Birth == "" {
		return none, effects.ErrEvidence
	}
	parts := strings.Split(claimID, ".")
	if len(parts) != 2 {
		return none, effects.ErrEvidence
	}
	unlock, err := effectSubjectLock(ctx, brain, parts[0])
	if err != nil {
		return none, err
	}
	defer unlock()
	r, err := effectSelectedProcess(brain, parts[0])
	if err != nil {
		return none, err
	}
	if r.Mode == "check" && r.Subject.ProgramRead != nil {
		if err := rejectProgramReadNoAction(ctx, brain, *r.Subject.ProgramRead); err != nil {
			return none, err
		}
	}
	if r.ClaimID != claimID || r.Stage != "prepared" || r.NonceSHA256 != effects.Digest(sha256.Sum256([]byte(nonce))) || !time.Now().Before(r.Deadline) {
		return none, effects.ErrStale
	}
	binary, err := os.Executable()
	if err != nil {
		return none, err
	}
	actual, err := effectExecutableHash(binary)
	if err != nil || actual != r.FactorySHA256 {
		return none, effects.ErrAuthority
	}
	r.Stage = "claimed"
	r.Revision++
	r.Host = identity
	if err := effectWriteProcess(brain, r); err != nil {
		return none, err
	}
	return r, nil
}

func sealProgramProcessLocked(ctx context.Context, brain, claimID string) (codex.ProgramProcessRecord, error) {
	var none codex.ProgramProcessRecord
	parts := strings.Split(claimID, ".")
	if len(parts) != 2 {
		return none, effects.ErrEvidence
	}
	unlock, err := effectSubjectLock(ctx, brain, parts[0])
	if err != nil {
		return none, err
	}
	defer unlock()
	return sealProgramProcessUnderSubjectLocked(ctx, brain, claimID)
}

func sealProgramProcessUnderSubjectLocked(ctx context.Context, brain, claimID string) (codex.ProgramProcessRecord, error) {
	var none codex.ProgramProcessRecord
	if err := ctx.Err(); err != nil {
		return none, err
	}
	r, err := effectReadProcess(brain, claimID)
	if err != nil {
		return none, err
	}
	if r.Stage == "sealed" || r.Stage == "closed" {
		return r, nil
	}
	if r.Stage != "prepared" && r.Stage != "claimed" {
		return none, effects.ErrEvidence
	}
	r.Stage = "sealed"
	r.Revision++
	r.SealedRevision = r.Revision
	if err := effectWriteProcess(brain, r); err != nil {
		return none, err
	}
	return r, nil
}

func closeProgramProcessLocked(ctx context.Context, brain string, expected codex.ProgramProcessRecord, dispatchID string) (codex.ProgramProcessRecord, error) {
	claimID := expected.ClaimID
	var none codex.ProgramProcessRecord
	parts := strings.Split(claimID, ".")
	if len(parts) != 2 {
		return none, effects.ErrEvidence
	}
	unlock, err := effectSubjectLock(ctx, brain, parts[0])
	if err != nil {
		return none, err
	}
	defer unlock()
	return closeProgramProcessUnderSubjectLocked(ctx, brain, expected, dispatchID)
}

func closeProgramProcessUnderSubjectLocked(ctx context.Context, brain string, expected codex.ProgramProcessRecord, dispatchID string) (codex.ProgramProcessRecord, error) {
	var none codex.ProgramProcessRecord
	if err := ctx.Err(); err != nil {
		return none, err
	}
	claimID := expected.ClaimID
	r, err := effectReadProcess(brain, claimID)
	if err != nil {
		return none, err
	}
	if r.Stage == "closed" {
		if r.ClosedProof.DispatchID != dispatchID {
			return none, effects.ErrConflict
		}
		return r, nil
	}
	if r.Stage != "sealed" {
		return none, effects.ErrBusy
	}
	if codex.ProgramProcessDigest(r) != codex.ProgramProcessDigest(expected) {
		return none, effects.ErrStale
	}
	kind := "never_claimed"
	if r.Host != (codex.ProcessIdentity{}) {
		if err := codex.VerifyOwnedProcessGroupGone(r.Host); err != nil {
			return none, err
		}
		kind = "owned_group_gone"
	} else if dispatchID != "" {
		return none, effects.ErrEvidence
	}
	r.Stage = "closed"
	r.NextCheckAt = time.Time{}
	r.Revision++
	r.ClosedProof = &codex.ProgramProcessClosedProof{Kind: kind, SubjectID: r.SubjectID, ClaimID: r.ClaimID, SealedRevision: r.SealedRevision, EnrollmentSHA256: r.EnrollmentSHA256, SelectionSHA256: r.SelectionSHA256, Host: r.Host, Deadline: r.Deadline, DispatchID: dispatchID}
	if err := effectWriteProcess(brain, r); err != nil {
		return none, err
	}
	return r, nil
}

// Only retry scheduling changes at this stage; the complete record hash is the CAS.
func deferProgramProcessFenceLocked(ctx context.Context, brain string, expected codex.ProgramProcessRecord) (codex.ProgramProcessRecord, error) {
	var none codex.ProgramProcessRecord
	unlock, err := effectSubjectLock(ctx, brain, expected.SubjectID)
	if err != nil {
		return none, err
	}
	defer unlock()
	current, err := effectReadProcess(brain, expected.ClaimID)
	if err != nil {
		return none, err
	}
	if current.Stage != "sealed" || codex.ProgramProcessDigest(current) != codex.ProgramProcessDigest(expected) {
		return none, effects.ErrStale
	}
	current.NextCheckAt = time.Now().UTC().Add(time.Minute)
	if err := effectWriteProcess(brain, current); err != nil {
		return none, err
	}
	return current, nil
}

type EffectProgramBootstrap struct{ Brain, GHConfigDir string }

// The concrete source facade has already admitted the claim under its own guard.
func programActorBootstrapLocked(ctx context.Context, brain string, expected codex.ProgramProcessRecord) (EffectProgramBootstrap, error) {
	var none EffectProgramBootstrap
	if err := ctx.Err(); err != nil {
		return none, err
	}
	current, err := effectSelectedProcess(brain, expected.SubjectID)
	if err != nil {
		return none, err
	}
	if current.Mode == "check" && current.Subject.ProgramRead != nil {
		if err := rejectProgramReadNoAction(ctx, brain, *current.Subject.ProgramRead); err != nil {
			return none, err
		}
	}
	group, err := syscall.Getpgid(os.Getpid())
	if err != nil || current.Stage != "claimed" || current.ClaimID != expected.ClaimID || current.EnrollmentSHA256 != expected.EnrollmentSHA256 || current.Host != expected.Host || current.Host.PID != os.Getpid() || current.Host.Group != group || !time.Now().Before(current.Deadline) {
		return none, effects.ErrStale
	}
	executable, err := os.Executable()
	if err != nil {
		return none, err
	}
	hash, err := effectExecutableHash(executable)
	if err != nil || hash != current.FactorySHA256 {
		return none, effects.ErrAuthority
	}
	recordPath, err := codex.ProgramProcessPath(brain, current.SubjectID, current.ClaimID)
	if err != nil {
		return none, err
	}
	config := filepath.Join(filepath.Dir(recordPath), current.ClaimID+".gh-config")
	if err := effectMkdir(config); err != nil {
		return none, err
	}
	return EffectProgramBootstrap{Brain: brain, GHConfigDir: config}, nil
}

func decodeEffectJSON(b []byte, v any) error { return toolcontract.DecodeStrict(b, v) }

// Process observation is outside every source/record/ledger guard.
func fenceProgramProcess(ctx context.Context, brain string, expected codex.ProgramProcessRecord) error {
	current, err := effectSelectedProcess(brain, expected.SubjectID)
	if err != nil {
		return err
	}
	if current.Stage != "sealed" || current.ClaimID != expected.ClaimID || codex.ProgramProcessDigest(current) != codex.ProgramProcessDigest(expected) {
		return effects.ErrStale
	}
	if !current.NextCheckAt.IsZero() && time.Now().Before(current.NextCheckAt) {
		return effects.ErrBusy
	}
	if current.Host == (codex.ProcessIdentity{}) {
		return nil
	}
	err = codex.StopOwnedProcessGroup(ctx, current.Host)
	if actual := codex.VerifyOwnedProcessGroupGone(current.Host); actual == nil {
		return nil
	}
	if err != nil {
		return err
	}
	return effects.ErrBusy
}

// This waiter owns no source guard. Its caller performs the source-specific seal.
func waitProgramProcess(ctx context.Context, brain, factoryBin string, initial codex.ProgramProcessRecord, nonce string) (codex.ProgramProcessRecord, error) {
	current, err := effectSelectedProcess(brain, initial.SubjectID)
	if err != nil {
		return current, err
	}
	if current.ClaimID != initial.ClaimID || current.EnrollmentSHA256 != initial.EnrollmentSHA256 {
		return current, effects.ErrStale
	}
	var finished <-chan error
	if current.Stage == "prepared" {
		if nonce == "" {
			return current, effects.ErrBusy
		}
		hash, err := effectExecutableHash(factoryBin)
		if err != nil || hash != current.FactorySHA256 {
			return current, effects.ErrAuthority
		}
		command := exec.Command(factoryBin, "effect-dispatch", "--claim", current.ClaimID, "--nonce", nonce)
		command.Env = zednative.WithoutGitHubCredentials(os.Environ())
		command.Env = append(command.Env, "FACTORY_BRAIN="+brain)
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := command.Start(); err != nil {
			return current, err
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait(); close(done) }()
		finished = done
	} else if nonce != "" {
		return current, effects.ErrStale
	}
	timer := time.NewTimer(time.Until(current.Deadline))
	defer timer.Stop()
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	for {
		current, err = effectReadProcess(brain, initial.ClaimID)
		if err != nil {
			return current, err
		}
		if current.EnrollmentSHA256 != initial.EnrollmentSHA256 {
			return current, effects.ErrStale
		}
		if current.Stage == "sealed" || current.Stage == "closed" {
			return current, nil
		}
		if finished == nil && current.Stage == "claimed" {
			alive, err := current.Host.Alive()
			if err != nil {
				return current, err
			}
			if !alive {
				return current, nil
			}
		}
		select {
		case err := <-finished:
			actual, readErr := effectReadProcess(brain, initial.ClaimID)
			if readErr != nil {
				return actual, readErr
			}
			return actual, err
		case <-ctx.Done():
			return current, ctx.Err()
		case <-timer.C:
			return current, context.DeadlineExceeded
		case <-poll.C:
		}
	}
}
