package reconcile

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
)

type ReadIngressClosure = codex.ProgramReadIngressClosure

type ProgramReadNoActionError struct{ Reference codex.ProgramReadNoActionRef }

func (*ProgramReadNoActionError) Error() string { return "programme read finished without execution" }
func (*ProgramReadNoActionError) Unwrap() error { return &gh.ProgramReadUnavailable{} }

func rejectProgramReadNoAction(ctx context.Context, brain string, ref codex.ProgramReadRef) error {
	actual, err := codex.ProgramReadNoActionForRequest(ctx, brain, ref)
	if err == nil {
		return &ProgramReadNoActionError{Reference: actual}
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// The caller holds tick. This negative settlement cannot admit a child.
func CloseUnselectedProgramReadLocked(ctx context.Context, brain string, ref codex.ProgramReadRef, closure ReadIngressClosure) (codex.ProgramReadNoActionRef, error) {
	return closeUnselectedProgramReadLocked(ctx, brain, ref, closure, codex.Contract2AcceptanceAuthority)
}
func closeUnselectedProgramReadLocked(ctx context.Context, brain string, ref codex.ProgramReadRef, closure ReadIngressClosure, verify nativeAcceptance) (codex.ProgramReadNoActionRef, error) {
	var none codex.ProgramReadNoActionRef
	request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
	if err != nil {
		return none, err
	}
	admission, err := readProgrammeAdmission(ctx, brain, request, verify)
	if err != nil {
		return none, err
	}
	if admission.Program != nil {
		if closure != (ReadIngressClosure{}) {
			return none, effects.ErrEvidence
		}
		return closeUnselectedRegistryCheckLocked(ctx, brain, request, ref, admission, verify)
	}
	subject, err := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	if err != nil {
		return none, err
	}
	unlock, err := effectSubjectLock(ctx, brain, subject)
	if err != nil {
		return none, err
	}
	defer unlock()
	existing, err := codex.ProgramReadNoActionForRequest(ctx, brain, ref)
	if err == nil {
		record, err := codex.ReadProgramReadNoActionLocal(ctx, brain, existing)
		if err != nil {
			return none, err
		}
		empty := closure == (ReadIngressClosure{})
		if empty != (record.Closure == nil) || !empty && codex.ProgramProcessDigest(closure) != codex.ProgramProcessDigest(*record.Closure) {
			return none, effects.ErrStale
		}
		return existing, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	selectedPath := effectSelectedPath(brain, subject)
	if err := validateProgramReadPhysicalParents(brain, filepath.Dir(selectedPath)); err != nil {
		return none, err
	}
	if _, err := os.Lstat(selectedPath); err == nil {
		selected, err := effectSelectedProcess(brain, subject)
		if err != nil {
			return none, err
		}
		if err := validateReadEnrollmentUsing(ctx, brain, request, selected, verify); err != nil {
			return none, err
		}
		return none, &ProgramReadHeld{Status: programReadStatus(selected), Cause: effects.ErrBusy}
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
	if err != nil {
		return none, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "result.json")); err == nil {
		return none, effects.ErrEvidence
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	enrollments, err := unselectedProgramReadEnrollments(ctx, brain, request, subject, verify)
	if err != nil {
		return none, err
	}
	original := admission.Helper
	record := codex.ProgramReadNoAction{Schema: 2, Request: ref, SubjectID: subject, Enrollments: enrollments, ObservedAt: time.Now().UTC()}
	if admission.Control != nil {
		record.Original = admission.Control.Original
	} else {
		record.Original = original.Original
		scope := admission.Work.Authorization()
		record.Scope = &scope
	}
	if closure == (ReadIngressClosure{}) {
		if record.ObservedAt.Before(request.Deadline) {
			return none, effects.ErrBusy
		}
		record.Code = codex.ProgramReadNoActionExpired
	} else {
		record.Code = codex.ProgramReadNoActionClosed
		record.Closure = &closure
	}
	return codex.PublishProgramReadNoActionLocked(ctx, brain, record)
}

func validateProgramReadPhysicalParents(brain, dir string) error {
	if !filepath.IsAbs(brain) || filepath.Clean(brain) != brain {
		return effects.ErrEvidence
	}
	current := brain
	relative, err := filepath.Rel(brain, dir)
	if err != nil || strings.HasPrefix(relative, "..") {
		return effects.ErrEvidence
	}
	for _, part := range append([]string{""}, strings.Split(relative, string(filepath.Separator))...) {
		if part != "" {
			current = filepath.Join(current, part)
		}
		st, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0022 != 0 {
			return effects.ErrEvidence
		}
		canonical, err := filepath.EvalSymlinks(current)
		if err != nil || canonical != current {
			return effects.ErrEvidence
		}
	}
	return nil
}

func unselectedProgramReadEnrollments(ctx context.Context, brain string, request codex.ProgramReadRequest, subject string, verify nativeAcceptance) ([]codex.ProgramProcessRef, error) {
	result := []codex.ProgramProcessRef{}
	dir := filepath.Dir(effectSelectedPath(brain, subject))
	directory, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	budget := 8192
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := directory.ReadDir(1)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			budget--
			if budget < 0 {
				return nil, effects.ErrEvidence
			}
			temporary, err := effectProcessTemporary(filepath.Join(dir, entry.Name()))
			if err != nil {
				return nil, err
			}
			if temporary {
				continue
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || entry.Name() == "selected.json" {
				return nil, effects.ErrEvidence
			}
			claimID := strings.TrimSuffix(entry.Name(), ".json")
			record, err := effectReadProcess(brain, claimID)
			if err != nil {
				return nil, err
			}
			if record.SubjectID != subject || record.Stage != "prepared" {
				return nil, effects.ErrEvidence
			}
			if err := validateReadEnrollmentUsing(ctx, brain, request, record, verify); err != nil {
				return nil, err
			}
			result = append(result, programReadStatus(record).Process)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ClaimID < result[j].ClaimID })
	return result, nil
}

func programmeReadIngressClosure(ctx context.Context, brain string, a ProgramReadAdmission) (ReadIngressClosure, error) {
	if a.Control != nil {
		seal, err := codex.New(brain, "").SealedAttempt(ctx, a.Control.Receipt.Control.Attempt)
		if err != nil {
			return ReadIngressClosure{}, err
		}
		return ReadIngressClosure{Original: a.Control.Original, AttemptSeal: &seal}, nil
	}
	if a.Helper == nil || a.Work == nil {
		return ReadIngressClosure{}, effects.ErrEvidence
	}
	fence, err := (helper.Service{Brain: brain}).ReadClosedScopeFence(ctx, a.Work.WorkID)
	if err != nil {
		return ReadIngressClosure{}, err
	}
	return ReadIngressClosure{Original: a.Helper.Original, WorkSeal: &effects.WorkIngressSeal{WorkID: a.Work.WorkID, OwnerID: a.Authority.OwnerID, OwnerRevision: a.Authority.Revision, Fence: effects.Artifact{Path: fence.Path, SHA256: fence.SHA256}}}, nil
}

func settleUnselectedProgrammeRead(ctx context.Context, brain string, request codex.ProgramReadRequest, ref codex.ProgramReadRef, verify nativeAcceptance, expired bool) (codex.ProgramReadNoActionRef, error) {
	a, err := readProgrammeAdmission(ctx, brain, request, verify)
	if err != nil {
		return codex.ProgramReadNoActionRef{}, err
	}
	var closure ReadIngressClosure
	if !expired {
		closure, err = programmeReadIngressClosure(ctx, brain, a)
		if err != nil {
			return codex.ProgramReadNoActionRef{}, err
		}
	}
	unlock, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return codex.ProgramReadNoActionRef{}, err
	}
	defer unlock()
	return closeUnselectedProgramReadLocked(ctx, brain, ref, closure, verify)
}
