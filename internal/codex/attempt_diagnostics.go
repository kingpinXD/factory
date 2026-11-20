package codex

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// ControlFailureCode never includes caller data or free-form error text.
func ControlFailureCode(cause error) string {
	var input *toolcontract.ToolInputError
	if errors.As(cause, &input) {
		return "invalid_input"
	}
	switch {
	case errors.Is(cause, effects.ErrAuthority), errors.Is(cause, helper.ErrAuthority), errors.Is(cause, ErrAttemptStale):
		return "control_authority_invalid"
	case errors.Is(cause, helper.ErrControlAdmissionClosed):
		return "control_admission_closed"
	case errors.Is(cause, ErrAcceptancePending), errors.Is(cause, ErrCapabilitiesUnverified):
		return "control_qualification_required"
	case errors.Is(cause, os.ErrNotExist):
		return "control_evidence_missing"
	case errors.Is(cause, effects.ErrEvidence):
		return "control_evidence_invalid"
	default:
		return "control_failed"
	}
}

// RecordAttemptHostFailure retains sanitized host error output even when its
// inherited stderr is /dev/null. Arbitrary subprocess output is never persisted.
func RecordAttemptHostFailure(brain string, ref AttemptRef, cause error) error {
	if cause == nil {
		return nil
	}
	return appendAttemptError(brain, ref, struct {
		At    time.Time `json:"at"`
		Stage string    `json:"stage"`
		Code  string    `json:"code"`
	}{time.Now().UTC(), "host_failure", ControlFailureCode(cause)})
}

func RecordAttemptControlRefusal(brain string, ref AttemptRef, stage, code string) error {
	return appendAttemptError(brain, ref, struct {
		At    time.Time `json:"at"`
		Stage string    `json:"stage"`
		Code  string    `json:"code"`
	}{time.Now().UTC(), stage, code})
}

func appendAttemptError(brain string, ref AttemptRef, value any) error {
	directory, err := New(brain, "").attemptDirectory(ref)
	if err != nil {
		return err
	}
	if err := privateAttemptParents(brain, directory, false); err != nil {
		return err
	}
	release, err := store.Lock(brain, "codex-errors-"+ref.SessionID+"-"+ref.AttemptID)
	if err != nil {
		return err
	}
	defer release()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	const limit = 64 * 1024
	if len(data) > limit {
		return ErrAttemptConflict
	}
	filename := filepath.Join(directory, "host-errors.log")
	for _, name := range []string{filename, filename + ".1"} {
		info, err := os.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		raw, ok := info.Sys().(*syscall.Stat_t)
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !ok || raw.Nlink != 1 || info.Size() > limit {
			return ErrAttemptConflict
		}
	}
	if info, err := os.Stat(filename); err == nil && info.Size()+int64(len(data)) > limit {
		if err := os.Rename(filename, filename+".1"); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fd, err := syscall.Open(filename, syscall.O_WRONLY|syscall.O_APPEND|syscall.O_CREAT|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), filename)
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}
