package codex

import (
	"context"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/store"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func (r *Runtime) Recover(ctx context.Context) error {
	return errors.Join(r.RecoverAttempts(ctx), r.RecoverLegacy(ctx))
}

func (r *Runtime) RecoverAttempts(ctx context.Context) error { return r.recoverSessions(ctx, true) }
func (r *Runtime) RecoverLegacy(ctx context.Context) error   { return r.recoverSessions(ctx, false) }

func (r *Runtime) recoverSessions(ctx context.Context, modern bool) error {
	entries, err := os.ReadDir(filepath.Join(store.Factory(r.Brain), "codex", "sessions"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var holds []error
	for _, entry := range entries {
		if !entry.IsDir() || !recordID.MatchString(entry.Name()) {
			holds = append(holds, ErrObservationUnknown)
			continue
		}
		if !modern {
			selected, e := r.hasAttemptIndex(entry.Name())
			if e != nil {
				holds = append(holds, e)
				continue
			}
			if selected {
				continue
			}
			source, e := r.inspectLegacy(entry.Name(), true)
			if e != nil {
				holds = append(holds, e)
				continue
			}
			record := source.Record
			if record.Host.PID == 0 {
				continue
			}
			alive, e := record.Host.Alive()
			if e != nil {
				holds = append(holds, e)
				continue
			}
			if alive {
				continue
			}
			if unsubmittedInitialTurn(record) && len(source.UnknownFields) == 0 {
				e = r.observeStopped(ctx, &record)
			} else {
				e = r.SettleLegacy(ctx, entry.Name())
			}
			if e != nil {
				holds = append(holds, e)
			}
			continue
		}
		record, err := r.read(entry.Name())
		if err != nil {
			holds = append(holds, err)
			continue
		}
		if record.Schema == 2 {
			if err := r.recoverAttempt(ctx, record); err != nil {
				holds = append(holds, err)
			}
		}
	}
	return errors.Join(holds...)
}

func (r *Runtime) observeStopped(ctx context.Context, record *Record) error {
	unsubmitted := unsubmittedInitialTurn(*record)
	if unsubmitted && record.State != "held" && record.State != "starting" {
		return ErrObservationUnknown
	}
	if record.ThreadID == "" || (record.TurnID == "" && !unsubmitted) || record.Rerouted {
		return ErrObservationUnknown
	}
	for _, state := range record.Deliveries {
		if state != "complete" {
			return ErrObservationUnknown
		}
	}
	dir, err := r.directory(record.ID)
	if err != nil {
		return err
	}
	owner := Owner{ID: record.ID, Generation: hash([]byte(record.Spec.Generation)), Worktree: record.Spec.Cwd, RunDir: filepath.Join(dir, fmt.Sprintf("remote-%d", record.Epoch)), OutputRoot: record.Spec.Cwd, Selection: record.Selection, ControlOnly: true}
	if r.Containment == nil {
		return ErrCapabilitiesUnverified
	}
	if err = r.Containment.Fence(ctx, owner); err != nil {
		return err
	}
	if record.Server.PID != 0 {
		if err = stopOwnedProcess(ctx, record.Server); err != nil {
			return err
		}
	}
	if unsubmitted {
		for _, identity := range []ProcessIdentity{record.Host, record.Server} {
			if identity.Group <= 0 || syscall.Kill(-identity.Group, 0) != syscall.ESRCH {
				return ErrObservationUnknown
			}
		}
		record.State = "stopped"
		return r.save(record)
	}
	observer, err := openServer(ctx, record.Selection, dir, r.Brain, "coordinator-observer")
	if err != nil {
		return err
	}
	defer observer.Close()
	if err = verifyStoppedTurn(ctx, observer, *record); err != nil {
		return err
	}
	record.State = "stopped"
	return r.save(record)
}

func unsubmittedInitialTurn(record Record) bool {
	if record.ThreadID == "" || record.TurnID != "" || record.TurnRequested || record.ResumeAttempt || record.Rerouted || !record.LastTurnEnded.IsZero() {
		return false
	}
	if len(record.Deliveries) != 0 || record.CompactionIntent != "" || record.CompactionDone != "" || record.CompactionTurnID != "" || record.CompactionItemID != "" || len(record.Commands) != 1 {
		return false
	}
	command := record.Commands[0]
	return command.Key == "initial" && command.Operation == "deliver" && command.State == "queued"
}

func stopOwnedProcess(ctx context.Context, identity ProcessIdentity) error {
	alive, err := identity.Alive()
	if err != nil {
		return err
	}
	if alive {
		if err = syscall.Kill(-identity.Group, syscall.SIGTERM); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	soft := time.NewTimer(time.Second)
	defer soft.Stop()
	hard := time.NewTimer(5 * time.Second)
	defer hard.Stop()
	for alive {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hard.C:
			return ErrObservationUnknown
		case <-soft.C:
			current, e := CaptureProcess(identity.PID)
			if e != nil || current != identity {
				return ErrObservationUnknown
			}
			if e = syscall.Kill(-identity.Group, syscall.SIGKILL); e != nil {
				return e
			}
		case <-ticker.C:
		}
		alive, err = identity.Alive()
		if err != nil {
			return err
		}
	}
	if syscall.Kill(-identity.Group, 0) != syscall.ESRCH {
		return ErrObservationUnknown
	}
	return nil
}

func waitRetiredHost(ctx context.Context, record Record) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		gone := true
		for _, identity := range []ProcessIdentity{record.Host, record.Server} {
			alive, err := identity.Alive()
			if err != nil {
				return err
			}
			if alive || syscall.Kill(-identity.Group, 0) != syscall.ESRCH {
				gone = false
			}
		}
		if gone {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrObservationUnknown
		case <-ticker.C:
		}
	}
}
